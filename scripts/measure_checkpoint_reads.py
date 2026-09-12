#!/usr/bin/env python3
"""Time complete pinned pagination of one published account on local ClickHouse.

Each pass must return exactly the manifest's nonzero slot count and identical
ordered content. Measures the Python reader/coordination/SQL path, not an HTTP API.
Set EVM_STATE_HOME to the database's existing controller directory.
"""
import argparse
import hashlib
import json
from pathlib import Path
import time

from evm_state.ch import ClickHouse
from evm_state.checkpoint import read_account
from evm_state.files import atomic_json
from evm_state.reader import page, pin, unpin
from qualify_throughput import distribution


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--snapshot-id", required=True)
    parser.add_argument("--account", required=True)
    parser.add_argument("--page-size", type=int, default=1000)
    parser.add_argument("--passes", type=int, default=5)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists() or not 1 <= args.passes <= 10 or not 1 <= args.page_size <= 10000:
        parser.error("use a new output file, 1..10 passes and page size 1..10000")
    client = ClickHouse(args.database)
    record = pin(client, args.snapshot_id, "qualification-read-latency")
    calls, passes, digests = [], [], []
    started = time.time_ns()
    try:
        account = read_account(client, args.snapshot_id, args.account)
        for iteration in range(args.passes):
            cursor, count, digest = None, 0, hashlib.sha256()
            began = time.monotonic()
            while True:
                before = time.monotonic()
                result = page(client, record["pin_id"], args.account, cursor, args.page_size)
                latency = time.monotonic() - before
                if result["snapshot_id"] != args.snapshot_id or result["header"] != record["header"]:
                    raise ValueError("reader changed checkpoint identity")
                for row in result["storage"]:
                    digest.update(bytes.fromhex(row["slot"][2:]))
                    digest.update(bytes.fromhex(row["value"][2:]))
                count += len(result["storage"])
                calls.append({"pass": iteration, "page": len(calls), "rows": len(result["storage"]),
                              "seconds": latency, "json_bytes": len(json.dumps(result).encode())})
                cursor = result["next_cursor"]
                if cursor is None:
                    break
            if count != int(account["nonzero_slots"]):
                raise ValueError("reader returned an incomplete account")
            digests.append(digest.hexdigest())
            passes.append({"pass": iteration, "seconds": time.monotonic() - began, "nonzero_slots": count})
        if len(set(digests)) != 1:
            raise ValueError("pinned account content changed between passes")
    finally:
        unpin(client, record["pin_id"])
    atomic_json(args.output, {"format_version": 1, "database": args.database, "snapshot_id": args.snapshot_id,
        "account": args.account, "header": record["header"], "started_at_unix_ns": started,
        "finished_at_unix_ns": time.time_ns(), "page_size": args.page_size, "passes": passes,
        "page_latency_seconds": distribution([call["seconds"] for call in calls]),
        "ordered_storage_sha256": digests[0], "calls": calls,
        "limitations": ["local Python reader, controller locks and ClickHouse SQL; no remote API network",
                        "sequential repeated scans, not concurrent reader load or a cold database cache",
                        "immutable published generation; publication latency measured separately"]})
    print(json.dumps({"output": str(args.output), "pages": len(calls),
                      "page_latency_seconds": distribution([call["seconds"] for call in calls])}))


if __name__ == "__main__":
    main()
