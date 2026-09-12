#!/usr/bin/env python3
"""Measure logical protobuf output from a complete, unpruned native interval.

Uses the frozen run's protobuf descriptor and verifies its block interval against
an RPC-finalized header. This is logical decoded output, not wire traffic or an
invoice, and does not establish complete initial account storage.
"""
import argparse
import hashlib
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tests"))

from google.protobuf import json_format
from evm_state.ch import ClickHouse
from evm_state.checkpoint import validate_interval
from evm_state.cursor import load_progress
from evm_state.files import atomic_json
from evm_state.header import encode_rpc_header, verify_header
from evm_state.rpc import RPC
from evm_state.source import verified_source
from native_stream import messages


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--state-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists(): parser.error("output already exists")
    directory = args.state_dir.resolve()
    run = json.loads((directory / "run.json").read_text())
    client, rpc = ClickHouse(args.database), RPC()
    end = load_progress(client, run, directory)["position"]["block"]["number"]
    if int(rpc.call("eth_chainId", []), 16) != 56:
        raise ValueError("expected the BSC RPC")
    if int(rpc.call("eth_getBlockByNumber", ["finalized", False])["number"], 16) < end:
        raise ValueError("native range has not finalized at RPC")
    raw = rpc.call("eth_getBlockByNumber", [hex(end), False])
    header = {"number": int(raw["number"], 16), "hash": raw["hash"].lower(), "state_root": raw["stateRoot"].lower(),
              "parent_hash": raw["parentHash"].lower(), "timestamp": int(raw["timestamp"], 16)}
    encoded = encode_rpc_header(raw)
    verify_header(encoded, header)
    declared = {k: run["identity"][k] for k in ["database", "accounts", "start_block", "module_hash", "final_blocks_only"]}
    count = total = maximum = 0
    digest = hashlib.sha256()
    first_timestamp = None
    with verified_source(client, declared, end_block=end) as checked:
        if checked["state_directory"] != str(directory):
            raise ValueError("measurement directory does not own the native source")
        message = messages(directory / "package.spkg")["BlockState"]
        groups = {f.name: 0 for f in message.DESCRIPTOR.fields if f.message_type}
        expected = validate_interval(client, checked, header)
        for row in client.rows("SELECT * FROM state_blocks FINAL WHERE number >= {start:UInt64} "
                              "AND number <= {end:UInt64} ORDER BY number", {"start": declared["start_block"], "end": end}):
            if first_timestamp is None:
                first_timestamp = int(row["timestamp"])
            value = {}
            for field in message.DESCRIPTOR.fields:
                if not field.message_type:
                    value[field.name] = row[field.name]
                else:
                    names = [f.name for f in field.message_type.fields]
                    columns = [row[field.name + "." + name] for name in names]
                    if len({len(column) for column in columns}) != 1:
                        raise ValueError("malformed native Nested arrays")
                    value[field.name] = [dict(zip(names, values)) for values in zip(*columns)]
                    groups[field.name] += len(value[field.name])
            encoded_output = json_format.ParseDict(value, message()).SerializeToString(deterministic=True)
            size = len(encoded_output)
            digest.update(size.to_bytes(8, "big"))
            digest.update(encoded_output)
            count += 1
            total += size
            maximum = max(maximum, size)
        if count != expected: raise ValueError("native interval changed during measurement")
    result = {"format_version": 1, "database": client.database, "run_id": run["run_id"],
        "module_hash": run["identity"]["module_hash"], "package_sha256": run["identity"]["package_sha256"],
        "accounts": declared["accounts"], "start_block": declared["start_block"], "end_block": end,
        "blocks": count, "logical_protobuf_bytes": total, "mean_protobuf_bytes_per_block": total / count,
        "max_protobuf_bytes_per_block": maximum, "changed_rows": groups, "header": header, "header_rlp": encoded,
        "ordered_output_sha256": digest.hexdigest(),
        "digest_encoding": "ascending blocks; deterministic protobuf; each prefixed by its uint64 big-endian byte length",
        "first_block_timestamp": first_timestamp,
        "mean_block_interval_seconds": (header["timestamp"] - first_timestamp) / (count - 1) if count > 1 else None,
        "header_trust": "provider-finalized-header", "validation": "encoded header, native interval and durable cursor verified",
        "limitations": ["account storage completeness is not established by this update sample",
                        "logical protobuf output reconstructed from deduplicated native rows; excludes framing, retries and billing adjustments",
                        "cache warmth and wall time are separate measurements"]}
    atomic_json(args.output, result)
    print(json.dumps({k: v for k, v in result.items() if k != "header_rlp"}, indent=2))


if __name__ == "__main__":
    main()
