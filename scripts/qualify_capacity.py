#!/usr/bin/env python3
"""Reproducible synthetic state/retention stress workload for capacity-run.

Uses native-generated ClickHouse schemas and genuine synthetic trie proofs. It
does not emulate BSC activity, establish a provider throughput SLA, or substitute
for the customer's account set. Requires the repository's test dependencies.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tests"))

from conftest import native_setup
from evm_state.ch import ClickHouse, identifier
from evm_state.checkpoint import build
from evm_state.export import export_checkpoint
from evm_state.importer import import_checkpoint
from evm_state.reader import page, pin, unpin
from evm_state.retention import prune
from export_fixtures import complete_bundle
from state_fixtures import block, insert_blocks, source, state


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prefix", required=True, help="new database prefix; refuses any existing database")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--accounts", type=int, default=64)
    parser.add_argument("--hot-slots", type=int, default=100000)
    parser.add_argument("--quiet-slots", type=int, default=64)
    parser.add_argument("--merge-only-mib", type=int, default=0,
                        help="run only a separate eight-part incompressible merge workload of this size")
    args = parser.parse_args()
    if not 1 <= args.accounts <= 64 or args.hot_slots < 2 or args.quiet_slots < 0:
        parser.error("require 1..64 accounts, at least two hot slots and nonnegative quiet slots")
    if not 0 <= args.merge_only_mib <= 1024:
        parser.error("merge-only size must be between 0 and 1024 MiB")
    prefix = identifier(args.prefix)
    out = args.output.resolve()
    out.mkdir(parents=True, exist_ok=False)
    os.environ["EVM_STATE_HOME"] = str(out / "control")
    source_db, target, restored = [ClickHouse(prefix + suffix) for suffix in ["_source", "_checkpoints", "_restored"]]
    admin = ClickHouse("default")
    names = [source_db.database, target.database, restored.database]
    existing = {r["name"] for r in admin.rows("SELECT name FROM system.databases")}
    if set(names) & existing:
        parser.error("qualification databases already exist; choose a fresh prefix")
    if args.merge_only_mib:
        admin.execute(f"CREATE DATABASE {target.database}")
        table = target.database + ".merge_stress"
        target.execute("CREATE TABLE merge_stress (key UInt64,value String) ENGINE=MergeTree ORDER BY key")
        target.execute("SYSTEM STOP MERGES " + table)
        rows = max(1, args.merge_only_mib * 1024 * 1024 // 4096 // 8)
        started = time.monotonic()
        try:
            for part in range(8):
                target.execute(f"INSERT INTO merge_stress SELECT number*8+{part},randomString(4096) FROM numbers({rows})")
            before = target.disk_usage()
            print(json.dumps({"phase": "merge-parts-staged", "rows": rows * 8, "parts_bytes": before}), flush=True)
        finally:
            target.execute("SYSTEM START MERGES " + table)
        merging = time.monotonic()
        target.execute("OPTIMIZE TABLE merge_stress FINAL")
        result = {"format_version": 1, "workload": "synthetic incompressible eight-part merge",
            "database": target.database, "rows": rows * 8, "payload_bytes_per_row": 4096,
            "parts_bytes_before": before, "parts_bytes_after": target.disk_usage(),
            "merge_seconds": time.monotonic() - merging, "total_seconds": time.monotonic() - started,
            "limitations": ["auxiliary merge stress table, not account state or a customer footprint",
                            "random payload bytes; row count and payload size are reproducible"]}
        (out / "result.json").write_text(json.dumps(result, indent=2) + "\n")
        print(json.dumps(result), flush=True)
        return
    native_setup(source_db.database, out / "native-schema")
    for client in [target, restored]:
        admin.execute(f"CREATE DATABASE {client.database}")
    accounts = ["0x" + f"{i + 1:040x}" for i in range(args.accounts)]
    def value(slot):
        # Deterministic, poorly compressible values make the fixture less
        # optimistic than rows filled with small repeated integers.
        return int.from_bytes(hashlib.sha256(str(slot).encode()).digest(), "big") or 1
    values = {account: state({slot: value(slot) for slot in range(
        args.hot_slots if i == 0 else args.quiet_slots)}) for i, account in enumerate(accounts)}
    phases = []
    def timed(name, operation):
        start = time.monotonic()
        result = operation()
        phase = {"phase": name, "seconds": time.monotonic() - start,
            "source_parts_bytes": source_db.disk_usage(), "checkpoint_parts_bytes": target.disk_usage(),
            "restored_parts_bytes": restored.disk_usage()}
        phases.append(phase)
        (out / "phases.json").write_text(json.dumps(phases, indent=2))
        print(json.dumps(phase), flush=True)
        return result
    def projection(number, bundle, changes):
        row = block(number, bundle, storage=changes)
        row.update(hash=bundle["header"]["hash"], parent_hash=bundle["header"]["parent_hash"])
        insert_blocks(source_db, [row])
    bundle = timed("construct-synthetic-proofs", lambda: complete_bundle(100, values))
    projection(100, bundle, {(account, slot): v for account, data in values.items() for slot, v in data["slots"].items()})
    declared = source(source_db, 100, accounts, target=target)
    first = timed("checkpoint-initial", lambda: build(target, bundle, [declared], work_dir=out / "trie-work"))
    exported = timed("export-initial", lambda: export_checkpoint(target, first["snapshot_id"], out / "export",
        page_size=10000, work_dir=out / "trie-work"))
    imported = timed("restore-and-reverify", lambda: import_checkpoint(restored, out / "export",
        expected_hash=first["header"]["hash"], work_dir=out / "trie-work"))
    assert imported["state_sha256"] == first["state_sha256"]
    protected = pin(target, first["snapshot_id"], "capacity-qualification-reader")
    changes = {}
    for slot in range(args.hot_slots // 2):
        changes[(accounts[0], slot)] = 0
        del values[accounts[0]]["slots"][slot]
        new = args.hot_slots + slot
        changes[(accounts[0], new)] = value(new)
        values[accounts[0]]["slots"][new] = value(new)
    next_bundle = timed("construct-updated-proofs", lambda: complete_bundle(101, values, first["header"]["hash"]))
    projection(101, next_bundle, changes)
    second = timed("checkpoint-slot-churn", lambda: build(target, next_bundle,
        [source(source_db, 101, accounts, target=target)], first["snapshot_id"], work_dir=out / "trie-work"))
    assert first["nonzero_slots"] == second["nonzero_slots"]
    retained = timed("prune-with-pinned-reader", lambda: prune(target, keep_latest=1))
    assert first["snapshot_id"] not in retained["remove"]
    assert int(page(target, protected["pin_id"], accounts[0], limit=1)["storage"][0]["slot"], 16) == 0
    unpin(target, protected["pin_id"])
    reclaimed = timed("prune-after-reader-release", lambda: prune(target, keep_latest=1))
    assert first["snapshot_id"] in reclaimed["remove"]
    result = {"format_version": 1, "workload": "synthetic native-schema state and retention stress fixture",
        "account_count": args.accounts, "hot_account_slots": args.hot_slots, "quiet_account_slots": args.quiet_slots,
        "total_nonzero_slots": first["nonzero_slots"], "cleared_and_replaced_slots": args.hot_slots // 2,
        "initial_checksum": first["state_sha256"], "restored_checksum": imported["state_sha256"],
        "updated_checksum": second["state_sha256"], "export_bytes": exported["bytes"],
        "databases": names, "phases": phases,
        "limitations": ["synthetic accounts and headers, not BSC or customer data",
                        "no server backprocessing or native transport throughput measurement",
                        "capacity summary reports sampled peaks plus publication/trie guard samples"]}
    (out / "result.json").write_text(json.dumps(result, indent=2) + "\n")


if __name__ == "__main__":
    main()
