#!/usr/bin/env python3
"""Measure full trie reconstruction from an isolated, checksummed real account.

Consumes the frozen aggregation comparison and matching account fields. Requires
capacity-run, preserves its working database and never publishes a checkpoint.
The reconstructed root is not an account proof or a complete-state acceptance.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import resource
import sys
import time

from evm_state.bootstrap import _digest
from evm_state.capacity import check as capacity_check
from evm_state.ch import ClickHouse
from evm_state.files import atomic_json
from evm_state.proof import VerificationError, storage_root
from evm_state.triedb import StorageSortDB, TrieDB


def peak_rss_bytes():
    value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return int(value if sys.platform == "darwin" else value * 1024)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--fields", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True, help="fresh directory inside capacity roots")
    parser.add_argument("--backend", choices=("incremental", "sorted"), default="incremental")
    parser.add_argument("--reference", type=Path, help="previous reconstruction result to require identical input/root")
    args = parser.parse_args()
    if not os.environ.get("EVM_STATE_CAPACITY_CONFIG"):
        parser.error("run this workload under capacity-run")
    evidence_raw, fields_raw = args.evidence.read_bytes(), args.fields.read_bytes()
    evidence = json.loads(evidence_raw)["successful_comparison"]
    if evidence.get("ordered_state_matches") is not True or len(evidence["accounts"]) != 1:
        parser.error("expected a completed isolated single-account comparison")
    fields = json.loads(fields_raw)
    if set(fields) != set(evidence["accounts"]):
        parser.error("metadata accounts differ from the isolated comparison")
    variant = next(v for v in evidence["variants"] if v["name"] == "default")
    client, out = ClickHouse(evidence["database"]), args.output.resolve()
    capacity_check(client, [out], "trie-workspace-start")
    out.mkdir(parents=True, exist_ok=False)
    measured = _digest(client, variant["generation"], fields, evidence["accounts"])
    if any(variant[key] != value for key, value in measured.items()):
        raise VerificationError("retained generation or account fields differ from the recorded state")
    identity = {"format_version": 1, "status": "unproven-trie-resource-measurement",
        "backend": args.backend,
        "account": evidence["accounts"][0], "header": evidence["target_header"],
        "database": client.database, "generation": variant["generation"],
        "run_id": evidence["run_id"], "module_hash": evidence["module_hash"],
        "package_sha256": evidence["package_sha256"],
        "evidence_sha256": hashlib.sha256(evidence_raw).hexdigest(),
        "fields_sha256": hashlib.sha256(fields_raw).hexdigest(),
        "script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        **measured,
        "qualification": "Resource measurement of a checksummed private account state. "
            "No account proof, independently accepted storage root or ready checkpoint is claimed."}
    reference = None
    if args.reference:
        reference_raw = args.reference.read_bytes()
        reference = json.loads(reference_raw)
        for key in ("account", "header", "database", "generation", "run_id", "module_hash",
                    "package_sha256", "fields_sha256", "nonzero_slots", "state_sha256"):
            if identity[key] != reference[key]:
                raise VerificationError(f"reference reconstruction differs in {key}")
        identity["reference_sha256"] = hashlib.sha256(reference_raw).hexdigest()
    atomic_json(out / "input.json", identity)
    path = out / ("storage.sqlite" if args.backend == "sorted" else "nodes.sqlite")
    database = (StorageSortDB if args.backend == "sorted" else TrieDB)(path)
    started, cpu_started = time.monotonic(), time.process_time()
    digest = hashlib.sha256(json.dumps(fields, sort_keys=True, separators=(",", ":")).encode())
    count, previous = 0, None
    try:
        with (out / "progress.jsonl").open("x") as progress:
            def point(stage):
                value = {"observed_ns": time.time_ns(), "stage": stage, "slots": count,
                    "elapsed_seconds": time.monotonic() - started,
                    "process_cpu_seconds": time.process_time() - cpu_started,
                    "process_peak_rss_bytes": peak_rss_bytes(),
                    "workspace_file_bytes": path.stat().st_size,
                    "workspace_allocated_bytes": path.stat().st_blocks * 512}
                progress.write(json.dumps(value, sort_keys=True) + "\n")
                progress.flush()
                capacity_check(client, [out], stage)
                print(json.dumps({k: value[k] for k in
                    ("slots", "elapsed_seconds", "process_peak_rss_bytes", "workspace_allocated_bytes")}), flush=True)
                return value

            def slots():
                nonlocal count, previous
                for row in client.rows("SELECT address,slot,value FROM bootstrap_storage "
                        "WHERE generation={id:String} ORDER BY address,slot", {"id": variant["generation"]}):
                    key = (row["address"], row["slot"])
                    if row["address"] != identity["account"] or (previous is not None and key <= previous):
                        raise VerificationError("unexpected account or unordered/duplicate storage key")
                    digest.update((row["address"] + row["slot"] + row["value"]).encode())
                    previous = key
                    yield row["slot"], row["value"]
                    count += 1
                    if count % 100000 == 0:
                        point("trie-workspace-progress")

            root, root_count = storage_root(slots(), database)
            seconds = time.monotonic() - started
            cpu_seconds = time.process_time() - cpu_started
            if count != root_count or count != measured["nonzero_slots"] or digest.hexdigest() != measured["state_sha256"]:
                raise VerificationError("trie input count/checksum differs from the frozen generation")
            if reference and reference["reconstructed_storage_root"] != "0x" + root.hex():
                raise VerificationError("reconstructed storage root differs from the reference")
            # Measure before closing the disposable SQLite transaction: its
            # file and cache represent actual verification workspace here.
            final = point("trie-workspace-finished")
            result = {**identity, "reconstructed_storage_root": "0x" + root.hex(),
                "trie_seconds_including_progress_guards": seconds,
                "process_cpu_seconds_during_trie": cpu_seconds,
                "whole_process_peak_rss_bytes": peak_rss_bytes(),
                "workspace_entries": len(database),
                "workspace_entry_kind": "hashed storage slots" if args.backend == "sorted" else "trie nodes",
                "workspace_file_bytes_before_close": final["workspace_file_bytes"],
                "workspace_allocated_bytes_before_close": final["workspace_allocated_bytes"],
                "streamed_input_checksum_matches": True,
                "reference_root_matches": True if reference else None,
                "workspace_note": "Disposable SQLite workspace is not a portable trie export. "
                    + ("Sorted slots are committed before hashing. " if args.backend == "sorted" else "Close may roll back trie nodes. ") +
                    "RSS covers this Python process, not the ClickHouse server or other processes."}
            atomic_json(out / "result.json", result)
    finally:
        database.close()


if __name__ == "__main__":
    main()
