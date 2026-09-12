#!/usr/bin/env python3
"""Qualify a real isolated cohort cutover, interrupted publication and continuation.

Starts from an existing published export source, imports into a fresh destination,
and uses recent empty-storage-root accounts as a bounded new cohort. It does not
claim that nonempty accounts can bootstrap from an arbitrary recent block.
Run under capacity-run with both the source control root and this output covered.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

from evm_state.ch import ClickHouse, identifier
from evm_state.checkpoint import build, manifest
from evm_state.export import export_checkpoint
from evm_state.files import atomic_json
from evm_state.importer import import_checkpoint
from evm_state.ingest import ingest
from evm_state.proof import EMPTY_STORAGE_ROOT, verify_account
from evm_state.reader import page, pin, unpin
from evm_state.rpc import RPC


def interrupted_child(args):
    target = ClickHouse(args.prefix + "_checkpoints")
    insert = target.insert
    def fail_after_accounts(table, rows, **kwargs):
        insert(table, rows, **kwargs)
        if table == "checkpoint_accounts":
            # Hard failure after acknowledged account data but before the ready
            # manifest. Confined to this qualification child, not the DB server.
            os.kill(os.getpid(), signal.SIGKILL)
    target.insert = fail_after_accounts
    root = args.root.resolve()
    build(target, json.loads((root / "cutover-proofs.json").read_text()),
          json.loads((root / "sources.json").read_text()),
          json.loads((root / "base.json").read_text())["snapshot_id"], work_dir=root / "trie-work")
    raise RuntimeError("publication fault was not exercised")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prefix", required=True)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--package", type=Path, required=True)
    parser.add_argument("--source-database")
    parser.add_argument("--source-snapshot")
    parser.add_argument("--source-control", type=Path)
    parser.add_argument("--new-accounts")
    parser.add_argument("--resume", action="store_true", help="resume a pre-cutover run after a capacity stop")
    parser.add_argument("--native-dsn-template", help="DSN containing {database}; prefer NATIVE_DSN_TEMPLATE environment")
    parser.add_argument("--interrupted-child", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    identifier(args.prefix)
    root = args.root.resolve()
    os.environ["EVM_STATE_HOME"] = str(root / "control")
    if args.interrupted_child:
        return interrupted_child(args)
    if not os.environ.get("EVM_STATE_CAPACITY_CONFIG"):
        parser.error("run under evm-state capacity-run")
    if not all([args.source_database, args.source_snapshot, args.source_control, args.new_accounts]):
        parser.error("source database/snapshot/control and new accounts are required")
    dsn_template = args.native_dsn_template or os.environ["NATIVE_DSN_TEMPLATE"]
    if "{database}" not in dsn_template:
        parser.error("native DSN template must contain {database}")
    names = [args.prefix + suffix for suffix in ["_checkpoints", "_old", "_new", "_combined"]]
    admin = ClickHouse("default")
    if args.resume:
        if not (root / "base.json").is_file() or (root / "cutover.json").exists():
            raise ValueError("resume requires an imported base and no completed cutover")
    else:
        for name in names:
            if int(admin.one("SELECT count() AS n FROM system.databases WHERE name={db:String}", {"db": name})["n"]):
                raise ValueError("qualification database already exists; choose a fresh prefix")
        if (root / "base.json").exists():
            raise ValueError("qualification root already contains a checkpoint")
    target = ClickHouse(names[0])
    rpc = RPC()
    phases = json.loads((root / "phases.json").read_text()) if args.resume else []
    def timed(label, operation):
        before = time.monotonic()
        value = operation()
        phases.append({"phase": label, "seconds": time.monotonic() - before})
        atomic_json(root / "phases.json", phases, overwrite=True)
        return value
    source = ClickHouse(args.source_database)
    os.environ["EVM_STATE_HOME"] = str(args.source_control.resolve())
    original = manifest(source, args.source_snapshot)
    if not args.resume:
        timed("export-existing-checkpoint", lambda: export_checkpoint(source, args.source_snapshot,
            root / "export", work_dir=root / "trie-work"))
    os.environ["EVM_STATE_HOME"] = str(root / "control")
    if args.resume:
        saved = json.loads((root / "base.json").read_text())
        base = manifest(target, saved["snapshot_id"])
        if base["state_sha256"] != original["state_sha256"] or base["header"] != original["header"]:
            raise ValueError("resume base differs from the original source")
    else:
        base = timed("restore-isolated-destination", lambda: import_checkpoint(target, root / "export",
            expected_hash=original["header"]["hash"], work_dir=root / "trie-work"))
        atomic_json(root / "base.json", {key: value for key, value in base.items() if key != "proof_bundle"})
    old_accounts = sorted(base["accounts"])
    new_accounts = sorted(set(args.new_accounts.split(",")))
    if set(old_accounts) & set(new_accounts):
        raise ValueError("new cohort overlaps the published base")
    accounts = sorted([*old_accounts, *new_accounts])
    if args.resume:
        proofs = json.loads((root / "cutover-proofs.json").read_text())
        if sorted(proofs["accounts"]) != accounts:
            raise ValueError("resume account filter differs from captured cutover")
    else:
        proofs = rpc.capture(accounts)
        atomic_json(root / "cutover-proofs.json", proofs)
    for account in new_accounts:
        if verify_account(proofs["header"]["state_root"], account, proofs["accounts"][account]["proof"]).storage_root != EMPTY_STORAGE_ROOT:
            raise ValueError("new account has nonempty storage and requires complete historical bootstrap")
    end = proofs["header"]["number"]
    def replay(suffix, selected, start, stop):
        database = args.prefix + "_" + suffix
        return ingest(ClickHouse(database), args.package, "bsc.substreams.pinax.network:443", selected,
            start, root / suffix, dsn_template.format(database=database), stop + 1,
            checkpoint_database=target.database, decode_batch_size=32, spool_max_idle_ms=1000,
            prometheus_addr="127.0.0.1:0")
    old = timed("existing-cohort-catch-up", lambda: replay("old", old_accounts, base["header"]["number"] + 1, end))
    new = timed("new-empty-storage-cohort-bootstrap", lambda: replay("new", new_accounts, end - 500, end))
    declared = [old["source"], new["source"]]
    atomic_json(root / "sources.json", declared, overwrite=args.resume)
    pinned = pin(target, base["snapshot_id"], "old-reader-during-interrupted-cutover")
    before_pages = [page(target, pinned["pin_id"], account) for account in old_accounts]
    before_manifests = int(target.one("SELECT count() AS n FROM checkpoints FINAL")["n"])
    child = [sys.executable, str(Path(__file__).resolve()), "--prefix", args.prefix, "--root", str(root),
             "--package", str(args.package), "--interrupted-child"]
    failed = timed("publication-child-sigkill", lambda: subprocess.run(child, timeout=300))
    if failed.returncode != -signal.SIGKILL:
        raise ValueError("publication child did not reach its intended failure point")
    if int(target.one("SELECT count() AS n FROM checkpoints FINAL")["n"]) != before_manifests:
        raise ValueError("failed publication exposed a new ready manifest")
    after_pages = [page(target, pinned["pin_id"], account) for account in old_accounts]
    if before_pages != after_pages:
        raise ValueError("old pinned account changed during failed cutover")
    cutover = timed("retry-and-publish-cutover", lambda: build(target, proofs, declared, base["snapshot_id"], work_dir=root / "trie-work"))
    atomic_json(root / "cutover.json", {key: value for key, value in cutover.items() if key != "proof_bundle"})
    if [page(target, pinned["pin_id"], account) for account in old_accounts] != before_pages:
        raise ValueError("old reader changed after new-cohort publication")
    next_proofs = rpc.capture(accounts)
    atomic_json(root / "continuation-proofs.json", next_proofs)
    combined = timed("new-combined-filter-continuation", lambda: replay("combined", accounts, end + 1, next_proofs["header"]["number"]))
    final = timed("verify-combined-continuation", lambda: build(target, next_proofs, [combined["source"]],
        cutover["snapshot_id"], work_dir=root / "trie-work"))
    unpin(target, pinned["pin_id"])
    result = {"format_version": 1, "databases": names, "old_accounts": old_accounts, "new_accounts": new_accounts,
        "base": {key: base[key] for key in ["snapshot_id", "header", "nonzero_slots", "state_sha256"]},
        "cutover": {key: cutover[key] for key in ["snapshot_id", "header", "nonzero_slots", "state_sha256", "sources"]},
        "continuation": {key: final[key] for key in ["snapshot_id", "header", "nonzero_slots", "state_sha256", "sources"]},
        "interrupted_publication_exit_code": failed.returncode, "old_reader_unchanged": True, "phases": phases,
        "native_sources": declared + [combined["source"]],
        "export_manifest_sha256": hashlib.sha256((root / "export/manifest.json").read_bytes()).hexdigest(),
        "limitations": ["public three-account sample; customer account list unavailable",
                        "two new accounts have proven empty storage, so recent enumeration can prove completeness",
                        "this does not qualify arbitrary recent bootstrap of a nonempty hot contract",
                        "process SIGKILL before ready manifest; no physical host power-loss emulation",
                        "provider-finalized encoded headers, not independent BSC consensus verification"]}
    atomic_json(root / "result.json", result)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
