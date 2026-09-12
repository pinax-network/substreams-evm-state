"""Explicit commands for native ingestion, proof capture and checkpoint publication."""
import argparse
import json
import os
from pathlib import Path
import sys

from .ch import ClickHouse
from .checkpoint import build, canonical_accounts, manifest, read_account, setup
from .rpc import RPC
from .files import atomic_json
from .ingest import ingest, prepare, recover_cursor
from .export import export_checkpoint, verify_export
from .reader import page, pin, unpin, list_pins
from .retention import plan as retention_plan, prune
from .importer import import_checkpoint
from .history import cleanup as cleanup_history
from .bootstrap import compact as compact_bootstrap, replay as bootstrap_replay
from .capacity import Meter, supervise, check as capacity_check


def main(argv=None):
    parser = argparse.ArgumentParser(prog="evm-state")
    parser.add_argument("--database", default="evm_state")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("init", help="create checkpoint tables (native block tables use substreams sink clickhouse setup)")
    for name, help_text in [("prepare", "bind a native sink to an isolated database and durable state directory"),
                            ("ingest", "run or resume the guarded finalized native sink"),
                            ("bootstrap-replay", "replay new accounts in chunks with private state compaction"),
                            ("recover-cursor", "restore a damaged native cursor from verified durable progress")]:
        native = commands.add_parser(name, help=help_text)
        native.add_argument("--package", type=Path, required=True)
        native.add_argument("--endpoint", default="bsc.substreams.pinax.network:443")
        native.add_argument("--accounts", required=True)
        native.add_argument("--start-block", type=int, required=True)
        native.add_argument("--state-dir", type=Path, required=True)
        native.add_argument("--checkpoint-database", help="sole checkpoint destination for this source (default: source database)")
        if name in {"ingest", "bootstrap-replay"}:
            native.add_argument("--stop-block", type=int, required=name == "bootstrap-replay")
            native.add_argument("--max-retries", type=int, default=3)
            native.add_argument("--decode-batch-size", type=int, default=32 if name == "bootstrap-replay" else 1,
                                help="blocks decoded together (ingest: 1 for finalized follow; bootstrap: 32)")
            native.add_argument("--spool-max-idle-ms", type=int, default=1000 if name == "bootstrap-replay" else 100,
                                help="seal idle spool after this many milliseconds (ingest: 100; bootstrap: 1000)")
        if name == "ingest":
            native.add_argument("--prometheus-addr", help="native metrics listener; use a distinct port for concurrent cohorts")
        if name == "bootstrap-replay":
            native.add_argument("--chunk-blocks", type=int, default=100000)
            native.add_argument("--budget-bytes", type=int, default=100_000_000_000)
    compact = commands.add_parser("compact-bootstrap", help="compact initial replay history without publishing ready state")
    compact.add_argument("--state-dir", type=Path, required=True)
    compact.add_argument("--end-block", type=int, help="inclusive end (default: durable cursor)")
    compact.add_argument("--budget-bytes", type=int, default=100_000_000_000)
    for name in ["capacity-report", "capacity-run"]:
        capacity = commands.add_parser(name, help="measure data directories and enforce sampled operating headroom")
        capacity.add_argument("--config", type=Path, required=True)
        if name == "capacity-run":
            capacity.add_argument("--output", type=Path, required=True)
            capacity.add_argument("--interval", type=float, default=1.0)
            capacity.add_argument("child_command", nargs=argparse.REMAINDER)
    capture = commands.add_parser("capture-proofs", help="capture a finalized header, account proofs and code before replay")
    capture.add_argument("--accounts", required=True)
    capture.add_argument("--block", default="finalized")
    capture.add_argument("--expected-hash")
    capture.add_argument("--output", type=Path, required=True)
    checkpoint = commands.add_parser("checkpoint", help="verify isolated source state and publish an immutable checkpoint")
    checkpoint.add_argument("--proofs", type=Path, required=True)
    checkpoint.add_argument("--sources", type=Path, required=True, help="JSON array of database/start_block/accounts source cohorts")
    checkpoint.add_argument("--base")
    checkpoint.add_argument("--budget-bytes", type=int, default=100_000_000_000)
    checkpoint.add_argument("--work-dir", type=Path, default=Path("localdata/verification"))
    checkpoint.add_argument("--output", type=Path)
    show = commands.add_parser("show", help="show a published manifest or one account at that immutable checkpoint")
    show.add_argument("snapshot_id")
    show.add_argument("--address")
    export = commands.add_parser("export", help="write a complete paginated checkpoint with offline proofs")
    export.add_argument("snapshot_id")
    export.add_argument("--output", type=Path, required=True)
    export.add_argument("--page-size", type=int, default=10000)
    export.add_argument("--work-dir", type=Path)
    verify = commands.add_parser("verify-export", help="verify exported files without database or RPC access")
    verify.add_argument("directory", type=Path)
    verify.add_argument("--expected-hash")
    verify.add_argument("--work-dir", type=Path)
    restore = commands.add_parser("import-export", help="verify and restore a portable checkpoint into a new generation")
    restore.add_argument("directory", type=Path)
    restore.add_argument("--expected-hash")
    restore.add_argument("--work-dir", type=Path)
    restore.add_argument("--budget-bytes", type=int, default=100_000_000_000)
    pinned = commands.add_parser("pin", help="protect a checkpoint while a consumer reads it")
    pinned.add_argument("snapshot_id")
    pinned.add_argument("--purpose", default="reader")
    commands.add_parser("pins", help="list persistent checkpoint pins, including abandoned readers")
    released = commands.add_parser("unpin", help="release a consumer's checkpoint retention pin")
    released.add_argument("pin_id")
    storage_page = commands.add_parser("page", help="page complete storage through an active checkpoint pin")
    storage_page.add_argument("pin_id")
    storage_page.add_argument("--address", required=True)
    storage_page.add_argument("--cursor")
    storage_page.add_argument("--limit", type=int, default=1000)
    for name, help_text in [("retention-plan", "show which checkpoint generations can be removed"),
                            ("prune-checkpoints", "remove old and failed checkpoints while preserving readers and latest account state")]:
        retention = commands.add_parser(name, help=help_text)
        retention.add_argument("--keep-latest", type=int, default=2)
    for name in ["source-retention-plan", "prune-source"]:
        native_retention = commands.add_parser(name, help="plan or remove checkpointed native history partitions")
        native_retention.add_argument("snapshot_id")
        native_retention.add_argument("--state-dir", type=Path, required=True)
        native_retention.add_argument("--keep-blocks", type=int, default=10000)
    args = parser.parse_args(argv)
    try:
        client = ClickHouse(args.database)
        if args.command == "init":
            setup(client)
            result = {"database": client.database, "checkpoint_schema": "ready"}
        elif args.command in {"prepare", "ingest", "recover-cursor", "bootstrap-replay"}:
            dsn = os.environ.get("SUBSTREAMS_SINK_DSN")
            if not dsn:
                raise ValueError("set SUBSTREAMS_SINK_DSN to the native ClickHouse connection string")
            values = (client, args.package, args.endpoint, args.accounts, args.start_block, args.state_dir, dsn)
            if args.command == "bootstrap-replay":
                result = bootstrap_replay(*values, args.stop_block, args.chunk_blocks, args.budget_bytes,
                                          args.max_retries, args.checkpoint_database, args.decode_batch_size,
                                          args.spool_max_idle_ms)
            elif args.command == "ingest":
                result = ingest(*values, args.stop_block, args.max_retries, args.checkpoint_database,
                                args.decode_batch_size, args.spool_max_idle_ms, args.prometheus_addr)
            else:
                result = {"prepare": prepare, "recover-cursor": recover_cursor}[args.command](
                    *values, checkpoint_database=args.checkpoint_database)
        elif args.command == "compact-bootstrap":
            result = compact_bootstrap(client, args.state_dir, args.end_block, args.budget_bytes)
        elif args.command in {"capacity-report", "capacity-run"}:
            meter = Meter(client, json.loads(args.config.read_text()))
            if args.command == "capacity-report":
                result = meter.sample()
            else:
                command = args.child_command[1:] if args.child_command[:1] == ["--"] else args.child_command
                result = supervise(meter, command, args.output, args.interval)
                if result["status"] != "completed":
                    print(json.dumps(result, sort_keys=True, indent=2))
                    parser.exit(1, "capacity-run stopped; inspect the capacity report and retained run state\n")
        elif args.command == "capture-proofs":
            if args.output.exists():
                raise ValueError("proof output already exists; choose a new capture file")
            capacity_check(client, [args.output.parent], "proof-capture")
            result = RPC().capture(canonical_accounts(args.accounts), args.block, args.expected_hash)
            atomic_json(args.output, result)
            result = {"output": str(args.output), "header": result["header"], "header_trust": result["header_trust"]}
        elif args.command == "checkpoint":
            if args.output and args.output.exists():
                raise ValueError("checkpoint output already exists; choose a new output file")
            capacity_check(client, [args.proofs, args.sources, *([args.output.parent] if args.output else [])],
                           "checkpoint-files")
            result = build(client, json.loads(args.proofs.read_text()), json.loads(args.sources.read_text()),
                           args.base, args.budget_bytes, args.work_dir)
            if args.output:
                atomic_json(args.output, result)
            result = {k: v for k, v in result.items() if k != "proof_bundle"}
        elif args.command == "export":
            result = export_checkpoint(client, args.snapshot_id, args.output, args.page_size, args.work_dir)
        elif args.command == "verify-export":
            result = verify_export(args.directory, args.expected_hash, args.work_dir)
        elif args.command == "import-export":
            restored = import_checkpoint(client, args.directory, args.expected_hash, args.work_dir, args.budget_bytes)
            result = {k: v for k, v in restored.items() if k != "proof_bundle"}
        elif args.command == "pin":
            result = pin(client, args.snapshot_id, args.purpose)
        elif args.command == "pins":
            result = list_pins(client)
        elif args.command == "unpin":
            result = unpin(client, args.pin_id)
        elif args.command == "page":
            result = page(client, args.pin_id, args.address, args.cursor, args.limit)
        elif args.command in {"retention-plan", "prune-checkpoints"}:
            result = (retention_plan if args.command == "retention-plan" else prune)(client, args.keep_latest)
        elif args.command in {"source-retention-plan", "prune-source"}:
            result = cleanup_history(client, args.state_dir, args.snapshot_id, args.keep_blocks, args.command == "prune-source")
        else:
            result = read_account(client, args.snapshot_id, args.address) if args.address else manifest(client, args.snapshot_id)
        print(json.dumps(result, sort_keys=True, indent=2))
    except (ValueError, RuntimeError, OSError, KeyError, EOFError) as error:
        parser.exit(1, f"evm-state: {error}\n")


if __name__ == "__main__":
    main()
