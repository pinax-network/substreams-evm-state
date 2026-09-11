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
from .ingest import ingest, prepare


def main(argv=None):
    parser = argparse.ArgumentParser(prog="evm-state")
    parser.add_argument("--database", default="evm_state")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("init", help="create checkpoint tables (native block tables use substreams sink clickhouse setup)")
    for name, help_text in [("prepare", "bind a native sink to an isolated database and durable state directory"),
                            ("ingest", "run or resume the guarded finalized native sink")]:
        native = commands.add_parser(name, help=help_text)
        native.add_argument("--package", type=Path, required=True)
        native.add_argument("--endpoint", default="bsc.substreams.pinax.network:443")
        native.add_argument("--accounts", required=True)
        native.add_argument("--start-block", type=int, required=True)
        native.add_argument("--state-dir", type=Path, required=True)
        if name == "ingest":
            native.add_argument("--stop-block", type=int)
            native.add_argument("--max-retries", type=int, default=3)
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
    args = parser.parse_args(argv)
    try:
        client = ClickHouse(args.database)
        if args.command == "init":
            setup(client)
            result = {"database": client.database, "checkpoint_schema": "ready"}
        elif args.command in {"prepare", "ingest"}:
            dsn = os.environ.get("SUBSTREAMS_SINK_DSN")
            if not dsn:
                raise ValueError("set SUBSTREAMS_SINK_DSN to the native ClickHouse connection string")
            values = (client, args.package, args.endpoint, args.accounts, args.start_block, args.state_dir, dsn)
            result = prepare(*values) if args.command == "prepare" else ingest(*values, args.stop_block, args.max_retries)
        elif args.command == "capture-proofs":
            if args.output.exists():
                raise ValueError("proof output already exists; choose a new capture file")
            result = RPC().capture(canonical_accounts(args.accounts), args.block, args.expected_hash)
            atomic_json(args.output, result)
            result = {"output": str(args.output), "header": result["header"], "header_trust": result["header_trust"]}
        elif args.command == "checkpoint":
            if args.output and args.output.exists():
                raise ValueError("checkpoint output already exists; choose a new output file")
            result = build(client, json.loads(args.proofs.read_text()), json.loads(args.sources.read_text()),
                           args.base, args.budget_bytes, args.work_dir)
            if args.output:
                atomic_json(args.output, result)
            result = {k: v for k, v in result.items() if k != "proof_bundle"}
        else:
            result = read_account(client, args.snapshot_id, args.address) if args.address else manifest(client, args.snapshot_id)
        print(json.dumps(result, sort_keys=True, indent=2))
    except (ValueError, RuntimeError, OSError, KeyError) as error:
        parser.exit(1, f"evm-state: {error}\n")


if __name__ == "__main__":
    main()
