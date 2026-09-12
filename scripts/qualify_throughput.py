#!/usr/bin/env python3
"""Run a bounded native ingest and record RPC finality lag alongside sink logs.

Run under evm-state capacity-run. Use a fresh database/directory each time, and
repeat the exact frozen package, filter and historical interval to measure cache
reuse. This harness records observations; it does not infer cache state from time.
Provider requests are confined to the configured BSC RPC and Substreams endpoint.
"""
import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import signal
import threading
import time

from evm_state.ch import ClickHouse
from evm_state.checkpoint import canonical_accounts
from evm_state.cursor import load_progress
from evm_state.files import atomic_json
from evm_state.ingest import ingest
from evm_state.rpc import RPC


def distribution(values):
    values = sorted(values)
    if not values:
        return None
    return {"samples": len(values), "min": values[0], "p50": values[math.ceil(len(values) * .5) - 1],
            "p95": values[math.ceil(len(values) * .95) - 1], "max": values[-1]}


def sink_events(path):
    """Extract structured observations without carrying arbitrary log text forward."""
    sessions, progress, settings = [], [], []
    for line in path.read_text().splitlines():
        if " {" not in line:
            continue
        prefix, payload = line.split(" {", 1)
        if not any(label in prefix for label in ["session initialized with remote endpoint", "substreams stream stats",
                                                  "Relational Mappings Mode sink settings"]):
            continue
        value = json.loads("{" + payload)
        stamp = prefix.split()[0]
        if "session initialized with remote endpoint" in prefix:
            sessions.append({"timestamp": stamp, **{key: value[key] for key in
                ["max_parallel_workers", "linear_handoff_block", "resolved_start_block", "trace_id"] if key in value}})
        elif "Relational Mappings Mode sink settings" in prefix:
            settings.append({key: value[key] for key in ["decode_workers", "decode_batch_size", "spool_max_idle",
                "spool_max_size", "db_write_target_duration", "db_write_max_size"] if key in value})
        else:
            progress.append({"timestamp": stamp, **{key: value[key] for key in
                ["progress_running_jobs", "progress_total_processed_blocks", "is_live", "last_block",
                 "data_msg_rate", "undo_msg_rate"] if key in value}})
    return {"sessions": sessions, "progress": progress, "native_settings": settings,
            "max_observed_running_jobs": max((sum(value.get("progress_running_jobs", {}).values())
                                               for value in progress if "progress_running_jobs" in value), default=None),
            "last_reported_processed_blocks": next((value["progress_total_processed_blocks"] for value in
                reversed(progress) if "progress_total_processed_blocks" in value), None)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--package", type=Path, required=True)
    parser.add_argument("--accounts", required=True)
    parser.add_argument("--endpoint", default="bsc.substreams.pinax.network:443")
    parser.add_argument("--start-block", type=int)
    parser.add_argument("--stop-block", type=int)
    parser.add_argument("--live-blocks", type=int, help="start 100 blocks behind finalized, stop N blocks ahead")
    parser.add_argument("--interval", type=float, default=5)
    parser.add_argument("--timeout", type=int, default=2400)
    parser.add_argument("--decode-batch-size", type=int, default=1)
    parser.add_argument("--spool-max-idle-ms", type=int, default=100)
    args = parser.parse_args()
    if not os.environ.get("EVM_STATE_CAPACITY_CONFIG"):
        parser.error("run this workload under evm-state capacity-run")
    if not math.isfinite(args.interval) or args.interval < 1 or args.timeout < 1:
        parser.error("interval must be finite and >= 1 second; timeout must be positive")
    root = args.root.resolve()
    root.mkdir(parents=True, exist_ok=True)
    if (root / "native").exists() or (root / "workload.json").exists():
        parser.error("use a fresh native run directory")
    client, rpc = ClickHouse(args.database), RPC()
    if int(rpc.call("eth_chainId", []), 16) != 56:
        raise ValueError("expected the BSC RPC")
    initial = rpc.call("eth_getBlockByNumber", ["finalized", False])
    initial_number = int(initial["number"], 16)
    if args.live_blocks is not None:
        if args.start_block is not None or args.stop_block is not None or not 1 <= args.live_blocks <= 10000:
            parser.error("live blocks must be 1..10000 and cannot be combined with an explicit range")
        start, stop = initial_number - 100, initial_number + args.live_blocks + 1
    else:
        start, stop = args.start_block, args.stop_block
        if start is None or stop is None or not 0 <= start < stop <= initial_number + 1 or stop - start > 100000:
            parser.error("provide an already-finalized range of at most 100000 blocks")
    accounts = canonical_accounts(args.accounts)
    workload = {"format_version": 1, "database": args.database, "accounts": accounts,
        "start_block": start, "stop_block_exclusive": stop, "live_blocks_requested": args.live_blocks,
        "endpoint": args.endpoint, "requested_parallel_workers": None,
        "worker_policy": "provider default; observed session limit is recorded in native log",
        "package_sha256": hashlib.sha256(args.package.read_bytes()).hexdigest(),
        "initial_finalized": {"number": initial_number, "hash": initial["hash"]},
        "sample_interval_seconds": args.interval, "timeout_seconds": args.timeout}
    workload.update(decode_batch_size=args.decode_batch_size, spool_max_idle_ms=args.spool_max_idle_ms)
    workload["harness_sha256"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    atomic_json(root / "workload.json", workload)
    done, samples = threading.Event(), []

    def sample():
        while not done.is_set():
            tick = time.monotonic()
            observation = {"started_at_unix_ns": time.time_ns()}
            try:
                block = rpc.call("eth_getBlockByNumber", ["finalized", False])
                observation["rpc_finalized_block"] = int(block["number"], 16)
                observation["rpc_finished_at_unix_ns"] = time.time_ns()
                durable = root / "native/durable_progress.json"
                if durable.exists():
                    # Atomic checked backup from the writer. Full source/cursor
                    # validation is repeated after it stops; never print tokens.
                    position = json.loads(durable.read_text())["position"]["block"]
                    observation["durable_block"] = position["number"]
                    observation["finalized_lag_blocks"] = observation["rpc_finalized_block"] - position["number"]
                    row = client.one("SELECT timestamp FROM state_blocks FINAL WHERE number={n:UInt64} "
                                     "AND hash={hash:String}", {"n": position["number"], "hash": position["hash"]})
                    observation["durable_block_age_seconds"] = time.time() - int(row["timestamp"])
            except Exception as error:
                # Keep a failed measurement visible, without credential-bearing
                # URLs/third-party error bodies. It is not a zero-lag sample.
                observation["error_type"] = type(error).__name__
            observation["sample_duration_seconds"] = time.monotonic() - tick
            observation["finished_at_unix_ns"] = time.time_ns()
            samples.append(observation)
            with (root / "lag-samples.jsonl").open("a") as handle:
                handle.write(json.dumps(observation, sort_keys=True) + "\n")
                handle.flush()
                os.fsync(handle.fileno())
            done.wait(max(0, args.interval - (time.monotonic() - tick)))

    def expired(signum, frame):
        raise RuntimeError("bounded throughput run timed out; retain cursor and spool")

    thread = threading.Thread(target=sample, daemon=True)
    started = time.time_ns()
    elapsed = time.monotonic()
    previous = signal.signal(signal.SIGALRM, expired)
    signal.alarm(args.timeout)
    thread.start()
    try:
        result = ingest(client, args.package, args.endpoint, accounts, start, root / "native",
                        os.environ["SUBSTREAMS_SINK_DSN"], stop, max_retries=3,
                        decode_batch_size=args.decode_batch_size, spool_max_idle_ms=args.spool_max_idle_ms,
                        prometheus_addr="127.0.0.1:0")
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, previous)
        done.set()
        thread.join(timeout=65)
    duration = time.monotonic() - elapsed
    if thread.is_alive():
        raise RuntimeError("RPC sampling did not finish")
    run = json.loads((root / "native/run.json").read_text())
    end = load_progress(client, run, root / "native")["position"]["block"]["number"]
    if end != stop - 1:
        raise ValueError("native run did not reach its bound")
    # Capture duration before final inspection; it includes setup, capacity
    # guards, streaming, flush, cursor checks, and waiting for the last RPC sample.
    valid = [value for value in samples if "error_type" not in value and "finalized_lag_blocks" in value]
    report = {**workload, "run_id": run["run_id"], "module_hash": run["identity"]["module_hash"],
        "started_at_unix_ns": started, "finished_at_unix_ns": time.time_ns(), "duration_seconds": duration,
        "blocks": stop - start, "blocks_per_second": (stop - start) / duration,
        "final_position": result["position"]["block"], "lag_samples": len(samples),
        "failed_lag_samples": sum("error_type" in value for value in samples),
        "finalized_lag_blocks_all_phases": distribution([value["finalized_lag_blocks"] for value in valid]),
        "database_parts_bytes_after": client.disk_usage(),
        "limitations": ["block-age uses the host wall clock and second-resolution chain timestamps",
                        "RPC and durable cursor are sampled sequentially; a negative lag can reflect sampling skew",
                        "finalized follow can have native is_live=false; use a declared startup exclusion and observed RPC chain growth",
                        "this update interval does not establish full initial storage or publish a checkpoint",
                        "cache state is not inferred from elapsed time"]}
    atomic_json(root / "result.json", report)
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
