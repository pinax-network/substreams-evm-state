#!/usr/bin/env python3
"""Summarize a completed cold/warm/live/live-tuned qualification directory.

The live window excludes the first 60 seconds by elapsed time, irrespective of
lag; slow samples cannot exclude themselves. Native is_live is not a head signal
for finalized-only cursors. Incomplete runs/samples and unequal replay content
fail this summary, and raw artifact hashes retain the measurement provenance.
"""
import argparse
import hashlib
import json
from pathlib import Path

from evm_state.files import atomic_json
from qualify_throughput import distribution, sink_events


def live_window(samples, start_ns, finish_ns):
    window = [sample for sample in samples if start_ns <= sample["started_at_unix_ns"] <= finish_ns]
    if len(window) < 10 or any("error_type" in sample or "finalized_lag_blocks" not in sample for sample in window):
        raise ValueError("live window lacks ten complete RPC/cursor samples")
    rpc = [sample["rpc_finalized_block"] for sample in window]
    durable = [sample["durable_block"] for sample in window]
    if rpc[-1] <= rpc[0] or durable[-1] <= durable[0] or any(a > b for a, b in zip(durable, durable[1:])):
        raise ValueError("live window did not follow a growing chain monotonically")
    return {"samples": len(window), "sample_window_seconds":
            (window[-1]["started_at_unix_ns"] - window[0]["started_at_unix_ns"]) / 1e9,
        "rpc_finalized_advanced_blocks": rpc[-1] - rpc[0], "durable_advanced_blocks": durable[-1] - durable[0],
        "finalized_lag_blocks": distribution([sample["finalized_lag_blocks"] for sample in window]),
        "durable_block_age_seconds": distribution([sample["durable_block_age_seconds"] for sample in window]),
        "rpc_sample_duration_seconds": distribution([sample["sample_duration_seconds"] for sample in window])}


def summarize(root):
    result = {"format_version": 1, "workload": "public BSC native ClickHouse throughput and read latency",
              "runs": {}, "artifact_sha256": {}}
    samples_by_phase = {}
    for phase in ["cold", "warm", "live", "live-tuned"]:
        directory = root / phase
        files = ["result.json", "output.json", "capacity/summary.json", "lag-samples.jsonl", "run.log"]
        for name in files:
            result["artifact_sha256"][phase + "/" + name] = hashlib.sha256((directory / name).read_bytes()).hexdigest()
        run = json.loads((directory / "result.json").read_text())
        output = json.loads((directory / "output.json").read_text())
        capacity = json.loads((directory / "capacity/summary.json").read_text())
        if capacity["status"] != "completed" or capacity["failed_samples"] or run["failed_lag_samples"]:
            raise ValueError("incomplete native/capacity/lag workload")
        if any(run[key] != output[key] for key in ["database", "run_id", "module_hash", "package_sha256", "accounts", "start_block", "blocks"]):
            raise ValueError("native output measurement does not match its timed workload")
        if run["stop_block_exclusive"] != output["end_block"] + 1:
            raise ValueError("native output stopped at another block")
        stats = sink_events(directory / "run.log")
        data = {"ingestion": run, "native_log": stats, "output": output,
            "capacity": {key: capacity[key] for key in ["samples", "guard_samples", "failed_samples", "status",
                "peak_observed_allocated_bytes", "maximum_sample_gap_ns", "rejected_guard_stages"]}}
        # Raw config paths and cursor tokens are not needed in a public record.
        data["capacity"].update({key: capacity["config"][key] for key in ["budget_bytes", "headroom_bytes", "min_free_bytes"]})
        periodic = [json.loads(line) for line in (directory / "capacity/samples.jsonl").read_text().splitlines()]
        data["capacity"]["peak_spool_allocated_bytes"] = max(sample["local_components"]["spool"]["allocated_bytes"] for sample in periodic)
        samples = [json.loads(line) for line in (directory / "lag-samples.jsonl").read_text().splitlines()]
        samples_by_phase[phase] = samples
        if phase.startswith("live"):
            data["after_first_60_seconds"] = live_window(samples, run["started_at_unix_ns"] + 60_000_000_000, run["finished_at_unix_ns"])
        result["runs"][phase] = data
    cold, warm = [result["runs"][phase] for phase in ["cold", "warm"]]
    for key in ["module_hash", "package_sha256", "start_block", "end_block", "ordered_output_sha256", "logical_protobuf_bytes"]:
        if cold["output"][key] != warm["output"][key]:
            raise ValueError("cold/warm runs differ in identity, interval or ordered output")
    if cold["native_log"]["last_reported_processed_blocks"] != cold["output"]["blocks"]:
        raise ValueError("cold run does not report the full server processing count")
    if warm["native_log"]["last_reported_processed_blocks"] != 0:
        raise ValueError("warm run reports additional server processing")
    if not cold["native_log"]["max_observed_running_jobs"] or warm["native_log"]["max_observed_running_jobs"] != 0:
        raise ValueError("server job observations do not support the cache comparison")
    overlap_start = max(result["runs"][phase]["ingestion"]["started_at_unix_ns"] for phase in ["live", "live-tuned"]) + 60_000_000_000
    overlap_end = min(result["runs"][phase]["ingestion"]["finished_at_unix_ns"] for phase in ["live", "live-tuned"])
    result["overlapping_live_window"] = {phase: live_window(samples_by_phase[phase], overlap_start, overlap_end)
                                         for phase in ["live", "live-tuned"]}
    result["reads"] = {}
    for name in ["quiet-reads.json", "large-reads.json"]:
        path = root / name
        record = json.loads(path.read_text())
        result["artifact_sha256"][name] = hashlib.sha256(path.read_bytes()).hexdigest()
        result["reads"][name] = {key: value for key, value in record.items() if key != "calls"}
    result["cache_design"] = json.loads((root / "cache-design.json").read_text())
    result["limitations"] = ["public three-account filter including WBNB; customer 19/64-account lists unavailable",
        "cache comparison adds one random cache-identity sentinel with no observed changes; not a customer contract",
        "fresh module-output identity and server progress qualify output cache work; upstream block/input caches are unknown",
        "warm replay is a new local database; local OS/ClickHouse caches are not flushed",
        "native is_live=false is expected for finalized-only delivery and does not measure chain lag",
        "live runs overlap and share a local ClickHouse server with other qualification work",
        "sampled directory totals include other server databases and do not establish long-term customer capacity",
        "logical protobuf output is distinct from wire bytes, billable egress and retained disk size",
        "no complete initial WBNB storage, production SLA, full-history rate or customer price follows from this sample"]
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    atomic_json(args.output, summarize(args.root))
    print(args.output)
