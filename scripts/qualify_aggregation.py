#!/usr/bin/env python3
"""Compare default and spilling aggregation on an isolated real bootstrap prefix.

Run under capacity-run. The source is read briefly under its cleanup lock; all
comparison writes use a fresh database. This does not publish or prove initial
state. Keep the resulting database and report for inspection.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import time
import uuid

from evm_state.bootstrap import _digest
from evm_state.capacity import check as capacity_check
from evm_state.ch import ClickHouse, identifier
from evm_state.checkpoint import ZERO, _observed_fields, _union_storage, validate_interval
from evm_state.cursor import binding, load_progress
from evm_state.files import atomic_json
from evm_state.proof import VerificationError
from evm_state.source import verified_source


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", type=Path, required=True)
    parser.add_argument("--database", required=True, help="fresh isolated comparison database")
    parser.add_argument("--output", type=Path, required=True, help="fresh workload directory")
    parser.add_argument("--delta-blocks", type=int, default=10000)
    args = parser.parse_args()
    if not 1 <= args.delta_blocks <= 100000:
        parser.error("delta-blocks must be between 1 and 100000")
    if not os.environ.get("EVM_STATE_CAPACITY_CONFIG"):
        parser.error("run this workload under capacity-run")
    directory, out = args.state_dir.resolve(), args.output.resolve()
    run = json.loads((directory / "run.json").read_text())
    identity = run["identity"]
    source = ClickHouse(identity["database"])
    target = ClickHouse(identifier(args.database))
    admin = ClickHouse("default")
    if int(admin.one("SELECT count() AS n FROM system.databases WHERE name={db:String}",
                     {"db": target.database})["n"]):
        parser.error("comparison database already exists")
    capacity_check(source, [directory, out], "aggregation-snapshot-start")
    out.mkdir(parents=True, exist_ok=False)
    declared = {k: identity[k] for k in
                ("database", "accounts", "start_block", "module_hash", "final_blocks_only")}
    locked_at = time.monotonic()
    with verified_source(source, declared) as checked:
        raw = (directory / "bootstrap.json").read_bytes()
        prefix = json.loads(raw)
        stored = source.one("SELECT manifest FROM bootstrap_generations WHERE generation={id:String}",
                            {"id": prefix["generation"]})
        if (json.loads(stored["manifest"]) != prefix or prefix.get("binding") != binding(run)
                or prefix.get("status") != "unverified-bootstrap"
                or prefix.get("accounts") != identity["accounts"]
                or prefix.get("start_block") != identity["start_block"]):
            raise VerificationError("source private prefix manifest or binding differs")
        end = prefix["header"]["number"] + args.delta_blocks
        durable = load_progress(source, run, directory)["position"]["block"]
        if durable["number"] < end:
            raise VerificationError("durable suffix is shorter than the requested comparison")
        params = {"prefix": prefix["generation"], "start": prefix["header"]["number"] + 1, "end": end}
        header = source.one("SELECT number,hash,parent_hash,state_root,timestamp FROM state_blocks FINAL "
                            "WHERE number={end:UInt64}", params)
        admin.execute(f"CREATE DATABASE {target.database}")
        for table in ("bootstrap_storage", "state_blocks"):
            target.execute(f"CREATE TABLE {table} AS {source.database}.{table}")
        target.execute(f"INSERT INTO bootstrap_storage SELECT * FROM {source.database}.bootstrap_storage "
                       "WHERE generation={prefix:String}", params)
        target.execute(f"INSERT INTO state_blocks SELECT * FROM {source.database}.state_blocks FINAL "
                       "WHERE number >= {start:UInt64} AND number <= {end:UInt64}", params)
    # No further source reads: cleanup can proceed while we verify the copy.
    lock_seconds = time.monotonic() - locked_at
    copied = _digest(target, prefix["generation"], prefix["fields"], prefix["accounts"])
    if any(prefix[key] != value for key, value in copied.items()):
        raise VerificationError("isolated prefix checksum/count differs")
    isolated = {**checked, "database": target.database, "bootstrap": prefix,
                "delta_start": prefix["header"]["number"] + 1}
    validate_interval(target, isolated, header)
    metadata = {"format_version": 1, "workload": "real private prefix plus contiguous native update suffix",
        "database": target.database, "run_id": run["run_id"],
        "module_hash": identity["module_hash"], "package_sha256": identity["package_sha256"],
        "accounts": prefix["accounts"], "prefix_header": prefix["header"],
        "prefix_generation": prefix["generation"], "prefix_json_sha256": hashlib.sha256(raw).hexdigest(),
        "prefix_state_sha256": prefix["state_sha256"], "prefix_nonzero_slots": prefix["nonzero_slots"],
        "suffix_blocks": args.delta_blocks, "target_header": header, "durable_source_block": durable,
        "source_copy_wait_and_lock_seconds": lock_seconds,
        "server_version": target.one("SELECT version() AS v")["v"],
        "default_settings": list(target.rows("SELECT name,value FROM system.settings WHERE name IN "
            "('max_threads','max_memory_usage','max_bytes_before_external_group_by',"
            "'max_bytes_ratio_before_external_group_by','max_bytes_before_external_sort',"
            "'max_bytes_ratio_before_external_sort') ORDER BY name")),
        "server_settings": list(target.rows("SELECT name,value FROM system.server_settings WHERE name IN "
            "('tmp_path','max_server_memory_usage') ORDER BY name")),
        "limitations": ["private input is checksummed but not a complete account-root proof or ready checkpoint",
            "sequential SQL comparison on shared infrastructure; not a capacity or latency guarantee",
            "query memory is ClickHouse-reported accounting, not whole-process resident memory",
            "query settings affect only these comparison queries; native replay configuration is unchanged"]}
    atomic_json(out / "input.json", metadata)
    base_options = {"max_execution_time": 120, "max_temporary_data_on_disk_size_for_query": 10 * 1024**3}
    variants = [("default", {}), ("spill_256m", {
        "max_memory_usage": 2 * 1024**3,
        "max_bytes_before_external_group_by": 256 * 1024**2,
        "max_bytes_ratio_before_external_group_by": 0,
        "max_bytes_before_external_sort": 128 * 1024**2,
        "max_bytes_ratio_before_external_sort": 0})]
    results = []
    expected = None
    for name, options in variants:
        capacity_check(target, [out], "aggregation-" + name + "-start")
        generation, tag = uuid.uuid4().hex, uuid.uuid4().hex
        params = {"id": generation, "end": end}
        union = _union_storage([isolated], None, target, params)
        settings = {**base_options, **options}
        clause = ",".join(f"{key}={value}" for key, value in settings.items())
        sql = "INSERT INTO bootstrap_storage SELECT {id:String},address,slot,argMax(value,position) AS final_value "
        sql += f"FROM ({union}) GROUP BY address,slot HAVING final_value != '{ZERO}' "
        # ClickHouse's SETTINGS parser needs a string literal here; the tag is
        # generated locally as hexadecimal UUID text, never caller-supplied SQL.
        sql += f"SETTINGS log_comment='{tag}'," + clause
        started = time.monotonic()
        target.execute(sql, params)
        seconds = time.monotonic() - started
        # The HTTP response can precede enqueueing QueryFinish. Flushing once
        # immediately after execute() can therefore miss this completed query.
        for _ in range(50):
            target.execute("SYSTEM FLUSH LOGS")
            records = list(target.rows("SELECT query_id,event_time,query_duration_ms,read_rows,written_rows,memory_usage,"
                "exception_code,mapFilter((k,v)->startsWith(k,'External'),ProfileEvents) AS external_events "
                "FROM system.query_log WHERE log_comment={tag:String} AND type!='QueryStart'", {"tag": tag}))
            if records:
                break
            time.sleep(0.2)
        if len(records) != 1:
            raise VerificationError("expected exactly one completed aggregation query record")
        query = records[0]
        if query["exception_code"]:
            raise VerificationError(f"aggregation query failed with code {query['exception_code']}")
        fields = _observed_fields(target, [isolated], None, params)
        measured = _digest(target, generation, fields, prefix["accounts"])
        if query["exception_code"] or int(query["written_rows"]) != measured["nonzero_slots"]:
            raise VerificationError("aggregation result differs from query completion")
        if expected is not None and measured != expected:
            raise VerificationError("spilling aggregation changed complete ordered state")
        if options and not any(int(v) > 0 for k, v in query["external_events"].items()
                               if k.startswith("ExternalAggregation")):
            raise VerificationError("comparison did not exercise external aggregation")
        expected = measured
        capacity_check(target, [out], "aggregation-" + name + "-verified")
        result = {"name": name, "settings": settings, "generation": generation,
                  "elapsed_seconds": seconds, "query": query, **measured}
        results.append(result)
        atomic_json(out / (name + ".json"), result)
        print(json.dumps({"variant": name, "seconds": seconds, "memory_usage": query["memory_usage"],
                          "nonzero_slots": measured["nonzero_slots"]}), flush=True)
    atomic_json(out / "result.json", {**metadata, "variants": results, "ordered_state_matches": True})


if __name__ == "__main__":
    main()
