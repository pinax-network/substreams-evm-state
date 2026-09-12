//! Compare default and external aggregation on an isolated copy of a real
//! private prefix and contiguous suffix. This never publishes a checkpoint.
use crate::{
    bootstrap, capacity,
    ch::{params, uint, ClickHouse},
    checkpoint::{self, ZERO},
    control::new_id,
    cursor, files,
    proof::string,
    source::verified_source,
};
use anyhow::{ensure, Context, Result};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    fs,
    path::Path,
    time::{Duration, Instant},
};

pub fn measure(target: &ClickHouse, directory: &Path, out: &Path, delta: u64) -> Result<Value> {
    ensure!(
        (1..=100000).contains(&delta),
        "delta blocks must be between 1 and 100000"
    );
    let directory = files::resolve(directory)?;
    let out = files::resolve(out)?;
    let run: Value = serde_json::from_slice(&fs::read(directory.join("run.json"))?)?;
    let identity = &run["identity"];
    let source = target.with_database(string(identity, "database")?)?;
    let admin = target.with_database("default")?;
    ensure!(
        uint(
            &admin.one(
                "SELECT count() AS n FROM system.databases WHERE name={db:String}",
                &params(json!({"db":target.database}))?
            )?["n"]
        )? == 0,
        "comparison database already exists"
    );
    capacity::check(
        &source,
        &[directory.clone(), out.clone()],
        "aggregation-snapshot-start",
    )?;
    fs::create_dir_all(out.parent().context("output has no parent")?)?;
    fs::create_dir(&out)?;
    let mut declared = json!({});
    for key in [
        "database",
        "accounts",
        "start_block",
        "module_hash",
        "final_blocks_only",
    ] {
        declared[key] = identity[key].clone();
    }
    let locked = Instant::now();
    let (checked, prefix, raw, header, durable) = {
        let guard = verified_source(&source, &declared, None, None)?;
        ensure!(
            guard.source["state_directory"]
                == directory.to_str().context("invalid native directory")?,
            "comparison directory does not own its source"
        );
        let raw = fs::read(directory.join("bootstrap.json"))?;
        let prefix: Value = serde_json::from_slice(&raw)?;
        let stored = source.one(
            "SELECT manifest FROM bootstrap_generations WHERE generation={id:String}",
            &params(json!({"id":prefix["generation"]}))?,
        )?;
        ensure!(
            serde_json::from_str::<Value>(string(&stored, "manifest")?)? == prefix
                && prefix["binding"] == cursor::binding(&run)?
                && prefix["status"] == "unverified-bootstrap"
                && prefix["accounts"] == identity["accounts"]
                && prefix["start_block"] == identity["start_block"],
            "source private prefix manifest or binding differs"
        );
        let start = uint(&prefix["header"]["number"])?
            .checked_add(1)
            .context("invalid prefix end")?;
        let end = start
            .checked_add(delta - 1)
            .context("comparison bound overflow")?;
        let durable =
            cursor::load_progress(&source, &run, &directory)?["position"]["block"].clone();
        ensure!(
            uint(&durable["number"])? >= end,
            "durable suffix is shorter than requested comparison"
        );
        let parameters = params(json!({"prefix":prefix["generation"],"start":start,"end":end}))?;
        let header=source.one("SELECT number,hash,parent_hash,state_root,timestamp FROM state_blocks FINAL WHERE number={end:UInt64}",&parameters)?;
        admin.execute(
            &format!("CREATE DATABASE {}", target.database),
            &Default::default(),
        )?;
        for table in ["bootstrap_storage", "state_blocks"] {
            target.execute(
                &format!("CREATE TABLE {table} AS {}.{table}", source.database),
                &Default::default(),
            )?;
        }
        target.execute(&format!("INSERT INTO bootstrap_storage SELECT * FROM {}.bootstrap_storage WHERE generation={{prefix:String}}",source.database),&parameters)?;
        target.execute(&format!("INSERT INTO state_blocks SELECT * FROM {}.state_blocks FINAL WHERE number >= {{start:UInt64}} AND number <= {{end:UInt64}}",source.database),&parameters)?;
        (guard.source.clone(), prefix, raw, header, durable)
    };
    // Release cleanup coordination before checksumming or running comparisons.
    let lock_seconds = locked.elapsed().as_secs_f64();
    let accounts = json!(checkpoint::canonical_accounts(&prefix["accounts"])?);
    let copied = bootstrap::digest(
        target,
        string(&prefix, "generation")?,
        &prefix["fields"],
        &accounts,
    )?;
    for (key, value) in copied.as_object().context("invalid prefix digest")? {
        ensure!(
            prefix.get(key) == Some(value),
            "isolated prefix checksum/count differs"
        );
    }
    let mut isolated = checked;
    isolated["database"] = json!(target.database);
    isolated["bootstrap"] = prefix.clone();
    isolated["delta_start"] = json!(uint(&prefix["header"]["number"])? + 1);
    checkpoint::validate_interval(target, &isolated, &header)?;
    let metadata = json!({"format_version":1,"workload":"real private prefix plus contiguous native update suffix","database":target.database,
        "run_id":run["run_id"],"module_hash":identity["module_hash"],"package_sha256":identity["package_sha256"],"accounts":prefix["accounts"],
        "prefix_header":prefix["header"],"prefix_generation":prefix["generation"],"prefix_json_sha256":hex::encode(Sha256::digest(raw)),
        "prefix_state_sha256":prefix["state_sha256"],"prefix_nonzero_slots":prefix["nonzero_slots"],"suffix_blocks":delta,"target_header":header,
        "durable_source_block":durable,"source_copy_wait_and_lock_seconds":lock_seconds,"server_version":target.one("SELECT version() AS v",&Default::default())?["v"],
        "default_settings":target.rows("SELECT name,value FROM system.settings WHERE name IN ('max_threads','max_memory_usage','max_bytes_before_external_group_by','max_bytes_ratio_before_external_group_by','max_bytes_before_external_sort','max_bytes_ratio_before_external_sort') ORDER BY name",&Default::default())?.collect::<Result<Vec<_>>>()?,
        "server_settings":target.rows("SELECT name,value FROM system.server_settings WHERE name IN ('tmp_path','max_server_memory_usage') ORDER BY name",&Default::default())?.collect::<Result<Vec<_>>>()?,
        "limitations":["private input is checksummed but not a complete account-root proof or ready checkpoint","sequential SQL comparison on shared infrastructure; not a capacity or latency guarantee","query memory is ClickHouse-reported accounting, not whole-process resident memory","query settings affect only these comparison queries; native replay configuration is unchanged"]});
    files::atomic_json(&out.join("input.json"), &metadata, false)?;
    let mut results = Vec::new();
    let mut expected = None;
    for name in ["default", "spill_256m"] {
        capacity::check(target, &[out.clone()], &format!("aggregation-{name}-start"))?;
        let generation = new_id();
        let tag = new_id();
        let mut parameters = params(json!({"id":generation,"end":header["number"]}))?;
        let union = checkpoint::union_storage(&[isolated.clone()], None, &mut parameters)?;
        let mut settings = json!({"max_execution_time":120,"max_temporary_data_on_disk_size_for_query":10u64*1024*1024*1024});
        if name == "spill_256m" {
            settings.as_object_mut().unwrap().extend(json!({"max_memory_usage":2u64*1024*1024*1024,"max_bytes_before_external_group_by":256*1024*1024,"max_bytes_ratio_before_external_group_by":0,"max_bytes_before_external_sort":128*1024*1024,"max_bytes_ratio_before_external_sort":0}).as_object().unwrap().clone());
        }
        let clause = settings
            .as_object()
            .unwrap()
            .iter()
            .map(|(key, value)| format!("{key}={value}"))
            .collect::<Vec<_>>()
            .join(",");
        let sql=format!("INSERT INTO bootstrap_storage SELECT {{id:String}},address,slot,argMax(value,position) AS final_value FROM ({union}) GROUP BY address,slot HAVING final_value != '{ZERO}' SETTINGS log_comment='{tag}',{clause}");
        let started = Instant::now();
        target.execute(&sql, &parameters)?;
        let seconds = started.elapsed().as_secs_f64();
        let mut records = Vec::new();
        for _ in 0..50 {
            target.execute("SYSTEM FLUSH LOGS", &Default::default())?;
            records=target.rows("SELECT query_id,event_time,query_duration_ms,read_rows,written_rows,memory_usage,exception_code,mapFilter((k,v)->startsWith(k,'External'),ProfileEvents) AS external_events FROM system.query_log WHERE log_comment={tag:String} AND type!='QueryStart'",&params(json!({"tag":tag}))?)?.collect::<Result<Vec<_>>>()?;
            if !records.is_empty() {
                break;
            }
            std::thread::sleep(Duration::from_millis(200));
        }
        ensure!(
            records.len() == 1,
            "expected exactly one completed aggregation query record"
        );
        let query = &records[0];
        ensure!(
            uint(&query["exception_code"])? == 0,
            "aggregation query failed"
        );
        let fields = checkpoint::observed_fields(target, &[isolated.clone()], None, &parameters)?;
        let measured = bootstrap::digest(target, &generation, &fields, &accounts)?;
        ensure!(
            uint(&query["written_rows"])? == uint(&measured["nonzero_slots"])?,
            "aggregation result differs from query completion"
        );
        ensure!(
            expected
                .as_ref()
                .is_none_or(|previous| previous == &measured),
            "spilling aggregation changed complete ordered state"
        );
        if name == "spill_256m" {
            ensure!(
                query["external_events"]
                    .as_object()
                    .context("missing external aggregation counters")?
                    .iter()
                    .any(|(key, value)| key.starts_with("ExternalAggregation")
                        && uint(value).is_ok_and(|v| v > 0)),
                "comparison did not exercise external aggregation"
            );
        }
        expected = Some(measured.clone());
        capacity::check(
            target,
            &[out.clone()],
            &format!("aggregation-{name}-verified"),
        )?;
        let mut result = json!({"name":name,"settings":settings,"generation":generation,"elapsed_seconds":seconds,"query":query});
        result
            .as_object_mut()
            .unwrap()
            .extend(measured.as_object().unwrap().clone());
        files::atomic_json(&out.join(format!("{name}.json")), &result, false)?;
        results.push(result);
        println!(
            "{}",
            json!({"variant":name,"seconds":seconds,"memory_usage":query["memory_usage"],"nonzero_slots":measured["nonzero_slots"]})
        );
    }
    let mut result = metadata;
    result["variants"] = json!(results);
    result["ordered_state_matches"] = json!(true);
    files::atomic_json(&out.join("result.json"), &result, false)?;
    Ok(result)
}
