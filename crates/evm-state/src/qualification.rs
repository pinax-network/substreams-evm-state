//! Evidence collection utilities. Private replay prefixes are never ready state.
use crate::{
    ch::{params, uint, ClickHouse},
    control::object_id,
    cursor,
    files::{canonical_json, resolve},
    proof::string,
    reader::now_ns,
};
use anyhow::{ensure, Context, Result};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, Read, Seek, SeekFrom, Write},
    path::Path,
    time::{Duration, Instant},
};

fn latest_sample(path: &Path) -> Result<Option<Value>> {
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    let length = file.metadata()?.len();
    file.seek(SeekFrom::Start(length.saturating_sub(262144)))?;
    let mut text = String::new();
    file.read_to_string(&mut text)?;
    let Some(last) = text.lines().last() else {
        return Ok(None);
    };
    // The sampler may be between writes; retain the rejection in its own log.
    Ok(serde_json::from_str(last).ok())
}

pub fn admit_growth(prefix: &Value, run: &Value, sample: &Value, now: u64) -> Result<bool> {
    ensure!(
        prefix["binding"] == cursor::binding(run)?
            && prefix["accounts"] == run["identity"]["accounts"]
            && prefix["status"] == "unverified-bootstrap",
        "prefix/run binding changed"
    );
    if sample["admitted"] != true || sample.get("accounted_allocated_bytes").is_none() {
        return Ok(false);
    }
    let finished = uint(&sample["sample_finished_ns"])?;
    Ok(finished <= now && now - finished <= 60_000_000_000)
}

pub fn compaction_query(
    client: &ClickHouse,
    generation: &str,
    expected_rows: u64,
) -> Result<Value> {
    // A later INSERT also mentions its input generation. Match the destination
    // literal at the start of ClickHouse's normalized query, not any occurrence.
    let destination = format!(
        "INSERT INTO bootstrap_storage SELECT '{}',",
        object_id(generation)?
    );
    let queries = client.rows("SELECT query_id,event_time,query_duration_ms,read_rows,written_rows,memory_usage,exception_code,mapFilter((k,v)->startsWith(k,'External'),ProfileEvents) AS external_events FROM system.query_log WHERE current_database={db:String} AND type='QueryFinish' AND startsWith(query,{destination:String})",
        &params(json!({"db":client.database,"destination":destination}))?)?.collect::<Result<Vec<_>>>()?;
    ensure!(
        queries.len() <= 1,
        "multiple compaction completions for one generation"
    );
    let query = queries.into_iter().next().unwrap_or(Value::Null);
    if !query.is_null() {
        ensure!(
            uint(&query["exception_code"])? == 0 && uint(&query["written_rows"])? == expected_rows,
            "compaction completion differs from private prefix"
        );
    }
    Ok(query)
}

/// Physical parts belonging to one immutable private generation, not the server.
pub fn generation_parts(client: &ClickHouse, generation: &str) -> Result<Value> {
    let generation = object_id(generation)?;
    let parts = client.rows("SELECT table,active,count() AS parts,sum(rows) AS rows,sum(data_compressed_bytes) AS compressed_bytes,sum(bytes_on_disk) AS part_bytes FROM system.parts WHERE database={db:String} AND table IN ('bootstrap_storage','bootstrap_generations') AND (partition={generation:String} OR partition={quoted:String}) GROUP BY table,active ORDER BY table,active",
        &params(json!({"db":client.database,"generation":generation,"quoted":format!("'{generation}'")}))?)?.collect::<Result<Vec<_>>>()?;
    let mut storage_rows = 0_u64;
    let mut manifest_rows = 0_u64;
    let mut active_bytes = 0_u64;
    let mut inactive_bytes = 0_u64;
    for part in &parts {
        let bytes = uint(&part["part_bytes"])?;
        if uint(&part["active"])? == 1 {
            active_bytes = active_bytes
                .checked_add(bytes)
                .context("part byte overflow")?;
            match string(part, "table")? {
                "bootstrap_storage" => storage_rows = uint(&part["rows"])?,
                "bootstrap_generations" => manifest_rows = uint(&part["rows"])?,
                _ => anyhow::bail!("unexpected private generation table"),
            }
        } else {
            inactive_bytes = inactive_bytes
                .checked_add(bytes)
                .context("part byte overflow")?;
        }
    }
    Ok(
        json!({"generation":generation,"active_storage_rows":storage_rows,
        "active_manifest_rows":manifest_rows,"active_part_bytes":active_bytes,
        "inactive_part_bytes":inactive_bytes,"parts":parts,
        "scope":"ClickHouse part catalog for this private generation only; excludes native history, other generations, exports, workspaces and server-wide allocation"}),
    )
}

pub fn record_growth(
    root: &Path,
    output: &Path,
    duration: Duration,
    reserve_bytes: u64,
    budget_bytes: u64,
) -> Result<()> {
    let root = resolve(root)?;
    let mut seen = BTreeMap::<String, (String, String)>::new();
    if output.try_exists()? {
        for line in BufReader::new(File::open(output)?).lines() {
            // Recover observed generations from complete prior records only.
            let line = line?;
            if let Ok(record) = serde_json::from_str::<Value>(&line) {
                // Enrich a legacy last observation once after an upgrade.
                if record.get("generation_parts").is_none() {
                    continue;
                }
                seen.insert(
                    string(&record, "cohort")?.into(),
                    (
                        string(&record, "phase")?.into(),
                        string(&record, "generation")?.into(),
                    ),
                );
            }
        }
    }
    fs::create_dir_all(output.parent().context("growth output has no parent")?)?;
    let mut log = OpenOptions::new().create(true).append(true).open(output)?;
    let started = Instant::now();
    while started.elapsed() < duration {
        for name in ["wbnb-bootstrap", "customer-example"] {
            let path = root.join(name);
            let active = path.join("active-capacity.json");
            let phase = if active.try_exists()? {
                string(
                    &serde_json::from_slice::<Value>(&fs::read(active)?)?,
                    "directory",
                )?
                .to_owned()
            } else {
                "host-disk-capacity".into()
            };
            let sample_dir = resolve(&path.join(&phase))?;
            ensure!(
                sample_dir.starts_with(&path),
                "capacity sample directory escapes its cohort"
            );
            let run: Value = serde_json::from_slice(&fs::read(path.join("native/run.json"))?)?;
            let raw = fs::read(path.join("native/bootstrap.json"))?;
            let prefix: Value = serde_json::from_slice(&raw)?;
            let generation = string(&prefix, "generation")?.to_owned();
            // Validate binding even when the generation has not advanced.
            ensure!(
                prefix["binding"] == cursor::binding(&run)?
                    && prefix["accounts"] == run["identity"]["accounts"]
                    && prefix["status"] == "unverified-bootstrap",
                "prefix/run binding changed"
            );
            if seen.get(name) == Some(&(phase.clone(), generation.clone())) {
                continue;
            }
            let Some(sample) = latest_sample(&sample_dir.join("samples.jsonl"))? else {
                continue;
            };
            let now = now_ns()?;
            if !admit_growth(&prefix, &run, &sample, now)? {
                continue;
            }
            let client = ClickHouse::new(string(&run["identity"], "database")?)?;
            ensure!(
                run["identity"]["http_url"] == client.url,
                "growth recorder source URL differs from its native run"
            );
            let query = compaction_query(&client, &generation, uint(&prefix["nonzero_slots"])?)?;
            let parts = generation_parts(&client, &generation)?;
            // Compaction can publish and clean a newer generation during these
            // read-only measurements. Retry that new pointer on the next pass.
            if fs::read(path.join("native/bootstrap.json"))? != raw {
                continue;
            }
            ensure!(
                parts["active_storage_rows"] == prefix["nonzero_slots"]
                    && parts["active_manifest_rows"] == 1,
                "generation part counts differ from the private prefix"
            );
            let record = json!({"phase":phase,"retained_original_volume_reserve_bytes":reserve_bytes,"overall_budget_bytes":budget_bytes,
                "observed_ns":now,"cohort":name,"run_id":run["run_id"],"module_hash":run["identity"]["module_hash"],
                "package_sha256":run["identity"]["package_sha256"],"prefix_json_sha256":hex::encode(Sha256::digest(raw)),
                "generation":generation,"header":prefix["header"],"nonzero_slots":prefix["nonzero_slots"],"state_sha256":prefix["state_sha256"],
                "status":prefix["status"],"capacity_sample_finished_ns":sample["sample_finished_ns"],
                "accounted_allocated_bytes":sample["accounted_allocated_bytes"],"admitted":sample["admitted"],"compaction_query":query,"generation_parts":parts,
                "qualification":"Checksummed private prefix and fresh admitted capacity sample; not a full account-root proof, retained-footprint bound or billing evidence. Query memory is server accounting, not process RSS."});
            writeln!(log, "{}", canonical_json(&record)?)?;
            log.flush()?;
            log.sync_data()?;
            seen.insert(name.to_owned(), (phase, generation));
            println!(
                "{}",
                json!({"cohort":name,"block":prefix["header"]["number"],"slots":prefix["nonzero_slots"],"query_memory_bytes":query["memory_usage"]})
            );
        }
        std::thread::sleep(Duration::from_secs(30).min(duration.saturating_sub(started.elapsed())));
    }
    Ok(())
}
