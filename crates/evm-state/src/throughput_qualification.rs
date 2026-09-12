//! Evidence checks for native cache/finality measurements. Missing telemetry is
//! unknown; it cannot become a zero-work or zero-lag claim.
use crate::{ch::uint, files, trie_qualification::file_hash};
use anyhow::{ensure, Context, Result};
use serde_json::{json, Value};
use std::{
    fs,
    io::{BufRead, BufReader},
    path::Path,
};

pub fn distribution(values: &[f64]) -> Result<Value> {
    ensure!(
        values.iter().all(|v| v.is_finite()),
        "non-finite throughput observation"
    );
    if values.is_empty() {
        return Ok(Value::Null);
    }
    let mut ordered = values.to_vec();
    ordered.sort_by(f64::total_cmp);
    let at = |p: f64| ordered[(ordered.len() as f64 * p).ceil() as usize - 1];
    Ok(
        json!({"samples":ordered.len(),"min":ordered[0],"p50":at(0.5),"p95":at(0.95),"max":ordered[ordered.len()-1]}),
    )
}
fn select(value: &Value, keys: &[&str], required: bool) -> Result<Value> {
    let mut selected = json!({});
    for key in keys {
        if let Some(value) = value.get(key) {
            selected[*key] = value.clone();
        } else {
            ensure!(!required, "measurement is missing {key}");
        }
    }
    Ok(selected)
}
pub fn sink_events(path: &Path) -> Result<Value> {
    let (mut sessions, mut progress, mut settings) = (Vec::new(), Vec::new(), Vec::new());
    for line in BufReader::new(fs::File::open(path)?).lines() {
        let line = line?;
        let Some((prefix, payload)) = line.split_once(" {") else {
            continue;
        };
        let session = prefix.contains("session initialized with remote endpoint");
        let setting = prefix.contains("Relational Mappings Mode sink settings");
        if !session && !setting && !prefix.contains("substreams stream stats") {
            continue;
        }
        let value: Value = serde_json::from_str(&format!("{{{payload}"))?;
        let keys: &[&str] = if session {
            &[
                "max_parallel_workers",
                "linear_handoff_block",
                "resolved_start_block",
                "trace_id",
            ]
        } else if setting {
            &[
                "decode_workers",
                "decode_batch_size",
                "spool_max_idle",
                "spool_max_size",
                "db_write_target_duration",
                "db_write_max_size",
            ]
        } else {
            &[
                "progress_running_jobs",
                "progress_total_processed_blocks",
                "is_live",
                "last_block",
                "data_msg_rate",
                "undo_msg_rate",
            ]
        };
        let mut record = select(&value, keys, false)?;
        if !setting {
            record["timestamp"] = json!(prefix
                .split_whitespace()
                .next()
                .context("log timestamp missing")?);
        }
        if session {
            sessions.push(record);
        } else if setting {
            settings.push(record);
        } else {
            progress.push(record);
        }
    }
    let mut maximum = None::<u64>;
    let mut last = None;
    for value in &progress {
        if let Some(jobs) = value.get("progress_running_jobs") {
            let count = jobs
                .as_object()
                .context("invalid running jobs telemetry")?
                .values()
                .try_fold(0u64, |sum, value| {
                    sum.checked_add(uint(value)?)
                        .context("running job count overflow")
                })?;
            maximum = Some(maximum.unwrap_or(0).max(count));
        }
        if let Some(count) = value.get("progress_total_processed_blocks") {
            last = Some(uint(count)?);
        }
    }
    Ok(
        json!({"sessions":sessions,"progress":progress,"native_settings":settings,
        "max_observed_running_jobs":maximum,"last_reported_processed_blocks":last}),
    )
}
pub fn live_window(samples: &[Value], start: u64, finish: u64) -> Result<Value> {
    ensure!(
        start <= finish,
        "live runs have no overlapping measurement window"
    );
    let mut window = Vec::new();
    for sample in samples {
        let time = uint(&sample["started_at_unix_ns"])?;
        if start <= time && time <= finish {
            window.push(sample);
        }
    }
    ensure!(
        window.len() >= 10,
        "live window lacks ten complete RPC/cursor samples"
    );
    let (mut rpc, mut durable, mut stamps) = (Vec::new(), Vec::new(), Vec::new());
    let (mut lag, mut age, mut duration) = (Vec::new(), Vec::new(), Vec::new());
    for sample in &window {
        ensure!(
            sample.get("error_type").is_none(),
            "live window contains a failed sample"
        );
        rpc.push(uint(&sample["rpc_finalized_block"])?);
        durable.push(uint(&sample["durable_block"])?);
        stamps.push(uint(&sample["started_at_unix_ns"])?);
        lag.push(
            sample["finalized_lag_blocks"]
                .as_f64()
                .context("missing finality lag")?,
        );
        age.push(
            sample["durable_block_age_seconds"]
                .as_f64()
                .context("missing block age")?,
        );
        let elapsed = sample["sample_duration_seconds"]
            .as_f64()
            .context("missing sample duration")?;
        ensure!(elapsed >= 0., "negative sample duration");
        duration.push(elapsed);
    }
    let first = 0;
    let last = window.len() - 1;
    ensure!(
        rpc[last] > rpc[first]
            && durable[last] > durable[first]
            && durable.windows(2).all(|w| w[0] <= w[1])
            && stamps.windows(2).all(|w| w[0] < w[1]),
        "live window did not follow a growing chain monotonically"
    );
    Ok(
        json!({"samples":window.len(),"sample_window_seconds":(stamps[last]-stamps[first]) as f64 / 1e9,
        "rpc_finalized_advanced_blocks":rpc[last]-rpc[first],"durable_advanced_blocks":durable[last]-durable[first],
        "finalized_lag_blocks":distribution(&lag)?,"durable_block_age_seconds":distribution(&age)?,
        "rpc_sample_duration_seconds":distribution(&duration)?}),
    )
}
fn read(path: &Path) -> Result<Value> {
    Ok(serde_json::from_slice(&fs::read(path)?)?)
}
fn json_lines(path: &Path) -> Result<Vec<Value>> {
    BufReader::new(fs::File::open(path)?)
        .lines()
        .map(|line| Ok(serde_json::from_str(&line?)?))
        .collect()
}
pub fn summarize(root: &Path, output: &Path) -> Result<Value> {
    ensure!(!output.try_exists()?, "summary output already exists");
    let mut result = json!({"format_version":1,"workload":"public BSC native ClickHouse throughput and read latency","runs":{},"artifact_sha256":{}});
    let mut by_phase = std::collections::BTreeMap::new();
    for phase in ["cold", "warm", "live", "live-tuned"] {
        let directory = root.join(phase);
        // Hash every input used in the summary, including periodic capacity data.
        for name in [
            "result.json",
            "output.json",
            "capacity/summary.json",
            "capacity/samples.jsonl",
            "lag-samples.jsonl",
            "run.log",
        ] {
            result["artifact_sha256"][format!("{phase}/{name}")] =
                json!(file_hash(&directory.join(name))?);
        }
        let run = read(&directory.join("result.json"))?;
        let bytes = read(&directory.join("output.json"))?;
        let capacity = read(&directory.join("capacity/summary.json"))?;
        ensure!(
            capacity["status"] == "completed"
                && uint(&capacity["failed_samples"])? == 0
                && uint(&run["failed_lag_samples"])? == 0,
            "incomplete native/capacity/lag workload"
        );
        for key in [
            "database",
            "run_id",
            "module_hash",
            "package_sha256",
            "accounts",
            "start_block",
            "blocks",
        ] {
            ensure!(
                run.get(key).is_some() && run[key] == bytes[key],
                "native output measurement does not match timed workload: {key}"
            );
        }
        ensure!(
            uint(&bytes["end_block"])?.checked_add(1) == Some(uint(&run["stop_block_exclusive"])?),
            "native output stopped at another block"
        );
        let mut meter = select(
            &capacity,
            &[
                "samples",
                "guard_samples",
                "failed_samples",
                "status",
                "peak_observed_allocated_bytes",
                "maximum_sample_gap_ns",
                "rejected_guard_stages",
            ],
            true,
        )?;
        meter.as_object_mut().unwrap().extend(
            select(
                &capacity["config"],
                &["budget_bytes", "headroom_bytes", "min_free_bytes"],
                true,
            )?
            .as_object()
            .unwrap()
            .clone(),
        );
        let periodic = json_lines(&directory.join("capacity/samples.jsonl"))?;
        ensure!(
            !periodic.is_empty(),
            "missing periodic capacity observations"
        );
        let peak = periodic
            .iter()
            .map(|s| uint(&s["local_components"]["spool"]["allocated_bytes"]))
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .max()
            .unwrap();
        meter["peak_spool_allocated_bytes"] = json!(peak);
        let samples = json_lines(&directory.join("lag-samples.jsonl"))?;
        ensure!(
            uint(&run["lag_samples"])? == samples.len() as u64
                && samples.iter().all(|s| s.get("error_type").is_none()),
            "lag summary omits failed or missing samples"
        );
        let mut data = json!({"ingestion":run,"native_log":sink_events(&directory.join("run.log"))?,"output":bytes,"capacity":meter});
        if phase.starts_with("live") {
            data["after_first_60_seconds"] = live_window(
                &samples,
                uint(&run["started_at_unix_ns"])?
                    .checked_add(60_000_000_000)
                    .context("invalid start time")?,
                uint(&run["finished_at_unix_ns"])?,
            )?;
        }
        by_phase.insert(phase, samples);
        result["runs"][phase] = data;
    }
    let cold = &result["runs"]["cold"];
    let warm = &result["runs"]["warm"];
    for key in [
        "module_hash",
        "package_sha256",
        "start_block",
        "end_block",
        "ordered_output_sha256",
        "logical_protobuf_bytes",
    ] {
        ensure!(
            cold["output"].get(key).is_some() && cold["output"][key] == warm["output"][key],
            "cold/warm runs differ in identity, interval or ordered output: {key}"
        );
    }
    ensure!(
        cold["native_log"]["last_reported_processed_blocks"] == cold["output"]["blocks"]
            && uint(&warm["native_log"]["last_reported_processed_blocks"])? == 0,
        "server processing telemetry does not support cache comparison"
    );
    ensure!(
        uint(&cold["native_log"]["max_observed_running_jobs"])? > 0
            && uint(&warm["native_log"]["max_observed_running_jobs"])? == 0,
        "server job observations do not support cache comparison"
    );
    let starts = ["live", "live-tuned"]
        .map(|phase| uint(&result["runs"][phase]["ingestion"]["started_at_unix_ns"]));
    let finishes = ["live", "live-tuned"]
        .map(|phase| uint(&result["runs"][phase]["ingestion"]["finished_at_unix_ns"]));
    let [a, b] = starts;
    let start = a?
        .max(b?)
        .checked_add(60_000_000_000)
        .context("invalid overlap time")?;
    let [a, b] = finishes;
    let finish = a?.min(b?);
    result["overlapping_live_window"] = json!({});
    for phase in ["live", "live-tuned"] {
        result["overlapping_live_window"][phase] = live_window(&by_phase[phase], start, finish)?;
    }
    result["reads"] = json!({});
    for name in ["quiet-reads.json", "large-reads.json"] {
        let mut record = read(&root.join(name))?;
        result["artifact_sha256"][name] = json!(file_hash(&root.join(name))?);
        record
            .as_object_mut()
            .context("invalid read evidence")?
            .remove("calls");
        result["reads"][name] = record;
    }
    result["cache_design"] = read(&root.join("cache-design.json"))?;
    result["artifact_sha256"]["cache-design.json"] =
        json!(file_hash(&root.join("cache-design.json"))?);
    result["limitations"] = json!(["public three-account filter including WBNB; customer 19/64-account lists unavailable",
        "cache comparison adds one random cache-identity sentinel with no observed changes; not a customer contract",
        "fresh module-output identity and server progress qualify output cache work; upstream block/input caches are unknown",
        "warm replay is a new local database; local OS/ClickHouse caches are not flushed",
        "native is_live=false is expected for finalized-only delivery and does not measure chain lag",
        "live runs overlap and share a local ClickHouse server with other qualification work",
        "sampled directory totals include other server databases and do not establish long-term customer capacity",
        "logical protobuf output is distinct from wire bytes, billable egress and retained disk size",
        "no complete initial WBNB storage, production SLA, full-history rate or customer price follows from this sample"]);
    files::atomic_json(output, &result, false)?;
    Ok(result)
}

#[derive(Clone, clap::Args)]
pub struct ThroughputOptions {
    #[arg(long)]
    pub database: String,
    #[arg(long)]
    pub root: std::path::PathBuf,
    #[arg(long)]
    pub package: std::path::PathBuf,
    #[arg(long)]
    pub accounts: String,
    #[arg(long, default_value = "bsc.substreams.pinax.network:443")]
    pub endpoint: String,
    #[arg(long)]
    pub start_block: Option<u64>,
    #[arg(long)]
    pub stop_block: Option<u64>,
    /// Begin 100 blocks behind finality and stop N blocks ahead.
    #[arg(long)]
    pub live_blocks: Option<u64>,
    #[arg(long, default_value_t = 5.0)]
    pub interval: f64,
    #[arg(long, default_value_t = 2400)]
    pub timeout: u64,
    #[arg(long, default_value_t = 1)]
    pub decode_batch_size: u32,
    #[arg(long, default_value_t = 100)]
    pub spool_max_idle_ms: u64,
}
impl ThroughputOptions {
    pub fn range(&self, finalized: u64) -> Result<(u64, u64)> {
        ensure!(
            self.interval.is_finite()
                && self.interval >= 1.
                && self.interval <= 3600.
                && self.timeout > 0,
            "invalid throughput sampling interval or timeout"
        );
        if let Some(live) = self.live_blocks {
            ensure!(
                self.start_block.is_none()
                    && self.stop_block.is_none()
                    && (1..=10000).contains(&live),
                "live blocks must be 1..10000 and cannot be combined with an explicit range"
            );
            Ok((
                finalized
                    .checked_sub(100)
                    .context("finalized chain is too short")?,
                finalized
                    .checked_add(live)
                    .and_then(|n| n.checked_add(1))
                    .context("live bound overflow")?,
            ))
        } else {
            let (start, stop) = (
                self.start_block.context("missing start block")?,
                self.stop_block.context("missing stop block")?,
            );
            ensure!(
                start < stop && stop - 1 <= finalized && stop - start <= 100000,
                "provide an already-finalized range of at most 100000 blocks"
            );
            Ok((start, stop))
        }
    }
}
fn now_ns() -> Result<u64> {
    Ok(std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos()
        .try_into()?)
}
fn lag_sample(
    client: &crate::ch::ClickHouse,
    rpc: &(impl crate::rpc::RpcCall + Sync),
    directory: &Path,
) -> Result<Value> {
    use crate::{
        ch::params,
        proof::{quantity, string},
    };
    let mut observation = json!({});
    let block = rpc.call("eth_getBlockByNumber", json!(["finalized", false]))?;
    let finalized = quantity(string(&block, "number")?, 64)?.to::<u64>();
    observation["rpc_finalized_block"] = json!(finalized);
    observation["rpc_finished_at_unix_ns"] = json!(now_ns()?);
    let durable = directory.join("durable_progress.json");
    if durable.try_exists()? {
        // This is the writer's atomic, checked backup. Full cursor/source
        // validation is repeated after ingestion stops; never retain its token.
        let data = read(&durable)?;
        let position = &data["position"]["block"];
        let number = uint(&position["number"])?;
        observation["durable_block"] = json!(number);
        observation["finalized_lag_blocks"] = json!(finalized as i128 - number as i128);
        let row = client.one("SELECT timestamp FROM state_blocks FINAL WHERE number={n:UInt64} AND hash={hash:String}", &params(json!({"n":number,"hash":position["hash"]}))?)?;
        observation["durable_block_age_seconds"] =
            json!(now_ns()? as f64 / 1e9 - uint(&row["timestamp"])? as f64);
    }
    Ok(observation)
}

pub fn measure(
    client: &crate::ch::ClickHouse,
    rpc: &(impl crate::rpc::RpcCall + Sync),
    options: &ThroughputOptions,
    dsn: &str,
) -> Result<Value> {
    use crate::{
        checkpoint::canonical_accounts,
        cursor,
        ingest::{self, IngestOptions, NativeOptions},
        proof::{quantity, string},
    };
    use std::{
        io::Write,
        sync::{
            atomic::{AtomicBool, Ordering},
            Condvar, Mutex,
        },
        time::{Duration, Instant},
    };
    let root = files::resolve(&options.root)?;
    ensure!(
        options.database == client.database,
        "measurement database differs from its client"
    );
    ensure!(
        !root.join("native").try_exists()? && !root.join("workload.json").try_exists()?,
        "use a fresh native run directory"
    );
    let chain = rpc.call("eth_chainId", json!([]))?;
    ensure!(
        quantity(chain.as_str().context("invalid chain ID")?, 256)?
            == alloy_primitives::U256::from(56),
        "expected the BSC RPC"
    );
    let initial = rpc.call("eth_getBlockByNumber", json!(["finalized", false]))?;
    let finalized = quantity(string(&initial, "number")?, 64)?.to::<u64>();
    let (start, stop) = options.range(finalized)?;
    let accounts = canonical_accounts(&json!(options.accounts))?;
    let native = NativeOptions {
        package: options.package.clone(),
        endpoint: options.endpoint.clone(),
        accounts: json!(accounts),
        start_block: start,
        state_dir: root.join("native"),
        dsn: dsn.into(),
        checkpoint_database: None,
    };
    let ingest_options = IngestOptions {
        stop_block: Some(stop),
        decode_batch_size: options.decode_batch_size,
        spool_max_idle_ms: options.spool_max_idle_ms,
        prometheus_addr: Some("127.0.0.1:0".into()),
        ..Default::default()
    };
    ingest_options.validate(start)?;
    let workload = json!({"format_version":1,"database":client.database,"accounts":accounts,"start_block":start,"stop_block_exclusive":stop,
        "live_blocks_requested":options.live_blocks,"endpoint":options.endpoint,"requested_parallel_workers":null,
        "worker_policy":"provider default; observed session limit is recorded in native log","package_sha256":file_hash(&options.package)?,
        "initial_finalized":{"number":finalized,"hash":initial["hash"]},"sample_interval_seconds":options.interval,"timeout_seconds":options.timeout,
        "decode_batch_size":options.decode_batch_size,"spool_max_idle_ms":options.spool_max_idle_ms,
        "harness_binary_sha256":file_hash(&std::env::current_exe()?)?});
    files::atomic_json(&root.join("workload.json"), &workload, false)?;
    let started = now_ns()?;
    let elapsed = Instant::now();
    let deadline = elapsed
        .checked_add(Duration::from_secs(options.timeout))
        .context("timeout is too large")?;
    let done = (Mutex::new(false), Condvar::new());
    let cancelled = AtomicBool::new(false);
    let (ingested, samples) = std::thread::scope(|scope| -> Result<_> {
        let sampling = scope.spawn(|| -> Result<Vec<Value>> {
            let result = (|| -> Result<Vec<Value>> {
                let mut file = fs::OpenOptions::new()
                    .create_new(true)
                    .write(true)
                    .open(root.join("lag-samples.jsonl"))?;
                let mut samples = Vec::new();
                while !*done.0.lock().unwrap() {
                    let tick = Instant::now();
                    let mut observation = json!({"started_at_unix_ns":now_ns()?});
                    match lag_sample(client, rpc, &native.state_dir) {
                        Ok(value) => observation
                            .as_object_mut()
                            .unwrap()
                            .extend(value.as_object().unwrap().clone()),
                        Err(_) => observation["error_type"] = json!("rpc_or_cursor_sample_failed"),
                    }
                    observation["sample_duration_seconds"] = json!(tick.elapsed().as_secs_f64());
                    observation["finished_at_unix_ns"] = json!(now_ns()?);
                    writeln!(file, "{}", files::canonical_json(&observation)?)?;
                    file.flush()?;
                    file.sync_all()?;
                    samples.push(observation);
                    let pause =
                        Duration::from_secs_f64(options.interval).saturating_sub(tick.elapsed());
                    let _ = done
                        .1
                        .wait_timeout_while(done.0.lock().unwrap(), pause, |finished| !*finished)
                        .unwrap();
                }
                Ok(samples)
            })();
            if result.is_err() {
                cancelled.store(true, Ordering::Relaxed);
            }
            result
        });
        // This direct call keeps the native process inside capacity-run's owned
        // process group. A timeout reaps the child and retains recoverable files.
        let result = ingest::ingest_bounded(
            client,
            &native,
            &ingest_options,
            Some(deadline),
            Some(&cancelled),
        );
        *done.0.lock().unwrap() = true;
        done.1.notify_all();
        let samples = sampling
            .join()
            .map_err(|_| anyhow::anyhow!("RPC sampling thread failed"))??;
        Ok((result?, samples))
    })?;
    let duration = elapsed.elapsed().as_secs_f64();
    let run = read(&native.state_dir.join("run.json"))?;
    for key in [
        "database",
        "accounts",
        "start_block",
        "package_sha256",
        "endpoint",
    ] {
        ensure!(
            run["identity"][key] == workload[key],
            "prepared source differs from the declared workload: {key}"
        );
    }
    let progress = cursor::load_progress(client, &run, &native.state_dir)?;
    ensure!(
        uint(&progress["position"]["block"]["number"])? == stop - 1,
        "native run did not reach its bound"
    );
    let lags = samples
        .iter()
        .filter(|s| s.get("error_type").is_none())
        .filter_map(|s| s["finalized_lag_blocks"].as_f64())
        .collect::<Vec<_>>();
    let mut report = workload;
    report.as_object_mut().unwrap().extend(json!({"run_id":run["run_id"],"module_hash":run["identity"]["module_hash"],
        "started_at_unix_ns":started,"finished_at_unix_ns":now_ns()?,"duration_seconds":duration,"blocks":stop-start,"blocks_per_second":(stop-start) as f64 / duration,
        "final_position":ingested["position"]["block"],"lag_samples":samples.len(),"failed_lag_samples":samples.iter().filter(|s| s.get("error_type").is_some()).count(),
        "finalized_lag_blocks_all_phases":distribution(&lags)?,"database_parts_bytes_after":client.disk_usage()?,
        "limitations":["block-age uses the host wall clock and second-resolution chain timestamps",
            "RPC and durable cursor are sampled sequentially; a negative lag can reflect sampling skew",
            "finalized follow can have native is_live=false; use a declared startup exclusion and observed RPC chain growth",
            "this update interval does not establish full initial storage or publish a checkpoint",
            "cache state is not inferred from elapsed time"]}).as_object().unwrap().clone());
    files::atomic_json(&root.join("result.json"), &report, false)?;
    Ok(report)
}
