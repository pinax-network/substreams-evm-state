//! Guarded native finalized ingestion with immutable source identity and recovery.
use crate::{
    capacity,
    ch::{identifier, params, uint, ClickHouse},
    checkpoint::canonical_accounts,
    control::new_id,
    cursor,
    files::{atomic_json, atomic_write, file_lock, resolve, spaced_json},
    host, process,
    proof::string,
};
use anyhow::{ensure, Context, Result};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use wait_timeout::ChildExt;

pub const MODULE: &str = "map_block_state";

#[derive(Clone)]
pub struct NativeOptions {
    pub package: PathBuf,
    pub endpoint: String,
    pub accounts: Value,
    pub start_block: u64,
    pub state_dir: PathBuf,
    pub dsn: String,
    pub checkpoint_database: Option<String>,
}

#[derive(Clone)]
pub struct IngestOptions {
    pub stop_block: Option<u64>,
    pub max_retries: i64,
    pub decode_batch_size: u32,
    pub spool_max_idle_ms: u64,
    pub prometheus_addr: Option<String>,
    pub parallel_workers: Option<u32>,
}
impl Default for IngestOptions {
    fn default() -> Self {
        Self {
            stop_block: None,
            max_retries: 3,
            decode_batch_size: 1,
            spool_max_idle_ms: 100,
            prometheus_addr: None,
            parallel_workers: None,
        }
    }
}
impl IngestOptions {
    pub fn validate(&self, start: u64) -> Result<()> {
        ensure!(
            self.parallel_workers.is_none_or(|v| v > 0),
            "parallel workers must be a positive integer or omitted"
        );
        ensure!(
            self.stop_block.is_none_or(|stop| stop > start),
            "stop block must be greater than start block (exclusive)"
        );
        ensure!(
            self.decode_batch_size > 0,
            "decode batch size must be a positive integer"
        );
        ensure!(
            self.spool_max_idle_ms > 0,
            "spool maximum idle milliseconds must be a positive integer"
        );
        Ok(())
    }
}

fn native(arguments: &[String], dsn: &str) -> Result<String> {
    let output = process::capture(
        Command::new("substreams")
            .args(arguments)
            .env("SUBSTREAMS_SINK_DSN", dsn),
        Duration::from_secs(120),
    )?
    .context("native Substreams command timed out")?;
    ensure!(
        output.status.success(),
        "native Substreams command failed (status {}); inspect the retained native state",
        process::exit_code(output.status)
    );
    String::from_utf8(output.stdout).context("invalid native Substreams response")
}

pub fn validate_target(client: &ClickHouse, dsn: &str) -> Result<reqwest::Url> {
    let parsed =
        reqwest::Url::parse(dsn).map_err(|_| anyhow::anyhow!("invalid native ClickHouse DSN"))?;
    let http = reqwest::Url::parse(&client.url)
        .map_err(|_| anyhow::anyhow!("invalid ClickHouse HTTP URL"))?;
    let normalize = |host: Option<&str>| {
        host.map(|value| {
            if value == "localhost" {
                "127.0.0.1".to_owned()
            } else {
                value.to_owned()
            }
        })
    };
    ensure!(
        parsed.scheme() == "clickhouse"
            && parsed.path() == format!("/{}", client.database)
            && normalize(parsed.host_str()) == normalize(http.host_str()),
        "native DSN and HTTP client must name the same ClickHouse host and database"
    );
    ensure!(
        http.username().is_empty()
            && http.password().is_none()
            && http.query().is_none()
            && http.fragment().is_none(),
        "use CH_USER/CH_PASSWORD for HTTP credentials"
    );
    Ok(parsed)
}

fn identity(
    client: &ClickHouse,
    options: &NativeOptions,
    directory: &Path,
) -> Result<(Value, Vec<u8>)> {
    let parsed = validate_target(client, &options.dsn)?;
    let selected = canonical_accounts(&options.accounts)?;
    let package = fs::read(&options.package)?;
    let info: Value = serde_json::from_str(&native(
        &[
            "info".into(),
            options
                .package
                .to_str()
                .context("invalid package path")?
                .into(),
            "--json".into(),
            "-p".into(),
            format!("{MODULE}={}", selected.join(",")),
        ],
        &options.dsn,
    )?)?;
    ensure!(
        fs::read(&options.package)? == package,
        "package changed during preparation; retry after the build finishes"
    );
    let module = info["modules"]
        .as_array()
        .context("missing native module metadata")?
        .iter()
        .find(|v| v["name"] == MODULE)
        .context("native block-state module is missing")?;
    ensure!(
        info["network"] == "bsc" && module["output_type"] == "proto:evm.state.v1.BlockState",
        "expected the BSC native block-state package"
    );
    let mut local_host = host::machine_id()?;
    if directory.join("run.json").try_exists()? {
        let previous: Value = serde_json::from_slice(&fs::read(directory.join("run.json"))?)?;
        if host::matches(&previous["identity"], directory)? {
            local_host = string(&previous["identity"], "host")?.into();
        }
    }
    Ok((
        json!({"format_version":3,"database":client.database,"http_url":client.url,
        "checkpoint_database":identifier(options.checkpoint_database.as_deref().unwrap_or(&client.database))?,
        "state_directory":directory.to_str().context("invalid source directory")?,"host":local_host,
        "native_target":format!("{}:{}/{}",parsed.host_str().context("missing native database host")?,parsed.port().unwrap_or(9000),client.database),
        "endpoint":options.endpoint,"accounts":selected,"start_block":options.start_block,"module":MODULE,
        "module_hash":string(module,"hash")?,"package_sha256":hex::encode(Sha256::digest(&package)),
        "network":"bsc","schema_version":1,"final_blocks_only":true}),
        package,
    ))
}

pub(crate) fn prepare_unlocked(
    client: &ClickHouse,
    options: &NativeOptions,
    directory: &Path,
) -> Result<Value> {
    // Reject changed local intent before invoking the native CLI. A changed
    // cohort/range/destination cannot become an external-command retry path.
    let previous_path = directory.join("run.json");
    if previous_path.try_exists()? {
        let previous: Value = serde_json::from_slice(&fs::read(previous_path)?)?;
        let previous = &previous["identity"];
        ensure!(
            previous["database"] == client.database
                && previous["http_url"] == client.url
                && previous["endpoint"] == options.endpoint
                && previous["start_block"] == options.start_block
                && previous["state_directory"]
                    == directory.to_str().context("invalid source directory")?
                && previous["accounts"]
                    == serde_json::to_value(canonical_accounts(&options.accounts)?)?
                && previous["checkpoint_database"]
                    == options
                        .checkpoint_database
                        .as_deref()
                        .unwrap_or(&client.database),
            "run identity changed; use a new isolated database and state directory"
        );
    }
    let (identity, package) = identity(client, options, directory)?;
    let record_path = directory.join("run.json");
    let admin = client.with_database("default")?;
    let query = params(json!({"db":client.database}))?;
    let exists = uint(
        &admin.one(
            "SELECT count() AS n FROM system.databases WHERE name={db:String}",
            &query,
        )?["n"],
    )? != 0;
    let mut record;
    if record_path.try_exists()? {
        record = serde_json::from_slice::<Value>(&fs::read(&record_path)?)?;
        ensure!(
            matches!(record["phase"].as_str(), Some("preparing" | "prepared")),
            "invalid native run phase"
        );
        ensure!(
            record["identity"] == identity,
            "run identity changed; use a new isolated database and state directory"
        );
        ensure!(
            record["phase"] != "prepared" || exists,
            "run database is missing; restore matching database and cursor metadata"
        );
    } else {
        if exists {
            ensure!(
                uint(
                    &client.one(
                        "SELECT count() AS n FROM system.tables WHERE database={db:String}",
                        &query
                    )?["n"]
                )? == 0,
                "database already contains tables without this run's local identity"
            );
        }
        for entry in fs::read_dir(directory)? {
            ensure!(
                entry?.file_name() == "run.lock",
                "state directory is not empty and has no run identity"
            );
        }
        record = json!({"run_id":new_id(),"identity":identity,"phase":"preparing"});
        atomic_json(&record_path, &record, false)?;
    }
    let frozen = directory.join("package.spkg");
    if frozen.try_exists()? {
        ensure!(
            hex::encode(Sha256::digest(fs::read(&frozen)?)) == string(&identity, "package_sha256")?,
            "frozen run package was changed"
        );
    } else {
        ensure!(
            record["phase"] != "prepared",
            "frozen run package is missing"
        );
        atomic_write(&frozen, &package, false)?;
    }
    admin.execute(
        &format!("CREATE DATABASE IF NOT EXISTS {}", client.database),
        &Default::default(),
    )?;
    let db = client.one(
        "SELECT toString(uuid) AS uuid FROM system.databases WHERE name={db:String}",
        &query,
    )?;
    ensure!(
        record
            .get("database_uuid")
            .is_none_or(|value| *value == db["uuid"]),
        "database was replaced; restore matching database and run metadata"
    );
    let owner_exists=uint(&client.one("SELECT count() AS n FROM system.tables WHERE database={db:String} AND name='_evm_state_run'",&query)?["n"])?!=0;
    if !owner_exists {
        ensure!(
            record["phase"] != "prepared",
            "database run identity is missing"
        );
        client.execute("CREATE TABLE _evm_state_run (run_id String, identity String) ENGINE=MergeTree ORDER BY run_id SETTINGS fsync_after_insert=1, fsync_part_directory=1",&Default::default())?;
        client.insert_values(
            "_evm_state_run",
            [json!({"run_id":record["run_id"],"identity":spaced_json(&identity)?})],
        )?;
    }
    let owner = client.one(
        "SELECT run_id,identity FROM _evm_state_run",
        &Default::default(),
    )?;
    ensure!(
        owner["run_id"] == record["run_id"]
            && serde_json::from_str::<Value>(string(&owner, "identity")?)? == identity,
        "database belongs to a different native run"
    );
    let meta = directory.join("meta");
    let metadata = meta.join(format!("{}_schema_hash.txt", client.database));
    if record["phase"] == "preparing" {
        fs::create_dir_all(&meta)?;
        native(
            &[
                "sink".into(),
                "clickhouse".into(),
                "setup".into(),
                frozen.to_str().context("invalid package path")?.into(),
                MODULE.into(),
                "--bytes-encoding".into(),
                "0xhex".into(),
                "--sink-info-folder".into(),
                meta.to_str().context("invalid metadata directory")?.into(),
            ],
            &options.dsn,
        )?;
        for table in ["state_blocks", "_blocks_"] {
            client.execute(&format!("ALTER TABLE {table} MODIFY SETTING fsync_after_insert=1, fsync_part_directory=1"),&Default::default())?;
        }
        fs::File::open(&metadata)?.sync_all()?;
        record["phase"] = json!("prepared");
        record["database_uuid"] = db["uuid"].clone();
        record["schema_hash"] = json!(fs::read_to_string(&metadata)?.trim());
        atomic_json(&record_path, &record, true)?;
    }
    ensure!(
        metadata.is_file()
            && fs::read_to_string(metadata)?.trim() == string(&record, "schema_hash")?,
        "native schema metadata is missing or changed"
    );
    Ok(record)
}

pub fn prepare(client: &ClickHouse, options: &NativeOptions) -> Result<Value> {
    let directory = resolve(&options.state_dir)?;
    capacity::check(client, &[directory.clone()], "native-prepare")?;
    let _lock = file_lock(&directory.join("run.lock"), true, false)?;
    prepare_unlocked(client, options, &directory)
}

pub fn recover_cursor(client: &ClickHouse, options: &NativeOptions) -> Result<Value> {
    let directory = resolve(&options.state_dir)?;
    capacity::check(client, &[directory.clone()], "native-recover")?;
    let _lock = file_lock(&directory.join("run.lock"), true, false)?;
    let run = prepare_unlocked(client, options, &directory)?;
    let progress = cursor::load_progress(client, &run, &directory)?;
    let cursor = directory.join("cursor.txt");
    if cursor.is_file() {
        atomic_write(
            &directory.join(format!("cursor-before-recovery-{}.txt", new_id())),
            &fs::read(&cursor)?,
            false,
        )?;
    }
    atomic_write(&cursor, string(&progress, "cursor")?.as_bytes(), true)?;
    Ok(json!({"run_id":run["run_id"],"recovered":true,"position":progress["position"]}))
}

pub fn command_args(
    options: &NativeOptions,
    ingest: &IngestOptions,
    record: &Value,
    directory: &Path,
) -> Result<Vec<String>> {
    ingest.validate(options.start_block)?;
    let text = |path: PathBuf| {
        path.to_str()
            .map(str::to_owned)
            .context("invalid native state path")
    };
    let accounts = record["identity"]["accounts"]
        .as_array()
        .context("missing native filter")?
        .iter()
        .map(|v| v.as_str().context("invalid native filter"))
        .collect::<Result<Vec<_>>>()?
        .join(",");
    let mut args = vec![
        "sink".into(),
        "clickhouse".into(),
        text(directory.join("package.spkg"))?,
        MODULE.into(),
        "-e".into(),
        options.endpoint.clone(),
        "-p".into(),
        format!("{MODULE}={accounts}"),
        "-s".into(),
        options.start_block.to_string(),
        "--final-blocks-only".into(),
        "--bytes-encoding".into(),
        "0xhex".into(),
        "--sink-info-folder".into(),
        text(directory.join("meta"))?,
        "--cursor-file-path".into(),
        text(directory.join("cursor.txt"))?,
        "--spool-dir".into(),
        text(directory.join("spool"))?,
        "--spool-max-size".into(),
        "1GiB".into(),
        "--spool-max-idle".into(),
        format!("{}ms", ingest.spool_max_idle_ms),
        "--max-retries".into(),
        ingest.max_retries.to_string(),
        "--decode-batch-size".into(),
        ingest.decode_batch_size.to_string(),
    ];
    if let Some(workers) = ingest.parallel_workers {
        args.extend([
            "--header".into(),
            format!("X-Substreams-Parallel-Workers:{workers}"),
        ]);
    }
    if let Some(address) = &ingest.prometheus_addr {
        args.extend(["--prometheus-addr".into(), address.clone()]);
    }
    if let Some(stop) = ingest.stop_block {
        args.extend(["-t".into(), stop.to_string()]);
    }
    Ok(args)
}

pub fn ingest(
    client: &ClickHouse,
    options: &NativeOptions,
    ingest: &IngestOptions,
) -> Result<Value> {
    ingest_bounded(client, options, ingest, None, None)
}

/// Optional qualification deadline/cancellation. The native child remains in
/// its supervisor's process group and is always reaped before returning.
pub fn ingest_bounded(
    client: &ClickHouse,
    options: &NativeOptions,
    ingest: &IngestOptions,
    deadline: Option<Instant>,
    cancelled: Option<&AtomicBool>,
) -> Result<Value> {
    let check_bound = || -> Result<()> {
        ensure!(
            deadline.is_none_or(|end| Instant::now() < end),
            "bounded native ingestion timed out; retain cursor and spool"
        );
        ensure!(
            !cancelled.is_some_and(|flag| flag.load(Ordering::Relaxed)),
            "native qualification cancelled; retain cursor and spool"
        );
        Ok(())
    };
    check_bound()?;
    ingest.validate(options.start_block)?;
    let directory = resolve(&options.state_dir)?;
    let lock = file_lock(&directory.join("run.lock"), true, false)?;
    capacity::check(client, &[directory.clone()], "native-start")?;
    let record = prepare_unlocked(client, options, &directory)?;
    check_bound()?;
    let cursor_path = directory.join("cursor.txt");
    let blocks = uint(
        &client.one(
            "SELECT count() AS n FROM state_blocks FINAL",
            &Default::default(),
        )?["n"],
    )?;
    ensure!(
        blocks == 0
            || cursor_path.is_file() && !fs::read_to_string(&cursor_path)?.trim().is_empty(),
        "native cursor is missing or empty for existing data; explicit recovery is required"
    );
    ensure!(
        blocks != 0 || !cursor_path.try_exists()?,
        "cursor exists without its block data; restore matching database and metadata"
    );
    let mut report = json!({"run_id":record["run_id"],"source":{"database":client.database,"accounts":record["identity"]["accounts"],
        "start_block":options.start_block,"module_hash":record["identity"]["module_hash"],"final_blocks_only":true}});
    if blocks != 0 {
        let progress = cursor::save_progress(
            client,
            &record,
            &directory,
            fs::read_to_string(&cursor_path)?.trim(),
        )?;
        if let Some(stop) = ingest.stop_block {
            let number = uint(&progress["position"]["block"]["number"])?;
            if number >= stop - 1 {
                ensure!(
                    number == stop - 1,
                    "requested stop precedes the already ingested cursor"
                );
                report["already_complete"] = json!(true);
                report["position"] = progress["position"].clone();
                return Ok(report);
            }
        }
    } else {
        ensure!(
            !directory.join("durable_progress.json").try_exists()?,
            "durable progress exists without its block data"
        );
    }
    let mut command = Command::new("substreams");
    command
        .args(command_args(options, ingest, &record, &directory)?)
        .env("SUBSTREAMS_SINK_DSN", &options.dsn);
    lock.inherit_in(&mut command);
    let mut child = process::ChildGuard(command.spawn()?);
    let stopped = Arc::new(AtomicBool::new(false));
    let signals = [signal_hook::consts::SIGINT, signal_hook::consts::SIGTERM]
        .into_iter()
        .map(|signal| signal_hook::flag::register(signal, stopped.clone()))
        .collect::<std::io::Result<Vec<_>>>()?;
    let run = (|| -> Result<()> {
        loop {
            check_bound()?;
            let status = child.wait_timeout(Duration::from_secs(1))?;
            check_bound()?;
            if let Some(status) = status {
                ensure!(
                    status.success(),
                    "native sink exited with status {}; retain its cursor and spool for recovery",
                    process::exit_code(status)
                );
                break;
            }
            ensure!(
                !stopped.load(Ordering::Relaxed),
                "native ingestion interrupted; retain its cursor and spool for recovery"
            );
            cursor::observe(client, &record, &directory)?;
            capacity::check(client, &[directory.clone()], "native-progress")?;
        }
        Ok(())
    })();
    for signal in signals {
        signal_hook::low_level::unregister(signal);
    }
    if child.try_wait()?.is_none() {
        // SAFETY: this PID belongs to our still-running native child.
        unsafe {
            libc::kill(child.id() as i32, libc::SIGTERM);
        }
        if child.wait_timeout(Duration::from_secs(15))?.is_none() {
            child.kill()?;
            child.wait()?;
        }
    }
    // A graceful stop may seal a spool and persist a newer valid cursor.
    let progress = cursor::observe(client, &record, &directory)?;
    run?;
    let progress = progress
        .context("native sink completed without a valid cursor; recover from durable progress")?;
    if let Some(stop) = ingest.stop_block {
        ensure!(
            uint(&progress["position"]["block"]["number"])? == stop - 1,
            "native sink stopped before its requested final block"
        );
    }
    fs::File::open(&cursor_path)?.sync_all()?;
    atomic_write(
        &directory.join("last_completed_cursor.txt"),
        string(&progress, "cursor")?.as_bytes(),
        true,
    )?;
    report["position"] = progress["position"].clone();
    Ok(report)
}
