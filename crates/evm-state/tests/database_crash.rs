//! Hard-restart only the newly created container owned by this test. Never use
//! the configured development database as a crash target.
use alloy_primitives::U256;
use anyhow::{ensure, Context, Result};
use evm_state::{
    ch::{uint, ClickHouse},
    checkpoint,
    control::new_id,
    files,
    ingest::{self, IngestOptions, NativeOptions},
    native_stream::{NativeStream, StreamOptions},
    process,
    proof::string,
    reader, retention,
    synthetic::{self, word, Account},
};
use serde_json::json;
use std::{
    collections::BTreeMap,
    fs,
    net::TcpListener,
    path::PathBuf,
    process::Command,
    time::{Duration, Instant},
};

const A: &str = "0x1111111111111111111111111111111111111111";
const BUDGET: u64 = 100_000_000_000;
struct OwnedContainer {
    name: String,
}
impl OwnedContainer {
    fn docker(&self, args: &[&str]) -> Result<String> {
        let mut command = Command::new("docker");
        command.args(args).arg(&self.name);
        let result = process::capture(&mut command, Duration::from_secs(60))?
            .context("owned container operation timed out")?;
        ensure!(
            result.status.success(),
            "owned container operation failed: {}",
            args.join(" ")
        );
        Ok(String::from_utf8(result.stdout)?)
    }
}
impl Drop for OwnedContainer {
    fn drop(&mut self) {
        let _ = self.docker(&["rm", "-f", "-v"]);
    }
}
fn wait_database(admin: &ClickHouse) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(45);
    loop {
        if admin
            .one("SELECT 1 AS n", &Default::default())
            .is_ok_and(|v| v["n"] == 1)
        {
            return Ok(());
        }
        ensure!(
            Instant::now() < deadline,
            "new crash-test database did not become ready"
        );
        std::thread::sleep(Duration::from_millis(200));
    }
}

#[test]
#[ignore = "requires Docker space for a NEW ClickHouse container and pinned substreams CLI"]
fn database_hard_restart_preserves_publication_and_cursor_recovery_boundaries() -> Result<()> {
    let owned = OwnedContainer {
        name: format!("evm-rust-crash-{}", new_id()),
    };
    // Keep both leases until the fixed ports have been selected. Explicit ports
    // preserve the run endpoint identity across Docker stop/start.
    let http_lease = TcpListener::bind("127.0.0.1:0")?;
    let native_lease = TcpListener::bind("127.0.0.1:0")?;
    let http = http_lease.local_addr()?.port();
    let native = native_lease.local_addr()?.port();
    drop((http_lease, native_lease));
    let mut create = Command::new("docker");
    create.args([
        "run",
        "-d",
        "--name",
        &owned.name,
        "--memory",
        "2g",
        "--cpus",
        "2",
        "-p",
        &format!("127.0.0.1:{http}:8123"),
        "-p",
        &format!("127.0.0.1:{native}:9000"),
        "-e",
        "CLICKHOUSE_USER=evm_state",
        "-e",
        "CLICKHOUSE_PASSWORD=local-development-only",
        "-e",
        "CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1",
        "clickhouse/clickhouse-server:26.3.33.24",
    ]);
    let created = process::capture(&mut create, Duration::from_secs(60))?
        .context("new crash-test container creation timed out")?;
    ensure!(
        created.status.success(),
        "cannot create the isolated crash-test container"
    );
    let root = tempfile::tempdir()?;
    let admin = ClickHouse::configured(
        "default",
        &format!("http://127.0.0.1:{http}"),
        "evm_state",
        "local-development-only",
    )?
    .with_control_home(root.path().join("control"));
    wait_database(&admin)?;
    let client = admin.with_database("source")?;
    let target = admin.with_database("checkpoints")?;
    let mut account = Account {
        nonce: 3,
        slots: BTreeMap::from([(word(1), U256::from(7)), (word(2), U256::from(8))]),
        ..Default::default()
    };
    let mut accounts = BTreeMap::from([(A.to_string(), account.clone())]);
    let b100 = synthetic::bundle(100, &accounts, None)?;
    let row100 = synthetic::block(
        &b100,
        &[
            (A.into(), word(1), U256::from(7)),
            (A.into(), word(2), U256::from(8)),
        ],
        true,
    )?;
    account.slots = BTreeMap::from([(word(2), U256::from(9))]);
    accounts.insert(A.into(), account.clone());
    let first_bundle = synthetic::bundle(101, &accounts, Some(string(&b100["header"], "hash")?))?;
    let row101 = synthetic::block(
        &first_bundle,
        &[
            (A.into(), word(1), U256::ZERO),
            (A.into(), word(2), U256::from(9)),
        ],
        false,
    )?;
    account.balance = U256::from(50);
    accounts.insert(A.into(), account);
    let b102 = synthetic::bundle(
        102,
        &accounts,
        Some(string(&first_bundle["header"], "hash")?),
    )?;
    let row102 = synthetic::block(&b102, &[], true)?;
    let latest = synthetic::bundle(103, &accounts, Some(string(&b102["header"], "hash")?))?;
    let package =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../spkg/evm-state-v0.1.0.spkg");
    let stream = NativeStream::new(
        &package,
        &[
            row100,
            row101,
            row102,
            synthetic::block(&latest, &[], false)?,
        ],
        StreamOptions::default(),
    )?;
    let options = NativeOptions {
        package,
        endpoint: stream.endpoint.clone(),
        accounts: json!([A]),
        start_block: 100,
        state_dir: root.path().join("native"),
        dsn: format!("clickhouse://evm_state:local-development-only@127.0.0.1:{native}/source"),
        checkpoint_database: Some(target.database.clone()),
    };
    let mut native_options = IngestOptions {
        stop_block: Some(102),
        decode_batch_size: 1,
        max_retries: 0,
        prometheus_addr: Some("127.0.0.1:0".into()),
        ..Default::default()
    };
    let run = ingest::ingest(&client, &options, &native_options)?;
    let source = run["source"].clone();
    let work = root.path().join("work");
    let error = checkpoint::build_observed(
        &target,
        &first_bundle,
        &[source.clone()],
        None,
        BUDGET,
        &work,
        &|| {
            owned.docker(&["kill", "--signal", "KILL"])?;
            anyhow::bail!("database stopped before ready publication")
        },
    )
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("database stopped before ready publication"),
        "{error:#}"
    );
    owned.docker(&["start"])?;
    wait_database(&admin)?;
    assert_eq!(
        uint(&target.one("SELECT count() AS n FROM checkpoints", &Default::default())?["n"])?,
        0
    );
    assert!(!retention::prune(&target, 1)?["unpublished_candidates"]
        .as_array()
        .unwrap()
        .is_empty());
    let first = checkpoint::build(
        &target,
        &first_bundle,
        &[source.clone()],
        None,
        BUDGET,
        &work,
    )?;
    let first_id = string(&first, "snapshot_id")?;
    let pinned = reader::pin(&target, first_id, "database-restart-reader")?;
    let pin = string(&pinned, "pin_id")?;
    let before = reader::page(&target, pin, A, None, 1000)?;

    // Second hard restart is after acknowledged ready publication. Corrupt the
    // native cursor and require exact recovery from the durable local backup.
    owned.docker(&["kill", "--signal", "KILL"])?;
    files::atomic_write(
        &options.state_dir.join("cursor.txt"),
        b"damaged native cursor",
        true,
    )?;
    owned.docker(&["start"])?;
    wait_database(&admin)?;
    assert_eq!(reader::page(&target, pin, A, None, 1000)?, before);
    assert_eq!(
        checkpoint::read_account(&target, first_id, A)?["nonzero_slots"],
        1
    );
    assert_eq!(
        ingest::recover_cursor(&client, &options)?["position"]["block"]["number"],
        101
    );
    native_options.stop_block = Some(104);
    ingest::ingest(&client, &options, &native_options)?;
    let mut continued = source;
    continued["start_block"] = json!(102);
    let second = checkpoint::build(
        &target,
        &latest,
        &[continued],
        Some(first_id),
        BUDGET,
        &work,
    )?;
    assert_eq!(
        checkpoint::read_account(&target, first_id, A)?["balance"],
        "25"
    );
    assert_eq!(
        checkpoint::read_account(&target, string(&second, "snapshot_id")?, A)?["balance"],
        "50"
    );
    assert_eq!(reader::page(&target, pin, A, None, 1000)?, before);
    reader::unpin(&target, pin)?;
    assert!(fs::read_dir(&options.state_dir)?.flatten().any(|e| e
        .file_name()
        .to_string_lossy()
        .starts_with("cursor-before-recovery-")));
    assert!(stream.errors().is_empty(), "{:?}", stream.errors());
    Ok(())
}
