use anyhow::Result;
use evm_state::{
    ch::{params, ClickHouse},
    control::new_id,
    cursor, files,
    ingest::{self, IngestOptions, NativeOptions},
    source,
};
use serde_json::{json, Value};
use std::{fs, path::PathBuf};

const A: &str = "0x1111111111111111111111111111111111111111";
struct Native {
    client: ClickHouse,
    options: NativeOptions,
    _root: tempfile::TempDir,
}
impl Native {
    fn new() -> Result<Self> {
        let root = tempfile::tempdir()?;
        let database = format!("evm_test_rust_{}", new_id());
        let client = ClickHouse::new(&database)?.with_control_home(root.path().join("control"));
        let options = NativeOptions {
            package: PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../spkg/evm-state-v0.1.0.spkg"),
            endpoint: "http://127.0.0.1:1".into(),
            accounts: json!([A]),
            start_block: 100,
            state_dir: root.path().join("native"),
            dsn: format!(
                "clickhouse://evm_state:local-development-only@localhost:19000/{database}"
            ),
            checkpoint_database: None,
        };
        Ok(Self {
            client,
            options,
            _root: root,
        })
    }
    fn plant(&self, number: u64) -> Result<()> {
        self.client.insert_values("state_blocks",[json!({"number":number,"hash":format!("0x{number:064x}"),"parent_hash":format!("0x{:064x}",number-1),
            "timestamp":1700000000+number,"state_root":format!("0x{:064x}",0),"accounts":A,"schema_version":1,"producer_version":5,
            "_block_number_":number,"_block_timestamp_":1700000000+number,"_version_":1,"_deleted_":false})])?;
        self.client.insert_values("_blocks_",[json!({"number":number,"hash":format!("{number:064x}"),"timestamp":1700000000+number,"version":1,"deleted":false})])?;
        Ok(())
    }
}
impl Drop for Native {
    fn drop(&mut self) {
        if let Ok(admin) = self.client.with_database("default") {
            let _ = admin.execute(
                &format!("DROP DATABASE IF EXISTS {} SYNC", self.client.database),
                &Default::default(),
            );
        }
    }
}

#[test]
fn invalid_worker_flush_and_stop_options_fail_before_state_creation() -> Result<()> {
    let client = ClickHouse::new("evm_test_unused")?;
    let root = tempfile::tempdir()?;
    let directory = root.path().join("not-created");
    let native = NativeOptions {
        package: "missing".into(),
        endpoint: "invalid".into(),
        accounts: json!([]),
        start_block: 100,
        state_dir: directory.clone(),
        dsn: "invalid".into(),
        checkpoint_database: None,
    };
    let defects = [
        IngestOptions {
            parallel_workers: Some(0),
            ..Default::default()
        },
        IngestOptions {
            decode_batch_size: 0,
            ..Default::default()
        },
        IngestOptions {
            spool_max_idle_ms: 0,
            ..Default::default()
        },
        IngestOptions {
            stop_block: Some(100),
            ..Default::default()
        },
    ];
    for defect in defects {
        assert!(ingest::ingest(&client, &native, &defect).is_err());
        assert!(!directory.exists());
    }
    Ok(())
}

#[test]
fn finalized_follow_and_backfill_arguments_preserve_worker_and_flush_settings() -> Result<()> {
    let root = tempfile::tempdir()?;
    let native = NativeOptions {
        package: "package.spkg".into(),
        endpoint: "endpoint:443".into(),
        accounts: json!([A]),
        start_block: 100,
        state_dir: root.path().into(),
        dsn: "unused".into(),
        checkpoint_database: None,
    };
    let record = json!({"identity":{"accounts":[A]}});
    let options = IngestOptions {
        stop_block: Some(200),
        parallel_workers: Some(200),
        prometheus_addr: Some("127.0.0.1:9991".into()),
        ..Default::default()
    };
    let args = ingest::command_args(&native, &options, &record, root.path())?;
    for pair in [
        ["--header", "X-Substreams-Parallel-Workers:200"],
        ["--decode-batch-size", "1"],
        ["--spool-max-idle", "100ms"],
        ["-t", "200"],
    ] {
        assert!(args.windows(2).any(|values| values == pair));
    }
    assert!(args.contains(&"--final-blocks-only".into()));
    assert!(
        !ingest::command_args(&native, &Default::default(), &record, root.path())?
            .contains(&"--header".into())
    );
    Ok(())
}

#[test]
#[ignore = "requires ClickHouse and pinned substreams CLI"]
fn repeated_prepare_is_idempotent_and_filter_range_endpoint_drift_are_rejected() -> Result<()> {
    let db = Native::new()?;
    let first = ingest::prepare(&db.client, &db.options)?;
    assert_eq!(ingest::prepare(&db.client, &db.options)?, first);
    for defect in ["filter", "range", "endpoint", "destination"] {
        let mut options = db.options.clone();
        match defect {
            "filter" => options.accounts = json!(["0x2222222222222222222222222222222222222222"]),
            "range" => options.start_block = 99,
            "endpoint" => options.endpoint = "another.example:443".into(),
            _ => options.checkpoint_database = Some("another".into()),
        }
        let error = ingest::prepare(&db.client, &options).unwrap_err();
        assert!(
            error.to_string().contains("identity changed"),
            "{defect}: {error:#}"
        );
    }
    Ok(())
}

#[test]
#[ignore = "requires ClickHouse and pinned substreams CLI"]
fn old_rows_require_explicit_cursor_recovery_and_completed_bounds_are_idempotent() -> Result<()> {
    let db = Native::new()?;
    let run = ingest::prepare(&db.client, &db.options)?;
    db.plant(100)?;
    let options = IngestOptions {
        stop_block: Some(101),
        ..Default::default()
    };
    assert!(ingest::ingest(&db.client, &db.options, &options)
        .unwrap_err()
        .to_string()
        .contains("cursor is missing"));
    let vectors: Value =
        serde_json::from_str(include_str!("../../../tests/fixtures/cursors.json"))?;
    let token = vectors["1"]["100"].as_str().unwrap();
    let saved = cursor::save_progress(&db.client, &run, &db.options.state_dir, token)?;
    for damage in ["", "truncated"] {
        fs::write(db.options.state_dir.join("cursor.txt"), damage)?;
        let recovered = ingest::recover_cursor(&db.client, &db.options)?;
        assert_eq!(recovered["position"], saved["position"]);
        assert_eq!(
            fs::read_to_string(db.options.state_dir.join("cursor.txt"))?,
            token
        );
        assert_eq!(
            ingest::ingest(&db.client, &db.options, &options)?["already_complete"],
            true
        );
    }
    Ok(())
}

#[test]
#[ignore = "requires ClickHouse and pinned substreams CLI"]
fn cursor_without_rows_and_durable_progress_without_rows_cannot_restart() -> Result<()> {
    let db = Native::new()?;
    ingest::prepare(&db.client, &db.options)?;
    fs::write(db.options.state_dir.join("cursor.txt"), "stale")?;
    assert!(ingest::ingest(&db.client, &db.options, &Default::default())
        .unwrap_err()
        .to_string()
        .contains("without its block data"));
    fs::remove_file(db.options.state_dir.join("cursor.txt"))?;
    fs::write(db.options.state_dir.join("durable_progress.json"), "{}")?;
    assert!(ingest::ingest(&db.client, &db.options, &Default::default())
        .unwrap_err()
        .to_string()
        .contains("durable progress exists"));
    Ok(())
}

#[test]
#[ignore = "requires ClickHouse and pinned substreams CLI"]
fn changed_schema_package_owner_database_or_phase_is_rejected() -> Result<()> {
    for defect in ["schema", "package", "owner", "database", "phase"] {
        let db = Native::new()?;
        ingest::prepare(&db.client, &db.options)?;
        match defect {
            "schema" => fs::remove_file(
                db.options
                    .state_dir
                    .join("meta")
                    .join(format!("{}_schema_hash.txt", db.client.database)),
            )?,
            "package" => fs::write(
                db.options.state_dir.join("package.spkg"),
                b"changed package",
            )?,
            "owner" => {
                db.client
                    .execute("DROP TABLE _evm_state_run SYNC", &Default::default())?;
            }
            "database" => {
                let admin = db.client.with_database("default")?;
                admin.execute(
                    &format!("DROP DATABASE {} SYNC", db.client.database),
                    &Default::default(),
                )?;
                admin.execute(
                    &format!("CREATE DATABASE {}", db.client.database),
                    &Default::default(),
                )?;
            }
            _ => {
                let path = db.options.state_dir.join("run.json");
                let mut record: Value = serde_json::from_slice(&fs::read(&path)?)?;
                record["phase"] = json!("corrupt");
                files::atomic_json(&path, &record, true)?;
            }
        }
        assert!(
            ingest::prepare(&db.client, &db.options).is_err(),
            "{defect}"
        );
    }
    Ok(())
}

#[test]
#[ignore = "requires ClickHouse and pinned substreams CLI"]
fn second_directory_or_writer_cannot_claim_a_native_run() -> Result<()> {
    let db = Native::new()?;
    ingest::prepare(&db.client, &db.options)?;
    let mut other = db.options.clone();
    other.state_dir = db.options.state_dir.with_file_name("other");
    assert!(ingest::prepare(&db.client, &other)
        .unwrap_err()
        .to_string()
        .contains("already contains tables"));
    let _lock = files::file_lock(&db.options.state_dir.join("run.lock"), true, false)?;
    assert!(ingest::prepare(&db.client, &db.options)
        .unwrap_err()
        .to_string()
        .contains("another process"));
    Ok(())
}

#[test]
#[ignore = "requires ClickHouse and pinned substreams CLI"]
fn verified_source_requires_exact_destination_ownership_package_and_durable_target() -> Result<()> {
    let db = Native::new()?;
    let run = ingest::prepare(&db.client, &db.options)?;
    db.plant(100)?;
    let vectors: Value =
        serde_json::from_str(include_str!("../../../tests/fixtures/cursors.json"))?;
    cursor::save_progress(
        &db.client,
        &run,
        &db.options.state_dir,
        vectors["1"]["100"].as_str().unwrap(),
    )?;
    let source = json!({"database":db.client.database,"accounts":[A],"start_block":100,"module_hash":run["identity"]["module_hash"],"final_blocks_only":true});
    let verified = source::verified_source(&db.client, &source, Some(&db.client), Some(100))?;
    assert_eq!(verified.source["run_id"], run["run_id"]);
    drop(verified);
    assert!(source::verified_source(&db.client, &source, Some(&db.client), Some(101)).is_err());
    assert!(source::verified_source(
        &db.client,
        &source,
        Some(&db.client.with_database("wrong_destination")?),
        Some(100)
    )
    .is_err());
    for (key, value) in [
        (
            "accounts",
            json!(["0x2222222222222222222222222222222222222222"]),
        ),
        ("start_block", json!(99)),
        ("module_hash", json!("f".repeat(40))),
        ("final_blocks_only", json!(false)),
        ("database_uuid", json!(new_id())),
    ] {
        let mut changed = source.clone();
        changed[key] = value;
        assert!(
            source::verified_source(&db.client, &changed, None, Some(100)).is_err(),
            "{key}"
        );
    }
    fs::write(db.options.state_dir.join("package.spkg"), b"changed")?;
    assert!(source::verified_source(&db.client, &source, None, Some(100)).is_err());
    assert_eq!(
        db.client
            .one("SELECT run_id FROM _evm_state_run", &params(json!({}))?)?["run_id"],
        run["run_id"]
    );
    Ok(())
}

fn proof_fixture() -> Result<Value> {
    Ok(
        serde_json::from_str::<Value>(include_str!("../../../tests/fixtures/proof-parity.json"))?
            ["account"]
            .clone(),
    )
}

fn bundle(number: u64) -> Result<Value> {
    let fixture = proof_fixture()?;
    Ok(
        json!({"format_version":1,"chain_id":56,"header":{"number":number,"hash":format!("0x{number:064x}"),"parent_hash":format!("0x{:064x}",number-1),
        "state_root":fixture["state_root"],"timestamp":1700000000+number},"header_trust":"provider-finalized-header",
        "accounts":{A:{"proof":fixture["proof"],"code":fixture["code"]}}}),
    )
}

fn state_block(number: u64) -> Result<Value> {
    Ok(
        json!({"number":number,"hash":format!("0x{number:064x}"),"parent_hash":format!("0x{:064x}",number-1),"state_root":proof_fixture()?["state_root"],
        "timestamp":1700000000+number,"accounts":A,"schema_version":1,"producer_version":5,"_block_number_":number,"_block_timestamp_":1700000000+number,"_version_":1,"_deleted_":false}),
    )
}

fn storage(row: &mut Value, slots: &[(String, String, u64)]) {
    row["storage.address"] = json!(slots.iter().map(|_| A).collect::<Vec<_>>());
    row["storage.slot"] = json!(slots.iter().map(|s| &s.0).collect::<Vec<_>>());
    row["storage.value"] = json!(slots.iter().map(|s| &s.1).collect::<Vec<_>>());
    row["storage.ordinal"] = json!(slots.iter().map(|s| s.2).collect::<Vec<_>>());
}

fn publish_source(db: &Native, run: &Value, rows: Vec<Value>, start: u64) -> Result<Value> {
    let number = rows.last().unwrap()["number"].as_u64().unwrap();
    db.client.insert_values("state_blocks", rows.clone())?;
    db.client.insert_values("_blocks_",rows.iter().map(|row|json!({"number":row["number"],"hash":row["hash"].as_str().unwrap().strip_prefix("0x").unwrap(),
        "timestamp":row["timestamp"],"version":1,"deleted":false})))?;
    let vectors: Value =
        serde_json::from_str(include_str!("../../../tests/fixtures/cursors.json"))?;
    cursor::save_progress(
        &db.client,
        run,
        &db.options.state_dir,
        vectors["1"][number.to_string()].as_str().unwrap(),
    )?;
    Ok(
        json!({"database":db.client.database,"accounts":[A],"start_block":start,"module_hash":run["identity"]["module_hash"],"final_blocks_only":true}),
    )
}

fn fixture_rows() -> Result<Vec<Value>> {
    let fixture = proof_fixture()?;
    let slots = fixture["slots"].as_array().unwrap();
    let mut first = state_block(100)?;
    storage(
        &mut first,
        &[
            (
                slots[0][0].as_str().unwrap().into(),
                format!("0x{:064x}", 1),
                1,
            ),
            (
                slots[1][0].as_str().unwrap().into(),
                slots[1][1].as_str().unwrap().into(),
                2,
            ),
            (format!("0x{:064x}", 2), format!("0x{:064x}", 123), 3),
        ],
    );
    let mut second = state_block(101)?;
    storage(
        &mut second,
        &[
            (
                slots[0][0].as_str().unwrap().into(),
                slots[0][1].as_str().unwrap().into(),
                1,
            ),
            (format!("0x{:064x}", 2), format!("0x{:064x}", 0), 2),
        ],
    );
    Ok(vec![first, second, state_block(102)?])
}

#[test]
#[ignore = "requires ClickHouse and pinned substreams CLI"]
fn complete_checkpoint_reduces_updates_and_zeroes_then_proves_every_slot() -> Result<()> {
    let db = Native::new()?;
    let run = ingest::prepare(&db.client, &db.options)?;
    let source = publish_source(&db, &run, fixture_rows()?, 100)?;
    let record = evm_state::checkpoint::build(
        &db.client,
        &bundle(102)?,
        &[source],
        None,
        100_000_000_000,
        &db._root.path().join("work"),
    )?;
    assert_eq!(record["nonzero_slots"], 2);
    assert_eq!(record["verification"][A]["storage_root"], "verified");
    let rows=db.client.rows("SELECT slot,value FROM checkpoint_storage FINAL WHERE snapshot_id={id:String} ORDER BY slot",&params(json!({"id":record["snapshot_id"]}))?)?.collect::<Result<Vec<_>>>()?;
    let fixture = proof_fixture()?;
    assert_eq!(
        rows,
        fixture["slots"]
            .as_array()
            .unwrap()
            .iter()
            .map(|pair| json!({"slot":pair[0],"value":pair[1]}))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        evm_state::checkpoint::manifest(&db.client, record["snapshot_id"].as_str().unwrap())?,
        record
    );
    Ok(())
}

#[test]
#[ignore = "requires ClickHouse and pinned substreams CLI"]
fn wrong_slot_missing_slot_metadata_gap_filter_and_extra_account_cannot_publish() -> Result<()> {
    for defect in [
        "slot", "missing", "nonce", "code", "gap", "parent", "filter", "extra",
    ] {
        let db = Native::new()?;
        let run = ingest::prepare(&db.client, &db.options)?;
        let mut rows = fixture_rows()?;
        match defect {
            "slot" => rows[1]["storage.value"][0] = json!(format!("0x{:064x}", 11)),
            "missing" => rows[0]["storage.value"][1] = json!(format!("0x{:064x}", 0)),
            "nonce" => {
                rows[2]["nonces.address"] = json!([A]);
                rows[2]["nonces.value"] = json!([4]);
                rows[2]["nonces.ordinal"] = json!([10]);
            }
            "code" => {
                rows[2]["codes.address"] = json!([A]);
                rows[2]["codes.code"] = json!(["0x6001"]);
                rows[2]["codes.hash"] = json!([format!(
                    "0x{}",
                    hex::encode(alloy_primitives::keccak256([0x60, 0x01]))
                )]);
                rows[2]["codes.ordinal"] = json!([10]);
            }
            "gap" => {
                rows.remove(1);
            }
            "parent" => rows[1]["parent_hash"] = json!(format!("0x{:064x}", 0)),
            "filter" => rows[1]["accounts"] = json!("0x2222222222222222222222222222222222222222"),
            _ => {
                rows[2]["storage.address"] = json!(["0x2222222222222222222222222222222222222222"]);
                rows[2]["storage.slot"] = json!([format!("0x{:064x}", 1)]);
                rows[2]["storage.value"] = json!([format!("0x{:064x}", 2)]);
                rows[2]["storage.ordinal"] = json!([1]);
            }
        }
        let source = publish_source(&db, &run, rows, 100)?;
        assert!(
            evm_state::checkpoint::build(
                &db.client,
                &bundle(102)?,
                &[source],
                None,
                100_000_000_000,
                &db._root.path().join("work")
            )
            .is_err(),
            "{defect}"
        );
        assert_eq!(
            evm_state::ch::uint(
                &db.client.one(
                    "SELECT count() AS n FROM checkpoints FINAL",
                    &Default::default()
                )?["n"]
            )?,
            0,
            "{defect}"
        );
    }
    Ok(())
}

#[test]
#[ignore = "requires ClickHouse and pinned substreams CLI"]
fn lifecycle_reset_cannot_resurrect_older_base_slots_or_replace_a_ready_checkpoint() -> Result<()> {
    let db = Native::new()?;
    let run = ingest::prepare(&db.client, &db.options)?;
    let source = publish_source(&db, &run, fixture_rows()?, 100)?;
    let first = evm_state::checkpoint::build(
        &db.client,
        &bundle(102)?,
        &[source],
        None,
        100_000_000_000,
        &db._root.path().join("work"),
    )?;
    let fixture = proof_fixture()?;
    let mut update = state_block(103)?;
    update["lifecycle.address"] = json!([A]);
    update["lifecycle.kind"] = json!(["storage_reset"]);
    update["lifecycle.ordinal"] = json!([5]);
    storage(
        &mut update,
        &[(
            fixture["slots"][0][0].as_str().unwrap().into(),
            fixture["slots"][0][1].as_str().unwrap().into(),
            6,
        )],
    );
    let source = publish_source(&db, &run, vec![update], 103)?;
    // The old proof contains two slots. Ignoring the reset would resurrect the
    // second slot and incorrectly make this deliberately stale proof pass.
    assert!(evm_state::checkpoint::build(
        &db.client,
        &bundle(103)?,
        &[source],
        first["snapshot_id"].as_str(),
        100_000_000_000,
        &db._root.path().join("work")
    )
    .is_err());
    assert_eq!(
        evm_state::checkpoint::manifest(&db.client, first["snapshot_id"].as_str().unwrap())?,
        first
    );
    assert_eq!(
        evm_state::ch::uint(
            &db.client.one(
                "SELECT count() AS n FROM checkpoints FINAL",
                &Default::default()
            )?["n"]
        )?,
        1
    );
    Ok(())
}
