//! Native schema integration, using independent storage/account proofs and
//! encoded headers. Every database and controller belongs to this test only.
use anyhow::Result;
use evm_state::{
    bootstrap,
    ch::{params, uint, ClickHouse},
    checkpoint,
    control::new_id,
    cursor, files, history,
    ingest::{self, IngestOptions, NativeOptions},
    proof, reader, retention,
};
use serde_json::{json, Value};
use std::{fs, path::PathBuf};

const A: &str = "0x1111111111111111111111111111111111111111";
const BUDGET: u64 = 100_000_000_000;
fn fixture() -> Result<Value> {
    Ok(serde_json::from_str(include_str!(
        "../../../tests/fixtures/proof-parity.json"
    ))?)
}
fn word(n: u64) -> String {
    format!("0x{n:064x}")
}
fn quantity(n: u64) -> Vec<u8> {
    let bytes = n.to_be_bytes();
    bytes[bytes.iter().position(|b| *b != 0).unwrap_or(8)..].to_vec()
}
fn header(number: u64) -> Result<(Value, String)> {
    let f = fixture()?;
    let encoded = proof::unhex(f["headers"][0]["encoded"].as_str().unwrap())?;
    let mut input = encoded.as_slice();
    let mut payload = alloy_rlp::Header::decode_bytes(&mut input, true)?;
    let mut fields = Vec::new();
    while !payload.is_empty() {
        fields.push(alloy_rlp::Header::decode_bytes(&mut payload, false)?.to_vec());
    }
    fields[3] = proof::unhex(f["account"]["state_root"].as_str().unwrap())?;
    let mut parent = word(99);
    let mut result = None;
    for n in 100..=number {
        let timestamp = 1_700_000_000 + (n - 100) * 32 * 86400;
        fields[0] = proof::unhex(&parent)?;
        fields[8] = quantity(n);
        fields[11] = quantity(timestamp);
        let encoded = proof::rlp_list(
            &fields
                .iter()
                .map(|f| alloy_rlp::encode(f.as_slice()))
                .collect::<Vec<_>>(),
        );
        let hash = format!("0x{}", hex::encode(alloy_primitives::keccak256(&encoded)));
        result = Some((
            json!({"number":n,"hash":hash,"parent_hash":parent,"state_root":f["account"]["state_root"],"timestamp":timestamp}),
            format!("0x{}", hex::encode(encoded)),
        ));
        parent = hash;
    }
    Ok(result.unwrap())
}
fn bundle(number: u64) -> Result<Value> {
    let (header, rlp) = header(number)?;
    let f = fixture()?;
    Ok(
        json!({"format_version":1,"chain_id":56,"header":header,"header_rlp":rlp,"header_trust":"operator-pinned-hash","accounts":{A:{"proof":f["account"]["proof"],"code":f["account"]["code"]}}}),
    )
}
fn block(n: u64, slots: &[(String, String, u64)]) -> Result<Value> {
    let (mut row, _) = header(n)?;
    row["accounts"] = json!(A);
    row["schema_version"] = json!(1);
    row["producer_version"] = json!(5);
    row["_block_number_"] = json!(n);
    row["_block_timestamp_"] = row["timestamp"].clone();
    row["_version_"] = json!(1);
    row["_deleted_"] = json!(false);
    row["storage.address"] = json!(slots.iter().map(|_| A).collect::<Vec<_>>());
    row["storage.slot"] = json!(slots.iter().map(|s| &s.0).collect::<Vec<_>>());
    row["storage.value"] = json!(slots.iter().map(|s| &s.1).collect::<Vec<_>>());
    row["storage.ordinal"] = json!(slots.iter().map(|s| s.2).collect::<Vec<_>>());
    Ok(row)
}
fn slots() -> Result<Vec<(String, String, u64)>> {
    Ok(fixture()?["account"]["slots"]
        .as_array()
        .unwrap()
        .iter()
        .enumerate()
        .map(|(i, p)| {
            (
                p[0].as_str().unwrap().into(),
                p[1].as_str().unwrap().into(),
                i as u64 + 1,
            )
        })
        .collect())
}
struct Native {
    client: ClickHouse,
    target: ClickHouse,
    options: NativeOptions,
    run: Value,
    root: tempfile::TempDir,
}
impl Native {
    fn new() -> Result<Self> {
        Self::at("http://127.0.0.1:1")
    }
    fn at(endpoint: &str) -> Result<Self> {
        let root = tempfile::tempdir()?;
        let client = ClickHouse::new(&format!("evm_test_rust_{}", new_id()))?
            .with_control_home(root.path().join("control"));
        let target = client.with_database(&format!("evm_test_rust_{}", new_id()))?;
        let options = NativeOptions {
            package: PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../spkg/evm-state-v0.1.0.spkg"),
            endpoint: endpoint.into(),
            accounts: json!([A]),
            start_block: 100,
            state_dir: root.path().join("native"),
            dsn: format!(
                "clickhouse://evm_state:local-development-only@localhost:19000/{}",
                client.database
            ),
            checkpoint_database: Some(target.database.clone()),
        };
        let mut test = Self {
            client,
            target,
            options,
            run: Value::Null,
            root,
        };
        test.run = ingest::prepare(&test.client, &test.options)?;
        checkpoint::setup(&test.target)?;
        Ok(test)
    }
    fn insert(&self, rows: Vec<Value>) -> Result<()> {
        let last = rows.last().unwrap().clone();
        self.client.insert_values("state_blocks", rows.clone())?;
        self.client.insert_values("_blocks_",rows.iter().map(|r|json!({"number":r["number"],"hash":r["hash"].as_str().unwrap().trim_start_matches("0x"),"timestamp":r["timestamp"],"version":1,"deleted":false})))?;
        let n = uint(&last["number"])?;
        let hash = last["hash"].as_str().unwrap().trim_start_matches("0x");
        let token = cursor::encode_public(&format!("c2:1:{n}:{hash}:{n}:{hash}"))?;
        files::atomic_write(
            &self.options.state_dir.join("cursor.txt"),
            token.as_bytes(),
            true,
        )?;
        cursor::save_progress(&self.client, &self.run, &self.options.state_dir, &token)?;
        Ok(())
    }
    fn initial(&self) -> Result<()> {
        self.insert(vec![block(100, &slots()?)?, block(101, &[])?])
    }
    fn compact(&self) -> Result<Value> {
        bootstrap::compact(&self.client, &self.options.state_dir, None, BUDGET)
    }
    fn source(&self, start: u64) -> Value {
        json!({"database":self.client.database,"accounts":[A],"start_block":start,"module_hash":self.run["identity"]["module_hash"],"final_blocks_only":true})
    }
    fn publish(&self, n: u64, start: u64, base: Option<&str>) -> Result<Value> {
        checkpoint::build(
            &self.target,
            &bundle(n)?,
            &[self.source(start)],
            base,
            BUDGET,
            &self.root.path().join("work"),
        )
    }
    fn numbers(&self, table: &str) -> Result<Vec<u64>> {
        self.client
            .rows(
                &format!("SELECT number FROM {table} FINAL ORDER BY number"),
                &Default::default(),
            )?
            .map(|r| uint(&r?["number"]))
            .collect()
    }
    fn prefix_slots(&self, p: &Value) -> Result<Vec<Value>> {
        self.client.rows("SELECT slot,value FROM bootstrap_storage WHERE generation={id:String} ORDER BY slot",&params(json!({"id":p["generation"]}))?)?.collect()
    }
}
impl Drop for Native {
    fn drop(&mut self) {
        if let Ok(admin) = self.client.with_database("default") {
            for database in [&self.client.database, &self.target.database] {
                let _ = admin.execute(
                    &format!("DROP DATABASE IF EXISTS {database} SYNC"),
                    &Default::default(),
                );
            }
        }
    }
}

#[test]
fn invalid_replay_and_retention_limits_fail_before_creating_state() -> Result<()> {
    let root = tempfile::tempdir()?;
    let directory = root.path().join("missing");
    let client = ClickHouse::new("evm_test_unused")?;
    assert!(history::cleanup(&client, &directory, "invalid", 0, true).is_err());
    assert!(bootstrap::compact(&client, &directory, None, 0).is_err());
    let options = NativeOptions {
        package: "missing".into(),
        endpoint: "invalid".into(),
        accounts: json!([]),
        start_block: 100,
        state_dir: directory.clone(),
        dsn: "invalid".into(),
        checkpoint_database: None,
    };
    for (stop, chunk, budget) in [(100, 10, 10), (101, 0, 10), (101, 10, 0)] {
        assert!(bootstrap::replay(
            &client,
            &options,
            &IngestOptions::default(),
            stop,
            chunk,
            budget
        )
        .is_err());
    }
    assert!(!directory.exists());
    Ok(())
}

#[test]
#[ignore = "requires ClickHouse and pinned substreams CLI"]
fn repeated_compaction_clears_slots_and_matches_full_proven_replay() -> Result<()> {
    let db = Native::new()?;
    let expected = slots()?;
    let mut initial = expected.clone();
    initial.push((word(2), word(123), 3));
    db.insert(vec![block(100, &initial)?, block(101, &[])?])?;
    let first = db.compact()?;
    assert_eq!(first["nonzero_slots"], 3);
    assert_eq!(db.numbers("state_blocks")?, vec![101]);
    assert_eq!(db.numbers("_blocks_")?, vec![101]);
    db.insert(vec![block(102, &[(word(2), word(0), 1)])?])?;
    let second = db.compact()?;
    assert_eq!(second["nonzero_slots"], 2);
    assert_ne!(first["generation"], second["generation"]);
    assert_eq!(db.compact()?["already_compacted"], true);
    db.insert(vec![block(103, &[])?])?;
    let ready = db.publish(103, 100, None)?;
    let plain = Native::new()?;
    plain.insert(vec![
        block(100, &initial)?,
        block(101, &[])?,
        block(102, &[(word(2), word(0), 1)])?,
        block(103, &[])?,
    ])?;
    assert_eq!(
        ready["state_sha256"],
        plain.publish(103, 100, None)?["state_sha256"]
    );
    assert!(db
        .compact()
        .unwrap_err()
        .to_string()
        .contains("already have a checkpoint"));
    assert_eq!(
        checkpoint::manifest(&db.target, ready["snapshot_id"].as_str().unwrap())?,
        ready
    );
    Ok(())
}

#[test]
#[ignore = "requires ClickHouse and pinned substreams CLI"]
fn lifecycle_reset_removes_untouched_prefix_and_old_proof_cannot_publish() -> Result<()> {
    let db = Native::new()?;
    db.initial()?;
    db.compact()?;
    let expected = slots()?;
    let mut row = block(102, &[(expected[0].0.clone(), expected[0].1.clone(), 6)])?;
    row["lifecycle.address"] = json!([A]);
    row["lifecycle.kind"] = json!(["storage_reset"]);
    row["lifecycle.ordinal"] = json!([5]);
    db.insert(vec![row])?;
    let p = db.compact()?;
    assert_eq!(p["nonzero_slots"], 1);
    assert_eq!(
        db.prefix_slots(&p)?,
        vec![json!({"slot":expected[0].0,"value":expected[0].1})]
    );
    assert!(db
        .publish(102, 100, None)
        .unwrap_err()
        .to_string()
        .contains("storage root"));
    db.insert(vec![block(
        103,
        &[(expected[1].0.clone(), expected[1].1.clone(), 1)],
    )?])?;
    db.compact()?;
    assert_eq!(db.publish(103, 100, None)?["nonzero_slots"], 2);
    Ok(())
}

#[test]
#[ignore = "requires ClickHouse and pinned substreams CLI"]
fn prefix_corruption_and_missing_pointer_fail_after_history_was_removed() -> Result<()> {
    for defect in ["slot", "duplicate", "pointer", "missing", "version"] {
        let db = Native::new()?;
        db.initial()?;
        let mut prefix = db.compact()?;
        match defect {
            "slot" | "duplicate" => db.client.insert_values("bootstrap_storage",[json!({"generation":prefix["generation"],"address":A,"slot":if defect=="duplicate" {slots()?[0].0.clone()} else {word(3)},"value":word(7)})])?,
            "missing" => fs::remove_file(db.options.state_dir.join("bootstrap.json"))?,
            _ => {prefix[if defect=="pointer" {"generation"} else {"format_version"}]=json!("changed");files::atomic_json(&db.options.state_dir.join("bootstrap.json"),&prefix,true)?;}
        }
        assert!(db.publish(101, 100, None).is_err(), "{defect}");
        assert!(db.compact().is_err(), "{defect}");
        assert_eq!(db.numbers("state_blocks")?, vec![101]);
    }
    Ok(())
}

#[test]
#[ignore = "requires ClickHouse and pinned substreams CLI"]
fn persisted_orphan_candidate_with_raw_input_is_rebuilt_and_proven() -> Result<()> {
    let db = Native::new()?;
    db.initial()?;
    let pointer = db.options.state_dir.join("bootstrap.json");
    // load_prefix must first see no pointer, so use a legitimate pre-existing
    // orphan candidate to model the persisted state of an interrupted rename.
    db.compact()?;
    let prefix = fs::read(&pointer)?;
    fs::remove_file(&pointer)?;
    // The committed candidate exists, but none of its historical deletions
    // should be assumed: restore the removed first native block as crash input.
    let row = block(100, &slots()?)?;
    db.client.insert_values("state_blocks", [row.clone()])?;
    db.client.insert_values("_blocks_",[json!({"number":100,"hash":row["hash"].as_str().unwrap().trim_start_matches("0x"),"timestamp":row["timestamp"],"version":1,"deleted":false})])?;
    assert_eq!(db.numbers("state_blocks")?, vec![100, 101]);
    let replacement = db.compact()?;
    assert_eq!(replacement["nonzero_slots"], 2);
    assert_ne!(
        serde_json::from_slice::<Value>(&prefix)?["generation"],
        replacement["generation"]
    );
    assert_eq!(db.numbers("state_blocks")?, vec![101]);
    assert_eq!(db.publish(101, 100, None)?["nonzero_slots"], 2);
    Ok(())
}

#[test]
#[ignore = "requires ClickHouse and pinned substreams CLI"]
fn broken_suffix_budget_cursor_and_active_locks_preserve_committed_prefix() -> Result<()> {
    for defect in [
        "gap",
        "parent",
        "filter",
        "budget",
        "cursor",
        "run.lock",
        "source_readers.lock",
    ] {
        let db = Native::new()?;
        db.initial()?;
        db.compact()?;
        let before = fs::read(db.options.state_dir.join("bootstrap.json"))?;
        let n = if defect == "gap" { 103 } else { 102 };
        let mut row = block(n, &[])?;
        if defect == "parent" {
            row["parent_hash"] = json!(word(0));
        }
        if defect == "filter" {
            row["accounts"] = json!("0x2222222222222222222222222222222222222222");
        }
        // A bad filter cannot be the cursor block; append a valid marker after it.
        db.insert(if defect == "filter" {
            vec![row, block(103, &[])?]
        } else {
            vec![row]
        })?;
        if defect == "cursor" {
            fs::write(db.options.state_dir.join("cursor.txt"), "truncated")?;
        }
        let _lock = if defect.ends_with(".lock") {
            Some(files::file_lock(
                &db.options.state_dir.join(defect),
                defect == "run.lock",
                false,
            )?)
        } else {
            None
        };
        assert!(
            bootstrap::compact(
                &db.client,
                &db.options.state_dir,
                None,
                if defect == "budget" { 1 } else { BUDGET }
            )
            .is_err(),
            "{defect}"
        );
        assert_eq!(
            fs::read(db.options.state_dir.join("bootstrap.json"))?,
            before,
            "{defect}"
        );
        assert!(db.numbers("state_blocks")?.contains(&101));
    }
    Ok(())
}

fn ready_history(db: &Native) -> Result<(Value, Value)> {
    db.initial()?;
    let first = db.publish(101, 100, None)?;
    db.insert(vec![block(102, &[])?, block(103, &[])?])?;
    let second = db.publish(103, 102, first["snapshot_id"].as_str())?;
    Ok((first, second))
}

#[test]
#[ignore = "requires ClickHouse and pinned substreams CLI"]
fn history_retains_every_checkpoint_continuation_pin_and_durable_cursor() -> Result<()> {
    let db = Native::new()?;
    let (first, second) = ready_history(&db)?;
    let pin = reader::pin(
        &db.target,
        first["snapshot_id"].as_str().unwrap(),
        "history test",
    )?;
    let preview = history::cleanup(
        &db.client,
        &db.options.state_dir,
        second["snapshot_id"].as_str().unwrap(),
        1,
        false,
    )?;
    assert_eq!(preview["remove_before"], 102);
    assert_eq!(db.numbers("state_blocks")?, vec![100, 101, 102, 103]);
    history::cleanup(
        &db.client,
        &db.options.state_dir,
        second["snapshot_id"].as_str().unwrap(),
        1,
        true,
    )?;
    assert_eq!(db.numbers("state_blocks")?, vec![102, 103]);
    assert_eq!(db.numbers("_blocks_")?, vec![102, 103]);
    assert_eq!(
        reader::page(&db.target, pin["pin_id"].as_str().unwrap(), A, None, 100)?["storage"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    db.insert(vec![block(104, &[])?])?;
    let third = db.publish(104, 102, first["snapshot_id"].as_str())?;
    assert_eq!(third["state_sha256"], first["state_sha256"]);
    reader::unpin(&db.target, pin["pin_id"].as_str().unwrap())?;
    retention::prune(&db.target, 1)?;
    history::cleanup(
        &db.client,
        &db.options.state_dir,
        third["snapshot_id"].as_str().unwrap(),
        1,
        true,
    )?;
    assert_eq!(db.numbers("state_blocks")?, vec![104]);
    assert_eq!(db.numbers("_blocks_")?, vec![104]);
    cursor::load_progress(&db.client, &db.run, &db.options.state_dir)?;
    Ok(())
}

#[test]
#[ignore = "requires ClickHouse and pinned substreams CLI"]
fn partial_history_drops_resume_with_ready_checkpoint_and_cursor_intact() -> Result<()> {
    let db = Native::new()?;
    let (_, ready) = ready_history(&db)?;
    let preview = history::cleanup(
        &db.client,
        &db.options.state_dir,
        ready["snapshot_id"].as_str().unwrap(),
        1,
        false,
    )?;
    // Persist the state left by a crash between native-table partition drops.
    for p in preview["partitions"]["state_blocks"].as_array().unwrap() {
        db.client.execute(
            "ALTER TABLE state_blocks DROP PARTITION ID {id:String}",
            &params(json!({"id":p["id"]}))?,
        )?;
    }
    assert_eq!(db.numbers("state_blocks")?, vec![102, 103]);
    assert_eq!(db.numbers("_blocks_")?, vec![100, 101, 102, 103]);
    history::cleanup(
        &db.client,
        &db.options.state_dir,
        ready["snapshot_id"].as_str().unwrap(),
        1,
        true,
    )?;
    assert_eq!(db.numbers("_blocks_")?, vec![102, 103]);
    assert_eq!(
        checkpoint::manifest(&db.target, ready["snapshot_id"].as_str().unwrap())?,
        ready
    );
    Ok(())
}

#[test]
#[ignore = "requires ClickHouse and pinned substreams CLI"]
fn history_rejects_cursor_drift_writers_readers_and_wrong_provenance() -> Result<()> {
    for defect in ["cursor", "run.lock", "source_readers.lock", "provenance"] {
        let db = Native::new()?;
        let (_, ready) = ready_history(&db)?;
        if defect == "cursor" {
            fs::write(db.options.state_dir.join("cursor.txt"), "truncated")?;
        }
        let _lock = if defect.ends_with(".lock") {
            Some(files::file_lock(
                &db.options.state_dir.join(defect),
                defect == "run.lock",
                false,
            )?)
        } else {
            None
        };
        if defect == "provenance" {
            let mut bad = ready.clone();
            bad["sources"][0]["package_sha256"] = json!("wrong");
            db.target.execute("ALTER TABLE checkpoints UPDATE manifest={manifest:String} WHERE snapshot_id={id:String} SETTINGS mutations_sync=2",&params(json!({"id":ready["snapshot_id"],"manifest":files::canonical_json(&bad)?}))?)?;
        }
        assert!(
            history::cleanup(
                &db.client,
                &db.options.state_dir,
                ready["snapshot_id"].as_str().unwrap(),
                1,
                true
            )
            .is_err(),
            "{defect}"
        );
        assert_eq!(db.numbers("state_blocks")?, vec![100, 101, 102, 103]);
    }
    Ok(())
}

fn legacy(db: &mut Native) -> Result<PathBuf> {
    db.initial()?;
    db.compact()?;
    let control = evm_state::control::Control::open(&db.target)?;
    db.run["identity"]["host"] = json!("legacy-host");
    files::atomic_json(&db.options.state_dir.join("run.json"), &db.run, true)?;
    db.client.execute("ALTER TABLE _evm_state_run UPDATE identity={identity:String} WHERE run_id={id:String} SETTINGS mutations_sync=2",&params(json!({"id":db.run["run_id"],"identity":files::canonical_json(&db.run["identity"])?}))?)?;
    let mut progress: Value = serde_json::from_slice(&fs::read(
        db.options.state_dir.join("durable_progress.json"),
    )?)?;
    progress
        .as_object_mut()
        .unwrap()
        .extend(cursor::binding(&db.run)?.as_object().unwrap().clone());
    files::atomic_json(
        &db.options.state_dir.join("durable_progress.json"),
        &progress,
        true,
    )?;
    let mut prefix: Value =
        serde_json::from_slice(&fs::read(db.options.state_dir.join("bootstrap.json"))?)?;
    prefix["binding"] = cursor::binding(&db.run)?;
    files::atomic_json(&db.options.state_dir.join("bootstrap.json"), &prefix, true)?;
    db.client.execute("ALTER TABLE bootstrap_generations UPDATE manifest={manifest:String} WHERE generation={id:String} SETTINGS mutations_sync=2",&params(json!({"id":prefix["generation"],"manifest":files::canonical_json(&prefix)?}))?)?;
    let mut binding = control.record;
    binding["host"] = json!("legacy-host");
    files::atomic_json(&control.path.join("binding.json"), &binding, true)?;
    db.target.execute("ALTER TABLE _evm_checkpoint_control UPDATE binding={binding:String} WHERE control_id={id:String} SETTINGS mutations_sync=2",&params(json!({"id":binding["control_id"],"binding":files::canonical_json(&binding)?}))?)?;
    Ok(control.path)
}

#[test]
#[ignore = "requires ClickHouse and pinned substreams CLI"]
fn legacy_host_recovery_preserves_exact_state_and_resumes_a_partial_attestation() -> Result<()> {
    let mut db = Native::new()?;
    let control = legacy(&mut db)?;
    let names = [
        "run.json",
        "bootstrap.json",
        "durable_progress.json",
        "cursor.txt",
    ];
    let original = names
        .iter()
        .map(|name| fs::read(db.options.state_dir.join(name)))
        .collect::<std::io::Result<Vec<_>>>()?;
    assert!(db.compact().is_err());
    let _lock = files::file_lock(&db.options.state_dir.join("run.lock"), true, false)?;
    assert!(
        evm_state::host_recovery::rebind(&db.client, &db.options.state_dir, "legacy-host").is_err()
    );
    drop(_lock);
    let partial = evm_state::host::recovery_record_for(
        &db.run["identity"],
        &db.options.state_dir,
        &evm_state::host::machine_id()?,
    )?;
    files::atomic_json(
        &db.options.state_dir.join("host-rebinding.json"),
        &partial,
        false,
    )?;
    let first = evm_state::host_recovery::rebind(&db.client, &db.options.state_dir, "legacy-host")?;
    assert_eq!(first["rebound"], true);
    assert!(control.join("host-rebinding.json").is_file());
    assert_eq!(
        evm_state::host_recovery::rebind(&db.client, &db.options.state_dir, "legacy-host")?,
        first
    );
    for (name, bytes) in names.iter().zip(original) {
        assert_eq!(fs::read(db.options.state_dir.join(name))?, bytes);
    }
    assert_eq!(db.compact()?["already_compacted"], true);
    let mut wrong = partial;
    wrong["machine_id"] = json!(format!("machine-sha256:{}", "d".repeat(64)));
    files::atomic_json(
        &db.options.state_dir.join("host-rebinding.json"),
        &wrong,
        true,
    )?;
    assert!(db.compact().is_err());
    assert!(
        evm_state::host_recovery::rebind(&db.client, &db.options.state_dir, "legacy-host").is_err()
    );
    Ok(())
}

#[test]
#[ignore = "requires ClickHouse and pinned substreams CLI"]
fn legacy_host_recovery_rejects_mismatched_state_before_writing_sidecars() -> Result<()> {
    for defect in ["wrong_host", "package", "prefix", "controller"] {
        let mut db = Native::new()?;
        let control = legacy(&mut db)?;
        match defect {
            "package" => fs::write(db.options.state_dir.join("package.spkg"), "changed")?,
            "prefix" => {
                let path = db.options.state_dir.join("bootstrap.json");
                let mut p: Value = serde_json::from_slice(&fs::read(&path)?)?;
                p["nonzero_slots"] = json!(99);
                files::atomic_json(&path, &p, true)?;
            }
            "controller" => fs::write(control.join("initialized"), "wrong-owner")?,
            _ => {}
        }
        assert!(
            evm_state::host_recovery::rebind(
                &db.client,
                &db.options.state_dir,
                if defect == "wrong_host" {
                    "wrong-host"
                } else {
                    "legacy-host"
                }
            )
            .is_err(),
            "{defect}"
        );
        assert!(!db.options.state_dir.join("host-rebinding.json").exists());
        assert!(!control.join("host-rebinding.json").exists());
    }
    Ok(())
}

#[test]
#[ignore = "requires ClickHouse and pinned substreams CLI"]
fn rust_s2_server_drives_real_native_sink_and_complete_proof_publication() -> Result<()> {
    use evm_state::native_stream::{NativeStream, StreamOptions};
    let package =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../spkg/evm-state-v0.1.0.spkg");
    let blocks = vec![
        block(100, &slots()?)?,
        block(101, &[])?,
        block(102, &[])?,
        block(103, &[])?,
    ];
    let stream = NativeStream::new(
        &package,
        &blocks,
        StreamOptions {
            backfill: true,
            ..Default::default()
        },
    )?;
    let db = Native::at(&stream.endpoint)?;
    let options = IngestOptions {
        stop_block: Some(104),
        max_retries: 0,
        decode_batch_size: 1,
        spool_max_idle_ms: 100,
        prometheus_addr: Some("127.0.0.1:0".into()),
        parallel_workers: Some(100),
    };
    let result = ingest::ingest(&db.client, &db.options, &options);
    assert!(stream.errors().is_empty(), "{:?}", stream.errors());
    result?;
    assert_eq!(db.numbers("state_blocks")?, vec![100, 101, 102, 103]);
    let requests = stream.requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0]["compression"], "s2");
    assert_eq!(requests[0]["compressed_frame"], true);
    assert_eq!(requests[0]["workers"], "100");
    assert_eq!(db.publish(103, 100, None)?["nonzero_slots"], 2);
    Ok(())
}

#[test]
#[ignore = "requires ClickHouse and pinned substreams CLI"]
fn native_chunked_bootstrap_resumes_private_prefix_and_worker_changes() -> Result<()> {
    use evm_state::native_stream::{NativeStream, StreamOptions};
    let package =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../spkg/evm-state-v0.1.0.spkg");
    let mut blocks = vec![block(100, &slots()?)?];
    for n in 101..106 {
        blocks.push(block(n, &[])?);
    }
    let stream = NativeStream::new(
        &package,
        &blocks,
        StreamOptions {
            backfill: true,
            ..Default::default()
        },
    )?;
    let db = Native::at(&stream.endpoint)?;
    let mut options = IngestOptions {
        stop_block: None,
        max_retries: 0,
        decode_batch_size: 1,
        spool_max_idle_ms: 100,
        prometheus_addr: Some("127.0.0.1:0".into()),
        parallel_workers: Some(50),
    };
    let partial = bootstrap::replay(&db.client, &db.options, &options, 104, 2, BUDGET)?;
    assert_eq!(partial["status"], "unverified-bootstrap");
    assert_eq!(partial["header"]["number"], 103);
    options.parallel_workers = Some(100);
    let final_prefix = bootstrap::replay(&db.client, &db.options, &options, 106, 2, BUDGET)?;
    assert_eq!(
        bootstrap::replay(&db.client, &db.options, &options, 106, 2, BUDGET)?,
        final_prefix
    );
    assert_eq!(db.numbers("state_blocks")?, vec![105]);
    assert_eq!(db.numbers("_blocks_")?, vec![105]);
    let requests = stream.requests();
    assert_eq!(requests.len(), 3);
    assert_eq!(
        requests
            .iter()
            .map(|r| r["workers"].clone())
            .collect::<Vec<_>>(),
        vec![json!("50"), json!("50"), json!("100")]
    );
    for request in &requests[1..] {
        assert!(!request["start_cursor"].as_str().unwrap().is_empty());
    }
    assert!(stream.errors().is_empty(), "{:?}", stream.errors());
    assert_eq!(db.publish(105, 100, None)?["nonzero_slots"], 2);
    Ok(())
}

fn start_wrapper(db: &Native, stop: u64) -> Result<evm_state::process::OwnedGroup> {
    use std::process::{Command, Stdio};
    let log = fs::File::create(db.root.path().join(format!("wrapper-{}.log", new_id())))?;
    let mut command = Command::new(env!("CARGO_BIN_EXE_evm-state"));
    command
        .args(["--database", &db.client.database, "ingest", "--package"])
        .arg(&db.options.package)
        .args([
            "--endpoint",
            &db.options.endpoint,
            "--accounts",
            A,
            "--start-block",
            "100",
            "--stop-block",
            &stop.to_string(),
            "--state-dir",
        ])
        .arg(&db.options.state_dir)
        .args([
            "--checkpoint-database",
            &db.target.database,
            "--max-retries",
            "0",
            "--decode-batch-size",
            "1",
            "--spool-max-idle-ms",
            "100",
            "--prometheus-addr",
            "127.0.0.1:0",
        ]);
    // The private fixture server never needs provider/RPC credentials.
    for (key, _) in std::env::vars_os() {
        if key
            .to_str()
            .is_some_and(|k| k.starts_with("SUBSTREAMS_") || k.starts_with("RPC_"))
        {
            command.env_remove(key);
        }
    }
    command
        .env("SUBSTREAMS_SINK_DSN", &db.options.dsn)
        .env("EVM_STATE_HOME", &db.target.control_home)
        .stdout(Stdio::from(log.try_clone()?))
        .stderr(Stdio::from(log));
    evm_state::process::OwnedGroup::spawn(&mut command)
}
fn wait_progress(
    db: &Native,
    process: &mut evm_state::process::OwnedGroup,
    number: u64,
) -> Result<()> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
    loop {
        if let Ok(progress) = cursor::load_progress(&db.client, &db.run, &db.options.state_dir) {
            if progress["position"]["block"]["number"] == number {
                return Ok(());
            }
        }
        anyhow::ensure!(
            process.child.try_wait()?.is_none(),
            "native wrapper exited before durable progress"
        );
        anyhow::ensure!(
            std::time::Instant::now() < deadline,
            "native wrapper did not advance its durable progress"
        );
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
}
fn finish_wrapper(process: &mut evm_state::process::OwnedGroup) -> Result<()> {
    use wait_timeout::ChildExt;
    let status = process
        .child
        .wait_timeout(std::time::Duration::from_secs(30))?
        .ok_or_else(|| anyhow::anyhow!("native wrapper did not finish"))?;
    anyhow::ensure!(status.success(), "native wrapper failed");
    process.complete();
    Ok(())
}

#[test]
#[ignore = "requires ClickHouse and pinned substreams CLI"]
fn killed_rust_wrapper_and_native_sink_resume_live_and_spooled_progress() -> Result<()> {
    use evm_state::native_stream::{NativeStream, StreamOptions};
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };
    let package =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../spkg/evm-state-v0.1.0.spkg");
    for backfill in [false, true] {
        let paused = Arc::new(AtomicBool::new(true));
        let pause = paused.clone();
        let blocks = vec![
            block(100, &slots()?)?,
            block(101, &[])?,
            block(102, &[])?,
            block(103, &[])?,
        ];
        let stream = NativeStream::new(
            &package,
            &blocks,
            StreamOptions {
                backfill,
                before_block: Some(Arc::new(move |number, context| {
                    if number == 102 && pause.load(Ordering::Relaxed) {
                        context.hold();
                    }
                    Ok(())
                })),
                ..Default::default()
            },
        )?;
        let db = Native::at(&stream.endpoint)?;
        let mut process = start_wrapper(&db, 104)?;
        wait_progress(&db, &mut process, 101)?;
        let before = cursor::load_progress(&db.client, &db.run, &db.options.state_dir)?;
        // SAFETY: this positive process-group ID belongs to the still-running
        // wrapper created by OwnedGroup, including its native sink child.
        assert_eq!(
            unsafe { libc::kill(-(process.child.id() as i32), libc::SIGKILL) },
            0
        );
        process.child.wait()?;
        process.complete();
        paused.store(false, Ordering::Relaxed);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            match files::file_lock(&db.options.state_dir.join("run.lock"), true, false) {
                Ok(_) => break,
                Err(e) if std::time::Instant::now() >= deadline => return Err(e),
                Err(_) => std::thread::sleep(std::time::Duration::from_millis(20)),
            }
        }
        let mut resumed = start_wrapper(&db, 104)?;
        finish_wrapper(&mut resumed)?;
        let requests = stream.requests();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[1]["start_cursor"], before["cursor"]);
        assert_eq!(db.numbers("state_blocks")?, vec![100, 101, 102, 103]);
        assert_eq!(db.publish(103, 100, None)?["nonzero_slots"], 2);
        assert!(stream.errors().is_empty(), "{:?}", stream.errors());
    }
    Ok(())
}

#[test]
#[ignore = "requires ClickHouse and pinned substreams CLI"]
fn native_data_write_before_cursor_failure_replays_from_previous_progress() -> Result<()> {
    use evm_state::native_stream::{NativeStream, StreamOptions};
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    };
    use wait_timeout::ChildExt;
    let package =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../spkg/evm-state-v0.1.0.spkg");
    let target = Arc::new(Mutex::new(None::<(ClickHouse, Value, PathBuf)>));
    let native = target.clone();
    let restarted = Arc::new(AtomicBool::new(false));
    let resumed = restarted.clone();
    let blocks = vec![
        block(100, &slots()?)?,
        block(101, &[])?,
        block(102, &[])?,
        block(103, &[])?,
    ];
    let stream = NativeStream::new(
        &package,
        &blocks,
        StreamOptions {
            before_block: Some(Arc::new(move |number, context| {
                if resumed.load(Ordering::Relaxed) {
                    return Ok(());
                }
                let (client, run, directory) = native.lock().unwrap().clone().unwrap();
                if number == 101 {
                    context.wait_until(
                        || {
                            Ok(cursor::load_progress(&client, &run, &directory)
                                .is_ok_and(|p| p["position"]["block"]["number"] == 100))
                        },
                        std::time::Duration::from_secs(15),
                    )?;
                    fs::rename(
                        directory.join("cursor.txt"),
                        directory.join("saved-cursor.txt"),
                    )?;
                    fs::create_dir(directory.join("cursor.txt"))?;
                }
                if number == 102 {
                    context.hold();
                }
                Ok(())
            })),
            ..Default::default()
        },
    )?;
    let db = Native::at(&stream.endpoint)?;
    *target.lock().unwrap() = Some((
        db.client.clone(),
        db.run.clone(),
        db.options.state_dir.clone(),
    ));
    let mut process = start_wrapper(&db, 104)?;
    let status = process
        .child
        .wait_timeout(std::time::Duration::from_secs(30))?
        .ok_or_else(|| anyhow::anyhow!("wrapper did not fail after cursor write failure"))?;
    assert!(!status.success());
    process.complete();
    assert_eq!(db.numbers("state_blocks")?, vec![100, 101]);
    let saved = fs::read_to_string(db.options.state_dir.join("saved-cursor.txt"))?;
    assert_eq!(
        cursor::load_progress(&db.client, &db.run, &db.options.state_dir)?["cursor"],
        saved
    );
    fs::remove_dir(db.options.state_dir.join("cursor.txt"))?;
    fs::rename(
        db.options.state_dir.join("saved-cursor.txt"),
        db.options.state_dir.join("cursor.txt"),
    )?;
    restarted.store(true, Ordering::Relaxed);
    let mut process = start_wrapper(&db, 104)?;
    finish_wrapper(&mut process)?;
    let requests = stream.requests();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[1]["start_cursor"], saved);
    assert_eq!(db.numbers("state_blocks")?, vec![100, 101, 102, 103]);
    assert_eq!(db.publish(103, 100, None)?["nonzero_slots"], 2);
    assert!(stream.errors().is_empty(), "{:?}", stream.errors());
    Ok(())
}

#[test]
#[ignore = "requires ClickHouse and pinned substreams CLI"]
fn throughput_runner_samples_real_native_progress_and_bounded_timeout_resumes() -> Result<()> {
    use evm_state::{
        native_stream::{NativeStream, StreamOptions},
        rpc::RpcCall,
        throughput_qualification::{self, ThroughputOptions},
    };
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };
    struct Rpc;
    impl RpcCall for Rpc {
        fn call(&self, method: &str, _: Value) -> Result<Value> {
            Ok(match method {
                "eth_chainId" => json!("0x38"),
                "eth_getBlockByNumber" => json!({"number":"0x100","hash":word(256)}),
                _ => anyhow::bail!("unexpected fixture method"),
            })
        }
    }
    let package =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../spkg/evm-state-v0.1.0.spkg");
    // Hold after two blocks: the normal finalized defaults must flush and expose
    // progress before another decode batch arrives, then the bounded run stops.
    let held = Arc::new(AtomicBool::new(true));
    let paused = held.clone();
    let stream = NativeStream::new(
        &package,
        &[
            block(100, &slots()?)?,
            block(101, &[])?,
            block(102, &[])?,
            block(103, &[])?,
        ],
        StreamOptions {
            before_block: Some(Arc::new(move |number, context| {
                if number == 102 && paused.load(Ordering::Relaxed) {
                    context.hold();
                }
                Ok(())
            })),
            ..Default::default()
        },
    )?;
    let db = Native::at(&stream.endpoint)?;
    let mut ingest_options = IngestOptions {
        stop_block: Some(104),
        prometheus_addr: Some("127.0.0.1:0".into()),
        ..Default::default()
    };
    let error = ingest::ingest_bounded(
        &db.client,
        &db.options,
        &ingest_options,
        Some(std::time::Instant::now() + std::time::Duration::from_secs(4)),
        None,
    )
    .unwrap_err();
    assert!(error.to_string().contains("timed out"));
    assert_eq!(
        cursor::load_progress(&db.client, &db.run, &db.options.state_dir)?["position"]["block"]
            ["number"],
        101
    );
    // The wrapper has reaped the native child and released its writer lock.
    {
        let _lock = files::file_lock(&db.options.state_dir.join("run.lock"), true, false)?;
    }
    held.store(false, Ordering::Relaxed);
    ingest_options.max_retries = 0;
    assert_eq!(
        ingest::ingest(&db.client, &db.options, &ingest_options)?["position"]["block"]["number"],
        103
    );
    assert_eq!(db.publish(103, 100, None)?["nonzero_slots"], 2);

    // A distinct empty source is timed by the full Rust qualifier, including its
    // real native setup, RPC sampling, rows/cursor check and retained run report.
    let root = tempfile::tempdir()?;
    let client = ClickHouse::new(&format!("evm_test_rust_{}", new_id()))?
        .with_control_home(root.path().join("control"));
    let options = ThroughputOptions {
        database: client.database.clone(),
        root: root.path().join("run"),
        package,
        accounts: A.into(),
        endpoint: stream.endpoint.clone(),
        start_block: Some(100),
        stop_block: Some(104),
        live_blocks: None,
        interval: 1.,
        timeout: 30,
        decode_batch_size: 1,
        spool_max_idle_ms: 100,
    };
    let dsn = format!(
        "clickhouse://evm_state:local-development-only@localhost:19000/{}",
        client.database
    );
    let measured = (|| -> Result<()> {
        let report = throughput_qualification::measure(&client, &Rpc, &options, &dsn)?;
        assert_eq!(report["blocks"], 4);
        assert_eq!(report["failed_lag_samples"], 0);
        assert!(uint(&report["lag_samples"])? > 0);
        assert!(report["duration_seconds"].as_f64().unwrap() > 0.);
        assert_eq!(report["final_position"]["number"], 103);
        assert!(options.root.join("lag-samples.jsonl").is_file());
        assert!(throughput_qualification::measure(&client, &Rpc, &options, &dsn).is_err());
        Ok(())
    })();
    client.execute(
        &format!("DROP DATABASE IF EXISTS {}", client.database),
        &Default::default(),
    )?;
    measured?;
    assert!(stream.errors().is_empty(), "{:?}", stream.errors());
    Ok(())
}
