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
        let root = tempfile::tempdir()?;
        let client = ClickHouse::new(&format!("evm_test_rust_{}", new_id()))?
            .with_control_home(root.path().join("control"));
        let target = client.with_database(&format!("evm_test_rust_{}", new_id()))?;
        let options = NativeOptions {
            package: PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../spkg/evm-state-v0.1.0.spkg"),
            endpoint: "http://127.0.0.1:1".into(),
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
