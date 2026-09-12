//! Opt-in integration tests use uniquely owned databases on the configured server.
//! Run: cargo test -p evm-state --test clickhouse_state -- --include-ignored
use anyhow::Result;
use evm_state::{
    ch::{params, uint, ClickHouse},
    checkpoint,
    control::{new_id, Control},
    cursor, files, proof, reader, retention,
};
use serde_json::{json, Value};
use std::fs;

struct Database {
    client: ClickHouse,
    home: tempfile::TempDir,
}
impl Database {
    fn new() -> Result<Self> {
        let home = tempfile::tempdir()?;
        let client = ClickHouse::new(&format!("evm_test_rust_{}", new_id()))?
            .with_control_home(home.path().join("control"));
        checkpoint::setup(&client)?;
        Ok(Self { client, home })
    }

    // Publication fixtures intentionally bypass the builder. These tests isolate
    // the reader/retention protocol, not completeness acceptance.
    fn publish(&self, number: u64, accounts: &[&str], slots: &[(u64, u64)]) -> Result<String> {
        let id = new_id();
        let header = json!({"number":number,"hash":word(number),"state_root":word(0)});
        let fixture: Value =
            serde_json::from_str(include_str!("../../../tests/fixtures/proof-parity.json"))?;
        let a = &fixture["account"];
        let proven = proof::verify_account(
            proof::string(a, "state_root")?,
            proof::string(a, "address")?,
            &a["proof"],
        )?;
        for address in accounts {
            let mut row = proven.json();
            row["snapshot_id"] = json!(id);
            row["address"] = json!(address);
            row["code"] = a["code"].clone();
            row["nonce"] = json!(u64::MAX);
            row["nonzero_slots"] = json!(slots.len());
            self.client.insert_values("checkpoint_accounts", [row])?;
            self.client.insert_values("checkpoint_storage",slots.iter().map(|(slot,value)|json!({"snapshot_id":id,"address":address,"slot":word(*slot),"value":word(*value)})))?;
        }
        let manifest = json!({"format_version":1,"status":"ready","snapshot_id":id,"header":header,"accounts":accounts});
        self.client.insert_values("checkpoints",[json!({"snapshot_id":id,"block_number":number,"block_hash":word(number),"created_at":number,"manifest":files::canonical_json(&manifest)?})])?;
        Ok(id)
    }
}
impl Drop for Database {
    fn drop(&mut self) {
        if let Ok(admin) = self.client.with_database("default") {
            let _ = admin.execute(
                &format!("DROP DATABASE {} SYNC", self.client.database),
                &Default::default(),
            );
        }
    }
}

fn word(n: u64) -> String {
    format!("0x{n:064x}")
}
const A: &str = "0x1111111111111111111111111111111111111111";
const B: &str = "0x2222222222222222222222222222222222222222";

#[test]
#[ignore = "requires ClickHouse"]
fn pinned_pages_remain_coherent_across_publication_and_retention() -> Result<()> {
    let db = Database::new()?;
    let first = db.publish(100, &[A], &[(0, 1), (1, 2), (2, 3), (7, 8), (99, 100)])?;
    let pin = reader::pin(&db.client, &first, "rust-parity")?;
    let pin_id = proof::string(&pin, "pin_id")?;
    let first_page = reader::page(&db.client, pin_id, A, None, 2)?;
    assert_eq!(first_page["account"]["nonce"], u64::MAX.to_string());
    let second = db.publish(101, &[A], &[(9, 42)])?;
    let third = db.publish(102, &[A], &[(10, 43)])?;
    let removed = retention::prune(&db.client, 1)?;
    assert!(removed["keep"].as_array().unwrap().contains(&json!(first)));
    assert!(removed["remove"]
        .as_array()
        .unwrap()
        .contains(&json!(second)));
    let mut rows = first_page["storage"].as_array().unwrap().clone();
    let mut next = first_page["next_cursor"].as_str().map(str::to_owned);
    while let Some(token) = next {
        let page = reader::page(&db.client, pin_id, A, Some(&token), 2)?;
        assert_eq!(page["header"], first_page["header"]);
        rows.extend(page["storage"].as_array().unwrap().clone());
        next = page["next_cursor"].as_str().map(str::to_owned);
    }
    assert_eq!(
        rows,
        [(0, 1), (1, 2), (2, 3), (7, 8), (99, 100)]
            .map(|(k, v)| json!({"slot":word(k),"value":word(v)}))
    );
    reader::unpin(&db.client, pin_id)?;
    assert!(reader::page(&db.client, pin_id, A, None, 1).is_err());
    assert!(retention::prune(&db.client, 1)?["remove"]
        .as_array()
        .unwrap()
        .contains(&json!(first)));
    assert_eq!(
        uint(&checkpoint::read_account(&db.client, &third, A)?["nonzero_slots"])?,
        1
    );
    Ok(())
}

#[test]
#[ignore = "requires ClickHouse"]
fn quiet_accounts_and_reader_pins_protect_their_latest_generation() -> Result<()> {
    let db = Database::new()?;
    let quiet = db.publish(100, &[A], &[(1, 1)])?;
    let old = db.publish(101, &[B], &[(2, 2)])?;
    let newest = db.publish(102, &[B], &[(3, 3)])?;
    let result = retention::prune(&db.client, 1)?;
    let mut keep = vec![json!(quiet), json!(newest)];
    keep.sort_by_key(|v| v.as_str().unwrap().to_owned());
    assert_eq!(result["keep"], json!(keep));
    assert_eq!(result["remove"], json!([old]));
    assert_eq!(
        uint(&checkpoint::read_account(&db.client, &quiet, A)?["nonzero_slots"])?,
        1
    );
    Ok(())
}

#[test]
#[ignore = "requires ClickHouse"]
fn unpublished_parts_are_invisible_and_active_reader_excludes_pruning() -> Result<()> {
    let db = Database::new()?;
    let ready = db.publish(100, &[A], &[(1, 1)])?;
    let candidate = new_id();
    db.client.insert_values(
        "checkpoint_storage",
        [json!({"snapshot_id":candidate,"address":A,"slot":word(1),"value":word(2)})],
    )?;
    assert!(checkpoint::read_account(&db.client, &candidate, A).is_err());
    let owner = Control::open(&db.client)?;
    {
        let _reader = owner.reader()?;
        assert!(retention::prune(&db.client, 1).is_err());
    }
    assert_eq!(
        retention::plan(&db.client, 1)?["unpublished_candidates"],
        json!([candidate])
    );
    assert_eq!(
        retention::prune(&db.client, 1)?["remove"],
        json!([candidate])
    );
    assert!(checkpoint::read_account(&db.client, &ready, A).is_ok());
    Ok(())
}

#[test]
#[ignore = "requires ClickHouse"]
fn cursors_cannot_cross_accounts_checkpoints_or_invalid_limits() -> Result<()> {
    let db = Database::new()?;
    let first = db.publish(100, &[A, B], &[(1, 1), (2, 2)])?;
    let p1 = reader::pin(&db.client, &first, "test")?;
    let pid = proof::string(&p1, "pin_id")?;
    let cursor = reader::page(&db.client, pid, A, None, 1)?["next_cursor"]
        .as_str()
        .unwrap()
        .to_owned();
    assert!(reader::page(&db.client, pid, B, Some(&cursor), 1).is_err());
    let second = db.publish(101, &[A], &[(1, 2)])?;
    let p2 = reader::pin(&db.client, &second, "test")?;
    assert!(reader::page(
        &db.client,
        proof::string(&p2, "pin_id")?,
        A,
        Some(&cursor),
        1
    )
    .is_err());
    for limit in [0, 10001, usize::MAX] {
        assert!(reader::page(&db.client, pid, A, None, limit).is_err());
    }
    reader::unpin(&db.client, pid)?;
    reader::unpin(&db.client, proof::string(&p2, "pin_id")?)?;
    Ok(())
}

#[test]
#[ignore = "requires ClickHouse"]
fn second_controller_and_lost_database_binding_cannot_be_silently_adopted() -> Result<()> {
    let db = Database::new()?;
    let first = db.publish(100, &[A], &[(1, 1)])?;
    reader::pin(&db.client, &first, "test")?;
    let foreign = db
        .client
        .clone()
        .with_control_home(db.home.path().join("foreign"));
    assert!(retention::prune(&foreign, 1)
        .unwrap_err()
        .to_string()
        .contains("control metadata is missing"));
    db.client.execute(
        "DROP TABLE _evm_checkpoint_control SYNC",
        &Default::default(),
    )?;
    assert!(retention::prune(&db.client, 1)
        .unwrap_err()
        .to_string()
        .contains("ownership is missing"));
    Ok(())
}

#[test]
#[ignore = "requires ClickHouse"]
fn corrupt_pin_and_initialization_marker_stop_retention() -> Result<()> {
    let db = Database::new()?;
    let first = db.publish(100, &[A], &[(1, 1)])?;
    let pin = reader::pin(&db.client, &first, "test")?;
    let owner = Control::open(&db.client)?;
    let pin_path = owner
        .pins
        .join(format!("{}.json", proof::string(&pin, "pin_id")?));
    let mut broken = pin.clone();
    broken["snapshot_id"] = json!(new_id());
    files::atomic_json(&pin_path, &broken, true)?;
    assert!(retention::plan(&db.client, 1)
        .unwrap_err()
        .to_string()
        .contains("pinned checkpoint is missing"));
    files::atomic_json(&pin_path, &pin, true)?;
    files::atomic_write(&owner.path.join("initialized"), b"corrupt", true)?;
    assert!(Control::open(&db.client)
        .err()
        .unwrap()
        .to_string()
        .contains("initialization marker is corrupt"));
    Ok(())
}

#[test]
#[ignore = "requires ClickHouse"]
fn interrupted_retention_becomes_an_unpublished_orphan() -> Result<()> {
    let db = Database::new()?;
    let first = db.publish(100, &[A], &[(1, 1)])?;
    db.publish(101, &[A], &[(2, 2)])?;
    db.client.execute(
        "ALTER TABLE checkpoints DROP PARTITION {id:String}",
        &params(json!({"id":first}))?,
    )?;
    assert!(checkpoint::manifest(&db.client, &first).is_err());
    let result = retention::prune(&db.client, 1)?;
    assert_eq!(result["unpublished_candidates"], json!([first]));
    assert_eq!(
        uint(
            &db.client.one(
                "SELECT count() AS n FROM checkpoint_storage FINAL WHERE snapshot_id={id:String}",
                &params(json!({"id":first}))?
            )?["n"]
        )?,
        0
    );
    Ok(())
}

fn cursor_fixture(db: &Database) -> Result<(Value, Value)> {
    db.client.execute("CREATE TABLE state_blocks (number UInt64,hash String,accounts String,schema_version UInt64,producer_version UInt64) ENGINE=ReplacingMergeTree ORDER BY number",&Default::default())?;
    db.client.execute("CREATE TABLE _blocks_ (number UInt64,hash String) ENGINE=ReplacingMergeTree ORDER BY number",&Default::default())?;
    db.client.insert_values("state_blocks",[100,101].map(|n|json!({"number":n,"hash":word(n),"accounts":A,"schema_version":1,"producer_version":5})))?;
    db.client.insert_values(
        "_blocks_",
        [100, 101].map(|n| json!({"number":n,"hash":format!("{n:064x}")})),
    )?;
    let run = json!({"run_id":new_id(),"database_uuid":new_id(),"identity":{"start_block":100,"accounts":[A]}});
    let cursors = serde_json::from_str(include_str!("../../../tests/fixtures/cursors.json"))?;
    Ok((run, cursors))
}

#[test]
#[ignore = "requires ClickHouse"]
fn durable_progress_rejects_ahead_regressed_and_torn_native_cursors() -> Result<()> {
    let db = Database::new()?;
    let (run, cursors) = cursor_fixture(&db)?;
    let directory = db.home.path().join("native");
    fs::create_dir_all(&directory)?;
    assert!(cursor::save_progress(
        &db.client,
        &run,
        &directory,
        cursors["1"]["102"].as_str().unwrap()
    )
    .is_err());
    let saved = cursor::save_progress(
        &db.client,
        &run,
        &directory,
        cursors["1"]["101"].as_str().unwrap(),
    )?;
    assert!(cursor::save_progress(
        &db.client,
        &run,
        &directory,
        cursors["1"]["100"].as_str().unwrap()
    )
    .is_err());
    assert_eq!(cursor::observe(&db.client, &run, &directory)?, None);
    fs::write(directory.join("cursor.txt"), "half-a-cursor")?;
    assert_eq!(cursor::observe(&db.client, &run, &directory)?, None);
    assert_eq!(cursor::load_progress(&db.client, &run, &directory)?, saved);
    fs::write(
        directory.join("cursor.txt"),
        cursors["1"]["102"].as_str().unwrap(),
    )?;
    assert!(cursor::observe(&db.client, &run, &directory).is_err());
    Ok(())
}

#[test]
#[ignore = "requires ClickHouse"]
fn durable_backup_requires_unchanged_identity_position_cursor_rows_and_marker() -> Result<()> {
    let db = Database::new()?;
    let (run, cursors) = cursor_fixture(&db)?;
    let directory = db.home.path().join("native");
    let saved = cursor::save_progress(
        &db.client,
        &run,
        &directory,
        cursors["1"]["101"].as_str().unwrap(),
    )?;
    let path = directory.join("durable_progress.json");
    for field in [
        "run_id",
        "database_uuid",
        "identity_sha256",
        "position",
        "cursor",
    ] {
        let mut corrupted = saved.clone();
        corrupted[field] = json!("wrong");
        files::atomic_json(&path, &corrupted, true)?;
        assert!(
            cursor::load_progress(&db.client, &run, &directory).is_err(),
            "{field}"
        );
    }
    files::atomic_json(&path, &saved, true)?;
    db.client
        .execute("TRUNCATE TABLE _blocks_", &Default::default())?;
    assert!(cursor::load_progress(&db.client, &run, &directory).is_err());
    db.client.insert_values(
        "_blocks_",
        [json!({"number":101,"hash":format!("{:064x}",101)})],
    )?;
    assert_eq!(cursor::load_progress(&db.client, &run, &directory)?, saved);
    db.client
        .execute("TRUNCATE TABLE state_blocks", &Default::default())?;
    assert!(cursor::load_progress(&db.client, &run, &directory).is_err());
    Ok(())
}
