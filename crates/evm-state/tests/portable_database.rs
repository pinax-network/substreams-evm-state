#[path = "support/portable.rs"]
mod portable;
use anyhow::Result;
use evm_state::{
    ch::{params, uint, ClickHouse},
    control::{new_id, Control},
    export, files, importer, reader, retention,
};
use serde_json::json;
use std::{fs, path::Path};

struct Database(ClickHouse);
impl Database {
    fn new(home: &Path) -> Result<Self> {
        Ok(Self(
            ClickHouse::new(&format!("evm_test_rust_{}", new_id()))?.with_control_home(home),
        ))
    }
}
impl Drop for Database {
    fn drop(&mut self) {
        if let Ok(admin) = self.0.with_database("default") {
            let _ = admin.execute(
                &format!("DROP DATABASE IF EXISTS {} SYNC", self.0.database),
                &Default::default(),
            );
        }
    }
}

#[test]
#[ignore = "requires ClickHouse"]
fn portable_import_export_roundtrip_preserves_proofs_and_pinned_pages() -> Result<()> {
    let root = tempfile::tempdir()?;
    let input = root.path().join("input");
    let layout = portable::create(&input)?;
    let db = Database::new(&root.path().join("control"))?;
    let work = root.path().join("work");
    let first = importer::import_checkpoint(
        &db.0,
        &input,
        layout["checkpoint"]["header"]["hash"].as_str(),
        &work,
        100_000_000_000,
    )?;
    assert_ne!(first["snapshot_id"], portable::ID);
    assert_eq!(first["state_sha256"], layout["checkpoint"]["state_sha256"]);
    let pin = reader::pin(
        &db.0,
        first["snapshot_id"].as_str().unwrap(),
        "portable-parity",
    )?;
    let first_page = reader::page(&db.0, pin["pin_id"].as_str().unwrap(), portable::A, None, 1)?;
    assert_eq!(first_page["account"]["nonce"], "3");
    assert_eq!(first_page["storage"].as_array().unwrap().len(), 1);
    let output = root.path().join("output");
    let exported = export::export_checkpoint(
        &db.0,
        first["snapshot_id"].as_str().unwrap(),
        &output,
        1,
        &work,
    )?;
    assert_eq!(exported["nonzero_slots"], 2);
    assert_eq!(exported["pages"], 2);
    assert_eq!(
        export::verify_export(&output, None, &work)?["state_sha256"],
        first["state_sha256"]
    );
    let second = importer::import_checkpoint(&db.0, &output, None, &work, 100_000_000_000)?;
    assert_eq!(second["imported_from"]["snapshot_id"], first["snapshot_id"]);
    let removed = retention::prune(&db.0, 1)?;
    assert!(removed["keep"]
        .as_array()
        .unwrap()
        .contains(&first["snapshot_id"]));
    let final_page = reader::page(
        &db.0,
        pin["pin_id"].as_str().unwrap(),
        portable::A,
        first_page["next_cursor"].as_str(),
        1,
    )?;
    assert_eq!(final_page["header"], first_page["header"]);
    assert!(final_page["next_cursor"].is_null());
    reader::unpin(&db.0, pin["pin_id"].as_str().unwrap())?;
    Ok(())
}

#[test]
#[ignore = "requires ClickHouse"]
fn corrupt_portable_input_is_rejected_before_database_creation() -> Result<()> {
    let root = tempfile::tempdir()?;
    let directory = root.path().join("input");
    portable::create(&directory)?;
    fs::write(directory.join("storage-000000.jsonl.gz"), b"corrupt")?;
    let db = Database::new(&root.path().join("control"))?;
    assert!(importer::import_checkpoint(
        &db.0,
        &directory,
        None,
        &root.path().join("work"),
        100_000_000_000
    )
    .is_err());
    assert_eq!(
        uint(
            &db.0.with_database("default")?.one(
                "SELECT count() AS n FROM system.databases WHERE name={db:String}",
                &params(json!({"db":db.0.database}))?
            )?["n"]
        )?,
        0
    );
    Ok(())
}

#[test]
#[ignore = "requires ClickHouse"]
fn failed_restore_preserves_previous_ready_state_and_unpublished_parts_are_removable() -> Result<()>
{
    let root = tempfile::tempdir()?;
    let directory = root.path().join("input");
    portable::create(&directory)?;
    let db = Database::new(&root.path().join("control"))?;
    let work = root.path().join("work");
    let first = importer::import_checkpoint(&db.0, &directory, None, &work, 100_000_000_000)?;
    let budget = db.0.disk_usage()? + 1;
    assert!(importer::import_checkpoint(&db.0, &directory, None, &work, budget).is_err());
    assert_eq!(
        uint(
            &db.0.one(
                "SELECT count() AS n FROM checkpoints FINAL",
                &Default::default()
            )?["n"]
        )?,
        1
    );
    assert_eq!(
        evm_state::checkpoint::manifest(&db.0, first["snapshot_id"].as_str().unwrap())?,
        first
    );
    let plan = retention::prune(&db.0, 1)?;
    assert!(!plan["unpublished_candidates"]
        .as_array()
        .unwrap()
        .is_empty());
    Ok(())
}

#[test]
#[ignore = "requires ClickHouse"]
fn corrupt_stored_state_cannot_publish_an_export_and_writer_lock_excludes_restore() -> Result<()> {
    let root = tempfile::tempdir()?;
    let input = root.path().join("input");
    portable::create(&input)?;
    let db = Database::new(&root.path().join("control"))?;
    let work = root.path().join("work");
    let first = importer::import_checkpoint(&db.0, &input, None, &work, 100_000_000_000)?;
    {
        let owner = Control::open(&db.0)?;
        let _writer = owner.publisher()?;
        assert!(importer::import_checkpoint(&db.0, &input, None, &work, 100_000_000_000).is_err());
    }
    db.0.execute(
        "ALTER TABLE checkpoint_storage DROP PARTITION {id:String}",
        &params(json!({"id":first["snapshot_id"]}))?,
    )?;
    let output = root.path().join("failed-export");
    assert!(export::export_checkpoint(
        &db.0,
        first["snapshot_id"].as_str().unwrap(),
        &output,
        1,
        &work
    )
    .is_err());
    assert!(!output.join("manifest.json").exists());
    assert!(files::resolve(&output)?.is_dir());
    Ok(())
}
