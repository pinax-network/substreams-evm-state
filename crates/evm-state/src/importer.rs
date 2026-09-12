//! Verify portable files, insert a new generation, verify stored state, then publish.
use crate::{
    capacity,
    ch::{params, uint, ClickHouse},
    checkpoint,
    control::{new_id, Control},
    export::{self, check_file, page_rows, read_json, verify_layout},
    files::{canonical_json, resolve},
    proof::{fixed, string, verify_account, verify_complete},
    reader::now_ns,
    retention::require_partitioned_schema,
};
use anyhow::{ensure, Context, Result};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{fs, path::Path};

pub(crate) fn verify_stored(
    client: &ClickHouse,
    snapshot_id: &str,
    record: &Value,
    work_dir: &Path,
    control_path: &Path,
) -> Result<()> {
    let accounts=client.rows("SELECT address,exists,nonce,balance,code_hash,code,storage_root,nonzero_slots FROM checkpoint_accounts FINAL WHERE snapshot_id={id:String} ORDER BY address",&params(json!({"id":snapshot_id}))?)?.collect::<Result<Vec<_>>>()?;
    ensure!(
        json!(accounts
            .iter()
            .map(|row| string(row, "address").map(str::to_owned))
            .collect::<Result<Vec<_>>>()?)
            == record["accounts"],
        "restored account coverage differs from the checkpoint"
    );
    let mut digest = Sha256::new();
    let mut count = 0_u64;
    for mut metadata in accounts {
        let account = string(&metadata, "address")?.to_owned();
        metadata["nonce"] = json!(uint(&metadata["nonce"])?);
        metadata["nonzero_slots"] = json!(uint(&metadata["nonzero_slots"])?);
        let evidence = &record["proof_bundle"]["accounts"][&account];
        let proven = verify_account(
            string(&record["header"], "state_root")?,
            &account,
            &evidence["proof"],
        )?;
        ensure!(
            metadata["exists"] == proven.exists
                && fixed::<32>(string(&metadata, "storage_root")?)?.as_slice()
                    == proven.storage_root.as_slice()
                && metadata["code"] == evidence["code"],
            "restored metadata differs from its proven account"
        );
        let rows=client.rows("SELECT slot,value FROM checkpoint_storage FINAL WHERE snapshot_id={id:String} AND address={address:String} ORDER BY slot",&params(json!({"id":snapshot_id,"address":account}))?)?;
        let slots = rows.map(|row| {
            let row = row?;
            let slot = string(&row, "slot")?.to_owned();
            let value = string(&row, "value")?.to_owned();
            digest.update(format!("{account}{slot}{value}"));
            Ok((slot, value))
        });
        let workspace = tempfile::Builder::new()
            .prefix("evm-import-verify-")
            .tempdir_in(work_dir)?;
        let slots_count = verify_complete(
            &proven,
            slots,
            string(&metadata, "code")?,
            &metadata,
            &workspace.path().join("storage.sqlite"),
        )?;
        capacity::check(
            client,
            &[work_dir.to_path_buf(), control_path.to_path_buf()],
            "import-trie",
        )?;
        ensure!(
            Some(slots_count) == metadata["nonzero_slots"].as_u64(),
            "restored storage count differs from account metadata"
        );
        count = count
            .checked_add(slots_count)
            .context("restored slot count overflow")?;
        digest.update(canonical_json(&metadata)?);
    }
    let actual = uint(
        &client.one(
            "SELECT count() AS n FROM checkpoint_storage FINAL WHERE snapshot_id={id:String}",
            &params(json!({"id":snapshot_id}))?,
        )?["n"],
    )?;
    ensure!(
        actual == count
            && Some(count) == record["nonzero_slots"].as_u64()
            && record["state_sha256"] == hex::encode(digest.finalize()),
        "restored checkpoint contains missing, unexpected or altered state"
    );
    Ok(())
}

pub fn import_checkpoint(
    client: &ClickHouse,
    directory: &Path,
    expected_hash: Option<&str>,
    work_dir: &Path,
    budget_bytes: u64,
) -> Result<Value> {
    let directory = resolve(directory)?;
    let layout = read_json(&directory.join("manifest.json"))?;
    verify_layout(client, &directory, &layout, expected_hash, work_dir)?;
    let record = &layout["checkpoint"];
    checkpoint::setup(client)?;
    require_partitioned_schema(client)?;
    fs::create_dir_all(work_dir)?;
    let owner = Control::open(client)?;
    let _publisher = owner.publisher()?;
    let paths = vec![
        directory.clone(),
        work_dir.to_path_buf(),
        owner.path.clone(),
    ];
    capacity::check(client, &paths, "import-start")?;
    ensure!(
        client.disk_usage()? < budget_bytes,
        "retained-data budget already exhausted"
    );
    let snapshot_id = new_id();
    for item in layout["storage_pages"]
        .as_array()
        .context("invalid storage pages")?
    {
        client.insert(
            "checkpoint_storage",
            page_rows(&directory, item)?.map(|row| {
                let mut row = row?;
                row["snapshot_id"] = json!(snapshot_id);
                Ok(row)
            }),
            10000,
        )?;
        ensure!(
            client.disk_usage()? < budget_bytes,
            "restore exceeds retained-data budget; candidate remains unpublished"
        );
        capacity::check(client, &paths, "import-page")?;
    }
    let accounts = read_json(&check_file(&directory, &layout["account_file"])?)?;
    let accounts = accounts
        .as_array()
        .context("invalid restored account fields")?;
    client.insert(
        "checkpoint_accounts",
        accounts.iter().map(|row| {
            export::validate_account_fields(row)?;
            let mut row = row.clone();
            row["nonce"] = json!(export::exact_nonce(&row["nonce"])?);
            row["snapshot_id"] = json!(snapshot_id);
            Ok(row)
        }),
        1000,
    )?;
    verify_stored(client, &snapshot_id, record, work_dir, &owner.path)?;
    ensure!(
        client.disk_usage()? < budget_bytes,
        "restore exceeds retained-data budget; candidate remains unpublished"
    );
    let mut restored = record.clone();
    restored["snapshot_id"] = json!(snapshot_id);
    restored["base_snapshot"] = Value::Null;
    restored["sources"] = json!([]);
    restored["created_at"] = json!(now_ns()?);
    restored["retained_budget_bytes"] = json!(budget_bytes);
    restored["imported_from"] = json!({"snapshot_id":record["snapshot_id"],"sources":record["sources"],"manifest_content_sha256":hex::encode(Sha256::digest(canonical_json(&layout)?))});
    capacity::check(client, &paths, "import-publish")?;
    client.insert_values("checkpoints",[json!({"snapshot_id":snapshot_id,"block_number":restored["header"]["number"],"block_hash":restored["header"]["hash"],"created_at":restored["created_at"],"manifest":canonical_json(&restored)?})])?;
    Ok(restored)
}
