//! Remove whole generations while preserving pins and each account's newest state.
use crate::{
    ch::{params, ClickHouse},
    control::{object_id, Control},
    proof::string,
    reader::pins_unlocked,
};
use anyhow::{ensure, Context, Result};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};

const TABLES: [&str; 3] = ["checkpoints", "checkpoint_accounts", "checkpoint_storage"];

pub fn require_partitioned_schema(client: &ClickHouse) -> Result<()> {
    let rows = client.rows("SELECT name,partition_key FROM system.tables WHERE database={db:String} AND name IN ('checkpoints','checkpoint_accounts','checkpoint_storage')", &params(json!({"db":client.database}))?)?.collect::<Result<Vec<_>>>()?;
    ensure!(rows.len() == 3 && rows.iter().all(|row|row["partition_key"] == "snapshot_id"), "checkpoint tables use an old prototype schema; export and import into a fresh database before retention");
    Ok(())
}

fn plan_unlocked(client: &ClickHouse, owner: &Control, keep_latest: usize) -> Result<Value> {
    ensure!(keep_latest >= 1, "keep_latest must be at least one");
    require_partitioned_schema(client)?;
    let mut records = Vec::new();
    for row in client.rows("SELECT snapshot_id,manifest FROM checkpoints FINAL ORDER BY block_number DESC, created_at DESC", &Default::default())? {
        let row = row?;
        let value: Value = serde_json::from_str(string(&row,"manifest")?)?;
        ensure!(value["snapshot_id"] == object_id(string(&row,"snapshot_id")?)? && value["status"] == "ready", "invalid ready manifest blocks retention");
        records.push(value);
    }
    let ready = records
        .iter()
        .map(|row| string(row, "snapshot_id").map(str::to_owned))
        .collect::<Result<BTreeSet<_>>>()?;
    let mut keep = records
        .iter()
        .take(keep_latest)
        .map(|row| string(row, "snapshot_id").map(str::to_owned))
        .collect::<Result<BTreeSet<_>>>()?;
    let mut latest = BTreeMap::new();
    for record in &records {
        for account in record["accounts"]
            .as_array()
            .context("invalid checkpoint accounts")?
        {
            let account = account.as_str().context("invalid checkpoint account")?;
            if !latest.contains_key(account) {
                let id = string(record, "snapshot_id")?.to_owned();
                latest.insert(account.to_owned(), id.clone());
                keep.insert(id);
            }
        }
    }
    let pins = pins_unlocked(owner)?;
    for pin in &pins {
        let id = string(pin, "snapshot_id")?;
        ensure!(
            ready.contains(id),
            "a pinned checkpoint is missing; retention stopped"
        );
        keep.insert(id.to_owned());
    }
    let mut partitions = BTreeSet::new();
    for row in client.rows("SELECT DISTINCT partition FROM system.parts WHERE database={db:String} AND active AND table IN ('checkpoints','checkpoint_accounts','checkpoint_storage')", &params(json!({"db":client.database}))?)? {
        partitions.insert(object_id(string(&row?,"partition")?.trim_matches('\''))?.to_owned());
    }
    let remove: BTreeSet<_> = partitions.difference(&keep).cloned().collect();
    Ok(
        json!({"keep":keep,"remove":remove,"unpublished_candidates":remove.difference(&ready).collect::<Vec<_>>(),
        "pins":pins.iter().map(|p|json!({"pin_id":p["pin_id"],"snapshot_id":p["snapshot_id"]})).collect::<Vec<_>>(),
        "latest_for_account":latest,"keep_latest":keep_latest}),
    )
}

pub fn plan(client: &ClickHouse, keep_latest: usize) -> Result<Value> {
    let owner = Control::open(client)?;
    let _retention = owner.retention()?;
    plan_unlocked(client, &owner, keep_latest)
}

pub fn prune(client: &ClickHouse, keep_latest: usize) -> Result<Value> {
    let owner = Control::open(client)?;
    let _retention = owner.retention()?;
    let mut result = plan_unlocked(client, &owner, keep_latest)?;
    result["bytes_before"] = json!(client.disk_usage()?);
    for id in result["remove"].as_array().unwrap() {
        // Remove publication first; interrupted cleanup becomes an unpublished candidate.
        for table in TABLES {
            client.execute(
                &format!("ALTER TABLE {table} DROP PARTITION {{id:String}}"),
                &params(json!({"id":id}))?,
            )?;
        }
    }
    result["bytes_after"] = json!(client.disk_usage()?);
    result["applied"] = json!(true);
    result["space_reclamation"] = json!(
        "ClickHouse may retain inactive parts until background cleanup; bytes_after includes them"
    );
    Ok(result)
}
