//! Immutable checkpoint schema and coherent published-state reads.
use crate::{
    ch::{params, ClickHouse},
    control::{object_id, Control},
    proof::{address, string},
};
use anyhow::{ensure, Context, Result};
use serde_json::{json, Value};
use std::collections::BTreeSet;

pub fn canonical_accounts(values: &Value) -> Result<Vec<String>> {
    let values: Vec<&str> = if let Some(text) = values.as_str() {
        text.split(|c: char| c == ',' || c.is_whitespace())
            .filter(|v| !v.is_empty())
            .collect()
    } else if let Some(map) = values.as_object() {
        map.keys().map(String::as_str).collect()
    } else {
        values
            .as_array()
            .context("account filter must be a list")?
            .iter()
            .map(|v| v.as_str().context("invalid account address"))
            .collect::<Result<_>>()?
    };
    let result: BTreeSet<_> = values.into_iter().map(address).collect::<Result<_>>()?;
    ensure!(!result.is_empty(), "selected account list is empty");
    Ok(result.into_iter().collect())
}

pub fn setup(client: &ClickHouse) -> Result<()> {
    client.with_database("default")?.execute(
        &format!("CREATE DATABASE IF NOT EXISTS {}", client.database),
        &Default::default(),
    )?;
    for statement in include_str!("../sql/checkpoints.sql")
        .split(';')
        .filter(|v| !v.trim().is_empty())
    {
        client.execute(statement, &Default::default())?;
    }
    Ok(())
}

pub fn manifest(client: &ClickHouse, snapshot_id: &str) -> Result<Value> {
    let owner = Control::open(client)?;
    let _reader = owner.reader()?;
    manifest_unlocked(client, snapshot_id)
}

pub(crate) fn manifest_unlocked(client: &ClickHouse, snapshot_id: &str) -> Result<Value> {
    object_id(snapshot_id)?;
    let row = client.one(
        "SELECT manifest FROM checkpoints FINAL WHERE snapshot_id={id:String}",
        &params(json!({"id":snapshot_id}))?,
    )?;
    let data: Value = serde_json::from_str(string(&row, "manifest")?)?;
    ensure!(
        data["snapshot_id"] == snapshot_id && data["status"] == "ready",
        "checkpoint has no valid ready manifest"
    );
    Ok(data)
}

pub fn read_account(client: &ClickHouse, snapshot_id: &str, selected: &str) -> Result<Value> {
    let owner = Control::open(client)?;
    let _reader = owner.reader()?;
    read_account_unlocked(client, snapshot_id, selected)
}

pub(crate) fn read_account_unlocked(
    client: &ClickHouse,
    snapshot_id: &str,
    selected: &str,
) -> Result<Value> {
    let published = manifest_unlocked(client, snapshot_id)?;
    let selected = address(selected)?;
    ensure!(
        published["accounts"]
            .as_array()
            .context("invalid checkpoint accounts")?
            .contains(&json!(selected)),
        "account is not ready in this checkpoint"
    );
    let mut row = client.one("SELECT address, exists, nonce, balance, code_hash, code, storage_root, nonzero_slots FROM checkpoint_accounts FINAL WHERE snapshot_id={id:String} AND address={address:String}",
        &params(json!({"id":snapshot_id,"address":selected}))?)?;
    row["snapshot_id"] = json!(snapshot_id);
    row["header"] = published["header"].clone();
    Ok(row)
}
