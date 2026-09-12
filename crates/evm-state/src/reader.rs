//! Durable pins and account-bound keyset cursors over immutable checkpoints.
use crate::{
    ch::{params, uint, ClickHouse},
    checkpoint::{manifest_unlocked, read_account_unlocked},
    control::{new_id, object_id, Control},
    files::{atomic_json, canonical_json},
    proof::{address, fixed, string},
};
use anyhow::{ensure, Context, Result};
use base64::{engine::general_purpose::URL_SAFE, Engine};
use serde_json::{json, Value};
use std::{
    fs,
    time::{SystemTime, UNIX_EPOCH},
};

pub fn now_ns() -> Result<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_nanos()
        .try_into()
        .context("timestamp exceeds uint64")
}

pub fn pin(client: &ClickHouse, snapshot_id: &str, purpose: &str) -> Result<Value> {
    let owner = Control::open(client)?;
    let _reader = owner.reader()?;
    let ready = manifest_unlocked(client, snapshot_id)?;
    let result = json!({"pin_id":new_id(),"snapshot_id":snapshot_id,"control_id":owner.record["control_id"],
        "created_at":now_ns()?,"purpose":purpose,"header":ready["header"]});
    atomic_json(
        &owner
            .pins
            .join(format!("{}.json", string(&result, "pin_id")?)),
        &result,
        false,
    )?;
    Ok(result)
}

pub(crate) fn read_pin(owner: &Control, pin_id: &str) -> Result<Value> {
    let record: Value = serde_json::from_slice(&fs::read(
        owner.pins.join(format!("{}.json", object_id(pin_id)?)),
    )?)?;
    ensure!(
        record["pin_id"] == pin_id && record["control_id"] == owner.record["control_id"],
        "pin belongs to another controller or is corrupt"
    );
    object_id(string(&record, "snapshot_id")?)?;
    Ok(record)
}

pub fn unpin(client: &ClickHouse, pin_id: &str) -> Result<Value> {
    let owner = Control::open(client)?;
    let _reader = owner.reader()?;
    let record = read_pin(&owner, pin_id)?;
    fs::remove_file(owner.pins.join(format!("{pin_id}.json")))?;
    fs::File::open(&owner.pins)?.sync_all()?;
    Ok(json!({"pin_id":pin_id,"snapshot_id":record["snapshot_id"],"released":true}))
}

pub fn list_pins(client: &ClickHouse) -> Result<Vec<Value>> {
    let owner = Control::open(client)?;
    let _reader = owner.reader()?;
    pins_unlocked(&owner)
}

pub(crate) fn pins_unlocked(owner: &Control) -> Result<Vec<Value>> {
    let mut pins = Vec::new();
    for entry in fs::read_dir(&owner.pins)? {
        let path = entry?.path();
        if path.extension().is_some_and(|v| v == "json") {
            let record = read_pin(
                owner,
                path.file_stem()
                    .and_then(|v| v.to_str())
                    .context("invalid pin file")?,
            )?;
            let created = uint(&record["created_at"])?;
            pins.push((created, record));
        }
    }
    pins.sort_by_key(|(time, _)| *time);
    Ok(pins.into_iter().map(|(_, record)| record).collect())
}

pub fn encode_cursor(snapshot_id: &str, account: &str, slot: &str) -> Result<String> {
    Ok(URL_SAFE.encode(canonical_json(
        &json!({"version":1,"snapshot_id":snapshot_id,"address":account,"after_slot":slot}),
    )?))
}

pub fn decode_cursor(token: &str, snapshot_id: &str, account: &str) -> Result<String> {
    ensure!(token.len() <= 1024, "oversized storage cursor");
    let value: Value =
        serde_json::from_slice(&URL_SAFE.decode(token).context("invalid storage cursor")?)?;
    ensure!(
        value["version"] == 1 && value["snapshot_id"] == snapshot_id && value["address"] == account,
        "cursor belongs to another checkpoint or account"
    );
    let slot = string(&value, "after_slot")?;
    fixed::<32>(slot)?;
    Ok(slot.into())
}

pub fn page(
    client: &ClickHouse,
    pin_id: &str,
    selected: &str,
    cursor: Option<&str>,
    limit: usize,
) -> Result<Value> {
    ensure!(
        (1..=10000).contains(&limit),
        "page limit must be between 1 and 10000"
    );
    let owner = Control::open(client)?;
    let selected = address(selected)?;
    let _reader = owner.reader()?;
    let pinned = read_pin(&owner, pin_id)?;
    let snapshot_id = string(&pinned, "snapshot_id")?;
    let mut metadata = read_account_unlocked(client, snapshot_id, &selected)?;
    metadata["nonce"] = json!(uint(&metadata["nonce"])?.to_string());
    let after = cursor
        .filter(|s| !s.is_empty())
        .map(|s| decode_cursor(s, snapshot_id, &selected))
        .transpose()?
        .unwrap_or_default();
    let mut rows = client.rows("SELECT slot,value FROM checkpoint_storage FINAL WHERE snapshot_id={id:String} AND address={address:String} AND slot>{after:String} ORDER BY slot LIMIT {limit:UInt32}",
        &params(json!({"id":snapshot_id,"address":selected,"after":after,"limit":limit+1}))?)?.collect::<Result<Vec<_>>>()?;
    let more = rows.len() > limit;
    rows.truncate(limit);
    let next = if more {
        Some(encode_cursor(
            snapshot_id,
            &selected,
            string(rows.last().context("empty nonterminal page")?, "slot")?,
        )?)
    } else {
        None
    };
    Ok(
        json!({"pin_id":pin_id,"snapshot_id":snapshot_id,"header":metadata["header"],"account":metadata,"storage":rows,"next_cursor":next}),
    )
}
