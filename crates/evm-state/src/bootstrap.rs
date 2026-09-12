//! Private replay prefixes retain their format and require an independently checked digest.
use crate::{
    ch::{params, uint, ClickHouse},
    control::object_id,
    cursor,
    files::canonical_json,
    proof::{fixed, string},
};
use anyhow::{ensure, Context, Result};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{collections::BTreeSet, fs, path::Path};

pub fn digest(
    client: &ClickHouse,
    generation: &str,
    fields: &Value,
    accounts: &Value,
) -> Result<Value> {
    let mut digest = Sha256::new();
    digest.update(canonical_json(fields)?);
    let accounts = accounts
        .as_array()
        .context("invalid bootstrap accounts")?
        .iter()
        .map(|a| a.as_str().context("invalid account"))
        .collect::<Result<BTreeSet<_>>>()?;
    ensure!(
        fields
            .as_object()
            .context("invalid bootstrap fields")?
            .keys()
            .all(|a| accounts.contains(a.as_str())),
        "bootstrap has account fields outside its filter"
    );
    let mut count = 0_u64;
    let mut previous = None::<(String, String)>;
    for row in client.rows("SELECT address,slot,value FROM bootstrap_storage WHERE generation={id:String} ORDER BY address,slot",&params(json!({"id":object_id(generation)?}))?)? {
        let row=row?;let account=string(&row,"address")?;let slot=string(&row,"slot")?;let value=string(&row,"value")?;
        let key=(account.to_owned(),slot.to_owned());
        ensure!(accounts.contains(account) && previous.as_ref().is_none_or(|previous|key>*previous),"bootstrap storage has unexpected accounts or duplicate keys");
        fixed::<32>(slot)?;ensure!(fixed::<32>(value)?.iter().any(|b|*b!=0),"bootstrap contains a noncanonical zero slot");
        digest.update(format!("{account}{slot}{value}"));previous=Some(key);count+=1;
    }
    Ok(json!({"state_sha256":hex::encode(digest.finalize()),"nonzero_slots":count}))
}

pub fn load_prefix(
    client: &ClickHouse,
    directory: &Path,
    run: Option<&Value>,
) -> Result<Option<Value>> {
    let pointer = directory.join("bootstrap.json");
    if !pointer.try_exists()? {
        return Ok(None);
    }
    let loaded;
    let run = if let Some(run) = run {
        run
    } else {
        loaded = serde_json::from_slice::<Value>(&fs::read(directory.join("run.json"))?)?;
        &loaded
    };
    let value: Value = serde_json::from_slice(&fs::read(pointer)?)?;
    ensure!(
        value["format_version"] == 1
            && value["status"] == "unverified-bootstrap"
            && value["binding"] == cursor::binding(run)?
            && value["start_block"] == run["identity"]["start_block"]
            && value["accounts"] == run["identity"]["accounts"],
        "bootstrap prefix belongs to another source or is corrupt"
    );
    let generation = object_id(string(&value, "generation")?)?;
    let stored = client.one(
        "SELECT manifest FROM bootstrap_generations WHERE generation={id:String}",
        &params(json!({"id":generation}))?,
    )?;
    ensure!(
        serde_json::from_str::<Value>(string(&stored, "manifest")?)? == value,
        "bootstrap pointer and stored manifest differ"
    );
    ensure!(
        uint(&value["header"]["number"])? >= uint(&value["start_block"])?,
        "bootstrap prefix range is invalid"
    );
    let measured = digest(client, generation, &value["fields"], &value["accounts"])?;
    ensure!(
        measured
            .as_object()
            .unwrap()
            .iter()
            .all(|(key, actual)| value.get(key) == Some(actual)),
        "bootstrap generation checksum or count differs"
    );
    Ok(Some(value))
}

pub fn select_prefix(client: &ClickHouse, source: &Value) -> Result<Value> {
    let mut source = source.as_object().cloned().context("invalid source")?;
    source.remove("bootstrap");
    source.remove("delta_start");
    let mut source = Value::Object(source);
    if let Some(prefix) = load_prefix(client, Path::new(string(&source, "state_directory")?), None)?
    {
        let number = uint(&prefix["header"]["number"])?;
        if uint(&source["start_block"])? <= number {
            ensure!(
                source["start_block"] == prefix["start_block"],
                "requested range begins inside a compacted bootstrap prefix"
            );
            source["delta_start"] =
                json!(number.checked_add(1).context("bootstrap block overflow")?);
            source["bootstrap"] = prefix;
        }
    }
    Ok(source)
}
