//! Bind source inputs to a prepared locally owned native ingestion run.
use crate::{
    ch::{params, uint, ClickHouse},
    checkpoint::canonical_accounts,
    cursor,
    files::{file_lock, resolve, Lock},
    host,
    proof::string,
};
use anyhow::{ensure, Context, Result};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{fs, path::Path};

pub struct VerifiedSource {
    pub source: Value,
    _reader: Lock,
}

fn selected(value: &Value) -> Result<Vec<String>> {
    ensure!(value.is_array(), "source accounts must be a nonempty list");
    canonical_accounts(value)
}

fn validate(
    client: &ClickHouse,
    source: &Value,
    identity: &Value,
    run_id: &str,
    directory: &Path,
) -> Result<Value> {
    let path = directory.to_str().context("invalid source directory")?;
    ensure!(
        identity["format_version"] == 3
            && host::matches(identity, directory)?
            && identity["state_directory"] == path,
        "source run has no matching host/directory binding; use a fresh guarded continuation"
    );
    ensure!(
        identity["database"] == client.database
            && identity["http_url"] == client.url
            && identity["module"] == "map_block_state"
            && identity["network"] == "bsc"
            && identity["schema_version"] == 1
            && identity["final_blocks_only"] == true,
        "source database identity is not the qualified finalized BSC module"
    );
    let module = string(identity, "module_hash")?;
    ensure!(
        source["final_blocks_only"] == true
            && source["module_hash"] == module
            && module.len() == 40
            && module
                .bytes()
                .all(|v| v.is_ascii_digit() || (b'a'..=b'f').contains(&v))
            && serde_json::to_value(selected(&source["accounts"])?)? == identity["accounts"],
        "source filter, module hash or finality differs from its native run"
    );
    ensure!(
        source["start_block"]
            .as_u64()
            .context("invalid source start block")?
            >= uint(&identity["start_block"])?,
        "source range starts before its native run"
    );
    let record: Value = serde_json::from_slice(&fs::read(directory.join("run.json"))?)?;
    let db = client.one(
        "SELECT toString(uuid) AS id FROM system.databases WHERE name={db:String}",
        &params(json!({"db":client.database}))?,
    )?;
    ensure!(
        record["phase"] == "prepared"
            && record["run_id"] == run_id
            && &record["identity"] == identity
            && record["database_uuid"] == db["id"],
        "source database and local run metadata differ"
    );
    ensure!(
        hex::encode(Sha256::digest(fs::read(directory.join("package.spkg"))?))
            == string(identity, "package_sha256")?,
        "source frozen package is missing or changed"
    );
    ensure!(
        fs::read_to_string(
            directory
                .join("meta")
                .join(format!("{}_schema_hash.txt", client.database))
        )?
        .trim()
            == string(&record, "schema_hash")?,
        "source schema metadata is missing or changed"
    );
    let verified = json!({"run_id":run_id,"database_uuid":db["id"],"package_sha256":identity["package_sha256"],"schema_hash":record["schema_hash"],"state_directory":path});
    let mut checked = source.as_object().cloned().context("invalid source")?;
    for (key, value) in verified.as_object().unwrap() {
        ensure!(
            source.get(key).is_none_or(|actual| actual == value),
            "source provenance differs from its recorded native run"
        );
        checked.insert(key.clone(), value.clone());
    }
    Ok(Value::Object(checked))
}

pub fn verified_source(
    client: &ClickHouse,
    source: &Value,
    checkpoints: Option<&ClickHouse>,
    end_block: Option<u64>,
) -> Result<VerifiedSource> {
    ensure!(uint(&client.one("SELECT count() AS n FROM system.tables WHERE database={db:String} AND name='_evm_state_run'", &params(json!({"db":client.database}))?)?["n"])? != 0, "source has no guarded native run ownership record");
    let owner = client.one(
        "SELECT run_id,identity FROM _evm_state_run",
        &Default::default(),
    )?;
    let identity: Value = serde_json::from_str(string(&owner, "identity")?)?;
    ensure!(
        identity.is_object() && identity["format_version"] == 3,
        "source uses an old native run identity; use a fresh guarded continuation"
    );
    if let Some(checkpoints) = checkpoints {
        ensure!(
            identity["checkpoint_database"] == checkpoints.database,
            "source is bound to a different checkpoint database"
        );
    }
    let directory = resolve(Path::new(string(&identity, "state_directory")?))?;
    ensure!(
        identity["state_directory"] == directory.to_str().context("invalid source directory")?
            && directory.is_dir()
            && host::matches(&identity, &directory)?,
        "source run has no matching host/directory binding"
    );
    let reader = file_lock(&directory.join("source_readers.lock"), false, true)?;
    let mut checked = validate(
        client,
        source,
        &identity,
        string(&owner, "run_id")?,
        &directory,
    )?;
    if let Some(end) = end_block {
        let run = serde_json::from_slice(&fs::read(directory.join("run.json"))?)?;
        let progress = cursor::load_progress(client, &run, &directory)?;
        ensure!(
            uint(&progress["position"]["block"]["number"])? >= end,
            "durable native progress has not reached the checkpoint target"
        );
        checked["durable_position"] = progress["position"]["block"].clone();
    }
    Ok(VerifiedSource {
        source: checked,
        _reader: reader,
    })
}
