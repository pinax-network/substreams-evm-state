//! Explicit same-machine recovery for legacy hostname-bound source/controller pairs.
use crate::{
    bootstrap,
    ch::{params, uint, ClickHouse},
    cursor,
    files::{atomic_json, file_lock, resolve},
    host,
    proof::string,
    trie_qualification::file_hash,
};
use anyhow::{ensure, Context, Result};
use serde_json::{json, Value};
use std::{fs, path::Path};

/// The operator must attest that this is the original machine. A legacy
/// hostname cannot establish that fact cryptographically. Original state and
/// identities stay byte-for-byte intact; only checked recovery sidecars are added.
pub fn rebind(client: &ClickHouse, directory: &Path, previous_host: &str) -> Result<Value> {
    ensure!(
        !previous_host.is_empty() && !previous_host.starts_with("machine-sha256:"),
        "recovery requires the exact legacy hostname"
    );
    let directory = resolve(directory)?;
    let run: Value = serde_json::from_slice(&fs::read(directory.join("run.json"))?)?;
    let identity = &run["identity"];
    let checkpoints = client.with_database(string(identity, "checkpoint_database")?)?;
    let checkpoint_uuid = checkpoints.one(
        "SELECT toString(uuid) AS id FROM system.databases WHERE name={db:String}",
        &params(json!({"db":checkpoints.database}))?,
    )?["id"]
        .clone();
    let id = uuid::Uuid::parse_str(
        checkpoint_uuid
            .as_str()
            .context("invalid checkpoint database UUID")?,
    )?;
    ensure!(
        !id.is_nil() && json!(id.to_string()) == checkpoint_uuid,
        "checkpoint coordination requires an Atomic ClickHouse database"
    );
    let control_dir = resolve(&client.control_home)?.join(id.to_string());
    let mut locks = Vec::new();
    for path in [
        directory.join("bootstrap_replay.lock"),
        directory.join("run.lock"),
        directory.join("source_readers.lock"),
        control_dir.join("initialize.lock"),
        control_dir.join("writer.lock"),
        control_dir.join("readers.lock"),
    ] {
        locks.push(file_lock(&path, true, false)?);
    }
    ensure!(
        serde_json::from_slice::<Value>(&fs::read(directory.join("run.json"))?)? == run,
        "run changed during host recovery"
    );
    let owner = client.one(
        "SELECT run_id,identity FROM _evm_state_run",
        &Default::default(),
    )?;
    let database = client.one(
        "SELECT toString(uuid) AS id FROM system.databases WHERE name={db:String}",
        &params(json!({"db":client.database}))?,
    )?;
    ensure!(
        run["phase"] == "prepared"
            && identity["format_version"] == 3
            && identity["host"] == previous_host
            && identity["state_directory"]
                == directory.to_str().context("invalid state directory")?
            && identity["database"] == client.database
            && identity["http_url"] == client.url
            && run["database_uuid"] == database["id"]
            && owner["run_id"] == run["run_id"]
            && serde_json::from_str::<Value>(string(&owner, "identity")?)? == *identity,
        "legacy native ownership does not match"
    );
    ensure!(
        file_hash(&directory.join("package.spkg"))? == string(identity, "package_sha256")?
            && fs::read_to_string(
                directory
                    .join("meta")
                    .join(format!("{}_schema_hash.txt", client.database))
            )?
            .trim()
                == string(&run, "schema_hash")?,
        "frozen package or schema differs"
    );
    let binding: Value = serde_json::from_slice(&fs::read(control_dir.join("binding.json"))?)?;
    let control_owner = checkpoints.one(
        "SELECT control_id,binding FROM _evm_checkpoint_control",
        &Default::default(),
    )?;
    ensure!(
        binding["host"] == previous_host
            && binding["database_uuid"] == checkpoint_uuid
            && binding["directory"]
                == control_dir
                    .to_str()
                    .context("invalid controller directory")?
            && control_owner["control_id"] == binding["control_id"]
            && serde_json::from_str::<Value>(string(&control_owner, "binding")?)? == binding
            && fs::read_to_string(control_dir.join("initialized"))?
                == string(&binding, "control_id")?,
        "legacy checkpoint ownership does not match"
    );
    let progress = cursor::load_progress(client, &run, &directory)?;
    let native = cursor::validate(
        client,
        &run,
        fs::read_to_string(directory.join("cursor.txt"))?.trim(),
    )?;
    ensure!(
        native.block.number >= uint(&progress["position"]["block"]["number"])?,
        "native cursor regressed behind durable progress"
    );
    let prefix = bootstrap::load_prefix(client, &directory, Some(&run))?;
    let machine = host::machine_id()?;
    let expected = [
        (
            directory.join("host-rebinding.json"),
            host::recovery_record_for(identity, &directory, &machine)?,
        ),
        (
            control_dir.join("host-rebinding.json"),
            host::recovery_record_for(&binding, &control_dir, &machine)?,
        ),
    ];
    // Validate both sides before writing either. A single matching sidecar from
    // an interrupted attempt can safely resume the second atomic write.
    for (path, record) in &expected {
        ensure!(
            !path.try_exists()? || serde_json::from_slice::<Value>(&fs::read(path)?)? == *record,
            "existing host recovery belongs to different state or machine"
        );
    }
    for (path, record) in &expected {
        if !path.try_exists()? {
            atomic_json(path, record, false)?;
        }
    }
    Ok(
        json!({"run_id":run["run_id"],"rebound":true,"cursor_block":native.block,"prefix_block":prefix.map(|p|p["header"].clone()),"qualification":"Operator-confirmed original machine; original identities and state preserved."}),
    )
}
