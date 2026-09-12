//! Single-host coordination. Every operation revalidates database ownership.
use crate::{
    ch::{params, uint, ClickHouse},
    files::{atomic_json, atomic_write, file_lock, resolve, Lock},
    host,
    proof::string,
};
use anyhow::{ensure, Context, Result};
use serde_json::{json, Value};
use std::{fs, path::PathBuf};

pub fn object_id(id: &str) -> Result<&str> {
    ensure!(
        id.len() == 32
            && id
                .bytes()
                .all(|v| v.is_ascii_digit() || (b'a'..=b'f').contains(&v)),
        "expected a 32-character checkpoint or pin ID"
    );
    Ok(id)
}

pub fn new_id() -> String {
    uuid::Uuid::new_v4().simple().to_string()
}

pub struct Control {
    pub path: PathBuf,
    pub pins: PathBuf,
    pub record: Value,
}
pub struct PublisherLock {
    _writer: Lock,
    _readers: Lock,
}

impl Control {
    pub fn open(client: &ClickHouse) -> Result<Self> {
        let db = params(json!({"db":client.database}))?;
        let database = client.one(
            "SELECT toString(uuid) AS id FROM system.databases WHERE name={db:String}",
            &db,
        )?;
        let database_uuid = string(&database, "id")?;
        let parsed = uuid::Uuid::parse_str(database_uuid)
            .context("checkpoint coordination requires an Atomic ClickHouse database")?;
        ensure!(
            !parsed.is_nil() && parsed.to_string() == database_uuid,
            "checkpoint coordination requires an Atomic ClickHouse database"
        );
        let path = resolve(&client.control_home)?.join(database_uuid);
        fs::create_dir_all(&path)?;
        let binding = path.join("binding.json");
        let initialized = path.join("initialized");
        let record;
        {
            let _lock = file_lock(&path.join("initialize.lock"), true, true)?;
            let exists = uint(&client.one("SELECT count() AS n FROM system.tables WHERE database={db:String} AND name='_evm_checkpoint_control'", &db)?["n"])? != 0;
            if binding.try_exists()? {
                record = serde_json::from_slice::<Value>(&fs::read(&binding)?)?;
            } else {
                ensure!(
                    !exists,
                    "checkpoint control metadata is missing; restore the bound EVM_STATE_HOME"
                );
                record = json!({"format_version":1,"control_id":new_id(),"database_uuid":database_uuid,
                    "directory":path.to_str().context("invalid controller path")?,"host":host::machine_id()?});
                atomic_json(&binding, &record, false)?;
            }
            ensure!(
                record["database_uuid"] == database_uuid
                    && record["directory"] == path.to_str().context("invalid controller path")?
                    && host::matches(&record, &path)?,
                "checkpoint controller belongs to another database, directory or host"
            );
            let control_id = object_id(string(&record, "control_id")?)?;
            if !exists {
                ensure!(!initialized.try_exists()?, "database checkpoint ownership is missing; restore matching database and control metadata");
                client.execute("CREATE TABLE _evm_checkpoint_control (control_id String, binding String) ENGINE=MergeTree ORDER BY control_id SETTINGS fsync_after_insert=1, fsync_part_directory=1", &Default::default())?;
                client.insert_values("_evm_checkpoint_control", [json!({"control_id":control_id,"binding":crate::files::canonical_json(&record)?})])?;
            }
            let owner = client.one(
                "SELECT control_id,binding FROM _evm_checkpoint_control",
                &Default::default(),
            )?;
            ensure!(
                owner["control_id"] == control_id
                    && serde_json::from_str::<Value>(string(&owner, "binding")?)? == record,
                "database is bound to a different checkpoint controller"
            );
            if initialized.try_exists()? {
                ensure!(
                    fs::read_to_string(&initialized)? == control_id,
                    "checkpoint initialization marker is corrupt"
                );
            } else {
                atomic_write(&initialized, control_id.as_bytes(), false)?;
            }
        }
        let pins = path.join("pins");
        fs::create_dir_all(&pins)?;
        Ok(Self { path, pins, record })
    }

    pub fn reader(&self) -> Result<Lock> {
        file_lock(&self.path.join("readers.lock"), false, true)
    }
    pub fn publisher(&self) -> Result<PublisherLock> {
        let writer = file_lock(&self.path.join("writer.lock"), true, false)?;
        Ok(PublisherLock {
            _writer: writer,
            _readers: self.reader()?,
        })
    }
    pub fn retention(&self) -> Result<PublisherLock> {
        let writer = file_lock(&self.path.join("writer.lock"), true, false)?;
        Ok(PublisherLock {
            _writer: writer,
            _readers: file_lock(&self.path.join("readers.lock"), true, false)?,
        })
    }
}
