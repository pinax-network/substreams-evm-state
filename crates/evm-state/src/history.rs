//! Whole native-history partition cleanup with retained continuation protection.
use crate::{
    ch::{params, uint, ClickHouse},
    checkpoint::{canonical_accounts, manifest},
    control::{Control, PublisherLock},
    cursor,
    files::{file_lock, resolve, Lock},
    proof::string,
    source::verified_source,
};
use anyhow::{ensure, Context, Result};
use serde_json::{json, Value};
use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
};

const PARTITIONS: [(&str, &str); 2] = [
    ("state_blocks", "toDate(_block_timestamp_)"),
    ("_blocks_", "toYYYYMM(timestamp)"),
];

pub fn partitions_before(client: &ClickHouse, before: u64) -> Result<Value> {
    let mut schemas = serde_json::Map::new();
    for row in client.rows("SELECT name,partition_key FROM system.tables WHERE database={db:String} AND name IN ('state_blocks','_blocks_')", &params(json!({"db":client.database}))?)? {
        let row = row?;
        ensure!(schemas.insert(string(&row,"name")?.into(), row["partition_key"].clone()).is_none(), "duplicate native history table");
    }
    ensure!(
        Value::Object(schemas)
            == json!(PARTITIONS
                .into_iter()
                .collect::<std::collections::BTreeMap<_, _>>()),
        "native history schema does not match the qualified partition layout"
    );
    let mut result = json!({});
    for (table, _) in PARTITIONS {
        result[table] = json!(client.rows(&format!("SELECT _partition_id AS id,min(number) AS first,max(number) AS last FROM {table} GROUP BY _partition_id HAVING last < {{before:UInt64}} ORDER BY first"), &params(json!({"before":before}))?)?.collect::<Result<Vec<_>>>()?);
    }
    Ok(result)
}

pub(crate) fn drop_partitions(client: &ClickHouse, partitions: &Value) -> Result<()> {
    for (table, _) in PARTITIONS {
        for partition in partitions[table]
            .as_array()
            .context("invalid partition plan")?
        {
            client.execute(
                &format!("ALTER TABLE {table} DROP PARTITION ID {{id:String}}"),
                &params(json!({"id":string(partition,"id")?}))?,
            )?;
        }
    }
    Ok(())
}

pub(crate) fn declaration(identity: &Value) -> Result<Value> {
    let mut source = json!({});
    for key in [
        "database",
        "accounts",
        "start_block",
        "module_hash",
        "final_blocks_only",
    ] {
        source[key] = identity
            .get(key)
            .context("incomplete native source identity")?
            .clone();
    }
    Ok(source)
}

/// Lock order matches checkpoint publication and excludes both native writers and
/// checkpoint readers of this source. The initial shared lock is dropped before
/// the exclusive reader lock is acquired; the run lock prevents identity changes.
pub(crate) struct SourceWriter {
    pub directory: PathBuf,
    pub run: Value,
    pub source: Value,
    pub checkpoints: ClickHouse,
    pub control_path: PathBuf,
    _publisher: PublisherLock,
    _run: Lock,
    _readers: Lock,
}
impl SourceWriter {
    pub fn open(client: &ClickHouse, directory: &Path, setup: bool) -> Result<Self> {
        let directory = resolve(directory)?;
        let run: Value = serde_json::from_slice(&fs::read(directory.join("run.json"))?)?;
        ensure!(
            run["identity"]["format_version"] == 3,
            "native cleanup requires destination-bound run identity format 3"
        );
        let checkpoints = client.with_database(string(&run["identity"], "checkpoint_database")?)?;
        if setup {
            crate::checkpoint::setup(&checkpoints)?;
        }
        let control = Control::open(&checkpoints)?;
        let publisher = control.publisher()?;
        let run_lock = file_lock(&directory.join("run.lock"), true, false)?;
        let checked = verified_source(
            client,
            &declaration(&run["identity"])?,
            Some(&checkpoints),
            None,
        )?;
        ensure!(
            checked.source["state_directory"]
                == directory.to_str().context("invalid source directory")?
                && checked.source["run_id"] == run["run_id"],
            "cleanup directory does not own this native source"
        );
        let source = checked.source.clone();
        drop(checked);
        let readers = file_lock(&directory.join("source_readers.lock"), true, false)?;
        Ok(Self {
            directory,
            run,
            source,
            checkpoints,
            control_path: control.path,
            _publisher: publisher,
            _run: run_lock,
            _readers: readers,
        })
    }
    pub fn progress(&self, client: &ClickHouse) -> Result<Value> {
        let progress = cursor::load_progress(client, &self.run, &self.directory)?;
        ensure!(
            fs::read_to_string(self.directory.join("cursor.txt"))?.trim()
                == string(&progress, "cursor")?,
            "native cursor differs from durable progress; recover or finish the run before cleanup"
        );
        Ok(progress)
    }
}

fn plan(
    client: &ClickHouse,
    owner: &SourceWriter,
    snapshot_id: &str,
    keep_blocks: u64,
) -> Result<Value> {
    ensure!(keep_blocks > 0, "keep_blocks must be at least one");
    let ready = manifest(&owner.checkpoints, snapshot_id)?;
    let covered = ready["sources"]
        .as_array()
        .context("invalid checkpoint sources")?
        .iter()
        .filter(|s| s.get("run_id") == owner.run.get("run_id"))
        .collect::<Vec<_>>();
    ensure!(
        covered.len() == 1
            && [
                "database_uuid",
                "package_sha256",
                "schema_hash",
                "module_hash",
                "accounts"
            ]
            .iter()
            .all(|key| covered[0].get(key) == owner.source.get(key)),
        "checkpoint does not cover this exact native source"
    );
    let selected = canonical_accounts(&owner.run["identity"]["accounts"])?
        .into_iter()
        .collect::<BTreeSet<_>>();
    ensure!(
        selected.is_subset(
            &canonical_accounts(&ready["accounts"])?
                .into_iter()
                .collect()
        ),
        "checkpoint does not cover all source accounts"
    );
    let progress = owner.progress(client)?;
    let tip = uint(&progress["position"]["block"]["number"])?;
    let mut floor = uint(&ready["header"]["number"])?;
    ensure!(
        tip >= floor,
        "native progress has not reached the checkpoint"
    );
    let mut protected = Vec::new();
    for row in owner.checkpoints.rows(
        "SELECT snapshot_id,manifest FROM checkpoints FINAL",
        &Default::default(),
    )? {
        let row = row?;
        let record: Value = serde_json::from_str(string(&row, "manifest")?)?;
        ensure!(
            record["status"] == "ready" && record["snapshot_id"] == row["snapshot_id"],
            "invalid retained checkpoint prevents native history cleanup"
        );
        let accounts = canonical_accounts(&record["accounts"])?;
        if accounts.iter().any(|a| selected.contains(a)) {
            floor = floor.min(uint(&record["header"]["number"])?);
            protected.push(string(&record, "snapshot_id")?.to_owned());
        }
    }
    protected.sort();
    let before = floor
        .saturating_add(1)
        .min(tip.saturating_sub(keep_blocks - 1));
    Ok(
        json!({"source":owner.source,"checkpoint":snapshot_id,"protected_checkpoints":protected,"remove_before":before,"keep_blocks":keep_blocks,"durable_block":progress["position"]["block"],"partitions":partitions_before(client,before)?}),
    )
}

pub fn cleanup(
    client: &ClickHouse,
    directory: &Path,
    snapshot_id: &str,
    keep_blocks: u64,
    apply: bool,
) -> Result<Value> {
    ensure!(keep_blocks > 0, "keep_blocks must be at least one");
    let owner = SourceWriter::open(client, directory, false)?;
    let mut result = plan(client, &owner, snapshot_id, keep_blocks)?;
    result["bytes_before"] = json!(client.disk_usage()?);
    if apply {
        drop_partitions(client, &result["partitions"])?;
        owner.progress(client)?;
    }
    result["applied"] = json!(apply);
    result["bytes_after"] = json!(client.disk_usage()?);
    result["space_reclamation"] = json!(
        "inactive parts may remain until ClickHouse background cleanup; byte counts include them"
    );
    Ok(result)
}
