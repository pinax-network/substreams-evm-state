//! Private replay prefixes retain their format and require an independently checked digest.
use crate::{
    capacity,
    ch::{params, uint, ClickHouse},
    checkpoint,
    control::{new_id, object_id, Control},
    cursor,
    files::{atomic_json, canonical_json, file_lock, resolve},
    history::{self, SourceWriter},
    ingest::{self, IngestOptions, NativeOptions},
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

fn drop_old_generations(client: &ClickHouse, keep: &str) -> Result<()> {
    for table in ["bootstrap_generations", "bootstrap_storage"] {
        let old = client
            .rows(
                &format!(
                    "SELECT DISTINCT generation FROM {table} WHERE generation != {{id:String}}"
                ),
                &params(json!({"id":keep}))?,
            )?
            .collect::<Result<Vec<_>>>()?;
        for row in old {
            client.execute(
                &format!("ALTER TABLE {table} DROP PARTITION {{id:String}}"),
                &params(json!({"id":object_id(string(&row,"generation")?)?}))?,
            )?;
        }
    }
    Ok(())
}

fn cleanup(client: &ClickHouse, prefix: &Value) -> Result<Value> {
    // Keep the last prefix block and native marker. Every drop is resumable
    // because the durable, checked prefix pointer is committed first.
    let partitions = history::partitions_before(client, uint(&prefix["header"]["number"])?)?;
    history::drop_partitions(client, &partitions)?;
    drop_old_generations(client, string(prefix, "generation")?)?;
    Ok(partitions)
}

fn require_initial(checkpoints: &ClickHouse, accounts: &Value) -> Result<()> {
    let accounts = checkpoint::canonical_accounts(accounts)?
        .into_iter()
        .collect::<BTreeSet<_>>();
    for row in checkpoints.rows(
        "SELECT manifest FROM checkpoints FINAL",
        &Default::default(),
    )? {
        let manifest: Value = serde_json::from_str(string(&row?, "manifest")?)?;
        ensure!(
            !checkpoint::canonical_accounts(&manifest["accounts"])?
                .iter()
                .any(|a| accounts.contains(a)),
            "accounts already have a checkpoint; use checkpoint continuation and source retention"
        );
    }
    Ok(())
}

fn combined_usage(client: &ClickHouse, checkpoints: &ClickHouse) -> Result<u64> {
    client
        .disk_usage()?
        .checked_add(if client.database == checkpoints.database {
            0
        } else {
            checkpoints.disk_usage()?
        })
        .context("database size overflow")
}

pub fn compact(
    client: &ClickHouse,
    directory: &Path,
    end_block: Option<u64>,
    budget_bytes: u64,
) -> Result<Value> {
    ensure!(
        budget_bytes > 0,
        "bootstrap budget must be a positive integer"
    );
    let owner = SourceWriter::open(client, directory, true)?;
    compact_locked(client, &owner, end_block, budget_bytes)
}

fn compact_locked(
    client: &ClickHouse,
    owner: &SourceWriter,
    end_block: Option<u64>,
    budget_bytes: u64,
) -> Result<Value> {
    let paths = [owner.directory.clone(), owner.control_path.clone()];
    capacity::check(client, &paths, "bootstrap-start")?;
    require_initial(&owner.checkpoints, &owner.run["identity"]["accounts"])?;
    let progress = owner.progress(client)?;
    let tip = uint(&progress["position"]["block"]["number"])?;
    let end = end_block.unwrap_or(tip);
    ensure!(
        uint(&owner.run["identity"]["start_block"])? <= end && end <= tip,
        "bootstrap end must be within durable native progress"
    );
    let mut header=client.one("SELECT number,hash,parent_hash,state_root,timestamp FROM state_blocks FINAL WHERE number={end:UInt64}",&params(json!({"end":end}))?)?;
    for field in ["number", "timestamp"] {
        header[field] = json!(uint(&header[field])?);
    }
    let source = select_prefix(client, &owner.source)?;
    let previous = source.get("bootstrap");
    checkpoint::validate_interval(client, &source, &header)?;
    if let Some(previous) = previous.filter(|p| p["header"]["number"] == end) {
        let removed = cleanup(client, previous)?;
        owner.progress(client)?;
        let mut result = previous.clone();
        result["removed_partitions"] = removed;
        result["already_compacted"] = json!(true);
        return Ok(result);
    }
    for statement in include_str!("../sql/bootstrap.sql")
        .split(';')
        .filter(|s| !s.trim().is_empty())
    {
        client.execute(statement, &Default::default())?;
    }
    drop_old_generations(
        client,
        previous
            .map(|p| string(p, "generation"))
            .transpose()?
            .unwrap_or(""),
    )?;
    ensure!(
        combined_usage(client, &owner.checkpoints)? < budget_bytes,
        "bootstrap database budget already exhausted"
    );
    let generation = new_id();
    let mut arguments = params(json!({"id":generation,"end":end}))?;
    let union = checkpoint::union_storage(&[source.clone()], None, &mut arguments)?;
    client.execute(&format!("INSERT INTO bootstrap_storage SELECT {{id:String}},address,slot,argMax(value,position) AS final_value FROM ({union}) GROUP BY address,slot HAVING final_value != '{}'",checkpoint::ZERO),&arguments)?;
    let fields = checkpoint::observed_fields(client, &[source], None, &arguments)?;
    let measured = digest(
        client,
        &generation,
        &fields,
        &owner.run["identity"]["accounts"],
    )?;
    ensure!(
        combined_usage(client, &owner.checkpoints)? < budget_bytes,
        "bootstrap candidate exceeds database budget; previous prefix and source retained"
    );
    let record = json!({"format_version":1,"status":"unverified-bootstrap","binding":cursor::binding(&owner.run)?,"generation":generation,"start_block":owner.run["identity"]["start_block"],"header":header,"accounts":owner.run["identity"]["accounts"],"fields":fields,"state_sha256":measured["state_sha256"],"nonzero_slots":measured["nonzero_slots"]});
    client.insert_values(
        "bootstrap_generations",
        [json!({"generation":generation,"manifest":canonical_json(&record)?})],
    )?;
    capacity::check(client, &paths, "bootstrap-commit")?;
    atomic_json(&owner.directory.join("bootstrap.json"), &record, true)?;
    // Re-read and digest the committed state before local history is discarded.
    load_prefix(client, &owner.directory, Some(&owner.run))?
        .context("bootstrap pointer disappeared")?;
    let removed = cleanup(client, &record)?;
    owner.progress(client)?;
    let mut result = record;
    result["removed_partitions"] = removed;
    result["database_bytes"] = json!(client.disk_usage()?);
    Ok(result)
}

/// Resume bounded native chunks, compacting each into a private initial state.
/// Stop is exclusive. Publication still requires a complete proof bundle.
pub fn replay(
    client: &ClickHouse,
    options: &NativeOptions,
    ingest_options: &IngestOptions,
    stop_block: u64,
    chunk_blocks: u64,
    budget_bytes: u64,
) -> Result<Value> {
    ensure!(
        chunk_blocks > 0 && budget_bytes > 0,
        "bootstrap chunk blocks and budget must be positive integers"
    );
    ensure!(
        stop_block > options.start_block,
        "bootstrap stop block must be greater than start block (exclusive)"
    );
    ingest_options.validate(options.start_block)?;
    let run = ingest::prepare(client, options)?;
    let directory = resolve(&options.state_dir)?;
    let mut options = options.clone();
    options.package = directory.join("package.spkg");
    let destination = client.with_database(string(&run["identity"], "checkpoint_database")?)?;
    checkpoint::setup(&destination)?;
    let _replay = file_lock(&directory.join("bootstrap_replay.lock"), true, false)?;
    {
        let control = Control::open(&destination)?;
        let _publisher = control.publisher()?;
        require_initial(&destination, &run["identity"]["accounts"])?;
    }
    loop {
        let cursor_path = directory.join("cursor.txt");
        let next;
        if cursor_path.is_file() && !fs::read_to_string(&cursor_path)?.trim().is_empty() {
            {
                let _run = file_lock(&directory.join("run.lock"), true, false)?;
                let progress = cursor::save_progress(
                    client,
                    &run,
                    &directory,
                    fs::read_to_string(&cursor_path)?.trim(),
                )?;
                next = uint(&progress["position"]["block"]["number"])?
                    .checked_add(1)
                    .context("bootstrap block overflow")?;
            }
            ensure!(
                next <= stop_block,
                "bootstrap stop precedes the already ingested cursor"
            );
            let prefix = compact(client, &directory, None, budget_bytes)?;
            if next == stop_block {
                return Ok(
                    json!({"run_id":run["run_id"],"status":"unverified-bootstrap","generation":prefix["generation"],"header":prefix["header"],"nonzero_slots":prefix["nonzero_slots"],"state_sha256":prefix["state_sha256"],"source":history::declaration(&run["identity"])?}),
                );
            }
        } else {
            next = options.start_block;
        }
        ensure!(
            combined_usage(client, &destination)? < budget_bytes,
            "bootstrap database budget already exhausted"
        );
        let mut chunk = ingest_options.clone();
        chunk.stop_block = Some(next.saturating_add(chunk_blocks).min(stop_block));
        ingest::ingest(client, &options, &chunk)?;
    }
}
