//! Immutable checkpoint schema and coherent published-state reads.
use crate::{
    capacity,
    ch::{identifier, uint, Params},
    control::new_id,
    files::canonical_json,
    header::verify_header,
    proof::{fixed, verify_account, verify_complete},
    source::verified_source,
};
use crate::{
    ch::{params, ClickHouse},
    control::{object_id, Control},
    proof::{address, string},
};
use anyhow::{ensure, Context, Result};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::{
    fs,
    path::{Path, PathBuf},
};

pub const ZERO: &str = "0x0000000000000000000000000000000000000000000000000000000000000000";

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

pub fn validate_interval(client: &ClickHouse, source: &Value, header: &Value) -> Result<u64> {
    let start = uint(&source["start_block"])?;
    let end = uint(&header["number"])?;
    ensure!(
        start <= end && end < u64::MAX,
        "invalid checkpoint input range"
    );
    let mut expected = start;
    let mut count = 0;
    let mut previous = None::<String>;
    let accounts = canonical_accounts(&source["accounts"])?.join(",");
    if let Some(prefix) = source.get("bootstrap") {
        let number = uint(&prefix["header"]["number"])?;
        ensure!(
            source["start_block"] == prefix["start_block"] && number <= end,
            "checkpoint range cannot use this compacted bootstrap prefix"
        );
        previous = Some(string(&prefix["header"], "hash")?.into());
        expected = number + 1;
        count = expected - start;
        if expected == end + 1 {
            ensure!(
                prefix["header"]["hash"] == header["hash"]
                    && prefix["header"]["state_root"] == header["state_root"],
                "bootstrap checkpoint differs from proof header"
            );
        }
    }
    for row in client.rows("SELECT number, hash, parent_hash, state_root, accounts, schema_version, producer_version FROM state_blocks FINAL WHERE number >= {start:UInt64} AND number <= {end:UInt64} ORDER BY number, hash",&params(json!({"start":expected,"end":end}))?)? {
        let row=row?;let number=uint(&row["number"])?;
        ensure!(number==expected,"missing or conflicting block at {expected}");
        ensure!(row["accounts"]==accounts && uint(&row["schema_version"])?==1,"account filter or schema changed at block {number}");
        ensure!(matches!(uint(&row["producer_version"])?,3..=5),"unsupported producer version at block {number}");
        fixed::<32>(string(&row,"hash")?)?;fixed::<32>(string(&row,"parent_hash")?)?;
        if let Some(previous)=&previous {ensure!(row["parent_hash"]==*previous,"broken parent continuity at block {number}");}
        if number==end {ensure!(row["hash"]==header["hash"] && row["state_root"]==header["state_root"],"stream checkpoint differs from proof header");}
        if count==0 {if let Some(parent)=source.get("parent_hash") {ensure!(row["parent_hash"]==*parent,"stream does not continue the base checkpoint");}}
        previous=Some(string(&row,"hash")?.into());expected+=1;count+=1;
    }
    ensure!(
        expected == end + 1,
        "incomplete stream: expected through block {end}, got through block {}",
        expected.saturating_sub(1)
    );
    Ok(count)
}

pub(crate) fn union_storage(
    sources: &[Value],
    base: Option<&Value>,
    params: &mut Params,
) -> Result<String> {
    let mut queries = Vec::new();
    let mut resets = Vec::new();
    for (i, source) in sources.iter().enumerate() {
        let database = identifier(string(source, "database")?)?;
        params.insert(
            format!("start{i}"),
            source
                .get("delta_start")
                .unwrap_or(&source["start_block"])
                .clone(),
        );
        if let Some(prefix) = source.get("bootstrap") {
            params.insert(format!("prefix{i}"), prefix["generation"].clone());
            params.insert(
                format!("prefix_number{i}"),
                prefix["header"]["number"].clone(),
            );
            queries.push(format!("SELECT address,slot,value,tuple({{prefix_number{i}:UInt64}},toUInt64(0)) AS position FROM {database}.bootstrap_storage WHERE generation={{prefix{i}:String}}"));
        }
        queries.push(format!("SELECT storage.address AS address, storage.slot AS slot, storage.value AS value, tuple(number, storage.ordinal) AS position FROM {database}.state_blocks FINAL ARRAY JOIN storage WHERE number >= {{start{i}:UInt64}} AND number <= {{end:UInt64}}"));
        resets.push(format!("SELECT lifecycle.address AS address, tuple(number, lifecycle.ordinal) AS position FROM {database}.state_blocks FINAL ARRAY JOIN lifecycle WHERE number >= {{start{i}:UInt64}} AND number <= {{end:UInt64}} AND lifecycle.kind='storage_reset'"));
    }
    if let Some(base) = base {
        params.insert("base".into(), base["snapshot_id"].clone());
        params.insert("base_number".into(), base["header"]["number"].clone());
        queries.push("SELECT address, slot, value, tuple({base_number:UInt64}, toUInt64(0)) AS position FROM checkpoint_storage FINAL WHERE snapshot_id={base:String}".into());
    }
    ensure!(!queries.is_empty(), "checkpoint has no source state");
    let union = queries.join(" UNION ALL ");
    if resets.is_empty() {
        return Ok(union);
    }
    Ok(format!("SELECT address,slot,value,position FROM ({union}) AS changes LEFT JOIN (SELECT address,max(position) AS reset,count() AS reset_count FROM ({}) GROUP BY address) AS deletions USING(address) WHERE reset_count=0 OR position > reset",resets.join(" UNION ALL ")))
}

pub(crate) fn observed_fields(
    client: &ClickHouse,
    sources: &[Value],
    base: Option<&Value>,
    params: &Params,
) -> Result<Value> {
    let mut result = json!({});
    if base.is_some() {
        for row in client.rows("SELECT address, nonce, balance, code_hash, code FROM checkpoint_accounts FINAL WHERE snapshot_id={base:String}",params)? {
            let row=row?;result[string(&row,"address")?]=json!({"nonce":uint(&row["nonce"])?,"balance":row["balance"],"code_hash":row["code_hash"],"code":row["code"]});
        }
    }
    for (i, source) in sources.iter().enumerate() {
        let database = identifier(string(source, "database")?)?;
        if let Some(prefix) = source.get("bootstrap") {
            for (account, fields) in prefix["fields"]
                .as_object()
                .context("invalid bootstrap fields")?
            {
                if result.get(account).is_none() {
                    result[account] = json!({});
                }
                result[account].as_object_mut().unwrap().extend(
                    fields
                        .as_object()
                        .context("invalid account fields")?
                        .clone(),
                );
            }
        }
        for (group, fields) in [
            ("balances", vec!["value"]),
            ("nonces", vec!["value"]),
            ("codes", vec!["hash", "code"]),
        ] {
            let selection = fields
                .iter()
                .map(|field| {
                    format!("argMax({group}.{field}, tuple(number, {group}.ordinal)) AS {field}")
                })
                .collect::<Vec<_>>()
                .join(", ");
            let sql=format!("SELECT {group}.address AS address, {selection} FROM {database}.state_blocks FINAL ARRAY JOIN {group} WHERE number >= {{start{i}:UInt64}} AND number <= {{end:UInt64}} GROUP BY address");
            for row in client.rows(&sql, params)? {
                let row = row?;
                let account = string(&row, "address")?;
                if result.get(account).is_none() {
                    result[account] = json!({});
                }
                match group {
                    "balances" => result[account]["balance"] = row["value"].clone(),
                    "nonces" => result[account]["nonce"] = json!(uint(&row["value"])?),
                    _ => {
                        result[account]["code_hash"] = row["hash"].clone();
                        result[account]["code"] = row["code"].clone();
                    }
                }
            }
        }
    }
    Ok(result)
}

pub fn build(
    client: &ClickHouse,
    bundle: &Value,
    sources: &[Value],
    base_id: Option<&str>,
    budget_bytes: u64,
    work_dir: &Path,
) -> Result<Value> {
    build_observed(
        client,
        bundle,
        sources,
        base_id,
        budget_bytes,
        work_dir,
        &|| Ok(()),
    )
}

/// Observe the acknowledged candidate account write before its ready manifest.
/// Qualification uses this boundary for process/database crash injection. An
/// observer failure leaves the candidate unpublished and preserves prior readers.
pub fn build_observed(
    client: &ClickHouse,
    bundle: &Value,
    sources: &[Value],
    base_id: Option<&str>,
    budget_bytes: u64,
    work_dir: &Path,
    after_accounts: &dyn Fn() -> Result<()>,
) -> Result<Value> {
    setup(client)?;
    let owner = Control::open(client)?;
    let _publisher = owner.publisher()?;
    let end = uint(&bundle["header"]["number"])?;
    let mut inputs = Vec::new();
    for source in sources {
        inputs.push(verified_source(
            &client.with_database(string(source, "database")?)?,
            source,
            Some(client),
            Some(end),
        )?);
    }
    let checked = inputs
        .iter()
        .map(|guard| guard.source.clone())
        .collect::<Vec<_>>();
    build_unlocked(
        client,
        bundle,
        &checked,
        base_id,
        budget_bytes,
        work_dir,
        &owner.path,
        after_accounts,
    )
}

fn build_unlocked(
    client: &ClickHouse,
    bundle: &Value,
    sources: &[Value],
    base_id: Option<&str>,
    budget_bytes: u64,
    work_dir: &Path,
    control_path: &Path,
    after_accounts: &dyn Fn() -> Result<()>,
) -> Result<Value> {
    ensure!(
        bundle["format_version"] == 1 && bundle["chain_id"] == 56,
        "v0.1 qualification requires a BSC proof bundle"
    );
    let header = &bundle["header"];
    let target = uint(&header["number"])?;
    fixed::<32>(string(header, "hash")?)?;
    fixed::<32>(string(header, "state_root")?)?;
    ensure!(
        matches!(
            bundle["header_trust"].as_str(),
            Some("provider-finalized-header" | "operator-pinned-hash")
        ),
        "unrecognized header trust source"
    );
    if let Some(encoded) = bundle["header_rlp"].as_str().filter(|v| !v.is_empty()) {
        verify_header(encoded, header, None)?;
    } else {
        ensure!(
            bundle["header_trust"] != "operator-pinned-hash",
            "a pinned block hash requires its encoded header"
        );
    }
    let accounts = canonical_accounts(&bundle["accounts"])?;
    let base = base_id
        .map(|id| manifest_unlocked(client, id))
        .transpose()?;
    let base_accounts = base
        .as_ref()
        .map(|base| canonical_accounts(&base["accounts"]))
        .transpose()?
        .unwrap_or_default()
        .into_iter()
        .collect::<BTreeSet<_>>();
    let selected = accounts.iter().cloned().collect::<BTreeSet<_>>();
    if let Some(base) = &base {
        ensure!(
            target > uint(&base["header"]["number"])? && base_accounts.is_subset(&selected),
            "checkpoint must advance its base and preserve all base accounts"
        );
    }
    let mut sources = sources
        .iter()
        .map(|source| {
            crate::bootstrap::select_prefix(
                &client.with_database(string(source, "database")?)?,
                source,
            )
        })
        .collect::<Result<Vec<_>>>()?;
    let mut seen = BTreeSet::new();
    let mut source_bytes = 0_u64;
    for source in &mut sources {
        let cohort = canonical_accounts(&source["accounts"])?
            .into_iter()
            .collect::<BTreeSet<_>>();
        ensure!(cohort.is_disjoint(&seen), "source cohorts overlap");
        seen.extend(cohort.clone());
        if let Some(base) = &base {
            if !cohort.is_disjoint(&base_accounts) {
                ensure!(
                    uint(&source["start_block"])? == uint(&base["header"]["number"])? + 1,
                    "existing accounts must continue immediately after the base checkpoint"
                );
                source["parent_hash"] = base["header"]["hash"].clone();
            }
        }
        let db = client.with_database(string(source, "database")?)?;
        validate_interval(&db, source, header)?;
        if db.database != client.database {
            source_bytes = source_bytes
                .checked_add(db.disk_usage()?)
                .context("source byte count overflow")?;
        }
    }
    ensure!(
        seen == selected,
        "source account coverage differs from proof bundle"
    );
    let mut paths = vec![control_path.to_path_buf(), work_dir.to_path_buf()];
    for source in &sources {
        paths.push(PathBuf::from(string(source, "state_directory")?));
    }
    capacity::check(client, &paths, "checkpoint-start")?;
    ensure!(
        u128::from(client.disk_usage()?) + u128::from(source_bytes) < u128::from(budget_bytes),
        "retained-data budget already exhausted"
    );
    let snapshot_id = new_id();
    let mut query = params(json!({"id":snapshot_id,"end":target}))?;
    let union = union_storage(&sources, base.as_ref(), &mut query)?;
    client.execute(&format!("INSERT INTO checkpoint_storage SELECT {{id:String}}, address, slot, argMax(value, position) AS final_value FROM ({union}) GROUP BY address, slot HAVING final_value != '{ZERO}'"),&query)?;
    let observed = observed_fields(client, &sources, base.as_ref(), &query)?;
    fs::create_dir_all(work_dir)?;
    let mut digest = Sha256::new();
    let mut account_rows = Vec::new();
    let mut verification = json!({});
    let mut total_slots = 0_u64;
    for account in &accounts {
        let evidence = &bundle["accounts"][account];
        let proven = verify_account(string(header, "state_root")?, account, &evidence["proof"])?;
        let mut metadata = proven.json();
        if let Some(fields) = observed.get(account) {
            metadata.as_object_mut().unwrap().extend(
                fields
                    .as_object()
                    .context("invalid observed account fields")?
                    .clone(),
            );
        }
        let code = metadata
            .get("code")
            .unwrap_or(&evidence["code"])
            .as_str()
            .context("invalid account bytecode")?;
        query.insert("address".into(), json!(account));
        let rows=client.rows("SELECT slot,value FROM checkpoint_storage FINAL WHERE snapshot_id={id:String} AND address={address:String} ORDER BY slot",&query)?;
        let slots = rows.map(|row| {
            let row = row?;
            let slot = string(&row, "slot")?.to_owned();
            let value = string(&row, "value")?.to_owned();
            digest.update(format!("{account}{slot}{value}"));
            Ok((slot, value))
        });
        let directory = tempfile::Builder::new()
            .prefix("evm-state-trie-")
            .tempdir_in(work_dir)?;
        let count = verify_complete(
            &proven,
            slots,
            code,
            &metadata,
            &directory.path().join("storage.sqlite"),
        )?;
        capacity::check(client, &paths, "checkpoint-trie")?;
        let mut row = proven.json();
        row["code"] = json!(code);
        row["nonzero_slots"] = json!(count);
        digest.update(canonical_json(&row)?);
        row["snapshot_id"] = json!(snapshot_id);
        account_rows.push(row);
        verification[account] =
            json!({"nonzero_slots":count,"account_proof":"verified","storage_root":"verified"});
        total_slots = total_slots
            .checked_add(count)
            .context("checkpoint slot count overflow")?;
        ensure!(
            u128::from(client.disk_usage()?) + u128::from(source_bytes) < u128::from(budget_bytes),
            "checkpoint exceeds retained-data budget; candidate remains unpublished"
        );
        capacity::check(client, &paths, "checkpoint-account")?;
    }
    ensure!(uint(&client.one("SELECT count() AS count FROM checkpoint_storage FINAL WHERE snapshot_id={id:String}",&query)?["count"])?==total_slots,"checkpoint contains unverified or unexpected storage accounts");
    ensure!(
        observed
            .as_object()
            .unwrap()
            .keys()
            .all(|account| selected.contains(account)),
        "stream contains account metadata outside its declared filter"
    );
    client.insert_values("checkpoint_accounts", account_rows)?;
    after_accounts()?;
    let record = json!({"format_version":1,"snapshot_id":snapshot_id,"status":"ready","chain_id":bundle["chain_id"],"header":header,
        "header_trust":bundle["header_trust"],"accounts":accounts,"base_snapshot":base_id,"sources":sources,"verification":verification,
        "state_sha256":hex::encode(digest.finalize()),"proof_bundle":bundle,"created_at":crate::reader::now_ns()?,"retained_budget_bytes":budget_bytes,
        "account_count":accounts.len(),"nonzero_slots":total_slots});
    capacity::check(client, &paths, "checkpoint-publish")?;
    client.insert_values("checkpoints",[json!({"snapshot_id":snapshot_id,"block_number":target,"block_hash":header["hash"],"created_at":record["created_at"],"manifest":canonical_json(&record)?})])?;
    Ok(record)
}
