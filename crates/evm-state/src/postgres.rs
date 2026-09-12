//! Coherent, fail-closed diagnostics for legacy PostgreSQL current-state tables.
//! These commands do not publish checkpoints or establish historical reads.
use crate::{
    header, process,
    proof::{address, fixed, string, unhex, verify_account, verify_complete, verify_metadata},
    rpc::RpcCall,
};
use anyhow::{ensure, Context, Result};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    process::Command,
    time::Duration,
};

pub struct Postgres {
    dsn: String,
    options: Option<String>,
}
impl Postgres {
    pub fn new(dsn: Option<&str>, options: Option<&str>) -> Self {
        Self {
            dsn: dsn.map(str::to_owned).unwrap_or_else(|| {
                std::env::var("PG_DSN").unwrap_or_else(|_| {
                    "postgresql://dev-node:insecure-change-me-in-prod@localhost:5432/dev-node"
                        .into()
                })
            }),
            options: options.map(str::to_owned),
        }
    }
    pub fn query(&self, query: &str) -> Result<String> {
        let mut command = Command::new("psql");
        command.args([
            "-XAtq",
            "--set=ON_ERROR_STOP=1",
            "--dbname",
            &self.dsn,
            "-c",
            query,
        ]);
        if let Some(options) = &self.options {
            command.env("PGOPTIONS", options);
        }
        let output=process::capture(&mut command,Duration::from_secs(120)).map_err(|_|anyhow::anyhow!("PostgreSQL snapshot query failed; check client availability and the 16 MiB output limit"))?.context("PostgreSQL snapshot query timed out")?;
        ensure!(
            output.status.success(),
            "PostgreSQL snapshot query failed; check the database connection and schema"
        );
        String::from_utf8(output.stdout).context("invalid PostgreSQL query output")
    }
    pub fn snapshot(&self, selected: Option<&str>, limit: Option<u64>) -> Result<Value> {
        let query = snapshot_query(selected, limit)?;
        let value: Value = serde_json::from_str(&self.query(&query)?)?;
        if let Some(selected) = selected {
            let selected = address(selected)?;
            ensure!(
                value["accounts"]
                    .as_array()
                    .context("invalid database accounts")?
                    .iter()
                    .map(|r| r["address"].clone())
                    .collect::<Vec<_>>()
                    == vec![json!(selected)],
                "selected account has no complete metadata row in the database"
            );
        }
        Ok(value)
    }
}

pub fn snapshot_query(selected: Option<&str>, limit: Option<u64>) -> Result<String> {
    let selected = selected.map(address).transpose()?;
    ensure!(
        limit.is_none_or(|limit| limit > 0),
        "storage sample limit must be a positive integer"
    );
    let where_account = selected
        .as_ref()
        .map(|a| format!("WHERE a.address='{a}'"))
        .unwrap_or_default();
    let where_storage = selected
        .as_ref()
        .map(|a| format!("WHERE address='{a}'"))
        .unwrap_or_default();
    let bound = limit.map(|n| format!("LIMIT {n}")).unwrap_or_default();
    // One statement makes all three subqueries share the same MVCC snapshot.
    Ok(format!("SELECT json_build_object(
      'header',(SELECT row_to_json(h) FROM (SELECT block_num AS number,block_hash AS hash,state_root FROM blocks ORDER BY block_num DESC LIMIT 1) h),
      'accounts',(SELECT coalesce(json_agg(row_to_json(a)),'[]'::json) FROM
        (SELECT a.address,a.nonce::text AS nonce,a.balance::text AS balance,a.code_hash,'0x'||encode(c.code,'hex') AS code,
          greatest(a.block_num,a.balance_block_num,a.nonce_block_num,a.code_block_num,c.first_block_num) AS last_block
         FROM accounts a LEFT JOIN code c ON a.code_hash=c.code_hash {where_account} ORDER BY a.address) a),
      'storage',(SELECT coalesce(json_agg(row_to_json(s)),'[]'::json) FROM
        (SELECT address,slot,value,block_num AS last_block FROM storage {where_storage} ORDER BY address,slot {bound}) s))"))
}

pub fn validate_snapshot(
    value: &Value,
    requested_block: Option<u64>,
) -> Result<BTreeMap<String, Value>> {
    let header = value
        .get("header")
        .filter(|h| h.is_object())
        .context("database snapshot has no head or account metadata")?;
    let rows = value["accounts"]
        .as_array()
        .filter(|a| !a.is_empty())
        .context("database snapshot has no head or account metadata")?;
    let number = header["number"].as_u64().context("invalid database head")?;
    ensure!(
        requested_block.is_none_or(|n| n == number),
        "current tables only support their captured head; --block cannot select historical state"
    );
    fixed::<32>(string(header, "hash")?)?;
    fixed::<32>(string(header, "state_root")?)?;
    let mut accounts = BTreeMap::new();
    for row in rows {
        let selected = address(string(row, "address")?)?;
        ensure!(
            !accounts.contains_key(&selected),
            "duplicate database account"
        );
        ensure!(
            ["nonce", "balance", "code_hash", "code"]
                .iter()
                .all(|key| row.get(key).is_some_and(|v| !v.is_null())),
            "missing required account metadata or bytecode; partial state cannot pass verification"
        );
        let nonce = match &row["nonce"] {
            Value::String(text) => {
                let n = text
                    .parse::<u64>()
                    .context("database nonce exceeds uint64 or is not an integer")?;
                ensure!(
                    n.to_string() == *text,
                    "database nonce must be an exact integer"
                );
                n
            }
            Value::Number(number) => number
                .as_u64()
                .context("database nonce exceeds uint64 or is not an integer")?,
            _ => anyhow::bail!("database nonce must be an exact integer"),
        };
        fixed::<32>(string(row, "code_hash")?)?;
        unhex(string(row, "code")?)?;
        let mut row = row.clone();
        row["nonce"] = json!(nonce);
        accounts.insert(selected, row);
    }
    let storage = value["storage"]
        .as_array()
        .context("invalid database storage")?;
    for row in rows.iter().chain(storage) {
        ensure!(
            row["last_block"]
                .as_u64()
                .is_some_and(|last| last <= number),
            "database state is newer than its block marker or has invalid provenance"
        );
    }
    let mut keys = BTreeSet::new();
    for row in storage {
        let selected = address(string(row, "address")?)?;
        ensure!(
            accounts.contains_key(&selected),
            "storage account has no verified metadata"
        );
        let key = fixed::<32>(string(row, "slot")?)?;
        fixed::<32>(string(row, "value")?)?;
        ensure!(
            keys.insert((selected, key)),
            "duplicate database storage slot"
        );
    }
    Ok(accounts)
}

pub fn verify(
    value: &Value,
    bundle: &Value,
    complete: bool,
    rpc: Option<&dyn RpcCall>,
    requested_block: Option<u64>,
    work: &Path,
) -> Result<Value> {
    let accounts = validate_snapshot(value, requested_block)?;
    let header = &value["header"];
    ensure!(
        bundle["chain_id"] == 56
            && ["number", "hash", "state_root"]
                .iter()
                .all(|key| header[key] == bundle["header"][key]),
        "database head differs from the finalized BSC proof header"
    );
    if let Some(encoded) = bundle.get("header_rlp") {
        header::verify_header(
            encoded.as_str().context("invalid encoded proof header")?,
            &bundle["header"],
            None,
        )?;
    }
    ensure!(
        accounts.keys().collect::<BTreeSet<_>>()
            == bundle["accounts"]
                .as_object()
                .context("invalid captured account proofs")?
                .keys()
                .collect(),
        "account coverage differs from captured proofs"
    );
    let mut count = 0_u64;
    for (selected, metadata) in &accounts {
        let proven = verify_account(
            string(header, "state_root")?,
            selected,
            &bundle["accounts"][selected]["proof"],
        )?;
        if complete {
            std::fs::create_dir_all(work)?;
            let directory = tempfile::Builder::new()
                .prefix("postgres-root-")
                .tempdir_in(work)?;
            let slots = value["storage"]
                .as_array()
                .unwrap()
                .iter()
                .filter_map(|row| {
                    // All addresses and fixed-width words were validated above.
                    if address(row["address"].as_str().unwrap()).unwrap() != *selected
                        || fixed::<32>(row["value"].as_str().unwrap())
                            .unwrap()
                            .iter()
                            .all(|b| *b == 0)
                    {
                        None
                    } else {
                        Some(Ok((
                            row["slot"].as_str().unwrap().into(),
                            row["value"].as_str().unwrap().into(),
                        )))
                    }
                });
            count += verify_complete(
                &proven,
                slots,
                string(metadata, "code")?,
                metadata,
                &directory.path().join("storage.sqlite"),
            )?;
        } else {
            verify_metadata(&proven, string(metadata, "code")?, metadata)?;
        }
    }
    if !complete {
        let rpc = rpc.context("sampled storage verification requires RPC")?;
        let number = header["number"].as_u64().unwrap();
        for row in value["storage"].as_array().unwrap() {
            let got = rpc.call(
                "eth_getStorageAt",
                json!([row["address"], row["slot"], format!("0x{number:x}")]),
            )?;
            ensure!(
                fixed::<32>(got.as_str().context("invalid RPC storage sample")?)?
                    == fixed::<32>(string(row, "value")?)?,
                "storage sample mismatch"
            );
            count += 1;
        }
        let after = rpc.call(
            "eth_getBlockByNumber",
            json!([format!("0x{number:x}"), false]),
        )?;
        ensure!(
            string(&after, "hash")?.to_ascii_lowercase() == string(header, "hash")?,
            "RPC header changed during sampled verification"
        );
    }
    Ok(
        json!({"status":if complete {"root-verified-diagnostic"} else {"sample-parity-only"},"header":header,"header_trust":bundle["header_trust"],"accounts":accounts.keys().collect::<Vec<_>>(),"storage_slots_checked":count,"storage_completeness_verified":complete,"account_metadata_and_code":"verified","published_checkpoint":false}),
    )
}
