//! Diagnostics for observed lifecycle updates. None of these paths publish a
//! checkpoint or claim that untouched account storage has been enumerated.
use crate::{
    ch::{params, uint, ClickHouse},
    checkpoint::{self, ZERO},
    files,
    header::verify_header,
    proof::{fixed, quantity, string, unhex, verify_account},
    rpc::RpcCall,
    source::verified_source,
    trie_qualification::file_hash,
};
use alloy_primitives::{keccak256, U256};
use anyhow::{ensure, Context, Result};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{collections::BTreeSet, fs, path::Path};

fn object(value: &Value) -> Result<&serde_json::Map<String, Value>> {
    value
        .as_object()
        .context("expected lifecycle metadata object")
}
fn array(value: &Value) -> Result<&Vec<Value>> {
    value.as_array().context("expected lifecycle array")
}
fn read(path: &Path) -> Result<Value> {
    Ok(serde_json::from_slice(&fs::read(path)?)?)
}
fn declared(run: &Value) -> Value {
    let mut result = json!({});
    for key in [
        "database",
        "accounts",
        "start_block",
        "module_hash",
        "final_blocks_only",
    ] {
        result[key] = run["identity"][key].clone();
    }
    result
}
fn chain(rpc: &impl RpcCall) -> Result<()> {
    let value = rpc.call("eth_chainId", json!([]))?;
    ensure!(
        quantity(value.as_str().context("invalid chain ID")?, 256)? == U256::from(56),
        "expected the BSC RPC"
    );
    Ok(())
}
fn same_header(rpc: &impl RpcCall, header: &Value) -> Result<()> {
    let number = uint(&header["number"])?;
    let current = rpc.call(
        "eth_getBlockByNumber",
        json!([format!("0x{number:x}"), false]),
    )?;
    ensure!(
        quantity(string(&current, "number")?, 64)?.to::<u64>() == number
            && fixed::<32>(string(&current, "hash")?)? == fixed::<32>(string(header, "hash")?)?,
        "native/captured/current RPC block identity differs"
    );
    Ok(())
}
pub fn matching_fields(actual: &Value, expected: &Value) -> Result<()> {
    for (key, value) in object(actual)? {
        ensure!(
            expected.get(key) == Some(value),
            "observed native metadata differs from captured or proven state: {key}"
        );
    }
    Ok(())
}
fn without_code(value: &Value) -> Result<Value> {
    let mut result = object(value)?.clone();
    result.remove("code");
    Ok(Value::Object(result))
}
fn group(row: &Value, group: &str, fields: &[&str]) -> Result<Vec<Value>> {
    let addresses = array(&row[format!("{group}.address")])?;
    let mut result = addresses
        .iter()
        .map(|address| json!({"address":address}))
        .collect::<Vec<_>>();
    for field in fields {
        let values = array(&row[format!("{group}.{field}")])?;
        ensure!(
            values.len() == result.len(),
            "malformed native Nested arrays"
        );
        for (record, value) in result.iter_mut().zip(values) {
            record[*field] = if *field == "ordinal" {
                json!(uint(value)?)
            } else {
                value.clone()
            };
        }
    }
    Ok(result)
}
fn archive(rpc: &impl RpcCall, address: &str, side: &Value, balance: bool) -> Result<()> {
    let block = format!("0x{:x}", uint(&side["block_number"])?);
    let code = rpc.call("eth_getCode", json!([address, block]))?;
    let nonce = rpc.call("eth_getTransactionCount", json!([address, block]))?;
    ensure!(
        code.as_str()
            .context("invalid archive bytecode")?
            .to_ascii_lowercase()
            == side["code"]
            && quantity(nonce.as_str().context("invalid archive nonce")?, 64)?.to::<u64>()
                == uint(&side["nonce"])?,
        "archive metadata differs from captured state"
    );
    if balance {
        let value = rpc.call("eth_getBalance", json!([address, block]))?;
        ensure!(
            quantity(value.as_str().context("invalid archive balance")?, 256)?.to_string()
                == side["balance"],
            "archive balance differs from capture"
        );
    }
    Ok(())
}

pub fn updates(
    client: &ClickHouse,
    rpc: &impl RpcCall,
    directory: &Path,
    proofs: &Path,
    output: &Path,
) -> Result<Value> {
    ensure!(!output.try_exists()?, "output already exists");
    let directory = files::resolve(directory)?;
    let _writer = files::file_lock(&directory.join("run.lock"), true, false)?;
    let run = read(&directory.join("run.json"))?;
    let source = declared(&run);
    let raw = fs::read(proofs)?;
    let bundle: Value = serde_json::from_slice(&raw)?;
    ensure!(
        bundle["format_version"] == 1
            && bundle["chain_id"] == 56
            && matches!(
                bundle["header_trust"].as_str(),
                Some("provider-finalized-header" | "operator-pinned-hash")
            ),
        "expected a BSC proof bundle with declared header trust"
    );
    let header = &bundle["header"];
    verify_header(string(&bundle, "header_rlp")?, header, None)?;
    ensure!(
        json!(object(&bundle["accounts"])?.keys().collect::<Vec<_>>()) == source["accounts"],
        "proof bundle and native filter differ"
    );
    chain(rpc)?;
    let end = uint(&header["number"])?;
    let checked = verified_source(client, &source, None, Some(end))?;
    ensure!(
        checked.source["state_directory"]
            == directory.to_str().context("invalid source directory")?,
        "directory does not own native source"
    );
    let blocks = checkpoint::validate_interval(client, &checked.source, header)?;
    let sources = [checked.source.clone()];
    let mut parameters = params(json!({"start0":source["start_block"],"end":end}))?;
    let observed = checkpoint::observed_fields(client, &sources, None, &parameters)?;
    ensure!(
        !object(&observed)?.is_empty()
            && object(&observed)?
                .keys()
                .all(|a| bundle["accounts"].get(a).is_some()),
        "native account field coverage is empty or unexpected"
    );
    let mut metadata = json!({});
    for (address, value) in object(&bundle["accounts"])? {
        let proven = verify_account(string(header, "state_root")?, address, &value["proof"])?;
        ensure!(
            keccak256(unhex(string(value, "code")?)?) == proven.code_hash,
            "saved bytecode differs from account proof"
        );
        let mut expected = proven.json();
        expected["code"] = value["code"].clone();
        let fields = observed.get(address).cloned().unwrap_or_else(|| json!({}));
        matching_fields(&fields, &expected)?;
        metadata[address] = fields;
    }
    let query = checkpoint::union_storage(&sources, None, &mut parameters)?;
    let mut counts = std::collections::BTreeMap::<String, u64>::new();
    let mut digest = Sha256::new();
    for row in client.rows(&format!("SELECT address,slot,argMax(value,position) AS value FROM ({query}) GROUP BY address,slot ORDER BY address,slot"),&parameters)? {
        let row=row?;let address=string(&row,"address")?;let slot=string(&row,"slot")?;
        ensure!(bundle["accounts"].get(address).is_some(),"unexpected storage account");
        let value=rpc.call("eth_getStorageAt",json!([address,slot,format!("0x{end:x}")]))?;
        let value=fixed::<32>(value.as_str().context("invalid archive slot")?)?;
        ensure!(value == fixed::<32>(string(&row,"value")?)?,"native storage differs from archive RPC");
        digest.update(fixed::<20>(address)?);digest.update(fixed::<32>(slot)?);digest.update(value);
        *counts.entry(address.into()).or_default()+=1;
    }
    same_header(rpc, header)?;
    let result = json!({"format_version":1,"chain_id":56,"header":header,"header_trust":bundle["header_trust"],"start_block":source["start_block"],"blocks":blocks,
        "database":client.database,"run_id":run["run_id"],"module_hash":source["module_hash"],"package_sha256":run["identity"]["package_sha256"],
        "proof_bundle_sha256":hex::encode(Sha256::digest(raw)),"metadata_verified_against_account_proofs":metadata,
        "final_touched_slots_verified_against_archive_rpc":counts,"ordered_slot_values_sha256":hex::encode(digest.finalize()),
        "slot_digest_encoding":"ascending address/slot, concatenated 20-byte address and 32-byte slot/value",
        "qualification":"Observed updates only; untouched storage completeness and BSC consensus are not verified."});
    files::atomic_json(output, &result, false)?;
    Ok(result)
}

#[derive(Clone, Copy, clap::ValueEnum)]
pub enum CapturedKind {
    Clears,
    Selfdestruct,
    Recreation,
}
const RECREATED: &str = "0xe82c715e37f2f2e190dd2ca86fb796cafaf0beff";
pub fn clear_case(filename: &str) -> Result<(&'static str, Option<u64>)> {
    Ok(match filename {
        "v4-failed-clear-reinstall.pb" => {
            ("0x2eecb88952aced531a7b29ac7320feca57e73a62", Some(9041))
        }
        "v5-failed-authority-clear.pb" => {
            ("0x73d718b4cf0d2d86eb4ac522f6fedf599bbbdfb7", Some(3442))
        }
        "v5-failed-self-clear.pb" => ("0xbbb90cdb4e271be14df46b7e84f4fbf3bab17b6e", Some(1170)),
        "v5-invalid-self-clear-noop.pb" => ("0x213864e51cdacf3fdacbdc12726dba9f12167514", None),
        _ => anyhow::bail!("unknown captured clear fixture"),
    })
}
pub fn validate_capture(record: &Value, fixture_dir: &Path) -> Result<()> {
    for (name, checksum) in [("filename", "sha256"), ("header_filename", "header_sha256")] {
        let filename = string(record, name)?;
        ensure!(
            Path::new(filename).components().count() == 1 && !filename.starts_with('.'),
            "invalid captured fixture path"
        );
        ensure!(
            file_hash(&fixture_dir.join(filename))? == string(record, checksum)?,
            "captured transaction or header changed"
        );
    }
    ensure!(
        fs::metadata(fixture_dir.join(string(record, "filename")?))?.len()
            == uint(&record["bytes"])?,
        "captured transaction length changed"
    );
    verify_header(
        string(record, "header_rlp")?,
        &record["header"],
        Some(string(record, "block_hash")?),
    )?;
    ensure!(
        record["header"]["number"] == record["block"],
        "captured block number differs from header"
    );
    for states in object(&record["rpc_block_end_state"])?.values() {
        for side in object(states)?.values() {
            ensure!(
                keccak256(unhex(string(side, "code")?)?).as_slice()
                    == fixed::<32>(string(side, "code_hash")?)?,
                "captured bytecode differs from its checksum"
            );
        }
    }
    Ok(())
}
pub fn clear_comparison(record: &Value, row: &Value, observed: &Value) -> Result<Value> {
    let filename = string(record, "filename")?;
    let (authority, ordinal) = clear_case(filename)?;
    let mut fields = json!({});
    for (address, sides) in object(&record["rpc_block_end_state"])? {
        let actual = observed
            .get(address)
            .context("captured account has no native metadata")?;
        ensure!(
            actual.get("nonce").is_some(),
            "native clear metadata lacks nonce"
        );
        matching_fields(actual, &sides["after"])?;
        fields[address] = without_code(actual)?;
    }
    let selected = |values: Vec<Value>| -> Vec<Value> {
        values
            .into_iter()
            .filter(|r| r["address"] == authority)
            .map(|mut r| {
                r.as_object_mut().unwrap().remove("address");
                r
            })
            .collect()
    };
    let codes = selected(group(row, "codes", &["code", "ordinal"])?);
    let markers = selected(group(row, "lifecycle", &["kind", "ordinal"])?);
    let before = &record["rpc_block_end_state"][authority]["before"];
    let after = &record["rpc_block_end_state"][authority]["after"];
    ensure!(
        array(&row["storage.address"])?.is_empty(),
        "unexpected storage patches in captured clear block"
    );
    ensure!(
        before["code"] != "0x",
        "capture does not start with delegation"
    );
    if let Some(ordinal) = ordinal {
        ensure!(
            markers == vec![json!({"kind":"code_cleared","ordinal":ordinal})],
            "native clear marker differs from capture"
        );
        if filename == "v4-failed-clear-reinstall.pb" {
            ensure!(
                codes == vec![json!({"code":after["code"],"ordinal":9043})]
                    && after["code"] != "0x"
                    && before["code"] != after["code"]
                    && observed[authority]["code"] == after["code"],
                "native clear/reinstallation ordering differs from capture"
            );
        } else {
            ensure!(
                codes == vec![json!({"code":"0x","ordinal":ordinal})]
                    && after["code"] == "0x"
                    && observed[authority]["code"] == "0x",
                "native pre-execution clear differs from capture"
            );
        }
    } else {
        ensure!(
            codes.is_empty() && markers.is_empty() && before["code"] == after["code"],
            "invalid authorization unexpectedly clears delegation"
        );
    }
    Ok(
        json!({"fixture":filename,"fixture_sha256":record["sha256"],"block":record["block"],"block_hash":row["hash"],"transaction_hash":record["hash"],
        "authority":authority,"native_metadata":fields,"native_code_patches":codes,"native_lifecycle":markers,"storage_patches":0,
        "code_bytes_before":unhex(string(before,"code")?)?.len(),"code_bytes_after":unhex(string(after,"code")?)?.len()}),
    )
}

pub fn captured(
    client: &ClickHouse,
    rpc: &impl RpcCall,
    directory: &Path,
    fixture_dir: &Path,
    kind: CapturedKind,
    requested: &[String],
    output: &Path,
) -> Result<Value> {
    ensure!(!output.try_exists()?, "output already exists");
    let manifest = read(&fixture_dir.join("manifest.json"))?;
    ensure!(manifest["chain_id"] == 56, "expected BSC capture manifest");
    let mut records = array(&manifest["records"])?
        .iter()
        .filter(|r| match kind {
            CapturedKind::Recreation => r["filename"]
                .as_str()
                .is_some_and(|n| n.starts_with("v3-metamorphic-")),
            _ => requested.iter().any(|n| r["filename"] == *n),
        })
        .cloned()
        .collect::<Vec<_>>();
    records.sort_by_key(|r| r["block"].as_u64());
    match kind {
        CapturedKind::Recreation => ensure!(
            requested.is_empty() && records.len() == 5,
            "expected five captured recreation/storage cases"
        ),
        CapturedKind::Clears => {
            ensure!(
                !requested.is_empty()
                    && records.len() == requested.iter().collect::<BTreeSet<_>>().len(),
                "missing captured clear case"
            );
            for record in &records {
                clear_case(string(record, "filename")?)?;
            }
        }
        CapturedKind::Selfdestruct => ensure!(
            records.len() == 1
                && requested.len() == 1
                && (3..=5).any(|version| requested[0]
                    == format!("v{version}-post-cancun-existing-selfdestruct.pb")),
            "expected one captured post-Cancun SELFDESTRUCT fixture"
        ),
    }
    for record in &records {
        validate_capture(record, fixture_dir)?;
    }
    let directory = files::resolve(directory)?;
    let _writer = files::file_lock(&directory.join("run.lock"), true, false)?;
    let run = read(&directory.join("run.json"))?;
    let source = declared(&run);
    let start = uint(&source["start_block"])?;
    let first = uint(&records[0]["block"])?;
    let header = &records.last().unwrap()["header"];
    let end = uint(&header["number"])?;
    ensure!(
        start < first,
        "native interval must include the preceding block"
    );
    if !matches!(kind, CapturedKind::Clears) {
        ensure!(
            start + 1 == first,
            "expected the captured interval and its predecessor"
        );
    }
    if matches!(kind, CapturedKind::Recreation) {
        ensure!(
            source["accounts"] == json!([RECREATED]),
            "unexpected recreation native filter"
        );
    }
    chain(rpc)?;
    let checked = verified_source(client, &source, None, Some(end))?;
    ensure!(
        checked.source["state_directory"] == directory.to_str().context("invalid directory")?,
        "directory does not own native source"
    );
    let blocks = checkpoint::validate_interval(client, &checked.source, header)?;
    let sources = [checked.source.clone()];
    let mut comparisons = Vec::new();
    let mut surviving = json!({});
    let mut known_slots = BTreeSet::new();
    if matches!(kind, CapturedKind::Recreation) {
        for record in &records {
            known_slots.extend(
                object(&record["rpc_block_end_state"][RECREATED]["after"]["storage"])?
                    .keys()
                    .cloned(),
            );
        }
    }
    for record in &records {
        let number = uint(&record["block"])?;
        let mut parameters = params(json!({"start0":start,"end":number}))?;
        let observed = checkpoint::observed_fields(client, &sources, None, &parameters)?;
        let row = client.one(
            "SELECT * FROM state_blocks FINAL WHERE number={end:UInt64}",
            &parameters,
        )?;
        ensure!(
            row["hash"] == record["block_hash"],
            "native and captured block identity differ"
        );
        same_header(rpc, &record["header"])?;
        match kind {
            CapturedKind::Clears => {
                for (address, sides) in object(&record["rpc_block_end_state"])? {
                    ensure!(
                        array(&source["accounts"])?.contains(&json!(address)),
                        "native filter lacks captured account"
                    );
                    archive(rpc, address, &sides["before"], true)?;
                    archive(rpc, address, &sides["after"], true)?;
                }
                comparisons.push(clear_comparison(record, &row, &observed)?);
            }
            CapturedKind::Selfdestruct => {
                let accounts = object(&record["rpc_block_end_state"])?
                    .keys()
                    .filter(|a| json!(*a) != record["from"])
                    .cloned()
                    .collect::<BTreeSet<_>>();
                ensure!(
                    !accounts.is_empty()
                        && accounts.iter().all(|a| source["accounts"]
                            .as_array()
                            .is_some_and(|v| v.contains(&json!(a)))),
                    "native filter lacks captured SELFDESTRUCT accounts"
                );
                for name in ["codes.address", "nonces.address", "storage.address"] {
                    ensure!(
                        array(&row[name])?
                            .iter()
                            .all(|a| !accounts.contains(a.as_str().unwrap_or(""))),
                        "unexpected code/nonce/storage patch for surviving account"
                    );
                }
                let markers = group(&row, "lifecycle", &["kind", "ordinal"])?
                    .into_iter()
                    .filter(|r| accounts.contains(r["address"].as_str().unwrap_or("")))
                    .collect::<Vec<_>>();
                ensure!(
                    markers
                        .iter()
                        .map(|r| r["address"].as_str().unwrap().to_owned())
                        .collect::<BTreeSet<_>>()
                        == accounts
                        && markers.iter().all(|r| r["kind"] == "selfdestruct"),
                    "expected diagnostic SELFDESTRUCT without account deletion"
                );
                for address in accounts {
                    let saved = &record["rpc_block_end_state"][&address];
                    let before = &saved["before"];
                    let after = &saved["after"];
                    ensure!(
                        before["code"] != "0x"
                            && before["code"] == after["code"]
                            && before["nonce"] == after["nonce"],
                        "capture does not demonstrate a surviving existing account"
                    );
                    archive(rpc, &address, before, false)?;
                    archive(rpc, &address, after, false)?;
                    let fields = observed.get(&address).cloned().unwrap_or_else(|| json!({}));
                    matching_fields(&fields, after)?;
                    surviving[&address] = json!({"code_hash_before_and_after":before["code_hash"],"code_bytes_before_and_after":unhex(string(before,"code")?)?.len(),"nonce_before_and_after":before["nonce"],"observed_native_fields":fields,"selfdestruct_ordinals":markers.iter().filter(|r|r["address"]==address).map(|r|r["ordinal"].clone()).collect::<Vec<_>>()});
                }
            }
            CapturedKind::Recreation => {
                ensure!(
                    object(&observed)?.len() == 1
                        && object(&observed[RECREATED])?
                            .keys()
                            .map(String::as_str)
                            .collect::<BTreeSet<_>>()
                            == BTreeSet::from(["balance", "nonce", "code", "code_hash"]),
                    "unexpected recreation metadata coverage"
                );
                matching_fields(
                    &observed[RECREATED],
                    &record["rpc_block_end_state"][RECREATED]["after"],
                )?;
                let union = checkpoint::union_storage(&sources, None, &mut parameters)?;
                let mut slots = std::collections::BTreeMap::new();
                for value in client.rows(
                    &format!(
                        "SELECT slot,argMax(value,position) AS value FROM ({union}) GROUP BY slot"
                    ),
                    &parameters,
                )? {
                    let value = value?;
                    slots.insert(string(&value, "slot")?.to_owned(), value["value"].clone());
                }
                let mut checks = json!({});
                for slot in &known_slots {
                    let value = rpc.call(
                        "eth_getStorageAt",
                        json!([RECREATED, slot, format!("0x{number:x}")]),
                    )?;
                    let value = value
                        .as_str()
                        .context("invalid archive slot")?
                        .to_ascii_lowercase();
                    ensure!(
                        fixed::<32>(&value)?
                            == fixed::<32>(
                                slots.get(slot).and_then(Value::as_str).unwrap_or(ZERO)
                            )?,
                        "native storage/reset differs from archive RPC"
                    );
                    checks[slot] = json!(value);
                }
                let lifecycle = group(&row, "lifecycle", &["kind", "ordinal"])?
                    .into_iter()
                    .map(|mut r| {
                        r.as_object_mut().unwrap().remove("address");
                        r
                    })
                    .collect::<Vec<_>>();
                comparisons.push(json!({"block":number,"block_hash":row["hash"],"transaction_hash":record["hash"],"fixture":record["filename"],"fixture_sha256":record["sha256"],"native_metadata":without_code(&observed[RECREATED])?,"code_bytes":unhex(string(&observed[RECREATED],"code")?)?.len(),"lifecycle":lifecycle,"storage_patches":array(&row["storage.slot"] )?.len(),"tracked_slot_values":checks}));
            }
        }
    }
    let mut result = json!({"format_version":1,"chain_id":56,"database":client.database,"start_block":start,"header":header,"blocks":blocks,"run_id":run["run_id"],"module_hash":source["module_hash"],"package_sha256":run["identity"]["package_sha256"]});
    match kind {
        CapturedKind::Clears => {
            result["comparisons"] = json!(comparisons);
            result["qualification"]=json!("Exact captured block-end updates and clear markers match archive RPC. No untouched storage completeness, account-root proof or ready checkpoint is claimed.");
        }
        CapturedKind::Selfdestruct => {
            let record = &records[0];
            for (key, value) in [
                ("fixture", &record["filename"]),
                ("fixture_sha256", &record["sha256"]),
                ("transaction_hash", &record["hash"]),
            ] {
                result[key] = value.clone();
            }
            result["comparisons"] = surviving;
            result["qualification"]=json!("Native diagnostic markers and unchanged code/nonce agree with captured archive RPC. No complete initial storage, historical account-root proof or ready checkpoint is claimed.");
        }
        CapturedKind::Recreation => {
            result["account"] = json!(RECREATED);
            result["comparisons"] = json!(comparisons);
            result["storage_patches_in_interval"]=json!(uint(&client.one("SELECT sum(length(`storage.slot`)) AS n FROM state_blocks FINAL WHERE number BETWEEN {start:UInt64} AND {end:UInt64}",&params(json!({"start":start,"end":end}))?)?["n"])?);
            result["qualification"]=json!("Native metadata and lifecycle parity against captured archive RPC values; no account/storage root verification, untouched-slot coverage or ready checkpoint.");
        }
    }
    files::atomic_json(output, &result, false)?;
    Ok(result)
}
