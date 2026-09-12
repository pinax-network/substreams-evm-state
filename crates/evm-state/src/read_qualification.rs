//! Measure the actual returned pages, proving their complete storage and metadata.
use crate::{
    capacity,
    ch::{uint, ClickHouse},
    checkpoint,
    control::Control,
    files::{atomic_json, canonical_json, resolve, spaced_json},
    header::verify_header,
    proof::{address, fixed, string, verify_account, verify_metadata, StorageSort},
    reader,
    trie_qualification::file_hash,
};
use anyhow::{ensure, Context, Result};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    fs,
    path::{Path, PathBuf},
    time::Instant,
};

/// The same checks run against real SQL pages and injected response defects.
pub trait ReadApi {
    fn database(&self) -> &str;
    fn control_path(&self) -> Result<PathBuf>;
    fn pin(&mut self, snapshot: &str) -> Result<Value>;
    fn manifest(&mut self, snapshot: &str) -> Result<Value>;
    fn account(&mut self, snapshot: &str, account: &str) -> Result<Value>;
    fn page(
        &mut self,
        pin: &str,
        account: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<Value>;
    fn unpin(&mut self, pin: &str) -> Result<()>;
    fn guard(&mut self, paths: &[PathBuf], phase: &str) -> Result<()>;
}
impl ReadApi for ClickHouse {
    fn database(&self) -> &str {
        &self.database
    }
    fn control_path(&self) -> Result<PathBuf> {
        Ok(Control::open(self)?.path)
    }
    fn pin(&mut self, s: &str) -> Result<Value> {
        reader::pin(self, s, "qualification-read-latency")
    }
    fn manifest(&mut self, s: &str) -> Result<Value> {
        checkpoint::manifest(self, s)
    }
    fn account(&mut self, s: &str, a: &str) -> Result<Value> {
        checkpoint::read_account(self, s, a)
    }
    fn page(&mut self, p: &str, a: &str, c: Option<&str>, n: usize) -> Result<Value> {
        reader::page(self, p, a, c, n)
    }
    fn unpin(&mut self, p: &str) -> Result<()> {
        reader::unpin(self, p)?;
        Ok(())
    }
    fn guard(&mut self, paths: &[PathBuf], phase: &str) -> Result<()> {
        capacity::check(self, paths, phase)?;
        Ok(())
    }
}

pub fn distribution(values: &[f64]) -> Result<Value> {
    ensure!(
        !values.is_empty() && values.iter().all(|v| v.is_finite() && *v >= 0.),
        "invalid latency samples"
    );
    let mut ordered = values.to_vec();
    ordered.sort_by(f64::total_cmp);
    let percentile =
        |p: f64| ordered[((ordered.len() as f64 * p).ceil() as usize).saturating_sub(1)];
    Ok(
        json!({"samples":ordered.len(),"min":ordered[0],"p50":percentile(0.5),"p95":percentile(0.95),"max":ordered[ordered.len()-1]}),
    )
}

pub fn measure(
    api: &mut impl ReadApi,
    snapshot: &str,
    selected: &str,
    output: &Path,
    work: &Path,
    passes_count: usize,
    page_size: usize,
) -> Result<Value> {
    let output = resolve(output)?;
    let work = resolve(work)?;
    let selected = address(selected)?;
    ensure!(
        !output.try_exists()?
            && (1..=10).contains(&passes_count)
            && (1..=10000).contains(&page_size),
        "use a new output file, 1..10 passes and page size 1..10000"
    );
    let paths = [
        output.parent().context("output has no parent")?.into(),
        work.clone(),
        api.control_path()?,
    ];
    api.guard(&paths, "read-measurement-start")?;
    fs::create_dir_all(&work)?;
    let binary_sha256 = file_hash(&std::env::current_exe()?)?;
    let started = reader::now_ns()?;
    let pinned = api.pin(snapshot)?;
    let pin = string(&pinned, "pin_id")?.to_owned();
    let measured = (|| -> Result<Value> {
        ensure!(
            pinned["snapshot_id"] == snapshot,
            "pin changed checkpoint identity"
        );
        let published = api.manifest(snapshot)?;
        let bundle = &published["proof_bundle"];
        ensure!(
            published["header"] == pinned["header"] && bundle["header"] == pinned["header"],
            "proof bundle changed checkpoint identity"
        );
        verify_header(string(bundle, "header_rlp")?, &pinned["header"], None)?;
        let evidence = &bundle["accounts"][&selected];
        let proven = verify_account(
            string(&pinned["header"], "state_root")?,
            &selected,
            &evidence["proof"],
        )?;
        let account = api.account(snapshot, &selected)?;
        ensure!(
            account["address"] == selected
                && account["snapshot_id"] == snapshot
                && account["header"] == pinned["header"]
                && account["exists"] == proven.exists
                && fixed::<32>(string(&account, "storage_root")?)?.as_slice()
                    == proven.storage_root.as_slice()
                && account["code"] == evidence["code"],
            "reader metadata differs from the captured account proof"
        );
        let mut metadata = account.clone();
        metadata["nonce"] = json!(uint(&account["nonce"])?);
        verify_metadata(&proven, string(&account, "code")?, &metadata)?;
        let declared = uint(&account["nonzero_slots"])?;
        let mut expected = account.clone();
        expected["nonce"] = json!(uint(&account["nonce"])?.to_string());
        let mut calls = Vec::new();
        let mut passes = Vec::new();
        let mut identity = None;
        for iteration in 0..passes_count {
            let directory = tempfile::Builder::new()
                .prefix("read-root-")
                .tempdir_in(&work)?;
            let mut sorter = StorageSort::new(&directory.path().join("storage.sqlite"))?;
            let mut digest = Sha256::new();
            let mut count = 0_u64;
            let mut cursor = None::<String>;
            let mut previous = None::<[u8; 32]>;
            let mut page_number = 0;
            let mut page_seconds = 0.;
            let began = Instant::now();
            loop {
                let before = Instant::now();
                let response = api.page(&pin, &selected, cursor.as_deref(), page_size)?;
                let latency = before.elapsed().as_secs_f64();
                page_seconds += latency;
                ensure!(
                    response["pin_id"] == pin
                        && response["snapshot_id"] == snapshot
                        && response["header"] == pinned["header"]
                        && response["account"] == expected,
                    "reader changed checkpoint identity or account metadata"
                );
                let rows = response["storage"]
                    .as_array()
                    .context("invalid storage page")?;
                ensure!(
                    rows.len() <= page_size,
                    "reader exceeded requested page size"
                );
                calls.push(json!({"pass":iteration,"page":page_number,"rows":rows.len(),"seconds":latency,"json_bytes":spaced_json(&response)?.len()}));
                for row in rows {
                    let key = fixed::<32>(string(row, "slot")?)?;
                    let value = fixed::<32>(string(row, "value")?)?;
                    ensure!(
                        previous.is_none_or(|p| key > p),
                        "reader returned duplicate or unordered slots"
                    );
                    count = count.checked_add(1).context("reader count overflow")?;
                    ensure!(
                        count <= declared,
                        "reader returned more slots than the account declares"
                    );
                    sorter.insert(string(row, "slot")?, string(row, "value")?)?;
                    digest.update(key);
                    digest.update(value);
                    previous = Some(key);
                }
                match response
                    .get("next_cursor")
                    .context("reader omitted continuation cursor")?
                {
                    Value::Null => break,
                    Value::String(next) => {
                        ensure!(
                            !rows.is_empty() && !next.is_empty(),
                            "reader returned an empty nonterminal page"
                        );
                        cursor = Some(next.clone());
                    }
                    _ => anyhow::bail!("invalid continuation cursor"),
                }
                page_number += 1;
            }
            let scan_seconds = began.elapsed().as_secs_f64();
            let (root, verified_count) = sorter.finish()?;
            let total_seconds = began.elapsed().as_secs_f64();
            ensure!(
                root == proven.storage_root,
                "complete storage root mismatch in returned pages"
            );
            ensure!(
                count == verified_count && count == declared,
                "reader returned an incomplete account"
            );
            let digest = hex::encode(digest.finalize());
            ensure!(
                identity.as_ref().is_none_or(|prior| prior == &digest),
                "pinned account content changed between passes"
            );
            identity = Some(digest);
            api.guard(&paths, "read-measurement-root")?;
            passes.push(json!({"pass":iteration,"scan_and_verification_seconds":total_seconds,"scan_and_staging_seconds":scan_seconds,"root_hash_seconds":total_seconds-scan_seconds,"page_call_seconds":page_seconds,"nonzero_slots":count}));
        }
        Ok(
            json!({"format_version":2,"database":api.database(),"snapshot_id":snapshot,"runtime":"Rust","binary_sha256":binary_sha256,"proof_bundle_sha256":hex::encode(Sha256::digest(canonical_json(bundle)?)),"account":selected,"header":pinned["header"],"started_at_unix_ns":started,"finished_at_unix_ns":reader::now_ns()?,"page_size":page_size,"passes":passes,"verified_storage_root":format!("0x{}",hex::encode(proven.storage_root)),"root_matches_captured_account_proof":true,"page_latency_seconds":distribution(&calls.iter().map(|c|c["seconds"].as_f64().unwrap()).collect::<Vec<_>>())?,"ordered_storage_sha256":identity.unwrap(),"calls":calls,
            "limitations":["local Rust reader, controller locks and ClickHouse SQL; no remote API network","sequential repeated scans, not concurrent reader load or a cold database cache","scan_and_staging_seconds includes slot validation and SQLite staging between page calls","root_hash_seconds includes the SQLite commit and ordered root assembly; capacity guards excluded from pass timings","immutable published generation; publication latency measured separately"]}),
        )
    })();
    // Release pins on every validation, transport and capacity failure. If release
    // itself fails, preserve the pin for an operator and never publish evidence.
    let released = api.unpin(&pin);
    let result = measured?;
    released?;
    api.guard(&paths, "read-measurement-finished")?;
    atomic_json(&output, &result, false)?;
    Ok(result)
}
