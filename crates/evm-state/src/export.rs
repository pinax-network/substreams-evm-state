//! Portable checkpoint files. The manifest is written only after offline verification.
use crate::{
    capacity,
    ch::{params, uint, ClickHouse},
    checkpoint::{canonical_accounts, manifest_unlocked},
    control::{object_id, Control},
    files::{atomic_json, canonical_json, resolve},
    header::verify_header,
    proof::{address, fixed, string, verify_account, verify_complete},
    trie_qualification::file_hash,
};
use anyhow::{ensure, Context, Result};
use flate2::{read::MultiGzDecoder, Compression, GzBuilder};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, Read, Write},
    os::unix::fs::OpenOptionsExt,
    path::{Path, PathBuf},
};

pub const FORMAT: &str = "evm-state-checkpoint-v1";
pub const ACCOUNT_FIELDS: [&str; 8] = [
    "address",
    "exists",
    "nonce",
    "balance",
    "code_hash",
    "code",
    "storage_root",
    "nonzero_slots",
];
const MAX_JSON_BYTES: u64 = 64 * 1024 * 1024;

fn open_regular(path: &Path) -> Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    ensure!(file.metadata()?.is_file(), "export file is not regular");
    Ok(file)
}

pub(crate) fn read_json(path: &Path) -> Result<Value> {
    let file = open_regular(path)?;
    ensure!(
        file.metadata()?.len() <= MAX_JSON_BYTES,
        "export metadata exceeds its size limit"
    );
    let mut bytes = Vec::new();
    file.take(MAX_JSON_BYTES + 1).read_to_end(&mut bytes)?;
    ensure!(
        bytes.len() as u64 <= MAX_JSON_BYTES,
        "export metadata exceeds its size limit"
    );
    serde_json::from_slice(&bytes).context("invalid export metadata")
}

pub(crate) fn check_file(directory: &Path, item: &Value) -> Result<PathBuf> {
    let name = string(item, "file")?;
    let numbered = name
        .strip_prefix("storage-")
        .and_then(|v| v.strip_suffix(".jsonl.gz"));
    ensure!(
        name == "accounts.json"
            || numbered.is_some_and(|n| n.len() == 6 && n.bytes().all(|c| c.is_ascii_digit())),
        "invalid checkpoint export filename"
    );
    let path = directory.join(name);
    let mut file = open_regular(&path)?;
    ensure!(
        Some(file.metadata()?.len()) == item["bytes"].as_u64(),
        "export file size or checksum mismatch"
    );
    let mut digest = Sha256::new();
    let mut buffer = [0; 1048576];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        digest.update(&buffer[..count]);
    }
    ensure!(
        hex::encode(digest.finalize()) == string(item, "sha256")?,
        "export file size or checksum mismatch"
    );
    Ok(path)
}

pub fn validate_account_fields(metadata: &Value) -> Result<()> {
    let fields = metadata
        .as_object()
        .context("invalid exported account metadata")?;
    ensure!(
        fields.len() == ACCOUNT_FIELDS.len()
            && ACCOUNT_FIELDS
                .iter()
                .all(|field| fields.contains_key(*field))
            && metadata["exists"].is_boolean(),
        "invalid exported account metadata"
    );
    Ok(())
}

pub fn exact_nonce(value: &Value) -> Result<u64> {
    let value = value
        .as_str()
        .context("export nonce must be exact decimal text")?;
    ensure!(
        value == "0"
            || !value.is_empty()
                && !value.starts_with('0')
                && value.bytes().all(|c| c.is_ascii_digit()),
        "export nonce must be exact decimal text"
    );
    value.parse().context("export nonce exceeds uint64")
}

pub(crate) fn page_rows(directory: &Path, item: &Value) -> Result<PageRows> {
    let path = check_file(directory, item)?;
    let expected = item["rows"]
        .as_u64()
        .context("invalid export page row count")?;
    ensure!(
        (1..=10000).contains(&expected),
        "invalid export page row count"
    );
    Ok(PageRows {
        reader: BufReader::new(MultiGzDecoder::new(open_regular(&path)?)),
        item: item.clone(),
        count: 0,
        first: None,
        last: None,
        finished: false,
    })
}

pub(crate) struct PageRows {
    reader: BufReader<MultiGzDecoder<File>>,
    item: Value,
    count: u64,
    first: Option<String>,
    last: Option<String>,
    finished: bool,
}
impl PageRows {
    fn read_next(&mut self) -> Result<Option<Value>> {
        let mut bytes = Vec::new();
        (&mut self.reader)
            .take(1025)
            .read_until(b'\n', &mut bytes)?;
        if bytes.is_empty() {
            self.finished = true;
            ensure!(
                Some(self.count) == self.item["rows"].as_u64()
                    && self.first.as_deref() == self.item["first_slot"].as_str()
                    && self.last.as_deref() == self.item["last_slot"].as_str(),
                "export page count or boundary mismatch"
            );
            return Ok(None);
        }
        ensure!(
            bytes.len() <= 1024 && bytes.last() == Some(&b'\n'),
            "oversized or truncated export storage row"
        );
        let row: Value = serde_json::from_slice(&bytes)?;
        ensure!(
            row.as_object().is_some_and(|fields| fields.len() == 3
                && ["address", "slot", "value"]
                    .iter()
                    .all(|field| fields.contains_key(*field)))
                && row["address"] == self.item["address"],
            "export row has unexpected fields or account"
        );
        let slot = string(&row, "slot")?;
        fixed::<32>(slot)?;
        fixed::<32>(string(&row, "value")?)?;
        ensure!(
            self.last.as_ref().is_none_or(|last| slot > last.as_str()),
            "export page contains duplicate or unordered slots"
        );
        if self.first.is_none() {
            self.first = Some(slot.into());
        }
        self.last = Some(slot.into());
        self.count += 1;
        ensure!(
            self.count <= self.item["rows"].as_u64().unwrap(),
            "export page exceeds its declared row count"
        );
        Ok(Some(row))
    }
}
impl Iterator for PageRows {
    type Item = Result<Value>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        match self.read_next() {
            Ok(Some(row)) => Some(Ok(row)),
            Ok(None) => None,
            Err(error) => {
                self.finished = true;
                Some(Err(error))
            }
        }
    }
}

fn write_page(directory: &Path, index: usize, account: &str, rows: &[Value]) -> Result<Value> {
    ensure!(
        index < 1_000_000 && !rows.is_empty(),
        "invalid export page index or empty page"
    );
    let name = format!("storage-{index:06}.jsonl.gz");
    let path = directory.join(&name);
    let mut writer = GzBuilder::new()
        .mtime(0)
        .write(File::create_new(&path)?, Compression::best());
    for row in rows {
        writeln!(writer, "{}", canonical_json(row)?)?;
    }
    let file = writer.finish()?;
    file.sync_all()?;
    Ok(
        json!({"file":name,"address":account,"rows":rows.len(),"bytes":file.metadata()?.len(),"sha256":file_hash(&path)?,
        "first_slot":rows[0]["slot"],"last_slot":rows.last().unwrap()["slot"]}),
    )
}

pub(crate) fn verify_layout(
    client: &ClickHouse,
    directory: &Path,
    layout: &Value,
    expected_hash: Option<&str>,
    work_dir: &Path,
) -> Result<Value> {
    let paths = vec![directory.to_path_buf(), work_dir.to_path_buf()];
    capacity::check(client, &paths, "export-verify-start")?;
    ensure!(
        layout["format"] == FORMAT && layout["status"] == "ready",
        "unsupported or incomplete checkpoint export"
    );
    let snapshot = &layout["checkpoint"];
    ensure!(
        snapshot["status"] == "ready"
            && snapshot["format_version"] == 1
            && snapshot["chain_id"] == 56,
        "invalid BSC checkpoint manifest"
    );
    object_id(string(snapshot, "snapshot_id")?)?;
    let bundle = &snapshot["proof_bundle"];
    ensure!(
        bundle["chain_id"] == 56
            && bundle["format_version"] == 1
            && bundle["header"] == snapshot["header"],
        "checkpoint and proof bundle identities differ"
    );
    ensure!(
        matches!(
            bundle["header_trust"].as_str(),
            Some("operator-pinned-hash" | "provider-finalized-header")
        ) && bundle["header_trust"] == snapshot["header_trust"],
        "checkpoint header trust record is inconsistent"
    );
    verify_header(
        string(bundle, "header_rlp")
            .context("portable exports require the encoded block header")?,
        &snapshot["header"],
        expected_hash,
    )?;
    let selected = canonical_accounts(&snapshot["accounts"])?;
    ensure!(
        json!(selected) == snapshot["accounts"]
            && selected
                == bundle["accounts"]
                    .as_object()
                    .context("invalid account proofs")?
                    .keys()
                    .cloned()
                    .collect::<Vec<_>>(),
        "checkpoint account coverage differs from its proofs"
    );
    let accounts = read_json(&check_file(directory, &layout["account_file"])?)?;
    let accounts = accounts.as_array().context("invalid exported accounts")?;
    ensure!(
        accounts
            .iter()
            .map(|row| string(row, "address").map(str::to_owned))
            .collect::<Result<Vec<_>>>()?
            == selected,
        "export account coverage is missing, duplicated or unordered"
    );
    ensure!(
        Some(accounts.len() as u64) == snapshot["account_count"].as_u64(),
        "export account count mismatch"
    );
    let mut pages: BTreeMap<String, Vec<&Value>> =
        selected.iter().map(|a| (a.clone(), Vec::new())).collect();
    let mut last_account = "";
    for (i, item) in layout["storage_pages"]
        .as_array()
        .context("invalid storage pages")?
        .iter()
        .enumerate()
    {
        let account = string(item, "address")?;
        ensure!(
            item["file"] == format!("storage-{i:06}.jsonl.gz")
                && pages.contains_key(account)
                && account >= last_account,
            "export page sequence or account is invalid"
        );
        pages.get_mut(account).unwrap().push(item);
        last_account = account;
    }
    let mut digest = Sha256::new();
    let mut total = 0_u64;
    fs::create_dir_all(work_dir)?;
    for row in accounts {
        validate_account_fields(row)?;
        let mut metadata = row.clone();
        metadata["nonce"] = json!(exact_nonce(&metadata["nonce"])?);
        let account = address(string(&metadata, "address")?)?;
        let proof = &bundle["accounts"][&account];
        let proven = verify_account(
            string(&snapshot["header"], "state_root")?,
            &account,
            &proof["proof"],
        )?;
        ensure!(
            metadata["exists"] == proven.exists
                && fixed::<32>(string(&metadata, "storage_root")?)?.as_slice()
                    == proven.storage_root.as_slice()
                && metadata["code"] == proof["code"],
            "exported metadata differs from its proven account"
        );
        let mut previous = None::<String>;
        let slots = pages[&account]
            .iter()
            .flat_map(|item| match page_rows(directory, item) {
                Ok(rows) => Box::new(rows) as Box<dyn Iterator<Item = Result<Value>>>,
                Err(error) => Box::new(std::iter::once(Err(error))),
            })
            .map(|row| {
                let row = row?;
                let slot = string(&row, "slot")?.to_owned();
                let value = string(&row, "value")?.to_owned();
                ensure!(
                    previous.as_ref().is_none_or(|previous| slot > *previous),
                    "duplicate or unordered slots across export pages"
                );
                previous = Some(slot.clone());
                digest.update(format!("{account}{slot}{value}"));
                Ok((slot, value))
            });
        let workspace = tempfile::Builder::new()
            .prefix("evm-export-verify-")
            .tempdir_in(work_dir)?;
        let count = verify_complete(
            &proven,
            slots,
            string(&metadata, "code")?,
            &metadata,
            &workspace.path().join("storage.sqlite"),
        )?;
        capacity::check(client, &paths, "export-verify-trie")?;
        ensure!(
            Some(count) == metadata["nonzero_slots"].as_u64(),
            "exported account storage count mismatch"
        );
        total = total
            .checked_add(count)
            .context("export slot count overflow")?;
        digest.update(canonical_json(&metadata)?);
    }
    let digest = hex::encode(digest.finalize());
    ensure!(
        Some(total) == snapshot["nonzero_slots"].as_u64() && snapshot["state_sha256"] == digest,
        "exported checkpoint count or state checksum mismatch"
    );
    Ok(
        json!({"snapshot_id":snapshot["snapshot_id"],"header":snapshot["header"],"header_trust":snapshot["header_trust"],"accounts":selected,
        "nonzero_slots":total,"state_sha256":digest,"verification":"account proofs, complete storage, code and header hash verified"}),
    )
}

pub fn verify_export(
    directory: &Path,
    expected_hash: Option<&str>,
    work_dir: &Path,
) -> Result<Value> {
    let directory = resolve(directory)?;
    let client = ClickHouse::new("default")?;
    verify_layout(
        &client,
        &directory,
        &read_json(&directory.join("manifest.json"))?,
        expected_hash,
        work_dir,
    )
}

pub fn export_checkpoint(
    client: &ClickHouse,
    snapshot_id: &str,
    directory: &Path,
    page_size: usize,
    work_dir: &Path,
) -> Result<Value> {
    ensure!(
        (1..=10000).contains(&page_size),
        "export page_size must be between 1 and 10000"
    );
    let directory = resolve(directory)?;
    let owner = Control::open(client)?;
    let _reader = owner.reader()?;
    let paths = vec![
        directory.clone(),
        work_dir.to_path_buf(),
        owner.path.clone(),
    ];
    capacity::check(client, &paths, "export-start")?;
    let snapshot = manifest_unlocked(client, snapshot_id)?;
    string(&snapshot["proof_bundle"], "header_rlp")
        .context("portable exports require the encoded block header")?;
    fs::create_dir_all(directory.parent().context("export has no parent")?)?;
    fs::create_dir(&directory)?;
    let mut accounts=client.rows("SELECT address,exists,nonce,balance,code_hash,code,storage_root,nonzero_slots FROM checkpoint_accounts FINAL WHERE snapshot_id={id:String} ORDER BY address",&params(json!({"id":snapshot_id}))?)?.collect::<Result<Vec<_>>>()?;
    for account in &mut accounts {
        account["nonce"] = json!(uint(&account["nonce"])?.to_string());
        account["nonzero_slots"] = json!(uint(&account["nonzero_slots"])?);
    }
    atomic_json(&directory.join("accounts.json"), &json!(accounts), false)?;
    let metadata = directory.join("accounts.json");
    let mut layout = json!({"format":FORMAT,"status":"ready","checkpoint":snapshot,"account_file":{"file":"accounts.json","bytes":fs::metadata(&metadata)?.len(),"sha256":file_hash(&metadata)?},"storage_pages":[]});
    let mut pages = Vec::new();
    for account in snapshot["accounts"]
        .as_array()
        .context("invalid checkpoint accounts")?
    {
        let account = account.as_str().context("invalid checkpoint account")?;
        let mut pending = Vec::new();
        for row in client.rows("SELECT address,slot,value FROM checkpoint_storage FINAL WHERE snapshot_id={id:String} AND address={address:String} ORDER BY slot",&params(json!({"id":snapshot_id,"address":account}))?)? {
            pending.push(row?);if pending.len()==page_size {pages.push(write_page(&directory,pages.len(),account,&pending)?);pending.clear();}
        }
        if !pending.is_empty() {
            pages.push(write_page(&directory, pages.len(), account, &pending)?);
        }
    }
    let page_count = pages.len();
    layout["storage_pages"] = json!(pages);
    let mut result = verify_layout(client, &directory, &layout, None, work_dir)?;
    capacity::check(client, &paths, "export-publish")?;
    atomic_json(&directory.join("manifest.json"), &layout, false)?;
    let mut bytes = 0_u64;
    for entry in fs::read_dir(&directory)? {
        let entry = entry?;
        if entry.file_type()?.is_file() {
            bytes = bytes
                .checked_add(entry.metadata()?.len())
                .context("export byte count overflow")?;
        }
    }
    result["directory"] = json!(directory);
    result["pages"] = json!(page_count);
    result["bytes"] = json!(bytes);
    Ok(result)
}
