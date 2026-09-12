//! Protocol fixtures use the captured independent account/storage proof bytes.
use anyhow::Result;
use evm_state::{
    files::{atomic_json, canonical_json},
    proof,
    trie_qualification::file_hash,
};
use flate2::{Compression, GzBuilder};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File},
    io::Write,
    path::Path,
};

pub const A: &str = "0x1111111111111111111111111111111111111111";
pub const ID: &str = "11111111111111111111111111111111";

pub fn fixture() -> Result<Value> {
    Ok(serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../tests/fixtures/proof-parity.json"
    )))?)
}
pub fn header() -> Result<(Value, String)> {
    let fixture = fixture()?;
    let encoded = proof::unhex(fixture["headers"][0]["encoded"].as_str().unwrap())?;
    let mut input = encoded.as_slice();
    let mut payload = alloy_rlp::Header::decode_bytes(&mut input, true)?;
    let mut fields = Vec::new();
    while !payload.is_empty() {
        fields.push(alloy_rlp::Header::decode_bytes(&mut payload, false)?.to_vec());
    }
    fields[3] = proof::unhex(fixture["account"]["state_root"].as_str().unwrap())?;
    let encoded = proof::rlp_list(
        &fields
            .iter()
            .map(|v| alloy_rlp::encode(v.as_slice()))
            .collect::<Vec<_>>(),
    );
    let mut summary = fixture["headers"][0]["summary"].clone();
    summary["state_root"] = fixture["account"]["state_root"].clone();
    summary["hash"] = json!(format!(
        "0x{}",
        hex::encode(alloy_primitives::keccak256(&encoded))
    ));
    Ok((summary, format!("0x{}", hex::encode(encoded))))
}

pub fn rows() -> Result<Vec<Value>> {
    Ok(fixture()?["account"]["slots"]
        .as_array()
        .unwrap()
        .iter()
        .map(|pair| json!({"address":A,"slot":pair[0],"value":pair[1]}))
        .collect())
}
pub fn write_page(path: &Path, rows: &[Value]) -> Result<()> {
    let mut gzip = GzBuilder::new()
        .mtime(0)
        .write(File::create(path)?, Compression::best());
    for row in rows {
        writeln!(gzip, "{}", canonical_json(row)?)?;
    }
    gzip.finish()?.sync_all()?;
    Ok(())
}
pub fn rehash(directory: &Path, item: &mut Value) -> Result<()> {
    let path = directory.join(item["file"].as_str().unwrap());
    item["bytes"] = json!(fs::metadata(&path)?.len());
    item["sha256"] = json!(file_hash(&path)?);
    Ok(())
}
pub fn save(directory: &Path, layout: &Value) -> Result<()> {
    atomic_json(&directory.join("manifest.json"), layout, true)
}

pub fn create(directory: &Path) -> Result<Value> {
    fs::create_dir_all(directory)?;
    let fixture = fixture()?;
    let f = &fixture["account"];
    let proven = proof::verify_account(f["state_root"].as_str().unwrap(), A, &f["proof"])?;
    let mut metadata = proven.json();
    metadata["code"] = f["code"].clone();
    metadata["nonzero_slots"] = json!(2);
    let rows = rows()?;
    let mut digest = Sha256::new();
    for row in &rows {
        digest.update(format!(
            "{}{}{}",
            A,
            row["slot"].as_str().unwrap(),
            row["value"].as_str().unwrap()
        ));
    }
    digest.update(canonical_json(&metadata)?);
    metadata["nonce"] = json!(metadata["nonce"].as_u64().unwrap().to_string());
    atomic_json(&directory.join("accounts.json"), &json!([metadata]), false)?;
    let (header, rlp) = header()?;
    let snapshot = json!({"format_version":1,"status":"ready","snapshot_id":ID,"chain_id":56,"header":header,"header_trust":"operator-pinned-hash", "accounts":[A],"sources":[],"account_count":1,"nonzero_slots":2,"state_sha256":hex::encode(digest.finalize()),
        "proof_bundle":{"format_version":1,"chain_id":56,"header":header,"header_rlp":rlp,"header_trust":"operator-pinned-hash","accounts":{A:{"proof":f["proof"],"code":f["code"]}}}});
    let mut layout = json!({"format":"evm-state-checkpoint-v1","status":"ready","checkpoint":snapshot,"account_file":{"file":"accounts.json"},"storage_pages":[]});
    rehash(directory, &mut layout["account_file"])?;
    for (i, row) in rows.into_iter().enumerate() {
        let name = format!("storage-{i:06}.jsonl.gz");
        write_page(&directory.join(&name), &[row.clone()])?;
        let mut item = json!({"file":name,"address":A,"rows":1,"first_slot":row["slot"],"last_slot":row["slot"]});
        rehash(directory, &mut item)?;
        layout["storage_pages"].as_array_mut().unwrap().push(item);
    }
    save(directory, &layout)?;
    Ok(layout)
}
