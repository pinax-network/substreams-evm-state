//! Decode the pinned bstream/opaque format. Public obfuscation, not authentication.
use crate::files::canonical_json;
use anyhow::{ensure, Context, Result};
use base64::{engine::general_purpose::URL_SAFE, Engine};
use crypto_secretbox::{
    aead::{Aead, KeyInit},
    XSalsa20Poly1305,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

// PUBLIC upstream constants from opaque 0c01d37ea308; these are not secrets.
const PUBLIC_KEY: &str = "7bfcacee257409099edd2bb6a442638b559a80bfbfc0b9acdea0d8344b10eb00";
const PUBLIC_NONCE: &str = "261554c45ab9b752abad4f19c242605702d55a0d91616a1b";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockRef {
    pub number: u64,
    pub hash: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Position {
    pub block: BlockRef,
    pub head: BlockRef,
    pub lib: BlockRef,
    pub step: u8,
}

fn cipher() -> XSalsa20Poly1305 {
    XSalsa20Poly1305::new_from_slice(&hex::decode(PUBLIC_KEY).unwrap()).unwrap()
}

pub fn decode(token: &str) -> Result<Position> {
    ensure!(
        !token.is_empty() && token.len() <= 2048,
        "invalid cursor length"
    );
    let bare = token.trim_end_matches('=');
    ensure!(
        token.len() - bare.len() <= 2
            && !bare.is_empty()
            && bare
                .bytes()
                .all(|v| v.is_ascii_alphanumeric() || v == b'_' || v == b'-'),
        "invalid cursor alphabet"
    );
    let encoded = URL_SAFE.decode(token).context("invalid cursor base64")?;
    let nonce = hex::decode(PUBLIC_NONCE).unwrap();
    let payload = cipher()
        .decrypt(nonce.as_slice().into(), encoded.as_slice())
        .map_err(|_| anyhow::anyhow!("invalid cursor ciphertext"))?;
    let payload = std::str::from_utf8(&payload).context("invalid cursor text")?;
    let parts: Vec<_> = payload.split(':').collect();
    ensure!(
        matches!(parts.first(), Some(&"c1" | &"c2")) && parts.len() == 6
            || parts.first() == Some(&"c3") && parts.len() == 8,
        "invalid cursor segments"
    );
    ensure!(
        matches!(parts[1], "1" | "16" | "17"),
        "unsupported cursor step"
    );
    let reference = |offset: usize| -> Result<BlockRef> {
        let number = parts[offset];
        let hash = parts[offset + 1];
        ensure!(
            number == "0"
                || (!number.is_empty()
                    && !number.starts_with('0')
                    && number.bytes().all(|v| v.is_ascii_digit())),
            "invalid BSC block number"
        );
        ensure!(
            hash.len() == 64
                && hash
                    .bytes()
                    .all(|v| v.is_ascii_digit() || (b'a'..=b'f').contains(&v)),
            "invalid BSC block hash"
        );
        Ok(BlockRef {
            number: number.parse().context("block number exceeds uint64")?,
            hash: format!("0x{hash}"),
        })
    };
    let block = reference(2)?;
    let head = if parts[0] == "c1" {
        block.clone()
    } else {
        reference(4)?
    };
    let lib = if parts[0] == "c2" {
        block.clone()
    } else {
        reference(if parts[0] == "c1" { 4 } else { 6 })?
    };
    ensure!(
        lib == block
            && head.number >= block.number
            && (head.number != block.number || head == block),
        "cursor is not on a finalized block"
    );
    Ok(Position {
        block,
        head,
        lib,
        step: parts[1].parse()?,
    })
}

/// Compatibility fixture generation; the public cipher authenticates no user.
pub fn encode_public(payload: &str) -> Result<String> {
    let nonce = hex::decode(PUBLIC_NONCE).unwrap();
    let value = cipher()
        .encrypt(nonce.as_slice().into(), payload.as_bytes())
        .map_err(|_| anyhow::anyhow!("cursor encoding failed"))?;
    Ok(URL_SAFE.encode(value))
}

pub fn binding(run: &Value) -> Result<Value> {
    let identity = run.get("identity").context("missing native run identity")?;
    Ok(
        json!({"format_version":1,"run_id":crate::proof::string(run,"run_id")?,
        "database_uuid":crate::proof::string(run,"database_uuid")?,
        "identity_sha256":hex::encode(Sha256::digest(canonical_json(identity)?.as_bytes()))}),
    )
}

pub fn validate(client: &crate::ch::ClickHouse, run: &Value, token: &str) -> Result<Position> {
    use crate::ch::{params, uint};
    let position = decode(token)?;
    let identity = &run["identity"];
    ensure!(
        position.block.number >= uint(&identity["start_block"])?,
        "native cursor precedes its run's start"
    );
    let query = params(json!({"number":position.block.number}))?;
    let rows = client.rows("SELECT hash,accounts,schema_version,producer_version FROM state_blocks FINAL WHERE number={number:UInt64}", &query)?.collect::<Result<Vec<_>>>()?;
    let markers = client
        .rows(
            "SELECT hash FROM _blocks_ FINAL WHERE number={number:UInt64}",
            &query,
        )?
        .collect::<Result<Vec<_>>>()?;
    let accounts = identity["accounts"]
        .as_array()
        .context("missing native account filter")?
        .iter()
        .map(|v| v.as_str().context("invalid native account filter"))
        .collect::<Result<Vec<_>>>()?
        .join(",");
    ensure!(
        rows.len() == 1
            && rows[0]["hash"] == position.block.hash
            && rows[0]["accounts"] == accounts
            && uint(&rows[0]["schema_version"])? == 1
            && matches!(uint(&rows[0]["producer_version"])?, 3..=5)
            && markers.len() == 1
            && format!(
                "0x{}",
                crate::proof::string(&markers[0], "hash")?.trim_start_matches("0x")
            ) == position.block.hash,
        "native cursor does not match its complete block data and marker"
    );
    Ok(position)
}

pub fn load_progress(
    client: &crate::ch::ClickHouse,
    run: &Value,
    directory: &std::path::Path,
) -> Result<Value> {
    let record: Value = serde_json::from_slice(
        &std::fs::read(directory.join("durable_progress.json"))
            .context("durable progress is missing; restore matching run metadata")?,
    )?;
    let expected = binding(run)?;
    ensure!(
        expected
            .as_object()
            .unwrap()
            .iter()
            .all(|(key, value)| record.get(key) == Some(value)),
        "durable progress belongs to another native run"
    );
    let position = validate(client, run, crate::proof::string(&record, "cursor")?)?;
    ensure!(
        record["position"] == serde_json::to_value(position)?,
        "durable progress position is corrupt"
    );
    Ok(record)
}

pub fn save_progress(
    client: &crate::ch::ClickHouse,
    run: &Value,
    directory: &std::path::Path,
    token: &str,
) -> Result<Value> {
    let position = validate(client, run, token)?;
    let path = directory.join("durable_progress.json");
    if path.try_exists()? {
        let previous = load_progress(client, run, directory)?;
        ensure!(
            crate::ch::uint(&previous["position"]["block"]["number"])? <= position.block.number,
            "native cursor regressed behind durable progress"
        );
        if previous["cursor"] == token {
            return Ok(previous);
        }
    }
    let mut record = binding(run)?;
    record["cursor"] = json!(token);
    record["position"] = serde_json::to_value(position)?;
    crate::files::atomic_json(&path, &record, true)?;
    Ok(record)
}

pub fn observe(
    client: &crate::ch::ClickHouse,
    run: &Value,
    directory: &std::path::Path,
) -> Result<Option<Value>> {
    use std::io::Read;
    let file = match std::fs::File::open(directory.join("cursor.txt")) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let mut bytes = Vec::new();
    file.take(2049).read_to_end(&mut bytes)?;
    let Ok(token) = std::str::from_utf8(&bytes) else {
        return Ok(None);
    };
    let token = token.trim();
    if decode(token).is_err() {
        return Ok(None);
    }
    // After decoding, mismatched rows or a failed durable write are hard errors.
    save_progress(client, run, directory, token).map(Some)
}
