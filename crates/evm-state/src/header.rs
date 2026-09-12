//! Verify the encoded BSC header separately from the caller's consensus trust.
use crate::proof::{fixed, quantity, rlp_list, string, unhex};
use alloy_primitives::{keccak256, B256, U256};
use anyhow::{ensure, Context, Result};
use serde_json::Value;

// RPC name, fixed byte length, integer bit width. Optional fork fields preserve
// the upstream positions, including empty encodings for omitted intermediate fields.
pub const FIELDS: &[(&str, Option<usize>, Option<usize>)] = &[
    ("parentHash", Some(32), None),
    ("sha3Uncles", Some(32), None),
    ("miner", Some(20), None),
    ("stateRoot", Some(32), None),
    ("transactionsRoot", Some(32), None),
    ("receiptsRoot", Some(32), None),
    ("logsBloom", Some(256), None),
    ("difficulty", None, Some(256)),
    ("number", None, Some(64)),
    ("gasLimit", None, Some(64)),
    ("gasUsed", None, Some(64)),
    ("timestamp", None, Some(64)),
    ("extraData", None, None),
    ("mixHash", Some(32), None),
    ("nonce", Some(8), None),
    ("baseFeePerGas", None, Some(256)),
    ("withdrawalsRoot", Some(32), None),
    ("blobGasUsed", None, Some(64)),
    ("excessBlobGas", None, Some(64)),
    ("parentBeaconBlockRoot", Some(32), None),
    ("requestsHash", Some(32), None),
    ("balHash", Some(32), None),
    ("slotNumber", None, Some(64)),
];

pub fn encode_rpc_header(header: &Value) -> Result<String> {
    let last = FIELDS
        .iter()
        .enumerate()
        .filter(|(i, (name, _, _))| *i < 15 || !header[*name].is_null())
        .map(|(i, _)| i)
        .max()
        .unwrap();
    let mut fields = Vec::new();
    for (i, (name, length, bits)) in FIELDS[..=last].iter().enumerate() {
        if header[*name].is_null() {
            ensure!(i >= 15, "RPC header missing {name}");
            fields.push(alloy_rlp::encode(&[] as &[u8]));
        } else if let Some(bits) = bits {
            fields.push(alloy_rlp::encode(quantity(string(header, name)?, *bits)?));
        } else {
            let bytes = unhex(string(header, name)?)?;
            ensure!(
                length.is_none_or(|n| bytes.len() == n),
                "invalid header field length for {name}"
            );
            fields.push(alloy_rlp::encode(bytes.as_slice()));
        }
    }
    let encoded = rlp_list(&fields);
    ensure!(
        keccak256(&encoded).as_slice() == fixed::<32>(string(header, "hash")?)?,
        "encoded RPC header does not match its block hash"
    );
    Ok(format!("0x{}", hex::encode(encoded)))
}

pub fn verify_header(
    encoded_hex: &str,
    summary: &Value,
    expected_hash: Option<&str>,
) -> Result<B256> {
    let encoded = unhex(encoded_hex)?;
    ensure!(encoded.len() <= 104_448, "oversized block header");
    let mut input = encoded.as_slice();
    let mut payload =
        alloy_rlp::Header::decode_bytes(&mut input, true).context("invalid RLP block header")?;
    ensure!(input.is_empty(), "trailing bytes after RLP header");
    let mut fields = Vec::new();
    while !payload.is_empty() {
        fields.push(
            alloy_rlp::Header::decode_bytes(&mut payload, false)
                .context("header field must be bytes")?,
        );
        ensure!(
            fields.len() <= FIELDS.len(),
            "unsupported header field count"
        );
    }
    ensure!(fields.len() >= 15, "unsupported header field count");
    for (i, (value, (_, length, bits))) in fields.iter().zip(FIELDS).enumerate() {
        if let Some(bits) = bits {
            ensure!(
                !value.starts_with(&[0]) && value.len() <= bits / 8,
                "invalid header integer"
            );
        }
        if let Some(length) = length {
            ensure!(
                value.len() == *length || (i >= 15 && value.is_empty()),
                "invalid header field length"
            );
        }
    }
    let digest = keccak256(&encoded);
    ensure!(
        digest.as_slice() == fixed::<32>(string(summary, "hash")?)?,
        "encoded header differs from checkpoint block hash"
    );
    if let Some(expected) = expected_hash {
        ensure!(
            digest.as_slice() == fixed::<32>(expected)?,
            "encoded header differs from expected hash"
        );
    }
    ensure!(
        fields[0] == fixed::<32>(string(summary, "parent_hash")?)?
            && fields[3] == fixed::<32>(string(summary, "state_root")?)?
            && U256::from_be_slice(fields[8])
                == U256::from(
                    summary["number"]
                        .as_u64()
                        .context("invalid header number")?
                )
            && U256::from_be_slice(fields[11])
                == U256::from(
                    summary["timestamp"]
                        .as_u64()
                        .context("invalid header timestamp")?
                ),
        "checkpoint metadata differs from encoded header"
    );
    Ok(digest)
}
