//! Synthetic qualification data with actual Ethereum trie proofs. These
//! headers/accounts are fabricated test inputs, never claimed to be BSC state.
use crate::{
    ch::ClickHouse,
    cursor, files,
    proof::{self, rlp_list, string},
};
use alloy_primitives::{keccak256, B256, U256};
use alloy_trie::{proof::ProofRetainer, HashBuilder, Nibbles, EMPTY_ROOT_HASH};
use anyhow::{ensure, Result};
use serde_json::{json, Value};
use std::{collections::BTreeMap, fs, path::Path};
#[derive(Clone)]
pub struct Account {
    pub slots: BTreeMap<B256, U256>,
    pub nonce: u64,
    pub balance: U256,
    pub code: Vec<u8>,
    pub exists: bool,
}
impl Default for Account {
    fn default() -> Self {
        Self {
            slots: Default::default(),
            nonce: 1,
            balance: U256::from(25),
            code: vec![0x60, 0, 0x60, 0],
            exists: true,
        }
    }
}
pub fn word(number: u64) -> B256 {
    B256::from(U256::from(number).to_be_bytes::<32>())
}
pub fn storage_root(slots: &BTreeMap<B256, U256>) -> B256 {
    let ordered = slots
        .iter()
        .filter(|(_, v)| !v.is_zero())
        .map(|(slot, value)| (keccak256(slot), *value))
        .collect::<BTreeMap<_, _>>();
    let mut builder = HashBuilder::default();
    for (key, value) in ordered {
        builder.add_leaf(Nibbles::unpack(key), &alloy_rlp::encode(value));
    }
    builder.root()
}
pub fn bundle(
    number: u64,
    accounts: &BTreeMap<String, Account>,
    parent: Option<&str>,
) -> Result<Value> {
    ensure!(!accounts.is_empty(), "synthetic bundle needs accounts");
    let mut metadata = BTreeMap::new();
    let mut leaves = BTreeMap::new();
    let mut targets = Vec::new();
    for (address, data) in accounts {
        let normalized = proof::address(address)?;
        ensure!(
            &normalized == address,
            "synthetic account keys must be canonical"
        );
        let address = normalized;
        let key = keccak256(proof::fixed::<20>(&address)?);
        targets.push(Nibbles::unpack(key));
        ensure!(
            data.exists
                || (data.nonce == 0
                    && data.balance.is_zero()
                    && data.code.is_empty()
                    && data.slots.is_empty()),
            "absent synthetic account has nonempty state"
        );
        let storage = storage_root(&data.slots);
        let code_hash = keccak256(&data.code);
        if data.exists {
            leaves.insert(
                key,
                rlp_list(&[
                    alloy_rlp::encode(data.nonce),
                    alloy_rlp::encode(data.balance),
                    alloy_rlp::encode(storage.as_slice()),
                    alloy_rlp::encode(code_hash.as_slice()),
                ]),
            );
        }
        metadata.insert(address,json!({"nonce":format!("0x{:x}",data.nonce),"balance":format!("{:#x}",data.balance),"storageHash":format!("{storage:#x}"),"codeHash":format!("{code_hash:#x}")}));
    }
    let mut builder = HashBuilder::default().with_proof_retainer(ProofRetainer::from_iter(targets));
    for (key, value) in leaves {
        builder.add_leaf(Nibbles::unpack(key), &value);
    }
    let state_root = builder.root();
    let proofs = builder.take_proof_nodes();
    let parent = parent
        .map(proof::fixed::<32>)
        .transpose()?
        .unwrap_or(word(number.saturating_sub(1)).into());
    let timestamp = 1_700_000_000u64
        .checked_add(number)
        .ok_or_else(|| anyhow::anyhow!("synthetic timestamp overflow"))?;
    let encoded = rlp_list(&[
        alloy_rlp::encode(parent.as_slice()),
        alloy_rlp::encode(keccak256([0xc0]).as_slice()),
        alloy_rlp::encode([0u8; 20].as_slice()),
        alloy_rlp::encode(state_root.as_slice()),
        alloy_rlp::encode(EMPTY_ROOT_HASH.as_slice()),
        alloy_rlp::encode(EMPTY_ROOT_HASH.as_slice()),
        alloy_rlp::encode([0u8; 256].as_slice()),
        alloy_rlp::encode(0u64),
        alloy_rlp::encode(number),
        alloy_rlp::encode(30_000_000u64),
        alloy_rlp::encode(0u64),
        alloy_rlp::encode(timestamp),
        alloy_rlp::encode(&[] as &[u8]),
        alloy_rlp::encode([0u8; 32].as_slice()),
        alloy_rlp::encode([0u8; 8].as_slice()),
    ]);
    let hash = keccak256(&encoded);
    let mut result = json!({"format_version":1,"chain_id":56,"header":{"number":number,"hash":format!("{hash:#x}"),"parent_hash":format!("0x{}",hex::encode(parent)),"state_root":format!("{state_root:#x}"),"timestamp":timestamp},"header_rlp":format!("0x{}",hex::encode(encoded)),"header_trust":"operator-pinned-hash","accounts":{}});
    for (address, data) in accounts {
        let mut fields = metadata.remove(address).unwrap();
        fields["address"] = json!(address);
        let key = keccak256(proof::fixed::<20>(address)?);
        fields["accountProof"] = json!(proofs
            .matching_nodes_sorted(&Nibbles::unpack(key))
            .iter()
            .map(|(_, node)| format!("0x{}", hex::encode(node)))
            .collect::<Vec<_>>());
        result["accounts"][address] =
            json!({"code":format!("0x{}",hex::encode(&data.code)),"proof":fields});
    }
    Ok(result)
}
/// A SQL representation of one annotated native row, for capacity fixtures.
pub fn block(
    bundle: &Value,
    patches: &[(String, B256, U256)],
    with_metadata: bool,
) -> Result<Value> {
    let mut row = bundle["header"].clone();
    let number = &bundle["header"]["number"];
    let accounts = bundle["accounts"]
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("invalid synthetic accounts"))?;
    row["accounts"] = json!(accounts.keys().cloned().collect::<Vec<_>>().join(","));
    row["schema_version"] = json!(1);
    row["producer_version"] = json!(5);
    row["_block_number_"] = number.clone();
    row["_block_timestamp_"] = bundle["header"]["timestamp"].clone();
    row["_version_"] = number.clone();
    row["_deleted_"] = json!(false);
    for (group, fields) in [
        ("storage", vec!["address", "slot", "value", "ordinal"]),
        ("balances", vec!["address", "value", "ordinal"]),
        ("nonces", vec!["address", "value", "ordinal"]),
        ("codes", vec!["address", "hash", "code", "ordinal"]),
        ("lifecycle", vec!["address", "kind", "ordinal"]),
    ] {
        for field in fields {
            row[format!("{group}.{field}")] = json!([]);
        }
    }
    for (i, (address, slot, value)) in patches.iter().enumerate() {
        ensure!(
            accounts.contains_key(address),
            "synthetic patch outside account filter"
        );
        for (field, value) in [
            ("address", json!(address)),
            ("slot", json!(format!("{slot:#x}"))),
            ("value", json!(format!("0x{value:064x}"))),
            ("ordinal", json!(i + 1)),
        ] {
            row[format!("storage.{field}")]
                .as_array_mut()
                .unwrap()
                .push(value);
        }
    }
    if with_metadata {
        for (address, data) in accounts {
            let proof = &data["proof"];
            for (name, value) in [
                ("balances.address", json!(address)),
                (
                    "balances.value",
                    json!(proof::quantity(string(proof, "balance")?, 256)?.to_string()),
                ),
                ("balances.ordinal", json!(10)),
                ("nonces.address", json!(address)),
                (
                    "nonces.value",
                    json!(proof::quantity(string(proof, "nonce")?, 64)?.to::<u64>()),
                ),
                ("nonces.ordinal", json!(10)),
                ("codes.address", json!(address)),
                ("codes.hash", proof["codeHash"].clone()),
                ("codes.code", data["code"].clone()),
                ("codes.ordinal", json!(20)),
            ] {
                row[name].as_array_mut().unwrap().push(value);
            }
        }
    }
    Ok(row)
}
/// Used only for owned synthetic fixture databases after native preparation.
pub fn insert(client: &ClickHouse, directory: &Path, rows: Vec<Value>) -> Result<()> {
    ensure!(!rows.is_empty(), "synthetic insertion needs rows");
    let _lock = files::file_lock(&directory.join("run.lock"), true, false)?;
    let run: Value = serde_json::from_slice(&fs::read(directory.join("run.json"))?)?;
    ensure!(
        run["identity"]["database"] == client.database,
        "synthetic source identity differs"
    );
    let last = rows.last().unwrap().clone();
    client.insert_values("state_blocks", rows.clone())?;
    client.insert_values("_blocks_",rows.into_iter().map(|r|json!({"number":r["number"],"hash":r["hash"].as_str().unwrap().trim_start_matches("0x"),"timestamp":r["timestamp"],"version":r["number"],"deleted":false})))?;
    let n = crate::ch::uint(&last["number"])?;
    let hash = string(&last, "hash")?.trim_start_matches("0x");
    let token = cursor::encode_public(&format!("c2:1:{n}:{hash}:{n}:{hash}"))?;
    files::atomic_write(&directory.join("cursor.txt"), token.as_bytes(), true)?;
    cursor::save_progress(client, &run, directory, &token)?;
    Ok(())
}
