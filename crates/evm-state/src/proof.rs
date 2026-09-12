//! Proofs bind complete account state to a chosen root, not to BSC consensus.
use alloy_primitives::{keccak256, Bytes, B256, U256};
use alloy_trie::{proof::verify_proof, HashBuilder, Nibbles, EMPTY_ROOT_HASH};
use anyhow::{bail, ensure, Context, Result};
use rusqlite::{params, Connection};
use serde_json::{json, Value};
use std::path::Path;

pub fn unhex(value: &str) -> Result<Vec<u8>> {
    let raw = value
        .strip_prefix("0x")
        .context("expected 0x-prefixed hex")?;
    ensure!(raw.len() % 2 == 0, "expected even-length hex");
    Ok(hex::decode(raw).context("invalid hex")?)
}

pub fn fixed<const N: usize>(value: &str) -> Result<[u8; N]> {
    unhex(value)?
        .try_into()
        .map_err(|_| anyhow::anyhow!("expected {N} bytes"))
}

pub fn string<'a>(value: &'a Value, field: &str) -> Result<&'a str> {
    value
        .get(field)
        .and_then(Value::as_str)
        .with_context(|| format!("missing string field {field}"))
}

pub fn address(value: &str) -> Result<String> {
    Ok(format!("0x{}", hex::encode(fixed::<20>(value)?)))
}

pub fn quantity(value: &str, bits: usize) -> Result<U256> {
    let raw = value
        .strip_prefix("0x")
        .context("expected JSON-RPC hex quantity")?;
    ensure!(
        !raw.is_empty() && raw.bytes().all(|v| v.is_ascii_hexdigit()),
        "invalid JSON-RPC quantity"
    );
    let number = U256::from_str_radix(raw, 16).context("quantity exceeds uint256")?;
    ensure!(number.bit_len() <= bits, "quantity exceeds uint{bits}");
    Ok(number)
}

pub fn rlp_list(encoded_items: &[Vec<u8>]) -> Vec<u8> {
    let mut out = Vec::new();
    alloy_rlp::Header {
        list: true,
        payload_length: encoded_items.iter().map(Vec::len).sum(),
    }
    .encode(&mut out);
    for item in encoded_items {
        out.extend_from_slice(item);
    }
    out
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Account {
    pub address: String,
    pub exists: bool,
    pub nonce: u64,
    pub balance: U256,
    pub storage_root: B256,
    pub code_hash: B256,
}

impl Account {
    pub fn json(&self) -> Value {
        json!({"address": self.address, "exists": self.exists, "nonce": self.nonce,
            "balance": self.balance.to_string(), "storage_root": format!("{:#x}", self.storage_root),
            "code_hash": format!("{:#x}", self.code_hash)})
    }
}

pub fn verify_account(state_root: &str, selected: &str, proof: &Value) -> Result<Account> {
    let root = B256::from(fixed::<32>(state_root)?);
    let selected = address(selected)?;
    if let Some(actual) = proof.get("address") {
        ensure!(
            address(actual.as_str().context("invalid proof address")?)? == selected,
            "proof is for a different account"
        );
    }
    let nonce: u64 = quantity(string(proof, "nonce")?, 64)?
        .try_into()
        .context("nonce exceeds uint64")?;
    let balance = quantity(string(proof, "balance")?, 256)?;
    let storage_root = B256::from(fixed::<32>(string(proof, "storageHash")?)?);
    let code_hash = B256::from(fixed::<32>(string(proof, "codeHash")?)?);
    let nodes = proof
        .get("accountProof")
        .and_then(Value::as_array)
        .context("missing account proof")?
        .iter()
        .map(|node| {
            Ok(Bytes::from(unhex(
                node.as_str().context("invalid account proof node")?,
            )?))
        })
        .collect::<Result<Vec<_>>>()?;
    let key = keccak256(fixed::<20>(&selected)?);
    // Verifying the canonical encoding of all reported fields against the root
    // gives them no authority of their own. A different field cannot pass.
    let encoded = rlp_list(&[
        alloy_rlp::encode(nonce),
        alloy_rlp::encode(balance),
        alloy_rlp::encode(storage_root.as_slice()),
        alloy_rlp::encode(code_hash.as_slice()),
    ]);
    let exists = if verify_proof(root, Nibbles::unpack(key), Some(encoded), &nodes).is_ok() {
        true
    } else {
        verify_proof(root, Nibbles::unpack(key), None, &nodes)
            .context("invalid or incomplete account proof")?;
        ensure!(
            nonce == 0
                && balance.is_zero()
                && storage_root == EMPTY_ROOT_HASH
                && code_hash == keccak256([]),
            "RPC account metadata does not match proven absence"
        );
        false
    };
    Ok(Account {
        address: selected,
        exists,
        nonce,
        balance,
        storage_root,
        code_hash,
    })
}

pub fn verify_metadata(account: &Account, code: &str, metadata: &Value) -> Result<()> {
    let nonce = metadata
        .get("nonce")
        .and_then(Value::as_u64)
        .context("nonce must be an exact uint64")?;
    let balance = string(metadata, "balance")?;
    ensure!(
        balance == "0"
            || (!balance.starts_with('0')
                && !balance.is_empty()
                && balance.bytes().all(|v| v.is_ascii_digit())),
        "balance must be an exact decimal string"
    );
    ensure!(
        nonce == account.nonce && U256::from_str_radix(balance, 10)? == account.balance,
        "account nonce/balance mismatch"
    );
    ensure!(
        B256::from(fixed::<32>(string(metadata, "code_hash")?)?) == account.code_hash,
        "account code hash mismatch"
    );
    ensure!(
        keccak256(unhex(code)?) == account.code_hash,
        "bytecode does not match proven code hash"
    );
    Ok(())
}

/// A single-use disk sort. No full account map or full trie is kept in memory.
pub struct StorageSort {
    connection: Connection,
    count: u64,
    failed: bool,
}

impl StorageSort {
    pub fn new(path: &Path) -> Result<Self> {
        let connection = Connection::open(path)?;
        connection.execute_batch(
            "PRAGMA journal_mode=OFF; PRAGMA synchronous=OFF; PRAGMA cache_size=-16384;
            CREATE TABLE storage (key BLOB PRIMARY KEY, value BLOB NOT NULL) WITHOUT ROWID; BEGIN",
        )?;
        Ok(Self {
            connection,
            count: 0,
            failed: false,
        })
    }

    pub fn insert(&mut self, slot: &str, value: &str) -> Result<()> {
        ensure!(!self.failed, "storage sorting workspace has failed");
        self.failed = true;
        let key = keccak256(fixed::<32>(slot)?);
        let value = U256::from_be_bytes(fixed::<32>(value)?);
        ensure!(
            !value.is_zero(),
            "complete-storage input must contain nonzero slots only"
        );
        let encoded = alloy_rlp::encode(value);
        let inserted = self
            .connection
            .prepare_cached("INSERT INTO storage VALUES (?1,?2)")?
            .execute(params![key.as_slice(), encoded]);
        if let Err(error) = inserted {
            if matches!(&error, rusqlite::Error::SqliteFailure(v, _) if v.code == rusqlite::ErrorCode::ConstraintViolation)
            {
                bail!("duplicate nonzero storage slot");
            }
            return Err(error.into());
        }
        self.count += 1;
        self.failed = false;
        Ok(())
    }

    pub fn finish(self) -> Result<(B256, u64)> {
        ensure!(!self.failed, "storage sorting workspace has failed");
        self.connection.execute_batch("COMMIT")?;
        let mut statement = self
            .connection
            .prepare("SELECT key,value FROM storage ORDER BY key")?;
        let mut rows = statement.query([])?;
        let mut builder = HashBuilder::default();
        let mut previous: Option<[u8; 32]> = None;
        let mut observed = 0_u64;
        while let Some(row) = rows.next()? {
            let key: Vec<u8> = row.get(0)?;
            let key: [u8; 32] = key
                .try_into()
                .map_err(|_| anyhow::anyhow!("invalid staged storage key"))?;
            ensure!(
                previous.is_none_or(|last| key > last),
                "unordered or duplicate staged storage key"
            );
            previous = Some(key);
            let value: Vec<u8> = row.get(1)?;
            builder.add_leaf(Nibbles::unpack(key), &value);
            observed += 1;
        }
        ensure!(observed == self.count, "staged storage count changed");
        Ok((builder.root(), observed))
    }
}

pub fn storage_root<I>(slots: I, path: &Path) -> Result<(B256, u64)>
where
    I: IntoIterator<Item = Result<(String, String)>>,
{
    let mut database = StorageSort::new(path)?;
    for pair in slots {
        let (slot, value) = pair?;
        database.insert(&slot, &value)?;
    }
    database.finish()
}

pub fn verify_complete<I>(
    account: &Account,
    slots: I,
    code: &str,
    metadata: &Value,
    path: &Path,
) -> Result<u64>
where
    I: IntoIterator<Item = Result<(String, String)>>,
{
    verify_metadata(account, code, metadata)?;
    let (root, count) = storage_root(slots, path)?;
    ensure!(
        root == account.storage_root,
        "storage root mismatch: incomplete, stale or incorrect account storage"
    );
    Ok(count)
}
