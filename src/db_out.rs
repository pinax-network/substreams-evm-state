//! Project `StateChanges` into PostgreSQL `DatabaseChanges`.
//!
//! Tables (see `postgres/schema.*.sql`):
//! * `blocks`   — one row per block, always (continuity marker).
//!
//! Block metadata comes from `StateChanges.block` (filled from the Firehose
//! header), so no `Clock` input is needed.
//! * `accounts` / `storage` / `code` — current state, upserted with the
//!   block-end value (last change by ordinal wins within the block).
//! * `storage_changes` / `balance_changes` / `nonce_changes` /
//!   `code_changes` / `set_code_authorizations` — event log, one row per
//!   persisted change.
//!
//! Every row is an `upsert_row` so re-processing a block is idempotent.

use std::collections::HashMap;

use substreams_database_change::pb::sf::substreams::sink::database::v1::DatabaseChanges;
use substreams_database_change::tables::{Row, Tables};

use crate::pb::evm::state::v1::{Origin, Scope, StateChanges};

fn hex(b: &[u8]) -> String {
    format!("0x{}", hex::encode(b))
}

/// Postgres BYTEA literal (hex input format).
fn bytea(b: &[u8]) -> String {
    format!("\\x{}", hex::encode(b))
}

/// 32-byte, left-padded hex word (storage keys/values may arrive trimmed).
fn word(b: &[u8]) -> String {
    let mut s = String::with_capacity(66);
    s.push_str("0x");
    for _ in b.len()..32 {
        s.push_str("00");
    }
    s.push_str(&hex::encode(b));
    s
}

fn scope_name(o: Option<&Origin>) -> &'static str {
    match o.map(|o| Scope::try_from(o.scope).unwrap_or(Scope::Unspecified)) {
        Some(Scope::Tx) => "tx",
        Some(Scope::TxFailedPersistent) => "tx_failed_persistent",
        Some(Scope::Tx7702) => "tx_7702",
        Some(Scope::SystemCall) => "system_call",
        Some(Scope::Block) => "block",
        _ => "unspecified",
    }
}

fn set_origin(row: &mut Row, o: Option<&Origin>) {
    let d = Origin::default();
    let o = o.unwrap_or(&d);
    row.set("scope", scope_name(Some(o)))
        .set("tx_hash", if o.tx_hash.is_empty() { String::new() } else { hex(&o.tx_hash) })
        .set("tx_index", o.tx_index)
        .set("tx_status", o.tx_status)
        .set("call_index", o.call_index);
}

fn ord(o: Option<&Origin>) -> u64 {
    o.map(|o| o.ordinal).unwrap_or(0)
}

pub fn project(ch: &StateChanges) -> DatabaseChanges {
    let mut tables = Tables::new();
    let info = ch.block.clone().unwrap_or_default();
    let block_num = info.number;
    let block_hash = hex(&info.hash);
    let timestamp = info.timestamp.to_string();

    // -- blocks: every block, including ones with no matching changes --------
    tables
        .upsert_row("blocks", [("block_num", block_num.to_string())])
        .set("block_hash", &block_hash)
        .set("parent_hash", hex(&info.parent_hash))
        .set("timestamp", &timestamp)
        .set("state_root", hex(&info.state_root))
        .set("coinbase", hex(&info.coinbase))
        .set("transaction_count", info.transaction_count)
        .set("storage_changes", ch.storage_changes.len() as u64)
        .set("balance_changes", ch.balance_changes.len() as u64)
        .set("nonce_changes", ch.nonce_changes.len() as u64)
        .set("code_changes", ch.code_changes.len() as u64);

    // -- event log ------------------------------------------------------------
    for c in &ch.storage_changes {
        let row = tables.upsert_row("storage_changes", [("block_num", block_num.to_string()), ("ordinal", ord(c.origin.as_ref()).to_string())]);
        row.set("address", hex(&c.address))
            .set("slot", word(&c.key))
            .set("old_value", word(&c.old_value))
            .set("new_value", word(&c.new_value))
            .set("block_hash", &block_hash)
            .set("timestamp", &timestamp);
        set_origin(row, c.origin.as_ref());
    }
    for c in &ch.balance_changes {
        let row = tables.upsert_row("balance_changes", [("block_num", block_num.to_string()), ("ordinal", ord(c.origin.as_ref()).to_string())]);
        row.set("address", hex(&c.address))
            .set("old_value", &c.old_value)
            .set("new_value", &c.new_value)
            .set("reason", c.reason)
            .set("block_hash", &block_hash)
            .set("timestamp", &timestamp);
        set_origin(row, c.origin.as_ref());
    }
    for c in &ch.nonce_changes {
        let row = tables.upsert_row("nonce_changes", [("block_num", block_num.to_string()), ("ordinal", ord(c.origin.as_ref()).to_string())]);
        row.set("address", hex(&c.address))
            .set("old_value", c.old_value)
            .set("new_value", c.new_value)
            .set("block_hash", &block_hash)
            .set("timestamp", &timestamp);
        set_origin(row, c.origin.as_ref());
    }
    for c in &ch.code_changes {
        let row = tables.upsert_row("code_changes", [("block_num", block_num.to_string()), ("ordinal", ord(c.origin.as_ref()).to_string())]);
        row.set("address", hex(&c.address))
            .set("old_hash", hex(&c.old_hash))
            .set("new_hash", hex(&c.new_hash))
            .set("block_hash", &block_hash)
            .set("timestamp", &timestamp);
        set_origin(row, c.origin.as_ref());
    }
    for a in &ch.set_code_authorizations {
        tables
            .upsert_row("set_code_authorizations", [("block_num", block_num.to_string()), ("tx_hash", hex(&a.tx_hash)), ("auth_index", a.index.to_string())])
            .set("block_hash", &block_hash)
            .set("timestamp", &timestamp)
            .set("tx_index", a.tx_index)
            .set("tx_status", a.tx_status)
            .set("authority", hex(&a.authority))
            .set("delegate", hex(&a.delegate))
            .set("nonce", a.nonce)
            .set("discarded", a.discarded);
    }

    // -- current state: last change by ordinal wins ---------------------------
    // storage (address, slot)
    let mut last_storage: HashMap<(&[u8], &[u8]), (u64, &[u8])> = HashMap::new();
    for c in &ch.storage_changes {
        let o = ord(c.origin.as_ref());
        let e = last_storage.entry((&c.address, &c.key)).or_insert((o, &c.new_value));
        if o >= e.0 {
            *e = (o, &c.new_value);
        }
    }
    for ((address, slot), (o, value)) in last_storage {
        tables
            .upsert_row("storage", [("address", hex(address)), ("slot", word(slot))])
            .set("value", word(value))
            .set("block_num", block_num)
            .set("ordinal", o);
    }

    // accounts: balance / nonce / code_hash independently, each last-by-ordinal
    #[derive(Default)]
    struct Acc<'a> {
        balance: Option<(u64, &'a str)>,
        nonce: Option<(u64, u64)>,
        code: Option<(u64, &'a [u8])>,
    }
    let mut accounts: HashMap<&[u8], Acc> = HashMap::new();
    for c in &ch.balance_changes {
        let o = ord(c.origin.as_ref());
        let a = accounts.entry(&c.address).or_default();
        if a.balance.map(|(po, _)| o >= po).unwrap_or(true) {
            a.balance = Some((o, &c.new_value));
        }
    }
    for c in &ch.nonce_changes {
        let o = ord(c.origin.as_ref());
        let a = accounts.entry(&c.address).or_default();
        if a.nonce.map(|(po, _)| o >= po).unwrap_or(true) {
            a.nonce = Some((o, c.new_value));
        }
    }
    for c in &ch.code_changes {
        let o = ord(c.origin.as_ref());
        let a = accounts.entry(&c.address).or_default();
        if a.code.map(|(po, _)| o >= po).unwrap_or(true) {
            a.code = Some((o, &c.new_hash));
        }
        // bytecode, deduplicated by hash
        if !c.new_code.is_empty() {
            tables
                .upsert_row("code", [("code_hash", hex(&c.new_hash))])
                .set("code", bytea(&c.new_code))
                .set("size", c.new_code.len() as u64)
                .set_if_null("first_block_num", block_num);
        }
    }
    for (address, a) in accounts {
        let row = tables.upsert_row("accounts", [("address", hex(address))]);
        row.set("block_num", block_num);
        if let Some((_, balance)) = a.balance {
            row.set("balance", balance).set("balance_block_num", block_num);
        }
        if let Some((_, nonce)) = a.nonce {
            row.set("nonce", nonce).set("nonce_block_num", block_num);
        }
        if let Some((_, code_hash)) = a.code {
            row.set("code_hash", hex(code_hash)).set("code_block_num", block_num);
        }
    }

    substreams::log::info!("block {} rows {}", block_num, tables.all_row_count());
    tables.to_database_changes()
}
