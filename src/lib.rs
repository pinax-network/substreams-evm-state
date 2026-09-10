//! EVM account-state projection Substreams.
//!
//! * `db_out(params, Block)` — the sink module. Applies the persistence
//!   rules (`persist.rs`), filters on the changed account (`params.rs`) and
//!   emits PostgreSQL `DatabaseChanges` (`db_out.rs`). Reads the Firehose
//!   block directly so nothing intermediate is cached on the server.
//! * `map_state_changes(params, Block)` — optional sibling for gRPC
//!   consumers; same rules, emits `evm.state.v1.StateChanges`. Not in
//!   `db_out`'s dependency chain, so it is not executed by the sink.

mod db_out;
mod params;
pub mod pb;
mod persist;

use substreams::errors::Error;
use substreams::scalar::BigInt as SBigInt;
use substreams_database_change::pb::sf::substreams::sink::database::v1::DatabaseChanges;
use substreams_ethereum::pb::eth::v2 as eth;

use crate::params::Filter;
use crate::pb::evm::state::v1::{
    BalanceChange, BlockInfo, CodeChange, NonceChange, Origin, SetCodeAuthorization, StateChanges, StorageChange,
};
use crate::persist::{Ctx, Sink};

struct Collector<'f> {
    filter: &'f Filter,
    out: StateChanges,
}

fn origin(ord: u64, ctx: Ctx) -> Option<Origin> {
    Some(Origin {
        ordinal: ord,
        scope: ctx.scope as i32,
        tx_hash: ctx.tx_hash.to_vec(),
        tx_index: ctx.tx_index,
        tx_status: ctx.tx_status,
        call_index: ctx.call_index,
    })
}

fn bigint_dec(v: Option<&eth::BigInt>) -> String {
    match v {
        Some(b) if !b.bytes.is_empty() => SBigInt::from_unsigned_bytes_be(&b.bytes).to_string(),
        _ => "0".to_string(),
    }
}

impl Sink for Collector<'_> {
    fn storage(&mut self, c: &eth::StorageChange, ctx: Ctx) {
        if !self.filter.matches(&c.address) {
            return;
        }
        self.out.storage_changes.push(StorageChange {
            address: c.address.clone(),
            key: c.key.clone(),
            old_value: c.old_value.clone(),
            new_value: c.new_value.clone(),
            origin: origin(c.ordinal, ctx),
        });
    }

    fn balance(&mut self, c: &eth::BalanceChange, ctx: Ctx) {
        if !self.filter.matches(&c.address) {
            return;
        }
        self.out.balance_changes.push(BalanceChange {
            address: c.address.clone(),
            old_value: bigint_dec(c.old_value.as_ref()),
            new_value: bigint_dec(c.new_value.as_ref()),
            reason: c.reason,
            origin: origin(c.ordinal, ctx),
        });
    }

    fn nonce(&mut self, c: &eth::NonceChange, ctx: Ctx) {
        if !self.filter.matches(&c.address) {
            return;
        }
        self.out.nonce_changes.push(NonceChange {
            address: c.address.clone(),
            old_value: c.old_value,
            new_value: c.new_value,
            origin: origin(c.ordinal, ctx),
        });
    }

    fn code(&mut self, c: &eth::CodeChange, ctx: Ctx) {
        if !self.filter.matches(&c.address) {
            return;
        }
        self.out.code_changes.push(CodeChange {
            address: c.address.clone(),
            old_hash: c.old_hash.clone(),
            new_hash: c.new_hash.clone(),
            new_code: c.new_code.clone(),
            origin: origin(c.ordinal, ctx),
        });
    }
}

/// Shared core: persisted, filtered, ordinal-sorted state changes of a block.
pub fn collect(params: &str, block: &eth::Block) -> Result<StateChanges, Error> {
    let filter = Filter::parse(params).map_err(|e| Error::msg(format!("params: {e}")))?;

    let header = block.header.as_ref();
    let mut col = Collector {
        filter: &filter,
        out: StateChanges {
            block: Some(BlockInfo {
                number: block.number,
                hash: block.hash.clone(),
                parent_hash: header.map(|h| h.parent_hash.clone()).unwrap_or_default(),
                timestamp: header.and_then(|h| h.timestamp.as_ref()).map(|t| t.seconds as u64).unwrap_or_default(),
                state_root: header.map(|h| h.state_root.clone()).unwrap_or_default(),
                coinbase: header.map(|h| h.coinbase.clone()).unwrap_or_default(),
                transaction_count: block.transaction_traces.len() as u32,
            }),
            ..Default::default()
        },
    };

    persist::collect_block(block, &mut col);

    // EIP-7702 authorization list (informational; the persisted effects are
    // already in nonce_changes / code_changes). Kept when authority OR
    // delegate matches the filter.
    for trx in &block.transaction_traces {
        if trx.r#type() != eth::transaction_trace::Type::TrxTypeSetCode {
            continue;
        }
        for (i, a) in trx.set_code_authorizations.iter().enumerate() {
            let authority = a.authority.clone().unwrap_or_default();
            if !(filter.matches(&authority) || filter.matches(&a.address)) {
                continue;
            }
            col.out.set_code_authorizations.push(SetCodeAuthorization {
                tx_hash: trx.hash.clone(),
                tx_index: trx.index,
                index: i as u32,
                authority,
                delegate: a.address.clone(),
                nonce: a.nonce,
                discarded: a.discarded,
                tx_status: trx.status,
            });
        }
    }

    let mut out = col.out;
    let ord = |o: &Option<Origin>| o.as_ref().map(|o| o.ordinal).unwrap_or(0);
    out.storage_changes.sort_by_key(|c| ord(&c.origin));
    out.balance_changes.sort_by_key(|c| ord(&c.origin));
    out.nonce_changes.sort_by_key(|c| ord(&c.origin));
    out.code_changes.sort_by_key(|c| ord(&c.origin));

    substreams::log::info!(
        "block {} filter={} storage={} balance={} nonce={} code={} auths={}",
        block.number,
        if filter.is_empty() { "all".to_string() } else { filter.len().to_string() },
        out.storage_changes.len(),
        out.balance_changes.len(),
        out.nonce_changes.len(),
        out.code_changes.len(),
        out.set_code_authorizations.len(),
    );
    Ok(out)
}

#[substreams::handlers::map]
pub fn map_state_changes(params: String, block: eth::Block) -> Result<StateChanges, Error> {
    collect(&params, &block)
}

#[substreams::handlers::map]
pub fn db_out(params: String, block: eth::Block) -> Result<DatabaseChanges, Error> {
    let changes = collect(&params, &block)?;
    Ok(db_out::project(&changes))
}
