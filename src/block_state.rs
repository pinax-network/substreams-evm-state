//! Native ClickHouse projection. One row is a whole block, not an account patch.
use std::collections::BTreeMap;

use substreams::errors::Error;
use substreams_ethereum::pb::eth::v2 as eth;

use crate::params::Filter;
use crate::pb::evm::state::v1::{
    BalanceValue, BlockState, CodeValue, LifecycleEffect, NonceValue, StorageValue,
};

const EMPTY_CODE_HASH: &str = "0xc5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470";

fn hex(bytes: &[u8]) -> String {
    format!("0x{}", hex::encode(bytes))
}

fn word(bytes: &[u8]) -> Result<String, Error> {
    if bytes.len() > 32 {
        return Err(Error::msg("storage word exceeds 32 bytes"));
    }
    Ok(format!("0x{:0>64}", hex::encode(bytes)))
}

fn require(condition: bool, message: &'static str) -> Result<(), Error> {
    if condition { Ok(()) } else { Err(Error::msg(message)) }
}

pub fn project(params: &str, block: &eth::Block) -> Result<BlockState, Error> {
    let filter = Filter::parse(params).map_err(Error::msg)?;
    require(!filter.is_empty(), "map_block_state requires explicit selected accounts")?;
    require(block.detail_level == eth::block::DetailLevel::DetaillevelExtended as i32,
        "account state requires Extended blocks")?;
    require((3..=5).contains(&block.ver), "unsupported producer version (supported: 3..5)")?;
    let header = block.header.as_ref().ok_or_else(|| Error::msg("missing block header"))?;
    require(block.hash.len() == 32 && header.parent_hash.len() == 32 && header.state_root.len() == 32,
        "missing or invalid block hash, parent hash or state root")?;
    require(header.number == block.number, "header/block number mismatch")?;
    let timestamp = header.timestamp.as_ref().ok_or_else(|| Error::msg("missing block timestamp"))?;
    require(timestamp.seconds >= 0 && (0..1_000_000_000).contains(&timestamp.nanos), "invalid block timestamp")?;
    for tx in &block.transaction_traces {
        require((1..=3).contains(&tx.status), "unknown transaction persistence status")?;
        require(!tx.calls.is_empty(), "Extended transaction missing calls")?;
    }
    let changes = crate::collect(params, block)?;
    let mut storage = BTreeMap::new();
    let mut balances = BTreeMap::new();
    let mut nonces = BTreeMap::new();
    let mut codes = BTreeMap::new();
    let mut lifecycle = BTreeMap::new();

    // collect() sorts each change kind by its block-global execution ordinal.
    // BTreeMap gives deterministic row order and collapses the last value per key.
    for c in changes.storage_changes {
        let address = hex(&c.address);
        let slot = word(&c.key)?;
        storage.insert((address.clone(), slot.clone()), StorageValue {
            address, slot, value: word(&c.new_value)?, ordinal: c.origin.unwrap().ordinal,
        });
    }
    for c in changes.balance_changes {
        let address = hex(&c.address);
        balances.insert(address.clone(), BalanceValue {
            address, value: c.new_value, ordinal: c.origin.unwrap().ordinal,
        });
    }
    for c in changes.nonce_changes {
        let address = hex(&c.address);
        let ordinal = c.origin.unwrap().ordinal;
        if c.new_value < c.old_value {
            lifecycle.insert((address.clone(), ordinal, "nonce_reset"), LifecycleEffect {
                address: address.clone(), kind: "nonce_reset".into(), ordinal,
            });
        }
        nonces.insert(address.clone(), NonceValue { address, value: c.new_value, ordinal });
    }
    for c in changes.code_changes {
        let address = hex(&c.address);
        let ordinal = c.origin.unwrap().ordinal;
        require(c.new_hash.len() == 32 || (c.new_hash.is_empty() && c.new_code.is_empty()),
            "invalid code hash")?;
        if c.new_code.is_empty() {
            lifecycle.insert((address.clone(), ordinal, "code_cleared"), LifecycleEffect {
                address: address.clone(), kind: "code_cleared".into(), ordinal,
            });
        }
        codes.insert(address.clone(), CodeValue {
            address,
            hash: if c.new_hash.is_empty() { EMPTY_CODE_HASH.into() } else { hex(&c.new_hash) },
            code: hex(&c.new_code), ordinal,
        });
    }

    // SELFDESTRUCT's storage semantics depend on the fork and creation context.
    // Preserve the signal for proof reconciliation; never silently infer a wipe.
    let committed_calls = block.system_calls.iter().chain(
        block.transaction_traces.iter()
            .filter(|tx| tx.status() == eth::TransactionTraceStatus::Succeeded)
            .flat_map(|tx| tx.calls.iter())
    );
    for call in committed_calls {
        if call.suicide && !call.state_reverted && filter.matches(&call.address) {
            let address = hex(&call.address);
            lifecycle.insert((address.clone(), call.end_ordinal, "selfdestruct"), LifecycleEffect {
                address, kind: "selfdestruct".into(), ordinal: call.end_ordinal,
            });
        }
    }

    Ok(BlockState {
        number: block.number, hash: hex(&block.hash), parent_hash: hex(&header.parent_hash),
        timestamp: timestamp.seconds as u64, state_root: hex(&header.state_root),
        producer_version: block.ver, accounts: filter.canonical(), schema_version: 1,
        storage: storage.into_values().collect(), balances: balances.into_values().collect(),
        nonces: nonces.into_values().collect(), codes: codes.into_values().collect(),
        lifecycle: lifecycle.into_values().collect(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;

    fn block() -> eth::Block {
        eth::Block {
            ver: 5, hash: vec![2; 32], number: 100,
            header: Some(eth::BlockHeader {
                number: 100, parent_hash: vec![1; 32], state_root: vec![3; 32],
                timestamp: Some(prost_types::Timestamp { seconds: 1000, nanos: 0 }),
                ..Default::default()
            }), ..Default::default()
        }
    }

    fn account() -> String { hex(&[4; 20]) }

    #[test]
    fn rejects_incomplete_source_and_unbounded_filter() {
        assert!(project("", &block()).is_err());
        let mut b = block(); b.detail_level = 2;
        assert!(project(&account(), &b).is_err());
        b = block(); b.header = None;
        assert!(project(&account(), &b).is_err());
        b = block(); b.ver = 6;
        assert!(project(&account(), &b).is_err());
        b = block(); b.header.as_mut().unwrap().number = 99;
        assert!(project(&account(), &b).is_err());
        b = block(); b.transaction_traces.push(eth::TransactionTrace::default());
        assert!(project(&account(), &b).is_err());
    }

    #[test]
    fn empty_blocks_keep_identity_and_normalized_accounts() {
        let a = account();
        let out = project(&format!("{a},{a}"), &block()).unwrap();
        assert_eq!(out.accounts, a);
        assert_eq!(out.number, 100);
        assert_eq!(out.hash, hex(&[2; 32]));
        assert!(out.storage.is_empty() && out.balances.is_empty());
        assert_eq!(BlockState::decode(out.encode_to_vec().as_slice()).unwrap(), out);
    }

    #[test]
    fn collapses_by_ordinal_normalizes_keys_and_preserves_zero() {
        let mut b = block();
        let change = |ordinal, key, value| eth::StorageChange {
            address: vec![4; 20], key, old_value: vec![8], new_value: value, ordinal,
        };
        b.system_calls.push(eth::Call {
            storage_changes: vec![change(9, vec![1], vec![]), change(3, vec![0; 32], vec![2]),
                change(4, vec![0; 31].into_iter().chain([1]).collect(), vec![3])],
            ..Default::default()
        });
        let out = project(&account(), &b).unwrap();
        assert_eq!(out.storage.len(), 2);
        assert_eq!(out.storage[1].ordinal, 9);
        assert_eq!(out.storage[1].value, word(&[]).unwrap());
        assert!(out.nonces.is_empty() && out.codes.is_empty());
        b.system_calls[0].storage_changes[0].key = vec![1; 33];
        assert!(project(&account(), &b).is_err());
    }

    #[test]
    fn surfaces_committed_lifecycle_effects_for_reverification() {
        let mut b = block();
        let mut call = eth::Call {
            address: vec![4; 20], suicide: true, end_ordinal: 5, ..Default::default()
        };
        b.system_calls.push(call.clone());
        assert_eq!(project(&account(), &b).unwrap().lifecycle[0].kind, "selfdestruct");
        call.state_reverted = true; b.system_calls[0] = call;
        assert!(project(&account(), &b).unwrap().lifecycle.is_empty());
    }
}
