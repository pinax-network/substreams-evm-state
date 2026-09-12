//! BSC account deletion at transaction end (EIP-6780), including delegated code.
use std::collections::{BTreeMap, HashSet};

use substreams::errors::Error;
use substreams_ethereum::pb::eth::v2::{Block, Call, CallType, TransactionTraceStatus};

// BSC mainnet params/config.go: Cancun and Haber activate together.
// Source revision and tracing evidence: docs/LIFECYCLE.md.
pub const BSC_CANCUN_TIME: u64 = 1_718_863_500;

pub fn execution_address<'a>(call: &'a Call, calls: &'a [Call]) -> Result<&'a [u8], Error> {
    let mut current = call;
    let mut visited = HashSet::new();
    while matches!(current.call_type(), CallType::Delegate | CallType::Callcode) {
        if !visited.insert(current.index) {
            return Err(Error::msg("cyclic delegated call ancestry"));
        }
        current = calls.iter().find(|parent| parent.index == current.parent_index && parent.index != current.index)
            .ok_or_else(|| Error::msg("delegated SELFDESTRUCT missing execution context"))?;
    }
    Ok(&current.address)
}

pub fn deletions(block: &Block, timestamp: u64) -> Result<BTreeMap<Vec<u8>, u64>, Error> {
    let mut result = BTreeMap::new();
    for tx in &block.transaction_traces {
        if tx.status() != TransactionTraceStatus::Succeeded { continue; }
        let created: HashSet<&[u8]> = tx.calls.iter()
            .filter(|c| !c.state_reverted && c.call_type() == CallType::Create)
            .map(|c| c.address.as_slice()).collect();
        for call in tx.calls.iter().filter(|c| c.suicide && !c.state_reverted) {
            let account = execution_address(call, &tx.calls)?;
            if timestamp < BSC_CANCUN_TIME || created.contains(account) {
                if tx.end_ordinal == 0 || tx.end_ordinal <= call.end_ordinal {
                    return Err(Error::msg("account deletion has no reliable transaction-end ordinal"));
                }
                // SELFDESTRUCT schedules deletion at transaction end. Storage is
                // still visible to subsequent calls in the same transaction.
                result.entry(account.to_vec()).and_modify(|end: &mut u64| *end = (*end).max(tx.end_ordinal))
                    .or_insert(tx.end_ordinal);
            }
        }
    }
    // BSC's qualified system calls do not create/destroy accounts. Do not turn
    // an unknown system execution context into an inferred account deletion.
    if block.system_calls.iter().any(|call| call.suicide && !call.state_reverted) {
        return Err(Error::msg("SELFDESTRUCT in system execution requires producer qualification"));
    }
    Ok(result)
}
