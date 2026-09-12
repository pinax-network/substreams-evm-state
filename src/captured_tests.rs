//! Unmodified producer messages, with original header context and literal RPC
//! expectations. The composed test blocks contain only the selected transaction.
//! Full-block replay/proof evidence is recorded separately in docs/LIFECYCLE.md.
use prost::Message;
use substreams_ethereum::pb::eth::v2 as eth;

use crate::block_state::project;
use crate::pb::evm::state::v1::Scope;

const EMPTY_CODE_HASH: &str = "0xc5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470";

macro_rules! sample {
    ($name:literal, $version:literal, $number:literal) => {{
        let header = eth::BlockHeader::decode(
            &include_bytes!(concat!("../tests/fixtures/lifecycle/", $name, "-header.pb"))[..],
        ).unwrap();
        let tx = eth::TransactionTrace::decode(
            &include_bytes!(concat!("../tests/fixtures/lifecycle/", $name, ".pb"))[..],
        ).unwrap();
        assert_eq!(header.number, $number);
        eth::Block { ver: $version, number: header.number, hash: header.hash.clone(),
            header: Some(header), transaction_traces: vec![tx], ..Default::default() }
    }};
}

#[test]
fn captured_v5_self_authorization_keeps_both_increments() {
    let block = sample!("v5-self-delegation", 5, 121464944);
    let address = "0x9999b0cdd35d7f3b281ba02efc0d228486940515";
    let changes = crate::collect(address, &block).unwrap();
    assert_eq!(changes.nonce_changes.iter().map(|c| c.new_value).collect::<Vec<_>>(),
               vec![71284328, 71284329]);
    let out = project(address, &block).unwrap();
    assert_eq!(out.nonces[0].value, 71284329);
    assert_eq!(out.nonces[0].ordinal, 71);
    assert_eq!(out.codes[0].code, "0xef0100789df93945b2f0fd90dc3aa01beccd560c95f037");
    assert_eq!(out.codes[0].hash, "0xd3c2af5290fc5c773a5bf70aa3d7ff209fba5a1541c32f93e962b152a377f637");
    assert_eq!(out.codes[0].ordinal, 72);
    assert!(out.lifecycle.is_empty());
}

#[test]
fn captured_v5_three_authorities_keep_separate_metadata() {
    let block = sample!("v5-multiple-authorities", 5, 121464945);
    let sender = "0x41c690e6093302a84630320ea17dd213b1a9187d";
    let authorities = ["0xdc379f81dc18c9a8ad82e937aecb0b79d143793a",
        "0x4e8a0f94b775f1ff377c26d34618100ac32b8354", "0xb2b1d7fc79ef3f3aa6c0c0156e64eec50b37c706"];
    let out = project(&format!("{sender},{}", authorities.join(",")), &block).unwrap();
    assert_eq!(out.nonces.len(), 4);
    assert_eq!(out.codes.len(), 3);
    assert_eq!(out.nonces.iter().find(|v| v.address == sender).unwrap().value, 202050);
    assert_eq!(out.balances.iter().find(|v| v.address == sender).unwrap().value, "179476641310317172");
    for (i, address) in authorities.iter().enumerate() {
        let nonce = out.nonces.iter().find(|v| &v.address == address).unwrap();
        assert_eq!(nonce.value, 1);
        assert_eq!(nonce.ordinal, 6491 + i as u64 * 2);
        let code = out.codes.iter().find(|v| &v.address == address).unwrap();
        assert_eq!(code.code, "0xef01008ecc1f892bb8dfc53e46ef57ca9795ccf8308c25");
        assert_eq!(code.hash, "0xc071e566ca0763cfb4ee7d6056f248e7023706b108ef49543f5581d444c6c09c");
    }
    assert!(out.storage.is_empty() && out.lifecycle.is_empty());
}

#[test]
fn captured_v5_discarded_authorization_and_reverted_slot_stay_unchanged() {
    let block = sample!("v5-discarded-reverted", 5, 121464971);
    let tx = &block.transaction_traces[0];
    assert_eq!(tx.status(), eth::TransactionTraceStatus::Reverted);
    assert!(tx.set_code_authorizations[0].discarded);
    // The trace contains an attempted 1 -> 2 slot write. RPC before/after both
    // report 1; the output must omit this reverted write, not publish its value.
    assert_eq!(tx.calls[0].storage_changes[0].new_value.last(), Some(&2));
    let sender = "0xdeca137a9b8106a41a4980f4f4d394615fd8c045";
    let filter = format!("{sender},0x6a4da820c8fc3258d1b44037572005264df821e2");
    let changes = crate::collect(&filter, &block).unwrap();
    assert_eq!(changes.nonce_changes.len(), 1);
    assert_eq!(changes.nonce_changes[0].origin.as_ref().unwrap().scope, Scope::TxFailedPersistent as i32);
    let out = project(&filter, &block).unwrap();
    assert!(out.storage.is_empty() && out.codes.is_empty() && out.lifecycle.is_empty());
    assert_eq!(out.nonces.len(), 1);
    assert_eq!(out.nonces[0].address, sender);
    assert_eq!(out.nonces[0].value, 425);
    assert_eq!(out.balances.iter().find(|v| v.address == sender).unwrap().value, "101368743581365559");
}

#[test]
fn captured_v5_delegation_clear_is_not_account_deletion() {
    let block = sample!("v5-clear-delegation", 5, 121464972);
    let out = project("0xdcef96a1771ae914b56686cbdb3beffc63f02fd9", &block).unwrap();
    assert_eq!(out.nonces[0].value, 141);
    assert_eq!(out.nonces[0].ordinal, 5282);
    assert_eq!(out.balances[0].value, "3090471330286710");
    assert_eq!(out.codes[0].code, "0x");
    assert_eq!(out.codes[0].hash, EMPTY_CODE_HASH);
    assert_eq!(out.codes[0].ordinal, 5283);
    assert_eq!(out.lifecycle.len(), 1);
    assert_eq!(out.lifecycle[0].kind, "code_cleared");
}

#[test]
fn captured_v4_installed_delegation_matches_rpc() {
    let block = sample!("v4-install-delegation", 4, 65805822);
    let out = project("0x417204ea716dfc4427bf9883521c820b036cdb7a", &block).unwrap();
    assert_eq!(out.nonces[0].value, 4292);
    assert_eq!(out.nonces[0].ordinal, 5079);
    assert_eq!(out.codes[0].code, "0xef01000000dcbcf779c73f0e0774fda22a1fe0f0f10000");
    assert_eq!(out.codes[0].hash, "0x4cf62dc7c5902b027cf76ba5c28b955cc1812c8d9d33358ad2e9101e9ef2d545");
    assert_eq!(out.codes[0].ordinal, 5080);
    assert!(out.storage.is_empty() && out.lifecycle.is_empty());
}

#[test]
fn captured_v4_failed_self_delegation_persists_nonce_and_code() {
    let block = sample!("v4-failed-self-delegation", 4, 64200086);
    let tx = &block.transaction_traces[0];
    assert_eq!(tx.status(), eth::TransactionTraceStatus::Failed);
    assert!(tx.calls[0].state_reverted);
    assert_eq!(tx.calls[0].begin_ordinal, 21804);
    let address = "0xa26b3b87710720def8c637c5de7b94eae25188c9";
    let changes = crate::collect(address, &block).unwrap();
    assert_eq!(changes.nonce_changes.iter().map(|c|
        (c.new_value, c.origin.as_ref().unwrap().scope)).collect::<Vec<_>>(),
        vec![(70, Scope::TxFailedPersistent as i32), (71, Scope::Tx7702 as i32)]);
    assert_eq!(changes.code_changes.len(), 1);
    assert_eq!(changes.code_changes[0].origin.as_ref().unwrap().scope, Scope::Tx7702 as i32);
    let out = project(address, &block).unwrap();
    assert_eq!(out.nonces[0].value, 71);
    assert_eq!(out.nonces[0].ordinal, 21802);
    assert_eq!(out.codes[0].code, "0xef010063c0c19a282a1b52b07dd5a65b58948a07dae32b");
    assert_eq!(out.codes[0].hash, "0xb09ef517c48d2bf6eed05457ff56871b2596e3fc904fc6e9795882a870c2e993");
    assert_eq!(out.codes[0].ordinal, 21803);
    assert_eq!(out.balances[0].value, "526996314130209396");
    assert!(out.storage.is_empty() && out.lifecycle.is_empty());
}

#[test]
fn captured_v3_wbnb_creation_matches_initial_slots_and_code() {
    let block = sample!("v3-wbnb-create", 3, 149268);
    assert_eq!(block.transaction_traces[0].calls[0].begin_ordinal, 0);
    let out = project("0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c", &block).unwrap();
    assert_eq!(out.nonces[0].value, 1);
    assert_eq!(out.codes[0].hash, "0xb7d84205eaaf83ce7b3940c6beaad6d22790255e34a9a2b486aa8cdfff118fe6");
    assert_eq!((out.codes[0].code.len() - 2) / 2, 3124);
    assert_eq!(out.storage.len(), 3);
    assert_eq!(out.storage[0].value, "0x5772617070656420424e42000000000000000000000000000000000000000016");
    assert_eq!(out.storage[1].value, "0x57424e4200000000000000000000000000000000000000000000000000000008");
    assert_eq!(out.storage[2].value, format!("0x{:064x}", 18));
}

#[test]
fn captured_v3_create2_and_delegated_initialization_match_rpc() {
    // RPC callTracer confirms CREATE2. The producer represents it as CREATE;
    // later delegated calls write storage for the newly created proxy account.
    let block = sample!("v3-create2", 3, 10000000);
    assert_eq!(block.transaction_traces[0].calls[1].call_type(), eth::CallType::Create);
    let out = project("0x58066f069811a69b8b5bac97c1dd76d54d428a72", &block).unwrap();
    assert_eq!(out.nonces[0].value, 1);
    assert_eq!(out.codes[0].code, "0x363d3d373d3d3d363d733d87ca9c54e8f64a3c75ba1f57764457856902795af43d82803e903d91602b57fd5bf3");
    assert_eq!(out.codes[0].hash, "0x487645ffef966b77a732b88516d55555a5ac76d8f4b5e1e0648c68b41fd76b68");
    assert_eq!(out.storage.len(), 3);
    assert_eq!(out.storage[0].value, "0x0000000000000000000000010000000000000000000000000000000000000000");
    assert_eq!(out.storage[1].slot, format!("0x{:064x}", 4));
    assert_eq!(out.storage[1].value, "0x000000000000000000000001f832fe2de5fdfe1d9681e7440b38a4d76f6ff2fb");
    assert_eq!(out.storage[1].ordinal, 934);
    assert_eq!(out.storage[2].value, "0x000000000000000000000000560027e2db2b8c25ca2e7d9fcdedea2e5e7da0b0");
    assert!(out.lifecycle.is_empty());
}

#[test]
fn captured_v3_pre_cancun_destruction_clears_all_three_accounts() {
    let block = sample!("v3-pre-cancun-selfdestruct", 3, 10000001);
    let addresses = ["0x070dc873b224fed094f6805d8a92328669311eb8",
        "0x7f8cd41fbb92f46b5b0a81284e55f180cf8b8669", "0x471f7890cfa751e1771e8abb6e69d709aa92d7a4"];
    let out = project(&addresses.join(","), &block).unwrap();
    assert_eq!(out.nonces.len(), 3);
    assert_eq!(out.codes.len(), 3);
    assert_eq!(out.lifecycle.len(), 6);
    for address in addresses {
        assert!(out.nonces.iter().any(|v| v.address == address && v.value == 0 && v.ordinal == 262));
        assert!(out.codes.iter().any(|v| v.address == address && v.code == "0x" && v.hash == EMPTY_CODE_HASH));
        assert!(out.lifecycle.iter().any(|v| v.address == address && v.kind == "storage_reset" && v.ordinal == 262));
    }
    assert!(out.storage.is_empty());
}

#[test]
fn captured_v3_post_cancun_same_transaction_creation_is_deleted() {
    let block = sample!("v3-post-cancun-create-selfdestruct", 3, 40000033);
    assert!(block.header.as_ref().unwrap().timestamp.as_ref().unwrap().seconds as u64 >= crate::lifecycle::BSC_CANCUN_TIME);
    let call = &block.transaction_traces[0].calls[1];
    assert!(call.suicide && !call.state_reverted);
    assert_eq!(call.call_type(), eth::CallType::Create);
    let out = project("0xcf67fc6b19d9b5dc510e589a9548940ace014943", &block).unwrap();
    // The trace records nonce 0 -> 1 during CREATE, but RPC's block-end nonce is
    // zero. Transaction-end deletion must take precedence over that increment.
    assert_eq!(out.nonces[0].value, 0);
    assert_eq!(out.nonces[0].ordinal, 3589);
    assert_eq!(out.codes[0].code, "0x");
    assert_eq!(out.codes[0].hash, EMPTY_CODE_HASH);
    assert!(out.lifecycle.iter().any(|v| v.kind == "storage_reset" && v.ordinal == 3589));
}

#[test]
fn captured_v3_same_address_destructions_reset_metadata_at_transaction_end() {
    let address = "0xe82c715e37f2f2e190dd2ca86fb796cafaf0beff";
    for (block, ordinal) in [
        (sample!("v3-metamorphic-destroy-1", 3, 37741077), 15880),
        (sample!("v3-metamorphic-destroy-2", 3, 37741218), 7707),
    ] {
        let tx = &block.transaction_traces[0];
        assert!(tx.calls[0].suicide && !tx.calls[0].state_reverted);
        // These producer messages omit explicit code and nonce clears. The
        // lifecycle marker must still make the block-end account empty.
        assert!(tx.calls.iter().all(|call| call.code_changes.is_empty()));
        let out = project(address, &block).unwrap();
        assert_eq!(out.nonces.len(), 1);
        assert_eq!((out.nonces[0].value, out.nonces[0].ordinal), (0, ordinal));
        assert_eq!(out.codes[0].code, "0x");
        assert_eq!(out.codes[0].hash, EMPTY_CODE_HASH);
        assert_eq!(out.balances[0].value, "0");
        assert!(out.lifecycle.iter().any(|v| v.kind == "storage_reset" && v.ordinal == ordinal));
        assert!(out.storage.is_empty());
    }
}

#[test]
fn captured_v3_recreation_installs_each_new_code_version() {
    let address = "0xe82c715e37f2f2e190dd2ca86fb796cafaf0beff";
    for (block, bytes, code_hash) in [
        (sample!("v3-metamorphic-create-1", 3, 37741078), 8227,
         "0xbb292f84e213053852c8011d016195f296785d8fd9e29c010bff2550bb681df9"),
        (sample!("v3-metamorphic-create-2", 3, 37741220), 8226,
         "0xf5d397bbb27d1f4304f0bcdad0bfa2d9d2bfece7bed8769cc198093b9890c215"),
    ] {
        let out = project(address, &block).unwrap();
        assert_eq!(out.nonces.len(), 1);
        assert_eq!(out.nonces[0].value, 1);
        assert_eq!(out.codes[0].hash, code_hash);
        assert_eq!((out.codes[0].code.len() - 2) / 2, bytes);
        assert!(out.storage.is_empty() && out.lifecycle.is_empty());
    }
}

#[test]
fn captured_v3_storage_between_recreation_cycles_keeps_last_committed_write() {
    let block = sample!("v3-metamorphic-storage", 3, 37741154);
    let out = project("0xe82c715e37f2f2e190dd2ca86fb796cafaf0beff", &block).unwrap();
    assert_eq!(out.storage.len(), 2);
    assert_eq!(out.storage[0].slot, format!("0x{:064x}", 0));
    assert_eq!(out.storage[0].value, format!("0x{:064x}", 0));
    assert_eq!(out.storage[0].ordinal, 3225);
    assert_eq!(out.storage[1].slot, "0xb82207f487d5f82a808c4a79eaef2903fd056d9256cb1af55d518291f0176329");
    assert_eq!(out.storage[1].value, "0x0000000000000000000000000000000000000000000000000080000000000000");
    assert_eq!(out.storage[1].ordinal, 3234);
    assert!(out.lifecycle.is_empty());
}

#[test]
fn captured_v5_reverted_repeated_authorizations_keep_accepted_nonce_and_code() {
    let block = sample!("v5-failed-repeated-authority", 5, 121468046);
    let tx = &block.transaction_traces[0];
    assert_eq!(tx.status(), eth::TransactionTraceStatus::Reverted);
    assert!(tx.calls[0].state_reverted);
    assert_eq!(tx.calls[0].begin_ordinal, 2334);
    assert_eq!(tx.set_code_authorizations.iter().map(|a| a.discarded).collect::<Vec<_>>(),
               vec![true, false, false]);
    let sender = "0x1a222f9b072aed5e9e815849de5a11b0fe3958ab";
    let authority = "0x7f7f5004b2fb7ede48fd8b326b30b71bd476479b";
    let filter = format!("{sender},{authority}");
    let changes = crate::collect(&filter, &block).unwrap();
    assert_eq!(changes.nonce_changes.iter().map(|c|
        (c.new_value, c.origin.as_ref().unwrap().scope)).collect::<Vec<_>>(),
        vec![(38853, Scope::TxFailedPersistent as i32), (166, Scope::Tx7702 as i32), (167, Scope::Tx7702 as i32)]);
    assert_eq!(changes.code_changes.len(), 1);
    assert_eq!(changes.code_changes[0].origin.as_ref().unwrap().scope, Scope::Tx7702 as i32);
    let out = project(&filter, &block).unwrap();
    assert_eq!(out.nonces.len(), 2);
    let nonce = out.nonces.iter().find(|v| v.address == authority).unwrap();
    assert_eq!((nonce.value, nonce.ordinal), (167, 2333));
    assert_eq!(out.codes[0].code, "0xef01002c6f01799fa9db1e7f2797e5ba892b72b865571d");
    assert_eq!(out.codes[0].hash, "0xa6af382bc82af1d985fae744c8d392a6f22a0ddf94d9ebed305860a5416bf2e7");
    assert_eq!(out.codes[0].ordinal, 2332);
    assert_eq!(out.balances.iter().find(|v| v.address == sender).unwrap().value, "69709153906374907");
    assert!(out.storage.is_empty() && out.lifecycle.is_empty());
}

#[test]
fn captured_v5_three_accepted_authorizations_increment_one_authority_three_times() {
    let block = sample!("v5-three-accepted-repeated", 5, 121468057);
    let authority = "0x952c6e846a50d4533bfdf0f144ed7d6cf3e06eae";
    let tx = &block.transaction_traces[0];
    assert_eq!(tx.set_code_authorizations.len(), 3);
    assert!(tx.set_code_authorizations.iter().all(|a| !a.discarded));
    let changes = crate::collect(authority, &block).unwrap();
    assert_eq!(changes.nonce_changes.iter().map(|c| c.new_value).collect::<Vec<_>>(), vec![80, 81, 82]);
    let out = project(authority, &block).unwrap();
    assert_eq!(out.nonces.len(), 1);
    assert_eq!((out.nonces[0].value, out.nonces[0].ordinal), (82, 1868));
    // The already-installed delegation is unchanged; no code patch is needed.
    assert!(out.codes.is_empty() && out.storage.is_empty() && out.lifecycle.is_empty());
}

#[test]
fn captured_v5_discarded_higher_nonces_do_not_replace_the_accepted_nonce() {
    let block = sample!("v5-discarded-repeated-authority", 5, 121468236);
    let tx = &block.transaction_traces[0];
    assert_eq!(tx.set_code_authorizations.len(), 13);
    assert_eq!(tx.set_code_authorizations[0].nonce, 30829);
    assert!(tx.set_code_authorizations[..12].iter().all(|a| a.discarded));
    assert!(!tx.set_code_authorizations[12].discarded);
    assert_eq!(tx.set_code_authorizations[12].nonce, 30817);
    let out = project("0xaadf24cd8aa8ba3c98c0bca989552f79401e98a5", &block).unwrap();
    assert_eq!(out.nonces.len(), 1);
    assert_eq!((out.nonces[0].value, out.nonces[0].ordinal), (30818, 5490));
    assert!(out.codes.is_empty() && out.storage.is_empty() && out.lifecycle.is_empty());
}
