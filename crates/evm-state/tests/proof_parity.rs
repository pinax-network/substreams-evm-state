use alloy_primitives::{keccak256, U256};
use alloy_trie::EMPTY_ROOT_HASH;
use evm_state::{
    header::{encode_rpc_header, verify_header},
    proof::*,
};
use serde_json::{json, Value};

fn fixture() -> Value {
    serde_json::from_str(include_str!("../../../tests/fixtures/proof-parity.json")).unwrap()
}

fn pairs(value: &Value) -> Vec<anyhow::Result<(String, String)>> {
    value
        .as_array()
        .unwrap()
        .iter()
        .map(|row| {
            Ok((
                row[0].as_str().unwrap().into(),
                row[1].as_str().unwrap().into(),
            ))
        })
        .collect()
}

fn word(n: u64) -> String {
    format!("0x{n:064x}")
}

#[test]
fn account_and_complete_storage_match_independent_golden_proof() {
    let f = fixture();
    let a = &f["account"];
    let proven = verify_account(
        a["state_root"].as_str().unwrap(),
        a["address"].as_str().unwrap(),
        &a["proof"],
    )
    .unwrap();
    assert_eq!(proven.nonce, 3);
    assert_eq!(proven.balance, U256::from(1) << 200);
    assert!(proven.exists);
    let temp = tempfile::tempdir().unwrap();
    assert_eq!(
        verify_complete(
            &proven,
            pairs(&a["slots"]),
            a["code"].as_str().unwrap(),
            &proven.json(),
            &temp.path().join("slots.sqlite")
        )
        .unwrap(),
        2
    );
}

#[test]
fn rpc_metadata_cannot_override_proven_values() {
    let f = fixture();
    let a = &f["account"];
    for (field, value) in [
        ("nonce", "0x4".into()),
        ("balance", "0x0".into()),
        ("storageHash", word(0)),
        ("codeHash", word(0)),
    ] {
        let mut proof = a["proof"].clone();
        proof[field] = json!(value);
        assert!(
            verify_account(
                a["state_root"].as_str().unwrap(),
                a["address"].as_str().unwrap(),
                &proof
            )
            .is_err(),
            "{field}"
        );
    }
}

#[test]
fn missing_proof_wrong_root_and_wrong_address_fail() {
    let f = fixture();
    let a = &f["account"];
    let root = a["state_root"].as_str().unwrap();
    let selected = a["address"].as_str().unwrap();
    assert!(verify_account(&word(22), selected, &a["proof"]).is_err());
    assert!(verify_account(root, &format!("0x{}", "22".repeat(20)), &a["proof"]).is_err());
    let mut proof = a["proof"].clone();
    proof["accountProof"] = json!([]);
    assert!(verify_account(root, selected, &proof).is_err());
}

#[test]
fn proven_absence_is_distinct_from_missing_metadata() {
    let root = format!("{EMPTY_ROOT_HASH:#x}");
    let selected = format!("0x{}", "11".repeat(20));
    let mut proof = json!({"address":selected,"nonce":"0x0","balance":"0x0","storageHash":root,
        "codeHash":format!("{:#x}",keccak256([])),"accountProof":[]});
    let account = verify_account(&root, &selected, &proof).unwrap();
    assert!(!account.exists);
    let temp = tempfile::tempdir().unwrap();
    assert_eq!(
        verify_complete(
            &account,
            [],
            "0x",
            &account.json(),
            &temp.path().join("empty.sqlite")
        )
        .unwrap(),
        0
    );
    proof["nonce"] = json!("0x1");
    assert!(verify_account(&root, &selected, &proof).is_err());
    proof.as_object_mut().unwrap().remove("nonce");
    assert!(verify_account(&root, &selected, &proof).is_err());
}

#[test]
fn partial_extra_duplicate_zero_or_wrong_metadata_cannot_pass() {
    let f = fixture();
    let a = &f["account"];
    let proven = verify_account(
        a["state_root"].as_str().unwrap(),
        a["address"].as_str().unwrap(),
        &a["proof"],
    )
    .unwrap();
    for defect in [
        "missing_slot",
        "extra_slot",
        "duplicate_slot",
        "zero_slot",
        "wrong_nonce",
        "missing_balance",
        "wrong_code",
        "missing_code_hash",
    ] {
        let mut slots = a["slots"].clone();
        let mut metadata = proven.json();
        let mut code = a["code"].as_str().unwrap();
        match defect {
            "missing_slot" => {
                slots.as_array_mut().unwrap().pop();
            }
            "extra_slot" => slots
                .as_array_mut()
                .unwrap()
                .push(json!([word(999), word(1)])),
            "duplicate_slot" => {
                let first = slots[0].clone();
                slots.as_array_mut().unwrap().push(first);
            }
            "zero_slot" => slots
                .as_array_mut()
                .unwrap()
                .push(json!([word(999), word(0)])),
            "wrong_nonce" => metadata["nonce"] = json!(4),
            "missing_balance" => {
                metadata.as_object_mut().unwrap().remove("balance");
            }
            "wrong_code" => code = "0x",
            "missing_code_hash" => {
                metadata.as_object_mut().unwrap().remove("code_hash");
            }
            _ => unreachable!(),
        }
        let temp = tempfile::tempdir().unwrap();
        assert!(
            verify_complete(
                &proven,
                pairs(&slots),
                code,
                &metadata,
                &temp.path().join("slots.sqlite")
            )
            .is_err(),
            "{defect}"
        );
    }
}

#[test]
fn malformed_input_poisoned_workspace_and_source_failure_are_rejected() {
    for (slot, value) in [
        (word(1), word(0)),
        ("0x01".into(), word(1)),
        (word(1), "0x01".into()),
    ] {
        let temp = tempfile::tempdir().unwrap();
        let mut db = StorageSort::new(&temp.path().join("slots.sqlite")).unwrap();
        assert!(db.insert(&slot, &value).is_err());
        assert!(db.insert(&word(2), &word(3)).is_err());
        assert!(db.finish().is_err());
    }
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("duplicate.sqlite");
    let mut db = StorageSort::new(&path).unwrap();
    db.insert(&format!("0x{}", "AB".repeat(32)), &word(1))
        .unwrap();
    assert!(db
        .insert(&format!("0x{}", "ab".repeat(32)), &word(1))
        .is_err());
    assert!(db.finish().is_err());
    assert!(StorageSort::new(&path).is_err());
    assert!(storage_root(
        [
            Ok((word(1), word(2))),
            Err(anyhow::anyhow!("source interrupted"))
        ],
        &temp.path().join("broken.sqlite")
    )
    .is_err());
}

#[test]
fn header_fork_encodings_match_independent_golden_vectors() {
    for h in fixture()["headers"].as_array().unwrap() {
        assert_eq!(
            encode_rpc_header(&h["rpc"]).unwrap(),
            h["encoded"].as_str().unwrap()
        );
        verify_header(
            h["encoded"].as_str().unwrap(),
            &h["summary"],
            h["rpc"]["hash"].as_str(),
        )
        .unwrap();
    }
}

#[test]
fn changed_rpc_or_manifest_fields_cannot_retain_a_pinned_hash() {
    let f = fixture();
    let h = &f["headers"][4];
    for field in [
        "stateRoot",
        "number",
        "timestamp",
        "parentHash",
        "requestsHash",
    ] {
        let mut rpc = h["rpc"].clone();
        rpc[field] = json!(if field == "number" || field == "timestamp" {
            "0x55".into()
        } else {
            word(55)
        });
        assert!(encode_rpc_header(&rpc).is_err(), "{field}");
    }
    for field in ["state_root", "number", "timestamp", "parent_hash", "hash"] {
        let mut summary = h["summary"].clone();
        summary[field] = if field == "number" || field == "timestamp" {
            json!(42)
        } else {
            json!(word(55))
        };
        assert!(
            verify_header(h["encoded"].as_str().unwrap(), &summary, None).is_err(),
            "{field}"
        );
    }
    let mut rpc = h["rpc"].clone();
    rpc.as_object_mut().unwrap().remove("stateRoot");
    assert!(encode_rpc_header(&rpc).is_err());
    let mut trailing = h["encoded"].as_str().unwrap().to_string();
    trailing.push_str("80");
    assert!(verify_header(&trailing, &h["summary"], None).is_err());
}

#[test]
fn real_bsc_header_matches_recorded_block_hash() {
    let rpc: Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/bsc-121294292-header.json"
    ))
    .unwrap();
    let encoded = encode_rpc_header(&rpc).unwrap();
    assert_eq!(
        hex::encode(keccak256(unhex(&encoded).unwrap())),
        "d0987f468e3b66fa2593eb2df41924fed5fd563c697828916416de436784bd90"
    );
}
