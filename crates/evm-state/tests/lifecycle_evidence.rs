//! Preserve independently captured producer/header/RPC assertions during the
//! language migration. These artifacts describe their original measured runs.
use alloy_primitives::{keccak256, U256};
use anyhow::Result;
use evm_state::{
    header::verify_header,
    lifecycle_qualification::{self, matching_fields},
    proof::{fixed, quantity, string, unhex, verify_account},
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::PathBuf,
};
fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}
fn captures() -> PathBuf {
    root().join("tests/fixtures/lifecycle")
}
fn read(path: impl AsRef<std::path::Path>) -> Result<Value> {
    Ok(serde_json::from_slice(&fs::read(path)?)?)
}
fn records() -> Result<BTreeMap<String, Value>> {
    Ok(read(captures().join("manifest.json"))?["records"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| (r["filename"].as_str().unwrap().into(), r.clone()))
        .collect())
}
fn evidence(name: &str) -> Result<Value> {
    read(root().join(format!("docs/evidence/{name}-2026-09-12.json")))
}
fn fields_match_proofs(value: &Value, proof_file: &str, hash: Option<&str>) -> Result<()> {
    let raw = fs::read(captures().join(proof_file))?;
    assert_eq!(
        value["proof_bundle_sha256"],
        hex::encode(Sha256::digest(&raw))
    );
    let bundle: Value = serde_json::from_slice(&raw)?;
    assert_eq!(value["header"], bundle["header"]);
    verify_header(string(&bundle, "header_rlp")?, &bundle["header"], hash)?;
    let fields = &value["metadata_verified_against_account_proofs"];
    for (address, value) in bundle["accounts"].as_object().unwrap() {
        let account = verify_account(
            string(&bundle["header"], "state_root")?,
            address,
            &value["proof"],
        )?;
        assert_eq!(keccak256(unhex(string(value, "code")?)?), account.code_hash);
        let mut expected = account.json();
        expected["code"] = value["code"].clone();
        matching_fields(&fields[address], &expected)?;
    }
    Ok(())
}
#[test]
fn all_captured_lifecycle_messages_headers_bytecode_and_create2_trace_match_provenance(
) -> Result<()> {
    let manifest = read(captures().join("manifest.json"))?;
    assert_eq!(manifest["chain_id"], 56);
    let records = records()?;
    assert_eq!(records.len(), 26);
    assert_eq!(
        records
            .values()
            .map(|r| r["producer_version"].as_u64().unwrap())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([3, 4, 5])
    );
    for record in records.values() {
        lifecycle_qualification::validate_capture(record, &captures())?;
        assert_eq!(
            quantity(string(record, "rpc_receipt_status")?, 64)? == U256::from(1),
            record["status"] == 1
        );
        if let Some(trace) = record.get("rpc_calltrace") {
            let raw = fs::read(captures().join(string(trace, "filename")?))?;
            assert_eq!(trace["sha256"], hex::encode(Sha256::digest(&raw)));
            let trace: Value = serde_json::from_slice(&raw)?;
            assert!(trace["calls"]
                .as_array()
                .unwrap()
                .iter()
                .any(|c| c["type"] == "CREATE2"
                    && c["to"] == "0x58066f069811a69b8b5bac97c1dd76d54d428a72"));
        }
    }
    Ok(())
}
#[test]
fn damaged_capture_files_headers_and_code_checksums_fail() -> Result<()> {
    let records = records()?;
    let record = records.values().next().unwrap();
    for defect in ["sha", "length", "header", "block", "code", "path"] {
        let mut value = record.clone();
        match defect {
            "sha" => value["sha256"] = json!("00"),
            "length" => value["bytes"] = json!(0),
            "header" => value["header"]["state_root"] = json!(format!("0x{}", "00".repeat(32))),
            "block" => value["block"] = json!(1),
            "path" => value["filename"] = json!("../outside"),
            "code" => {
                let address = value["rpc_block_end_state"]
                    .as_object()
                    .unwrap()
                    .keys()
                    .next()
                    .unwrap()
                    .clone();
                value["rpc_block_end_state"][address]["after"]["code_hash"] =
                    json!(format!("0x{}", "00".repeat(32)));
            }
            _ => unreachable!(),
        }
        assert!(
            lifecycle_qualification::validate_capture(&value, &captures()).is_err(),
            "{defect}"
        );
    }
    Ok(())
}
#[test]
fn saved_native_lifecycle_and_authorization_updates_match_full_account_proofs() -> Result<()> {
    for (name, file, accounts, fields, blocks, hash) in [
        (
            "bsc-lifecycle-matrix",
            "recent-proofs.json",
            8,
            24,
            None,
            Some("0x7cc97d80f89e23bc713c37d6150b27bb65bb56eb78955c082c12a53a78ee0b04"),
        ),
        (
            "bsc-authorization-edges",
            "authorization-edge-proofs.json",
            5,
            9,
            Some(9740),
            Some("0x352f27ef9341ca0976bdba3f3f444ddfc40ed27d434996168f8c13a58eb9ba11"),
        ),
        (
            "bsc-failed-distinct-authorities",
            "failed-distinct-proofs.json",
            3,
            8,
            Some(80813),
            Some("0x848c7f22bf3e1846f080d80623eef146f266db5bf8085818aa6ab7ea7eed5cb0"),
        ),
        (
            "bsc-existing-selfdestruct-recent",
            "existing-selfdestruct-proofs.json",
            3,
            7,
            Some(280137),
            None,
        ),
        (
            "bsc-failed-clears",
            "failed-clear-proofs.json",
            3,
            10,
            Some(959503),
            None,
        ),
    ] {
        let value = evidence(name)?;
        fields_match_proofs(&value, file, hash)?;
        if let Some(blocks) = blocks {
            assert_eq!(value["blocks"], blocks);
        }
        let actual = value["metadata_verified_against_account_proofs"]
            .as_object()
            .unwrap();
        assert_eq!(actual.len(), accounts);
        assert_eq!(
            actual
                .values()
                .map(|v| v.as_object().unwrap().len())
                .sum::<usize>(),
            fields
        );
        if name == "bsc-failed-distinct-authorities" {
            assert_eq!(value["start_block"], 121403152);
            for authority in [
                "0xa96669262c911d4158e26b972aaeabbb08979ddb",
                "0x0dcc966314b622bf094c7afb31b6632d646880f9",
            ] {
                assert_eq!(actual[authority]["nonce"], 1);
                assert_eq!(
                    actual[authority]["code"],
                    "0xef0100cb4dd2ac21ee75be478989d8b05897de225e5910"
                );
            }
        }
        if matches!(
            name,
            "bsc-existing-selfdestruct-recent" | "bsc-failed-clears"
        ) {
            assert_eq!(
                value["final_touched_slots_verified_against_archive_rpc"],
                json!({})
            );
        }
    }
    Ok(())
}
#[test]
fn captured_recreation_metadata_and_slot_resets_remain_bound_to_native_evidence() -> Result<()> {
    let value = evidence("bsc-recreation")?;
    let records = records()?;
    assert_eq!(value["blocks"], 145);
    assert_eq!(value["storage_patches_in_interval"], 2);
    assert_eq!(value["comparisons"].as_array().unwrap().len(), 5);
    let slot = "0xb82207f487d5f82a808c4a79eaef2903fd056d9256cb1af55d518291f0176329";
    for checked in value["comparisons"].as_array().unwrap() {
        let record = &records[string(checked, "fixture")?];
        assert_eq!(record["sha256"], checked["fixture_sha256"]);
        assert_eq!(record["block_hash"], checked["block_hash"]);
        let expected = &record["rpc_block_end_state"][string(&value, "account")?]["after"];
        matching_fields(&checked["native_metadata"], expected)?;
        assert_eq!(
            checked["code_bytes"],
            unhex(string(expected, "code")?)?.len()
        );
        let expected = if checked["block"] == 37741154 {
            U256::from(1) << 55
        } else {
            U256::ZERO
        };
        assert_eq!(
            U256::from_be_bytes(fixed::<32>(string(&checked["tracked_slot_values"], slot)?)?),
            expected
        );
        if [37741077, 37741218].iter().any(|n| checked["block"] == *n) {
            assert!(checked["lifecycle"]
                .as_array()
                .unwrap()
                .iter()
                .any(|v| v["kind"] == "storage_reset" && v["ordinal"] == record["tx_end_ordinal"]));
        }
    }
    Ok(())
}
#[test]
fn surviving_selfdestruct_captures_preserve_code_nonce_and_native_diagnostics() -> Result<()> {
    let records = records()?;
    for (version, count, bytes) in [(3, 6, 21), (4, 1, 427), (5, 1, 4)] {
        let value = evidence(&format!("bsc-existing-selfdestruct-v{version}"))?;
        let record = &records[string(&value, "fixture")?];
        assert_eq!(value["fixture_sha256"], record["sha256"]);
        assert_eq!(value["header"], record["header"]);
        assert_eq!(value["blocks"], 2);
        assert_eq!(value["comparisons"].as_object().unwrap().len(), count);
        for (address, checked) in value["comparisons"].as_object().unwrap() {
            let states = &record["rpc_block_end_state"][address];
            assert!(!checked["selfdestruct_ordinals"]
                .as_array()
                .unwrap()
                .is_empty());
            for side in states.as_object().unwrap().values() {
                assert_eq!(checked["code_hash_before_and_after"], side["code_hash"]);
                assert_eq!(checked["nonce_before_and_after"], side["nonce"]);
                assert_eq!(side["nonce"], 1);
                assert_eq!(
                    checked["code_bytes_before_and_after"],
                    unhex(string(side, "code")?)?.len()
                );
                assert_eq!(checked["code_bytes_before_and_after"], bytes);
            }
            matching_fields(&checked["observed_native_fields"], &states["after"])?;
        }
    }
    Ok(())
}
fn clear_inputs(checked: &Value, record: &Value) -> Result<(Value, Value)> {
    let authority = string(checked, "authority")?;
    let codes = checked["native_code_patches"].as_array().unwrap();
    let markers = checked["native_lifecycle"].as_array().unwrap();
    let row = json!({"hash":checked["block_hash"],"storage.address":[],"codes.address":vec![authority;codes.len()],"codes.code":codes.iter().map(|c|c["code"].clone()).collect::<Vec<_>>(),"codes.ordinal":codes.iter().map(|c|c["ordinal"].clone()).collect::<Vec<_>>(),"lifecycle.address":vec![authority;markers.len()],"lifecycle.kind":markers.iter().map(|c|c["kind"].clone()).collect::<Vec<_>>(),"lifecycle.ordinal":markers.iter().map(|c|c["ordinal"].clone()).collect::<Vec<_>>()});
    let mut fields = checked["native_metadata"].clone();
    if !codes.is_empty() {
        fields[authority]["code"] =
            record["rpc_block_end_state"][authority]["after"]["code"].clone();
    }
    Ok((row, fields))
}
#[test]
fn captured_clear_checks_retain_exact_ordinals_and_reject_later_update_substitution() -> Result<()>
{
    let records = records()?;
    for (name, blocks, cases) in [
        ("bsc-failed-clears-captured", 60249, 2),
        ("bsc-invalid-self-clear", 40373, 1),
        ("bsc-failed-clear-reinstall-v4", 2, 1),
    ] {
        let value = evidence(name)?;
        assert_eq!(value["blocks"], blocks);
        assert_eq!(value["comparisons"].as_array().unwrap().len(), cases);
        for checked in value["comparisons"].as_array().unwrap() {
            let record = &records[string(checked, "fixture")?];
            assert_eq!(checked["fixture_sha256"], record["sha256"]);
            assert_eq!(checked["block_hash"], record["block_hash"]);
            assert_eq!(checked["block"], record["block"]);
            let (row, fields) = clear_inputs(checked, record)?;
            assert_eq!(
                lifecycle_qualification::clear_comparison(record, &row, &fields)?,
                *checked
            );
            let nonce = match string(checked, "fixture")? {
                "v5-failed-authority-clear.pb" => 10581,
                "v5-failed-self-clear.pb" => 40,
                "v5-invalid-self-clear-noop.pb" => 102,
                _ => 2966,
            };
            let authority = string(checked, "authority")?;
            assert_eq!(fields[authority]["nonce"], nonce);
            assert_eq!(checked["code_bytes_before"], 23);
            for defect in ["marker", "code", "storage", "nonce", "ordinal"] {
                let (mut row, mut fields) = clear_inputs(checked, record)?;
                match defect {
                    "marker" => {
                        row["lifecycle.address"] = json!([authority]);
                        row["lifecycle.kind"] = json!(["storage_reset"]);
                        row["lifecycle.ordinal"] = json!([1]);
                    }
                    "code" => fields[authority]["code"] = json!("0xdead"),
                    "storage" => row["storage.address"] = json!([authority]),
                    "nonce" => fields[authority]["nonce"] = json!(0),
                    "ordinal" => {
                        row["codes.address"] = json!([authority]);
                        row["codes.code"] = json!(["0x"]);
                        row["codes.ordinal"] = json!([99999]);
                    }
                    _ => unreachable!(),
                }
                assert!(
                    lifecycle_qualification::clear_comparison(record, &row, &fields).is_err(),
                    "{name}: {defect}"
                );
            }
        }
    }
    Ok(())
}
