#[path = "support/portable.rs"]
mod portable;
use anyhow::Result;
use evm_state::{
    postgres::{self, Postgres},
    rpc::RpcCall,
};
use serde_json::{json, Value};

fn snapshot() -> Result<(Value, Value, tempfile::TempDir)> {
    let root = tempfile::tempdir()?;
    let layout = portable::create(&root.path().join("input"))?;
    let ready = &layout["checkpoint"];
    let mut account =
        serde_json::from_slice::<Value>(&std::fs::read(root.path().join("input/accounts.json"))?)?
            [0]
        .clone();
    account["last_block"] = ready["header"]["number"].clone();
    let mut storage = portable::rows()?;
    for row in &mut storage {
        row["last_block"] = ready["header"]["number"].clone();
    }
    storage.push(json!({"address":portable::A,"slot":format!("0x{:064x}",2),"value":format!("0x{:064x}",0),"last_block":ready["header"]["number"]}));
    Ok((
        json!({"header":ready["header"],"accounts":[account],"storage":storage}),
        ready["proof_bundle"].clone(),
        root,
    ))
}
#[test]
fn full_legacy_diagnostic_proves_metadata_code_and_every_nonzero_slot() -> Result<()> {
    let (value, bundle, root) = snapshot()?;
    let verified = postgres::verify(&value, &bundle, true, None, None, root.path())?;
    assert_eq!(verified["storage_slots_checked"], 2);
    assert_eq!(verified["storage_completeness_verified"], true);
    assert_eq!(verified["published_checkpoint"], false);
    Ok(())
}
#[test]
fn missing_and_mismatched_metadata_fail_despite_correct_storage() -> Result<()> {
    for field in ["nonce", "balance", "code_hash", "code"] {
        for missing in [true, false] {
            let (mut value, bundle, root) = snapshot()?;
            value["accounts"][0][field] = if missing {
                Value::Null
            } else {
                match field {
                    "nonce" | "balance" => json!("999"),
                    "code" => json!("0x"),
                    _ => json!(format!("0x{:064x}", 0)),
                }
            };
            assert!(
                postgres::verify(&value, &bundle, true, None, None, root.path()).is_err(),
                "{field}/{missing}"
            );
        }
    }
    Ok(())
}
#[test]
fn missing_extra_duplicate_future_and_unbound_rows_never_pass_completeness() -> Result<()> {
    for defect in [
        "missing_slot",
        "extra_slot",
        "root",
        "hash",
        "proof",
        "future",
        "no_head",
        "no_account",
        "duplicate",
        "foreign",
        "coverage",
        "encoded_header",
    ] {
        let (mut value, mut bundle, root) = snapshot()?;
        match defect {
            "missing_slot" => {
                value["storage"].as_array_mut().unwrap().remove(0);
            }
            "extra_slot" => value["storage"][2]["value"] = json!(format!("0x{:064x}", 9)),
            "root" => value["header"]["state_root"] = json!(format!("0x{:064x}", 0)),
            "hash" => value["header"]["hash"] = json!(format!("0x{:064x}", 0)),
            "proof" => bundle["accounts"][portable::A]["proof"]["accountProof"] = json!([]),
            "future" => {
                value["accounts"][0]["last_block"] =
                    json!(value["header"]["number"].as_u64().unwrap() + 1)
            }
            "no_head" => value["header"] = Value::Null,
            "no_account" => value["accounts"] = json!([]),
            "duplicate" => {
                let duplicate = value["storage"][0].clone();
                value["storage"].as_array_mut().unwrap().push(duplicate);
            }
            "foreign" => {
                value["storage"][0]["address"] = json!("0x2222222222222222222222222222222222222222")
            }
            "coverage" => bundle["accounts"] = json!({}),
            _ => bundle["header_rlp"] = json!("0xc0"),
        }
        assert!(
            postgres::verify(&value, &bundle, true, None, None, root.path()).is_err(),
            "{defect}"
        );
    }
    Ok(())
}
#[test]
fn exact_head_and_exact_uint64_nonce_are_required() -> Result<()> {
    let (value, bundle, root) = snapshot()?;
    let number = value["header"]["number"].as_u64().unwrap();
    for requested in [0, number - 1, number + 1] {
        assert!(
            postgres::verify(&value, &bundle, true, None, Some(requested), root.path())
                .unwrap_err()
                .to_string()
                .contains("historical state")
        );
    }
    for bad in [
        json!(true),
        json!(1.5),
        json!("01"),
        json!("-1"),
        json!("18446744073709551616"),
    ] {
        let mut value = value.clone();
        value["accounts"][0]["nonce"] = bad;
        assert!(postgres::validate_snapshot(&value, None).is_err());
    }
    Ok(())
}
struct Provider {
    value: Value,
    changed: bool,
    wrong: bool,
}
impl RpcCall for Provider {
    fn call(&self, method: &str, args: Value) -> Result<Value> {
        let number = self.value["header"]["number"].as_u64().unwrap();
        if method == "eth_getStorageAt" {
            assert_eq!(args[2], format!("0x{number:x}"));
            if self.wrong {
                return Ok(json!(format!("0x{:064x}", 99)));
            }
            return Ok(self.value["storage"]
                .as_array()
                .unwrap()
                .iter()
                .find(|r| r["slot"] == args[1])
                .unwrap()["value"]
                .clone());
        }
        assert_eq!(method, "eth_getBlockByNumber");
        assert_eq!(args, json!([format!("0x{number:x}"), false]));
        Ok(
            json!({"hash":if self.changed {json!(format!("0x{:064x}",999))} else {self.value["header"]["hash"].clone()}}),
        )
    }
}
#[test]
fn sampled_parity_is_incomplete_and_checks_storage_and_rpc_header() -> Result<()> {
    let (value, bundle, root) = snapshot()?;
    let mut rpc = Provider {
        value: value.clone(),
        changed: false,
        wrong: false,
    };
    let verified = postgres::verify(&value, &bundle, false, Some(&rpc), None, root.path())?;
    assert_eq!(verified["status"], "sample-parity-only");
    assert_eq!(verified["storage_slots_checked"], 3);
    assert_eq!(verified["storage_completeness_verified"], false);
    rpc.changed = true;
    assert!(postgres::verify(&value, &bundle, false, Some(&rpc), None, root.path()).is_err());
    rpc.changed = false;
    rpc.wrong = true;
    assert!(postgres::verify(&value, &bundle, false, Some(&rpc), None, root.path()).is_err());
    assert!(postgres::verify(&value, &bundle, false, None, None, root.path()).is_err());
    Ok(())
}
#[test]
fn invalid_sql_inputs_and_connection_errors_do_not_expose_credentials() -> Result<()> {
    let database = Postgres::new(
        Some("postgresql://dummy:private-dummy-password@127.0.0.1:1/dummy?connect_timeout=1"),
        None,
    );
    assert!(database.snapshot(Some("0x' OR TRUE--"), Some(1)).is_err());
    assert!(database.snapshot(Some(portable::A), Some(0)).is_err());
    let error = database.snapshot(Some(portable::A), None).unwrap_err();
    assert!(!format!("{error:#}").contains("private-dummy-password"));
    assert!(!format!("{error:#}").contains("postgresql://"));
    Ok(())
}
