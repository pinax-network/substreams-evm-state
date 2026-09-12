use anyhow::{ensure, Result};
use evm_state::{
    proof::{quantity, string},
    rpc::{self, RpcCall},
};
use serde_json::{json, Value};
use std::cell::RefCell;

struct Fixture {
    responses: RefCell<Vec<(String, Value, Value)>>,
}
impl RpcCall for Fixture {
    fn call(&self, method: &str, params: Value) -> Result<Value> {
        let (expected, arguments, response) = self.responses.borrow_mut().remove(0);
        ensure!(
            expected == method && arguments == params,
            "unexpected RPC request"
        );
        Ok(response)
    }
}
fn header() -> Value {
    serde_json::from_str::<Value>(include_str!("../../../tests/fixtures/proof-parity.json"))
        .unwrap()["headers"][0]["rpc"]
        .clone()
}
#[test]
fn finalized_capture_requires_exact_requested_number_before_requesting_proofs() -> Result<()> {
    let raw = header();
    let number = quantity(string(&raw, "number")?, 64)?.to::<u64>();
    for requested in [number - 1, number + 1] {
        let rpc = Fixture {
            responses: RefCell::new(vec![
                (
                    "eth_getBlockByNumber".into(),
                    json!(["finalized", false]),
                    raw.clone(),
                ),
                (
                    "eth_getBlockByNumber".into(),
                    json!([format!("0x{requested:x}"), false]),
                    raw.clone(),
                ),
            ]),
        };
        let error = rpc::capture(
            &rpc,
            ["0x1111111111111111111111111111111111111111"],
            Some(requested),
            None,
        )
        .unwrap_err();
        assert!(error.to_string().contains("different block number"));
        assert!(rpc.responses.borrow().is_empty());
    }
    Ok(())
}
#[test]
fn finalized_header_validates_hash_and_finality_for_implicit_and_explicit_targets() -> Result<()> {
    let raw = header();
    let number = quantity(string(&raw, "number")?, 64)?.to::<u64>();
    for explicit in [false, true] {
        for defect in ["none", "hash", "unfinalized"] {
            let mut selected = raw.clone();
            let mut finalized = raw.clone();
            if defect == "hash" {
                selected["hash"] = json!(format!("0x{}", "00".repeat(32)));
            }
            if defect == "unfinalized" {
                finalized["number"] = json!(format!("0x{:x}", number - 1));
            }
            let mut responses = vec![(
                "eth_getBlockByNumber".into(),
                json!(["finalized", false]),
                if explicit {
                    finalized
                } else {
                    selected.clone()
                },
            )];
            if explicit {
                responses.push((
                    "eth_getBlockByNumber".into(),
                    json!([format!("0x{number:x}"), false]),
                    selected,
                ));
            }
            let rpc = Fixture {
                responses: RefCell::new(responses),
            };
            let result = rpc::finalized_header(&rpc, explicit.then_some(number));
            assert_eq!(
                result.is_ok(),
                defect == "none" || (!explicit && defect == "unfinalized"),
                "{explicit}: {defect}"
            );
        }
    }
    Ok(())
}
