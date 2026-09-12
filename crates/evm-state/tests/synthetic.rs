use alloy_primitives::{B256, U256};
use anyhow::Result;
use evm_state::{
    capacity_qualification::StressOptions,
    header::verify_header,
    proof::{self, string},
    synthetic::{self, word, Account},
};
use serde_json::{json, Value};
use std::collections::BTreeMap;
#[test]
fn generated_tries_match_independent_golden_state_and_verify_present_and_absent_accounts(
) -> Result<()> {
    let golden: Value =
        serde_json::from_str(include_str!("../../../tests/fixtures/proof-parity.json"))?;
    let f = &golden["account"];
    let account = Account {
        slots: f["slots"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| {
                Ok((
                    B256::from(proof::fixed::<32>(v[0].as_str().unwrap())?),
                    U256::from_be_bytes(proof::fixed::<32>(v[1].as_str().unwrap())?),
                ))
            })
            .collect::<Result<_>>()?,
        nonce: 3,
        balance: U256::from(1) << 200,
        code: proof::unhex(string(f, "code")?)?,
        exists: true,
    };
    let address = string(f, "address")?.to_owned();
    let mut accounts = BTreeMap::from([(address.clone(), account)]);
    let bundle = synthetic::bundle(100, &accounts, None)?;
    assert_eq!(bundle["header"]["state_root"], f["state_root"]);
    assert_eq!(
        bundle["accounts"][&address]["proof"]["storageHash"],
        f["proof"]["storageHash"]
    );
    let absent = "0x2222222222222222222222222222222222222222".to_owned();
    accounts.insert(
        absent.clone(),
        Account {
            slots: Default::default(),
            nonce: 0,
            balance: U256::ZERO,
            code: Vec::new(),
            exists: false,
        },
    );
    let bundle = synthetic::bundle(101, &accounts, Some(string(&bundle["header"], "hash")?))?;
    verify_header(string(&bundle, "header_rlp")?, &bundle["header"], None)?;
    for (address, data) in &accounts {
        let proven = proof::verify_account(
            string(&bundle["header"], "state_root")?,
            address,
            &bundle["accounts"][address]["proof"],
        )?;
        assert_eq!(proven.exists, data.exists);
        let temp = tempfile::tempdir()?;
        let rows = data
            .slots
            .iter()
            .map(|(slot, value)| Ok((format!("{slot:#x}"), format!("0x{value:064x}"))));
        assert_eq!(
            proof::verify_complete(
                &proven,
                rows,
                &format!("0x{}", hex::encode(&data.code)),
                &proven.json(),
                &temp.path().join("slots.sqlite")
            )?,
            data.slots.len() as u64
        );
    }
    accounts.get_mut(&absent).unwrap().nonce = 1;
    assert!(synthetic::bundle(102, &accounts, None).is_err());
    Ok(())
}
#[test]
fn stress_bounds_and_synthetic_rows_preserve_zero_clears_and_exact_values() -> Result<()> {
    let options = StressOptions {
        prefix: "fixture".into(),
        output: "run".into(),
        package: "package.spkg".into(),
        accounts: 64,
        hot_slots: 100000,
        quiet_slots: 64,
        merge_only_mib: 0,
        budget_bytes: 100_000_000_000,
    };
    options.validate()?;
    for defect in ["accounts", "slots", "total", "merge", "prefix", "budget"] {
        let mut bad = options.clone();
        match defect {
            "accounts" => bad.accounts = 0,
            "slots" => bad.hot_slots = 1,
            "total" => bad.quiet_slots = u64::MAX,
            "merge" => bad.merge_only_mib = 1025,
            "prefix" => bad.prefix = "bad;DROP".into(),
            "budget" => bad.budget_bytes = 0,
            _ => unreachable!(),
        };
        assert!(bad.validate().is_err(), "{defect}");
    }
    let address = "0x1111111111111111111111111111111111111111".to_owned();
    let accounts = BTreeMap::from([(
        address.clone(),
        Account {
            nonce: u64::MAX,
            balance: U256::from(1) << 240,
            ..Default::default()
        },
    )]);
    let bundle = synthetic::bundle(100, &accounts, None)?;
    let row = synthetic::block(&bundle, &[(address, word(1), U256::ZERO)], true)?;
    assert_eq!(
        row["storage.value"],
        json!([format!("0x{}", "00".repeat(32))])
    );
    assert_eq!(row["nonces.value"], json!([u64::MAX]));
    assert_eq!(
        row["balances.value"],
        json!([(U256::from(1) << 240usize).to_string()])
    );
    Ok(())
}
