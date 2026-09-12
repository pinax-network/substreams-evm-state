use alloy_primitives::U256;
use anyhow::Result;
use evm_state::{
    bootstrap,
    ch::{params, uint, ClickHouse},
    checkpoint,
    control::new_id,
    export, importer,
    ingest::{self, NativeOptions},
    proof::string,
    synthetic::{self, word, Account},
};
use serde_json::{json, Value};
use std::{cell::RefCell, collections::BTreeMap, path::PathBuf};
const A: &str = "0x1111111111111111111111111111111111111111";
const B: &str = "0x2222222222222222222222222222222222222222";
const BUDGET: u64 = 100_000_000_000;
struct Fixture {
    root: tempfile::TempDir,
    target: ClickHouse,
    databases: RefCell<Vec<String>>,
}
impl Fixture {
    fn new() -> Result<Self> {
        let root = tempfile::tempdir()?;
        let target = ClickHouse::new(&format!("evm_test_rust_{}", new_id()))?
            .with_control_home(root.path().join("control"));
        Ok(Self {
            root,
            target: target.clone(),
            databases: RefCell::new(vec![target.database]),
        })
    }
    fn source(
        &self,
        accounts: &[&str],
        start: u64,
        rows: Vec<Value>,
        compact: bool,
    ) -> Result<Value> {
        let client = self
            .target
            .with_database(&format!("evm_test_rust_{}", new_id()))?;
        self.databases.borrow_mut().push(client.database.clone());
        let directory = self.root.path().join(&client.database);
        let options = NativeOptions {
            package: PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../spkg/evm-state-v0.1.0.spkg"),
            endpoint: "http://127.0.0.1:1".into(),
            accounts: json!(accounts),
            start_block: start,
            state_dir: directory.clone(),
            dsn: format!(
                "clickhouse://evm_state:local-development-only@localhost:19000/{}",
                client.database
            ),
            checkpoint_database: Some(self.target.database.clone()),
        };
        let run = ingest::prepare(&client, &options)?;
        synthetic::insert(&client, &directory, rows)?;
        if compact {
            bootstrap::compact(&client, &directory, None, BUDGET)?;
        }
        Ok(
            json!({"database":client.database,"accounts":accounts,"start_block":start,"module_hash":run["identity"]["module_hash"],"final_blocks_only":true}),
        )
    }
    fn publish(&self, bundle: &Value, sources: &[Value], base: Option<&str>) -> Result<Value> {
        checkpoint::build(
            &self.target,
            bundle,
            sources,
            base,
            BUDGET,
            &self.root.path().join("work"),
        )
    }
    fn storage(&self, id: &str, account: &str) -> Result<Vec<Value>> {
        self.target.rows("SELECT slot,value FROM checkpoint_storage FINAL WHERE snapshot_id={id:String} AND address={a:String} ORDER BY slot", &params(json!({"id":id,"a":account}))?)?.collect()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        if let Ok(admin) = self.target.with_database("default") {
            for name in self.databases.borrow().iter() {
                let _ = admin.execute(
                    &format!("DROP DATABASE IF EXISTS {name} SYNC"),
                    &Default::default(),
                );
            }
        }
    }
}
fn selected(bundle: &Value, accounts: &[&str]) -> Value {
    let mut value = bundle.clone();
    value["accounts"]
        .as_object_mut()
        .unwrap()
        .retain(|a, _| accounts.contains(&a.as_str()));
    value
}
fn row(
    bundle: &Value,
    accounts: &[&str],
    patches: &[(String, alloy_primitives::B256, U256)],
    metadata: bool,
) -> Result<Value> {
    synthetic::block(&selected(bundle, accounts), patches, metadata)
}
fn account(slots: &[(u64, u64)]) -> Account {
    Account {
        slots: slots
            .iter()
            .map(|(s, v)| (word(*s), U256::from(*v)))
            .collect(),
        ..Default::default()
    }
}

#[test]
#[ignore = "requires ClickHouse and pinned substreams CLI"]
fn compacted_nonempty_cohort_joins_restored_base_and_continues_without_losing_quiet_state(
) -> Result<()> {
    let db = Fixture::new()?;
    let mut a = account(&[(1, 7), (2, 8)]);
    a.nonce = (1u64 << 63) + 9;
    let mut accounts = BTreeMap::from([(A.into(), a.clone()), (B.into(), account(&[(7, 11)]))]);
    let b100 = synthetic::bundle(100, &accounts, None)?;
    let b101 = synthetic::bundle(101, &accounts, Some(string(&b100["header"], "hash")?))?;
    let origin = db.source(
        &[A],
        100,
        vec![
            row(
                &b100,
                &[A],
                &[
                    (A.into(), word(1), U256::from(7)),
                    (A.into(), word(2), U256::from(8)),
                ],
                true,
            )?,
            row(&b101, &[A], &[], false)?,
        ],
        false,
    )?;
    let initial = db.publish(&selected(&b101, &[A]), &[origin], None)?;
    let initial_id = string(&initial, "snapshot_id")?;
    let old_storage = db.storage(initial_id, A)?;
    let directory = db.root.path().join("export");
    export::export_checkpoint(
        &db.target,
        initial_id,
        &directory,
        1,
        &db.root.path().join("work"),
    )?;
    let restored = importer::import_checkpoint(
        &db.target,
        &directory,
        None,
        &db.root.path().join("work"),
        BUDGET,
    )?;
    assert_ne!(restored["snapshot_id"], initial["snapshot_id"]);
    assert_eq!(restored["state_sha256"], initial["state_sha256"]);
    assert_eq!(
        uint(
            &checkpoint::read_account(&db.target, string(&restored, "snapshot_id")?, A)?["nonce"]
        )?,
        a.nonce
    );

    a.slots = BTreeMap::from([(word(2), U256::from(9))]);
    a.balance = U256::from(50);
    accounts.insert(A.into(), a.clone());
    let b102 = synthetic::bundle(102, &accounts, Some(string(&b101["header"], "hash")?))?;
    let b103 = synthetic::bundle(103, &accounts, Some(string(&b102["header"], "hash")?))?;
    let mut updated = row(
        &b102,
        &[A],
        &[
            (A.into(), word(1), U256::ZERO),
            (A.into(), word(2), U256::from(9)),
        ],
        false,
    )?;
    updated["balances.address"] = json!([A]);
    updated["balances.value"] = json!(["50"]);
    updated["balances.ordinal"] = json!([10]);
    let old = db.source(
        &[A],
        102,
        vec![updated, row(&b103, &[A], &[], false)?],
        false,
    )?;
    let new = db.source(
        &[B],
        100,
        vec![
            row(&b100, &[B], &[(B.into(), word(7), U256::from(11))], true)?,
            row(&b101, &[B], &[], false)?,
            row(&b102, &[B], &[], false)?,
            row(&b103, &[B], &[], false)?,
        ],
        true,
    )?;
    let sources = [old, new];
    let ready = db.publish(&b103, &sources, restored["snapshot_id"].as_str())?;
    let ready_id = string(&ready, "snapshot_id")?;
    assert_eq!(
        db.storage(ready_id, A)?,
        vec![json!({"slot":format!("{:#x}",word(2)),"value":format!("0x{:064x}",9)})]
    );
    assert_eq!(
        db.storage(ready_id, B)?,
        vec![json!({"slot":format!("{:#x}",word(7)),"value":format!("0x{:064x}",11)})]
    );
    assert_eq!(db.storage(initial_id, A)?, old_storage);
    let metadata = checkpoint::read_account(&db.target, ready_id, A)?;
    assert_eq!(uint(&metadata["nonce"])?, a.nonce);
    assert_eq!(metadata["balance"], "50");
    assert_eq!(metadata["code"], "0x60006000");
    assert!(checkpoint::read_account(&db.target, initial_id, B).is_err());
    let retry = db.publish(&b103, &sources, restored["snapshot_id"].as_str())?;
    assert_ne!(retry["snapshot_id"], ready["snapshot_id"]);
    assert_eq!(retry["state_sha256"], ready["state_sha256"]);
    a.slots.clear();
    accounts.insert(A.into(), a);
    let b104 = synthetic::bundle(104, &accounts, Some(string(&b103["header"], "hash")?))?;
    let combined = db.source(
        &[A, B],
        104,
        vec![row(
            &b104,
            &[A, B],
            &[(A.into(), word(2), U256::ZERO)],
            false,
        )?],
        false,
    )?;
    let final_state = db.publish(&b104, &[combined], Some(ready_id))?;
    assert_eq!(final_state["nonzero_slots"], 1);
    assert_eq!(
        db.storage(string(&final_state, "snapshot_id")?, B)?,
        db.storage(ready_id, B)?
    );
    Ok(())
}

#[test]
#[ignore = "requires ClickHouse and pinned substreams CLI"]
fn proven_deletion_recreation_and_diagnostic_lifecycle_signals_preserve_exact_storage() -> Result<()>
{
    for mode in [
        "zero-clear",
        "deletion",
        "same-block",
        "next-block",
        "selfdestruct",
        "code_cleared",
        "nonce_reset",
    ] {
        let db = Fixture::new()?;
        let initial_account = account(&[(1, 7), (2, 8)]);
        let b100 = synthetic::bundle(
            100,
            &BTreeMap::from([(A.into(), initial_account.clone())]),
            None,
        )?;
        let source = db.source(
            &[A],
            100,
            vec![row(
                &b100,
                &[A],
                &[
                    (A.into(), word(1), U256::from(7)),
                    (A.into(), word(2), U256::from(8)),
                ],
                true,
            )?],
            false,
        )?;
        let base = db.publish(&b100, &[source], None)?;
        let base_id = string(&base, "snapshot_id")?;
        let original = db.storage(base_id, A)?;
        let absent = Account {
            slots: BTreeMap::new(),
            nonce: 0,
            balance: U256::ZERO,
            code: vec![],
            exists: false,
        };
        let recreated = Account {
            slots: BTreeMap::from([(word(3), U256::from(11))]),
            balance: U256::ZERO,
            code: vec![0x60, 1],
            ..Default::default()
        };
        let diagnostic = !matches!(
            mode,
            "zero-clear" | "deletion" | "same-block" | "next-block"
        );
        let final_account = if diagnostic {
            Account {
                nonce: 2,
                code: vec![],
                ..initial_account
            }
        } else if mode.ends_with("block") {
            recreated.clone()
        } else {
            absent.clone()
        };
        let first_account = if mode == "next-block" {
            absent
        } else {
            final_account.clone()
        };
        let b101 = synthetic::bundle(
            101,
            &BTreeMap::from([(A.into(), first_account)]),
            Some(string(&b100["header"], "hash")?),
        )?;
        let latest = synthetic::bundle(
            102,
            &BTreeMap::from([(A.into(), final_account.clone())]),
            Some(string(&b101["header"], "hash")?),
        )?;
        let patches = if mode == "zero-clear" {
            vec![
                (A.into(), word(1), U256::ZERO),
                (A.into(), word(2), U256::ZERO),
            ]
        } else if diagnostic {
            vec![]
        } else {
            vec![(A.into(), word(1), U256::from(99))]
        };
        let mut first = row(&b101, &[A], &patches, true)?;
        if mode != "zero-clear" {
            first["lifecycle.address"] = json!([A]);
            first["lifecycle.kind"] = json!([if diagnostic { mode } else { "storage_reset" }]);
            first["lifecycle.ordinal"] = json!([5]);
        }
        if mode == "same-block" {
            for (key, value) in [
                ("storage.address", json!(A)),
                ("storage.slot", json!(format!("{:#x}", word(3)))),
                ("storage.value", json!(format!("0x{:064x}", 11))),
                ("storage.ordinal", json!(6)),
            ] {
                first[key].as_array_mut().unwrap().push(value);
            }
        }
        let second = row(
            &latest,
            &[A],
            &if mode == "next-block" {
                vec![(A.into(), word(3), U256::from(11))]
            } else {
                vec![]
            },
            mode == "next-block",
        )?;
        let source = db.source(&[A], 101, vec![first, second], false)?;
        let ready = db.publish(&latest, &[source], Some(base_id))?;
        let id = string(&ready, "snapshot_id")?;
        assert_eq!(
            ready["nonzero_slots"],
            final_account.slots.len() as u64,
            "{mode}"
        );
        let metadata = checkpoint::read_account(&db.target, id, A)?;
        assert_eq!(metadata["exists"], final_account.exists);
        assert_eq!(uint(&metadata["nonce"])?, final_account.nonce);
        assert_eq!(
            metadata["code"],
            format!("0x{}", hex::encode(final_account.code))
        );
        assert_eq!(metadata["balance"], final_account.balance.to_string());
        assert_eq!(db.storage(base_id, A)?, original, "{mode}");
        if diagnostic {
            assert_eq!(db.storage(id, A)?, original, "{mode}");
        }
    }
    Ok(())
}
