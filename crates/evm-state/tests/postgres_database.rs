#[path = "support/portable.rs"]
mod portable;
use anyhow::Result;
use evm_state::{
    control::new_id,
    postgres::{self, Postgres},
};
use serde_json::{json, Value};
use std::{collections::BTreeSet, fs};

struct Database {
    pg: Postgres,
    schema: String,
    root: tempfile::TempDir,
    bundle: Value,
}
impl Database {
    fn new() -> Result<Self> {
        let schema = format!("evm_test_rust_{}", new_id());
        let root = tempfile::tempdir()?;
        let layout = portable::create(&root.path().join("input"))?;
        Postgres::new(None, None).query(&format!("CREATE SCHEMA {schema}"))?;
        let db = Self {
            pg: Postgres::new(None, Some(&format!("-c search_path={schema}"))),
            schema,
            root,
            bundle: layout["checkpoint"]["proof_bundle"].clone(),
        };
        db.pg.query(&format!(
            "{}\n{}",
            include_str!("../../../postgres/schema.0.blocks.sql"),
            include_str!("../../../postgres/schema.1.state.sql")
        ))?;
        let account = serde_json::from_slice::<Value>(&fs::read(
            db.root.path().join("input/accounts.json"),
        )?)?[0]
            .clone();
        let number = db.bundle["header"]["number"].as_u64().unwrap();
        let a = portable::A;
        db.pg.query(&format!("INSERT INTO blocks(block_num,block_hash,parent_hash,timestamp,state_root,coinbase,transaction_count) VALUES ({number},'{}','{}',now(),'{}','{a}',1); INSERT INTO code VALUES ('{}',decode('{}','hex'),{},{}); INSERT INTO accounts VALUES ('{a}',{}, {},'{}',{number},{number},{number},{number});",
            db.bundle["header"]["hash"].as_str().unwrap(),db.bundle["header"]["parent_hash"].as_str().unwrap(),db.bundle["header"]["state_root"].as_str().unwrap(),account["code_hash"].as_str().unwrap(),account["code"].as_str().unwrap().trim_start_matches("0x"),(account["code"].as_str().unwrap().len()-2)/2,number,account["balance"].as_str().unwrap(),account["nonce"].as_str().unwrap(),account["code_hash"].as_str().unwrap()))?;
        for row in portable::rows()? {
            db.pg.query(&format!(
                "INSERT INTO storage VALUES ('{a}','{}','{}',{number},1)",
                row["slot"].as_str().unwrap(),
                row["value"].as_str().unwrap()
            ))?;
        }
        Ok(db)
    }
}
impl Drop for Database {
    fn drop(&mut self) {
        let _ = Postgres::new(None, None).query(&format!("DROP SCHEMA {} CASCADE", self.schema));
    }
}

#[test]
#[ignore = "requires PostgreSQL; creates a unique owned schema"]
fn actual_postgres_snapshot_proves_full_state_and_rejects_changed_nonce() -> Result<()> {
    let db = Database::new()?;
    let captured = db.pg.snapshot(Some(portable::A), None)?;
    assert_eq!(
        postgres::verify(&captured, &db.bundle, true, None, None, db.root.path())?
            ["storage_slots_checked"],
        2
    );
    db.pg.query("UPDATE accounts SET nonce=99")?;
    assert!(postgres::verify(
        &db.pg.snapshot(Some(portable::A), None)?,
        &db.bundle,
        true,
        None,
        None,
        db.root.path()
    )
    .unwrap_err()
    .to_string()
    .contains("nonce/balance mismatch"));
    Ok(())
}

#[test]
#[ignore = "requires PostgreSQL; creates a unique owned schema"]
fn single_statement_snapshot_never_mixes_concurrently_committed_blocks() -> Result<()> {
    let db = Database::new()?;
    db.pg.query(&format!("UPDATE blocks SET block_num=100; UPDATE accounts SET balance=100,block_num=100; UPDATE storage SET value='0x{:064x}',block_num=100",100))?;
    let mut sql = String::new();
    for n in 101..201 {
        sql.push_str(&format!("BEGIN; UPDATE blocks SET block_num={n},block_hash='0x{n:064x}'; UPDATE accounts SET balance={n},block_num={n}; UPDATE storage SET value='0x{n:064x}',block_num={n}; COMMIT; SELECT pg_sleep(0.01);\n"));
    }
    let options = format!("-c search_path={}", db.schema);
    let writer = std::thread::spawn(move || Postgres::new(None, Some(&options)).query(&sql));
    let checked = (|| -> Result<()> {
        let mut seen = BTreeSet::new();
        for _ in 0..20 {
            let captured = db.pg.snapshot(Some(portable::A), None)?;
            let number = captured["header"]["number"].as_u64().unwrap();
            seen.insert(number);
            anyhow::ensure!(
                captured["accounts"][0]["balance"] == json!(number.to_string()),
                "account and block marker use different snapshots"
            );
            anyhow::ensure!(
                captured["storage"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .all(|row| row["value"] == format!("0x{number:064x}")),
                "storage and block marker use different snapshots"
            );
        }
        anyhow::ensure!(
            seen.len() > 1,
            "concurrent writer made no observed progress"
        );
        Ok(())
    })();
    let written = writer
        .join()
        .map_err(|_| anyhow::anyhow!("writer thread failed"))?;
    checked?;
    written?;
    Ok(())
}
