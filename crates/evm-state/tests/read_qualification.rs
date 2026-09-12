#[path = "support/portable.rs"]
mod portable;
use anyhow::Result;
use evm_state::{
    proof,
    read_qualification::{self, ReadApi},
};
use serde_json::{json, Value};
use std::{fs, path::PathBuf};

struct Reader {
    root: tempfile::TempDir,
    published: Value,
    account: Value,
    storage: Vec<Value>,
    selected: String,
    pinned: bool,
    defect: &'static str,
}
impl Reader {
    fn new(absent: bool) -> Result<Self> {
        let root = tempfile::tempdir()?;
        let layout = portable::create(&root.path().join("export"))?;
        let mut published = layout["checkpoint"].clone();
        let mut account =
            serde_json::from_slice::<Value>(&fs::read(root.path().join("export/accounts.json"))?)?
                [0]
            .clone();
        let mut storage = portable::rows()?
            .into_iter()
            .map(|r| json!({"slot":r["slot"],"value":r["value"]}))
            .collect::<Vec<_>>();
        let mut selected = portable::A.to_owned();
        if absent {
            selected = "0x2222222222222222222222222222222222222222".into();
            let empty_root = format!("{:#x}", alloy_trie::EMPTY_ROOT_HASH);
            let code_hash = format!("{:#x}", alloy_primitives::keccak256([]));
            let mut proof = published["proof_bundle"]["accounts"][portable::A]["proof"].clone();
            proof["address"] = json!(selected);
            proof["nonce"] = json!("0x0");
            proof["balance"] = json!("0x0");
            proof["codeHash"] = json!(code_hash);
            proof["storageHash"] = json!(empty_root);
            published["accounts"] = json!([selected]);
            published["proof_bundle"]["accounts"] =
                json!({selected.clone():{"proof":proof,"code":"0x"}});
            account = json!({"address":selected,"exists":false,"nonce":"0","balance":"0","code_hash":code_hash,"code":"0x","storage_root":empty_root,"nonzero_slots":0});
            storage.clear();
        }
        account["snapshot_id"] = json!(portable::ID);
        account["header"] = published["header"].clone();
        Ok(Self {
            root,
            published,
            account,
            storage,
            selected,
            pinned: false,
            defect: "",
        })
    }
    fn measure(&mut self) -> Result<Value> {
        let output = self.root.path().join("result.json");
        let work = self.root.path().join("work");
        let selected = self.selected.clone();
        read_qualification::measure(self, portable::ID, &selected, &output, &work, 2, 1)
    }
}
impl ReadApi for Reader {
    fn database(&self) -> &str {
        "fake_sql_pages"
    }
    fn control_path(&self) -> Result<PathBuf> {
        Ok(self.root.path().join("control"))
    }
    fn pin(&mut self, snapshot: &str) -> Result<Value> {
        assert!(!self.pinned);
        self.pinned = true;
        Ok(
            json!({"pin_id":"22222222222222222222222222222222","snapshot_id":snapshot,"header":self.published["header"]}),
        )
    }
    fn manifest(&mut self, _: &str) -> Result<Value> {
        Ok(self.published.clone())
    }
    fn account(&mut self, _: &str, _: &str) -> Result<Value> {
        Ok(self.account.clone())
    }
    fn page(&mut self, pin: &str, _: &str, cursor: Option<&str>, limit: usize) -> Result<Value> {
        anyhow::ensure!(self.defect != "interrupted", "read interrupted");
        assert!(self.pinned);
        let start = cursor.map(str::parse::<usize>).transpose()?.unwrap_or(0);
        let end = (start + limit).min(self.storage.len());
        let mut response = json!({"pin_id":pin,"snapshot_id":portable::ID,"header":self.published["header"],"account":self.account,"storage":self.storage[start..end],"next_cursor":if end<self.storage.len() {json!(end.to_string())} else {Value::Null}});
        match self.defect {
            "stable_wrong_value" => {
                response["storage"][0]["value"] = json!(format!("0x{:064x}", 123))
            }
            "stable_wrong_key" if response["next_cursor"].is_null() => {
                response["storage"][0]["slot"] =
                    json!("0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff")
            }
            "duplicate" if start > 0 => response["storage"][0] = self.storage[0].clone(),
            "metadata" => response["account"]["nonce"] = json!("999"),
            "header" => response["header"]["hash"] = json!(format!("0x{:064x}", 999)),
            "empty_nonterminal" => {
                response["storage"] = json!([]);
                response["next_cursor"] = json!("more");
            }
            "short" => response["next_cursor"] = Value::Null,
            "missing_cursor" => {
                response.as_object_mut().unwrap().remove("next_cursor");
            }
            _ => {}
        }
        Ok(response)
    }
    fn unpin(&mut self, _: &str) -> Result<()> {
        self.pinned = false;
        anyhow::ensure!(self.defect != "unpin", "injected pin release failure");
        Ok(())
    }
    fn guard(&mut self, _: &[PathBuf], phase: &str) -> Result<()> {
        anyhow::ensure!(self.defect != phase, "capacity rejected");
        Ok(())
    }
}

#[test]
fn returned_storage_is_proven_for_nonempty_and_absent_accounts() -> Result<()> {
    for absent in [false, true] {
        let mut reader = Reader::new(absent)?;
        let result = reader.measure()?;
        assert_eq!(result["root_matches_captured_account_proof"], true);
        assert_eq!(
            result["verified_storage_root"],
            reader.account["storage_root"]
        );
        assert_eq!(
            result["passes"]
                .as_array()
                .unwrap()
                .iter()
                .map(|p| p["nonzero_slots"].as_u64().unwrap())
                .collect::<Vec<_>>(),
            vec![if absent { 0 } else { 2 }; 2]
        );
        assert_eq!(
            result["calls"].as_array().unwrap().len(),
            if absent { 2 } else { 4 }
        );
        assert!(!reader.pinned);
        assert!(reader.root.path().join("result.json").is_file());
    }
    Ok(())
}

#[test]
fn stable_but_wrong_pages_cannot_pass_counts_or_repeated_digests() -> Result<()> {
    for defect in [
        "stable_wrong_value",
        "stable_wrong_key",
        "duplicate",
        "metadata",
        "header",
        "empty_nonterminal",
        "interrupted",
        "short",
        "missing_cursor",
    ] {
        let mut reader = Reader::new(false)?;
        reader.defect = defect;
        assert!(reader.measure().is_err(), "{defect}");
        assert!(!reader.pinned, "{defect}");
        assert!(!reader.root.path().join("result.json").exists(), "{defect}");
    }
    Ok(())
}

#[test]
fn baseline_metadata_and_header_must_match_captured_proofs() -> Result<()> {
    for defect in [
        "nonce",
        "balance",
        "code_hash",
        "code",
        "exists",
        "storage_root",
        "header",
    ] {
        let mut reader = Reader::new(false)?;
        match defect {
            "nonce" => reader.account[defect] = json!("99"),
            "balance" => reader.account[defect] = json!("99"),
            "code" => reader.account[defect] = json!("0x00"),
            "exists" => reader.account[defect] = json!(false),
            "header" => reader.published["proof_bundle"]["header"]["number"] = json!(0),
            _ => reader.account[defect] = json!(format!("0x{:064x}", 0)),
        }
        assert!(reader.measure().is_err(), "{defect}");
        assert!(!reader.pinned);
        assert!(!reader.root.path().join("result.json").exists());
    }
    Ok(())
}

#[test]
fn capacity_and_pin_release_failures_never_publish_measurement() -> Result<()> {
    for defect in [
        "read-measurement-start",
        "read-measurement-root",
        "read-measurement-finished",
        "unpin",
    ] {
        let mut reader = Reader::new(false)?;
        reader.defect = defect;
        assert!(reader.measure().is_err(), "{defect}");
        assert!(!reader.pinned);
        assert!(!reader.root.path().join("result.json").exists());
    }
    Ok(())
}

#[test]
fn invalid_counts_and_existing_output_fail_before_pin_creation() -> Result<()> {
    for (passes, size) in [(0, 1), (11, 1), (1, 0), (1, 10001)] {
        let mut reader = Reader::new(false)?;
        let output = reader.root.path().join("result.json");
        let work = reader.root.path().join("work");
        assert!(read_qualification::measure(
            &mut reader,
            portable::ID,
            portable::A,
            &output,
            &work,
            passes,
            size
        )
        .is_err());
        assert!(!reader.pinned);
        assert!(!output.exists());
    }
    let mut reader = Reader::new(false)?;
    fs::write(reader.root.path().join("result.json"), "existing")?;
    assert!(reader.measure().is_err());
    assert!(!reader.pinned);
    Ok(())
}

#[test]
fn latency_distribution_uses_nearest_rank_and_rejects_invalid_samples() -> Result<()> {
    let d = read_qualification::distribution(&[4., 1., 3., 2.])?;
    assert_eq!(d, json!({"samples":4,"min":1.,"p50":2.,"p95":4.,"max":4.}));
    assert!(read_qualification::distribution(&[]).is_err());
    assert!(read_qualification::distribution(&[f64::NAN]).is_err());
    // Ensure the absence fixture really uses a valid exclusion proof, independent
    // of the reader's declared count or metadata.
    let reader = Reader::new(true)?;
    let b = &reader.published["proof_bundle"];
    assert!(
        !proof::verify_account(
            b["header"]["state_root"].as_str().unwrap(),
            &reader.selected,
            &b["accounts"][&reader.selected]["proof"]
        )?
        .exists
    );
    Ok(())
}
