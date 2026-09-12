#[path = "support/portable.rs"]
mod portable;
use anyhow::Result;
use evm_state::{export, files};
use flate2::{Compression, GzBuilder};
use serde_json::{json, Value};
use std::{
    fs::{self, File},
    io::Write,
};

#[test]
fn portable_checkpoint_verifies_offline_with_a_pinned_header() -> Result<()> {
    let root = tempfile::tempdir()?;
    let directory = root.path().join("export");
    let layout = portable::create(&directory)?;
    let result = export::verify_export(
        &directory,
        layout["checkpoint"]["header"]["hash"].as_str(),
        &root.path().join("work"),
    )?;
    assert_eq!(result["nonzero_slots"], 2);
    assert_eq!(result["state_sha256"], layout["checkpoint"]["state_sha256"]);
    assert!(export::verify_export(
        &directory,
        Some(&format!("0x{}", "f".repeat(64))),
        &root.path().join("work")
    )
    .is_err());
    Ok(())
}

#[test]
fn altered_account_metadata_cannot_be_fixed_by_rehashing_the_file() -> Result<()> {
    for (field, value) in [
        ("nonce", json!("4")),
        ("nonce", json!(3)),
        ("nonce", json!("03")),
        ("nonce", json!("18446744073709551616")),
        ("balance", json!("1")),
        ("code", json!("0x6001")),
        ("code_hash", json!(format!("0x{}", "0".repeat(64)))),
        ("exists", json!(false)),
        ("storage_root", json!(format!("0x{}", "0".repeat(64)))),
        ("nonzero_slots", json!(1)),
        ("snapshot_id", json!("00000000000000000000000000000000")),
    ] {
        let root = tempfile::tempdir()?;
        let directory = root.path().join("export");
        let mut layout = portable::create(&directory)?;
        let path = directory.join("accounts.json");
        let mut accounts: Value = serde_json::from_slice(&fs::read(&path)?)?;
        accounts[0][field] = value;
        files::atomic_json(&path, &accounts, true)?;
        portable::rehash(&directory, &mut layout["account_file"])?;
        portable::save(&directory, &layout)?;
        assert!(
            export::verify_export(&directory, None, &root.path().join("work")).is_err(),
            "{field}"
        );
    }
    Ok(())
}

#[test]
fn wrong_zero_duplicate_missing_and_foreign_storage_rows_are_rejected() -> Result<()> {
    for defect in [
        "wrong",
        "zero",
        "duplicate",
        "missing",
        "foreign",
        "extra-field",
    ] {
        let root = tempfile::tempdir()?;
        let directory = root.path().join("export");
        let mut layout = portable::create(&directory)?;
        let mut rows = portable::rows()?;
        match defect {
            "wrong" => rows[1]["value"] = json!(format!("0x{:064x}", 99)),
            "zero" => rows[1]["value"] = json!(format!("0x{:064x}", 0)),
            "duplicate" => {
                rows[1] = rows[0].clone();
                layout["storage_pages"][1]["first_slot"] = rows[0]["slot"].clone();
                layout["storage_pages"][1]["last_slot"] = rows[0]["slot"].clone();
            }
            "foreign" => rows[1]["address"] = json!("0x2222222222222222222222222222222222222222"),
            "extra-field" => rows[1]["unexpected"] = json!(true),
            _ => {
                layout["storage_pages"].as_array_mut().unwrap().pop();
            }
        }
        if defect != "missing" {
            portable::write_page(
                &directory.join("storage-000001.jsonl.gz"),
                &[rows[1].clone()],
            )?;
            portable::rehash(&directory, &mut layout["storage_pages"][1])?;
        }
        portable::save(&directory, &layout)?;
        assert!(
            export::verify_export(&directory, None, &root.path().join("work")).is_err(),
            "{defect}"
        );
    }
    Ok(())
}

#[test]
fn compression_crc_and_truncated_or_oversized_rows_fail_after_file_rehash() -> Result<()> {
    for defect in ["crc", "truncated-json", "oversized"] {
        let root = tempfile::tempdir()?;
        let directory = root.path().join("export");
        let mut layout = portable::create(&directory)?;
        let path = directory.join("storage-000000.jsonl.gz");
        if defect == "crc" {
            let mut bytes = fs::read(&path)?;
            let end = bytes.len();
            bytes[end - 8] ^= 1;
            fs::write(&path, bytes)?;
        } else {
            let mut writer = GzBuilder::new()
                .mtime(0)
                .write(File::create(&path)?, Compression::best());
            if defect == "oversized" {
                writer.write_all(&vec![b'a'; 1025])?;
            } else {
                writer.write_all(files::canonical_json(&portable::rows()?[0])?.as_bytes())?;
            }
            writer.finish()?;
        }
        portable::rehash(&directory, &mut layout["storage_pages"][0])?;
        portable::save(&directory, &layout)?;
        assert!(
            export::verify_export(&directory, None, &root.path().join("work")).is_err(),
            "{defect}"
        );
    }
    Ok(())
}

#[test]
fn gzip_members_are_all_read_and_bound_to_the_declared_page_count() -> Result<()> {
    let root = tempfile::tempdir()?;
    let directory = root.path().join("export");
    let mut layout = portable::create(&directory)?;
    let path = directory.join("storage-000000.jsonl.gz");
    let mut bytes = fs::read(&path)?;
    bytes.extend(fs::read(directory.join("storage-000001.jsonl.gz"))?);
    fs::write(&path, bytes)?;
    layout["storage_pages"][0]["rows"] = json!(2);
    layout["storage_pages"][0]["last_slot"] = layout["storage_pages"][1]["last_slot"].clone();
    layout["storage_pages"].as_array_mut().unwrap().pop();
    portable::rehash(&directory, &mut layout["storage_pages"][0])?;
    portable::save(&directory, &layout)?;
    assert_eq!(
        export::verify_export(&directory, None, &root.path().join("work"))?["nonzero_slots"],
        2
    );
    layout["storage_pages"][0]["rows"] = json!(1);
    portable::save(&directory, &layout)?;
    assert!(export::verify_export(&directory, None, &root.path().join("work")).is_err());
    Ok(())
}

#[test]
fn missing_manifests_path_traversal_symlinks_and_oversized_metadata_fail() -> Result<()> {
    for defect in [
        "missing",
        "traversal",
        "symlink",
        "oversized",
        "sequence",
        "header",
        "coverage",
    ] {
        let root = tempfile::tempdir()?;
        let directory = root.path().join("export");
        let mut layout = portable::create(&directory)?;
        match defect {
            "traversal" => layout["account_file"]["file"] = json!("../accounts.json"),
            "sequence" => layout["storage_pages"].as_array_mut().unwrap().swap(0, 1),
            "header" => layout["checkpoint"]["header"]["number"] = json!(99),
            "coverage" => layout["checkpoint"]["accounts"] = json!([]),
            "symlink" => {
                fs::rename(
                    directory.join("accounts.json"),
                    root.path().join("accounts.json"),
                )?;
                std::os::unix::fs::symlink(
                    root.path().join("accounts.json"),
                    directory.join("accounts.json"),
                )?;
            }
            _ => {}
        }
        portable::save(&directory, &layout)?;
        if defect == "missing" {
            fs::remove_file(directory.join("manifest.json"))?;
        }
        if defect == "oversized" {
            File::create(directory.join("manifest.json"))?.set_len(64 * 1024 * 1024 + 1)?;
        }
        assert!(
            export::verify_export(&directory, None, &root.path().join("work")).is_err(),
            "{defect}"
        );
    }
    Ok(())
}
