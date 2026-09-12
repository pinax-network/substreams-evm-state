//! Independent fixed roots captured from the former py-trie test oracle.
//! Rust consumes only fixture data; no Python or Go is executed.
use alloy_primitives::B256;
use alloy_trie::{HashBuilder, Nibbles};
use anyhow::Result;
use evm_state::proof::{self, StorageSort};
use serde_json::Value;
use std::io::Read;

#[test]
fn trie_branch_inline_random_and_committed_disk_boundaries_match_independent_roots() -> Result<()> {
    let mut data = Vec::new();
    flate2::read::GzDecoder::new(
        include_bytes!("../../../tests/fixtures/trie-boundaries.json.gz").as_slice(),
    )
    .read_to_end(&mut data)?;
    let fixture: Value = serde_json::from_slice(&data)?;
    let cases = fixture["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 20);
    for case in cases {
        let name = case["name"].as_str().unwrap();
        let expected: B256 = hex::decode(case["root"].as_str().unwrap())?
            .as_slice()
            .try_into()?;
        let mut builder = HashBuilder::default();
        let mut previous = None;
        for entry in case["entries"].as_array().unwrap() {
            let key: [u8; 32] = hex::decode(entry[0].as_str().unwrap())?.try_into().unwrap();
            assert!(
                previous.is_none_or(|p| key > p),
                "fixture input order: {name}"
            );
            builder.add_leaf(
                Nibbles::unpack(key),
                &hex::decode(entry[1].as_str().unwrap())?,
            );
            previous = Some(key);
        }
        assert_eq!(builder.root(), expected, "{name}");
        if let Some(slots) = case.get("slots") {
            let root = tempfile::tempdir()?;
            let path = root.path().join("storage.sqlite");
            let mut sorter = StorageSort::new(&path)?;
            for slot in slots.as_array().unwrap() {
                sorter.insert(slot[0].as_str().unwrap(), slot[1].as_str().unwrap())?;
            }
            let (actual, count) = sorter.finish()?;
            assert_eq!(actual, expected, "{name}");
            assert_eq!(count, slots.as_array().unwrap().len() as u64);
            // Check actual committed data through an independent SQLite handle.
            let connection = rusqlite::Connection::open(&path)?;
            assert_eq!(
                connection.query_row("SELECT count(*) FROM storage", [], |row| row
                    .get::<_, u64>(0))?,
                count
            );
            assert!(StorageSort::new(&path).is_err());
        }
    }
    Ok(())
}

#[test]
fn late_invalid_or_duplicate_input_poisoned_after_a_full_sort_batch() -> Result<()> {
    let word = |n: u64| format!("0x{n:064x}");
    for (slot, value) in [
        (word(10000), word(0)),
        ("0x01".into(), word(1)),
        (word(10000), "0x01".into()),
        (word(10000), format!("0x{}", "00".repeat(33))),
        (format!("0x{}", "ab".repeat(32)), word(1)),
    ] {
        let root = tempfile::tempdir()?;
        let path = root.path().join("storage.sqlite");
        let mut sorter = StorageSort::new(&path)?;
        sorter.insert(&format!("0x{}", "AB".repeat(32)), &word(1))?;
        for n in 0..4097 {
            sorter.insert(&word(n), &word(n + 1))?;
        }
        assert!(sorter.insert(&slot, &value).is_err());
        assert!(sorter.insert(&word(20000), &word(1)).is_err());
        assert!(sorter.finish().is_err());
        assert!(StorageSort::new(&path).is_err());
    }
    let root = tempfile::tempdir()?;
    let path = root.path().join("interrupted.sqlite");
    let input = [
        Ok((word(1), word(2))),
        Err(anyhow::anyhow!("source interrupted")),
    ];
    assert!(proof::storage_root(input, &path)
        .unwrap_err()
        .to_string()
        .contains("source interrupted"));
    assert!(StorageSort::new(&path).is_err());
    Ok(())
}
