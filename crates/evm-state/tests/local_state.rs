use evm_state::{
    cursor::{binding, decode, encode_public},
    files::*,
    host::*,
};
use serde_json::{json, Value};

#[test]
fn cursors_match_upstream_go_vectors_in_both_directions() {
    let vectors: Value =
        serde_json::from_str(include_str!("../../../tests/fixtures/cursors.json")).unwrap();
    for step in ["1", "17"] {
        for (number, token) in vectors[step].as_object().unwrap() {
            let n: u64 = number.parse().unwrap();
            let position = decode(token.as_str().unwrap()).unwrap();
            assert_eq!(position.block.number, n);
            assert_eq!(position.block.hash, format!("0x{n:064x}"));
            assert_eq!(position.block, position.head);
            assert_eq!(position.block, position.lib);
            assert_eq!(position.step.to_string(), step);
            assert_eq!(
                encode_public(&format!("c1:{step}:{n}:{n:064x}:{n}:{n:064x}")).unwrap(),
                token.as_str().unwrap()
            );
        }
    }
}

#[test]
fn finalized_cursor_may_have_a_later_head_but_no_ambiguous_reference() {
    let h = "a".repeat(64);
    let head = "b".repeat(64);
    let valid = decode(&encode_public(&format!("c2:17:100:{h}:103:{head}")).unwrap()).unwrap();
    assert_eq!(valid.block, valid.lib);
    assert_eq!(valid.head.number, 103);
    for text in [
        "c1".into(),
        format!("c1:1:100:{h}:99:{head}"),
        format!("c1:2:100:{h}:100:{h}"),
        format!("c2:17:100:{h}:99:{head}"),
        format!("c2:17:100:{h}:100:{head}"),
        format!("c2:17:-1:{h}:100:{head}"),
        format!("c2:17:18446744073709551616:{h}:18446744073709551616:{h}"),
        format!("c3:17:100:{h}:103:{head}:99:{h}"),
        "c1:17:100:bad:100:bad".into(),
        format!("c4:17:100:{h}:100:{h}"),
    ] {
        assert!(decode(&encode_public(&text).unwrap()).is_err(), "{text}");
    }
}

#[test]
fn torn_and_corrupt_cursors_fail() {
    let vectors: Value =
        serde_json::from_str(include_str!("../../../tests/fixtures/cursors.json")).unwrap();
    let token = vectors["1"]["100"].as_str().unwrap();
    for invalid in [
        "".into(),
        "abc".into(),
        "!not-base64!".into(),
        "a".repeat(2049),
        token[1..].into(),
        token.replace('_', "/").replace('-', "+"),
    ] {
        assert!(decode(&invalid).is_err());
    }
}

#[test]
fn canonical_identity_json_preserves_python_ascii_and_sorting() {
    assert_eq!(
        canonical_json(&json!({"z":"é😀","b":{"x":true,"a":null},"a":1})).unwrap(),
        r#"{"a":1,"b":{"a":null,"x":true},"z":"\u00e9\ud83d\ude00"}"#
    );
    let one = binding(&json!({"identity":{"z":1,"a":2},"run_id":"r","database_uuid":"d"})).unwrap();
    let two = binding(&json!({"identity":{"a":2,"z":1},"database_uuid":"d","run_id":"r"})).unwrap();
    assert_eq!(one, two);
}

#[test]
fn atomic_metadata_never_overwrites_another_capture() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("proof.json");
    atomic_json(&path, &json!({"generation":1}), false).unwrap();
    assert!(atomic_json(&path, &json!({"generation":2}), false).is_err());
    assert_eq!(
        serde_json::from_slice::<Value>(&std::fs::read(&path).unwrap()).unwrap(),
        json!({"generation":1})
    );
    assert_eq!(std::fs::read_dir(temp.path()).unwrap().count(), 1);
    atomic_json(&path, &json!({"generation":3}), true).unwrap();
    assert_eq!(
        serde_json::from_slice::<Value>(&std::fs::read(&path).unwrap()).unwrap(),
        json!({"generation":3})
    );
}

#[test]
fn shared_readers_exclude_retention_and_writer_locks_release_on_drop() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("readers.lock");
    let a = file_lock(&path, false, false).unwrap();
    let b = file_lock(&path, false, false).unwrap();
    assert!(file_lock(&path, true, false).is_err());
    drop(a);
    drop(b);
    let writer = file_lock(&path, true, false).unwrap();
    assert!(file_lock(&path, true, false).is_err());
    assert!(file_lock(&path, false, false).is_err());
    drop(writer);
    file_lock(&path, true, false).unwrap();
}

#[test]
fn persistent_identity_rejects_missing_zero_and_invalid_os_values() {
    let linux = from_os_value("Linux", &"1".repeat(32)).unwrap();
    assert!(linux.starts_with("machine-sha256:"));
    assert!(!linux.contains(&"1".repeat(32)));
    assert!(
        from_os_value("Darwin", "12345678-1234-1234-1234-123456789abc")
            .unwrap()
            .starts_with("machine-sha256:")
    );
    for bad in ["", "not-a-machine-id", &"0".repeat(32)] {
        assert!(from_os_value("Linux", bad).is_err());
    }
    assert!(from_os_value("Darwin", "00000000-0000-0000-0000-000000000000").is_err());
    assert!(from_os_value("Windows", &"1".repeat(32)).is_err());
}

#[test]
fn legacy_recovery_is_bound_to_record_directory_and_machine() {
    let temp = tempfile::tempdir().unwrap();
    let dir = temp.path();
    let machine = format!("machine-sha256:{}", "1".repeat(64));
    let record = json!({"host":"old-name","database":"example"});
    assert!(!matches_for(&record, dir, &machine, "new-name").unwrap());
    let recovery = recovery_record_for(&record, dir, &machine).unwrap();
    atomic_json(&dir.join("host-rebinding.json"), &recovery, false).unwrap();
    assert!(matches_for(&record, dir, &machine, "new-name").unwrap());
    assert!(!matches_for(
        &json!({"host":"old-name","database":"different"}),
        dir,
        &machine,
        "new-name"
    )
    .unwrap());
    let copy = dir.join("copy");
    std::fs::create_dir(&copy).unwrap();
    atomic_json(&copy.join("host-rebinding.json"), &recovery, false).unwrap();
    assert!(!matches_for(&record, &copy, &machine, "new-name").unwrap());
    assert!(!matches_for(
        &record,
        dir,
        &format!("machine-sha256:{}", "2".repeat(64)),
        "new-name"
    )
    .unwrap());
}
#[test]
fn path_resolution_handles_relative_dangling_symlinks_and_rejects_loops() -> anyhow::Result<()> {
    use std::os::unix::fs::symlink;
    let root = tempfile::tempdir()?;
    let root = std::fs::canonicalize(root.path())?;
    symlink("not-created", root.join("dangling"))?;
    assert_eq!(
        evm_state::files::resolve(&root.join("dangling/child/../state"))?,
        root.join("not-created/state")
    );
    symlink("loop", root.join("loop"))?;
    assert!(evm_state::files::resolve(&root.join("loop/state")).is_err());
    Ok(())
}
