use anyhow::Result;
use evm_state::output_qualification::OutputMeter;
use prost::Message;
use prost_types::{
    field_descriptor_proto::{Label, Type},
    DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
fn field(name: &str, number: i32, kind: Type) -> FieldDescriptorProto {
    FieldDescriptorProto {
        name: Some(name.into()),
        number: Some(number),
        r#type: Some(kind as i32),
        label: Some(Label::Optional as i32),
        ..Default::default()
    }
}
fn fixture() -> Result<(tempfile::TempDir, std::path::PathBuf)> {
    let temp = tempfile::tempdir()?;
    let path = temp.path().join("package.spkg");
    let mut storage = field("storage", 10, Type::Message);
    storage.label = Some(Label::Repeated as i32);
    storage.type_name = Some(".evm.state.v1.StorageValue".into());
    let descriptor = FileDescriptorSet {
        file: vec![FileDescriptorProto {
            name: Some("test.proto".into()),
            package: Some("evm.state.v1".into()),
            syntax: Some("proto3".into()),
            message_type: vec![
                DescriptorProto {
                    name: Some("BlockState".into()),
                    field: vec![
                        field("number", 1, Type::Uint64),
                        field("timestamp", 4, Type::Uint64),
                        storage,
                    ],
                    ..Default::default()
                },
                DescriptorProto {
                    name: Some("StorageValue".into()),
                    field: vec![
                        field("address", 1, Type::String),
                        field("slot", 2, Type::String),
                        field("value", 3, Type::String),
                        field("ordinal", 4, Type::Uint64),
                    ],
                    ..Default::default()
                },
            ],
            ..Default::default()
        }],
    };
    std::fs::write(&path, descriptor.encode_to_vec())?;
    Ok((temp, path))
}
fn row() -> Value {
    json!({"number":7,"timestamp":9,"storage.address":["a"],"storage.slot":["s"],"storage.value":["v"],"storage.ordinal":[1],"_version_":999})
}
#[test]
fn descriptor_output_matches_independent_wire_bytes_and_length_prefixed_digest() -> Result<()> {
    let (_temp, package) = fixture()?;
    let mut meter = OutputMeter::new(&package)?;
    meter.add(&row())?;
    let actual = meter.finish(1, &json!({"number":7,"timestamp":9}))?;
    // Manually encoded protobuf: number=7, timestamp=9, one 11-byte nested row.
    let wire = hex::decode("08072009520b0a01611201731a01762001")?;
    let mut digest = Sha256::new();
    digest.update((wire.len() as u64).to_be_bytes());
    digest.update(&wire);
    assert_eq!(actual["logical_protobuf_bytes"], wire.len());
    assert_eq!(
        actual["ordered_output_sha256"],
        hex::encode(digest.finalize())
    );
    assert_eq!(actual["changed_rows"]["storage"], 1);
    assert!(actual["mean_block_interval_seconds"].is_null());
    Ok(())
}
#[test]
fn malformed_nested_rows_missing_blocks_wrong_bound_and_reordered_output_fail() -> Result<()> {
    let (_temp, package) = fixture()?;
    for defect in ["array", "gap", "duplicate", "count", "end", "time"] {
        let mut meter = OutputMeter::new(&package)?;
        let mut block = row();
        if defect == "array" {
            block["storage.value"] = json!([]);
            assert!(meter.add(&block).is_err());
            continue;
        }
        meter.add(&block)?;
        if defect == "gap" || defect == "duplicate" {
            block["number"] = json!(if defect == "gap" { 9 } else { 7 });
            assert!(meter.add(&block).is_err());
            continue;
        }
        assert!(meter.finish(if defect == "count" {2} else {1}, &json!({"number":if defect == "end" {8} else {7},"timestamp":if defect == "time" {0} else {9}})).is_err(), "{defect}");
    }
    Ok(())
}
