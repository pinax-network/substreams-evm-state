use anyhow::Result;
use evm_state::native_stream::{decode_frame, wire_block};
use serde_json::json;
use std::io::Write;

fn framed(flag: u8, bytes: &[u8]) -> Vec<u8> {
    let mut result = vec![flag];
    result.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
    result.extend_from_slice(bytes);
    result
}

#[test]
fn s2_and_uncompressed_grpc_frames_decode_exact_payloads() -> Result<()> {
    let payload = "repeatable payload ".repeat(10000).into_bytes();
    let mut compressed = Vec::new();
    {
        let mut writer = minlz::s2::Writer::new(&mut compressed);
        writer.write_all(&payload)?;
        writer.flush()?;
    }
    assert_eq!(decode_frame(&framed(1, &compressed), "s2")?, payload);
    assert_eq!(decode_frame(&framed(0, &payload), "identity")?, payload);
    Ok(())
}
#[test]
fn corrupt_truncated_extra_and_unknown_compressed_frames_are_rejected() -> Result<()> {
    for bytes in [
        vec![],
        vec![0; 4],
        vec![0, 0, 0, 0, 2, 1],
        vec![0, 0, 0, 0, 0, 1],
        vec![2, 0, 0, 0, 0],
        vec![0, 0xff, 0xff, 0xff, 0xff],
        framed(1, b"not s2"),
    ] {
        assert!(decode_frame(&bytes, "s2").is_err());
    }
    assert!(decode_frame(&framed(1, b""), "gzip").is_err());
    let mut compressed = Vec::new();
    {
        let mut writer = minlz::s2::Writer::new(&mut compressed);
        writer.write_all(b"payload")?;
        writer.flush()?;
    }
    compressed.pop();
    assert!(decode_frame(&framed(1, &compressed), "s2").is_err());
    Ok(())
}
#[test]
fn flattened_sql_fixtures_preserve_protobuf_nested_array_alignment() -> Result<()> {
    let mut row = json!({"number":100,"_version_":1,"storage.address":["a"],"storage.slot":["s"],"storage.value":["v"],"storage.ordinal":[4]});
    assert_eq!(
        wire_block(&row)?,
        json!({"number":100,"storage":[{"address":"a","slot":"s","value":"v","ordinal":4}]})
    );
    row["storage.value"] = json!([]);
    assert!(wire_block(&row).is_err());
    Ok(())
}
