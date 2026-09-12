//! Reconstruct logical protobuf output from an owned, complete native interval.
//! These bytes are neither network traffic nor a provider invoice.
use crate::{
    ch::{params, uint, ClickHouse},
    checkpoint, cursor, files,
    header::{encode_rpc_header, verify_header},
    native_stream::wire_block,
    proof::{quantity, string},
    rpc::{self, RpcCall},
    source::verified_source,
};
use anyhow::{ensure, Context, Result};
use prost::Message;
use prost_reflect::{DescriptorPool, DynamicMessage, Kind, MessageDescriptor};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, fs, path::Path};

pub struct OutputMeter {
    descriptor: MessageDescriptor,
    groups: BTreeMap<String, u64>,
    digest: Sha256,
    count: u64,
    total: u64,
    maximum: u64,
    first_timestamp: Option<u64>,
    last_number: Option<u64>,
}
impl OutputMeter {
    pub fn new(package: &Path) -> Result<Self> {
        let descriptors = prost_types::FileDescriptorSet::decode(fs::read(package)?.as_slice())?;
        let pool = DescriptorPool::from_file_descriptor_set(descriptors)?;
        let descriptor = pool
            .get_message_by_name("evm.state.v1.BlockState")
            .context("package has no BlockState schema")?;
        let groups = descriptor
            .fields()
            .filter(|field| matches!(field.kind(), Kind::Message(_)))
            .map(|field| (field.name().to_owned(), 0))
            .collect();
        Ok(Self {
            descriptor,
            groups,
            digest: Sha256::new(),
            count: 0,
            total: 0,
            maximum: 0,
            first_timestamp: None,
            last_number: None,
        })
    }
    pub fn add(&mut self, row: &Value) -> Result<()> {
        let number = uint(&row["number"])?;
        ensure!(
            self.last_number
                .is_none_or(|last| last.checked_add(1) == Some(number)),
            "output blocks must be consecutive and ordered"
        );
        let wire = wire_block(row)?;
        let message = DynamicMessage::deserialize(self.descriptor.clone(), &wire)?;
        // BlockState has no map fields. prost-reflect emits fields in numeric
        // order and retains repeated order, matching the independent protobuf
        // deterministic encoding used by the original measurement.
        let encoded = message.encode_to_vec();
        for (name, count) in &mut self.groups {
            if let Some(value) = wire.get(name) {
                *count = count
                    .checked_add(
                        value
                            .as_array()
                            .context("invalid nested output array")?
                            .len() as u64,
                    )
                    .context("output row count overflow")?;
            }
        }
        let size = encoded.len() as u64;
        self.digest.update(size.to_be_bytes());
        self.digest.update(encoded);
        self.count = self
            .count
            .checked_add(1)
            .context("output block count overflow")?;
        self.total = self
            .total
            .checked_add(size)
            .context("output byte count overflow")?;
        self.maximum = self.maximum.max(size);
        self.first_timestamp.get_or_insert(uint(&row["timestamp"])?);
        self.last_number = Some(number);
        Ok(())
    }
    pub fn finish(self, expected: u64, header: &Value) -> Result<Value> {
        ensure!(
            self.count > 0
                && self.count == expected
                && self.last_number == Some(uint(&header["number"])?),
            "native interval changed during output measurement"
        );
        let first = self.first_timestamp.unwrap();
        let elapsed = uint(&header["timestamp"])?
            .checked_sub(first)
            .context("block timestamps regressed")?;
        Ok(
            json!({"blocks":self.count,"logical_protobuf_bytes":self.total,
            "mean_protobuf_bytes_per_block":self.total as f64 / self.count as f64,
            "max_protobuf_bytes_per_block":self.maximum,"changed_rows":self.groups,
            "ordered_output_sha256":hex::encode(self.digest.finalize()),
            "digest_encoding":"ascending blocks; deterministic protobuf; each prefixed by its uint64 big-endian byte length",
            "first_block_timestamp":first,"mean_block_interval_seconds":if self.count > 1 {Some(elapsed as f64 / (self.count-1) as f64)} else {None}}),
        )
    }
}

pub fn measure(
    client: &ClickHouse,
    rpc: &impl RpcCall,
    directory: &Path,
    output: &Path,
) -> Result<Value> {
    ensure!(!output.try_exists()?, "output already exists");
    let directory = files::resolve(directory)?;
    // Keep ingestion and compaction out while measuring the same deduplicated
    // rows, cursor and frozen package. The shared source reader excludes prune.
    let _writer = files::file_lock(&directory.join("run.lock"), true, false)?;
    let run: Value = serde_json::from_slice(&fs::read(directory.join("run.json"))?)?;
    let end =
        uint(&cursor::load_progress(client, &run, &directory)?["position"]["block"]["number"])?;
    let chain = rpc.call("eth_chainId", json!([]))?;
    ensure!(
        quantity(chain.as_str().context("invalid chain ID")?, 256)?
            == alloy_primitives::U256::from(56),
        "expected the BSC RPC"
    );
    let raw = rpc::finalized_header(rpc, Some(end))?;
    let header = json!({"number":end,"hash":string(&raw,"hash")?.to_ascii_lowercase(),
        "state_root":string(&raw,"stateRoot")?.to_ascii_lowercase(),"parent_hash":string(&raw,"parentHash")?.to_ascii_lowercase(),
        "timestamp":quantity(string(&raw,"timestamp")?,64)?.to::<u64>()});
    let encoded = encode_rpc_header(&raw)?;
    verify_header(&encoded, &header, None)?;
    let mut declared = json!({});
    for key in [
        "database",
        "accounts",
        "start_block",
        "module_hash",
        "final_blocks_only",
    ] {
        declared[key] = run["identity"][key].clone();
    }
    let checked = verified_source(client, &declared, None, Some(end))?;
    ensure!(
        checked.source["state_directory"] == directory.to_str().context("invalid directory")?,
        "measurement directory does not own the native source"
    );
    let expected = checkpoint::validate_interval(client, &checked.source, &header)?;
    let mut meter = OutputMeter::new(&directory.join("package.spkg"))?;
    for row in client.rows("SELECT * FROM state_blocks FINAL WHERE number >= {start:UInt64} AND number <= {end:UInt64} ORDER BY number", &params(json!({"start":declared["start_block"],"end":end}))?)? {
        meter.add(&row?)?;
    }
    let mut result = meter.finish(expected, &header)?;
    let metadata = json!({"format_version":1,"database":client.database,"run_id":run["run_id"],
        "module_hash":run["identity"]["module_hash"],"package_sha256":run["identity"]["package_sha256"],
        "accounts":declared["accounts"],"start_block":declared["start_block"],"end_block":end,
        "header":header,"header_rlp":encoded,"header_trust":"provider-finalized-header",
        "validation":"encoded header, native interval and durable cursor verified",
        "limitations":["account storage completeness is not established by this update sample",
            "logical protobuf output reconstructed from deduplicated native rows; excludes framing, retries and billing adjustments",
            "cache warmth and wall time are separate measurements"]});
    result
        .as_object_mut()
        .unwrap()
        .extend(metadata.as_object().unwrap().clone());
    files::atomic_json(output, &result, false)?;
    Ok(result)
}
