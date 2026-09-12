//! Capture proofs against an immutable finalized block before replay begins.
use crate::{
    header::{encode_rpc_header, verify_header},
    proof::{fixed, quantity, string, unhex, verify_account},
};
use alloy_primitives::keccak256;
use anyhow::{ensure, Context, Result};
use serde_json::{json, Value};
use std::{collections::BTreeSet, env, time::Duration};

pub trait RpcCall {
    fn call(&self, method: &str, params: Value) -> Result<Value>;
}

pub struct Rpc {
    url: String,
    key: String,
    http: reqwest::blocking::Client,
}
impl Rpc {
    pub fn new(url: Option<&str>, key: Option<&str>) -> Result<Self> {
        Ok(Self {
            url: url.map(str::to_owned).unwrap_or_else(|| {
                env::var("RPC_URL").unwrap_or_else(|_| "https://bsc.rpc.pinax.network".into())
            }),
            key: key.map(str::to_owned).unwrap_or_else(|| {
                env::var("RPC_API_KEY")
                    .or_else(|_| env::var("SUBSTREAMS_API_KEY"))
                    .unwrap_or_default()
            }),
            http: reqwest::blocking::Client::builder()
                .timeout(Duration::from_secs(60))
                .redirect(reqwest::redirect::Policy::none())
                .build()?,
        })
    }
}
impl RpcCall for Rpc {
    fn call(&self, method: &str, params: Value) -> Result<Value> {
        let mut request = self
            .http
            .post(&self.url)
            .json(&json!({"jsonrpc":"2.0","id":1,"method":method,"params":params}));
        if !self.key.is_empty() {
            request = request.header("X-Api-Key", &self.key);
        }
        let response = request
            .send()
            .map_err(|_| anyhow::anyhow!("RPC {method} transport failed"))?;
        ensure!(
            response.status().is_success(),
            "RPC {method} failed (HTTP {})",
            response.status().as_u16()
        );
        let result: Value = response.json().context("invalid RPC JSON response")?;
        ensure!(
            result.get("error").is_none() && !result["result"].is_null(),
            "RPC {method} returned an error or null result"
        );
        Ok(result["result"].clone())
    }
}

pub fn capture(
    rpc: &impl RpcCall,
    accounts: impl IntoIterator<Item = impl AsRef<str>>,
    block: Option<u64>,
    expected_hash: Option<&str>,
) -> Result<Value> {
    let accounts = accounts
        .into_iter()
        .map(|a| crate::proof::address(a.as_ref()))
        .collect::<Result<BTreeSet<_>>>()?;
    ensure!(!accounts.is_empty(), "at least one account is required");
    let finalized = rpc.call("eth_getBlockByNumber", json!(["finalized", false]))?;
    let header = if let Some(block) = block {
        rpc.call(
            "eth_getBlockByNumber",
            json!([format!("0x{block:x}"), false]),
        )?
    } else {
        finalized.clone()
    };
    let number = quantity(string(&header, "number")?, 64)?.to::<u64>();
    ensure!(
        number <= quantity(string(&finalized, "number")?, 64)?.to::<u64>(),
        "checkpoint block is not finalized"
    );
    if let Some(hash) = expected_hash {
        ensure!(
            fixed::<32>(hash)? == fixed::<32>(string(&header, "hash")?)?,
            "RPC header differs from the expected block hash"
        );
    }
    let chain = rpc.call("eth_chainId", json!([]))?;
    let chain = quantity(chain.as_str().context("invalid chain ID")?, 256)?;
    let mut bundle = json!({"format_version":1,"chain_id":serde_json::from_str::<Value>(&chain.to_string())?,
        "header":{"number":number,"hash":string(&header,"hash")?.to_ascii_lowercase(),
            "parent_hash":string(&header,"parentHash")?.to_ascii_lowercase(),"state_root":string(&header,"stateRoot")?.to_ascii_lowercase(),
            "timestamp":quantity(string(&header,"timestamp")?,64)?.to::<u64>()},
        "header_trust":if expected_hash.is_some() {"operator-pinned-hash"} else {"provider-finalized-header"},
        "accounts":{},"header_rlp":encode_rpc_header(&header)?});
    verify_header(
        string(&bundle, "header_rlp")?,
        &bundle["header"],
        expected_hash,
    )?;
    for account in accounts {
        let proof = rpc.call(
            "eth_getProof",
            json!([account, [], format!("0x{number:x}")]),
        )?;
        let proven = verify_account(string(&header, "stateRoot")?, &account, &proof)?;
        let code = rpc.call("eth_getCode", json!([account, format!("0x{number:x}")]))?;
        let code = code.as_str().context("invalid RPC bytecode")?;
        ensure!(
            keccak256(unhex(code)?) == proven.code_hash,
            "RPC bytecode differs from the proven code hash"
        );
        bundle["accounts"][account] = json!({"proof":proof,"code":code.to_ascii_lowercase()});
    }
    let after = rpc.call(
        "eth_getBlockByNumber",
        json!([format!("0x{number:x}"), false]),
    )?;
    ensure!(
        string(&after, "hash")?.eq_ignore_ascii_case(string(&header, "hash")?),
        "checkpoint header changed during proof capture"
    );
    Ok(bundle)
}
