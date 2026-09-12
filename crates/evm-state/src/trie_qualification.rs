//! Resource parity against an independently recorded, immutable private prefix.
use crate::{
    bootstrap, capacity,
    ch::{params, uint, ClickHouse},
    files::{atomic_json, canonical_json, resolve},
    proof::{string, StorageSort},
    reader::now_ns,
};
use anyhow::{ensure, Context, Result};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File},
    io::{Read, Write},
    os::unix::fs::MetadataExt,
    path::Path,
    time::Instant,
};

pub fn file_hash(path: &Path) -> Result<String> {
    let mut file = File::open(path)?;
    let mut buffer = [0; 1048576];
    let mut digest = Sha256::new();
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        digest.update(&buffer[..count]);
    }
    Ok(hex::encode(digest.finalize()))
}

pub fn process_usage() -> Result<(f64, u64)> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: getrusage initializes this struct on success, checked before read.
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) } != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    let usage = unsafe { usage.assume_init() };
    let cpu = usage.ru_utime.tv_sec as f64
        + usage.ru_utime.tv_usec as f64 / 1_000_000.0
        + usage.ru_stime.tv_sec as f64
        + usage.ru_stime.tv_usec as f64 / 1_000_000.0;
    let rss = u64::try_from(usage.ru_maxrss).context("invalid process RSS counter")?;
    Ok((
        cpu,
        if cfg!(target_os = "macos") {
            rss
        } else {
            rss.checked_mul(1024).context("RSS counter overflow")?
        },
    ))
}

struct Progress<'a> {
    client: &'a ClickHouse,
    out: &'a Path,
    path: &'a Path,
    log: File,
    started: Instant,
    cpu_started: f64,
}
impl Progress<'_> {
    fn point(&mut self, stage: &str, count: u64) -> Result<Value> {
        let metadata = fs::metadata(self.path)?;
        let (cpu, rss) = process_usage()?;
        let value = json!({"observed_ns":now_ns()?,"stage":stage,"slots":count,"elapsed_seconds":self.started.elapsed().as_secs_f64(),
            "process_cpu_seconds":cpu-self.cpu_started,"process_peak_rss_bytes":rss,"workspace_file_bytes":metadata.len(),"workspace_allocated_bytes":metadata.blocks()*512});
        writeln!(self.log, "{}", canonical_json(&value)?)?;
        self.log.flush()?;
        self.log.sync_all()?;
        capacity::check(self.client, &[self.out.to_path_buf()], stage)?;
        println!(
            "{}",
            json!({"slots":count,"elapsed_seconds":value["elapsed_seconds"],"process_peak_rss_bytes":rss,"workspace_allocated_bytes":value["workspace_allocated_bytes"]})
        );
        Ok(value)
    }
}

pub fn measure(
    evidence_path: &Path,
    fields_path: &Path,
    output: &Path,
    reference_path: Option<&Path>,
) -> Result<Value> {
    ensure!(
        std::env::var_os("EVM_STATE_CAPACITY_CONFIG").is_some(),
        "run this workload under capacity-run"
    );
    let evidence_raw = fs::read(evidence_path)?;
    let fields_raw = fs::read(fields_path)?;
    let record: Value = serde_json::from_slice(&evidence_raw)?;
    let evidence = &record["successful_comparison"];
    let accounts = evidence["accounts"]
        .as_array()
        .context("invalid comparison accounts")?;
    ensure!(
        evidence["ordered_state_matches"] == true && accounts.len() == 1,
        "expected a completed isolated single-account comparison"
    );
    let fields: Value = serde_json::from_slice(&fields_raw)?;
    ensure!(
        fields
            .as_object()
            .context("invalid account fields")?
            .keys()
            .map(|s| json!(s))
            .collect::<Vec<_>>()
            == *accounts,
        "metadata accounts differ from the isolated comparison"
    );
    let variant = evidence["variants"]
        .as_array()
        .context("invalid comparison variants")?
        .iter()
        .find(|v| v["name"] == "default")
        .context("missing default comparison variant")?;
    let client = ClickHouse::new(string(evidence, "database")?)?;
    let out = resolve(output)?;
    capacity::check(&client, &[out.clone()], "trie-workspace-start")?;
    fs::create_dir_all(out.parent().context("workspace has no parent")?)?;
    fs::create_dir(&out)?;
    let measured = bootstrap::digest(
        &client,
        string(variant, "generation")?,
        &fields,
        &evidence["accounts"],
    )?;
    ensure!(
        measured
            .as_object()
            .unwrap()
            .iter()
            .all(|(key, value)| variant.get(key) == Some(value)),
        "retained generation or account fields differ from the recorded state"
    );
    let mut identity = json!({"format_version":2,"status":"unproven-trie-resource-measurement","backend":"rust-sorted-alloy",
        "account":accounts[0],"header":evidence["target_header"],"database":client.database,"generation":variant["generation"],
        "run_id":evidence["run_id"],"module_hash":evidence["module_hash"],"package_sha256":evidence["package_sha256"],
        "evidence_sha256":hex::encode(Sha256::digest(evidence_raw)),"fields_sha256":hex::encode(Sha256::digest(fields_raw)),
        "binary_sha256":file_hash(&std::env::current_exe()?)?,"crate_version":env!("CARGO_PKG_VERSION"),
        "state_sha256":measured["state_sha256"],"nonzero_slots":measured["nonzero_slots"],
        "qualification":"Resource measurement of a checksummed private account state. No account proof, independently accepted storage root or ready checkpoint is claimed."});
    let reference = if let Some(path) = reference_path {
        let raw = fs::read(path)?;
        let reference: Value = serde_json::from_slice(&raw)?;
        for key in [
            "account",
            "header",
            "database",
            "generation",
            "run_id",
            "module_hash",
            "package_sha256",
            "fields_sha256",
            "nonzero_slots",
            "state_sha256",
        ] {
            ensure!(
                identity[key] == reference[key],
                "reference reconstruction differs in {key}"
            );
        }
        identity["reference_sha256"] = json!(hex::encode(Sha256::digest(raw)));
        Some(reference)
    } else {
        None
    };
    atomic_json(&out.join("input.json"), &identity, false)?;
    let path = out.join("storage.sqlite");
    let mut database = StorageSort::new(&path)?;
    let mut progress = Progress {
        client: &client,
        out: &out,
        path: &path,
        log: File::create_new(out.join("progress.jsonl"))?,
        started: Instant::now(),
        cpu_started: process_usage()?.0,
    };
    let mut digest = Sha256::new();
    digest.update(canonical_json(&fields)?);
    let mut count = 0_u64;
    let mut previous = None::<String>;
    for row in client.rows("SELECT address,slot,value FROM bootstrap_storage WHERE generation={id:String} ORDER BY address,slot",&params(json!({"id":variant["generation"]}))?)? {
        let row=row?;let account=string(&row,"address")?;let slot=string(&row,"slot")?;let value=string(&row,"value")?;
        ensure!(row["address"]==identity["account"] && previous.as_ref().is_none_or(|last|slot>last.as_str()),"unexpected account or unordered/duplicate storage key");
        digest.update(format!("{account}{slot}{value}"));database.insert(slot,value)?;previous=Some(slot.into());count+=1;
        if count%100000==0 {progress.point("trie-workspace-progress",count)?;}
    }
    let (root, root_count) = database.finish()?;
    let seconds = progress.started.elapsed().as_secs_f64();
    let cpu_seconds = process_usage()?.0 - progress.cpu_started;
    ensure!(
        count == root_count
            && count == uint(&measured["nonzero_slots"])?
            && hex::encode(digest.finalize()) == string(&measured, "state_sha256")?,
        "trie input count/checksum differs from the frozen generation"
    );
    let root = format!("0x{}", hex::encode(root));
    if let Some(reference) = &reference {
        ensure!(
            reference["reconstructed_storage_root"] == root,
            "reconstructed storage root differs from the reference"
        );
    }
    let final_point = progress.point("trie-workspace-finished", count)?;
    let mut result = identity;
    result.as_object_mut().unwrap().extend(json!({"reconstructed_storage_root":root,"trie_seconds_including_progress_guards":seconds,
        "process_cpu_seconds_during_trie":cpu_seconds,"whole_process_peak_rss_bytes":process_usage()?.1,"workspace_entries":root_count,
        "workspace_entry_kind":"hashed storage slots","workspace_file_bytes":final_point["workspace_file_bytes"],"workspace_allocated_bytes":final_point["workspace_allocated_bytes"],
        "streamed_input_checksum_matches":true,"reference_root_matches":reference.as_ref().map(|_|true),
        "workspace_note":"Disposable SQLite workspace is not a portable trie export. Sorted slots are committed before hashing; workspace file sizes are measured after closing the database. RSS covers this Rust process, not the ClickHouse server or other processes."}).as_object().unwrap().clone());
    atomic_json(&out.join("result.json"), &result, false)?;
    Ok(result)
}
