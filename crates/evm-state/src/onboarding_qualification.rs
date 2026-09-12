//! Real isolated cohort cutover with a killed publisher and combined-filter
//! continuation. Recent bootstrap is allowed only for proven-empty new storage.
use crate::{
    ch::{identifier, params, uint, ClickHouse},
    checkpoint, export, files, importer,
    ingest::{self, IngestOptions, NativeOptions},
    process,
    proof::{string, verify_account},
    reader,
    rpc::{self, RpcCall},
    trie_qualification::file_hash,
};
use alloy_trie::EMPTY_ROOT_HASH;
use anyhow::{ensure, Context, Result};
use serde_json::{json, Value};
use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
    time::{Duration, Instant},
};
#[derive(Clone, clap::Args)]
pub struct OnboardingOptions {
    #[arg(long)]
    pub prefix: String,
    #[arg(long)]
    pub root: PathBuf,
    #[arg(long)]
    pub package: PathBuf,
    #[arg(long)]
    pub source_database: Option<String>,
    #[arg(long)]
    pub source_snapshot: Option<String>,
    #[arg(long)]
    pub source_control: Option<PathBuf>,
    #[arg(long)]
    pub new_accounts: Option<String>,
    #[arg(long, default_value = "bsc.substreams.pinax.network:443")]
    pub endpoint: String,
    /// Resume an imported base before completed cutover after a capacity stop.
    #[arg(long)]
    pub resume: bool,
    #[arg(long, hide = true)]
    pub interrupted_child: bool,
    #[arg(long, default_value_t = 100_000_000_000u64)]
    pub budget_bytes: u64,
}
fn read(path: &Path) -> Result<Value> {
    Ok(serde_json::from_slice(&fs::read(path)?)?)
}
fn without_proofs(value: &Value) -> Value {
    let mut value = value.clone();
    value.as_object_mut().unwrap().remove("proof_bundle");
    value
}
fn selected(value: &Value, keys: &[&str]) -> Value {
    let mut result = json!({});
    for key in keys {
        result[*key] = value[*key].clone();
    }
    result
}
fn timed(
    name: &str,
    root: &Path,
    phases: &mut Vec<Value>,
    operation: impl FnOnce() -> Result<Value>,
) -> Result<Value> {
    let start = Instant::now();
    let result = operation()?;
    let phase = json!({"phase":name,"seconds":start.elapsed().as_secs_f64()});
    phases.push(phase.clone());
    files::atomic_json(&root.join("phases.json"), &json!(phases), true)?;
    println!("{phase}");
    Ok(result)
}
pub fn interrupted_child(admin: &ClickHouse, options: &OnboardingOptions) -> Result<Value> {
    identifier(&options.prefix)?;
    let root = files::resolve(&options.root)?;
    let target = admin
        .with_database(&format!("{}_checkpoints", options.prefix))?
        .with_control_home(root.join("control"));
    let proofs = read(&root.join("cutover-proofs.json"))?;
    let sources: Vec<Value> = serde_json::from_slice(&fs::read(root.join("sources.json"))?)?;
    let base = read(&root.join("base.json"))?;
    checkpoint::build_observed(
        &target,
        &proofs,
        &sources,
        Some(string(&base, "snapshot_id")?),
        options.budget_bytes,
        &root.join("trie-work"),
        &|| {
            // SAFETY: this hidden qualification child deliberately kills itself
            // after acknowledged candidate rows, never another process or server.
            unsafe {
                libc::raise(libc::SIGKILL);
            }
            anyhow::bail!("publication failure signal did not stop its own child")
        },
    )?;
    anyhow::bail!("publication fault was not exercised")
}
pub fn measure(
    admin: &ClickHouse,
    rpc: &impl RpcCall,
    options: &OnboardingOptions,
    dsn_template: &str,
) -> Result<Value> {
    identifier(&options.prefix)?;
    ensure!(
        dsn_template.contains("{database}"),
        "native DSN template must contain {{database}}"
    );
    let source_database = options
        .source_database
        .as_deref()
        .context("source database is required")?;
    let source_snapshot = options
        .source_snapshot
        .as_deref()
        .context("source snapshot is required")?;
    let source_control = options
        .source_control
        .as_deref()
        .context("source control is required")?;
    let new = checkpoint::canonical_accounts(&json!(options
        .new_accounts
        .as_deref()
        .context("new accounts are required")?))?;
    let root = files::resolve(&options.root)?;
    let names = ["_checkpoints", "_old", "_new", "_combined"]
        .map(|suffix| format!("{}{suffix}", options.prefix));
    for name in &names {
        identifier(name)?;
    }
    if options.resume {
        ensure!(
            root.join("base.json").is_file() && !root.join("cutover.json").try_exists()?,
            "resume requires an imported base and no completed cutover"
        );
    } else {
        for name in &names {
            ensure!(
                uint(
                    &admin.one(
                        "SELECT count() AS n FROM system.databases WHERE name={db:String}",
                        &params(json!({"db":name}))?
                    )?["n"]
                )? == 0,
                "qualification database already exists; choose a fresh prefix"
            );
        }
        ensure!(
            !root.join("base.json").try_exists()?,
            "qualification root already contains a checkpoint"
        );
    }
    crate::capacity::check(
        admin,
        &[root.clone(), files::resolve(source_control)?],
        "onboarding-start",
    )?;
    fs::create_dir_all(&root)?;
    let target = admin
        .with_database(&names[0])?
        .with_control_home(root.join("control"));
    let mut phases = if options.resume {
        read(&root.join("phases.json"))?
            .as_array()
            .context("invalid onboarding phases")?
            .clone()
    } else {
        Vec::new()
    };
    let source = admin
        .with_database(source_database)?
        .with_control_home(files::resolve(source_control)?);
    let original = checkpoint::manifest(&source, source_snapshot)?;
    let work = root.join("trie-work");
    let base = if options.resume {
        let saved = read(&root.join("base.json"))?;
        let base = checkpoint::manifest(&target, string(&saved, "snapshot_id")?)?;
        ensure!(
            base["state_sha256"] == original["state_sha256"]
                && base["header"] == original["header"],
            "resume base differs from original source"
        );
        base
    } else {
        timed("export-existing-checkpoint", &root, &mut phases, || {
            export::export_checkpoint(&source, source_snapshot, &root.join("export"), 10000, &work)
        })?;
        let base = timed("restore-isolated-destination", &root, &mut phases, || {
            importer::import_checkpoint(
                &target,
                &root.join("export"),
                Some(string(&original["header"], "hash")?),
                &work,
                options.budget_bytes,
            )
        })?;
        files::atomic_json(&root.join("base.json"), &without_proofs(&base), false)?;
        base
    };
    let old = checkpoint::canonical_accounts(&base["accounts"])?;
    ensure!(
        old.iter().all(|a| !new.contains(a)),
        "new cohort overlaps published base"
    );
    let mut accounts = old.clone();
    accounts.extend(new.clone());
    accounts.sort();
    checkpoint::canonical_accounts(&json!(accounts))?;
    let proofs = if options.resume {
        let proofs = read(&root.join("cutover-proofs.json"))?;
        ensure!(
            json!(proofs["accounts"]
                .as_object()
                .context("invalid cutover proof accounts")?
                .keys()
                .collect::<Vec<_>>())
                == json!(accounts),
            "resume filter differs from captured cutover"
        );
        proofs
    } else {
        let proofs = rpc::capture(rpc, &accounts, None, None)?;
        files::atomic_json(&root.join("cutover-proofs.json"), &proofs, false)?;
        proofs
    };
    for account in &new {
        ensure!(
            verify_account(
                string(&proofs["header"], "state_root")?,
                account,
                &proofs["accounts"][account]["proof"]
            )?
            .storage_root
                == EMPTY_ROOT_HASH,
            "new account has nonempty storage and needs complete historical bootstrap"
        );
    }
    let end = uint(&proofs["header"]["number"])?;
    let base_end = uint(&base["header"]["number"])?;
    ensure!(
        end > base_end && end >= 500 && end < u64::MAX,
        "cutover must advance the base and permit a bounded new-cohort interval"
    );
    let replay = |suffix: &str, selected: &[String], start: u64, stop: u64| -> Result<Value> {
        let database = format!("{}_{suffix}", options.prefix);
        let client = target.with_database(&database)?;
        let native = NativeOptions {
            package: options.package.clone(),
            endpoint: options.endpoint.clone(),
            accounts: json!(selected),
            start_block: start,
            state_dir: root.join(suffix),
            dsn: dsn_template.replace("{database}", &database),
            checkpoint_database: Some(target.database.clone()),
        };
        let ingest_options = IngestOptions {
            stop_block: Some(stop.checked_add(1).context("native stop overflows")?),
            decode_batch_size: 32,
            spool_max_idle_ms: 1000,
            prometheus_addr: Some("127.0.0.1:0".into()),
            ..Default::default()
        };
        ingest::ingest(&client, &native, &ingest_options)
    };
    let old_run = timed("existing-cohort-catch-up", &root, &mut phases, || {
        replay("old", &old, base_end + 1, end)
    })?;
    let new_run = timed(
        "new-empty-storage-cohort-bootstrap",
        &root,
        &mut phases,
        || replay("new", &new, end - 500, end),
    )?;
    let sources = vec![old_run["source"].clone(), new_run["source"].clone()];
    files::atomic_json(&root.join("sources.json"), &json!(sources), options.resume)?;
    let pinned = reader::pin(
        &target,
        string(&base, "snapshot_id")?,
        "old-reader-during-interrupted-cutover",
    )?;
    let pin = string(&pinned, "pin_id")?;
    let result = (|| -> Result<Value> {
        let pages = || -> Result<Vec<Value>> {
            old.iter()
                .map(|account| reader::page(&target, pin, account, None, 1000))
                .collect()
        };
        let before = pages()?;
        let manifests = uint(
            &target.one(
                "SELECT count() AS n FROM checkpoints FINAL",
                &Default::default(),
            )?["n"],
        )?;
        let failed = timed("publication-child-sigkill", &root, &mut phases, || {
            let mut command = Command::new(std::env::current_exe()?);
            command
                .arg("onboarding")
                .arg("--prefix")
                .arg(&options.prefix)
                .arg("--root")
                .arg(&root)
                .arg("--package")
                .arg(&options.package)
                .arg("--budget-bytes")
                .arg(options.budget_bytes.to_string())
                .arg("--interrupted-child");
            let output = process::capture(&mut command, Duration::from_secs(300))?
                .context("publication child timed out before intended failure")?;
            Ok(json!({"exit_code":process::exit_code(output.status)}))
        })?;
        ensure!(
            failed["exit_code"] == -libc::SIGKILL,
            "publication child did not reach its intended failure point"
        );
        ensure!(
            uint(
                &target.one(
                    "SELECT count() AS n FROM checkpoints FINAL",
                    &Default::default()
                )?["n"]
            )? == manifests,
            "failed publication exposed a ready manifest"
        );
        ensure!(
            pages()? == before,
            "old pinned account changed during failed cutover"
        );
        let cutover = timed("retry-and-publish-cutover", &root, &mut phases, || {
            checkpoint::build(
                &target,
                &proofs,
                &sources,
                Some(string(&base, "snapshot_id")?),
                options.budget_bytes,
                &work,
            )
        })?;
        files::atomic_json(&root.join("cutover.json"), &without_proofs(&cutover), false)?;
        ensure!(
            pages()? == before,
            "old reader changed after cohort publication"
        );
        let next = rpc::capture(rpc, &accounts, None, None)?;
        let next_end = uint(&next["header"]["number"])?;
        ensure!(
            next_end > end,
            "finalized chain did not advance after cutover"
        );
        files::atomic_json(&root.join("continuation-proofs.json"), &next, false)?;
        let combined = timed(
            "new-combined-filter-continuation",
            &root,
            &mut phases,
            || replay("combined", &accounts, end + 1, next_end),
        )?;
        let continued = timed("verify-combined-continuation", &root, &mut phases, || {
            checkpoint::build(
                &target,
                &next,
                &[combined["source"].clone()],
                Some(string(&cutover, "snapshot_id")?),
                options.budget_bytes,
                &work,
            )
        })?;
        let mut all = sources.clone();
        all.push(combined["source"].clone());
        Ok(
            json!({"format_version":1,"databases":names,"old_accounts":old,"new_accounts":new,
            "base":selected(&base,&["snapshot_id","header","nonzero_slots","state_sha256"]),"cutover":selected(&cutover,&["snapshot_id","header","nonzero_slots","state_sha256","sources"]),
            "continuation":selected(&continued,&["snapshot_id","header","nonzero_slots","state_sha256","sources"]),"interrupted_publication_exit_code":failed["exit_code"],"old_reader_unchanged":true,"phases":phases,"native_sources":all,
            "export_manifest_sha256":file_hash(&root.join("export/manifest.json"))?,"runtime_binary_sha256":file_hash(&std::env::current_exe()?)?,
            "limitations":["public cohort qualification; customer account list unavailable","new accounts have proven empty storage, so recent enumeration can prove completeness","this does not qualify arbitrary recent bootstrap of a nonempty hot contract","process SIGKILL before ready manifest; no physical host power-loss emulation","provider-finalized encoded headers, not independent BSC consensus verification"]}),
        )
    })();
    let unpinned = reader::unpin(&target, pin);
    let result = result?;
    unpinned?;
    crate::capacity::check(&target, &[root.clone()], "onboarding-verified")?;
    files::atomic_json(&root.join("result.json"), &result, false)?;
    Ok(result)
}
