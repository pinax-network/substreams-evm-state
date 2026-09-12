//! Reproducible synthetic state/retention and incompressible merge workloads.
use crate::{
    capacity,
    ch::{identifier, params, uint, ClickHouse},
    checkpoint, export, files, importer,
    ingest::{self, NativeOptions},
    proof::{fixed, string},
    reader, retention,
    synthetic::{self, word, Account},
};
use alloy_primitives::U256;
use anyhow::{ensure, Context, Result};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    time::Instant,
};
#[derive(Clone, clap::Args)]
pub struct StressOptions {
    #[arg(long)]
    pub prefix: String,
    #[arg(long)]
    pub output: PathBuf,
    #[arg(long, default_value = "spkg/evm-state-v0.1.0.spkg")]
    pub package: PathBuf,
    #[arg(long, default_value_t = 64)]
    pub accounts: usize,
    #[arg(long, default_value_t = 100000)]
    pub hot_slots: u64,
    #[arg(long, default_value_t = 64)]
    pub quiet_slots: u64,
    #[arg(long, default_value_t = 0)]
    pub merge_only_mib: u64,
    #[arg(long, default_value_t = 100_000_000_000u64)]
    pub budget_bytes: u64,
}
impl StressOptions {
    pub fn validate(&self) -> Result<()> {
        identifier(&self.prefix)?;
        ensure!(
            (1..=64).contains(&self.accounts) && self.hot_slots >= 2,
            "require 1..64 accounts and at least two hot slots"
        );
        let total = self
            .quiet_slots
            .checked_mul((self.accounts - 1) as u64)
            .and_then(|n| n.checked_add(self.hot_slots))
            .context("synthetic slot count overflow")?;
        ensure!(
            total <= 1_000_000 && self.merge_only_mib <= 1024,
            "synthetic fixture exceeds one million slots or 1024 MiB merge limit"
        );
        ensure!(self.budget_bytes > 0, "invalid synthetic workload budget");
        Ok(())
    }
}
fn value(slot: u64) -> U256 {
    let value = U256::from_be_slice(&Sha256::digest(slot.to_string()));
    if value.is_zero() {
        U256::from(1)
    } else {
        value
    }
}
fn timed(
    name: &str,
    out: &Path,
    clients: &[ClickHouse; 3],
    phases: &mut Vec<Value>,
    operation: impl FnOnce() -> Result<Value>,
) -> Result<Value> {
    let start = Instant::now();
    let result = operation()?;
    let phase = json!({"phase":name,"seconds":start.elapsed().as_secs_f64(),"source_parts_bytes":clients[0].disk_usage()?,"checkpoint_parts_bytes":clients[1].disk_usage()?,"restored_parts_bytes":clients[2].disk_usage()?});
    phases.push(phase.clone());
    files::atomic_json(&out.join("phases.json"), &json!(phases), true)?;
    println!("{phase}");
    Ok(result)
}
fn merge(target: &ClickHouse, out: &Path, mib: u64) -> Result<Value> {
    target.with_database("default")?.execute(
        &format!("CREATE DATABASE {}", target.database),
        &Default::default(),
    )?;
    target.execute(
        "CREATE TABLE merge_stress (key UInt64,value String) ENGINE=MergeTree ORDER BY key",
        &Default::default(),
    )?;
    let table = format!("{}.merge_stress", target.database);
    target.execute(&format!("SYSTEM STOP MERGES {table}"), &Default::default())?;
    let rows = (mib * 1024 * 1024 / 4096 / 8).max(1);
    let started = Instant::now();
    let staged = (|| -> Result<u64> {
        for part in 0..8 {
            target.execute(&format!("INSERT INTO merge_stress SELECT number*8+{part},randomString(4096) FROM numbers({rows})"),&Default::default())?;
            capacity::check(target, &[out.into()], "merge-part-staged")?;
        }
        target.disk_usage()
    })();
    let restart = target.execute(&format!("SYSTEM START MERGES {table}"), &Default::default());
    let before = staged?;
    restart?;
    println!(
        "{}",
        json!({"phase":"merge-parts-staged","rows":rows*8,"parts_bytes":before})
    );
    let merging = Instant::now();
    target.execute("OPTIMIZE TABLE merge_stress FINAL", &Default::default())?;
    capacity::check(target, &[out.into()], "merge-completed")?;
    let result = json!({"format_version":1,"workload":"synthetic incompressible eight-part merge","database":target.database,"rows":rows*8,"payload_bytes_per_row":4096,
        "parts_bytes_before":before,"parts_bytes_after":target.disk_usage()?,"merge_seconds":merging.elapsed().as_secs_f64(),"total_seconds":started.elapsed().as_secs_f64(),
        "limitations":["auxiliary merge stress table, not account state or a customer footprint","random payload bytes; row count and payload size are reproducible"]});
    files::atomic_json(&out.join("result.json"), &result, false)?;
    Ok(result)
}
pub fn measure(admin: &ClickHouse, options: &StressOptions, dsn_template: &str) -> Result<Value> {
    options.validate()?;
    let out = files::resolve(&options.output)?;
    let names = ["_source", "_checkpoints", "_restored"]
        .map(|suffix| format!("{}{suffix}", options.prefix));
    for name in &names {
        identifier(name)?;
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
    let clients = names
        .iter()
        .map(|name| {
            Ok(admin
                .with_database(name)?
                .with_control_home(out.join("control")))
        })
        .collect::<Result<Vec<_>>>()?;
    let clients: [ClickHouse; 3] = clients
        .try_into()
        .map_err(|_| anyhow::anyhow!("invalid workload clients"))?;
    capacity::check(admin, &[out.clone()], "synthetic-workload-start")?;
    fs::create_dir_all(out.parent().context("output has no parent")?)?;
    fs::create_dir(&out)?;
    if options.merge_only_mib > 0 {
        return merge(&clients[1], &out, options.merge_only_mib);
    }
    ensure!(
        dsn_template.contains("{database}"),
        "native DSN template must contain {{database}}"
    );
    let selected = (1..=options.accounts)
        .map(|i| format!("0x{i:040x}"))
        .collect::<Vec<_>>();
    let native = NativeOptions {
        package: options.package.clone(),
        endpoint: "http://127.0.0.1:1".into(),
        accounts: json!(selected),
        start_block: 100,
        state_dir: out.join("native-schema"),
        dsn: dsn_template.replace("{database}", &names[0]),
        checkpoint_database: Some(names[1].clone()),
    };
    let run = ingest::prepare(&clients[0], &native)?;
    for client in &clients[1..] {
        checkpoint::setup(client)?;
    }
    let mut values = BTreeMap::new();
    for (index, address) in selected.iter().enumerate() {
        let mut account = Account::default();
        for slot in 0..if index == 0 {
            options.hot_slots
        } else {
            options.quiet_slots
        } {
            account.slots.insert(word(slot), value(slot));
        }
        values.insert(address.clone(), account);
    }
    let mut phases = Vec::new();
    let bundle = timed(
        "construct-synthetic-proofs",
        &out,
        &clients,
        &mut phases,
        || synthetic::bundle(100, &values, None),
    )?;
    let patches = values
        .iter()
        .flat_map(|(address, account)| {
            account
                .slots
                .iter()
                .map(move |(slot, value)| (address.clone(), *slot, *value))
        })
        .collect::<Vec<_>>();
    synthetic::insert(
        &clients[0],
        &native.state_dir,
        vec![synthetic::block(&bundle, &patches, true)?],
    )?;
    drop(patches);
    let mut source = json!({});
    for key in [
        "database",
        "accounts",
        "start_block",
        "module_hash",
        "final_blocks_only",
    ] {
        source[key] = run["identity"][key].clone();
    }
    let work = out.join("trie-work");
    let first = timed("checkpoint-initial", &out, &clients, &mut phases, || {
        checkpoint::build(
            &clients[1],
            &bundle,
            &[source.clone()],
            None,
            options.budget_bytes,
            &work,
        )
    })?;
    let initial = string(&first, "snapshot_id")?;
    let exported = timed("export-initial", &out, &clients, &mut phases, || {
        export::export_checkpoint(&clients[1], initial, &out.join("export"), 10000, &work)
    })?;
    let imported = timed("restore-and-reverify", &out, &clients, &mut phases, || {
        importer::import_checkpoint(
            &clients[2],
            &out.join("export"),
            Some(string(&first["header"], "hash")?),
            &work,
            options.budget_bytes,
        )
    })?;
    ensure!(
        imported["state_sha256"] == first["state_sha256"],
        "restored synthetic checksum differs"
    );
    let pinned = reader::pin(&clients[1], initial, "capacity-qualification-reader")?;
    let pin = string(&pinned, "pin_id")?;
    let operation = (|| -> Result<Value> {
        let hot = &selected[0];
        let mut patches = Vec::new();
        for slot in 0..options.hot_slots / 2 {
            values.get_mut(hot).unwrap().slots.remove(&word(slot));
            patches.push((hot.clone(), word(slot), U256::ZERO));
            let new = options.hot_slots + slot;
            values
                .get_mut(hot)
                .unwrap()
                .slots
                .insert(word(new), value(new));
            patches.push((hot.clone(), word(new), value(new)));
        }
        let next = timed(
            "construct-updated-proofs",
            &out,
            &clients,
            &mut phases,
            || synthetic::bundle(101, &values, Some(string(&first["header"], "hash")?)),
        )?;
        synthetic::insert(
            &clients[0],
            &native.state_dir,
            vec![synthetic::block(&next, &patches, false)?],
        )?;
        drop(patches);
        source["start_block"] = json!(101);
        let second = timed("checkpoint-slot-churn", &out, &clients, &mut phases, || {
            checkpoint::build(
                &clients[1],
                &next,
                &[source.clone()],
                Some(initial),
                options.budget_bytes,
                &work,
            )
        })?;
        ensure!(
            first["nonzero_slots"] == second["nonzero_slots"],
            "slot churn changed expected slot count"
        );
        let retained = timed(
            "prune-with-pinned-reader",
            &out,
            &clients,
            &mut phases,
            || retention::prune(&clients[1], 1),
        )?;
        ensure!(
            !retained["remove"]
                .as_array()
                .context("invalid retention result")?
                .contains(&json!(initial)),
            "pinned initial checkpoint was removed"
        );
        let page = reader::page(&clients[1], pin, hot, None, 1)?;
        ensure!(
            fixed::<32>(string(&page["storage"][0], "slot")?)? == [0u8; 32],
            "old pinned state changed during slot churn"
        );
        Ok(second)
    })();
    let unpinned = reader::unpin(&clients[1], pin);
    let second = operation?;
    unpinned?;
    let reclaimed = timed(
        "prune-after-reader-release",
        &out,
        &clients,
        &mut phases,
        || retention::prune(&clients[1], 1),
    )?;
    ensure!(
        reclaimed["remove"]
            .as_array()
            .context("invalid retention result")?
            .contains(&json!(initial)),
        "released initial checkpoint was not reclaimed"
    );
    capacity::check(&clients[1], &[out.clone()], "synthetic-workload-verified")?;
    let result = json!({"format_version":1,"workload":"synthetic native-schema state and retention stress fixture","account_count":options.accounts,"hot_account_slots":options.hot_slots,"quiet_account_slots":options.quiet_slots,
        "total_nonzero_slots":first["nonzero_slots"],"cleared_and_replaced_slots":options.hot_slots/2,"initial_checksum":first["state_sha256"],"restored_checksum":imported["state_sha256"],"updated_checksum":second["state_sha256"],"export_bytes":exported["bytes"],"databases":names,"phases":phases,
        "limitations":["synthetic accounts and headers, not BSC or customer data","no server backprocessing or native transport throughput measurement","capacity summary reports sampled peaks plus publication/trie guard samples"]});
    files::atomic_json(&out.join("result.json"), &result, false)?;
    Ok(result)
}
