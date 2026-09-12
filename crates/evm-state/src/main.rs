use anyhow::Result;
use clap::{Args, Parser, Subcommand};
use evm_state::{ch::ClickHouse, checkpoint, reader, retention};
use serde_json::json;
use std::path::PathBuf;

#[derive(Parser)]
#[command(version, about)]
struct Cli {
    #[arg(long, default_value = "evm_state")]
    database: String,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Verify and restore a portable checkpoint into a new generation.
    ImportExport {
        directory: PathBuf,
        #[arg(long)]
        expected_hash: Option<String>,
        #[arg(long)]
        work_dir: Option<PathBuf>,
        #[arg(long, default_value_t = 100_000_000_000_u64)]
        budget_bytes: u64,
    },
    /// Write a complete paginated checkpoint with offline proofs.
    Export {
        snapshot_id: String,
        #[arg(long)]
        output: PathBuf,
        #[arg(long, default_value_t = 10000)]
        page_size: usize,
        #[arg(long)]
        work_dir: Option<PathBuf>,
    },
    /// Verify exported files without database or RPC access.
    VerifyExport {
        directory: PathBuf,
        #[arg(long)]
        expected_hash: Option<String>,
        #[arg(long)]
        work_dir: Option<PathBuf>,
    },
    /// Verify isolated source state and publish an immutable checkpoint.
    Checkpoint {
        #[arg(long)]
        proofs: PathBuf,
        #[arg(long)]
        sources: PathBuf,
        #[arg(long)]
        base: Option<String>,
        #[arg(long, default_value_t = 100_000_000_000_u64)]
        budget_bytes: u64,
        #[arg(long, default_value = "localdata/verification")]
        work_dir: PathBuf,
        #[arg(long)]
        output: Option<PathBuf>,
    },
    /// Bind a native sink to an isolated database and durable directory.
    Prepare(NativeArgs),
    /// Run or resume the guarded finalized native sink.
    Ingest(IngestArgs),
    /// Restore a damaged native cursor from verified durable progress.
    RecoverCursor(NativeArgs),
    /// Capture a finalized header, account proofs and code before replay.
    CaptureProofs {
        #[arg(long)]
        accounts: String,
        #[arg(long, default_value = "finalized")]
        block: String,
        #[arg(long)]
        expected_hash: Option<String>,
        #[arg(long)]
        output: PathBuf,
    },
    /// Measure whole data directories against a declared policy.
    CapacityReport {
        #[arg(long)]
        config: PathBuf,
    },
    /// Supervise a command under a sampled capacity policy.
    CapacityRun {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        output: PathBuf,
        #[arg(long, default_value_t = 1.0)]
        interval: f64,
        #[arg(last = true, required = true)]
        child_command: Vec<String>,
    },
    /// Create immutable checkpoint tables.
    Init,
    /// Show one published manifest or account.
    Show {
        snapshot_id: String,
        #[arg(long)]
        address: Option<String>,
    },
    /// Protect a checkpoint while a consumer reads it.
    Pin {
        snapshot_id: String,
        #[arg(long, default_value = "reader")]
        purpose: String,
    },
    /// List durable reader pins.
    Pins,
    /// Release a reader pin.
    Unpin { pin_id: String },
    /// Page complete storage through an active checkpoint pin.
    Page {
        pin_id: String,
        #[arg(long)]
        address: String,
        #[arg(long)]
        cursor: Option<String>,
        #[arg(long, default_value_t = 1000)]
        limit: usize,
    },
    /// Plan generation retention, respecting pins and account coverage.
    RetentionPlan {
        #[arg(long, default_value_t = 2)]
        keep_latest: usize,
    },
    /// Remove old and failed generations after a verified retention plan.
    PruneCheckpoints {
        #[arg(long, default_value_t = 2)]
        keep_latest: usize,
    },
}

#[derive(Args)]
struct NativeArgs {
    #[arg(long)]
    package: PathBuf,
    #[arg(long, default_value = "bsc.substreams.pinax.network:443")]
    endpoint: String,
    #[arg(long)]
    accounts: String,
    #[arg(long)]
    start_block: u64,
    #[arg(long)]
    state_dir: PathBuf,
    #[arg(long)]
    checkpoint_database: Option<String>,
}
impl NativeArgs {
    fn options(self) -> Result<evm_state::ingest::NativeOptions> {
        Ok(evm_state::ingest::NativeOptions {
            package: self.package,
            endpoint: self.endpoint,
            accounts: json!(self.accounts),
            start_block: self.start_block,
            state_dir: self.state_dir,
            checkpoint_database: self.checkpoint_database,
            dsn: std::env::var("SUBSTREAMS_SINK_DSN").map_err(|_| {
                anyhow::anyhow!(
                    "set SUBSTREAMS_SINK_DSN to the native ClickHouse connection string"
                )
            })?,
        })
    }
}
#[derive(Args)]
struct IngestArgs {
    #[command(flatten)]
    native: NativeArgs,
    #[arg(long)]
    stop_block: Option<u64>,
    #[arg(long, default_value_t = 3)]
    max_retries: u32,
    #[arg(long, default_value_t = 1)]
    decode_batch_size: u32,
    #[arg(long, default_value_t = 100)]
    spool_max_idle_ms: u64,
    #[arg(long)]
    prometheus_addr: Option<String>,
    #[arg(long)]
    parallel_workers: Option<u32>,
}

fn run() -> Result<()> {
    let args = Cli::parse();
    let client = ClickHouse::new(&args.database)?;
    let result = match args.command {
        Commands::ImportExport {
            directory,
            expected_hash,
            work_dir,
            budget_bytes,
        } => {
            let mut record = evm_state::importer::import_checkpoint(
                &client,
                &directory,
                expected_hash.as_deref(),
                &work_dir.unwrap_or_else(std::env::temp_dir),
                budget_bytes,
            )?;
            record.as_object_mut().unwrap().remove("proof_bundle");
            record
        }
        Commands::Export {
            snapshot_id,
            output,
            page_size,
            work_dir,
        } => evm_state::export::export_checkpoint(
            &client,
            &snapshot_id,
            &output,
            page_size,
            &work_dir.unwrap_or_else(std::env::temp_dir),
        )?,
        Commands::VerifyExport {
            directory,
            expected_hash,
            work_dir,
        } => evm_state::export::verify_export(
            &directory,
            expected_hash.as_deref(),
            &work_dir.unwrap_or_else(std::env::temp_dir),
        )?,
        Commands::Checkpoint {
            proofs,
            sources,
            base,
            budget_bytes,
            work_dir,
            output,
        } => {
            if let Some(output) = &output {
                anyhow::ensure!(
                    !output.try_exists()?,
                    "checkpoint output already exists; choose a new output file"
                );
            }
            let mut paths = vec![proofs.clone(), sources.clone()];
            if let Some(output) = &output {
                paths.push(output.parent().unwrap_or(std::path::Path::new(".")).into());
            }
            evm_state::capacity::check(&client, &paths, "checkpoint-files")?;
            let bundle: serde_json::Value = serde_json::from_slice(&std::fs::read(proofs)?)?;
            let inputs: Vec<serde_json::Value> = serde_json::from_slice(&std::fs::read(sources)?)?;
            let mut record = checkpoint::build(
                &client,
                &bundle,
                &inputs,
                base.as_deref(),
                budget_bytes,
                &work_dir,
            )?;
            if let Some(output) = output {
                evm_state::files::atomic_json(&output, &record, false)?;
            }
            record.as_object_mut().unwrap().remove("proof_bundle");
            record
        }
        Commands::Prepare(native) => evm_state::ingest::prepare(&client, &native.options()?)?,
        Commands::RecoverCursor(native) => {
            evm_state::ingest::recover_cursor(&client, &native.options()?)?
        }
        Commands::Ingest(args) => {
            let options = evm_state::ingest::IngestOptions {
                stop_block: args.stop_block,
                max_retries: args.max_retries,
                decode_batch_size: args.decode_batch_size,
                spool_max_idle_ms: args.spool_max_idle_ms,
                prometheus_addr: args.prometheus_addr,
                parallel_workers: args.parallel_workers,
            };
            evm_state::ingest::ingest(&client, &args.native.options()?, &options)?
        }
        Commands::CaptureProofs {
            accounts,
            block,
            expected_hash,
            output,
        } => {
            anyhow::ensure!(
                !output.try_exists()?,
                "proof output already exists; choose a new capture file"
            );
            evm_state::capacity::check(
                &client,
                &[output.parent().unwrap_or(std::path::Path::new(".")).into()],
                "proof-capture",
            )?;
            let number = if block == "finalized" {
                None
            } else {
                Some(block.parse::<u64>()?)
            };
            let bundle = evm_state::rpc::capture(
                &evm_state::rpc::Rpc::new(None, None)?,
                checkpoint::canonical_accounts(&json!(accounts))?,
                number,
                expected_hash.as_deref(),
            )?;
            evm_state::files::atomic_json(&output, &bundle, false)?;
            json!({"output":output,"header":bundle["header"],"header_trust":bundle["header_trust"]})
        }
        Commands::CapacityReport { config } => evm_state::capacity::Meter::new(
            &client,
            serde_json::from_slice(&std::fs::read(config)?)?,
        )?
        .sample()?,
        Commands::CapacityRun {
            config,
            output,
            interval,
            child_command,
        } => {
            let mut meter = evm_state::capacity::Meter::new(
                &client,
                serde_json::from_slice(&std::fs::read(config)?)?,
            )?;
            let report =
                evm_state::capacity::supervise(&mut meter, &child_command, &output, interval)?;
            if report["status"] != "completed" {
                println!("{}", serde_json::to_string_pretty(&report)?);
                anyhow::bail!(
                    "capacity-run stopped; inspect the capacity report and retained run state"
                );
            }
            report
        }
        Commands::Init => {
            checkpoint::setup(&client)?;
            json!({"database":client.database,"checkpoint_schema":"ready"})
        }
        Commands::Show {
            snapshot_id,
            address,
        } => {
            if let Some(address) = address {
                checkpoint::read_account(&client, &snapshot_id, &address)?
            } else {
                checkpoint::manifest(&client, &snapshot_id)?
            }
        }
        Commands::Pin {
            snapshot_id,
            purpose,
        } => reader::pin(&client, &snapshot_id, &purpose)?,
        Commands::Pins => json!(reader::list_pins(&client)?),
        Commands::Unpin { pin_id } => reader::unpin(&client, &pin_id)?,
        Commands::Page {
            pin_id,
            address,
            cursor,
            limit,
        } => reader::page(&client, &pin_id, &address, cursor.as_deref(), limit)?,
        Commands::RetentionPlan { keep_latest } => retention::plan(&client, keep_latest)?,
        Commands::PruneCheckpoints { keep_latest } => retention::prune(&client, keep_latest)?,
    };
    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}

fn main() {
    if let Err(error) = run() {
        eprintln!("evm-state: {error:#}");
        std::process::exit(1);
    }
}
