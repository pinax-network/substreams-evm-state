use anyhow::Result;
use clap::{Parser, Subcommand};
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

fn run() -> Result<()> {
    let args = Cli::parse();
    let client = ClickHouse::new(&args.database)?;
    let result = match args.command {
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
