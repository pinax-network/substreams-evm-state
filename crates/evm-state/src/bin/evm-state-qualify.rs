use anyhow::Result;
use clap::{Parser, Subcommand};
use std::{path::PathBuf, time::Duration};

#[derive(Parser)]
#[command(version, about = "Measure and qualify selective EVM state operations")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}
#[derive(Subcommand)]
enum Commands {
    /// Time pinned pagination and prove the complete returned account state.
    CheckpointReads {
        #[arg(long)]
        database: String,
        #[arg(long)]
        snapshot_id: String,
        #[arg(long)]
        account: String,
        #[arg(long, default_value_t = 1000)]
        page_size: usize,
        #[arg(long, default_value_t = 5)]
        passes: usize,
        #[arg(long)]
        output: PathBuf,
        #[arg(long)]
        work_dir: PathBuf,
    },
    /// Reconstruct a frozen private account trie under capacity supervision.
    TrieWorkspace {
        #[arg(long)]
        evidence: PathBuf,
        #[arg(long)]
        fields: PathBuf,
        #[arg(long)]
        output: PathBuf,
        #[arg(long)]
        reference: Option<PathBuf>,
    },
    /// Append freshly admitted private-prefix growth observations.
    RecordGrowth {
        #[arg(long, default_value = "localdata/qualification")]
        root: PathBuf,
        #[arg(long)]
        output: PathBuf,
        #[arg(long, default_value_t = 7200)]
        duration_seconds: u64,
        #[arg(long, default_value_t = 0)]
        reserve_bytes: u64,
        #[arg(long, default_value_t = 100_000_000_000)]
        budget_bytes: u64,
    },
}
fn run() -> Result<()> {
    match Cli::parse().command {
        Commands::CheckpointReads {
            database,
            snapshot_id,
            account,
            page_size,
            passes,
            output,
            work_dir,
        } => {
            anyhow::ensure!(
                std::env::var_os("EVM_STATE_CAPACITY_CONFIG").is_some(),
                "run this workload under capacity-run"
            );
            let mut client = evm_state::ch::ClickHouse::new(&database)?;
            let result = evm_state::read_qualification::measure(
                &mut client,
                &snapshot_id,
                &account,
                &output,
                &work_dir,
                passes,
                page_size,
            )?;
            println!(
                "{}",
                serde_json::json!({"output":output,"pages":result["calls"].as_array().unwrap().len(),"page_latency_seconds":result["page_latency_seconds"],"root_matches_captured_account_proof":result["root_matches_captured_account_proof"]})
            );
            Ok(())
        }
        Commands::TrieWorkspace {
            evidence,
            fields,
            output,
            reference,
        } => {
            evm_state::trie_qualification::measure(
                &evidence,
                &fields,
                &output,
                reference.as_deref(),
            )?;
            Ok(())
        }
        Commands::RecordGrowth {
            root,
            output,
            duration_seconds,
            reserve_bytes,
            budget_bytes,
        } => evm_state::qualification::record_growth(
            &root,
            &output,
            Duration::from_secs(duration_seconds),
            reserve_bytes,
            budget_bytes,
        ),
    }
}
fn main() {
    if let Err(error) = run() {
        eprintln!("evm-state-qualify: {error:#}");
        std::process::exit(1);
    }
}
