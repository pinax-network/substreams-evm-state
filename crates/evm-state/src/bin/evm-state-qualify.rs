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
