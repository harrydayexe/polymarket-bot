use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};
use clap_verbosity_flag::{Verbosity, WarnLevel};

#[derive(Parser)]
#[command(
    name = "PolyMarket Archiver",
    version,
    about = "A tool to archive PolyMarket API data"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Args)]
pub struct CommonArgs {
    /// Path to the config file. By default looks for ./config.toml
    #[arg(short, long, default_value = "./config.toml")]
    pub config: PathBuf,

    #[command(flatten)]
    pub verbose: Verbosity<WarnLevel>,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Starts the recorder. Runs until stopped
    Run(CommonArgs),

    /// Pre-flight checks
    Check(CommonArgs),

    /// Reads the status file. Exits 0 if healthy, 3 if not.
    Status(CommonArgs),

    /// Prints the markets the registry would select right now, with counts.
    Markets(CommonArgs),

    /// Takes REST snapshot immediately and writes them to the archive
    Snapshot(crate::commands::snapshot::SnapshotArgs),

    /// Compaction for one or more finished days
    Compact(crate::commands::compact::CompactArgs),

    /// Offline consistency check and report
    Verify(crate::commands::verify::VerifyArgs),

    /// Lists recorded gaps with durations and reasons
    Gaps(crate::commands::gaps::GapsArgs),
}

impl Commands {
    pub fn common(&self) -> &CommonArgs {
        match self {
            Commands::Run(args) => &args,
            Commands::Check(args) => &args,
            Commands::Status(args) => &args,
            Commands::Markets(args) => &args,
            Commands::Snapshot(args) => &args.common,
            Commands::Compact(args) => &args.common,
            Commands::Verify(args) => &args.common,
            Commands::Gaps(args) => &args.common,
        }
    }
}
