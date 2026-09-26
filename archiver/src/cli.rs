use chrono::NaiveDate;
use clap::{Parser, Subcommand};

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

#[derive(Subcommand)]
pub enum Commands {
    /// Starts the recorder. Runs until stopped
    Run,

    /// Pre-flight checks
    Check,

    /// Reads the status file. Exits 0 if healthy, 3 if not.
    Status,

    /// Prints the markets the registry would select right now, with counts.
    Markets,

    /// Takes REST snapshot immediately and writes them to the archive
    Snapshot(crate::commands::snapshot::SnapshotArgs),

    /// Compaction for one or more finished days
    Compact(crate::commands::compact::CompactArgs),

    /// Offline consistency check and report
    Verify(crate::commands::verify::VerifyArgs),

    /// Lists recorded gaps with durations and reasons
    Gaps(crate::commands::gaps::GapsArgs),
}
