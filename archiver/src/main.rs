use chrono::NaiveDate;
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = "PolyMarket Archiver",
    version,
    about = "A tool to archive PolyMarket API data"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Starts the recorder. Runs until stopped
    Run,

    /// Pre-flight checks
    Check,

    /// Reads the status file. Exits 0 if healthy, 3 if not.
    Status,

    /// Prints the markets the registry would select right now, with counts.
    Markets,

    /// Takes REST snapshot immediately and writes them to the archive
    Snapshot {
        /// Snapshot everything
        #[arg(short, long, conflicts_with = "tokens")]
        all: bool,

        /// Snapshot specific tokens
        #[arg(short, long, value_delimiter = ',', num_args = 1..)]
        tokens: Option<Vec<String>>,
    },

    /// Compaction for one or more finished days
    Compact {
        /// Date to compact since
        #[arg(short, long, value_parser = parse_date, visible_alias = "since")]
        date: NaiveDate,
    },

    /// Offline consistency check and report
    Verify {
        /// Specify the date to verify
        #[arg(short, long, value_parser = parse_date)]
        date: NaiveDate,
    },

    /// Lists recorded gaps with durations and reasons
    Gaps {
        /// Specify the date to check for gaps on
        #[arg(short, long, value_parser = parse_date)]
        date: NaiveDate,

        /// Which tokens to check for gaps in
        #[arg(short, long, value_delimiter = ',', num_args = 1..)]
        tokens: Option<Vec<String>>,
    },
}

/// Parse a YYYY-MM-DD format date string into a string, or return an error
fn parse_date(s: &str) -> Result<NaiveDate, String> {
    NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .map_err(|e| format!("invalid date '{s}' (expected YYYY-MM-DD): {e}"))
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::Run => {
            println!("Run...")
        }
        Commands::Check => {
            println!("Check...")
        }
        Commands::Status => {
            println!("Status...")
        }
        Commands::Markets => {
            println!("Markets...")
        }
        Commands::Snapshot { all, tokens } => {
            println!("Snapshot...")
        }
        Commands::Compact { date } => {
            println!("Snapshot...")
        }
        Commands::Verify { date } => {
            println!("Verify...")
        }
        Commands::Gaps { date, tokens } => {
            println!("Gaps...")
        }
    }
}
