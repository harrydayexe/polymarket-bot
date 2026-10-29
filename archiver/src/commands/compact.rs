use chrono::{NaiveDate, TimeDelta, Utc};
use clap::Args;

use crate::{
    cli::CommonArgs,
    compactor::{self, DayManifest, Options, TABLES},
    config::Config,
};

#[derive(Args)]
pub struct CompactArgs {
    #[command(flatten)]
    pub common: CommonArgs,

    /// UTC day to compact (YYYY-MM-DD), replacing any earlier compaction of it. Defaults to
    /// yesterday
    #[arg(
        short,
        long,
        value_parser = crate::commands::parse_date::parse_date,
        conflicts_with = "since"
    )]
    date: Option<NaiveDate>,

    /// Compact every finished day from this one (YYYY-MM-DD) up to yesterday that has raw
    /// files and has not been compacted yet
    #[arg(long, value_parser = crate::commands::parse_date::parse_date)]
    since: Option<NaiveDate>,
}

pub async fn execute(args: CompactArgs) -> anyhow::Result<()> {
    let config = Config::load(&args.common.config)?;
    let root = config.archive_dir;
    tokio::task::spawn_blocking(move || {
        let today = Utc::now().date_naive();
        let days = match args.since {
            Some(since) => {
                let days = compactor::pending_days(&root, since, today)?;
                // Refuse before compacting anything if any of the days is unfinished.
                for date in &days {
                    compactor::check_finished(&root, *date, today)?;
                }
                if days.is_empty() {
                    println!("nothing to compact since {since}");
                }
                days
            }
            None => vec![args.date.unwrap_or(today - TimeDelta::days(1))],
        };
        for date in days {
            let manifest = compactor::compact_day(&root, date, today, &Options::default())?;
            print_summary(&manifest);
        }
        Ok(())
    })
    .await?
}

fn print_summary(manifest: &DayManifest) {
    println!(
        "compacted {}: {} input files, {} malformed lines",
        manifest.date,
        manifest.inputs.len(),
        manifest.malformed_lines
    );
    for table in TABLES {
        let rows = manifest.row_counts.get(table).copied().unwrap_or(0);
        println!("  {table:<14} {rows:>12}");
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use crate::cli::{Cli, Commands};

    fn parse(args: &[&str]) -> Result<Cli, clap::Error> {
        Cli::try_parse_from([&["archiver", "compact"], args].concat())
    }

    fn compact_args(args: &[&str]) -> super::CompactArgs {
        match parse(args).unwrap().command {
            Commands::Compact(args) => args,
            _ => panic!("not compact"),
        }
    }

    #[test]
    fn date_and_since_are_separate_and_exclusive() {
        let args = compact_args(&["--since", "2026-10-01"]);
        assert_eq!(args.date, None);
        assert_eq!(args.since.unwrap().to_string(), "2026-10-01");
        let args = compact_args(&["--date", "2026-10-01"]);
        assert_eq!(args.date.unwrap().to_string(), "2026-10-01");
        assert_eq!(args.since, None);
        let args = compact_args(&[]);
        assert_eq!((args.date, args.since), (None, None));
        assert!(parse(&["--date", "2026-10-01", "--since", "2026-10-02"]).is_err());
        assert!(parse(&["--since", "yesterday"]).is_err());
    }
}
