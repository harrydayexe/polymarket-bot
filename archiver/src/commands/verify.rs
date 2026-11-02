use chrono::{DateTime, NaiveDate, TimeDelta, Utc};
use clap::Args;

use crate::{
    cli::CommonArgs,
    clock::{Clock, SystemClock},
    config::Config,
    verifier,
};

/// How many mismatches the summary lists.
const MISMATCHES_PRINTED: usize = 20;

#[derive(Args)]
pub struct VerifyArgs {
    #[command(flatten)]
    pub common: CommonArgs,

    /// The day to verify, YYYY-MM-DD (UTC). Defaults to yesterday
    #[arg(short, long, value_parser = crate::commands::parse_date::parse_date)]
    date: Option<NaiveDate>,
}

/// Checks a finished day of the archive, writes the report to
/// `<archive_dir>/reports/verify/date=YYYY-MM-DD.json` and prints a summary. Succeeds whenever
/// the check ran, mismatches or not.
pub async fn execute(args: VerifyArgs) -> anyhow::Result<()> {
    let config = Config::load(&args.common.config)?;
    let date = args.date.unwrap_or_else(|| yesterday(SystemClock.now()));
    let root = config.archive_dir;

    let report = tokio::task::spawn_blocking({
        let root = root.clone();
        move || verifier::verify_day(&root, date)
    })
    .await??;
    let path = verifier::write_report(&root, &report)?;

    print!("{}", report.summary(MISMATCHES_PRINTED));
    println!("report: {}", path.display());
    Ok(())
}

fn yesterday(now: DateTime<Utc>) -> NaiveDate {
    (now - TimeDelta::days(1)).date_naive()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_to_yesterday_in_utc() {
        let now: DateTime<Utc> = "2026-10-29T00:30:00Z".parse().unwrap();
        assert_eq!(
            yesterday(now),
            NaiveDate::from_ymd_opt(2026, 10, 28).unwrap()
        );
    }
}
