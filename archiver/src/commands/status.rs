use std::path::Path;

use anyhow::Result;
use chrono::{DateTime, NaiveTime, TimeDelta, Utc};

use crate::{
    archive::layout::{PARQUET_DIR, STATUS_FILE, Stream, files_for_date},
    cli::CommonArgs,
    commands::Exit,
    config::Config,
    status::{
        health::{Compaction, evaluate},
        status_file::{StatusFile, read_json},
    },
};

/// Exit code when unhealthy (section 12.1).
const UNHEALTHY: u8 = 3;
/// Yesterday must be compacted by this time of day.
const COMPACTION_DUE: NaiveTime = NaiveTime::from_hms_opt(6, 0, 0).expect("valid time");

pub async fn execute(args: CommonArgs) -> Result<()> {
    let config = Config::load(&args.config)?;
    let root = &config.archive_dir;
    let now = Utc::now();
    let path = root.join(STATUS_FILE);
    let status: StatusFile = match read_json(&path) {
        Ok(Some(status)) => status,
        Ok(None) => {
            println!(
                "UNHEALTHY: no status file at {}: the recorder has not run here",
                path.display()
            );
            return Err(Exit(UNHEALTHY).into());
        }
        Err(e) => {
            println!("UNHEALTHY: {e:#}");
            return Err(Exit(UNHEALTHY).into());
        }
    };

    print_summary(&status, now);
    let problems = evaluate(
        &status,
        now,
        config.min_free_disk_gb,
        compaction(root, now)?,
    );
    println!();
    if problems.is_empty() {
        println!("healthy");
        Ok(())
    } else {
        for problem in &problems {
            println!("UNHEALTHY: {problem}");
        }
        Err(Exit(UNHEALTHY).into())
    }
}

/// Whether yesterday is compacted, if it is due.
fn compaction(root: &Path, now: DateTime<Utc>) -> Result<Compaction> {
    if now.time() < COMPACTION_DUE {
        return Ok(Compaction::NotDue);
    }
    let date = now.date_naive() - TimeDelta::days(1);
    let manifest = root
        .join(PARQUET_DIR)
        .join(format!("date={date}"))
        .join("manifest.json");
    if manifest.exists() {
        return Ok(Compaction::Done);
    }
    let has_data = [Stream::Ws, Stream::Control]
        .into_iter()
        .map(|stream| files_for_date(root, stream, date, true))
        .collect::<std::io::Result<Vec<_>>>()?
        .iter()
        .any(|files| !files.is_empty());
    Ok(if has_data {
        Compaction::Missing { date }
    } else {
        Compaction::NotDue
    })
}

fn print_summary(s: &StatusFile, now: DateTime<Utc>) {
    let ago = |t: DateTime<Utc>| format_age(now - t);
    println!(
        "run            {} (version {})",
        s.run_id, s.program_version
    );
    println!(
        "started        {} ({} ago)",
        fmt(s.started_at),
        ago(s.started_at)
    );
    println!(
        "status written {} ago{}",
        ago(s.written_at),
        if s.clean_stop {
            ", stopped cleanly"
        } else {
            ""
        }
    );
    match s.last_message_at {
        Some(t) => println!("last message   {} ago", ago(t)),
        None => println!("last message   none"),
    }
    let streaming = s
        .connections
        .iter()
        .filter(|c| c.state == "streaming")
        .count();
    println!(
        "connections    {streaming} of {} streaming",
        s.connections.len()
    );
    for c in s.connections.iter().filter(|c| c.state != "streaming") {
        println!(
            "  {} {} for {} ({} tokens)",
            c.slot,
            c.state,
            ago(c.since),
            c.tokens
        );
    }
    println!("tokens         {} tracked", s.tokens_tracked);
    println!(
        "messages       {} in the last minute",
        s.messages_last_minute
    );
    println!(
        "write queue    {} of {} (highest {}), {} dropped in the last hour",
        s.queue.depth, s.queue.capacity, s.queue.high_water, s.queue.dropped_last_hour
    );
    println!("open gaps      {} tokens", s.open_gaps);
    println!("snapshots      {} in the last hour", s.snapshots_last_hour);
    match s.disk_free_gb {
        Some(gb) => println!("disk free      {gb:.1} GB"),
        None => println!("disk free      unknown"),
    }
    if !s.tokens_unhealthy.is_empty() {
        println!(
            "warning: {} tokens have had no book since subscribing:",
            s.tokens_unhealthy.len()
        );
        for t in s.tokens_unhealthy.iter().take(10) {
            println!("  {} since {}", t.token, fmt(t.since));
        }
    }
}

fn fmt(t: DateTime<Utc>) -> String {
    t.format("%Y-%m-%d %H:%M:%S UTC").to_string()
}

/// A duration as `1h 2m`, `3m 4s` or `5s`.
pub fn format_age(d: TimeDelta) -> String {
    let secs = d.num_seconds().max(0);
    match secs {
        s if s >= 86_400 => format!("{}d {}h", s / 86_400, s % 86_400 / 3600),
        s if s >= 3600 => format!("{}h {}m", s / 3600, s % 3600 / 60),
        s if s >= 60 => format!("{}m {}s", s / 60, s % 60),
        s => format!("{s}s"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(s: &str) -> DateTime<Utc> {
        s.parse().unwrap()
    }

    #[test]
    fn compaction_is_due_after_six_with_data() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        assert_eq!(
            compaction(root, at("2026-09-25T07:00:00Z")).unwrap(),
            Compaction::NotDue,
            "no data yesterday"
        );
        let control = root.join("control/date=2026-09-24");
        std::fs::create_dir_all(&control).unwrap();
        std::fs::write(control.join("r-1.jsonl"), "").unwrap();
        assert_eq!(
            compaction(root, at("2026-09-25T05:59:00Z")).unwrap(),
            Compaction::NotDue
        );
        let date = chrono::NaiveDate::from_ymd_opt(2026, 9, 24).unwrap();
        assert_eq!(
            compaction(root, at("2026-09-25T06:01:00Z")).unwrap(),
            Compaction::Missing { date }
        );
        let day = root.join("parquet/date=2026-09-24");
        std::fs::create_dir_all(&day).unwrap();
        std::fs::write(day.join("manifest.json"), "{}").unwrap();
        assert_eq!(
            compaction(root, at("2026-09-25T06:01:00Z")).unwrap(),
            Compaction::Done
        );
    }

    #[test]
    fn ages_read_well() {
        assert_eq!(format_age(TimeDelta::seconds(5)), "5s");
        assert_eq!(format_age(TimeDelta::seconds(185)), "3m 5s");
        assert_eq!(format_age(TimeDelta::seconds(7300)), "2h 1m");
        assert_eq!(format_age(TimeDelta::seconds(90_000)), "1d 1h");
    }
}
