use std::collections::{BTreeMap, HashMap, HashSet};

use anyhow::Result;
use chrono::{DateTime, NaiveDate, TimeDelta, Utc};
use clap::Args;
use marcasite::types::TokenId;

use crate::{
    archive::{
        envelope::{StoredBody, StoredRecord},
        layout::{Stream, files_for_date},
        reader::read_records,
    },
    cli::CommonArgs,
    commands::status::format_age,
    config::Config,
    messages::{ControlBody, GapReason},
    types::RunId,
};

#[derive(Args)]
pub struct GapsArgs {
    #[command(flatten)]
    pub common: CommonArgs,

    /// The UTC date to list gaps for (default: today)
    #[arg(short, long, value_parser = crate::commands::parse_date::parse_date)]
    date: Option<NaiveDate>,

    /// Only these tokens; lists each token's gaps separately
    #[arg(short, long, value_delimiter = ',', num_args = 1..)]
    tokens: Option<Vec<String>>,
}

/// A recorded gap for a set of tokens.
#[derive(Debug, Clone, PartialEq)]
pub struct Gap {
    pub run_id: RunId,
    pub tokens: Vec<TokenId>,
    pub start: DateTime<Utc>,
    /// `None` if the run ended without closing it (a crash; the next run's `process_restart`
    /// gap covers the time after).
    pub end: Option<DateTime<Utc>>,
    pub reason: GapReason,
    pub closed_by: Option<GapReason>,
}

pub async fn execute(args: GapsArgs) -> Result<()> {
    let config = Config::load(&args.common.config)?;
    let date = args.date.unwrap_or_else(|| Utc::now().date_naive());

    // Gaps can span midnight, so read the days either side too.
    let mut records = vec![];
    let mut bad_lines = 0;
    for day in [date - TimeDelta::days(1), date, date + TimeDelta::days(1)] {
        for path in files_for_date(&config.archive_dir, Stream::Control, day, true)? {
            for line in read_records(&path)? {
                match line?.1 {
                    Ok(record) => records.push(record),
                    Err(_) => bad_lines += 1,
                }
            }
        }
    }
    if bad_lines > 0 {
        eprintln!("warning: {bad_lines} unreadable control lines skipped");
    }

    let filter: Option<HashSet<TokenId>> = args
        .tokens
        .map(|ids| ids.into_iter().map(TokenId::new).collect());
    let day_start = date.and_hms_opt(0, 0, 0).expect("midnight").and_utc();
    let day_end = day_start + TimeDelta::days(1);
    let mut gaps: Vec<Gap> = collect_gaps(records)
        .into_iter()
        .filter(|g| g.start < day_end && g.end.is_none_or(|end| end >= day_start))
        .collect();
    if let Some(filter) = &filter {
        gaps = gaps
            .into_iter()
            .flat_map(|g| {
                g.tokens
                    .iter()
                    .filter(|t| filter.contains(*t))
                    .map(|t| Gap {
                        tokens: vec![t.clone()],
                        ..g.clone()
                    })
                    .collect::<Vec<_>>()
            })
            .collect();
    }

    if gaps.is_empty() {
        println!("no gaps recorded on {date}");
        return Ok(());
    }
    println!(
        "{:<23}  {:<23}  {:>9}  {:>6}  {:<18}  closed by",
        "start (UTC)", "end (UTC)", "duration", "tokens", "reason"
    );
    for g in &gaps {
        let fmt = |t: DateTime<Utc>| t.format("%Y-%m-%d %H:%M:%S%.3f").to_string();
        let end = g.end.map(fmt).unwrap_or_else(|| "never closed".into());
        let duration = g
            .end
            .map(|end| format_age(end - g.start))
            .unwrap_or_else(|| "-".into());
        let token_col = match (&filter, g.tokens.as_slice()) {
            (Some(_), [t]) => t.to_string(),
            _ => g.tokens.len().to_string(),
        };
        println!(
            "{:<23}  {:<23}  {:>9}  {:>6}  {:<18}  {}",
            fmt(g.start),
            end,
            duration,
            token_col,
            g.reason.code(),
            g.closed_by.as_ref().map(GapReason::code).unwrap_or("-")
        );
    }
    let open = gaps.iter().filter(|g| g.end.is_none()).count();
    println!(
        "{} gaps on {date}{}",
        gaps.len(),
        if open > 0 {
            format!(", {open} never closed (the run ended without stopping cleanly)")
        } else {
            String::new()
        }
    );
    Ok(())
}

/// Pairs each run's `gap_open` and `gap_close` records, per token, and groups tokens that
/// share an opening and closing record.
pub fn collect_gaps(mut records: Vec<StoredRecord>) -> Vec<Gap> {
    records.sort_by(|a, b| (&a.run_id, a.seq).cmp(&(&b.run_id, b.seq)));

    // Open gaps per run and token: the seq of the opening record, its start and reason.
    let mut open: HashMap<(RunId, TokenId), (u64, DateTime<Utc>, GapReason)> = HashMap::new();
    // Grouped by (run, open seq, close seq).
    let mut grouped: BTreeMap<(RunId, u64, Option<u64>), Gap> = BTreeMap::new();

    for record in records {
        let StoredBody::Control(body) = record.body else {
            continue;
        };
        match body {
            ControlBody::GapOpen {
                tokens,
                start,
                reason,
            } => {
                for token in tokens {
                    open.entry((record.run_id.clone(), token)).or_insert((
                        record.seq,
                        start,
                        reason.clone(),
                    ));
                }
            }
            ControlBody::GapClose {
                tokens,
                end,
                reason,
            } => {
                for token in tokens {
                    let Some((seq, start, opened)) =
                        open.remove(&(record.run_id.clone(), token.clone()))
                    else {
                        continue;
                    };
                    grouped
                        .entry((record.run_id.clone(), seq, Some(record.seq)))
                        .or_insert_with(|| Gap {
                            run_id: record.run_id.clone(),
                            tokens: vec![],
                            start,
                            end: Some(end),
                            reason: opened,
                            closed_by: Some(reason.clone()),
                        })
                        .tokens
                        .push(token);
                }
            }
            _ => {}
        }
    }
    for ((run_id, token), (seq, start, reason)) in open {
        grouped
            .entry((run_id.clone(), seq, None))
            .or_insert_with(|| Gap {
                run_id,
                tokens: vec![],
                start,
                end: None,
                reason,
                closed_by: None,
            })
            .tokens
            .push(token);
    }

    let mut gaps: Vec<Gap> = grouped.into_values().collect();
    for g in &mut gaps {
        g.tokens.sort();
    }
    gaps.sort_by(|a, b| (a.start, &a.run_id).cmp(&(b.start, &b.run_id)));
    gaps
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(s: &str) -> DateTime<Utc> {
        s.parse().unwrap()
    }

    fn record(run: &str, seq: u64, body: ControlBody) -> StoredRecord {
        StoredRecord {
            recv_ts: at("2026-09-25T10:00:00Z"),
            seq,
            run_id: RunId::new(run),
            conn_id: None,
            body: StoredBody::Control(body),
        }
    }

    fn tokens(ids: &[&str]) -> HashSet<TokenId> {
        ids.iter().map(|t| TokenId::new(*t)).collect()
    }

    #[test]
    fn pairs_opens_and_closes_per_run() {
        let closed = GapReason::ConnectionClosed {
            error: "reset".into(),
        };
        let records = vec![
            record(
                "r-1",
                5,
                ControlBody::GapOpen {
                    tokens: tokens(&["1", "2"]),
                    start: at("2026-09-25T10:00:00Z"),
                    reason: closed.clone(),
                },
            ),
            record(
                "r-1",
                9,
                ControlBody::GapClose {
                    tokens: tokens(&["1", "2"]),
                    end: at("2026-09-25T10:00:07Z"),
                    reason: GapReason::Resumed,
                },
            ),
            // Another run's gap that never closed.
            record(
                "r-2",
                3,
                ControlBody::GapOpen {
                    tokens: tokens(&["3"]),
                    start: at("2026-09-25T11:00:00Z"),
                    reason: GapReason::HeartbeatTimeout,
                },
            ),
            // A close without an open is ignored.
            record(
                "r-2",
                4,
                ControlBody::GapClose {
                    tokens: tokens(&["9"]),
                    end: at("2026-09-25T11:00:00Z"),
                    reason: GapReason::Resumed,
                },
            ),
        ];
        let gaps = collect_gaps(records);
        assert_eq!(
            gaps,
            vec![
                Gap {
                    run_id: RunId::new("r-1"),
                    tokens: vec![TokenId::new("1"), TokenId::new("2")],
                    start: at("2026-09-25T10:00:00Z"),
                    end: Some(at("2026-09-25T10:00:07Z")),
                    reason: closed,
                    closed_by: Some(GapReason::Resumed),
                },
                Gap {
                    run_id: RunId::new("r-2"),
                    tokens: vec![TokenId::new("3")],
                    start: at("2026-09-25T11:00:00Z"),
                    end: None,
                    reason: GapReason::HeartbeatTimeout,
                    closed_by: None,
                },
            ]
        );
    }

    #[test]
    fn tokens_closed_separately_are_separate_gaps() {
        let records = vec![
            record(
                "r-1",
                1,
                ControlBody::GapOpen {
                    tokens: tokens(&["1", "2"]),
                    start: at("2026-09-25T10:00:00Z"),
                    reason: GapReason::HeartbeatTimeout,
                },
            ),
            record(
                "r-1",
                2,
                ControlBody::GapClose {
                    tokens: tokens(&["1"]),
                    end: at("2026-09-25T10:00:01Z"),
                    reason: GapReason::Unsubscribed,
                },
            ),
            record(
                "r-1",
                3,
                ControlBody::GapClose {
                    tokens: tokens(&["2"]),
                    end: at("2026-09-25T10:00:05Z"),
                    reason: GapReason::Resumed,
                },
            ),
        ];
        let gaps = collect_gaps(records);
        assert_eq!(gaps.len(), 2);
        assert_eq!(gaps[0].closed_by, Some(GapReason::Unsubscribed));
        assert_eq!(gaps[1].end, Some(at("2026-09-25T10:00:05Z")));
    }
}
