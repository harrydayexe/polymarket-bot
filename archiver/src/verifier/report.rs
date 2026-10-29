//! The `verify` report (design section 5.8, `VerifyReport`), written as JSON.
//!
//! Times are microseconds since the epoch, as in the archive.

use std::{collections::BTreeMap, fmt::Write as _};

use chrono::serde::{ts_microseconds, ts_microseconds_option};
use chrono::{DateTime, NaiveDate, SecondsFormat, Utc};
use marcasite::types::TokenId;
use serde::Serialize;

use crate::book::LevelDiff;

/// How many differing levels a mismatch lists.
pub const DIFFERENCES_LISTED: usize = 5;

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct VerifyReport {
    /// `YYYY-MM-DD`.
    pub date: NaiveDate,
    /// Tokens with at least one full book on the day.
    pub tokens_checked: usize,
    pub counts: WindowCounts,
    pub input: InputStats,
    pub windows: Vec<VerifyWindow>,
}

/// What was read.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct InputStats {
    /// Finished websocket, snapshot and control files read.
    pub files_read: usize,
    /// Records read from them.
    pub records_read: u64,
    /// Lines that were not a valid record (such as a line cut off by a crash).
    pub malformed_lines: u64,
    /// Snapshot books that could not be read or had no timestamp, and were skipped.
    pub unusable_snapshot_books: u64,
    /// `.jsonl.open` files of the day, not read.
    pub open_files_ignored: usize,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct WindowCounts {
    pub windows: usize,
    pub matches: usize,
    pub mismatches: usize,
    pub not_checkable: usize,
    pub not_checkable_by_reason: BTreeMap<NotCheckableReason, usize>,
}

impl WindowCounts {
    pub fn of(windows: &[VerifyWindow]) -> Self {
        let mut counts = WindowCounts {
            windows: windows.len(),
            ..Default::default()
        };
        for window in windows {
            match &window.result {
                WindowResult::Match => counts.matches += 1,
                WindowResult::Mismatch { .. } => counts.mismatches += 1,
                WindowResult::NotCheckable { reason } => {
                    counts.not_checkable += 1;
                    *counts.not_checkable_by_reason.entry(*reason).or_default() += 1;
                }
            }
        }
        counts
    }
}

/// Where a full book came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BookSource {
    /// A websocket `book` message.
    Ws,
    /// A REST snapshot.
    Snapshot,
}

/// One check: the book rebuilt from the anchor book and the level changes after it, compared
/// with the token's next full book (the target).
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct VerifyWindow {
    pub token: TokenId,
    /// Exchange time of the anchor book; `None` when there was none.
    #[serde(with = "ts_microseconds_option")]
    pub start: Option<DateTime<Utc>>,
    /// Exchange time of the target book.
    #[serde(with = "ts_microseconds")]
    pub end: DateTime<Utc>,
    pub anchor: Option<BookSource>,
    pub target: BookSource,
    /// Level changes recorded for the token in the window.
    pub changes: usize,
    #[serde(flatten)]
    pub result: WindowResult,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "result", rename_all = "snake_case")]
pub enum WindowResult {
    /// The rebuilt book equals the target.
    Match,
    /// The rebuilt book differs from the target in `differing_levels` levels, the first few
    /// of which are listed (`left` the rebuilt size, `right` the target's).
    Mismatch {
        differing_levels: usize,
        first_differences: Vec<LevelDiff>,
    },
    /// The window could not be checked.
    NotCheckable { reason: NotCheckableReason },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NotCheckableReason {
    /// The token had no full book earlier in the day (or earlier in the run's records).
    NoAnchor,
    /// The anchor is from an earlier run.
    RunBoundary,
    /// A recorded gap for the token overlaps the window, or one was left open.
    Gap,
    /// The token was not subscribed for part of the window.
    Unsubscribed,
    /// A level change for the token in the window could not be read.
    UnreadableChange,
    /// The target is a snapshot, and only levels changed at the snapshot's exact exchange
    /// time differ: whether the snapshot includes those changes cannot be told.
    AmbiguousOrder,
}

impl NotCheckableReason {
    fn describe(self) -> &'static str {
        match self {
            NotCheckableReason::NoAnchor => "no anchor",
            NotCheckableReason::RunBoundary => "run boundary",
            NotCheckableReason::Gap => "gap",
            NotCheckableReason::Unsubscribed => "unsubscribed",
            NotCheckableReason::UnreadableChange => "unreadable change",
            NotCheckableReason::AmbiguousOrder => "ambiguous order",
        }
    }
}

fn time(t: DateTime<Utc>) -> String {
    t.to_rfc3339_opts(SecondsFormat::Millis, true)
}

impl VerifyReport {
    pub fn new(
        date: NaiveDate,
        tokens_checked: usize,
        input: InputStats,
        windows: Vec<VerifyWindow>,
    ) -> Self {
        VerifyReport {
            date,
            tokens_checked,
            counts: WindowCounts::of(&windows),
            input,
            windows,
        }
    }

    /// A few lines for the terminal: the counts, then up to `max_mismatches` mismatches.
    pub fn summary(&self, max_mismatches: usize) -> String {
        let c = &self.counts;
        let mut out = String::new();
        let _ = writeln!(out, "verify {}", self.date);
        let _ = writeln!(out, "  tokens         {}", self.tokens_checked);
        let _ = writeln!(out, "  windows        {}", c.windows);
        let _ = writeln!(out, "  matches        {}", c.matches);
        let _ = writeln!(out, "  mismatches     {}", c.mismatches);
        let _ = write!(out, "  not checkable  {}", c.not_checkable);
        if !c.not_checkable_by_reason.is_empty() {
            let reasons: Vec<String> = c
                .not_checkable_by_reason
                .iter()
                .map(|(reason, n)| format!("{}: {n}", reason.describe()))
                .collect();
            let _ = write!(out, " ({})", reasons.join(", "));
        }
        out.push('\n');
        if self.input.malformed_lines > 0 {
            let _ = writeln!(
                out,
                "  malformed lines skipped: {}",
                self.input.malformed_lines
            );
        }
        if self.input.unusable_snapshot_books > 0 {
            let _ = writeln!(
                out,
                "  unusable snapshot books skipped: {}",
                self.input.unusable_snapshot_books
            );
        }

        let mismatches = self
            .windows
            .iter()
            .filter_map(|w| match &w.result {
                WindowResult::Mismatch {
                    differing_levels, ..
                } => Some((w, *differing_levels)),
                _ => None,
            })
            .take(max_mismatches)
            .collect::<Vec<_>>();
        if !mismatches.is_empty() {
            let _ = writeln!(out, "first mismatches:");
            for (w, differing) in mismatches {
                let start = w.start.map_or_else(|| "?".to_string(), time);
                let _ = writeln!(
                    out,
                    "  {} {} .. {}: {differing} differing level{}",
                    w.token,
                    start,
                    time(w.end),
                    if differing == 1 { "" } else { "s" }
                );
            }
            if c.mismatches > max_mismatches {
                let _ = writeln!(out, "  ... and {} more", c.mismatches - max_mismatches);
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal_macros::dec;
    use serde_json::json;

    use super::*;
    use crate::book::{BookSide, tests::t};

    fn window(result: WindowResult) -> VerifyWindow {
        VerifyWindow {
            token: TokenId::new("111"),
            start: Some(t(1_790_348_400_120)),
            end: t(1_790_348_460_000),
            anchor: Some(BookSource::Ws),
            target: BookSource::Snapshot,
            changes: 3,
            result,
        }
    }

    fn report() -> VerifyReport {
        VerifyReport::new(
            NaiveDate::from_ymd_opt(2026, 9, 25).unwrap(),
            1,
            InputStats::default(),
            vec![
                window(WindowResult::Match),
                window(WindowResult::Mismatch {
                    differing_levels: 1,
                    first_differences: vec![LevelDiff {
                        side: BookSide::Bid,
                        price: dec!(0.5),
                        left: Some(dec!(1)),
                        right: None,
                    }],
                }),
                window(WindowResult::NotCheckable {
                    reason: NotCheckableReason::Gap,
                }),
                window(WindowResult::NotCheckable {
                    reason: NotCheckableReason::Gap,
                }),
            ],
        )
    }

    #[test]
    fn counts_each_result() {
        let r = report();
        assert_eq!(r.counts.windows, 4);
        assert_eq!(r.counts.matches, 1);
        assert_eq!(r.counts.mismatches, 1);
        assert_eq!(r.counts.not_checkable, 2);
        assert_eq!(
            r.counts.not_checkable_by_reason,
            BTreeMap::from([(NotCheckableReason::Gap, 2)])
        );
    }

    #[test]
    fn serialises_as_designed() {
        let value = serde_json::to_value(report()).unwrap();
        assert_eq!(value["date"], json!("2026-09-25"));
        assert_eq!(value["tokens_checked"], json!(1));
        assert_eq!(
            value["counts"]["not_checkable_by_reason"],
            json!({"gap": 2})
        );
        assert_eq!(
            value["windows"][0],
            json!({"token":"111","start":1_790_348_400_120_000_i64,
                   "end":1_790_348_460_000_000_i64,"anchor":"ws","target":"snapshot",
                   "changes":3,"result":"match"})
        );
        assert_eq!(value["windows"][1]["result"], json!("mismatch"));
        assert_eq!(value["windows"][1]["differing_levels"], json!(1));
        assert_eq!(
            value["windows"][1]["first_differences"],
            json!([{"side":"bid","price":"0.5","left":"1","right":null}])
        );
        assert_eq!(
            value["windows"][2],
            json!({"token":"111","start":1_790_348_400_120_000_i64,
                   "end":1_790_348_460_000_000_i64,"anchor":"ws","target":"snapshot",
                   "changes":3,"result":"not_checkable","reason":"gap"})
        );
    }

    #[test]
    fn summary_lists_counts_and_mismatches() {
        let text = report().summary(20);
        assert!(text.contains("tokens         1"), "{text}");
        assert!(text.contains("mismatches     1"), "{text}");
        assert!(text.contains("not checkable  2 (gap: 2)"), "{text}");
        assert!(
            text.contains(
                "111 2026-09-25T15:00:00.120Z .. 2026-09-25T15:01:00.000Z: 1 differing level\n"
            ),
            "{text}"
        );
        assert!(!report().summary(0).contains("first mismatches"));
    }
}
