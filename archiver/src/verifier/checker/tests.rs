//! The checker on records built in memory, including every row of the design's "Book
//! rebuild" table (section 14.3) that involves windows.

use std::collections::HashSet;

use rust_decimal_macros::dec;
use serde_json::{Value, json};

use super::*;
use crate::{
    book::tests::t,
    connection_manager::types::ConnSlotId,
    messages::{ControlBody, GapReason, SnapshotReason},
};

/// Exchange times in the tests are milliseconds after this.
pub(crate) const T0: i64 = 1_790_348_400_000;

type Levels<'a> = &'a [(&'a str, &'a str)];

fn levels_json(levels: Levels) -> Vec<Value> {
    levels
        .iter()
        .map(|(p, s)| json!({"price": p, "size": s}))
        .collect()
}

/// A websocket `book` at exchange time `T0 + ms`.
pub(crate) fn book_json(token: &str, ms: i64, bids: Levels, asks: Levels) -> Value {
    json!({"event_type":"book","asset_id":token,"market":"0xabc",
        "bids":levels_json(bids),"asks":levels_json(asks),
        "timestamp":(T0 + ms).to_string(),"hash":"h"})
}

/// A `/books` book, shaped as the exchange sends it.
pub(crate) fn snapshot_json(token: &str, ms: i64, bids: Levels, asks: Levels) -> Value {
    json!({"market":"0xabc","asset_id":token,"timestamp":(T0 + ms).to_string(),"hash":"h",
        "bids":levels_json(bids),"asks":levels_json(asks),"min_order_size":"5",
        "tick_size":"0.01","neg_risk":false,"last_trade_price":""})
}

/// A `price_change` with entries `(token, side, price, size)`.
pub(crate) fn changes_json(ms: i64, changes: &[(&str, &str, &str, &str)]) -> Value {
    let changes: Vec<Value> = changes
        .iter()
        .map(|(token, side, price, size)| {
            json!({"asset_id":token,"price":price,"size":size,"side":side,"hash":"h"})
        })
        .collect();
    json!({"event_type":"price_change","market":"0xabc",
        "timestamp":(T0 + ms).to_string(),"price_changes":changes})
}

fn tick_json(token: &str, ms: i64, new: &str) -> Value {
    json!({"event_type":"tick_size_change","asset_id":token,"market":"0xabc",
        "old_tick_size":"0.01","new_tick_size":new,"timestamp":(T0 + ms).to_string()})
}

fn token_set(tokens: &[&str]) -> HashSet<TokenId> {
    tokens.iter().map(|t| TokenId::new(*t)).collect()
}

pub(crate) fn gap_open(tokens: &[&str], ms: i64) -> ControlBody {
    ControlBody::GapOpen {
        tokens: token_set(tokens),
        start: t(T0 + ms),
        reason: GapReason::ConnectionClosed {
            error: "reset".into(),
        },
    }
}

pub(crate) fn gap_close(tokens: &[&str], ms: i64) -> ControlBody {
    ControlBody::GapClose {
        tokens: token_set(tokens),
        end: t(T0 + ms),
        reason: GapReason::Resumed,
    }
}

fn unsubscribe(tokens: &[&str]) -> ControlBody {
    ControlBody::Unsubscribe {
        slot: ConnSlotId::new("c-0001"),
        tokens: token_set(tokens),
    }
}

fn subscribe(tokens: &[&str]) -> ControlBody {
    ControlBody::Subscribe {
        slot: ConnSlotId::new("c-0001"),
        tokens: token_set(tokens),
    }
}

/// A day's records in archive order: each gets the next `seq` of its run. Receive times are
/// `T0 + recv_ms`.
struct Day {
    run: String,
    seq: u64,
    records: Vec<StoredRecord>,
    control: Vec<StoredRecord>,
}

impl Day {
    fn new() -> Self {
        Day {
            run: "r-1".into(),
            seq: 0,
            records: vec![],
            control: vec![],
        }
    }

    fn new_run(&mut self, run: &str) {
        self.run = run.into();
        self.seq = 0;
    }

    fn record(&mut self, recv_ms: i64, body: StoredBody) -> StoredRecord {
        self.seq += 1;
        StoredRecord {
            recv_ts: t(T0 + recv_ms),
            seq: self.seq,
            run_id: RunId::new(&self.run),
            conn_id: None,
            body,
        }
    }

    fn ws(&mut self, recv_ms: i64, event: Value) {
        let event = serde_json::from_value(event).unwrap();
        let record = self.record(recv_ms, StoredBody::Ws { event });
        self.records.push(record);
    }

    fn snapshot(&mut self, recv_ms: i64, books: Vec<Value>) {
        let body = StoredBody::Snapshot {
            reason: SnapshotReason::Scheduled,
            requested_tokens: vec![],
            books,
        };
        let record = self.record(recv_ms, body);
        self.records.push(record);
    }

    fn control(&mut self, recv_ms: i64, body: ControlBody) {
        let record = self.record(recv_ms, StoredBody::Control(body));
        self.control.push(record);
    }

    fn check(self) -> VerifyReport {
        let mut checker = Checker::new(Coverage::from_control(self.control));
        for record in &self.records {
            checker.begin_run(record.run_id.clone());
            checker.observe(record);
        }
        checker.finish(
            NaiveDate::from_ymd_opt(2026, 9, 25).unwrap(),
            InputStats::default(),
        )
    }
}

fn results(report: &VerifyReport) -> Vec<WindowResult> {
    report.windows.iter().map(|w| w.result.clone()).collect()
}

fn not_checkable(reason: NotCheckableReason) -> WindowResult {
    WindowResult::NotCheckable { reason }
}

fn last_mismatch(report: &VerifyReport) -> usize {
    match &report.windows.last().unwrap().result {
        WindowResult::Mismatch {
            differing_levels, ..
        } => *differing_levels,
        other => panic!("expected a mismatch, got {other:?}"),
    }
}

/// An anchor book, three changes (one event also changing another token) and a book that
/// matches them.
fn matching_day() -> Day {
    let mut day = Day::new();
    day.ws(
        50,
        book_json("A", 0, &[("0.08", "100"), ("0.07", "5")], &[("0.10", "20")]),
    );
    day.ws(
        1050,
        changes_json(
            1000,
            &[("A", "BUY", "0.08", "150"), ("B", "BUY", "0.5", "1")],
        ),
    );
    day.ws(2050, changes_json(2000, &[("A", "SELL", "0.11", "7")]));
    day.ws(3050, changes_json(3000, &[("A", "BUY", "0.07", "0")]));
    day.ws(
        4050,
        book_json(
            "A",
            4000,
            &[("0.08", "150")],
            &[("0.11", "7"), ("0.10", "20")],
        ),
    );
    day
}

// "Match".
#[test]
fn rebuilt_book_equal_to_the_next_book_matches() {
    let report = matching_day().check();
    assert_eq!(
        results(&report),
        [
            not_checkable(NotCheckableReason::NoAnchor),
            WindowResult::Match
        ]
    );
    let window = &report.windows[1];
    assert_eq!(window.token, TokenId::new("A"));
    assert_eq!(window.start, Some(t(T0)));
    assert_eq!(window.end, t(T0 + 4000));
    assert_eq!(window.changes, 3);
    assert_eq!(window.anchor, Some(BookSource::Ws));
    assert_eq!(window.target, BookSource::Ws);
    assert_eq!(report.tokens_checked, 1);
    assert_eq!(report.counts.matches, 1);
    assert_eq!(report.counts.mismatches, 0);
}

// "Mismatch": one delta deleted from the input.
#[test]
fn a_missing_delta_is_a_mismatch_with_token_and_window() {
    let mut day = matching_day();
    day.records.remove(2);
    let report = day.check();
    assert_eq!(last_mismatch(&report), 1);
    let window = report.windows.last().unwrap();
    assert_eq!(window.token, TokenId::new("A"));
    assert_eq!((window.start, window.end), (Some(t(T0)), t(T0 + 4000)));
    let WindowResult::Mismatch {
        first_differences, ..
    } = &window.result
    else {
        unreachable!()
    };
    assert_eq!(first_differences[0].side, BookSide::Ask);
    assert_eq!(first_differences[0].price, dec!(0.11));
    assert_eq!(first_differences[0].left, None);
    assert_eq!(first_differences[0].right, Some(dec!(7)));
    assert_eq!(report.counts.mismatches, 1);
}

#[test]
fn every_book_is_an_anchor() {
    let mut day = matching_day();
    day.ws(5050, changes_json(5000, &[("A", "BUY", "0.09", "1")]));
    // Missing the 0.09 bid.
    day.ws(
        6050,
        book_json(
            "A",
            6000,
            &[("0.08", "150")],
            &[("0.11", "7"), ("0.10", "20")],
        ),
    );
    day.ws(7050, changes_json(7000, &[("A", "BUY", "0.09", "2")]));
    day.ws(
        8050,
        book_json(
            "A",
            8000,
            &[("0.09", "2"), ("0.08", "150")],
            &[("0.11", "7"), ("0.10", "20")],
        ),
    );
    let report = day.check();
    assert_eq!(report.windows.len(), 4);
    assert!(matches!(
        report.windows[2].result,
        WindowResult::Mismatch { .. }
    ));
    // After a mismatch the target is the new anchor, so the next window matches.
    assert_eq!(report.windows[3].result, WindowResult::Match);
    assert_eq!(report.windows[3].start, Some(t(T0 + 6000)));
}

// "Gap respected".
#[test]
fn a_recorded_gap_makes_the_window_not_checkable() {
    let mut day = matching_day();
    day.records.remove(2);
    day.control(2500, gap_open(&["A", "B"], 2100));
    day.control(2600, gap_close(&["A", "B"], 2600));
    let report = day.check();
    assert_eq!(
        results(&report),
        [
            not_checkable(NotCheckableReason::NoAnchor),
            not_checkable(NotCheckableReason::Gap)
        ]
    );
    assert_eq!(report.counts.mismatches, 0);
}

#[test]
fn a_gap_left_open_makes_later_windows_not_checkable() {
    let mut day = matching_day();
    day.control(2500, gap_open(&["A"], 2100));
    day.ws(
        5050,
        book_json(
            "A",
            5000,
            &[("0.08", "150")],
            &[("0.11", "7"), ("0.10", "20")],
        ),
    );
    let report = day.check();
    assert_eq!(
        results(&report)[1..],
        [
            not_checkable(NotCheckableReason::Gap),
            not_checkable(NotCheckableReason::Gap)
        ]
    );
}

#[test]
fn a_gap_outside_the_window_or_for_another_token_does_not_matter() {
    let mut day = matching_day();
    day.control(9000, gap_open(&["A"], 5000));
    day.control(9500, gap_close(&["A"], 9500));
    day.control(2500, gap_open(&["B"], 2100));
    let report = day.check();
    assert_eq!(results(&report)[1], WindowResult::Match);
}

#[test]
fn an_unsubscribed_period_makes_the_window_not_checkable() {
    let mut day = matching_day();
    day.control(2500, unsubscribe(&["A"]));
    day.control(3500, subscribe(&["A"]));
    let report = day.check();
    assert_eq!(
        results(&report)[1],
        not_checkable(NotCheckableReason::Unsubscribed)
    );
}

// "Tick size change".
#[test]
fn a_tick_size_change_mid_window_does_not_stop_the_rebuild() {
    let mut day = Day::new();
    day.ws(50, book_json("A", 0, &[("0.50", "10")], &[("0.52", "5")]));
    day.ws(1050, tick_json("A", 1000, "0.001"));
    day.ws(
        2050,
        changes_json(
            2000,
            &[("A", "BUY", "0.505", "3"), ("A", "SELL", "0.515", "4")],
        ),
    );
    day.ws(
        3050,
        book_json(
            "A",
            3000,
            &[("0.505", "3"), ("0.5", "10")],
            &[("0.515", "4"), ("0.52", "5")],
        ),
    );
    assert_eq!(results(&day.check())[1], WindowResult::Match);
}

#[test]
fn a_window_across_runs_is_not_checkable() {
    let mut day = matching_day();
    day.records.pop();
    day.new_run("r-2");
    day.ws(9050, book_json("A", 9000, &[("0.01", "1")], &[]));
    day.ws(10050, changes_json(10000, &[("A", "BUY", "0.02", "1")]));
    day.ws(
        11050,
        book_json("A", 11000, &[("0.02", "1"), ("0.01", "1")], &[]),
    );
    let report = day.check();
    assert_eq!(
        results(&report),
        [
            not_checkable(NotCheckableReason::NoAnchor),
            not_checkable(NotCheckableReason::RunBoundary),
            WindowResult::Match
        ]
    );
}

#[test]
fn a_repeated_book_is_ignored() {
    let mut day = Day::new();
    day.ws(50, book_json("A", 0, &[("0.5", "10")], &[]));
    day.ws(60, book_json("A", 0, &[("0.50", "10")], &[]));
    day.ws(1050, changes_json(1000, &[("A", "BUY", "0.4", "1")]));
    day.ws(
        1060,
        book_json("A", 1000, &[("0.5", "10"), ("0.4", "1")], &[]),
    );
    day.ws(
        1070,
        book_json("A", 1000, &[("0.5", "10"), ("0.4", "1")], &[]),
    );
    let report = day.check();
    assert_eq!(
        results(&report),
        [
            not_checkable(NotCheckableReason::NoAnchor),
            WindowResult::Match
        ]
    );
}

#[test]
fn a_book_with_the_same_content_at_a_new_time_is_a_window() {
    let mut day = Day::new();
    day.ws(50, book_json("A", 0, &[("0.5", "10")], &[]));
    day.ws(1050, book_json("A", 1000, &[("0.5", "10")], &[]));
    assert_eq!(results(&day.check())[1], WindowResult::Match);
}

#[test]
fn an_unreadable_change_makes_the_window_not_checkable() {
    let mut day = matching_day();
    let target = day.records.pop().unwrap();
    day.ws(3500, changes_json(3450, &[("A", "HOLD", "0.3", "1")]));
    day.records.push(target);
    let report = day.check();
    assert_eq!(
        results(&report)[1],
        not_checkable(NotCheckableReason::UnreadableChange)
    );
}

#[test]
fn a_snapshot_target_leaves_out_changes_after_its_time_and_they_carry_over() {
    let mut day = Day::new();
    day.ws(50, book_json("A", 0, &[("0.5", "10")], &[]));
    day.ws(1050, changes_json(1000, &[("A", "BUY", "0.4", "1")]));
    // Recorded before the snapshot, but after its exchange time.
    day.ws(1250, changes_json(1200, &[("A", "BUY", "0.3", "2")]));
    day.snapshot(
        1300,
        vec![snapshot_json(
            "A",
            1100,
            &[("0.5", "10"), ("0.4", "1")],
            &[],
        )],
    );
    // The 0.3 bid is carried into the next window, as the snapshot does not have it.
    day.ws(2050, changes_json(2000, &[("A", "BUY", "0.2", "3")]));
    day.ws(
        2060,
        book_json(
            "A",
            2000,
            &[("0.5", "10"), ("0.4", "1"), ("0.3", "2"), ("0.2", "3")],
            &[],
        ),
    );
    let report = day.check();
    assert_eq!(
        results(&report),
        [
            not_checkable(NotCheckableReason::NoAnchor),
            WindowResult::Match,
            WindowResult::Match
        ]
    );
    assert_eq!(report.windows[1].target, BookSource::Snapshot);
    assert_eq!(report.windows[2].anchor, Some(BookSource::Snapshot));
    assert_eq!(report.windows[2].changes, 2);
}

#[test]
fn a_snapshot_anchor_reapplies_changes_at_its_own_time() {
    let mut day = Day::new();
    day.ws(1150, changes_json(1100, &[("A", "BUY", "0.3", "2")]));
    // Whether the snapshot includes the change at 1100 or not, the next window matches.
    day.snapshot(
        1200,
        vec![snapshot_json(
            "A",
            1100,
            &[("0.5", "10"), ("0.3", "2")],
            &[],
        )],
    );
    day.ws(
        2050,
        book_json("A", 2000, &[("0.5", "10"), ("0.3", "2")], &[]),
    );
    day.ws(2150, changes_json(2100, &[("A", "BUY", "0.3", "0")]));
    // Taken at 2100 without the change at 2100.
    day.snapshot(
        2200,
        vec![snapshot_json(
            "A",
            2100,
            &[("0.5", "10"), ("0.3", "2")],
            &[],
        )],
    );
    day.ws(3050, book_json("A", 3000, &[("0.5", "10")], &[]));
    let report = day.check();
    assert_eq!(
        results(&report),
        [
            not_checkable(NotCheckableReason::NoAnchor),
            WindowResult::Match,
            not_checkable(NotCheckableReason::AmbiguousOrder),
            // The removal at 2100 is applied again on top of the snapshot.
            WindowResult::Match
        ]
    );
    assert_eq!(report.windows[3].changes, 1);
}

#[test]
fn a_snapshot_target_differing_only_at_its_own_time_is_ambiguous() {
    let mut day = Day::new();
    day.ws(50, book_json("A", 0, &[("0.5", "10")], &[]));
    day.ws(1050, changes_json(1000, &[("A", "BUY", "0.4", "1")]));
    day.ws(1150, changes_json(1100, &[("A", "BUY", "0.3", "2")]));
    // Taken at 1100 without the change at 1100.
    day.snapshot(
        1200,
        vec![snapshot_json(
            "A",
            1100,
            &[("0.5", "10"), ("0.4", "1")],
            &[],
        )],
    );
    assert_eq!(
        results(&day.check())[1],
        not_checkable(NotCheckableReason::AmbiguousOrder)
    );
}

#[test]
fn a_snapshot_target_with_other_differences_is_a_mismatch() {
    let mut day = Day::new();
    day.ws(50, book_json("A", 0, &[("0.5", "10")], &[]));
    day.ws(1150, changes_json(1100, &[("A", "BUY", "0.3", "2")]));
    // Lacks the change at its own time, and has an unexplained level.
    day.snapshot(
        1200,
        vec![snapshot_json(
            "A",
            1100,
            &[("0.5", "10"), ("0.1", "1")],
            &[],
        )],
    );
    assert_eq!(last_mismatch(&day.check()), 2);
}

#[test]
fn a_websocket_target_takes_every_change_before_it() {
    let mut day = Day::new();
    day.ws(50, book_json("A", 0, &[("0.5", "10")], &[]));
    // Same exchange time as the book after it: archive order decides.
    day.ws(1050, changes_json(1000, &[("A", "BUY", "0.4", "1")]));
    day.ws(
        1060,
        book_json("A", 1000, &[("0.5", "10"), ("0.4", "1")], &[]),
    );
    assert_eq!(results(&day.check())[1], WindowResult::Match);
}

#[test]
fn a_snapshot_with_several_books_checks_each_and_skips_unusable_ones() {
    let mut day = Day::new();
    day.ws(50, book_json("A", 0, &[("0.5", "10")], &[]));
    day.ws(60, book_json("B", 0, &[("0.5", "10")], &[]));
    day.ws(
        1050,
        changes_json(1000, &[("A", "BUY", "0.4", "1"), ("B", "SELL", "0.6", "1")]),
    );
    let mut no_time = snapshot_json("B", 1100, &[], &[]);
    no_time["timestamp"] = json!("");
    day.snapshot(
        1300,
        vec![
            snapshot_json("A", 1100, &[("0.5", "10"), ("0.4", "1")], &[]),
            json!({"asset_id":"B","bids":"oops"}),
            no_time,
            snapshot_json("B", 1100, &[("0.5", "10")], &[("0.6", "1")]),
        ],
    );
    let report = day.check();
    assert_eq!(report.tokens_checked, 2);
    assert_eq!(report.counts.matches, 2);
    assert_eq!(report.input.unusable_snapshot_books, 2);
}
