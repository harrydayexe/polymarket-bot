//! `verify_day` against archive folders written in the recorder's line format and layout.

use std::{collections::BTreeMap, sync::atomic::AtomicBool};

use chrono::{DateTime, Utc};
use serde_json::Value;

use super::*;
use crate::{
    archive::{
        compress::compress_file,
        envelope::encode,
        layout::{DONE_SUFFIX, FileKey, OPEN_SUFFIX, file_stem},
    },
    book::tests::t,
    connection_manager::types::ConnAttemptId,
    messages::{ControlBody, Record, SnapshotReason},
    types::RunId,
    verifier::{
        checker::tests::{T0, book_json, changes_json, gap_close, gap_open, snapshot_json},
        report::{NotCheckableReason, WindowResult},
    },
};

/// Writes records as the recorder would: one file per run and stream (and hour), lines in
/// `seq` order.
struct Archive {
    dir: tempfile::TempDir,
    run: RunId,
    seq: u64,
    files: BTreeMap<PathBuf, Vec<u8>>,
}

impl Archive {
    fn new() -> Self {
        Archive {
            dir: tempfile::tempdir().unwrap(),
            run: RunId::new("r-20260925-1500-a1b2"),
            seq: 0,
            files: BTreeMap::new(),
        }
    }

    fn root(&self) -> &Path {
        self.dir.path()
    }

    fn new_run(&mut self, run: &str) {
        self.run = RunId::new(run);
        self.seq = 0;
    }

    fn push(&mut self, record: Record) {
        self.seq += 1;
        let key = FileKey::for_time(record.stream(), record.recv_ts());
        let path = key
            .dir(self.dir.path())
            .join(format!("{}{DONE_SUFFIX}", file_stem(&self.run, 1)));
        let out = self.files.entry(path).or_default();
        encode(&record, self.seq, &self.run, out).unwrap();
    }

    fn at(recv_ms: i64) -> DateTime<Utc> {
        t(T0 + recv_ms)
    }

    fn ws(&mut self, recv_ms: i64, event: Value) {
        self.push(Record::WsFrame {
            recv_ts: Self::at(recv_ms),
            conn_id: ConnAttemptId::new("c-0001-1"),
            event: serde_json::from_value(event).unwrap(),
        });
    }

    fn snapshot(&mut self, recv_ms: i64, books: Vec<Value>) {
        self.push(Record::Snapshot {
            recv_ts: Self::at(recv_ms),
            requested_tokens: vec![],
            reason: SnapshotReason::Scheduled,
            books,
        });
    }

    fn control(&mut self, recv_ms: i64, body: ControlBody) {
        self.push(Record::Control {
            recv_ts: Self::at(recv_ms),
            conn_id: None,
            body,
        });
    }

    /// Writes the files out.
    fn write(&self) {
        for (path, text) in &self.files {
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, text).unwrap();
        }
    }

    fn date(&self) -> NaiveDate {
        Self::at(0).date_naive()
    }

    fn verify(&self) -> VerifyReport {
        self.write();
        verify_day(self.root(), self.date()).unwrap()
    }
}

fn results(report: &VerifyReport) -> Vec<WindowResult> {
    report.windows.iter().map(|w| w.result.clone()).collect()
}

fn not_checkable(reason: NotCheckableReason) -> WindowResult {
    WindowResult::NotCheckable { reason }
}

const HOUR: i64 = 3_600_000;

/// Token A over two hours: a book, changes, a snapshot an hour later that matches, a gap, and
/// a book after it. Token B: a book and a snapshot that is missing a change.
fn day() -> Archive {
    let mut a = Archive::new();
    a.ws(50, book_json("A", 0, &[("0.5", "10")], &[("0.6", "5")]));
    a.ws(60, book_json("B", 0, &[("0.2", "1")], &[]));
    a.ws(
        1050,
        changes_json(1000, &[("A", "BUY", "0.45", "3"), ("B", "BUY", "0.1", "4")]),
    );
    a.ws(HOUR + 50, changes_json(HOUR, &[("A", "SELL", "0.6", "0")]));
    a.snapshot(
        HOUR + 500,
        vec![
            snapshot_json("A", HOUR + 100, &[("0.5", "10"), ("0.45", "3")], &[]),
            // Missing B's 0.1 bid.
            snapshot_json("B", HOUR + 100, &[("0.2", "1")], &[]),
        ],
    );
    a.control(HOUR + 2000, gap_open(&["A", "B"], HOUR + 1000));
    a.control(HOUR + 3000, gap_close(&["A", "B"], HOUR + 3000));
    a.ws(
        HOUR + 3050,
        book_json("A", HOUR + 3000, &[("0.3", "1")], &[]),
    );
    a
}

#[test]
fn checks_a_day_across_hours_and_streams() {
    let a = day();
    let report = a.verify();
    assert_eq!(report.date, a.date());
    assert_eq!(report.tokens_checked, 2);
    let tokens: Vec<&str> = report.windows.iter().map(|w| w.token.as_str()).collect();
    assert_eq!(tokens, ["A", "B", "A", "B", "A"]);
    let results = results(&report);
    assert_eq!(results[0], not_checkable(NotCheckableReason::NoAnchor));
    assert_eq!(results[1], not_checkable(NotCheckableReason::NoAnchor));
    assert_eq!(results[2], WindowResult::Match);
    assert!(matches!(
        results[3],
        WindowResult::Mismatch {
            differing_levels: 1,
            ..
        }
    ));
    assert_eq!(results[4], not_checkable(NotCheckableReason::Gap));
    let b = &report.windows[3];
    assert_eq!(b.start, Some(t(T0)));
    assert_eq!(b.end, t(T0 + HOUR + 100));
    assert_eq!(report.counts.mismatches, 1);
    // Two ws hour files, one snapshot file, one control file.
    assert_eq!(report.input.files_read, 4);
    assert_eq!(report.input.records_read, 8);
    assert_eq!(report.input.malformed_lines, 0);
}

// "Mismatch": one delta deleted from the input, through the files.
#[test]
fn a_deleted_delta_in_the_files_is_a_mismatch() {
    let mut a = Archive::new();
    a.ws(50, book_json("A", 0, &[("0.08", "100")], &[]));
    a.ws(1050, changes_json(1000, &[("A", "BUY", "0.08", "150")]));
    a.ws(2050, book_json("A", 2000, &[("0.08", "150")], &[]));
    assert_eq!(results(&a.verify())[1], WindowResult::Match);

    let mut b = Archive::new();
    b.ws(50, book_json("A", 0, &[("0.08", "100")], &[]));
    b.seq += 1; // The change's line is gone.
    b.ws(2050, book_json("A", 2000, &[("0.08", "150")], &[]));
    let report = b.verify();
    assert!(matches!(
        report.windows[1].result,
        WindowResult::Mismatch {
            differing_levels: 1,
            ..
        }
    ));
}

#[test]
fn runs_are_read_in_order_of_their_first_record() {
    let mut a = Archive::new();
    // A run name that sorts after the second run's.
    a.new_run("r-z");
    a.ws(50, book_json("A", 0, &[("0.5", "1")], &[]));
    a.ws(1050, changes_json(1000, &[("A", "BUY", "0.4", "1")]));
    a.new_run("r-a");
    a.ws(5050, book_json("A", 5000, &[("0.5", "1")], &[]));
    a.ws(6050, changes_json(6000, &[("A", "BUY", "0.3", "1")]));
    a.ws(
        7050,
        book_json("A", 7000, &[("0.5", "1"), ("0.3", "1")], &[]),
    );
    assert_eq!(
        results(&a.verify()),
        [
            not_checkable(NotCheckableReason::NoAnchor),
            not_checkable(NotCheckableReason::RunBoundary),
            WindowResult::Match
        ]
    );
}

#[test]
fn records_of_a_run_are_merged_by_seq_across_files() {
    let mut a = Archive::new();
    a.ws(50, book_json("A", 0, &[("0.5", "10")], &[]));
    // The snapshot file comes first by path, but its record comes last by `seq`.
    a.ws(
        HOUR - 100,
        changes_json(HOUR - 150, &[("A", "BUY", "0.4", "1")]),
    );
    a.ws(HOUR + 50, changes_json(HOUR, &[("A", "BUY", "0.3", "1")]));
    a.snapshot(
        HOUR + 500,
        vec![snapshot_json(
            "A",
            HOUR + 100,
            &[("0.5", "10"), ("0.4", "1"), ("0.3", "1")],
            &[],
        )],
    );
    assert_eq!(results(&a.verify())[1], WindowResult::Match);
}

#[test]
fn unfinished_files_are_not_read() {
    let a = day();
    a.write();
    // Rename the second hour's websocket file to unfinished.
    let hour = FileKey::for_time(crate::archive::layout::Stream::Ws, Archive::at(HOUR));
    let done = hour
        .dir(a.root())
        .join(format!("{}{DONE_SUFFIX}", file_stem(&a.run, 1)));
    let open = hour
        .dir(a.root())
        .join(format!("{}{OPEN_SUFFIX}", file_stem(&a.run, 1)));
    std::fs::rename(&done, &open).unwrap();
    let report = verify_day(a.root(), a.date()).unwrap();
    assert_eq!(report.input.open_files_ignored, 1);
    // A's last book is gone with that file.
    assert_eq!(report.windows.len(), 4);
}

#[test]
fn compressed_files_are_read() {
    let a = day();
    a.write();
    for path in a.files.keys() {
        if !path.starts_with(a.root().join("control")) {
            assert!(compress_file(path, &AtomicBool::new(false)).unwrap());
        }
    }
    let plain = day().verify();
    let report = verify_day(a.root(), a.date()).unwrap();
    assert_eq!(report.windows, plain.windows);
    assert_eq!(report.input.records_read, 8);
}

#[test]
fn malformed_lines_are_counted_and_skipped() {
    let mut a = Archive::new();
    a.ws(50, book_json("A", 0, &[("0.5", "1")], &[]));
    a.ws(2050, book_json("A", 2000, &[("0.5", "1")], &[]));
    let path = a.files.keys().next().unwrap().clone();
    a.files
        .get_mut(&path)
        .unwrap()
        .extend_from_slice(b"{\"recv_ts\":1,\"se");
    let report = a.verify();
    assert_eq!(report.input.malformed_lines, 1);
    assert_eq!(report.input.records_read, 2);
    assert_eq!(results(&report)[1], WindowResult::Match);
}

#[test]
fn an_empty_day_gives_an_empty_report() {
    let a = Archive::new();
    let report = verify_day(a.root(), a.date()).unwrap();
    assert_eq!(report.tokens_checked, 0);
    assert!(report.windows.is_empty());
}

#[test]
fn a_missing_archive_is_an_error() {
    let a = Archive::new();
    let missing = a.root().join("nope");
    assert!(verify_day(&missing, a.date()).is_err());
}

#[test]
fn the_report_is_written_as_json() {
    let a = day();
    let report = a.verify();
    let path = write_report(a.root(), &report).unwrap();
    assert_eq!(
        path,
        a.root()
            .join("reports/verify")
            .join(format!("date={}.json", a.date().format("%Y-%m-%d")))
    );
    let value: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    assert_eq!(value["date"], serde_json::json!(a.date().to_string()));
    assert_eq!(value["tokens_checked"], serde_json::json!(2));
    assert_eq!(value["counts"]["mismatches"], serde_json::json!(1));
    assert_eq!(value["windows"].as_array().unwrap().len(), 5);
    // Writing again replaces it.
    write_report(a.root(), &report).unwrap();
    let entries: Vec<_> = std::fs::read_dir(path.parent().unwrap()).unwrap().collect();
    assert_eq!(entries.len(), 1);
}
