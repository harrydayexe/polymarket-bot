//! Compaction of a finished UTC day of raw files into Parquet tables (section 11).
//!
//! [compact_day] reads every finished raw file of the day (all four streams, files in path
//! order, lines in file order), splits websocket and snapshot records into events
//! ([route]) and writes one Parquet file per table ([tables]) plus a `manifest.json` into a
//! temporary folder, `parquet/.tmp-date=D-<unique>`. Rows are built into Arrow arrays a
//! batch at a time, so a day never has to fit in memory. Only when every line and event is
//! accounted for is the folder synced and swapped into place as `parquet/date=D`.
//!
//! The swap is two renames: an existing `date=D` is first moved aside to
//! `.old-date=D-<unique>`, then the new folder takes its place, then the old one is
//! deleted. A crash between the two renames leaves no `date=D`; the next compaction of the
//! day moves the old folder back before doing anything else. Leftover `.tmp-` folders from
//! a crash mid-write are never read and are deleted by the next compaction of their day.
//! Two compactions of the same day must not run at the same time.
//!
//! Raw retention (compressing raw files after a successful compaction and deleting them
//! after a configurable period) is deliberately not done yet: the raw files stay the
//! source of truth, and nothing here deletes or changes them.

mod route;
mod tables;
#[cfg(test)]
mod tests;

use std::{
    collections::BTreeMap,
    fs::File,
    io::Write,
    path::{Path, PathBuf},
};

use anyhow::Context;
use chrono::{NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use self::{
    route::{Event, control_row, market_tick_size, snapshot_events, ws_events},
    tables::{TableWriter, Val},
};
use crate::archive::{
    envelope::{StoredBody, StoredRecord, decode},
    layout::{DONE_SUFFIX, OPEN_SUFFIX, PARQUET_DIR, Stream, files_for_date},
    reader::read_lines,
};

pub use tables::TABLES;

pub const MANIFEST_FILE: &str = "manifest.json";

/// Why a day was not compacted. Nothing was changed.
#[derive(Debug, Error, PartialEq)]
pub enum Refusal {
    #[error("{date} is not over yet (today is {today} UTC)")]
    DayNotOver { date: NaiveDate, today: NaiveDate },
    #[error("{date} is not finished: {} file(s) still open, such as {}", files.len(), files[0].display())]
    OpenFiles {
        date: NaiveDate,
        files: Vec<PathBuf>,
    },
    #[error("no raw files for {date}")]
    NoRawFiles { date: NaiveDate },
    #[error("accounting check failed for {date}: {detail}")]
    Accounting { date: NaiveDate, detail: String },
}

/// Settings of one compaction. Only tests change them.
#[derive(Debug, Default, Clone)]
pub struct Options {
    /// Test-only: silently drop the first event row, to prove the accounting check fails.
    #[cfg(test)]
    pub lose_one_row: bool,
}

/// One raw file read for the day.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InputFile {
    /// Relative to the archive root.
    pub path: String,
    /// Non-blank lines.
    pub lines: u64,
    /// Lines that could not be read as a record.
    pub malformed_lines: u64,
}

/// The totals the accounting check compares.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Accounting {
    /// Non-blank lines read.
    pub lines_read: u64,
    pub malformed_lines: u64,
    /// Events split out of websocket and snapshot records, a book counting as one.
    pub events_extracted: u64,
    /// Books written (each is one or more rows of `books`).
    pub books: u64,
}

impl Accounting {
    fn count(&mut self, event: &Event) {
        self.events_extracted += 1;
        if matches!(event, Event::Book(_)) {
            self.books += 1;
        }
    }
}

/// `manifest.json` of a compacted day (section 5.8's `DayManifest`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DayManifest {
    pub date: String,
    pub program_version: String,
    /// Microseconds since the epoch, UTC.
    pub compacted_at: i64,
    pub inputs: Vec<InputFile>,
    pub row_counts: BTreeMap<String, u64>,
    pub malformed_lines: u64,
    pub accounting: Accounting,
    pub accounting_ok: bool,
}

/// `<root>/parquet/date=YYYY-MM-DD`.
pub fn day_dir(root: &Path, date: NaiveDate) -> PathBuf {
    root.join(PARQUET_DIR).join(day_name(date))
}

fn day_name(date: NaiveDate) -> String {
    format!("date={}", date.format("%Y-%m-%d"))
}

/// Whether `date` has been compacted (its manifest exists).
pub fn is_compacted(root: &Path, date: NaiveDate) -> bool {
    day_dir(root, date).join(MANIFEST_FILE).is_file()
}

fn raw_files(root: &Path, date: NaiveDate, include_open: bool) -> anyhow::Result<Vec<PathBuf>> {
    let mut files = vec![];
    for stream in Stream::ALL {
        files.extend(
            files_for_date(root, stream, date, include_open)
                .with_context(|| format!("listing {stream:?} files for {date}"))?,
        );
    }
    files.sort();
    Ok(files)
}

fn open_files(root: &Path, date: NaiveDate) -> anyhow::Result<Vec<PathBuf>> {
    Ok(raw_files(root, date, true)?
        .into_iter()
        .filter(|p| p.to_string_lossy().ends_with(OPEN_SUFFIX))
        .collect())
}

/// Refuses a day that is not over or still has open files.
pub fn check_finished(root: &Path, date: NaiveDate, today: NaiveDate) -> anyhow::Result<()> {
    if date >= today {
        return Err(Refusal::DayNotOver { date, today }.into());
    }
    let open = open_files(root, date)?;
    if !open.is_empty() {
        return Err(Refusal::OpenFiles { date, files: open }.into());
    }
    Ok(())
}

/// The days from `since` up to the day before `today` that have raw files and no manifest.
pub fn pending_days(
    root: &Path,
    since: NaiveDate,
    today: NaiveDate,
) -> anyhow::Result<Vec<NaiveDate>> {
    if since >= today {
        return Err(Refusal::DayNotOver { date: since, today }.into());
    }
    let mut days = vec![];
    for date in since.iter_days().take_while(|d| *d < today) {
        if !raw_files(root, date, true)?.is_empty() && !is_compacted(root, date) {
            days.push(date);
        }
    }
    Ok(days)
}

/// Compacts `date`, replacing any earlier compaction of it. `today` is the current UTC
/// date. Returns the manifest written.
///
/// # Errors
///
/// A [Refusal] if the day is not over, has open files, has no raw files or fails the
/// accounting check; or an I/O error. Either way any earlier output for the day is left
/// as it was.
pub fn compact_day(
    root: &Path,
    date: NaiveDate,
    today: NaiveDate,
    options: &Options,
) -> anyhow::Result<DayManifest> {
    check_finished(root, date, today)?;
    let inputs = raw_files(root, date, false)?;
    if inputs.is_empty() {
        return Err(Refusal::NoRawFiles { date }.into());
    }

    let parquet = root.join(PARQUET_DIR);
    std::fs::create_dir_all(&parquet).with_context(|| format!("creating {}", parquet.display()))?;
    recover(&parquet, date)?;

    let unique = format!(
        "{}-{}",
        std::process::id(),
        Utc::now().timestamp_nanos_opt().unwrap_or_default()
    );
    let tmp = parquet.join(format!(".tmp-{}-{unique}", day_name(date)));
    std::fs::create_dir(&tmp).with_context(|| format!("creating {}", tmp.display()))?;

    let manifest = match write_day(root, date, &inputs, &tmp, options) {
        Ok(manifest) => manifest,
        Err(e) => {
            let _ = std::fs::remove_dir_all(&tmp);
            return Err(e);
        }
    };

    let day = day_dir(root, date);
    let old = parquet.join(format!(".old-{}-{unique}", day_name(date)));
    let had_old = day.exists();
    if had_old {
        std::fs::rename(&day, &old).with_context(|| format!("moving {} aside", day.display()))?;
    }
    if let Err(e) = std::fs::rename(&tmp, &day) {
        if had_old {
            let _ = std::fs::rename(&old, &day);
        }
        let _ = std::fs::remove_dir_all(&tmp);
        return Err(e).with_context(|| format!("moving {} into place", tmp.display()));
    }
    sync_dir(&parquet)?;
    if had_old {
        std::fs::remove_dir_all(&old).with_context(|| format!("removing {}", old.display()))?;
    }
    Ok(manifest)
}

/// Undoes what a crashed compaction of `date` left behind: brings back an output moved
/// aside but not replaced, and deletes temporary and moved-aside folders.
fn recover(parquet: &Path, date: NaiveDate) -> anyhow::Result<()> {
    let tmp_prefix = format!(".tmp-{}-", day_name(date));
    let old_prefix = format!(".old-{}-", day_name(date));
    let day = parquet.join(day_name(date));
    let mut entries: Vec<PathBuf> = std::fs::read_dir(parquet)?
        .map(|e| e.map(|e| e.path()))
        .collect::<Result<_, _>>()?;
    entries.sort();
    for path in entries {
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if name.starts_with(&tmp_prefix) {
            tracing::warn!(
                "removing {} left by an interrupted compaction",
                path.display()
            );
            std::fs::remove_dir_all(&path)?;
        } else if name.starts_with(&old_prefix) {
            if day.exists() {
                std::fs::remove_dir_all(&path)?;
            } else {
                tracing::warn!(
                    "restoring {} moved aside by an interrupted compaction",
                    path.display()
                );
                std::fs::rename(&path, &day)?;
            }
        }
    }
    Ok(())
}

fn sync_dir(dir: &Path) -> std::io::Result<()> {
    File::open(dir)?.sync_all()
}

/// Lines of a raw file (plain or zstd) with their numbers (from 1), skipping blank lines as
/// the archive reader does; the raw bytes are kept so the `frames` table can store each line
/// exactly.
fn raw_lines(
    path: &Path,
) -> std::io::Result<impl Iterator<Item = std::io::Result<(u64, Vec<u8>)>> + use<'_>> {
    Ok(read_lines(path)?
        .enumerate()
        .filter_map(|(i, line)| match line {
            Ok(line) if line.iter().all(u8::is_ascii_whitespace) => None,
            Ok(line) => Some(Ok((i as u64 + 1, line))),
            Err(e) => Some(Err(e)),
        }))
}

/// The `.jsonl.zst` the recorder makes of a finished `.jsonl`, removing the original.
fn compressed_copy(path: &Path) -> Option<PathBuf> {
    path.to_string_lossy()
        .ends_with(DONE_SUFFIX)
        .then(|| PathBuf::from(format!("{}.zst", path.display())))
}

/// Envelope columns shared by every row from one record.
struct Env<'a> {
    recv_ts: i64,
    seq: u64,
    run_id: &'a str,
    conn_id: Option<&'a str>,
    source: &'static str,
    file: &'a str,
    line_no: u64,
}

impl<'a> Env<'a> {
    fn of(record: &'a StoredRecord, source: &'static str, file: &'a str, line_no: u64) -> Self {
        Env {
            recv_ts: record.recv_ts.timestamp_micros(),
            seq: record.seq,
            run_id: record.run_id.as_str(),
            conn_id: record.conn_id.as_ref().map(|c| c.as_str()),
            source,
            file,
            line_no,
        }
    }
}

struct Tables {
    frames: TableWriter,
    books: TableWriter,
    price_changes: TableWriter,
    trades: TableWriter,
    tick_sizes: TableWriter,
    best_bid_ask: TableWriter,
    lifecycle: TableWriter,
    control: TableWriter,
    markets: TableWriter,
    unparsed: TableWriter,
    /// Books written; each is one or more rows of `books`.
    books_written: u64,
    /// Rows of `unparsed` for malformed lines rather than events.
    malformed_rows: u64,
    lose_one_row: bool,
}

impl Tables {
    fn create(dir: &Path, options: &Options) -> anyhow::Result<Self> {
        use tables::*;
        #[cfg(test)]
        let lose_one_row = options.lose_one_row;
        #[cfg(not(test))]
        let lose_one_row = {
            let _ = options;
            false
        };
        Ok(Self {
            frames: TableWriter::create(dir, "frames", FRAMES)?,
            books: TableWriter::create(dir, "books", BOOKS)?,
            price_changes: TableWriter::create(dir, "price_changes", PRICE_CHANGES)?,
            trades: TableWriter::create(dir, "trades", TRADES)?,
            tick_sizes: TableWriter::create(dir, "tick_sizes", TICK_SIZES)?,
            best_bid_ask: TableWriter::create(dir, "best_bid_ask", BEST_BID_ASK)?,
            lifecycle: TableWriter::create(dir, "lifecycle", LIFECYCLE)?,
            control: TableWriter::create(dir, "control", CONTROL)?,
            markets: TableWriter::create(dir, "markets", MARKETS)?,
            unparsed: TableWriter::create(dir, "unparsed", UNPARSED)?,
            books_written: 0,
            malformed_rows: 0,
            lose_one_row,
        })
    }

    fn frame(&mut self, env: &Env, line: &str) -> anyhow::Result<()> {
        self.frames.push(&[
            Val::Ts(Some(env.recv_ts)),
            Val::U64(Some(env.seq)),
            Val::Str(Some(env.run_id)),
            Val::Str(env.conn_id),
            Val::Str(Some(env.source)),
            Val::Str(Some(line)),
        ])
    }

    fn malformed(
        &mut self,
        file: &str,
        line_no: u64,
        bytes: &[u8],
        error: &str,
    ) -> anyhow::Result<()> {
        let text = String::from_utf8_lossy(bytes);
        let not_utf8 = std::str::from_utf8(bytes).is_err();
        self.unparsed.push(&[
            Val::Ts(None),
            Val::U64(None),
            Val::Str(None),
            Val::Str(None),
            Val::Str(Some("malformed")),
            Val::Str(Some(file)),
            Val::U64(Some(line_no)),
            Val::Str(Some(error)),
            Val::Str(Some(&text)),
            Val::Bin(not_utf8.then_some(bytes)),
        ])?;
        self.malformed_rows += 1;
        Ok(())
    }

    fn event(&mut self, env: &Env, event: &Event) -> anyhow::Result<()> {
        if self.lose_one_row {
            self.lose_one_row = false;
            return Ok(());
        }
        let ts = Val::Ts(Some(env.recv_ts));
        let seq = Val::U64(Some(env.seq));
        let run = Val::Str(Some(env.run_id));
        let conn = Val::Str(env.conn_id);
        match event {
            Event::Book(b) => {
                let book_id = format!("{}:{}:{}", env.run_id, env.seq, b.index);
                let mut row = |side: Option<&str>, level: Option<&(i128, i128)>| {
                    self.books.push(&[
                        Val::Str(Some(&book_id)),
                        Val::Str(Some(source_name(env.source))),
                        ts.clone(),
                        Val::Ts(b.exchange_ts),
                        Val::Str(Some(&b.asset_id)),
                        Val::Str(b.market.as_deref()),
                        Val::Str(side),
                        Val::Dec(level.map(|l| l.0)),
                        Val::Dec(level.map(|l| l.1)),
                        Val::Str(b.hash.as_deref()),
                        seq.clone(),
                        run.clone(),
                        conn.clone(),
                    ])
                };
                if b.bids.is_empty() && b.asks.is_empty() {
                    row(None, None)?;
                }
                for level in &b.bids {
                    row(Some("bid"), Some(level))?;
                }
                for level in &b.asks {
                    row(Some("ask"), Some(level))?;
                }
                self.books_written += 1;
                Ok(())
            }
            Event::PriceChange(c) => self.price_changes.push(&[
                ts,
                Val::Ts(Some(c.exchange_ts)),
                seq,
                run,
                conn,
                Val::Str(Some(c.asset_id)),
                Val::Str(Some(c.market)),
                Val::Str(Some(c.side)),
                Val::Dec(Some(c.price)),
                Val::Dec(Some(c.size)),
                Val::Dec(c.best_bid),
                Val::Dec(c.best_ask),
                Val::Str(Some(c.hash)),
            ]),
            Event::Trade(t) => self.trades.push(&[
                ts,
                Val::Ts(Some(t.exchange_ts)),
                seq,
                run,
                conn,
                Val::Str(Some(t.asset_id)),
                Val::Str(Some(t.market)),
                Val::Dec(Some(t.price)),
                Val::Dec(Some(t.size)),
                Val::Str(Some(t.side)),
                Val::Dec(t.fee_rate_bps),
                Val::Str(t.transaction_hash),
            ]),
            Event::TickSize(t) => self.tick_sizes.push(&[
                ts,
                Val::Ts(t.exchange_ts),
                Val::Str(Some(t.exchange_ts_raw)),
                seq,
                run,
                conn,
                Val::Str(Some(t.asset_id)),
                Val::Str(Some(t.market)),
                Val::Dec(Some(t.old_tick_size)),
                Val::Dec(Some(t.new_tick_size)),
            ]),
            Event::BestBidAsk(b) => self.best_bid_ask.push(&[
                ts,
                Val::Ts(b.exchange_ts),
                Val::Str(Some(b.exchange_ts_raw)),
                seq,
                run,
                conn,
                Val::Str(Some(b.asset_id)),
                Val::Str(Some(b.market)),
                Val::Dec(b.best_bid),
                Val::Dec(b.best_ask),
                Val::Dec(b.spread),
            ]),
            Event::Lifecycle(l) => self.lifecycle.push(&[
                ts,
                Val::Ts(l.exchange_ts),
                Val::Str(Some(l.exchange_ts_raw)),
                seq,
                run,
                conn,
                Val::Str(Some(l.kind)),
                Val::Str(Some(l.market)),
                Val::Str(Some(l.market_id)),
                Val::StrList(Some(l.asset_ids.clone())),
                Val::Str(l.question),
                Val::Str(l.slug),
                Val::StrList(l.outcomes.clone()),
                Val::Str(l.winning_asset_id),
                Val::Str(l.winning_outcome),
                Val::StrList(l.tags.clone()),
                Val::Str(Some(&l.event)),
            ]),
            Event::Unparsed { error, raw } => self.unparsed.push(&[
                ts,
                seq,
                run,
                conn,
                Val::Str(Some(env.source)),
                Val::Str(Some(env.file)),
                Val::U64(Some(env.line_no)),
                Val::Str(Some(error)),
                Val::Str(Some(raw)),
                Val::Bin(None),
            ]),
        }
    }

    fn record(
        &mut self,
        record: &StoredRecord,
        line: &str,
        file: &str,
        line_no: u64,
        acc: &mut Accounting,
    ) -> anyhow::Result<()> {
        match &record.body {
            StoredBody::Ws { event } => {
                let env = Env::of(record, "ws", file, line_no);
                self.frame(&env, line)?;
                for e in ws_events(event) {
                    acc.count(&e);
                    self.event(&env, &e)?;
                }
            }
            StoredBody::Snapshot { books, .. } => {
                let env = Env::of(record, "snapshot", file, line_no);
                self.frame(&env, line)?;
                for e in snapshot_events(books) {
                    acc.count(&e);
                    self.event(&env, &e)?;
                }
            }
            StoredBody::Control(body) => {
                let row = control_row(body);
                let tokens = row
                    .tokens
                    .as_ref()
                    .map(|t| t.iter().map(String::as_str).collect());
                self.control.push(&[
                    Val::Ts(Some(record.recv_ts.timestamp_micros())),
                    Val::U64(Some(record.seq)),
                    Val::Str(Some(record.run_id.as_str())),
                    Val::Str(record.conn_id.as_ref().map(|c| c.as_str())),
                    Val::Str(Some(&row.kind)),
                    Val::StrList(tokens),
                    Val::Ts(row.start),
                    Val::Ts(row.end),
                    Val::Str(row.reason.as_deref()),
                    Val::Str(row.detail.as_deref()),
                    Val::Str(row.slot.as_deref()),
                    Val::Str(Some(&row.json)),
                ])?;
            }
            StoredBody::Market { change, market } => {
                let change = serde_json::to_value(change)?;
                let liquidity = market.liquidity_usd.map(|l| l.to_string());
                let json = serde_json::to_string(market)?;
                self.markets.push(&[
                    Val::Ts(Some(record.recv_ts.timestamp_micros())),
                    Val::U64(Some(record.seq)),
                    Val::Str(Some(record.run_id.as_str())),
                    Val::Str(Some(change.as_str().unwrap_or_default())),
                    Val::Str(Some(market.condition_id.as_str())),
                    Val::Str(Some(&market.question)),
                    Val::Str(market.slug.as_deref()),
                    Val::Str(market.event_id.as_ref().map(|e| e.as_str())),
                    Val::Str(Some(market.tokens.yes.as_str())),
                    Val::Str(Some(market.tokens.no.as_str())),
                    Val::StrList(Some(market.tags.iter().map(String::as_str).collect())),
                    Val::Ts(market.start_date.map(|t| t.timestamp_micros())),
                    Val::Ts(market.end_date.map(|t| t.timestamp_micros())),
                    Val::Bool(Some(market.active)),
                    Val::Bool(Some(market.closed)),
                    Val::Bool(Some(market.accepting_orders)),
                    Val::Str(liquidity.as_deref()),
                    Val::Dec(market_tick_size(market)),
                    Val::Bool(Some(market.neg_risk)),
                    Val::Ts(Some(market.fetched_at.timestamp_micros())),
                    Val::Str(Some(&json)),
                ])?;
            }
        }
        Ok(())
    }

    /// Finishes every file and returns the row count of each table.
    fn finish(self) -> anyhow::Result<BTreeMap<String, u64>> {
        let tables = [
            ("frames", self.frames),
            ("books", self.books),
            ("price_changes", self.price_changes),
            ("trades", self.trades),
            ("tick_sizes", self.tick_sizes),
            ("best_bid_ask", self.best_bid_ask),
            ("lifecycle", self.lifecycle),
            ("control", self.control),
            ("markets", self.markets),
            ("unparsed", self.unparsed),
        ];
        let mut counts = BTreeMap::new();
        for (name, table) in tables {
            counts.insert(name.to_string(), table.finish()?);
        }
        Ok(counts)
    }
}

/// The `books.source` value for a record source.
fn source_name(source: &str) -> &'static str {
    if source == "snapshot" {
        "snapshot"
    } else {
        "websocket"
    }
}

/// Checks that every line and every event was written somewhere.
fn check_accounting(
    acc: &Accounting,
    rows: &BTreeMap<String, u64>,
    books: u64,
    malformed_rows: u64,
) -> Result<(), String> {
    let n = |t: &str| rows.get(t).copied().unwrap_or(0);
    let line_rows = n("frames") + n("control") + n("markets") + malformed_rows;
    if acc.lines_read != line_rows || acc.malformed_lines != malformed_rows {
        return Err(format!(
            "{} lines read ({} malformed) but frames + control + markets + malformed = {} + {} + {} + {}",
            acc.lines_read,
            acc.malformed_lines,
            n("frames"),
            n("control"),
            n("markets"),
            malformed_rows
        ));
    }
    let event_rows = books
        + n("price_changes")
        + n("trades")
        + n("tick_sizes")
        + n("best_bid_ask")
        + n("lifecycle")
        + (n("unparsed") - malformed_rows);
    if acc.events_extracted != event_rows || acc.books != books {
        return Err(format!(
            "{} events extracted ({} books) but {event_rows} written ({books} books)",
            acc.events_extracted, acc.books
        ));
    }
    Ok(())
}

/// Writes every table and the manifest into `dir`.
fn write_day(
    root: &Path,
    date: NaiveDate,
    inputs: &[PathBuf],
    dir: &Path,
    options: &Options,
) -> anyhow::Result<DayManifest> {
    let mut tables = Tables::create(dir, options)?;
    let mut acc = Accounting::default();
    let mut files = vec![];
    for listed in inputs {
        // A `.jsonl` compressed by the recorder since it was listed is read from its copy.
        let zst = compressed_copy(listed);
        let (path, lines) = match (raw_lines(listed), &zst) {
            (Err(e), Some(zst)) if e.kind() == std::io::ErrorKind::NotFound => (
                zst.as_path(),
                raw_lines(zst).with_context(|| format!("opening {}", zst.display()))?,
            ),
            (lines, _) => (
                listed.as_path(),
                lines.with_context(|| format!("opening {}", listed.display()))?,
            ),
        };
        let rel = path
            .strip_prefix(root)
            .unwrap_or(path)
            .to_string_lossy()
            .into_owned();
        let mut input = InputFile {
            path: rel.clone(),
            lines: 0,
            malformed_lines: 0,
        };
        for line in lines {
            let (line_no, bytes) = line.with_context(|| format!("reading {}", path.display()))?;
            input.lines += 1;
            let parsed = std::str::from_utf8(&bytes)
                .map_err(|e| format!("not UTF-8: {e}"))
                .and_then(|text| decode(text).map(|r| (text, r)).map_err(|e| e.to_string()));
            match parsed {
                Ok((text, record)) => tables.record(&record, text, &rel, line_no, &mut acc)?,
                Err(error) => {
                    input.malformed_lines += 1;
                    tables.malformed(&rel, line_no, &bytes, &error)?;
                }
            }
        }
        acc.lines_read += input.lines;
        acc.malformed_lines += input.malformed_lines;
        files.push(input);
    }
    let books = tables.books_written;
    let malformed_rows = tables.malformed_rows;
    let row_counts = tables.finish()?;
    check_accounting(&acc, &row_counts, books, malformed_rows)
        .map_err(|detail| Refusal::Accounting { date, detail })?;

    let manifest = DayManifest {
        date: date.format("%Y-%m-%d").to_string(),
        program_version: env!("CARGO_PKG_VERSION").to_string(),
        compacted_at: Utc::now().timestamp_micros(),
        inputs: files,
        row_counts,
        malformed_lines: acc.malformed_lines,
        accounting: acc,
        accounting_ok: true,
    };
    let path = dir.join(MANIFEST_FILE);
    let mut file = File::create(&path)?;
    serde_json::to_writer_pretty(&mut file, &manifest)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    sync_dir(dir)?;
    Ok(manifest)
}
