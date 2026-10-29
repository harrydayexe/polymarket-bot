//! The "Compactor" cases of section 14.3, run against raw files written in the real line
//! format.

use std::{
    collections::HashSet,
    fs::{File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use arrow_array::{
    Array, RecordBatch,
    cast::AsArray,
    types::{Decimal128Type, TimestampMicrosecondType, UInt64Type},
};
use arrow_schema::DataType;
use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use marcasite::{types::TokenId, ws::MarketEvent};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use rust_decimal::Decimal;
use serde_json::{Value, json};

use super::*;
use crate::{
    archive::{envelope::encode, layout::FileKey},
    connection_manager::types::ConnAttemptId,
    market_registry::{
        market_info::MarketInfo, registry::MarketChange, token_changes::MarketTokens,
    },
    messages::{ControlBody, GapReason, Record, SnapshotReason},
    types::RunId,
};

fn day() -> NaiveDate {
    NaiveDate::from_ymd_opt(2026, 10, 20).unwrap()
}

fn today() -> NaiveDate {
    day().succ_opt().unwrap()
}

/// A time on [day].
fn at(hour: u32, micros: i64) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 10, 20, hour, 0, 0).unwrap()
        + chrono::TimeDelta::microseconds(micros)
}

fn ws(hour: u32, json: Value) -> Record {
    Record::WsFrame {
        recv_ts: at(hour, 123_456),
        conn_id: ConnAttemptId::new("c-0001-1"),
        event: serde_json::from_value::<MarketEvent>(json).unwrap(),
    }
}

/// A book shaped like a live `/books` element (`last_trade_price` is "" for an untraded token).
fn summary(asset: &str, bids: Value, asks: Value) -> Value {
    json!({
        "market":"0xabc","asset_id":asset,"timestamp":"1792472400120","hash":"h",
        "bids":bids,"asks":asks,"min_order_size":"5","tick_size":"0.01","neg_risk":false,
        "last_trade_price":""
    })
}

fn snapshot(hour: u32, books: Vec<Value>) -> Record {
    Record::Snapshot {
        recv_ts: at(hour, 7),
        requested_tokens: books
            .iter()
            .filter_map(|b| b["asset_id"].as_str().map(TokenId::new))
            .collect(),
        reason: SnapshotReason::Scheduled,
        books,
    }
}

fn control(hour: u32) -> Record {
    Record::Control {
        recv_ts: at(hour, 9),
        conn_id: Some(ConnAttemptId::new("c-0001-1")),
        body: ControlBody::GapOpen {
            tokens: [TokenId::new("111"), TokenId::new("222")].into(),
            start: at(hour, 1),
            reason: GapReason::ConnectFailed {
                error: "refused".into(),
            },
        },
    }
}

fn market(hour: u32) -> Record {
    Record::Market {
        recv_ts: at(hour, 11),
        change: MarketChange::Added,
        market: MarketInfo {
            condition_id: "0xabc".into(),
            question: "Will it?".into(),
            slug: Some("will-it".into()),
            event_id: None,
            tags: vec!["politics".into()],
            tokens: MarketTokens {
                yes: "111".into(),
                no: "222".into(),
            },
            start_date: Some(at(0, 0)),
            end_date: None,
            active: true,
            closed: false,
            accepting_orders: true,
            liquidity_usd: Some("1234.123456789".parse().unwrap()),
            tick_size: Some("0.01".parse().unwrap()),
            neg_risk: false,
            fetched_at: at(hour, 0),
        },
    }
}

fn book_event() -> Value {
    json!({"event_type":"book","asset_id":"111","market":"0xabc",
        "bids":[{"price":"0.48","size":"30.5"},{"price":"0.49","size":"20"}],
        "asks":[{"price":"0.52","size":"25"},{"price":"0.51","size":"219.217767"}],
        "timestamp":"1792472400120","hash":"0xh1","tick_size":"0.01"})
}

/// One message of every kind the market channel sends, plus one it does not.
fn every_event() -> Vec<Value> {
    vec![
        book_event(),
        json!({"event_type":"price_change","market":"0xabc","timestamp":"1792472400121",
            "price_changes":[{"asset_id":"111","price":"0.001","size":"0","side":"BUY",
                "hash":"h2","best_bid":"0.49","best_ask":"0.51"}]}),
        json!({"event_type":"last_trade_price","asset_id":"111","market":"0xabc",
            "price":"0.5","size":"12","fee_rate_bps":"0","side":"BUY",
            "timestamp":"1792472400122","transaction_hash":"0xt"}),
        json!({"event_type":"tick_size_change","asset_id":"111","market":"0xabc",
            "old_tick_size":"0.01","new_tick_size":"0.001","timestamp":"1792472400123"}),
        json!({"event_type":"best_bid_ask","asset_id":"111","market":"0xabc",
            "best_bid":"0.49","best_ask":"","spread":"","timestamp":"1792472400124"}),
        json!({"event_type":"new_market","id":"1","question":"Q?","market":"0xdef",
            "slug":"q","description":null,"assets_ids":["333","444"],
            "outcomes":["Yes","No"],"event_message":null,"timestamp":"1792472400125",
            "tags":["politics"]}),
        json!({"event_type":"market_resolved","id":"1","market":"0xdef",
            "assets_ids":["333","444"],"winning_asset_id":"333","winning_outcome":"Yes",
            "event_message":null,"timestamp":"1792472400126","tags":null}),
        json!({"event_type":"something_new","x":[1,2,{"y":"z"}]}),
    ]
}

/// Writes `records` as run `r-1`, each in the file the writer would put it in, numbered
/// from 1.
fn write_records(root: &Path, records: &[Record]) {
    let run = RunId::new("r-1");
    for (i, record) in records.iter().enumerate() {
        let dir = FileKey::for_time(record.stream(), record.recv_ts()).dir(root);
        std::fs::create_dir_all(&dir).unwrap();
        let mut line = vec![];
        encode(record, i as u64 + 1, &run, &mut line).unwrap();
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(dir.join("r-1.jsonl"))
            .unwrap()
            .write_all(&line)
            .unwrap();
    }
}

fn compact(root: &Path) -> anyhow::Result<DayManifest> {
    compact_day(root, day(), today(), &Options::default())
}

fn table_path(root: &Path, table: &str) -> PathBuf {
    day_dir(root, day()).join(format!("{table}.parquet"))
}

/// The rows of `table`, as one batch.
fn read(root: &Path, table: &str) -> RecordBatch {
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(File::open(table_path(root, table)).unwrap())
            .unwrap();
    let schema = builder.schema().clone();
    let batches: Vec<RecordBatch> = builder.build().unwrap().map(Result::unwrap).collect();
    assert!(batches.len() <= 1, "small tests fit in one batch");
    batches
        .into_iter()
        .next()
        .unwrap_or_else(|| RecordBatch::new_empty(schema))
}

fn strs(batch: &RecordBatch, col: &str) -> Vec<Option<String>> {
    let array = batch.column_by_name(col).unwrap().as_string::<i32>();
    (0..array.len())
        .map(|i| array.is_valid(i).then(|| array.value(i).to_string()))
        .collect()
}

fn decs(batch: &RecordBatch, col: &str) -> Vec<Option<Decimal>> {
    let array = batch
        .column_by_name(col)
        .unwrap()
        .as_primitive::<Decimal128Type>();
    assert_eq!(array.data_type(), &DataType::Decimal128(38, 6));
    (0..array.len())
        .map(|i| {
            array
                .is_valid(i)
                .then(|| Decimal::from_i128_with_scale(array.value(i), 6))
        })
        .collect()
}

fn u64s(batch: &RecordBatch, col: &str) -> Vec<u64> {
    let array = batch
        .column_by_name(col)
        .unwrap()
        .as_primitive::<UInt64Type>();
    array.values().to_vec()
}

fn times(batch: &RecordBatch, col: &str) -> Vec<Option<i64>> {
    let array = batch
        .column_by_name(col)
        .unwrap()
        .as_primitive::<TimestampMicrosecondType>();
    (0..array.len())
        .map(|i| array.is_valid(i).then(|| array.value(i)))
        .collect()
}

fn dec(s: &str) -> Decimal {
    s.parse().unwrap()
}

fn counts(manifest: &DayManifest) -> Vec<(&str, u64)> {
    TABLES
        .iter()
        .map(|t| (*t, manifest.row_counts[*t]))
        .collect()
}

#[test]
fn routes_each_kind_of_record_to_its_table() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let mut records: Vec<Record> = every_event().into_iter().map(|e| ws(14, e)).collect();
    records.push(snapshot(
        15,
        vec![summary(
            "111",
            json!([{"price":"0.49","size":"20"}]),
            json!([{"price":"0.51","size":"5"}]),
        )],
    ));
    records.push(control(16));
    records.push(market(16));
    write_records(root, &records);

    let manifest = compact(root).unwrap();
    assert_eq!(
        counts(&manifest),
        [
            ("frames", 9),
            ("books", 6),
            ("price_changes", 1),
            ("trades", 1),
            ("tick_sizes", 1),
            ("best_bid_ask", 1),
            ("lifecycle", 2),
            ("control", 1),
            ("markets", 1),
            ("unparsed", 1),
        ]
    );
    assert!(manifest.accounting_ok);
    assert_eq!(manifest.date, "2026-10-20");
    assert_eq!(manifest.program_version, env!("CARGO_PKG_VERSION"));
    assert_eq!(manifest.accounting.lines_read, 11);
    assert_eq!(manifest.accounting.events_extracted, 9);
    assert_eq!(manifest.accounting.books, 2);
    let paths: Vec<&str> = manifest.inputs.iter().map(|i| i.path.as_str()).collect();
    assert_eq!(
        paths,
        [
            "control/date=2026-10-20/r-1.jsonl",
            "markets/date=2026-10-20/r-1.jsonl",
            "raw/snapshots/date=2026-10-20/hour=15/r-1.jsonl",
            "raw/ws/date=2026-10-20/hour=14/r-1.jsonl",
        ]
    );
    assert_eq!(manifest.inputs[3].lines, 8);
    // The manifest on disk is the one returned.
    let on_disk: DayManifest =
        serde_json::from_slice(&std::fs::read(day_dir(root, day()).join(MANIFEST_FILE)).unwrap())
            .unwrap();
    assert_eq!(on_disk, manifest);

    // `frames` holds every market data line exactly as written.
    let frames = read(root, "frames");
    let mut written: Vec<String> = [
        "raw/snapshots/date=2026-10-20/hour=15",
        "raw/ws/date=2026-10-20/hour=14",
    ]
    .iter()
    .flat_map(|d| {
        std::fs::read_to_string(root.join(d).join("r-1.jsonl"))
            .unwrap()
            .lines()
            .map(str::to_owned)
            .collect::<Vec<_>>()
    })
    .collect();
    written.sort();
    let mut lines: Vec<String> = strs(&frames, "line")
        .into_iter()
        .map(Option::unwrap)
        .collect();
    lines.sort();
    assert_eq!(lines, written);
    assert_eq!(
        strs(&frames, "source")
            .iter()
            .filter(|s| s.as_deref() == Some("snapshot"))
            .count(),
        1
    );

    let books = read(root, "books");
    let sources: HashSet<_> = strs(&books, "source").into_iter().flatten().collect();
    assert_eq!(
        sources,
        HashSet::from(["websocket".to_string(), "snapshot".to_string()])
    );
    assert_eq!(times(&books, "exchange_ts")[0], Some(1_792_472_400_120_000));

    let lifecycle = read(root, "lifecycle");
    assert_eq!(
        strs(&lifecycle, "kind"),
        [Some("new_market".into()), Some("market_resolved".into())]
    );
    assert_eq!(
        strs(&lifecycle, "winning_outcome"),
        [None, Some("Yes".into())]
    );
    assert_eq!(strs(&lifecycle, "question"), [Some("Q?".into()), None]);
    assert_eq!(
        strs(&lifecycle, "exchange_ts_raw")[0].as_deref(),
        Some("1792472400125")
    );

    let control = read(root, "control");
    assert_eq!(strs(&control, "kind"), [Some("gap_open".into())]);
    assert_eq!(strs(&control, "reason"), [Some("connect_failed".into())]);
    assert_eq!(strs(&control, "detail"), [Some("refused".into())]);
    assert_eq!(
        times(&control, "start"),
        [Some(at(16, 1).timestamp_micros())]
    );

    let markets = read(root, "markets");
    assert_eq!(strs(&markets, "yes_token"), [Some("111".into())]);
    assert_eq!(
        strs(&markets, "liquidity_usd"),
        [Some("1234.123456789".into())]
    );
    assert_eq!(decs(&markets, "tick_size"), [Some(dec("0.01"))]);

    let best = read(root, "best_bid_ask");
    assert_eq!(decs(&best, "best_bid"), [Some(dec("0.49"))]);
    assert_eq!(decs(&best, "best_ask"), [None]);
}

#[test]
fn multi_token_price_change_is_a_row_per_token() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let change = |asset: &str| {
        json!({"asset_id":asset,"price":"0.5","size":"10","side":"SELL","hash":"h",
            "best_bid":"0.49","best_ask":"0.5"})
    };
    write_records(
        root,
        &[ws(
            3,
            json!({"event_type":"price_change","market":"0xabc","timestamp":"1792472400121",
                "price_changes":[change("1"), change("2"), change("3")]}),
        )],
    );
    let manifest = compact(root).unwrap();
    assert_eq!(manifest.row_counts["price_changes"], 3);
    assert_eq!(manifest.accounting.events_extracted, 3);

    let rows = read(root, "price_changes");
    assert_eq!(
        strs(&rows, "asset_id"),
        [Some("1".into()), Some("2".into()), Some("3".into())]
    );
    assert_eq!(u64s(&rows, "seq"), [1, 1, 1]);
    assert_eq!(
        times(&rows, "recv_ts"),
        [Some(at(3, 123_456).timestamp_micros()); 3]
    );
    assert_eq!(
        times(&rows, "exchange_ts"),
        [Some(1_792_472_400_121_000); 3]
    );
}

#[test]
fn every_book_of_a_snapshot_has_its_own_id() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let mut books: Vec<Value> = (1..30)
        .map(|i| {
            summary(
                &i.to_string(),
                json!([{"price":"0.49","size":"20"}]),
                json!([]),
            )
        })
        .collect();
    // An empty book is kept as a single row with no side, price or size.
    books.insert(0, summary("0", json!([]), json!([])));
    write_records(root, &[snapshot(5, books)]);

    let manifest = compact(root).unwrap();
    assert_eq!(manifest.row_counts["books"], 30);
    assert_eq!(manifest.accounting.books, 30);
    let rows = read(root, "books");
    let ids: HashSet<_> = strs(&rows, "book_id").into_iter().flatten().collect();
    assert_eq!(ids.len(), 30);
    assert!(ids.contains("r-1:1:0") && ids.contains("r-1:1:29"));
    assert_eq!(strs(&rows, "asset_id")[0].as_deref(), Some("0"));
    assert_eq!(strs(&rows, "side")[0], None);
    assert_eq!(decs(&rows, "price")[0], None);
    assert_eq!(strs(&rows, "side")[1].as_deref(), Some("bid"));
}

#[test]
fn prices_and_sizes_are_stored_exactly() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    write_records(
        root,
        &[ws(1, book_event()), ws(1, every_event()[1].clone())],
    );
    compact(root).unwrap();

    let books = read(root, "books");
    assert_eq!(
        strs(&books, "side")
            .into_iter()
            .flatten()
            .collect::<Vec<_>>(),
        ["bid", "bid", "ask", "ask"]
    );
    assert_eq!(
        decs(&books, "size"),
        [
            Some(dec("30.5")),
            Some(dec("20")),
            Some(dec("25")),
            Some(dec("219.217767"))
        ]
    );
    assert_eq!(decs(&books, "size")[3].unwrap().to_string(), "219.217767");
    let changes = read(root, "price_changes");
    assert_eq!(decs(&changes, "price"), [Some(dec("0.001"))]);
    assert_eq!(
        decs(&changes, "price")[0].unwrap().normalize().to_string(),
        "0.001"
    );
}

#[test]
fn unknown_types_go_to_unparsed() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let unknown = json!({"event_type":"something_new","x":[1,2,{"y":"z"}]});
    // A level with seven decimal places would have to be rounded, so the book is unparsed.
    let mut too_precise = book_event();
    too_precise["bids"][0]["size"] = json!("1.0000001");
    write_records(root, &[ws(2, unknown.clone()), ws(2, too_precise)]);

    let manifest = compact(root).unwrap();
    assert_eq!(manifest.row_counts["unparsed"], 2);
    assert_eq!(manifest.row_counts["books"], 0);
    assert_eq!(manifest.row_counts["frames"], 2);
    let rows = read(root, "unparsed");
    assert_eq!(
        strs(&rows, "source"),
        [Some("ws".into()), Some("ws".into())]
    );
    let errors = strs(&rows, "error");
    assert!(errors[0].as_ref().unwrap().contains("something_new"));
    assert!(errors[1].as_ref().unwrap().contains("1.0000001"));
    let raw: Value = serde_json::from_str(strs(&rows, "raw")[0].as_ref().unwrap()).unwrap();
    assert_eq!(raw, unknown);
    assert_eq!(u64s(&rows, "line_no"), [1, 2]);
}

#[test]
fn a_cut_off_line_is_counted_and_kept() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    write_records(root, &[ws(2, book_event()), control(2)]);
    let path = FileKey::for_time(crate::archive::layout::Stream::Ws, at(2, 0))
        .dir(root)
        .join("r-1.jsonl");
    OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap()
        .write_all(b"{\"recv_ts\":1792472400,\"se")
        .unwrap();

    let manifest = compact(root).unwrap();
    assert_eq!(manifest.malformed_lines, 1);
    let input = manifest
        .inputs
        .iter()
        .find(|i| i.path.starts_with("raw/ws"))
        .unwrap();
    assert_eq!((input.lines, input.malformed_lines), (2, 1));
    assert_eq!(manifest.row_counts["frames"], 1);
    let rows = read(root, "unparsed");
    assert_eq!(strs(&rows, "source"), [Some("malformed".into())]);
    assert_eq!(
        strs(&rows, "raw"),
        [Some("{\"recv_ts\":1792472400,\"se".into())]
    );
    assert_eq!(u64s(&rows, "line_no"), [2]);
    assert_eq!(times(&rows, "recv_ts"), [None]);
}

fn raw_snapshot(root: &Path) -> Vec<(PathBuf, Vec<u8>)> {
    raw_files(root, day(), true)
        .unwrap()
        .into_iter()
        .map(|p| {
            let bytes = std::fs::read(&p).unwrap();
            (p, bytes)
        })
        .collect()
}

fn parquet_entries(root: &Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(root.join(PARQUET_DIR))
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    names
}

#[test]
fn a_failed_accounting_check_changes_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    write_records(
        root,
        &[ws(2, book_event()), ws(2, every_event()[1].clone())],
    );
    let raw = raw_snapshot(root);
    let lose = Options { lose_one_row: true };

    // Nothing compacted before: nothing appears.
    let err = compact_day(root, day(), today(), &lose).unwrap_err();
    assert!(
        matches!(
            err.downcast_ref::<Refusal>(),
            Some(Refusal::Accounting { .. })
        ),
        "{err:#}"
    );
    assert!(parquet_entries(root).is_empty());

    // An earlier compaction stays as it was.
    let first = compact(root).unwrap();
    let err = compact_day(root, day(), today(), &lose).unwrap_err();
    assert!(matches!(
        err.downcast_ref::<Refusal>(),
        Some(Refusal::Accounting { .. })
    ));
    assert_eq!(parquet_entries(root), ["date=2026-10-20"]);
    let manifest: DayManifest =
        serde_json::from_slice(&std::fs::read(day_dir(root, day()).join(MANIFEST_FILE)).unwrap())
            .unwrap();
    assert_eq!(manifest, first);
    assert_eq!(raw_snapshot(root), raw);
}

#[test]
fn refuses_a_day_with_open_files() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    write_records(root, &[ws(2, book_event()), control(3)]);
    let open = FileKey::for_time(crate::archive::layout::Stream::Ws, at(23, 0)).dir(root);
    std::fs::create_dir_all(&open).unwrap();
    std::fs::write(open.join("r-2.jsonl.open"), b"").unwrap();

    let err = compact(root).unwrap_err();
    let Some(Refusal::OpenFiles { files, .. }) = err.downcast_ref::<Refusal>() else {
        panic!("{err:#}")
    };
    assert_eq!(files, &[open.join("r-2.jsonl.open")]);
    assert!(!root.join(PARQUET_DIR).exists());
}

#[test]
fn refuses_a_day_that_is_not_over() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    write_records(root, &[ws(2, book_event())]);
    for today in [day(), day().pred_opt().unwrap()] {
        let err = compact_day(root, day(), today, &Options::default()).unwrap_err();
        assert!(matches!(
            err.downcast_ref::<Refusal>(),
            Some(Refusal::DayNotOver { .. })
        ));
    }
    assert!(!root.join(PARQUET_DIR).exists());
}

#[test]
fn refuses_a_day_without_raw_files() {
    let dir = tempfile::tempdir().unwrap();
    let err = compact(dir.path()).unwrap_err();
    assert_eq!(
        err.downcast_ref::<Refusal>(),
        Some(&Refusal::NoRawFiles { date: day() })
    );
}

fn all_records() -> Vec<Record> {
    let mut records: Vec<Record> = every_event().into_iter().map(|e| ws(10, e)).collect();
    records.push(snapshot(
        11,
        vec![summary(
            "111",
            json!([]),
            json!([{"price":"0.6","size":"1"}]),
        )],
    ));
    records.push(control(12));
    records.push(market(12));
    records
}

#[test]
fn compacting_again_gives_identical_tables() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    write_records(root, &all_records());

    let first = compact(root).unwrap();
    let tables: Vec<Vec<u8>> = TABLES
        .iter()
        .map(|t| std::fs::read(table_path(root, t)).unwrap())
        .collect();
    let second = compact(root).unwrap();
    assert_eq!(second.row_counts, first.row_counts);
    assert_eq!(second.inputs, first.inputs);
    for (table, bytes) in TABLES.iter().zip(&tables) {
        assert_eq!(
            &std::fs::read(table_path(root, table)).unwrap(),
            bytes,
            "{table}"
        );
    }
    assert_eq!(parquet_entries(root), ["date=2026-10-20"]);
}

#[test]
fn leftovers_of_a_crash_are_ignored_and_cleaned_up() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    write_records(root, &all_records());
    let first = compact(root).unwrap();
    let parquet = root.join(PARQUET_DIR);

    // A compaction that died mid-write leaves a temporary folder with half the tables.
    let stale = parquet.join(".tmp-date=2026-10-20-999-1");
    std::fs::create_dir(&stale).unwrap();
    std::fs::write(stale.join("frames.parquet"), b"half").unwrap();
    // Readers only see the finished day.
    assert!(is_compacted(root, day()));
    assert_eq!(
        read(root, "frames").num_rows() as u64,
        first.row_counts["frames"]
    );

    let second = compact(root).unwrap();
    assert_eq!(second.row_counts, first.row_counts);
    assert_eq!(parquet_entries(root), ["date=2026-10-20"]);

    // A compaction that died between its two renames left the old day moved aside: it is
    // brought back even if the next compaction fails.
    std::fs::rename(
        day_dir(root, day()),
        parquet.join(".old-date=2026-10-20-999-1"),
    )
    .unwrap();
    std::fs::create_dir(&stale).unwrap();
    let err = compact_day(root, day(), today(), &Options { lose_one_row: true }).unwrap_err();
    assert!(matches!(
        err.downcast_ref::<Refusal>(),
        Some(Refusal::Accounting { .. })
    ));
    assert_eq!(parquet_entries(root), ["date=2026-10-20"]);
    assert!(is_compacted(root, day()));
}

#[test]
fn pending_days_skips_compacted_and_empty_days() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    write_records(root, &[ws(2, book_event())]);
    let earlier = NaiveDate::from_ymd_opt(2026, 10, 17).unwrap();
    let record = Record::Control {
        recv_ts: Utc.with_ymd_and_hms(2026, 10, 18, 1, 0, 0).unwrap(),
        conn_id: None,
        body: ControlBody::RunStop { clean: true },
    };
    write_records(root, &[record]);
    let later = NaiveDate::from_ymd_opt(2026, 10, 23).unwrap();

    let days = pending_days(root, earlier, later).unwrap();
    assert_eq!(days, [day().pred_opt().unwrap().pred_opt().unwrap(), day()]);
    compact(root).unwrap();
    assert_eq!(pending_days(root, earlier, later).unwrap().len(), 1);
    // Today is never pending.
    assert_eq!(pending_days(root, earlier, day()).unwrap().len(), 1);
    assert!(pending_days(root, later, later).is_err());
}

#[test]
fn large_tables_are_written_in_several_batches() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let n = tables::BATCH_ROWS + 10;
    let changes: Vec<Value> = (0..n)
        .map(|i| json!({"asset_id":i.to_string(),"price":"0.5","size":"1","side":"BUY","hash":"h"}))
        .collect();
    write_records(
        root,
        &[ws(
            1,
            json!({"event_type":"price_change","market":"0xabc","timestamp":"1792472400121",
                "price_changes":changes}),
        )],
    );
    let manifest = compact(root).unwrap();
    assert_eq!(manifest.row_counts["price_changes"], n as u64);
    let file = File::open(table_path(root, "price_changes")).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    assert_eq!(builder.metadata().file_metadata().num_rows(), n as i64);
}

#[test]
fn a_snapshot_book_that_cannot_be_read_is_unparsed() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let good = summary("1", json!([{"price":"0.5","size":"1"}]), json!([]));
    let bad = json!({"asset_id":"2","bids":[{"price":"x","size":"1"}],"asks":[]});
    // Only the token and the levels are needed.
    let bare = json!({"asset_id":"3","bids":[],"asks":[{"price":"0.6","size":"2"}]});
    write_records(root, &[snapshot(4, vec![good, bad.clone(), bare])]);

    let manifest = compact(root).unwrap();
    assert_eq!(manifest.row_counts["frames"], 1);
    assert_eq!(manifest.row_counts["books"], 2);
    assert_eq!(manifest.row_counts["unparsed"], 1);
    assert_eq!(manifest.accounting.events_extracted, 3);
    let rows = read(root, "unparsed");
    assert_eq!(strs(&rows, "source"), [Some("snapshot".into())]);
    let raw: Value = serde_json::from_str(strs(&rows, "raw")[0].as_ref().unwrap()).unwrap();
    assert_eq!(raw, bad);
    let books = read(root, "books");
    assert_eq!(
        strs(&books, "book_id"),
        [Some("r-1:1:0".into()), Some("r-1:1:2".into())]
    );
    assert_eq!(strs(&books, "market"), [Some("0xabc".into()), None]);
    assert_eq!(times(&books, "exchange_ts")[1], None);
}

#[test]
fn compressed_raw_files_are_read() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    write_records(root, &all_records());
    let plain = compact(root).unwrap();
    let frames = strs(&read(root, "frames"), "line");

    let stop = std::sync::atomic::AtomicBool::new(false);
    for path in raw_files(root, day(), false).unwrap() {
        if path.starts_with(root.join("raw")) {
            assert!(crate::archive::compress::compress_file(&path, &stop).unwrap());
        }
    }
    let compressed = compact(root).unwrap();
    assert_eq!(compressed.row_counts, plain.row_counts);
    let paths: Vec<&str> = compressed.inputs.iter().map(|i| i.path.as_str()).collect();
    assert!(
        paths.contains(&"raw/ws/date=2026-10-20/hour=10/r-1.jsonl.zst"),
        "{paths:?}"
    );
    assert_eq!(strs(&read(root, "frames"), "line"), frames);
}
