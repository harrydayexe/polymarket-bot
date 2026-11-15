//! The Parquet tables of a compacted day: their schemas, and a writer that builds Arrow
//! arrays a batch at a time so a day never has to fit in memory.

use std::{fs::File, path::Path, sync::Arc};

use anyhow::Context;
use arrow_array::{
    ArrayRef, RecordBatch,
    builder::{
        ArrayBuilder, BinaryBuilder, BooleanBuilder, Decimal128Builder, ListBuilder, StringBuilder,
        TimestampMicrosecondBuilder, UInt64Builder,
    },
};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use parquet::{
    arrow::ArrowWriter,
    basic::{Compression, ZstdLevel},
    file::properties::WriterProperties,
};

/// Rows gathered before they are turned into a record batch and handed to the Parquet writer.
pub const BATCH_ROWS: usize = 65_536;
/// Rows per Parquet row group. The writer keeps the current row group's encoded pages in
/// memory, so this bounds memory use as well as setting the read granularity.
const ROW_GROUP_ROWS: usize = 4 * BATCH_ROWS;

/// Every price and size column is `Decimal128(38, 6)`.
pub const DECIMAL_PRECISION: u8 = 38;
pub const DECIMAL_SCALE: u32 = 6;

/// The type of a column.
#[derive(Debug, Clone, Copy)]
pub enum Kind {
    Str,
    U64,
    Bool,
    /// Microseconds since the epoch, UTC.
    Ts,
    /// Fixed point, [DECIMAL_SCALE] places.
    Dec,
    StrList,
    Bin,
}

impl Kind {
    fn data_type(self) -> DataType {
        match self {
            Kind::Str => DataType::Utf8,
            Kind::U64 => DataType::UInt64,
            Kind::Bool => DataType::Boolean,
            Kind::Ts => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            Kind::Dec => DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE as i8),
            Kind::StrList => DataType::List(Arc::new(Field::new_list_field(DataType::Utf8, true))),
            Kind::Bin => DataType::Binary,
        }
    }

    fn builder(self) -> Builder {
        match self {
            Kind::Str => Builder::Str(StringBuilder::new()),
            Kind::U64 => Builder::U64(UInt64Builder::new()),
            Kind::Bool => Builder::Bool(BooleanBuilder::new()),
            Kind::Ts => Builder::Ts(TimestampMicrosecondBuilder::new().with_timezone("UTC")),
            Kind::Dec => Builder::Dec(
                Decimal128Builder::new()
                    .with_precision_and_scale(DECIMAL_PRECISION, DECIMAL_SCALE as i8)
                    .expect("valid decimal type"),
            ),
            Kind::StrList => Builder::StrList(ListBuilder::new(StringBuilder::new())),
            Kind::Bin => Builder::Bin(BinaryBuilder::new()),
        }
    }
}

/// One column of a table.
#[derive(Debug, Clone, Copy)]
pub struct Col {
    pub name: &'static str,
    pub kind: Kind,
    pub nullable: bool,
}

const fn req(name: &'static str, kind: Kind) -> Col {
    Col {
        name,
        kind,
        nullable: false,
    }
}

const fn opt(name: &'static str, kind: Kind) -> Col {
    Col {
        name,
        kind,
        nullable: true,
    }
}

use Kind::{Bin, Bool, Dec, Str, StrList, Ts, U64};

/// The tables, by name, in the order they are listed in summaries.
pub const TABLES: [&str; 10] = [
    "frames",
    "books",
    "price_changes",
    "trades",
    "tick_sizes",
    "best_bid_ask",
    "lifecycle",
    "control",
    "markets",
    "unparsed",
];

/// One row per `ws` or `snapshot` line, with the line itself, so the Parquet copy of the
/// market data is lossless on its own.
pub const FRAMES: &[Col] = &[
    req("recv_ts", Ts),
    req("seq", U64),
    req("run_id", Str),
    opt("conn_id", Str),
    req("source", Str),
    req("line", Str),
];

/// One row per price level of a full book. A book with no levels on either side is one row
/// with `side`, `price` and `size` null, so empty books are kept. `exchange_ts`, `market` and
/// `hash` are always set for websocket books; a snapshot book may lack them.
pub const BOOKS: &[Col] = &[
    req("book_id", Str),
    req("source", Str),
    req("recv_ts", Ts),
    opt("exchange_ts", Ts),
    req("asset_id", Str),
    opt("market", Str),
    opt("side", Str),
    opt("price", Dec),
    opt("size", Dec),
    opt("hash", Str),
    req("seq", U64),
    req("run_id", Str),
    opt("conn_id", Str),
];

pub const PRICE_CHANGES: &[Col] = &[
    req("recv_ts", Ts),
    req("exchange_ts", Ts),
    req("seq", U64),
    req("run_id", Str),
    opt("conn_id", Str),
    req("asset_id", Str),
    req("market", Str),
    req("side", Str),
    req("price", Dec),
    req("size", Dec),
    opt("best_bid", Dec),
    opt("best_ask", Dec),
    req("hash", Str),
];

pub const TRADES: &[Col] = &[
    req("recv_ts", Ts),
    req("exchange_ts", Ts),
    req("seq", U64),
    req("run_id", Str),
    opt("conn_id", Str),
    req("asset_id", Str),
    req("market", Str),
    req("price", Dec),
    req("size", Dec),
    req("side", Str),
    opt("fee_rate_bps", Dec),
    opt("transaction_hash", Str),
];

/// `exchange_ts` is the exchange's `timestamp` read as Unix milliseconds (null if it is
/// not a number); `exchange_ts_raw` is the text as sent, since the documentation gives
/// no unit for these events.
pub const TICK_SIZES: &[Col] = &[
    req("recv_ts", Ts),
    opt("exchange_ts", Ts),
    req("exchange_ts_raw", Str),
    req("seq", U64),
    req("run_id", Str),
    opt("conn_id", Str),
    req("asset_id", Str),
    req("market", Str),
    req("old_tick_size", Dec),
    req("new_tick_size", Dec),
];

pub const BEST_BID_ASK: &[Col] = &[
    req("recv_ts", Ts),
    opt("exchange_ts", Ts),
    req("exchange_ts_raw", Str),
    req("seq", U64),
    req("run_id", Str),
    opt("conn_id", Str),
    req("asset_id", Str),
    req("market", Str),
    opt("best_bid", Dec),
    opt("best_ask", Dec),
    opt("spread", Dec),
];

/// `new_market` and `market_resolved`. `event` is the whole event as JSON.
pub const LIFECYCLE: &[Col] = &[
    req("recv_ts", Ts),
    opt("exchange_ts", Ts),
    req("exchange_ts_raw", Str),
    req("seq", U64),
    req("run_id", Str),
    opt("conn_id", Str),
    req("kind", Str),
    req("market", Str),
    req("market_id", Str),
    req("asset_ids", StrList),
    opt("question", Str),
    opt("slug", Str),
    opt("outcomes", StrList),
    opt("winning_asset_id", Str),
    opt("winning_outcome", Str),
    opt("tags", StrList),
    req("event", Str),
];

/// One row per control line. The common fields get their own columns; `json` holds the
/// whole body (`kind` and every field), so nothing is lost.
pub const CONTROL: &[Col] = &[
    req("recv_ts", Ts),
    req("seq", U64),
    req("run_id", Str),
    opt("conn_id", Str),
    req("kind", Str),
    opt("tokens", StrList),
    opt("start", Ts),
    opt("end", Ts),
    opt("reason", Str),
    opt("detail", Str),
    opt("slot", Str),
    req("json", Str),
];

/// One row per market line. `liquidity_usd` is the exact decimal text: Gamma sends it
/// with more places than the fixed-point columns hold. `json` is the whole market.
pub const MARKETS: &[Col] = &[
    req("recv_ts", Ts),
    req("seq", U64),
    req("run_id", Str),
    req("change", Str),
    req("condition_id", Str),
    req("question", Str),
    opt("slug", Str),
    opt("event_id", Str),
    req("yes_token", Str),
    req("no_token", Str),
    req("tags", StrList),
    opt("start_date", Ts),
    opt("end_date", Ts),
    req("active", Bool),
    req("closed", Bool),
    req("accepting_orders", Bool),
    opt("liquidity_usd", Str),
    opt("tick_size", Dec),
    req("neg_risk", Bool),
    req("fetched_at", Ts),
    req("json", Str),
];

/// Anything not understood. `source` is `ws` or `snapshot` for an event that could not be
/// turned into a row (with `raw` that event's JSON), or `malformed` for a line that could not
/// be read at all (envelope columns null, `raw` the line). `raw_bytes` holds the line only
/// when it is not valid UTF-8, where `raw` is a lossy copy.
pub const UNPARSED: &[Col] = &[
    opt("recv_ts", Ts),
    opt("seq", U64),
    opt("run_id", Str),
    opt("conn_id", Str),
    req("source", Str),
    req("file", Str),
    req("line_no", U64),
    req("error", Str),
    req("raw", Str),
    opt("raw_bytes", Bin),
];

/// A cell of a row being added.
#[derive(Debug, Clone)]
pub enum Val<'a> {
    Str(Option<&'a str>),
    U64(Option<u64>),
    Bool(Option<bool>),
    Ts(Option<i64>),
    Dec(Option<i128>),
    StrList(Option<Vec<&'a str>>),
    Bin(Option<&'a [u8]>),
}

enum Builder {
    Str(StringBuilder),
    U64(UInt64Builder),
    Bool(BooleanBuilder),
    Ts(TimestampMicrosecondBuilder),
    Dec(Decimal128Builder),
    StrList(ListBuilder<StringBuilder>),
    Bin(BinaryBuilder),
}

impl Builder {
    fn finish(&mut self) -> ArrayRef {
        match self {
            Builder::Str(b) => Arc::new(b.finish()),
            Builder::U64(b) => Arc::new(b.finish()),
            Builder::Bool(b) => Arc::new(b.finish()),
            Builder::Ts(b) => Arc::new(b.finish()),
            Builder::Dec(b) => Arc::new(b.finish()),
            Builder::StrList(b) => Arc::new(b.finish()),
            Builder::Bin(b) => Arc::new(b.finish()),
        }
    }

    fn len(&self) -> usize {
        match self {
            Builder::Str(b) => b.len(),
            Builder::U64(b) => b.len(),
            Builder::Bool(b) => b.len(),
            Builder::Ts(b) => b.len(),
            Builder::Dec(b) => b.len(),
            Builder::StrList(b) => b.len(),
            Builder::Bin(b) => b.len(),
        }
    }

    /// Appends `val`, or returns false if it is the wrong type for this column.
    fn append(&mut self, val: &Val) -> bool {
        match (self, val) {
            (Builder::Str(b), Val::Str(v)) => b.append_option(*v),
            (Builder::U64(b), Val::U64(v)) => b.append_option(*v),
            (Builder::Bool(b), Val::Bool(v)) => b.append_option(*v),
            (Builder::Ts(b), Val::Ts(v)) => b.append_option(*v),
            (Builder::Dec(b), Val::Dec(v)) => b.append_option(*v),
            (Builder::StrList(b), Val::StrList(v)) => match v {
                Some(items) => {
                    for item in items {
                        b.values().append_value(item);
                    }
                    b.append(true);
                }
                None => b.append(false),
            },
            (Builder::Bin(b), Val::Bin(v)) => b.append_option(*v),
            _ => return false,
        }
        true
    }
}

pub fn schema(cols: &[Col]) -> SchemaRef {
    Arc::new(Schema::new(
        cols.iter()
            .map(|c| Field::new(c.name, c.kind.data_type(), c.nullable))
            .collect::<Vec<_>>(),
    ))
}

/// Writes one table to `<dir>/<name>.parquet`, a batch of rows at a time.
pub struct TableWriter {
    name: &'static str,
    cols: &'static [Col],
    schema: SchemaRef,
    builders: Vec<Builder>,
    pending: usize,
    rows: u64,
    writer: ArrowWriter<File>,
}

impl TableWriter {
    pub fn create(dir: &Path, name: &'static str, cols: &'static [Col]) -> anyhow::Result<Self> {
        let path = dir.join(format!("{name}.parquet"));
        let file = File::create(&path).with_context(|| format!("creating {}", path.display()))?;
        let schema = schema(cols);
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::default()))
            .set_max_row_group_row_count(Some(ROW_GROUP_ROWS))
            .build();
        let writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;
        Ok(Self {
            name,
            cols,
            schema,
            builders: cols.iter().map(|c| c.kind.builder()).collect(),
            pending: 0,
            rows: 0,
            writer,
        })
    }

    /// Adds one row, with a value for every column in schema order.
    ///
    /// # Panics
    ///
    /// If the row does not match the schema: a bug in the caller, not bad input.
    pub fn push(&mut self, row: &[Val]) -> anyhow::Result<()> {
        assert_eq!(row.len(), self.cols.len(), "row width for {}", self.name);
        for ((builder, val), col) in self.builders.iter_mut().zip(row).zip(self.cols) {
            assert!(
                builder.append(val),
                "{}.{}: {val:?} is not {:?}",
                self.name,
                col.name,
                col.kind
            );
        }
        debug_assert!(self.builders.iter().all(|b| b.len() == self.pending + 1));
        self.pending += 1;
        self.rows += 1;
        if self.pending >= BATCH_ROWS {
            self.flush()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> anyhow::Result<()> {
        if self.pending == 0 {
            return Ok(());
        }
        let arrays = self.builders.iter_mut().map(Builder::finish).collect();
        let batch = RecordBatch::try_new(self.schema.clone(), arrays)
            .with_context(|| format!("building a batch of {}", self.name))?;
        self.writer.write(&batch)?;
        self.pending = 0;
        Ok(())
    }

    /// Writes the remaining rows and the file footer, and syncs the file to disk. Returns
    /// the number of rows written.
    pub fn finish(mut self) -> anyhow::Result<u64> {
        self.flush()?;
        let file = self
            .writer
            .into_inner()
            .with_context(|| format!("finishing {}", self.name))?;
        file.sync_all()?;
        Ok(self.rows)
    }
}
