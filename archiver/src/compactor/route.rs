//! Splitting records into events (the design's `parse_frame`, section 5.8).
//!
//! Pure functions that never fail: anything that cannot become a row, including a price or
//! size with more decimal places than the fixed-point columns hold, becomes
//! [Event::Unparsed] carrying the event's JSON and the reason.

use chrono::{DateTime, Utc};
use marcasite::ws::MarketEvent;
use rust_decimal::Decimal;
use serde::Serialize;
use serde_json::Value;

use super::tables::DECIMAL_SCALE;
use crate::{
    archive::snapshot_book::SnapshotBook, market_registry::market_info::MarketInfo,
    messages::ControlBody,
};

/// A decimal as the mantissa of a `Decimal128(38, 6)`, or why it does not fit. Never
/// rounds: a value with more than six significant decimal places is an error.
pub fn fixed(value: Decimal) -> Result<i128, String> {
    let mut d = value;
    if d.scale() > DECIMAL_SCALE {
        // Trailing zeros (such as "0.50000000") lose nothing.
        d = d.normalize();
    }
    if d.scale() > DECIMAL_SCALE {
        return Err(format!(
            "{value} has more than {DECIMAL_SCALE} decimal places"
        ));
    }
    // A rust_decimal mantissa is below 2^96 (about 7.9e28), so this stays under the 38
    // digits of the column.
    Ok(d.mantissa() * 10_i128.pow(DECIMAL_SCALE - d.scale()))
}

fn fixed_opt(value: Option<Decimal>) -> Result<Option<i128>, String> {
    value.map(fixed).transpose()
}

fn micros(t: DateTime<Utc>) -> i64 {
    t.timestamp_micros()
}

/// One price level, as fixed-point price and size.
pub type Level = (i128, i128);

/// A full book, from a websocket `book` event or one book of a snapshot. Snapshot books
/// are stored as `/books` sent them, so only the token and the levels are certain.
#[derive(Debug, PartialEq)]
pub struct Book {
    /// Position of the book within its record (0 for a websocket book).
    pub index: usize,
    pub exchange_ts: Option<i64>,
    pub asset_id: String,
    pub market: Option<String>,
    pub hash: Option<String>,
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
}

#[derive(Debug, PartialEq)]
pub struct PriceChange<'a> {
    pub exchange_ts: i64,
    pub asset_id: &'a str,
    pub market: &'a str,
    pub side: &'a str,
    pub price: i128,
    pub size: i128,
    pub best_bid: Option<i128>,
    pub best_ask: Option<i128>,
    pub hash: &'a str,
}

#[derive(Debug, PartialEq)]
pub struct Trade<'a> {
    pub exchange_ts: i64,
    pub asset_id: &'a str,
    pub market: &'a str,
    pub price: i128,
    pub size: i128,
    pub side: &'a str,
    pub fee_rate_bps: Option<i128>,
    pub transaction_hash: Option<&'a str>,
}

#[derive(Debug, PartialEq)]
pub struct TickSize<'a> {
    pub exchange_ts: Option<i64>,
    pub exchange_ts_raw: &'a str,
    pub asset_id: &'a str,
    pub market: &'a str,
    pub old_tick_size: i128,
    pub new_tick_size: i128,
}

#[derive(Debug, PartialEq)]
pub struct BestBidAsk<'a> {
    pub exchange_ts: Option<i64>,
    pub exchange_ts_raw: &'a str,
    pub asset_id: &'a str,
    pub market: &'a str,
    pub best_bid: Option<i128>,
    pub best_ask: Option<i128>,
    pub spread: Option<i128>,
}

#[derive(Debug, PartialEq)]
pub struct Lifecycle<'a> {
    pub kind: &'static str,
    pub exchange_ts: Option<i64>,
    pub exchange_ts_raw: &'a str,
    pub market: &'a str,
    pub market_id: &'a str,
    pub asset_ids: Vec<&'a str>,
    pub question: Option<&'a str>,
    pub slug: Option<&'a str>,
    pub outcomes: Option<Vec<&'a str>>,
    pub winning_asset_id: Option<&'a str>,
    pub winning_outcome: Option<&'a str>,
    pub tags: Option<Vec<&'a str>>,
    pub event: String,
}

/// One event extracted from a websocket or snapshot record; each is one row of an event
/// table, except a book, which is a row per level.
#[derive(Debug, PartialEq)]
pub enum Event<'a> {
    Book(Book),
    PriceChange(PriceChange<'a>),
    Trade(Trade<'a>),
    TickSize(TickSize<'a>),
    BestBidAsk(BestBidAsk<'a>),
    Lifecycle(Lifecycle<'a>),
    Unparsed { error: String, raw: String },
}

fn to_json(value: &impl Serialize) -> String {
    serde_json::to_string(value).unwrap_or_else(|e| format!("<not serialisable: {e}>"))
}

fn unparsed(error: String, value: &impl Serialize) -> Event<'static> {
    Event::Unparsed {
        error,
        raw: to_json(value),
    }
}

/// Price levels as fixed point. Websocket and REST books have their own level types, so
/// this takes `(price, size)` pairs.
fn levels(levels: impl IntoIterator<Item = (Decimal, Decimal)>) -> Result<Vec<Level>, String> {
    levels
        .into_iter()
        .map(|(price, size)| Ok((fixed(price)?, fixed(size)?)))
        .collect()
}

fn strs<T: AsRef<str>>(items: &[T]) -> Vec<&str> {
    items.iter().map(AsRef::as_ref).collect()
}

/// The events of one websocket record (one market channel message).
pub fn ws_events(event: &MarketEvent) -> Vec<Event<'_>> {
    let parsed = match event {
        MarketEvent::Book(b) => (|| {
            Ok(vec![Event::Book(Book {
                index: 0,
                exchange_ts: Some(micros(b.timestamp)),
                asset_id: b.asset_id.to_string(),
                market: Some(b.market.to_string()),
                hash: Some(b.hash.clone()),
                bids: levels(b.bids.iter().map(|l| (l.price, l.size)))?,
                asks: levels(b.asks.iter().map(|l| (l.price, l.size)))?,
            })])
        })(),
        MarketEvent::PriceChange(pc) => pc
            .price_changes
            .iter()
            .map(|c| {
                Ok(Event::PriceChange(PriceChange {
                    exchange_ts: micros(pc.timestamp),
                    asset_id: c.asset_id.as_str(),
                    market: pc.market.as_str(),
                    side: c.side.as_str(),
                    price: fixed(c.price)?,
                    size: fixed(c.size)?,
                    best_bid: fixed_opt(c.best_bid)?,
                    best_ask: fixed_opt(c.best_ask)?,
                    hash: &c.hash,
                }))
            })
            .collect(),
        MarketEvent::LastTradePrice(t) => (|| {
            Ok(vec![Event::Trade(Trade {
                exchange_ts: micros(t.timestamp),
                asset_id: t.asset_id.as_str(),
                market: t.market.as_str(),
                price: fixed(t.price)?,
                size: fixed(t.size)?,
                side: t.side.as_str(),
                fee_rate_bps: fixed_opt(t.fee_rate_bps)?,
                transaction_hash: t.transaction_hash.as_deref(),
            })])
        })(),
        MarketEvent::TickSizeChange(t) => (|| {
            Ok(vec![Event::TickSize(TickSize {
                exchange_ts: t.timestamp_millis().map(micros),
                exchange_ts_raw: &t.timestamp,
                asset_id: t.asset_id.as_str(),
                market: t.market.as_str(),
                old_tick_size: fixed(t.old_tick_size)?,
                new_tick_size: fixed(t.new_tick_size)?,
            })])
        })(),
        MarketEvent::BestBidAsk(b) => (|| {
            Ok(vec![Event::BestBidAsk(BestBidAsk {
                exchange_ts: b.timestamp_millis().map(micros),
                exchange_ts_raw: &b.timestamp,
                asset_id: b.asset_id.as_str(),
                market: b.market.as_str(),
                best_bid: fixed_opt(b.best_bid)?,
                best_ask: fixed_opt(b.best_ask)?,
                spread: fixed_opt(b.spread)?,
            })])
        })(),
        MarketEvent::NewMarket(m) => Ok(vec![Event::Lifecycle(Lifecycle {
            kind: "new_market",
            exchange_ts: m.timestamp_millis().map(micros),
            exchange_ts_raw: &m.timestamp,
            market: m.market.as_str(),
            market_id: m.id.as_str(),
            asset_ids: strs(&m.assets_ids),
            question: Some(&m.question),
            slug: Some(&m.slug),
            outcomes: Some(strs(&m.outcomes)),
            winning_asset_id: None,
            winning_outcome: None,
            tags: m.tags.as_deref().map(strs),
            event: to_json(event),
        })]),
        MarketEvent::MarketResolved(m) => Ok(vec![Event::Lifecycle(Lifecycle {
            kind: "market_resolved",
            exchange_ts: m.timestamp_millis().map(micros),
            exchange_ts_raw: &m.timestamp,
            market: m.market.as_str(),
            market_id: m.id.as_str(),
            asset_ids: strs(&m.assets_ids),
            question: None,
            slug: None,
            outcomes: None,
            winning_asset_id: Some(m.winning_asset_id.as_str()),
            winning_outcome: Some(&m.winning_outcome),
            tags: m.tags.as_deref().map(strs),
            event: to_json(event),
        })]),
        MarketEvent::Unknown(value) => Err(match value.get("event_type") {
            Some(Value::String(kind)) => format!("unrecognised event_type {kind:?}"),
            Some(_) => "event_type is not a string".to_string(),
            None => "no event_type".to_string(),
        }),
        // MarketEvent is non-exhaustive: a type added to marcasite later is kept, not lost.
        _ => Err("event type not handled by the compactor".to_string()),
    };
    parsed.unwrap_or_else(|error| vec![unparsed(error, event)])
}

/// The events of one snapshot record: one book per element of `books` (each as `/books`
/// sent it). A book that cannot be read is one unparsed event.
pub fn snapshot_events(books: &[Value]) -> Vec<Event<'static>> {
    books
        .iter()
        .enumerate()
        .map(|(index, value)| {
            let book = SnapshotBook::parse(value).and_then(|b| {
                Ok(Book {
                    index,
                    exchange_ts: b.timestamp.map(micros),
                    bids: levels(b.bids.iter().map(|l| (l.price, l.size)))?,
                    asks: levels(b.asks.iter().map(|l| (l.price, l.size)))?,
                    asset_id: b.asset_id.into_inner(),
                    market: b.market.map(|m| m.into_inner()),
                    hash: b.hash,
                })
            });
            book.map_or_else(|error| unparsed(error, value), Event::Book)
        })
        .collect()
}

/// The columns of a control row, read from the body's JSON so that a new kind of control
/// record still fills what it can.
#[derive(Debug, PartialEq)]
pub struct ControlRow {
    pub kind: String,
    pub tokens: Option<Vec<String>>,
    pub start: Option<i64>,
    pub end: Option<i64>,
    pub reason: Option<String>,
    pub detail: Option<String>,
    pub slot: Option<String>,
    pub json: String,
}

pub fn control_row(body: &ControlBody) -> ControlRow {
    let value = serde_json::to_value(body).unwrap_or(Value::Null);
    let text = |key: &str| value.get(key).and_then(Value::as_str).map(str::to_owned);
    let int = |key: &str| value.get(key).and_then(Value::as_i64);
    let tokens = match (value.get("tokens"), value.get("token")) {
        (Some(Value::Array(items)), _) => Some(
            items
                .iter()
                .map(|t| t.as_str().map_or_else(|| t.to_string(), str::to_owned))
                .collect(),
        ),
        (_, Some(Value::String(token))) => Some(vec![token.clone()]),
        _ => None,
    };
    ControlRow {
        kind: text("kind").unwrap_or_default(),
        tokens,
        start: int("start").or_else(|| int("since")),
        end: int("end"),
        reason: text("reason"),
        detail: text("detail").or_else(|| text("error")),
        slot: text("slot"),
        json: value.to_string(),
    }
}

/// The fixed-point tick size of a market, or `None` if it has none or does not fit the
/// column (the exact value is still in the row's `json`).
pub fn market_tick_size(market: &MarketInfo) -> Option<i128> {
    market.tick_size.and_then(|t| fixed(t).ok())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn dec(s: &str) -> Decimal {
        s.parse().unwrap()
    }

    #[test]
    fn fixed_point_is_exact() {
        assert_eq!(fixed(dec("0.001")), Ok(1_000));
        assert_eq!(fixed(dec("219.217767")), Ok(219_217_767));
        assert_eq!(fixed(dec("0")), Ok(0));
        assert_eq!(fixed(dec("-1.5")), Ok(-1_500_000));
        assert_eq!(fixed(dec("0.50000000")), Ok(500_000));
        assert!(fixed(dec("0.1234567")).is_err());
    }

    #[test]
    fn too_many_places_makes_the_whole_event_unparsed() {
        let event: MarketEvent = serde_json::from_value(json!({
            "event_type":"price_change","market":"0xabc","timestamp":"1790348400121",
            "price_changes":[
                {"asset_id":"1","price":"0.5","size":"1","side":"BUY","hash":"a"},
                {"asset_id":"2","price":"0.5","size":"1.0000001","side":"SELL","hash":"b"}]
        }))
        .unwrap();
        let events = ws_events(&event);
        assert_eq!(events.len(), 1);
        let Event::Unparsed { error, raw } = &events[0] else {
            panic!("{events:?}")
        };
        assert!(error.contains("1.0000001"), "{error}");
        assert!(raw.contains("\"1.0000001\""), "{raw}");
    }

    #[test]
    fn control_columns_come_from_the_body() {
        let row = control_row(&ControlBody::TokenUnhealthy {
            token: "7".into(),
            since: DateTime::from_timestamp_micros(5).unwrap(),
        });
        assert_eq!(row.kind, "token_unhealthy");
        assert_eq!(row.tokens, Some(vec!["7".to_string()]));
        assert_eq!(row.start, Some(5));
        let row = control_row(&ControlBody::FrameDecodeFailed { error: "x".into() });
        assert_eq!(row.detail.as_deref(), Some("x"));
        assert_eq!(row.tokens, None);
    }
}
