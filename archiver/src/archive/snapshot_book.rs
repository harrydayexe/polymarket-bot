//! Reading the books of a snapshot record, which are stored exactly as `/books` sent them.

use chrono::{DateTime, Utc};
use marcasite::types::{ConditionId, TokenId};
use rust_decimal::Decimal;
use serde_json::Value;

/// One price level.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Level {
    pub price: Decimal,
    pub size: Decimal,
}

/// A book from a `/books` response. Only the token and the levels are required; the other
/// fields are `None` when missing or empty (the exchange sends `"last_trade_price": ""` for a
/// token that has not traded).
#[derive(Debug, Clone, PartialEq)]
pub struct SnapshotBook {
    pub asset_id: TokenId,
    pub market: Option<ConditionId>,
    /// Exchange time of the book (sent as a string of Unix milliseconds).
    pub timestamp: Option<DateTime<Utc>>,
    pub hash: Option<String>,
    /// As sent; not necessarily sorted.
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
    pub tick_size: Option<Decimal>,
    pub min_order_size: Option<Decimal>,
    pub neg_risk: Option<bool>,
    pub last_trade_price: Option<Decimal>,
}

impl SnapshotBook {
    /// # Errors
    ///
    /// If the token or either side of the book is missing or malformed.
    pub fn parse(book: &Value) -> Result<Self, String> {
        let text = |key: &str| book.get(key).and_then(Value::as_str);
        let asset_id = text("asset_id")
            .filter(|s| !s.is_empty())
            .ok_or("book has no asset_id")?;
        Ok(Self {
            asset_id: TokenId::new(asset_id),
            market: text("market").map(ConditionId::new),
            timestamp: text("timestamp")
                .and_then(|t| t.trim().parse::<i64>().ok())
                .and_then(DateTime::from_timestamp_millis),
            hash: text("hash").map(str::to_string),
            bids: levels(book, "bids")?,
            asks: levels(book, "asks")?,
            tick_size: decimal(book.get("tick_size")),
            min_order_size: decimal(book.get("min_order_size")),
            neg_risk: book.get("neg_risk").and_then(Value::as_bool),
            last_trade_price: decimal(book.get("last_trade_price")),
        })
    }
}

fn levels(book: &Value, side: &str) -> Result<Vec<Level>, String> {
    let levels = book
        .get(side)
        .and_then(Value::as_array)
        .ok_or_else(|| format!("book has no {side} list"))?;
    levels
        .iter()
        .map(|level| {
            let field = |key: &str| {
                decimal(level.get(key)).ok_or_else(|| format!("{side} level has no valid {key}"))
            };
            Ok(Level {
                price: field("price")?,
                size: field("size")?,
            })
        })
        .collect()
}

/// A decimal sent as a string (or, leniently, a number). `None` if missing, empty or invalid.
fn decimal(value: Option<&Value>) -> Option<Decimal> {
    match value? {
        Value::String(s) => s.trim().parse().ok(),
        Value::Number(n) => n.to_string().parse().ok(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal_macros::dec;
    use serde_json::json;

    use super::*;

    #[test]
    fn parses_a_live_shaped_book() {
        let book = SnapshotBook::parse(&json!({
            "market":"0xabc","asset_id":"111","timestamp":"1790348400120","hash":"h",
            "bids":[{"price":"0.054","size":"50.33"}],"asks":[{"price":"0.06","size":"219.217767"}],
            "min_order_size":"5","tick_size":"0.001","neg_risk":true,"last_trade_price":""
        }))
        .unwrap();
        assert_eq!(book.asset_id, TokenId::new("111"));
        assert_eq!(
            book.timestamp,
            DateTime::from_timestamp_millis(1_790_348_400_120)
        );
        assert_eq!(
            book.bids,
            [Level {
                price: dec!(0.054),
                size: dec!(50.33)
            }]
        );
        assert_eq!(book.asks[0].size.to_string(), "219.217767");
        assert_eq!(book.tick_size, Some(dec!(0.001)));
        assert_eq!(book.neg_risk, Some(true));
        assert_eq!(book.last_trade_price, None);
    }

    #[test]
    fn missing_levels_or_token_are_errors() {
        assert!(SnapshotBook::parse(&json!({"asset_id":"1","bids":[]})).is_err());
        assert!(SnapshotBook::parse(&json!({"bids":[],"asks":[]})).is_err());
        assert!(
            SnapshotBook::parse(&json!({"asset_id":"1","bids":[{"price":""}],"asks":[]})).is_err()
        );
        let empty = SnapshotBook::parse(&json!({"asset_id":"1","bids":[],"asks":[]})).unwrap();
        assert!(empty.bids.is_empty() && empty.market.is_none());
    }
}
