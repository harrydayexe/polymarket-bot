//! Builders shared by this crate's tests.

use chrono::{DateTime, Utc};
use marcasite::{
    types::{ConditionId, TokenId},
    ws::{BookEvent, LastTradePriceEvent, PriceChangeEvent},
};
use pm_core::{
    market::MarketInfo,
    test_util::{NO, YES, condition_id, gamma_market, t},
    units::{FeeParams, FeeSource},
};
use rust_decimal::Decimal;
use serde_json::{Value, json};

use crate::event::LiveMarket;

pub fn yes(n: u8) -> TokenId {
    TokenId::new(format!("{n}{YES}"))
}

pub fn no(n: u8) -> TokenId {
    TokenId::new(format!("{n}{NO}"))
}

pub fn condition(n: u8) -> ConditionId {
    ConditionId::new(condition_id(n))
}

pub fn fees(rate: Decimal, source: FeeSource, seen_at: DateTime<Utc>) -> FeeParams {
    FeeParams {
        rate,
        exponent: 1,
        source,
        seen_at,
    }
}

/// Market `n` of `pm_core::test_util::gamma_market`, paying 0.04 from Gamma.
pub fn live_market(n: u8) -> LiveMarket {
    let info = MarketInfo::from_gamma(gamma_market(n, json!({})), t(0)).unwrap();
    LiveMarket {
        info,
        fees_enabled: Some(true),
        fees: fees(Decimal::new(4, 2), FeeSource::Gamma, t(0)),
    }
}

fn levels(levels: &[(&str, &str)]) -> Value {
    levels
        .iter()
        .map(|(price, size)| json!({"price": price, "size": size}))
        .collect()
}

/// A `book` for `token` stamped `at_ms`.
pub fn book_json(
    token: &TokenId,
    bids: &[(&str, &str)],
    asks: &[(&str, &str)],
    at_ms: i64,
) -> Value {
    json!({
        "event_type": "book",
        "asset_id": token,
        "market": "0xabc",
        "bids": levels(bids),
        "asks": levels(asks),
        "timestamp": at_ms.to_string(),
        "hash": "h",
    })
}

pub fn book(
    token: &TokenId,
    bids: &[(&str, &str)],
    asks: &[(&str, &str)],
    at_ms: i64,
) -> BookEvent {
    serde_json::from_value(book_json(token, bids, asks, at_ms)).unwrap()
}

/// A `price_change` of one level: `(token, price, size, side)` with side `BUY` (bid) or
/// `SELL` (ask).
pub fn price_change_json(changes: &[(&TokenId, &str, &str, &str)], at_ms: i64) -> Value {
    let changes: Vec<Value> = changes
        .iter()
        .map(|(token, price, size, side)| {
            json!({"asset_id": token, "price": price, "size": size, "side": side, "hash": "h"})
        })
        .collect();
    json!({
        "event_type": "price_change",
        "market": "0xabc",
        "price_changes": changes,
        "timestamp": at_ms.to_string(),
    })
}

pub fn price_change(changes: &[(&TokenId, &str, &str, &str)], at_ms: i64) -> PriceChangeEvent {
    serde_json::from_value(price_change_json(changes, at_ms)).unwrap()
}

pub fn trade(
    market: &ConditionId,
    token: &TokenId,
    side: &str,
    price: &str,
    size: &str,
    tx: &str,
    at_ms: i64,
) -> LastTradePriceEvent {
    serde_json::from_value(json!({
        "event_type": "last_trade_price",
        "asset_id": token,
        "market": market,
        "price": price,
        "size": size,
        "side": side,
        "timestamp": at_ms.to_string(),
        "transaction_hash": tx,
    }))
    .unwrap()
}
