//! Helpers for tests, in this crate and in crates that depend on it (feature `test-util`).
//!
//! Also re-exports the test doubles that live with what they stand in for:
//! [TokioClock] and [ScriptedRandom].

use std::time::Duration;

use chrono::{DateTime, TimeZone, Utc};
use marcasite::gamma::Market;
use rust_decimal::Decimal;
use serde_json::{Value, json};

use crate::market::{registry::RegistryConfig, selection::SelectionRules};
pub use crate::{clock::TokioClock, random::ScriptedRandom};

/// The time `ms` milliseconds after the epoch.
pub fn t(ms: i64) -> DateTime<Utc> {
    Utc.timestamp_millis_opt(ms).unwrap()
}

/// The archiver's default registry settings: politics and geopolitics, a $1,000 liquidity
/// floor, a 5-minute cycle and a 6-hour grace period. [gamma_market] qualifies under them.
pub fn registry_config() -> RegistryConfig {
    RegistryConfig {
        selection: SelectionRules {
            category_tags: vec!["politics".into(), "geopolitics".into()],
            min_liquidity_usd: Decimal::from(1000),
        },
        registry_interval: Duration::from_secs(300),
        closed_grace_period: Duration::from_hours(6),
    }
}

pub const YES: &str = "111";
pub const NO: &str = "222";

pub fn condition_id(n: u8) -> String {
    format!("0x{n:064x}")
}

/// A Gamma market that parses and qualifies under [registry_config], with fields overridden
/// by `overrides`. A `null` override removes the field.
pub fn gamma_market(n: u8, overrides: Value) -> Market {
    let mut market = json!({
        "conditionId": condition_id(n),
        "question": format!("Question {n}?"),
        "slug": format!("question-{n}"),
        "outcomes": "[\"Yes\", \"No\"]",
        "clobTokenIds": format!("[\"{n}{YES}\", \"{n}{NO}\"]"),
        "active": true,
        "closed": false,
        "acceptingOrders": true,
        "liquidityNum": 5000,
        "orderPriceMinTickSize": 0.01,
        "tags": [{ "slug": "politics" }],
    });
    for (key, value) in overrides.as_object().expect("overrides must be an object") {
        match value {
            Value::Null => market.as_object_mut().unwrap().remove(key),
            _ => market
                .as_object_mut()
                .unwrap()
                .insert(key.clone(), value.clone()),
        };
    }
    serde_json::from_value(market).expect("fixture should deserialize")
}
