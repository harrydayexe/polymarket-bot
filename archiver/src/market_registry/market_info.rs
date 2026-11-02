use chrono::{DateTime, Utc};
use marcasite::{
    gamma::Market,
    types::{ConditionId, EventId, TokenId},
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::market_registry::token_changes::MarketTokens;

/// A market as last seen on Gamma, reduced to the fields the archiver uses.
///
/// Serialised into `markets/` records, with times as microseconds since the epoch.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MarketInfo {
    pub condition_id: ConditionId,
    pub question: String,
    /// Short name used in the website address.
    pub slug: Option<String>,
    /// Groups related markets.
    pub event_id: Option<EventId>,
    /// Tag slugs.
    pub tags: Vec<String>,
    pub tokens: MarketTokens,
    #[serde(with = "chrono::serde::ts_microseconds_option")]
    pub start_date: Option<DateTime<Utc>>,
    #[serde(with = "chrono::serde::ts_microseconds_option")]
    pub end_date: Option<DateTime<Utc>>,
    pub active: bool,
    pub closed: bool,
    pub accepting_orders: bool,
    pub liquidity_usd: Option<Decimal>,
    pub tick_size: Option<Decimal>,
    /// Part of a multi-outcome group.
    pub neg_risk: bool,
    #[serde(with = "chrono::serde::ts_microseconds")]
    pub fetched_at: DateTime<Utc>,
}

/// Why a Gamma market could not be turned into a [MarketInfo].
#[derive(Debug, Error, PartialEq)]
pub enum MarketParseError {
    #[error("market has no {0}")]
    Missing(&'static str),
    #[error("market has {0} clob_token_ids, expected 2")]
    TokenCount(usize),
    #[error("market outcomes are {0:?}, expected Yes and No")]
    Outcomes(Vec<String>),
}

impl MarketInfo {
    pub fn from_gamma(market: Market, fetched_at: DateTime<Utc>) -> Result<Self, MarketParseError> {
        use MarketParseError::Missing;

        let outcomes = market.outcomes.ok_or(Missing("outcomes"))?;
        let token_ids = market.clob_token_ids.ok_or(Missing("clob_token_ids"))?;
        let [t0, t1] = <[TokenId; 2]>::try_from(token_ids)
            .map_err(|ids| MarketParseError::TokenCount(ids.len()))?;

        let tokens = match outcomes.as_slice() {
            [a, b] if a == "Yes" && b == "No" => MarketTokens { yes: t0, no: t1 },
            [a, b] if a == "No" && b == "Yes" => MarketTokens { yes: t1, no: t0 },
            _ => return Err(MarketParseError::Outcomes(outcomes)),
        };

        Ok(Self {
            condition_id: market.condition_id.ok_or(Missing("condition_id"))?,
            question: market.question.ok_or(Missing("question"))?,
            slug: market.slug,
            event_id: market
                .events
                .and_then(|events| events.into_iter().find_map(|event| event.id)),
            tags: market
                .tags
                .unwrap_or_default()
                .into_iter()
                .filter_map(|tag| tag.slug)
                .collect(),
            tokens,
            start_date: market.start_date,
            end_date: market.end_date,
            active: market.active.ok_or(Missing("active"))?,
            closed: market.closed.ok_or(Missing("closed"))?,
            accepting_orders: market.accepting_orders.ok_or(Missing("accepting_orders"))?,
            liquidity_usd: market.liquidity_num,
            tick_size: market.order_price_min_tick_size,
            neg_risk: market.neg_risk.unwrap_or(false),
            fetched_at,
        })
    }

    /// Compares the fields that matter for later analysis. Fields that change constantly, such as
    /// liquidity, are ignored so they don't flood the archive with market records.
    pub fn details_match(&self, other: &MarketInfo) -> bool {
        self.question == other.question
            && self.start_date == other.start_date
            && self.end_date == other.end_date
            && self.tags == other.tags
            && self.tick_size == other.tick_size
            && self.tokens == other.tokens
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use serde_json::{Value, json};

    use super::*;

    pub const YES: &str = "111";
    pub const NO: &str = "222";

    pub fn condition_id(n: u8) -> String {
        format!("0x{n:064x}")
    }

    /// A Gamma market that parses and qualifies under the default config, with fields overridden
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

    fn parse(overrides: Value) -> Result<MarketInfo, MarketParseError> {
        MarketInfo::from_gamma(gamma_market(1, overrides), Utc::now())
    }

    #[test]
    fn maps_yes_and_no_tokens() {
        let info = parse(json!({})).unwrap();
        assert_eq!(info.tokens.yes, TokenId::from(format!("1{YES}")));
        assert_eq!(info.tokens.no, TokenId::from(format!("1{NO}")));
        assert_eq!(info.tags, vec!["politics".to_string()]);
    }

    #[test]
    fn maps_tokens_when_outcomes_are_reversed() {
        let info = parse(json!({ "outcomes": "[\"No\", \"Yes\"]" })).unwrap();
        assert_eq!(info.tokens.yes, TokenId::from(format!("1{NO}")));
        assert_eq!(info.tokens.no, TokenId::from(format!("1{YES}")));
    }

    #[test]
    fn rejects_missing_token_ids() {
        let err = parse(json!({ "clobTokenIds": null })).unwrap_err();
        assert_eq!(err, MarketParseError::Missing("clob_token_ids"));
    }

    #[test]
    fn rejects_one_token() {
        let err = parse(json!({ "clobTokenIds": "[\"111\"]" })).unwrap_err();
        assert_eq!(err, MarketParseError::TokenCount(1));
    }

    #[test]
    fn rejects_named_outcomes() {
        let err = parse(json!({ "outcomes": "[\"Alice\", \"Bob\"]" })).unwrap_err();
        assert!(matches!(err, MarketParseError::Outcomes(_)));
    }

    #[test]
    fn rejects_missing_condition_id() {
        let err = parse(json!({ "conditionId": null })).unwrap_err();
        assert_eq!(err, MarketParseError::Missing("condition_id"));
    }

    #[test]
    fn details_ignore_liquidity_and_fetch_time() {
        let a = parse(json!({})).unwrap();
        let b = MarketInfo::from_gamma(gamma_market(1, json!({ "liquidityNum": 1 })), Utc::now())
            .unwrap();
        assert!(a.details_match(&b));
    }

    #[test]
    fn details_compare_question_and_tick_size() {
        let a = parse(json!({})).unwrap();
        assert!(!a.details_match(&parse(json!({ "question": "Edited?" })).unwrap()));
        assert!(!a.details_match(&parse(json!({ "orderPriceMinTickSize": 0.001 })).unwrap()));
    }
}
