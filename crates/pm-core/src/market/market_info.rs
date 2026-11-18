use chrono::{DateTime, Utc};
use marcasite::{
    gamma::Market,
    types::{ConditionId, EventId, TokenId},
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    market::token_changes::MarketTokens,
    units::{FeeParams, FeeSource},
};

/// Fee parameters from a Gamma market's `feesEnabled` and `feeSchedule` (§3.7, D13).
///
/// Fees disabled means rate 0 (exponent from the schedule if it has one, else 1). Otherwise
/// both the rate and a whole exponent are needed; anything else is `None`, so the caller falls
/// back to the CLOB.
pub fn gamma_fee_params(market: &Market, seen_at: DateTime<Utc>) -> Option<FeeParams> {
    let schedule = market.fee_schedule.as_ref();
    let exponent = schedule
        .and_then(|s| s.exponent)
        .map(marcasite_core::fees::exponent_from_decimal);
    let params = |rate: Decimal, exponent: u32| FeeParams {
        rate,
        exponent,
        source: FeeSource::Gamma,
        seen_at,
    };
    if market.fees_enabled == Some(false) {
        let exponent = exponent.and_then(Result::ok).unwrap_or(1);
        return Some(params(Decimal::ZERO, exponent));
    }
    let rate = schedule.and_then(|s| s.rate)?;
    let exponent = exponent?.ok()?;
    if rate < Decimal::ZERO {
        tracing::warn!(?market.condition_id, %rate, "Gamma fee schedule has a negative rate");
        return None;
    }
    Some(params(rate, exponent))
}

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
    /// Gamma `feesEnabled`. Not serialised: the archive's `markets/` records are unchanged,
    /// and the archiver ignores fees.
    #[serde(skip)]
    pub fees_enabled: Option<bool>,
    /// Fee parameters from Gamma `feeSchedule` (source `gamma`, seen at `fetched_at`); rate 0
    /// when fees are disabled; `None` when Gamma sent no usable schedule. Not serialised, as
    /// `fees_enabled`.
    #[serde(skip)]
    pub fee_params: Option<FeeParams>,
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

        let fee_params = gamma_fee_params(&market, fetched_at);
        let fees_enabled = market.fees_enabled;
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
            fees_enabled,
            fee_params,
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
mod tests {
    use serde_json::{Value, json};

    use super::*;
    use crate::test_util::{NO, YES, gamma_market};

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

    #[test]
    fn fee_params_from_the_gamma_schedule() {
        let info = parse(json!({"feesEnabled": true, "feeSchedule": {"rate": "0.01", "exponent": "1", "takerOnly": true}})).unwrap();
        let fees = info.fee_params.unwrap();
        assert_eq!(fees.rate, Decimal::new(1, 2));
        assert_eq!(fees.exponent, 1);
        assert_eq!(fees.source, FeeSource::Gamma);
        assert_eq!(fees.seen_at, info.fetched_at);
        assert_eq!(info.fees_enabled, Some(true));
        // Numbers as well as strings.
        let info = parse(json!({"feeSchedule": {"rate": 0.05, "exponent": 1}})).unwrap();
        assert_eq!(info.fee_params.unwrap().rate, Decimal::new(5, 2));
    }

    #[test]
    fn fees_disabled_means_rate_zero() {
        let info = parse(json!({"feesEnabled": false})).unwrap();
        let fees = info.fee_params.unwrap();
        assert_eq!(fees.rate, Decimal::ZERO);
        assert_eq!(fees.exponent, 1);
        let info =
            parse(json!({"feesEnabled": false, "feeSchedule": {"rate": "0.04", "exponent": "1"}}))
                .unwrap();
        assert_eq!(info.fee_params.unwrap().rate, Decimal::ZERO);
    }

    #[test]
    fn no_usable_schedule_gives_none() {
        assert_eq!(parse(json!({})).unwrap().fee_params, None);
        assert_eq!(
            parse(json!({"feesEnabled": true})).unwrap().fee_params,
            None
        );
        let no_exponent = json!({"feeSchedule": {"rate": "0.04"}});
        assert_eq!(parse(no_exponent).unwrap().fee_params, None);
        let fractional = json!({"feeSchedule": {"rate": "0.04", "exponent": "1.5"}});
        assert_eq!(parse(fractional).unwrap().fee_params, None);
        let negative = json!({"feeSchedule": {"rate": "-0.04", "exponent": "1"}});
        assert_eq!(parse(negative).unwrap().fee_params, None);
    }

    #[test]
    fn fee_fields_stay_out_of_the_archive() {
        let with_fees =
            parse(json!({"feesEnabled": true, "feeSchedule": {"rate": "0.01", "exponent": "1"}}))
                .unwrap();
        let without = MarketInfo {
            fees_enabled: None,
            fee_params: None,
            ..with_fees.clone()
        };
        assert_eq!(
            serde_json::to_string(&with_fees).unwrap(),
            serde_json::to_string(&without).unwrap()
        );
        assert!(!serde_json::to_string(&with_fees).unwrap().contains("fee"));
    }
}
