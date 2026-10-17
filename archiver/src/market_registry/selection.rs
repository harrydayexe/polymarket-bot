use chrono::{DateTime, Utc};
use marcasite::{gamma::Market, types::ConditionId};

use crate::{
    config::Config,
    market_registry::market_info::{MarketInfo, MarketParseError},
};

/// A Gamma market that could not be parsed.
#[derive(Debug)]
pub struct SkippedMarket {
    pub condition_id: Option<ConditionId>,
    pub reason: MarketParseError,
}

/// Parse Gamma markets, skipping any that are malformed so one bad market cannot stop the rest.
pub fn parse_markets(
    markets: Vec<Market>,
    fetched_at: DateTime<Utc>,
) -> (Vec<MarketInfo>, Vec<SkippedMarket>) {
    let mut parsed = Vec::with_capacity(markets.len());
    let mut skipped = Vec::new();

    for market in markets {
        let condition_id = market.condition_id.clone();
        match MarketInfo::from_gamma(market, fetched_at) {
            Ok(info) => parsed.push(info),
            Err(reason) => skipped.push(SkippedMarket {
                condition_id,
                reason,
            }),
        }
    }

    (parsed, skipped)
}

/// Check if a market meets the rules for *joining* the tracked set: a configured category tag, open
/// and accepting orders, and liquidity at or above the floor.
pub fn select(market: &MarketInfo, config: &Config) -> bool {
    let in_category = market
        .tags
        .iter()
        .any(|tag| config.category_tags.contains(tag));
    let tradable = market.active && !market.closed && market.accepting_orders;
    let liquid = market
        .liquidity_usd
        .is_some_and(|liquidity| liquidity >= config.min_liquidity_usd);

    in_category && tradable && liquid
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::market_registry::market_info::tests::gamma_market;

    fn selected(overrides: serde_json::Value) -> bool {
        let info = MarketInfo::from_gamma(gamma_market(1, overrides), Utc::now()).unwrap();
        select(&info, &Config::default())
    }

    #[test]
    fn qualifies() {
        assert!(selected(json!({})));
    }

    #[test]
    fn wrong_category() {
        assert!(!selected(json!({ "tags": [{ "slug": "sports" }] })));
    }

    #[test]
    fn several_tags() {
        assert!(selected(
            json!({ "tags": [{ "slug": "sports" }, { "slug": "geopolitics" }] })
        ));
    }

    #[test]
    fn below_floor() {
        assert!(!selected(json!({ "liquidityNum": 200 })));
        assert!(!selected(json!({ "liquidityNum": null })));
    }

    #[test]
    fn not_tradable() {
        assert!(!selected(json!({ "acceptingOrders": false })));
        assert!(!selected(json!({ "active": false })));
        assert!(!selected(json!({ "closed": true })));
    }

    #[test]
    fn malformed_market_is_skipped_and_others_kept() {
        let markets = vec![
            gamma_market(1, json!({})),
            gamma_market(2, json!({ "clobTokenIds": "[\"1\"]" })),
            gamma_market(3, json!({})),
        ];
        let (parsed, skipped) = parse_markets(markets, Utc::now());
        assert_eq!(parsed.len(), 2);
        assert_eq!(skipped.len(), 1);
        assert_eq!(skipped[0].reason, MarketParseError::TokenCount(1));
    }
}
