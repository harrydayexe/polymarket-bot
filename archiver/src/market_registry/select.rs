use std::collections::{HashMap, HashSet};

use crate::{config::Config, market_registry::market_info::MarketInfo, types::ConditionId};

#[derive(Debug)]
pub struct SelectResults {
    pub markets: HashMap<ConditionId, MarketInfo>,
    pub category_rejections: u64,
    pub tradable_rejections: u64,
    pub liquidity_rejections: u64,
}

/// Select is a filter over markets which selects only the markets that data should be consumed
/// from.
///
/// # Returns
///
/// A struct containing the selected markets as well as counts of how many markets were rejected for
/// each reason.
pub fn select(markets: Vec<MarketInfo>, config: &Config) -> SelectResults {
    let mut seen: HashSet<ConditionId> = HashSet::new();
    let mut category_rejections = 0u64;
    let mut tradable_rejections = 0u64;
    let mut liquidity_rejections = 0u64;

    let selected_markets: HashMap<ConditionId, MarketInfo> = markets
        .into_iter()
        .filter(|market| seen.insert(market.condition_id.clone()))
        .filter(|market| {
            let ok = market
                .tags
                .iter()
                .any(|tag| config.category_tags.contains(tag));
            if !ok {
                category_rejections += 1;
            }
            ok
        })
        .filter(|market| {
            let ok = market.active && !market.closed && market.accepting_orders;
            if !ok {
                tradable_rejections += 1;
            }
            ok
        })
        .filter(|market| {
            let ok = market
                .liquidity_usd
                .as_decimal()
                .map(|v| v >= config.min_liquidity_usd)
                .unwrap_or(false);
            if !ok {
                liquidity_rejections += 1;
            }
            ok
        })
        .map(|market| (market.condition_id.clone(), market))
        .collect();

    SelectResults {
        markets: selected_markets,
        category_rejections,
        tradable_rejections,
        liquidity_rejections,
    }
}

#[cfg(test)]
mod tests {
    use crate::market_registry::test_support::{M, config};
    use crate::types::DecimalString;

    use super::*;
    use rust_decimal::Decimal;

    fn ids(r: &SelectResults) -> Vec<String> {
        let mut keys: Vec<String> = r.markets.keys().map(|id| id.0.clone()).collect();
        keys.sort();
        keys
    }

    // ---------- spec table ----------

    #[test]
    fn qualifying_market_is_selected_with_both_tokens() {
        let r = select(vec![M::new("a").build()], &config());
        assert_eq!(ids(&r), vec!["a"]);
        assert_eq!(
            r.markets[&ConditionId("a".to_string())].yes_token,
            "a-yes".to_string().into()
        );
        assert_eq!(
            r.markets[&ConditionId("a".to_string())].no_token,
            "a-no".to_string().into()
        );
        assert_eq!(r.category_rejections, 0);
        assert_eq!(r.tradable_rejections, 0);
        assert_eq!(r.liquidity_rejections, 0);
    }

    #[test]
    fn wrong_category_is_rejected() {
        let r = select(vec![M::new("a").tags(&["sports"]).build()], &config());
        assert!(r.markets.is_empty());
        assert_eq!(r.category_rejections, 1);
    }

    #[test]
    fn any_matching_tag_among_several_selects() {
        let r = select(
            vec![M::new("a").tags(&["sports", "geopolitics"]).build()],
            &config(),
        );
        assert_eq!(ids(&r), vec!["a"]);
        assert_eq!(r.category_rejections, 0);
    }

    #[test]
    fn below_liquidity_floor_is_rejected() {
        let r = select(vec![M::new("a").liquidity(200).build()], &config());
        assert!(r.markets.is_empty());
        assert_eq!(r.liquidity_rejections, 1);
    }

    // ---------- additional edge cases ----------

    #[test]
    fn liquidity_exactly_at_floor_is_selected() {
        let r = select(vec![M::new("a").liquidity(1_000).build()], &config());
        assert_eq!(ids(&r), vec!["a"]);
    }

    #[test]
    fn liquidity_just_below_floor_is_rejected() {
        let mut m = M::new("a").build();
        m.liquidity_usd = Decimal::new(99_999, 2).into(); // 999.99
        let r = select(vec![m], &config());
        assert!(r.markets.is_empty());
        assert_eq!(r.liquidity_rejections, 1);
    }

    #[test]
    fn unparseable_liquidity_is_rejected_as_liquidity() {
        // Adapt: construct a liquidity value whose as_decimal() returns None.
        let mut m = M::new("a").build();
        m.liquidity_usd = DecimalString("not-a-number".to_string()); // must yield None from as_decimal()
        assert!(
            m.liquidity_usd.as_decimal().is_err(),
            "fixture must be unparseable"
        );
        let r = select(vec![m], &config());
        assert!(r.markets.is_empty());
        assert_eq!(r.liquidity_rejections, 1);
    }

    #[test]
    fn inactive_closed_or_not_accepting_orders_is_rejected() {
        let r = select(
            vec![
                M::new("inactive").active(false).build(),
                M::new("closed").closed(true).build(),
                M::new("no-orders").accepting_orders(false).build(),
            ],
            &config(),
        );
        assert!(r.markets.is_empty());
        assert_eq!(r.tradable_rejections, 3);
        assert_eq!(r.category_rejections, 0);
        assert_eq!(r.liquidity_rejections, 0);
    }

    #[test]
    fn market_with_no_tags_is_rejected_on_category() {
        let r = select(vec![M::new("a").tags(&[]).build()], &config());
        assert!(r.markets.is_empty());
        assert_eq!(r.category_rejections, 1);
    }

    #[test]
    fn duplicate_condition_ids_are_deduplicated_and_not_counted_as_rejections() {
        let r = select(
            vec![
                M::new("a").build(),
                M::new("a").build(),
                M::new("a").build(),
            ],
            &config(),
        );
        assert_eq!(ids(&r), vec!["a"]);
        assert_eq!(r.category_rejections, 0);
        assert_eq!(r.tradable_rejections, 0);
        assert_eq!(r.liquidity_rejections, 0);
    }

    #[test]
    fn rejections_are_attributed_to_first_failing_filter_only() {
        // Fails all three; only category (first filter) should count it.
        let m = M::new("a")
            .tags(&["sports"])
            .closed(true)
            .liquidity(1)
            .build();
        let r = select(vec![m], &config());
        assert_eq!(r.category_rejections, 1);
        assert_eq!(r.tradable_rejections, 0);
        assert_eq!(r.liquidity_rejections, 0);
    }

    #[test]
    fn mixed_batch_selects_only_qualifying_and_counts_each_reason() {
        let r = select(
            vec![
                M::new("ok1").build(),
                M::new("sports").tags(&["sports"]).build(),
                M::new("closed").closed(true).build(),
                M::new("poor").liquidity(200).build(),
                M::new("ok2").tags(&["sports", "geopolitics"]).build(),
            ],
            &config(),
        );
        assert_eq!(ids(&r), vec!["ok1", "ok2"]);
        assert_eq!(r.category_rejections, 1);
        assert_eq!(r.tradable_rejections, 1);
        assert_eq!(r.liquidity_rejections, 1);
    }

    #[test]
    fn empty_input_yields_empty_results() {
        let r = select(vec![], &config());
        assert!(r.markets.is_empty());
        assert_eq!(r.category_rejections, 0);
        assert_eq!(r.tradable_rejections, 0);
        assert_eq!(r.liquidity_rejections, 0);
    }
}
