use std::collections::HashSet;

use crate::{config::Config, market_registry::market_info::MarketInfo, types::ConditionId};

#[derive(Debug)]
pub struct SelectResults {
    markets: Vec<MarketInfo>,
    category_rejections: u64,
    tradable_rejections: u64,
    liquidity_rejections: u64,
}

/// Select is a filter over markets which selects only the markets that data should be consumed
/// from.
///
/// # Returns
///
/// A struct containing the selected markets as well as counts of how many markets were rejected for
/// each reason.
pub fn select(markets: Vec<MarketInfo>, config: Config) -> SelectResults {
    let mut seen: HashSet<ConditionId> = HashSet::new();
    let mut category_rejections = 0u64;
    let mut tradable_rejections = 0u64;
    let mut liquidity_rejections = 0u64;

    let selected_markets: Vec<MarketInfo> = markets
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
    use crate::types::{DecimalString, UtcMicros};

    use super::*;
    use rust_decimal::Decimal;

    fn config() -> Config {
        Config {
            category_tags: ["politics", "geopolitics"]
                .into_iter()
                .map(String::from)
                .collect(),
            min_liquidity_usd: Decimal::from(1_000),
            ..Config::default()
        }
    }

    struct M(MarketInfo);

    impl M {
        fn new(id: &str) -> Self {
            M(MarketInfo {
                condition_id: ConditionId(id.to_string()),
                question: "".to_string(),
                slug: "".to_string(),
                event_id: None,
                tags: vec!["politics".into()],
                yes_token: format!("{id}-yes").into(),
                no_token: format!("{id}-no").into(),
                start_date: None,
                end_date: None,
                active: true,
                closed: false,
                liquidity_usd: Decimal::from(5_000).into(),
                tick_size: DecimalString("0".to_string()),
                neg_risk: false,
                fetched_at: UtcMicros(0),
                accepting_orders: true,
                fees_enabled: false,
                fee_type: None,
                fee_rate: None,
            })
        }
        fn tags(mut self, t: &[&str]) -> Self {
            self.0.tags = t.iter().map(|s| s.to_string()).collect();
            self
        }
        fn liquidity(mut self, v: i64) -> Self {
            self.0.liquidity_usd = Decimal::from(v).into();
            self
        }
        fn active(mut self, v: bool) -> Self {
            self.0.active = v;
            self
        }
        fn closed(mut self, v: bool) -> Self {
            self.0.closed = v;
            self
        }
        fn accepting_orders(mut self, v: bool) -> Self {
            self.0.accepting_orders = v;
            self
        }
        fn build(self) -> MarketInfo {
            self.0
        }
    }

    fn ids(r: &SelectResults) -> Vec<String> {
        r.markets.iter().map(|m| m.condition_id.0.clone()).collect()
    }

    // ---------- spec table ----------

    #[test]
    fn qualifying_market_is_selected_with_both_tokens() {
        let r = select(vec![M::new("a").build()], config());
        assert_eq!(ids(&r), vec!["a"]);
        assert_eq!(r.markets[0].yes_token, "a-yes".to_string().into());
        assert_eq!(r.markets[0].no_token, "a-no".to_string().into());
        assert_eq!(r.category_rejections, 0);
        assert_eq!(r.tradable_rejections, 0);
        assert_eq!(r.liquidity_rejections, 0);
    }

    #[test]
    fn wrong_category_is_rejected() {
        let r = select(vec![M::new("a").tags(&["sports"]).build()], config());
        assert!(r.markets.is_empty());
        assert_eq!(r.category_rejections, 1);
    }

    #[test]
    fn any_matching_tag_among_several_selects() {
        let r = select(
            vec![M::new("a").tags(&["sports", "geopolitics"]).build()],
            config(),
        );
        assert_eq!(ids(&r), vec!["a"]);
        assert_eq!(r.category_rejections, 0);
    }

    #[test]
    fn below_liquidity_floor_is_rejected() {
        let r = select(vec![M::new("a").liquidity(200).build()], config());
        assert!(r.markets.is_empty());
        assert_eq!(r.liquidity_rejections, 1);
    }

    // ---------- additional edge cases ----------

    #[test]
    fn liquidity_exactly_at_floor_is_selected() {
        let r = select(vec![M::new("a").liquidity(1_000).build()], config());
        assert_eq!(ids(&r), vec!["a"]);
    }

    #[test]
    fn liquidity_just_below_floor_is_rejected() {
        let mut m = M::new("a").build();
        m.liquidity_usd = Decimal::new(99_999, 2).into(); // 999.99
        let r = select(vec![m], config());
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
        let r = select(vec![m], config());
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
            config(),
        );
        assert!(r.markets.is_empty());
        assert_eq!(r.tradable_rejections, 3);
        assert_eq!(r.category_rejections, 0);
        assert_eq!(r.liquidity_rejections, 0);
    }

    #[test]
    fn market_with_no_tags_is_rejected_on_category() {
        let r = select(vec![M::new("a").tags(&[]).build()], config());
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
            config(),
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
        let r = select(vec![m], config());
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
            config(),
        );
        assert_eq!(ids(&r), vec!["ok1", "ok2"]);
        assert_eq!(r.category_rejections, 1);
        assert_eq!(r.tradable_rejections, 1);
        assert_eq!(r.liquidity_rejections, 1);
    }

    #[test]
    fn preserves_input_order() {
        let r = select(
            vec![
                M::new("c").build(),
                M::new("a").build(),
                M::new("b").build(),
            ],
            config(),
        );
        assert_eq!(ids(&r), vec!["c", "a", "b"]);
    }

    #[test]
    fn empty_input_yields_empty_results() {
        let r = select(vec![], config());
        assert!(r.markets.is_empty());
        assert_eq!(r.category_rejections, 0);
        assert_eq!(r.tradable_rejections, 0);
        assert_eq!(r.liquidity_rejections, 0);
    }
}
