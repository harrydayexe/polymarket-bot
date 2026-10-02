use core::fmt;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use crate::{
    clock::Clock,
    config::Config,
    market_registry::{
        market_info::MarketInfo,
        tracked_market::{
            ClosingCause::ClosedOnGamma,
            MarketStatus::{Closing, Tracking},
            TrackedMarket,
        },
    },
    types::{ConditionId, UtcMicros},
};

#[derive(Debug)]
pub struct PlannedChanges {
    add: HashMap<ConditionId, MarketInfo>,
    remove: HashSet<ConditionId>,
    details_changed: HashMap<ConditionId, TrackedMarket>,
}

impl fmt::Display for PlannedChanges {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Planned Changes:")?;
        writeln!(f, "- #Added: {}", self.add.len())?;
        writeln!(f, "\nDiscovered Markets")?;
        for market in self.add.values() {
            writeln!(f, "{}", market.question)?;
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct Registry {
    /// Markets currently being tracked (either open or in grace period).
    markets: HashMap<ConditionId, TrackedMarket>,
    /// Markets that have recently been removed.
    recently_removed: HashMap<ConditionId, UtcMicros>,
    last_accepted_count: u64,
    last_success_at: Option<UtcMicros>,
}

impl Registry {
    /// Apply a set of `PlannedChanges` to a registry object
    pub fn apply_changes(&mut self, plan: PlannedChanges, at: UtcMicros) {
        for id in &plan.remove {
            self.markets.remove(id);
        }

        for (id, info) in plan.add {
            self.markets.insert(
                id,
                TrackedMarket {
                    info,
                    added_at: at,
                    status: Tracking,
                },
            );
        }

        self.markets.extend(plan.details_changed);
    }

    /// Check the current set of tracked markets for markets where the grace period has expired.
    fn find_expired_markets_to_remove(&self, clock: &impl Clock) -> HashSet<ConditionId> {
        self.markets
            .iter()
            .filter(
            |(_, v)| matches!(v.status, Closing { grace_until, .. } if grace_until <= clock.wall_now()),
        )
            .map(|(k, _)| k.clone())
            .collect()
    }

    /// Check if a market should be selected.
    ///
    /// The criteria checked is: Tradability, Category, and Liquidity.
    pub fn should_select(&self, market: &MarketInfo, _config: Arc<Config>) -> bool {
        // Recently Removed Check
        if self.recently_removed.contains_key(&market.condition_id) {
            return false;
        }

        // Tradability Check
        if !market.active || market.closed || !market.accepting_orders {
            return false;
        }

        true
    }
}

pub fn plan_changes(
    registry: &Registry,
    results: Vec<MarketInfo>,
    config: Arc<Config>,
    clock: &impl Clock,
) -> PlannedChanges {
    let mut seen: HashSet<ConditionId> = HashSet::new();
    let mut add = HashMap::<ConditionId, MarketInfo>::new();
    let mut details_changed = HashMap::<ConditionId, TrackedMarket>::new();
    let remove = registry.find_expired_markets_to_remove(clock);

    for market in results {
        // Add condition ID to seen set and skip if seen previously
        if !seen.insert(market.condition_id.clone()) {
            continue;
        };

        // Check if market has already been removed
        if registry.recently_removed.contains_key(&market.condition_id)
            || remove.contains(&market.condition_id)
        {
            continue;
        }

        // Check if we already track the market
        let tm = registry.markets.get_key_value(&market.condition_id);
        match tm {
            // Market is not tracked
            None => {
                if registry.should_select(&market, config.clone()) {
                    add.insert(market.condition_id.clone(), market);
                }
            }
            // Market is tracked already
            Some((k, v)) => {
                // Gamma shows as closed, but currently tracking
                if market.closed && v.status == Tracking {
                    let mut updated_tm = v.clone();
                    updated_tm.info.update_from(&market);
                    updated_tm.status = Closing {
                        grace_until: clock.wall_in_hours(config.closed_grace_period_h),
                        cause: ClosedOnGamma,
                    };
                    details_changed.insert(k.clone(), updated_tm);
                }

                // Gamma shows as open and details have changed
                if !market.closed && !(v.info == market) {
                    let mut updated_tm = v.clone();
                    updated_tm.info.update_from(&market);
                    details_changed.insert(k.clone(), updated_tm);
                }
            }
        }
    }

    PlannedChanges {
        add,
        remove,
        details_changed,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::market_registry::{
        test_support::{FixedClock, HOUR, M, config},
        tracked_market::{ClosingCause::ClosedOnWebSocket, MarketStatus},
    };
    // use crate::types::TokenId;

    const T: UtcMicros = UtcMicros(1_790_000_000_000_000);

    fn at(offset_h: i64) -> UtcMicros {
        UtcMicros(T.0 + offset_h * HOUR)
    }

    fn id(s: &str) -> ConditionId {
        ConditionId(s.to_string())
    }

    fn tracked(info: MarketInfo, status: MarketStatus) -> TrackedMarket {
        TrackedMarket {
            info,
            added_at: at(-24),
            status,
        }
    }

    fn registry(markets: Vec<TrackedMarket>) -> Registry {
        Registry {
            markets: markets
                .into_iter()
                .map(|tm| (tm.info.condition_id.clone(), tm))
                .collect(),
            recently_removed: HashMap::new(),
            last_accepted_count: 0,
            last_success_at: None,
        }
    }

    fn plan(reg: &Registry, results: Vec<MarketInfo>) -> PlannedChanges {
        plan_changes(reg, results, Arc::new(config()), &FixedClock(T))
    }

    fn assert_no_changes(p: &PlannedChanges) {
        assert!(p.add.is_empty(), "add: {:?}", p.add.keys());
        assert!(p.remove.is_empty(), "remove: {:?}", p.remove);
        assert!(
            p.details_changed.is_empty(),
            "details_changed: {:?}",
            p.details_changed.keys()
        );
    }

    // ---------- selection rules ----------

    #[test]
    fn below_floor_new_market_is_not_selected() {
        let reg = registry(vec![]);
        let p = plan(&reg, vec![M::new("a").liquidity(200).build()]);
        assert_no_changes(&p);
    }

    #[test]
    fn below_floor_already_tracked_market_is_kept() {
        let reg = registry(vec![tracked(M::new("a").build(), Tracking)]);
        let p = plan(&reg, vec![M::new("a").liquidity(200).build()]);
        assert_no_changes(&p);
    }

    #[test]
    fn closed_inside_grace_period_is_kept() {
        // Closed 2 h ago with a 6 h grace period: grace ends in 4 h.
        let reg = registry(vec![tracked(
            M::new("a").closed(true).build(),
            Closing {
                grace_until: at(4),
                cause: ClosedOnGamma,
            },
        )]);
        let p = plan(&reg, vec![M::new("a").closed(true).build()]);
        assert_no_changes(&p);
    }

    #[test]
    fn closed_past_grace_period_is_removed() {
        // Closed 7 h ago with a 6 h grace period: grace ended 1 h ago.
        let reg = registry(vec![tracked(
            M::new("a").closed(true).build(),
            Closing {
                grace_until: at(-1),
                cause: ClosedOnGamma,
            },
        )]);
        let p = plan(&reg, vec![M::new("a").closed(true).build()]);
        assert_eq!(p.remove, HashSet::from([id("a")]));
        assert!(p.add.is_empty());
        assert!(p.details_changed.is_empty());
    }

    #[test]
    fn newly_closed_on_gamma_starts_grace_period() {
        let reg = registry(vec![tracked(M::new("a").build(), Tracking)]);
        let p = plan(&reg, vec![M::new("a").closed(true).build()]);
        assert!(p.add.is_empty());
        assert!(p.remove.is_empty());
        let tm = &p.details_changed[&id("a")];
        assert_eq!(
            tm.status,
            Closing {
                grace_until: at(6),
                cause: ClosedOnGamma,
            }
        );
        assert!(tm.info.closed);
    }

    #[test]
    fn resolved_via_websocket_past_grace_is_removed_while_gamma_says_open() {
        let reg = registry(vec![tracked(
            M::new("a").build(),
            Closing {
                grace_until: at(-1),
                cause: ClosedOnWebSocket,
            },
        )]);
        let p = plan(&reg, vec![M::new("a").build()]);
        assert_eq!(p.remove, HashSet::from([id("a")]));

        let mut reg = reg;
        reg.apply_changes(p, T);
        assert!(!reg.markets.contains_key(&id("a")));
    }

    #[test]
    fn resolved_via_websocket_past_grace_is_removed_even_if_details_changed() {
        let reg = registry(vec![tracked(
            M::new("a").build(),
            Closing {
                grace_until: at(-1),
                cause: ClosedOnWebSocket,
            },
        )]);
        let p = plan(&reg, vec![M::new("a").end_date(at(48)).build()]);

        let mut reg = reg;
        reg.apply_changes(p, T);
        assert!(!reg.markets.contains_key(&id("a")));
    }

    #[test]
    fn recently_removed_market_is_not_re_added() {
        let mut reg = registry(vec![]);
        reg.recently_removed.insert(id("a"), at(-1));
        let p = plan(&reg, vec![M::new("a").build()]);
        assert_no_changes(&p);
    }

    // ---------- difference ----------

    #[test]
    fn no_change_gives_empty_lists() {
        let reg = registry(vec![
            tracked(M::new("a").build(), Tracking),
            tracked(M::new("b").build(), Tracking),
        ]);
        let p = plan(&reg, vec![M::new("a").build(), M::new("b").build()]);
        assert_no_changes(&p);
    }

    // #[test]
    // fn new_market_adds_exactly_its_two_tokens() {
    //     let reg = registry(vec![tracked(M::new("a").build(), Tracking)]);
    //     let p = plan(&reg, vec![M::new("a").build(), M::new("b").build()]);
    //     assert!(p.remove.is_empty());
    //     assert!(p.details_changed.is_empty());
    //     assert_eq!(p.add.len(), 1);
    //     let added = &p.add[&id("b")];
    //     assert_eq!(added.yes_token, TokenId("b-yes".into()));
    //     assert_eq!(added.no_token, TokenId("b-no".into()));
    // }

    #[test]
    fn duplicate_results_are_added_once() {
        let reg = registry(vec![]);
        let p = plan(&reg, vec![M::new("a").build(), M::new("a").build()]);
        assert_eq!(p.add.len(), 1);
    }

    #[test]
    fn details_change_writes_record_without_add_or_remove() {
        let reg = registry(vec![tracked(
            M::new("a").end_date(at(24)).build(),
            Tracking,
        )]);
        let p = plan(&reg, vec![M::new("a").end_date(at(48)).build()]);
        assert!(p.add.is_empty());
        assert!(p.remove.is_empty());
        let tm = &p.details_changed[&id("a")];
        assert_eq!(tm.info.end_date, Some(at(48)));
        // assert_eq!(tm.info.yes_token, TokenId("a-yes".into()));
        // assert_eq!(tm.info.no_token, TokenId("a-no".into()));

        let mut reg = reg;
        reg.apply_changes(p, T);
        let tm = &reg.markets[&id("a")];
        assert_eq!(tm.info.end_date, Some(at(48)));
        assert_eq!(tm.added_at, at(-24));
        assert_eq!(tm.status, Tracking);
    }

    #[test]
    fn liquidity_only_change_is_not_a_details_change() {
        let reg = registry(vec![tracked(M::new("a").build(), Tracking)]);
        let p = plan(&reg, vec![M::new("a").liquidity(9_000).build()]);
        assert_no_changes(&p);
    }

    #[test]
    fn apply_changes_adds_as_tracking_at_given_time() {
        let mut reg = registry(vec![]);
        let p = plan(&reg, vec![M::new("a").build()]);
        reg.apply_changes(p, T);
        let tm = &reg.markets[&id("a")];
        assert_eq!(tm.added_at, T);
        assert_eq!(tm.status, Tracking);
    }
}
