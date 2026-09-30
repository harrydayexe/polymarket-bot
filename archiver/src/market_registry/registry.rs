use std::collections::{HashMap, HashSet};

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

#[derive(Debug)]
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
    pub fn should_select(&self, market: &MarketInfo, config: &Config) -> bool {
        // Recently Removed Check
        if self.recently_removed.contains_key(&market.condition_id) {
            return false;
        }

        // Tradability Check
        if !market.active || market.closed || !market.accepting_orders {
            return false;
        }

        // Category Check
        if !market
            .tags
            .iter()
            .any(|tag| config.category_tags.contains(tag))
        {
            return false;
        }

        // Liquidity Check
        if !market
            .liquidity_usd
            .as_decimal()
            .map(|v| v >= config.min_liquidity_usd)
            .unwrap_or(false)
        {
            return false;
        }

        true
    }
}

pub fn plan_changes(
    registry: &Registry,
    results: Vec<MarketInfo>,
    config: &Config,
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
        if registry.recently_removed.contains_key(&market.condition_id) {
            continue;
        }

        // Check if we already track the market
        let tm = registry.markets.get_key_value(&market.condition_id);
        match tm {
            // Market is not tracked
            None => {
                if registry.should_select(&market, config) {
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
