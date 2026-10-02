use anyhow::{Context, Result};
use core::fmt;
use futures_util::StreamExt;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use chrono::{DateTime, TimeDelta, Utc};
use marcasite::{Paginated, data::ConditionId, gamma::Market};

use crate::{
    clock::{Clock, SharedClock},
    config::Config,
    market_registry::tracked_market::{
        MarketStatus::{self},
        TrackedMarket,
    },
};

use super::tracked_market::ClosingCause;

#[derive(Debug)]
pub struct PlannedChanges {
    add: HashMap<ConditionId, TrackedMarket>,
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
    recently_removed: HashMap<ConditionId, DateTime<Utc>>,
    last_accepted_count: u64,
    last_success_at: Option<DateTime<Utc>>,
}

impl Registry {
    /// Apply a set of `PlannedChanges` to a registry object
    pub fn apply_changes(&mut self, plan: PlannedChanges) {
        for id in &plan.remove {
            self.markets.remove(id);
        }

        for (id, market) in plan.add {
            self.markets.insert(id, market);
        }

        self.markets.extend(plan.details_changed);
    }

    /// Check the current set of tracked markets for markets where the grace period has expired.
    fn find_expired_markets_to_remove(&self, clock: SharedClock) -> HashSet<ConditionId> {
        self.markets
            .iter()
            .filter( |(_, v)| matches!(v.status, MarketStatus::Closing { grace_until, .. } if grace_until <= clock.now()))
            .map(|(k, _)| k.clone())
            .collect()
    }

    /// Check if a market should be selected.
    ///
    /// The criteria checked is: Tradability, Category, and Liquidity.
    pub fn should_select(&self, market: &TrackedMarket, _config: Arc<Config>) -> bool {
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

pub async fn plan_changes(
    registry: &Registry,
    mut results: Paginated<Market>,
    config: Arc<Config>,
    clock: SharedClock,
) -> Result<PlannedChanges> {
    let mut seen: HashSet<ConditionId> = HashSet::new();
    let mut add = HashMap::<ConditionId, TrackedMarket>::new();
    let mut details_changed = HashMap::<ConditionId, TrackedMarket>::new();
    let remove = registry.find_expired_markets_to_remove(clock.clone());

    let mut count = 0usize;
    let tags = config.category_tags.clone();
    while let Some(market_resp) = results.next().await {
        let market_resp = market_resp.with_context(|| {
            format!("listing markets (tags={tags:?}, closed=false) failed after {count} markets")
        })?;
        count += 1;

        let market: TrackedMarket = market_resp
            .try_into()
            .context("failed to process market response from Gamma")?;

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
                if market.closed && v.status == MarketStatus::Tracking {
                    let mut updated_tm = v.clone();
                    updated_tm.update_from(&market);
                    updated_tm.status = MarketStatus::Closing {
                        grace_until: clock.now() + TimeDelta::hours(config.closed_grace_period_h),
                        cause: ClosingCause::ClosedOnGamma,
                    };
                    details_changed.insert(k.clone(), updated_tm);
                }

                // Gamma shows as open and details have changed
                if !market.closed && !(v == &market) {
                    let mut updated_tm = v.clone();
                    updated_tm.update_from(&market);
                    details_changed.insert(k.clone(), updated_tm);
                }
            }
        }
    }

    Ok(PlannedChanges {
        add,
        remove,
        details_changed,
    })
}
