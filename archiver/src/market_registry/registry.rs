use anyhow::{Context, Result};
use futures_util::StreamExt;
use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
    sync::Arc,
};

use chrono::{DateTime, TimeDelta, Utc};
use marcasite::{
    Paginated,
    gamma::Market,
    types::{ConditionId, TokenId},
};

use crate::{
    clock::SharedClock,
    config::Config,
    market_registry::{
        sanity_check::sanity_check,
        token_changes::TokenChanges,
        tracked_market::{
            MarketStatus::{self},
            TrackedMarket,
        },
    },
};

use super::tracked_market::ClosingCause;

#[derive(Debug)]
struct PlannedChanges {
    add: HashMap<ConditionId, TrackedMarket>,
    remove: HashSet<ConditionId>,
    details_changed: HashMap<ConditionId, TrackedMarket>,
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

impl Display for Registry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for market in self.markets.values() {
            writeln!(f, "{}", market.question)?;
        }
        Ok(())
    }
}

impl Registry {
    fn insert_or_update(
        &mut self,
        map: HashMap<ConditionId, TrackedMarket>,
        added: &mut HashSet<TokenId>,
        removed: &mut HashSet<TokenId>,
    ) {
        for (id, market) in map {
            added.insert(market.yes_token.clone());
            added.insert(market.no_token.clone());
            if let Some(old_market) = self.markets.insert(id, market) {
                removed.insert(old_market.yes_token);
                removed.insert(old_market.no_token);
            }
        }
    }

    /// Apply a set of `PlannedChanges` to a registry object
    fn apply_changes(&mut self, plan: PlannedChanges) -> TokenChanges {
        let mut added = HashSet::<TokenId>::new();
        let mut removed = HashSet::<TokenId>::new();

        for id in &plan.remove {
            if let Some(market) = self.markets.remove(id) {
                removed.insert(market.yes_token);
                removed.insert(market.no_token);
            }
        }

        self.insert_or_update(plan.add, &mut added, &mut removed);
        self.insert_or_update(plan.details_changed, &mut added, &mut removed);

        TokenChanges { added, removed }
    }

    /// Check the current set of tracked markets for markets where the grace period has expired.
    fn find_expired_markets_to_remove(&self, current: &DateTime<Utc>) -> HashSet<ConditionId> {
        self.markets
            .iter()
            .filter( |(_, v)| matches!(v.status, MarketStatus::Closing { grace_until, .. } if &grace_until <= current))
            .map(|(k, _)| k.clone())
            .collect()
    }

    /// Check if a market should be selected.
    ///
    /// The criteria checked is: Tradability, Category, and Liquidity.
    fn should_select(&self, market: &TrackedMarket, _config: Arc<Config>) -> bool {
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

    pub async fn get_changes(
        &mut self,
        mut results: Paginated<Market>,
        config: Arc<Config>,
        at: DateTime<Utc>,
    ) -> Result<TokenChanges> {
        let mut seen: HashSet<ConditionId> = HashSet::new();
        let mut add = HashMap::<ConditionId, TrackedMarket>::new();
        let mut details_changed = HashMap::<ConditionId, TrackedMarket>::new();
        let remove = self.find_expired_markets_to_remove(&at);

        let mut count = 0usize;
        let tags = config.category_tags.clone();
        while let Some(market_resp) = results.next().await {
            let market_resp = market_resp.with_context(|| {
                format!(
                    "listing markets (tags={tags:?}, closed=false) failed after {count} markets"
                )
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
            if self.recently_removed.contains_key(&market.condition_id)
                || remove.contains(&market.condition_id)
            {
                continue;
            }

            // Check if we already track the market
            let tm = self.markets.get_key_value(&market.condition_id);
            match tm {
                // Market is not tracked
                None => {
                    if self.should_select(&market, config.clone()) {
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
                            grace_until: at + TimeDelta::hours(config.closed_grace_period_h),
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

        sanity_check(count as u64, self.last_accepted_count)?;

        let plan = PlannedChanges {
            add,
            remove,
            details_changed,
        };

        self.last_success_at = Some(at);

        Ok(self.apply_changes(plan))
    }
}
