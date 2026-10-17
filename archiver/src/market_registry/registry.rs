use anyhow::{Context, Result};
use futures_util::StreamExt;
use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
    sync::Arc,
};

use chrono::{DateTime, Utc};
use marcasite::{
    Paginated,
    gamma::Market,
    types::{ConditionId, TokenId},
};

use crate::{
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
pub struct PlannedChanges {
    pub add: HashMap<ConditionId, TrackedMarket>,
    pub remove: HashSet<ConditionId>,
    pub details_changed: HashMap<ConditionId, TrackedMarket>,
    markets_received: usize,
}
impl Display for PlannedChanges {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for market in self.add.values() {
            writeln!(f, "{}", market.question)?;
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct Registry {
    config: Arc<Config>,
    /// Markets currently being tracked (either open or in grace period).
    markets: HashMap<ConditionId, TrackedMarket>,
    /// Markets that have recently been removed.
    recently_removed: HashMap<ConditionId, DateTime<Utc>>,
    last_accepted_count: u64,
    last_success_at: Option<DateTime<Utc>>,
}

impl Registry {
    pub fn new(config: Arc<Config>) -> Self {
        Self {
            config,
            markets: HashMap::new(),
            recently_removed: HashMap::new(),
            last_accepted_count: 0,
            last_success_at: None,
        }
    }
}

impl Registry {
    /// Enter a market as closed via websocket, and enter the grace period.
    ///
    /// # Returns
    ///
    /// A bool indicating if the market was currently in the tracked set.
    pub fn resolve_from_websocket(&mut self, id: &ConditionId, at: DateTime<Utc>) -> bool {
        match self.markets.get_mut(id) {
            Some(market) if market.status == MarketStatus::Tracking => {
                market.status = MarketStatus::Closing {
                    grace_until: at + self.config.closed_grace_period,
                    cause: ClosingCause::ClosedOnWebSocket,
                };
                true
            }
            Some(_) => {
                tracing::info!("Market marked as closed via websocket is already in grace period");
                true
            }
            None => {
                tracing::warn!(
                    "Market removed via websocket which was not currently being tracked."
                );
                false
            }
        }
    }

    /// Insert or replace markets, recording only the tokens that actually change.
    fn insert_or_update(
        &mut self,
        map: &HashMap<ConditionId, TrackedMarket>,
        added: &mut HashSet<TokenId>,
        removed: &mut HashSet<TokenId>,
    ) {
        for (id, market) in map {
            let new_tokens = [market.yes_token.clone(), market.no_token.clone()];
            let old_tokens = self
                .markets
                .insert(id.clone(), market.clone())
                .map(|old| vec![old.yes_token, old.no_token])
                .unwrap_or_default();

            for token in &new_tokens {
                if !old_tokens.contains(token) {
                    added.insert(token.clone());
                }
            }
            for token in old_tokens {
                if !new_tokens.contains(&token) {
                    removed.insert(token);
                }
            }
        }
    }

    /// Apply a set of `PlannedChanges` to a registry object
    pub(super) fn apply_plan(&mut self, plan: &PlannedChanges, at: DateTime<Utc>) -> TokenChanges {
        self.last_accepted_count = plan.markets_received as u64;
        self.last_success_at = Some(at);

        let mut added = HashSet::<TokenId>::new();
        let mut removed = HashSet::<TokenId>::new();

        for id in &plan.remove {
            if let Some(market) = self.markets.remove(id) {
                removed.insert(market.yes_token);
                removed.insert(market.no_token);
                self.recently_removed.insert(id.clone(), at);
            }
        }

        self.insert_or_update(&plan.add, &mut added, &mut removed);
        self.insert_or_update(&plan.details_changed, &mut added, &mut removed);

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

    /// Check if a market is tradable. Category and liquidity are filtered by the Gamma query.
    fn should_select(&self, market: &TrackedMarket) -> bool {
        market.active && !market.closed && market.accepting_orders
    }

    pub(super) async fn plan_changes(
        &self,
        mut results: Paginated<Market>,
        at: DateTime<Utc>,
    ) -> Result<PlannedChanges> {
        let mut seen: HashSet<ConditionId> = HashSet::new();
        let mut add = HashMap::<ConditionId, TrackedMarket>::new();
        let mut details_changed = HashMap::<ConditionId, TrackedMarket>::new();
        let remove = self.find_expired_markets_to_remove(&at);

        let mut count = 0usize;
        let tags = self.config.category_tags.clone();
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
                    if self.should_select(&market) {
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
                            grace_until: at + self.config.closed_grace_period,
                            cause: ClosingCause::ClosedOnGamma,
                        };
                        details_changed.insert(k.clone(), updated_tm);
                    }

                    // Gamma shows as open and details have changed
                    if !market.closed && v != &market {
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
            markets_received: count,
        };

        Ok(plan)
    }
}
