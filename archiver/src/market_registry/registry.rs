use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use chrono::{DateTime, TimeDelta, Utc};
use marcasite::types::ConditionId;

use crate::{
    config::Config,
    market_registry::{
        market_info::MarketInfo,
        not_enough_markets_error::NotEnoughMarketsError,
        sanity_check::sanity_check,
        selection::select,
        token_changes::TokenChanges,
        tracked_market::{ClosingCause, MarketStatus, TrackedMarket},
    },
};

/// How many registry intervals a removed market is kept out of the tracked set, so a lagging
/// Gamma listing cannot add it straight back.
const RECENTLY_REMOVED_INTERVALS: u32 = 3;

/// Changes to the tracked set worked out from one Gamma listing.
#[derive(Debug, Default)]
pub struct PlannedChanges {
    /// Markets to start tracking.
    pub add: Vec<TrackedMarket>,
    /// Tracked markets with a newer state from Gamma.
    pub update: Vec<MarketUpdate>,
    /// Qualifying markets in the listing, for the next sanity check.
    accepted_count: u64,
}

#[derive(Debug)]
pub struct MarketUpdate {
    pub market: TrackedMarket,
    /// Whether the fields recorded in the archive changed.
    pub details_changed: bool,
}

/// Whether a market record is for a newly tracked market or for changed details.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketChange {
    Added,
    Updated,
}

/// The result of applying [PlannedChanges]: what to write and what to tell the connection manager.
#[derive(Debug, Default)]
pub struct AppliedChanges {
    pub records: Vec<(MarketChange, MarketInfo)>,
    pub tokens: TokenChanges,
}

#[derive(Debug)]
pub struct Registry {
    config: Arc<Config>,
    /// Markets currently being tracked (either open or in grace period).
    markets: HashMap<ConditionId, TrackedMarket>,
    /// Markets removed recently, and when, so they are not re-added straight away.
    recently_removed: HashMap<ConditionId, DateTime<Utc>>,
    /// `market_resolved` notices for markets that were not tracked when they arrived.
    pending_resolved: HashMap<ConditionId, DateTime<Utc>>,
    last_accepted_count: u64,
    last_success_at: Option<DateTime<Utc>>,
}

impl Registry {
    pub fn new(config: Arc<Config>) -> Self {
        Self {
            config,
            markets: HashMap::new(),
            recently_removed: HashMap::new(),
            pending_resolved: HashMap::new(),
            last_accepted_count: 0,
            last_success_at: None,
        }
    }

    /// Start the grace period for a market resolved on the websocket at `at`.
    ///
    /// A notice for an untracked market is kept, so that if Gamma still lists the market as open
    /// it joins already in its grace period.
    pub fn resolve_from_websocket(&mut self, id: &ConditionId, at: DateTime<Utc>) {
        match self.markets.get_mut(id) {
            Some(market) if market.status == MarketStatus::Tracking => {
                market.status = MarketStatus::Closing {
                    grace_until: add_saturating(at, self.config.closed_grace_period),
                    cause: ClosingCause::ResolvedOnWebsocket,
                };
            }
            Some(_) => {
                tracing::debug!(%id, "resolved market is already in its grace period");
            }
            None => {
                tracing::debug!(%id, "resolved market is not tracked, remembering notice");
                self.pending_resolved.entry(id.clone()).or_insert(at);
            }
        }
    }

    /// Remove markets whose grace period has passed, and forget old bookkeeping. Needs nothing from
    /// Gamma, so it runs every cycle even when Gamma is unavailable.
    pub fn expire(&mut self, now: DateTime<Utc>) -> TokenChanges {
        let expired: Vec<ConditionId> = self
            .markets
            .iter()
            .filter(|(_, market)| {
                matches!(market.status, MarketStatus::Closing { grace_until, .. } if grace_until <= now)
            })
            .map(|(id, _)| id.clone())
            .collect();

        let mut changes = TokenChanges::default();
        for id in expired {
            if let Some(market) = self.markets.remove(&id) {
                tracing::info!(%id, question = market.info.question, "grace period over, removing market");
                changes.removed.insert(id.clone(), market.info.tokens);
                self.recently_removed.insert(id, now);
            }
        }

        let removed_ttl = self.config.registry_interval * RECENTLY_REMOVED_INTERVALS;
        self.recently_removed
            .retain(|_, removed_at| add_saturating(*removed_at, removed_ttl) > now);
        let grace = self.config.closed_grace_period;
        self.pending_resolved
            .retain(|_, resolved_at| add_saturating(*resolved_at, grace) > now);

        changes
    }

    /// Count the qualifying markets in a Gamma listing and check it is believable.
    ///
    /// # Returns
    ///
    /// The number of qualifying markets, to pass to [Registry::plan_changes].
    pub fn check_listing(&self, listing: &[MarketInfo]) -> Result<u64, NotEnoughMarketsError> {
        let qualifying = listing
            .iter()
            .filter(|market| select(market, &self.config))
            .map(|market| &market.condition_id)
            .collect::<HashSet<_>>()
            .len() as u64;
        sanity_check(qualifying, self.last_accepted_count)?;
        Ok(qualifying)
    }

    /// Tracked markets absent from `listing`. They may have closed or dropped below the liquidity
    /// floor, so Gamma has to be asked about them directly.
    pub fn missing_from(&self, listing: &[MarketInfo]) -> Vec<ConditionId> {
        let listed: HashSet<&ConditionId> = listing.iter().map(|m| &m.condition_id).collect();
        self.markets
            .keys()
            .filter(|id| !listed.contains(id))
            .cloned()
            .collect()
    }

    /// Work out changes from the open-market `listing` and the `followup` lookup of tracked markets
    /// missing from it. Tracked markets stay tracked until they close, whatever their liquidity.
    pub fn plan_changes(
        &self,
        listing: Vec<MarketInfo>,
        followup: Vec<MarketInfo>,
        accepted_count: u64,
        now: DateTime<Utc>,
    ) -> PlannedChanges {
        let mut plan = PlannedChanges {
            accepted_count,
            ..PlannedChanges::default()
        };
        let mut seen = HashSet::new();

        for info in listing.into_iter().chain(followup) {
            if !seen.insert(info.condition_id.clone()) {
                continue;
            }

            match self.markets.get(&info.condition_id) {
                Some(tracked) => plan.update.extend(self.plan_update(tracked, info, now)),
                None => plan.add.extend(self.plan_add(info, now)),
            }
        }

        let missing = self.markets.keys().filter(|id| !seen.contains(*id)).count();
        if missing > 0 {
            tracing::warn!(missing, "tracked markets not found on Gamma, keeping them");
        }

        plan
    }

    fn plan_add(&self, info: MarketInfo, now: DateTime<Utc>) -> Option<TrackedMarket> {
        if self.recently_removed.contains_key(&info.condition_id) || !select(&info, &self.config) {
            return None;
        }

        let status = match self.pending_resolved.get(&info.condition_id) {
            None => MarketStatus::Tracking,
            Some(&resolved_at) => {
                let grace_until = add_saturating(resolved_at, self.config.closed_grace_period);
                if grace_until <= now {
                    return None;
                }
                MarketStatus::Closing {
                    grace_until,
                    cause: ClosingCause::ResolvedOnWebsocket,
                }
            }
        };

        Some(TrackedMarket {
            info,
            added_at: now,
            status,
        })
    }

    fn plan_update(
        &self,
        tracked: &TrackedMarket,
        info: MarketInfo,
        now: DateTime<Utc>,
    ) -> Option<MarketUpdate> {
        let details_changed = !tracked.info.details_match(&info);

        let status = match tracked.status {
            MarketStatus::Tracking if info.closed => MarketStatus::Closing {
                grace_until: add_saturating(now, self.config.closed_grace_period),
                cause: ClosingCause::ClosedOnGamma,
            },
            _ => tracked.status.clone(),
        };

        if !details_changed && status == tracked.status {
            return None;
        }

        Some(MarketUpdate {
            market: TrackedMarket {
                info,
                added_at: tracked.added_at,
                status,
            },
            details_changed,
        })
    }

    /// Apply a plan made by [Registry::plan_changes].
    pub fn apply_plan(&mut self, plan: PlannedChanges, now: DateTime<Utc>) -> AppliedChanges {
        self.last_accepted_count = plan.accepted_count;
        self.last_success_at = Some(now);

        let mut applied = AppliedChanges::default();

        for market in plan.add {
            let id = market.info.condition_id.clone();
            self.pending_resolved.remove(&id);
            applied
                .tokens
                .added
                .insert(id.clone(), market.info.tokens.clone());
            applied
                .records
                .push((MarketChange::Added, market.info.clone()));
            self.markets.insert(id, market);
        }

        for MarketUpdate {
            market,
            details_changed,
        } in plan.update
        {
            let id = market.info.condition_id.clone();
            if details_changed {
                applied
                    .records
                    .push((MarketChange::Updated, market.info.clone()));
            }
            if let Some(old) = self.markets.insert(id.clone(), market)
                && old.info.tokens != self.markets[&id].info.tokens
            {
                applied.tokens.removed.insert(id.clone(), old.info.tokens);
                applied
                    .tokens
                    .added
                    .insert(id.clone(), self.markets[&id].info.tokens.clone());
            }
        }

        applied
    }
}

/// `at + duration`, saturating at the latest representable time instead of panicking.
fn add_saturating(at: DateTime<Utc>, duration: Duration) -> DateTime<Utc> {
    TimeDelta::from_std(duration)
        .ok()
        .and_then(|delta| at.checked_add_signed(delta))
        .unwrap_or(DateTime::<Utc>::MAX_UTC)
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use serde_json::{Value, json};

    use super::*;
    use crate::market_registry::{market_info::tests::gamma_market, token_changes::MarketTokens};

    fn t0() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap()
    }

    fn hours(h: i64) -> TimeDelta {
        TimeDelta::hours(h)
    }

    fn market(n: u8, overrides: Value) -> MarketInfo {
        MarketInfo::from_gamma(gamma_market(n, overrides), t0()).unwrap()
    }

    fn id(n: u8) -> ConditionId {
        market(n, json!({})).condition_id
    }

    fn registry() -> Registry {
        Registry::new(Arc::new(Config::default()))
    }

    /// Run one Gamma cycle the way the registry task does, without the network.
    fn cycle(
        registry: &mut Registry,
        listing: Vec<MarketInfo>,
        followup: Vec<MarketInfo>,
        now: DateTime<Utc>,
    ) -> AppliedChanges {
        let accepted = registry.check_listing(&listing).unwrap();
        let plan = registry.plan_changes(listing, followup, accepted, now);
        registry.apply_plan(plan, now)
    }

    fn ids(map: &HashMap<ConditionId, MarketTokens>) -> HashSet<ConditionId> {
        map.keys().cloned().collect()
    }

    #[test]
    fn new_market_adds_both_tokens_and_a_record() {
        let mut registry = registry();
        let m1 = market(1, json!({}));

        let applied = cycle(&mut registry, vec![m1.clone()], vec![], t0());

        assert_eq!(applied.tokens.added.get(&m1.condition_id), Some(&m1.tokens));
        assert!(applied.tokens.removed.is_empty());
        assert_eq!(applied.records, vec![(MarketChange::Added, m1)]);
    }

    #[test]
    fn no_change_gives_empty_lists() {
        let mut registry = registry();
        cycle(&mut registry, vec![market(1, json!({}))], vec![], t0());

        let applied = cycle(
            &mut registry,
            vec![market(1, json!({}))],
            vec![],
            t0() + hours(1),
        );

        assert!(applied.tokens.is_empty());
        assert!(applied.records.is_empty());
    }

    #[test]
    fn below_floor_new_market_is_not_added() {
        let mut registry = registry();
        let listing = vec![
            market(1, json!({})),
            market(2, json!({ "liquidityNum": 200 })),
        ];

        let applied = cycle(&mut registry, listing, vec![], t0());

        assert_eq!(ids(&applied.tokens.added), HashSet::from([id(1)]));
    }

    #[test]
    fn below_floor_tracked_market_is_kept() {
        let mut registry = registry();
        cycle(
            &mut registry,
            vec![market(1, json!({})), market(2, json!({}))],
            vec![],
            t0(),
        );

        // Market 1 dropped below the floor, so it is missing from the listing.
        let listing = vec![market(2, json!({}))];
        assert_eq!(registry.missing_from(&listing), vec![id(1)]);
        let followup = vec![market(1, json!({ "liquidityNum": 200 }))];
        let applied = cycle(&mut registry, listing, followup, t0() + hours(1));

        assert!(applied.tokens.is_empty());
        assert_eq!(registry.markets[&id(1)].status, MarketStatus::Tracking);
    }

    #[test]
    fn tracked_market_missing_everywhere_is_kept() {
        let mut registry = registry();
        cycle(
            &mut registry,
            vec![market(1, json!({})), market(2, json!({}))],
            vec![],
            t0(),
        );

        let applied = cycle(
            &mut registry,
            vec![market(2, json!({}))],
            vec![],
            t0() + hours(1),
        );

        assert!(applied.tokens.is_empty());
        assert!(registry.markets.contains_key(&id(1)));
    }

    #[test]
    fn details_change_writes_a_record_without_resubscribing() {
        let mut registry = registry();
        cycle(&mut registry, vec![market(1, json!({}))], vec![], t0());

        let edited = market(1, json!({ "question": "Edited?" }));
        let applied = cycle(&mut registry, vec![edited.clone()], vec![], t0() + hours(1));

        assert!(applied.tokens.is_empty());
        assert_eq!(applied.records, vec![(MarketChange::Updated, edited)]);
    }

    #[test]
    fn liquidity_change_is_not_a_details_change() {
        let mut registry = registry();
        cycle(&mut registry, vec![market(1, json!({}))], vec![], t0());

        let applied = cycle(
            &mut registry,
            vec![market(1, json!({ "liquidityNum": 9000 }))],
            vec![],
            t0() + hours(1),
        );

        assert!(applied.records.is_empty());
    }

    #[test]
    fn changed_token_ids_resubscribe_the_market() {
        let mut registry = registry();
        let old = market(1, json!({}));
        cycle(&mut registry, vec![old.clone()], vec![], t0());

        let new = market(1, json!({ "clobTokenIds": "[\"9\", \"8\"]" }));
        let applied = cycle(&mut registry, vec![new.clone()], vec![], t0() + hours(1));

        assert_eq!(applied.tokens.removed.get(&id(1)), Some(&old.tokens));
        assert_eq!(applied.tokens.added.get(&id(1)), Some(&new.tokens));
        assert_eq!(applied.records, vec![(MarketChange::Updated, new)]);
    }

    #[test]
    fn closed_on_gamma_is_kept_through_the_grace_period() {
        let mut registry = registry();
        cycle(
            &mut registry,
            vec![market(1, json!({})), market(2, json!({}))],
            vec![],
            t0(),
        );

        // Closed markets drop out of the listing, so the follow-up lookup finds the closure.
        let followup = vec![market(1, json!({ "closed": true }))];
        cycle(&mut registry, vec![market(2, json!({}))], followup, t0());
        assert!(matches!(
            registry.markets[&id(1)].status,
            MarketStatus::Closing {
                cause: ClosingCause::ClosedOnGamma,
                ..
            }
        ));

        assert!(registry.expire(t0() + hours(2)).is_empty());
        let expired = registry.expire(t0() + hours(7));
        assert_eq!(ids(&expired.removed), HashSet::from([id(1)]));
        assert!(!registry.markets.contains_key(&id(1)));
    }

    #[test]
    fn resolved_via_websocket_is_removed_after_grace_and_not_re_added() {
        let mut registry = registry();
        cycle(
            &mut registry,
            vec![market(1, json!({})), market(2, json!({}))],
            vec![],
            t0(),
        );
        registry.resolve_from_websocket(&id(1), t0());

        let expired = registry.expire(t0() + hours(7));
        assert_eq!(ids(&expired.removed), HashSet::from([id(1)]));

        // Gamma still lists the market as open.
        let listing = vec![market(1, json!({})), market(2, json!({}))];
        let applied = cycle(&mut registry, listing, vec![], t0() + hours(7));
        assert!(applied.tokens.is_empty());
    }

    #[test]
    fn removed_market_can_return_after_the_block_expires() {
        let mut registry = registry();
        cycle(
            &mut registry,
            vec![market(1, json!({})), market(2, json!({}))],
            vec![],
            t0(),
        );
        registry.resolve_from_websocket(&id(1), t0());
        registry.expire(t0() + hours(7));

        let later = t0() + hours(8);
        registry.expire(later);
        let listing = vec![market(1, json!({})), market(2, json!({}))];
        let applied = cycle(&mut registry, listing, vec![], later);

        assert_eq!(ids(&applied.tokens.added), HashSet::from([id(1)]));
    }

    #[test]
    fn resolved_before_tracking_joins_in_grace_period() {
        let mut registry = registry();
        registry.resolve_from_websocket(&id(1), t0());

        cycle(
            &mut registry,
            vec![market(1, json!({}))],
            vec![],
            t0() + hours(1),
        );

        assert_eq!(
            registry.markets[&id(1)].status,
            MarketStatus::Closing {
                grace_until: t0() + hours(6),
                cause: ClosingCause::ResolvedOnWebsocket,
            }
        );
    }

    #[test]
    fn resolved_long_before_tracking_is_not_added() {
        let mut registry = registry();
        registry.resolve_from_websocket(&id(1), t0());
        let listing = vec![market(1, json!({})), market(2, json!({}))];

        let applied = cycle(&mut registry, listing, vec![], t0() + hours(7));

        assert_eq!(ids(&applied.tokens.added), HashSet::from([id(2)]));
    }

    #[test]
    fn empty_listing_is_rejected() {
        let mut registry = registry();
        cycle(&mut registry, vec![market(1, json!({}))], vec![], t0());

        assert!(registry.check_listing(&[]).is_err());
    }

    #[test]
    fn sanity_check_counts_only_qualifying_markets() {
        let mut registry = registry();
        let qualifying: Vec<_> = (1..=4).map(|n| market(n, json!({}))).collect();
        cycle(&mut registry, qualifying, vec![], t0());

        let mostly_below_floor: Vec<_> = (1..=4)
            .map(|n| market(n, json!({ "liquidityNum": if n == 1 { 5000 } else { 1 } })))
            .collect();
        assert!(registry.check_listing(&mostly_below_floor).is_err());
    }

    #[test]
    fn duplicate_markets_are_counted_once() {
        let registry = registry();
        let listing = vec![market(1, json!({})), market(1, json!({}))];

        assert_eq!(registry.check_listing(&listing).unwrap(), 1);
        let plan = registry.plan_changes(listing, vec![], 1, t0());
        assert_eq!(plan.add.len(), 1);
    }
}
