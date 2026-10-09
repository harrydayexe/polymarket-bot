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
    fn apply_changes(&mut self, plan: PlannedChanges, at: DateTime<Utc>) -> TokenChanges {
        let mut added = HashSet::<TokenId>::new();
        let mut removed = HashSet::<TokenId>::new();

        for id in plan.remove {
            if let Some(market) = self.markets.remove(&id) {
                removed.insert(market.yes_token);
                removed.insert(market.no_token);
                self.recently_removed.insert(id, at);
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

    pub(super) async fn get_changes(
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

        self.last_accepted_count = count as u64;
        self.last_success_at = Some(at);

        Ok(self.apply_changes(plan, at))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use chrono::TimeZone;
    use futures::stream;
    use serde_json::json;

    /// Gamma flags for a market. Defaults to a tradable market.
    struct Flags {
        active: bool,
        closed: bool,
        accepting_orders: bool,
    }

    const OPEN: Flags = Flags {
        active: true,
        closed: false,
        accepting_orders: true,
    };

    const CLOSED: Flags = Flags {
        active: true,
        closed: true,
        accepting_orders: false,
    };

    fn condition_id(n: u64) -> String {
        format!("0x{n:064x}")
    }

    fn yes_token(n: u64) -> TokenId {
        serde_json::from_value(json!(format!("{n}1"))).unwrap()
    }

    fn no_token(n: u64) -> TokenId {
        serde_json::from_value(json!(format!("{n}2"))).unwrap()
    }

    /// Builds a Gamma market numbered `n`, with Yes/No tokens `yes_token(n)`/`no_token(n)`.
    fn market(n: u64, question: &str, flags: Flags) -> Market {
        serde_json::from_value(json!({
            "conditionId": condition_id(n),
            "question": question,
            "active": flags.active,
            "closed": flags.closed,
            "acceptingOrders": flags.accepting_orders,
            "outcomes": "[\"Yes\", \"No\"]",
            "clobTokenIds": format!("[\"{}\", \"{}\"]", yes_token(n), no_token(n)),
        }))
        .unwrap()
    }

    fn open(n: u64) -> Market {
        market(n, &format!("Market {n}?"), OPEN)
    }

    fn page(markets: Vec<Market>) -> Paginated<Market> {
        Paginated::new(stream::iter(markets.into_iter().map(Ok)))
    }

    fn t0() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap()
    }

    fn config() -> Arc<Config> {
        Arc::new(Config::default())
    }

    fn grace() -> TimeDelta {
        TimeDelta::hours(config().closed_grace_period_h)
    }

    fn tokens(ids: &[TokenId]) -> HashSet<TokenId> {
        ids.iter().cloned().collect()
    }

    fn cid(n: u64) -> ConditionId {
        serde_json::from_value(json!(condition_id(n))).unwrap()
    }

    async fn fetch(
        registry: &mut Registry,
        markets: Vec<Market>,
        at: DateTime<Utc>,
    ) -> Result<TokenChanges> {
        registry.get_changes(page(markets), config(), at).await
    }

    #[tokio::test]
    async fn new_market_adds_both_tokens() {
        let mut registry = Registry::default();

        let changes = fetch(&mut registry, vec![open(1)], t0()).await.unwrap();

        assert_eq!(changes.added, tokens(&[yes_token(1), no_token(1)]));
        assert!(changes.removed.is_empty());
        assert_eq!(registry.markets[&cid(1)].status, MarketStatus::Tracking);
    }

    #[tokio::test]
    async fn no_yes_outcome_order_maps_tokens_by_outcome() {
        let mut registry = Registry::default();
        let mut reversed = open(1);
        reversed.outcomes = Some(vec!["No".into(), "Yes".into()]);

        fetch(&mut registry, vec![reversed], t0()).await.unwrap();

        let tracked = &registry.markets[&cid(1)];
        assert_eq!(tracked.yes_token, no_token(1));
        assert_eq!(tracked.no_token, yes_token(1));
    }

    #[tokio::test]
    async fn untradable_markets_are_not_added() {
        let mut registry = Registry::default();
        let inactive = Flags {
            active: false,
            ..OPEN
        };
        let not_accepting = Flags {
            accepting_orders: false,
            ..OPEN
        };

        let changes = fetch(
            &mut registry,
            vec![
                market(1, "Inactive?", inactive),
                market(2, "Closed?", CLOSED),
                market(3, "Not accepting?", not_accepting),
            ],
            t0(),
        )
        .await
        .unwrap();

        assert!(changes.added.is_empty());
        assert!(changes.removed.is_empty());
        assert!(registry.markets.is_empty());
    }

    #[tokio::test]
    async fn duplicate_markets_in_one_response_are_added_once() {
        let mut registry = Registry::default();

        let changes = fetch(
            &mut registry,
            vec![open(1), market(1, "Renamed?", OPEN)],
            t0(),
        )
        .await
        .unwrap();

        assert_eq!(changes.added, tokens(&[yes_token(1), no_token(1)]));
        assert_eq!(registry.markets.len(), 1);
        assert_eq!(registry.markets[&cid(1)].question, "Market 1?");
    }

    #[tokio::test]
    async fn unchanged_market_produces_no_changes() {
        let mut registry = Registry::default();
        fetch(&mut registry, vec![open(1)], t0()).await.unwrap();

        let changes = fetch(&mut registry, vec![open(1)], t0() + TimeDelta::minutes(1))
            .await
            .unwrap();

        assert!(changes.added.is_empty(), "added: {:?}", changes.added);
        assert!(changes.removed.is_empty(), "removed: {:?}", changes.removed);
    }

    #[tokio::test]
    async fn changed_details_update_tracked_market() {
        let mut registry = Registry::default();
        fetch(&mut registry, vec![open(1)], t0()).await.unwrap();

        fetch(
            &mut registry,
            vec![market(1, "Reworded?", OPEN)],
            t0() + TimeDelta::minutes(1),
        )
        .await
        .unwrap();

        assert_eq!(registry.markets[&cid(1)].question, "Reworded?");
    }

    #[tokio::test]
    async fn tracked_market_closed_on_gamma_enters_grace_period() {
        let mut registry = Registry::default();
        fetch(&mut registry, vec![open(1)], t0()).await.unwrap();

        let at = t0() + TimeDelta::minutes(1);
        let changes = fetch(&mut registry, vec![market(1, "Market 1?", CLOSED)], at)
            .await
            .unwrap();

        assert_eq!(
            registry.markets[&cid(1)].status,
            MarketStatus::Closing {
                grace_until: at + grace(),
                cause: ClosingCause::ClosedOnGamma,
            }
        );
        // Still subscribed during the grace period.
        assert!(
            changes.removed.is_subset(&changes.added),
            "removed {:?} without re-adding",
            changes.removed
        );
    }

    #[tokio::test]
    async fn closing_market_stays_closing_while_still_listed() {
        let mut registry = Registry::default();
        fetch(&mut registry, vec![open(1)], t0()).await.unwrap();
        let closed_at = t0() + TimeDelta::minutes(1);
        fetch(
            &mut registry,
            vec![market(1, "Market 1?", CLOSED)],
            closed_at,
        )
        .await
        .unwrap();
        let expected = registry.markets[&cid(1)].status.clone();

        fetch(
            &mut registry,
            vec![open(1)],
            closed_at + TimeDelta::minutes(1),
        )
        .await
        .unwrap();

        assert_eq!(registry.markets[&cid(1)].status, expected);
    }

    #[tokio::test]
    async fn closing_market_is_kept_until_grace_period_ends() {
        let mut registry = Registry::default();
        fetch(&mut registry, vec![open(1)], t0()).await.unwrap();
        fetch(&mut registry, vec![market(1, "Market 1?", CLOSED)], t0())
            .await
            .unwrap();

        let changes = fetch(
            &mut registry,
            vec![open(2)],
            t0() + grace() - TimeDelta::seconds(1),
        )
        .await
        .unwrap();

        assert!(registry.markets.contains_key(&cid(1)));
        assert!(!changes.removed.contains(&yes_token(1)));
    }

    #[tokio::test]
    async fn closing_market_is_removed_when_grace_period_ends() {
        let mut registry = Registry::default();
        fetch(&mut registry, vec![open(1)], t0()).await.unwrap();
        fetch(&mut registry, vec![market(1, "Market 1?", CLOSED)], t0())
            .await
            .unwrap();

        let changes = fetch(&mut registry, vec![open(2)], t0() + grace())
            .await
            .unwrap();

        assert!(!registry.markets.contains_key(&cid(1)));
        assert_eq!(changes.removed, tokens(&[yes_token(1), no_token(1)]));
        assert_eq!(changes.added, tokens(&[yes_token(2), no_token(2)]));
    }

    #[tokio::test]
    async fn expired_market_is_not_re_added_by_the_same_response() {
        let mut registry = Registry::default();
        fetch(&mut registry, vec![open(1)], t0()).await.unwrap();
        fetch(&mut registry, vec![market(1, "Market 1?", CLOSED)], t0())
            .await
            .unwrap();

        let changes = fetch(&mut registry, vec![open(1)], t0() + grace())
            .await
            .unwrap();

        assert!(!registry.markets.contains_key(&cid(1)));
        assert!(changes.added.is_empty());
    }

    #[tokio::test]
    async fn removed_market_is_not_re_added_by_a_later_response() {
        let mut registry = Registry::default();
        fetch(&mut registry, vec![open(1)], t0()).await.unwrap();
        fetch(&mut registry, vec![market(1, "Market 1?", CLOSED)], t0())
            .await
            .unwrap();
        fetch(&mut registry, vec![open(2)], t0() + grace())
            .await
            .unwrap();

        let changes = fetch(
            &mut registry,
            vec![open(1), open(2)],
            t0() + grace() + TimeDelta::minutes(1),
        )
        .await
        .unwrap();

        assert!(!registry.markets.contains_key(&cid(1)));
        assert!(!changes.added.contains(&yes_token(1)));
    }

    #[tokio::test]
    async fn empty_response_is_rejected_and_leaves_registry_unchanged() {
        let mut registry = Registry::default();
        fetch(&mut registry, vec![open(1)], t0()).await.unwrap();

        let result = fetch(&mut registry, vec![], t0() + TimeDelta::minutes(1)).await;

        assert!(result.is_err());
        assert!(registry.markets.contains_key(&cid(1)));
        assert_eq!(registry.last_success_at, Some(t0()));
    }

    #[tokio::test]
    async fn much_smaller_response_than_last_is_rejected() {
        let mut registry = Registry::default();
        fetch(&mut registry, (1..=10).map(open).collect(), t0())
            .await
            .unwrap();

        let result = fetch(&mut registry, vec![open(1)], t0() + TimeDelta::minutes(1)).await;

        assert!(result.is_err());
        assert_eq!(registry.markets.len(), 10);
    }

    #[tokio::test]
    async fn malformed_market_is_rejected_and_leaves_registry_unchanged() {
        let mut registry = Registry::default();
        let mut malformed = open(2);
        malformed.outcomes = None;

        let result = fetch(&mut registry, vec![open(1), malformed], t0()).await;

        assert!(result.is_err());
        assert!(registry.markets.is_empty());
        assert_eq!(registry.last_success_at, None);
    }

    #[tokio::test]
    async fn display_lists_tracked_questions() {
        let mut registry = Registry::default();
        fetch(&mut registry, vec![open(1)], t0()).await.unwrap();

        assert_eq!(registry.to_string(), "Market 1?\n");
    }
}
