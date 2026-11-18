//! The registry task: polls Gamma, plans with pm-core's [Registry] (universe selection, the
//! sanity check, grace periods), resolves each market's fee parameters (D13) and sends the
//! engine a `RegistryUpdate`.
//!
//! Fees, per market and poll: Gamma `feeSchedule` (`gamma`); if absent, CLOB `fee_details`
//! (`clob`); if that fails, the dated table for the market's tags (`fallback`, or
//! `fallback_conservative` for politics); if the tags match no table category, the `other` row
//! as `fallback_conservative`. A changed (rate, exponent) is a `changed` entry.

use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use async_trait::async_trait;
use futures_util::TryStreamExt;
use marcasite::{
    clob::ClobClient,
    gamma::{GammaClient, Market, TagId},
    types::ConditionId,
};
use pm_core::{
    clock::SharedClock,
    fees::FeeTable,
    invariant,
    invariant::InvariantViolation,
    market::{MarketInfo, Registry, TokenChanges, parse_markets, registry::MarketChange},
    units::{FeeParams, FeeSource, UtcMicros},
};
use rust_decimal::Decimal;
use tokio::{
    sync::mpsc,
    time::{self, MissedTickBehavior},
};
use tokio_util::sync::CancellationToken;

use crate::{
    action::RegistryHint,
    config::IngestConfig,
    event::{Event, LiveMarket, MarketChanged, RemovedMarket, Stamped},
};

/// Gamma, as the registry needs it.
#[async_trait]
pub trait MarketSource: Send + Sync + 'static {
    /// Open markets in the universe's categories above the liquidity floor.
    async fn open_markets(&self) -> Result<Vec<Market>>;
    /// Markets by condition id, open or closed.
    async fn markets(&self, ids: &[ConditionId]) -> Result<Vec<Market>>;
}

/// CLOB `/clob-markets/{condition_id}` fee details.
#[async_trait]
pub trait ClobFees: Send + Sync + 'static {
    /// `(rate, exponent)`, or `None` if the market has no usable fee details.
    async fn fee_details(&self, condition: &ConditionId) -> Result<Option<(Decimal, u32)>>;
}

const CONDITION_IDS_PER_REQUEST: usize = 50;

/// [MarketSource] over marcasite's Gamma client (on the `meta` budget).
pub struct GammaSource {
    client: GammaClient,
    tag_ids: Vec<TagId>,
    min_liquidity_usd: Decimal,
}

impl GammaSource {
    /// Resolves the category tag slugs to ids.
    ///
    /// # Errors
    ///
    /// If a tag cannot be looked up.
    pub async fn connect(client: GammaClient, config: &IngestConfig) -> Result<Self> {
        let mut tag_ids = Vec::with_capacity(config.category_tags.len());
        for tag in &config.category_tags {
            let id = client
                .get_tag_by_slug(tag)
                .send()
                .await
                .with_context(|| format!("failed to look up category tag {tag:?}"))?
                .id
                .with_context(|| format!("category tag {tag:?} has no id"))?;
            tag_ids.push(id);
        }
        Ok(Self {
            client,
            tag_ids,
            min_liquidity_usd: config.min_liquidity_usd,
        })
    }
}

#[async_trait]
impl MarketSource for GammaSource {
    async fn open_markets(&self) -> Result<Vec<Market>> {
        self.client
            .list_markets_keyset()
            .tag_ids(self.tag_ids.clone())
            .closed(false)
            .liquidity_num_min(self.min_liquidity_usd)
            .include_tag(true)
            .into_stream()
            .try_collect()
            .await
            .context("failed to list open markets")
    }

    async fn markets(&self, ids: &[ConditionId]) -> Result<Vec<Market>> {
        let mut markets = Vec::with_capacity(ids.len());
        for chunk in ids.chunks(CONDITION_IDS_PER_REQUEST) {
            for closed in [true, false] {
                let page: Vec<Market> = self
                    .client
                    .list_markets_keyset()
                    .condition_ids(chunk.to_vec())
                    .closed(closed)
                    .include_tag(true)
                    .into_stream()
                    .try_collect()
                    .await
                    .context("failed to look up markets by condition id")?;
                markets.extend(page);
            }
        }
        Ok(markets)
    }
}

#[async_trait]
impl ClobFees for ClobClient {
    async fn fee_details(&self, condition: &ConditionId) -> Result<Option<(Decimal, u32)>> {
        let details = self
            .get_clob_market_info(condition.clone())
            .await
            .with_context(|| format!("failed to get CLOB market {condition}"))?;
        let Some(fees) = details.fee_details else {
            return Ok(None);
        };
        let (Some(rate), Some(exponent)) = (fees.rate, fees.exponent) else {
            return Ok(None);
        };
        let exponent = marcasite_core::fees::exponent_from_decimal(exponent)
            .with_context(|| format!("CLOB fee exponent {exponent} of {condition}"))?;
        Ok((rate >= Decimal::ZERO).then_some((rate, exponent)))
    }
}

/// The registry's state between polls.
pub struct RegistryState {
    registry: Registry,
    table: FeeTable,
    /// The fee parameters last reported per tracked market.
    fees: HashMap<ConditionId, FeeParams>,
    interval: Duration,
    new_market_min_gap: Duration,
}

impl RegistryState {
    pub fn new(config: &IngestConfig, table: FeeTable) -> Self {
        Self {
            registry: Registry::new(Arc::new(config.registry_config())),
            table,
            fees: HashMap::new(),
            interval: config.registry_interval,
            new_market_min_gap: config.new_market_min_gap,
        }
    }

    pub fn tracked(&self) -> usize {
        self.registry.len()
    }

    /// The fee parameters for `info` at `now` (D13).
    async fn resolve_fees(
        &self,
        info: &MarketInfo,
        clob: &dyn ClobFees,
        now: UtcMicros,
    ) -> FeeParams {
        if let Some(fees) = info.fee_params {
            return fees;
        }
        match clob.fee_details(&info.condition_id).await {
            Ok(Some((rate, exponent))) => {
                return FeeParams {
                    rate,
                    exponent,
                    source: FeeSource::Clob,
                    seen_at: now,
                };
            }
            Ok(None) => tracing::debug!(condition = %info.condition_id, "no CLOB fee details"),
            Err(error) => {
                tracing::warn!(condition = %info.condition_id, %error, "CLOB fee lookup failed")
            }
        }
        if let Some(fees) = self.table.fallback(&info.tags, now) {
            return fees;
        }
        tracing::warn!(condition = %info.condition_id, tags = ?info.tags, "no fee category for the market's tags; using `other`");
        let (rate, exponent) = self
            .table
            .lookup("other", now)
            .map_or((Decimal::ZERO, 1), |row| (row.rate, row.exponent));
        FeeParams {
            rate,
            exponent,
            source: FeeSource::FallbackConservative,
            seen_at: now,
        }
    }

    /// One poll at `now`: expiry, listing, plan, fees. `None` if nothing changed.
    ///
    /// # Errors
    ///
    /// If an internal invariant fails. Fetch failures and rejected listings are logged and
    /// leave the universe as it was (expiries still apply).
    pub async fn poll(
        &mut self,
        source: &dyn MarketSource,
        clob: &dyn ClobFees,
        now: UtcMicros,
    ) -> Result<Option<Event>, InvariantViolation> {
        let expired = self.registry.expire(now);
        let mut remove = removed_markets(&expired);
        let mut add = Vec::new();
        let mut changed = Vec::new();
        match self.fetch(source, now).await {
            Ok((listing, followup)) => {
                let seen: Vec<MarketInfo> = listing.iter().chain(&followup).cloned().collect();
                match self.registry.check_listing(&listing) {
                    Err(rejected) => {
                        self.registry.note_rejected();
                        tracing::warn!(%rejected, "Gamma listing rejected; keeping the current universe");
                    }
                    Ok(accepted) => {
                        let plan = self.registry.plan_changes(listing, followup, accepted, now);
                        let applied = self.registry.apply_plan(plan, now);
                        remove.extend(removed_markets(&applied.tokens));
                        let details_changed: Vec<ConditionId> = applied
                            .records
                            .iter()
                            .filter(|(change, _)| *change == MarketChange::Updated)
                            .map(|(_, info)| info.condition_id.clone())
                            .collect();
                        let mut added: Vec<&ConditionId> = applied.tokens.added.keys().collect();
                        added.sort();
                        for id in added {
                            let Some(tracked) = self.registry.get(id) else {
                                continue;
                            };
                            let info = tracked.info.clone();
                            let fees = self.resolve_fees(&info, clob, now).await;
                            self.fees.insert(id.clone(), fees);
                            add.push(LiveMarket {
                                fees_enabled: info.fees_enabled,
                                info,
                                fees,
                            });
                        }
                        let just_added: Vec<ConditionId> =
                            add.iter().map(|m| m.info.condition_id.clone()).collect();
                        for info in seen {
                            let id = info.condition_id.clone();
                            if just_added.contains(&id) || self.registry.get(&id).is_none() {
                                continue;
                            }
                            let fees = self.resolve_fees(&info, clob, now).await;
                            let fee_changed =
                                self.fees.get(&id).is_none_or(|old| !old.same_curve(&fees));
                            let details = details_changed.contains(&id);
                            if !fee_changed && !details {
                                continue;
                            }
                            if fee_changed {
                                self.fees.insert(id.clone(), fees);
                            }
                            let Some(tracked) = self.registry.get(&id) else {
                                continue;
                            };
                            changed.push(MarketChanged {
                                market: LiveMarket {
                                    info: tracked.info.clone(),
                                    fees_enabled: info.fees_enabled,
                                    fees: self.fees[&id],
                                },
                                details_changed: details,
                                fee_changed,
                            });
                        }
                    }
                }
            }
            Err(error) => {
                tracing::warn!(error = %format!("{error:#}"), "registry poll failed; keeping the current universe")
            }
        }
        for market in &remove {
            if self.registry.get(&market.condition_id).is_none() {
                self.fees.remove(&market.condition_id);
            }
        }
        invariant!(
            self.fees.len() == self.registry.len(),
            Err(InvariantViolation(
                "every tracked market has fee parameters"
            ))
        );
        if add.is_empty() && remove.is_empty() && changed.is_empty() {
            return Ok(None);
        }
        tracing::info!(
            added = add.len(),
            removed = remove.len(),
            changed = changed.len(),
            tracked = self.registry.len(),
            "registry updated"
        );
        Ok(Some(Event::RegistryUpdate {
            add,
            remove,
            changed,
        }))
    }

    async fn fetch(
        &self,
        source: &dyn MarketSource,
        now: UtcMicros,
    ) -> Result<(Vec<MarketInfo>, Vec<MarketInfo>)> {
        let open = time::timeout(self.interval, source.open_markets())
            .await
            .context("listing timed out")??;
        let (listing, skipped) = parse_markets(open, now);
        if !skipped.is_empty() {
            tracing::debug!(skipped = skipped.len(), "malformed Gamma markets skipped");
        }
        let missing = self.registry.missing_from(&listing);
        let followup = if missing.is_empty() {
            Vec::new()
        } else {
            let markets = time::timeout(self.interval, source.markets(&missing))
                .await
                .context("follow-up lookup timed out")??;
            parse_markets(markets, now).0
        };
        Ok((listing, followup))
    }

    /// A market resolved on the websocket.
    pub fn resolved(&mut self, id: &ConditionId, at: UtcMicros) {
        self.registry.resolve_from_websocket(id, at);
    }
}

fn removed_markets(changes: &TokenChanges) -> Vec<RemovedMarket> {
    let mut removed: Vec<RemovedMarket> = changes
        .removed
        .iter()
        .map(|(id, tokens)| RemovedMarket {
            condition_id: id.clone(),
            tokens: tokens.clone(),
        })
        .collect();
    removed.sort_by(|a, b| a.condition_id.cmp(&b.condition_id));
    removed
}

/// Polls every `registry_interval` (and at once on a `new_market` hint) until cancelled.
///
/// # Errors
///
/// If an invariant fails or the engine is gone.
pub async fn registry_task(
    mut state: RegistryState,
    source: Arc<dyn MarketSource>,
    clob: Arc<dyn ClobFees>,
    clock: SharedClock,
    out: mpsc::Sender<Stamped>,
    mut hints: mpsc::UnboundedReceiver<RegistryHint>,
    token: CancellationToken,
) -> Result<()> {
    let mut tick = time::interval(state.interval);
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    // `new_market` arrives for every market created anywhere on Polymarket (several a second,
    // with no tags), so hints only bring the next poll forward, to at most one per
    // `min_gap`.
    let min_gap = state.new_market_min_gap;
    let mut last_poll: Option<time::Instant> = None;
    let mut next_poll = time::Instant::now();
    loop {
        tokio::select! {
            biased;
            () = token.cancelled() => return Ok(()),
            hint = hints.recv() => match hint {
                Some(RegistryHint::NewMarket) => {
                    let due = last_poll.map_or_else(time::Instant::now, |at| (at + min_gap).max(time::Instant::now()));
                    if due < next_poll {
                        tick.reset_at(due);
                        next_poll = due;
                    }
                }
                Some(RegistryHint::Resolved { condition_id, at }) => state.resolved(&condition_id, at),
                None => return Ok(()),
            },
            _ = tick.tick() => {
                last_poll = Some(time::Instant::now());
                next_poll = time::Instant::now() + state.interval;
                let now = clock.now();
                let polled = tokio::select! {
                    () = token.cancelled() => return Ok(()),
                    polled = state.poll(source.as_ref(), clob.as_ref(), now) => polled?,
                };
                if let Some(event) = polled {
                    out.send(Stamped::at(clock.now(), event)).await.context("the engine has stopped")?;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests;
