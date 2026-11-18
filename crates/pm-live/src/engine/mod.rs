//! The engine: one task owns all decision state and consumes envelopes in order through a
//! pure [Engine::step] (no I/O, no clock reads; time is the envelope's `available_at`).
//!
//! C2 state: the universe as the registry task reported it, token placement on connection
//! slots, per-token books with trust (§3.4), per-condition rolling volume (§3.6), the
//! `price_change`/`last_trade_price` ordering tracker, timers, and counters written as metrics.
//!
//! Determinism: anything that produces actions iterates ordered maps or sorted vectors, so a
//! replay of the same envelopes yields the same actions.

pub mod books;
pub mod ordering;
pub mod volume;

use std::collections::{BTreeMap, BTreeSet, HashMap};

use chrono::TimeDelta;
use marcasite::{
    types::{ConditionId, TokenId},
    ws::{BookEvent, LastTradePriceEvent, MarketResolvedEvent, PriceChangeEvent},
};
use pm_core::{
    archive::record::ConnSlotId,
    book::{LevelChange, OrderBook},
    invariant,
    invariant::InvariantViolation,
    market::{MarketTokens, TokenAssignment},
    normalize::{NormalisedPrint, RawPrint},
    units::{Usd, UtcMicros},
};

use crate::{
    action::{Action, Metric, Record, RegistryHint},
    config::EngineConfig,
    event::{BackfillPrint, Envelope, Event, LiveMarket, MarketChanged, RemovedMarket, Stamped},
};
use books::{Distrust, RingLimits, TokenBook};
use ordering::{Observation, OrderingTracker};
use volume::ConditionVolume;

/// A connection slot as the engine sees it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SlotState {
    pub tokens: BTreeSet<TokenId>,
    /// Since when the connection has been up, if it is.
    pub connected_since: Option<UtcMicros>,
    /// When the last market message arrived on it.
    pub last_message_at: Option<UtcMicros>,
    /// A decode error's resnapshot is due; errors until then are coalesced into it.
    pub decode_errors: Option<DecodeErrors>,
}

/// Decode errors on one slot awaiting their resnapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecodeErrors {
    /// The latest error.
    pub last: UtcMicros,
    /// The first error after the one that scheduled the resnapshot, if any: these errors'
    /// backfill is requested with the resnapshot.
    pub coalesced_from: Option<UtcMicros>,
}

impl SlotState {
    pub fn connected(&self) -> bool {
        self.connected_since.is_some()
    }
}

/// Something due at a time.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Timer {
    /// Resubscribe tokens of `slot` still untrusted after its decode errors
    /// ([SlotState::decode_errors]), and backfill the coalesced ones.
    Resnapshot { slot: ConnSlotId },
}

/// Counters, written as metrics every `metrics_interval` and reset.
#[derive(Debug, Clone, Default)]
struct Counters {
    counts: BTreeMap<(&'static str, &'static str), u64>,
}

impl Counters {
    fn add(&mut self, name: &'static str, label: &'static str, n: u64) {
        *self.counts.entry((name, label)).or_default() += n;
    }

    fn drain(&mut self, at: UtcMicros) -> Vec<Metric> {
        std::mem::take(&mut self.counts)
            .into_iter()
            .map(|((name, label), n)| {
                let metric = Metric::new(name, at, n as f64);
                if label.is_empty() {
                    metric
                } else {
                    metric.label("kind", label)
                }
            })
            .collect()
    }
}

/// The engine's state.
#[derive(Debug)]
pub struct Engine {
    config: EngineConfig,
    limits: RingLimits,
    /// The last sequence number consumed.
    seq: u64,
    /// The latest `available_at` seen; time never goes back.
    now: Option<UtcMicros>,
    markets: BTreeMap<ConditionId, LiveMarket>,
    /// Tracked markets whose resolution has been recorded.
    resolved: BTreeSet<ConditionId>,
    token_market: HashMap<TokenId, ConditionId>,
    assignment: TokenAssignment,
    slots: BTreeMap<ConnSlotId, SlotState>,
    token_slot: HashMap<TokenId, ConnSlotId>,
    books: BTreeMap<TokenId, TokenBook>,
    volumes: BTreeMap<ConditionId, ConditionVolume>,
    ordering: OrderingTracker,
    timers: BTreeMap<(UtcMicros, u64), Timer>,
    next_timer: u64,
    counters: Counters,
    next_metrics_at: Option<UtcMicros>,
    next_scan_at: Option<UtcMicros>,
}

fn delta(duration: std::time::Duration) -> TimeDelta {
    TimeDelta::from_std(duration).unwrap_or(TimeDelta::MAX)
}

fn sorted(tokens: impl IntoIterator<Item = TokenId>) -> Vec<TokenId> {
    let mut tokens: Vec<TokenId> = tokens.into_iter().collect();
    tokens.sort();
    tokens.dedup();
    tokens
}

/// Conditions to backfill, grouped by interval.
type Backfills = BTreeMap<(UtcMicros, UtcMicros), BTreeSet<ConditionId>>;

fn push_backfills(backfills: Backfills, actions: &mut Vec<Action>) {
    for ((from, until), conditions) in backfills {
        actions.push(Action::RequestBackfill {
            conditions: conditions.into_iter().collect(),
            from,
            until,
        });
    }
}

impl Engine {
    pub fn new(config: EngineConfig) -> Self {
        let limits = RingLimits {
            window: config.book_ring_window,
            capacity: config.book_ring_capacity,
        };
        Self {
            assignment: TokenAssignment::new(config.tokens_per_connection),
            config,
            limits,
            seq: 0,
            now: None,
            markets: BTreeMap::new(),
            resolved: BTreeSet::new(),
            token_market: HashMap::new(),
            slots: BTreeMap::new(),
            token_slot: HashMap::new(),
            books: BTreeMap::new(),
            volumes: BTreeMap::new(),
            ordering: OrderingTracker::default(),
            timers: BTreeMap::new(),
            next_timer: 0,
            counters: Counters::default(),
            next_metrics_at: None,
            next_scan_at: None,
        }
    }

    /// Gives a received event the next sequence number.
    pub fn accept(&self, stamped: Stamped) -> Envelope {
        stamped.into_envelope(self.seq + 1)
    }

    /// The last sequence number consumed.
    pub fn seq(&self) -> u64 {
        self.seq
    }

    pub fn market(&self, condition: &ConditionId) -> Option<&LiveMarket> {
        self.markets.get(condition)
    }

    pub fn markets(&self) -> impl Iterator<Item = &LiveMarket> {
        self.markets.values()
    }

    pub fn book(&self, token: &TokenId) -> Option<&TokenBook> {
        self.books.get(token)
    }

    pub fn slots(&self) -> &BTreeMap<ConnSlotId, SlotState> {
        &self.slots
    }

    /// The trailing volume of a condition at `at` and whether its window is complete.
    pub fn volume(
        &self,
        condition: &ConditionId,
        at: UtcMicros,
    ) -> Option<Result<(Usd, bool), InvariantViolation>> {
        self.volumes.get(condition).map(|v| v.total(at))
    }

    /// Consumes one envelope: fires timers due by its `available_at`, then handles its event.
    ///
    /// # Errors
    ///
    /// If an internal invariant fails (out-of-order sequence, inconsistent books); the run
    /// should stop.
    pub fn step(&mut self, envelope: Envelope) -> Result<Vec<Action>, InvariantViolation> {
        invariant!(
            envelope.seq == self.seq + 1,
            Err(InvariantViolation("envelopes are consumed in sequence"))
        );
        self.seq = envelope.seq;
        // Receive times can tie or, across tasks, step back slightly; the engine's clock
        // never does.
        let now = self
            .now
            .map_or(envelope.available_at, |n| n.max(envelope.available_at));
        self.now = Some(now);
        let mut actions = self.fire_timers(now);
        self.counters.add("events", envelope.event.name(), 1);
        if let Some(slot) = envelope.event.ws_slot()
            && let Some(state) = self.slots.get_mut(slot)
        {
            state.last_message_at = Some(now);
        }
        match envelope.event {
            Event::WsBook { slot, book } => self.on_book(&slot, &book, now)?,
            Event::WsPriceChange { change, .. } => {
                self.on_price_change(&change, now, &mut actions)?
            }
            Event::WsTrade { trade, .. } => self.on_trade(&trade, now, &mut actions),
            Event::WsTickSize { tick, .. } => {
                if let Some(book) = self.books.get_mut(&tick.asset_id) {
                    book.apply_tick_size(
                        tick.new_tick_size,
                        tick.timestamp_millis(),
                        now,
                        self.limits,
                    )?;
                }
            }
            Event::WsMarketResolved { resolved, .. } => {
                if !self.markets.contains_key(&resolved.market) {
                    // Broadcast for every market, and once per connection.
                    self.counters
                        .add("ws_untracked_market", "market_resolved", 1);
                } else {
                    self.on_resolved(&resolved, now, &mut actions);
                }
            }
            Event::WsNewMarket { .. } => {
                actions.push(Action::RegistryHint(RegistryHint::NewMarket))
            }
            Event::WsDropped {
                best_bid_ask,
                unknown,
                ..
            } => {
                self.counters
                    .add("ws_dropped", "best_bid_ask", best_bid_ask);
                self.counters.add("ws_dropped", "unknown", unknown);
            }
            Event::ConnUp { slot, tokens, .. } => {
                self.on_conn_up(&slot, &tokens, now, &mut actions)
            }
            Event::ConnDown { slot, tokens, .. } => self.on_conn_down(&slot, &tokens, now),
            Event::ConnFailed { .. } => self.counters.add("conn_failed", "", 1),
            Event::ConnDecodeError { slot, tokens, .. } => {
                self.on_decode_error(slot, &tokens, now, &mut actions)
            }
            Event::RegistryUpdate {
                add,
                remove,
                changed,
            } => self.on_registry(add, remove, changed, now, &mut actions)?,
            Event::VolumeBackfill {
                condition,
                prints,
                complete,
                from,
                until,
            } => self.on_backfill(&condition, &prints, complete, (from, until), now)?,
            Event::Tick => {}
        }
        self.housekeeping(now, &mut actions);
        Ok(actions)
    }

    /// A tracked market resolved. Every connection carries the event, so only the first
    /// copy is kept.
    fn on_resolved(
        &mut self,
        resolved: &MarketResolvedEvent,
        now: UtcMicros,
        actions: &mut Vec<Action>,
    ) {
        if !self.resolved.insert(resolved.market.clone()) {
            self.counters.add("ws_duplicate", "market_resolved", 1);
            return;
        }
        // An exchange time from the future must not hold the market in its grace period.
        let at = resolved.timestamp_millis().map_or(now, |at| at.min(now));
        actions.push(Action::RegistryHint(RegistryHint::Resolved {
            condition_id: resolved.market.clone(),
            at,
        }));
        actions.push(Action::Persist(Record::Resolution {
            condition_id: resolved.market.clone(),
            winning_token: resolved.winning_asset_id.clone(),
            resolved_at: at,
            source: "websocket".into(),
        }));
    }

    fn on_backfill(
        &mut self,
        condition: &ConditionId,
        prints: &[BackfillPrint],
        complete: bool,
        interval: (UtcMicros, UtcMicros),
        now: UtcMicros,
    ) -> Result<(), InvariantViolation> {
        let tokens = self.markets.get(condition).map(|m| m.info.tokens.clone());
        let (Some(volume), Some(tokens)) = (self.volumes.get_mut(condition), tokens) else {
            // Removed since it was requested.
            self.counters.add("backfill_untracked", "", 1);
            return Ok(());
        };
        let counts = volume.merge_backfill(condition, &tokens, prints, complete, interval, now)?;
        self.counters.add("backfill_rows", "kept", counts.kept);
        self.counters
            .add("backfill_rows", "seen_live", counts.seen_live);
        self.counters
            .add("backfill_rows", "outside", counts.outside);
        self.counters
            .add("backfill_rows", "invalid", counts.invalid);
        self.counters.add(
            "backfills",
            if complete { "complete" } else { "incomplete" },
            1,
        );
        Ok(())
    }

    fn on_book(
        &mut self,
        slot: &ConnSlotId,
        event: &BookEvent,
        now: UtcMicros,
    ) -> Result<(), InvariantViolation> {
        let Some(token) = self.books.get_mut(&event.asset_id) else {
            self.counters.add("ws_untracked_token", "book", 1);
            return Ok(());
        };
        if self.token_slot.get(&event.asset_id) != Some(slot) {
            // A snapshot from a connection that no longer carries the token.
            self.counters.add("ws_wrong_slot", "book", 1);
            return Ok(());
        }
        token.apply_snapshot(
            OrderBook::from_ws_book(event),
            Some(event.timestamp),
            now,
            self.limits,
        )
    }

    fn on_price_change(
        &mut self,
        event: &PriceChangeEvent,
        now: UtcMicros,
        actions: &mut Vec<Action>,
    ) -> Result<(), InvariantViolation> {
        let mut by_token: BTreeMap<TokenId, Vec<LevelChange>> = BTreeMap::new();
        for change in &event.price_changes {
            match LevelChange::from_price_change(change, event.timestamp) {
                Ok(level) => by_token
                    .entry(change.asset_id.clone())
                    .or_default()
                    .push(level),
                Err(_) => self.counters.add("ws_bad_side", "price_change", 1),
            }
            if self.books.contains_key(&change.asset_id)
                && let Some(seen) =
                    self.ordering
                        .on_change(&change.asset_id, change.price, event.timestamp, now)
            {
                actions.push(observation_metric(seen));
            }
        }
        for (token, changes) in by_token {
            let Some(book) = self.books.get_mut(&token) else {
                self.counters.add("ws_untracked_token", "price_change", 1);
                continue;
            };
            if !book.apply_changes(&changes, Some(event.timestamp), now, self.limits)? {
                self.counters.add("price_change_without_book", "", 1);
            }
        }
        Ok(())
    }

    fn on_trade(&mut self, event: &LastTradePriceEvent, now: UtcMicros, actions: &mut Vec<Action>) {
        let Some(condition) = self.token_market.get(&event.asset_id).cloned() else {
            self.counters.add("ws_untracked_token", "trade", 1);
            return;
        };
        if let Some(seen) =
            self.ordering
                .on_trade(&event.asset_id, event.price, event.timestamp, now)
        {
            actions.push(observation_metric(seen));
        }
        let Some(market) = self.markets.get(&condition) else {
            return;
        };
        let print = RawPrint::from_ws(event, now)
            .and_then(|raw| NormalisedPrint::new(&raw, &market.info.tokens));
        match (print, self.volumes.get_mut(&condition)) {
            (Ok(print), Some(volume)) => {
                volume.record_live(&print);
            }
            (Err(_), _) => self.counters.add("trades_not_normalisable", "", 1),
            (Ok(_), None) => {}
        }
    }

    fn on_conn_up(
        &mut self,
        slot: &ConnSlotId,
        tokens: &[TokenId],
        now: UtcMicros,
        actions: &mut Vec<Action>,
    ) {
        self.counters.add("conn_up", "", 1);
        let Some(state) = self.slots.get_mut(slot) else {
            // A slot closed since; its tokens are elsewhere or gone.
            self.counters.add("stale_lifecycle", "conn_up", 1);
            return;
        };
        state.connected_since = Some(now);
        state.last_message_at = Some(now);
        let tokens = self.slot_tokens(slot, tokens);
        let tokens = tokens.as_slice();
        for token in tokens {
            if let Some(book) = self.books.get_mut(token) {
                book.distrust_from(Distrust::AwaitingSnapshot, now);
            }
        }
        self.start_live(tokens, now, actions);
    }

    /// The tokens a lifecycle event of `slot` concerns: every token the engine placed on the
    /// slot (all share its connection, including any whose `Subscribe` the connection had not
    /// applied when the event was made), plus any the event names that the slot still
    /// carries (an event can name tokens unsubscribed since).
    fn slot_tokens(&self, slot: &ConnSlotId, tokens: &[TokenId]) -> Vec<TokenId> {
        let mut own: BTreeSet<TokenId> = self
            .slots
            .get(slot)
            .map(|state| state.tokens.clone())
            .unwrap_or_default();
        own.extend(
            tokens
                .iter()
                .filter(|token| self.token_slot.get(*token) == Some(slot))
                .cloned(),
        );
        own.into_iter().collect()
    }

    /// Live prints of `tokens`' markets flow from `now`: asks for the backfill each needs (the
    /// whole window the first time, the gap after a disconnect).
    fn start_live(&mut self, tokens: &[TokenId], now: UtcMicros, actions: &mut Vec<Action>) {
        let lookback = delta(self.config.volume_window) + delta(self.config.volume_slack);
        let conditions: BTreeSet<ConditionId> = tokens
            .iter()
            .filter_map(|token| self.token_market.get(token).cloned())
            .collect();
        let mut backfills = Backfills::new();
        for condition in conditions {
            if let Some(volume) = self.volumes.get_mut(&condition)
                && let Some(interval) = volume.connected(now, lookback)
            {
                backfills.entry(interval).or_default().insert(condition);
            }
        }
        push_backfills(backfills, actions);
    }

    fn on_conn_down(&mut self, slot: &ConnSlotId, tokens: &[TokenId], now: UtcMicros) {
        self.counters.add("conn_down", "", 1);
        let Some(state) = self.slots.get_mut(slot) else {
            self.counters.add("stale_lifecycle", "conn_down", 1);
            return;
        };
        state.connected_since = None;
        // Prints may be missing since the last message, not just since the loss was noticed.
        let last_message = state.last_message_at.map_or(now, |at| at.min(now));
        for token in &self.slot_tokens(slot, tokens) {
            if let Some(book) = self.books.get_mut(token) {
                book.distrust_from(Distrust::Disconnected, now);
            }
            if let Some(volume) = self
                .token_market
                .get(token)
                .and_then(|c| self.volumes.get_mut(c))
            {
                volume.disconnected(last_message);
            }
        }
    }

    fn on_decode_error(
        &mut self,
        slot: ConnSlotId,
        tokens: &[TokenId],
        now: UtcMicros,
        actions: &mut Vec<Action>,
    ) {
        self.counters.add("decode_errors", "", 1);
        let Some(state) = self.slots.get(&slot) else {
            self.counters.add("stale_lifecycle", "decode_error", 1);
            return;
        };
        let pending = state.decode_errors;
        let carried = self.slot_tokens(&slot, tokens);
        for token in &carried {
            if let Some(book) = self.books.get_mut(token) {
                book.distrust_from(Distrust::DecodeError, now);
            }
        }
        // The lost message may have been a trade: refetch that moment's prints.
        let conditions = self.conditions_of(&carried);
        if let Some(pending) = pending {
            // A resnapshot is already due: a recurring bad message must not set off a
            // backfill and a resubscribe each time, so this one waits for it.
            self.counters.add("decode_errors_coalesced", "", 1);
            for condition in &conditions {
                if let Some(volume) = self.volumes.get_mut(condition) {
                    volume.suspect(now);
                }
            }
            if let Some(state) = self.slots.get_mut(&slot) {
                state.decode_errors = Some(DecodeErrors {
                    last: now,
                    coalesced_from: Some(pending.coalesced_from.unwrap_or(now)),
                });
            }
            return;
        }
        let mut backfills = Backfills::new();
        for condition in conditions {
            if let Some(interval) = self
                .volumes
                .get_mut(&condition)
                .and_then(|volume| volume.lost_message(now))
            {
                backfills.entry(interval).or_default().insert(condition);
            }
        }
        push_backfills(backfills, actions);
        if let Some(state) = self.slots.get_mut(&slot) {
            state.decode_errors = Some(DecodeErrors {
                last: now,
                coalesced_from: None,
            });
        }
        let due = now + delta(self.config.resnapshot_after_decode_error);
        self.schedule(due, Timer::Resnapshot { slot });
    }

    fn conditions_of<'a>(
        &self,
        tokens: impl IntoIterator<Item = &'a TokenId>,
    ) -> BTreeSet<ConditionId> {
        tokens
            .into_iter()
            .filter_map(|token| self.token_market.get(token).cloned())
            .collect()
    }

    fn on_registry(
        &mut self,
        add: Vec<LiveMarket>,
        remove: Vec<RemovedMarket>,
        changed: Vec<MarketChanged>,
        now: UtcMicros,
        actions: &mut Vec<Action>,
    ) -> Result<(), InvariantViolation> {
        // Removals first: a market whose tokens changed is removed and added again.
        let remove: Vec<RemovedMarket> = remove
            .into_iter()
            .filter(|removed| {
                let tracked = self
                    .markets
                    .get(&removed.condition_id)
                    .is_some_and(|m| m.info.tokens == removed.tokens);
                if !tracked {
                    // Never added (rejected below), or already gone.
                    self.counters.add("registry_ignored", "remove", 1);
                }
                tracked
            })
            .collect();
        let removed_tokens: Vec<MarketTokens> = remove.iter().map(|r| r.tokens.clone()).collect();
        for (slot, tokens) in self.assignment.unassign(&removed_tokens) {
            let Some(state) = self.slots.get_mut(&slot) else {
                invariant!(
                    self.slots.contains_key(&slot),
                    Err(InvariantViolation("an assigned slot is open"))
                );
                continue;
            };
            for token in &tokens {
                state.tokens.remove(token);
            }
            if state.tokens.is_empty() {
                self.slots.remove(&slot);
                actions.push(Action::CloseSlot { slot });
            } else {
                actions.push(Action::Unsubscribe {
                    slot,
                    tokens: sorted(tokens),
                });
            }
        }
        for removed in &remove {
            for token in [&removed.tokens.yes, &removed.tokens.no] {
                self.books.remove(token);
                self.token_slot.remove(token);
                self.token_market.remove(token);
                self.ordering.forget(token);
            }
            self.markets.remove(&removed.condition_id);
            self.volumes.remove(&removed.condition_id);
            self.resolved.remove(&removed.condition_id);
        }

        let add = self.acceptable(add);
        let added_tokens: Vec<MarketTokens> = add.iter().map(|m| m.info.tokens.clone()).collect();
        for market in add {
            let id = market.info.condition_id.clone();
            for token in [&market.info.tokens.yes, &market.info.tokens.no] {
                self.token_market.insert(token.clone(), id.clone());
                self.books.insert(token.clone(), TokenBook::new(now));
            }
            self.volumes.insert(
                id.clone(),
                ConditionVolume::new(self.config.volume_window, self.config.volume_slack)?,
            );
            actions.push(Action::Persist(fee_record(&market)));
            actions.push(Action::Persist(Record::Market {
                seen_at: now,
                market: Box::new(market.clone()),
            }));
            self.markets.insert(id, market);
        }
        for (slot, tokens) in self.assignment.assign(&added_tokens) {
            for token in &tokens {
                self.token_slot.insert(token.clone(), slot.clone());
            }
            let tokens = sorted(tokens);
            match self.slots.get_mut(&slot) {
                Some(state) => {
                    state.tokens.extend(tokens.iter().cloned());
                    // On a connection that is up, no `ConnUp` will follow for these tokens:
                    // their live prints start now. (On one that is down, its next `ConnUp`
                    // carries them.)
                    let connected = state.connected();
                    actions.push(Action::Subscribe {
                        slot,
                        tokens: tokens.clone(),
                    });
                    if connected {
                        self.start_live(&tokens, now, actions);
                    }
                }
                None => {
                    self.slots.insert(
                        slot.clone(),
                        SlotState {
                            tokens: tokens.iter().cloned().collect(),
                            ..SlotState::default()
                        },
                    );
                    actions.push(Action::OpenSlot { slot, tokens });
                }
            }
        }

        for change in changed {
            let id = change.market.info.condition_id.clone();
            if !self.markets.contains_key(&id) {
                continue;
            }
            if change.fee_changed {
                actions.push(Action::Persist(fee_record(&change.market)));
            }
            if change.details_changed {
                actions.push(Action::Persist(Record::Market {
                    seen_at: now,
                    market: Box::new(change.market.clone()),
                }));
            }
            self.markets.insert(id, change.market);
        }
        invariant!(
            self.token_market.len() == 2 * self.markets.len(),
            Err(InvariantViolation("every tracked market has two tokens"))
        );
        invariant!(
            self.token_slot.len() == self.token_market.len(),
            Err(InvariantViolation("every tracked token is on a slot"))
        );
        Ok(())
    }

    /// The added markets that can be tracked: remote data must not break the engine's
    /// one-token-one-market bookkeeping, so a market already tracked, or sharing a token with
    /// one, is skipped (and stays untracked: its later removal or change is ignored too).
    fn acceptable(&mut self, add: Vec<LiveMarket>) -> Vec<LiveMarket> {
        let mut tokens: BTreeSet<TokenId> = BTreeSet::new();
        let mut accepted = Vec::with_capacity(add.len());
        for market in add {
            let info = &market.info;
            let fresh = !self.markets.contains_key(&info.condition_id)
                && info.tokens.yes != info.tokens.no
                && [&info.tokens.yes, &info.tokens.no]
                    .into_iter()
                    .all(|token| !self.token_market.contains_key(token) && !tokens.contains(token));
            if !fresh {
                tracing::warn!(condition = %info.condition_id, "skipping a market already tracked or sharing a token");
                self.counters.add("registry_ignored", "add", 1);
                continue;
            }
            tokens.insert(info.tokens.yes.clone());
            tokens.insert(info.tokens.no.clone());
            accepted.push(market);
        }
        accepted
    }

    fn schedule(&mut self, due: UtcMicros, timer: Timer) {
        self.next_timer += 1;
        self.timers.insert((due, self.next_timer), timer);
    }

    fn fire_timers(&mut self, now: UtcMicros) -> Vec<Action> {
        let mut actions = Vec::new();
        while let Some(entry) = self.timers.first_entry() {
            if entry.key().0 > now {
                break;
            }
            match entry.remove() {
                Timer::Resnapshot { slot } => self.resnapshot(slot, now, &mut actions),
            }
        }
        actions
    }

    /// A decode error's resnapshot is due: resubscribes the slot's tokens still untrusted from
    /// its errors, and backfills the errors coalesced into it.
    fn resnapshot(&mut self, slot: ConnSlotId, now: UtcMicros, actions: &mut Vec<Action>) {
        let Some(state) = self.slots.get_mut(&slot) else {
            return;
        };
        let Some(errors) = state.decode_errors.take() else {
            // Only `on_decode_error` schedules this timer, and it sets the errors first.
            tracing::error!(%slot, "a resnapshot is due with no decode errors recorded");
            return;
        };
        let connected = state.connected();
        let carried: Vec<TokenId> = state.tokens.iter().cloned().collect();
        let waiting: Vec<TokenId> = carried
            .iter()
            .filter(|token| {
                self.books.get(*token).is_some_and(|book| {
                    book.distrust() == Some(Distrust::DecodeError)
                        && book.untrusted_since().is_some_and(|at| at <= errors.last)
                })
            })
            .cloned()
            .collect();
        let resubscribing = !waiting.is_empty() && connected;
        let resubscribed = self.conditions_of(&waiting);
        if resubscribing {
            self.counters.add("resnapshots", "", 1);
            actions.push(Action::Resnapshot {
                slot,
                tokens: waiting,
            });
        }
        // Prints lost to the errors coalesced into this one, and to the resubscribe itself
        // (a trade between the unsubscribe and the subscribe).
        let mut backfills = Backfills::new();
        for condition in self.conditions_of(&carried) {
            let lost = match (
                errors.coalesced_from,
                resubscribed.contains(&condition) && resubscribing,
            ) {
                (Some(from), resub) => Some((from, if resub { now } else { errors.last })),
                (None, true) => Some((now, now)),
                (None, false) => None,
            };
            if let Some((from, until)) = lost
                && let Some(interval) = self
                    .volumes
                    .get_mut(&condition)
                    .and_then(|volume| volume.lost_messages(from, until))
            {
                backfills.entry(interval).or_default().insert(condition);
            }
        }
        push_backfills(backfills, actions);
    }

    /// Counters not yet written, as metrics: for a clean shutdown.
    pub fn flush_metrics(&mut self) -> Vec<Action> {
        let Some(now) = self.now else {
            return Vec::new();
        };
        self.counters
            .drain(now)
            .into_iter()
            .map(|m| Action::Persist(Record::Metric(m)))
            .collect()
    }

    /// Periodic work: ordering expiry, untrusted-token alarms, volume eviction, metrics.
    fn housekeeping(&mut self, now: UtcMicros, actions: &mut Vec<Action>) {
        for seen in self.ordering.expire(now) {
            actions.push(observation_metric(seen));
        }
        if self.next_scan_at.is_none_or(|at| now >= at) {
            self.next_scan_at = Some(now + TimeDelta::seconds(1));
            let alarm_after = delta(self.config.untrusted_alarm_after);
            for (token, book) in &mut self.books {
                let connected_since = self
                    .token_slot
                    .get(token)
                    .and_then(|slot| self.slots.get(slot))
                    .and_then(|state| state.connected_since);
                let (Some(untrusted_since), Some(reason), Some(connected_since)) =
                    (book.untrusted_since(), book.distrust(), connected_since)
                else {
                    continue;
                };
                // Time while disconnected does not count.
                let since = untrusted_since.max(connected_since);
                if !book.alarmed && now - since >= alarm_after {
                    book.alarmed = true;
                    tracing::warn!(%token, reason = reason.as_str(), since = %since, "token untrusted too long while connected");
                    actions.push(Action::Persist(Record::Metric(
                        Metric::new(
                            "untrusted_token_alarm",
                            now,
                            (now - since).num_milliseconds() as f64,
                        )
                        .label("token", token.to_string())
                        .label("reason", reason.as_str()),
                    )));
                }
            }
        }
        if self.next_metrics_at.is_none_or(|at| now >= at) {
            let first = self.next_metrics_at.is_none();
            self.next_metrics_at = Some(now + delta(self.config.metrics_interval));
            if first {
                return;
            }
            for volume in self.volumes.values_mut() {
                volume.evict(now);
            }
            let untrusted = self.books.values().filter(|b| !b.is_trusted()).count();
            let mut metrics = self.counters.drain(now);
            metrics.push(Metric::new("tokens_tracked", now, self.books.len() as f64));
            metrics.push(Metric::new("tokens_untrusted", now, untrusted as f64));
            metrics.push(Metric::new(
                "markets_tracked",
                now,
                self.markets.len() as f64,
            ));
            metrics.push(Metric::new(
                "slots_connected",
                now,
                self.slots.values().filter(|s| s.connected()).count() as f64,
            ));
            actions.extend(
                metrics
                    .into_iter()
                    .map(|m| Action::Persist(Record::Metric(m))),
            );
        }
    }
}

fn fee_record(market: &LiveMarket) -> Record {
    Record::MarketFees {
        condition_id: market.info.condition_id.clone(),
        fees: market.fees,
        fee_exponent_unverified: market.fees.exponent_unverified(),
    }
}

fn observation_metric(seen: Observation) -> Action {
    // No exchange delta (a trade with no matching change): a count, not a NaN, which would
    // neither serialise nor compare equal on replay.
    let metric = match seen.exchange_delta {
        Some(d) => Metric::new(
            "ws_trade_vs_price_change_ms",
            seen.at,
            d.num_microseconds().unwrap_or(0) as f64 / 1000.0,
        ),
        None => Metric::new("ws_trade_vs_price_change_unmatched", seen.at, 1.0),
    };
    Action::Persist(Record::Metric(metric.label("order", seen.arrival.as_str())))
}

#[cfg(test)]
mod tests;
