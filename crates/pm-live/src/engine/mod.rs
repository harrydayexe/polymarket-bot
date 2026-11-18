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
    ws::{BookEvent, LastTradePriceEvent, PriceChangeEvent},
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
    event::{Envelope, Event, LiveMarket, MarketChanged, RemovedMarket, Stamped},
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
}

impl SlotState {
    pub fn connected(&self) -> bool {
        self.connected_since.is_some()
    }
}

/// Something due at a time.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Timer {
    /// Resubscribe tokens of `slot` still untrusted after a decode error at `since`.
    Resnapshot { slot: ConnSlotId, since: UtcMicros },
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
        match envelope.event {
            Event::WsBook { slot, book } => self.on_book(&slot, &book, now)?,
            Event::WsPriceChange { change, .. } => self.on_price_change(&change, now)?,
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
                let at = resolved.timestamp_millis().unwrap_or(now);
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
            Event::ConnDecodeError { slot, tokens, .. } => self.on_decode_error(slot, &tokens, now),
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
            } => {
                let tokens = self.markets.get(&condition).map(|m| m.info.tokens.clone());
                if let (Some(volume), Some(tokens)) = (self.volumes.get_mut(&condition), tokens) {
                    let counts = volume.merge_backfill(
                        &condition,
                        &tokens,
                        &prints,
                        complete,
                        (from, until),
                        now,
                    )?;
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
                }
            }
            Event::Tick => {}
        }
        self.housekeeping(now, &mut actions);
        Ok(actions)
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
            if self.books.contains_key(&change.asset_id) {
                self.ordering
                    .on_change(&change.asset_id, change.price, event.timestamp, now);
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
        if let Some(state) = self.slots.get_mut(slot) {
            state.connected_since = Some(now);
        }
        let lookback = delta(self.config.volume_window) + delta(self.config.volume_slack);
        let mut backfills: BTreeMap<(UtcMicros, UtcMicros), BTreeSet<ConditionId>> =
            BTreeMap::new();
        for token in tokens {
            if let Some(book) = self.books.get_mut(token) {
                book.distrust_from(Distrust::AwaitingSnapshot, now);
            }
            let Some(condition) = self.token_market.get(token) else {
                continue;
            };
            if backfills.values().any(|set| set.contains(condition)) {
                continue;
            }
            if let Some(volume) = self.volumes.get_mut(condition)
                && let Some(interval) = volume.connected(now, lookback)
            {
                backfills
                    .entry(interval)
                    .or_default()
                    .insert(condition.clone());
            }
        }
        for ((from, until), conditions) in backfills {
            actions.push(Action::RequestBackfill {
                conditions: conditions.into_iter().collect(),
                from,
                until,
            });
        }
    }

    fn on_conn_down(&mut self, slot: &ConnSlotId, tokens: &[TokenId], now: UtcMicros) {
        self.counters.add("conn_down", "", 1);
        if let Some(state) = self.slots.get_mut(slot) {
            state.connected_since = None;
        }
        for token in tokens {
            if let Some(book) = self.books.get_mut(token) {
                book.distrust_from(Distrust::Disconnected, now);
            }
            if let Some(volume) = self
                .token_market
                .get(token)
                .and_then(|c| self.volumes.get_mut(c))
            {
                volume.disconnected(now);
            }
        }
    }

    fn on_decode_error(&mut self, slot: ConnSlotId, tokens: &[TokenId], now: UtcMicros) {
        self.counters.add("decode_errors", "", 1);
        let carried: Vec<TokenId> = match self.slots.get(&slot) {
            Some(state) => state.tokens.iter().cloned().collect(),
            None => tokens.to_vec(),
        };
        for token in carried.iter().chain(tokens) {
            if let Some(book) = self.books.get_mut(token) {
                book.distrust_from(Distrust::DecodeError, now);
            }
        }
        let due = now + delta(self.config.resnapshot_after_decode_error);
        self.schedule(due, Timer::Resnapshot { slot, since: now });
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
        let removed_tokens: Vec<MarketTokens> = remove.iter().map(|r| r.tokens.clone()).collect();
        for (slot, tokens) in self.assignment.unassign(&removed_tokens) {
            let state = self.slots.entry(slot.clone()).or_default();
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
        }

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
                    actions.push(Action::Subscribe { slot, tokens });
                }
                None => {
                    self.slots.insert(
                        slot.clone(),
                        SlotState {
                            tokens: tokens.iter().cloned().collect(),
                            connected_since: None,
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
                Timer::Resnapshot { slot, since } => {
                    let Some(state) = self.slots.get(&slot) else {
                        continue;
                    };
                    let waiting: Vec<TokenId> = state
                        .tokens
                        .iter()
                        .filter(|token| {
                            self.books.get(*token).is_some_and(|book| {
                                book.distrust() == Some(Distrust::DecodeError)
                                    && book.untrusted_since().is_some_and(|at| at <= since)
                            })
                        })
                        .cloned()
                        .collect();
                    if !waiting.is_empty() && state.connected() {
                        self.counters.add("resnapshots", "", 1);
                        actions.push(Action::Resnapshot {
                            slot,
                            tokens: waiting,
                        });
                    }
                }
            }
        }
        actions
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
    let value = seen.exchange_delta.map_or(f64::NAN, |d| {
        d.num_microseconds().unwrap_or(0) as f64 / 1000.0
    });
    Action::Persist(Record::Metric(
        Metric::new("ws_trade_vs_price_change_ms", seen.at, value)
            .label("order", seen.arrival.as_str()),
    ))
}

#[cfg(test)]
mod tests;
