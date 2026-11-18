//! Websocket connection slots (D11): each slot owns one marcasite
//! `ReconnectingMarketChannel` and turns its stream into envelopes for the engine.
//!
//! [translate] is the pure mapping of the spec's C2 table; [run_slot] is the loop around it,
//! generic over the stream so tests can feed scripted items on paused time.

use std::time::Duration;

use futures_util::{Stream, StreamExt};
use marcasite::{
    Error, WebSocketErrorKind,
    types::TokenId,
    ws::{
        MarketChannel, MarketEvent, MarketSubscription, ReconnectEvent, ReconnectPolicy,
        ReconnectingMarketChannel,
    },
};
use pm_core::{archive::record::ConnSlotId, clock::SharedClock, units::UtcMicros};
use tokio::sync::mpsc;

use crate::{
    config::IngestConfig,
    event::{Event, Stamped},
};

/// What one stream item becomes.
#[derive(Debug)]
pub enum Translated {
    /// An event for the engine, with its exchange time if it has one.
    Event {
        event: Event,
        exchange_time: Option<UtcMicros>,
    },
    /// A market event the engine does not take; counted.
    Dropped(DroppedKind),
    /// The stream ended for good (attempts exhausted or a fatal error): the run halts.
    Terminal(Error),
    /// A lifecycle item this version does not know; logged and skipped.
    Ignored,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DroppedKind {
    BestBidAsk,
    Unknown,
}

/// Maps one item of a slot's stream (spec C2, "Translate the stream"). `tokens` gives the
/// slot's current subscription; it is only called for decode errors.
pub fn translate(
    slot: &ConnSlotId,
    item: marcasite::Result<ReconnectEvent>,
    tokens: impl FnOnce() -> Vec<TokenId>,
) -> Translated {
    let event = |event| Translated::Event {
        event,
        exchange_time: None,
    };
    match item {
        Ok(ReconnectEvent::Connected { asset_ids, attempt }) => event(Event::ConnUp {
            slot: slot.clone(),
            tokens: asset_ids,
            attempt,
        }),
        Ok(ReconnectEvent::Disconnected { asset_ids, error }) => event(Event::ConnDown {
            slot: slot.clone(),
            tokens: asset_ids,
            reason: error.map(|e| e.to_string()),
        }),
        Ok(ReconnectEvent::ConnectFailed {
            attempt,
            error,
            retry_in,
        }) => event(Event::ConnFailed {
            slot: slot.clone(),
            attempt,
            error: error.to_string(),
            retry_in_ms: u64::try_from(retry_in.as_millis()).unwrap_or(u64::MAX),
        }),
        Ok(ReconnectEvent::Market(market)) => {
            let exchange_time = market.timestamp();
            match Event::from_market_event(slot.clone(), market) {
                Ok(event) => Translated::Event {
                    event,
                    exchange_time,
                },
                Err(other) => Translated::Dropped(match *other {
                    MarketEvent::BestBidAsk(_) => DroppedKind::BestBidAsk,
                    _ => DroppedKind::Unknown,
                }),
            }
        }
        Ok(_) => Translated::Ignored,
        Err(Error::WebSocket(error)) if error.kind() == WebSocketErrorKind::Decode => {
            event(Event::ConnDecodeError {
                slot: slot.clone(),
                tokens: tokens(),
                error: error.to_string(),
            })
        }
        Err(error) => Translated::Terminal(error),
    }
}

/// How often a slot reports its dropped-message counts at most, besides on lifecycle events.
const DROPPED_REPORT_INTERVAL: Duration = Duration::from_secs(60);

#[derive(Debug, Default)]
struct DroppedCounts {
    best_bid_ask: u64,
    unknown: u64,
    since: Option<UtcMicros>,
}

impl DroppedCounts {
    fn take(&mut self, slot: &ConnSlotId, now: UtcMicros) -> Option<Event> {
        self.since = Some(now);
        if self.best_bid_ask == 0 && self.unknown == 0 {
            return None;
        }
        let event = Event::WsDropped {
            slot: slot.clone(),
            best_bid_ask: self.best_bid_ask,
            unknown: self.unknown,
        };
        self.best_bid_ask = 0;
        self.unknown = 0;
        Some(event)
    }
}

/// Why a slot stopped.
#[derive(Debug, thiserror::Error)]
pub enum SlotError {
    #[error("connection slot {slot} gave up: {source}")]
    Terminal { slot: ConnSlotId, source: Error },
}

/// Runs a slot until its stream ends (closed) or fails for good, or the engine is gone.
/// Every envelope is stamped with `clock.now()` when its item is yielded.
///
/// # Errors
///
/// [SlotError::Terminal] when the stream fails for good; a `ConnDown` is sent first.
pub async fn run_slot<S>(
    slot: ConnSlotId,
    mut stream: S,
    tokens: impl Fn() -> Vec<TokenId> + Send,
    clock: SharedClock,
    out: mpsc::Sender<Stamped>,
) -> Result<(), SlotError>
where
    S: Stream<Item = marcasite::Result<ReconnectEvent>> + Unpin,
{
    let mut dropped = DroppedCounts::default();
    let report_every =
        chrono::TimeDelta::from_std(DROPPED_REPORT_INTERVAL).unwrap_or(chrono::TimeDelta::MAX);
    while let Some(item) = stream.next().await {
        let now = clock.now();
        let lifecycle = matches!(
            item,
            Ok(ReconnectEvent::Connected { .. } | ReconnectEvent::Disconnected { .. })
        );
        let mut send = Vec::with_capacity(2);
        match translate(&slot, item, &tokens) {
            Translated::Event {
                event,
                exchange_time,
            } => send.push(Stamped {
                event_time: exchange_time.unwrap_or(now),
                available_at: now,
                event,
            }),
            Translated::Dropped(kind) => match kind {
                DroppedKind::BestBidAsk => dropped.best_bid_ask += 1,
                DroppedKind::Unknown => dropped.unknown += 1,
            },
            Translated::Ignored => tracing::debug!(%slot, "unrecognised channel item skipped"),
            Translated::Terminal(source) => {
                tracing::error!(%slot, error = %source, "connection slot gave up");
                let down = Event::ConnDown {
                    slot: slot.clone(),
                    tokens: tokens(),
                    reason: Some(source.to_string()),
                };
                // If the engine is gone too, the run is ending anyway.
                let _ = out.send(Stamped::at(now, down)).await;
                return Err(SlotError::Terminal { slot, source });
            }
        }
        if (lifecycle
            || dropped
                .since
                .is_none_or(|since| now - since >= report_every))
            && let Some(event) = dropped.take(&slot, now)
        {
            send.insert(0, Stamped::at(now, event));
        }
        for stamped in send {
            if out.send(stamped).await.is_err() {
                return Ok(());
            }
        }
    }
    Ok(())
}

/// Builds a slot's channel: reconnecting with the configured policy, custom features on
/// (`new_market`, `market_resolved`), the same settings on every connection (open question
/// 26).
///
/// # Errors
///
/// If the policy or subscription is invalid.
pub fn open_channel(
    url: &url::Url,
    config: &IngestConfig,
    tokens: Vec<TokenId>,
) -> marcasite::Result<ReconnectingMarketChannel> {
    let policy = ReconnectPolicy::new(
        config.reconnect_initial_backoff,
        config.reconnect_max_backoff,
    )?
    .unlimited_attempts()
    .stable_after(config.reconnect_stable_after);
    MarketChannel::builder()
        .url(url.as_str())
        .connect_timeout(config.connect_timeout)
        .heartbeat_interval(config.heartbeat_interval)
        .idle_timeout(config.idle_timeout)
        .reconnecting()
        .policy(policy)
        .build(MarketSubscription::new(tokens).custom_feature_enabled(true))
}

#[cfg(test)]
mod tests;
