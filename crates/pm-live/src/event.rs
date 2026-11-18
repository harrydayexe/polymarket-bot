//! What the engine consumes: [Envelope]s carrying an [Event], stamped with when the fact
//! happened (`event_time`) and when this process received it (`available_at`, rule 3 of the
//! Phase 1 spec).
//!
//! Every type here serialises, because every envelope is written to the engine log (D10) and
//! a run can be replayed from it.

use chrono::{DateTime, Utc};
use marcasite::{
    types::{ConditionId, TokenId},
    ws::{
        BookEvent, LastTradePriceEvent, MarketEvent, MarketResolvedEvent, NewMarketEvent,
        PriceChangeEvent, TickSizeChangeEvent,
    },
};
use pm_core::{
    archive::record::ConnSlotId,
    market::{MarketInfo, MarketTokens},
    normalize::{TakerSide, TxHash},
    units::{FeeParams, Price, Shares, UtcMicros, micros},
};
use serde::{Deserialize, Serialize};

/// One input to the engine.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Envelope {
    /// Assigned by the engine on receipt, from 1; consecutive within a run.
    pub seq: u64,
    /// When the fact happened: the exchange timestamp where there is one, otherwise the
    /// receive time.
    #[serde(with = "micros")]
    pub event_time: UtcMicros,
    /// When this process received it. Decisions may only use facts with
    /// `available_at ≤ now`.
    #[serde(with = "micros")]
    pub available_at: UtcMicros,
    pub event: Event,
}

/// An event before the engine has given it a sequence number: what I/O tasks send.
#[derive(Debug, Clone, PartialEq)]
pub struct Stamped {
    pub event_time: UtcMicros,
    pub available_at: UtcMicros,
    pub event: Event,
}

impl Stamped {
    /// An event whose only time is when it was received (no exchange timestamp).
    pub fn at(available_at: UtcMicros, event: Event) -> Self {
        Self {
            event_time: available_at,
            available_at,
            event,
        }
    }

    pub fn into_envelope(self, seq: u64) -> Envelope {
        Envelope {
            seq,
            event_time: self.event_time,
            available_at: self.available_at,
            event: self.event,
        }
    }
}

/// The engine's inputs. C2 produces the websocket, connection, registry and backfill events;
/// later components add theirs.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Event {
    /// A full order book snapshot.
    WsBook { slot: ConnSlotId, book: BookEvent },
    /// Price level changes, possibly on both tokens of a market.
    WsPriceChange {
        slot: ConnSlotId,
        change: PriceChangeEvent,
    },
    /// A trade print (`last_trade_price`).
    WsTrade {
        slot: ConnSlotId,
        trade: LastTradePriceEvent,
    },
    WsTickSize {
        slot: ConnSlotId,
        tick: TickSizeChangeEvent,
    },
    WsMarketResolved {
        slot: ConnSlotId,
        resolved: Box<MarketResolvedEvent>,
    },
    /// A hint that a market was created: triggers an early registry poll. Selection still
    /// comes from Gamma.
    WsNewMarket {
        slot: ConnSlotId,
        market: Box<NewMarketEvent>,
    },
    /// Messages a slot received and dropped (`best_bid_ask`, unknown), counted since its last
    /// report.
    WsDropped {
        slot: ConnSlotId,
        best_bid_ask: u64,
        unknown: u64,
    },
    /// A slot's connection is up and subscribed to `tokens`; a `book` per token follows.
    ConnUp {
        slot: ConnSlotId,
        tokens: Vec<TokenId>,
        attempt: u32,
    },
    /// A slot's connection was lost; nothing arrives for `tokens` until the next `ConnUp`.
    ConnDown {
        slot: ConnSlotId,
        tokens: Vec<TokenId>,
        reason: Option<String>,
    },
    /// A connection attempt failed; the slot retries after `retry_in_ms`.
    ConnFailed {
        slot: ConnSlotId,
        attempt: u32,
        error: String,
        retry_in_ms: u64,
    },
    /// A message on the slot's connection could not be decoded: it may have been a lost
    /// `price_change`, so every token on the connection is untrusted until its next `book`.
    ConnDecodeError {
        slot: ConnSlotId,
        tokens: Vec<TokenId>,
        error: String,
    },
    /// Changes from a registry poll. Process `remove` before `add` (a market whose tokens
    /// changed is in both).
    RegistryUpdate {
        add: Vec<LiveMarket>,
        remove: Vec<RemovedMarket>,
        changed: Vec<MarketChanged>,
    },
    /// Taker prints of the last 24 h of one condition from the Data API, newest first as
    /// returned, for the interval `[from, until)`.
    VolumeBackfill {
        condition: ConditionId,
        prints: Vec<BackfillPrint>,
        /// Whether every page in the interval was fetched.
        complete: bool,
        #[serde(with = "micros")]
        from: UtcMicros,
        #[serde(with = "micros")]
        until: UtcMicros,
    },
    /// Fires engine timers in quiet periods (every 100 ms).
    Tick,
}

impl Event {
    /// The websocket event as received, for the `Ws*` variants; the event back for kinds the
    /// engine does not take (`best_bid_ask`, unknown).
    pub fn from_market_event(
        slot: ConnSlotId,
        event: MarketEvent,
    ) -> Result<Event, Box<MarketEvent>> {
        Ok(match event {
            MarketEvent::Book(book) => Event::WsBook { slot, book },
            MarketEvent::PriceChange(change) => Event::WsPriceChange { slot, change },
            MarketEvent::LastTradePrice(trade) => Event::WsTrade { slot, trade },
            MarketEvent::TickSizeChange(tick) => Event::WsTickSize { slot, tick },
            MarketEvent::MarketResolved(resolved) => Event::WsMarketResolved { slot, resolved },
            MarketEvent::NewMarket(market) => Event::WsNewMarket { slot, market },
            other => return Err(Box::new(other)),
        })
    }

    /// The slot of a market message (`Ws*` events other than `WsDropped`).
    pub fn ws_slot(&self) -> Option<&ConnSlotId> {
        match self {
            Event::WsBook { slot, .. }
            | Event::WsPriceChange { slot, .. }
            | Event::WsTrade { slot, .. }
            | Event::WsTickSize { slot, .. }
            | Event::WsMarketResolved { slot, .. }
            | Event::WsNewMarket { slot, .. } => Some(slot),
            _ => None,
        }
    }

    /// A short name for logs and metrics.
    pub fn name(&self) -> &'static str {
        match self {
            Event::WsBook { .. } => "ws_book",
            Event::WsPriceChange { .. } => "ws_price_change",
            Event::WsTrade { .. } => "ws_trade",
            Event::WsTickSize { .. } => "ws_tick_size",
            Event::WsMarketResolved { .. } => "ws_market_resolved",
            Event::WsNewMarket { .. } => "ws_new_market",
            Event::WsDropped { .. } => "ws_dropped",
            Event::ConnUp { .. } => "conn_up",
            Event::ConnDown { .. } => "conn_down",
            Event::ConnFailed { .. } => "conn_failed",
            Event::ConnDecodeError { .. } => "conn_decode_error",
            Event::RegistryUpdate { .. } => "registry_update",
            Event::VolumeBackfill { .. } => "volume_backfill",
            Event::Tick => "tick",
        }
    }
}

/// A market in the trading universe with the fee parameters in force for it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LiveMarket {
    /// As parsed from Gamma. Its own fee fields are not serialised; [Self::fees] is.
    pub info: MarketInfo,
    pub fees_enabled: Option<bool>,
    pub fees: FeeParams,
}

/// A market that left the universe (after its grace period), or whose tokens changed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemovedMarket {
    pub condition_id: ConditionId,
    pub tokens: MarketTokens,
}

/// A tracked market whose details or fee curve changed on this poll.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MarketChanged {
    pub market: LiveMarket,
    pub details_changed: bool,
    /// The (rate, exponent) differs from the last one seen (D13).
    pub fee_changed: bool,
}

/// One Data API taker trade, as needed for rolling volume (§3.6).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackfillPrint {
    pub token: TokenId,
    pub side: TakerSide,
    pub price: Price,
    pub size: Shares,
    pub tx_hash: TxHash,
    /// Exchange time, to the second (the Data API's resolution).
    #[serde(with = "micros")]
    pub timestamp: DateTime<Utc>,
}
