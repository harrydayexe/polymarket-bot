//! What the engine asks the I/O side to do ([Action]) and the rows it stores ([Record]).

use std::collections::BTreeMap;

use marcasite::types::{ConditionId, TokenId};
use pm_core::{
    archive::record::ConnSlotId,
    units::{FeeParams, UtcMicros, micros},
};
use serde::{Deserialize, Serialize};

use crate::event::LiveMarket;

/// An effect requested by [crate::engine::Engine::step]. Executed in order.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Action {
    /// Start a new connection slot subscribed to `tokens`.
    OpenSlot {
        slot: ConnSlotId,
        tokens: Vec<TokenId>,
    },
    /// Add tokens to a slot's subscription (applied on reconnect if it is down).
    Subscribe {
        slot: ConnSlotId,
        tokens: Vec<TokenId>,
    },
    /// Remove tokens from a slot's subscription.
    Unsubscribe {
        slot: ConnSlotId,
        tokens: Vec<TokenId>,
    },
    /// Close a slot that has no tokens left.
    CloseSlot { slot: ConnSlotId },
    /// Unsubscribe and resubscribe `tokens` to force fresh `book` snapshots (§3.4).
    Resnapshot {
        slot: ConnSlotId,
        tokens: Vec<TokenId>,
    },
    /// Fetch Data API taker prints of `conditions` in `[from, until)`.
    RequestBackfill {
        conditions: Vec<ConditionId>,
        #[serde(with = "micros")]
        from: UtcMicros,
        #[serde(with = "micros")]
        until: UtcMicros,
    },
    /// Tell the registry task something it should act on.
    RegistryHint(RegistryHint),
    /// Store a row.
    Persist(Record),
}

/// Websocket news the registry task acts on.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RegistryHint {
    /// A `new_market` arrived: poll Gamma early.
    NewMarket,
    /// A market resolved on the websocket: start its grace period.
    Resolved {
        condition_id: ConditionId,
        #[serde(with = "micros")]
        at: UtcMicros,
    },
}

/// A stored row. These are the rows C8 puts in Postgres; until then they go to a JSONL file.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "table", rename_all = "snake_case")]
pub enum Record {
    /// `markets`: a market as first seen or after its details changed.
    Market {
        #[serde(with = "micros")]
        seen_at: UtcMicros,
        market: Box<LiveMarket>,
    },
    /// `market_fees`: the fee parameters of a market from `fees.seen_at`, one row per change.
    MarketFees {
        condition_id: ConditionId,
        fees: FeeParams,
        /// The exponent is not one live markets have been seen to send (§3.7).
        fee_exponent_unverified: bool,
    },
    /// `resolutions`.
    Resolution {
        condition_id: ConditionId,
        winning_token: TokenId,
        #[serde(with = "micros")]
        resolved_at: UtcMicros,
        source: String,
    },
    /// `metrics`.
    Metric(Metric),
}

/// One measurement: `metrics(name, at, value, labels)`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Metric {
    pub name: String,
    #[serde(with = "micros")]
    pub at: UtcMicros,
    pub value: f64,
    pub labels: BTreeMap<String, String>,
}

impl Metric {
    pub fn new(name: &str, at: UtcMicros, value: f64) -> Self {
        Self {
            name: name.into(),
            at,
            value,
            labels: BTreeMap::new(),
        }
    }

    pub fn label(mut self, key: &str, value: impl Into<String>) -> Self {
        self.labels.insert(key.into(), value.into());
        self
    }
}
