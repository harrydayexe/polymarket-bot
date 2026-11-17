use std::collections::HashSet;

use chrono::{DateTime, Utc};
use marcasite::types::{ConditionId, TokenId};

use crate::market_registry::token_changes::TokenChanges;

pub use pm_core::archive::record::{ControlBody, GapReason, Record, SnapshotReason};

#[derive(Debug)]
pub enum RegistryMsg {
    /// A `market_resolved` frame, received at `at`.
    MarketResolved {
        condition_id: ConditionId,
        at: DateTime<Utc>,
    },
    /// A `new_market` frame. Triggers an early refresh.
    NewMarketSeen { condition_id: ConditionId },
}

#[derive(Debug)]
pub enum ConnectionManagerMsg {
    TokenChanges(TokenChanges),
    /// Full books for these tokens were written (websocket `book` messages or REST snapshots).
    BooksSeen {
        tokens: HashSet<TokenId>,
    },
}

/// Messages to the snapshot worker.
#[derive(Debug, PartialEq, Eq)]
pub enum SnapshotMsg {
    /// Take a REST snapshot of `tokens`.
    SnapshotRequest {
        tokens: HashSet<TokenId>,
        reason: SnapshotReason,
    },
    /// The tracked tokens changed. Keeps the scheduled snapshots current.
    TokensTracked {
        add: HashSet<TokenId>,
        remove: HashSet<TokenId>,
    },
}
