use std::{collections::HashSet, fmt::Display};

use chrono::{DateTime, Utc};
use marcasite::types::{ConditionId, TokenId};

use crate::{
    connection_manager::types::{ConnAttemptId, ConnSlotId},
    market_registry::{
        market_info::MarketInfo, registry::MarketChange, token_changes::TokenChanges,
    },
    types::RunId,
};

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
}

/// Messages to the snapshot worker.
#[derive(Debug, PartialEq, Eq)]
pub enum SnapshotMsg {
    /// Take a REST snapshot of `tokens`.
    SnapshotRequest {
        tokens: HashSet<TokenId>,
        reason: SnapshotReason,
    },
}

#[derive(Debug)]
pub enum Record {
    /// One websocket text frame, exactly as received.
    WsFrame {
        recv_ts: DateTime<Utc>,
        conn_id: ConnAttemptId,
        raw: String,
    },
    Market {
        recv_ts: DateTime<Utc>,
        market: MarketInfo,
        change: MarketChange,
    },
    Control {
        recv_ts: DateTime<Utc>,
        conn_id: Option<ConnAttemptId>,
        body: ControlBody,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlBody {
    RunStart {
        program_version: String,
        config_hash: String,
        previous_run: Option<RunId>,
    },
    RunStop {
        clean: bool,
    },
    Connect {
        slot: ConnSlotId,
        token_count: usize,
    },
    Disconnect {
        slot: ConnSlotId,
        reason: String,
    },
    Subscribe {
        slot: ConnSlotId,
        tokens: HashSet<TokenId>,
    },
    Unsubscribe {
        slot: ConnSlotId,
        tokens: HashSet<TokenId>,
    },
    GapOpen {
        tokens: HashSet<TokenId>,
        start: DateTime<Utc>,
        reason: GapReason,
    },
    GapClose {
        tokens: HashSet<TokenId>,
        end: DateTime<Utc>,
        reason: GapReason,
    },
    SnapshotFailed {
        tokens: HashSet<TokenId>,
        error: String,
        attempts: u32,
    },
    QueueOverflow {
        slot: ConnSlotId,
        dropped: u64,
    },
    RegistryRejected {
        reason: String,
        fetched: u64,
        previous: u64,
    },
    TokenUnhealthy {
        token: TokenId,
        since: DateTime<Utc>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SnapshotReason {
    Subscribe,
    NewToken,
    Scheduled,
    Manual,
    MissingBook,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GapReason {
    ConnectFailed { error: String },
    ConnectionClosed { error: String },
    HeartbeatTimeout,
    QueueOverflow,
    ProcessRestart,
    WriteFailure,
    Shutdown,
    Unsubscribed,
    Resumed,
}

impl Display for GapReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GapReason::ConnectFailed { error } => {
                write!(f, "connection failed due to error: {error}")
            }
            GapReason::ConnectionClosed { error } => {
                write!(f, "connection closed due to error: {error}")
            }
            GapReason::HeartbeatTimeout => write!(f, "heartbeat timed out"),
            GapReason::QueueOverflow => write!(f, "queue overflowed"),
            GapReason::ProcessRestart => write!(f, "process restarted"),
            GapReason::WriteFailure => write!(f, "write failed"),
            GapReason::Shutdown => write!(f, "shutdown"),
            GapReason::Unsubscribed => write!(f, "unsubscribed"),
            GapReason::Resumed => write!(f, "resumed"),
        }
    }
}
