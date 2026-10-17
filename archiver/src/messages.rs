use std::{collections::HashSet, fmt::Display};

use chrono::{DateTime, Utc};
use marcasite::{data::TokenId, types::ConditionId};

use crate::{
    connection_manager::types::{ConnAttemptId, ConnSlotId},
    market_registry::{registry::PlannedChanges, token_changes::TokenChanges},
    types::RunId,
};

#[derive(Debug)]
pub enum RegistryMsg {
    MarketResolved(ConditionId),
    NewMarketSeen,
}

#[derive(Debug)]
pub enum ConnectionManagerMsg {
    TokenChanges(TokenChanges),
}

#[derive(Debug)]
pub enum Record {
    Market {
        recv_ts: DateTime<Utc>,
        changes: PlannedChanges,
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
        attempts: u32,
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
