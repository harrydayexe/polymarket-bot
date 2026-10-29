use std::{collections::HashSet, fmt::Display};

use chrono::{DateTime, Utc, serde::ts_microseconds};
use marcasite::{
    types::{ConditionId, TokenId},
    ws::MarketEvent,
};
use serde::{Deserialize, Serialize, Serializer};

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

#[derive(Debug)]
pub enum Record {
    /// One market channel message, as decoded by marcasite. A frame holding an array is one
    /// record per element; a message marcasite does not recognise is kept as raw JSON
    /// (`MarketEvent::Unknown`).
    WsFrame {
        recv_ts: DateTime<Utc>,
        conn_id: ConnAttemptId,
        event: MarketEvent,
    },
    /// One `POST /books` response. `books` has a book for each requested token the exchange
    /// knows, each exactly as sent (see [crate::archive::snapshot_book] to read one).
    Snapshot {
        recv_ts: DateTime<Utc>,
        requested_tokens: Vec<TokenId>,
        reason: SnapshotReason,
        books: Vec<serde_json::Value>,
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

/// The body of a control record. Serialised with a `kind` field naming the variant
/// (`gap_open`, `run_stop`, ...), times as microseconds since the epoch and token sets as
/// sorted lists.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
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
        #[serde(serialize_with = "sorted")]
        tokens: HashSet<TokenId>,
    },
    Unsubscribe {
        slot: ConnSlotId,
        #[serde(serialize_with = "sorted")]
        tokens: HashSet<TokenId>,
    },
    GapOpen {
        #[serde(serialize_with = "sorted")]
        tokens: HashSet<TokenId>,
        #[serde(with = "ts_microseconds")]
        start: DateTime<Utc>,
        #[serde(flatten)]
        reason: GapReason,
    },
    GapClose {
        #[serde(serialize_with = "sorted")]
        tokens: HashSet<TokenId>,
        #[serde(with = "ts_microseconds")]
        end: DateTime<Utc>,
        #[serde(flatten)]
        reason: GapReason,
    },
    SnapshotFailed {
        #[serde(serialize_with = "sorted")]
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
        #[serde(with = "ts_microseconds")]
        since: DateTime<Utc>,
    },
    /// A recognised message that did not match its documented schema, so it is not in the
    /// archive. `error` quotes the start of it.
    FrameDecodeFailed {
        error: String,
    },
}

/// Serialises a token set as a sorted list, so the same set always gives the same line.
fn sorted<S: Serializer>(tokens: &HashSet<TokenId>, serializer: S) -> Result<S::Ok, S::Error> {
    let mut ids: Vec<&TokenId> = tokens.iter().collect();
    ids.sort_unstable();
    serializer.collect_seq(ids)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotReason {
    Subscribe,
    NewToken,
    Scheduled,
    Manual,
    MissingBook,
}

/// Why a gap opened or closed. Serialised as a `reason` code (`connect_failed`, `resumed`,
/// ...) plus a `detail` with the error for the two reasons that carry one.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(into = "GapReasonRepr", try_from = "GapReasonRepr")]
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

impl GapReason {
    /// The reason's code as written to disk.
    pub fn code(&self) -> &'static str {
        match self {
            GapReason::ConnectFailed { .. } => "connect_failed",
            GapReason::ConnectionClosed { .. } => "connection_closed",
            GapReason::HeartbeatTimeout => "heartbeat_timeout",
            GapReason::QueueOverflow => "queue_overflow",
            GapReason::ProcessRestart => "process_restart",
            GapReason::WriteFailure => "write_failure",
            GapReason::Shutdown => "shutdown",
            GapReason::Unsubscribed => "unsubscribed",
            GapReason::Resumed => "resumed",
        }
    }
}

#[derive(Serialize, Deserialize)]
struct GapReasonRepr {
    reason: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    detail: Option<String>,
}

impl From<GapReason> for GapReasonRepr {
    fn from(reason: GapReason) -> Self {
        let code = reason.code().to_string();
        let detail = match reason {
            GapReason::ConnectFailed { error } | GapReason::ConnectionClosed { error } => {
                Some(error)
            }
            _ => None,
        };
        GapReasonRepr {
            reason: code,
            detail,
        }
    }
}

impl TryFrom<GapReasonRepr> for GapReason {
    type Error = String;

    fn try_from(repr: GapReasonRepr) -> Result<Self, Self::Error> {
        let error = || repr.detail.clone().unwrap_or_default();
        Ok(match repr.reason.as_str() {
            "connect_failed" => GapReason::ConnectFailed { error: error() },
            "connection_closed" => GapReason::ConnectionClosed { error: error() },
            "heartbeat_timeout" => GapReason::HeartbeatTimeout,
            "queue_overflow" => GapReason::QueueOverflow,
            "process_restart" => GapReason::ProcessRestart,
            "write_failure" => GapReason::WriteFailure,
            "shutdown" => GapReason::Shutdown,
            "unsubscribed" => GapReason::Unsubscribed,
            "resumed" => GapReason::Resumed,
            other => return Err(format!("unknown gap reason {other:?}")),
        })
    }
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
