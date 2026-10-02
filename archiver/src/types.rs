//! Custom types for interacting with the PolyMarket API and state machines.
//! Defined here so that if the type changes, its all in one place.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// A type containing a per-run identifer, created once per process start.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunId(pub String);

/// A type identifying a connection slot that is stable across re-connects.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnSlotId(pub String);

/// A type identifying attempts to a particular connection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnAttemptId(pub String);

/// A type containing a per-process record counter.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Seq(pub i64);

/// A type giving the reason for a snapshot request
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SnapshotReason {
    Subscribe,
    NewToken,
    Scheduled,
    Manual,
    MissingBook,
}

/// A type giving the reason for a gap in history
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum GapReason {
    ConnectFailed,
    ConnectionClosed,
    HeartbeatTimeout,
    QueueOverflow,
    ProcessRestart,
    WriteFailure,
    Shutdown,
    Unsubscribed,
    Resumed,
}

#[derive(Debug)]
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
        token_count: i64,
    },
    Disconnect {
        slot: ConnSlotId,
        reason: String,
    },
    Subscribe {
        slot: ConnSlotId,
        // tokens: HashSet<TokenId>,
    },
    Unsubscribe {
        slot: ConnSlotId,
        // tokens: HashSet<TokenId>,
    },
    GapOpen {
        // tokens: HashSet<TokenId>,
        start: DateTime<Utc>,
        reason: GapReason,
    },
    GapClose {
        // tokens: HashSet<TokenId>,
        end: DateTime<Utc>,
        reason: GapReason,
    },
    SnapshotFailed {
        // tokens: HashSet<TokenId>,
        error: String,
        attempts: i64,
    },
    QueueOverflow {
        slot: ConnSlotId,
        dropped: i64,
    },
    RegistryRejected {
        reason: String,
        fetched: i64,
        previous: i64,
    },
    TokenUnhealthy {
        // token: TokenId,
        since: DateTime<Utc>,
    },
}
