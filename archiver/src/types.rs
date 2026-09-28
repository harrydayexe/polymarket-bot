//! Custom types for interacting with the PolyMarket API and state machines.
//! Defined here so that if the type changes, its all in one place.

use serde::{Deserialize, Serialize};
use std::{collections::HashSet, time::Instant};

/// A type identifying tokens. This is a very long decimal number but is stored
/// as a string as it overflows 64-bit integers.
#[derive(Debug, Clone)]
pub struct TokenId(pub String);

/// A type containing a hexadecimal market identifier.
#[derive(Debug, Clone)]
pub struct ConditionId(pub String);

/// A type containing a per-run identifer, created once per process start.
#[derive(Debug, Clone)]
pub struct RunId(pub String);

/// A type identifying a connection slot that is stable across re-connects.
#[derive(Debug, Clone)]
pub struct ConnSlotId(pub String);

/// A type identifying attempts to a particular connection.
#[derive(Debug, Clone)]
pub struct ConnAttemptId(pub String);

/// A type containing a price or size exactly as the exchange sent it.
#[derive(Debug, Clone)]
pub struct DecimalString(pub String);

/// Wall-clock time, in microseconds since the UNIX epoch. Used when writing to
/// disk.
#[derive(Debug, Clone, PartialEq)]
pub struct UtcMicros(pub i64);

/// A monotonic clock time, used only for timers and durations. Should never be
/// written to disk.
#[derive(Debug, Clone)]
pub struct MonoTime(pub Instant);

/// The timestamps received exactly as they are from the exchange.
#[derive(Debug, Clone)]
pub struct ExchangeMillis(pub String);

/// A type containing a per-process record counter.
#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
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
        tokens: HashSet<TokenId>,
    },
    Unsubscribe {
        slot: ConnSlotId,
        tokens: HashSet<TokenId>,
    },
    GapOpen {
        tokens: HashSet<TokenId>,
        start: UtcMicros,
        reason: GapReason,
    },
    GapClose {
        tokens: HashSet<TokenId>,
        end: UtcMicros,
        reason: GapReason,
    },
    SnapshotFailed {
        tokens: HashSet<TokenId>,
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
        token: TokenId,
        since: UtcMicros,
    },
}
