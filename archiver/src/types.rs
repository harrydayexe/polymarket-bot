//! Custom types for interacting with the PolyMarket API and state machines.
//! Defined here so that if the type changes, its all in one place.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::{collections::HashSet, str::FromStr};
use tokio::time::Instant;

/// A type identifying tokens. This is a very long decimal number but is stored
/// as a string as it overflows 64-bit integers.
#[derive(Debug, Clone, Serialize, Deserialize, Hash, PartialEq, Eq)]
pub struct TokenId(pub String);
impl From<String> for TokenId {
    fn from(value: String) -> Self {
        TokenId(value)
    }
}

/// A type containing a hexadecimal market identifier.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, Default)]
pub struct ConditionId(pub String);
impl From<String> for ConditionId {
    fn from(value: String) -> Self {
        ConditionId(value)
    }
}

/// A type containing a per-run identifer, created once per process start.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunId(pub String);

/// A type identifying a connection slot that is stable across re-connects.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnSlotId(pub String);

/// A type identifying attempts to a particular connection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnAttemptId(pub String);

/// A type containing a price or size exactly as the exchange sent it.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DecimalString(pub String);
impl DecimalString {
    /// Parse the decimal string or return an error
    pub fn as_decimal(&self) -> Result<Decimal, rust_decimal::Error> {
        Decimal::from_str(&self.0)
    }
}
impl From<Decimal> for DecimalString {
    fn from(value: Decimal) -> Self {
        DecimalString(value.to_string())
    }
}
impl From<String> for DecimalString {
    fn from(value: String) -> Self {
        Self(value)
    }
}
impl From<f64> for DecimalString {
    fn from(value: f64) -> Self {
        Self(value.to_string())
    }
}

/// Wall-clock time, in microseconds since the UNIX epoch. Used when writing to
/// disk.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct UtcMicros(pub i64);
impl From<DateTime<Utc>> for UtcMicros {
    fn from(value: DateTime<Utc>) -> Self {
        Self(value.timestamp_micros())
    }
}

/// A monotonic clock time, used only for timers and durations. Should never be
/// written to disk.
pub type MonoTime = Instant;

/// The timestamps received exactly as they are from the exchange.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExchangeMillis(pub String);

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
