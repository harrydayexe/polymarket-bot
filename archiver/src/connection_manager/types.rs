use std::{collections::HashSet, fmt::Display};

use chrono::{DateTime, Utc};
use marcasite::types::TokenId;
use tokio::time::Instant;

use crate::messages::{ControlBody, GapReason, SnapshotReason};

marcasite_core::string_id! {
    /// Identifier for a connection slot, stable across reconnects.
    pub struct ConnSlotId;
}

marcasite_core::string_id! {
    /// Slot + attempt number, new for each connection attempt.
    pub struct ConnAttemptId;
}

impl ConnAttemptId {
    pub fn create(slot: ConnSlotId, seq: u64) -> Self {
        ConnAttemptId::new(format!("{}-{}", slot, seq))
    }
}

#[derive(Debug, PartialEq)]
pub enum ConnectionState {
    Connecting {
        started_at: Instant,
    },
    Subscribing {
        since: Instant,
    },
    Streaming {
        since: Instant,
        next_ping_at: Instant,
        pong_deadline: Instant,
    },
    Backoff {
        until: Instant,
    },
    Closed,
}

#[derive(Debug)]
pub enum ConnectionEvent {
    SocketOpened,
    /// The socket could not connect.
    SocketFailed {
        error: String,
    },
    SocketClosed {
        reason: String,
    },
    SendFailed {
        error: String,
    },
    PongReceived {
        at: Instant,
    },
    /// Regular timer tick (e.g. every 100ms).
    Tick,
    TokensAssigned {
        tokens: HashSet<TokenId>,
    },
    SubscribeSent,
    TokensRemoved {
        tokens: HashSet<TokenId>,
    },
    /// This connection's frames were dropped
    QueueOverflow,
    Shutdown,
}

impl Display for ConnectionEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnectionEvent::SocketOpened => write!(f, "SocketOpened"),
            ConnectionEvent::SocketFailed { error: _ } => write!(f, "SocketFailed"),
            ConnectionEvent::SocketClosed { reason: _ } => write!(f, "SocketClosed"),
            ConnectionEvent::SendFailed { error: _ } => write!(f, "SendFailed"),
            ConnectionEvent::PongReceived { at: _ } => write!(f, "PongReceived"),
            ConnectionEvent::Tick => write!(f, "Tick"),
            ConnectionEvent::TokensAssigned { tokens: _ } => write!(f, "TokensAssigned"),
            ConnectionEvent::SubscribeSent => write!(f, "SubscribeSent"),
            ConnectionEvent::TokensRemoved { tokens: _ } => write!(f, "TokensRemoved"),
            ConnectionEvent::QueueOverflow => write!(f, "QueueOverflow"),
            ConnectionEvent::Shutdown => write!(f, "Shutdown"),
        }
    }
}

#[derive(Debug)]
pub enum ConnectionAction {
    OpenSocket {
        url: url::Url,
        attempt_id: ConnAttemptId,
    },
    CloseSocket,
    SendSubscribe {
        tokens: HashSet<TokenId>,
    },
    SendUnsubscribe {
        tokens: HashSet<TokenId>,
    },
    SendPing,
    RequestSnapshot {
        tokens: HashSet<TokenId>,
        reason: SnapshotReason,
    },
    OpenGaps {
        tokens: HashSet<TokenId>,
        start: DateTime<Utc>,
        reason: GapReason,
    },
    CloseGaps {
        tokens: HashSet<TokenId>,
        end: DateTime<Utc>,
        reason: GapReason,
    },
    Emit {
        body: ControlBody,
    },
}
