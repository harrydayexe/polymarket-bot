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

/// There is no `Closing` state as in the design doc: closing is a single step (the manager
/// carries out `CloseSocket`), so a connection goes straight to `Closed`, which is terminal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectionEvent {
    /// Something reported by the socket tasks of one connection attempt. Tagged with the
    /// attempt so events from an earlier, already abandoned socket can be told apart from
    /// the current one.
    Socket {
        attempt_id: ConnAttemptId,
        event: SocketEvent,
    },
    /// Regular timer tick (e.g. every 100ms).
    Tick {
        /// Latest value of the socket reader's last-message time, copied in by the manager.
        last_message_at: Option<DateTime<Utc>>,
    },
    TokensAssigned {
        tokens: HashSet<TokenId>,
    },
    TokensRemoved {
        tokens: HashSet<TokenId>,
    },
    Shutdown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SocketEvent {
    Opened,
    /// The socket could not connect.
    Failed {
        error: String,
    },
    Closed {
        reason: String,
    },
    SendFailed {
        error: String,
    },
    SubscribeSent,
    PongReceived {
        at: Instant,
    },
    /// This connection's frames were dropped
    QueueOverflow,
}

impl Display for ConnectionEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnectionEvent::Socket { attempt_id, event } => write!(f, "{event} ({attempt_id})"),
            ConnectionEvent::Tick { .. } => write!(f, "Tick"),
            ConnectionEvent::TokensAssigned { .. } => write!(f, "TokensAssigned"),
            ConnectionEvent::TokensRemoved { .. } => write!(f, "TokensRemoved"),
            ConnectionEvent::Shutdown => write!(f, "Shutdown"),
        }
    }
}

impl Display for SocketEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SocketEvent::Opened => write!(f, "SocketOpened"),
            SocketEvent::Failed { .. } => write!(f, "SocketFailed"),
            SocketEvent::Closed { .. } => write!(f, "SocketClosed"),
            SocketEvent::SendFailed { .. } => write!(f, "SendFailed"),
            SocketEvent::SubscribeSent => write!(f, "SubscribeSent"),
            SocketEvent::PongReceived { .. } => write!(f, "PongReceived"),
            SocketEvent::QueueOverflow => write!(f, "QueueOverflow"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
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
