use std::{collections::HashSet, fmt::Display, time::Duration};

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

/// Differs from the design doc because marcasite's market channel does part of the work:
///
/// - No `Subscribing` state: the subscription is sent as part of connecting, so a socket that
///   opens is already subscribed.
/// - No ping schedule or pong deadline: the channel sends `PING` and ends the stream when
///   nothing arrives within the heartbeat timeout.
/// - No connect timeout: the channel bounds the handshake and the subscription send.
/// - No `Closing` state: closing is a single step (the manager carries out `CloseSocket`), so a
///   connection goes straight to `Closed`, which is terminal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    Connecting,
    Streaming { since: Instant },
    Backoff { until: Instant },
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
    /// Regular timer tick (e.g. every 100ms). Ends the backoff wait.
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
    /// The socket opened and its subscription was sent.
    Opened,
    /// The socket could not connect.
    Failed {
        error: String,
        /// How long the server asked us to wait, if it refused the handshake with one.
        retry_after: Option<Duration>,
    },
    /// The socket was closed, by the server or by an error.
    Closed { reason: String },
    /// Nothing arrived within the heartbeat timeout, or a write stalled: the connection is
    /// dead even if the socket looks open.
    TimedOut { error: String },
    /// A live subscribe or unsubscribe could not be sent.
    SendFailed { error: String },
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
            SocketEvent::TimedOut { .. } => write!(f, "SocketTimedOut"),
            SocketEvent::SendFailed { .. } => write!(f, "SendFailed"),
            SocketEvent::QueueOverflow => write!(f, "QueueOverflow"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectionAction {
    /// Open a socket subscribed to `tokens`.
    OpenSocket {
        url: url::Url,
        attempt_id: ConnAttemptId,
        tokens: HashSet<TokenId>,
    },
    CloseSocket,
    /// Subscribe more tokens on the open socket.
    SendSubscribe {
        tokens: HashSet<TokenId>,
    },
    SendUnsubscribe {
        tokens: HashSet<TokenId>,
    },
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
