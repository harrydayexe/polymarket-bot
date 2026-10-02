use chrono::{DateTime, Utc};
use tokio::time::Instant;

use crate::types::{ConnAttemptId, ConnSlotId, ControlBody, GapReason, SnapshotReason};

#[derive(Debug)]
pub struct Connection {
    slot_id: ConnSlotId,
    /// Absent before the first attempt.
    attempt_id: Option<ConnAttemptId>,
    /// Drives backoff, reset after stable streaming.
    attempt_count: i64,
    state: ConnState,
    /// Which tokens we **want** to subscribe to.
    // assigned_tokens: HashSet<TokenId>,
    /// Which tokens we **are** currently subscribed to.
    // subscribed_tokens: HashSet<TokenId>,
    /// Written by the socket reader only.
    last_message_at: Option<DateTime<Utc>>,
    last_pong_at: Option<Instant>,
    last_ping_at: Option<Instant>,
}

#[derive(Debug, Clone)]
enum ConnState {
    Connecting { started_at: Instant },
    Subscribing,
    Streaming { since: Instant },
    Backoff { until: Instant },
    Closing,
    Closed,
}

#[derive(Debug)]
enum ConnAction {
    OpenSocket {
        url: String,
        attempt_id: ConnAttemptId,
    },
    CloseSocket,
    SendSubscribe {
        // tokens: HashSet<TokenId>,
        custom_features: bool,
    },
    SendUnsubscribe {
        // tokens: HashSet<TokenId>,
    },
    SendPing,
    RequestSnapshot {
        // tokens: HashSet<TokenId>,
        reason: SnapshotReason,
    },
    OpenGaps {
        // tokens: HashSet<TokenId>,
        start: DateTime<Utc>,
        reason: GapReason,
    },
    CloseGaps {
        // tokens: HashSet<TokenId>,
        end: DateTime<Utc>,
        reason: GapReason,
    },
    Emit {
        record: ControlBody,
    },
}
