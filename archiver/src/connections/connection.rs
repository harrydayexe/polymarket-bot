use std::collections::HashSet;

use crate::types::{
    ConnAttemptId, ConnSlotId, ControlBody, GapReason, MonoTime, SnapshotReason, TokenId, UtcMicros,
};

#[derive(Debug)]
pub struct Connection {
    slot_id: ConnSlotId,
    /// Absent before the first attempt.
    attempt_id: Option<ConnAttemptId>,
    /// Drives backoff, reset after stable streaming.
    attempt_count: i64,
    state: ConnState,
    /// Which tokens we **want** to subscribe to.
    assigned_tokens: HashSet<TokenId>,
    /// Which tokens we **are** currently subscribed to.
    subscribed_tokens: HashSet<TokenId>,
    /// Written by the socket reader only.
    last_message_at: Option<UtcMicros>,
    last_pong_at: Option<MonoTime>,
    last_ping_at: Option<MonoTime>,
}

#[derive(Debug, Clone)]
enum ConnState {
    Connecting { started_at: MonoTime },
    Subscribing,
    Streaming { since: MonoTime },
    Backoff { until: MonoTime },
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
        tokens: HashSet<TokenId>,
        custom_features: bool,
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
        start: UtcMicros,
        reason: GapReason,
    },
    CloseGaps {
        tokens: HashSet<TokenId>,
        end: UtcMicros,
        reason: GapReason,
    },
    Emit {
        record: ControlBody,
    },
}
