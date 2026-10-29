use std::sync::Arc;

use chrono::{DateTime, Utc};
use marcasite::types::TokenId;
use tokio::sync::watch;

use crate::connection_manager::types::ConnSlotId;

/// What the connection manager publishes for the status file, refreshed on every tick.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ManagerSummary {
    pub connections: Vec<ConnectionSummary>,
    pub tokens_tracked: usize,
    /// Tokens with a gap open.
    pub open_gaps: usize,
    /// Tokens that got no full book, from the websocket or a REST snapshot, after subscribing.
    pub tokens_unhealthy: Vec<UnhealthyToken>,
    /// Websocket messages dropped because the write queue was full, since the start.
    pub frames_dropped: u64,
    /// Receive time of the latest message on any connection.
    pub last_message_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ConnectionSummary {
    pub slot: ConnSlotId,
    /// `connecting`, `streaming` or `backoff`.
    pub state: &'static str,
    pub tokens: usize,
    /// Since when the connection has been streaming, or not streaming (connecting and backoff
    /// count as one), whichever it is now.
    pub since: DateTime<Utc>,
    /// Receive time of the latest message on the current attempt.
    pub last_message_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct UnhealthyToken {
    pub token: TokenId,
    /// When it was subscribed without a book arriving.
    pub since: DateTime<Utc>,
}

/// The sending side of what the connection manager publishes.
pub struct ManagerPublisher {
    pub summary: watch::Sender<ManagerSummary>,
    /// Every tracked token, sorted. Changes only when the tracked set does.
    pub tracked_tokens: watch::Sender<Arc<Vec<TokenId>>>,
}

/// The receiving side of [ManagerPublisher].
#[derive(Clone)]
pub struct ManagerWatch {
    pub summary: watch::Receiver<ManagerSummary>,
    pub tracked_tokens: watch::Receiver<Arc<Vec<TokenId>>>,
}

pub fn manager_channels() -> (ManagerPublisher, ManagerWatch) {
    let (summary_tx, summary) = watch::channel(ManagerSummary::default());
    let (tracked_tx, tracked_tokens) = watch::channel(Arc::new(vec![]));
    (
        ManagerPublisher {
            summary: summary_tx,
            tracked_tokens: tracked_tx,
        },
        ManagerWatch {
            summary,
            tracked_tokens,
        },
    )
}
