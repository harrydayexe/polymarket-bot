use crate::types::{ConnAttemptId, RunId};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Envelope wraps unparsed data from the API with some metadata.
#[derive(Debug, Serialize, Deserialize)]
pub struct Envelope {
    /// When the frame was received, from the system clock.
    recv_ts: DateTime<Utc>,

    /// A counter that increases by one for every record written.
    seq: i64,

    /// Unique ID created at process start.
    run_id: RunId,

    /// Unique ID of the connection attempt that received the frame (empty for
    /// snapshots).
    conn_id: Option<ConnAttemptId>,

    /// Where the record came from.
    source: Source,

    /// The frame's exact text, unparsed.
    raw: String,
}

/// A type defining the different sources of records from the API.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
enum Source {
    /// Websocket
    Ws,
    Snapshot,
    Control,
}
