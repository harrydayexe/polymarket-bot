use std::path::Path;

use anyhow::{Context, Result};
use chrono::{
    DateTime, Utc,
    serde::{ts_microseconds, ts_microseconds_option},
};
use marcasite::types::TokenId;
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::{connection_manager::types::ConnSlotId, types::RunId};

/// `state/status.json`, rewritten every few seconds by `archiver run`. Times are
/// microseconds since the epoch.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StatusFile {
    pub run_id: RunId,
    pub program_version: String,
    #[serde(with = "ts_microseconds")]
    pub started_at: DateTime<Utc>,
    #[serde(with = "ts_microseconds")]
    pub written_at: DateTime<Utc>,
    /// Set by the last write of a run that stopped cleanly.
    pub clean_stop: bool,
    /// Latest message received on any connection.
    #[serde(default, with = "ts_microseconds_option")]
    pub last_message_at: Option<DateTime<Utc>>,
    /// Latest websocket record known to be synced to disk.
    #[serde(default, with = "ts_microseconds_option")]
    pub durable_through: Option<DateTime<Utc>>,
    pub tokens_tracked: usize,
    pub connections: Vec<ConnectionStatus>,
    pub messages_last_minute: u64,
    pub queue: QueueStatus,
    pub open_gaps: usize,
    pub tokens_unhealthy: Vec<UnhealthyTokenStatus>,
    pub snapshots_last_hour: u64,
    /// Free space on the archive's disk, if it could be read.
    pub disk_free_gb: Option<f64>,
    pub current_files: Vec<String>,
    pub previously_tracked_tokens_file: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConnectionStatus {
    pub slot: ConnSlotId,
    /// `connecting`, `streaming` or `backoff`.
    pub state: String,
    pub tokens: usize,
    /// Since when it has been streaming, or not streaming, whichever it is now.
    #[serde(with = "ts_microseconds")]
    pub since: DateTime<Utc>,
    #[serde(default, with = "ts_microseconds_option")]
    pub last_message_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueueStatus {
    pub depth: usize,
    pub high_water: usize,
    pub capacity: usize,
    pub dropped_total: u64,
    pub dropped_last_hour: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UnhealthyTokenStatus {
    pub token: TokenId,
    #[serde(with = "ts_microseconds")]
    pub since: DateTime<Utc>,
}

/// `state/tracked_tokens.json`: the tokens tracked by a run, rewritten whenever they change.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TrackedTokensFile {
    pub run_id: RunId,
    #[serde(with = "ts_microseconds")]
    pub written_at: DateTime<Utc>,
    pub tokens: Vec<TokenId>,
}

/// Writes `value` as JSON to `path` through a temporary file and a rename, so a reader never
/// sees a half-written file.
pub fn write_json_atomic(path: &Path, value: &impl Serialize) -> Result<()> {
    let tmp = path.with_extension("json.tmp");
    let text = serde_json::to_vec_pretty(value).context("could not serialise")?;
    std::fs::write(&tmp, text).with_context(|| format!("could not write {}", tmp.display()))?;
    std::fs::rename(&tmp, path)
        .with_context(|| format!("could not rename {} into place", tmp.display()))
}

/// Reads a JSON file. `Ok(None)` if it does not exist.
pub fn read_json<T: DeserializeOwned>(path: &Path) -> Result<Option<T>> {
    let text = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e).with_context(|| format!("could not read {}", path.display())),
    };
    serde_json::from_str(&text)
        .map(Some)
        .with_context(|| format!("{} is not valid", path.display()))
}
