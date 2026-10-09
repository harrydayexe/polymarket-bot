use thiserror::Error;

#[derive(Error, Debug)]
#[error("fetched too few records: {fetched_count} ({last_accepted_count} last fetch)")]
pub struct NotEnoughMarketsError {
    pub(super) fetched_count: u64,
    pub(super) last_accepted_count: u64,
    pub(super) threshold: u64,
}

