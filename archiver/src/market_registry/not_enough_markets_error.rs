use thiserror::Error;

#[derive(Error, Debug)]
#[error(
    "too few qualifying markets: {fetched_count}, need at least {threshold} \
     ({last_accepted_count} last accepted)"
)]
pub struct NotEnoughMarketsError {
    pub(super) fetched_count: u64,
    pub(super) last_accepted_count: u64,
    pub(super) threshold: u64,
}
