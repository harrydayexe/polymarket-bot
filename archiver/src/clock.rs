use std::sync::Arc;

#[cfg(test)]
use chrono::Duration;
use chrono::{DateTime, Utc};
#[cfg(test)]
use tokio::time::Instant;

pub trait Clock: Send + Sync + 'static {
    fn now(&self) -> DateTime<Utc>;
}

pub type SharedClock = Arc<dyn Clock>;

/// Production clock: real wall time.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

/// Test clock: wall time derived from tokio's (pausable) Instant.
/// With `tokio::time::pause()`, `advance()` and sleeps move this clock too.
#[cfg(test)]
pub struct TokioClock {
    start_wall: DateTime<Utc>,
    start_instant: Instant,
}

#[cfg(test)]
impl TokioClock {
    /// Must be created inside a tokio runtime.
    pub fn new(start_wall: DateTime<Utc>) -> Self {
        Self {
            start_wall,
            start_instant: Instant::now(),
        }
    }
}

#[cfg(test)]
impl Clock for TokioClock {
    fn now(&self) -> DateTime<Utc> {
        let elapsed = self.start_instant.elapsed();
        self.start_wall + Duration::from_std(elapsed).expect("elapsed overflow")
    }
}
