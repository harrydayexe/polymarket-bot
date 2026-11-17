//! Injected wall-clock time.

use std::sync::Arc;

#[cfg(any(test, feature = "test-util"))]
use chrono::Duration;
use chrono::{DateTime, Utc};
#[cfg(any(test, feature = "test-util"))]
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
#[cfg(any(test, feature = "test-util"))]
pub struct TokioClock {
    start_wall: DateTime<Utc>,
    start_instant: Instant,
}

#[cfg(any(test, feature = "test-util"))]
impl TokioClock {
    /// Must be created inside a tokio runtime.
    pub fn new(start_wall: DateTime<Utc>) -> Self {
        Self {
            start_wall,
            start_instant: Instant::now(),
        }
    }
}

#[cfg(any(test, feature = "test-util"))]
impl Clock for TokioClock {
    fn now(&self) -> DateTime<Utc> {
        let elapsed = self.start_instant.elapsed();
        self.start_wall + Duration::from_std(elapsed).expect("elapsed overflow")
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;

    #[tokio::test(start_paused = true)]
    async fn tokio_clock_follows_paused_time() {
        let start = Utc.with_ymd_and_hms(2026, 11, 17, 12, 0, 0).unwrap();
        let clock = TokioClock::new(start);
        assert_eq!(clock.now(), start);
        tokio::time::advance(std::time::Duration::from_millis(1500)).await;
        assert_eq!(clock.now(), start + Duration::milliseconds(1500));
    }
}
