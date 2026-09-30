use std::time::{SystemTime, UNIX_EPOCH};

use crate::types::{MonoTime, UtcMicros};

pub trait Clock {
    /// Get the current wall time in UTC microseconds.
    fn wall_now(&self) -> UtcMicros;
    fn wall_in_hours(&self, hours: i64) -> UtcMicros;
}

pub struct SystemClock;

impl Clock for SystemClock {
    fn wall_now(&self) -> UtcMicros {
        let micros: i64 = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock before 1970")
            .as_micros() as i64;

        UtcMicros(micros)
    }

    fn wall_in_hours(&self, hours: i64) -> UtcMicros {
        UtcMicros(self.wall_now().0 + hours * 3_600_000_000)
    }
}

/// Test clock: wall time moves in step with Tokio's paused clock
pub struct FakeClock {
    base_wall: UtcMicros,
    base_mono: MonoTime,
}

impl Clock for FakeClock {
    fn wall_now(&self) -> UtcMicros {
        let elapsed = MonoTime::now() - self.base_mono;
        UtcMicros(self.base_wall.0 + elapsed.as_micros() as i64)
    }

    fn wall_in_hours(&self, hours: i64) -> UtcMicros {
        UtcMicros(self.wall_now().0 + hours * 3_600_000_000)
    }
}
