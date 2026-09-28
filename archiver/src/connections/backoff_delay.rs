use std::{collections::VecDeque, num::NonZeroU32, time::Duration};

use crate::config::Config;

pub trait RandomSource {
    /// Uniformly between 0.0 (inclusive) and 1.0 (exclusive)
    fn next_fraction(&mut self) -> f64;
}

/// A random source implemented using the system random generator
pub struct SystemRandom;
impl RandomSource for SystemRandom {
    fn next_fraction(&mut self) -> f64 {
        rand::random::<f64>()
    }
}

pub struct FakeRandom {
    values: VecDeque<f64>,
    last: f64,
}

impl FakeRandom {
    pub fn fixed(value: f64) -> Self {
        Self {
            values: VecDeque::new(),
            last: value,
        }
    }

    pub fn sequence(values: impl IntoIterator<Item = f64>) -> Self {
        let values: VecDeque<f64> = values.into_iter().collect();
        let last = values.back().copied().unwrap_or(0.0);
        Self { values, last }
    }
}

impl RandomSource for FakeRandom {
    fn next_fraction(&mut self) -> f64 {
        self.values.pop_front().unwrap_or(self.last)
    }
}

/// The backoff settings from `Config` (section 12.2).
#[derive(Debug, Clone, Copy)]
struct BackoffConfig {
    pub initial: Duration, // backoff_initial_s, default 1 s
    pub max: Duration,     // backoff_max_s, default 60 s (caps the base, before jitter)
}

impl From<&Config> for BackoffConfig {
    fn from(c: &Config) -> Self {
        Self {
            initial: Duration::from_secs(c.backoff_initial_s),
            max: Duration::from_secs(c.backoff_max_s),
        }
    }
}

/// Fraction of the base wait added at most as random jitter (0.5 = up to 100%).
const JITTER_FRACTION: f64 = 0.5;

/// Exponent limit: 2^30 seconds is far above any sensible cap, and keeps the
/// multiplication from overflowing during a very long outage.
const MAX_EXPONENT: u32 = 30;

/// Calculates how long to wait before reconnect attempt number `attempt`
///
/// Base doubles from `initial` and is capped at `max`; then 0-50% random jitter
/// is added. The cap is applied before jitter so that any connections which
/// have been failing for a while still spread out instead of all retrying at
/// `max`.
/// With the default config of `initial`=1s and `max`=60s, the longest possible
/// wait is therefore 90s.
pub fn backoff_delay(
    attempt: NonZeroU32,
    config: &Config,
    random: &mut dyn RandomSource,
) -> Duration {
    let bc = BackoffConfig::from(config);
    calculate_backoff_delay(attempt, &bc, random)
}

fn calculate_backoff_delay(
    attempt: NonZeroU32,
    config: &BackoffConfig,
    random: &mut dyn RandomSource,
) -> Duration {
    let exponent = (attempt.get() - 1).min(MAX_EXPONENT);
    let base = config
        .initial
        .saturating_mul(1u32 << exponent)
        .min(config.max);

    // Guard against a misbehaving source: Duration::mul_f64 panics on Nan,
    // infinity or negative values.
    let fraction = random.next_fraction();
    let fraction = if fraction.is_finite() {
        fraction.clamp(0.0, 1.0)
    } else {
        0.0
    };

    base + base.mul_f64(JITTER_FRACTION * fraction)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn delay(attempt: NonZeroU32, jitter: f64) -> Duration {
        let config = Config {
            backoff_initial_s: 1,
            backoff_max_s: 60,
            ..Default::default()
        };
        backoff_delay(attempt, &config, &mut FakeRandom::fixed(jitter))
    }

    #[test]
    fn first_failure_waits_one_second() {
        assert_eq!(
            delay(NonZeroU32::new(1).unwrap(), 0.0),
            Duration::from_secs(1)
        );
    }

    #[test]
    fn wait_doubles_each_attempt() {
        let waits: Vec<Duration> = (1..=5)
            .map(|a| delay(NonZeroU32::new(a).unwrap(), 0.0))
            .collect();
        let expected: Vec<Duration> = [1, 2, 4, 8, 16].map(Duration::from_secs).to_vec();
        assert_eq!(waits, expected);
    }

    #[test]
    fn wait_is_capped_at_sixty_seconds() {
        assert_eq!(
            delay(NonZeroU32::new(10).unwrap(), 0.0),
            Duration::from_secs(60)
        );
    }

    #[test]
    fn jitter_lower_bound_adds_nothing() {
        assert_eq!(
            delay(NonZeroU32::new(3).unwrap(), 0.0),
            Duration::from_secs(4)
        );
    }

    #[test]
    fn jitter_upper_bound_stays_just_under_six_seconds() {
        let d = delay(NonZeroU32::new(3).unwrap(), 0.999);
        assert!(d > Duration::from_millis(5_990), "got {d:?}");
        assert!(d < Duration::from_secs(6), "got {d:?}");
    }
}
