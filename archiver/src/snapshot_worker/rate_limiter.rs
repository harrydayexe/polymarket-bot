use std::time::Duration;

use tokio::time::Instant;

/// A token bucket: holds up to `capacity` permits, gains `per_second` permits a second, and
/// each request spends one. (The permits are unrelated to market tokens; the name is standard.)
///
/// Rather than counting permits, it keeps the time at which the bucket will be full again (the
/// "generic cell rate algorithm"), which behaves the same but needs no floating point, so a
/// permit promised by [ready_at](Self::ready_at) is always there at that instant.
#[derive(Debug)]
pub struct RateLimiter {
    /// Time to gain one permit.
    per_permit: Duration,
    /// How far ahead of now `full_at` may be: the time to gain `capacity - 1` permits.
    burst: Duration,
    /// When the bucket will be full again, if nothing more is spent.
    full_at: Instant,
}

impl RateLimiter {
    /// A full bucket of `capacity` permits, refilled at `per_second`.
    ///
    /// # Panics
    ///
    /// If `capacity` or `per_second` is 0.
    pub fn new(capacity: u32, per_second: u32, now: Instant) -> Self {
        assert!(capacity > 0, "capacity must be greater than 0");
        assert!(per_second > 0, "per_second must be greater than 0");
        let per_permit = Duration::from_secs(1) / per_second;
        Self {
            per_permit,
            burst: per_permit * (capacity - 1),
            full_at: now,
        }
    }

    /// The earliest time, at or after `now`, at which a permit is available.
    pub fn ready_at(&self, now: Instant) -> Instant {
        now.max(self.full_at.checked_sub(self.burst).unwrap_or(now))
    }

    /// Spends a permit if one is available at `now`.
    pub fn try_acquire(&mut self, now: Instant) -> bool {
        if self.ready_at(now) > now {
            return false;
        }
        self.full_at = self.full_at.max(now) + self.per_permit;
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    #[test]
    fn starts_full_then_refills_at_the_rate() {
        let t0 = Instant::now();
        let mut limiter = RateLimiter::new(2, 2, t0);

        assert!(limiter.try_acquire(t0));
        assert!(limiter.try_acquire(t0));
        assert!(!limiter.try_acquire(t0));
        assert_eq!(limiter.ready_at(t0), t0 + ms(500));

        assert!(!limiter.try_acquire(t0 + ms(499)));
        assert!(limiter.try_acquire(t0 + ms(500)));
        assert_eq!(limiter.ready_at(t0 + ms(500)), t0 + ms(1000));
    }

    #[test]
    fn idle_time_refills_only_up_to_capacity() {
        let t0 = Instant::now();
        let mut limiter = RateLimiter::new(2, 2, t0);
        assert!(limiter.try_acquire(t0));

        let later = t0 + Duration::from_secs(60);
        assert!(limiter.try_acquire(later));
        assert!(limiter.try_acquire(later));
        assert!(!limiter.try_acquire(later));
    }

    #[test]
    fn ready_at_is_now_when_a_permit_is_available() {
        let t0 = Instant::now();
        let limiter = RateLimiter::new(1, 1, t0);
        assert_eq!(limiter.ready_at(t0), t0);
        assert_eq!(limiter.ready_at(t0 + ms(10)), t0 + ms(10));
    }

    #[test]
    fn permits_promised_by_ready_at_are_there() {
        let t0 = Instant::now();
        let mut limiter = RateLimiter::new(3, 7, t0);
        let mut now = t0;
        for _ in 0..100 {
            now = limiter.ready_at(now);
            assert!(limiter.try_acquire(now));
        }
        // 3 at once, then 97 at 7 a second.
        assert_eq!(now, t0 + (Duration::from_secs(1) / 7) * 97);
    }
}
