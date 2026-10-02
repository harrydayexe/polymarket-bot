// use crate::{config::Config, types::MonoTime};
//
// /// RateLimiter holds up to a maximum amount of permits. This bucket is refilled at a set rate,
// /// allowing for quick bursts of requests, while keeping the long-term average capped.
// ///
// /// There is no thread actively "refilling" the bucket. Instead the amount to add at each call of
// /// `try_acquire`, keeping the object simple.
// #[derive(Debug, Clone)]
// pub struct RateLimiter {
//     capacity: f64,
//     refill_per_second: f64,
//     tokens: f64,
//     last_refill: MonoTime,
// }
//
// impl From<&Config> for RateLimiter {
//     fn from(c: &Config) -> Self {
//         Self {
//             capacity: 1.0,
//             refill_per_second: c.snapshot_max_requests_per_s as f64,
//             tokens: 1.0,
//             last_refill: MonoTime::now(),
//         }
//     }
// }
//
// impl RateLimiter {
//     /// Get approval to send a snapshot request.
//     pub fn try_acquire(&mut self) -> bool {
//         let now = MonoTime::now();
//         let elapsed = now - self.last_refill;
//         self.tokens =
//             (self.tokens + elapsed.as_secs_f64() * self.refill_per_second).min(self.capacity);
//         self.last_refill = now;
//
//         if self.tokens >= 1.0 {
//             self.tokens -= 1.0;
//             true
//         } else {
//             false
//         }
//     }
// }
//
// #[cfg(test)]
// mod tests {
//     use super::*;
//     use std::time::Duration;
//     use tokio::time::{Instant, advance};
//
//     // Power-of-two step so f64 refill arithmetic stays exact.
//     const STEP: Duration = Duration::from_millis(250);
//
//     fn limiter(per_second: u64) -> RateLimiter {
//         let cfg = Config {
//             snapshot_max_requests_per_s: per_second,
//             ..Config::default()
//         };
//         RateLimiter::from(&cfg)
//     }
//
//     #[tokio::test(start_paused = true)]
//     async fn first_call_succeeds_then_blocks() {
//         let mut rl = limiter(2);
//         assert!(rl.try_acquire());
//         assert!(!rl.try_acquire()); // no burst: capacity is 1
//     }
//
//     #[tokio::test(start_paused = true)]
//     async fn refills_after_interval() {
//         let mut rl = limiter(2);
//         assert!(rl.try_acquire());
//
//         advance(Duration::from_millis(250)).await; // 0.5 token
//         assert!(!rl.try_acquire());
//
//         advance(Duration::from_millis(250)).await; // 1.0 token
//         assert!(rl.try_acquire());
//         assert!(!rl.try_acquire());
//     }
//
//     #[tokio::test(start_paused = true)]
//     async fn fractional_refill_is_not_lost() {
//         // Regression for the as_secs() truncation bug: calls more often
//         // than once per second must still accumulate tokens.
//         let mut rl = limiter(1);
//         assert!(rl.try_acquire());
//
//         for _ in 0..3 {
//             advance(STEP).await;
//             assert!(!rl.try_acquire()); // 0.25, 0.5, 0.75
//         }
//         advance(STEP).await; // 1.0
//         assert!(rl.try_acquire());
//     }
//
//     #[tokio::test(start_paused = true)]
//     async fn idle_time_does_not_accumulate_burst() {
//         let mut rl = limiter(2);
//         advance(Duration::from_secs(3600)).await;
//
//         assert!(rl.try_acquire());
//         assert!(!rl.try_acquire()); // capped at capacity 1
//     }
//
//     #[tokio::test(start_paused = true)]
//     async fn ten_batches_at_two_per_second() {
//         let mut rl = limiter(2);
//         let start = Instant::now();
//         let mut sent_at = Vec::new();
//
//         while sent_at.len() < 10 {
//             if rl.try_acquire() {
//                 sent_at.push(start.elapsed());
//             } else {
//                 advance(STEP).await;
//             }
//         }
//
//         // Evenly spaced at 0.5s: 0, 0.5, ..., 4.5s.
//         for (i, t) in sent_at.iter().enumerate() {
//             assert_eq!(*t, Duration::from_millis(500 * i as u64));
//         }
//         // Never more than 2 sends in any 1s window.
//         for w in sent_at.windows(3) {
//             assert!(w[2] - w[0] >= Duration::from_secs(1), "{w:?}");
//         }
//     }
// }
