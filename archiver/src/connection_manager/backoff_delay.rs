use crate::{config::Config, types::RandomSource};
use std::time::Duration;

pub fn backoff_delay(failures: u32, cfg: &Config, rng: &mut dyn RandomSource) -> Duration {
    let base = cfg
        .backoff_initial
        .saturating_mul(1 << (failures - 1).min(16))
        .min(cfg.backoff_max);
    base.mul_f64(1.0 + 0.5 * rng.unit()) // jitter: +0–50%
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ScriptedRandom;

    fn delay(failures: u32, jitter: f64) -> Duration {
        backoff_delay(
            failures,
            &Config::default(),
            &mut ScriptedRandom::new([jitter]),
        )
    }

    #[test]
    fn first_failure_waits_the_initial_delay() {
        assert_eq!(delay(1, 0.0), Duration::from_secs(1));
    }

    #[test]
    fn delay_doubles_with_each_failure() {
        let delays: Vec<u64> = (1..=5).map(|n| delay(n, 0.0).as_secs()).collect();
        assert_eq!(delays, vec![1, 2, 4, 8, 16]);
    }

    #[test]
    fn delay_is_capped_at_the_maximum() {
        assert_eq!(delay(10, 0.0), Duration::from_secs(60));
        assert_eq!(delay(u32::MAX, 0.0), Duration::from_secs(60));
    }

    #[test]
    fn jitter_adds_up_to_half_the_delay() {
        let mut rng = ScriptedRandom::new([0.0, 0.999]);
        let cfg = Config::default();
        assert_eq!(backoff_delay(3, &cfg, &mut rng), Duration::from_secs(4));
        let jittered = backoff_delay(3, &cfg, &mut rng);
        assert!(jittered > Duration::from_millis(5_990), "{jittered:?}");
        assert!(jittered < Duration::from_secs(6), "{jittered:?}");
    }
}
