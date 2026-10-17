use crate::{config::Config, types::RandomSource};
use std::time::Duration;

pub fn backoff_delay(failures: u32, cfg: &Config, rng: &mut dyn RandomSource) -> Duration {
    let base = cfg
        .backoff_initial
        .saturating_mul(1 << (failures - 1).min(16))
        .min(cfg.backoff_max);
    base.mul_f64(1.0 + 0.5 * rng.unit()) // jitter: +0–50%
}
