//! Injected randomness. Code that needs random numbers takes a [RandomSource], so tests can
//! script the values and runs can be replayed from a seed.

use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha20Rng;

use crate::invariant;

const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

/// Uniform integer in [0, n).
fn index<R: RandomSource + ?Sized>(rng: &mut R, n: usize) -> usize {
    ((rng.unit() * n as f64) as usize).min(n - 1)
}

pub trait RandomSource {
    /// Uniform in [0, 1).
    fn unit(&mut self) -> f64;

    fn random_id(&mut self, len: usize) -> String {
        (0..len)
            .map(|_| CHARSET[index(self, CHARSET.len())] as char)
            .collect()
    }
}

// Production
pub struct SystemRandom;
impl RandomSource for SystemRandom {
    fn unit(&mut self) -> f64 {
        rand::random::<f64>()
    }
}

/// Reproducible randomness: the same seed gives the same sequence on every platform and
/// version of this crate (ChaCha20, 53 random bits per value). For engine state that must
/// replay exactly.
#[derive(Debug, Clone)]
pub struct SeededRandom {
    rng: ChaCha20Rng,
    seed: u64,
}

impl SeededRandom {
    pub fn new(seed: u64) -> Self {
        Self {
            rng: ChaCha20Rng::seed_from_u64(seed),
            seed,
        }
    }

    /// The seed this generator started from, to record with a run.
    pub fn seed(&self) -> u64 {
        self.seed
    }
}

/// 2^-53: the gap between consecutive values [SeededRandom::unit] can return.
const UNIT_STEP: f64 = 1.0 / (1u64 << 53) as f64;

impl RandomSource for SeededRandom {
    fn unit(&mut self) -> f64 {
        // The top 53 bits of a u64 are exactly representable as an f64 in [0, 2^53).
        let bits = self.rng.next_u64() >> 11;
        let value = bits as f64 * UNIT_STEP;
        invariant!(bits < (1u64 << 53), 0.0);
        invariant!((0.0..1.0).contains(&value), 0.0);
        value
    }
}

/// Tests: returns scripted values in order.
#[cfg(any(test, feature = "test-util"))]
pub struct ScriptedRandom(std::collections::VecDeque<f64>);
#[cfg(any(test, feature = "test-util"))]
impl ScriptedRandom {
    pub fn new(values: impl IntoIterator<Item = f64>) -> Self {
        Self(values.into_iter().collect())
    }
}
#[cfg(any(test, feature = "test-util"))]
impl RandomSource for ScriptedRandom {
    fn unit(&mut self) -> f64 {
        self.0.pop_front().expect("ran out of scripted values")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_seed_same_sequence() {
        let mut a = SeededRandom::new(42);
        let mut b = SeededRandom::new(42);
        let xs: Vec<f64> = (0..100).map(|_| a.unit()).collect();
        let ys: Vec<f64> = (0..100).map(|_| b.unit()).collect();
        assert_eq!(xs, ys);
        assert_eq!(a.random_id(8), b.random_id(8));
        assert_eq!(a.seed(), 42);
    }

    #[test]
    fn different_seeds_differ() {
        let xs: Vec<f64> = {
            let mut r = SeededRandom::new(1);
            (0..10).map(|_| r.unit()).collect()
        };
        let ys: Vec<f64> = {
            let mut r = SeededRandom::new(2);
            (0..10).map(|_| r.unit()).collect()
        };
        assert_ne!(xs, ys);
    }

    /// Pins the sequence, so a dependency upgrade that changed it would be noticed: a
    /// recorded run must replay with the same draws.
    #[test]
    fn sequence_is_stable() {
        let mut r = SeededRandom::new(7);
        let first: Vec<String> = (0..3).map(|_| format!("{:.12}", r.unit())).collect();
        assert_eq!(first, SEED_7_FIRST_THREE);
    }

    const SEED_7_FIRST_THREE: [&str; 3] = ["0.023335618701", "0.534607713771", "0.951386061432"];

    #[test]
    fn values_are_in_the_unit_interval() {
        let mut r = SeededRandom::new(3);
        assert!(
            (0..10_000)
                .map(|_| r.unit())
                .all(|x| (0.0..1.0).contains(&x))
        );
    }

    #[test]
    fn random_ids_use_the_charset() {
        let id = SeededRandom::new(9).random_id(32);
        assert_eq!(id.len(), 32);
        assert!(id.bytes().all(|b| CHARSET.contains(&b)));
    }

    #[test]
    fn scripted_values_come_back_in_order() {
        let mut r = ScriptedRandom::new([0.0, 0.5, 0.999]);
        assert_eq!(r.unit(), 0.0);
        assert_eq!(r.unit(), 0.5);
        assert_eq!(index(&mut r, 10), 9);
    }
}
