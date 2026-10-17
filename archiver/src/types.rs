use chrono::{DateTime, Utc};

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

// Tests: returns scripted values in order
pub struct ScriptedRandom(std::collections::VecDeque<f64>);
impl ScriptedRandom {
    pub fn new(values: impl IntoIterator<Item = f64>) -> Self {
        Self(values.into_iter().collect())
    }
}
impl RandomSource for ScriptedRandom {
    fn unit(&mut self) -> f64 {
        self.0.pop_front().expect("ran out of scripted values")
    }
}

marcasite_core::string_id! {
    /// Identifer created once per process start
    /// Takes the form:
    pub struct RunId;
}

impl RunId {
    pub fn create(wall: DateTime<Utc>, rng: &mut dyn RandomSource) -> Self {
        let suffix = rng.random_id(4);
        Self::new(format!("r-{}-{}", wall.format("%Y%m%d-%H%M"), suffix))
    }
}
