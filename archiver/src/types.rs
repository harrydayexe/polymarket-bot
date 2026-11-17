//! Identifiers and randomness, now defined in pm-core.

pub use pm_core::{
    archive::record::RunId,
    random::{RandomSource, SystemRandom},
};

#[cfg(test)]
pub use pm_core::random::ScriptedRandom;
