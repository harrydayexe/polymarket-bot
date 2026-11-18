//! Shared library for the Polymarket archiver and the live paper-trading system.
//!
//! Code here is pure where it can be (books, market selection, registry planning, print
//! normalisation, grouping and rolling volume), with time and randomness injected through
//! [clock::Clock] and [random::RandomSource] so callers and tests control both.
//!
//! | Module | Contents |
//! | --- | --- |
//! | [book] | Order books rebuilt from `book` and `price_change` messages; depth, walks, midpoint |
//! | [clock] | Injected wall-clock time |
//! | [random] | Injected randomness, including a seeded generator for reproducible runs |
//! | [market] | Gamma markets, universe selection, the sanity check and registry planning |
//! | [archive] | The archive's on-disk layout and line format, and a reader |
//! | [fees] | The dated table of fee coefficients by category, the last fee fallback |
//! | [units] | Money and probability newtypes (§3.1 of the Phase 1 spec), logits and fees |
//! | [normalize] | Prints as buys of one outcome, grouping into sweeps, rolling pair volume |
//! | [client] | Polymarket endpoints, client settings and the live system's request budgets |
//! | [mod@invariant] | The error returned when an internal invariant does not hold |

pub mod archive;
pub mod book;
pub mod client;
pub mod clock;
pub mod fees;
pub mod invariant;
pub mod market;
pub mod normalize;
pub mod random;
pub mod units;

#[cfg(any(test, feature = "test-util"))]
pub mod test_util;
