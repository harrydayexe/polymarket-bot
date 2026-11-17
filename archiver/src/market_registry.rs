//! The market registry task. The pure parts (market parsing, selection, the sanity check and
//! change planning) are shared with the live system, in pm-core.

pub mod cycle;
pub mod registry_task;

pub use pm_core::market::{
    market_info, not_enough_markets_error, registry, selection, token_changes,
};
