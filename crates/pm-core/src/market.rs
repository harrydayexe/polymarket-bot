//! Markets as seen on Gamma, which of them belong to the universe, and the pure planning
//! behind the market registry (what to add, update and remove each cycle), and how tokens are
//! placed on websocket connections ([assignment]).
//!
//! The registry's I/O (fetching listings, writing records, telling connections) belongs to
//! the program using it; [registry::Registry] only plans and applies changes.

pub mod assignment;
pub mod market_info;
pub mod not_enough_markets_error;
pub mod registry;
pub mod sanity_check;
pub mod selection;
pub mod token_changes;
pub mod tracked_market;

pub use assignment::TokenAssignment;
pub use market_info::{MarketInfo, MarketParseError};
pub use registry::{Registry, RegistryConfig};
pub use sanity_check::sanity_check;
pub use selection::{SelectionRules, parse_markets, qualifies};
pub use token_changes::{MarketTokens, TokenChanges};
