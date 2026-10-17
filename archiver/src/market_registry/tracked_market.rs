use chrono::{DateTime, Utc};

use crate::market_registry::market_info::MarketInfo;

#[derive(Debug, Clone, PartialEq)]
pub struct TrackedMarket {
    /// Latest version seen on Gamma.
    pub info: MarketInfo,
    pub added_at: DateTime<Utc>,
    pub status: MarketStatus,
}

#[derive(Debug, PartialEq, Clone)]
pub enum MarketStatus {
    Tracking,
    Closing {
        grace_until: DateTime<Utc>,
        cause: ClosingCause,
    },
}

#[derive(Debug, PartialEq, Clone)]
pub enum ClosingCause {
    ClosedOnGamma,
    ResolvedOnWebsocket,
}
