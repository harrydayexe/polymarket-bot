use crate::market_registry::market_info::MarketInfo;
use crate::types::UtcMicros;

#[derive(Debug, Clone)]
pub struct TrackedMarket {
    pub info: MarketInfo,
    pub added_at: UtcMicros,
    pub status: MarketStatus,
}

#[derive(Debug, PartialEq, Clone)]
pub enum MarketStatus {
    Tracking,
    Closing {
        grace_until: UtcMicros,
        cause: ClosingCause,
    },
}

#[derive(Debug, PartialEq, Clone)]
pub enum ClosingCause {
    ClosedOnGamma,
    ClosedOnWebSocket,
}
