use crate::market_registry::{market_info::MarketInfo, types::UtcMicros};

#[derive(Debug, Clone)]
pub struct TrackedMarket {
    info: MarketInfo,
    added_at: UtcMicros,
    status: MarketStatus,
}

#[derive(Debug, Clone, PartialEq)]
enum MarketStatus {
    Tracking,
    Closing {
        grace_until: UtcMicros,
        cause: String,
    },
    ResolvedOnWebsocket,
}
