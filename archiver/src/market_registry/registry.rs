use std::collections::HashMap;

use crate::market_registry::tracked_market::TrackedMarket;

#[derive(Debug)]
pub struct Registry {
    markets: HashMap<String, TrackedMarket>,
}
