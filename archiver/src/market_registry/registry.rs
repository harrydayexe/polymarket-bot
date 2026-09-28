use std::collections::HashMap;

use crate::{
    market_registry::tracked_market::TrackedMarket,
    types::{ConditionId, UtcMicros},
};

#[derive(Debug)]
pub struct Registry {
    markets: HashMap<ConditionId, TrackedMarket>,
    last_accepted_count: u64,
    last_success_at: Option<UtcMicros>,
}
