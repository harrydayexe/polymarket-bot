use crate::types::{ConditionId, DecimalString, TokenId, UtcMicros};

#[derive(Debug, Clone)]
pub struct MarketInfo {
    condition_id: ConditionId,
    question: String,
    slug: String,
    event_id: Option<String>,
    tags: Vec<String>,
    yes_token: TokenId,
    no_token: TokenId,
    start_date: Option<UtcMicros>,
    end_date: Option<UtcMicros>,
    active: bool,
    closed: bool,
    liquidity_usd: DecimalString,
    tick_size: DecimalString,
    neg_risk: bool,
    fetched_at: UtcMicros,
}
