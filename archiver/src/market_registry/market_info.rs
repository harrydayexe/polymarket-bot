use serde::Deserialize;

use crate::types::{ConditionId, DecimalString, TokenId, UtcMicros};

#[derive(Debug, Deserialize)]
pub struct MarketInfo {
    pub condition_id: ConditionId,
    pub question: String,
    pub slug: String,
    pub event_id: Option<String>,
    pub tags: Vec<String>,
    pub yes_token: TokenId,
    pub no_token: TokenId,
    pub start_date: Option<UtcMicros>,
    pub end_date: Option<UtcMicros>,
    pub active: bool,
    pub closed: bool,
    pub liquidity_usd: DecimalString,
    pub tick_size: DecimalString,
    pub neg_risk: bool,
    pub fetched_at: UtcMicros,
    pub accepting_orders: bool,
    pub fees_enabled: bool,
    pub fee_type: Option<String>,
    pub fee_rate: Option<DecimalString>,
}
