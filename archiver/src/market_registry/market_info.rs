use serde::Deserialize;

use crate::types::{ConditionId, DecimalString, TokenId, UtcMicros};

#[derive(Debug, Deserialize, Clone)]
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

impl PartialEq for MarketInfo {
    fn eq(&self, other: &Self) -> bool {
        self.condition_id == other.condition_id
            && self.question == other.question
            && self.slug == other.slug
            && self.event_id == other.event_id
            && self.tags == other.tags
            && self.yes_token == other.yes_token
            && self.no_token == other.no_token
            && self.start_date == other.start_date
            && self.end_date == other.end_date
            && self.active == other.active
            && self.closed == other.closed
            && self.tick_size == other.tick_size
            && self.neg_risk == other.neg_risk
            && self.accepting_orders == other.accepting_orders
            && self.fees_enabled == other.fees_enabled
            && self.fee_type == other.fee_type
            && self.fee_rate == other.fee_rate
    }
}

impl MarketInfo {
    /// Update a `MarketInfo` object from another one.
    ///
    /// This update ignores `liquidity_usd` and `fetched_at` values as they will change on most new
    /// fetches
    pub fn update_from(&mut self, other: &Self) {
        if self == other {
            return;
        }
        let other = other.clone();

        self.condition_id = other.condition_id;
        self.question = other.question;
        self.slug = other.slug;
        self.event_id = other.event_id;
        self.tags = other.tags;
        self.yes_token = other.yes_token;
        self.no_token = other.no_token;
        self.start_date = other.start_date;
        self.end_date = other.end_date;
        self.active = other.active;
        self.closed = other.closed;
        self.liquidity_usd = other.liquidity_usd;
        self.tick_size = other.tick_size;
        self.neg_risk = other.neg_risk;
        self.fetched_at = other.fetched_at;
        self.accepting_orders = other.accepting_orders;
        self.fees_enabled = other.fees_enabled;
        self.fee_type = other.fee_type;
        self.fee_rate = other.fee_rate;
    }
}
