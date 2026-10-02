use anyhow::Error;
use serde::Deserialize;

use crate::{
    gamma_client::keyset_markets_response::Market,
    types::{ConditionId, DecimalString, UtcMicros},
};

#[derive(Debug, Deserialize, Clone)]
pub struct MarketInfo {
    pub condition_id: ConditionId,
    pub question: String,
    pub slug: String,
    pub event_id: Option<String>,
    pub tags: Vec<String>,
    // pub yes_token: TokenId,
    // pub no_token: TokenId,
    pub start_date: Option<UtcMicros>,
    pub end_date: Option<UtcMicros>,
    pub active: bool,
    pub closed: bool,
    pub liquidity_usd: DecimalString,
    pub tick_size: DecimalString,
    pub neg_risk: Option<bool>,
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
            // && self.yes_token == other.yes_token
            // && self.no_token == other.no_token
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
        // self.yes_token = other.yes_token;
        // self.no_token = other.no_token;
        self.start_date = other.start_date;
        self.end_date = other.end_date;
        self.active = other.active;
        self.closed = other.closed;
        self.liquidity_usd = other.liquidity_usd;
        self.tick_size = other.tick_size;
        self.neg_risk = other.neg_risk;
        self.accepting_orders = other.accepting_orders;
        self.fees_enabled = other.fees_enabled;
        self.fee_type = other.fee_type;
        self.fee_rate = other.fee_rate;
    }
}

impl TryFrom<Market> for MarketInfo {
    type Error = Error;

    fn try_from(value: Market) -> Result<Self, Self::Error> {
        Ok(Self {
            condition_id: ConditionId::from(value.condition_id),
            question: value
                .question
                .ok_or_else(|| anyhow::anyhow!("missing question"))?,
            slug: value.slug.ok_or_else(|| anyhow::anyhow!("missing slug"))?,
            event_id: value
                .events
                .clone()
                .and_then(|events| events.first().map(|e| e.id.clone())),
            tags: value
                .tags
                .unwrap_or_default()
                .into_iter()
                .map(|tag| tag.id)
                .collect(),
            start_date: value.start_date.map(UtcMicros::from),
            end_date: value.end_date.map(UtcMicros::from),
            active: value
                .active
                .ok_or_else(|| anyhow::anyhow!("missing active"))?,
            closed: value
                .closed
                .ok_or_else(|| anyhow::anyhow!("missing closed"))?,
            liquidity_usd: value
                .liquidity
                .ok_or_else(|| anyhow::anyhow!("missing liquidity_usd for market: {}", value.id))?
                .into(),
            tick_size: value
                .order_price_min_tick_size
                .ok_or_else(|| anyhow::anyhow!("missing tick_size"))?
                .into(),
            neg_risk: value
                .events
                .and_then(|events| events.first().and_then(|e| e.neg_risk)),
            accepting_orders: value
                .accepting_orders
                .ok_or_else(|| anyhow::anyhow!("missing accepting_orders"))?,
            fees_enabled: value
                .fees_enabled
                .ok_or_else(|| anyhow::anyhow!("missing fees_enabled"))?,
            fee_type: value.fee,
            fee_rate: value
                .fee_schedule
                .and_then(|schedule| schedule.rate.map(DecimalString::from)),
        })
    }
}
