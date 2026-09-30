//! Shared builders for market registry tests.

use rust_decimal::Decimal;

use crate::{
    clock::Clock,
    config::Config,
    market_registry::market_info::MarketInfo,
    types::{ConditionId, DecimalString, UtcMicros},
};

pub const HOUR: i64 = 3_600_000_000;

pub fn config() -> Config {
    Config {
        category_tags: ["politics", "geopolitics"]
            .into_iter()
            .map(String::from)
            .collect(),
        min_liquidity_usd: Decimal::from(1_000),
        closed_grace_period_h: 6,
        ..Config::default()
    }
}

/// Clock that always reports the same wall time.
pub struct FixedClock(pub UtcMicros);

impl Clock for FixedClock {
    fn wall_now(&self) -> UtcMicros {
        self.0
    }

    fn wall_in_hours(&self, hours: i64) -> UtcMicros {
        UtcMicros(self.0.0 + hours * HOUR)
    }
}

/// Builder for a qualifying `MarketInfo`: politics, active, open, $5,000 liquidity.
pub struct M(MarketInfo);

impl M {
    pub fn new(id: &str) -> Self {
        M(MarketInfo {
            condition_id: ConditionId(id.to_string()),
            question: "".to_string(),
            slug: "".to_string(),
            event_id: None,
            tags: vec!["politics".into()],
            yes_token: format!("{id}-yes").into(),
            no_token: format!("{id}-no").into(),
            start_date: None,
            end_date: None,
            active: true,
            closed: false,
            liquidity_usd: Decimal::from(5_000).into(),
            tick_size: DecimalString("0".to_string()),
            neg_risk: false,
            fetched_at: UtcMicros(0),
            accepting_orders: true,
            fees_enabled: false,
            fee_type: None,
            fee_rate: None,
        })
    }
    pub fn tags(mut self, t: &[&str]) -> Self {
        self.0.tags = t.iter().map(|s| s.to_string()).collect();
        self
    }
    pub fn liquidity(mut self, v: i64) -> Self {
        self.0.liquidity_usd = Decimal::from(v).into();
        self
    }
    pub fn active(mut self, v: bool) -> Self {
        self.0.active = v;
        self
    }
    pub fn closed(mut self, v: bool) -> Self {
        self.0.closed = v;
        self
    }
    pub fn accepting_orders(mut self, v: bool) -> Self {
        self.0.accepting_orders = v;
        self
    }
    pub fn end_date(mut self, v: UtcMicros) -> Self {
        self.0.end_date = Some(v);
        self
    }
    pub fn build(self) -> MarketInfo {
        self.0
    }
}
