use anyhow::anyhow;
use chrono::{DateTime, Utc};
use marcasite::{data::ConditionId, gamma::Market};

#[derive(Debug, Clone)]
pub struct TrackedMarket {
    pub condition_id: ConditionId,
    pub question: String,
    pub active: bool,
    pub closed: bool,
    pub accepting_orders: bool,
    pub added_at: DateTime<Utc>,
    pub status: MarketStatus,
}

impl PartialEq for TrackedMarket {
    fn eq(&self, other: &Self) -> bool {
        self.condition_id == other.condition_id
            && self.question == other.question
            && self.active == other.active
            && self.closed == other.closed
            && self.accepting_orders == other.accepting_orders
            && self.added_at == other.added_at
            && self.status == other.status
    }
}

impl TryFrom<Market> for TrackedMarket {
    type Error = anyhow::Error;

    fn try_from(value: Market) -> Result<Self, Self::Error> {
        Ok(Self {
            condition_id: value
                .condition_id
                .ok_or(anyhow!("market does not contain condition_id"))?,
            question: value
                .question
                .ok_or(anyhow!("market does not contain question"))?,
            active: value
                .active
                .ok_or(anyhow!("market does not contain active"))?,
            closed: value
                .closed
                .ok_or(anyhow!("market does not contain closed"))?,
            accepting_orders: value
                .accepting_orders
                .ok_or(anyhow!("market does not contain accepting_orders"))?,
            added_at: Utc::now(),
            status: MarketStatus::Tracking,
        })
    }
}

impl TrackedMarket {
    pub fn update_from(&mut self, other: &TrackedMarket) {
        if self == other {
            return;
        }

        self.condition_id = other.condition_id.clone();
        self.question = other.question.clone();
        self.active = other.active;
        self.closed = other.closed;
        self.accepting_orders = other.accepting_orders;
        self.added_at = other.added_at;
        self.status = other.status.clone();
    }
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
    ClosedOnWebSocket,
}
