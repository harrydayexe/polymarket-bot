use anyhow::anyhow;
use chrono::{DateTime, Utc};
use marcasite::{
    clob::Token,
    gamma::Market,
    types::{ConditionId, TokenId},
};

#[derive(Debug, Clone)]
pub struct TrackedMarket {
    pub condition_id: ConditionId,
    pub question: String,
    pub active: bool,
    pub closed: bool,
    pub accepting_orders: bool,
    pub added_at: DateTime<Utc>,
    pub status: MarketStatus,
    pub yes_token: TokenId,
    pub no_token: TokenId,
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
        let outcomes = value
            .outcomes
            .ok_or(anyhow!("market does not contain outcomes"))?;
        let tokens = value
            .clob_token_ids
            .ok_or(anyhow!("market does not contain clob_token_ids"))?;

        let [t0, t1] = <[TokenId; 2]>::try_from(tokens)
            .map_err(|_| anyhow!("market does not contain exactly two clob_token_ids"))?;

        let (yes_token, no_token) = match outcomes.as_slice() {
            [a, b] if a == "Yes" && b == "No" => (t0, t1),
            [a, b] if a == "No" && b == "Yes" => (t1, t0),
            _ => return Err(anyhow!("market outcomes are not Yes and No")),
        };

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
            yes_token,
            no_token,
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
