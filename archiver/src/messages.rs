use chrono::{DateTime, Utc};
use marcasite::types::ConditionId;

use crate::market_registry::{registry::PlannedChanges, token_changes::TokenChanges};

pub enum RegistryMsg {
    MarketResolved(ConditionId),
    NewMarketSeen,
}

pub enum ConnectionManagerMsg {
    TokenChanges(TokenChanges),
}

pub enum Record {
    Market {
        recv_ts: DateTime<Utc>,
        changes: PlannedChanges,
    },
    Control,
}
