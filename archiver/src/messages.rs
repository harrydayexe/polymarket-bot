use marcasite::types::ConditionId;

use crate::market_registry::token_changes::TokenChanges;

pub enum RegistryMsg {
    MarketResolved(ConditionId),
    NewMarketSeen,
}

pub enum ConnectionManagerMsg {
    TokenChanges(TokenChanges),
}
