use std::collections::HashMap;

use marcasite::types::{ConditionId, TokenId};

/// The two tokens of a market. They are always tracked together.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketTokens {
    pub yes: TokenId,
    pub no: TokenId,
}

/// Markets whose tokens the [Registry](super::registry::Registry) started or stopped tracking.
///
/// A market can appear in both maps if its token IDs changed. Apply removals before additions.
#[derive(Debug, Default, PartialEq)]
pub struct TokenChanges {
    /// Markets whose tokens should be subscribed.
    pub added: HashMap<ConditionId, MarketTokens>,
    /// Markets whose tokens should be unsubscribed.
    pub removed: HashMap<ConditionId, MarketTokens>,
}

impl TokenChanges {
    pub fn is_empty(&self) -> bool {
        self.added.is_empty() && self.removed.is_empty()
    }
}
