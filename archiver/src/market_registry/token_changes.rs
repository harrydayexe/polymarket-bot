use std::collections::HashSet;

use marcasite::types::TokenId;

/// A set of tokens changed by the [Registry].
#[derive(Debug)]
pub struct TokenChanges {
    /// Tokens added to the registry.
    pub added: HashSet<TokenId>,
    /// Tokens removed from the registry.
    pub removed: HashSet<TokenId>,
}
