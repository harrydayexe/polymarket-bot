use std::collections::HashMap;

use crate::{
    connections::connection::Connection,
    types::{ConnSlotId, TokenId, UtcMicros},
};

#[derive(Debug)]
pub struct ConnectionManager {
    /// Map of connections to their slot ID.
    connections: HashMap<ConnSlotId, Connection>,

    /// Which connection carries each token.
    token_slot: HashMap<TokenId, ConnSlotId>,

    next_slot_no: i64,

    // TODO: add gaps tracker
    // gaps: GapTracker

    // Subscribed but no full book yet, since when
    awaiting_book: HashMap<TokenId, UtcMicros>,
}
