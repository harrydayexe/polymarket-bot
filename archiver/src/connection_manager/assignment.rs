use std::collections::{BTreeMap, HashMap, HashSet};

use marcasite::types::TokenId;

use crate::{connection_manager::types::ConnSlotId, market_registry::token_changes::MarketTokens};

/// Which connection slot carries each token.
///
/// New markets go to the least-loaded slot with room for both tokens, or to a new slot when
/// every slot is full. Both tokens of a market always share a slot, and tokens never move
/// between slots once placed. A slot whose last token is removed is forgotten and its ID never
/// reused, so its connection can close while new slots open.
#[derive(Debug)]
pub struct TokenAssignment {
    tokens_per_connection: usize,
    slots: BTreeMap<ConnSlotId, HashSet<TokenId>>,
    token_slot: HashMap<TokenId, ConnSlotId>,
    next_slot_no: u64,
}

impl TokenAssignment {
    pub fn new(tokens_per_connection: usize) -> Self {
        Self {
            tokens_per_connection,
            slots: BTreeMap::new(),
            token_slot: HashMap::new(),
            next_slot_no: 1,
        }
    }

    /// Places the tokens of `markets` that are not already placed.
    ///
    /// # Returns
    ///
    /// The tokens added to each slot. A slot not seen before is new and needs a connection.
    pub fn assign<'a>(
        &mut self,
        markets: impl IntoIterator<Item = &'a MarketTokens>,
    ) -> BTreeMap<ConnSlotId, HashSet<TokenId>> {
        let mut added: BTreeMap<ConnSlotId, HashSet<TokenId>> = BTreeMap::new();
        for market in markets {
            let new: Vec<&TokenId> = [&market.yes, &market.no]
                .into_iter()
                .filter(|t| !self.token_slot.contains_key(*t))
                .collect();
            if new.is_empty() {
                continue;
            }
            let slot = self.slot_with_room(new.len());
            for t in new {
                self.token_slot.insert(t.clone(), slot.clone());
                self.slots
                    .entry(slot.clone())
                    .or_default()
                    .insert(t.clone());
                added.entry(slot.clone()).or_default().insert(t.clone());
            }
        }
        added
    }

    /// Removes the tokens of `markets`.
    ///
    /// # Returns
    ///
    /// The tokens removed from each slot.
    pub fn unassign<'a>(
        &mut self,
        markets: impl IntoIterator<Item = &'a MarketTokens>,
    ) -> BTreeMap<ConnSlotId, HashSet<TokenId>> {
        let mut removed: BTreeMap<ConnSlotId, HashSet<TokenId>> = BTreeMap::new();
        for market in markets {
            for t in [&market.yes, &market.no] {
                let Some(slot) = self.token_slot.remove(t) else {
                    continue;
                };
                let tokens = self.slots.get_mut(&slot).expect("slot of a placed token");
                tokens.remove(t);
                if tokens.is_empty() {
                    self.slots.remove(&slot);
                }
                removed.entry(slot).or_default().insert(t.clone());
            }
        }
        removed
    }

    /// The least-loaded slot with room for `n` more tokens (the lowest ID on a tie), or a new
    /// slot if none has room.
    fn slot_with_room(&mut self, n: usize) -> ConnSlotId {
        let existing = self
            .slots
            .iter()
            .filter(|(_, tokens)| tokens.len() + n <= self.tokens_per_connection)
            .min_by_key(|(_, tokens)| tokens.len())
            .map(|(slot, _)| slot.clone());
        existing.unwrap_or_else(|| {
            let slot = ConnSlotId::new(format!("c-{:04}", self.next_slot_no));
            self.next_slot_no += 1;
            slot
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn market(n: u32) -> MarketTokens {
        MarketTokens {
            yes: TokenId::new(format!("{n}-yes")),
            no: TokenId::new(format!("{n}-no")),
        }
    }

    fn markets(ns: impl IntoIterator<Item = u32>) -> Vec<MarketTokens> {
        ns.into_iter().map(market).collect()
    }

    fn slot(n: u64) -> ConnSlotId {
        ConnSlotId::new(format!("c-{n:04}"))
    }

    fn loads(a: &TokenAssignment) -> Vec<(ConnSlotId, usize)> {
        a.slots.iter().map(|(s, t)| (s.clone(), t.len())).collect()
    }

    /// Two slots, holding `first` and `second` (fewer) markets.
    fn with_loads(limit: usize, first: u32, second: u32) -> TokenAssignment {
        // Fill c-0001 to its temporary limit, so the second batch opens c-0002.
        let mut a = TokenAssignment::new(first as usize * 2);
        a.assign(&markets(0..first));
        a.assign(&markets(1000..1000 + second));
        a.tokens_per_connection = limit;
        assert_eq!(
            loads(&a),
            vec![
                (slot(1), first as usize * 2),
                (slot(2), second as usize * 2)
            ]
        );
        a
    }

    #[test]
    fn empty_start_fills_one_connection() {
        let mut a = TokenAssignment::new(250);
        let added = a.assign(&markets(0..10));
        assert_eq!(added.len(), 1);
        assert_eq!(added[&slot(1)].len(), 20);
        assert_eq!(a.token_slot.len(), 20);
    }

    #[test]
    fn new_market_goes_to_the_least_loaded_connection() {
        let mut a = with_loads(250, 50, 20);
        let added = a.assign(&[market(5000)]);
        assert_eq!(added.keys().collect::<Vec<_>>(), vec![&slot(2)]);
        assert_eq!(loads(&a), vec![(slot(1), 100), (slot(2), 42)]);
    }

    #[test]
    fn new_connection_when_all_are_full() {
        let mut a = TokenAssignment::new(4);
        a.assign(&markets(0..2));
        let added = a.assign(&[market(2)]);
        assert_eq!(added.keys().collect::<Vec<_>>(), vec![&slot(2)]);
        assert_eq!(loads(&a), vec![(slot(1), 4), (slot(2), 2)]);
    }

    #[test]
    fn a_market_is_never_split_across_connections() {
        let mut a = TokenAssignment::new(5);
        a.assign(&markets(0..2));
        // One free place on c-0001 is not enough for a market.
        let added = a.assign(&[market(2)]);
        assert_eq!(added.keys().collect::<Vec<_>>(), vec![&slot(2)]);
        assert_eq!(added[&slot(2)].len(), 2);
    }

    #[test]
    fn removing_tokens_does_not_rebalance() {
        let mut a = with_loads(250, 120, 30);
        let before: HashSet<TokenId> = a.slots[&slot(2)].clone();
        let removed = a.unassign(&markets(0..100));
        assert_eq!(removed.keys().collect::<Vec<_>>(), vec![&slot(1)]);
        assert_eq!(removed[&slot(1)].len(), 200);
        assert_eq!(a.slots[&slot(2)], before);
    }

    #[test]
    fn empty_connection_is_forgotten_and_its_id_not_reused() {
        let mut a = TokenAssignment::new(250);
        a.assign(&[market(0)]);
        let removed = a.unassign(&[market(0)]);
        assert_eq!(removed[&slot(1)].len(), 2);
        assert!(a.slots.is_empty());

        let added = a.assign(&[market(1)]);
        assert_eq!(added.keys().collect::<Vec<_>>(), vec![&slot(2)]);
    }

    #[test]
    fn already_placed_and_unknown_tokens_are_ignored() {
        let mut a = TokenAssignment::new(250);
        a.assign(&[market(0)]);
        assert!(a.assign(&[market(0)]).is_empty());
        assert!(a.unassign(&[market(1)]).is_empty());
        assert_eq!(a.token_slot.len(), 2);
    }
}
