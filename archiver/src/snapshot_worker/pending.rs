use std::collections::{HashMap, HashSet, VecDeque};

use marcasite::types::TokenId;

use crate::messages::SnapshotReason;

/// Tokens waiting for a snapshot, first in first out, each at most once.
#[derive(Debug, Default)]
pub struct PendingQueue {
    order: VecDeque<TokenId>,
    reasons: HashMap<TokenId, SnapshotReason>,
}

/// Tokens to request together. All share one reason, so the snapshot record can carry it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Batch {
    pub tokens: Vec<TokenId>,
    pub reason: SnapshotReason,
}

impl PendingQueue {
    pub fn is_empty(&self) -> bool {
        self.order.is_empty()
    }

    /// Queues `tokens` behind those already waiting. A token already waiting keeps its place
    /// and its reason.
    pub fn push(&mut self, tokens: impl IntoIterator<Item = TokenId>, reason: &SnapshotReason) {
        for token in tokens {
            if !self.reasons.contains_key(&token) {
                self.reasons.insert(token.clone(), reason.clone());
                self.order.push_back(token);
            }
        }
    }

    /// Takes up to `max` tokens sharing the reason of the token at the front, in queue order.
    /// Tokens with other reasons keep their places, so the front token always goes next and
    /// nothing waits for ever.
    pub fn take_batch(&mut self, max: usize) -> Option<Batch> {
        let reason = self.reasons.get(self.order.front()?)?.clone();
        let mut tokens = Vec::new();
        self.order.retain(|token| {
            if tokens.len() < max && self.reasons[token] == reason {
                tokens.push(token.clone());
                false
            } else {
                true
            }
        });
        for token in &tokens {
            self.reasons.remove(token);
        }
        Some(Batch { tokens, reason })
    }

    /// Drops `tokens` from the queue.
    pub fn remove(&mut self, tokens: &HashSet<TokenId>) {
        self.order.retain(|token| !tokens.contains(token));
        self.reasons.retain(|token, _| !tokens.contains(token));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn token(n: usize) -> TokenId {
        TokenId::new(format!("t{n:05}"))
    }

    fn tokens(ns: impl IntoIterator<Item = usize>) -> Vec<TokenId> {
        ns.into_iter().map(token).collect()
    }

    #[test]
    fn batches_of_at_most_max_in_order() {
        let mut queue = PendingQueue::default();
        queue.push(tokens(0..1200), &SnapshotReason::Subscribe);

        let sizes: Vec<usize> = std::iter::from_fn(|| queue.take_batch(500))
            .map(|batch| {
                assert_eq!(batch.reason, SnapshotReason::Subscribe);
                batch.tokens.len()
            })
            .collect();
        assert_eq!(sizes, [500, 500, 200]);
        assert!(queue.is_empty());
    }

    #[test]
    fn duplicates_are_queued_once() {
        let mut queue = PendingQueue::default();
        queue.push(tokens([1, 2]), &SnapshotReason::Subscribe);
        queue.push(tokens([1]), &SnapshotReason::NewToken);
        queue.push(tokens([2, 1, 3]), &SnapshotReason::Subscribe);

        assert_eq!(
            queue.take_batch(500),
            Some(Batch {
                tokens: tokens([1, 2, 3]),
                reason: SnapshotReason::Subscribe,
            })
        );
        assert_eq!(queue.take_batch(500), None);
    }

    #[test]
    fn a_batch_shares_the_front_tokens_reason() {
        let mut queue = PendingQueue::default();
        queue.push(tokens([1]), &SnapshotReason::Scheduled);
        queue.push(tokens([2]), &SnapshotReason::NewToken);
        queue.push(tokens([3, 4]), &SnapshotReason::Scheduled);
        queue.push(tokens([5]), &SnapshotReason::NewToken);

        assert_eq!(
            queue.take_batch(2),
            Some(Batch {
                tokens: tokens([1, 3]),
                reason: SnapshotReason::Scheduled,
            })
        );
        assert_eq!(
            queue.take_batch(500),
            Some(Batch {
                tokens: tokens([2, 5]),
                reason: SnapshotReason::NewToken,
            })
        );
        assert_eq!(
            queue.take_batch(500),
            Some(Batch {
                tokens: tokens([4]),
                reason: SnapshotReason::Scheduled,
            })
        );
    }

    #[test]
    fn removed_tokens_leave_the_queue() {
        let mut queue = PendingQueue::default();
        queue.push(tokens([1, 2, 3]), &SnapshotReason::Subscribe);
        queue.remove(&tokens([2]).into_iter().collect());

        assert_eq!(queue.take_batch(500).unwrap().tokens, tokens([1, 3]));

        // A removed token can be queued again.
        queue.push(tokens([2]), &SnapshotReason::NewToken);
        assert_eq!(queue.take_batch(500).unwrap().tokens, tokens([2]));
    }
}
