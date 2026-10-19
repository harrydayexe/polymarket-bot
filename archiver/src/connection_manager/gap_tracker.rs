use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Utc};
use marcasite::types::TokenId;

use crate::messages::{ControlBody, GapReason};

#[derive(Debug, Clone, PartialEq, Eq)]
struct OpenGap {
    start: DateTime<Utc>,
    reason: GapReason,
}

/// The gaps currently open, per token. Turns the connections' gap actions into control
/// records, so a token's gaps always come in matched open/close pairs.
#[derive(Debug, Default)]
pub struct GapTracker {
    open_gaps: HashMap<TokenId, OpenGap>,
}

impl GapTracker {
    /// Opens a gap for the tokens that do not already have one. A token with an open gap keeps
    /// its earlier start.
    ///
    /// # Returns
    ///
    /// The `GapOpen` record to write, or `None` if every token already had an open gap.
    pub fn open(
        &mut self,
        tokens: &HashSet<TokenId>,
        start: DateTime<Utc>,
        reason: &GapReason,
    ) -> Option<ControlBody> {
        let opened: HashSet<TokenId> = tokens
            .iter()
            .filter(|t| !self.open_gaps.contains_key(*t))
            .cloned()
            .collect();
        if opened.is_empty() {
            return None;
        }
        for t in &opened {
            self.open_gaps.insert(
                t.clone(),
                OpenGap {
                    start,
                    reason: reason.clone(),
                },
            );
        }
        Some(ControlBody::GapOpen {
            tokens: opened,
            start,
            reason: reason.clone(),
        })
    }

    /// Closes the open gaps of `tokens`. Tokens without an open gap are ignored: connections
    /// close gaps whenever data may have resumed, whether or not one was open.
    ///
    /// # Returns
    ///
    /// The `GapClose` record to write, or `None` if none of the tokens had an open gap.
    pub fn close(
        &mut self,
        tokens: &HashSet<TokenId>,
        end: DateTime<Utc>,
        reason: &GapReason,
    ) -> Option<ControlBody> {
        let closed: HashSet<TokenId> = tokens
            .iter()
            .filter(|t| self.open_gaps.remove(*t).is_some())
            .cloned()
            .collect();
        if closed.is_empty() {
            return None;
        }
        Some(ControlBody::GapClose {
            tokens: closed,
            end,
            reason: reason.clone(),
        })
    }

    /// Closes every open gap.
    pub fn close_all(&mut self, end: DateTime<Utc>, reason: &GapReason) -> Option<ControlBody> {
        let tokens: HashSet<TokenId> = self.open_gaps.keys().cloned().collect();
        self.close(&tokens, end, reason)
    }
}

#[cfg(test)]
mod tests {
    use chrono::{TimeDelta, TimeZone};

    use super::*;

    fn tokens(ns: impl IntoIterator<Item = u32>) -> HashSet<TokenId> {
        ns.into_iter()
            .map(|n| TokenId::new(n.to_string()))
            .collect()
    }

    fn at(secs: i64) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 9, 25, 10, 0, 0).unwrap() + TimeDelta::seconds(secs)
    }

    #[test]
    fn simple_gap_is_one_matched_pair() {
        let mut gaps = GapTracker::default();
        let open = gaps.open(&tokens([1, 2]), at(0), &GapReason::HeartbeatTimeout);
        assert_eq!(
            open,
            Some(ControlBody::GapOpen {
                tokens: tokens([1, 2]),
                start: at(0),
                reason: GapReason::HeartbeatTimeout,
            })
        );
        assert_eq!(gaps.open_gaps.len(), 2);

        let close = gaps.close(&tokens([1, 2]), at(7), &GapReason::Resumed);
        assert_eq!(
            close,
            Some(ControlBody::GapClose {
                tokens: tokens([1, 2]),
                end: at(7),
                reason: GapReason::Resumed,
            })
        );
        assert_eq!(gaps.open_gaps.len(), 0);
    }

    #[test]
    fn double_open_keeps_the_earlier_start() {
        let mut gaps = GapTracker::default();
        gaps.open(&tokens([1]), at(0), &GapReason::HeartbeatTimeout);
        assert_eq!(
            gaps.open(&tokens([1]), at(30), &GapReason::QueueOverflow),
            None
        );
        // Only the token without a gap is opened.
        assert_eq!(
            gaps.open(&tokens([1, 2]), at(30), &GapReason::QueueOverflow),
            Some(ControlBody::GapOpen {
                tokens: tokens([2]),
                start: at(30),
                reason: GapReason::QueueOverflow,
            })
        );
        assert_eq!(gaps.open_gaps[&TokenId::new("1")].start, at(0));
    }

    #[test]
    fn close_without_open_writes_nothing() {
        let mut gaps = GapTracker::default();
        assert_eq!(gaps.close(&tokens([1]), at(0), &GapReason::Resumed), None);

        gaps.open(&tokens([1]), at(0), &GapReason::HeartbeatTimeout);
        assert_eq!(
            gaps.close(&tokens([1, 2]), at(5), &GapReason::Unsubscribed),
            Some(ControlBody::GapClose {
                tokens: tokens([1]),
                end: at(5),
                reason: GapReason::Unsubscribed,
            })
        );
    }

    #[test]
    fn open_gaps_are_closed_at_shutdown() {
        let mut gaps = GapTracker::default();
        gaps.open(&tokens([1, 2]), at(0), &GapReason::HeartbeatTimeout);
        gaps.open(&tokens([3]), at(4), &GapReason::QueueOverflow);
        assert_eq!(
            gaps.close_all(at(9), &GapReason::Shutdown),
            Some(ControlBody::GapClose {
                tokens: tokens([1, 2, 3]),
                end: at(9),
                reason: GapReason::Shutdown,
            })
        );
        assert_eq!(gaps.close_all(at(10), &GapReason::Shutdown), None);
    }
}
