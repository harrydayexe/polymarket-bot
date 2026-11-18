//! Which arrives first for one trade: its `price_change` or its `last_trade_price` (D3 depends
//! on the answer).
//!
//! A trade on token T at price p is matched to the `price_change` entry on T at p nearest in
//! exchange time, within [MATCH_WINDOW]. Each trade yields one observation: the arrival order
//! and the exchange-time difference (`price_change` minus trade), or `unmatched` if no change
//! arrived within the window either side.

use std::collections::{BTreeMap, VecDeque};

use chrono::TimeDelta;
use marcasite::types::TokenId;
use pm_core::units::UtcMicros;
use rust_decimal::Decimal;

/// How far apart in exchange time, and how long in receive time, a trade and its change may be.
pub const MATCH_WINDOW: TimeDelta = TimeDelta::seconds(2);

/// The order a trade and its level change arrived in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Arrival {
    PriceChangeFirst,
    TradeFirst,
    Unmatched,
}

impl Arrival {
    pub fn as_str(self) -> &'static str {
        match self {
            Arrival::PriceChangeFirst => "price_change_first",
            Arrival::TradeFirst => "trade_first",
            Arrival::Unmatched => "unmatched",
        }
    }
}

/// One trade's outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Observation {
    pub arrival: Arrival,
    /// `price_change` exchange time minus trade exchange time; `None` when unmatched.
    pub exchange_delta: Option<TimeDelta>,
    /// When the observation was made (for the metric's time).
    pub at: UtcMicros,
}

#[derive(Debug, Clone)]
struct Seen {
    price: Decimal,
    exchange_ts: UtcMicros,
    recv_ts: UtcMicros,
}

/// Recent unmatched changes and trades, per token.
#[derive(Debug, Clone, Default)]
pub struct OrderingTracker {
    changes: BTreeMap<TokenId, VecDeque<Seen>>,
    trades: BTreeMap<TokenId, VecDeque<Seen>>,
}

fn take_nearest(
    queue: &mut VecDeque<Seen>,
    price: Decimal,
    exchange_ts: UtcMicros,
) -> Option<Seen> {
    let (index, _) = queue
        .iter()
        .enumerate()
        .filter(|(_, seen)| {
            seen.price == price && (seen.exchange_ts - exchange_ts).abs() <= MATCH_WINDOW
        })
        .min_by_key(|(_, seen)| (seen.exchange_ts - exchange_ts).abs())?;
    queue.remove(index)
}

impl OrderingTracker {
    /// A `price_change` entry on `token` at `price`. Returns an observation if it completes a
    /// waiting trade.
    pub fn on_change(
        &mut self,
        token: &TokenId,
        price: Decimal,
        exchange_ts: UtcMicros,
        recv_ts: UtcMicros,
    ) -> Option<Observation> {
        if let Some(trades) = self.trades.get_mut(token)
            && let Some(trade) = take_nearest(trades, price, exchange_ts)
        {
            return Some(Observation {
                arrival: Arrival::TradeFirst,
                exchange_delta: Some(exchange_ts - trade.exchange_ts),
                at: recv_ts,
            });
        }
        self.changes
            .entry(token.clone())
            .or_default()
            .push_back(Seen {
                price,
                exchange_ts,
                recv_ts,
            });
        None
    }

    /// A trade on `token` at `price`. Returns an observation if a change for it already came.
    pub fn on_trade(
        &mut self,
        token: &TokenId,
        price: Decimal,
        exchange_ts: UtcMicros,
        recv_ts: UtcMicros,
    ) -> Option<Observation> {
        if let Some(changes) = self.changes.get_mut(token)
            && let Some(change) = take_nearest(changes, price, exchange_ts)
        {
            return Some(Observation {
                arrival: Arrival::PriceChangeFirst,
                exchange_delta: Some(change.exchange_ts - exchange_ts),
                at: recv_ts,
            });
        }
        self.trades
            .entry(token.clone())
            .or_default()
            .push_back(Seen {
                price,
                exchange_ts,
                recv_ts,
            });
        None
    }

    /// Forgets changes and trades received more than [MATCH_WINDOW] before `now`; each
    /// forgotten trade is `unmatched`.
    pub fn expire(&mut self, now: UtcMicros) -> Vec<Observation> {
        let cutoff = now - MATCH_WINDOW;
        for queue in self.changes.values_mut() {
            while queue.front().is_some_and(|seen| seen.recv_ts < cutoff) {
                queue.pop_front();
            }
        }
        self.changes.retain(|_, queue| !queue.is_empty());
        let mut out = Vec::new();
        for queue in self.trades.values_mut() {
            while queue.front().is_some_and(|seen| seen.recv_ts < cutoff) {
                queue.pop_front();
                out.push(Observation {
                    arrival: Arrival::Unmatched,
                    exchange_delta: None,
                    at: now,
                });
            }
        }
        self.trades.retain(|_, queue| !queue.is_empty());
        out
    }

    /// Forgets a token that left the universe.
    pub fn forget(&mut self, token: &TokenId) {
        self.changes.remove(token);
        self.trades.remove(token);
    }
}

#[cfg(test)]
mod tests {
    use pm_core::test_util::t;
    use rust_decimal_macros::dec;

    use super::*;

    #[test]
    fn change_then_trade() {
        let mut tracker = OrderingTracker::default();
        let token = TokenId::new("1");
        assert!(
            tracker
                .on_change(&token, dec!(0.42), t(1000), t(1010))
                .is_none()
        );
        assert!(
            tracker
                .on_change(&token, dec!(0.43), t(1000), t(1010))
                .is_none()
        );
        let seen = tracker
            .on_trade(&token, dec!(0.42), t(1003), t(1020))
            .unwrap();
        assert_eq!(seen.arrival, Arrival::PriceChangeFirst);
        assert_eq!(seen.exchange_delta, Some(TimeDelta::milliseconds(-3)));
        assert!(
            tracker.expire(t(10_000)).is_empty(),
            "leftover changes are not observations"
        );
    }

    #[test]
    fn trade_then_change_and_unmatched() {
        let mut tracker = OrderingTracker::default();
        let token = TokenId::new("1");
        assert!(
            tracker
                .on_trade(&token, dec!(0.42), t(1000), t(1010))
                .is_none()
        );
        assert!(
            tracker
                .on_trade(&token, dec!(0.50), t(1000), t(1010))
                .is_none()
        );
        let seen = tracker
            .on_change(&token, dec!(0.42), t(1001), t(1015))
            .unwrap();
        assert_eq!(seen.arrival, Arrival::TradeFirst);
        assert_eq!(seen.exchange_delta, Some(TimeDelta::milliseconds(1)));
        // A change far away in exchange time does not match.
        assert!(
            tracker
                .on_change(&token, dec!(0.50), t(9000), t(9000))
                .is_none()
        );
        let expired = tracker.expire(t(3011));
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].arrival, Arrival::Unmatched);
    }
}
