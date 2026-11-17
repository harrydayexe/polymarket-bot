//! Rolling traded volume of one market's pair of tokens (§3.6).
//!
//! The volume at `t` is the notional, in bought-outcome dollars (D2), of every print on
//! either token with exchange time in (t − window, t]. A transaction hash counts only on the
//! token it was first seen on, as in grouping (§3.3).
//!
//! Prints are kept a little longer than the window ([PairVolume::new]'s `slack`) so that
//! queries for slightly earlier times, as when groups close out of order, still see every
//! print they need. The caller feeds prints once each: overlap between a backfill and live
//! prints must be removed before they get here.

use std::{
    collections::{BTreeMap, HashMap},
    time::Duration,
};

use chrono::{DateTime, TimeDelta, Utc};
use marcasite::types::TokenId;
use rust_decimal::Decimal;

use crate::{
    invariant,
    invariant::InvariantViolation,
    normalize::print::{NormalisedPrint, TxHash},
    units::Usd,
};

/// The rolling volume of one market. Pure: time moves only through the arguments.
#[derive(Debug, Clone)]
pub struct PairVolume {
    window: TimeDelta,
    slack: TimeDelta,
    /// Notional by (exchange time, arrival order).
    prints: BTreeMap<(DateTime<Utc>, u64), Decimal>,
    /// Sum of `prints`.
    total: Decimal,
    next: u64,
    /// The token each hash was first seen on, and the latest exchange time it was seen at.
    first_token: HashMap<TxHash, (TokenId, DateTime<Utc>)>,
    /// Prints at or before this time have been evicted.
    evicted_through: Option<DateTime<Utc>>,
    /// The earliest time from which every print has been fed in.
    covered_from: Option<DateTime<Utc>>,
    duplicates: u64,
}

impl PairVolume {
    /// An empty window of `window` (24 h), keeping prints `slack` longer for late queries.
    ///
    /// # Errors
    ///
    /// If the window is zero or a duration does not fit a [TimeDelta].
    pub fn new(window: Duration, slack: Duration) -> Result<Self, InvariantViolation> {
        invariant!(
            !window.is_zero(),
            Err(InvariantViolation("the volume window is positive"))
        );
        let window = TimeDelta::from_std(window)
            .map_err(|_| InvariantViolation("the volume window fits a TimeDelta"))?;
        let slack = TimeDelta::from_std(slack)
            .map_err(|_| InvariantViolation("the volume slack fits a TimeDelta"))?;
        Ok(Self {
            window,
            slack,
            prints: BTreeMap::new(),
            total: Decimal::ZERO,
            next: 0,
            first_token: HashMap::new(),
            evicted_through: None,
            covered_from: None,
            duplicates: 0,
        })
    }

    /// Adds a print. Returns `false` if it was dropped as a duplicate of the same
    /// transaction on the sibling token, or is too old to matter.
    pub fn record(&mut self, print: &NormalisedPrint) -> bool {
        if self
            .evicted_through
            .is_some_and(|through| print.exchange_ts <= through)
        {
            return false;
        }
        if let Some(hash) = &print.tx_hash {
            match self.first_token.get_mut(hash) {
                Some((token, _)) if *token != print.printed_token => {
                    self.duplicates += 1;
                    return false;
                }
                Some((_, last)) => *last = (*last).max(print.exchange_ts),
                None => {
                    self.first_token.insert(
                        hash.clone(),
                        (print.printed_token.clone(), print.exchange_ts),
                    );
                }
            }
        }
        let notional = print.notional.value();
        self.prints.insert((print.exchange_ts, self.next), notional);
        self.next += 1;
        self.total += notional;
        true
    }

    /// Notes that every print from `from` on has been, or will be, fed in (after a complete
    /// backfill, or from the moment live prints started if there was none). Keeps the
    /// earliest such time.
    pub fn mark_covered_from(&mut self, from: DateTime<Utc>) {
        self.covered_from = Some(self.covered_from.map_or(from, |c| c.min(from)));
    }

    /// Whether the window ending at `at` is fully covered by prints fed in.
    pub fn window_complete(&self, at: DateTime<Utc>) -> bool {
        self.covered_from
            .is_some_and(|from| from <= at - self.window)
    }

    /// Prints dropped as sibling-token duplicates.
    pub fn duplicates(&self) -> u64 {
        self.duplicates
    }

    /// The volume in (at − window, at]. Exact; round the final result.
    ///
    /// # Errors
    ///
    /// If prints the window needs have already been evicted (`at` is more than the slack
    /// earlier than a time passed to [evict](Self::evict)).
    pub fn total(&self, at: DateTime<Utc>) -> Result<Usd, InvariantViolation> {
        let start = at - self.window;
        invariant!(
            self.evicted_through.is_none_or(|through| through <= start),
            Err(InvariantViolation(
                "the window's prints have not been evicted"
            ))
        );
        // Usually few prints are outside the window, so subtract them from the running total.
        let before: Decimal = self
            .prints
            .range(..=(start, u64::MAX))
            .map(|(_, n)| *n)
            .sum();
        let after: Decimal = self.prints.range((at, u64::MAX)..).map(|(_, n)| *n).sum();
        let total = self.total - before - after;
        invariant!(
            total >= Decimal::ZERO,
            Err(InvariantViolation("volume is not negative"))
        );
        Ok(Usd::new(total))
    }

    /// Forgets prints that no query at `now` − slack or later can need.
    pub fn evict(&mut self, now: DateTime<Utc>) {
        let through = now - self.window - self.slack;
        if self.evicted_through.is_some_and(|t| t >= through) {
            return;
        }
        let keep = self.prints.split_off(&(through, u64::MAX));
        let gone: Decimal = self.prints.values().copied().sum();
        self.prints = keep;
        self.total -= gone;
        self.first_token.retain(|_, (_, last)| *last > through);
        self.evicted_through = Some(through);
    }

    /// Prints currently held.
    pub fn len(&self) -> usize {
        self.prints.len()
    }

    pub fn is_empty(&self) -> bool {
        self.prints.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use marcasite::types::ConditionId;
    use rust_decimal_macros::dec;

    use super::*;
    use crate::{
        market::MarketTokens,
        normalize::print::{RawPrint, TakerSide},
        test_util::t,
        units::{Price, Shares},
    };

    const HOUR: i64 = 3_600_000;
    const DAY: i64 = 24 * HOUR;

    fn print(
        token: &str,
        side: TakerSide,
        price: Decimal,
        size: Decimal,
        tx: &str,
        ex: i64,
    ) -> NormalisedPrint {
        let raw = RawPrint {
            condition_id: ConditionId::new("0xc1"),
            token: TokenId::new(token),
            side,
            price: Price::new(price).unwrap(),
            size: Shares::new(size).unwrap(),
            tx_hash: Some(TxHash::new(tx)),
            exchange_ts: t(ex),
            recv_ts: t(ex + 50),
        };
        let tokens = MarketTokens {
            yes: TokenId::new("1111"),
            no: TokenId::new("1222"),
        };
        NormalisedPrint::new(&raw, &tokens).unwrap()
    }

    fn volume() -> PairVolume {
        PairVolume::new(Duration::from_hours(24), Duration::from_hours(1)).unwrap()
    }

    #[test]
    fn both_tokens_count_in_bought_outcome_dollars() {
        let mut v = volume();
        assert!(v.record(&print("1111", TakerSide::Buy, dec!(0.2), dec!(100), "a", 0)));
        // SELL YES at 0.82 is a buy of NO at 0.18: $18.
        assert!(v.record(&print(
            "1111",
            TakerSide::Sell,
            dec!(0.82),
            dec!(100),
            "b",
            10
        )));
        assert!(v.record(&print("1222", TakerSide::Buy, dec!(0.5), dec!(10), "c", 20)));
        assert_eq!(v.total(t(20)).unwrap(), Usd::new(dec!(43)));
    }

    #[test]
    fn a_shared_hash_counts_once_on_the_first_seen_token() {
        let mut v = volume();
        assert!(v.record(&print("1111", TakerSide::Buy, dec!(0.4), dec!(10), "a", 0)));
        assert!(v.record(&print("1111", TakerSide::Buy, dec!(0.41), dec!(10), "a", 0)));
        assert!(!v.record(&print("1222", TakerSide::Sell, dec!(0.6), dec!(10), "a", 0)));
        assert!(!v.record(&print(
            "1222",
            TakerSide::Sell,
            dec!(0.59),
            dec!(10),
            "a",
            0
        )));
        assert_eq!(v.total(t(0)).unwrap(), Usd::new(dec!(8.1)));
        assert_eq!(v.duplicates(), 2);
    }

    #[test]
    fn window_evicts_at_exactly_24_hours() {
        let mut v = volume();
        v.record(&print(
            "1111",
            TakerSide::Buy,
            dec!(0.5),
            dec!(10),
            "a",
            1000,
        ));
        v.record(&print(
            "1111",
            TakerSide::Buy,
            dec!(0.5),
            dec!(20),
            "b",
            2000,
        ));
        // (t − 24 h, t]: the print at 1000 is in at 1000 + 24 h − 1 ms, out at 1000 + 24 h.
        assert_eq!(v.total(t(1000 + DAY - 1)).unwrap(), Usd::new(dec!(15)));
        assert_eq!(v.total(t(1000 + DAY)).unwrap(), Usd::new(dec!(10)));
        assert_eq!(v.total(t(2000 + DAY)).unwrap(), Usd::ZERO);
        // A print after `at` is not counted.
        assert_eq!(v.total(t(1500)).unwrap(), Usd::new(dec!(5)));
        assert_eq!(v.total(t(999)).unwrap(), Usd::ZERO);
    }

    #[test]
    fn eviction_keeps_the_slack_and_refuses_queries_before_it() {
        let mut v = volume();
        v.record(&print("1111", TakerSide::Buy, dec!(0.5), dec!(10), "a", 0));
        v.record(&print(
            "1111",
            TakerSide::Buy,
            dec!(0.5),
            dec!(20),
            "b",
            2 * HOUR,
        ));
        v.evict(t(DAY + HOUR + 1));
        assert_eq!(v.len(), 1);
        // A query up to the slack earlier still sees everything it needs.
        assert_eq!(v.total(t(DAY + 1)).unwrap(), Usd::new(dec!(10)));
        assert!(v.total(t(DAY)).is_err());
        // An old print arriving now is ignored rather than corrupting the total.
        assert!(!v.record(&print("1111", TakerSide::Buy, dec!(0.5), dec!(1), "c", 0)));
        // Eviction never goes back.
        v.evict(t(0));
        assert_eq!(v.len(), 1);
    }

    #[test]
    fn eviction_forgets_old_hashes() {
        let mut v = volume();
        v.record(&print("1111", TakerSide::Buy, dec!(0.5), dec!(10), "a", 0));
        v.evict(t(DAY + HOUR + 1));
        assert!(v.is_empty());
        assert!(v.record(&print(
            "1222",
            TakerSide::Buy,
            dec!(0.5),
            dec!(10),
            "a",
            DAY + 2 * HOUR
        )));
    }

    #[test]
    fn completeness_needs_a_full_window_of_coverage() {
        let mut v = volume();
        assert!(!v.window_complete(t(DAY)));
        v.mark_covered_from(t(1000));
        assert!(!v.window_complete(t(DAY + 999)));
        assert!(v.window_complete(t(DAY + 1000)));
        // A later start does not undo earlier coverage.
        v.mark_covered_from(t(5000));
        assert!(v.window_complete(t(DAY + 1000)));
    }

    #[test]
    fn invalid_window_is_refused() {
        assert!(PairVolume::new(Duration::ZERO, Duration::ZERO).is_err());
    }
}
