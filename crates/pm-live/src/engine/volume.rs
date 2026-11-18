//! Trailing 24 h volume of each condition (§3.6), from live prints plus Data API backfills.
//!
//! Live prints start at `live_since`, the first time the condition's connection came up; a
//! backfill of `[now − window − slack, live_since + margin)` is then requested, so it always
//! overlaps live coverage. A backfilled print is kept iff its time is in the requested interval
//! and its transaction was not already seen live (so overlap is harmless, and every interval
//! gets [BACKFILL_MARGIN] either side: Data API times are whole seconds, and a loss is only
//! noticed some time after the last message). After a disconnect, a gap backfill covers the
//! time since the connection's last message; after a decode error (maybe a lost trade), the
//! moment of the error (further errors on the connection shortly after are coalesced into one
//! later backfill, and count as missing from the first of them until it is requested). Until a
//! backfill is merged its interval counts as missing; a failed one leaves a hole. The window is
//! complete only when no missing interval overlaps it.

use std::{collections::HashSet, time::Duration};

use chrono::TimeDelta;
use pm_core::{
    invariant,
    invariant::InvariantViolation,
    market::MarketTokens,
    normalize::{NormalisedPrint, PairVolume, RawPrint, TxHash},
    units::{Usd, UtcMicros},
};

use crate::event::BackfillPrint;
use marcasite::types::ConditionId;

/// What became of one backfill's rows.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MergeCounts {
    pub kept: u64,
    /// Already fed in live.
    pub seen_live: u64,
    /// Outside the requested interval.
    pub outside: u64,
    /// Not normalisable (a token of another market, a bad price).
    pub invalid: u64,
}

/// Added either side of every backfill interval. Data API times are whole seconds (a print up
/// to 1 s earlier than its exchange time) and a message is received some time after it was
/// sent, so one second is not enough on the early side.
pub const BACKFILL_MARGIN: TimeDelta = TimeDelta::seconds(3);

/// One condition's rolling volume and its coverage.
#[derive(Debug, Clone)]
pub struct ConditionVolume {
    volume: PairVolume,
    window: TimeDelta,
    slack: TimeDelta,
    live_since: Option<UtcMicros>,
    down_since: Option<UtcMicros>,
    /// Intervals no prints could be fetched for.
    holes: Vec<(UtcMicros, UtcMicros)>,
    /// Backfills requested and not merged yet.
    pending: Vec<(UtcMicros, UtcMicros)>,
    /// Messages were lost from here on; their backfill is not requested yet.
    suspect_since: Option<UtcMicros>,
}

/// A backfill the engine should request: `[from, until)`.
pub type BackfillInterval = (UtcMicros, UtcMicros);

impl ConditionVolume {
    pub fn new(window: Duration, slack: Duration) -> Result<Self, InvariantViolation> {
        Ok(Self {
            volume: PairVolume::new(window, slack)?,
            window: TimeDelta::from_std(window)
                .map_err(|_| InvariantViolation("the volume window fits a TimeDelta"))?,
            slack: TimeDelta::from_std(slack)
                .map_err(|_| InvariantViolation("the volume slack fits a TimeDelta"))?,
            live_since: None,
            down_since: None,
            holes: Vec::new(),
            pending: Vec::new(),
            suspect_since: None,
        })
    }

    /// The connection carrying this condition came up at `now`. Returns the backfill to
    /// request: the full window the first time, the gap after a disconnect, else nothing.
    pub fn connected(&mut self, now: UtcMicros, lookback: TimeDelta) -> Option<BackfillInterval> {
        let interval = match (self.live_since, self.down_since.take()) {
            (None, _) => {
                self.live_since = Some(now);
                self.volume.mark_covered_from(now);
                (now - lookback, now + BACKFILL_MARGIN)
            }
            (Some(_), Some(down)) => (down - BACKFILL_MARGIN, now + BACKFILL_MARGIN),
            (Some(_), None) => return None,
        };
        self.pending.push(interval);
        Some(interval)
    }

    /// The connection was lost; its last message came at `last_message`: prints from then
    /// until the next `connected` are missing.
    pub fn disconnected(&mut self, last_message: UtcMicros) {
        if self.live_since.is_some() && self.down_since.is_none() {
            self.down_since = Some(last_message);
        }
    }

    /// A message that might have been a trade was lost at `at`: the backfill to request.
    pub fn lost_message(&mut self, at: UtcMicros) -> Option<BackfillInterval> {
        if self.live_since.is_none() || self.down_since.is_some() {
            // Not live yet, or already down: a backfill will cover it.
            return None;
        }
        let interval = (at - BACKFILL_MARGIN, at + BACKFILL_MARGIN);
        self.pending.push(interval);
        Some(interval)
    }

    /// Further messages were lost from `at`; their backfill comes later, from
    /// [Self::lost_messages]. Until then the window counts as missing from `at`.
    pub fn suspect(&mut self, at: UtcMicros) {
        if self.live_since.is_some() {
            self.suspect_since.get_or_insert(at);
        }
    }

    /// Messages that might have been trades were lost between `from` and `until`: the
    /// backfill to request. Clears [Self::suspect].
    pub fn lost_messages(&mut self, from: UtcMicros, until: UtcMicros) -> Option<BackfillInterval> {
        self.suspect_since = None;
        // A gap backfill after a disconnect may overlap this one; prints merge by hash.
        self.live_since?;
        let interval = (from.min(until) - BACKFILL_MARGIN, until + BACKFILL_MARGIN);
        self.pending.push(interval);
        Some(interval)
    }

    /// A live print.
    pub fn record_live(&mut self, print: &NormalisedPrint) -> bool {
        self.volume.record(print)
    }

    /// Merges a backfill of `[from, until)`.
    pub fn merge_backfill(
        &mut self,
        condition: &ConditionId,
        tokens: &MarketTokens,
        prints: &[BackfillPrint],
        complete: bool,
        (from, until): BackfillInterval,
        recv_ts: UtcMicros,
    ) -> Result<MergeCounts, InvariantViolation> {
        invariant!(
            from <= until,
            Err(InvariantViolation("a backfill interval is not reversed"))
        );
        let mut counts = MergeCounts::default();
        // Transactions already held before this merge (fed in live, or by an earlier
        // backfill). Not checked against the volume as it fills: one transaction can have
        // several fills on a token, and all of them count.
        let held: HashSet<&TxHash> = prints
            .iter()
            .map(|print| &print.tx_hash)
            .filter(|hash| self.volume.has_hash(hash))
            .collect();
        // Oldest first, so the volume sees prints in time order.
        for print in prints.iter().rev() {
            if print.timestamp < from || print.timestamp >= until {
                counts.outside += 1;
                continue;
            }
            if held.contains(&print.tx_hash) {
                counts.seen_live += 1;
                continue;
            }
            let raw = RawPrint {
                condition_id: condition.clone(),
                token: print.token.clone(),
                side: print.side,
                price: print.price,
                size: print.size,
                tx_hash: Some(print.tx_hash.clone()),
                exchange_ts: print.timestamp,
                recv_ts,
            };
            match NormalisedPrint::new(&raw, tokens) {
                Ok(normalised) => {
                    if self.volume.record(&normalised) {
                        counts.kept += 1;
                    } else {
                        counts.outside += 1;
                    }
                }
                Err(_) => counts.invalid += 1,
            }
        }
        if let Some(index) = self.pending.iter().position(|&p| p == (from, until)) {
            self.pending.swap_remove(index);
        }
        if !complete {
            self.add_hole((from, until));
        }
        // Coverage starts at the earliest `from` (the initial backfill's); failed intervals are
        // holes, checked separately.
        self.volume.mark_covered_from(from);
        invariant!(
            counts.kept + counts.seen_live + counts.outside + counts.invalid == prints.len() as u64,
            Err(InvariantViolation("every backfilled row is accounted for"))
        );
        Ok(counts)
    }

    /// The volume in `(at − window, at]` and whether that window is fully covered.
    pub fn total(&self, at: UtcMicros) -> Result<(Usd, bool), InvariantViolation> {
        let total = self.volume.total(at)?;
        let start = at - self.window;
        let holes_clear = self
            .holes
            .iter()
            .chain(&self.pending)
            .all(|&(from, until)| until <= start || from > at);
        let down_clear = self.down_since.is_none_or(|down| down > at);
        let suspect_clear = self.suspect_since.is_none_or(|since| since > at);
        Ok((
            total,
            self.volume.window_complete(at) && holes_clear && down_clear && suspect_clear,
        ))
    }

    /// Forgets prints and holes no query at `now` − slack or later can need.
    pub fn evict(&mut self, now: UtcMicros) {
        self.volume.evict(now);
        let start = now - self.slack - self.window;
        self.holes.retain(|&(_, until)| until > start);
        // A backfill that never came back (its task stopped) is a hole, not pending forever.
        let stale = now - TimeDelta::hours(1);
        let (old, recent): (Vec<_>, Vec<_>) =
            self.pending.iter().partition(|&&(_, until)| until < stale);
        for interval in old {
            self.add_hole(interval);
        }
        self.pending = recent;
    }

    /// Adds a hole, merged with any it overlaps or touches, so repeated failures over the same
    /// stretch do not pile up.
    fn add_hole(&mut self, (mut from, mut until): BackfillInterval) {
        self.holes.retain(|&(f, u)| {
            let overlaps = f <= until && from <= u;
            if overlaps {
                from = from.min(f);
                until = until.max(u);
            }
            !overlaps
        });
        self.holes.push((from, until));
    }
}

#[cfg(test)]
mod tests {
    use marcasite::types::TokenId;
    use pm_core::{
        normalize::{TakerSide, TxHash},
        test_util::t,
        units::{Price, Shares},
    };
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use super::*;

    const HOUR: i64 = 3_600_000;

    fn tokens() -> MarketTokens {
        MarketTokens {
            yes: TokenId::new("1111"),
            no: TokenId::new("1222"),
        }
    }

    fn row(token: &str, price: Decimal, size: Decimal, hash: &str, at_ms: i64) -> BackfillPrint {
        BackfillPrint {
            token: TokenId::new(token),
            side: TakerSide::Buy,
            price: Price::new(price).unwrap(),
            size: Shares::new(size).unwrap(),
            tx_hash: TxHash::new(hash),
            timestamp: t(at_ms),
        }
    }

    fn live(hash: &str, size: Decimal, at_ms: i64) -> NormalisedPrint {
        let raw = RawPrint {
            condition_id: ConditionId::new("0xc1"),
            token: TokenId::new("1111"),
            side: TakerSide::Buy,
            price: Price::new(dec!(0.5)).unwrap(),
            size: Shares::new(size).unwrap(),
            tx_hash: Some(TxHash::new(hash)),
            exchange_ts: t(at_ms),
            recv_ts: t(at_ms),
        };
        NormalisedPrint::new(&raw, &tokens()).unwrap()
    }

    fn volume() -> ConditionVolume {
        ConditionVolume::new(Duration::from_hours(24), Duration::from_secs(600)).unwrap()
    }

    fn lookback() -> TimeDelta {
        TimeDelta::hours(24) + TimeDelta::minutes(10)
    }

    #[test]
    fn backfill_fills_the_window_without_double_counting_live_prints() {
        let mut v = volume();
        let now = 30 * HOUR;
        let (from, until) = v.connected(t(now), lookback()).unwrap();
        assert_eq!(until, t(now) + BACKFILL_MARGIN);
        assert!(
            !v.total(t(now)).unwrap().1,
            "incomplete until the backfill is merged"
        );
        assert!(v.record_live(&live("live", dec!(10), now + 100)));
        // Newest first, as the Data API returns them.
        let rows = [
            row("1111", dec!(0.5), dec!(10), "live", now + 100), // seen live
            row("1111", dec!(0.5), dec!(1), "late", now + 3000), // after the margin: outside
            row("1111", dec!(0.5), dec!(4), "b", now - 1000),
            row("1222", dec!(0.25), dec!(8), "c", now - 2 * HOUR),
            row("1111", dec!(0.5), dec!(100), "old", now - 25 * HOUR), // before from
        ];
        let counts = v
            .merge_backfill(
                &ConditionId::new("0xc1"),
                &tokens(),
                &rows,
                true,
                (from, until),
                t(now + 2000),
            )
            .unwrap();
        assert_eq!(
            counts,
            MergeCounts {
                kept: 2,
                seen_live: 1,
                outside: 2,
                invalid: 0
            }
        );
        // 10 × 0.5 live + 4 × 0.5 + 8 × 0.25 backfilled.
        let (total, complete) = v.total(t(now + 2000)).unwrap();
        assert_eq!(total, Usd::new(dec!(9)));
        assert!(complete);
    }

    #[test]
    fn a_row_seen_live_inside_the_interval_is_dropped() {
        let mut v = volume();
        let now = 30 * HOUR;
        let interval = v.connected(t(now), lookback()).unwrap();
        // A print stamped (to the second) just before live_since but seen live.
        assert!(v.record_live(&live("x", dec!(10), now - 300)));
        let rows = [row("1111", dec!(0.5), dec!(10), "x", now - 1000)];
        let counts = v
            .merge_backfill(
                &ConditionId::new("0xc1"),
                &tokens(),
                &rows,
                true,
                interval,
                t(now),
            )
            .unwrap();
        assert_eq!(counts.seen_live, 1);
        assert_eq!(v.total(t(now)).unwrap().0, Usd::new(dec!(5)));
    }

    #[test]
    fn coverage_needs_a_full_window_and_no_holes() {
        let mut v = volume();
        let now = 30 * HOUR;
        assert!(!v.total(t(now)).unwrap().1, "nothing covered yet");
        let interval = v.connected(t(now), lookback()).unwrap();
        v.merge_backfill(
            &ConditionId::new("0xc1"),
            &tokens(),
            &[],
            false,
            interval,
            t(now),
        )
        .unwrap();
        assert!(!v.total(t(now)).unwrap().1, "the failed backfill is a hole");
        // Once the hole has left the window, live prints alone cover it.
        assert!(
            !v.total(t(now + 24 * HOUR + 2999)).unwrap().1,
            "the hole runs to the margin"
        );
        assert!(v.total(t(now + 24 * HOUR + 3000)).unwrap().1);
    }

    #[test]
    fn a_disconnect_asks_for_the_gap() {
        let mut v = volume();
        let now = 30 * HOUR;
        let interval = v.connected(t(now), lookback()).unwrap();
        v.merge_backfill(
            &ConditionId::new("0xc1"),
            &tokens(),
            &[],
            true,
            interval,
            t(now),
        )
        .unwrap();
        assert!(
            v.connected(t(now + 10), lookback()).is_none(),
            "no gap, no backfill"
        );
        // The last message came at +1 h; the loss was noticed 30 s later.
        v.disconnected(t(now + HOUR));
        assert!(
            !v.total(t(now + HOUR + 10)).unwrap().1,
            "incomplete while down"
        );
        let gap = v.connected(t(now + HOUR + 35_000), lookback()).unwrap();
        assert_eq!(gap, (t(now + HOUR - 3000), t(now + HOUR + 38_000)));
        assert!(
            !v.total(t(now + HOUR + 36_000)).unwrap().1,
            "incomplete until the gap is merged"
        );
        let rows = [row("1111", dec!(0.5), dec!(2), "g", now + HOUR + 1000)];
        let counts = v
            .merge_backfill(
                &ConditionId::new("0xc1"),
                &tokens(),
                &rows,
                true,
                gap,
                t(now + HOUR + 39_000),
            )
            .unwrap();
        assert_eq!(counts.kept, 1);
        let (total, complete) = v.total(t(now + HOUR + 39_000)).unwrap();
        assert_eq!(total, Usd::new(dec!(1)));
        assert!(complete);
    }

    #[test]
    fn a_lost_message_asks_for_a_small_backfill() {
        let mut v = volume();
        let now = 30 * HOUR;
        let c = ConditionId::new("0xc1");
        assert!(v.lost_message(t(now)).is_none(), "not live yet");
        let interval = v.connected(t(now), lookback()).unwrap();
        v.merge_backfill(&c, &tokens(), &[], true, interval, t(now))
            .unwrap();
        let lost = v.lost_message(t(now + 5000)).unwrap();
        assert_eq!(lost, (t(now + 2000), t(now + 8000)));
        assert!(!v.total(t(now + 6000)).unwrap().1);
        v.merge_backfill(&c, &tokens(), &[], true, lost, t(now + 9000))
            .unwrap();
        assert!(v.total(t(now + 9000)).unwrap().1);
    }

    #[test]
    fn coalesced_losses_count_as_missing_until_their_backfill_is_merged() {
        let mut v = volume();
        let now = 30 * HOUR;
        let c = ConditionId::new("0xc1");
        let interval = v.connected(t(now), lookback()).unwrap();
        v.merge_backfill(&c, &tokens(), &[], true, interval, t(now))
            .unwrap();
        v.suspect(t(now + 5000));
        v.suspect(t(now + 6000));
        assert!(
            v.total(t(now + 4000)).unwrap().1,
            "complete before the loss"
        );
        assert!(!v.total(t(now + 7000)).unwrap().1);
        let lost = v.lost_messages(t(now + 5000), t(now + 9000)).unwrap();
        assert_eq!(lost, (t(now + 2000), t(now + 12_000)));
        v.merge_backfill(&c, &tokens(), &[], true, lost, t(now + 13_000))
            .unwrap();
        assert!(v.total(t(now + 13_000)).unwrap().1);
    }

    #[test]
    fn every_fill_of_a_backfilled_transaction_counts() {
        let mut v = volume();
        let now = 30 * HOUR;
        let interval = v.connected(t(now), lookback()).unwrap();
        // One sweep: two fills on the same token in one transaction.
        let rows = [
            row("1111", dec!(0.41), dec!(10), "sweep", now - 1000),
            row("1111", dec!(0.40), dec!(10), "sweep", now - 1000),
        ];
        let counts = v
            .merge_backfill(
                &ConditionId::new("0xc1"),
                &tokens(),
                &rows,
                true,
                interval,
                t(now),
            )
            .unwrap();
        assert_eq!(counts.kept, 2);
        assert_eq!(v.total(t(now)).unwrap().0, Usd::new(dec!(8.1)));
    }

    #[test]
    fn holes_are_kept_for_queries_back_to_the_slack_and_merge() {
        let mut v = volume();
        let now = 30 * HOUR;
        let c = ConditionId::new("0xc1");
        let interval = v.connected(t(now), lookback()).unwrap();
        v.merge_backfill(&c, &tokens(), &[], true, interval, t(now))
            .unwrap();
        let lost = v.lost_message(t(now + HOUR)).unwrap();
        v.merge_backfill(&c, &tokens(), &[], false, lost, t(now + HOUR))
            .unwrap();
        let again = v.lost_message(t(now + HOUR + 1000)).unwrap();
        v.merge_backfill(&c, &tokens(), &[], false, again, t(now + HOUR))
            .unwrap();
        assert_eq!(v.holes.len(), 1, "overlapping holes merge");
        // Evicting at +25 h 5 min must keep what a query at +25 h (within the slack) needs.
        assert!(!v.total(t(now + 25 * HOUR)).unwrap().1);
        v.evict(t(now + 25 * HOUR + 5 * 60_000));
        assert!(!v.total(t(now + 25 * HOUR)).unwrap().1);
    }

    #[test]
    fn a_backfill_that_never_returns_becomes_a_hole() {
        let mut v = volume();
        let now = 30 * HOUR;
        v.connected(t(now), lookback()).unwrap();
        v.evict(t(now + 2 * HOUR));
        assert!(!v.total(t(now + 2 * HOUR)).unwrap().1);
        assert!(
            v.total(t(now + 25 * HOUR)).unwrap().1,
            "until it leaves the window"
        );
    }
}
