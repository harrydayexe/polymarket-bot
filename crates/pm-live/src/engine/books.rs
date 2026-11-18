//! Per-token order books with a short history and a trust state (§3.4).
//!
//! Each token keeps a ring of book versions `(exchange_ts, recv_ts, Arc<OrderBook>)`, one per
//! applied `book` or `price_change`. Versions older than the window (by receive time) leave the
//! ring, except the newest: an initial snapshot carries the time of the book's last change,
//! which can be long before it arrives, and a quiet book must still have a "before" version.

use std::{collections::VecDeque, sync::Arc, time::Duration};

use chrono::TimeDelta;
use pm_core::{
    book::{BookSide, LevelChange, OrderBook},
    invariant,
    invariant::InvariantViolation,
    units::UtcMicros,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// One state of a token's book.
#[derive(Debug, Clone, PartialEq)]
pub struct BookVersion {
    /// The exchange timestamp of the message that produced it; `None` if it had none, in
    /// which case it is never used as a "before" version.
    pub exchange_ts: Option<UtcMicros>,
    pub recv_ts: UtcMicros,
    pub book: Arc<OrderBook>,
}

/// Why a token's book is not trusted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Distrust {
    /// No full `book` since the token was subscribed or its connection came up.
    AwaitingSnapshot,
    /// Its connection is down.
    Disconnected,
    /// Its connection failed to decode a message, which may have been a lost `price_change`.
    DecodeError,
}

impl Distrust {
    pub fn as_str(self) -> &'static str {
        match self {
            Distrust::AwaitingSnapshot => "awaiting_snapshot",
            Distrust::Disconnected => "disconnected",
            Distrust::DecodeError => "decode_error",
        }
    }
}

/// Ring settings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RingLimits {
    pub window: Duration,
    pub capacity: usize,
}

/// A token's book history and trust.
#[derive(Debug, Clone)]
pub struct TokenBook {
    ring: VecDeque<BookVersion>,
    distrust: Option<Distrust>,
    /// Since when the book has been untrusted (any reason).
    untrusted_since: Option<UtcMicros>,
    /// Whether the untrusted-for-too-long alarm has fired for this episode.
    pub(crate) alarmed: bool,
    /// Some message for this token had no usable exchange timestamp.
    no_exchange_ts: bool,
}

impl TokenBook {
    /// A token just subscribed: no book yet, untrusted until its first snapshot.
    pub fn new(now: UtcMicros) -> Self {
        Self {
            ring: VecDeque::new(),
            distrust: Some(Distrust::AwaitingSnapshot),
            untrusted_since: Some(now),
            alarmed: false,
            no_exchange_ts: false,
        }
    }

    /// The current book, if a snapshot has arrived.
    pub fn current(&self) -> Option<&BookVersion> {
        self.ring.back()
    }

    pub fn is_trusted(&self) -> bool {
        self.distrust.is_none()
    }

    /// Why the book is untrusted, or `None` if it is trusted.
    pub fn distrust(&self) -> Option<Distrust> {
        self.distrust
    }

    pub fn untrusted_since(&self) -> Option<UtcMicros> {
        self.untrusted_since
    }

    /// Whether a message for this token came without a usable exchange timestamp.
    pub fn no_exchange_ts(&self) -> bool {
        self.no_exchange_ts
    }

    /// The versions held, oldest first.
    pub fn versions(&self) -> impl Iterator<Item = &BookVersion> {
        self.ring.iter()
    }

    /// The newest version with an exchange timestamp strictly before `ts` (§3.4 "before").
    pub fn version_before(&self, ts: UtcMicros) -> Option<&BookVersion> {
        self.ring
            .iter()
            .rev()
            .find(|v| v.exchange_ts.is_some_and(|at| at < ts))
    }

    /// Whether a version with exchange timestamp at or after `ts` has been applied (§3.4
    /// "after").
    pub fn has_version_since(&self, ts: UtcMicros) -> bool {
        self.ring
            .iter()
            .rev()
            .any(|v| v.exchange_ts.is_some_and(|at| at >= ts))
    }

    /// Marks the book untrusted for `reason` from `at` (keeping the earlier start of an
    /// ongoing episode).
    pub fn distrust_from(&mut self, reason: Distrust, at: UtcMicros) {
        self.distrust = Some(reason);
        self.untrusted_since.get_or_insert(at);
    }

    /// A full snapshot: replaces the book and makes it trusted.
    pub fn apply_snapshot(
        &mut self,
        book: OrderBook,
        exchange_ts: Option<UtcMicros>,
        recv_ts: UtcMicros,
        limits: RingLimits,
    ) -> Result<(), InvariantViolation> {
        self.push(Arc::new(book), exchange_ts, recv_ts, limits)?;
        self.distrust = None;
        self.untrusted_since = None;
        self.alarmed = false;
        Ok(())
    }

    /// Level changes for this token from one `price_change` message. Returns `false` (and
    /// changes nothing) when there is no book to apply them to yet.
    pub fn apply_changes(
        &mut self,
        changes: &[LevelChange],
        exchange_ts: Option<UtcMicros>,
        recv_ts: UtcMicros,
        limits: RingLimits,
    ) -> Result<bool, InvariantViolation> {
        let Some(current) = self.ring.back() else {
            return Ok(false);
        };
        let mut book = OrderBook::clone(&current.book);
        for change in changes {
            invariant!(
                book.apply_in_place(change),
                Err(InvariantViolation(
                    "a level change applies to its own token's book"
                ))
            );
        }
        self.push(Arc::new(book), exchange_ts, recv_ts, limits)?;
        Ok(true)
    }

    /// A new tick size: a new version with the same levels.
    pub fn apply_tick_size(
        &mut self,
        tick_size: Decimal,
        exchange_ts: Option<UtcMicros>,
        recv_ts: UtcMicros,
        limits: RingLimits,
    ) -> Result<bool, InvariantViolation> {
        let Some(current) = self.ring.back() else {
            return Ok(false);
        };
        let book = current.book.with_tick_size(tick_size);
        self.push(Arc::new(book), exchange_ts, recv_ts, limits)?;
        Ok(true)
    }

    fn push(
        &mut self,
        book: Arc<OrderBook>,
        exchange_ts: Option<UtcMicros>,
        recv_ts: UtcMicros,
        limits: RingLimits,
    ) -> Result<(), InvariantViolation> {
        invariant!(
            self.ring.back().is_none_or(|last| last.recv_ts <= recv_ts),
            Err(InvariantViolation("book versions arrive in receive order"))
        );
        if exchange_ts.is_none() {
            self.no_exchange_ts = true;
        }
        self.ring.push_back(BookVersion {
            exchange_ts,
            recv_ts,
            book,
        });
        self.prune(recv_ts, limits);
        invariant!(
            !self.ring.is_empty() && self.ring.len() <= limits.capacity.max(1),
            Err(InvariantViolation(
                "the ring holds between 1 and capacity versions"
            ))
        );
        Ok(())
    }

    /// Drops versions received more than the window before `now` and any beyond the
    /// capacity, never the newest.
    pub fn prune(&mut self, now: UtcMicros, limits: RingLimits) {
        let window = TimeDelta::from_std(limits.window).unwrap_or(TimeDelta::MAX);
        let cutoff = now.checked_sub_signed(window);
        while self.ring.len() > 1
            && (self.ring.len() > limits.capacity
                || cutoff.is_some_and(|cut| self.ring[0].recv_ts < cut))
        {
            self.ring.pop_front();
        }
    }
}

/// A price level where a YES book and its NO book are not mirrors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MirrorMismatch {
    /// The side of the YES book.
    pub yes_side: BookSide,
    /// The YES price of the level.
    pub price: Decimal,
    /// Size on the YES book at `price`.
    pub yes_size: Option<Decimal>,
    /// Size on the NO book at `1 − price`, opposite side.
    pub no_size: Option<Decimal>,
}

/// Where the YES and NO books are not mirrors (§3.4): YES bids at p must equal NO asks at
/// 1 − p, and YES asks at p NO bids at 1 − p. Empty when they mirror.
pub fn mirror_mismatches(yes: &OrderBook, no: &OrderBook) -> Vec<MirrorMismatch> {
    let mut out = Vec::new();
    for (yes_side, no_side) in [
        (BookSide::Bid, BookSide::Ask),
        (BookSide::Ask, BookSide::Bid),
    ] {
        let mut prices: Vec<Decimal> = yes.levels(yes_side).map(|l| l.price).collect();
        prices.extend(no.levels(no_side).map(|l| Decimal::ONE - l.price));
        prices.sort();
        prices.dedup();
        for price in prices {
            let yes_size = yes.size_at(yes_side, price);
            let no_size = no.size_at(no_side, Decimal::ONE - price);
            if yes_size != no_size {
                out.push(MirrorMismatch {
                    yes_side,
                    price,
                    yes_size,
                    no_size,
                });
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use marcasite::types::TokenId;
    use marcasite::ws::BookEvent;
    use pm_core::test_util::t;
    use rust_decimal_macros::dec;

    use super::*;

    const LIMITS: RingLimits = RingLimits {
        window: Duration::from_secs(5),
        capacity: 256,
    };

    fn book(at_ms: i64) -> OrderBook {
        OrderBook::from_levels(
            TokenId::new("1"),
            [(dec!(0.40), dec!(100))],
            [(dec!(0.42), dec!(100))],
            t(at_ms),
            Some(dec!(0.01)),
        )
    }

    fn change(price: Decimal, size: Decimal, at_ms: i64) -> LevelChange {
        LevelChange {
            token: TokenId::new("1"),
            side: BookSide::Ask,
            price,
            size,
            at: t(at_ms),
        }
    }

    #[test]
    fn trusted_from_the_first_snapshot_until_distrusted() {
        let mut token = TokenBook::new(t(0));
        assert_eq!(token.distrust(), Some(Distrust::AwaitingSnapshot));
        assert_eq!(token.untrusted_since(), Some(t(0)));
        assert!(
            !token
                .apply_changes(&[change(dec!(0.42), dec!(1), 5)], Some(t(5)), t(5), LIMITS)
                .unwrap()
        );
        token
            .apply_snapshot(book(10), Some(t(10)), t(10), LIMITS)
            .unwrap();
        assert!(token.is_trusted());
        token.distrust_from(Distrust::Disconnected, t(20));
        token.distrust_from(Distrust::DecodeError, t(30));
        assert_eq!(token.distrust(), Some(Distrust::DecodeError));
        assert_eq!(token.untrusted_since(), Some(t(20)));
        token
            .apply_snapshot(book(40), Some(t(40)), t(40), LIMITS)
            .unwrap();
        assert!(token.is_trusted() && token.untrusted_since().is_none());
    }

    #[test]
    fn ring_holds_at_most_capacity_and_the_window() {
        let mut token = TokenBook::new(t(0));
        token
            .apply_snapshot(book(0), Some(t(0)), t(0), LIMITS)
            .unwrap();
        // 300 updates in one second.
        for i in 1..=300 {
            let at = i * 1000 / 300;
            token
                .apply_changes(
                    &[change(dec!(0.42), Decimal::from(i), at)],
                    Some(t(at)),
                    t(at),
                    LIMITS,
                )
                .unwrap();
        }
        assert_eq!(token.versions().count(), 256);
        // Six seconds later one more: everything older than 5 s goes, the newest stays.
        token
            .apply_changes(
                &[change(dec!(0.43), dec!(1), 7000)],
                Some(t(7000)),
                t(7000),
                LIMITS,
            )
            .unwrap();
        assert_eq!(token.versions().count(), 1);
        token.prune(t(60_000), LIMITS);
        assert_eq!(
            token.versions().count(),
            1,
            "the newest version is never pruned"
        );
        assert!(token.versions().all(|v| v.recv_ts >= t(2000)));
    }

    #[test]
    fn before_is_strictly_earlier_by_exchange_time() {
        let mut token = TokenBook::new(t(0));
        // An initial snapshot stamped long before it arrives.
        token
            .apply_snapshot(book(-9000), Some(t(-9000)), t(0), LIMITS)
            .unwrap();
        token
            .apply_changes(
                &[change(dec!(0.42), dec!(50), 100)],
                Some(t(100)),
                t(120),
                LIMITS,
            )
            .unwrap();
        token
            .apply_changes(&[change(dec!(0.42), dec!(0), 200)], None, t(220), LIMITS)
            .unwrap();
        assert!(token.no_exchange_ts());
        let before = token.version_before(t(100)).unwrap();
        assert_eq!(before.exchange_ts, Some(t(-9000)));
        let before = token.version_before(t(101)).unwrap();
        assert_eq!(
            before.book.size_at(BookSide::Ask, dec!(0.42)),
            Some(dec!(50))
        );
        // The version without a timestamp is never "before" anything.
        assert_eq!(
            token.version_before(t(10_000)).unwrap().exchange_ts,
            Some(t(100))
        );
        assert!(token.version_before(t(-9000)).is_none());
        assert!(token.has_version_since(t(100)));
        assert!(!token.has_version_since(t(101)));
    }

    #[test]
    fn live_yes_and_no_books_are_mirrors() {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../fixtures/books/mirror_yes_no.json"
        );
        let fixture: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
        let yes: BookEvent = serde_json::from_value(fixture["yes"].clone()).unwrap();
        let no: BookEvent = serde_json::from_value(fixture["no"].clone()).unwrap();
        let (yes, no) = (OrderBook::from_ws_book(&yes), OrderBook::from_ws_book(&no));
        assert!(
            yes.levels(BookSide::Bid).count() > 1 && yes.levels(BookSide::Ask).count() > 1,
            "a non-trivial book"
        );
        assert_eq!(mirror_mismatches(&yes, &no), vec![]);
        // A level that differs is reported.
        let mut doctored = no.clone();
        let best_ask = doctored.best_ask().unwrap();
        doctored.set_level(
            BookSide::Ask,
            best_ask.price,
            best_ask.size + Decimal::ONE,
            t(0),
        );
        let mismatches = mirror_mismatches(&yes, &doctored);
        assert_eq!(mismatches.len(), 1);
        assert_eq!(mismatches[0].yes_side, BookSide::Bid);
        assert_eq!(mismatches[0].price, Decimal::ONE - best_ask.price);
    }
}
