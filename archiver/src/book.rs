//! An order book rebuilt from archived data (design section 5.8), used by `verify` and meant
//! for later analysis.
//!
//! A book is built from a full book (a websocket `book` message or a REST snapshot) and moved
//! forward with level changes (the levels of `price_change` messages). A level change carries
//! the new total size at its price, so a size of 0 removes the level (the first assumption of
//! design section 16, which `verify` exists to check).
//!
//! Prices and sizes are normalised (`0.50` and `0.5` are the same level and print the same),
//! and a book never holds a level of size 0.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use marcasite::{
    types::{Side, TokenId},
    ws::{BookEvent, PriceChange},
};
use rust_decimal::Decimal;
use serde::Serialize;

use crate::archive::snapshot_book::SnapshotBook;

/// The two sides of a book.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BookSide {
    /// Buy orders (`BUY` in a `price_change`).
    Bid,
    /// Sell orders (`SELL` in a `price_change`).
    Ask,
}

impl BookSide {
    /// The side a `price_change` with this order side affects, or `None` for a side this
    /// version does not know.
    pub fn from_order_side(side: &Side) -> Option<Self> {
        match side {
            Side::Buy => Some(BookSide::Bid),
            Side::Sell => Some(BookSide::Ask),
            _ => None,
        }
    }
}

/// One price level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct Level {
    pub price: Decimal,
    pub size: Decimal,
}

/// A change to one level of one token's book: the new total size at `price` (0 removes the
/// level), at exchange time `at`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LevelChange {
    pub token: TokenId,
    pub side: BookSide,
    pub price: Decimal,
    pub size: Decimal,
    pub at: DateTime<Utc>,
}

/// A `price_change` whose side is neither `BUY` nor `SELL`.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("unknown side {0:?} in a price change")]
pub struct UnknownSide(pub String);

impl LevelChange {
    /// One entry of a `price_change` message; `at` is the message's timestamp.
    ///
    /// # Errors
    ///
    /// If the entry's side is not `BUY` or `SELL`.
    pub fn from_price_change(change: &PriceChange, at: DateTime<Utc>) -> Result<Self, UnknownSide> {
        let side = BookSide::from_order_side(&change.side)
            .ok_or_else(|| UnknownSide(change.side.to_string()))?;
        Ok(LevelChange {
            token: change.asset_id.clone(),
            side,
            price: change.price,
            size: change.size,
            at,
        })
    }
}

/// A level that differs between two books. `left` and `right` are the sizes in each book,
/// `None` where the book has no such level.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LevelDiff {
    pub side: BookSide,
    pub price: Decimal,
    pub left: Option<Decimal>,
    pub right: Option<Decimal>,
}

/// One token's order book at a moment.
///
/// `bids` and `asks` map price to size; the best bid is the highest bid price and the best ask
/// the lowest ask price. `as_of` is the exchange time of the book or of the last change
/// applied. Comparisons ([OrderBook::same_levels], [OrderBook::differences]) look at levels
/// only, never at `as_of` or the tick size.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderBook {
    token: TokenId,
    bids: BTreeMap<Decimal, Decimal>,
    asks: BTreeMap<Decimal, Decimal>,
    as_of: DateTime<Utc>,
    tick_size: Option<Decimal>,
}

impl OrderBook {
    /// A book with the given levels as `(price, size)`, in any order. Levels of size 0 are left
    /// out; when a price appears twice on one side, the later one wins.
    pub fn from_levels(
        token: TokenId,
        bids: impl IntoIterator<Item = (Decimal, Decimal)>,
        asks: impl IntoIterator<Item = (Decimal, Decimal)>,
        as_of: DateTime<Utc>,
        tick_size: Option<Decimal>,
    ) -> Self {
        let mut book = OrderBook {
            token,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            as_of,
            tick_size: tick_size.map(|t| t.normalize()),
        };
        for (price, size) in bids {
            book.set(BookSide::Bid, price, size);
        }
        for (price, size) in asks {
            book.set(BookSide::Ask, price, size);
        }
        book
    }

    /// The book of a websocket `book` message. Its levels need not be sorted.
    pub fn from_ws_book(event: &BookEvent) -> Self {
        Self::from_levels(
            event.asset_id.clone(),
            event.bids.iter().map(|l| (l.price, l.size)),
            event.asks.iter().map(|l| (l.price, l.size)),
            event.timestamp,
            event.tick_size,
        )
    }

    /// The book of one REST snapshot, or `None` if it has no timestamp (a book cannot be
    /// placed in time without one). Its levels need not be sorted.
    pub fn from_snapshot(book: &SnapshotBook) -> Option<Self> {
        Some(Self::from_levels(
            book.asset_id.clone(),
            book.bids.iter().map(|l| (l.price, l.size)),
            book.asks.iter().map(|l| (l.price, l.size)),
            book.timestamp?,
            book.tick_size,
        ))
    }

    pub fn token(&self) -> &TokenId {
        &self.token
    }

    pub fn as_of(&self) -> DateTime<Utc> {
        self.as_of
    }

    pub fn tick_size(&self) -> Option<Decimal> {
        self.tick_size
    }

    /// The bids, best (highest price) first.
    #[cfg_attr(not(test), allow(dead_code, reason = "for later analysis"))]
    pub fn bids(&self) -> impl Iterator<Item = Level> + '_ {
        self.bids
            .iter()
            .rev()
            .map(|(&price, &size)| Level { price, size })
    }

    /// The asks, best (lowest price) first.
    #[cfg_attr(not(test), allow(dead_code, reason = "for later analysis"))]
    pub fn asks(&self) -> impl Iterator<Item = Level> + '_ {
        self.asks
            .iter()
            .map(|(&price, &size)| Level { price, size })
    }

    #[cfg_attr(not(test), allow(dead_code, reason = "for later analysis"))]
    pub fn best_bid(&self) -> Option<Level> {
        self.bids().next()
    }

    #[cfg_attr(not(test), allow(dead_code, reason = "for later analysis"))]
    pub fn best_ask(&self) -> Option<Level> {
        self.asks().next()
    }

    /// The size at `price` on `side`, if there is such a level.
    #[cfg_attr(not(test), allow(dead_code, reason = "for later analysis"))]
    pub fn size_at(&self, side: BookSide, price: Decimal) -> Option<Decimal> {
        self.side(side).get(&price.normalize()).copied()
    }

    /// The book after `change`. A change for another token leaves the book as it is.
    #[cfg_attr(not(test), allow(dead_code, reason = "for later analysis"))]
    pub fn apply(&self, change: &LevelChange) -> OrderBook {
        let mut book = self.clone();
        book.apply_in_place(change);
        book
    }

    /// Applies `change` to this book. Returns whether it was for this book's token (a change
    /// for another token is ignored).
    pub fn apply_in_place(&mut self, change: &LevelChange) -> bool {
        if change.token != self.token {
            return false;
        }
        self.set_level(change.side, change.price, change.size, change.at);
        true
    }

    /// Sets the size at `price` on `side` (0 removes the level) as of exchange time `at`,
    /// without checking the token. The building block of [OrderBook::apply_in_place], for
    /// callers that keep changes without their token.
    pub fn set_level(&mut self, side: BookSide, price: Decimal, size: Decimal, at: DateTime<Utc>) {
        self.set(side, price, size);
        self.as_of = at;
    }

    /// The book with a new tick size. Levels are kept as they are: existing levels stay valid
    /// and later changes may use prices at the new tick.
    #[cfg_attr(not(test), allow(dead_code, reason = "for later analysis"))]
    pub fn with_tick_size(&self, tick_size: Decimal) -> OrderBook {
        let mut book = self.clone();
        book.set_tick_size(tick_size);
        book
    }

    pub fn set_tick_size(&mut self, tick_size: Decimal) {
        self.tick_size = Some(tick_size.normalize());
    }

    /// Whether both books have exactly the same levels (token, time and tick size are not
    /// compared).
    pub fn same_levels(&self, other: &OrderBook) -> bool {
        self.bids == other.bids && self.asks == other.asks
    }

    /// How many levels differ: present in one book only, or with a different size.
    #[cfg_attr(not(test), allow(dead_code, reason = "for later analysis"))]
    pub fn differing_levels(&self, other: &OrderBook) -> usize {
        self.differences(other).len()
    }

    /// The levels that differ, bids first, then by price. `left` is this book's size.
    pub fn differences(&self, other: &OrderBook) -> Vec<LevelDiff> {
        let mut diffs = vec![];
        for side in [BookSide::Bid, BookSide::Ask] {
            let (left, right) = (self.side(side), other.side(side));
            for (&price, &size) in left {
                if right.get(&price) != Some(&size) {
                    diffs.push(LevelDiff {
                        side,
                        price,
                        left: Some(size),
                        right: right.get(&price).copied(),
                    });
                }
            }
            for (&price, &size) in right {
                if !left.contains_key(&price) {
                    diffs.push(LevelDiff {
                        side,
                        price,
                        left: None,
                        right: Some(size),
                    });
                }
            }
        }
        diffs.sort_by_key(|d| (d.side, d.price));
        diffs
    }

    fn side(&self, side: BookSide) -> &BTreeMap<Decimal, Decimal> {
        match side {
            BookSide::Bid => &self.bids,
            BookSide::Ask => &self.asks,
        }
    }

    fn set(&mut self, side: BookSide, price: Decimal, size: Decimal) {
        let levels = match side {
            BookSide::Bid => &mut self.bids,
            BookSide::Ask => &mut self.asks,
        };
        let price = price.normalize();
        if size.is_zero() {
            levels.remove(&price);
        } else {
            levels.insert(price, size.normalize());
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use chrono::TimeZone;
    use rust_decimal_macros::dec;
    use serde_json::json;

    use super::*;

    pub(crate) fn t(ms: i64) -> DateTime<Utc> {
        Utc.timestamp_millis_opt(ms).unwrap()
    }

    fn token() -> TokenId {
        TokenId::new("111")
    }

    fn book(bids: &[(Decimal, Decimal)], asks: &[(Decimal, Decimal)]) -> OrderBook {
        OrderBook::from_levels(
            token(),
            bids.iter().copied(),
            asks.iter().copied(),
            t(1000),
            Some(dec!(0.01)),
        )
    }

    fn change(side: BookSide, price: Decimal, size: Decimal) -> LevelChange {
        LevelChange {
            token: token(),
            side,
            price,
            size,
            at: t(2000),
        }
    }

    fn levels(book: &OrderBook) -> (Vec<Level>, Vec<Level>) {
        (book.bids().collect(), book.asks().collect())
    }

    fn level(price: Decimal, size: Decimal) -> Level {
        Level { price, size }
    }

    // Book rebuild table (design 14.3): "Level update".
    #[test]
    fn level_update_replaces_the_size() {
        let before = book(&[(dec!(0.08), dec!(100))], &[]);
        let after = before.apply(&change(BookSide::Bid, dec!(0.08), dec!(150)));
        assert_eq!(after.best_bid(), Some(level(dec!(0.08), dec!(150))));
        assert_eq!(after.bids().count(), 1);
        assert_eq!(after.as_of(), t(2000));
        // `apply` returns a new book; the old one is unchanged.
        assert_eq!(before.best_bid(), Some(level(dec!(0.08), dec!(100))));
        assert_eq!(before.as_of(), t(1000));
    }

    // "New level".
    #[test]
    fn new_level_goes_in_price_order() {
        let before = book(
            &[(dec!(0.40), dec!(1)), (dec!(0.48), dec!(2))],
            &[(dec!(0.52), dec!(3)), (dec!(0.60), dec!(4))],
        );
        let after = before
            .apply(&change(BookSide::Bid, dec!(0.45), dec!(5)))
            .apply(&change(BookSide::Bid, dec!(0.49), dec!(6)))
            .apply(&change(BookSide::Ask, dec!(0.55), dec!(7)))
            .apply(&change(BookSide::Ask, dec!(0.51), dec!(8)));
        let (bids, asks) = levels(&after);
        assert_eq!(
            bids,
            [
                level(dec!(0.49), dec!(6)),
                level(dec!(0.48), dec!(2)),
                level(dec!(0.45), dec!(5)),
                level(dec!(0.40), dec!(1)),
            ]
        );
        assert_eq!(
            asks,
            [
                level(dec!(0.51), dec!(8)),
                level(dec!(0.52), dec!(3)),
                level(dec!(0.55), dec!(7)),
                level(dec!(0.60), dec!(4)),
            ]
        );
        assert_eq!(after.best_bid(), Some(level(dec!(0.49), dec!(6))));
        assert_eq!(after.best_ask(), Some(level(dec!(0.51), dec!(8))));
    }

    // "Level removed".
    #[test]
    fn size_zero_removes_the_level() {
        let before = book(
            &[(dec!(0.48), dec!(2)), (dec!(0.49), dec!(1))],
            &[(dec!(0.51), dec!(3))],
        );
        let after = before
            .apply(&change(BookSide::Bid, dec!(0.49), dec!(0)))
            .apply(&change(BookSide::Ask, dec!(0.51), dec!(0.000)));
        assert_eq!(after.best_bid(), Some(level(dec!(0.48), dec!(2))));
        assert_eq!(after.best_ask(), None);
        assert_eq!(after.size_at(BookSide::Bid, dec!(0.49)), None);
        // Removing a level that is not there changes nothing.
        let again = after.apply(&change(BookSide::Bid, dec!(0.30), dec!(0)));
        assert!(again.same_levels(&after));
    }

    // "Wrong token ignored".
    #[test]
    fn change_for_another_token_is_ignored() {
        let before = book(&[(dec!(0.08), dec!(100))], &[]);
        let mut other = change(BookSide::Bid, dec!(0.08), dec!(150));
        other.token = TokenId::new("222");
        let after = before.apply(&other);
        assert_eq!(after, before);
        let mut in_place = before.clone();
        assert!(!in_place.apply_in_place(&other));
        assert_eq!(in_place, before);
    }

    // "Tick size change": levels stay, and prices at the new tick can be added.
    #[test]
    fn tick_size_change_keeps_levels_and_allows_finer_prices() {
        let before = book(&[(dec!(0.50), dec!(10))], &[(dec!(0.52), dec!(5))]);
        let finer = before.with_tick_size(dec!(0.001));
        assert!(finer.same_levels(&before));
        assert_eq!(finer.tick_size(), Some(dec!(0.001)));
        let after = finer.apply(&change(BookSide::Bid, dec!(0.505), dec!(3)));
        assert_eq!(after.best_bid(), Some(level(dec!(0.505), dec!(3))));
        assert_eq!(after.size_at(BookSide::Bid, dec!(0.5)), Some(dec!(10)));
    }

    #[test]
    fn equal_decimals_are_the_same_level() {
        let a = book(&[(dec!(0.50), dec!(10.0))], &[]);
        let b = book(&[(dec!(0.5), dec!(10))], &[]);
        assert!(a.same_levels(&b));
        assert_eq!(a.differing_levels(&b), 0);
        // Normalised, so it also prints the same.
        assert_eq!(a.best_bid().unwrap().price.to_string(), "0.5");
        assert_eq!(a.best_bid().unwrap().size.to_string(), "10");
        let updated = a.apply(&change(BookSide::Bid, dec!(0.500), dec!(4)));
        assert_eq!(updated.bids().count(), 1);
        assert_eq!(updated.size_at(BookSide::Bid, dec!(0.5)), Some(dec!(4)));
    }

    #[test]
    fn zero_size_levels_are_not_kept_when_building() {
        let b = book(&[(dec!(0.5), dec!(0)), (dec!(0.4), dec!(1))], &[]);
        assert_eq!(b.bids().count(), 1);
    }

    #[test]
    fn comparison_counts_each_differing_level() {
        let a = book(
            &[(dec!(0.48), dec!(2)), (dec!(0.49), dec!(1))],
            &[(dec!(0.51), dec!(3))],
        );
        let b = book(
            &[(dec!(0.48), dec!(2)), (dec!(0.47), dec!(1))],
            &[(dec!(0.51), dec!(4))],
        );
        assert!(!a.same_levels(&b));
        assert_eq!(a.differing_levels(&b), 3);
        assert_eq!(
            a.differences(&b),
            [
                LevelDiff {
                    side: BookSide::Bid,
                    price: dec!(0.47),
                    left: None,
                    right: Some(dec!(1)),
                },
                LevelDiff {
                    side: BookSide::Bid,
                    price: dec!(0.49),
                    left: Some(dec!(1)),
                    right: None,
                },
                LevelDiff {
                    side: BookSide::Ask,
                    price: dec!(0.51),
                    left: Some(dec!(3)),
                    right: Some(dec!(4)),
                },
            ]
        );
        // Time and tick size are not compared.
        let later = a.with_tick_size(dec!(0.001));
        assert!(a.same_levels(&later));
    }

    #[test]
    fn a_level_on_the_other_side_is_a_difference() {
        let a = book(&[(dec!(0.5), dec!(1))], &[]);
        let b = book(&[], &[(dec!(0.5), dec!(1))]);
        assert_eq!(a.differing_levels(&b), 2);
    }

    #[test]
    fn built_from_an_unsorted_websocket_book() {
        let event: BookEvent = serde_json::from_value(json!({
            "asset_id":"111","market":"0xabc",
            "bids":[{"price":"0.47","size":"1"},{"price":"0.48","size":"30.5"},
                    {"price":"0.49","size":"20"}],
            "asks":[{"price":"0.53","size":"1"},{"price":"0.52","size":"25"},
                    {"price":"0.51","size":"219.217767"}],
            "timestamp":"1790348400120","hash":"0xh1","tick_size":"0.01"
        }))
        .unwrap();
        let b = OrderBook::from_ws_book(&event);
        assert_eq!(b.token(), &token());
        assert_eq!(b.as_of(), t(1_790_348_400_120));
        assert_eq!(b.tick_size(), Some(dec!(0.01)));
        assert_eq!(b.best_bid(), Some(level(dec!(0.49), dec!(20))));
        assert_eq!(b.best_ask(), Some(level(dec!(0.51), dec!(219.217767))));
        assert_eq!(b.bids().count(), 3);
        assert_eq!(b.asks().last(), Some(level(dec!(0.53), dec!(1))));
    }

    #[test]
    fn built_from_a_snapshot() {
        let mut value = json!({
            "market":"0xabc","asset_id":"111","timestamp":"1790348400120","hash":"abc",
            "bids":[{"price":"0.48","size":"1"},{"price":"0.49","size":"20"}],
            "asks":[{"price":"0.51","size":"5.000001"}],
            "min_order_size":"5","tick_size":"0.001","neg_risk":false,"last_trade_price":""
        });
        let b = OrderBook::from_snapshot(&SnapshotBook::parse(&value).unwrap()).unwrap();
        assert_eq!(b.as_of(), t(1_790_348_400_120));
        assert_eq!(b.tick_size(), Some(dec!(0.001)));
        assert_eq!(b.best_bid(), Some(level(dec!(0.49), dec!(20))));
        assert_eq!(b.best_ask(), Some(level(dec!(0.51), dec!(5.000001))));

        value["timestamp"] = json!("");
        assert_eq!(
            OrderBook::from_snapshot(&SnapshotBook::parse(&value).unwrap()),
            None
        );
    }

    #[test]
    fn level_changes_from_a_price_change() {
        let event: marcasite::ws::PriceChangeEvent = serde_json::from_value(json!({
            "market":"0xabc","timestamp":"1790348400121",
            "price_changes":[
                {"asset_id":"111","price":"0.5","size":"10","side":"BUY","hash":"h"},
                {"asset_id":"111","price":"0.6","size":"0","side":"SELL","hash":"h"},
                {"asset_id":"111","price":"0.6","size":"0","side":"HOLD","hash":"h"}]
        }))
        .unwrap();
        let at = event.timestamp;
        let changes: Vec<_> = event
            .price_changes
            .iter()
            .map(|c| LevelChange::from_price_change(c, at))
            .collect();
        assert_eq!(
            changes[0],
            Ok(LevelChange {
                token: token(),
                side: BookSide::Bid,
                price: dec!(0.5),
                size: dec!(10),
                at: t(1_790_348_400_121),
            })
        );
        assert_eq!(changes[1].as_ref().unwrap().side, BookSide::Ask);
        assert_eq!(changes[2], Err(UnknownSide("HOLD".into())));
    }
}
