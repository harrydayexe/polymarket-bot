//! The pure part of `verify`: rebuilds each token's book window by window from records given
//! in archive order, and compares it with the next full book.
//!
//! Every full book of a token (websocket `book` or snapshot book) is an anchor. The window from
//! one anchor to the token's next full book (the target) is checked by applying the token's
//! level changes recorded after the anchor and before the target, in archive order, to the
//! anchor, and comparing the result with the target. The target then becomes the next anchor.
//!
//! A REST snapshot is taken apart from the stream, so archive order does not say which changes
//! it includes. Its exchange timestamp does instead:
//!
//! - **Snapshot target** at time `T`: only changes with exchange time `<= T` are applied.
//!   Changes at exactly `T` may or may not be in it, so when every differing level is one
//!   changed at `T` the window is [NotCheckableReason::AmbiguousOrder] rather than a mismatch.
//! - **Snapshot anchor** at time `T`: changes recorded before it with exchange time `>= T` may
//!   be missing from it, so they are carried into the next window and applied again first.
//!   Since a change carries the new total size of its level, applying one the snapshot already
//!   includes changes nothing (later changes to that level are applied after it).
//!
//! A window is not checkable if the token had no anchor yet, the anchor is from an earlier
//! run, a recorded gap or an unsubscribed period overlaps it (by receive time), or one of its
//! changes could not be read. A book identical to the anchor (same levels and exchange time)
//! is a repeat and is ignored.

use std::collections::{HashMap, HashSet};

use chrono::{DateTime, NaiveDate, Utc};
use marcasite::{types::TokenId, ws::MarketEvent};
use rust_decimal::Decimal;
use serde_json::Value;

use crate::{
    archive::{
        envelope::{StoredBody, StoredRecord},
        snapshot_book::SnapshotBook,
    },
    book::{BookSide, LevelChange, OrderBook},
    types::RunId,
    verifier::{
        coverage::{Coverage, InterruptionKind},
        report::{
            BookSource, DIFFERENCES_LISTED, InputStats, NotCheckableReason, VerifyReport,
            VerifyWindow, WindowResult,
        },
    },
};

/// A level change kept for a window, without its token.
#[derive(Debug, Clone, Copy)]
struct Change {
    side: BookSide,
    price: Decimal,
    size: Decimal,
    at: DateTime<Utc>,
}

struct Anchor {
    book: OrderBook,
    source: BookSource,
    /// Index of the run it was recorded in.
    run: usize,
    recv_ts: DateTime<Utc>,
}

struct TokenState {
    anchor: Option<Anchor>,
    /// The run the changes below were recorded in.
    run: usize,
    /// Changes since the anchor (and changes carried from a snapshot anchor), in archive order.
    changes: Vec<Change>,
    /// A change of the window could not be read.
    unreadable: bool,
    tick_size: Option<Decimal>,
}

impl TokenState {
    fn new(run: usize) -> Self {
        TokenState {
            anchor: None,
            run,
            changes: vec![],
            unreadable: false,
            tick_size: None,
        }
    }

    /// Drops changes from an earlier run: a window crossing runs is not checked, and a book in
    /// the new run must not be rebuilt from them.
    fn enter_run(&mut self, run: usize) {
        if self.run != run {
            self.run = run;
            self.changes.clear();
            self.unreadable = false;
        }
    }
}

/// Checks a day's windows. Feed it the day's websocket and snapshot records run by run, each
/// run in `seq` order, calling [Checker::begin_run] before each run's records.
pub struct Checker {
    coverage: Coverage,
    runs: Vec<RunId>,
    tokens: HashMap<TokenId, TokenState>,
    windows: Vec<VerifyWindow>,
    unusable_snapshot_books: u64,
}

/// How many unusable snapshot books are logged one by one.
const UNUSABLE_LOGGED: u64 = 20;

impl Checker {
    pub fn new(coverage: Coverage) -> Self {
        Checker {
            coverage,
            runs: vec![],
            tokens: HashMap::new(),
            windows: vec![],
            unusable_snapshot_books: 0,
        }
    }

    /// The following records are from `run`.
    pub fn begin_run(&mut self, run: RunId) {
        if self.runs.last() != Some(&run) {
            self.runs.push(run);
        }
    }

    fn run_index(&self) -> usize {
        self.runs.len().saturating_sub(1)
    }

    /// Takes the next record in archive order. Records other than websocket and snapshot
    /// records are ignored.
    pub fn observe(&mut self, record: &StoredRecord) {
        if self.runs.is_empty() {
            self.begin_run(record.run_id.clone());
        }
        match &record.body {
            StoredBody::Ws { event } => match event {
                MarketEvent::Book(book) => self.full_book(
                    OrderBook::from_ws_book(book),
                    BookSource::Ws,
                    record.recv_ts,
                ),
                MarketEvent::PriceChange(event) => {
                    for change in &event.price_changes {
                        match LevelChange::from_price_change(change, event.timestamp) {
                            Ok(change) => self.level_change(change),
                            Err(_) => self.unreadable_change(&change.asset_id),
                        }
                    }
                }
                MarketEvent::TickSizeChange(event) => {
                    let run = self.run_index();
                    self.state(&event.asset_id, run).tick_size = Some(event.new_tick_size);
                }
                _ => {}
            },
            StoredBody::Snapshot { books, .. } => {
                for book in books {
                    self.snapshot_book(book, record.recv_ts);
                }
            }
            _ => {}
        }
    }

    fn state(&mut self, token: &TokenId, run: usize) -> &mut TokenState {
        let state = self
            .tokens
            .entry(token.clone())
            .or_insert_with(|| TokenState::new(run));
        state.enter_run(run);
        state
    }

    fn level_change(&mut self, change: LevelChange) {
        let run = self.run_index();
        self.state(&change.token, run).changes.push(Change {
            side: change.side,
            price: change.price,
            size: change.size,
            at: change.at,
        });
    }

    fn unreadable_change(&mut self, token: &TokenId) {
        let run = self.run_index();
        self.state(token, run).unreadable = true;
    }

    /// A book of a snapshot record, as `/books` sent it. One that cannot be read, or has no
    /// timestamp, is skipped and counted.
    fn snapshot_book(&mut self, value: &Value, recv_ts: DateTime<Utc>) {
        let book = SnapshotBook::parse(value).and_then(|book| {
            OrderBook::from_snapshot(&book).ok_or_else(|| "book has no timestamp".to_string())
        });
        match book {
            Ok(book) => self.full_book(book, BookSource::Snapshot, recv_ts),
            Err(e) => {
                self.unusable_snapshot_books += 1;
                if self.unusable_snapshot_books <= UNUSABLE_LOGGED {
                    tracing::warn!("skipping a snapshot book received at {recv_ts}: {e}");
                }
            }
        }
    }

    fn full_book(&mut self, mut target: OrderBook, source: BookSource, recv_ts: DateTime<Utc>) {
        let run = self.run_index();
        let run_id = self.runs[run].clone();
        let token = target.token().clone();
        let state = self
            .tokens
            .entry(token.clone())
            .or_insert_with(|| TokenState::new(run));
        state.enter_run(run);

        if let Some(anchor) = &state.anchor
            && anchor.run == run
            && anchor.book.as_of() == target.as_of()
            && anchor.book.same_levels(&target)
        {
            // A repeat of the previous book: the window carries on.
            return;
        }

        let not_checkable = match &state.anchor {
            None => Some(NotCheckableReason::NoAnchor),
            Some(anchor) if anchor.run != run => Some(NotCheckableReason::RunBoundary),
            Some(anchor) => {
                match self
                    .coverage
                    .interrupted(&run_id, &token, anchor.recv_ts, recv_ts)
                {
                    Some(InterruptionKind::Gap) => Some(NotCheckableReason::Gap),
                    Some(InterruptionKind::Unsubscribed) => Some(NotCheckableReason::Unsubscribed),
                    None if state.unreadable => Some(NotCheckableReason::UnreadableChange),
                    None => None,
                }
            }
        };
        let result = match (not_checkable, &state.anchor) {
            (Some(reason), _) => WindowResult::NotCheckable { reason },
            (None, Some(anchor)) => compare(&anchor.book, &state.changes, &target, source),
            (None, None) => unreachable!("a window without an anchor is not checkable"),
        };
        self.windows.push(VerifyWindow {
            token,
            start: state.anchor.as_ref().map(|a| a.book.as_of()),
            end: target.as_of(),
            anchor: state.anchor.as_ref().map(|a| a.source),
            target: source,
            changes: state.changes.len(),
            result,
        });

        // The target is the next anchor.
        if target.tick_size().is_none()
            && let Some(tick) = state.tick_size
        {
            target.set_tick_size(tick);
        }
        let at = target.as_of();
        match source {
            BookSource::Ws => state.changes.clear(),
            BookSource::Snapshot => state.changes.retain(|c| c.at >= at),
        }
        state.unreadable = false;
        state.anchor = Some(Anchor {
            book: target,
            source,
            run,
            recv_ts,
        });
    }

    /// The report, with the windows checked so far.
    pub fn finish(self, date: NaiveDate, mut input: InputStats) -> VerifyReport {
        input.unusable_snapshot_books += self.unusable_snapshot_books;
        let tokens_checked = self.tokens.values().filter(|s| s.anchor.is_some()).count();
        VerifyReport::new(date, tokens_checked, input, self.windows)
    }
}

/// Rebuilds the window's book and compares it with the target.
fn compare(
    anchor: &OrderBook,
    changes: &[Change],
    target: &OrderBook,
    source: BookSource,
) -> WindowResult {
    let cutoff = target.as_of();
    let mut rebuilt = anchor.clone();
    for change in changes {
        if source == BookSource::Snapshot && change.at > cutoff {
            continue;
        }
        rebuilt.set_level(change.side, change.price, change.size, change.at);
    }
    if rebuilt.same_levels(target) {
        return WindowResult::Match;
    }

    let differences = rebuilt.differences(target);
    if source == BookSource::Snapshot {
        let ambiguous: HashSet<(BookSide, Decimal)> = changes
            .iter()
            .filter(|c| c.at == cutoff)
            .map(|c| (c.side, c.price.normalize()))
            .collect();
        if !ambiguous.is_empty()
            && differences
                .iter()
                .all(|d| ambiguous.contains(&(d.side, d.price)))
        {
            return WindowResult::NotCheckable {
                reason: NotCheckableReason::AmbiguousOrder,
            };
        }
    }
    WindowResult::Mismatch {
        differing_levels: differences.len(),
        first_differences: differences.into_iter().take(DIFFERENCES_LISTED).collect(),
    }
}

#[cfg(test)]
pub(crate) mod tests;
