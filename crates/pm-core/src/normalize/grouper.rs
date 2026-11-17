//! Grouping normalised prints into the sweeps that produced them (§3.3).
//!
//! Per condition, an open group accepts a new print if either
//!
//! - its transaction hash is already in the group, or
//! - it buys the same outcome, its exchange time is at most `window` after the group's last
//!   print (and not before it), and its price is at least the last print's (a sweep's prices
//!   only rise).
//!
//! When several open groups accept, the transaction rule wins, then the most recently opened
//! group. A print no open group accepts opens a new group. A group closes when `window` of
//! receive time passes with no print joining it; it is stamped closed at that moment (last
//! join + `window`), whenever [Grouper::advance] notices.
//!
//! A transaction hash seen on both tokens of a condition (one match reported twice) is kept
//! only on the token it was first seen on: prints of that hash on the sibling token are
//! dropped and counted in the group's `dup_cross_token`, and in `direction_conflict` (with
//! [GroupFlag::DirectionConflict]) when they normalise to the other outcome. Hashes are
//! remembered for `dedup_retention` after they were last seen, so a duplicate arriving after
//! its group closed is still dropped (and counted in [GrouperStats]).
//!
//! The grouper is pure: time only moves through the receive times of pushed prints and the
//! `now` given to [Grouper::advance], which must not go backwards.

use std::{
    collections::{BTreeMap, HashMap},
    time::Duration,
};

use chrono::{DateTime, TimeDelta, Utc};
use marcasite::types::{ConditionId, TokenId};
use serde::{Deserialize, Serialize};

use crate::{
    invariant,
    invariant::InvariantViolation,
    normalize::print::{NormalisedPrint, Outcome, TxHash},
    units::{Price, Shares, Usd, micros},
};

/// Grouping settings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GrouperConfig {
    /// Largest exchange-time gap within a sweep, and the receive-time quiet period that
    /// closes a group (250 ms).
    pub window: Duration,
    /// How long a transaction hash is remembered after it was last seen, to drop late
    /// duplicates from the sibling token.
    pub dedup_retention: Duration,
}

impl Default for GrouperConfig {
    fn default() -> Self {
        Self {
            window: Duration::from_millis(250),
            dedup_retention: Duration::from_secs(60),
        }
    }
}

/// Something unusual about a group, recorded with it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GroupFlag {
    /// A print of one of the group's transactions normalised to the other outcome.
    DirectionConflict,
}

/// A closed group of prints.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrintGroup {
    /// Order in which groups were opened, from 0; unique per grouper.
    pub seq: u64,
    pub condition_id: ConditionId,
    /// The outcome the group buys (that of its first print).
    pub outcome: Outcome,
    pub bought_token: TokenId,
    /// In the order they joined. Never empty.
    pub prints: Vec<NormalisedPrint>,
    /// Distinct transaction hashes, in the order first seen.
    pub tx_hashes: Vec<TxHash>,
    /// Prints of the group's transactions dropped because they were on the sibling token.
    pub dup_cross_token: u32,
    /// How many of those normalised to the other outcome (or joined by hash while buying the
    /// other outcome).
    pub direction_conflict: u32,
    pub flags: Vec<GroupFlag>,
    /// Receive time at which the group closed: its last join plus the window.
    #[serde(with = "micros")]
    pub closed_at: DateTime<Utc>,
}

impl PrintGroup {
    pub fn n_prints(&self) -> usize {
        self.prints.len()
    }

    pub fn first_print(&self) -> &NormalisedPrint {
        &self.prints[0]
    }

    pub fn last_print(&self) -> &NormalisedPrint {
        &self.prints[self.prints.len() - 1]
    }

    /// Earliest exchange time of any print (prints can join slightly out of order by hash).
    pub fn first_exchange_ts(&self) -> DateTime<Utc> {
        self.prints
            .iter()
            .map(|p| p.exchange_ts)
            .min()
            .unwrap_or(self.closed_at)
    }

    pub fn last_exchange_ts(&self) -> DateTime<Utc> {
        self.prints
            .iter()
            .map(|p| p.exchange_ts)
            .max()
            .unwrap_or(self.closed_at)
    }

    pub fn shares(&self) -> Shares {
        self.prints.iter().map(|p| p.shares).sum()
    }

    /// Exact total notional in bought-outcome dollars.
    pub fn notional(&self) -> Usd {
        self.prints.iter().map(|p| p.notional).sum()
    }

    /// Volume-weighted average price, rounded half-even to 6 places; `None` if the group
    /// has no shares.
    pub fn vwap(&self) -> Option<Price> {
        let shares = self.shares();
        if shares.is_zero() {
            return None;
        }
        let vwap = Price::new(self.notional().value() / shares.value()).ok()?;
        let (lo, hi) = self
            .prints
            .iter()
            .fold((Price::ONE, Price::ZERO), |(lo, hi), p| {
                (lo.min(p.price), hi.max(p.price))
            });
        invariant!(lo <= vwap && vwap <= hi, None);
        Some(vwap.rounded())
    }
}

/// Counts of duplicates the grouper dropped after their group had closed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GrouperStats {
    pub late_cross_token_dups: u64,
    pub late_direction_conflicts: u64,
    /// Prints or `advance` calls whose time was earlier than one already seen; processed
    /// at the latest time instead.
    pub time_reversals: u64,
}

#[derive(Debug)]
struct OpenGroup {
    group: PrintGroup,
    last_join_recv: DateTime<Utc>,
}

#[derive(Debug)]
struct SeenHash {
    token: TokenId,
    outcome: Outcome,
    group_seq: u64,
    last_seen_recv: DateTime<Utc>,
}

#[derive(Debug, Default)]
struct ConditionState {
    /// Open groups, oldest first.
    open: Vec<OpenGroup>,
    hashes: HashMap<TxHash, SeenHash>,
}

/// Groups prints of every condition. See the [module documentation](self).
#[derive(Debug)]
pub struct Grouper {
    window: TimeDelta,
    retention: TimeDelta,
    conditions: BTreeMap<ConditionId, ConditionState>,
    next_seq: u64,
    now: Option<DateTime<Utc>>,
    stats: GrouperStats,
}

impl Grouper {
    /// # Errors
    ///
    /// If the window is zero or a duration does not fit a [TimeDelta].
    pub fn new(config: GrouperConfig) -> Result<Self, InvariantViolation> {
        invariant!(
            !config.window.is_zero(),
            Err(InvariantViolation("the grouping window is positive"))
        );
        let window = TimeDelta::from_std(config.window)
            .map_err(|_| InvariantViolation("the grouping window fits a TimeDelta"))?;
        let retention = TimeDelta::from_std(config.dedup_retention)
            .map_err(|_| InvariantViolation("the dedup retention fits a TimeDelta"))?;
        invariant!(
            retention >= window,
            Err(InvariantViolation(
                "hashes are remembered at least as long as a group is open"
            ))
        );
        Ok(Self {
            window,
            retention,
            conditions: BTreeMap::new(),
            next_seq: 0,
            now: None,
            stats: GrouperStats::default(),
        })
    }

    pub fn stats(&self) -> GrouperStats {
        self.stats
    }

    /// How many groups are open, across conditions.
    pub fn open_groups(&self) -> usize {
        self.conditions.values().map(|c| c.open.len()).sum()
    }

    /// Closes the groups quiet for a window at `now`, oldest closing time first, and forgets
    /// hashes past their retention.
    pub fn advance(&mut self, now: DateTime<Utc>) -> Vec<PrintGroup> {
        let now = self.observe(now);
        let (window, retention) = (self.window, self.retention);
        let mut closed = vec![];
        for state in self.conditions.values_mut() {
            let (done, open): (Vec<_>, Vec<_>) = std::mem::take(&mut state.open)
                .into_iter()
                .partition(|g| g.last_join_recv + window <= now);
            state.open = open;
            closed.extend(done.into_iter().map(|g| close(g, window)));
            let open_seqs: Vec<u64> = state.open.iter().map(|g| g.group.seq).collect();
            state.hashes.retain(|_, seen| {
                open_seqs.contains(&seen.group_seq) || seen.last_seen_recv + retention > now
            });
        }
        self.conditions
            .retain(|_, state| !state.open.is_empty() || !state.hashes.is_empty());
        closed.sort_by_key(|g| (g.closed_at, g.seq));
        closed
    }

    /// Closes every open group, as at shutdown or the end of a replay. Each is stamped with
    /// the time it would have closed.
    pub fn flush(&mut self) -> Vec<PrintGroup> {
        let window = self.window;
        let mut closed: Vec<PrintGroup> = self
            .conditions
            .values_mut()
            .flat_map(|state| std::mem::take(&mut state.open))
            .map(|g| close(g, window))
            .collect();
        closed.sort_by_key(|g| (g.closed_at, g.seq));
        closed
    }

    /// Adds a print received at its `recv_ts`, after closing the groups due by then.
    /// Returns those closed groups.
    pub fn push(&mut self, print: NormalisedPrint) -> Vec<PrintGroup> {
        let closed = self.advance(print.recv_ts);
        let now = self.now.unwrap_or(print.recv_ts);
        let state = self
            .conditions
            .entry(print.condition_id.clone())
            .or_default();

        if let Some(hash) = &print.tx_hash
            && let Some(seen) = state.hashes.get_mut(hash)
        {
            seen.last_seen_recv = now;
            let seq = seen.group_seq;
            let open = state.open.iter_mut().find(|g| g.group.seq == seq);
            if seen.token != print.printed_token {
                let conflict = seen.outcome != print.outcome;
                match open {
                    Some(g) => {
                        g.group.dup_cross_token += 1;
                        if conflict {
                            note_conflict(&mut g.group);
                        }
                    }
                    None => {
                        self.stats.late_cross_token_dups += 1;
                        self.stats.late_direction_conflicts += u64::from(conflict);
                    }
                }
                return closed;
            }
            if let Some(g) = open {
                if g.group.outcome != print.outcome {
                    note_conflict(&mut g.group);
                }
                join(g, print, now);
                return closed;
            }
        }

        let window = self.window;
        let sweep = state.open.iter_mut().rev().find(|g| {
            let last = g.group.last_print();
            let gap = print.exchange_ts - last.exchange_ts;
            g.group.outcome == print.outcome
                && gap >= TimeDelta::zero()
                && gap <= window
                && print.price >= last.price
        });
        let seq = match sweep {
            Some(g) => {
                let seq = g.group.seq;
                join(g, print.clone(), now);
                seq
            }
            None => {
                let seq = self.next_seq;
                self.next_seq += 1;
                state.open.push(OpenGroup {
                    group: PrintGroup {
                        seq,
                        condition_id: print.condition_id.clone(),
                        outcome: print.outcome,
                        bought_token: print.bought_token.clone(),
                        prints: vec![],
                        tx_hashes: vec![],
                        dup_cross_token: 0,
                        direction_conflict: 0,
                        flags: vec![],
                        closed_at: now,
                    },
                    last_join_recv: now,
                });
                let g = state.open.last_mut().expect("just pushed");
                join(g, print.clone(), now);
                seq
            }
        };
        if let Some(hash) = print.tx_hash {
            state.hashes.insert(
                hash,
                SeenHash {
                    token: print.printed_token,
                    outcome: print.outcome,
                    group_seq: seq,
                    last_seen_recv: now,
                },
            );
        }
        closed
    }

    /// Moves the grouper's time to `at`, or keeps it (and counts a reversal) if `at` is
    /// earlier than a time already seen. Returns the time to act at.
    fn observe(&mut self, at: DateTime<Utc>) -> DateTime<Utc> {
        match self.now {
            Some(now) if at < now => {
                tracing::error!(%at, %now, "grouper time went backwards; using the later time");
                self.stats.time_reversals += 1;
                now
            }
            _ => {
                self.now = Some(at);
                at
            }
        }
    }
}

fn join(g: &mut OpenGroup, print: NormalisedPrint, now: DateTime<Utc>) {
    if let Some(hash) = &print.tx_hash
        && !g.group.tx_hashes.contains(hash)
    {
        g.group.tx_hashes.push(hash.clone());
    }
    g.group.prints.push(print);
    g.last_join_recv = now;
}

fn note_conflict(group: &mut PrintGroup) {
    group.direction_conflict += 1;
    if !group.flags.contains(&GroupFlag::DirectionConflict) {
        group.flags.push(GroupFlag::DirectionConflict);
    }
}

fn close(g: OpenGroup, window: TimeDelta) -> PrintGroup {
    let mut group = g.group;
    group.closed_at = g.last_join_recv + window;
    group
}

#[cfg(test)]
mod tests;
