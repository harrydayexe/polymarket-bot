//! When each token's stream was interrupted, worked out from the control records of a day
//! (design section 10, "Gap contract").
//!
//! For each run and token this lists:
//!
//! - **Gaps**: from a `gap_open`'s start to the matching `gap_close`'s end. A gap still open
//!   when the run's records end lasts to the end of the run; a `gap_close` with no earlier
//!   mention of the token in the run (its gap opened on an earlier day) is open from the start.
//! - **Unsubscribed periods**: from an `unsubscribe` to the next `subscribe`, or to the end of
//!   the run. A token whose first mention in a run is a `subscribe` was not subscribed before
//!   it.
//!
//! Times are receive times (the recorder's clock), to be compared with the receive times of
//! the records around a window.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use marcasite::types::TokenId;

use crate::{
    archive::envelope::{StoredBody, StoredRecord},
    messages::ControlBody,
    types::RunId,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InterruptionKind {
    Gap,
    Unsubscribed,
}

/// A period when a token's stream was not complete. `None` bounds are open.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Interruption {
    pub kind: InterruptionKind,
    pub start: Option<DateTime<Utc>>,
    pub end: Option<DateTime<Utc>>,
}

impl Interruption {
    /// Whether the interruption overlaps the period from `from` to `to`. Touching at an end
    /// does not count: a book received exactly when a gap starts is the last message before
    /// it, and one received exactly when it ends is after it.
    fn overlaps(&self, from: DateTime<Utc>, to: DateTime<Utc>) -> bool {
        self.start.is_none_or(|start| start < to) && self.end.is_none_or(|end| end > from)
    }
}

/// Interruptions by run and token.
#[derive(Debug, Default)]
pub struct Coverage {
    runs: HashMap<RunId, HashMap<TokenId, Vec<Interruption>>>,
}

#[derive(Default)]
struct Track {
    /// The token appeared in an earlier control record of the run.
    seen: bool,
    open_gap: Option<DateTime<Utc>>,
    unsubscribed_at: Option<DateTime<Utc>>,
    done: Vec<Interruption>,
}

impl Track {
    fn push(
        &mut self,
        kind: InterruptionKind,
        start: Option<DateTime<Utc>>,
        end: Option<DateTime<Utc>>,
    ) {
        self.done.push(Interruption { kind, start, end });
    }

    fn finish(mut self) -> Vec<Interruption> {
        if let Some(start) = self.open_gap.take() {
            self.push(InterruptionKind::Gap, Some(start), None);
        }
        if let Some(start) = self.unsubscribed_at.take() {
            self.push(InterruptionKind::Unsubscribed, Some(start), None);
        }
        self.done
    }
}

impl Coverage {
    /// Builds the interruptions from a day's records. Records other than control records are
    /// ignored; the order given does not matter (they are put in run and `seq` order).
    pub fn from_control(mut records: Vec<StoredRecord>) -> Self {
        records.retain(|r| matches!(r.body, StoredBody::Control(_)));
        records.sort_by(|a, b| (&a.run_id, a.seq).cmp(&(&b.run_id, b.seq)));

        let mut tracks: HashMap<RunId, HashMap<TokenId, Track>> = HashMap::new();
        for record in records {
            let StoredBody::Control(body) = record.body else {
                continue;
            };
            let run = tracks.entry(record.run_id).or_default();
            match body {
                ControlBody::GapOpen { tokens, start, .. } => {
                    for token in tokens {
                        let track = run.entry(token).or_default();
                        track.open_gap.get_or_insert(start);
                        track.seen = true;
                    }
                }
                ControlBody::GapClose { tokens, end, .. } => {
                    for token in tokens {
                        let track = run.entry(token).or_default();
                        match track.open_gap.take() {
                            Some(start) => {
                                track.push(InterruptionKind::Gap, Some(start), Some(end))
                            }
                            None if !track.seen => {
                                track.push(InterruptionKind::Gap, None, Some(end))
                            }
                            None => {}
                        }
                        track.seen = true;
                    }
                }
                ControlBody::Unsubscribe { tokens, .. } => {
                    for token in tokens {
                        let track = run.entry(token).or_default();
                        track.unsubscribed_at.get_or_insert(record.recv_ts);
                        track.seen = true;
                    }
                }
                ControlBody::Subscribe { tokens, .. } => {
                    for token in tokens {
                        let track = run.entry(token).or_default();
                        match track.unsubscribed_at.take() {
                            Some(start) => track.push(
                                InterruptionKind::Unsubscribed,
                                Some(start),
                                Some(record.recv_ts),
                            ),
                            None if !track.seen => track.push(
                                InterruptionKind::Unsubscribed,
                                None,
                                Some(record.recv_ts),
                            ),
                            None => {}
                        }
                        track.seen = true;
                    }
                }
                _ => {}
            }
        }

        let runs = tracks
            .into_iter()
            .map(|(run, tokens)| {
                let tokens = tokens
                    .into_iter()
                    .map(|(token, track)| (token, track.finish()))
                    .filter(|(_, list)| !list.is_empty())
                    .collect();
                (run, tokens)
            })
            .collect();
        Coverage { runs }
    }

    /// The interruptions of `token` in `run`.
    pub fn interruptions(&self, run: &RunId, token: &TokenId) -> &[Interruption] {
        self.runs
            .get(run)
            .and_then(|tokens| tokens.get(token))
            .map_or(&[], Vec::as_slice)
    }

    /// What interrupted `token`'s stream in `run` between receive times `from` and `to`, if
    /// anything. A gap is reported ahead of an unsubscribed period.
    pub fn interrupted(
        &self,
        run: &RunId,
        token: &TokenId,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Option<InterruptionKind> {
        let overlapping = || {
            self.interruptions(run, token)
                .iter()
                .filter(move |i| i.overlaps(from, to))
        };
        if overlapping().any(|i| i.kind == InterruptionKind::Gap) {
            Some(InterruptionKind::Gap)
        } else {
            overlapping().next().map(|i| i.kind)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;
    use crate::{book::tests::t, connection_manager::types::ConnSlotId, messages::GapReason};

    fn record(run: &str, seq: u64, ms: i64, body: ControlBody) -> StoredRecord {
        StoredRecord {
            recv_ts: t(ms),
            seq,
            run_id: RunId::new(run),
            conn_id: None,
            body: StoredBody::Control(body),
        }
    }

    fn tokens(ids: &[&str]) -> HashSet<TokenId> {
        ids.iter().map(|id| TokenId::new(*id)).collect()
    }

    fn open(ids: &[&str], start: i64) -> ControlBody {
        ControlBody::GapOpen {
            tokens: tokens(ids),
            start: t(start),
            reason: GapReason::HeartbeatTimeout,
        }
    }

    fn close(ids: &[&str], end: i64) -> ControlBody {
        ControlBody::GapClose {
            tokens: tokens(ids),
            end: t(end),
            reason: GapReason::Resumed,
        }
    }

    fn subscribe(ids: &[&str]) -> ControlBody {
        ControlBody::Subscribe {
            slot: ConnSlotId::new("c-0001"),
            tokens: tokens(ids),
        }
    }

    fn unsubscribe(ids: &[&str]) -> ControlBody {
        ControlBody::Unsubscribe {
            slot: ConnSlotId::new("c-0001"),
            tokens: tokens(ids),
        }
    }

    fn gap(start: Option<i64>, end: Option<i64>) -> Interruption {
        Interruption {
            kind: InterruptionKind::Gap,
            start: start.map(t),
            end: end.map(t),
        }
    }

    fn unsub(start: Option<i64>, end: Option<i64>) -> Interruption {
        Interruption {
            kind: InterruptionKind::Unsubscribed,
            start: start.map(t),
            end: end.map(t),
        }
    }

    #[test]
    fn gaps_pair_up_in_seq_order_per_run() {
        let r = RunId::new("r-1");
        let coverage = Coverage::from_control(vec![
            // Given out of order: sorted by seq.
            record("r-1", 3, 300, close(&["a"], 300)),
            record("r-1", 1, 100, subscribe(&["a", "b"])),
            record("r-1", 2, 200, open(&["a", "b"], 150)),
            // A double open keeps the earlier start.
            record("r-1", 4, 400, open(&["b"], 390)),
            record("r-1", 5, 500, close(&["b"], 500)),
            record("r-2", 1, 600, open(&["a"], 600)),
        ]);
        let a = TokenId::new("a");
        let b = TokenId::new("b");
        assert_eq!(
            coverage.interruptions(&r, &a),
            [unsub(None, Some(100)), gap(Some(150), Some(300))]
        );
        assert_eq!(
            coverage.interruptions(&r, &b),
            [unsub(None, Some(100)), gap(Some(150), Some(500))]
        );
        // Left open in run 2: lasts to the end of that run, and does not affect run 1.
        assert_eq!(
            coverage.interruptions(&RunId::new("r-2"), &a),
            [gap(Some(600), None)]
        );
    }

    #[test]
    fn a_close_for_a_gap_opened_before_the_day_is_open_at_the_start() {
        let coverage = Coverage::from_control(vec![record("r-1", 1, 100, close(&["a"], 100))]);
        assert_eq!(
            coverage.interruptions(&RunId::new("r-1"), &TokenId::new("a")),
            [gap(None, Some(100))]
        );
    }

    #[test]
    fn unsubscribed_periods() {
        let coverage = Coverage::from_control(vec![
            record("r-1", 1, 100, unsubscribe(&["a", "b"])),
            record("r-1", 2, 200, subscribe(&["a"])),
        ]);
        let r = RunId::new("r-1");
        assert_eq!(
            coverage.interruptions(&r, &TokenId::new("a")),
            [unsub(Some(100), Some(200))]
        );
        assert_eq!(
            coverage.interruptions(&r, &TokenId::new("b")),
            [unsub(Some(100), None)]
        );
    }

    #[test]
    fn overlap_excludes_touching_ends() {
        let coverage = Coverage::from_control(vec![
            record("r-1", 1, 100, open(&["a"], 100)),
            record("r-1", 2, 200, close(&["a"], 200)),
        ]);
        let (r, a) = (RunId::new("r-1"), TokenId::new("a"));
        assert_eq!(coverage.interrupted(&r, &a, t(0), t(100)), None);
        assert_eq!(coverage.interrupted(&r, &a, t(200), t(300)), None);
        assert_eq!(
            coverage.interrupted(&r, &a, t(0), t(101)),
            Some(InterruptionKind::Gap)
        );
        assert_eq!(
            coverage.interrupted(&r, &a, t(199), t(300)),
            Some(InterruptionKind::Gap)
        );
        assert_eq!(
            coverage.interrupted(&r, &a, t(120), t(130)),
            Some(InterruptionKind::Gap)
        );
        assert_eq!(
            coverage.interrupted(&r, &TokenId::new("b"), t(0), t(300)),
            None
        );
        assert_eq!(
            coverage.interrupted(&RunId::new("r-2"), &a, t(0), t(300)),
            None
        );
    }
}
