use std::{collections::HashSet, time::Duration};

use marcasite::types::TokenId;
use tokio::time::Instant;

use crate::{
    config::Config,
    messages::{ControlBody, SnapshotMsg, SnapshotReason},
    snapshot_worker::{
        pending::{Batch, PendingQueue},
        rate_limiter::RateLimiter,
        schedule::Schedule,
    },
};

/// Attempts at a batch before giving up on it: the first and three retries.
pub const MAX_ATTEMPTS: u32 = 4;

/// Wait before the first retry, doubling for each one after.
const RETRY_INITIAL: Duration = Duration::from_secs(1);

/// One attempt at fetching a batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Request {
    pub batch: Batch,
    /// 1 for the first attempt.
    pub attempt: u32,
}

/// A failed request waiting to be sent again.
#[derive(Debug)]
struct Retry {
    request: Request,
    at: Instant,
}

/// Decides what to snapshot and when: which tokens are waiting, when tracked tokens are next
/// due, the request rate and retries. Pure state; [snapshot_worker_task] does the fetching and
/// writing.
///
/// [snapshot_worker_task]: super::worker_task::snapshot_worker_task
#[derive(Debug)]
pub struct SnapshotWorker {
    pending: PendingQueue,
    schedule: Schedule,
    limiter: RateLimiter,
    retry: Option<Retry>,
    /// Batches that failed every attempt, queued again at the next round.
    failed: Vec<Batch>,
    batch_size: usize,
}

impl SnapshotWorker {
    pub fn new(config: &Config, now: Instant) -> Self {
        let per_second = u32::try_from(config.snapshot_max_requests_per_s).unwrap_or(u32::MAX);
        Self {
            pending: PendingQueue::default(),
            schedule: Schedule::new(config.snapshot_interval),
            limiter: RateLimiter::new(per_second, per_second, now),
            retry: None,
            failed: Vec::new(),
            batch_size: config.snapshot_batch_size,
        }
    }

    pub fn handle(&mut self, msg: SnapshotMsg, now: Instant) {
        match msg {
            SnapshotMsg::SnapshotRequest { tokens, reason } => {
                self.pending.push(sorted(tokens), &reason);
            }
            SnapshotMsg::TokensTracked { add, remove } => {
                self.schedule.remove(&remove);
                self.pending.remove(&remove);
                for batch in &mut self.failed {
                    batch.tokens.retain(|token| !remove.contains(token));
                }
                self.failed.retain(|batch| !batch.tokens.is_empty());
                self.schedule.add(&add, now);
            }
        }
    }

    /// Queues the tokens due a scheduled snapshot, and those whose last snapshot failed.
    pub fn round(&mut self, now: Instant) {
        for batch in self.failed.drain(..) {
            self.pending.push(batch.tokens, &batch.reason);
        }
        let due = self.schedule.take_due(now);
        if !due.is_empty() {
            tracing::debug!(tokens = due.len(), "scheduled snapshots due");
        }
        self.pending.push(due, &SnapshotReason::Scheduled);
    }

    /// When the next request may be sent, or `None` if there is nothing to send.
    pub fn next_request_at(&self, now: Instant) -> Option<Instant> {
        let earliest = match &self.retry {
            Some(retry) => retry.at.max(now),
            None if !self.pending.is_empty() => now,
            None => return None,
        };
        Some(self.limiter.ready_at(earliest))
    }

    /// The request to send at `now`, if one is due and the rate allows it. A batch waiting for
    /// a retry goes before new ones.
    pub fn start_request(&mut self, now: Instant) -> Option<Request> {
        if self.next_request_at(now)? > now || !self.limiter.try_acquire(now) {
            return None;
        }
        match self.retry.take() {
            Some(Retry { mut request, .. }) => {
                request.attempt += 1;
                Some(request)
            }
            None => Some(Request {
                batch: self.pending.take_batch(self.batch_size)?,
                attempt: 1,
            }),
        }
    }

    /// Records that `request` failed. It is retried after a doubling wait, or the server's
    /// `retry_after` if that is longer. After the last attempt its tokens wait for the next
    /// round, and the `snapshot_failed` record to write is returned.
    pub fn request_failed(
        &mut self,
        request: Request,
        error: String,
        retry_after: Option<Duration>,
        now: Instant,
    ) -> Option<ControlBody> {
        if request.attempt >= MAX_ATTEMPTS {
            let body = ControlBody::SnapshotFailed {
                tokens: request.batch.tokens.iter().cloned().collect(),
                error,
                attempts: request.attempt,
            };
            self.failed.push(request.batch);
            return Some(body);
        }

        let wait =
            (RETRY_INITIAL * 2u32.pow(request.attempt - 1)).max(retry_after.unwrap_or_default());
        self.retry = Some(Retry {
            request,
            at: now + wait,
        });
        None
    }
}

fn sorted(tokens: HashSet<TokenId>) -> Vec<TokenId> {
    let mut tokens: Vec<TokenId> = tokens.into_iter().collect();
    tokens.sort();
    tokens
}

#[cfg(test)]
mod tests {
    use super::*;

    fn token(n: u32) -> TokenId {
        TokenId::new(format!("t{n:05}"))
    }

    fn tokens(ns: impl IntoIterator<Item = u32>) -> HashSet<TokenId> {
        ns.into_iter().map(token).collect()
    }

    fn token_list(ns: impl IntoIterator<Item = u32>) -> Vec<TokenId> {
        ns.into_iter().map(token).collect()
    }

    fn worker(now: Instant) -> SnapshotWorker {
        SnapshotWorker::new(&Config::default(), now)
    }

    fn request(
        worker: &mut SnapshotWorker,
        ns: impl IntoIterator<Item = u32>,
        reason: SnapshotReason,
        now: Instant,
    ) {
        worker.handle(
            SnapshotMsg::SnapshotRequest {
                tokens: tokens(ns),
                reason,
            },
            now,
        );
    }

    fn track(
        worker: &mut SnapshotWorker,
        add: impl IntoIterator<Item = u32>,
        remove: impl IntoIterator<Item = u32>,
        now: Instant,
    ) {
        worker.handle(
            SnapshotMsg::TokensTracked {
                add: tokens(add),
                remove: tokens(remove),
            },
            now,
        );
    }

    /// Fails `request` at `now`, expecting a retry.
    fn fail(worker: &mut SnapshotWorker, request: Request, now: Instant) {
        assert_eq!(
            worker.request_failed(request, "boom".into(), None, now),
            None
        );
    }

    #[test]
    fn nothing_to_send_when_idle() {
        let t0 = Instant::now();
        let mut w = worker(t0);
        assert_eq!(w.next_request_at(t0), None);
        assert_eq!(w.start_request(t0), None);
    }

    #[test]
    fn requested_tokens_are_sent_in_order() {
        let t0 = Instant::now();
        let mut w = worker(t0);
        request(&mut w, [2, 1], SnapshotReason::Subscribe, t0);

        assert_eq!(w.next_request_at(t0), Some(t0));
        assert_eq!(
            w.start_request(t0),
            Some(Request {
                batch: Batch {
                    tokens: token_list([1, 2]),
                    reason: SnapshotReason::Subscribe,
                },
                attempt: 1,
            })
        );
        assert_eq!(w.next_request_at(t0), None);
    }

    #[test]
    fn retries_wait_longer_each_time_then_give_up() {
        let t0 = Instant::now();
        let mut w = worker(t0);
        request(&mut w, [1], SnapshotReason::NewToken, t0);

        let mut now = t0;
        for (attempt, wait) in [(1, 1), (2, 2), (3, 4)] {
            let r = w.start_request(now).unwrap();
            assert_eq!(r.attempt, attempt);
            fail(&mut w, r, now);
            let retry_at = now + Duration::from_secs(wait);
            assert_eq!(w.next_request_at(now), Some(retry_at));
            assert_eq!(w.start_request(retry_at - Duration::from_millis(1)), None);
            now = retry_at;
        }

        let last = w.start_request(now).unwrap();
        assert_eq!(last.attempt, MAX_ATTEMPTS);
        assert_eq!(
            w.request_failed(last, "boom".into(), None, now),
            Some(ControlBody::SnapshotFailed {
                tokens: tokens([1]),
                error: "boom".into(),
                attempts: 4,
            })
        );
        assert_eq!(w.next_request_at(now), None);

        // Queued again, with its reason, at the next round.
        w.round(now);
        let again = w.start_request(now).unwrap();
        assert_eq!(again.batch.tokens, token_list([1]));
        assert_eq!(again.batch.reason, SnapshotReason::NewToken);
        assert_eq!(again.attempt, 1);
    }

    #[test]
    fn a_longer_retry_after_is_honoured() {
        let t0 = Instant::now();
        let mut w = worker(t0);
        request(&mut w, [1], SnapshotReason::Subscribe, t0);

        let r = w.start_request(t0).unwrap();
        let wait = Duration::from_secs(30);
        assert_eq!(
            w.request_failed(r, "slow down".into(), Some(wait), t0),
            None
        );
        assert_eq!(w.next_request_at(t0), Some(t0 + wait));
    }

    #[test]
    fn a_retry_goes_before_new_batches() {
        let t0 = Instant::now();
        let mut w = worker(t0);
        request(&mut w, [1], SnapshotReason::Subscribe, t0);
        let r = w.start_request(t0).unwrap();
        request(&mut w, [2], SnapshotReason::Subscribe, t0);
        fail(&mut w, r, t0);

        let later = t0 + Duration::from_secs(1);
        assert_eq!(w.next_request_at(t0), Some(later));
        assert_eq!(
            w.start_request(later).unwrap().batch.tokens,
            token_list([1])
        );
        assert_eq!(
            w.start_request(later).unwrap().batch.tokens,
            token_list([2])
        );
    }

    #[test]
    fn requests_wait_for_the_rate_limiter() {
        let t0 = Instant::now();
        let mut w = worker(t0); // 2 a second
        request(&mut w, 0..1500, SnapshotReason::Subscribe, t0);

        assert!(w.start_request(t0).is_some());
        assert!(w.start_request(t0).is_some());
        assert_eq!(w.start_request(t0), None);
        assert_eq!(w.next_request_at(t0), Some(t0 + Duration::from_millis(500)));
    }

    #[test]
    fn tracked_tokens_are_snapshotted_on_schedule() {
        let t0 = Instant::now();
        let mut w = worker(t0);
        track(&mut w, [1, 2], [], t0);

        w.round(t0);
        assert_eq!(w.next_request_at(t0), None);

        let hour = t0 + Duration::from_secs(3600);
        w.round(hour);
        let r = w.start_request(hour).unwrap();
        assert_eq!(r.batch.tokens, token_list([1, 2]));
        assert_eq!(r.batch.reason, SnapshotReason::Scheduled);
    }

    #[test]
    fn untracked_tokens_are_dropped_everywhere() {
        let t0 = Instant::now();
        let mut w = worker(t0);
        track(&mut w, [1, 2, 3], [], t0);

        // 3 fails every attempt.
        request(&mut w, [3], SnapshotReason::Subscribe, t0);
        let mut now = t0;
        while let Some(r) = w.start_request(now) {
            w.request_failed(r, "boom".into(), None, now);
            now += Duration::from_secs(10);
        }
        request(&mut w, [1, 2], SnapshotReason::Subscribe, now);

        track(&mut w, [], [1, 3], now);
        let r = w.start_request(now).unwrap();
        assert_eq!(r.batch.tokens, token_list([2]));

        // Neither the failed token nor the schedule brings them back.
        w.round(t0 + Duration::from_secs(3600));
        let r = w.start_request(t0 + Duration::from_secs(3600)).unwrap();
        assert_eq!(r.batch.tokens, token_list([2]));
        assert_eq!(w.next_request_at(t0 + Duration::from_secs(3600)), None);
    }
}
