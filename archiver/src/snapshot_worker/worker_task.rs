use std::{collections::HashSet, sync::Arc, time::Duration};

use anyhow::Result;
use marcasite::{clob::OrderBookSummary, types::TokenId};
use tokio::{
    sync::mpsc,
    time::{self, Instant, MissedTickBehavior},
};
use tokio_util::sync::CancellationToken;

use crate::{
    clock::SharedClock,
    config::Config,
    messages::{Record, SnapshotMsg},
    snapshot_worker::{
        books_client::BooksClient,
        worker::{Request, SnapshotWorker},
    },
};

/// Rounds per `snapshot_interval`. Each round queues the tracked tokens that have come due
/// since the last, so scheduled snapshots go in batches rather than one request per token.
const ROUNDS_PER_INTERVAL: u32 = 60;

/// Take REST snapshots of the order books until `token` is cancelled: those requested, and
/// every tracked token once per `snapshot_interval`.
///
/// # Errors
///
/// Only if the write queue has gone away. A failed fetch is retried, then recorded as
/// `snapshot_failed` and tried again at the next round.
pub async fn snapshot_worker_task(
    mut inbox: mpsc::UnboundedReceiver<SnapshotMsg>,
    write_queue: mpsc::Sender<Record>,
    token: CancellationToken,
    client: Arc<impl BooksClient>,
    config: Arc<Config>,
    clock: SharedClock,
) -> Result<()> {
    let mut worker = SnapshotWorker::new(&config, Instant::now());
    let task = WorkerTask {
        client,
        write_queue,
        clock,
    };

    let round_period =
        (config.snapshot_interval / ROUNDS_PER_INTERVAL).max(Duration::from_millis(1));
    let mut rounds = time::interval(round_period);
    rounds.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        let next_request = worker.next_request_at(Instant::now());

        tokio::select! {
            biased;

            _ = token.cancelled() => return Ok(()),

            // Disabled once the connection manager has gone; it only goes at shutdown or on a
            // failure, which cancels `token`.
            Some(msg) = inbox.recv() => worker.handle(msg, Instant::now()),

            _ = rounds.tick() => worker.round(Instant::now()),

            _ = time::sleep_until(next_request.unwrap_or_else(Instant::now)),
                if next_request.is_some() =>
            {
                let Some(request) = worker.start_request(Instant::now()) else {
                    continue;
                };
                tokio::select! {
                    _ = token.cancelled() => return Ok(()),
                    fetched = task.client.fetch_books(&request.batch.tokens) => {
                        task.finish(&mut worker, request, fetched).await?;
                    }
                }
            }
        }
    }
}

struct WorkerTask<C> {
    client: Arc<C>,
    write_queue: mpsc::Sender<Record>,
    clock: SharedClock,
}

impl<C: BooksClient> WorkerTask<C> {
    /// Writes the snapshot record for a fetched batch, or hands a failure back to `worker`.
    async fn finish(
        &self,
        worker: &mut SnapshotWorker,
        request: Request,
        fetched: Result<Vec<OrderBookSummary>>,
    ) -> Result<()> {
        let recv_ts = self.clock.now();
        let Request { batch, attempt } = request;

        match fetched {
            Ok(books) => {
                log_missing(&batch.tokens, &books);
                tracing::debug!(
                    tokens = batch.tokens.len(),
                    reason = ?batch.reason,
                    "snapshot taken"
                );
                self.write(Record::Snapshot {
                    recv_ts,
                    requested_tokens: batch.tokens,
                    reason: batch.reason,
                    books,
                })
                .await
            }
            Err(e) => {
                let retry_after: Option<Duration> = e
                    .downcast_ref::<marcasite::Error>()
                    .and_then(marcasite::Error::retry_after);
                let error = format!("{e:#}");
                tracing::warn!(
                    tokens = batch.tokens.len(),
                    attempt,
                    error,
                    "snapshot failed"
                );

                let request = Request { batch, attempt };
                match worker.request_failed(request, error, retry_after, Instant::now()) {
                    Some(body) => {
                        self.write(Record::Control {
                            recv_ts,
                            conn_id: None,
                            body,
                        })
                        .await
                    }
                    None => Ok(()),
                }
            }
        }
    }

    async fn write(&self, record: Record) -> Result<()> {
        if self.write_queue.send(record).await.is_err() {
            anyhow::bail!("write queue gone, snapshot worker exiting");
        }
        Ok(())
    }
}

/// Warns about requested tokens the response has no book for.
fn log_missing(requested: &[TokenId], books: &[OrderBookSummary]) {
    let returned: HashSet<&TokenId> = books.iter().map(|book| &book.asset_id).collect();
    let missing = requested
        .iter()
        .filter(|token| !returned.contains(token))
        .count();
    if missing > 0 {
        tracing::warn!(
            missing,
            requested = requested.len(),
            "snapshot response has no book for some tokens"
        );
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::VecDeque, sync::Mutex};

    use async_trait::async_trait;
    use chrono::{TimeZone, Utc};
    use serde_json::json;
    use tokio::task::JoinHandle;

    use super::*;
    use crate::{
        clock::TokioClock,
        messages::{ControlBody, SnapshotReason},
    };

    const SECOND: Duration = Duration::from_secs(1);

    fn token(n: u32) -> TokenId {
        TokenId::new(format!("t{n:05}"))
    }

    fn tokens(ns: impl IntoIterator<Item = u32>) -> HashSet<TokenId> {
        ns.into_iter().map(token).collect()
    }

    fn book(token: &TokenId) -> OrderBookSummary {
        serde_json::from_value(json!({
            "market": "0x01",
            "asset_id": token,
            "timestamp": "1790934258308",
            "hash": "abc",
            "bids": [{ "price": "0.48", "size": "30" }],
            "asks": [],
            "min_order_size": "5",
            "tick_size": "0.01",
            "neg_risk": false,
            "last_trade_price": "0.5",
        }))
        .unwrap()
    }

    /// A `/books` stand-in. Answers with a book per token, or fails while `failures` lasts.
    #[derive(Default)]
    struct FakeBooks {
        /// When each request arrived, and its tokens.
        calls: Mutex<Vec<(Instant, Vec<TokenId>)>>,
        /// Whether each next request fails; succeeds once empty.
        failures: Mutex<VecDeque<bool>>,
    }

    impl FakeBooks {
        fn failing(failures: impl IntoIterator<Item = bool>) -> Self {
            Self {
                failures: Mutex::new(failures.into_iter().collect()),
                ..Self::default()
            }
        }

        fn calls(&self) -> Vec<(Instant, Vec<TokenId>)> {
            self.calls.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl BooksClient for FakeBooks {
        async fn fetch_books(&self, tokens: &[TokenId]) -> Result<Vec<OrderBookSummary>> {
            self.calls
                .lock()
                .unwrap()
                .push((Instant::now(), tokens.to_vec()));
            if self.failures.lock().unwrap().pop_front() == Some(true) {
                anyhow::bail!("503 Service Unavailable");
            }
            Ok(tokens.iter().map(book).collect())
        }
    }

    struct Harness {
        books: Arc<FakeBooks>,
        inbox: mpsc::UnboundedSender<SnapshotMsg>,
        records: mpsc::Receiver<Record>,
        token: CancellationToken,
        task: JoinHandle<Result<()>>,
        start: Instant,
    }

    impl Harness {
        fn start(books: FakeBooks) -> Self {
            let books = Arc::new(books);
            let (inbox, inbox_rx) = mpsc::unbounded_channel();
            let (write_tx, records) = mpsc::channel(64);
            let token = CancellationToken::new();
            let clock = Arc::new(TokioClock::new(
                Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
            ));
            let task = tokio::spawn(snapshot_worker_task(
                inbox_rx,
                write_tx,
                token.clone(),
                books.clone(),
                Arc::new(Config::default()),
                clock,
            ));
            Harness {
                books,
                inbox,
                records,
                token,
                task,
                start: Instant::now(),
            }
        }

        fn request(&self, ns: impl IntoIterator<Item = u32>, reason: SnapshotReason) {
            self.inbox
                .send(SnapshotMsg::SnapshotRequest {
                    tokens: tokens(ns),
                    reason,
                })
                .unwrap();
        }

        fn track(&self, ns: impl IntoIterator<Item = u32>) {
            self.inbox
                .send(SnapshotMsg::TokensTracked {
                    add: tokens(ns),
                    remove: HashSet::new(),
                })
                .unwrap();
        }

        async fn record(&mut self) -> Record {
            time::timeout(Duration::from_secs(7200), self.records.recv())
                .await
                .expect("timed out waiting for a record")
                .expect("write queue closed")
        }

        /// The next record, which must be a snapshot: its requested tokens and reason.
        async fn snapshot(&mut self) -> (Vec<TokenId>, SnapshotReason) {
            match self.record().await {
                Record::Snapshot {
                    requested_tokens,
                    reason,
                    books,
                    ..
                } => {
                    let returned: Vec<TokenId> = books.into_iter().map(|b| b.asset_id).collect();
                    assert_eq!(returned, requested_tokens);
                    (requested_tokens, reason)
                }
                other => panic!("expected a snapshot record, got {other:?}"),
            }
        }

        /// Request start times, relative to the start.
        fn call_times(&self) -> Vec<Duration> {
            self.books
                .calls()
                .into_iter()
                .map(|(at, _)| at - self.start)
                .collect()
        }

        async fn stop(self) {
            self.token.cancel();
            self.task.await.unwrap().unwrap();
        }
    }

    #[tokio::test(start_paused = true)]
    async fn batches_of_at_most_500() {
        let mut h = Harness::start(FakeBooks::default());
        h.request(0..1200, SnapshotReason::Subscribe);

        let mut sizes = Vec::new();
        for _ in 0..3 {
            let (tokens, reason) = h.snapshot().await;
            assert_eq!(reason, SnapshotReason::Subscribe);
            sizes.push(tokens.len());
        }
        assert_eq!(sizes, [500, 500, 200]);
        h.stop().await;
    }

    #[tokio::test(start_paused = true)]
    async fn duplicates_are_requested_once() {
        let mut h = Harness::start(FakeBooks::default());
        for _ in 0..3 {
            h.request([7], SnapshotReason::Subscribe);
        }

        assert_eq!(h.snapshot().await.0, vec![token(7)]);
        assert_eq!(h.books.calls().len(), 1);
        h.stop().await;
    }

    #[tokio::test(start_paused = true)]
    async fn requests_are_spread_at_the_rate_limit() {
        let mut h = Harness::start(FakeBooks::default());
        h.request(0..5000, SnapshotReason::Subscribe);

        for _ in 0..10 {
            h.snapshot().await;
        }
        let ms = |n| Duration::from_millis(n);
        assert_eq!(
            h.call_times(),
            [
                ms(0),
                ms(0),
                ms(500),
                ms(1000),
                ms(1500),
                ms(2000),
                ms(2500),
                ms(3000),
                ms(3500),
                ms(4000)
            ]
        );
        h.stop().await;
    }

    #[tokio::test(start_paused = true)]
    async fn retry_then_succeed() {
        let mut h = Harness::start(FakeBooks::failing([true, true]));
        h.request([1, 2], SnapshotReason::NewToken);

        assert_eq!(
            h.snapshot().await,
            (vec![token(1), token(2)], SnapshotReason::NewToken)
        );
        assert_eq!(h.call_times(), [Duration::ZERO, SECOND, SECOND * 3]);
        h.stop().await;
    }

    #[tokio::test(start_paused = true)]
    async fn retries_exhausted() {
        let mut h = Harness::start(FakeBooks::failing([true; 4]));
        h.request([1, 2], SnapshotReason::Subscribe);

        match h.record().await {
            Record::Control {
                conn_id: None,
                body:
                    ControlBody::SnapshotFailed {
                        tokens: failed,
                        error,
                        attempts,
                    },
                ..
            } => {
                assert_eq!(failed, tokens([1, 2]));
                assert!(error.contains("503"), "{error}");
                assert_eq!(attempts, 4);
            }
            other => panic!("expected snapshot_failed, got {other:?}"),
        }
        assert_eq!(
            h.call_times(),
            [Duration::ZERO, SECOND, SECOND * 3, SECOND * 7]
        );

        // Re-queued for the next round, a minute after the worker started.
        assert_eq!(
            h.snapshot().await,
            (vec![token(1), token(2)], SnapshotReason::Subscribe)
        );
        assert_eq!(h.call_times()[4], Duration::from_secs(60));
        h.stop().await;
    }

    #[tokio::test(start_paused = true)]
    async fn tracked_tokens_are_spread_over_the_hour() {
        let mut h = Harness::start(FakeBooks::default());
        h.track(0..3600);

        let mut taken = HashSet::new();
        for _ in 0..60 {
            let (tokens, reason) = h.snapshot().await;
            assert_eq!(reason, SnapshotReason::Scheduled);
            assert_eq!(tokens.len(), 60);
            taken.extend(tokens);
        }
        assert_eq!(taken.len(), 3600);

        // One request a minute, not all at minute 0.
        let expected: Vec<Duration> = (1..=60).map(|m| Duration::from_secs(60 * m)).collect();
        assert_eq!(h.call_times(), expected);
        h.stop().await;
    }

    #[tokio::test(start_paused = true)]
    async fn exits_when_the_write_queue_is_gone() {
        let Harness {
            inbox,
            records,
            task,
            ..
        } = Harness::start(FakeBooks::default());
        drop(records);
        inbox
            .send(SnapshotMsg::SnapshotRequest {
                tokens: tokens([1]),
                reason: SnapshotReason::Manual,
            })
            .unwrap();

        let result = task.await.unwrap();
        assert!(result.is_err());
    }
}
