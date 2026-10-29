use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result};
use futures::FutureExt;
use tokio::{
    sync::{mpsc, watch},
    time::{Instant, timeout_at},
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::{
    cli::CommonArgs,
    client::{APIClient, MarcasiteClient},
    clock::{SharedClock, SystemClock},
    config::Config,
    connection_manager::{manager_task::connection_manager_task, summary::manager_channels},
    market_registry::registry_task::registry_task,
    messages::{ConnectionManagerMsg, ControlBody, GapReason, Record, RegistryMsg, SnapshotMsg},
    recovery::{lock_archive, recover},
    snapshot_worker::{
        books_client::{BooksClient, ClobBooksClient},
        worker_task::snapshot_worker_task,
    },
    status::reporter::{StatusReporter, status_task},
    types::{RunId, SystemRandom},
    writer::{inspector::FrameInspector, writer::Writer, writer_task::writer_task},
};

/// How long a stop may take before the process exits anyway (section 12.1).
const SHUTDOWN_LIMIT: Duration = Duration::from_secs(15);
/// Of the shutdown time, how much is kept for the writer to drain the queue and close files.
const WRITER_DRAIN_TIME: Duration = Duration::from_secs(4);

pub async fn execute(args: CommonArgs, token: CancellationToken) -> Result<()> {
    let config = Arc::new(Config::load(&args.config)?);
    // Connect first: if Gamma is unreachable nothing is written, and the previous run's
    // status still dates the gap when a later start succeeds.
    let gamma = Arc::new(MarcasiteClient::connect(&config).await?);
    let books = Arc::new(ClobBooksClient::new(&config)?);
    run(config, token, Arc::new(SystemClock), gamma, books).await
}

/// Records until `token` is cancelled or a task fails.
///
/// # Errors
///
/// If the archive cannot be set up, or any task failed (the stop is then recorded as unclean).
pub async fn run(
    config: Arc<Config>,
    token: CancellationToken,
    clock: SharedClock,
    gamma: Arc<impl APIClient>,
    books: Arc<impl BooksClient>,
) -> Result<()> {
    let root = config.archive_dir.clone();
    let _lock = lock_archive(&root)?;

    let started_at = clock.now();
    let run_id = RunId::create(started_at, &mut SystemRandom);
    let recovery = recover(&root, started_at)?;
    tracing::info!(%run_id, previous_run = ?recovery.previous_run, archive = %root.display(), "starting");

    // The run's first records are on disk before anything else happens, so a crash loop still
    // leaves its restart gaps behind.
    let mut writer = Writer::new(&root, run_id.clone());
    let control = |body| Record::Control {
        recv_ts: started_at,
        conn_id: None,
        body,
    };
    writer.write(&control(ControlBody::RunStart {
        program_version: env!("CARGO_PKG_VERSION").to_string(),
        config_hash: config.fingerprint(),
        previous_run: recovery.previous_run.clone(),
    }))?;
    if let Some(gap) = &recovery.restart_gap {
        tracing::warn!(
            tokens = gap.tokens.len(),
            since = %gap.start,
            "previous run did not stop cleanly, recording a process_restart gap"
        );
        writer.write(&control(ControlBody::GapOpen {
            tokens: gap.tokens.clone(),
            start: gap.start,
            reason: GapReason::ProcessRestart,
        }))?;
        writer.write(&control(ControlBody::GapClose {
            tokens: gap.tokens.clone(),
            end: gap.end,
            reason: GapReason::ProcessRestart,
        }))?;
    }
    writer
        .sync_all()
        .context("could not write the run's first records")?;

    let (publisher, manager_watch) = manager_channels();
    let (writer_stats_tx, writer_stats) = watch::channel(writer.stats().clone());
    let mut reporter = StatusReporter::new(
        &root,
        run_id,
        started_at,
        config.write_queue_capacity,
        manager_watch,
        writer_stats,
    );
    reporter
        .write(started_at, false)
        .context("could not write the status file")?;

    let (registry_tx, registry_rx) = mpsc::unbounded_channel::<RegistryMsg>();
    let (conn_mgr_tx, conn_mgr_rx) = mpsc::channel::<ConnectionManagerMsg>(256);
    let (write_tx, write_rx) = mpsc::channel::<Record>(config.write_queue_capacity);
    let (snapshot_tx, snapshot_rx) = mpsc::unbounded_channel::<SnapshotMsg>();

    let inspector = FrameInspector::new(conn_mgr_tx.clone(), registry_tx, &config.category_tags);
    let writer_handle = tokio::spawn(supervise(
        "writer",
        token.clone(),
        writer_task(
            writer,
            write_rx,
            inspector,
            writer_stats_tx,
            config.clone(),
            clock.clone(),
        ),
    ));

    let status_token = CancellationToken::new();
    let status_handle = tokio::spawn(status_task(reporter, status_token.clone(), clock.clone()));

    let tracker = TaskTracker::new();
    let registry = tracker.spawn(supervise(
        "registry",
        token.clone(),
        registry_task(
            registry_rx,
            conn_mgr_tx,
            write_tx.clone(),
            token.child_token(),
            gamma,
            config.clone(),
            clock.clone(),
        ),
    ));
    let snapshots = tracker.spawn(supervise(
        "snapshot worker",
        token.clone(),
        snapshot_worker_task(
            snapshot_rx,
            write_tx.clone(),
            token.child_token(),
            books,
            config.clone(),
            clock.clone(),
        ),
    ));
    let conn_mgr = tracker.spawn(supervise(
        "connection manager",
        token.clone(),
        connection_manager_task(
            conn_mgr_rx,
            write_tx.clone(),
            snapshot_tx,
            token.child_token(),
            config.clone(),
            clock.clone(),
            publisher,
        ),
    ));
    tracker.close();

    // Run until stopped, or until a task fails and cancels `token`. The tasks only end on
    // their own if something is wrong.
    tokio::select! {
        _ = token.cancelled() => {}
        _ = tracker.wait() => {
            tracing::error!("tasks ended without being asked to, stopping");
            token.cancel();
        }
    }
    let deadline = Instant::now() + SHUTDOWN_LIMIT;
    tracing::info!("stopping");

    // The tasks close their connections, write their last records and end.
    let tasks_done = timeout_at(deadline - WRITER_DRAIN_TIME, tracker.wait())
        .await
        .is_ok();
    if !tasks_done {
        tracing::warn!("tasks did not stop in time");
    }
    // A task that is unfinished, panicked or returned an error makes the stop unclean.
    let tasks_ok = [registry, conn_mgr, snapshots]
        .into_iter()
        .all(|task| matches!(task.now_or_never(), Some(Ok(true))));

    // `run_stop` is the writer's last record; it then closes every file.
    let clean = tasks_done && tasks_ok;
    let stop = Record::Control {
        recv_ts: clock.now(),
        conn_id: None,
        body: ControlBody::RunStop { clean },
    };
    let stop_sent = timeout_at(deadline - Duration::from_secs(1), write_tx.send(stop))
        .await
        .is_ok_and(|sent| sent.is_ok());
    drop(write_tx);
    let writer_ok = stop_sent
        && matches!(
            timeout_at(deadline - Duration::from_secs(1), writer_handle).await,
            Ok(Ok(true))
        );
    if !writer_ok {
        tracing::error!("the writer did not finish cleanly");
    }

    status_token.cancel();
    let clean = clean && writer_ok;
    match timeout_at(deadline, status_handle).await {
        Ok(Ok(mut reporter)) => {
            if let Err(e) = reporter.write(clock.now(), clean) {
                tracing::error!(error = format!("{e:#}"), "could not write the final status");
            }
        }
        _ => tracing::error!("the status reporter did not finish"),
    }

    if !clean {
        anyhow::bail!("the archiver did not stop cleanly, see the log for details");
    }
    tracing::info!("stopped cleanly");
    Ok(())
}

/// Run a task to completion. On error, log it and cancel `shutdown` so the rest of the
/// process stops too.
///
/// # Returns
///
/// `true` if the task finished without error.
async fn supervise(
    name: &'static str,
    shutdown: CancellationToken,
    task: impl Future<Output = anyhow::Result<()>>,
) -> bool {
    match task.await {
        Ok(()) => true,
        Err(e) => {
            tracing::error!(task = name, error = format!("{e:#}"), "task failed");
            shutdown.cancel();
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, path::Path};

    use async_trait::async_trait;
    use marcasite::{clob::OrderBookSummary, gamma::Market, types::ConditionId, types::TokenId};
    use serde_json::json;

    use super::*;
    use crate::{
        archive::{
            envelope::{StoredBody, StoredRecord},
            layout::{STATUS_FILE, Stream, TRACKED_TOKENS_FILE, files_for_date, open_files},
            reader::read_records,
        },
        connection_manager::fake_server::{FakeServer, TIMEOUT},
        market_registry::market_info::tests::gamma_market,
        status::status_file::{StatusFile, TrackedTokensFile, read_json, write_json_atomic},
    };

    struct FakeGamma(Vec<Market>);

    #[async_trait]
    impl APIClient for FakeGamma {
        async fn fetch_open_markets(&self) -> Result<Vec<Market>> {
            Ok(self.0.clone())
        }

        async fn fetch_markets(&self, _ids: &[ConditionId]) -> Result<Vec<Market>> {
            Ok(vec![])
        }
    }

    struct FakeBooks;

    #[async_trait]
    impl BooksClient for FakeBooks {
        async fn fetch_books(&self, tokens: &[TokenId]) -> Result<Vec<OrderBookSummary>> {
            Ok(tokens
                .iter()
                .map(|token| {
                    serde_json::from_value(json!({
                        "market": "0x01", "asset_id": token, "timestamp": "1790934258308",
                        "hash": "abc", "bids": [{ "price": "0.48", "size": "30" }], "asks": [],
                        "min_order_size": "5", "tick_size": "0.01", "neg_risk": false,
                        "last_trade_price": "0.5",
                    }))
                    .unwrap()
                })
                .collect())
        }
    }

    fn config(dir: &Path, server: &FakeServer) -> Arc<Config> {
        Arc::new(Config {
            archive_dir: dir.to_path_buf(),
            websocket_url: server.url(),
            heartbeat_interval: Duration::from_millis(100),
            heartbeat_timeout: Duration::from_millis(500),
            connect_timeout: Duration::from_secs(1),
            backoff_initial: Duration::from_millis(50),
            backoff_max: Duration::from_millis(200),
            flush_interval: Duration::from_millis(50),
            sync_interval: Duration::from_millis(100),
            ..Config::default()
        })
    }

    fn start(config: Arc<Config>) -> (CancellationToken, tokio::task::JoinHandle<Result<()>>) {
        let token = CancellationToken::new();
        let task = tokio::spawn(run(
            config,
            token.clone(),
            Arc::new(SystemClock),
            Arc::new(FakeGamma(vec![gamma_market(1, json!({}))])),
            Arc::new(FakeBooks),
        ));
        (token, task)
    }

    /// Every record in the archive, in `seq` order, per run.
    fn archive(root: &Path) -> Vec<StoredRecord> {
        let today = chrono::Utc::now().date_naive();
        let mut records = vec![];
        for date in [today.pred_opt().unwrap(), today] {
            for stream in Stream::ALL {
                for path in files_for_date(root, stream, date, false).unwrap() {
                    for line in read_records(&path).unwrap() {
                        records.push(line.unwrap().1.expect("valid line"));
                    }
                }
            }
        }
        records.sort_by_key(|r| (r.run_id.clone(), r.seq));
        records
    }

    fn control(records: &[StoredRecord]) -> Vec<ControlBody> {
        records
            .iter()
            .filter_map(|r| match &r.body {
                StoredBody::Control(body) => Some(body.clone()),
                _ => None,
            })
            .collect()
    }

    fn tokens() -> HashSet<TokenId> {
        HashSet::from([TokenId::new("1111"), TokenId::new("1222")])
    }

    #[tokio::test]
    async fn records_to_disk_and_stops_cleanly() {
        let dir = tempfile::tempdir().unwrap();
        let mut server = FakeServer::start().await;
        let (token, task) = start(config(dir.path(), &server));

        let mut conn = server.accept().await;
        let subscription = conn.recv_json().await;
        assert_eq!(subscription["assets_ids"], json!(["1111", "1222"]));
        conn.send(
            r#"{"event_type":"book","asset_id":"1111","market":"0x01","timestamp":"1757908892351",
                "hash":"h1","bids":[{"price":"0.48","size":"30"}],"asks":[]}"#,
        );
        conn.send(
            r#"{"event_type":"price_change","market":"0x01","timestamp":"1757908892352",
                "price_changes":[{"asset_id":"1111","price":"0.48","size":"0","side":"BUY",
                "hash":"h2","best_bid":"","best_ask":""}]}"#,
        );
        // Let the frames, the subscribe snapshot and a status write happen.
        tokio::time::sleep(Duration::from_millis(5500)).await;
        let status: StatusFile = read_json(&dir.path().join(STATUS_FILE)).unwrap().unwrap();
        assert!(!status.clean_stop);
        assert_eq!(status.tokens_tracked, 2);
        assert_eq!(status.connections[0].state, "streaming");
        assert!(status.durable_through.is_some());

        token.cancel();
        tokio::time::timeout(SHUTDOWN_LIMIT, task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        assert!(open_files(dir.path()).unwrap().is_empty());
        let records = archive(dir.path());
        let seqs: Vec<u64> = records.iter().map(|r| r.seq).collect();
        assert_eq!(seqs, (1..=records.len() as u64).collect::<Vec<_>>());

        let control = control(&records);
        assert!(matches!(
            control[0],
            ControlBody::RunStart {
                previous_run: None,
                ..
            }
        ));
        assert_eq!(control.last(), Some(&ControlBody::RunStop { clean: true }));
        assert!(control.contains(&ControlBody::Subscribe {
            slot: "c-0001".into(),
            tokens: tokens(),
        }));
        let ws = records
            .iter()
            .filter(|r| matches!(r.body, StoredBody::Ws { .. }))
            .count();
        assert_eq!(ws, 2);
        assert!(records.iter().any(|r| matches!(
            &r.body,
            StoredBody::Snapshot { reason: crate::messages::SnapshotReason::Subscribe, books, .. }
                if books.len() == 2
        )));
        assert_eq!(
            records
                .iter()
                .filter(|r| matches!(r.body, StoredBody::Market { .. }))
                .count(),
            1
        );

        let status: StatusFile = read_json(&dir.path().join(STATUS_FILE)).unwrap().unwrap();
        assert!(status.clean_stop);
        let tracked: TrackedTokensFile = read_json(&dir.path().join(TRACKED_TOKENS_FILE))
            .unwrap()
            .unwrap();
        assert_eq!(tracked.tokens.into_iter().collect::<HashSet<_>>(), tokens());
    }

    #[tokio::test]
    async fn a_crash_is_recorded_as_a_restart_gap() {
        let dir = tempfile::tempdir().unwrap();
        let mut server = FakeServer::start().await;
        let (token, task) = start(config(dir.path(), &server));
        let mut conn = server.accept().await;
        conn.recv_json().await;
        tokio::time::sleep(Duration::from_millis(300)).await;
        token.cancel();
        task.await.unwrap().unwrap();

        // Pretend the first run crashed: no clean stop, and a file left open.
        let mut status: StatusFile = read_json(&dir.path().join(STATUS_FILE)).unwrap().unwrap();
        let first_run = status.run_id.clone();
        status.clean_stop = false;
        write_json_atomic(&dir.path().join(STATUS_FILE), &status).unwrap();
        let leftover = dir.path().join("control/date=2020-01-01");
        std::fs::create_dir_all(&leftover).unwrap();
        std::fs::write(leftover.join("r-x.jsonl.open"), "").unwrap();

        let (token, task) = start(config(dir.path(), &server));
        let mut conn = tokio::time::timeout(TIMEOUT, server.accept())
            .await
            .unwrap();
        conn.recv_json().await;
        tokio::time::sleep(Duration::from_millis(300)).await;
        token.cancel();
        task.await.unwrap().unwrap();

        assert!(leftover.join("r-x.jsonl").exists());
        let records = archive(dir.path());
        let second: Vec<StoredRecord> = records
            .into_iter()
            .filter(|r| r.run_id != first_run)
            .collect();
        let control = control(&second);
        assert!(
            matches!(&control[0], ControlBody::RunStart { previous_run: Some(p), .. } if *p == first_run)
        );
        assert!(matches!(
            &control[1],
            ControlBody::GapOpen { tokens: t, reason: GapReason::ProcessRestart, .. } if *t == tokens()
        ));
        assert!(matches!(
            &control[2],
            ControlBody::GapClose { tokens: t, reason: GapReason::ProcessRestart, .. } if *t == tokens()
        ));
    }

    #[tokio::test]
    async fn a_second_recorder_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let mut server = FakeServer::start().await;
        let (token, task) = start(config(dir.path(), &server));
        server.accept().await;
        let (_other_token, other) = start(config(dir.path(), &server));
        let err = other.await.unwrap().unwrap_err();
        assert!(format!("{err:#}").contains("already running"));
        token.cancel();
        task.await.unwrap().unwrap();
    }
}
