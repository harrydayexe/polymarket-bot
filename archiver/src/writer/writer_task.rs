use std::sync::Arc;

use anyhow::{Context, Result};
use tokio::{
    sync::{mpsc, watch},
    time::{Instant, sleep_until},
};

use crate::{
    clock::SharedClock,
    config::Config,
    messages::{ControlBody, Record},
    writer::{
        inspector::FrameInspector,
        writer::{Writer, WriterStats},
    },
};

/// Write every record from the queue until a `run_stop` record has been written or every
/// producer has gone, then close all files. Flushes every `flush_interval`, syncs every
/// `sync_interval` and closes files once their hour or day is over. Publishes its counters on
/// `stats` every flush.
///
/// # Errors
///
/// If a file cannot be written, synced or renamed. Fatal: the process must stop, and the next
/// start records the lost time as a `process_restart` gap.
pub async fn writer_task(
    mut writer: Writer,
    mut queue: mpsc::Receiver<Record>,
    mut inspector: FrameInspector,
    stats: watch::Sender<WriterStats>,
    config: Arc<Config>,
    clock: SharedClock,
) -> Result<()> {
    let result = run(
        &mut writer,
        &mut queue,
        &mut inspector,
        &stats,
        &config,
        &clock,
    )
    .await;
    if let Err(e) = &result {
        tracing::error!(
            error = format!("{e:#}"),
            "WRITING TO THE ARCHIVE FAILED, stopping"
        );
        return result;
    }

    writer
        .close_all()
        .context("could not close the archive files")?;
    inspector.send_books_seen();
    publish(&mut writer, &queue, &stats);
    tracing::info!("writer finished, all files closed");
    Ok(())
}

async fn run(
    writer: &mut Writer,
    queue: &mut mpsc::Receiver<Record>,
    inspector: &mut FrameInspector,
    stats: &watch::Sender<WriterStats>,
    config: &Config,
    clock: &SharedClock,
) -> Result<()> {
    let mut next_flush = Instant::now() + config.flush_interval;
    let mut next_sync = Instant::now() + config.sync_interval;

    loop {
        // Not biased: under a constant stream of records the timers must still run.
        tokio::select! {
            record = queue.recv() => {
                let Some(record) = record else {
                    return Ok(());
                };
                let depth = queue.len() + 1;
                let s = writer.stats_mut();
                s.queue_high_water = s.queue_high_water.max(depth);

                let stop = matches!(
                    record,
                    Record::Control { body: ControlBody::RunStop { .. }, .. }
                );
                writer.write(&record).context("could not write a record")?;
                inspector.inspect(&record);
                if stop {
                    // Nothing is covered after `run_stop`, so anything still queued is left
                    // out of the archive.
                    return Ok(());
                }
            }
            _ = sleep_until(next_flush.min(next_sync)) => {}
        }

        let now = Instant::now();
        if now >= next_flush {
            next_flush = now + config.flush_interval;
            writer
                .flush()
                .context("could not flush the archive files")?;
            writer
                .rotate(clock.now())
                .context("could not close a finished archive file")?;
            inspector.send_books_seen();
            publish(writer, queue, stats);
        }
        if now >= next_sync {
            next_sync = now + config.sync_interval;
            let pending = writer
                .prepare_sync()
                .context("could not flush the archive files")?;
            let pending = tokio::task::spawn_blocking(move || pending.run().map(|()| pending))
                .await
                .context("sync task failed")?
                .context("could not sync the archive files to disk")?;
            writer.synced(pending);
        }
    }
}

fn publish(
    writer: &mut Writer,
    queue: &mpsc::Receiver<Record>,
    stats: &watch::Sender<WriterStats>,
) {
    let current_files = writer.current_files();
    let s = writer.stats_mut();
    s.queue_depth = queue.len();
    s.current_files = current_files;
    stats.send_replace(s.clone());
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use chrono::{DateTime, Utc};
    use serde_json::json;

    use super::*;
    use crate::{
        archive::{
            envelope::{StoredBody, decode},
            layout::{OPEN_SUFFIX, open_files},
        },
        clock::{Clock, TokioClock},
        connection_manager::types::ConnAttemptId,
        messages::{ConnectionManagerMsg, RegistryMsg},
        types::RunId,
    };

    fn at(s: &str) -> DateTime<Utc> {
        s.parse().unwrap()
    }

    struct Harness {
        dir: tempfile::TempDir,
        queue: mpsc::Sender<Record>,
        stats: watch::Receiver<WriterStats>,
        conn_mgr: mpsc::Receiver<ConnectionManagerMsg>,
        _registry: mpsc::UnboundedReceiver<RegistryMsg>,
        clock: Arc<TokioClock>,
        task: tokio::task::JoinHandle<Result<()>>,
    }

    fn start(capacity: usize, start: &str) -> Harness {
        let dir = tempfile::tempdir().unwrap();
        let (queue, rx) = mpsc::channel(capacity);
        let (conn_tx, conn_mgr) = mpsc::channel(16);
        let (reg_tx, registry) = mpsc::unbounded_channel();
        let (stats_tx, stats) = watch::channel(WriterStats::default());
        let clock = Arc::new(TokioClock::new(at(start)));
        let config = Arc::new(Config {
            archive_dir: dir.path().to_path_buf(),
            ..Config::default()
        });
        let task = tokio::spawn(writer_task(
            Writer::new(dir.path(), RunId::new("r-1")),
            rx,
            FrameInspector::new(conn_tx, reg_tx, &config.category_tags),
            stats_tx,
            config,
            clock.clone(),
        ));
        Harness {
            dir,
            queue,
            stats,
            conn_mgr,
            _registry: registry,
            clock,
            task,
        }
    }

    fn book(recv_ts: DateTime<Utc>, token: &str) -> Record {
        Record::WsFrame {
            recv_ts,
            conn_id: ConnAttemptId::new("c-0001-1"),
            event: serde_json::from_value(json!({"event_type":"book","asset_id":token,
                "market":"0xabc","bids":[],"asks":[],"timestamp":"1","hash":"h"}))
            .unwrap(),
        }
    }

    fn stop(recv_ts: DateTime<Utc>) -> Record {
        Record::Control {
            recv_ts,
            conn_id: None,
            body: ControlBody::RunStop { clean: true },
        }
    }

    fn lines(dir: &std::path::Path, rel: &str) -> Vec<crate::archive::envelope::StoredRecord> {
        std::fs::read_to_string(dir.join(rel))
            .unwrap()
            .lines()
            .map(|l| decode(l).unwrap())
            .collect()
    }

    #[tokio::test(start_paused = true)]
    async fn writes_flushes_and_publishes() {
        let mut h = start(16, "2026-09-25T14:00:00Z");
        h.queue.send(book(h.clock.now(), "1")).await.unwrap();
        tokio::time::sleep(Duration::from_millis(1100)).await;

        let stats = h.stats.borrow_and_update().clone();
        assert_eq!(stats.ws_records, 1);
        assert_eq!(
            stats.current_files,
            [format!("raw/ws/date=2026-09-25/hour=14/r-1{OPEN_SUFFIX}")]
        );
        assert_eq!(
            lines(h.dir.path(), &stats.current_files[0]).len(),
            1,
            "flushed within a second"
        );
        match h.conn_mgr.try_recv().unwrap() {
            ConnectionManagerMsg::BooksSeen { tokens } => {
                assert_eq!(tokens, [marcasite::types::TokenId::new("1")].into())
            }
            other => panic!("unexpected {other:?}"),
        }

        tokio::time::sleep(Duration::from_secs(5)).await;
        assert!(
            h.stats.borrow().durable_through.is_some(),
            "synced within 5s"
        );
        drop(h.queue);
        h.task.await.unwrap().unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn rotates_files_at_the_hour() {
        let h = start(16, "2026-09-25T14:59:59Z");
        h.queue.send(book(h.clock.now(), "1")).await.unwrap();
        tokio::time::sleep(Duration::from_secs(12)).await;
        h.queue.send(book(h.clock.now(), "1")).await.unwrap();
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert!(
            h.dir
                .path()
                .join("raw/ws/date=2026-09-25/hour=14/r-1.jsonl")
                .exists()
        );
        drop(h.queue);
        h.task.await.unwrap().unwrap();
        assert!(open_files(h.dir.path()).unwrap().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn stops_after_run_stop_and_closes_everything() {
        let h = start(16, "2026-09-25T14:00:00Z");
        let t = h.clock.now();
        h.queue.send(book(t, "1")).await.unwrap();
        h.queue.send(stop(t)).await.unwrap();
        h.queue.send(book(t, "2")).await.unwrap();
        h.task.await.unwrap().unwrap();
        // The producer is still there, but the writer has finished.
        assert!(h.queue.send(book(t, "3")).await.is_err());

        assert!(open_files(h.dir.path()).unwrap().is_empty());
        let ws = lines(h.dir.path(), "raw/ws/date=2026-09-25/hour=14/r-1.jsonl");
        assert_eq!(ws.len(), 1, "nothing after run_stop");
        let control = lines(h.dir.path(), "control/date=2026-09-25/r-1.jsonl");
        assert_eq!(
            control[0].body,
            StoredBody::Control(ControlBody::RunStop { clean: true })
        );
    }

    #[tokio::test(start_paused = true)]
    async fn tracks_the_queue_high_water_mark() {
        let h = start(64, "2026-09-25T14:00:00Z");
        let t = h.clock.now();
        // Fill the queue before the writer gets to run.
        for _ in 0..50 {
            h.queue.try_send(book(t, "1")).unwrap();
        }
        tokio::time::sleep(Duration::from_millis(1100)).await;
        let stats = h.stats.borrow().clone();
        assert_eq!(stats.queue_high_water, 50);
        assert_eq!(stats.queue_depth, 0);
        drop(h.queue);
        h.task.await.unwrap().unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn a_write_failure_ends_the_task_with_an_error() {
        let h = start(16, "2026-09-25T14:00:00Z");
        std::fs::write(h.dir.path().join("raw"), "not a folder").unwrap();
        h.queue.send(book(h.clock.now(), "1")).await.unwrap();
        let result = h.task.await.unwrap();
        assert!(result.is_err());
    }
}
