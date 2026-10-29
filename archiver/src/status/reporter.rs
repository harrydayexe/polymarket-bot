use std::{collections::VecDeque, path::PathBuf, sync::Arc, time::Duration};

use anyhow::Result;
use chrono::{DateTime, TimeDelta, Utc};
use marcasite::types::TokenId;
use tokio::{
    sync::watch,
    time::{self, MissedTickBehavior},
};
use tokio_util::sync::CancellationToken;

use crate::{
    archive::layout::{STATUS_FILE, TRACKED_TOKENS_FILE},
    clock::SharedClock,
    connection_manager::summary::ManagerWatch,
    status::status_file::{
        ConnectionStatus, QueueStatus, StatusFile, TrackedTokensFile, UnhealthyTokenStatus,
        write_json_atomic,
    },
    types::RunId,
    writer::writer::WriterStats,
};

/// How often the status file is rewritten.
pub const STATUS_INTERVAL: Duration = Duration::from_secs(5);
/// How often the summary line is logged.
const SUMMARY_INTERVAL: Duration = Duration::from_secs(60);

/// Cumulative counters at one moment, for the "last minute" and "last hour" figures.
#[derive(Debug, Clone, Copy)]
struct Sample {
    at: DateTime<Utc>,
    ws_records: u64,
    snapshot_records: u64,
    dropped: u64,
}

/// Builds and writes the status file from what the connection manager and the writer
/// publish, and keeps `state/tracked_tokens.json` current.
pub struct StatusReporter {
    root: PathBuf,
    run_id: RunId,
    started_at: DateTime<Utc>,
    queue_capacity: usize,
    manager: ManagerWatch,
    writer: watch::Receiver<WriterStats>,
    samples: VecDeque<Sample>,
    /// The tracked tokens last written, to rewrite the file only when they change.
    tracked_written: Option<Arc<Vec<TokenId>>>,
}

impl StatusReporter {
    pub fn new(
        root: impl Into<PathBuf>,
        run_id: RunId,
        started_at: DateTime<Utc>,
        queue_capacity: usize,
        manager: ManagerWatch,
        writer: watch::Receiver<WriterStats>,
    ) -> Self {
        Self {
            root: root.into(),
            run_id,
            started_at,
            queue_capacity,
            manager,
            writer,
            samples: VecDeque::new(),
            tracked_written: None,
        }
    }

    /// The status as of `now`, recording a sample for the rates.
    pub fn build(&mut self, now: DateTime<Utc>, clean_stop: bool) -> StatusFile {
        let manager = self.manager.summary.borrow().clone();
        let writer = self.writer.borrow().clone();

        let sample = Sample {
            at: now,
            ws_records: writer.ws_records,
            snapshot_records: writer.snapshot_records,
            dropped: manager.frames_dropped,
        };
        self.samples.push_back(sample);
        // Keep one sample older than an hour, as the base for the hourly figures.
        while self.samples.len() > 2 && self.samples[1].at <= now - TimeDelta::hours(1) {
            self.samples.pop_front();
        }
        let minute_ago = self.base(now - TimeDelta::minutes(1));
        let hour_ago = self.base(now - TimeDelta::hours(1));

        StatusFile {
            run_id: self.run_id.clone(),
            program_version: env!("CARGO_PKG_VERSION").to_string(),
            started_at: self.started_at,
            written_at: now,
            clean_stop,
            last_message_at: manager.last_message_at,
            durable_through: writer.durable_through,
            tokens_tracked: manager.tokens_tracked,
            connections: manager
                .connections
                .iter()
                .map(|c| ConnectionStatus {
                    slot: c.slot.clone(),
                    state: c.state.to_string(),
                    tokens: c.tokens,
                    since: c.since,
                    last_message_at: c.last_message_at,
                })
                .collect(),
            messages_last_minute: sample.ws_records - minute_ago.ws_records,
            queue: QueueStatus {
                depth: writer.queue_depth,
                high_water: writer.queue_high_water,
                capacity: self.queue_capacity,
                dropped_total: sample.dropped,
                dropped_last_hour: sample.dropped - hour_ago.dropped,
            },
            open_gaps: manager.open_gaps,
            tokens_unhealthy: manager
                .tokens_unhealthy
                .iter()
                .map(|t| UnhealthyTokenStatus {
                    token: t.token.clone(),
                    since: t.since,
                })
                .collect(),
            snapshots_last_hour: sample.snapshot_records - hour_ago.snapshot_records,
            disk_free_gb: fs4::available_space(&self.root)
                .ok()
                .map(|bytes| (bytes as f64 / 1e9 * 10.0).round() / 10.0),
            current_files: writer.current_files,
            previously_tracked_tokens_file: TRACKED_TOKENS_FILE.to_string(),
        }
    }

    /// The latest sample taken at or before `at`, or the oldest if there is none.
    fn base(&self, at: DateTime<Utc>) -> Sample {
        self.samples
            .iter()
            .rev()
            .find(|s| s.at <= at)
            .or(self.samples.front())
            .copied()
            .expect("a sample was just added")
    }

    /// Writes the status file and, if the tracked tokens changed, the tracked tokens file.
    ///
    /// # Returns
    ///
    /// The status written.
    pub fn write(&mut self, now: DateTime<Utc>, clean_stop: bool) -> Result<StatusFile> {
        // Tracked tokens first: recovery after a crash reads both.
        // Compared by identity rather than with `has_changed`, which stops reporting changes
        // once the connection manager has gone, at shutdown.
        let tokens = self.manager.tracked_tokens.borrow().clone();
        if self
            .tracked_written
            .as_ref()
            .is_none_or(|written| !Arc::ptr_eq(written, &tokens))
        {
            write_json_atomic(
                &self.root.join(TRACKED_TOKENS_FILE),
                &TrackedTokensFile {
                    run_id: self.run_id.clone(),
                    written_at: now,
                    tokens: tokens.to_vec(),
                },
            )?;
            self.tracked_written = Some(tokens);
        }
        let status = self.build(now, clean_stop);
        write_json_atomic(&self.root.join(STATUS_FILE), &status)?;
        Ok(status)
    }
}

/// Rewrite the status file every [STATUS_INTERVAL] and log a summary line every minute,
/// until `token` is cancelled.
///
/// # Returns
///
/// The reporter, for the final write with the clean-stop flag.
pub async fn status_task(
    mut reporter: StatusReporter,
    token: CancellationToken,
    clock: SharedClock,
) -> StatusReporter {
    let mut tick = time::interval(STATUS_INTERVAL);
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut last_summary = time::Instant::now();
    loop {
        tokio::select! {
            _ = token.cancelled() => return reporter,
            _ = tick.tick() => {}
        }
        match reporter.write(clock.now(), false) {
            Ok(status) => {
                if last_summary.elapsed() >= SUMMARY_INTERVAL {
                    last_summary = time::Instant::now();
                    log_summary(&status);
                }
            }
            Err(e) => tracing::error!(error = format!("{e:#}"), "could not write the status file"),
        }
    }
}

fn log_summary(status: &StatusFile) {
    let streaming = status
        .connections
        .iter()
        .filter(|c| c.state == "streaming")
        .count();
    tracing::info!(
        connections = format!("{streaming}/{} streaming", status.connections.len()),
        tokens = status.tokens_tracked,
        messages_last_minute = status.messages_last_minute,
        queue_depth = status.queue.depth,
        open_gaps = status.open_gaps,
        unhealthy = status.tokens_unhealthy.len(),
        snapshots_last_hour = status.snapshots_last_hour,
        "summary"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        connection_manager::{
            summary::{ConnectionSummary, ManagerPublisher, ManagerSummary, manager_channels},
            types::ConnSlotId,
        },
        status::status_file::read_json,
    };

    fn at(s: &str) -> DateTime<Utc> {
        s.parse().unwrap()
    }

    struct Harness {
        dir: tempfile::TempDir,
        reporter: StatusReporter,
        manager: ManagerPublisher,
        writer: watch::Sender<WriterStats>,
    }

    fn harness() -> Harness {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("state")).unwrap();
        let (manager, watch) = manager_channels();
        let (writer, writer_rx) = watch::channel(WriterStats::default());
        let reporter = StatusReporter::new(
            dir.path(),
            RunId::new("r-1"),
            at("2026-09-25T14:00:00Z"),
            100,
            watch,
            writer_rx,
        );
        Harness {
            dir,
            reporter,
            manager,
            writer,
        }
    }

    #[test]
    fn writes_the_status_and_tracked_tokens_files() {
        let mut h = harness();
        let now = at("2026-09-25T14:05:00Z");
        h.manager.summary.send_replace(ManagerSummary {
            connections: vec![ConnectionSummary {
                slot: ConnSlotId::new("c-0001"),
                state: "streaming",
                tokens: 2,
                since: at("2026-09-25T14:00:01Z"),
                last_message_at: Some(at("2026-09-25T14:04:59Z")),
            }],
            tokens_tracked: 2,
            last_message_at: Some(at("2026-09-25T14:04:59Z")),
            ..ManagerSummary::default()
        });
        h.manager
            .tracked_tokens
            .send_replace(Arc::new(vec![TokenId::new("1"), TokenId::new("2")]));
        h.writer.send_replace(WriterStats {
            queue_depth: 3,
            queue_high_water: 40,
            durable_through: Some(at("2026-09-25T14:04:55Z")),
            current_files: vec!["raw/ws/x.jsonl.open".into()],
            ..WriterStats::default()
        });

        let written = h.reporter.write(now, false).unwrap();
        let status: StatusFile = read_json(&h.dir.path().join(STATUS_FILE)).unwrap().unwrap();
        assert_eq!(status, written);
        assert_eq!(status.run_id, RunId::new("r-1"));
        assert_eq!(status.written_at, now);
        assert!(!status.clean_stop);
        assert_eq!(status.connections[0].state, "streaming");
        assert_eq!(status.queue.depth, 3);
        assert_eq!(status.queue.high_water, 40);
        assert_eq!(status.queue.capacity, 100);
        assert_eq!(status.durable_through, Some(at("2026-09-25T14:04:55Z")));
        assert!(status.disk_free_gb.is_some());

        let tracked: TrackedTokensFile = read_json(&h.dir.path().join(TRACKED_TOKENS_FILE))
            .unwrap()
            .unwrap();
        assert_eq!(tracked.tokens, vec![TokenId::new("1"), TokenId::new("2")]);
        assert!(!h.dir.path().join("state/status.json.tmp").exists());
    }

    #[test]
    fn rates_come_from_counter_differences() {
        let mut h = harness();
        let t0 = at("2026-09-25T14:00:00Z");
        let mut stats = WriterStats::default();
        let mut summary = ManagerSummary::default();
        for i in 0..=(70 * 12) {
            // Every 5 s for 70 minutes: 10 messages each time, a snapshot every minute.
            stats.ws_records = 10 * i;
            stats.snapshot_records = i / 12;
            summary.frames_dropped = if i == 6 { 1 } else { summary.frames_dropped };
            h.writer.send_replace(stats.clone());
            h.manager.summary.send_replace(summary.clone());
            let status = h
                .reporter
                .build(t0 + TimeDelta::seconds(5 * i as i64), false);
            if i == 12 {
                assert_eq!(status.messages_last_minute, 120);
                assert_eq!(status.queue.dropped_last_hour, 1);
            }
        }
        let status = h
            .reporter
            .build(t0 + TimeDelta::seconds(5 * 70 * 12 + 1), false);
        assert_eq!(status.messages_last_minute, 120);
        assert_eq!(status.snapshots_last_hour, 60);
        assert_eq!(status.queue.dropped_total, 1);
        assert_eq!(
            status.queue.dropped_last_hour, 0,
            "the drop was over an hour ago"
        );
        assert!(h.reporter.samples.len() <= 12 * 60 + 3);
    }
}
