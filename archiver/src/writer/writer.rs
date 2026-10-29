use std::{
    collections::{BTreeMap, HashMap},
    fs::{File, OpenOptions},
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use chrono::{DateTime, Utc};

use crate::{
    archive::{
        envelope::encode,
        layout::{DONE_SUFFIX, FileKey, OPEN_SUFFIX, Stream, file_stem},
    },
    messages::Record,
    types::RunId,
};

/// How long after its hour (or day) has ended a file stays open, for records stamped just
/// before the boundary that reach the writer just after it.
pub const ROTATION_GRACE: Duration = Duration::from_secs(10);

/// Buffer size of each open file. Flushed at least every `flush_interval`.
const BUFFER_BYTES: usize = 256 * 1024;

/// Counters and state the writer publishes for the status file.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct WriterStats {
    pub ws_records: u64,
    pub snapshot_records: u64,
    pub control_records: u64,
    pub market_records: u64,
    /// Receive time of the latest websocket record written.
    pub last_ws_recv_ts: Option<DateTime<Utc>>,
    /// Receive time of the latest websocket record known to be synced to disk. After a crash,
    /// data is lost from here at the latest.
    pub durable_through: Option<DateTime<Utc>>,
    pub queue_depth: usize,
    pub queue_high_water: usize,
    /// Files being written, relative to the archive root.
    pub current_files: Vec<String>,
}

struct OutputFile {
    open_path: PathBuf,
    file: Arc<File>,
    out: BufWriter<Arc<File>>,
    /// Written to since the last sync.
    dirty: bool,
}

/// Files waiting for a sync, taken by [Writer::prepare_sync] and finished with
/// [Writer::synced].
pub struct PendingSync {
    pub files: Vec<Arc<File>>,
    ws_mark: Option<DateTime<Utc>>,
}

impl PendingSync {
    /// Syncs every file to disk. Blocking.
    pub fn run(&self) -> std::io::Result<()> {
        self.files.iter().try_for_each(|f| f.sync_data())
    }
}

/// Appends records to the hourly and daily files of one run (section 8.3). Synchronous: the
/// writer task drives it.
pub struct Writer {
    root: PathBuf,
    run_id: RunId,
    next_seq: u64,
    files: BTreeMap<FileKey, OutputFile>,
    /// Highest part number used per key, so a key reopened after it closed gets a new file.
    parts: HashMap<FileKey, u32>,
    line: Vec<u8>,
    stats: WriterStats,
}

impl Writer {
    pub fn new(root: impl Into<PathBuf>, run_id: RunId) -> Self {
        Self {
            root: root.into(),
            run_id,
            next_seq: 1,
            files: BTreeMap::new(),
            parts: HashMap::new(),
            line: Vec::with_capacity(4096),
            stats: WriterStats::default(),
        }
    }

    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    pub fn stats(&self) -> &WriterStats {
        &self.stats
    }

    pub fn stats_mut(&mut self) -> &mut WriterStats {
        &mut self.stats
    }

    /// Appends `record` to its file, opening the file if needed, and gives it the next `seq`.
    ///
    /// # Returns
    ///
    /// The `seq` given, or `None` if the record could not be serialised (logged; a bug, never
    /// expected).
    ///
    /// # Errors
    ///
    /// If the file cannot be created or written. Fatal: the caller must stop.
    pub fn write(&mut self, record: &Record) -> std::io::Result<Option<u64>> {
        let seq = self.next_seq;
        self.line.clear();
        if let Err(e) = encode(record, seq, &self.run_id, &mut self.line) {
            tracing::error!(error = %e, ?record, "could not serialise record, not written");
            return Ok(None);
        }

        let key = FileKey::for_time(record.stream(), record.recv_ts());
        let file = match self.files.entry(key) {
            std::collections::btree_map::Entry::Occupied(e) => e.into_mut(),
            std::collections::btree_map::Entry::Vacant(e) => {
                let part = self.parts.entry(key).or_insert(0);
                *part += 1;
                let opened = open(&self.root, &key, &self.run_id, *part)?;
                tracing::debug!(path = %opened.open_path.display(), "opened file");
                e.insert(opened)
            }
        };
        file.out.write_all(&self.line)?;
        file.dirty = true;
        self.next_seq += 1;

        match record {
            Record::WsFrame { recv_ts, .. } => {
                self.stats.ws_records += 1;
                self.stats.last_ws_recv_ts = self.stats.last_ws_recv_ts.max(Some(*recv_ts));
            }
            Record::Snapshot { .. } => self.stats.snapshot_records += 1,
            Record::Control { .. } => self.stats.control_records += 1,
            Record::Market { .. } => self.stats.market_records += 1,
        }
        Ok(Some(seq))
    }

    /// Hands buffered lines to the operating system.
    pub fn flush(&mut self) -> std::io::Result<()> {
        self.files.values_mut().try_for_each(|f| f.out.flush())
    }

    /// Flushes, and takes the files written since the last sync. Run the returned sync (it
    /// blocks), then call [Writer::synced].
    pub fn prepare_sync(&mut self) -> std::io::Result<PendingSync> {
        self.flush()?;
        let files = self
            .files
            .values_mut()
            .filter_map(|f| std::mem::take(&mut f.dirty).then(|| f.file.clone()))
            .collect();
        Ok(PendingSync {
            files,
            ws_mark: self.stats.last_ws_recv_ts,
        })
    }

    /// Records that a sync from [Writer::prepare_sync] completed.
    pub fn synced(&mut self, sync: PendingSync) {
        self.stats.durable_through = self.stats.durable_through.max(sync.ws_mark);
    }

    /// Flushes and syncs every file. Blocking.
    pub fn sync_all(&mut self) -> std::io::Result<()> {
        let sync = self.prepare_sync()?;
        sync.run()?;
        self.synced(sync);
        Ok(())
    }

    /// Closes the files whose hour (or day) ended more than [ROTATION_GRACE] before `now`:
    /// flush, sync, then rename from `.jsonl.open` to `.jsonl`. Blocking.
    ///
    /// # Returns
    ///
    /// How many hourly (market data) files were closed.
    pub fn rotate(&mut self, now: DateTime<Utc>) -> std::io::Result<usize> {
        let grace = chrono::Duration::from_std(ROTATION_GRACE).expect("small duration");
        let ended: Vec<FileKey> = self
            .files
            .keys()
            .filter(|key| key.period_end() + grace <= now)
            .copied()
            .collect();
        let hourly = ended.iter().filter(|key| key.stream.hourly()).count();
        for key in ended {
            self.close(&key)?;
        }
        Ok(hourly)
    }

    /// Closes every file. Blocking.
    pub fn close_all(&mut self) -> std::io::Result<()> {
        let keys: Vec<FileKey> = self.files.keys().copied().collect();
        for key in keys {
            self.close(&key)?;
        }
        Ok(())
    }

    fn close(&mut self, key: &FileKey) -> std::io::Result<()> {
        let Some(mut file) = self.files.remove(key) else {
            return Ok(());
        };
        file.out.flush()?;
        file.file.sync_data()?;
        if key.stream == Stream::Ws {
            self.stats.durable_through = self.stats.durable_through.max(self.stats.last_ws_recv_ts);
        }
        let done = done_name(&file.open_path);
        std::fs::rename(&file.open_path, &done)?;
        tracing::debug!(path = %done.display(), "closed file");
        Ok(())
    }

    /// Paths of the open files, relative to the archive root.
    pub fn current_files(&self) -> Vec<String> {
        self.files
            .values()
            .map(|f| {
                f.open_path
                    .strip_prefix(&self.root)
                    .unwrap_or(&f.open_path)
                    .display()
                    .to_string()
            })
            .collect()
    }
}

fn open(root: &Path, key: &FileKey, run_id: &RunId, part: u32) -> std::io::Result<OutputFile> {
    let dir = key.dir(root);
    std::fs::create_dir_all(&dir)?;
    let open_path = dir.join(format!("{}{OPEN_SUFFIX}", file_stem(run_id, part)));
    let file = Arc::new(
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(&open_path)?,
    );
    Ok(OutputFile {
        open_path,
        out: BufWriter::with_capacity(BUFFER_BYTES, file.clone()),
        file,
        dirty: false,
    })
}

fn done_name(open_path: &Path) -> PathBuf {
    let name = open_path
        .file_name()
        .and_then(|n| n.to_str())
        .and_then(|n| n.strip_suffix(OPEN_SUFFIX))
        .expect("writer files end in .jsonl.open");
    open_path.with_file_name(format!("{name}{DONE_SUFFIX}"))
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use serde_json::json;

    use super::*;
    use crate::{
        archive::{
            envelope::{StoredBody, StoredRecord, decode},
            layout::files_for_date,
        },
        connection_manager::types::ConnAttemptId,
        messages::{ControlBody, SnapshotReason},
    };

    fn at(s: &str) -> DateTime<Utc> {
        s.parse().unwrap()
    }

    fn frame(recv_ts: DateTime<Utc>) -> Record {
        Record::WsFrame {
            recv_ts,
            conn_id: ConnAttemptId::new("c-0001-1"),
            event: serde_json::from_value(json!({"event_type":"something","n":1})).unwrap(),
        }
    }

    fn control(recv_ts: DateTime<Utc>) -> Record {
        Record::Control {
            recv_ts,
            conn_id: None,
            body: ControlBody::RunStop { clean: true },
        }
    }

    fn read(path: &Path) -> Vec<StoredRecord> {
        std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(|l| decode(l).unwrap())
            .collect()
    }

    fn all_files(root: &Path) -> Vec<String> {
        let mut found = vec![];
        for entry in walk(root) {
            found.push(entry.strip_prefix(root).unwrap().display().to_string());
        }
        found.sort();
        found
    }

    fn walk(dir: &Path) -> Vec<PathBuf> {
        let mut out = vec![];
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                out.extend(walk(&path));
            } else {
                out.push(path);
            }
        }
        out
    }

    fn writer(dir: &Path, run: &str) -> Writer {
        Writer::new(dir, RunId::new(run))
    }

    #[test]
    fn records_go_to_their_utc_hour_folder() {
        let dir = tempfile::tempdir().unwrap();
        let mut w = writer(dir.path(), "r-1");
        w.write(&frame(at("2026-09-25T14:23:00Z"))).unwrap();
        w.flush().unwrap();
        assert_eq!(
            all_files(dir.path()),
            ["raw/ws/date=2026-09-25/hour=14/r-1.jsonl.open"]
        );
        assert_eq!(
            w.current_files(),
            ["raw/ws/date=2026-09-25/hour=14/r-1.jsonl.open"]
        );
    }

    #[test]
    fn rotation_closes_and_renames_after_the_hour() {
        let dir = tempfile::tempdir().unwrap();
        let mut w = writer(dir.path(), "r-1");
        w.write(&frame(at("2026-09-25T14:59:59.999Z"))).unwrap();
        w.write(&frame(at("2026-09-25T15:00:00Z"))).unwrap();
        w.rotate(at("2026-09-25T15:00:05Z")).unwrap();
        assert_eq!(w.current_files().len(), 2, "still within the grace period");
        w.rotate(at("2026-09-25T15:00:10Z")).unwrap();
        assert_eq!(
            all_files(dir.path()),
            [
                "raw/ws/date=2026-09-25/hour=14/r-1.jsonl",
                "raw/ws/date=2026-09-25/hour=15/r-1.jsonl.open"
            ]
        );
        let closed = read(&dir.path().join("raw/ws/date=2026-09-25/hour=14/r-1.jsonl"));
        assert_eq!(closed.len(), 1);
        assert_eq!(closed[0].seq, 1);
        assert_eq!(
            w.stats().durable_through,
            Some(at("2026-09-25T15:00:00Z")),
            "closing syncs everything written so far"
        );
    }

    #[test]
    fn midnight_splits_date_folders() {
        let dir = tempfile::tempdir().unwrap();
        let mut w = writer(dir.path(), "r-1");
        w.write(&control(at("2026-09-25T23:59:59Z"))).unwrap();
        w.write(&control(at("2026-09-26T00:00:00Z"))).unwrap();
        w.close_all().unwrap();
        assert_eq!(
            all_files(dir.path()),
            [
                "control/date=2026-09-25/r-1.jsonl",
                "control/date=2026-09-26/r-1.jsonl"
            ]
        );
    }

    #[test]
    fn a_late_record_for_a_closed_file_gets_a_new_part() {
        let dir = tempfile::tempdir().unwrap();
        let mut w = writer(dir.path(), "r-1");
        w.write(&frame(at("2026-09-25T14:59:00Z"))).unwrap();
        w.rotate(at("2026-09-25T15:00:30Z")).unwrap();
        w.write(&frame(at("2026-09-25T14:59:59Z"))).unwrap();
        w.close_all().unwrap();
        assert_eq!(
            all_files(dir.path()),
            [
                "raw/ws/date=2026-09-25/hour=14/r-1-p2.jsonl",
                "raw/ws/date=2026-09-25/hour=14/r-1.jsonl"
            ]
        );
        let date = at("2026-09-25T00:00:00Z").date_naive();
        assert_eq!(
            files_for_date(dir.path(), Stream::Ws, date, false)
                .unwrap()
                .len(),
            2
        );
    }

    #[test]
    fn two_runs_in_the_same_hour_use_separate_files() {
        let dir = tempfile::tempdir().unwrap();
        let mut a = writer(dir.path(), "r-a");
        let mut b = writer(dir.path(), "r-b");
        a.write(&frame(at("2026-09-25T14:00:00Z"))).unwrap();
        b.write(&frame(at("2026-09-25T14:00:01Z"))).unwrap();
        a.close_all().unwrap();
        b.close_all().unwrap();
        assert_eq!(
            all_files(dir.path()),
            [
                "raw/ws/date=2026-09-25/hour=14/r-a.jsonl",
                "raw/ws/date=2026-09-25/hour=14/r-b.jsonl"
            ]
        );
    }

    #[test]
    fn each_source_goes_to_its_own_folder() {
        let dir = tempfile::tempdir().unwrap();
        let mut w = writer(dir.path(), "r-1");
        let t = at("2026-09-25T14:00:00Z");
        w.write(&frame(t)).unwrap();
        w.write(&Record::Snapshot {
            recv_ts: t,
            requested_tokens: vec![],
            reason: SnapshotReason::Scheduled,
            books: vec![],
        })
        .unwrap();
        w.write(&control(t)).unwrap();
        let market: crate::market_registry::market_info::MarketInfo =
            serde_json::from_value(json!({
                "condition_id":"0x1","question":"q","slug":null,"event_id":null,"tags":[],
                "tokens":{"yes":"1","no":"2"},"start_date":null,"end_date":null,"active":true,
                "closed":false,"accepting_orders":true,"liquidity_usd":null,"tick_size":null,
                "neg_risk":false,"fetched_at":0
            }))
            .unwrap();
        w.write(&Record::Market {
            recv_ts: t,
            market,
            change: crate::market_registry::registry::MarketChange::Added,
        })
        .unwrap();
        w.close_all().unwrap();
        assert_eq!(
            all_files(dir.path()),
            [
                "control/date=2026-09-25/r-1.jsonl",
                "markets/date=2026-09-25/r-1.jsonl",
                "raw/snapshots/date=2026-09-25/hour=14/r-1.jsonl",
                "raw/ws/date=2026-09-25/hour=14/r-1.jsonl",
            ]
        );
        let s = w.stats();
        assert_eq!(
            (
                s.ws_records,
                s.snapshot_records,
                s.control_records,
                s.market_records
            ),
            (1, 1, 1, 1)
        );
    }

    #[test]
    fn seq_runs_without_gaps_across_files() {
        let dir = tempfile::tempdir().unwrap();
        let mut w = writer(dir.path(), "r-1");
        let base = at("2026-09-25T14:59:00Z");
        for i in 0..1000 {
            let t = base + chrono::Duration::milliseconds(i * 100);
            let record = if i % 3 == 0 { control(t) } else { frame(t) };
            assert_eq!(w.write(&record).unwrap(), Some(i as u64 + 1));
        }
        w.close_all().unwrap();
        let mut seqs: Vec<u64> = walk(dir.path())
            .iter()
            .flat_map(|p| read(p))
            .map(|r| r.seq)
            .collect();
        seqs.sort_unstable();
        assert_eq!(seqs, (1..=1000).collect::<Vec<_>>());
    }

    #[test]
    fn receive_time_is_written_in_microseconds() {
        let dir = tempfile::tempdir().unwrap();
        let mut w = writer(dir.path(), "r-1");
        let t = Utc.timestamp_micros(1_790_348_400_123_456).unwrap();
        w.write(&frame(t)).unwrap();
        w.close_all().unwrap();
        let text = std::fs::read_to_string(walk(dir.path()).pop().unwrap()).unwrap();
        assert!(text.starts_with(r#"{"recv_ts":1790348400123456,"seq":1,"run_id":"r-1","conn_id":"c-0001-1","source":"ws","event":"#));
    }

    #[test]
    fn flushed_data_is_visible_to_another_reader() {
        let dir = tempfile::tempdir().unwrap();
        let mut w = writer(dir.path(), "r-1");
        w.write(&frame(at("2026-09-25T14:00:00Z"))).unwrap();
        let path = dir
            .path()
            .join("raw/ws/date=2026-09-25/hour=14/r-1.jsonl.open");
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "", "buffered");
        w.flush().unwrap();
        assert_eq!(read(&path).len(), 1);
    }

    #[test]
    fn sync_marks_written_frames_durable() {
        let dir = tempfile::tempdir().unwrap();
        let mut w = writer(dir.path(), "r-1");
        let t = at("2026-09-25T14:00:00Z");
        w.write(&frame(t)).unwrap();
        assert_eq!(w.stats().durable_through, None);
        let sync = w.prepare_sync().unwrap();
        assert_eq!(sync.files.len(), 1);
        sync.run().unwrap();
        w.synced(sync);
        assert_eq!(w.stats().durable_through, Some(t));
        assert!(
            w.prepare_sync().unwrap().files.is_empty(),
            "nothing new to sync"
        );
    }

    #[test]
    fn a_full_disk_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let folder = dir.path().join("raw/ws/date=2026-09-25/hour=14");
        std::fs::create_dir_all(&folder).unwrap();
        std::os::unix::fs::symlink("/dev/full", folder.join("r-1.jsonl.open")).unwrap();
        let mut w = writer(dir.path(), "r-1");
        let mut failed = false;
        // Writes are buffered, so the error shows at the latest when flushing.
        for _ in 0..10 {
            if w.write(&frame(at("2026-09-25T14:00:00Z"))).is_err() {
                failed = true;
                break;
            }
        }
        failed |= w.flush().is_err();
        assert!(failed);
    }

    #[test]
    fn an_unwritable_folder_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("raw"), "not a folder").unwrap();
        let mut w = writer(dir.path(), "r-1");
        assert!(w.write(&frame(at("2026-09-25T14:00:00Z"))).is_err());
    }

    #[test]
    fn written_records_read_back() {
        let dir = tempfile::tempdir().unwrap();
        let mut w = writer(dir.path(), "r-1");
        w.write(&control(at("2026-09-25T14:00:00Z"))).unwrap();
        w.close_all().unwrap();
        let records = read(&dir.path().join("control/date=2026-09-25/r-1.jsonl"));
        assert_eq!(
            records[0].body,
            StoredBody::Control(ControlBody::RunStop { clean: true })
        );
    }
}
