//! The engine log: every [Envelope] the engine consumes, appended to disk so a run can be
//! replayed bit-for-bit later.
//!
//! Layout under a root directory (`<data_dir>/engine_log`):
//!
//! ```text
//! date=YYYY-MM-DD/hour=HH/<run_id>.jsonl       while the hour is open
//! date=YYYY-MM-DD/hour=HH/<run_id>.jsonl.zst   once the hour is closed
//! ```
//!
//! The hour of an envelope is its `available_at` in UTC. Each line is one JSON object, the
//! serde form of [Envelope]. A closed hour is compressed on a background thread so the engine
//! is never blocked on it.

use std::{
    collections::{BTreeMap, VecDeque},
    ffi::OsString,
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Write},
    path::{Path, PathBuf},
    thread::JoinHandle,
};

use chrono::{DateTime, NaiveDate, Timelike, Utc};
use pm_core::{invariant, invariant::InvariantViolation};

use crate::event::Envelope;

/// The zstd level used for closed hours.
const ZSTD_LEVEL: i32 = 3;
/// Extension of an open (uncompressed) log file.
const OPEN_EXT: &str = "jsonl";
/// Extension of a closed (compressed) log file.
const CLOSED_EXT: &str = "jsonl.zst";

/// Why the engine log could not be written or read.
#[derive(Debug, thiserror::Error)]
pub enum EngineLogError {
    /// A file system error; the message names the path where one is known.
    #[error("engine log I/O error: {0}")]
    Io(#[from] std::io::Error),
    /// An envelope could not be serialised, or a line could not be parsed.
    #[error("engine log JSON error: {0}")]
    Json(#[from] serde_json::Error),
    /// An envelope broke the order the log requires (`seq` strictly increasing, hours
    /// non-decreasing). Nothing was written.
    #[error("engine log out of order: {what}: previous {previous}, got {got}")]
    OutOfOrder {
        /// Which ordering was violated.
        what: &'static str,
        /// The last accepted value.
        previous: String,
        /// The refused value.
        got: String,
    },
    /// A background compression failed or panicked.
    #[error("engine log compression failed: {0}")]
    Compression(String),
    /// An internal consistency check failed.
    #[error(transparent)]
    Invariant(#[from] InvariantViolation),
}

/// An hour of UTC time: the unit of one log file.
type HourKey = (NaiveDate, u32);

fn hour_key(t: &DateTime<Utc>) -> HourKey {
    (t.date_naive(), t.hour())
}

fn hour_dir(root: &Path, key: HourKey) -> PathBuf {
    root.join(format!("date={}", key.0.format("%Y-%m-%d")))
        .join(format!("hour={:02}", key.1))
}

/// An I/O error whose message names the path it concerns.
fn io_at(path: &Path, err: std::io::Error) -> EngineLogError {
    EngineLogError::Io(std::io::Error::new(
        err.kind(),
        format!("{}: {err}", path.display()),
    ))
}

/// `path` with `.zst` appended (`x.jsonl` becomes `x.jsonl.zst`).
fn with_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut name: OsString = path.as_os_str().to_owned();
    name.push(suffix);
    PathBuf::from(name)
}

/// The file being appended to.
struct OpenFile {
    path: PathBuf,
    writer: BufWriter<File>,
    lines: u64,
}

type CompressHandle = JoinHandle<Result<(), String>>;

/// Appends envelopes to the engine log, one file per UTC hour of `available_at`.
pub struct EngineLogWriter {
    root: PathBuf,
    run_id: String,
    open: Option<OpenFile>,
    open_hour: Option<HourKey>,
    last_seq: Option<u64>,
    /// Compressed paths of closed files, in order.
    closed: Vec<PathBuf>,
    handles: Vec<CompressHandle>,
    /// Failures of background compressions already joined.
    failures: Vec<String>,
}

impl EngineLogWriter {
    /// Creates `root` if needed. Writes nothing until the first append.
    pub fn new(root: impl Into<PathBuf>, run_id: impl Into<String>) -> Self {
        let root = root.into();
        if let Err(e) = fs::create_dir_all(&root) {
            // Opening a file creates its directories again, and reports the error there.
            tracing::warn!(root = %root.display(), error = %e, "cannot create engine log root");
        }
        Self {
            root,
            run_id: run_id.into(),
            open: None,
            open_hour: None,
            last_seq: None,
            closed: Vec::new(),
            handles: Vec::new(),
            failures: Vec::new(),
        }
    }

    /// Appends one envelope. If its hour differs from the open file's, the open file is
    /// flushed, closed and compressed (in a background thread, so the engine is not
    /// blocked), and a new file is opened. Envelopes must arrive with strictly increasing
    /// `seq`, else it is an error ([EngineLogError::OutOfOrder]) and nothing is written.
    /// Receive times from different tasks can step back slightly (the engine's clock does
    /// not), so an envelope stamped in an hour already closed goes in the open file: files
    /// stay in `seq` order, and replay reads them in that order.
    pub fn append(&mut self, envelope: &Envelope) -> Result<(), EngineLogError> {
        let stamped = hour_key(&envelope.available_at);
        let key = self.open_hour.map_or(stamped, |open| open.max(stamped));
        if let Some(prev) = self.last_seq
            && envelope.seq <= prev
        {
            return Err(EngineLogError::OutOfOrder {
                what: "seq must be strictly increasing",
                previous: prev.to_string(),
                got: envelope.seq.to_string(),
            });
        }
        let mut line = serde_json::to_vec(envelope)?;
        line.push(b'\n');

        if self.open_hour != Some(key) {
            self.close_open()?;
            self.reap_finished();
            self.open_file(key)?;
        }
        invariant!(
            self.open_hour == Some(key),
            Err(InvariantViolation("the open file is for the envelope's hour").into())
        );
        let Some(open) = self.open.as_mut() else {
            return Err(InvariantViolation("a file is open after opening one").into());
        };
        open.writer
            .write_all(&line)
            .map_err(|e| io_at(&open.path, e))?;
        open.lines += 1;
        self.last_seq = Some(envelope.seq);
        Ok(())
    }

    /// Flushes buffered lines to the OS.
    pub fn flush(&mut self) -> Result<(), EngineLogError> {
        if let Some(open) = self.open.as_mut() {
            open.writer.flush().map_err(|e| io_at(&open.path, e))?;
        }
        Ok(())
    }

    /// Flushes and closes the open file, compresses it, and waits for every background
    /// compression to finish. Returns the paths of all files written (compressed), in order.
    pub fn finish(mut self) -> Result<Vec<PathBuf>, EngineLogError> {
        self.close_open()?;
        for handle in std::mem::take(&mut self.handles) {
            self.join(handle);
        }
        invariant!(
            self.handles.is_empty(),
            Err(InvariantViolation("every compression was joined").into())
        );
        if !self.failures.is_empty() {
            return Err(EngineLogError::Compression(self.failures.join("; ")));
        }
        Ok(self.closed)
    }

    fn open_file(&mut self, key: HourKey) -> Result<(), EngineLogError> {
        invariant!(
            self.open.is_none(),
            Err(InvariantViolation("no file is open when opening the next").into())
        );
        let dir = hour_dir(&self.root, key);
        fs::create_dir_all(&dir).map_err(|e| io_at(&dir, e))?;
        let path = dir.join(format!("{}.{OPEN_EXT}", self.run_id));
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|e| io_at(&path, e))?;
        self.open = Some(OpenFile {
            path,
            writer: BufWriter::new(file),
            lines: 0,
        });
        self.open_hour = Some(key);
        Ok(())
    }

    /// Flushes and closes the open file and starts compressing it in the background.
    fn close_open(&mut self) -> Result<(), EngineLogError> {
        let Some(mut open) = self.open.take() else {
            return Ok(());
        };
        open.writer.flush().map_err(|e| io_at(&open.path, e))?;
        let OpenFile {
            path,
            writer,
            lines,
        } = open;
        drop(writer);
        if lines == 0 {
            // Nothing was written (the first write failed): leave no empty file behind.
            fs::remove_file(&path).map_err(|e| io_at(&path, e))?;
            return Ok(());
        }
        let target = with_suffix(&path, ".zst");
        self.closed.push(target);
        let name = format!("engine-log-zstd-{}", self.run_id);
        let handle = std::thread::Builder::new()
            .name(name)
            .spawn(move || compress_file(&path).map_err(|e| e.to_string()))
            .map_err(EngineLogError::Io)?;
        self.handles.push(handle);
        Ok(())
    }

    /// Joins the compressions that have already finished, so the vec does not grow.
    fn reap_finished(&mut self) {
        let (done, running): (Vec<_>, Vec<_>) = std::mem::take(&mut self.handles)
            .into_iter()
            .partition(|h| h.is_finished());
        self.handles = running;
        for handle in done {
            self.join(handle);
        }
    }

    fn join(&mut self, handle: CompressHandle) {
        match handle.join() {
            Ok(Ok(())) => {}
            Ok(Err(msg)) => {
                tracing::error!(error = %msg, "engine log compression failed");
                self.failures.push(msg);
            }
            Err(_) => {
                tracing::error!("engine log compression thread panicked");
                self.failures.push("compression thread panicked".to_owned());
            }
        }
    }
}

/// Compresses `src` (`x.jsonl`) to `x.jsonl.zst` via a temp name, then removes `src`.
fn compress_file(src: &Path) -> Result<(), EngineLogError> {
    invariant!(
        src.extension().is_some_and(|e| e == OPEN_EXT),
        Err(InvariantViolation("only .jsonl files are compressed").into())
    );
    let dst = with_suffix(src, ".zst");
    let tmp = with_suffix(src, ".zst.tmp");
    let input = File::open(src).map_err(|e| io_at(src, e))?;
    let output = File::create(&tmp).map_err(|e| io_at(&tmp, e))?;
    let mut output = BufWriter::new(output);
    zstd::stream::copy_encode(BufReader::new(input), &mut output, ZSTD_LEVEL)
        .map_err(|e| io_at(&tmp, e))?;
    let output = output
        .into_inner()
        .map_err(|e| io_at(&tmp, e.into_error()))?;
    output.sync_all().map_err(|e| io_at(&tmp, e))?;
    drop(output);
    fs::rename(&tmp, &dst).map_err(|e| io_at(&dst, e))?;
    invariant!(
        dst.exists(),
        Err(InvariantViolation("the compressed file exists before the source is removed").into())
    );
    fs::remove_file(src).map_err(|e| io_at(src, e))?;
    Ok(())
}

/// The log files of `run_id` under `root`, in time order (by date then hour), compressed
/// (`.jsonl.zst`) or not (`.jsonl`, an hour still open or a crashed run). If both exist for one
/// hour (crash mid-compression), the `.jsonl` is preferred.
pub fn run_files(root: &Path, run_id: &str) -> Result<Vec<PathBuf>, EngineLogError> {
    invariant!(
        !run_id.is_empty() && !run_id.contains(['/', '\\']),
        Err(InvariantViolation("run id is a plain file name").into())
    );
    if !root.exists() {
        return Ok(Vec::new());
    }
    let plain = format!("{run_id}.{OPEN_EXT}");
    let packed = format!("{run_id}.{CLOSED_EXT}");
    let mut found: BTreeMap<HourKey, PathBuf> = BTreeMap::new();
    for date_entry in read_dir(root)? {
        let Some(date) = date_entry
            .file_name()
            .to_str()
            .and_then(|n| n.strip_prefix("date="))
            .and_then(|d| NaiveDate::parse_from_str(d, "%Y-%m-%d").ok())
        else {
            continue;
        };
        for hour_entry in read_dir(&date_entry.path())? {
            let Some(hour) = hour_entry
                .file_name()
                .to_str()
                .and_then(|n| n.strip_prefix("hour="))
                .and_then(|h| h.parse::<u32>().ok())
                .filter(|h| *h < 24)
            else {
                continue;
            };
            let dir = hour_entry.path();
            let plain_path = dir.join(&plain);
            let packed_path = dir.join(&packed);
            let chosen = if plain_path.is_file() {
                plain_path
            } else if packed_path.is_file() {
                packed_path
            } else {
                continue;
            };
            found.insert((date, hour), chosen);
        }
    }
    let files: Vec<PathBuf> = found.into_values().collect();
    invariant!(
        files
            .iter()
            .all(|p| p.extension().is_some_and(|e| e == OPEN_EXT || e == "zst")),
        Err(InvariantViolation("only log files are returned").into())
    );
    Ok(files)
}

fn read_dir(dir: &Path) -> Result<Vec<fs::DirEntry>, EngineLogError> {
    let mut entries = Vec::new();
    for entry in fs::read_dir(dir).map_err(|e| io_at(dir, e))? {
        entries.push(entry.map_err(|e| io_at(dir, e))?);
    }
    Ok(entries)
}

/// A file being read.
struct Source {
    path: PathBuf,
    reader: Box<dyn BufRead + Send>,
    is_last: bool,
    line_no: u64,
}

/// Reads envelopes back from log files in order, decompressing `.zst` files transparently.
///
/// An incomplete final line in the LAST file (a crash mid-write) is skipped with a warning;
/// an unparseable line anywhere else is an error, after which the iterator ends.
pub struct LogReader {
    pending: VecDeque<PathBuf>,
    current: Option<Source>,
    buf: Vec<u8>,
    failed: bool,
}

impl LogReader {
    /// A reader over `files`, which must be in time order (see [run_files]).
    pub fn open(files: Vec<PathBuf>) -> Self {
        Self {
            pending: files.into(),
            current: None,
            buf: Vec::new(),
            failed: false,
        }
    }

    fn open_source(path: PathBuf, is_last: bool) -> Result<Source, EngineLogError> {
        let file = File::open(&path).map_err(|e| io_at(&path, e))?;
        let reader: Box<dyn BufRead + Send> = if path.extension().is_some_and(|e| e == "zst") {
            let decoder = zstd::stream::read::Decoder::new(file).map_err(|e| io_at(&path, e))?;
            Box::new(BufReader::new(decoder))
        } else {
            Box::new(BufReader::new(file))
        };
        Ok(Source {
            path,
            reader,
            is_last,
            line_no: 0,
        })
    }

    fn fail(&mut self, err: EngineLogError) -> Option<Result<Envelope, EngineLogError>> {
        self.failed = true;
        Some(Err(err))
    }
}

impl Iterator for LogReader {
    type Item = Result<Envelope, EngineLogError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.failed {
                return None;
            }
            if self.current.is_none() {
                let path = self.pending.pop_front()?;
                let is_last = self.pending.is_empty();
                match Self::open_source(path, is_last) {
                    Ok(src) => self.current = Some(src),
                    Err(e) => return self.fail(e),
                }
            }
            invariant!(
                self.current.is_some(),
                Some(Err(
                    InvariantViolation("a source is open when reading").into()
                ))
            );
            let src = self.current.as_mut()?;
            self.buf.clear();
            let n = match src.reader.read_until(b'\n', &mut self.buf) {
                Ok(n) => n,
                Err(e) => {
                    let err = io_at(&src.path, e);
                    return self.fail(err);
                }
            };
            if n == 0 {
                self.current = None;
                continue;
            }
            src.line_no += 1;
            let complete = self.buf.last() == Some(&b'\n');
            let mut end = self.buf.len();
            while end > 0 && matches!(self.buf[end - 1], b'\n' | b'\r') {
                end -= 1;
            }
            if end == 0 {
                continue;
            }
            match serde_json::from_slice::<Envelope>(&self.buf[..end]) {
                Ok(envelope) => return Some(Ok(envelope)),
                Err(_) if !complete && src.is_last => {
                    tracing::warn!(
                        path = %src.path.display(),
                        line = src.line_no,
                        "skipping incomplete final line of the engine log"
                    );
                    self.current = None;
                }
                Err(e) => {
                    tracing::error!(
                        path = %src.path.display(),
                        line = src.line_no,
                        error = %e,
                        "unparseable engine log line"
                    );
                    return self.fail(e.into());
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use pm_core::archive::record::ConnSlotId;

    use super::*;
    use crate::event::Event;

    fn at(h: u32, m: u32, s: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 3, 5, h, m, s).unwrap()
    }

    fn env(seq: u64, available_at: DateTime<Utc>) -> Envelope {
        let event = if seq.is_multiple_of(2) {
            Event::Tick
        } else {
            Event::ConnFailed {
                slot: ConnSlotId::new("c-0001"),
                attempt: 1,
                error: "x".into(),
                retry_in_ms: 5,
            }
        };
        Envelope {
            seq,
            event_time: available_at - chrono::Duration::milliseconds(3),
            available_at,
            event,
        }
    }

    fn read_all(files: Vec<PathBuf>) -> Vec<Envelope> {
        LogReader::open(files)
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    }

    #[test]
    fn round_trip_within_one_hour() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("engine_log");
        let mut w = EngineLogWriter::new(&root, "run-a");
        let envs = vec![
            env(1, at(13, 0, 1)),
            env(2, at(13, 30, 0)),
            env(3, at(13, 59, 59)),
        ];
        for e in &envs {
            w.append(e).unwrap();
        }
        w.flush().unwrap();
        // While open, the file is uncompressed and readable.
        let open = run_files(&root, "run-a").unwrap();
        assert_eq!(open.len(), 1);
        assert!(open[0].to_string_lossy().ends_with("run-a.jsonl"));
        assert_eq!(read_all(open), envs);

        let files = w.finish().unwrap();
        assert_eq!(files.len(), 1);
        assert!(files[0].ends_with("date=2026-03-05/hour=13/run-a.jsonl.zst"));
        assert!(files[0].exists());
        assert!(!root.join("date=2026-03-05/hour=13/run-a.jsonl").exists());
        assert_eq!(run_files(&root, "run-a").unwrap(), files);
        assert_eq!(read_all(files), envs);
    }

    #[test]
    fn rollover_produces_one_file_per_hour() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("engine_log");
        let mut w = EngineLogWriter::new(&root, "run-a");
        let envs = vec![
            env(1, at(13, 10, 0)),
            env(2, at(13, 59, 59)),
            env(3, at(14, 0, 0)),
            env(4, at(14, 5, 0)),
        ];
        for e in &envs {
            w.append(e).unwrap();
        }
        let files = w.finish().unwrap();
        assert_eq!(files.len(), 2);
        assert_eq!(
            files[0],
            root.join("date=2026-03-05/hour=13/run-a.jsonl.zst")
        );
        assert_eq!(
            files[1],
            root.join("date=2026-03-05/hour=14/run-a.jsonl.zst")
        );
        for f in &files {
            assert!(f.exists());
            assert!(
                !f.with_extension("").exists(),
                "{f:?} left an uncompressed twin"
            );
        }
        assert_eq!(run_files(&root, "run-a").unwrap(), files);
        assert_eq!(read_all(files), envs);
    }

    #[test]
    fn rollover_across_midnight_sorts_by_date() {
        let dir = tempfile::tempdir().unwrap();
        let mut w = EngineLogWriter::new(dir.path(), "r");
        let late = Utc.with_ymd_and_hms(2026, 3, 5, 23, 59, 59).unwrap();
        let next = Utc.with_ymd_and_hms(2026, 3, 6, 0, 0, 1).unwrap();
        let envs = vec![env(1, late), env(2, next)];
        for e in &envs {
            w.append(e).unwrap();
        }
        w.finish().unwrap();
        let files = run_files(dir.path(), "r").unwrap();
        assert_eq!(files.len(), 2);
        assert!(
            files[0]
                .to_string_lossy()
                .contains("date=2026-03-05/hour=23")
        );
        assert!(
            files[1]
                .to_string_lossy()
                .contains("date=2026-03-06/hour=00")
        );
        assert_eq!(read_all(files), envs);
    }

    #[test]
    fn out_of_order_seq_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let mut w = EngineLogWriter::new(dir.path(), "r");
        w.append(&env(5, at(13, 0, 0))).unwrap();
        for bad in [5, 4] {
            let err = w.append(&env(bad, at(13, 1, 0))).unwrap_err();
            assert!(matches!(err, EngineLogError::OutOfOrder { .. }), "{err}");
        }
        w.append(&env(6, at(13, 2, 0))).unwrap();
        let files = w.finish().unwrap();
        let seqs: Vec<u64> = read_all(files).iter().map(|e| e.seq).collect();
        assert_eq!(seqs, vec![5, 6]);
    }

    #[test]
    fn an_envelope_stamped_in_a_closed_hour_goes_in_the_open_file() {
        let dir = tempfile::tempdir().unwrap();
        let mut w = EngineLogWriter::new(dir.path(), "r");
        w.append(&env(1, at(14, 0, 0))).unwrap();
        // Stamped by another task just before the hour turned, queued behind seq 1.
        w.append(&env(2, at(13, 59, 59))).unwrap();
        w.append(&env(3, at(14, 0, 1))).unwrap();
        let files = w.finish().unwrap();
        assert_eq!(files.len(), 1);
        assert!(!dir.path().join("date=2026-03-05/hour=13").exists());
        let seqs: Vec<u64> = read_all(files).iter().map(|e| e.seq).collect();
        assert_eq!(seqs, vec![1, 2, 3]);
    }

    #[test]
    fn nothing_is_written_until_the_first_append() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("a/b/engine_log");
        let w = EngineLogWriter::new(&root, "r");
        assert!(root.is_dir());
        assert!(w.finish().unwrap().is_empty());
        assert_eq!(fs::read_dir(&root).unwrap().count(), 0);
    }

    fn write_lines(path: &Path, content: &[u8]) {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, content).unwrap();
    }

    fn line(e: &Envelope) -> Vec<u8> {
        let mut v = serde_json::to_vec(e).unwrap();
        v.push(b'\n');
        v
    }

    #[test]
    fn truncated_last_line_of_last_file_is_skipped() {
        let dir = tempfile::tempdir().unwrap();
        let (a, b) = (env(1, at(13, 0, 0)), env(2, at(13, 0, 1)));
        let mut content = line(&a);
        content.extend(line(&b));
        let third = line(&env(3, at(13, 0, 2)));
        content.extend(&third[..third.len() / 2]);
        let path = dir.path().join("date=2026-03-05/hour=13/r.jsonl");
        write_lines(&path, &content);
        let files = run_files(dir.path(), "r").unwrap();
        assert_eq!(files, vec![path]);
        assert_eq!(read_all(files), vec![a, b]);
    }

    #[test]
    fn truncated_line_in_a_non_last_file_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let first = line(&env(1, at(13, 0, 0)));
        write_lines(
            &dir.path().join("date=2026-03-05/hour=13/r.jsonl"),
            &first[..first.len() - 8],
        );
        write_lines(
            &dir.path().join("date=2026-03-05/hour=14/r.jsonl"),
            &line(&env(2, at(14, 0, 0))),
        );
        let files = run_files(dir.path(), "r").unwrap();
        let results: Vec<_> = LogReader::open(files).collect();
        assert_eq!(results.len(), 1);
        assert!(matches!(results[0], Err(EngineLogError::Json(_))));
    }

    #[test]
    fn garbage_in_a_middle_line_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let mut content = line(&env(1, at(13, 0, 0)));
        content.extend(b"this is not json\n");
        content.extend(line(&env(3, at(13, 0, 2))));
        write_lines(
            &dir.path().join("date=2026-03-05/hour=13/r.jsonl"),
            &content,
        );
        let files = run_files(dir.path(), "r").unwrap();
        let results: Vec<_> = LogReader::open(files).collect();
        assert_eq!(results.len(), 2, "the iterator ends after the error");
        assert!(results[0].is_ok());
        assert!(matches!(results[1], Err(EngineLogError::Json(_))));
    }

    #[test]
    fn run_files_ignores_other_run_ids() {
        let dir = tempfile::tempdir().unwrap();
        for run in ["run-a", "run-b"] {
            let mut w = EngineLogWriter::new(dir.path(), run);
            w.append(&env(1, at(13, 0, 0))).unwrap();
            w.append(&env(2, at(15, 0, 0))).unwrap();
            w.finish().unwrap();
        }
        // Noise: a stray directory, a temp file and a name that merely starts with the run id.
        write_lines(
            &dir.path()
                .join("date=2026-03-05/hour=14/run-a.jsonl.zst.tmp"),
            b"x",
        );
        write_lines(
            &dir.path().join("date=2026-03-05/hour=14/run-a2.jsonl"),
            b"x",
        );
        write_lines(&dir.path().join("date=bogus/hour=01/run-a.jsonl"), b"x");
        let files = run_files(dir.path(), "run-a").unwrap();
        assert_eq!(files.len(), 2);
        assert!(
            files
                .iter()
                .all(|f| f.file_name().unwrap() == "run-a.jsonl.zst")
        );
        assert!(files[0].to_string_lossy().contains("hour=13"));
        assert!(files[1].to_string_lossy().contains("hour=15"));
        assert!(run_files(dir.path(), "nope").unwrap().is_empty());
        assert!(
            run_files(&dir.path().join("missing"), "run-a")
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn run_files_prefers_plain_when_both_exist() {
        let dir = tempfile::tempdir().unwrap();
        let hour = dir.path().join("date=2026-03-05/hour=13");
        write_lines(&hour.join("r.jsonl"), b"");
        write_lines(&hour.join("r.jsonl.zst"), b"");
        let files = run_files(dir.path(), "r").unwrap();
        assert_eq!(files, vec![hour.join("r.jsonl")]);
    }
}
