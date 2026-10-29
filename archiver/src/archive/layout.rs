use std::path::{Path, PathBuf};

use chrono::{DateTime, Datelike, NaiveDate, TimeDelta, Timelike, Utc};

use crate::types::RunId;

/// Suffix of a file still being written. The offline commands never read these.
pub const OPEN_SUFFIX: &str = ".jsonl.open";
/// Suffix of a finished file.
pub const DONE_SUFFIX: &str = ".jsonl";
/// Suffix of a finished file once compressed (market data only, see [super::compress]).
pub const ZST_SUFFIX: &str = ".jsonl.zst";

/// Folder of the state files, relative to the archive root.
pub const STATE_DIR: &str = "state";
pub const STATUS_FILE: &str = "state/status.json";
pub const TRACKED_TOKENS_FILE: &str = "state/tracked_tokens.json";
/// Held locked by `archiver run`, so two recorders never share an archive.
pub const LOCK_FILE: &str = "state/run.lock";
/// Folder of the compacted days.
pub const PARQUET_DIR: &str = "parquet";

/// The four kinds of raw file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Stream {
    Ws,
    Snapshots,
    Control,
    Markets,
}

impl Stream {
    pub const ALL: [Stream; 4] = [
        Stream::Ws,
        Stream::Snapshots,
        Stream::Control,
        Stream::Markets,
    ];

    /// Folder of the stream, relative to the archive root.
    pub fn dir(self) -> &'static str {
        match self {
            Stream::Ws => "raw/ws",
            Stream::Snapshots => "raw/snapshots",
            Stream::Control => "control",
            Stream::Markets => "markets",
        }
    }

    /// Market data is split by hour; control and market records by date only.
    pub fn hourly(self) -> bool {
        matches!(self, Stream::Ws | Stream::Snapshots)
    }
}

/// Which file a record belongs in: its stream and the UTC date (and hour) of its receive time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct FileKey {
    pub stream: Stream,
    pub date: NaiveDate,
    pub hour: Option<u32>,
}

impl FileKey {
    pub fn for_time(stream: Stream, at: DateTime<Utc>) -> Self {
        Self {
            stream,
            date: at.date_naive(),
            hour: stream.hourly().then(|| at.hour()),
        }
    }

    /// The folder holding this key's files.
    pub fn dir(&self, root: &Path) -> PathBuf {
        let date = date_dir(root, self.stream, self.date);
        match self.hour {
            Some(hour) => date.join(format!("hour={hour:02}")),
            None => date,
        }
    }

    /// When the period this key covers ends: the next hour, or the next midnight.
    pub fn period_end(&self) -> DateTime<Utc> {
        let start = self
            .date
            .and_hms_opt(self.hour.unwrap_or(0), 0, 0)
            .expect("valid hour")
            .and_utc();
        start
            + match self.hour {
                Some(_) => TimeDelta::hours(1),
                None => TimeDelta::days(1),
            }
    }
}

/// `<root>/<stream>/date=YYYY-MM-DD`.
pub fn date_dir(root: &Path, stream: Stream, date: NaiveDate) -> PathBuf {
    root.join(stream.dir()).join(format!(
        "date={:04}-{:02}-{:02}",
        date.year(),
        date.month(),
        date.day()
    ))
}

/// File name without its suffix. The first file of a run for a key is named after the run;
/// later ones (only when a late record arrives after its file was closed) get `-p<part>`.
pub fn file_stem(run_id: &RunId, part: u32) -> String {
    if part <= 1 {
        run_id.to_string()
    } else {
        format!("{run_id}-p{part}")
    }
}

/// The finished name of a `.jsonl.open` file.
pub fn done_path(open: &Path) -> Option<PathBuf> {
    let name = open.file_name()?.to_str()?;
    let stem = name.strip_suffix(OPEN_SUFFIX)?;
    Some(open.with_file_name(format!("{stem}{DONE_SUFFIX}")))
}

/// Every raw file of `stream` for `date`, sorted by path: finished files, plain or compressed
/// (only the compressed one when both exist), and `.jsonl.open` files only if `include_open`.
pub fn files_for_date(
    root: &Path,
    stream: Stream,
    date: NaiveDate,
    include_open: bool,
) -> std::io::Result<Vec<PathBuf>> {
    let dir = date_dir(root, stream, date);
    let mut dirs = vec![];
    if stream.hourly() {
        for hour in 0..24 {
            dirs.push(dir.join(format!("hour={hour:02}")));
        }
    } else {
        dirs.push(dir);
    }

    let mut files = vec![];
    for dir in dirs {
        let entries = match std::fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e),
        };
        for entry in entries {
            let path = entry?.path();
            let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            if name.ends_with(DONE_SUFFIX) {
                // Compressed meanwhile, possibly after this listing began.
                let compressed = PathBuf::from(format!("{}.zst", path.display()));
                files.push(if compressed.exists() {
                    compressed
                } else {
                    path
                });
            } else if name.ends_with(ZST_SUFFIX) || (include_open && name.ends_with(OPEN_SUFFIX)) {
                files.push(path);
            }
        }
    }
    files.sort();
    files.dedup();
    Ok(files)
}

/// Every `.jsonl.open` file anywhere under the raw, control and market folders.
pub fn open_files(root: &Path) -> std::io::Result<Vec<PathBuf>> {
    fn walk(dir: &Path, found: &mut Vec<PathBuf>) -> std::io::Result<()> {
        let entries = match std::fs::read_dir(dir) {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e),
        };
        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            if entry.file_type()?.is_dir() {
                walk(&path, found)?;
            } else if path
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.ends_with(OPEN_SUFFIX))
            {
                found.push(path);
            }
        }
        Ok(())
    }

    let mut found = vec![];
    for stream in Stream::ALL {
        walk(&root.join(stream.dir()), &mut found)?;
    }
    found.sort();
    Ok(found)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(s: &str) -> DateTime<Utc> {
        s.parse().unwrap()
    }

    #[test]
    fn hourly_streams_go_in_hour_folders() {
        let key = FileKey::for_time(Stream::Ws, at("2026-09-25T14:23:00Z"));
        assert_eq!(
            key.dir(Path::new("/a")),
            Path::new("/a/raw/ws/date=2026-09-25/hour=14")
        );
        assert_eq!(key.period_end(), at("2026-09-25T15:00:00Z"));
    }

    #[test]
    fn daily_streams_go_in_date_folders() {
        let key = FileKey::for_time(Stream::Control, at("2026-09-25T23:59:59Z"));
        assert_eq!(
            key.dir(Path::new("/a")),
            Path::new("/a/control/date=2026-09-25")
        );
        assert_eq!(key.period_end(), at("2026-09-26T00:00:00Z"));
    }

    #[test]
    fn each_stream_has_its_own_folder() {
        let t = at("2026-09-25T14:00:00Z");
        let dirs: Vec<PathBuf> = Stream::ALL
            .iter()
            .map(|s| FileKey::for_time(*s, t).dir(Path::new("/a")))
            .collect();
        assert_eq!(
            dirs,
            [
                "/a/raw/ws/date=2026-09-25/hour=14",
                "/a/raw/snapshots/date=2026-09-25/hour=14",
                "/a/control/date=2026-09-25",
                "/a/markets/date=2026-09-25",
            ]
            .map(PathBuf::from)
        );
    }

    #[test]
    fn names_carry_the_run_and_part() {
        let run = RunId::new("r-20260925-1400-a1b2");
        assert_eq!(file_stem(&run, 1), "r-20260925-1400-a1b2");
        assert_eq!(file_stem(&run, 2), "r-20260925-1400-a1b2-p2");
        assert_eq!(
            done_path(Path::new("/x/r-1.jsonl.open")).unwrap(),
            Path::new("/x/r-1.jsonl")
        );
        assert_eq!(done_path(Path::new("/x/r-1.jsonl")), None);
    }
}
