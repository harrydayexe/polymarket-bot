//! Reading a day's raw files for `verify`, in archive order, without holding them in memory.
//!
//! Archive order is run, then `seq`: `seq` is a total order within a run, across every stream.
//! Runs are ordered by the earliest receive time among the first records of their files. Each
//! run's files are read together, merged by `seq`, one record at a time.

use std::{
    cmp::Reverse,
    collections::{BTreeMap, BinaryHeap},
    path::{Path, PathBuf},
};

use chrono::{DateTime, NaiveDate, Utc};

use crate::{
    archive::{
        envelope::StoredRecord,
        layout::{OPEN_SUFFIX, Stream, files_for_date},
        reader::{ReadLine, read_records},
    },
    types::RunId,
};

/// How many malformed lines are logged one by one.
const MALFORMED_LOGGED: u64 = 20;

/// The finished files of a day, and the unfinished ones left out.
#[derive(Debug, Default)]
pub struct DayFiles {
    /// Websocket and snapshot files.
    pub market_data: Vec<PathBuf>,
    pub control: Vec<PathBuf>,
    /// `.jsonl.open` files of the day's websocket, snapshot and control folders.
    pub open: Vec<PathBuf>,
}

impl DayFiles {
    pub fn find(root: &Path, date: NaiveDate) -> std::io::Result<Self> {
        let mut files = DayFiles::default();
        for stream in [Stream::Ws, Stream::Snapshots, Stream::Control] {
            for path in files_for_date(root, stream, date, true)? {
                let open = path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.ends_with(OPEN_SUFFIX));
                if open {
                    files.open.push(path);
                } else if stream == Stream::Control {
                    files.control.push(path);
                } else {
                    files.market_data.push(path);
                }
            }
        }
        Ok(files)
    }
}

/// Counts what was read and logs malformed lines.
#[derive(Debug, Default)]
pub struct Tally {
    pub records: u64,
    pub malformed: u64,
}

impl Tally {
    fn line(&mut self, path: &Path, line: ReadLine) -> Option<StoredRecord> {
        match line {
            (_, Ok(record)) => {
                self.records += 1;
                Some(record)
            }
            (n, Err(e)) => {
                self.malformed += 1;
                if self.malformed <= MALFORMED_LOGGED {
                    tracing::warn!("skipping malformed line {}:{n}: {e}", path.display());
                } else if self.malformed == MALFORMED_LOGGED + 1 {
                    tracing::warn!("more malformed lines; not logging each one");
                }
                None
            }
        }
    }
}

/// Every record of `files`, in file order. For the control files, which are small.
pub fn read_all(files: &[PathBuf], tally: &mut Tally) -> std::io::Result<Vec<StoredRecord>> {
    let mut records = vec![];
    for path in files {
        for line in read_records(path)? {
            records.extend(tally.line(path, line?));
        }
    }
    Ok(records)
}

/// One run's files.
#[derive(Debug, PartialEq, Eq)]
pub struct RunFiles {
    pub run: RunId,
    pub first_recv: DateTime<Utc>,
    pub files: Vec<PathBuf>,
}

/// Groups `files` by the run of their first record, ordered by the runs' first receive time.
/// A file without any valid record is left out (its lines are counted as malformed).
pub fn group_by_run(files: &[PathBuf], tally: &mut Tally) -> std::io::Result<Vec<RunFiles>> {
    let mut runs: BTreeMap<RunId, RunFiles> = BTreeMap::new();
    for path in files {
        // Only the first valid record is needed here; lines before it are counted when the
        // file is read again for the merge.
        let mut first = None;
        let mut malformed = 0;
        for line in read_records(path)? {
            match line? {
                (_, Ok(record)) => {
                    first = Some(record);
                    break;
                }
                (_, Err(_)) => malformed += 1,
            }
        }
        let Some(first) = first else {
            if malformed > 0 {
                tracing::warn!(
                    "skipping {}: no valid record ({malformed} malformed lines)",
                    path.display()
                );
                tally.malformed += malformed;
            }
            continue;
        };
        let run = runs
            .entry(first.run_id.clone())
            .or_insert_with(|| RunFiles {
                run: first.run_id.clone(),
                first_recv: first.recv_ts,
                files: vec![],
            });
        run.first_recv = run.first_recv.min(first.recv_ts);
        run.files.push(path.clone());
    }
    let mut runs: Vec<RunFiles> = runs.into_values().collect();
    runs.sort_by(|a, b| (a.first_recv, &a.run).cmp(&(b.first_recv, &b.run)));
    Ok(runs)
}

type Lines<'a> = Box<dyn Iterator<Item = std::io::Result<ReadLine>> + 'a>;

/// Reads `files` (all of one run) merged by `seq`, handing each record to `each`. Within a
/// file records are in `seq` order, so only the next record of each file is held.
pub fn merge_by_seq<'a>(
    files: &'a [PathBuf],
    tally: &mut Tally,
    mut each: impl FnMut(StoredRecord),
) -> std::io::Result<()> {
    let mut sources: Vec<(&Path, Lines<'a>)> = vec![];
    for path in files {
        sources.push((path, Box::new(read_records(path)?)));
    }
    let mut heads: Vec<Option<StoredRecord>> = vec![None; sources.len()];
    let mut heap = BinaryHeap::new();

    let mut advance = |i: usize,
                       heads: &mut Vec<Option<StoredRecord>>,
                       heap: &mut BinaryHeap<Reverse<(u64, usize)>>|
     -> std::io::Result<()> {
        let (path, lines) = &mut sources[i];
        for line in lines.by_ref() {
            if let Some(record) = tally.line(path, line?) {
                heap.push(Reverse((record.seq, i)));
                heads[i] = Some(record);
                break;
            }
        }
        Ok(())
    };

    for i in 0..heads.len() {
        advance(i, &mut heads, &mut heap)?;
    }
    while let Some(Reverse((_, i))) = heap.pop() {
        let record = heads[i].take().expect("a queued file has a record");
        advance(i, &mut heads, &mut heap)?;
        each(record);
    }
    Ok(())
}
