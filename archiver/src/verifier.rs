//! The offline consistency check (`archiver verify`, design section 10 "Offline check").
//!
//! For one day, every token's book is rebuilt from each full book (the anchor) with the level
//! changes recorded after it, and compared with the token's next full book. A mismatch means
//! data was lost without a recorded gap, or that the rebuild's assumptions are wrong (design
//! section 16). See [checker] for how windows are formed and judged, [coverage] for gaps.
//!
//! [verify_day] does the file reading; the checking itself is pure and fed one record at a
//! time, so a day is never held in memory: only each token's current anchor and the changes
//! since it, and the day's gap intervals.

pub mod checker;
pub mod coverage;
pub mod input;
pub mod report;

use std::{
    fs::File,
    io::{BufWriter, Write},
    path::{Path, PathBuf},
};

use anyhow::{Context, bail};
use chrono::NaiveDate;

use crate::verifier::{
    checker::Checker,
    coverage::Coverage,
    input::{DayFiles, Tally, group_by_run, merge_by_seq, read_all},
    report::{InputStats, VerifyReport},
};

/// Folder of the reports, relative to the archive root.
pub const REPORT_DIR: &str = "reports/verify";

/// How many unfinished files are named in the warning about them.
const OPEN_FILES_NAMED: usize = 5;

/// Checks the finished websocket, snapshot and control files of `date` under the archive at
/// `root`. Unfinished (`.jsonl.open`) files are not read; a warning names them.
///
/// # Errors
///
/// If the archive folder does not exist or a file cannot be read. Malformed lines are skipped
/// and counted, not errors.
pub fn verify_day(root: &Path, date: NaiveDate) -> anyhow::Result<VerifyReport> {
    if !root.is_dir() {
        bail!("archive folder {} does not exist", root.display());
    }
    let files = DayFiles::find(root, date)
        .with_context(|| format!("listing the files of {date} in {}", root.display()))?;
    if !files.open.is_empty() {
        let named: Vec<String> = files
            .open
            .iter()
            .take(OPEN_FILES_NAMED)
            .map(|p| p.display().to_string())
            .collect();
        tracing::warn!(
            "{} unfinished file(s) for {date} not read (is the recorder still writing this day?): {}{}",
            files.open.len(),
            named.join(", "),
            if files.open.len() > OPEN_FILES_NAMED {
                ", ..."
            } else {
                ""
            }
        );
    }
    if files.market_data.is_empty() {
        tracing::warn!("no websocket or snapshot files for {date}");
    }

    let mut tally = Tally::default();
    let control = read_all(&files.control, &mut tally).context("reading control files")?;
    let mut checker = Checker::new(Coverage::from_control(control));
    let runs = group_by_run(&files.market_data, &mut tally).context("reading raw files")?;
    for run in runs {
        checker.begin_run(run.run);
        merge_by_seq(&run.files, &mut tally, |record| checker.observe(&record))
            .context("reading raw files")?;
    }

    let input = InputStats {
        files_read: files.market_data.len() + files.control.len(),
        records_read: tally.records,
        malformed_lines: tally.malformed,
        open_files_ignored: files.open.len(),
        unusable_snapshot_books: 0,
    };
    Ok(checker.finish(date, input))
}

/// `<root>/reports/verify/date=YYYY-MM-DD.json`.
pub fn report_path(root: &Path, date: NaiveDate) -> PathBuf {
    root.join(REPORT_DIR)
        .join(format!("date={}.json", date.format("%Y-%m-%d")))
}

/// Writes `report` as JSON to its [report_path], replacing any earlier report for the day.
/// Written to a temporary file first, so a reader never sees half a report.
pub fn write_report(root: &Path, report: &VerifyReport) -> anyhow::Result<PathBuf> {
    let path = report_path(root, report.date);
    let dir = path.parent().expect("the report path has a folder");
    std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    let tmp = path.with_extension("json.tmp");
    let write = || -> anyhow::Result<()> {
        let mut out = BufWriter::new(File::create(&tmp)?);
        serde_json::to_writer(&mut out, report)?;
        out.write_all(b"\n")?;
        out.into_inner().map_err(|e| e.into_error())?.sync_all()?;
        std::fs::rename(&tmp, &path)?;
        Ok(())
    };
    write().with_context(|| format!("writing {}", path.display()))?;
    Ok(path)
}

#[cfg(test)]
mod tests;
