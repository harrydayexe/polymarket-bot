//! Startup recovery (sections 7 and 8.4): finish the files a crashed run left open, and work
//! out the `process_restart` gap from the previous run's status file.

use std::{collections::HashSet, fs::File, path::Path};

use anyhow::{Context, Result};
use chrono::{DateTime, TimeDelta, Utc};
use marcasite::types::TokenId;

use crate::{
    archive::layout::{
        LOCK_FILE, STATE_DIR, STATUS_FILE, TRACKED_TOKENS_FILE, done_path, open_files,
    },
    status::status_file::{StatusFile, TrackedTokensFile, read_json},
    types::RunId,
};

/// The data lost between a run that did not stop cleanly and this one.
#[derive(Debug, Clone, PartialEq)]
pub struct RestartGap {
    pub tokens: HashSet<TokenId>,
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
}

#[derive(Debug, Default, PartialEq)]
pub struct Recovery {
    pub previous_run: Option<RunId>,
    pub restart_gap: Option<RestartGap>,
    /// `.jsonl.open` files renamed to `.jsonl`.
    pub files_finished: usize,
}

/// What the previous run left behind.
#[derive(Debug)]
pub enum PreviousRun {
    /// No status file: the first run in this archive.
    None,
    Status(Box<StatusFile>),
    /// A status file that could not be read, last modified at the given time.
    Unreadable {
        modified: Option<DateTime<Utc>>,
    },
}

/// Locks the archive for this process, so two recorders never write to it at once. The lock
/// is held until the returned file is dropped (or the process ends).
///
/// # Errors
///
/// If another process holds the lock, or the state folder cannot be written.
pub fn lock_archive(root: &Path) -> Result<File> {
    std::fs::create_dir_all(root.join(STATE_DIR))
        .with_context(|| format!("could not create {}", root.join(STATE_DIR).display()))?;
    let path = root.join(LOCK_FILE);
    let file = File::create(&path).with_context(|| format!("could not open {}", path.display()))?;
    match file.try_lock() {
        Ok(()) => Ok(file),
        Err(std::fs::TryLockError::WouldBlock) => anyhow::bail!(
            "another archiver is already running on {} (it holds {})",
            root.display(),
            path.display()
        ),
        Err(std::fs::TryLockError::Error(e)) => {
            Err(e).with_context(|| format!("could not lock {}", path.display()))
        }
    }
}

/// Reads the previous run's state, works out the restart gap, and renames leftover open files.
/// Call with the archive locked, before anything is written.
///
/// # Errors
///
/// If a leftover file cannot be renamed.
pub fn recover(root: &Path, now: DateTime<Utc>) -> Result<Recovery> {
    let previous = read_previous(root);
    let tokens = match &previous {
        PreviousRun::None => HashSet::new(),
        _ => read_tracked_tokens(root),
    };
    let restart_gap = restart_gap(&previous, tokens, now);
    let previous_run = match &previous {
        PreviousRun::Status(status) => Some(status.run_id.clone()),
        _ => None,
    };

    let mut files_finished = 0;
    for open in open_files(root).context("could not list leftover open files")? {
        let done = done_path(&open).expect("open files end in .jsonl.open");
        std::fs::rename(&open, &done)
            .with_context(|| format!("could not rename {}", open.display()))?;
        tracing::warn!(path = %done.display(), "finished a file left open by the previous run");
        files_finished += 1;
    }

    Ok(Recovery {
        previous_run,
        restart_gap,
        files_finished,
    })
}

fn read_previous(root: &Path) -> PreviousRun {
    let path = root.join(STATUS_FILE);
    match read_json::<StatusFile>(&path) {
        Ok(None) => PreviousRun::None,
        Ok(Some(status)) => PreviousRun::Status(Box::new(status)),
        Err(e) => {
            tracing::warn!(
                error = format!("{e:#}"),
                "previous status file unreadable, treating the last run as crashed"
            );
            let modified = std::fs::metadata(&path)
                .and_then(|m| m.modified())
                .ok()
                .map(DateTime::<Utc>::from);
            PreviousRun::Unreadable { modified }
        }
    }
}

fn read_tracked_tokens(root: &Path) -> HashSet<TokenId> {
    match read_json::<TrackedTokensFile>(&root.join(TRACKED_TOKENS_FILE)) {
        Ok(Some(file)) => file.tokens.into_iter().collect(),
        Ok(None) => HashSet::new(),
        Err(e) => {
            tracing::warn!(
                error = format!("{e:#}"),
                "previously tracked tokens unreadable"
            );
            HashSet::new()
        }
    }
}

/// The `process_restart` gap for the previous run, if it did not stop cleanly: from the last
/// data known to be on disk to `now`, for every token it tracked.
///
/// The gap starts at the latest websocket data synced to disk, or failing that the run's
/// start. An unreadable status file is dated from its modification time, less the status
/// interval and sync interval it could be behind by.
pub fn restart_gap(
    previous: &PreviousRun,
    tokens: HashSet<TokenId>,
    now: DateTime<Utc>,
) -> Option<RestartGap> {
    let start = match previous {
        PreviousRun::None => return None,
        PreviousRun::Status(status) if status.clean_stop => return None,
        PreviousRun::Status(status) => status.durable_through.unwrap_or(status.started_at),
        PreviousRun::Unreadable { modified } => {
            modified.map(|m| m - TimeDelta::seconds(10)).unwrap_or(now)
        }
    };
    if tokens.is_empty() {
        tracing::warn!("previous run did not stop cleanly, but no tracked tokens are known");
        return None;
    }
    Some(RestartGap {
        tokens,
        start: start.min(now),
        end: now,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::status::status_file::{QueueStatus, write_json_atomic};

    fn at(s: &str) -> DateTime<Utc> {
        s.parse().unwrap()
    }

    fn status(clean_stop: bool, durable_through: Option<DateTime<Utc>>) -> StatusFile {
        StatusFile {
            run_id: RunId::new("r-old"),
            program_version: "0.1.0".into(),
            started_at: at("2026-09-25T09:00:00Z"),
            written_at: at("2026-09-25T09:58:03Z"),
            clean_stop,
            last_message_at: durable_through,
            durable_through,
            tokens_tracked: 2,
            connections: vec![],
            messages_last_minute: 0,
            queue: QueueStatus {
                depth: 0,
                high_water: 0,
                capacity: 1,
                dropped_total: 0,
                dropped_last_hour: 0,
            },
            open_gaps: 0,
            tokens_unhealthy: vec![],
            snapshots_last_hour: 0,
            disk_free_gb: None,
            current_files: vec![],
            previously_tracked_tokens_file: TRACKED_TOKENS_FILE.into(),
        }
    }

    fn tokens() -> HashSet<TokenId> {
        HashSet::from([TokenId::new("1"), TokenId::new("2")])
    }

    fn write_state(root: &Path, status: Option<&StatusFile>) {
        std::fs::create_dir_all(root.join(STATE_DIR)).unwrap();
        if let Some(status) = status {
            write_json_atomic(&root.join(STATUS_FILE), status).unwrap();
        }
        write_json_atomic(
            &root.join(TRACKED_TOKENS_FILE),
            &TrackedTokensFile {
                run_id: RunId::new("r-old"),
                written_at: at("2026-09-25T09:00:00Z"),
                tokens: vec![TokenId::new("1"), TokenId::new("2")],
            },
        )
        .unwrap();
    }

    #[test]
    fn clean_stop_has_no_gap() {
        let dir = tempfile::tempdir().unwrap();
        write_state(dir.path(), Some(&status(true, None)));
        let r = recover(dir.path(), at("2026-09-25T10:05:00Z")).unwrap();
        assert_eq!(r.restart_gap, None);
        assert_eq!(r.previous_run, Some(RunId::new("r-old")));
    }

    #[test]
    fn crash_gap_covers_every_tracked_token() {
        let dir = tempfile::tempdir().unwrap();
        write_state(
            dir.path(),
            Some(&status(false, Some(at("2026-09-25T09:58:00Z")))),
        );
        let r = recover(dir.path(), at("2026-09-25T10:05:00Z")).unwrap();
        assert_eq!(
            r.restart_gap,
            Some(RestartGap {
                tokens: tokens(),
                start: at("2026-09-25T09:58:00Z"),
                end: at("2026-09-25T10:05:00Z"),
            })
        );
    }

    #[test]
    fn crash_without_data_starts_at_the_previous_start() {
        let previous = PreviousRun::Status(Box::new(status(false, None)));
        let gap = restart_gap(&previous, tokens(), at("2026-09-25T10:05:00Z")).unwrap();
        assert_eq!(gap.start, at("2026-09-25T09:00:00Z"));
    }

    #[test]
    fn leftover_files_are_renamed() {
        let dir = tempfile::tempdir().unwrap();
        let ws = dir.path().join("raw/ws/date=2026-09-25/hour=09");
        let control = dir.path().join("control/date=2026-09-25");
        std::fs::create_dir_all(&ws).unwrap();
        std::fs::create_dir_all(&control).unwrap();
        std::fs::write(ws.join("r-old.jsonl.open"), "{}\n{\"cut").unwrap();
        std::fs::write(control.join("r-old.jsonl.open"), "").unwrap();
        let r = recover(dir.path(), at("2026-09-25T10:05:00Z")).unwrap();
        assert_eq!(r.files_finished, 2);
        assert!(ws.join("r-old.jsonl").exists());
        assert!(control.join("r-old.jsonl").exists());
        assert!(!ws.join("r-old.jsonl.open").exists());
    }

    #[test]
    fn first_ever_run() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(
            recover(dir.path(), at("2026-09-25T10:05:00Z")).unwrap(),
            Recovery::default()
        );
    }

    #[test]
    fn corrupt_status_file_is_treated_as_a_crash() {
        let dir = tempfile::tempdir().unwrap();
        write_state(dir.path(), None);
        std::fs::write(dir.path().join(STATUS_FILE), "{\"run_id\": \"r-o").unwrap();
        let now = Utc::now() + TimeDelta::minutes(5);
        let r = recover(dir.path(), now).unwrap();
        let gap = r.restart_gap.expect("a gap");
        assert_eq!(gap.tokens, tokens());
        assert_eq!(gap.end, now);
        assert!(
            gap.start < now - TimeDelta::minutes(4),
            "dated from the file"
        );
        assert_eq!(r.previous_run, None);
    }

    #[test]
    fn the_lock_is_exclusive() {
        let dir = tempfile::tempdir().unwrap();
        let held = lock_archive(dir.path()).unwrap();
        let err = lock_archive(dir.path()).unwrap_err();
        assert!(format!("{err:#}").contains("already running"), "{err:#}");
        drop(held);
        lock_archive(dir.path()).unwrap();
    }
}
