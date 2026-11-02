//! Compressing finished raw market data files, which are several times smaller as zstd.
//!
//! A finished `x.jsonl` becomes `x.jsonl.zst` through a temporary `x.jsonl.zst.tmp`, synced
//! and renamed before the original is removed, so a crash at any point leaves a complete
//! copy (readers prefer the `.zst` when both exist).

use std::{
    fs::File,
    io::{BufReader, Read, Write},
    path::{Path, PathBuf},
    sync::atomic::{AtomicBool, Ordering},
};

use crate::archive::layout::{DONE_SUFFIX, Stream, ZST_SUFFIX};

const LEVEL: i32 = 3;
const TMP_SUFFIX: &str = ".tmp";

/// Compresses every finished `.jsonl` file of the websocket and snapshot streams, and removes
/// temporary files left by an interrupted run. Stops early, leaving the current file as it
/// was, once `stop` is set. Blocking.
///
/// # Returns
///
/// How many files were compressed.
///
/// # Errors
///
/// If a folder cannot be listed or a file cannot be compressed.
pub fn compress_finished(root: &Path, stop: &AtomicBool) -> std::io::Result<usize> {
    let mut files = vec![];
    for stream in [Stream::Ws, Stream::Snapshots] {
        walk(&root.join(stream.dir()), &mut files)?;
    }
    files.sort();

    let mut compressed = 0;
    for path in files {
        if stop.load(Ordering::Relaxed) {
            break;
        }
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or_default();
        if name.ends_with(&format!("{ZST_SUFFIX}{TMP_SUFFIX}")) {
            std::fs::remove_file(&path)?;
        } else if name.ends_with(DONE_SUFFIX) && compress_file(&path, stop)? {
            compressed += 1;
        }
    }
    Ok(compressed)
}

/// Compresses one finished file.
///
/// # Returns
///
/// `false` if it stopped early because `stop` was set.
pub fn compress_file(path: &Path, stop: &AtomicBool) -> std::io::Result<bool> {
    let target = PathBuf::from(format!("{}{}", path.display(), ".zst"));
    if target.exists() {
        // A crash after the rename, before the original was removed.
        std::fs::remove_file(path)?;
        return Ok(true);
    }
    let tmp = PathBuf::from(format!("{}{TMP_SUFFIX}", target.display()));

    let mut input = BufReader::new(File::open(path)?);
    let mut encoder = zstd::Encoder::new(File::create(&tmp)?, LEVEL)?;
    let mut buf = vec![0u8; 1 << 20];
    loop {
        if stop.load(Ordering::Relaxed) {
            drop(encoder);
            std::fs::remove_file(&tmp)?;
            return Ok(false);
        }
        let n = input.read(&mut buf)?;
        if n == 0 {
            break;
        }
        encoder.write_all(&buf[..n])?;
    }
    let file = encoder.finish()?;
    file.sync_all()?;
    std::fs::rename(&tmp, &target)?;
    std::fs::remove_file(path)?;
    Ok(true)
}

fn walk(dir: &Path, found: &mut Vec<PathBuf>) -> std::io::Result<()> {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e),
    };
    for entry in entries {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            walk(&entry.path(), found)?;
        } else {
            found.push(entry.path());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;

    use super::*;
    use crate::archive::layout::files_for_date;

    #[test]
    fn compresses_finished_files_only() {
        let dir = tempfile::tempdir().unwrap();
        let hour = dir.path().join("raw/ws/date=2026-09-25/hour=14");
        std::fs::create_dir_all(&hour).unwrap();
        let text = "{\"a\":1}\n".repeat(10_000);
        std::fs::write(hour.join("r-1.jsonl"), &text).unwrap();
        std::fs::write(hour.join("r-2.jsonl.open"), "x").unwrap();
        std::fs::write(hour.join("r-0.jsonl.zst.tmp"), "partial").unwrap();
        let control = dir.path().join("control/date=2026-09-25");
        std::fs::create_dir_all(&control).unwrap();
        std::fs::write(control.join("r-1.jsonl"), "{}\n").unwrap();

        let n = compress_finished(dir.path(), &AtomicBool::new(false)).unwrap();
        assert_eq!(n, 1);
        let mut names: Vec<String> = std::fs::read_dir(&hour)
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        names.sort();
        assert_eq!(names, ["r-1.jsonl.zst", "r-2.jsonl.open"]);
        assert!(
            control.join("r-1.jsonl").exists(),
            "control files stay as they are"
        );

        let zst = std::fs::read(hour.join("r-1.jsonl.zst")).unwrap();
        assert!(zst.len() < text.len() / 10);
        assert_eq!(zstd::decode_all(&zst[..]).unwrap(), text.as_bytes());
    }

    #[test]
    fn a_stop_leaves_the_original() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("r-1.jsonl");
        std::fs::write(&path, "{}\n").unwrap();
        assert!(!compress_file(&path, &AtomicBool::new(true)).unwrap());
        assert!(path.exists());
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
    }

    #[test]
    fn readers_prefer_the_compressed_copy() {
        let dir = tempfile::tempdir().unwrap();
        let hour = dir.path().join("raw/ws/date=2026-09-25/hour=14");
        std::fs::create_dir_all(&hour).unwrap();
        // As left by a crash between the rename and the removal.
        std::fs::write(hour.join("r-1.jsonl"), "{}\n").unwrap();
        std::fs::write(
            hour.join("r-1.jsonl.zst"),
            zstd::encode_all(&b"{}\n"[..], 3).unwrap(),
        )
        .unwrap();
        std::fs::write(hour.join("r-2.jsonl"), "{}\n").unwrap();
        let date = NaiveDate::from_ymd_opt(2026, 9, 25).unwrap();
        let files = files_for_date(dir.path(), Stream::Ws, date, false).unwrap();
        assert_eq!(files, [hour.join("r-1.jsonl.zst"), hour.join("r-2.jsonl")]);

        assert!(compress_file(&hour.join("r-1.jsonl"), &AtomicBool::new(false)).unwrap());
        assert!(!hour.join("r-1.jsonl").exists());
    }
}
