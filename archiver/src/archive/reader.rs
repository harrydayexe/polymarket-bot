use std::{
    fs::File,
    io::{BufRead, BufReader, Read},
    path::Path,
};

use crate::archive::{
    envelope::{StoredRecord, decode},
    layout::ZST_SUFFIX,
};

/// One line of a raw file: its record, or why it could not be read.
pub type ReadLine = (usize, Result<StoredRecord, String>);

/// The lines of a raw file, plain or zstd-compressed (by its `.jsonl.zst` name), without
/// their newlines.
///
/// # Errors
///
/// If the file cannot be opened. Reading errors come from the iterator.
pub fn read_lines(path: &Path) -> std::io::Result<impl Iterator<Item = std::io::Result<Vec<u8>>>> {
    let file = File::open(path)?;
    let input: Box<dyn Read> = if path.to_string_lossy().ends_with(ZST_SUFFIX) {
        Box::new(zstd::Decoder::new(file)?)
    } else {
        Box::new(file)
    };
    Ok(BufReader::with_capacity(1 << 20, input).split(b'\n'))
}

/// Reads the records of one raw file, numbering lines from 1. A line that is not valid UTF-8
/// or not a valid record (such as the final line of a file cut off by a crash) is an `Err`;
/// reading carries on after it.
///
/// # Errors
///
/// If the file cannot be opened, or reading it fails.
pub fn read_records(
    path: &Path,
) -> std::io::Result<impl Iterator<Item = std::io::Result<ReadLine>>> {
    Ok(read_lines(path)?.enumerate().filter_map(|(i, line)| {
        let line = match line {
            Ok(line) => line,
            Err(e) => return Some(Err(e)),
        };
        // A blank line (such as a trailing newline after a cut-off write) holds no record.
        if line.iter().all(u8::is_ascii_whitespace) {
            return None;
        }
        let record = match std::str::from_utf8(&line) {
            Ok(text) => decode(text).map_err(|e| e.to_string()),
            Err(e) => Err(format!("not UTF-8: {e}")),
        };
        Some(Ok((i + 1, record)))
    }))
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;
    use crate::{
        archive::envelope::{StoredBody, encode},
        messages::{ControlBody, Record},
        types::RunId,
    };

    #[test]
    fn reads_records_and_reports_bad_lines() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("f.jsonl");
        let run = RunId::new("r-1");
        let mut text = vec![];
        for seq in 1..=2 {
            let record = Record::Control {
                recv_ts: chrono::Utc.timestamp_micros(1).unwrap(),
                conn_id: None,
                body: ControlBody::RunStop { clean: true },
            };
            encode(&record, seq, &run, &mut text).unwrap();
        }
        text.extend_from_slice(b"{\"recv_ts\":1,\"se");
        std::fs::write(&path, text).unwrap();

        let lines: Vec<ReadLine> = read_records(&path).unwrap().map(Result::unwrap).collect();
        assert_eq!(lines.len(), 3);
        assert_eq!(lines[0].0, 1);
        let first = lines[0].1.as_ref().unwrap();
        assert_eq!(first.seq, 1);
        assert_eq!(
            first.body,
            StoredBody::Control(ControlBody::RunStop { clean: true })
        );
        assert_eq!(lines[1].1.as_ref().unwrap().seq, 2);
        assert_eq!(lines[2].0, 3);
        assert!(lines[2].1.is_err());

        let zst = dir.path().join("f.jsonl.zst");
        std::fs::write(
            &zst,
            zstd::encode_all(&std::fs::read(&path).unwrap()[..], 3).unwrap(),
        )
        .unwrap();
        let again: Vec<ReadLine> = read_records(&zst).unwrap().map(Result::unwrap).collect();
        assert_eq!(again, lines);
    }
}
