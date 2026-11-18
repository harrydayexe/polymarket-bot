//! Where `Persist` rows go. C8 adds the Postgres writer; until then each run writes its rows to
//! `<data_dir>/records/<run_id>.jsonl`, one `{"run_id", "record"}` object per line.

use std::{
    fs::{File, OpenOptions},
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use serde::Serialize;

use crate::action::Record;

/// A destination for stored rows.
pub trait RecordSink: Send {
    /// # Errors
    ///
    /// If the row cannot be written.
    fn write(&mut self, record: &Record) -> std::io::Result<()>;

    /// # Errors
    ///
    /// If buffered rows cannot be written.
    fn flush(&mut self) -> std::io::Result<()>;
}

#[derive(Serialize)]
struct Line<'a> {
    run_id: &'a str,
    record: &'a Record,
}

/// Rows as JSON lines in a file.
pub struct JsonlSink {
    run_id: String,
    path: PathBuf,
    writer: BufWriter<File>,
}

impl JsonlSink {
    /// Creates (or appends to) `<data_dir>/records/<run_id>.jsonl`.
    ///
    /// # Errors
    ///
    /// If the directory or file cannot be created.
    pub fn create(data_dir: &Path, run_id: &str) -> std::io::Result<Self> {
        let dir = data_dir.join("records");
        std::fs::create_dir_all(&dir)?;
        let path = dir.join(format!("{run_id}.jsonl"));
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        Ok(Self {
            run_id: run_id.into(),
            path,
            writer: BufWriter::new(file),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl RecordSink for JsonlSink {
    fn write(&mut self, record: &Record) -> std::io::Result<()> {
        serde_json::to_writer(
            &mut self.writer,
            &Line {
                run_id: &self.run_id,
                record,
            },
        )?;
        self.writer.write_all(b"\n")
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()
    }
}

/// Rows kept in memory, for tests and tools. Clones share the rows.
#[derive(Debug, Clone, Default)]
pub struct MemorySink(pub Arc<Mutex<Vec<Record>>>);

impl MemorySink {
    pub fn records(&self) -> Vec<Record> {
        self.0.lock().map(|rows| rows.clone()).unwrap_or_default()
    }
}

impl RecordSink for MemorySink {
    fn write(&mut self, record: &Record) -> std::io::Result<()> {
        self.0
            .lock()
            .map_err(|_| std::io::Error::other("the memory sink's lock is poisoned"))?
            .push(record.clone());
        Ok(())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use pm_core::test_util::t;

    use super::*;
    use crate::action::Metric;

    #[test]
    fn rows_are_json_lines_with_the_run_id() {
        let dir = tempfile::tempdir().unwrap();
        let mut sink = JsonlSink::create(dir.path(), "r-1").unwrap();
        sink.write(&Record::Metric(Metric::new("m", t(0), 1.0)))
            .unwrap();
        sink.write(&Record::Metric(Metric::new("n", t(1), 2.0)))
            .unwrap();
        sink.flush().unwrap();
        let text = std::fs::read_to_string(sink.path()).unwrap();
        let lines: Vec<serde_json::Value> = text
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0]["run_id"], "r-1");
        assert_eq!(lines[1]["record"]["table"], "metric");
        assert_eq!(lines[1]["record"]["name"], "n");
    }
}
