//! The health rules of section 13, as a pure function of the status file and the time.

use chrono::{DateTime, NaiveDate, TimeDelta, Utc};

use crate::status::status_file::StatusFile;

/// Older than this, the status file means the recorder is stopped or stuck.
const STALE_AFTER: TimeDelta = TimeDelta::seconds(30);
/// A connection not streaming for longer than this means reconnects are failing.
const NOT_STREAMING_AFTER: TimeDelta = TimeDelta::minutes(5);
/// No message on any connection for longer than this means something is wrong.
const SILENT_AFTER: TimeDelta = TimeDelta::minutes(10);

/// Whether yesterday's compaction is late.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compaction {
    /// Before 06:00 UTC, or no data for yesterday.
    NotDue,
    Done,
    Missing {
        date: NaiveDate,
    },
}

/// Why the recorder is unhealthy; empty when healthy.
pub fn evaluate(
    status: &StatusFile,
    now: DateTime<Utc>,
    min_free_disk_gb: u64,
    compaction: Compaction,
) -> Vec<String> {
    let mut problems = vec![];
    let age = now - status.written_at;
    if status.clean_stop {
        problems.push(format!(
            "the recorder is not running: run {} stopped cleanly at {}",
            status.run_id, status.written_at
        ));
    } else if age > STALE_AFTER {
        problems.push(format!(
            "status file last written {}s ago: the recorder is stopped or stuck",
            age.num_seconds()
        ));
    }

    for c in &status.connections {
        let down_for = now - c.since;
        if c.state != "streaming" && down_for > NOT_STREAMING_AFTER {
            problems.push(format!(
                "connection {} not streaming ({}) for {} minutes",
                c.slot,
                c.state,
                down_for.num_minutes()
            ));
        }
    }

    let running_for = now - status.started_at;
    if status.tokens_tracked == 0 {
        if running_for > SILENT_AFTER {
            problems.push("no tokens tracked".to_string());
        }
    } else {
        let silent_for = now - status.last_message_at.unwrap_or(status.started_at);
        if silent_for > SILENT_AFTER {
            problems.push(format!(
                "no message on any connection for {} minutes",
                silent_for.num_minutes()
            ));
        }
    }

    let q = &status.queue;
    if q.depth * 2 > q.capacity {
        problems.push(format!(
            "write queue {}% full ({} of {})",
            q.depth * 100 / q.capacity.max(1),
            q.depth,
            q.capacity
        ));
    }
    if q.dropped_last_hour > 0 {
        problems.push(format!(
            "{} frames dropped in the last hour (write queue full)",
            q.dropped_last_hour
        ));
    }

    if let Some(free) = status.disk_free_gb
        && free < min_free_disk_gb as f64
    {
        problems.push(format!(
            "free disk {free:.1} GB is below the minimum of {min_free_disk_gb} GB"
        ));
    }

    if let Compaction::Missing { date } = compaction {
        problems.push(format!("{date} has not been compacted"));
    }
    problems
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        connection_manager::types::ConnSlotId,
        status::status_file::{ConnectionStatus, QueueStatus},
        types::RunId,
    };

    fn at(s: &str) -> DateTime<Utc> {
        s.parse().unwrap()
    }

    fn now() -> DateTime<Utc> {
        at("2026-09-25T14:00:00Z")
    }

    fn healthy() -> StatusFile {
        StatusFile {
            run_id: RunId::new("r-1"),
            program_version: "0.1.0".into(),
            started_at: at("2026-09-25T10:00:00Z"),
            written_at: now() - TimeDelta::seconds(3),
            clean_stop: false,
            last_message_at: Some(now() - TimeDelta::seconds(1)),
            durable_through: Some(now() - TimeDelta::seconds(4)),
            tokens_tracked: 4,
            connections: vec![ConnectionStatus {
                slot: ConnSlotId::new("c-0001"),
                state: "streaming".into(),
                tokens: 4,
                since: at("2026-09-25T10:00:01Z"),
                last_message_at: Some(now() - TimeDelta::seconds(1)),
            }],
            messages_last_minute: 100,
            queue: QueueStatus {
                depth: 10,
                high_water: 300,
                capacity: 100_000,
                dropped_total: 0,
                dropped_last_hour: 0,
            },
            open_gaps: 0,
            tokens_unhealthy: vec![],
            snapshots_last_hour: 4,
            disk_free_gb: Some(100.0),
            current_files: vec![],
            previously_tracked_tokens_file: "state/tracked_tokens.json".into(),
        }
    }

    fn problems(status: &StatusFile) -> Vec<String> {
        evaluate(status, now(), 10, Compaction::NotDue)
    }

    #[test]
    fn all_good() {
        assert!(problems(&healthy()).is_empty());
        assert!(evaluate(&healthy(), now(), 10, Compaction::Done).is_empty());
    }

    #[test]
    fn stale_file() {
        let mut s = healthy();
        s.written_at = now() - TimeDelta::seconds(31);
        assert_eq!(problems(&s).len(), 1);
        assert!(problems(&s)[0].contains("stopped or stuck"));
    }

    #[test]
    fn clean_stop_is_not_running() {
        let mut s = healthy();
        s.clean_stop = true;
        assert!(problems(&s)[0].contains("not running"));
    }

    #[test]
    fn connection_down() {
        let mut s = healthy();
        s.connections[0].state = "backoff".into();
        s.connections[0].since = now() - TimeDelta::minutes(6);
        let p = problems(&s);
        assert_eq!(p.len(), 1);
        assert!(p[0].contains("c-0001"), "{p:?}");

        s.connections[0].since = now() - TimeDelta::minutes(4);
        assert!(problems(&s).is_empty());
    }

    #[test]
    fn total_silence() {
        let mut s = healthy();
        s.last_message_at = Some(now() - TimeDelta::minutes(11));
        assert!(problems(&s)[0].contains("no message"));
        s.last_message_at = None;
        assert!(problems(&s)[0].contains("no message"));
    }

    #[test]
    fn nothing_tracked() {
        let mut s = healthy();
        s.tokens_tracked = 0;
        s.last_message_at = None;
        assert_eq!(problems(&s), ["no tokens tracked"]);
    }

    #[test]
    fn queue_pressure() {
        let mut s = healthy();
        s.queue.depth = 60_000;
        assert!(problems(&s)[0].contains("60% full"));
        let mut s = healthy();
        s.queue.dropped_last_hour = 3;
        assert!(problems(&s)[0].contains("dropped"));
    }

    #[test]
    fn disk() {
        let mut s = healthy();
        s.disk_free_gb = Some(8.0);
        assert!(problems(&s)[0].contains("free disk"));
    }

    #[test]
    fn compaction_late() {
        let date = NaiveDate::from_ymd_opt(2026, 9, 24).unwrap();
        let p = evaluate(&healthy(), now(), 10, Compaction::Missing { date });
        assert_eq!(p, ["2026-09-24 has not been compacted"]);
    }

    #[test]
    fn several_problems() {
        let mut s = healthy();
        s.written_at = now() - TimeDelta::minutes(1);
        s.disk_free_gb = Some(1.0);
        assert_eq!(problems(&s).len(), 2);
    }
}
