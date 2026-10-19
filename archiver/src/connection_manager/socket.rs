use std::{
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    },
    time::Duration,
};

use chrono::{DateTime, SubsecRound, Utc};
use marcasite_core::{
    Service,
    ws::{WsConfig, WsConnection, WsSender},
};
use tokio::{
    sync::mpsc::{self, error::TrySendError},
    time::Instant,
};
use tokio_util::sync::CancellationToken;

use crate::{
    clock::SharedClock,
    connection_manager::types::{ConnAttemptId, ConnSlotId, SocketEvent},
    messages::Record,
};

/// The server's reply to the `PING` heartbeat. Reported, not recorded.
const PONG: &str = "PONG";

/// Receive time of the last frame on one connection attempt. Written by its socket task only,
/// read by the connection manager on every tick.
#[derive(Debug, Clone, Default)]
pub struct LastMessageAt(Arc<AtomicI64>);

impl LastMessageAt {
    /// Microseconds since the epoch; 0 means no frame yet.
    const NONE: i64 = 0;

    fn set(&self, at: DateTime<Utc>) {
        self.0.store(at.timestamp_micros(), Ordering::Relaxed);
    }

    pub fn get(&self) -> Option<DateTime<Utc>> {
        match self.0.load(Ordering::Relaxed) {
            Self::NONE => None,
            micros => DateTime::from_timestamp_micros(micros),
        }
    }
}

/// Something a socket task tells the connection manager about its attempt.
#[derive(Debug)]
pub struct SocketReport {
    pub slot: ConnSlotId,
    pub attempt_id: ConnAttemptId,
    pub kind: ReportKind,
}

#[derive(Debug)]
pub enum ReportKind {
    /// The socket opened; frames are sent through this sender.
    Opened(WsSender),
    Event(SocketEvent),
    /// How many frames were dropped because the write queue was full, reported once the
    /// socket has ended.
    FramesDropped(u64),
}

/// Everything a socket task needs to run one connection attempt.
pub struct SocketTask {
    pub slot: ConnSlotId,
    pub attempt_id: ConnAttemptId,
    pub url: url::Url,
    pub connect_timeout: Duration,
    pub write_queue: mpsc::Sender<Record>,
    pub reports: mpsc::UnboundedSender<SocketReport>,
    pub last_message_at: LastMessageAt,
    pub clock: SharedClock,
    /// Cancelled by the manager to close the socket. Frames already received are still
    /// recorded while the close completes.
    pub cancel: CancellationToken,
}

impl SocketTask {
    /// Opens the socket, then reads it until it ends: every frame is stamped with its receive
    /// time and put on the write queue, and nothing else is done with it.
    pub async fn run(self) {
        let config = WsConfig::new(Service::MarketChannel, self.url.clone())
            .connect_timeout(self.connect_timeout);
        let connected = tokio::select! {
            _ = self.cancel.cancelled() => return,
            r = WsConnection::connect(config) => r,
        };
        let mut conn = match connected {
            Ok(conn) => conn,
            Err(e) => {
                self.report(ReportKind::Event(SocketEvent::Failed {
                    error: e.to_string(),
                }));
                return;
            }
        };
        self.report(ReportKind::Opened(conn.sender()));

        let mut closing = false;
        let mut dropped = 0u64;
        loop {
            let item = tokio::select! {
                biased;
                _ = self.cancel.cancelled(), if !closing => {
                    // The driver completes the close handshake within a bounded time, then
                    // ends the stream.
                    conn.close();
                    closing = true;
                    continue;
                }
                item = conn.recv() => item,
            };
            let text = match item {
                Some(Ok(text)) => text,
                Some(Err(e)) => {
                    self.report_closed(closing, e.to_string());
                    break;
                }
                None => {
                    self.report_closed(closing, "closed by the server".into());
                    break;
                }
            };

            // Microseconds, as recorded on disk, so the gap start matches the frame exactly.
            let recv_ts = self.clock.now().trunc_subsecs(6);
            if text == PONG {
                self.report(ReportKind::Event(SocketEvent::PongReceived {
                    at: Instant::now(),
                }));
                continue;
            }
            self.last_message_at.set(recv_ts);
            let record = Record::WsFrame {
                recv_ts,
                conn_id: self.attempt_id.clone(),
                raw: text,
            };
            match self.write_queue.try_send(record) {
                Ok(()) => {}
                Err(TrySendError::Full(_)) => {
                    dropped += 1;
                    if dropped == 1 {
                        self.report(ReportKind::Event(SocketEvent::QueueOverflow));
                    }
                }
                Err(TrySendError::Closed(_)) => {
                    tracing::error!(attempt = %self.attempt_id, "write queue closed, closing socket");
                    self.report_closed(closing, "write queue closed".into());
                    break;
                }
            }
        }

        if dropped > 0 {
            self.report(ReportKind::FramesDropped(dropped));
        }
    }

    /// Reports the end of a socket the manager did not ask to close.
    fn report_closed(&self, closing: bool, reason: String) {
        if !closing {
            self.report(ReportKind::Event(SocketEvent::Closed { reason }));
        }
    }

    fn report(&self, kind: ReportKind) {
        // The manager only goes away at shutdown, when nothing more needs reporting.
        let _ = self.reports.send(SocketReport {
            slot: self.slot.clone(),
            attempt_id: self.attempt_id.clone(),
            kind,
        });
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;

    #[test]
    fn last_message_at_round_trips_with_microsecond_precision() {
        let at = LastMessageAt::default();
        assert_eq!(at.get(), None);
        let t = Utc.timestamp_micros(1_790_348_400_123_456).unwrap();
        at.set(t);
        assert_eq!(at.get(), Some(t));
    }
}
