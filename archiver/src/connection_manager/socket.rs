use std::{
    ops::ControlFlow,
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    },
};

use chrono::{DateTime, SubsecRound, Utc};
use futures::StreamExt;
use marcasite::{
    Error, WebSocketErrorKind,
    ws::{MarketChannelBuilder, MarketChannelHandle, MarketSubscription},
};
use tokio::sync::mpsc::{self, error::TrySendError};
use tokio_util::sync::CancellationToken;

use crate::{
    clock::SharedClock,
    connection_manager::types::{ConnAttemptId, ConnSlotId, SocketEvent},
    messages::{ControlBody, Record},
};

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
    /// The channel connected and subscribed; subscription changes go through this handle.
    Opened(MarketChannelHandle),
    Event(SocketEvent),
    /// How many records were dropped because the write queue was full, reported once the
    /// channel has ended.
    FramesDropped(u64),
}

/// Everything a socket task needs to run one connection attempt.
pub struct SocketTask {
    pub slot: ConnSlotId,
    pub attempt_id: ConnAttemptId,
    /// URL, timeouts and heartbeat of the market channel.
    pub channel: MarketChannelBuilder,
    pub subscription: MarketSubscription,
    pub write_queue: mpsc::Sender<Record>,
    pub reports: mpsc::UnboundedSender<SocketReport>,
    pub last_message_at: LastMessageAt,
    pub clock: SharedClock,
    /// Cancelled by the manager to close the channel. Events already received are still
    /// recorded while the close completes.
    pub cancel: CancellationToken,
}

impl SocketTask {
    /// Connects the market channel, then reads it until it ends: every event is stamped with
    /// its receive time and put on the write queue, and nothing else is done with it.
    pub async fn run(self) {
        let connected = tokio::select! {
            _ = self.cancel.cancelled() => return,
            r = self.channel.clone().connect(self.subscription.clone()) => r,
        };
        let mut channel = match connected {
            Ok(channel) => channel,
            Err(e) => {
                self.report(ReportKind::Event(SocketEvent::Failed {
                    retry_after: e.retry_after(),
                    error: e.to_string(),
                }));
                return;
            }
        };
        self.report(ReportKind::Opened(channel.handle()));

        let mut closing = false;
        let mut dropped = 0u64;
        loop {
            let item = tokio::select! {
                biased;
                _ = self.cancel.cancelled(), if !closing => {
                    // The channel completes the close handshake within a bounded time, then
                    // ends the stream.
                    channel.close();
                    closing = true;
                    continue;
                }
                item = channel.next() => item,
            };
            // Microseconds, as recorded on disk, so the gap start matches the frame exactly.
            let recv_ts = self.clock.now().trunc_subsecs(6);
            let record = match item {
                Some(Ok(event)) => Record::WsFrame {
                    recv_ts,
                    conn_id: self.attempt_id.clone(),
                    event,
                },
                // Not fatal: the stream goes on. The message cannot be archived, so record the
                // error, which quotes it.
                Some(Err(Error::WebSocket(e))) if e.kind() == WebSocketErrorKind::Decode => {
                    tracing::warn!(attempt = %self.attempt_id, error = %e, "undecodable frame");
                    Record::Control {
                        recv_ts,
                        conn_id: Some(self.attempt_id.clone()),
                        body: ControlBody::FrameDecodeFailed {
                            error: e.to_string(),
                        },
                    }
                }
                Some(Err(e)) => {
                    self.report_lost(closing, lost_event(e));
                    break;
                }
                None => {
                    self.report_lost(
                        closing,
                        SocketEvent::Closed {
                            reason: "closed by the server".into(),
                        },
                    );
                    break;
                }
            };
            self.last_message_at.set(recv_ts);
            if self.put(record, &mut dropped).is_break() {
                self.report_lost(
                    closing,
                    SocketEvent::Closed {
                        reason: "write queue closed".into(),
                    },
                );
                break;
            }
        }

        if dropped > 0 {
            self.report(ReportKind::FramesDropped(dropped));
        }
    }

    /// Puts `record` on the write queue without waiting. A full queue drops it, reporting the
    /// first drop.
    ///
    /// # Returns
    ///
    /// `Break` if the write queue has closed.
    fn put(&self, record: Record, dropped: &mut u64) -> ControlFlow<()> {
        match self.write_queue.try_send(record) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                *dropped += 1;
                if *dropped == 1 {
                    self.report(ReportKind::Event(SocketEvent::QueueOverflow));
                }
            }
            Err(TrySendError::Closed(_)) => {
                tracing::error!(attempt = %self.attempt_id, "write queue closed, closing socket");
                return ControlFlow::Break(());
            }
        }
        ControlFlow::Continue(())
    }

    /// Reports the end of a channel the manager did not ask to close.
    fn report_lost(&self, closing: bool, event: SocketEvent) {
        if !closing {
            self.report(ReportKind::Event(event));
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

/// The event for the final error of a channel.
fn lost_event(e: Error) -> SocketEvent {
    match &e {
        // No frame within the idle timeout (the heartbeat went unanswered), or a stalled write.
        Error::WebSocket(ws) if ws.kind() == WebSocketErrorKind::Timeout => SocketEvent::TimedOut {
            error: e.to_string(),
        },
        _ => SocketEvent::Closed {
            reason: e.to_string(),
        },
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
