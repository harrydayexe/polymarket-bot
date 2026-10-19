use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    sync::Arc,
    time::Duration,
};

use anyhow::Result;
use marcasite::types::TokenId;
use marcasite::ws::{
    MarketChannel, MarketChannelHandle, MarketSubscription, MarketSubscriptionUpdate,
};
use tokio::{sync::mpsc, time::Instant};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::{
    clock::SharedClock,
    config::Config,
    connection_manager::{
        assignment::TokenAssignment,
        connection::{Connection, Now, step},
        gap_tracker::GapTracker,
        socket::{LastMessageAt, ReportKind, SocketReport, SocketTask},
        types::{
            ConnAttemptId, ConnSlotId, ConnectionAction, ConnectionEvent, ConnectionState,
            SocketEvent,
        },
    },
    market_registry::token_changes::TokenChanges,
    messages::{ControlBody, GapReason, Record, SnapshotMsg},
    types::RandomSource,
};

/// How long shutdown waits for the socket tasks to finish closing and hand on their last
/// frames. The market channel bounds the close handshake to 2 s.
const SOCKET_CLOSE_TIMEOUT: Duration = Duration::from_secs(3);

/// The socket of a connection's current attempt.
struct Socket {
    attempt_id: ConnAttemptId,
    /// Set once the channel has opened.
    handle: Option<MarketChannelHandle>,
    cancel: CancellationToken,
    last_message_at: LastMessageAt,
}

/// Owns the websocket connections: places tokens on connections, runs each connection's state
/// machine and carries out its actions. Carrying out an action is the only place sockets are
/// touched.
pub struct ConnectionManager<R> {
    config: Arc<Config>,
    clock: SharedClock,
    rng: R,
    connections: BTreeMap<ConnSlotId, Connection>,
    sockets: HashMap<ConnSlotId, Socket>,
    assignment: TokenAssignment,
    gaps: GapTracker,
    write_queue: mpsc::Sender<Record>,
    snapshots: mpsc::UnboundedSender<SnapshotMsg>,
    reports: mpsc::UnboundedSender<SocketReport>,
    socket_tasks: TaskTracker,
}

impl<R: RandomSource> ConnectionManager<R> {
    pub fn new(
        config: Arc<Config>,
        clock: SharedClock,
        rng: R,
        write_queue: mpsc::Sender<Record>,
        snapshots: mpsc::UnboundedSender<SnapshotMsg>,
        reports: mpsc::UnboundedSender<SocketReport>,
    ) -> Self {
        Self {
            assignment: TokenAssignment::new(config.tokens_per_connection),
            config,
            clock,
            rng,
            connections: BTreeMap::new(),
            sockets: HashMap::new(),
            gaps: GapTracker::default(),
            write_queue,
            snapshots,
            reports,
            socket_tasks: TaskTracker::new(),
        }
    }

    /// Places added markets on connections and takes removed ones off. Removals go first, so a
    /// market whose token IDs changed is unsubscribed before its new tokens are subscribed.
    ///
    /// # Errors
    ///
    /// If the write queue or snapshot worker has gone away.
    pub async fn apply_token_changes(&mut self, changes: TokenChanges) -> Result<()> {
        let mut removed: Vec<_> = changes.removed.into_iter().collect();
        removed.sort_by(|a, b| a.0.cmp(&b.0));
        let unassigned = self.assignment.unassign(removed.iter().map(|(_, m)| m));
        for (slot, tokens) in unassigned {
            self.dispatch(&slot, ConnectionEvent::TokensRemoved { tokens })
                .await?;
        }

        let mut added: Vec<_> = changes.added.into_iter().collect();
        added.sort_by(|a, b| a.0.cmp(&b.0));
        let assigned = self.assignment.assign(added.iter().map(|(_, m)| m));
        for (slot, tokens) in assigned {
            if self.connections.contains_key(&slot) {
                self.dispatch(&slot, ConnectionEvent::TokensAssigned { tokens })
                    .await?;
            } else {
                let (connection, actions) = Connection::new(slot.clone(), tokens, &self.config);
                tracing::info!(%slot, tokens = connection.assigned_tokens().len(), "opening connection");
                self.connections.insert(slot.clone(), connection);
                self.drive(&slot, actions, VecDeque::new()).await?;
            }
        }

        self.drop_closed();
        Ok(())
    }

    /// Feeds a socket task's report to its connection.
    ///
    /// # Errors
    ///
    /// If the write queue or snapshot worker has gone away.
    pub async fn handle_report(&mut self, report: SocketReport) -> Result<()> {
        let SocketReport {
            slot,
            attempt_id,
            kind,
        } = report;
        let event = match kind {
            ReportKind::Opened(handle) => match self.sockets.get_mut(&slot) {
                Some(socket) if socket.attempt_id == attempt_id => {
                    socket.handle = Some(handle);
                    SocketEvent::Opened
                }
                // Opened after the connection gave up on it.
                _ => {
                    handle.close();
                    return Ok(());
                }
            },
            ReportKind::Event(event) => event,
            ReportKind::FramesDropped(dropped) => {
                tracing::warn!(%attempt_id, dropped, "write queue full, frames dropped");
                return self
                    .write(
                        Some(attempt_id),
                        ControlBody::QueueOverflow { slot, dropped },
                    )
                    .await;
            }
        };
        self.dispatch(&slot, ConnectionEvent::Socket { attempt_id, event })
            .await?;
        self.drop_closed();
        Ok(())
    }

    /// Runs every connection's timers, which end backoff waits.
    ///
    /// # Errors
    ///
    /// If the write queue or snapshot worker has gone away.
    pub async fn tick(&mut self) -> Result<()> {
        let slots: Vec<ConnSlotId> = self.connections.keys().cloned().collect();
        for slot in slots {
            let last_message_at = self
                .sockets
                .get(&slot)
                .and_then(|s| s.last_message_at.get());
            self.dispatch(&slot, ConnectionEvent::Tick { last_message_at })
                .await?;
        }
        self.drop_closed();
        Ok(())
    }

    /// Closes every connection, closing their gaps with reason `shutdown`, and waits a bounded
    /// time for the sockets to hand on their last frames.
    ///
    /// # Errors
    ///
    /// If the write queue has gone away.
    pub async fn shutdown(&mut self) -> Result<()> {
        let slots: Vec<ConnSlotId> = self.connections.keys().cloned().collect();
        for slot in slots {
            self.dispatch(&slot, ConnectionEvent::Shutdown).await?;
        }
        self.connections.clear();
        // Every gap belongs to a connection's tokens, so none should be left.
        if let Some(body) = self.gaps.close_all(self.clock.now(), &GapReason::Shutdown) {
            tracing::warn!("closing gaps left open at shutdown");
            self.write(None, body).await?;
        }

        self.socket_tasks.close();
        if tokio::time::timeout(SOCKET_CLOSE_TIMEOUT, self.socket_tasks.wait())
            .await
            .is_err()
        {
            tracing::warn!("sockets did not close within {SOCKET_CLOSE_TIMEOUT:?}");
        }
        Ok(())
    }

    fn now(&self) -> Now {
        Now {
            mono: Instant::now(),
            wall: self.clock.now(),
        }
    }

    /// Steps `slot`'s connection with `event` and carries out the resulting actions.
    async fn dispatch(&mut self, slot: &ConnSlotId, event: ConnectionEvent) -> Result<()> {
        self.drive(slot, vec![], VecDeque::from([event])).await
    }

    /// Carries out `actions`, then steps the connection with each event that results (a failed
    /// send), until none are left.
    async fn drive(
        &mut self,
        slot: &ConnSlotId,
        mut actions: Vec<ConnectionAction>,
        mut events: VecDeque<ConnectionEvent>,
    ) -> Result<()> {
        loop {
            for action in std::mem::take(&mut actions) {
                if let Some(event) = self.execute(slot, action).await? {
                    events.push_back(event);
                }
            }
            let Some(event) = events.pop_front() else {
                return Ok(());
            };
            let now = self.now();
            let Some(connection) = self.connections.get_mut(slot) else {
                return Ok(());
            };
            actions = step(connection, event, now, &self.config, &mut self.rng);
        }
    }

    /// Carries out one action of `slot`'s connection.
    ///
    /// # Returns
    ///
    /// The event a send produced, to be fed back to the connection.
    async fn execute(
        &mut self,
        slot: &ConnSlotId,
        action: ConnectionAction,
    ) -> Result<Option<ConnectionEvent>> {
        let attempt_id = self
            .connections
            .get(slot)
            .and_then(|c| c.attempt_id().cloned());
        match action {
            ConnectionAction::OpenSocket {
                url,
                attempt_id,
                tokens,
            } => {
                self.open_socket(slot, url, attempt_id, &tokens);
                Ok(None)
            }
            ConnectionAction::CloseSocket => {
                if let Some(socket) = self.sockets.remove(slot) {
                    socket.cancel.cancel();
                }
                Ok(None)
            }
            ConnectionAction::SendSubscribe { tokens } => {
                let update = MarketSubscriptionUpdate::subscribe(sorted(&tokens))
                    .custom_feature_enabled(true);
                Ok(self.send(slot, update))
            }
            ConnectionAction::SendUnsubscribe { tokens } => {
                Ok(self.send(slot, MarketSubscriptionUpdate::unsubscribe(sorted(&tokens))))
            }
            ConnectionAction::RequestSnapshot { tokens, reason } => {
                if self
                    .snapshots
                    .send(SnapshotMsg::SnapshotRequest { tokens, reason })
                    .is_err()
                {
                    anyhow::bail!("snapshot worker gone, connection manager exiting");
                }
                Ok(None)
            }
            ConnectionAction::OpenGaps {
                tokens,
                start,
                reason,
            } => {
                if let Some(body) = self.gaps.open(&tokens, start, &reason) {
                    self.write(attempt_id, body).await?;
                }
                Ok(None)
            }
            ConnectionAction::CloseGaps {
                tokens,
                end,
                reason,
            } => {
                if let Some(body) = self.gaps.close(&tokens, end, &reason) {
                    self.write(attempt_id, body).await?;
                }
                Ok(None)
            }
            ConnectionAction::Emit { body } => {
                log_control(&body);
                self.write(attempt_id, body).await?;
                Ok(None)
            }
        }
    }

    fn open_socket(
        &mut self,
        slot: &ConnSlotId,
        url: url::Url,
        attempt_id: ConnAttemptId,
        tokens: &HashSet<TokenId>,
    ) {
        if let Some(old) = self.sockets.remove(slot) {
            old.cancel.cancel();
        }
        let socket = Socket {
            attempt_id: attempt_id.clone(),
            handle: None,
            cancel: CancellationToken::new(),
            last_message_at: LastMessageAt::default(),
        };
        // The channel sends `PING` every heartbeat interval and ends the stream with a timeout
        // if nothing, not even the `PONG`, arrives within the heartbeat timeout.
        let channel = MarketChannel::builder()
            .url(url.as_str())
            .connect_timeout(self.config.connect_timeout)
            .heartbeat_interval(self.config.heartbeat_interval)
            .idle_timeout(self.config.heartbeat_timeout);
        let task = SocketTask {
            slot: slot.clone(),
            attempt_id,
            channel,
            subscription: MarketSubscription::new(sorted(tokens)).custom_feature_enabled(true),
            write_queue: self.write_queue.clone(),
            reports: self.reports.clone(),
            last_message_at: socket.last_message_at.clone(),
            clock: self.clock.clone(),
            cancel: socket.cancel.clone(),
        };
        self.socket_tasks.spawn(task.run());
        self.sockets.insert(slot.clone(), socket);
    }

    /// Sends a subscription change on `slot`'s channel.
    ///
    /// # Returns
    ///
    /// A `SendFailed` event if it could not be sent.
    fn send(&self, slot: &ConnSlotId, update: MarketSubscriptionUpdate) -> Option<ConnectionEvent> {
        let sent = match self.sockets.get(slot).and_then(|s| s.handle.as_ref()) {
            Some(handle) => handle
                .update_subscription(update)
                .map_err(|e| e.to_string()),
            None => Err("channel not open".to_string()),
        };
        sent.err()
            .map(|error| self.socket_event(slot, SocketEvent::SendFailed { error }))
    }

    /// `event` as reported by the current attempt of `slot`'s connection.
    fn socket_event(&self, slot: &ConnSlotId, event: SocketEvent) -> ConnectionEvent {
        let attempt_id = self
            .connections
            .get(slot)
            .and_then(|c| c.attempt_id().cloned())
            .expect("a connection that sends has made an attempt");
        ConnectionEvent::Socket { attempt_id, event }
    }

    /// Forgets connections that have closed for good.
    fn drop_closed(&mut self) {
        self.connections.retain(|slot, c| {
            let open = c.state() != ConnectionState::Closed;
            if !open {
                tracing::info!(%slot, "connection closed");
            }
            open
        });
    }

    async fn write(&self, conn_id: Option<ConnAttemptId>, body: ControlBody) -> Result<()> {
        let record = Record::Control {
            recv_ts: self.clock.now(),
            conn_id,
            body,
        };
        if self.write_queue.send(record).await.is_err() {
            anyhow::bail!("write queue gone, connection manager exiting");
        }
        Ok(())
    }
}

fn log_control(body: &ControlBody) {
    match body {
        ControlBody::Connect { slot, token_count } => {
            tracing::info!(%slot, tokens = token_count, "connected");
        }
        ControlBody::Disconnect { slot, reason } => {
            tracing::warn!(%slot, reason, "disconnected");
        }
        _ => {}
    }
}

/// Token IDs in a stable order, so the same set always gives the same message.
fn sorted(tokens: &HashSet<TokenId>) -> Vec<TokenId> {
    let mut ids: Vec<TokenId> = tokens.iter().cloned().collect();
    ids.sort_unstable();
    ids
}
