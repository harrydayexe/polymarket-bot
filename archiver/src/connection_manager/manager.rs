use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    sync::Arc,
    time::Duration,
};

use anyhow::Result;
use chrono::{DateTime, Utc};
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
        summary::{ConnectionSummary, ManagerPublisher, ManagerSummary, UnhealthyToken},
        types::{
            ConnAttemptId, ConnSlotId, ConnectionAction, ConnectionEvent, ConnectionState,
            SocketEvent,
        },
    },
    market_registry::token_changes::{MarketTokens, TokenChanges},
    messages::{ControlBody, GapReason, Record, SnapshotMsg, SnapshotReason},
    types::RandomSource,
};

/// How long shutdown waits for the socket tasks to finish closing and hand on their last
/// frames. The market channel bounds the close handshake to 2 s.
const SOCKET_CLOSE_TIMEOUT: Duration = Duration::from_secs(3);

/// A subscribed token that has had no full book yet.
struct AwaitingBook {
    since: DateTime<Utc>,
    snapshot_requested_at: Option<DateTime<Utc>>,
}

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
    awaiting_book: HashMap<TokenId, AwaitingBook>,
    unhealthy: BTreeMap<TokenId, DateTime<Utc>>,
    /// Whether each connection was streaming when last published, and since when.
    streaming_since: HashMap<ConnSlotId, (bool, DateTime<Utc>)>,
    last_message_at: Option<DateTime<Utc>>,
    frames_dropped: u64,
    publisher: ManagerPublisher,
}

impl<R: RandomSource> ConnectionManager<R> {
    pub fn new(
        config: Arc<Config>,
        clock: SharedClock,
        rng: R,
        write_queue: mpsc::Sender<Record>,
        snapshots: mpsc::UnboundedSender<SnapshotMsg>,
        reports: mpsc::UnboundedSender<SocketReport>,
        publisher: ManagerPublisher,
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
            awaiting_book: HashMap::new(),
            unhealthy: BTreeMap::new(),
            streaming_since: HashMap::new(),
            last_message_at: None,
            frames_dropped: 0,
            publisher,
        }
    }

    /// Places added markets on connections and takes removed ones off. Removals go first, so a
    /// market whose token IDs changed is unsubscribed before its new tokens are subscribed.
    ///
    /// # Errors
    ///
    /// If the write queue or snapshot worker has gone away.
    pub async fn apply_token_changes(&mut self, changes: TokenChanges) -> Result<()> {
        // First, so the snapshot worker schedules the tokens before their subscribe snapshots
        // are requested.
        let both = |m: &MarketTokens| [m.yes.clone(), m.no.clone()];
        let tracked = SnapshotMsg::TokensTracked {
            add: changes.added.values().flat_map(both).collect(),
            remove: changes.removed.values().flat_map(both).collect(),
        };
        if self.snapshots.send(tracked).is_err() {
            anyhow::bail!("snapshot worker gone, connection manager exiting");
        }

        for market in changes.removed.values() {
            for token in [&market.yes, &market.no] {
                self.awaiting_book.remove(token);
                self.unhealthy.remove(token);
            }
        }
        // New tokens are published before they are subscribed, so after a crash recovery
        // knows about everything that may have been recorded.
        if !changes.added.is_empty() {
            self.publish_tracked(changes.added.values().flat_map(both));
        }

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
        self.publish_tracked(std::iter::empty());
        Ok(())
    }

    /// Publishes the tracked tokens, with `extra` ones about to be added.
    fn publish_tracked(&self, extra: impl Iterator<Item = TokenId>) {
        let mut tracked: Vec<TokenId> = self.assignment.tokens().cloned().chain(extra).collect();
        tracked.sort_unstable();
        tracked.dedup();
        self.publisher
            .tracked_tokens
            .send_replace(Arc::new(tracked));
    }

    /// Full books were written for `tokens`, so their data can be trusted.
    pub fn books_seen(&mut self, tokens: &HashSet<TokenId>) {
        for token in tokens {
            self.awaiting_book.remove(token);
            if self.unhealthy.remove(token).is_some() {
                tracing::info!(%token, "book received, token healthy again");
            }
        }
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
                self.frames_dropped += dropped;
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
            self.last_message_at = self.last_message_at.max(last_message_at);
            self.dispatch(&slot, ConnectionEvent::Tick { last_message_at })
                .await?;
        }
        self.drop_closed();
        self.check_missing_books().await?;
        self.publish();
        Ok(())
    }

    /// Requests a REST snapshot for tokens subscribed `missing_book_timeout` ago without a full
    /// book, and marks them unhealthy if that does not bring one either within the same time
    /// (section 10).
    async fn check_missing_books(&mut self) -> Result<()> {
        let now = self.clock.now();
        let wait = chrono::Duration::from_std(self.config.missing_book_timeout)
            .expect("config durations fit");
        let mut request = HashSet::new();
        let mut unhealthy = vec![];
        for (token, awaiting) in &mut self.awaiting_book {
            match awaiting.snapshot_requested_at {
                None if awaiting.since + wait <= now => {
                    awaiting.snapshot_requested_at = Some(now);
                    request.insert(token.clone());
                }
                Some(at) if at + wait <= now => unhealthy.push((token.clone(), awaiting.since)),
                _ => {}
            }
        }

        if !request.is_empty() {
            tracing::warn!(
                tokens = request.len(),
                "no book since subscribing, requesting snapshots"
            );
            self.request_snapshot(request, SnapshotReason::MissingBook)?;
        }
        for (token, since) in unhealthy {
            tracing::warn!(%token, "no book from the websocket or a snapshot, token unhealthy");
            self.awaiting_book.remove(&token);
            self.unhealthy.insert(token.clone(), since);
            self.write(None, ControlBody::TokenUnhealthy { token, since })
                .await?;
        }
        Ok(())
    }

    fn request_snapshot(&self, tokens: HashSet<TokenId>, reason: SnapshotReason) -> Result<()> {
        if self
            .snapshots
            .send(SnapshotMsg::SnapshotRequest { tokens, reason })
            .is_err()
        {
            anyhow::bail!("snapshot worker gone, connection manager exiting");
        }
        Ok(())
    }

    /// Publishes the summary for the status file.
    fn publish(&mut self) {
        let now = self.clock.now();
        let mut connections = Vec::with_capacity(self.connections.len());
        for (slot, connection) in &self.connections {
            let state = match connection.state() {
                ConnectionState::Connecting => "connecting",
                ConnectionState::Streaming { .. } => "streaming",
                ConnectionState::Backoff { .. } => "backoff",
                ConnectionState::Closed => continue,
            };
            let streaming = state == "streaming";
            let entry = self
                .streaming_since
                .entry(slot.clone())
                .or_insert((streaming, now));
            if entry.0 != streaming {
                *entry = (streaming, now);
            }
            connections.push(ConnectionSummary {
                slot: slot.clone(),
                state,
                tokens: connection.assigned_tokens().len(),
                since: entry.1,
                last_message_at: self.sockets.get(slot).and_then(|s| s.last_message_at.get()),
            });
        }
        self.streaming_since
            .retain(|slot, _| self.connections.contains_key(slot));

        self.publisher.summary.send_replace(ManagerSummary {
            connections,
            tokens_tracked: self.assignment.tokens().count(),
            open_gaps: self.gaps.open_count(),
            tokens_unhealthy: self
                .unhealthy
                .iter()
                .map(|(token, since)| UnhealthyToken {
                    token: token.clone(),
                    since: *since,
                })
                .collect(),
            frames_dropped: self.frames_dropped,
            last_message_at: self.last_message_at,
        });
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
                self.request_snapshot(tokens, reason)?;
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
                match &body {
                    ControlBody::Subscribe { tokens, .. } => {
                        let now = self.clock.now();
                        for token in tokens {
                            // A token already waiting keeps its time; an unhealthy one stays
                            // so until a book arrives.
                            if !self.unhealthy.contains_key(token) {
                                self.awaiting_book
                                    .entry(token.clone())
                                    .or_insert(AwaitingBook {
                                        since: now,
                                        snapshot_requested_at: None,
                                    });
                            }
                        }
                    }
                    ControlBody::Unsubscribe { tokens, .. } => {
                        for token in tokens {
                            self.awaiting_book.remove(token);
                            self.unhealthy.remove(token);
                        }
                    }
                    _ => {}
                }
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
        // Expected at shutdown; anything else is worth a warning.
        ControlBody::Disconnect { slot, reason } if reason == "shutdown" => {
            tracing::info!(%slot, reason, "disconnected");
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
