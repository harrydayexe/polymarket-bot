use std::collections::HashSet;

use chrono::{DateTime, Utc};
use marcasite::types::TokenId;
use tokio::time::Instant;

use crate::{
    config::Config,
    connection_manager::{
        backoff_delay::backoff_delay,
        types::{
            ConnAttemptId, ConnSlotId, ConnectionAction, ConnectionEvent, ConnectionState,
            SocketEvent,
        },
    },
    messages::{ControlBody, GapReason, SnapshotReason},
    types::RandomSource,
};

#[derive(Debug)]
pub struct Connection {
    slot_id: ConnSlotId,
    /// None until the first attempt is made.
    attempt_id: Option<ConnAttemptId>,
    attempt_seq: u64,
    failures: u32,
    state: ConnectionState,
    /// Which tokens the connection *should* be subscribed to.
    assigned_tokens: HashSet<TokenId>,
    /// The currently subscribed to token set.
    subscribed_tokens: HashSet<TokenId>,
    /// Should be written by the socket reader only.
    last_message_at: Option<DateTime<Utc>>,
    last_pong_at: Option<Instant>,
    last_ping_at: Option<Instant>,
}

#[derive(Debug, Clone, Copy)]
pub struct Now {
    pub mono: Instant,
    pub wall: DateTime<Utc>,
}

impl Connection {
    /// Creates the connection for a slot and starts its first attempt.
    pub fn new(
        slot_id: ConnSlotId,
        tokens: HashSet<TokenId>,
        now: Now,
        cfg: &Config,
    ) -> (Self, Vec<ConnectionAction>) {
        let mut connection = Connection {
            slot_id,
            attempt_id: None,
            attempt_seq: 0,
            failures: 0,
            state: ConnectionState::Closed,
            assigned_tokens: tokens,
            subscribed_tokens: HashSet::new(),
            last_message_at: None,
            last_pong_at: None,
            last_ping_at: None,
        };
        let actions = start_attempt(&mut connection, now, cfg);
        (connection, actions)
    }

    pub fn slot_id(&self) -> &ConnSlotId {
        &self.slot_id
    }

    pub fn attempt_id(&self) -> Option<&ConnAttemptId> {
        self.attempt_id.as_ref()
    }

    pub fn state(&self) -> ConnectionState {
        self.state
    }

    pub fn assigned_tokens(&self) -> &HashSet<TokenId> {
        &self.assigned_tokens
    }

    pub fn subscribed_tokens(&self) -> &HashSet<TokenId> {
        &self.subscribed_tokens
    }

    pub fn last_pong_at(&self) -> Option<Instant> {
        self.last_pong_at
    }
}

/// The socket exists (or is being opened) and must be closed when leaving this state.
fn socket_live(state: ConnectionState) -> bool {
    matches!(
        state,
        ConnectionState::Connecting { .. }
            | ConnectionState::Subscribing { .. }
            | ConnectionState::Streaming { .. }
    )
}

/// The socket opened and a `Connect` control record was emitted, so leaving this state must
/// emit the matching `Disconnect`.
fn socket_open(state: ConnectionState) -> bool {
    matches!(
        state,
        ConnectionState::Subscribing { .. } | ConnectionState::Streaming { .. }
    )
}

fn start_attempt(c: &mut Connection, now: Now, cfg: &Config) -> Vec<ConnectionAction> {
    c.attempt_seq += 1;
    let id = ConnAttemptId::create(c.slot_id.clone(), c.attempt_seq);
    c.attempt_id = Some(id.clone());
    c.last_message_at = None;
    c.last_pong_at = None;
    c.last_ping_at = None;
    c.state = ConnectionState::Connecting {
        started_at: now.mono,
    };
    vec![ConnectionAction::OpenSocket {
        url: cfg.websocket_url.clone(),
        attempt_id: id,
    }]
}

fn enter_backoff(
    c: &mut Connection,
    reason: GapReason,
    now: Now,
    cfg: &Config,
    rng: &mut impl RandomSource,
) -> Vec<ConnectionAction> {
    if let ConnectionState::Streaming { since, .. } = c.state
        && now.mono - since >= cfg.backoff_reset_after
    {
        c.failures = 0;
    }
    let was_open = socket_open(c.state);

    c.failures += 1;
    let delay = backoff_delay(c.failures, cfg, rng);
    c.subscribed_tokens.clear();
    c.state = ConnectionState::Backoff {
        until: now.mono + delay,
    };
    let mut actions = vec![
        ConnectionAction::CloseSocket,
        ConnectionAction::OpenGaps {
            tokens: c.assigned_tokens.clone(),
            start: c.last_message_at.unwrap_or(now.wall),
            reason: reason.clone(),
        },
    ];
    if was_open {
        actions.push(ConnectionAction::Emit {
            body: ControlBody::Disconnect {
                slot: c.slot_id.clone(),
                reason: reason.to_string(),
            },
        });
    }
    actions
}

/// Closes the connection for good. Any socket is closed, and the gaps of every assigned token
/// are closed with `reason`, so no gap is left open past the end of this connection.
fn close(c: &mut Connection, disconnect_reason: &str) -> Vec<ConnectionAction> {
    let mut actions = vec![];
    if socket_live(c.state) {
        actions.push(ConnectionAction::CloseSocket);
    }
    if socket_open(c.state) {
        actions.push(ConnectionAction::Emit {
            body: ControlBody::Disconnect {
                slot: c.slot_id.clone(),
                reason: disconnect_reason.into(),
            },
        });
    }
    c.subscribed_tokens.clear();
    c.state = ConnectionState::Closed;
    actions
}

fn shutdown(c: &mut Connection, now: Now) -> Vec<ConnectionAction> {
    let tokens = c.assigned_tokens.clone();
    let mut actions = close(c, &GapReason::Shutdown.to_string());
    actions.push(ConnectionAction::CloseGaps {
        tokens,
        end: now.wall,
        reason: GapReason::Shutdown,
    });
    actions
}

/// Subscribes newly assigned tokens on the live socket.
fn subscribe_new(c: &mut Connection, tokens: HashSet<TokenId>) -> Vec<ConnectionAction> {
    c.subscribed_tokens.extend(tokens.iter().cloned());
    vec![
        ConnectionAction::SendSubscribe {
            tokens: tokens.clone(),
        },
        ConnectionAction::RequestSnapshot {
            tokens: tokens.clone(),
            reason: SnapshotReason::NewToken,
        },
        ConnectionAction::Emit {
            body: ControlBody::Subscribe {
                slot: c.slot_id.clone(),
                tokens,
            },
        },
    ]
}

/// Unsubscribes tokens on the live socket.
fn unsubscribe(c: &mut Connection, tokens: HashSet<TokenId>) -> Vec<ConnectionAction> {
    for t in &tokens {
        c.subscribed_tokens.remove(t);
    }
    vec![
        ConnectionAction::SendUnsubscribe {
            tokens: tokens.clone(),
        },
        ConnectionAction::Emit {
            body: ControlBody::Unsubscribe {
                slot: c.slot_id.clone(),
                tokens,
            },
        },
    ]
}

/// Removes tokens from the assigned set in any open state. Only a streaming connection
/// unsubscribes on the wire; in every other state the next subscribe simply leaves them out.
/// Closes the connection once no tokens are left.
fn remove_tokens(
    c: &mut Connection,
    mut tokens: HashSet<TokenId>,
    now: Now,
) -> Vec<ConnectionAction> {
    tokens.retain(|t| c.assigned_tokens.remove(t));
    if tokens.is_empty() {
        return vec![];
    }

    let mut actions = vec![];
    if matches!(c.state, ConnectionState::Streaming { .. }) {
        actions.extend(unsubscribe(c, tokens.clone()));
    }
    actions.push(ConnectionAction::CloseGaps {
        tokens,
        end: now.wall,
        reason: GapReason::Unsubscribed,
    });
    if c.assigned_tokens.is_empty() {
        actions.extend(close(c, "no tokens"));
    }
    actions
}

/// Handles a socket event the current state has no specific transition for. Anything saying
/// the socket is gone always sends the connection to backoff, so the machine never believes a
/// dead socket is alive. Anything else is logged and ignored.
fn socket_lost_or_unexpected(
    c: &mut Connection,
    event: SocketEvent,
    now: Now,
    config: &Config,
    rng: &mut impl RandomSource,
) -> Vec<ConnectionAction> {
    let reason = match event {
        SocketEvent::Failed { error } => GapReason::ConnectFailed { error },
        SocketEvent::Closed { reason } => GapReason::ConnectionClosed { error: reason },
        SocketEvent::SendFailed { error } => GapReason::ConnectionClosed { error },
        SocketEvent::QueueOverflow => GapReason::QueueOverflow,
        e => {
            tracing::warn!(
                "unexpected event for slot {} in state {:?}: {}",
                c.slot_id,
                c.state,
                e
            );
            return vec![];
        }
    };
    enter_backoff(c, reason, now, config, rng)
}

fn connecting_state_transition(
    connection: &mut Connection,
    event: ConnectionEvent,
    now: Now,
    config: &Config,
    rng: &mut impl RandomSource,
) -> Vec<ConnectionAction> {
    let ConnectionState::Connecting { started_at } = connection.state else {
        unreachable!("expected connection state to be ConnectionState::Connecting");
    };
    match event {
        ConnectionEvent::Socket {
            event: SocketEvent::Opened,
            ..
        } => {
            connection.subscribed_tokens = connection.assigned_tokens.clone();
            connection.state = ConnectionState::Subscribing { since: now.mono };
            vec![
                ConnectionAction::Emit {
                    body: ControlBody::Connect {
                        slot: connection.slot_id.clone(),
                        token_count: connection.assigned_tokens.len(),
                    },
                },
                ConnectionAction::SendSubscribe {
                    tokens: connection.assigned_tokens.clone(),
                },
                ConnectionAction::RequestSnapshot {
                    tokens: connection.assigned_tokens.clone(),
                    reason: SnapshotReason::Subscribe,
                },
                ConnectionAction::CloseGaps {
                    tokens: connection.assigned_tokens.clone(),
                    end: now.wall,
                    reason: GapReason::Resumed,
                },
                ConnectionAction::Emit {
                    body: ControlBody::Subscribe {
                        slot: connection.slot_id.clone(),
                        tokens: connection.assigned_tokens.clone(),
                    },
                },
            ]
        }
        ConnectionEvent::Socket { event, .. } => {
            socket_lost_or_unexpected(connection, event, now, config, rng)
        }
        ConnectionEvent::Tick { .. } if now.mono - started_at >= config.connect_timeout => {
            enter_backoff(
                connection,
                GapReason::ConnectFailed {
                    error: "connect timed out".to_string(),
                },
                now,
                config,
                rng,
            )
        }
        ConnectionEvent::Tick { .. } => vec![],
        ConnectionEvent::TokensAssigned { tokens } => {
            connection.assigned_tokens.extend(tokens);
            vec![]
        }
        ConnectionEvent::TokensRemoved { tokens } => remove_tokens(connection, tokens, now),
        ConnectionEvent::Shutdown => shutdown(connection, now),
    }
}

fn subscribing_state_transition(
    connection: &mut Connection,
    event: ConnectionEvent,
    now: Now,
    config: &Config,
    rng: &mut impl RandomSource,
) -> Vec<ConnectionAction> {
    let ConnectionState::Subscribing { since } = connection.state else {
        unreachable!("expected connection state to be ConnectionState::Subscribing");
    };
    match event {
        ConnectionEvent::Socket {
            event: SocketEvent::SubscribeSent,
            ..
        } => {
            connection.state = ConnectionState::Streaming {
                since: now.mono,
                next_ping_at: now.mono + config.heartbeat_interval,
                pong_deadline: now.mono + config.heartbeat_timeout,
            };
            // Token changes that arrived while the first subscribe was in flight.
            let to_add: HashSet<TokenId> = connection
                .assigned_tokens
                .difference(&connection.subscribed_tokens)
                .cloned()
                .collect();
            let to_remove: HashSet<TokenId> = connection
                .subscribed_tokens
                .difference(&connection.assigned_tokens)
                .cloned()
                .collect();
            let mut actions = vec![];
            if !to_add.is_empty() {
                actions.extend(subscribe_new(connection, to_add));
            }
            if !to_remove.is_empty() {
                actions.extend(unsubscribe(connection, to_remove));
            }
            actions
        }
        ConnectionEvent::Socket { event, .. } => {
            socket_lost_or_unexpected(connection, event, now, config, rng)
        }
        ConnectionEvent::Tick { .. } if now.mono - since >= config.connect_timeout => {
            enter_backoff(
                connection,
                GapReason::ConnectFailed {
                    error: "subscribe timed out".to_string(),
                },
                now,
                config,
                rng,
            )
        }
        ConnectionEvent::Tick { .. } => vec![],
        // Applied by the reconciliation once the subscribe in flight has been sent.
        ConnectionEvent::TokensAssigned { tokens } => {
            connection.assigned_tokens.extend(tokens);
            vec![]
        }
        ConnectionEvent::TokensRemoved { tokens } => remove_tokens(connection, tokens, now),
        ConnectionEvent::Shutdown => shutdown(connection, now),
    }
}

fn streaming_state_transition(
    connection: &mut Connection,
    event: ConnectionEvent,
    now: Now,
    config: &Config,
    rng: &mut impl RandomSource,
) -> Vec<ConnectionAction> {
    let ConnectionState::Streaming {
        since,
        next_ping_at,
        pong_deadline,
    } = connection.state
    else {
        unreachable!("expected connection state to be ConnectionState::Streaming");
    };
    match event {
        ConnectionEvent::Tick { .. } if now.mono >= pong_deadline => {
            enter_backoff(connection, GapReason::HeartbeatTimeout, now, config, rng)
        }
        ConnectionEvent::Tick { .. } if now.mono >= next_ping_at => {
            connection.state = ConnectionState::Streaming {
                since,
                next_ping_at: now.mono + config.heartbeat_interval,
                pong_deadline,
            };
            connection.last_ping_at = Some(now.mono);
            vec![ConnectionAction::SendPing]
        }
        ConnectionEvent::Tick { .. } => vec![],
        ConnectionEvent::Socket {
            event: SocketEvent::PongReceived { at },
            ..
        } => {
            connection.last_pong_at = Some(at);
            connection.state = ConnectionState::Streaming {
                since,
                next_ping_at,
                pong_deadline: pong_deadline.max(at + config.heartbeat_timeout),
            };
            vec![]
        }
        // Completion of a live subscribe; nothing to do.
        ConnectionEvent::Socket {
            event: SocketEvent::SubscribeSent,
            ..
        } => vec![],
        ConnectionEvent::Socket { event, .. } => {
            socket_lost_or_unexpected(connection, event, now, config, rng)
        }
        ConnectionEvent::TokensAssigned { tokens } => {
            let new: HashSet<TokenId> = tokens
                .difference(&connection.assigned_tokens)
                .cloned()
                .collect();
            if new.is_empty() {
                return vec![];
            }
            connection.assigned_tokens.extend(new.iter().cloned());
            subscribe_new(connection, new)
        }
        ConnectionEvent::TokensRemoved { tokens } => remove_tokens(connection, tokens, now),
        ConnectionEvent::Shutdown => shutdown(connection, now),
    }
}

fn backoff_state_transition(
    connection: &mut Connection,
    event: ConnectionEvent,
    now: Now,
    config: &Config,
) -> Vec<ConnectionAction> {
    let ConnectionState::Backoff { until } = connection.state else {
        unreachable!("expected connection state to be ConnectionState::Backoff");
    };
    match event {
        ConnectionEvent::Tick { .. } if now.mono >= until => start_attempt(connection, now, config),
        ConnectionEvent::Tick { .. } => vec![],
        // Late events from the socket this connection just closed, e.g. the close we asked for.
        ConnectionEvent::Socket { event, .. } => {
            tracing::debug!(
                "ignoring {} for slot {} from the closed socket",
                event,
                connection.slot_id
            );
            vec![]
        }
        ConnectionEvent::TokensAssigned { tokens } => {
            connection.assigned_tokens.extend(tokens);
            vec![]
        }
        ConnectionEvent::TokensRemoved { tokens } => remove_tokens(connection, tokens, now),
        ConnectionEvent::Shutdown => shutdown(connection, now),
    }
}

fn closed_state_transition(
    connection: &mut Connection,
    event: ConnectionEvent,
) -> Vec<ConnectionAction> {
    if !matches!(connection.state, ConnectionState::Closed) {
        unreachable!("expected connection state to be ConnectionState::Closed");
    };
    match event {
        ConnectionEvent::TokensAssigned { tokens } => {
            // The manager must drop closed connections rather than assign to them.
            tracing::error!(
                "{} tokens assigned to closed slot {}; they will not be recorded",
                tokens.len(),
                connection.slot_id
            );
            debug_assert!(tokens.is_empty(), "tokens assigned to a closed connection");
            vec![]
        }
        e @ (ConnectionEvent::TokensRemoved { .. } | ConnectionEvent::Shutdown) => {
            tracing::debug!("ignoring {} for closed slot {}", e, connection.slot_id);
            vec![]
        }
        ConnectionEvent::Socket { .. } | ConnectionEvent::Tick { .. } => vec![],
    }
}

pub fn step(
    connection: &mut Connection,
    event: ConnectionEvent,
    now: Now,
    config: &Config,
    rng: &mut impl RandomSource,
) -> Vec<ConnectionAction> {
    match &event {
        ConnectionEvent::Tick {
            last_message_at: Some(at),
        } => connection.last_message_at = Some(*at),
        // Events from an earlier attempt's socket can still be queued after the connection has
        // given up on it. Applying them to the current attempt would kill a healthy socket, or
        // treat one as open before it is.
        ConnectionEvent::Socket { attempt_id, event }
            if connection.attempt_id.as_ref() != Some(attempt_id) =>
        {
            tracing::debug!(
                "dropping {} for slot {} from stale attempt {}",
                event,
                connection.slot_id,
                attempt_id
            );
            return vec![];
        }
        _ => {}
    }

    match connection.state {
        ConnectionState::Connecting { .. } => {
            connecting_state_transition(connection, event, now, config, rng)
        }
        ConnectionState::Subscribing { .. } => {
            subscribing_state_transition(connection, event, now, config, rng)
        }
        ConnectionState::Streaming { .. } => {
            streaming_state_transition(connection, event, now, config, rng)
        }
        ConnectionState::Backoff { .. } => backoff_state_transition(connection, event, now, config),
        ConnectionState::Closed => closed_state_transition(connection, event),
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use chrono::{TimeDelta, TimeZone};

    use super::*;

    struct FixedRandom(f64);
    impl RandomSource for FixedRandom {
        fn unit(&mut self) -> f64 {
            self.0
        }
    }

    fn token(n: u32) -> TokenId {
        TokenId::new(n.to_string())
    }

    fn tokens(ns: impl IntoIterator<Item = u32>) -> HashSet<TokenId> {
        ns.into_iter().map(token).collect()
    }

    fn slot() -> ConnSlotId {
        ConnSlotId::new("c-0001")
    }

    fn attempt(n: u64) -> ConnAttemptId {
        ConnAttemptId::create(slot(), n)
    }

    fn secs(s: u64) -> Duration {
        Duration::from_secs(s)
    }

    fn millis(ms: u64) -> Duration {
        Duration::from_millis(ms)
    }

    fn disconnect(reason: &str) -> ConnectionAction {
        ConnectionAction::Emit {
            body: ControlBody::Disconnect {
                slot: slot(),
                reason: reason.into(),
            },
        }
    }

    /// Drives one connection through `step` with a fake clock and no jitter.
    struct Harness {
        conn: Connection,
        cfg: Config,
        rng: FixedRandom,
        start_mono: Instant,
        start_wall: DateTime<Utc>,
        elapsed: Duration,
    }

    impl Harness {
        /// A connection for tokens 1 to 4, in Connecting on its first attempt.
        fn new() -> Self {
            Self::with_tokens(tokens(1..=4))
        }

        fn with_tokens(t: HashSet<TokenId>) -> Self {
            let cfg = Config::default();
            let start_mono = Instant::now();
            let start_wall = Utc.with_ymd_and_hms(2026, 9, 25, 14, 0, 0).unwrap();
            let now = Now {
                mono: start_mono,
                wall: start_wall,
            };
            let (conn, actions) = Connection::new(slot(), t, now, &cfg);
            assert_eq!(
                actions,
                vec![ConnectionAction::OpenSocket {
                    url: cfg.websocket_url.clone(),
                    attempt_id: attempt(1),
                }]
            );
            Harness {
                conn,
                cfg,
                rng: FixedRandom(0.0),
                start_mono,
                start_wall,
                elapsed: Duration::ZERO,
            }
        }

        fn now(&self) -> Now {
            Now {
                mono: self.start_mono + self.elapsed,
                wall: self.start_wall + TimeDelta::from_std(self.elapsed).unwrap(),
            }
        }

        fn mono(&self) -> Instant {
            self.now().mono
        }

        fn wall(&self) -> DateTime<Utc> {
            self.now().wall
        }

        fn state(&self) -> ConnectionState {
            self.conn.state()
        }

        fn advance(&mut self, d: Duration) {
            self.elapsed += d;
        }

        fn send(&mut self, event: ConnectionEvent) -> Vec<ConnectionAction> {
            let now = self.now();
            step(&mut self.conn, event, now, &self.cfg, &mut self.rng)
        }

        /// A socket event from the current attempt.
        fn socket(&mut self, event: SocketEvent) -> Vec<ConnectionAction> {
            let attempt_id = self.conn.attempt_id().unwrap().clone();
            self.send(ConnectionEvent::Socket { attempt_id, event })
        }

        /// A socket event from attempt number `n`, regardless of the current one.
        fn socket_from(&mut self, n: u64, event: SocketEvent) -> Vec<ConnectionAction> {
            self.send(ConnectionEvent::Socket {
                attempt_id: attempt(n),
                event,
            })
        }

        fn tick(&mut self) -> Vec<ConnectionAction> {
            self.send(ConnectionEvent::Tick {
                last_message_at: None,
            })
        }

        fn tick_after(&mut self, d: Duration) -> Vec<ConnectionAction> {
            self.advance(d);
            self.tick()
        }

        fn pong(&mut self) -> Vec<ConnectionAction> {
            let at = self.mono();
            self.socket(SocketEvent::PongReceived { at })
        }

        fn assign(&mut self, t: HashSet<TokenId>) -> Vec<ConnectionAction> {
            self.send(ConnectionEvent::TokensAssigned { tokens: t })
        }

        fn remove(&mut self, t: HashSet<TokenId>) -> Vec<ConnectionAction> {
            self.send(ConnectionEvent::TokensRemoved { tokens: t })
        }

        fn subscribing(mut self) -> Self {
            self.socket(SocketEvent::Opened);
            assert!(matches!(self.state(), ConnectionState::Subscribing { .. }));
            self
        }

        fn streaming(self) -> Self {
            let mut h = self.subscribing();
            h.socket(SocketEvent::SubscribeSent);
            assert!(matches!(h.state(), ConnectionState::Streaming { .. }));
            h
        }

        fn backoff(mut self) -> Self {
            self.socket(SocketEvent::Failed {
                error: "refused".into(),
            });
            assert!(matches!(self.state(), ConnectionState::Backoff { .. }));
            self
        }

        /// Waits out the current backoff and starts the next attempt.
        fn reconnect(&mut self) {
            let ConnectionState::Backoff { until } = self.state() else {
                panic!("expected backoff, got {:?}", self.state());
            };
            self.elapsed += until - self.mono();
            let actions = self.tick();
            assert!(
                matches!(actions.as_slice(), [ConnectionAction::OpenSocket { .. }]),
                "{actions:?}"
            );
        }

        /// Streams for `d`, answering pings so the heartbeat stays healthy.
        fn stream_for(&mut self, d: Duration) {
            let mut left = d;
            while !left.is_zero() {
                let step = left.min(self.cfg.heartbeat_interval);
                self.tick_after(step);
                self.pong();
                left -= step;
            }
            assert!(matches!(self.state(), ConnectionState::Streaming { .. }));
        }

        /// The wait of the backoff just entered.
        fn backoff_wait(&self) -> Duration {
            let ConnectionState::Backoff { until } = self.state() else {
                panic!("expected backoff, got {:?}", self.state());
            };
            until - self.mono()
        }
    }

    // Connecting

    #[test]
    fn connect_succeeds_subscribes_all_tokens() {
        let mut h = Harness::new();
        let actions = h.socket(SocketEvent::Opened);

        assert_eq!(h.state(), ConnectionState::Subscribing { since: h.mono() });
        assert_eq!(
            actions,
            vec![
                ConnectionAction::Emit {
                    body: ControlBody::Connect {
                        slot: slot(),
                        token_count: 4,
                    },
                },
                ConnectionAction::SendSubscribe {
                    tokens: tokens(1..=4),
                },
                ConnectionAction::RequestSnapshot {
                    tokens: tokens(1..=4),
                    reason: SnapshotReason::Subscribe,
                },
                ConnectionAction::CloseGaps {
                    tokens: tokens(1..=4),
                    end: h.wall(),
                    reason: GapReason::Resumed,
                },
                ConnectionAction::Emit {
                    body: ControlBody::Subscribe {
                        slot: slot(),
                        tokens: tokens(1..=4),
                    },
                },
            ]
        );
        assert_eq!(h.conn.subscribed_tokens(), &tokens(1..=4));
    }

    #[test]
    fn connect_fails_opens_gaps_without_disconnect() {
        let mut h = Harness::new();
        h.advance(millis(500));
        let actions = h.socket(SocketEvent::Failed {
            error: "refused".into(),
        });

        assert_eq!(
            h.state(),
            ConnectionState::Backoff {
                until: h.mono() + secs(1)
            }
        );
        assert_eq!(
            actions,
            vec![
                ConnectionAction::CloseSocket,
                ConnectionAction::OpenGaps {
                    tokens: tokens(1..=4),
                    start: h.wall(),
                    reason: GapReason::ConnectFailed {
                        error: "refused".into()
                    },
                },
            ]
        );
    }

    #[test]
    fn connect_times_out() {
        let mut h = Harness::new();
        assert_eq!(h.tick_after(millis(9_900)), vec![]);
        assert!(matches!(h.state(), ConnectionState::Connecting { .. }));

        let actions = h.tick_after(millis(100));
        assert!(matches!(h.state(), ConnectionState::Backoff { .. }));
        assert_eq!(
            actions,
            vec![
                ConnectionAction::CloseSocket,
                ConnectionAction::OpenGaps {
                    tokens: tokens(1..=4),
                    start: h.wall(),
                    reason: GapReason::ConnectFailed {
                        error: "connect timed out".into()
                    },
                },
            ]
        );
    }

    #[test]
    fn tokens_changed_while_connecting_are_used_by_the_subscribe() {
        let mut h = Harness::new();
        assert_eq!(h.assign(tokens([5])), vec![]);
        assert_eq!(
            h.remove(tokens([1, 99])),
            vec![ConnectionAction::CloseGaps {
                tokens: tokens([1]),
                end: h.wall(),
                reason: GapReason::Unsubscribed,
            }]
        );

        let actions = h.socket(SocketEvent::Opened);
        assert!(actions.contains(&ConnectionAction::SendSubscribe {
            tokens: tokens(2..=5)
        }));
    }

    #[test]
    fn all_tokens_removed_while_connecting_closes_without_disconnect() {
        let mut h = Harness::new();
        let actions = h.remove(tokens(1..=4));

        assert_eq!(h.state(), ConnectionState::Closed);
        assert_eq!(
            actions,
            vec![
                ConnectionAction::CloseGaps {
                    tokens: tokens(1..=4),
                    end: h.wall(),
                    reason: GapReason::Unsubscribed,
                },
                ConnectionAction::CloseSocket,
            ]
        );
    }

    #[test]
    fn socket_closed_while_connecting_backs_off() {
        let mut h = Harness::new();
        h.socket(SocketEvent::Closed {
            reason: "reset".into(),
        });
        assert!(matches!(h.state(), ConnectionState::Backoff { .. }));
    }

    #[test]
    fn unexpected_socket_events_while_connecting_are_ignored() {
        let mut h = Harness::new();
        let at = h.mono();
        assert_eq!(h.socket(SocketEvent::PongReceived { at }), vec![]);
        assert_eq!(h.socket(SocketEvent::SubscribeSent), vec![]);
        assert!(matches!(h.state(), ConnectionState::Connecting { .. }));
    }

    // Subscribing

    #[test]
    fn subscribe_sent_starts_streaming() {
        let mut h = Harness::new().subscribing();
        h.advance(millis(50));
        assert_eq!(h.socket(SocketEvent::SubscribeSent), vec![]);
        assert_eq!(
            h.state(),
            ConnectionState::Streaming {
                since: h.mono(),
                next_ping_at: h.mono() + secs(10),
                pong_deadline: h.mono() + secs(30),
            }
        );
    }

    #[test]
    fn subscribe_times_out() {
        let mut h = Harness::new().subscribing();
        assert_eq!(h.tick_after(millis(9_900)), vec![]);
        let actions = h.tick_after(millis(100));

        assert!(matches!(h.state(), ConnectionState::Backoff { .. }));
        let reason = GapReason::ConnectFailed {
            error: "subscribe timed out".into(),
        };
        assert_eq!(
            actions,
            vec![
                ConnectionAction::CloseSocket,
                ConnectionAction::OpenGaps {
                    tokens: tokens(1..=4),
                    start: h.wall(),
                    reason: reason.clone(),
                },
                disconnect(&reason.to_string()),
            ]
        );
    }

    #[test]
    fn subscribe_send_failure_backs_off_and_disconnects() {
        let mut h = Harness::new().subscribing();
        let actions = h.socket(SocketEvent::SendFailed {
            error: "broken pipe".into(),
        });

        assert!(matches!(h.state(), ConnectionState::Backoff { .. }));
        assert!(h.conn.subscribed_tokens().is_empty());
        assert_eq!(
            actions,
            vec![
                ConnectionAction::CloseSocket,
                ConnectionAction::OpenGaps {
                    tokens: tokens(1..=4),
                    start: h.wall(),
                    reason: GapReason::ConnectionClosed {
                        error: "broken pipe".into()
                    },
                },
                disconnect("connection closed due to error: broken pipe"),
            ]
        );
    }

    #[test]
    fn token_changes_while_subscribing_are_reconciled_when_streaming() {
        let mut h = Harness::new().subscribing();
        assert_eq!(h.assign(tokens([5, 6])), vec![]);
        assert_eq!(
            h.remove(tokens([1])),
            vec![ConnectionAction::CloseGaps {
                tokens: tokens([1]),
                end: h.wall(),
                reason: GapReason::Unsubscribed,
            }]
        );

        let actions = h.socket(SocketEvent::SubscribeSent);
        assert_eq!(
            actions,
            vec![
                ConnectionAction::SendSubscribe {
                    tokens: tokens([5, 6]),
                },
                ConnectionAction::RequestSnapshot {
                    tokens: tokens([5, 6]),
                    reason: SnapshotReason::NewToken,
                },
                ConnectionAction::Emit {
                    body: ControlBody::Subscribe {
                        slot: slot(),
                        tokens: tokens([5, 6]),
                    },
                },
                ConnectionAction::SendUnsubscribe {
                    tokens: tokens([1]),
                },
                ConnectionAction::Emit {
                    body: ControlBody::Unsubscribe {
                        slot: slot(),
                        tokens: tokens([1]),
                    },
                },
            ]
        );
        assert_eq!(h.conn.assigned_tokens(), &tokens(2..=6));
        assert_eq!(h.conn.subscribed_tokens(), &tokens(2..=6));
    }

    #[test]
    fn token_removed_and_readded_while_subscribing_needs_no_change() {
        let mut h = Harness::new().subscribing();
        h.remove(tokens([1]));
        h.assign(tokens([1]));
        assert_eq!(h.socket(SocketEvent::SubscribeSent), vec![]);
    }

    #[test]
    fn all_tokens_removed_while_subscribing_closes() {
        let mut h = Harness::new().subscribing();
        let actions = h.remove(tokens(1..=4));

        assert_eq!(h.state(), ConnectionState::Closed);
        assert_eq!(
            actions,
            vec![
                ConnectionAction::CloseGaps {
                    tokens: tokens(1..=4),
                    end: h.wall(),
                    reason: GapReason::Unsubscribed,
                },
                ConnectionAction::CloseSocket,
                disconnect("no tokens"),
            ]
        );
    }

    #[test]
    fn unexpected_socket_events_while_subscribing_are_ignored() {
        let mut h = Harness::new().subscribing();
        let at = h.mono();
        assert_eq!(h.socket(SocketEvent::Opened), vec![]);
        assert_eq!(h.socket(SocketEvent::PongReceived { at }), vec![]);
        assert!(matches!(h.state(), ConnectionState::Subscribing { .. }));
    }

    // Streaming

    #[test]
    fn socket_close_opens_gaps_from_last_message() {
        let mut h = Harness::new().streaming();
        h.advance(secs(5));
        let last_message = h.wall();
        h.send(ConnectionEvent::Tick {
            last_message_at: Some(last_message),
        });
        h.advance(secs(3));

        let actions = h.socket(SocketEvent::Closed {
            reason: "eof".into(),
        });
        assert_eq!(
            actions[1],
            ConnectionAction::OpenGaps {
                tokens: tokens(1..=4),
                start: last_message,
                reason: GapReason::ConnectionClosed {
                    error: "eof".into()
                },
            }
        );
        assert_ne!(last_message, h.wall());
    }

    #[test]
    fn heartbeat_pings_every_interval() {
        let mut h = Harness::new().streaming();
        assert_eq!(h.tick_after(millis(9_900)), vec![]);
        assert_eq!(h.tick_after(millis(100)), vec![ConnectionAction::SendPing]);
        assert_eq!(h.tick(), vec![]);
        assert_eq!(h.tick_after(millis(9_900)), vec![]);
        assert_eq!(h.tick_after(millis(100)), vec![ConnectionAction::SendPing]);
    }

    #[test]
    fn heartbeat_dead_at_exactly_the_timeout() {
        let mut h = Harness::new().streaming();
        assert_eq!(h.tick_after(secs(10)), vec![ConnectionAction::SendPing]);
        assert_eq!(h.tick_after(secs(10)), vec![ConnectionAction::SendPing]);
        assert_eq!(h.tick_after(millis(9_900)), vec![]);
        assert!(matches!(h.state(), ConnectionState::Streaming { .. }));

        let actions = h.tick_after(millis(100));
        assert!(matches!(h.state(), ConnectionState::Backoff { .. }));
        assert_eq!(
            actions,
            vec![
                ConnectionAction::CloseSocket,
                ConnectionAction::OpenGaps {
                    tokens: tokens(1..=4),
                    start: h.wall(),
                    reason: GapReason::HeartbeatTimeout,
                },
                disconnect("heartbeat timed out"),
            ]
        );
    }

    #[test]
    fn regular_pongs_keep_the_connection_alive() {
        let mut h = Harness::new().streaming();
        for _ in 0..30 {
            assert_eq!(h.tick_after(secs(10)), vec![ConnectionAction::SendPing]);
            h.pong();
        }
        assert!(matches!(h.state(), ConnectionState::Streaming { .. }));
        assert_eq!(h.conn.last_pong_at(), Some(h.mono()));
    }

    #[test]
    fn data_messages_are_not_liveness() {
        let mut h = Harness::new().streaming();
        for _ in 0..29 {
            h.advance(secs(1));
            let at = h.wall();
            h.send(ConnectionEvent::Tick {
                last_message_at: Some(at),
            });
        }
        assert!(matches!(h.state(), ConnectionState::Streaming { .. }));

        h.advance(secs(1));
        let at = h.wall();
        h.send(ConnectionEvent::Tick {
            last_message_at: Some(at),
        });
        assert!(matches!(h.state(), ConnectionState::Backoff { .. }));
    }

    #[test]
    fn wall_clock_jump_does_not_affect_heartbeat() {
        let mut h = Harness::new().streaming();
        h.start_wall -= TimeDelta::hours(1);
        assert_eq!(h.tick_after(secs(10)), vec![ConnectionAction::SendPing]);
        h.pong();
        assert_eq!(h.tick_after(secs(29)), vec![ConnectionAction::SendPing]);
        assert!(matches!(h.state(), ConnectionState::Streaming { .. }));
    }

    #[test]
    fn tokens_added_while_streaming_subscribes_only_new_ones() {
        let mut h = Harness::new().streaming();
        let actions = h.assign(tokens(3..=6));

        assert_eq!(
            actions,
            vec![
                ConnectionAction::SendSubscribe {
                    tokens: tokens([5, 6]),
                },
                ConnectionAction::RequestSnapshot {
                    tokens: tokens([5, 6]),
                    reason: SnapshotReason::NewToken,
                },
                ConnectionAction::Emit {
                    body: ControlBody::Subscribe {
                        slot: slot(),
                        tokens: tokens([5, 6]),
                    },
                },
            ]
        );
        assert_eq!(h.conn.assigned_tokens(), &tokens(1..=6));
        assert_eq!(h.conn.subscribed_tokens(), &tokens(1..=6));
        assert!(matches!(h.state(), ConnectionState::Streaming { .. }));
    }

    #[test]
    fn already_assigned_tokens_while_streaming_do_nothing() {
        let mut h = Harness::new().streaming();
        assert_eq!(h.assign(tokens([1, 2])), vec![]);
    }

    #[test]
    fn live_subscribe_completion_is_ignored() {
        let mut h = Harness::new().streaming();
        h.assign(tokens([5]));
        assert_eq!(h.socket(SocketEvent::SubscribeSent), vec![]);
        assert!(matches!(h.state(), ConnectionState::Streaming { .. }));
    }

    #[test]
    fn tokens_removed_while_streaming_unsubscribes() {
        let mut h = Harness::new().streaming();
        let actions = h.remove(tokens([1, 99]));

        assert_eq!(
            actions,
            vec![
                ConnectionAction::SendUnsubscribe {
                    tokens: tokens([1]),
                },
                ConnectionAction::Emit {
                    body: ControlBody::Unsubscribe {
                        slot: slot(),
                        tokens: tokens([1]),
                    },
                },
                ConnectionAction::CloseGaps {
                    tokens: tokens([1]),
                    end: h.wall(),
                    reason: GapReason::Unsubscribed,
                },
            ]
        );
        assert_eq!(h.conn.subscribed_tokens(), &tokens(2..=4));
    }

    #[test]
    fn unknown_tokens_removed_while_streaming_do_nothing() {
        let mut h = Harness::new().streaming();
        assert_eq!(h.remove(tokens([99])), vec![]);
    }

    #[test]
    fn last_token_removed_closes_the_connection() {
        let mut h = Harness::new().streaming();
        let actions = h.remove(tokens(1..=4));

        assert_eq!(h.state(), ConnectionState::Closed);
        assert_eq!(
            actions,
            vec![
                ConnectionAction::SendUnsubscribe {
                    tokens: tokens(1..=4),
                },
                ConnectionAction::Emit {
                    body: ControlBody::Unsubscribe {
                        slot: slot(),
                        tokens: tokens(1..=4),
                    },
                },
                ConnectionAction::CloseGaps {
                    tokens: tokens(1..=4),
                    end: h.wall(),
                    reason: GapReason::Unsubscribed,
                },
                ConnectionAction::CloseSocket,
                disconnect("no tokens"),
            ]
        );
    }

    #[test]
    fn queue_overflow_forces_a_reconnect() {
        let mut h = Harness::new().streaming();
        let actions = h.socket(SocketEvent::QueueOverflow);

        assert!(matches!(h.state(), ConnectionState::Backoff { .. }));
        assert_eq!(
            actions[1],
            ConnectionAction::OpenGaps {
                tokens: tokens(1..=4),
                start: h.wall(),
                reason: GapReason::QueueOverflow,
            }
        );
    }

    #[test]
    fn duplicate_open_while_streaming_is_ignored() {
        let mut h = Harness::new().streaming();
        assert_eq!(h.socket(SocketEvent::Opened), vec![]);
        assert!(matches!(h.state(), ConnectionState::Streaming { .. }));
    }

    // Backoff

    #[test]
    fn backoff_waits_then_reconnects_with_a_new_attempt_id() {
        let mut h = Harness::new().backoff();
        assert_eq!(h.tick_after(millis(999)), vec![]);
        assert_eq!(
            h.tick_after(millis(1)),
            vec![ConnectionAction::OpenSocket {
                url: h.cfg.websocket_url.clone(),
                attempt_id: attempt(2),
            }]
        );
        assert_eq!(
            h.state(),
            ConnectionState::Connecting {
                started_at: h.mono()
            }
        );
        assert_eq!(h.conn.attempt_id(), Some(&attempt(2)));
    }

    #[test]
    fn tokens_added_in_backoff_are_subscribed_on_reconnect() {
        let mut h = Harness::new().backoff();
        assert_eq!(h.assign(tokens([5])), vec![]);
        assert!(matches!(h.state(), ConnectionState::Backoff { .. }));

        h.reconnect();
        let actions = h.socket(SocketEvent::Opened);
        assert!(actions.contains(&ConnectionAction::SendSubscribe {
            tokens: tokens(1..=5)
        }));
        assert!(actions.contains(&ConnectionAction::RequestSnapshot {
            tokens: tokens(1..=5),
            reason: SnapshotReason::Subscribe,
        }));
    }

    #[test]
    fn tokens_removed_in_backoff_close_their_gaps() {
        let mut h = Harness::new().streaming();
        h.socket(SocketEvent::Closed {
            reason: "eof".into(),
        });
        assert_eq!(
            h.remove(tokens([1])),
            vec![ConnectionAction::CloseGaps {
                tokens: tokens([1]),
                end: h.wall(),
                reason: GapReason::Unsubscribed,
            }]
        );

        h.reconnect();
        let actions = h.socket(SocketEvent::Opened);
        assert!(actions.contains(&ConnectionAction::SendSubscribe {
            tokens: tokens(2..=4)
        }));
    }

    #[test]
    fn all_tokens_removed_in_backoff_closes_without_socket_actions() {
        let mut h = Harness::new().backoff();
        let actions = h.remove(tokens(1..=4));

        assert_eq!(h.state(), ConnectionState::Closed);
        assert_eq!(
            actions,
            vec![ConnectionAction::CloseGaps {
                tokens: tokens(1..=4),
                end: h.wall(),
                reason: GapReason::Unsubscribed,
            }]
        );
    }

    #[test]
    fn backoff_doubles_on_repeated_failures() {
        let mut h = Harness::new();
        for expected in [1, 2, 4, 8, 16] {
            h.socket(SocketEvent::Failed {
                error: "refused".into(),
            });
            assert_eq!(h.backoff_wait(), secs(expected));
            h.reconnect();
        }
    }

    #[test]
    fn backoff_is_capped() {
        let mut h = Harness::new();
        for _ in 0..10 {
            h.socket(SocketEvent::Failed {
                error: "refused".into(),
            });
            h.reconnect();
        }
        h.socket(SocketEvent::Failed {
            error: "refused".into(),
        });
        assert_eq!(h.backoff_wait(), secs(60));
    }

    /// Fails three times (waits of 1, 2 and 4 s), then streams for `d` and loses the socket.
    fn wait_after_streaming_for(d: Duration) -> Duration {
        let mut h = Harness::new();
        for _ in 0..3 {
            h.socket(SocketEvent::Failed {
                error: "refused".into(),
            });
            h.reconnect();
        }
        h.socket(SocketEvent::Opened);
        h.socket(SocketEvent::SubscribeSent);
        h.stream_for(d);
        h.socket(SocketEvent::Closed {
            reason: "eof".into(),
        });
        h.backoff_wait()
    }

    #[test]
    fn backoff_resets_after_stable_streaming() {
        assert_eq!(wait_after_streaming_for(Duration::from_mins(5)), secs(1));
    }

    #[test]
    fn backoff_continues_if_streaming_was_short() {
        assert_eq!(wait_after_streaming_for(secs(299)), secs(8));
    }

    // Shutdown

    fn close_gaps_on_shutdown(h: &Harness) -> ConnectionAction {
        ConnectionAction::CloseGaps {
            tokens: tokens(1..=4),
            end: h.wall(),
            reason: GapReason::Shutdown,
        }
    }

    #[test]
    fn shutdown_while_connecting() {
        let mut h = Harness::new();
        let actions = h.send(ConnectionEvent::Shutdown);
        assert_eq!(h.state(), ConnectionState::Closed);
        assert_eq!(
            actions,
            vec![ConnectionAction::CloseSocket, close_gaps_on_shutdown(&h)]
        );
    }

    #[test]
    fn shutdown_while_subscribing() {
        let mut h = Harness::new().subscribing();
        let actions = h.send(ConnectionEvent::Shutdown);
        assert_eq!(h.state(), ConnectionState::Closed);
        assert_eq!(
            actions,
            vec![
                ConnectionAction::CloseSocket,
                disconnect("shutdown"),
                close_gaps_on_shutdown(&h),
            ]
        );
    }

    #[test]
    fn shutdown_while_streaming() {
        let mut h = Harness::new().streaming();
        let actions = h.send(ConnectionEvent::Shutdown);
        assert_eq!(h.state(), ConnectionState::Closed);
        assert!(h.conn.subscribed_tokens().is_empty());
        assert_eq!(
            actions,
            vec![
                ConnectionAction::CloseSocket,
                disconnect("shutdown"),
                close_gaps_on_shutdown(&h),
            ]
        );
    }

    #[test]
    fn shutdown_while_in_backoff() {
        let mut h = Harness::new().backoff();
        let actions = h.send(ConnectionEvent::Shutdown);
        assert_eq!(h.state(), ConnectionState::Closed);
        assert_eq!(actions, vec![close_gaps_on_shutdown(&h)]);
    }

    #[test]
    fn shutdown_twice_does_nothing_the_second_time() {
        let mut h = Harness::new().streaming();
        h.send(ConnectionEvent::Shutdown);
        assert_eq!(h.send(ConnectionEvent::Shutdown), vec![]);
        assert_eq!(h.state(), ConnectionState::Closed);
    }

    // Closed

    #[test]
    fn closed_connection_ignores_socket_events_ticks_and_removals() {
        let mut h = Harness::new().streaming();
        h.send(ConnectionEvent::Shutdown);
        assert_eq!(
            h.socket(SocketEvent::Closed {
                reason: "eof".into()
            }),
            vec![]
        );
        assert_eq!(h.tick_after(Duration::from_hours(1)), vec![]);
        assert_eq!(h.remove(tokens([1])), vec![]);
        assert_eq!(h.state(), ConnectionState::Closed);
    }

    #[test]
    #[should_panic(expected = "tokens assigned to a closed connection")]
    fn tokens_assigned_to_a_closed_connection_is_a_bug() {
        let mut h = Harness::new();
        h.send(ConnectionEvent::Shutdown);
        h.assign(tokens([5]));
    }

    // Races between the state machine and the socket tasks

    #[test]
    fn stale_events_while_connecting_are_ignored() {
        let mut h = Harness::new().backoff();
        h.reconnect();
        for event in [
            SocketEvent::Opened,
            SocketEvent::Failed {
                error: "refused".into(),
            },
            SocketEvent::Closed {
                reason: "eof".into(),
            },
            SocketEvent::SendFailed {
                error: "broken pipe".into(),
            },
            SocketEvent::QueueOverflow,
        ] {
            assert_eq!(h.socket_from(1, event), vec![]);
            assert!(matches!(h.state(), ConnectionState::Connecting { .. }));
        }
    }

    #[test]
    fn stale_events_while_streaming_are_ignored() {
        let mut h = Harness::new().backoff();
        h.reconnect();
        h.socket(SocketEvent::Opened);
        h.socket(SocketEvent::SubscribeSent);

        let failures = h.conn.failures;
        assert_eq!(
            h.socket_from(
                1,
                SocketEvent::SendFailed {
                    error: "broken pipe".into()
                }
            ),
            vec![]
        );
        assert_eq!(
            h.socket_from(
                1,
                SocketEvent::Closed {
                    reason: "eof".into()
                }
            ),
            vec![]
        );
        assert!(matches!(h.state(), ConnectionState::Streaming { .. }));
        assert_eq!(h.conn.failures, failures);
    }

    #[test]
    fn stale_pong_does_not_extend_the_heartbeat() {
        let mut h = Harness::new().backoff();
        h.reconnect();
        h.socket(SocketEvent::Opened);
        h.socket(SocketEvent::SubscribeSent);

        h.tick_after(secs(10));
        let at = h.mono();
        assert_eq!(h.socket_from(1, SocketEvent::PongReceived { at }), vec![]);
        h.tick_after(secs(10));
        h.tick_after(secs(10));
        assert!(matches!(h.state(), ConnectionState::Backoff { .. }));
    }

    #[test]
    fn events_from_the_socket_just_closed_do_not_back_off_twice() {
        let mut h = Harness::new().streaming();
        h.socket(SocketEvent::QueueOverflow);
        let state = h.state();
        assert_eq!(h.conn.failures, 1);

        h.advance(millis(10));
        assert_eq!(
            h.socket(SocketEvent::Closed {
                reason: "closed by us".into()
            }),
            vec![]
        );
        assert_eq!(h.socket(SocketEvent::QueueOverflow), vec![]);
        assert_eq!(h.state(), state);
        assert_eq!(h.conn.failures, 1);
    }

    #[test]
    fn event_from_a_future_attempt_is_ignored() {
        let mut h = Harness::new();
        assert_eq!(h.socket_from(7, SocketEvent::Opened), vec![]);
        assert!(matches!(h.state(), ConnectionState::Connecting { .. }));
    }

    // Invariants over random event sequences

    /// Small deterministic generator, so failures are repeatable from the seed.
    struct Lcg(u64);
    impl Lcg {
        fn below(&mut self, n: u64) -> u64 {
            self.0 = self
                .0
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (self.0 >> 33) % n
        }

        fn tokens(&mut self) -> HashSet<TokenId> {
            let count = 1 + self.below(2);
            (0..count)
                .map(|_| token(1 + self.below(10) as u32))
                .collect()
        }
    }

    /// Tracks the control-stream obligations that analysis tools rely on.
    #[derive(Default)]
    struct Ledger {
        open_gaps: HashSet<TokenId>,
        connected: bool,
    }

    impl Ledger {
        fn apply(&mut self, actions: &[ConnectionAction], assigned_before: &HashSet<TokenId>) {
            for action in actions {
                match action {
                    ConnectionAction::OpenGaps { tokens, .. } => {
                        assert!(tokens.is_subset(assigned_before));
                        self.open_gaps.extend(tokens.iter().cloned());
                    }
                    ConnectionAction::CloseGaps { tokens, .. } => {
                        for t in tokens {
                            self.open_gaps.remove(t);
                        }
                    }
                    ConnectionAction::Emit {
                        body: ControlBody::Connect { .. },
                    } => {
                        assert!(!self.connected, "connect without disconnect");
                        self.connected = true;
                    }
                    ConnectionAction::Emit {
                        body: ControlBody::Disconnect { .. },
                    } => {
                        assert!(self.connected, "disconnect without connect");
                        self.connected = false;
                    }
                    _ => {}
                }
            }
        }
    }

    fn random_event(h: &Harness, rng: &mut Lcg) -> ConnectionEvent {
        let current = h.conn.attempt_seq;
        let attempt_id = match rng.below(5) {
            0 => attempt(current.saturating_sub(1)),
            _ => attempt(current),
        };
        let socket = |event| ConnectionEvent::Socket {
            attempt_id: attempt_id.clone(),
            event,
        };
        match rng.below(12) {
            0..=2 => ConnectionEvent::Tick {
                last_message_at: (rng.below(2) == 0).then(|| h.wall()),
            },
            3 => socket(SocketEvent::Opened),
            4 => socket(SocketEvent::Failed {
                error: "refused".into(),
            }),
            5 => socket(SocketEvent::Closed {
                reason: "eof".into(),
            }),
            6 => socket(SocketEvent::SendFailed {
                error: "broken pipe".into(),
            }),
            7 => socket(SocketEvent::SubscribeSent),
            8 => socket(SocketEvent::PongReceived { at: h.mono() }),
            9 => socket(SocketEvent::QueueOverflow),
            10 => ConnectionEvent::TokensAssigned {
                tokens: rng.tokens(),
            },
            _ => ConnectionEvent::TokensRemoved {
                tokens: rng.tokens(),
            },
        }
    }

    #[test]
    fn random_event_sequences_keep_the_control_stream_consistent() {
        // Steps taken in Connecting, Subscribing, Streaming and Backoff, to show the sequences
        // actually exercise every state.
        let mut visited = [0u32; 4];
        for seed in 0..200 {
            let mut rng = Lcg(seed);
            let mut h = Harness::with_tokens(tokens(1..=6));
            let mut ledger = Ledger::default();

            for _ in 0..2_000 {
                if h.state() == ConnectionState::Closed {
                    break;
                }
                h.advance(millis(rng.below(15_000)));
                let event = random_event(&h, &mut rng);
                let assigned_before = h.conn.assigned_tokens().clone();
                let actions = h.send(event);
                ledger.apply(&actions, &assigned_before);

                match h.state() {
                    ConnectionState::Connecting { .. } => {
                        visited[0] += 1;
                        assert!(h.conn.subscribed_tokens().is_empty());
                    }
                    ConnectionState::Subscribing { .. } => visited[1] += 1,
                    ConnectionState::Streaming { .. } => {
                        visited[2] += 1;
                        assert_eq!(h.conn.subscribed_tokens(), h.conn.assigned_tokens());
                    }
                    ConnectionState::Backoff { .. } => {
                        visited[3] += 1;
                        assert!(h.conn.subscribed_tokens().is_empty());
                    }
                    ConnectionState::Closed => assert!(h.conn.subscribed_tokens().is_empty()),
                }
                if h.state() != ConnectionState::Closed {
                    assert!(!h.conn.assigned_tokens().is_empty());
                }
            }

            let assigned_before = h.conn.assigned_tokens().clone();
            let actions = h.send(ConnectionEvent::Shutdown);
            ledger.apply(&actions, &assigned_before);

            assert_eq!(h.state(), ConnectionState::Closed, "seed {seed}");
            assert!(
                ledger.open_gaps.is_empty(),
                "seed {seed}: gaps left open {:?}",
                ledger.open_gaps
            );
            assert!(!ledger.connected, "seed {seed}: connect left unpaired");
        }
        assert!(visited.iter().all(|&n| n >= 100), "{visited:?}");
    }
}
