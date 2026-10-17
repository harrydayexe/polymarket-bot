use std::{collections::HashSet, time::Duration, vec};

use chrono::{DateTime, Utc};
use marcasite::types::TokenId;
use tokio::time::Instant;

use crate::{
    config::Config,
    connection_manager::{
        backoff_delay::backoff_delay,
        types::{ConnAttemptId, ConnSlotId, ConnectionAction, ConnectionEvent, ConnectionState},
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

pub struct Now {
    mono: Instant,
    wall: DateTime<Utc>,
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
        && now.mono - since >= cfg.backoff_max
    {
        c.failures = 0;
    }

    c.failures += 1;
    let delay = backoff_delay(c.failures, cfg, rng);
    c.subscribed_tokens.clear();
    c.state = ConnectionState::Backoff {
        until: now.mono + delay,
    };
    vec![
        ConnectionAction::CloseSocket,
        ConnectionAction::OpenGaps {
            tokens: c.assigned_tokens.clone(),
            start: c.last_message_at.unwrap_or(now.wall),
            reason: reason.clone(),
        },
        ConnectionAction::Emit {
            body: ControlBody::Disconnect {
                slot: c.slot_id.clone(),
                reason: reason.to_string(),
            },
        },
    ]
}

fn connecting_state_transitions(
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
        ConnectionEvent::SocketOpened => {
            connection.subscribed_tokens = connection.assigned_tokens.clone();
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
        ConnectionEvent::SocketFailed { error } => enter_backoff(
            connection,
            GapReason::ConnectFailed { error },
            now,
            config,
            rng,
        ),
        ConnectionEvent::Tick if now.mono - started_at >= Duration::from_secs(10) => enter_backoff(
            connection,
            GapReason::ConnectFailed {
                error: "socket connect timed out".to_string(),
            },
            now,
            config,
            rng,
        ),
        ConnectionEvent::TokensAssigned { tokens } => {
            connection.assigned_tokens.extend(tokens.iter().cloned());
            vec![]
        }
        ConnectionEvent::TokensRemoved { tokens } => {
            for t in &tokens {
                connection.assigned_tokens.remove(t);
            }
            if connection.assigned_tokens.is_empty() {
                connection.state = ConnectionState::Closed;
                vec![ConnectionAction::CloseSocket]
            } else {
                vec![ConnectionAction::CloseGaps {
                    tokens,
                    end: now.wall,
                    reason: GapReason::Unsubscribed,
                }]
            }
        }
        ConnectionEvent::Shutdown => {
            connection.state = ConnectionState::Closed;
            vec![
                ConnectionAction::CloseSocket,
                ConnectionAction::CloseGaps {
                    tokens: connection.assigned_tokens.clone(),
                    end: now.wall,
                    reason: GapReason::Shutdown,
                },
            ]
        }
        e => {
            tracing::warn!(
                "impossible input to state machine, in state connecting, received event: {}",
                e
            );
            vec![]
        }
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
        ConnectionEvent::SocketClosed { reason } => enter_backoff(
            connection,
            GapReason::ConnectionClosed { error: reason },
            now,
            config,
            rng,
        ),
        ConnectionEvent::SendFailed { error } => enter_backoff(
            connection,
            GapReason::ConnectionClosed { error },
            now,
            config,
            rng,
        ),
        ConnectionEvent::Tick if now.mono - since >= Duration::from_secs(10) => enter_backoff(
            connection,
            GapReason::ConnectFailed {
                error: "socket connect timed out".to_string(),
            },
            now,
            config,
            rng,
        ),
        ConnectionEvent::TokensAssigned { tokens } => {
            let new: HashSet<TokenId> = tokens
                .difference(&connection.assigned_tokens)
                .cloned()
                .collect();
            if new.is_empty() {
                return vec![];
            }
            connection.assigned_tokens.extend(new.clone());
            connection.subscribed_tokens.extend(new.clone());

            vec![
                ConnectionAction::SendSubscribe {
                    tokens: new.clone(),
                },
                ConnectionAction::RequestSnapshot {
                    tokens: new.clone(),
                    reason: SnapshotReason::NewToken,
                },
                ConnectionAction::Emit {
                    body: ControlBody::Subscribe {
                        slot: connection.slot_id.clone(),
                        tokens: new.clone(),
                    },
                },
            ]
        }
        ConnectionEvent::SubscribeSent => {
            connection.state = ConnectionState::Streaming {
                since,
                next_ping_at: now.mono,
                pong_deadline: now.mono + Duration::from_secs(30),
            };
            vec![]
        }
        ConnectionEvent::TokensRemoved { mut tokens } => {
            tokens.retain(|t| connection.assigned_tokens.remove(t));
            if tokens.is_empty() {
                return vec![];
            }
            let mut actions = vec![
                ConnectionAction::SendUnsubscribe {
                    tokens: tokens.clone(),
                },
                ConnectionAction::CloseGaps {
                    tokens: tokens.clone(),
                    end: now.wall,
                    reason: GapReason::Unsubscribed,
                },
                ConnectionAction::Emit {
                    body: ControlBody::Unsubscribe {
                        slot: connection.slot_id.clone(),
                        tokens: tokens.clone(),
                    },
                },
            ];

            if connection.assigned_tokens.is_empty() {
                actions.push(ConnectionAction::CloseSocket);
                actions.push(ConnectionAction::Emit {
                    body: ControlBody::Disconnect {
                        slot: connection.slot_id.clone(),
                        reason: "no tokens".into(),
                    },
                });
                connection.state = ConnectionState::Closed;
            }

            actions
        }
        ConnectionEvent::QueueOverflow => {
            enter_backoff(connection, GapReason::QueueOverflow, now, config, rng)
        }
        ConnectionEvent::Shutdown => {
            connection.state = ConnectionState::Closed;
            vec![
                ConnectionAction::CloseSocket,
                ConnectionAction::Emit {
                    body: ControlBody::Disconnect {
                        slot: connection.slot_id.clone(),
                        reason: "shutdown".into(),
                    },
                },
            ]
        }
        e => {
            tracing::warn!(
                "impossible input to state machine, in state connecting, received event: {}",
                e
            );
            vec![]
        }
    }
}

fn closed_state_transition(
    connection: &mut Connection,
    event: ConnectionEvent,
    now: Now,
    config: &Config,
    rng: &mut impl RandomSource,
) -> Vec<ConnectionAction> {
    if !matches!(connection.state, ConnectionState::Closed) {
        unreachable!("expected connection state to be ConnectionState::Subscribing");
    };

    todo!("DO matches here");
}

pub fn step(
    connection: &mut Connection,
    event: ConnectionEvent,
    now: Now,
    config: &Config,
    rng: &mut impl RandomSource,
) -> Vec<ConnectionAction> {
    match connection.state {
        ConnectionState::Connecting { started_at: _ } => {
            connecting_state_transitions(connection, event, now, config, rng)
        }
        ConnectionState::Subscribing { since: _ } => {
            subscribing_state_transition(connection, event, now, config, rng)
        }
        ConnectionState::Streaming { since } => todo!(),
        ConnectionState::Backoff { until } => todo!(),
        ConnectionState::Closed => todo!(),
    }
}
