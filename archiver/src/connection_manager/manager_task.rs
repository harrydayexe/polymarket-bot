use std::{sync::Arc, time::Duration};

use anyhow::Result;
use tokio::{
    sync::mpsc,
    time::{self, MissedTickBehavior},
};
use tokio_util::sync::CancellationToken;

use crate::{
    clock::SharedClock,
    config::Config,
    connection_manager::manager::ConnectionManager,
    messages::{ConnectionManagerMsg, Record, SnapshotMsg},
    types::SystemRandom,
};

/// How often every connection's timers are checked.
const TICK_INTERVAL: Duration = Duration::from_millis(100);

/// Keep the websocket connections for the tracked tokens open until `token` is cancelled, then
/// close them.
///
/// # Errors
///
/// Only if the write queue or the snapshot worker has gone away. Socket failures are recorded
/// as gaps and the connection reconnects.
pub async fn connection_manager_task(
    mut inbox: mpsc::Receiver<ConnectionManagerMsg>,
    write_queue: mpsc::Sender<Record>,
    snapshots: mpsc::UnboundedSender<SnapshotMsg>,
    token: CancellationToken,
    config: Arc<Config>,
    clock: SharedClock,
) -> Result<()> {
    let (reports_tx, mut reports) = mpsc::unbounded_channel();
    let mut manager = ConnectionManager::new(
        config,
        clock,
        SystemRandom,
        write_queue,
        snapshots,
        reports_tx,
    );

    let mut tick = time::interval(TICK_INTERVAL);
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);

    let result = loop {
        // Socket reports before registry changes before timers, so a connection's own news is
        // applied before anything else touches it.
        let r = tokio::select! {
            biased;

            _ = token.cancelled() => break Ok(()),

            Some(report) = reports.recv() => manager.handle_report(report).await,

            // Disabled once the registry has gone; it only goes at shutdown or on a failure,
            // which cancels `token`.
            Some(msg) = inbox.recv() => match msg {
                ConnectionManagerMsg::TokenChanges(changes) => {
                    manager.apply_token_changes(changes).await
                }
            },

            _ = tick.tick() => manager.tick().await,
        };
        if let Err(e) = r {
            break Err(e);
        }
    };

    let closed = manager.shutdown().await;
    result.and(closed)
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use chrono::{DateTime, Utc};
    use marcasite::{
        types::{ConditionId, TokenId},
        ws::MarketEvent,
    };
    use serde_json::{Value, json};
    use tokio::task::JoinHandle;

    use super::*;
    use crate::{
        clock::SystemClock,
        connection_manager::{
            fake_server::{FakeServer, TIMEOUT},
            types::{ConnAttemptId, ConnSlotId},
        },
        market_registry::token_changes::{MarketTokens, TokenChanges},
        messages::{ControlBody, GapReason, SnapshotReason},
    };

    fn yes(n: u32) -> TokenId {
        TokenId::new(format!("{n}-yes"))
    }

    fn no(n: u32) -> TokenId {
        TokenId::new(format!("{n}-no"))
    }

    fn tokens_of(markets: &[u32]) -> HashSet<TokenId> {
        markets.iter().flat_map(|&n| [yes(n), no(n)]).collect()
    }

    fn market_changes(markets: &[u32]) -> HashMap<ConditionId, MarketTokens> {
        markets
            .iter()
            .map(|&n| {
                (
                    ConditionId::new(format!("0x{n:02}")),
                    MarketTokens {
                        yes: yes(n),
                        no: no(n),
                    },
                )
            })
            .collect()
    }

    /// The `assets_ids` of a subscription message, as a set.
    fn asset_ids(msg: &Value) -> HashSet<TokenId> {
        msg["assets_ids"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| TokenId::new(t.as_str().unwrap()))
            .collect()
    }

    fn slot(n: u64) -> ConnSlotId {
        ConnSlotId::new(format!("c-{n:04}"))
    }

    fn attempt(slot_no: u64, n: u64) -> ConnAttemptId {
        ConnAttemptId::create(slot(slot_no), n)
    }

    /// Runs the connection manager task against a fake market channel, with timings shrunk so
    /// each test takes well under a second.
    struct Harness {
        server: FakeServer,
        registry: mpsc::Sender<ConnectionManagerMsg>,
        records: mpsc::Receiver<Record>,
        snapshots: mpsc::UnboundedReceiver<SnapshotMsg>,
        token: CancellationToken,
        task: JoinHandle<Result<()>>,
    }

    impl Harness {
        async fn start() -> Self {
            Self::with(|_| {}).await
        }

        async fn with(customise: impl FnOnce(&mut Config)) -> Self {
            let server = FakeServer::start().await;
            let mut config = Config {
                websocket_url: server.url(),
                heartbeat_interval: Duration::from_millis(100),
                heartbeat_timeout: Duration::from_millis(300),
                connect_timeout: Duration::from_secs(1),
                backoff_initial: Duration::from_millis(50),
                backoff_max: Duration::from_millis(200),
                ..Config::default()
            };
            customise(&mut config);
            let (registry, inbox) = mpsc::channel(16);
            let (write_queue, records) = mpsc::channel(config.write_queue_capacity);
            let (snapshot_tx, snapshots) = mpsc::unbounded_channel();
            let token = CancellationToken::new();
            let task = tokio::spawn(connection_manager_task(
                inbox,
                write_queue,
                snapshot_tx,
                token.clone(),
                Arc::new(config),
                Arc::new(SystemClock),
            ));
            Harness {
                server,
                registry,
                records,
                snapshots,
                token,
                task,
            }
        }

        async fn add(&self, markets: &[u32]) {
            let changes = TokenChanges {
                added: market_changes(markets),
                ..TokenChanges::default()
            };
            self.registry
                .send(ConnectionManagerMsg::TokenChanges(changes))
                .await
                .unwrap();
        }

        async fn remove(&self, markets: &[u32]) {
            let changes = TokenChanges {
                removed: market_changes(markets),
                ..TokenChanges::default()
            };
            self.registry
                .send(ConnectionManagerMsg::TokenChanges(changes))
                .await
                .unwrap();
        }

        async fn record(&mut self) -> Record {
            time::timeout(TIMEOUT, self.records.recv())
                .await
                .expect("timed out waiting for a record")
                .expect("write queue closed")
        }

        /// The next control record, failing on a frame.
        async fn control(&mut self) -> (Option<ConnAttemptId>, ControlBody) {
            match self.record().await {
                Record::Control { conn_id, body, .. } => (conn_id, body),
                other => panic!("expected a control record, got {other:?}"),
            }
        }

        /// The next frame record, failing on anything else.
        async fn frame(&mut self) -> (ConnAttemptId, MarketEvent, DateTime<Utc>) {
            match self.record().await {
                Record::WsFrame {
                    conn_id,
                    event,
                    recv_ts,
                } => (conn_id, event, recv_ts),
                other => panic!("expected a frame, got {other:?}"),
            }
        }

        async fn snapshot(&mut self) -> SnapshotMsg {
            time::timeout(TIMEOUT, self.snapshots.recv())
                .await
                .expect("timed out waiting for a snapshot request")
                .expect("snapshot channel closed")
        }

        /// Adds `markets` as the first tokens, then accepts the connection and checks it
        /// connected and subscribed to them.
        async fn streaming(
            &mut self,
            markets: &[u32],
        ) -> crate::connection_manager::fake_server::FakeConn {
            self.add(markets).await;
            let mut conn = self.server.accept().await;
            assert_eq!(asset_ids(&conn.recv_json().await), tokens_of(markets));
            assert!(matches!(
                self.control().await.1,
                ControlBody::Connect { .. }
            ));
            assert!(matches!(
                self.control().await.1,
                ControlBody::Subscribe { .. }
            ));
            conn
        }

        /// Cancels the task and waits for it to finish.
        async fn stop(&mut self) {
            self.token.cancel();
            time::timeout(TIMEOUT, &mut self.task)
                .await
                .expect("task did not stop")
                .unwrap()
                .expect("task failed");
        }
    }

    #[tokio::test]
    async fn frames_are_recorded_as_received() {
        let mut h = Harness::start().await;
        h.add(&[1, 2]).await;

        let mut conn = h.server.accept().await;
        let subscription = conn.recv_json().await;
        assert_eq!(
            subscription,
            json!({
                "assets_ids": ["1-no", "1-yes", "2-no", "2-yes"],
                "type": "market",
                "custom_feature_enabled": true,
            })
        );
        assert_eq!(
            h.control().await,
            (
                Some(attempt(1, 1)),
                ControlBody::Connect {
                    slot: slot(1),
                    token_count: 4,
                }
            )
        );
        assert_eq!(
            h.control().await,
            (
                Some(attempt(1, 1)),
                ControlBody::Subscribe {
                    slot: slot(1),
                    tokens: tokens_of(&[1, 2]),
                }
            )
        );
        assert_eq!(
            h.snapshot().await,
            SnapshotMsg::SnapshotRequest {
                tokens: tokens_of(&[1, 2]),
                reason: SnapshotReason::Subscribe,
            }
        );

        conn.send(
            r#"{"event_type":"book","asset_id":"1-yes","market":"0x01","timestamp":"1757908892351",
                "hash":"h1","bids":[{"price":"0.48","size":"30"}],"asks":[]}"#,
        );
        // An array frame is one record per element; unrecognised messages are kept as JSON.
        conn.send(r#"[{"event_type":"brand_new"},{"event_type":"brand_new","n":2}]"#);
        conn.send("not json at all");

        let (conn_id, event, _) = h.frame().await;
        assert_eq!(conn_id, attempt(1, 1));
        let MarketEvent::Book(book) = event else {
            panic!("expected a book, got {event:?}");
        };
        assert_eq!(
            (book.asset_id.as_ref(), book.hash.as_str()),
            ("1-yes", "h1")
        );
        for expected in [
            json!({"event_type": "brand_new"}),
            json!({"event_type": "brand_new", "n": 2}),
            json!("not json at all"),
        ] {
            let (conn_id, event, _) = h.frame().await;
            assert_eq!(conn_id, attempt(1, 1));
            assert_eq!(event, MarketEvent::Unknown(expected));
        }

        // A recognised message that does not decode is not lost silently.
        conn.send(r#"{"event_type":"book","asset_id":"1-yes"}"#);
        let (conn_id, body) = h.control().await;
        assert_eq!(conn_id, Some(attempt(1, 1)));
        assert!(
            matches!(&body, ControlBody::FrameDecodeFailed { error } if error.contains("book")),
            "{body:?}"
        );

        h.stop().await;
        assert_eq!(
            h.control().await.1,
            ControlBody::Disconnect {
                slot: slot(1),
                reason: "shutdown".into(),
            }
        );
        conn.closed().await;
    }

    #[tokio::test]
    async fn server_close_opens_a_gap_and_resubscribes_every_token() {
        let mut h = Harness::start().await;
        let conn = h.streaming(&[1]).await;

        conn.send(r#"{"event_type":"anything"}"#);
        let (_, _, last_message_at) = h.frame().await;
        // Let a tick copy the last message time into the connection.
        time::sleep(Duration::from_millis(250)).await;

        // A market added while streaming is subscribed live.
        h.add(&[2]).await;
        let mut conn = conn;
        assert_eq!(
            conn.recv_json().await,
            json!({
                "operation": "subscribe",
                "assets_ids": ["2-no", "2-yes"],
                "custom_feature_enabled": true,
            })
        );
        assert_eq!(
            h.control().await.1,
            ControlBody::Subscribe {
                slot: slot(1),
                tokens: tokens_of(&[2]),
            }
        );

        conn.close();
        let (conn_id, body) = h.control().await;
        assert_eq!(conn_id, Some(attempt(1, 1)));
        let ControlBody::GapOpen {
            tokens,
            start,
            reason: GapReason::ConnectionClosed { .. },
        } = body
        else {
            panic!("expected a connection_closed gap, got {body:?}");
        };
        assert_eq!(tokens, tokens_of(&[1, 2]));
        assert_eq!(start, last_message_at);
        assert!(matches!(
            h.control().await.1,
            ControlBody::Disconnect { .. }
        ));

        // The reconnect subscribes every assigned token and closes the gap.
        let mut conn = h.server.accept().await;
        assert_eq!(asset_ids(&conn.recv_json().await), tokens_of(&[1, 2]));
        assert_eq!(
            h.control().await,
            (
                Some(attempt(1, 2)),
                ControlBody::Connect {
                    slot: slot(1),
                    token_count: 4,
                }
            )
        );
        let (_, body) = h.control().await;
        assert!(
            matches!(
                &body,
                ControlBody::GapClose { tokens, reason: GapReason::Resumed, .. }
                    if *tokens == tokens_of(&[1, 2])
            ),
            "{body:?}"
        );
        assert!(matches!(h.control().await.1, ControlBody::Subscribe { .. }));

        h.stop().await;
        conn.closed().await;
    }

    #[tokio::test]
    async fn refused_connections_are_one_gap() {
        let mut h = Harness::start().await;
        h.server.refuse(true);
        h.add(&[1]).await;

        let (_, body) = h.control().await;
        assert!(
            matches!(
                body,
                ControlBody::GapOpen {
                    reason: GapReason::ConnectFailed { .. },
                    ..
                }
            ),
            "{body:?}"
        );
        // Several attempts fail meanwhile.
        time::sleep(Duration::from_millis(400)).await;
        h.server.refuse(false);

        h.server.accept().await;
        let (conn_id, body) = h.control().await;
        assert!(matches!(body, ControlBody::Connect { .. }), "{body:?}");
        assert_ne!(conn_id, Some(attempt(1, 1)));
        assert!(matches!(
            h.control().await.1,
            ControlBody::GapClose {
                reason: GapReason::Resumed,
                ..
            }
        ));
        assert!(matches!(h.control().await.1, ControlBody::Subscribe { .. }));
        h.stop().await;
    }

    #[tokio::test]
    async fn gaps_open_at_shutdown_are_closed() {
        let mut h = Harness::start().await;
        h.server.refuse(true);
        h.add(&[1]).await;
        assert!(matches!(h.control().await.1, ControlBody::GapOpen { .. }));

        h.stop().await;
        // Never connected, so no disconnect.
        assert!(matches!(
            h.control().await.1,
            ControlBody::GapClose {
                reason: GapReason::Shutdown,
                ..
            }
        ));
        assert!(h.records.recv().await.is_none());
    }

    #[tokio::test]
    async fn removed_markets_are_unsubscribed_and_an_empty_connection_closes() {
        let mut h = Harness::start().await;
        let mut conn = h.streaming(&[1, 2]).await;

        h.remove(&[1]).await;
        assert_eq!(
            conn.recv_json().await,
            json!({
                "operation": "unsubscribe",
                "assets_ids": ["1-no", "1-yes"],
            })
        );
        assert_eq!(
            h.control().await.1,
            ControlBody::Unsubscribe {
                slot: slot(1),
                tokens: tokens_of(&[1]),
            }
        );

        h.remove(&[2]).await;
        assert_eq!(asset_ids(&conn.recv_json().await), tokens_of(&[2]));
        assert!(matches!(
            h.control().await.1,
            ControlBody::Unsubscribe { .. }
        ));
        assert_eq!(
            h.control().await.1,
            ControlBody::Disconnect {
                slot: slot(1),
                reason: "no tokens".into(),
            }
        );
        conn.closed().await;

        // The next market opens a new slot.
        h.add(&[3]).await;
        h.server.accept().await;
        assert_eq!(
            h.control().await,
            (
                Some(attempt(2, 1)),
                ControlBody::Connect {
                    slot: slot(2),
                    token_count: 2,
                }
            )
        );
        h.stop().await;
    }

    #[tokio::test]
    async fn markets_are_spread_across_connections() {
        let mut h = Harness::with(|c| c.tokens_per_connection = 4).await;
        h.add(&[1, 2, 3]).await;

        let mut subscribed = vec![];
        for _ in 0..2 {
            let mut conn = h.server.accept().await;
            subscribed.push(asset_ids(&conn.recv_json().await));
        }
        subscribed.sort_by_key(HashSet::len);
        assert_eq!(subscribed, vec![tokens_of(&[3]), tokens_of(&[1, 2])]);
        h.stop().await;
    }

    #[tokio::test]
    async fn full_write_queue_drops_frames_and_reconnects() {
        let mut h = Harness::with(|c| c.write_queue_capacity = 4).await;
        let conn = h.streaming(&[1]).await;

        for i in 0..10 {
            conn.send(&format!("frame {i}"));
        }
        // Let the socket fill the queue while nothing reads it.
        time::sleep(Duration::from_millis(300)).await;
        for i in 0..4 {
            assert_eq!(
                h.frame().await.1,
                MarketEvent::Unknown(json!(format!("frame {i}")))
            );
        }
        assert!(matches!(
            h.control().await.1,
            ControlBody::GapOpen {
                reason: GapReason::QueueOverflow,
                ..
            }
        ));
        assert!(matches!(
            h.control().await.1,
            ControlBody::Disconnect { .. }
        ));

        h.server.accept().await;
        let mut dropped = None;
        let mut reconnected = false;
        while dropped.is_none() || !reconnected {
            match h.control().await {
                (
                    Some(conn_id),
                    ControlBody::QueueOverflow {
                        slot: s,
                        dropped: n,
                    },
                ) => {
                    assert_eq!((conn_id, s), (attempt(1, 1), slot(1)));
                    dropped = Some(n);
                }
                (_, ControlBody::Connect { .. }) => reconnected = true,
                _ => {}
            }
        }
        assert_eq!(dropped, Some(6));
        h.stop().await;
    }
}
