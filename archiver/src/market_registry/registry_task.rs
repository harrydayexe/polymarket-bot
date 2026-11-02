use anyhow::Result;
use chrono::{DateTime, Utc};
use std::sync::Arc;

use tokio::{
    sync::mpsc,
    time::{self, Interval, MissedTickBehavior},
};
use tokio_util::sync::CancellationToken;

use crate::{
    client::APIClient,
    clock::SharedClock,
    config::Config,
    market_registry::{
        cycle::{CycleError, plan_cycle},
        registry::{AppliedChanges, Registry},
        token_changes::TokenChanges,
    },
    messages::{ConnectionManagerMsg, ControlBody, Record, RegistryMsg},
};

/// Keep the set of tracked markets up to date until `token` is cancelled.
///
/// # Errors
///
/// Only if the connection manager or write queue has gone away. Gamma failures are logged and the
/// current set is kept.
pub async fn registry_task(
    inbox: mpsc::UnboundedReceiver<RegistryMsg>,
    conn_mgr: mpsc::Sender<ConnectionManagerMsg>,
    write_queue: mpsc::Sender<Record>,
    token: CancellationToken,
    client: Arc<impl APIClient>,
    config: Arc<Config>,
    clock: SharedClock,
) -> Result<()> {
    let mut task = RegistryTask {
        registry: Registry::new(config.clone()),
        client,
        conn_mgr,
        write_queue,
        config,
        clock,
    };
    task.run(inbox, token).await
}

struct RegistryTask<C> {
    registry: Registry,
    client: Arc<C>,
    conn_mgr: mpsc::Sender<ConnectionManagerMsg>,
    write_queue: mpsc::Sender<Record>,
    config: Arc<Config>,
    clock: SharedClock,
}

impl<C: APIClient> RegistryTask<C> {
    async fn run(
        &mut self,
        mut inbox: mpsc::UnboundedReceiver<RegistryMsg>,
        token: CancellationToken,
    ) -> Result<()> {
        let mut tick = time::interval(self.config.registry_interval);
        tick.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            // Inbox before tick, so a burst of new-market notices leads to a single refresh.
            tokio::select! {
                biased;

                _ = token.cancelled() => return Ok(()),

                Some(msg) = inbox.recv() => self.handle(msg, &mut tick),

                _ = tick.tick() => tokio::select! {
                    _ = token.cancelled() => return Ok(()),
                    r = self.refresh() => r?,
                },
            }
        }
    }

    fn handle(&mut self, msg: RegistryMsg, tick: &mut Interval) {
        match msg {
            RegistryMsg::MarketResolved { condition_id, at } => {
                self.registry.resolve_from_websocket(&condition_id, at);
            }
            RegistryMsg::NewMarketSeen { condition_id } => {
                tracing::debug!(%condition_id, "new market seen, refreshing registry early");
                tick.reset_immediately();
            }
        }
    }

    /// Run one registry cycle.
    async fn refresh(&mut self) -> Result<()> {
        let now = self.clock.now();

        let expired = self.registry.expire(now);
        self.publish(
            AppliedChanges {
                tokens: expired,
                ..AppliedChanges::default()
            },
            now,
        )
        .await?;

        let limit = self.config.registry_interval;
        let planned = time::timeout(limit, plan_cycle(&*self.client, &self.registry, now))
            .await
            .unwrap_or(Err(CycleError::Timeout(limit)));

        match planned {
            Ok(plan) => {
                let applied = self.registry.apply_plan(plan, now);
                log_changes(&applied.tokens);
                self.publish(applied, now).await
            }
            Err(CycleError::Rejected(e)) => {
                tracing::warn!(error = %e, "Gamma response rejected, keeping current markets");
                self.write(Record::Control {
                    recv_ts: now,
                    conn_id: None,
                    body: ControlBody::RegistryRejected {
                        reason: e.to_string(),
                        fetched: e.fetched_count,
                        previous: e.last_accepted_count,
                    },
                })
                .await
            }
            Err(e) => {
                tracing::warn!(error = %e, "registry update failed, keeping current markets");
                Ok(())
            }
        }
    }

    /// Write market records, then tell the connection manager, so no token is subscribed before
    /// its details are saved.
    async fn publish(&self, applied: AppliedChanges, at: DateTime<Utc>) -> Result<()> {
        for (change, market) in applied.records {
            self.write(Record::Market {
                recv_ts: at,
                market,
                change,
            })
            .await?;
        }

        if !applied.tokens.is_empty()
            && self
                .conn_mgr
                .send(ConnectionManagerMsg::TokenChanges(applied.tokens))
                .await
                .is_err()
        {
            anyhow::bail!("connection manager gone, registry exiting");
        }

        Ok(())
    }

    async fn write(&self, record: Record) -> Result<()> {
        if self.write_queue.send(record).await.is_err() {
            anyhow::bail!("write queue gone, registry exiting");
        }
        Ok(())
    }
}

fn log_changes(tokens: &TokenChanges) {
    if tokens.is_empty() {
        tracing::debug!("registry updated, no changes");
    } else {
        tracing::info!(
            added = tokens.added.len(),
            removed = tokens.removed.len(),
            "registry updated"
        );
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Mutex,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use async_trait::async_trait;
    use chrono::TimeZone;
    use marcasite::{gamma::Market, types::ConditionId};
    use serde_json::json;
    use tokio::task::JoinHandle;

    use super::*;
    use crate::{
        clock::TokioClock,
        market_registry::{market_info::tests::gamma_market, registry::MarketChange},
    };

    /// A Gamma stand-in. `open` is the listing to return, or `None` to fail.
    #[derive(Default)]
    struct FakeGamma {
        open: Mutex<Option<Vec<Market>>>,
        listings: AtomicUsize,
    }

    impl FakeGamma {
        fn set_open(&self, markets: Option<Vec<Market>>) {
            *self.open.lock().unwrap() = markets;
        }
    }

    #[async_trait]
    impl APIClient for FakeGamma {
        async fn fetch_open_markets(&self) -> Result<Vec<Market>> {
            self.listings.fetch_add(1, Ordering::SeqCst);
            self.open
                .lock()
                .unwrap()
                .clone()
                .ok_or_else(|| anyhow::anyhow!("Gamma is down"))
        }

        async fn fetch_markets(&self, _ids: &[ConditionId]) -> Result<Vec<Market>> {
            Ok(Vec::new())
        }
    }

    struct Harness {
        gamma: Arc<FakeGamma>,
        inbox: mpsc::UnboundedSender<RegistryMsg>,
        conn_mgr: mpsc::Receiver<ConnectionManagerMsg>,
        writes: mpsc::Receiver<Record>,
        token: CancellationToken,
        task: JoinHandle<Result<()>>,
    }

    /// Start the registry task with `open` as the first Gamma listing.
    fn start(open: Option<Vec<Market>>) -> Harness {
        let gamma = Arc::new(FakeGamma::default());
        gamma.set_open(open);
        let (inbox, inbox_rx) = mpsc::unbounded_channel();
        let (conn_tx, conn_mgr) = mpsc::channel(16);
        let (write_tx, writes) = mpsc::channel(16);
        let token = CancellationToken::new();
        let clock = Arc::new(TokioClock::new(
            Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
        ));

        let task = tokio::spawn(registry_task(
            inbox_rx,
            conn_tx,
            write_tx,
            token.clone(),
            gamma.clone(),
            Arc::new(Config::default()),
            clock,
        ));

        Harness {
            gamma,
            inbox,
            conn_mgr,
            writes,
            token,
            task,
        }
    }

    impl Harness {
        async fn token_changes(&mut self) -> TokenChanges {
            match self.conn_mgr.recv().await {
                Some(ConnectionManagerMsg::TokenChanges(changes)) => changes,
                Some(other) => panic!("unexpected message: {other:?}"),
                None => panic!("registry task ended"),
            }
        }

        /// Let the task run until it is idle, and check it sent no token changes.
        async fn assert_no_token_changes(&mut self) {
            let next = time::timeout(Duration::from_secs(1), self.conn_mgr.recv()).await;
            assert!(next.is_err(), "unexpected message: {next:?}");
        }

        async fn next_cycle(&self) {
            time::advance(Config::default().registry_interval).await;
        }
    }

    fn markets(ns: &[u8]) -> Option<Vec<Market>> {
        Some(ns.iter().map(|&n| gamma_market(n, json!({}))).collect())
    }

    #[tokio::test(start_paused = true)]
    async fn first_cycle_writes_records_and_subscribes() {
        let mut h = start(markets(&[1, 2]));

        let changes = h.token_changes().await;
        assert_eq!(changes.added.len(), 2);

        for _ in 0..2 {
            let record = h.writes.try_recv().expect("market record written first");
            assert!(matches!(
                record,
                Record::Market {
                    change: MarketChange::Added,
                    ..
                }
            ));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn gamma_error_keeps_the_current_set() {
        let mut h = start(markets(&[1]));
        h.token_changes().await;

        h.gamma.set_open(None);
        h.next_cycle().await;

        h.assert_no_token_changes().await;
        assert_eq!(h.gamma.listings.load(Ordering::SeqCst), 2);
        assert!(!h.task.is_finished());
    }

    #[tokio::test(start_paused = true)]
    async fn empty_response_writes_registry_rejected() {
        let mut h = start(markets(&[1]));
        h.token_changes().await;
        while h.writes.try_recv().is_ok() {}

        h.gamma.set_open(Some(Vec::new()));
        h.next_cycle().await;

        h.assert_no_token_changes().await;
        let record = h.writes.try_recv().expect("rejection recorded");
        assert!(matches!(
            record,
            Record::Control {
                body: ControlBody::RegistryRejected {
                    fetched: 0,
                    previous: 1,
                    ..
                },
                ..
            }
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn burst_of_new_market_notices_triggers_one_refresh() {
        let mut h = start(markets(&[1]));
        h.token_changes().await;

        h.gamma.set_open(markets(&[1, 2]));
        let condition_id = gamma_market(2, json!({})).condition_id.unwrap();
        for _ in 0..5 {
            h.inbox
                .send(RegistryMsg::NewMarketSeen {
                    condition_id: condition_id.clone(),
                })
                .unwrap();
        }

        let changes = h.token_changes().await;
        assert!(changes.added.contains_key(&condition_id));
        h.assert_no_token_changes().await;
        assert_eq!(h.gamma.listings.load(Ordering::SeqCst), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn resolved_market_is_unsubscribed_after_grace_even_if_gamma_is_down() {
        let mut h = start(markets(&[1]));
        h.token_changes().await;

        h.gamma.set_open(None);
        let condition_id = gamma_market(1, json!({})).condition_id.unwrap();
        h.inbox
            .send(RegistryMsg::MarketResolved {
                condition_id: condition_id.clone(),
                at: Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
            })
            .unwrap();
        time::advance(Duration::from_hours(6)).await;

        let changes = h.token_changes().await;
        assert!(changes.removed.contains_key(&condition_id));
    }

    #[tokio::test(start_paused = true)]
    async fn cancellation_stops_the_task_cleanly() {
        let mut h = start(markets(&[1]));
        h.token_changes().await;

        h.token.cancel();

        assert!(h.task.await.unwrap().is_ok());
    }

    #[tokio::test(start_paused = true)]
    async fn connection_manager_gone_is_fatal() {
        let h = start(markets(&[1]));
        drop(h.conn_mgr);

        let result = h.task.await.unwrap();

        assert!(result.is_err());
    }
}
