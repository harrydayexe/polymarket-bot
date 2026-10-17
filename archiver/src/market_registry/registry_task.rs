use anyhow::{Context, Result};
use std::sync::Arc;

use tokio::{sync::mpsc, time};
use tokio_util::sync::CancellationToken;

use crate::{
    client::APIClient,
    clock::SharedClock,
    config::Config,
    market_registry::{registry::Registry, update_from_gamma::update_from_gamma},
    messages::{ConnectionManagerMsg, Record, RegistryMsg},
};

pub async fn registry_task(
    mut inbox: mpsc::Receiver<RegistryMsg>,
    conn_mgr: mpsc::Sender<ConnectionManagerMsg>,
    write_queue: mpsc::Sender<Record>,
    token: CancellationToken,
    client: Arc<impl APIClient>,
    config: Arc<Config>,
    clock: SharedClock,
) -> Result<()> {
    let mut registry = Registry::new(config.clone());
    let mut tick = time::interval(config.registry_interval);

    loop {
        tokio::select! {
            _ = token.cancelled() => return Ok(()),

            _ = tick.tick() => update_from_gamma(
                    client.clone(),
                    &mut registry,
                    conn_mgr.clone(),
                    write_queue.clone(),
                    config.clone(),
                    clock.clone(),
                    token.clone()
                ).await.context("failed to update registry from gamma")?,

            Some(msg) = inbox.recv() => match msg {
                RegistryMsg::MarketResolved(id) => {
                    registry.resolve_from_websocket(&id, clock.now());
                }
                RegistryMsg::NewMarketSeen => {
                    tracing::debug!("early update of registry triggered");
                    update_from_gamma(
                        client.clone(),
                        &mut registry,
                        conn_mgr.clone(),
                        write_queue.clone(),
                        config.clone(),
                        clock.clone(),
                        token.clone()
                    ).await.context("failed to update registry out of band from gamma")?;
                }
            },

            else => return Ok(()),
        }
    }
}
