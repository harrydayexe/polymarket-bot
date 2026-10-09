use std::{sync::Arc, time::Duration};

use tokio::{sync::mpsc, time};
use tokio_util::sync::CancellationToken;

use crate::{
    client::APIClient,
    clock::SharedClock,
    config::Config,
    market_registry::{fetch_from_gamma::fetch_markets_from_gamma, registry::Registry},
    messages::{ConnectionManagerMsg, RegistryMsg},
};

pub async fn registry_task(
    mut inbox: mpsc::Receiver<RegistryMsg>,
    conn_mgr: mpsc::Sender<ConnectionManagerMsg>,
    token: CancellationToken,
    client: Arc<impl APIClient>,
    config: Arc<Config>,
    clock: SharedClock,
) {
    let mut registry = Registry::new(config.clone());
    let mut tick = time::interval(Duration::from_secs(config.registry_interval_s));

    loop {
        tokio::select! {
            _ = token.cancelled() => {
                break;
            }
            _ = tick.tick() => {
                match fetch_markets_from_gamma(client.clone(), &mut registry, config.clone(), clock.clone(), token.clone()).await {
                    Err(e) => {
                        tracing::warn!(error = format!("{e:#}"), "registry update failed");
                    }
                    Ok(Some(changes)) => {
                        if conn_mgr.send(ConnectionManagerMsg::TokenChanges(changes)).await.is_err() {
                            tracing::error!("connection manager gone, registry exiting");
                            break;
                        }
                    }
                    _ => { /* cancelled */ }
                }
            }
            Some(msg) = inbox.recv() => match msg {
                RegistryMsg::MarketResolved(id) => {
                    registry.resolve_from_websocket(&id, clock.now());
                }
                RegistryMsg::NewMarketSeen => {
                    tracing::debug!("early update of registry triggered");
                    if let Err(e) = fetch_markets_from_gamma(client.clone(), &mut registry, config.clone(), clock.clone(), token.clone()) .await {
                        tracing::warn!(error = format!("{e:#}"), "registry update failed");
                    }
                }
            },
            else => break,
        }
    }
}
