use std::sync::Arc;

use anyhow::Result;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::{
    client::APIClient,
    clock::SharedClock,
    config::Config,
    market_registry::{registry::Registry, token_changes::TokenChanges},
    messages::{ConnectionManagerMsg, Record},
};

pub(super) async fn update_from_gamma(
    client: Arc<impl APIClient>,
    registry: &mut Registry,
    conn_mgr: mpsc::Sender<ConnectionManagerMsg>,
    write_queue: mpsc::Sender<Record>,
    config: Arc<Config>,
    clock: SharedClock,
    token: CancellationToken,
) -> Result<()> {
    let at = clock.now();

    tokio::select! {
        _ = token.cancelled() => Ok(()),
        r = async {
            let results = match client.fetch_page(&config.category_tags).await {
                Err(e) => {
                    tracing::warn!(error = format!("{e:#}"), "accessing gamma endpoint failed");
                    return Ok(());
                }
                Ok(r) => r,
            };

            let plan = match registry.plan_changes(results, at).await {
                Err(e) => {
                    tracing::warn!(error = format!("{e:#}"), "planning registry changes failed");
                    return Ok(());
                }
                Ok(p) => p,
            };

            let changes = registry.apply_plan(&plan, at);
            tracing::debug!("registry updated");

            if conn_mgr.send(ConnectionManagerMsg::TokenChanges(changes)).await.is_err() {
                return Err(anyhow::anyhow!("connection manager gone, registry exiting"));
            }

            if write_queue.send(Record::Market { recv_ts: at, changes: plan }).await.is_err() {
                return Err(anyhow::anyhow!("write queue gone, registry exiting"));
            }

            tracing::info!("registry updated and logged");

            Ok(())
        } => r,
    }
}
