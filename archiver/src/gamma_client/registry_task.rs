// use std::{sync::Arc, time::Duration};
//
// use tokio::{sync::mpsc, time::MissedTickBehavior};
// use tokio_util::sync::CancellationToken;
//
// use crate::{
//     clock::Clock,
//     config::Config,
//     gamma_client::client::GammaClient,
//     market_registry::registry::Registry,
//     types::{ConditionId, UtcMicros},
// };
//
// pub enum RegistryMsg {
//     MarketResolved {
//         condition_id: ConditionId,
//         at: UtcMicros,
//     },
//     NewMarketSeen {
//         condition_id: ConditionId,
//         at: UtcMicros,
//     },
// }
//
// pub struct RegistryTask<G: GammaClient, C: Clock> {
//     registry: Registry,
//     gamma: G,
//     clock: C,
//     config: Arc<Config>,
//     shutdown: CancellationToken,
//     inbox: mpsc::Receiver<RegistryMsg>,
// }
//
// impl<G: GammaClient, C: Clock> RegistryTask<G, C> {
//     pub async fn run(mut self) {
//         let mut tick = tokio::time::interval(Duration::from_secs(self.config.registry_interval_s));
//         tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
//         loop {
//             tokio::select! {
//                 _ = self.shutdown.cancelled() => break,
//                 // Some(msg) = self.inbox.recv() => self.pending_notices.push(msg.into()),
//                 _ = tick.tick() => self.refresh().await,
//             }
//         }
//     }
//
//     async fn refresh(&mut self) {
//         let sel = match fetch_and_select(&self.gamma, &self.cfg).await {
//             Ok(s) => s,
//             Err(e) => {
//                 tracing::warn!(?e, "gamma fetch failed");
//                 return;
//             }
//         };
//         if let Err(r) = sanity_check(sel.selected.len(), self.registry.last_accepted_count) {
//             tracing::warn!(?r, "rejected gamma response");
//             return;
//         }
//         let plan = plan_changes(
//             &self.registry,
//             &sel.selected,
//             &self.pending_notices,
//             self.clock.wall_now(),
//         );
//         self.registry = plan.new_registry;
//         self.pending_notices.clear();
//         // send market records + TokenChanges
//     }
// }
