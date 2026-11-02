use std::collections::HashSet;

use marcasite::{types::TokenId, ws::MarketEvent};
use tokio::sync::mpsc::{self, error::TrySendError};

use crate::messages::{ConnectionManagerMsg, Record, RegistryMsg};

/// Looks inside records after they are written (section 5.7): full books go to the connection
/// manager as `BooksSeen`, and `market_resolved` / `new_market` messages to the registry. It
/// never waits: the writer must not be held up by either task.
pub struct FrameInspector {
    conn_mgr: mpsc::Sender<ConnectionManagerMsg>,
    registry: mpsc::UnboundedSender<RegistryMsg>,
    /// Tag slugs the registry tracks. A new market with tags, none of them these, cannot
    /// qualify, so it does not trigger an early registry refresh.
    category_tags: HashSet<String>,
    books_seen: HashSet<TokenId>,
}

impl FrameInspector {
    pub fn new(
        conn_mgr: mpsc::Sender<ConnectionManagerMsg>,
        registry: mpsc::UnboundedSender<RegistryMsg>,
        category_tags: &[String],
    ) -> Self {
        Self {
            conn_mgr,
            registry,
            category_tags: category_tags.iter().map(|t| t.to_lowercase()).collect(),
            books_seen: HashSet::new(),
        }
    }

    pub fn inspect(&mut self, record: &Record) {
        match record {
            Record::WsFrame { recv_ts, event, .. } => match event {
                MarketEvent::Book(book) => {
                    self.books_seen.insert(book.asset_id.clone());
                }
                MarketEvent::MarketResolved(resolved) => {
                    tracing::info!(condition_id = %resolved.market, "market resolved on the websocket");
                    let _ = self.registry.send(RegistryMsg::MarketResolved {
                        condition_id: resolved.market.clone(),
                        at: *recv_ts,
                    });
                }
                MarketEvent::NewMarket(new) => {
                    let relevant = new.tags.as_ref().is_none_or(|tags| {
                        tags.iter()
                            .any(|t| self.category_tags.contains(&t.to_lowercase()))
                    });
                    if relevant {
                        let _ = self.registry.send(RegistryMsg::NewMarketSeen {
                            condition_id: new.market.clone(),
                        });
                    }
                }
                _ => {}
            },
            Record::Snapshot { books, .. } => {
                self.books_seen.extend(
                    books
                        .iter()
                        .filter_map(|book| book.get("asset_id")?.as_str())
                        .map(TokenId::new),
                );
            }
            Record::Market { .. } | Record::Control { .. } => {}
        }
    }

    /// Sends the books seen since the last call. If the connection manager's inbox is full they
    /// are kept for the next call.
    pub fn send_books_seen(&mut self) {
        if self.books_seen.is_empty() {
            return;
        }
        let tokens = std::mem::take(&mut self.books_seen);
        match self
            .conn_mgr
            .try_send(ConnectionManagerMsg::BooksSeen { tokens })
        {
            Ok(()) => {}
            Err(TrySendError::Full(ConnectionManagerMsg::BooksSeen { tokens })) => {
                self.books_seen = tokens;
            }
            // Full only ever hands back what was sent; the manager has gone (shutdown).
            Err(_) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use serde_json::{Value, json};

    use super::*;
    use crate::{connection_manager::types::ConnAttemptId, messages::SnapshotReason};

    fn frame(json: Value) -> Record {
        Record::WsFrame {
            recv_ts: Utc::now(),
            conn_id: ConnAttemptId::new("c-0001-1"),
            event: serde_json::from_value(json).unwrap(),
        }
    }

    fn book(token: &str) -> Value {
        json!({"event_type":"book","asset_id":token,"market":"0xabc","bids":[],"asks":[],
            "timestamp":"1790348400120","hash":"h"})
    }

    fn new_market(tags: Value) -> Value {
        json!({"event_type":"new_market","id":"1","question":"q","market":"0xdef","slug":"q",
            "description":null,"assets_ids":["3","4"],"outcomes":["Yes","No"],
            "event_message":null,"timestamp":"1","tags":tags})
    }

    struct Harness {
        inspector: FrameInspector,
        conn_mgr: mpsc::Receiver<ConnectionManagerMsg>,
        registry: mpsc::UnboundedReceiver<RegistryMsg>,
    }

    fn harness(capacity: usize) -> Harness {
        let (conn_tx, conn_mgr) = mpsc::channel(capacity);
        let (reg_tx, registry) = mpsc::unbounded_channel();
        Harness {
            inspector: FrameInspector::new(conn_tx, reg_tx, &["politics".to_string()]),
            conn_mgr,
            registry,
        }
    }

    fn books_seen(msg: ConnectionManagerMsg) -> HashSet<TokenId> {
        match msg {
            ConnectionManagerMsg::BooksSeen { tokens } => tokens,
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn books_from_frames_and_snapshots_are_batched() {
        let mut h = harness(4);
        h.inspector.inspect(&frame(book("1")));
        h.inspector.inspect(&frame(book("1")));
        let summary = json!({
            "market":"0xabc","asset_id":"2","timestamp":"1","hash":"h","bids":[],"asks":[],
            "min_order_size":"5","tick_size":"0.01","neg_risk":false,"last_trade_price":""
        });
        h.inspector.inspect(&Record::Snapshot {
            recv_ts: Utc::now(),
            requested_tokens: vec!["2".into(), "9".into()],
            reason: SnapshotReason::Scheduled,
            books: vec![summary],
        });
        h.inspector.send_books_seen();
        assert_eq!(
            books_seen(h.conn_mgr.try_recv().unwrap()),
            HashSet::from(["1".into(), "2".into()])
        );
        h.inspector.send_books_seen();
        assert!(h.conn_mgr.try_recv().is_err(), "nothing new to send");
    }

    #[test]
    fn a_full_inbox_keeps_books_for_later() {
        let mut h = harness(1);
        h.inspector.inspect(&frame(book("1")));
        h.inspector.send_books_seen();
        h.inspector.inspect(&frame(book("2")));
        h.inspector.send_books_seen();
        assert_eq!(
            books_seen(h.conn_mgr.try_recv().unwrap()),
            HashSet::from(["1".into()])
        );
        h.inspector.send_books_seen();
        assert_eq!(
            books_seen(h.conn_mgr.try_recv().unwrap()),
            HashSet::from(["2".into()])
        );
    }

    #[test]
    fn resolutions_and_relevant_new_markets_go_to_the_registry() {
        let mut h = harness(4);
        h.inspector
            .inspect(&frame(json!({"event_type":"market_resolved","id":"1",
            "market":"0xdef","assets_ids":["3","4"],"winning_asset_id":"3",
            "winning_outcome":"Yes","event_message":null,"timestamp":"1","tags":null})));
        h.inspector.inspect(&frame(new_market(json!(["Politics"]))));
        h.inspector.inspect(&frame(new_market(json!(["sports"]))));
        h.inspector.inspect(&frame(new_market(Value::Null)));
        assert!(matches!(
            h.registry.try_recv().unwrap(),
            RegistryMsg::MarketResolved { condition_id, .. } if condition_id == "0xdef"
        ));
        assert!(matches!(
            h.registry.try_recv().unwrap(),
            RegistryMsg::NewMarketSeen { .. }
        ));
        assert!(
            matches!(
                h.registry.try_recv().unwrap(),
                RegistryMsg::NewMarketSeen { .. }
            ),
            "a new market without tags may qualify"
        );
        assert!(h.registry.try_recv().is_err(), "sports market skipped");
    }
}
