//! The line written for each record (section 8.1, as changed by the use of marcasite).
//!
//! Every line is one JSON object with the common envelope fields:
//!
//! | Field | Meaning |
//! | --- | --- |
//! | `recv_ts` | Receive time, microseconds since the epoch, UTC |
//! | `seq` | Per-process counter, one per record written |
//! | `run_id` | The process run |
//! | `conn_id` | Connection attempt, or `null` |
//! | `source` | `ws`, `snapshot`, `control` or `market` |
//!
//! and then, by source:
//!
//! - `ws`: `event`, the market channel message as re-encoded by marcasite (one per array
//!   element; unrecognised messages kept as their JSON).
//! - `snapshot`: `reason`, `requested_tokens` and `books` (marcasite's `OrderBookSummary`s).
//! - `control`: `kind` and the fields of [ControlBody].
//! - `market`: `change` (`added` or `updated`) and `market` ([MarketInfo]).

use chrono::{DateTime, Utc, serde::ts_microseconds};
use marcasite::{clob::OrderBookSummary, types::TokenId, ws::MarketEvent};
use serde::{Deserialize, Serialize};

use crate::{
    archive::layout::Stream,
    connection_manager::types::ConnAttemptId,
    market_registry::{market_info::MarketInfo, registry::MarketChange},
    messages::{ControlBody, Record, SnapshotReason},
    types::RunId,
};

impl Record {
    pub fn recv_ts(&self) -> DateTime<Utc> {
        match self {
            Record::WsFrame { recv_ts, .. }
            | Record::Snapshot { recv_ts, .. }
            | Record::Market { recv_ts, .. }
            | Record::Control { recv_ts, .. } => *recv_ts,
        }
    }

    pub fn stream(&self) -> Stream {
        match self {
            Record::WsFrame { .. } => Stream::Ws,
            Record::Snapshot { .. } => Stream::Snapshots,
            Record::Market { .. } => Stream::Markets,
            Record::Control { .. } => Stream::Control,
        }
    }
}

#[derive(Serialize)]
struct Line<'a> {
    #[serde(with = "ts_microseconds")]
    recv_ts: DateTime<Utc>,
    seq: u64,
    run_id: &'a RunId,
    conn_id: Option<&'a ConnAttemptId>,
    #[serde(flatten)]
    body: Body<'a>,
}

#[derive(Serialize)]
#[serde(tag = "source", rename_all = "snake_case")]
enum Body<'a> {
    Ws {
        event: &'a MarketEvent,
    },
    Snapshot {
        reason: &'a SnapshotReason,
        requested_tokens: &'a [TokenId],
        books: &'a [OrderBookSummary],
    },
    Control(&'a ControlBody),
    Market {
        change: MarketChange,
        market: &'a MarketInfo,
    },
}

/// Appends `record` to `out` as one line, newline included.
///
/// # Errors
///
/// If the record cannot be serialised; `out` is then unchanged.
pub fn encode(
    record: &Record,
    seq: u64,
    run_id: &RunId,
    out: &mut Vec<u8>,
) -> serde_json::Result<()> {
    let (conn_id, body) = match record {
        Record::WsFrame { conn_id, event, .. } => (Some(conn_id), Body::Ws { event }),
        Record::Snapshot {
            requested_tokens,
            reason,
            books,
            ..
        } => (
            None,
            Body::Snapshot {
                reason,
                requested_tokens,
                books,
            },
        ),
        Record::Market { market, change, .. } => (
            None,
            Body::Market {
                change: *change,
                market,
            },
        ),
        Record::Control { conn_id, body, .. } => (conn_id.as_ref(), Body::Control(body)),
    };
    let line = Line {
        recv_ts: record.recv_ts(),
        seq,
        run_id,
        conn_id,
        body,
    };
    let len = out.len();
    if let Err(e) = serde_json::to_writer(&mut *out, &line) {
        out.truncate(len);
        return Err(e);
    }
    out.push(b'\n');
    Ok(())
}

/// A record read back from the archive.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct StoredRecord {
    #[serde(with = "ts_microseconds")]
    pub recv_ts: DateTime<Utc>,
    pub seq: u64,
    pub run_id: RunId,
    pub conn_id: Option<ConnAttemptId>,
    #[serde(flatten)]
    pub body: StoredBody,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(tag = "source", rename_all = "snake_case")]
pub enum StoredBody {
    Ws {
        event: MarketEvent,
    },
    Snapshot {
        reason: SnapshotReason,
        requested_tokens: Vec<TokenId>,
        books: Vec<OrderBookSummary>,
    },
    Control(ControlBody),
    Market {
        change: MarketChange,
        market: MarketInfo,
    },
}

/// Parses one line of a raw file.
pub fn decode(line: &str) -> serde_json::Result<StoredRecord> {
    serde_json::from_str(line)
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use chrono::TimeZone;
    use serde_json::{Value, json};

    use super::*;
    use crate::{
        connection_manager::types::ConnSlotId, market_registry::token_changes::MarketTokens,
        messages::GapReason,
    };

    fn ts() -> DateTime<Utc> {
        Utc.timestamp_micros(1_790_348_400_123_456).unwrap()
    }

    fn run() -> RunId {
        RunId::new("r-20260925-1400-a1b2")
    }

    fn line(record: &Record, seq: u64) -> String {
        let mut out = vec![];
        encode(record, seq, &run(), &mut out).unwrap();
        let text = String::from_utf8(out).unwrap();
        assert!(text.ends_with('\n'));
        assert_eq!(text.matches('\n').count(), 1, "one line per record");
        text
    }

    fn round_trip(record: &Record) -> StoredRecord {
        let text = line(record, 7);
        let stored = decode(text.trim_end()).unwrap();
        assert_eq!(stored.recv_ts, ts());
        assert_eq!(stored.seq, 7);
        assert_eq!(stored.run_id, run());
        stored
    }

    fn event(json: Value) -> MarketEvent {
        serde_json::from_value(json).unwrap()
    }

    /// One example of every market channel message, shaped like the live stream.
    pub(crate) fn sample_events() -> Vec<Value> {
        vec![
            json!({"event_type":"book","asset_id":"111","market":"0xabc",
                "bids":[{"price":"0.48","size":"30.5"},{"price":"0.49","size":"20"}],
                "asks":[{"price":"0.52","size":"25"},{"price":"0.51","size":"219.217767"}],
                "timestamp":"1790348400120","hash":"0xh1","tick_size":"0.01",
                "last_trade_price":"0.50"}),
            json!({"event_type":"price_change","market":"0xabc","timestamp":"1790348400121",
                "price_changes":[
                    {"asset_id":"111","price":"0.001","size":"0","side":"BUY","hash":"h2",
                     "best_bid":"0.49","best_ask":"0.51"},
                    {"asset_id":"222","price":"0.5","size":"10","side":"SELL","hash":"h3",
                     "best_bid":"","best_ask":"0.5"}]}),
            json!({"event_type":"last_trade_price","asset_id":"111","market":"0xabc",
                "price":"0.5","size":"12","fee_rate_bps":"0","side":"BUY",
                "timestamp":"1790348400122","transaction_hash":"0xt"}),
            json!({"event_type":"tick_size_change","asset_id":"111","market":"0xabc",
                "old_tick_size":"0.01","new_tick_size":"0.001","timestamp":"1790348400123"}),
            json!({"event_type":"best_bid_ask","asset_id":"111","market":"0xabc",
                "best_bid":"0.49","best_ask":"","spread":"","timestamp":"1790348400124"}),
            json!({"event_type":"new_market","id":"1","question":"Q? \"quoted\" \\ é 🎉",
                "market":"0xdef","slug":"q","description":null,"assets_ids":["333","444"],
                "outcomes":["Yes","No"],"event_message":null,"timestamp":"1790348400125",
                "tags":["politics"]}),
            json!({"event_type":"market_resolved","id":"1","market":"0xdef",
                "assets_ids":["333","444"],"winning_asset_id":"333","winning_outcome":"Yes",
                "event_message":null,"timestamp":"1790348400126","tags":["politics"]}),
            json!({"event_type":"something_new","x":[1,2,{"y":"z"}]}),
            json!({"no_event_type":true}),
        ]
    }

    #[test]
    fn every_market_event_round_trips() {
        for json in sample_events() {
            let original = event(json.clone());
            let record = Record::WsFrame {
                recv_ts: ts(),
                conn_id: ConnAttemptId::new("c-0003-7"),
                event: original.clone(),
            };
            let stored = round_trip(&record);
            assert_eq!(stored.conn_id, Some(ConnAttemptId::new("c-0003-7")));
            assert_eq!(stored.body, StoredBody::Ws { event: original }, "{json}");
        }
    }

    #[test]
    fn envelope_fields_are_written() {
        let record = Record::WsFrame {
            recv_ts: ts(),
            conn_id: ConnAttemptId::new("c-0003-7"),
            event: event(sample_events()[0].clone()),
        };
        let value: Value = serde_json::from_str(&line(&record, 88213)).unwrap();
        assert_eq!(value["recv_ts"], json!(1_790_348_400_123_456_i64));
        assert_eq!(value["seq"], json!(88213));
        assert_eq!(value["run_id"], json!("r-20260925-1400-a1b2"));
        assert_eq!(value["conn_id"], json!("c-0003-7"));
        assert_eq!(value["source"], json!("ws"));
        // Prices and sizes stay exact decimal strings.
        assert_eq!(value["event"]["asks"][1]["size"], json!("219.217767"));
        assert_eq!(value["event"]["timestamp"], json!("1790348400120"));
    }

    #[test]
    fn unknown_messages_are_kept_unchanged() {
        for json in &sample_events()[7..] {
            let record = Record::WsFrame {
                recv_ts: ts(),
                conn_id: ConnAttemptId::new("c-0001-1"),
                event: event(json.clone()),
            };
            let value: Value = serde_json::from_str(&line(&record, 1)).unwrap();
            assert_eq!(&value["event"], json);
        }
    }

    #[test]
    fn snapshot_round_trips() {
        let book: OrderBookSummary = serde_json::from_value(json!({
            "market":"0xabc","asset_id":"111","timestamp":"1790348400120","hash":"abc",
            "bids":[{"price":"0.49","size":"20"}],"asks":[{"price":"0.51","size":"5.000001"}],
            "min_order_size":"5","tick_size":"0.01","neg_risk":false,"last_trade_price":"0.5"
        }))
        .unwrap();
        let record = Record::Snapshot {
            recv_ts: ts(),
            requested_tokens: vec![TokenId::new("111"), TokenId::new("222")],
            reason: SnapshotReason::MissingBook,
            books: vec![book.clone()],
        };
        let value: Value = serde_json::from_str(&line(&record, 1)).unwrap();
        assert_eq!(value["source"], json!("snapshot"));
        assert_eq!(value["reason"], json!("missing_book"));
        assert_eq!(value["conn_id"], Value::Null);
        assert_eq!(
            round_trip(&record).body,
            StoredBody::Snapshot {
                reason: SnapshotReason::MissingBook,
                requested_tokens: vec![TokenId::new("111"), TokenId::new("222")],
                books: vec![book],
            }
        );
    }

    #[test]
    fn market_round_trips() {
        let market = MarketInfo {
            condition_id: "0xabc".into(),
            question: "Will it?".into(),
            slug: Some("will-it".into()),
            event_id: None,
            tags: vec!["politics".into()],
            tokens: MarketTokens {
                yes: "111".into(),
                no: "222".into(),
            },
            start_date: Some(ts()),
            end_date: None,
            active: true,
            closed: false,
            accepting_orders: true,
            liquidity_usd: Some("1234.5".parse().unwrap()),
            tick_size: Some("0.01".parse().unwrap()),
            neg_risk: false,
            fetched_at: ts(),
        };
        let record = Record::Market {
            recv_ts: ts(),
            market: market.clone(),
            change: MarketChange::Added,
        };
        let value: Value = serde_json::from_str(&line(&record, 1)).unwrap();
        assert_eq!(value["source"], json!("market"));
        assert_eq!(value["change"], json!("added"));
        assert_eq!(value["market"]["tokens"], json!({"yes":"111","no":"222"}));
        assert_eq!(
            value["market"]["start_date"],
            json!(1_790_348_400_123_456_i64)
        );
        assert_eq!(
            round_trip(&record).body,
            StoredBody::Market {
                change: MarketChange::Added,
                market
            }
        );
    }

    fn tokens(ids: &[&str]) -> HashSet<TokenId> {
        ids.iter().map(|id| TokenId::new(*id)).collect()
    }

    #[test]
    fn every_control_body_round_trips() {
        let slot = ConnSlotId::new("c-0001");
        let bodies = vec![
            ControlBody::RunStart {
                program_version: "0.1.0".into(),
                config_hash: "abcd".into(),
                previous_run: Some(RunId::new("r-1")),
            },
            ControlBody::RunStop { clean: true },
            ControlBody::Connect {
                slot: slot.clone(),
                token_count: 2,
            },
            ControlBody::Disconnect {
                slot: slot.clone(),
                reason: "closed".into(),
            },
            ControlBody::Subscribe {
                slot: slot.clone(),
                tokens: tokens(&["2", "1"]),
            },
            ControlBody::Unsubscribe {
                slot: slot.clone(),
                tokens: tokens(&["1"]),
            },
            ControlBody::GapOpen {
                tokens: tokens(&["1", "2"]),
                start: ts(),
                reason: GapReason::ConnectFailed {
                    error: "refused".into(),
                },
            },
            ControlBody::GapOpen {
                tokens: tokens(&["1"]),
                start: ts(),
                reason: GapReason::HeartbeatTimeout,
            },
            ControlBody::GapClose {
                tokens: tokens(&["1"]),
                end: ts(),
                reason: GapReason::Resumed,
            },
            ControlBody::GapClose {
                tokens: tokens(&["1"]),
                end: ts(),
                reason: GapReason::ConnectionClosed {
                    error: "reset".into(),
                },
            },
            ControlBody::SnapshotFailed {
                tokens: tokens(&["1"]),
                error: "500".into(),
                attempts: 4,
            },
            ControlBody::QueueOverflow {
                slot: slot.clone(),
                dropped: 3,
            },
            ControlBody::RegistryRejected {
                reason: "too few".into(),
                fetched: 1,
                previous: 10,
            },
            ControlBody::TokenUnhealthy {
                token: "1".into(),
                since: ts(),
            },
            ControlBody::FrameDecodeFailed {
                error: "bad".into(),
            },
        ];
        for reason in [
            GapReason::QueueOverflow,
            GapReason::ProcessRestart,
            GapReason::WriteFailure,
            GapReason::Shutdown,
            GapReason::Unsubscribed,
        ] {
            let body = ControlBody::GapClose {
                tokens: tokens(&["1"]),
                end: ts(),
                reason: reason.clone(),
            };
            let record = Record::Control {
                recv_ts: ts(),
                conn_id: None,
                body: body.clone(),
            };
            assert_eq!(round_trip(&record).body, StoredBody::Control(body));
        }
        for body in bodies {
            let record = Record::Control {
                recv_ts: ts(),
                conn_id: Some(ConnAttemptId::new("c-0001-1")),
                body: body.clone(),
            };
            let stored = round_trip(&record);
            assert_eq!(stored.conn_id, Some(ConnAttemptId::new("c-0001-1")));
            assert_eq!(stored.body, StoredBody::Control(body));
        }
    }

    #[test]
    fn control_lines_have_a_kind_and_flat_fields() {
        let record = Record::Control {
            recv_ts: ts(),
            conn_id: None,
            body: ControlBody::GapOpen {
                tokens: tokens(&["2", "1", "10"]),
                start: ts(),
                reason: GapReason::ConnectFailed {
                    error: "refused".into(),
                },
            },
        };
        let value: Value = serde_json::from_str(&line(&record, 1)).unwrap();
        assert_eq!(
            value,
            json!({
                "recv_ts": 1_790_348_400_123_456_i64, "seq": 1,
                "run_id": "r-20260925-1400-a1b2", "conn_id": null, "source": "control",
                "kind": "gap_open", "tokens": ["1", "10", "2"],
                "start": 1_790_348_400_123_456_i64, "reason": "connect_failed",
                "detail": "refused"
            })
        );
    }

    #[test]
    fn truncated_lines_do_not_decode() {
        let record = Record::Control {
            recv_ts: ts(),
            conn_id: None,
            body: ControlBody::RunStop { clean: true },
        };
        let text = line(&record, 1);
        assert!(decode(&text[..text.len() / 2]).is_err());
    }
}
