use std::{sync::Arc, time::Duration};

use chrono::{TimeZone, Utc};
use futures_util::stream;
use marcasite::{Service, WebSocketError, ws::MarketEvent};
use pm_core::test_util::{
    TokioClock,
    fake_ws::{FakeServer, TIMEOUT},
};
use serde_json::json;
use tokio::time::timeout;

use super::*;
use crate::test_support::{book_json, no, price_change_json, yes};

fn slot() -> ConnSlotId {
    ConnSlotId::new("c-0001")
}

fn market(value: serde_json::Value) -> marcasite::Result<ReconnectEvent> {
    Ok(ReconnectEvent::Market(
        serde_json::from_value::<MarketEvent>(value).unwrap(),
    ))
}

fn decode_error() -> marcasite::Result<ReconnectEvent> {
    Err(Error::WebSocket(Box::new(WebSocketError::new(
        Service::MarketChannel,
        WebSocketErrorKind::Decode,
        "failed to decode",
    ))))
}

fn connected(tokens: &[TokenId]) -> marcasite::Result<ReconnectEvent> {
    Ok(ReconnectEvent::Connected {
        asset_ids: tokens.to_vec(),
        attempt: 1,
    })
}

async fn collect(
    items: Vec<marcasite::Result<ReconnectEvent>>,
    clock: SharedClock,
) -> (Result<(), SlotError>, Vec<Stamped>) {
    let (tx, mut rx) = mpsc::channel(64);
    let result = run_slot(
        slot(),
        stream::iter(items),
        || vec![yes(1), no(1)],
        clock,
        tx,
    )
    .await;
    let mut out = vec![];
    while let Ok(stamped) = rx.try_recv() {
        out.push(stamped);
    }
    (result, out)
}

#[tokio::test(start_paused = true)]
async fn envelopes_carry_the_receive_time_and_the_exchange_time() {
    let start = Utc.with_ymd_and_hms(2026, 11, 18, 12, 0, 0).unwrap();
    let clock: SharedClock = Arc::new(TokioClock::new(start));
    let items = vec![
        connected(&[yes(1), no(1)]),
        market(book_json(
            &yes(1),
            &[("0.4", "10")],
            &[("0.6", "10")],
            1_795_000_000_000,
        )),
        market(price_change_json(
            &[(&yes(1), "0.41", "5", "BUY")],
            1_795_000_000_100,
        )),
        market(
            json!({"event_type": "best_bid_ask", "market": "0xabc", "asset_id": "1111", "best_bid": "0.4", "best_ask": "0.6", "spread": "0.2", "timestamp": "1795000000200"}),
        ),
        market(json!({"event_type": "something_new"})),
        decode_error(),
        Ok(ReconnectEvent::Disconnected {
            asset_ids: vec![yes(1), no(1)],
            error: None,
        }),
    ];
    let (result, out) = collect(items, clock).await;
    result.unwrap();
    let kinds: Vec<&str> = out.iter().map(|s| s.event.name()).collect();
    assert_eq!(
        kinds,
        [
            "conn_up",
            "ws_book",
            "ws_price_change",
            "conn_decode_error",
            "ws_dropped",
            "conn_down"
        ]
    );
    assert!(
        out.iter().all(|s| s.available_at == start),
        "every envelope is stamped at the paused clock"
    );
    assert_eq!(
        out[1].event_time,
        Utc.timestamp_millis_opt(1_795_000_000_000).unwrap()
    );
    assert_eq!(
        out[0].event_time, start,
        "no exchange time: the receive time"
    );
    assert!(
        matches!(&out[3].event, Event::ConnDecodeError { tokens, .. } if tokens == &[yes(1), no(1)])
    );
    assert!(matches!(
        out[4].event,
        Event::WsDropped {
            best_bid_ask: 1,
            unknown: 1,
            ..
        }
    ));
}

#[tokio::test(start_paused = true)]
async fn a_terminal_error_reports_the_loss_and_stops() {
    let clock: SharedClock = Arc::new(TokioClock::new(
        Utc.with_ymd_and_hms(2026, 11, 18, 12, 0, 0).unwrap(),
    ));
    let fatal = Err(Error::WebSocket(Box::new(WebSocketError::new(
        Service::MarketChannel,
        WebSocketErrorKind::Closed,
        "gave up",
    ))));
    let (result, out) = collect(
        vec![connected(&[yes(1)]), fatal, connected(&[yes(1)])],
        clock,
    )
    .await;
    assert!(matches!(result, Err(SlotError::Terminal { .. })));
    assert_eq!(out.len(), 2);
    assert!(matches!(
        &out[1].event,
        Event::ConnDown {
            reason: Some(_),
            ..
        }
    ));
}

fn ingest() -> IngestConfig {
    IngestConfig {
        reconnect_initial_backoff: Duration::from_millis(20),
        reconnect_max_backoff: Duration::from_millis(50),
        reconnect_stable_after: Duration::from_millis(200),
        heartbeat_interval: Duration::from_millis(500),
        idle_timeout: Duration::from_secs(2),
        connect_timeout: Duration::from_secs(1),
        ..IngestConfig::default()
    }
}

async fn next_event(rx: &mut mpsc::Receiver<Stamped>) -> Event {
    timeout(TIMEOUT, rx.recv())
        .await
        .expect("an event in time")
        .expect("the slot is running")
        .event
}

/// Real sockets (real time, short backoffs): a channel against the fake server.
#[tokio::test]
async fn subscriptions_made_while_disconnected_are_restored_and_decode_errors_keep_the_connection()
{
    let mut server = FakeServer::start().await;
    let channel = open_channel(&server.url(), &ingest(), vec![yes(1), no(1)]).unwrap();
    let handle = channel.handle();
    let (tx, mut rx) = mpsc::channel(64);
    let tokens = {
        let handle = handle.clone();
        move || handle.asset_ids()
    };
    let clock: SharedClock = Arc::new(pm_core::clock::SystemClock);
    let task = tokio::spawn(run_slot(slot(), channel, tokens, clock, tx));

    let mut conn = server.accept().await;
    let subscription = conn.recv_json().await;
    assert_eq!(subscription["assets_ids"], json!([yes(1), no(1)]));
    assert_eq!(subscription["custom_feature_enabled"], json!(true));
    assert!(matches!(next_event(&mut rx).await, Event::ConnUp { .. }));

    // A known message type with a field of the wrong type does not decode.
    conn.send(r#"{"event_type":"book","asset_id":"1111","market":"0xabc","bids":"oops","asks":[],"timestamp":"1","hash":"h"}"#);
    assert!(
        matches!(next_event(&mut rx).await, Event::ConnDecodeError { tokens, .. } if tokens.len() == 2)
    );
    conn.send(&book_json(&yes(1), &[("0.4", "1")], &[], 5).to_string());
    assert!(
        matches!(next_event(&mut rx).await, Event::WsBook { .. }),
        "still connected"
    );

    // Drop the connection and refuse reconnects; subscribe three more tokens meanwhile.
    server.refuse(true);
    conn.close();
    assert!(matches!(next_event(&mut rx).await, Event::ConnDown { .. }));
    handle.subscribe([yes(2), no(2), yes(3)]).unwrap();
    assert!(matches!(
        next_event(&mut rx).await,
        Event::ConnFailed { .. }
    ));
    server.refuse(false);
    let mut conn = server.accept().await;
    let subscription = conn.recv_json().await;
    let mut subscribed: Vec<String> =
        serde_json::from_value(subscription["assets_ids"].clone()).unwrap();
    subscribed.sort();
    assert_eq!(subscribed, ["1111", "1222", "2111", "2222", "3111"]);
    loop {
        match next_event(&mut rx).await {
            Event::ConnFailed { .. } => continue,
            Event::ConnUp { tokens, .. } => {
                assert_eq!(tokens.len(), 5);
                break;
            }
            other => panic!("unexpected {other:?}"),
        }
    }
    handle.close();
    timeout(TIMEOUT, task).await.unwrap().unwrap().unwrap();
}
