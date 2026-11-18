use std::sync::Arc;

use chrono::{TimeZone, Utc};
use rust_decimal_macros::dec;
use serde_json::{Value, json};
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path, query_param, query_param_is_missing},
};

use super::*;
use crate::test_support::{condition, no, yes};

const T0: i64 = 1_795_000_000; // seconds

fn row(n: u8, token: &str, side: &str, price: &str, size: &str, hash: &str, secs: i64) -> Value {
    json!({
        "proxy_wallet": "0x0000000000000000000000000000000000000001",
        "side": side,
        "token_id": token,
        "condition_id": condition(n),
        "size": size.parse::<f64>().unwrap(),
        "price": price.parse::<f64>().unwrap(),
        "timestamp": secs,
        "title": "t", "slug": "s", "icon": "", "event_slug": "e",
        "outcome": "Yes", "outcome_index": 0,
        "name": "", "pseudonym": "", "bio": "",
        "profile_image": "", "profile_image_optimized": "",
        "transaction_hash": hash,
    })
}

fn page(rows: Vec<Value>, next: Option<&str>) -> ResponseTemplate {
    ResponseTemplate::new(200).set_body_json(json!({
        "data": rows,
        "pagination": {"limit": 1000, "offset": 0, "has_more": next.is_some(), "next_cursor": next},
    }))
}

async fn server(second: ResponseTemplate) -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v2/trades"))
        .and(query_param_is_missing("cursor"))
        .and(query_param("taker_only", "true"))
        .respond_with(page(
            vec![
                row(1, yes(1).as_ref(), "BUY", "0.5", "10", "0xAA", T0 + 100), // after `until`
                row(1, yes(1).as_ref(), "BUY", "0.5", "10", "0xBB", T0 - 10),
                row(2, no(2).as_ref(), "SELL", "0.3", "4", "0xcc", T0 - 20),
            ],
            Some("c1"),
        ))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v2/trades"))
        .and(query_param("cursor", "c1"))
        .respond_with(second)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v2/trades"))
        .and(query_param("cursor", "c2"))
        .respond_with(page(
            vec![
                row(1, yes(1).as_ref(), "SELL", "0.6", "2", "0xdd", T0 - 3000),
                row(2, yes(2).as_ref(), "BUY", "0.1", "1", "0xee", T0 - 99_999), // before `from`
            ],
            None,
        ))
        .mount(&server)
        .await;
    server
}

fn client(server: &MockServer) -> DataClient {
    DataClient::builder()
        .base_url(server.uri())
        .build()
        .unwrap()
}

fn t(secs: i64) -> UtcMicros {
    Utc.timestamp_opt(secs, 0).unwrap()
}

#[tokio::test]
async fn three_pages_give_one_complete_backfill_per_condition() {
    let second = page(
        vec![row(2, no(2).as_ref(), "BUY", "0.7", "1", "0xff", T0 - 2000)],
        Some("c2"),
    );
    let server = server(second).await;
    let result = fetch_batch(
        &client(&server),
        &[condition(1), condition(2)],
        t(T0 - 50_000),
        t(T0),
        50,
    )
    .await;
    assert!(result.complete);
    assert_eq!(result.pages, 3);
    assert_eq!(result.rows, 6);
    let hashes = |n: u8| -> Vec<String> {
        result.prints[&condition(n)]
            .iter()
            .map(|p| p.tx_hash.to_string())
            .collect()
    };
    assert_eq!(
        hashes(1),
        ["0xbb", "0xdd"],
        "rows in [from, until), hashes lower-cased"
    );
    assert_eq!(hashes(2), ["0xcc", "0xff"]);
    let first = &result.prints[&condition(2)][0];
    assert_eq!(
        (first.side, first.price.value(), first.size.value()),
        (TakerSide::Sell, dec!(0.3), dec!(4))
    );
    assert_eq!(first.timestamp, t(T0 - 20));
}

#[tokio::test]
async fn more_pages_without_a_usable_cursor_leave_the_backfill_incomplete() {
    // `has_more` with an empty cursor, before `from` is reached.
    let second = ResponseTemplate::new(200).set_body_json(json!({
        "data": [row(1, yes(1).as_ref(), "BUY", "0.5", "1", "0x11", T0 - 2000)],
        "pagination": {"limit": 1000, "offset": 0, "has_more": true, "next_cursor": ""},
    }));
    let server = server(second).await;
    let result = fetch_batch(&client(&server), &[condition(1)], t(T0 - 50_000), t(T0), 50).await;
    assert_eq!(result.pages, 2);
    assert!(!result.complete);
}

#[tokio::test]
async fn a_failed_page_leaves_the_backfill_incomplete() {
    let server = server(ResponseTemplate::new(500)).await;
    let result = fetch_batch(
        &client(&server),
        &[condition(1), condition(2)],
        t(T0 - 50_000),
        t(T0),
        50,
    )
    .await;
    assert!(!result.complete);
    assert_eq!(result.pages, 1);
    assert_eq!(
        result.prints[&condition(1)].len(),
        1,
        "rows of the pages fetched are kept"
    );
}

#[tokio::test]
async fn paging_stops_once_rows_are_older_than_the_start_or_at_the_cap() {
    let second = page(
        vec![row(2, no(2).as_ref(), "BUY", "0.7", "1", "0xff", T0 - 2000)],
        Some("c2"),
    );
    let server = server(second).await;
    // Page 2 already reaches back past `from`.
    let result = fetch_batch(
        &client(&server),
        &[condition(1), condition(2)],
        t(T0 - 1000),
        t(T0),
        50,
    )
    .await;
    assert!(result.complete);
    assert_eq!(result.pages, 2);
    let capped = fetch_batch(&client(&server), &[condition(1)], t(T0 - 50_000), t(T0), 1).await;
    assert!(!capped.complete);
}

#[tokio::test]
async fn the_task_sends_one_event_per_condition() {
    let second = page(vec![], Some("c2"));
    let server = server(second).await;
    let (tx, mut rx) = mpsc::channel(16);
    let (requests, requests_rx) = mpsc::unbounded_channel();
    let token = CancellationToken::new();
    let clock: SharedClock = Arc::new(pm_core::clock::SystemClock);
    let task = tokio::spawn(backfill_task(
        client(&server),
        2,
        50,
        clock,
        tx,
        requests_rx,
        token.clone(),
    ));
    requests
        .send(BackfillRequest {
            conditions: vec![condition(1), condition(2)],
            from: t(T0 - 50_000),
            until: t(T0),
        })
        .unwrap();
    let mut seen = vec![];
    for _ in 0..2 {
        match rx.recv().await.unwrap().event {
            Event::VolumeBackfill {
                condition,
                complete,
                from,
                until,
                ..
            } => {
                assert!(complete);
                assert_eq!((from, until), (t(T0 - 50_000), t(T0)));
                seen.push(condition);
            }
            other => panic!("unexpected {other:?}"),
        }
    }
    seen.sort();
    assert_eq!(seen, [condition(1), condition(2)]);
    token.cancel();
    task.await.unwrap().unwrap();
}
