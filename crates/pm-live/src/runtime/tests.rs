//! End to end on real sockets and real time (short timings): a fake market channel, fake
//! Gamma and CLOB, and a Data API stub.

use std::sync::Mutex;

use async_trait::async_trait;
use marcasite::{gamma::Market, types::ConditionId};
use pm_core::{
    book::{LevelChange, OrderBook},
    test_util::{
        fake_ws::{FakeConn, FakeServer, TIMEOUT},
        gamma_market,
    },
};
use rust_decimal::Decimal;
use serde_json::json;
use tokio::time::{sleep, timeout};
use wiremock::{Mock, MockServer, ResponseTemplate, matchers::path};

use super::*;
use crate::{
    engine::books::Distrust,
    engine_log::LogReader,
    event::Envelope,
    sink::MemorySink,
    test_support::{book_json, no, price_change_json, yes},
};

struct OneMarket(Mutex<Vec<Market>>);

#[async_trait]
impl MarketSource for OneMarket {
    async fn open_markets(&self) -> Result<Vec<Market>> {
        Ok(self.0.lock().unwrap().clone())
    }
    async fn markets(&self, _: &[ConditionId]) -> Result<Vec<Market>> {
        Ok(vec![])
    }
}

struct NoClob;

#[async_trait]
impl ClobFees for NoClob {
    async fn fee_details(&self, _: &ConditionId) -> Result<Option<(Decimal, u32)>> {
        Ok(None)
    }
}

async fn data_api() -> MockServer {
    let server = MockServer::start().await;
    Mock::given(path("/v2/trades"))
        .respond_with(ResponseTemplate::new(200).set_body_json(
            json!({"data": [], "pagination": {"limit": 1000, "offset": 0, "has_more": false}}),
        ))
        .mount(&server)
        .await;
    server
}

fn config(server: &FakeServer, data_dir: &std::path::Path) -> Config {
    let mut config = Config::default();
    config.run.data_dir = data_dir.into();
    config.endpoints.ws = server.url();
    config.ingest.min_liquidity_usd = Decimal::from(1000);
    config.ingest.reconnect_initial_backoff = Duration::from_millis(20);
    config.ingest.reconnect_max_backoff = Duration::from_millis(50);
    config.ingest.reconnect_stable_after = Duration::from_millis(100);
    config.ingest.heartbeat_interval = Duration::from_millis(500);
    config.ingest.idle_timeout = Duration::from_secs(3);
    config.ingest.resnapshot_after_decode_error = Duration::from_millis(300);
    config.sync();
    config
}

/// The next message from the client that is not a subscription snapshot request we ignore.
async fn recv(conn: &mut FakeConn) -> serde_json::Value {
    timeout(TIMEOUT, conn.recv_json())
        .await
        .expect("a client message in time")
}

fn send_books(conn: &FakeConn, at_ms: i64) {
    conn.send(
        &json!([
            book_json(&yes(1), &[("0.40", "100")], &[("0.42", "80")], at_ms),
            book_json(&no(1), &[("0.58", "80")], &[("0.60", "100")], at_ms),
        ])
        .to_string(),
    );
}

#[tokio::test]
async fn ingest_end_to_end_with_a_decode_error_a_reconnect_and_a_replay() {
    let mut server = FakeServer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let config = Arc::new(config(&server, dir.path()));
    let engine_config = config.engine.clone();
    let data = data_api().await;
    let sources = Sources {
        markets: Arc::new(OneMarket(Mutex::new(vec![gamma_market(
            1,
            json!({"feeSchedule": {"rate": 0.04, "exponent": 1}}),
        )]))),
        clob: Arc::new(NoClob),
        data_bulk: DataClient::builder().base_url(data.uri()).build().unwrap(),
    };
    let table = FeeTable::load(
        &std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../config/fee_schedule_history.toml"),
    )
    .unwrap();
    let sink = MemorySink::default();
    let token = CancellationToken::new();
    let clock: SharedClock = Arc::new(pm_core::clock::SystemClock);
    let run = tokio::spawn(run(
        config,
        "r-test",
        clock,
        sources,
        table,
        Box::new(sink.clone()),
        token.clone(),
    ));

    // Subscribed to both tokens with custom features.
    let mut conn = server.accept().await;
    let subscription = recv(&mut conn).await;
    assert_eq!(subscription["assets_ids"], json!([yes(1), no(1)]));

    // Snapshots, then 50 changes to the YES book.
    send_books(&conn, 1000);
    let mut expected = OrderBook::from_ws_book(
        &serde_json::from_value(book_json(
            &yes(1),
            &[("0.40", "100")],
            &[("0.42", "80")],
            1000,
        ))
        .unwrap(),
    );
    for i in 0..50i64 {
        let price = format!("0.{}", 30 + (i % 20));
        let size = (i * 2 % 9).to_string();
        let side = if i % 20 < 11 { "BUY" } else { "SELL" };
        let frame = price_change_json(&[(&yes(1), &price, &size, side)], 1001 + i);
        let event: marcasite::ws::PriceChangeEvent = serde_json::from_value(frame.clone()).unwrap();
        for change in &event.price_changes {
            expected
                .apply_in_place(&LevelChange::from_price_change(change, event.timestamp).unwrap());
        }
        conn.send(&frame.to_string());
    }

    // An undecodable frame: no book follows within the delay, so the engine resubscribes.
    conn.send(r#"{"event_type":"price_change","market":"0xabc","price_changes":"lost","timestamp":"1060"}"#);
    let unsubscribe = recv(&mut conn).await;
    assert_eq!(unsubscribe["operation"], "unsubscribe", "{unsubscribe}");
    let resubscribe = recv(&mut conn).await;
    assert_eq!(resubscribe["operation"], "subscribe", "{resubscribe}");
    let mut tokens: Vec<String> =
        serde_json::from_value(resubscribe["assets_ids"].clone()).unwrap();
    tokens.sort();
    assert_eq!(tokens, [yes(1).to_string(), no(1).to_string()]);
    // The snapshots the server sends on resubscribing restore trust.
    send_books(&conn, 2000);
    for i in 0..50i64 {
        let frame = price_change_json(
            &[(
                &yes(1),
                &format!("0.{}", 30 + (i % 20)),
                &(i * 2 % 9).to_string(),
                if i % 20 < 11 { "BUY" } else { "SELL" },
            )],
            2001 + i,
        );
        conn.send(&frame.to_string());
    }

    // The connection drops; the channel reconnects and the books arrive a moment later.
    sleep(Duration::from_millis(200)).await;
    conn.close();
    let mut conn = server.accept().await;
    let subscription = recv(&mut conn).await;
    assert_eq!(subscription["assets_ids"], json!([yes(1), no(1)]));
    sleep(Duration::from_millis(300)).await;
    send_books(&conn, 3000);
    sleep(Duration::from_millis(300)).await;

    token.cancel();
    let outcome = timeout(SHUTDOWN_LIMIT + TIMEOUT, run)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let engine = &outcome.engine;
    assert!(
        engine.book(&yes(1)).unwrap().is_trusted() && engine.book(&no(1)).unwrap().is_trusted()
    );

    // Replay the engine log through a fresh engine, checking trust along the way.
    let envelopes: Vec<Envelope> = LogReader::open(outcome.log_files.clone())
        .map(Result::unwrap)
        .collect();
    assert_eq!(envelopes.len() as u64, engine.seq());
    let mut replay = Engine::new(engine_config.clone());
    let mut second = Engine::new(engine_config);
    let mut saw = Vec::new();
    let mut resnapshots = 0;
    for envelope in envelopes {
        let name = envelope.event.name();
        if name == "conn_decode_error" {
            // The book after the first 50 changes equals one rebuilt from the same messages.
            let book = &replay.book(&yes(1)).unwrap().current().unwrap().book;
            assert!(
                book.same_levels(&expected),
                "{:?}",
                book.differences(&expected)
            );
        }
        let a = replay.step(envelope.clone()).unwrap();
        let b = second.step(envelope).unwrap();
        assert_eq!(a, b, "replays are deterministic");
        resnapshots += a
            .iter()
            .filter(|action| matches!(action, Action::Resnapshot { .. }))
            .count();
        let distrust = replay.book(&yes(1)).and_then(|book| book.distrust());
        if matches!(
            name,
            "conn_up" | "conn_down" | "conn_decode_error" | "ws_book"
        ) {
            saw.push((name, distrust));
        }
    }
    assert_eq!(
        resnapshots, 1,
        "the replay asks for the same resnapshot as the run"
    );
    assert!(
        replay
            .book(&yes(1))
            .unwrap()
            .current()
            .unwrap()
            .book
            .same_levels(&engine.book(&yes(1)).unwrap().current().unwrap().book)
    );
    let trust: Vec<_> = saw.iter().map(|(name, d)| (*name, *d)).collect();
    let decode = trust
        .iter()
        .position(|(n, _)| *n == "conn_decode_error")
        .unwrap();
    assert_eq!(trust[decode].1, Some(Distrust::DecodeError));
    let down = trust.iter().position(|(n, _)| *n == "conn_down").unwrap();
    assert_eq!(trust[down].1, Some(Distrust::Disconnected));
    assert_eq!(
        trust[down + 1],
        ("conn_up", Some(Distrust::AwaitingSnapshot))
    );
    assert_eq!(
        trust.last().unwrap().1,
        None,
        "trusted again on the snapshot"
    );

    // Rows: the market and its fees, a complete (empty) backfill, and metrics.
    let records = sink.records();
    assert!(records.iter().any(|r| matches!(r, crate::action::Record::MarketFees { fees, .. } if fees.source == pm_core::units::FeeSource::Gamma)));
    assert!(
        records
            .iter()
            .any(|r| matches!(r, crate::action::Record::Metric(m) if m.name == "decode_errors"))
    );
}
