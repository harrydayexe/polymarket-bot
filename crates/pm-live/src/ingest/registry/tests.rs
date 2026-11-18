use std::{
    collections::VecDeque,
    path::Path,
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use chrono::{DateTime, TimeZone, Utc};
use pm_core::test_util::{TokioClock, gamma_market};
use rust_decimal_macros::dec;
use serde_json::{Value, json};

use super::*;
use crate::test_support::condition;

#[derive(Default)]
struct FakeGamma {
    open: Mutex<Vec<Market>>,
}

impl FakeGamma {
    fn set(&self, markets: Vec<Market>) {
        *self.open.lock().unwrap() = markets;
    }
}

#[async_trait]
impl MarketSource for FakeGamma {
    async fn open_markets(&self) -> Result<Vec<Market>> {
        Ok(self.open.lock().unwrap().clone())
    }

    async fn markets(&self, ids: &[ConditionId]) -> Result<Vec<Market>> {
        Ok(self
            .open
            .lock()
            .unwrap()
            .iter()
            .filter(|m| m.condition_id.as_ref().is_some_and(|id| ids.contains(id)))
            .cloned()
            .collect())
    }
}

type ClobAnswer = Result<Option<(Decimal, u32)>>;

#[derive(Default)]
struct FakeClob {
    answers: Mutex<VecDeque<ClobAnswer>>,
    calls: AtomicUsize,
}

impl FakeClob {
    fn answer(&self, answer: ClobAnswer) {
        self.answers.lock().unwrap().push_back(answer);
    }
}

#[async_trait]
impl ClobFees for FakeClob {
    async fn fee_details(&self, _: &ConditionId) -> Result<Option<(Decimal, u32)>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.answers
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_else(|| Err(anyhow::anyhow!("500 Internal Server Error")))
    }
}

fn table() -> FeeTable {
    FeeTable::load(
        &Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config/fee_schedule_history.toml"),
    )
    .unwrap()
}

fn config() -> IngestConfig {
    IngestConfig {
        min_liquidity_usd: Decimal::from(1000),
        closed_grace_period: Duration::from_secs(300),
        ..IngestConfig::default()
    }
}

fn with_fees(n: u8, rate: Value) -> Market {
    gamma_market(
        n,
        json!({"feesEnabled": true, "feeSchedule": {"rate": rate, "exponent": 1}}),
    )
}

fn at(text: &str) -> DateTime<Utc> {
    text.parse().unwrap()
}

fn unpack(event: Option<Event>) -> (Vec<LiveMarket>, Vec<RemovedMarket>, Vec<MarketChanged>) {
    match event {
        Some(Event::RegistryUpdate {
            add,
            remove,
            changed,
        }) => (add, remove, changed),
        other => panic!("expected a registry update, got {other:?}"),
    }
}

#[tokio::test]
async fn fees_from_gamma_need_no_clob_call() {
    let gamma = FakeGamma::default();
    gamma.set(vec![with_fees(1, json!(0.01))]);
    let clob = FakeClob::default();
    let mut state = RegistryState::new(&config(), table());
    let now = at("2026-11-21T00:00:00Z");
    let (add, remove, changed) = unpack(state.poll(&gamma, &clob, now).await.unwrap());
    assert_eq!(add.len(), 1);
    assert!(remove.is_empty() && changed.is_empty());
    assert_eq!(add[0].fees.source, FeeSource::Gamma);
    assert_eq!(add[0].fees.rate, dec!(0.01));
    assert_eq!(add[0].fees_enabled, Some(true));
    assert_eq!(clob.calls.load(Ordering::SeqCst), 0);
    // The same poll again changes nothing.
    assert!(state.poll(&gamma, &clob, now).await.unwrap().is_none());
}

#[tokio::test]
async fn without_a_gamma_schedule_fees_come_from_the_clob_then_the_table() {
    let gamma = FakeGamma::default();
    gamma.set(vec![gamma_market(1, json!({}))]);
    let clob = FakeClob::default();
    clob.answer(Ok(Some((dec!(0.05), 1))));
    let mut state = RegistryState::new(&config(), table());
    let first = at("2026-11-19T12:00:00Z");
    let (add, ..) = unpack(state.poll(&gamma, &clob, first).await.unwrap());
    assert_eq!(add[0].fees.source, FeeSource::Clob);
    assert_eq!(add[0].fees.rate, dec!(0.05));
    assert_eq!(add[0].fees.seen_at, first);
    // Next poll the CLOB fails (500): the table row in force, politics at its higher rate.
    let second = at("2026-11-19T12:01:00Z");
    let (_, _, changed) = unpack(state.poll(&gamma, &clob, second).await.unwrap());
    assert_eq!(changed.len(), 1);
    let fees = changed[0].market.fees;
    assert!(changed[0].fee_changed && !changed[0].details_changed);
    assert_eq!(
        (fees.rate, fees.source),
        (dec!(0.04), FeeSource::FallbackConservative)
    );
    assert_eq!(fees.seen_at, second);
    assert_eq!(
        clob.calls.load(Ordering::SeqCst),
        2,
        "the CLOB is asked whenever Gamma has no schedule"
    );
}

#[tokio::test]
async fn tags_with_no_fee_category_use_the_other_row() {
    let gamma = FakeGamma::default();
    gamma.set(vec![gamma_market(
        1,
        json!({"tags": [{"slug": "politics"}, {"slug": "bitcoin"}]}),
    )]);
    let mut state = RegistryState::new(&config(), table());
    let (add, ..) = unpack(
        state
            .poll(&gamma, &FakeClob::default(), at("2026-12-01T00:00:00Z"))
            .await
            .unwrap(),
    );
    assert_eq!(add[0].fees.source, FeeSource::FallbackConservative);
    let unknown = gamma_market(2, json!({"tags": [{"slug": "politics"}]}));
    let mut info = MarketInfo::from_gamma(unknown, at("2026-12-01T00:00:00Z")).unwrap();
    info.tags = vec!["crypto".into()];
    let fees = state
        .resolve_fees(&info, &FakeClob::default(), at("2026-12-01T00:00:00Z"))
        .await;
    assert_eq!(
        (fees.rate, fees.source),
        (dec!(0.05), FeeSource::FallbackConservative)
    );
}

struct Running {
    gamma: Arc<FakeGamma>,
    events: mpsc::Receiver<Stamped>,
    hints: mpsc::UnboundedSender<RegistryHint>,
    token: CancellationToken,
}

fn start(start: DateTime<Utc>, markets: Vec<Market>) -> Running {
    let gamma = Arc::new(FakeGamma::default());
    gamma.set(markets);
    let (tx, events) = mpsc::channel(16);
    let (hints, hints_rx) = mpsc::unbounded_channel();
    let token = CancellationToken::new();
    let clock: SharedClock = Arc::new(TokioClock::new(start));
    tokio::spawn(registry_task(
        RegistryState::new(&config(), table()),
        gamma.clone(),
        Arc::new(FakeClob::default()),
        clock,
        tx,
        hints_rx,
        token.clone(),
    ));
    Running {
        gamma,
        events,
        hints,
        token,
    }
}

impl Running {
    async fn next(
        &mut self,
    ) -> (
        DateTime<Utc>,
        (Vec<LiveMarket>, Vec<RemovedMarket>, Vec<MarketChanged>),
    ) {
        let stamped = self.events.recv().await.unwrap();
        (stamped.available_at, unpack(Some(stamped.event)))
    }
}

#[tokio::test(start_paused = true)]
async fn a_fee_change_across_the_20_november_switch_is_reported() {
    let start_at = Utc.with_ymd_and_hms(2026, 11, 20, 23, 58, 30).unwrap();
    let mut run = start(start_at, vec![with_fees(1, json!(0.04))]);
    let (_, (add, ..)) = run.next().await;
    assert_eq!(add[0].fees.rate, dec!(0.04));
    run.gamma.set(vec![with_fees(1, json!(0.01))]);
    let (when, (add, remove, changed)) = run.next().await;
    assert!(add.is_empty() && remove.is_empty());
    assert_eq!(
        when,
        Utc.with_ymd_and_hms(2026, 11, 20, 23, 59, 30).unwrap()
    );
    assert!(changed[0].fee_changed);
    assert_eq!(changed[0].market.fees.rate, dec!(0.01));
    assert_eq!(changed[0].market.fees.seen_at, when);
    run.token.cancel();
}

#[tokio::test(start_paused = true)]
async fn a_closed_market_is_removed_after_its_grace_period() {
    let start_at = Utc.with_ymd_and_hms(2026, 11, 18, 12, 0, 0).unwrap();
    let mut run = start(
        start_at,
        vec![with_fees(1, json!(0.04)), with_fees(2, json!(0.04))],
    );
    let (_, (add, ..)) = run.next().await;
    assert_eq!(add.len(), 2);
    // Market 1 closes on Gamma: it stays (grace) for 5 minutes, then goes.
    run.gamma.set(vec![
        gamma_market(1, json!({"closed": true, "feesEnabled": true, "feeSchedule": {"rate": 0.04, "exponent": 1}})),
        with_fees(2, json!(0.04)),
    ]);
    // Closing changes nothing the engine needs: the next update is the removal, once the
    // grace period from the poll that saw the close (one interval in) has passed.
    let (removed_at, (add, remove, _)) = run.next().await;
    assert!(add.is_empty());
    assert_eq!(
        remove
            .iter()
            .map(|r| r.condition_id.clone())
            .collect::<Vec<_>>(),
        [condition(1)]
    );
    let closing_at = start_at + chrono::TimeDelta::seconds(60);
    assert!(
        removed_at - closing_at >= chrono::TimeDelta::minutes(5),
        "removed at {removed_at}"
    );
    run.token.cancel();
}

#[tokio::test(start_paused = true)]
async fn new_market_hints_bring_the_next_poll_forward_at_most_to_the_minimum_gap() {
    let start_at = Utc.with_ymd_and_hms(2026, 11, 18, 12, 0, 0).unwrap();
    let mut run = start(start_at, vec![with_fees(1, json!(0.04))]);
    run.next().await;
    run.gamma
        .set(vec![with_fees(1, json!(0.04)), with_fees(2, json!(0.04))]);
    tokio::time::sleep(Duration::from_secs(5)).await;
    for _ in 0..50 {
        run.hints.send(RegistryHint::NewMarket).unwrap();
    }
    // Brought forward from 60 s to the 30 s minimum gap, once.
    let (when, (add, ..)) = run.next().await;
    assert_eq!(add.len(), 1);
    assert_eq!(
        when - start_at,
        chrono::TimeDelta::seconds(30),
        "polled at {when}"
    );
    run.gamma.set(vec![
        with_fees(1, json!(0.04)),
        with_fees(2, json!(0.04)),
        with_fees(3, json!(0.04)),
    ]);
    let (when, _) = run.next().await;
    assert_eq!(
        when - start_at,
        chrono::TimeDelta::seconds(90),
        "then the normal interval"
    );
    run.token.cancel();
}

mod clob_adapter {
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };

    use super::*;

    #[tokio::test]
    async fn fee_details_come_from_fd() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(format!("/clob-markets/{}", condition(1))))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({"fd": {"r": 0.05, "e": 1, "to": true}, "tbf": 1000})),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(format!("/clob-markets/{}", condition(2))))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"tbf": 0})))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(format!("/clob-markets/{}", condition(3))))
            .respond_with(ResponseTemplate::new(500))
            .mount(&server)
            .await;
        let clob = ClobClient::builder()
            .base_url(server.uri())
            .build()
            .unwrap();
        assert_eq!(
            clob.fee_details(&condition(1)).await.unwrap(),
            Some((dec!(0.05), 1))
        );
        assert_eq!(clob.fee_details(&condition(2)).await.unwrap(), None);
        assert!(clob.fee_details(&condition(3)).await.is_err());
    }
}
