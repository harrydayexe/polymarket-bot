use std::collections::HashMap;

use proptest::prelude::*;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::Deserialize;

use super::*;
use crate::{
    market::MarketTokens,
    normalize::print::{RawPrint, TakerSide},
    test_util::t,
};

// ---- Golden fixtures: fixtures/tier1/grouping.json ----

#[derive(Deserialize)]
struct Fixture {
    market: FixtureMarket,
    config: FixtureConfig,
    cases: Vec<Case>,
}

#[derive(Deserialize)]
struct FixtureMarket {
    condition_id: ConditionId,
    yes: TokenId,
    no: TokenId,
}

#[derive(Deserialize)]
struct FixtureConfig {
    window_ms: u64,
    dedup_retention_ms: u64,
}

#[derive(Deserialize)]
struct Case {
    name: String,
    prints: Vec<FixturePrint>,
    expected: Expected,
}

#[derive(Deserialize)]
struct FixturePrint {
    token: TokenId,
    side: TakerSide,
    price: Price,
    size: Shares,
    tx: Option<TxHash>,
    exchange_ts: i64,
    recv_ts: i64,
}

#[derive(Deserialize)]
struct Expected {
    groups: Vec<ExpectedGroup>,
    late_cross_token_dups: u64,
}

#[derive(Deserialize)]
struct ExpectedGroup {
    seq: u64,
    prints: Vec<usize>,
    outcome: Outcome,
    bought_token: TokenId,
    tx_hashes: Vec<TxHash>,
    dup_cross_token: u32,
    direction_conflict: u32,
    flags: Vec<GroupFlag>,
    shares: Decimal,
    notional: Decimal,
    vwap: Decimal,
    closed_at: i64,
}

fn micros(us: i64) -> DateTime<Utc> {
    DateTime::from_timestamp_micros(us).unwrap()
}

fn load_fixture() -> Fixture {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../fixtures/tier1/grouping.json"
    );
    let text = std::fs::read_to_string(path).expect("fixture file exists");
    serde_json::from_str(&text).expect("fixture parses")
}

#[test]
fn golden_grouping_cases() {
    let fixture = load_fixture();
    let tokens = MarketTokens {
        yes: fixture.market.yes.clone(),
        no: fixture.market.no.clone(),
    };
    let config = GrouperConfig {
        window: Duration::from_millis(fixture.config.window_ms),
        dedup_retention: Duration::from_millis(fixture.config.dedup_retention_ms),
    };
    assert!(fixture.cases.len() >= 13);
    for case in &fixture.cases {
        let prints: Vec<NormalisedPrint> = case
            .prints
            .iter()
            .map(|p| {
                let raw = RawPrint {
                    condition_id: fixture.market.condition_id.clone(),
                    token: p.token.clone(),
                    side: p.side,
                    price: p.price,
                    size: p.size,
                    tx_hash: p.tx.clone(),
                    exchange_ts: micros(p.exchange_ts),
                    recv_ts: micros(p.recv_ts),
                };
                NormalisedPrint::new(&raw, &tokens).unwrap()
            })
            .collect();

        let mut grouper = Grouper::new(config).unwrap();
        let mut groups = vec![];
        for print in &prints {
            groups.extend(grouper.push(print.clone()));
        }
        groups.extend(grouper.flush());

        let name = &case.name;
        assert_eq!(
            groups.len(),
            case.expected.groups.len(),
            "{name}: group count"
        );
        for (got, want) in groups.iter().zip(&case.expected.groups) {
            let indices: Vec<usize> = got
                .prints
                .iter()
                .map(|p| prints.iter().position(|q| q == p).expect("a pushed print"))
                .collect();
            assert_eq!(got.seq, want.seq, "{name}: seq");
            assert_eq!(indices, want.prints, "{name}: prints of group {}", want.seq);
            assert_eq!(got.outcome, want.outcome, "{name}: outcome");
            assert_eq!(got.bought_token, want.bought_token, "{name}: bought token");
            assert_eq!(got.tx_hashes, want.tx_hashes, "{name}: tx hashes");
            assert_eq!(got.dup_cross_token, want.dup_cross_token, "{name}: dups");
            assert_eq!(
                got.direction_conflict, want.direction_conflict,
                "{name}: conflicts"
            );
            assert_eq!(got.flags, want.flags, "{name}: flags");
            assert_eq!(got.shares().value(), want.shares, "{name}: shares");
            assert_eq!(got.notional().value(), want.notional, "{name}: notional");
            assert_eq!(got.vwap().unwrap().value(), want.vwap, "{name}: vwap");
            assert_eq!(got.closed_at, micros(want.closed_at), "{name}: closed_at");
        }
        assert_eq!(
            grouper.stats().late_cross_token_dups,
            case.expected.late_cross_token_dups,
            "{name}: late duplicates"
        );
        assert_eq!(grouper.open_groups(), 0);
    }
}

// ---- Hand-written cases for behaviour the fixture does not pin ----

fn tokens() -> MarketTokens {
    MarketTokens {
        yes: TokenId::new("1111"),
        no: TokenId::new("1222"),
    }
}

fn print(
    token: &str,
    side: TakerSide,
    price: Decimal,
    tx: Option<&str>,
    ex: i64,
    recv: i64,
) -> NormalisedPrint {
    let raw = RawPrint {
        condition_id: ConditionId::new("0xc1"),
        token: TokenId::new(token),
        side,
        price: Price::new(price).unwrap(),
        size: Shares::new(dec!(10)).unwrap(),
        tx_hash: tx.map(TxHash::new),
        exchange_ts: t(ex),
        recv_ts: t(recv),
    };
    NormalisedPrint::new(&raw, &tokens()).unwrap()
}

fn grouper() -> Grouper {
    Grouper::new(GrouperConfig::default()).unwrap()
}

#[test]
fn groups_close_on_advance_not_before() {
    let mut g = grouper();
    assert!(
        g.push(print("1111", TakerSide::Buy, dec!(0.2), Some("a"), 0, 50))
            .is_empty()
    );
    assert!(g.advance(t(299)).is_empty());
    assert_eq!(g.open_groups(), 1);
    let closed = g.advance(t(300));
    assert_eq!(closed.len(), 1);
    assert_eq!(closed[0].closed_at, t(300));
    // Noticed late, still stamped at the moment it closed.
    let mut g = grouper();
    g.push(print("1111", TakerSide::Buy, dec!(0.2), Some("a"), 0, 50));
    assert_eq!(g.advance(t(5000))[0].closed_at, t(300));
}

#[test]
fn conditions_are_grouped_separately() {
    let mut g = grouper();
    let a = print("1111", TakerSide::Buy, dec!(0.2), Some("a"), 0, 50);
    let mut b = print("1111", TakerSide::Buy, dec!(0.3), Some("b"), 10, 60);
    b.condition_id = ConditionId::new("0xc2");
    g.push(a);
    g.push(b);
    assert_eq!(g.open_groups(), 2);
    let closed = g.flush();
    assert_eq!(closed.len(), 2);
    assert_ne!(closed[0].condition_id, closed[1].condition_id);
}

#[test]
fn prints_without_a_hash_group_by_the_sweep_rule() {
    let mut g = grouper();
    g.push(print("1111", TakerSide::Buy, dec!(0.2), None, 0, 50));
    g.push(print("1111", TakerSide::Buy, dec!(0.2), None, 10, 60));
    let closed = g.flush();
    assert_eq!(closed.len(), 1);
    assert_eq!(closed[0].n_prints(), 2);
    assert!(closed[0].tx_hashes.is_empty());
}

#[test]
fn an_earlier_exchange_time_does_not_join_by_the_sweep_rule() {
    let mut g = grouper();
    g.push(print(
        "1111",
        TakerSide::Buy,
        dec!(0.2),
        Some("a"),
        100,
        150,
    ));
    g.push(print("1111", TakerSide::Buy, dec!(0.3), Some("b"), 50, 160));
    assert_eq!(g.flush().len(), 2);
}

#[test]
fn time_going_backwards_is_counted_and_ignored() {
    let mut g = grouper();
    g.push(print("1111", TakerSide::Buy, dec!(0.2), Some("a"), 0, 500));
    assert!(g.advance(t(100)).is_empty());
    assert_eq!(g.stats().time_reversals, 1);
    assert_eq!(g.open_groups(), 1);
}

#[test]
fn hashes_are_forgotten_after_the_retention() {
    let mut g = grouper();
    g.push(print("1111", TakerSide::Buy, dec!(0.3), Some("a"), 0, 50));
    g.advance(t(60_050));
    // Too late to recognise as a duplicate: it opens a group of its own.
    let closed = g.push(print(
        "1222",
        TakerSide::Sell,
        dec!(0.7),
        Some("a"),
        0,
        60_100,
    ));
    assert!(closed.is_empty());
    assert_eq!(g.stats().late_cross_token_dups, 0);
    assert_eq!(g.flush().len(), 1);
}

#[test]
fn invalid_config_is_refused() {
    assert!(
        Grouper::new(GrouperConfig {
            window: Duration::ZERO,
            ..GrouperConfig::default()
        })
        .is_err()
    );
    assert!(
        Grouper::new(GrouperConfig {
            window: Duration::from_secs(2),
            dedup_retention: Duration::from_secs(1),
        })
        .is_err()
    );
}

// ---- Properties ----

/// A random stream of prints on one market: two tokens, either side, a few shared
/// transaction hashes, exchange and receive times moving forward by small steps. Sizes are
/// unique so each print can be told apart.
fn arb_prints() -> impl Strategy<Value = Vec<NormalisedPrint>> {
    prop::collection::vec(
        (
            any::<bool>(),
            any::<bool>(),
            1u32..99,
            prop::option::of(0u8..6),
            0i64..400,
            0i64..400,
        ),
        1..60,
    )
    .prop_map(|steps| {
        let (mut ex, mut recv) = (1_000_000i64, 1_000_050i64);
        steps
            .into_iter()
            .enumerate()
            .map(|(i, (yes, buy, cents, tx, dex, drecv))| {
                ex += dex - 100;
                recv += drecv;
                let raw = RawPrint {
                    condition_id: ConditionId::new("0xc1"),
                    token: TokenId::new(if yes { "1111" } else { "1222" }),
                    side: if buy { TakerSide::Buy } else { TakerSide::Sell },
                    price: Price::new(Decimal::new(i64::from(cents), 2)).unwrap(),
                    size: Shares::new(Decimal::from(i as u64 + 1)).unwrap(),
                    tx_hash: tx.map(|n| TxHash::new(format!("0x{n}"))),
                    exchange_ts: t(ex),
                    recv_ts: t(recv),
                };
                NormalisedPrint::new(&raw, &tokens()).unwrap()
            })
            .collect()
    })
}

fn run(
    prints: &[NormalisedPrint],
    extra_advances: &[(usize, i64)],
) -> (Vec<PrintGroup>, GrouperStats) {
    let advances: HashMap<usize, i64> = extra_advances.iter().copied().collect();
    let mut g = grouper();
    let mut groups = vec![];
    for (i, p) in prints.iter().enumerate() {
        // An advance to any time up to this print's receive time, as a tick or a message
        // boundary would cause.
        if let Some(&back) = advances.get(&i) {
            let prev = if i == 0 {
                p.recv_ts
            } else {
                prints[i - 1].recv_ts
            };
            let at = (p.recv_ts - TimeDelta::milliseconds(back)).max(prev);
            groups.extend(g.advance(at));
        }
        groups.extend(g.push(p.clone()));
    }
    groups.extend(g.flush());
    (groups, g.stats())
}

proptest! {
    #[test]
    fn every_print_is_grouped_once_or_counted_as_a_duplicate(prints in arb_prints()) {
        let (groups, stats) = run(&prints, &[]);
        let mut seen: Vec<Decimal> = groups
            .iter()
            .flat_map(|g| g.prints.iter().map(|p| p.shares.value()))
            .collect();
        let dropped: u64 = groups.iter().map(|g| u64::from(g.dup_cross_token)).sum::<u64>()
            + stats.late_cross_token_dups;
        prop_assert_eq!(seen.len() as u64 + dropped, prints.len() as u64);
        seen.sort();
        let before = seen.len();
        seen.dedup();
        prop_assert_eq!(seen.len(), before, "a print is in two groups");
        for g in &groups {
            prop_assert!(!g.prints.is_empty());
            prop_assert!(g.prints.iter().all(|p| p.condition_id == g.condition_id));
            prop_assert!(g.vwap().is_some());
        }
    }

    #[test]
    fn grouping_does_not_depend_on_message_boundaries(
        prints in arb_prints(),
        advances in prop::collection::vec((0usize..60, 0i64..400), 0..30),
    ) {
        let (plain, plain_stats) = run(&prints, &[]);
        let (chunked, chunked_stats) = run(&prints, &advances);
        prop_assert_eq!(plain, chunked);
        prop_assert_eq!(plain_stats, chunked_stats);
    }
}
