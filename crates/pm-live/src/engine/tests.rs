use marcasite::types::TokenId;
use pm_core::{
    archive::record::ConnSlotId,
    book::{BookSide, LevelChange, OrderBook},
    normalize::{TakerSide, TxHash},
    test_util::t,
    units::{Price, Shares, Usd},
};
use rust_decimal_macros::dec;

use super::*;
use crate::{
    config::EngineConfig,
    event::BackfillPrint,
    test_support::{book, condition, live_market, no, price_change, trade, yes},
};

const SEC: i64 = 1000;
const HOUR: i64 = 3_600_000;

struct Harness {
    engine: Engine,
}

fn slot() -> ConnSlotId {
    ConnSlotId::new("c-0001")
}

impl Harness {
    fn new() -> Self {
        Self::with(|_| {})
    }

    fn with(edit: impl FnOnce(&mut EngineConfig)) -> Self {
        let mut config = EngineConfig::default();
        edit(&mut config);
        Self {
            engine: Engine::new(config),
        }
    }

    fn feed(&mut self, at_ms: i64, event: Event) -> Vec<Action> {
        let envelope = self.engine.accept(Stamped::at(t(at_ms), event));
        self.engine.step(envelope).unwrap()
    }

    fn add(&mut self, at_ms: i64, markets: &[u8]) -> Vec<Action> {
        let add = markets.iter().map(|&n| live_market(n)).collect();
        self.feed(
            at_ms,
            Event::RegistryUpdate {
                add,
                remove: vec![],
                changed: vec![],
            },
        )
    }

    fn up(&mut self, at_ms: i64, tokens: &[TokenId]) -> Vec<Action> {
        self.feed(
            at_ms,
            Event::ConnUp {
                slot: slot(),
                tokens: tokens.to_vec(),
                attempt: 1,
            },
        )
    }

    fn snapshot(&mut self, at_ms: i64, token: &TokenId) -> Vec<Action> {
        self.feed(
            at_ms,
            Event::WsBook {
                slot: slot(),
                book: book(
                    token,
                    &[("0.40", "100"), ("0.39", "50")],
                    &[("0.42", "80")],
                    at_ms - 5,
                ),
            },
        )
    }

    fn trusted(&self, token: &TokenId) -> bool {
        self.engine.book(token).unwrap().is_trusted()
    }

    fn distrust(&self, token: &TokenId) -> Option<Distrust> {
        self.engine.book(token).unwrap().distrust()
    }
}

fn persisted(actions: &[Action]) -> Vec<&Record> {
    actions
        .iter()
        .filter_map(|a| match a {
            Action::Persist(record) => Some(record),
            _ => None,
        })
        .collect()
}

fn metrics<'a>(actions: &'a [Action], name: &str) -> Vec<&'a Metric> {
    persisted(actions)
        .into_iter()
        .filter_map(|r| match r {
            Record::Metric(m) if m.name == name => Some(m),
            _ => None,
        })
        .collect()
}

#[test]
fn new_markets_open_a_slot_and_store_market_and_fee_rows() {
    let mut h = Harness::new();
    let actions = h.add(0, &[1, 2]);
    let open: Vec<_> = actions
        .iter()
        .filter(|a| matches!(a, Action::OpenSlot { .. }))
        .collect();
    assert_eq!(
        open,
        [&Action::OpenSlot {
            slot: slot(),
            tokens: vec![yes(1), no(1), yes(2), no(2)]
                .into_iter()
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect(),
        }]
    );
    let records = persisted(&actions);
    assert_eq!(
        records
            .iter()
            .filter(|r| matches!(r, Record::MarketFees { .. }))
            .count(),
        2
    );
    assert_eq!(
        records
            .iter()
            .filter(|r| matches!(r, Record::Market { .. }))
            .count(),
        2
    );
    assert_eq!(h.distrust(&yes(1)), Some(Distrust::AwaitingSnapshot));
    // A third market fits on the same slot: a subscribe, not a new slot.
    let actions = h.add(10, &[3]);
    assert!(actions.contains(&Action::Subscribe {
        slot: slot(),
        tokens: vec![yes(3), no(3)],
    }));
}

#[test]
fn slots_hold_at_most_tokens_per_connection() {
    let mut h = Harness::with(|c| c.tokens_per_connection = 4);
    let actions = h.add(0, &[1, 2, 3]);
    let opened: Vec<_> = actions
        .iter()
        .filter_map(|a| match a {
            Action::OpenSlot { slot, tokens } => Some((slot.clone(), tokens.len())),
            _ => None,
        })
        .collect();
    assert_eq!(
        opened,
        [
            (ConnSlotId::new("c-0001"), 4),
            (ConnSlotId::new("c-0002"), 2)
        ]
    );
}

#[test]
fn book_maintenance_matches_a_rebuild_from_the_same_messages() {
    let mut h = Harness::new();
    h.add(0, &[1]);
    h.up(10, &[yes(1), no(1)]);
    let snapshot = book(&yes(1), &[("0.40", "100")], &[("0.42", "80")], 15);
    h.feed(
        20,
        Event::WsBook {
            slot: slot(),
            book: snapshot.clone(),
        },
    );
    let mut expected = OrderBook::from_ws_book(&snapshot);
    for i in 0..50i64 {
        let price = format!("0.{}", 30 + (i % 20));
        let size = if i % 7 == 0 {
            "0".to_string()
        } else {
            (i * 3).to_string()
        };
        let side = if i % 20 < 11 { "BUY" } else { "SELL" };
        let event = price_change(&[(&yes(1), &price, &size, side)], 100 + i);
        for change in &event.price_changes {
            expected
                .apply_in_place(&LevelChange::from_price_change(change, event.timestamp).unwrap());
        }
        h.feed(
            100 + i,
            Event::WsPriceChange {
                slot: slot(),
                change: event,
            },
        );
    }
    let current = h.engine.book(&yes(1)).unwrap().current().unwrap();
    assert!(
        current.book.same_levels(&expected),
        "{:?}",
        current.book.differences(&expected)
    );
    assert!(h.trusted(&yes(1)));
}

#[test]
fn trust_across_a_reconnect() {
    let mut h = Harness::new();
    h.add(0, &[1]);
    h.up(10, &[yes(1), no(1)]);
    h.snapshot(20, &yes(1));
    h.snapshot(20, &no(1));
    assert!(h.trusted(&yes(1)) && h.trusted(&no(1)));
    h.feed(
        5 * SEC,
        Event::ConnDown {
            slot: slot(),
            tokens: vec![yes(1), no(1)],
            reason: Some("reset".into()),
        },
    );
    assert_eq!(h.distrust(&yes(1)), Some(Distrust::Disconnected));
    h.up(6 * SEC, &[yes(1), no(1)]);
    assert_eq!(h.distrust(&yes(1)), Some(Distrust::AwaitingSnapshot));
    assert_eq!(
        h.engine.book(&yes(1)).unwrap().untrusted_since(),
        Some(t(5 * SEC))
    );
    h.snapshot(8 * SEC, &yes(1));
    assert!(h.trusted(&yes(1)));
    assert!(!h.trusted(&no(1)), "the other token waits for its own book");
}

#[test]
fn a_decode_error_distrusts_the_connection_and_resnapshots_after_the_delay() {
    let mut h = Harness::new();
    h.add(0, &[1, 2]);
    h.up(10, &[yes(1), no(1), yes(2), no(2)]);
    for token in [yes(1), no(1), yes(2), no(2)] {
        h.snapshot(20, &token);
    }
    h.feed(
        SEC,
        Event::ConnDecodeError {
            slot: slot(),
            tokens: vec![],
            error: "bad".into(),
        },
    );
    assert!(
        [yes(1), no(1), yes(2), no(2)]
            .iter()
            .all(|token| h.distrust(token) == Some(Distrust::DecodeError))
    );
    // One token gets a book on its own (a trade happened).
    h.snapshot(3 * SEC, &yes(2));
    assert!(h.feed(SEC + 9_999, Event::Tick).is_empty());
    let actions = h.feed(SEC + 10_000, Event::Tick);
    let margin = volume::BACKFILL_MARGIN;
    assert_eq!(
        actions,
        [
            Action::Resnapshot {
                slot: slot(),
                tokens: vec![yes(1), no(1), no(2)]
            },
            // A trade between the unsubscribe and the subscribe would be lost.
            Action::RequestBackfill {
                conditions: vec![condition(1), condition(2)],
                from: t(11 * SEC) - margin,
                until: t(11 * SEC) + margin,
            }
        ]
    );
    for token in [yes(1), no(1), no(2)] {
        h.snapshot(12 * SEC, &token);
    }
    assert!(
        [yes(1), no(1), yes(2), no(2)]
            .iter()
            .all(|token| h.trusted(token))
    );
}

#[test]
fn repeated_decode_errors_are_coalesced_into_one_resnapshot_and_backfill() {
    let mut h = Harness::new();
    h.add(0, &[1]);
    let now = 30 * HOUR;
    h.up(now, &[yes(1), no(1)]);
    let error = |h: &mut Harness, at: i64| {
        h.feed(
            at,
            Event::ConnDecodeError {
                slot: slot(),
                tokens: vec![],
                error: "bad".into(),
            },
        )
    };
    let backfills = |actions: &[Action]| {
        actions
            .iter()
            .filter(|a| matches!(a, Action::RequestBackfill { .. }))
            .count()
    };
    assert_eq!(
        backfills(&error(&mut h, now + SEC)),
        1,
        "the first asks at once"
    );
    for at in [2, 3, 4] {
        assert!(error(&mut h, now + at * SEC).is_empty(), "later ones wait");
    }
    let margin = volume::BACKFILL_MARGIN;
    let actions = h.feed(now + 11 * SEC, Event::Tick);
    assert_eq!(
        actions,
        [
            Action::Resnapshot {
                slot: slot(),
                tokens: vec![yes(1), no(1)]
            },
            Action::RequestBackfill {
                conditions: vec![condition(1)],
                from: t(now + 2 * SEC) - margin,
                until: t(now + 11 * SEC) + margin,
            }
        ]
    );
    // The window opens again: the next error asks at once.
    assert_eq!(backfills(&error(&mut h, now + 12 * SEC)), 1);
}

#[test]
fn a_market_already_tracked_or_sharing_a_token_is_not_added() {
    let mut h = Harness::new();
    h.add(0, &[1]);
    let mut clash = live_market(2);
    clash.info.tokens.no = yes(1);
    let actions = h.feed(
        10,
        Event::RegistryUpdate {
            add: vec![live_market(1), clash.clone()],
            remove: vec![],
            changed: vec![],
        },
    );
    assert!(
        actions
            .iter()
            .all(|a| !matches!(a, Action::Subscribe { .. }))
    );
    assert!(h.engine.market(&condition(2)).is_none());
    // Removing the market that was never added leaves the other's token alone.
    h.feed(
        20,
        Event::RegistryUpdate {
            add: vec![],
            remove: vec![RemovedMarket {
                condition_id: condition(2),
                tokens: clash.info.tokens.clone(),
            }],
            changed: vec![],
        },
    );
    assert!(h.engine.book(&yes(1)).is_some());
    assert!(h.engine.market(&condition(1)).is_some());
}

#[test]
fn only_the_first_resolution_of_a_tracked_market_is_recorded() {
    let mut h = Harness::new();
    h.add(0, &[1]);
    let resolved = |market: ConditionId| Event::WsMarketResolved {
        slot: slot(),
        resolved: Box::new(
            serde_json::from_value(serde_json::json!({
                "id": "1",
                "market": market.to_string(),
                "assets_ids": [yes(1).to_string(), no(1).to_string()],
                "winning_asset_id": yes(1).to_string(),
                "winning_outcome": "Yes",
                "timestamp": "99999999999999",
            }))
            .unwrap(),
        ),
    };
    let untracked = h.feed(SEC, resolved(ConditionId::new("0xother")));
    assert!(persisted(&untracked).is_empty());
    let first = h.feed(2 * SEC, resolved(condition(1)));
    assert!(
        first.contains(&Action::RegistryHint(RegistryHint::Resolved {
            condition_id: condition(1),
            at: t(2 * SEC),
        })),
        "a future exchange time is capped at now"
    );
    let again = h.feed(3 * SEC, resolved(condition(1)));
    assert!(
        again.is_empty(),
        "the copy from another connection is dropped"
    );
}

#[test]
fn a_disconnect_covers_tokens_the_event_does_not_list() {
    let mut h = Harness::new();
    h.add(0, &[1]);
    let now = 30 * HOUR;
    h.up(now, &[yes(1), no(1)]);
    // Market 2 joins the connected slot; the connection's own list lags behind.
    h.add(now + SEC, &[2]);
    h.feed(
        now + 2 * SEC,
        Event::ConnDown {
            slot: slot(),
            tokens: vec![yes(1), no(1)],
            reason: None,
        },
    );
    assert_eq!(h.distrust(&yes(2)), Some(Distrust::Disconnected));
    let actions = h.up(now + 600 * SEC, &[yes(1), no(1), yes(2), no(2)]);
    let gap = actions.iter().find_map(|a| match a {
        Action::RequestBackfill { conditions, .. } => Some(conditions.clone()),
        _ => None,
    });
    assert_eq!(gap, Some(vec![condition(1), condition(2)]));
}

#[test]
fn no_resnapshot_while_the_connection_is_down() {
    let mut h = Harness::new();
    h.add(0, &[1]);
    h.up(10, &[yes(1), no(1)]);
    h.feed(
        SEC,
        Event::ConnDecodeError {
            slot: slot(),
            tokens: vec![yes(1), no(1)],
            error: "bad".into(),
        },
    );
    h.feed(
        2 * SEC,
        Event::ConnDown {
            slot: slot(),
            tokens: vec![yes(1), no(1)],
            reason: None,
        },
    );
    assert!(h.feed(20 * SEC, Event::Tick).is_empty());
}

#[test]
fn removed_markets_unsubscribe_and_an_empty_slot_closes() {
    let mut h = Harness::new();
    h.add(0, &[1, 2]);
    let remove = |n: u8| crate::event::RemovedMarket {
        condition_id: condition(n),
        tokens: live_market(n).info.tokens,
    };
    let actions = h.feed(
        10,
        Event::RegistryUpdate {
            add: vec![],
            remove: vec![remove(1)],
            changed: vec![],
        },
    );
    assert_eq!(
        actions,
        [Action::Unsubscribe {
            slot: slot(),
            tokens: vec![yes(1), no(1)]
        }]
    );
    assert!(h.engine.book(&yes(1)).is_none() && h.engine.market(&condition(1)).is_none());
    let actions = h.feed(
        20,
        Event::RegistryUpdate {
            add: vec![],
            remove: vec![remove(2)],
            changed: vec![],
        },
    );
    assert_eq!(actions, [Action::CloseSlot { slot: slot() }]);
    assert!(h.engine.slots().is_empty());
}

#[test]
fn a_fee_change_is_stored_and_details_only_when_changed() {
    let mut h = Harness::new();
    h.add(0, &[1]);
    let mut market = live_market(1);
    market.fees.rate = dec!(0.01);
    let actions = h.feed(
        10,
        Event::RegistryUpdate {
            add: vec![],
            remove: vec![],
            changed: vec![MarketChanged {
                market,
                details_changed: false,
                fee_changed: true,
            }],
        },
    );
    let records = persisted(&actions);
    assert_eq!(records.len(), 1);
    assert!(matches!(records[0], Record::MarketFees { fees, .. } if fees.rate == dec!(0.01)));
    assert_eq!(
        h.engine.market(&condition(1)).unwrap().fees.rate,
        dec!(0.01)
    );
}

#[test]
fn backfill_is_requested_on_first_connect_and_merged() {
    let mut h = Harness::new();
    h.add(0, &[1, 2]);
    let now = 30 * HOUR;
    let actions = h.up(now, &[yes(1), no(1), yes(2), no(2)]);
    let actions: Vec<_> = actions
        .into_iter()
        .filter(|a| matches!(a, Action::RequestBackfill { .. }))
        .collect();
    let lookback = TimeDelta::hours(24) + TimeDelta::minutes(10);
    assert_eq!(
        actions,
        [Action::RequestBackfill {
            conditions: vec![condition(1), condition(2)],
            from: t(now) - lookback,
            until: t(now) + volume::BACKFILL_MARGIN,
        }]
    );
    // A live trade, then the backfill (which also reports it, stamped to the second).
    h.feed(
        now + 500,
        Event::WsTrade {
            slot: slot(),
            trade: trade(
                &condition(1),
                &yes(1),
                "BUY",
                "0.5",
                "10",
                "0xaa",
                now + 400,
            ),
        },
    );
    let print = |hash: &str, at_ms: i64| BackfillPrint {
        token: yes(1),
        side: TakerSide::Sell,
        price: Price::new(dec!(0.8)).unwrap(),
        size: Shares::new(dec!(10)).unwrap(),
        tx_hash: TxHash::new(hash),
        timestamp: t(at_ms),
    };
    h.feed(
        now + 2 * SEC,
        Event::VolumeBackfill {
            condition: condition(1),
            prints: vec![print("0xaa", now), print("0xbb", now - HOUR)],
            complete: true,
            from: t(now) - lookback,
            until: t(now) + volume::BACKFILL_MARGIN,
        },
    );
    // 10 × 0.5 live + a sell of YES at 0.8 = a buy of NO at 0.2: 10 × 0.2.
    let (total, complete) = h
        .engine
        .volume(&condition(1), t(now + 2 * SEC))
        .unwrap()
        .unwrap();
    assert_eq!(total, Usd::new(dec!(7)));
    assert!(complete);
    // Market 2 has no backfill yet: incomplete.
    assert!(
        !h.engine
            .volume(&condition(2), t(now + 2 * SEC))
            .unwrap()
            .unwrap()
            .1
    );
    // A reconnect without a disconnect asks for nothing more.
    assert!(
        h.up(now + 3 * SEC, &[yes(1), no(1)])
            .iter()
            .all(|a| !matches!(a, Action::RequestBackfill { .. }))
    );
}

#[test]
fn ordering_of_trade_and_price_change_is_measured() {
    let mut h = Harness::new();
    h.add(0, &[1]);
    h.up(10, &[yes(1), no(1)]);
    h.snapshot(20, &yes(1));
    h.feed(
        SEC,
        Event::WsPriceChange {
            slot: slot(),
            change: price_change(&[(&yes(1), "0.42", "30", "SELL")], SEC - 2),
        },
    );
    let actions = h.feed(
        SEC + 5,
        Event::WsTrade {
            slot: slot(),
            trade: trade(&condition(1), &yes(1), "BUY", "0.42", "50", "0x1", SEC - 1),
        },
    );
    let seen = metrics(&actions, "ws_trade_vs_price_change_ms");
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].labels["order"], "price_change_first");
    assert_eq!(seen[0].value, -1.0);
    // A trade nobody matches is reported unmatched once the window has passed.
    h.feed(
        2 * SEC,
        Event::WsTrade {
            slot: slot(),
            trade: trade(&condition(1), &yes(1), "BUY", "0.55", "1", "0x2", 2 * SEC),
        },
    );
    let actions = h.feed(5 * SEC, Event::Tick);
    let unmatched = metrics(&actions, "ws_trade_vs_price_change_unmatched");
    assert_eq!(unmatched.len(), 1);
    assert_eq!(unmatched[0].labels["order"], "unmatched");
    assert!(
        unmatched[0].value.is_finite(),
        "rows must serialise and replay"
    );
}

#[test]
fn a_token_untrusted_too_long_while_connected_raises_one_alarm() {
    let mut h = Harness::new();
    h.add(0, &[1]);
    h.up(10, &[yes(1), no(1)]);
    h.snapshot(20, &no(1));
    let alarms = |actions: &[Action]| metrics(actions, "untrusted_token_alarm").len();
    assert_eq!(alarms(&h.feed(119 * SEC, Event::Tick)), 0);
    let actions = h.feed(121 * SEC, Event::Tick);
    assert_eq!(alarms(&actions), 1);
    assert_eq!(
        metrics(&actions, "untrusted_token_alarm")[0].labels["token"],
        yes(1).to_string()
    );
    assert_eq!(
        alarms(&h.feed(200 * SEC, Event::Tick)),
        0,
        "once per episode"
    );
    // While disconnected there is no alarm.
    h.feed(
        201 * SEC,
        Event::ConnDown {
            slot: slot(),
            tokens: vec![yes(1), no(1)],
            reason: None,
        },
    );
    assert_eq!(alarms(&h.feed(400 * SEC, Event::Tick)), 0);
}

#[test]
fn counters_become_metrics_each_interval() {
    let mut h = Harness::new();
    h.add(0, &[1]);
    h.up(10, &[yes(1), no(1)]);
    h.feed(
        20,
        Event::WsDropped {
            slot: slot(),
            best_bid_ask: 7,
            unknown: 1,
        },
    );
    let actions = h.feed(61 * SEC, Event::Tick);
    let dropped = metrics(&actions, "ws_dropped");
    assert!(
        dropped
            .iter()
            .any(|m| m.labels["kind"] == "best_bid_ask" && m.value == 7.0)
    );
    assert_eq!(metrics(&actions, "tokens_untrusted")[0].value, 2.0);
    assert_eq!(metrics(&actions, "slots_connected")[0].value, 1.0);
}

#[test]
fn envelopes_must_come_in_sequence() {
    let mut h = Harness::new();
    let envelope = h.engine.accept(Stamped::at(t(0), Event::Tick));
    let mut skipped = envelope.clone();
    skipped.seq += 1;
    assert!(h.engine.step(skipped).is_err());
    assert!(h.engine.step(envelope).is_ok());
}

#[test]
fn price_changes_on_both_tokens_update_both_books() {
    let mut h = Harness::new();
    h.add(0, &[1]);
    h.up(10, &[yes(1), no(1)]);
    h.snapshot(20, &yes(1));
    h.snapshot(20, &no(1));
    h.feed(
        SEC,
        Event::WsPriceChange {
            slot: slot(),
            change: price_change(
                &[(&yes(1), "0.41", "5", "BUY"), (&no(1), "0.59", "5", "SELL")],
                SEC,
            ),
        },
    );
    let size = |token: &TokenId, side, price| {
        h.engine
            .book(token)
            .unwrap()
            .current()
            .unwrap()
            .book
            .size_at(side, price)
    };
    assert_eq!(size(&yes(1), BookSide::Bid, dec!(0.41)), Some(dec!(5)));
    assert_eq!(size(&no(1), BookSide::Ask, dec!(0.59)), Some(dec!(5)));
    assert_eq!(h.engine.book(&yes(1)).unwrap().versions().count(), 2);
}

#[test]
fn markets_added_to_a_connected_slot_start_live_volume_at_once() {
    let mut h = Harness::new();
    h.add(0, &[1]);
    h.up(10, &[yes(1), no(1)]);
    let actions = h.add(HOUR, &[2]);
    let lookback = TimeDelta::hours(24) + TimeDelta::minutes(10);
    assert!(actions.contains(&Action::Subscribe {
        slot: slot(),
        tokens: vec![yes(2), no(2)]
    }));
    assert!(actions.contains(&Action::RequestBackfill {
        conditions: vec![condition(2)],
        from: t(HOUR) - lookback,
        until: t(HOUR) + volume::BACKFILL_MARGIN,
    }));
    // While the slot is down, the next ConnUp starts them instead.
    h.feed(
        HOUR + 1,
        Event::ConnDown {
            slot: slot(),
            tokens: vec![yes(1), no(1), yes(2), no(2)],
            reason: None,
        },
    );
    let actions = h.add(HOUR + 2, &[3]);
    assert!(
        !actions
            .iter()
            .any(|a| matches!(a, Action::RequestBackfill { .. }))
    );
    let actions = h.up(HOUR + 3, &[yes(1), no(1), yes(2), no(2), yes(3), no(3)]);
    let backfills: Vec<_> = actions
        .iter()
        .filter(|a| matches!(a, Action::RequestBackfill { .. }))
        .collect();
    assert_eq!(
        backfills.len(),
        2,
        "market 3's full window and the gap of 1 and 2: {backfills:?}"
    );
}

#[test]
fn a_trade_before_its_price_change_is_measured() {
    let mut h = Harness::new();
    h.add(0, &[1]);
    h.up(10, &[yes(1), no(1)]);
    h.snapshot(20, &yes(1));
    h.feed(
        SEC,
        Event::WsTrade {
            slot: slot(),
            trade: trade(&condition(1), &yes(1), "BUY", "0.42", "5", "0x1", SEC),
        },
    );
    let actions = h.feed(
        SEC + 3,
        Event::WsPriceChange {
            slot: slot(),
            change: price_change(&[(&yes(1), "0.42", "75", "SELL")], SEC + 2),
        },
    );
    let seen = metrics(&actions, "ws_trade_vs_price_change_ms");
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].labels["order"], "trade_first");
    assert_eq!(seen[0].value, 2.0);
}

#[test]
fn a_decode_error_refetches_the_moment_and_a_gap_starts_at_the_last_message() {
    let mut h = Harness::new();
    h.add(0, &[1]);
    let now = 30 * HOUR;
    h.up(now, &[yes(1), no(1)]);
    let margin = volume::BACKFILL_MARGIN;
    let actions = h.feed(
        now + 5 * SEC,
        Event::ConnDecodeError {
            slot: slot(),
            tokens: vec![],
            error: "bad".into(),
        },
    );
    assert!(actions.contains(&Action::RequestBackfill {
        conditions: vec![condition(1)],
        from: t(now + 5 * SEC) - margin,
        until: t(now + 5 * SEC) + margin,
    }));
    // The last message at +6 s; the loss noticed at +40 s (an idle timeout); back at +41 s.
    h.snapshot(now + 6 * SEC, &yes(1));
    h.feed(
        now + 40 * SEC,
        Event::ConnDown {
            slot: slot(),
            tokens: vec![yes(1), no(1)],
            reason: None,
        },
    );
    let actions = h.up(now + 41 * SEC, &[yes(1), no(1)]);
    assert!(actions.contains(&Action::RequestBackfill {
        conditions: vec![condition(1)],
        from: t(now + 6 * SEC) - margin,
        until: t(now + 41 * SEC) + margin,
    }));
}

#[test]
fn lifecycle_events_of_closed_slots_and_foreign_tokens_are_ignored() {
    let mut h = Harness::new();
    h.add(0, &[1]);
    h.up(10, &[yes(1), no(1)]);
    h.snapshot(20, &yes(1));
    // A closed slot's late events change nothing.
    let closed = ConnSlotId::new("c-0009");
    h.feed(
        30,
        Event::ConnDown {
            slot: closed.clone(),
            tokens: vec![yes(1)],
            reason: None,
        },
    );
    h.feed(
        40,
        Event::ConnUp {
            slot: closed,
            tokens: vec![yes(1)],
            attempt: 2,
        },
    );
    assert!(h.trusted(&yes(1)));
}
