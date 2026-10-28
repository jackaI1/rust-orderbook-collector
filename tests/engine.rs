use orderbook_collector::{
    Side,
    adapter::Event,
    engine::{Engine, Record},
};
fn snap(t: i64, q: i64) -> Event {
    Event::Snapshot {
        symbol: "a".into(),
        timestamp_ms: t,
        bids: vec![(400000, q)],
        asks: vec![(600000, 2000000)],
    }
}
fn delta(t: i64, q: i64) -> Event {
    Event::Delta {
        symbol: "a".into(),
        timestamp_ms: t,
        side: Side::Buy,
        price: 400000,
        quantity: q,
    }
}
fn engine() -> Engine {
    let mut e = Engine::new(2, 10000);
    e.set_universe(["a".to_string()].into());
    e
}
#[test]
fn requires_snapshot_and_zero_deletes() {
    let mut e = engine();
    assert!(e.apply(0, delta(1, 1), 1).unwrap().is_empty());
    assert_eq!(e.apply(0, snap(2, 5), 2).unwrap().len(), 1);
    assert_eq!(e.apply(0, delta(3, 0), 3).unwrap().len(), 1);
    let r = e.snapshots(4);
    assert!(matches!(&r[0],Record::Snapshot(s) if s.bids.is_empty()));
}
#[test]
fn duplicate_is_noop_and_regression_invalidates() {
    let mut e = engine();
    e.apply(0, snap(2, 5), 2).unwrap();
    assert!(e.apply(0, delta(3, 5), 3).unwrap().is_empty());
    assert!(e.apply(0, delta(1, 10), 4).is_err());
    assert!(e.snapshots(60000).is_empty());
    assert!(e.apply(0, delta(5, 10), 5).unwrap().is_empty());
    assert_eq!(e.apply(0, snap(6, 10), 6).unwrap().len(), 1);
}
#[test]
fn failover_emits_standby_snapshot_before_delta() {
    let mut e = engine();
    e.apply(0, snap(10, 5), 10).unwrap();
    assert!(e.apply(1, snap(11, 8), 11).unwrap().is_empty());
    let r = e.disconnect(0, 12);
    assert!(matches!(&r[0],Record::Snapshot(s) if s.bids[0].quantity==8));
    let r = e.apply(1, delta(12, 7), 13).unwrap();
    assert!(matches!(&r[0],Record::Delta(d) if d.quantity==7));
}
#[test]
fn behind_standby_waits_until_exchange_watermark_is_caught_up() {
    let mut e = engine();
    e.apply(0, snap(20, 5), 20).unwrap();
    e.apply(1, snap(19, 8), 21).unwrap();
    assert!(e.disconnect(0, 22).is_empty());
    assert!(e.snapshots(23).is_empty());
    assert_eq!(e.ready_count(60_000), 0);
    let r = e.apply(1, delta(20, 9), 60_000).unwrap();
    assert!(
        matches!(&r[..], [Record::Snapshot(s)] if s.exchange_timestamp.timestamp_millis() == 20 && s.bids[0].quantity == 9)
    );
}
#[test]
fn removal_drops_books_and_late_frames() {
    let mut e = engine();
    e.apply(0, snap(1, 5), 1).unwrap();
    e.set_universe(Default::default());
    assert!(e.snapshots(60000).is_empty());
    assert!(e.apply(0, snap(2, 7), 2).unwrap().is_empty());
}
#[test]
fn quiet_books_remain_in_minute_snapshots_until_source_disconnects() {
    let mut e = engine();
    e.apply(0, snap(1, 5), 1).unwrap();
    for now in [60_000, 120_000, 3_600_000] {
        let r = e.snapshots(now);
        assert!(
            matches!(&r[..], [Record::Snapshot(s)] if s.received_at.timestamp_millis() == now && s.exchange_timestamp.timestamp_millis() == 1 && s.bids[0].quantity == 5)
        );
        assert_eq!(e.ready_count(now), 1);
        assert_eq!(e.coverage(now).ready, 1);
    }
    assert_eq!(e.metrics.invalidations, 0);
    assert!(e.disconnect(0, 3_600_001).is_empty());
    assert!(e.snapshots(3_660_000).is_empty());
    assert_eq!(e.ready_count(3_660_000), 0);
}
#[test]
fn equal_timestamp_changes_preserve_order_and_two_sides() {
    let mut e = engine();
    e.apply(0, snap(1, 5), 1).unwrap();
    e.apply(0, delta(1, 7), 2).unwrap();
    e.apply(
        0,
        Event::Delta {
            symbol: "a".into(),
            timestamp_ms: 1,
            side: Side::Sell,
            price: 600000,
            quantity: 0,
        },
        3,
    )
    .unwrap();
    let r = e.snapshots(4);
    assert!(matches!(&r[0],Record::Snapshot(s) if s.bids[0].quantity==7 && s.asks.is_empty()));
}
#[test]
fn resolved_tombstone_prevents_stale_discovery_readdition() {
    let mut e = engine();
    e.apply(0, snap(1, 5), 1).unwrap();
    e.apply(
        0,
        Event::Resolved {
            symbols: vec!["a".into()],
        },
        2,
    )
    .unwrap();
    e.set_universe(["a".to_string()].into());
    assert_eq!(e.symbol_count(), 0);
    assert!(e.apply(0, snap(3, 7), 3).unwrap().is_empty());
}
#[test]
fn disconnected_source_cannot_resume_with_delta() {
    let mut e = engine();
    e.apply(0, snap(1, 5), 1).unwrap();
    e.disconnect(0, 2);
    assert!(e.apply(0, delta(3, 7), 3).unwrap().is_empty());
    assert!(e.snapshots(4).is_empty());
}
#[test]
fn scoped_reset_promotes_healthy_replica_and_gates_old_source_until_snapshot() {
    let mut e = engine();
    e.apply(0, snap(10, 5), 10).unwrap();
    e.apply(1, snap(11, 8), 11).unwrap();
    let r = e.reset(0, &["a".to_string()].into(), 60_000);
    assert!(matches!(&r[0],Record::Snapshot(s) if s.bids[0].quantity==8));
    assert!(e.apply(0, delta(13, 99), 60_001).unwrap().is_empty());
    let r = e.snapshots(120_000);
    assert!(matches!(&r[0],Record::Snapshot(s) if s.bids[0].quantity==8));
}

#[test]
fn quiet_selected_book_is_not_replaced_by_standby_updates() {
    let mut e = engine();
    e.apply(0, snap(10, 5), 10).unwrap();
    e.apply(1, snap(11, 8), 11).unwrap();
    let r = e.apply(1, delta(12, 7), 10011).unwrap();
    assert!(r.is_empty());
    let r = e.snapshots(60_000);
    assert!(matches!(&r[..], [Record::Snapshot(s)] if s.bids[0].quantity == 5));
    let r = e.disconnect(0, 120_000);
    assert!(matches!(&r[..], [Record::Snapshot(s)] if s.bids[0].quantity == 7));
}
#[test]
fn quiet_replica_accepts_deltas_without_another_snapshot() {
    let mut e = engine();
    e.apply(0, snap(10, 5), 10).unwrap();
    let r = e.apply(0, delta(11, 7), 60_000).unwrap();
    assert!(matches!(&r[..], [Record::Delta(d)] if d.quantity == 7));
    let r = e.snapshots(120_000);
    assert!(matches!(&r[..], [Record::Snapshot(s)] if s.bids[0].quantity == 7));
    assert_eq!(e.metrics.invalidations, 0);
}

#[test]
fn coverage_counts_replica_readiness_without_hiding_missing_books() {
    let mut e = Engine::new(8, 10000);
    e.set_universe(["a".to_string(), "b".to_string()].into());
    e.apply(0, snap(1, 5), 1).unwrap();
    e.apply(1, snap(1, 5), 1).unwrap();
    let h = e.coverage(2);
    assert_eq!(h.symbols, 2);
    assert_eq!(h.ready, 1);
    assert_eq!(h.redundant_ready, 1);
    assert_eq!(h.uninitialized, 1);
    assert_eq!(h.replica_books, 2);
    assert_eq!(h.uninitialized_sample, vec!["b"]);
    let h = e.coverage(10002);
    assert_eq!(h.ready, 1);
    assert_eq!(h.redundant_ready, 1);
    assert_eq!(h.stale, 0);
    assert_eq!(h.uninitialized, 1);
    assert_eq!(h.replica_books, 2);
    assert_eq!(h.uninitialized_sample, vec!["b"]);
}

#[test]
fn quiet_standby_at_watermark_can_be_promoted() {
    let mut e = engine();
    e.apply(0, snap(20, 5), 20).unwrap();
    e.apply(1, snap(20, 8), 21).unwrap();
    let r = e.disconnect(0, 3_600_000);
    assert!(
        matches!(&r[..], [Record::Snapshot(s)] if s.exchange_timestamp.timestamp_millis() == 20 && s.bids[0].quantity == 8)
    );
    assert_eq!(e.ready_count(3_660_000), 1);
    assert_eq!(e.metrics.failovers, 1);
}

#[test]
fn backwards_receipt_clock_preserves_stream_order_and_actual_timestamps() {
    let mut e = engine();
    e.apply(0, snap(10, 5), 100_000).unwrap();
    for (timestamp, received, quantity) in [(11, 90_000, 7), (11, 85_000, 8), (12, 80_000, 9)] {
        let r = e.apply(0, delta(timestamp, quantity), received).unwrap();
        assert!(
            matches!(&r[..], [Record::Delta(d)] if d.backtest_timestamp.timestamp_millis() == received && d.exchange_timestamp.timestamp_millis() == timestamp && d.quantity == quantity)
        );
    }
    assert_eq!(e.ready_count(70_000), 1);
    assert_eq!(e.coverage(70_000).ready, 1);
    let r = e.snapshots(70_000);
    assert!(
        matches!(&r[..], [Record::Snapshot(s)] if s.received_at.timestamp_millis() == 70_000 && s.exchange_timestamp.timestamp_millis() == 12 && s.bids[0].quantity == 9)
    );
    assert_eq!(e.metrics.invalidations, 0);
    assert!(e.apply(0, delta(11, 99), 70_001).is_err());
    assert_eq!(e.metrics.invalidations, 1);
    assert!(e.snapshots(70_002).is_empty());
}

#[test]
fn backwards_receipt_clock_does_not_exclude_healthy_standby() {
    let mut e = engine();
    e.apply(0, snap(10, 5), 100_000).unwrap();
    e.apply(1, snap(11, 8), 100_001).unwrap();
    let r = e.disconnect(0, 90_000);
    assert!(
        matches!(&r[..], [Record::Snapshot(s)] if s.received_at.timestamp_millis() == 90_000 && s.bids[0].quantity == 8)
    );
    assert_eq!(e.coverage(90_000).ready, 1);
}

#[test]
fn backwards_receipt_clock_preserves_snapshot_receipt_time() {
    let mut e = engine();
    e.apply(0, snap(10, 5), 100_000).unwrap();
    let r = e.apply(0, snap(11, 8), 90_000).unwrap();
    assert!(
        matches!(&r[..], [Record::Snapshot(s)] if s.received_at.timestamp_millis() == 90_000 && s.exchange_timestamp.timestamp_millis() == 11 && s.bids[0].quantity == 8)
    );
    assert_eq!(e.metrics.invalidations, 0);
}

#[test]
fn snapshot_sorts_levels_and_deltas_set_absolute_quantities() {
    let mut e = engine();
    let r = e
        .apply(
            0,
            Event::Snapshot {
                symbol: "a".into(),
                timestamp_ms: 10,
                bids: vec![(300_000, 3), (400_000, 4), (200_000, 2)],
                asks: vec![(700_000, 7), (800_000, 8), (600_000, 6)],
            },
            10,
        )
        .unwrap();
    let Record::Snapshot(s) = &r[0] else {
        panic!("expected initial snapshot")
    };
    assert_eq!(
        s.bids.iter().map(|b| b.price_level).collect::<Vec<_>>(),
        vec![400_000, 300_000, 200_000]
    );
    assert_eq!(
        s.asks.iter().map(|a| a.price_level).collect::<Vec<_>>(),
        vec![600_000, 700_000, 800_000]
    );
    e.apply(0, delta(11, 7), 11).unwrap();
    let r = e.snapshots(12);
    assert!(matches!(&r[..], [Record::Snapshot(s)] if s.bids[0].quantity == 7));
    e.apply(0, delta(12, 0), 12).unwrap();
    let r = e.snapshots(13);
    assert!(
        matches!(&r[..], [Record::Snapshot(s)] if s.bids.len() == 2 && s.bids[0].price_level == 300_000)
    );
}

#[test]
fn all_sources_disconnected_leaves_no_ready_books() {
    let mut e = engine();
    e.apply(0, snap(10, 5), 10).unwrap();
    e.apply(1, snap(11, 8), 11).unwrap();
    assert_eq!(e.disconnect(0, 12).len(), 1);
    assert!(e.disconnect(1, 13).is_empty());
    assert!(e.snapshots(60_000).is_empty());
    assert_eq!(e.ready_count(60_000), 0);
    let h = e.coverage(60_000);
    assert_eq!((h.ready, h.redundant_ready, h.replica_books), (0, 0, 0));
    assert_eq!(h.uninitialized, 1);
    assert!(e.apply(0, delta(12, 99), 60_001).unwrap().is_empty());
    assert!(e.apply(1, delta(12, 99), 60_002).unwrap().is_empty());
    assert!(e.snapshots(60_003).is_empty());
}

#[test]
fn two_replica_maintenance_keeps_quiet_books_until_actual_disconnect() {
    let mut e = Engine::new(usize::MAX, 10_000);
    e.set_universe(["a".to_string()].into());
    e.apply(40, snap(20, 5), 20).unwrap();
    e.apply(41, snap(20, 8), 21).unwrap();
    e.set_universe(["a".to_string()].into());
    let r = e.snapshots(3_600_000);
    assert!(matches!(&r[..], [Record::Snapshot(s)] if s.bids[0].quantity == 5));
    assert_eq!(e.coverage(3_600_000).redundant_ready, 1);
    assert_eq!(e.metrics.invalidations, 0);

    // The source manager warms a new physical connection before retiring the old one.
    assert!(e.apply(42, snap(20, 9), 3_600_001).unwrap().is_empty());
    let r = e.disconnect(40, 3_600_002);
    assert!(matches!(&r[..], [Record::Snapshot(s)] if s.bids[0].quantity == 8));
    assert_eq!(e.coverage(3_600_002).replica_books, 2);
    let r = e.disconnect(41, 7_200_000);
    assert!(matches!(&r[..], [Record::Snapshot(s)] if s.bids[0].quantity == 9));
    assert_eq!(e.ready_count(7_200_000), 1);
    assert_eq!(e.metrics.failovers, 2);
}

#[test]
fn streaming_snapshots_match_legacy_order_and_stop_on_sink_error() {
    let mut e = engine();
    e.set_universe(["c".into(), "a".into(), "b".into()].into());
    for symbol in ["c", "a", "b"] {
        e.apply(
            0,
            Event::Snapshot {
                symbol: symbol.into(),
                timestamp_ms: 10,
                bids: vec![(300_000, 3), (400_000, 4)],
                asks: vec![(700_000, 7), (600_000, 6)],
            },
            10,
        )
        .unwrap();
    }
    let expected = e.snapshots(60_000);
    let mut streamed = Vec::new();
    assert_eq!(
        e.visit_snapshots(60_000, |record| {
            streamed.push(record);
            Ok::<_, &'static str>(())
        }),
        Ok(3)
    );
    assert_eq!(streamed, expected);
    let before = e.metrics.snapshots;
    let mut attempted = Vec::new();
    let result = e.visit_snapshots(120_000, |record| {
        let Record::Snapshot(snapshot) = record else {
            panic!("snapshot expected")
        };
        attempted.push(snapshot.symbol);
        if attempted.len() == 2 {
            Err("sink full")
        } else {
            Ok(())
        }
    });
    assert_eq!(result, Err("sink full"));
    assert_eq!(attempted, ["a", "b"]);
    assert_eq!(e.metrics.snapshots, before + 1);
    assert_eq!(e.ready_count(120_000), 3);
    assert_eq!(e.visit_snapshots(180_000, |_| Ok::<_, ()>(())), Ok(3));
}

#[test]
fn invalid_times_and_sources_do_not_corrupt_accepted_books() {
    let mut e = engine();
    e.apply(0, snap(10, 5), 10).unwrap();
    assert!(e.apply(2, delta(11, 99), 11).is_err());
    assert!(e.apply(0, delta(-1, 99), 11).is_err());
    assert!(e.apply(0, delta(11, 99), -1).is_err());
    assert!(e.apply(0, delta(i64::MAX, 99), 11).is_err());
    let before = e.metrics.snapshots;
    assert_eq!(
        e.visit_snapshots(-1, |_| -> Result<(), ()> {
            panic!("invalid clock must not emit")
        }),
        Ok(0)
    );
    assert_eq!(e.metrics.snapshots, before);
    assert!(matches!(&e.snapshots(60_000)[..], [Record::Snapshot(s)]
        if s.bids[0].quantity == 5 && s.exchange_timestamp.timestamp_millis() == 10));
    assert_eq!(e.metrics.invalidations, 0);
}

#[test]
fn regression_invalidates_only_its_source_and_keeps_watermark_guard() {
    let mut e = engine();
    e.apply(0, snap(20, 5), 20).unwrap();
    e.apply(1, snap(19, 8), 21).unwrap();
    assert!(e.apply(0, delta(19, 99), 22).is_err());
    assert_eq!(e.coverage(22).replica_books, 1);
    assert!(e.snapshots(60_000).is_empty());
    assert!(e.apply(0, delta(21, 99), 60_001).unwrap().is_empty());
    let r = e.apply(1, delta(20, 9), 60_002).unwrap();
    assert!(matches!(&r[..], [Record::Snapshot(s)] if s.bids[0].quantity == 9));
    assert_eq!(e.metrics.invalidations, 1);
}

#[test]
fn engine_catalog_and_tombstones_share_the_registered_asset() {
    use orderbook_collector::assets::{intern, prune};
    use std::sync::Arc;
    let asset = intern("engine-interning-lifecycle-test");
    let baseline = Arc::strong_count(&asset);
    let mut e = Engine::new(2, 0);
    e.set_universe([asset.to_string()].into());
    assert_eq!(Arc::strong_count(&asset), baseline + 1);
    e.set_universe([asset.to_string()].into());
    assert_eq!(Arc::strong_count(&asset), baseline + 1);
    e.set_universe_assets([asset.clone()].into());
    assert_eq!(Arc::strong_count(&asset), baseline + 1);
    e.apply(
        0,
        Event::Resolved {
            symbols: vec![asset.to_string()],
        },
        1,
    )
    .unwrap();
    assert_eq!(Arc::strong_count(&asset), baseline + 1);
    e.set_universe_assets([asset.clone()].into());
    assert_eq!(e.symbol_count(), 0);
    prune();
    assert!(Arc::ptr_eq(&asset, &intern(&asset)));
    drop(e);
    assert_eq!(Arc::strong_count(&asset), baseline);
}

#[test]
fn shared_catalog_reset_keeps_compatible_source_failover() {
    use orderbook_collector::assets::intern;
    let mut e = engine();
    e.apply(0, snap(10, 5), 10).unwrap();
    e.apply(1, snap(11, 8), 11).unwrap();
    let records = e.reset_assets(0, &[intern("a")].into(), 12);
    assert!(matches!(&records[..], [Record::Snapshot(s)] if s.bids[0].quantity == 8));
    assert!(e.apply(0, delta(12, 99), 13).unwrap().is_empty());
}

#[test]
fn resnapshot_comparison_distinguishes_same_timestamp_from_later_changes() {
    let mut engine = Engine::new(2, 0);
    engine.set_universe(std::collections::BTreeSet::from(["a".into()]));
    let book = |time, quantity| Event::Snapshot {
        symbol: "a".into(),
        timestamp_ms: time,
        bids: vec![(400_000, quantity)],
        asks: vec![],
    };
    engine.apply(0, book(1_000, 1_000_000), 1_000).unwrap();
    engine.apply(0, book(1_000, 1_000_000), 1_001).unwrap();
    engine.apply(0, book(1_000, 2_000_000), 1_002).unwrap();
    engine.apply(0, book(1_001, 3_000_000), 1_003).unwrap();
    assert_eq!(engine.metrics.resnapshots, 3);
    assert_eq!(engine.metrics.same_timestamp_book_matches, 1);
    assert_eq!(engine.metrics.same_timestamp_book_differences, 1);
}

#[test]
fn incremental_lifecycle_additions_preserve_books_and_resolution_tombstones() {
    let mut engine = Engine::new(2, 0);
    engine.set_universe(std::collections::BTreeSet::from(["a".into()]));
    engine
        .apply(
            0,
            Event::Snapshot {
                symbol: "a".into(),
                timestamp_ms: 1_000,
                bids: vec![(400_000, 1_000_000)],
                asks: vec![],
            },
            1_000,
        )
        .unwrap();
    engine.add_universe_assets([
        orderbook_collector::assets::intern("a"),
        orderbook_collector::assets::intern("b"),
    ]);
    assert_eq!(engine.symbol_count(), 2);
    assert_eq!(engine.ready_count(1_001), 1);
    let records = engine.snapshots(1_001);
    assert_eq!(records.len(), 1);
    let Record::Snapshot(book) = &records[0] else {
        panic!("snapshot")
    };
    assert_eq!(book.symbol, "a");
    assert_eq!(book.bids[0].quantity, 1_000_000);
    engine
        .apply(
            0,
            Event::Resolved {
                symbols: vec!["a".into()],
            },
            1_002,
        )
        .unwrap();
    engine.add_universe_assets([orderbook_collector::assets::intern("a")]);
    assert_eq!(engine.symbol_count(), 1);
}

fn symbol_snap(symbol: &str, timestamp_ms: i64, quantity: i64) -> Event {
    Event::Snapshot {
        symbol: symbol.into(),
        timestamp_ms,
        bids: vec![(400_000, quantity)],
        asks: vec![(600_000, quantity + 1)],
    }
}

#[test]
fn streaming_and_legacy_disconnects_are_scoped_to_physical_shards() {
    let mut e = Engine::new(usize::MAX, 0);
    e.set_universe(["a".into(), "b".into(), "c".into(), "d".into()].into());
    for (source, symbols, quantity) in [
        (40, ["b", "a"], 10),
        (41, ["a", "b"], 20),
        (usize::MAX - 2, ["c", "d"], 30),
        (usize::MAX - 1, ["d", "c"], 40),
    ] {
        for symbol in symbols {
            e.apply(source, symbol_snap(symbol, 10, quantity), 10)
                .unwrap();
        }
    }
    // Repeated snapshots must not duplicate index membership.
    e.apply(40, symbol_snap("a", 11, 11), 11).unwrap();
    e.apply(41, symbol_snap("a", 11, 21), 11).unwrap();
    let before = e.metrics.disconnect_books_visited;
    let mut records = Vec::new();
    assert_eq!(
        e.visit_disconnect(40, 12, |record| {
            records.push(record);
            Ok::<_, ()>(())
        }),
        Ok(2)
    );
    assert_eq!(e.metrics.disconnect_books_visited - before, 2);
    assert!(
        matches!(&records[..], [Record::Snapshot(a), Record::Snapshot(b)]
        if a.symbol == "a" && b.symbol == "b"
        && a.bids[0].quantity == 21 && b.bids[0].quantity == 20)
    );
    let other = e.disconnect(usize::MAX - 2, 12);
    assert!(
        matches!(&other[..], [Record::Snapshot(c), Record::Snapshot(d)]
        if c.symbol == "c" && d.symbol == "d"
        && c.bids[0].quantity == 40 && d.bids[0].quantity == 40)
    );
    assert_eq!(e.coverage(12).replica_books, 4);
    assert_eq!(e.ready_count(12), 4);
    let before = e.metrics.disconnect_books_visited;
    assert!(e.disconnect(40, 13).is_empty());
    assert!(e.disconnect(9_999, 13).is_empty());
    assert_eq!(e.metrics.disconnect_books_visited, before);
}

#[test]
fn disconnect_sink_failure_still_invalidates_all_books_and_keeps_healthy_replicas() {
    let mut e = Engine::new(4, 0);
    e.set_universe(["a".into(), "b".into(), "c".into(), "d".into()].into());
    for symbol in ["a", "b", "c", "d"] {
        e.apply(0, symbol_snap(symbol, 10, 10), 10).unwrap();
        if symbol != "d" {
            e.apply(1, symbol_snap(symbol, 11, 20), 11).unwrap();
        }
    }
    let before = e.metrics.snapshots;
    let mut attempted = Vec::new();
    assert_eq!(
        e.visit_disconnect(0, 12, |record| {
            let Record::Snapshot(snapshot) = record else {
                panic!("expected failover snapshot")
            };
            attempted.push(snapshot.symbol);
            if attempted.len() == 2 {
                Err("sink full")
            } else {
                Ok(())
            }
        }),
        Err("sink full")
    );
    assert_eq!(attempted, ["a", "b"]);
    assert_eq!(e.metrics.snapshots, before + 1);
    assert_eq!(e.metrics.invalidations, 4);
    assert_eq!(e.metrics.failovers, 3);
    assert_eq!(e.metrics.disconnect_books_visited, 4);
    assert_eq!(e.coverage(12).replica_books, 3);
    assert_eq!(e.ready_count(12), 3);
    for symbol in ["a", "b", "c", "d"] {
        let mut update = delta(13, 99);
        let Event::Delta { symbol: name, .. } = &mut update else {
            unreachable!()
        };
        *name = symbol.into();
        assert!(e.apply(0, update, 13).unwrap().is_empty());
    }
    assert_eq!(e.metrics.gated, 4);
    let records = e.snapshots(14);
    assert_eq!(records.len(), 3);
    for record in records {
        assert!(matches!(record, Record::Snapshot(s) if s.bids[0].quantity == 20));
    }
    assert!(e.disconnect(0, 15).is_empty());
    assert_eq!(e.metrics.disconnect_books_visited, 4);
    assert!(e.disconnect(1, 15).is_empty());
    assert_eq!(e.ready_count(15), 0);
}

#[test]
fn lifecycle_removals_clear_source_membership_and_release_asset_handles() {
    use orderbook_collector::assets::{intern, prune};
    use std::sync::Arc;
    let names = [
        "index-reset",
        "index-regression",
        "index-resolved",
        "index-strings",
        "index-assets",
    ];
    let assets: Vec<_> = names.iter().map(|name| intern(name)).collect();
    let probes: Vec<_> = assets.iter().map(Arc::downgrade).collect();
    let mut e = Engine::new(usize::MAX, 0);
    e.set_universe(names.iter().map(|s| s.to_string()).collect());
    drop(assets);
    for (offset, name) in names.iter().enumerate() {
        for source in [100 + offset, 200 + offset] {
            e.apply(source, symbol_snap(name, 10, 5), 10).unwrap();
        }
    }
    e.reset(100, &[names[0].into()].into(), 11);
    e.reset_assets(200, &[intern(names[0])].into(), 11);
    assert!(e.apply(101, symbol_snap(names[1], 9, 99), 11).is_err());
    assert!(e.apply(201, symbol_snap(names[1], 9, 99), 11).is_err());
    e.apply(
        0,
        Event::Resolved {
            symbols: vec![names[2].into()],
        },
        11,
    )
    .unwrap();
    e.set_universe(
        names[..3]
            .iter()
            .chain(&names[4..])
            .map(|s| s.to_string())
            .collect(),
    );
    e.set_universe_assets(names[..3].iter().map(|s| intern(s)).collect());
    // Catalog churn must not leave source entries pointing at removed states.
    let before = e.metrics.disconnect_books_visited;
    for offset in 0..5 {
        for source in [100 + offset, 200 + offset] {
            assert!(e.disconnect(source, 12).is_empty());
        }
    }
    assert_eq!(e.metrics.disconnect_books_visited, before);
    assert_eq!(e.coverage(12).replica_books, 0);
    // Re-add removed symbols; old physical sources must not regain their books.
    e.add_universe_assets(names.iter().map(|s| intern(s)));
    assert_eq!(e.symbol_count(), 4); // resolution tombstone survives discovery
    assert!(e.snapshots(13).is_empty());
    let mut update = delta(14, 99);
    let Event::Delta { symbol, .. } = &mut update else {
        unreachable!()
    };
    *symbol = names[0].into();
    assert!(e.apply(100, update, 14).unwrap().is_empty());
    assert_eq!(e.ready_count(14), 0);
    e.apply(100, symbol_snap(names[0], 15, 7), 15).unwrap();
    assert!(e.disconnect(100, 16).is_empty());
    assert_eq!(e.metrics.disconnect_books_visited, before + 1);
    e.set_universe(Default::default());
    prune();
    for (i, probe) in probes.iter().enumerate() {
        assert_eq!(
            probe.upgrade().is_some(),
            i == 2,
            "only tombstone is retained"
        );
    }
    drop(e);
    prune();
    assert!(probes.iter().all(|probe| probe.upgrade().is_none()));
}

#[test]
fn invalid_disconnect_time_preserves_index_for_a_later_valid_disconnect() {
    let mut e = engine();
    e.apply(0, snap(10, 5), 10).unwrap();
    e.apply(1, snap(11, 8), 11).unwrap();
    assert_eq!(
        e.visit_disconnect(0, -1, |_| -> Result<(), ()> {
            panic!("invalid timestamp must not emit")
        }),
        Ok(0)
    );
    assert_eq!(e.metrics.disconnect_books_visited, 0);
    assert_eq!(e.coverage(12).replica_books, 2);
    assert_eq!(e.disconnect(0, 12).len(), 1);
    assert_eq!(e.metrics.disconnect_books_visited, 1);
}
