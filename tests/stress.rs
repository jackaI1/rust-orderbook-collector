use orderbook_collector::{
    Side,
    adapter::Event,
    engine::{Engine, Record},
};
#[test]
fn many_independent_books_and_replica_failovers_preserve_quantities() {
    let count = 2000;
    let mut e = Engine::new(8, 300000);
    e.set_universe((0..count).map(|n| format!("symbol-{n}")).collect());
    for n in 0..count {
        let symbol = format!("symbol-{n}");
        for replica in 0..2 {
            let r = e
                .apply(
                    replica,
                    Event::Snapshot {
                        symbol: symbol.clone(),
                        timestamp_ms: 1000,
                        bids: vec![(123456, n + 1)],
                        asks: vec![(654321, 1)],
                    },
                    1000,
                )
                .unwrap();
            assert_eq!(r.len(), usize::from(replica == 0));
        }
    }
    for t in 1001..1011 {
        for n in 0..count {
            for replica in 0..2 {
                e.apply(
                    replica,
                    Event::Delta {
                        symbol: format!("symbol-{n}"),
                        timestamp_ms: t,
                        side: Side::Buy,
                        price: 123456,
                        quantity: n + t,
                    },
                    t,
                )
                .unwrap();
            }
        }
    }
    let r = e.disconnect(0, 1011);
    assert_eq!(r.len(), count as usize);
    for record in r {
        let Record::Snapshot(s) = record else {
            panic!("failover must snapshot")
        };
        let n: i64 = s.symbol.strip_prefix("symbol-").unwrap().parse().unwrap();
        assert_eq!(s.bids[0].quantity, n + 1010);
    }
    assert_eq!(e.ready_count(1011), count as usize);
    assert_eq!(e.metrics.deltas, 20000);
    assert_eq!(e.metrics.failovers, 2000);
}
