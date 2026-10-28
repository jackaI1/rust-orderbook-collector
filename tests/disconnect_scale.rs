//! Offline, opt-in disconnect benchmark. This is a 555000-symbol catalog with
//! 34000 populated symbols / 68000 nonempty replica books, not full live depth.
//! Run separately from other scale tests:
//! cargo test --release --test disconnect_scale -- --ignored --nocapture
use orderbook_collector::{
    adapter::Event,
    assets::{Asset, intern, prune},
    engine::{Engine, Record},
};
use std::{collections::BTreeSet, sync::Arc, time::Instant};

const SYMBOLS: usize = 555_000;
const SHARD_SYMBOLS: usize = 17_000;
const POPULATED: usize = 2 * SHARD_SYMBOLS;

fn symbol(index: usize) -> String {
    format!("2{index:076}")
}

fn peak_rss_bytes() -> u64 {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: getrusage initializes this output on success; only this fixture's
    // process is measured, never another worker or the user's applications.
    assert_eq!(
        unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) },
        0
    );
    let peak = unsafe { usage.assume_init() }.ru_maxrss as u64;
    if cfg!(target_os = "macos") {
        peak
    } else {
        peak * 1024
    }
}

#[test]
#[ignore = "555000-symbol catalog; run explicitly without competing scale fixtures"]
fn disconnect_visits_only_its_17000_nonempty_books_in_a_555000_symbol_catalog() {
    let started = Instant::now();
    let catalog: BTreeSet<Asset> = (0..SYMBOLS).map(|n| intern(&symbol(n))).collect();
    let probe = Arc::downgrade(catalog.first().unwrap());
    let mut engine = Engine::new(usize::MAX, 0);
    engine.set_universe_assets(catalog.clone());
    for (index, asset) in catalog.iter().take(POPULATED).enumerate() {
        let sources = if index < SHARD_SYMBOLS {
            [40, 41]
        } else {
            [80, 81]
        };
        for source in sources {
            engine
                .apply(
                    source,
                    Event::Snapshot {
                        symbol: asset.to_string(),
                        timestamp_ms: 1_000,
                        bids: vec![(400_000, source as i64)],
                        asks: vec![(600_000, source as i64 + 1)],
                    },
                    1_000,
                )
                .unwrap();
        }
    }
    let coverage = engine.coverage(1_001);
    assert_eq!(coverage.symbols, SYMBOLS);
    assert_eq!(coverage.ready, POPULATED);
    assert_eq!(coverage.replica_books, 2 * POPULATED);
    assert_eq!(coverage.uninitialized, SYMBOLS - POPULATED);

    let before = engine.metrics.disconnect_books_visited;
    let disconnect_started = Instant::now();
    let mut accepted = 0;
    let emitted = engine
        .visit_disconnect(40, 2_000, |record| {
            let Record::Snapshot(snapshot) = record else {
                panic!("expected failover snapshot")
            };
            assert_eq!(snapshot.symbol, symbol(accepted));
            assert_eq!(snapshot.bids.len(), 1);
            assert_eq!(snapshot.asks.len(), 1);
            assert_eq!(snapshot.bids[0].quantity, 41);
            assert_eq!(snapshot.asks[0].quantity, 42);
            assert_eq!(snapshot.received_at.timestamp_millis(), 2_000);
            assert_eq!(snapshot.exchange_timestamp.timestamp_millis(), 1_000);
            accepted += 1;
            Ok::<_, ()>(())
        })
        .unwrap();
    let disconnect_elapsed = disconnect_started.elapsed();
    let visits = engine.metrics.disconnect_books_visited - before;
    assert_eq!(emitted, SHARD_SYMBOLS);
    assert_eq!(accepted, SHARD_SYMBOLS);
    assert_eq!(visits, SHARD_SYMBOLS as u64);
    assert_eq!(engine.ready_count(2_000), POPULATED);
    assert_eq!(engine.coverage(2_000).replica_books, 3 * SHARD_SYMBOLS);
    assert!(engine.disconnect(40, 2_001).is_empty());
    assert_eq!(engine.metrics.disconnect_books_visited - before, visits);
    eprintln!(
        "disconnect scale: catalog={SYMBOLS} populated_symbols={POPULATED} initial_nonempty_books={} visits={visits} emitted={emitted} disconnect_seconds={:.6} setup_and_check_seconds={:.3} peak_rss_bytes={}",
        2 * POPULATED,
        disconnect_elapsed.as_secs_f64(),
        started.elapsed().as_secs_f64(),
        peak_rss_bytes()
    );
    assert!(
        peak_rss_bytes() < 1024 * 1024 * 1024,
        "bounded fixture exceeded 1 GiB RSS"
    );

    drop(engine);
    drop(catalog);
    assert!(prune() >= SYMBOLS);
    assert!(probe.upgrade().is_none());
}
