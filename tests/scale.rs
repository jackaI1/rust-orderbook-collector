//! Offline full-catalog check. Opt in so normal test runs do not compete with
//! the live collector for memory: cargo test --release --test scale -- --ignored --nocapture
use orderbook_collector::{
    adapter::Event,
    assets::{Asset, intern, prune},
    engine::{Engine, Record},
};
use std::{collections::BTreeSet, time::Instant};

const SYMBOLS: usize = 554_212;
const RSS_BUDGET: u64 = 1024 * 1024 * 1024;

fn symbol(index: usize) -> String {
    // 77 decimal digits, beyond integer/floating-point identifier precision.
    format!("1{index:076}")
}

fn peak_rss_bytes() -> u64 {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: getrusage initializes the output on success; RUSAGE_SELF measures
    // only this test process, never the existing live collector.
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

fn report(phase: &str, started: Instant) {
    eprintln!(
        "scale {phase}: elapsed={:.3}s peak_rss={} bytes",
        started.elapsed().as_secs_f64(),
        peak_rss_bytes()
    );
}

#[test]
#[ignore = "554212 assets with two replicas; run explicitly and without competing scale tests"]
fn full_catalog_two_replicas_stream_minutes_with_bounded_output() {
    let started = Instant::now();
    let catalog: BTreeSet<Asset> = (0..SYMBOLS).map(|n| intern(&symbol(n))).collect();
    // Simulate persistent subscription sets: every owner shares the text
    // allocation, even though its own tree nodes still require memory.
    let subscriptions = [catalog.clone(), catalog.clone()];
    let mut engine = Engine::new(2, 0);
    engine.set_universe_assets(catalog.clone());
    report("catalog registered", started);
    for asset in &catalog {
        for source in 0..2 {
            let records = engine
                .apply(
                    source,
                    Event::Snapshot {
                        symbol: asset.to_string(),
                        timestamp_ms: 1_000,
                        bids: Vec::new(),
                        asks: Vec::new(),
                    },
                    1_000,
                )
                .unwrap();
            assert_eq!(records.len(), usize::from(source == 0));
        }
    }
    let coverage = engine.coverage(60_000);
    assert_eq!(coverage.symbols, SYMBOLS);
    assert_eq!(coverage.ready, SYMBOLS);
    assert_eq!(coverage.redundant_ready, SYMBOLS);
    assert_eq!(coverage.replica_books, 2 * SYMBOLS);
    assert_eq!(coverage.uninitialized, 0);
    report("two empty-book replicas loaded", started);
    assert!(
        peak_rss_bytes() < RSS_BUDGET,
        "empty-book fixture exceeded 1 GiB RSS budget"
    );

    // The sink owns only the current record and one previous identifier, not a
    // vector of 554212 snapshots. Compare complete streams across two minutes.
    let mut first_checksum = None;
    for minute in [60_000, 120_000] {
        let minute_started = Instant::now();
        let mut count = 0;
        let mut previous: Option<String> = None;
        let mut checksum = 0_u64;
        let emitted = engine
            .visit_snapshots(minute, |record| {
                let Record::Snapshot(snapshot) = record else {
                    panic!("minute must contain snapshots")
                };
                assert_eq!(snapshot.symbol.len(), 77);
                assert_eq!(snapshot.symbol, symbol(count));
                if let Some(previous) = &previous {
                    assert!(previous < &snapshot.symbol);
                }
                assert!(snapshot.bids.is_empty() && snapshot.asks.is_empty());
                assert_eq!(snapshot.received_at.timestamp_millis(), minute);
                assert_eq!(snapshot.exchange_timestamp.timestamp_millis(), 1_000);
                for byte in snapshot.symbol.bytes() {
                    checksum = checksum.wrapping_mul(31).wrapping_add(u64::from(byte));
                }
                previous = Some(snapshot.symbol);
                count += 1;
                Ok::<_, &'static str>(())
            })
            .unwrap();
        assert_eq!(emitted, SYMBOLS);
        assert_eq!(count, SYMBOLS);
        if let Some(first) = first_checksum {
            assert_eq!(checksum, first);
        }
        first_checksum = Some(checksum);
        eprintln!(
            "scale minute={minute}: count={emitted} elapsed={:.3}s checksum={checksum} peak_rss={} bytes",
            minute_started.elapsed().as_secs_f64(),
            peak_rss_bytes()
        );
    }

    let before = engine.metrics.snapshots;
    let mut attempted = 0;
    let result = engine.visit_snapshots(180_000, |_| {
        attempted += 1;
        if attempted == 1_025 {
            Err("simulated bounded sink failure")
        } else {
            Ok(())
        }
    });
    assert_eq!(result, Err("simulated bounded sink failure"));
    assert_eq!(attempted, 1_025);
    assert_eq!(engine.metrics.snapshots - before, 1_024);
    assert_eq!(engine.ready_count(180_000), SYMBOLS);
    assert!(
        peak_rss_bytes() < RSS_BUDGET,
        "streaming fixture exceeded 1 GiB RSS budget"
    );
    report("streams and backpressure verified", started);

    // A weak probe establishes that no registry/consumer strong owner survives
    // teardown + pruning. Its control block drops with the probe; allocator-held
    // RSS need not return to its baseline.
    let probe = std::sync::Arc::downgrade(catalog.first().unwrap());
    drop(engine);
    drop(subscriptions);
    drop(catalog);
    let removed = prune();
    assert!(removed >= SYMBOLS);
    assert!(probe.upgrade().is_none());
    report("fixture dropped and registry pruned", started);
}
