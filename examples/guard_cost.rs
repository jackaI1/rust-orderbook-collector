//! Offline microbenchmark of the old per-poll set comparison, not end-to-end throughput.
use std::{collections::BTreeSet, hint::black_box, sync::Arc, time::Instant};
fn main() {
    let desired: BTreeSet<Arc<str>> = (0..17_000)
        .map(|n| Arc::from(format!("1{n:076}")))
        .collect();
    let requested = desired.clone();
    let polls = 10_000u64;
    let start = Instant::now();
    let mut changed = 0;
    for _ in 0..polls {
        changed += u64::from(black_box(&requested) != black_box(&desired));
    }
    let equality_ns = start.elapsed().as_nanos();
    let dirty = black_box(false);
    let start = Instant::now();
    for _ in 0..polls {
        changed += u64::from(black_box(dirty));
    }
    let flag_ns = start.elapsed().as_nanos();
    assert_eq!(changed, 0);
    println!(
        "{{\"assets\":17000,\"polls\":{polls},\"set_comparison_ns\":{equality_ns},\"cached_flag_ns\":{flag_ns}}}"
    );
}
