use orderbook_collector::monitor::{Monitor, Reason};
use std::collections::BTreeSet;

fn monitor(stale_ms: u64) -> Monitor {
    Monitor::new(["a".to_owned()], stale_ms, 1_000)
}

// Three complete windows of exactly 100 valid book events each.
fn establish_rate(m: &mut Monitor, symbol: &str) {
    for window in 0..3 {
        for event in 0..100 {
            let now = window * 1_000 + event;
            m.observe(symbol, now == 0, now);
        }
    }
    assert!(m.due(3_000, 100).is_empty());
}

#[test]
fn continual_deltas_cannot_postpone_the_initial_snapshot_deadline() {
    let mut m = monitor(1_000);
    for now in [0, 200, 400, 600, 800, 999] {
        m.observe("a", false, now);
        assert!(m.due(now, 100).is_empty());
    }
    m.observe("a", false, 1_000);
    assert_eq!(m.due(1_000, 100), [("a".into(), Reason::Unsynchronized)]);
}

#[test]
fn only_a_snapshot_initializes_and_book_progress_extends_the_silence_deadline() {
    let mut m = monitor(1_000);
    m.observe("a", true, 900);
    assert!(m.due(1_000, 100).is_empty());
    m.observe("a", false, 1_800);
    assert!(m.due(2_799, 100).is_empty());
    assert_eq!(m.due(2_800, 100), [("a".into(), Reason::Silent)]);
}

#[test]
fn sparse_books_do_not_build_a_rate_drop_expectation() {
    let mut m = monitor(100_000);
    m.observe("a", true, 0);
    for now in 1..50 {
        m.observe("a", false, now * 1_000 - 1);
        assert!(m.due(now * 1_000, 100).is_empty());
    }
}

#[test]
fn sustained_rate_drop_recovers_with_nonzero_traffic_before_silence() {
    let mut m = monitor(100_000);
    establish_rate(&mut m, "a");
    m.observe("a", false, 3_500);
    assert!(m.due(4_000, 100).is_empty());
    m.observe("a", false, 4_500);
    assert!(m.due(4_999, 100).is_empty());
    assert_eq!(m.due(5_000, 100), [("a".into(), Reason::RateDrop)]);
}

#[test]
fn fewer_than_three_baseline_windows_cannot_trigger_rate_recovery() {
    let mut m = monitor(100_000);
    for now in 0..200 {
        m.observe("a", now == 0, (now / 100) * 1_000 + now % 100);
    }
    m.observe("a", false, 2_500);
    m.observe("a", false, 3_500);
    assert!(m.due(4_000, 100).is_empty());
    m.observe("a", false, 4_500);
    assert_eq!(m.due(5_000, 100), [("a".into(), Reason::RateDrop)]);
}

#[test]
fn twenty_percent_is_not_a_drop_and_a_healthy_window_breaks_the_streak() {
    let mut m = monitor(100_000);
    establish_rate(&mut m, "a");
    m.observe("a", false, 3_500);
    assert!(m.due(4_000, 100).is_empty());
    for event in 0..20 {
        m.observe("a", false, 4_000 + event);
    }
    assert!(m.due(5_000, 100).is_empty());
    m.observe("a", false, 5_500);
    assert!(m.due(6_000, 100).is_empty());
    m.observe("a", false, 6_500);
    assert_eq!(m.due(7_000, 100), [("a".into(), Reason::RateDrop)]);
}

#[test]
fn reset_restarts_the_snapshot_deadline_without_erasing_recovery_cooldown() {
    let mut m = monitor(1_000);
    assert_eq!(m.due(1_000, 100), [("a".into(), Reason::Unsynchronized)]);
    m.reset("a", 2_000);
    m.observe("a", false, 2_500);
    assert!(m.due(2_999, 100).is_empty());
    assert!(m.due(3_000, 100).is_empty());
    assert!(m.due(30_999, 100).is_empty());
    assert_eq!(m.due(31_000, 100), [("a".into(), Reason::Unsynchronized)]);
}

#[test]
fn reset_without_prior_recovery_grants_a_fresh_quiet_deadline() {
    let mut m = monitor(1_000);
    m.observe("a", true, 0);
    m.reset("a", 900);
    m.observe("a", false, 1_800);
    assert!(m.due(1_899, 100).is_empty());
    assert_eq!(m.due(1_900, 100), [("a".into(), Reason::Unsynchronized)]);
}

#[test]
fn reset_discards_learned_rate_expectations() {
    let mut m = monitor(100_000);
    establish_rate(&mut m, "a");
    m.reset("a", 3_000);
    m.observe("a", true, 3_000);
    for now in 4..40 {
        m.observe("a", false, now * 1_000 - 1);
        assert!(m.due(now * 1_000, 100).is_empty());
    }
}

#[test]
fn recovery_cooldown_is_at_least_the_stale_interval() {
    let mut m = monitor(60_000);
    assert_eq!(m.due(60_000, 100), [("a".into(), Reason::Unsynchronized)]);
    assert!(m.due(119_999, 100).is_empty());
    assert_eq!(m.due(120_000, 100), [("a".into(), Reason::Unsynchronized)]);
}

#[test]
fn batching_rotates_even_when_earlier_symbols_become_eligible_again() {
    let mut m = Monitor::new((0..205).map(|i| format!("s{i:03}")), 1, 1_000);
    assert!(m.due(1, 0).is_empty());
    let first = m.due(1, 100);
    let second = m.due(30_001, 100);
    let third = m.due(60_001, 100);
    assert_eq!((first.len(), second.len(), third.len()), (100, 100, 100));
    assert_eq!(first[0].0, "s000");
    assert_eq!(second[0].0, "s100");
    assert_eq!(third[0].0, "s200");
    let seen: BTreeSet<_> = first
        .into_iter()
        .chain(second)
        .chain(third)
        .map(|x| x.0)
        .collect();
    assert_eq!(seen.len(), 205);
}

#[test]
fn ignored_symbols_and_duplicate_configuration_do_not_create_extra_work() {
    let mut m = Monitor::new(["a".into(), "a".into()], 1_000, 1_000);
    m.observe("missing", true, 900);
    m.reset("missing", 900);
    assert_eq!(m.due(1_000, 100), [("a".into(), Reason::Unsynchronized)]);
    assert!(Monitor::new([], 1, 1).due(u64::MAX, 100).is_empty());
}

#[test]
fn a_delayed_poll_accounts_for_empty_windows_without_replaying_time() {
    let mut m = monitor(u64::MAX);
    establish_rate(&mut m, "a");
    assert_eq!(
        m.due(1_000_000_000_000, 100),
        [("a".into(), Reason::RateDrop)]
    );
}

#[test]
fn timestamp_limits_and_zero_window_do_not_overflow_or_panic() {
    let mut m = Monitor::new(["a".into()], 10, 0);
    m.observe("a", true, u64::MAX - 10);
    assert!(m.due(u64::MAX - 1, 100).is_empty());
    assert_eq!(m.due(u64::MAX, 100), [("a".into(), Reason::Silent)]);
    assert!(m.due(u64::MAX, 100).is_empty());
}

#[test]
fn rate_detection_activates_at_twenty_events_per_baseline_window() {
    for events in [19, 20] {
        let mut m = monitor(100_000);
        for window in 0..3 {
            for event in 0..events {
                let now = window * 1_000 + event;
                m.observe("a", now == 0, now);
            }
        }
        for now in [3_500, 4_500] {
            m.observe("a", false, now);
        }
        let due = m.due(5_000, 100);
        if events == 19 {
            assert!(due.is_empty());
        } else {
            assert_eq!(due, [("a".into(), Reason::RateDrop)]);
        }
    }
}

#[test]
fn additional_snapshots_do_not_erase_a_learned_rate() {
    let mut m = monitor(100_000);
    establish_rate(&mut m, "a");
    m.observe("a", true, 3_500);
    m.observe("a", true, 4_500);
    assert_eq!(m.due(5_000, 100), [("a".into(), Reason::RateDrop)]);
}

#[test]
fn rate_recovery_reserves_cooldown_across_reset_and_new_learning() {
    let mut m = monitor(10_000);
    establish_rate(&mut m, "a");
    m.observe("a", false, 3_500);
    m.observe("a", false, 4_500);
    assert!(m.due(5_000, 0).is_empty());
    assert_eq!(m.due(5_000, 100), [("a".into(), Reason::RateDrop)]);
    m.reset("a", 5_000);
    for window in 5..8 {
        for event in 0..100 {
            let now = window * 1_000 + event;
            m.observe("a", now == 5_000, now);
        }
    }
    for window in 8..35 {
        m.observe("a", false, window * 1_000 + 500);
        assert!(m.due((window + 1) * 1_000 - 1, 100).is_empty());
    }
    assert_eq!(m.due(35_000, 100), [("a".into(), Reason::RateDrop)]);
}

#[test]
fn book_progress_on_one_symbol_does_not_mask_another_symbols_silence() {
    let mut m = Monitor::new(["a".into(), "b".into()], 1_000, 1_000);
    m.observe("a", true, 0);
    m.observe("b", true, 900);
    m.observe("b", false, 1_000);
    assert_eq!(m.due(1_000, 100), [("a".into(), Reason::Silent)]);
    assert_eq!(m.due(2_000, 100), [("b".into(), Reason::Silent)]);
}

#[test]
fn late_subscription_batches_get_their_own_initialization_deadline() {
    let mut monitor = Monitor::new(Vec::<String>::new(), 1000, 100);
    assert!(monitor.due(5000, 100).is_empty());
    monitor.subscribe("late".into(), 5000);
    assert!(monitor.due(5999, 100).is_empty());
    assert_eq!(
        monitor.due(6000, 100),
        vec![("late".into(), Reason::Unsynchronized)]
    );
}
#[test]
fn resumed_stale_deltas_do_not_reinitialize_the_book() {
    let mut monitor = Monitor::new(["a".to_string()], 1000, 100);
    monitor.observe("a", true, 0);
    monitor.observe("a", false, 1001);
    monitor.observe("a", false, 1002);
    assert_eq!(
        monitor.due(1002, 100),
        vec![("a".into(), Reason::Unsynchronized)]
    );
}

#[test]
fn removing_and_resubscribing_updates_the_compact_index_and_deadline() {
    let mut m = Monitor::new(["a".into(), "b".into(), "c".into()], 1_000, 1_000);
    assert_eq!(m.due(1_000, 1).len(), 1);
    m.unsubscribe("b");
    m.unsubscribe("missing");
    m.observe("c", true, 1_000);
    let due = m.due(2_000, 10);
    assert_eq!(due, [("c".into(), Reason::Silent)]);
    m.unsubscribe("a");
    m.unsubscribe("c");
    assert!(m.due(99_000, 10).is_empty());
    m.subscribe("b".into(), 99_000);
    assert!(m.due(99_999, 10).is_empty());
    assert_eq!(m.due(100_000, 1), [("b".into(), Reason::Unsynchronized)]);
}

#[test]
fn never_initialized_assets_are_served_before_merely_quiet_books() {
    let mut m = Monitor::new(["quiet".into(), "uninitialized".into()], 1_000, 1_000);
    m.observe("quiet", true, 0);
    assert_eq!(
        m.due(1_000, 1),
        [("uninitialized".into(), Reason::Unsynchronized)]
    );
    assert_eq!(m.due(1_000, 1), [("quiet".into(), Reason::Silent)]);
}
