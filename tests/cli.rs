use orderbook_collector::util::{AlignedSbeReader, OrderbookSchema};
use std::process::Command;
#[test]
fn replay_writes_real_sbe_and_inspector_reads_it() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("demo.sbe");
    let out = Command::new(env!("CARGO_BIN_EXE_orderbook-collector"))
        .args([
            "--no-stdout",
            "--output",
            path.to_str().unwrap(),
            "replay",
            "--input",
            "fixtures/demo.jsonl",
        ])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(out.stdout.is_empty());
    let mut r = AlignedSbeReader::<OrderbookSchema>::open(&path).unwrap();
    let mut count = 0;
    while r.read_msg().unwrap().is_some() {
        count += 1;
    }
    assert_eq!(count, 8);
    let out = Command::new(env!("CARGO_BIN_EXE_orderbook-collector"))
        .args(["inspect", "--input", path.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(out.status.success());
    let text = String::from_utf8(out.stdout).unwrap();
    assert_eq!(text.lines().count(), 8);
    assert!(text.contains("\"quantity\":0"));
}
#[test]
fn malformed_replay_fails_and_flushes_prior_records() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("bad.jsonl");
    let path = dir.path().join("out.sbe");
    std::fs::write(&input,"{\"at_ms\":1000,\"universe\":[\"a\"]}\n{\"at_ms\":1001,\"source\":0,\"frame\":{\"event_type\":\"book\",\"asset_id\":\"a\",\"timestamp\":\"1000\",\"bids\":[],\"asks\":[]}}\ninvalid\n").unwrap();
    let out = Command::new(env!("CARGO_BIN_EXE_orderbook-collector"))
        .args([
            "--no-stdout",
            "--output",
            path.to_str().unwrap(),
            "replay",
            "--input",
            input.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(!out.status.success());
    let mut r = AlignedSbeReader::<OrderbookSchema>::open(path).unwrap();
    assert!(r.read_msg().unwrap().is_some());
    assert!(r.read_msg().unwrap().is_none());
}
#[test]
fn rejects_no_output_and_existing_files() {
    let out = Command::new(env!("CARGO_BIN_EXE_orderbook-collector"))
        .args(["--no-stdout", "replay", "--input", "fixtures/demo.jsonl"])
        .output()
        .unwrap();
    assert!(!out.status.success());
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("keep");
    std::fs::write(&p, "preserve").unwrap();
    let out = Command::new(env!("CARGO_BIN_EXE_orderbook-collector"))
        .args([
            "--output",
            p.to_str().unwrap(),
            "replay",
            "--input",
            "fixtures/demo.jsonl",
        ])
        .output()
        .unwrap();
    assert!(!out.status.success());
    assert_eq!(std::fs::read_to_string(p).unwrap(), "preserve");
}
#[test]
fn stdout_is_supplied_sbe_encoding_without_log_text() {
    use orderbook_collector::util::dtos::{SbeEncode, challenge_orderbook::SnapshotDomain};
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("one.jsonl");
    std::fs::write(&input,"{\"at_ms\":1000,\"universe\":[\"a\"]}\n{\"at_ms\":1001,\"source\":0,\"frame\":{\"event_type\":\"book\",\"asset_id\":\"a\",\"timestamp\":\"1000\",\"bids\":[],\"asks\":[]}}\n").unwrap();
    let result = Command::new(env!("CARGO_BIN_EXE_orderbook-collector"))
        .args(["replay", "--input", input.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(result.status.success());
    let expected = SnapshotDomain {
        symbol: "a".into(),
        received_at: chrono::DateTime::from_timestamp_millis(1001).unwrap(),
        exchange_timestamp: chrono::DateTime::from_timestamp_millis(1000).unwrap(),
        bids: vec![],
        asks: vec![],
    }
    .to_bytes()
    .unwrap();
    assert_eq!(result.stdout, expected);
    assert!(String::from_utf8_lossy(&result.stderr).contains("synthetic_replay"));
}
