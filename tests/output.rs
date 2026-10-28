use orderbook_collector::{
    engine::Record,
    output::Output,
    util::{
        AlignedSbeReader, OrderbookSchema,
        dtos::challenge_orderbook::{AnyMessage, SnapshotDomain},
    },
};
#[test]
fn supplied_writer_roundtrip_and_flush() {
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("out.sbe");
    let mut o = Output::new(Some(&p), false).unwrap();
    o.write(&Record::Snapshot(SnapshotDomain {
        received_at: chrono::DateTime::from_timestamp_millis(1).unwrap(),
        exchange_timestamp: chrono::DateTime::from_timestamp_millis(1).unwrap(),
        bids: vec![],
        asks: vec![],
        symbol: "a".into(),
    }))
    .unwrap();
    o.finish().unwrap();
    let mut r = AlignedSbeReader::<OrderbookSchema>::open(p).unwrap();
    assert!(matches!(
        r.read_msg().unwrap(),
        Some(AnyMessage::Snapshot(_))
    ));
    assert!(r.read_msg().unwrap().is_none());
}
