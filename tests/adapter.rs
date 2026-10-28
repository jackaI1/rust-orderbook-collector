use orderbook_collector::Side;
use orderbook_collector::adapter::{Event, decode, fixed};
use serde_json::{Value, json};

fn book() -> Value {
    json!({
        "event_type": "book", "market": "condition", "asset_id": "token-a",
        "timestamp": "1782753357257", "hash": "hash",
        "bids": [{"price": "0.08", "size": "33343.4"}],
        "asks": [{"price": "0.99", "size": "218442.27"}]
    })
}

fn changes() -> Value {
    json!({
        "event_type": "price_change", "market": "condition",
        "timestamp": "1782753357257",
        "price_changes": [
            {"asset_id": "token-a", "price": "0.08", "size": "33343.4",
             "side": "BUY", "hash": "hash", "best_bid": "0.08", "best_ask": "0.09"},
            {"asset_id": "token-b", "price": "1", "size": "0", "side": "SELL"}
        ]
    })
}

#[test]
fn fixed_scales_exactly_without_floating_point_rounding() {
    for (input, expected) in [
        ("0", 0),
        ("1", 1_000_000),
        ("0.000001", 1),
        ("00012.340000000", 12_340_000),
        ("0.1234560000", 123_456),
        ("9223372036854.775807000", i64::MAX),
    ] {
        assert_eq!(fixed(input).unwrap(), expected, "{input}");
    }
}

#[test]
fn fixed_rejects_invalid_precision_syntax_and_overflow() {
    for input in [
        "",
        "-0",
        "-1",
        "+1",
        ".5",
        "1.",
        " 1",
        "1 ",
        "1e0",
        "1E-6",
        "NaN",
        "inf",
        "1.2.3",
        "１",
        "0.0000001",
        "0.12345600001",
        "9223372036854.775808",
        "9223372036855",
        "99999999999999999999999999",
    ] {
        assert!(fixed(input).is_err(), "accepted {input:?}");
    }
}

#[test]
fn decodes_raw_book_and_omits_zero_levels() {
    let mut input = book();
    input["bids"]
        .as_array_mut()
        .unwrap()
        .push(json!({"price":"0", "size":"0"}));
    assert_eq!(
        decode(&input.to_string()).unwrap(),
        vec![Event::Snapshot {
            symbol: "token-a".into(),
            timestamp_ms: 1_782_753_357_257,
            bids: vec![(80_000, 33_343_400_000)],
            asks: vec![(990_000, 218_442_270_000)],
        }]
    );
}

#[test]
fn empty_books_and_same_price_on_opposite_sides_are_valid() {
    let mut input = book();
    input["bids"] = json!([]);
    input["asks"] = json!([]);
    assert_eq!(
        decode(&input.to_string()).unwrap(),
        vec![Event::Snapshot {
            symbol: "token-a".into(),
            timestamp_ms: 1_782_753_357_257,
            bids: vec![],
            asks: vec![],
        }]
    );
    input["bids"] = json!([{"price":"0.5", "size":"1"}]);
    input["asks"] = json!([{"price":"0.5", "size":"2"}]);
    assert!(decode(&input.to_string()).is_ok());
}

#[test]
fn rejects_same_side_duplicate_prices_even_if_zero_or_differently_spelled() {
    for side in ["bids", "asks"] {
        let mut input = book();
        input[side] = json!([
            {"price":"0.5", "size":"0"}, {"price":"0.5000000", "size":"1"}
        ]);
        assert!(decode(&input.to_string()).is_err(), "{side}");
    }
}

#[test]
fn plural_price_changes_produce_ordered_deltas_including_deletions() {
    assert_eq!(
        decode(&changes().to_string()).unwrap(),
        vec![
            Event::Delta {
                symbol: "token-a".into(),
                timestamp_ms: 1_782_753_357_257,
                side: Side::Buy,
                price: 80_000,
                quantity: 33_343_400_000
            },
            Event::Delta {
                symbol: "token-b".into(),
                timestamp_ms: 1_782_753_357_257,
                side: Side::Sell,
                price: 1_000_000,
                quantity: 0
            },
        ]
    );
}

#[test]
fn prices_must_be_inclusive_binary_market_range() {
    for input in ["0", "1", "1.000000000"] {
        let mut frame = changes();
        frame["price_changes"][0]["price"] = json!(input);
        assert!(decode(&frame.to_string()).is_ok(), "{input}");
    }
    for invalid in ["-0.1", "1.000001", "2", "0.0000001"] {
        let mut snapshot = book();
        snapshot["asks"][0]["price"] = json!(invalid);
        assert!(decode(&snapshot.to_string()).is_err(), "{invalid}");
        let mut delta = changes();
        delta["price_changes"][0]["price"] = json!(invalid);
        assert!(decode(&delta.to_string()).is_err(), "{invalid}");
    }
}

#[test]
fn quantities_reject_negatives_overflow_and_lost_precision() {
    for invalid in ["-1", "-0", "0.0000001", "9223372036854.775808", "1e3"] {
        let mut snapshot = book();
        snapshot["bids"][0]["size"] = json!(invalid);
        assert!(decode(&snapshot.to_string()).is_err(), "{invalid}");
        let mut delta = changes();
        delta["price_changes"][0]["size"] = json!(invalid);
        assert!(decode(&delta.to_string()).is_err(), "{invalid}");
    }
}

#[test]
fn accepts_only_exact_wire_sides() {
    for invalid in ["buy", "sell", "Buy", "BID", "ASK", "", "HOLD"] {
        let mut input = changes();
        input["price_changes"][0]["side"] = json!(invalid);
        assert!(decode(&input.to_string()).is_err(), "{invalid}");
    }
}

#[test]
fn timestamps_are_nonnegative_integer_unix_milliseconds_within_chrono_range() {
    for value in [
        json!("0"),
        json!(0),
        json!("1782753357257"),
        json!(1782753357257_i64),
    ] {
        let mut input = book();
        input["timestamp"] = value.clone();
        assert!(decode(&input.to_string()).is_ok(), "{value}");
    }
    for value in [
        json!("-1"),
        json!(-1),
        json!("+1"),
        json!("1.0"),
        json!(1.5),
        json!("1e3"),
        json!(""),
        json!(null),
        json!(i64::MAX),
        json!("9223372036854775808"),
    ] {
        for mut input in [book(), changes()] {
            input["timestamp"] = value.clone();
            assert!(decode(&input.to_string()).is_err(), "{value}");
        }
    }
}

#[test]
fn lifecycle_uses_raw_assets_ids_for_all_resolved_outcomes() {
    let input = json!([
        {"event_type":"new_market", "id":"market", "assets_ids":["a","b"],
         "timestamp":"1782753357257"},
        {"event_type":"market_resolved", "assets_ids":["a","b"],
         "winning_asset_id":"a", "timestamp":"1782753357257"}
    ]);
    assert_eq!(
        decode(&input.to_string()).unwrap(),
        vec![
            Event::NewMarket {
                symbols: vec!["a".into(), "b".into()]
            },
            Event::Resolved {
                symbols: vec!["a".into(), "b".into()]
            },
        ]
    );
}

#[test]
fn malformed_lifecycle_symbol_lists_and_timestamps_fail() {
    for input in [
        json!({"event_type":"market_resolved"}),
        json!({"event_type":"market_resolved", "asset_ids":["a"]}),
        json!({"event_type":"market_resolved", "assets_ids":null}),
        json!({"event_type":"market_resolved", "assets_ids":[12]}),
        json!({"event_type":"market_resolved", "assets_ids":[""]}),
        json!({"event_type":"market_resolved", "assets_ids":["a"], "timestamp":"-1"}),
        json!({"event_type":"new_market", "timestamp":"-1"}),
    ] {
        assert!(decode(&input.to_string()).is_err(), "{input}");
    }
}

#[test]
fn ignores_heartbeat_and_unrelated_market_events() {
    assert_eq!(decode("PONG").unwrap(), vec![]);
    for event_type in ["last_trade_price", "tick_size_change", "best_bid_ask"] {
        assert_eq!(
            decode(&json!({"event_type":event_type}).to_string()).unwrap(),
            vec![]
        );
    }
    assert_eq!(decode("[]").unwrap(), vec![]);
}

#[test]
fn rejects_errors_and_incompatible_envelopes() {
    for input in [
        "PING",
        "null",
        "42",
        "\"PONG\"",
        "{}",
        "[[]]",
        "[null]",
        "{",
        r#"{"event_type":""}"#,
        r#"{"event_type":"error"}"#,
        r#"{"error":"invalid subscription"}"#,
        r#"{"errors":["bad"]}"#,
        r#"{"event_type":"best_bid_ask","error":"bad"}"#,
        r#"{"event_type":"future_event","error":"bad"}"#,
        r#"{"event_type":"future_event","success":false}"#,
        r#"{"event_type":"new_market","success":false}"#,
        r#"{"topic":"market","type":"book","payload":{}}"#,
        r#"{"event_type":"price_change","asset_id":"a","changes":[],"timestamp":"0"}"#,
    ] {
        assert!(decode(input).is_err(), "accepted {input}");
    }
}

#[test]
fn required_book_and_delta_fields_cannot_be_missing_or_wrong_types() {
    for field in ["asset_id", "timestamp", "bids", "asks"] {
        let mut input = book();
        input.as_object_mut().unwrap().remove(field);
        assert!(decode(&input.to_string()).is_err(), "missing {field}");
    }
    for field in ["asset_id", "price", "size", "side"] {
        let mut input = changes();
        input["price_changes"][0]
            .as_object_mut()
            .unwrap()
            .remove(field);
        assert!(decode(&input.to_string()).is_err(), "missing {field}");
    }
    for (field, invalid) in [
        ("asset_id", json!("")),
        ("asset_id", json!(12)),
        ("bids", json!(null)),
        ("asks", json!({})),
    ] {
        let mut input = book();
        input[field] = invalid;
        assert!(decode(&input.to_string()).is_err(), "invalid {field}");
    }
    let mut input = book();
    input["bids"][0]["price"] = json!(0.08);
    assert!(decode(&input.to_string()).is_err());
}

#[test]
fn arrays_preserve_event_order_and_fail_atomically_on_any_bad_member() {
    let frame = json!([book(), changes(), {"event_type":"new_market", "assets_ids":["a","b"]}]);
    let events = decode(&frame.to_string()).unwrap();
    assert_eq!(events.len(), 4);
    assert!(matches!(&events[0], Event::Snapshot {symbol, ..} if symbol == "token-a"));
    assert!(matches!(&events[1], Event::Delta {symbol, ..} if symbol == "token-a"));
    assert!(matches!(&events[2], Event::Delta {symbol, ..} if symbol == "token-b"));
    assert_eq!(
        events[3],
        Event::NewMarket {
            symbols: vec!["a".into(), "b".into()]
        }
    );
    let mut bad_changes = changes();
    bad_changes["price_changes"][1]["side"] = json!("invalid");
    for input in [
        json!([book(), {"event_type":"book"}]),
        json!([book(), bad_changes]),
    ] {
        assert!(decode(&input.to_string()).is_err());
    }
}

#[test]
fn new_market_preserves_exact_string_ids_order_and_ignores_optional_metadata() {
    let input = json!({
        "event_type":"new_market", "id":"123456", "market":"condition",
        "assets_ids":[
            "107505882767731489358349912513945399560393482969656700824895970500493757150417",
            "001", "1", "opaque-token-id"
        ],
        "asset_ids":["not-the-raw-field"],
        "timestamp":"1782753357257", "outcomes":["Yes","No"],
        "future_metadata":{"nested":[null, true]}, "fees_enabled":null
    });
    assert_eq!(
        decode(&input.to_string()).unwrap(),
        vec![Event::NewMarket {
            symbols: vec![
                "107505882767731489358349912513945399560393482969656700824895970500493757150417"
                    .into(),
                "001".into(),
                "1".into(),
                "opaque-token-id".into()
            ]
        }]
    );
}

#[test]
fn new_market_rejects_duplicate_empty_and_malformed_asset_ids() {
    for assets_ids in [
        json!([]),
        json!(["a", "a"]),
        json!([""]),
        json!([" \t"]),
        json!([" a"]),
        json!(["a "]),
        json!([12]),
        json!(["a", null]),
        json!([true]),
        json!([{}]),
        json!("[\"a\"]"),
        json!({"asset_id":"a"}),
    ] {
        let input = json!({"event_type":"new_market", "assets_ids":assets_ids});
        assert!(decode(&input.to_string()).is_err(), "accepted {input}");
        assert!(decode(&json!([book(), input]).to_string()).is_err());
    }
}

#[test]
fn new_market_without_asset_ids_is_ignored_without_discarding_book_events() {
    for metadata in [
        json!({"event_type":"new_market"}),
        json!({"event_type":"new_market", "assets_ids":null}),
        json!({"event_type":"new_market", "asset_ids":["sdk-only"]}),
        json!({"event_type":"new_market", "future_metadata":{"active":true},
               "timestamp":"1782753357257"}),
    ] {
        assert_eq!(decode(&metadata.to_string()).unwrap(), vec![]);
        assert_eq!(
            decode(&json!([book(), metadata, changes()]).to_string()).unwrap(),
            decode(&json!([book(), changes()]).to_string()).unwrap()
        );
    }
}

#[test]
fn unknown_extension_events_are_ignored_without_book_mutation() {
    let extension = json!({"event_type":"future_event", "timestamp":"not-a-book-time",
                           "asset_id":"token-a", "bids":"metadata", "future":{}});
    assert_eq!(decode(&extension.to_string()).unwrap(), vec![]);
    assert_eq!(
        decode(&json!([book(), extension, changes()]).to_string()).unwrap(),
        decode(&json!([book(), changes()]).to_string()).unwrap()
    );
}
