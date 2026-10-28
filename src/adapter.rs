use crate::Side;
use anyhow::{Context, Result, bail, ensure};
use chrono::{DateTime, Utc};
use serde_json::Value;
use std::collections::HashSet;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    Snapshot {
        symbol: String,
        timestamp_ms: i64,
        bids: Vec<(i64, i64)>,
        asks: Vec<(i64, i64)>,
    },
    Delta {
        symbol: String,
        timestamp_ms: i64,
        side: Side,
        price: i64,
        quantity: i64,
    },
    NewMarket {
        symbols: Vec<String>,
    },
    Resolved {
        symbols: Vec<String>,
    },
}

/// Parse an unsigned plain decimal exactly at a scale of one million.
/// Additional fractional digits are accepted only when they are all zero.
pub fn fixed(s: &str) -> Result<i64> {
    let (whole, fraction) = s.split_once('.').map_or((s, None), |(w, f)| (w, Some(f)));
    ensure!(
        !whole.is_empty() && whole.bytes().all(|b| b.is_ascii_digit()),
        "expected an unsigned plain decimal"
    );
    let mut integer = 0_i64;
    for digit in whole.bytes() {
        integer = integer
            .checked_mul(10)
            .and_then(|n| n.checked_add(i64::from(digit - b'0')))
            .context("fixed-point overflow")?;
    }
    let mut fractional = 0_i64;
    if let Some(fraction) = fraction {
        ensure!(
            !fraction.is_empty() && fraction.bytes().all(|b| b.is_ascii_digit()),
            "expected decimal fractional digits"
        );
        for (index, digit) in fraction.bytes().enumerate() {
            if index < 6 {
                fractional = fractional * 10 + i64::from(digit - b'0');
            } else {
                ensure!(digit == b'0', "nonzero precision beyond six decimal places");
            }
        }
        for _ in fraction.len()..6 {
            fractional *= 10;
        }
    }
    integer
        .checked_mul(1_000_000)
        .and_then(|n| n.checked_add(fractional))
        .context("fixed-point overflow")
}

/// Decode a complete raw Polymarket market WebSocket frame.
/// Unknown extension events produce no events and never mutate books.
/// No events are returned if any recognized member of the frame is invalid.
pub fn decode(text: &str) -> Result<Vec<Event>> {
    if text.trim() == "PONG" {
        return Ok(Vec::new());
    }
    let frame: Value = serde_json::from_str(text).context("invalid market frame JSON")?;
    let mut events = Vec::new();
    if let Value::Array(messages) = &frame {
        for (index, message) in messages.iter().enumerate() {
            decode_message(message, &mut events)
                .with_context(|| format!("invalid market frame member {index}"))?;
        }
    } else {
        decode_message(&frame, &mut events)?;
    }
    Ok(events)
}

fn decode_message(message: &Value, events: &mut Vec<Event>) -> Result<()> {
    let object = message
        .as_object()
        .context("market event must be an object")?;
    ensure!(
        !object.contains_key("error")
            && !object.contains_key("errors")
            && object.get("success") != Some(&Value::Bool(false)),
        "server returned an error response"
    );
    match string(message, "event_type")? {
        "book" => events.push(Event::Snapshot {
            symbol: symbol(message, "asset_id")?,
            timestamp_ms: timestamp(message.get("timestamp").context("missing timestamp")?)?,
            bids: levels(message, "bids")?,
            asks: levels(message, "asks")?,
        }),
        "price_change" => {
            let timestamp_ms = timestamp(message.get("timestamp").context("missing timestamp")?)?;
            for change in array(message, "price_changes")? {
                let side = match string(change, "side")? {
                    "BUY" => Side::Buy,
                    "SELL" => Side::Sell,
                    _ => bail!("invalid price-change side; expected BUY or SELL"),
                };
                events.push(Event::Delta {
                    symbol: symbol(change, "asset_id")?,
                    timestamp_ms,
                    side,
                    price: price(change)?,
                    quantity: fixed(string(change, "size")?).context("invalid quantity")?,
                });
            }
        }
        "new_market" => {
            validate_optional_timestamp(message)?;
            // The raw WebSocket API uses assets_ids; asset_ids is an SDK field.
            // Lifecycle metadata without IDs cannot discover symbols, but must
            // not interrupt unrelated order-book updates in the same frame.
            if message.get("assets_ids").is_none_or(Value::is_null) {
                return Ok(());
            }
            let assets = array(message, "assets_ids")?;
            ensure!(!assets.is_empty(), "new-market asset IDs must not be empty");
            let mut seen = HashSet::new();
            let mut symbols = Vec::with_capacity(assets.len());
            for asset in assets {
                let asset = asset
                    .as_str()
                    .context("new-market asset ID must be a string")?;
                ensure!(
                    !asset.is_empty() && asset.trim() == asset,
                    "new-market asset ID must not be empty or padded"
                );
                ensure!(seen.insert(asset), "duplicate new-market asset ID");
                symbols.push(asset.to_owned());
            }
            events.push(Event::NewMarket { symbols });
        }
        "market_resolved" => {
            validate_optional_timestamp(message)?;
            let symbols = array(message, "assets_ids")?
                .iter()
                .map(|value| {
                    let value = value
                        .as_str()
                        .context("resolved asset ID must be a string")?;
                    ensure!(
                        !value.trim().is_empty(),
                        "resolved asset ID must not be empty"
                    );
                    Ok(value.to_owned())
                })
                .collect::<Result<Vec<_>>>()?;
            events.push(Event::Resolved { symbols });
        }
        "last_trade_price" | "tick_size_change" | "best_bid_ask" => {}
        "error" | "" => bail!("invalid market event_type or server error"),
        // Forward-compatible metadata is deliberately a no-op. Known book and
        // lifecycle events above still validate fully before decode returns.
        _ => {}
    }
    Ok(())
}

fn string<'a>(value: &'a Value, field: &str) -> Result<&'a str> {
    value
        .get(field)
        .and_then(Value::as_str)
        .with_context(|| format!("missing or non-string {field}"))
}

fn array<'a>(value: &'a Value, field: &str) -> Result<&'a Vec<Value>> {
    value
        .get(field)
        .and_then(Value::as_array)
        .with_context(|| format!("missing or non-array {field}"))
}

fn symbol(value: &Value, field: &str) -> Result<String> {
    let symbol = string(value, field)?;
    ensure!(!symbol.trim().is_empty(), "asset ID must not be empty");
    Ok(symbol.to_owned())
}

fn price(value: &Value) -> Result<i64> {
    let price = fixed(string(value, "price")?).context("invalid price")?;
    ensure!(price <= 1_000_000, "price exceeds one");
    Ok(price)
}

fn levels(message: &Value, side: &str) -> Result<Vec<(i64, i64)>> {
    let mut seen = HashSet::new();
    let mut levels = Vec::new();
    for level in array(message, side)? {
        let price = price(level)?;
        let quantity = fixed(string(level, "size")?).context("invalid quantity")?;
        ensure!(seen.insert(price), "duplicate snapshot price on {side}");
        if quantity != 0 {
            levels.push((price, quantity));
        }
    }
    Ok(levels)
}

fn timestamp(value: &Value) -> Result<i64> {
    let timestamp_ms = match value {
        Value::String(text) => {
            ensure!(
                !text.is_empty() && text.bytes().all(|b| b.is_ascii_digit()),
                "timestamp must contain unsigned integer milliseconds"
            );
            text.parse::<i64>().context("timestamp overflow")?
        }
        Value::Number(number) => number
            .as_i64()
            .context("timestamp must be integer milliseconds")?,
        _ => bail!("timestamp must be string or integer milliseconds"),
    };
    ensure!(timestamp_ms >= 0, "timestamp precedes Unix epoch");
    ensure!(
        DateTime::<Utc>::from_timestamp_millis(timestamp_ms).is_some(),
        "timestamp outside chrono range"
    );
    Ok(timestamp_ms)
}

fn validate_optional_timestamp(message: &Value) -> Result<()> {
    if let Some(value) = message.get("timestamp") {
        timestamp(value)?;
    }
    Ok(())
}
