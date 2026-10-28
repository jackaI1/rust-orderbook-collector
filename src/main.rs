use anyhow::{Context, Result, ensure};
use clap::{Parser, Subcommand};
use orderbook_collector::{
    adapter,
    engine::{Engine, Record},
    live::{self, Config, Input},
    output::Output,
    util::{
        AlignedSbeReader, OrderbookSchema,
        dtos::challenge_orderbook::{AnyMessage, PriceLevelUpdateDomain, SnapshotDomain},
    },
};
use serde::Deserialize;
use serde_json::json;
use std::{
    collections::BTreeSet,
    io::{BufRead, Write},
    path::PathBuf,
    time::Duration,
};
use tokio_util::sync::CancellationToken;

#[derive(Parser)]
#[command(
    version,
    about = "Read-only Polymarket orderbook collector; required Orderbook SBE output"
)]
struct Cli {
    #[arg(
        long,
        global = true,
        help = "New aligned SBE file (existing paths are rejected)"
    )]
    output: Option<PathBuf>,
    #[arg(long, global = true, help = "Disable binary stdout; requires --output")]
    no_stdout: bool,
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
enum Command {
    /// Collect all discoverable live outcome books; no credentials or orders.
    Live {
        #[arg(
            long,
            default_value = "https://gamma-api.polymarket.com/markets/keyset"
        )]
        discovery_url: String,
        #[arg(
            long,
            default_value = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
        )]
        websocket_url: String,
        #[arg(
            long,
            default_value_t = 0,
            help = "Assets per socket; 0 automatically sizes shards for at most 128 base sockets"
        )]
        shard_size: usize,
        #[arg(long, default_value_t = 1)]
        replicas: usize,
        #[arg(
            long,
            help = "Cross-check one standalone market page against each complete event catalog"
        )]
        audit_catalog: bool,
        #[arg(long, default_value_t = 60)]
        discovery_secs: u64,
        #[arg(long, default_value_t = 300)]
        stale_secs: u64,
        #[arg(long, default_value_t = 3600)]
        rotate_secs: u64,
        #[arg(
            long,
            help = "Explicit partial-universe diagnostic mode; omit for challenge run"
        )]
        max_symbols: Option<usize>,
        #[arg(
            long,
            help = "Stop gracefully after this many seconds (bounded live probe)"
        )]
        duration_secs: Option<u64>,
    },
    /// Deterministic SYNTHETIC/OFFLINE JSONL replay through the real book engine.
    Replay {
        #[arg(long)]
        input: PathBuf,
    },
    /// Decode aligned SBE using the supplied upstream reader; JSONL on stdout.
    Inspect {
        #[arg(long)]
        input: PathBuf,
    },
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReplayLine {
    at_ms: i64,
    universe: Option<BTreeSet<String>>,
    source: Option<usize>,
    frame: Option<serde_json::Value>,
    disconnect: Option<usize>,
    tick: Option<bool>,
}
fn emit(output: &mut Output, records: Vec<Record>) -> Result<()> {
    for record in records {
        output.write(&record)?;
    }
    Ok(())
}
fn frame(
    engine: &mut Engine,
    output: &mut Output,
    source: usize,
    text: &str,
    at: i64,
) -> Result<()> {
    for event in adapter::decode(text)? {
        emit(output, engine.apply(source, event, at)?)?;
    }
    Ok(())
}
fn replay(input: PathBuf, output: &mut Output) -> Result<()> {
    let reader = std::io::BufReader::new(std::fs::File::open(input)?);
    let mut engine = Engine::new(usize::MAX, 300_000);
    let mut previous = None;
    let mut next = None;
    for (index, line) in reader.lines().enumerate() {
        let line = line?;
        ensure!(line.len() <= 8 << 20, "replay line too large");
        if line.trim().is_empty() {
            continue;
        }
        let row: ReplayLine =
            serde_json::from_str(&line).with_context(|| format!("replay line {}", index + 1))?;
        ensure!(
            row.at_ms >= 0 && chrono::DateTime::from_timestamp_millis(row.at_ms).is_some(),
            "invalid receive timestamp"
        );
        ensure!(
            previous.is_none_or(|p| row.at_ms >= p),
            "replay time regressed"
        );
        ensure!(
            previous.is_none_or(|p| row.at_ms - p <= 86_400_000),
            "replay gap exceeds 24h; split the fixture"
        );
        previous = Some(row.at_ms);
        let deadline = next.get_or_insert(row.at_ms + 60_000);
        while row.at_ms >= *deadline {
            emit(output, engine.snapshots(*deadline))?;
            *deadline += 60_000;
        }
        let actions = usize::from(row.universe.is_some())
            + usize::from(row.frame.is_some())
            + usize::from(row.disconnect.is_some())
            + usize::from(row.tick == Some(true));
        ensure!(actions == 1, "replay line must contain exactly one action");
        if let Some(symbols) = row.universe {
            engine.set_universe(symbols);
        } else if let Some(value) = row.frame {
            let text = match value {
                serde_json::Value::String(s) => s,
                v => v.to_string(),
            };
            frame(
                &mut engine,
                output,
                row.source.context("frame requires source")?,
                &text,
                row.at_ms,
            )?;
        } else if let Some(source) = row.disconnect {
            emit(output, engine.disconnect(source, row.at_ms))?;
        }
    }
    eprintln!(
        "{}",
        json!({"mode":"synthetic_replay","records":output.records,"metrics":engine.metrics})
    );
    Ok(())
}
fn inspect(input: PathBuf) -> Result<()> {
    let size = std::fs::metadata(&input)?.len();
    ensure!(
        size % (64 << 20) == 0,
        "truncated aligned SBE file: length must be a multiple of 64 MiB"
    );
    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(input)?;
    let mut out = std::io::BufWriter::new(std::io::stdout().lock());
    while let Some(message) = reader.read_msg()? {
        let value = match message {
            AnyMessage::Snapshot(d) => {
                let s = SnapshotDomain::try_from_decoder(d)?;
                json!({"type":"snapshot","symbol":s.symbol,"received_at":s.received_at.to_rfc3339(),"exchange_timestamp":s.exchange_timestamp.to_rfc3339(),"bids":s.bids.iter().map(|b|json!({"price":b.price_level,"quantity":b.quantity})).collect::<Vec<_>>(),"asks":s.asks.iter().map(|b|json!({"price":b.price_level,"quantity":b.quantity})).collect::<Vec<_>>()})
            }
            AnyMessage::PriceLevelUpdate(d) => {
                let d = PriceLevelUpdateDomain::try_from_decoder(d)?;
                json!({"type":"delta","symbol":d.symbol,"received_at":d.backtest_timestamp.to_rfc3339(),"exchange_timestamp":d.exchange_timestamp.to_rfc3339(),"side":d.side,"price":d.price_level,"quantity":d.quantity})
            }
            _ => anyhow::bail!("unsupported SBE message"),
        };
        writeln!(out, "{value}")?;
    }
    out.flush()?;
    Ok(())
}
enum Action {
    Stop,
    Producer(std::result::Result<Result<()>, tokio::task::JoinError>),
    Input(Input),
    Snapshot,
    Health,
}

async fn next_action<F: std::future::Future<Output = ()>>(
    cancel: &CancellationToken,
    producer: &mut tokio::task::JoinHandle<Result<()>>,
    rx: &mut tokio::sync::mpsc::Receiver<Input>,
    mut deadline: std::pin::Pin<&mut F>,
    snapshots: &mut tokio::time::Interval,
    health: &mut tokio::time::Interval,
) -> Action {
    tokio::select! {
        biased;
        _ = cancel.cancelled() => Action::Stop,
        _ = &mut deadline => Action::Stop,
        // Due timers precede data so a continuously ready queue cannot starve them.
        _ = snapshots.tick() => Action::Snapshot,
        _ = health.tick() => Action::Health,
        result = producer => Action::Producer(result),
        input = rx.recv() => input.map_or(Action::Stop, Action::Input),
    }
}

async fn collect(config: Config, duration: Option<u64>, output: &mut Output) -> Result<()> {
    let cancel = CancellationToken::new();
    let (tx, mut rx) = tokio::sync::mpsc::channel(64);
    let freshness = i64::try_from(config.stale_secs)?
        .checked_mul(1000)
        .context("stale duration overflow")?;
    let (hints_tx, hints_rx) = tokio::sync::mpsc::channel(64);
    let mut producer = tokio::spawn(live::run_with_hints(config, tx, cancel.clone(), hints_rx));
    let mut engine = Engine::new(usize::MAX, freshness);
    let signal_cancel = cancel.clone();
    let signal = tokio::spawn(async move {
        tokio::select! {
            _ = signal_cancel.cancelled() => {},
            result = tokio::signal::ctrl_c() => {
                if result.is_ok() { signal_cancel.cancel(); }
            }
        }
    });
    let deadline = async {
        if let Some(seconds) = duration {
            tokio::time::sleep(Duration::from_secs(seconds)).await;
        } else {
            std::future::pending::<()>().await;
        }
    };
    tokio::pin!(deadline);
    let mut snapshots = tokio::time::interval_at(
        tokio::time::Instant::now() + Duration::from_secs(60),
        Duration::from_secs(60),
    );
    snapshots.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut health = tokio::time::interval(Duration::from_secs(10));
    health.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut max_delay_ms = None::<i64>;
    let mut frames = 0_u64;
    let mut producer_done = false;
    let mut input_actions = 0u64;
    let mut input_service_us = 0u64;
    let mut max_input_service_us = 0u64;
    let result: Result<()> = async {
        loop {
            match next_action(&cancel, &mut producer, &mut rx, deadline.as_mut(), &mut snapshots, &mut health).await {
                Action::Stop => break,
                Action::Producer(result) => { producer_done = true; result??; break; },
                Action::Input(event) => {
                    let started=std::time::Instant::now();
                    let handled=handle(event, &mut engine, output, &mut frames, &mut max_delay_ms, &hints_tx);
                    let elapsed=u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);
                    input_actions=input_actions.saturating_add(1);input_service_us=input_service_us.saturating_add(elapsed);
                    max_input_service_us=max_input_service_us.max(elapsed);handled?;
                },
                Action::Snapshot => {
                    let started = std::time::Instant::now();
                    let received_ms = chrono::Utc::now().timestamp_millis();
                    let universe = engine.symbol_count();
                    let record_start = output.records;
                    let symbols = engine.visit_snapshots(received_ms, |record| output.write(&record))?;
                    output.flush_stdout()?;
                    eprintln!("{}", json!({"mode":"minute_snapshot","received_ms":received_ms,
                        "universe":universe,"symbols":symbols,"missing":universe.saturating_sub(symbols),
                        "record_start":record_start,"record_end":output.records,
                        "duration_ms":started.elapsed().as_millis()}));
                },
                Action::Health => {
                    let now = chrono::Utc::now().timestamp_millis();
                    let queue_depth = rx.len();
                    let coverage=engine.coverage(now);
                    let symbols=coverage.symbols;let ready=coverage.ready;
                    eprintln!("{}", json!({
                        "mode":"live", "received_ms":now, "symbols":symbols, "ready":ready,
                        "frames":frames, "queue_depth":queue_depth, "queue_capacity":64, "coverage":coverage,
                        "max_observed_exchange_to_receive_ms":max_delay_ms, "metrics":engine.metrics,
                        "consumer_service":{"actions":input_actions,"total_us":input_service_us,"max_us":max_input_service_us}
                    }));
                    output.flush_stdout()?;
                }
            }
        }
        Ok(())
    }.await;
    cancel.cancel();
    // Drain accepted frames in order while producers close and release senders.
    let mut final_result = result;
    while let Some(event) = rx.recv().await {
        if final_result.is_ok()
            && let Err(error) = handle(
                event,
                &mut engine,
                output,
                &mut frames,
                &mut max_delay_ms,
                &hints_tx,
            )
        {
            final_result = Err(error);
        }
    }
    if !producer_done {
        match producer.await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                if final_result.is_ok() {
                    final_result = Err(error);
                }
            }
            Err(error) => {
                if final_result.is_ok() {
                    final_result = Err(error.into());
                }
            }
        }
    }
    signal.await?;
    eprintln!(
        "{}",
        json!({"mode":"live_shutdown", "frames":frames, "records":output.records, "metrics":engine.metrics})
    );
    final_result
}

fn handle(
    input: Input,
    engine: &mut Engine,
    output: &mut Output,
    frames: &mut u64,
    max_delay: &mut Option<i64>,
    hints: &tokio::sync::mpsc::Sender<live::Hint>,
) -> Result<()> {
    match input {
        Input::Universe(symbols) => engine.set_universe_assets(symbols),
        Input::AddSymbols(symbols) => engine.add_universe_assets(symbols),
        Input::RemoveSymbols(symbols) => {
            engine.apply(
                0,
                adapter::Event::Resolved {
                    symbols: symbols.into_iter().map(|s| s.to_string()).collect(),
                },
                chrono::Utc::now().timestamp_millis(),
            )?;
        }
        Input::Reset { source, symbols } => emit(
            output,
            engine.reset(source, &symbols, chrono::Utc::now().timestamp_millis()),
        )?,
        Input::Disconnected { source } => {
            engine.visit_disconnect(source, chrono::Utc::now().timestamp_millis(), |record| {
                output.write(&record)
            })?;
        }
        Input::Frame {
            source,
            text,
            received_ms,
        } => {
            *frames += 1;
            let events = adapter::decode(&text)
                .context("invalid exchange frame; stopping to avoid silent corruption")?;
            apply_events(
                source,
                events,
                received_ms,
                engine,
                output,
                max_delay,
                hints,
            )?;
        }
        Input::Events {
            source,
            events,
            received_ms,
        } => {
            *frames += 1;
            apply_events(
                source,
                events,
                received_ms,
                engine,
                output,
                max_delay,
                hints,
            )?;
        }

        Input::Fatal(message) => anyhow::bail!("{message}"),
    }
    Ok(())
}
fn apply_events(
    source: usize,
    events: Vec<adapter::Event>,
    received_ms: i64,
    engine: &mut Engine,
    output: &mut Output,
    max_delay: &mut Option<i64>,
    hints: &tokio::sync::mpsc::Sender<live::Hint>,
) -> Result<()> {
    for event in events {
        let hint = match &event {
            adapter::Event::NewMarket { symbols } => Some(live::Hint::NewMarket(symbols.clone())),
            adapter::Event::Resolved { symbols } => Some(live::Hint::Resolved(symbols.clone())),
            _ => None,
        };
        if let Some(hint) = hint
            && hints.try_send(hint).is_err()
        {
            eprintln!(
                "{}",
                json!({"event":"lifecycle_hint_deferred","reason":"bounded hint mailbox unavailable; full sweep remains authoritative"})
            );
        }
        let affected = match &event {
            adapter::Event::Snapshot {
                symbol,
                timestamp_ms,
                ..
            }
            | adapter::Event::Delta {
                symbol,
                timestamp_ms,
                ..
            } => {
                let delay = received_ms.saturating_sub(*timestamp_ms);
                *max_delay = Some(max_delay.map_or(delay, |v| v.max(delay)));
                Some(symbol.clone())
            }
            _ => None,
        };
        match engine.apply(source, event, received_ms) {
            Ok(records) => emit(output, records)?,
            Err(error) => {
                eprintln!(
                    "{}",
                    json!({"event":"engine_source_invalidated","source":source,
                    "reason":error.to_string().chars().take(512).collect::<String>()})
                );
                if let Some(symbol) = affected {
                    emit(
                        output,
                        engine.reset(source, &BTreeSet::from([symbol]), received_ms),
                    )?;
                } else {
                    emit(output, engine.disconnect(source, received_ms))?;
                }
            }
        }
    }
    Ok(())
}

#[tokio::main(worker_threads = 4)]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    if let Command::Inspect { input } = cli.command {
        return inspect(input);
    }
    let mut output = Output::new(cli.output.as_deref(), !cli.no_stdout)?;
    let result = match cli.command {
        Command::Replay { input } => replay(input, &mut output),
        Command::Live {
            discovery_url,
            websocket_url,
            shard_size,
            replicas,
            audit_catalog,
            discovery_secs,
            stale_secs,
            rotate_secs,
            max_symbols,
            duration_secs,
        } => {
            collect(
                Config {
                    discovery_url,
                    websocket_url,
                    shard_size,
                    replicas,
                    audit_catalog,
                    discovery_secs,
                    stale_secs,
                    rotate_secs,
                    max_symbols,
                },
                duration_secs,
                &mut output,
            )
            .await
        }
        Command::Inspect { .. } => unreachable!(),
    };
    let flushed = output.finish();
    result.and(flushed)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn universe_reconciliation_keeps_shared_asset_allocation() {
        let directory = tempfile::tempdir().unwrap();
        let mut output = Output::new(Some(&directory.path().join("assets.sbe")), false).unwrap();
        let mut engine = Engine::new(2, 0);
        let asset: orderbook_collector::assets::Asset =
            std::sync::Arc::from("catalog-shared-allocation");
        let weak = std::sync::Arc::downgrade(&asset);
        let (tx, _rx) = tokio::sync::mpsc::channel(2);
        handle(
            Input::Universe([asset].into()),
            &mut engine,
            &mut output,
            &mut 0,
            &mut None,
            &tx,
        )
        .unwrap();
        assert_eq!(engine.symbol_count(), 1);
        assert!(
            weak.upgrade().is_some(),
            "reconciliation converted a shared asset to a fresh string allocation"
        );
        drop(engine);
        assert!(weak.upgrade().is_none());
    }
    #[test]
    fn normalized_lifecycle_event_sends_a_hint_without_fabricating_a_book() {
        let directory = tempfile::tempdir().unwrap();
        let mut output = Output::new(Some(&directory.path().join("hint.sbe")), false).unwrap();
        let mut engine = Engine::new(2, 0);
        let mut delay = None;
        let (tx, mut rx) = tokio::sync::mpsc::channel(2);
        apply_events(
            0,
            vec![adapter::Event::NewMarket {
                symbols: vec!["new-yes".into(), "new-no".into()],
            }],
            1_000,
            &mut engine,
            &mut output,
            &mut delay,
            &tx,
        )
        .unwrap();
        assert!(
            matches!(rx.try_recv().unwrap(),live::Hint::NewMarket(symbols) if symbols==["new-yes","new-no"])
        );
        assert_eq!(engine.symbol_count(), 0);
        assert_eq!(output.records, 0);
    }
    #[test]
    fn engine_guard_failure_does_not_stop_other_symbols() {
        let directory = tempfile::tempdir().unwrap();
        let mut output = Output::new(Some(&directory.path().join("guard.sbe")), false).unwrap();
        let mut engine = Engine::new(2, 0);
        let mut delay = None;
        engine.set_universe(BTreeSet::from(["a".into(), "b".into()]));
        let (tx, _rx) = tokio::sync::mpsc::channel(2);
        let snapshot = |symbol: &str, time| adapter::Event::Snapshot {
            symbol: symbol.into(),
            timestamp_ms: time,
            bids: vec![],
            asks: vec![],
        };
        apply_events(
            0,
            vec![snapshot("a", 1_000), snapshot("b", 1_000)],
            1_000,
            &mut engine,
            &mut output,
            &mut delay,
            &tx,
        )
        .unwrap();
        apply_events(
            0,
            vec![snapshot("a", 999), snapshot("b", 1_001)],
            1_001,
            &mut engine,
            &mut output,
            &mut delay,
            &tx,
        )
        .unwrap();
        assert_eq!(engine.symbol_count(), 2);
        assert_eq!(engine.ready_count(1_001), 1);
        assert_eq!(output.records, 3);
    }
    #[tokio::test(start_paused = true)]
    async fn minute_and_health_timers_are_not_starved_by_ready_input() {
        let cancel = CancellationToken::new();
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        tx.send(Input::Universe(BTreeSet::new())).await.unwrap();
        let mut producer = tokio::spawn(std::future::pending::<Result<()>>());
        let deadline = tokio::time::sleep(Duration::from_secs(3600));
        tokio::pin!(deadline);
        let mut snapshots = tokio::time::interval_at(
            tokio::time::Instant::now() + Duration::from_secs(60),
            Duration::from_secs(60),
        );
        let mut health = tokio::time::interval(Duration::from_secs(10));
        tokio::time::advance(Duration::from_secs(60)).await;
        assert!(matches!(
            next_action(
                &cancel,
                &mut producer,
                &mut rx,
                deadline.as_mut(),
                &mut snapshots,
                &mut health
            )
            .await,
            Action::Snapshot
        ));
        assert!(matches!(
            next_action(
                &cancel,
                &mut producer,
                &mut rx,
                deadline.as_mut(),
                &mut snapshots,
                &mut health
            )
            .await,
            Action::Health
        ));
        producer.abort();
        assert!(producer.await.unwrap_err().is_cancelled());
    }
}
