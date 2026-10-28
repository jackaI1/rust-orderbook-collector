//! Read-only Gamma discovery and market-stream transport. Each source has one
//! serial owner; its disconnect is queued before a later connection can emit.
//! Protocol references: https://docs.polymarket.com/market-data/discover-markets
//! and https://docs.polymarket.com/market-data/realtime-data .
use crate::assets::{Asset, intern};
use anyhow::{Context, Result, bail, ensure};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};
use tokio::{
    sync::{Mutex, mpsc},
    task::JoinSet,
    time::{Instant, sleep},
};
use tokio_util::sync::CancellationToken;

const MAX_CONNECTIONS: usize = 128;
const IO_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Clone)]
pub struct Config {
    /// Gamma origin, /markets/keyset, or /events/keyset endpoint.
    pub discovery_url: String,
    pub websocket_url: String,
    pub shard_size: usize,
    pub replicas: usize,
    pub discovery_secs: u64,
    pub stale_secs: u64,
    pub rotate_secs: u64,
    /// An explicit deterministic selection, applied only after full discovery.
    pub max_symbols: Option<usize>,
    /// Compare one bounded standalone market page with the completed event catalog.
    pub audit_catalog: bool,
}

#[derive(Debug)]
pub enum Input {
    Universe(BTreeSet<Asset>),
    AddSymbols(BTreeSet<Asset>),
    RemoveSymbols(BTreeSet<Asset>),
    Frame {
        source: usize,
        text: String,
        received_ms: i64,
    },
    /// A wire frame decoded and atomically validated by its source task.
    Events {
        source: usize,
        events: Vec<crate::adapter::Event>,
        received_ms: i64,
    },
    Disconnected {
        source: usize,
    },
    /// Invalidate only these source books before requesting fresh snapshots.
    Reset {
        source: usize,
        symbols: BTreeSet<String>,
    },
    Fatal(String),
}

#[derive(Debug)]
pub enum Hint {
    NewMarket(Vec<String>),
    Resolved(Vec<String>),
}

fn failure_log(stage: &str, source: Option<usize>, failures: u32, reason: &str) {
    let reason: String = reason.chars().take(512).collect();
    eprintln!(
        "{}",
        json!({"event":"collector_failure", "stage":stage, "source":source,
        "consecutive_failures":failures, "reason":reason})
    );
}

type SharedAssets = Arc<BTreeSet<Asset>>;
type Shards = BTreeMap<usize, SharedAssets>;
type Markets = Arc<BTreeMap<String, Arc<BTreeSet<Asset>>>>;
struct Discovery {
    symbols: BTreeSet<Asset>,
    markets: Markets,
}

#[derive(Debug)]
struct AccessDenied(String);
impl std::fmt::Display for AccessDenied {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for AccessDenied {}

/// Run until cancellation, consumer closure, or a fatal error. All connection
/// tasks are canceled and joined before this future returns. The caller should
/// cancel the token and await this future rather than aborting/dropping it.
pub async fn run(config: Config, tx: mpsc::Sender<Input>, cancel: CancellationToken) -> Result<()> {
    let (_hints_tx, hints_rx) = mpsc::channel(64);
    run_with_hints(config, tx, cancel, hints_rx).await
}

pub async fn run_with_hints(
    config: Config,
    tx: mpsc::Sender<Input>,
    cancel: CancellationToken,
    mut hints: mpsc::Receiver<Hint>,
) -> Result<()> {
    let stop = cancel.child_token();
    let mut tasks = JoinSet::new();
    let result = drive(&config, &tx, &stop, &mut tasks, &mut hints).await;
    stop.cancel();
    // Workers bound their close handshake, and every blocked send/dial observes
    // stop. Draining also reaps workers canceled while a reconciliation failed.
    let mut result = result;
    while let Some(joined) = tasks.join_next().await {
        if let Err(error) = joined
            && result.is_ok()
        {
            result = Err(error.into());
        }
    }
    if cancel.is_cancelled() || tx.is_closed() {
        return Ok(());
    }
    if let Err(error) = &result {
        emit(&tx, Input::Fatal(format!("{error:#}")), &cancel).await;
    }
    result
}

fn validate(config: &Config) -> Result<()> {
    // Zero selects a capacity sized to a small bounded number of sockets.

    ensure!(
        (1..=MAX_CONNECTIONS).contains(&config.replicas),
        "replicas must be between 1 and {MAX_CONNECTIONS}; reduce replicas"
    );
    for (name, seconds) in [
        ("discovery_secs", config.discovery_secs),
        ("stale_secs", config.stale_secs),
        ("rotate_secs", config.rotate_secs),
    ] {
        ensure!(
            (1..=31_536_000).contains(&seconds),
            "{name} must be between 1 second and 365 days"
        );
    }
    let url = reqwest::Url::parse(&config.websocket_url).context("invalid websocket_url")?;
    ensure!(
        matches!(url.scheme(), "ws" | "wss"),
        "websocket_url must use ws or wss"
    );
    Ok(())
}

async fn drive(
    config: &Config,
    tx: &mpsc::Sender<Input>,
    stop: &CancellationToken,
    tasks: &mut JoinSet<(usize, Result<()>)>,
    hints: &mut mpsc::Receiver<Hint>,
) -> Result<()> {
    validate(config)?;
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(IO_TIMEOUT)
        .timeout(Duration::from_secs(20))
        .build()?;
    let empty_catalog = Arc::new(BTreeMap::new());
    let mut startup_failures = 0u32;
    let first = loop {
        let result = tokio::select! { biased;
            _ = stop.cancelled() => return Ok(()),
            _ = tx.closed() => return Ok(()),
            result = discover(&client, config, &empty_catalog) => result,
        };
        match result {
            Ok(discovery) => break discovery,
            Err(error) if error.downcast_ref::<AccessDenied>().is_some() => return Err(error),
            Err(error) => {
                startup_failures = startup_failures.saturating_add(1);
                failure_log(
                    "initial_discovery",
                    None,
                    startup_failures,
                    &format!("{error:#}"),
                );
                tokio::select! {
                    _ = stop.cancelled() => return Ok(()),
                    _ = tx.closed() => return Ok(()),
                    _ = sleep(backoff(startup_failures,0)) => {},
                }
            }
        }
    };
    let mut topology = Topology::new();
    let mut catalog = first.markets;
    let mut pending = first.symbols;
    let mut failures = 0u32;
    let mut fast_hints = BTreeMap::<Asset, Instant>::new();
    let mut hints_open = true;
    let mut pending_additions = BTreeSet::<Asset>::new();
    let mut pending_removals = BTreeSet::<Asset>::new();
    let mut hint_flush = tokio::time::interval(Duration::from_secs(1));
    hint_flush.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        // A completed sweep must not consume a queued resolution before its
        // ordered removal notification reaches the engine.
        if !pending_removals.is_empty() {
            topology
                .remove_symbols(
                    std::mem::take(&mut pending_removals),
                    config,
                    tx,
                    stop,
                    tasks,
                )
                .await?;
        }
        topology.reconcile(pending, config, tx, stop, tasks).await?;
        let delay = Duration::from_secs(config.discovery_secs).max(if failures == 0 {
            Duration::ZERO
        } else {
            backoff(failures, 0)
        });
        // Keep the complete discovery future alive while observing worker exits.
        // A WS denial can therefore stop even during a slow HTTP response.
        let prior_catalog = catalog.clone();
        let discovery = async {
            sleep(delay).await;
            discover(&client, config, &prior_catalog).await
        };
        tokio::pin!(discovery);
        let result = loop {
            tokio::select! { biased;
                _ = stop.cancelled() => return Ok(()),
                _ = tx.closed() => return Ok(()),
                joined = tasks.join_next(), if !tasks.is_empty() => {
                    let (source, result) = joined.context("missing worker")?.context("connection task panicked")?;
                    result?; bail!("connection source {source} stopped unexpectedly");
                },
                _ = hint_flush.tick(), if !pending_additions.is_empty() || !pending_removals.is_empty() => {
                    if !pending_removals.is_empty() {
                        let removals=std::mem::take(&mut pending_removals);
                        topology.remove_symbols(removals,config,tx,stop,tasks).await?;
                    }
                    let additions=std::mem::take(&mut pending_additions).into_iter()
                        .filter(|asset|!topology.universe.contains(asset) && !topology.resolved.contains(asset)).collect::<BTreeSet<_>>();
                    if !additions.is_empty() {
                        for asset in &additions {fast_hints.insert(asset.clone(),Instant::now());}
                        let added=additions.len();topology.add_symbols(additions,config,tx,stop,tasks).await?;
                        eprintln!("{}",json!({"event":"lifecycle_fast_add","added":added,"provisional_hints":fast_hints.len(),"symbols":topology.universe.len()}));
                    }
                },
                result = &mut discovery => break result,
                hint = hints.recv(), if hints_open => {
                    let was_empty=pending_additions.is_empty() && pending_removals.is_empty();
                    match hint {
                        Some(Hint::NewMarket(symbols)) if config.max_symbols.is_none() => {
                            for symbol in symbols {
                                if !topology.universe.contains(symbol.as_str()) && !topology.resolved.contains(symbol.as_str())
                                    && !pending_additions.contains(symbol.as_str()) {pending_additions.insert(intern(&symbol));}
                            }
                        },
                        Some(Hint::Resolved(symbols)) => {
                            for symbol in symbols {let asset=intern(&symbol);pending_additions.remove(&asset);fast_hints.remove(&asset);
                                topology.resolved.insert(asset.clone());pending_removals.insert(asset);}
                        },
                        None => hints_open=false,
                        _ => {},
                    }
                    // An elapsed disabled interval must not flush the first
                    // member immediately. Start a bounded coalescing window.
                    if was_empty && (!pending_additions.is_empty() || !pending_removals.is_empty()) { hint_flush.reset(); }
                },
            }
        };
        match result {
            Ok(complete) => {
                fast_hints.retain(|asset, seen| {
                    !complete.symbols.contains(asset)
                        && !topology.resolved.contains(asset)
                        && seen.elapsed() < Duration::from_secs(600)
                });
                pending = complete.symbols;
                pending.extend(fast_hints.keys().cloned());
                catalog = complete.markets;
                failures = 0;
            }
            Err(error) if error.downcast_ref::<AccessDenied>().is_some() => return Err(error),
            // Never publish or reconcile a partial sweep. Existing workers keep
            // their subscriptions through HTTP, JSON, and pagination failures.
            Err(error) => {
                failures = failures.saturating_add(1);
                failure_log("discovery", None, failures, &format!("{error:#}"));
                pending = topology.universe.clone();
            }
        }
    }
}

struct Topology {
    shards: Shards,
    owners: BTreeMap<usize, (CancellationToken, tokio::sync::watch::Sender<SharedAssets>)>,
    source_ids: Arc<std::sync::atomic::AtomicUsize>,
    dial: Arc<Mutex<Instant>>,
    universe: BTreeSet<Asset>,
    initialized: bool,
    warm_slots: Arc<tokio::sync::Semaphore>,
    subscriptions: Arc<crate::connection::SubscriptionBudget>,
    resolved: BTreeSet<Asset>,
}
impl Topology {
    fn new() -> Self {
        Self {
            shards: Shards::new(),
            owners: BTreeMap::new(),
            source_ids: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            dial: Arc::new(Mutex::new(Instant::now())),
            universe: BTreeSet::new(),
            initialized: false,
            warm_slots: Arc::new(tokio::sync::Semaphore::new(4)),
            subscriptions: Arc::new(crate::connection::SubscriptionBudget::new()),
            resolved: BTreeSet::new(),
        }
    }
    async fn reconcile(
        &mut self,
        mut pending: BTreeSet<Asset>,
        config: &Config,
        tx: &mpsc::Sender<Input>,
        stop: &CancellationToken,
        tasks: &mut JoinSet<(usize, Result<()>)>,
    ) -> Result<()> {
        pending.retain(|asset| !self.resolved.contains(asset));
        if self.initialized && pending == self.universe {
            crate::assets::prune();
            return Ok(());
        }

        let target_shards = (128 / config.replicas).max(1);
        let capacity = if config.shard_size == 0 {
            200.max(pending.len().div_ceil(target_shards))
        } else {
            config.shard_size
        };
        let proposed = assign(&self.shards, &pending, capacity);
        let connections = proposed
            .len()
            .checked_mul(config.replicas)
            .context("connection count overflow")?;
        ensure!(
            connections <= MAX_CONNECTIONS,
            "discovery requires {connections} WebSocket connections (limit {MAX_CONNECTIONS}); increase shard_size, reduce replicas, or explicitly set max_symbols"
        );
        self.stop_removed(&proposed, tx, stop, tasks).await?;
        // Observe worker failures while this update waits for consumer capacity,
        // so an access denial stops all other workers even if output is blocked.
        let published = self
            .publish(Input::Universe(pending.clone()), tx, stop, tasks)
            .await?;
        if !published {
            return Ok(());
        }
        self.update_workers(&proposed, config, tx, stop, tasks);
        self.shards = proposed;
        self.universe = pending;
        self.initialized = true;
        crate::assets::prune();
        Ok(())
    }
    async fn publish(
        &self,
        input: Input,
        tx: &mpsc::Sender<Input>,
        stop: &CancellationToken,
        tasks: &mut JoinSet<(usize, Result<()>)>,
    ) -> Result<bool> {
        let published = tokio::select! { biased;
            _ = stop.cancelled() => false,
            joined = tasks.join_next(), if !tasks.is_empty() => {
                let (source, result) = joined.context("missing worker")?.context("connection task panicked")?;
                result?;
                bail!("connection source {source} stopped unexpectedly");
            },
            result = tx.send(input) => result.is_ok(),
        };
        Ok(published)
    }
    fn update_workers(
        &mut self,
        proposed: &Shards,
        config: &Config,
        tx: &mpsc::Sender<Input>,
        stop: &CancellationToken,
        tasks: &mut JoinSet<(usize, Result<()>)>,
    ) {
        for (&id, assets) in proposed {
            if let Some((_, updates)) = self.owners.get(&id) {
                if self.shards.get(&id) != Some(assets) {
                    updates.send_replace(assets.clone());
                }
                continue;
            }
            let local = stop.child_token();
            let (updates, receiver) = tokio::sync::watch::channel(assets.clone());
            self.owners.insert(id, (local.clone(), updates));
            let config = config.clone();
            let tx = tx.clone();
            let dial = self.dial.clone();
            let sources = self.source_ids.clone();
            let warm_slots = self.warm_slots.clone();
            let subscriptions = self.subscriptions.clone();
            tasks.spawn(async move {
                (
                    id,
                    crate::connection::run_shard_shared_limited(
                        config,
                        receiver,
                        tx,
                        local,
                        dial,
                        sources,
                        warm_slots,
                        subscriptions,
                    )
                    .await,
                )
            });
        }
    }
    async fn add_symbols(
        &mut self,
        additions: BTreeSet<Asset>,
        config: &Config,
        tx: &mpsc::Sender<Input>,
        stop: &CancellationToken,
        tasks: &mut JoinSet<(usize, Result<()>)>,
    ) -> Result<()> {
        let capacity = if config.shard_size == 0 {
            200.max(
                (self.universe.len() + additions.len()).div_ceil((128 / config.replicas).max(1)),
            )
        } else {
            config.shard_size
        };
        let mut proposed = self.shards.clone();
        for symbol in &additions {
            let id = proposed
                .iter()
                .find(|(_, assets)| assets.len() < capacity)
                .map(|(&id, _)| id)
                .unwrap_or_else(|| (0..).find(|id| !proposed.contains_key(id)).unwrap());
            let assets = proposed
                .entry(id)
                .or_insert_with(|| Arc::new(BTreeSet::new()));
            Arc::make_mut(assets).insert(symbol.clone());
        }
        ensure!(
            proposed
                .len()
                .checked_mul(config.replicas)
                .is_some_and(|count| count <= MAX_CONNECTIONS),
            "lifecycle additions exceed connection guard; increase shard_size or reduce replicas"
        );
        if !self
            .publish(Input::AddSymbols(additions.clone()), tx, stop, tasks)
            .await?
        {
            return Ok(());
        }
        self.update_workers(&proposed, config, tx, stop, tasks);
        self.shards = proposed;
        self.universe.extend(additions);
        Ok(())
    }
    async fn stop_removed(
        &mut self,
        proposed: &Shards,
        tx: &mpsc::Sender<Input>,
        stop: &CancellationToken,
        tasks: &mut JoinSet<(usize, Result<()>)>,
    ) -> Result<()> {
        // Only fully removed shards stop. Asset changes use live subscription
        // updates so neither replica loses its existing books.
        let removed: Vec<_> = self
            .shards
            .keys()
            .filter(|id| !proposed.contains_key(id))
            .copied()
            .collect();
        let mut stopping = BTreeSet::new();
        for id in removed {
            if let Some((token, _)) = self.owners.remove(&id) {
                token.cancel();
                stopping.insert(id);
            }
        }
        while !stopping.is_empty() {
            let joined = tokio::select! {biased;
                _=stop.cancelled()=>return Ok(()),
                _=tx.closed()=>return Ok(()),
                joined=tasks.join_next()=>joined.context("missing shard task")?,
            };
            let (id, result) = joined.context("shard task panicked")?;
            result?;
            ensure!(stopping.remove(&id), "shard {id} stopped unexpectedly");
        }
        Ok(())
    }
    async fn remove_symbols(
        &mut self,
        removals: BTreeSet<Asset>,
        config: &Config,
        tx: &mpsc::Sender<Input>,
        stop: &CancellationToken,
        tasks: &mut JoinSet<(usize, Result<()>)>,
    ) -> Result<()> {
        let removed: BTreeSet<_> = removals
            .into_iter()
            .filter(|asset| self.universe.contains(asset))
            .collect();
        if removed.is_empty() {
            return Ok(());
        }
        let mut proposed = self.shards.clone();
        for assets in proposed.values_mut() {
            if removed.iter().any(|asset| assets.contains(asset)) {
                let owned = Arc::make_mut(assets);
                for asset in &removed {
                    owned.remove(asset);
                }
            }
        }
        proposed.retain(|_, assets| !assets.is_empty());
        if !self
            .publish(Input::RemoveSymbols(removed.clone()), tx, stop, tasks)
            .await?
        {
            return Ok(());
        }
        self.stop_removed(&proposed, tx, stop, tasks).await?;
        self.update_workers(&proposed, config, tx, stop, tasks);
        self.shards = proposed;
        for asset in &removed {
            self.universe.remove(asset);
        }
        eprintln!(
            "{}",
            json!({"event":"lifecycle_remove","removed":removed.len(),"symbols":self.universe.len()})
        );
        Ok(())
    }
}

enum MarketRecord {
    Inactive,
    Unresolved(String),
    Candidate {
        id: String,
        assets: BTreeSet<Asset>,
        uncertain: bool,
    },
}

fn parse_market(market: &Market) -> Result<MarketRecord> {
    let flags = [
        ("active", true),
        ("closed", false),
        ("enableOrderBook", true),
        ("acceptingOrders", true),
    ];
    if flags.iter().any(|(flag, expected)| {
        market
            .get(flag)
            .and_then(Value::as_bool)
            .is_some_and(|actual| actual != *expected)
    }) {
        return Ok(MarketRecord::Inactive);
    }
    let mut uncertain = false;
    for (flag, _) in flags {
        match market.get(flag) {
            Some(Value::Bool(_)) => {}
            None | Some(Value::Null) => uncertain = true,
            _ => bail!("malformed eligibility flag {flag}"),
        }
    }
    let id = market
        .get("id")
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
        .context("missing market ID")?;
    let canonical: Option<Vec<String>> = match market.get("version").and_then(Value::as_str) {
        Some("v1") => match market.get("clobTokenIds") {
            Some(Value::String(text)) => {
                Some(serde_json::from_str(text).context("malformed v1 canonical IDs")?)
            }
            None | Some(Value::Null) => None,
            _ => bail!("v1 canonical IDs must be a JSON string"),
        },
        Some("v2") => match market.get("positionIds") {
            Some(Value::Array(ids)) => Some(
                serde_json::from_value(Value::Array(ids.clone()))
                    .context("malformed v2 canonical IDs")?,
            ),
            None | Some(Value::Null) => None,
            _ => bail!("v2 canonical IDs must be an array"),
        },
        _ => None,
    };
    let Some(ids) = canonical.filter(|ids| !ids.is_empty()) else {
        return Ok(MarketRecord::Unresolved(
            "canonical IDs not yet available or unsupported protocol".into(),
        ));
    };
    let assets: BTreeSet<_> = ids.iter().map(|id| intern(id)).collect();
    ensure!(assets.len() == ids.len(), "duplicate canonical IDs");
    ensure!(
        ids.iter()
            .all(|id| !id.trim().is_empty() && id.trim() == id),
        "empty/padded canonical ID"
    );
    if let Some(outcomes) = market.get("outcomes").filter(|value| !value.is_null()) {
        let outcomes: Vec<Value> = if let Some(text) = outcomes.as_str() {
            serde_json::from_str(text)?
        } else {
            serde_json::from_value(outcomes.clone())?
        };
        ensure!(
            outcomes.len() == ids.len(),
            "canonical IDs do not align with outcomes"
        );
    }
    Ok(MarketRecord::Candidate {
        id: id.into(),
        assets,
        uncertain,
    })
}

#[derive(Default)]
struct Market {
    id: Option<Value>,
    version: Option<Value>,
    active: Option<Value>,
    closed: Option<Value>,
    enable_order_book: Option<Value>,
    accepting_orders: Option<Value>,
    clob_token_ids: Option<Value>,
    position_ids: Option<Value>,
    outcomes: Option<Value>,
}
// Decode only used fields; large optional metadata is skipped without storing
// it. A non-object record becomes a quarantinable empty record, not a failed page.
impl<'de> Deserialize<'de> for Market {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = Market;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("market metadata")
            }
            fn visit_map<M: serde::de::MapAccess<'de>>(
                self,
                mut map: M,
            ) -> std::result::Result<Market, M::Error> {
                let mut market = Market::default();
                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "id" => market.id = Some(map.next_value()?),
                        "version" => market.version = Some(map.next_value()?),
                        "active" => market.active = Some(map.next_value()?),
                        "closed" => market.closed = Some(map.next_value()?),
                        "enableOrderBook" => market.enable_order_book = Some(map.next_value()?),
                        "acceptingOrders" => market.accepting_orders = Some(map.next_value()?),
                        "clobTokenIds" => market.clob_token_ids = Some(map.next_value()?),
                        "positionIds" => market.position_ids = Some(map.next_value()?),
                        "outcomes" => market.outcomes = Some(map.next_value()?),
                        _ => {
                            map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(market)
            }
            fn visit_seq<S: serde::de::SeqAccess<'de>>(
                self,
                mut seq: S,
            ) -> std::result::Result<Market, S::Error> {
                while seq.next_element::<serde::de::IgnoredAny>()?.is_some() {}
                Ok(Market::default())
            }
            fn visit_bool<E: serde::de::Error>(self, _: bool) -> std::result::Result<Market, E> {
                Ok(Market::default())
            }
            fn visit_i64<E: serde::de::Error>(self, _: i64) -> std::result::Result<Market, E> {
                Ok(Market::default())
            }
            fn visit_u64<E: serde::de::Error>(self, _: u64) -> std::result::Result<Market, E> {
                Ok(Market::default())
            }
            fn visit_f64<E: serde::de::Error>(self, _: f64) -> std::result::Result<Market, E> {
                Ok(Market::default())
            }
            fn visit_str<E: serde::de::Error>(self, _: &str) -> std::result::Result<Market, E> {
                Ok(Market::default())
            }
            fn visit_unit<E: serde::de::Error>(self) -> std::result::Result<Market, E> {
                Ok(Market::default())
            }
        }
        deserializer.deserialize_any(Visitor)
    }
}
impl Market {
    fn get(&self, field: &str) -> Option<&Value> {
        match field {
            "id" => self.id.as_ref(),
            "version" => self.version.as_ref(),
            "active" => self.active.as_ref(),
            "closed" => self.closed.as_ref(),
            "enableOrderBook" => self.enable_order_book.as_ref(),
            "acceptingOrders" => self.accepting_orders.as_ref(),
            "clobTokenIds" => self.clob_token_ids.as_ref(),
            "positionIds" => self.position_ids.as_ref(),
            "outcomes" => self.outcomes.as_ref(),
            _ => None,
        }
    }
}
#[derive(Deserialize)]
struct EventMetadata {
    #[serde(default)]
    markets: Option<Vec<Market>>,
}
#[derive(Deserialize)]
struct GammaPage {
    #[serde(default)]
    markets: Option<Vec<Market>>,
    #[serde(default)]
    events: Option<Vec<EventMetadata>>,
    #[serde(default)]
    next_cursor: Option<Value>,
}

#[derive(Debug)]
struct PageTooLarge;
impl std::fmt::Display for PageTooLarge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Gamma page exceeds32MiB")
    }
}
impl std::error::Error for PageTooLarge {}

async fn fetch_page(client: &reqwest::Client, url: reqwest::Url) -> Result<GammaPage> {
    let mut last_error = None;
    for attempt in 0..4u32 {
        let mut retry_after = None;
        let result: Result<GammaPage> = async {
            let mut response = client
                .get(url.clone())
                .send()
                .await
                .context("Gamma request failed")?;
            let status = response.status();
            if status.as_u16() == 429 {
                retry_after = response
                    .headers()
                    .get(reqwest::header::RETRY_AFTER)
                    .and_then(|h| h.to_str().ok())
                    .and_then(|s| s.parse::<u64>().ok())
                    .map(Duration::from_secs);
            }
            if matches!(status.as_u16(), 401 | 403) {
                return Err(
                    AccessDenied(format!("Gamma HTTP {status}; stopped without retry")).into(),
                );
            }
            ensure!(status.is_success(), "Gamma HTTP {status}");
            if response
                .content_length()
                .is_some_and(|length| length > 32 * 1024 * 1024)
            {
                return Err(PageTooLarge.into());
            }
            let mut bytes = Vec::new();
            while let Some(chunk) = response.chunk().await? {
                if bytes.len().saturating_add(chunk.len()) > 32 * 1024 * 1024 {
                    return Err(PageTooLarge.into());
                }
                bytes.extend_from_slice(&chunk);
            }
            serde_json::from_slice(&bytes).context("invalid Gamma page JSON")
        }
        .await;
        match result {
            Ok(page) => return Ok(page),
            Err(error)
                if error.downcast_ref::<AccessDenied>().is_some()
                    || error.downcast_ref::<PageTooLarge>().is_some() =>
            {
                return Err(error);
            }
            Err(error) => {
                failure_log("discovery_page", None, attempt + 1, &format!("{error:#}"));
                last_error = Some(error);
                if attempt < 3 {
                    sleep(retry_after.unwrap_or_else(|| backoff(attempt + 1, 0))).await;
                }
            }
        }
    }
    Err(last_error.expect("four attempts produced an error"))
}

async fn discover(client: &reqwest::Client, config: &Config, prior: &Markets) -> Result<Discovery> {
    let mut endpoint =
        reqwest::Url::parse(&config.discovery_url).context("invalid discovery_url")?;
    ensure!(
        matches!(endpoint.scheme(), "http" | "https"),
        "discovery_url must use http or https"
    );
    match endpoint.path().trim_end_matches('/') {
        "" => endpoint.set_path("/markets/keyset"),
        "/markets" => endpoint.set_path("/markets/keyset"),
        _ => {}
    }
    let preserved: Vec<_> = endpoint
        .query_pairs()
        .filter(|(key, _)| {
            !matches!(
                key.as_ref(),
                "closed" | "limit" | "version" | "after_cursor" | "offset"
            )
        })
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect();
    endpoint.set_query(None);
    endpoint
        .query_pairs_mut()
        .extend_pairs(preserved)
        .append_pair("closed", "false")
        .append_pair("version", "v1")
        .append_pair("version", "v2");
    let mut page_size = 100usize;
    let mut cursor = None::<String>;
    let mut visited = BTreeSet::new();
    let mut symbols = BTreeSet::new();
    let mut scanned = 0usize;
    let mut provisional = BTreeSet::new();
    let mut provisional_markets = 0usize;
    let mut markets_by_id = BTreeMap::new();
    let mut unresolved = 0usize;
    let mut unresolved_sample = Vec::new();
    let mut quarantined = 0usize;
    let mut seen_ids = BTreeSet::new();
    let mut unknown_record = false;
    let mut quarantined_events = 0usize;
    let mut page_failures = 0u32;
    // A bad endpoint cannot create an unbounded cursor loop or silently publish
    // the prefix when it reaches a guard.
    for _ in 0..10_000 {
        let mut url = endpoint.clone();
        url.query_pairs_mut()
            .append_pair("limit", &page_size.to_string());
        if let Some(cursor) = &cursor {
            url.query_pairs_mut().append_pair("after_cursor", cursor);
        }
        let page = match fetch_page(client, url).await {
            Err(error) if error.downcast_ref::<PageTooLarge>().is_some() && page_size > 1 => {
                page_size = (page_size / 2).max(1);
                eprintln!(
                    "{}",
                    json!({"event":"discovery_page_size_reduced","limit":page_size})
                );
                continue;
            }
            Err(error)
                if error.downcast_ref::<AccessDenied>().is_some()
                    || error.downcast_ref::<PageTooLarge>().is_some() =>
            {
                return Err(error);
            }
            Err(error) => {
                page_failures = page_failures.saturating_add(1);
                failure_log(
                    "discovery_cursor_resume",
                    None,
                    page_failures,
                    &format!("{error:#}"),
                );
                sleep(backoff(page_failures, 0)).await;
                continue;
            }
            Ok(page) => {
                page_failures = 0;
                page
            }
        };
        let is_events = endpoint.path().contains("/events");
        let mut market_refs = Vec::new();
        let page_len;
        if is_events {
            let events = page
                .events
                .as_ref()
                .context("Gamma event page requires events array")?;
            page_len = events.len();
            for event in events {
                if let Some(nested) = &event.markets {
                    market_refs.extend(nested.iter());
                } else {
                    quarantined_events += 1;
                    unknown_record = true;
                }
            }
        } else {
            let markets = page
                .markets
                .as_ref()
                .context("Gamma market page requires markets array")?;
            page_len = markets.len();
            market_refs.extend(markets.iter());
        }
        for market in market_refs {
            scanned += 1;
            let id = market
                .get("id")
                .and_then(Value::as_str)
                .filter(|id| !id.is_empty());
            if let Some(id) = id {
                seen_ids.insert(id.to_owned());
            }
            let parsed = parse_market(market);
            match parsed {
                Ok(MarketRecord::Inactive) => {}
                Ok(MarketRecord::Candidate {
                    id,
                    assets,
                    uncertain,
                }) => {
                    if uncertain {
                        provisional_markets += 1;
                        provisional.extend(assets.iter().cloned());
                    }
                    symbols.extend(assets.iter().cloned());
                    markets_by_id.insert(id, Arc::new(assets));
                }
                other => {
                    unresolved += 1;
                    let reason = match other {
                        Ok(MarketRecord::Unresolved(reason)) => reason,
                        Err(error) => {
                            quarantined += 1;
                            error.to_string()
                        }
                        _ => unreachable!(),
                    };
                    if unresolved_sample.len() < 8 {
                        unresolved_sample.push(json!({"market":id,"reason":reason}));
                    }
                    if let Some(id) = id {
                        let retained = prior.get(id).cloned().unwrap_or_default();
                        symbols.extend(retained.iter().cloned());
                        provisional.extend(retained.iter().cloned());
                        markets_by_id.insert(id.to_owned(), retained);
                    } else {
                        unknown_record = true;
                    }
                }
            }
        }
        if (visited.len() + 1).is_multiple_of(25) {
            eprintln!(
                "{}",
                json!({"event":"discovery_progress","pages":visited.len()+1,"markets_scanned":scanned,"canonical_candidates":symbols.len(),"unresolved_markets":unresolved})
            );
        }
        cursor = match page.next_cursor.as_ref() {
            None if page_len >= page_size => {
                bail!("full keyset page omitted cursor; refusing truncated universe")
            }
            None | Some(Value::Null) => None,
            Some(Value::String(cursor)) if cursor.is_empty() => None,
            Some(Value::String(cursor)) => Some(cursor.clone()),
            _ => bail!("Gamma next_cursor must be string or null"),
        };
        match &cursor {
            None => {
                let mut deferred_removals = 0usize;
                if unknown_record {
                    for (id, assets) in prior.iter() {
                        if !seen_ids.contains(id) {
                            markets_by_id.insert(id.clone(), assets.clone());
                            symbols.extend(assets.iter().cloned());
                            provisional.extend(assets.iter().cloned());
                            deferred_removals += 1;
                        }
                    }
                }
                if config.audit_catalog && is_events {
                    let mut audit_url = endpoint.clone();
                    audit_url.set_path("/markets/keyset");
                    audit_url.query_pairs_mut().append_pair("limit", "100");
                    let audit = fetch_page(client, audit_url).await?;
                    let records = audit
                        .markets
                        .context("catalog audit requires standalone markets")?;
                    let mut compared = BTreeSet::new();
                    let mut unresolved_records = 0;
                    for record in &records {
                        match parse_market(record) {
                            Ok(MarketRecord::Candidate { assets, .. }) => compared.extend(assets),
                            Ok(MarketRecord::Inactive) => {}
                            _ => unresolved_records += 1,
                        }
                    }
                    let missing: Vec<_> = compared.difference(&symbols).cloned().collect();
                    eprintln!(
                        "{}",
                        json!({"event":"catalog_sample_crosscheck","sample_market_records":records.len(),
                        "sample_assets":compared.len(),"missing_assets":missing.len(),"unresolved_sample_records":unresolved_records,
                        "missing_sample":missing.iter().take(8).collect::<Vec<_>>(),
                        "scope":"one standalone market keyset page; not full catalog reconciliation"})
                    );
                    // A sampled omission prevents claiming a complete universe.
                    ensure!(
                        missing.is_empty(),
                        "standalone market sample found assets missing from event catalog; use /markets/keyset for authoritative discovery"
                    );
                }
                eprintln!(
                    "{}",
                    json!({"event":"discovery_complete", "sweep_complete":true, "pages":visited.len()+1,
                    "catalog_scope":if is_events { "open_events_embedded_markets" } else { "standalone_open_markets" },
                    "markets_scanned":scanned, "canonical_candidates":symbols.len(), "provisional_markets":provisional_markets,
                    "provisional_symbols":provisional.len(), "confirmed_eligible_symbols":symbols.len()-provisional.len(),
                    "unresolved_markets":unresolved, "quarantined_markets":quarantined, "quarantined_events":quarantined_events, "deferred_removals":deferred_removals,"unresolved_sample":unresolved_sample,
                    "diagnostic_limit":config.max_symbols})
                );
                let selected = symbols
                    .into_iter()
                    .take(config.max_symbols.unwrap_or(usize::MAX))
                    .collect();
                return Ok(Discovery {
                    symbols: selected,
                    markets: Arc::new(markets_by_id),
                });
            }
            Some(cursor) => {
                ensure!(
                    visited.insert(cursor.clone()),
                    "Gamma repeated keyset cursor; refusing incomplete universe"
                );
                sleep(Duration::from_millis(100)).await;
            }
        }
    }
    bail!("Gamma discovery exceeded 10000 pages; refusing incomplete universe")
}

fn assign(old: &Shards, universe: &BTreeSet<Asset>, capacity: usize) -> Shards {
    let mut next: BTreeMap<usize, BTreeSet<Asset>> = old
        .iter()
        .map(|(&id, assets)| (id, assets.intersection(universe).cloned().collect()))
        .collect();
    let retained: BTreeSet<_> = next
        .values()
        .flat_map(|assets| assets.iter().cloned())
        .collect();
    for symbol in universe.difference(&retained) {
        let id = next
            .iter()
            .find(|(_, assets)| assets.len() < capacity)
            .map(|(&id, _)| id)
            .unwrap_or_else(|| (0..).find(|id| !next.contains_key(id)).unwrap());
        next.entry(id).or_default().insert(symbol.clone());
    }
    next.into_iter()
        .filter(|(_, assets)| !assets.is_empty())
        .map(|(id, assets)| (id, Arc::new(assets)))
        .collect()
}

async fn emit(tx: &mpsc::Sender<Input>, input: Input, stop: &CancellationToken) -> bool {
    tokio::select! { biased; _ = stop.cancelled() => false, result = tx.send(input) => result.is_ok() }
}

/// One shared mutex is held through the actual dial start, preventing overdue
/// reservations from bunching after a scheduler stall.
fn backoff(failures: u32, source: usize) -> Duration {
    // Equal jitter, 0.5--1s initially, bounded at 30s. No RNG dependency or
    // private state; the per-attempt clock seed disperses replicas and sweeps.
    let cap = (1000u64 << failures.saturating_sub(1).min(5)).min(30_000);
    let clock = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos() as u64;
    let mut seed = clock ^ (source as u64).wrapping_mul(0x9e3779b97f4a7c15) ^ u64::from(failures);
    seed ^= seed >> 12;
    seed ^= seed << 25;
    seed ^= seed >> 27;
    Duration::from_millis(cap / 2 + seed.wrapping_mul(0x2545f4914f6cdd1d) % (cap / 2 + 1))
}
