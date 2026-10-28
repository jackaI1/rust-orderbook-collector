//! A shard owns all its physical sockets, including temporary warm replacements.
//! The supervisor serializes frames and disconnects; a socket never reuses an ID.
//! Rotation warms a replacement; bounded asset verification uses the same socket.
use crate::{
    adapter::Event,
    assets::{Asset, intern},
    live::{Config, Input},
    monitor::Monitor,
};
use anyhow::{Context, Result, ensure};
use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    net::TcpStream,
    sync::{Mutex, Notify, OwnedSemaphorePermit, Semaphore, mpsc, watch},
    task::JoinSet,
    time::{Instant, MissedTickBehavior, interval_at, sleep_until, timeout},
};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async_with_config,
    tungstenite::{Error as WsError, Message, protocol::WebSocketConfig},
};
use tokio_util::sync::CancellationToken;

const FRAME_LIMIT: usize = 8 * 1024 * 1024;
const DIAL_SPACING: Duration = Duration::from_millis(500);
const IO_TIMEOUT: Duration = Duration::from_secs(10);
const CLOSE_TIMEOUT: Duration = Duration::from_millis(250);
// Large initial dumps can lag the last paced subscription by over a minute.
// Bound that network/processing margin independently of the subscription ramp.
const WARM_TIMEOUT: Duration = Duration::from_secs(120);
const COOLDOWN: Duration = Duration::from_secs(30);
const RECONNECT_STABILITY: Duration = Duration::from_secs(300);
const BATCH_INTERVAL: Duration = Duration::from_secs(1);
const BATCH_SIZE: usize = 100;
const GLOBAL_BATCH_INTERVAL: Duration = Duration::from_millis(25);
const WARM_BACKOFF_CAP: Duration = Duration::from_secs(30 * 60);
type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Share one budget across every collector shard, base connection, and warm
/// replacement. There is no refill worker or saved credit: each actual request
/// starts a fresh 25ms interval. Each socket also enforces its own 1s cadence.
pub struct SubscriptionBudget {
    state: StdMutex<BudgetState>,
    changed: Notify,
}
struct BudgetState {
    next: Instant,
    queue: VecDeque<BudgetEntry>,
    reserved: Option<Arc<()>>,
}
struct BudgetEntry {
    ticket: Arc<()>,
    parked: bool,
}
impl Default for SubscriptionBudget {
    fn default() -> Self {
        Self {
            state: StdMutex::new(BudgetState {
                next: Instant::now(),
                queue: VecDeque::new(),
                reserved: None,
            }),
            changed: Notify::new(),
        }
    }
}
impl SubscriptionBudget {
    pub fn new() -> Self {
        Self::default()
    }

    fn enqueue(self: &Arc<Self>) -> SubscriptionWait {
        let ticket = Arc::new(());
        self.state.lock().unwrap().queue.push_back(BudgetEntry {
            ticket: ticket.clone(),
            parked: false,
        });
        SubscriptionWait {
            budget: self.clone(),
            ticket,
        }
    }
}
// A parked socket keeps its seniority but is skipped by eligible waiters.
// Readiness reserves the grant under the same mutex as parking/resuming: an
// older parked ticket cannot steal a grant after its successor becomes ready.
// There is one ticket per socket, no saved credit, worker, or lock across await.
struct SubscriptionWait {
    budget: Arc<SubscriptionBudget>,
    ticket: Arc<()>,
}
impl SubscriptionWait {
    fn park(&self) {
        self.set_parked(true);
    }
    fn resume(&self) {
        self.set_parked(false);
    }
    fn set_parked(&self, parked: bool) {
        let mut changed = false;
        {
            let mut state = self.budget.state.lock().unwrap();
            if let Some(entry) = state
                .queue
                .iter_mut()
                .find(|entry| Arc::ptr_eq(&entry.ticket, &self.ticket))
            {
                changed = entry.parked != parked;
                entry.parked = parked;
            }
            if parked
                && state
                    .reserved
                    .as_ref()
                    .is_some_and(|owner| Arc::ptr_eq(owner, &self.ticket))
            {
                state.reserved = None;
                changed = true;
            }
        }
        if changed {
            self.budget.changed.notify_waiters();
        }
    }

    async fn ready(&self) {
        loop {
            let changed = self.budget.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let next = {
                let mut state = self.budget.state.lock().unwrap();
                if state
                    .reserved
                    .as_ref()
                    .is_some_and(|owner| Arc::ptr_eq(owner, &self.ticket))
                {
                    return;
                }
                if state.reserved.is_none()
                    && state
                        .queue
                        .iter()
                        .find(|entry| !entry.parked)
                        .is_some_and(|entry| Arc::ptr_eq(&entry.ticket, &self.ticket))
                {
                    if Instant::now() >= state.next {
                        state.reserved = Some(self.ticket.clone());
                        return;
                    }
                    Some(state.next)
                } else {
                    None
                }
            };
            if let Some(next) = next {
                tokio::select! { _ = sleep_until(next) => {}, _ = &mut changed => {} }
            } else {
                changed.await;
            }
        }
    }

    fn spend(self) {
        {
            let mut state = self.budget.state.lock().unwrap();
            assert!(
                state
                    .reserved
                    .as_ref()
                    .is_some_and(|owner| Arc::ptr_eq(owner, &self.ticket))
            );
            state.reserved = None;
            state
                .queue
                .retain(|entry| !Arc::ptr_eq(&entry.ticket, &self.ticket));
            state.next = Instant::now() + GLOBAL_BATCH_INTERVAL;
        }
        self.budget.changed.notify_waiters();
    }
}
impl Drop for SubscriptionWait {
    fn drop(&mut self) {
        {
            let mut state = self.budget.state.lock().unwrap();
            state
                .queue
                .retain(|entry| !Arc::ptr_eq(&entry.ticket, &self.ticket));
            if state
                .reserved
                .as_ref()
                .is_some_and(|owner| Arc::ptr_eq(owner, &self.ticket))
            {
                state.reserved = None;
            }
        }
        self.budget.changed.notify_waiters();
    }
}
fn park_turn(turn: &Option<SubscriptionWait>) {
    if let Some(turn) = turn {
        turn.park();
    }
}

#[derive(Clone)]
enum SubscriptionSource {
    Legacy {
        receiver: watch::Receiver<BTreeSet<String>>,
        current: Arc<BTreeSet<Asset>>,
        dirty: bool,
    },
    Shared(watch::Receiver<Arc<BTreeSet<Asset>>>),
}
impl SubscriptionSource {
    fn legacy(mut receiver: watch::Receiver<BTreeSet<String>>) -> Self {
        let current = Arc::new(
            receiver
                .borrow_and_update()
                .iter()
                .map(|s| intern(s))
                .collect(),
        );
        Self::Legacy {
            receiver,
            current,
            dirty: false,
        }
    }

    fn latest(&mut self) -> Arc<BTreeSet<Asset>> {
        match self {
            Self::Legacy {
                receiver,
                current,
                dirty,
            } => {
                // Ref::has_changed also sees a final unseen generation after
                // sender closure; Receiver::has_changed would return Err then.
                let generation = receiver.borrow_and_update();
                if *dirty || generation.has_changed() {
                    *current = Arc::new(generation.iter().map(|s| intern(s)).collect());
                    *dirty = false;
                }
                current.clone()
            }
            Self::Shared(receiver) => receiver.borrow_and_update().clone(),
        }
    }

    async fn changed(&mut self) -> Result<(), watch::error::RecvError> {
        match self {
            Self::Legacy {
                receiver, dirty, ..
            } => {
                receiver.changed().await?;
                *dirty = true;
                Ok(())
            }
            Self::Shared(receiver) => receiver.changed().await,
        }
    }
}

struct Progress {
    symbol: Asset,
    timestamp: i64,
    snapshot: bool,
}
enum Notice {
    Connected {
        source: usize,
    },
    Reset {
        source: usize,
        symbols: BTreeSet<Asset>,
    },
    Subscribed {
        source: usize,
        symbols: Vec<Asset>,
        added: bool,
    },
    Frame {
        source: usize,
        events: Vec<Event>,
        received_ms: i64,
        progress: Vec<Progress>,
    },
    End {
        source: usize,
        reason: &'static str,
    },
}
struct Feed {
    slot: usize,
    cancel: CancellationToken,
    started: Instant,
    retiring: bool,
    initial_ramp_complete: bool,
    subscribed: BTreeSet<Asset>,
    subscriptions_pending: usize,
    initialized: BTreeSet<Asset>,
    waters: BTreeMap<Asset, i64>,
}
struct Slot {
    active: Option<usize>,
    failures: u32,
    retry_at: Instant,
    rotate_at: Instant,
    warm_failures: u32,
}
struct Warm {
    source: usize,
    slot: usize,
    deadline: Option<Instant>,
    ramp_deadline: Option<Instant>,
    target: Option<BTreeMap<Asset, i64>>,
    _permit: OwnedSemaphorePermit,
}
struct Shard {
    config: Config,
    assets: SubscriptionSource,
    current: Arc<BTreeSet<Asset>>,
    tx: mpsc::Sender<Input>,
    stop: CancellationToken,
    dial: Arc<Mutex<Instant>>,
    source_ids: Arc<AtomicUsize>,
    notices: mpsc::Sender<Notice>,
    tasks: JoinSet<()>,
    feeds: BTreeMap<usize, Feed>,
    slots: Vec<Slot>,
    warm: Option<Warm>,
    warm_slots: Arc<Semaphore>,
    subscription_budget: Arc<SubscriptionBudget>,
    fatal: watch::Sender<Option<String>>,
}

/// Run one stable shard until cancellation/consumer closure or fatal denial.
/// Cancel and await this future: it closes and joins every owned socket. Warm
/// replacements add at most one physical connection to `config.replicas`.
pub async fn run_shard(
    config: Config,
    assets: watch::Receiver<BTreeSet<String>>,
    tx: mpsc::Sender<Input>,
    cancel: CancellationToken,
    dial: Arc<Mutex<Instant>>,
    source_ids: Arc<AtomicUsize>,
) -> Result<()> {
    run_shard_source(
        config,
        SubscriptionSource::legacy(assets),
        tx,
        cancel,
        dial,
        source_ids,
        Arc::new(Semaphore::new(1)),
        Arc::new(SubscriptionBudget::new()),
    )
    .await
}

/// Production subscription path: generations share their set and asset handles
/// with every physical socket, without owned-string conversion or reinterning.
pub async fn run_shard_shared(
    config: Config,
    assets: watch::Receiver<Arc<BTreeSet<Asset>>>,
    tx: mpsc::Sender<Input>,
    cancel: CancellationToken,
    dial: Arc<Mutex<Instant>>,
    source_ids: Arc<AtomicUsize>,
) -> Result<()> {
    run_shard_source(
        config,
        SubscriptionSource::Shared(assets),
        tx,
        cancel,
        dial,
        source_ids,
        Arc::new(Semaphore::new(1)),
        Arc::new(SubscriptionBudget::new()),
    )
    .await
}

/// Production-wide warm cap and shared rate budget. The caller must pass the
/// same subscription budget to every shard, including later topology additions.
#[allow(clippy::too_many_arguments)]
pub async fn run_shard_shared_limited(
    config: Config,
    assets: watch::Receiver<Arc<BTreeSet<Asset>>>,
    tx: mpsc::Sender<Input>,
    cancel: CancellationToken,
    dial: Arc<Mutex<Instant>>,
    source_ids: Arc<AtomicUsize>,
    warm_slots: Arc<Semaphore>,
    subscription_budget: Arc<SubscriptionBudget>,
) -> Result<()> {
    run_shard_source(
        config,
        SubscriptionSource::Shared(assets),
        tx,
        cancel,
        dial,
        source_ids,
        warm_slots,
        subscription_budget,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn run_shard_source(
    config: Config,
    mut assets: SubscriptionSource,
    tx: mpsc::Sender<Input>,
    cancel: CancellationToken,
    dial: Arc<Mutex<Instant>>,
    source_ids: Arc<AtomicUsize>,
    warm_slots: Arc<Semaphore>,
    subscription_budget: Arc<SubscriptionBudget>,
) -> Result<()> {
    ensure!(
        (1..=128).contains(&config.replicas),
        "replicas must be between 1 and 128"
    );
    ensure!(
        (1..=31_536_000).contains(&config.stale_secs)
            && (1..=31_536_000).contains(&config.rotate_secs),
        "connection intervals must be between 1 second and 365 days"
    );
    let stop = cancel.child_token();
    // At most eight full frames wait in this mailbox; producer backpressure is
    // cancellation-aware. No detached reader, timer, writer, or reconnect task.
    let (notices, mut incoming) = mpsc::channel(8);
    let (fatal, _) = watch::channel(None);
    let now = Instant::now();
    let rotation = Duration::from_secs(config.rotate_secs.max(1));
    let slots = (0..config.replicas)
        .map(|index| Slot {
            active: None,
            failures: 0,
            retry_at: now,
            rotate_at: now + rotation + rotation.mul_f64(index as f64 / config.replicas as f64),
            warm_failures: 0,
        })
        .collect();
    let current = assets.latest();
    let mut shard = Shard {
        config,
        assets,
        current,
        tx,
        stop,
        dial,
        source_ids,
        notices,
        tasks: JoinSet::new(),
        feeds: BTreeMap::new(),
        slots,
        warm: None,
        warm_slots,
        subscription_budget,
        fatal,
    };
    let result = shard.drive(&mut incoming).await;
    shard.stop.cancel();
    let mut result = result;
    while let Some(joined) = shard.tasks.join_next().await {
        if let Err(error) = joined
            && result.is_ok()
        {
            result = Err(error.into());
        }
    }
    // A fatal signal cancels sockets immediately, even when output is full. It
    // survives the cancellation path and must still cause the root to stop.
    let denial = shard.fatal.borrow().clone();
    if let Some(reason) = denial {
        result = Err(anyhow::anyhow!(reason));
    }
    if let Err(error) = &result {
        let message = format!("{error:#}").chars().take(512).collect();
        // Bounded best effort if the consumer is wedged; returning Err is the
        // authoritative fatal signal. External cancellation never blocks here.
        tokio::select! { biased;
            _ = cancel.cancelled() => {},
            _ = timeout(CLOSE_TIMEOUT, shard.tx.send(Input::Fatal(message))) => {},
        }
    }
    result
}

impl Shard {
    async fn drive(&mut self, incoming: &mut mpsc::Receiver<Notice>) -> Result<()> {
        let mut tick = interval_at(Instant::now(), BATCH_INTERVAL);
        tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut watching = true;
        loop {
            tokio::select! { biased;
                _ = self.stop.cancelled() => return Ok(()),
                _ = self.tx.closed() => return Ok(()),
                joined = self.tasks.join_next(), if !self.tasks.is_empty() => {
                    joined.context("missing socket task")?.context("socket task panicked")?;
                },
                changed = self.assets.changed(), if watching => {
                    if changed.is_err() { watching = false; }
                    else {
                        self.current = self.assets.latest();
                        for feed in self.feeds.values_mut() {
                            feed.subscriptions_pending = feed.subscribed.symmetric_difference(&self.current).count();
                        }
                        if let Some(warm) = &mut self.warm
                            && self.feeds[&warm.source].subscriptions_pending > 0
                        { warm.deadline = None; warm.target = None; }
                    }
                },
                _ = tick.tick() => self.maintain()?,
                notice = incoming.recv() => {
                    let notice = notice.context("socket notice channel closed")?;
                    if !self.notice(notice).await? { return Ok(()); }
                },
            }
        }
    }

    fn launch(&mut self, slot: usize, replacement: bool) -> Result<usize> {
        #[allow(deprecated)] // checked fetch_update is also available on stable Rust.
        let source = self
            .source_ids
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                value.checked_add(1)
            })
            .map_err(|_| anyhow::anyhow!("physical source ID exhausted"))?;
        let local = self.stop.child_token();
        self.feeds.insert(
            source,
            Feed {
                slot,
                cancel: local.clone(),
                started: Instant::now(),
                retiring: false,
                initial_ramp_complete: false,
                subscribed: BTreeSet::new(),
                subscriptions_pending: self.current.len(),
                initialized: BTreeSet::new(),
                waters: BTreeMap::new(),
            },
        );
        let config = self.config.clone();
        let assets = self.assets.clone();
        let notices = self.notices.clone();
        let stop = self.stop.clone();
        let dial = self.dial.clone();
        let fatal = self.fatal.clone();
        let subscription_budget = self.subscription_budget.clone();
        self.tasks.spawn(async move {
            physical(
                config,
                assets,
                source,
                slot,
                notices,
                local,
                stop,
                dial,
                fatal,
                subscription_budget,
            )
            .await;
        });
        metric(
            "connection_start",
            source,
            slot,
            if replacement { "warm" } else { "reconnect" },
        );
        Ok(source)
    }

    fn capture_warm_target(&mut self) {
        let Some(warm) = &mut self.warm else {
            return;
        };
        if warm.target.is_none()
            && warm.deadline.is_some()
            && self.feeds[&warm.source].subscriptions_pending == 0
        {
            warm.target = Some(
                self.slots[warm.slot]
                    .active
                    .and_then(|id| self.feeds.get(&id))
                    .into_iter()
                    .flat_map(|feed| {
                        feed.initialized
                            .iter()
                            .filter(|s| self.current.contains(*s))
                            .filter_map(|s| feed.waters.get(s).map(|t| (s.clone(), *t)))
                    })
                    .collect(),
            );
        }
    }

    fn maintain(&mut self) -> Result<()> {
        self.capture_warm_target();
        let now = Instant::now();
        // Retiring sockets are joined before allowing another maintenance dial.
        // This keeps the physical count <= replicas + 1 even with slow closes.
        let retiring = self.feeds.values().any(|feed| feed.retiring);
        for index in 0..self.slots.len() {
            if self.slots[index].active.is_none()
                && !self.warm.as_ref().is_some_and(|w| w.slot == index)
                && now >= self.slots[index].retry_at
            {
                let source = self.launch(index, false)?;
                self.slots[index].active = Some(source);
            }
        }
        if let Some(warm) = &self.warm {
            let old = self.slots[warm.slot]
                .active
                .and_then(|s| self.feeds.get(&s));
            let new = self.feeds.get(&warm.source).context("missing warm feed")?;
            // Gate only the outgoing feed's currently usable books. A reset,
            // quarantine, removal, or never-book asset cannot wedge replacement.
            let ready = !new.retiring
                && new.subscriptions_pending == 0
                && warm.target.as_ref().is_some_and(|target| {
                    target
                        .iter()
                        .filter(|(symbol, _)| {
                            self.current.contains(*symbol)
                                && old.is_some_and(|feed| feed.initialized.contains(*symbol))
                        })
                        .all(|(symbol, prior)| {
                            new.initialized.contains(symbol)
                                && new.waters.get(symbol).is_some_and(|water| water >= prior)
                        })
                });

            if ready {
                let warm = self.warm.take().unwrap();
                let slot = &mut self.slots[warm.slot];
                if let Some(old) = slot.active.replace(warm.source) {
                    let feed = self.feeds.get_mut(&old).context("missing predecessor")?;
                    feed.retiring = true;
                    feed.cancel.cancel();
                }
                slot.rotate_at = now + Duration::from_secs(self.config.rotate_secs);
                slot.warm_failures = 0;
                slot.failures = 0;
                metric(
                    "connection_promoted",
                    warm.source,
                    warm.slot,
                    "fresh_snapshots_ready",
                );
            } else if warm.deadline.is_some_and(|deadline| now >= deadline)
                || warm
                    .ramp_deadline
                    .is_some_and(|deadline| now >= deadline && new.subscriptions_pending > 0)
            {
                let warm = self.warm.take().unwrap();
                let feed = self
                    .feeds
                    .get_mut(&warm.source)
                    .context("missing warm feed")?;
                feed.retiring = true;
                feed.cancel.cancel();
                let slot = &mut self.slots[warm.slot];
                slot.warm_failures = slot.warm_failures.saturating_add(1);
                slot.rotate_at = now + warm_backoff(slot.warm_failures);
                metric(
                    "connection_warm_failed",
                    warm.source,
                    warm.slot,
                    "snapshot_timeout_old_retained",
                );
            }
            return Ok(());
        }
        if retiring {
            return Ok(());
        }
        // One shard-wide permit represented by `warm`; initial/unexpected
        // reconnects do not break any healthy source and need no such permit.
        let next = self
            .slots
            .iter()
            .enumerate()
            .filter_map(|(index, slot)| {
                let active = slot.active.and_then(|s| self.feeds.get(&s))?;
                ((now >= slot.rotate_at) && active.subscriptions_pending == 0).then_some(index)
            })
            .min_by_key(|&index| self.slots[index].rotate_at);
        if let Some(index) = next {
            let Ok(permit) = self.warm_slots.clone().try_acquire_owned() else {
                return Ok(());
            };
            let source = self.launch(index, true)?;
            self.warm = Some(Warm {
                source,
                slot: index,
                // Arm only after connection establishment and subscription completion.
                deadline: None,
                ramp_deadline: None,
                target: None,
                _permit: permit,
            });
        }
        Ok(())
    }

    async fn notice(&mut self, notice: Notice) -> Result<bool> {
        match notice {
            Notice::Connected { source } => {
                if let Some(feed) = self.feeds.get(&source)
                    && self.slots[feed.slot].active == Some(source)
                {
                    self.slots[feed.slot].rotate_at =
                        Instant::now() + Duration::from_secs(self.config.rotate_secs);
                }
                if let Some(warm) = &mut self.warm
                    && warm.source == source
                {
                    warm.ramp_deadline = Some(Instant::now() + warm_ramp_limit(self.current.len()));
                    warm.deadline = self
                        .feeds
                        .get(&source)
                        .filter(|feed| feed.subscriptions_pending == 0)
                        .map(|_| Instant::now() + WARM_TIMEOUT);
                }
            }
            Notice::Reset { source, symbols } => {
                if let Some(warm) = &mut self.warm
                    && self.slots[warm.slot].active == Some(source)
                    && let Some(target) = &mut warm.target
                {
                    target.retain(|symbol, _| !symbols.contains(symbol));
                }
                if let Some(feed) = self.feeds.get_mut(&source) {
                    for symbol in &symbols {
                        feed.initialized.remove(symbol);
                        feed.waters.remove(symbol);
                    }
                    if !emit(
                        &self.tx,
                        Input::Reset {
                            source,
                            symbols: symbols.iter().map(ToString::to_string).collect(),
                        },
                        &self.stop,
                    )
                    .await
                    {
                        return Ok(false);
                    }
                }
            }
            Notice::Subscribed {
                source,
                symbols,
                added,
            } => {
                if let Some(feed) = self.feeds.get_mut(&source) {
                    let was_pending = feed.subscriptions_pending;
                    for symbol in symbols {
                        if added {
                            if feed.subscribed.insert(symbol.clone()) {
                                if self.current.contains(&symbol) {
                                    feed.subscriptions_pending -= 1;
                                } else {
                                    feed.subscriptions_pending += 1;
                                }
                            }
                        } else {
                            if feed.subscribed.remove(&symbol) {
                                if self.current.contains(&symbol) {
                                    feed.subscriptions_pending += 1;
                                } else {
                                    feed.subscriptions_pending -= 1;
                                }
                            }
                            feed.initialized.remove(&symbol);
                            feed.waters.remove(&symbol);
                        }
                    }
                    if feed.subscriptions_pending == 0 {
                        // Catalog churn must not postpone rotation indefinitely.
                        // Only this physical source's first completed ramp arms
                        // its interval; later incremental updates retain it.
                        if was_pending > 0 && !feed.initial_ramp_complete {
                            feed.initial_ramp_complete = true;
                            if self.slots[feed.slot].active == Some(source) {
                                self.slots[feed.slot].rotate_at =
                                    Instant::now() + Duration::from_secs(self.config.rotate_secs);
                            }
                        }
                        if let Some(warm) = &mut self.warm
                            && warm.source == source
                            && warm.deadline.is_none()
                        {
                            warm.deadline = Some(Instant::now() + WARM_TIMEOUT);
                        }
                    } else if let Some(warm) = &mut self.warm
                        && warm.source == source
                    {
                        warm.deadline = None;
                    }
                }
            }
            Notice::Frame {
                source,
                events,
                received_ms,
                progress,
            } => {
                let Some(feed) = self.feeds.get_mut(&source) else {
                    return Ok(true);
                };
                // Keep the supervisor's readiness state tied to frames actually
                // enqueued to the engine, rather than merely read from the wire.
                if !emit(
                    &self.tx,
                    Input::Events {
                        source,
                        events,
                        received_ms,
                    },
                    &self.stop,
                )
                .await
                {
                    return Ok(false);
                }
                for event in progress {
                    if self.current.contains(&event.symbol)
                        && feed.subscribed.contains(&event.symbol)
                    {
                        if event.snapshot {
                            feed.initialized.insert(event.symbol.clone());
                        }
                        feed.waters.insert(event.symbol, event.timestamp);
                    }
                }
            }
            Notice::End { source, reason } => {
                let Some(feed) = self.feeds.remove(&source) else {
                    return Ok(true);
                };
                // End shares the source's FIFO mailbox with all its frames.
                // Reconnect can only be launched after this disconnect is sent.
                if !emit(&self.tx, Input::Disconnected { source }, &self.stop).await {
                    return Ok(false);
                }
                let slot = &mut self.slots[feed.slot];
                if slot.active == Some(source) {
                    slot.active = None;
                    if feed.started.elapsed() >= RECONNECT_STABILITY {
                        slot.failures = 0;
                    }
                    slot.failures = slot.failures.saturating_add(1);
                    slot.retry_at = Instant::now() + backoff(slot.failures, source);
                }
                if self.warm.as_ref().is_some_and(|warm| warm.source == source) {
                    self.warm = None;
                    slot.warm_failures = slot.warm_failures.saturating_add(1);
                    slot.rotate_at = Instant::now() + warm_backoff(slot.warm_failures);
                }
                metric("connection_end", source, feed.slot, reason);
            }
        }
        self.capture_warm_target();
        Ok(true)
    }
}

fn metric(event: &str, source: usize, replica: usize, reason: &str) {
    // Fixed reason codes only: errors/frames/asset identifiers are not logged.
    eprintln!(
        "{}",
        json!({"event":event,"source":source,"replica":replica,"reason":reason,"received_ms":chrono::Utc::now().timestamp_millis()})
    );
}
fn warm_backoff(failures: u32) -> Duration {
    COOLDOWN
        .saturating_mul(1u32 << failures.saturating_sub(1).min(6))
        .min(WARM_BACKOFF_CAP)
}
// A silent/missing PONG must not retain one of the global warm slots forever.
// Allow all 128 base + four warm sockets a fair turn per batch, plus grace;
// the 15-minute floor tolerates short bursts and long initialization pauses.
fn warm_ramp_limit(assets: usize) -> Duration {
    let batches = u32::try_from(assets.div_ceil(BATCH_SIZE)).unwrap_or(u32::MAX);
    GLOBAL_BATCH_INTERVAL
        .saturating_mul(batches)
        .saturating_mul(132)
        .saturating_add(WARM_TIMEOUT)
        .max(Duration::from_secs(15 * 60))
}
fn backoff(failures: u32, source: usize) -> Duration {
    let cap = (1000u64 << failures.saturating_sub(1).min(5)).min(30_000);
    let clock = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos() as u64;
    let seed = clock ^ (source as u64).wrapping_mul(0x9e3779b97f4a7c15) ^ u64::from(failures);
    Duration::from_millis(cap / 2 + seed.wrapping_mul(0x2545f4914f6cdd1d) % (cap / 2 + 1))
}
async fn emit(tx: &mpsc::Sender<Input>, value: Input, stop: &CancellationToken) -> bool {
    tokio::select! { biased; _ = stop.cancelled() => false, result = tx.send(value) => result.is_ok() }
}
async fn report(tx: &mpsc::Sender<Notice>, value: Notice, stop: &CancellationToken) -> bool {
    tokio::select! { biased; _ = stop.cancelled() => false, result = tx.send(value) => result.is_ok() }
}

// Retain fairness across fast reads, but never leave an unused FIFO head behind
// while downstream backpressure suspends this socket. Other shards can proceed.
async fn report_read(
    tx: &mpsc::Sender<Notice>,
    value: Notice,
    stop: &CancellationToken,
    turn: &mut Option<SubscriptionWait>,
) -> bool {
    if stop.is_cancelled() {
        return false;
    }
    match tx.try_send(value) {
        Ok(()) => true,
        Err(mpsc::error::TrySendError::Full(value)) => {
            park_turn(turn);
            report(tx, value, stop).await
        }
        Err(mpsc::error::TrySendError::Closed(_)) => false,
    }
}

/// Keep the pacing lock through establishment. The handshake timeout starts
/// after the (possibly long) wait for the global pacer; cancellation wraps both.
async fn paced_dial(url: &str, dial: &Mutex<Instant>) -> Result<Socket, DialError> {
    let mut next = dial.lock().await;
    sleep_until(*next).await;
    let settings = WebSocketConfig::default()
        .read_buffer_size(16 * 1024)
        .write_buffer_size(16 * 1024)
        .max_write_buffer_size(2 * FRAME_LIMIT)
        .max_frame_size(Some(FRAME_LIMIT))
        .max_message_size(Some(FRAME_LIMIT));
    *next = Instant::now() + DIAL_SPACING;
    let result = timeout(
        IO_TIMEOUT,
        connect_async_with_config(url, Some(settings), false),
    )
    .await;
    *next = Instant::now() + DIAL_SPACING;
    match result {
        Ok(Ok((socket, _))) => Ok(socket),
        Ok(Err(WsError::Http(response))) if matches!(response.status().as_u16(), 401 | 403) => {
            Err(DialError::Denied(response.status().as_u16()))
        }
        Ok(Err(_)) => Err(DialError::Failed("handshake_failed")),
        Err(_) => Err(DialError::Failed("handshake_timeout")),
    }
}
enum DialError {
    Denied(u16),
    Failed(&'static str),
}

#[allow(clippy::too_many_arguments)]
async fn physical(
    config: Config,
    assets: SubscriptionSource,
    source: usize,
    slot: usize,
    notices: mpsc::Sender<Notice>,
    local: CancellationToken,
    stop: CancellationToken,
    dial: Arc<Mutex<Instant>>,
    fatal: watch::Sender<Option<String>>,
    subscription_budget: Arc<SubscriptionBudget>,
) {
    let connected = tokio::select! { biased;
        _ = local.cancelled() => return,
        result = paced_dial(&config.websocket_url, &dial) => result,
    };
    let reason = match connected {
        Ok(mut socket) => {
            if !report(&notices, Notice::Connected { source }, &local).await {
                let _ = timeout(CLOSE_TIMEOUT, socket.close(None)).await;
                return;
            }
            let result = tokio::select! { biased;
                _ = local.cancelled() => Ok("cancelled"),
                result = session(&config, assets, source, slot, &mut socket, &notices, &local, subscription_budget) => result,
            };
            let _ = timeout(CLOSE_TIMEOUT, socket.close(None)).await;
            match result {
                Ok(reason) | Err(reason) => reason,
            }
        }
        Err(DialError::Denied(status)) => {
            fatal.send_if_modified(|reason| {
                if reason.is_some() {
                    false
                } else {
                    *reason = Some(format!(
                        "WebSocket access denied (HTTP {status}); stopped without retry"
                    ));
                    true
                }
            });
            stop.cancel();
            return;
        }
        Err(DialError::Failed(reason)) => reason,
    };
    if reason == "peer_policy_denied" {
        fatal.send_if_modified(|message| {
            if message.is_some() {
                false
            } else {
                *message = Some("WebSocket policy denial; stopped without retry".into());
                true
            }
        });
        stop.cancel();
        return;
    }
    // Cancellation of an individual old/warm socket still delivers End to the
    // shard. Only shard-wide shutdown may discard the remaining mailbox.
    report(&notices, Notice::End { source, reason }, &stop).await;
}

async fn write(socket: &mut Socket, message: Message) -> Result<(), &'static str> {
    match timeout(IO_TIMEOUT, socket.send(message)).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(_)) => Err("write_failed"),
        Err(_) => Err("write_timeout"),
    }
}
// Source-local watermarks never borrow decoded event strings. Only touched
// assets are staged for a frame, avoiding a full shard clone on every update.
#[derive(Clone, Default)]
struct Watermark {
    latest: Option<i64>,
    snapshot: Option<i64>,
    quarantined: bool,
}

// Fixed safety bounds, not tuning knobs. A source that cannot retain every
// pre-book change fails closed; it must never advertise partial continuity.
const PREBOOK_PER_ASSET: usize = 256;
const PREBOOK_BYTES: usize = 4 * 1024 * 1024;
#[derive(Default)]
struct PrebookDeltas {
    assets: BTreeMap<Asset, Vec<Event>>,
    bytes: usize,
}
impl PrebookDeltas {
    fn cost(event: &Event) -> usize {
        let Event::Delta { symbol, .. } = event else {
            unreachable!()
        };
        // Include spare Vec capacity and map/key overhead conservatively.
        2 * std::mem::size_of::<Event>() + symbol.capacity() + 128
    }
    fn push(&mut self, asset: &Asset, event: Event) -> Result<()> {
        let cost = Self::cost(&event);
        ensure!(
            self.bytes + cost <= PREBOOK_BYTES,
            "global pre-book buffer overflow"
        );
        let pending = self.assets.entry(asset.clone()).or_default();
        ensure!(
            pending.len() < PREBOOK_PER_ASSET,
            "asset pre-book buffer overflow"
        );
        self.bytes += cost;
        pending.push(event);
        Ok(())
    }
    fn take(&mut self, asset: &Asset) -> Vec<Event> {
        let events = self.assets.remove(asset).unwrap_or_default();
        self.bytes -= events.iter().map(Self::cost).sum::<usize>();
        events
    }
}

struct Validated {
    events: Vec<Event>,
    progress: Vec<Progress>,
    resets: BTreeSet<Asset>,
}

fn diagnostic(
    source: usize,
    count: &mut u64,
    symbol: &str,
    prior: i64,
    incoming: i64,
    kind: &str,
    cause: &str,
) {
    *count = count.saturating_add(1);
    // Eight samples then sparse aggregate samples; never include wire payloads.
    if *count <= 8 || count.is_power_of_two() {
        eprintln!(
            "{}",
            json!({"event":"asset_stream_diagnostic", "source":source,
            "count":count, "asset":symbol.chars().take(128).collect::<String>(),
            "previous_ms":prior, "incoming_ms":incoming, "event_type":kind, "cause":cause})
        );
    }
}

#[allow(clippy::too_many_arguments)]
fn validate_frame(
    text: &str,
    requested: &BTreeSet<Asset>,
    suspended: &BTreeSet<Asset>,
    timestamps: &mut BTreeMap<Asset, Watermark>,
    prebook: &mut PrebookDeltas,
    monitor: &mut Monitor,
    elapsed: u64,
    source: usize,
    diagnostics: &mut u64,
) -> Result<Validated> {
    // Validate the entire frame before applying events, resets, watermark
    // changes, or monitoring effects; a later member may be malformed.
    let decoded = crate::adapter::decode(text)?;
    let mut events = Vec::with_capacity(decoded.len());
    let mut ready = BTreeSet::new();
    for event in decoded {
        let (symbol, snapshot) = match &event {
            Event::Snapshot { symbol, .. } => (symbol, true),
            Event::Delta { symbol, .. } => (symbol, false),
            _ => {
                events.push(event);
                continue;
            }
        };
        if let Some(asset) = requested.get(symbol.as_str())
            && !suspended.contains(asset)
            && timestamps
                .get(asset)
                .is_none_or(|state| state.snapshot.is_none() && !state.quarantined)
        {
            if snapshot {
                ready.insert(asset.clone());
            } else {
                prebook.push(asset, event)?;
                continue;
            }
        }
        events.push(event);
    }
    // Include every delta for a newly initialized asset in this frame, even
    // those following its book. Otherwise a buffered future delta could make
    // an earlier in-frame delta appear to regress. Stable sort preserves the
    // arrival order of equal timestamps and absolute-size replacement writes.
    for asset in ready {
        let mut pending = prebook.take(&asset);
        pending.sort_by_key(|event| match event {
            Event::Delta { timestamp_ms, .. } => *timestamp_ms,
            _ => unreachable!(),
        });
        events.extend(pending);
    }
    let mut staged = BTreeMap::<Asset, Watermark>::new();
    let mut resets = BTreeSet::new();
    let mut keep = vec![true; events.len()];
    for (index, event) in events.iter().enumerate() {
        let (symbol, timestamp, snapshot) = match event {
            Event::Snapshot {
                symbol,
                timestamp_ms,
                ..
            } => (symbol, *timestamp_ms, true),
            Event::Delta {
                symbol,
                timestamp_ms,
                ..
            } => (symbol, *timestamp_ms, false),
            _ => continue,
        };
        let Some(asset) = requested.get(symbol.as_str()) else {
            keep[index] = false;
            continue;
        };
        if suspended.contains(asset) {
            keep[index] = false;
            continue;
        }
        let state = staged
            .entry(asset.clone())
            .or_insert_with(|| timestamps.get(asset).cloned().unwrap_or_default());
        let kind = if snapshot { "book" } else { "price_change" };
        if !snapshot && state.snapshot.is_none() {
            // Buffered changes cannot reach the engine without a usable book
            // or advance its exchange timestamp watermark.
            keep[index] = false;
        } else if resets.contains(asset) || (state.quarantined && !snapshot) {
            keep[index] = false;
        } else if !snapshot && state.snapshot.is_some_and(|full| timestamp < full) {
            // A complete snapshot already contains these older changes. Equal
            // timestamps stay legal because feed order within a millisecond matters.
            keep[index] = false;
            diagnostic(
                source,
                diagnostics,
                symbol,
                state.snapshot.unwrap(),
                timestamp,
                kind,
                "delta_incorporated_by_snapshot",
            );
        } else if snapshot
            && state.snapshot.is_some()
            && !state.quarantined
            && state.latest.is_some_and(|previous| timestamp < previous)
        {
            // Initial/refresh dumps can race a newer price change. Retain the
            // newer maintained book instead of invalidating a healthy replica.
            keep[index] = false;
            diagnostic(
                source,
                diagnostics,
                symbol,
                state.latest.unwrap(),
                timestamp,
                kind,
                "older_snapshot_ignored",
            );
        } else if state.latest.is_some_and(|previous| timestamp < previous) {
            keep[index] = false;
            diagnostic(
                source,
                diagnostics,
                symbol,
                state.latest.unwrap(),
                timestamp,
                kind,
                "asset_timestamp_regression",
            );
            if !state.quarantined {
                resets.insert(asset.clone());
            }
            state.quarantined = true;
        } else {
            state.latest = Some(timestamp);
            if snapshot {
                state.snapshot = Some(timestamp);
                state.quarantined = false;
            }
        }
    }
    // A regression invalidates every update for that asset in this frame,
    // including updates preceding the offending member. Other assets survive.
    for (index, event) in events.iter().enumerate() {
        let symbol = match event {
            Event::Snapshot { symbol, .. } | Event::Delta { symbol, .. } => symbol,
            _ => continue,
        };
        if resets.contains(symbol.as_str()) {
            keep[index] = false;
        }
    }
    let mut accepted = Vec::with_capacity(events.len());
    let mut progress = BTreeMap::<Asset, Progress>::new();
    for (event, keep) in events.into_iter().zip(keep) {
        if !keep {
            continue;
        }
        let (symbol, timestamp, snapshot) = match &event {
            Event::Snapshot {
                symbol,
                timestamp_ms,
                ..
            } => (symbol, *timestamp_ms, true),
            Event::Delta {
                symbol,
                timestamp_ms,
                ..
            } => (symbol, *timestamp_ms, false),
            _ => {
                accepted.push(event);
                continue;
            }
        };
        let Some(asset) = requested.get(symbol.as_str()) else {
            continue;
        };
        monitor.observe(symbol, snapshot, elapsed);
        let prior_snapshot = progress.get(asset).is_some_and(|p| p.snapshot);
        progress.insert(
            asset.clone(),
            Progress {
                symbol: asset.clone(),
                timestamp,
                snapshot: snapshot || prior_snapshot,
            },
        );
        accepted.push(event);
    }
    for symbol in &resets {
        monitor.reset(symbol, elapsed);
    }
    timestamps.extend(staged);
    Ok(Validated {
        events: accepted,
        progress: progress.into_values().collect(),
        resets,
    })
}

#[allow(clippy::too_many_arguments)]
async fn session(
    config: &Config,
    mut assets: SubscriptionSource,
    source: usize,
    slot: usize,
    socket: &mut Socket,
    notices: &mpsc::Sender<Notice>,
    local: &CancellationToken,
    subscription_budget: Arc<SubscriptionBudget>,
) -> Result<&'static str, &'static str> {
    let start = Instant::now();
    let mut desired = assets.latest();
    let mut watching = true;
    let mut requested = BTreeSet::<Asset>::new();
    let mut pending_added: VecDeque<Asset> = desired.iter().cloned().collect();
    let mut pending_removed = VecDeque::<Asset>::new();
    // Set equality is linear in shard size: never evaluate it on every frame.
    let mut subscriptions_dirty = !desired.is_empty();
    let mut initial = true;
    let mut timestamps = BTreeMap::<Asset, Watermark>::new();
    let mut prebook = PrebookDeltas::default();
    let stale_ms = config.stale_secs.saturating_mul(1000);
    let mut monitor = Monitor::new([], stale_ms, 10_000);
    let mut batch_at = start;
    let mut subscription_wait: Option<SubscriptionWait> = None;
    let mut heartbeat = interval_at(start + IO_TIMEOUT, IO_TIMEOUT);
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut check = interval_at(start + Duration::from_secs(1), Duration::from_secs(1));
    check.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut pong_deadline: Option<Instant> = None;
    // Plain PONG carries no request ID. It is a progress signal, not a
    // one-for-one acknowledgement sequence. Do not retain a permanent debt
    // when replies are omitted/coalesced. Control PONGs do not release it.
    let mut pending_pings = 0u64;
    let mut pending_since = (!desired.is_empty()).then_some(start);
    let mut last_stall_report = start;
    // Integration contract: Reset precedes refresh unsubscribe. A single copy
    // intentionally has missing coverage until its fresh snapshot; replicas may
    // promote through the engine's existing emitted-watermark guard.
    let mut refreshing = Vec::<Asset>::new();
    let mut resubscribe = false;
    // Staggered replica refreshes leave another socket reading while one
    // briefly unsubscribes. Refreshing books remain invalid until a snapshot.
    let mut suspended = BTreeSet::<Asset>::new();
    let mut maintenance_after = start + COOLDOWN.mul_f64(slot as f64 / config.replicas as f64);
    let mut urgent = BTreeSet::<Asset>::new();
    let mut diagnostics = 0;
    let mut ping_count = 0u64;
    let mut pong_count = 0u64;
    let mut data_frames = 0u64;
    let mut last_ping_ms = 0u64;
    loop {
        let work = subscriptions_dirty || !refreshing.is_empty();
        // Removing resolved assets is safe even while new work is paused.
        if work {
            pending_since.get_or_insert_with(Instant::now);
        } else {
            pending_since = None;
        }
        // Restore completes an already-started unsubscribe even without PONG.
        let eligible = work
            && (resubscribe
                || pending_pings == 0
                || (refreshing.is_empty() && !pending_removed.is_empty()));
        if !work {
            subscription_wait = None;
        } else if Instant::now() >= batch_at {
            let turn = subscription_wait.get_or_insert_with(|| subscription_budget.enqueue());
            if eligible {
                turn.resume();
            } else {
                turn.park();
            }
        }
        let deadline =
            pong_deadline.unwrap_or_else(|| Instant::now() + Duration::from_secs(31_536_000));
        tokio::select! { biased;
            _ = local.cancelled() => return Ok("cancelled"),
            _ = sleep_until(deadline), if pong_deadline.is_some() => return Err("application_pong_timeout"),
            changed = assets.changed(), if watching => {
                if changed.is_err() { watching = false; }
                else {
                    desired = assets.latest();
                    pending_added = desired.difference(&requested).cloned().collect();
                    pending_removed = requested.difference(&desired).cloned().collect();
                    subscriptions_dirty = !pending_added.is_empty() || !pending_removed.is_empty();
                    refreshing.retain(|s| desired.contains(s) && requested.contains(s));
                    suspended.retain(|s| refreshing.contains(s));
                    if refreshing.is_empty() { resubscribe = false; }
                }
            },
            _ = heartbeat.tick() => {
                let blocked = Instant::now();
                park_turn(&subscription_wait);
                write(socket, Message::Text("PING".into())).await?;
                rebase_pong(&mut pong_deadline, blocked);
                pending_pings = pending_pings.saturating_add(1);
                ping_count+=1;last_ping_ms=millis(start);
                // Later PINGs never extend the oldest unanswered deadline.
                pong_deadline.get_or_insert_with(|| Instant::now() + Duration::from_secs(20));
            },
            _ = sleep_until(batch_at), if eligible && subscription_wait.is_none() => {},
            _ = async { subscription_wait.as_ref().unwrap().ready().await }, if eligible && subscription_wait.is_some() => {
                let turn = subscription_wait.take().unwrap();
                let blocked = Instant::now();
                if !refreshing.is_empty() {
                    refreshing.retain(|s| desired.contains(s) && requested.contains(s));
                    if !refreshing.is_empty() {
                        if !resubscribe {
                            suspended = refreshing.iter().cloned().collect();
                            // Regression quarantine has already sent its Reset. Only
                            // invalidate books that are still usable at refresh start.
                            let invalidate = refreshing.iter().filter(|symbol| timestamps.get(*symbol)
                                .is_some_and(|state| state.snapshot.is_some() && !state.quarantined))
                                .cloned().collect::<BTreeSet<_>>();
                            for symbol in &refreshing {
                                timestamps.remove(symbol);
                                prebook.take(symbol);
                                monitor.reset(symbol, millis(start));
                            }
                            if !invalidate.is_empty() && !report(notices, Notice::Reset { source, symbols: invalidate }, local).await { return Ok("cancelled"); }
                            maintenance_after = Instant::now() + COOLDOWN;
                            eprintln!("{}", json!({"event":"asset_refresh", "source":source,
                                "assets":refreshing.len(), "reason":"bounded_same_socket_verification"}));
                        }
                        let operation = if resubscribe { "subscribe" } else { "unsubscribe" };
                        let value = json!({"assets_ids":refreshing,"operation":operation,"custom_feature_enabled":true,"initial_dump":true});
                        let text = value.to_string();
                        turn.spend();
                        write(socket, Message::Text(text.into())).await?;
                    }
                    if refreshing.is_empty() { resubscribe = false; }
                    else if resubscribe { suspended.clear(); refreshing.clear(); resubscribe = false; }
                    else { resubscribe = true; }
                } else {
                    let added = pending_removed.is_empty();
                    let pending = if added { &mut pending_added } else { &mut pending_removed };
                    let symbols: Vec<_> = pending.drain(..pending.len().min(BATCH_SIZE)).collect();
                    let value = if !added { json!({"assets_ids":symbols,"operation":"unsubscribe"}) }
                        else if initial { json!({"assets_ids":symbols,"type":"market","custom_feature_enabled":true,"initial_dump":true}) }
                        else { json!({"assets_ids":symbols,"operation":"subscribe","custom_feature_enabled":true,"initial_dump":true}) };
                    let text = value.to_string();
                    turn.spend();
                    write(socket, Message::Text(text.into())).await?;
                    let elapsed = millis(start);
                    for symbol in &symbols {
                        if added {
                            requested.insert(symbol.clone());
                            monitor.subscribe_asset(symbol.clone(), elapsed);
                        } else { requested.remove(symbol); monitor.unsubscribe(symbol); urgent.remove(symbol); }
                        timestamps.remove(symbol);
                        prebook.take(symbol);
                    }
                    subscriptions_dirty = !pending_added.is_empty() || !pending_removed.is_empty();
                    if added { initial = false; }
                    if !report(notices, Notice::Subscribed { source, symbols, added }, local).await { return Ok("cancelled"); }
                }
                // Rebase from completion, never from an old interval tick:
                // a delayed PONG or global turn cannot accumulate past credits.
                batch_at = Instant::now() + BATCH_INTERVAL;
                pending_since = Some(Instant::now());
                rebase_pong(&mut pong_deadline, blocked);
            },
            _ = check.tick() => {
                // Data liveness is independent of initialization progress.
                // Recover a source that can no longer subscribe new symbols;
                // stagger replicas so a healthy copy can remain available.
                if work
                    && pending_since.is_some_and(|since| since.elapsed() >= Duration::from_secs(180 + (slot as u64).saturating_mul(30))) {
                    return Err("subscription_progress_timeout");
                }
                if work && pending_since.is_some_and(|since| since.elapsed() >= Duration::from_secs(30))
                    && last_stall_report.elapsed() >= Duration::from_secs(60) {
                    last_stall_report = Instant::now();
                    eprintln!("{}", json!({"event":"subscription_stall","source":source,
                        "session_ms":millis(start),"desired":desired.len(),"subscribed":requested.len(),
                        "pending_additions":pending_added.len(),"awaiting_application_pong":pending_pings>0,
                        "pings":ping_count,"pongs":pong_count,"budget_waiting":subscription_wait.is_some()}));
                }
                urgent.retain(|s| requested.contains(s) && desired.contains(s));
                if refreshing.is_empty() && !subscriptions_dirty && Instant::now() >= maintenance_after {
                    refreshing = urgent.iter().take(BATCH_SIZE).cloned().collect();
                    for symbol in &refreshing { urgent.remove(symbol); }
                    if refreshing.is_empty() {
                        refreshing = monitor.due(millis(start), BATCH_SIZE).into_iter()
                            .filter_map(|(symbol, _)| requested.get(symbol.as_str()).cloned()).collect();
                    }
                }
            },
            incoming = socket.next() => match incoming {
                Some(Ok(Message::Text(text))) if text.trim() == "PONG" => { pong_count+=1; pending_pings = 0; pong_deadline = None; },
                Some(Ok(Message::Text(text))) if text.as_str() == "PING" => { park_turn(&subscription_wait); pong_deadline=None; write(socket, Message::Text("PONG".into())).await?; },
                Some(Ok(Message::Text(text))) => {
                    // A queued PONG cannot make a continuously delivering market
                    // stream dead. Subscription/book progress is monitored separately.
                    pong_deadline = None;data_frames+=1;
                    let blocked = Instant::now();
                    let received_ms = chrono::Utc::now().timestamp_millis();
                    let valid = validate_frame(&text, &requested, &suspended, &mut timestamps, &mut prebook, &mut monitor, millis(start), source, &mut diagnostics)
                        .map_err(|_| "invalid_frame")?;
                    for progress in &valid.progress { if progress.snapshot { urgent.remove(&progress.symbol); } }
                    if !valid.resets.is_empty() {
                        urgent.extend(valid.resets.iter().cloned());
                        if !report_read(notices, Notice::Reset { source, symbols: valid.resets }, local, &mut subscription_wait).await { return Ok("cancelled"); }
                    }
                    if !report_read(notices, Notice::Frame { source, events: valid.events, received_ms, progress: valid.progress }, local, &mut subscription_wait).await { return Ok("cancelled"); }
                    // No reads are possible while validating or waiting for the
                    // bounded mailbox. A queued PONG must get its original read
                    // opportunity after that intentional pause. A truly dead peer
                    // still expires after twenty seconds of available read time.
                    rebase_pong(&mut pong_deadline, blocked);
                },
                Some(Ok(Message::Ping(bytes))) => { let blocked=Instant::now(); park_turn(&subscription_wait); write(socket, Message::Pong(bytes)).await?; rebase_pong(&mut pong_deadline, blocked); },
                Some(Ok(Message::Pong(_))) => {},
                Some(Ok(Message::Close(frame))) => {
                    let denied=frame.as_ref().is_some_and(|frame| frame.code==tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Policy);
                    eprintln!("{}",json!({"event":"socket_peer_close","source":source,
                        "code":frame.as_ref().map(|frame|u16::from(frame.code)),
                        "reason":frame.as_ref().map(|frame|frame.reason.chars().take(256).collect::<String>()),
                        "session_ms":millis(start),"pings":ping_count,"pongs":pong_count}));
                    if denied {return Err("peer_policy_denied");}
                    return Ok("peer_closed");
                },
                None => return Ok("peer_closed"),
                Some(Err(error)) => {
                    eprintln!("{}", json!({"event":"socket_read_error","source":source,
                        "reason":error.to_string().chars().take(512).collect::<String>(),
                        "session_ms":millis(start),"pings":ping_count,"pongs":pong_count,"data_frames":data_frames,"last_ping_ms":last_ping_ms}));
                    return Err("transport_failed");
                },
                Some(Ok(Message::Binary(_))) => return Err("binary_frame"),
                Some(Ok(_)) => {},
            }
        }
    }
}
fn rebase_pong(deadline: &mut Option<Instant>, blocked_since: Instant) {
    if let Some(deadline) = deadline {
        *deadline += blocked_since.elapsed();
    }
}
fn millis(start: Instant) -> u64 {
    start.elapsed().as_millis().min(u128::from(u64::MAX)) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn refresh_keeps_healthy_book_until_global_budget_grants_unsubscribe() {
        use futures_util::FutureExt;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let pid = std::process::id();
        let started = std::process::Command::new("ps")
            .args(["-p", &pid.to_string(), "-o", "lstart="])
            .output()
            .unwrap();
        let resource = |event| {
            eprintln!(
                "RESOURCE {}",
                json!({"owner":"connection-budget-unit", "event":event,
            "pid":pid,"start_time":String::from_utf8_lossy(&started.stdout).trim(),"ports":[addr.port()],
            "cleanup":format!("cancel session, join, drop sockets, rebind; emergency kill -INT {pid}")})
            )
        };
        resource("bound");
        let (client, accepted) = tokio::join!(
            connect_async_with_config(format!("ws://{addr}"), None, false),
            async {
                let (tcp, _) = listener.accept().await.unwrap();
                tokio_tungstenite::accept_async(tcp).await.unwrap()
            }
        );
        let (mut client, _) = client.unwrap();
        let mut peer = accepted;
        let mut config = supervisor_fixture().0.config;
        config.stale_secs = 1;
        let (_assets, receiver) = watch::channel(BTreeSet::from(["a".to_owned()]));
        let (tx, mut notices) = mpsc::channel(32);
        let stop = CancellationToken::new();
        let token = stop.clone();
        let budget = Arc::new(SubscriptionBudget::new());
        let session_budget = budget.clone();
        let mut task = tokio::spawn(async move {
            session(
                &config,
                SubscriptionSource::legacy(receiver),
                0,
                0,
                &mut client,
                &tx,
                &token,
                session_budget,
            )
            .await
        });
        let mut paused = false;
        let outcome = std::panic::AssertUnwindSafe(async {
            assert!(matches!(peer.next().await, Some(Ok(Message::Text(_)))));
            assert!(matches!(notices.recv().await, Some(Notice::Subscribed { .. })));
            peer.send(Message::Text(json!({"event_type":"book","asset_id":"a","timestamp":"1000","bids":[],"asks":[]}).to_string().into())).await.unwrap();
            assert!(matches!(notices.recv().await, Some(Notice::Frame { .. })));
            tokio::time::pause(); paused = true;
            let blocker = budget.enqueue();
            blocker.ready().await;
            tokio::time::advance(Duration::from_secs(2)).await;
            for _ in 0..200 { tokio::task::yield_now().await; }
            assert!(notices.try_recv().is_err(), "budget wait reset a healthy book");
            peer.send(Message::Text(json!({"event_type":"price_change","timestamp":"1001","price_changes":[{"asset_id":"a","side":"BUY","price":"0.5","size":"7"}]}).to_string().into())).await.unwrap();
            let mut healthy = false;
            for _ in 0..20_000 {
                tokio::task::yield_now().await;
                match notices.try_recv() {
                    Ok(Notice::Frame { events, .. }) => { assert!(matches!(events.as_slice(), [Event::Delta { quantity:7_000_000, .. }])); healthy=true; break; },
                    Ok(Notice::Reset { .. }) => panic!("budget wait invalidated live updates"),
                    _ => {}
                }
            }
            assert!(healthy, "budget wait stalled valid updates");
            drop(blocker);
            let mut unsubscribed = false;
            for _ in 0..20_000 {
                tokio::task::yield_now().await;
                if let Some(Some(Ok(Message::Text(text)))) = peer.next().now_or_never() {
                    assert_eq!(serde_json::from_str::<serde_json::Value>(&text).unwrap()["operation"], "unsubscribe");
                    unsubscribed=true; break;
                }
            }
            assert!(unsubscribed);
            assert!(matches!(notices.try_recv(), Ok(Notice::Reset { symbols, .. }) if symbols == BTreeSet::from([intern("a")])));
        }).catch_unwind().await;
        if paused {
            tokio::time::resume();
        }
        stop.cancel();
        let joined = timeout(Duration::from_secs(3), &mut task).await;
        let clean = matches!(joined, Ok(Ok(Ok("cancelled"))));
        if joined.is_err() {
            task.abort();
            let _ = task.await;
        }
        drop(peer);
        drop(listener);
        let socket = tokio::net::TcpSocket::new_v4().unwrap();
        socket.set_reuseaddr(true).unwrap();
        socket.bind(addr).unwrap();
        drop(socket.listen(1).unwrap());
        resource("joined_and_rebound");
        assert!(clean, "session did not cancel and join cleanly");
        if let Err(panic) = outcome {
            std::panic::resume_unwind(panic);
        }
    }

    fn supervisor_fixture() -> (Shard, mpsc::Receiver<Input>) {
        let (_, receiver) = watch::channel(Arc::new(BTreeSet::new()));
        let (tx, input) = mpsc::channel(32);
        let (notices, _) = mpsc::channel(1);
        let (fatal, _) = watch::channel(None);
        let now = Instant::now();
        let shard = Shard {
            config: Config {
                discovery_url: String::new(),
                websocket_url: String::new(),
                shard_size: 1000,
                replicas: 1,
                discovery_secs: 60,
                stale_secs: 300,
                rotate_secs: 3600,
                max_symbols: None,
                audit_catalog: false,
            },
            assets: SubscriptionSource::Shared(receiver),
            current: Arc::new(BTreeSet::new()),
            tx,
            stop: CancellationToken::new(),
            dial: Arc::new(Mutex::new(now)),
            source_ids: Arc::new(AtomicUsize::new(0)),
            notices,
            tasks: JoinSet::new(),
            feeds: BTreeMap::new(),
            slots: vec![Slot {
                active: None,
                failures: 3,
                retry_at: now,
                rotate_at: now,
                warm_failures: 0,
            }],
            warm: None,
            warm_slots: Arc::new(Semaphore::new(1)),
            subscription_budget: Arc::new(SubscriptionBudget::new()),
            fatal,
        };
        (shard, input)
    }

    #[tokio::test(start_paused = true)]
    async fn later_catalog_updates_do_not_postpone_initial_rotation() {
        let (mut shard, _input) = supervisor_fixture();
        let a = intern("a");
        let b = intern("b");
        shard.current = Arc::new([a.clone()].into());
        shard.slots[0].active = Some(0);
        shard.feeds.insert(
            0,
            Feed {
                slot: 0,
                cancel: CancellationToken::new(),
                started: Instant::now(),
                retiring: false,
                initial_ramp_complete: false,
                subscribed: BTreeSet::new(),
                subscriptions_pending: 1,
                initialized: BTreeSet::new(),
                waters: BTreeMap::new(),
            },
        );
        shard
            .notice(Notice::Subscribed {
                source: 0,
                symbols: vec![a.clone()],
                added: true,
            })
            .await
            .unwrap();
        let initial_deadline = shard.slots[0].rotate_at;
        tokio::time::advance(Duration::from_secs(3590)).await;
        shard.current = Arc::new([a.clone(), b.clone()].into());
        shard.feeds.get_mut(&0).unwrap().subscriptions_pending = 1;
        shard
            .notice(Notice::Subscribed {
                source: 0,
                symbols: vec![b.clone()],
                added: true,
            })
            .await
            .unwrap();
        assert_eq!(
            shard.slots[0].rotate_at, initial_deadline,
            "catalog addition postponed rotation"
        );
        shard.current = Arc::new([a].into());
        shard.feeds.get_mut(&0).unwrap().subscriptions_pending = 1;
        shard
            .notice(Notice::Subscribed {
                source: 0,
                symbols: vec![b],
                added: false,
            })
            .await
            .unwrap();
        assert_eq!(
            shard.slots[0].rotate_at, initial_deadline,
            "catalog removal postponed rotation"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn pong_deadline_counts_exactly_twenty_seconds_of_available_reads() {
        let ping = Instant::now();
        let mut deadline = Some(ping + Duration::from_secs(20));
        tokio::time::advance(Duration::from_secs(7)).await;
        let blocked = Instant::now();
        tokio::time::advance(Duration::from_secs(60)).await;
        rebase_pong(&mut deadline, blocked);
        assert_eq!(deadline, Some(ping + Duration::from_secs(80)));
        tokio::time::advance(Duration::from_secs(12)).await;
        assert!(Instant::now() < deadline.unwrap());
        tokio::time::advance(Duration::from_secs(1)).await;
        assert_eq!(Instant::now(), deadline.unwrap());
    }

    #[tokio::test(start_paused = true)]
    async fn blocked_notice_releases_budget_turn_and_cancels_cleanly() {
        use futures_util::FutureExt;
        let budget = Arc::new(SubscriptionBudget::new());
        let mut head = Some(budget.enqueue());
        let tail = budget.enqueue();
        let (tx, _rx) = mpsc::channel(1);
        tx.try_send(Notice::End {
            source: 0,
            reason: "test",
        })
        .unwrap();
        let stop = CancellationToken::new();
        {
            let report = report_read(
                &tx,
                Notice::End {
                    source: 1,
                    reason: "test",
                },
                &stop,
                &mut head,
            );
            tokio::pin!(report);
            assert!(report.as_mut().now_or_never().is_none());
            assert!(
                tail.ready().now_or_never().is_some(),
                "backpressured shard retained global FIFO head"
            );
            tail.spend();
            stop.cancel();
            assert!(!report.await);
        }
        drop(head);
        assert!(budget.state.lock().unwrap().queue.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn backpressure_keeps_ticket_seniority_after_the_mailbox_drains() {
        use futures_util::FutureExt;
        let budget = Arc::new(SubscriptionBudget::new());
        let mut head = Some(budget.enqueue());
        let tail = budget.enqueue();
        let (tx, mut rx) = mpsc::channel(1);
        tx.try_send(Notice::Connected { source: 0 }).unwrap();
        let stop = CancellationToken::new();
        {
            let report = report_read(&tx, Notice::Connected { source: 1 }, &stop, &mut head);
            tokio::pin!(report);
            assert!(report.as_mut().now_or_never().is_none());
            assert!(tail.ready().now_or_never().is_some());
            tail.spend();
            rx.try_recv().unwrap();
            assert!(report.await);
        }
        assert!(
            head.is_some(),
            "mailbox backpressure discarded socket seniority"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn parked_head_resumes_before_ungranted_tail_and_grants_are_exclusive() {
        use futures_util::FutureExt;
        let budget = Arc::new(SubscriptionBudget::new());
        let head = budget.enqueue();
        let middle = budget.enqueue();
        let tail = budget.enqueue();
        head.park();
        assert!(middle.ready().now_or_never().is_some());
        // Resume races with a successor that already owns a grant.
        head.resume();
        assert!(head.ready().now_or_never().is_none());
        assert!(tail.ready().now_or_never().is_none());
        middle.spend();
        tokio::time::advance(GLOBAL_BATCH_INTERVAL).await;
        assert!(tail.ready().now_or_never().is_none());
        assert!(head.ready().now_or_never().is_some());
        // Parking an unused grant releases it without spending future credit.
        head.park();
        assert!(tail.ready().now_or_never().is_some());
        drop(tail);
        head.resume();
        assert!(head.ready().now_or_never().is_some());
        head.spend();
        assert!(budget.state.lock().unwrap().queue.is_empty());
        assert!(budget.state.lock().unwrap().reserved.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn cancelling_parked_and_reserved_tickets_releases_without_spending() {
        use futures_util::FutureExt;
        let budget = Arc::new(SubscriptionBudget::new());
        let head = budget.enqueue();
        let tail = budget.enqueue();
        head.park();
        drop(head);
        tail.ready().await;
        drop(tail);
        let next = budget.enqueue();
        assert!(next.ready().now_or_never().is_some());
        next.spend();
        assert_eq!(
            budget.state.lock().unwrap().next,
            Instant::now() + GLOBAL_BATCH_INTERVAL
        );
    }

    #[tokio::test(start_paused = true)]
    async fn warm_grace_starts_after_actual_ramp_even_with_long_shared_budget_wait() {
        let (mut shard, _input) = supervisor_fixture();
        let asset = intern("a");
        shard.current = Arc::new([asset.clone()].into());
        shard.slots[0].active = Some(0);
        for source in [0, 1] {
            shard.feeds.insert(
                source,
                Feed {
                    slot: 0,
                    cancel: CancellationToken::new(),
                    started: Instant::now(),
                    retiring: false,
                    initial_ramp_complete: false,
                    subscribed: if source == 0 {
                        [asset.clone()].into()
                    } else {
                        BTreeSet::new()
                    },
                    subscriptions_pending: usize::from(source == 1),
                    initialized: if source == 0 {
                        [asset.clone()].into()
                    } else {
                        BTreeSet::new()
                    },
                    waters: if source == 0 {
                        [(asset.clone(), 1000)].into()
                    } else {
                        BTreeMap::new()
                    },
                },
            );
        }
        shard.warm = Some(Warm {
            source: 1,
            slot: 0,
            deadline: None,
            ramp_deadline: None,
            target: None,
            _permit: shard.warm_slots.clone().acquire_owned().await.unwrap(),
        });
        shard.notice(Notice::Connected { source: 1 }).await.unwrap();
        // Global budget/PONG backlogs may delay the ramp longer than the entire
        // snapshot grace. That is not evidence of a missing warm snapshot yet.
        tokio::time::advance(Duration::from_secs(240)).await;
        shard.maintain().unwrap();
        assert!(shard.warm.is_some());
        assert!(!shard.feeds[&1].cancel.is_cancelled());
        shard
            .notice(Notice::Subscribed {
                source: 1,
                symbols: vec![asset],
                added: true,
            })
            .await
            .unwrap();
        tokio::time::advance(Duration::from_secs(119)).await;
        shard.maintain().unwrap();
        assert!(shard.warm.is_some());
        tokio::time::advance(Duration::from_secs(1)).await;
        shard.maintain().unwrap();
        assert!(
            shard.warm.is_none(),
            "missing snapshots never timed out after ramp completion"
        );
        assert!(shard.feeds[&1].cancel.is_cancelled());
        assert!(
            !shard.feeds[&0].cancel.is_cancelled(),
            "deadline broke outgoing feed"
        );
        assert!(shard.tasks.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn a_never_completed_warm_ramp_releases_its_global_slot() {
        let (mut shard, _input) = supervisor_fixture();
        let asset = intern("a");
        shard.current = Arc::new([asset.clone()].into());
        shard.slots[0].active = Some(0);
        for source in [0, 1] {
            shard.feeds.insert(
                source,
                Feed {
                    slot: 0,
                    cancel: CancellationToken::new(),
                    started: Instant::now(),
                    retiring: false,
                    initial_ramp_complete: false,
                    subscribed: if source == 0 {
                        [asset.clone()].into()
                    } else {
                        BTreeSet::new()
                    },
                    subscriptions_pending: usize::from(source == 1),
                    initialized: if source == 0 {
                        [asset.clone()].into()
                    } else {
                        BTreeSet::new()
                    },
                    waters: if source == 0 {
                        [(asset.clone(), 1000)].into()
                    } else {
                        BTreeMap::new()
                    },
                },
            );
        }
        shard.warm = Some(Warm {
            source: 1,
            slot: 0,
            deadline: None,
            ramp_deadline: None,
            target: None,
            _permit: shard.warm_slots.clone().acquire_owned().await.unwrap(),
        });
        shard.notice(Notice::Connected { source: 1 }).await.unwrap();
        let limit = warm_ramp_limit(shard.current.len());
        tokio::time::advance(limit - Duration::from_secs(1)).await;
        shard.maintain().unwrap();
        assert!(shard.warm.is_some());
        tokio::time::advance(Duration::from_secs(1)).await;
        shard.maintain().unwrap();
        assert!(
            shard.warm.is_none(),
            "uncompleted ramp retained global warm slot forever"
        );
        assert_eq!(shard.warm_slots.available_permits(), 1);
        assert!(shard.feeds[&1].cancel.is_cancelled());
        assert!(!shard.feeds[&0].cancel.is_cancelled());
        assert!(shard.tasks.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn subscription_budget_is_fifo_cancellable_and_never_saves_credits() {
        use futures_util::FutureExt;
        let budget = Arc::new(SubscriptionBudget::new());
        let first = budget.enqueue();
        first.ready().await;
        first.spend();
        let head = budget.enqueue();
        let middle = budget.enqueue();
        let tail = budget.enqueue();
        assert!(head.ready().now_or_never().is_none());
        // Cancelling queue members spends nothing and releases the next waiter.
        drop(head);
        drop(middle);
        tokio::time::advance(Duration::from_millis(24)).await;
        assert!(tail.ready().now_or_never().is_none());
        tokio::time::advance(Duration::from_millis(1)).await;
        tail.ready().await;
        tail.spend();
        tokio::time::advance(Duration::from_secs(120)).await;
        let first = budget.enqueue();
        let second = budget.enqueue();
        assert!(second.ready().now_or_never().is_none(), "FIFO overtaking");
        first.ready().await;
        first.spend();
        assert!(
            second.ready().now_or_never().is_none(),
            "idle time accumulated burst credits"
        );
        tokio::time::advance(Duration::from_millis(25)).await;
        second.ready().await;
        second.spend();
        assert!(
            budget.state.lock().unwrap().queue.is_empty(),
            "cancelled tickets leaked"
        );
    }

    #[tokio::test]
    async fn shared_subscription_source_keeps_generation_and_asset_allocations() {
        let asset = Asset::from("shared-without-registration");
        let initial: Arc<BTreeSet<Asset>> = Arc::new([asset.clone()].into());
        let (tx, rx) = watch::channel(initial.clone());
        let mut source = SubscriptionSource::Shared(rx);
        assert!(Arc::ptr_eq(&initial, &source.latest()));
        let mut replica = source.clone();
        assert!(Arc::ptr_eq(&initial, &replica.latest()));
        tx.send_replace(Arc::new([Asset::from("overwritten")].into()));
        let next: Arc<BTreeSet<Asset>> = Arc::new([asset.clone(), Asset::from("next")].into());
        tx.send_replace(next.clone());
        source.changed().await.unwrap();
        let latest = source.latest();
        assert!(Arc::ptr_eq(&next, &latest));
        assert!(Arc::ptr_eq(&asset, latest.get(asset.as_ref()).unwrap()));
        replica.changed().await.unwrap();
        assert!(Arc::ptr_eq(&next, &replica.latest()));
        assert_eq!(source.latest().len(), 2);
    }

    #[tokio::test]
    async fn legacy_subscription_source_converts_only_unseen_generations() {
        let (tx, rx) = watch::channel(["legacy-a".to_owned()].into());
        let mut source = SubscriptionSource::legacy(rx);
        let initial = source.latest();
        assert!(Arc::ptr_eq(&initial, &source.latest()));
        let mut replica = source.clone();
        assert!(Arc::ptr_eq(&initial, &replica.latest()));
        tx.send_replace(["legacy-b".to_owned()].into());
        source.changed().await.unwrap();
        let next = source.latest();
        assert!(next.contains("legacy-b"));
        assert!(!next.contains("legacy-a"));
        assert!(Arc::ptr_eq(&next, &source.latest()));
        assert!(Arc::ptr_eq(&next, &source.clone().latest()));
        replica.changed().await.unwrap();
        assert!(replica.latest().contains("legacy-b"));
        tx.send_replace(["legacy-final".to_owned()].into());
        drop(tx);
        assert!(source.latest().contains("legacy-final"));
    }

    #[tokio::test]
    async fn reconnect_failures_require_five_minutes_of_stability_to_reset() {
        let (mut shard, mut input) = supervisor_fixture();
        for (source, (seconds, expected)) in [(31, 4), (299, 5), (300, 1)].into_iter().enumerate() {
            shard.slots[0].active = Some(source);
            shard.feeds.insert(
                source,
                Feed {
                    slot: 0,
                    cancel: CancellationToken::new(),
                    started: Instant::now() - Duration::from_secs(seconds),
                    retiring: false,
                    initial_ramp_complete: false,
                    subscribed: BTreeSet::new(),
                    subscriptions_pending: 0,
                    initialized: BTreeSet::new(),
                    waters: BTreeMap::new(),
                },
            );
            assert!(
                shard
                    .notice(Notice::End {
                        source,
                        reason: "peer_closed"
                    })
                    .await
                    .unwrap()
            );
            assert!(
                matches!(input.recv().await, Some(Input::Disconnected { source: s }) if s == source)
            );
            assert_eq!(shard.slots[0].failures, expected, "lifetime {seconds}s");
        }
    }
}
