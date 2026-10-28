//! Loopback-only transport tests. Every fixture is cancelled/joined even after
//! an assertion panic, and its listener is rebound before propagating the panic.
use futures_util::{FutureExt, SinkExt, StreamExt, future::BoxFuture};
use orderbook_collector::{
    assets::Asset,
    connection::{run_shard, run_shard_shared},
    live::{Config, Input},
};
use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    future::Future,
    net::SocketAddr,
    panic::AssertUnwindSafe,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::{Mutex, broadcast, mpsc, watch},
    task::{JoinHandle, JoinSet},
    time::{Instant, timeout},
};
use tokio_tungstenite::{accept_async, tungstenite::Message};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Default)]
struct Behavior {
    stall_first_handshake: bool,
    hold_after: Option<usize>,
    deny_after: Option<(usize, u16)>,
    no_pong: bool,
}
#[derive(Debug)]
enum Wire {
    Open(usize, Instant),
    Text(usize, String, Instant),
    Closed(usize),
}
#[derive(Debug, Clone)]
enum Command {
    Text(usize, String),
    Close(usize),
    PolicyClose(usize),
    Ping(usize),
}
struct Fixture {
    addr: SocketAddr,
    listener: Option<Arc<TcpListener>>,
    stop: CancellationToken,
    tasks: JoinSet<()>,
    commands: broadcast::Sender<Command>,
    events: mpsc::UnboundedReceiver<Wire>,
    run_stop: CancellationToken,
    run: Option<JoinHandle<anyhow::Result<()>>>,
    assets: watch::Sender<BTreeSet<String>>,
    input: mpsc::Receiver<Input>,
    ids: Arc<AtomicUsize>,
    dial: Arc<Mutex<Instant>>,
}
fn set(ids: &[&str]) -> BTreeSet<String> {
    ids.iter().map(|s| (*s).into()).collect()
}
fn book(symbol: &str, timestamp: u64) -> String {
    json!({"event_type":"book","asset_id":symbol,"timestamp":timestamp.to_string(),"bids":[],"asks":[]}).to_string()
}
fn log_resource(event: &str, addr: SocketAddr) {
    let pid = std::process::id();
    let started = std::process::Command::new("ps")
        .args(["-p", &pid.to_string(), "-o", "lstart="])
        .output()
        .unwrap();
    eprintln!(
        "RESOURCE {}",
        json!({"owner":"connection-loopback-tests","event":event,"pid":pid,
        "start_time":String::from_utf8_lossy(&started.stdout).trim(),"ports":[addr.port()],
        "cleanup":format!("scoped cancellation, join, rebind; emergency kill -INT {pid}")})
    );
}
impl Fixture {
    async fn new(behavior: Behavior) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let listener = Arc::new(listener);
        let owned_listener = listener.clone();
        let addr = listener.local_addr().unwrap();
        log_resource("bound", addr);
        let stop = CancellationToken::new();
        let token = stop.clone();
        let (events_tx, events) = mpsc::unbounded_channel();
        let (commands, _) = broadcast::channel::<Command>(128);
        let sender = commands.clone();
        let mut tasks = JoinSet::new();
        tasks.spawn(async move {
            let mut children = JoinSet::new(); let mut next = 0;
            loop { tokio::select! {
                _ = token.cancelled() => break,
                Some(result) = children.join_next() => result.unwrap(),
                accepted = listener.accept() => {
                    let (mut stream, _) = accepted.unwrap(); let id = next; next += 1;
                    let events = events_tx.clone(); let token = token.clone();
                    let mut commands = sender.subscribe(); let behavior = behavior.clone();
                    let _ = events.send(Wire::Open(id, Instant::now()));
                    children.spawn(async move {
                        let work = async {
                            if behavior.stall_first_handshake && id == 0 { std::future::pending::<()>().await; }
                            if let Some((first, status)) = behavior.deny_after && id >= first {
                                let mut bytes = [0; 4096]; let _ = stream.read(&mut bytes).await;
                                let _ = stream.write_all(format!("HTTP/1.1 {status} Denied\r\nContent-Length: 0\r\n\r\n").as_bytes()).await;
                                return;
                            }
                            let Ok(mut socket) = accept_async(stream).await else { return; };
                            loop { tokio::select! {
                                _ = token.cancelled() => { let _ = timeout(Duration::from_millis(100), socket.close(None)).await; break; },
                                command = commands.recv() => match command {
                                    Ok(Command::Text(target, text)) if target == id => { if socket.send(Message::Text(text.into())).await.is_err() { break; } },
                                    Ok(Command::Close(target)) if target == id => { let _ = socket.close(None).await; break; },
                                    Ok(Command::PolicyClose(target)) if target == id => {
                                        use tokio_tungstenite::tungstenite::protocol::{CloseFrame, frame::coding::CloseCode};
                                        let _ = socket.close(Some(CloseFrame { code: CloseCode::Policy, reason: "Forbidden".into() })).await;
                                        break;
                                    },
                                    Ok(Command::Ping(target)) if target == id && socket.send(Message::Ping(b"test".to_vec().into())).await.is_err() => break,
                                    _ => {},
                                },
                                incoming = socket.next() => match incoming {
                                    Some(Ok(Message::Text(text))) => {
                                        let text = text.to_string(); let _ = events.send(Wire::Text(id, text.clone(), Instant::now()));
                                        if text == "PING" { if !behavior.no_pong { let _ = socket.send(Message::Text("PONG".into())).await; } continue; }
                                        if let Ok(v) = serde_json::from_str::<Value>(&text)
                                            && v["operation"] != "unsubscribe" && !behavior.hold_after.is_some_and(|n| id >= n)
                                            && let Some(ids) = v["assets_ids"].as_array() {
                                                let books: Vec<_> = ids.iter().filter_map(Value::as_str).filter(|s| *s != "provisional")
                                                    .map(|s| serde_json::from_str::<Value>(&book(s, 1000)).unwrap()).collect();
                                                if socket.send(Message::Text(json!(books).to_string().into())).await.is_err() { break; }
                                            }
                                    },
                                    Some(Ok(Message::Ping(bytes))) => { let _ = socket.send(Message::Pong(bytes)).await; },
                                    Some(Ok(Message::Pong(_))) => { let _ = events.send(Wire::Text(id, "CONTROL_PONG".into(), Instant::now())); },
                                    Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break,
                                    _ => {},
                                }
                            }}
                        };
                        tokio::select! { _ = token.cancelled() => {}, _ = work => {} }
                        let _ = events.send(Wire::Closed(id));
                    });
                }
            }}
            while let Some(result) = children.join_next().await { result.unwrap(); }
        });
        let (assets, _) = watch::channel(BTreeSet::new());
        let (_, input) = mpsc::channel(1);
        Self {
            addr,
            listener: Some(owned_listener),
            stop,
            tasks,
            commands,
            events,
            run_stop: CancellationToken::new(),
            run: None,
            assets,
            input,
            ids: Arc::new(AtomicUsize::new(10)),
            dial: Arc::new(Mutex::new(Instant::now())),
        }
    }
    fn config(&self) -> Config {
        Config {
            discovery_url: String::new(),
            websocket_url: format!("ws://{}/market", self.addr),
            shard_size: 1000,
            replicas: 1,
            discovery_secs: 60,
            stale_secs: 300,
            rotate_secs: 3600,
            max_symbols: None,
            audit_catalog: false,
        }
    }
    fn start(&mut self, config: Config, assets: BTreeSet<String>, capacity: usize) {
        self.assets.send_replace(assets);
        let (tx, input) = mpsc::channel(capacity);
        self.input = input;
        self.run = Some(tokio::spawn(run_shard(
            config,
            self.assets.subscribe(),
            tx,
            self.run_stop.clone(),
            self.dial.clone(),
            self.ids.clone(),
        )));
    }
    fn start_shared(
        &mut self,
        config: Config,
        initial: Arc<BTreeSet<Asset>>,
        capacity: usize,
    ) -> watch::Sender<Arc<BTreeSet<Asset>>> {
        let (assets, receiver) = watch::channel(initial);
        let (tx, input) = mpsc::channel(capacity);
        self.input = input;
        self.run = Some(tokio::spawn(run_shard_shared(
            config,
            receiver,
            tx,
            self.run_stop.clone(),
            self.dial.clone(),
            self.ids.clone(),
        )));
        assets
    }
    async fn event(&mut self) -> Wire {
        within(self.events.recv()).await.expect("events closed")
    }
    async fn subscription(&mut self) -> (usize, Value, Instant) {
        loop {
            if let Wire::Text(id, text, at) = self.event().await
                && let Ok(value) = serde_json::from_str::<Value>(&text)
            {
                return (id, value, at);
            }
        }
    }
    async fn frame(&mut self) -> (usize, Vec<orderbook_collector::adapter::Event>) {
        loop {
            match within(self.input.recv()).await.expect("input closed") {
                Input::Frame { source, text, .. } => {
                    return (source, orderbook_collector::adapter::decode(&text).unwrap());
                }
                Input::Events { source, events, .. } => return (source, events),
                Input::Reset { .. } => panic!("maintenance invalidated a trusted book"),
                _ => {}
            }
        }
    }
    fn send(&self, id: usize, text: String) {
        self.commands.send(Command::Text(id, text)).unwrap();
    }
    async fn finish(&mut self) -> anyhow::Result<()> {
        // Borrow rather than move the handle: if the timeout panics, fixture
        // cleanup still owns and joins it.
        let result = within(self.run.as_mut().unwrap()).await.unwrap();
        self.run.take();
        result
    }
    async fn cleanup(&mut self) {
        self.run_stop.cancel();
        let mut failures = Vec::new();
        if let Some(mut run) = self.run.take()
            && timeout(Duration::from_secs(3), &mut run).await.is_err()
        {
            run.abort();
            let _ = run.await;
            failures.push("shard did not join");
        }
        self.stop.cancel();
        while let Some(result) = within(self.tasks.join_next()).await {
            if result.is_err() {
                failures.push("fixture task panicked");
            }
        }
        // Keep the listener reserved until all tasks are joined. Drop and
        // synchronously rebind, so other ephemeral fixtures cannot take its
        // address while this cleanup future is awaiting a join.
        let socket = tokio::net::TcpSocket::new_v4().unwrap();
        socket.set_reuseaddr(true).unwrap();
        drop(self.listener.take());
        within(async {
            loop {
                match socket.bind(self.addr) {
                    Ok(()) => break,
                    // A cancelled TCP handshake can still be completing its
                    // kernel close after both futures are joined. Verify the
                    // port becomes bindable without killing any process.
                    Err(error) if error.kind() == std::io::ErrorKind::AddrInUse => {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                    Err(error) => panic!("listener rebind failed: {error}"),
                }
            }
        })
        .await;
        let rebound = socket.listen(128).unwrap();
        drop(rebound);
        log_resource("joined_and_rebound", self.addr);
        assert!(failures.is_empty(), "cleanup failures: {failures:?}");
    }
}
async fn within<T>(future: impl Future<Output = T>) -> T {
    timeout(Duration::from_secs(6), future)
        .await
        .expect("loopback operation timed out")
}
async fn fixture(
    behavior: Behavior,
    test: impl for<'a> FnOnce(&'a mut Fixture) -> BoxFuture<'a, ()>,
) {
    let mut f = Fixture::new(behavior).await;
    let outcome = AssertUnwindSafe(test(&mut f)).catch_unwind().await;
    f.cleanup().await;
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

// A watch update must not cancel/restart any healthy replica, and all batches
// must be <=100 IDs, paced while reads keep delivering snapshots.
#[tokio::test]
async fn incremental_assets_keep_sockets_and_pace_batches() {
    fixture(Behavior::default(), |f| {
        Box::pin(async move {
            f.start(f.config(), set(&["a"]), 64);
            let (id, _, _) = f.subscription().await;
            let (source, _) = f.frame().await;
            let assets: BTreeSet<_> = (0..201).map(|n| format!("asset-{n:03}")).collect();
            f.assets.send_replace(assets);
            let mut previous = None;
            let mut added = 0;
            while added < 201 {
                let (same, value, at) = f.subscription().await;
                assert_eq!(same, id, "universe update restarted socket");
                assert!(value["assets_ids"].as_array().unwrap().len() <= 100);
                if let Some(last) = previous {
                    assert!(at.duration_since(last) >= Duration::from_millis(90));
                }
                previous = Some(at);
                if value["operation"] == "subscribe" {
                    added += value["assets_ids"].as_array().unwrap().len();
                    assert_eq!(value["initial_dump"], true);
                }
            }
            for _ in 0..3 {
                assert_eq!(f.frame().await.0, source);
            }
            assert_eq!(f.ids.load(Ordering::SeqCst), 11);
        })
    })
    .await;
}

// Both shared-watch generations and in-flight subscription batches may be
// overwritten without restarting the physical connection.
#[tokio::test]
async fn shared_watch_coalesces_overwrites_and_replaces_pending_batches() {
    fixture(Behavior::default(), |f| {
        Box::pin(async move {
            let initial: Arc<BTreeSet<Asset>> =
                Arc::new((0..301).map(|n| Asset::from(format!("a{n:03}"))).collect());
            let assets = f.start_shared(f.config(), initial, 64);
            let (socket, first, _) = f.subscription().await;
            assert_eq!(first["type"], "market");
            assert_eq!(first["assets_ids"].as_array().unwrap().len(), 100);
            let (source, _) = f.frame().await;
            // No await between overwrites: the poison generation must coalesce.
            assets.send_replace(Arc::new([Asset::from("poison")].into()));
            let desired: BTreeSet<Asset> = (50..250)
                .map(|n| Asset::from(format!("a{n:03}")))
                .chain((0..101).map(|n| Asset::from(format!("z{n:03}"))))
                .collect();
            assets.send_replace(Arc::new(desired));
            let (same, removed, _) = f.subscription().await;
            assert_eq!(same, socket);
            assert_eq!(removed["operation"], "unsubscribe");
            assert_eq!(removed["assets_ids"].as_array().unwrap().len(), 50);
            let (_, added, _) = f.subscription().await;
            assert_eq!(added["operation"], "subscribe");
            assert_eq!(added["assets_ids"][0], "a100");
            assert_eq!(added["assets_ids"].as_array().unwrap().len(), 100);
            assert_eq!(f.frame().await.0, source);
            // Replace the generation while 151 additions still remain queued.
            assets.send_replace(Arc::new([Asset::from("a050"), Asset::from("final")].into()));
            let mut removed_count = 0;
            loop {
                let (same, value, _) = f.subscription().await;
                assert_eq!(same, socket);
                let ids = value["assets_ids"].as_array().unwrap();
                assert!(ids.len() <= 100);
                if value["operation"] == "unsubscribe" {
                    removed_count += ids.len();
                } else {
                    assert_eq!(ids, &[json!("final")], "obsolete pending additions leaked");
                    break;
                }
            }
            assert_eq!(removed_count, 149);
            assert_eq!(f.frame().await.0, source);
            assert_eq!(f.ids.load(Ordering::SeqCst), 11);
        })
    })
    .await;
}

#[tokio::test]
async fn cancelled_refresh_starts_next_refresh_with_unsubscribe() {
    fixture(Behavior::default(), |f| {
        Box::pin(async move {
            let mut config = f.config();
            config.stale_secs = 1;
            let assets = f.start_shared(config, Arc::new([Asset::from("a")].into()), 64);
            let (socket, _, _) = f.subscription().await;
            f.frame().await;
            let (_, refresh, _) = f.subscription().await;
            assert_eq!(refresh["operation"], "unsubscribe");
            // Cancel the refresh between its unsubscribe/subscribe phases.
            assets.send_replace(Arc::new([Asset::from("b")].into()));
            let (_, remove, _) = f.subscription().await;
            assert_eq!(remove["operation"], "unsubscribe");
            assert_eq!(remove["assets_ids"], json!(["a"]));
            let (_, add, _) = f.subscription().await;
            assert_eq!(add["operation"], "subscribe");
            assert_eq!(add["assets_ids"], json!(["b"]));
            // The cancelled refresh for a emitted its reset before unsubscribe.
            assert!(matches!(
                within(f.input.recv()).await,
                Some(Input::Reset { .. })
            ));
            f.frame().await;
            tokio::time::pause();
            // Service heartbeats explicitly while waiting out the refresh cooldown.
            let mut next_refresh = None;
            for _ in 0..32 {
                tokio::time::advance(Duration::from_secs(1)).await;
                for _ in 0..100 {
                    tokio::task::yield_now().await;
                }
                while let Ok(event) = f.events.try_recv() {
                    if let Wire::Text(id, text, _) = event {
                        if text == "PING" {
                            f.send(id, "PONG".into());
                        } else if let Ok(value) = serde_json::from_str::<Value>(&text) {
                            next_refresh.get_or_insert((id, value));
                        }
                    }
                }
                if next_refresh.is_some() {
                    break;
                }
            }
            tokio::time::resume();
            let (same, refresh) = next_refresh.expect("next refresh did not run");
            assert_eq!(same, socket);
            assert_eq!(
                refresh["operation"], "unsubscribe",
                "cancelled phase leaked into next refresh"
            );
            assert_eq!(refresh["assets_ids"], json!(["b"]));
        })
    })
    .await;
}

// A warming socket may not retire its predecessor, nor allow a second replica
// to rotate, until its fresh snapshot catches up. Never-book assets cannot block.
#[tokio::test]
async fn rotation_serializes_replicas_and_keeps_old_reading_until_warm() {
    fixture(
        Behavior {
            hold_after: Some(2),
            ..Behavior::default()
        },
        |f| {
            Box::pin(async move {
                let mut config = f.config();
                config.replicas = 2;
                config.rotate_secs = 1;
                f.start(config, set(&["a", "provisional"]), 128);
                let (first, _, _) = f.subscription().await;
                let (source0, _) = f.frame().await;
                let (second, _, _) = f.subscription().await;
                let (source1, _) = f.frame().await;
                assert_ne!(source0, source1);
                let (warm, _, _) = f.subscription().await;
                assert_eq!(warm, 2);
                tokio::time::sleep(Duration::from_millis(650)).await;
                while let Ok(event) = f.events.try_recv() {
                    assert!(
                        !matches!(event, Wire::Closed(_) | Wire::Open(3, _)),
                        "overlapping rotation or old socket lost: {event:?}"
                    );
                }
                f.send(first, book("a", 1002));
                assert_eq!(f.frame().await.0, source0);
                f.send(second, book("a", 1002));
                assert_eq!(f.frame().await.0, source1);
                f.send(warm, book("a", 999));
                let (new_source, _) = f.frame().await;
                assert_ne!(new_source, source0);
                tokio::time::sleep(Duration::from_millis(150)).await;
                while let Ok(event) = f.events.try_recv() {
                    assert!(
                        !matches!(event, Wire::Closed(_)),
                        "behind-watermark warm socket retired old"
                    );
                }
                f.send(warm, book("a", 1002));
                assert_eq!(f.frame().await.0, new_source);
                loop {
                    match f.event().await {
                        Wire::Closed(id) => {
                            assert_eq!(id, first);
                            break;
                        }
                        Wire::Open(3, _) => panic!("second warm before retiring first"),
                        _ => {}
                    }
                }
                let (next_warm, _, _) = f.subscription().await;
                assert_eq!(next_warm, 3);
                f.send(next_warm, book("a", 1002));
                f.frame().await;
                loop {
                    if let Wire::Closed(id) = f.event().await {
                        assert_eq!(id, second);
                        break;
                    }
                }
            })
        },
    )
    .await;
}

#[tokio::test]
async fn quiet_recovery_resets_only_bounded_assets_on_same_socket() {
    fixture(Behavior::default(), |f| {
        Box::pin(async move {
            let mut config = f.config();
            config.stale_secs = 1;
            let assets = (0..250).map(|n| format!("asset-{n:03}")).collect();
            f.start(config, assets, 64);
            let (old, _, _) = f.subscription().await;
            let (source, _) = f.frame().await;
            for _ in 0..2 {
                assert_eq!(f.subscription().await.0, old);
                f.frame().await;
            }
            let (same, unsubscribe, _) = f.subscription().await;
            assert_eq!(same, old, "quiet maintenance replaced the whole socket");
            assert_eq!(unsubscribe["operation"], "unsubscribe");
            let ids = unsubscribe["assets_ids"].as_array().unwrap();
            assert_eq!(ids.len(), 100);
            let (same, subscribe, _) = f.subscription().await;
            assert_eq!(same, old);
            assert_eq!(subscribe["operation"], "subscribe");
            assert_eq!(subscribe["assets_ids"], unsubscribe["assets_ids"]);
            assert_eq!(subscribe["initial_dump"], true);
            assert!(matches!(within(f.input.recv()).await, Some(Input::Reset { source:s, symbols }) if s==source && symbols.len()==100));
            assert_eq!(f.frame().await.0, source);
            tokio::time::sleep(Duration::from_millis(1300)).await;
            while let Ok(event) = f.events.try_recv() {
                assert!(
                    !matches!(event, Wire::Closed(_) | Wire::Open(_, _)),
                    "quiet recovery churn: {event:?}"
                );
                if let Wire::Text(_, text, _) = event {
                    assert!(text == "PING", "maintenance exceeded its batch cooldown");
                }
            }
            assert!(
                f.input.try_recv().is_err(),
                "quiet refresh reset a trusted book"
            );
            assert_eq!(f.ids.load(Ordering::SeqCst), 11);
        })
    })
    .await;
}

fn delta(symbol: &str, timestamp: u64, size: &str) -> Value {
    json!({"event_type":"price_change", "timestamp":timestamp.to_string(),
        "price_changes":[{"asset_id":symbol,"side":"BUY","price":"0.5","size":size}]})
}

#[tokio::test]
async fn snapshot_absorbs_strictly_older_deltas_but_equal_timestamp_remains_ordered() {
    fixture(Behavior::default(), |f| Box::pin(async move {
        f.start(f.config(), set(&["a", "b"]), 32);
        let (socket, _, _) = f.subscription().await; let (source, _) = f.frame().await;
        f.send(socket, book("a", 2000)); f.frame().await;
        f.send(socket, json!([delta("a", 1999, "9"), delta("a", 2000, "2"), delta("b", 1001, "3")]).to_string());
        let (same, events) = f.frame().await;
        assert_eq!(same, source);
        assert_eq!(events.len(), 2, "snapshot-incorporated stale delta reached engine");
        assert!(matches!(&events[0], orderbook_collector::adapter::Event::Delta { symbol, timestamp_ms: 2000, quantity: 2_000_000, .. } if symbol == "a"));
        assert_eq!(f.ids.load(Ordering::SeqCst), 11);
    })).await;
}

#[tokio::test]
async fn regression_quarantines_only_asset_and_reset_precedes_same_socket_recovery() {
    fixture(Behavior { hold_after: Some(0), ..Behavior::default() }, |f| Box::pin(async move {
        f.start(f.config(), set(&["a", "b"]), 32);
        let (socket, _, _) = f.subscription().await;
        f.send(socket, format!("[{},{}]", book("a", 1000), book("b", 1000)));
        let (source, _) = f.frame().await;
        f.send(socket, delta("a", 1002, "2").to_string()); f.frame().await;
        // a:1001 is newer than the snapshot, but regresses the latest delta.
        // The earlier a:1003 in this same frame must also be discarded atomically.
        f.send(socket, json!([delta("a", 1003, "3"), delta("b", 1001, "4"), delta("a", 1001, "9")]).to_string());
        assert!(matches!(within(f.input.recv()).await, Some(Input::Reset { source: s, symbols }) if s == source && symbols == set(&["a"])));
        let (same, events) = f.frame().await; assert_eq!(same, source);
        assert_eq!(events.len(), 1);
        assert!(matches!(&events[0], orderbook_collector::adapter::Event::Delta { symbol, .. } if symbol == "b"));
        let (same, unsub, _) = f.subscription().await; assert_eq!(same, socket);
        assert_eq!(unsub["operation"], "unsubscribe"); assert_eq!(unsub["assets_ids"], json!(["a"]));
        let (same, sub, _) = f.subscription().await; assert_eq!(same, socket);
        assert_eq!(sub["initial_dump"], true);
        f.send(socket, json!([delta("a", 1004, "8"), delta("b", 1002, "5")]).to_string());
        let (_, events) = f.frame().await;
        assert_eq!(events.len(), 1, "quarantined delta escaped");
        f.send(socket, book("a", 1004)); assert_eq!(f.frame().await.0, source);
        f.send(socket, delta("a", 1004, "6").to_string()); assert_eq!(f.frame().await.0, source);
        assert_eq!(f.ids.load(Ordering::SeqCst), 11, "asset regression replaced unrelated books");
    })).await;
}

#[tokio::test]
async fn access_denial_is_fatal_without_retry_and_closes_other_owned_connections() {
    for status in [401, 403] {
        fixture(
            Behavior {
                deny_after: Some((1, status)),
                ..Behavior::default()
            },
            move |f| {
                Box::pin(async move {
                    let mut config = f.config();
                    config.replicas = 2;
                    f.start(config, set(&["a"]), 32);
                    f.subscription().await;
                    f.frame().await;
                    loop {
                        match within(f.input.recv()).await {
                            Some(Input::Fatal(reason)) => {
                                assert!(reason.contains(&status.to_string()));
                                break;
                            }
                            Some(_) => {}
                            None => panic!("denial omitted fatal input"),
                        }
                    }
                    assert!(f.finish().await.is_err());
                    assert_eq!(f.ids.load(Ordering::SeqCst), 12);
                    loop {
                        if let Wire::Closed(id) = f.event().await
                            && id == 0
                        {
                            break;
                        }
                    }
                })
            },
        )
        .await;
    }
}

#[tokio::test]
async fn global_dial_spacing_and_backpressured_cancel_are_scoped() {
    fixture(Behavior::default(), |f| {
        Box::pin(async move {
            let mut config = f.config();
            config.replicas = 3;
            f.start(config, set(&["a"]), 1);
            let mut starts = Vec::new();
            while starts.len() < 3 {
                if let Wire::Open(_, at) = f.event().await {
                    starts.push(at);
                }
            }
            assert!(
                starts
                    .windows(2)
                    .all(|w| w[1].duration_since(w[0]) >= Duration::from_millis(240))
            );
            f.run_stop.cancel();
            assert!(f.finish().await.is_ok());
            let mut closed = BTreeSet::new();
            while closed.len() < 3 {
                if let Wire::Closed(id) = f.event().await {
                    closed.insert(id);
                }
            }
        })
    })
    .await;
}

#[tokio::test]
async fn overflow_is_fatal_instead_of_reusing_a_source() {
    fixture(Behavior::default(), |f| {
        Box::pin(async move {
            f.ids.store(usize::MAX, Ordering::SeqCst);
            f.start(f.config(), set(&["a"]), 8);
            assert!(matches!(
                within(f.input.recv()).await,
                Some(Input::Fatal(_))
            ));
            assert!(f.finish().await.is_err());
            assert_eq!(f.ids.load(Ordering::SeqCst), usize::MAX);
        })
    })
    .await;
}

#[tokio::test]
async fn control_ping_and_unexpected_transport_failure_are_source_scoped() {
    fixture(Behavior::default(), |f| Box::pin(async move {
        let mut config = f.config(); config.replicas = 2;
        f.start(config, set(&["a"]), 32);
        let (first, _, _) = f.subscription().await; let (source0, _) = f.frame().await;
        let (second, _, _) = f.subscription().await; let (source1, _) = f.frame().await;
        f.commands.send(Command::Ping(first)).unwrap();
        loop { if let Wire::Text(id, text, _) = f.event().await && text == "CONTROL_PONG" { assert_eq!(id, first); break; } }
        f.commands.send(Command::Close(first)).unwrap();
        assert!(matches!(within(f.input.recv()).await, Some(Input::Disconnected { source }) if source == source0));
        f.send(second, book("a", 1001)); assert_eq!(f.frame().await.0, source1);
        f.subscription().await; let (fresh, _) = f.frame().await;
        assert_ne!(fresh, source0); assert_ne!(fresh, source1);
    })).await;
}

// Retain the trusted source if a replacement handshake fails or the new
// socket never delivers a book. A cooldown limits repeated replacement attempts.
#[tokio::test]
async fn failed_warm_socket_keeps_old_and_cools_down() {
    fixture(Behavior { hold_after: Some(1), ..Behavior::default() }, |f| Box::pin(async move {
        let mut config = f.config(); config.rotate_secs = 1;
        f.start(config, set(&["a"]), 32);
        let (old, _, _) = f.subscription().await; let (source, _) = f.frame().await;
        let (warm, _, _) = f.subscription().await;
        f.commands.send(Command::Close(warm)).unwrap();
        assert!(matches!(within(f.input.recv()).await, Some(Input::Disconnected { source: s }) if s != source));
        f.send(old, book("a", 1001)); assert_eq!(f.frame().await.0, source);
        tokio::time::sleep(Duration::from_millis(1200)).await;
        assert_eq!(f.ids.load(Ordering::SeqCst), 12, "failed warmup retried without cooldown");
        while let Ok(event) = f.events.try_recv() { assert!(!matches!(event, Wire::Closed(0)), "healthy predecessor was closed"); }
    })).await;
}

#[tokio::test]
async fn heartbeat_requires_application_pong_after_twenty_seconds() {
    fixture(
        Behavior {
            no_pong: true,
            ..Behavior::default()
        },
        |f| {
            Box::pin(async move {
                f.start(f.config(), set(&["a"]), 32);
                let (old, _, _) = f.subscription().await;
                let (source, _) = f.frame().await;
                // Pause only after real TCP/WS establishment, keeping paused network
                // operations free of timeout futures (which could auto-advance time).
                tokio::time::pause();
                tokio::time::advance(Duration::from_secs(11)).await;
                let first_ping_at=Instant::now();
                spin_until(|| {
                    matches!(f.events.try_recv(), Ok(Wire::Text(id, text, _)) if id==old && text=="PING")
                }).await;
                assert_eq!(Instant::now(),first_ping_at);
                tokio::time::advance(Duration::from_millis(10001)).await;
                spin_until(|| matches!(f.events.try_recv(), Ok(Wire::Text(_, text, _)) if text=="PING")).await;
                // A control PONG must not satisfy the application PONG contract.
                f.commands.send(Command::Ping(old)).unwrap();
                spin_until(|| matches!(f.events.try_recv(), Ok(Wire::Text(_, text, _)) if text=="CONTROL_PONG")).await;
                tokio::time::advance(Duration::from_millis(8999)).await;
                for _ in 0..100 { tokio::task::yield_now().await; }
                tokio::time::advance(Duration::from_millis(999)).await;
                for _ in 0..100 { tokio::task::yield_now().await; }
                assert_eq!(Instant::now()-first_ping_at,Duration::from_millis(19999));
                assert!(f.input.try_recv().is_err(), "PONG deadline fired before twenty seconds");
                // Tokio's timer wheel rounds deadlines upward to a millisecond.
                // Cross that driver tick explicitly; no I/O await may auto-advance.
                tokio::time::advance(Duration::from_millis(2)).await;
                spin_until(|| matches!(f.input.try_recv(), Ok(Input::Disconnected { source:s }) if s==source)).await;
                assert_eq!(Instant::now()-first_ping_at,Duration::from_millis(20001));
                tokio::time::resume();
            })
        },
    )
    .await;
}

#[tokio::test]
async fn malformed_and_oversized_frames_are_not_forwarded() {
    for payload in [
        "not json".into(),
        format!("[{},{{\"event_type\":\"book\"}}]", book("a", 1001)),
        "x".repeat(8 * 1024 * 1024 + 1),
    ] {
        fixture(Behavior::default(), move |f| Box::pin(async move {
            f.start(f.config(), set(&["a"]), 32);
            let (old, _, _) = f.subscription().await; let (source, _) = f.frame().await;
            f.send(old, payload);
            assert!(matches!(within(f.input.recv()).await, Some(Input::Disconnected { source: s }) if s == source));
            f.subscription().await; assert_ne!(f.frame().await.0, source);
        })).await;
    }
}

#[tokio::test]
async fn warm_timeout_preserves_old_and_does_not_immediately_retry() {
    fixture(Behavior { hold_after: Some(1), ..Behavior::default() }, |f| Box::pin(async move {
        let mut config = f.config(); config.rotate_secs = 1;
        f.start(config, set(&["a"]), 32);
        let (old, _, _) = f.subscription().await; let (source, _) = f.frame().await;
        let (warm, _, _) = f.subscription().await;
        tokio::time::pause();
        // Advance in small steps and consume PINGs so only the snapshot timeout
        // is under test, not an unserviced heartbeat in a frozen TCP runtime.
        let mut warm_closed = false;
        for second in 1..=121 {
            tokio::time::advance(Duration::from_secs(1)).await;
            for _ in 0..10 { tokio::task::yield_now().await; }
            while let Ok(event) = f.events.try_recv() {
                match event {
                    Wire::Text(id, text, _) if text == "PING" => f.send(id, "PONG".into()),
                    Wire::Closed(id) => { assert_eq!(id, warm, "timeout broke trusted old source"); warm_closed = true; },
                    _ => {},
                }
            }
            if second < 119 { assert!(!warm_closed, "bounded snapshot grace ended too early"); }
        }
        tokio::time::resume();
        if !warm_closed { loop { if let Wire::Closed(id) = f.event().await { assert_eq!(id, warm, "timeout broke trusted old source"); break; } } }
        assert!(matches!(within(f.input.recv()).await, Some(Input::Disconnected { source: s }) if s != source));
        f.send(old, book("a", 1001)); assert_eq!(f.frame().await.0, source);
        assert_eq!(f.ids.load(Ordering::SeqCst), 12);
    })).await;
}

#[tokio::test]
async fn pacer_wait_is_outside_handshake_timeout_and_is_cancel_aware() {
    fixture(Behavior::default(), |f| {
        Box::pin(async move {
            *f.dial.lock().await = Instant::now() + Duration::from_secs(11);
            tokio::time::pause();
            f.start(f.config(), set(&["a"]), 16);
            for _ in 0..20 {
                tokio::task::yield_now().await;
            }
            tokio::time::advance(Duration::from_secs(10)).await;
            for _ in 0..20 {
                tokio::task::yield_now().await;
            }
            assert!(
                f.input.try_recv().is_err(),
                "global pacer wait consumed handshake timeout"
            );
            assert!(
                f.events.try_recv().is_err(),
                "dial started before global pacer deadline"
            );
            tokio::time::advance(Duration::from_secs(1)).await;
            tokio::time::resume();
            f.subscription().await;
            f.frame().await;
            assert_eq!(f.ids.load(Ordering::SeqCst), 11);
        })
    })
    .await;
    fixture(Behavior::default(), |f| {
        Box::pin(async move {
            let dial = f.dial.clone();
            let guard = dial.lock().await;
            f.start(f.config(), set(&["a"]), 16);
            tokio::task::yield_now().await;
            f.run_stop.cancel();
            assert!(f.finish().await.is_ok());
            assert!(f.events.try_recv().is_err());
            drop(guard);
        })
    })
    .await;
}

#[tokio::test]
async fn fatal_denial_cancels_every_socket_even_when_consumer_is_full() {
    fixture(
        Behavior {
            deny_after: Some((2, 403)),
            ..Behavior::default()
        },
        |f| {
            Box::pin(async move {
                let mut config = f.config();
                config.replicas = 3;
                f.start(config, set(&["a"]), 1);
                // Keep the first snapshot in the one-slot output queue, so forwarding
                // the second source blocks when the third handshake is denied.
                assert!(f.finish().await.is_err());
                let mut closed = BTreeSet::new();
                while !closed.contains(&0) || !closed.contains(&1) {
                    if let Wire::Closed(id) = f.event().await {
                        closed.insert(id);
                    }
                }
                assert_eq!(f.ids.load(Ordering::SeqCst), 13);
            })
        },
    )
    .await;
}

#[tokio::test]
async fn cancelled_handshake_still_reserves_global_dial_spacing() {
    fixture(
        Behavior {
            stall_first_handshake: true,
            ..Behavior::default()
        },
        |f| {
            Box::pin(async move {
                f.start(f.config(), set(&["a"]), 8);
                let first_at = loop {
                    if let Wire::Open(0, at) = f.event().await {
                        break at;
                    }
                };
                f.run_stop.cancel();
                assert!(f.finish().await.is_ok());
                f.run_stop = CancellationToken::new();
                f.start(f.config(), set(&["a"]), 8);
                let next_at = loop {
                    if let Wire::Open(1, at) = f.event().await {
                        break at;
                    }
                };
                assert!(
                    next_at.duration_since(first_at) >= Duration::from_millis(240),
                    "cancelled handshake lost its global dial reservation"
                );
                f.subscription().await;
                f.frame().await;
            })
        },
    )
    .await;
}

// Rotation must survive a paced 30k-asset ramp plus a delayed full snapshot,
// without charging the global dial queue against its snapshot grace period.
#[tokio::test]
async fn thirty_thousand_asset_rotation_budgets_ramp_after_global_pacer_wait() {
    fixture(
        Behavior {
            hold_after: Some(1),
            ..Behavior::default()
        },
        |f| {
            Box::pin(async move {
                let mut config = f.config();
                config.rotate_secs = 1;
                f.start(
                    config,
                    (0..30_000).map(|n| format!("asset-{n:05}")).collect(),
                    64,
                );
                let (old, first, _) = f.subscription().await;
                let mut old_assets: BTreeSet<String> = first["assets_ids"].as_array().unwrap().iter().map(|id| id.as_str().unwrap().to_owned()).collect();
                let mut old_count = old_assets.len();
                let mut warm_assets = BTreeSet::<String>::new();
                let mut warm_count = 0;
                let mut premature_close = false;
                let mut promoted = false;
                let mut deferred_at = None;
                let mut warm_open = None;
                let mut warm_socket = None;
                let mut dump_at = None;
                let mut dumped = false;
                let origin = Instant::now();
                let mut dump_elapsed = None;
                let mut saw_snapshot = false;
                tokio::time::pause();
                // Two 300s ramps, a 40s pacer delay and a 75s response delay.
                // Maintenance can repeat subscriptions; count distinct assets.
                for _ in 0..40_000 {
                    tokio::time::advance(Duration::from_millis(25)).await;
                    for _ in 0..100 {
                        tokio::task::yield_now().await;
                    }
                    while let Ok(event) = f.events.try_recv() {
                        match event {
                            Wire::Text(id, text, _) => {
                                if let Ok(v) = serde_json::from_str::<Value>(&text)
                                    && let Some(ids) = v["assets_ids"].as_array()
                                {
                                    if id == old {
                                        old_assets.extend(ids.iter().map(|id| id.as_str().unwrap().to_owned()));
                                        old_count = old_assets.len();
                                    } else {
                                        warm_assets.extend(ids.iter().map(|id| id.as_str().unwrap().to_owned()));
                                        warm_count = warm_assets.len();
                                        warm_socket = Some(id);
                                    }
                                }
                            }
                            Wire::Open(id, at) if id != old => {
                                warm_open.get_or_insert(at);
                            }
                            Wire::Closed(id) => {
                                if warm_count < 30_000 {
                                    premature_close = true;
                                }
                                if id == old {
                                    promoted = true;
                                }
                            }
                            _ => {}
                        }
                    }
                    while f.input.try_recv().is_ok() {}
                    if old_count == 30_000 && deferred_at.is_none() {
                        *f.dial.lock().await = Instant::now() + Duration::from_secs(40);
                        deferred_at = Some(Instant::now());
                    }
                    if warm_count == 30_000 && dump_at.is_none() {
                        // Initial responses can trail a full-universe ramp by
                        // more than a minute even while transport remains live.
                        dump_at = Some(Instant::now() + Duration::from_secs(75));
                    }
                    if !dumped && dump_at.is_some_and(|at| Instant::now() >= at) {
                        let books = (0..30_000)
                            .map(|n| {
                                serde_json::from_str::<Value>(&book(&format!("asset-{n:05}"), 1000))
                                    .unwrap()
                            })
                            .collect::<Vec<_>>();
                        dump_elapsed = Some(origin.elapsed().as_secs_f64());
                        f.send(warm_socket.unwrap(), json!(books).to_string());
                        // Drain the multi-megabyte TCP frame at this virtual
                        // instant, instead of charging each read-buffer turn as
                        // another 100ms of simulated exchange latency.
                        for _ in 0..100_000 {
                            let mut seen = false;
                            while let Ok(input) = f.input.try_recv() {
                                if let Input::Events { events, .. } = input {
                                    seen |= events.len() == 30_000;
                                }
                            }
                            if seen {
                                saw_snapshot = true;
                                break;
                            }
                            tokio::task::yield_now().await;
                        }
                        dumped = true;
                    }
                    if promoted {
                        break;
                    }
                }
                tokio::time::resume();
                if !promoted { eprintln!("scale timing: warm={:?} dump={dump_elapsed:?} seen={saw_snapshot} count={warm_count}", warm_open.map(|at| at.duration_since(origin).as_secs_f64())); }
                assert!(
                    deferred_at.is_some(),
                    "initial 30k subscription did not complete"
                );
                assert!(
                    warm_open.is_some_and(
                        |at| at.duration_since(deferred_at.unwrap()) >= Duration::from_secs(40)
                    ),
                    "warm dial did not exercise the global pacer wait"
                );
                assert!(dumped, "test never delivered delayed warm snapshots");
                assert!(
                    !premature_close,
                    "warm deadline expired before paced subscription ramp completed"
                );
                assert_eq!(warm_count, 30_000);
                assert!(promoted, "fully warmed socket never replaced predecessor");
                assert_eq!(f.ids.load(Ordering::SeqCst), 12);
            })
        },
    )
    .await;
}

#[tokio::test]
async fn repeated_warm_failures_back_off_exponentially() {
    fixture(
        Behavior {
            hold_after: Some(1),
            ..Behavior::default()
        },
        |f| {
            Box::pin(async move {
                let mut config = f.config();
                config.rotate_secs = 1;
                f.start(config, set(&["a"]), 64);
                let (old, _, _) = f.subscription().await;
                f.frame().await;
                let (warm, _, first) = f.subscription().await;
                let mut attempts = vec![first];
                f.commands.send(Command::Close(warm)).unwrap();
                tokio::time::pause();
                let mut old_closed = false;
                for _ in 0..250 {
                    tokio::time::advance(Duration::from_secs(1)).await;
                    for _ in 0..100 {
                        tokio::task::yield_now().await;
                    }
                    while let Ok(event) = f.events.try_recv() {
                        match event {
                            Wire::Text(id, text, at) if id != old => {
                                if let Ok(v) = serde_json::from_str::<Value>(&text)
                                    && v.get("assets_ids").is_some()
                                {
                                    attempts.push(at);
                                    let _ = f.commands.send(Command::Close(id));
                                }
                            }
                            Wire::Closed(id) if id == old => old_closed = true,
                            _ => {}
                        }
                    }
                    while f.input.try_recv().is_ok() {}
                }
                tokio::time::resume();
                assert!(!old_closed);
                assert_eq!(
                    attempts.len(),
                    4,
                    "failed rotations retained a fixed minute retry loop"
                );
                for (pair, minimum) in attempts.windows(2).zip([30, 60, 120]) {
                    assert!(pair[1].duration_since(pair[0]) >= Duration::from_secs(minimum));
                }
            })
        },
    )
    .await;
}

#[tokio::test]
async fn queued_pong_survives_intentional_output_backpressure() {
    fixture(
        Behavior {
            no_pong: true,
            ..Behavior::default()
        },
        |f| {
            Box::pin(async move {
                f.start(f.config(), set(&["a"]), 1);
                let (socket, _, _) = f.subscription().await;
                // Snapshot occupies the sole output slot. Start the application heartbeat
                // before blocking the source behind its supervisor's bounded notice queue.
                tokio::time::pause();
                tokio::time::advance(Duration::from_secs(10)).await;
                loop {
                    if let Some(Wire::Text(_, text, _)) = f.events.recv().await
                        && text == "PING"
                    {
                        break;
                    }
                }
                for n in 1001..1020 {
                    f.send(socket, book("a", n));
                }
                for _ in 0..100 {
                    tokio::task::yield_now().await;
                }
                f.send(socket, "PONG".into());
                tokio::time::advance(Duration::from_secs(25)).await;
                // Drain without timeout futures, which can auto-advance the paused clock.
                let mut disconnected = false;
                for _ in 0..500 {
                    while let Ok(input) = f.input.try_recv() {
                        disconnected |= matches!(input, Input::Disconnected { .. });
                    }
                    tokio::task::yield_now().await;
                }
                tokio::time::resume();
                assert!(
                    !disconnected,
                    "queued PONG lost to time spent intentionally backpressured"
                );
                f.send(socket, book("a", 1020));
                f.frame().await;
                assert_eq!(f.ids.load(Ordering::SeqCst), 11);
            })
        },
    )
    .await;
}

#[tokio::test]
async fn market_data_keeps_connection_alive_while_application_pong_is_delayed() {
    fixture(
        Behavior {
            no_pong: true,
            ..Behavior::default()
        },
        |f| {
            Box::pin(async move {
                f.start(f.config(), set(&["a"]), 32);
                let (socket, _, _) = f.subscription().await;
                let (source, _) = f.frame().await;
                tokio::time::pause();
                for n in 0..8 {
                    tokio::time::advance(Duration::from_secs(5)).await;
                    f.send(socket, book("a", 1001 + n));
                    let (s, _) = f.frame().await;
                    assert_eq!(s, source);
                }
                tokio::time::resume();
                assert_eq!(f.ids.load(Ordering::SeqCst), 11);
            })
        },
    )
    .await;
}

#[tokio::test]
async fn an_older_full_dump_cannot_discard_a_newer_maintained_book() {
    fixture(
        Behavior {
            hold_after: Some(0),
            ..Behavior::default()
        },
        |f| {
            Box::pin(async move {
                f.start(f.config(), set(&["a"]), 32);
                let (socket, _, _) = f.subscription().await;
                f.send(socket, book("a", 1000));
                let (source, _) = f.frame().await;
                f.send(socket, delta("a", 1100, "2").to_string());
                f.frame().await;
                f.send(socket, book("a", 1099));
                let (s, events) = f.frame().await;
                assert_eq!(s, source);
                assert!(events.is_empty());
                f.send(socket, delta("a", 1101, "3").to_string());
                let (s, events) = f.frame().await;
                assert_eq!(s, source);
                assert_eq!(events.len(), 1);
                assert_eq!(f.ids.load(Ordering::SeqCst), 11);
            })
        },
    )
    .await;
}

#[tokio::test]
async fn shared_warm_limit_holds_other_shards_until_a_replacement_finishes() {
    fixture(
        Behavior {
            hold_after: Some(2),
            ..Behavior::default()
        },
        |f| {
            Box::pin(async move {
                let mut config = f.config();
                config.rotate_secs = 1;
                let (send_a, rx_a) =
                    watch::channel(Arc::new([orderbook_collector::assets::intern("a")].into()));
                let (send_b, rx_b) =
                    watch::channel(Arc::new([orderbook_collector::assets::intern("b")].into()));
                let (tx, rx) = mpsc::channel(64);
                f.input = rx;
                let limit = Arc::new(tokio::sync::Semaphore::new(1));
                let held = limit.clone().acquire_owned().await.unwrap();
                let stop = f.run_stop.clone();
                let dial = f.dial.clone();
                let ids = f.ids.clone();
                let c = config.clone();
                let budget = Arc::new(orderbook_collector::connection::SubscriptionBudget::new());
                f.run = Some(tokio::spawn(async move {
                    let _generations = (send_a, send_b);
                    let (a, b) = tokio::join!(
                        orderbook_collector::connection::run_shard_shared_limited(
                            c,
                            rx_a,
                            tx.clone(),
                            stop.clone(),
                            dial.clone(),
                            ids.clone(),
                            limit.clone(),
                            budget.clone()
                        ),
                        orderbook_collector::connection::run_shard_shared_limited(
                            config, rx_b, tx, stop, dial, ids, limit, budget
                        )
                    );
                    a?;
                    b
                }));
                f.subscription().await;
                f.subscription().await;
                f.frame().await;
                f.frame().await;
                tokio::time::sleep(Duration::from_millis(1300)).await;
                assert_eq!(f.ids.load(Ordering::SeqCst), 12);
                drop(held);
                let (warm, request, _) = f.subscription().await;
                tokio::time::sleep(Duration::from_millis(400)).await;
                assert_eq!(
                    f.ids.load(Ordering::SeqCst),
                    13,
                    "global warming limit exceeded"
                );
                let asset = request["assets_ids"][0].as_str().unwrap();
                f.send(warm, book(asset, 1000));
                let (other, _, _) = f.subscription().await;
                assert_ne!(other, warm);
                assert_eq!(f.ids.load(Ordering::SeqCst), 14);
            })
        },
    )
    .await;
}

#[tokio::test]
async fn prebook_deltas_do_not_poison_first_snapshot_or_array_order() {
    fixture(Behavior { hold_after: Some(0), ..Behavior::default() }, |f| Box::pin(async move {
        f.start(f.config(), set(&["a", "b"]), 32);
        let (socket, _, _) = f.subscription().await;
        f.send(socket, delta("a", 1005, "9").to_string());
        assert!(f.frame().await.1.is_empty(), "pre-book delta reached engine");
        // Pre-book changes replay after the snapshot in stable timestamp order.
        f.send(socket, json!([
            delta("a", 999, "99"),
            delta("a", 1004, "8"),
            serde_json::from_str::<Value>(&book("a", 1000)).unwrap(),
            delta("a", 1001, "2"),
            delta("b", 1009, "9"),
            delta("b", 1000, "4"),
            serde_json::from_str::<Value>(&book("b", 1000)).unwrap(),
            delta("b", 1000, "3")
        ]).to_string());
        let (_, events) = f.frame().await;
        assert_eq!(events.len(), 8, "pre-book deltas were discarded instead of replayed");
        assert!(matches!(&events[0], orderbook_collector::adapter::Event::Snapshot { symbol, timestamp_ms: 1000, .. } if symbol == "a"));
        assert!(matches!(&events[1], orderbook_collector::adapter::Event::Snapshot { symbol, timestamp_ms: 1000, .. } if symbol == "b"));
        let a: Vec<_> = events.iter().filter_map(|event| match event {
            orderbook_collector::adapter::Event::Delta { symbol, timestamp_ms, quantity, .. } if symbol == "a" => Some((*timestamp_ms, *quantity)),
            _ => None,
        }).collect();
        assert_eq!(a, vec![(1001, 2_000_000), (1004, 8_000_000), (1005, 9_000_000)]);
        let b: Vec<_> = events.iter().filter_map(|event| match event {
            orderbook_collector::adapter::Event::Delta { symbol, timestamp_ms, quantity, .. } if symbol == "b" => Some((*timestamp_ms, *quantity)),
            _ => None,
        }).collect();
        assert_eq!(b, vec![(1000, 4_000_000), (1000, 3_000_000), (1009, 9_000_000)]);
        // Whole-frame validation must still reject a malformed later member.
        f.send(socket, format!("[{},{{\"event_type\":\"book\"}}]", book("a", 2000)));
        loop {
            match within(f.input.recv()).await.unwrap() {
                Input::Disconnected { .. } => break,
                Input::Events { .. } | Input::Reset { .. } => panic!("malformed array leaked partial effects"),
                _ => {}
            }
        }
    })).await;
}

#[tokio::test]
async fn reconnected_active_gets_rotation_interval_after_its_new_ramp() {
    fixture(Behavior::default(), |f| {
        Box::pin(async move {
            let mut config = f.config();
            config.rotate_secs = 10;
            f.start(config, set(&["a"]), 64);
            let (old, _, _) = f.subscription().await;
            f.frame().await;
            tokio::time::pause();
            tokio::time::advance(Duration::from_secs(9)).await;
            f.commands.send(Command::Close(old)).unwrap();
            for _ in 0..1000 {
                tokio::task::yield_now().await;
            }
            // Force the reconnect handshake to wait past the old rotation deadline.
            *f.dial.lock().await = Instant::now() + Duration::from_secs(15);
            let mut replacement = None;
            for _ in 0..180 {
                tokio::time::advance(Duration::from_millis(100)).await;
                for _ in 0..100 {
                    tokio::task::yield_now().await;
                }
                while let Ok(event) = f.events.try_recv() {
                    if let Wire::Text(id, text, _) = event {
                        if text == "PING" {
                            f.send(id, "PONG".into());
                        }
                        if text.contains("assets_ids") {
                            replacement = Some(id);
                        }
                    }
                }
                while f.input.try_recv().is_ok() {}
                if replacement.is_some() {
                    break;
                }
            }
            assert!(replacement.is_some(), "reconnect never completed");
            for _ in 0..50 {
                tokio::time::advance(Duration::from_millis(100)).await;
                for _ in 0..100 {
                    tokio::task::yield_now().await;
                }
                while let Ok(event) = f.events.try_recv() {
                    assert!(
                        !matches!(event, Wire::Open(_, _)),
                        "fresh reconnect immediately opened a warm third ramp"
                    );
                    if let Wire::Text(id, text, _) = event
                        && text == "PING"
                    {
                        f.send(id, "PONG".into());
                    }
                }
                while f.input.try_recv().is_ok() {}
            }
            tokio::time::resume();
            assert_eq!(f.ids.load(Ordering::SeqCst), 12);
        })
    })
    .await;
}

#[tokio::test]
async fn quarantined_outgoing_asset_does_not_wedge_warm_promotion() {
    fixture(Behavior { hold_after: Some(1), ..Behavior::default() }, |f| Box::pin(async move {
        let mut config = f.config(); config.rotate_secs = 1;
        f.start(config, set(&["a", "b"]), 64);
        let (old, _, _) = f.subscription().await; let (source, _) = f.frame().await;
        let (warm, _, _) = f.subscription().await;
        f.send(old, delta("a", 1002, "2").to_string()); f.frame().await;
        f.send(old, delta("a", 1001, "9").to_string());
        assert!(matches!(within(f.input.recv()).await, Some(Input::Reset { source: s, .. }) if s == source));
        f.frame().await;
        f.send(warm, book("b", 1000)); f.frame().await;
        loop {
            if let Wire::Closed(id) = f.event().await { assert_eq!(id, old); break; }
        }
        assert_eq!(f.ids.load(Ordering::SeqCst), 12);
    })).await;
}

#[tokio::test]
async fn pending_application_pongs_pause_subscriptions_but_keep_reading_and_pinging() {
    fixture(
        Behavior {
            no_pong: true,
            ..Behavior::default()
        },
        |f| {
            Box::pin(async move {
                f.start(f.config(), set(&["a"]), 64);
                let (socket, _, _) = f.subscription().await;
                let (source, _) = f.frame().await;
                tokio::time::pause();
                tokio::time::advance(Duration::from_secs(10)).await;
                loop {
                    if let Some(Wire::Text(_, text, _)) = f.events.recv().await
                        && text == "PING"
                    {
                        break;
                    }
                }
                let mut assets = set(&["a"]);
                assets.extend((0..201).map(|n| format!("new-{n:03}")));
                f.assets.send_replace(assets);
                f.commands.send(Command::Ping(socket)).unwrap();
                // Data keeps transport live, but cannot acknowledge application PINGs.
                let mut pings = 1;
                let mut control_pong = false;
                for n in 0..12 {
                    tokio::time::advance(Duration::from_secs(1)).await;
                    f.send(socket, book("a", 1001 + n));
                    assert_eq!(
                        f.frame().await.0,
                        source,
                        "budget wait stalled socket reads"
                    );
                    for _ in 0..100 {
                        tokio::task::yield_now().await;
                    }
                    while let Ok(event) = f.events.try_recv() {
                        match event {
                            Wire::Text(_, text, _) if text == "PING" => pings += 1,
                            Wire::Text(_, text, _) if text == "CONTROL_PONG" => control_pong = true,
                            Wire::Text(_, text, _) => assert!(
                                !text.contains("assets_ids"),
                                "new subscriptions escaped pending PONG gate"
                            ),
                            Wire::Closed(_) => {
                                panic!("delayed application PONG closed live transport")
                            }
                            _ => {}
                        }
                    }
                }
                assert_eq!(
                    pings, 2,
                    "heartbeats stalled while subscription budget was held"
                );
                assert!(control_pong, "control PING response stalled");
                // PONG has no sequence/request ID. One observed reply must
                // restore progress even if an earlier reply was omitted.
                f.send(socket, "PONG".into());
                let mut added = 0;
                let mut previous = None;
                for _ in 0..50 {
                    tokio::time::advance(Duration::from_millis(100)).await;
                    for _ in 0..100 {
                        tokio::task::yield_now().await;
                    }
                    while let Ok(event) = f.events.try_recv() {
                        if let Wire::Text(_, text, at) = event
                            && let Ok(value) = serde_json::from_str::<Value>(&text)
                        {
                            if let Some(last) = previous {
                                assert!(
                                    at.duration_since(last) >= Duration::from_secs(1),
                                    "PONG release accumulated burst credits"
                                );
                            }
                            previous = Some(at);
                            added += value["assets_ids"].as_array().unwrap().len();
                        }
                    }
                    while f.input.try_recv().is_ok() {}
                    if added == 201 {
                        break;
                    }
                }
                tokio::time::resume();
                assert_eq!(
                    added, 201,
                    "application PONGs did not resume paced subscriptions"
                );
                assert_eq!(f.ids.load(Ordering::SeqCst), 11);
            })
        },
    )
    .await;
}

#[tokio::test]
async fn shared_subscription_budget_paces_two_shards_without_stalling_readers() {
    two_shard_budget_fixture(false).await;
}
#[tokio::test]
async fn busy_backpressured_shard_keeps_subscription_progress() {
    two_shard_budget_fixture(true).await;
}
async fn two_shard_budget_fixture(backpressured: bool) {
    fixture(Behavior::default(), move |f| {
        Box::pin(async move {
            use orderbook_collector::{
                assets::intern,
                connection::{SubscriptionBudget, run_shard_shared_limited},
            };
            let (a, rx_a) = watch::channel(Arc::new([intern("a")].into()));
            let (b, rx_b) = watch::channel(Arc::new([intern("b")].into()));
            let (tx, rx) = mpsc::channel(if backpressured { 1 } else { 64 });
            f.input = rx;
            let stop = f.run_stop.clone();
            let dial = f.dial.clone();
            let ids = f.ids.clone();
            let config = f.config();
            let other_config = config.clone();
            let budget = Arc::new(SubscriptionBudget::new());
            let warm = Arc::new(tokio::sync::Semaphore::new(4));
            f.run = Some(tokio::spawn(async move {
                let (left, right) = tokio::join!(
                    run_shard_shared_limited(
                        config,
                        rx_a,
                        tx.clone(),
                        stop.clone(),
                        dial.clone(),
                        ids.clone(),
                        warm.clone(),
                        budget.clone()
                    ),
                    run_shard_shared_limited(other_config, rx_b, tx, stop, dial, ids, warm, budget)
                );
                left?;
                right
            }));
            let (socket_a, _, _) = f.subscription().await;
            let (socket_b, _, _) = f.subscription().await;
            f.frame().await;
            f.frame().await;
            // Both sockets have finished their local cadence before simultaneous
            // watch updates; independent limiters would send at the same instant.
            tokio::time::pause();
            tokio::time::advance(Duration::from_millis(1100)).await;
            let mut assets_a: BTreeSet<_> =
                (0..201).map(|n| intern(&format!("a-{n:03}"))).collect();
            assets_a.insert(intern("a"));
            let mut assets_b: BTreeSet<_> =
                (0..201).map(|n| intern(&format!("b-{n:03}"))).collect();
            assets_b.insert(intern("b"));
            a.send_replace(Arc::new(assets_a));
            b.send_replace(Arc::new(assets_b));
            f.commands.send(Command::Ping(socket_a)).unwrap();
            f.commands.send(Command::Ping(socket_b)).unwrap();
            let mut requests = Vec::new();
            let mut snapshots = 0;
            let mut controls = 0;
            for step in 0..700 {
                if backpressured && step % 100 == 0 {
                    // Overflow the eight-frame mailbox while the engine has one
                    // output slot; completed reads must park, retaining seniority.
                    for n in 0..20 {
                        f.send(socket_a, book("a", 2000 + step * 20 + n));
                    }
                }
                tokio::time::advance(Duration::from_millis(5)).await;
                for _ in 0..100 {
                    tokio::task::yield_now().await;
                }
                while let Ok(event) = f.events.try_recv() {
                    match event {
                        Wire::Text(_, text, _) if text == "CONTROL_PONG" => controls += 1,
                        Wire::Text(id, text, at) => {
                            if let Ok(value) = serde_json::from_str::<Value>(&text) {
                                let count = value["assets_ids"].as_array().unwrap().len();
                                assert!(count <= 100);
                                requests.push((id, at, count));
                            }
                        }
                        Wire::Closed(_) => panic!("budget waiting disconnected a socket"),
                        _ => {}
                    }
                }
                while let Ok(input) = f.input.try_recv() {
                    if let Input::Events { events, .. } = input {
                        snapshots += events
                            .iter()
                            .filter(|event| {
                                matches!(event,
                            orderbook_collector::adapter::Event::Snapshot { symbol, .. }
                                if symbol.starts_with("a-") || symbol.starts_with("b-"))
                            })
                            .count();
                    }
                }
                if requests.len() == 6 && snapshots == 402 && controls == 2 {
                    break;
                }
            }
            tokio::time::resume();
            assert_eq!(requests.len(), 6);
            assert_eq!(snapshots, 402);
            assert_eq!(controls, 2);
            for pair in requests.windows(2) {
                assert!(
                    pair[1].1.duration_since(pair[0].1) >= Duration::from_millis(25),
                    "collector-wide subscription burst: {requests:?}"
                );
            }
            for id in [socket_a, socket_b] {
                let times: Vec<_> = requests.iter().filter(|r| r.0 == id).map(|r| r.1).collect();
                assert_eq!(times.len(), 3, "shared budget starved a shard");
                for pair in times.windows(2) {
                    assert!(pair[1].duration_since(pair[0]) >= Duration::from_secs(1));
                }
            }
            assert_eq!(f.ids.load(Ordering::SeqCst), 12);
        })
    })
    .await;
}

#[tokio::test]
async fn removed_outgoing_asset_does_not_gate_warm_promotion() {
    fixture(
        Behavior {
            hold_after: Some(1),
            ..Behavior::default()
        },
        |f| {
            Box::pin(async move {
                let mut config = f.config();
                config.rotate_secs = 1;
                f.start(config, set(&["a", "b"]), 64);
                let (old, _, _) = f.subscription().await;
                f.frame().await;
                let (warm, _, _) = f.subscription().await;
                f.assets.send_replace(set(&["b"]));
                f.send(warm, book("b", 1000));
                f.frame().await;
                loop {
                    if let Wire::Closed(id) = f.event().await {
                        assert_eq!(id, old);
                        break;
                    }
                }
                assert_eq!(f.ids.load(Ordering::SeqCst), 12);
            })
        },
    )
    .await;
}

#[tokio::test]
async fn a_later_pong_recovers_refresh_when_an_earlier_reply_was_missing() {
    fixture(
        Behavior {
            no_pong: true,
            ..Behavior::default()
        },
        |f| {
            Box::pin(async move {
                let mut config = f.config();
                config.stale_secs = 12;
                f.start(config, set(&["a", "b"]), 64);
                let (socket, _, _) = f.subscription().await;
                f.frame().await;
                tokio::time::pause();
                tokio::time::advance(Duration::from_secs(10)).await;
                loop {
                    if let Some(Wire::Text(_, text, _)) = f.events.recv().await
                        && text == "PING"
                    {
                        break;
                    }
                }
                let mut pings = 1;
                for n in 0..12 {
                    tokio::time::advance(Duration::from_secs(1)).await;
                    // b keeps the socket live; a independently becomes stale.
                    f.send(socket, book("b", 1001 + n));
                    // Keep the virtual clock stationary while real TCP delivers
                    // this update; a timeout future could auto-advance it.
                    let mut observed = false;
                    for _ in 0..10_000 {
                        tokio::task::yield_now().await;
                        while let Ok(input) = f.input.try_recv() {
                            assert!(!matches!(input, Input::Reset { .. }), "refresh reset a healthy book while waiting for PONG");
                            if let Input::Events { events, .. } = input {
                                observed |= events.iter().any(|event| matches!(event,
                                    orderbook_collector::adapter::Event::Snapshot { symbol, .. } if symbol == "b"));
                            }
                        }
                        if observed { break; }
                    }
                    assert!(observed, "fresh b update never reached the coordinator");
                    for _ in 0..100 {
                        tokio::task::yield_now().await;
                    }
                    while let Ok(event) = f.events.try_recv() {
                        if let Wire::Text(_, text, _) = event {
                            if text == "PING" {
                                pings += 1;
                            } else {
                                assert!(
                                    !text.contains("assets_ids"),
                                    "refresh escaped application PONG backlog"
                                );
                            }
                        }
                    }
                }
                assert_eq!(pings, 2);
                f.send(socket, delta("a", 1005, "7").to_string());
                spin_until(|| match f.input.try_recv() {
                    Ok(Input::Events { events, .. }) => events.iter().any(|event| matches!(event,
                        orderbook_collector::adapter::Event::Delta { symbol, quantity: 7_000_000, .. } if symbol == "a")),
                    Ok(Input::Reset { .. }) => panic!("waiting refresh invalidated healthy a"),
                    _ => false,
                }).await;
                f.send(socket, "PONG".into());
                let mut requests = Vec::new();
                for _ in 0..40 {
                    tokio::time::advance(Duration::from_millis(100)).await;
                    for _ in 0..100 {
                        tokio::task::yield_now().await;
                    }
                    while let Ok(event) = f.events.try_recv() {
                        if let Wire::Text(id, text, _) = event
                            && let Ok(value) = serde_json::from_str::<Value>(&text)
                        {
                            assert_eq!(id, socket);
                            assert_eq!(value["assets_ids"], json!(["a"]));
                            requests.push(value);
                        }
                    }
                    while let Ok(input) = f.input.try_recv() {
                        assert!(!matches!(
                            input,
                            Input::Disconnected { .. }
                        ));
                    }
                    if requests.len() == 2 {
                        break;
                    }
                }
                tokio::time::resume();
                assert_eq!(
                    requests.len(),
                    2,
                    "answered PONG backlog did not resume refresh"
                );
                assert_eq!(requests[0]["operation"], "unsubscribe");
                assert_eq!(requests[1]["operation"], "subscribe");
                assert_eq!(f.ids.load(Ordering::SeqCst), 11);
            })
        },
    )
    .await;
}

#[tokio::test]
async fn peer_policy_denial_stops_all_sources_without_retries() {
    fixture(Behavior::default(), |f| {
        Box::pin(async move {
            let mut config = f.config();
            config.replicas = 2;
            f.start(config, set(&["a"]), 32);
            let (first, _, _) = f.subscription().await;
            f.frame().await;
            f.subscription().await;
            f.frame().await;
            f.commands.send(Command::PolicyClose(first)).unwrap();
            let result = f.finish().await;
            assert!(result.is_err());
            assert!(format!("{:#}", result.unwrap_err()).contains("policy denial"));
            tokio::time::sleep(Duration::from_millis(600)).await;
            assert_eq!(
                f.ids.load(Ordering::SeqCst),
                12,
                "policy denial was retried"
            );
        })
    })
    .await;
}

#[tokio::test]
async fn missing_all_application_pongs_cannot_hide_an_unfinished_subscription_forever() {
    fixture(
        Behavior {
            no_pong: true,
            ..Behavior::default()
        },
        |f| {
            Box::pin(async move {
                f.start(f.config(), set(&["a"]), 64);
                let (socket, _, _) = f.subscription().await;
                let (source, _) = f.frame().await;
                tokio::time::pause();
                tokio::time::advance(Duration::from_secs(10)).await;
                loop {
                    if let Some(Wire::Text(_, text, _)) = f.events.recv().await
                        && text == "PING"
                    {
                        break;
                    }
                }
                f.assets.send_replace(set(&["a", "unsubscribed-new-asset"]));
                let mut disconnected = false;
                for n in 0..220 {
                    tokio::time::advance(Duration::from_secs(1)).await;
                    f.send(socket, book("a", 1001 + n));
                    for _ in 0..100 {
                        tokio::task::yield_now().await;
                    }
                    while let Ok(input) = f.input.try_recv() {
                        if matches!(input,Input::Disconnected{source:s} if s==source) {
                            disconnected = true;
                        }
                    }
                    if disconnected {
                        break;
                    }
                }
                tokio::time::resume();
                assert!(
                    disconnected,
                    "market activity concealed permanently stalled new subscriptions"
                );
            })
        },
    )
    .await;
}

// Virtual time never waits on real I/O: yielding keeps the runtime runnable and
// prevents Tokio's idle auto-advance from consuming a deadline under assertion.
async fn spin_until(mut predicate: impl FnMut() -> bool) {
    for _ in 0..50_000 {
        if predicate() {
            return;
        }
        tokio::task::yield_now().await;
    }
    assert!(predicate(), "stationary-clock I/O did not complete");
}

#[tokio::test]
async fn idle_then_new_work_gets_its_own_progress_deadline() {
    fixture(
        Behavior {
            no_pong: true,
            ..Behavior::default()
        },
        |f| {
            Box::pin(async move {
                let mut config = f.config();
                config.stale_secs = 3600;
                f.start(config, set(&["a"]), 64);
                let (socket, _, _) = f.subscription().await;
                let (source, _) = f.frame().await;
                tokio::time::pause();
                // Long idle with live market data and unanswered application PINGs.
                for n in 0..40 {
                    tokio::time::advance(Duration::from_secs(10)).await;
                    f.send(socket, book("a", 1001 + n));
                    spin_until(|| matches!(f.input.try_recv(), Ok(Input::Events { .. }))).await;
                }
                f.assets.send_replace(set(&["a", "new"]));
                for _ in 0..100 {
                    tokio::task::yield_now().await;
                }
                tokio::time::advance(Duration::from_secs(1)).await;
                f.send(socket, book("a", 2000));
                spin_until(|| matches!(f.input.try_recv(), Ok(Input::Events { .. }))).await;
                assert_eq!(
                    f.ids.load(Ordering::SeqCst),
                    11,
                    "old idle timestamp reset a new lifecycle"
                );
                let mut disconnected = false;
                for n in 0..185 {
                    tokio::time::advance(Duration::from_secs(1)).await;
                    f.send(socket, book("a", 2001 + n));
                    for _ in 0..100 {
                        tokio::task::yield_now().await;
                    }
                    while let Ok(input) = f.input.try_recv() {
                        disconnected |=
                            matches!(input, Input::Disconnected { source: s } if s == source);
                    }
                    assert!(
                        n >= 178 || !disconnected,
                        "new lifecycle inherited an expired idle timer"
                    );
                    if disconnected {
                        break;
                    }
                }
                tokio::time::resume();
                assert!(
                    disconnected,
                    "new work never reached its own bounded deadline"
                );
            })
        },
    )
    .await;
}

#[tokio::test]
async fn refresh_resets_before_unsubscribe_and_restores_across_unanswered_ping() {
    fixture(Behavior { no_pong: true, hold_after: Some(0), ..Behavior::default() }, |f| Box::pin(async move {
        let mut config = f.config(); config.stale_secs = 9;
        f.start(config, set(&["a", "b"]), 64);
        let (socket, _, _) = f.subscription().await;
        f.send(socket, json!([serde_json::from_str::<Value>(&book("a",1000)).unwrap(), serde_json::from_str::<Value>(&book("b",1000)).unwrap()]).to_string());
        let (source, _) = f.frame().await;
        tokio::time::pause();
        tokio::time::advance(Duration::from_millis(100)).await;
        // Keep b fresh while a reaches maintenance immediately before PING.
        let mut reset = false;
        for n in 1..=9 {
            tokio::time::advance(Duration::from_secs(1)).await;
            f.send(socket, book("b",1000+n));
            spin_until(|| match f.input.try_recv() {
                Ok(Input::Reset { source:s, symbols }) => { assert_eq!(s,source); assert_eq!(symbols,set(&["a"])); reset=true; false },
                Ok(Input::Events { .. }) => true,
                _ => false,
            }).await;
        }
        let mut requests = Vec::new();
        spin_until(|| {
            while let Ok(Wire::Text(_, text, _)) = f.events.try_recv() {
                if let Ok(v) = serde_json::from_str::<Value>(&text) { requests.push(v); }
            }
            !requests.is_empty()
        }).await;
        assert_eq!(requests[0]["operation"], "unsubscribe");
        while let Ok(input) = f.input.try_recv() {
            if let Input::Reset { source:s, symbols } = input { assert_eq!(s,source); assert_eq!(symbols,set(&["a"])); reset=true; }
        }
        assert!(reset, "refresh unsubscribe left a stale valid book");
        // A pre-snapshot delta has no base after refresh invalidation.
        f.send(socket, delta("a",1001,"9").to_string());
        let mut observed = false;
        spin_until(|| {
            if let Ok(Input::Events { events, .. }) = f.input.try_recv() { assert!(events.is_empty()); observed=true; }
            observed
        }).await;
        // At ten seconds PING wins before the restore's 1s socket cadence.
        tokio::time::advance(Duration::from_millis(1002)).await;
        let mut ping = false;
        spin_until(|| {
            while let Ok(Wire::Text(_, text, _)) = f.events.try_recv() {
                if text=="PING" { ping=true; }
                if let Ok(v)=serde_json::from_str::<Value>(&text) {
                    assert!(ping, "restore did not exercise PING between refresh halves");
                    requests.push(v);
                }
            }
            ping && requests.len()==2
        }).await;
        assert_eq!(requests[1]["operation"],"subscribe", "PONG barrier stranded an unsubscribe");
        f.send(socket, delta("a",1007,"7").to_string());
        spin_until(|| matches!(f.input.try_recv(), Ok(Input::Events { events, .. }) if events.is_empty())).await;
        f.send(socket, book("a",1002));
        spin_until(|| match f.input.try_recv() {
            Ok(Input::Events { events, .. }) => {
                assert_eq!(events.len(),2, "refresh lost a newer pre-book delta or retained a half-refresh delta");
                assert!(matches!(&events[0], orderbook_collector::adapter::Event::Snapshot { timestamp_ms:1002, .. }));
                assert!(matches!(&events[1], orderbook_collector::adapter::Event::Delta { timestamp_ms:1007, quantity:7_000_000, .. }));
                true
            },
            _ => false,
        }).await;
        f.send(socket, delta("a",1008,"2").to_string());
        spin_until(|| matches!(f.input.try_recv(), Ok(Input::Events { events, .. }) if events.len()==1)).await;
        tokio::time::resume();
        assert_eq!(f.ids.load(Ordering::SeqCst),11);
    })).await;
}

#[tokio::test]
async fn warm_uses_frozen_target_and_engine_blocks_later_rollback() {
    fixture(
        Behavior {
            hold_after: Some(1),
            ..Behavior::default()
        },
        |f| {
            Box::pin(async move {
                use orderbook_collector::engine::Engine;
                let mut engine = Engine::new(64, 300_000);
                engine.set_universe(set(&["a"]));
                let mut config = f.config();
                config.rotate_secs = 1;
                f.start(config, set(&["a"]), 64);
                let (old, _, _) = f.subscription().await;
                let (source, events) = f.frame().await;
                for e in events {
                    engine.apply(source, e, 1000).unwrap();
                }
                let (warm, _, _) = f.subscription().await;
                // Ramp completion freezes the predecessor at 1000, then it advances.
                f.send(old, book("a", 2000));
                let (_, events) = f.frame().await;
                for e in events {
                    engine.apply(source, e, 2000).unwrap();
                }
                f.send(warm, book("a", 1000));
                let (replacement, events) = f.frame().await;
                for e in events {
                    assert!(engine.apply(replacement, e, 2001).unwrap().is_empty());
                }
                loop {
                    if let Wire::Closed(id) = f.event().await {
                        assert_eq!(id, old);
                        break;
                    }
                }
                assert!(
                    engine.disconnect(source, 2002).is_empty(),
                    "engine emitted rollback on transport promotion"
                );
                assert!(
                    engine.snapshots(2003).is_empty(),
                    "behind-watermark book emitted"
                );
                f.send(warm, book("a", 2001));
                let (_, events) = f.frame().await;
                for e in events {
                    engine.apply(replacement, e, 2004).unwrap();
                }
                assert_eq!(engine.snapshots(2005).len(), 1);
            })
        },
    )
    .await;
}

#[tokio::test]
async fn late_addition_rearms_warm_target_at_its_new_ramp_completion() {
    fixture(Behavior { hold_after: Some(1), ..Behavior::default() }, |f| Box::pin(async move {
        let mut config=f.config(); config.rotate_secs=1;
        f.start(config,set(&["a"]),64);
        let (old,_,_)=f.subscription().await; let (source,_)=f.frame().await;
        let (warm,_,_)=f.subscription().await;
        f.send(old,book("a",2000)); f.frame().await;
        f.assets.send_replace(set(&["a","b"]));
        for _ in 0..2 {
            let (_,request,_)=f.subscription().await;
            assert_eq!(request["assets_ids"],json!(["b"]));
        }
        f.frame().await; // outgoing b's initial snapshot
        f.send(warm,book("a",1000)); let (replacement,_)=f.frame().await;
        f.send(warm,book("b",1000)); f.frame().await;
        tokio::time::pause();
        tokio::time::advance(Duration::from_millis(1100)).await;
        for _ in 0..100 { tokio::task::yield_now().await; }
        while let Ok(event)=f.events.try_recv() {
            assert!(!matches!(event,Wire::Closed(id) if id==old), "late catalog ramp reused the first ramp's obsolete target");
        }
        f.send(warm,book("a",2000));
        spin_until(|| matches!(f.input.try_recv(), Ok(Input::Events { source:s, .. }) if s==replacement)).await;
        tokio::time::advance(Duration::from_millis(1100)).await;
        spin_until(|| matches!(f.events.try_recv(), Ok(Wire::Closed(id)) if id==old)).await;
        tokio::time::resume();
        assert_ne!(source,replacement);
    })).await;
}

#[tokio::test]
async fn prebook_buffer_overflow_fails_source_without_partial_output() {
    fixture(
        Behavior {
            hold_after: Some(0),
            ..Behavior::default()
        },
        |f| {
            Box::pin(async move {
                f.start(f.config(), set(&["a"]), 32);
                let (socket, _, _) = f.subscription().await;
                let changes: Vec<_> = (0..257).map(|n| delta("a", 1000 + n, "1")).collect();
                f.send(socket, json!(changes).to_string());
                loop {
                    match within(f.input.recv()).await.unwrap() {
                        Input::Disconnected { .. } => break,
                        Input::Events { .. } | Input::Reset { .. } => {
                            panic!("overflowing pre-book buffer silently accepted a partial frame")
                        }
                        _ => {}
                    }
                }
            })
        },
    )
    .await;
}

#[tokio::test]
async fn prebook_global_buffer_overflow_fails_source_across_assets() {
    fixture(
        Behavior {
            hold_after: Some(0),
            ..Behavior::default()
        },
        |f| {
            Box::pin(async move {
                let assets: BTreeSet<_> = (0..64).map(|n| format!("asset-{n}")).collect();
                f.start(f.config(), assets.clone(), 32);
                let (socket, _, _) = f.subscription().await;
                // Each asset is below its individual bound. The aggregate must still
                // be bounded, and overflow must disconnect rather than claim coverage.
                let changes: Vec<_> = assets
                    .iter()
                    .flat_map(|asset| (0..256).map(move |n| delta(asset, 1000 + n, "1")))
                    .collect();
                f.send(socket, json!(changes).to_string());
                loop {
                    match within(f.input.recv()).await.unwrap() {
                        Input::Disconnected { .. } => break,
                        Input::Events { .. } | Input::Reset { .. } => {
                            panic!("global pre-book overflow leaked partial output")
                        }
                        _ => {}
                    }
                }
            })
        },
    )
    .await;
}
