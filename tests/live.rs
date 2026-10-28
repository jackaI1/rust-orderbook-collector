use futures_util::{FutureExt, SinkExt, StreamExt, future::BoxFuture};
use orderbook_collector::live::{self, Config, Input};
use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    future::Future,
    net::SocketAddr,
    panic::AssertUnwindSafe,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::{broadcast, mpsc},
    task::{JoinHandle, JoinSet},
    time::{Instant, timeout},
};
use tokio_tungstenite::{accept_async, tungstenite::Message};
use tokio_util::sync::CancellationToken;

type HttpReply = Arc<dyn Fn(&str, usize) -> (u16, Value) + Send + Sync>;
#[derive(Clone, Default)]
struct Behavior {
    deny: bool,
    deny_after: Option<usize>,
    denial_delay: Duration,
    no_pong: bool,
    oversized: bool,
    omit_initial_book: bool,
}
#[derive(Debug)]
struct Event {
    id: usize,
    at: Instant,
    text: String,
}
#[derive(Debug, Clone)]
enum Command {
    Close(usize),
    Text(usize, String),
    Ping(usize),
}
struct Fixture {
    http: SocketAddr,
    ws: SocketAddr,
    requests: Arc<Mutex<Vec<String>>>,
    events: mpsc::UnboundedReceiver<Event>,
    commands: broadcast::Sender<Command>,
    cancel: CancellationToken,
    tasks: JoinSet<()>,
    live_cancel: CancellationToken,
    live: Option<JoinHandle<anyhow::Result<()>>>,
    inputs: mpsc::Receiver<Input>,
}
fn resource_log(event: &str, http: SocketAddr, ws: SocketAddr) {
    static START: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    let pid = std::process::id();
    let started = START.get_or_init(|| {
        let output = std::process::Command::new("ps")
            .args(["-p", &pid.to_string(), "-o", "lstart="])
            .output()
            .unwrap();
        String::from_utf8_lossy(&output.stdout).trim().to_owned()
    });
    let line = format!(
        "RESOURCE {}\n",
        json!({"owner":"orderbook-local-tests", "event":event, "pid":pid,
        "start_time":started, "ports":[http.port(),ws.port()],
        "cleanup":format!("scoped cancellation + joined tasks + listener rebinding; emergency: kill -INT {pid}")})
    );
    std::io::Write::write_all(&mut std::io::stderr().lock(), line.as_bytes()).unwrap();
}
impl Fixture {
    async fn new(reply: HttpReply, behavior: Behavior) -> Self {
        let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ws = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http_addr = http.local_addr().unwrap();
        let ws_addr = ws.local_addr().unwrap();
        resource_log("bound", http_addr, ws_addr);
        let cancel = CancellationToken::new();
        let mut tasks = JoinSet::new();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let token = cancel.clone();
        let seen = requests.clone();
        tasks.spawn(async move {
            let mut children = JoinSet::new();
            loop { tokio::select! {
                _ = token.cancelled() => break,
                Some(result) = children.join_next() => { result.unwrap(); },
                accepted = http.accept() => {
                    let (mut stream, _) = accepted.unwrap(); let reply = reply.clone(); let seen = seen.clone(); let token = token.clone();
                    children.spawn(async move {
                        let work = async {
                            let mut bytes = Vec::new();
                            while !bytes.ends_with(b"\r\n\r\n") && bytes.len() < 16_384 {
                                let byte = stream.read_u8().await?; bytes.push(byte);
                            }
                            let request = String::from_utf8_lossy(&bytes).into_owned();
                            let index = { let mut requests = seen.lock().unwrap(); let n = requests.len(); requests.push(request.clone()); n };
                            let (status, value) = reply(&request, index); let body = value.to_string();
                            let retry_after=if status==429 {"Retry-After: 2\r\n"}else{""};
                            let response = format!("HTTP/1.1 {status} Fixture\r\nContent-Type: application/json\r\nContent-Length: {}\r\n{retry_after}Connection: close\r\n\r\n{body}", body.len());
                            stream.write_all(response.as_bytes()).await?; stream.shutdown().await
                        };
                        tokio::select! { _ = token.cancelled() => {}, _ = work => {} }
                    });
                }
            }}
            while let Some(result) = children.join_next().await { result.unwrap(); }
        });
        let (event_tx, events) = mpsc::unbounded_channel();
        let (commands, _) = broadcast::channel::<Command>(64);
        let sender = commands.clone();
        let token = cancel.clone();
        tasks.spawn(async move {
            let mut children = JoinSet::new(); let mut next = 0;
            loop { tokio::select! {
                _ = token.cancelled() => break,
                Some(result) = children.join_next() => { result.unwrap(); },
                accepted = ws.accept() => {
                    let (mut stream, _) = accepted.unwrap(); let id = next; next += 1;
                    let events = event_tx.clone(); let token = token.clone(); let behavior = behavior.clone(); let mut commands = sender.subscribe();
                    children.spawn(async move {
                        let work = async {
                            if behavior.deny || behavior.deny_after.is_some_and(|first| id >= first) {
                                let mut bytes = [0; 4096]; let _ = stream.read(&mut bytes).await;
                                tokio::time::sleep(behavior.denial_delay).await;
                                let _ = stream.write_all(b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\n\r\n").await; return;
                            }
                            let Ok(mut socket) = accept_async(stream).await else { return; };
                            loop { tokio::select! {
                                _ = token.cancelled() => { let _ = timeout(Duration::from_millis(100),socket.close(None)).await; break; },
                                command = commands.recv() => match command {
                                    Ok(Command::Close(target)) if target == id => { let _ = socket.close(None).await; break; },
                                    Ok(Command::Text(target,text)) if target == id => { if socket.send(Message::Text(text.into())).await.is_err() { break; } },
                                    Ok(Command::Ping(target)) if target == id && socket.send(Message::Ping(b"fixture".to_vec().into())).await.is_err() => break,
                                    _ => {},
                                },
                                message = socket.next() => match message {
                                    Some(Ok(Message::Text(text))) => {
                                        let text = text.to_string(); let _ = events.send(Event{id,at:Instant::now(),text:text.clone()});
                                        if text == "PING" { if !behavior.no_pong { let _ = socket.send(Message::Text("PONG".into())).await; } continue; }
                                        if let Ok(value) = serde_json::from_str::<Value>(&text)
                                            && value.get("operation").and_then(Value::as_str) != Some("unsubscribe") {
                                                if behavior.oversized { let _ = socket.send(Message::Text("x".repeat(8 * 1024 * 1024 + 1).into())).await; }
                                                else if !behavior.omit_initial_book && let Some(assets) = value["assets_ids"].as_array() {
                                                    let books:Vec<_> = assets.iter().map(|id|json!({"event_type":"book","asset_id":id,"bids":[],"asks":[],"timestamp":"1000"})).collect();
                                                    let _ = socket.send(Message::Text(json!(books).to_string().into())).await;
                                                }
                                        }
                                    },
                                    Some(Ok(Message::Pong(bytes))) => { let _ = events.send(Event{id,at:Instant::now(),text:format!("CONTROL_PONG:{}",String::from_utf8_lossy(&bytes))}); },
                                    Some(Ok(Message::Ping(bytes))) => { let _ = socket.send(Message::Pong(bytes)).await; },
                                    Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break,
                                    _ => {},
                                }
                            }}
                        };
                        tokio::select! { _ = token.cancelled() => {}, _ = work => {} }
                    });
                }
            }}
            while let Some(result) = children.join_next().await { result.unwrap(); }
        });
        let (_, inputs) = mpsc::channel(1);
        Self {
            http: http_addr,
            ws: ws_addr,
            requests,
            events,
            commands,
            cancel,
            tasks,
            live_cancel: CancellationToken::new(),
            live: None,
            inputs,
        }
    }
    fn config(&self) -> Config {
        Config {
            discovery_url: format!("http://{}/markets/keyset", self.http),
            websocket_url: format!("ws://{}/ws/market", self.ws),
            shard_size: 2,
            replicas: 1,
            discovery_secs: 60,
            stale_secs: 300,
            rotate_secs: 600,
            max_symbols: None,
            audit_catalog: false,
        }
    }
    fn start(&mut self, config: Config, capacity: usize) {
        let (tx, rx) = mpsc::channel(capacity);
        self.inputs = rx;
        self.live = Some(tokio::spawn(live::run(
            config,
            tx,
            self.live_cancel.clone(),
        )));
    }
    async fn input(&mut self) -> Input {
        timeout(Duration::from_secs(8), self.inputs.recv())
            .await
            .expect("input timeout")
            .expect("input channel closed")
    }
    async fn event(&mut self) -> Event {
        timeout(Duration::from_secs(8), self.events.recv())
            .await
            .expect("WS timeout")
            .unwrap()
    }
    async fn finish(&mut self) -> anyhow::Result<()> {
        let mut handle = self.live.take().unwrap();
        match timeout(Duration::from_secs(5), &mut handle).await {
            Ok(result) => result.unwrap(),
            Err(_) => {
                handle.abort();
                let _ = handle.await;
                panic!("live failed to join");
            }
        }
    }

    async fn cleanup(&mut self) {
        self.live_cancel.cancel();
        let mut failures = Vec::new();
        if let Some(mut handle) = self.live.take()
            && timeout(Duration::from_secs(5), &mut handle).await.is_err()
        {
            handle.abort();
            let _ = handle.await;
            failures.push("live task required abort");
        }
        self.cancel.cancel();
        loop {
            match timeout(Duration::from_secs(5), self.tasks.join_next()).await {
                Ok(Some(Ok(()))) => {}
                Ok(Some(Err(_))) => failures.push("fixture task panicked"),
                Ok(None) => break,
                Err(_) => {
                    self.tasks.abort_all();
                    while self.tasks.join_next().await.is_some() {}
                    failures.push("fixture tasks required abort");
                    break;
                }
            }
        }
        // Socket rebinding proves the scoped listeners were released, including
        // assertion failure paths. No fixture process is detached.
        drop(
            TcpListener::bind(self.http)
                .await
                .expect("HTTP fixture leaked"),
        );
        drop(TcpListener::bind(self.ws).await.expect("WS fixture leaked"));
        assert!(failures.is_empty(), "cleanup failed: {failures:?}");
        resource_log("joined_ports_rebound_and_closed", self.http, self.ws);
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.live_cancel.cancel();
        self.cancel.cancel();
        if let Some(handle) = &self.live {
            handle.abort();
        }
    }
}
async fn fixture(
    reply: HttpReply,
    behavior: Behavior,
    test: impl for<'a> FnOnce(&'a mut Fixture) -> BoxFuture<'a, ()>,
) {
    let mut f = Fixture::new(reply, behavior).await;
    let result = AssertUnwindSafe(test(&mut f)).catch_unwind().await;
    f.cleanup().await;
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}
fn market(ids: Value) -> Value {
    json!({"id":ids.to_string(),"version":"v1","active":true,"closed":false,"enableOrderBook":true,"acceptingOrders":true,"clobTokenIds":ids})
}
fn page(ids: &[&str]) -> Value {
    json!({"markets":[market(json!(ids).to_string().into())],"next_cursor":null})
}
fn fixed(ids: &'static [&'static str]) -> HttpReply {
    Arc::new(move |_, _| (200, page(ids)))
}
fn universe(input: Input) -> BTreeSet<String> {
    match input {
        Input::Universe(set) => set.into_iter().map(|asset| asset.to_string()).collect(),
        _ => panic!("expected universe"),
    }
}
fn assets(event: &Event) -> Vec<String> {
    serde_json::from_str::<Value>(&event.text).unwrap()["assets_ids"]
        .as_array()
        .unwrap()
        .iter()
        .map(|x| x.as_str().unwrap().to_owned())
        .collect()
}
async fn eventually<T>(future: impl Future<Output = T>) -> T {
    timeout(Duration::from_secs(15), future)
        .await
        .expect("deadline")
}

// Catches accepting partial pages, dropping v2 IDs, permissive boolean coercions, or broken query cursors.
#[tokio::test]
async fn complete_keyset_discovery_filters_exactly_and_subscribes_both_protocols() {
    fixture(Arc::new(|request,_| {
        if request.contains("after_cursor=next%2Bpage") { (200,json!({"markets":[{"active":true,"closed":false,"enableOrderBook":true,"acceptingOrders":true,"id":"v2-c","version":"v2","positionIds":["c"]}],"next_cursor":null})) }
        else { let mut rejected=market(json!("[\"bad\"]")); rejected["acceptingOrders"]=json!(false); (200,json!({"markets":[market(json!("[\"a\",\"b\"]")),rejected],"next_cursor":"next+page"})) }
    }),Behavior::default(),|f|Box::pin(async move {
        f.start(f.config(),32); assert_eq!(universe(f.input().await),BTreeSet::from(["a".into(),"b".into(),"c".into()]));
        let first=f.event().await; let second=f.event().await;
        assert_eq!(assets(&first),["a","b"]); assert_eq!(assets(&second),["c"]);
        assert!(second.at.duration_since(first.at)>=Duration::from_millis(240));
        let subscription:Value=serde_json::from_str(&first.text).unwrap(); assert_eq!(subscription["type"],"market"); assert_eq!(subscription["custom_feature_enabled"],true);
        let requests=f.requests.lock().unwrap(); assert_eq!(requests.len(),2);
        for request in requests.iter() { for value in ["/markets/keyset?","closed=false","limit=100","version=v1","version=v2"] { assert!(request.contains(value),"missing {value}: {request}"); } }
    })).await;
}

#[tokio::test]
async fn initial_transient_discovery_is_retried_before_subscription() {
    fixture(
        Arc::new(|_, index| {
            if index < 2 {
                (500, json!({}))
            } else {
                (200, page(&["a"]))
            }
        }),
        Behavior::default(),
        |f| {
            Box::pin(async move {
                f.start(f.config(), 16);
                assert_eq!(universe(f.input().await), BTreeSet::from(["a".into()]));
                assert!(f.requests.lock().unwrap().len() >= 3);
                assert_eq!(assets(&f.event().await), ["a"]);
            })
        },
    )
    .await;
}

#[tokio::test]
async fn discovery_access_denied_is_fatal_and_not_retried() {
    fixture(
        Arc::new(|_, _| (401, json!({}))),
        Behavior::default(),
        |f| {
            Box::pin(async move {
                f.start(f.config(), 8);
                assert!(matches!(f.input().await,Input::Fatal(reason) if reason.contains("401")));
                assert!(f.finish().await.is_err());
                assert_eq!(f.requests.lock().unwrap().len(), 1);
            })
        },
    )
    .await;
}

#[tokio::test]
async fn websocket_access_denied_stops_every_replica_without_retry() {
    fixture(
        fixed(&["a"]),
        Behavior {
            deny: true,
            ..Default::default()
        },
        |f| {
            Box::pin(async move {
                let mut config = f.config();
                config.replicas = 2;
                f.start(config, 8);
                let _ = universe(f.input().await);
                eventually(async {
                    loop {
                        if let Input::Fatal(reason) = f.input().await {
                            assert!(reason.contains("403"));
                            break;
                        }
                    }
                })
                .await;
                assert!(f.finish().await.is_err());
            })
        },
    )
    .await;
}

#[tokio::test]
async fn reconnect_delivers_disconnect_before_next_generation_frame() {
    fixture(fixed(&["a"]), Behavior::default(), |f| {
        Box::pin(async move {
            f.start(f.config(), 32);
            let _ = universe(f.input().await);
            let first = f.event().await;
            let source = match f.input().await {
                Input::Events {
                    source,
                    events,
                    received_ms,
                } => {
                    assert!(events.iter().any(|event| matches!(
                        event,
                        orderbook_collector::adapter::Event::Snapshot { .. }
                    )));
                    assert!(received_ms > 0);
                    source
                }
                _ => panic!("frame"),
            };
            f.commands.send(Command::Close(first.id)).unwrap();
            assert!(matches!(f.input().await,Input::Disconnected{source:s} if s==source));
            assert!(matches!(f.input().await,Input::Events{source:s,..} if s!=source));
            let second = f.event().await;
            assert!(second.at.duration_since(first.at) >= Duration::from_millis(250));
        })
    })
    .await;
}

#[tokio::test]
async fn periodic_partial_failure_preserves_universe_and_unchanged_shards() {
    fixture(
        Arc::new(|request, index| match index {
            0 => (200, page(&["a", "b", "c"])),
            1 => (
                200,
                json!({"markets":[market(json!("[\"a\"]"))],"next_cursor":"broken"}),
            ),
            2..=5 => {
                assert!(request.contains("after_cursor=broken"));
                (500, json!({}))
            }
            _ => (200, page(&["a", "b", "d"])),
        }),
        Behavior::default(),
        |f| {
            Box::pin(async move {
                let mut config = f.config();
                config.discovery_secs = 1;
                f.start(config, 64);
                let _ = universe(f.input().await);
                let first = f.event().await;
                let second = f.event().await;
                assert_eq!(assets(&first), ["a", "b"]);
                assert_eq!(assets(&second), ["c"]);
                let updated = eventually(async {
                    loop {
                        // Four page attempts can exceed the generic 8s input deadline.
                        if let Input::Universe(set) =
                            f.inputs.recv().await.expect("input channel closed")
                        {
                            break set;
                        }
                    }
                })
                .await;
                assert_eq!(
                    updated,
                    BTreeSet::from(["a".into(), "b".into(), "d".into()])
                );
                let removed = f.event().await;
                let changed = f.event().await;
                assert_eq!(assets(&removed), ["c"]);
                assert_eq!(
                    serde_json::from_str::<Value>(&removed.text).unwrap()["operation"],
                    "unsubscribe"
                );
                assert_eq!(assets(&changed), ["d"]);
                assert_eq!(
                    changed.id, second.id,
                    "lifecycle update restarted a healthy socket"
                );
                assert!(f.events.try_recv().is_err());
            })
        },
    )
    .await;
}

#[tokio::test]
async fn replicas_have_distinct_sources_and_cancel_unblocks_full_output() {
    fixture(fixed(&["a", "b"]), Behavior::default(), |f| {
        Box::pin(async move {
            let mut config = f.config();
            config.replicas = 2;
            f.start(config, 1);
            let _ = universe(f.input().await);
            let mut sources = BTreeSet::new();
            for _ in 0..2 {
                if let Input::Events { source, .. } = f.input().await {
                    sources.insert(source);
                }
            }
            assert_eq!(sources.len(), 2);
            let first = f.event().await;
            let second = f.event().await;
            for _ in 0..4 {
                let _ = f.commands.send(Command::Text(first.id, "[]".into()));
                let _ = f.commands.send(Command::Text(second.id, "[]".into()));
            }
            tokio::time::sleep(Duration::from_millis(30)).await;
            f.live_cancel.cancel();
            f.finish().await.unwrap();
        })
    })
    .await;
}

#[tokio::test]
async fn socket_guard_rejects_excess_without_silently_truncating() {
    fixture(fixed(&["a"]), Behavior::default(), |f| {
        Box::pin(async move {
            let mut config = f.config();
            config.replicas = usize::MAX;
            f.start(config, 8);
            let error = f.finish().await.unwrap_err().to_string();
            assert!(error.contains("replicas") || error.contains("connection"));
            assert!(f.events.try_recv().is_err());
        })
    })
    .await;
}

#[tokio::test]
async fn quiet_symbols_trigger_scoped_snapshot_recovery_and_rotation() {
    fixture(fixed(&["a"]), Behavior::default(), |f| {
        Box::pin(async move {
            let mut config = f.config();
            config.stale_secs = 1;
            config.rotate_secs = 3;
            f.start(config, 64);
            let _ = universe(f.input().await);
            let first = f.event().await;
            let recovery = f.event().await;
            assert_eq!(assets(&recovery), ["a"]);
            // Resubscription and scoped connection restart both request a fresh snapshot.
            assert!(recovery.at.duration_since(first.at) >= Duration::from_secs(1));
            eventually(async {
                loop {
                    let next = f.event().await;
                    if next.id != first.id {
                        break;
                    }
                }
            })
            .await;
        })
    })
    .await;
}

// A max_symbols selection must not turn an incomplete traversal into success.
#[tokio::test]
async fn explicit_symbol_limit_still_validates_and_quarantines_later_records() {
    fixture(
        Arc::new(|_, index| {
            if index == 0 {
                (
                    200,
                    json!({"markets":[market(json!("[\"z\",\"a\"]"))],"next_cursor":"more"}),
                )
            } else {
                (
                    200,
                    json!({"markets":[market(json!("not valid JSON"))],"next_cursor":null}),
                )
            }
        }),
        Behavior::default(),
        |f| {
            Box::pin(async move {
                let mut config = f.config();
                config.max_symbols = Some(1);
                f.start(config, 16);
                assert_eq!(universe(f.input().await), BTreeSet::from(["a".into()]));
                assert_eq!(f.requests.lock().unwrap().len(), 2);
                assert_eq!(assets(&f.event().await), ["a"]);
            })
        },
    )
    .await;
}

#[tokio::test]
async fn repeated_cursor_never_publishes_a_partial_universe() {
    fixture(
        Arc::new(|_, _| {
            (
                200,
                json!({"markets":[market(json!("[\"a\"]"))],"next_cursor":"same"}),
            )
        }),
        Behavior::default(),
        |f| {
            Box::pin(async move {
                f.start(f.config(), 16);
                eventually(async {
                    while f.requests.lock().unwrap().len() < 4 {
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                })
                .await;
                assert!(f.inputs.try_recv().is_err());
                assert!(f.events.try_recv().is_err());
                f.live_cancel.cancel();
                f.finish().await.unwrap();
            })
        },
    )
    .await;
}

#[tokio::test]
async fn explicit_limit_selects_sorted_ids_only_after_complete_discovery() {
    fixture(fixed(&["z", "b", "a"]), Behavior::default(), |f| {
        Box::pin(async move {
            let mut config = f.config();
            config.max_symbols = Some(2);
            f.start(config, 8);
            assert_eq!(
                universe(f.input().await),
                BTreeSet::from(["a".into(), "b".into()])
            );
            assert_eq!(assets(&f.event().await), ["a", "b"]);
        })
    })
    .await;
}

#[tokio::test]
async fn periodic_access_denied_ends_existing_connections() {
    fixture(
        Arc::new(|_, index| {
            if index == 0 {
                (200, page(&["a"]))
            } else {
                (403, json!({}))
            }
        }),
        Behavior::default(),
        |f| {
            Box::pin(async move {
                let mut config = f.config();
                config.discovery_secs = 1;
                f.start(config, 16);
                let _ = universe(f.input().await);
                let _ = f.event().await;
                eventually(async {
                    loop {
                        if let Input::Fatal(reason) = f.input().await {
                            assert!(reason.contains("403"));
                            break;
                        }
                    }
                })
                .await;
                assert!(f.finish().await.is_err());
                assert_eq!(f.requests.lock().unwrap().len(), 2);
            })
        },
    )
    .await;
}

#[tokio::test]
async fn oversized_ws_message_is_never_forwarded() {
    fixture(
        fixed(&["a"]),
        Behavior {
            oversized: true,
            ..Default::default()
        },
        |f| {
            Box::pin(async move {
                f.start(f.config(), 16);
                let _ = universe(f.input().await);
                let _ = f.event().await;
                assert!(matches!(f.input().await, Input::Disconnected { .. }));
            })
        },
    )
    .await;
}

#[tokio::test]
async fn quiet_socket_sends_ping_and_reconnects_without_inbound_data() {
    fixture(
        fixed(&["a"]),
        Behavior {
            no_pong: true,
            ..Default::default()
        },
        |f| {
            Box::pin(async move {
                f.start(f.config(), 16);
                let _ = universe(f.input().await);
                let first = f.event().await;
                let _ = f.input().await;
                let ping = timeout(Duration::from_secs(12), f.events.recv())
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(ping.text, "PING");
                assert!(ping.at.duration_since(first.at) >= Duration::from_secs(9));
                assert!(matches!(
                    timeout(Duration::from_secs(22), f.inputs.recv())
                        .await
                        .unwrap(),
                    Some(Input::Disconnected { .. })
                ));
            })
        },
    )
    .await;
}

#[tokio::test]
async fn receiver_closure_joins_without_explicit_cancellation() {
    fixture(fixed(&["a"]), Behavior::default(), |f| {
        Box::pin(async move {
            f.start(f.config(), 8);
            let _ = universe(f.input().await);
            let _ = f.event().await;
            f.inputs.close();
            f.finish().await.unwrap();
        })
    })
    .await;
}

#[tokio::test]
async fn websocket_denial_preempts_a_backpressured_universe_update() {
    fixture(
        Arc::new(|_, index| {
            (
                200,
                if index == 0 {
                    page(&["a", "b"])
                } else {
                    page(&["a", "b", "c"])
                },
            )
        }),
        Behavior {
            deny_after: Some(1),
            denial_delay: Duration::from_secs(2),
            ..Default::default()
        },
        |f| {
            Box::pin(async move {
                let mut config = f.config();
                config.shard_size = 1;
                config.discovery_secs = 1;
                f.start(config, 1);
                let _ = universe(f.input().await);
                let _ = f.event().await;
                // The first snapshot fills output. Discovery's next Universe then
                // blocks while another source's handshake returns access denied.
                tokio::time::sleep(Duration::from_secs(3)).await;
                assert!(matches!(f.input().await, Input::Events { .. }));
                assert!(matches!(f.input().await,Input::Fatal(reason) if reason.contains("403")));
                assert!(f.finish().await.is_err());
            })
        },
    )
    .await;
}

#[tokio::test]
async fn discovered_connection_guard_rejects_the_whole_universe() {
    fixture(
        Arc::new(|_, _| {
            let ids: Vec<_> = (0..129).map(|n| n.to_string()).collect();
            (
                200,
                json!({"markets":[market(json!(ids).to_string().into())],"next_cursor":null}),
            )
        }),
        Behavior::default(),
        |f| {
            Box::pin(async move {
                let mut config = f.config();
                config.shard_size = 1;
                f.start(config, 8);
                assert!(
                    f.finish()
                        .await
                        .unwrap_err()
                        .to_string()
                        .contains("increase shard_size")
                );
                assert!(matches!(f.input().await, Input::Fatal(_)));
                assert!(f.events.try_recv().is_err());
            })
        },
    )
    .await;
}

#[tokio::test]
async fn explicit_zero_symbols_publishes_empty_universe_without_dials() {
    fixture(fixed(&["a"]), Behavior::default(), |f| {
        Box::pin(async move {
            let mut config = f.config();
            config.max_symbols = Some(0);
            f.start(config, 8);
            assert!(universe(f.input().await).is_empty());
            f.live_cancel.cancel();
            f.finish().await.unwrap();
            assert!(f.events.try_recv().is_err());
        })
    })
    .await;
}

#[tokio::test]
async fn cancellation_interrupts_an_incomplete_websocket_handshake() {
    fixture(
        fixed(&["a"]),
        Behavior {
            deny: true,
            denial_delay: Duration::from_secs(60),
            ..Default::default()
        },
        |f| {
            Box::pin(async move {
                f.start(f.config(), 8);
                let _ = universe(f.input().await);
                tokio::time::sleep(Duration::from_millis(50)).await;
                f.live_cancel.cancel();
                f.finish().await.unwrap();
            })
        },
    )
    .await;
}

#[tokio::test]
async fn control_ping_is_answered_and_application_pong_keeps_connection_alive() {
    fixture(fixed(&["a"]), Behavior::default(), |f| {
        Box::pin(async move {
            f.start(f.config(), 16);
            let _ = universe(f.input().await);
            let initial = f.event().await;
            let _ = f.input().await;
            f.commands.send(Command::Ping(initial.id)).unwrap();
            assert_eq!(f.event().await.text, "CONTROL_PONG:fixture");
            for _ in 0..2 {
                let ping = timeout(Duration::from_secs(12), f.events.recv())
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(ping.id, initial.id);
                assert_eq!(ping.text, "PING");
            }
            assert!(
                f.inputs.try_recv().is_err(),
                "heartbeats must not become data frames or disconnects"
            );
        })
    })
    .await;
}

#[tokio::test]
async fn quiet_recovery_resets_its_replica_and_resynchronizes_on_same_socket() {
    fixture(fixed(&["a"]), Behavior::default(), |f| {
        Box::pin(async move {
            let mut config = f.config();
            config.stale_secs = 1;
            config.rotate_secs = 3600;
            f.start(config, 64);
            let _ = universe(f.input().await);
            let first = f.event().await;
            let source = match f.input().await {
                Input::Events { source, .. } => source,
                _ => panic!("snapshot"),
            };
            let unsubscribe = f.event().await;
            let subscribe = f.event().await;
            assert_eq!(unsubscribe.id, first.id);
            assert_eq!(subscribe.id, first.id);
            assert_eq!(
                serde_json::from_str::<Value>(&unsubscribe.text).unwrap()["operation"],
                "unsubscribe"
            );
            assert_eq!(
                serde_json::from_str::<Value>(&subscribe.text).unwrap()["operation"],
                "subscribe"
            );
            assert!(matches!(f.input().await,Input::Reset{source:s,symbols} if s==source && symbols==BTreeSet::from(["a".into()])));
            assert!(matches!(f.input().await,Input::Events{source:s,..} if s==source));
            assert!(
                f.inputs.try_recv().is_err(),
                "quiet verification produced extra resets or disconnected the source"
            );
        })
    })
    .await;
}

#[tokio::test]
async fn ambiguous_eligibility_preserves_existing_universe() {
    for broken in [Value::Null, json!("true")] {
        let broken = broken.clone();
        fixture(
            Arc::new(move |_, index| {
                if index == 1 {
                    let mut m = market(json!("[\"a\"]"));
                    m["active"] = broken.clone();
                    (200, json!({"markets":[m],"next_cursor":null}))
                } else {
                    (200, page(&["a"]))
                }
            }),
            Behavior::default(),
            |f| {
                Box::pin(async move {
                    let mut config = f.config();
                    config.discovery_secs = 1;
                    f.start(config, 64);
                    assert_eq!(universe(f.input().await), BTreeSet::from(["a".into()]));
                    f.event().await;
                    assert!(matches!(f.input().await, Input::Events { .. }));
                    eventually(async {
                        while f.requests.lock().unwrap().len() < 3 {
                            tokio::time::sleep(Duration::from_millis(20)).await;
                        }
                    })
                    .await;
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    assert!(
                        f.inputs.try_recv().is_err(),
                        "ambiguous discovery removed a live symbol"
                    );
                    assert!(
                        f.events.try_recv().is_err(),
                        "unchanged subscription restarted"
                    );
                })
            },
        )
        .await;
    }
}

#[tokio::test]
async fn continuous_deltas_without_snapshot_still_trigger_recovery() {
    fixture(fixed(&["a"]),Behavior{omit_initial_book:true,..Default::default()},|f|Box::pin(async move {
        let mut config=f.config();config.stale_secs=1;config.rotate_secs=3600;
        f.start(config,256);let _=universe(f.input().await);let first=f.event().await;
        let deadline=tokio::time::sleep(Duration::from_secs(3));tokio::pin!(deadline);
        let mut traffic=tokio::time::interval(Duration::from_millis(50));
        loop {
            tokio::select! {
                _=&mut deadline=>panic!("continuous unsynchronized deltas postponed recovery"),
                _=traffic.tick()=>{f.commands.send(Command::Text(first.id,json!({"event_type":"price_change","timestamp":"1001","price_changes":[{"asset_id":"a","side":"BUY","price":"0.4","size":"2"}]}).to_string())).unwrap();},
                event=f.events.recv()=>{let event=event.unwrap();if event.id == first.id && event.text.contains("unsubscribe"){break;}}
            }
        }
    })).await;
}

#[tokio::test]
async fn trade_activity_does_not_keep_a_stale_book_subscribed() {
    fixture(fixed(&["a"]),Behavior::default(),|f|Box::pin(async move {
        let mut config=f.config();config.stale_secs=1;config.rotate_secs=3600;
        f.start(config,256);let _=universe(f.input().await);let first=f.event().await;
        assert!(matches!(f.input().await,Input::Events{..}));
        let deadline=tokio::time::sleep(Duration::from_secs(3));tokio::pin!(deadline);
        let mut traffic=tokio::time::interval(Duration::from_millis(50));
        loop {tokio::select! {
            _=&mut deadline=>panic!("non-book messages postponed stale book recovery"),
            _=traffic.tick()=>{f.commands.send(Command::Text(first.id,json!({"event_type":"last_trade_price","timestamp":"1001","asset_id":"a","price":"0.4"}).to_string())).unwrap();},
            event=f.events.recv()=>{let event=event.unwrap();if event.id == first.id && event.text.contains("unsubscribe"){break;}}
        }}
    })).await;
}

#[tokio::test]
async fn explicitly_inactive_records_do_not_require_unrelated_flags() {
    fixture(Arc::new(|_,_|(200,json!({"markets":[market(json!("[\"a\"]")),{"active":false,"closed":false,"clobTokenIds":"[\"inactive\"]"},{"active":true,"closed":false,"enableOrderBook":false}],"next_cursor":null}))),Behavior::default(),|f|Box::pin(async move {
        f.start(f.config(),16);
        assert_eq!(universe(f.input().await),BTreeSet::from(["a".into()]));
        assert_eq!(assets(&f.event().await),["a"]);
    })).await;
}

#[tokio::test]
async fn malformed_frame_reconnects_only_its_source_before_forwarding_more_data() {
    fixture(fixed(&["a"]), Behavior::default(), |f| {
        Box::pin(async move {
            f.start(f.config(), 32);
            let _ = universe(f.input().await);
            let first = f.event().await;
            let source = match f.input().await {
                Input::Events { source, .. } => source,
                _ => panic!("expected initial snapshot"),
            };
            f.commands
                .send(Command::Text(first.id, "not JSON".into()))
                .unwrap();
            assert!(matches!(f.input().await,Input::Disconnected{source:s} if s==source));
            assert!(matches!(f.input().await,Input::Events{source:s,..} if s!=source));
        })
    })
    .await;
}

#[tokio::test]
async fn regressing_delta_resets_only_its_asset_and_keeps_the_socket() {
    fixture(fixed(&["a","b"]),Behavior::default(),|f|Box::pin(async move {
        f.start(f.config(),32); let _=universe(f.input().await); let first=f.event().await;
        let source=match f.input().await {Input::Events{source,..}=>source,_=>panic!("snapshot")};
        f.commands.send(Command::Text(first.id,json!({"event_type":"price_change","timestamp":"1100","price_changes":[{"asset_id":"a","side":"BUY","price":"0.4","size":"2"}]}).to_string())).unwrap();
        let _=f.input().await;
        f.commands.send(Command::Text(first.id,json!({"event_type":"price_change","timestamp":"1050","price_changes":[{"asset_id":"a","side":"BUY","price":"0.4","size":"3"}]}).to_string())).unwrap();
        assert!(matches!(f.input().await,Input::Reset{source:s,symbols} if s==source && symbols==BTreeSet::from(["a".into()])));
        let unsubscribe=f.event().await; let subscribe=f.event().await;
        assert_eq!(unsubscribe.id,first.id); assert_eq!(subscribe.id,first.id);
        assert_eq!(assets(&unsubscribe),["a"]);assert_eq!(assets(&subscribe),["a"]);
        assert!(matches!(f.input().await,Input::Events{source:s,..} if s==source));
    })).await;
}

#[tokio::test]
async fn short_terminal_keyset_page_can_omit_cursor() {
    fixture(
        Arc::new(|_, _| (200, json!({"markets":[market(json!("[\"a\"]"))]}))),
        Behavior::default(),
        |f| {
            Box::pin(async move {
                f.start(f.config(), 8);
                assert_eq!(universe(f.input().await), BTreeSet::from(["a".into()]));
            })
        },
    )
    .await;
}
#[tokio::test]
async fn canonical_protocol_identifier_does_not_subscribe_both_id_spaces() {
    fixture(Arc::new(|_,_|(200,json!({"markets":[
        {"active":true,"closed":false,"enableOrderBook":true,"acceptingOrders":true,"id":"ctf-market","version":"v1","clobTokenIds":"[\"ctf\"]","positionIds":["wrong-position"]},
        {"active":true,"closed":false,"enableOrderBook":true,"acceptingOrders":true,"id":"position-market","version":"v2","clobTokenIds":"[\"wrong-token\"]","positionIds":["position"]}],"next_cursor":null}))),Behavior::default(),|f|Box::pin(async move {
        f.start(f.config(),16);assert_eq!(universe(f.input().await),BTreeSet::from(["ctf".into(),"position".into()]));
    })).await;
}
#[tokio::test]
async fn nullable_accepting_state_retains_previous_symbols_but_does_not_invent_live_markets() {
    fixture(
        Arc::new(|_, index| {
            let mut known = market(json!("[\"a\"]"));
            let mut pending = market(json!("[\"pending\"]"));
            pending["acceptingOrders"] = Value::Null;
            if index > 0 {
                known["acceptingOrders"] = Value::Null;
            }
            (200, json!({"markets":[known,pending],"next_cursor":null}))
        }),
        Behavior::default(),
        |f| {
            Box::pin(async move {
                let mut config = f.config();
                config.discovery_secs = 1;
                f.start(config, 32);
                assert_eq!(
                    universe(f.input().await),
                    BTreeSet::from(["a".into(), "pending".into()])
                );
                f.event().await;
                f.input().await;
                eventually(async {
                    while f.requests.lock().unwrap().len() < 2 {
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                })
                .await;
                tokio::time::sleep(Duration::from_millis(30)).await;
                assert!(f.inputs.try_recv().is_err());
                assert!(f.events.try_recv().is_err());
            })
        },
    )
    .await;
}

#[tokio::test]
async fn automatic_sharding_covers_a_large_universe_without_silent_truncation() {
    let ids: Vec<String> = (0..20000).map(|n| format!("asset-{n}")).collect();
    let expected: BTreeSet<_> = ids.iter().cloned().collect();
    fixture(
        Arc::new(move |_, _| {
            (
                200,
                json!({"markets":[market(json!(ids).to_string().into())],"next_cursor":null}),
            )
        }),
        Behavior::default(),
        |f| {
            Box::pin(async move {
                let mut config = f.config();
                config.shard_size = 0;
                config.replicas = 2;
                f.start(config, 16);
                assert_eq!(universe(f.input().await), expected);
            })
        },
    )
    .await;
}
#[tokio::test]
async fn initial_subscriptions_are_batched_while_socket_remains_readable() {
    let ids: Vec<String> = (0..201).map(|n| format!("asset-{n:03}")).collect();
    fixture(
        Arc::new(move |_, _| {
            (
                200,
                json!({"markets":[market(json!(ids).to_string().into())],"next_cursor":null}),
            )
        }),
        Behavior::default(),
        |f| {
            Box::pin(async move {
                let mut config = f.config();
                config.shard_size = 1000;
                config.replicas = 1;
                f.start(config, 16);
                let _ = universe(f.input().await);
                let first = f.event().await;
                let second = f.event().await;
                let third = f.event().await;
                assert_eq!(assets(&first).len(), 100);
                assert_eq!(assets(&second).len(), 100);
                assert_eq!(assets(&third).len(), 1);
                assert_eq!(first.id, second.id);
                assert_eq!(second.id, third.id);
                assert!(second.at.duration_since(first.at) >= Duration::from_millis(90));
                let mut seen = BTreeSet::new();
                for _ in 0..3 {
                    if let Input::Events { events, .. } = f.input().await {
                        for e in events {
                            if let orderbook_collector::adapter::Event::Snapshot {
                                symbol, ..
                            } = e
                            {
                                seen.insert(symbol);
                            }
                        }
                    }
                }
                assert_eq!(seen.len(), 201);
            })
        },
    )
    .await;
}

#[tokio::test]
async fn unresolved_market_ids_preserve_that_markets_previous_subscriptions() {
    fixture(Arc::new(|_,index|{
        let mut known=market(json!("[\"a\"]"));known["id"]=json!("stable");
        if index>0 {known["clobTokenIds"]=Value::Null;}
        (200,json!({"markets":[known,{"id":"not-launched","version":"v1","active":true,"closed":false,"enableOrderBook":true,"acceptingOrders":null,"clobTokenIds":null}],"next_cursor":null}))
    }),Behavior::default(),|f|Box::pin(async move {
        let mut config=f.config();config.discovery_secs=1;f.start(config,32);
        assert_eq!(universe(f.input().await),BTreeSet::from(["a".into()]));f.event().await;f.input().await;
        eventually(async{while f.requests.lock().unwrap().len()<2 {tokio::time::sleep(Duration::from_millis(20)).await;}}).await;
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(f.inputs.try_recv().is_err());assert!(f.events.try_recv().is_err());
    })).await;
}

#[tokio::test]
async fn transient_page_error_retries_same_cursor_without_restarting_the_sweep() {
    fixture(
        Arc::new(|request, index| match index {
            0 => (
                200,
                json!({"markets":[market(json!("[\"a\"]"))],"next_cursor":"second"}),
            ),
            1 => {
                assert!(request.contains("after_cursor=second"));
                (503, json!({}))
            }
            _ => {
                assert!(request.contains("after_cursor=second"));
                (200, page(&["b"]))
            }
        }),
        Behavior::default(),
        |f| {
            Box::pin(async move {
                f.start(f.config(), 16);
                assert_eq!(
                    universe(f.input().await),
                    BTreeSet::from(["a".into(), "b".into()])
                );
                assert_eq!(f.requests.lock().unwrap().len(), 3);
            })
        },
    )
    .await;
}
#[tokio::test]
async fn malformed_market_is_quarantined_without_blocking_healthy_new_markets() {
    fixture(
        Arc::new(|_, index| {
            let mut bad = market(json!("[\"a\"]"));
            bad["id"] = json!("stable");
            if index > 0 {
                bad["active"] = json!("broken");
                (
                    200,
                    json!({"markets":[bad,market(json!("[\"new\"]"))],"next_cursor":null}),
                )
            } else {
                (200, json!({"markets":[bad],"next_cursor":null}))
            }
        }),
        Behavior::default(),
        |f| {
            Box::pin(async move {
                let mut config = f.config();
                config.discovery_secs = 1;
                f.start(config, 32);
                assert_eq!(universe(f.input().await), BTreeSet::from(["a".into()]));
                let updated = eventually(async {
                    loop {
                        if let Input::Universe(set) = f.input().await {
                            break set;
                        }
                    }
                })
                .await;
                assert_eq!(updated, BTreeSet::from(["a".into(), "new".into()]));
            })
        },
    )
    .await;
}

#[tokio::test]
async fn event_index_discovers_nested_regular_markets_with_canonical_versions() {
    fixture(Arc::new(|_,_|(200,json!({"events":[{"id":"event","markets":[market(json!("[\"a\",\"b\"]")),{"id":"v2","version":"v2","active":true,"closed":false,"enableOrderBook":true,"acceptingOrders":true,"positionIds":["c"]}]}],"next_cursor":null}))),Behavior::default(),|f|Box::pin(async move {
        let mut config=f.config();config.discovery_url=config.discovery_url.replace("/markets/keyset","/events/keyset");f.start(config,16);
        assert_eq!(universe(f.input().await),BTreeSet::from(["a".into(),"b".into(),"c".into()]));
    })).await;
}

#[tokio::test]
async fn rate_limited_page_honors_retry_after_before_same_cursor_retry() {
    fixture(
        Arc::new(|_, index| {
            if index == 0 {
                (429, json!({}))
            } else {
                (200, page(&["a"]))
            }
        }),
        Behavior::default(),
        |f| {
            Box::pin(async move {
                let start = Instant::now();
                f.start(f.config(), 16);
                assert_eq!(universe(f.input().await), BTreeSet::from(["a".into()]));
                assert!(start.elapsed() >= Duration::from_millis(1900));
                assert_eq!(f.requests.lock().unwrap().len(), 2);
            })
        },
    )
    .await;
}
#[tokio::test]
async fn oversized_metadata_page_reduces_page_size_instead_of_repeating_it() {
    fixture(
        Arc::new(|request, index| {
            if index == 0 {
                assert!(request.contains("limit=100"));
                (
                    200,
                    json!({"markets":[],"padding":"x".repeat(33*1024*1024),"next_cursor":null}),
                )
            } else {
                assert!(request.contains("limit=50"));
                (200, page(&["a"]))
            }
        }),
        Behavior::default(),
        |f| {
            Box::pin(async move {
                f.start(f.config(), 16);
                assert_eq!(universe(f.input().await), BTreeSet::from(["a".into()]));
                assert_eq!(f.requests.lock().unwrap().len(), 2);
            })
        },
    )
    .await;
}

#[tokio::test]
async fn nonobject_metadata_record_is_quarantined_without_losing_known_symbols() {
    fixture(
        Arc::new(|_, index| {
            if index == 0 {
                (200, page(&["a"]))
            } else {
                (
                    200,
                    json!({"markets":[42,market(json!("[\"b\"]"))],"next_cursor":null}),
                )
            }
        }),
        Behavior::default(),
        |f| {
            Box::pin(async move {
                let mut config = f.config();
                config.discovery_secs = 1;
                f.start(config, 32);
                assert_eq!(universe(f.input().await), BTreeSet::from(["a".into()]));
                loop {
                    if let Input::Universe(set) = f.input().await {
                        assert_eq!(set, BTreeSet::from(["a".into(), "b".into()]));
                        break;
                    }
                }
            })
        },
    )
    .await;
}

#[tokio::test]
async fn lifecycle_hint_adds_symbols_without_waiting_for_a_slow_metadata_sweep() {
    fixture(fixed(&["a"]), Behavior::default(), |f| {
        Box::pin(async move {
            let (tx, rx) = mpsc::channel(32);
            f.inputs = rx;
            let (hint_tx, hints) = mpsc::channel(2);
            let mut config = f.config();
            config.discovery_secs = 3600;
            config.shard_size = 100;
            f.live = Some(tokio::spawn(live::run_with_hints(
                config,
                tx,
                f.live_cancel.clone(),
                hints,
            )));
            assert_eq!(universe(f.input().await), BTreeSet::from(["a".into()]));
            let first = f.event().await;
            hint_tx
                .send(live::Hint::NewMarket(vec![
                    "new-yes".into(),
                    "new-no".into(),
                ]))
                .await
                .unwrap();
            loop {
                if let Input::AddSymbols(set) = f.input().await {
                    assert_eq!(
                        set,
                        BTreeSet::from([
                            orderbook_collector::assets::intern("new-no"),
                            orderbook_collector::assets::intern("new-yes")
                        ])
                    );
                    break;
                }
            }
            let added = f.event().await;
            assert_eq!(added.id, first.id);
            assert_eq!(assets(&added), ["new-no", "new-yes"]);
            for _ in 0..100 {
                hint_tx
                    .send(live::Hint::NewMarket(vec![
                        "new-yes".into(),
                        "new-no".into(),
                    ]))
                    .await
                    .unwrap();
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
            while let Ok(input) = f.inputs.try_recv() {
                assert!(
                    !matches!(input, Input::AddSymbols(_) | Input::Universe(_)),
                    "duplicate hint republished universe"
                );
            }
            assert!(
                f.events.try_recv().is_err(),
                "duplicate hint resubscribed assets"
            );
            assert_eq!(
                f.requests.lock().unwrap().len(),
                1,
                "hint restarted metadata traversal"
            );
        })
    })
    .await;
}

#[tokio::test]
async fn event_catalog_is_crosschecked_with_a_standalone_market_sample() {
    fixture(
        Arc::new(|request, _| {
            if request.starts_with("GET /markets/keyset?") {
                (200, page(&["a"]))
            } else {
                (
                    200,
                    json!({"events":[{"markets":[market(json!("[\"a\"]"))]}],"next_cursor":null}),
                )
            }
        }),
        Behavior::default(),
        |f| {
            Box::pin(async move {
                let mut config = f.config();
                config.audit_catalog = true;
                config.discovery_url = config
                    .discovery_url
                    .replace("/markets/keyset", "/events/keyset");
                f.start(config, 32);
                assert_eq!(universe(f.input().await), BTreeSet::from(["a".into()]));
                let requests = f.requests.lock().unwrap();
                assert_eq!(requests.len(), 2);
                assert!(requests[0].starts_with("GET /events/keyset?"));
                assert!(requests[1].starts_with("GET /markets/keyset?"));
            })
        },
    )
    .await;
}

#[tokio::test]
async fn a_sampled_catalog_omission_cannot_publish_an_event_universe() {
    fixture(
        Arc::new(|request, _| {
            if request.starts_with("GET /markets/keyset?") {
                (200, page(&["missing"]))
            } else {
                (
                    200,
                    json!({"events":[{"markets":[market(json!("[\"a\"]"))]}],"next_cursor":null}),
                )
            }
        }),
        Behavior::default(),
        |f| {
            Box::pin(async move {
                let mut config = f.config();
                config.audit_catalog = true;
                config.discovery_url = config
                    .discovery_url
                    .replace("/markets/keyset", "/events/keyset");
                f.start(config, 32);
                eventually(async {
                    while f.requests.lock().unwrap().len() < 2 {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                })
                .await;
                tokio::time::sleep(Duration::from_millis(50)).await;
                assert!(
                    f.inputs.try_recv().is_err(),
                    "universe published despite catalog omission"
                );
                assert!(f.events.try_recv().is_err());
            })
        },
    )
    .await;
}

#[tokio::test]
async fn exhausted_page_attempts_resume_the_same_cursor_without_restarting() {
    fixture(
        Arc::new(|request, index| match index {
            0 => (
                200,
                json!({"markets":[market(json!("[\"a\"]"))],"next_cursor":"checkpoint"}),
            ),
            1..=4 => {
                assert!(request.contains("after_cursor=checkpoint"));
                (503, json!({}))
            }
            _ => {
                assert!(request.contains("after_cursor=checkpoint"));
                (200, page(&["b"]))
            }
        }),
        Behavior::default(),
        |f| {
            Box::pin(async move {
                f.start(f.config(), 16);
                let input = eventually(f.inputs.recv()).await.unwrap();
                assert_eq!(universe(input), BTreeSet::from(["a".into(), "b".into()]));
                assert_eq!(f.requests.lock().unwrap().len(), 6);
            })
        },
    )
    .await;
}

#[tokio::test]
async fn resolved_lifecycle_unsubscribes_promptly_and_stale_metadata_cannot_resurrect() {
    fixture(fixed(&["a", "b"]), Behavior::default(), |f| {
        Box::pin(async move {
            let (tx, rx) = mpsc::channel(32);
            f.inputs = rx;
            let (hint_tx, hints) = mpsc::channel(8);
            let mut config = f.config();
            config.discovery_secs = 1;
            config.shard_size = 100;
            f.live = Some(tokio::spawn(live::run_with_hints(
                config,
                tx,
                f.live_cancel.clone(),
                hints,
            )));
            assert_eq!(
                universe(f.input().await),
                BTreeSet::from(["a".into(), "b".into()])
            );
            let first = f.event().await;
            hint_tx
                .send(live::Hint::Resolved(vec!["a".into()]))
                .await
                .unwrap();
            loop {
                if let Input::RemoveSymbols(set) = f.input().await {
                    assert_eq!(
                        set,
                        BTreeSet::from([orderbook_collector::assets::intern("a")])
                    );
                    break;
                }
            }
            let removed = f.event().await;
            assert_eq!(removed.id, first.id);
            assert_eq!(assets(&removed), ["a"]);
            assert_eq!(
                serde_json::from_str::<Value>(&removed.text).unwrap()["operation"],
                "unsubscribe"
            );
            hint_tx
                .send(live::Hint::NewMarket(vec!["a".into()]))
                .await
                .unwrap();
            eventually(async {
                while f.requests.lock().unwrap().len() < 2 {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await;
            tokio::time::sleep(Duration::from_millis(100)).await;
            while let Ok(input) = f.inputs.try_recv() {
                assert!(
                    !matches!(input, Input::AddSymbols(_) | Input::Universe(_)),
                    "resolved symbol resurrected"
                );
            }
            assert!(
                f.events.try_recv().is_err(),
                "resolved subscription resurrected"
            );
        })
    })
    .await;
}

#[tokio::test]
async fn lifecycle_burst_coalesces_into_bounded_engine_additions() {
    fixture(fixed(&["a"]), Behavior::default(), |f| {
        Box::pin(async move {
            let (tx, rx) = mpsc::channel(64);
            f.inputs = rx;
            let (hint_tx, hints) = mpsc::channel(64);
            let mut config = f.config();
            config.discovery_secs = 3600;
            config.shard_size = 1000;
            f.live = Some(tokio::spawn(live::run_with_hints(
                config,
                tx,
                f.live_cancel.clone(),
                hints,
            )));
            let _ = universe(f.input().await);
            let first = f.event().await;
            // Consume the initial interval tick before the burst, then queue it synchronously.
            hint_tx
                .send(live::Hint::NewMarket(vec!["seed".into()]))
                .await
                .unwrap();
            loop {
                if matches!(f.input().await, Input::AddSymbols(_)) {
                    break;
                }
            }
            let _ = f.event().await;
            for n in 0..50 {
                hint_tx
                    .try_send(live::Hint::NewMarket(vec![format!("new-{n:02}")]))
                    .unwrap();
            }
            let added = loop {
                if let Input::AddSymbols(set) = f.input().await {
                    break set;
                }
            };
            assert_eq!(added.len(), 50, "burst was not coalesced");
            let wire = f.event().await;
            assert_eq!(wire.id, first.id);
            assert_eq!(assets(&wire).len(), 50);
        })
    })
    .await;
}
