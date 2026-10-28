//! One read-only WebSocket connection, one asset, no retries; no REST /book use.
use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use orderbook_collector::{adapter, engine::Engine};
use serde_json::json;
use std::time::Duration;
use tokio_tungstenite::{connect_async, tungstenite::Message};
#[tokio::main(worker_threads = 1)]
async fn main() -> Result<()> {
    let symbol = std::env::args()
        .nth(1)
        .context("usage: cargo run --example live_probe -- ASSET_ID")?;
    let url = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
    eprintln!(
        "{}",
        json!({"probe":"single_public_websocket","pid":std::process::id(),"at":chrono::Utc::now().to_rfc3339(),"url":url,"symbols":1,"retries":0})
    );
    let (mut socket, _) = match tokio::time::timeout(Duration::from_secs(10), connect_async(url))
        .await
    {
        Ok(Ok(s)) => s,
        Ok(Err(error)) => {
            eprintln!(
                "{}",
                json!({"result":"connection_failed","error":error.to_string(),"action":"stopped without retry or bypass"})
            );
            return Err(error.into());
        }
        Err(e) => {
            eprintln!(
                "{}",
                json!({"result":"timeout","action":"stopped without retry"})
            );
            return Err(e.into());
        }
    };
    let result:Result<()>=async {
  socket.send(Message::Text(json!({"assets_ids":[symbol],"type":"market","custom_feature_enabled":true}).to_string().into())).await?;
  let mut engine=Engine::new(1,300000);engine.set_universe([symbol].into());let mut frames=0;let mut records=0;
  let deadline=tokio::time::sleep(Duration::from_secs(12));tokio::pin!(deadline);let mut heartbeat=tokio::time::interval_at(tokio::time::Instant::now()+Duration::from_secs(10),Duration::from_secs(10));
  loop {tokio::select!{
   _=&mut deadline=>break,
   _=heartbeat.tick()=>socket.send(Message::Text("PING".into())).await?,
   message=socket.next()=>match message {
    Some(Ok(Message::Text(t)))=>{frames+=1;for event in adapter::decode(&t)? {records+=engine.apply(0,event,chrono::Utc::now().timestamp_millis())?.len();}},
    Some(Ok(Message::Ping(p)))=>socket.send(Message::Pong(p)).await?,
    Some(Ok(Message::Close(_)))|None=>break,
    Some(Err(e))=>return Err(e.into()),_=>{},
   }
  }}
  eprintln!("{}",json!({"result":"observed","frames":frames,"records":records,"ready":engine.ready_count(chrono::Utc::now().timestamp_millis()),"scope":"one asset for at most 12 seconds; not full-universe evidence"}));anyhow::ensure!(records>0,"no usable book observed");Ok(())
 }.await;
    let closed = tokio::time::timeout(Duration::from_secs(1), socket.close(None)).await;
    eprintln!(
        "{}",
        json!({"cleanup":"close_attempt_completed","close_ok":matches!(closed,Ok(Ok(())))})
    );
    result
}
