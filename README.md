# Rust orderbook collector

A read-only Polymarket market-data collector in Rust, with SBE serialization. It discovers markets, distributes subscriptions across WebSockets, maintains per-outcome order books, and writes deltas plus minute snapshots in a standardized SBE format.

## Quickstart

Requires Rust stable (edition 2024), Cargo and a C compiler for TLS. Python 3 is needed for the shutdown integration check. Dependencies are locked; the first build downloads crates.

```sh
cargo test --locked
mkdir -p output
cargo run --locked -- --no-stdout --output output/demo.sbe replay --input fixtures/demo.jsonl
cargo run --locked -- inspect --input output/demo.sbe
```

The offline fixture produces eight records covering updates, zero-size deletion, standby failover, minute snapshots and full-book replacement. Replay advances a virtual clock; it is not live-market evidence. Prices and quantities use millionths (`400000` means `0.40`). Output paths must be new. The upstream aligned writer pads to 64 MiB, including for this small demo.

## Live collection

```sh
cargo run --locked --release -- --no-stdout --output output/live.sbe live
# Ctrl-C closes connections, drains accepted input and flushes output.
```

For binary stdout instead:

```sh
cargo run --locked --release -- live > output/live-stream.sbe
```

Stdout uses the upstream SBE encoder. `--output` also writes the aligned file format used by `inspect`; `--no-stdout` selects file-only operation. Diagnostics go to stderr. No account, credentials or trading operations are required.

`live --help` lists configuration. Defaults use standalone Gamma market discovery, automatic sizing with at most 128 base sockets, one primary per shard, a 300-second activity monitor and hourly connection rotation. `--replicas 2` enables independent standby books. Discovery requests both market protocol versions; a complete initial walk can take tens of minutes before collection starts. Completed sweeps reconcile additions/removals, and stream lifecycle hints add markets between sweeps. `--max-symbols N` explicitly selects a partial diagnostic run.

## Design

- Exact checked decimal conversion; quantities are absolute and zero deletes a level.
- Independent books per physical source, snapshot gating and watermark-safe failover. Switching sources emits a complete snapshot before later deltas.
- Stable shards, incremental subscriptions, paced reconnects and bounded queues. Rotation warms a replacement before closing the old connection.
- Whole-frame validation, bounded pre-snapshot buffering and explicit counts for missing books. Quiet but healthy books do not expire by age.
- Upstream encoder, aligned writer, reader, schema and `Side` retained unchanged.

See [architecture](docs/ARCHITECTURE.md), [validation procedure](docs/LIVE_VALIDATION.md) and [sources/attribution](docs/SOURCES.md). Serialization provenance and the original template documentation are retained in [THIRD_PARTY.md](THIRD_PARTY.md).

## Verification and scope

The automated suite covers book semantics, precision, local HTTP/WebSocket faults, reconnects, failover, backpressure, output failures and SIGINT. Two explicit release fixtures exercise a 554,212-symbol empty-book catalog and indexed disconnects with populated replica books. Commands and measured scope are in [the verification record](evidence/VERIFICATION.md).

The final implementation completed a small live check that grew to 42 assets, with two copies, four warm promotions and complete decoded minute batches. **Sustained full-universe coverage is not verified.** An earlier broad run completed discovery but developed coverage gaps; the final recovery corrections have not had a comparable broad soak. Missing books remain reported rather than replaced with fabricated snapshots.

The public feed has no documented contiguous sequence, so this collector cannot establish gap-free or exactly-once history. Pre-book buffer overflow currently resets the whole source; a backpressured refresh can delay other subscription work. File flush is not fsync durability. These limitations and their recovery behavior are documented in the architecture.
