# Verification

Tested on macOS arm64 with Rust/Cargo 1.99.1. Dependencies are locked. Automated tests use synthetic data and local HTTP/WebSocket fixtures; they do not require an exchange account or call public exchange services.

## Automated checks

- 187 ordinary Rust tests passed, zero failures. Two large scale fixtures are ignored in the ordinary suite and passed when invoked explicitly in release mode.
- 26 Python tests passed, including isolated execution.
- Formatting and strict all-target Clippy passed.
- A real 61.27-second subprocess test observed the scheduled minute, WebSocket Close, four decoded SBE records, child reaping and released listener ports.
- The offline replay encodes and decodes eight records using the supplied helpers, including a failover snapshot and zero-size deletion.

```sh
cargo test --locked
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
python3 -I -m unittest discover -s tests -p 'test_*.py'
cargo test --locked --release --test scale --test disconnect_scale -- --ignored --nocapture --test-threads=1
python3 scripts/verify_shutdown.py --verify-minute
```

First build needs access to crates.io. After dependencies are cached, Cargo checks can also run with `--offline`. Required helper, schema and `Side` files remain byte-identical to upstream `97360aefacaa455289f68cbec51b9965f8b7e949`.

## Scale fixtures

The streamed-snapshot fixture uses 554,212 symbols, two **empty-book** replicas and shared subscription catalogs. Two complete ordered passes took 0.182s / 0.183s at 690,077,696 bytes peak RSS. An injected callback failure stops emission. This does not measure populated-depth memory, network transport or SBE serialization.

The indexed-disconnect fixture uses a 555,000-symbol catalog with 68,000 populated replica books over 34,000 symbols. It visits and emits exactly the affected 17,000 symbols in 6.11ms, at 253,607,936 bytes peak RSS. These are process-local observations, not exchange-throughput benchmarks.

## Public data observations

The final implementation completed a 211.43-second small-scope live run. It started from one market-ID query and grew to 42 assets through global lifecycle hints. Two independent copies remained initialized in every observed health sample. Four warm promotions completed. Three minute batches emitted and decoded 26 / 26 / 34 records, with no missing books or ordering errors. All 6,108 records decoded; bounded SIGINT exited zero and temporary output was removed. Four discovery sweeps completed only the filtered query, not a full exchange catalog.

An earlier broad run completed a standalone walk of 2,713 pages, 271,277 market records and 542,344 canonical candidate IDs, with 44 unresolved market records separately reported. It reached 99.68% peak health readiness but developed gaps; its longest observed stretch at or above 95% was nine minutes. All 15 nonempty minute ranges decoded correctly, but none included every candidate. That run preceded the final refresh and rotation corrections.

**Sustained full-universe readiness of the final implementation remains unverified.** The small run establishes its own scope only. Missing books and unresolved metadata are reported explicitly; no placeholder or undocumented readiness filter hides them. On this test host CLOB REST returned HTTP 403 and was not retried or bypassed; the collector uses documented public Gamma and WebSocket services.

Exact counts and measurement scope are in [submission-results.json](submission-results.json). The architecture documents remaining source-wide overflow recovery, backpressure, timestamp ambiguity and durability limits. No exactly-once or gap-free history guarantee is claimed.
