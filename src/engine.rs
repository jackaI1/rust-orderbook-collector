//! Single-owner state machine. Each connection has an independent book.
//! Quiet books remain valid until disconnect, explicit reset, or market removal.
use crate::{
    Side,
    adapter::Event,
    assets::{Asset, intern},
    util::dtos::challenge_orderbook::{
        PriceLevelUpdateDomain, SnapshotAsksEntryDomain, SnapshotBidsEntryDomain, SnapshotDomain,
    },
};
use anyhow::{Context, ensure};
use chrono::{DateTime, Utc};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, PartialEq)]
pub enum Record {
    Snapshot(SnapshotDomain),
    Delta(PriceLevelUpdateDomain),
}
#[derive(Default, Debug, Clone, Serialize)]
pub struct Metrics {
    pub snapshots: u64,
    pub deltas: u64,
    pub duplicates: u64,
    pub invalidations: u64,
    pub failovers: u64,
    pub gated: u64,
    pub resnapshots: u64,
    pub same_timestamp_book_matches: u64,
    pub same_timestamp_book_differences: u64,
    /// Actual indexed symbol states visited by disconnects, including standbys.
    pub disconnect_books_visited: u64,
}
#[derive(Default, Debug, Clone, Serialize)]
pub struct Coverage {
    pub symbols: usize,
    pub ready: usize,
    pub redundant_ready: usize,
    pub uninitialized: usize,
    pub watermark_blocked: usize,
    /// Legacy age-based counter, retained for consumers; always zero.
    pub stale: usize,
    pub replica_books: usize,
    pub uninitialized_sample: Vec<String>,
}
#[derive(Clone)]
struct Book {
    bids: BTreeMap<i64, i64>,
    asks: BTreeMap<i64, i64>,
    timestamp: i64,
}
#[derive(Default)]
struct Symbol {
    books: BTreeMap<usize, Book>,
    active: Option<usize>,
    watermark: Option<i64>,
}
pub struct Engine {
    symbols: BTreeMap<Asset, Symbol>,
    // Physical connection IDs are sparse and never reused by the transport.
    // Only existing books own memberships; empty source sets are removed.
    source_symbols: BTreeMap<usize, BTreeSet<Asset>>,
    resolved: BTreeSet<Asset>,
    source_limit: usize,
    pub metrics: Metrics,
}
fn time(ms: i64) -> anyhow::Result<DateTime<Utc>> {
    ensure!(ms >= 0, "negative timestamp");
    DateTime::from_timestamp_millis(ms).context("timestamp outside chrono range")
}
impl Book {
    fn snapshot(&self, symbol: &str, now: i64) -> Record {
        Record::Snapshot(SnapshotDomain {
            received_at: time(now).expect("validated receive time"),
            exchange_timestamp: time(self.timestamp).expect("validated exchange time"),
            symbol: symbol.into(),
            bids: self
                .bids
                .iter()
                .rev()
                .map(|(&price_level, &quantity)| SnapshotBidsEntryDomain {
                    price_level,
                    quantity,
                })
                .collect(),
            asks: self
                .asks
                .iter()
                .map(|(&price_level, &quantity)| SnapshotAsksEntryDomain {
                    price_level,
                    quantity,
                })
                .collect(),
        })
    }
}
impl Engine {
    /// `fresh_ms` is a legacy, unused argument. Connection health belongs to
    /// the transport, which must report disconnects (including heartbeat loss).
    pub fn new(source_limit: usize, _fresh_ms: i64) -> Self {
        Self {
            symbols: BTreeMap::new(),
            source_symbols: BTreeMap::new(),
            resolved: BTreeSet::new(),
            source_limit,
            metrics: Metrics::default(),
        }
    }
    pub fn set_universe(&mut self, symbols: BTreeSet<String>) {
        self.retain_symbols(|s| symbols.contains(s.as_ref()));
        for s in symbols {
            // Existing book keys own their shared handles, so catalog refreshes
            // can skip the sharded interner lookup.
            if !self.resolved.contains(s.as_str()) && !self.symbols.contains_key(s.as_str()) {
                self.symbols.insert(intern(&s), Symbol::default());
            }
        }
    }
    /// Reconcile an already-interned catalog without allocating string keys.
    pub fn set_universe_assets(&mut self, symbols: BTreeSet<Asset>) {
        self.retain_symbols(|s| symbols.contains(s));
        for s in symbols {
            if !self.resolved.contains(s.as_ref()) {
                self.symbols.entry(s).or_default();
            }
        }
    }
    /// Add lifecycle-discovered symbols without scanning or replacing existing books.
    pub fn add_universe_assets(&mut self, symbols: impl IntoIterator<Item = Asset>) {
        for symbol in symbols {
            if !self.resolved.contains(symbol.as_ref()) {
                self.symbols.entry(symbol).or_default();
            }
        }
    }
    fn unindex(index: &mut BTreeMap<usize, BTreeSet<Asset>>, source: usize, symbol: &str) {
        if let std::collections::btree_map::Entry::Occupied(mut entry) = index.entry(source) {
            entry.get_mut().remove(symbol);
            if entry.get().is_empty() {
                entry.remove();
            }
        }
    }
    fn retain_symbols(&mut self, mut keep: impl FnMut(&Asset) -> bool) {
        self.symbols.retain(|symbol, state| {
            if keep(symbol) {
                return true;
            }
            for &source in state.books.keys() {
                Self::unindex(&mut self.source_symbols, source, symbol);
            }
            false
        });
    }
    pub fn symbol_count(&self) -> usize {
        self.symbols.len()
    }
    pub fn ready_count(&self, _now: i64) -> usize {
        self.symbols
            .values()
            .filter(|s| s.active.is_some_and(|id| s.books.contains_key(&id)))
            .count()
    }
    pub fn coverage(&self, _now: i64) -> Coverage {
        let mut coverage = Coverage {
            symbols: self.symbols.len(),
            ..Coverage::default()
        };
        for (symbol, state) in &self.symbols {
            coverage.replica_books += state.books.len();
            coverage.redundant_ready += usize::from(state.books.len() >= 2);
            coverage.ready +=
                usize::from(state.active.is_some_and(|id| state.books.contains_key(&id)));
            if state.active.is_none() && !state.books.is_empty() {
                coverage.watermark_blocked += 1;
            }
            if state.books.is_empty() {
                coverage.uninitialized += 1;
                if coverage.uninitialized_sample.len() < 8 {
                    coverage.uninitialized_sample.push(symbol.to_string());
                }
            }
        }
        coverage
    }
    fn select_replica(state: &mut Symbol) -> Option<usize> {
        let candidate = state
            .books
            .iter()
            .filter(|(_, b)| state.watermark.is_none_or(|t| b.timestamp >= t))
            .max_by_key(|(id, b)| (b.timestamp, std::cmp::Reverse(**id)))
            .map(|(&id, _)| id)?;
        state.active = Some(candidate);
        state.watermark = Some(state.books[&candidate].timestamp);
        Some(candidate)
    }
    fn promote(symbol: &str, state: &mut Symbol, now: i64) -> Option<Record> {
        let candidate = Self::select_replica(state)?;
        Some(state.books[&candidate].snapshot(symbol, now))
    }
    pub fn apply(
        &mut self,
        source: usize,
        event: Event,
        received: i64,
    ) -> anyhow::Result<Vec<Record>> {
        ensure!(
            source < self.source_limit,
            "source exceeds configured limit"
        );
        time(received)?;
        enum Update {
            Snapshot {
                bids: Vec<(i64, i64)>,
                asks: Vec<(i64, i64)>,
            },
            Delta {
                side: Side,
                price: i64,
                quantity: i64,
            },
        }
        let (symbol, timestamp, update) = match event {
            Event::Snapshot {
                symbol,
                timestamp_ms,
                bids,
                asks,
            } => (symbol, timestamp_ms, Update::Snapshot { bids, asks }),
            Event::Delta {
                symbol,
                timestamp_ms,
                side,
                price,
                quantity,
            } => (
                symbol,
                timestamp_ms,
                Update::Delta {
                    side,
                    price,
                    quantity,
                },
            ),
            Event::Resolved { symbols } => {
                for s in symbols {
                    if let Some((asset, state)) = self.symbols.remove_entry(s.as_str()) {
                        for &source in state.books.keys() {
                            Self::unindex(&mut self.source_symbols, source, &asset);
                        }
                        self.resolved.insert(asset);
                    } else if !self.resolved.contains(s.as_str()) {
                        self.resolved.insert(intern(&s));
                    }
                }
                return Ok(vec![]);
            }
            Event::NewMarket { .. } => return Ok(vec![]),
        };
        time(timestamp)?;
        // Only snapshots need a shared key for a possible first-book insertion.
        // Ordinary deltas retain exactly one symbol lookup and no registry lock.
        let asset = if matches!(&update, Update::Snapshot { .. }) {
            self.symbols
                .get_key_value(symbol.as_str())
                .map(|(asset, _)| asset.clone())
        } else {
            None
        };
        let Some(state) = self.symbols.get_mut(symbol.as_str()) else {
            return Ok(vec![]);
        };
        if state
            .books
            .get(&source)
            .is_some_and(|b| timestamp < b.timestamp)
        {
            state.books.remove(&source);
            Self::unindex(&mut self.source_symbols, source, &symbol);
            if state.active == Some(source) {
                state.active = None;
            }
            self.metrics.invalidations += 1;
            anyhow::bail!("regressing stream for {symbol}; source invalidated, snapshot required")
        }
        let is_snapshot = matches!(&update, Update::Snapshot { .. });
        let recovering = state.watermark.is_some();
        let mut changed = false;
        let delta_fields = match &update {
            Update::Delta {
                side,
                price,
                quantity,
                ..
            } => Some((*side, *price, *quantity)),
            _ => None,
        };
        match update {
            Update::Snapshot { bids, asks } => {
                let replacement = Book {
                    bids: bids.into_iter().collect(),
                    asks: asks.into_iter().collect(),
                    timestamp,
                };
                if let Some(prior) = state.books.get(&source) {
                    self.metrics.resnapshots += 1;
                    if prior.timestamp == replacement.timestamp {
                        if prior.bids == replacement.bids && prior.asks == replacement.asks {
                            self.metrics.same_timestamp_book_matches += 1;
                        } else {
                            self.metrics.same_timestamp_book_differences += 1;
                        }
                    }
                }
                if state.books.insert(source, replacement).is_none() {
                    self.source_symbols
                        .entry(source)
                        .or_default()
                        .insert(asset.expect("snapshot for registered symbol"));
                }
            }
            Update::Delta {
                side,
                price,
                quantity,
            } => {
                let Some(book) = state.books.get_mut(&source) else {
                    self.metrics.gated += 1;
                    if state.active.is_none()
                        && let Some(record) = Self::promote(&symbol, state, received)
                    {
                        self.metrics.snapshots += 1;
                        self.metrics.failovers += u64::from(recovering);
                        return Ok(vec![record]);
                    }
                    return Ok(vec![]);
                };
                let levels = if side == Side::Buy {
                    &mut book.bids
                } else {
                    &mut book.asks
                };
                let prior = levels.get(&price).copied().unwrap_or(0);
                changed = prior != quantity;
                if quantity == 0 {
                    levels.remove(&price);
                } else {
                    levels.insert(price, quantity);
                }
                book.timestamp = timestamp;
            }
        }
        if state.active.is_none() {
            if let Some(record) = Self::promote(&symbol, state, received) {
                self.metrics.snapshots += 1;
                self.metrics.failovers += u64::from(recovering);
                return Ok(vec![record]);
            }
            return Ok(vec![]);
        }
        if state.active != Some(source) {
            return Ok(vec![]);
        }
        state.watermark = Some(timestamp);
        if is_snapshot {
            self.metrics.snapshots += 1;
            return Ok(vec![state.books[&source].snapshot(&symbol, received)]);
        }
        if !changed {
            self.metrics.duplicates += 1;
            return Ok(vec![]);
        }
        let (side, price_level, quantity) = delta_fields.expect("delta event");
        self.metrics.deltas += 1;
        Ok(vec![Record::Delta(PriceLevelUpdateDomain {
            backtest_timestamp: time(received)?,
            exchange_timestamp: time(timestamp)?,
            symbol,
            side,
            price_level,
            quantity,
        })])
    }
    /// Invalidate only subscriptions being refreshed; retain every other book.
    pub fn reset(&mut self, source: usize, symbols: &BTreeSet<String>, now: i64) -> Vec<Record> {
        self.reset_symbols(source, symbols.iter().map(String::as_str), now)
    }
    /// Reset shared catalog handles without materializing a second string set.
    pub fn reset_assets(
        &mut self,
        source: usize,
        symbols: &BTreeSet<Asset>,
        now: i64,
    ) -> Vec<Record> {
        self.reset_symbols(source, symbols.iter().map(AsRef::as_ref), now)
    }
    fn reset_symbols<'a>(
        &mut self,
        source: usize,
        symbols: impl IntoIterator<Item = &'a str>,
        now: i64,
    ) -> Vec<Record> {
        if time(now).is_err() {
            return vec![];
        }
        let mut out = vec![];
        for symbol in symbols {
            let Some(state) = self.symbols.get_mut(symbol) else {
                continue;
            };
            if state.books.remove(&source).is_some() {
                Self::unindex(&mut self.source_symbols, source, symbol);
            }
            if state.active == Some(source) {
                state.active = None;
                self.metrics.invalidations += 1;
                if let Some(record) = Self::promote(symbol, state, now) {
                    self.metrics.failovers += 1;
                    self.metrics.snapshots += 1;
                    out.push(record);
                }
            }
        }
        out
    }
    /// Invalidate only books belonging to this physical source, emitting
    /// failover snapshots in lexical order with at most one output allocated.
    /// On callback error, stop emitting but finish invalidating every affected
    /// book and selecting eligible healthy replicas, then return the first
    /// error. The caller must abort ingestion after an output error: subsequent
    /// deltas may require failover snapshots that the sink did not accept.
    /// `snapshots` and the returned count include only accepted records;
    /// `failovers` counts all state promotions, even after a callback error.
    /// Invalid receive times leave all state untouched, matching `disconnect`.
    pub fn visit_disconnect<E>(
        &mut self,
        source: usize,
        now: i64,
        mut emit: impl FnMut(Record) -> Result<(), E>,
    ) -> Result<usize, E> {
        if time(now).is_err() {
            return Ok(0);
        }
        let Some(symbols) = self.source_symbols.remove(&source) else {
            return Ok(0);
        };
        let mut count = 0;
        let mut error = None;
        for symbol in symbols {
            self.metrics.disconnect_books_visited += 1;
            let Some(state) = self.symbols.get_mut(symbol.as_ref()) else {
                continue;
            };
            state.books.remove(&source);
            if state.active == Some(source) {
                state.active = None;
                self.metrics.invalidations += 1;
                if let Some(candidate) = Self::select_replica(state) {
                    self.metrics.failovers += 1;
                    if error.is_none() {
                        match emit(state.books[&candidate].snapshot(&symbol, now)) {
                            Ok(()) => {
                                self.metrics.snapshots += 1;
                                count += 1;
                            }
                            Err(err) => error = Some(err),
                        }
                    }
                }
            }
        }
        match error {
            Some(err) => Err(err),
            None => Ok(count),
        }
    }
    /// Collecting compatibility wrapper. Live disconnect emission should use
    /// `visit_disconnect` so failover output applies backpressure.
    pub fn disconnect(&mut self, source: usize, now: i64) -> Vec<Record> {
        let mut out = Vec::new();
        let result = self.visit_disconnect(source, now, |record| {
            out.push(record);
            Ok::<_, std::convert::Infallible>(())
        });
        match result {
            Ok(_) => out,
            Err(never) => match never {},
        }
    }
    /// Emit ready books in lexical symbol order, retaining at most one output
    /// record at a time. Stops immediately on callback failure; the returned
    /// count and snapshot metric include only accepted records. Promotions made
    /// before a callback failure remain in engine state. An invalid receive
    /// timestamp emits nothing, matching the legacy `snapshots` API.
    pub fn visit_snapshots<E>(
        &mut self,
        now: i64,
        mut emit: impl FnMut(Record) -> Result<(), E>,
    ) -> Result<usize, E> {
        if time(now).is_err() {
            return Ok(0);
        }
        let mut count = 0;
        for (symbol, state) in &mut self.symbols {
            let record = if state.active.is_none() {
                let recovering = state.watermark.is_some();
                let record = Self::promote(symbol, state, now);
                if record.is_some() {
                    self.metrics.failovers += u64::from(recovering);
                }
                record
            } else {
                state
                    .active
                    .and_then(|id| state.books.get(&id))
                    .map(|book| book.snapshot(symbol, now))
            };
            if let Some(record) = record {
                emit(record)?;
                count += 1;
                self.metrics.snapshots += 1;
            }
        }
        Ok(count)
    }
    /// Collecting compatibility wrapper for replay consumers and small tests.
    /// Live minute emission should call `visit_snapshots` to apply backpressure.
    pub fn snapshots(&mut self, now: i64) -> Vec<Record> {
        let mut out = Vec::new();
        let result = self.visit_snapshots(now, |record| {
            out.push(record);
            Ok::<_, std::convert::Infallible>(())
        });
        match result {
            Ok(_) => out,
            Err(never) => match never {},
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retired_physical_sources_do_not_accumulate_empty_index_sets() {
        let mut engine = Engine::new(usize::MAX, 0);
        engine.set_universe(["a".into()].into());
        for source in 0..128 {
            engine
                .apply(
                    source,
                    Event::Snapshot {
                        symbol: "a".into(),
                        timestamp_ms: 10,
                        bids: vec![(400_000, 5)],
                        asks: vec![],
                    },
                    10,
                )
                .unwrap();
            assert_eq!(engine.source_symbols.len(), 1);
            match source % 5 {
                0 => {
                    engine.disconnect(source, 11);
                }
                1 => {
                    engine.reset(source, &["a".into()].into(), 11);
                }
                2 => {
                    assert!(
                        engine
                            .apply(
                                source,
                                Event::Delta {
                                    symbol: "a".into(),
                                    timestamp_ms: 9,
                                    side: Side::Buy,
                                    price: 400_000,
                                    quantity: 99,
                                },
                                11
                            )
                            .is_err()
                    );
                }
                3 => {
                    engine.set_universe(Default::default());
                    engine.set_universe(["a".into()].into());
                }
                _ => {
                    engine.set_universe_assets(Default::default());
                    engine.set_universe_assets([intern("a")].into());
                }
            }
            assert!(engine.source_symbols.is_empty());
        }
        engine
            .apply(
                128,
                Event::Snapshot {
                    symbol: "a".into(),
                    timestamp_ms: 10,
                    bids: vec![],
                    asks: vec![],
                },
                10,
            )
            .unwrap();
        assert_eq!(engine.source_symbols.len(), 1);
        engine
            .apply(
                128,
                Event::Resolved {
                    symbols: vec!["a".into()],
                },
                11,
            )
            .unwrap();
        assert!(engine.source_symbols.is_empty());
    }
}
