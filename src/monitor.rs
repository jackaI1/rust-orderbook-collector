//! Pure book-progress monitoring. All timestamps use one monotonic millisecond
//! origin, starting at zero; callers supply only validated snapshot/delta events.

use crate::assets::{Asset, intern};
use std::collections::HashMap;

pub struct Monitor {
    symbols: Vec<Symbol>,
    index: HashMap<Asset, usize>,
    stale_ms: u64,
    window_ms: u64,
    cooldown_ms: u64,
    cursor: usize,
}

struct Symbol {
    name: Asset,
    // Before initialization this is the subscription/reset time. Deltas must
    // not move that deadline until a snapshot has established a usable book.
    last_progress: u64,
    rate: Option<Rate>,
    last_recovery: Option<u64>,
}

struct Rate {
    start: u64,
    events: u64,
    baseline: u128,
    baseline_windows: u8,
    low_windows: u8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reason {
    Unsynchronized,
    Silent,
    RateDrop,
}

impl Monitor {
    pub fn new(symbols: impl IntoIterator<Item = String>, stale_ms: u64, window_ms: u64) -> Self {
        let mut monitor = Self {
            symbols: Vec::new(),
            index: HashMap::new(),
            stale_ms,
            window_ms: window_ms.max(1),
            cooldown_ms: stale_ms.max(30_000),
            cursor: 0,
        };
        for name in symbols {
            let name = intern(&name);
            if monitor.index.contains_key(&name) {
                continue;
            }
            monitor.index.insert(name.clone(), monitor.symbols.len());
            monitor.symbols.push(Symbol {
                name,
                last_progress: 0,
                rate: None,
                last_recovery: None,
            });
        }
        monitor
    }

    /// Begin monitoring only when an initial subscription batch is sent.
    pub fn subscribe(&mut self, name: String, now_ms: u64) {
        self.subscribe_asset(intern(&name), now_ms);
    }
    pub fn subscribe_asset(&mut self, name: Asset, now_ms: u64) {
        if self.index.contains_key(&name) {
            return;
        }
        self.index.insert(name.clone(), self.symbols.len());
        self.symbols.push(Symbol {
            name,
            last_progress: now_ms,
            rate: None,
            last_recovery: None,
        });
    }
    /// Remove a subscription without retaining its name or activity state.
    pub fn unsubscribe(&mut self, name: &str) {
        let Some(index) = self.index.remove(name) else {
            return;
        };
        self.symbols.swap_remove(index);
        if let Some(moved) = self.symbols.get(index) {
            self.index.insert(moved.name.clone(), index);
        }
        self.cursor = if self.symbols.is_empty() {
            0
        } else {
            self.cursor % self.symbols.len()
        };
    }

    /// Record one valid book-progress event, never a trade or transport heartbeat.
    pub fn observe(&mut self, symbol: &str, snapshot: bool, now_ms: u64) {
        let Some(&index) = self.index.get(symbol) else {
            return;
        };
        let symbol = &mut self.symbols[index];
        if !snapshot
            && symbol.rate.is_some()
            && now_ms.saturating_sub(symbol.last_progress) > self.stale_ms
        {
            symbol.rate = None;
            return;
        }
        if symbol.rate.is_none() && snapshot {
            symbol.rate = Some(Rate {
                start: now_ms,
                events: 0,
                baseline: 0,
                baseline_windows: 0,
                low_windows: 0,
            });
        }
        if let Some(rate) = &mut symbol.rate {
            rate.advance(now_ms, self.window_ms);
            rate.events = rate.events.saturating_add(1);
            symbol.last_progress = now_ms;
        }
    }

    /// Select recovery work fairly and immediately reserve its cooldown.
    pub fn due(&mut self, now_ms: u64, limit: usize) -> Vec<(String, Reason)> {
        let mut due = Vec::new();
        if limit == 0 {
            return due;
        }
        for uninitialized_first in [true, false] {
            for _ in 0..self.symbols.len() {
                let index = self.cursor;
                self.cursor = (self.cursor + 1) % self.symbols.len();
                let symbol = &mut self.symbols[index];
                if symbol.rate.is_none() != uninitialized_first {
                    continue;
                }
                if let Some(rate) = &mut symbol.rate {
                    rate.advance(now_ms, self.window_ms);
                }
                if symbol
                    .last_recovery
                    .is_some_and(|last| now_ms.saturating_sub(last) < self.cooldown_ms)
                {
                    continue;
                }
                let reason = if now_ms.saturating_sub(symbol.last_progress) >= self.stale_ms {
                    Some(if symbol.rate.is_some() {
                        Reason::Silent
                    } else {
                        Reason::Unsynchronized
                    })
                } else if symbol
                    .rate
                    .as_ref()
                    .is_some_and(|rate| rate.low_windows >= 2)
                {
                    Some(Reason::RateDrop)
                } else {
                    None
                };
                if let Some(reason) = reason {
                    symbol.last_recovery = Some(now_ms);
                    due.push((symbol.name.to_string(), reason));
                    if due.len() == limit {
                        return due;
                    }
                }
            }
        }
        due
    }

    /// Invalidate the book and learned rate, retaining any recovery cooldown.
    pub fn reset(&mut self, symbol: &str, now_ms: u64) {
        if let Some(&index) = self.index.get(symbol) {
            let symbol = &mut self.symbols[index];
            symbol.last_progress = now_ms;
            symbol.rate = None;
        }
    }
}

impl Rate {
    fn advance(&mut self, now_ms: u64, window_ms: u64) {
        let windows = now_ms.saturating_sub(self.start) / window_ms;
        if windows == 0 {
            return;
        }
        self.complete(self.events);
        // Empty windows reach a fixed point after at most three warmup windows
        // and two low windows. Huge clock gaps therefore take constant work.
        for _ in 0..(windows - 1).min(5) {
            self.complete(0);
        }
        self.start += windows * window_ms;
        self.events = 0;
    }

    fn complete(&mut self, events: u64) {
        let events = u128::from(events);
        if self.baseline_windows < 3 {
            self.baseline += events;
            self.baseline_windows += 1;
            if self.baseline_windows == 3 {
                self.baseline /= 3;
            }
        } else if events * 5 < self.baseline {
            // Freeze the expectation during a drop so sustained low traffic
            // cannot teach the monitor to accept a potentially broken feed.
            if self.baseline >= 20 {
                self.low_windows = (self.low_windows + 1).min(2);
            }
        } else {
            self.low_windows = 0;
            self.baseline = (self.baseline * 7 + events) / 8;
        }
    }
}
