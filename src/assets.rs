//! Shared asset identifiers. Persistent catalog/book keys should clone an
//! `Asset`; frame lookups should borrow `&str` from their existing map instead
//! of calling `intern` on every update.
//!
//! The registry stores one strong handle per distinct registered identifier,
//! with no second owned string key. It is a lifecycle cache, not a fixed-size
//! cache: between `prune` calls it retains the union of identifiers registered
//! since the previous prune and identifiers still held by consumers. Call
//! `prune` after catalog reconciliation/removal and when dropping a collector.
//! A prune removes cache-only handles and releases spare hash-table capacity;
//! live identifiers keep their shared allocation. Its cost is linear in the
//! registry size, so it belongs at catalog boundaries, never per frame.
use std::{
    collections::{HashSet, hash_map::DefaultHasher},
    hash::{Hash, Hasher},
    sync::{Arc, OnceLock, RwLock},
};

pub type Asset = Arc<str>;
const SHARDS: usize = 32;

struct Interner {
    shards: [RwLock<HashSet<Asset>>; SHARDS],
}
impl Interner {
    fn new() -> Self {
        Self {
            shards: std::array::from_fn(|_| RwLock::new(HashSet::new())),
        }
    }
    fn intern(&self, value: &str) -> Asset {
        let mut hash = DefaultHasher::new();
        value.hash(&mut hash);
        let shard = &self.shards[hash.finish() as usize % SHARDS];
        if let Some(asset) = shard.read().expect("asset registry poisoned").get(value) {
            return Arc::clone(asset);
        }
        // Recheck under the write lock: another registrant may have won.
        let mut entries = shard.write().expect("asset registry poisoned");
        if let Some(asset) = entries.get(value) {
            return Arc::clone(asset);
        }
        let asset: Asset = Arc::from(value);
        entries.insert(Arc::clone(&asset));
        asset
    }
    fn prune(&self) -> usize {
        let mut removed = 0;
        for shard in &self.shards {
            let mut entries = shard.write().expect("asset registry poisoned");
            let before = entries.len();
            entries.retain(|asset| Arc::strong_count(asset) > 1);
            removed += before - entries.len();
            entries.shrink_to_fit();
        }
        removed
    }
}
fn registry() -> &'static Interner {
    static REGISTRY: OnceLock<Interner> = OnceLock::new();
    REGISTRY.get_or_init(Interner::new)
}

/// Return the shared allocation for this identifier. Registration uses a
/// sharded read lock (and a write lock only on insertion); hot-path book lookups
/// should use `map.get(symbol.as_str())`, without registry access.
pub fn intern(value: &str) -> Asset {
    registry().intern(value)
}

/// Remove handles held only by the registry. Returns the number removed.
/// Concurrent registration is safe; an external handle keeps its allocation
/// alive and internable. Consumers dropped during a sweep may wait until the
/// next sweep, so callers should prune again after their final teardown.
pub fn prune() -> usize {
    registry().prune()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_allocation_survives_prune_until_last_consumer_drops() {
        let registry = Interner::new();
        let asset = registry.intern("12345678901234567890");
        let other = registry.intern(&asset);
        assert!(Arc::ptr_eq(&asset, &other));
        assert_eq!(registry.prune(), 0);
        drop(other);
        assert!(Arc::ptr_eq(&asset, &registry.intern(&asset)));
        let weak = Arc::downgrade(&asset);
        drop(asset);
        assert_eq!(registry.prune(), 1);
        assert!(weak.upgrade().is_none());
        assert_eq!(registry.prune(), 0);
    }

    #[test]
    fn catalog_churn_releases_obsolete_identifiers_at_each_boundary() {
        let registry = Interner::new();
        let persistent = registry.intern("persistent");
        for generation in 0..3 {
            let catalog: Vec<_> = (0..1024)
                .map(|n| registry.intern(&format!("generation-{generation}-{n}")))
                .collect();
            let probes: Vec<_> = catalog.iter().map(Arc::downgrade).collect();
            drop(catalog);
            assert_eq!(registry.prune(), 1024);
            assert!(probes.iter().all(|probe| probe.upgrade().is_none()));
            assert!(Arc::ptr_eq(&persistent, &registry.intern("persistent")));
        }
        drop(persistent);
        assert_eq!(registry.prune(), 1);
    }

    #[test]
    fn concurrent_registration_uses_one_allocation() {
        let registry = Interner::new();
        std::thread::scope(|scope| {
            let handles: Vec<_> = (0..8)
                .map(|_| scope.spawn(|| registry.intern("same asset")))
                .collect();
            let assets: Vec<_> = handles.into_iter().map(|t| t.join().unwrap()).collect();
            assert!(assets.iter().all(|a| Arc::ptr_eq(a, &assets[0])));
            assert_eq!(registry.prune(), 0);
        });
        assert_eq!(registry.prune(), 1);
    }
}
