use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use parking_lot::Mutex;

use crate::rewrite::SimpleFormat;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IndexCacheKey {
    pub project: String,
    pub format: SimpleFormat,
}

#[derive(Debug, Clone)]
pub struct CachedIndex {
    pub body: Bytes,
    pub content_type: String,
    pub proxy_etag: String,
    pub upstream_etag: Option<String>,
    pub last_serial: Option<String>,
    pub fetched_at: Instant,
}

pub enum IndexLookup {
    Fresh(Arc<CachedIndex>),
    Stale(Arc<CachedIndex>),
    Miss,
}

struct StoredIndex {
    entry: Arc<CachedIndex>,
    seq: u64,
    size: u64,
}

struct Inner {
    map: HashMap<IndexCacheKey, StoredIndex>,
    lru: BTreeSet<(u64, IndexCacheKey)>,
    total_bytes: u64,
}

/// Memory-bounded LRU + TTL cache for rewritten Simple API index responses.
pub struct IndexCache {
    inner: Mutex<Inner>,
    max_bytes: u64,
    ttl: Duration,
    seq: AtomicU64,
}

impl IndexCache {
    pub fn new(max_bytes: u64, ttl: Duration) -> Self {
        Self {
            inner: Mutex::new(Inner {
                map: HashMap::new(),
                lru: BTreeSet::new(),
                total_bytes: 0,
            }),
            max_bytes,
            ttl,
            seq: AtomicU64::new(1),
        }
    }

    pub fn lookup(&self, key: &IndexCacheKey) -> IndexLookup {
        if self.max_bytes == 0 {
            return IndexLookup::Miss;
        }
        let mut inner = self.inner.lock();
        let Some(stored) = inner.map.get_mut(key) else {
            return IndexLookup::Miss;
        };

        let old_seq = stored.seq;
        let new_seq = self.seq.fetch_add(1, Ordering::Relaxed);
        stored.seq = new_seq;
        let entry = Arc::clone(&stored.entry);

        inner.lru.remove(&(old_seq, key.clone()));
        inner.lru.insert((new_seq, key.clone()));

        if !self.ttl.is_zero() && entry.fetched_at.elapsed() < self.ttl {
            IndexLookup::Fresh(entry)
        } else {
            IndexLookup::Stale(entry)
        }
    }

    pub fn insert(&self, key: IndexCacheKey, entry: CachedIndex) -> Arc<CachedIndex> {
        let entry = Arc::new(entry);
        let size = entry.body.len() as u64;
        if self.max_bytes == 0 || size > self.max_bytes {
            return entry;
        }

        let mut inner = self.inner.lock();
        if let Some(prev) = inner.map.remove(&key) {
            inner.lru.remove(&(prev.seq, key.clone()));
            inner.total_bytes = inner.total_bytes.saturating_sub(prev.size);
        }

        while inner.total_bytes + size > self.max_bytes {
            let Some((_, oldest_key)) = inner.lru.pop_first() else {
                break;
            };
            if let Some(evicted) = inner.map.remove(&oldest_key) {
                inner.total_bytes = inner.total_bytes.saturating_sub(evicted.size);
            }
        }

        let seq = self.seq.fetch_add(1, Ordering::Relaxed);
        inner.total_bytes += size;
        inner.lru.insert((seq, key.clone()));
        inner.map.insert(
            key,
            StoredIndex {
                entry: Arc::clone(&entry),
                seq,
                size,
            },
        );
        entry
    }

    /// Updates `fetched_at` on an existing entry after upstream confirms `304 Not Modified`.
    pub fn mark_revalidated(&self, key: &IndexCacheKey) -> Option<Arc<CachedIndex>> {
        let mut inner = self.inner.lock();
        let stored = inner.map.get_mut(key)?;
        let mut updated = (*stored.entry).clone();
        updated.fetched_at = Instant::now();
        let arc = Arc::new(updated);
        let old_seq = stored.seq;
        let new_seq = self.seq.fetch_add(1, Ordering::Relaxed);
        stored.entry = Arc::clone(&arc);
        stored.seq = new_seq;

        inner.lru.remove(&(old_seq, key.clone()));
        inner.lru.insert((new_seq, key.clone()));
        Some(arc)
    }

    pub fn stats(&self) -> (usize, u64) {
        let inner = self.inner.lock();
        (inner.map.len(), inner.total_bytes)
    }
}

impl Ord for IndexCacheKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (&self.project, self.format.cache_tag()).cmp(&(&other.project, other.format.cache_tag()))
    }
}

impl PartialOrd for IndexCacheKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_entry(payload: &'static [u8]) -> CachedIndex {
        CachedIndex {
            body: Bytes::from_static(payload),
            content_type: "application/vnd.pypi.simple.v1+json".into(),
            proxy_etag: "W/\"test\"".into(),
            upstream_etag: Some("\"up\"".into()),
            last_serial: None,
            fetched_at: Instant::now(),
        }
    }

    #[test]
    fn test_index_cache_lru_eviction() {
        let cache = IndexCache::new(10, Duration::from_secs(60));
        let k1 = IndexCacheKey {
            project: "a".into(),
            format: SimpleFormat::Json,
        };
        let k2 = IndexCacheKey {
            project: "b".into(),
            format: SimpleFormat::Json,
        };
        let k3 = IndexCacheKey {
            project: "c".into(),
            format: SimpleFormat::Json,
        };

        cache.insert(k1.clone(), make_entry(b"12345"));
        cache.insert(k2.clone(), make_entry(b"67890"));
        // Touch k1 so k2 is LRU
        assert!(matches!(cache.lookup(&k1), IndexLookup::Fresh(_)));
        // Insert k3 (5 bytes) -> evicts k2
        cache.insert(k3.clone(), make_entry(b"abcde"));

        assert!(matches!(cache.lookup(&k1), IndexLookup::Fresh(_)));
        assert!(matches!(cache.lookup(&k2), IndexLookup::Miss));
        assert!(matches!(cache.lookup(&k3), IndexLookup::Fresh(_)));
    }
}
