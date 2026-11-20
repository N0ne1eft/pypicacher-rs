use std::sync::atomic::{AtomicU64, Ordering};

use serde::Serialize;

#[derive(Debug, Default)]
pub struct Metrics {
    pub package_hits: AtomicU64,
    pub package_misses: AtomicU64,
    pub package_coalesced: AtomicU64,
    pub package_range_bypasses: AtomicU64,
    pub index_hits: AtomicU64,
    pub index_misses: AtomicU64,
    pub index_revalidated: AtomicU64,
    pub evictions: AtomicU64,
    pub evicted_bytes: AtomicU64,
    pub bytes_served_from_cache: AtomicU64,
    pub bytes_downloaded_from_upstream: AtomicU64,
}

#[derive(Debug, Clone, Serialize)]
pub struct StatsSnapshot {
    pub cached_packages: usize,
    pub cache_bytes_used: u64,
    pub cache_max_bytes: u64,
    pub cache_usage_ratio: f64,
    pub in_flight_downloads: usize,
    pub index_cache_entries: usize,
    pub index_cache_bytes: u64,
    pub package_hits: u64,
    pub package_misses: u64,
    pub package_coalesced: u64,
    pub package_range_bypasses: u64,
    pub index_hits: u64,
    pub index_misses: u64,
    pub index_revalidated: u64,
    pub evictions: u64,
    pub evicted_bytes: u64,
    pub bytes_served_from_cache: u64,
    pub bytes_downloaded_from_upstream: u64,
}

impl Metrics {
    pub fn snapshot(
        &self,
        cached_packages: usize,
        cache_bytes_used: u64,
        cache_max_bytes: u64,
        in_flight_downloads: usize,
        index_cache_entries: usize,
        index_cache_bytes: u64,
    ) -> StatsSnapshot {
        let ratio = if cache_max_bytes > 0 {
            cache_bytes_used as f64 / cache_max_bytes as f64
        } else {
            0.0
        };
        StatsSnapshot {
            cached_packages,
            cache_bytes_used,
            cache_max_bytes,
            cache_usage_ratio: ratio,
            in_flight_downloads,
            index_cache_entries,
            index_cache_bytes,
            package_hits: self.package_hits.load(Ordering::Relaxed),
            package_misses: self.package_misses.load(Ordering::Relaxed),
            package_coalesced: self.package_coalesced.load(Ordering::Relaxed),
            package_range_bypasses: self.package_range_bypasses.load(Ordering::Relaxed),
            index_hits: self.index_hits.load(Ordering::Relaxed),
            index_misses: self.index_misses.load(Ordering::Relaxed),
            index_revalidated: self.index_revalidated.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
            evicted_bytes: self.evicted_bytes.load(Ordering::Relaxed),
            bytes_served_from_cache: self.bytes_served_from_cache.load(Ordering::Relaxed),
            bytes_downloaded_from_upstream: self
                .bytes_downloaded_from_upstream
                .load(Ordering::Relaxed),
        }
    }
}
