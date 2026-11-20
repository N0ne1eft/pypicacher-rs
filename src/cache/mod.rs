pub mod index_cache;

use std::collections::{BTreeSet, HashMap};
use std::fs::FileTimes;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::metrics::Metrics;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PackageMeta {
    pub key: String,
    pub hash: String,
    pub upstream_url: String,
    pub filename: String,
    pub content_type: String,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub size: u64,
    pub created_at_nanos: u128,
}

#[derive(Debug, Clone)]
pub struct CachedPackage {
    pub meta: PackageMeta,
    pub data_path: PathBuf,
}

struct CacheRecord {
    meta: PackageMeta,
    data_path: PathBuf,
    meta_path: PathBuf,
    last_accessed_nanos: u128,
    seq: u64,
}

struct CacheState {
    entries: HashMap<String, CacheRecord>,
    /// Ordered oldest -> newest by `(last_accessed_nanos, seq, hash)`.
    lru: BTreeSet<(u128, u64, String)>,
    /// Pending on-disk mtime updates flushed asynchronously to keep hot path syscall-free.
    dirty_atimes: HashMap<PathBuf, SystemTime>,
    total_bytes: u64,
    last_ts_nanos: u128,
}

pub enum CommitOutcome {
    Committed(Box<CachedPackage>),
    SkippedTooLarge,
}

/// High-performance sharded disk cache with exact in-memory LRU eviction by access time,
/// crash-safe atomic commits, and batched on-disk access-time persistence.
pub struct DiskCache {
    objects_dir: PathBuf,
    tmp_dir: PathBuf,
    max_size: u64,
    high_watermark_bytes: u64,
    low_watermark_bytes: u64,
    state: Mutex<CacheState>,
    seq: AtomicU64,
    metrics: Arc<Metrics>,
}

impl DiskCache {
    pub async fn open(
        cache_dir: &Path,
        max_size: u64,
        evict_high_watermark: f64,
        evict_low_watermark: f64,
        metrics: Arc<Metrics>,
    ) -> Result<Arc<Self>> {
        let objects_dir = cache_dir.join("objects");
        let tmp_dir = cache_dir.join("tmp");

        tokio::fs::create_dir_all(&objects_dir)
            .await
            .with_context(|| format!("failed to create {}", objects_dir.display()))?;
        tokio::fs::create_dir_all(&tmp_dir)
            .await
            .with_context(|| format!("failed to create {}", tmp_dir.display()))?;

        let high_watermark_bytes = ((max_size as f64) * evict_high_watermark).round() as u64;
        let high_watermark_bytes = high_watermark_bytes.min(max_size);
        let low_watermark_bytes = ((max_size as f64) * evict_low_watermark).round() as u64;
        let low_watermark_bytes = low_watermark_bytes.min(high_watermark_bytes);

        let objects_dir_clone = objects_dir.clone();
        let tmp_dir_clone = tmp_dir.clone();
        let (initial_state, next_seq, orphan_count) = tokio::task::spawn_blocking(move || {
            recover_and_scan(&objects_dir_clone, &tmp_dir_clone)
        })
        .await
        .context("cache startup scan task panicked")??;

        let initial_entries = initial_state.entries.len();
        let initial_bytes = initial_state.total_bytes;

        let cache = Arc::new(Self {
            objects_dir,
            tmp_dir,
            max_size,
            high_watermark_bytes,
            low_watermark_bytes,
            state: Mutex::new(initial_state),
            seq: AtomicU64::new(next_seq),
            metrics,
        });

        info!(
            cache_dir = %cache_dir.display(),
            entries = initial_entries,
            bytes = initial_bytes,
            max_size,
            high_watermark_bytes,
            low_watermark_bytes,
            cleaned_orphans = orphan_count,
            "disk cache initialized"
        );

        // Enforce quota immediately if max_size was lowered since previous run.
        if initial_bytes > high_watermark_bytes {
            cache.prune_to_target(low_watermark_bytes, None).await;
        }

        Ok(cache)
    }

    /// Computes a deterministic SHA-256 hex digest for a canonical upstream URL key.
    pub fn key_hash(cache_key: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(cache_key.as_bytes());
        hex::encode(hasher.finalize())
    }

    pub fn max_size(&self) -> u64 {
        self.max_size
    }

    pub fn new_temp_path(&self) -> PathBuf {
        self.tmp_dir.join(format!("{}.tmp", Uuid::new_v4()))
    }

    pub fn data_path_for_hash(&self, hash: &str) -> PathBuf {
        let shard = &hash[..2];
        self.objects_dir.join(shard).join(format!("{hash}.bin"))
    }

    pub fn meta_path_for_hash(&self, hash: &str) -> PathBuf {
        let shard = &hash[..2];
        self.objects_dir
            .join(shard)
            .join(format!("{hash}.meta.json"))
    }

    /// Looks up a package in the disk cache without updating access time (useful for `HEAD`).
    pub fn peek(&self, cache_key: &str) -> Option<CachedPackage> {
        let hash = Self::key_hash(cache_key);
        let state = self.state.lock();
        let rec = state.entries.get(&hash)?;
        Some(CachedPackage {
            meta: rec.meta.clone(),
            data_path: rec.data_path.clone(),
        })
    }

    /// Looks up a package in the disk cache and marks it as recently accessed (LRU touch).
    pub fn get_and_touch(&self, cache_key: &str) -> Option<CachedPackage> {
        let hash = Self::key_hash(cache_key);
        let mut state = self.state.lock();

        let now = SystemTime::now();
        let now_nanos = monotonic_nanos(&mut state.last_ts_nanos, now);
        let new_seq = self.seq.fetch_add(1, Ordering::Relaxed);

        let (old_ts, old_seq, data_path, meta) = {
            let rec = state.entries.get_mut(&hash)?;
            let old_ts = rec.last_accessed_nanos;
            let old_seq = rec.seq;
            rec.last_accessed_nanos = now_nanos;
            rec.seq = new_seq;
            (old_ts, old_seq, rec.data_path.clone(), rec.meta.clone())
        };

        state.lru.remove(&(old_ts, old_seq, hash.clone()));
        state.lru.insert((now_nanos, new_seq, hash));
        state.dirty_atimes.insert(data_path.clone(), now);

        Some(CachedPackage { meta, data_path })
    }

    /// Proactively prunes oldest packages when incoming download size would exceed quota.
    pub async fn make_room_for(&self, incoming_bytes: u64) {
        if incoming_bytes == 0 || incoming_bytes > self.max_size {
            return;
        }
        let need_prune = {
            let state = self.state.lock();
            state.total_bytes.saturating_add(incoming_bytes) > self.max_size
        };
        if need_prune {
            let max_allowed = self.max_size.saturating_sub(incoming_bytes);
            let target = max_allowed.min(self.low_watermark_bytes);
            self.prune_to_target(target, None).await;
        }
    }

    /// Atomically commits a completed temp file into the sharded cache and enforces LRU quota.
    pub async fn commit_temp_file(
        &self,
        tmp_path: &Path,
        mut meta: PackageMeta,
    ) -> Result<CommitOutcome> {
        if meta.size > self.max_size {
            debug!(
                key = %meta.key,
                size = meta.size,
                max_size = self.max_size,
                "package exceeds max_cache_size, skipping disk cache commit"
            );
            return Ok(CommitOutcome::SkippedTooLarge);
        }

        let hash = Self::key_hash(&meta.key);
        meta.hash = hash.clone();

        let shard_dir = self.objects_dir.join(&hash[..2]);
        tokio::fs::create_dir_all(&shard_dir)
            .await
            .with_context(|| format!("failed to create shard dir {}", shard_dir.display()))?;

        let data_path = shard_dir.join(format!("{hash}.bin"));
        let meta_path = shard_dir.join(format!("{hash}.meta.json"));
        let meta_tmp_path = self.tmp_dir.join(format!("{}.meta.tmp", Uuid::new_v4()));

        // Pre-prune before commit if needed to keep peak disk usage bounded.
        self.make_room_for(meta.size).await;

        // 1. Atomically move data file into place first.
        tokio::fs::rename(tmp_path, &data_path)
            .await
            .with_context(|| {
                format!(
                    "failed to rename {} -> {}",
                    tmp_path.display(),
                    data_path.display()
                )
            })?;

        // 2. Write metadata JSON to tmp and atomically rename into place last so
        // presence of .meta.json guarantees .bin is complete and intact.
        let meta_bytes = serde_json::to_vec(&meta)?;
        tokio::fs::write(&meta_tmp_path, &meta_bytes)
            .await
            .with_context(|| format!("failed to write {}", meta_tmp_path.display()))?;
        if let Err(err) = tokio::fs::rename(&meta_tmp_path, &meta_path).await {
            let _ = tokio::fs::remove_file(&meta_tmp_path).await;
            let _ = tokio::fs::remove_file(&data_path).await;
            return Err(err).with_context(|| {
                format!(
                    "failed to rename {} -> {}",
                    meta_tmp_path.display(),
                    meta_path.display()
                )
            });
        }

        // 3. Register in memory with newest access timestamp.
        let now = SystemTime::now();
        let should_prune = {
            let mut state = self.state.lock();
            let now_nanos = monotonic_nanos(&mut state.last_ts_nanos, now);
            let new_seq = self.seq.fetch_add(1, Ordering::Relaxed);

            if let Some(prev) = state.entries.remove(&hash) {
                state
                    .lru
                    .remove(&(prev.last_accessed_nanos, prev.seq, hash.clone()));
                state.total_bytes = state.total_bytes.saturating_sub(prev.meta.size);
            }

            state.total_bytes = state.total_bytes.saturating_add(meta.size);
            state.lru.insert((now_nanos, new_seq, hash.clone()));
            state.entries.insert(
                hash.clone(),
                CacheRecord {
                    meta: meta.clone(),
                    data_path: data_path.clone(),
                    meta_path,
                    last_accessed_nanos: now_nanos,
                    seq: new_seq,
                },
            );
            state.dirty_atimes.insert(data_path.clone(), now);

            state.total_bytes > self.high_watermark_bytes
        };

        // 4. Prune oldest entries if storage quota is running low (protecting just-committed key).
        if should_prune {
            self.prune_to_target(self.low_watermark_bytes, Some(&hash))
                .await;
        }

        Ok(CommitOutcome::Committed(Box::new(CachedPackage {
            meta,
            data_path,
        })))
    }

    /// Evicts least-recently-accessed packages until `total_bytes <= target_bytes`
    /// (excluding `protected_hash`, if provided).
    pub async fn prune_to_target(&self, target_bytes: u64, protected_hash: Option<&str>) {
        let victims = {
            let mut state = self.state.lock();
            if state.total_bytes <= target_bytes {
                return;
            }

            let mut victims = Vec::new();
            let mut skipped_protected = None;

            while state.total_bytes > target_bytes {
                let Some(oldest) = state.lru.pop_first() else {
                    break;
                };
                if protected_hash.is_some_and(|p| p == oldest.2) {
                    skipped_protected = Some(oldest);
                    continue;
                }
                if let Some(rec) = state.entries.remove(&oldest.2) {
                    state.total_bytes = state.total_bytes.saturating_sub(rec.meta.size);
                    state.dirty_atimes.remove(&rec.data_path);
                    victims.push((rec.meta.filename, rec.meta_path, rec.data_path, rec.meta.size));
                }
            }

            if let Some(prot) = skipped_protected {
                state.lru.insert(prot);
            }

            victims
        };

        if victims.is_empty() {
            return;
        }

        let mut evicted_count = 0u64;
        let mut evicted_bytes = 0u64;

        for (filename, meta_path, data_path, size) in victims {
            // Remove metadata first so crash between deletes leaves orphan .bin cleaned on restart.
            if let Err(err) = tokio::fs::remove_file(&meta_path).await
                && err.kind() != std::io::ErrorKind::NotFound
            {
                warn!(path = %meta_path.display(), error = %err, "failed to remove evicted metadata");
            }
            if let Err(err) = tokio::fs::remove_file(&data_path).await
                && err.kind() != std::io::ErrorKind::NotFound
            {
                warn!(path = %data_path.display(), error = %err, "failed to remove evicted package");
            }
            evicted_count += 1;
            evicted_bytes += size;
            debug!(filename = %filename, size, "evicted LRU package from disk cache");
        }

        self.metrics
            .evictions
            .fetch_add(evicted_count, Ordering::Relaxed);
        self.metrics
            .evicted_bytes
            .fetch_add(evicted_bytes, Ordering::Relaxed);

        let (remaining_count, remaining_bytes) = self.stats();
        info!(
            evicted_count,
            evicted_bytes,
            remaining_count,
            remaining_bytes,
            max_size = self.max_size,
            "completed LRU cache pruning"
        );
    }

    /// Flushes batched access-time updates onto `.bin` file mtimes so LRU order persists across restarts.
    pub async fn flush_access_times(&self) {
        let batch: Vec<(PathBuf, SystemTime)> = {
            let mut state = self.state.lock();
            if state.dirty_atimes.is_empty() {
                return;
            }
            state.dirty_atimes.drain().collect()
        };

        let _ = tokio::task::spawn_blocking(move || {
            for (path, ts) in batch {
                if let Ok(file) = std::fs::OpenOptions::new().write(true).open(&path) {
                    let times = FileTimes::new().set_modified(ts);
                    let _ = file.set_times(times);
                }
            }
        })
        .await;
    }

    /// Spawns a background task that periodically flushes dirty access timestamps to disk.
    pub fn spawn_background_flusher(
        self: &Arc<Self>,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> tokio::task::JoinHandle<()> {
        let cache = Arc::clone(self);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        cache.flush_access_times().await;
                    }
                    _ = shutdown.changed() => {
                        cache.flush_access_times().await;
                        break;
                    }
                }
            }
        })
    }

    pub fn stats(&self) -> (usize, u64) {
        let state = self.state.lock();
        (state.entries.len(), state.total_bytes)
    }
}

fn system_time_to_nanos(t: SystemTime) -> u128 {
    t.duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
}

fn monotonic_nanos(last_ts: &mut u128, now: SystemTime) -> u128 {
    let raw = system_time_to_nanos(now);
    let ts = raw.max(*last_ts);
    *last_ts = ts;
    ts
}

/// Scans `objects_dir` on startup, loads valid entries, and cleans up leftover temp/orphan files.
fn recover_and_scan(objects_dir: &Path, tmp_dir: &Path) -> Result<(CacheState, u64, usize)> {
    let mut orphan_count = 0usize;

    // 1. Clean up any incomplete temp files from previous runs.
    if let Ok(read_dir) = std::fs::read_dir(tmp_dir) {
        for entry in read_dir.flatten() {
            let path = entry.path();
            if path.is_file() && std::fs::remove_file(&path).is_ok() {
                orphan_count += 1;
            }
        }
    }

    let mut entries = HashMap::new();
    let mut lru = BTreeSet::new();
    let mut total_bytes = 0u64;
    let mut seq = 1u64;
    let mut max_ts = 0u128;

    let mut meta_files = Vec::new();
    let mut bin_files = Vec::new();

    if let Ok(shards) = std::fs::read_dir(objects_dir) {
        for shard_entry in shards.flatten() {
            let shard_path = shard_entry.path();
            if !shard_path.is_dir() {
                continue;
            }
            if let Ok(files) = std::fs::read_dir(&shard_path) {
                for file_entry in files.flatten() {
                    let path = file_entry.path();
                    if !path.is_file() {
                        continue;
                    }
                    let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                        continue;
                    };
                    if name.ends_with(".meta.json") {
                        meta_files.push(path);
                    } else if name.ends_with(".bin") {
                        bin_files.push(path);
                    } else if std::fs::remove_file(&path).is_ok() {
                        orphan_count += 1;
                    }
                }
            }
        }
    }

    for meta_path in meta_files {
        let Some(file_name) = meta_path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        let Some(hash) = file_name.strip_suffix(".meta.json") else {
            continue;
        };
        let data_path = meta_path.with_file_name(format!("{hash}.bin"));

        let Ok(raw_meta) = std::fs::read(&meta_path) else {
            let _ = std::fs::remove_file(&meta_path);
            orphan_count += 1;
            continue;
        };
        let Ok(mut meta) = serde_json::from_slice::<PackageMeta>(&raw_meta) else {
            let _ = std::fs::remove_file(&meta_path);
            let _ = std::fs::remove_file(&data_path);
            orphan_count += 1;
            continue;
        };
        let Ok(fs_meta) = std::fs::metadata(&data_path) else {
            let _ = std::fs::remove_file(&meta_path);
            orphan_count += 1;
            continue;
        };

        let actual_size = fs_meta.len();
        if actual_size != meta.size {
            let _ = std::fs::remove_file(&meta_path);
            let _ = std::fs::remove_file(&data_path);
            orphan_count += 1;
            continue;
        }

        meta.hash = hash.to_string();
        let mtime_nanos = fs_meta
            .modified()
            .map(system_time_to_nanos)
            .unwrap_or(meta.created_at_nanos);
        let last_accessed_nanos = mtime_nanos.max(meta.created_at_nanos);
        max_ts = max_ts.max(last_accessed_nanos);

        let cur_seq = seq;
        seq += 1;
        total_bytes = total_bytes.saturating_add(actual_size);
        lru.insert((last_accessed_nanos, cur_seq, hash.to_string()));
        entries.insert(
            hash.to_string(),
            CacheRecord {
                meta,
                data_path,
                meta_path,
                last_accessed_nanos,
                seq: cur_seq,
            },
        );
    }

    // Remove orphan .bin files whose .meta.json does not exist.
    for bin_path in bin_files {
        let Some(file_name) = bin_path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        let Some(hash) = file_name.strip_suffix(".bin") else {
            continue;
        };
        if !entries.contains_key(hash) && std::fs::remove_file(&bin_path).is_ok() {
            orphan_count += 1;
        }
    }

    Ok((
        CacheState {
            entries,
            lru,
            dirty_atimes: HashMap::new(),
            total_bytes,
            last_ts_nanos: max_ts,
        },
        seq,
        orphan_count,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn write_and_commit(cache: &DiskCache, key: &str, data: &[u8]) -> CommitOutcome {
        let tmp = cache.new_temp_path();
        tokio::fs::write(&tmp, data).await.unwrap();
        let meta = PackageMeta {
            key: key.to_string(),
            hash: String::new(),
            upstream_url: key.to_string(),
            filename: key.rsplit('/').next().unwrap_or("pkg.whl").to_string(),
            content_type: "application/octet-stream".to_string(),
            etag: None,
            last_modified: None,
            size: data.len() as u64,
            created_at_nanos: system_time_to_nanos(SystemTime::now()),
        };
        cache.commit_temp_file(&tmp, meta).await.unwrap()
    }

    #[tokio::test]
    async fn test_lru_eviction_by_access_time() {
        let dir = tempfile::tempdir().unwrap();
        let metrics = Arc::new(Metrics::default());
        // Max size 100 bytes, high=1.0, low=0.85
        let cache = DiskCache::open(dir.path(), 100, 1.0, 0.85, metrics.clone())
            .await
            .unwrap();

        write_and_commit(&cache, "https://example.com/a.whl", &[1u8; 40]).await;
        write_and_commit(&cache, "https://example.com/b.whl", &[2u8; 40]).await;
        assert_eq!(cache.stats(), (2, 80));

        // Access a.whl so b.whl becomes the least recently accessed
        assert!(cache.get_and_touch("https://example.com/a.whl").is_some());

        // Commit c.whl (40 bytes -> 120 > 100): should evict b.whl, keeping a.whl & c.whl (80 <= 85)
        write_and_commit(&cache, "https://example.com/c.whl", &[3u8; 40]).await;

        assert!(cache.peek("https://example.com/a.whl").is_some());
        assert!(cache.peek("https://example.com/b.whl").is_none());
        assert!(cache.peek("https://example.com/c.whl").is_some());
        assert_eq!(cache.stats(), (2, 80));
        assert_eq!(metrics.evictions.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn test_lru_persists_across_restart() {
        let dir = tempfile::tempdir().unwrap();
        let metrics = Arc::new(Metrics::default());

        {
            let cache = DiskCache::open(dir.path(), 100, 1.0, 0.85, metrics.clone())
                .await
                .unwrap();
            write_and_commit(&cache, "https://example.com/a.whl", &[1u8; 40]).await;
            tokio::time::sleep(Duration::from_millis(20)).await;
            write_and_commit(&cache, "https://example.com/b.whl", &[2u8; 40]).await;
            tokio::time::sleep(Duration::from_millis(20)).await;
            // Access a.whl AFTER b.whl and flush mtime to disk
            assert!(cache.get_and_touch("https://example.com/a.whl").is_some());
            cache.flush_access_times().await;
        }

        // Reopen cache with smaller quota (50 bytes): b.whl (older access time) must be evicted!
        let cache = DiskCache::open(dir.path(), 50, 1.0, 1.0, metrics.clone())
            .await
            .unwrap();
        assert!(cache.peek("https://example.com/a.whl").is_some());
        assert!(cache.peek("https://example.com/b.whl").is_none());
        assert_eq!(cache.stats(), (1, 40));
    }

    #[tokio::test]
    async fn test_oversized_file_skipped_without_wiping_cache() {
        let dir = tempfile::tempdir().unwrap();
        let metrics = Arc::new(Metrics::default());
        let cache = DiskCache::open(dir.path(), 100, 0.95, 0.85, metrics)
            .await
            .unwrap();

        write_and_commit(&cache, "https://example.com/small.whl", &[1u8; 40]).await;
        let outcome = write_and_commit(&cache, "https://example.com/huge.whl", &[9u8; 150]).await;
        assert!(matches!(outcome, CommitOutcome::SkippedTooLarge));
        assert!(cache.peek("https://example.com/small.whl").is_some());
        assert!(cache.peek("https://example.com/huge.whl").is_none());
    }
}
