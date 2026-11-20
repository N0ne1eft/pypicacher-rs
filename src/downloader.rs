use std::collections::HashMap;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use futures_util::{Stream, StreamExt, stream};
use http::{StatusCode, header};
use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{Notify, watch};
use tracing::{debug, warn};
use url::Url;

use crate::cache::{CommitOutcome, DiskCache, PackageMeta};
use crate::metrics::Metrics;
use crate::upstream::UpstreamClients;

const READ_BUF_SIZE: usize = 256 * 1024; // 256 KiB LAN-friendly streaming buffer

#[derive(Debug, Clone)]
pub struct UpstreamMeta {
    pub status: StatusCode,
    pub content_type: String,
    pub content_length: Option<u64>,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
}

/// Ensures temporary download files are deleted once all readers and the downloader drop them,
/// unless the file was atomically committed into the disk cache.
pub struct TempFileGuard {
    pub path: PathBuf,
    committed: AtomicBool,
}

impl TempFileGuard {
    fn new(path: PathBuf) -> Self {
        Self {
            path,
            committed: AtomicBool::new(false),
        }
    }

    fn mark_committed(&self) {
        self.committed.store(true, Ordering::Release);
    }
}

impl Drop for TempFileGuard {
    fn drop(&mut self) {
        if !self.committed.load(Ordering::Acquire) {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

#[derive(Clone)]
enum DownloadPhase {
    Connecting,
    Streaming {
        meta: UpstreamMeta,
        temp_file: Arc<TempFileGuard>,
        written: u64,
    },
    Completed {
        meta: UpstreamMeta,
        written: u64,
        final_path: Option<PathBuf>,
        temp_file: Arc<TempFileGuard>,
    },
    OversizedPassthrough {
        meta: UpstreamMeta,
        response_slot: Arc<Mutex<Option<reqwest::Response>>>,
    },
    UpstreamNonOk {
        status: StatusCode,
        content_type: Option<String>,
        body: Bytes,
    },
    Failed(String),
}

struct InFlightDownload {
    id: u64,
    progress_rx: watch::Receiver<DownloadPhase>,
    active_readers: AtomicUsize,
    cancel_notify: Notify,
}

/// RAII guard tracking how many downstream clients are currently reading an in-flight download.
pub struct ReaderGuard {
    handle: Arc<InFlightDownload>,
}

impl Drop for ReaderGuard {
    fn drop(&mut self) {
        if self.handle.active_readers.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.handle.cancel_notify.notify_one();
        }
    }
}

pub type BoxedByteStream =
    std::pin::Pin<Box<dyn Stream<Item = Result<Bytes, io::Error>> + Send + 'static>>;

pub enum DownloadOutcome {
    /// Live stream (backed by in-flight temp file or direct passthrough for oversized packages).
    Stream {
        meta: UpstreamMeta,
        body: BoxedByteStream,
        coalesced: bool,
        cache_bypass: bool,
    },
    /// Upstream returned non-200 status (e.g. 404 Not Found).
    UpstreamNonOk {
        status: StatusCode,
        content_type: Option<String>,
        body: Bytes,
    },
    /// Upstream or I/O failure.
    Error(String),
}

/// Coordinates singleflight upstream downloads with simultaneous streaming to 1..N downstream
/// clients and atomic disk cache commits.
pub struct DownloadManager {
    in_flight: Mutex<HashMap<String, Arc<InFlightDownload>>>,
    next_id: std::sync::atomic::AtomicU64,
    cache: Arc<DiskCache>,
    upstream: UpstreamClients,
    metrics: Arc<Metrics>,
}

impl DownloadManager {
    pub fn new(cache: Arc<DiskCache>, upstream: UpstreamClients, metrics: Arc<Metrics>) -> Arc<Self> {
        Arc::new(Self {
            in_flight: Mutex::new(HashMap::new()),
            next_id: std::sync::atomic::AtomicU64::new(1),
            cache,
            upstream,
            metrics,
        })
    }

    pub fn in_flight_count(&self) -> usize {
        self.in_flight.lock().len()
    }

    /// Joins an existing in-flight download for `cache_key` or starts a new one,
    /// returning a live streaming response as soon as upstream headers arrive.
    pub async fn fetch_and_stream(
        self: &Arc<Self>,
        cache_key: String,
        upstream_url: Url,
        filename: String,
    ) -> DownloadOutcome {
        let (handle, reader_guard, coalesced) = self.acquire_or_spawn(
            cache_key.clone(),
            upstream_url.clone(),
            filename.clone(),
        );

        let mut progress_rx = handle.progress_rx.clone();

        // Wait until upstream headers arrive (or fail).
        let phase = loop {
            let current = progress_rx.borrow_and_update().clone();
            match current {
                DownloadPhase::Connecting => {
                    if progress_rx.changed().await.is_err() {
                        return DownloadOutcome::Error(
                            "upstream download task terminated before headers".to_string(),
                        );
                    }
                }
                other => break other,
            }
        };

        match phase {
            DownloadPhase::Streaming {
                meta, temp_file, ..
            } => match open_tail_file(&temp_file.path, &mut progress_rx).await {
                Ok(file) => {
                    let body = build_tail_stream(file, progress_rx, reader_guard, temp_file);
                    DownloadOutcome::Stream {
                        meta,
                        body,
                        coalesced,
                        cache_bypass: false,
                    }
                }
                Err(err) => DownloadOutcome::Error(format!("failed to open stream buffer: {err}")),
            },
            DownloadPhase::Completed {
                meta,
                final_path,
                temp_file,
                ..
            } => {
                let path = final_path.as_deref().unwrap_or(&temp_file.path);
                match tokio::fs::File::open(path).await {
                    Ok(file) => {
                        let body = build_tail_stream(file, progress_rx, reader_guard, temp_file);
                        DownloadOutcome::Stream {
                            meta,
                            body,
                            coalesced,
                            cache_bypass: final_path.is_none(),
                        }
                    }
                    Err(err) => {
                        DownloadOutcome::Error(format!("failed to open completed file: {err}"))
                    }
                }
            }
            DownloadPhase::OversizedPassthrough {
                meta,
                response_slot,
            } => {
                drop(reader_guard);
                let taken = response_slot.lock().take();
                if let Some(resp) = taken {
                    let metrics = Arc::clone(&self.metrics);
                    let body: BoxedByteStream = Box::pin(resp.bytes_stream().map(move |res| {
                        match res {
                            Ok(bytes) => {
                                metrics
                                    .bytes_downloaded_from_upstream
                                    .fetch_add(bytes.len() as u64, Ordering::Relaxed);
                                Ok(bytes)
                            }
                            Err(err) => Err(io::Error::other(err)),
                        }
                    }));
                    DownloadOutcome::Stream {
                        meta,
                        body,
                        coalesced: false,
                        cache_bypass: true,
                    }
                } else {
                    // Another coalesced caller already took the direct stream; perform a direct passthrough.
                    self.direct_passthrough(upstream_url).await
                }
            }
            DownloadPhase::UpstreamNonOk {
                status,
                content_type,
                body,
            } => DownloadOutcome::UpstreamNonOk {
                status,
                content_type,
                body,
            },
            DownloadPhase::Failed(msg) => DownloadOutcome::Error(msg),
            DownloadPhase::Connecting => unreachable!(),
        }
    }

    async fn direct_passthrough(&self, upstream_url: Url) -> DownloadOutcome {
        let resp = match self.upstream.package_client.get(upstream_url).send().await {
            Ok(r) => r,
            Err(err) => return DownloadOutcome::Error(format!("upstream request failed: {err}")),
        };
        if resp.status() != StatusCode::OK {
            let status = resp.status();
            let content_type = resp
                .headers()
                .get(header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .map(str::to_string);
            let body = resp.bytes().await.unwrap_or_default();
            return DownloadOutcome::UpstreamNonOk {
                status,
                content_type,
                body,
            };
        }

        let meta = extract_upstream_meta(&resp);
        let metrics = Arc::clone(&self.metrics);
        let body: BoxedByteStream = Box::pin(resp.bytes_stream().map(move |res| match res {
            Ok(bytes) => {
                metrics
                    .bytes_downloaded_from_upstream
                    .fetch_add(bytes.len() as u64, Ordering::Relaxed);
                Ok(bytes)
            }
            Err(err) => Err(io::Error::other(err)),
        }));
        DownloadOutcome::Stream {
            meta,
            body,
            coalesced: false,
            cache_bypass: true,
        }
    }

    fn acquire_or_spawn(
        self: &Arc<Self>,
        cache_key: String,
        upstream_url: Url,
        filename: String,
    ) -> (Arc<InFlightDownload>, ReaderGuard, bool) {
        let mut map = self.in_flight.lock();
        if let Some(existing) = map.get(&cache_key) {
            existing.active_readers.fetch_add(1, Ordering::AcqRel);
            let handle = Arc::clone(existing);
            let guard = ReaderGuard {
                handle: Arc::clone(&handle),
            };
            return (handle, guard, true);
        }

        let (progress_tx, progress_rx) = watch::channel(DownloadPhase::Connecting);
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let handle = Arc::new(InFlightDownload {
            id,
            progress_rx,
            active_readers: AtomicUsize::new(1),
            cancel_notify: Notify::new(),
        });
        map.insert(cache_key.clone(), Arc::clone(&handle));
        drop(map);

        let guard = ReaderGuard {
            handle: Arc::clone(&handle),
        };

        let manager = Arc::clone(self);
        let handle_for_task = Arc::clone(&handle);
        tokio::spawn(async move {
            manager
                .run_download(cache_key, upstream_url, filename, handle_for_task, progress_tx)
                .await;
        });

        (handle, guard, false)
    }

    fn remove_in_flight(&self, cache_key: &str, handle_id: u64) {
        let mut map = self.in_flight.lock();
        if map.get(cache_key).is_some_and(|h| h.id == handle_id) {
            map.remove(cache_key);
        }
    }

    /// Atomically verifies `active_readers == 0` under the `in_flight` lock and removes the
    /// entry before aborting so no late-joining client can attach to an aborted download.
    fn try_cancel_if_unused(&self, cache_key: &str, handle: &InFlightDownload) -> bool {
        let mut map = self.in_flight.lock();
        if handle.active_readers.load(Ordering::Acquire) == 0 {
            if map.get(cache_key).is_some_and(|h| h.id == handle.id) {
                map.remove(cache_key);
            }
            return true;
        }
        false
    }

    async fn run_download(
        self: Arc<Self>,
        cache_key: String,
        upstream_url: Url,
        filename: String,
        handle: Arc<InFlightDownload>,
        progress_tx: watch::Sender<DownloadPhase>,
    ) {
        // 1. Send upstream GET request (cancellable if all waiting clients disconnect).
        let send_fut = self.upstream.package_client.get(upstream_url.clone()).send();
        tokio::pin!(send_fut);

        let resp = loop {
            tokio::select! {
                res = &mut send_fut => {
                    match res {
                        Ok(r) => break r,
                        Err(err) => {
                            let msg = format!("upstream fetch failed: {err}");
                            warn!(url = %upstream_url, error = %err, "upstream package fetch failed");
                            let _ = progress_tx.send(DownloadPhase::Failed(msg));
                            self.remove_in_flight(&cache_key, handle.id);
                            return;
                        }
                    }
                }
                _ = handle.cancel_notify.notified() => {
                    if self.try_cancel_if_unused(&cache_key, &handle) {
                        debug!(url = %upstream_url, "all clients disconnected during connect; aborting");
                        let _ = progress_tx.send(DownloadPhase::Failed("cancelled".into()));
                        return;
                    }
                }
            }
        };

        // 2. Non-200 status codes are forwarded directly without caching.
        if resp.status() != StatusCode::OK {
            let status = resp.status();
            let content_type = resp
                .headers()
                .get(header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .map(str::to_string);
            let body = resp.bytes().await.unwrap_or_default();
            let _ = progress_tx.send(DownloadPhase::UpstreamNonOk {
                status,
                content_type,
                body,
            });
            self.remove_in_flight(&cache_key, handle.id);
            return;
        }

        let meta = extract_upstream_meta(&resp);

        // 3. If known Content-Length exceeds max_cache_size, stream straight through with 0 disk usage.
        if let Some(len) = meta.content_length
            && len > self.cache.max_size()
        {
            debug!(
                url = %upstream_url,
                content_length = len,
                max_cache_size = self.cache.max_size(),
                "package larger than max_cache_size; streaming directly without caching"
            );
            let response_slot = Arc::new(Mutex::new(Some(resp)));
            let _ = progress_tx.send(DownloadPhase::OversizedPassthrough {
                meta,
                response_slot,
            });
            self.remove_in_flight(&cache_key, handle.id);
            return;
        }

        // 4. Proactively free disk quota if incoming size + current cache exceeds max_cache_size.
        if let Some(len) = meta.content_length {
            self.cache.make_room_for(len).await;
        }

        // 5. Create temp file and notify downstream clients immediately so streaming begins.
        let tmp_path = self.cache.new_temp_path();
        let mut tmp_file = match tokio::fs::File::create(&tmp_path).await {
            Ok(f) => f,
            Err(err) => {
                let msg = format!("failed to create cache temp file: {err}");
                let _ = progress_tx.send(DownloadPhase::Failed(msg));
                self.remove_in_flight(&cache_key, handle.id);
                return;
            }
        };
        let temp_guard = Arc::new(TempFileGuard::new(tmp_path.clone()));

        let _ = progress_tx.send(DownloadPhase::Streaming {
            meta: meta.clone(),
            temp_file: Arc::clone(&temp_guard),
            written: 0,
        });

        // 6. Stream chunks from upstream -> temp file -> notify tailing readers concurrently.
        let mut stream = resp.bytes_stream();
        let mut written: u64 = 0;
        let mut skip_cache = false;

        loop {
            let all_expected_received = meta
                .content_length
                .is_some_and(|expected| written >= expected);

            let next_item = if all_expected_received {
                // Once every expected byte has been written, Hyper may close the downstream
                // response body and drop ReaderGuard before upstream yields final EOF.
                // Never cancel a complete download while draining final EOF.
                stream.next().await
            } else {
                tokio::select! {
                    biased;
                    item = stream.next() => item,
                    _ = handle.cancel_notify.notified() => {
                        if self.try_cancel_if_unused(&cache_key, &handle) {
                            debug!(url = %upstream_url, written, "all downstream clients disconnected; aborting download");
                            let _ = progress_tx.send(DownloadPhase::Failed("cancelled".into()));
                            return;
                        }
                        continue;
                    }
                }
            };

            match next_item {
                Some(Ok(chunk)) => {
                    if chunk.is_empty() {
                        continue;
                    }
                    if let Err(err) = tmp_file.write_all(&chunk).await {
                        let msg = format!("disk write error while caching package: {err}");
                        warn!(url = %upstream_url, error = %err, "disk write error");
                        let _ = progress_tx.send(DownloadPhase::Failed(msg));
                        self.remove_in_flight(&cache_key, handle.id);
                        return;
                    }
                    let n = chunk.len() as u64;
                    written += n;
                    self.metrics
                        .bytes_downloaded_from_upstream
                        .fetch_add(n, Ordering::Relaxed);
                    if written > self.cache.max_size() {
                        skip_cache = true;
                    }
                    progress_tx.send_modify(|phase| {
                        if let DownloadPhase::Streaming { written: w, .. } = phase {
                            *w = written;
                        }
                    });
                }
                Some(Err(err)) => {
                    let msg = format!("upstream stream error: {err}");
                    warn!(url = %upstream_url, written, error = %err, "upstream stream error");
                    let _ = progress_tx.send(DownloadPhase::Failed(msg));
                    self.remove_in_flight(&cache_key, handle.id);
                    return;
                }
                None => break,
            }
        }

        // 7. Verify Content-Length completeness before committing to cache.
        if let Some(expected) = meta.content_length
            && written != expected
        {
            let msg = format!(
                "upstream stream truncated: expected {expected} bytes, received {written} bytes"
            );
            warn!(url = %upstream_url, expected, written, "truncated upstream package");
            let _ = progress_tx.send(DownloadPhase::Failed(msg));
            self.remove_in_flight(&cache_key, handle.id);
            return;
        }

        if let Err(err) = tmp_file.flush().await {
            let msg = format!("failed to flush temp file: {err}");
            let _ = progress_tx.send(DownloadPhase::Failed(msg));
            self.remove_in_flight(&cache_key, handle.id);
            return;
        }
        drop(tmp_file);

        // 8. Commit atomically into DiskCache before removing from in_flight map.
        let final_path = if !skip_cache {
            let pkg_meta = PackageMeta {
                key: cache_key.clone(),
                hash: String::new(),
                upstream_url: upstream_url.to_string(),
                filename,
                content_type: meta.content_type.clone(),
                etag: meta.etag.clone(),
                last_modified: meta.last_modified.clone(),
                size: written,
                created_at_nanos: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos(),
            };
            match self.cache.commit_temp_file(&tmp_path, pkg_meta).await {
                Ok(CommitOutcome::Committed(pkg)) => {
                    temp_guard.mark_committed();
                    debug!(url = %upstream_url, size = written, "committed package to disk cache");
                    Some(pkg.data_path)
                }
                Ok(CommitOutcome::SkippedTooLarge) => None,
                Err(err) => {
                    warn!(url = %upstream_url, error = %err, "failed to commit package to disk cache; streaming from temp file");
                    None
                }
            }
        } else {
            None
        };

        let _ = progress_tx.send(DownloadPhase::Completed {
            meta,
            written,
            final_path,
            temp_file: temp_guard,
        });
        self.remove_in_flight(&cache_key, handle.id);
    }
}

fn extract_upstream_meta(resp: &reqwest::Response) -> UpstreamMeta {
    let headers = resp.headers();
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/octet-stream")
        .to_string();
    let content_length = headers
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok());
    let etag = headers
        .get(header::ETAG)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let last_modified = headers
        .get(header::LAST_MODIFIED)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);

    UpstreamMeta {
        status: resp.status(),
        content_type,
        content_length,
        etag,
        last_modified,
    }
}

async fn open_tail_file(
    tmp_path: &std::path::Path,
    progress_rx: &mut watch::Receiver<DownloadPhase>,
) -> io::Result<tokio::fs::File> {
    match tokio::fs::File::open(tmp_path).await {
        Ok(file) => Ok(file),
        Err(err) if err.kind() == io::ErrorKind::NotFound => {
            // Download may have just atomically renamed tmp_path -> final_path before
            // publishing DownloadPhase::Completed. Wait for the terminal phase.
            loop {
                let terminal = {
                    let phase = progress_rx.borrow_and_update();
                    match &*phase {
                        DownloadPhase::Completed {
                            final_path: Some(fp),
                            ..
                        } => Some(Ok(fp.clone())),
                        DownloadPhase::Completed {
                            final_path: None, ..
                        } => Some(Ok(tmp_path.to_path_buf())),
                        DownloadPhase::Failed(msg) => Some(Err(io::Error::other(msg.clone()))),
                        _ => None,
                    }
                };
                if let Some(res) = terminal {
                    let path = res?;
                    return tokio::fs::File::open(&path).await;
                }
                if progress_rx.changed().await.is_err() {
                    return Err(err);
                }
            }
        }
        Err(err) => Err(err),
    }
}

struct TailState {
    file: tokio::fs::File,
    progress_rx: watch::Receiver<DownloadPhase>,
    pos: u64,
    buf: Vec<u8>,
    _reader_guard: ReaderGuard,
    _temp_guard: Arc<TempFileGuard>,
}

fn build_tail_stream(
    file: tokio::fs::File,
    progress_rx: watch::Receiver<DownloadPhase>,
    reader_guard: ReaderGuard,
    temp_guard: Arc<TempFileGuard>,
) -> BoxedByteStream {
    let state = TailState {
        file,
        progress_rx,
        pos: 0,
        buf: vec![0u8; READ_BUF_SIZE],
        _reader_guard: reader_guard,
        _temp_guard: temp_guard,
    };

    Box::pin(stream::unfold(state, |mut st| async move {
        loop {
            let (available, finished, failed) = {
                let phase = st.progress_rx.borrow_and_update();
                match &*phase {
                    DownloadPhase::Streaming { written, .. } => (*written, false, None),
                    DownloadPhase::Completed { written, .. } => (*written, true, None),
                    DownloadPhase::Failed(msg) => (0, false, Some(msg.clone())),
                    _ => (0, true, None),
                }
            };

            if let Some(err_msg) = failed {
                return Some((Err(io::Error::other(err_msg)), st));
            }

            if st.pos < available {
                let to_read = ((available - st.pos) as usize).min(READ_BUF_SIZE);
                match st.file.read(&mut st.buf[..to_read]).await {
                    Ok(0) => {
                        tokio::task::yield_now().await;
                        continue;
                    }
                    Ok(n) => {
                        st.pos += n as u64;
                        let chunk = Bytes::copy_from_slice(&st.buf[..n]);
                        return Some((Ok(chunk), st));
                    }
                    Err(err) => return Some((Err(err), st)),
                }
            }

            if finished {
                return None;
            }

            if st.progress_rx.changed().await.is_err() {
                return Some((
                    Err(io::Error::other("download producer channel closed")),
                    st,
                ));
            }
        }
    }))
}
