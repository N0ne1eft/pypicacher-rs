use std::collections::HashMap;
use std::io::SeekFrom;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;

use axum::Router;
use axum::body::Body;
use axum::extract::{Path, RawQuery, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode, header};
use axum::response::{IntoResponse, Redirect, Response};
use axum::routing::get;
use bytes::Bytes;
use futures_util::StreamExt;
use parking_lot::Mutex;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::sync::watch;
use tokio_util::io::ReaderStream;
use tower_http::compression::CompressionLayer;
use tracing::{debug, warn};
use url::Url;

use crate::cache::DiskCache;
use crate::cache::index_cache::{CachedIndex, IndexCache, IndexCacheKey, IndexLookup};
use crate::config::Config;
use crate::downloader::{DownloadManager, DownloadOutcome};
use crate::metrics::Metrics;
use crate::range::{RangeParseResult, parse_range_header};
use crate::rewrite::{
    RewriteError, SimpleFormat, UrlMapper, negotiate_simple_format, normalize_project_name,
    rewrite_simple_html, rewrite_simple_json,
};
use crate::upstream::UpstreamClients;

const CACHE_STREAM_BUF_SIZE: usize = 256 * 1024; // 256 KiB
const HEADER_X_CACHE: HeaderName = HeaderName::from_static("x-cache");
const HEADER_X_PYPI_LAST_SERIAL: HeaderName = HeaderName::from_static("x-pypi-last-serial");

#[derive(Clone)]
enum IndexFetchPhase {
    Fetching,
    Ready {
        entry: Arc<CachedIndex>,
        revalidated: bool,
    },
    UpstreamNonOk {
        status: StatusCode,
        content_type: Option<String>,
        body: Bytes,
    },
    Failed(String),
}

pub struct AppState {
    pub config: Config,
    pub cache: Arc<DiskCache>,
    pub index_cache: Arc<IndexCache>,
    pub mapper: Arc<UrlMapper>,
    pub upstream: UpstreamClients,
    pub downloader: Arc<DownloadManager>,
    pub metrics: Arc<Metrics>,
    in_flight_index: Mutex<HashMap<IndexCacheKey, watch::Receiver<IndexFetchPhase>>>,
}

impl AppState {
    pub async fn build(config: Config) -> anyhow::Result<Arc<Self>> {
        let metrics = Arc::new(Metrics::default());
        let cache = DiskCache::open(
            &config.cache_dir,
            config.max_cache_size,
            config.evict_high_watermark,
            config.evict_low_watermark,
            Arc::clone(&metrics),
        )
        .await?;

        let index_cache = Arc::new(IndexCache::new(
            config.index_cache_max_bytes,
            config.index_ttl,
        ));
        let mapper = Arc::new(UrlMapper::new(
            &config.upstream,
            config.upstream_packages_url.clone(),
            &config.allowed_upstream_hosts,
        ));
        let upstream = UpstreamClients::from_config(&config)?;
        let downloader = DownloadManager::new(
            Arc::clone(&cache),
            upstream.clone(),
            Arc::clone(&metrics),
        );

        Ok(Arc::new(Self {
            config,
            cache,
            index_cache,
            mapper,
            upstream,
            downloader,
            metrics,
            in_flight_index: Mutex::new(HashMap::new()),
        }))
    }
}

pub fn build_router(state: Arc<AppState>) -> Router {
    // Enable gzip/brotli compression strictly for Simple API HTML/JSON routes,
    // leaving already-compressed binary package streams (.whl, .tar.gz) untouched.
    let simple_routes = Router::new()
        .route("/simple", get(redirect_simple_root))
        .route("/simple/", get(handle_simple_root).head(handle_simple_root))
        .route(
            "/simple/{project}",
            get(redirect_simple_project).head(redirect_simple_project),
        )
        .route(
            "/simple/{project}/",
            get(handle_simple_project).head(handle_simple_project),
        )
        .layer(CompressionLayer::new());

    Router::new()
        .route("/", get(handle_home))
        .route("/healthz", get(handle_healthz))
        .route("/stats", get(handle_stats))
        .merge(simple_routes)
        .route(
            "/packages/{*path}",
            get(handle_packages).head(handle_packages),
        )
        .route(
            "/proxy/{scheme}/{authority}/{*path}",
            get(handle_proxy).head(handle_proxy),
        )
        .with_state(state)
}

async fn handle_home(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let (count, bytes) = state.cache.stats();
    let max = state.cache.max_size();
    let body = format!(
        "pypicacher-rs {}\n\
         \n\
         Upstream: {}\n\
         Cache: {} packages ({} / {} bytes)\n\
         \n\
         Usage:\n\
         - uv:  uv pip install --default-index http://<host>:<port>/simple/ <package>\n\
         - pip: pip install --index-url http://<host>:<port>/simple/ --trusted-host <host> <package>\n\
         \n\
         Endpoints:\n\
         - /simple/  PyPI Simple Repository API (PEP 503 & PEP 691)\n\
         - /stats    JSON cache and traffic metrics\n\
         - /healthz  Health check\n",
        env!("CARGO_PKG_VERSION"),
        state.config.upstream,
        count,
        bytes,
        max
    );
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
        body,
    )
}

async fn handle_healthz() -> impl IntoResponse {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        r#"{"status":"ok"}"#,
    )
}

async fn handle_stats(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let (cached_packages, cache_bytes_used) = state.cache.stats();
    let (index_entries, index_bytes) = state.index_cache.stats();
    let snapshot = state.metrics.snapshot(
        cached_packages,
        cache_bytes_used,
        state.cache.max_size(),
        state.downloader.in_flight_count(),
        index_entries,
        index_bytes,
    );
    axum::Json(snapshot)
}

async fn redirect_simple_root() -> Redirect {
    Redirect::permanent("/simple/")
}

async fn redirect_simple_project(Path(project): Path<String>) -> Response {
    match normalize_project_name(&project) {
        Ok(normalized) => Redirect::permanent(&format!("/simple/{normalized}/")).into_response(),
        Err(err) => (StatusCode::BAD_REQUEST, err.to_string()).into_response(),
    }
}

/// Streams the upstream root `/simple/` index directly without buffering tens of megabytes in RAM.
async fn handle_simple_root(
    State(state): State<Arc<AppState>>,
    method: Method,
    headers: HeaderMap,
) -> Response {
    let format = negotiate_simple_format(
        headers
            .get(header::ACCEPT)
            .and_then(|v| v.to_str().ok()),
    );

    let req = if method == Method::HEAD {
        state
            .upstream
            .index_client
            .head(state.config.upstream.clone())
    } else {
        state
            .upstream
            .index_client
            .get(state.config.upstream.clone())
    }
    .header(header::ACCEPT, format.upstream_accept_header());

    let resp = match req.send().await {
        Ok(r) => r,
        Err(err) => {
            warn!(error = %err, "failed to fetch upstream root /simple/");
            return (StatusCode::BAD_GATEWAY, format!("upstream error: {err}")).into_response();
        }
    };

    let status = resp.status();
    let mut builder = Response::builder().status(status);
    if let Some(ct) = resp.headers().get(header::CONTENT_TYPE) {
        builder = builder.header(header::CONTENT_TYPE, ct);
    }
    if let Some(etag) = resp.headers().get(header::ETAG) {
        builder = builder.header(header::ETAG, etag);
    }
    if let Some(serial) = resp.headers().get(&HEADER_X_PYPI_LAST_SERIAL) {
        builder = builder.header(HEADER_X_PYPI_LAST_SERIAL, serial);
    }
    builder = builder.header(header::VARY, "Accept, Accept-Encoding");

    let body = if method == Method::HEAD {
        Body::empty()
    } else {
        Body::from_stream(
            resp.bytes_stream()
                .map(|res| res.map_err(std::io::Error::other)),
        )
    };

    builder.body(body).unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

async fn handle_simple_project(
    State(state): State<Arc<AppState>>,
    method: Method,
    Path(project): Path<String>,
    headers: HeaderMap,
) -> Response {
    let normalized = match normalize_project_name(&project) {
        Ok(n) => n,
        Err(err) => return (StatusCode::BAD_REQUEST, err.to_string()).into_response(),
    };

    let format = negotiate_simple_format(
        headers
            .get(header::ACCEPT)
            .and_then(|v| v.to_str().ok()),
    );
    let key = IndexCacheKey {
        project: normalized.clone(),
        format,
    };

    // pip sends `Cache-Control: max-age=0` on every index lookup; let `--index-ttl`
    // govern normal freshness and only force upstream revalidation on explicit `no-cache`
    // (such as `uv --refresh`).
    let client_no_cache = headers
        .get(header::CACHE_CONTROL)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|cc| cc.to_ascii_lowercase().contains("no-cache"));

    let client_if_none_match = headers
        .get(header::IF_NONE_MATCH)
        .and_then(|v| v.to_str().ok());

    let stale_prev = match state.index_cache.lookup(&key) {
        IndexLookup::Fresh(entry) if !client_no_cache => {
            state.metrics.index_hits.fetch_add(1, Ordering::Relaxed);
            return build_index_response(&entry, method, client_if_none_match, "HIT", &state.config);
        }
        IndexLookup::Fresh(entry) | IndexLookup::Stale(entry) => Some(entry),
        IndexLookup::Miss => None,
    };

    // Singleflight fetch/revalidation against upstream Simple API.
    let mut rx = acquire_or_spawn_index_fetch(Arc::clone(&state), key.clone(), stale_prev);
    let result = loop {
        let current = rx.borrow_and_update().clone();
        match current {
            IndexFetchPhase::Fetching => {
                if rx.changed().await.is_err() {
                    return (StatusCode::BAD_GATEWAY, "upstream index fetch interrupted")
                        .into_response();
                }
            }
            other => break other,
        }
    };

    match result {
        IndexFetchPhase::Ready { entry, revalidated } => {
            let x_cache = if revalidated { "REVALIDATED" } else { "MISS" };
            build_index_response(&entry, method, client_if_none_match, x_cache, &state.config)
        }
        IndexFetchPhase::UpstreamNonOk {
            status,
            content_type,
            body,
        } => {
            let mut builder = Response::builder()
                .status(status)
                .header(HEADER_X_CACHE, "MISS");
            if let Some(ct) = content_type {
                builder = builder.header(header::CONTENT_TYPE, ct);
            }
            let body = if method == Method::HEAD {
                Body::empty()
            } else {
                Body::from(body)
            };
            builder
                .body(body)
                .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
        }
        IndexFetchPhase::Failed(msg) => (StatusCode::BAD_GATEWAY, msg).into_response(),
        IndexFetchPhase::Fetching => unreachable!(),
    }
}

fn acquire_or_spawn_index_fetch(
    state: Arc<AppState>,
    key: IndexCacheKey,
    stale_prev: Option<Arc<CachedIndex>>,
) -> watch::Receiver<IndexFetchPhase> {
    let mut map = state.in_flight_index.lock();
    if let Some(existing) = map.get(&key) {
        return existing.clone();
    }

    let (tx, rx) = watch::channel(IndexFetchPhase::Fetching);
    map.insert(key.clone(), rx.clone());
    drop(map);

    let state_clone = Arc::clone(&state);
    tokio::spawn(async move {
        let phase = fetch_and_rewrite_index(&state_clone, &key, stale_prev).await;
        let _ = tx.send(phase);
        state_clone.in_flight_index.lock().remove(&key);
    });

    rx
}

async fn fetch_and_rewrite_index(
    state: &AppState,
    key: &IndexCacheKey,
    stale_prev: Option<Arc<CachedIndex>>,
) -> IndexFetchPhase {
    let upstream_url = match state.config.upstream.join(&format!("{}/", key.project)) {
        Ok(u) => u,
        Err(err) => return IndexFetchPhase::Failed(format!("invalid upstream project URL: {err}")),
    };

    let mut req = state
        .upstream
        .index_client
        .get(upstream_url.clone())
        .header(header::ACCEPT, key.format.upstream_accept_header());

    if let Some(ref prev) = stale_prev
        && let Some(ref etag) = prev.upstream_etag
    {
        req = req.header(header::IF_NONE_MATCH, etag);
    }

    let resp = match req.send().await {
        Ok(r) => r,
        Err(err) => {
            warn!(project = %key.project, error = %err, "upstream index fetch failed");
            return IndexFetchPhase::Failed(format!("upstream index fetch failed: {err}"));
        }
    };

    if resp.status() == StatusCode::NOT_MODIFIED
        && let Some(prev) = stale_prev
    {
        state.metrics.index_revalidated.fetch_add(1, Ordering::Relaxed);
        let entry = state
            .index_cache
            .mark_revalidated(key)
            .unwrap_or(prev);
        return IndexFetchPhase::Ready {
            entry,
            revalidated: true,
        };
    }

    if resp.status() != StatusCode::OK {
        let status = resp.status();
        let content_type = resp
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        let body = resp.bytes().await.unwrap_or_default();
        return IndexFetchPhase::UpstreamNonOk {
            status,
            content_type,
            body,
        };
    }

    state.metrics.index_misses.fetch_add(1, Ordering::Relaxed);

    let final_url = resp.url().clone();
    let content_type = resp
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or(match key.format {
            SimpleFormat::Json => "application/vnd.pypi.simple.v1+json",
            SimpleFormat::Html => "text/html; charset=utf-8",
        })
        .to_string();
    let upstream_etag = resp
        .headers()
        .get(header::ETAG)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let last_serial = resp
        .headers()
        .get(&HEADER_X_PYPI_LAST_SERIAL)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);

    let raw_bytes = match resp.bytes().await {
        Ok(b) => b,
        Err(err) => {
            warn!(project = %key.project, error = %err, "failed to read upstream index body");
            return IndexFetchPhase::Failed(format!("failed to read upstream index body: {err}"));
        }
    };

    let rewritten_bytes = if content_type.to_ascii_lowercase().contains("json") {
        match rewrite_simple_json(&raw_bytes, &final_url, &state.mapper) {
            Ok(b) => Bytes::from(b),
            Err(err) => {
                warn!(project = %key.project, error = %err, "failed to rewrite Simple JSON");
                return IndexFetchPhase::Failed(format!("invalid upstream Simple JSON: {err}"));
            }
        }
    } else {
        let html = String::from_utf8_lossy(&raw_bytes);
        Bytes::from(rewrite_simple_html(&html, &final_url, &state.mapper).into_bytes())
    };

    let proxy_etag = compute_weak_etag(&rewritten_bytes);
    let cached = CachedIndex {
        body: rewritten_bytes,
        content_type,
        proxy_etag,
        upstream_etag,
        last_serial,
        fetched_at: Instant::now(),
    };

    let entry = state.index_cache.insert(key.clone(), cached);
    IndexFetchPhase::Ready {
        entry,
        revalidated: false,
    }
}

fn build_index_response(
    entry: &CachedIndex,
    method: Method,
    client_if_none_match: Option<&str>,
    x_cache: &'static str,
    config: &Config,
) -> Response {
    let ttl_secs = config.index_ttl.as_secs();
    let cache_control = if ttl_secs > 0 {
        format!("public, max-age={ttl_secs}")
    } else {
        "no-cache".to_string()
    };

    if let Some(inm) = client_if_none_match
        && etag_matches(inm, &entry.proxy_etag)
    {
        let mut builder = Response::builder()
            .status(StatusCode::NOT_MODIFIED)
            .header(header::ETAG, &entry.proxy_etag)
            .header(header::CACHE_CONTROL, cache_control)
            .header(header::VARY, "Accept, Accept-Encoding")
            .header(HEADER_X_CACHE, x_cache);
        if let Some(ref serial) = entry.last_serial {
            builder = builder.header(HEADER_X_PYPI_LAST_SERIAL, serial);
        }
        return builder
            .body(Body::empty())
            .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response());
    }

    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, &entry.content_type)
        .header(header::ETAG, &entry.proxy_etag)
        .header(header::CACHE_CONTROL, cache_control)
        .header(header::VARY, "Accept, Accept-Encoding")
        .header(HEADER_X_CACHE, x_cache);

    if let Some(ref serial) = entry.last_serial {
        builder = builder.header(HEADER_X_PYPI_LAST_SERIAL, serial);
    }

    let body = if method == Method::HEAD {
        builder = builder.header(header::CONTENT_LENGTH, entry.body.len());
        Body::empty()
    } else {
        Body::from(entry.body.clone())
    };

    builder
        .body(body)
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

async fn handle_packages(
    State(state): State<Arc<AppState>>,
    method: Method,
    Path(path): Path<String>,
    RawQuery(query): RawQuery,
    headers: HeaderMap,
) -> Response {
    match state
        .mapper
        .resolve_packages_path(&path, query.as_deref())
    {
        Ok(upstream_url) => serve_package(state, method, upstream_url, headers).await,
        Err(err) => rewrite_error_response(err),
    }
}

async fn handle_proxy(
    State(state): State<Arc<AppState>>,
    method: Method,
    Path((scheme, authority, path)): Path<(String, String, String)>,
    RawQuery(query): RawQuery,
    headers: HeaderMap,
) -> Response {
    match state
        .mapper
        .resolve_proxy_path(&scheme, &authority, &path, query.as_deref())
    {
        Ok(upstream_url) => serve_package(state, method, upstream_url, headers).await,
        Err(err) => rewrite_error_response(err),
    }
}

fn rewrite_error_response(err: RewriteError) -> Response {
    let status = match err {
        RewriteError::ForbiddenHost(_) => StatusCode::FORBIDDEN,
        RewriteError::PackagesBaseNotConfigured => StatusCode::NOT_FOUND,
        _ => StatusCode::BAD_REQUEST,
    };
    (status, err.to_string()).into_response()
}

async fn serve_package(
    state: Arc<AppState>,
    method: Method,
    upstream_url: Url,
    req_headers: HeaderMap,
) -> Response {
    let cache_key = upstream_url.as_str().to_string();
    let filename = upstream_url
        .path_segments()
        .and_then(|mut s| s.next_back())
        .filter(|s| !s.is_empty())
        .unwrap_or("package")
        .to_string();

    // 1. Handle HEAD requests without triggering a full package download on miss.
    if method == Method::HEAD {
        if let Some(pkg) = state.cache.peek(&cache_key) {
            let mut builder = Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, &pkg.meta.content_type)
                .header(header::CONTENT_LENGTH, pkg.meta.size)
                .header(header::ACCEPT_RANGES, "bytes")
                .header(
                    header::CACHE_CONTROL,
                    "public, max-age=31536000, immutable",
                )
                .header(HEADER_X_CACHE, "HIT");
            if let Some(ref etag) = pkg.meta.etag {
                builder = builder.header(header::ETAG, etag);
            }
            if let Some(ref lm) = pkg.meta.last_modified {
                builder = builder.header(header::LAST_MODIFIED, lm);
            }
            return builder
                .body(Body::empty())
                .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response());
        }
        return proxy_head_request(&state, upstream_url).await;
    }

    // 2. Check disk cache (and touch LRU access time on hit).
    if let Some(pkg) = state.cache.get_and_touch(&cache_key) {
        if let Some(inm) = req_headers
            .get(header::IF_NONE_MATCH)
            .and_then(|v| v.to_str().ok())
            && let Some(ref etag) = pkg.meta.etag
            && etag_matches(inm, etag)
        {
            state.metrics.package_hits.fetch_add(1, Ordering::Relaxed);
            return Response::builder()
                .status(StatusCode::NOT_MODIFIED)
                .header(header::ETAG, etag)
                .header(HEADER_X_CACHE, "HIT")
                .body(Body::empty())
                .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response());
        }

        match tokio::fs::File::open(&pkg.data_path).await {
            Ok(mut file) => {
                let total_size = pkg.meta.size;

                // Handle Range requests directly from disk cache (crucial for uv!).
                if let Some(range_str) = req_headers
                    .get(header::RANGE)
                    .and_then(|v| v.to_str().ok())
                {
                    match parse_range_header(range_str, total_size) {
                        RangeParseResult::Unsatisfiable => {
                            return Response::builder()
                                .status(StatusCode::RANGE_NOT_SATISFIABLE)
                                .header(header::CONTENT_RANGE, format!("bytes */{total_size}"))
                                .header(HEADER_X_CACHE, "HIT")
                                .body(Body::empty())
                                .unwrap_or_else(|_| {
                                    StatusCode::INTERNAL_SERVER_ERROR.into_response()
                                });
                        }
                        RangeParseResult::Satisfiable(range) => {
                            if let Err(err) = file.seek(SeekFrom::Start(range.start)).await {
                                warn!(path = %pkg.data_path.display(), error = %err, "seek failed on cached file");
                                return (StatusCode::INTERNAL_SERVER_ERROR, "cache read error")
                                    .into_response();
                            }
                            let len = range.len();
                            let limited = file.take(len);
                            let stream = ReaderStream::with_capacity(limited, CACHE_STREAM_BUF_SIZE);

                            state.metrics.package_hits.fetch_add(1, Ordering::Relaxed);
                            state
                                .metrics
                                .bytes_served_from_cache
                                .fetch_add(len, Ordering::Relaxed);

                            let mut builder = Response::builder()
                                .status(StatusCode::PARTIAL_CONTENT)
                                .header(header::CONTENT_TYPE, &pkg.meta.content_type)
                                .header(header::CONTENT_LENGTH, len)
                                .header(
                                    header::CONTENT_RANGE,
                                    range.content_range_header(total_size),
                                )
                                .header(header::ACCEPT_RANGES, "bytes")
                                .header(
                                    header::CACHE_CONTROL,
                                    "public, max-age=31536000, immutable",
                                )
                                .header(HEADER_X_CACHE, "HIT");
                            if let Some(ref etag) = pkg.meta.etag {
                                builder = builder.header(header::ETAG, etag);
                            }
                            if let Some(ref lm) = pkg.meta.last_modified {
                                builder = builder.header(header::LAST_MODIFIED, lm);
                            }
                            return builder
                                .body(Body::from_stream(stream))
                                .unwrap_or_else(|_| {
                                    StatusCode::INTERNAL_SERVER_ERROR.into_response()
                                });
                        }
                        RangeParseResult::Ignore => {
                            // Fall through to full 200 OK per RFC 9110.
                        }
                    }
                }

                let stream = ReaderStream::with_capacity(file, CACHE_STREAM_BUF_SIZE);
                state.metrics.package_hits.fetch_add(1, Ordering::Relaxed);
                state
                    .metrics
                    .bytes_served_from_cache
                    .fetch_add(total_size, Ordering::Relaxed);

                debug!(filename = %pkg.meta.filename, size = total_size, "cache HIT");

                let mut builder = Response::builder()
                    .status(StatusCode::OK)
                    .header(header::CONTENT_TYPE, &pkg.meta.content_type)
                    .header(header::CONTENT_LENGTH, total_size)
                    .header(header::ACCEPT_RANGES, "bytes")
                    .header(
                        header::CACHE_CONTROL,
                        "public, max-age=31536000, immutable",
                    )
                    .header(HEADER_X_CACHE, "HIT");
                if let Some(ref etag) = pkg.meta.etag {
                    builder = builder.header(header::ETAG, etag);
                }
                if let Some(ref lm) = pkg.meta.last_modified {
                    builder = builder.header(header::LAST_MODIFIED, lm);
                }
                return builder
                    .body(Body::from_stream(stream))
                    .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response());
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                // Rare race with concurrent eviction; seamlessly fall through to upstream fetch.
                debug!(path = %pkg.data_path.display(), "cached file removed concurrently, refetching");
            }
            Err(err) => {
                warn!(path = %pkg.data_path.display(), error = %err, "failed to open cached package");
            }
        }
    }

    // 3. Cache MISS with a Range header (e.g. uv peeking wheel ZIP metadata): pass through directly.
    if let Some(range_hdr) = req_headers.get(header::RANGE).cloned() {
        state
            .metrics
            .package_range_bypasses
            .fetch_add(1, Ordering::Relaxed);
        return proxy_range_request(&state, upstream_url, range_hdr).await;
    }

    // 4. Cache MISS (full GET): download from upstream and stream to downstream concurrently!
    match state
        .downloader
        .fetch_and_stream(cache_key, upstream_url, filename)
        .await
    {
        DownloadOutcome::Stream {
            meta,
            body,
            coalesced,
            cache_bypass,
        } => {
            let x_cache = if cache_bypass {
                state.metrics.package_misses.fetch_add(1, Ordering::Relaxed);
                "BYPASS"
            } else if coalesced {
                state
                    .metrics
                    .package_coalesced
                    .fetch_add(1, Ordering::Relaxed);
                "COALESCED"
            } else {
                state.metrics.package_misses.fetch_add(1, Ordering::Relaxed);
                "MISS"
            };

            let mut builder = Response::builder()
                .status(meta.status)
                .header(header::CONTENT_TYPE, meta.content_type)
                .header(header::ACCEPT_RANGES, "bytes")
                .header(
                    header::CACHE_CONTROL,
                    "public, max-age=31536000, immutable",
                )
                .header(HEADER_X_CACHE, x_cache);

            if let Some(len) = meta.content_length {
                builder = builder.header(header::CONTENT_LENGTH, len);
            }
            if let Some(etag) = meta.etag {
                builder = builder.header(header::ETAG, etag);
            }
            if let Some(lm) = meta.last_modified {
                builder = builder.header(header::LAST_MODIFIED, lm);
            }

            builder
                .body(Body::from_stream(body))
                .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
        }
        DownloadOutcome::UpstreamNonOk {
            status,
            content_type,
            body,
        } => {
            let mut builder = Response::builder()
                .status(status)
                .header(HEADER_X_CACHE, "MISS");
            if let Some(ct) = content_type {
                builder = builder.header(header::CONTENT_TYPE, ct);
            }
            builder
                .body(Body::from(body))
                .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
        }
        DownloadOutcome::Error(msg) => (StatusCode::BAD_GATEWAY, msg).into_response(),
    }
}

async fn proxy_head_request(state: &AppState, upstream_url: Url) -> Response {
    let resp = match state.upstream.package_client.head(upstream_url).send().await {
        Ok(r) => r,
        Err(err) => {
            return (StatusCode::BAD_GATEWAY, format!("upstream HEAD failed: {err}"))
                .into_response();
        }
    };

    let builder = copy_passthrough_headers(
        resp.headers(),
        Response::builder()
            .status(resp.status())
            .header(HEADER_X_CACHE, "MISS"),
    );
    builder
        .body(Body::empty())
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

async fn proxy_range_request(
    state: &AppState,
    upstream_url: Url,
    range_hdr: HeaderValue,
) -> Response {
    let resp = match state
        .upstream
        .package_client
        .get(upstream_url)
        .header(header::RANGE, range_hdr)
        .send()
        .await
    {
        Ok(r) => r,
        Err(err) => {
            return (
                StatusCode::BAD_GATEWAY,
                format!("upstream Range request failed: {err}"),
            )
                .into_response();
        }
    };

    let builder = copy_passthrough_headers(
        resp.headers(),
        Response::builder()
            .status(resp.status())
            .header(HEADER_X_CACHE, "BYPASS"),
    );

    let metrics = Arc::clone(&state.metrics);
    let stream = resp.bytes_stream().map(move |res| match res {
        Ok(chunk) => {
            metrics
                .bytes_downloaded_from_upstream
                .fetch_add(chunk.len() as u64, Ordering::Relaxed);
            Ok(chunk)
        }
        Err(err) => Err(std::io::Error::other(err)),
    });

    builder
        .body(Body::from_stream(stream))
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

fn copy_passthrough_headers(
    src: &HeaderMap,
    mut dst: axum::http::response::Builder,
) -> axum::http::response::Builder {
    for name in [
        header::CONTENT_TYPE,
        header::CONTENT_LENGTH,
        header::CONTENT_RANGE,
        header::ACCEPT_RANGES,
        header::ETAG,
        header::LAST_MODIFIED,
        header::CACHE_CONTROL,
    ] {
        if let Some(val) = src.get(&name) {
            dst = dst.header(name, val);
        }
    }
    dst
}

fn compute_weak_etag(body: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(body);
    let digest = hex::encode(hasher.finalize());
    format!("W/\"{}\"", &digest[..32])
}

/// Weak ETag comparison per RFC 9110 Section 8.8.3.2.
fn etag_matches(if_none_match: &str, current: &str) -> bool {
    fn strip_weak(s: &str) -> &str {
        let s = s.trim();
        s.strip_prefix("W/")
            .or_else(|| s.strip_prefix("w/"))
            .unwrap_or(s)
            .trim()
    }
    let target = strip_weak(current);
    for candidate in if_none_match.split(',') {
        let c = candidate.trim();
        if c == "*" || strip_weak(c) == target {
            return true;
        }
    }
    false
}
