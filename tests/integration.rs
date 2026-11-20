use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use bytes::Bytes;
use futures_util::StreamExt;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::sync::Notify;
use url::Url;

use pypicacher_rs::config::Config;
use pypicacher_rs::server::{AppState, build_router};

#[derive(Clone)]
struct MockUpstreamState {
    files: Arc<HashMap<String, Vec<u8>>>,
    index_hits: Arc<AtomicUsize>,
    file_hits: Arc<AtomicUsize>,
    stream_gate: Arc<Notify>,
    stream_first_chunk_sent: Arc<Notify>,
}

async fn start_mock_upstream(files: HashMap<String, Vec<u8>>) -> (SocketAddr, MockUpstreamState) {
    let state = MockUpstreamState {
        files: Arc::new(files),
        index_hits: Arc::new(AtomicUsize::new(0)),
        file_hits: Arc::new(AtomicUsize::new(0)),
        stream_gate: Arc::new(Notify::new()),
        stream_first_chunk_sent: Arc::new(Notify::new()),
    };

    let router = Router::new()
        .route("/simple/{project}/", get(mock_simple_project))
        .route("/files/{filename}", get(mock_file_handler))
        .route("/gated/{filename}", get(mock_gated_stream))
        .route("/truncated/{filename}", get(mock_truncated_stream))
        .with_state(state.clone());

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });

    (addr, state)
}

async fn mock_simple_project(
    State(state): State<MockUpstreamState>,
    Path(project): Path<String>,
    headers: HeaderMap,
) -> Response {
    state.index_hits.fetch_add(1, Ordering::SeqCst);

    let accept = headers
        .get(header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("text/html");

    let host = headers
        .get(header::HOST)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("127.0.0.1");

    let matching_files: Vec<(&String, &Vec<u8>)> = state
        .files
        .iter()
        .filter(|(k, _)| !k.ends_with(".metadata") && k.starts_with(&project))
        .collect();

    if matching_files.is_empty() {
        return (StatusCode::NOT_FOUND, "project not found").into_response();
    }

    if accept.contains("application/vnd.pypi.simple.v1+json") {
        let mut files_json = Vec::new();
        for (name, data) in &matching_files {
            let has_meta = state.files.contains_key(&format!("{name}.metadata"));
            files_json.push(serde_json::json!({
                "filename": name,
                "url": format!("http://{host}/files/{name}"),
                "hashes": { "sha256": "0000" },
                "size": data.len(),
                "core-metadata": has_meta,
            }));
        }
        let body = serde_json::json!({
            "meta": { "api-version": "1.1" },
            "name": project,
            "files": files_json,
        });
        return (
            StatusCode::OK,
            [
                (header::CONTENT_TYPE, "application/vnd.pypi.simple.v1+json"),
                (header::ETAG, "\"mock-json-etag\""),
            ],
            serde_json::to_vec(&body).unwrap(),
        )
            .into_response();
    }

    let mut html = format!("<!DOCTYPE html><html><body><h1>Links for {project}</h1>");
    for (name, _) in &matching_files {
        let meta_attr = if state.files.contains_key(&format!("{name}.metadata")) {
            r#" data-core-metadata="true""#
        } else {
            ""
        };
        html.push_str(&format!(
            r#"<a href="http://{host}/files/{name}#sha256=0000"{meta_attr}>{name}</a><br/>"#
        ));
    }
    html.push_str("</body></html>");

    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "application/vnd.pypi.simple.v1+html"),
            (header::ETAG, "\"mock-html-etag\""),
        ],
        html,
    )
        .into_response()
}

async fn mock_file_handler(
    State(state): State<MockUpstreamState>,
    Path(filename): Path<String>,
    headers: HeaderMap,
) -> Response {
    state.file_hits.fetch_add(1, Ordering::SeqCst);

    let Some(data) = state.files.get(&filename) else {
        return (StatusCode::NOT_FOUND, "not found").into_response();
    };

    if let Some(range_str) = headers.get(header::RANGE).and_then(|v| v.to_str().ok())
        && let pypicacher_rs::range::RangeParseResult::Satisfiable(r) =
            pypicacher_rs::range::parse_range_header(range_str, data.len() as u64)
    {
        let slice = data[r.start as usize..=r.end as usize].to_vec();
        return Response::builder()
            .status(StatusCode::PARTIAL_CONTENT)
            .header(header::CONTENT_TYPE, "application/octet-stream")
            .header(header::CONTENT_LENGTH, slice.len())
            .header(header::CONTENT_RANGE, r.content_range_header(data.len() as u64))
            .header(header::ACCEPT_RANGES, "bytes")
            .body(Body::from(slice))
            .unwrap();
    }

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .header(header::CONTENT_LENGTH, data.len())
        .header(header::ACCEPT_RANGES, "bytes")
        .header(header::ETAG, format!("\"{filename}\""))
        .body(Body::from(data.clone()))
        .unwrap()
}

async fn mock_gated_stream(
    State(state): State<MockUpstreamState>,
    Path(_filename): Path<String>,
) -> Response {
    state.file_hits.fetch_add(1, Ordering::SeqCst);

    let chunk1 = Bytes::from_static(b"FIRST_CHUNK_DATA_");
    let chunk2 = Bytes::from_static(b"SECOND_CHUNK_DATA");
    let total_len = chunk1.len() + chunk2.len();

    let gate = Arc::clone(&state.stream_gate);
    let sent_notify = Arc::clone(&state.stream_first_chunk_sent);

    let stream = async_stream_two_chunks(chunk1, chunk2, sent_notify, gate);

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .header(header::CONTENT_LENGTH, total_len)
        .body(Body::from_stream(stream))
        .unwrap()
}

fn async_stream_two_chunks(
    c1: Bytes,
    c2: Bytes,
    sent_notify: Arc<Notify>,
    gate: Arc<Notify>,
) -> impl futures_util::Stream<Item = Result<Bytes, std::io::Error>> + Send + 'static {
    futures_util::stream::unfold(0u8, move |step| {
        let c1 = c1.clone();
        let c2 = c2.clone();
        let sent_notify = Arc::clone(&sent_notify);
        let gate = Arc::clone(&gate);
        async move {
            match step {
                0 => {
                    sent_notify.notify_waiters();
                    Some((Ok(c1), 1))
                }
                1 => {
                    gate.notified().await;
                    Some((Ok(c2), 2))
                }
                _ => None,
            }
        }
    })
}

async fn mock_truncated_stream(
    State(state): State<MockUpstreamState>,
    Path(_filename): Path<String>,
) -> Response {
    state.file_hits.fetch_add(1, Ordering::SeqCst);
    // Claims 500 bytes in Content-Length, only yields 50 bytes then EOF.
    let stream = futures_util::stream::once(async { Ok::<Bytes, std::io::Error>(Bytes::from(vec![b'x'; 50])) });
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .header(header::CONTENT_LENGTH, 500)
        .body(Body::from_stream(stream))
        .unwrap()
}

async fn start_proxy_server(
    upstream_addr: SocketAddr,
    cache_dir: std::path::PathBuf,
    max_cache_size: u64,
    upstream_proxy: Option<String>,
) -> (SocketAddr, Arc<AppState>) {
    let config = Config {
        listen: "127.0.0.1:0".parse().unwrap(),
        cache_dir,
        max_cache_size,
        evict_high_watermark: 1.0,
        evict_low_watermark: 0.85,
        upstream: Url::parse(&format!("http://{upstream_addr}/simple/")).unwrap(),
        upstream_packages_url: Some(Url::parse(&format!("http://{upstream_addr}/files/")).unwrap()),
        allowed_upstream_hosts: vec![],
        upstream_proxy,
        no_proxy: None,
        no_system_proxy: true,
        ca_cert: None,
        danger_accept_invalid_certs: false,
        index_ttl: Duration::from_secs(60),
        index_cache_max_bytes: 16 * 1024 * 1024,
        connect_timeout: Duration::from_secs(10),
        read_timeout: Duration::from_secs(10),
        log_level: "warn".to_string(),
    };

    let state = AppState::build(config).await.unwrap();
    let router = build_router(Arc::clone(&state));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });

    (addr, state)
}

fn direct_client() -> reqwest::Client {
    reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap()
}

async fn resp_json(r: reqwest::Response) -> serde_json::Value {
    let bytes = r.bytes().await.unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

async fn wait_in_flight_idle(state: &AppState) {
    for _ in 0..200 {
        if state.downloader.in_flight_count() == 0 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("timed out waiting for in-flight downloads to commit");
}

async fn fetch_pkg(client: &reqwest::Client, url: &str) -> (String, Bytes) {
    let resp = client.get(url).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "unexpected status for {url}");
    let x_cache = resp
        .headers()
        .get("x-cache")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let body = resp.bytes().await.unwrap();
    (x_cache, body)
}

#[tokio::test]
async fn test_simple_json_html_and_metadata_caching() {
    let mut files = HashMap::new();
    files.insert("demo-1.0-py3-none-any.whl".into(), b"WHEEL_BYTES_123456".to_vec());
    files.insert(
        "demo-1.0-py3-none-any.whl.metadata".into(),
        b"Metadata-Version: 2.1\nName: demo\nVersion: 1.0\n".to_vec(),
    );

    let (up_addr, up_state) = start_mock_upstream(files).await;
    let tmp_dir = tempfile::tempdir().unwrap();
    let (proxy_addr, state) =
        start_proxy_server(up_addr, tmp_dir.path().to_path_buf(), 10 * 1024 * 1024, None).await;
    let client = direct_client();

    // 1. Missing trailing slash redirects to normalized path
    let r = client
        .get(format!("http://{proxy_addr}/simple/Demo_Pkg"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::PERMANENT_REDIRECT);
    assert_eq!(r.headers().get(header::LOCATION).unwrap(), "/simple/demo-pkg/");

    // 2. PEP 691 JSON Simple API
    let r = client
        .get(format!("http://{proxy_addr}/simple/demo/"))
        .header(
            header::ACCEPT,
            "application/vnd.pypi.simple.v1+json, application/vnd.pypi.simple.v1+html;q=0.2",
        )
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::OK);
    assert_eq!(r.headers().get("x-cache").unwrap(), "MISS");
    let etag = r.headers().get(header::ETAG).unwrap().to_str().unwrap().to_string();
    let json = resp_json(r).await;
    let wheel_url = json["files"][0]["url"].as_str().unwrap();
    assert_eq!(wheel_url, "/packages/demo-1.0-py3-none-any.whl");

    // 3. Second JSON lookup hits index cache and supports 304 Not Modified
    let r = client
        .get(format!("http://{proxy_addr}/simple/demo/"))
        .header(header::ACCEPT, "application/vnd.pypi.simple.v1+json")
        .header(header::IF_NONE_MATCH, &etag)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::NOT_MODIFIED);
    assert_eq!(r.headers().get("x-cache").unwrap(), "HIT");
    assert_eq!(up_state.index_hits.load(Ordering::SeqCst), 1);

    // 4. PEP 503 HTML Simple API rewrites href and preserves fragment & attributes
    let r = client
        .get(format!("http://{proxy_addr}/simple/demo/"))
        .header(header::ACCEPT, "text/html")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::OK);
    let html = r.text().await.unwrap();
    assert!(html.contains(r#"href="/packages/demo-1.0-py3-none-any.whl#sha256=0000""#));
    assert!(html.contains(r#"data-core-metadata="true""#));

    // 5. Download wheel (MISS -> HIT)
    let (xc1, b1) = fetch_pkg(&client, &format!("http://{proxy_addr}{wheel_url}")).await;
    assert_eq!(xc1, "MISS");
    assert_eq!(b1, Bytes::from_static(b"WHEEL_BYTES_123456"));
    wait_in_flight_idle(&state).await;

    let (xc2, b2) = fetch_pkg(&client, &format!("http://{proxy_addr}{wheel_url}")).await;
    assert_eq!(xc2, "HIT");
    assert_eq!(b2, Bytes::from_static(b"WHEEL_BYTES_123456"));
    assert_eq!(up_state.file_hits.load(Ordering::SeqCst), 1);

    // 6. PEP 658 .metadata sidecar works by appending `.metadata` to rewritten URL
    let (mxc1, mb1) = fetch_pkg(&client, &format!("http://{proxy_addr}{wheel_url}.metadata")).await;
    assert_eq!(mxc1, "MISS");
    assert!(String::from_utf8_lossy(&mb1).contains("Name: demo"));
    wait_in_flight_idle(&state).await;

    let (mxc2, mb2) = fetch_pkg(&client, &format!("http://{proxy_addr}{wheel_url}.metadata")).await;
    assert_eq!(mxc2, "HIT");
    assert!(String::from_utf8_lossy(&mb2).contains("Name: demo"));
    assert_eq!(up_state.file_hits.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn test_simultaneous_streaming_while_downloading() {
    let (up_addr, up_state) = start_mock_upstream(HashMap::new()).await;
    let tmp_dir = tempfile::tempdir().unwrap();
    let (proxy_addr, state) =
        start_proxy_server(up_addr, tmp_dir.path().to_path_buf(), 10 * 1024 * 1024, None).await;
    let client = direct_client();

    let url = format!("http://{proxy_addr}/proxy/http/{up_addr}/gated/live.whl");
    let resp = client.get(&url).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(resp.headers().get("x-cache").unwrap(), "MISS");

    let mut stream = resp.bytes_stream();

    // Downstream client MUST receive chunk 1 BEFORE upstream is allowed to send chunk 2!
    let first = tokio::time::timeout(Duration::from_secs(3), stream.next())
        .await
        .expect("timed out waiting for live streamed first chunk")
        .expect("stream ended prematurely")
        .unwrap();
    assert_eq!(first, Bytes::from_static(b"FIRST_CHUNK_DATA_"));

    // Now unblock upstream chunk 2
    up_state.stream_gate.notify_waiters();

    let mut rest = Vec::new();
    while let Some(chunk) = stream.next().await {
        rest.extend_from_slice(&chunk.unwrap());
    }
    assert_eq!(rest, b"SECOND_CHUNK_DATA");

    wait_in_flight_idle(&state).await;

    // Subsequent request MUST hit disk cache without touching upstream again
    let (xc, body) = fetch_pkg(&client, &url).await;
    assert_eq!(xc, "HIT");
    assert_eq!(body, Bytes::from_static(b"FIRST_CHUNK_DATA_SECOND_CHUNK_DATA"));
    assert_eq!(up_state.file_hits.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn test_concurrent_request_coalescing() {
    let (up_addr, up_state) = start_mock_upstream(HashMap::new()).await;
    let tmp_dir = tempfile::tempdir().unwrap();
    let (proxy_addr, _state) =
        start_proxy_server(up_addr, tmp_dir.path().to_path_buf(), 10 * 1024 * 1024, None).await;

    let url = format!("http://{proxy_addr}/proxy/http/{up_addr}/gated/shared.whl");
    let client = direct_client();

    let mut handles = Vec::new();
    for _ in 0..5 {
        let c = client.clone();
        let u = url.clone();
        handles.push(tokio::spawn(async move {
            let r = c.get(&u).send().await.unwrap();
            assert_eq!(r.status(), StatusCode::OK);
            let x_cache = r.headers().get("x-cache").unwrap().to_str().unwrap().to_string();
            let body = r.bytes().await.unwrap();
            (x_cache, body)
        }));
    }

    // Wait briefly for all 5 requests to attach to the single in-flight download, then release gate.
    tokio::time::sleep(Duration::from_millis(150)).await;
    up_state.stream_gate.notify_waiters();

    let mut miss_count = 0;
    let mut coalesced_count = 0;
    for h in handles {
        let (x_cache, body) = h.await.unwrap();
        assert_eq!(body, Bytes::from_static(b"FIRST_CHUNK_DATA_SECOND_CHUNK_DATA"));
        match x_cache.as_str() {
            "MISS" => miss_count += 1,
            "COALESCED" => coalesced_count += 1,
            other => panic!("unexpected X-Cache header: {other}"),
        }
    }

    assert_eq!(up_state.file_hits.load(Ordering::SeqCst), 1);
    assert_eq!(miss_count, 1);
    assert_eq!(coalesced_count, 4);
}

#[tokio::test]
async fn test_range_requests_for_uv() {
    let payload: Vec<u8> = (0..100u8).collect();
    let mut files = HashMap::new();
    files.insert("wheel-1.0.whl".into(), payload.clone());

    let (up_addr, up_state) = start_mock_upstream(files).await;
    let tmp_dir = tempfile::tempdir().unwrap();
    let (proxy_addr, state) =
        start_proxy_server(up_addr, tmp_dir.path().to_path_buf(), 10 * 1024 * 1024, None).await;
    let client = direct_client();
    let url = format!("http://{proxy_addr}/packages/wheel-1.0.whl");

    // 1. Suffix range on cache MISS -> bypasses disk cache, returns 206 Partial Content
    let r = client
        .get(&url)
        .header(header::RANGE, "bytes=-10")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(r.headers().get("x-cache").unwrap(), "BYPASS");
    assert_eq!(r.headers().get(header::CONTENT_RANGE).unwrap(), "bytes 90-99/100");
    assert_eq!(r.bytes().await.unwrap().as_ref(), &payload[90..100]);

    // 2. Full GET populates disk cache
    let (xc, body) = fetch_pkg(&client, &url).await;
    assert_eq!(xc, "MISS");
    assert_eq!(body.as_ref(), payload.as_slice());
    assert_eq!(up_state.file_hits.load(Ordering::SeqCst), 2);
    wait_in_flight_idle(&state).await;

    // 3. Suffix range on cache HIT -> served from disk cache (upstream untouched!)
    let r = client
        .get(&url)
        .header(header::RANGE, "bytes=-16")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(r.headers().get("x-cache").unwrap(), "HIT");
    assert_eq!(r.headers().get(header::CONTENT_RANGE).unwrap(), "bytes 84-99/100");
    assert_eq!(r.bytes().await.unwrap().as_ref(), &payload[84..100]);
    assert_eq!(up_state.file_hits.load(Ordering::SeqCst), 2);

    // 4. Unsatisfiable range on cache HIT -> 416
    let r = client
        .get(&url)
        .header(header::RANGE, "bytes=500-600")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::RANGE_NOT_SATISFIABLE);
}

#[tokio::test]
async fn test_disk_quota_prunes_oldest_by_access_time() {
    let mut files = HashMap::new();
    files.insert("a.whl".into(), vec![b'a'; 400]);
    files.insert("b.whl".into(), vec![b'b'; 400]);
    files.insert("c.whl".into(), vec![b'c'; 400]);

    let (up_addr, _up_state) = start_mock_upstream(files).await;
    let tmp_dir = tempfile::tempdir().unwrap();
    // Max cache size 1000 bytes: fits 2 packages (800B), adding 3rd (1200B > 1000B) prunes oldest.
    let (proxy_addr, state) =
        start_proxy_server(up_addr, tmp_dir.path().to_path_buf(), 1000, None).await;
    let client = direct_client();

    // Cache a.whl and b.whl
    let (xc, b) = fetch_pkg(&client, &format!("http://{proxy_addr}/packages/a.whl")).await;
    assert_eq!(xc, "MISS");
    assert_eq!(b.len(), 400);
    wait_in_flight_idle(&state).await;

    let (xc, b) = fetch_pkg(&client, &format!("http://{proxy_addr}/packages/b.whl")).await;
    assert_eq!(xc, "MISS");
    assert_eq!(b.len(), 400);
    wait_in_flight_idle(&state).await;

    // Touch a.whl so b.whl becomes oldest by access time
    let (xc, _) = fetch_pkg(&client, &format!("http://{proxy_addr}/packages/a.whl")).await;
    assert_eq!(xc, "HIT");

    // Download c.whl -> triggers LRU pruning, evicting b.whl
    let (xc, b) = fetch_pkg(&client, &format!("http://{proxy_addr}/packages/c.whl")).await;
    assert_eq!(xc, "MISS");
    assert_eq!(b.len(), 400);
    wait_in_flight_idle(&state).await;

    // a.whl and c.whl remain HIT; b.whl was evicted (becomes MISS)
    let (xc, _) = fetch_pkg(&client, &format!("http://{proxy_addr}/packages/a.whl")).await;
    assert_eq!(xc, "HIT");
    let (xc, _) = fetch_pkg(&client, &format!("http://{proxy_addr}/packages/c.whl")).await;
    assert_eq!(xc, "HIT");

    let stats = resp_json(
        client
            .get(format!("http://{proxy_addr}/stats"))
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(stats["cached_packages"].as_u64().unwrap(), 2);
    assert_eq!(stats["cache_bytes_used"].as_u64().unwrap(), 800);
    assert_eq!(stats["evictions"].as_u64().unwrap(), 1);

    let (xc, _) = fetch_pkg(&client, &format!("http://{proxy_addr}/packages/b.whl")).await;
    assert_eq!(xc, "MISS");
}

/// Starts a minimal HTTP forward proxy supporting absolute-URI forwarding and Basic auth validation.
async fn start_local_http_proxy(
    expected_auth: Option<String>,
) -> (SocketAddr, Arc<AtomicUsize>) {
    let hits = Arc::new(AtomicUsize::new(0));
    let hits_clone = Arc::clone(&hits);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        while let Ok((socket, _)) = listener.accept().await {
            let hits = Arc::clone(&hits_clone);
            let expected_auth = expected_auth.clone();
            tokio::spawn(async move {
                let mut reader = BufReader::new(socket);
                let mut request_line = String::new();
                if reader.read_line(&mut request_line).await.unwrap_or(0) == 0 {
                    return;
                }
                let parts: Vec<&str> = request_line.split_whitespace().collect();
                if parts.len() < 3 {
                    return;
                }
                let method = parts[0].to_string();
                let target = parts[1].to_string();

                let mut headers_raw = Vec::new();
                let mut proxy_auth = None;
                loop {
                    let mut line = String::new();
                    if reader.read_line(&mut line).await.unwrap_or(0) == 0 {
                        break;
                    }
                    if line == "\r\n" || line == "\n" {
                        break;
                    }
                    if let Some((k, v)) = line.split_once(':') {
                        if k.eq_ignore_ascii_case("proxy-authorization") {
                            proxy_auth = Some(v.trim().to_string());
                            continue;
                        }
                        if k.eq_ignore_ascii_case("proxy-connection") {
                            continue;
                        }
                    }
                    headers_raw.push(line);
                }

                let mut client_socket = reader.into_inner();
                if let Some(ref exp) = expected_auth
                    && proxy_auth.as_deref() != Some(exp.as_str())
                {
                    let _ = client_socket
                        .write_all(b"HTTP/1.1 407 Proxy Authentication Required\r\nContent-Length: 0\r\n\r\n")
                        .await;
                    return;
                }

                hits.fetch_add(1, Ordering::SeqCst);

                let Ok(url) = Url::parse(&target) else {
                    let _ = client_socket
                        .write_all(b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n")
                        .await;
                    return;
                };
                let host = url.host_str().unwrap_or("127.0.0.1");
                let port = url.port_or_known_default().unwrap_or(80);
                let path_and_query = match url.query() {
                    Some(q) => format!("{}?{q}", url.path()),
                    None => url.path().to_string(),
                };

                let Ok(mut upstream) = tokio::net::TcpStream::connect((host, port)).await else {
                    let _ = client_socket
                        .write_all(b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n\r\n")
                        .await;
                    return;
                };

                let mut out = format!("{method} {path_and_query} HTTP/1.1\r\n");
                for h in headers_raw {
                    if !h.to_ascii_lowercase().starts_with("connection:") {
                        out.push_str(&h);
                    }
                }
                out.push_str("Connection: close\r\n\r\n");
                let _ = upstream.write_all(out.as_bytes()).await;

                let mut buf = [0u8; 16384];
                loop {
                    match upstream.read(&mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(n) => {
                            if client_socket.write_all(&buf[..n]).await.is_err() {
                                break;
                            }
                        }
                    }
                }
            });
        }
    });

    (addr, hits)
}

#[tokio::test]
async fn test_upstream_http_proxy_with_auth() {
    let mut files = HashMap::new();
    files.insert("proxied-1.0.whl".into(), b"THROUGH_HTTP_PROXY_OK".to_vec());

    let (up_addr, _up_state) = start_mock_upstream(files).await;
    // base64("alice:secret") == "YWxpY2U6c2VjcmV0"
    let (http_proxy_addr, proxy_hits) =
        start_local_http_proxy(Some("Basic YWxpY2U6c2VjcmV0".to_string())).await;

    let tmp_dir = tempfile::tempdir().unwrap();
    let (cacher_addr, state) = start_proxy_server(
        up_addr,
        tmp_dir.path().to_path_buf(),
        10 * 1024 * 1024,
        Some(format!("http://alice:secret@{http_proxy_addr}")),
    )
    .await;

    let client = direct_client();

    // 1. Index lookup goes through HTTP proxy
    let idx = client
        .get(format!("http://{cacher_addr}/simple/proxied/"))
        .header(header::ACCEPT, "application/vnd.pypi.simple.v1+json")
        .send()
        .await
        .unwrap();
    assert_eq!(idx.status(), StatusCode::OK);
    let json = resp_json(idx).await;
    let wheel_path = json["files"][0]["url"].as_str().unwrap();

    // 2. Package download goes through HTTP proxy on MISS
    let (xc1, b1) = fetch_pkg(&client, &format!("http://{cacher_addr}{wheel_path}")).await;
    assert_eq!(xc1, "MISS");
    assert_eq!(b1, Bytes::from_static(b"THROUGH_HTTP_PROXY_OK"));
    assert_eq!(proxy_hits.load(Ordering::SeqCst), 2);
    wait_in_flight_idle(&state).await;

    // 3. Second package download is served from disk cache (HTTP proxy NOT hit again!)
    let (xc2, b2) = fetch_pkg(&client, &format!("http://{cacher_addr}{wheel_path}")).await;
    assert_eq!(xc2, "HIT");
    assert_eq!(b2, Bytes::from_static(b"THROUGH_HTTP_PROXY_OK"));
    assert_eq!(proxy_hits.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn test_ssrf_oversized_and_truncated_safety() {
    let mut files = HashMap::new();
    files.insert("huge.whl".into(), vec![b'Z'; 2000]);

    let (up_addr, _up_state) = start_mock_upstream(files).await;
    let tmp_dir = tempfile::tempdir().unwrap();
    let (proxy_addr, _state) =
        start_proxy_server(up_addr, tmp_dir.path().to_path_buf(), 500, None).await;
    let client = direct_client();

    // 1. Disallowed upstream host rejected with 403
    let ssrf = client
        .get(format!(
            "http://{proxy_addr}/proxy/http/169.254.169.254/latest/meta-data/"
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(ssrf.status(), StatusCode::FORBIDDEN);

    // 2. Oversized package (> max_cache_size) streams cleanly with BYPASS
    let huge = client
        .get(format!("http://{proxy_addr}/packages/huge.whl"))
        .send()
        .await
        .unwrap();
    assert_eq!(huge.status(), StatusCode::OK);
    assert_eq!(huge.headers().get("x-cache").unwrap(), "BYPASS");
    assert_eq!(huge.bytes().await.unwrap().len(), 2000);

    // 3. Truncated upstream response is not committed to cache
    let trunc = client
        .get(format!(
            "http://{proxy_addr}/proxy/http/{up_addr}/truncated/bad.whl"
        ))
        .send()
        .await
        .unwrap();
    let _ = trunc.bytes().await; // Expect stream error / incomplete
    tokio::time::sleep(Duration::from_millis(50)).await;

    let stats = resp_json(
        client
            .get(format!("http://{proxy_addr}/stats"))
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(stats["cached_packages"].as_u64().unwrap(), 0);
}

#[tokio::test]
async fn test_rapid_back_to_back_requests_never_fail() {
    let mut files = HashMap::new();
    for i in 0..20 {
        files.insert(format!("tiny-{i}.whl"), format!("PAYLOAD_{i}").into_bytes());
    }

    let (up_addr, up_state) = start_mock_upstream(files).await;
    let tmp_dir = tempfile::tempdir().unwrap();
    let (proxy_addr, _state) =
        start_proxy_server(up_addr, tmp_dir.path().to_path_buf(), 10 * 1024 * 1024, None).await;
    let client = direct_client();

    // Immediately fire back-to-back requests with zero delay between them to stress the commit rename window.
    for i in 0..20 {
        let url = format!("http://{proxy_addr}/packages/tiny-{i}.whl");
        let expected = Bytes::from(format!("PAYLOAD_{i}"));
        let (_, b1) = fetch_pkg(&client, &url).await;
        assert_eq!(b1, expected);
        let (_, b2) = fetch_pkg(&client, &url).await;
        assert_eq!(b2, expected);
        let (_, b3) = fetch_pkg(&client, &url).await;
        assert_eq!(b3, expected);
    }

    // Each of the 20 packages should have hit upstream AT MOST once!
    assert_eq!(up_state.file_hits.load(Ordering::SeqCst), 20);
}
