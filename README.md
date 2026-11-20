# pypicacher-rs

A high-performance, lightweight PyPI caching proxy written in Rust designed to accelerate `uv` and `pip` package installations across a LAN.

## Features

- **Full `uv` & `pip` compatibility**: Supports PEP 503 (HTML Simple API), PEP 691 (JSON Simple API content negotiation), PEP 658 (`.metadata` sidecar caching), and HTTP `Range` requests (`206 Partial Content` including suffix ranges such as `bytes=-16384` used by `uv` for wheel metadata inspection).
- **Simultaneous download & stream**: On a cache miss, chunks received from upstream PyPI are streamed immediately to the downstream client while being written to a temporary file on disk—no waiting for the full upstream download to finish before bytes start flowing.
- **Singleflight request coalescing**: Concurrent LAN clients requesting the same uncached package share a single upstream download and all stream concurrently at O(1) memory overhead.
- **Bounded disk cache with LRU pruning by access time**: Configurable max cache size (`--max-cache-size`) with high/low watermarks (`--evict-high-watermark`, `--evict-low-watermark`). Oldest packages by access time are pruned proactively when storage quota runs low, with crash-safe atomic commits and persisted access times across restarts.
- **Upstream HTTP proxy support**: Connects upstream via `--upstream-proxy` (`http://`, `https://`, `socks5://`, with optional `user:pass@` authentication) or standard `HTTP_PROXY`/`HTTPS_PROXY`/`NO_PROXY` environment variables, plus custom CA certificate bundles (`--ca-cert`).
- **Simple index caching & conditional revalidation**: Bounded in-memory LRU+TTL cache for rewritten `/simple/<project>/` responses with `ETag` / `If-None-Match` (`304 Not Modified`) and gzip/brotli compression.
- **Built-in observability**: `/healthz` and `/stats` JSON metrics endpoints plus `X-Cache` (`HIT`, `MISS`, `COALESCED`, `BYPASS`, `REVALIDATED`) response headers.

## Quick Start

### Build & Run

```bash
cargo build --release

./target/release/pypicacher-rs \
  --listen 0.0.0.0:8080 \
  --cache-dir /var/cache/pypicacher \
  --max-cache-size 20GiB
```

### Connecting Upstream Through an HTTP Proxy

```bash
./target/release/pypicacher-rs \
  --listen 0.0.0.0:8080 \
  --cache-dir /var/cache/pypicacher \
  --max-cache-size 20GiB \
  --upstream-proxy http://proxy.corp.lan:3128
```

Authenticated proxy and custom corporate CA bundle:

```bash
./target/release/pypicacher-rs \
  --upstream-proxy http://user:password@proxy.corp.lan:3128 \
  --ca-cert /etc/ssl/certs/corp-proxy-ca.pem
```

If `--upstream-proxy` is omitted, `pypicacher-rs` automatically honors `HTTP_PROXY`, `HTTPS_PROXY`, `ALL_PROXY`, and `NO_PROXY` from the environment (pass `--no-system-proxy` to disable).

## Pointing `uv` and `pip` to `pypicacher-rs`

Replace `192.168.1.10:8080` with your LAN host and port.

### `uv`

```bash
# Single command
uv pip install --default-index http://192.168.1.10:8080/simple/ requests

# Or via environment variable (applies to `uv pip`, `uv sync`, `uv add`, `uv run`)
export UV_DEFAULT_INDEX="http://192.168.1.10:8080/simple/"
```

Or in `uv.toml` / `pyproject.toml`:

```toml
[[tool.uv.index]]
url = "http://192.168.1.10:8080/simple/"
default = true
```

### `pip`

```bash
# Single command (--trusted-host is required by pip for plain HTTP on non-localhost)
pip install \
  --index-url http://192.168.1.10:8080/simple/ \
  --trusted-host 192.168.1.10 \
  requests

# Or via environment variables
export PIP_INDEX_URL="http://192.168.1.10:8080/simple/"
export PIP_TRUSTED_HOST="192.168.1.10"
```

Or in `~/.config/pip/pip.conf`:

```ini
[global]
index-url = http://192.168.1.10:8080/simple/
trusted-host = 192.168.1.10
```

## Configuration

Every setting can be passed via CLI flag, environment variable (`PYPICACHER_*`), or TOML file (`--config config.example.toml`). Precedence is: CLI/env > TOML file > defaults.

| CLI Flag | Env Variable | Default | Description |
| --- | --- | --- | --- |
| `-l, --listen` | `PYPICACHER_LISTEN` | `0.0.0.0:8080` | Bind address and port |
| `-c, --cache-dir` | `PYPICACHER_CACHE_DIR` | `./cache` | On-disk cache directory |
| `-m, --max-cache-size` | `PYPICACHER_MAX_CACHE_SIZE` | `10GiB` | Maximum disk cache size (`B`, `K`/`KiB`/`KB`, `M`/`MiB`/`MB`, `G`/`GiB`/`GB`, `T`/`TiB`/`TB`) |
| `--evict-high-watermark` | `PYPICACHER_EVICT_HIGH_WATERMARK` | `0.95` | Trigger proactive LRU eviction above this fraction of max size |
| `--evict-low-watermark` | `PYPICACHER_EVICT_LOW_WATERMARK` | `0.85` | Prune oldest entries down to this fraction of max size |
| `-u, --upstream` | `PYPICACHER_UPSTREAM` | `https://pypi.org/simple/` | Upstream Simple Repository API URL |
| `--upstream-packages-url` | `PYPICACHER_UPSTREAM_PACKAGES_URL` | auto | Upstream packages base URL mapped to `/packages/` |
| `--allowed-upstream-hosts` | `PYPICACHER_ALLOWED_UPSTREAM_HOSTS` | *(none)* | Extra allowed upstream hosts (SSRF protection) |
| `-p, --upstream-proxy` | `PYPICACHER_UPSTREAM_PROXY` | *(env)* | Explicit upstream HTTP/HTTPS/SOCKS5 proxy URL |
| `--no-proxy` | `PYPICACHER_NO_PROXY` | *(none)* | Comma-separated proxy bypass list |
| `--no-system-proxy` | `PYPICACHER_NO_SYSTEM_PROXY` | `false` | Ignore system `HTTP_PROXY`/`HTTPS_PROXY` env vars |
| `--ca-cert` | `PYPICACHER_CA_CERT` | *(none)* | Additional PEM CA certificate bundle |
| `--index-ttl` | `PYPICACHER_INDEX_TTL` | `60s` | TTL for cached `/simple/<project>/` responses |
| `--index-cache-max-bytes` | `PYPICACHER_INDEX_CACHE_MAX_BYTES` | `128MiB` | Max RAM for Simple index cache |
| `--connect-timeout` | `PYPICACHER_CONNECT_TIMEOUT` | `30s` | Upstream TCP/TLS connect timeout |
| `--read-timeout` | `PYPICACHER_READ_TIMEOUT` | `60s` | Per-chunk upstream read timeout |
| `--config` | `PYPICACHER_CONFIG` | *(none)* | Optional TOML config file path |

## Endpoints

- `GET /simple/` — Proxied root Simple index
- `GET /simple/<project>/` — Rewritten PEP 503 HTML / PEP 691 JSON project index
- `GET|HEAD /packages/<path>` — Cached/streamed package files & `.metadata` sidecars
- `GET|HEAD /proxy/<scheme>/<authority>/<path>` — SSRF-protected cached/streamed files for alternative/mirror upstream origins
- `GET /stats` — JSON cache, eviction, and bandwidth metrics
- `GET /healthz` — Health check (`{"status":"ok"}`)

## Running Tests

```bash
cargo test
```
