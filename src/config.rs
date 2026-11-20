use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use clap::Parser;
use serde::Deserialize;
use url::Url;

pub const DEFAULT_LISTEN: &str = "0.0.0.0:8080";
pub const DEFAULT_CACHE_DIR: &str = "./cache";
pub const DEFAULT_MAX_CACHE_SIZE: u64 = 10 * 1024 * 1024 * 1024; // 10 GiB
pub const DEFAULT_EVICT_HIGH_WATERMARK: f64 = 0.95;
pub const DEFAULT_EVICT_LOW_WATERMARK: f64 = 0.85;
pub const DEFAULT_UPSTREAM: &str = "https://pypi.org/simple/";
pub const DEFAULT_INDEX_TTL_SECS: u64 = 60;
pub const DEFAULT_INDEX_CACHE_MAX_BYTES: u64 = 128 * 1024 * 1024; // 128 MiB
pub const DEFAULT_CONNECT_TIMEOUT_SECS: u64 = 30;
pub const DEFAULT_READ_TIMEOUT_SECS: u64 = 60;

#[derive(Debug, Parser)]
#[command(
    name = "pypicacher-rs",
    version,
    about = "High-performance PyPI caching proxy for uv and pip"
)]
pub struct CliArgs {
    /// Path to optional TOML configuration file
    #[arg(long, env = "PYPICACHER_CONFIG")]
    pub config: Option<PathBuf>,

    /// Address and port to listen on (e.g. 0.0.0.0:8080)
    #[arg(short = 'l', long, env = "PYPICACHER_LISTEN")]
    pub listen: Option<SocketAddr>,

    /// Directory to store cached package files on disk
    #[arg(short = 'c', long, env = "PYPICACHER_CACHE_DIR")]
    pub cache_dir: Option<PathBuf>,

    /// Maximum disk cache size (e.g. 10GiB, 500M, 20GB, or raw bytes)
    #[arg(short = 'm', long, env = "PYPICACHER_MAX_CACHE_SIZE")]
    pub max_cache_size: Option<String>,

    /// Start proactive LRU pruning when cache usage exceeds this fraction of max size (0.0..=1.0)
    #[arg(long, env = "PYPICACHER_EVICT_HIGH_WATERMARK")]
    pub evict_high_watermark: Option<f64>,

    /// Target fraction of max cache size after LRU pruning (0.0..=evict_high_watermark)
    #[arg(long, env = "PYPICACHER_EVICT_LOW_WATERMARK")]
    pub evict_low_watermark: Option<f64>,

    /// Upstream PyPI Simple index URL (default: https://pypi.org/simple/)
    #[arg(short = 'u', long, env = "PYPICACHER_UPSTREAM")]
    pub upstream: Option<String>,

    /// Optional upstream packages base URL mapped to /packages/ (auto-detected for pypi.org)
    #[arg(long, env = "PYPICACHER_UPSTREAM_PACKAGES_URL")]
    pub upstream_packages_url: Option<String>,

    /// Additional allowed upstream hosts for proxied package links (comma-separated)
    #[arg(long, env = "PYPICACHER_ALLOWED_UPSTREAM_HOSTS", value_delimiter = ',')]
    pub allowed_upstream_hosts: Option<Vec<String>>,

    /// Upstream HTTP/HTTPS/SOCKS5 proxy URL (e.g. http://proxy.lan:3128 or http://user:pass@proxy:3128)
    #[arg(short = 'p', long, env = "PYPICACHER_UPSTREAM_PROXY")]
    pub upstream_proxy: Option<String>,

    /// Comma-separated host list to bypass the upstream proxy
    #[arg(long, env = "PYPICACHER_NO_PROXY")]
    pub no_proxy: Option<String>,

    /// Disable reading system HTTP_PROXY / HTTPS_PROXY environment variables
    #[arg(long, env = "PYPICACHER_NO_SYSTEM_PROXY")]
    pub no_system_proxy: bool,

    /// Optional custom PEM CA certificate file for upstream / HTTPS proxy connections
    #[arg(long, env = "PYPICACHER_CA_CERT")]
    pub ca_cert: Option<PathBuf>,

    /// Accept invalid TLS certificates when connecting upstream (INSECURE - testing only)
    #[arg(long, env = "PYPICACHER_DANGER_ACCEPT_INVALID_CERTS")]
    pub danger_accept_invalid_certs: bool,

    /// TTL for cached /simple/<project>/ index pages (e.g. 60s, 5m, 0s)
    #[arg(long, env = "PYPICACHER_INDEX_TTL")]
    pub index_ttl: Option<String>,

    /// Maximum memory used by the in-memory /simple/ index cache (e.g. 128MiB)
    #[arg(long, env = "PYPICACHER_INDEX_CACHE_MAX_BYTES")]
    pub index_cache_max_bytes: Option<String>,

    /// Upstream TCP/TLS connect timeout (e.g. 30s)
    #[arg(long, env = "PYPICACHER_CONNECT_TIMEOUT")]
    pub connect_timeout: Option<String>,

    /// Upstream per-chunk read timeout (e.g. 60s)
    #[arg(long, env = "PYPICACHER_READ_TIMEOUT")]
    pub read_timeout: Option<String>,

    /// Log filter directive (e.g. info, debug, pypicacher_rs=debug)
    #[arg(long, env = "PYPICACHER_LOG_LEVEL")]
    pub log_level: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct FileConfig {
    listen: Option<SocketAddr>,
    cache_dir: Option<PathBuf>,
    max_cache_size: Option<String>,
    evict_high_watermark: Option<f64>,
    evict_low_watermark: Option<f64>,
    upstream: Option<String>,
    upstream_packages_url: Option<String>,
    allowed_upstream_hosts: Option<Vec<String>>,
    upstream_proxy: Option<String>,
    no_proxy: Option<String>,
    no_system_proxy: Option<bool>,
    ca_cert: Option<PathBuf>,
    danger_accept_invalid_certs: Option<bool>,
    index_ttl: Option<String>,
    index_cache_max_bytes: Option<String>,
    connect_timeout: Option<String>,
    read_timeout: Option<String>,
    log_level: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub listen: SocketAddr,
    pub cache_dir: PathBuf,
    pub max_cache_size: u64,
    pub evict_high_watermark: f64,
    pub evict_low_watermark: f64,
    pub upstream: Url,
    pub upstream_packages_url: Option<Url>,
    pub allowed_upstream_hosts: Vec<String>,
    pub upstream_proxy: Option<String>,
    pub no_proxy: Option<String>,
    pub no_system_proxy: bool,
    pub ca_cert: Option<PathBuf>,
    pub danger_accept_invalid_certs: bool,
    pub index_ttl: Duration,
    pub index_cache_max_bytes: u64,
    pub connect_timeout: Duration,
    pub read_timeout: Duration,
    pub log_level: String,
}

impl Config {
    pub fn from_cli(args: CliArgs) -> Result<Self> {
        let file_cfg = match &args.config {
            Some(path) => load_toml_config(path)?,
            None => FileConfig::default(),
        };

        let listen = args
            .listen
            .or(file_cfg.listen)
            .unwrap_or_else(|| DEFAULT_LISTEN.parse().expect("valid default listen addr"));

        let cache_dir = args
            .cache_dir
            .or(file_cfg.cache_dir)
            .unwrap_or_else(|| PathBuf::from(DEFAULT_CACHE_DIR));

        let max_cache_size = match args.max_cache_size.or(file_cfg.max_cache_size) {
            Some(s) => parse_byte_size(&s)
                .with_context(|| format!("invalid max_cache_size '{s}'"))?,
            None => DEFAULT_MAX_CACHE_SIZE,
        };
        if max_cache_size == 0 {
            bail!("max_cache_size must be greater than 0");
        }

        let evict_high_watermark = args
            .evict_high_watermark
            .or(file_cfg.evict_high_watermark)
            .unwrap_or(DEFAULT_EVICT_HIGH_WATERMARK);
        let evict_low_watermark = args
            .evict_low_watermark
            .or(file_cfg.evict_low_watermark)
            .unwrap_or(DEFAULT_EVICT_LOW_WATERMARK);

        if !(evict_high_watermark > 0.0 && evict_high_watermark <= 1.0) {
            bail!("evict_high_watermark must be in (0.0, 1.0], got {evict_high_watermark}");
        }
        if !(evict_low_watermark > 0.0 && evict_low_watermark <= evict_high_watermark) {
            bail!(
                "evict_low_watermark ({evict_low_watermark}) must be in (0.0, evict_high_watermark ({evict_high_watermark})]"
            );
        }

        let upstream_raw = args
            .upstream
            .or(file_cfg.upstream)
            .unwrap_or_else(|| DEFAULT_UPSTREAM.to_string());
        let upstream = normalize_base_url(&upstream_raw)
            .with_context(|| format!("invalid upstream URL '{upstream_raw}'"))?;

        let upstream_packages_url = match args
            .upstream_packages_url
            .or(file_cfg.upstream_packages_url)
        {
            Some(raw) if !raw.trim().is_empty() => Some(
                normalize_base_url(&raw)
                    .with_context(|| format!("invalid upstream_packages_url '{raw}'"))?,
            ),
            _ => default_packages_url(&upstream),
        };

        let allowed_upstream_hosts = args
            .allowed_upstream_hosts
            .or(file_cfg.allowed_upstream_hosts)
            .unwrap_or_default()
            .into_iter()
            .map(|h| h.trim().to_ascii_lowercase())
            .filter(|h| !h.is_empty())
            .collect();

        let upstream_proxy = args
            .upstream_proxy
            .or(file_cfg.upstream_proxy)
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());

        let no_proxy = args
            .no_proxy
            .or(file_cfg.no_proxy)
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());

        let no_system_proxy = args.no_system_proxy || file_cfg.no_system_proxy.unwrap_or(false);
        let ca_cert = args.ca_cert.or(file_cfg.ca_cert);
        let danger_accept_invalid_certs = args.danger_accept_invalid_certs
            || file_cfg.danger_accept_invalid_certs.unwrap_or(false);

        let index_ttl = match args.index_ttl.or(file_cfg.index_ttl) {
            Some(s) => humantime::parse_duration(&s)
                .with_context(|| format!("invalid index_ttl '{s}'"))?,
            None => Duration::from_secs(DEFAULT_INDEX_TTL_SECS),
        };

        let index_cache_max_bytes = match args
            .index_cache_max_bytes
            .or(file_cfg.index_cache_max_bytes)
        {
            Some(s) => parse_byte_size(&s)
                .with_context(|| format!("invalid index_cache_max_bytes '{s}'"))?,
            None => DEFAULT_INDEX_CACHE_MAX_BYTES,
        };

        let connect_timeout = match args.connect_timeout.or(file_cfg.connect_timeout) {
            Some(s) => humantime::parse_duration(&s)
                .with_context(|| format!("invalid connect_timeout '{s}'"))?,
            None => Duration::from_secs(DEFAULT_CONNECT_TIMEOUT_SECS),
        };

        let read_timeout = match args.read_timeout.or(file_cfg.read_timeout) {
            Some(s) => humantime::parse_duration(&s)
                .with_context(|| format!("invalid read_timeout '{s}'"))?,
            None => Duration::from_secs(DEFAULT_READ_TIMEOUT_SECS),
        };

        let log_level = args
            .log_level
            .or(file_cfg.log_level)
            .unwrap_or_else(|| "info".to_string());

        Ok(Self {
            listen,
            cache_dir,
            max_cache_size,
            evict_high_watermark,
            evict_low_watermark,
            upstream,
            upstream_packages_url,
            allowed_upstream_hosts,
            upstream_proxy,
            no_proxy,
            no_system_proxy,
            ca_cert,
            danger_accept_invalid_certs,
            index_ttl,
            index_cache_max_bytes,
            connect_timeout,
            read_timeout,
            log_level,
        })
    }
}

fn load_toml_config(path: &Path) -> Result<FileConfig> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read config file {}", path.display()))?;
    toml::from_str(&raw)
        .with_context(|| format!("failed to parse TOML config file {}", path.display()))
}

pub fn normalize_base_url(raw: &str) -> Result<Url> {
    let mut url = Url::parse(raw.trim())?;
    match url.scheme() {
        "http" | "https" => {}
        other => bail!("unsupported URL scheme '{other}', expected http or https"),
    }
    if !url.path().ends_with('/') {
        let new_path = format!("{}/", url.path());
        url.set_path(&new_path);
    }
    Ok(url)
}

fn default_packages_url(upstream: &Url) -> Option<Url> {
    match upstream.host_str()?.to_ascii_lowercase().as_str() {
        "pypi.org" => Url::parse("https://files.pythonhosted.org/packages/").ok(),
        "test.pypi.org" => Url::parse("https://test-files.pythonhosted.org/packages/").ok(),
        _ => None,
    }
}

/// Parses human-readable byte sizes such as `10GiB`, `500M`, `20GB`, `1024KiB`, or `1048576`.
pub fn parse_byte_size(input: &str) -> Result<u64> {
    let cleaned: String = input
        .trim()
        .chars()
        .filter(|c| !c.is_whitespace() && *c != '_')
        .collect();
    if cleaned.is_empty() {
        bail!("empty byte size");
    }

    let split_idx = cleaned
        .find(|c: char| !c.is_ascii_digit() && c != '.')
        .unwrap_or(cleaned.len());
    let (num_str, unit_str) = cleaned.split_at(split_idx);

    if num_str.is_empty() {
        bail!("missing numeric value in '{input}'");
    }

    let multiplier: u128 = match unit_str.to_ascii_lowercase().as_str() {
        "" | "b" => 1,
        "k" | "kib" => 1024,
        "kb" => 1_000,
        "m" | "mib" => 1024_u128.pow(2),
        "mb" => 1_000_u128.pow(2),
        "g" | "gib" => 1024_u128.pow(3),
        "gb" => 1_000_u128.pow(3),
        "t" | "tib" => 1024_u128.pow(4),
        "tb" => 1_000_u128.pow(4),
        other => bail!("unknown size unit '{other}' in '{input}'"),
    };

    if num_str.contains('.') {
        let val: f64 = num_str
            .parse()
            .with_context(|| format!("invalid number '{num_str}'"))?;
        if !val.is_finite() || val < 0.0 {
            bail!("invalid byte size '{input}'");
        }
        let bytes = val * (multiplier as f64);
        if bytes > u64::MAX as f64 {
            bail!("byte size overflow in '{input}'");
        }
        Ok(bytes.round() as u64)
    } else {
        let val: u128 = num_str
            .parse()
            .with_context(|| format!("invalid integer '{num_str}'"))?;
        let bytes = val
            .checked_mul(multiplier)
            .ok_or_else(|| anyhow::anyhow!("byte size overflow in '{input}'"))?;
        u64::try_from(bytes).map_err(|_| anyhow::anyhow!("byte size overflow in '{input}'"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_byte_size() {
        assert_eq!(parse_byte_size("1024").unwrap(), 1024);
        assert_eq!(parse_byte_size("512B").unwrap(), 512);
        assert_eq!(parse_byte_size("1K").unwrap(), 1024);
        assert_eq!(parse_byte_size("1KiB").unwrap(), 1024);
        assert_eq!(parse_byte_size("1KB").unwrap(), 1000);
        assert_eq!(parse_byte_size("10M").unwrap(), 10 * 1024 * 1024);
        assert_eq!(parse_byte_size("10MB").unwrap(), 10_000_000);
        assert_eq!(parse_byte_size("1.5GiB").unwrap(), 1_610_612_736);
        assert_eq!(parse_byte_size("2 G").unwrap(), 2 * 1024 * 1024 * 1024);
        assert!(parse_byte_size("").is_err());
        assert!(parse_byte_size("10XB").is_err());
    }

    #[test]
    fn test_normalize_base_url() {
        assert_eq!(
            normalize_base_url("https://pypi.org/simple").unwrap().as_str(),
            "https://pypi.org/simple/"
        );
        assert_eq!(
            normalize_base_url("https://pypi.org/simple/").unwrap().as_str(),
            "https://pypi.org/simple/"
        );
        assert!(normalize_base_url("ftp://pypi.org/simple/").is_err());
    }
}
