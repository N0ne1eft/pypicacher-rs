use std::time::Duration;

use anyhow::{Context, Result};
use reqwest::{Certificate, Client, ClientBuilder, NoProxy, Proxy, redirect};
use tracing::info;

use crate::config::Config;

/// Dual-client upstream HTTP pool:
/// - `index_client`: enables gzip/brotli/deflate for fast Simple API index fetches.
/// - `package_client`: disables transparent decompression so streamed package bytes
///   (and `.metadata` sidecars) preserve exact upstream `Content-Length` and hashes.
#[derive(Clone)]
pub struct UpstreamClients {
    pub index_client: Client,
    pub package_client: Client,
}

impl UpstreamClients {
    pub fn from_config(config: &Config) -> Result<Self> {
        let index_client = build_base_builder(config, true)?
            .build()
            .context("failed to build upstream index HTTP client")?;

        let package_client = build_base_builder(config, false)?
            .no_gzip()
            .no_brotli()
            .no_deflate()
            .build()
            .context("failed to build upstream package HTTP client")?;

        if let Some(ref proxy) = config.upstream_proxy {
            info!(proxy = %redact_proxy_url(proxy), "configured explicit upstream proxy");
        } else if config.no_system_proxy {
            info!("system HTTP proxy disabled (--no-system-proxy)");
        } else {
            info!("using system proxy environment variables if set (HTTP_PROXY/HTTPS_PROXY)");
        }

        Ok(Self {
            index_client,
            package_client,
        })
    }
}

fn build_base_builder(config: &Config, enable_compression: bool) -> Result<ClientBuilder> {
    let mut builder = Client::builder()
        .user_agent(format!("pypicacher-rs/{}", env!("CARGO_PKG_VERSION")))
        .connect_timeout(config.connect_timeout)
        .read_timeout(config.read_timeout)
        .pool_idle_timeout(Duration::from_secs(90))
        .pool_max_idle_per_host(64)
        .tcp_nodelay(true)
        .redirect(redirect::Policy::limited(10))
        .use_rustls_tls()
        .tls_built_in_native_certs(true)
        .tls_built_in_webpki_certs(true);

    if enable_compression {
        builder = builder.gzip(true).brotli(true).deflate(true);
    }

    if let Some(ref proxy_raw) = config.upstream_proxy {
        if proxy_raw.eq_ignore_ascii_case("none") {
            builder = builder.no_proxy();
        } else {
            let mut proxy = Proxy::all(proxy_raw)
                .with_context(|| format!("invalid upstream proxy URL '{}'", redact_proxy_url(proxy_raw)))?;
            if let Some(ref np) = config.no_proxy {
                proxy = proxy.no_proxy(NoProxy::from_string(np));
            }
            // Disable env proxy fallback so explicit --upstream-proxy takes full control.
            builder = builder.no_proxy().proxy(proxy);
        }
    } else if config.no_system_proxy {
        builder = builder.no_proxy();
    }

    if let Some(ref ca_path) = config.ca_cert {
        let pem = std::fs::read(ca_path)
            .with_context(|| format!("failed to read CA certificate {}", ca_path.display()))?;
        let certs = Certificate::from_pem_bundle(&pem)
            .with_context(|| format!("failed to parse PEM CA certificate {}", ca_path.display()))?;
        for cert in certs {
            builder = builder.add_root_certificate(cert);
        }
    }

    if config.danger_accept_invalid_certs {
        builder = builder.danger_accept_invalid_certs(true);
    }

    Ok(builder)
}

fn redact_proxy_url(raw: &str) -> String {
    match url::Url::parse(raw) {
        Ok(mut u) => {
            if u.password().is_some() {
                let _ = u.set_password(Some("***"));
            }
            u.to_string()
        }
        Err(_) => "<invalid-proxy-url>".to_string(),
    }
}
