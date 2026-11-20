use std::collections::HashSet;

use parking_lot::RwLock;
use percent_encoding::{AsciiSet, CONTROLS, percent_decode_str, utf8_percent_encode};
use thiserror::Error;
use url::Url;

/// Percent-encoding set for authority path segments (keeps host:port readable while encoding
/// brackets/slashes/reserved chars like IPv6 `[::1]`).
const AUTHORITY_ENCODE_SET: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'"')
    .add(b'#')
    .add(b'%')
    .add(b'/')
    .add(b'<')
    .add(b'>')
    .add(b'?')
    .add(b'[')
    .add(b'\\')
    .add(b']')
    .add(b'^')
    .add(b'`')
    .add(b'{')
    .add(b'|')
    .add(b'}');

#[derive(Debug, Error)]
pub enum RewriteError {
    #[error("invalid project name")]
    InvalidProjectName,
    #[error("invalid or unsafe path")]
    InvalidPath,
    #[error("upstream packages base URL is not configured")]
    PackagesBaseNotConfigured,
    #[error("upstream host '{0}' is not allowed")]
    ForbiddenHost(String),
    #[error("invalid upstream URL: {0}")]
    InvalidUrl(String),
    #[error("failed to parse Simple API JSON: {0}")]
    InvalidJson(String),
}

/// Negotiated Simple Repository API format (PEP 503 / PEP 691).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SimpleFormat {
    Json,
    Html,
}

impl SimpleFormat {
    pub fn upstream_accept_header(&self) -> &'static str {
        match self {
            SimpleFormat::Json => {
                "application/vnd.pypi.simple.v1+json, application/vnd.pypi.simple.v1+html;q=0.5, text/html;q=0.1"
            }
            SimpleFormat::Html => "application/vnd.pypi.simple.v1+html, text/html;q=0.9",
        }
    }

    pub fn cache_tag(&self) -> &'static str {
        match self {
            SimpleFormat::Json => "json",
            SimpleFormat::Html => "html",
        }
    }
}

/// Normalizes a Python package name per PEP 503:
/// lowercase and replace any run of `-`, `_`, or `.` with a single `-`.
pub fn normalize_project_name(name: &str) -> Result<String, RewriteError> {
    if name.is_empty() || name.len() > 256 {
        return Err(RewriteError::InvalidProjectName);
    }
    let mut out = String::with_capacity(name.len());
    let mut prev_sep = false;
    for ch in name.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
            prev_sep = false;
        } else if ch == '-' || ch == '_' || ch == '.' {
            if !prev_sep {
                out.push('-');
                prev_sep = true;
            }
        } else {
            return Err(RewriteError::InvalidProjectName);
        }
    }
    if out.is_empty() || out == "-" {
        return Err(RewriteError::InvalidProjectName);
    }
    Ok(out)
}

/// Negotiates PEP 691 JSON vs PEP 503 HTML from an HTTP `Accept` header.
/// Per PEP 691, defaults to HTML if no explicit preference favors JSON.
pub fn negotiate_simple_format(accept_header: Option<&str>) -> SimpleFormat {
    let Some(header) = accept_header else {
        return SimpleFormat::Html;
    };

    let mut json_q: Option<f32> = None;
    let mut html_q: Option<f32> = None;

    for raw_item in header.split(',') {
        let mut parts = raw_item.trim().split(';');
        let Some(media_type) = parts.next().map(|s| s.trim().to_ascii_lowercase()) else {
            continue;
        };
        if media_type.is_empty() {
            continue;
        }

        let mut q: f32 = 1.0;
        for param in parts {
            let param = param.trim();
            if let Some(val) = param
                .strip_prefix("q=")
                .or_else(|| param.strip_prefix("Q="))
                && let Ok(parsed) = val.trim().parse::<f32>()
            {
                q = parsed.clamp(0.0, 1.0);
            }
        }

        match media_type.as_str() {
            "application/vnd.pypi.simple.v1+json"
            | "application/vnd.pypi.simple.latest+json"
            | "application/json" => {
                json_q = Some(json_q.map_or(q, |prev| prev.max(q)));
            }
            "application/vnd.pypi.simple.v1+html"
            | "application/vnd.pypi.simple.latest+html"
            | "text/html" => {
                html_q = Some(html_q.map_or(q, |prev| prev.max(q)));
            }
            _ => {}
        }
    }

    match (json_q, html_q) {
        (Some(jq), Some(hq)) if jq > 0.0 && jq >= hq => SimpleFormat::Json,
        (Some(jq), None) if jq > 0.0 => SimpleFormat::Json,
        _ => SimpleFormat::Html,
    }
}

/// Manages deterministic, SSRF-safe mapping between upstream package URLs and proxy paths.
pub struct UrlMapper {
    upstream_packages_url: Option<Url>,
    /// Allowed exact `host:port` authorities (including dynamically discovered upstream link hosts).
    allowed_authorities: RwLock<HashSet<(String, u16)>>,
    /// Allowed hostnames (any port), from explicit user config without a port suffix.
    allowed_any_port_hosts: HashSet<String>,
    /// True if user explicitly passed `*` in allowed hosts.
    allow_all_hosts: bool,
}

impl UrlMapper {
    pub fn new(
        upstream_index: &Url,
        upstream_packages_url: Option<Url>,
        extra_allowed_hosts: &[String],
    ) -> Self {
        let mut allowed_authorities = HashSet::new();
        let mut allowed_any_port_hosts = HashSet::new();
        let mut allow_all_hosts = false;

        if let Some(auth) = extract_authority_key(upstream_index) {
            allowed_authorities.insert(auth);
        }
        if let Some(ref pkg_url) = upstream_packages_url
            && let Some(auth) = extract_authority_key(pkg_url)
        {
            allowed_authorities.insert(auth);
        }

        for entry in extra_allowed_hosts {
            let entry = entry.trim().to_ascii_lowercase();
            if entry == "*" {
                allow_all_hosts = true;
            } else if let Some((host, port_str)) = entry.rsplit_once(':')
                && !host.contains(':')
                && let Ok(port) = port_str.parse::<u16>()
            {
                allowed_authorities.insert((host.to_string(), port));
            } else if !entry.is_empty() {
                allowed_any_port_hosts.insert(entry);
            }
        }

        Self {
            upstream_packages_url,
            allowed_authorities: RwLock::new(allowed_authorities),
            allowed_any_port_hosts,
            allow_all_hosts,
        }
    }

    fn remember_upstream_url(&self, url: &Url) {
        if let Some(auth) = extract_authority_key(url) {
            let guard = self.allowed_authorities.read();
            if guard.contains(&auth) {
                return;
            }
            drop(guard);
            self.allowed_authorities.write().insert(auth);
        }
    }

    pub fn is_allowed_upstream(&self, url: &Url) -> bool {
        if !matches!(url.scheme(), "http" | "https") {
            return false;
        }
        if self.allow_all_hosts {
            return true;
        }
        let Some((host, port)) = extract_authority_key(url) else {
            return false;
        };
        if self.allowed_any_port_hosts.contains(&host) {
            return true;
        }
        self.allowed_authorities.read().contains(&(host, port))
    }

    /// Rewrites an upstream package link (absolute or relative to `base_url`) into a
    /// root-relative proxy URL (`/packages/...` or `/proxy/{scheme}/{authority}/...`).
    pub fn rewrite_file_url(&self, raw_url: &str, base_url: &Url) -> Option<String> {
        let resolved = base_url.join(raw_url.trim()).ok()?;
        if !matches!(resolved.scheme(), "http" | "https") {
            return None;
        }
        self.remember_upstream_url(&resolved);

        let mut rewritten = if let Some(ref pkg_base) = self.upstream_packages_url {
            if same_origin(pkg_base, &resolved) && resolved.path().starts_with(pkg_base.path()) {
                let suffix = &resolved.path()[pkg_base.path().len()..];
                format!("/packages/{}", suffix.trim_start_matches('/'))
            } else {
                build_proxy_path(&resolved)?
            }
        } else {
            build_proxy_path(&resolved)?
        };

        if let Some(query) = resolved.query() {
            rewritten.push('?');
            rewritten.push_str(query);
        }
        if let Some(fragment) = resolved.fragment() {
            rewritten.push('#');
            rewritten.push_str(fragment);
        }
        Some(rewritten)
    }

    /// Resolves a client request to `/packages/{path}` back to the upstream package URL.
    pub fn resolve_packages_path(
        &self,
        path: &str,
        query: Option<&str>,
    ) -> Result<Url, RewriteError> {
        validate_relative_path(path)?;
        let Some(ref pkg_base) = self.upstream_packages_url else {
            return Err(RewriteError::PackagesBaseNotConfigured);
        };
        let mut url = pkg_base.clone();
        let clean_path = path.trim_start_matches('/');
        let base_path = pkg_base.path().trim_end_matches('/');
        url.set_path(&format!("{base_path}/{clean_path}"));
        url.set_query(query);
        Ok(url)
    }

    /// Resolves a client request to `/proxy/{scheme}/{encoded_authority}/{path}` back to the
    /// upstream URL after validating SSRF allowlist rules.
    pub fn resolve_proxy_path(
        &self,
        scheme: &str,
        encoded_authority: &str,
        path: &str,
        query: Option<&str>,
    ) -> Result<Url, RewriteError> {
        if !matches!(scheme, "http" | "https") {
            return Err(RewriteError::InvalidUrl(format!(
                "unsupported scheme '{scheme}'"
            )));
        }
        validate_relative_path(path)?;

        let authority = percent_decode_str(encoded_authority)
            .decode_utf8()
            .map_err(|_| RewriteError::InvalidUrl("invalid UTF-8 authority".into()))?;
        if authority.is_empty()
            || authority.contains('/')
            || authority.contains('@')
            || authority.contains('?')
            || authority.contains('#')
        {
            return Err(RewriteError::InvalidUrl(format!(
                "invalid authority '{authority}'"
            )));
        }

        let clean_path = path.trim_start_matches('/');
        let raw = format!("{scheme}://{authority}/{clean_path}");
        let mut url =
            Url::parse(&raw).map_err(|e| RewriteError::InvalidUrl(e.to_string()))?;
        url.set_query(query);

        if !self.is_allowed_upstream(&url) {
            return Err(RewriteError::ForbiddenHost(authority.into_owned()));
        }

        Ok(url)
    }
}

fn extract_authority_key(url: &Url) -> Option<(String, u16)> {
    let host = url.host_str()?.to_ascii_lowercase();
    let port = url.port_or_known_default()?;
    Some((host, port))
}

fn same_origin(a: &Url, b: &Url) -> bool {
    a.scheme() == b.scheme()
        && a.host_str().map(|h| h.to_ascii_lowercase())
            == b.host_str().map(|h| h.to_ascii_lowercase())
        && a.port_or_known_default() == b.port_or_known_default()
}

fn build_proxy_path(url: &Url) -> Option<String> {
    let host = url.host_str()?;
    let authority = match url.port() {
        Some(port) => format!("{host}:{port}"),
        None => host.to_string(),
    };
    let encoded_authority = utf8_percent_encode(&authority, AUTHORITY_ENCODE_SET);
    let path = url.path().trim_start_matches('/');
    Some(format!("/proxy/{}/{}/{}", url.scheme(), encoded_authority, path))
}

fn validate_relative_path(path: &str) -> Result<(), RewriteError> {
    if path.is_empty() || path.contains('\0') || path.contains('\\') {
        return Err(RewriteError::InvalidPath);
    }
    for segment in path.split('/') {
        if segment == ".." {
            return Err(RewriteError::InvalidPath);
        }
        if let Ok(decoded) = percent_decode_str(segment).decode_utf8()
            && (decoded == ".." || decoded.contains('/') || decoded.contains('\\'))
        {
            return Err(RewriteError::InvalidPath);
        }
    }
    Ok(())
}

/// Rewrites all package file URLs inside a PEP 691 JSON Simple API response.
pub fn rewrite_simple_json(
    body: &[u8],
    base_url: &Url,
    mapper: &UrlMapper,
) -> Result<Vec<u8>, RewriteError> {
    let mut value: serde_json::Value =
        serde_json::from_slice(body).map_err(|e| RewriteError::InvalidJson(e.to_string()))?;

    if let Some(files) = value.get_mut("files").and_then(|v| v.as_array_mut()) {
        for file in files {
            if let Some(obj) = file.as_object_mut() {
                if let Some(raw_url) = obj.get("url").and_then(|u| u.as_str())
                    && let Some(rewritten) = mapper.rewrite_file_url(raw_url, base_url)
                {
                    obj.insert("url".to_string(), serde_json::Value::String(rewritten));
                }
                if let Some(raw_prov) = obj.get("provenance").and_then(|u| u.as_str())
                    && let Some(rewritten) = mapper.rewrite_file_url(raw_prov, base_url)
                {
                    obj.insert(
                        "provenance".to_string(),
                        serde_json::Value::String(rewritten),
                    );
                }
            }
        }
    }

    serde_json::to_vec(&value).map_err(|e| RewriteError::InvalidJson(e.to_string()))
}

/// Rewrites `<a ... href="...">` links inside a PEP 503 HTML Simple API response while
/// preserving every other byte, tag, and attribute verbatim.
pub fn rewrite_simple_html(html: &str, base_url: &Url, mapper: &UrlMapper) -> String {
    let bytes = html.as_bytes();
    let len = bytes.len();
    let mut out = String::with_capacity(len);
    let mut i = 0;

    while i < len {
        // Find next '<'
        let Some(rel_lt) = bytes[i..].iter().position(|&b| b == b'<') else {
            out.push_str(&html[i..]);
            break;
        };
        let lt = i + rel_lt;
        out.push_str(&html[i..lt]);

        // Skip HTML comments verbatim
        if html[lt..].starts_with("<!--") {
            if let Some(end_rel) = html[lt + 4..].find("-->") {
                let end = lt + 4 + end_rel + 3;
                out.push_str(&html[lt..end]);
                i = end;
            } else {
                out.push_str(&html[lt..]);
                break;
            }
            continue;
        }

        // Check if this is an `<a ` or `<A ` start tag
        if lt + 2 < len
            && bytes[lt + 1].eq_ignore_ascii_case(&b'a')
            && bytes[lt + 2].is_ascii_whitespace()
        {
            let tag_end = find_tag_end(bytes, lt + 2);
            let rewritten_tag = rewrite_anchor_tag(&html[lt..tag_end], base_url, mapper);
            out.push_str(&rewritten_tag);
            i = tag_end;
        } else {
            out.push('<');
            i = lt + 1;
        }
    }

    out
}

/// Finds the byte index just past the closing `>` of an HTML tag, respecting quoted attributes.
fn find_tag_end(bytes: &[u8], mut pos: usize) -> usize {
    let len = bytes.len();
    let mut quote: Option<u8> = None;
    while pos < len {
        let b = bytes[pos];
        match quote {
            Some(q) => {
                if b == q {
                    quote = None;
                }
            }
            None => match b {
                b'"' | b'\'' => quote = Some(b),
                b'>' => return pos + 1,
                _ => {}
            },
        }
        pos += 1;
    }
    len
}

fn rewrite_anchor_tag(tag: &str, base_url: &Url, mapper: &UrlMapper) -> String {
    let bytes = tag.as_bytes();
    let len = bytes.len();
    // Skip `<a`
    let mut pos = 2;

    while pos < len {
        // Skip whitespace
        while pos < len && bytes[pos].is_ascii_whitespace() {
            pos += 1;
        }
        if pos >= len || bytes[pos] == b'>' || bytes[pos] == b'/' {
            break;
        }

        // Read attribute name
        let name_start = pos;
        while pos < len
            && !bytes[pos].is_ascii_whitespace()
            && bytes[pos] != b'='
            && bytes[pos] != b'>'
            && bytes[pos] != b'/'
        {
            pos += 1;
        }
        let name_end = pos;
        let attr_name = &tag[name_start..name_end];

        // Skip whitespace before '='
        let mut eq_pos = pos;
        while eq_pos < len && bytes[eq_pos].is_ascii_whitespace() {
            eq_pos += 1;
        }
        if eq_pos >= len || bytes[eq_pos] != b'=' {
            // Boolean attribute (no value)
            pos = eq_pos;
            continue;
        }

        // Advance past '=' and whitespace
        pos = eq_pos + 1;
        while pos < len && bytes[pos].is_ascii_whitespace() {
            pos += 1;
        }
        if pos >= len || bytes[pos] == b'>' {
            break;
        }

        // Parse attribute value (double-quoted, single-quoted, or unquoted)
        let (val_start, val_end, next_pos, quote_char) = if bytes[pos] == b'"' || bytes[pos] == b'\''
        {
            let q = bytes[pos];
            let vs = pos + 1;
            let mut ve = vs;
            while ve < len && bytes[ve] != q {
                ve += 1;
            }
            let np = if ve < len { ve + 1 } else { len };
            (vs, ve, np, Some(q as char))
        } else {
            let vs = pos;
            let mut ve = vs;
            while ve < len && !bytes[ve].is_ascii_whitespace() && bytes[ve] != b'>' {
                ve += 1;
            }
            (vs, ve, ve, None)
        };

        if attr_name.eq_ignore_ascii_case("href") {
            let raw_escaped = &tag[val_start..val_end];
            let unescaped = unescape_html_attr(raw_escaped);
            if let Some(rewritten) = mapper.rewrite_file_url(&unescaped, base_url) {
                let escaped = escape_html_attr(&rewritten, quote_char);
                let mut rebuilt = String::with_capacity(tag.len() + escaped.len());
                rebuilt.push_str(&tag[..val_start]);
                rebuilt.push_str(&escaped);
                rebuilt.push_str(&tag[val_end..]);
                return rebuilt;
            }
            return tag.to_string();
        }

        pos = next_pos;
    }

    tag.to_string()
}

fn unescape_html_attr(s: &str) -> String {
    if !s.contains('&') {
        return s.to_string();
    }
    s.replace("&amp;", "&")
        .replace("&quot;", "\"")
        .replace("&#34;", "\"")
        .replace("&#39;", "'")
        .replace("&apos;", "'")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
}

fn escape_html_attr(s: &str, quote: Option<char>) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '"' if quote != Some('\'') => out.push_str("&quot;"),
            '\'' if quote == Some('\'') => out.push_str("&#39;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            _ => out.push(ch),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_project_name() {
        assert_eq!(normalize_project_name("Flask").unwrap(), "flask");
        assert_eq!(
            normalize_project_name("Flask_SQLAlchemy.2").unwrap(),
            "flask-sqlalchemy-2"
        );
        assert_eq!(normalize_project_name("zope..interface").unwrap(), "zope-interface");
        assert!(normalize_project_name("").is_err());
        assert!(normalize_project_name("../etc").is_err());
        assert!(normalize_project_name("foo/bar").is_err());
    }

    #[test]
    fn test_negotiate_simple_format() {
        // uv Accept header
        assert_eq!(
            negotiate_simple_format(Some(
                "application/vnd.pypi.simple.v1+json, application/vnd.pypi.simple.v1+html;q=0.2, text/html;q=0.01"
            )),
            SimpleFormat::Json
        );
        // Legacy pip / browser
        assert_eq!(negotiate_simple_format(Some("text/html")), SimpleFormat::Html);
        assert_eq!(negotiate_simple_format(Some("*/*")), SimpleFormat::Html);
        assert_eq!(negotiate_simple_format(None), SimpleFormat::Html);
        // Explicit HTML higher priority
        assert_eq!(
            negotiate_simple_format(Some(
                "application/vnd.pypi.simple.v1+html;q=1.0, application/vnd.pypi.simple.v1+json;q=0.5"
            )),
            SimpleFormat::Html
        );
    }

    #[test]
    fn test_url_mapper_pypi_and_ssrf() {
        let upstream = Url::parse("https://pypi.org/simple/").unwrap();
        let packages = Url::parse("https://files.pythonhosted.org/packages/").unwrap();
        let mapper = UrlMapper::new(&upstream, Some(packages), &[]);
        let base = Url::parse("https://pypi.org/simple/packaging/").unwrap();

        let rewritten = mapper
            .rewrite_file_url(
                "https://files.pythonhosted.org/packages/bc/4f/pkg-1.0.whl#sha256=deadbeef",
                &base,
            )
            .unwrap();
        assert_eq!(rewritten, "/packages/bc/4f/pkg-1.0.whl#sha256=deadbeef");

        // Resolve back, including PEP 658 .metadata suffix
        assert_eq!(
            mapper
                .resolve_packages_path("bc/4f/pkg-1.0.whl.metadata", None)
                .unwrap()
                .as_str(),
            "https://files.pythonhosted.org/packages/bc/4f/pkg-1.0.whl.metadata"
        );

        // Path traversal rejected
        assert!(mapper.resolve_packages_path("../secret", None).is_err());
        assert!(mapper.resolve_packages_path("a/%2e%2e/secret", None).is_err());

        // Arbitrary host rejected (SSRF protection)
        assert!(matches!(
            mapper.resolve_proxy_path("http", "169.254.169.254", "latest/meta-data/", None),
            Err(RewriteError::ForbiddenHost(_))
        ));
    }

    #[test]
    fn test_rewrite_simple_json() {
        let upstream = Url::parse("https://pypi.org/simple/").unwrap();
        let packages = Url::parse("https://files.pythonhosted.org/packages/").unwrap();
        let mapper = UrlMapper::new(&upstream, Some(packages), &[]);
        let base = Url::parse("https://pypi.org/simple/demo/").unwrap();

        let input = serde_json::json!({
            "meta": { "api-version": "1.1" },
            "name": "demo",
            "files": [
                {
                    "filename": "demo-1.0-py3-none-any.whl",
                    "url": "https://files.pythonhosted.org/packages/aa/bb/demo-1.0-py3-none-any.whl",
                    "hashes": { "sha256": "1234" },
                    "core-metadata": { "sha256": "5678" }
                },
                {
                    "filename": "demo-1.0.tar.gz",
                    "url": "../../files/demo-1.0.tar.gz"
                }
            ]
        });

        let rewritten = rewrite_simple_json(&serde_json::to_vec(&input).unwrap(), &base, &mapper)
            .unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&rewritten).unwrap();

        assert_eq!(
            parsed["files"][0]["url"].as_str().unwrap(),
            "/packages/aa/bb/demo-1.0-py3-none-any.whl"
        );
        assert_eq!(
            parsed["files"][1]["url"].as_str().unwrap(),
            "/proxy/https/pypi.org/files/demo-1.0.tar.gz"
        );
    }

    #[test]
    fn test_rewrite_simple_html() {
        let upstream = Url::parse("https://pypi.org/simple/").unwrap();
        let packages = Url::parse("https://files.pythonhosted.org/packages/").unwrap();
        let mapper = UrlMapper::new(&upstream, Some(packages), &[]);
        let base = Url::parse("https://pypi.org/simple/demo/").unwrap();

        let html = r#"<!DOCTYPE html>
<html><body>
<!-- <a href="https://files.pythonhosted.org/packages/ignored.whl">ignored</a> -->
<a href="https://files.pythonhosted.org/packages/aa/bb/demo-1.0.whl#sha256=abc" data-requires-python="&gt;=3.8" data-core-metadata="sha256=def">demo-1.0.whl</a><br />
<A data-yanked="bad > 1" HREF='../../tarballs/demo-1.0.tar.gz?x=1&amp;y=2#sha256=999'>demo-1.0.tar.gz</A>
</body></html>"#;

        let out = rewrite_simple_html(html, &base, &mapper);
        assert!(out.contains(r#"<!-- <a href="https://files.pythonhosted.org/packages/ignored.whl">ignored</a> -->"#));
        assert!(out.contains(r#"<a href="/packages/aa/bb/demo-1.0.whl#sha256=abc" data-requires-python="&gt;=3.8" data-core-metadata="sha256=def">demo-1.0.whl</a>"#));
        assert!(out.contains(r#"<A data-yanked="bad > 1" HREF='/proxy/https/pypi.org/tarballs/demo-1.0.tar.gz?x=1&amp;y=2#sha256=999'>demo-1.0.tar.gz</A>"#));
    }
}
