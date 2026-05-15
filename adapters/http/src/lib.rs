// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! HTTP adapter: executes operations as HTTP requests.
//!
//! Op template fields map to HTTP request components:
//! - `method` — GET, POST, PUT, DELETE, PATCH, HEAD (default: GET)
//! - `uri` or `url` — the request URL (required)
//! - `body` — request body (for POST/PUT/PATCH)
//! - `content_type` — Content-Type header (default: application/json)
//! - `headers` — additional headers as "Name: Value" lines
//! - `ok_status` — expected status codes (default: 200-299)
//!
//! Example workload:
//! ```yaml
//! bindings: |
//!   user_id := mod(hash(cycle), 1000000)
//! ops:
//!   read:
//!     method: GET
//!     uri: "http://localhost:8080/api/users/{user_id}"
//!   write:
//!     method: POST
//!     uri: "http://localhost:8080/api/users"
//!     body: '{"id": {user_id}, "name": "user_{user_id}"}'
//!     content_type: application/json
//! ```

use nbrs_activity::adapter::{
    AdapterError, DriverAdapter, ExecutionError, JsonBody, OpDispenser, OpResult, ResultBody, TextBody,
};
use nbrs_workload::model::ParsedOp;

/// Configuration for the HTTP adapter.
pub struct HttpConfig {
    /// Base URL prefix prepended to relative URIs.
    pub base_url: Option<String>,
    /// Default timeout per request in milliseconds.
    pub timeout_ms: u64,
    /// Whether to follow redirects.
    pub follow_redirects: bool,
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            base_url: None,
            timeout_ms: 30_000,
            follow_redirects: true,
        }
    }
}

impl HttpConfig {
    /// Construct a config from CLI/workload params.
    pub fn from_params(params: &std::collections::HashMap<String, String>) -> Self {
        Self {
            base_url: params.get("base_url").or(params.get("host")).cloned(),
            timeout_ms: params.get("timeout")
                .and_then(|s| s.parse().ok()).unwrap_or(30_000),
            follow_redirects: true,
        }
    }
}

/// The HTTP adapter: executes ops as HTTP requests.
pub struct HttpAdapter {
    client: reqwest::Client,
    base_url: Option<String>,
}

impl HttpAdapter {
    /// Create with default config.
    pub fn new() -> Self {
        Self::with_config(HttpConfig::default())
    }

    /// Create with explicit config.
    pub fn with_config(config: HttpConfig) -> Self {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_millis(config.timeout_ms))
            .redirect(if config.follow_redirects {
                reqwest::redirect::Policy::limited(10)
            } else {
                reqwest::redirect::Policy::none()
            })
            .build()
            .expect("failed to build HTTP client");

        Self {
            client,
            base_url: config.base_url,
        }
    }
}

/// Classify a reqwest error into an error name for the error router.
fn classify_reqwest_error(e: &reqwest::Error) -> String {
    if e.is_timeout() {
        "Timeout".into()
    } else if e.is_connect() {
        "ConnectionRefused".into()
    } else if e.is_request() {
        "RequestError".into()
    } else {
        "HttpError".into()
    }
}

impl DriverAdapter for HttpAdapter {
    fn name(&self) -> &str { "http" }

    /// HTTP adapter reads a closed vocabulary of op fields:
    /// request-shape (`method`, `uri` / `url`), body framing
    /// (`content_type`, `body`), and header overrides
    /// (`headers`). Declaring the list opts this adapter into
    /// SRD 30's unknown-field guard — typos like `bdoy:` or
    /// misplaced core directives surface at init time rather
    /// than silently becoming ResolvedFields the adapter never
    /// looks at.
    fn known_op_fields(&self) -> Option<&'static [&'static str]> {
        // `request_timeout_ms` (not `timeout_ms`) to avoid
        // colliding with the polling wrapper's `timeout_ms`,
        // which is the loop-level deadline. The HTTP adapter's
        // value is a single-request budget.
        Some(&["method", "content_type", "uri", "url", "body", "headers", "request_timeout_ms"])
    }

    fn map_op(
        &self,
        template: &ParsedOp,
        parent: std::sync::Arc<nbrs_activity::adapter::GkKernel>,
    ) -> Result<Box<dyn OpDispenser>, String> {
        // Extract static method from template (default GET)
        let method = template.op.get("method")
            .and_then(|v: &serde_json::Value| v.as_str())
            .map(|s: &str| s.to_uppercase())
            .unwrap_or_else(|| "GET".into());

        // Extract content type (default application/json)
        let content_type = template.op.get("content_type")
            .and_then(|v: &serde_json::Value| v.as_str())
            .unwrap_or("application/json")
            .to_string();

        // SRD-68 Push 5: snapshot the per-cycle field templates at
        // map_op. Each is rendered through `substitute_via_wires`
        // at execute — the generic GK API resolves bind points by
        // name, no synthesis-layer ResolvedFields involvement.
        // `url` is an alias for `uri`; honour whichever appears.
        let uri_template = template.op.get("uri")
            .or_else(|| template.op.get("url"))
            .and_then(|v| v.as_str())
            .map(String::from);
        let body_template = template.op.get("body")
            .and_then(|v| v.as_str())
            .map(String::from);
        let headers_template = template.op.get("headers")
            .and_then(|v| v.as_str())
            .map(String::from);
        // Per-op timeout override. Cassandra's
        // `forceKeyspaceCompaction` JMX op is synchronous (blocks
        // for the entire compaction); the default 30s client
        // timeout is far too short for any real table size. This
        // field lets workloads opt into a longer per-request
        // budget without raising the adapter-wide default.
        // Named `request_timeout_ms` (not `timeout_ms`) so it
        // doesn't collide with the polling wrapper's loop-level
        // `timeout_ms`.
        let per_op_timeout_ms = template.op.get("request_timeout_ms")
            .and_then(|v| v.as_u64()
                .or_else(|| v.as_str().and_then(|s| s.parse::<u64>().ok())));

        Ok(Box::new(HttpDispenser {
            client: self.client.clone(),
            base_url: self.base_url.clone(),
            method,
            content_type,
            canonical_kernel: parent,
            uri_template,
            body_template,
            headers_template,
            per_op_timeout_ms,
        }))
    }
}

/// Op dispenser for the HTTP adapter. Pre-analyzes method and content type
/// at init time; resolves URI and body from wires per-cycle.
struct HttpDispenser {
    client: reqwest::Client,
    base_url: Option<String>,
    method: String,
    content_type: String,
    /// SRD-68 invariant I-3: dispenser-owned canonical GK kernel.
    canonical_kernel: std::sync::Arc<nbrs_activity::adapter::GkKernel>,
    /// Cycle-time templates rendered through `substitute_via_wires`.
    /// `uri` is mandatory; `body` and `headers` are optional.
    uri_template: Option<String>,
    body_template: Option<String>,
    headers_template: Option<String>,
    /// Optional per-op request timeout override. When set, the
    /// builder applies `.timeout(...)` on the request — bypassing
    /// the adapter's client-wide default. Use for long-running
    /// JMX/REST calls (e.g. Jolokia synchronous
    /// `forceKeyspaceCompaction`) that legitimately take many
    /// minutes.
    per_op_timeout_ms: Option<u64>,
}


impl OpDispenser for HttpDispenser {
    fn canonical_kernel(&self) -> Option<&std::sync::Arc<nbrs_activity::adapter::GkKernel>> {
        Some(&self.canonical_kernel)
    }

    fn execute<'a>(
        &'a self,
        _cycle: u64,
        ctx: &'a nbrs_activity::adapter::ExecCtx<'a>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<OpResult, ExecutionError>> + Send + 'a>> {
        let wires = ctx.wires;
        Box::pin(async move {
            let uri_template = self.uri_template.as_deref()
                .ok_or_else(|| ExecutionError::Op(AdapterError {
                    error_name: "missing_field".into(),
                    message: "HTTP op requires a 'uri' or 'url' field".into(),
                    retryable: false,
                }))?;

            // SRD-68 Push 5: render each per-cycle template via the
            // generic wires API. Bind-point resolution failures are
            // returned as op errors so the error router decides.
            let uri = nbrs_activity::wires::substitute_via_wires(uri_template, wires)
                .map_err(|e| ExecutionError::Op(AdapterError {
                    error_name: "BindError".into(),
                    message: format!("uri: {e}"),
                    retryable: false,
                }))?;

            let full_url = if let Some(ref base) = self.base_url {
                if uri.starts_with("http://") || uri.starts_with("https://") {
                    uri.clone()
                } else {
                    format!("{}{}", base.trim_end_matches('/'), uri)
                }
            } else {
                uri.clone()
            };

            let body = match &self.body_template {
                Some(t) => Some(nbrs_activity::wires::substitute_via_wires(t, wires)
                    .map_err(|e| ExecutionError::Op(AdapterError {
                        error_name: "BindError".into(),
                        message: format!("body: {e}"),
                        retryable: false,
                    }))?),
                None => None,
            };

            // Parse additional headers from the rendered headers
            // field. Per-line `Name: Value` entries.
            let extra_headers: Vec<(String, String)> = match &self.headers_template {
                Some(t) => {
                    let rendered = nbrs_activity::wires::substitute_via_wires(t, wires)
                        .map_err(|e| ExecutionError::Op(AdapterError {
                            error_name: "BindError".into(),
                            message: format!("headers: {e}"),
                            retryable: false,
                        }))?;
                    rendered.lines()
                        .filter_map(|line| {
                            let mut parts = line.splitn(2, ':');
                            let name = parts.next()?.trim().to_string();
                            let value = parts.next()?.trim().to_string();
                            Some((name, value))
                        })
                        .collect()
                }
                None => Vec::new(),
            };

            let mut builder = match self.method.as_str() {
                "GET" => self.client.get(&full_url),
                "POST" => self.client.post(&full_url),
                "PUT" => self.client.put(&full_url),
                "DELETE" => self.client.delete(&full_url),
                "PATCH" => self.client.patch(&full_url),
                "HEAD" => self.client.head(&full_url),
                other => return Err(ExecutionError::Op(AdapterError {
                    error_name: "InvalidMethod".into(),
                    message: format!("unsupported HTTP method: {other}"),
                    retryable: false,
                })),
            };

            builder = builder.header("Content-Type", &self.content_type);

            for (name, value) in &extra_headers {
                builder = builder.header(name.as_str(), value.as_str());
            }

            // Per-op timeout override. When unset, reqwest falls
            // back to the adapter's client-wide default
            // (`timeout=` in workload params, 30s otherwise).
            if let Some(ms) = self.per_op_timeout_ms {
                builder = builder.timeout(std::time::Duration::from_millis(ms));
            }

            if let Some(body_str) = body {
                builder = builder.body(body_str);
            }

            let response = builder.send().await.map_err(|e| {
                let retryable = e.is_timeout() || e.is_connect();
                let scope = if e.is_connect() {
                    ExecutionError::Adapter
                } else {
                    ExecutionError::Op
                };
                scope(AdapterError {
                    error_name: classify_reqwest_error(&e),
                    message: e.to_string(),
                    retryable,
                })
            })?;

            let status = response.status().as_u16() as i32;
            let success = response.status().is_success();
            // Capture content-type before consuming the response
            // body so we can pick the right `ResultBody` shape.
            // `application/json` (or any `…/json` subtype like
            // `application/vnd.api+json`) parses into a `JsonBody`
            // — verify-blocks can then address nested fields
            // (`field: status, eq: "200"`) instead of substring
            // matching on the raw text.
            let content_type_says_json = response.headers()
                .get(reqwest::header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .map(|ct| ct.contains("json"))
                .unwrap_or(false);
            let body_text = response.text().await.map_err(|e| {
                ExecutionError::Op(AdapterError {
                    error_name: "BodyReadError".into(),
                    message: format!("failed to read response body: {e}"),
                    retryable: false,
                })
            })?;

            if success {
                // Promote to `JsonBody` whenever the body parses
                // as JSON — not just when the server bothered to
                // set the right Content-Type. Jolokia 1.x and
                // various JMX bridges return JSON with a
                // `text/plain` (or missing) content type;
                // requiring the header would make verify blocks
                // unable to address nested fields (`field:
                // status, eq: "200"` → `<not-json>` even though
                // the body literally is JSON).
                //
                // Gate the parse attempt on a cheap prefix check
                // (`{` / `[` after whitespace) so we don't
                // serde_json::from_str scan arbitrary text
                // bodies that happen to start with a digit or a
                // quoted string. That keeps "parse a scalar like
                // "42" into a JSON number" — a real risk for
                // plain-text endpoints — from happening.
                let looks_like_json = body_text.trim_start()
                    .starts_with(|c: char| c == '{' || c == '[');
                let parsed_json = if content_type_says_json || looks_like_json {
                    serde_json::from_str::<serde_json::Value>(&body_text).ok()
                } else { None };
                let body: Box<dyn ResultBody> = match parsed_json {
                    Some(v) => Box::new(JsonBody(v)),
                    None => Box::new(TextBody(body_text)),
                };
                Ok(OpResult {
                    body: Some(body),
                    skipped: false,
                })
            } else {
                Err(ExecutionError::Op(AdapterError {
                    error_name: format!("HttpStatus{}", status),
                    message: format!("HTTP {} {}: {}", status, full_url, &body_text),
                    retryable: (500..600).contains(&status),
                }))
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config() {
        let config = HttpConfig::default();
        assert_eq!(config.timeout_ms, 30_000);
        assert!(config.follow_redirects);
        assert!(config.base_url.is_none());
    }

    #[test]
    fn adapter_creates() {
        let _adapter = HttpAdapter::new();
    }
}

// =========================================================================
// Adapter Registration (inventory-based, link-time)
// =========================================================================

inventory::submit! {
    nbrs_activity::adapter::AdapterRegistration {
        names: || &["http"],
        known_params: || &["base_url", "host", "timeout"],
        display_preference: || nbrs_activity::adapter::DisplayPreference::Auto,
        create: |params| Box::pin(async move {
            Ok(std::sync::Arc::new(HttpAdapter::with_config(HttpConfig::from_params(&params)))
                as std::sync::Arc<dyn nbrs_activity::adapter::DriverAdapter>)
        }),
    }
}

// SRD-35 Push C: HTTP adapter declares itself
// pool-shareable. The reqwest `Client` is documented
// thread-safe and pools connections internally; sharing
// one `HttpAdapter` across all phases that target the
// same `(base_url, timeout)` combination eliminates the
// per-phase TLS handshake / connection-establish storm.
//
// `base_url` and `timeout` are instance-shaping (the same
// reqwest client serves every request that uses them);
// per-call URL paths and method overrides come in via the
// op-template layer and don't affect the resource key.
inventory::submit! {
    nbrs_activity::adapter::SharedDriverRegistration {
        adapter: "http",
        driver: nbrs_activity::adapter::DEFAULT_DRIVER_NAME,
        share_capability: nbrs_activity::resource_pool::ShareCapability::Shared,
        resource_key: |params| {
            let cfg = HttpConfig::from_params(params);
            Ok(nbrs_activity::resource_pool::ResourceKey::new("http")
                .with("base_url", cfg.base_url.unwrap_or_default())
                .with("timeout_ms", cfg.timeout_ms.to_string()))
        },
    }
}
