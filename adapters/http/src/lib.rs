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
        //
        // `on_timeout` is the SRD-74-style modifier that turns
        // an HTTP-client-side timeout into a non-error empty
        // result. Pairs with a short `request_timeout_ms` to
        // express "fire and yield" — the server keeps doing
        // its work whether or not the client is still
        // listening (the canonical use case is Cassandra's
        // synchronous `forceKeyspaceCompaction`, where the
        // poll layer is the actual waiter / observer).
        Some(&["method", "content_type", "uri", "url", "body", "headers",
               "request_timeout_ms", "on_timeout"])
    }

    fn map_op<'a>(
        &'a self,
        template: &'a ParsedOp,
        parent: std::sync::Arc<nbrs_activity::adapter::GkKernel>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Box<dyn OpDispenser>, String>> + Send + 'a>> {
        Box::pin(async move {
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

        // `on_timeout: accept` is the fire-and-yield modifier
        // (SRD-74-style). When a request_timeout_ms is set and
        // the HTTP client trips it, the adapter would normally
        // return a `Timeout` ExecutionError that fails the
        // op. With `accept`, that specific outcome converts to
        // a successful `OpResult` with no body — the server
        // is presumed to still be doing the work; the polling
        // layer is what observes its completion.
        //
        // Errors that are NOT `is_timeout()` (connection
        // refused, body read errors, non-2xx responses) still
        // surface normally. The modifier only translates
        // client-side request-timeout firings.
        let on_timeout_accept = template.op.get("on_timeout")
            .and_then(|v| v.as_str())
            .map(|s| s.eq_ignore_ascii_case("accept"))
            .unwrap_or(false);

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
            on_timeout_accept,
        }) as Box<dyn OpDispenser>)
        })
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
    /// When `true`, a request-timeout firing translates to a
    /// successful empty-body `OpResult` instead of a
    /// `Timeout` op error. Pair with a short
    /// `per_op_timeout_ms` to express "fire and yield":
    /// submit the request, give the server a brief window to
    /// respond, but don't fail the phase if it doesn't —
    /// the server keeps working server-side regardless of
    /// whether the client is still listening.
    on_timeout_accept: bool,
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

            let request_start = std::time::Instant::now();
            let response = match builder.send().await {
                Ok(r) => r,
                Err(e) => {
                    // `on_timeout: accept` converts ONLY the
                    // client-side request-timeout firing into a
                    // benign empty-body success. Every other error
                    // path (connection refused, request build
                    // failure, body read failure) still surfaces
                    // — the modifier is narrowly scoped to the
                    // "fire and yield" pattern where the
                    // expectation is that the request reached the
                    // server but the server is doing long work
                    // synchronously, and the polling layer is the
                    // canonical waiter / observer.
                    if e.is_timeout() && self.on_timeout_accept {
                        // Diagnostic: this branch is the ONLY way
                        // the HTTP adapter returns `body: None`.
                        // Without this log, a downstream
                        // `validation_failed` with
                        // `<no body returned by op>` is opaque
                        // because the verify clause can't tell
                        // why the body is missing. Surfacing the
                        // accept here makes the chain obvious in
                        // session.log without changing the
                        // success-shape semantics.
                        let elapsed_ms = request_start.elapsed().as_millis();
                        let configured_ms = self.per_op_timeout_ms
                            .map(|n| n.to_string())
                            .unwrap_or_else(|| "client-default".to_string());
                        nbrs_activity::observer::log(
                            nbrs_activity::observer::LogLevel::Warn,
                            &format!(
                                "http: `on_timeout: accept` swallowed a \
                                 request timeout after {elapsed_ms}ms \
                                 (configured per_op_timeout_ms={configured_ms}) \
                                 → returning Ok(body=None). \
                                 URL={full_url}. \
                                 If a downstream verify clause expected a \
                                 body, the field assertion will fail with \
                                 `<no body returned by op>`."
                            ),
                        );
                        return Ok(OpResult { body: None, skipped: false });
                    }
                    let retryable = e.is_timeout() || e.is_connect();
                    let scope = if e.is_connect() {
                        ExecutionError::Adapter
                    } else {
                        ExecutionError::Op
                    };
                    return Err(scope(AdapterError {
                        error_name: classify_reqwest_error(&e),
                        message: e.to_string(),
                        retryable,
                    }));
                }
            };

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
    use nbrs_workload::model::ParsedOp;

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

    /// Spin up a TCP listener that accepts a single connection
    /// and then sleeps forever — the canonical "server is busy
    /// doing the long thing, won't answer" shape that
    /// `on_timeout: accept` exists to handle. Returns the
    /// listener's bound port; the caller addresses
    /// `http://127.0.0.1:<port>/`.
    async fn spawn_stalling_listener() -> u16 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await
            .expect("bind 127.0.0.1:0");
        let port = listener.local_addr().expect("local_addr").port();
        tokio::spawn(async move {
            // Accept connections in a loop so a test that
            // retries doesn't deadlock on the second attempt.
            // Each accepted socket is just held — never read,
            // never written — until the test process tears it
            // down.
            loop {
                match listener.accept().await {
                    Ok((sock, _)) => {
                        // Stash the socket on the task heap so
                        // the OS doesn't drop the connection
                        // and let the client see EOF instead
                        // of a timeout.
                        tokio::spawn(async move {
                            let _hold = sock;
                            std::future::pending::<()>().await;
                        });
                    }
                    Err(_) => break,
                }
            }
        });
        port
    }

    fn test_kernel() -> std::sync::Arc<polydat::kernel::GkKernel> {
        std::sync::Arc::new(
            polydat::dsl::compile::compile_gk("input cycle: u64\n").unwrap()
        )
    }

    /// Build an HTTP op-template programmatically. Bypasses
    /// the full workload parser to keep the test focused on
    /// the adapter's per-op behaviour.
    fn http_op(method: &str, uri: &str,
               request_timeout_ms: Option<&str>,
               on_timeout: Option<&str>) -> ParsedOp {
        let mut template = ParsedOp::simple("test", "");
        template.op.remove("stmt");
        template.op.insert("method".into(),
            serde_json::Value::String(method.into()));
        template.op.insert("uri".into(),
            serde_json::Value::String(uri.into()));
        if let Some(ms) = request_timeout_ms {
            template.op.insert("request_timeout_ms".into(),
                serde_json::Value::String(ms.into()));
        }
        if let Some(v) = on_timeout {
            template.op.insert("on_timeout".into(),
                serde_json::Value::String(v.into()));
        }
        template
    }

    /// With `on_timeout: accept`, a request-timeout firing
    /// converts to a successful empty-body `OpResult`. The
    /// canonical use case is Cassandra's synchronous
    /// `forceKeyspaceCompaction` (server keeps working
    /// regardless of whether the client is still listening);
    /// here we stand up a TcpListener that accepts the
    /// connection but never answers, which produces the same
    /// client-side reqwest error.
    #[tokio::test]
    async fn on_timeout_accept_swallows_request_timeout() {
        let port = spawn_stalling_listener().await;
        let adapter = HttpAdapter::new();
        let template = http_op(
            "GET",
            &format!("http://127.0.0.1:{port}/"),
            Some("100"),       // 100ms request timeout
            Some("accept"),    // swallow client-side timeout
        );
        let dispenser = adapter.map_op(&template, test_kernel())
            .expect("map_op");

        let mut k = polydat::dsl::compile::compile_gk("input cycle: u64\n").unwrap();
        let cw = nbrs_activity::wires::CycleWires::new(&mut k);
        let pulls = nbrs_activity::fixture::ResolvedPulls::empty();
        let empty = nbrs_activity::adapter::ResolvedFields::new(Vec::new(), Vec::new());
        let ctx = nbrs_activity::adapter::ExecCtx::with_wires(&empty, &pulls, &cw);

        let result = dispenser.execute(0, &ctx).await
            .expect("on_timeout: accept should map Timeout → Ok(empty)");
        assert!(result.body.is_none(),
            "expected empty-body OpResult after accepted timeout");
        assert!(!result.skipped,
            "accepted-timeout is a real (not skipped) op result");
    }

    /// Without `on_timeout: accept`, the same stalling-listener
    /// scenario produces a `Timeout` op error. Pins the
    /// negative case so the accept-branch can't accidentally
    /// regress to swallowing every error category.
    #[tokio::test]
    async fn timeout_without_accept_still_errors() {
        let port = spawn_stalling_listener().await;
        let adapter = HttpAdapter::new();
        let template = http_op(
            "GET",
            &format!("http://127.0.0.1:{port}/"),
            Some("100"),
            None,
        );
        let dispenser = adapter.map_op(&template, test_kernel())
            .expect("map_op");

        let mut k = polydat::dsl::compile::compile_gk("input cycle: u64\n").unwrap();
        let cw = nbrs_activity::wires::CycleWires::new(&mut k);
        let pulls = nbrs_activity::fixture::ResolvedPulls::empty();
        let empty = nbrs_activity::adapter::ResolvedFields::new(Vec::new(), Vec::new());
        let ctx = nbrs_activity::adapter::ExecCtx::with_wires(&empty, &pulls, &cw);

        let err = dispenser.execute(0, &ctx).await
            .expect_err("default behaviour: client-side timeout → op error");
        match err {
            ExecutionError::Op(ad) => assert_eq!(ad.error_name, "Timeout",
                "expected error_name='Timeout', got: {ad:?}"),
            other => panic!("expected ExecutionError::Op(Timeout), got {other:?}"),
        }
    }

    /// Happy-path regression test: when the server returns a
    /// well-formed JSON body, the adapter's `OpResult.body` is
    /// `Some(JsonBody(...))` — not `None`. This pins the
    /// invariant that the HTTP adapter never returns
    /// `body: None` for a successful request (the only None
    /// path is the timeout-accept branch tested elsewhere). A
    /// regression here would surface as
    /// `<no body returned by op>` in validation diagnostics
    /// even though the server replied normally.
    #[tokio::test]
    async fn successful_json_response_populates_body() {
        // Spin up a one-shot HTTP server that replies with a
        // Jolokia-shaped JSON body.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await
            .expect("bind 127.0.0.1:0");
        let port = listener.local_addr().expect("local_addr").port();
        tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            loop {
                let Ok((mut sock, _)) = listener.accept().await else { break };
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 4096];
                    let _ = sock.read(&mut buf).await;
                    let body = r#"{"status":200,"value":null,"request":{"type":"exec"}}"#;
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body,
                    );
                    let _ = sock.write_all(response.as_bytes()).await;
                });
            }
        });

        let adapter = HttpAdapter::new();
        let template = http_op(
            "POST",
            &format!("http://127.0.0.1:{port}/jolokia/"),
            None,    // no per-op timeout
            None,    // no on_timeout
        );
        let dispenser = adapter.map_op(&template, test_kernel())
            .expect("map_op");

        let mut k = polydat::dsl::compile::compile_gk("input cycle: u64\n").unwrap();
        let cw = nbrs_activity::wires::CycleWires::new(&mut k);
        let pulls = nbrs_activity::fixture::ResolvedPulls::empty();
        let empty = nbrs_activity::adapter::ResolvedFields::new(Vec::new(), Vec::new());
        let ctx = nbrs_activity::adapter::ExecCtx::with_wires(&empty, &pulls, &cw);

        let result = dispenser.execute(0, &ctx).await
            .expect("successful HTTP request should return Ok");
        let body = result.body.as_ref()
            .expect("successful response with body must populate result.body — \
                     no body indicates an adapter regression (the only legit \
                     body=None path is timeout-accept, which this test doesn't \
                     exercise)");
        let json = body.to_json();
        assert_eq!(json.get("status").and_then(|v| v.as_u64()), Some(200),
            "body should preserve the server's `status` field; got: {json}");
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
