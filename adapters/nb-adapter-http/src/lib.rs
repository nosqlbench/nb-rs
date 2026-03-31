// Copyright 2024-2026 nosqlbench contributors
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


use nb_activity::adapter::{Adapter, AdapterError, AssembledOp, OpResult};

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

/// The HTTP adapter: executes AssembledOps as HTTP requests.
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

impl Adapter for HttpAdapter {
    fn execute(&self, op: &AssembledOp) -> impl std::future::Future<Output = Result<OpResult, AdapterError>> + Send {
        let method = op.fields.get("method")
            .map(|s| s.to_uppercase())
            .unwrap_or_else(|| "GET".into());

        let uri = op.fields.get("uri")
            .or_else(|| op.fields.get("url"))
            .cloned()
            .unwrap_or_default();

        let full_url = if let Some(ref base) = self.base_url {
            if uri.starts_with("http://") || uri.starts_with("https://") {
                uri
            } else {
                format!("{}{}", base.trim_end_matches('/'), uri)
            }
        } else {
            uri
        };

        let body = op.fields.get("body").cloned();
        let content_type = op.fields.get("content_type")
            .cloned()
            .unwrap_or_else(|| "application/json".into());

        // Parse additional headers
        let extra_headers: Vec<(String, String)> = op.fields.get("headers")
            .map(|h| {
                h.lines()
                    .filter_map(|line| {
                        let mut parts = line.splitn(2, ':');
                        let name = parts.next()?.trim().to_string();
                        let value = parts.next()?.trim().to_string();
                        Some((name, value))
                    })
                    .collect()
            })
            .unwrap_or_default();

        let client = self.client.clone();

        async move {
            let mut builder = match method.as_str() {
                "GET" => client.get(&full_url),
                "POST" => client.post(&full_url),
                "PUT" => client.put(&full_url),
                "DELETE" => client.delete(&full_url),
                "PATCH" => client.patch(&full_url),
                "HEAD" => client.head(&full_url),
                other => return Err(AdapterError {
                    error_name: "InvalidMethod".into(),
                    message: format!("unsupported HTTP method: {other}"),
                }),
            };

            builder = builder.header("Content-Type", &content_type);

            for (name, value) in &extra_headers {
                builder = builder.header(name.as_str(), value.as_str());
            }

            if let Some(body_str) = body {
                builder = builder.body(body_str);
            }

            let response = builder.send().await.map_err(|e| AdapterError {
                error_name: classify_reqwest_error(&e),
                message: e.to_string(),
            })?;

            let status = response.status().as_u16() as i32;
            let success = response.status().is_success();
            let body = response.text().await.ok();

            if success {
                Ok(OpResult { success: true, status, body })
            } else {
                Err(AdapterError {
                    error_name: format!("HttpStatus{}", status),
                    message: format!("HTTP {} {}: {}", status, full_url,
                        body.as_deref().unwrap_or("(no body)")),
                })
            }
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
