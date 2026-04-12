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

use nb_activity::adapter::{
    AdapterError, DriverAdapter, ExecutionError, OpDispenser, OpResult, ResolvedFields, TextBody,
};
use nb_workload::model::ParsedOp;

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

    fn map_op(&self, template: &ParsedOp) -> Result<Box<dyn OpDispenser>, String> {
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

        Ok(Box::new(HttpDispenser {
            client: self.client.clone(),
            base_url: self.base_url.clone(),
            method,
            content_type,
        }))
    }
}

/// Op dispenser for the HTTP adapter. Pre-analyzes method and content type
/// at init time; resolves URI and body from fields per-cycle.
struct HttpDispenser {
    client: reqwest::Client,
    base_url: Option<String>,
    method: String,
    content_type: String,
}

impl OpDispenser for HttpDispenser {
    fn execute<'a>(
        &'a self,
        _cycle: u64,
        fields: &'a ResolvedFields,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<OpResult, ExecutionError>> + Send + 'a>> {
        Box::pin(async move {
            let uri = fields.get_str("uri")
                .or_else(|| fields.get_str("url"))
                .ok_or_else(|| ExecutionError::Op(AdapterError {
                    error_name: "missing_field".into(),
                    message: "HTTP op requires a 'uri' or 'url' field".into(),
                    retryable: false,
                }))?;

            let full_url = if let Some(ref base) = self.base_url {
                if uri.starts_with("http://") || uri.starts_with("https://") {
                    uri.to_string()
                } else {
                    format!("{}{}", base.trim_end_matches('/'), uri)
                }
            } else {
                uri.to_string()
            };

            let body = fields.get_str("body").map(|s| s.to_string());

            // Parse additional headers from fields
            let extra_headers: Vec<(String, String)> = fields.get_str("headers")
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
            let body_text = response.text().await.map_err(|e| {
                ExecutionError::Op(AdapterError {
                    error_name: "BodyReadError".into(),
                    message: format!("failed to read response body: {e}"),
                    retryable: false,
                })
            })?;

            if success {
                Ok(OpResult {
                    body: Some(Box::new(TextBody(body_text))),
                    captures: std::collections::HashMap::new(),
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
