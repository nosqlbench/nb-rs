// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Elasticsearch adapter: executes operations against an Elasticsearch cluster.
//!
//! Op template fields:
//! - `action`  — one of `index`, `search`, `get`, `delete`, `bulk` (default: `index`)
//! - `index`   — index name; supports `{bind_point}` substitution
//! - `id`      — document ID; supports `{bind_point}` substitution (required for `get`/`delete`)
//! - `body`    — JSON body string; supports `{bind_point}` substitution
//!               For `bulk`, provide NDJSON: one JSON object per line, action+source pairs.
//!               For `search`, omitting `body` defaults to `{"query":{"match_all":{}}}`.
//!
//! Adapter-level params (set on the scenario/activity, not per-op):
//! - `hosts`    — comma-separated Elasticsearch URLs (default: `http://localhost:9200`)
//! - `timeout`  — request timeout in milliseconds (default: 30000)
//! - `username` — HTTP Basic auth username
//! - `password` — HTTP Basic auth password
//!
//! Example workload:
//! ```yaml
//! bindings: |
//!   doc_id := mod(hash(cycle), 1000000)
//!   payload := json(doc(cycle))
//! ops:
//!   write:
//!     action: index
//!     index: my-index
//!     id: "{doc_id}"
//!     body: "{payload}"
//!   read:
//!     action: get
//!     index: my-index
//!     id: "{doc_id}"
//!   find:
//!     action: search
//!     index: my-index
//!     body: '{"query":{"term":{"id":{doc_id}}}}'
//! ```

use std::sync::Arc;

use elasticsearch::{
    BulkParts, DeleteParts, Elasticsearch, GetParts, IndexParts, SearchParts,
};
use elasticsearch::auth::Credentials;
use elasticsearch::http::transport::{SingleNodeConnectionPool, TransportBuilder};
use nbrs_activity::adapter::{
    AdapterError, DriverAdapter, ExecutionError, JsonBody, OpDispenser, OpResult, PolydatKernel,
};
use nbrs_workload::model::ParsedOp;
use serde_json::Value;

/// Adapter-level configuration, parsed once from workload/activity params.
pub struct ElasticsearchConfig {
    pub hosts: Vec<String>,
    pub timeout_ms: u64,
    pub username: Option<String>,
    pub password: Option<String>,
}

impl Default for ElasticsearchConfig {
    fn default() -> Self {
        Self {
            hosts: vec!["http://localhost:9200".into()],
            timeout_ms: 30_000,
            username: None,
            password: None,
        }
    }
}

impl ElasticsearchConfig {
    pub fn from_params(params: &std::collections::HashMap<String, String>) -> Self {
        Self {
            hosts: params
                .get("hosts")
                .map(|h| h.split(',').map(|s| s.trim().to_owned()).collect())
                .unwrap_or_else(|| vec!["http://localhost:9200".into()]),
            timeout_ms: params
                .get("timeout")
                .and_then(|s| s.parse().ok())
                .unwrap_or(30_000),
            username: params.get("username").cloned(),
            password: params.get("password").cloned(),
        }
    }
}

/// The Elasticsearch adapter. Holds a shared client for all ops.
pub struct ElasticsearchAdapter {
    client: Elasticsearch,
}

impl ElasticsearchAdapter {
    pub fn with_config(config: ElasticsearchConfig) -> Result<Self, String> {
        let host = config
            .hosts
            .into_iter()
            .next()
            .unwrap_or_else(|| "http://localhost:9200".into());
        let url = elasticsearch::http::Url::parse(&host)
            .map_err(|e| format!("invalid Elasticsearch host URL '{host}': {e}"))?;
        let pool = SingleNodeConnectionPool::new(url);
        let mut builder = TransportBuilder::new(pool)
            .timeout(std::time::Duration::from_millis(config.timeout_ms));
        if let (Some(user), Some(pass)) = (config.username, config.password) {
            builder = builder.auth(Credentials::Basic(user, pass));
        }
        let transport = builder
            .build()
            .map_err(|e| format!("failed to build Elasticsearch transport: {e}"))?;
        Ok(Self {
            client: Elasticsearch::new(transport),
        })
    }
}

/// ES operation type, parsed once at `map_op` time.
#[derive(Clone, Copy, Debug)]
enum EsAction {
    Index,
    Search,
    Get,
    Delete,
    Bulk,
}

impl EsAction {
    fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "index" => Some(Self::Index),
            "search" => Some(Self::Search),
            "get" => Some(Self::Get),
            "delete" => Some(Self::Delete),
            "bulk" => Some(Self::Bulk),
            _ => None,
        }
    }
}

/// Per-op dispenser. Created once per op template; `execute` is the hot path.
struct EsDispenser {
    client: Elasticsearch,
    action: EsAction,
    /// Template strings rendered per-cycle through `substitute_via_wires`.
    index_template: Option<String>,
    id_template: Option<String>,
    body_template: Option<String>,
    canonical_kernel: Arc<PolydatKernel>,
}

// ── helpers ──────────────────────────────────────────────────────────────────

fn bind_error(field: &str, msg: impl std::fmt::Display) -> ExecutionError {
    ExecutionError::Op(AdapterError {
        error_name: "BindError".into(),
        message: format!("{field}: {msg}"),
        retryable: false,
    })
}

fn missing_field(field: &str, action: &str) -> ExecutionError {
    ExecutionError::Op(AdapterError {
        error_name: "MissingField".into(),
        message: format!("'{action}' action requires the '{field}' op field"),
        retryable: false,
    })
}

fn parse_json(s: &str, context: &str) -> Result<Value, ExecutionError> {
    serde_json::from_str(s).map_err(|e| {
        ExecutionError::Op(AdapterError {
            error_name: "JsonParseError".into(),
            message: format!("{context}: {e}"),
            retryable: false,
        })
    })
}

// ── DriverAdapter ─────────────────────────────────────────────────────────────

impl DriverAdapter for ElasticsearchAdapter {
    fn name(&self) -> &str {
        "elasticsearch"
    }

    fn known_op_fields(&self) -> Option<&'static [&'static str]> {
        Some(&["action", "index", "id", "body"])
    }

    fn map_op<'a>(
        &'a self,
        template: &'a ParsedOp,
        parent: Arc<PolydatKernel>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Box<dyn OpDispenser>, String>> + Send + 'a>,
    > {
        Box::pin(async move {
            let action_str = template
                .op
                .get("action")
                .and_then(|v| v.as_str())
                .unwrap_or("index");
            let action = EsAction::parse(action_str).ok_or_else(|| {
                format!(
                    "unknown Elasticsearch action '{action_str}'; \
                     expected one of: index, search, get, delete, bulk"
                )
            })?;

            Ok(Box::new(EsDispenser {
                client: self.client.clone(),
                action,
                index_template: template
                    .op
                    .get("index")
                    .and_then(|v| v.as_str())
                    .map(String::from),
                id_template: template
                    .op
                    .get("id")
                    .and_then(|v| v.as_str())
                    .map(String::from),
                body_template: template
                    .op
                    .get("body")
                    .and_then(|v| v.as_str())
                    .map(String::from),
                canonical_kernel: parent,
            }) as Box<dyn OpDispenser>)
        })
    }
}

// ── OpDispenser ───────────────────────────────────────────────────────────────

impl OpDispenser for EsDispenser {
    fn canonical_kernel(&self) -> Option<&Arc<PolydatKernel>> {
        Some(&self.canonical_kernel)
    }

    fn execute<'a>(
        &'a self,
        _cycle: u64,
        ctx: &'a nbrs_activity::adapter::ExecCtx<'a>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<OpResult, ExecutionError>> + Send + 'a>,
    > {
        let wires = ctx.wires;
        Box::pin(async move {
            // Resolve all templated fields before dispatching.
            let resolve = |tmpl: &str, field: &'static str| {
                nbrs_activity::wires::substitute_via_wires(tmpl, wires)
                    .map_err(|e| bind_error(field, e))
            };
            let index = self
                .index_template
                .as_deref()
                .map(|t| resolve(t, "index"))
                .transpose()?;
            let id = self
                .id_template
                .as_deref()
                .map(|t| resolve(t, "id"))
                .transpose()?;
            let body_str = self
                .body_template
                .as_deref()
                .map(|t| resolve(t, "body"))
                .transpose()?;

            let map_transport = |e: elasticsearch::Error| {
                ExecutionError::Adapter(AdapterError {
                    error_name: "TransportError".into(),
                    message: e.to_string(),
                    retryable: true,
                })
            };

            let mut response = match self.action {
                EsAction::Index => {
                    let idx =
                        index.as_deref().ok_or_else(|| missing_field("index", "index"))?;
                    let body = body_str
                        .as_deref()
                        .map(|s| parse_json(s, "body"))
                        .transpose()?
                        .unwrap_or(Value::Object(Default::default()));
                    let req = match id.as_deref() {
                        Some(doc_id) => self.client.index(IndexParts::IndexId(idx, doc_id)),
                        None => self.client.index(IndexParts::Index(idx)),
                    };
                    req.body(body).send().await.map_err(map_transport)?
                }
                EsAction::Search => {
                    let body = body_str
                        .as_deref()
                        .map(|s| parse_json(s, "body"))
                        .transpose()?
                        .unwrap_or_else(|| serde_json::json!({"query": {"match_all": {}}}));
                    // Collect owned strings so references are valid across the await point.
                    let idx_owned: Vec<String> = index
                        .as_deref()
                        .map(|i| i.split(',').map(|s| s.trim().to_owned()).collect())
                        .unwrap_or_default();
                    let idx_refs: Vec<&str> = idx_owned.iter().map(String::as_str).collect();
                    let parts = if idx_refs.is_empty() {
                        SearchParts::None
                    } else {
                        SearchParts::Index(&idx_refs)
                    };
                    self.client
                        .search(parts)
                        .body(body)
                        .send()
                        .await
                        .map_err(map_transport)?
                }
                EsAction::Get => {
                    let idx =
                        index.as_deref().ok_or_else(|| missing_field("index", "get"))?;
                    let doc_id =
                        id.as_deref().ok_or_else(|| missing_field("id", "get"))?;
                    self.client
                        .get(GetParts::IndexId(idx, doc_id))
                        .send()
                        .await
                        .map_err(map_transport)?
                }
                EsAction::Delete => {
                    let idx =
                        index.as_deref().ok_or_else(|| missing_field("index", "delete"))?;
                    let doc_id =
                        id.as_deref().ok_or_else(|| missing_field("id", "delete"))?;
                    self.client
                        .delete(DeleteParts::IndexId(idx, doc_id))
                        .send()
                        .await
                        .map_err(map_transport)?
                }
                EsAction::Bulk => {
                    let raw = body_str
                        .as_deref()
                        .ok_or_else(|| missing_field("body", "bulk"))?;
                    // Parse NDJSON body: each non-empty line is one JSON object.
                    // Callers provide action+source pairs in the correct order.
                    let lines: Vec<Value> = raw
                        .lines()
                        .filter(|l| !l.trim().is_empty())
                        .map(|l| parse_json(l, "bulk body line"))
                        .collect::<Result<Vec<_>, _>>()?;
                    let parts = match index.as_deref() {
                        Some(idx) => BulkParts::Index(idx),
                        None => BulkParts::None,
                    };
                    self.client
                        .bulk(parts)
                        .body(lines)
                        .send()
                        .await
                        .map_err(map_transport)?
                }
            };

            let status = response.status_code();
            if status.is_success() {
                let json: Value = response.json().await.map_err(|e| {
                    ExecutionError::Op(AdapterError {
                        error_name: "ResponseReadError".into(),
                        message: format!("failed to read Elasticsearch response body: {e}"),
                        retryable: false,
                    })
                })?;
                Ok(OpResult {
                    body: Some(Box::new(JsonBody(json))),
                    skipped: false,
                })
            } else {
                let code = status.as_u16();
                let text = response.text().await.unwrap_or_default();
                Err(ExecutionError::Op(AdapterError {
                    error_name: format!("EsStatus{code}"),
                    message: format!("Elasticsearch HTTP {code}: {text}"),
                    retryable: code >= 500,
                }))
            }
        })
    }
}

// =========================================================================
// Adapter Registration (inventory-based, link-time)
// =========================================================================

inventory::submit! {
    nbrs_activity::adapter::AdapterRegistration {
        names: || &["elasticsearch", "es"],
        known_params: || &["hosts", "timeout", "username", "password"],
        display_preference: || nbrs_activity::adapter::DisplayPreference::Auto,
        create: |params| Box::pin(async move {
            ElasticsearchAdapter::with_config(ElasticsearchConfig::from_params(&params))
                .map(|a| std::sync::Arc::new(a) as std::sync::Arc<dyn nbrs_activity::adapter::DriverAdapter>)
        }),
    }
}

// The Elasticsearch client is documented thread-safe and pools connections
// internally. Share one adapter instance across phases with matching
// (hosts, timeout) so they reuse the connection pool.
inventory::submit! {
    nbrs_activity::adapter::SharedDriverRegistration {
        adapter: "elasticsearch",
        driver: nbrs_activity::adapter::DEFAULT_DRIVER_NAME,
        share_capability: nbrs_activity::resource_pool::ShareCapability::Shared,
        resource_key: |params| {
            let cfg = ElasticsearchConfig::from_params(&params);
            let host = cfg.hosts.into_iter().next().unwrap_or_default();
            Ok(nbrs_activity::resource_pool::ResourceKey::new("elasticsearch")
                .with("host", host)
                .with("timeout_ms", cfg.timeout_ms.to_string()))
        },
    }
}
