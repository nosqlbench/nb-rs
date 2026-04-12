// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! CQL/Cassandra adapter for nb-rs.
//!
//! Uses the Apache Cassandra C++ driver via the `cassandra-cpp` crate.
//! Compatible with Apache Cassandra, ScyllaDB, and DataStax Astra.

use std::collections::HashMap;
use std::sync::Arc;

use cassandra_cpp as cass;
use cass::LendingIterator;
use nb_activity::adapter::{
    AdapterError, DriverAdapter, ExecutionError, OpDispenser, OpResult, ResolvedFields, ResultBody,
};
use nb_workload::model::ParsedOp;

// =========================================================================
// CqlResultBody: native result type for validation and capture
// =========================================================================

/// Native CQL result body carrying typed row data.
///
/// Consumers can downcast via `as_any()` to extract typed column values
/// without JSON round-tripping — used by `ValidatingDispenser` for
/// relevancy measurement.
#[derive(Debug)]
pub struct CqlResultBody {
    /// Rows as JSON-compatible maps: column_name → value.
    pub rows: Vec<HashMap<String, serde_json::Value>>,
}

impl CqlResultBody {
    /// Build from a cassandra-cpp CassResult by iterating rows and columns.
    fn from_cass_result(result: &cass::CassResult) -> Self {
        let row_count = result.row_count() as usize;
        let mut rows = Vec::with_capacity(row_count);
        let col_count = result.column_count() as usize;
        let mut iter = result.iter();
        while let Some(row) = iter.next() {
            let mut map = HashMap::new();
            for col_idx in 0..col_count {
                let col_name = result.column_name(col_idx)
                    .unwrap_or("?")
                    .to_string();
                let value = Self::extract_column_value(&row, col_idx);
                map.insert(col_name, value);
            }
            rows.push(map);
        }
        Self { rows }
    }

    /// Extract a single column value as serde_json::Value.
    fn extract_column_value(row: &cass::Row, col_idx: usize) -> serde_json::Value {
        // Try common types in order of likelihood
        if let Ok(v) = row.get_column(col_idx).and_then(|c| c.get_string()) {
            return serde_json::Value::String(v);
        }
        if let Ok(v) = row.get_column(col_idx).and_then(|c| c.get_i64()) {
            return serde_json::json!(v);
        }
        if let Ok(v) = row.get_column(col_idx).and_then(|c| c.get_i32()) {
            return serde_json::json!(v);
        }
        if let Ok(v) = row.get_column(col_idx).and_then(|c| c.get_f64()) {
            return serde_json::json!(v);
        }
        if let Ok(v) = row.get_column(col_idx).and_then(|c| c.get_f32()) {
            return serde_json::json!(v);
        }
        if let Ok(v) = row.get_column(col_idx).and_then(|c| c.get_bool()) {
            return serde_json::json!(v);
        }
        // Fallback: null for unsupported types
        serde_json::Value::Null
    }

    /// Get a column value from the first row as i64 (for relevancy extraction).
    pub fn get_column_i64_values(&self, column: &str) -> Vec<i64> {
        self.rows.iter()
            .filter_map(|row| row.get(column)?.as_i64())
            .collect()
    }

    /// Get a column value from the first row as string (for capture).
    pub fn get_column_string_values(&self, column: &str) -> Vec<String> {
        self.rows.iter()
            .filter_map(|row| {
                let v = row.get(column)?;
                match v {
                    serde_json::Value::String(s) => Some(s.clone()),
                    other => Some(other.to_string()),
                }
            })
            .collect()
    }
}

impl ResultBody for CqlResultBody {
    fn to_json(&self) -> serde_json::Value {
        serde_json::Value::Array(
            self.rows.iter()
                .map(|row| serde_json::Value::Object(
                    row.iter()
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect()
                ))
                .collect()
        )
    }

    fn as_any(&self) -> &dyn std::any::Any { self }

    fn element_count(&self) -> u64 { self.rows.len() as u64 }
}

/// Configuration for the CQL adapter.
#[derive(Debug, Clone)]
pub struct CqlConfig {
    pub hosts: String,
    pub port: u16,
    pub keyspace: String,
    pub consistency: String,
    pub username: Option<String>,
    pub password: Option<String>,
    pub request_timeout_ms: u64,
}

impl Default for CqlConfig {
    fn default() -> Self {
        Self {
            hosts: "127.0.0.1".into(),
            port: 9042,
            keyspace: String::new(),
            consistency: "LOCAL_ONE".into(),
            username: None,
            password: None,
            request_timeout_ms: 12_000,
        }
    }
}

impl CqlConfig {
    pub fn from_params(params: &HashMap<String, String>) -> Self {
        let mut config = Self::default();
        if let Some(v) = params.get("hosts").or(params.get("host")) { config.hosts = v.clone(); }
        if let Some(v) = params.get("port") { config.port = v.parse().unwrap_or(9042); }
        if let Some(v) = params.get("keyspace") { config.keyspace = v.clone(); }
        if let Some(v) = params.get("consistency") { config.consistency = v.clone(); }
        if let Some(v) = params.get("username") { config.username = Some(v.clone()); }
        if let Some(v) = params.get("password") { config.password = Some(v.clone()); }
        if let Some(v) = params.get("request_timeout_ms") { config.request_timeout_ms = v.parse().unwrap_or(12_000); }
        config
    }
}

fn parse_consistency(s: &str) -> cass::Consistency {
    match s.to_uppercase().as_str() {
        "ANY" => cass::Consistency::ANY,
        "ONE" => cass::Consistency::ONE,
        "TWO" => cass::Consistency::TWO,
        "THREE" => cass::Consistency::THREE,
        "QUORUM" => cass::Consistency::QUORUM,
        "ALL" => cass::Consistency::ALL,
        "LOCAL_QUORUM" => cass::Consistency::LOCAL_QUORUM,
        "EACH_QUORUM" => cass::Consistency::EACH_QUORUM,
        "LOCAL_ONE" => cass::Consistency::LOCAL_ONE,
        _ => cass::Consistency::LOCAL_ONE,
    }
}

/// CQL adapter using the Apache Cassandra C++ driver.
pub struct CqlAdapter {
    session: cass::Session,
    consistency: cass::Consistency,
}

unsafe impl Send for CqlAdapter {}
unsafe impl Sync for CqlAdapter {}

impl CqlAdapter {
    pub async fn connect(config: &CqlConfig) -> Result<Self, String> {
        let mut cluster = cass::Cluster::default();
        cluster.set_contact_points(&config.hosts)
            .map_err(|e| format!("set contact points: {e}"))?;
        cluster.set_port(config.port)
            .map_err(|e| format!("set port: {e}"))?;
        if let (Some(u), Some(p)) = (&config.username, &config.password) {
            cluster.set_credentials(u, p)
                .map_err(|e| format!("set credentials: {e}"))?;
        }
        cluster.set_request_timeout(std::time::Duration::from_millis(config.request_timeout_ms));

        let session = if config.keyspace.is_empty() {
            cluster.connect().await.map_err(|e| format!("connect: {e}"))?
        } else {
            cluster.connect_keyspace(&config.keyspace).await
                .map_err(|e| format!("connect keyspace '{}': {e}", config.keyspace))?
        };

        Ok(Self {
            session,
            consistency: parse_consistency(&config.consistency),
        })
    }
}

// =========================================================================
// DriverAdapter: dispatch to the correct dispenser based on field name
// =========================================================================

/// Statement field names that select the execution mode.
const STMT_FIELD_NAMES: &[&str] = &["raw", "simple", "prepared", "stmt"];

impl DriverAdapter for CqlAdapter {
    fn name(&self) -> &str { "cql" }

    fn map_op(&self, template: &ParsedOp) -> Result<Box<dyn OpDispenser>, String> {
        // Find the statement text and determine execution mode from the field name.
        let (stmt_text, mode, field_name) = STMT_FIELD_NAMES.iter()
            .find_map(|key| -> Option<(String, &str, String)> {
                let v = template.op.get(*key)?;
                let text = v.as_str()?;
                Some((text.to_string(), *key, key.to_string()))
            })
            .ok_or_else(|| "CQL op requires a 'raw:', 'simple:', 'prepared:', or 'stmt:' field".to_string())?;

        // Bind point names: op field keys that aren't the statement field
        let bind_names: Vec<String> = template.op.keys()
            .filter(|k| !STMT_FIELD_NAMES.contains(&k.as_str()))
            .cloned()
            .collect();

        let session = SessionHandle(&self.session as *const cass::Session);
        let consistency = self.consistency;

        match mode {
            "raw" => {
                // Raw: all bind points are string-interpolated by the executor.
                // The dispenser receives the fully-rendered text and executes it.
                Ok(Box::new(CqlRawDispenser {
                    session,
                    consistency,
                    field_name,
                }))
            }
            "simple" => {
                // Simple: parameterized but not prepared.
                // Note: cassandra-cpp doesn't expose SimpleStatement with positional
                // params directly. For now, this behaves like raw at the driver level.
                // The distinction is preserved for future drivers that support it.
                Ok(Box::new(CqlRawDispenser {
                    session,
                    consistency,
                    field_name,
                }))
            }
            _ => {
                // Prepared (default): prepare + typed bind.
                if bind_names.is_empty() {
                    // No bind points — execute as raw (DDL, simple queries).
                    Ok(Box::new(CqlRawDispenser {
                        session,
                        consistency,
                        field_name,
                    }))
                } else {
                    Ok(Box::new(CqlPreparedDispenser {
                        session,
                        consistency,
                        stmt_text,
                        field_name,
                        bind_names,
                        prepared: std::sync::Mutex::new(None),
                    }))
                }
            }
        }
    }
}

// =========================================================================
// Session handle wrapper (Send+Sync for raw pointer)
// =========================================================================

struct SessionHandle(*const cass::Session);
unsafe impl Send for SessionHandle {}
unsafe impl Sync for SessionHandle {}

impl SessionHandle {
    fn get(&self) -> &cass::Session {
        unsafe { &*self.0 }
    }
}

// =========================================================================
// CqlRawDispenser: string interpolation, direct execute
// =========================================================================

/// Executes the fully-interpolated statement text directly.
///
/// Used for:
/// - `raw:` mode (all bind points resolved to text by the executor)
/// - `simple:` mode (same driver path, distinction preserved for API)
/// - `prepared:`/`stmt:` mode when there are no bind points (DDL)
struct CqlRawDispenser {
    session: SessionHandle,
    consistency: cass::Consistency,
    /// The op field name that carries the statement ("raw", "simple", "prepared", "stmt").
    field_name: String,
}

impl OpDispenser for CqlRawDispenser {
    fn execute<'a>(
        &'a self,
        _cycle: u64,
        fields: &'a ResolvedFields,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<OpResult, ExecutionError>> + Send + 'a>> {
        Box::pin(async move {
            // Read the fully-interpolated statement from the resolved field
            let stmt_text = fields.get_str(&self.field_name)
                .ok_or_else(|| ExecutionError::Op(AdapterError {
                    error_name: "missing_field".into(),
                    message: format!("CQL op missing '{}' field", self.field_name),
                    retryable: false,
                }))?;

            let result = self.session.get().execute(stmt_text).await
                .map_err(|e| ExecutionError::Op(AdapterError {
                    error_name: "cql_error".into(),
                    message: format!("{e}"),
                    retryable: false,
                }))?;

            let body = if result.row_count() > 0 {
                Some(Box::new(CqlResultBody::from_cass_result(&result)) as Box<dyn ResultBody>)
            } else {
                None
            };
            Ok(OpResult {
                body,
                captures: std::collections::HashMap::new(),
            })
        })
    }
}

// =========================================================================
// CqlPreparedDispenser: prepare once, bind typed values per cycle
// =========================================================================

/// Prepares the statement lazily on first execute, then binds typed
/// values by name for each subsequent cycle.
struct CqlPreparedDispenser {
    session: SessionHandle,
    consistency: cass::Consistency,
    stmt_text: String,
    /// The op field name (for error messages).
    field_name: String,
    /// Names of bind point fields to extract from ResolvedFields.
    bind_names: Vec<String>,
    /// Lazily prepared statement (first execute prepares it).
    prepared: std::sync::Mutex<Option<Arc<cass::PreparedStatement>>>,
}

impl CqlPreparedDispenser {
    async fn get_prepared(&self) -> Result<Arc<cass::PreparedStatement>, ExecutionError> {
        {
            let guard = self.prepared.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(p) = guard.as_ref() {
                return Ok(p.clone());
            }
        }
        let prepared = self.session.get().prepare(&self.stmt_text).await
            .map_err(|e| ExecutionError::Op(AdapterError {
                error_name: "prepare_error".into(),
                message: format!("prepare '{}': {e}", self.stmt_text),
                retryable: false,
            }))?;
        let arc = Arc::new(prepared);
        *self.prepared.lock().unwrap_or_else(|e| e.into_inner()) = Some(arc.clone());
        Ok(arc)
    }
}

impl OpDispenser for CqlPreparedDispenser {
    fn execute<'a>(
        &'a self,
        _cycle: u64,
        fields: &'a ResolvedFields,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<OpResult, ExecutionError>> + Send + 'a>> {
        Box::pin(async move {
            let prepared = self.get_prepared().await?;
            let mut stmt = prepared.bind();
            stmt.set_consistency(self.consistency)
                .map_err(|e| ExecutionError::Op(AdapterError {
                    error_name: "bind_error".into(),
                    message: format!("set consistency: {e}"),
                    retryable: false,
                }))?;

            // Bind typed values by name
            for name in &self.bind_names {
                if let Some(value) = fields.get_value(name) {
                    let r = match value {
                        nb_variates::node::Value::U64(v) => stmt.bind_int64_by_name(name, *v as i64),
                        nb_variates::node::Value::F64(v) => stmt.bind_double_by_name(name, *v),
                        nb_variates::node::Value::Bool(v) => stmt.bind_bool_by_name(name, *v),
                        nb_variates::node::Value::Str(v) => stmt.bind_string_by_name(name, v),
                        nb_variates::node::Value::Bytes(v) => stmt.bind_bytes_by_name(name, v.clone()),
                        _ => stmt.bind_string_by_name(name, &value.to_display_string()),
                    };
                    r.map_err(|e| ExecutionError::Op(AdapterError {
                        error_name: "bind_error".into(),
                        message: format!("bind '{name}': {e}"),
                        retryable: false,
                    }))?;
                }
            }

            let result = stmt.execute().await
                .map_err(|e| ExecutionError::Op(AdapterError {
                    error_name: "cql_error".into(),
                    message: format!("{e}"),
                    retryable: false,
                }))?;

            let body = if result.row_count() > 0 {
                Some(Box::new(CqlResultBody::from_cass_result(&result)) as Box<dyn ResultBody>)
            } else {
                None
            };
            Ok(OpResult {
                body,
                captures: std::collections::HashMap::new(),
            })
        })
    }
}
