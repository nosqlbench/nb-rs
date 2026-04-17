// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! CQL/Cassandra adapter for nb-rs.
//!
//! Uses the Apache Cassandra C++ driver via the `cassandra-cpp` crate.
//! Compatible with Apache Cassandra, ScyllaDB, and DataStax Astra.
//!
//! Also registers CQL-specific GK nodes via the `inventory` mechanism:
//! - `cql_timeuuid`: generates a deterministic type-1-like UUID string
//!   from a u64 seed, suitable for CQL `timeuuid` columns.

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
    /// Keyspace to connect to. If empty, connects without a keyspace
    /// (required for CREATE KEYSPACE DDL). The `{keyspace}` bind point
    /// in op templates resolves from the workload param, not this field.
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
    pub fn from_params(params: &HashMap<String, String>) -> Result<Self, String> {
        let mut config = Self::default();
        if let Some(v) = params.get("hosts").or(params.get("host")) { config.hosts = v.clone(); }
        if let Some(v) = params.get("port") {
            config.port = v.parse()
                .map_err(|_| format!("invalid port value '{v}' — expected an integer"))?;
        }
        // connect_keyspace overrides keyspace for the driver connection,
        // leaving {keyspace} in op templates to resolve from workload params.
        // Use connect_keyspace="" to connect without a keyspace (for DDL).
        if let Some(v) = params.get("connect_keyspace") {
            config.keyspace = v.clone();
        } else if let Some(v) = params.get("keyspace") {
            config.keyspace = v.clone();
        }
        if let Some(v) = params.get("consistency") {
            if parse_consistency(v).is_none() {
                return Err(format!(
                    "unrecognized consistency level '{v}'. \
                     Valid: ANY, ONE, TWO, THREE, QUORUM, ALL, LOCAL_QUORUM, EACH_QUORUM, LOCAL_ONE"
                ));
            }
            config.consistency = v.clone();
        }
        if let Some(v) = params.get("username") { config.username = Some(v.clone()); }
        if let Some(v) = params.get("password") { config.password = Some(v.clone()); }
        if let Some(v) = params.get("request_timeout_ms") {
            config.request_timeout_ms = v.parse()
                .map_err(|_| format!("invalid request_timeout_ms value '{v}' — expected an integer"))?;
        }
        Ok(config)
    }
}

fn parse_consistency(s: &str) -> Option<cass::Consistency> {
    match s.to_uppercase().as_str() {
        "ANY" => Some(cass::Consistency::ANY),
        "ONE" => Some(cass::Consistency::ONE),
        "TWO" => Some(cass::Consistency::TWO),
        "THREE" => Some(cass::Consistency::THREE),
        "QUORUM" => Some(cass::Consistency::QUORUM),
        "ALL" => Some(cass::Consistency::ALL),
        "LOCAL_QUORUM" => Some(cass::Consistency::LOCAL_QUORUM),
        "EACH_QUORUM" => Some(cass::Consistency::EACH_QUORUM),
        "LOCAL_ONE" => Some(cass::Consistency::LOCAL_ONE),
        _ => None,
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

        let consistency = parse_consistency(&config.consistency)
            .ok_or_else(|| format!("unrecognized consistency level '{}'", config.consistency))?;

        // Try to connect to the specified keyspace. If it doesn't exist,
        // fall back to connecting without a keyspace (needed for DDL phases
        // that create the keyspace).
        let session = if config.keyspace.is_empty() {
            cluster.connect().await.map_err(|e| format!("connect: {e}"))?
        } else {
            match cluster.connect_keyspace(&config.keyspace).await {
                Ok(s) => s,
                Err(e) => {
                    let msg = e.to_string();
                    // Only fall back for keyspace-not-found errors.
                    // Auth failures, network errors, etc. should propagate.
                    if msg.contains("Keyspace") || msg.contains("keyspace") || msg.contains("not found") {
                        eprintln!("cassnbrs: keyspace '{}' not found, connecting without keyspace", config.keyspace);
                        cluster.connect().await.map_err(|e| format!("connect (no keyspace): {e}"))?
                    } else {
                        return Err(format!("connect to keyspace '{}': {e}", config.keyspace));
                    }
                }
            }
        };

        Ok(Self {
            session,
            consistency,
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

    fn default_status_metrics(&self) -> Vec<nb_activity::adapter::StatusMetric> {
        vec![
            nb_activity::adapter::StatusMetric {
                metric_name: "rows_inserted".to_string(),
                display: "rows/s".to_string(),
                render: nb_activity::adapter::StatusRender::Rate,
            },
        ]
    }

    fn map_op(&self, template: &ParsedOp) -> Result<Box<dyn OpDispenser>, String> {
        // Find the statement text and determine execution mode from the field name.
        let (stmt_text, mode, field_name) = STMT_FIELD_NAMES.iter()
            .find_map(|key| -> Option<(String, &str, String)> {
                let v = template.op.get(*key)?;
                let text = v.as_str()?;
                Some((text.to_string(), *key, key.to_string()))
            })
            .ok_or_else(|| "CQL op requires a 'poll:', 'raw:', 'simple:', 'prepared:', or 'stmt:' field".to_string())?;

        // Extract bind point names from the statement text ({name} patterns)
        // and build the CQL-parameterized version with ? markers for prepared mode.
        let bind_names: Vec<String> = nb_workload::bindpoints::referenced_bindings(&stmt_text);
        let prepared_text = nb_workload::bindpoints::replace_bind_points_with_markers(&stmt_text);

        let session = SessionHandle(&self.session as *const cass::Session);
        let consistency = self.consistency;

        // Check for batch configuration on this op.
        // batch: <integer> — batch size (rows per batch), type defaults to unlogged.
        // batchtype: logged|unlogged|counter — overrides batch type.
        let has_batch = template.params.contains_key("batch");
        let batch_type = template.params.get("batchtype")
            .and_then(|v| v.as_str())
            .map(|s| match s.to_lowercase().as_str() {
                "logged" => cass::BatchType::LOGGED,
                "counter" => cass::BatchType::COUNTER,
                _ => cass::BatchType::UNLOGGED,
            })
            .unwrap_or(cass::BatchType::UNLOGGED);

        match mode {
            "raw" => {
                Ok(Box::new(CqlRawDispenser {
                    session,
                    field_name,
                }))
            }
            "simple" => {
                Ok(Box::new(CqlRawDispenser {
                    session,
                    field_name,
                }))
            }
            _ => {
                if has_batch {
                    eprintln!("[cql] creating batch dispenser: type={batch_type:?}");
                    Ok(Box::new(CqlBatchDispenser {
                        session,
                        consistency,
                        stmt_text: prepared_text.clone(),
                        stmt_field: "stmt".to_string(),
                        bind_names,
                        prepared: std::sync::OnceLock::new(),
                        batch_type,
                        rows_timer: nb_metrics::instruments::timer::Timer::new(
                            nb_metrics::labels::Labels::of("name", "rows_inserted"),
                        ),
                        rows_total: std::sync::atomic::AtomicU64::new(0),
                    }))
                } else if bind_names.is_empty() {
                    // No bind points — execute as raw (DDL, simple queries)
                    Ok(Box::new(CqlRawDispenser {
                        session,
                        field_name,
                    }))
                } else {
                    Ok(Box::new(CqlPreparedDispenser {
                        session,
                        consistency,
                        stmt_text: prepared_text,
                        bind_names,
                        prepared: std::sync::OnceLock::new(),
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
                .map_err(|e| {
                    let truncated = if stmt_text.len() > 200 {
                        format!("{}...", &stmt_text[..200])
                    } else {
                        stmt_text.to_string()
                    };
                    ExecutionError::Op(AdapterError {
                        error_name: "cql_error".into(),
                        message: format!("{e}\n  statement: {truncated}"),
                        retryable: false,
                    })
                })?;

            let body = if result.row_count() > 0 {
                Some(Box::new(CqlResultBody::from_cass_result(&result)) as Box<dyn ResultBody>)
            } else {
                None
            };
            Ok(OpResult {
                body,
                captures: std::collections::HashMap::new(),
                skipped: false,
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
    /// Names of bind point fields to extract from ResolvedFields.
    bind_names: Vec<String>,
    /// Prepared once on first execute, then lock-free reads thereafter.
    prepared: std::sync::OnceLock<Arc<cass::PreparedStatement>>,
}

impl CqlPreparedDispenser {
    async fn get_prepared(&self) -> Result<Arc<cass::PreparedStatement>, ExecutionError> {
        if let Some(p) = self.prepared.get() {
            return Ok(p.clone());
        }
        let prepared = self.session.get().prepare(&self.stmt_text).await
            .map_err(|e| ExecutionError::Op(AdapterError {
                error_name: "prepare_error".into(),
                message: format!("prepare '{}': {e}", self.stmt_text),
                retryable: false,
            }))?;
        let arc = Arc::new(prepared);
        let _ = self.prepared.set(arc.clone());
        Ok(self.prepared.get().unwrap().clone())
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

            // Bind typed values by position (GK names don't match CQL column names)
            for (bind_idx, name) in self.bind_names.iter().enumerate() {
                if let Some(value) = fields.get_value(name) {
                    let r = match value {
                        nb_variates::node::Value::U64(v) => stmt.bind_int64(bind_idx, *v as i64),
                        nb_variates::node::Value::F64(v) => stmt.bind_double(bind_idx, *v),
                        nb_variates::node::Value::Bool(v) => stmt.bind_bool(bind_idx, *v),
                        nb_variates::node::Value::Str(v) => stmt.bind_string(bind_idx, v),
                        nb_variates::node::Value::Bytes(v) => stmt.bind_bytes(bind_idx, v.clone()),
                        _ => stmt.bind_string(bind_idx, &value.to_display_string()),
                    };
                    r.map_err(|e| ExecutionError::Op(AdapterError {
                        error_name: "bind_error".into(),
                        message: format!("bind position {bind_idx} ('{name}'): {e}"),
                        retryable: false,
                    }))?;
                }
            }

            let result = stmt.execute().await
                .map_err(|e| {
                    let truncated = if self.stmt_text.len() > 200 {
                        format!("{}...", &self.stmt_text[..200])
                    } else {
                        self.stmt_text.clone()
                    };
                    ExecutionError::Op(AdapterError {
                        error_name: "cql_error".into(),
                        message: format!("{e}\n  statement: {truncated}"),
                        retryable: false,
                    })
                })?;

            let body = if result.row_count() > 0 {
                Some(Box::new(CqlResultBody::from_cass_result(&result)) as Box<dyn ResultBody>)
            } else {
                None
            };
            Ok(OpResult {
                body,
                captures: std::collections::HashMap::new(),
                skipped: false,
            })
        })
    }
}

// =========================================================================
// CqlBatchDispenser: groups multiple bound statements into one CQL BATCH
// =========================================================================

/// Wraps a prepared statement template and executes batches of bound
/// statements as one CQL BATCH call.
///
/// The executor calls `execute_batch()` with N resolved field sets
/// (one per cycle in the batch). Each is bound to the prepared
/// statement and added to a `cass::Batch`. The batch is executed
/// once. Per-cycle latency is meaningless — only batch latency matters.
struct CqlBatchDispenser {
    session: SessionHandle,
    consistency: cass::Consistency,
    stmt_text: String,
    /// The op field name carrying the statement (for finding it in resolved fields).
    #[allow(dead_code)]
    stmt_field: String,
    #[allow(dead_code)] // retained for diagnostics
    bind_names: Vec<String>,
    /// Prepared once on first execute, then lock-free reads thereafter.
    prepared: std::sync::OnceLock<Arc<cass::PreparedStatement>>,
    batch_type: cass::BatchType,
    /// Per-row timer: records amortized latency (batch_nanos / row_count)
    /// for each row in the batch. Enables rows/s throughput in the summary.
    rows_timer: nb_metrics::instruments::timer::Timer,
    /// Cumulative row counter for the status line. Not reset on snapshot.
    rows_total: std::sync::atomic::AtomicU64,
}

impl CqlBatchDispenser {
    /// Get or prepare the statement. Lock-free after first call.
    /// Multiple fibers may race to prepare on first execute — the
    /// OnceLock ensures only one result is stored; the CQL driver
    /// handles duplicate prepares gracefully.
    async fn get_prepared(&self) -> Result<Arc<cass::PreparedStatement>, ExecutionError> {
        if let Some(p) = self.prepared.get() {
            return Ok(p.clone());
        }
        let prepared = self.session.get().prepare(&self.stmt_text).await
            .map_err(|e| ExecutionError::Op(AdapterError {
                error_name: "prepare_error".into(),
                message: format!("prepare '{}': {e}", self.stmt_text),
                retryable: false,
            }))?;
        let arc = Arc::new(prepared);
        // First to finish wins; others' results are harmlessly dropped.
        let _ = self.prepared.set(arc.clone());
        Ok(self.prepared.get().unwrap().clone())
    }
}

impl OpDispenser for CqlBatchDispenser {
    fn status_counters(&self) -> Vec<(&str, u64)> {
        let total = self.rows_total.load(std::sync::atomic::Ordering::Relaxed);
        if total == 0 { return Vec::new(); }
        vec![("rows_inserted", total)]
    }

    fn adapter_metrics(&self) -> Vec<nb_metrics::frame::Sample> {
        let snap = self.rows_timer.snapshot();
        let total = self.rows_total.load(std::sync::atomic::Ordering::Relaxed);
        let mut samples = Vec::new();
        if snap.count > 0 {
            samples.push(nb_metrics::frame::Sample::Timer {
                labels: self.rows_timer.labels().clone(),
                count: snap.count,
                histogram: snap.histogram,
            });
        }
        if total > 0 {
            samples.push(nb_metrics::frame::Sample::Counter {
                labels: nb_metrics::labels::Labels::of("name", "rows_inserted_total"),
                value: total,
            });
        }
        samples
    }

    fn execute<'a>(
        &'a self,
        _cycle: u64,
        fields: &'a ResolvedFields,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<OpResult, ExecutionError>> + Send + 'a>> {
        Box::pin(async move {
            let prepared = self.get_prepared().await?;
            let mut batch = self.session.get().batch(self.batch_type);

            // Bind typed values positionally to the prepared statement.
            // batch_fields carries typed GK values per row (from BindPlan),
            // with names matching the bind point names in `?` position order.
            // Build per-position binder functions from prepared statement
            // metadata. Each binder knows the CQL column type and applies
            // the correct conversion from GK Value. Built once per prepare,
            // applied per row with zero branching on type.
            // Build per-position type-aware binders from prepared metadata.
            // Query the CQL column type for each `?` position via the
            // FFI and create a conversion function that coerces GK values.
            let binders: Vec<Box<dyn Fn(&mut cass::Statement, usize, &nb_variates::node::Value)
                -> Result<(), cass::Error> + Send + Sync>> =
                (0..self.bind_names.len()).map(|i| {
                    let dt = prepared.parameter_data_type(i);
                    let vt = get_const_data_type_value_type(&dt);
                    make_binder(vt)
                }).collect();

            let bind_row = |row_values: &[nb_variates::node::Value]|
                -> Result<cass::Statement, ExecutionError>
            {
                let mut stmt = prepared.bind();
                let _ = stmt.set_consistency(self.consistency)
                    .map_err(|e| ExecutionError::Op(AdapterError {
                        error_name: "bind_error".into(),
                        message: format!("set consistency: {e}"),
                        retryable: false,
                    }))?;
                for (idx, value) in row_values.iter().enumerate() {
                    binders[idx](&mut stmt, idx, value)
                        .map_err(|e| ExecutionError::Op(AdapterError {
                            error_name: "bind_error".into(),
                            message: format!("bind position {idx}: {e}"),
                            retryable: false,
                        }))?;
                }
                Ok(stmt)
            };

            let row_count;
            if fields.batch_fields.is_empty() {
                // Single row from base fields — bind by position
                let stmt = bind_row(&fields.values)?;
                batch.add_statement(stmt)
                    .map_err(|e| ExecutionError::Op(AdapterError {
                        error_name: "batch_error".into(),
                        message: format!("add_statement: {e}"),
                        retryable: false,
                    }))?;
                row_count = 1;
            } else {
                // Multiple rows — each batch_fields entry has typed values
                for field_set in &fields.batch_fields {
                    let stmt = bind_row(&field_set.values)?;
                    batch.add_statement(stmt)
                        .map_err(|e| ExecutionError::Op(AdapterError {
                            error_name: "batch_error".into(),
                            message: format!("add_statement: {e}"),
                            retryable: false,
                        }))?;
                }
                row_count = fields.batch_fields.len();
            }

            let batch_start = std::time::Instant::now();

            let _result = self.session.get().execute_batch(&batch).await
                .map_err(|e| ExecutionError::Op(AdapterError {
                    error_name: "batch_error".into(),
                    message: format!("execute_batch ({row_count} statements): {e}"),
                    retryable: false,
                }))?;

            let batch_nanos = batch_start.elapsed().as_nanos() as u64;
            let per_row_nanos = batch_nanos / row_count.max(1) as u64;
            for _ in 0..row_count {
                self.rows_timer.record(per_row_nanos);
            }
            self.rows_total.fetch_add(row_count as u64, std::sync::atomic::Ordering::Relaxed);

            Ok(OpResult {
                body: None,
                captures: {
                    let mut c = std::collections::HashMap::new();
                    c.insert("rows_inserted".to_string(),
                        nb_variates::node::Value::U64(row_count as u64));
                    c
                },
                skipped: false,
            })
        })
    }
}

// =========================================================================
// CqlTimeuuid GK node: deterministic type-1-like UUID from a seed
// =========================================================================

/// GK node that generates a deterministic UUID string formatted as a CQL
/// `timeuuid` (RFC 4122 version 1 layout) from a u64 seed.
///
/// The output is always a valid UUID string; the bits are derived entirely
/// from two successive xxHash3 passes over the seed, so the same seed always
/// produces the same UUID with no external state.
///
/// Signature: `cql_timeuuid(seed: u64) -> (String)`
///
/// JIT level: P1 (eval only; string allocation prevents P2).
pub struct CqlTimeuuid {
    meta: nb_variates::node::NodeMeta,
}

impl Default for CqlTimeuuid {
    fn default() -> Self { Self::new() }
}

impl CqlTimeuuid {
    /// Create a new `CqlTimeuuid` node.
    pub fn new() -> Self {
        use nb_variates::node::{NodeMeta, Port, PortType, Slot};
        Self {
            meta: NodeMeta {
                name: "cql_timeuuid".into(),
                outs: vec![Port::new("output", PortType::Str)],
                ins: vec![Slot::Wire(Port::u64("seed"))],
            },
        }
    }
}

impl nb_variates::node::GkNode for CqlTimeuuid {
    fn meta(&self) -> &nb_variates::node::NodeMeta { &self.meta }

    /// Evaluate: derive UUID bits from two xxHash3 passes over the seed.
    ///
    /// Bit layout follows RFC 4122 §4.1:
    /// - `version` field set to `1` (time-based)
    /// - `variant` field set to `10` (RFC 4122)
    fn eval(&self, inputs: &[nb_variates::node::Value], outputs: &mut [nb_variates::node::Value]) {
        let seed = inputs[0].as_u64();
        let h1 = xxhash_rust::xxh3::xxh3_64(&seed.to_le_bytes());
        let h2 = xxhash_rust::xxh3::xxh3_64(&h1.to_le_bytes());

        // Split h1 into time fields (version 1 layout)
        let time_low   = (h1 & 0xFFFF_FFFF) as u32;
        let time_mid   = ((h1 >> 32) & 0xFFFF) as u16;
        let time_hi    = (((h1 >> 48) & 0x0FFF) as u16) | 0x1000; // version 1

        // Split h2 into clock sequence + node
        let clock_seq: u16 = ((h2 & 0x3FFF) as u16) | 0x8000;     // variant RFC 4122
        let node       = h2 >> 16 & 0xFFFF_FFFF_FFFF;             // 48-bit node

        outputs[0] = nb_variates::node::Value::Str(format!(
            "{time_low:08x}-{time_mid:04x}-{time_hi:04x}-{clock_seq:04x}-{node:012x}"
        ));
    }
}

// ---------------------------------------------------------------------------
// GK registry integration: signatures + builder + inventory submit
// ---------------------------------------------------------------------------

/// Return the static signature slice for `cql_timeuuid`.
pub fn cql_signatures() -> &'static [nb_variates::dsl::registry::FuncSig] {
    use nb_variates::dsl::registry::{Arity, FuncCategory, FuncSig, ParamSpec};
    use nb_variates::node::{Commutativity, SlotType};
    static SIGS: std::sync::OnceLock<Vec<nb_variates::dsl::registry::FuncSig>> =
        std::sync::OnceLock::new();
    SIGS.get_or_init(|| {
        vec![FuncSig {
            name: "cql_timeuuid",
            category: FuncCategory::RealData,
            outputs: 1,
            description: "deterministic CQL timeuuid from seed",
            help: "Generate a deterministic RFC 4122 version-1 UUID string suitable \
                   for CQL timeuuid columns. The same seed always produces the same UUID. \
                   Example: cql_timeuuid(hash(cycle))",
            identity: None,
            variadic_ctor: None,
            params: &[ParamSpec { name: "seed", slot_type: SlotType::Wire, required: true, example: "cycle" }],
            arity: Arity::Fixed,
            commutativity: Commutativity::Positional,
        }]
    })
}

/// Attempt to build a `cql_timeuuid` node from the registry dispatch path.
pub fn cql_build_node(
    name: &str,
    _wires: &[nb_variates::assembly::WireRef],
    _consts: &[nb_variates::dsl::ConstArg],
) -> Option<Result<Box<dyn nb_variates::node::GkNode>, String>> {
    match name {
        "cql_timeuuid" => Some(Ok(Box::new(CqlTimeuuid::new()))),
        _ => None,
    }
}

nb_variates::register_nodes!(cql_signatures, cql_build_node);

// =========================================================================
// Adapter Registration (inventory-based, link-time)
// =========================================================================

inventory::submit! {
    nb_activity::adapter::AdapterRegistration {
        names: || &["cql", "cassandra"],
        known_params: || &[
            "hosts", "host", "port", "keyspace", "consistency",
            "username", "password", "request_timeout_ms",
        ],
        create: |params| Box::pin(async move {
            let config = CqlConfig::from_params(&params)
                .map_err(|e| format!("CQL config error: {e}"))?;
            eprintln!("cassnbrs: connecting to {} (keyspace: {})",
                config.hosts,
                if config.keyspace.is_empty() { "<none>" } else { &config.keyspace });
            CqlAdapter::connect(&config).await
                .map(|a| std::sync::Arc::new(a) as std::sync::Arc<dyn nb_activity::adapter::DriverAdapter>)
                .map_err(|e| format!("CQL connection failed: {e}"))
        }),
    }
}

// =========================================================================
// Type-aware value binders
// =========================================================================

/// Extract the CQL ValueType from a ConstDataType.
///
/// The safe cassandra-cpp wrapper doesn't expose get_type() on
/// ConstDataType, so we call the C FFI directly. ConstDataType
/// is a newtype over `*const CassDataType` (with PhantomData).
fn get_const_data_type_value_type<T>(dt: &T) -> cass::ValueType {
    // ConstDataType layout: (*const _CassDataType, PhantomData)
    // We read the first pointer-sized field.
    let raw: *const cassandra_cpp_sys::CassDataType_ = unsafe {
        *(dt as *const _ as *const *const cassandra_cpp_sys::CassDataType_)
    };
    let cass_vt = unsafe { cassandra_cpp_sys::cass_data_type_type(raw) };
    // Map the C enum value to the Rust ValueType.
    // CassValueType_ values match ValueType variant ordering.
    cass_value_type_from_raw(cass_vt)
}

/// Convert a raw CassValueType_ C enum to cass::ValueType.
fn cass_value_type_from_raw(raw: cassandra_cpp_sys::CassValueType_) -> cass::ValueType {
    use cassandra_cpp_sys::CassValueType_::*;
    match raw {
        CASS_VALUE_TYPE_ASCII => cass::ValueType::ASCII,
        CASS_VALUE_TYPE_BIGINT => cass::ValueType::BIGINT,
        CASS_VALUE_TYPE_BLOB => cass::ValueType::BLOB,
        CASS_VALUE_TYPE_BOOLEAN => cass::ValueType::BOOLEAN,
        CASS_VALUE_TYPE_COUNTER => cass::ValueType::COUNTER,
        CASS_VALUE_TYPE_DOUBLE => cass::ValueType::DOUBLE,
        CASS_VALUE_TYPE_FLOAT => cass::ValueType::FLOAT,
        CASS_VALUE_TYPE_INT => cass::ValueType::INT,
        CASS_VALUE_TYPE_TEXT => cass::ValueType::TEXT,
        CASS_VALUE_TYPE_VARCHAR => cass::ValueType::VARCHAR,
        CASS_VALUE_TYPE_SMALL_INT => cass::ValueType::SMALL_INT,
        CASS_VALUE_TYPE_TINY_INT => cass::ValueType::TINY_INT,
        CASS_VALUE_TYPE_CUSTOM => cass::ValueType::CUSTOM,
        _ => cass::ValueType::UNKNOWN,
    }
}

/// Create a binder function for a given CQL column type.
///
/// The returned closure converts a GK `Value` to the correct CQL
/// type and binds it at the given position. Built once per `?`
/// position in a prepared statement; applied per row.
/// Parse a GK vector string `[0.1, 0.2, ...]` into CQL vector
/// binary encoding (big-endian IEEE 754 floats, concatenated).
fn parse_vector_to_bytes(s: &str) -> Vec<u8> {
    let trimmed = s.trim();
    if !trimmed.starts_with('[') || !trimmed.ends_with(']') {
        return Vec::new();
    }
    let inner = &trimmed[1..trimmed.len()-1];
    let mut bytes = Vec::new();
    for part in inner.split(',') {
        let part = part.trim();
        if let Ok(f) = part.parse::<f32>() {
            bytes.extend_from_slice(&f.to_be_bytes());
        } else {
            return Vec::new(); // not a float vector
        }
    }
    bytes
}

type BinderFn = Box<dyn Fn(&mut cass::Statement, usize, &nb_variates::node::Value)
    -> cass::Result<()> + Send + Sync>;

fn make_binder(cql_type: cass::ValueType) -> BinderFn {
    match cql_type {
        // String types
        cass::ValueType::ASCII | cass::ValueType::TEXT | cass::ValueType::VARCHAR => {
            Box::new(|stmt, idx, value| {
                stmt.bind_string(idx, &value.to_display_string())?; Ok(())
            })
        }
        // 32-bit integer types
        cass::ValueType::INT | cass::ValueType::SMALL_INT | cass::ValueType::TINY_INT => {
            Box::new(|stmt, idx, value| {
                let n = match value {
                    nb_variates::node::Value::U64(v) => *v as i32,
                    nb_variates::node::Value::F64(v) => *v as i32,
                    nb_variates::node::Value::Str(s) => s.parse::<i32>().unwrap_or(0),
                    _ => 0,
                };
                stmt.bind_int32(idx, n)?; Ok(())
            })
        }
        // 64-bit integer types
        cass::ValueType::BIGINT | cass::ValueType::COUNTER => {
            Box::new(|stmt, idx, value| {
                let n = match value {
                    nb_variates::node::Value::U64(v) => *v as i64,
                    nb_variates::node::Value::F64(v) => *v as i64,
                    nb_variates::node::Value::Str(s) => s.parse::<i64>().unwrap_or(0),
                    _ => 0,
                };
                stmt.bind_int64(idx, n)?; Ok(())
            })
        }
        // Float
        cass::ValueType::FLOAT => {
            Box::new(|stmt, idx, value| {
                let f = match value {
                    nb_variates::node::Value::F64(v) => *v as f32,
                    nb_variates::node::Value::U64(v) => *v as f32,
                    nb_variates::node::Value::Str(s) => s.parse::<f32>().unwrap_or(0.0),
                    _ => 0.0,
                };
                stmt.bind_float(idx, f)?; Ok(())
            })
        }
        // Double
        cass::ValueType::DOUBLE => {
            Box::new(|stmt, idx, value| {
                let f = match value {
                    nb_variates::node::Value::F64(v) => *v,
                    nb_variates::node::Value::U64(v) => *v as f64,
                    nb_variates::node::Value::Str(s) => s.parse::<f64>().unwrap_or(0.0),
                    _ => 0.0,
                };
                stmt.bind_double(idx, f)?; Ok(())
            })
        }
        // Boolean
        cass::ValueType::BOOLEAN => {
            Box::new(|stmt, idx, value| {
                let b = match value {
                    nb_variates::node::Value::Bool(v) => *v,
                    nb_variates::node::Value::U64(v) => *v != 0,
                    nb_variates::node::Value::Str(s) => s == "true" || s == "1",
                    _ => false,
                };
                stmt.bind_bool(idx, b)?; Ok(())
            })
        }
        // CUSTOM type includes vectors. Parse the GK string
        // representation `[0.1, 0.2, ...]` into raw float bytes
        // (big-endian IEEE 754, concatenated) for CQL binding.
        cass::ValueType::CUSTOM => {
            Box::new(|stmt, idx, value| {
                let s = value.to_display_string();
                let bytes = parse_vector_to_bytes(&s);
                if bytes.is_empty() {
                    // Not a vector — fall back to string binding
                    stmt.bind_string(idx, &s)?;
                } else {
                    stmt.bind_bytes(idx, bytes)?;
                }
                Ok(())
            })
        }
        // Everything else: bind as string
        _ => {
            Box::new(|stmt, idx, value| {
                stmt.bind_string(idx, &value.to_display_string())?; Ok(())
            })
        }
    }
}
