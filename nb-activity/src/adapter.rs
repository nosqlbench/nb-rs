// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Adapter traits: the tiered interface that database/protocol drivers implement (SRD 38).
//!
//! The tiered interface separates init-time template analysis from cycle-time execution:
//! - `DriverAdapter::map_op()` — called once per template at activity startup
//! - `OpDispenser::execute()` — called per-cycle to bind values and execute

use std::sync::OnceLock;
use std::any::Any;
use std::collections::HashMap;
use std::fmt;

/// Trait for adapter-specific result bodies.
///
/// The adapter defines its own concrete result type and implements
/// this trait. Internal adapter code can downcast via `as_any()` to
/// access native types (e.g., CQL rows, HTTP response structs).
/// External consumers call `to_json()` for a universal representation.
///
/// The `element_count()` and `byte_count()` methods support result
/// traversal — the framework uses these to verify that result data
/// was fully received and to record consumption metrics.
pub trait ResultBody: Send + Sync + fmt::Debug {
    /// Serialize this result to JSON for logging, capture, verification.
    fn to_json(&self) -> serde_json::Value;

    /// Downcast support for adapter-internal use.
    fn as_any(&self) -> &dyn Any;

    /// Count of logical elements in this result (rows, records, items).
    /// Used by the result traverser for metrics. Default 1 (one result).
    fn element_count(&self) -> u64 { 1 }

    /// Size in bytes of the result payload, if known.
    /// Used by the result traverser for throughput metrics.
    fn byte_count(&self) -> Option<u64> { None }
}

/// Simple text result body.
#[derive(Debug, Clone)]
pub struct TextBody(pub String);

impl ResultBody for TextBody {
    fn to_json(&self) -> serde_json::Value {
        serde_json::Value::String(self.0.clone())
    }
    fn as_any(&self) -> &dyn Any { self }
}

/// The result of a successful operation.
///
/// If you have an `OpResult`, the operation succeeded. Failure is
/// represented by `ExecutionError`, not by a flag on the result.
/// Protocol-specific status codes (HTTP, CQL) live inside the
/// adapter's `ResultBody` implementation, not on the generic result.
pub struct OpResult {
    /// Adapter-specific response body. The adapter owns the native
    /// type; consumers call `.to_json()` for a universal view.
    /// Adapter-internal code can downcast via `.as_any()`.
    /// `None` for operations with no meaningful result (e.g., DDL).
    pub body: Option<Box<dyn ResultBody>>,
    /// Captured values from the result (populated by adapters that
    /// support capture points). Key = capture alias name.
    pub captures: HashMap<String, nb_variates::node::Value>,
    /// If true, this op was conditionally skipped (via `if:` field).
    /// The activity loop counts this as a skip, not a success or error.
    pub skipped: bool,
}

impl Default for OpResult {
    fn default() -> Self {
        Self { body: None, captures: HashMap::new(), skipped: false }
    }
}

impl OpResult {
    /// Create a skipped result (no execution, no captures).
    pub fn skipped() -> Self {
        Self { body: None, captures: HashMap::new(), skipped: true }
    }
}

impl fmt::Debug for OpResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpResult")
            .field("body", &self.body.as_ref().map(|b| b.to_json()))
            .finish()
    }
}

/// Execution error with scope delamination.
///
/// Distinguishes between per-op errors (template-specific, retryable)
/// and adapter-level errors (connection-wide, affect all ops).
#[derive(Debug)]
pub enum ExecutionError {
    /// Per-op failure: this specific operation failed. Template-specific,
    /// may be retried with the same resolved fields.
    Op(AdapterError),
    /// Adapter-level failure: the driver connection or session is
    /// degraded. Affects all ops. The activity may need to pause or stop.
    Adapter(AdapterError),
}

impl ExecutionError {
    /// Access the inner AdapterError regardless of scope.
    pub fn error(&self) -> &AdapterError {
        match self {
            ExecutionError::Op(e) | ExecutionError::Adapter(e) => e,
        }
    }

    /// Whether this is an adapter-level (connection-wide) error.
    pub fn is_adapter_level(&self) -> bool {
        matches!(self, ExecutionError::Adapter(_))
    }
}

impl fmt::Display for ExecutionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExecutionError::Op(e) => write!(f, "[op] {e}"),
            ExecutionError::Adapter(e) => write!(f, "[adapter] {e}"),
        }
    }
}

impl std::error::Error for ExecutionError {}

/// Error from an adapter operation.
#[derive(Debug)]
pub struct AdapterError {
    /// Error classification name (for error handler routing).
    pub error_name: String,
    /// Human-readable error message.
    pub message: String,
    /// Hint to the executor: is this error worth retrying?
    pub retryable: bool,
}

impl fmt::Display for AdapterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}] {}", self.error_name, self.message)
    }
}

impl std::error::Error for AdapterError {}

/// A protocol-specific driver adapter. Constructed once per activity,
/// shared across fibers via Arc.
///
/// The adapter owns the driver connection (session, client, pool) and
/// provides OpDispensers that pre-process each op template at init time.
pub trait DriverAdapter: Send + Sync + 'static {
    /// Human-readable adapter name (e.g., "cql", "http", "stdout").
    fn name(&self) -> &str;

    /// Map an op template into a dispenser. Called once per unique op
    /// template at activity startup — before any cycles execute.
    ///
    /// Init-time work: parse the op, prepare statements, pre-compute
    /// bind-point resolution, validate field names, attach metrics.
    fn map_op(&self, template: &nb_workload::model::ParsedOp)
        -> Result<Box<dyn OpDispenser>, String>;

    /// Default metric names to display on the status line for this adapter.
    ///
    /// Each entry is a metric name (matching `adapter_metrics()` labels)
    /// and a display label. Workloads can override this via a `status:`
    /// field on phases or ops. Default: empty (no adapter-specific status).
    fn default_status_metrics(&self) -> Vec<StatusMetric> { Vec::new() }
}

/// A metric to display on the activity status line.
pub struct StatusMetric {
    /// Metric name matching the `name` label in `adapter_metrics()` samples.
    pub metric_name: String,
    /// Short display label for the status line (e.g., "rows/s").
    pub display: String,
    /// How to render the value: "rate" (count/elapsed), "count", "latency".
    pub render: StatusRender,
}

/// How to render a status metric value.
pub enum StatusRender {
    /// Show as rate: count / elapsed seconds (e.g., "1.2K/s").
    Rate,
    /// Show as raw count.
    Count,
    /// Show as latency with auto-scaled units.
    Latency,
}

/// A per-template op factory. Created at init time by the adapter's
/// `map_op()`, called per-cycle to bind values and execute operations.
///
/// The dispenser captures template-specific state (prepared statement,
/// field names, bind-point indices, metrics) so the per-cycle path is
/// minimal: bind resolved values and execute.
///
/// Dispensers are shared across fibers and must be thread-safe.
pub trait OpDispenser: Send + Sync {
    /// Execute an operation for the given cycle with resolved field values.
    fn execute<'a>(
        &'a self,
        cycle: u64,
        fields: &'a ResolvedFields,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<OpResult, ExecutionError>> + Send + 'a>>;

    /// Snapshot adapter-specific metrics for inclusion in the capture frame.
    ///
    /// Called by the metrics scheduler alongside the standard activity
    /// metrics. Adapters return additional `Sample`s (timers, counters)
    /// that represent adapter-internal state (e.g., rows/s for batched CQL).
    /// These appear in the summary report.
    /// Default: no additional metrics.
    fn adapter_metrics(&self) -> Vec<nb_metrics::frame::Sample> {
        if let Some(inner) = self.inner_dispenser() {
            inner.adapter_metrics()
        } else {
            Vec::new()
        }
    }

    /// Adapter-specific status line entries (cumulative, non-destructive read).
    ///
    /// Unlike `adapter_metrics()` which snapshots delta timers, this method
    /// returns cumulative counters safe to read from the progress thread
    /// without interfering with the metrics pipeline.
    /// Returns `(display_name, cumulative_count)` pairs.
    /// Default: delegates to inner dispenser (for wrapper chains).
    fn status_counters(&self) -> Vec<(&str, u64)> {
        if let Some(inner) = self.inner_dispenser() {
            inner.status_counters()
        } else {
            Vec::new()
        }
    }

    /// Returns the inner dispenser if this is a wrapper.
    /// Used for delegating `adapter_metrics()` and `status_counters()`
    /// through wrapper chains (TraversingDispenser, etc.).
    fn inner_dispenser(&self) -> Option<&dyn OpDispenser> { None }
}

/// Resolved field values for a single cycle. Produced by the GK
/// synthesis pipeline, consumed by the OpDispenser.
///
/// Fields are indexed by name. Typed values are always available;
/// string rendering is deferred until first access to avoid wasted
/// work for adapters that bind typed values natively (e.g., CQL).
///
/// For ops with `for_each` batch expansion, `batch_fields` contains
/// the expanded field sets — one per iteration of the for_each.
/// The adapter uses these to build a batch statement. The primary
/// `names`/`values` contain the fields from the base cycle.
pub struct ResolvedFields {
    /// Field names in op template declaration order.
    pub names: Vec<String>,
    /// Typed values, parallel to `names`.
    pub values: Vec<nb_variates::node::Value>,
    /// Lazily rendered string representations, parallel to `names`.
    strings: OnceLock<Vec<String>>,
    /// Expanded field sets for batch ops (one per for_each iteration).
    /// Empty for non-batch ops.
    pub batch_fields: Vec<ResolvedFieldSet>,
}

/// A single set of resolved field values (used in batch expansion).
#[derive(Clone)]
pub struct ResolvedFieldSet {
    pub names: Vec<String>,
    pub values: Vec<nb_variates::node::Value>,
}

impl fmt::Debug for ResolvedFields {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ResolvedFields")
            .field("names", &self.names)
            .field("values", &self.values)
            .finish()
    }
}

impl Clone for ResolvedFields {
    fn clone(&self) -> Self {
        Self {
            names: self.names.clone(),
            values: self.values.clone(),
            strings: self.strings.clone(),
            batch_fields: self.batch_fields.clone(),
        }
    }
}

impl ResolvedFields {
    /// Create with names and typed values. Strings are lazily rendered.
    pub fn new(names: Vec<String>, values: Vec<nb_variates::node::Value>) -> Self {
        Self { names, values, strings: OnceLock::new(), batch_fields: Vec::new() }
    }

    /// Access the lazily-rendered string representations.
    /// Computed once on first call, then cached.
    pub fn strings(&self) -> &[String] {
        self.strings.get_or_init(|| {
            self.values.iter().map(|v| v.to_display_string()).collect()
        })
    }

    /// Get a field value by name as a string.
    pub fn get_str(&self, name: &str) -> Option<&str> {
        self.names.iter().position(|n| n == name)
            .map(|i| self.strings()[i].as_str())
    }

    /// Get a field value by name as a typed Value.
    pub fn get_value(&self, name: &str) -> Option<&nb_variates::node::Value> {
        self.names.iter().position(|n| n == name)
            .map(|i| &self.values[i])
    }

    /// Get a string by index (triggers lazy rendering if needed).
    pub fn str_at(&self, index: usize) -> &str {
        &self.strings()[index]
    }

    /// Return a copy with the named field removed.
    /// Used by `ConditionalDispenser` to strip internal fields
    /// before the adapter sees them.
    pub fn without(&self, name: &str) -> Self {
        let mut names = Vec::new();
        let mut values = Vec::new();
        for (i, n) in self.names.iter().enumerate() {
            if n != name {
                names.push(n.clone());
                values.push(self.values[i].clone());
            }
        }
        Self::new(names, values)
    }

    /// Serialize all fields to JSON for diagnostic/logging use.
    pub fn to_json(&self) -> serde_json::Value {
        let map: serde_json::Map<String, serde_json::Value> = self.names.iter()
            .zip(self.values.iter())
            .map(|(name, value)| {
                let json_val = match value {
                    nb_variates::node::Value::U64(v) => serde_json::Value::Number((*v).into()),
                    nb_variates::node::Value::F64(v) => {
                        serde_json::Number::from_f64(*v)
                            .map(serde_json::Value::Number)
                            .unwrap_or(serde_json::Value::Null)
                    }
                    nb_variates::node::Value::Bool(v) => serde_json::Value::Bool(*v),
                    _ => serde_json::Value::String(value.to_display_string()),
                };
                (name.clone(), json_val)
            })
            .collect();
        serde_json::Value::Object(map)
    }
}

/// Capture point declaration in an op template.
///
/// Parsed from `[name]`, `[source as alias]`, or `[(Type)name]` syntax.
#[derive(Debug, Clone)]
pub struct CaptureDecl {
    /// The field name in the operation result.
    pub source_name: String,
    /// The name under which the value is stored in the capture context.
    pub as_name: String,
    /// Optional type qualifier for validation.
    pub type_qualifier: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolved_fields_lazy_strings() {
        let fields = ResolvedFields::new(
            vec!["a".into(), "b".into()],
            vec![nb_variates::node::Value::U64(42), nb_variates::node::Value::F64(3.14)],
        );
        // Strings not computed yet
        assert!(fields.strings.get().is_none());
        // First access triggers rendering
        assert_eq!(fields.get_str("a"), Some("42"));
        assert!(fields.strings.get().is_some());
        assert_eq!(fields.get_str("b"), Some("3.14"));
    }

    #[test]
    fn resolved_fields_get_value() {
        let fields = ResolvedFields::new(
            vec!["x".into()],
            vec![nb_variates::node::Value::F64(3.14)],
        );
        match fields.get_value("x") {
            Some(nb_variates::node::Value::F64(v)) => assert!((v - 3.14).abs() < 1e-10),
            other => panic!("expected F64(3.14), got {other:?}"),
        }
        // get_value doesn't trigger string rendering
        assert!(fields.strings.get().is_none());
    }

    #[test]
    fn execution_error_display() {
        let op_err = ExecutionError::Op(AdapterError {
            error_name: "Timeout".into(),
            message: "timed out".into(),
            retryable: true,
        });
        assert!(format!("{op_err}").contains("[op]"));
        assert!(!op_err.is_adapter_level());

        let adapter_err = ExecutionError::Adapter(AdapterError {
            error_name: "ConnectionRefused".into(),
            message: "refused".into(),
            retryable: false,
        });
        assert!(format!("{adapter_err}").contains("[adapter]"));
        assert!(adapter_err.is_adapter_level());
    }
}

// =========================================================================
// Adapter Registration (inventory-based, link-time collection)
// =========================================================================

/// An adapter module's registration, submitted at link time via `inventory`.
///
/// Each adapter crate (nb-adapter-stdout, cassnbrs-adapter-cql, etc.)
/// submits one of these. The shared runner collects all submissions to
/// build the adapter dispatch table without any explicit adapter list.
pub struct AdapterRegistration {
    /// Driver names this adapter responds to (e.g., `&["stdout"]` or `&["cql", "cassandra"]`).
    pub names: fn() -> &'static [&'static str],
    /// Extra param names this adapter accepts (for CLI validation).
    pub known_params: fn() -> &'static [&'static str],
    /// Async factory: given params, create the adapter.
    /// Returns a boxed future so async connect is supported (e.g., CQL).
    pub create: fn(std::collections::HashMap<String, String>)
        -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<std::sync::Arc<dyn DriverAdapter>, String>> + Send>>,
}

inventory::collect!(AdapterRegistration);

/// Look up an adapter by driver name from all link-time registrations.
pub fn find_adapter_registration(driver: &str) -> Option<&'static AdapterRegistration> {
    for reg in inventory::iter::<AdapterRegistration> {
        if (reg.names)().contains(&driver) {
            return Some(reg);
        }
    }
    None
}

/// List all registered driver names.
pub fn registered_driver_names() -> Vec<&'static str> {
    let mut names = Vec::new();
    for reg in inventory::iter::<AdapterRegistration> {
        names.extend_from_slice((reg.names)());
    }
    names
}

/// Collect all extra known params from registered adapters.
pub fn registered_adapter_params() -> Vec<&'static str> {
    let mut params = Vec::new();
    for reg in inventory::iter::<AdapterRegistration> {
        params.extend_from_slice((reg.known_params)());
    }
    params
}
