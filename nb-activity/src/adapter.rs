// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! Adapter trait: the interface that database/protocol drivers implement.

use std::collections::HashMap;
use std::fmt;

/// An assembled operation ready for execution.
///
/// Built from an op template + variate values. The adapter receives
/// this and translates it into a protocol-specific request.
///
/// Fields carry typed `Value`s — adapters read the types they need:
/// - Stdout/model call `value.to_display_string()` for text output
/// - HTTP calls `value.to_display_string()` or `value.to_json_value()`
/// - CQL downcasts `Value::Ext` to native types via `as_any()`
#[derive(Debug, Clone)]
pub struct AssembledOp {
    /// The op name (from the op template).
    pub name: String,
    /// Resolved op fields with typed values.
    pub typed_fields: HashMap<String, nb_variates::node::Value>,
    /// Resolved op fields as strings (for backward-compatible adapters).
    /// Derived from typed_fields via `to_display_string()`.
    pub fields: HashMap<String, String>,
}

/// The result of executing an operation.
#[derive(Debug)]
pub struct OpResult {
    /// Whether the operation succeeded.
    pub success: bool,
    /// Status code (HTTP status, CQL error code, etc.)
    pub status: i32,
    /// Optional response body or message.
    pub body: Option<String>,
}

/// Error from an adapter operation.
#[derive(Debug)]
pub struct AdapterError {
    /// Error classification name (for error handler routing).
    pub error_name: String,
    /// Human-readable error message.
    pub message: String,
}

impl fmt::Display for AdapterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}] {}", self.error_name, self.message)
    }
}

impl std::error::Error for AdapterError {}

/// The async adapter trait. Protocol drivers implement this.
///
/// For now, designed around HTTP. The associated types allow future
/// adapters to use protocol-specific op and result types.
pub trait Adapter: Send + Sync + 'static {
    /// Execute an assembled operation.
    fn execute(&self, op: &AssembledOp) -> impl std::future::Future<Output = Result<OpResult, AdapterError>> + Send;
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

/// Trait for adapters that can extract captured values from results.
///
/// Adapters that support capture points implement this in addition to
/// `Adapter`. The executor checks for this trait after each operation
/// and extracts captured values into the stanza's `CaptureContext`.
pub trait CaptureExtractor: Send + Sync {
    /// Extract named values from an operation result.
    ///
    /// Called after successful op execution when the op template
    /// declares capture points. Returns a map of capture_name → value.
    fn extract_captures(
        &self,
        result: &OpResult,
        captures: &[CaptureDecl],
    ) -> Result<HashMap<String, nb_variates::node::Value>, AdapterError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A no-op adapter for testing.
    struct NoopAdapter;

    impl Adapter for NoopAdapter {
        async fn execute(&self, _op: &AssembledOp) -> Result<OpResult, AdapterError> {
            Ok(OpResult { success: true, status: 200, body: None })
        }
    }

    #[tokio::test]
    async fn noop_adapter_works() {
        let adapter = NoopAdapter;
        let op = AssembledOp {
            name: "test".into(),
            fields: HashMap::new(),
        };
        let result = adapter.execute(&op).await.unwrap();
        assert!(result.success);
        assert_eq!(result.status, 200);
    }
}
