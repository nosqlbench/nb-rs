// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! Adapter trait: the interface that database/protocol drivers implement.

use std::collections::HashMap;
use std::fmt;

/// An assembled operation ready for execution.
///
/// Built from an op template + variate values. The adapter receives
/// this and translates it into a protocol-specific request.
#[derive(Debug, Clone)]
pub struct AssembledOp {
    /// The op name (from the op template).
    pub name: String,
    /// Resolved op fields: all bind points replaced with concrete values.
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
