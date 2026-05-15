// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Prepared-statement dispenser.
//!
//! Used for `prepared:` / `stmt:` op fields without a `batch:`
//! param. The statement is prepared once on first execute and
//! cached in a `OnceLock<Arc<PreparedStatement>>`. Each
//! subsequent execute binds the resolved fields against the
//! cached prepared statement and runs `execute_unpaged`.

use std::sync::Arc;

use nbrs_activity::adapter::{ExecutionError, OpDispenser, OpResult, ResultBody};
use nbrs_variates::node::Value;
use scylla::client::session::Session;
use scylla::statement::{Consistency, prepared::PreparedStatement};

use super::{ScyllaResultBody, binders, format_cql_error, op_error, truncate_stmt};

pub(super) struct ScyllaPreparedDispenser {
    session: Arc<Session>,
    consistency: Consistency,
    stmt_text: String,
    /// Bind-point names in `?` order, captured from the statement
    /// text at map-op time. `ResolvedFields` carries every op
    /// field (including the stmt text itself); we look up each
    /// bind position by name so non-bind fields stay off the wire.
    bind_names: Vec<String>,
    prepared: std::sync::OnceLock<Arc<PreparedStatement>>,
}

impl ScyllaPreparedDispenser {
    pub fn new(
        session: Arc<Session>,
        consistency: Consistency,
        stmt_text: String,
        bind_names: Vec<String>,
    ) -> Self {
        Self {
            session,
            consistency,
            stmt_text,
            bind_names,
            prepared: std::sync::OnceLock::new(),
        }
    }

    async fn get_prepared(&self) -> Result<Arc<PreparedStatement>, ExecutionError> {
        if let Some(p) = self.prepared.get() {
            return Ok(p.clone());
        }
        let mut prep = self.session.prepare(self.stmt_text.clone()).await
            .map_err(|e| op_error(
                "prepare_error",
                format!("prepare '{}': {e}", truncate_stmt(&self.stmt_text)),
                false,
            ))?;
        prep.set_consistency(self.consistency);
        let arc = Arc::new(prep);
        let _ = self.prepared.set(arc.clone());
        Ok(self.prepared.get().unwrap().clone())
    }
}

impl OpDispenser for ScyllaPreparedDispenser {
    fn execute<'a>(
        &'a self,
        _cycle: u64,
        ctx: &'a nbrs_activity::adapter::ExecCtx<'a>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<OpResult, ExecutionError>> + Send + 'a>> {
        let wires = ctx.wires;
        Box::pin(async move {
            let prepared = self.get_prepared().await?;

            // SRD-68 Push 5: pull values by bind-point name in `?`
            // order through the generic wires API. Empty string is
            // the legacy fallback for an unresolved bind name; the
            // GK compiler should have provisioned every name, but
            // an absent name shouldn't fail-stop the cycle.
            let bind_values: Vec<Value> = self.bind_names.iter()
                .map(|name| wires.get(name).unwrap_or(Value::Str(String::new())))
                .collect();
            let col_specs = prepared.get_variable_col_specs();
            let row = binders::build_row(col_specs, &bind_values)
                .map_err(|e| op_error("bind_error", e, false))?;

            let result = self.session.execute_unpaged(&prepared, row).await
                .map_err(|e| op_error(
                    "cql_error",
                    format_cql_error(&e.to_string(), &self.stmt_text),
                    false,
                ))?;

            let body = ScyllaResultBody::from_query_result(result);
            let body_box: Option<Box<dyn ResultBody>> = if body.element_count() > 0 {
                Some(Box::new(body))
            } else {
                None
            };
            Ok(OpResult {
                body: body_box,
                skipped: false,
            })
        })
    }
}
