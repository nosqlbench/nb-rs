// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Raw / unprepared statement dispenser.
//!
//! Used for `raw:` / `simple:` op fields. The statement text is
//! resolved at cycle time (bind points already substituted by
//! the runtime) and handed straight to `session.query_unpaged`.
//! Skips the prepared-statement cache; appropriate for one-shot
//! DDL phases (`CREATE KEYSPACE`, `DROP TABLE`, …) and
//! diagnostic runs where the per-prepare cost would dominate.

use std::sync::Arc;

use nbrs_activity::adapter::{ExecutionError, OpDispenser, OpResult, ResultBody};
use scylla::client::session::Session;
use scylla::statement::{Consistency, Statement};

use super::{ScyllaResultBody, format_cql_error, op_error};

pub(super) struct ScyllaRawDispenser {
    session: Arc<Session>,
    consistency: Consistency,
    /// Original statement template, with `{name}` placeholders
    /// intact. Rendered at cycle time through the generic GK
    /// wires API (SRD-68 Push 5).
    stmt_template: String,
}

impl ScyllaRawDispenser {
    pub fn new(session: Arc<Session>, consistency: Consistency, stmt_template: String) -> Self {
        Self { session, consistency, stmt_template }
    }
}

impl OpDispenser for ScyllaRawDispenser {
    fn execute<'a>(
        &'a self,
        _cycle: u64,
        ctx: &'a nbrs_activity::adapter::ExecCtx<'a>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<OpResult, ExecutionError>> + Send + 'a>> {
        let wires = ctx.wires;
        Box::pin(async move {
            let text = nbrs_activity::wires::substitute_via_wires(&self.stmt_template, wires)
                .map_err(|e| op_error("bind_error", e, false))?;

            let mut stmt = Statement::new(text.clone());
            stmt.set_consistency(self.consistency);

            let result = self.session.query_unpaged(stmt, ()).await
                .map_err(|e| op_error(
                    "cql_error",
                    format_cql_error(&e.to_string(), &text),
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
