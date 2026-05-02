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
    /// Op-template field name carrying the statement text (raw /
    /// simple). Looked up in `ResolvedFields` at cycle time.
    field_name: String,
}

impl ScyllaRawDispenser {
    pub fn new(session: Arc<Session>, consistency: Consistency, field_name: String) -> Self {
        Self { session, consistency, field_name }
    }
}

impl OpDispenser for ScyllaRawDispenser {
    fn execute<'a>(
        &'a self,
        _cycle: u64,
        ctx: &'a nbrs_activity::adapter::ExecCtx<'a>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<OpResult, ExecutionError>> + Send + 'a>> {
        let fields = ctx.fields;
        Box::pin(async move {
            let text = fields.get_str(&self.field_name).ok_or_else(|| op_error(
                "missing_stmt",
                format!("op missing '{}' field at cycle time", self.field_name),
                false,
            ))?;

            let mut stmt = Statement::new(text.to_string());
            stmt.set_consistency(self.consistency);

            let result = self.session.query_unpaged(stmt, ()).await
                .map_err(|e| op_error(
                    "cql_error",
                    format_cql_error(&e.to_string(), text),
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
                captures: std::collections::HashMap::new(),
                skipped: false,
            })
        })
    }
}
