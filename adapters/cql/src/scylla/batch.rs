// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Batch dispenser.
//!
//! Used for `prepared:` / `stmt:` ops with a `batch:` param.
//! Prepares the statement once and caches it; on each `execute`
//! call, reads `fields.batch_fields` (one entry per row) and
//! packs them into a single `Batch`. When `batch_fields` is
//! empty (single-row mode), falls back to one statement bound
//! from `fields.values`.
//!
//! Mirrors the cassandra-cpp adapter's batch model — the
//! `OpDispenser` trait has no separate `execute_batch`; batching
//! is selected by the presence of multiple rows in
//! `ResolvedFields.batch_fields`.

use std::sync::Arc;

use nbrs_activity::adapter::{ExecutionError, OpDispenser, OpResult, ResultBody};
use nbrs_variates::node::Value;
use scylla::client::session::Session;
use scylla::statement::{Consistency, batch::{Batch, BatchType}, prepared::PreparedStatement};

use super::{ScyllaResultBody, binders, format_cql_error, op_error, truncate_stmt};

pub(super) struct ScyllaBatchDispenser {
    session: Arc<Session>,
    consistency: Consistency,
    stmt_text: String,
    /// Bind-point names in `?` order. Each row's values come from
    /// `wires.get(name)` after `wires.advance(coord)` per row
    /// (SRD-68 invariant: "each iteration of the batch is
    /// considered another pull").
    bind_names: Vec<String>,
    /// Batch row count from `batch: N` op param. Mirrors the
    /// cassandra-cpp adapter: 0 → single-row fallback.
    batch_size: usize,
    batch_type: BatchType,
    prepared: std::sync::OnceLock<Arc<PreparedStatement>>,
}

impl ScyllaBatchDispenser {
    pub fn new(
        session: Arc<Session>,
        consistency: Consistency,
        stmt_text: String,
        bind_names: Vec<String>,
        batch_size: usize,
        batch_type: BatchType,
    ) -> Self {
        Self {
            session,
            consistency,
            stmt_text,
            bind_names,
            batch_size: if batch_size == 0 { 1 } else { batch_size },
            batch_type,
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

impl OpDispenser for ScyllaBatchDispenser {
    fn execute<'a>(
        &'a self,
        cycle: u64,
        ctx: &'a nbrs_activity::adapter::ExecCtx<'a>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<OpResult, ExecutionError>> + Send + 'a>> {
        let wires = ctx.wires;
        Box::pin(async move {
            let prepared = self.get_prepared().await?;
            let col_specs = prepared.get_variable_col_specs();

            // SRD-68 batch contract: each iteration of the batch is
            // another pull. Advance the per-fiber wire coord and
            // re-read bind values per row; build one row per cycle
            // step. Materialize all bind values up front so any
            // borrowed-slice NbrsCells stay valid through `batch()`.
            let mut row_value_sets: Vec<Vec<Value>> = Vec::with_capacity(self.batch_size);
            for row_idx in 0..self.batch_size {
                let row_coord = cycle + row_idx as u64;
                wires.advance(row_coord);
                let row_values: Vec<Value> = self.bind_names.iter()
                    .map(|n| wires.get(n).unwrap_or(Value::Str(String::new().into())))
                    .collect();
                row_value_sets.push(row_values);
            }
            let mut rows: Vec<Vec<binders::NbrsCell<'_>>> = Vec::with_capacity(self.batch_size);
            for values in &row_value_sets {
                rows.push(binders::build_row(col_specs, values)
                    .map_err(|e| op_error("bind_error", e, false))?);
            }
            let row_count = rows.len();

            // Build the batch — one entry per bound row, each
            // pointing at the same prepared statement.
            let mut batch = Batch::new(self.batch_type);
            batch.set_consistency(self.consistency);
            for _ in 0..row_count {
                batch.append_statement((*prepared).clone());
            }

            let result = self.session.batch(&batch, rows).await
                .map_err(|e| op_error(
                    "cql_error",
                    format!(
                        "batch ({row_count} rows): {}",
                        format_cql_error(&e.to_string(), &self.stmt_text),
                    ),
                    false,
                ))?;

            let body = ScyllaResultBody::from_query_result(result);
            let body_box: Option<Box<dyn ResultBody>> = if body.element_count() > 0 {
                Some(Box::new(body))
            } else {
                None
            };
            // Mirror nbrs batch dispenser's `rows_inserted`
            // capture — drives the `rows/s` status metric. Lands on
            // the per-fiber kernel via ctx.wires.write.
            let _ = ctx.wires.write(
                "rows_inserted",
                nbrs_variates::node::Value::U64(row_count as u64),
            );
            Ok(OpResult {
                body: body_box,
                skipped: false,
            })
        })
    }
}
