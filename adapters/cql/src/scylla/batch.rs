// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Batch dispenser.
//!
//! Used for `prepared:` / `stmt:` ops with a `batch:` row cap and/or
//! a `max_batch_size:` byte budget (SRD-103 §6). Prepares the
//! statement once and caches it; on each `execute` call it pulls a
//! deterministic number of rows (advancing the per-fiber wire coord
//! per row, SRD-68) and packs them into one or more CQL `Batch`
//! round-trips.
//!
//! Fill strategy (see [`crate::common::size_estimator`]):
//! - `batch: N` alone → exactly `N` rows in one batch (today's model).
//! - `max_batch_size` alone → a byte-budgeted batch whose row count
//!   is predicted from the first row's estimated encoded size.
//! - both → `N` rows total, split into sub-batches each held under
//!   the byte budget; whichever limit hits first flushes.
//! - neither → a single row.
//!
//! One op invocation may therefore emit multiple batch round-trips;
//! the work is summed into a single [`OpResult`] and a failure on any
//! sub-batch fails the whole op.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use nbrs_runtime::adapter::{ExecutionError, OpDispenser, OpResult, ResultBody};
use polydat::ast::Value;
use scylla::client::session::Session;
use scylla::statement::{Consistency, batch::{Batch, BatchType}, prepared::PreparedStatement};

use super::{ScyllaResultBody, binders, format_cql_error, op_error};
use crate::common::size_estimator;

pub(super) struct ScyllaBatchDispenser {
    session: Arc<Session>,
    /// Pre-prepared inner statement (consistency + modifiers
    /// already applied at `map_op` time). Same dispenser-init
    /// contract as [`super::prepared::ScyllaPreparedDispenser`].
    prepared: Arc<PreparedStatement>,
    /// Statement text — retained for error diagnostics only.
    stmt_text: String,
    /// Bind-point names in `?` order. Each row's values come from
    /// `wires.get(name)` after `wires.advance(coord)` per row
    /// (SRD-68 invariant: "each iteration of the batch is
    /// considered another pull").
    bind_names: Vec<String>,
    /// Raw `batch: N` row cap. `0` → unset (the byte budget drives
    /// the row count).
    batch_n: usize,
    /// SRD-103 §6 byte budget (`max_batch_size`, a literal
    /// magnitude). `None` → no byte cap.
    max_batch_bytes: Option<u64>,
    batch_type: BatchType,
    /// Batch-level consistency — applied to the [`Batch`] itself
    /// per execute. Distinct from the consistency baked into the
    /// inner prepared statement: scylla's batch wrapper has its
    /// own consistency setter that overrides the per-statement
    /// value when both are present.
    consistency: Consistency,
    /// Fires the "single row exceeds max_batch_size" warning at most
    /// once per dispenser lifetime.
    oversize_warned: AtomicBool,
}

impl ScyllaBatchDispenser {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        session: Arc<Session>,
        prepared: Arc<PreparedStatement>,
        stmt_text: String,
        bind_names: Vec<String>,
        batch_n: usize,
        max_batch_bytes: Option<u64>,
        batch_type: BatchType,
        consistency: Consistency,
    ) -> Self {
        Self {
            session,
            prepared,
            stmt_text,
            bind_names,
            batch_n,
            max_batch_bytes,
            batch_type,
            consistency,
            oversize_warned: AtomicBool::new(false),
        }
    }

    /// Execute one CQL `Batch` over the given row-value sets. Returns
    /// the query result body when it carries rows (e.g. the
    /// `[applied]` status of a conditional/LWT batch), else `None`.
    /// Shared by every sub-batch flush.
    async fn flush_sub_batch(
        &self,
        row_value_sets: &[Vec<Value>],
    ) -> Result<Option<Box<dyn ResultBody>>, ExecutionError> {
        let col_specs = self.prepared.get_variable_col_specs();
        let mut rows: Vec<Vec<binders::NbrsCell<'_>>> = Vec::with_capacity(row_value_sets.len());
        for values in row_value_sets {
            rows.push(binders::build_row(col_specs, values)
                .map_err(|e| op_error("bind_error", e, false))?);
        }
        let row_count = rows.len();

        let mut batch = Batch::new(self.batch_type);
        batch.set_consistency(self.consistency);
        for _ in 0..row_count {
            batch.append_statement((*self.prepared).clone());
        }

        let result = self.session.batch(&batch, rows).await
            .map_err(|e| op_error(
                "cql_error",
                format!(
                    "batch ({row_count} rows): {}",
                    format_cql_error(&e.to_string(), &self.stmt_text),
                ),
                crate::common::cql_error_is_retryable(&e.to_string()),
            ))?;

        let body = ScyllaResultBody::from_query_result(result);
        if body.element_count() > 0 {
            Ok(Some(Box::new(body)))
        } else {
            Ok(None)
        }
    }
}

impl OpDispenser for ScyllaBatchDispenser {
    fn execute<'a>(
        &'a self,
        cycle: u64,
        ctx: &'a nbrs_runtime::adapter::ExecCtx<'a>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<OpResult, ExecutionError>> + Send + 'a>> {
        let wires = ctx.wires;
        Box::pin(async move {
            // SRD-68 batch contract: each iteration of the batch is
            // another pull. Advance the per-fiber wire coord and
            // re-read bind values per row. Row 0 doubles as the size
            // sample used to plan the total row count for a purely
            // byte-budgeted op (SRD-103 §6).
            let gather_row = |coord: u64| -> Vec<Value> {
                wires.advance(coord);
                self.bind_names.iter()
                    .map(|n| wires.get(n).unwrap_or(Value::Str(String::new().into())))
                    .collect()
            };

            let row0 = gather_row(cycle);
            let first_bytes = size_estimator::estimate_row_size(&row0);
            let total = size_estimator::plan_total_rows(
                first_bytes, self.batch_n, self.max_batch_bytes);

            // Materialize all row-value sets up front so any
            // borrowed-slice NbrsCells stay valid through each
            // `batch()` call.
            let mut all_rows: Vec<Vec<Value>> = Vec::with_capacity(total);
            all_rows.push(row0);
            for row_idx in 1..total {
                all_rows.push(gather_row(cycle + row_idx as u64));
            }

            // Fill-to-budget: accumulate rows into the current
            // sub-batch, flushing before a row that would push the
            // estimated size over `max_batch_bytes`. Always ≥1 row
            // per sub-batch; `batch: N` (= `total` here) bounds the
            // overall pull. Sum row work into one op result.
            let mut submitted: usize = 0;
            let mut last_body: Option<Box<dyn ResultBody>> = None;
            let mut cur: Vec<Vec<Value>> = Vec::new();
            let mut cur_bytes: u64 = 0;
            for values in all_rows {
                let row_bytes = size_estimator::estimate_row_size(&values);
                if let Some(budget) = self.max_batch_bytes {
                    if !cur.is_empty() && cur_bytes + row_bytes > budget {
                        if let Some(b) = self.flush_sub_batch(&cur).await? {
                            last_body = Some(b);
                        }
                        submitted += cur.len();
                        cur.clear();
                        cur_bytes = 0;
                    }
                    if cur.is_empty() && row_bytes > budget
                        && !self.oversize_warned.swap(true, Ordering::Relaxed)
                    {
                        nbrs_runtime::diag!(
                            nbrs_runtime::observer::LogLevel::Warn,
                            "cql batch: a single row (~{row_bytes} B) exceeds \
                             max_batch_size ({budget} B); sending it as a \
                             one-row batch — the server may still reject it",
                        );
                    }
                }
                cur.push(values);
                cur_bytes += row_bytes;
            }
            if !cur.is_empty() {
                if let Some(b) = self.flush_sub_batch(&cur).await? {
                    last_body = Some(b);
                }
                submitted += cur.len();
            }

            // Mirror nbrs batch dispenser's `rows_inserted`
            // capture — drives the `rows/s` status metric. Lands on
            // the per-fiber kernel via ctx.wires.write.
            let _ = ctx.wires.write(
                "rows_inserted",
                polydat::ast::Value::U64(submitted as u64),
            );
            Ok(OpResult {
                body: last_body,
                skipped: false,
            })
        })
    }
}
