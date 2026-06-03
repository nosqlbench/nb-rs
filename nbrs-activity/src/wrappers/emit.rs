// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Emit wrapper — prints the inner op's result body (as JSON)
//! plus the op-template kernel's wire snapshot, after each
//! cycle. Adapter-agnostic; activated either by `emit: true` on
//! the op template or by the workload's external display path.

use std::sync::Arc;

use crate::adapter::{ExecutionError, OpDispenser, OpResult};
use crate::adapter::WrappingDispenser;
use crate::wrapper_registry::{WrapperName, WrapperRegistration};
use nbrs_workload::model::ParsedOp;

/// SRD-32a wrapper name.
pub const NAME: WrapperName = WrapperName::new("emit");

/// Trigger: op carries `emit: true` (bool, or string "true").
fn triggers(template: &ParsedOp) -> bool {
    template
        .params
        .get("emit")
        .map(|v| {
            v.as_bool().unwrap_or_else(|| {
                v.as_str().map(|s| s == "true").unwrap_or(false)
            })
        })
        .unwrap_or(false)
}

fn describe_assignment(_: &ParsedOp) -> Option<String> {
    Some("emit: rendered op text to stdout".into())
}

inventory::submit! {
    WrapperRegistration {
        name: NAME,
        owned_fields: &["emit"],
        triggers,
        // SRD-32a's table lists `emit.requires_inner = [result]`,
        // but the cascade composes `result` OUTSIDE `emit`
        // (innermost-first list ends `..., emit, result,
        // metrics`). Declaring `requires_inner = [result]` would
        // make `result` innermore than `emit`, which contradicts
        // the cascade and breaks the byte-identical-output test
        // bar in §"Migration".
        requires_inner: &[],
        forbids_outer: &[],
        mutually_exclusive_with: &[],
        describe_assignment,
    }
}

/// Wraps any adapter's dispenser and prints the result body to stdout
/// as JSON after each execution. Adapter-agnostic — works with CQL,
/// HTTP, stdout, or any adapter that returns a ResultBody.
///
/// Enabled by wrapping at init time when `dryrun=emit` is active
/// or when the op has `emit: true`.
pub struct EmitDispenser {
    inner: Arc<dyn OpDispenser>,
    op_name: String,
}

impl EmitDispenser {
    pub fn wrap(inner: Arc<dyn OpDispenser>, op_name: &str) -> Arc<dyn OpDispenser> {
        Arc::new(Self {
            inner,
            op_name: op_name.to_string(),
        })
    }
}

impl WrappingDispenser for EmitDispenser {}

impl OpDispenser for EmitDispenser {
    fn inner_dispenser(&self) -> Option<&dyn OpDispenser> { Some(self.inner.as_ref()) }
    fn execute<'a>(
        &'a self,
        cycle: u64,
        ctx: &'a crate::fixture::ExecCtx<'a>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<OpResult, ExecutionError>> + Send + 'a>> {
        Box::pin(async move {
            let result = self.inner.execute(cycle, ctx).await?;

            // Print result body as JSON
            if let Some(ref body) = result.body {
                let json = body.to_json();
                println!("[{}@{}] {} rows: {}",
                    self.op_name, cycle,
                    body.element_count(),
                    serde_json::to_string_pretty(&json).unwrap_or_else(|_| json.to_string()));
            } else {
                println!("[{}@{}] (no result body)", self.op_name, cycle);
            }

            // Print every wire the op-template kernel knows about
            // alongside its current value. Replaces the prior
            // result.captures dump now that captures live on the
            // kernel rather than a sidecar HashMap.
            for name in ctx.wires.names() {
                if let Some(value) = ctx.wires.get(&name) {
                    println!("  wire {name} = {}", value.to_display_string());
                }
            }

            Ok(result)
        })
    }
}
