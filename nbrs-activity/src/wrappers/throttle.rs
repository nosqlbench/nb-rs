// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Per-cycle delay wrapper. Reads a delay value through the
//! cycle's pull plan and sleeps before delegating to the inner
//! op. u64 → nanoseconds; f64 → milliseconds.

use std::sync::Arc;

use crate::adapter::{ExecutionError, OpDispenser, OpResult};
use crate::adapter::WrappingDispenser;
use crate::wrapper_registry::{WrapperName, WrapperRegistration};
use nbrs_workload::model::ParsedOp;

/// SRD-32a wrapper name.
pub const NAME: WrapperName = WrapperName::new("throttle");

/// Trigger: an op declares a per-cycle `delay:` GK binding name.
fn triggers(template: &ParsedOp) -> bool {
    template.delay.is_some()
}

/// One-line assignment summary for init-time diagnostics.
fn describe_assignment(template: &ParsedOp) -> Option<String> {
    template.delay.as_ref().map(|name| {
        let trimmed = crate::wrapper_registrations::trim_braces(name);
        format!("throttle: delay binding `{trimmed}`")
    })
}

inventory::submit! {
    WrapperRegistration {
        name: NAME,
        // `delay` is the field carried at the top level of
        // ParsedOp; the legacy aliases `rate` / `rate_limiter`
        // are documented in the SRD but never landed as
        // first-class fields. Listing the actual storage key
        // keeps parse-time validation honest.
        owned_fields: &["delay"],
        triggers,
        requires_inner: &[super::traversing::NAME],
        forbids_outer: &[],
        mutually_exclusive_with: &[],
        describe_assignment,
    }
}

/// Wraps an inner OpDispenser with a per-cycle delay.
///
/// Reads a delay value via `PullHandle` from the cycle's
/// `ResolvedPulls`. u64 values are interpreted as nanoseconds;
/// f64 values are interpreted as milliseconds. The delay is
/// invisible to the inner adapter — it's never in `ResolvedFields`.
pub struct ThrottleDispenser {
    inner: Arc<dyn OpDispenser>,
    /// Memoized handle for the delay GK name registered into the
    /// scope fixture at init.
    delay_handle: crate::fixture::PullHandle,
}

impl ThrottleDispenser {
    /// Wrap an inner dispenser with a per-cycle delay, registering
    /// `delay_field` into the supplied scope fixture so the
    /// per-cycle read goes through the canonical PullPlan path
    /// (SRD 32 §"Init-Time Fixture and Consumer Self-Registration").
    ///
    /// Errors if the kernel doesn't know `delay_field`.
    pub fn wrap(
        inner: Arc<dyn OpDispenser>,
        delay_field: &str,
        fx: &mut crate::fixture::ScopeFixture,
    ) -> Result<Arc<dyn OpDispenser>, String> {
        let delay_handle = fx.register_pull(delay_field).map_err(|e| {
            format!("throttle `delay`: {e}")
        })?;
        Ok(Arc::new(Self { inner, delay_handle }))
    }
}

impl WrappingDispenser for ThrottleDispenser {}

impl OpDispenser for ThrottleDispenser {
    fn inner_dispenser(&self) -> Option<&dyn OpDispenser> { Some(self.inner.as_ref()) }
    fn execute<'a>(
        &'a self,
        cycle: u64,
        ctx: &'a crate::fixture::ExecCtx<'a>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<OpResult, ExecutionError>> + Send + 'a>> {
        Box::pin(async move {
            let value = ctx.pulls.get(self.delay_handle);
            let nanos = match value {
                polydat::ast::Value::U64(ns) => *ns,
                polydat::ast::Value::F64(ms) => (*ms * 1_000_000.0) as u64,
                _ => 0,
            };
            if nanos > 0 {
                tokio::time::sleep(std::time::Duration::from_nanos(nanos)).await;
            }
            self.inner.execute(cycle, ctx).await
        })
    }
}
