// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Composable op dispenser wrappers.
//!
//! Each wrapper lives in its own submodule under this directory.
//! The mod.rs holds shared traits/types + the wrappers still
//! awaiting extraction; submodules are re-exported here so the
//! existing `crate::wrappers::<Name>` import paths keep working.

// Per-wrapper modules. As each wrapper migrates out of this
// file into its own module, add a `pub mod` line + re-export.
// SRD-82/92 — the first PHASE-level wrapper. Unlike the rest it is not an
// `OpDispenser`: a phase layer wraps the phase seam (`PhaseShell::run`), so
// there is no dispenser type to re-export.
pub mod interval;
pub mod dryrun;
pub use dryrun::DryRunWrapper;
pub mod memo;
pub use memo::MemoDispenser;
pub mod gutter;
pub use gutter::GutterDispenser;
pub mod delay;
pub use delay::DelayDispenser;
pub mod r#if;
pub use r#if::ConditionalDispenser;
pub mod r#while;
pub use r#while::WhileWrapper;
pub mod op_rate;
pub use op_rate::OpRateWrapper;
pub mod fields;
pub use fields::FieldsDispenser;
pub mod poll;
pub use poll::{PollingDispenser, PollingMetrics};
pub mod readout;
pub use readout::ReadoutDispenser;
pub mod result;
pub use result::ResultDispenser;
pub mod metrics;
pub use metrics::MetricsDispenser;
pub mod traverse;
pub use traverse::{TraversingDispenser, TraversalStats};
pub mod tries;
pub use tries::TriesDispenser;
pub mod errors;
pub use errors::ErrorHandlerDispenser;

// All wrappers now live in their own submodules:
//   if / throttle.rs / poll / result.rs /
//   metrics.rs / fields.rs / memo.rs / dryrun / traverse.

