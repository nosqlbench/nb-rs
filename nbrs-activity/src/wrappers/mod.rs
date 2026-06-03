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
pub mod dryrun;
pub use dryrun::DryRunWrapper;
pub mod memo;
pub use memo::MemoDispenser;
pub mod delay;
pub use delay::DelayDispenser;
pub mod r#if;
pub use r#if::ConditionalDispenser;
pub mod r#while;
pub use r#while::WhileWrapper;
pub mod op_rate;
pub use op_rate::OpRateWrapper;
pub mod emit;
pub use emit::EmitDispenser;
pub mod poll;
pub use poll::{PollingDispenser, PollingMetrics};
pub mod result;
pub use result::ResultDispenser;
pub mod metrics;
pub use metrics::MetricsDispenser;
pub mod traverse;
pub use traverse::{TraversingDispenser, TraversalStats};

// All wrappers now live in their own submodules:
//   if / throttle.rs / poll / result.rs /
//   metrics.rs / emit.rs / memo.rs / dryrun / traverse.

