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
pub mod dry_run;
pub use dry_run::DryRunWrapper;
pub mod memo;
pub use memo::MemoDispenser;
pub mod throttle;
pub use throttle::ThrottleDispenser;
pub mod conditional;
pub use conditional::ConditionalDispenser;
pub mod emit;
pub use emit::EmitDispenser;
pub mod polling;
pub use polling::{PollingDispenser, PollingMetrics};
pub mod result;
pub use result::ResultDispenser;
pub mod metrics;
pub use metrics::MetricsDispenser;
pub mod traversing;
pub use traversing::{TraversingDispenser, TraversalStats};

// All wrappers now live in their own submodules:
//   conditional.rs / throttle.rs / polling.rs / result.rs /
//   metrics.rs / emit.rs / memo.rs / dry_run.rs / traversing.rs.

