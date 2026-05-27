// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Polydat node registrations that need nbrs-activity's runtime
//! services (component tree, controls, fiber context).
//!
//! These nodes were originally inside `polydat::nodes` but got
//! relocated here so polydat can publish standalone — they take a
//! dependency on `nbrs_metrics` (controls + component tree) that
//! polydat can't carry without dragging nbrs-metrics onto crates.io
//! first. The `inventory` crate is the registration channel: this
//! module submits `polydat::register_nodes!` invocations which
//! polydat picks up at link time without knowing where they came
//! from.

pub mod runtime_context;
