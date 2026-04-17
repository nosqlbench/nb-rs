// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! # nb-tui
//!
//! Terminal UI for nb-rs using ratatui. Provides a live dashboard
//! showing phase progress, throughput, latency percentiles, scenario
//! tree, and adapter-specific metrics during workload execution.

pub mod app;
pub mod state;
pub mod widgets;
pub mod reporter;
