// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! # nb-tui
//!
//! Terminal UI for nb-rs using ratatui. Provides a live dashboard
//! showing metrics, progress, op throughput, error rates, and
//! latency percentiles during a running activity.

pub mod app;
pub mod widgets;
pub mod reporter;
