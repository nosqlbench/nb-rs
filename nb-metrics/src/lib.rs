// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! # nb-metrics
//!
//! Metrics collection and reporting for nb-rs.
//! Frame-based capture with delta HDR histograms,
//! hierarchical coalescing, and concurrent reporters.

pub mod labels;
pub mod instruments;
pub mod summaries;
pub mod selector;
pub mod controls;
pub mod scheduler;
pub mod reporters;
pub mod component;
pub mod diag;
pub mod snapshot;
pub mod cadence;
pub mod cadence_reporter;
pub mod metrics_query;
