// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! # nb-metrics
//!
//! Metrics collection and reporting for nb-rs.
//! Frame-based capture with delta HDR histograms,
//! hierarchical coalescing, and concurrent reporters.

pub mod labels;
pub mod instruments;
pub mod frame;
pub mod scheduler;
pub mod reporters;
