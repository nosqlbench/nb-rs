// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! nb-web: Axum + htmx web dashboard for nb-rs.
//!
//! Provides a browser-based UI for monitoring running workloads,
//! browsing GK functions and stdlib modules, and interactively
//! viewing DAG visualizations.

pub mod server;
pub mod routes;
pub mod models;
pub mod ws;
pub mod graph;
