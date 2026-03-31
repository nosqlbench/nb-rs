// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! View models for Askama templates.

use serde::Serialize;

/// A function entry for the function browser.
#[derive(Debug, Clone, Serialize)]
pub struct FunctionView {
    pub name: String,
    pub params_display: String,
    pub arity_display: String,
    pub level: String,
    pub description: String,
}

/// A grouped set of functions under a category.
pub type FunctionGroup = (String, Vec<FunctionView>);

/// A stdlib module entry for the module browser.
#[derive(Debug, Clone, Serialize)]
pub struct StdlibModuleView {
    pub name: String,
    pub signature: String,
    pub description: String,
    pub source: String,
    pub category: String,
}

/// An activity status entry for the dashboard.
#[derive(Debug, Clone, Serialize)]
pub struct ActivityView {
    pub name: String,
    pub status: String,
    pub status_class: String,
    pub cycles_done: u64,
    pub cycles_total: u64,
    pub ops_per_sec: String,
    pub error_count: u64,
}

/// Dashboard summary stats.
#[derive(Debug, Clone, Serialize)]
pub struct DashboardStats {
    pub total_cycles: String,
    pub ops_per_sec: String,
    pub p99_ms: String,
    pub error_count: String,
}

impl Default for DashboardStats {
    fn default() -> Self {
        Self {
            total_cycles: "0".into(),
            ops_per_sec: "—".into(),
            p99_ms: "—".into(),
            error_count: "0".into(),
        }
    }
}
