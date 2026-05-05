// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! # nbrs-metrics
//!
//! Metrics collection and reporting for nb-rs. The design centers
//! on a **component tree** — a hierarchical, label-keyed structure
//! where every node owns its own instruments, controls, and child
//! components. Workload state, phase progress, latency histograms,
//! and dynamic-control values are all reified as components and
//! their attached instruments, queryable via [`selector::Selector`]
//! or directly via parent / child traversal.
//!
//! ## Pieces
//!
//! - **Components** ([`component::Component`]) hold dimensional
//!   labels (`session=…`, `phase=…`, `activity=…`), per-component
//!   props, an instrument set, and a control registry. Each
//!   component's effective labels are the union of its own labels
//!   and its parent chain.
//! - **Instruments** ([`instruments`]) are counters, gauges,
//!   histograms, timers — owned by a component and accessed by
//!   name. Histograms are HDR-backed; reads return delta windows
//!   so downstream reporters get *what changed since last read*,
//!   not cumulative state.
//! - **Snapshots** ([`snapshot::MetricSet`]) are OpenMetrics-shaped
//!   captures of one moment in time across the tree. The
//!   [`cadence_reporter::CadenceReporter`] coalesces snapshots
//!   into declared windows (1s, 10s, 30s, 1m, 5m, …) and fans
//!   them out to async subscribers (SQLite, VictoriaMetrics push,
//!   TUI).
//! - **Controls** ([`controls`]) live next to instruments — same
//!   tree, same label addressing — but carry mutable typed values
//!   that GK kernels read at cycle time and the runtime applies
//!   via `ControlApplier`s. See SRD 23 for the full surface.
//! - **MetricsQuery** ([`metrics_query::MetricsQuery`]) is the
//!   read-side handle that consumers (TUI, web, summary reports)
//!   use to pull cadence-window snapshots without touching the
//!   shared store directly.
//!
//! ## Quick tour
//!
//! Build a root component, then walk it:
//!
//! ```
//! use std::collections::HashMap;
//! use nbrs_metrics::component::{self, Component};
//! use nbrs_metrics::labels::Labels;
//! use nbrs_metrics::selector::Selector;
//!
//! let root = Component::root(
//!     Labels::of("session", "demo"),
//!     HashMap::new(),
//! );
//!
//! // Effective labels include the chain back to the root.
//! let guard = root.read().unwrap();
//! assert_eq!(guard.effective_labels().get("session"), Some("demo"));
//! drop(guard);
//!
//! // Empty selector matches the root only on a fresh tree.
//! let hits = component::find(&root, &Selector::new());
//! assert_eq!(hits.len(), 1);
//! ```
//!
//! ## Design briefs
//!
//! See the SRD for the full rationale:
//!
//! - SRD 19 — component tree
//! - SRD 23 — dynamic controls
//! - SRD 24 — selector lookup semantics
//! - SRD 40 / 42 — metrics capture, cadence reporter,
//!   subscription dispatch

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
pub mod validation;
pub mod cadence;
pub mod cadence_reporter;
pub mod metrics_query;
