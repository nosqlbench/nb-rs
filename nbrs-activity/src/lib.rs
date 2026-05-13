// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! # nbrs-activity
//!
//! Activity execution engine for nb-rs. Owns the async dispatch
//! loop, the adapter trait that workload backends implement, op
//! sequencing across stanzas, error-handler integration, observer
//! callbacks, and the runner that ties everything together.
//!
//! This is the integration crate — it depends on every other
//! `nb-*` library and is depended on by `nbrs` (and by every
//! persona binary). External consumers shouldn't usually need to
//! reach into it directly; the public-facing path is `nbrs run`
//! or [`runner::Runner`].
//!
//! ## Pieces
//!
//! - [`adapter::DriverAdapter`] — the trait every workload
//!   backend implements. CQL, HTTP, stdout, testkit, plotter, and
//!   user-supplied adapters all register via the inventory
//!   pattern in [`adapters`].
//! - [`activity::Activity`] — one running concurrency unit. Owns
//!   the cycle source, the op sequencer, the fiber pool, the
//!   error router, and the metrics scope. Multiple activities
//!   can run concurrently within one phase.
//! - [`runner::Runner`] — orchestrates the whole session: parse
//!   workload, build component tree, route metrics, walk the
//!   scenario tree, supervise activities.
//! - [`scope_tree`] / [`scene_tree`] — the canonical scenario-
//!   tree shape and the runtime presentation surface (SRD 18b).
//! - [`scheduler`] — `schedule=` CLI param parses to a
//!   [`scheduler::ScheduleSpec`]; the [`scheduler::TreeScheduler`]
//!   walks the tree and forks concurrent siblings via
//!   `tokio::JoinSet` + `Semaphore` based on per-level limits.
//! - [`observer::RunObserver`] — lifecycle callbacks (phase
//!   start / progress / complete / fail). The TUI is one
//!   implementor; stderr is the default.
//! - [`bindings`] / [`scope`] — workload bindings → GK kernel
//!   compilation, with cache-and-rebind across phase iterations.
//!
//! ## Out of scope
//!
//! - GK DSL parsing and compilation: see [`nbrs_variates`].
//! - Workload YAML parsing: see [`nbrs_workload`].
//! - Component tree, instruments, cadence reporter: see
//!   [`nbrs_metrics`].
//! - Rate limiting: see [`nbrs_rate`].
//! - Error handler primitives: see [`nbrs_errorhandler`].
//!
//! ## See also
//!
//! - SRD 01 (`docs/sysref/01_system_overview.md`) — overall
//!   architecture.
//! - SRD 18b — scenario-tree, scope-tree, scheduler.
//! - SRD 22 (`docs/sysref/22_op_sequencing.md`) — op sequencing
//!   and stanza model.
//! - SRD 30 (`docs/sysref/30_adapter_interface.md`) — adapter
//!   trait surface.

pub mod cycle;
pub mod wires;
pub mod binder;
pub mod adapter;
pub mod opseq;
pub mod activity;
pub mod adapters;
pub mod synthesis;
pub mod bindings;
pub mod params;
pub mod scope;
pub mod scope_tree;
pub mod scope_flattening;
pub mod scene_tree;
pub mod checkpoint;
pub mod scheduler;
pub mod profiler;
pub mod session_signals;
pub mod observer;
pub mod trace_router;
pub mod session;
pub mod runner;
pub mod executor;
pub mod resource_pool;
pub mod describe;
pub mod wrappers;
pub mod wrapper_registry;
pub mod wrapper_registrations;
pub mod wrapper_resolver;
pub mod relevancy;
pub mod fixture;
pub mod validation;
pub mod linearize;
pub mod fiber_pool;
pub mod log_sink;
pub mod readouts;
pub mod readout_context;
pub mod report_anchor;
