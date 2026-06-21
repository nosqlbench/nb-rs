// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! # nbrs-runtime
//!
//! Workload execution runtime for nb-rs. Owns the async dispatch
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
//! - [`bindings`] / [`scope`] — workload bindings → Polydat Kernel
//!   compilation, with cache-and-rebind across phase iterations.
//!
//! ## Out of scope
//!
//! - Polydat DSL parsing and compilation: see [`polydat`].
//! - Workload YAML parsing: see [`nbrs_workload`].
//! - Component tree, instruments, cadence reporter: see
//!   [`nbrs_metrics`].
//! - Rate limiting: see [`nbrs_rate`].
//! - Error handler primitives: see [`nbrs_errorhandler`].
//!
//! ## See also
//!
//! - SRD 29 (`docs/SRD/29_execution_engine.md`) — the engine
//!   front door: this crate's public contract surface, the
//!   load-bearing axioms, and the SRD ↔ module map.
//! - SRD 01 (`docs/SRD/01_system_overview.md`) — overall
//!   architecture.
//! - SRD 18b — scenario-tree, scope-tree, scheduler.
//! - SRD 22 (`docs/SRD/22_op_sequencing.md`) — op sequencing
//!   and stanza model.
//! - SRD 30 (`docs/SRD/30_adapter_interface.md`) — adapter
//!   trait surface.

// Polydat node registrations that need nbrs-runtime's runtime
// services (component tree + controls + fiber context). Moved out
// of polydat itself so polydat can publish standalone — see
// `polydat_nodes/mod.rs` for the rationale.
pub mod polydat_nodes;
pub mod wires;
pub mod adapter;
pub mod control_catalog;
pub mod op_modifier;
pub mod opseq;
pub mod activity;
pub(crate) mod adapters;
pub mod synthesis;
pub mod scope_synth;
pub mod bindings;
pub(crate) mod params;
pub mod scope;
pub mod scope_tree;
pub(crate) mod scope_elision;
pub mod scene_tree;
pub(crate) mod phase_filter;
/// SRD-71 P3 phase-scoped CLI parameter overrides
/// (`<phase-pattern>.<param>=<value>`).
pub(crate) mod phase_params;
/// SRD-76 phase outcome disposition (structured
/// per-phase status + error list).
pub mod phase_outcome;
/// Phase-end trigger registry — content-agnostic callbacks
/// that fire after every phase completion or failure. Used by
/// the `watch=plots` / `watch=report` CLI flags to keep an
/// external view (plot image, report html) up-to-date as the
/// run progresses.
pub mod phase_end_triggers;
/// SRD-77 refine plan — pre-computed skip set + next-execution
/// id, derived from a session's prior `phase_outcomes` rows.
/// The runner builds one when `nbrs refine` re-attaches to an
/// existing session; the executor's phase-walk gate checks it
/// before dispatching each phase's per-cycle work.
pub mod refine_plan;
pub mod checkpoint;
pub(crate) mod scheduler;
pub(crate) mod profiler;
pub mod session_signals;
pub mod execution_context;
pub mod concurrent;
pub mod observer;
pub mod output_channel;
pub(crate) mod trace_router;
pub mod session;
pub mod runner;
pub(crate) mod executor;
pub(crate) mod error_policy;
pub(crate) mod stop_conditions;
pub(crate) mod workload_shell;
/// SRD-86 — the optimizer service boundary: the `Optimizer`/`Objective`
/// contract + registry the phase-execution driver uses, defined here in the
/// core with no dependency on any algorithm crate. Public so algorithm crates
/// (e.g. `nbrs-optimizers`) can register against it and the CLI can discover.
pub mod optimize;
pub mod resource_pool;
pub(crate) mod describe;
pub mod wrappers;
pub mod wrapper_registry;
pub(crate) mod wrapper_registrations;
pub mod wrapper_resolver;
pub(crate) mod relevancy;
pub mod fixture;
pub mod validation;
pub(crate) mod fiber_pool;
pub(crate) mod daemon_pool;
pub mod log_sink;
/// Lifecycle event vocabulary: the kind-tag [`lifecycle::EventType`]
/// and its [`lifecycle::SubjectKind`], shared by the readout
/// binder and the checkpoint log.
pub mod lifecycle;
pub mod readouts;
pub(crate) mod readout_context;
pub mod report_anchor;
