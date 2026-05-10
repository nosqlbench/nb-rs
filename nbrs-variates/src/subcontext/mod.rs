// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! SRD-67 — parent-gated GK sub-context construction (Phase 1 surface).
//!
//! This module is the **additive** typed entry point for constructing
//! a GK child kernel as a function of a parent kernel. It implements
//! the protocol from
//! [`docs/sysref/67_gk_subcontext_construction.md`](../../../../docs/sysref/67_gk_subcontext_construction.md):
//!
//! 1. Parent yields a builder via [`ScopeKernel::subcontext_builder`].
//! 2. Builder accumulates module matter (imports, exports, body
//!    fragments, pull consumers) via [`SubcontextBuilder`].
//! 3. `finalize` closes the builder, validates imports against the
//!    parent's exports, compiles the body into a [`ScopeModule`] with
//!    a typed [`ScopeContract`].
//! 4. Parent spawns the child via [`ScopeKernel::spawn`] — the single
//!    chokepoint where every cross-binding is resolved.
//!
//! ## Phase scope
//!
//! Phase 1 (shipped) is **additive** — it coexists with the
//! existing `bind_outer_scope` / `from_program` / `compile_gk`
//! machinery. Phase 2 (this push) lands Rule 2 (write-through
//! rewrite for shared exports) end-to-end and migrates the
//! do-loop synthesiser to the builder protocol; the other
//! synthesisers stay on their existing untyped paths until
//! Phase 3.
//!
//! Cross-binding rules from SRD-67 §"Cross-binding rules" are
//! enforced at [`SubcontextBuilder::finalize`] and
//! [`ScopeKernel::spawn`]:
//!
//! * Rule 1 — import resolution at finalize.
//! * Rule 2 — export collision with `final` parent surfaces as
//!   [`ContractViolation::FinalShadow`]; collision with `shared`
//!   parent rewrites the body's `X := <expr>` into
//!   `extern X: <type>` + `__write_X := <expr>` and records a
//!   [`WriteThroughBinding`] on the artifact. Per-cycle eval
//!   calls [`ScopeKernel::commit_write_throughs`] to fan values
//!   through the parent's `SharedCell`.
//! * Rule 4 — coordinate routing handled by `bind_outer_scope`'s
//!   IterationExtern input-kind.
//! * Rule 5 — closure-binding economy: unused imports surface as
//!   finalize diagnostics rather than errors.
//!
//! ## Cross-crate boundary
//!
//! [`PullConsumer`] is defined as a trait so that `nbrs-activity`'s
//! `ScopeFixture` can implement it without `nbrs-variates`
//! depending on `nbrs-activity` (the crate dependency runs the
//! other way). Phase 1 ships a minimal trait shape — the names a
//! consumer wants to pull at cycle time — sufficient for the
//! activity-side `ScopeFixture::register_consumer` adapter to
//! absorb. The eventual seal of the SRD-32 `PullPlan` happens on
//! the activity side; the artifact only carries the requested
//! names.
//!
//! ## Walled-off invariant (SRD-67 Phase 4)
//!
//! Per SRD-67 §"Walled-off invariant", the legacy
//! cross-binding primitives `GkKernel::bind_outer_scope` and
//! `GkKernel::from_program` are `pub(crate)` after Phase 4.
//! External consumers must go through the typed surface:
//! [`SubcontextBuilder`] / [`ScopeKernel::spawn`] for child
//! construction, [`instance_program`] for parentless re-instancing
//! of a compiled program, [`chain_kernel_under_parent`] for top-
//! level kernel chaining.
//!
//! The following compile-fail doctests guard the seal — if any
//! of them starts compiling, the seal is broken and a Phase 4
//! invariant has regressed.
//!
//! `bind_outer_scope` is not public:
//!
//! ```compile_fail
//! use nbrs_variates::dsl::compile::compile_gk;
//! let mut inner = compile_gk("inputs := (cycle)\n").unwrap();
//! let outer = compile_gk("inputs := (cycle)\n").unwrap();
//! inner.bind_outer_scope(&outer); // pub(crate) — must not compile
//! ```
//!
//! `GkKernel::from_program` is not public:
//!
//! ```compile_fail
//! use nbrs_variates::dsl::compile::compile_gk;
//! use nbrs_variates::kernel::GkKernel;
//! let kernel = compile_gk("inputs := (cycle)\n").unwrap();
//! let _ = GkKernel::from_program(kernel.program().clone()); // pub(crate)
//! ```

mod builder;
mod error;
mod kernel;
mod module;
mod name;
mod pull;
mod spec;

#[cfg(test)]
mod tests;

pub use builder::{CompileOptions, SubcontextBuilder};
pub use error::{ContractViolation, SourceContext};
pub use kernel::{
    Child, GkMatter, GkMatterBuilder, RootMarker,
};
pub(crate) use kernel::GkMatterInner;
pub use kernel::{
    ScopeKernel, SharedCellInScope,
};
pub use module::{BodyFragment, ScopeContract, ScopeModule};
pub(crate) use module::WriteThroughBinding;
pub use name::ChildName;
pub use pull::{NamedPullConsumer, PullConsumer, RegisteredPullConsumer};
pub use spec::{ExportClassification, ExportSpec, ImportClassification, ImportSpec};
