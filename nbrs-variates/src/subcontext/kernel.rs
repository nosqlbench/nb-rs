// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! [`ScopeKernel<M>`] — typed wrapper around [`crate::kernel::GkKernel`].
//!
//! Per SRD-67 §"Walled-off invariant", `ScopeKernel<M>` is the
//! typed surface; the underlying `GkKernel` stays public for
//! Phase 1 (legacy call sites still construct it directly), and
//! becomes `pub(crate)` in Phase 4 once the migration lands.
//!
//! The kernel exposes:
//! - [`Self::subcontext_builder`] — yields a typed
//!   [`super::SubcontextBuilder`] from an `Arc<Self>`. The single
//!   public entry point for child construction.
//! - [`Self::spawn`] — the single chokepoint where every
//!   cross-binding is resolved; takes a closed
//!   [`super::ScopeModule`] artifact, applies SRD-67's
//!   cross-binding rules, returns a typed child kernel and
//!   records the spawn under `name` in this kernel's registry.
//! - [`Self::release_child`] — drop a registry entry to allow
//!   re-spawn under the same name (for per-iteration
//!   re-traversal).

use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};

use crate::kernel::{GkKernel, SharedCell};
use crate::node::{PortType, Value};

use super::builder::SubcontextBuilder;
use super::error::{ContractViolation, SourceContext};
use super::module::{ScopeModule, WriteThroughBinding};
use super::name::ChildName;
use super::pull::RegisteredPullConsumer;

/// Phantom-marker brand for the workload-root scope kernel —
/// the top of any spawn type chain. Tests / examples that need
/// a "starting" identity use this.
#[derive(Debug)]
pub struct RootMarker;

/// Phantom-marker brand for "child of `P`". `spawn` returns
/// `ScopeKernel<Child<P>>`, distinct at the type level from a
/// sibling's `Child<P>` *value* but type-compatible at the
/// module-identity level (per SRD-67 §"Decision 6").
#[derive(Debug)]
pub struct Child<P>(PhantomData<fn() -> P>);

/// Internal record of a spawned child — used for the
/// duplicate-spawn diagnostic.
#[derive(Debug)]
struct ChildEntry {
    site: SourceContext,
}

/// Typed wrapper around an `Arc<GkKernel>`.
///
/// Construction via this type goes through the SRD-67 protocol
/// (`subcontext_builder` → `finalize` → `spawn`); direct
/// construction from a `GkKernel` is `pub(crate)` for the
/// Phase 1 internal bridge.
pub struct ScopeKernel<M> {
    name: ChildName,
    inner: Arc<Mutex<GkKernel>>,
    site: SourceContext,
    children: Mutex<HashMap<ChildName, ChildEntry>>,
    consumers: Mutex<Vec<RegisteredPullConsumer>>,
    /// Rule 2 write-through bindings. Per-cycle eval of this
    /// kernel must call [`Self::commit_write_throughs`] after
    /// producing values to fan them through the parent's
    /// `SharedCell`s.
    write_throughs: Vec<WriteThroughBinding>,
    _module: PhantomData<fn() -> M>,
}

impl<M> std::fmt::Debug for ScopeKernel<M> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScopeKernel")
            .field("name", &self.name)
            .field("site", &self.site)
            .finish()
    }
}

/// One shared cell visible at a parent scope, reified for
/// transitive cross-binding. Returned by
/// [`ScopeKernel::shared_cells_in_scope`].
///
/// Carries the name a child must use to bind to the cell,
/// the port type (so Rule 2 / `extern` synthesis at finalize
/// can declare a typed input slot), and the cell handle (so
/// spawn can attach it to the child's matching input).
///
/// "In scope" semantics: a cell visible at the parent is one
/// the parent itself can read or write at this scope —
/// covering both:
///
/// 1. Cells the parent declared via its own program
///    (`shared X := <init>` produces a cell-bound input slot).
/// 2. Cells inherited from the parent's own ancestors
///    (attached during the parent's spawn). Without this
///    case, a `shared` cell at the workload root would not
///    propagate to grand-children whose immediate parent's
///    body never references the name.
///
/// Both cases are answered by walking the parent's input
/// slots and reading [`crate::kernel::engines::Engines::shared_cell`].
#[derive(Clone)]
pub struct SharedCellInScope {
    pub name: String,
    pub port_type: PortType,
    pub cell: SharedCell,
}

impl std::fmt::Debug for SharedCellInScope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedCellInScope")
            .field("name", &self.name)
            .field("port_type", &self.port_type)
            .finish()
    }
}

impl<M> ScopeKernel<M> {
    /// Enumerate every shared cell visible at this scope.
    /// Delegates to [`GkKernel::shared_cells_in_scope`] —
    /// the carrier lives at the kernel layer so it survives
    /// any wrap/unwrap dance the activity layer does. The
    /// `SharedCellInScope` re-export is kept for callers in
    /// the SRD-67 builder; it's a thin alias over the kernel
    /// layer's [`SharedCellEntry`].
    pub fn shared_cells_in_scope(&self) -> Vec<SharedCellInScope> {
        let inner = self.lock_inner();
        inner
            .shared_cells_in_scope()
            .into_iter()
            .map(|e| SharedCellInScope {
                name: e.name,
                port_type: e.port_type,
                cell: e.cell,
            })
            .collect()
    }

    /// Internal constructor — only callers within the crate (the
    /// builder / spawn path; tests via `Self::wrap_for_test`)
    /// produce a `ScopeKernel` directly. Public callers go
    /// through the protocol.
    pub(crate) fn new_internal(
        name: ChildName,
        kernel: GkKernel,
        site: SourceContext,
        consumers: Vec<RegisteredPullConsumer>,
    ) -> Self {
        Self::new_with_write_throughs(name, kernel, site, consumers, Vec::new())
    }

    pub(crate) fn new_with_write_throughs(
        name: ChildName,
        kernel: GkKernel,
        site: SourceContext,
        consumers: Vec<RegisteredPullConsumer>,
        write_throughs: Vec<WriteThroughBinding>,
    ) -> Self {
        Self {
            name,
            inner: Arc::new(Mutex::new(kernel)),
            site,
            children: Mutex::new(HashMap::new()),
            consumers: Mutex::new(consumers),
            write_throughs,
            _module: PhantomData,
        }
    }

    /// The structured name this kernel was spawned under (for
    /// child kernels) or its self-label (for root kernels).
    pub fn name(&self) -> &ChildName {
        &self.name
    }

    /// Diagnostic site for this kernel's construction.
    pub fn site(&self) -> &SourceContext {
        &self.site
    }

    /// Borrow the underlying `GkKernel` for read-only
    /// operations. The lock is released when the returned guard
    /// is dropped. Phase 1 exposes this so legacy call sites
    /// (and tests) can still drive the kernel via the existing
    /// API; Phase 4 narrows or removes it once the migration
    /// completes.
    pub fn lock_inner(&self) -> std::sync::MutexGuard<'_, GkKernel> {
        self.inner.lock().expect("ScopeKernel inner kernel poisoned")
    }

    /// The pull consumers registered with this kernel. Used by
    /// the activity-side fixture adapter at seal time.
    pub fn consumers(&self) -> Vec<RegisteredPullConsumer> {
        self.consumers
            .lock()
            .expect("ScopeKernel consumers poisoned")
            .clone()
    }

    /// Whether `name` is recorded in this kernel's named-child
    /// registry. Diagnostic; Phase 1 tests assert against this.
    pub fn has_child(&self, name: &ChildName) -> bool {
        self.children
            .lock()
            .expect("ScopeKernel children registry poisoned")
            .contains_key(name)
    }

    /// Drop the named child from this kernel's registry. The
    /// child kernel itself is unaffected — only the registry
    /// entry. After release, the same name may be spawned again
    /// (typical for comprehension scopes that re-traverse per
    /// iteration). See SRD-67 §"Release semantics".
    pub fn release_child(&self, name: &ChildName) {
        self.children
            .lock()
            .expect("ScopeKernel children registry poisoned")
            .remove(name);
    }

    /// Begin construction of a child sub-context. Per SRD-67
    /// §"Step 1 — Parent yields a builder": the builder borrows
    /// an `Arc` of the parent, accumulates module matter, and
    /// produces a closed [`ScopeModule`] artifact at finalize.
    pub fn subcontext_builder(self: Arc<Self>) -> SubcontextBuilder<M> {
        SubcontextBuilder::new(self)
    }

    /// Spawn a child kernel from a closed [`ScopeModule`]
    /// artifact. Per SRD-67 §"Step 4 — Parent spawns the child
    /// kernel": this is the single chokepoint where every cross-
    /// binding is resolved.
    ///
    /// Phase 1 implementation: applies Rules 1, 4, and 5 by
    /// delegating to the existing `bind_outer_scope`. Rule 2
    /// (write-through rewrite) and Rule 3 (init pull post-bind)
    /// surface as diagnostics / TODOs for Phase 2 — the kernel
    /// synthesis change required to rewrite assignment LHS into
    /// shared-cell writes is out of scope here.
    pub fn spawn(
        self: &Arc<Self>,
        name: ChildName,
        artifact: ScopeModule<Child<M>>,
    ) -> Result<ScopeKernel<Child<M>>, ContractViolation> {
        // ----- Named-child registry guard (SRD-67 §"Spawn
        // semantics") -----
        {
            let mut children = self
                .children
                .lock()
                .expect("ScopeKernel children registry poisoned");
            if let Some(prior) = children.get(&name) {
                return Err(ContractViolation::DuplicateChild {
                    name: name.clone(),
                    prior_site: prior.site.clone(),
                    this_site: artifact.context.clone(),
                });
            }
            children.insert(
                name.clone(),
                ChildEntry {
                    site: artifact.context.clone(),
                },
            );
        }

        // ----- Cross-binding resolution -----
        // Single chokepoint: `bind_outer_scope` walks every
        // cell visible at the parent (own slots + transit
        // cells inherited from ancestors), attaches each to
        // any matching child slot, and forwards the rest as
        // transit on the child kernel. This is the transitive
        // cascade — an ancestral `shared X` cell remains
        // visible to deep descendants regardless of how many
        // intermediate scopes' bodies skip the name.
        //
        // Honours Rule 1 (import resolution validated at
        // finalize), Rule 2 (write-through rewrite produces
        // the matching child input slot finalize-side),
        // Rule 4 (coordinate routing via IterationExtern
        // input-kind), Rule 5 (closure-binding economy —
        // unreferenced names skip cell attachment but still
        // ride the transit channel for grand-children).
        let parent_inner = self.lock_inner();
        let child_kernel = parent_inner.materialize_subscope(artifact.program.clone(), &[]);
        drop(parent_inner);

        let child_site = artifact.context.clone();
        let child_consumers = artifact.consumers.clone();
        let child_write_throughs = artifact.write_throughs.clone();

        Ok(ScopeKernel::new_with_write_throughs(
            name,
            child_kernel,
            child_site,
            child_consumers,
            child_write_throughs,
        ))
    }

    /// The Rule 2 write-through bindings carried by this kernel.
    /// Empty for the vast majority of kernels; populated only
    /// when the artifact's `finalize` rewrote a child export to
    /// a parent `shared` cell write.
    pub fn write_throughs(&self) -> &[WriteThroughBinding] {
        &self.write_throughs
    }

    /// Per-cycle Rule 2 commit: pulls every write-through's
    /// synthetic source output (`__write_<X>`) and stores its
    /// value through the corresponding child input slot for
    /// `<X>`. Because `bind_outer_scope` attached the parent's
    /// `SharedCell` to that slot, the write propagates to the
    /// cell, where it becomes visible to the parent and to any
    /// sibling that shares the same cell.
    ///
    /// No-op for kernels with no write-throughs.
    pub fn commit_write_throughs(&self) {
        if self.write_throughs.is_empty() {
            return;
        }
        let mut inner = self.lock_inner();
        // Two-pass to avoid holding two mutable borrows of the
        // kernel at once: pull each value first (the pull mutates
        // state), collect (idx, value) pairs, then write through
        // in a second pass.
        let mut pending: Vec<(usize, Value)> = Vec::with_capacity(self.write_throughs.len());
        for wt in &self.write_throughs {
            let Some(idx) = inner.program().find_input(&wt.export_name) else {
                continue;
            };
            let value = inner.pull(&wt.source_output).clone();
            pending.push((idx, value));
        }
        for (idx, value) in pending {
            inner.state().set_input(idx, value);
        }
    }
}

/// Construct a workload-root [`ScopeKernel<RootMarker>`] from a
/// pre-compiled [`GkKernel`]. Phase 1 bridge for callers that
/// already have a kernel and want to use it as the parent of a
/// typed sub-context.
///
/// In Phase 4 once the migration completes, the workload-root
/// path will produce a `ScopeKernel<RootMarker>` directly from
/// the workload-load entry point.
pub(crate) fn wrap_root_kernel(kernel: GkKernel, label: impl Into<String>) -> Arc<ScopeKernel<RootMarker>> {
    let label = label.into();
    let name = ChildName::from_segments([label.clone()]);
    let site = SourceContext::new(label);
    Arc::new(ScopeKernel::new_internal(name, kernel, site, Vec::new()))
}

/// SRD-67 Phase 2 — synthesise a [`GkKernel`] under a borrowed
/// parent kernel via the subcontext-builder protocol. The bridge
/// migration callers (e.g. `build_do_loop_scope_kernel`) use to
/// route through `spawn` without changing their call-site
/// signatures.
///
/// Construction sequence:
/// 1. Wrap a `from_program` clone of the parent in a transient
///    `ScopeKernel<RootMarker>` so the builder can examine the
///    parent's program shape (output names, modifiers, input
///    ports) for Rule 1 / Rule 2 validation.
/// 2. Run [`SubcontextBuilder::finalize`] — this is where the
///    Rule 2 rewrite (parent `shared X` collision with child
///    `X := <expr>`) and `mark_inherited_outputs` are applied
///    to the freshly-compiled program.
/// 3. Construct the child `GkKernel` from the closed program.
///    Spawn-time cross-binding (cell attachment via
///    `bind_outer_scope`) is applied against the **original**
///    parent kernel so the child's input slots see live outer-
///    scope values, not the `from_program` clone's default-zero
///    state.
///
/// Typed gk matter accepted by both kernel-construction
/// paths — root and subscope. Opaque externally: the only way
/// to obtain a `GkMatter` value is via [`GkMatter::builder`].
///
/// Internally carries one of three input forms — fresh source,
/// pre-parsed statements (the "module parser" output), or a
/// pre-compiled program. The builder validates that exactly
/// one form is provided.
pub struct GkMatter<'a> {
    pub(crate) inner: GkMatterInner<'a>,
}

pub(crate) enum GkMatterInner<'a> {
    Source(SourceMatter),
    Statements(StatementsMatter),
    Program(ProgramMatter<'a>),
}

pub(crate) struct SourceMatter {
    pub(crate) label: String,
    pub(crate) body: String,
    pub(crate) result_bindings: Option<String>,
    pub(crate) inherited_outputs: Vec<String>,
    pub(crate) options: super::builder::CompileOptions,
}

pub(crate) struct StatementsMatter {
    pub(crate) label: String,
    pub(crate) statements: Vec<crate::dsl::ast::Statement>,
    pub(crate) result_bindings: Option<String>,
    pub(crate) inherited_outputs: Vec<String>,
    pub(crate) options: super::builder::CompileOptions,
}

pub(crate) struct ProgramMatter<'a> {
    pub(crate) program: Arc<crate::kernel::GkProgram>,
    pub(crate) iter_bindings: &'a [(String, Value)],
}

impl<'a> GkMatter<'a> {
    /// Begin building gk matter. The builder is the only
    /// constructor of `GkMatter`; the variants and their
    /// fields are not exposed.
    #[inline]
    pub fn builder() -> GkMatterBuilder<'a> {
        GkMatterBuilder::new()
    }
}

/// Builder for [`GkMatter`]. Configure exactly one input form
/// (source, pre-parsed statements, or program), plus optional
/// metadata, then call [`Self::build`].
#[derive(Default)]
pub struct GkMatterBuilder<'a> {
    label: Option<String>,
    body: Option<String>,
    statements: Option<Vec<crate::dsl::ast::Statement>>,
    program: Option<Arc<crate::kernel::GkProgram>>,
    iter_bindings: &'a [(String, Value)],
    result_bindings: Option<String>,
    inherited_outputs: Vec<String>,
    options: super::builder::CompileOptions,
}

impl<'a> GkMatterBuilder<'a> {
    fn new() -> Self {
        Self::default()
    }

    /// Diagnostic label for this matter. Surfaces in compile
    /// errors and the `__transient` parent name during the
    /// SubcontextBuilder dance.
    pub fn label(mut self, label: impl Into<String>) -> Self {
        self.label = Some(label.into());
        self
    }

    /// Provide gk source as a string. Mutually exclusive with
    /// [`Self::statements`] and [`Self::program`].
    pub fn source(mut self, body: impl Into<String>) -> Self {
        self.body = Some(body.into());
        self
    }

    /// Provide gk source as pre-parsed AST statements. Mutually
    /// exclusive with [`Self::source`] and [`Self::program`].
    /// Use when the caller has already run the module parser
    /// (e.g. when synthesising scope source from a structured
    /// model and wanting to skip a string round-trip).
    pub fn statements(mut self, stmts: Vec<crate::dsl::ast::Statement>) -> Self {
        self.statements = Some(stmts);
        self
    }

    /// Provide a pre-compiled program. Mutually exclusive with
    /// [`Self::source`] and [`Self::statements`]. Used for per-
    /// fiber state forks, comprehension iteration, and other
    /// call sites that hold a compiled program directly.
    pub fn program(mut self, program: Arc<crate::kernel::GkProgram>) -> Self {
        self.program = Some(program);
        self
    }

    /// Iter-var bindings applied before the parent binds the
    /// child. Only meaningful for the program form.
    pub fn iter_bindings(mut self, bindings: &'a [(String, Value)]) -> Self {
        self.iter_bindings = bindings;
        self
    }

    /// SRD-66 result-binding source. Folded through
    /// [`super::SubcontextBuilder::add_result_bindings`] at
    /// finalize. Only meaningful for source / statements forms.
    pub fn result_bindings(mut self, src: impl Into<String>) -> Self {
        self.result_bindings = Some(src.into());
        self
    }

    /// Names to pass through `mark_inherited_outputs` so the
    /// scope tree can distinguish own exports from cascade-
    /// inherited names. Source / statements forms only.
    pub fn inherited_outputs(mut self, names: Vec<String>) -> Self {
        self.inherited_outputs = names;
        self
    }

    /// Compile-time knobs (lib paths, strict mode, required
    /// outputs, cursor limit). Source / statements forms only.
    pub fn options(mut self, options: super::builder::CompileOptions) -> Self {
        self.options = options;
        self
    }

    /// Validate and produce typed matter. Errors when zero or
    /// more than one input form is configured.
    pub fn build(self) -> Result<GkMatter<'a>, String> {
        let forms = [self.body.is_some(), self.statements.is_some(), self.program.is_some()];
        let count = forms.iter().filter(|x| **x).count();
        if count == 0 {
            return Err("GkMatter::builder: no input form set (use .source / .statements / .program)".into());
        }
        if count > 1 {
            return Err("GkMatter::builder: multiple input forms set; choose exactly one".into());
        }
        let label = self.label.unwrap_or_else(|| "(matter)".to_string());
        let inner = if let Some(body) = self.body {
            GkMatterInner::Source(SourceMatter {
                label,
                body,
                result_bindings: self.result_bindings,
                inherited_outputs: self.inherited_outputs,
                options: self.options,
            })
        } else if let Some(stmts) = self.statements {
            GkMatterInner::Statements(StatementsMatter {
                label,
                statements: stmts,
                result_bindings: self.result_bindings,
                inherited_outputs: self.inherited_outputs,
                options: self.options,
            })
        } else {
            // program
            GkMatterInner::Program(ProgramMatter {
                program: self.program.expect("program form set per count above"),
                iter_bindings: self.iter_bindings,
            })
        };
        Ok(GkMatter { inner })
    }
}

impl GkKernel {
    /// THE subscope-construction path. Per the kernel-construction
    /// invariant, this is the ONE method through which a parent
    /// kernel produces a child. `compile_gk` produces root
    /// kernels; everything else is a subscope and routes here.
    ///
    /// Cell propagation, scope-coordinate plumbing, and Rule 2
    /// write-throughs flow from `self` (the parent) into the
    /// returned child. Returns the child kernel plus any
    /// write-through bindings finalize produced (empty for the
    /// program-matter form, populated for the source-matter
    /// form when a result-LHS collides with a parent `shared`
    /// cell).
    pub fn build_subscope(
        &self,
        matter: GkMatter<'_>,
    ) -> Result<GkKernel, ContractViolation> {
        use super::module::BodyFragment;
        match matter.inner {
            GkMatterInner::Program(p) => {
                Ok(self.materialize_subscope(p.program, p.iter_bindings))
            }
            GkMatterInner::Source(s) => {
                let transient = self.transient_typed_parent(&s.label);
                let mut builder = transient.clone().subcontext_builder();
                builder
                    .context(SourceContext::new(s.label.clone()))
                    .mark_inherited_outputs(s.inherited_outputs)
                    .with_compile_options(s.options)
                    .body(BodyFragment::GkSource(s.body));
                if let Some(src) = s.result_bindings {
                    builder.add_result_bindings(&src)?;
                }
                let module = builder.finalize()?;
                let child = self.materialize_subscope(module.program.clone(), &[]);
                drop(transient);
                Ok(child)
            }
            GkMatterInner::Statements(s) => {
                let transient = self.transient_typed_parent(&s.label);
                let mut builder = transient.clone().subcontext_builder();
                builder
                    .context(SourceContext::new(s.label.clone()))
                    .mark_inherited_outputs(s.inherited_outputs)
                    .with_compile_options(s.options)
                    .body(BodyFragment::Statements(s.statements));
                if let Some(src) = s.result_bindings {
                    builder.add_result_bindings(&src)?;
                }
                let module = builder.finalize()?;
                let child = self.materialize_subscope(module.program.clone(), &[]);
                drop(transient);
                Ok(child)
            }
        }
    }

    /// Snapshot a typed `ScopeKernel<RootMarker>` over `self`'s
    /// live cell view, used as the transient parent the
    /// SubcontextBuilder validates against.
    fn transient_typed_parent(&self, label: &str) -> Arc<ScopeKernel<RootMarker>> {
        wrap_root_kernel(self.snapshot_with_cells(), format!("{label}__transient"))
    }
}

// `bind_program_under_parent` and the `build_kernel_under_parent_*`
// family of free-function bridges are removed. Per the kernel-
// construction invariant, only two paths exist:
//
//   1. Root kernel built from source via `compile_gk` (and family).
//   2. Subscope kernel materialized by a parent kernel via
//      [`GkKernel::materialize_subscope`], [`GkKernel::adopt_subscope`],
//      or [`GkKernel::build_subscope_from_source`] — all methods on
//      `GkKernel` itself, parent-supervised, typed.
//
// External callers go through these GkKernel-controlled paths
// directly; no free-function bridges remain.

// `instance_program` is removed. The two sanctioned construction
// paths are:
//
//   1. Root kernel built from source via `compile_gk` family.
//   2. Subscope kernel materialized by an existing parent
//      kernel via `GkKernel::materialize_subscope` or
//      `GkKernel::build_subscope_from_source`.
//
// Tests that need a kernel from pre-compiled program matter use
// `GkAssembler::compile()` (which returns a root kernel) directly.
