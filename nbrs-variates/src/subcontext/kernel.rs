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

use crate::kernel::GkKernel;
use crate::node::Value;

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

impl<M> ScopeKernel<M> {
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
        // Honours Rule 1 (import resolution against parent
        // exports — validated at finalize), Rule 2 (write-through
        // rewrite for shared exports — the rewrite happens at
        // finalize; spawn carries the resulting bindings forward
        // so per-cycle eval fans values through the parent's
        // `SharedCell`s via [`Self::commit_write_throughs`]),
        // Rule 4 (coordinate routing — handled inside
        // `bind_outer_scope` via the existing IterationExtern
        // input-kind), and Rule 5 (closure-binding economy —
        // an unreferenced import has no input slot, so
        // `bind_outer_scope` is a no-op for it).
        let parent_inner = self.lock_inner();

        let mut child_kernel = GkKernel::from_program(artifact.program.clone());
        child_kernel.bind_outer_scope(&parent_inner);
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
pub fn wrap_root_kernel(kernel: GkKernel, label: impl Into<String>) -> Arc<ScopeKernel<RootMarker>> {
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
/// The transient `ScopeKernel<RootMarker>` carries its own
/// named-child registry — this bridge does NOT register the
/// child against any caller-visible parent registry; that
/// responsibility belongs to the call site (Phase 4 narrows
/// it).
pub fn build_kernel_under_parent(
    parent: &GkKernel,
    label: &str,
    body_source: String,
    inherited_outputs: Vec<String>,
) -> Result<GkKernel, ContractViolation> {
    let (kernel, _wts) = build_kernel_under_parent_with_options(
        parent,
        label,
        body_source,
        inherited_outputs,
        super::builder::CompileOptions::default(),
    )?;
    Ok(kernel)
}

/// SRD-67 Phase 3 extension — same as [`build_kernel_under_parent`]
/// but threads [`super::builder::CompileOptions`] (lib paths,
/// strict, required-output filter, source dir, context label)
/// through the builder. Used by the for_each / op-template /
/// phase-scope migrations to preserve byte-identical
/// `compile_gk_with_libs` invocations.
///
/// SRD-67 Phase 5 — also returns the [`WriteThroughBinding`]
/// vector finalize produced. Op-template synthesisers that fold
/// SRD-66 `result:` source through
/// [`SubcontextBuilder::add_result_bindings`] need these
/// bindings at runtime to drive `commit_write_throughs`.
pub fn build_kernel_under_parent_with_options(
    parent: &GkKernel,
    label: &str,
    body_source: String,
    inherited_outputs: Vec<String>,
    compile_options: super::builder::CompileOptions,
) -> Result<(GkKernel, Vec<WriteThroughBinding>), ContractViolation> {
    build_kernel_under_parent_full(
        parent,
        label,
        body_source,
        None,
        inherited_outputs,
        compile_options,
    )
}

/// SRD-67 Phase 5 — full bridge entry point that accepts an
/// optional `result_bindings` source folded through
/// [`SubcontextBuilder::add_result_bindings`] before finalize.
///
/// Returns the bound child kernel plus the
/// [`WriteThroughBinding`]s the Rule 2 rewrite produced (empty
/// when no result-LHS collides with a parent `shared` export).
pub fn build_kernel_under_parent_full(
    parent: &GkKernel,
    label: &str,
    body_source: String,
    result_bindings: Option<&str>,
    inherited_outputs: Vec<String>,
    compile_options: super::builder::CompileOptions,
) -> Result<(GkKernel, Vec<WriteThroughBinding>), ContractViolation> {
    use super::module::BodyFragment;
    // The builder needs an Arc<ScopeKernel<P>>. Construct a
    // transient one whose inner is a `from_program` clone of the
    // parent — sufficient for builder-side checks that read
    // program shape (output names + modifiers + input ports).
    // The bridge re-runs `bind_outer_scope` against the original
    // `parent` below so the child's runtime sees live values.
    let transient_parent_kernel = GkKernel::from_program(parent.program().clone());
    let transient_parent = wrap_root_kernel(transient_parent_kernel, format!("{label}__transient"));

    let mut builder = transient_parent.clone().subcontext_builder();
    builder
        .context(SourceContext::new(label.to_string()))
        .mark_inherited_outputs(inherited_outputs)
        .with_compile_options(compile_options)
        .body(BodyFragment::GkSource(body_source));
    if let Some(src) = result_bindings {
        builder.add_result_bindings(src)?;
    }
    let module = builder.finalize()?;

    // Build the child `GkKernel` directly from the closed
    // program (rather than going through `spawn` against the
    // transient), then apply `bind_outer_scope` against the
    // **live** parent — this is the same final sequence the
    // legacy `build_do_loop_scope_kernel` executed and preserves
    // byte-identical runtime behaviour. Spawn's Rule 2 hook is
    // not strictly needed here because the do-loop synthesiser
    // doesn't currently produce shared-export collisions
    // (cascade externs only); if it later does, a follow-up can
    // wire `commit_write_throughs` into the do-loop's per-
    // iteration evaluation.
    let mut child_kernel = GkKernel::from_program(module.program.clone());
    child_kernel.bind_outer_scope(parent);
    // Drop the transient-parent registry entries — the bridge's
    // child-name was synthetic, never user-visible.
    drop(transient_parent);
    let write_throughs = module.write_throughs.clone();
    // SRD-67 Phase 5 — fold the write-throughs onto the kernel
    // so per-cycle code can call
    // [`GkKernel::commit_write_throughs`] without threading the
    // bindings as a side channel.
    let kernel_wts: Vec<crate::kernel::KernelWriteThrough> = write_throughs
        .iter()
        .map(|wt| crate::kernel::KernelWriteThrough {
            export_name: wt.export_name.clone(),
            source_output: wt.source_output.clone(),
        })
        .collect();
    child_kernel.set_write_throughs(kernel_wts);
    Ok((child_kernel, write_throughs))
}

/// SRD-67 Phase 3 — bind a pre-compiled child program under a
/// borrowed parent kernel. This is the "compiled once, bound
/// per-fiber" shape used by the phase-scope cache-and-rebind
/// path (SRD-13c §"Per-Scope Canonical Kernel Cache") and by
/// `OpBuilder::create_fiber_builder`'s per-op-template instancing
/// loop. Encapsulates the `from_program → bind_outer_scope`
/// sequence so callers stop reaching into both APIs directly;
/// Phase 4 narrows the underlying surface to `pub(crate)`.
///
/// The optional `inherited_outputs` is applied via
/// `mark_inherited_outputs` before the bind, mirroring the
/// `build_kernel_under_parent` flow. Callers that already marked
/// the program at compile-time (the typical phase-cache cache-hit
/// path) pass an empty vec.
pub fn bind_program_under_parent(
    parent: &GkKernel,
    program: Arc<crate::kernel::GkProgram>,
    inherited_outputs: Vec<String>,
) -> GkKernel {
    let mut child_kernel = GkKernel::from_program(program);
    if !inherited_outputs.is_empty() {
        child_kernel.mark_inherited_outputs(inherited_outputs);
    }
    child_kernel.bind_outer_scope(parent);
    child_kernel
}

/// SRD-67 Phase 4 — instantiate a parentless `GkKernel` from a
/// previously-compiled program. Replaces direct
/// `GkKernel::from_program` calls in `nbrs-activity`; the
/// underlying constructor is `pub(crate)` after Phase 4.
///
/// This is the "no-parent" sibling of [`bind_program_under_parent`]:
/// used by call sites that need a fresh state on top of an
/// existing program but have no outer scope to chain through
/// (e.g. workload-canonical kernels, fiber-builder construction,
/// `mem::replace` placeholders). Per SRD-67's lifecycle rule —
/// "compile once, spawn once, fiber-state separately" — this
/// surfaces the per-instance state-clone primitive without
/// re-opening the legacy public API.
pub fn instance_program(program: Arc<crate::kernel::GkProgram>) -> GkKernel {
    GkKernel::from_program(program)
}

/// SRD-67 Phase 4 — chain `child` to receive imports from
/// `parent`. Replaces direct `GkKernel::bind_outer_scope` calls
/// in `nbrs-activity` (the underlying method is `pub(crate)`
/// after Phase 4).
///
/// Used at workload-root construction time: after the
/// workload-bindings kernel is compiled, it chains through the
/// workload-params kernel so descendant kernels see params via
/// the standard scope-chain. This is a top-level chain step,
/// not a synthesiser call — the typed
/// [`SubcontextBuilder`] / [`ScopeKernel::spawn`] path is for
/// child-kernel construction; this entry handles the
/// compose-already-built-roots case.
pub fn chain_kernel_under_parent(child: &mut GkKernel, parent: &GkKernel) {
    child.bind_outer_scope(parent);
}
