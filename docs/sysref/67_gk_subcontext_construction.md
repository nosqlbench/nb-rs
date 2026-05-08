# SRD-67 — Parent-Gated GK Sub-Context Construction

**Status:** Pushes 1–5 shipped 2026-05-08. Typed surface
landed (Phase 1); do-loop migrated and Rule 2 SharedCell
write-through implemented as compile-time AST rewrite
(Phase 2); remaining synthesisers migrated through
`build_kernel_under_parent_with_options` /
`bind_program_under_parent` bridges (Phase 3); public API
narrowed — `bind_outer_scope` and `from_program` are
`pub(crate)`, two `compile_fail` doctests guard the seal,
typed shims `instance_program` and `chain_kernel_under_parent`
cover the legitimate cross-crate cases (Phase 4); SRD-66's
result-bindings wired via `SubcontextBuilder::add_result_bindings`
with closure-binding economy on `body`/`count`/`ok`/captures,
per-cycle write through `FiberBuilder::commit_op_template_write_throughs`
(Phase 5). 18 unit tests pass; full workspace + integration
green; CQL workload `full_cql_vector.yaml` compiles cleanly
through the new kernel-driven path with the dialect-detection
booleans materialising as workload-root shared cells.
**Owner:** nbrs-variates (kernel/program API), nbrs-activity
(scope synthesis call sites)
**Cross-refs:** SRD-13c (scope model — `bind_outer_scope`,
manifest extraction), SRD-13d (op-template scope layer),
SRD-13e (scope-as-module typed contracts — the data model
this SRD's API operates on), SRD-16 (`shared` mutability),
SRD-32 (init-time fixture / pull plan — read-side analogue),
SRD-66 (the motivating use case — result-wire writes to
outer-scope `shared` wires)

---

## What this SRD specifies

The **construction protocol** for GK sub-contexts: how a
child kernel comes into existence as a function of a parent
kernel.

SRD-13e specifies *what* sub-contexts are (typed
`ScopeModule` with import / export contracts). SRD-67
specifies *how* they're constructed: the API surface, the
lifecycle sequence, and the parent-side responsibility that
makes cross-bindings (cell-backed externs, shared writes,
import resolution) deterministic at construction time
instead of at use time.

The load-bearing rule:

> **A GK sub-context instance can only come into existence
> through its parent. The parent gates construction by
> handing out a builder, accepting a closed module-matter
> artifact, and producing the child kernel as the single
> point where every cross-binding is decided.**

No ad-hoc `bind_outer_scope`-on-an-already-constructed
kernel. No "compile here, attach there" patchwork. One
typed protocol; one place where the parent/child contract
is resolved.

---

## Why this SRD now

Implementing SRD-66 §"Push 2" surfaced the cross-binding
trap: result-wires that want to write back to outer-scope
`shared` wires need an `extern` slot in the inner kernel,
but declaring `extern X` AND assigning `X := <expr>` in
the same scope produces "duplicate node name `__port_X`"
because the kernel synthesiser treats them as independent
declarations rather than coordinated lifecycle events.

The deeper problem is the same one SRD-13e diagnosed: **the
parent/child contract is implicit and resolved late.**
SRD-13e proposes typed contracts (the *what*); SRD-67
proposes the construction protocol (the *how*). The two
are complementary — typed contracts without a marshalled
construction path still allow the bypass that produces
duplicate-port errors; a marshalled construction path
without typed contracts still allows mismatched-shape
binding. Both pieces are needed.

The bug shapes that motivate SRD-67 mirror those that
motivated 13e:

1. **`bind_outer_scope` on a fully-constructed inner
   kernel.** Today's pattern: synthesise inner source,
   compile inner kernel, THEN call
   `inner.bind_outer_scope(outer)`. The inner is fully
   formed before the parent gets to inspect it; any cross-
   binding that needs structural changes to the inner
   (like "this LHS should write to my shared cell, not
   declare a new node") can't happen — the inner's program
   is already frozen.

2. **Externs declared independently of imports.** An inner
   kernel can declare `extern X: T` whether or not the
   parent has a matching export. Today the mismatch
   produces a runtime no-op
   (`bind_outer_scope` walks parent outputs and only
   attaches when the inner has a matching input —
   missing-extern silently drops the binding). The compiler
   never sees the mismatch.

3. **Multiple paths to inner-kernel creation.** The
   activity layer calls `compile_gk_*` directly; the SRD-13d
   Phase 9 op-template synthesisers build their own
   strings; the comprehension synthesiser does another
   variant; the do-loop synthesiser does yet another.
   Each path knows its own tribal rules for what to extern
   and what to inline; none of them go through a parent
   that could enforce the contract.

The construction protocol below collapses all three into
"the parent's builder is the only way in."

---

## Vocabulary

- **Parent context.** A `ScopeKernel` (in SRD-13e terms) or
  `GkKernel` (today) that owns a compiled program and a
  state. It has `outputs` (named values it exposes) and
  `imports` (named values it expects from its own parent,
  recursively).
- **Sub-context builder.** An opaque value handed out by
  the parent. Borrows the parent for its lifetime;
  accumulates module-matter declarations (bindings, externs,
  outputs, lifecycle modifiers, etc.); cannot be cloned or
  outlive its parent.
- **Module-matter artifact.** The closed, immutable result
  of finalising a builder. A self-describing data structure
  carrying everything needed to compile the child: source
  expressions, declared imports, declared exports, lifecycle
  classifications. Has no live references to the parent —
  it can be moved, persisted, hashed, debug-printed.
- **Spawn.** The parent-side operation that takes a
  module-matter artifact and produces a `ScopeKernel` for
  the child. The single point where cross-bindings are
  resolved: each artifact import is matched against the
  parent's exports; each artifact export is registered as a
  child-only output; cell-backed externs are wired; init
  passes run.
- **Cross-binding.** Any wiring between parent and child
  that requires both sides to agree: shared-cell attachment
  for `shared` writes, import-export type matching, init-
  binding lifecycle propagation. SRD-67 makes every cross-
  binding happen at spawn time, never later.

---

## The construction protocol

### Step 1 — Parent yields a builder

```rust
impl<P> ScopeKernel<P> {
    /// Begin construction of a child sub-context. Takes
    /// an `Arc` of the parent — kernels are already shared
    /// via `Arc` across fibers in the nbrs-activity layer,
    /// so this is the natural shape; an `&self` overload
    /// would force a clone for the builder's lifetime.
    /// The builder is the ONLY way to produce a child
    /// `ScopeModule` for this parent.
    pub fn subcontext_builder(self: Arc<Self>) -> SubcontextBuilder<P>;
}
```

The builder owns an `Arc<ScopeKernel<P>>` for the parent.
The parent's reference count rises by one for the builder's
lifetime; the parent can be passed around freely (other
Arcs unaffected). Dropping the builder releases its arc.

`P` is the parent's module-identity type (per §"Decision 5"
below) — the builder is parameterised by it, so the child
module identity is computed at finalize as a function of
the parent's identity, preserving the typed-handle property
SRD-13e specifies.

### Step 2 — Builder accumulates module matter

```rust
impl<P> SubcontextBuilder<P> {
    /// Declare an import: a name the child's body will
    /// reference, expecting the parent to export it.
    /// At finalize-time, the artifact records this name
    /// and its expected type; at spawn-time, the parent
    /// validates that it does export this name with a
    /// compatible type and lifecycle classification.
    pub fn import(&mut self, spec: ImportSpec) -> &mut Self;

    /// Declare an export: a named value this child
    /// produces, available to its own descendants. Modifier
    /// (`final` / `shared` / none) classifies its
    /// lifecycle; `shared` exports get cell-backed slots.
    pub fn export(&mut self, spec: ExportSpec) -> &mut Self;

    /// Add a body fragment. Free identifiers in the
    /// fragment will be resolved against this scope's
    /// imports and prior body fragments at finalize-time.
    pub fn body(&mut self, fragment: BodyFragment) -> &mut Self;

    /// Diagnostic context for compile errors and runtime
    /// panics — file path, line range, originating SRD.
    pub fn context(&mut self, ctx: SourceContext) -> &mut Self;

    /// Register a wrapper / dispenser consumer that pulls
    /// named GK values at runtime. Equivalent to the
    /// pre-SRD-67 `ScopeFixture::register_consumer` —
    /// folded into the builder so init-time pull-plan
    /// accumulation goes through the same single surface
    /// as everything else. The seal-into-PullPlan step
    /// happens at `finalize()` alongside compile.
    pub fn register_pull(&mut self, consumer: PullConsumer)
        -> &mut Self;

    /// Close the builder. Validates that imports match the
    /// parent's exports (types + modifiers + lifecycle
    /// classifications) and that body fragments compile
    /// against the resolved import set + prior body
    /// fragments. Seals the registered pull consumers
    /// into the artifact's `PullPlan`. Returns a closed,
    /// immutable `ScopeModule`; the parent arc held by
    /// the builder is released.
    pub fn finalize(self) -> Result<ScopeModule<Child<P>>, ContractViolation>;
}

/// Body fragments — the two shapes a builder accepts.
pub enum BodyFragment {
    /// User-facing GK source (the content of `bindings:`
    /// / `result:` block strings). Parsed into
    /// `Vec<Statement>` at finalize.
    GkSource(String),

    /// Pre-parsed statements, for synthesisers that
    /// construct GK programmatically (the activity-layer
    /// scope synthesisers under SRD-13d, the comprehension
    /// walker, etc.).
    Statements(Vec<crate::dsl::ast::Statement>),
}
```

Notes:

- `Statement` is reused from `nbrs-variates/src/dsl/ast.rs`
  — no parallel enum. Synthesisers that already produce
  `Statement`s can submit them directly without round-
  tripping through GK source strings.
- `Child<P>` is a phantom-marker type that brands the
  resulting `ScopeModule`'s identity as "a child of `P`."
  Spawn returns `ScopeKernel<Child<P>>`, distinct from any
  sibling's `ScopeKernel<Child<P>>` instance at the value
  level (each spawn produces a fresh kernel with a fresh
  state) but type-compatible at the module-identity level
  (handles bound to one sibling can't be applied to
  another — the value-level distinction enforces that even
  when types coincide).

Notes:

- The builder's methods take `&mut self` and return
  `&mut Self`, so the builder pattern composes fluently.
- `finalize` consumes the builder — the parent borrow ends
  at this point. The artifact is now a free-standing value.
- Validation happens at `finalize`, not at each `body`
  call. The body might reference identifiers that aren't
  yet declared as imports (the imports could be added
  later); finalize is the point where everything must
  resolve.

### Step 3 — Artifact is a closed value

The closed artifact IS SRD-13e's `ScopeModule` type — same
data, single name across both SRDs. SRD-67 retains
"module matter" as descriptive vocabulary for what the
type carries, not as a competing type name.

```rust
pub struct ScopeModule<M> {
    /// Imports (typed) the child requires.
    imports: Vec<ImportSpec>,

    /// Exports (typed) the child produces.
    exports: Vec<ExportSpec>,

    /// Compiled program plus the typed handle bundle
    /// (SRD-13e §1.2).
    program: Arc<GkProgram>,
    contract: ScopeContract<M>,

    /// Diagnostic context.
    context: SourceContext,
}
```

`M` is the module-identity type — `Child<P>` for a module
built under parent `P` via this protocol. The phantom
parameter makes spawn's return type
`ScopeKernel<Child<P>>` distinguishable from any other
spawn target.

Key properties:

- **No live reference to the parent.** The artifact
  encodes everything needed for the parent to spawn — type
  contracts, the compiled program, the contract handles —
  but it doesn't borrow the parent. The parent's arc that
  the builder held is released at finalize. Artifacts can
  be moved, stored in a `Vec`, hashed for caching,
  serialised for inspection.
- **Self-describing.** Anyone holding a `ScopeModule<M>`
  can inspect its imports without running the spawn.
  Useful for `nbrs describe` tooling and for caching
  identical artifacts across many parent instances (e.g.
  one op-template module artifact spawning under one
  parent per fiber).
- **Immutable.** No `add_binding` after finalize. Anything
  the child needs to know is captured.

### Step 4 — Parent spawns the child kernel

```rust
impl<P> ScopeKernel<P> {
    /// Construct a child kernel from a `ScopeModule`
    /// artifact. This is the SINGLE place where cross-
    /// bindings are resolved:
    ///
    /// - Each artifact import is matched against `self`'s
    ///   exports. Type / lifecycle mismatches surface here
    ///   as a `ContractViolation`.
    /// - Cell-backed externs (artifact imports where
    ///   `self` exports a `shared`-modifier output) get
    ///   their `SharedCell` attached.
    /// - Imports flagged `compile-const` get folded into
    ///   the child's compile-time constants (parent's
    ///   `final` exports are sufficient sources here).
    /// - The child's `ScopeInit` lifecycle nodes evaluate
    ///   once, after extern wiring, before the kernel is
    ///   handed back. This replaces the "remember to call
    ///   init pull after bind_outer_scope" pattern with
    ///   an unconditional spawn-time step.
    /// - The scope-coordinates path is computed and frozen.
    /// - The parent's named-child registry records the
    ///   spawn under `name`; a second spawn with the same
    ///   `name` is a `ContractViolation::DuplicateChild`.
    ///   See §"Named-child registry" below.
    ///
    /// `name` identifies the LOGICAL sub-kernel — typically
    /// the workload yaml's phase / op-template / scope
    /// label. It's the unit at which "spawn this once"
    /// applies. Per-fiber GkState cloning happens via the
    /// existing GK API on the returned `ScopeKernel`, NOT
    /// via re-spawn (see §"Compile once, spawn once,
    /// fiber-state separately").
    ///
    /// Returns the spawned child as a `ScopeKernel<Child<P>>`.
    /// The returned kernel's module-identity type is bound
    /// to the parent's via `Child<P>`; handles issued by
    /// the spawned kernel can't be applied against a
    /// sibling spawn at the type level.
    pub fn spawn(
        self: &Arc<Self>,
        name: ChildName,
        artifact: ScopeModule<Child<P>>,
    ) -> Result<ScopeKernel<Child<P>>, ContractViolation>;
}
```

Spawn takes `&Arc<Self>` so the parent's arc isn't moved
into spawn — the spawned child holds its own arc to the
parent (for shared-cell binding lifetimes), and the caller
keeps using the parent for further spawns of sibling
children.

`ChildName` is a structured identifier (`PathBuf`-shaped:
hierarchical, comparable, debug-printable). The runtime
typically constructs it from the workload's scope-tree
node label.

---

## Compile once, spawn once, fiber-state separately

A `ScopeModule<M>` is compiled exactly once per logical
scope — at workload-load time, by the
synthesisers / parsers that produce it. Spawn is exactly
once per parent → named-child relationship. Per-fiber
state is a separate concern handled by the existing GK
API:

| Concern               | Cardinality                      | API                                |
|-----------------------|----------------------------------|------------------------------------|
| Compile               | Once per logical scope           | `SubcontextBuilder::finalize()`    |
| Spawn                 | Once per (parent, named-child)   | `ScopeKernel::spawn(name, module)` |
| Per-fiber GkState     | Once per (fiber, kernel)         | Existing GK state-clone on the returned `ScopeKernel<M>` |

Spawn is NOT called per fiber. The spawned `ScopeKernel<M>`
carries an `Arc<GkProgram>` (the compiled program — shared
freely across fibers) plus a "canonical" `GkState`; each
fiber receives its own state clone via the existing
per-fiber clone machinery, not via a fresh spawn.

This separation matters for two reasons:

1. **Cross-binding shouldn't repeat.** Spawn's contract-
   resolution work (cell attachment, init pulls, scope-
   coord freeze) is per-scope, not per-fiber. Re-spawning
   per fiber would redo all of it for no benefit and
   risk inconsistency across fibers if the parent state
   shifted between spawns.
2. **Module identity is per logical scope.** Two fibers
   running the same op-template see the same logical
   `ScopeKernel<Child<P>>`. They diverge only at the
   GkState level, which is the right boundary — typed
   handles issued at spawn time stay valid across all
   fibers' state instances.

The named-child registry below makes this discipline
enforceable: re-spawn would be caught at the API boundary,
not as a subtle correctness drift downstream.

---

## Named-child registry

Each parent `ScopeKernel<P>` maintains an internal
registry of names it has spawned children under. The
registry is part of the parent's structural state, not a
side-channel; it lives alongside the parent's exports and
shared cells.

### Spawn semantics

- `parent.spawn(name, module)` records `name` in the
  registry. The recorded entry includes the spawn site's
  diagnostic context (so the duplicate diagnostic can
  point to both spawns).
- A second `parent.spawn(name, _)` with the same `name`
  returns `ContractViolation::DuplicateChild { name,
  prior_site, this_site }`. The runtime never silently
  overwrites; the operator must either pick a different
  name or drop the prior child first (see "release"
  below).

### Release semantics

A spawned child can be released (e.g. when a scope ends
and the kernel goes out of use):

```rust
impl<P> ScopeKernel<P> {
    /// Drop the named child from this parent's registry.
    /// The child kernel itself is unaffected — only the
    /// registry entry. After release, the same name may
    /// be spawned again (typical for scope-tree
    /// re-traversal across iterations).
    pub fn release_child(&self, name: &ChildName);
}
```

For comprehension scopes that iterate (e.g. `for_each`
re-spawning the inner scope per iteration tuple), the
loop releases the prior child before spawning the next:

```rust
for tuple in iter {
    if let Some(prior) = prior_child {
        parent.release_child(prior.name());
        drop(prior);
    }
    let child = parent.spawn(tuple.scope_name(), module.clone())?;
    ...
}
```

This is explicit by design: per-iteration re-spawn is a
valid pattern (same logical module, fresh per-iteration
state); but the writer must opt in by releasing first.
The default — silently allowing duplicate spawn — is the
shape that produced the bug class this SRD is solving.

### Why named, not anonymous

Two reasons:

1. **Diagnostics.** When a workload misconfiguration
   produces a duplicate spawn, the error names which
   logical sub-kernel collided. Without names, the only
   diagnostic available is "two spawns under the same
   parent" with no way to identify which one is the
   accident.
2. **Cross-cutting tooling.** `nbrs describe op` and
   similar walks the scope tree by name. Aligning the
   spawn-time name with the workload yaml's scope label
   means the protocol's registry IS the discovery
   surface; no parallel "what scopes exist" map needed.

`ChildName` should match the scope-tree node naming used
elsewhere in nbrs-activity (per SRD-13d / SRD-18b's scope-
tree pre-walk). The runtime constructs the name
structurally; user-visible identifiers (phase names, op-
template names, iteration coords) compose into it.

`spawn` is the chokepoint. Every cross-binding decision
happens here. The parent has full information: it knows
its own exports, it sees every artifact import, it can
detect that `artifact.exports` contains a name that's also
one of `self.exports` and route the child's write through
the parent's shared cell rather than declare a new local
output.

This is what unsticks the SRD-66 collision:

- Workload root has `shared X := false` (export with
  `shared` modifier).
- Op-template artifact declares an export `X := <expr>`
  (the result-wire assignment).
- At spawn time, the parent recognises `X` as a name it
  itself exports with `shared`. Instead of letting the
  child declare a new output `__port_X`, the parent rewrites
  the spawn to route the child's `X := <expr>` evaluation
  to a write-through node that calls
  `parent.shared_cell(X).set(value)` per cycle.
- The duplicate-port error is structurally impossible
  because the parent owns the resolution; there's no path
  by which the child can declare `X` independently.

---

## Cross-binding rules

The spawn step applies these rules in order; each is a
hard error if it fails.

### Rule 1 — Import resolution

Every artifact import name must exist as an export on the
parent (or a transitive ancestor reachable through the
import chain). If not: `ContractViolation::UnboundImport`
naming the artifact's source context, the import name, and
the closest match among parent exports.

Type / modifier compatibility:

| Artifact import | Parent export                     | Resolution                           |
|-----------------|-----------------------------------|--------------------------------------|
| `final X: T`    | `final X: T` (or compile-const)   | Fold parent's value into child       |
| `extern X: T`   | any modifier, type T              | Wire input slot to parent value      |
| `shared X: T`   | `shared X: T`                     | Share cell                           |
| `shared X: T`   | non-shared `X: T`                 | Error — `ContractViolation::Modifier`|
| `extern X: T`   | export `X: U` (T ≠ U, no widen)   | Error — `ContractViolation::Type`    |

### Rule 2 — Export collision with parent

When an artifact export `X` matches a parent export `X`
(same name), spawn checks the parent's modifier:

- Parent `shared X := init` + child `X := <expr>` →
  spawn rewrites the child's assignment as a write-
  through to the parent's shared cell. The child's local
  output table does NOT include `X`; reads of `X` in the
  child resolve to the parent's cell-backed value (via the
  cell's read path, picking up the latest write).
- Parent `final X := ...` + child `X := <expr>` →
  `ContractViolation::FinalShadow`. The child can't
  redefine an immutable parent export.
- Parent has no `X` export, child exports `X` →
  child-only export, registered locally.

This rule is the crux of SRD-66's blocker: the parent
recognises that the child's `X := <expr>` is a write-
through to the shared cell, not a duplicate declaration.

### Rule 3 — Lifecycle propagation

`ScopeInit` lifecycle nodes in the child run once at
spawn, after extern wiring (so the inits see post-bind
values, not the compile-time fold default). This replaces
the "post-bind init pull" pattern in
`OpBuilder::create_fiber_builder` with a spawn-time step.

`ScopeShared` lifecycle nodes — the `shared X := init`
declarations on the child itself — are materialised at
spawn: a `SharedCell` is created on the child's input
slot, initialised to the literal, and exposed as a child
export with `shared` modifier so the child's own children
can bind cells against it.

### Rule 4 — Coordinate / iteration externs

When an artifact body references a name classified by the
parent as a `Coordinate` export (iteration variable in a
comprehension), spawn routes the read through the parent's
coordinate buffer rather than declaring a child-side
extern slot. This codifies the SRD-13d Phase 9 followup
fix that today is a manual rule in the synthesiser.

### Rule 5 — Closure-binding economy

An artifact import is materialised as a child-side input
slot ONLY if the artifact's body actually references it.
The compiler walks the body's free identifiers; unused
imports are dropped at spawn (not ignored — dropped, with
no slot, no per-cycle write). This is the
"closure bindings only where gk module matter detects
linkages" rule from SRD-66 §"Compilation lifecycle."

---

## Walled-off invariant

The `nbrs-variates` crate's public API exposes ONLY:

- `ScopeKernel<M>` (the typed kernel handle).
- `Arc<ScopeKernel<P>>::subcontext_builder() -> SubcontextBuilder<P>`.
- `SubcontextBuilder<P>` methods: `import`, `export`,
  `body`, `context`, `register_pull`, `finalize`.
- `ScopeModule<M>` (the closed artifact, defined by SRD-13e).
- `ScopeKernel<P>::spawn(name, module) -> ScopeKernel<Child<P>>`.
- `ScopeKernel<P>::release_child(name)`.
- `ChildName`, `PullConsumer`, `ImportSpec` / `ExportSpec`
  / `BodyFragment` / `SourceContext` / `ContractViolation`
  / `Child<P>` / `ScopeContract<M>` (re-exported from SRD-
  13e where applicable).

It does **NOT** expose:

- `GkKernel::new` / `GkKernel::compile` style direct
  constructors that bypass the builder.
- `bind_outer_scope` as public surface (becomes
  `pub(crate)`, called only from `spawn`).
- `from_program` as public surface (becomes
  `pub(crate)`, called only from `spawn`).
- Any `set_input` path on a kernel that hasn't been
  spawned through the protocol.

The crate's tests can use the internal surfaces; consumers
(including `nbrs-activity`) can't. This is the "walled-off"
property the user's guidelines call for.

Direct compile-from-string (`compile_gk(src)`) stays as a
test / scratch utility but produces a kernel with no
parent — the result can't be used as a child. To use it
as a parent, you call `subcontext_builder()` on it like
any other kernel. To use it as a child, you compile via
the builder.

---

## What disappears

When this SRD lands, the following call sites — each of
which is a manual cross-binding implementation — collapse
into `parent.subcontext_builder() ... .spawn()`:

1. `nbrs-activity/src/synthesis.rs::build_op_template_scope_kernel`
   — string-concatenated externs, hand-rolled bind, manual
   init pull.
2. `nbrs-activity/src/scope_tree.rs::synthesize_for_each_scope`
   — comprehension scope synthesis with iteration-var
   externs.
3. `nbrs-activity/src/scope_tree.rs::build_do_loop_scope_kernel`
   — do-loop scope synthesis with counter externs.
4. `nbrs-activity/src/scope.rs::build_scope` — phase scope
   synthesis with cascade externs.
5. The `bind_outer_scope` + `propagate_parent_inputs` +
   `mark_inherited_outputs` + post-bind init-pull dance in
   `OpBuilder::create_fiber_builder` — all of it lives
   inside `spawn` after this SRD.

Each of these becomes a "fill the builder, finalize,
spawn" sequence:

```rust
// `parent_kernel: Arc<ScopeKernel<WorkloadRoot>>`
let mut b = parent_kernel.clone().subcontext_builder();
b.context(SourceContext::for_phase(phase_name));
for (name, ty) in iter_vars {
    b.export(ExportSpec::iter_var(name, ty));
}
b.body(BodyFragment::GkSource(phase.bindings.as_str().into()));
// Fixture / pull-plan accumulation happens through the
// same builder — `register_pull` replaces SRD-32's
// separate `ScopeFixture::register_consumer`.
for consumer in dispenser_pull_consumers(phase) {
    b.register_pull(consumer);
}
let phase_module: ScopeModule<Child<WorkloadRoot>> = b.finalize()?;
let phase_kernel: ScopeKernel<Child<WorkloadRoot>> =
    parent_kernel.spawn(ChildName::phase(phase_name), phase_module)?;
```

The type chain (`WorkloadRoot` → `Child<WorkloadRoot>`)
makes the parent/child relationship visible at the type
level. Handles issued by `phase_kernel` carry
`Child<WorkloadRoot>` and can't be applied to a sibling
spawn (different `phase_kernel` instance) thanks to the
SRD-13e typed-handle property — value-level identity
catches what type-level identity alone can't.

No more strings. No more "remember to call X after Y". No
more synthesiser-specific tribal rules.

---

## Lifecycle boundary contract

The protocol enforces these lifecycle boundaries by
construction:

| Boundary             | Before SRD-67                                         | After SRD-67                                          |
|----------------------|-------------------------------------------------------|-------------------------------------------------------|
| Parent active        | Implicit (no relationship)                            | Explicit — builder borrows `&parent`                  |
| Module matter open   | While string is being built up                        | While builder exists                                  |
| Module matter closed | When `compile_gk` returns                             | At `builder.finalize()`                               |
| Child constructed    | Some time after compile, after `bind_outer_scope`     | At `parent.spawn(artifact)`                           |
| Cross-bindings live  | After `bind_outer_scope` (and later `set_input` calls)| At spawn return — single moment                       |
| Child handles valid  | While the program object exists                       | While the parent and child kernels both exist (typed) |

The "single moment" property is the load-bearing one. Today
cross-bindings can be added, modified, or omitted at any
point after compile; that means any code path that gets a
kernel handle can affect the binding shape. After SRD-67,
the only place cross-bindings happen is `spawn`, and any
mutation after that is a violation of the kernel's frozen
contract.

---

## Composition with SRD-13e

SRD-13e specifies the `ScopeModule` data shape and typed
contracts. SRD-67 specifies the construction API.

The mapping is direct:

- `ScopeModule<M>` is the same type in both SRDs — SRD-13e
  defines it; SRD-67 produces it via the builder. SRD-67's
  prose uses "module matter" as descriptive vocabulary
  for what the type carries, but the type name in the
  Rust API is `ScopeModule<M>` (Decision 2).
- SRD-13e's `ImportSpec` / `ExportSpec` / `ScopeContract` /
  `ScopeKernel` types stay as 13e specifies; SRD-67
  re-exports them through its public surface (Decision 3).
- SRD-13e's `ScopeModule::instance_under(parent)` is
  SRD-67's `parent.spawn(artifact)`. Same operation, named
  from the parent's perspective in 67 to emphasise that
  the parent is the agent. The two SRDs converge on a
  single `spawn` method; `instance_under` (if it appears
  in 13e prose) is renamed to match.

If 13e and 67 land together, the implementation merges
their migration plans. If 13e ships first, SRD-67's
contribution is the API surface (builder, walled-off
construction). If 67 ships first, SRD-13e's typed
contracts layer on top of 67's API once the
`ImportSpec` / `ExportSpec` types land.

---

## Composition with SRD-66

The motivating use case. After SRD-67 lands:

1. Workload root compiles as a top-level
   `ScopeKernel<WorkloadRoot>`.
   `shared has_sai_column_indexes := false` becomes an
   export with `shared` modifier — a `SharedCell` is
   created at workload-root construction.
2. The `detect_dialect` phase's `ScopeModule
   <Child<WorkloadRoot>>` is built via
   `workload_root.clone().subcontext_builder()`. The
   builder records `result: |\n
   has_sai_column_indexes := …` as exports.
3. At `workload_root.spawn(detect_dialect_module)`,
   Rule 2 (export collision with parent) fires:
   `has_sai_column_indexes` exists as a parent `shared`
   export. The phase module's `:= <expr>` is rewritten
   as a write-through to the parent's shared cell.
4. Per cycle, the phase's expression evaluates and the
   value flows to the parent's cell.
5. The `await_index` phase compiles as another
   `ScopeModule<Child<WorkloadRoot>>` and spawns under the
   same workload root. Its `pick(has_sai_column_indexes,
   …)` reads from the parent cell, which now carries the
   probe's value.

No `extern` declarations needed in the workload YAML. No
duplicate-port errors. No special-case for the result-wire
write path — it's the same Rule 2 that handles every
shared-cell write. The type chain makes the
detect-then-consume ordering visible at the API layer:
both phase kernels share `Child<WorkloadRoot>` so handles
can flow between them via the parent's exports.

---

## Migration plan

### Phase 1 — Add the surface (additive)

- Implement `SubcontextBuilder<P>`, `ScopeModule<M>` (if
  not already landed by SRD-13e), `BodyFragment`,
  `Child<P>` phantom marker, `ChildName`, `PullConsumer`,
  the parent's named-child registry, `ScopeKernel<P>
  ::subcontext_builder`, `ScopeKernel<P>::spawn`,
  `ScopeKernel<P>::release_child` in `nbrs-variates`.
- The implementation reuses the existing `compile_gk`,
  `bind_outer_scope`, `from_program` machinery internally.
  No semantic change; the builder is a typed shim.
- `bind_outer_scope` and friends stay public temporarily
  for the existing call sites to keep working.
- `ScopeFixture` (SRD-32) gets `pub(crate)` /
  deprecation-flag treatment; its `register_consumer`
  bridges to `SubcontextBuilder::register_pull` for the
  call sites that haven't migrated yet.

### Phase 2 — Migrate one synthesiser

Pick the simplest one — probably `build_do_loop_scope_kernel`
— and rewrite it to go through the builder. Validate that
the runtime behaviour is byte-identical.

### Phase 3 — Migrate the rest

`build_op_template_scope_kernel`, `synthesize_for_each_scope`,
`build_scope`, the `OpBuilder` post-bind dance. Each is its
own commit. Per-step validation against existing test
suites.

### Phase 4 — Lock the door

`bind_outer_scope`, `from_program`, direct `compile_gk`
become `pub(crate)`. The `nbrs-activity` crate compiles
only against the new public surface.

### Phase 5 — Wire SRD-66's gk-call form

With the protocol in place, the result-wire kernel-driven
path is a small additional builder method —
`SubcontextBuilder::add_result_bindings(source)` — that
parses the result-bindings and adds the LHS names as
exports. Spawn applies Rule 2 to route shared writes; the
duplicate-port collision is gone by construction.

---

## Strict mode

Per SRD-15, the following promote from warn → error under
`--strict`:

- An artifact with an unused import (declared but not
  referenced in the body). Today the closure-binding
  economy silently drops these; under strict, declaring
  an unused import is a hint that the workload author
  expected something to bind that doesn't.
- An artifact with an export that no descendant ever
  imports (workload-resolve-time check). Suggests an
  unused declaration.

Always-error, strict-independent:

- Unbound import (Rule 1).
- Type mismatch on import (Rule 1).
- Final-shadow on export (Rule 2).
- Direct construction of a kernel bypassing the protocol
  — caught at compile time by the public-API constraint
  (the constructors aren't exposed).

---

## Out of scope

- **Persistence of `ScopeModule<M>` artifacts.** Caching
  artifacts across runs (for resume / hot-reload) is a
  future concern. The artifact is `Serialize`-friendly by
  construction, but the serialisation format and cache
  invalidation rules belong in their own SRD.
- **Multi-parent spawn.** A child with two parents (e.g.
  inheriting from both the workload root and a sibling
  phase's exports) is not in scope. The protocol assumes
  a single parent at spawn time.
- **Hot rebinding.** Changing the parent of a constructed
  child is impossible by construction. If a workload needs
  a child to bind against a different parent, it spawns a
  fresh child from a fresh artifact under the new parent.
- **Direct GK source compilation as a public surface.**
  `compile_gk(src)` stays for tests, but produces a parent-
  less kernel that can only be used as a parent (via
  `subcontext_builder()`), never a child. Workload
  consumers always go through the builder.

---

## Decisions made

1. **Builder pattern stays.** The user's original
   guidelines explicitly named "get a GK sub-context
   builder from the parent" as step 1 — the fluent
   construction surface IS the design, not a wrapper. The
   builder lets imports / exports / body fragments be
   added incrementally (matters for synthesisers that
   discover declarations during a walk) at the cost of
   one extra type beyond `ScopeModule` itself.

2. **Single name: `ScopeModule`.** Same type as SRD-13e.
   SRD-67's prose uses "module matter" as descriptive
   vocabulary for the type's contents, but the type name
   in the Rust API is `ScopeModule<M>`. Avoids the
   parallel-names-for-the-same-thing trap.

3. **`ImportSpec` / `ExportSpec` re-exported from
   SRD-13e.** The typed contract types are 13e's
   contribution; SRD-67 re-exports them rather than
   redefining. One source of truth; one place where
   contract evolution happens.

4. **`BodyFragment` is a small enum.**
   `GkSource(String)` for user-facing `bindings:` /
   `result:` content (parsed at finalize), and
   `Statements(Vec<Statement>)` for synthesisers that
   produce GK programmatically (the existing comprehension
   / do-loop / op-template walkers). Reuses
   `nbrs-variates/src/dsl/ast.rs::Statement` directly — no
   parallel enum.

5. **Parent ref: `Arc<ScopeKernel<P>>` only.** Single API
   signature on `subcontext_builder`. Kernels in
   `nbrs-activity` are already shared via `Arc` across
   fibers; an `&self` overload would force a clone for
   the builder's lifetime. Callers that have `&kernel`
   wrap with `Arc::new(...)` (rare) or use the existing
   `Arc<ScopeKernel<P>>` (common case). The single shape
   keeps the public surface minimal.

6. **`spawn` returns `ScopeKernel<Child<P>>` (typed by
   module identity).** Inherits SRD-13e's typed-handle
   safety property: handles issued by one spawn can't be
   applied to a sibling spawn at the type level. The
   `Child<P>` phantom marker brands the child's identity
   as a function of the parent's; spawn's return type is
   a function of the parent type. Builder construction
   carries `P` through so the type chain stays intact.

7. **`ScopeFixture` (SRD-32) folds into the builder.**
   Pre-SRD-67 the fixture was a separate init-time
   accumulator owned by the wrapper-construction site;
   under this SRD it's `SubcontextBuilder::register_pull`,
   sharing the builder's lifecycle and seal point. There
   is no `ScopeFixture` type after this SRD lands. The
   pull-plan output is sealed into the artifact at
   `finalize()` and made available through the spawned
   kernel's typed handle bundle.

8. **Compile once, spawn once, fiber-state separate.**
   `ScopeModule<M>` is compiled exactly once per logical
   scope; `spawn` is exactly once per (parent, named-
   child); per-fiber `GkState` cloning happens via the
   existing GK API on the spawned kernel, NOT via re-
   spawn. Conflating these would re-run cross-binding
   work per fiber — wasteful and risk-prone if the parent
   shifted between spawns. See §"Compile once, spawn
   once, fiber-state separately" for the load-bearing
   table.

9. **Named-child registry; duplicate spawn is an error.**
   Each parent tracks the names it has spawned children
   under. A second spawn with the same name returns
   `ContractViolation::DuplicateChild`, naming both
   spawn sites. Comprehension iteration explicitly
   releases the prior child before spawning the next
   tuple; per-iteration re-spawn is a valid pattern but
   must opt in by calling `release_child(name)`. The
   default — silently allowing duplicate spawn — is the
   shape that produces the bug class this SRD addresses.

---

## Open questions

(All three open items from the prior draft were resolved
by review feedback and folded into the body of the SRD as
load-bearing rules. They're recorded here as the decision
trail.)

1. ~~Builder vs `ScopeFixture` duplication~~ — **resolved.**
   `ScopeFixture` is absorbed into `SubcontextBuilder`.
   See §"Construction protocol §Step 2" for the merged
   surface and §"What disappears" for the call-site
   collapse. There is no `ScopeFixture` type after this
   SRD lands; the builder is the single init-time
   accumulator.
2. ~~Compile vs spawn cost; caching~~ — **resolved.**
   `ScopeModule<M>` is compiled once per logical scope.
   Spawn is the per-scope-instance step that produces a
   `ScopeKernel<M>`. Per-fiber concerns are orthogonal —
   they're handled by GK's existing program / state
   separation (`GkProgram` shared, `GkState` cloned per
   fiber); spawn is NOT called per fiber. See §"Compile
   once, spawn once, fiber-state separately" for the
   load-bearing rule.
3. ~~Parallel spawn concurrency~~ — **resolved.**
   Recast as a safety constraint: duplicate spawn of the
   same logical sub-kernel name under the same parent is
   an error. Parents track their named children
   internally. See §"Named-child registry" for the rule.

---

## See also

- SRD-11 — GK evaluation lifecycles (the
  `compile-const` / `scope-init` / `dynamic` taxonomy).
- SRD-13c — Scope model (today's `bind_outer_scope`
  surface).
- SRD-13d — Op-template scope (a current consumer of the
  ad-hoc protocol).
- SRD-13e — Scope-as-Module (the typed contracts SRD-67's
  protocol operates on).
- SRD-16 — Mutability rules (`shared` semantics, the
  cell-backed export lifecycle).
- SRD-32 — Init-time fixture / pull plan (the read-side
  surface). SRD-67 absorbs `ScopeFixture` into
  `SubcontextBuilder::register_pull`; SRD-32's pull-plan
  spec (the data shape, the per-consumer contract) stays
  as 32 specifies. The accumulator-side surface unifies;
  the plan-side data shape doesn't change.
- SRD-66 — Runtime feature detection (the motivating
  consumer; result-wires write to outer-scope `shared`
  via Rule 2).
