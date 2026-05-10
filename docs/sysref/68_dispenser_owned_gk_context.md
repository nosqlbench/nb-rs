# SRD-68 — Dispenser-Owned GK Context and Single-Surface Resolution

**Status:** 2026-05-10 — **complete**. Pushes 1–5 shipped end-to-end:
single resolution surface at cycle time, dispenser-owned canonical
kernels with construction-time structural resolution, per-fiber
fan-out via standard `build_subscope`, all three CQL modes (raw,
prepared, batch) migrated to wires, every adapter exposes
`canonical_kernel()`, all workload-model mutation paths retired
(`resolve_placeholders_via_kernel` and
`resolve_placeholders_in_params_only` both deleted). The
`OpDispenser::describe()` diagnostic returns truly pristine yaml
text — every `{name}` placeholder intact. Initial motivation: the
`full_cql_vector.yaml` workload hit an unresolved-bindpoint failure
that surfaced as a misleading "op-template" diagnostic; investigation
showed the existing two-pass substitution (workload-load text
mutation + cycle-time bind point resolve) violated the "GK is
canonical scope" axiom by maintaining a parallel resolution path
against a different kernel from the one the dispenser logically owns.
The completed migration moves cycle-time reads to a single resolution
surface — the dispenser-owned per-fiber kernel via the narrow
`WireSource` trait — and moves construction-time structural resolution
(workload params, iter vars) into the adapter's `map_op` (and the
validation wrapper's `wrap`) against its own canonical kernel.

**Owner:** nbrs-activity (synthesis, executor, scope, dispenser
construction), nbrs-adapter-* (every adapter's `map_op`).

**Cross-refs:** SRD-13c (scope model — `bind_outer_scope`, manifest
extraction), SRD-13d (op-template scope layer), SRD-13e
(scope-as-module typed contracts), SRD-30 (adapter interface), SRD-31
(op execution pipeline), SRD-67 (parent-gated GK sub-context
construction — the construction protocol this SRD applies at the
dispenser layer).

---

## What this SRD specifies

How an op dispenser owns its GK context, how that context relates to
the dispenser's enclosing scope and to per-fiber state, and how
op-template bind points get resolved at cycle time.

SRD-67 says *how* a child GK kernel comes into existence as a function
of a parent. SRD-68 says *the dispenser is one of those construction
sites*: when a dispenser is built, it is the call site that
materialises a child GK context (or reuses the parent's, when no
matter is added) and from that point forward owns the canonical
kernel reference for op-template-scope name resolution.

This is the architectural follow-through of the SRD-67 invariants
applied to the adapter layer. It also collapses several existing
parallel structures (`OpBuilder::op_template_kernels` HashMap, the
synthesis-layer `substitute_bind_points*` functions, the
text-mutating half of `resolve_placeholders_via_kernel`) into the
standard subcontext mechanism.

---

## Load-bearing invariants

These are axioms. A design that contradicts one of these is wrong,
not the rule.

### I-1. One kernel per dispenser

A dispenser has **exactly one** GK context for op-template-scope
name resolution: the canonical kernel materialised at its
construction. There is no fallback kernel, no cascade across
parent kernels, no name-keyed lookup table that maps op names to
kernels. Every `{name}` reference in the dispenser's op template
resolves through that one kernel — or fails as an unresolved
bindpoint error.

If a name the dispenser needs isn't reachable through its kernel,
the bug is in matter assembly during dispenser construction, not in
the read site. Adding a fallback kernel to the read path is a
documented anti-pattern.

### I-2. Narrow read surface

The dispenser accesses its kernel only through a narrow
read-by-name trait (`WireSource`). The dispenser does not see
`GkKernel` internals — no `program()`, no `state()`, no
`scope_coordinates()`, no metadata APIs. The trait is the wall
between adapter code and GK runtime mechanics; what the dispenser
needs from its context is a value lookup by name and (for
introspection / diagnostics) a list of declared names.

### I-3. Dispenser construction uses two-phase materialisation

The dispenser is constructed with a `SubcontextBuilder`, not a
ready-made kernel. Construction has two phases:

1. **Matter assembly.** The dispenser walks its op-template
   structure (statement text bind-point references, phase
   `bindings:` block, `result:` block, `metrics:`, `delay:`,
   `condition:`, anything else GK-relevant) and adds the
   collected matter to the builder.

2. **Materialisation.** The builder materialises the kernel. If
   matter was added, a new subscope kernel layered on the parent
   is produced. If matter was empty, the builder returns a
   reference to the parent's kernel — no allocation. This is
   exactly the SRD-67 path; the dispenser is just another caller.

The dispenser stores the resulting canonical kernel reference and
the pristine op-template text. It performs any one-time
initialisation that depends on the kernel (e.g., CQL prepared
statement compilation, where structural bind points are
text-substituted into the SQL the dispenser hands to `prepare()`)
during construction, against its own kernel through the narrow
trait.

### I-4. Per-fiber fan-out via the standard subscope mechanism

A dispenser is built once per phase activation. Fibers spawn after
that and live for the phase's duration. Each fiber, at startup,
walks the dispenser registry and calls `build_subscope` on each
dispenser's canonical kernel to produce a per-fiber kernel
instance. The fiber holds these as `Vec<GkKernel>` indexed parallel
to the dispenser list.

There is no manual lookup table from op name to fiber state.
Per-fiber kernel instancing reuses the same SRD-67 mechanism every
other subscope uses; the dispenser layer doesn't get its own
parallel infrastructure.

At cycle time, the executor passes the firing fiber's kernel
slot for the firing dispenser into `ExecCtx`. The dispenser
reads through `WireSource` against that slot. Different fibers
have different state; no aliasing, no locking on the hot path.

### I-5. Dispenser-init is the prepared-statement compilation site

CQL `prepared:` mode (and any analogous adapter-specific
ahead-of-cycle work) compiles its underlying form **once at
dispenser construction**, using the canonical kernel via the
narrow trait. Structural bind points (`{table}`, `{keyspace}`)
that the kernel can answer at construction time are
text-substituted into the SQL before `prepare()` is called.
Value bind points become `?` markers; their names are recorded in
declaration order for cycle-time `?`-binding.

Cycle-time binding is purely value lookup against per-fiber state;
no text substitution runs per cycle for prepared mode. For raw
mode, cycle-time render walks the pristine template and asks the
narrow trait for each `{name}`.

### I-6. Workload-load pre-flight is non-mutating

Pre-flight bindpoint validation walks every op text + every op
template's prospective matter and confirms each `{name}`
resolves through what the dispenser's canonical kernel will
expose. It accumulates errors and returns a `Result`. It does
not mutate `op[key]` strings. The mutation half of the prior
`resolve_placeholders_via_kernel` is removed; the validation
half remains as a pure validator.

---

## What this replaces

Before SRD-68 the resolution path looked like this:

```
parse YAML → ParsedOp.op[key] = pristine text
  ↓
phase activation:
  resolve_placeholders_via_kernel(&mut ops, &parent_kernel)
    — mutates op[key], substituting workload params, iter vars,
      ancestor scope outputs in place; defers per-cycle bindings
  ↓
adapter.map_op(template)
  — reads partially-substituted text from template.op[key]
  — stores it on the dispenser
  — has no GK context
  ↓
OpBuilder synthesis:
  op_template_kernels: HashMap<String, GkKernel>
  op_template_programs: HashMap<String, Arc<GkProgram>>
  per-fiber instancing walks the maps
  ↓
cycle execution:
  fiber resolves "what kernel does this op use?" via name lookup
  synthesis::resolve_cached → substitute_bind_points_with_state
    — runs against main_kernel by default (with my recent fallback
      to op-template kernel for phase bindings)
    — different kernel than the one used at phase activation
```

After SRD-68:

```
parse YAML → ParsedOp.op[key] = pristine text (never mutated)
  ↓
phase activation:
  validate_placeholders(&ops, &parent_subcontext) — pure read
  ↓
adapter.map_op(template, &parent_subcontext_builder)
  — assembles its op-template GK matter
  — materialises canonical kernel (or reuses parent reference)
  — performs init-time work against its kernel via WireSource
  — stores: pristine text + canonical kernel ref
  ↓
fiber spawn:
  fiber walks dispenser registry, build_subscope per dispenser,
  holds Vec<GkKernel> indexed parallel to dispensers
  ↓
cycle execution:
  ExecCtx { wires: &dyn WireSource, ... } points at the firing
  dispenser's per-fiber kernel slot
  dispenser reads names via WireSource — single resolution surface
```

Removed structures (over the migration):

- `OpBuilder::op_template_kernels: HashMap<String, GkKernel>`
- `OpBuilder::op_template_programs: HashMap<String, Arc<GkProgram>>`
- `OpBuilder::commit_op_template_write_throughs(op_name)`
- `FiberBuilder::resolve_pulls_for_op(op_name, …)`
- `synthesis::substitute_bind_points` and
  `synthesis::substitute_bind_points_with_state`
- `synthesis::resolve_cached`'s "is the name on the op-template
  kernel?" branch
- `scope::resolve_placeholders_via_kernel`'s text-mutation half
  (validation half remains, factored out as
  `validate_placeholders_via_kernel`)

---

## The narrow trait

```rust
/// Cycle-time read surface a dispenser uses to resolve names from
/// its bound GK context. The wall between adapter code and GK
/// runtime internals: dispensers see this and only this.
pub trait WireSource: Send + Sync {
    /// Get the current value of `name` in the dispenser's kernel.
    /// Returns None if `name` isn't declared in this scope —
    /// callers treat that as a resolution error, not a fallback
    /// opportunity (I-1).
    fn get(&self, name: &str) -> Option<&Value>;

    /// Iterate declared names. Used by validators and by the
    /// `describe_resolved` introspection path; not for hot-path
    /// cycle reads.
    fn names(&self) -> Box<dyn Iterator<Item = &str> + '_>;
}
```

`GkKernel` implements `WireSource`. The implementation pulls
through the kernel's existing `lookup` chain (input slots,
outputs, inherited scope state via `bind_outer_scope`) — single
entry point, single resolution surface.

Dispensers store nothing more than `Arc<GkKernel>` (the canonical
kernel) plus their own state. `&dyn WireSource` is what reaches
the read site through `ExecCtx`.

---

## ExecCtx surface

```rust
pub struct ExecCtx<'a> {
    pub wires: &'a dyn WireSource,
    pub fields: &'a ResolvedFields,   // [retained during migration]
    pub pulls:  &'a ResolvedPulls,    // [retained during migration]
    // ...
}
```

Phase 1 of the migration adds `wires` alongside the existing
`fields`/`pulls` so adapters can move at their own pace. The end
state has `fields`/`pulls` either deleted or reduced to a typed
value-binding carrier for prepared `?`-binding (where the value
needs to flow as a typed `Value`, not a string). Synthesis-layer
text-substitution machinery is gone; adapters use `wires` for all
op-template name resolution.

---

## Adapter API surface

```rust
trait DriverAdapter {
    fn map_op(
        &self,
        template: &ParsedOp,
        parent_subcontext: &SubcontextBuilder,
    ) -> Result<Box<dyn OpDispenser>, String>;
}
```

The new `parent_subcontext` parameter is a SRD-67
`SubcontextBuilder` rooted at the phase-scope kernel. Adapters
that need no GK matter of their own simply ignore it (the
materialisation step returns the parent kernel reference).
Adapters with op-template matter (CQL prepared, anything that
honours `bindings:` / `result:` declarations attached to the op)
add their matter to the builder and materialise.

The signature change is mechanical for adapters with no matter
(HTTP, stdout, plotter, openapi, testkit pass through). CQL is
the only adapter that materially uses the new parameter in the
initial migration.

---

## Lifecycles, drop ordering, exception safety

- **Construction order:** dispensers are built after phase
  activation's parent kernel is populated; matter assembly is the
  hard chokepoint — the canonical kernel program shape is frozen
  by the end of construction. No late binding.

- **Fiber spawn order:** fibers spawn after all dispensers exist.
  A fiber's `Vec<GkKernel>` is built in one pass over the
  dispenser registry; index discipline is enforced by both sides
  iterating the same ordered slice.

- **Drop:** `Arc<GkProgram>` keeps the program alive until the
  last per-fiber kernel referencing it drops. Fibers drop
  naturally at phase teardown; dispensers drop when the activity
  unwinds. No manual lifetime management.

- **Mid-cycle panic:** caught by the existing `catch_unwind` at
  `activity.rs::execute_one_cycle`. Fiber breaks, its per-fiber
  state goes with it. State is fiber-owned, so no cross-fiber
  contamination. Preserved unchanged.

- **Phase-end teardown:** fibers drain → drop their per-fiber
  kernel `Vec` → dispensers' canonical kernels' refcount drops →
  programs drop. Standard refcount discipline.

---

## Diagnostic surface

`OpDispenser::describe()` returns the pristine op-template text —
the operator's words from the workload yaml, unmutated. Match
back to the source line by line is trivial.

`OpDispenser::describe_resolved(wires: &dyn WireSource)` (the
SRD-68 form supersedes the `&ResolvedFields`-taking shape from
the interim migration step) renders against the per-fiber kernel
for the failing cycle — the dryrun-equivalent view of what the
dispenser actually sent. Adapters render their own shape: CQL
inline-substitutes `?` with the bound value's display form, etc.

The error reporter at `activity.rs::execute_one_cycle` calls both
and renders two lines:

```
op-template: <pristine yaml form, all bind points intact>
op-resolved: <fully interpolated form for this cycle's failure>
```

The diagnostic is **always honest**: pristine matches the source,
resolved matches the wire.

---

## Pushes

Each push leaves the tree green; the system is functional at every
boundary.

### Push 1 — `WireSource` trait + `ExecCtx.wires`
- Define `WireSource` on `nbrs-activity::adapter`.
- Implement for `GkKernel`.
- Add `wires: &dyn WireSource` to `ExecCtx`.
- Adapters keep their existing `fields`/`pulls` paths; new field
  unused. Pure additive.
- Tests: existing suites pass unchanged; one new test covers
  `WireSource` round-trip on `GkKernel`.

### Push 2 — `map_op` takes `SubcontextBuilder`; CQL adapter migrates
- `DriverAdapter::map_op` signature grows the
  `&SubcontextBuilder` parameter.
- Every adapter passes through (no behavior change for adapters
  with no matter).
- CQL `map_op` actually uses it: assembles op-template matter
  (phase bindings, result-bindings — what currently lives in
  per-op programs), materialises canonical kernel, stores it on
  the dispenser. Prepared init runs against the kernel through
  `WireSource`. `describe()` returns pristine text.
  `describe_resolved` renders through `WireSource` against the
  per-fiber kernel handed in via `ExecCtx`.
- The `op_template_kernels` HashMap in `OpBuilder` still
  populated and consulted by non-CQL paths — no global cutover.
- Tests: CQL workload `full_cql_vector.yaml` produces a clean
  `op-template:` line with all bind points intact + a correct
  `op-resolved:` line for a failing cycle.

### Push 3 — Per-fiber fan-out via `build_subscope` from canonical kernels
- Fiber spawn walks dispenser registry, calls `build_subscope` on
  each dispenser's canonical kernel, stores `Vec<GkKernel>`
  indexed parallel to dispensers.
- Executor hands the firing slot into `ExecCtx.wires` at cycle
  time.
- CQL adapter switches off the legacy `fields`/`pulls` plumbing
  (still works for non-CQL adapters via the legacy synthesis
  path).
- The `op_template_kernels` HashMap in `OpBuilder` is deprecated
  but not deleted; non-CQL adapters still consult it.
- Tests: CQL workload runs end-to-end on the new fan-out;
  multi-fiber concurrent execution preserves per-fiber state
  isolation; cross-op captures within a stanza still flow
  correctly.

### Push 4 — Migrate remaining adapters
- HTTP, stdout, plotter, openapi, testkit each migrate their
  `map_op` to the new shape. Most pass through (no matter).
- After the last adapter migrates: delete the
  `OpBuilder::op_template_kernels` and
  `OpBuilder::op_template_programs` maps,
  `commit_op_template_write_throughs(op_name)`, and
  `FiberBuilder::resolve_pulls_for_op(op_name, …)`.
- Tests: full workspace + integration suite green.

### Push 5 — Phase-scope canonical + cycle-time wires + validator + LUT delete

This is the consumer-side migration. Held back when an attempt at the
naive form surfaced an iter-var inheritance gap that the SRD didn't
originally call out.

**Iter-var inheritance gap.** The dispenser's canonical kernel must
inherit from the *phase scope kernel* (with iter vars from for_each
scopes already populated), not from the workload-root source kernel.
Iter vars like `optimize_for`, `table`, `profile` are scope outputs
of for_each comprehensions that sit between workload root and phase.
The legacy synthesis pipeline carries them via `scope_values` into
its per-fiber op_template_kernels; the dispenser-owned model needs
the same value flow into its canonical.

This means either:
- The `parent` argument to `map_op` becomes the phase-scope kernel
  (whatever the synthesis pipeline currently uses to build per-op
  programs against), OR
- `OpBuilder::canonical_kernel_for_op` consults `scope_values` and
  applies them to the canonical's input slots before handing off.

**Sub-pushes:**

5a. **Phase-scope canonical** — wire iter-var values into the
    canonical kernel chain. Verify a unit test that
    `cycle_wires.get(iter_var_name)` returns the current iteration's
    value end-to-end.

5b. **CQL execute via wires** — migrate `CqlRawDispenser`,
    `CqlPreparedDispenser`, `CqlBatchDispenser` execute paths from
    `fields.get_str(field_name)` / `fields.get_value(bind_name)` to
    `substitute_via_wires(stmt_template, ctx.wires)` /
    `ctx.wires.get(bind_name)`. Synthesis-layer
    `substitute_bind_points*` then has no CQL callers.

    **Shipped 2026-05-10.** Batch mode follows the user-confirmed
    architectural model: "each iteration of the batch is
    considered another pull, just as if the operation inside the
    batch were separate. It is simply an iteration container."
    `WireSource::advance(coord)` added (default no-op; `CycleWires`
    overrides to mutate the wrapped kernel's coord input). The
    batch dispenser internally iterates rows; for each row,
    `wires.advance(cycle + row)` then `wires.get(bind_name)` per
    bind position.

5c. **resolve_placeholders → validator** — rename
    `resolve_placeholders_via_kernel` to
    `validate_placeholders_via_kernel`. Drop the in-place mutation
    of `op[key]`. The `op-template:` diagnostic line then renders
    the operator's pristine yaml text.

5d. **LUT delete** — drop `OpBuilder::op_template_kernels` HashMap,
    `commit_op_template_write_throughs(op_name)`,
    `FiberBuilder::resolve_pulls_for_op(op_name, …)`,
    `synthesis::substitute_bind_points*`. Replaced by Push 3's
    per-fiber `per_op_kernels: Vec<Option<GkKernel>>` and Push 5b's
    wires-driven cycle reads.

Each sub-push is bounded enough to land + verify in a focused session.

---

## Why this is the right shape

Three load-bearing simplifications fall out:

1. **The dispenser becomes a participant in the SRD-67
   construction protocol** rather than a layer that needs its own
   parallel kernel-management infrastructure. The two-phase
   materialisation pattern handles it.

2. **`describe()` becomes trivial** because the dispenser holds
   the pristine text and never had a reason to overwrite it. The
   prior pristine-snapshot side-table approach was patching over
   the architectural smell of mutating the workload model in
   place.

3. **Per-fiber instancing reuses the standard subscope
   mechanism**, so the system has one mental model for "how do
   you get a per-fiber kernel": build_subscope from the canonical.
   The op-template-kernels map was a bolted-on second mechanism
   doing the same job differently.

The net effect: less code, fewer abstractions, single resolution
surface, honest diagnostics, no parallel LUTs. The system shape
matches the architectural axiom ("GK kernels are the canonical
state holder") at the dispenser layer.
