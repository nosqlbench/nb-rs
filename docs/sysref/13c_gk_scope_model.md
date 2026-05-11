# 13c: GK Scope Model

How GK kernels compose across lifecycle boundaries (phases,
`for_each` iterations, scope groups) with visibility,
mutability, and isolation rules. Subordinate to SRD 13b
(GK Combination Modes) — this is the in-depth specification
of the **scope-composition** mode (#2 of the four modes).
Read 13b first if you're unsure which composition mode
applies to your situation.

---

## Principles

1. **The GK API provides scope composition primitives.**
   `GkKernel::bind_outer_scope()` copies matching constant
   values from an outer kernel into an inner kernel's extern
   inputs. `GkKernel::scope_values()` extracts bound values
   for fiber replication. `GkProgram` carries output modifier
   metadata (`shared`, `final`) so callers can query scope
   behavior. These are standard API methods — callers invoke
   them at scope boundaries without interpreting GK internals.

2. **Each scope is a standard GK kernel.** An inner scope is
   just a `GkProgram` + `GkState` that happens to have some
   `extern` inputs whose values come from an outer scope's
   outputs. The kernel doesn't know or care where the values
   come from.

3. **Callers wire scopes via the GK API.** The phase runner
   (or any other caller) calls `bind_outer_scope()` and
   `scope_values()` to connect kernels at lifecycle
   boundaries. The GK API does the name matching and value
   copying. The caller decides *when* to wire, not *how*.

4. **No runtime delegation chains inside GK.** The inner
   kernel does NOT hold a reference to the outer kernel. It
   has extern inputs that the GK scope API populates at
   construction time. From the kernel's perspective, they're
   ordinary inputs.

---

## Scope Hierarchy

```
Workload scope
  GkProgram_outer + GkState_outer
  Outputs: dim, base_count, profiles, ...
  │
  └── Phase scope (or for_each iteration)
        GkProgram_inner + GkState_inner
        Extern inputs: dim, base_count (populated from outer outputs)
        Own bindings: id, train_vector, ...
        Outputs: id, train_vector, dim (read-through), base_count (read-through)
```

The inner kernel is compiled from the phase's own `bindings:`
block. Names referenced in the phase ops that aren't defined
in the phase bindings are auto-declared as `extern` inputs,
typed from the outer scope's output manifest.

### What is NOT a scope boundary

Ops, stanzas, and cycles are evaluations within the enclosing
scope — they do not create new GK contexts. Each op dispenser
holds a reference to the enclosing scope's `GkProgram`. Op-level
`bindings:` blocks augment the enclosing scope's DAG at compile
time. They cannot shadow enclosing names (compile error).

The only exception is standalone const expressions
(`eval_const_expr`) which have no parent scope.

---

## No Flattening, No Duplication

The inner kernel does NOT include the outer scope's source
text. It does NOT re-fold the outer scope's constants.

Instead:
1. The outer kernel is compiled and folded normally.
2. Its **output manifest** — `[(name, PortType, modifier)]` —
   is extracted. This is metadata about what the outer scope
   produces, used to generate `extern` declarations.
3. When compiling the inner kernel, any name referenced but
   not defined in the inner bindings is looked up in the
   outer manifest. If found, it becomes an `extern` input
   on the inner kernel with the matching type.
4. The GK API provides `bind_outer_scope()` on `GkKernel`,
   which copies matching constant values from the outer
   kernel's outputs into the inner kernel's extern inputs.
   `scope_values()` extracts the bound values for replication
   across fibers. From the kernel's perspective, these are
   ordinary input values — the API handles name matching and
   value copying. Callers (e.g., the phase runner) invoke
   these methods at scope boundaries but do not themselves
   interpret or manage GK internal state.

Result: the inner kernel is small (only its own nodes), its
constants are already resolved (copied from outer), and there
is no source duplication or redundant compilation.

---

## Visibility Rules

### Default: Read-Through

By default, all outer scope outputs are visible to the inner
scope as read-only values. The inner scope can reference them
in its bindings and op templates without declaring them.

```yaml
# Workload level
bindings: |
  inputs := (cycle)
  dim := vector_dim("{dataset}")

phases:
  rampup:
    bindings: |
      inputs := (cycle)
      train_vector := vector_at(cycle, "{dataset}")
    ops:
      insert:
        # {dim} comes from outer scope — no declaration needed
        stmt: "INSERT INTO t (id, vec) VALUES ({id}, {train_vector})"
```

The compiler auto-generates `extern dim: u64` in the inner
kernel because `dim` is referenced but not defined.

### Shadowing

An inner scope can define a name that exists in the outer scope.
The inner definition wins within the inner scope. The outer
scope is unaffected.

```yaml
phases:
  rampup:
    bindings: |
      inputs := (cycle)
      # Shadows workload-level dim with profile-specific dim
      dim := vector_dim("{dataset}:{pname}")
```

Shadowing is implicit — defining a name in the inner scope
shadows the outer name. No special keyword needed.

### Explicit Occlude Prevention

To prevent accidental shadowing, a workload can mark bindings
as `final`:

```
bindings: |
  inputs := (cycle)
  final dim := vector_dim("{dataset}")
```

A `final` binding cannot be shadowed by inner scopes. Attempting
to redefine it in a phase `bindings:` block is a compile error.

---

## Mutability Rules

### Default: Immutable Propagation

> **Superseded by [SRD-13f §"Architectural model"](13f_cross_scope_wire_materialization.md#architectural-model)
> + [§"Plan to true-up"](13f_cross_scope_wire_materialization.md#plan-to-true-up).**
> The snapshot-at-scope-creation rule below was the legacy
> default; SRD-13f reframes it. The read invariant is uniform
> across all visible cross-scope wires — inner reads return
> outer's current value via construction-time wiring (cells
> for shared, value-only cells / per-cycle refresh for
> non-shared). "Snapshot" is one specific materialization
> choice the matter interpreter makes for strictly-constant
> wires, not a general visibility rule. The `shared` modifier
> controls only the *write* path; read mediation is uniform.
> The "Plan to true-up" section in SRD-13f documents the
> push-by-push migration (Pushes A–F) that landed the
> SRD-13f model in code; all pushes are shipped as of the
> SRD-13f §"Plan to true-up" status board.

Outer scope values are propagated to the inner scope as
**immutable snapshot inputs**. The inner scope reads them but
cannot modify them. The values are copied once at inner scope
creation, not per-cycle.

### Shared Mutable

A binding declared `shared` in the outer scope is writable
from inner scopes. Writes propagate back to the outer state.

```
bindings: |
  inputs := (cycle)
  shared error_budget := 100
```

Inner scope mutations to `error_budget` become visible to
the outer scope and to subsequent inner scopes.

#### Implementation: SharedCell-backed input slots

Storage is unified across kernels via `SharedCell` —
`Arc<Mutex<Value>>` — attached to input slots. The
mechanism:

1. **Compile.** `shared X := <literal>` (cycle-binding form
   with `Shared` modifier and a literal RHS) compiles to an
   input slot for `X` with the literal as initial value, plus
   a passthrough output `X` reading from that slot, with the
   output marked `Shared`. Storage is identical to `extern X:
   type = literal` — the modifier carries the cross-scope
   intent.

2. **Outer construction.** `GkKernel::new_with_inputs` and
   `from_program` call `seed_shared_cells` after the
   modifier pipeline runs (`set_output_modifiers`). Every
   `Shared`-modifier output that has a backing input slot
   gets a fresh `SharedCell` initialized from the slot's
   current value, attached to outer's state.

3. **Bind.** `inner.bind_outer_scope(&outer)` looks at outer's
   shared cells (`outer.state.shared_cell(idx)`) and attaches
   the same `Arc` to inner's matching input slots. Reads at
   bind time are taken from the cell. Both kernels now hold
   clones of the same `Arc`.

4. **Write through.** `inner.state.set_input(idx, val)` on a
   shared-cell-backed slot does two things: writes to inner's
   local snapshot (for fast reads inside inner's eval), and
   writes through the `Mutex` to the cell. Other kernels
   sharing the cell don't see the update in their local
   snapshots until they refresh.

5. **Read intrinsically.** `kernel.lookup(name)` and the
   per-cycle eval path both read through the cell on every
   access — no explicit "refresh" step. The cell is the
   canonical storage for shared slots; `eval_node` queries
   it via `EngineCore::read_input` during input gathering, so
   evaluations against a kernel pick up sibling writes
   automatically. Dispatchers don't need to know which slots
   are shared.

`bind_outer_scope` and the cell wiring replace the earlier
explicit `propagate_shared_to` round-trip — writes flow
through shared storage automatically and reads see them
intrinsically. No scope-exit copy, no refresh step, no
dispatcher-side bookkeeping.

#### Concurrent semantics: last-write-wins

The `Mutex` serializes concurrent writers. The current
semantic is **last-write-wins by lock-acquisition order** —
no merging, no atomic-fetch-add, no aggregation. For the
canonical `shared error_budget := 100` example with two
workers each decrementing on errors, two concurrent writes
of `99` produce a final value of `99`, not `98` —
read-modify-write is not atomic at this level.

This is the documented contract for now. Templated patterns
(see §"Open: per-binding sharing pattern templates" below)
are the path to atomic counters, sum-reduction, set-merge,
and other semantics that this baseline doesn't deliver.

#### Open: per-binding sharing pattern templates

The shipped `SharedCell` mechanism delivers **last-write-wins**
across concurrent writers. That's correct for some patterns
(latest-status flags, coalesced metadata) and wrong for
others (atomic counters, summed totals, merged sets).

The canonical example — `shared error_budget := 100`
decremented by many workers — wants `fetch_sub` atomic
semantics, not a lock. With the current Mutex-based shim,
two concurrent decrements of an unrelated cell write the
same `99` and lose one decrement.

Concurrent shared mutation has more than one viable
semantic, and the right choice is per-binding, not global:

- **Atomic reduction** — `shared count: u64` decremented by
  many workers wants `fetch_sub` semantics. Specialized to
  numeric-monoid types.
- **Mutex with last-write-wins** — `shared latest_status:
  String` where it's fine for one worker's value to
  overwrite another. The current default. Works for any
  type but loses intermediate writes.
- **Coalescing / merge** — `shared seen_keys: Set` where
  writes are unioned, not overwritten. Requires a typed
  merge operator per binding.
- **Aggregation** — `shared total_latency: f64` where each
  iteration's contribution sums into a running total.
  Combiner is the binding's responsibility; the runtime
  invokes it at write time.

These are distinct contracts with distinct correctness
guarantees. A future implementation needs to be **explicit
about templating which one applies per `shared` declaration**
— the syntax / modifier surface should let the user pick:

```
shared(atomic) error_budget: u64 := 100         # fetch_sub
shared(last)   latest_status: String := "init"  # last-write-wins (current default)
shared(merge)  seen_keys: Set := []             # union
shared(sum)    total_latency: f64 := 0.0        # aggregation
```

(Strawman syntax; real spelling is open.) Without this,
`shared` defaults to last-write-wins for all types, which
is right for some workloads and wrong for others.

Until templates land:
- Last-write-wins is the documented contract.
- Workloads that need atomic-counter semantics either run
  sequentially (no race) or accept the last-write-wins loss.
- The four primitive read APIs (`shared_cell`,
  `attach_shared_cell`, `refresh_shared`, `set_input`'s
  write-through) are the building blocks any future
  template would compose against — no rewrite of the
  storage layer is needed when templates are added.

#### Non-literal `shared` initializers

`shared X := <literal>` (numeric, string, or `true`/`false`)
gives X a real input slot and SharedCell. `shared X :=
<non-literal-expression>` keeps the legacy cycle-binding
shape: a computation node tagged with the `Shared` modifier,
no input slot, no cell. The Shared metadata survives but
cross-scope mutability is not active for that binding.

This is a deliberate restriction. A non-literal init
(`shared rolling := hash(cycle)`) doesn't have a sensible
single initial value — the cell would be populated from
some particular cycle's evaluation, and subsequent inner
writes would compete with the per-cycle re-evaluation. The
semantics aren't well-defined yet. Future work either folds
compile-const non-literals (e.g. `shared base :=
mod(hash(0), 1000)`) at compile time, or rejects them with a
clear error.

#### Idempotence and safety (sequential path)

`propagate_shared_to` is safe to call repeatedly: a no-op
when no shared outputs differ from outer's current input
values. Writing through to outer's input slot dirties
outer's dependent nodes per the standard `set_input`
invalidation path; subsequent outer reads re-evaluate. Flow
stays outer → inner → outer (sequential, not circular)
because outer evaluations don't fire during inner's window.

### Inner-Only Mutable

By default, inner scope `extern` inputs are mutable within
the inner scope (via captures), but mutations do NOT propagate
to the outer scope. This is the standard capture behavior.

---

## Scope Lifecycle for `for_each`

A `for_each` phase creates two scope boundaries:

1. **Loop scope** — wraps all iterations. Created once per
   phase execution. Controls how the iteration context is
   seeded from the outer scope.

2. **Iteration scope** — one per iteration. Controls how
   each iteration is seeded from the loop scope.

Two orthogonal knobs configure these:

### `loop_scope: clean|inherit` (default: `clean`)

How the loop context is initialized from the outer scope.

- **`clean`**: snapshot of outer scope at loop entry. The
  loop sees the outer scope's state as it was when the phase
  started, regardless of what prior phases may have modified.

- **`inherit`**: outer scope's live state at loop entry.
  If a prior phase mutated shared state, the loop sees
  those mutations.

```yaml
phases:
  rampup:
    for_each: "pname in ..."
    loop_scope: inherit   # see prior phase mutations
```

### `iter_scope: clean|inherit` (default: `inherit` for `for_each`)

How each iteration is initialized from the loop scope.

- **`inherit`** (default for `for_each`): each iteration
  starts from the loop scope's current state. All loop-level
  variables are implicitly shared with iterations — each
  iteration sees mutations from prior iterations. This makes
  `for_each` iterations behave like a sequential program by
  default.

- **`clean`**: each iteration starts from the loop scope's
  original snapshot. Iterations are fully isolated from each
  other. Use this when iterations must be independent.

```yaml
phases:
  rampup:
    for_each: "pname in ..."
    iter_scope: clean   # override: isolate iterations
```

### Variable sharing within `for_each`

All variables at the for_each loop level are implicitly
shared with iterations. The loop-scoped values form the
iteration's read/write context. This means:

- Iterations can read and write loop-level variables
- With the default `iter_scope: inherit`, iteration N+1
  sees what iteration N wrote
- With `iter_scope: clean`, each iteration gets a fresh
  copy and mutations are discarded

The `shared` keyword is only needed to propagate values
**upward** — from the for_each loop level back to the
enclosing outer scope. Without `shared`, loop-level
mutations stay within the for_each boundary.

### `final`

A binding declared `final` cannot be shadowed or mutated
by inner scopes. Compile error if redefined. Useful for
values that must remain constant across all iterations.

### The 2x2 matrix

| `loop_scope` | `iter_scope` | Behavior |
|---|---|---|
| `clean` | `inherit` | Iters see each other's state, seeded from outer snapshot. **(Default for for_each)** |
| `clean` | `clean` | Fully isolated. Each iter sees outer snapshot. |
| `inherit` | `inherit` | Iters see each other's state, seeded from outer's live state. |
| `inherit` | `clean` | Each iter sees outer's live state. Isolated from siblings. |

---

## Implementation via Existing Mechanisms

### Per-Scope Canonical Kernel Cache

Each non-trivial `ScopeNode` in `nbrs-activity::scope_tree`
carries a `cached_kernel: OnceLock<Arc<GkKernel>>` slot
(M3.1+). The canonical kernel is the *single authoritative
answer* for "what is `<name>` at this scope?" — every name
visible at this scope (own outputs plus parent-inherited values
bound via `bind_outer_scope`) resolves through the standard GK
API on this one kernel. Callers do not walk the scope tree to
do name resolution; GK's auto-extern + outer-scope wiring
already encapsulates the layering.

Per-execution kernels (per-iteration in for_each, per-fiber in
phase) come from `GkKernel::from_program(canonical.program()
.clone())` — the cache-and-rebind primitive whose docstring
references this section directly. The canonical's program is
`Arc`-shared; only state is cloned per execution.

For text interpolation against a kernel's name space, callers
use `nbrs_activity::interpolate::interpolate_via_kernel(text,
&kernel)`. The implementation tries `get_constant` (own
outputs) first, then `get_input` (extern slots populated by
`bind_outer_scope` or the dispatcher's per-clause `set_input`).
That two-step lookup is the runtime expression of the
shadowing rule in §"Visibility Rules" — own bindings shadow
inherited values.

### Output Manifest

The runner extracts the outer scope's output manifest before
the outer kernel is consumed by `OpBuilder`. Each entry
carries name, type, and binding modifier (`shared`/`final`/none):

```rust
struct ManifestEntry {
    name: String,
    port_type: PortType,
    modifier: BindingModifier,
}
```

### Auto-Extern Generation

The runner scans inner-scope ops to find names referenced in
op templates but not defined in inner bindings. For each such
name found in the outer manifest, it prepends an `extern`
declaration to the inner bindings source.

**Final enforcement**: if an inner scope defines a name that
is `final` in the outer manifest, the runner emits an error
and exits. This check happens in `BindingScope::validate()`
(see `scope.rs`) before compilation.

### Structural vs Parametric Detection

Before the iteration loop, the runner checks whether the
`for_each` variable appears in any `BindingsDef::GkSource`
string. If not — only in op field strings — the variable
is **parametric** and the outer kernel is reused across all
iterations (no recompilation). If it appears in bindings,
the variable is **structural** and each iteration compiles
a fresh inner kernel.

### State Wiring

At scope boundaries, callers use the GK scope API:

```rust
// Simple case: bind all matching outer outputs to inner inputs
inner_kernel.bind_outer_scope(&outer_kernel);

// Fine-grained: wire specific scope values (e.g., from carried scope)
for (name, value) in scope_values {
    if let Some(idx) = inner_kernel.program().find_input(name) {
        inner_kernel.state().set_input(idx, value.clone());
    }
}

// Extract bound values for fiber replication
let values = inner_kernel.scope_values();
```

The GK API handles name matching and value copying. The caller
decides when to wire (at phase start, per iteration, etc.).

### Iteration State Carrying (`iter_scope: inherit`)

The runner maintains `iter_carried_scope` — a mutable copy of
the loop scope values. After each iteration (structural only),
the inner kernel's constant-folded outputs are extracted and
merged into `iter_carried_scope`. The next iteration receives
these updated values instead of the original loop snapshot.

All loop-level variables are implicitly shared with iterations.
No `shared` keyword required within the for_each boundary.

### Shared Write-Back (`shared` keyword)

Shared write-back is now a GK API: `inner.propagate_shared_to(&mut outer)`
at each iteration boundary copies inner's `shared`-output values
back into outer's matching input slots. See §"Mutability Rules:
Shared Mutable" above for the full rationale; the rest of this
section describes the pre-API runner pattern that the GK call
replaces.

(Pre-API, the runner did this manually: at the end of each
iteration, scan the outer manifest for outputs marked `shared`,
copy the last iteration's value from `iter_carried_scope` back
into `outer_scope_values`.)

`shared` write-back is just updating an input slot on the
outer state — it cannot cause runaway because:
- The outer DAG is already compiled and constant-folded
- Setting an input doesn't trigger re-evaluation
- Flow is always outer → inner → outer (sequential, not circular)

### Loop Scope Seeding (`loop_scope`)

The runner captures `original_scope_values` (the workload
compilation snapshot) before any phase runs. At loop entry:
- `clean`: seeds from `original_scope_values` (ignores shared write-back)
- `inherit`: seeds from current `outer_scope_values` (includes shared mutations from prior phases)

### Modifier Pipeline

Binding modifiers flow through the full compilation pipeline:

1. **Lexer**: `shared` and `final` keywords → `Shared`/`Final` tokens
2. **Parser**: `shared name := expr` / `final name := expr` → `BindingModifier` on AST nodes
3. **Compiler**: sets `asm.set_output_modifier(name, modifier)` per binding
4. **Assembler**: carries `output_modifiers` through `ResolvedDag`
5. **GkKernel**: applies modifiers via `set_output_modifiers()` before Arc sharing
6. **GkProgram**: stores `output_modifiers: HashMap<String, BindingModifier>`
7. **Runner**: queries `program.output_modifier(name)` for manifest extraction

Combined forms are supported: `shared init name = expr` and
`final init name = expr` (modifier before `init` keyword).

---

## Variable Partitioning

### Structural vs Parametric

Some variables affect DAG topology (which nodes exist):
- `train_vector := vector_at(cycle, "example:label-1")`
- The source string `"example:label-1"` is a node constructor
  argument — changing it changes which node is instantiated.

Other variables only affect values flowing through a fixed DAG:
- `{keyspace}` in a SQL template — string substitution, no
  node change.

**Structural variables** require recompilation when they change.
**Parametric variables** can be GK inputs — no recompilation.

The `for_each` iteration variable is structural when it appears
in bindings source (affects node construction) and parametric
when it only appears in op template fields (string substitution).

The runner determines this automatically:
- If `{var}` appears in any `bindings:` source → structural
  → recompile per iteration
- If `{var}` only appears in op fields → parametric
  → set as input, no recompilation

This optimization avoids unnecessary recompilation for simple
iteration patterns like iterating over table names.

---

## Syntax Summary

| Keyword | Location | Meaning |
|---------|----------|---------|
| `extern name: type` | bindings | Declare input from outer scope |
| `final name := expr` | bindings | Immutable; cannot be shadowed by inner scopes |
| `shared name := expr` | bindings | Mutable; propagates upward to outer scope after for_each |
| `shared init name = expr` | bindings | Combined: shared + scope-init binding (per [SRD 11](11_gk_evaluation.md) §"Init Binding Contract") |
| `final init name = expr` | bindings | Combined: final + scope-init binding (per [SRD 11](11_gk_evaluation.md) §"Init Binding Contract") |
| `loop_scope: clean\|inherit` | phase | How loop is seeded from outer (default: `clean`) |
| `iter_scope: clean\|inherit` | phase | How iteration is seeded from loop (default: `inherit` for for_each, `clean` otherwise) |
| `for_each: "var in expr"` | phase | Iterate, creating per-iteration scope |

---

## How It Works: Plugging Graphs Together

Think of each GK scope as a circuit board with labeled
connectors on the edges. The outer scope has output jacks.
The inner scope has input jacks. The runner plugs wires
between matching names.

```
┌─── Outer Scope ──────────────────────────┐
│                                           │
│  cycle ──▶ hash ──▶ mod ──▶ [dim]  ●────────┐
│                                           │  │
│  cycle ──▶ hash ──▶ count ─▶ [base] ●───────┤
│                                           │  │
└───────────────────────────────────────────┘  │
         output jacks                          │ wires (by name)
┌─── Inner Scope ──────────────────────────┐  │
│                                           │  │
│  ●──── [dim]   (extern input) ◀──────────────┘
│                    │                      │
│  ●──── [base]  (extern input) ◀──────────┘
│                    │
│  cycle ──▶ vector_at ──▶ [train_vec]
│                 │
│           (uses dim for validation)
│                                           │
└───────────────────────────────────────────┘
```

Each board is a standard GK DAG — nodes, wires, inputs,
outputs. The boards don't know about each other. The runner
plugs them together at the boundary.

### The rules are simple

1. **Each scope is its own board.** It has its own nodes,
   its own wiring, its own compilation. It doesn't include
   the other board's circuitry.

2. **Output jacks on the outer board become input jacks on
   the inner board.** If the inner board references `dim`
   but doesn't define it, the compiler adds an input jack
   for `dim` (an `extern` declaration). The runner connects
   the wire at startup.

3. **Values flow downward.** Outer → inner. The runner reads
   the outer board's output, writes it to the inner board's
   input. One-time copy at scope creation, not per-cycle.

4. **Inner boards can shadow outer names.** If the inner
   board defines its own `dim`, it uses that instead. The
   outer board's `dim` is disconnected — no wire plugged.

5. **Mutations stay local** (by default). If the inner board
   changes a value via capture, that change lives on the
   inner board only. The outer board doesn't see it.

### In graph theory terms

This is **DAG composition at a named interface**. Two directed
acyclic graphs are joined by identifying output ports of one
with input ports of the other by name. The composed system
is still acyclic because values only flow outer → inner.

The `extern` declarations are the **interface contract** — they
specify which ports the inner graph expects the outer graph to
provide, and what types they carry. The runner is the
**composition operator** that connects the ports and copies
the values.

Properties that composition preserves:
- **Acyclicity** — outer outputs feed inner inputs, never reverse
- **Determinism** — same outer values + same cycle = same inner outputs
- **Provenance** — the inner graph tracks which of its inputs
  changed, including the extern inputs from the outer scope
- **Constant folding** — outer values are already folded; the
  inner graph receives them as pre-computed input values,
  which fold further if they feed only constant-path nodes

---

## What This Does NOT Change

- `GkNode` trait — unchanged
- `GkProgram` struct — unchanged
- `GkState` struct — unchanged
- `Value` enum — unchanged
- `WireSource` enum — unchanged
- `InputDef` struct — unchanged
- The GK compiler — unchanged (already handles `extern`)
- The provenance system — unchanged
- The evaluation loop — unchanged

All scoping is orchestrated by the runner using existing
kernel APIs. The GK core remains a flat, single-scope
evaluation engine.

---

## Open Design Issue: constant/non-constant duality at the read API

**Status:** unresolved as of 2026-04-30. Documented for later
review; do not paper over with more wrappers.

The current public read surface on `GkKernel` exposes the
storage-strategy split:

- `get_constant(name)` — reads the folded-output buffer.
- `get_input(name)` — reads the input-slot array.
- `lookup(name)` — wraps the two-tier shadowing read
  (`get_constant.or_else(get_input)`) into a single named
  idiom. Internally still two reads.

`lookup` removes the duplicated `or_else` pattern at call
sites (was inlined three times: `interpolate_via_kernel`,
`bind_outer_scope`, `propagate_shared_to`). It does *not*
remove the underlying duality — the caller can still see
`get_constant` and `get_input` on the type, and the kernel
still has two physical storage planes for what is logically
one wire's value.

This is a design smell at the caller surface area. From the
user's perspective, asking "what is the value of `<name>` in
this scope?" should be one question with one read. Today the
caller has three options (`pull`, `lookup`, or the raw
primitives), and `lookup`'s implementation reveals that
"folded constant" and "live input" are distinct things the
runtime tracks separately.

The reasons this isn't trivially collapsible — and the
constraints any future fix must respect:

- **Mutability.** `pull` must take `&mut self` because
  evaluating a dirty node mutates the buffer + clean flags.
  `lookup` is `&self` and never evaluates. Collapsing to one
  method forces a choice: every read takes `&mut`, or
  evaluation moves elsewhere (eager seeding, interior
  mutability, etc.). All three options have downsides
  rejected during this design pass — eager seeding loses
  per-scope buffer reuse, interior mutability violates the
  shared-engine/per-fiber-state split, `&mut` everywhere
  fights Rust aliasing at every callsite.

- **Engine vs state split.** `Arc<GkProgram>` is the engine,
  `GkState` is per-fiber. The scope tree caches the canonical
  state via `Arc<GkKernel>` so multiple readers share the
  seeded folded constants without re-computing. Whatever the
  unified read becomes, it needs to preserve this split.

- **Storage layouts are real and load-bearing.** Folded
  constants live in `state.core.buffers[node][port]`; input
  slots live in `state.core.inputs[idx]`. Auto-passthrough
  outputs (`__port_<name>`) have a name in the output map
  and a wire source pointing at an input slot — their
  "buffer" is empty by design because evaluating an identity
  passthrough is wasted work; the input slot is the truth.
  Any unification has to pick one source of truth or
  reconcile both at read time.

The current `lookup` is a *containment* of the smell, not a
fix. Future work — possibly a redesign of the read path —
should aim to make `kernel.lookup(name)` (or whatever it's
called) the *only* read on the public type, retiring
`get_constant` and `get_input` from the public surface, and
push the storage-strategy choice fully into the kernel's
internals where the caller never sees it.

Anything that adds *more* surface area in the meantime
(e.g., a new method that returns `Option<Value>` for
"computed values" alongside the existing two) makes this
worse, not better.


## Design Rationale

Why these mechanics, given the alternatives.

**Why structural variables require per-iteration compilation?**

GK bindings like `vector_at(cycle, "example:label-1")` need
the dataset source at node construction time — the node
opens a file handle, loads metadata, and preallocates
buffers. This is scope-init work (SRD 11) that can't be
deferred to per-cycle evaluation. Structural variable
substitution before compilation ensures the source string
is a literal that the node constructor can act on.

Different profiles may have different vector counts,
dimensions, or available facets. A shared kernel would
need to handle all profiles simultaneously, which breaks
the "one cycle = one vector ordinal" invariant.
Per-iteration compilation keeps the cycle semantics clean:
cycle 0 is always the first vector in THIS profile, not an
offset into a global index.

**Why parametric variables skip recompilation?**

When the `for_each` variable only appears in op field
strings (e.g., table names in SQL templates), no GK nodes
change between iterations. The DAG topology is identical.
The runner detects this automatically (`Structural vs
Parametric Detection`, above) and reuses the outer kernel,
avoiding unnecessary recompilation for simple iteration
patterns like iterating over table names or keyspaces.

**Why not merge phase bindings with workload bindings?**

Merging creates ambiguity about which definition wins.
Replacement is explicit — if you need workload bindings in
a phase, include them. This makes each phase's GK program
self-contained and readable without tracing inheritance
chains.

**Why auto-extern instead of delegation or flattening?**

Three rejected designs:

- **Delegation** — inner kernel holds a reference to the
  outer kernel and resolves names upward at evaluation
  time. Breaks constant folding (the inner kernel can't
  fold what it can't see at compile time) and complicates
  provenance tracking.
- **Flattening** — duplicate the outer scope's source text
  into every inner scope. Causes redundant compilation and
  node instantiation; makes the per-scope kernel cache
  pointless.
- **Auto-extern** (used) — outer kernel is compiled and
  constant-folded; output manifest exposes names + types +
  modifiers; inner scope's references to undefined names
  become `extern` ports populated by the runner at scope
  creation. Inner kernel stays small (only its own nodes);
  cache friendliness preserved; provenance is local.
