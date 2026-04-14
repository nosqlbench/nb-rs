# 16: GK Scope Model

How GK kernels compose across lifecycle boundaries (phases,
for_each iterations) with visibility, mutability, and isolation
rules.

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

Inner scope mutations to `error_budget` are visible to the
outer scope and to subsequent inner scopes (if sequential).

Implementation: the runner maps `error_budget` to a shared
input slot. `set_input()` on the inner state writes through
to the outer state's input slot. Provenance invalidation
propagates normally.

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
and exits. This check happens in `generate_auto_externs()`
before compilation.

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

After a `for_each` loop completes, the runner scans the outer
manifest for outputs marked `shared`. For each, it copies the
last iteration's value from `iter_carried_scope` back into
`outer_scope_values`. This makes shared mutations visible to
subsequent phases.

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
- `train_vector := vector_at(cycle, "sift1m:label-1")`
- The source string `"sift1m:label-1"` is a node constructor
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
| `shared init name = expr` | bindings | Combined: shared + init-time binding |
| `final init name = expr` | bindings | Combined: final + init-time binding |
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
