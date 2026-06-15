# Wire Materialization

The mechanism for cross-scope wire flow: architectural model
(one logical graph; scope boundaries partition lifecycle and
access; the uniform read invariant; the write contract) and
the materialization gradient the matter interpreter applies
(inlined constant / value-only shared cell / read-write shared
cell, plus shadow suppression and value-clone economy).

This doc extends axiom-level statements:
- [composition_substrate.md L1 (lifecycle isolation) + S5 (cross-tier write-through) + T1 (typed return)](composition_substrate.md)
- [cross_fiber_invalidation.md (canonical validity-tracking mechanism for shared cells — revision counter + intent vectors + per-fiber `last_seen`)](cross_fiber_invalidation.md)
- [graph_compiler.md CF1-CF4 (Context Fusion) + CF3 (gradient honoring)](graph_compiler.md)
- [runtime_model.md R1 (clean-flag memoization) + R2 (hybrid push/pull invalidation)](runtime_model.md)
- [scope_model.md (visibility rules — the "Default: Immutable Propagation" clause this doc updates)](scope_model.md)
- [subcontext_construction.md (parent-walking lookup; SharedCell write-through)](subcontext_construction.md)

The host-side wire-reference classification synthesizer rule,
implementation status, plan-to-true-up, and open questions are
documented by the host. Related polydat docs:
[engines.md (per-scope canonical kernel cache)](engines.md),
[module_system.md](module_system.md).

---

## What this SRD covers

How a wire defined in an outer scope becomes readable (and
optionally writable) from an inner scope's kernel. Specifically:

- What "wiring an inner kernel to its enclosing scope" means
  semantically, independent of any specific API method name.
- Why the read invariant is "reading on inner returns what
  reading on outer returns" — uniformly, without per-cycle vs.
  not-per-cycle special casing, without the caller composing
  fallback chains.
- What materialization the matter interpreter chooses for each
  visible wire (literal fold, value-only shared cell, read-write
  shared cell) and what determines the choice.
- How the `shared` modifier relates to all of this (write
  permission, *not* read-mediation).

This SRD updates two earlier clauses that have become misaligned
with how the runtime needs to work:

1. **[scope_model.md §"Default: Immutable Propagation"](scope_model.md)** —
   the snapshot-at-scope-creation default does not match the
   invariant that inner reads of an outer-defined wire return
   the *current* outer value. This SRD reframes the default
   around uniform read-live wiring; "snapshot" is one
   *materialization* choice the matter interpreter makes for
   strictly-constant wires, not a general visibility rule.
2. **`bind_outer_scope` as the name of the construction-time
   wiring step** — the name implies a narrow value-copy +
   cell-attach operation, but the actual responsibility is
   general matter-AST interpretation that installs whatever
   wiring each visible wire's matter classification prescribes.
   This SRD specifies the operation; the name has been retired
   in favor of `materialize_wiring_from_outer` (see the nbrs-side
   §"Plan to true-up").

---

## Architectural model

### One logical graph; scope boundaries partition lifecycle and access

The Polydat matter spanning a workload is *one logical graph*. Scope
boundaries (workload, phase, for_each iteration, op-template,
per-fiber) are not value-isolation barriers — they partition:

- **Lifecycle.** A wire's value-instance lifetime is bounded by
  the scope that owns it (phase scope owns phase bindings; fiber
  state owns per-cycle coordinates; etc.). Outer scopes outlive
  inner; inner scope ends release inner-owned wires.
- **Access plane.** Each scope's kernel exposes a *local handle*
  for every wire it is permitted to read (and a write handle for
  every wire it is permitted to write to). The handle is the
  inner-side surface for the same logical wire defined further
  out; reading the handle returns the current value of the
  logical wire.

The wire's *identity* is preserved across scopes. The wire's
*materialization* on each scope's kernel — whether the value is
inlined as a constant, stored in a cell, or wired to chain to an
upstream pull — is an implementation detail of how the matter
interpreter materializes the access plane on that scope.

### The read invariant

> Reading an inner-side handle for a cross-scope wire returns the
> same value that reading the wire on its owning kernel would
> return at the same moment.

This is uniform. It does not depend on whether the wire is
per-cycle, per-iteration, constant, or otherwise. It does not
depend on whether the wire is `shared`. The matter interpreter is
responsible for materializing each handle so the invariant holds
without the *caller* (the wires layer, the dispenser, an adapter)
doing anything beyond a local read.

### The write contract

> Writing through an inner-side handle is permitted only when the
> wire's matter classifies it as `shared` (or equivalent
> cross-scope-writable form). When permitted, the write goes
> through the same storage the reads observe, mutex-gated,
> last-write-wins. Other scopes' readers see the new value on
> subsequent reads.

The `shared` modifier is purely a write-permission flag. It does
*not* control read-mediation — read access is uniform.

### Local reads are O(1)

Inner kernel reads do not walk scope chains. The local kernel's
slot for the handle is the read surface. Whatever wiring backs
the slot (literal constant, shared cell with valid bit, value-only
cell with subscription to upstream invalidation) is set up at
*construction*. Reads check the slot's validity and return the
value, taking no special path through ancestor kernels at read
time. Recomputable wires use the existing per-node valid-bit
discipline extended across the scope boundary so invalidation
propagates without read-time traversal.

---

## Materialization gradient

The matter interpreter chooses one of these forms for each
visible cross-scope wire at inner kernel construction. The
choice is dictated by the wire's matter classification, *not*
by the caller.

### Inlined constant (compile-time fold)

When the outer wire's value is statically known (literal RHS,
folded const bindings) and no intermediate scope might
legitimately want to *shadow* it (the bindings's lexical layer
in SRD-18), the matter interpreter *inlines the value into the
inner program* as a `const` constant. No cell, no slot, no
valid bit — the value is part of the inner kernel's compiled
artifact. Reads are direct constant lookups.

This is the materialization for:

- Author-declared workload `bindings: | const X := <literal>`
  where the author asserts compile-time const semantics.
- Outer `const X := <literal>` declarations from any scope
  whose chain-position is known not to be a shadow site.
- Folded const bindings whose value resolved at compile time.

**Workload parameters use the chain-wired form instead.** A
separate **params-kernel** sits at the root of the chain
(below workload-root) and holds one `const NAME := <literal>`
per workload param. The workload-root program emits
`extern NAME: T` for each param and is marked
`inherited_outputs` for those names so they cascade through as
auto-passthrough slots without being treated as workload-root's
own iteration coordinates. Every descendant scope (phase, op-
template, comprehension, do-loop, scenario-tree bindings)
likewise externs them. The value flows from the params-kernel
through each scope's input-slot wiring via
`materialize_wiring_from_outer`.

The motivation for the indirection is **lexical shadowing**: a
scenario-tree `bindings:` (or its `set:` sugar form) needs to
be able to redeclare a workload-param name for its subtree
without rewriting the workload-root program. With the params-
kernel design, the SetParam scope sits as a lexical layer
between params-kernel and any descendant, its local
`const NAME := <override>` shadows the chain-cascaded value, and
descendants resolve NAME via the standard `extern NAME` lookup
through the chain. Without the indirection, the workload-root's
folded `const NAME := <literal>` would short-circuit lookup
via get_constant before the chain wiring is consulted, and the
override would be silently masked.

A previous design baked `const NAME := <literal>` into the
workload-root program for every workload param, which works
for the common case but makes scenario-tree shadowing
unimplementable; the current design is a deliberate trade-off
in favor of the more general lexical-scope semantics.

### Value-only shared cell

When the outer wire is recomputable (non-literal RHS, depends on
inputs that change) but is read-only from the inner scope's
perspective, the matter interpreter installs a *value-only shared
cell*: shared storage between outer and inner, mutex-protected
for concurrent reader safety, paired with a `revision: AtomicU64`
counter. Inner's local slot is wired to read through this cell.
Outer's per-cycle re-eval of the wire writes its new value into
the cell, bumps the revision (Release), and sets the cell's
intent bit on the defining scope's intent-dirty vector. Every
consumer fiber's cone walker observes the change on its next
read via the bulk-mask + per-cell-revision compare protocol
specified in [cross_fiber_invalidation.md].

The inner side has no write surface to this cell.

This is the materialization for:

- Phase bindings that descendants reference (e.g., `load :=
  add(cycle, 1)`).
- Any computed binding visible across scope boundaries that
  isn't `shared`.

### Read-write shared cell (mutex)

When the matter classifies the wire as `shared` (or an
equivalent cross-scope-mutable form), the matter interpreter
installs a read-write shared cell. Storage is shared with the
defining scope's kernel; both sides hold local handles backed by
the same cell; the mutex serializes writes; the cell's
`revision: AtomicU64` counter and the defining scope's intent-
dirty bit are bumped on every write per
[cross_fiber_invalidation.md]. Inner writes are visible to outer
(and to siblings sharing the cell) on next read, without any
host-side refresh ceremony.

This is the materialization for:

- `shared X := <literal>` declarations.
- For_each iteration variables (treated as shared internally
  today by polydat's comprehension synthesis path — the same
  surface that backs polydat spec §9.5's `scope_once`).
- Any other matter that explicitly opts into cross-scope
  write-back.

### Why the gradient is matter-driven, not caller-driven

The caller (the scope synthesizer, the wires layer, the
dispenser) does not pick the materialization. The matter AST
classifies each wire; the matter interpreter materializes
accordingly. This:

- Removes the "is this wire per-cycle or not" question from
  every caller site — the matter knows.
- Eliminates external chain composition (e.g.,
  `CycleWires::with_fallback`) — the inner kernel's local read
  is correct on its own because the matter set the wiring up.
- Lets the same wire's materialization change (literal →
  computed) without touching consumers — only the matter and
  interpreter change.

### Local-authoritative shadow (transit suppression)

When an inner scope declares `const NAME := …` or
`const NAME := …` for a name that is *also* exported by an outer
scope in the chain (as a folded constant or a passthrough
output), the inner declaration is the new authoritative writer
for that name over its subtree. The chain must not carry the
upstream value past this scope, or descendants would read it
instead of the local declaration.

The materialize step enforces this through **transit
suppression**: during step 1 (cell cascade), any shared cell
visible at the outer scope whose name matches a local
const output on `self` is dropped on the floor — not
attached to a self slot, not transit-forwarded to descendants.
Step 2's value-copy path then runs normally and copies the
outer's view via `self.lookup(name)` into the self slot
(for non-shadow names) or skips the name entirely (for
shadow names, because `self`'s own folded buffer / init
binding owns it).

This is the same mechanism every scope-tree node uses —
phases, op-templates, comprehensions, do-loops, scenario-tree
bindings. The synthesizer doesn't need to coordinate; the
materializer enforces the invariant uniformly. The result is
standard lexical-scope shadowing: the closest declaration
wins, transit cells stop at the first redeclaration, and the
chain remains self-consistent.

This is the mechanism that makes scenario-tree
`bindings:` / `set:` actually shadow workload params — see
SRD-18 §"bindings: (scenario level) and the set: sugar form"
for the surface, this section for the underlying rule.

### Value-clone economy on the chain

When materialize-wiring value-copies an outer scope's output
into an inner scope's input slot (the "no cell, plain
passthrough" path), the cost depends on which `Value` variant
the chain is carrying. Every non-primitive variant is Arc-
backed:

- `Str(Arc<str>)`
- `Bytes(Arc<[u8]>)`
- `Json(Arc<serde_json::Value>)`
- `VecF32(SliceArc<f32>)` / `VecI32(SliceArc<i32>)`
- `Handle(Arc<dyn Any + Send + Sync>)`

All clone via `Arc::clone` — one atomic increment, no heap
allocation. Primitive variants (U64, F64, Bool) memcpy.

For per-cycle reads from the input slot via `read_input`
(`engines.rs::EngineCore::read_input`), the same economy
applies: the slot's stored `Value` is cloned to hand back to
the caller. A workload referencing the same `const` / `const`
wire across 30k cycles/sec produces ~30k atomic increments
and zero heap allocations on that wire, regardless of the
variant carried. The implementation aligns with what the
grammar says: declarations signaling shareability map to
types that clone cheaply.

The Json variant being Arc-backed matters specifically for
result-body wire paths: when a host produces a row-
shaped JSON body and downstream nodes extract multiple
columns from it (`body_column_i32`, `body_column_str`, …),
each consumer reads the body wire per cycle. Pre-Arc that was
one full deep-clone of the JSON tree per column per cycle;
post-Arc it's one atomic increment. The two consumers that
need an owned `serde_json::Value` — adapters that mutate the
body before serializing, validation paths that walk and
transform — explicitly deep-clone via `(*v).clone()` at the
consume site (`Value::to_json_value` does this for the
`Json(...)` variant).
