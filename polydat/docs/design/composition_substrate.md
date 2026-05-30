# The Composition Substrate — Polydat Design

**Subtitle:** Context Synthesis, Type Safety, State Layering.

**Status:** DRAFT — formalises the substrate that makes
polydat's free graph composition work. Names the three
pillars, the axioms under each, and the boundary handlers that
connect them. Where prior SRDs (10, 11, 13c, 13d, 13e, 13f, 67,
74) describe individual mechanisms, this doc names the
substrate the mechanisms collectively form.

## Authoritative ownership declaration

This document is the **single authoritative reference** for the
three-pillar composition substrate — Context Synthesis, Type
Safety, and State Layering — that together make polydat's
graph-composition properties (free node embedding, Context
Fusion, Node Fusion, parallel-safe evaluation) possible. The
SRDs listed under "Companion documents" describe individual
mechanisms; this doc names the substrate they collectively
form and states the axioms each mechanism preserves. Apparent
contradictions between a non-polydat SRD and this document
resolve in favor of this document; §10 below names each
touching SRD's role under this declaration.

## Companion documents

- [SRD-10: GK Language and Compilation](../../../docs/sysref/10_gk_language.md)
  — node trait, port-type system, expression grammar. Owns
  the syntactic substrate. This doc references the typed-port
  surface SRD-10 defines.
- [SRD-11: GK Evaluation Model](../../../docs/sysref/11_gk_evaluation.md)
  — kernel/state split, effectively-const vs dynamic
  classification, const-binding contract. Owns the lifecycle
  mechanism that Pillar 3 (State Layering) builds on.
- [SRD-13c: GK Scope Model](../../../docs/sysref/13c_gk_scope_model.md)
  — `bind_outer_scope`, `scope_values`, auto-extern, manifest
  extraction. Owns the synthesis mechanisms Pillar 1 (Context
  Synthesis) builds on.
- [SRD-13f: Cross-Scope Wire Materialization](../../../docs/sysref/13f_cross_scope_wire_materialization.md)
  — value-only vs shared-cell classification, read-invariant
  across the chain, write-through semantics. Owns the
  cross-tier read/write contract Pillar 3 (State Layering)
  preserves.
- [SRD-67: Parent-gated Subcontext Construction](../../../docs/sysref/67_gk_subcontext_construction.md)
  — typed [`ScopeKernel`], [`SubcontextBuilder`], the
  walled-off cross-binding API. Owns the construction-tier
  enforcement of all three pillars.
- [SRD-74: None Propagation](../../../docs/sysref/74_none_propagation.md)
  — `Value::None` propagation rules. Owns None semantics; a
  consequence of the typed-slot contract under Pillar 2.
- [The Runtime Model](runtime_model.md) — R-axioms (data
  flow, caching, invalidation) and D-axioms (determinism
  guarantees). The runtime realisation of this doc's
  static contract. L1 (each layer owns its state) maps to
  per-fiber `GkState` at runtime; T1 (typed slots) gives
  D1 (typed-return determinism) as a direct consequence.
- [The Polydat Grammar](grammar.md) — G-axioms. The
  grammar-level commitments that underwrite this doc's
  S/T/L axioms. G1 (auto-extern discovery) + G4 (port-
  typed expressions) compose into S1 + T1; G3 (scope-
  chain transparency) + G4 compose into L1 + L2.

The forcing question: **given that polydat is a graph
compiler producing kernels that run in concurrent fibers and
host user-typed function nodes inside layered scopes — what
substrate makes free composition possible at every layer
without per-composition negotiation between the node tier and
the scope tier?** This doc says: three pillars composed —
Context Synthesis, Type Safety, State Layering. The pillars
are not independent; they reinforce each other. The
*substrate* is the three pillars in composition, not any one
in isolation.

---

## 0. Status legend

Each axiom in this doc carries an explicit status:

- **SHIPPED** — the property holds in the current code; the
  axiom is descriptive of current behavior.
- **PARTIAL** — partially implemented; the gap is named in
  the axiom's "Status note" with a forward link to where
  the remaining work is tracked.
- **PLANNED** — not yet implemented; the axiom describes
  the target contract. Hosts depending on a PLANNED axiom
  hit the listed gap until the implementation lands.

Status as of this draft:

| Axiom | Status |
|---|---|
| S1 — Auto-extern as the synthesis surface discovery rule | SHIPPED |
| S2 — Binding-time materialisation as the synthesis fill rule | SHIPPED |
| S3 — Cycle clock as the per-cycle synthesis advance | SHIPPED |
| T1 — Every slot is typed | SHIPPED |
| T2 — Type mismatches are construction-time or auto-healed | **PARTIAL** (intra-graph yes; embedding-boundary sites PLANNED per Expression Engine §5.4.2) |
| T3 — JIT preserves the slot type contract | SHIPPED |
| L1 — Each layer owns its own state | SHIPPED |
| L2 — Two-lifecycle classification bridges layers | SHIPPED |
| L3 — Captures are layered cycle-time bindings | SHIPPED |
| L4 — Cross-tier writes preserve layer ownership | SHIPPED |

---

## 1. The claim

The composition substrate has three pillars:

```text
┌─────────────────────┐  ┌─────────────────────┐  ┌─────────────────────┐
│ Context Synthesis   │  │  Type Safety        │  │  State Layering     │
│ (S-axioms)          │  │  (T-axioms)         │  │  (L-axioms)         │
├─────────────────────┤  ├─────────────────────┤  ├─────────────────────┤
│ Host context →      │  │ Every slot has a    │  │ Scope state is      │
│ kernel input slots, │  │ declared PortType.  │  │ layered: workload   │
│ synthesised by the  │  │ Mismatches caught   │  │ → scenario → for_   │
│ chain via auto-     │  │ at construction or  │  │ each → phase → op   │
│ extern + materialize│  │ healed by edge      │  │ → result-binding.   │
│ + cycle clock.      │  │ adapters.           │  │ Lifecycle bridges   │
│                     │  │                     │  │ layers.             │
└──────────┬──────────┘  └──────────┬──────────┘  └──────────┬──────────┘
           │                        │                        │
           └────────────────────────┴────────────────────────┘
                                    │
                                    ▼
                       The Slot Contract — §2
                       (the consequence)
                                    │
                                    ▼
                  Free composition at every layer
                  Context Fusion / Node Fusion / parallel safety
                                  (§11)
```

S, T, and L compose into the **slot contract**, the abstraction
barrier between the node tier and the scope tier. Below the
barrier: typed-port function nodes consuming inputs and
producing outputs. Above the barrier: layered scope state
synthesised into typed slots by the chain. The node never
reaches up; the scope never reaches down; the slot is the only
crossing, and it crosses *one direction at a time per
classification*.

The substrate is **not** any single pillar. S alone gives you
context delivery without type guarantees. T alone gives you
type checking without layered composition. L alone gives you
layered state without a chain to deliver it. The three
together — and only the three together — make the slot
contract durable across layers, types, and scopes.

---

## 2. The slot contract — the consequence

The substrate's externally-visible product is the **slot
contract**. A `GkKernel` exposes:

```text
input_defs:    Vec<InputDef>          // declared slots — name + PortType + InputKind
inputs:        Vec<Value>             // slot values at evaluation time
port_values:   Vec<Value>             // capture slots (subset of inputs)
node_buffers:  Vec<Vec<Value>>        // per-node output buffers
```

Each `InputDef` declares one slot's identity (name), its type
(PortType), and its origin (InputKind: Coordinate,
IterationExtern, Extern, Capture, Const, etc.).

The slot contract has three guarantees, one from each pillar:

| Guarantee | Pillar | What it promises |
|---|---|---|
| Slot is **filled** | Context Synthesis (S) | At evaluation time, every declared slot holds a value. The chain has synthesised it from scope state per the slot's `InputKind`. |
| Slot is **typed** | Type Safety (T) | The value matches the slot's declared `PortType`. Mismatches were caught at construction or healed by an auto-inserted adapter. |
| Slot is **layered-aware** | State Layering (L) | The value's lifecycle (effectively-const at scope-init vs dynamic per cycle) is determined by the slot's `InputKind` and upstream chain. Nodes consume per the lifecycle; they do not enforce it. |

**A node consumes inputs through slots.** It reads from a
declared input port — typed, named, layer-aware — and writes
to a declared output port. It does not look up names in a
scope, does not request values from the kernel chain, does
not enumerate parent state. The slot is the abstraction
barrier; the substrate is what makes the barrier work.

---

## 3. Pillar 1 — Context Synthesis

The chain *synthesises* host-provided scope state into the
kernel's declared input slots. This is an active construction
process at three timings: compile (auto-extern), scope-init
(binding-time materialisation), and per-cycle (set_inputs).

### Axiom S1 — Auto-extern as the synthesis surface discovery rule (SHIPPED)

**At kernel compilation, the compiler walks the body, finds
every identifier reference that resolves to an outer-scope
binding, and synthesises an `extern X: T` slot for it. The
inner kernel's `input_defs` accordingly contains a typed slot
for every outer-scope value the body consumes. The set of
discovered externs is the *synthesis surface* — the precise
set of layered-state values the chain must deliver at
scope-init time.**

Enforcement: SRD-13c §"Auto-extern" defines the discovery
rule; the kernel compiler executes it. The workload author
does not declare these slots manually; the compiler discovers
them. This is what makes the substrate *free for the author*:
they write `query[id={k}]`, the compiler discovers `{k}`
references the outer iter-var, and the slot appears.

### Axiom S2 — Binding-time materialisation as the synthesis fill rule (SHIPPED)

**At scope-init time, `bind_outer_scope(outer: &GkKernel)`
(driving `materialize_wiring_from_outer`) iterates the kernel's
extern slots and for each looks up the corresponding binding
in the outer chain. Per SRD-13f's gradient, the binding is
classified as inlined-constant, value-only-cell, or
read-write-shared-cell; the chain fills the slot per
classification. After binding-time materialisation, every
declared slot holds a value.**

Enforcement: `kernel/state.rs::materialize_wiring_from_outer`
+ SRD-13f's classification rules. The walled-off invariant
under SRD-67 ensures this is the *only* path by which outer
state crosses into inner slots — there is no second channel.

### Axiom S3 — Cycle clock as the per-cycle synthesis advance (SHIPPED)

**The only per-cycle slot mutation is `set_inputs(&[u64])`,
which mutates exactly the slots whose `InputKind` is
`Coordinate`. Every other slot — externs from outer scope,
captures from prior ops, effectively-const bindings — retains
its scope-init or last-cycle value. Per-cycle advance is
narrow, named, and typed.**

Enforcement: the `GkKernel` API surface — `set_inputs` is the
only public method that mutates input slots during a scope's
lifetime. SRD-11's two-lifecycles classification is what
distinguishes coordinate inputs (dynamic, per-cycle) from
every other input (effectively-const for the scope's
lifetime).

---

## 4. Pillar 2 — Type Safety

Every slot in the kernel — input, output, capture — has a
declared `PortType`. The chain guarantees a value of that
type at every read; the compiler catches mismatches at
construction or heals them with auto-inserted edge adapters.
The type contract is *enforceable*, not aspirational.

### Axiom T1 — Every slot is typed (SHIPPED)

**Every entry in `input_defs` carries a declared `PortType`
(U64, F64, Bool, Str, VecF32, VecI32, Bytes, Json, …). Every
node output port carries a declared `PortType`. No slot,
input or output, is untyped. There is no "any" type at the
slot tier.**

Enforcement: the `InputDef` and `Port` types in
[`ast`]/[`kernel`]. SRD-10 owns the type-system definitions;
this axiom is the substrate's claim that nothing escapes the
typing.

### Axiom T2 — Type mismatches are construction-time or auto-healed (PARTIAL)

**Status note:** the catalog operates at the intra-graph
wire-validation site today (`compile::assembly::resolve` +
auto-inserted edge adapters from `library::convert`). Two
additional sites — input-binding adapters during Context
Fusion synthesis (S2) and return-path adapters at the
embedding boundary — are PLANNED per
[Expression Engine §5.4.2](expression_engine.md). Hosts
that hit a type mismatch at those sites currently see a
typed error rather than auto-healing.

**The assembly pass ([`compile::assembly`]) validates every
wire's source `PortType` against its consumer's expectation.
A direct mismatch fails construction with a typed
`AssemblyError::TypeMismatch`. A mismatch with a known
auto-conversion edge (e.g., U64 → Str via `U64ToString`, F64
→ Str via `F64ToString`, U64 → F64 via `U64ToF64`) is healed
by inserting the adapter node in line. After assembly,
every wire's source type matches its consumer's expectation,
either directly or via a justified adapter chain. Nodes never
see a value of the wrong type.**

Enforcement: `compile::assembly::resolve` + the edge-adapter
catalog ([`library::convert`]). The catalog of known
conversions is finite and explicit; novel conversions require
adding to the catalog. The substrate does not silently coerce.

### Axiom T3 — JIT preserves the slot type contract (SHIPPED)

**JIT-compiled subgraphs receive their inputs via the same
typed-slot mechanism. The JIT does not bypass type checks; it
does not coerce silently; it reads from u64 slot positions
that the compiler has validated against the producing node's
declared `PortType`. If a subgraph cannot be JIT-compiled
while preserving the type contract, the hybrid kernel
([`compile::hybrid`]) falls back to interpreted evaluation
for that subgraph.**

Enforcement: SRD-16b's JIT boundary. The Cranelift signature
for each JIT-compiled segment is a function of the segment's
declared input/output types; nothing crosses the boundary
that the type contract didn't authorise.

---

## 5. Pillar 3 — State Layering

Scope state is *layered*. Each layer owns its own state. The
chain composes layers; the lifecycle classification bridges
them. Nothing crosses between layers except through declared
slots populated per lifecycle.

### The layer taxonomy

```text
Workload ─────────────────── workload-level params
   ▼
Scenario ─────────────────── scenario configuration, top-level externs
   ▼
for_each (any nesting) ───── iter-var bindings (per scope, dependent tuple)
   ▼
Phase ────────────────────── shared cells, phase-level captures
   ▼
Op-template ──────────────── op-field modifiers, runtime context
   ▼
Op-execution ──────────────── result-body captures (cycle-time)
```

Each layer owns its own state. Inner layers see outer-layer
state via auto-extern + binding-time materialisation (S1+S2).
Outer layers do not see inner-layer state (no callback up the
chain). Cross-tier writes are bounded to the SharedCell
write-through mechanism (§9.2).

### Axiom L1 — Each layer owns its own state (SHIPPED)

**A scope-tier instance (workload root, scenario node, for_each
scope, phase scope, op-template scope, op-execution context)
owns its own state set. State written at one layer is not
visible at outer layers; state read at one layer comes from
that layer's own bindings or from outer layers via
chain-synthesised slots. There is no cross-tier shared mutable
state outside the named SharedCell write-through mechanism.**

Enforcement: the kernel chain is a parent-child tree
constructed via SRD-67's walled-off API. Construction is
parent-gated; binding-time materialisation is the only
state-crossing surface; SRD-13f's classification governs
read/write semantics.

### Axiom L2 — Two-lifecycle classification bridges layers (SHIPPED)

**Per SRD-11, every input slot has one of two lifecycles:
*effectively-const* (resolved once at scope-init, frozen for
the scope's lifetime) or *dynamic* (resolved per pull at
cycle time). The lifecycle is *structural* — determined by
the slot's `InputKind` and its upstream wire chain, not by a
runtime flag. This classification is the layer-bridging
mechanism: an effectively-const slot is filled by the chain at
scope-init from an outer layer's binding; a dynamic slot is
filled per cycle from the current layer's state advance.**

Enforcement: SRD-11's classification rules + the const-binding
contract (Plan A compile-time check + Plan B scope-init
materialisation). The classification is *known* before the
node tier ever sees a value; the chain enforces it by
populating slots according to each input's lifecycle.

### Axiom L3 — Captures are layered cycle-time bindings (SHIPPED)

**A capture is a port-typed input populated at op-execution
time from a result body (SRD-34). Captures occupy a subset of
input slots (`GkState.port_values`). Their *layer* is the
phase scope (where the capture is declared); their *timing
window* is op-execution → next op-execution within a phase.
Nodes consuming captures read from the slot via the ordinary
input contract; the chain handles the timing.**

Enforcement: SRD-34's capture-points surface + SRD-69's
unified capture-write contract (when shipped). The
`ctx.wires.write(name, value)` chokepoint is the single
write-side surface; reads are ordinary slot reads.

### Axiom L4 — Cross-tier writes preserve layer ownership (SHIPPED)

**SRD-13f's `SharedCell` write-through is the *only* mechanism
by which an inner-tier node's output can mutate outer-tier
state. The rewrite happens at the *compilation* layer: a node
that declares a write to a wire owned by an outer scope's
shared cell has its local output rewritten to a write-through
call. The node itself is unchanged — it still produces a
value to its declared output port. The chain captures the
output and routes it to the outer cell. Layer ownership is
preserved because the outer cell remains the canonical state
holder; the inner node is just a write source.**

Enforcement: SRD-13f §"Matter-AST classification" + the
write-through emission in the compiler. Per SRD-13f's
"single kernel handle" invariant (B.2 partial), the wires
layer takes the kernel as the authoritative resolver, and
write-through routing is the only cross-tier write surface.

---

## 6. The three pillars compose — the substrate as product

S, T, and L each provide one guarantee. The slot contract
(§2) is the product of all three. The substrate's claim is
that the three together are *sufficient* to make the slot
contract durable — and that *no two of three* is sufficient.

| If you have | But lack | The slot contract... |
|---|---|---|
| T + L | S | ...cannot be filled. Slots are typed and layer-aware but the chain has no synthesis mechanism — slots stay empty or take ad-hoc values. |
| S + L | T | ...is fillable but unsafe. The chain delivers values, but nothing guarantees type. Nodes do defensive type checking or coerce silently. |
| S + T | L | ...is fillable and typed but flat. Without layering, scope composition collapses; no lifecycle distinction, no nested scope tiers, no cross-tier write semantics. |
| **S + T + L** | — | **...is the durable slot contract.** Slots are filled, typed, layer-aware. Free composition follows. |

The substrate is *exactly* S + T + L. Adding more (e.g., a
fourth pillar for capture timing or determinism) would
strengthen specific guarantees but the slot contract holds
under S + T + L alone.

---

## 7. The slot contract enables free graph composition

From the substrate, this consequence:

**A pure-typed function node composes inside a layered
stateful scope without either tier knowing about the other,
because the chain has synthesised the scope's relevant state
into the node's typed input slots. The node consumes typed
slots; the scope owns layered state; the chain mediates. No
per-composition wiring overhead at either tier.**

This consequence is what was previously informally called
"the overlap" or "the embedding property" — the load-bearing
fact that polydat graph composition has zero per-composition
negotiation cost. The substrate establishes it as a derivable
property of S + T + L, not a primitive claim.

---

## 8. Boundary mechanisms — named handlers

Certain timing and identity boundaries need named handlers.
These aren't substrate violations — they're substrate
extensions, each preserving the S/T/L axioms.

### 8.1 The capture timing window

Per L3, captures fill port-typed slots at op-execution time.
The timing window (op-execution → next op-execution within a
phase) is the temporal boundary. SRD-34 owns the timing
contract; SRD-69 (when shipped) consolidates the write-side
into `ctx.wires.write`. Reads are ordinary slot reads; writes
go through the chokepoint. T2 (type-checked slots) and L1
(layer ownership) hold across the boundary.

### 8.2 SharedCell write-through (§5, L4)

A node's output value crosses an outer-tier boundary via
write-through routing. The node is unchanged; the chain
performs the routing. L4 names this as the only mechanism;
T2 ensures the type-check holds across the cell; S2's
synthesis at the outer scope's next-cycle reads the updated
cell value.

### 8.3 Const lifecycle violations

When a `const X := <expr>` binding's RHS depends on a dynamic
input, L2's structural classification fails. SRD-11's
const-binding contract owns the detection: Plan A
(compile-time wire-chain analysis) catches structural
violations; Plan B (scope-init `catch_unwind`) catches semantic
violations. The node tier never sees a violation — it sees a
value from the chain or an error from the construction layer.

### 8.4 JIT delegation (T3)

JIT-compiled subgraphs are ordinary slot consumers from the
substrate's perspective — declared inputs, declared outputs,
typed `PortType`s, consuming a slot vector. When a node
cannot be JIT-compiled while preserving the substrate's
guarantees (e.g., it uses a runtime-context shadow), the
hybrid kernel ([`compile::hybrid`]) keeps that node
interpreted and JIT-compiles the rest. T3 is the axiom that
makes this fall-back sound.

### 8.5 Diagnostic node observable side effects

Some diagnostic nodes (`log_info`, `log_debug`, etc.) write
to stderr or to a log buffer during `eval`. From the
substrate's perspective: the node's *returned value* is still
a function of its inputs (T1, T2 preserved); the side effect
is *observable* but not *typed* — it does not flow through a
slot. These nodes are explicitly marked as having observable
side effects and are not JIT-compiled. See §11.3 for the open
question on how to formalise this within the substrate.

---

## 9. SRD cross-references and roles

| SRD | Role under this declaration |
|---|---|
| [SRD-10](../../../docs/sysref/10_gk_language.md) | Syntactic substrate. Defines `GkNode`, `Value`, `PortType`. The axioms reference types SRD-10 defines. |
| [SRD-11](../../../docs/sysref/11_gk_evaluation.md) | Two-lifecycle classification — Pillar 3 (L2). Const-binding contract — boundary handler §8.3. |
| [SRD-13c](../../../docs/sysref/13c_gk_scope_model.md) | Auto-extern (S1), `bind_outer_scope` (S2), manifest extraction. The synthesis-mechanism layer. |
| [SRD-13d](../../../docs/sysref/13d_op_template_scope.md) | Op-template scope tier in the layering (L1). Adds a layer to the taxonomy; the axioms apply transitively. |
| [SRD-13e](../../../docs/sysref/13e_scope_as_module.md) | Typed `ScopeModule` refinement. Strengthens T1 — modules carry typed import/export contracts that propagate the type contract across the chain. |
| [SRD-13f](../../../docs/sysref/13f_cross_scope_wire_materialization.md) | Cross-scope read/write semantics. Read-invariant (Pillar 3, L1); write-through routing (L4, §8.2). |
| [SRD-16](../../../docs/sysref/16_gk_engines.md) | Engine variants. T3 applies across every engine (P1 interpreted, P2 closures, P3 JIT). |
| [SRD-16b](../../../docs/sysref/16b_gk_jit.md) | JIT boundary. Owns T3's enforcement at the Cranelift boundary. |
| [SRD-34](../../../docs/sysref/34_capture_points.md) | Captures (L3). Owns the timing-window contract. |
| [SRD-67](../../../docs/sysref/67_gk_subcontext_construction.md) | Parent-gated child construction. Owns the walled-off enforcement of all three pillars at the construction tier. |
| [SRD-69](../../../docs/sysref/69_capture_semantics.md) | Unified capture-write contract (when shipped). The single chokepoint through which L3's stateful crossings funnel. |
| [SRD-74](../../../docs/sysref/74_none_propagation.md) | `Value::None` propagation. Consequence of T1 (typed slots include `Option<T>` semantics via None) — propagation is deterministic, not silent coercion. |

---

## 10. Why this substrate matters

The substrate is what makes three of polydat's distinctive
capabilities work freely:

### 10.1 Context Fusion (Graph Fusion Phase 1) depends on the substrate

Per the focal-point treatment, **Context Fusion** is the
scope-init-time phase where host context fuses into the
graph's declared slots. S1 + S2 are the synthesis mechanism;
T1 + T2 guarantee the values arrive typed; L1 + L2 carry the
layered lifecycle. Context Fusion is the substrate in motion
at scope-init.

### 10.2 Node Fusion (Graph Fusion Phase 2) is sound under the substrate

Per the focal-point treatment, **Node Fusion** is the
compile-time phase where the compiler recognises subgraph
patterns and rewrites them. Soundness reduces to "the rewrite
preserves the slot contract" — same input slots, same output
slots, same typed values, same lifecycle classification. T1 +
T2 give the rewriter a typed substrate; the rewrite preserves
the slot contract by construction. Fusion correctness is a
trivial closure property over T2 + L2.

### 10.3 Parallel evaluation is safe under the substrate

Per SRD-02, polydat kernels run in concurrent fibers. L1
(each layer owns its state) + the per-fiber `GkState` rule
(SRD-11) means there is no shared mutable state at the node
tier across fibers. The substrate is what makes the parallel
safety claim cheap — it follows directly from L1 + T1 (typed
slots, layer-owned state), not as a separate concurrency
proof.

---

## 11. What this document does NOT specify

- **The grammar productions.** SRD-10 owns syntax. Focal-point
  D (the grammar) will formalize the productions; this doc
  relies on the grammar exposing typed input ports.
- **The compilation pipeline mechanics.** Focal-point A (the
  graph compiler + kernel hoisting + Graph Fusion two-phase
  pipeline) will formalize the compiler's scope-aware passes.
  This doc relies on the compiler enforcing S1 (auto-extern),
  T1+T2 (typed slot construction), L2 (lifecycle
  classification).
- **The expression system as host utility.** Focal-point C
  will formalize the host-facing expression engine. This doc
  relies on expression evaluation being a special case of
  node evaluation — same slot contract, same chain mediation.
- **The kernel-composition algebra in full.** SRDs 13c-f
  already cover the mechanics in detail; this doc names the
  substrate they collectively form but does not re-derive
  their machinery.

---

## 12. Open questions

The substrate is established and held by current
implementations. Three open questions for future revision:

### 12.1 Capture timing window — formal spec

L3 names the capture timing window ("op-execution → next
op-execution within a phase") but doesn't formally specify
it as a sequence of states. SRD-34 carries the practical
contract; SRD-69 (draft) will carry the unified spec. A
future revision should pull a formal timing-window spec into
§8.1, since it's load-bearing for L3.

### 12.2 JIT escape-hatch enumeration

T3 promises JIT preserves the typed slot contract with
fall-back to interpreted for non-JIT-eligible nodes. The
enumeration of "what makes a node non-JIT-eligible" is
partially documented in node implementations
(`GkNode::supports_jit`) and SRD-16b but not consolidated. A
future revision should consolidate the escape-hatch list
under §8.4.

### 12.3 Diagnostic side effects — substrate amendment or port refactor

§8.5 describes diagnostic nodes as preserving the *typed
return value* but having observable side effects (log output)
that don't flow through a slot. This is a known substrate
asymmetry. Two possible resolutions, both deferred:

- **Amendment**: extend T1 to allow "observable side
  channels" as an explicit category, with declared rules
  about JIT eligibility and parallel safety.
- **Port refactor**: replace diagnostic side effects with a
  structured event-emission port (typed output port → host
  sink), bringing them inside the slot contract.

A future revision should pick one and execute.

---

[`ast`]: ../../src/ast.rs
[`kernel`]: ../../src/kernel/mod.rs
[`ScopeKernel`]: ../../src/kernel/subcontext/kernel.rs
[`SubcontextBuilder`]: ../../src/kernel/subcontext/builder.rs
[`compile::assembly`]: ../../src/compile/assembly.rs
[`compile::hybrid`]: ../../src/compile/hybrid.rs
[`library::convert`]: ../../src/library/convert.rs
