# The Composition Substrate — Polydat Design

**Subtitle:** Context Synthesis, Type Safety, State Layering.

Formalises the substrate that makes polydat's free graph
composition work. Names the three pillars, the axioms under
each, and the boundary handlers that connect them. Where
prior SRDs (10, 11, 13c, 13d, 13e, 13f, 67, 74) describe
individual mechanisms, this doc names the substrate the
mechanisms collectively form.

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

- [SRD-10: GK Language and Compilation](language_spec.md)
  — node trait, port-type system, expression grammar. Owns
  the syntactic substrate. This doc references the typed-port
  surface SRD-10 defines.
- [SRD-11: GK Evaluation Model](evaluation_model.md)
  — kernel/state split, effectively-const vs dynamic
  classification, const-binding contract. Owns the lifecycle
  mechanism that Pillar 3 (State Layering) builds on.
- [SRD-13c: GK Scope Model](scope_model.md)
  — `bind_outer_scope`, `scope_values`, auto-extern, manifest
  extraction. Owns the synthesis mechanisms Pillar 1 (Context
  Synthesis) builds on.
- [SRD-13f: Cross-Scope Wire Materialization](wire_materialization.md)
  — value-only vs shared-cell classification, read-invariant
  across the chain, write-through semantics. Owns the
  cross-tier read/write contract Pillar 3 (State Layering)
  preserves.
- [SRD-67: Parent-gated Subcontext Construction](subcontext_construction.md)
  — typed [`ScopeKernel`], [`SubcontextBuilder`], the
  walled-off cross-binding API. Owns the construction-tier
  enforcement of all three pillars.
- [SRD-74: None Propagation](none_semantics.md)
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
port_values:   Vec<Value>             // externally-written slots (subset of inputs)
node_buffers:  Vec<Vec<Value>>        // per-node output buffers
```

Each `InputDef` declares one slot's identity (name), its type
(PortType), and its origin (InputKind: Coordinate,
IterationExtern, Extern, ExternalWrite, Const, etc.). The
`ExternalWrite` kind is the polydat-side surface that hosts
use for runtime injection patterns; hosts give those patterns
their own names (e.g., nbrs's *capture* uses this kind).

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

### Axiom S1 — Auto-extern as the synthesis surface discovery rule

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

### Axiom S2 — Binding-time materialisation as the synthesis fill rule

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

### Axiom S3 — Cycle clock as the per-cycle synthesis advance

**The only per-cycle slot mutation is `set_inputs(&[u64])`,
which mutates exactly the slots whose `InputKind` is
`Coordinate`. Every other slot — externs from outer scope,
externally-written slots, effectively-const bindings —
retains its scope-init or last-write value. Per-cycle
advance is narrow, named, and typed.**

Enforcement: the `GkKernel` API surface — `set_inputs` is the
only public method that mutates input slots during a scope's
lifetime. SRD-11's two-lifecycles classification is what
distinguishes coordinate inputs (dynamic, per-cycle) from
every other input (effectively-const for the scope's
lifetime).

### Axiom S4 — External-write synthesis as the open-granularity fill path

**A polydat kernel exposes port-typed input slots that
external producers may populate at any granularity —
construction-time scope-init, cycle-time injection, or
arbitrary external-write-time. These slots are ordinary
wires: nodes reading from them use the standard port-read
mechanism; provenance tracking (per R2) marks downstream
consumers dirty when an external write changes a slot;
clean-flag memoization (per R1) re-evaluates them on next
pull. S4 names the existence and contract of the
external-write surface itself; cross-tier write semantics —
an inner-scope writer populating an outer-scope's `shared`
wire — are governed by S5's SharedCell write-through
mechanism.**

Enforcement: the kernel exposes typed-write entry points
for external producers. T1 + T2 ensure writes are
type-checked at the boundary. The kernel makes no
assumption about who the producer is, when it writes, or
what host-level semantic the write carries — the contract
is generic external-port population, and downstream nodes
re-evaluate per the standard provenance rules whenever an
input changes.

Where the other S-axioms describe chain-internal fill
paths (S1 discovers the surface; S2 fills at scope-init;
S3 advances per cycle), S4 names the open-granularity
surface for fills *originated outside the kernel*. The
producer is host code, an iteration driver, an event
source, or any other component the polydat layer does not
need to know about. The synthesis contract is preserved
because the typed-write API is the chain's entry point for
external producers, not a side channel around the chain.

**Volatility opt-in (cross-reference to R1).** When a
wire's value is not a function of its declared inputs —
temporal sources, entropy sources, or any genuinely
non-deterministic producer — the wire is *volatile*.
Volatility arises from two distinct sources:

- **Intrinsic.** Certain library nodes declare themselves
  volatile (e.g., `current_epoch_millis`, `counter`,
  `elapsed_millis`, `thread_id`). The library imposes
  volatility; no user opt-in is required, and the marker
  cannot be removed by the workload author.
- **User opt-in.** A wire is declared `volatile` via the
  modifier on its binding. The author marks the wire as
  must-recompute-on-every-read.

Both sources produce the same runtime effect: opt-out of
clean-flag memoization, and contagion through dependents
(see R1's volatile sub-axiom in runtime_model.md).
Volatility is not required for ordinary S4 external
writes; the provenance machinery handles re-evaluation
correctly via input-change tracking. Volatility is the
explicit marker for the genuinely-non-deterministic case
where input-change tracking is insufficient because the
value is not a function of the declared inputs.

### Axiom S5 — Compile-emit write-through as the cross-tier synthesis path

**SRD-13f's `SharedCell` write-through is the *only*
mechanism by which an inner-tier node's output can mutate
outer-tier state. The rewrite happens at the *compilation*
layer: a node that declares a write to a wire owned by an
outer scope's shared cell has its local output rewritten to
a write-through call. The node itself is unchanged — it
still produces a value to its declared output port. The
chain intercepts the output and routes it to the outer
cell; the outer cell's slot gets filled with the inner-tier
write through the standard slot-filling contract.**

S5 sits on the S-axis because cross-tier writes are
fundamentally a slot-filling operation: the inner-tier
node's output value fills the outer-tier cell's slot. The
mechanism is compile-emit synthesis (the compiler emits the
write-through call); the runtime effect is that the outer
cell's slot has its value updated through the routing. The
layer-ownership guarantee — that the outer cell remains the
canonical state holder and no two layers race for write
authority — is L1's claim, not S5's. S5 names how the
mechanism preserves L1's invariant: write-through routes
through the typed-slot surface, so the outer cell's owner
sees an ordinary slot-write rather than a cross-tier
mutation.

Enforcement: SRD-13f §"Matter-AST classification" + the
write-through emission in the compiler. Per SRD-13f's
"single kernel handle" invariant (B.2 partial), the wires
layer takes the kernel as the authoritative resolver, and
write-through routing is the only cross-tier write surface.

**Sub-axiom S5.r — `shared` carries write permission only.**
The `shared` modifier on a wire declares that the wire is
*writable across tier boundaries* via the write-through
mechanism described above. It does NOT affect read access:
inner-scope reads of any visible cross-tier wire return the
current outer-tier value via the uniform read invariant
(per L1), whether or not the wire is marked `shared`. Read
mediation is governed by the chain's wiring synthesis at
construction time; the `shared` declaration only enables
the inner tier to *write* to the wire, opening the
write-through path. Read uniformity is the substrate's
guarantee; write permission is the explicit opt-in.

---

## 4. Pillar 2 — Type Safety

Every slot in the kernel — input, output, externally-written
— has a declared `PortType`. The chain guarantees a value of that
type at every read; the compiler catches mismatches at
construction or heals them with auto-inserted edge adapters.
The type contract is *enforceable*, not aspirational.

### Axiom T1 — Every slot is typed

**Every entry in `input_defs` carries a declared `PortType`
(U64, F64, Bool, Str, VecF32, VecI32, Bytes, Json, …). Every
node output port carries a declared `PortType`. No slot,
input or output, is untyped. There is no "any" type at the
slot tier.**

Enforcement: the `InputDef` and `Port` types in
[`ast`]/[`kernel`]. SRD-10 owns the type-system definitions;
this axiom is the substrate's claim that nothing escapes the
typing.

### Axiom T2 — Type mismatches are construction-time or auto-healed

The adapter catalog operates at three sites: intra-graph
wire validation (`compile::assembly::resolve` +
auto-inserted edge adapters from `library::convert`),
Context Fusion synthesis (boundary adapters via
`adapt_boundary_value` in `kernel/state.rs`), and the
typed-embedding return path (catalog dispatch in
`dsl::compile::eval_const_expr_typed`). The catalog is
the single source of truth across all three sites.

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

### Axiom T3 — JIT preserves the slot type contract

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
Outer scope ──────────────── chain-cascaded params, top-level externs
   ▼
Nested scopes (any depth) ── per-scope bindings, shared cells, externs
   ▼
Cycle-time ───────────────── per-pull state + external-write injection
```

(Hosts impose their own layer names on this generic
structure — e.g., nbrs maps these tiers to workload /
scenario / phase / op-template / op-execution; polydat
itself remains layer-name-agnostic.)

Each layer owns its own state. Inner layers see outer-layer
state via auto-extern + binding-time materialisation (S1+S2).
Outer layers do not see inner-layer state (no callback up the
chain). Cross-tier writes are bounded to the SharedCell
write-through mechanism (S5).

### Axiom L1 — Each layer owns its own state

**A scope-tier instance (workload root, scenario node, for_each
scope, phase scope, op-template scope, op-execution context)
owns its own state set. State written at one layer is not
visible at outer layers; state read at one layer comes from
that layer's own bindings or from outer layers via
chain-synthesised slots. There is no cross-tier shared mutable
state outside the named SharedCell write-through mechanism
specified by S5 (compile-emit write-through as the cross-tier
synthesis path).**

Enforcement: the kernel chain is a parent-child tree
constructed via SRD-67's walled-off API. Construction is
parent-gated; binding-time materialisation is the only
state-crossing surface; SRD-13f's classification governs
read/write semantics. S5 specifies the only cross-tier
write surface; this axiom's claim is that the outer cell
remains the canonical state holder regardless of which
inner tier issues the write, so layer ownership is preserved
across S5's routing.

### Axiom L2 — Two-lifecycle classification bridges layers

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

**Sub-axiom L2.f — Failed const materialisation falls
through to the outer chain (L2 ⊓ T1).** When an
effectively-const binding's scope-init evaluation yields
`Value::None` (per the None propagation contract — see
[none_semantics.md](none_semantics.md)), the slot is
considered *unfilled at this layer*. The read invariant
(L1) then returns the outer scope's value for the same
name via the standard lookup chain. The effectively-const
guarantee is preserved at the outer layer: the outer
binding is itself effectively-const for the scope's
lifetime, so the value the inner reader observes is
stable across the activation. This composition of L2 (the
inner binding's lifecycle) with T1 (typed slots include
the None sentinel) gives the conditional-shadow semantic
on which the host's `set:`-style sugar relies: an
intermediate-layer `const X := <expr>` that yields a real
value shadows the outer X; one that yields None leaves the
outer X visible.

**Strict-mode hardening — open.** Silent fall-through on
intermediate-layer None can mask author intent: did the
layer mean to provide a shadow that happens to compute to
None, or did it mean to declare an `extern X` and forget
to? A future strict-mode rule should diagnose this case
("layer provided matter for X that yielded None at
scope-init; declare `extern X` explicitly if fall-through
to outer was intended"), forcing the author to be explicit
about whether the layer is participating in the binding or
deferring entirely. Current behavior is permissive
fall-through; the strict diagnostic is a candidate rule,
not a commitment.

### Note on cross-tier writes

The cross-tier-write mechanism (`SharedCell` write-through)
is a **synthesis-axis** concern and lives at S5 in §3 — the
mechanism fills outer-tier slots via inner-tier nodes' typed
output emission, so it belongs with the other slot-filling
axioms. The **layer-ownership guarantee** that S5 preserves
— that the outer cell remains the canonical state holder
and inner-tier writes flow through the typed-slot surface
rather than as ad-hoc cross-tier mutation — is L1's claim
(see §"Axiom L1 — Each layer owns its own state" above,
which names the SharedCell write-through as the only
permitted exception to single-layer state ownership).

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
| S + T | L | ...is fillable and typed but flat. Without layering, scope composition collapses; no lifecycle distinction, no nested scope tiers; S5's cross-tier write mechanism would still route values but L1's layer-ownership guarantee would be absent — written-to cells would have no stable owner. |
| **S + T + L** | — | **...is the durable slot contract.** Slots are filled, typed, layer-aware. Free composition follows. |

The substrate is *exactly* S + T + L. Adding more (e.g., a
fourth pillar for external-write timing, determinism, or
cross-tier write semantics)
would strengthen specific guarantees but the slot contract
holds under S + T + L alone.

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

### 8.1 The external-write boundary

Per S4, external producers populate port-typed slots through
the kernel's typed-write API. The timing of writes is
determined by the producer, not by the kernel; the kernel's
contract is that any write triggers standard provenance
invalidation per R2, and consumers re-evaluate on next pull
per R1. Reads from the slot are ordinary port reads; T1 + T2
ensure type safety at the write boundary.

Hosts with specific timing requirements (runtimes injecting
values at well-defined synchronisation points, producers
driving values from event streams, or any other host-level
write-timing semantic) own that contract at the host layer.
The polydat kernel itself makes no commitment beyond "writes
trigger normal invalidation." This separation is what allows
polydat to serve as a substrate for diverse host runtimes
without embedding any host's specific write-timing
semantics.

### 8.2 SharedCell write-through (§3, S5)

A node's output value crosses an outer-tier boundary via
write-through routing. The node is unchanged; the chain
performs the routing. S5 names this as the only mechanism;
T2 ensures the type-check holds across the cell; S2's
synthesis at the outer scope's next-cycle reads the updated
cell value; L1's layer-ownership invariant is preserved
because the outer cell remains the canonical state holder.

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
| [SRD-10](language_spec.md) | Syntactic substrate. Defines `GkNode`, `Value`, `PortType`. The axioms reference types SRD-10 defines. |
| [SRD-11](evaluation_model.md) | Two-lifecycle classification — Pillar 3 (L2). Const-binding contract — boundary handler §8.3. |
| [SRD-13c](scope_model.md) | Auto-extern (S1), `bind_outer_scope` (S2), manifest extraction. The synthesis-mechanism layer. |
| [SRD-13d](../../../docs/sysref/13d_op_template_scope.md) | Op-template scope tier in the layering (L1). Adds a layer to the taxonomy; the axioms apply transitively. |
| [SRD-13e](../../../docs/sysref/13e_scope_as_module.md) | Typed `ScopeModule` refinement. Strengthens T1 — modules carry typed import/export contracts that propagate the type contract across the chain. |
| [SRD-13f](wire_materialization.md) | Cross-scope read/write semantics. Read-invariant (Pillar 3, L1); write-through routing (S5, §8.2). |
| [SRD-16](engines.md) | Engine variants. T3 applies across every engine (P1 interpreted, P2 closures, P3 JIT). |
| [SRD-16b](jit_boundary.md) | JIT boundary. Owns T3's enforcement at the Cranelift boundary. |
| [SRD-67](subcontext_construction.md) | Parent-gated child construction. Owns the walled-off enforcement of all three pillars at the construction tier. |
| [SRD-74](none_semantics.md) | `Value::None` propagation. Consequence of T1 (typed slots include `Option<T>` semantics via None) — propagation is deterministic, not silent coercion. |

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

### 12.1 External-write ordering semantics — formal spec

S4 names external-write events as the mechanism for
populating port-typed slots dynamically, but the *ordering
semantics* between an external write and a concurrent fiber
pull are not specified at the substrate layer. The current
implementation serialises writes through a per-port
mutex-protected slot; this is a runtime concern (per R1 +
R2). A future revision should formalise the read-write
ordering guarantees the substrate provides — particularly
for the case where a producer wants "this value visible to
all subsequent reads on this fiber starting from cycle N"
semantics. Hosts with stricter timing needs currently
establish those guarantees in their own contract; making the
substrate itself commit to a richer ordering spec is open.

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
