# The Graph Compiler — Polydat Design

**Subtitle:** Hoisting and Graph Fusion (Context Fusion + Node
Fusion).

Formalises the scope-aware compiler that produces ready
kernels. Names hoisting as the cross-kernel composition
mechanism, Graph Fusion as the two-phase accommodation
pipeline, and the axioms each preserves over the
[composition substrate](composition_substrate.md).

## Authoritative ownership declaration

This document is the **single authoritative reference** for
polydat's graph compiler — the analysis and rewriting passes
that turn an authored graph into a ready kernel. It owns the
**hoisting** algebra (what computation moves between layers
and why), the **Context Fusion** synthesis pipeline (host
context fills declared slots at scope-init), and the **Node
Fusion** rewriting pipeline (compiler recognises and rewrites
subgraph patterns at compile time). SRDs 16 (engines) and 16b
(JIT wiring) describe execution; this doc describes
construction. Where they overlap on the construction tier,
this document is authoritative.

## Companion documents

- [Composition Substrate](composition_substrate.md) — the
  S/T/L pillars and the slot contract. This doc's mechanisms
  operate *over* the substrate; the substrate's axioms are the
  invariants this doc preserves.
- [The Runtime Model](runtime_model.md) — the R-axioms
  (data flow, caching, invalidation) and D-axioms
  (determinism guarantees). What this doc's compiled
  output *does* at runtime. The hoisting analysis (§3) is
  what determines what runs at scope-init vs per-cycle
  according to the Runtime Model's R1/R2 mechanics.
- [The Polydat Grammar](grammar.md) — G-axioms. The
  grammar-level commitments that underwrite this doc's
  H/CF/NF axioms. G2 (lifecycle declared at the syntactic
  surface) + G5 (structural lifecycle classification)
  compose into H1 (classification totality) + H2
  (monotonicity); G1 (auto-extern discovery) underwrites
  CF1 (synthesis surface completeness).
- [SRD-10: GK Language and Compilation](language_spec.md)
  — DSL syntax, compiler pipeline overview. Owns the
  language; this doc owns the compiler's scope-aware passes.
- [SRD-11: GK Evaluation Model](evaluation_model.md)
  — two-lifecycle classification, const-binding contract.
  Hoisting eligibility (§3.3) is a direct function of SRD-11's
  classification.
- [SRD-13c: GK Scope Model](scope_model.md)
  — auto-extern, `bind_outer_scope`, manifest extraction.
  Context Fusion (§4) is the runtime fulfilment of SRD-13c's
  scope-init mechanism.
- [SRD-13f: Cross-Scope Wire Materialization](wire_materialization.md)
  — value-only vs shared-cell classification. Context Fusion
  (§4.4) honours SRD-13f's gradient at synthesis time.
- [SRD-16: GK Engines](engines.md)
  — P1/P2/P3 execution engines, auto-selection heuristic.
  Owns execution-time engine variants; this doc owns the
  construction-time pipeline that produces them.
- [SRD-16b: GK JIT Wiring](jit_boundary.md)
  — Cranelift boundary semantics. Node Fusion (§5.5)
  interacts with JIT eligibility at fusion-time.
- [SRD-67: Parent-gated Subcontext Construction](subcontext_construction.md)
  — the walled-off cross-binding API. Context Fusion (§4) is
  the typed-builder construction surface SRD-67 gates.

The forcing question: **a polydat workload declares its
intent — clauses, bindings, ops — but never declares the
machinery (which values live where, which subgraphs collapse,
which inputs are constant for which scope's lifetime). The
compiler infers all of it. What is the compiler doing, and
what invariants does it preserve?** This doc says: the
compiler performs *hoisting* (lifecycle analysis that
identifies which computation moves to which scope layer) and
*Graph Fusion* in two phases (Context Fusion at scope-init,
Node Fusion at compile time). Together they produce a
ready-kernel that honours the substrate's slot contract.

---

## 1. The claim

The compiler is **scope-aware**: it does not treat a kernel
as a closed compilation unit but as a participant in a chain
of nested scopes, each with its own lifecycle classification
and synthesis surface. The compiler's load-bearing
construction work is three coordinated mechanisms:

```text
                  AUTHORED GRAPH
                        │
                        ▼
                ┌──────────────────┐
                │   Node Fusion    │   compile-time
                │   (Phase 2 GF)   │   pattern rewriting
                └────────┬─────────┘
                         │
                         ▼
                ┌──────────────────┐
                │     Hoisting     │   compile-time lifecycle
                │     (analysis)   │   classification
                └────────┬─────────┘
                         │
                         ▼
                ┌──────────────────┐
                │   compiled       │   the program is now
                │   GkProgram      │   ready to instantiate
                └────────┬─────────┘
                         │
                         │   (per scope-init)
                         ▼
                ┌──────────────────┐
                │  Context Fusion  │   scope-init-time
                │  (Phase 1 GF)    │   slot filling
                └────────┬─────────┘
                         │
                         ▼
                  READY KERNEL
                  (slot contract held)
```

**Hoisting** is the lifecycle analysis: which input is
effectively-const for which scope, what computation can be
folded once at scope-init versus per-cycle.

**Graph Fusion Phase 2 (Node Fusion)** is compile-time
pattern rewriting: subgraph recognition, polyfill insertion,
type-coercion edge adapters, fused-node substitution.

**Graph Fusion Phase 1 (Context Fusion)** is scope-init-time
slot filling: extern slots get bound from outer scope,
manifest exports get hooked up, the chain materialises the
synthesis surface auto-extern declared.

Together they form the *construction pipeline*. The substrate
gives them the typed slot contract to preserve; the compiler
preserves it through every pass.

---

## 2. Pipeline overview

The full compiler pipeline, in dependency order:

| Pass | Timing | Owns | Substrate guarantee preserved |
|---|---|---|---|
| Parse | DSL → AST | Lexer, parser, AST construction (SRD-10) | — |
| Bind | AST → assembly DAG | Identifier resolution, output declaration | — |
| **Node Fusion** | DAG → DAG | Pattern recognition, polyfills, adapters | T1+T2 (typed slots), L2 (lifecycle) |
| Topological sort | DAG → ordered DAG | Cycle detection, evaluation order | structural |
| **Hoisting analysis** | Ordered DAG → annotated DAG | Lifecycle classification of every wire | L2 (lifecycle) |
| Engine selection | Annotated DAG → engine choice | Heuristic over graph shape (SRD-16) | T3 (JIT preserves type) |
| Emit | Annotated DAG + engine → `GkProgram` | Final program structure | all three pillars |
| **Context Fusion** | `GkProgram` + outer scope → ready kernel | Slot synthesis at scope-init (SRD-13c, SRD-13f) | S1+S2 (synthesis) |

The bold rows are owned by this document. The rest are
covered by other SRDs and treated here only insofar as their
output is the input to a load-bearing pass.

---

## 3. Hoisting

### 3.0 What "hoisting" means in polydat

In compiler theory, *hoisting* often means cross-scope code
motion — moving an inner-loop-invariant computation to
before the loop. In polydat, hoisting has a more specific
meaning:

> **Hoisting is the per-wire lifecycle classification within
> a single kernel's program, used to partition the program's
> execution into two code paths emitted in the same
> `GkProgram`: a scope-init buffer evaluation that runs once
> when the kernel materialises, and a per-cycle dispatch
> that runs each `set_inputs` advance.**

It is within-kernel partitioning, not cross-kernel code
motion. The classical CS sense — moving a computation from
an inner kernel to an outer kernel so the work happens once
across all inner iterations — is not currently a polydat
optimisation; it surfaces as the open question in §10.2.

This narrower meaning is what makes the pipeline ordering
unambiguous: hoisting's classification is a property of the
*final* wire chain (which inputs each wire reaches through
which computed nodes); Node Fusion changes wire structure;
therefore Node Fusion must precede Hoisting. The hoisting
pass needs to see the post-fusion graph to classify
correctly.

### 3.1 The hoisting algebra

Per SRD-11, every input slot in a kernel has one of two
lifecycles:

- **Effectively-const** — resolved once at scope-init,
  frozen for the scope's lifetime.
- **Dynamic** — resolved per pull at cycle time.

Hoisting analysis walks the kernel's wire chain and classifies
every wire (every node output port, every input slot) by
lifecycle. The classification is structural:

```text
classify(wire) =
    if wire is a Coordinate input slot:        Dynamic
    if wire is an ExternalWrite input slot:    Dynamic
    if wire is an IterationExtern input slot:  Effectively-const (per-scope-init)
    if wire is a const-marked output:          Effectively-const (per-scope-init)
    if wire is an outer-extern with const RHS: Effectively-const (per-scope-init)
    otherwise (computed wire):
        if all upstream wires are Effectively-const: Effectively-const
        otherwise:                                    Dynamic
```

A computed wire's classification is the *join* of its
upstream wires' classifications — Effectively-const if every
upstream is Effectively-const, Dynamic otherwise. This is
monotonic and bottom-up; one pass over the topologically-
sorted DAG suffices.

### 3.2 The hoisting boundary

The hoisting boundary in a kernel is the set of wires that
mark "everything upstream is Effectively-const; everything
downstream is Dynamic." The compiler emits two evaluation
codepaths in the `GkProgram`:

- **Scope-init path**: evaluate every Effectively-const wire
  exactly once at scope-init, store in slot or buffer.
- **Per-cycle path**: evaluate every Dynamic wire per
  `set_inputs` advance, reading Effectively-const upstream
  values from their pre-evaluated buffers.

The boundary is *structural*, not declared. A node author
does not say "I'm hoistable"; the analysis derives it from
the upstream wire chain.

### 3.3 Hoisting eligibility — what moves to scope-init

A computed wire is hoisting-eligible iff its full upstream
cone reaches *only* Effectively-const inputs. Any Dynamic
upstream pins the wire to the per-cycle path.

This is a property of the wire chain, not of the node.
Concrete consequences:

- A `hash(k)` node where `k` is a for_each iter-var is
  hoistable — `k` is `IterationExtern` (Effectively-const for
  the scope's lifetime).
- A `hash(cycle)` node is *not* hoistable — `cycle` is a
  `Coordinate` (Dynamic).
- A `const X := <expr>` binding where the RHS uses only
  iter-vars and other consts is hoistable — and SRD-11's
  const-binding contract guarantees Plan A's structural check
  catches any violation.
- A `const X := hash(cycle)` binding is *rejected* by SRD-11
  Plan A — the const surface cannot host a Dynamic upstream.

### 3.4 The Axiom suite — H-axioms

The substrate's axioms (S1–S5, T1–T3, L1–L2) are general
guarantees about slots and layers; hoisting adds specific
guarantees about the analysis itself.

#### Axiom H1 — Classification is total

**Every wire in a well-formed graph has exactly one
lifecycle classification (Effectively-const or Dynamic).
The classification function is total — there is no "unknown"
or "deferred" state.**

Enforcement: the structural walk in §3.1 is exhaustive;
unresolved upstream wires fail at construction (SRD-15
strict mode promotes the warning to error).

#### Axiom H2 — Classification is monotonic and stable under composition

**Wire `w`'s classification is a monotonic function of its
upstream cone's classifications: if any upstream is Dynamic,
`w` is Dynamic; otherwise `w` is Effectively-const. The
classification is preserved under fan-in composition (joining
upstream cones) and under fan-out (multiple consumers of
`w`).**

Enforcement: the join rule in §3.1. Adding an upstream wire
can change `w` from Effectively-const to Dynamic but never
the reverse; this monotonicity is what makes the analysis
stable under graph rewrites (Node Fusion, §5).

#### Axiom H3 — Hoisting boundary is preservation-of-determinism

**A wire `w` moved from the per-cycle path to the scope-init
path produces the same value at evaluation time as it would
have produced per-cycle, for every cycle. That is, hoisting
does not change the semantic value of any wire; it only
changes *when* the value is computed.**

Enforcement: H1's totality + S3 (cycle clock advances
coordinates only) + L2 (lifecycle bridging). The
Effectively-const lifecycle is precisely the property that
"value doesn't change after scope-init"; hoisting only
applies the lifecycle's promise.

### 3.5 Caching: `from_program` and `instance_program`

Per SRD-67, the typed construction surface exposes
`from_program(Arc<GkProgram>) -> GkKernel` as the
cache-and-rebind primitive: the same compiled program is
instanced freshly per execution context, with new
`GkState` per fiber.

Hoisting interacts with caching critically: because the
scope-init path is part of the compiled `GkProgram`, the
*work* of evaluating Effectively-const wires is incurred
once per kernel instance (not per `GkProgram` compilation,
since a program is reused). The program is shared; the state
(holding the post-hoisting Effectively-const buffer values)
is per-instance.

This is the load-bearing property for parallel safety:
parallel fibers each get their own state; the Effectively-
const cache lives in the state; the program is read-only and
shared.

---

## 4. Graph Fusion Phase 1 — Context Fusion

Context Fusion is the scope-init-time fulfilment of the
substrate's Synthesis pillar (S1, S2). When a scope
materialises, its declared extern slots are populated from
the outer scope's bindings; its Effectively-const buffer is
evaluated; its dispatch state is initialised.

### 4.1 The synthesis surface

Per S1, the compiler discovers extern slots via auto-extern.
The set of discovered externs is the **synthesis surface** —
the precise set of slot names + types that the chain must
fill from outer state at scope-init.

The synthesis surface is encoded in the kernel's
`input_defs`: each entry whose `InputKind` is `Extern` /
`IterationExtern` is a slot to be filled by Context Fusion.

### 4.2 The synthesis act

Per S2, `bind_outer_scope(outer: &GkKernel)` walks the
synthesis surface (driving the internal
`materialize_wiring_from_outer` pass). For each declared
slot in `self`, the synthesis act:

1. Looks up the slot's name in `outer`'s shared-cell view
   (`outer.shared_cells_in_scope()` — `outer`'s own
   input-slot cells PLUS its transit cells).
2. Classifies the binding per SRD-13f's gradient:
   - **inlined-constant**: outer's binding is a literal or
     compile-time-foldable. Copy the value into the slot.
   - **value-only-cell**: outer's binding is a runtime value
     with no mutation contract. Copy the value into the slot.
   - **read-write-shared-cell**: outer's binding is marked
     `shared`. Attach a `SharedCell` handle to the slot.
3. Forwards as **transit**: any cell from `outer`'s view
   whose name has no matching local slot is recorded on
   `self`'s transit list, so a deeper descendant can pick it
   up when *it* materialises against `self`.

After the walk, every declared extern slot in `self` has a
value or a `SharedCell` handle, and transit cells are queued
for downstream materialisations. The Effectively-const
buffer evaluation (the hoisted scope-init path, §3.5) then
runs against these synthesised slots.

(The output **manifest** — the typed contract a program
exposes to descendant synthesizers — is a separate
read-only summary produced by `kernel::extract_manifest`. It
is consumed by synthesizers *before* compiling an inner
program, to decide what auto-externs the inner program may
declare. It is not fired during synthesis; it informs the
shape the synthesis surface will take.)

### 4.3 The Axiom suite — CF-axioms

#### Axiom CF1 — Surface completeness

**Every extern slot in the kernel's `input_defs` is present
in the synthesis surface, and Context Fusion fills every slot
before scope-init evaluation begins. No "lazy" slots, no
"resolved on first read" — every declared slot is filled at
scope-init time.**

Enforcement: `materialize_wiring_from_outer` iterates
`input_defs` exhaustively; SRD-67's walled-off construction
gates ensure no alternative synthesis path exists.

#### Axiom CF2 — Deterministic synthesis

**For a fixed outer scope state and a fixed `GkProgram`,
Context Fusion produces the same slot values every time.
There is no nondeterministic ordering, no synthesis-time
random choice, no implicit context not derivable from the
outer chain.**

Enforcement: the synthesis walk is structural over
`input_defs` (defined ordering) and the outer chain lookup
is shadow-aware (SRD-21). Identical inputs produce identical
slot vectors.

#### Axiom CF3 — Gradient honouring

**The classification of each slot during synthesis (inlined-
constant / value-only / shared-cell) is *exactly* SRD-13f's
gradient classification, derived from outer's binding
modifier and ownership. Context Fusion does not "promote"
or "demote" classifications; it honours them.**

Enforcement: the classification is computed by SRD-13f's
Matter-AST classification at outer's compile time, recorded
in outer's program, and read by Context Fusion at scope-
init. No re-classification at synthesis.

#### Axiom CF4 — Synthesis happens once per scope-init

**Context Fusion fires once at scope-init: when the kernel
is being instantiated as a fresh scope. It does not fire
per cycle. Per-cycle state advance is the cycle clock (S3),
which is narrow and named.**

Enforcement: `bind_outer_scope` is called once per
construction; `set_inputs` is the per-cycle surface and
mutates only coordinate slots. There is no "re-fuse the
context mid-scope" surface.

### 4.4 Interaction with the substrate

Context Fusion is the runtime mechanism that gives the
Synthesis pillar (S1, S2) its concrete realisation. Where
the substrate says "the chain synthesises scope state into
slots," Context Fusion is the synthesis act.

T1, T2 (type-checked slots) are enforced *during* synthesis:
the slot's declared `PortType` is verified against the
outer binding's value type; mismatches use auto-conversion
edge adapters (T2). L2 (lifecycle bridging) is the
classification that drives the synthesis path — only slots
of Effectively-const lifecycle (per H1) are filled by
Context Fusion; Dynamic slots are filled by S3 per cycle.

---

## 5. Graph Fusion Phase 2 — Node Fusion

Node Fusion is the compile-time pass that recognises subgraph
patterns and rewrites them. It runs **before** hoisting
analysis, so its rewrites are visible to hoisting and
classification reflects the post-fusion structure.

### 5.1 What Node Fusion does

Three kinds of rewrites:

| Rewrite | Pattern | Result |
|---|---|---|
| **Subgraph fusion** | A recognised pattern of N nodes | One fused node performing the equivalent computation |
| **Polyfill insertion** | A node whose target engine cannot execute it natively | The node + an inserted polyfill substituting an equivalent engine-supported computation |
| **Adapter insertion** | A wire whose source `PortType` differs from its consumer's expectation | The wire + an inserted edge adapter (e.g., `U64ToString`, `F64ToString`) that converts |

All three are *graph rewrites*: nodes are added, removed, or
reconnected. The graph after Node Fusion is a structurally
different graph from the one the parser produced; the
hoisting pass that follows sees the fused graph, not the
original.

### 5.2 The fusion catalog

The compiler maintains a registry of recognised subgraph
patterns. Concrete examples currently in `library/`:

- `hash` fusion patterns (in `library/hash.rs`): combine
  hash chains.
- `lerp` fusion patterns (in `library/lerp.rs`): collapse
  linear-interpolation chains.
- `weighted` fusion patterns (in `library/weighted.rs`):
  optimise weighted selection over constant weight tables.

The registry contract: each entry declares (a) the
pattern's matcher, (b) the replacement node constructor, and
(c) a soundness proof or test (NF3 below).

### 5.3 The polyfill mechanism

A polyfill is a *shape-fitting* rewrite: a node whose
declared shape (input port-types, computation, output
port-types) doesn't fit the substrate's slot contract as
authored gets rewritten into an equivalent subgraph whose
constituent nodes do fit. Polyfills are how the compiler
flexes "graph matter" (what the author wrote) into "graph
fixture form" (what the substrate accepts).

Example shapes that polyfills address: a node that consumes
a `VecF32` where only `Bytes` is type-supported by adjacent
nodes; a node whose computation has no single-node
representation but decomposes into a small graph of
substrate-supported primitives; a node whose semantics
require a setup/teardown pair that the slot contract can't
express as a single node.

**Polyfills are orthogonal to engine selection.** This is
load-bearing: engine selection (SRD-16) is the *runtime
variant* choice (P1 interpreted / P2 closures / P3 JIT /
hybrid) based on heuristics over the assembled DAG.
Polyfills are *compile-time shape rewrites* that produce a
DAG every engine variant can execute. The two concerns are
deliberately separated:

- A polyfill applies regardless of which engine the
  selector picks downstream. The rewritten DAG honours T1
  + T2 + L2 by construction; every engine variant operates
  on the slot contract uniformly.
- The engine selector reads the post-fusion DAG (which
  includes polyfill rewrites) and chooses a variant. It
  does not insert nodes; it picks an execution strategy
  over an already-fitted graph.

Polyfills are Node Fusion concern: graph rewriting at
compile time, preserving the slot contract per NF1-NF4. The
runtime sees a uniform graph; the engine selector picks how
to execute it; the two concerns compose without conflict.

### 5.4 The Axiom suite — NF-axioms

#### Axiom NF1 — Slot contract preservation

**A Node Fusion rewrite must preserve the slot contract:
the input slots, output slots, and their declared
`PortType`s of the rewritten subgraph match the input
slots, output slots, and types of the original subgraph
modulo type-compatible edge adapters.**

Enforcement: the assembler ([`compile::assembly`]) re-
validates the post-fusion DAG against T1+T2. A fusion that
introduces a slot-contract violation fails assembly.

#### Axiom NF2 — Determinism preservation

**A Node Fusion rewrite must preserve evaluation determinism:
for every input vector `v`, the rewritten subgraph produces
the same output vector as the original subgraph.**

Enforcement: per-fusion-pattern test gates. The fusion
catalog requires a determinism property test per entry
(equivalence on a representative input space). Subgraph
fusion adds the rewritten node; the test confirms equivalence.

#### Axiom NF3 — Lifecycle preservation or weakening

**A Node Fusion rewrite may preserve or *weaken* the
lifecycle classification of any wire, but may not strengthen
it. That is: Effectively-const → Effectively-const (preserve)
or Effectively-const → Dynamic (weaken) is allowed; Dynamic
→ Effectively-const requires explicit proof and lifecycle
pinning.**

Enforcement: the post-fusion hoisting pass (§3) re-classifies
every wire from scratch. A weakening is visible (formerly
hoisted wires moved to per-cycle). A strengthening is detected
as a lifecycle change in the unexpected direction; the
compiler emits a diagnostic.

#### Axiom NF4 — Compositional closure

**The result of a Node Fusion rewrite is itself a subgraph
eligible for further Node Fusion. The fusion pass iterates
until no further patterns apply (or until a fixed iteration
limit; the limit prevents pathological recursive fusions).**

Enforcement: the fusion pass is implemented as a fixpoint
iteration over the DAG with the registered pattern matchers;
post-fusion DAG re-enters the matcher pool.

### 5.5 Interaction with hoisting and Context Fusion

**Node Fusion precedes hoisting.** The fusion rewrites are
visible to hoisting's lifecycle analysis; the analysis sees
the *fused* graph, not the original. NF3 ensures that
fusion-induced lifecycle changes are visible and monotonic.

**Node Fusion is invisible to Context Fusion.** Context
Fusion operates on the synthesis surface (the input_defs of
the compiled program), which is determined post-fusion. The
synthesis act doesn't know which nodes were fused; it just
fills declared slots.

**Adapter-insertion fusions interact with the slot contract
at construction.** A wire with a `U64` source type connecting
to a `Str` consumer triggers an adapter-insertion fusion
(inserting `U64ToString`). The post-fusion wire chain has
both source and consumer typed correctly; the substrate's T1
+ T2 hold across the inserted adapter.

---

## 6. The pipeline as ordered composition

```text
                  ┌─────────────────────────────┐
                  │   Parse + Bind              │   identifier resolution,
                  │                             │   output declaration
                  └─────────────┬───────────────┘
                                │ DAG
                                ▼
                  ┌─────────────────────────────┐
                  │   Node Fusion (§5)          │   pattern-recognition,
                  │   - Subgraph fusion         │   polyfills, adapters
                  │   - Polyfill insertion      │
                  │   - Adapter insertion       │
                  └─────────────┬───────────────┘
                                │ rewritten DAG
                                ▼
                  ┌─────────────────────────────┐
                  │   Topological sort          │   cycle detection
                  └─────────────┬───────────────┘
                                │ ordered DAG
                                ▼
                  ┌─────────────────────────────┐
                  │   Hoisting analysis (§3)    │   classify every wire
                  │   - Wire classification     │   per H1, H2
                  │   - Boundary identification │
                  └─────────────┬───────────────┘
                                │ annotated DAG
                                ▼
                  ┌─────────────────────────────┐
                  │   Engine selection (SRD-16) │   variant-selection
                  │                             │   heuristic
                  └─────────────┬───────────────┘
                                │ DAG + engine choice
                                ▼
                  ┌─────────────────────────────┐
                  │   Emit GkProgram            │   scope-init path
                  │   - Effectively-const buf   │   + per-cycle path
                  │   - Per-cycle dispatch      │   compiled in
                  └─────────────┬───────────────┘
                                │ Arc<GkProgram>
                                ▼
                       --- compile end ---

                       --- scope-init begin ---

                                │
                                ▼
                  ┌─────────────────────────────┐
                  │   Context Fusion (§4)       │   per-scope-init slot
                  │   - Synthesis surface walk  │   synthesis from outer
                  │   - Slot filling per S2/S5  │
                  │   - Scope-init buffer eval  │
                  └─────────────┬───────────────┘
                                │ ready GkKernel
                                ▼
                       --- READY KERNEL ---
                       (slot contract held)
```

The pipeline's correctness is the composition of every pass's
axioms with the substrate's. The substrate guarantees the
slot contract holds at every cross-pass boundary; this doc's
axioms (H, CF, NF) guarantee each pass preserves the contract.

---

## 7. Cross-references and roles

| SRD | Role under this declaration |
|---|---|
| [Composition Substrate](composition_substrate.md) | The slot contract (S/T/L) this doc's mechanisms preserve. |
| [SRD-10](language_spec.md) | Parse + Bind passes. This doc's pipeline starts with SRD-10's output (assembly DAG). |
| [SRD-11](evaluation_model.md) | Two-lifecycle classification. Hoisting (§3) is SRD-11's classification rule applied compositionally over the wire chain. |
| [SRD-13c](scope_model.md) | Auto-extern + `bind_outer_scope`. The synthesis-surface discovery (S1) and the synthesis act (S2) — Context Fusion's foundations. |
| [SRD-13f](wire_materialization.md) | Gradient classification for outer bindings. CF3 honours SRD-13f's classification; CF cannot rewrite. |
| [SRD-15](../../../docs/sysref/15_strict_mode.md) | Strict mode promotion. H1's totality is enforced at construction; SRD-15 promotes the warnings to errors. |
| [SRD-16](engines.md) | Engine selection (P1/P2/P3/hybrid). The pipeline's engine-selection pass (§6) is owned by SRD-16; this doc references it. |
| [SRD-16b](jit_boundary.md) | JIT boundary. Node Fusion (§5.3 polyfills) interacts with JIT eligibility; SRD-16b owns the boundary semantics. |
| [SRD-67](subcontext_construction.md) | Walled-off construction API. CF1 + CF4 are gated by SRD-67's typed-builder chokepoint. |

---

## 8. Why this matters

The graph compiler is what makes polydat's *zero-overhead
composition* claim concrete. Without the three mechanisms in
this doc, the substrate's slot contract would be a static
property that workload authors would have to manually
honour. With them, the contract is *machinery*: the compiler
holds the substrate's axioms across every authored graph,
automatically.

Three specific properties depend on this:

### 8.1 Workload authors don't write synthesis boilerplate

Per S1 + auto-extern, the compiler discovers what slots
need filling. Per S2 + binding-time materialisation, the
chain fills them. The author writes `query[id={k}]`; the
compiler discovers `{k}` references the outer iter-var;
Context Fusion fills the slot. No "I declare that this
kernel reads k from the outer scope" syntax exists, and none
is needed.

### 8.2 Performance comes from compile-time analysis, not runtime cleverness

Per H1 + H2 + H3, hoisting moves work from per-cycle to
scope-init. The compiler does the analysis once per
`GkProgram`; the runtime executes the partitioned code paths
straight-line. There is no per-cycle "is this value still
the same?" check — the lifecycle classification carries the
proof.

### 8.3 Pattern-level optimisations compose with the substrate

Per NF1–NF4, Node Fusion rewrites preserve the slot
contract. A fusion catalog entry doesn't have to argue from
first principles that its rewrite is sound across every
upstream / downstream combination; it argues from NF1–NF4.
Per-fusion soundness follows from the substrate-level
guarantees + the per-pattern equivalence test.

---

## 9. What this document does NOT specify

- **The per-pattern fusion catalog.** The fusion registry's
  entries are implementation details in `library/`; this
  doc owns the meta-axioms (NF1–NF4) but not the per-pattern
  recognisers.
- **The engine-selection heuristic.** SRD-16 owns the
  variant-selection rules; this doc references the pipeline
  position only.
- **The JIT compiler internals.** SRD-16b owns the Cranelift
  wiring; this doc references the boundary as one of the
  engine variants.
- **Workload-level params and YAML-shape concerns.** SRD-20
  / SRD-21 own those layers; the synthesis surface this doc
  describes is the kernel-level surface, with workload params
  appearing as outer-scope bindings to be synthesised.

---

## 10. Open questions

### 10.1 Polyfill catalog formalisation

§5.3 names polyfills as Node Fusion rewrites that substitute
engine-supported equivalents for non-supported nodes. The
catalog of polyfills is implementation-distributed (each
non-supported node declares its polyfill substitution). A
future revision should formalise the polyfill catalog as a
registry parallel to the fusion catalog, with NF1–NF4
axiomatised over polyfills explicitly.

### 10.2 Cross-scope hoisting (classical sense)

Per §3.0, polydat's current "hoisting" is within-kernel
lifecycle partitioning, not classical cross-scope code
motion. The classical optimisation — moving a computation
from an inner kernel to an outer kernel so the work happens
once across all inner iterations instead of once per inner
materialisation — is currently absent.

Concrete example: in `for_each(k in {k_values}) { ... }`,
if the inner kernel contains a wire whose upstream cone
reaches *only* scenario-level (or workload-level) bindings
— not the for_each iter-var `k` — that wire could in
principle be evaluated once in the *outer* kernel and
re-presented to every inner materialisation as a
pre-filled slot. Today, each inner materialisation
re-evaluates it independently.

Cross-scope hoisting would require: (a) a cross-kernel
classification analysis (which wires in an inner program
reach only outer-scope bindings); (b) a synthesis-time
optimisation where outer evaluates the wire once and
materialise-into-self caches it for each child; (c) cache
invalidation when outer state advances. A future revision
should specify this as an explicit optimisation tier with
its own H-axioms.

### 10.3 Node Fusion ordering — when does the catalog matter?

NF4 establishes fusion as fixpoint-iterating until no further
patterns apply. The *order* in which catalog entries are
applied within an iteration is currently a registration-
order property of the catalog. Some fusion entries' soundness
might depend on a specific application order. A future
revision should either prove order-independence or formally
specify the ordering as part of NF4.

---

[`ast`]: ../../src/ast.rs
[`kernel`]: ../../src/kernel/mod.rs
[`compile::assembly`]: ../../src/compile/assembly.rs
[`compile::fusion`]: ../../src/compile/fusion.rs
[`compile::hybrid`]: ../../src/compile/hybrid.rs
[`compile::select`]: ../../src/compile/select.rs
