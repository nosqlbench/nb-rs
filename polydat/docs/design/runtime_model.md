# The Runtime Model — Polydat Design

**Subtitle:** Data Flow, Caching, Invalidation, and the
Determinism Suite.

Formalises the runtime mechanism by which compiled polydat
programs execute. Establishes the R-axioms (runtime
mechanics — data flow, caching, invalidation) and the
D-axioms (determinism guarantees the runtime delivers to
consumers). This doc is the canonical cross-cutting
reference cited by [Expression Engine], [Graph Compiler],
and any future doc that needs to talk about runtime
behaviour.

## Authoritative ownership declaration

This document is the **single authoritative reference** for
polydat's runtime evaluation model — how values flow along
wires, how nodes are cached via clean-flag tracking, how invalidation
propagates (lazily), and what determinism guarantees the
host can rely on. Where the [Composition Substrate]
describes the *static contract* (slot contract via S/T/L
axioms) and the [Graph Compiler] describes the *construction
passes* (H/CF/NF axioms) that produce a `PolydatProgram`, this
doc describes the *execution-time behaviour* that compiled
programs exhibit. The R-axioms (runtime mechanics) and
D-axioms (determinism guarantees) are the load-bearing
contract.

## Companion documents

- [The Composition Substrate](composition_substrate.md) —
  S/T/L axioms; the slot contract this doc's mechanism
  realises at runtime. R3 + D1 build directly on L1 + T1.
- [The Graph Compiler](graph_compiler.md) — H/CF/NF axioms
  for construction. The runtime model is what compiled
  programs do; the H-axioms classify what *will* run when
  and where.
- [The Expression Engine](expression_engine.md) —
  embedded-evaluation surface. E3 (bounded determinism)
  references D1/D2/D3 from this doc.
- [SRD-11: Polydat Evaluation Model](evaluation_model.md)
  — kernel/state split, two-lifecycle classification, const-
  binding contract. Owns the foundational evaluation
  semantics that R-axioms operationalise.
- The host's concurrency model owns the cross-fiber contract:
  D-axioms hold per-fiber (each fiber has its own kernel state);
  the host coordinates across fibers.
- [The Polydat Grammar](grammar.md) — G-axioms. The
  grammar-level commitments that underwrite this doc's
  R/D axioms. G4 (port-typed expressions) underwrites D1
  (typed-return determinism); G5 (structural lifecycle
  classification) underwrites R1 + D3 (cost determinism).

The forcing question: **given a compiled `PolydatProgram` and a
per-fiber `PolydatState`, how do values flow at runtime, what
caching does the kernel perform, how does invalidation
propagate, and what determinism guarantees does the
composition of those mechanics deliver to consumers?** This
doc says: data flows along declared wires alone (R3); nodes
are memoized via per-node clean flags (R1); invalidation
is a hybrid push/pull model — set_inputs proactively
marks dependents dirty, pulls lazily re-evaluate dirty
nodes that the cone reaches (R2); the determinism the runtime
delivers has three explicit bounds (D1, D2, D3), each named
and enforced.

---

## 1. Data flow along the wire chain

Every value in a polydat kernel flows along a **declared
wire**. The compiled `PolydatProgram`'s wiring is the data-
dependency graph: wire `w` connects node `u`'s output port
to node `v`'s input port iff the assembled DAG (per the
Graph Compiler's pipeline) declared that connection. No
data flows outside the wire chain — nodes do not write to
or read from shared state, do not consult global registries
not named in their declared inputs, do not observe timing
or order-of-evaluation beyond their declared input slots.

This is what the substrate calls "data linearisation
embedded in graph structure" — the graph IS the
linearisation. There is no separate execution-order plan
overlaying it.

Concrete consequences:

- Two pulls of the same output, with the same upstream
  state, produce the same flow. No order-of-call dependence.
- Adjacent fibers operating on identical `Arc<GkProgram>`
  with identical `set_inputs` produce identical wire values
  per fiber. The per-fiber `PolydatState` is the only mutable
  surface.
- A node's output is a function of its inputs and its
  configuration. Nothing else.

---

## 2. Dependency tracking

Each wire's **upstream cone** — the set of nodes whose
outputs (transitively) feed into the wire — is known at
compile time, structurally. The Graph Compiler's hoisting
analysis (§3 of [graph_compiler.md]) computes the cone for
every wire as part of lifecycle classification: H1
guarantees totality, H2 guarantees monotonicity under
fan-in.

The compiled program stores cone information for runtime
use: `kernel::compute_provenance` (called during P2/P3
construction) computes a per-node provenance bitmap
recording every input each node ultimately depends on. At
runtime, the pull walker uses this information to know
which subset of the DAG to traverse for a given output.

**The host can ask of any output: which inputs is this a
function of?** The answer is exact, computed at compile
time, constant across evaluations. There is no runtime
discovery of dependencies; everything is structural.

---

## 3. Node caching — per-eval clean tracking

`PolydatState` maintains a per-node `node_clean: Vec<bool>`
flag. When the kernel pulls a value, it walks the cone
recursively from the requested output and for each upstream
node checks whether it has already been evaluated since
its last dirtying:

```text
pull(name):
  let node_idx = program.output_map[name].node
  eval_node(program, node_idx)
  return state.buffers[node_idx][port_idx]

eval_node(program, node_idx):
  if state.node_clean[node_idx]:
    return                           # already fresh; cached
  for source in program.wiring[node_idx]:
    if let NodeOutput(upstream_idx, _) = source:
      eval_node(program, upstream_idx)
  // gather inputs from upstream buffers / input slots
  node.eval(inputs, outputs)
  state.node_clean[node_idx] = true
```

The effect: **a node is evaluated at most once between any
two dirtying events**. Multiple pulls touching the node
between dirtying events reuse the cached output. This is
the substrate's "T1 slot contract" property realised at
runtime — same inputs at the slot tier, same outputs at
the slot tier, and we trust the cache.

The **Effectively-const buffer** (per the Graph Compiler's
hoisting analysis) is the special case: its values are
computed once at scope-init and never dirtied within the
scope's lifetime. Their `node_clean[i]` stays `true`
across every per-cycle pull; the walker visits them once
during scope-init and never re-evaluates.

### Axiom R1 — Per-eval clean-flag memoization

**A node's `eval` is invoked at most once between any two
dirtying events in a given `PolydatState`. Multiple pulls
touching the node between dirtying events use the cached
result; the cache is reset only when an upstream input
change marks the node dirty.**

Enforcement: the pull walker's `node_clean[i]` check (see
pseudocode above). The substrate's L1 (each layer owns its
state) guarantees that `node_clean[i]` is owned by this
fiber's `PolydatState`; no cross-fiber cache contention.

### Sub-axiom R1.v — Volatility carves out per-cycle clean-flag memoization

**A wire is *volatile* when its value is not a function of
its declared inputs. Volatile wires opt out of per-cycle
clean-flag memoization: every cycle (every `set_inputs`
advance) re-evaluates the producing node on next pull,
regardless of whether any of the node's declared inputs
changed. Within a single cycle (between two `set_inputs`
calls), the node is evaluated at most once and the result
is cached for subsequent reads — this gives consumers
within-cycle consistency. Volatility is contagious —
the compiler's lifecycle classifier propagates the Dynamic
classification through the wire chain so every node whose
dependency cone touches a volatile producer is itself
treated as Dynamic.**

Volatility arises from two distinct sources:

- **Intrinsic.** A library node declares itself volatile by
  returning `Purity::Nondeterministic { reason }` from
  `GkNode::purity`. Examples: `current_epoch_millis`,
  `counter`, `elapsed_millis`, `thread_id`,
  `session_start_millis`, entropy sources, and any node
  whose output is not a pure function of its declared
  inputs. The library imposes volatility; no user opt-in is
  required, and the workload author cannot remove the marker.
- **User opt-in.** A wire's binding declares the `volatile`
  modifier, marking the wire as must-not-be-const-folded.
  The author is asserting that the value should never be
  cached across cycles even though the compiler can't infer
  it from the wire chain (e.g., a node that reads external
  mutable state the polydat layer cannot see).

Both sources produce identical runtime behavior:

- The wire is excluded from compile-time const-folding —
  the canonical workload hash sees node-type + wiring shape
  but never the value, keeping workload identity stable
  across processes.
- The lifecycle classifier marks the producing node
  Dynamic; the fixed-point propagation pass then marks
  every downstream consumer Dynamic too. Dynamic nodes are
  re-evaluated each cycle when their (now-dirty) inputs
  resolve.
- For intrinsic-volatile nodes specifically (those with
  `Purity::Nondeterministic`), the `nondeterministic_nodes`
  list at construction time records them for unconditional
  per-cycle dirty marking — `set_inputs(coords)` resets
  their `node_clean` flag every cycle whether or not their
  declared inputs changed.
- The intrinsic declaration is authoritative: an absent
  user modifier does not override a library-declared
  volatile node, and a present user modifier on a wire that
  reads a library-declared-pure node still makes the wire
  itself volatile (and contaminates its downstream via the
  lifecycle propagation).

**Within-cycle consistency.** Within a single cycle's
evaluation window, a volatile node is evaluated at most
once and cached — consumers reading the same value
multiple times during one op-execution observe a
consistent value. The "every cycle re-evaluates" guarantee
is at the cycle granularity, not per-individual-pull. This
is the correct semantic for temporal nodes (a single op
reading `current_epoch_millis` multiple times sees one
consistent timestamp for that cycle) and matches the
runtime mechanism polydat actually delivers.

What volatility is NOT for: ordinary external-write inputs
(per composition_substrate S4). The provenance machinery
handles re-evaluation correctly via input-change tracking
when an external write modifies a slot — the consumers
re-evaluate on next pull through the standard R2 dirty
mechanism. Volatility is the explicit marker for the
genuinely-non-deterministic case where input-change
tracking is insufficient because the value does not depend
on the declared inputs at all.

Enforcement: at construction time, `program.rs::create_state`
builds `nondeterministic_nodes: Vec<usize>` from nodes that
either are nullary (no declared inputs) OR return
`Purity::Nondeterministic` from `GkNode::purity`. At runtime,
`set_inputs(coords)` walks the list and unconditionally
clears `node_clean[idx]` for each entry. User-opt-in
`volatile` modifier is enforced by the lifecycle classifier:
the producing node is marked `EvalLifecycle::Dynamic` and the
fixed-point propagation contaminates downstream consumers.
The transitive set is computed
`PolydatProgram`, not the per-fiber `PolydatState`. The grammar's
`volatile` modifier (G2) is the syntactic surface; SRD-10
documents the specific modifier syntax.

---

## 4. Invalidation effects

`set_inputs(&[u64])` writes new coordinate values into the
input slots AND **proactively marks dependent nodes
dirty** by looking up the per-input dependent list
(`input_dependents: Vec<Vec<usize>>`, precomputed at
construction time from the wire-chain provenance):

```text
set_inputs(coords):
  for i in 0..coords.len():
    state.inputs[i] = Value::U64(coords[i])
    for node_idx in input_dependents[i]:
      state.node_clean[node_idx] = false   // push-side dirty mark
  for node_idx in nondeterministic_nodes:
    state.node_clean[node_idx] = false     // always-dirty floor

pull(name):
  // walks the cone, evaluating any node whose clean flag
  // is false; cached otherwise. See §3.
```

This is the **hybrid push/pull invalidation model**: the
dirty *signal* is push-side (input changes proactively
mark dependents dirty); the dirty *response* is pull-side
(`eval_node` lazily re-evaluates only when reached by a
pull). The model has three named properties:

- **Lazy at the pull side.** Unused outputs are never
  recomputed. If a host call pulls only output `out1`,
  dependencies of `out2` that share upstream with `out1`
  are evaluated (because they're in `out1`'s cone), but
  dependencies of `out2` that are *not* in `out1`'s cone
  are not visited even if they were marked dirty by
  `set_inputs`.
- **Eager at the push side.** `set_inputs` does proactive
  dirty-marking via the precomputed `input_dependents`
  list. The lookup is O(dependents of the changed input),
  not O(total nodes); the dependent list is structural
  (computed at compile time from `compute_provenance` +
  `compute_dependents` in `kernel/program.rs`).
- **Forward-only.** Dirty marks propagate forward along
  the wire chain (an input change dirties downstream
  consumers, not upstream producers). Invalidation never
  crosses a layered scope boundary unguarded (S5's
  `SharedCell` write-through is the only legitimate
  cross-tier write surface).

### Axiom R2 — Hybrid push/pull invalidation

**Invalidation in polydat is a hybrid: `set_inputs`
proactively marks dirty every node whose upstream cone
reaches a changed input (via the precomputed
`input_dependents` list); subsequent pulls then lazily
re-evaluate only the dirty nodes that the pull's cone
walk reaches. Nodes not reached by any pull are never
re-evaluated, regardless of upstream changes.**

Enforcement: `set_inputs` in `kernel/engines.rs` walks
`input_dependents[i]` for each changed input index and
sets `node_clean[node_idx] = false`. `eval_node` in the
same file returns early if `node_clean[node_idx]` is
true. The two halves together realise the hybrid model.

### Axiom R3 — Forward-only data flow

**Data flow in a kernel evaluation is forward-only along
declared wires from input slots to output slots.
Invalidation never propagates backward; cross-tier writes
are restricted to the substrate's S5 SharedCell write-
through mechanism; there is no out-of-band data channel
between nodes or between scopes.**

Enforcement: the wire-chain structure is acyclic (the
assembler rejects cycles per `AssemblyError::CycleDetected`);
the pull walker visits nodes in topological order; the
substrate's S5 is the only cross-tier write surface. SRD-67
walls off any alternative construction path that could
violate this.

---

## 5. State-layering at runtime

The substrate's L-axioms hold at runtime with these specific
realisations:

| L-axiom | Runtime realisation |
|---|---|
| **L1** (each layer owns its state) | Per-fiber `PolydatState`. The kernel's program is `Arc<GkProgram>` (shared, read-only); state is owned by the fiber that holds the kernel. No cross-fiber state sharing at the node tier. |
| **L2** (two-lifecycle classification bridges layers) | Effectively-const wires are evaluated once during the kernel's scope-init phase; dynamic wires are evaluated on demand per `set_inputs` advance. The buffer layout reflects this — Effectively-const values live in a separate region computed at scope-init. |

The runtime model is the *enactment* of the substrate's
layered state contract: at every cycle, every layer's state
is owned by its layer (L1); every cross-tier read goes
through synthesised slots (S1+S2); every cross-tier write
goes through S5's chokepoint. The runtime mechanism
preserves the layering inherited from compilation.

The S-axis axioms beyond S1+S2 have their own runtime
realisations alongside R1/R2/R3:

| S-axiom | Runtime realisation |
|---|---|
| **S4** (external-write synthesis, open granularity) | External-write slot values (`GkState.port_values`) are populated through the kernel's typed-write API at any granularity the producer chooses; the provenance machinery (R2) marks consumers dirty on write; clean-flag memoization (R1) re-evaluates on next pull. Volatility (R1.v) is the explicit marker for wires whose value is not a function of declared inputs and so cannot be cached even between pulls. |
| **S5** (compile-emit write-through, cross-tier path) | `SharedCell` write-through routes a writing node's output to a parent-tier cell at compile-emit time (per Graph Compiler §5); at runtime the write fires as an ordinary node output, intercepted by the chain and propagated outward. The outer cell's slot is filled through the standard slot-filling contract; L1's layer-ownership guarantee holds because the outer cell remains the canonical state holder. |

---

## 6. Determinism — the D-axiom suite

The R-axioms (R1, R2, R3) describe the runtime mechanics.
The D-axioms describe the **determinism guarantees** the
runtime delivers as consequences of those mechanics. Three
distinct bounds, each named.

### Axiom D1 — Typed Return Determinism

**For a fixed `Arc<GkProgram>`, fixed input vector, and
fixed node registry, the typed return value at every
declared output is byte-identical across evaluations,
across fibers, across processes. This holds
unconditionally — even when the program includes impure
constituent nodes — because the substrate's slot contract
(T1 + T2) carries only typed values, and impure side
effects do not cross slot boundaries into adjacent nodes'
typed inputs.**

Enforcement: composition of R1 (memoization), R3 (forward-
only data flow), T1+T2 (typed slot contract), and the
substrate's L1 (per-fiber state ownership). The compiler's
H3 (hoisting preserves value) seals the property at the
construction tier.

D1 is the strongest determinism guarantee and the cheapest
to verify — the host pattern-matches on the typed return
and gets identical bytes per run.

### Axiom D2 — Side-Channel Determinism

**Impure constituent nodes' side channels (logging output,
file I/O, network calls, etc.) are deterministic
*conditional on each impure node's declared semantics*. A
node that writes "X" to stderr for input `i` produces an
identical stderr line for input `i` every time. A node
whose side channel depends on external state (e.g., wall
clock, process ID, network state) produces side effects
deterministic only modulo that external state.**

The substrate's slot contract bounds impurity: side
effects are *additional* observables outside the typed
return; the typed return is still deterministic per D1.
What varies between evaluations is the impure node's side
channel, not the typed result the host receives.

Enforcement: per-node metadata declarations (currently
implicit via JIT compile-level; explicit `GkNode::purity()`
planned per Expression Engine §12.2). Hosts that care
about side-channel determinism examine the constituent
nodes' declared purity status.

### Axiom D3 — Cost Determinism

**The cost of a single evaluation — measured as count of
node `eval` invocations — equals the cone size of the
output(s) pulled, minus the count of currently-clean nodes
in that cone. The cost is a structural property of the
compiled program; it does not depend on runtime values or
evaluation history (modulo the cache state captured by
each node's `node_clean` flag).**

Enforcement: R1 (clean-flag memoization bounds eval count
to one per dirty-to-clean transition) + R2 (no work for
unreached cones) + structural cone size (compile-time
computed via `compute_provenance` in `kernel/program.rs`).
The host can predict evaluation cost from the program's
structure plus the current state's cache profile.

Cost determinism gives the host a predictable performance
model: a small expression with a 3-node cone costs 3 evals
on a cold state (all nodes dirty), 0 on a fully-warm state
(all nodes clean), with the intermediate region
characterised by which nodes were dirtied by the most
recent `set_inputs`.

---

## 7. The R-axioms and D-axioms compose

```text
       ┌──────────────────────────────────────┐
       │  R1 — clean-flag memoization         │   ≤1 eval/dirty→clean
       │  R2 — hybrid push/pull invalidation  │   push-dirty + pull-lazy
       │  R3 — forward-only flow              │   wire chain is the path
       └────────────────┬─────────────────────┘
                     │
                     ▼  yields
                     │
       ┌───────────────────────────┐
       │  D1 — typed return        │   bytewise identical
       │  D2 — side channels       │   conditional on metadata
       │  D3 — cost                │   structurally bounded
       └───────────────────────────┘
```

R-axioms describe **how** the runtime evaluates. D-axioms
describe **what guarantees** the host can rely on as a
consequence. Together they form the runtime contract:
mechanism plus guarantees, neither one alone sufficient.

A reader who wants to understand "what does polydat
runtime evaluation deliver?" reads §6 (D-axioms). A reader
who wants to understand "how does polydat make those
guarantees real?" reads §3–§5 (R-axioms and state layering).

---

## 8. SRD cross-references and roles

| SRD / doc | Role under this declaration |
|---|---|
| [Composition Substrate](composition_substrate.md) | The static contract (S/T/L axioms). D1's typed-return guarantee follows from T1+T2 at the slot tier. |
| [Graph Compiler](graph_compiler.md) | Construction passes (H/CF/NF axioms). The R-axioms operate over compiled output; H1's lifecycle classification determines what runs at scope-init vs per-cycle. |
| [Expression Engine](expression_engine.md) | Embedded-evaluation surface. E3 references D1/D2/D3 as the realisation of bounded determinism. |
| [SRD-11](evaluation_model.md) | Foundational evaluation semantics. R1 / R2 are SRD-11's two-lifecycle classification at runtime. The const-binding contract is the scope-init expression of R1. |
| [SRD-13f](wire_materialization.md) | Cross-scope read/write. R3's "forward-only with S5 carve-out" cites SRD-13f's SharedCell write-through as the named exception. |
| [SRD-67](subcontext_construction.md) | Walled-off construction. SRD-67's API prevents alternative construction paths that could violate R3. |
| [SRD-74](none_semantics.md) | `Value::None` propagation. D1 holds for None propagation — same input None → same output None — because T1 carries the None typing as part of the slot contract. |

---

## 9. Open questions

### 9.1 Cross-fiber determinism — formal statement

D1 holds *per fiber*. The cross-fiber claim (two fibers
with identical program + identical state produce identical
output) is currently informal; a future revision should
state it as D4 or a corollary to D1, with explicit
reference to SRD-02's no-shared-mutable-state rule.

### 9.2 Cost-of-cache-warmup characterisation

D3 names cost as cone size minus warm nodes. A formal
characterisation of cache warmup curves (how many calls
before a cone is fully warm) would help hosts predict
amortised cost. Profile-driven; only worth formalising if
host consumers measure this.

### 9.3 Side-channel observability granularity

D2 holds conditional on declared per-node semantics. Some
side channels (e.g., stderr writes that interleave between
nodes within a single pull's cone walk) are deterministic at the
*per-node* level but produce non-deterministic *combined*
output when multiple impure nodes share a sink. A future
revision should specify the granularity of D2's claim and
how multi-node side-channel ordering composes.

### 9.4 R-axiom interaction with the parallel evaluator

The runtime model assumes pull-through within a single
fiber. The future parallel evaluator (running independent
subtrees concurrently within one fiber's pull) needs an
explicit R4 covering parallel-subtree caching and the
ordering of side effects D2 references. Deferred until the
parallel evaluator is specified.

---

[`Expression Engine`]: expression_engine.md
[`Graph Compiler`]: graph_compiler.md
[`Composition Substrate`]: composition_substrate.md
[`composition_substrate.md`]: composition_substrate.md
[`graph_compiler.md`]: graph_compiler.md
