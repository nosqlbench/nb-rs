# 13b: GK Combination Modes

GK lets workloads combine generation graphs in several distinct
ways. Until now those modes have shared overloaded vocabulary —
"module embedding," "kernel composition," "subgraph" all show up
in different SRDs meaning subtly different things. This sysref
nails down the four combination modes and the term for each.

The modes differ on one axis: **how many kernels exist after
compilation**, and **how their state and lifecycle are wired**.

| Mode | After compile | Lifecycle | Where used |
|------|--------------|-----------|------------|
| **Inline** | One kernel | One state | Module bodies, stdlib functions, host workload bindings |
| **Scope composition** | Multiple kernels | Per-scope state, parent feeds child via extern | Workload → phase → `for_each` iteration |
| **Subgraph** | (within one kernel) | (within one state) | Fusion, DCE, stable-cone analysis |
| **Reification** | (one kernel that reads) | Runtime fixtures | Controls, metrics, fiber context |

These are orthogonal: a workload can have inline modules within a
phase scope, with subgraph-level fusion within both, all of which
read reified runtime values via context nodes. The terms apply
independently.

---

## 1. Inline

The host's compile parses the imported module's body and **splices
its `Statement`s into the same DAG**, with name prefixing to avoid
collisions:

```gk
// foo.gk
inputs := (cycle)
inner := mod(hash(cycle), 1000)

// host
inner_value := foo()  // foo's `inner` becomes `__foo_0_inner`
```

After compile:

- One `GkProgram`, one `GkState`.
- The module boundary is gone — there is no runtime "inner kernel."
- Constant folding, fusion, DCE all see the merged graph as a
  single unit.
- Source provenance survives in event-log form
  (`CompileEvent::ModuleInlined`) but not as a runtime structural
  boundary.

**Use when** the module is a reusable subgraph that should
participate in the host's optimization passes. Stdlib helpers,
domain-specific binding macros, and most `.gk` files are inlined.

**Don't expect** independent state, separate fold passes, or
per-module pragma scopes that the host can't see — inline is by
definition flattening.

See SRD 13 §"Module Inlining" and `nb-variates/src/dsl/modules.rs`.

---

## 2. Scope composition

A different operator: **two kernels connected by extern inputs**.
The inner kernel is a real `GkProgram` + `GkState` whose `extern`
ports are populated from the outer kernel's output manifest at
scope-boundary crossings. **No flattening** — each scope retains
its own DAG, its own fold pass, its own runtime state.

```
Workload scope
  GkProgram_outer + GkState_outer
  Outputs: dim, base_count, profiles, …
  │
  └── Phase scope (or for_each iteration)
        GkProgram_inner + GkState_inner
        Extern inputs: dim, base_count (populated from outer outputs)
        Own bindings: id, train_vector, …
        Outputs: id, train_vector, dim, base_count
```

The outer kernel runs once. The inner kernel runs many times (once
per phase entry, once per `for_each` iteration), each time with
the outer's outputs bound to its externs.

**Use when** the inner unit has its own lifecycle: it runs at a
different cadence than the outer, or it iterates with different
inputs, or it should be initialised per-fiber. The phase runner
and `for_each` machinery use scope composition because phases need
their own state and their bindings need to compose with — not
collapse into — the workload bindings.

**Don't conflate with inline.** A binding called `metric_query`
that both files define means "two distinct values, one per scope"
under scope composition — under inline it'd be a name collision.

Cross-scope effects, including **pragma propagation** (SRD 15
§"Pragma Scope"), live at this boundary. Outer-wins conflict
resolution and the strict-mode error policy fire when the outer
scope's `PragmaSet` and the inner scope's `PragmaSet` disagree.
Module inlining (mode 1) does not surface conflicts — it's
additive within one kernel.

See SRD 16 §"GK Scope Model" for the mechanism, and the workload
side in `nb-activity` for the runner that actually wires scopes.

---

## 3. Subgraph

A **region within one kernel's DAG**, named for fusion, DCE, or
performance analysis. Subgraphs are internal structure of an
already-compiled kernel; they are not a composition operator on
their own.

- "Stable subgraph" = a region whose inputs don't change per
  cycle, eligible for full caching.
- "Fusion subgraph" = a pattern (e.g. `mod(hash(x), N)`) replaced
  by a fused node.
- "Output cone" = the subgraph reachable from a given output.

The lifetime of a subgraph is exactly the lifetime of its
containing kernel. Subgraphs do not carry their own pragmas, do
not appear in scope chains, and do not have separate state.

See SRD 11 §"Provenance", SRD 16 §"Engines", SRD 17.

---

## 4. Reification

Runtime fixtures — dynamic controls, metrics, fiber context —
appear inside a kernel as **wires populated externally each
cycle** rather than as a separate kernel. Reification is how
"things outside the kernel" become values inside it; it doesn't
combine two graphs.

Example: `metric("errors", "rate")` looks like a node call but
resolves to a runtime hook reading the current rate from the
metrics system. The kernel sees a wire; the fixture provides the
value.

See SRD 10 §"GK as the unified access surface" and SRD 23.

---

## Retired terminology

**"Module embedding"** (SRD 13) was used in two different senses
in earlier docs:

- "embedding multiple `.gk` files into a single program" — that's
  *inline* applied to multiple sources.
- "embedding a module into a phase scope with extern wiring" —
  that's *scope composition*.

Both should be expressed using the precise terms above. Authors
of new sysrefs should not use "embedding" as a primary term; if
unavoidable, qualify it ("inline-embedding" / "scope-embedding").

---

## Quick reference

| If you mean… | Say… | Don't say |
|--------------|------|-----------|
| Module body spliced into host kernel | inline | embed |
| Phase / iteration with own state | scope composition | inline |
| Region within a kernel | subgraph | scope |
| Runtime value lifted into wires | reification | binding |
| Static / compile-time identity of a kernel + its parent | **scope** | "GK context" |
| Runtime / mutable per-fiber or per-executor state | **context** | "GK scope" |

### "Scope" vs. "context"

The codebase already uses both terms for cleanly distinct things;
keep the distinction:

- **Scope** is structural and static: a `GkProgram` + `GkState`
  pair, its position in the scope tree, its pragmas, its extern
  wiring. The compile-time identity of one nested unit.
- **Context** is runtime and mutable: `FiberContext` (task-local
  state), `ExecCtx` (executor's running state), the compile-time
  diagnostic label string passed to the parser. Things that vary
  during execution.

A scope *has* a context when it's running. They are not synonyms.
Documentation that uses "GK context" interchangeably with "GK
scope" — including earlier drafts of this doc — should be updated
to one of these terms.

---

## Implications for pragmas, controls, metrics

Anything that needs **per-scope visibility** (pragmas, scope
modifiers like `shared`/`final`, scoped controls) lives at scope
composition boundaries. Their rules apply across kernels via
extern wiring and the scope chain.

Anything that needs **whole-DAG visibility within one kernel**
(fusion, fold, type adapters, const constraint validation) lives
at the inline level.

These rules don't overlap: a pragma never causes inline to act
like scope composition, and a fusion never crosses scope
boundaries.
