# 18b: Scenario Tree, Scope Hierarchy, and Scheduler

How the scenario's *structural* tree (scenarios, control flow,
phases) becomes the *runtime* scope hierarchy, and how a
configurable scheduler controls when scopes execute relative to
each other. Builds on SRD 13b §"Scope composition" and SRD 18.

---

## Why this exists

SRD 18 §"Principles" already stated the design intent:

> Loop counters are explicit. If a loop needs an iteration index,
> it declares a named counter variable. This is a GK scope value
> — visible to all children via the standard scope composition
> mechanism (auto-externs, `shared`/`final`).

The current implementation in `nb-activity` does not do this.
Instead it accumulates iteration variables as a flat
`HashMap<String, String>` and *text-substitutes* `{var}` into the
leaf phase's source before compiling per iteration. That collapses
the tree of scenario nodes into a pair of scopes (workload + leaf
phase), recompiles the leaf each iteration, and prevents
intermediate scopes from carrying their own pragmas, externs, or
runtime state.

This SRD documents what the implementation *should* do — a true
N-level scope hierarchy mirroring the scenario tree — and the
scheduler that decides when those scopes run.

---

## Two trees, two responsibilities

| Tree | What it is | Source of truth |
|------|-----------|-----------------|
| **Scenario tree** | The static structure of the workload as authored: scenarios, `for_each`, `for_combinations`, `do_while`, `do_until`, phases. Each node is a kind. | `nb-workload` parses YAML / `.gk` into this. |
| **Scope tree** | The runtime hierarchy of GK scopes (one `GkProgram` per non-trivial node) plus their pragma chains and extern wiring. Mirrors the scenario tree 1:1 for control-flow and phase nodes. | `nb-activity` builds this from the scenario tree at compile time. |

The scenario tree is what the user wrote. The scope tree is how
GK sees the same structure. The scheduler operates on both:
*structurally* it walks the scenario tree, *semantically* every
step is a scope construction or a scope activation.

---

## Canonical traversal: scope per non-trivial node

Each scenario node maps to a scope as follows:

| Node kind | Scope semantics |
|-----------|-----------------|
| Scenario root | The outermost scope under the workload scope. Carries scenario-name and any scenario-level pragmas. |
| Phase | A leaf scope. Compiles its own `GkProgram` if it has its own bindings or extern needs; otherwise reuses the parent. |
| `for_each var in [...]` | Its own scope. The list elements are *iteration values*; `var` is a binding *output* of this scope (one value per iteration). Children see `var` as an extern. |
| `for_combinations` | One scope per dimension, nested in declaration order. Each dimension's variable is a binding output visible to its inner dimensions and the leaf. |
| `do_while` / `do_until` | Its own scope. Carries the optional counter as a binding output. The condition is evaluated against this scope's outputs after each child execution. |

Scope nesting follows the scenario tree exactly. Three nested
`for_each`s are three nested scopes. The runtime sees:

```
Workload scope               (PragmaSet { ... }, GkProgram_workload)
  │
  └─ Scenario "default"      (parent: → workload)
       │
       └─ for_each x in [1, 2]   (parent: → scenario; output: x)
            │
            └─ for_each y in [a, b]  (parent: → for_each x; output: y)
                 │
                 └─ phase P    (parent: → for_each y; reads x and y as externs)
```

`P` is compiled *once*. Its extern inputs `x` and `y` are
populated by the enclosing `for_each` scopes' current iteration
values. The kernel doesn't recompile across iterations — it
re-runs with new extern values, which is exactly the SRD 16
contract.

---

## Iteration variables as scope outputs

The current "text-substitute `{var}` then recompile" pattern is
replaced by:

1. The `for_each var in expr` scope is a `GkProgram` whose
   *output manifest* contains `var` (typed by the value list's
   element type).
2. Its kernel computes the value for the current iteration. For a
   literal list, that's a select-by-index node. For a
   list-resolved-from-source (e.g. `vector_count`), the
   for_each scope itself depends on its parent's manifest as
   externs.
3. Children declare `var` as an extern (the auto-extern pass
   handles this — names referenced but not defined locally
   become externs typed from the enclosing manifest).
4. At runtime the for_each scope runs N times, each time
   producing a different `var` output, and the child scope's
   extern is bound to the latest output before the child runs.

Same mechanism for `do_while`/`do_until` counters — they're scope
outputs, not text substitutions.

This means **one compile per leaf phase**, regardless of how
many iterations enclose it. Compile cost is paid once at session
start; iteration is just rebinding extern inputs.

---

## Pragma chain along the scope tree

Per SRD 15 §"Pragma Scope":

- Each scope carries its own `PragmaSet` (parsed from its own
  source / config).
- At each compose step (parent scope → child scope), the child
  calls `PragmaSet::attach_to(parent_pragmas)`. The chain walk
  on `strict_types()` / `strict_values()` / etc. resolves through
  parent pointers.
- Conflicts surface as advisories (non-strict) or errors
  (`--strict`) at the boundary where the inner scope is
  constructed.
- A `for_each` declaring `pragma strict_values` applies that
  contract to its *own* scope's externs and outputs, *and* to
  every child scope that doesn't override it. A nested phase
  inherits the for_each's pragmas without re-declaring.

This is invisible to the user when they don't use pragmas. It's
the load-bearing mechanism when they do.

---

## Pre-mapping vs. dynamic walk

The scenario tree is *pre-mapped* before execution:

1. Parse the scenario tree from the workload model.
2. Walk it depth-first, building the scope tree: at each node,
   construct the inner scope by attaching the parent. Compile its
   GK kernel (extern wiring + pragma attach). Record the scope as
   a child of its parent.
3. Result: a fully-compiled tree where every leaf is ready to
   execute. No further compilation happens at runtime — only
   value-rebinding through the extern wires.

This pre-mapping is also what the TUI and observers use to plan
the display before the run starts (the existing
`scenario_pre_mapped` observer hook already does this for phase
listing — extending it to the scope tree is straightforward).

The *dynamic walk* is then purely about scheduling: pick which
scope to activate next, set its extern inputs, run its kernel /
op stanza, repeat. The walk strategy is the scheduler's
responsibility.

---

## Display: flat or hierarchical, same source

The pre-mapped scope tree drives every display mode:

- **Flat list** (today's TUI/CLI): linearise the tree depth-first;
  show each phase or iteration as a line, indented by depth.
- **Hierarchical**: render the tree directly; collapse / expand
  branches; show a phase under its enclosing scenario / for_each
  with proper visual nesting.
- **Activity log**: emit events for scope-enter / scope-exit /
  iteration-start / iteration-end so the consumer reconstructs
  whichever shape it wants.

The accumulator in each case starts from the same canonical tree
— what changes is the rendering, not the underlying structure.
This also restores the lost grouping of *scenario name* vs. its
interior phases: the scenario node is a real ancestor in the
tree, and "phase P in scenario default" is a path query rather
than a flattened label.

---

## Scheduler abstraction

The scheduler is a separate concern from the scope tree. Its job:
given a pre-mapped scope tree, walk it according to a policy and
launch each scope's execution.

### Default: depth-first serial

The current behavior (one scope at a time, traversed
depth-first) is the default. No surprises for existing
workloads.

### Per-level concurrency spec

A workload-level config string controls how concurrent the
scheduler is at each *depth* of the scope tree:

```
schedule=<level0>/<level1>/<level2>/...
```

| Token | Meaning |
|-------|---------|
| `1` | Serial at this level — at most one child runs at a time. |
| `N` (N ≥ 2) | Up to N children concurrent at this level. |
| `*` | Unlimited concurrency at this level. |
| (omitted trailing) | Inherit the previous level's value. |

Levels count from the root. Level 0 is the workload's immediate
children (scenarios). Level 1 is *their* children. And so on.

Examples:

```
schedule=1                # everything serial (default; equivalent to omitting schedule=)
schedule=*                # every level unlimited concurrent
schedule=1/4              # scenarios serial, their children up to 4-way, deeper inherits 4
schedule=1/*/1            # scenarios serial; for_each iterations unlimited; phases serial
schedule=1/4/4/1          # explicit 4 levels
```

Default if omitted: `schedule=1` (serial throughout).

### How it interacts with concurrency control

The existing `concurrency=N` parameter governs *fiber*
concurrency *within a phase* (how many cycles run in parallel
for a single phase op stream). That's orthogonal to the schedule
spec, which governs how many *scopes* run in parallel.

Both can be active. `concurrency=8 schedule=1/4` means: scenarios
serial, up to 4 inner scopes concurrent, and each phase still
runs 8 fibers internally.

### Why per-level rather than per-kind

Per-level keeps the spec a single short string. Per-kind
(`for_each:*,phase:1`) is more expressive but verbose, and the
common cases users actually want are level-shaped: "let
iterations run in parallel but keep phases sequential." If a
workload genuinely needs per-kind control, it can be added later
as a dict syntax in the same `schedule=` parameter without
breaking the level-list form.

### Pluggability

The scheduler is a trait so future strategies (work-stealing,
priority-based, deadline-aware) can plug in without touching the
scope-tree builder. The trait shape:

```rust
trait PhaseScheduler {
    fn run<'a>(
        &'a self,
        ctx: &'a mut ExecCtx,
        nodes: &'a [ScenarioNode],
        bindings: &'a HashMap<String, String>,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;
}
```

The default `TreeScheduler` reads `ctx.schedule_spec` and walks
the tree under those constraints. Concurrency at each depth is a
local decision inside the walk: at a depth where `limit_at(depth)`
is non-serial, sibling scopes (and ForEach iterations at the next
depth) fork via cloned per-task `ExecCtx`, join on a `JoinSet`,
and gate through a `Semaphore` for `Bounded(N)`. The `Arc`-shared
fields (program, scope tree, observers, metrics handles) alias
cheaply across forks; only `label_stack` mutates per task and is
cloned-on-fork so concurrent siblings carry independent label
paths.

---

## Migration

The scope-tree model is a sizeable change to `nb-activity`. The
migration is incremental:

1. **Introduce the canonical scope tree as a data structure** —
   a new `ScopeTree` mirroring `ScenarioNode` 1:1 plus parent
   pointers, depth, scope-level pragma sets, and slots for
   per-node compiled kernels. Build it lazily at first; existing
   code continues to work. *Done — `nb-activity/src/scope_tree.rs`.*

2. **Wire scope-aware compilation** —
   - Per-phase pragma extraction + chain attach
     (`ScopeTree::populate_pragmas`). *Done.*
   - Pragma chain → phase compile via
     `compile_from_scope(... &PragmaSet)` and
     `prepend_effective_pragmas`. *Done.*
   - Iteration variables migrate from text-substitution to
     extern binding (`add_iteration_var` declares a typed
     extern; `run_phase` populates the extern's value per
     iteration). *Done — pure architectural change with the
     same observable behavior.*
   - **Cache-and-rebind**: each phase scope's `GkProgram`
     compiles once and caches in the scope tree node;
     subsequent iterations build a fresh state from the
     cached program, set outer-scope and iteration-variable
     externs, and execute. Replaces the prior "recompile per
     call" pattern in `run_phase`. *Done — see below for the
     contract.*

### Cache-and-rebind contract

Each phase scope node owns a `OnceLock<Arc<GkProgram>>`. It's
empty at scope-tree construction; the first `run_phase` call
that builds a kernel populates it via the existing
`compile_from_scope` path. Subsequent calls retrieve the cached
`Arc<GkProgram>`, instantiate a fresh `GkState` via
`GkProgram::create_state()`, wrap them in a `GkKernel`, set
outer-scope and iteration-variable externs, and proceed exactly
as before.

Properties:

- **One compile per phase.** A workload with
  `for_each row in xs { phase P }` over `|xs|` iterations
  compiles `P`'s kernel once, regardless of `|xs|`. Same when
  the iter chain is deeper.
- **Independent state per call.** Each `run_phase` call gets
  its own `GkState` from the cached program. Concurrent calls
  (when the scheduler eventually allows) don't contend on
  state; the program is `Arc`-shared, immutable after fold.
- **Cache lifetime = session.** No invalidation needed —
  pragmas, externs, and source are static for the session
  once the scope tree is built.
- **Fallback path preserved.** Phases that the executor would
  previously have run against the workload's outer kernel
  (no own bindings, no iter parent) continue to do so without
  touching the cache. The cache is opt-in by virtue of going
  through the inner-kernel path.

Public API extension: `GkKernel::from_program(Arc<GkProgram>)`
makes the cache-and-rebind path expressible without
internal-only constructors.

3. **Pluggable scheduler** — extract the depth-first walk into a
   `PhaseScheduler` trait, parse the `schedule=` spec, and run
   it. *Done.*
   - Trait + serial default + spec parser shipped first.
   - `ExecCtx` got `#[derive(Clone)]`. Every field is either
     immutable post-init or already `Arc`-wrapped, so a clone
     is cheap; `label_stack` is the only piece that forks per
     task. No `SharedCtx`/`LocalCtx` split was needed — the
     `Arc`-via-`Clone` shape captures the same invariants with
     less ceremony.
   - The single `TreeScheduler` impl handles every spec.
     `execute_tree` consults `ctx.schedule_spec.limit_at(depth)`
     at each level: serial → tight loop, non-serial → fork
     each sibling onto `tokio::JoinSet` with a per-task
     `ExecCtx` clone, gated by a `Semaphore` for `Bounded(N)`.
     ForEach (and phase-level for_each) iterations apply the
     same rule at `depth + 1`, so `schedule=1/4` parallelizes
     four iterations of an outer for_each while keeping
     scenarios serial.
   - Integration coverage: `nbrs/tests/concurrent_scheduler.rs`
     proves order-based concurrency at the stderr-log level
     (serial completes phase A before announcing phase B; both
     `schedule=2` and `schedule=*` interleave the entry lines).

4. **Hierarchical display** — surface the scope tree to the TUI /
   observers / web API so renderers can show nesting. *Done.*
   - New canonical type `nb_activity::scene_tree::SceneTree` —
     parent / children pointers, depth tags, per-node lifecycle
     status, DFS iterator, scope-level aggregate-status walk.
     Distinct from the static `ScopeTree`: for_each iterations
     are unrolled here so each iteration is a phase child under
     a per-iteration scope header.
   - `executor::pre_map_tree` now returns `SceneTree` directly
     (was `Vec<PreMapEntry>`). The recursive walk it already did
     is a tree build with the flatten step removed.
   - `RunObserver::scenario_pre_mapped(&SceneTree)` — replaces
     the flat tuple slice. Renderers either store the tree and
     mutate node statuses on lifecycle callbacks, or walk it
     fresh per frame.
   - `nb-tui` keeps a denormalized DFS view (`RunState.phases`)
     that's rebuilt after every tree mutation, so existing render
     code paths read it as before. Heavy `PhaseSummary` data
     lives in a `summaries: HashMap<SceneNodeId, PhaseSummary>`
     side-map keyed by node id.
   - `nb_activity::scene_tree::install_global` / `current` /
     `with_global_mut` give out-of-band consumers a process-wide
     `OnceLock<Arc<RwLock<SceneTree>>>`. The runner publishes
     after pre-map; executor lifecycle emits update the global
     tree at the same callsites that emit observer events.
   - `GET /api/scope-tree` (web) returns the live tree as JSON
     for richer renderers.

Each step is independently shippable and gates the next one.

### Why pragmas don't get their own migration step

Pragmas ride along with their scope. The `pragmas: PragmaSet`
field is on every `ScopeNode` directly; chain-walking is built
into `PragmaSet` (parent pointer + `attach_to`). Once the tree
exists (step 1) and we compile each scope's kernel against its
parent (step 2), pragma propagation is just *populating the
field* and *querying it through the chain*. No separate
machinery; no separate step.

---

## Quick glossary

| Term | Meaning |
|------|---------|
| Scenario tree | Static authored structure (YAML → AST). |
| Scope tree | Runtime hierarchy of GK scopes — same shape as scenario tree, but compiled. |
| Pre-mapping | Build the scope tree at session start; no further compilation at runtime. |
| Iteration variable | A `for_each` / `do_while` counter, exposed as the scope's output binding (not a text substitution). |
| Scheduler | The component that walks the scope tree at runtime and decides concurrency. |
| `schedule=` spec | One-liner describing per-level concurrency: `1/4/*`. Wildcard = unlimited, omitted = inherit. |
