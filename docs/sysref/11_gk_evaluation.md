# 11: GK Evaluation Model

The GK evaluation model separates the immutable program (shared)
from mutable per-fiber state (private). This enables lock-free
concurrent evaluation across hundreds of fibers.

---

## Program / State Split

```
GkProgram (Arc, immutable, shared)
  ├── nodes[]          — node instances
  ├── wiring[]         — input source tables
  ├── input_names[]    — input dimension names
  ├── output_map       — name → (node_idx, port_idx)
  └── ports             — external port definitions (captures)

GkState (per-fiber, mutable, private)
  ├── buffers[][]        — per-node output value slots
  ├── node_clean[]       — per-node cache validity (bool)
  ├── inputs[]           — current input values
  └── port_values[]      — external ports (persist across set_inputs)
```

`GkProgram` is created once at compilation time and shared via
`Arc` across all fibers. `GkState` is created per-fiber via
`program.create_state()`.

---

## Provenance-Based Invalidation

Each node has a compile-time **provenance bitmask**: bit i is set
if the node transitively depends on graph input i. On input
change, only nodes whose provenance overlaps the changed inputs
are invalidated. Nodes depending on unchanged inputs stay cached.

```
1. fiber.set_inputs(&[cycle])
   → compare each input old vs new
   → build changed_mask (only inputs that actually changed)
   → for each node: if (provenance & changed_mask) != 0 → dirty

2. state.pull(program, "user_id")
   → if node_clean[node] → return cached buffer
   → recursively evaluate dirty upstream nodes
   → gather inputs, evaluate, mark node_clean = true
   → return &buffers[node_idx][port_idx]
```

This replaces the previous generation counter model. Nodes that
don't depend on the changed input skip evaluation entirely —
no generation comparison, just a boolean check.

**Diamond optimization:** In a diamond-shaped DAG where only one
input branch changed, the unchanged branch stays cached. The
generation counter model re-evaluated everything.

**Memoization granularity:** Every node's output buffer is cached.
For diamond-shaped flows where intermediate nodes are never
directly referenced as outputs, this memoization
has no consumer — the intermediate values are computed, cached,
and then recomputed from scratch on the next input change
anyway. A more targeted approach would memoize only at output
nodes and nodes with multiple downstream consumers. See
[10: GK Language](10_gk_language.md), Incremental Invalidation
for the broader discussion of provenance-based invalidation.

---

## Three Evaluation Lifecycles

A GK node's *lifecycle* is the granularity at which it is
re-evaluated. Three are recognised, ordered from coldest to
hottest:

| Lifecycle | When evaluated | Re-evaluated when… |
|-----------|----------------|---------------------|
| **compile-const** | Once, during GK compilation | Never. Replaced in the DAG with a leaf const node. |
| **scope-init** | Once per scope activation, immediately after `bind_outer_scope` populates iteration-variable externs | The enclosing comprehension advances to its next iteration (the scope is re-activated with new extern values). Never per cycle. |
| **dynamic** | Once per pull, on demand at execution time | Whenever a transitively dependent input changes (provenance-based invalidation). Includes per-cycle pulls *and* intra-stanza recomputation when capture ports or `do_while`/`do_until` counters tick. |

The previous binary "init-time vs cycle-time" terminology
collapsed compile-const and scope-init into one bucket. They are
distinct: compile-const is resolved before the kernel is even
shared; scope-init is resolved each time an enclosing comprehension
re-activates the scope. Both are **scope-stable** — they hold a
single value for the entire activation of their owning scope —
which is the property the init-binding contract below depends on.

### Effectively-Const Nodes

A node is **effectively-const** at a given scope-init point if
it produces exactly one value for the entire activation of the
owning scope. The set is closed under upstream traversal: a node
whose every upstream wire reaches an effectively-const producer
is itself effectively-const.

| Producer | Effectively-const? | Why |
|----------|-------------------|-----|
| Literal in source | Yes | Resolved at parse / compile. |
| Compile-const fold result | Yes | Already a leaf const node. |
| Workload param (`final` binding) | Yes | Bound once at workload-kernel init, never reassigned. |
| `for_each` / `for_combinations` iteration extern | Yes — *for the duration of one activation* | Rebound by `bind_outer_scope` on each iteration; held constant for every cycle within that iteration. See [SRD 18b §"Iteration variables as scope outputs"](18b_scenario_tree_and_scheduler.md). |
| `do_while` / `do_until` counter | **No** | Dynamic — ticks within the scope's own evaluation; not stable for the activation. |
| Graph input (e.g. `cycle`) | **No** | Dynamic — changes every cycle. |
| Capture / volatile port | **No** | Dynamic — mutated by op execution within a stanza. |
| Non-deterministic source (`counter`, `current_epoch_millis`, `elapsed_millis`, `thread_id`) | **No** | Excluded by construction even when wires would suggest otherwise. |

The iteration-extern entry is the load-bearing case the prior
"binary" model handled wrong. A leaf phase nested inside
`for_combinations [profile, table]` sees `profile` and `table`
as input slots; the data-flow analysis flagged any binding
downstream of those slots as dynamic and refused to fold it.
But `profile` is rebound exactly once per phase activation and
held fixed for every cycle — the same stability guarantee as a
folded literal. Treating iteration externs as effectively-const
is what permits `init prebuffered = dataset_prebuffer("{dataset}:{profile}")`
to be a legal init binding inside such a scope.

### Compile-Time Constant Folding

Compile-time fold is the implementation of the compile-const
lifecycle. It runs once per `GkProgram` build, before the program
is wrapped in `Arc` and shared:

```
Phase 1: Classify each node by upstream wire chain
  - Graph input / external port / non-deterministic source
                                  → not effectively-const
  - NodeOutput whose source is not effectively-const
                                  → not effectively-const (propagates)
  - Wire to an iteration extern (for_each / for_combinations)
                                  → not effectively-const at *compile*
                                    time. Extern values are unknown
                                    until scope activation; folding is
                                    deferred to the scope-init pass.
  - Everything else               → effectively-const at compile time

Phase 2: Evaluate compile-const nodes with dummy inputs

Phase 3: Replace evaluated nodes with leaf const nodes
         (ConstU64, ConstF64, ConstStr, ConstHandle, …)
```

Type adapter nodes (`__u64_to_f64`, etc.) participate. A chain
like `ConstU64(42) → __u64_to_f64 → sin` folds to
`ConstF64(sin(42.0))` — the whole chain is evaluated once and
replaced with a single constant.

Folded constants are available via `kernel.get_constant(name)` for
activity config resolution (cycles, concurrency from dataset
metadata).

### Scope-Init Pass

The scope-init pass is the implementation of the scope-init
lifecycle. It runs once per scope activation, *after*
`bind_outer_scope` has populated the kernel's iteration-extern
input slots and *before* any fiber is created.

```
For each declared init binding b in this scope's program:
  1. Pull b's name on the activation kernel's state. The standard
     pull walks back through b's subgraph, evaluating each upstream
     node against the populated externs and caching the result in
     the state's per-node buffer (clean flag set to true).
  2. Verify the resulting value is non-`None` (Plan B, below).

After every init binding has been pulled, the executor wraps the
kernel in an `OpBuilder` that snapshots the
`(node_idx, port_idx, Value)` triples for those bindings as
`init_overrides`. Each fiber spawned from this `OpBuilder` seeds
the triples into its own state's buffers and marks the
corresponding nodes clean. A fiber's first dynamic pull of an
init binding reads the seeded buffer directly — the binding's
eval function does not re-fire, regardless of how many cycles
or fibers traverse it.
```

This is the runtime side of the init-binding contract: one eval
per scope activation, full stop, regardless of fiber count.

Reference points in the code:
- `nbrs_variates::kernel::engines::GkState::seed_node_buffer` —
  primitive that writes a value into a node's buffer slot and
  marks it clean.
- `nbrs_activity::synthesis::OpBuilder::init_overrides` — the
  per-activation snapshot that fiber state inherits.
- `nbrs_activity::executor::run_phase` Plan B block — the
  per-init-binding pull + non-None verification, immediately
  after `kernel.bind_outer_scope(parent_kernel)`.

---

## Init Binding Contract

`init <name> = <expr>` is a **const-like constraint**: it asserts
that `<expr>` evaluates to a single value for the entire
activation of the enclosing scope. The compiler and runtime
together enforce two checks:

### Compile-Time Check (Plan A)

During GK compilation, after wire resolution and topological
sort:

> For every binding declared `init`, every node in its upstream
> wire chain must be either compile-const or an iteration extern
> (effectively-const at scope-init time per the table above).

If any upstream node is non-effectively-const — a graph input,
a capture port, a `do_while`/`do_until` counter, a chain through
a non-deterministic source — compilation **fails** with a
diagnostic naming the init binding and the offending wire. There
is no soft fall-through to dynamic evaluation.

This check runs in the compile-time fold pass. Compile-const
classification (above) and the init-binding check share the same
upstream walk; the init check simply demands the upstream set be
a subset of `{compile-const ∪ iteration externs}`.

### Scope-Activation Check (Plan B)

After scope-init evaluation runs (the scope is activated, externs
populated, the scope-init pass has stashed values), the kernel
verifies:

> Every binding declared `init` has produced a single concrete
> value and is materialized as a leaf const-like node (ConstU64,
> ConstF64, ConstStr, ConstHandle, etc.) — no remaining wires,
> no deferred eval.

If any init binding fails to materialize — most commonly because
its value type is not foldable to a leaf node, or its eval
returned `Value::None`, or a panic was caught and the node was
left unfolded — this is a **hard runtime error** at scope
activation, before any cycles run. The phase fails to start;
the diagnostic names the binding, the residual node type, and
the eval result.

Plan A is the type-system-style check that runs at compile time
when iteration-extern values are unknown but the wire structure
is fully visible. Plan B is the construction-correctness check
that runs at scope activation when the values are known and the
fold pass has had its chance. Together they ensure: an init
binding either evaluates exactly once per scope activation, or
the workload refuses to run.

### Why Both Checks

Plan A alone catches structural errors at workload-author time
(no need to wait for runtime; failures travel with the source).
But it cannot catch runtime conditions — a remote facet that
returns 403, an opaque eval panic, a `Value::None` from an
otherwise-valid init function — because those depend on real
extern values.

Plan B alone is robust against runtime conditions but defers
clear structural errors (e.g. an init binding that wires through
a `cycle`-dependent node) to runtime, where the failure surface
is larger and the diagnostic less localized to the source line.

Both are cheap. Both run at most once per scope activation. The
combined check is the contract.

### Diagnostic Format

Both checks emit the same shape — `init binding '<name>'
violates the init contract: <reason>`. The reason names the
offending wire (Plan A) or the runtime failure mode (Plan B).
Plan B errors carry the executor's `gk_context` prefix
identifying the phase / scope.

Plan A reasons (compile-time, from
`fold_init_constants_impl`):

- **`wire on node '<n>' reaches coordinate input '<name>'
  (dynamic; changes every cycle)`** — init binding wired to a
  graph input declared by `input ...: u64`.
- **`wire on node '<n>' reaches capture port '<name>' (dynamic;
  mutated by op execution)`** — init binding wired to an
  `extern X: T = default` port (capture surface).
- **`wire on node '<n>' reaches non-deterministic source '<name>'
  (dynamic by construction)`** — `counter`, `current_epoch_millis`,
  `elapsed_millis`, `session_start_millis`, or `thread_id`.
- **`wire on node '<n>' reaches dynamic node '<upstream>'
  upstream`** — fallback when the chain is dynamic but the
  immediate seed isn't one of the patterns above (e.g. a chain
  through a `do_while` counter).

Plan B reasons (scope-activation, from `executor::run_phase`):

- **`scope-init eval returned Value::None`** — the eval function
  signaled a fatal failure (e.g. `dataset_prebuffer` couldn't
  resolve the source) and refused to produce a value.
- **`scope-init eval panicked: <message>`** — the eval function
  panicked; details captured via `catch_unwind`. The panic does
  *not* poison the fiber pool; the phase fails to start cleanly.

---

## Non-Deterministic Nodes

`counter`, `current_epoch_millis`, `elapsed_millis`, `thread_id`
are excluded from compile-const folding *and* from
effectively-const classification regardless of their input
wires. They are inherently dynamic even when a static analysis
would suggest otherwise. An `init` binding that depends on one
of these fails the Plan A check.

---

## Input Spaces

Most workloads use a single `cycle` input. Multi-dimensional
inputs enable nested iteration:

```
input cycle: u64

// Mixed-radix decomposition: flat cycle → nested indices
row := mixed_radix(cycle, 1000, 0)     // cycle / 1000
col := mixed_radix(cycle, 1000, 1)     // cycle % 1000
```

The input space is defined inside GK, not in the activity
layer. This enables composition with other nodes and keeps the
executor simple (it just passes `[cycle]`).

---

## Capture Context

External values injected into the GK evaluation via ports:

- **Volatile ports**: reset to defaults on `set_inputs()`.
  Used for per-cycle external inputs.
- **Sticky ports**: persist across cycles within a stanza.
  Used for inter-op capture flow.

```
Op A executes → captures "user_name" from result
  → fiber.capture("user_name", value)
  → writes directly to port in GkState

Op B resolves → {user_name} reads from port via GK wiring
```

Ports persist across `set_inputs()` calls within a stanza.
`reset_ports()` is called at stanza boundaries to prevent
capture leakage.

---

## Compilation Levels

The compiled DAG can run at one of three execution levels
— P1 interpreter, P2 closures, P3 Cranelift JIT — selected
automatically per subgraph based on node eligibility and
projected payoff. Per-node costs, eligibility rules, the
auto-selection heuristic, and the JIT call-boundary
contract live in **SRD 16 (GK Engines)** and
**SRD 16b (GK JIT Wiring)**.

This file (SRD 11) covers what *evaluation* is —
program/state split, lifecycles, provenance — independent
of which engine runs it.

---

## FiberBuilder

The per-fiber bridge between GK and the execution engine:

```rust
pub struct FiberBuilder {
    program: Arc<GkProgram>,   // shared, immutable
    state: GkState,            // per-fiber, mutable
}

impl FiberBuilder {
    pub fn new(program: Arc<GkProgram>) -> Self;
    pub fn set_inputs(&mut self, inputs: &[u64]);
    pub fn resolve_with_field_pulls(
        &mut self, template: &ParsedOp, field_pull_names: &[String]
    ) -> ResolvedFields;
    pub fn resolve_pulls(&mut self, plan: &PullPlan) -> ResolvedPulls;
    pub fn capture(&mut self, name: &str, value: Value);
    pub fn reset_captures(&mut self, cycle: u64);
    pub fn apply_captures(&mut self);
}
```

No separate params argument — workload params are injected into
the GK source as constant bindings before compilation and resolve
as normal GK outputs. No globals mechanism needed.

`resolve_with_field_pulls` iterates the op's field map, substitutes
`{name}` bind points from GK outputs and captures, and additionally
pulls each name in `field_pull_names` (the union of bind-point
names referenced by op fields) into `ResolvedFields` for the inner
adapter's name-keyed reads.

`resolve_pulls` materializes a [`PullPlan`] (sealed at init from
the per-template `ScopeFixture`, SRD 32 §"Init-Time Fixture and
Consumer Self-Registration") into a `ResolvedPulls` keyed by
`PullHandle`. This is the wrapper-side read path — distinct from
`ResolvedFields` and bundled alongside it in `ExecCtx` (SRD 31
§"Pull plan vs bind plan").

---

## Cursor-Driven Evaluation

When a GK program declares `cursor` bindings, the evaluation
model extends from counter-driven to cursor-driven iteration.
A cursor is a GK node whose output is a `u64` ordinal. The
runtime advances the cursor externally; downstream accessor
nodes re-evaluate via provenance-based invalidation.

### Advance / Access Separation

The cursor model separates **advance** (moving the position
forward) from **access** (reading data at the current position):

1. **Advance**: The runtime calls `Cursors::advance()` to move
   each targeted cursor to its next position. This is a pull
   from the underlying `DataSource` reader.
2. **Inject**: `Cursors::inject_into_state()` writes the new
   ordinal into the GK state's input slot for the cursor.
3. **Access**: The GK DAG re-evaluates. Accessor functions
   (e.g., `vector_at(base, ...)`) read the updated cursor
   ordinal and produce typed values. Provenance-based
   invalidation ensures only nodes downstream of the changed
   cursor are re-evaluated.

```
loop {
    if !cursors.advance() { break }  // cursor exhausted
    cursors.inject_into_state(&mut state);
    let fields = fiber.resolve_with_field_pulls(template, &field_pulls[idx]);
    let pulls  = fiber.resolve_pulls(&pull_plans[idx]);
    let ctx = ExecCtx::new(&fields, &pulls);
    dispenser.execute(cycle, &ctx).await;
}
```

### Cursors Type

`Cursors` is a provenance-driven advancer that targets only
the cursor nodes relevant to a specific set of output fields:

```rust
pub struct Cursors {
    targets: Vec<CursorTarget>,  // (DataSource reader, GK input index)
    last_items: Vec<Option<SourceItem>>,
    advances: u64,
}
```

Built at phase setup via `Cursors::for_fields()`, which traces
GK provenance from the op template's referenced field names
back to root cursor nodes. Only those cursors advance on each
iteration — unused cursors are left untouched. This enables
phases with multiple cursors where different ops consume
different data sources independently.

### Lazy Evaluation After Cursor Advance

After cursor advance and injection, the GK DAG does not
eagerly re-evaluate all nodes. Values are pulled lazily when
`resolve_with_field_pulls` (or `PullPlan::resolve` for wrapper
reads) requests specific outputs. Only nodes in the provenance
chain of the requested output are evaluated. Combined with
per-node caching, this means accessor functions for unrequested
fields are never called.

### DataSource API

The underlying data readers implement the `DataSource` trait:

```
DataSource (per-cursor, stateful)
  ├── next() → Option<SourceItem>     — pull next item
  ├── next_chunk(n) → Vec<SourceItem> — pull up to n items
  ├── extent() → Option<u64>           — known size
  └── consumed() → u64                 — progress
```

All source API surface (`DataSource`, `SourceItem`,
`SourceSchema`, `DataSourceFactory`, `Cursors`) lives in
`nbrs-variates::source`. The runtime crates consume these types
but do not define them.
