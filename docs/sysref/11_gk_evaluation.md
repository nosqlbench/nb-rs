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

## Init-Time vs Cycle-Time

The two lifecycles exist because some work is too expensive or
nonsensical to repeat every cycle. Init-time bindings form their
own DAG resolved once at assembly — before the first cycle fires.
This lets expensive setup (LUT construction, CSV loading, alias
table building, connection establishment) happen in a dedicated
phase where cost doesn't matter. The cycle-time DAG then starts
with all init values frozen, no lazy initialization, no first-cycle
penalties. Init-time values are shared via `Arc` — one alias table
used by many nodes is built once and referenced by all of them.

### Cycle-Time Nodes

Depend on inputs (directly or transitively). Evaluated every
cycle. Examples: `hash(cycle)`, `mod(hash(cycle), 1000000)`.

### Init-Time Nodes

No input dependency. Evaluated once during constant folding
and replaced with literal constants in the DAG. Examples:
`vector_dim("glove-25")`, `vector_count("glove-25")`.

### Constant Folding

After compilation, `fold_init_constants()` identifies init-time
nodes and evaluates them:

```
Phase 1: Mark nodes as init-time or cycle-time
  - Graph inputs → cycle-time
  - External ports → cycle-time
  - NodeOutput from cycle-time node → cycle-time (propagates)
  - Everything else → init-time

Phase 2: Evaluate init-time nodes with dummy inputs

Phase 3: Replace evaluated nodes with ConstU64/ConstF64/ConstStr
```

Type adapter nodes (`__u64_to_f64`, etc.) participate in folding.
A chain like `ConstU64(42) → __u64_to_f64 → sin` folds to
`ConstF64(sin(42.0))` — the entire chain is evaluated once and
replaced with a single constant.

Non-deterministic nodes (`counter`, `current_epoch_millis`,
`elapsed_millis`, `thread_id`) are excluded from folding regardless
of their input status.

Folded constants are available via `kernel.get_constant(name)` for
use by activity config resolution (cycles, concurrency from
dataset metadata).

---

## Input Spaces

Most workloads use a single `cycle` input. Multi-dimensional
inputs enable nested iteration:

```
inputs := (cycle)

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

The current implementation uses Phase 1 (runtime interpreter with
dynamic dispatch):

- `Box<dyn GkNode>` trait objects
- `Value` enum for all intermediate values
- ~70ns per node evaluation

Higher compilation levels for eligible subgraphs:

- **Phase 2 (closures)**: Flat `u64` buffers, closure steps,
  ~4.5ns/node. Requires all-u64 subgraph.
- **Phase 3 (JIT)**: Inline machine instructions, ~0.2ns/node.
  Eliminates closure indirection.
- **Hybrid**: Per-node optimal — JIT for u64 paths, interpreter
  for string/complex nodes.

Nodes declare P3 eligibility via `jit_eligible()` in their
metadata. The compiler selects the highest available level per
subgraph.

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
    pub fn resolve_with_extras(
        &mut self, template: &ParsedOp, extra_bindings: &[String]
    ) -> ResolvedFields;
    pub fn capture(&mut self, name: &str, value: Value);
    pub fn reset_captures(&mut self, cycle: u64);
    pub fn apply_captures(&mut self);
}
```

No separate params argument — workload params are injected into
the GK source as constant bindings before compilation and resolve
as normal GK outputs. No globals mechanism needed.

`resolve_with_extras` iterates the op's field map, substitutes
`{name}` bind points from GK outputs and captures,
and pulls any extra bindings needed by validation.

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
    let fields = fiber.resolve_with_extras(template, extras);
    dispenser.execute(cycle, &fields).await;
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
`resolve_with_extras` requests specific outputs. Only nodes
in the provenance chain of the requested output are evaluated.
Combined with per-node caching, this means accessor functions
for unrequested fields are never called.

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
`nb-variates::source`. The runtime crates consume these types
but do not define them.
