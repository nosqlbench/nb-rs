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
  ├── globals          — resolved workload params (set once)
  ├── volatile_ports   — port definitions (reset per eval)
  └── sticky_ports     — port definitions (persist per stanza)

GkState (per-fiber, mutable, private)
  ├── buffers[][]        — per-node output value slots
  ├── node_clean[]       — per-node cache validity (bool)
  ├── input_provenance[] — copy of program's provenance bitmasks
  ├── inputs[]           — current input values
  ├── volatile_values[]  — external ports (reset per eval)
  └── sticky_values[]    — external ports (persist across evals)
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
   → volatile ports reset (port bit 63 set in changed_mask)

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
  - Volatile/sticky ports → cycle-time
  - NodeOutput from cycle-time node → cycle-time (propagates)
  - Everything else → init-time

Phase 2: Evaluate init-time nodes with dummy inputs

Phase 3: Replace evaluated nodes with ConstU64/ConstF64/ConstStr
```

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
  → stored in CaptureContext

fiber.apply_captures()
  → CaptureContext values written to sticky ports in GkState

Op B resolves → {capture:user_name} reads from sticky port
```

Captures reset at stanza boundaries (`fiber.reset_captures()`).

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
    program: Arc<GkProgram>,   // shared, immutable — includes globals
    state: GkState,            // per-fiber, mutable
    captures: CaptureContext,  // per-stanza capture state
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

No separate params argument — workload params (globals) are
stored on `GkProgram` and accessed via `program.globals()`.
Fibers read from the shared immutable program; no per-fiber
params map.

`resolve_with_extras` iterates the op's field map, substitutes
`{name}` bind points from GK outputs, captures, and globals,
and pulls any extra bindings needed by validation.
