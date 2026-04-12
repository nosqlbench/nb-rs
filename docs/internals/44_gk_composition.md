# SRD 44: GK Kernel Composition and Init-Time Value Propagation

## Overview

GK kernels should be composable units. A kernel compiled and
optimized at any level can be combined with other kernels, and the
result should be automatically recompiled at the highest supported
optimization level. This enables modular workload construction,
init-time computed constants, and pipeline staging.

## Composition Modes

### 1. Pipeline Staging

Two or more GK kernels are connected in sequence. Named outputs from
an earlier stage that match named inputs on a later stage are
auto-wired.

```
Stage 1 (init-time):                Stage 2 (cycle-time):
  dataset_size := count(...)          user_id := mod(hash(cycle), dataset_size)
  shard_count := div(...)             shard := mod(user_id, shard_count)
  ──► outputs: dataset_size,          ──► outputs: user_id, shard
      shard_count
```

Stage 1 runs once at init time. Its outputs (`dataset_size`,
`shard_count`) become constant inputs to Stage 2. Stage 2 runs per
cycle with those constants baked in.

This enables **init-time computed constants**: a function node in
Stage 1 computes a value (e.g., the cardinality of a loaded dataset),
and that value flows downstream as a compile-time constant in Stage 2.
The JIT can fold it — no per-cycle lookup.

### 2. Module Embedding

A kernel is embedded within another kernel as a subgraph. This is
the existing GK module system (SRD 27): `.gk` files are inlined
into the host kernel at compile time. The combined graph is
recompiled and optimized as a single unit.

```gk
// timeseries.gk module
coordinates := (cycle)
timestamp := add(epoch_offset(cycle, 1704067200000), mod(hash(cycle), 86400000))

// host workload
use timeseries
reading := unit_interval(hash(cycle))
```

The module's nodes merge into the host kernel's DAG. After merging,
the full graph is re-optimized: fusion, constant folding, JIT.

## Init-Time Value Propagation

### The Problem

A workload needs a computed constant that depends on init-time state.
For example:

```yaml
params:
  dataset: "glove-100"
bindings: |
  coordinates := (cycle)
  vectors := load_vectors("{dataset}")
  dataset_size := vector_count(vectors)   # init-time: how many vectors?
  idx := mod(hash(cycle), dataset_size)   # cycle-time: index into dataset
  vector := vector_at(idx, vectors)       # cycle-time: retrieve vector
```

Here `dataset_size` is computed at init time (it depends on the loaded
dataset) but used at cycle time as a constant modulus. The value must
propagate from the init phase to the cycle phase without sacrificing
optimization.

### The Solution: Two-Phase Compilation

1. **Phase A (init):** Compile and evaluate the init-time subgraph.
   Nodes that depend only on constants and init objects (no
   coordinates) are evaluated once. Their outputs become concrete
   values.

2. **Phase B (cycle):** Recompile the cycle-time subgraph with the
   Phase A outputs injected as constants. The JIT sees them as
   literals and can fold them.

```
Phase A: compile init subgraph → evaluate → { dataset_size: 1000000 }
Phase B: compile cycle subgraph with dataset_size=1000000 baked in
         → mod(hash(cycle), 1000000) fuses to a single JIT instruction
```

This is transparent to the user. The compiler detects which nodes are
init-time evaluable (no coordinate dependencies) and automatically
splits the compilation. No explicit `init` keyword needed for values
that can be inferred as init-time.

### Inference Rules

A node is init-time evaluable if:
- All its inputs are constants, init objects, or other init-time nodes
- It has no wire dependency on any coordinate input
- It does not depend on capture context or volatile ports

The compiler builds a dependency graph and partitions nodes into
init-time and cycle-time sets. Init-time nodes evaluate once; their
values replace the nodes in the cycle-time graph as constants.

## Composition API

### Programmatic

```rust
// Compile two kernels independently
let init_kernel = compile_gk("dataset_size := count(load_vectors(...))")?;
let cycle_kernel = compile_gk("idx := mod(hash(cycle), {dataset_size})")?;

// Compose: init outputs feed cycle inputs
let composed = GkProgram::compose_pipeline(vec![
    init_kernel.into_program(),
    cycle_kernel.into_program(),
]);
// → dataset_size is evaluated from init_kernel,
//   then baked into cycle_kernel as a constant
```

### DSL

In the GK DSL, composition is implicit. The compiler handles the
init/cycle split automatically based on dependency analysis:

```gk
coordinates := (cycle)

// These are init-time (no cycle dependency):
vectors := load_vectors("glove-100")
n := vector_count(vectors)

// These are cycle-time:
idx := mod(hash(cycle), n)    // n is auto-constant from init phase
vec := vector_at(idx, vectors)
```

## Recompilation and Optimization

When kernels are composed:

1. The combined graph is **recompiled** — not just wired together.
   Node fusion, constant folding, and type adapter insertion run on
   the merged graph.

2. The result is compiled at the **highest supported level**. If both
   source kernels support P3 (JIT), the composed result is P3. If
   one is P2-only, the composed result uses hybrid mode (P3 where
   possible, P2 for the rest).

3. Init-time evaluated values are injected as `ConstU64`, `ConstF64`,
   or `ConstStr` nodes — the same representation as literal constants
   in the source. The optimizer treats them identically.

## Relationship to Other SRDs

- **SRD 12 (Init vs Cycle State):** Defines the init/cycle split.
  This SRD extends it with automatic inference and value propagation.
- **SRD 24 (Compilation Levels):** Composed kernels inherit the
  highest supported compilation level.
- **SRD 27 (GK Modules):** Module embedding is one form of
  composition. This SRD generalizes it to pipeline staging.
- **SRD 36 (Node Fusion):** Fusion runs on the composed graph,
  potentially finding new optimization opportunities across module
  boundaries.
- **SRD 42 (Workload Parameters):** Parameters are a special case of
  init-time constants. This SRD subsumes them — a parameter is just
  a constant node with an external value source.
