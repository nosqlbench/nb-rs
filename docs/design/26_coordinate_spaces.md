# SRD 26 — Coordinate Spaces and Traversal

## Overview

A GK kernel is a pure function from an input coordinate tuple to
named output values. The coordinate space — how a monotonic cycle
counter maps to a multi-dimensional domain — is defined inside the
GK program, not at the activity layer.

This is a deliberate design choice: the GK DAG is already the right
modeling substrate for coordinate transforms, and there is no strong
reason to promote decomposition to the activity executor.

## The Single-Cycle Interface

The kernel's external interface is a single u64 coordinate:

```
coordinates := (cycle)
```

The activity executor calls `kernel.eval(&[cycle])` with a monotonic
counter. Everything downstream — decomposition into domain dimensions,
hashing, generation — is internal to the GK.

This keeps the activity layer simple: it manages cycle ranges,
concurrency, and rate limiting. It does not need to understand
coordinate spaces, dimension ranges, or traversal orders.

## Coordinate Decomposition via mixed_radix

The primary tool for mapping a flat cycle into a multi-dimensional
space is `mixed_radix`:

```
coordinates := (cycle)
(x, y, z) := mixed_radix(cycle, 100, 100, 0)
```

This decomposes `cycle` into three dimensions:
- `x = cycle % 100` — innermost, increments every cycle
- `y = (cycle / 100) % 100` — middle
- `z = cycle / 10000` — outermost, unbounded (radix 0)

The traversal order is nested-loop, innermost first:

```
cycle 0     → (0,  0, 0)
cycle 1     → (1,  0, 0)
cycle 99    → (99, 0, 0)
cycle 100   → (0,  1, 0)     x wraps, y increments
cycle 9999  → (99, 99, 0)
cycle 10000 → (0,  0, 1)     x,y wrap, z increments
```

The radix values define the size of each dimension. A trailing `0`
means unbounded — the outermost dimension consumes whatever remains.

## Why Decomposition Stays Inside the GK

Several alternatives were considered and rejected:

**Alternative: Activity-level coordinate spaces**

The activity executor could accept coordinate range declarations
(`x: 0..100, y: 0..100`) and handle the enumeration externally,
passing `(x, y, z)` as a tuple to the kernel. This was rejected
because:

1. The GK already has sufficient abstraction for any coordinate
   transform — mixed_radix, interleave, hash, modular arithmetic.
2. Any traversal strategy (nested loop, space-filling curve, random
   sampling, strided) is better expressed as GK nodes than as
   activity-layer configuration.
3. Pulling coordinates to the surface would split domain logic across
   two layers (activity config + GK program) instead of keeping it
   in one place.

**Alternative: Implicit cycle coordinate**

The `coordinates := (cycle)` declaration could be implicit, since
95% of workloads use a single cycle input. This was considered but
deferred: the explicit form is clear, self-documenting, and necessary
for multi-coordinate kernels (`coordinates := (row, col)`). Adding
implicit defaults would be sugar, not a capability gap.

**Alternative: Composable pre-phase graph**

A coordinate pre-processor could be a separate GK subgraph whose
outputs feed into the main graph's coordinate inputs. This would
be modeled as two aligned subgraphs within a single GK program —
the GK's DAG structure already supports this naturally without
any new concepts.

## Traversal Strategies

Since decomposition is inside the GK, different traversal strategies
are just different node compositions:

### Nested loop (default)

```
(x, y, z) := mixed_radix(cycle, Nx, Ny, 0)
```

Visits every (x,y) pair before incrementing z. Good for workloads
where the innermost dimension is the "hot" access pattern.

### Row-major table scan

```
(row, col) := mixed_radix(cycle, num_cols, 0)
```

Scans columns within each row. Natural for tabular inserts.

### Hashed random access

```
h := hash(cycle)
(x, y) := mixed_radix(h, Nx, Ny)
```

Pseudo-random (x,y) point for each cycle. Good for uniform random
reads across a 2D space.

### Interleaved dimensions

```
h := hash(cycle)
x := mod(h, Nx)
y := mod(hash(h), Ny)
```

Independent hash-derived coordinates. Each dimension is statistically
independent rather than following the mixed_radix nesting order.

### Strided access

```
base := mul(cycle, stride)
(x, y) := mixed_radix(base, Nx, Ny)
```

Skip `stride` positions per cycle. Useful for sampling a subgrid.

## Domain Modeling Examples

### IoT: devices × readings

```
coordinates := (cycle)
(device, reading) := mixed_radix(cycle, 10000, 0)
```

10,000 devices. Reading index is unbounded — each device accumulates
readings as cycles advance. Device 0 gets readings 0,1,2,...; then
device 1 gets readings 0,1,2,...; etc.

### Table: rows × columns

```
coordinates := (cycle)
(col, row) := mixed_radix(cycle, num_cols, 0)
```

Column varies fastest. Each row is fully populated before moving to
the next. Natural for INSERT batches.

### Vector embeddings: vectors × dimensions

```
coordinates := (cycle)
(dim, vector_id) := mixed_radix(cycle, 768, 0)
```

768 dimensions per vector. Cycle 0..767 fills vector 0, cycle
768..1535 fills vector 1, etc.

### Time series: entities × timestamps

```
coordinates := (cycle)
(entity, ts_offset) := mixed_radix(cycle, num_entities, 0)
```

Entity varies fastest — all entities get timestamp 0, then all get
timestamp 1, etc. This produces interleaved writes across entities,
modeling concurrent ingestion.

### 3D point cloud

```
coordinates := (cycle)
(x, y, z) := mixed_radix(cycle, 100, 100, 100)
```

Fixed 100×100×100 grid. Total space is exactly 1,000,000 points.
Running with `cycles=1M` visits every point once.

## Properties

**Deterministic**: Same cycle always maps to the same coordinate
tuple. Replaying a cycle range reproduces identical output.

**Composable**: Decomposed coordinates are regular GK wires. They
can feed into hash, interleave, mod, or any other node. The
coordinate space is not a special concept — it's just data flowing
through the DAG.

**Thread-safe**: Each thread evaluates its own cycles independently.
The coordinate transform is a pure function with no shared state.

**Enumeration-agnostic**: The GK doesn't know or care whether cycles
are assigned sequentially, in batches, or randomly. It just maps
whatever cycle value it receives to the corresponding point.
