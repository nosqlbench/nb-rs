# 12: GK Standard Library

The GK node library provides deterministic, composable functions
for data generation. Nodes are registered in the DSL compiler's
function registry and available by name in `.gk` source.

### Wire Cost Classes

Some node inputs are **configuration wires** — changing them
invalidates expensive internal state (e.g., recomputing a
lookup table for weighted selection). Other inputs are
**data wires** — cheap per-cycle values that drive the
node's primary computation.

Node metadata should declare the cost class of each input port:

| Class | Semantics | Example |
|-------|-----------|---------|
| `config` | Expensive to change. Initializes internal state (LUT, distribution table). Expected to be wired to init-time constants or rarely-changing values. | `weighted_strings` weights parameter |
| `data` | Cheap per-cycle input. The node's primary computation path. | `hash` input value, `mod` dividend |

The compiler can use this information to:
- **Warn** when a `config` wire is connected to a cycle-time
  binding (the LUT would be rebuilt every cycle)
- **Error** when the cost would be catastrophic (e.g., O(n)
  rebuild per cycle on a million-entry distribution)
- **Allow** explicit override when the user intentionally wants
  per-cycle reconfiguration (functional testing of the node)

This is a metadata annotation on `PortMeta`, not a runtime
enforcement — the node always works correctly regardless of
wiring, but the compiler protects users from accidental
performance traps.

---

## Node Categories

### Hash and Distribution

| Node | Signature | Description |
|------|-----------|-------------|
| `hash` | `u64 → u64` | xxh3 deterministic hash |
| `hash_range` | `u64, u64 → u64` | hash into [0, range) |
| `mod` | `u64, u64 → u64` | modular arithmetic |
| `unit_interval` | `u64 → f64` | hash to [0.0, 1.0) |
| `uniform` | `u64, f64, f64 → f64` | hash to [lo, hi) |

### Arithmetic

| Node | Signature | Description |
|------|-----------|-------------|
| `add` | `u64, u64 → u64` | addition |
| `mul` | `u64, u64 → u64` | multiplication |
| `pow` | `f64, f64 → f64` | exponentiation |
| `clamp` | `f64, f64, f64 → f64` | clamp to [min, max] |
| `lerp` | `f64, f64, f64 → f64` | linear interpolation |
| `min` / `max` | `u64, u64 → u64` | min/max selection |

### String Generation

| Node | Signature | Description |
|------|-----------|-------------|
| `format_u64` | `u64, u64 → String` | zero-padded decimal |
| `hex` | `u64 → String` | hex representation |
| `weighted_strings` | `u64, String → String` | weighted selection from list |
| `one_of` | `u64, String → String` | uniform selection from list |
| `alpha_numeric` | `u64, u64 → String` | random alphanumeric string |
| `uuid_from_u64` | `u64 → String` | deterministic UUID |

### Random Number Generation

| Node | Signature | Description |
|------|-----------|-------------|
| `pcg` | `u64, u64 → u64` | PCG-RXS-M-XS 64/64 (seekable) |
| `pcg_stream` | `u64 → u64` | PCG with wire-time stream ID |

PCG provides reproducible, seekable random streams. O(log N) seek
via repeated squaring. Entity-correlated: one stream per entity ID.

### Weighted Selection

| Node | Signature | Description |
|------|-----------|-------------|
| `fair_coin` | `u64 → u64` | 50/50 binary selection |
| `select` | `u64, String → u64` | weighted index selection |
| `one_of_weighted` | `u64, String → String` | weighted string selection |

Uses alias method for O(1) weighted selection regardless of
category count.

### Time and Identity

| Node | Signature | Description |
|------|-----------|-------------|
| `identity` | `u64 → u64` | pass-through |
| `counter` | `→ u64` | monotonic counter (non-deterministic) |
| `mixed_radix` | `u64, u64, u64 → u64` | input decomposition |

### Vectordata Integration (feature-gated)

| Node | Signature | Description |
|------|-----------|-------------|
| `vector_at` | `u64, String → String` | training vector at index |
| `query_vector_at` | `u64, String → String` | query vector at index |
| `neighbor_indices_at` | `u64, String → String` | ground truth neighbors |
| `neighbor_distances_at` | `u64, String → String` | ground truth distances |
| `vector_dim` | `String → u64` | dataset dimension count |
| `vector_count` | `String → u64` | dataset training set size |
| `dataset_distance_function` | `String → String` | similarity metric name |

Dataset resolution: bare name → `vectordata` catalog → URL → download + cache.
Datasets loaded once globally via `DATASET_CACHE`.

---

## Registration

Nodes are registered in the DSL compiler's function registry
(`nb-variates/src/dsl/registry.rs`). Each entry maps a function
name to a factory that produces a `Box<dyn GkNode>` from parsed
arguments.

```rust
registry.register("hash", |args| {
    Ok(Box::new(Hash64::new()))
});

registry.register("mod", |args| {
    let modulus = args.get_u64(0)?;
    Ok(Box::new(ModU64::new(modulus)))
});
```

Vectordata nodes are registered behind a `vectordata` feature gate.

---

## GK Modules

Reusable `.gk` files that define subgraphs:

```
// latency_model.gk
inputs := (cycle)
base_ns := uniform(hash(cycle), 500000.0, 2000000.0)
jitter := uniform(hash(add(cycle, 1)), 0.9, 1.1)
latency_ns := mul(base_ns, jitter)
```

Module interface inferred: graph inputs = unbound references,
outputs = terminal bindings. Modules inline into the host DAG
with name prefixing to avoid collision.

Resolution chain: workload directory → `--gk-lib` paths →
bundled stdlib → error.

---

## Node Fusion

Assembly-time graph optimization: recognize subgraph patterns
and replace with fused nodes.

| Pattern | Fused To |
|---------|----------|
| `mod(hash(x), K)` | `hash_range(x, K)` |
| `lerp(unit_interval(hash(x)), lo, hi)` | `hash_interval(x, lo, hi)` |

Fusion rules match on node types and check for external consumers
of intermediate nodes before replacing.
