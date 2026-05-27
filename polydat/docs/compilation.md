# Compilation Levels

Polydat compiles a function graph at one of three levels. The compiled
artifact is the same shape at each level — a kernel callers can poke
coordinates into and pull outputs out of — so picking a level is a
performance / build-size trade rather than a behaviour switch.

| Level   | Mechanism                 | Throughput    | Feature |
|---------|---------------------------|---------------|---------|
| Phase 1 | Pull-through interpreter  | ~70ns/node    | always  |
| Phase 2 | Compiled u64 closures     | ~4.5ns/node   | always  |
| Hybrid  | Per-node optimal          | best of P2/P3 | `jit`   |
| Phase 3 | Cranelift JIT native code | ~0.2ns/node   | `jit`   |

Phase 1 and 2 are always available — no extra dependency, no codegen
backend. Phase 3 needs the Cranelift JIT and ships behind the `jit`
feature flag.

The Hybrid level isn't a fourth implementation; it's the runtime
picking, per node, between the Phase 2 closure path and the Phase 3
JIT path depending on which produced better code for that node. The
Hybrid result is always ≤ the throughput of pure Phase 3 and ≥ pure
Phase 2.

## Cargo Features

- **`jit`** (default) — Cranelift JIT for Phase 3 / Hybrid compilation.
  Disable with `default-features = false` for a lighter build (~50MB
  smaller).
- **`vectordata`** — vector dataset access nodes for ML/AI workloads.
