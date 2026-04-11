# nb-variates

Deterministic variate generation kernel (GK) for workload testing.

Transforms named `u64` coordinate tuples into typed output variates
via a compiled DAG of composable function nodes. The same coordinate
always produces the same outputs — deterministic, reproducible, and
parallelizable with zero shared mutable state.

Part of the [nb-rs](https://github.com/nosqlbench/nb-rs) workspace.

## Quick Start

### From GK DSL

```rust
use nb_variates::dsl::compile_gk;

let mut kernel = compile_gk(r#"
    coordinates := (cycle)
    hashed := hash(cycle)
    user_id := mod(hashed, 1000000)
"#).unwrap();

kernel.set_coordinates(&[42]);
let user_id = kernel.pull("user_id").as_u64();
assert!(user_id < 1_000_000);
```

### From the Assembler API

```rust
use nb_variates::assembly::{GkAssembler, WireRef};
use nb_variates::nodes::hash::Hash64;
use nb_variates::nodes::arithmetic::ModU64;

let mut asm = GkAssembler::new(vec!["cycle".into()]);
asm.add_node("hashed", Box::new(Hash64::new()), vec![WireRef::coord("cycle")]);
asm.add_node("user_id", Box::new(ModU64::new(1_000_000)), vec![WireRef::node("hashed")]);
asm.add_output("user_id", WireRef::node("user_id"));

let mut kernel = asm.compile().unwrap();
kernel.set_coordinates(&[42]);
assert!(kernel.pull("user_id").as_u64() < 1_000_000);
```

## Compilation Levels

| Level | Mechanism | Throughput | Feature |
|-------|-----------|------------|---------|
| Phase 1 | Pull-through interpreter | ~70ns/node | always |
| Phase 2 | Compiled u64 closures | ~4.5ns/node | always |
| Hybrid | Per-node optimal | best of P2/P3 | `jit` |
| Phase 3 | Cranelift JIT native code | ~0.2ns/node | `jit` |

## Features

- **`jit`** (default): Cranelift JIT for Phase 3 compilation.
  Disable with `default-features = false` for a lighter build (~50MB smaller).
- **`vectordata`**: Vector dataset access nodes for ML/AI workloads.

## Node Library

250+ built-in function nodes across 20 categories:

- **Hashing**: hash, hash_range, hash_interval
- **Arithmetic**: add, mul, div, mod, clamp, sum, product, min, max
- **Interpolation**: lerp, scale_range, inv_lerp, remap, quantize
- **Math**: sin, cos, tan, sqrt, exp, ln, pow, atan2
- **Distributions**: normal, exponential, uniform, Pareto, Zipf, empirical
- **Sampling**: alias tables, histribution, weighted selection
- **String**: combinations, format, number_to_words, printf
- **DateTime**: epoch scale/offset, timestamp formatting, date components
- **Noise**: Perlin 1D/2D, simplex, fractal Brownian motion
- **JSON**: construction, merge, serialization, field access
- **Encoding**: base64, hex, SHA-256, MD5, HTML entities, URL encoding
- **Real data**: Census names, US states, countries, nationalities
- **Context**: counter, wall clock, thread ID, elapsed time
- **Permutation**: PCG RNG, shuffle, cycle walk

## License

Apache-2.0
