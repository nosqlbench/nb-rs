# SRD 30 — GK Standard Library

## Overview

The GK standard library (stdlib) is a collection of `.gk` module
files that ship with `nbrs`. It provides reusable higher-level
patterns composed from native nodes — latency models, identity
generators, time series helpers, and common workload building
blocks.

The stdlib complements native nodes. Native nodes are primitives
(hash, mod, select, fair_coin) implemented in Rust with P3 JIT
support. Stdlib modules compose those primitives into patterns
that would otherwise be copy-pasted across workloads.

## Two Layers

### Native nodes (Rust, JIT-able)

Primitive operations added to the GK function library. These are
fast, single-purpose, and compiled to native code:

**Probability primitives** (new, from SRD 29):

| Node | Signature | Description |
|------|-----------|-------------|
| `fair_coin(input)` | u64 → u64 | 50/50: returns 0 or 1 |
| `unfair_coin(input, p)` | u64, f64 → u64 | Returns 1 with probability p |
| `select(cond, a, b)` | u64, T, T → T | Conditional: a if cond≠0, else b |
| `one_of(input, values...)` | u64, T... → T | Uniform pick from N values |
| `one_of_weighted(input, spec)` | u64, init → T | Weighted pick via alias method |
| `n_of(input, n, m)` | u64, init, init → u64 | Exactly n of every m return 1 |
| `chance(input, p)` | u64, f64 → f64 | Like unfair_coin, returns f64 |
| `blend(input, a, b, mix)` | u64, f64, f64, f64 → f64 | Weighted mix of two values |

These are native because they're primitive, frequently used, and
benefit from JIT compilation. They belong in `nb-variates/src/nodes/`.

### Stdlib modules (.gk, compositions)

Higher-level patterns composed from native nodes. These are `.gk`
files embedded in the `nbrs` binary and loaded on demand through
the module resolution chain.

## Resolution Chain

When the GK compiler encounters an unknown function name:

```
1. Workload directory    — <name>.gk or any .gk in the same dir
2. --gk-lib paths        — explicit user library directories
3. Standard library      — bundled in the nbrs binary
4. Error                 — unknown function '<name>'
```

Each level can override the next. A workload-local `gaussian_latency.gk`
shadows the stdlib version. A `--gk-lib` module shadows the stdlib
but not the workload-local version.

### Standard library: bundled in the binary

The stdlib `.gk` source files live in the nbrs source tree at
`nb-rs/stdlib/` and are embedded in the binary at compile time
using Rust's `include_str!()`:

```
nb-rs/
  stdlib/
    latency.gk
    identity.gk
    timeseries.gk
    modeling.gk
```

```rust
const STDLIB_MODULES: &[(&str, &str)] = &[
    ("latency.gk", include_str!("stdlib/latency.gk")),
    ("identity.gk", include_str!("stdlib/identity.gk")),
    ("timeseries.gk", include_str!("stdlib/timeseries.gk")),
    ("modeling.gk", include_str!("stdlib/modeling.gk")),
];
```

Stdlib modules are:
- **Always available** — no filesystem access, no installation step
- **Implicitly referenceable** — just call `gaussian_latency(...)` and
  it resolves from the bundled stdlib without any import or path config
- **Discoverable** — `nbrs describe gk stdlib` lists all bundled modules
- **Overridable** — a local `.gk` file with the same name takes priority

The stdlib is part of the nbrs distribution. Updating nbrs updates
the stdlib. The embedded sources are parsed on first reference and
cached for the lifetime of the process.

### User libraries: `--gk-lib`

For organization-wide shared modules that don't ship with nbrs,
the `--gk-lib` flag adds directories to the resolution chain:

```
nbrs run workload=w.yaml --gk-lib=/opt/myorg/gk-modules --gk-lib=./shared
```

Multiple `--gk-lib` paths are searched in the order specified,
after the workload directory and before the stdlib. There is no
implicit default user library path — if you want a shared library,
you specify it.

### Override semantics

A module found at a higher-priority level completely replaces the
lower-priority version. No merging, no inheritance. If you place
`gaussian_latency.gk` next to your workload, the stdlib version
is invisible for that workload.

## Stdlib Contents

### `stdlib/latency.gk` — Latency models

```gk
// gaussian_latency(input, mean, stddev)
// → latency_ms: f64
//
// Deterministic normally-distributed latency in milliseconds.
latency_ms := icd_normal(unit_interval(hash(input)), mean, stddev)
```

```gk
// bimodal_latency(input, fast_mean, fast_std, slow_mean, slow_std, slow_fraction)
// → latency_ms: f64
//
// Two-mode latency: fast path (1-slow_fraction) and slow path (slow_fraction).
is_slow := unfair_coin(input, slow_fraction)
fast := icd_normal(unit_interval(hash(input)), fast_mean, fast_std)
slow := icd_normal(unit_interval(hash(hash(input))), slow_mean, slow_std)
latency_ms := select(is_slow, slow, fast)
```

```gk
// jittered_latency(input, base_ms, jitter_fraction)
// → latency_ms: f64
//
// Base latency ± jitter_fraction * base_ms.
noise := scale_range(hash(input), -1.0, 1.0)
jitter := mul(mul(base_ms, jitter_fraction), noise)
latency_ms := add(base_ms, jitter)
```

### `stdlib/identity.gk` — Identity generators

```gk
// hashed_id(input, bound)
// → id: u64
//
// Deterministic bounded ID from hash.
h := hash(input)
id := mod(h, bound)
```

```gk
// euler_circuit(position, range, seed, stream)
// → value: u64
//
// Bijective permutation of [0, range) via PCG cycle-walking.
// Every position maps to a unique value.
value := cycle_walk(position, range, seed, stream)
```

```gk
// unique_pair(input, bound_a, bound_b)
// → a: u64, b: u64
//
// Two independent bounded IDs from one input.
ha := hash(input)
hb := hash(ha)
a := mod(ha, bound_a)
b := mod(hb, bound_b)
```

### `stdlib/timeseries.gk` — Time series helpers

```gk
// monotonic_ts(input, base_epoch, interval_ms)
// → ts: u64
//
// Monotonically increasing timestamp.
offset := mul(input, interval_ms)
ts := add(base_epoch, offset)
```

```gk
// jittered_ts(input, base_epoch, interval_ms, jitter_ms)
// → ts: u64
//
// Monotonic with per-point jitter.
base := add(base_epoch, mul(input, interval_ms))
noise := mod(hash(input), jitter_ms)
ts := add(base, noise)
```

### `stdlib/modeling.gk` — Service modeling patterns

```gk
// service_model(input, fast_ms, slow_ms, slow_pct, error_pct)
// → latency_ms: f64, is_error: u64
//
// Complete service model: bimodal latency + error injection.
is_slow := unfair_coin(input, slow_pct)
fast := icd_normal(unit_interval(hash(input)), fast_ms, 0.5)
slow := icd_normal(unit_interval(hash(hash(input))), slow_ms, 5.0)
latency_ms := select(is_slow, slow, fast)
fast_err := unfair_coin(hash(input), error_pct)
slow_err := unfair_coin(hash(input), mul(error_pct, 3.0))
is_error := select(is_slow, slow_err, fast_err)
```

```gk
// retry_model(input, base_latency, retry_pct, max_retries)
// → latency_ms: f64, attempts: u64
//
// Models retry behavior: each retry adds latency.
needs_retry := unfair_coin(input, retry_pct)
attempts := select(needs_retry, 2, 1)
latency_ms := mul(base_latency, attempts)
```

## Discoverability

### `describe gk stdlib`

Lists all available stdlib modules with their inferred interfaces:

```
$ nbrs describe gk stdlib

Standard Library Modules
════════════════════════

  ── Latency ──
  gaussian_latency     (input, mean, stddev) → latency_ms
  bimodal_latency      (input, fast_mean, fast_std, slow_mean, slow_std, slow_fraction) → latency_ms
  jittered_latency     (input, base_ms, jitter_fraction) → latency_ms

  ── Identity ──
  hashed_id            (input, bound) → id
  euler_circuit        (position, range, seed, stream) → value
  unique_pair          (input, bound_a, bound_b) → a, b

  ── Time Series ──
  monotonic_ts         (input, base_epoch, interval_ms) → ts
  jittered_ts          (input, base_epoch, interval_ms, jitter_ms) → ts

  ── Modeling ──
  service_model        (input, fast_ms, slow_ms, slow_pct, error_pct) → latency_ms, is_error
  retry_model          (input, base_latency, retry_pct, max_retries) → latency_ms, attempts
```

### `describe gk modules`

Lists modules available in the current workload directory plus
user library, in addition to stdlib:

```
$ nbrs describe gk modules --dir examples/

Local Modules (examples/)
  euler_circuit.gk     (position, range, seed, stream) → value
  hashed_id.gk         (input, bound) → id

Standard Library: 10 modules (use 'describe gk stdlib' for details)
```

## Stdlib vs Native: Decision Criteria

A function should be a **native node** when:
- It's a primitive operation (one concept, one step)
- It benefits from P3 JIT compilation
- It's used in inner loops of hot GK kernels
- It can't be composed from existing nodes

A function should be a **stdlib module** when:
- It composes multiple primitives into a pattern
- It encodes a domain convention (latency model, identity scheme)
- It benefits from being readable/editable as GK source
- Its performance is dominated by the primitives it calls, not
  by composition overhead

When in doubt, start as a stdlib module. Promote to native when
profiling shows the composition overhead matters.
