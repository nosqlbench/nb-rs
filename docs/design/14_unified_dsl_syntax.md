# Unified DSL Syntax — Final Draft

This document consolidates all prior syntax decisions into one
coherent reference. It supersedes the syntax fragments scattered
across earlier design documents.

---

## Two Lifecycles, One Language

The GK DSL has two kinds of bindings, distinguished by a single
keyword:

```
init name = expr    // resolved once at assembly, frozen for session
name := expr        // evaluated per cycle, flows through the DAG
```

That's the entire lifecycle model. `init` is the boundary marker.
Everything else is inferred from the graph.

---

## Cycle-Time Bindings (`:=`)

The core DAG wiring syntax, unchanged from earlier design:

```
// Declare input coordinates
coordinates := (cycle)

// Single output
hashed := hash(cycle)

// Multi-output (destructuring)
(tenant, device, reading) := mixed_radix(cycle, 100, 1000, 0)

// String interpolation
device_id := "{tenant_code}-{device_seq}"

// Inline nesting
group_id := hash(interleave(tenant, device))
```

### Rules

- Bare identifiers are wire references: `cycle`, `hashed`, `tenant`
- Numeric literals are constants: `100`, `72.0`
- Quoted strings are string constants or interpolation templates
- `{name}` inside quoted strings references a wire
- Declaration order is flexible (warning if forward-referenced)
- Wire names must start with a letter

---

## Init-Time Bindings (`init`)

Init bindings build frozen artifacts consumed by cycle-time nodes.

```
// Scalar params
init mean = 72.0
init stddev = 5.0

// Distribution table (positional args — simple, few params)
init temp_lut = dist_normal(72.0, 5.0)

// Distribution table (named args — clearer for many params)
init temp_lut = dist_normal(mean: 72.0, stddev: 5.0)

// Load from file
init weights = load_csv(path: "regions.csv", column: "weight")

// Build from another init value
init region_table = alias_table(weights)

// Inline array
init custom_weights = [60.0, 20.0, 15.0, 5.0]
init custom_table = alias_table(custom_weights)
```

### Rules

- `init` bindings are resolved eagerly at assembly time, in
  dependency order.
- `init` bindings may depend on other `init` bindings.
- `init` bindings may NOT depend on coordinates or cycle-time wires.
  This is enforced: if a cycle-time value feeds an `init` binding,
  assembly rejects the DAG with an error.
- Multiple cycle-time nodes may reference the same `init` binding.
  The artifact is shared by reference, not copied.
- `init` bindings are immutable for the duration of the session.

---

## Function Arguments

Functions accept three kinds of arguments, distinguished lexically:

```
init lut = dist_normal(mean: 72.0, stddev: 5.0, resolution: 2000)
                       ^^^^  ^^^^  ^^^^^^  ^^^  ^^^^^^^^^^  ^^^^
                       name  value name    val  name        value
```

| Form | What It Is | Example |
|------|-----------|---------|
| Bare identifier | Wire reference (cycle) or init ref | `cycle`, `lut` |
| Numeric literal | Constant | `72.0`, `1000` |
| Quoted string | String constant | `"regions.csv"` |
| `[...]` | Array literal | `[60.0, 20.0, 15.0]` |
| `name: value` | Named argument | `mean: 72.0` |

Positional and named arguments may be mixed, but positional must
come first (same convention as Python/Swift):

```
// OK: positional then named
init lut = dist_normal(72.0, 5.0, resolution: 2000)

// OK: all named
init lut = dist_normal(mean: 72.0, stddev: 5.0)

// OK: all positional
init lut = dist_normal(72.0, 5.0)

// ERROR: named before positional
init lut = dist_normal(mean: 72.0, 5.0)
```

---

## Init Objects as Arguments to Cycle-Time Nodes

Init bindings flow into cycle-time nodes as frozen configuration.
They appear as ordinary arguments — the assembly phase knows they're
init-time because their provenance traces back to `init` bindings
with no coordinate dependency.

```
init lut = dist_normal(72.0, 5.0)

coordinates := (cycle)
seed := hash(cycle)
quantile := unit_interval(seed)
temperature := lut_sample(quantile, lut)
                          ^^^^^^^^  ^^^
                          cycle-time  init-time (frozen)
```

The node `lut_sample` declares that its second port is init-only.
The assembly phase verifies that `lut` is indeed init-time. If
someone accidentally wires a cycle-time value there, it's an error.

---

## Kernel Definitions

### Bare file (interface inferred)

```
// keygen.gk — name comes from filename
init lut = dist_normal(0.0, 1.0)

hashed := hash(id)
code := mod(hashed, 10000)
quantile := unit_interval(hashed)
score := lut_sample(quantile, lut)
```

Inputs: `id` (unreferenced coordinate). Outputs: `code`, `score`
(assigned but unconsumed).

### Formal wrapper (interface declared)

```
// In a multi-kernel file, or for explicit documentation
keygen(id: u64) -> (code: u64, score: f64) := {
    init lut = dist_normal(0.0, 1.0)

    hashed := hash(id)
    code := mod(hashed, 10000)
    quantile := unit_interval(hashed)
    score := lut_sample(quantile, lut)
}
```

### Anonymous wrapper (in a .gk file)

```
// keygen.gk — name comes from filename, types declared
(id: u64) -> (code: u64, score: f64) := {
    init lut = dist_normal(0.0, 1.0)
    ...
}
```

---

## Comments

```
// Line comments only. No block comments.
```

---

## Complete Example

A sensor telemetry workload:

```
// === Init-time: build tables and load data ===

init temp_lut = dist_normal(mean: 22.0, stddev: 3.0)
init humid_lut = dist_normal(mean: 55.0, stddev: 10.0)
init battery_lut = dist_exponential(rate: 0.02)
init site_weights = [60.0, 20.0, 15.0, 5.0]
init site_table = alias_table(site_weights)

// === Cycle-time: the DAG ===

coordinates := (cycle)

// Decompose into domain dimensions
(site, sensor, reading) := mixed_radix(cycle, 100, 500, 0)

// Site identity
site_h := hash(site)
site_category := alias_sample(site_h, site_table)
site_code := mod(site_h, 10000)

// Sensor identity
sensor_h := hash(interleave(site, sensor))
sensor_code := mod(sensor_h, 100000)

// Independent quantiles via chained hashes
combined := interleave(sensor_h, reading)
h0 := hash(combined)
h1 := hash(h0)
h2 := hash(h1)
q_temp := unit_interval(h0)
q_humid := unit_interval(h1)
q_batt := unit_interval(h2)

// Distribution sampling
temperature := lut_sample(q_temp, temp_lut)
humidity := clamp_f64(lut_sample(q_humid, humid_lut), 0.0, 100.0)
battery := clamp_f64(lut_sample(q_batt, battery_lut), 0.0, 100.0)

// Timestamp
timestamp := add(reading, 1710000000000)

// String outputs
sensor_label := "{site_code}-{sensor_code}"
```

---

## Syntax Summary

| Element | Syntax | Example |
|---------|--------|---------|
| Init binding | `init name = expr` | `init lut = dist_normal(72.0, 5.0)` |
| Cycle binding | `name := expr` | `seed := hash(cycle)` |
| Coordinates | `coordinates := (...)` | `coordinates := (cycle, thread)` |
| Destructuring | `(a, b) := f(x)` | `(tenant, device) := mixed_radix(cycle, 100, 0)` |
| Named args | `name: value` | `dist_normal(mean: 72.0, stddev: 5.0)` |
| Array literal | `[...]` | `[60.0, 20.0, 15.0, 5.0]` |
| String interp | `"{name}"` | `"{site_code}-{sensor_code}"` |
| Comment | `//` | `// this is a comment` |
| Kernel wrapper | `name(...) -> (...) := { ... }` | See above |
| Anon wrapper | `(...) -> (...) := { ... }` | See above |

### Operator distinction

- `=` is used for init bindings (`init name = expr`). It reads as
  "name equals this value, permanently."
- `:=` is used for cycle bindings (`name := expr`). It reads as
  "name is defined as this computation, per cycle."

This is a deliberate visual distinction. `=` is simpler and final.
`:=` suggests ongoing assignment — a definition that's evaluated
repeatedly.
