# Init Objects — Non-Scalar Configuration Artifacts

> **Note:** The DSL syntax for init objects (`init name = expr`) is
> finalized in `14_unified_dsl_syntax.md`. This document describes the
> internal mechanics, types, and lifecycle of init objects.

Init objects are structured data artifacts built at assembly time and
consumed by cycle-time nodes as frozen configuration. They are the
"heavy" counterpart to scalar parameters like `1000` or `72.0`.

---

## What They Are

An init object is any assembly-time value that is:
- **Non-scalar** — not a single number or string, but a data structure
  with internal complexity (arrays, tables, indices).
- **Expensive to build** — construction involves computation, I/O, or
  allocation that cannot happen per-cycle.
- **Immutable after construction** — frozen for the session once built.
- **Consumed by reference** — cycle-time nodes hold a shared reference
  (`Arc`), not a copy.

### Examples

| Init Object | What It Holds | Built From | Consumed By |
|-------------|---------------|-----------|-------------|
| `LutF64` | Interpolation table (Vec<f64>) | `dist_normal(72.0, 5.0)` | `lut_sample` node |
| `AliasTableU64` | Alias method slots (3 parallel Vecs) | `alias_table([60, 20, 15, 5])` | `alias_sample` node |
| `AliasTable<String>` | Alias method slots with string outcomes | `load_csv("regions.csv")` | `alias_sample_str` node |
| `WeightedDataset` | (value, weight) pairs from a file | `load_csv("data.csv", "name", "weight")` | `alias_table`, `weighted_lookup` |
| `StringPool` | Indexed string table | Extracted from CSV or inline | String lookup by index |

---

## How They Differ From Scalar Params

| Aspect | Scalar Param | Init Object |
|--------|-------------|-------------|
| Example | `1000`, `72.0`, `"hello"` | LUT, alias table, dataset |
| DSL syntax | Literal in function args | Result of an init-time function call |
| Storage | Captured by value in closure | Shared by `Arc` reference |
| Size | 8 bytes (u64/f64) or small string | Kilobytes to megabytes |
| Build cost | Zero | Milliseconds to seconds |
| Dependencies | None | May depend on other init objects |
| Shareable | N/A (trivially copyable) | Yes — multiple nodes reference one instance |

In the DSL, both look like function arguments. The distinction is in
what the function produces and how the assembly phase handles it:

```
// Scalar param: 1000 is a literal, captured by value.
bounded := mod(input, 1000)

// Init object: dist_normal builds a LutF64, captured by Arc.
lut := dist_normal(72.0, 5.0)
sample := lut_sample(quantile, lut)
```

---

## Lifecycle

```
Parse DSL
    │
    ▼
Identify init subgraph (provenance tracing)
    │
    ▼
Resolve init objects (topological order, eager)
    │  ← This is where LUTs are built, CSVs are loaded,
    │    alias tables are constructed.
    ▼
Wrap in Arc, store in init object registry
    │
    ▼
Construct cycle-time nodes, injecting Arc<InitObject> refs
    │
    ▼
Wire cycle DAG, compile
    │
    ▼
Run cycles (init objects are frozen, accessed by reference)
    │
    ▼
Session ends → Arc refs drop → init objects deallocated
```

---

## Init Object Types

The system needs a way to represent init objects in the assembly phase
before they're consumed by specific node types. This is the init-time
analog of the `Value` enum for cycle-time data.

```rust
/// An assembly-time artifact, built once and frozen.
pub enum InitObject {
    /// A precomputed f64 interpolation table.
    LutF64(Arc<LutF64>),
    /// An alias sampling table with u64 outcomes.
    AliasU64(Arc<AliasTableU64>),
    /// An alias sampling table with string outcomes.
    AliasStr(Arc<AliasTable<String>>),
    /// A loaded dataset (rows of named fields).
    Dataset(Arc<Dataset>),
    /// A pool of strings indexed by position.
    StringPool(Arc<Vec<String>>),
    /// A scalar that was promoted to init object for sharing.
    Scalar(f64),
}
```

This enum is extensible — new init object types can be added as new
node categories are introduced (e.g., HDF5 datasets, vector indices).

---

## Init Object Producers

Init objects are produced by **init-time functions** — functions that
the assembly phase calls during the init resolution step. These are
distinct from cycle-time node functions.

| Producer Function | Inputs | Output |
|-------------------|--------|--------|
| `dist_normal(mean, stddev)` | Two scalars | `LutF64` |
| `dist_zipf(n, exponent)` | Two scalars | `LutF64` |
| `alias_table(weights)` | Vec<f64> or Dataset | `AliasTableU64` |
| `load_csv(path, col, ...)` | File path + column names | `Dataset` |
| `string_pool(dataset, col)` | Dataset + column name | `StringPool` |

Init-time functions can consume other init objects as inputs, forming
a dependency chain:

```
dataset := load_csv("sensors.csv", "name", "weight")
table := alias_table(dataset)
names := string_pool(dataset, "name")
```

Here `table` and `names` both depend on `dataset`. The assembly phase
resolves `dataset` first, then `table` and `names` can be resolved
in any order (or in parallel).

---

## Init Object Consumers

Cycle-time nodes that consume init objects declare an init-typed input
port. The assembly phase injects the resolved `Arc` reference at node
construction time.

```rust
pub struct LutSample {
    meta: NodeMeta,
    table: Arc<LutF64>,  // init object, injected at construction
}

impl LutSample {
    pub fn new(table: Arc<LutF64>) -> Self { ... }
}
```

The node's cycle-time `eval` method reads from the `Arc` reference
without any locking or synchronization — the data is immutable.

### Port lifecycle annotation

Node ports declare their lifecycle:

```rust
Port {
    name: "quantile",
    typ: PortType::F64,
    lifecycle: Lifecycle::Cycle,  // changes per eval
}
Port {
    name: "table",
    typ: PortType::InitLutF64,
    lifecycle: Lifecycle::Init,   // frozen, injected once
}
```

The assembly phase uses this to:
- Validate that init ports receive init-time values.
- Error if a cycle-time wire feeds an init port.
- Warn if a cycle-time wire feeds an expensive node without an init
  port (suggesting the user should restructure).

---

## Sharing

Init objects are `Arc`-wrapped by default. When multiple nodes
reference the same init binding, they receive the same `Arc` — no
duplication.

```
lut := dist_normal(0.0, 1.0)

// Both get Arc::clone(&lut) — same underlying table.
a := lut_sample(q1, lut)
b := lut_sample(q2, lut)
```

This is automatic and transparent. The user doesn't request sharing;
it falls out of the reference semantics.

---

## Limitations

### 1. No cycle-time mutation

Init objects are immutable. A node cannot write back to a LUT or
update an alias table during cycle evaluation. This is by design —
mutability would break the AOT compilation model and introduce
synchronization concerns.

### 2. No lazy init

All init objects are resolved eagerly at assembly time, before the
first cycle runs. There is no lazy-init path where an init object
is built on first access. This keeps the runtime path simple and
predictable.

### 3. No cross-session persistence

Init objects live for one session (one kernel assembly → teardown
cycle). They are not cached on disk or shared between sessions.
Rebuilding is expected to be fast enough for interactive use; if
not, the user should pre-compute and load from a file.

### 4. No generic init objects

The `InitObject` enum is closed — adding a new init object type
requires adding a variant. This is intentional for Phase 1 (the
set of init object types is small and known). Phase 2 might
introduce a trait-based approach for extensibility.

### 5. Init dependency cycles are errors

The init subgraph must be acyclic. If `A` depends on `B` and `B`
depends on `A`, the assembly phase rejects the DAG. This is the
same rule as the cycle DAG.

### 6. Init objects are opaque to the cycle DAG

A cycle-time node cannot inspect the internals of an init object
(e.g., read the 500th entry of a LUT directly). Init objects are
consumed only through the node that owns them. This keeps the
abstraction boundary clean.

---

## DSL Examples

### Simple: distribution sampling

```
// Init object built from scalar params.
lut := dist_normal(72.0, 5.0)

// Cycle-time evaluation using the init object.
coordinates := (cycle)
seed := hash(cycle)
q := unit_interval(seed)
temperature := lut_sample(q, lut)
```

### Chained: CSV → alias table

```
// Init chain: load file, build table.
dataset := load_csv("regions.csv", "name", "weight")
table := alias_table(dataset, "weight")
names := string_pool(dataset, "name")

// Cycle-time: sample an index, look up the name.
coordinates := (cycle)
idx := alias_sample(hash(cycle), table)
region_name := string_lookup(idx, names)
```

### Shared: one LUT, two consumers

```
lut := dist_exponential(0.5)

coordinates := (cycle)
(a, b) := mixed_radix(cycle, 1000, 0)
seed_a := hash(a)
seed_b := hash(b)
q_a := unit_interval(seed_a)
q_b := unit_interval(seed_b)

// Same LUT, different quantile inputs.
wait_a := lut_sample(q_a, lut)
wait_b := lut_sample(q_b, lut)
```

### Pinned: guarding against accidental cycle-time contamination

```
// The user wants to be deliberate: this must stay init-time.
$lut := dist_normal(72.0, 5.0)

// If someone refactors and accidentally makes the mean depend on
// a cycle-time value, the $ pin catches it at assembly time.
```

---

## Relation to Other Design Documents

- **12_init_vs_cycle_state.md** — defines the two-lifecycle model
  and the `$` pinning sigil. This document elaborates the init
  object mechanics within that framework.
- **04_gk_function_library.md** — the `WeightedLookup` node from
  Tier 1 is an init-object consumer (backed by an alias table
  loaded from CSV).
- **09_alias_method.md** — `AliasTableU64` and `AliasTable<String>`
  are init objects.
- **11_icd_sampling.md** — `LutF64` tables built by `dist_*`
  functions are init objects.
- **10_aot_compilation.md** — init objects are captured by `Arc` in
  compiled closures, compatible with the Phase 2 model.
