# Library Catalog

The Polydat node library provides deterministic, composable functions
for data generation. Nodes are registered in the DSL compiler's
function registry and available by name in `.polydat` source.

This doc is the catalog reference for the polydat library
(`polydat/src/library/`). The node-metadata contract that every
entry satisfies is specified in
[composition_substrate.md §2 (slot contract)](composition_substrate.md);
fusion of catalog nodes is in
[graph_compiler.md §5](graph_compiler.md); virtual-node
registration is in
[expression_engine.md §5.5](expression_engine.md).

### Wire Cost Classes

Some node inputs are **configuration wires** — changing them
invalidates expensive internal state (e.g., recomputing a
lookup table for weighted selection). Other inputs are
**data wires** — cheap per-cycle values that drive the
node's primary computation.

Node metadata should declare the cost class of each input port:

| Class | Semantics | Example |
|-------|-----------|---------|
| `config` | Expensive to change. Initializes internal state (LUT, distribution table). Expected to be wired to effectively-const sources (per [GK Evaluation Model](evaluation_model.md): compile-const, scope-init, or iteration externs). | `weighted_strings` weights parameter |
| `data` | Cheap dynamic input. The node's primary computation path. | `hash` input value, `mod` dividend |

The compiler can use this information to:
- **Warn** when a `config` wire is connected to a dynamic
  binding (the LUT would be rebuilt on every pull)
- **Error** when the cost would be catastrophic (e.g., O(n)
  rebuild per pull on a million-entry distribution)
- **Allow** explicit override when the user intentionally wants
  dynamic reconfiguration (functional testing of the node)

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

### Math (binary f64)

| Node | Signature | Description |
|------|-----------|-------------|
| `f64_add` | `f64, f64 → f64` | addition |
| `f64_sub` | `f64, f64 → f64` | subtraction |
| `f64_mul` | `f64, f64 → f64` | multiplication |
| `f64_div` | `f64, f64 → f64` | division |
| `f64_mod` | `f64, f64 → f64` | modulo |
| `to_f64` | `u64 → f64` | widen u64 to f64 |

### Integer (two-wire u64)

| Node | Signature | Description |
|------|-----------|-------------|
| `u64_add` | `u64, u64 → u64` | addition |
| `u64_sub` | `u64, u64 → u64` | subtraction |
| `u64_mul` | `u64, u64 → u64` | multiplication |
| `u64_div` | `u64, u64 → u64` | division |
| `u64_mod` | `u64, u64 → u64` | modulo |

### Bitwise

| Node | Signature | Description |
|------|-----------|-------------|
| `u64_and` | `u64, u64 → u64` | bitwise AND |
| `u64_or` | `u64, u64 → u64` | bitwise OR |
| `u64_xor` | `u64, u64 → u64` | bitwise XOR |
| `u64_shl` | `u64, u64 → u64` | left shift |
| `u64_shr` | `u64, u64 → u64` | logical right shift |
| `u64_not` | `u64 → u64` | bitwise complement |

### Checked Arithmetic

| Node | Signature | Description |
|------|-----------|-------------|
| `checked_add` | `u64, u64 → u64, bool` | add with overflow flag |
| `checked_sub` | `u64, u64 → u64, bool` | subtract with underflow flag |
| `checked_mul` | `u64, u64 → u64, bool` | multiply with overflow flag |

The bool output wire is `true` on overflow/underflow. Safe
replacement for wrapping arithmetic when correctness matters.

### String Generation (extended)

| Node | Signature | Description |
|------|-----------|-------------|
| `hashed_uuid` | `u64 → String` | deterministic UUID v4-format from hash |
| `char_buf` | `u64, u64 → String` | fixed-length character buffer |
| `file_line_at` | `u64, String → String` | line from file at index |

### Array and JSON

| Node | Signature | Description |
|------|-----------|-------------|
| `array_len` | `JSON → u64` | number of elements in JSON array |
| `array_at` | `JSON, u64 → JSON` | element at index |
| `normalize_vector` | `JSON → JSON` | L2-normalize a float array |
| `random_vector` | `u64, u64 → JSON` | random unit vector of given dimension |

### Diagnostic

| Node | Signature | Description |
|------|-----------|-------------|
| `fft_analyze` | `JSON → JSON` | FFT frequency analysis of a float array |
| `log_debug` | `T → T` | Emit value at Debug log level; return input unchanged. Declared `Purity::SideChannel { sink: LogBuffer }` so output elision can't drop it when the return value is unused. |
| `log_info` | `T → T` | As `log_debug`, at Info level. |
| `log_warn` | `T → T` | As `log_debug`, at Warn level. |
| `log_error` | `T → T` | As `log_debug`, at Error level. |

Each log node takes one wire input, emits a log line at the
named level via the runtime's diag pipeline (per
[SRD-41](../../../docs/sysref/41_logging.md)), and returns the
input unchanged. The pass-through return value lets workloads
insert logging into a binding chain without restructuring;
the side-channel purity declaration suppresses output elision
when the return value is unused. The logged line carries the
value (formatted via `Value::Display` / `Debug`), the wire
name when known, and the op-template context — same
diagnostic enrichment the eval-panic path uses.

### Branched dispatch and structured-body assertions

These nodes back the runtime-feature-detection pattern
documented as workload surface in
[SRD-66](../../../docs/sysref/66_runtime_feature_detection.md).

| Node | Signature | Description |
|------|-----------|-------------|
| `pick` | `Bool×N, T×N → T` | Variadic dispatcher: 2N wires (N selectors + N values). Exactly one selector must be `true`; returns the matching value. Zero-true or multi-true is an eval-time error. All values must share a common type. |
| `exactly_one_value` | `<StructuralBody> → V` | Assert a structural body is unary (1 row × 1 column × 1 cell) and return the cell value. Non-unary input is an eval-time error naming the actual `<r> rows × <c> columns`. |

#### `pick` — semantics

- Evaluates all 2N inputs (no short-circuit; Polydat is data-flow).
- Counts how many of `b0..bN-1` are `true`.
  - Exactly one true → return the corresponding `vi`.
  - Zero true → eval-time error: "pick: no selector matched
    (all N booleans false); workload author guarantees one of
    {b0, …, bN-1} is true at this point."
  - Two or more true → eval-time error: "pick: multiple
    selectors matched (b1, b3, …); selectors must be mutually
    exclusive."
- Errors flow through `enrich_eval_panic` so the operator
  sees the function name, the result-wire context, and the
  inputs.

**Argument shape.** Selectors and values are split into two
halves rather than interleaved `(b0,v0,b1,v1,…)`. The split
form scans more cleanly for long lists
(`pick(has_sai, has_idx, has_dse, "tbl_a", "tbl_b", "tbl_c")`),
composes naturally with construction helpers (operators pull
the two lists from separate variables), and catches a missing
pair at compile time as "odd total" — a more direct diagnostic
than the interleaved form's "value missing in last pair".

**Type rules.**

- All `bi` MUST be `Bool` at compile time. A non-bool slot is
  rejected by the parameter validator.
- All `vi` MUST share a common type at compile time
  (`Str`+`Str`, `U64`+`U64`, etc. — no implicit promotion). The
  output type is that common type.
- Mixing types in the value slots is a compile-time error
  pointing at the first mismatched index.

**Variadic registration.** Registers via
`Arity::VariadicWires { min_wires: 2 }`. The variadic
constructor takes `n: usize` (total wire count) and validates
`n` is even and `n ≥ 2`; the half-point `N = n / 2` is stored
so eval-time indexing is direct: selectors at `inputs[0..N]`,
values at `inputs[N..2N]`.

**Diagnostic guidance** is a static suffix added by the
`pick` node's panic handler — generic enough to fit every
misuse without guessing the workload's structure:

```text
pick: no selector matched (all N=2 booleans false)
  ↳ in node `pick` (output `target_index_table`)
     while evaluating <op-template `indexes_present`>
  ↳ inputs: [Bool(false), Bool(false), Str("system_views.sai_column_indexes"), Str("system_views.indexes")]
  ↳ hint: did the probe phase that sets these booleans run
     before this phase? Check scenario-tree DFS order or
     declare a `detect_*` phase ahead of consumers.
```

#### `exactly_one_value` — motivation and semantics

The motivating use case has a CQL `describe keyspace` op
whose body is a single row × single text column carrying
schema text. To regex-match against that text, the workload
needs to **unwrap** the structural body — extract the
one-and-only text value — before applying `regex_match`.

`exactly_one_value(body)` is the explicit-assertion approach
(rejecting the implicit-modal-projection alternative that the
adapter's `body.to_text()` could choose unary vs JSON
stringify based on shape — implicit modal behaviour is
unwelcome).

**Semantics.**

- Walk the body's rows (must be one), columns (must be one),
  cells (must be one).
- If exactly one row × one column × one cell, return the
  cell value.
- Otherwise eval-time error: `"exactly_one_value: expected
  unary structure (1 row × 1 column), found <r> rows × <c>
  columns"`.

The error flows through `enrich_eval_panic`; the operator
sees the function name, the result-wire context, and the
body's actual shape.

**Composition with regex** reads as a two-step dance —
assertively extract the unary value, then match against it:

```yaml
result: |
  has_sai_column_indexes := regex_match(exactly_one_value(body),
                            "(?im)^\s*(VIRTUAL\s+)?TABLE\s+system_views\.sai_column_indexes\s*\(")
```

**Body type.** The exact Polydat type of the `body` extern is a
structural value wide enough to round-trip through the
JSON-AST representation that map-shape result wires already
need (per SRD-66 Surface 1 "Map shape composite wire").
`exactly_one_value` is type-agnostic at the substrate layer.

### Runtime context nodes

The reification principle (SRD 10 §"GK as the unified access
surface") makes Polydat the default way for a workload to read any
runtime value. The nodes in this category are how reified
runtime state is named in the DSL. Each one projects a single,
well-defined runtime surface into a Polydat wire — no side channels,
no templating hooks, no ad-hoc reader APIs.

| Node | Signature | Description |
|------|-----------|-------------|
| `control` | `String → f64` | Current committed value of a [dynamic control](../../../docs/sysref/23_dynamic_controls.md) addressed by name, projected through its reified gauge. Resolves by walking up the component tree from the session root, honoring branch scope. Missing controls, non-reified controls, or non-numeric projections return `0.0`. |
| `control_u64` | `String → u64` | As `control`, cast to `u64` (negative values clamp to `0`). Sugar over `f64_to_u64(control(name))`. |
| `control_bool` | `String → bool` | As `control`, projected to `true` iff the gauge value is non-zero. Missing controls return `false`. |
| `control_str` | `String → String` | As `control`, rendered via the control's erased `value_string()`. Useful for enum-valued or string-valued controls. |
| `control_set` | `String, f64 → u64` | Non-blocking write into a named control. Spawns an async task that calls the erased `set_f64` path; the control's `from_f64` converter maps to its native type. Return value is `1` if dispatched, `0` if no session root is installed. The committed `Versioned<T>::origin` carries the enclosing DSL binding name as attribution. |
| `metric` | `String → f64` | Latest reading of a named metric series, scoped to the nearest ancestor component that publishes the series. Pairs with `metric_window(name, duration)` for aggregated views (SRD 42). |
| `phase` | `→ String` | Name of the currently-executing phase. Reads pin against the enclosing executor — never resolves to "some other phase's name". Backed by a `tokio::task_local!` scope so tokio work-stealing can't leak phase identity across fibers. |
| `cycle` | `→ u64` | Current cycle ordinal for the running fiber. Sugar for reaching the cycle value without declaring it as an explicit input. |
| `concurrency` | `→ f64` | Alias for `control("concurrency")` — reads the activity's live fiber count through the reified gauge. |
| `rate` | `→ f64` | Alias for `control("rate")` — reads the live rate-limiter target in ops/sec. |

Writes to runtime state go through the control-write nodes
(`control_set(name, value)` — SRD 23). Read-side context
nodes are side-effect-free and fold / JIT like any other
deterministic projection, subject to the same caveat as
live metric reads: their output changes between cycles by
definition, so constant-folding them is illegal. The engine
registers them as `volatile` so the folder leaves them in
place.

When a new piece of mutable runtime state is added (a new
wrapper knob, a per-adapter tuning dial, an internal counter),
the authoring checklist is:

1. Attach it to the component whose behavior it governs.
2. Decide whether it's a read-only projection (context node)
   or a writable value (control). Both are fine; neither is a
   template / env-var / global.
3. Register the node or control so DSL authors see it by name
   in `--explain` and `dryrun=controls`.

### Parameter resolution and validation

These nodes let a workload compose layered defaults and assert
preconditions on any value flowing through a binding. They
operate on the same Polydat wires everything else does — a
`required(...)` on a workload param is the same mechanism as
`required(...)` on an externally-written wire or a runtime
control.

| Node | Signature | Description |
|------|-----------|-------------|
| `this_or` | `T?, T → T` | Returns the first argument if it resolves to a defined value, otherwise the second. Lets a workload explicitly say "use this or fall back to that" across scopes. Arguments are ordinary wires; `default` can be a literal, a param lookup, an externally-written wire, or another `this_or`. |
| `required` | `T? → T` | Compile / scope-init assertion that the input resolves to a defined, non-empty value. Passes the value through on success; raises an error with the parameter name on failure. Use to catch missing-parameter bugs before cycles run. |
| `is_positive` | `N → N` | Predicate: pass through if value > 0, error otherwise (numeric types). |
| `in_range` | `N, N, N → N` | Predicate: pass through if `lo ≤ value ≤ hi`, error with a range-mismatch diagnostic otherwise. |
| `matches` | `String, String → String` | Predicate: pass through if value matches the regex, error otherwise. |
| `is_one_of` | `T, [T] → T` | Predicate: pass through if value is in the allowed set, error otherwise. (Distinct from the probabilistic `one_of` selector.) |

Predicates stack — the same value can carry several — and are
evaluated at the earliest time the input is known (compile
time for const-folded values, init time for workload params,
cycle time for live reads). Violations at cycle time surface as
`panic!` regardless of compilation level (P1 interpreter, P2
closure, P3 JIT); the JIT path reaches that same observable
behavior through a setjmp/longjmp shim documented in
[JIT Boundary](../design/jit_boundary.md).

### Cursor Partitions (SRD 71)

Partition-typed nodes (`library/partition.rs`). `Partition` /
`PartitionList` ride wires as `Value::Ext` reflected values
(`iteration/cursor_partition.rs`); these nodes are how
workload-author code reads and derives them. The partition
value is effectively-const for a scope activation, so each
eval reduces to constant arithmetic.

| Node | Signature | Description |
|------|-----------|-------------|
| `cardinality` | `Partition → u64` | `end_ord - start_ord` — the number of ordinals the partition covers. |
| `start_of` | `Partition → u64` | Start ordinal (inclusive). |
| `end_of` | `Partition → u64` | End ordinal (exclusive). |
| `idx_of` | `Partition → u64` | 0-based generation position in the resolved list (stable under spec-level reordering). |
| `count_of` | `Partition → u64` | Total partitions in the list this one was resolved as part of; 1 for single-partition specs. Function form of the `partition_count` projection. |
| `mod_in` | `u64, Partition → u64` | `start + (n mod cardinality)` — wraps an arbitrary integer into the partition's range. Cardinality 0 returns the start. |
| `at` | `Partition, u64 → u64` | Bounds-checked `start + i`; panics at eval when `i ≥ cardinality`. The consume-each-ordinal-once counterpart to `mod_in`. |
| `clamp_in` | `u64, Partition → u64` | Saturating projection into the partition (`max(start, min(n, end-1))`); no wrap. |
| `random_in` | `Partition, u64 → u64` | `start + xxh3(seed) mod cardinality` — deterministic per seed, same entropy source as `hash`. Cardinality 0 returns the start. |
| `subdivide` | `Partition, u64 → PartitionList` | `n` near-equal sub-partitions (sizes differ by ≤ 1 ordinal; boundary math identical to the `*/N` spec token). Indices restart at 0 with `count = n`; `base_extent` propagates; pcts interpolate the parent's span. Panics when `n` is 0 or exceeds the cardinality. Also a kernel-aware comprehension source: `for: "inner in subdivide(outer, n)"`. |
| `partitions` | `Str[, Const<u64>] → PartitionList` | Parse a partition spec string and resolve it against `[0, extent)` (extent defaults to 100). The canonical comprehension source for `for: "p in partitions(...)"`. Full spec grammar — forms, tail tokens, `xN`/`~` modifiers, `in` windows, order keywords — in SRD 71. |

The numeric comprehension generator formerly named `subdivide`
(evenly spaced *values* over an interval) is `linear_starts`;
see SRD 18c's named-generator table.

### Vectordata Integration (feature-gated)

Vectors are array-shaped data. The canonical access path keeps
them in their packed binary form end-to-end — the runtime reads
mmap'd `f32` slices, hands them out as `Bytes`, and prepared-
binding adapters wire the bytes directly into the wire protocol
(CQL prepared statements do an LE → BE swap and bind as a
`vector<float, dim>` BLOB; no string round-trip). The string
variants below predate the byte path and remain only for
diagnostic display / debug printing.

**Bytes accessors (production fast path):**

| Node | Signature | Description |
|------|-----------|-------------|
| `vector_at_bytes` | `u64, String → Bytes` | training vector at index, packed `f32` LE bytes |
| `query_vector_at_bytes` | `u64, String → Bytes` | query vector at index, packed `f32` LE bytes |

**Scalar / count accessors:**

| Node | Signature | Description |
|------|-----------|-------------|
| `vector_dim` | `String → u64` | dataset dimension count (const-folded at init) |
| `vector_count` | `String → u64` | training-set size (const-folded at init) |
| `query_count` | `String → u64` | query-set size (const-folded at init) |
| `metadata_value_at` | `u64, String → String` | per-vector metadata value (string for now — metadata types are dataset-conditional and dynamically dispatched) |
| `dataset_distance_function` | `String → String` | similarity metric name |

**Typed-vector accessors:**

| Node | Signature | Description |
|------|-----------|-------------|
| `vector_at` | `Handle, u64 → VecF32` | base vector at index |
| `query_vector_at` | `Handle, u64 → VecF32` | query vector at index |
| `neighbor_indices_at` | `Handle, u64 → VecI32` | ground-truth neighbor indices |
| `neighbor_distances_at` | `Handle, u64 → VecF32` | ground-truth neighbor distances |

The legacy string and byte accessor variants
(`vector_at_bytes` / `query_vector_at_bytes`) are removed.
Vectors flow as typed `Value::VecF32(Arc<[f32]>)` or
`Value::VecI32(Arc<[i32]>)` end-to-end; the CQL adapter binds
them via scylla's native `SerializeValue` for `[T]` and the
display path via `Value::to_display_string()` renders them as
JSON-array text. See SRD 53 §"Native Vector Binding".

Dataset resolution: bare name → `vectordata` catalog → URL →
download + cache. Datasets loaded once globally via
`DATASET_CACHE`.

For workloads, the vectordata module also registers
**cursor-construction sugar** so `cursor row = vectordata_base("ds",
"profile")` collapses the verbose `range` + `dataset_prebuffer` +
`vector_at_bytes` boilerplate into one line and auto-publishes the
`row.vector` projection. See SRD 10 §"Cursor-Constructor Sugar"
for the full surface and how to add a new sugar family for a
different source kind (CSV, streaming, etc.).

---

## Registration

Library nodes are authored via the `#[polydat_node]`
attribute macro and self-register through the `inventory`
crate at link time. The macro is the SOLE authoring path for
workload-callable library nodes (SRD-80b §"the macro is the
sole authoring path"). The canonical-form table in
[`docs/sysref/80b_macro_universal_authoring.md`](../../../docs/sysref/80b_macro_universal_authoring.md)
§"Authoring patterns" lists every recognised function-signature
shape; new nodes use what's there.

```rust
// Scalar — body returns the output value; macro reads
// `category` + `purity` / `commutativity` per attribute.
#[polydat_node(category = Hashing)]
fn hash(input: u64) -> u64 {
    xxh3_64(&input.to_le_bytes())
}

// Const arg — `Const<T>` wraps a workload-supplied literal.
#[polydat_node(category = Arithmetic)]
fn mod_u64(input: u64, modulus: Const<u64>) -> u64 {
    input % modulus.0
}
```

The macro reads everything from the function signature:
- Per-arg metadata via Wire trait consts (`PORT`, `JIT`,
  `RESOLVER`, `WIRE_COST`) — no `#[polydat_node]` attribute
  noise for these
- Marker wrappers (`Const<T>`, `Ext<T>`, `Resolved<R, T>`,
  `Config<T>`, `Option<T>`, `DynamicOutputs<T>`) cover every
  shape the operator surfaces explicitly
- Variadic shapes recognised syntactically: `&[T]` (single),
  two `&[T]` args (split-halves), `Const<Vec<C>>` (trailing
  scalar consts)

Vectordata nodes are registered behind a `vectordata` feature
gate. Cross-crate registration works identically — adapter
crates (`nbrs-adapter-cql`, etc.) declare `#[polydat_node]`
functions in their own source and their nodes appear in the
Polydat registry at link time.

### Carve-outs from the canonical path

Six files contain hand-written `impl PolydatNode for X` blocks
by explicit architectural design:

- `polydat/src/library/identity.rs` — `PortPassthrough`,
  `ConstHandle`, `ConstExt`: compiler-synthesised
  infrastructure (extern-port nodes, fold-pass synthesised
  from runtime values), not DSL-callable.
- `polydat/src/library/assertions.rs` — `AssertType`,
  `AssertValue`: compiler-synthesised via runtime-PortType /
  runtime-ConstConstraint dispatch.
- `polydat/src/library/context.rs` — `CursorLimit`:
  cursor-compiler synthesised; no workload signature.
- `polydat/src/library/sampling/lut.rs` — `LutSample`:
  Rust-internal composition primitive backing the `dist_*`
  family; no DSL surface.

This carve-out list is the ceiling — any new hand-written
`impl PolydatNode for X` in `polydat/src/library/**` outside
these files fails the SRD-80b invariant test at
`polydat/tests/srd80b_invariant.rs`.

---

## Polydat Modules

Reusable `.polydat` files that define subgraphs:

```
// latency_model.polydat
input cycle: u64
base_ns := uniform(hash(cycle), 500000.0, 2000000.0)
jitter := uniform(hash(add(cycle, 1)), 0.9, 1.1)
latency_ns := mul(base_ns, jitter)
```

Module interface inferred: graph inputs = unbound references,
outputs = terminal bindings. Modules inline into the host DAG
with name prefixing to avoid collision.

Resolution chain: workload directory → `--polydat-lib` paths →
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
