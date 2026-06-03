# Node Library

A **node** in polydat is a pure function from typed inputs to a typed
output, registered under a string name. Once compiled into a function
graph, a node becomes a vertex the runtime evaluates against the
current coordinates and any upstream node outputs. Determinism is the
contract: the same inputs always produce the same output, with no
shared mutable state.

Workload authors rarely reach for nodes by name. The Polydat DSL and the
op-template binding cascade compose them on the author's behalf — you
write `mod(hash(cycle), 1000)` and the resolver wires `hash` and `mod`
nodes into the graph. The catalog below is the menu the resolver picks
from, not a usage manual.

**Inventory:** 230 registered names across 30 source files (live in
[`src/nodes/`](../src/nodes/)). This file is a hand-curated snapshot;
when the runtime diverges, the source files are authoritative.

---

## Numeric core

The arithmetic and comparison building blocks every workload reaches
for, plus the type-conversion bridges between `u64`, `f64`, and `bool`.

### `arithmetic` — common math operations
`add`, `mul`, `div`, `mod`, `clamp`, `interleave`, `mixed_radix`,
`ceil_to_multiple`, `multiples_at_least`, `div_wire`, `mod_wire`,
`set_or_get`. The `_wire` variants take the divisor as a wire instead
of a const; `mixed_radix` decomposes a `u64` into positional digits
(useful for `(device, reading) := mixed_radix(cycle, 100, 0)` style
coordinate splits).

### `bitwise` — explicit `u64` ops + overflow-checked variants
`u64_add`, `u64_sub`, `u64_mul`, `u64_div`, `u64_mod`, `u64_and`,
`u64_or`, `u64_xor`, `u64_not`, `u64_shl`, `u64_shr`, `checked_add`,
`checked_sub`, `checked_mul`. Distinct from `arithmetic` because the
type discipline is stricter — no implicit f64 coercion — and the
`checked_*` family surfaces overflow as a typed result.

### `math` — trig, exp/log, f64 arithmetic
`sin`, `cos`, `tan`, `asin`, `acos`, `atan`, `atan2`, `sqrt`, `exp`,
`ln`, `pow`, `abs_f64`, `f64_add`, `f64_sub`, `f64_mul`, `f64_div`,
`f64_mod`. The `f64_*` arithmetic is the floating-point counterpart to
`bitwise`'s `u64_*` family.

### `compare` — predicates and ternary selection
`u64_eq`, `u64_ne`, `u64_lt`, `u64_le`, `u64_gt`, `u64_ge`,
`f64_eq`, `f64_ne`, `f64_lt`, `f64_le`, `f64_gt`, `f64_ge`,
`str_eq`, `str_ne`, `select_u64`, `select_f64`, `select_str`. The
`select_*` family is a typed `if (cond, then, else)` returning the
matching branch.

### `convert` — cross-type coercion and rounding
`to_f64`, `f64_to_u64`, `floor_to_u64`, `ceil_to_u64`, `round_to_u64`,
`clamp_f64`, `unit_interval`, `discretize`, `format_u64`, `format_f64`,
`zero_pad_u64`. `unit_interval` maps a `u64` hash to `[0.0, 1.0)` —
the input most distribution-sampling nodes expect.

### `lerp` — interpolation and range remap
`lerp`, `scale_range`, `quantize`. `scale_range` is the common
"map this u64 hash into [low, high]" workhorse.

---

## Determinism and sampling

How polydat turns a single `cycle` integer into reproducible variates
spread across a population — without shared state, without RNG
seeding.

### `hash` — the source of all randomness
`hash`. A 64-bit hash of one or more u64 inputs. Chaining `hash(hash(x))`
produces independent streams from the same coordinate without an RNG.

### `pcg` — PCG-RXS-M-XS 64/64 random sequences
`pcg`, `pcg_stream`, `shuffle`, `cycle_walk`. Used when a single
hash isn't enough — e.g., generating an N-element permutation
(`shuffle`) or walking a stream of independent values from one seed.

### `probability` — modeled probabilistic selection
`chance`, `fair_coin`, `unfair_coin`, `one_of`, `one_of_weighted`,
`n_of`, `select`, `blend`, `default_or`, `a`, `b`, `c`. The
single-letter `a`/`b`/`c` are positional outputs of multi-output nodes
(see [SRD-66 `pick`](../../docs/sysref/66_runtime_feature_detection.md)).

### `weighted` — convenience weighted-output selection
`weighted_pick`, `weighted_u64`, `weighted_strings`,
`dynamic_weighted_select`. Author-facing shortcuts for the common
"pick one with these weights" pattern; lower-level nodes do the actual
work.

### `partition` — partition-typed indexing (SRD 71)
`partitions`, `at`, `start_of`, `end_of`, `cardinality`, `idx_of`,
`mod_in`, `clamp_in`. The `Partition` type wraps a closed-open `u64`
range and the partition nodes give type-checked access to its bounds
and to per-index slicing.

### `noise` — coherent noise
`perlin_1d`, `perlin_2d`, `simplex_2d`, `fractal_noise_1d`,
`fractal_noise_2d`. Smooth pseudo-random fields for procedural
landscape / texture / time-series workloads.

---

## Strings, encoding, formatting

### `string` — construction and case
`str_concat`, `str_lower`, `str_upper`, `char_buf`, `combinations`,
`number_to_words`, `hashed_uuid`, `file_line_at`. `combinations`
generates the k-th element of an ordered cross-product over symbol
alphabets — useful for generating "label_01", "label_02", … without
storing the list.

### `format` — printf-style
`printf`. Format-string node accepting positional `{0}`, `{1}` slots.

### `regex` — match and substitute
`regex_match`, `regex_replace`.

### `encoding` — HTML and URL
`html_encode`, `html_decode`, `url_encode`, `url_decode`.

### `digest` — cryptographic hashes and base encoding
`sha256`, `md5`, `to_base64`, `from_base64`.

### `bytebuf` — raw byte assembly
`u64_to_bytes`, `bytes_from_hash`, `to_hex`, `from_hex`. The
hex/bytes nodes are the bridge between integer hashes and on-the-wire
byte payloads.

---

## Structured data

### `json` — construction, merge, projection
`json_text`, `to_json`, `json_to_str`, `json_merge`, `escape_json`,
`array_at`, `array_len`, `body_column_i32`, `normalize_vector`,
`random_vector`. `to_json` accepts a wire and returns its JSON
serialization; `json_merge` composes two JSON objects with right-wins
precedence.

### `datetime` — timestamps and epoch math
`epoch_offset`, `epoch_scale`, `to_timestamp`, `date_components`.
`epoch_scale` + `epoch_offset` together let you map a cycle index
into a timestamp distribution (e.g., "spread 1M events over the last
24 hours").

---

## Runtime context and I/O

These nodes break determinism on purpose — they read state that exists
outside the function graph (env vars, the wall clock, live metrics,
runtime controls). Use sparingly; most workload values should stay
hash-derived.

### `context` — session-scoped state
`counter`, `current_epoch_millis`, `session_start_millis`,
`elapsed_millis`, `thread_id`, `tmp_dir`, `env`, `env_or`, `limit`.

### `runtime_context` — declared controls + Polydat runtime axes
`cycle`, `phase`, `rate`, `concurrency`, `control`, `control_u64`,
`control_str`, `control_bool`, `control_set`. The `control` family
reads dynamic controls (SRD-23) declared on the active component.

### Live-metric nodes (registered externally)
`metric`, `metric_window`, `rate`, `mean`, `p50`, `p99`, `cycles`,
`errors`. These read live values from a session's metrics store and
are useful inside `relevancy:` / stop-condition expressions. They
live in the **`nbrs-metrics`** crate (`polydat_nodes` module),
registered into polydat's node catalog at link time via the
`inventory` channel — polydat's published surface doesn't include
them. Build a binary that links `nbrs-metrics` (or call
`nbrs_metrics::polydat_nodes::set_global_query` from your runner) to
get them.

### `log_levels` — pass-through logging at each level
`log_debug`, `log_info`, `log_warn`, `log_error`. Each takes a value,
emits it to the named log level, and returns the value unchanged
(taps in a graph for debugging without breaking the data flow).

### `diagnostic` — introspection tools
`inspect`, `debug_repr`, `type_of`, `fft_analyze`. `inspect` is the
on-graph equivalent of a print statement; `type_of` returns the
runtime PortType for debugging type-error chains.

---

## Real-world data and external sources

### `realer` — bundled human-readable datasets
`first_names`, `full_names`, `country_names`, `state_codes`. Backed by
CSV files in [`data/`](../data/) — Census names, ISO country codes,
US state abbreviations.

### `datafile` — ordinal access to user-provided files
`csv_field`, `csv_row`, `csv_row_count`, `jsonl_field`, `jsonl_row`,
`jsonl_row_count`. Reads a row or field by index from a CSV or JSONL
file the workload points at — the bridge from polydat to arbitrary
tabular data.

### `vectors` — vectordata integration (feature `vectordata`)
35 nodes for ML/AI vector dataset access — `dataset_open`,
`dataset_prebuffer`, `vector_at`, `query_vector_at`, `neighbor_indices`,
`neighbor_distances`, `matching_profiles`, `profile_facets`, and many
profile / filter / metadata accessors. Used by recall-benchmark
workloads that need to compare generated query vectors against
pre-computed ground-truth neighbor sets.

---

## Validation and assertion

### `param_helpers` — declarative validation predicates
`required`, `in_range`, `is_positive`, `is_one_of`, `matches`,
`this_or`. Used in op-template parameter validation — fail at compile
time rather than letting a bad param surface at cycle dispatch.

### `exactly_one` — explicit unary unwrap
`exactly_one_value`. Asserts a multi-element structural body has
exactly one element and returns it. SRD-66 §"Surface 3" details
the dispatch contract.

### `assertions` — type and value assertions
Family of nodes (`assert_u64`, `assert_f64`, `assert_str`,
`assert_vec_f32`, `assert_u64_nonzero`, `assert_u64_range`, …)
generated by type/constraint builders rather than registered under
fixed names. Used by the runtime to enforce port-type and
const-constraint contracts at wire boundaries.

---

## Source layout

| File | Nodes |
|------|------:|
| [`vectors.rs`](../src/nodes/vectors.rs) | 35 |
| [`math.rs`](../src/nodes/math.rs) | 17 |
| [`compare.rs`](../src/nodes/compare.rs) | 17 |
| [`probability.rs`](../src/nodes/probability.rs) | 12 |
| [`bitwise.rs`](../src/nodes/bitwise.rs) | 14 |
| [`arithmetic.rs`](../src/nodes/arithmetic.rs) | 12 |
| [`convert.rs`](../src/nodes/convert.rs) | 11 |
| [`json.rs`](../src/nodes/json.rs) | 10 |
| [`runtime_context.rs`](../src/nodes/runtime_context.rs) | 9 |
| [`context.rs`](../src/nodes/context.rs) | 9 |
| [`partition.rs`](../src/nodes/partition.rs) | 8 |
| [`string.rs`](../src/nodes/string.rs) | 8 |
| `nbrs-metrics::polydat_nodes` (externally registered) | 8 |
| [`param_helpers.rs`](../src/nodes/param_helpers.rs) | 6 |
| [`datafile.rs`](../src/nodes/datafile.rs) | 6 |
| [`noise.rs`](../src/nodes/noise.rs) | 5 |
| [`weighted.rs`](../src/nodes/weighted.rs) | 4 |
| [`log_levels.rs`](../src/nodes/log_levels.rs) | 4 |
| [`digest.rs`](../src/nodes/digest.rs) | 4 |
| [`encoding.rs`](../src/nodes/encoding.rs) | 4 |
| [`pcg.rs`](../src/nodes/pcg.rs) | 4 |
| [`bytebuf.rs`](../src/nodes/bytebuf.rs) | 4 |
| [`realer.rs`](../src/nodes/realer.rs) | 4 |
| [`datetime.rs`](../src/nodes/datetime.rs) | 4 |
| [`diagnostic.rs`](../src/nodes/diagnostic.rs) | 4 |
| [`lerp.rs`](../src/nodes/lerp.rs) | 3 |
| [`regex.rs`](../src/nodes/regex.rs) | 2 |
| [`hash.rs`](../src/nodes/hash.rs) | 1 |
| [`format.rs`](../src/nodes/format.rs) | 1 |
| [`exactly_one.rs`](../src/nodes/exactly_one.rs) | 1 |
| [`assertions.rs`](../src/nodes/assertions.rs) | (typed family — not name-registered) |
| [`pick.rs`](../src/nodes/pick.rs) | (op-template dispatch primitive) |
| [`random.rs`](../src/nodes/random.rs) | (non-deterministic prototyping) |
| [`fixed.rs`](../src/nodes/fixed.rs) | (const / fixed-value family) |
| [`identity.rs`](../src/nodes/identity.rs) | (identity / constant) |
