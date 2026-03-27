# GK Function Library — Design Sketch

Initial scoping of the standard function library for the generation kernel,
derived from the warehouse assessment of Java virtdata's 526
`@ThreadSafeMapper` classes (see `03_virtdata_function_catalog.md`).

---

## Design Principles

1. **Fewer functions, more composition** — the GK's DAG model, *-arity
   nodes, and named wires eliminate entire categories of Java functions
   that existed only to work around virtdata's unary pipeline limitation.

2. **Generics over duplication** — Rust generics and traits collapse the
   Java pattern of N type-variant copies of the same function (e.g.,
   `Hash` as long→long, long→int, int→int) into single implementations.

3. **Data-driven over code-driven** — realistic data (names, places) and
   weighted sampling should be a single parameterized node backed by data
   files, not dozens of bespoke classes.

4. **Edge adapters for type crossing** — type conversion lives on edges,
   not as first-class domain nodes. A small set of generic adapters
   replaces ~80 Java conversion classes.

5. **Defer what's not needed yet** — vector/DNN operations, HDF5 access,
   and file format readers can come in a later phase.

6. **Explicit hashing provenance** — no function may implicitly hash its
   input. Hashing is always an explicit, visible step in the DAG. Hash
   nodes are injected by the user (or by library constructs) at specific
   points to make the data semantics and relationships clear. This means
   distribution nodes receive pre-hashed (uniform) input — they do not
   hash internally. The user controls where entropy is introduced, and
   the DAG makes that visible. N-ary hash functions (combining multiple
   coordinates into one hash) are also explicit nodes, not hidden inside
   downstream consumers.

7. **Default hash algorithm: xxHash3** — sketched in as the core hash
   function for initial development. The function library is modular, so
   alternative algorithms (xoroshiro variants, PCG, murmur3, etc.) can
   be offered as additional hash nodes. Final selection will be informed
   by benchmarking throughput and distribution quality on the hot path.
   The important thing is that the hash node interface is algorithm-
   agnostic — swapping the implementation doesn't change the DAG
   structure.

8. **Named parameters everywhere** — the user-facing model uses named
   parameters in all contexts: GK wire names, template placeholders, op
   template bind points. Positional addressing is an internal
   optimization only, never exposed to the user.

9. **Surface-level expression language** — a lightweight expression
   language is needed outside the GK for runtime configuration concerns:
   external wiring, system integration conventions, situational
   parameters that must be derived reliably. This is not a GK hot-path
   concern — expressions are evaluated at configuration/assembly time.
   Within the GK, arithmetic is expressed as explicit nodes (Add, Mul,
   etc.), not parsed expressions. The expression language should follow
   Rust ecosystem idioms; candidates to evaluate include existing crates
   rather than inventing a custom parser.

---

## Tier 1: Core Nodes (Essential for Initial Release)

These are the irreducible building blocks. Every realistic workload will
use some combination of these.

### 1.1 Hashing

The foundation of deterministic pseudo-random generation. A hash converts
a coordinate into an unpredictable but reproducible value.

| Node          | Arity | Signature       | Description                        |
|---------------|-------|-----------------|------------------------------------|
| Hash64        | 1→1   | u64 → u64       | High-quality 64-bit hash (xxhash3 default) |

**Design note:** Java had Hash, FullHash, SignedHash, HashRange,
HashRangeScaled, AddHashRange — ~46 classes. The GK has one hash node.
Bounding a hash to a range is `hash → mod`. Scaling to a float interval
is `hash → u64_to_f64 → scale`. There are no fused hash+transform
nodes — the single-responsibility principle applies, and the AOT
compiler eliminates the composition overhead. Users who want a
"hash_range" shorthand build a library kernel.

### 1.2 Arithmetic

| Node     | Arity | Signature(s)         | Description              |
|----------|-------|----------------------|--------------------------|
| Add      | 1→1   | u64 → u64, f64 → f64 | Add constant             |
| Mul      | 1→1   | u64 → u64, f64 → f64 | Multiply by constant     |
| Div      | 1→1   | u64 → u64, f64 → f64 | Divide by constant       |
| Mod      | 1→1   | u64 → u64            | Modulo                   |
| Clamp    | 1→1   | u64 → u64, f64 → f64 | Clamp to [min, max]      |
| Min      | N→1   | (u64, ...) → u64     | Minimum of inputs        |
| Max      | N→1   | (u64, ...) → u64     | Maximum of inputs        |
| AddWire  | 2→1   | (u64, u64) → u64     | Add two wired inputs     |
| MulWire  | 2→1   | (u64, u64) → u64     | Multiply two wired inputs|

**Design note:** `Add`, `Mul`, `Div`, `Mod`, `Clamp` are parameterized
with a constant at assembly time. `AddWire` and `MulWire` combine two
live DAG values. This replaces Java's pattern of fusing arithmetic into
hash functions (AddHashRange, HashRangeScaled, etc.).

### 1.3 Identity & Constants

| Node       | Arity | Signature  | Description                   |
|------------|-------|------------|-------------------------------|
| Identity   | 1→1   | u64 → u64  | Passthrough                   |
| Const      | 0→1   | () → u64   | Emit a fixed value            |
| ConstStr   | 0→1   | () → String | Emit a fixed string          |

### 1.4 Decomposition & Composition

These are the *-arity workhorses that distinguish the GK from virtdata.

| Node          | Arity | Signature              | Description                        |
|---------------|-------|------------------------|------------------------------------|
| MixedRadix    | 1→N   | u64 → (u64, u64, ...) | Decompose into mixed-radix digits  |
| CycleRange    | 1→1   | u64 → u64              | Cycle through [0, size)            |
| Interleave    | N→1   | (u64, ...) → u64       | Interleave bits from N inputs      |
| Concat        | N→1   | (u64, ...) → u64       | Concatenate via shift-and-or       |
| Pack          | N→1   | (u64, ...) → u64       | Pack N values into fields of one   |
| Unpack        | 1→N   | u64 → (u64, u64, ...)  | Unpack fields from one value       |

**Design note:** `MixedRadix` is the key node for cartesian coordinate
decomposition — it replaces the Java pattern of chaining `Div` and `Mod`
to extract dimensions from a flat cycle number.

### 1.5 Statistical Distributions

Shaped sampling from the u64 coordinate space. Each distribution node
takes a hashed u64 input (uniform) and maps it through the inverse CDF
to produce a shaped output.

| Node          | Arity | Signature   | Description                         |
|---------------|-------|-------------|-------------------------------------|
| Normal        | 1→1   | u64 → f64   | Gaussian (mean, stddev)             |
| Uniform       | 1→1   | u64 → f64   | Continuous uniform [min, max)       |
| Zipf          | 1→1   | u64 → u64   | Power-law rank (n, exponent)        |
| Pareto        | 1→1   | u64 → f64   | Power-law (scale, shape)            |
| Exponential   | 1→1   | u64 → f64   | Exponential (rate)                  |
| Poisson       | 1→1   | u64 → u64   | Poisson (lambda)                    |
| Binomial      | 1→1   | u64 → u64   | Binomial (trials, probability)      |
| Gamma         | 1→1   | u64 → f64   | Gamma (shape, scale)                |
| LogNormal     | 1→1   | u64 → f64   | Log-normal (mean, stddev)           |
| Beta          | 1→1   | u64 → f64   | Beta (alpha, beta)                  |
| Geometric     | 1→1   | u64 → u64   | Geometric (probability)             |
| Weibull       | 1→1   | u64 → f64   | Weibull (shape, scale)              |

**Design note:** Java had 70 distribution classes (each distribution × 4
type combos). In the GK, each distribution is one node. Input is always
u64 (pre-hashed). Output type (u64 or f64) is fixed per distribution
semantics. Edge adapters handle any further type conversion. The 20
continuous and 8 discrete distributions in Java reduce to ~12 essential
nodes that cover real-world workload shaping needs.

### 1.6 Weighted Lookup

A single generic node that replaces Java's CSVSampler, WeightedStrings,
WeightedInts, and all 23 "realer" classes.

| Node             | Arity | Signature    | Description                      |
|------------------|-------|--------------|----------------------------------|
| WeightedLookup   | 1→1   | u64 → T      | Sample from weighted dataset     |

Parameterized at assembly time with:
- A data source (inline table, CSV file, or embedded dataset)
- A weight column (optional — uniform if absent)
- An output column and type

**Design note:** This one node, combined with bundled data files (US
census names, geographic data, etc.), replaces all of virtdata-lib-realer
and the CSV/delimited samplers from virtdata-lib-basics.

---

## Tier 2: Standard Nodes (Expected for Practical Workloads)

These extend the core with commonly needed capabilities.

### 2.1 String Construction

| Node          | Arity | Signature              | Description                     |
|---------------|-------|------------------------|---------------------------------|
| Template      | N→1   | (T, ...) → String      | Named-parameter interpolation   |
| Join          | N→1   | (String, ...) → String | Join with delimiter             |
| Substring     | 1→1   | String → String        | Extract substring               |
| Prefix        | 1→1   | String → String        | Prepend constant                |
| Suffix        | 1→1   | String → String        | Append constant                 |
| PadLeft       | 1→1   | String → String        | Left-pad to width               |
| PadRight      | 1→1   | String → String        | Right-pad to width              |
| RegexReplace  | 1→1   | String → String        | Regex substitution              |

**Design note:** `Template` is an N→1 node that consumes multiple named
wires and produces a single string via named-parameter interpolation
(e.g., `"{name} lives in {city}"`). Wire names map directly to template
placeholders. This replaces Java's StringCompositor/StringBindings
pattern. A separate `Format` with positional arguments is not exposed to
the user — positional addressing may be used internally as an
optimization after assembly resolves names to positions.

### 2.2 Collection Assembly

| Node       | Arity | Signature              | Description                    |
|------------|-------|------------------------|--------------------------------|
| ListOf     | N→1   | (T, ...) → List<T>     | Assemble inputs into a list    |
| MapOf      | N→1   | (K,V, ...) → Map<K,V>  | Assemble pairs into a map      |
| SetOf      | N→1   | (T, ...) → Set<T>      | Assemble inputs into a set     |
| Repeat     | 1→1   | (u64, fn) → List<T>    | Apply fn for [0..n), collect   |

**Design note:** Java had 20+ collection classes (ListSized, ListHashed,
MapSizedStepped, etc.). In the GK, collection assembly is a natural N→1
node. Sizing and stepping are upstream DAG concerns, not collection node
parameters.

### 2.3 Time & Date

| Node           | Arity | Signature    | Description                     |
|----------------|-------|--------------|---------------------------------|
| EpochMillis    | 1→1   | u64 → u64    | Scale to epoch milliseconds     |
| ToTimestamp    | 1→1   | u64 → String | Format as ISO-8601 or custom    |
| ToUUID         | 1→1   | u64 → String | Deterministic UUID from u64     |
| ToTimeUUID     | 1→1   | u64 → String | Time-based UUID (v1-style)      |

### 2.4 Encoding

| Node           | Arity | Signature        | Description                  |
|----------------|-------|------------------|------------------------------|
| ToHex          | 1→1   | u64 → String      | Hexadecimal encoding         |
| ToBase64       | 1→1   | bytes → String    | Base64 encoding              |
| ToJSON         | 1→1   | T → String        | JSON serialization           |
| URLEncode      | 1→1   | String → String   | URL encoding                 |

---

## Tier 3: Deferred Nodes (Later Phases)

### 3.1 Vector Operations (deferred)

- Vector generation (deterministic, hashed)
- DNN-specific patterns (angular, euclidean)
- Normalization (L2)
- Padding, slicing, type conversion (float[] ↔ double[])

### 3.2 External Data Access (deferred)

- HDF5 file reading
- fvec/ivec file readers
- File line readers (DirectoryLines)

### 3.3 Digest / Crypto (deferred)

- MD5, SHA digests
- ByteBuffer operations

---

## Edge Adapters (Type Conversion on Edges)

These are unary, live on edges, and are auto-inserted where unambiguous.

| Adapter      | Signature       | Auto-insert? | Notes                    |
|--------------|-----------------|--------------|--------------------------|
| U64ToString  | u64 → String    | yes          | Decimal representation   |
| F64ToString  | f64 → String    | yes          | Default precision        |
| U64ToF64     | u64 → f64       | yes          | Lossless for ≤ 2^53     |
| F64ToU64     | f64 → u64       | no           | Lossy — require explicit |
| BoolToU64    | bool → u64      | yes          | 0/1                      |
| U64ToBool    | u64 → bool      | yes          | 0 = false, else true     |
| U64ToBytes   | u64 → [u8; 8]   | no           | Endianness matters       |

**Design note:** Java had ~52 dedicated conversion classes. Rust's
`From`/`Into` traits and the edge adapter model reduce this to a small
set. The assembly phase inserts adapters automatically for common
coercions. Lossy conversions (f64→u64, narrowing) require explicit user
choice.

---

## Function Count Summary

| Tier        | Nodes | Replaces (Java) | Notes                         |
|-------------|------:|:----------------:|-------------------------------|
| Tier 1 Core |   ~35 | ~350             | Hashing, arithmetic, distributions, lookup |
| Tier 2 Std  |   ~20 | ~120             | Strings, collections, time, encoding |
| Edge Adapt  |    ~7 | ~52              | Type conversions              |
| **Total**   |**~62**| **~522**         | **~12% of Java class count**  |
| Tier 3 Def  |   ~20 | remaining        | Vectors, HDF5, digests        |

---

## Open Questions

- ~~Q10a: resolved — see design decisions below~~
- ~~Q10b: resolved — see design decisions below~~
- ~~Q10c: resolved — CSV, loaded at assembly time, not on hot path~~
- ~~Q10d: resolved — named parameters at user level, positional internal only~~
- ~~Q10e: resolved — lightweight expression language needed, but not inside the GK hot path~~
