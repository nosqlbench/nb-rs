# GK Function Reference

Complete reference for all native node functions and standard library
modules available in the GK generation kernel.

---

## Native Node Functions

### Hashing

#### `hash(input: u64) -> u64`
64-bit xxHash3 of the input. The fundamental building block for
deterministic pseudo-random generation. Every other randomized
function ultimately derives from hashing.

**When to use:** As the first step in any chain that needs
deterministic randomness. Hash converts a predictable input (cycle
counter, entity ID) into a uniformly-distributed u64.

**Example:**
```gk
h := hash(cycle)           // pseudo-random u64 from cycle
user_id := mod(h, 1000000) // bounded to [0, 1M)
```

**JIT level:** P3 (extern call, ~2ns)

---

### Arithmetic

#### `add(input: u64, addend: u64) -> u64`
Wrapping addition of a constant. Overflows wrap around at 2^64.

**When to use:** Offset a value by a fixed amount. Common for
shifting a range: `mod(h, 100)` gives [0,100), `add(mod(h,100), 500)`
gives [500,600).

#### `mul(input: u64, factor: u64) -> u64`
Wrapping multiplication by a constant.

**When to use:** Scale a value. Often used with timestamps:
`mul(reading_idx, 1000)` converts a reading index to millisecond
offsets.

#### `div(input: u64, divisor: u64) -> u64`
Integer division by a constant. Rounds toward zero.

**When to use:** Coarsen a value. `div(cycle, 100)` groups 100
consecutive cycles into one bucket.

#### `mod(input: u64, modulus: u64) -> u64`
Modulo by a constant. Result is in [0, modulus).

**When to use:** Bound a value to a range. The most common
operation after hash: `mod(hash(cycle), N)` gives a uniformly
distributed integer in [0, N). Also used for cyclic patterns:
`mod(cycle, period)` repeats every `period` cycles.

#### `clamp(input: u64, min: u64, max: u64) -> u64`
Clamp an unsigned integer to [min, max].

**When to use:** Enforce bounds when the input might exceed the
desired range. Unlike mod (which wraps), clamp saturates at the
boundary.

#### `interleave(a: u64, b: u64) -> u64`
Interleave the bits of two u64 values into one. Bit 0 of a goes
to bit 0, bit 0 of b goes to bit 1, bit 1 of a goes to bit 2, etc.

**When to use:** Combine two independent dimensions into a single
hash input that preserves locality from both. Essential for
generating values that depend on two coordinates:
`hash(interleave(device_id, reading_idx))` gives a value that
changes when either dimension changes.

#### `mixed_radix(input: u64, radixes...) -> (d0, d1, ..., dN)`
Decompose an integer into mixed-radix digits. Each output is the
value in that radix position. A trailing radix of 0 means unbounded
(consumes the remainder).

**When to use:** The primary tool for coordinate decomposition.
Maps a flat cycle counter into a multi-dimensional space:
```gk
(device, reading) := mixed_radix(cycle, 10000, 0)
// device = cycle % 10000, reading = cycle / 10000
```

**JIT level:** P3 (unrolled urem/udiv chain)

#### `identity(input: u64) -> u64`
Passthrough — returns the input unchanged.

**When to use:** Rarely needed explicitly. Used internally for
wire aliasing. Occasionally useful in conditional patterns where
one branch is "do nothing."

**JIT level:** P3 (single load/store)

---

### Type Conversions

#### `unit_interval(input: u64) -> f64`
Normalize a u64 to a uniform f64 in [0.0, 1.0). Maps 0 → 0.0 and
u64::MAX → ~1.0.

**When to use:** Bridge from integer space to float space. Typically
follows hash: `unit_interval(hash(cycle))` gives a uniform float.
This is the entry point for distribution sampling — the unit
interval value feeds into ICD (inverse CDF) functions.

#### `clamp_f64(input: f64, min: f64, max: f64) -> f64`
Clamp a float to [min, max].

**When to use:** Bound distribution samples to a realistic range.
Normal distributions have infinite tails — clamp to physical limits:
`clamp_f64(icd_normal(q, 22.0, 3.0), 0.0, 100.0)` bounds
temperature to [0, 100].

#### `f64_to_u64(input: f64) -> u64`
Truncate f64 to u64 (toward zero). Lossy for large values or
fractional parts.

**When to use:** When you need an integer from a float computation.
Prefer `round_to_u64` or `floor_to_u64` for explicit rounding
behavior.

#### `round_to_u64(input: f64) -> u64`
Round f64 to nearest u64 (half-up).

**When to use:** Convert a distribution sample to a discrete value
with minimal bias. Best for "approximately N" semantics.

#### `floor_to_u64(input: f64) -> u64`
Floor f64 to u64 (round toward negative infinity).

**When to use:** When you want the lower bound of a range.
`floor_to_u64(scale_range(h, 0.0, 10.0))` gives integers [0, 9].

#### `ceil_to_u64(input: f64) -> u64`
Ceiling f64 to u64 (round toward positive infinity).

**When to use:** When you want the upper bound of a range. Less
common than floor — use when "at least N" semantics are needed.

#### `discretize(input: f64, range: f64, buckets: u64) -> u64`
Bin a continuous f64 in [0, range) into N equal-width buckets.
Returns a bucket index in [0, buckets).

**When to use:** Histogram-style binning. Convert a continuous
measurement into a category: `discretize(temp, 100.0, 10)` bins
temperatures [0,100) into 10 buckets.

#### `format_u64(input: u64, radix: u32) -> String`
Format a u64 as a string in the given radix (2, 8, 10, 16).
Radix 16 prefixes with "0x", radix 2 with "0b", radix 8 with "0o".

**When to use:** Generate hex IDs, binary representations, or
human-readable numbers. Default radix is 10 (decimal).

#### `format_f64(input: f64, precision: usize) -> String`
Format an f64 with a fixed number of decimal places.

**When to use:** Control output precision for float values:
`format_f64(temp, 2)` gives "22.50" instead of "22.500000000001".

#### `zero_pad_u64(input: u64, width: usize) -> String`
Zero-pad an integer to a fixed character width.

**When to use:** Generate fixed-width numeric codes:
`zero_pad_u64(mod(h, 1000000), 8)` gives "00527897".

---

### Distribution Sampling

#### `icd_normal(quantile: f64, mean: f64, stddev: f64) -> f64`
Sample from a normal (Gaussian) distribution via inverse CDF.
The quantile input should be in [0, 1] (from `unit_interval`).

**When to use:** Generate realistic continuous measurements —
temperatures, heights, response times, scores. The most commonly
used distribution.

**Typical chain:** `hash → unit_interval → icd_normal`

#### `icd_exponential(quantile: f64, rate: f64) -> f64`
Sample from an exponential distribution via inverse CDF.

**When to use:** Model wait times, inter-arrival intervals, or
any "memoryless" process. Rate = 1/mean.

#### `lut_sample(quantile: f64, lut: LUT) -> f64`
Interpolating lookup in a precomputed table. The quantile input
maps into the table with linear interpolation between entries.

**When to use:** Used internally by icd_* functions. Use directly
when you've built a custom LUT via init-time `dist_*` builders.

**JIT level:** P3 (extern call with baked LUT pointer)

#### `dist_normal(mean: f64, stddev: f64) -> LUT`
#### `dist_exponential(rate: f64) -> LUT`
#### `dist_uniform(min: f64, max: f64) -> LUT`
#### `dist_pareto(scale: f64, shape: f64) -> LUT`
#### `dist_zipf(n: u64, exponent: f64) -> LUT`
Init-time distribution LUT builders. These construct precomputed
inverse CDF tables at assembly time.

**When to use:** With the init/cycle split in `.gk` files:
```gk
init lut = dist_normal(mean: 72.0, stddev: 5.0)
value := lut_sample(unit_interval(hash(cycle)), lut)
```

---

### Interpolation

#### `lerp(t: f64, a: f64, b: f64) -> f64`
Linear interpolation: `a + t * (b - a)`. When t=0 → a, t=1 → b.

**When to use:** Map a unit interval to any float range:
`lerp(unit_interval(h), 10.0, 20.0)` maps [0,1) to [10,20).
Also used for blending between two values.

#### `scale_range(input: u64, min: f64, max: f64) -> f64`
Map a u64 linearly to [min, max). Combines unit_interval + lerp.

**When to use:** One-step conversion from integer to float range:
`scale_range(hash(cycle), 0.0, 100.0)` gives a uniform float in
[0, 100).

#### `quantize(input: f64, step: f64) -> f64`
Round to the nearest multiple of step.

**When to use:** Snap continuous values to a grid:
`quantize(temp, 0.5)` rounds temperature to the nearest 0.5 degree.

---

### Datetime

#### `epoch_scale(input: u64, factor: u64) -> u64`
Multiply by a scale factor. Equivalent to `mul` but semantically
indicates timestamp scaling.

**When to use:** Convert a counter to millisecond offsets:
`epoch_scale(reading_idx, 1000)` turns reading index into ms.

#### `epoch_offset(input: u64, base: u64) -> u64`
Add a base epoch offset. Equivalent to `add` but semantically
indicates timestamp base.

**When to use:** Shift relative offsets to absolute timestamps:
`epoch_offset(offset, 1700000000000)` adds a base epoch.

#### `to_timestamp(input: u64) -> String`
Format epoch milliseconds as an ISO-8601 timestamp string.

**When to use:** Generate human-readable timestamps for logging
or database insertion.

#### `date_components(input: u64) -> (year, month, day, hour, min, sec, ms)`
Decompose epoch milliseconds into calendar components.

**When to use:** When you need individual date parts — partition
keys by month, group by hour, etc.

---

### Encoding

#### `html_encode(input: String) -> String` / `html_decode`
HTML entity encoding/decoding. Escapes `<`, `>`, `&`, `"`, `'`.

#### `url_encode(input: String) -> String` / `url_decode`
URL percent-encoding/decoding per RFC 3986.

#### `to_hex(input: Bytes) -> String` / `from_hex`
Hex string encoding/decoding.

#### `to_base64(input: Bytes) -> String` / `from_base64`
Base64 encoding/decoding.

#### `escape_json(input: String) -> String`
Escape a string for safe embedding in JSON (backslash escapes).

**When to use:** When constructing JSON strings that contain
user-generated content.

---

### String Generation

#### `combinations(input: u64, pattern: String, length: u64) -> String`
Mixed-radix character set mapping. Generates a string of `length`
characters from the given character set pattern.

**When to use:** Generate codes, identifiers, license plates:
`combinations(h, '0-9A-Z', 8)` gives 8-character alphanumeric codes.

Pattern syntax: `'0-9'` (digits), `'A-Z'` (uppercase), `'a-z'`
(lowercase), `'A-Za-z0-9'` (all alphanumeric). Ranges and literals
can be mixed.

#### `number_to_words(input: u64) -> String`
Spell out a number in English words. 42 → "forty-two".

**When to use:** Generate human-readable labels or test data that
needs natural language numbers.

---

### Weighted Selection

#### `weighted_strings(input: u64, spec: String) -> String`
Select a string from a weighted specification.

**When to use:** Generate categorical data with realistic
distributions: `weighted_strings(h, "GET:60,POST:25,PUT:10,DELETE:5")`

#### `weighted_u64(input: u64, spec: String) -> u64`
Select an integer from a weighted specification.

**When to use:** Generate numeric categories with controlled
frequencies.

---

### Permutation

#### `shuffle(input: u64, size: u64) -> u64`
Bijective LFSR permutation over [0, size). Every input maps to a
unique output — no collisions.

**When to use:** When you need unique values without gaps. Unlike
`mod(hash(x), N)` which can have collisions, shuffle guarantees
each input in [0, size) maps to a different output in [0, size).
Use for primary keys, unique IDs, and visit-every-element patterns.

**JIT level:** P3 (extern call)

---

### Noise

#### `perlin_1d(input: f64, seed: u64, frequency: f64) -> f64`
1D Perlin noise. Returns values in approximately [-1, 1].

**When to use:** Generate smooth, spatially correlated randomness.
Adjacent inputs produce similar (but not identical) outputs. Good
for terrain, gradients, and natural-looking variation.

#### `perlin_2d(x: f64, y: f64, seed: u64, frequency: f64) -> f64`
2D Perlin noise.

**When to use:** Generate 2D terrain, texture coordinates, or any
data that needs spatial correlation in two dimensions.

#### `simplex_2d(x: f64, y: f64, seed: u64, frequency: f64) -> f64`
2D simplex noise. Similar to Perlin but with fewer directional
artifacts and better performance in higher dimensions.

---

### Digest

#### `sha256(input: Bytes) -> Bytes`
SHA-256 cryptographic hash digest.

#### `md5(input: Bytes) -> Bytes`
MD5 hash digest.

**When to use:** Generate deterministic fingerprints or content
hashes. For workload generation (not security), use `hash` instead
— it's much faster.

---

### Byte Buffers

#### `u64_to_bytes(input: u64) -> Bytes`
Convert a u64 to its 8-byte little-endian representation.

#### `bytes_from_hash(input: u64, size: u64) -> Bytes`
Generate N deterministic bytes from a hash chain.

**When to use:** Generate binary payloads of a specific size for
blob/binary column testing.

---

### JSON

#### `to_json(input: Value) -> JSON`
Promote any value to a JSON value.

#### `json_to_str(input: JSON) -> String`
Serialize a JSON value to a compact string.

#### `json_merge(a: JSON, b: JSON) -> JSON`
Shallow merge two JSON objects. Keys in `b` override keys in `a`.

---

### Real-World Data

#### `first_names(input: u64) -> String`
US Census weighted first name selection.

#### `full_names(input: u64) -> String`
Full name (first + last) from Census data.

#### `state_codes(input: u64) -> String`
US state abbreviation (e.g., "CA", "NY").

#### `country_names(input: u64) -> String`
Country name from geographic data.

**When to use:** Generate realistic-looking personal and geographic
data for demos, testing, and workload prototyping.

---

### Context (Non-Deterministic)

#### `current_epoch_millis() -> u64`
Current wall-clock time in milliseconds since Unix epoch.

#### `counter() -> u64`
Monotonically increasing counter (not cycle-based).

#### `session_start_millis() -> u64`
Timestamp frozen at session init. Same value for all cycles.

**When to use:** When you need real-time values rather than
deterministic generation. These break reproducibility — use
sparingly and only when the use case requires actual wall-clock
time.

---

### Diagnostic

#### `type_of(input: Value) -> String`
Emit the type name of the input value ("u64", "f64", "String", etc.).

#### `debug_repr(input: Value) -> String`
Emit the Rust Debug representation of the value.

#### `inspect(input: u64) -> u64`
Passthrough that logs the value to stderr. Does not modify the
value.

**When to use:** Debugging GK graph wiring. Insert `inspect` into
a chain to see intermediate values without changing the graph
structure.

---

## Standard Library Modules

All stdlib modules use formal typed signatures and are bundled in
the nbrs binary. They are implicitly available — just reference
them by name.

### Hashing (`stdlib/hashing.gk`)

#### `hash_range(input: u64, max: u64) -> (value: u64)`
Hash and bound to [0, max). Equivalent to `mod(hash(input), max)`.

**When to use:** The most common pattern — one-step bounded hash.
Use instead of writing `mod(hash(...), N)` everywhere.

#### `hash_interval(input: u64, min: f64, max: f64) -> (value: f64)`
Hash to a uniform float in [min, max).

**When to use:** One-step float generation with specified range.

#### `hash_range_offset(input: u64, base: u64, range: u64) -> (value: u64)`
Hash, bound to [0, range), and add base offset. Result in
[base, base+range).

**When to use:** Generate IDs in an offset range:
`hash_range_offset(cycle, base: 10000, range: 5000)` gives
[10000, 15000).

---

### Distributions (`stdlib/distributions.gk`)

#### `normal_sample(input: u64, mean: f64, stddev: f64) -> (value: f64)`
Sample from a normal distribution. All-in-one: hash → unit_interval
→ icd_normal.

**When to use:** When you want a gaussian value from a u64 input
in one call. Equivalent to Java nosqlbench's `Normal(mean, stddev, 'hash')`.

#### `exponential_sample(input: u64, rate: f64) -> (value: f64)`
Sample from an exponential distribution.

#### `uniform_sample(input: u64, min: f64, max: f64) -> (value: f64)`
Continuous uniform in [min, max).

#### `zipf_sample(input: u64, n: u64, exponent: f64) -> (value: u64)`
Zipf distribution sample, returns integer in [1, n].

**When to use:** Model rank-frequency distributions: web page
popularity, word frequencies, database access patterns. Higher
exponent → more skewed toward rank 1.

#### `bounded_normal_int(input: u64, mean: f64, stddev: f64, min: f64, max: f64) -> (value: u64)`
Normal sample clamped to [min, max] and rounded to integer.

**When to use:** Generate realistic bounded counts: ages, quantities,
scores. `bounded_normal_int(h, mean: 35.0, stddev: 10.0, min: 18.0, max: 80.0)`
for ages.

---

### Identity (`stdlib/identity.gk`)

#### `hashed_id(input: u64, bound: u64) -> (id: u64)`
Deterministic bounded ID. Same as `hash_range` but with semantic
naming for ID generation contexts.

#### `shuffled_id(input: u64, size: u64) -> (value: u64)`
Bijective permutation. Wraps the native `shuffle` node.

**When to use:** When you need collision-free ID generation:
every input in [0, size) maps to a different output.

#### `euler_circuit(position: u64, range: u64, seed: u64, stream: u64) -> (value: u64)`
Bijective permutation via PCG cycle-walking.

**Status:** Stub — falls back to hash+mod until the native
`cycle_walk` node is implemented.

---

### Strings (`stdlib/strings.gk`)

#### `alpha_numeric(input: u64, length: u64) -> (value: String)`
Fixed-length alphanumeric string (digits + letters).

**When to use:** Generate usernames, codes, session IDs:
`alpha_numeric(input: h, length: 12)` → "k7Bm9xPq2nR4"

#### `padded_id(input: u64, bound: u64, width: u64) -> (value: String)`
Zero-padded numeric ID string.

**When to use:** Fixed-width numeric codes for databases or display:
`padded_id(input: h, bound: 1000000, width: 8)` → "00527897"

#### `hex_id(input: u64, bound: u64) -> (value: String)`
Hexadecimal ID string.

---

### Time Series (`stdlib/timeseries.gk`)

#### `monotonic_ts(input: u64, base_epoch: u64, interval_ms: u64) -> (ts: u64)`
Monotonically increasing timestamp.

**When to use:** Generate evenly-spaced time series data:
`monotonic_ts(input: reading_idx, base_epoch: 1710000000000, interval_ms: 1000)`
gives one-second-spaced timestamps.

#### `jittered_ts(input: u64, base_epoch: u64, interval_ms: u64, jitter_ms: u64) -> (ts: u64)`
Monotonic timestamp with per-point random jitter.

**When to use:** More realistic time series where measurements
don't arrive at exact intervals. Jitter is deterministic (hashed
from input).

---

### Latency Models (`stdlib/latency.gk`)

#### `gaussian_latency(input: u64, mean: f64, stddev: f64) -> (latency_ms: f64)`
Normally-distributed latency in milliseconds, clamped to [0.1, 999999].

**When to use:** With the model adapter's `result-latency` field
for simulating realistic service response times.

#### `exponential_latency(input: u64, rate: f64) -> (latency_ms: f64)`
Exponentially-distributed latency.

**When to use:** Model queuing-theory-style wait times where most
requests are fast but some are very slow.

---

### Waves (`stdlib/waves.gk`)

#### `sawtooth(input: u64, period: u64) -> (value: f64)`
Sawtooth wave: ramps from 0.0 to 1.0 over `period` cycles, then
resets.

**When to use:** Generate periodic patterns — load ramps, cyclic
workload intensity, phased test stages.

---

### Modeling (`stdlib/modeling.gk`)

#### `service_latency(input: u64, mean_ms: f64, stddev_ms: f64) -> (latency_ms: f64)`
Single-mode service latency with gaussian profile.

**When to use:** Quick latency model for the model adapter. For
bimodal or conditional latency, compose your own using native
probability nodes (when available).

**Status:** Full bimodal models require native `select`/`unfair_coin`
nodes from SRD 29/30, not yet implemented.
