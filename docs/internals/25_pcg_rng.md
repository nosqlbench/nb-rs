# SRD 25 — PCG RNG Nodes

## Motivation

The current GK function library relies on hash functions (xxHash3) for
deterministic value generation. Hash is excellent for independent
cycle→value mappings, but it cannot produce **correlated sequences** —
a stream of values where consecutive positions come from the same
generator state. This matters for workloads that model:

- Sensor readings over time (device D's temperature at reading N)
- User sessions (user U's action sequence at step K)
- Tabular data (row R, column C → cell value)
- Vector embeddings (dimension D of vector V)

Hash gives you `f(cycle)` — a single isolated value. PCG gives you
`f(stream, position)` — a seekable, reproducible sequence per stream.

## Why PCG

PCG (Permuted Congruential Generator) is chosen over other RNG families:

| Property | Hash (xxH3) | PCG-RXS-M-XS | Xoshiro | MT |
|----------|-------------|---------------|---------|-----|
| Deterministic | ✓ | ✓ | ✓ | ✓ |
| Stateless per call | ✓ | ✓ (via seek) | ✗ | ✗ |
| Independent streams | N/A | ✓ (inc param) | ✗ | ✗ |
| Seekable | N/A | O(log N) | ✗ | ✗ |
| Output quality | excellent | excellent | good | good |
| Speed (single value) | ~2ns | ~2ns step | ~1ns | ~4ns |
| JIT-able (u64 only) | ✓ (extern) | ✓ (all u64 arith) | ✓ | ✗ |

Key advantages over the Java nosqlbench approach (which has no RNG
support — only Murmur3F hashing and LFSR shuffling):

- **Stream independence**: `inc = 2*stream + 1` gives 2^63 independent
  full-period sequences from a single seed.
- **Seekability**: Jump to position N in O(log N) without computing
  all intermediate states. Enables pure-function deterministic flow.
- **Pure-function model**: `pcg(seed, stream, position) → u64` is a
  pure function with no mutable state. The cycle coordinate IS the
  position.

## Algorithm: PCG-RXS-M-XS 64/64

We use the 64-bit state → 64-bit output variant (PCG-RXS-M-XS). This
keeps everything in u64 space, making it a natural P3/JIT target.

```
Constants:
  MULT = 6364136223846793005u64

State transition (LCG):
  state' = state * MULT + inc
  where inc = 2 * stream + 1  (must be odd)

Output permutation (RXS-M-XS):
  word = ((state >> ((state >> 59) + 5)) ^ state) * 12605985483714917081
  output = (word >> 43) ^ word

Seek to position N from initial state s0:
  s_N = acc_mult * s0 + acc_plus
  where (acc_mult, acc_plus) are computed via O(log N) repeated squaring
  of the LCG recurrence.
```

The seek algorithm (64 iterations worst case for 64-bit position):

```rust
fn pcg_seek(seed: u64, inc: u64, position: u64) -> u64 {
    let mut cur_mult: u64 = MULT;
    let mut cur_plus: u64 = inc;
    let mut acc_mult: u64 = 1;
    let mut acc_plus: u64 = 0;
    let mut delta = position;

    while delta > 0 {
        if delta & 1 != 0 {
            acc_mult = acc_mult.wrapping_mul(cur_mult);
            acc_plus = acc_plus.wrapping_mul(cur_mult).wrapping_add(cur_plus);
        }
        cur_plus = cur_mult.wrapping_add(1).wrapping_mul(cur_plus);
        cur_mult = cur_mult.wrapping_mul(cur_mult);
        delta >>= 1;
    }

    let state = acc_mult.wrapping_mul(seed).wrapping_add(acc_plus);
    pcg_output(state)
}
```

All operations are wrapping u64 arithmetic — no 128-bit math needed.

## Node Signatures

### `pcg(seed, stream)` — The Workhorse

```
Inputs:  1 wire (position: u64)
Outputs: 1 (value: u64)
Params:  seed: u64 (init), stream: u64 (init)
```

The most common variant. Seed and stream are init-time constants,
position comes from the cycle coordinate (or any upstream node).
Each (seed, stream) pair defines an independent full-period sequence.

```yaml
bindings:
  col_a: Pcg(seed=42, stream=0)        # stream 0 at cycle position
  col_b: Pcg(seed=42, stream=1)        # independent stream
  col_c: Pcg(seed=42, stream=2)        # independent stream
```

### `pcg_stream(seed)` — Dynamic Stream Selection

```
Inputs:  2 wires (position: u64, stream_id: u64)
Outputs: 1 (value: u64)
Params:  seed: u64 (init)
```

Stream ID comes from a wire input, not a constant. This is the key
node for entity-correlated generation: the entity ID selects the
stream, so each entity gets a deterministic independent sequence.

```yaml
bindings:
  device_id := MixedRadix(10000, 0).d1
  reading_idx := MixedRadix(10000, 0).d0
  temperature: PcgStream(seed=42)(reading_idx, device_id)
```

### `pcg_dyn` — Fully Dynamic

```
Inputs:  3 wires (position: u64, seed: u64, stream_id: u64)
Outputs: 1 (value: u64)
Params:  none
```

Everything from wire inputs. Rare, but enables meta-generation where
the seed itself is computed from upstream nodes.

### `pcg_n(seed, stream, count)` — Multi-Output

```
Inputs:  1 wire (position: u64)
Outputs: N (value_0..value_N: u64)
Params:  seed: u64, stream: u64, count: usize (all init)
```

Returns N consecutive values from the stream starting at position.
The seek targets `position * count`, then steps N times. Useful for
generating N fields per entity from one stream.

## Practical Examples

### Example 1: IoT Sensor Time Series

Generate readings for 10,000 devices, each with an independent
temperature sequence that's deterministic and reproducible.

```yaml
params:
  num_devices: 10000

bindings:
  # Decompose cycle into (reading_within_device, device_id)
  device_id    := MixedRadix(<<num_devices>>, 0).d1
  reading_idx  := MixedRadix(<<num_devices>>, 0).d0

  # Stable device metadata (same for all readings from this device)
  device_name  : Hash(device_id); Combinations('sensor-AAA-000')
  location     : Hash(device_id); Mod(50)   # 50 locations

  # Sensor readings: each device gets its own PCG stream
  # reading_idx is the position — consecutive readings are correlated
  temperature  : PcgStream(seed=100)(reading_idx, device_id); UnitInterval(); Lerp(15.0, 45.0)
  humidity     : PcgStream(seed=101)(reading_idx, device_id); UnitInterval(); Lerp(20.0, 95.0)
  pressure     : PcgStream(seed=102)(reading_idx, device_id); UnitInterval(); Lerp(980.0, 1040.0)

  # Timestamp: monotonic per device
  ts           : EpochOffset(1700000000000); Mul(reading_idx, 1000)
```

**Why PCG over Hash here:** Device 42's temperature at reading 0, 1,
2, ... comes from a single PCG stream. If you replay cycles
0..10000 vs 10000..20000, you get the exact same readings for every
device. With hash, you'd get statistically independent values at each
reading — no way to produce a "sequence" for a device.

### Example 2: User Session Events

Model users generating sequences of page views, where each user's
browsing pattern is an independent reproducible stream.

```yaml
params:
  pages_per_session: 50
  num_users: 100000

bindings:
  user_id   := MixedRadix(<<pages_per_session>>, 0).d1
  page_idx  := MixedRadix(<<pages_per_session>>, 0).d0

  # User profile (stable, derived from user_id via hash)
  username  : Hash(user_id); Combinations('user_AAAA')

  # Session events (sequential, derived from PCG stream per user)
  page_id   : PcgStream(seed=10)(page_idx, user_id); Mod(100000)
  dwell_ms  : PcgStream(seed=11)(page_idx, user_id); Mod(30000); Add(500)
  action    : PcgStream(seed=12)(page_idx, user_id); Mod(5)
```

**Key insight:** Hash gives user_id → username (stable identity).
PCG gives (user_id, page_idx) → page sequence (temporal behavior).
These are complementary tools.

### Example 3: Vector Embedding Generation

Generate deterministic high-dimensional vectors where each vector is
a reproducible sequence of floats.

```yaml
params:
  dimensions: 768
  num_vectors: 1000000

bindings:
  vector_id := MixedRadix(<<dimensions>>, 0).d1
  dim_idx   := MixedRadix(<<dimensions>>, 0).d0

  # Each vector is a PCG stream; each dimension is a position
  component : PcgStream(seed=42)(dim_idx, vector_id); UnitInterval(); Lerp(-1.0, 1.0)
```

This generates 1M vectors × 768 dimensions. Vector 42's dimension 0
is always the same value. You can regenerate any single vector by
running cycles `42*768 .. 42*768+768`.

### Example 4: Tabular Data with Column Independence

Generate a table where each column is independent but each row is
reproducible.

```yaml
params:
  num_rows: 10000000

bindings:
  row_id : Identity()

  # Each column: same seed, different stream constant
  name   : Pcg(seed=1, stream=0); Combinations('AAAA AAAA')
  age    : Pcg(seed=1, stream=1); Mod(80); Add(18)
  salary : Pcg(seed=1, stream=2); Mod(150000); Add(30000)
  dept   : Pcg(seed=1, stream=3); Mod(12)
  active : Pcg(seed=1, stream=4); Mod(2)
```

Here `Pcg(seed=1, stream=N)` at position=cycle gives independent
columns. Row 42 always gets the same name, age, salary, etc.

**Why not just Hash?** You could do `Hash(); Mod(80); Add(18)` for
age. The difference: with PCG streams, columns 0-4 are provably
independent (different LCG increments), while with hash you rely on
the hash function's statistical properties to avoid correlation.
For most workloads hash is fine, but PCG gives a formal guarantee.

## Compilation Levels

### Phase 1 (Value interpreter)

Standard `GkNode` trait implementation. The `eval()` method calls the
seek function directly. ~120ns per call (dominated by the O(log N)
seek loop).

### Phase 2 (Compiled closure)

The `compiled_u64()` closure captures seed and inc as constants.
Same seek algorithm, but no `Value` enum overhead. ~100ns.

### Phase 3 (JIT)

**Approach: extern call.** The seek loop has data-dependent branching
(the `while delta > 0` loop), making it awkward to inline as
Cranelift IR. Instead, emit an extern call to a precompiled seek
function — same pattern as hash and shuffle.

```rust
extern "C" fn jit_pcg(position: u64, seed: u64, inc: u64) -> u64;
extern "C" fn jit_pcg_stream(position: u64, stream_id: u64, seed: u64) -> u64;
```

JIT overhead is ~1-2ns for the call. The seek itself is ~100ns.
Total ~102ns — dominated by the seek, not the dispatch.

**JitOp variants:**

```rust
/// Pcg with init-time seed/stream: extern call
PcgConst(u64, u64),       // seed, inc (= 2*stream + 1)

/// PcgStream with init-time seed, wire stream_id
PcgStreamConst(u64),      // seed

/// PcgDyn: all from wire
PcgDyn,
```

**Future optimization — memoized sequential access:**

When the system detects that a PCG node's position input is the raw
cycle coordinate (monotonically incrementing), it can switch to a
single-step LCG update instead of a full seek. This is a runtime
optimization that preserves the pure-function semantic contract:

```
if position == last_position + 1:
    state = state * MULT + inc       // O(1) step
else:
    state = seek(seed, inc, position) // O(log N) seek
```

This drops per-call cost from ~100ns to ~2ns for sequential access.
The pure-function contract is preserved: same (seed, stream, position)
always returns the same value regardless of access pattern.

## Init-Time vs Wire-Time Parameters

| Parameter | `pcg` | `pcg_stream` | `pcg_dyn` |
|-----------|-------|-------------|-----------|
| seed | init (const param) | init (const param) | wire input |
| stream | init (const param) | wire input | wire input |
| position | wire input | wire input | wire input |

Init-time parameters are baked into the node at assembly and captured
in the JIT constants / P2 closure. Wire-time parameters flow through
the DAG at cycle time.

The `inc` value is always derived as `2 * stream + 1` (ensuring it's
odd, which is required for full-period LCG). For init-time streams,
this computation happens once at assembly. For wire-time streams, it
happens per evaluation.

## Relationship to Existing Nodes

PCG complements, not replaces, the existing function library:

| Use case | Best tool |
|----------|-----------|
| Single deterministic value from cycle | `Hash()` |
| Bounded integer from cycle | `Hash(); Mod(N)` |
| Bijective permutation | `Shuffle(size)` |
| Independent sequence per entity | **`PcgStream(seed)(pos, entity_id)`** |
| N columns of independent data | **`Pcg(seed, stream=0..N)`** |
| Seekable reproducible stream | **`Pcg(seed, stream)(position)`** |
| High-dimensional vectors | **`PcgStream(seed)(dim, vector_id)`** |

## Test Plan

1. **Determinism**: Same (seed, stream, position) always returns same value
2. **Stream independence**: Different streams produce uncorrelated sequences
3. **Seek correctness**: seek(s, inc, N) == step(step(...step(s, inc)...), inc) N times
4. **Full period**: PCG with 64-bit state has period 2^64 — verify no short cycles
5. **Statistical quality**: Output passes basic chi-squared and serial correlation tests
6. **JIT parity**: JIT extern call produces identical results to Phase 1 eval
7. **Wire vs init equivalence**: `PcgStream(seed=42)(pos, 7)` == `Pcg(seed=42, stream=7)(pos)`
