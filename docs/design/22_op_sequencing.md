# Op Sequencing and Stanza Design

How operations are arranged, sequenced, and dispatched within an
activity. This covers the pipeline from workload YAML to runtime
cycle execution.

---

## nosqlbench Pipeline (for reference)

```
YAML → tag filter → OpTemplate → OpMapper → OpDispenser
                                              ↓
                          SequencePlanner (ratios + strategy)
                                              ↓
                          OpSequence LUT: cycle % len → dispenser
                                              ↓
                          StrideMotor: batch of cycles
                                              ↓
                          StandardAction: per-cycle dispatch
```

Key concepts carried forward:
- **Ratio-based sequencing** — each op has a ratio; the LUT repeats
  ops according to their ratios
- **Pre-computed LUT** — cycle → op mapping is a simple array index
- **Stride** — a batch of cycles processed together

---

## nb-rs Pipeline

```
YAML → tag filter → ParsedOp → Adapter build_op() → AssembledOp
                                     ↓
                    OpSequence LUT: cycle % len → op template
                                     ↓
                    Stanza (group of cycles, replaces "stride")
                                     ↓
                    Async executor tasks: per-cycle dispatch
```

### What Changes from nosqlbench

1. **No OpDispenser/OpMapper indirection.** In nosqlbench, the
   adapter pre-bakes a `cycle → CycleOp` function at init time via
   OpMapper → OpDispenser. In nb-rs, the adapter receives an
   `AssembledOp` (already resolved from variates) and executes it
   directly. The variate resolution + op assembly happens in the
   executor loop, not pre-baked.

2. **"Stanza" instead of "stride."** A stanza is a group of cycles
   that forms one complete rotation through the op sequence. The
   stanza length equals the sum of all ratios. This is the natural
   unit of work — after one stanza, every op has been executed its
   ratio-proportional number of times.

3. **Async tasks instead of Motor threads.** No StrideMotor. Each
   async task pulls cycles from the CycleSource and processes them.
   A stanza is a logical concept (sequence length), not a thread
   boundary.

---

## Stanza

A **stanza** is one complete pass through the op sequence LUT.

```
Ops:   read(ratio=3)  write(ratio=2)  delete(ratio=1)
LUT:   [read, write, delete, read, write, read]
        ──────────── one stanza ────────────
Len:   6 (sum of ratios)
```

After one stanza (6 cycles), read has executed 3 times, write 2
times, delete 1 time — exactly matching the ratios.

### Why This Matters

- **Rate limiting can operate per-stanza** or per-cycle. A stanza
  rate limit of 100/s with a stanza of length 6 means 600 ops/s
  total (100 stanzas × 6 ops each).
- **The stanza length is the natural stride for metrics reporting.**
  After each stanza, the op mix is proportionally correct.
- **Cycles should ideally be a multiple of the stanza length** for
  clean proportionality. If not, the last partial stanza is fine —
  it just won't have perfect ratio balance.

---

## Sequencing Strategies

Three strategies for arranging ops within a stanza:

### Bucket (default)

Round-robin from ratio-sized buckets. Each pass draws one from each
non-empty bucket, cycling until all buckets are empty.

```
read:3, write:2, delete:1

Pass 1: read, write, delete  (draw one from each)
Pass 2: read, write          (delete exhausted)
Pass 3: read                 (write exhausted)

LUT: [R, W, D, R, W, R]
```

Good general-purpose interleaving.

### Interval

Evenly spaced across the stanza based on frequency. Each op is
scheduled at fractional positions within [0, stanza_length).

```
read:3, write:2, delete:1

read at positions:   0, 2, 4    (every 2)
write at positions:  0, 3       (every 3)
delete at position:  0          (every 6)

Sorted: R, W, D, R, W, R
LUT: [R, W, D, R, W, R]
```

Fair interleaving that distributes ops evenly over time.

### Concat

All of one op, then all of the next, etc.

```
read:3, write:2, delete:1

LUT: [R, R, R, W, W, D]
```

Good for sequential phases (schema setup, then data loading, then
queries).

---

## Activity Configuration Trait

```rust
/// How an activity is configured with its workload ops.
pub trait ActivitySetup {
    /// Load and filter op templates from the workload.
    fn load_ops(
        &self,
        workload: &Workload,
        tag_filter: Option<&str>,
    ) -> Vec<ParsedOp>;

    /// Build the op sequence from filtered ops.
    fn build_sequence(
        &self,
        ops: Vec<ParsedOp>,
        strategy: SequencerType,
    ) -> OpSequence;

    /// Build the GK kernel from the workload's bindings.
    fn build_kernel(
        &self,
        workload: &Workload,
    ) -> GkKernel;
}
```

### SequencerType

```rust
pub enum SequencerType {
    /// Round-robin from ratio buckets (default).
    Bucket,
    /// Evenly spaced by frequency.
    Interval,
    /// All of first, then all of second, etc.
    Concat,
}
```

### Ratio Extraction

The `ratio` field on an op template defaults to 1:

```yaml
ops:
  read:
    ratio: 3
    stmt: "SELECT ..."
  write:
    ratio: 1
    stmt: "INSERT ..."
```

Extracted from `ParsedOp.params["ratio"]` during sequence building.

---

## Runtime: Cycle → Op → Execute

At runtime, the executor task does:

```rust
let cycle = cycle_source.next();
let op_template = op_sequence.get(cycle);  // LUT lookup
let assembled_op = build_op(cycle, op_template);  // variates → fields
let result = adapter.execute(&assembled_op).await;
```

The `build_op` closure:
1. Sets the GK kernel coordinate to the cycle
2. Pulls output variates referenced by the op template's bind points
3. Substitutes bind points with concrete values
4. Returns an `AssembledOp` with all fields resolved

This happens per-cycle, not pre-baked. The GK kernel is the "fast
pre-baking" — it compiles the variate generation at init time, and
the per-cycle cost is just pulling values from the compiled kernel.

---

## Resolved Questions

- **Q36:** Stanza length = sum of ratios. Always. Dynamic scheduling
  is a future enhancement.
- **Q37:** Both stanza-rate and cycle-rate limiting supported.
  `rate=` controls per-cycle. `stanzarate=` controls per-stanza.
  Both optional, both use nb-rate.
- **Q38:** Sequencer type is configurable per-activity via `seq=`
  parameter. Values: `bucket` (default), `interval`, `concat`.
