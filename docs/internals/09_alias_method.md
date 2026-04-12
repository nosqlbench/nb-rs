# Alias Method — Design Sketch

O(1) sampling from discrete probability distributions using Vose's
alias method. This is the core mechanism behind weighted lookup, CSV
frequency sampling, and all discrete distribution nodes in the GK.

---

## What It Does

Given a set of N outcomes with associated weights, the alias method
pre-computes a table that allows O(1) sampling. A single uniform
random value (a u64 in the GK) selects an outcome in constant time
regardless of N.

## Algorithm (Vose's Alias Method)

**Construction (O(N)):**

1. Normalize weights so they sum to N (the number of outcomes).
2. Partition outcomes into two queues: `small` (weight < 1.0) and
   `large` (weight >= 1.0).
3. While both queues are non-empty:
   - Pop one from each: `s` from small, `l` from large.
   - Create a table slot: primary = s, alias = l, bias = s.weight.
   - Adjust: l.weight -= (1.0 - s.weight).
   - Re-queue l into the appropriate queue.
4. Any remaining items become their own alias with bias = 1.0.

**Sampling (O(1)):**

1. Convert input u64 to a slot index and a fractional position within
   the slot.
2. If the fraction is below the bias → return the primary outcome.
3. Otherwise → return the alias outcome.

## Java nosqlbench Implementation

Three classes:
- `AliasSamplerDoubleInt` — ByteBuffer-backed, int outcomes
- `AliasSamplerDoubleLong` — ByteBuffer-backed, long outcomes
- `AliasElementSampler<T>` — generic, array-backed

Performance: ~70M+ samples/sec single-threaded, independent of N.
Memory: 16-24 bytes per outcome depending on output type.

Used by: `CSVFrequencySampler`, `WeightedStringsFromCSV`,
`WeightedInts`, `DelimFrequencySampler`, and indirectly by all
discrete distribution curve classes.

## nb-rs Design

### Simplification

Java had three separate classes for different output types. In Rust,
we use a single generic struct with the outcome type as a parameter,
plus a specialized u64-only variant for GK compiled mode.

### Struct: `AliasTable<T>`

```rust
pub struct AliasTable<T> {
    /// Per-slot: (bias, primary_outcome, alias_outcome)
    slots: Vec<AliasSlot<T>>,
}

struct AliasSlot<T> {
    bias: f64,
    primary: T,
    alias: T,
}
```

For the common case of u64 outcomes, `AliasTable<u64>` stores
everything inline — no heap indirection per slot. For string outcomes,
`AliasTable<String>` works naturally.

### Specialized: `AliasTableU64`

A flat-buffer variant optimized for the Phase 2 compiled kernel path.
Three parallel arrays for cache-friendly access:

```rust
pub struct AliasTableU64 {
    biases: Vec<f64>,
    primaries: Vec<u64>,
    aliases: Vec<u64>,
}
```

### GK Node: `AliasSample`

A GK node that wraps an `AliasTable` and samples from it.

Signature: `(input: u64) -> (u64)`

The input u64 is treated as a uniform value (the user is responsible
for hashing upstream, per the explicit hashing provenance principle).
The node decomposes it into a slot index and fractional bias test.

### Construction from Data

```rust
AliasTable::from_weights(outcomes: &[T], weights: &[f64]) -> AliasTable<T>
AliasTable::uniform(outcomes: &[T]) -> AliasTable<T>
AliasTableU64::from_weights(weights: &[f64]) -> AliasTableU64
```

For the u64 variant, outcomes are implicitly 0..N (index-based).

### Integration Points

- **WeightedLookup node**: uses `AliasTable<String>` backed by CSV
  data. The GK node hashes → alias samples → returns the string.
- **Discrete distributions** (Zipf, Poisson, Binomial, etc.): the
  distribution's PMF is pre-computed over a range and loaded into an
  `AliasTableU64`. Sampling is then O(1).
- **Inline weighted selection**: `WeightedStrings('a:0.3;b:0.7')`
  parses into an `AliasTable<String>` at assembly time.
