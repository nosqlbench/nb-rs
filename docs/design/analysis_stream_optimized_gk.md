# Stream-Optimized GK: Analysis

## The Core Idea

Today's GK evaluates **one coordinate tuple at a time**. The proposed mode flips this: given that cycles are strictly monotonic, process a **batch of N coordinates simultaneously**, restructuring the DAG from scalar operations into array operations that expose SIMD parallelism and amortize per-eval overhead.

This is essentially the **SoA (Structure-of-Arrays) transformation** applied to GK evaluation, plus pattern-level fusion.

---

## Layer 1: Coordinate Transform is Known + Monotonic Cycles

When the coordinate transform from `cycle → coords` is known at assembly time and cycles are strictly monotonic, the system knows something powerful: **the input sequence is predictable.** This enables:

**Pre-buffering coordinates.** Instead of calling `set_coordinates(cycle)` one at a time, you can fill a coordinate column:

```
coords_col[0..N] = [transform(cycle), transform(cycle+1), ..., transform(cycle+N-1)]
```

For common transforms (identity, mixed_radix decomposition), this is itself vectorizable. Identity is just an iota fill. MixedRadix on a monotonic sequence has patterns — the low-order radix digits cycle predictably, higher digits change infrequently.

**PCG sequential optimization generalizes.** SRD 24 already notes the single-step PCG memo. In batch mode, you can generate N consecutive PCG outputs in a tight loop — this is one of the best cases for auto-vectorization since each step is an independent wrapping multiply+add.

**Trade-off:** The cost is that you must commit to evaluating all N coordinates, even if an adapter only needs one result. But GK workloads almost always consume every generated value, so this is rarely wasted.

---

## Layer 2: Subgraph Fusion (Pattern Recognition)

This is the most intellectually interesting part. The idea: scan the DAG for subgraphs that match known algorithmic patterns, and replace them with fused implementations.

**Concrete candidates in GK's node vocabulary:**

| Pattern | Nodes | Fused Form |
|---------|-------|-----------|
| hash → mod | Hash64 + ModU64 | `hash_range(x, n)` — avoids full 64-bit hash when only low bits needed (debiased modular reduction) |
| hash → unit_interval → lerp | Hash64 + UnitInterval + Lerp | `hash_to_range_f64(x, lo, hi)` — single division + FMA |
| mixed_radix → [hash, hash, ...] | MixedRadix + N×Hash64 | Each decomposed digit feeds an independent hash — embarrassingly parallel, perfect SIMD lane assignment |
| unit_interval → quantize | UnitInterval + Quantize | `quantized_unit(x, step)` — single FP op chain |
| hash → lut_sample | Hash64 + LutSample | `direct_lut(hash(x))` — the alias method table is already a flat array; inlining the hash removes a buffer round-trip |
| iota → hash → mod | Identity + Hash64 + ModU64 | The most common GK subgraph: cycle → hash → bounded int. Entire thing is one function call. |

**But here's the critical question:** How much does fusion actually buy over Phase 3 JIT?

The Phase 3 JIT already eliminates inter-node overhead (no gather/scatter, no closure calls). Fusion wins only when the fused algorithm is **fundamentally cheaper** than the composition — not just fewer instructions, but a different algorithm:

- **hash → mod**: A debiased modular reduction (Lemire's method) avoids the full 64-bit hash when the output range is small. This is a genuine algorithmic win — ~2 instructions vs. a full xxh3 call.
- **mixed_radix decomposition on monotonic input**: Instead of N independent `urem/udiv` chains, you can increment a multi-digit counter. O(1) amortized vs. O(radix_count) per cycle. This is a huge win for high-radix decompositions.
- **hash → unit_interval → lerp**: FMA fusion. Marginal — maybe 1 instruction saved.

The honest assessment: **most fusions save single-digit nanoseconds over Phase 3 JIT.** The exceptions are the algorithmic transformations (monotonic mixed_radix, debiased mod reduction) where the fused form is a different algorithm entirely.

---

## Layer 3: Array Buffers and Demand Buffering

This is where SoA transformation meets caching economics.

**Current layout (AoS, scalar):**
```
buffer: [coord0, coord1, node0_out0, node0_out1, node1_out0, ...]
         ← one cycle's worth →
```

**Proposed layout (SoA, columnar):**
```
coord0_col:    [c0_cycle0, c0_cycle1, ..., c0_cycleN]
coord1_col:    [c1_cycle0, c1_cycle1, ..., c1_cycleN]
node0_out0_col:[n0o0_c0,   n0o0_c1,  ..., n0o0_cN  ]
...
```

Each "slot" becomes a column of N values. Node evaluation becomes a loop over columns.

**Demand buffering:** Not all columns need to be materialized simultaneously. A column is allocated when first written and can be freed after its last consumer has read it. For a DAG with depth D, only the columns on the current "wavefront" need to be live. This is analogous to register liveness analysis but for column buffers.

**The cache arithmetic:**

| Batch Size N | Column Size (u64) | Total Live Columns (typical 20-slot DAG) | Working Set |
|---|---|---|---|
| 64 | 512 B | ~8 (wavefront) | **4 KB** — fits L1 (32-64 KB) |
| 256 | 2 KB | ~8 | **16 KB** — fits L1 |
| 1024 | 8 KB | ~8 | **64 KB** — pushes L1 boundary |
| 4096 | 32 KB | ~8 | **256 KB** — L2 (256 KB-1 MB) |
| 16384 | 128 KB | ~8 | **1 MB** — pushes L2 boundary |

The sweet spot is **N = 256 to 1024**. Here's why:

- **N = 256 (2 KB columns):** Working set fits comfortably in L1. SIMD registers process 4 u64s (AVX2) or 8 u64s (AVX-512) per instruction, so 256 elements = 64 or 32 SIMD iterations per column. Enough to amortize loop overhead and keep the pipeline full.

- **N = 1024 (8 KB columns):** Still fits L1 if the wavefront is narrow. Better amortization of any per-batch setup (coordinate filling, PCG state prep). But if your DAG is wide (many live columns), you start spilling to L2.

- **N > 4096:** You're in L2 territory. The SIMD throughput gains start losing to cache miss penalties. And since GK evaluation is typically compute-bound (not memory-bound), there's no prefetch advantage to larger batches.

**Demand buffering is essential at larger N.** Without it, a 20-slot DAG at N=1024 would need 160 KB live — blowing L1. With wavefront-aware allocation, only ~8 columns are live at once: 64 KB, still in L1.

---

## Layer 4: SIMD Vectorization via Intrinsic Macros

This is where the rubber meets the road. The question: which GK operations actually benefit from explicit SIMD?

**Tier 1 — Excellent SIMD fit (4-8x speedup possible):**

| Operation | SIMD Approach | Lanes (AVX2) |
|-----------|--------------|--------------|
| Add/Mul/Div/Mod (const) | `vpaddq`, `vpmullq` (AVX-512), emulated for AVX2 | 4 |
| UnitInterval (u64→f64) | `vcvtuqq2pd` (AVX-512) or scalar fallback | 4-8 |
| Clamp | `vpminuq`/`vpmaxuq` | 4 |
| Lerp (f64) | `vfmadd` FMA chain | 4 |
| Bitwise ops | `vpand`/`vpor`/`vpxor` | 4 |
| Iota fill (coord gen) | Trivial: base + lane offset | 4 |

**Tier 2 — Moderate SIMD fit (2-3x speedup, needs care):**

| Operation | Challenge |
|-----------|-----------|
| Hash (xxh3) | Vectorized xxh3 exists but is complex; 4-way parallel hashing is feasible with restructured state |
| MixedRadix | Division is expensive in SIMD (no integer divide instruction in AVX2). Use magic-number multiply trick. |
| PCG step | Wrapping multiply is fine; the output permutation has bit rotates that need AVX-512 `vprorvq` or emulation |
| Shuffle (LFSR) | State-dependent, hard to parallelize across lanes. Better to parallelize across cycles. |

**Tier 3 — Poor SIMD fit (stick with scalar):**

| Operation | Why |
|-----------|-----|
| LutSample / AliasSample | Gather loads (`vpgatherqq`) — each lane indexes a different table position. Gather is slow on most hardware (~12 cycles vs. ~4 for contiguous load). |
| String operations | Variable-length, heap-allocated, not SIMD-friendly |
| Conditional branches | Lane divergence kills SIMD efficiency |

**The intrinsics model:** Rather than hand-coding SIMD for every node, define a library of **vectorized macro operations**:

```
simd_hash_col(src_col, dst_col, N)          // 4-way parallel xxh3
simd_mod_const_col(src_col, dst_col, m, N)  // magic-number multiply
simd_lerp_col(src_col, dst_col, lo, hi, N)  // FMA chain
simd_iota_fill(dst_col, base, step, N)      // coord generation
```

The JIT (or a new Phase 4 compiler) selects the appropriate intrinsic per node, falling back to scalar loops for Tier 3 nodes. This is exactly how NumPy/Polars work — column-at-a-time with prebuilt vectorized kernels.

---

## The Net Speedup Calculation

Let's work a concrete example. Consider the most common GK pattern:

```
cycle → mixed_radix(26,26,10000) → [hash→mod(100), hash→mod(50), hash→unit→lerp(0,1)]
```

**Current Phase 3 JIT (scalar, per-cycle):**
- MixedRadix: ~5 ns (3 divisions)
- Hash×3: ~15 ns (3 xxh3 calls)
- Mod×2 + UnitInterval + Lerp: ~2 ns
- **Total: ~22 ns/cycle**

**Stream-optimized (N=256, SIMD):**
- MixedRadix: Monotonic counter mode — O(1) amortized, ~0.5 ns/cycle
- Hash×3: 4-way parallel xxh3 — ~4 ns/cycle (3.75x speedup)
- Mod×2: Magic multiply — ~0.3 ns/cycle
- UnitInterval + Lerp: SIMD FMA — ~0.2 ns/cycle
- Column overhead: ~0.5 ns/cycle (amortized loop setup, column pointer management)
- **Total: ~5.5 ns/cycle**

**Speedup: ~4x over Phase 3 JIT.**

That's meaningful but not transformative. Where does the other expected speedup go?

1. **xxh3 is the bottleneck.** It's ~70% of the per-cycle cost, and hashing parallelizes at most 4x (AVX2 lane width for u64). You can't SIMD away the algorithmic complexity of a good hash.

2. **Memory bandwidth is NOT the bottleneck.** At 256 elements × 8 bytes × ~8 columns = 16 KB working set, everything is L1-resident. You're compute-bound, not memory-bound. Larger batches don't help — they just push you into L2 for no gain.

3. **The fusion wins are modest.** Monotonic mixed_radix is the big one (~10x improvement on that node). The rest save single nanoseconds.

---

## When Does This Mode NOT Pay Off?

| Scenario | Problem |
|----------|---------|
| Small DAGs (1-3 nodes) | Per-batch setup overhead dominates. Scalar Phase 3 at 3.8 ns for 16 nodes is already ~0.2 ns/node. |
| DAGs with Tier 3 nodes (LutSample, strings) | These force scalar fallback in the SIMD loop, breaking vectorization. If the bottleneck node is Tier 3, the whole batch mode is overhead. |
| Non-monotonic cycle assignment | Kills the monotonic mixed_radix optimization and PCG sequential stepping. You fall back to the general case for coordinate generation. |
| Very wide DAGs (many outputs) | Column wavefront grows, pushing working set out of L1. At 30+ live columns with N=256, you're at 60 KB — L1 boundary. |
| Workloads that discard results | If the adapter processes results one at a time (e.g., CQL prepared statement bind), you've generated 256 results and then consumed them serially. The batching helped the GK but not the I/O path. |

---

## The Honest Assessment

**Realistic net speedup: 2-4x over Phase 3 JIT** for the common case (hash-heavy numeric DAGs with monotonic cycles).

**Where the wins come from (ranked):**
1. **Monotonic mixed_radix** — algorithmic improvement, not just SIMD
2. **Parallel hashing** — 4-way xxh3 is the biggest raw SIMD win
3. **Amortized loop overhead** — one function call per batch vs. per cycle
4. **FMA fusion for f64 chains** — modest per-op, adds up

**Where the wins don't come from:**
- Cache effects — you're already L1-resident at scalar widths
- Memory bandwidth — not the bottleneck
- Most algebraic fusions — Phase 3 JIT already eliminates inter-node overhead

**Implementation cost:** High. You need a new compiler pass (pattern matching for fusion), a columnar buffer allocator with liveness analysis, a library of SIMD intrinsic kernels (platform-specific: AVX2, AVX-512, NEON), and a batch-aware eval interface that the executor/adapter layer must understand.

**The key question:** Is 2-4x on the GK evaluation path worth it when the GK is typically not the bottleneck? If adapter I/O (CQL round-trip, HTTP request) takes 500 µs and GK takes 22 ns, a 4x GK speedup saves 16.5 ns — invisible. The stream mode matters only for:
- **Benchmarks where GK is the SUT** (testing the generator itself)
- **Pre-generation** (filling a buffer of test data, no I/O)
- **Extremely high-throughput adapters** (in-memory stores where I/O is < 100 ns)

For the common nbrs use case (driving a database at 100K-1M ops/sec), the GK is already faster than needed. The stream mode is an optimization for workloads where data generation itself is the bottleneck.
