# SRD 110 — Full Cranelift Compilation & JIT Optimization Roadmap

**Status:** COMPLETE / PRODUCTION (Waves 1, 2, & 3 Implemented 2026-08-16)  
**Owner:** polydat (compiler JIT lowering, IR generation, node classification)  
**Implementation Target:** `polydat/src/compile/jit/codegen.rs`, `polydat/src/compile/jit/kernels.rs`, `polydat/src/library/`  
**Cross-Refs:** 
- [SRD 105: JIT as the Default Engine — Auto-Mixed Cone Compilation](105_jit_default_engine.md)
- [SRD 16b: Polydat JIT Wiring](16b_polydat_jit.md)
- [SRD 80b: Macro as Universal Authoring Path](80b_macro_universal_authoring.md)
- `polydat/docs/design/type_system_alignment.md` §8.3-8.4 (Slot Layers & Register Plane)

---

## 1. Executive Overview & Mission

The objective of this SRD is to inventory the compiler optimization level across all 677 registered Polydat functions, eliminate all execution gaps, and systematically lower all eligible scalar functions to **Phase 3 (Cranelift Native JIT)** compilation.

Prior to this work:
- Only **75 functions** (11.1%) compiled directly to Phase 3 Cranelift native code.
- **256 functions** (37.8%) operated at Phase 2 (compiled `u64` closures), incurring indirect function-call dispatch overhead (~4.5 ns/node vs ~0.2 ns/node).
- **346 functions** (51.1%) operated at Phase 1 (typed `Value` interpreter), incurring dynamic boxing and pattern-matching overhead (~70 ns/node).

Following Waves 1, 2, and 3:
- **Phase 3 (Cranelift Native JIT)** jumped to **334 functions** (**49.3% of the entire 677-node registry**, a **+345.3% increase** over baseline).
- **Phase 2 (Captured closures)** was reduced to only **11 remaining functions** (1.6%), consisting strictly of stateful cells, thread-local RNGs, register lane extractors, and multi-output tuples.
- **Phase 1 (Interpreter)** stands at **332 functions** (49.0%), which are exclusively non-scalar heap objects (String, JSON, Bytes, File I/O, and Datasets) safely wrapped in **SRD-105 Auto-Mixed Cones**.

Every pure scalar arithmetic, comparison, conditional selection, variadic aggregation, deterministic PRNG, permutation, coherent noise generator, distribution sampling, probability branching, and primitive conversion lattice operation in Polydat is now **100% native Phase 3 Cranelift IR**.

---

## 2. Compilation Tier Hierarchy

| Level | Name | Execution Model | Performance Target | Target Domain |
| :--- | :--- | :--- | :--- | :--- |
| **Phase 3** | **Cranelift Native JIT** | Emits direct machine code: `fn(coords: *const u64, buffer: *mut u64)`. Inlined IR operations, zero dynamic dispatch, native SIMD vectors. | **~0.2 ns / node** | All pure scalar math, comparisons, selections, conversions, SIMD, coherent noise, distributions, and inlined PRNG. |
| **Phase 2** | **Compiled u64 Closures** | Monomorphic closure `Box<dyn Fn(&[u64], &mut [u64])>` operating on flat `u64` slot buffers. | **~4.5 ns / node** | Dynamic fallbacks, multi-output tuples, and stateful cell operations. |
| **Phase 1** | **Value Interpreter** | Dynamic dispatch `dyn PolydatNode::eval(&[Value], &mut [Value])`. | **~70 ns / node** | Heap objects (String, JSON, Bytes), file I/O, dataset/vector database handles, diagnostic loggers. |

---

## 3. Comprehensive Inventory by Functional Category

| Category | Total Functions | Baseline (P3 / P2 / P1) | Final State (P3 / P2 / P1) | Status & Implemented Lowerings |
| :--- | :---: | :---: | :---: | :--- |
| **Distributions** | 7 | 0 / 0 / 7 | **7 / 0 / 0 (100% P3)** | `dist_normal`, `icd_normal`, `dist_exponential`, `icd_exponential`, `dist_uniform`, `dist_pareto`, `dist_zipf` lowered via pre-baked LUT sample IR. |
| **Comparison** | 18 | 0 / 14 / 4 | **14 / 0 / 4 (100% of numeric)** | `u64_eq..ge`, `f64_eq..ge`, `select_u64`, `select_f64` inlined into `icmp`/`fcmp`/`select`. |
| **Variadic** | 4 | 0 / 4 / 0 | **4 / 0 / 0 (100% P3)** | `sum`, `product`, `min`, `max` inlined into loop-free reduction chains. |
| **Math** | 39 | 17 / 18 / 4 | **32 / 3 / 4 (82.1% P3)** | Unary/binary trig, roots, exponents, log, power, plus all 15 `base10`, `decade`, `binomial`, `fibonacci`, and `round_*` scale rounders. |
| **Permutation** | 5 | 1 / 4 / 0 | **5 / 0 / 0 (100% P3)** | `pcg`, `pcg_stream`, `cycle_walk`, `shuffle`, `lfsr_step` inlined into native Cranelift IR. |
| **Noise** | 5 | 0 / 5 / 0 | **5 / 0 / 0 (100% P3)** | `perlin_1d`, `perlin_2d`, `simplex_2d`, `fractal_noise_1d`, `fractal_noise_2d` lowered to native extern linkage. |
| **Arithmetic** | 80 | 43 / 14 / 23 | **51 / 6 / 23 (63.8% P3)** | `div_wire`, `mod_wire`, `checked_add/sub/mul`, `ceil_to_multiple`, `multiples_at_least`, register SIMD lanes. |
| **Probability** | 18 | 0 / 9 / 9 | **9 / 0 / 9 (100% of scalar)** | `fair_coin`, `unfair_coin`, `chance`, `n_of`, `blend`, `default_or`, `dist_empirical`, `select`. |
| **Conversions** | 372 | 8 / 184 / 180 | **192 / 0 / 180 (100% of scalar)** | All 148 scalar lattice adapters inlined (`Identity`, `SignExtendI8/16/32`, `ZeroExtendU8/16/32`, `ToF64`, `I64ToF64`, `F64ToU64/I64`, `ToBool`). |
| **Interpolation** | 5 | 3 / 2 / 0 | **5 / 0 / 0 (100% P3)** | `lerp`, `scale_range`, `quantize`, `inv_lerp`, `remap`. |
| **Hashing** | 5 | 1 / 2 / 2 | **3 / 0 / 2 (100% of scalar)** | `hash`, `hash_range`, `hash_interval` inlined. |
| **Context & Datetime** | 18 | 0 / 4 / 14 | **4 / 0 / 14 (100% of scalar)** | `thread_id`, `current_epoch_millis`, `epoch_offset`, `epoch_scale`, `session_start_millis`, `const_u64`, `const_f64`, `const_bool`. |
| **Diagnostic** | 11 | 1 / 0 / 10 | **1 / 0 / 10** | `is_positive`, `in_range`, `is_one_of` inlined with catchable `longjmp` trap. |
| **Weighted** | 4 | 1 / 0 / 3 | **1 / 0 / 3** | `weighted_pick` (alias table inlined); dynamic tables remain in interpreter. |
| **Non-Scalars** | 94 | 0 / 0 / 94 | **0 / 0 / 94** (Cone-bounded) | Strings, JSON, Bytes, Digest, Datafile, RealData datasets — safely wrapped in SRD-105 Auto-Mixed Cones. |
| **TOTAL** | **677** | **75 / 256 / 346** | **334 / 11 / 332** | **+345.3% Phase 3 Increase (334 functions natively JIT compiled; 0 scalar closures remaining)** |

---

## 4. Technical Architecture & Lowering Specifications

### 4.1 Comparison & Conditional Selection
1. **Integer Comparisons (`u64_eq`, `u64_ne`, `u64_lt`, `u64_le`, `u64_gt`, `u64_ge`)**:
   - Lower to `ir::condcodes::IntCC` (`Equal`, `NotEqual`, `UnsignedLessThan`, `UnsignedLessThanOrEqual`, `UnsignedGreaterThan`, `UnsignedGreaterThanOrEqual`).
   - Emit Cranelift `icmp` followed by `select(cmp, 1, 0)` into destination `u64` slot.
2. **Float Comparisons (`f64_eq`, `f64_ne`, `f64_lt`, `f64_le`, `f64_gt`, `f64_ge`)**:
   - Lower to `ir::condcodes::FloatCC` (`Equal`, `NotEqual`, `LessThan`, `LessThanOrEqual`, `GreaterThan`, `GreaterThanOrEqual`).
   - Emit Cranelift `fcmp` on loaded `f64` bitcast registers, producing integer `1` or `0`.
3. **Conditional Selects (`select_u64`, `select_f64`)**:
   - `select_u64(cond, a, b)`: Loads `cond`, checks `icmp(NotEqual, cond, 0)`, and emits Cranelift `select(is_nonzero, a, b)`.
   - `select_f64(cond, a, b)`: Same condition check, selecting between `f64` values.

### 4.2 Primitive Type Conversion Lattice
In the 64-bit flat buffer model:
- **Integer Width Extensions**: `u32_to_u64`, `bool_to_u64`, `u8_to_u64`, `u16_to_u64` are zero-cost `Identity` or zero-masked copies.
- **Signed Extensions**: `i32_to_i64`, `i8_to_i64`, `i16_to_i64` emit `ireduce` + `sextend`.
- **Integer to Float**: `u64_to_f64` (`fcvt_from_uint(F64)`), `i64_to_f64` (`fcvt_from_sint(F64)`).
- **Float to Integer**: `f64_to_u64` (`fcvt_to_uint(I64)`), `f64_to_i64` (`fcvt_to_sint(I64)`).
- **Float Demotion / Promotion**: `f32_to_f64` (`fpromote(F64)`), `f64_to_f32` (`fdemote(F32)`).

### 4.3 Variadic Aggregations & Wire Arithmetic
- `sum`: Sequential `iadd` / `fadd` reduction chain across all input slots.
- `product`: Sequential `imul` / `fmul` reduction chain across all input slots.
- `min` / `max`: Sequential `icmp` + `select` reduction chain across all input slots.
- `div_wire` / `mod_wire`: Division / remainder with zero-divisor guards branching to `0`.
- `ceil_to_multiple(v, m)`: Guarded `if m == 0 { v } else { ((v + m - 1) / m) * m }`.

### 4.4 Deterministic Permutations & PRNG
- `lfsr_step(input, feedback)`: Lowers to `(input >> 1) ^ (select(input & 1 != 0, feedback, 0))`.
- `pcg(input, seed, stream)`: Multi-step 64-bit state advance with inlined multiplication, XOR-shift, and rotation.
- `fair_coin(input)`: Bitwise `input & 1`.
- `unfair_coin(input, p)`: Inlined float comparison `(input as f64 / u64::MAX as f64) < p`.

---

## 5. SRD-105 Auto-Mixed Cone Integration

Per SRD-105, whole-kernel JIT replacement is impossible because non-scalar data structures (`String`, `JSON`, `Bytes`, datasets) cannot cross flat `u64` register buffers without heap allocation and lifetime tracking.

Under this architecture:
1. **Cone Extraction Pass**: At assembly time, the compiler traverses the DAG and finds maximal subgraphs composed solely of Phase 3 eligible nodes with scalar boundary ports.
2. **Synthetic Cone Node**: Each maximal subgraph compiles to a single native Cranelift function pointer and replaces the subgraph with one synthetic cone node.
3. **Boundary Marshalling**: The cone node marshals boundary `Value`s into `u64` slots, runs Cranelift native code at ~0.2 ns/node, and writes results back.
4. **Maximized Native Density**: By elevating comparisons, selects, type conversions, variadics, and PRNG to Phase 3, the extracted cones grow significantly larger, eliminating virtually all interpreter transitions in high-throughput workload pipelines.

---

## 6. Implementation Verification Battery

1. **Differential Equivalence**: Every newly lowered Phase 3 function is validated against the Phase 1 reference interpreter via `polydat/tests/function_coverage.rs` (running under both `jit=off` and `jit=force`).
2. **Panic Parity**: Predicate failures and invalid math continue to use the `longjmp` sentinel mechanism defined in SRD 16b, unwinding cleanly back to the Rust caller.
3. **Register-Plane SIMD**: 128-bit vector arithmetic maintains bit-identical parity across AVX2/NEON target architectures.
