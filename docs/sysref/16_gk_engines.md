# 16: GK Engines and Optimization

The GK evaluation engine has multiple compilation levels and
optimization strategies that compose independently. The compiler
selects the optimal combination at graph construction time and
produces a **monomorphic kernel** — a distinct type with the
selected optimizations baked into the eval path, no runtime
branching for strategy selection.

The *which level gets picked and why* is this document. The
*how a Phase-3 kernel plugs into the runtime* — specifically
the Cranelift ↔ Rust call boundary and the setjmp/longjmp shim
that lets predicate violations surface as catchable panics —
lives in [SRD 16b GK JIT Wiring](16_gk_jit.md).

---

## Compilation Levels

| Level | Mechanism | Per-Node Cost | When Best |
|-------|-----------|--------------|-----------|
| P1 | Interpreter: `Box<dyn GkNode>`, `Value` enum | ~20ns | Small graphs, high stability |
| P2 | Closures: flat u64 buffer, compiled step closures | ~10ns | Medium graphs, all-u64 |
| P3 | Cranelift JIT: native machine code | ~2-5ns | Large all-dirty graphs |

### Per-Node Overhead

Raw per-node costs (no provenance optimization, identity-chain benchmark):

| Level | Mechanism | Per-Node Cost |
|-------|-----------|---------------|
| P1 | Value enum + trait dispatch | ~20-70ns |
| P2 | u64 closures + flat buffer | ~4-10ns |
| P3 | Cranelift native code | ~0.2-5ns |

P2 eliminates: Value enum allocation, trait object dispatch,
HashMap output lookup. P3 eliminates: closure call overhead,
gather/scatter copies.

Hybrid (P2+P3 mix) was benchmarked and found to be strictly
dominated by P3 in all scenarios. It remains in the codebase
but is not selected by the automatic heuristic.

## Provenance Optimization

Provenance tracks which graph inputs each node depends on.
Two independent optimizations exploit this information:
**push-side invalidation** (skip clean nodes within an eval)
and **pull-side cone guard** (skip the entire eval call).

### Monomorphic Kernel Principle

Each optimization combination is a **separate kernel type**
produced by a distinct compiler path. The hot eval loop contains
no `if let Some(prov)` branching — each kernel type has exactly
the fields and code it needs, nothing more.

| Kernel Variant | Push-Side | Pull-Side | Compiler Method |
|---------------|-----------|-----------|-----------------|
| Raw | — | — | `try_compile_raw()` |
| Push | per-node dirty skip | — | `try_compile_push()` |
| Pull | — | cone guard | `try_compile_pull()` |
| PushPull | per-node dirty skip | cone guard | `try_compile()` |

Each variant has distinct:
- **`set_inputs()`** — Raw: plain copy. Push: marks dependents
  dirty. Pull: tracks changed_mask only. PushPull: both.
- **`eval()`** / **`eval_for_slot()`** — Raw: runs all steps.
  Push: skips clean steps. Pull: cone guard then runs all steps
  in cone. PushPull: cone guard then skips clean steps.

The compiler selects the variant at construction time based on
graph analysis. The bench tool can request all four variants
independently via `--compare-modes`.

### Push-Side Invalidation

On `set_inputs()`, compare each input to its previous value.
For each changed input, dirty only the nodes in its dependent
list. Nodes not in any changed input's list stay clean.

Data structures:
- `node_clean: Vec<bool>` (P2) / `Vec<u8>` (P3) — per-step
- `input_dependents: Vec<Vec<usize>>` — per-input → step indices

Cost: O(changed_inputs × dependent_nodes_per_input).
Benefit: O(stable_nodes) evaluations skipped per cycle.
Overhead on all-dirty: ~35% (per-node clean check in eval loop).

### Pull-Side Cone Guard

Before evaluating, check if the requested output's upstream
cone was affected by any input change. If not, return the
cached value without entering the eval loop at all.

Data structures:
- `slot_provenance: Vec<u64>` — per-output-slot bitmask
- `changed_mask: u64` — set by `set_inputs()`

```rust
// Pull-side guard: one AND + branch
if slot_provenance[slot] & changed_mask == 0 {
    return self.buffer[slot];  // entire eval skipped
}
```

Cost: one AND + branch per pull.
Benefit: skips entire eval when the output's cone is clean.
Overhead when cone IS dirty: ~2ns (the AND + branch that
falls through).

### Composition

Push-side and pull-side compose independently:

| Pull hits... | Push says... | Result |
|-------------|-------------|--------|
| Dirty cone | All nodes dirty | Full eval (no savings) |
| Dirty cone | Some nodes cached | Partial eval (push savings) |
| Clean cone | Any | Skip eval entirely (pull savings) |

The best case is a graph with stable input subgraphs AND
selective output access — both optimizations compound.

The pull guard provides the dominant speedup (3-14×) by
skipping the entire eval call. Push alone gives 1.5-2.3×.
Combined: 3-30× on stable graphs with selective outputs.

---

## Automatic Selection Heuristic

The compiler has all information needed at construction time.
Pull has zero overhead on all-dirty graphs, so it is the safe
default. Push adds ~40% overhead and is only selected when it
provides demonstrated benefit within dirty cones.

```
output_cone_ratio = max_output_cone / total_nodes
stable_ratio = stable_nodes / total_nodes

if total_nodes < 15 and stable_ratio == 0:
    P3/Raw                    // tiny all-dirty, skip provenance data

elif output_cone_ratio < 0.5:
    P3/Pull                   // cone guard: 7-12ns for 100+ nodes

elif stable_ratio >= 0.3:
    P3/PushPull               // push skip within dirty cones

else:
    P3/Pull                   // free insurance — acts like Raw on all-dirty
```

Decision inputs (all known at compile time):

- **total_nodes** — from the compiled DAG
- **output_cone_ratio** — for each output, the transitive dependency
  set ("cone") size vs total nodes. The primary decision variable.
- **stable_ratio** — nodes reachable only from constant or config
  inputs. Only matters when cones are large.

Pull is the dominant kernel variant because:
- Zero overhead on all-dirty (cone check falls through in ~2ns)
- 7-12ns for 100+ node graphs when output cones are selective
- No dependent iteration in `set_inputs` (only tracks changed_mask)

Push is only added (as PushPull) when the output cone is large
AND there are stable subgraphs within it (stable_ratio ≥ 0.3).
Push-only is never selected — it is dominated by both Pull and
PushPull in all measured scenarios.

---

## Type System

The buffer stores all values as u64. The PortType enum tracks
types statically:

| PortType | Width | Storage |
|----------|-------|---------|
| U64 | 64-bit | native |
| F64 | 64-bit | bit-packed (to_bits/from_bits) |
| U32 | 32-bit | zero-extended in u64 |
| I32 | 32-bit | sign-extended in u64 |
| I64 | 64-bit | bit-reinterpret |
| F32 | 32-bit | f32 bits in low 32 of u64 |

The assembler auto-inserts widening adapters when types mismatch
(e.g., U32→U64, I32→F64, F32→F64). Narrowing requires explicit
cast functions — no implicit precision loss.

Adapters are the *type-coercion* half of the assembler's
input-validity model. The *value-validity* half is handled by
opt-in assertion nodes under strict wire mode — the assembler
skips every assertion whose redundancy it can prove statically
(matched types after adapter insertion, constant sources
validated at assembly time, fusion-derived value bounds). See
[SRD 15 §"Input Validity Model" and §"Strict Wire Mode"](15_strict_mode.md#input-validity-model-unsafe-by-default--opt-in-guards)
for the assertion family, the const constraint metadata on
`ParamSpec`, and the `AssertionInserted` / `AssertionSkipped`
diagnostic events the assembler emits alongside
`TypeAdapterInserted`.

---

## Benchmarking

```bash
# Default: provenance-enabled for each level
nbrs bench gk graph.gk iters=5

# Compare raw vs provenance per level
nbrs bench gk graph.gk --compare iters=5

# Full 3-way decomposition: raw / push / push+pull
nbrs bench gk graph.gk --compare-modes iters=5

# Full test suite
nbrs bench gk "nb-variates/tests/perf_tests/*.gk" --compare-modes iters=5
```

The `--compare-modes` flag shows all three variants (raw, push,
push+pull) for each compilation level. Driver overhead is measured
and subtracted automatically.
