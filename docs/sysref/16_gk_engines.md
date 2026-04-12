# 16: GK Engines and Optimization

The GK evaluation engine has multiple compilation levels and
optimization strategies that compose independently. The compiler
selects the optimal combination at graph construction time.

---

## Compilation Levels

| Level | Mechanism | Per-Node Cost | When Best |
|-------|-----------|--------------|-----------|
| P1 | Interpreter: `Box<dyn GkNode>`, `Value` enum | ~20ns | Small graphs, high stability |
| P2 | Closures: flat u64 buffer, compiled step closures | ~10ns | Medium graphs, all-u64 |
| Hybrid | Mix of JIT + closures per node | ~5-10ns | Mixed node types |
| P3 | Cranelift JIT: native machine code | ~2-5ns | Large all-dirty graphs |

## Provenance Optimization

Provenance tracks which graph inputs each node depends on.
When an input doesn't change, nodes that depend only on it
skip evaluation entirely.

### Push-Side Invalidation

On `set_inputs()`, compare each input to its previous value.
For each changed input, dirty only the nodes in its dependent
list. Nodes not in any changed input's list stay clean.

| Engine | Mechanism |
|--------|-----------|
| P1 | `input_dependents[i]` → set `node_clean[j] = false` |
| P2 | Same, on `ProvenanceState.node_clean` |
| P3 | Same, on `JitProvenanceState.node_clean` (u8 array) |

Cost: O(changed_inputs × dependent_nodes_per_input).
Benefit: O(stable_nodes) evaluations skipped.

### Pull-Side Guard

Before evaluating, check if the requested output's upstream
cone was affected by any input change. If not, return the
cached value without running any evaluation.

```rust
pub fn eval_for_slot(&mut self, coords: &[u64], slot: usize) -> u64 {
    self.set_inputs(coords);
    // Pull-side guard: slot provenance AND changed mask
    if slot_provenance[slot] & changed_mask == 0 {
        return self.buffer[slot];  // entire eval skipped
    }
    self.run_eval();  // only dirty steps execute
    self.buffer[slot]
}
```

Cost: one AND + branch per pull.
Benefit: skips entire eval call when the output's cone is clean.

### Composition

Push-side and pull-side compose:

| Pull hits... | Push says... | Result |
|-------------|-------------|--------|
| Dirty cone | All nodes dirty | Full eval (no savings) |
| Dirty cone | Some nodes cached | Partial eval (push savings) |
| Clean cone | Any | Skip eval entirely (pull savings) |
| Clean cone | Some nodes cached | Skip eval (pull + push redundant) |

The best case is a graph with stable input subgraphs AND
selective output access — both optimizations compound.

---

## Engine Variants

Each compilation level has two variants:

| Variant | Constructor | Eval Method |
|---------|------------|-------------|
| Raw | `new()` / `try_compile_raw()` | `eval()` — all nodes, no checks |
| Provenance | `new_with_provenance()` / `try_compile()` | `eval()` + `eval_for_slot()` |

The variant is chosen at construction time. No runtime branching
for variant selection — each is a separate code path.

---

## Automatic Selection Heuristic

The compiler has all information needed at construction time:

```
stable_ratio = nodes_with_no_changing_input_provenance / total_nodes
output_selectivity = avg_output_cone_size / total_nodes

if total_nodes < 15:
    P3 raw                  // too small for provenance overhead
elif stable_ratio > 0.5:
    P1/prov                 // skip most nodes via provenance
elif stable_ratio > 0.3 or output_selectivity < 0.5:
    P3/prov + eval_for_slot // JIT speed + pull-side guard
else:
    P3 raw                  // all dirty, pure JIT throughput
```

## Benchmarking

```bash
# Compare all engine × provenance combinations
nbrs bench gk graph.gk --compare iters=5

# Full test suite
nbrs bench gk "nb-variates/tests/perf_tests/*.gk" --compare iters=5
```

The `--compare` flag shows raw and provenance variants for each
level side-by-side. Driver overhead is measured and subtracted
automatically.
