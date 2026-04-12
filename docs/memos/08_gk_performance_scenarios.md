# Memo 08: GK Performance Test Scenarios

A canonical set of `.gk` files for benchmarking the GK evaluation
engine across the dimensions that actually matter for performance.

---

## What We Know

From benchmarking so far:

1. **Provenance helps when inputs are heterogeneous.** With one
   input that changes every cycle, provenance adds overhead (the
   clean flag scan costs more than the evaluations it skips —
   because nothing gets skipped). With multiple inputs where
   some are stable, provenance avoids re-evaluating entire
   subgraphs.

2. **Graph topology matters.** Chains (depth) scale differently
   from fan-out (breadth). Dense reuse patterns (many nodes
   sharing few predecessors) behave differently from wide
   independent subgraphs.

3. **Output access pattern matters.** Pulling one output
   triggers evaluation of its upstream cone. Pulling all outputs
   evaluates everything. A graph with 1000 nodes but one output
   that depends on only 10 of them should be fast if provenance
   works correctly.

4. **Node cost varies.** Hash is ~5ns, mod is ~1ns, LUT lookup
   is ~3ns. Expensive nodes (icd_normal, pcg_seek) are 10-50ns.
   The ratio of node cost to invalidation overhead determines
   when provenance pays off.

5. **Compilation level interacts with provenance.** P3 JIT
   inlines node operations, making each node cheaper. This
   raises the relative cost of provenance checks. P1 (interpreted)
   has higher per-node overhead, making provenance checks
   relatively cheaper.

---

## Performance Dimensions

### Dimension 1: Input Count and Stability

How many graph inputs, and which change per cycle.

| Scenario | Inputs | Changes Per Cycle | Provenance Impact |
|----------|--------|-------------------|-------------------|
| Single-cycle | 1 (cycle) | All | None — everything dirty |
| Cycle + params | 3 (cycle, keyspace, k) | 1 of 3 | High — 2/3 of graph stable |
| Multi-input | 4 (cycle, partition, cluster, seed) | 1-2 of 4 | Medium |
| All-changing | 3 (a, b, c) | All 3 | None — provenance overhead only |

### Dimension 2: Output Access Pattern

Which outputs are pulled, and what fraction of the graph they
touch.

| Scenario | Outputs Pulled | Graph Coverage |
|----------|---------------|----------------|
| Single leaf | 1 output at end of chain | Entire chain |
| Partial cone | 1 output from a subgraph | Subset of nodes |
| All outputs | Every node is an output | Everything evaluates |
| Alternating | Different output each cycle | Varies per cycle |

### Dimension 3: Topology

The shape of the DAG.

| Scenario | Structure | Key Property |
|----------|-----------|-------------|
| Chain | A → B → C → ... | Maximum depth, minimum breadth |
| Wide parallel | A → X, B → Y, C → Z | Independent subgraphs, no sharing |
| Fan-out | A → {B, C, D, E, F} | One node feeds many |
| Fan-in | {A, B, C, D, E} → F | Many nodes feed one |
| Diamond | A → {B, C} → D | Shared predecessor, merge point |
| Layered | Width W, depth D | Moderate connectivity |
| Multi-root | Separate trees per input | Input-specific subgraphs |

### Dimension 4: Node Cost

Cheap vs expensive per-node operations.

| Scenario | Node Types | Approx Cost |
|----------|-----------|-------------|
| Cheap | hash, mod, identity | 1-5ns each |
| Mixed | hash + icd_normal + pcg | 5-50ns each |
| Expensive | pcg_seek chains | 20-50ns each |

### Dimension 5: Scale

| Size | Nodes | Typical Use |
|------|-------|-------------|
| Tiny | 5-10 | Simple workload bindings |
| Small | 20-50 | Typical production workload |
| Medium | 100-200 | Complex multi-op workload |
| Large | 500-1000 | Stress test / extreme |

---

## Test Matrix

Not every combination is meaningful. The canonical scenarios
should cover the performance-relevant corners:

### Group A: Provenance Sensitivity

These test when provenance helps vs hurts.

| File | Inputs | Stable | Topology | Nodes | Key Question |
|------|--------|--------|----------|-------|-------------|
| `a1_single_input.gk` | 1 (cycle) | 0 | chain | 50 | Baseline: provenance overhead with no benefit |
| `a2_stable_params.gk` | 3 (cycle, ks, ds) | 2 | multi-root | 50 | Provenance payoff: 2 stable subgraphs |
| `a3_all_changing.gk` | 3 (a, b, c) | 0 | layered | 50 | Worst case: provenance cost, no caching |
| `a4_mostly_stable.gk` | 5 (cycle + 4 params) | 4 | multi-root | 100 | Best case: 80% of graph cached |

### Group B: Output Access Patterns

These test pull-on-demand vs evaluate-everything.

| File | Inputs | Outputs Pulled | Total Nodes | Key Question |
|------|--------|---------------|-------------|-------------|
| `b1_single_output.gk` | 1 | 1 (end of chain) | 100 | Only upstream cone evaluates |
| `b2_partial_cone.gk` | 2 | 1 (from input-1 subgraph) | 100 | Only input-1 subgraph evaluates |
| `b3_all_outputs.gk` | 1 | all | 100 | Everything evaluates |

### Group C: Topology Scaling

These test how topology interacts with scale.

| File | Topology | Nodes | Degree | Key Question |
|------|----------|-------|--------|-------------|
| `c1_deep_chain.gk` | chain | 200 | 1 | Depth scaling |
| `c2_wide_parallel.gk` | parallel | 200 | 1 | Breadth scaling, no sharing |
| `c3_fan_out.gk` | fan-out | 200 | 6 out | One root feeds many |
| `c4_diamond.gk` | diamond | 200 | 2 | Shared predecessors |
| `c5_multi_root.gk` | multi-root | 200 | 1 | Separate subgraphs per input |

### Group D: Realistic Workloads

These approximate real nb-rs workload binding patterns.

| File | Pattern | Nodes | Description |
|------|---------|-------|-------------|
| `d1_keyvalue.gk` | key-value rampup | ~10 | hash → mod → format (typical INSERT) |
| `d2_vector_search.gk` | ANN workload | ~15 | hash, vector_at*, format, ground_truth |
| `d3_service_model.gk` | bimodal latency | ~20 | pcg, icd_normal, select, format |
| `d4_multi_table.gk` | multi-table workload | ~40 | 4 independent table generators sharing cycle |

---

## What Each Scenario Tests

**Group A** answers: "When does provenance pay for itself?"
Run with `--provenance` and `--no-provenance` and compare.

**Group B** answers: "Does lazy evaluation (pull only what's
needed) matter?" This interacts with provenance — if only a
small cone is pulled and only its inputs changed, provenance
makes the rest free.

**Group C** answers: "How does topology affect scaling?" This
is the traditional complexity benchmark. Run at multiple scales
to see linear vs superlinear behavior.

**Group D** answers: "What does real-world performance look
like?" These are the workload patterns users actually write.
The other groups isolate variables; this group validates that
the isolated findings translate to reality.

---

## Execution

```bash
# Full suite with provenance comparison
nbrs bench gk perf_tests/a*.gk --provenance iters=10
nbrs bench gk perf_tests/a*.gk --no-provenance iters=10

# Topology scaling
nbrs bench gk perf_tests/c*.gk iters=10

# Realistic workloads
nbrs bench gk perf_tests/d*.gk iters=10
```

The comparison table shows all files side-by-side with
P1/P2/Hybrid/P3 across the matrix.
