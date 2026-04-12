# Memo 07: DAG Provenance Invalidation

Provenance-based invalidation replaces the generation counter
model. Only nodes downstream of a changed input re-evaluate.
This eliminates the need for "global" vs "cycle" input
classifications — all inputs are just inputs, and the DAG
structure determines what needs recomputation.

**Status: Implemented.** The mechanism below is live in
`kernel.rs` and passes all 533 tests.

---

## Mechanism

### Compile Time: Provenance Bitmask

Each node gets a bitmask recording which graph inputs it
transitively depends on. Computed once from the DAG wiring in
topological order:

```rust
fn compute_provenance(nodes, wiring) -> Vec<u64> {
    let mut prov = vec![0u64; nodes.len()];
    for i in 0..nodes.len() {        // topological order
        for source in &wiring[i] {
            match source {
                Input(idx) => prov[i] |= 1 << idx,
                NodeOutput(upstream, _) => prov[i] |= prov[upstream],
                VolatilePort(_) | StickyPort(_) => prov[i] |= 1 << 63,
            }
        }
    }
    prov
}
```

>> Is it not better to use a bitmask which is an "invalidate transitively dependent nodes" mask for each input?
>> Then, each node will know from its single bit whether or not it is already invalided by a mutation of an upstream input. The invalidation bitmask here can automatically be calculated to cover all downstream transitive impacts.

>> We need to create a way to select a particular GKProgram implementation by name, and then bench between different implementations.

>> The designed behavior of GK programs must be stable according ot the spec, but the implementations can vary with the name selection mechanism once implemented.

Stored on `GkProgram` (immutable) and copied to each `GkState`
at creation for fast access without indirection.

Bit 63 is reserved for the "port" provenance — volatile and
sticky ports that change via captures.

### Runtime: Per-Node Clean Flag

Each `GkState` holds a `Vec<bool>` — one clean flag per node.
Initially all false (dirty). After evaluation, the flag is set
to true. On input change, only nodes whose provenance overlaps
the changed inputs are dirtied.

### set_inputs()

```rust
pub fn set_inputs(&mut self, new_values: &[u64]) {
    let mut changed_mask = 0u64;
    for i in 0..new_values.len() {
        if self.inputs[i] != new_values[i] {
            self.inputs[i] = new_values[i];
            changed_mask |= 1 << i;
        }
    }
    // Reset volatile ports (always counts as port change)
    self.volatile_values.clone_from_slice(&defaults);
    changed_mask |= 1 << 63;

    // Selectively invalidate: only dirty nodes that depend
    // on the inputs that actually changed
    if changed_mask != 0 {
        for (i, clean) in self.node_clean.iter_mut().enumerate() {
            if *clean && (self.input_provenance[i] & changed_mask) != 0 {
                *clean = false;
            }
        }
    }
}
```

**Key property:** A node that doesn't depend on any changed
input stays clean. `vector_dim(dataset)` (provenance: bit 1)
stays cached when only `cycle` (bit 0) changes.

### eval_node()

```rust
fn eval_node(&mut self, program, node_idx) {
    if self.node_clean[node_idx] {
        return;  // cached
    }
    // Evaluate upstream first
    for source in wiring[node_idx] {
        if let NodeOutput(upstream, _) = source {
            self.eval_node(program, upstream);
        }
    }
    // Gather inputs, evaluate, mark clean
    program.nodes[node_idx].eval(inputs, outputs);
    self.node_clean[node_idx] = true;
}
```

No generation counter. No per-input generation. No max_gen
computation. Just a boolean per node.

---

## Why This Eliminates "Global" Inputs

The previous model needed a "global" classification because
`set_inputs()` incremented a single generation counter,
invalidating ALL nodes. Params had to be kept outside GK to
avoid this.

With provenance invalidation:
- `set_inputs(&[43])` only dirties nodes with bit 0 in their
  provenance (cycle-dependent nodes)
- Nodes depending only on `dataset` (bit 1) are untouched
- No separate "global" storage needed

Params become regular GK inputs, set once at init. The
provenance bitmask ensures they never cause re-evaluation
(their bit in `changed_mask` is never set after init).

---

## Cost

**Memory per GkState:**
- `node_clean: Vec<bool>` — N bytes (one per node)
- `input_provenance: Vec<u64>` — 8N bytes (copied from program)

For a 50-node kernel: 450 bytes. Negligible.

**Per set_inputs() cost:**
- N comparisons for input values (was 0, now N — but N is
  typically 1-3 inputs)
- If any changed: N AND-and-branch for provenance check
- With one input (common case): ~50 comparisons for 50 nodes

**Per eval_node() cost:**
- One boolean check (was: one integer comparison)
- Identical or cheaper than before

**Net:** Slight increase in `set_inputs()` cost (provenance
scan). Decrease in eval_node cost for nodes that don't need
re-evaluation (skip instead of compare). For workloads with
params or captures, the savings from not re-evaluating stable
nodes far exceed the provenance scan cost.

---

## Optimization Opportunities

### 1. Bitmask-Level Bulk Invalidation

For architectures with SIMD, the provenance check can process
multiple nodes in parallel:

```
changed_mask = 0b0001  (only cycle changed)
// Load 4 provenance masks, AND with changed_mask, test zero
// Skip all 4 nodes if none depend on cycle
```

Not needed yet — the scalar loop is fast enough.

### 2. Tiered Dirty Propagation

Instead of scanning all N nodes on every set_inputs(), maintain
a per-input **dirty list**: for each input, a precomputed list
of node indices that depend on it. On change, dirty only those
nodes. O(affected) instead of O(all).

```rust
// Precomputed at compile time:
input_dependents: Vec<Vec<usize>>  // input_idx → [node_indices]

// On set_inputs():
for i in changed_inputs {
    for &node in &input_dependents[i] {
        node_clean[node] = false;
    }
}
```

Worth it when most inputs are stable (params, dataset) and
only one input changes per cycle (cycle). The scan skips
stable inputs entirely.

### 3. Evaluation-Order Shortcut

Nodes are in topological order. If the first N nodes in the
sorted order all have provenance = {cycle} and none depend on
ports, and only cycle changed, we know exactly which range of
nodes to dirty — no per-node provenance check needed:

```
nodes 0..K: cycle-only (provenance == 0x1)
nodes K..M: param-dependent (provenance has other bits)
nodes M..N: port-dependent (provenance has bit 63)

If only cycle changed: dirty nodes 0..K, skip K..N
```

This is a compile-time partition optimization. Effective when
the DAG has a clean separation between cycle-dependent and
param-dependent subgraphs.

### 4. Value-Based Deduplication

A node that re-evaluates but produces the same output as before
doesn't need to dirty its downstream consumers. This is the
"value-identical" optimization:

```rust
let old_output = buffers[node_idx][0].clone();
program.nodes[node_idx].eval(inputs, outputs);
if buffers[node_idx][0] == old_output {
    // Output unchanged — don't dirty downstream
}
```

Expensive for complex values (String comparison), cheap for u64.
Worth it for diamond-shaped DAGs where an intermediate node's
output is often unchanged despite input changes.

---

## Relationship to Other Mechanisms

| Old Mechanism | Replaced By |
|---------------|-------------|
| `generation: u64` | `node_clean: Vec<bool>` |
| `node_generation: Vec<u64>` | `node_clean: Vec<bool>` |
| `fold_init_constants()` | Nodes with empty provenance cache after first eval |
| "Global" inputs on GkProgram | Regular inputs, set once, never dirtied |

`fold_init_constants()` still exists as an explicit optimization
(replaces nodes with constant values, reducing the DAG). The
provenance system makes it less necessary but doesn't replace
the node-count reduction benefit.
