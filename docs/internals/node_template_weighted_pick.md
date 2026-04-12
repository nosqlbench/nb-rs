# Memo 04: Annotated Reference Implementation — WeightedPick

`WeightedPick` in `nb-variates/src/nodes/weighted.rs` is the reference
implementation that exercises every GK metadata feature. Use it as a
template when building new nodes.

---

## Features Exercised

| Feature | Where |
|---------|-------|
| Wire input | `Slot::Wire(Port::u64("input"))` in `ins` |
| Constant inputs | `Slot::const_f64("w0", w)`, `Slot::const_u64("v0", v)` in `ins` |
| Phase 1 eval | `fn eval()` with `Value` enum |
| Phase 2 closure | `fn compiled_u64()` captures alias table arrays |
| JIT constants | `fn jit_constants()` exposes values |
| Commutativity | `fn commutativity()` returns `Positional` (pairs baked at construction) |
| Fusion equivalence | `FusedNode::decomposed()` decomposes to `WeightedU64` |
| Variadic group | `Arity::VariadicGroup` in FuncSig with `[ConstF64, ConstU64]` repeat |
| Slot metadata | `meta().const_slots()` returns named, typed constants |

---

## The Struct

```rust
pub struct WeightedPick {
    meta: NodeMeta,
    weights: Vec<f64>,
    values: Vec<u64>,
    table: AliasTableU64,
}
```

Four fields:

- `meta` — the node's interface metadata. Every GK node has this.
- `weights` — the original weight values, kept for `decomposed()`.
  Not needed for evaluation (the alias table subsumes them), but
  the fusion equivalence contract needs to reconstruct the spec.
- `values` — the u64 outcome values. Indexed by the alias table's
  output. Also captured by the Phase 2 closure.
- `table` — the pre-built alias table for O(1) weighted sampling.
  Constructed once at assembly time from `weights`. The table's
  internal arrays (biases, primaries, aliases) are captured by
  the Phase 2 closure.

---

## The Constructor

```rust
pub fn new(pairs: &[(f64, u64)]) -> Self {
    assert!(!pairs.is_empty(), "weighted_pick requires at least one pair");
    let weights: Vec<f64> = pairs.iter().map(|(w, _)| *w).collect();
    let values: Vec<u64> = pairs.iter().map(|(_, v)| *v).collect();
    let table = AliasTableU64::from_weights(&weights);

    let mut ins = vec![Slot::Wire(Port::u64("input"))];
    for (i, &(w, v)) in pairs.iter().enumerate() {
        ins.push(Slot::const_f64(format!("w{i}"), w));
        ins.push(Slot::const_u64(format!("v{i}"), v));
    }

    Self {
        meta: NodeMeta {
            name: "weighted_pick".into(),
            ins,
            outs: vec![Port::u64("output")],
        },
        weights,
        values,
        table,
    }
}
```

### Input: `pairs: &[(f64, u64)]`

The caller provides weight/value pairs. The DSL compiler builds this
from interleaved constant arguments:
`weighted_pick(hash(cycle), 0.5, 10, 0.3, 20, 0.2, 30)` becomes
`WeightedPick::new(&[(0.5, 10), (0.3, 20), (0.2, 30)])`.

### Alias table construction

`AliasTableU64::from_weights(&weights)` builds the Vose alias table
at assembly time. This is an O(N) init cost that pays for O(1) per-cycle
sampling. The table is never rebuilt — it's frozen after construction.

### Slot population

```rust
let mut ins = vec![Slot::Wire(Port::u64("input"))];
for (i, &(w, v)) in pairs.iter().enumerate() {
    ins.push(Slot::const_f64(format!("w{i}"), w));
    ins.push(Slot::const_u64(format!("v{i}"), v));
}
```

Position 0 is the wire input (the entropy source). Positions 1..N
are interleaved constant pairs: `w0, v0, w1, v1, ...`. Each constant
has a name (`"w0"`, `"v0"`) and a typed value (`ConstValue::F64` or
`ConstValue::U64`).

This is the first node to use **interleaved heterogeneous constants**:
f64 weights alternating with u64 values. The `FuncSig` describes this
pattern with `Arity::VariadicGroup`:

```rust
arity: Arity::VariadicGroup {
    group: &[SlotType::ConstF64, SlotType::ConstU64],
    min_repeats: 1,
}
```

The fusion pass and diagnostic tooling can inspect these constants
via `meta().const_slots()`, which returns `[("w0", F64(0.5)), ("v0", U64(10)), ...]`.

---

## GkNode Trait Implementation

### `fn eval()`

```rust
fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
    let idx = self.table.sample(inputs[0].as_u64()) as usize;
    outputs[0] = Value::U64(self.values[idx]);
}
```

Phase 1 runtime evaluation. The alias table's `sample()` method
takes a uniform u64 and returns an outcome index in O(1):
- Low bits select a slot: `input % N`
- High bits test the bias threshold for that slot
- Returns either the primary or alias outcome

Then `self.values[idx]` maps the index to the actual u64 value.

### `fn commutativity()`

```rust
fn commutativity(&self) -> Commutativity {
    Commutativity::Positional
}
```

Returns `Positional` because the constants are already baked at
construction — there's nothing to permute at the wire level. The
fact that reordering pairs doesn't change behavior is a function-level
property expressed on `FuncSig.commutativity`, not on the node instance.

### `fn compiled_u64()`

```rust
fn compiled_u64(&self) -> Option<CompiledU64Op> {
    let values = self.values.clone();
    let biases = self.table.biases().to_vec();
    let primaries = self.table.primaries().to_vec();
    let aliases = self.table.aliases().to_vec();
    let n = values.len();
    Some(Box::new(move |inputs, outputs| {
        let input = inputs[0];
        let slot = (input as usize) % n;
        let bias_test = ((input >> 32) as f64) / (u32::MAX as f64);
        let index = if bias_test < biases[slot] {
            primaries[slot]
        } else {
            aliases[slot]
        };
        outputs[0] = values[index as usize];
    }))
}
```

Phase 2 compiled closure. Captures four flat arrays by value:

- `values` — the outcome values (u64)
- `biases` — per-slot threshold (f64)
- `primaries` — primary outcome index per slot (u64)
- `aliases` — alias outcome index per slot (u64)

The closure inlines the alias sampling algorithm: no `Value` enum,
no virtual dispatch, no `self`. Cost: ~4.5 ns/node baseline + one
branch + two array lookups.

### `fn jit_constants()`

```rust
fn jit_constants(&self) -> Vec<u64> {
    self.values.clone()
}
```

Exposes the outcome values for Phase 3 JIT classification. The JIT
classifier uses this to determine the `JitOp` variant. For nodes
with complex internal state (like the alias table), only the values
are exposed — the table structure is an implementation detail.

Note: `meta().jit_constants_from_slots()` returns ALL constants
(weights as f64 bits interleaved with values). This is a richer
view — the two methods serve different consumers:
- `jit_constants()` → JIT classifier (needs values only)
- `jit_constants_from_slots()` → metadata tooling (needs everything)

---

## FusedNode Implementation

```rust
impl FusedNode for WeightedPick {
    fn decomposed(&self) -> DecomposedGraph {
        let spec: String = self.values.iter().zip(self.weights.iter())
            .map(|(v, w)| format!("{v}:{w}"))
            .collect::<Vec<_>>()
            .join(";");
        let mut g = DecomposedGraph::new(1);
        let wu = g.add_node(
            Box::new(WeightedU64::new(&spec)),
            vec![DecomposedWire::Input(0)],
        );
        g.set_outputs(vec![DecomposedWire::Node(wu, 0)]);
        g
    }
}
```

### The equivalence claim

`weighted_pick(input, 0.5, 10, 0.3, 20, 0.2, 30)` is semantically
equivalent to `weighted_u64(input, "10:0.5;20:0.3;30:0.2")`.

The `decomposed()` method reconstructs the spec string from the stored
weights and values, builds a `WeightedU64` node with that spec, and
returns a one-node graph. The equivalence test verifies this on 10,000
random inputs.

### Why this matters

The existing `WeightedU64` parses a spec string and is the established
implementation. `WeightedPick` is a structured alternative with typed
constants instead of a string blob. The fusion equivalence proves they
produce identical outputs — users can use either form and get the same
results.

---

## FuncSig Registration

```rust
FuncSig {
    name: "weighted_pick",
    category: C::Weighted,
    outputs: 1,
    description: "weighted u64 selection from inline weight/value pairs",
    identity: None,
    variadic_ctor: None,
    params: &[
        ParamSpec { name: "input", slot_type: SlotType::Wire, required: true },
        ParamSpec { name: "weight", slot_type: SlotType::ConstF64, required: true },
        ParamSpec { name: "value", slot_type: SlotType::ConstU64, required: true },
    ],
    arity: Arity::VariadicGroup {
        group: &[SlotType::ConstF64, SlotType::ConstU64],
        min_repeats: 1,
    },
    commutativity: Commutativity::Positional,
}
```

### `params`

Three parameter specs: one fixed wire, then a repeating group of
(f64 weight, u64 value). The `params` list shows the template;
`arity: VariadicGroup` says the last two repeat.

### `Arity::VariadicGroup`

This is the first real usage of `VariadicGroup` in the codebase.
The `group` field describes the repeating unit: one `ConstF64`
followed by one `ConstU64`. `min_repeats: 1` means at least one
weight/value pair is required.

The DSL compiler can use this to validate calls:
`weighted_pick(hash(cycle), 0.5, 10, 0.3, 20)` — valid (2 pairs).
`weighted_pick(hash(cycle), 0.5)` — invalid (incomplete pair).

---

## Tests

| Test | Verifies |
|------|----------|
| `weighted_pick_valid_outputs` | Only declared values appear in output |
| `weighted_pick_respects_weights` | High-weight values dominate statistically |
| `weighted_pick_single_pair` | Degenerate case: always returns the one value |
| `weighted_pick_equal_weights` | Uniform distribution when all weights equal |
| `weighted_pick_compiled_matches_eval` | Phase 2 closure agrees with Phase 1 eval |
| `weighted_pick_slot_consistency` | Slot metadata is well-formed and complete |
| `weighted_pick_equivalence_with_weighted_u64` | FusedNode contract: matches WeightedU64 on 10K seeds |
| `weighted_pick_metadata_complete` | All ins/outs/wire_inputs/const_slots correct |
