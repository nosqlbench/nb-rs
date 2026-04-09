# MEMO: Annotated Implementation of HashRange

A walkthrough of the `HashRange` node in `nb-variates/src/nodes/hash.rs`.
This node exercises five metadata features: wire input, constant input,
Phase 2 compiled closure, Phase 3 JIT constants, and a fusion equivalence
contract. It is the most feature-complete single node in the codebase.

---

## The Struct

```rust
pub struct HashRange {
    meta: NodeMeta,
    max: u64,
}
```

Two fields:

- `meta` — the node's interface metadata (inputs, outputs). Every
  GK node carries this.
- `max` — the upper bound for the output range `[0, max)`. This is
  an assembly-time constant: set once at construction, immutable
  thereafter. It appears in three places: as a struct field (for
  `eval()`), in `meta.ins` as a `Slot::Const` (for metadata
  visibility), and in the `compiled_u64()` closure (captured by
  value). The struct field and the closure capture are the runtime
  representations; the slot is the inspectable metadata form.

---

## The Constructor

```rust
impl HashRange {
    pub fn new(max: u64) -> Self {
        Self {
            meta: NodeMeta {
                name: "hash_range".into(),
                ins: vec![
                    Slot::Wire(Port::u64("input")),
                    Slot::const_u64("max", max),
                ],
                outs: vec![Port::u64("output")],
            },
            max,
        }
    }
}
```

### `name: "hash_range"`

The function name as it appears in the DSL. Users write
`hash_range(cycle, 1000)`. The fusion pass also matches against
this name when deciding whether a subgraph has been fused.

### `ins` — two slots in positional order

```rust
ins: vec![
    Slot::Wire(Port::u64("input")),    // position 0: wire
    Slot::const_u64("max", max),       // position 1: constant
]
```

The positional order matches the DSL call syntax:
`hash_range(input, max)`. This is enforced by convention — the
DSL compiler maps argument positions to slots.

**`Slot::Wire(Port::u64("input"))`** — the first argument is a
runtime wire. During each evaluation cycle, this receives a `u64`
value from the upstream node (typically a coordinate or hash output).

**`Slot::const_u64("max", max)`** — the second argument is an
assembly-time constant. The convenience constructor expands to:
```rust
Slot::Const {
    name: "max".into(),
    value: ConstValue::U64(max),
}
```
This makes the constant visible in metadata: the fusion pass can
read it via `meta().const_slots()`, the JIT can extract it via
`meta().jit_constants_from_slots()`, and describe/diagnostic
tooling can display it.

### `outs: vec![Port::u64("output")]`

One output port of type `u64`. The result is always in `[0, max)`.

### `max` (struct field)

The same value as in `Slot::const_u64("max", max)`, stored
separately for direct access in `eval()` and `compiled_u64()`.
This is the performance path — reading a struct field is cheaper
than searching the `ins` vec. The slot carries the value for
metadata consumers; the struct field carries it for evaluation.

---

## The GkNode Trait Implementation

```rust
impl GkNode for HashRange {
    fn meta(&self) -> &NodeMeta {
        &self.meta
    }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let v = inputs[0].as_u64();
        let h = xxh3_64(&v.to_le_bytes());
        outputs[0] = Value::U64(h % self.max);
    }

    fn compiled_u64(&self) -> Option<CompiledU64Op> {
        let max = self.max;
        Some(Box::new(move |inputs, outputs| {
            outputs[0] = xxh3_64(&inputs[0].to_le_bytes()) % max;
        }))
    }

    fn jit_constants(&self) -> Vec<u64> { vec![self.max] }
}
```

### `fn meta(&self) -> &NodeMeta`

Returns the metadata. Used by assembly (arity validation, type
checking), fusion (pattern matching, constant extraction), JIT
(node classification), and visualization.

### `fn eval(&self, inputs: &[Value], outputs: &mut [Value])`

**Phase 1** evaluation. Called through dynamic dispatch (`dyn GkNode`)
when the kernel runs in interpreter mode.

- `inputs[0].as_u64()` — extracts the u64 from the `Value` enum.
  Only wire inputs appear in `inputs[]`; constants are baked into
  the struct. The assembly phase guarantees `inputs` has exactly
  `wire_inputs().len()` elements (here, 1).
- `xxh3_64(&v.to_le_bytes())` — hashes the input using xxHash3.
  Converts the u64 to 8 little-endian bytes first (xxh3 operates
  on byte slices).
- `h % self.max` — modular reduction to `[0, max)`.
- `Value::U64(...)` — wraps the result for the output port.

Cost: ~70 ns baseline + ~5 ns for hash + mod. The `Value` enum
wrapping/unwrapping and dynamic dispatch are the overhead that
Phase 2 eliminates.

### `fn compiled_u64(&self) -> Option<CompiledU64Op>`

**Phase 2** compiled closure. Returns `Some(...)` because this node
operates entirely in u64 space (input: u64, output: u64).

```rust
let max = self.max;   // capture the constant by value
Some(Box::new(move |inputs, outputs| {
    outputs[0] = xxh3_64(&inputs[0].to_le_bytes()) % max;
}))
```

The closure captures `max` at assembly time via `move`. At runtime,
it reads/writes raw `u64` slices — no `Value` enum, no dynamic
dispatch. The closure is called through a function pointer in the
flat evaluation loop.

Key differences from `eval()`:
- No `Value` wrapping — `inputs` and `outputs` are `&[u64]` and
  `&mut [u64]` directly.
- No `self` — the closure is detached from the struct. All state
  is captured by value at construction time.
- Called in a flat loop over all nodes in topological order, not
  through pull-through recursion.

Cost: ~4.5 ns/node baseline. The xxh3 call dominates.

### `fn jit_constants(&self) -> Vec<u64>`

Exposes constants for **Phase 3** JIT compilation. The JIT
classifier (`jit.rs::classify_node`) reads this to determine
the `JitOp` variant:

```rust
"hash_range" => {
    if let Some(&max) = consts.first() {
        JitOp::HashRangeConst(max)
    } else { JitOp::Fallback }
}
```

The JIT then emits Cranelift IR that bakes `max` as an immediate
operand in the generated machine code — no closure, no function
pointer, no buffer slot for the constant.

This method is transitional: eventually `meta().jit_constants_from_slots()`
will replace it, deriving the same `Vec<u64>` from the typed
`ConstValue` in the slot metadata. Both paths produce identical
output — verified by the `slot_constants_match_jit_constants` test
in `arithmetic.rs`.

### Methods NOT overridden (using defaults)

- `fn commutativity(&self) -> Commutativity` — defaults to
  `Positional`. Correct: `hash_range(x, K)` has one wire input
  and one constant, so commutativity is vacuous.

---

## The FusedNode Implementation

```rust
impl FusedNode for HashRange {
    fn decomposed(&self) -> DecomposedGraph {
        use crate::nodes::arithmetic::ModU64;
        let mut g = DecomposedGraph::new(1);
        let h = g.add_node(
            Box::new(Hash64::new()),
            vec![DecomposedWire::Input(0)],
        );
        let m = g.add_node(
            Box::new(ModU64::new(self.max)),
            vec![DecomposedWire::Node(h, 0)],
        );
        g.set_outputs(vec![DecomposedWire::Node(m, 0)]);
        g
    }
}
```

### What this declares

`HashRange` claims: "I am semantically equivalent to `mod(hash(x), K)`."
The `decomposed()` method builds a mini-DAG representing this
unfused form:

```
Input(0) → Hash64 → ModU64(max) → Output
```

### How it's tested

The equivalence property test in `fusion.rs` calls both forms on
10,000 random inputs and asserts bit-exact agreement:

```rust
#[test]
fn hash_range_equivalence() {
    for max in [1, 2, 7, 100, 10_000, u64::MAX] {
        let fused = HashRange::new(max);
        assert_equivalence(&fused, 10_000);
    }
}
```

`assert_equivalence` evaluates the fused node directly and the
decomposed graph independently, comparing outputs. If someone
changes the hash algorithm or the modular reduction and forgets
to update the other, this test catches it.

### How it connects to fusion

The fusion pass has a rule that recognizes the decomposed pattern
and replaces it with the fused form:

```rust
FusionRule {
    name: "hash_mod_to_hash_range",
    pattern: FusionPattern::node(
        "mod",
        vec![
            FusionPattern::node(
                "hash",
                vec![FusionPattern::any("x")],
                "hash_node",
            ),
        ],
        "mod_node",
    ),
    replacement: |m| {
        let max = m.const_u64("mod_node");
        Box::new(HashRange::new(max))
    },
    input_bindings: &["x"],
}
```

When a user writes `mod(hash(cycle), 100)` in their `.gk` file,
the assembly phase recognizes the `hash → mod` pattern and replaces
it with `HashRange::new(100)`. The user gets the fused node
automatically — one fewer buffer slot, one fewer node in the
evaluation loop.

The `decomposed()` contract and the fusion rule are inverses:
- `decomposed()` says: "HashRange = mod(hash(x), K)"
- The fusion rule says: "mod(hash(x), K) = HashRange"
- The equivalence test proves both directions agree.

---

## Summary: Feature Coverage

| Feature | How HashRange uses it |
|---------|----------------------|
| Wire input | `Slot::Wire(Port::u64("input"))` in `ins` |
| Constant input | `Slot::const_u64("max", max)` in `ins` |
| Phase 1 eval | `fn eval()` with `Value` enum |
| Phase 2 closure | `fn compiled_u64()` captures `max` by value |
| Phase 3 JIT constants | `fn jit_constants()` exposes `max` as `Vec<u64>` |
| Fusion equivalence | `FusedNode::decomposed()` declares `mod(hash(x), K)` |
| Fusion rule target | `hash_mod_to_hash_range` rule produces `HashRange` |
| Equivalence testing | `hash_range_equivalence` test with 10K seeds x 6 moduli |

The only feature this node does NOT exercise is `commutativity()`
override — it has one wire input, making commutativity vacuous.
For that, see `SumN`, `ProductN`, `MinN`, `MaxN` which override
`fn commutativity(&self) -> Commutativity::AllCommutative`.
