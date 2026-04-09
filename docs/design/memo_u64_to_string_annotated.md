# MEMO: Annotated Implementation of U64ToString

A walkthrough of the `U64ToString` node in `nb-variates/src/nodes/convert.rs`,
explaining how a minimal GK node is structured and how it connects to the
assembly pipeline.

---

## The Struct

```rust
pub struct U64ToString {
    meta: NodeMeta,
}
```

Every GK node is a struct that carries a `NodeMeta` describing its
interface. `U64ToString` has no parameters — no constants, no
configuration. It's a pure type conversion. The only field is `meta`,
which holds the input/output slot declarations.

Nodes with parameters (like `AddU64`) would also have fields for the
baked-in constants (e.g., `addend: u64`). This node has none.

---

## The Constructor

```rust
impl U64ToString {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "__u64_to_string".into(),
                ins: vec![Slot::Wire(Port::u64("input"))],
                outs: vec![Port::str("output")],
            },
        }
    }
}
```

### `name: "__u64_to_string"`

The double-underscore prefix is a convention for **edge adapters** —
nodes that are auto-inserted by the assembly phase, not placed by
users. The name appears in diagnostic output and DAG visualization
but is not a user-facing function name in the DSL.

### `ins: vec![Slot::Wire(Port::u64("input"))]`

The `ins` field declares all inputs in positional order. This node
has one input: a wire carrying a `u64` value. No constants.

- `Slot::Wire(Port)` — a runtime wire input that receives a value
  each evaluation cycle. The `Port` carries a name (`"input"`) and
  a type (`PortType::U64`).
- `Port::u64("input")` — convenience constructor for a u64-typed port.

If this node had a constant parameter, it would also have a
`Slot::Const { name, value }` entry. For example, `AddU64` has:
```rust
ins: vec![
    Slot::Wire(Port::u64("input")),
    Slot::const_u64("addend", addend),
]
```

### `outs: vec![Port::str("output")]`

One output port of type `String`. This is what makes this node a
type-crossing node: the input is `u64`, the output is `String`.

The assembly phase uses the input and output port types to validate
wiring. If a `u64` output feeds a `String` input and no explicit
conversion is in the graph, the assembler auto-inserts this node.

---

## The GkNode Trait Implementation

```rust
impl GkNode for U64ToString {
    fn meta(&self) -> &NodeMeta {
        &self.meta
    }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::Str(inputs[0].as_u64().to_string());
    }
}
```

### `fn meta(&self) -> &NodeMeta`

Required. Returns a reference to the node's metadata. The assembly
phase, JIT classifier, fusion pass, and visualization all read this
to understand the node's interface without evaluating it.

### `fn eval(&self, inputs: &[Value], outputs: &mut [Value])`

The Phase 1 runtime evaluation function. Called once per cycle when
this node's output is pulled.

- `inputs[0]` — the first (and only) input wire value. The assembly
  phase guarantees this is a `Value::U64(...)` because the port
  declared `PortType::U64`.
- `inputs[0].as_u64()` — unwraps the `Value` enum to get the raw
  `u64`. Panics if the variant doesn't match — but assembly-time
  type checking ensures this can't happen at runtime.
- `.to_string()` — Rust's standard decimal formatting.
- `Value::Str(...)` — wraps the result as a `Value::Str` for the
  output port.

### Methods NOT overridden (using defaults)

- `fn commutativity(&self) -> Commutativity` — defaults to
  `Positional`. Correct: this node has one input, so commutativity
  is vacuous.
- `fn compiled_u64(&self) -> Option<CompiledU64Op>` — defaults to
  `None`. This node CANNOT participate in Phase 2 compiled kernels
  because its output is `String`, not `u64`. The Phase 2 path
  requires all ports to be `u64`.
- `fn jit_constants(&self) -> Vec<u64>` — defaults to empty. No
  constants to expose.

---

## How It Gets Auto-Inserted

In `nb-variates/src/assembly.rs`, the `auto_adapter()` function maps
type mismatches to adapter nodes:

```rust
fn auto_adapter(from: PortType, to: PortType) -> Option<Box<dyn GkNode>> {
    match (from, to) {
        (PortType::U64, PortType::Str) => Some(Box::new(U64ToString::new())),
        (PortType::F64, PortType::Str) => Some(Box::new(F64ToString::new())),
        (PortType::U64, PortType::F64) => Some(Box::new(U64ToF64::new())),
        (PortType::Json, PortType::Str) => Some(Box::new(JsonToStr::new())),
        _ => None,
    }
}
```

During `resolve()`, when the assembler finds a wire from a `u64`
output port feeding a `String` input port, it calls
`auto_adapter(U64, Str)` which returns a `U64ToString`. The adapter
node is spliced into the graph between the two original nodes, with
a generated name like `__adapt_0`.

The user's `.gk` source never mentions `__u64_to_string`. The graph:

```
hash(cycle) → [u64] → weighted_strings(...)
```

becomes:

```
hash(cycle) → [u64] → __adapt_0 (__u64_to_string) → [String] → weighted_strings(...)
```

---

## Compilation Level

This node is **Phase 1 only**: it uses `Value::Str` which is
heap-allocated and variable-length. It cannot be compiled to Phase 2
(no `compiled_u64`) or Phase 3 (no JIT classification). In hybrid
mode, it runs as a Phase 2 closure fallback.

This is typical for type-crossing nodes at the boundary between the
u64 numeric core and the string/JSON output layer. The numeric core
runs at Phase 3 speed (~0.2 ns/node); the string conversion at the
boundary runs at Phase 1 speed (~70 ns). Since string formatting is
inherently expensive (heap allocation, decimal formatting), the Phase 1
overhead is negligible relative to the work being done.
