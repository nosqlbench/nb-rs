# Memo 01: Unified Parameter Model for GK Nodes

Describes the refactoring of node metadata and function signatures
from a split wire/constant model to a unified slot-based model.

---

## Problem

The GK node system had a split personality: wire inputs were first-class
in metadata (named, typed, validated), but constants were invisible.
They lived as opaque struct fields on individual nodes, surfaced only
through `jit_constants() -> Vec<u64>` — an untyped bag of bits with
no names or structure.

The metadata also had parallel type hierarchies across two structs:
`NodeMeta` (per-instance) and `FuncSig` (per-function-type) described
the same inputs using different types (`Slot`/`ConstSlot`/`ConstType`
vs `ParamSpec`/`ParamKind`), with commutativity and arity duplicated
or misplaced.

This caused six limitations:
1. No variadic constants (MixedRadix was a special case)
2. No mixed variadic (e.g., alternating weight/wire pairs)
3. No positional disambiguation (DSL split by syntax, not spec)
4. Fusion couldn't match variadic subgraphs
5. Constants weren't visible in metadata for tooling or optimization
6. Parallel type hierarchies describing the same concepts

---

## Before

### NodeMeta (node.rs) — 4 fields

```rust
pub struct NodeMeta {
    pub name: String,
    pub inputs: Vec<Port>,          // wire inputs only
    pub outputs: Vec<Port>,
    pub commutativity: Commutativity,
}
```

Constants were invisible. The only way to access them was through the
`jit_constants()` trait method, which returned an opaque `Vec<u64>`.

### FuncSig (registry.rs) — 9 fields

```rust
pub struct FuncSig {
    pub name: &'static str,
    pub category: FuncCategory,
    pub wire_inputs: usize,                            // count only
    pub const_params: &'static [(&'static str, bool)], // names, no types
    pub outputs: usize,
    pub description: &'static str,
    pub variadic: bool,                                // wire-only
    pub identity: Option<u64>,
    pub variadic_ctor: Option<fn(usize) -> Box<dyn GkNode>>,
}
```

### Parallel type hierarchies (before)

```
NodeMeta world:          FuncSig world:
  Slot::Wire(Port)         ParamKind::Wire
  Slot::Const(ConstSlot)   ParamKind::ConstU64/ConstF64/...
  ConstSlot.typ: ConstType (separate enum)
  Commutativity on NodeMeta
  Arity on NodeMeta (dead code)
  SlotKind enum
```

### Example: AddU64 (before)

```
NodeMeta:
  name: "add"
  inputs: [Port::u64("input")]       ← wire only
  outputs: [Port::u64("output")]
  commutativity: Positional

FuncSig:
  wire_inputs: 1                      ← count, no names
  const_params: [("addend", true)]    ← name + required, no type
  variadic: false

jit_constants(): [42]                 ← opaque u64, no name or type
```

---

## After

### SlotType (node.rs) — the shared vocabulary

One discriminant enum used by both `NodeMeta` (via `Slot`) and
`FuncSig` (via `ParamSpec`). Replaces `ParamKind`, `ConstType`,
and `SlotKind`.

```rust
pub enum SlotType {
    Wire,
    ConstU64, ConstF64, ConstStr, ConstVecU64, ConstVecF64,
}
```

### Slot (node.rs) — realized input, on NodeMeta

```rust
pub enum Slot {
    Wire(Port),
    Const { name: String, value: ConstValue },
}
```

With convenience constructors: `Slot::const_u64("name", v)`,
`Slot::const_f64(...)`, `Slot::const_str(...)`, etc.

### ParamSpec (registry.rs) — slot template, on FuncSig

A `Slot` without a concrete value. Uses the same `SlotType`.

```rust
pub struct ParamSpec {
    pub name: &'static str,
    pub slot_type: SlotType,
    pub required: bool,
}
```

### NodeMeta (node.rs) — 3 fields

```rust
pub struct NodeMeta {
    pub name: String,
    pub ins: Vec<Slot>,
    pub outs: Vec<Port>,
}
```

Commutativity moved to `GkNode::commutativity()` trait method
(default: `Positional`). Only `SumN`, `ProductN`, `MinN`, `MaxN`
override it.

### FuncSig (registry.rs) — 9 fields

```rust
pub struct FuncSig {
    pub name: &'static str,
    pub category: FuncCategory,
    pub outputs: usize,
    pub description: &'static str,
    pub identity: Option<u64>,
    pub variadic_ctor: Option<fn(usize) -> Box<dyn GkNode>>,
    pub params: &'static [ParamSpec],
    pub arity: Arity,
    pub commutativity: Commutativity,
}
```

Old fields removed, replaced by derived methods:
```rust
sig.wire_input_count()    // was: sig.wire_inputs
sig.is_variadic()         // was: sig.variadic
sig.const_param_info()    // was: sig.const_params
```

### Relationship: Slot is the realized ParamSpec

```
FuncSig (type-level template)     NodeMeta (constructed instance)
  ParamSpec {                       Slot::Wire(Port::u64("input"))
    name: "input",                  Slot::Const {
    slot_type: SlotType::Wire,          name: "addend",
    required: true,                     value: ConstValue::U64(42),
  }                                 }
  ParamSpec {                       ↑ same SlotType vocabulary
    name: "addend",                 ↑ but with concrete values filled in
    slot_type: SlotType::ConstU64,
    required: true,
  }
```

The only shared datum between a `NodeMeta` and its `FuncSig` is
`name` — which exists at different lifetimes (`String` vs `&'static str`)
by necessity. Everything else is complementary, not redundant.

---

## Examples

### AddU64

```
NodeMeta:
  name: "add"
  outputs: [Port::u64("output")]
  slots: [
    Wire(Port::u64("input")),
    Const { name: "addend", value: U64(42) },
  ]

FuncSig:
  params: [
    { name: "input",  slot_type: Wire,     required: true },
    { name: "addend", slot_type: ConstU64, required: true },
  ]
  arity: Fixed
  commutativity: Positional
```

### SumN (variadic wires)

```
NodeMeta:                              ← instance with 3 inputs
  name: "sum"
  outputs: [Port::u64("output")]
  slots: [Wire("in_0"), Wire("in_1"), Wire("in_2")]

GkNode::commutativity(): AllCommutative  ← trait method override

FuncSig:                               ← function signature
  params: [{ name: "input", slot_type: Wire, required: false }]
  arity: VariadicWires { min_wires: 0 }
  commutativity: AllCommutative
  identity: Some(0)
```

### MixedRadix (variadic constants)

```
NodeMeta:
  name: "mixed_radix"
  outputs: [Port::u64("d0"), Port::u64("d1"), Port::u64("d2")]
  slots: [
    Wire(Port::u64("input")),
    Const { name: "radixes", value: VecU64([100, 1000, 0]) },
  ]

FuncSig:
  params: [{ name: "input", slot_type: Wire, required: true }]
  arity: VariadicConsts { min_consts: 1 }
```

---

## What Was Eliminated

| Eliminated | Replaced By |
|-----------|-------------|
| `NodeMeta.inputs: Vec<Port>` | `NodeMeta.slots` via `wire_inputs()` |
| `NodeMeta.commutativity` | `GkNode::commutativity()` trait method |
| `NodeMeta.arity` | Dead code removed; arity lives on `FuncSig.arity` |
| `ConstSlot` struct | Inlined into `Slot::Const { name, value }` |
| `ConstType` enum | `SlotType` (shared discriminant) |
| `ParamKind` enum | `SlotType` (shared discriminant) |
| `SlotKind` enum | `SlotType` |
| `SigArity` enum | Renamed to `Arity` (sole arity type) |
| `FuncSig.wire_inputs` | `sig.wire_input_count()` (derived) |
| `FuncSig.const_params` | `sig.const_param_info()` (derived) |
| `FuncSig.variadic` | `sig.is_variadic()` (derived) |
| `Param` / `ParamValue` | Were unused, removed early |

## Fusion Pass

The fusion pattern matcher captures typed constants:

```
MatchResult.constants: Vec<(String, Vec<u64>)>           // u64 form (JIT compat)
MatchResult.typed_constants: Vec<(String, Vec<ConstValue>)>  // typed form
```

Binding names are `String` (not `&'static str`) to support dynamic
variadic names (`x_0`, `x_1`, ...) from `FusionPattern::VariadicNode`.

## Files Changed

| File | Nature of Change |
|------|-----------------|
| `nb-variates/src/node.rs` | `SlotType`, `ConstValue`, `Slot` (with convenience ctors), `Commutativity`; `NodeMeta` reduced to 3 fields; `GkNode` gains `commutativity()` |
| `nb-variates/src/dsl/registry.rs` | `ParamSpec` uses `SlotType`; `Arity` (was `SigArity`); `FuncSig` gains `commutativity`, loses old fields |
| `nb-variates/src/dsl/compile.rs` | `sig.is_variadic()` |
| `nb-variates/src/fusion.rs` | `node.commutativity()`, typed constants, `VariadicNode` pattern |
| `nb-variates/src/assembly.rs` | `meta().wire_inputs()` |
| `nb-variates/src/nodes/*.rs` | All ~130 nodes: `Slot::const_*()` constructors, `commutativity` overrides on 4 nodes |
| `nb-variates/src/sampling/*.rs` | Same |
| `nb-rs/src/main.rs` | Derived methods for describe output |
| `nb-web/src/{routes,graph}.rs` | Derived methods for web UI |
| `nb-activity/src/bindings.rs` | Derived methods for compile-level probing |
