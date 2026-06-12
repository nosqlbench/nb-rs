# Polydat Type System

The static `PortType` contract on wires, its runtime
`Value` representation, and the adapter catalog that
moves values between types.

Implementation: `polydat/src/ast.rs` (`PortType`,
`Value`), `polydat/src/library/convert.rs` (adapter
nodes), `polydat/src/compile/assembly.rs::auto_adapter`
(catalog dispatch), `polydat/src/kernel/state.rs::adapt_boundary_value`
(boundary application).

---

## 1. PortType — the static wire contract

`PortType` is the type a wire carries from producer to
consumer. Every `Port` (input or output) on every node
declares one. The assembler validates that producer
and consumer agree, inserting auto-adapter nodes from
the catalog where it can heal a mismatch.

| Variant | Width | Storage at runtime | Notes |
| --- | --- | --- | --- |
| `U64`     | 64-bit unsigned | `Value::U64(u64)` | Workhorse: hash outputs, counters, primary keys |
| `I64`     | 64-bit signed   | `Value::I64(i64)` | **Honest signed carrier** (alignment §5) — display/JSON render negatives correctly |
| `U32`/`U16`/`U8` | narrow unsigned | `Value::U64` (zero-extended) | Widen to `U64` automatically |
| `I32`/`I16`/`I8` | narrow signed | `Value::I64` (sign-extended) | Widen to `I64` automatically |
| `F64`     | 64-bit float    | `Value::F64(f64)` | IEEE 754 double |
| `F32`     | 32-bit float    | `Value::U64` (as `f32::to_bits() as u64`) | Bit-stuffed; widens to `F64` |
| `F16`     | 16-bit float    | `Value::U64` (as `f16::to_bits() as u64`) | binary16; widens exactly to F32/F64 |
| `U128`/`I128` | 128-bit int | `Value::U128`/`I128(Bits128)` | Two u64 limbs (keeps `Value` at align 8); interpreter-only |
| `Bool`    | logical          | `Value::Bool(bool)` | Distinct runtime variant |
| `Str`     | UTF-8 string     | `Value::Str(Arc<str>)` | Cheap-clone via Arc |
| `Bytes`   | raw bytes        | `Value::Bytes(Arc<[u8]>)` | Cheap-clone via Arc |
| `Json`    | serde_json Value | `Value::Json(Arc<serde_json::Value>)` | Cheap-clone via Arc |
| `Ext`     | adapter-contributed | `Value::Ext(Box<dyn ReflectedValue>)` | UUIDs, timestamps, IPs |
| `Handle`  | type-erased Arc  | `Value::Handle(Arc<dyn Any + Send + Sync>)` | Datasets, prepared stmts |
| `VecF32`/`VecF64`/`VecF16` | float lanes | `Value::Vec*(SliceArc<T>)` | Typed slices; native CQL vector binding |
| `VecI8`/`VecI16`/`VecI32`/`VecI64` | int lanes | `Value::Vec*(SliceArc<T>)` | Complete cranelift lane family (alignment §8.2) |

The scalar-width set is the full cranelift scalar vocabulary
(every integer width in both signednesses, f16/f32/f64) minus
F128, which stable Rust cannot carry; the vector element set is
every cranelift lane type with a JSON Number projection. See
`type_system_alignment.md` for the derivation and §8 for the
full-scope model.

**Three classes of types** group the variants:

- **Numeric bit-stuffed** (`U64`, `U32`, `U16`, `U8`,
  `I64`, `I32`, `I16`, `I8`, `F64`, `F32`, `F16`) —
  runtime storage is `Value::U64`, `Value::I64`
  (signed widths — the honest carrier), or
  `Value::F64`; the `PortType` declares how the bits
  should be interpreted. Bit-stuffing is what makes
  JIT P3 cheap: every narrow width fits in one
  `compiled_u64` slot (unsigned zero-extend, signed
  sign-extend, floats by `to_bits`).
- **Two-limb 128-bit** (`U128`, `I128`) — carried as
  `Bits128([u64; 2])` so `Value` keeps alignment 8;
  interpreter-only until a two-slot JIT ABI exists.
- **Heap-cheap-clone** (`Str`, `Bytes`, `Json`, and
  the `Vec*` lane family) — runtime storage is an
  Arc-backed handle. Clone cost is one atomic
  increment; no allocation, no deep-copy. Multiple
  consumer fibers share the underlying allocation by
  Arc.
- **Type-erased** (`Ext`, `Handle`) — runtime carries
  an opaque handle the producer minted and the
  consumer downcasts. Adapters that produce these
  attach a `ReflectedValue` impl (Ext) or rely on
  consumer downcast (Handle).

---

## 2. Value — the runtime representation

`Value` has fewer variants than `PortType` because
the bit-stuffed numeric forms share two storage
slots:

```rust
pub enum Value {
    U64(u64),                          // also stores U32/U16/U8 (zero-extended)
    I64(i64),                          // honest signed carrier; also I32/I16/I8 (sign-extended)
    U128(Bits128), I128(Bits128),      // two u64 limbs (align-8 envelope)
    F64(f64),                          // also stores F32/F16 bits via to_bits()
    Bool(bool),
    Str(Arc<str>),
    Bytes(Arc<[u8]>),
    Json(Arc<serde_json::Value>),
    Ext(Box<dyn ReflectedValue>),
    Handle(Arc<dyn Any + Send + Sync>),
    VecF32(SliceArc<f32>), VecF64(SliceArc<f64>), VecF16(SliceArc<half::f16>),
    VecI8(SliceArc<i8>), VecI16(SliceArc<i16>),
    VecI32(SliceArc<i32>), VecI64(SliceArc<i64>),
    None,                              // sentinel for absent / uninit
}
```

Note the floats: an `F32`-typed *node output* carries its bit
pattern in `Value::U64` (the macro's `IntoValue for f32`), while
a host-written `F32` slot value may arrive as `Value::F64`;
`satisfies_slot` accepts both. `F16` follows the same dual
convention.

`Value::None` is the *absent* sentinel. It appears in
freshly-allocated buffer slots before first
evaluation and as the "no value yet" marker for
optional ports. Per SRD-74 it propagates through node
evaluation: any node whose inputs include `None`
emits `None` on every output unless it explicitly
opts in via `GkNode::accepts_none_inputs()`.

`Value::port_type()` reports the *runtime variant's*
PortType, which collapses the narrow widths into their
carriers — U32/U16/U8 → U64, I32/I16/I8 → I64, F32/F16
→ their stuffed carrier — (or returns `PortType::U64`
for `None` as a placeholder). The static slot's declared `PortType`
is the canonical type contract; `Value::port_type()`
is only used at boundary check sites
(`adapt_boundary_value`) to detect mismatches.

---

## 3. Adapter catalogs

Two catalogs live in
`polydat/src/compile/assembly.rs`:

```rust
pub fn auto_adapter(from: PortType, to: PortType) -> Option<Box<dyn GkNode>>;
pub fn boundary_adapter(from: PortType, to: PortType) -> Option<Box<dyn GkNode>>;
```

- **`auto_adapter`** — intra-graph wire validation.
  Consulted by the assembler when a producer node's
  output `PortType` differs from a consumer node's
  input `PortType`. Strict: never returns an adapter
  that can panic on unparseable input. The intent is
  that any wire mismatch the catalog cannot heal is
  an author-detectable compile-time error.
- **`boundary_adapter`** — host-boundary writes via
  `adapt_boundary_value`. A strict superset of
  `auto_adapter`: delegates to it first, then adds
  boundary-only parser adapters (`Str→{Bool, U64,
  F64}`) for the workload-param flow where the
  source is intrinsically textual.

When either function returns `Some(node)`, the wire
chain inserts that adapter node. When it returns
`None`, the boundary surfaces
`WriteError::TypeMismatch` (or an assembler error at
compile time) with `from` and `to` in the
diagnostic.

The matrix below shows the original 12 core types
(identity diagonal excluded). Cells are marked **A** (in
`auto_adapter`, intra-graph), **B** (in `boundary_adapter`
only), or **·** (not in any catalog — fails with
`TypeMismatch`).

The narrow widths (`u8`/`i8`/`u16`/`i16`/`f16`) and the
128-bit integers extend the matrix by the same family
rules — each mirrors its wider sibling's row/column
(u8/u16 ↔ u32, i8/i16 ↔ i32, f16 ↔ f32; u128/i128 widen
from the 64-bit carriers, project to JSON as decimal
strings, and serde to exactly-16 LE bytes). Their
adapters live in `library/polyfill_narrow.rs` and
`library/polyfill_128.rs`; the catalog functions in
`compile/assembly.rs` are the cell-level source of
truth. The extended `Vec*` lane types (`vec_f64`,
`vec_i64`, `vec_f16`, `vec_i16`, `vec_i8`) currently
have no adapter rows — like their wider siblings, they
move between types via explicit nodes only.

```
            ┌──────────────────────────── to ────────────────────────────┐
   from →   U64  U32  I64  I32  F64  F32  Bool  Str  Bytes  Json  VecF32 VecI32
   ─────   ───  ───  ───  ───  ───  ───  ────  ───  ─────  ────  ─────  ─────
   U64      ─    B    B    B    A    B    A    A    A      A     ·      ·
   U32      A    ─    B    B    A    B    A    A    A      A     ·      ·
   I64      B    B    ─    B    A    B    A    A    A      A     ·      ·
   I32      B    B    A    ─    A    B    A    A    A      A     ·      ·
   F64      B    B    B    B    ─    B    A    A    A      B     ·      ·
   F32      B    B    B    B    A    ─    A    A    A      B     ·      ·
   Bool     A    A    A    A    A    A    ─    A    A      A     ·      ·
   Str      B    B    B    B    B    B    B    ─    B      B     B      B
   Bytes    B    B    B    B    B    B    B    B    ─      B     B      B
   Json     B    B    B    B    B    B    B    A    B      ─     B      B
   VecF32   ·    ·    ·    ·    ·    ·    ·    B    A      B     ─      B
   VecI32   ·    ·    ·    ·    ·    ·    ·    A    A      A     A      ─

   A = always-defined (auto_adapter, intra-graph + boundary)
   B = can panic on input (boundary_adapter only — keeps intra-graph strict)
   · = not in catalog; Vec → scalar fails with hint pointing at vec_len/vec_first/…
   ─ = identity (no adapter needed)
```

**Class A — always-defined** (lossless or fully-
defined for every valid runtime input):

- **Numeric widening** — `U32→U64`, `I32→I64`,
  `F32→F64`, `U64→F64`, `U32→F64`, `I32→F64`,
  `I64→F64`.
- **X → Str** — every numeric type plus `Bool` and
  `Json` render as a string via Display.
- **Bool ↔ numeric** — 1/0 mapping in both
  directions; nonzero test on numerics; never
  panics.
- **X → Bytes** — little-endian serialize for every
  numeric, Bool, and Vec source; always succeeds.
- **X → Json** — wraps as the corresponding
  `Json::Number` / `Json::Bool` / `Json::Array`.
  Integer and Bool sources never panic; VecI32 is
  in this class because `i32` is always
  representable. `F64→Json`, `F32→Json`, and
  `VecF32→Json` are class B because non-finite
  floats are not representable in standard JSON.
- **VecI32 → VecF32** — lossless cast of every
  element.

**Class B — can panic on input** (lossy, parseable,
or shape-checking):

- **Numeric narrowings + non-widening casts** —
  `U64→{U32, I64, I32, F32}`, `I64→{U64, U32, I32,
  F32}`, `F64→{U64, U32, I64, I32, F32}`, etc.
  Range-checked; panic on out-of-range. Float→int
  also panics on NaN/Inf.
- **Str → X parsers** — `Str→{Bool, U64, U32, I64,
  I32, F64, F32, Bytes, Json, VecF32, VecI32}`.
  Trim + parse; panic on unparseable. Workload-
  param flow (YAML interpolation, comma-split iter-
  values) lives here.
- **Bytes → X parsers** — length-checked, little-
  endian decode. Numeric targets require exactly
  `sizeof(N)` bytes; Vec targets require a multiple
  of `sizeof(element)`; panic on wrong length.
- **Json → X extractors** — shape-checked.
  `Json::Number` expected for numerics,
  `Json::Bool` for Bool, `Json::Array` for Vec,
  `Json::String` (hex) for Bytes. Panic on
  mismatch.
- **`F64→Json`, `F32→Json`, `VecF32→Json`,
  `VecF32→Str`** — class B because
  `serde_json::Number::from_f64` rejects non-finite
  floats; the adapter panics with a useful
  diagnostic.
- **`VecF32→VecI32`** — round each element; panic
  on non-finite or out-of-range.

**Class · — not in either catalog** (intentional
exclusion):

- **Vec → scalar** — `VecF32→{numeric, Bool}` and
  `VecI32→{numeric, Bool}` are excluded because no
  single collection-to-scalar convention is natural
  (first? last? length? sum? mean?). When the
  boundary rejects this pair, `WriteError::TypeMismatch`
  appends a hint pointing at the explicit helpers:
  `vec_len(v)` for the element count,
  `vec_first(v)` / `vec_last(v)` for an element,
  `vec_sum(v)` / `vec_mean(v)` for an aggregate.
- **`Ext` / `Handle`** — never in the auto-adapter
  catalog. `Ext` exposes typed access via
  `ReflectedValue::try_as_str` etc. at consume
  sites; `Handle` is downcast by the consumer node
  to the concrete type. The matrix above doesn't
  list these rows / columns.

### 3.1 Bytes conventions

| Axis | Convention |
| --- | --- |
| Numeric ↔ Bytes | little-endian, exactly `sizeof(N)` bytes (panic on wrong length) |
| Bool ↔ Bytes | 1 byte (`0x01` / `0x00`) |
| Vec ↔ Bytes | little-endian element bytes; length must be a multiple of `sizeof(element)` |
| Bytes ↔ Str | lowercase hex (`data_encoding::HEXLOWER`) — roundtrip-lossless, unambiguous, JSON-safe |
| Bytes ↔ Json | `Json::String` holding the lowercase hex |

**Little-endian** matches native CPU layout
(x86_64, ARM64) and the binary protocols this
substrate adapts to (CQL `vector<float, N>`, Postgres
binary). Authors who need big-endian byte order use
explicit `pack_u64_be` / `unpack_u64_be` nodes
(not provided by polyfill).

**Lowercase hex** for Bytes ↔ Str round-trip avoids
the URL-safety question entirely and reads
unambiguously in logs. Compact base64 encoding is
available as an explicit `base64_encode` node when
size matters.

### 3.2 Str → Json convention: try-parse-or-error-wrap

`Str→Json` attempts `serde_json::from_str`. Well-
formed JSON parses through:

- `"{"a":1}"` → `Json::Object({"a": 1})`
- `"[1, 2, 3]"` → `Json::Array([1, 2, 3])`
- `"\"hello\""` → `Json::String("hello")`
- `"42"` → `Json::Number(42)`

Malformed JSON wraps in a structured error rather
than panicking, so the substrate never silently
loses the original content:

```json
{
  "error": "invalid JSON",
  "message": "expected value at line 1 column 1",
  "raw": "{bad json"
}
```

Workload authors who pull the resulting `Json`
downstream see the wrapped error and can branch on
its shape. This matches the substrate's general
posture of "fail loud with useful context, don't
panic when the upstream might be human-typed."

---

## 4. Why the `auto_adapter` / `boundary_adapter` split

Intra-graph wire validation should be **strict**:
when an author writes `s := "LATENCY"; overscan := s << 2`
the substrate should reject it at compile time, not
auto-insert a parser that runtime-panics on
"LATENCY". The intra-graph catalog (`auto_adapter`)
therefore holds only **class A** adapters from §3 —
adapters whose `eval` is total over the input
domain.

The host boundary (`adapt_boundary_value`) needs
**permissive** healing: workload-param values arrive
as `Value::Str` (YAML interpolation, comma-split
iter-values) and need to coerce into typed slots
the workload author declared as `Bool`, `U64`,
`F64`, etc. The boundary catalog (`boundary_adapter`)
is a strict superset of `auto_adapter` plus every
**class B** adapter — narrowings, parsers, shape-
checking extractors, non-finite-float refusers. The
panic-on-input semantics is appropriate at the
boundary because the Str source IS data the host
explicitly fed in, and "parse or panic with a
useful diagnostic" matches user expectation.

`Value::satisfies_slot(slot_type)` is the bit-
stuffing equivalence helper the residual check
uses post-adapter: `Value::U64` storage is accepted
for the unsigned widths, `F16`, and (legacy, during
the honest-I64 migration) the signed widths;
`Value::I64` for `I64`/`I32`/`I16`/`I8` slots;
`Value::F64` for `F64`/`F32`/`F16`. This lets
narrowing adapters that output the bit-stuffed
runtime form (e.g. `__u64_to_u32` produces
`Value::U64` with low 32 bits) pass validation for
the narrower slot type without a separate Value
variant per port type. The pre-adapter strict
check `value.port_type() == slot_type` keeps an
unadapted `Value::U64` from silently truncating
into a U32 slot — the adapter MUST run.

---

## 5. Explicit conversion nodes (outside the catalog)

Nodes that perform deliberately lossy, formatted, or
parameterized conversions are NOT in the auto_adapter
catalog. Users place them deliberately by name.
Defined in `polydat/src/library/convert.rs`:

| Node | Signature | Semantics |
| --- | --- | --- |
| `f64_to_u64` | `(f64) → u64` | Truncating cast (`as u64`) |
| `round_to_u64` | `(f64) → u64` | Round half-up to nearest |
| `floor_to_u64` | `(f64) → u64` | Truncate down |
| `ceil_to_u64` | `(f64) → u64` | Round up |
| `discretize` | `(input, range, buckets) → u64` | Bin a continuous f64 into `buckets` equal-width buckets over `[0, range)`; clamps out-of-range |
| `format_u64` | `(u64, base) → str` | Hex / binary / decimal rendering with prefix |
| `format_f64` | `(f64, precision) → str` | Fixed-precision decimal |

The principle: **if the user has to choose a rounding
mode, a base, a precision, a base unit, or a
truncation policy, the node is explicit**. The
auto-adapter catalog only contains conversions where
there is no semantic choice (widening, to-string via
Display, Bool ↔ U64 as 1/0).

---

## 6. Boundary adapter application

Two call sites apply the catalog:

### 6.1 Assembler — intra-graph wire validation

`compile::assembly::resolve` walks every wire after
parse, compares the producer's output `PortType` to
the consumer's input `PortType`, and:

- Equal types → wire it through, no adapter.
- Mismatch with a catalog entry → insert the adapter
  node inline, rewriting the wire to route
  `producer → adapter → consumer`.
- Mismatch with no catalog entry → fail construction
  with `AssemblyError::TypeMismatch` carrying both
  port types and the offending wire path.

This runs once at kernel construction; the resulting
program has no remaining mismatches.

### 6.2 Boundary — `adapt_boundary_value`

`polydat/src/kernel/state.rs::adapt_boundary_value`
applies the catalog at runtime mismatches where the
assembler has no view — outer scope values
crossing into inner kernels via the `set:` /
`bindings:` lowering, host writes via
`Dataflow::set_wire_idx`, etc. The helper:

1. Compares incoming `Value`'s `port_type()` to the
   slot's declared `PortType`.
2. Equal → returns the value unchanged.
3. Mismatch → consults `boundary_adapter(from, to)`
   (which checks `auto_adapter` first, then the
   boundary-only Str→X parsers from §4), runs the
   adapter's `eval` against the value, and returns
   the result.
4. No catalog entry → returns the value unchanged
   with a one-line audit warning; the caller's
   `set_wire_idx` then detects the residual type
   mismatch and surfaces `WriteError::TypeMismatch`
   to the host (scope-init code in
   `nbrs-activity/src/synthesis.rs::apply_scope_values`
   re-raises this as a `panic!`).

---

## 7. Wire-type fusion (open question)

A separate design question — orthogonal to the
catalog gap — that the `eh` case surfaces.

Today the DSL parser
(`polydat/src/dsl/parser.rs::parse_interpolated_string`)
turns every interpolated string literal into a
`printf` call:

| Source | Desugared |
| --- | --- |
| `"hello"`           | `Expr::StringLit("hello")` |
| `"hello {name}"`    | `printf("hello {}", name)` |
| `"{a}-{b}"`         | `printf("{}-{}", a, b)` |
| `"{eh}"`            | `printf("{}", eh)`  ← **always Str** |

The sole-placeholder case `"{eh}"` is
syntactically a string template, but semantically
the author's intent is "pass `eh` through" — they
want the typed value of `eh`, not a string-formatted
copy. The current behavior wraps it in `printf`,
which always produces `Str`, losing whatever type
`eh` had.

If the parser detected sole-placeholder templates
and desugared `"{X}"` (whole literal = one
placeholder) to `X` directly (preserving type), the
`eh` case would route a Bool through to the
`enable_hierarchy: Bool` slot with no adapter
needed.

**Fusion vs polyfill are complementary, not
alternatives**:

- Fusion fixes the sole-placeholder case at parse
  time (no Str ever produced; no adapter needed).
- The Str→X polyfill adapters fix the genuinely-Str
  case — comma-split iter-values, multi-placeholder
  templates, host-supplied strings — where the
  source is intrinsically textual.

Even with fusion, comma-split iter-values like
`eh_values: "false, true"` would still produce
`Str("false")` iter-values; the polyfill is what
heals them at the slot boundary. Fusion would
prevent the *unnecessary* Str detour when the source
is already typed.

Fusion is not implemented; the parser comment at
lines 655-664 of `dsl/parser.rs` documents the
current "every placeholder → printf" rule. Adding
fusion is one branch in `parse_interpolated_string`:
if `segments.len() == 1 && matches!(segments[0],
Segment::Placeholder(_))`, return the placeholder
expression directly instead of building the printf
call.

---

## 8. Cross-references

- [composition_substrate.md] §T1, §T2, §T3 — the
  typed-slot axioms this catalog enforces.
- [`polydat/src/library/convert.rs`] — adapter node
  implementations.
- [`polydat/src/compile/assembly.rs::auto_adapter`]
  — catalog dispatch.
- [`polydat/src/kernel/state.rs::adapt_boundary_value`]
  — boundary-time application.
- [`polydat/src/kernel/api_impl.rs::set_wire_idx`]
  — the typed Dataflow write surface that surfaces
  `WriteError::TypeMismatch` when the catalog can't
  heal.
- [`polydat/src/dsl/parser.rs::parse_interpolated_string`]
  — current "every placeholder → printf" desugar
  (the wire-type-fusion question's locus).
- [SRD-74](none_semantics.md) — `Value::None`
  propagation, the absent-sentinel rule.

[composition_substrate.md]: composition_substrate.md
[`polydat/src/library/convert.rs`]: ../../src/library/convert.rs
[`polydat/src/compile/assembly.rs::auto_adapter`]: ../../src/compile/assembly.rs
[`polydat/src/kernel/state.rs::adapt_boundary_value`]: ../../src/kernel/state.rs
[`polydat/src/kernel/api_impl.rs::set_wire_idx`]: ../../src/kernel/api_impl.rs
[`polydat/src/dsl/parser.rs::parse_interpolated_string`]: ../../src/dsl/parser.rs
