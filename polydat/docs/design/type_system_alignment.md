# Type System Alignment — Cranelift Native Types × JSON AST Types

**Status: APPROVED — FULL-CRANELIFT SCOPE (datatypes branch, 2026-06-11)**

> **Scope revision (2026-06-11):** the demand-gated deferrals in the
> original brief (§6 R2/R4 triggers, §7.6) are retired by user
> directive: polydat supports **all cranelift types, including SIMD**.
> §8 below replaces those gates with the full-scope model and the
> phase plan. One exclusion stands, as a hard technical constraint
> rather than a scope choice: **F128** — stable Rust has no `f128`
> type to carry it, and cranelift 0.116.1's own F128 support is
> marked incomplete. Everything else is in.

Polydat's type system grew by accretion: scalar wires for the JIT
core, Arc-backed heap types for strings and structure, a typed-vector
family pulled in by CQL `vector<T, N>` binding, and two type-erased
escapes. This brief re-derives the type system from two external
standards and shows that polydat is already *nearly* the intersection
of them — then names the one genuine misalignment, the closure rules
that keep the set from growing arbitrarily, and the punch list to
finish the alignment.

The two standards:

1. **Cranelift native IR types** (cranelift-codegen 0.116.1) — what
   the JIT engine can hold in a register. This bounds the *fast*
   tier.
2. **serde_json AST types** (serde_json 1.0.150) — what a JSON (or
   YAML, via serde_yaml's bridge) document can express. This bounds
   the *interchange* tier: workload params, result bodies, captures,
   reports, checkpoints all cross this surface.

Companion doc: [type_system.md] describes the PortType/Value/adapter
mechanics this brief constrains. Implementation: `polydat/src/ast.rs`,
`polydat/src/derive_support.rs`, `polydat/src/compile/jit/`.

---

## 1. Standard set A — Cranelift 0.116.1 native types

Source: `cranelift-codegen-meta-0.116.1/src/shared/types.rs` (pinned
in Cargo.lock; all cranelift crates at 0.116.1).

**Scalars (9):**

| Type | Width | Status in 0.116.1 |
| --- | --- | --- |
| `I8`   | 8-bit int   | stable |
| `I16`  | 16-bit int  | stable |
| `I32`  | 32-bit int  | stable |
| `I64`  | 64-bit int  | stable |
| `I128` | 128-bit int | stable, expensive on most ISAs |
| `F16`  | binary16    | **work-in-progress, incomplete** |
| `F32`  | binary32    | stable |
| `F64`  | binary64    | stable |
| `F128` | binary128   | **work-in-progress, incomplete** |

**Vectors:** every lane type above × power-of-two lane counts
(2..256), as fixed SIMD types (`F32X4`, `I64X2`, …) and dynamic
variants. Reference types (`R32`/`R64`) are removed in this version.

Two properties of this set matter for alignment:

- **Integers are sign-agnostic.** Cranelift has `I64`, not
  `i64`/`u64`. Signedness lives in the *operation* (`sdiv`/`udiv`,
  `sextend`/`uextend`, signed/unsigned `icmp` codes), not in the
  type. A static type tag that declares interpretation over shared
  64-bit storage — exactly polydat's `PortType`-over-`Value::U64`
  bit-stuffing scheme — *is* the cranelift model.
- **There is no string, no map, no null, no bytes.** Anything
  heap-shaped is a pointer plus convention. Cranelift bounds the
  scalar tier only.

What polydat's JIT actually uses today: `I8` (bool load/const),
`I64` (everything pointer/integer), `F64` — three of the nine
scalars, no vectors (`compile/jit/codegen.rs`).

## 2. Standard set B — serde_json 1.0.150 AST types

Source: `serde_json-1.0.150/src/value/mod.rs`, `src/number.rs`.

```rust
pub enum Value {
    Null,
    Bool(bool),
    Number(Number),        // N::PosInt(u64) | N::NegInt(i64) | N::Float(f64)
    String(String),
    Array(Vec<Value>),
    Object(Map<String, Value>),
}
```

Six surface variants; `Number` is internally three, so the leaf
scalar set is **{null, bool, u64, i64, f64, string}** plus two
recursive shapes **{array, object}**. The `arbitrary_precision`
feature (Number-as-string) is not enabled anywhere in this
workspace and is out of scope.

Two properties matter for alignment:

- **Signedness is in the value.** `N::NegInt(i64)` vs
  `N::PosInt(u64)` is decided by the number's actual sign, per
  value, at runtime. This is the opposite placement from cranelift —
  and both placements are correct for their tier (see §5).
- **Arrays are heterogeneous.** JSON has one array type; "vector of
  f32" is a *convention* over `Array(Number…)`. Polydat's typed
  `Vec*` family is a specialization of this shape, not a new type.

YAML is not a third standard: `nbrs-workload/src/parse.rs` parses
workload YAML through serde_yaml directly *into*
`serde_json::Value`. The JSON AST is already the single interchange
model for both formats.

## 3. Current polydat inventory

| Layer | Variants | Count |
| --- | --- | --- |
| `Value` (ast.rs:181) | U64, F64, Bool, Str, Bytes, Json, Ext, Handle, VecF32, VecI32, VecF64, VecI64, VecF16, VecI16, None | 15 |
| `PortType` (ast.rs:738) | the 15 above as static tags, plus narrow projections U32, I32, I64, F32 | 18 |
| `JitType` (ast.rs:1082) | U64, F64, Bool | 3 |
| `ConstValue` (ast.rs:1094) | U64, F64, Str, VecU64, VecF64 | 5 |

Narrow numerics (`U32`, `I32`, `I64`, `F32`) are PortType-only:
runtime storage bit-stuffs into `Value::U64` / `Value::F64`, and the
static tag declares interpretation ([type_system.md] §1).

## 4. Overlap analysis

Lay the three sets over each other at the leaf-scalar level:

```
                 cranelift 0.116.1        serde_json 1.0.150
                 ┌────────────────────────┬──────────────────────┐
                 │  I8 I16 I32 I128       │                      │
                 │  F32 (F16) (F128)      │                      │
                 │                        │                      │
                 │      ┌─────────────────┼────────┐             │
                 │      │   I64 (=u64+i64 bits)    │             │
                 │      │   F64                    │             │
                 │      │   bool (I8 0/1 ↔ Bool)   │             │
                 │      └─────────────────┼────────┘             │
                 │                        │  null                │
                 │  lanes × counts (SIMD) │  string              │
                 │                        │  array, object       │
                 └────────────────────────┴──────────────────────┘
                            outside both: bytes, Ext, Handle
```

**The intersection is exactly polydat's JIT core.** {64-bit int,
f64, bool} = `JitType` = the three cranelift types codegen.rs emits
= JSON's `Number` + `Bool`. This is not a coincidence to preserve by
accident; it is the alignment invariant: **a type is JIT-eligible
iff it is in CL ∩ JSON.** Null is the near-miss — JSON-expressible,
JIT-inexpressible — which is precisely why `Value::None` gates a
kernel to the interpreter tier today (SRD-74).

**The typed-vector family is the bridge, not a third axis.** Every
`Vec*` element type — f16, f32, f64, i16, i32, i64 — is (a) a
cranelift lane type and (b) JSON-projectable as a homogeneous
`Array` of `Number`. The family is the cross product
*(cranelift lanes ∩ JSON-number-projectable) × demand*, which gives
a closure rule instead of an open-ended enum (§6 R4).

**Cranelift-only types stay static.** I8/I16/I32/F32 (and I128,
F16, F128) have no JSON identity — a JSON document cannot say "this
is an i32." They therefore must not become `Value` variants; they
are static interpretations (PortType projections) of the 64-bit
runtime slots, which is also exactly how cranelift itself treats
narrow values in wide registers.

**JSON-only types stay heap.** String → `Str`; Array/Object
(heterogeneous) → `Json`; Null → `None`. Already aligned.

**Three types are in neither standard** and are kept as documented
escapes, not first-class citizens of either tier: `Bytes` (JSON
projection by lowercase-hex string convention, [type_system.md]
§3.1; cranelift-side it is "pointer + length"), `Ext` and `Handle`
(type-erased; `Handle` has *no* JSON projection — `to_json_value`
maps it to `Null` — and neither ever rides JIT).

**Verdict: the overlap is strong.** The aligned system needs no new
runtime capability tiers, no new storage classes, and removes
nothing. Re-deriving from the two standards reproduces 14 of the 15
current `Value` variants exactly and surfaces exactly one
misalignment — signed 64-bit integers — plus a handful of
consistency gaps (§7).

## 5. The one real misalignment: signed 64-bit integers

JSON's leaf scalar set includes **i64 as a distinct value-level
type** (`N::NegInt`). Polydat carries i64 only as a *static*
interpretation: `PortType::I64` over `Value::U64(i64 as u64)`.

Cranelift-side this is free — sign-agnostic I64 means the bits are
identical either way. JSON-side it is a live defect class: every
projection site that has the `Value` but not the slot's `PortType`
renders the unsigned reinterpretation. Concretely, with `x` an
I64-typed wire holding `-5`:

- `to_display_string` (ast.rs:541) → `"18446744073709551611"` in
  templates, plot labels, diagnostics.
- `to_json_value` (ast.rs:675) → `Number(PosInt(18446744073709551611))`
  in captures, reports, checkpoints.
- Extraction inbound: a result-body JSON field holding `-5`
  (`N::NegInt`) has no honest `Value` to land in; generic capture
  paths must choose between lossy `F64` and a bit-stuffed `U64`
  that re-triggers the outbound defect.

Two options:

- **Option A — status quo (cranelift placement only).** One 64-bit
  int `Value`, signedness static. Requires threading `PortType`
  into every display/JSON/debug projection site, forever, including
  sites (logging a bare `Value`, `Debug` derive) where no slot
  exists. The defect class is contained, never closed.
- **Option B — add `Value::I64(i64)` (both placements).** The
  `Value` enum is the dynamic/interchange representation — it
  aligns to the JSON standard, where signedness is in the value.
  `PortType` keeps the cranelift placement: `PortType::I64` slots
  now store `Value::I64` instead of stuffed `U64`; the narrow
  projections (`I32` widening into `I64`, sign-extended) are
  unchanged in spirit. At the JIT boundary the bits are identical
  (`i64 as u64` into the same u64 slot), so `JitType` gains `I64`
  as a fourth tag whose read/write is a bitcast — zero codegen
  cost, exactly like `F64`'s `to_bits` ride.

**Recommendation: Option B.** It is the only option under which
*both* standards are satisfied at the tier each one governs:
JSON-shaped honesty at the `Value`/interchange tier, sign-agnostic
bit-identity at the cranelift/JIT tier. The number extraction rule
becomes total and lossless in both directions:
`PosInt↔U64, NegInt↔I64, Float↔F64`.

Blast radius (Option B): `Value` + `PortType` storage contract,
`satisfies_slot`, the adapter matrix rows that currently special-case
I64-as-stuffed-U64 (most adapters already exist — `__i64_to_f64`,
`__i64_to_string`, `__i32_to_i64` in convert.rs produce/consume the
logical type), `to_display_string` / `to_json_value` gain honest
arms, JIT read/write picks the bitcast by `JitType::I64`,
`ConstValue` gains `I64` (§7.4), and the `Wire` impl for `i64`
flips from bit-stuffing to the new variant. Bool's lenient extract
(`U64(n != 0)`) is precedent for accepting stuffed legacy values
during migration.

## 6. The aligned model — closure rules

Four rules replace "add a variant when needed":

- **R1 — `Value` ≅ JSON AST + typed vectors + escapes.** The
  `Value` enum is the JSON leaf set {None↔Null, Bool, U64+I64↔Number,
  F64↔Number, Str↔String} plus `Json` (heterogeneous Array/Object),
  plus the R4 vector family (homogeneous Array specializations),
  plus exactly three escapes (Bytes, Ext, Handle). Every variant
  except the escapes must have a total `to_json_value` projection
  and a defined extraction from its JSON shape. No `Value` variant
  may exist that a JSON document cannot express, escapes excepted.
- **R2 — `PortType` = `Value` variants + cranelift narrow scalar
  projections.** Static-only tags are limited to cranelift-native
  scalar widths that widen losslessly into a `Value` carrier:
  today {U32, I32 → 64-bit int; F32 → F64}. {I8, I16, U8, U16} may
  join *as PortTypes* if an adapter needs the static width (CQL
  tinyint/smallint currently route via `Ext`); they never become
  `Value` variants (no JSON identity). {I128, F128} are excluded:
  no JSON identity *and* no lossless ride in a 64-bit slot.
- **R3 — `JitType` = CL ∩ JSON, riding u64 slots.** {U64, I64, F64,
  Bool} after Option B. A type becomes JIT-eligible only by being
  expressible in both standards and bit-cast-able into the
  `fn(coords: *const u64, buffer: *mut u64)` ABI. None stays
  JIT-ineligible (JSON-only); F16 scalar stays out while cranelift
  F16 is WIP; SIMD vector types stay out of scope for the kernel
  ABI (vectors are Phase-1 heap values; any future SIMD use is an
  internal codegen optimization, not a type-system surface).
- **R4 — vector element set = cranelift lanes with a JSON Number
  projection, by demand.** Today {i16, i32, i64, f16, f32, f64} —
  f16 qualifies via its f32→Number widening (ast.rs:696). u8 lanes
  are spelled `Bytes`. Candidate next members under the rule: i8
  (lane ✓, Number ✓ — admit when an adapter demands it); u64/u32
  lanes (Number ✓, but not distinct cranelift lanes from i64/i32 —
  admit as `VecU64`/`VecU32` only with a concrete signedness
  demand, since the JSON projection differs). Non-members forever:
  Vec<Str>, Vec<Json> (not cranelift lanes — spell those `Json`),
  VecI128/VecF128 (excluded by R2's rationale).

Resulting enumeration (target state):

| Tier | Types | Governed by |
| --- | --- | --- |
| Scalar core (JIT) | U64, I64, F64, Bool | CL ∩ JSON |
| Absent | None ↔ Null | JSON (interpreter-only) |
| Narrow static | U32, I32, F32 (+I8/I16/U8/U16 on demand) | CL only — PortType, never Value |
| Interchange heap | Str, Json | JSON only |
| Typed vectors | VecI16, VecI32, VecI64, VecF16, VecF32, VecF64 (+VecI8 on demand) | CL lanes × JSON Array |
| Escapes | Bytes, Ext, Handle | neither — documented conventions |

Total: 15 `Value` variants today → 16 after Option B; PortType
18 → 19. The alignment *adds one variant and closes the set* — the
payoff is not a smaller enum but a derivation: every membership
question ("do we add VecU8? F128? a Map variant?") now has a
rule-based answer instead of a judgment call.

## 7. Gaps and punch list

1. **`Value::I64` (Option B, §5).** The substantive change. Includes
   `JitType::I64`, `Wire for i64` rework, adapter-matrix update,
   honest display/JSON arms, and an audit of inbound JSON-number
   extraction sites (`json.rs::json_value_as_i32` and the capture
   paths) for `NegInt` handling.
2. **[type_system.md] is stale.** It documents 12 PortTypes /
   11 Value variants; code has 18/15 (VecF64/VecI64/VecF16/VecI16
   missing from the §3 adapter matrix and §1 table). Re-sync as part
   of this work, and fold §6's closure rules in so the doc carries
   the membership criteria, not just the inventory.
3. **serde_json `preserve_order` is inconsistent across the
   workspace.** `nbrs` and `nbrs-workload` enable it; `polydat`
   does not. Under feature unification, `cargo test -p polydat`
   builds `Map` as BTreeMap (sorted) while full-workspace builds
   get IndexMap (insertion-ordered) — `Value::Json` map iteration
   order silently differs between polydat-only tests and
   production. Declare the feature in polydat's Cargo.toml so the
   AST standard is one thing everywhere.
4. **`ConstValue` should follow the same rules.** {U64, F64, Str,
   VecU64, VecF64} gains `I64` with Option B. Note `VecU64` is a
   const-only vector type with no `Value`/wire counterpart — either
   admit it under R4 reasoning at the const tier (its consumers are
   builders, not wires) or document it as a deliberate const-tier
   exception; today it's just unstated.
5. **Bool's JIT carrier.** Bool rides as I8 loads + 0/1 in u64
   slots; with R3 written down, codegen's `types::I8` use is the
   only place a non-{I64,F64} cranelift type appears — document it
   in jit_boundary.md as the Bool ABI convention rather than
   leaving it implicit in codegen.rs.
6. **Deferred, with triggers.** Scalar F16 (`half::f16` already a
   dependency): admit to PortType/JitType only when cranelift F16
   exits WIP *and* a workload demands scalar half math. VecI8 /
   narrow-int PortTypes: admit when a concrete adapter binding
   (CQL tinyint vectors, Postgres int2) demands them. I128/F128 /
   SIMD ABI types: no trigger; excluded by rule.

---

## 8. Full-cranelift scope — the approved model

The closure rules of §6 keep their *derivation* role but lose their
demand gates. The target type system covers every cranelift type
that stable Rust can carry:

### 8.1 Scalars — every width, both signednesses

| PortType | Storage | JIT ride | New? |
| --- | --- | --- | --- |
| `U8`, `I8`     | 64-bit slot, zero/sign-extended | u64 slot | new |
| `U16`, `I16`   | 64-bit slot, zero/sign-extended | u64 slot | new |
| `U32`, `I32`   | 64-bit slot (existing scheme)   | u64 slot | exists |
| `U64`          | `Value::U64`                    | u64 slot | exists |
| `I64`          | **`Value::I64`** (honest variant, §5 Option B) | u64 slot, bitcast | changed |
| `F16`          | `Value::F64` (f16→f64 widening is exact) | f64 bits | new |
| `F32`          | `Value::F64` via `to_bits` (existing) | f64 bits | exists |
| `F64`          | `Value::F64`                    | f64 bits | exists |
| `U128`, `I128` | **`Value::U128` / `Value::I128`** (new variants; i128 fits the existing `Value` size envelope) | interpreter-only until the two-slot ABI lands (Phase 5+) | new |
| `Bool`         | `Value::Bool`                   | 0/1 in u64 slot (I8 load convention) | exists |
| `F128`         | **excluded** — no stable-Rust carrier; cranelift F128 incomplete | — | — |

Cranelift's sign-agnostic integers map onto signedness-carrying
PortTypes in pairs, as today. Narrow widths stay PortType-only
projections (no `Value` variants — no JSON identity); 128-bit ints
get real variants because they cannot ride a 64-bit slot. Their
JSON projection is a decimal string (Number cannot hold them),
mirroring the Bytes-as-hex convention.

### 8.2 Vectors — complete lane family + SIMD execution

Polydat vectors remain arbitrary-length `SliceArc<T>` heap values —
cranelift's fixed-width SIMD registers (`F32X4`, …) are an
*execution* vocabulary, not a wire-type vocabulary. "SIMD support"
therefore lands in two places:

- **Type level:** the lane family completes to every cranelift lane
  type stable Rust carries: `VecI8` joins {VecI16, VecI32, VecI64,
  VecF16, VecF32, VecF64}. `Bytes` remains the semantic u8 buffer
  (encodings, digests); `VecI8` is the numeric i8 lane. Unsigned
  lane vecs are not distinct cranelift lanes and stay out unless a
  JSON-projection demand appears. 128-bit lane vecs follow the
  128-bit scalar story (interpreter-only).
- **Execution level:** the JIT ABI extends so `Vec*` values cross
  into compiled kernels as (ptr, len) slot pairs, and element-wise
  vector ops compile to cranelift SIMD-chunked loops (e.g. F32X4
  over the slice body, scalar remainder loop for the tail). Lane
  types whose cranelift support is WIP (F16 lanes) evaluate via
  the interpreter until cranelift stabilizes them — the type
  system does not wait on the optimizer.

### 8.3 Phase plan (status as of 2026-06-11)

1. **`Value::I64` + `JitType::I64`** — SHIPPED. Compiler-guided
   exhaustive-match sweep; lenient legacy extract (Bool
   precedent); signed vector peels and JSON `NegInt` extraction
   fixed en route.
2. **Narrow scalar widths** — SHIPPED. U8/I8/U16/I16/F16
   PortTypes, Wire impls, full adapter rows
   (`library/polyfill_narrow.rs`). The macro's Phase-2 codegen
   is width-aware (one internal `JitType` variant per width), so
   narrow-typed nodes emit `compiled_u64` closures matching the
   Wire storage conventions.
3. **128-bit integers** — SHIPPED. `Bits128` two-limb carrier
   (keeps `Value` in the 40-byte / align-8 envelope — pinned by
   the `value_size_probe` test), decimal-string JSON convention,
   `library/polyfill_128.rs` adapters, interpreter-only.
4. **`VecI8`** — SHIPPED; lane family complete.
5. **JIT slice ABI** — PENDING (the follow-on push); design
   fixed in §8.4 below.
6. **SIMD codegen** — SHIPPED in extern-kernel form:
   `compile/jit/simd.rs` compiles F32X4 dot/l2sq/add/scale
   through the cranelift engine; consumed by the new
   `library/vector_math.rs` nodes (`vec_add`, `vec_scale`,
   `vec_dot`, `vec_l2`, `vec_cosine`, `vec_norm`, plus the
   `hash_vec` synthetic generator) with scalar fallback.
   *Inline* SIMD inside kernel segments rides on phase 5.
7. **Doc sync + `preserve_order`** — SHIPPED: type_system.md
   re-synced, jit_boundary.md conventions added, polydat
   serde_json feature parity restored.

Each phase landed green (build + full workspace test suite).
Fixed en route: the bit-rotted `not(feature = "jit")` /
`no-default-features` build (rotted `WireSource::Port` match arm
in hybrid.rs; ungated `library::vectors` references in the SRD-53
resolver synthesis).

### 8.4 The vector execution substrate (phase 5 — design RATIFIED 2026-06-11)

Three distinct "vector things" share this section; conflating
them was the original draft's weakness. The ratified model keeps
them separate and layered. Decisions D1–D4 below were settled
with the user on 2026-06-11.

**Plane A — heap slices** (`Vec*` / `SliceArc<T>`):
arbitrary-length, data-dependent, memory-resident values.
**Plane B — register values**: 128-bit SIMD words as plain
*values* (two u64 slots), with lane-typed and raw views.
**Plane C — auto-vectorization**: compiler transforms that
execute flows through plane B / SIMD loops.

#### Layer 1 — per-port slot widths (the shared mechanism)

`build_p2_layout` / `build_jit_layout` / hybrid construction
allocate `slot_width(port.typ)` slots per port (scalar = 1,
register value = 2, heap-slice (ptr, len) = 2, and u128/i128 can
later ride as 2 limb slots). `WireSource::NodeOutput(node, port)`
consumers resolve through a per-node port-offset table instead of
`slot_base + port`. Provenance bitmasks assign all slots of one
value the same provenance word. One allocator change serves every
layer below.

#### Layer 2 — the register-value plane (D1: full lane family)

One 16-byte carrier (the shipped `Bits128` shape) with static
`PortType` *views*, mirroring the narrow-scalar philosophy
(static tag over shared storage):

- **Lane-typed views**: `reg_i8x16`, `reg_i16x8`, `reg_i32x4`,
  `reg_i64x2`, `reg_f16x8`, `reg_f32x4`, `reg_f64x2` — the
  128-bit unit over the existing lane family. Interpreter-side
  each projects to/from `[T; N]`; conversions to/from the
  matching `Vec*` (a `reg_f32x4` ↔ a length-4 `vec_f32`), plus
  chunk/gather ops for streaming a slice through registers.
- **Raw view**: `reg128` — the full word as algorithm-defined
  buffer state. Lanes may have heterogeneous roles (packed
  accumulators, PRNG state words, SWAR bit tricks, per-lane
  counters); the type system does not impose lane uniformity on
  this view. Lane extract/insert/shuffle ops work at any lane
  typing.
- All views are free bitcasts of each other in P3 (cranelift
  vector-to-vector bitcast emits no instruction); retyping the
  word mid-flow costs nothing.
- Register values are *values*: they ride the buffer, the
  interpreter, checkpoints (JSON projection: array form for
  lane-typed views, hex string for `reg128`) with no pointers,
  no lifetime, no pinning. None of plane A's ownership questions
  exist here.

**D2 — determinism: the 128-bit unit is fixed in the type
system.** Lane count is part of the type (`i16x8` everywhere),
so results are bit-identical across hosts; only speed varies.
This matches what cranelift 0.116 backends actually lower
(128-bit on x64 and aarch64/NEON), and wider-ISA economy is
recovered later by unrolling two reg128 ops per iteration —
without changing any types or any results.

#### Layer 3 — heap-slice transport (D3: scratch + forwarding)

For plane A inside compiled kernels: `(ptr, len)` slot pairs; a
vector-*producing* port owns a dedicated scratch `Vec<T>` in
kernel state (resized on first eval / dimension change, reused
steady-state) that its slots view. No per-cycle allocation, no
Arc refcount traffic, trivially correct lifetime. Provably
pass-through nodes forward their input `(ptr, len)` verbatim
instead of copying. Host-supplied vector inputs are written as
the `(ptr, len)` of a `SliceArc` the caller keeps alive for the
eval call. Vector-bearing nodes emit a `CompiledSlotOp`
(`Fn(&[u64], &mut [u64], &mut KernelScratch)`); scalar nodes
keep `CompiledU64Op` unchanged. Known cost accepted with D3:
returning a borrowed window of an mmap'd dataset from inside a
compiled kernel requires a copy into scratch.

#### Layer 4 — auto-vectorization (the payoff)

Two transforms, both deferred behind layers 1–3 but specified
now:

- **Spatial fusion** (plane A): adjacent elementwise slice ops
  (`vec_scale(vec_add(a, b), k)`) fuse into one SIMD loop;
  intermediates stay in registers instead of scratch.
- **Temporal batching** (plane B as IR): evaluate N cycles per
  kernel call, one lane per cycle. The key identity: *a batched
  scalar wire IS a register value* — the batched form of an
  `i16` wire is `reg_i16x8`. The transform is a typed rewrite:
  segment wires `T → reg(T, N)`, ops → their lane-wise SIMD
  counterparts, gather/scatter at segment boundaries. Under the
  fixed 128-bit unit every batched wire occupies exactly 2 slots
  regardless of element type (8×i16 = 4×f32 = 2×f64 = one
  register), so the layer-1 allocator already fits.

Temporal-batching constraints (the segment-selection rules):

- N is uniform per segment, so the economy accrues to
  *uniformly narrow* flows — an i16-typed flow batches 8
  cycles/instruction, f64 only 2. Segment selection prefers
  maximal same-width subgraphs. Narrow types are thereby a
  workload-author performance lever, not just storage tags.
- **lanes(reg128) = 1.** The "every batched wire is 2 slots"
  invariant holds for scalar element types only; a reg-typed
  wire under N-wide batching would need N full registers. Rule:
  explicit register-plane wires cap their segment at N = 1 —
  manual vectorization and auto-vectorization compete for the
  same registers, and the author's explicit packing wins; the
  batcher batches *around* reg-typed wires, never through them.
  A flow claims SIMD economy one way or the other: scalar-narrow
  (machine batches it) or hand-packed reg128 (author already
  filled the register).
- Ineligible nodes split segments: stateful/volatile nodes (no
  lane semantics for cross-cycle state), externs without
  lane-wise forms (xxh3 — hash-rooted flows batch only
  *downstream* of the hash), and predicate guards until the
  violation plumbing learns lane→cycle attribution (a range
  check failing in lane 5 of 8 must name the offending cycle;
  the setjmp path is scalar-minded today — design this before
  any batching codegen).
- **Split mechanics + the state taxonomy.** A split runs
  [batched N-wide] → [scalar replay ×N in cycle order] →
  [re-batched]; replay preserves scalar semantics exactly, but
  each boundary pays a gather/scatter, so a *cheap* hidden-state
  node can erase the whole win — segment maps belong in dryrun
  diagnostics so batch-poison is visible to authors. The three
  state shapes and their fates: (1) *hidden node state*
  (per-eval mutation; `Purity::Nondeterministic`) splits
  segments; (2) *wire-carried state within one cycle* (reg128
  buffer words, packed accumulators, SWAR) is plain pure
  dataflow — nothing splits, subject only to the lanes(reg128)=1
  rule; (3) *cross-cycle recurrence* is not expressible in the
  DAG — the canonical reformulation is state-as-f(cycle)
  (hash / PRNG jump-ahead, with reg128 as the full-width state
  word type), which restores purity and full batching; the
  genuinely-serial residue (non-jumpable running aggregates) is
  an associative-fold concern for the comprehension layer's
  future scan lowering, not for per-cycle batching.
- Float determinism: lane-wise float ops are bit-exact under
  batching (each lane sees the operations it would have seen
  scalar); only *cross-lane* reductions reorder. Elementwise
  float flows batch freely; horizontal ops fix a tree-reduction
  shape in their contract or stay scalar. Integer flows are
  exact everywhere — narrow-int flows are the flagship case.

#### Build order / status (2026-06-11)

1. **Layers 1 + 2 — SHIPPED.** Slot widths flow through all four
   kernel builders and all three provenance paths (per-input
   dependents expand to per-slot so dirty masks stay coherent);
   the `#[polydat_node]` macro emits two-slot limb reads/writes
   (`Bits128`) for register words and u128/i128, making them
   Phase-2 eligible; `RegView` (the materialized free-bitcast)
   is auto-inserted for any reg→reg wire and is itself a P2
   two-slot copy / P3 128-bit move. P3 emits native cranelift
   vector instructions for splats and 17 of the 18 element-wise
   ops (`imul.i8x16` has no lowering below AVX-512 — `reg_mul_i8`
   is closure-only by classification); P1↔P2 and P1↔P3
   equivalence sweeps assert bit-identical results across every
   lane family. Known pre-existing limitation surfaced en route:
   a *literal* operand lowers to a const node that classifies
   `Fallback`, declining pure-P3 kernels that contain one —
   const-node classification is an independent gap, noted for a
   future push.
2. **Layer 3 — SHIPPED.** `CompiledSlotOp`
   (`Fn(&[u64], &mut [u64], &mut [ScratchBuf])`) joins
   `CompiledU64Op` as the second compiled-op shape; the
   `#[polydat_node]` macro emits it automatically for any node
   mixing jit-able scalars with typed-slice args / `Vec<elem>`
   returns (slice inputs read `(ptr, len)` slot pairs; vector
   returns move into kernel-owned scratch and publish their
   `(ptr, len)`). All four P2 builders and both hybrid builders
   select `compiled_u64` first, `compiled_slot` second, typed-eval
   third; every engine eval loop dispatches both shapes with the
   step's scratch slice. End-to-end equivalence proven for
   `hash_vec → vec_add → vec_dot` flows through P2 and hybrid
   (bit-identical with typed eval), including interleaved-cycle
   scratch-reuse coherence. The slice ops' bodies execute the
   cranelift-SIMD kernels, so SIMD runs natively on this path
   already; *segment-resident* extern calls (`vec_dot` lowered to
   a direct `simd_dot_f32(a_ptr, b_ptr, len)` call inside a JIT
   segment, eliding the closure dispatch) remain as identified
   optimization headroom, not a capability gap.
3. Layer 4 spatial fusion, then temporal batching (after the
   lane→cycle violation-attribution design) — the remaining open
   work of this section.

#### 8.5 Slot-state axioms (RATIFIED 2026-06-12)

The buffer-state contract that layers 1–3 flow from — slot
colors, pointer containment, single-writer scratch,
publish-before-read, skip coherence, sequential-by-axiom,
one-static-deref, P1-as-oracle, deterministic validation, and
the unsafe tripwire — is normative in
[jit_boundary.md](jit_boundary.md) §"Slot-state axioms (S1–S10)"
(the single canonical home). Layer-4 work and any engine surgery
must read S4/S6 first; temporal batching lives entirely in Imm2
(register values), so it never interacts with Ref state.

## 9. Cross-references

- [type_system.md] — PortType/Value/adapter mechanics (to be
  re-synced per §7.2).
- [jit_boundary.md](jit_boundary.md) — `fn(coords, buffer)` ABI,
  bit-riding conventions, setjmp predicate plumbing.
- [none_semantics.md](none_semantics.md) (SRD-74) — None/Null
  propagation; why None is interpreter-only.
- `docs/sysref/80b_macro_universal_authoring.md` — `Wire` trait as
  the sole authoring bridge; Commitment 2 ("type identity is Rust +
  serde_json::Value semantics") is the prior statement of this
  brief's R1.
- `cranelift-codegen-meta-0.116.1/src/shared/types.rs` — standard
  set A source of truth.
- `serde_json-1.0.150/src/value/mod.rs`, `src/number.rs` — standard
  set B source of truth.

[type_system.md]: type_system.md
