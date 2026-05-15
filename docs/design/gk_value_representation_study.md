# GK Value Representation — Ecosystem Study

**Status:** Design study, 2026-05-14. Not a decision; an inventory and
trade-off map for the project to evaluate.

## Question

Today GK uses a custom `Value` enum with a parallel `PortType` enum
for static type metadata. The variants are emulative of common
runtime types — `U64`, `F64`, `Str`, `Bool`, typed slices (`VecF32`,
`VecI32`), opaque carriers (`Json`, `Ext`, `Handle`), `Bytes`, a
`None` sentinel.

Would we be better served by adopting an established
value/type-representation library? What does each candidate buy us,
what does it cost, and what would folding GK's
non-representational metadata (port types, lifecycle, wire cost)
onto one of these look like?

## Current GK shape (baseline)

```rust
pub enum Value {                  // ~10 variants
    U64(u64),    F64(f64),    Bool(bool),
    Str(String), Bytes(Vec<u8>),
    Json(serde_json::Value),
    Ext(Box<dyn ReflectedValue>),
    Handle(Arc<dyn Any + Send + Sync>),
    VecF32(SliceArc<f32>),    VecI32(SliceArc<i32>),
    None,
}

pub enum PortType {               // ~14 variants
    U64, I64, U32, I32, F64, F32, Bool, Str, Bytes,
    Json, Ext, Handle, VecF32, VecI32,
}
```

**Notable asymmetry:** `PortType` carries more variants than `Value`.
The assembler does type-widening (`U32`/`I32`/`F32` → `U64`/`I64`/`F64`
at storage), and `PortType` carries this widening metadata. `Value`
stores only the widened forms. Type metadata is richer than runtime
storage.

**Non-representational metadata on ports:**
- `Lifecycle` — Init vs Cycle (init-time constants vs per-cycle).
- `WireCost` — Data vs Config (per-cycle hot path vs init/setup
  reconfig).
- `Commutativity`, variadicity, and the constraint system in the DSL
  registry.

Any ecosystem standardization needs to either (a) carry these
non-representational axes externally or (b) extend the chosen value
library's metadata layer to hold them.

>> We would use the chosen library's metadata layer to hold anything which is an obvious one-to-one representation without performance, semantic, or tooling trade-offs.
>> We would hold our own non-representational axes externally as the logical layer that holds the representational types.

## Working stance (per the review)

Two architectural commitments frame the rest of this study:

**Stance 1 — Co-opt, don't surrender.** GK's `Value` + `PortType`
remain the authoritative type surface. Where an ecosystem library
offers an obvious one-to-one representation that costs us nothing
on performance / semantics / tooling, we use it (today: serde_json
inside `Value::Json`, in future: Arrow scalar types as new variants
when they earn their keep). The chosen library doesn't replace our
surface; we absorb pieces of it via NewType-shaped variants when
the expressivity buys real ground.

**Stance 2 — Non-representational axes stay ours.** `Lifecycle`,
`WireCost`, commutativity, variadicity — these are GK's own
metadata. They sit external to the representational type system
and decorate ports in our layer. We don't fold them into an
ecosystem library's metadata bag (e.g. Arrow's `Field` metadata
HashMap) because doing so couples our static analysis to the
library's update cycle and lookup ergonomics.

The candidates below are evaluated under these constraints — what
each one would let us absorb piecewise vs. demand we adopt
wholesale.

## What standardization could buy

What was originally five hand-wavy claims, now qualified.

### 1. Serialization (partial — depends on the variant)

What's actually free if we adopt serde + a JSON projection of
`Value`:

- **Cleanly free:** `U64` / `F64` / `Bool` / `Str` / `None`. Scalar
  JSON shapes are direct.
- **Free with a typed wrapper:** `Bytes` (base64 or hex string),
  `Json` (already JSON), `VecF32` / `VecI32` (JSON array of
  numbers). The "typed wrapper" cost is choosing a discriminator
  shape like `{"_type": "VecF32", "_value": [...]}` — bespoke but
  short.
- **Conventional choices needed:** `Ext` carries a trait object
  with a `display()` method; the JSON projection serializes the
  display string + type name and re-hydration is one-way (we can
  display but not reconstruct without an adapter-registered
  parser). For our purposes (snapshot for inspection, not
  round-trip eval), that's fine — but it isn't a free
  bidirectional ser/de.
- **Not free, period:** `Handle(Arc<dyn Any>)` cannot serialize.
  The resource the handle points to (dataset, prepared statement)
  isn't `Serialize`. Snapshots would have to substitute a stable
  identifier (the resource key) and reconstitute at replay time
  via a registry. That's a real engineering item, not "free."

**Bottom line:** scalars + bytes + str + json + typed slices
serialize via a small bespoke projection. `Ext` is read-only
serialized (display + type name). `Handle` requires a separate
registry round-trip. The "free serialization" pitch is honest
for ~80% of the variants; the remaining 20% are exactly the ones
we'd care most about for replay (handles point at real resources)
and that fraction needs design work no library short-cuts.

### 2. Tooling (specific operations, with honest scope)

Concrete tools and what they'd do on GK output, if we projected
state to a standard format:

- **jq / jaq over JSON snapshots.** Given a per-cycle JSON state
  dump like `[{"cycle": 42, "wires": {"key": "...", "row_count":
  10, "rows_per_op": 9.8}}, ...]`, an operator could pipe
  `jaq '.[] | select(.wires.row_count != 10)'` to find cycles
  where the row count surprised. We get this today only by
  parsing audit.log lines.
- **schemars (JSON Schema generation) over `PortType`.** Today our
  `nbrs describe op` already names types per binding. JSON Schema
  would add: language-portable type files VSCode / web editors can
  validate against. Worth it only if we expect external tooling
  to author workload YAMLs (today: not the use case).
- **Polars on Parquet exports.** If each phase emits a Parquet
  file whose columns are the op-template kernel's outputs, an
  operator could load it in a notebook (`pl.read_parquet(...)`)
  and run `df.group_by("k").agg(pl.col("recall").mean())` for
  ad-hoc cuts the in-tree SQLite reporter doesn't pre-bake. This
  is the genuinely useful "tooling" win.
- **DataFusion SQL over the same Parquet.** Run `SELECT k,
  PERCENTILE_CONT(recall, 0.5) FROM run WHERE phase = 'pvs_query'
  GROUP BY k`. Same shape as Polars; SQL surface vs. dataframe.
- **Arrow Flight RPC.** Stream per-cycle batches to a live
  dashboard (Grafana with the Arrow plugin, Streamlit, custom
  web UI). Today the inspector socket carries our own protocol.
- **CBOR diag tools (`cbor.me`).** Inspect binary snapshots that
  travel via channels (Slack pastes, support tickets) without
  needing the workload yaml + nbrs binary at the other end.

What this DOESN'T add: nothing about hot-path execution. None of
these tools operate on live in-flight GK state; they all consume
exports at the boundary.

### 3. Schema as artifact — what's actually new

The reviewer's pushback: our `PortType` enum + DSL grammar
already implies a schema. Output names + types are derivable from
the op-template kernel today (we have `nbrs describe op` and a
manifest emission path). What does emitting JSON Schema /
Arrow Schema buy that the implied schema doesn't?

Honest answer: **only what external consumers can read.** Our
implied schema is consumable by tooling that imports our crates
or scrapes `nbrs describe`. JSON / Arrow / Avro Schema is
consumable by:

- IDE schema-validation plugins (VSCode JSON Schema extension)
  that would lint a workload YAML against the op-template's
  result-binding output types.
- Database-side `CREATE TABLE` generation from Arrow Schema
  (DuckDB, BigQuery import paths).
- Cross-language clients that read a Parquet file and want field
  metadata.

If we don't have any of those use cases on the roadmap, emitting
JSON Schema is a lateral move — same information, different
format. The honest pitch is "round-trip with external schema
consumers" not "schemas are new." We don't gain anything we don't
already have until a real external consumer asks for one of those
formats.

**Verdict:** keep this option in mind for `nbrs describe schema
--format=arrow` as a *future* CLI surface, but it doesn't earn its
own infrastructure today.

### 4. Type lattice depth — the genuine win, with performance honesty

Arrow's `DataType` enum is ~30 variants vs our 14 PortTypes. The
gap that actually matters for nb-rs workloads:

| Arrow has | We don't | Use case |
|---|---|---|
| `Timestamp(unit, tz)` | `U64` / `Str` | Cycle timestamps, latency anchors |
| `Duration(unit)` | `U64` / `F64` | Latency / interval values without unit-suffix gymnastics |
| `Decimal128(precision, scale)` | `F64` | Financial / exact-arithmetic columns from CQL |
| `FixedSizeList(item_type, n)` | `VecF32` (unsized) | Embedding vectors with declared dimension |
| `Dictionary(key, value)` | `Str` | Cardinality-bounded enum columns (status codes, etc.) |
| `Struct(fields)` | `Json` (untyped) | Multi-field captures without losing per-field types |

Of those, `FixedSizeList(Float32, 768)` is genuinely interesting
for the vector workloads we care about — it carries embedding
dimension as static metadata. `Decimal128` and `Timestamp(unit)`
would let CQL adapter columns flow through GK without lossy
casts.

**Performance trade-off of going wider.** Three real costs to
weigh:

1. **Enum dispatch fan-out.** `Value` match arms are
   compiler-optimised jump tables; doubling the variant count
   roughly doubles the table size and pushes a few hot-path
   matches off the I-cache fast track. Concrete cost: O(few %)
   slowdown on per-cell scalar matching, measurable with criterion
   but probably not catastrophic.
2. **Storage size per Value.** Arrow's `DataType::Dictionary`
   stores `(Box<DataType>, Box<DataType>)` inside the variant —
   ~16 bytes of indirection. If we lift Arrow types directly,
   variants with metadata grow `Value`'s sizeof beyond the
   current ~32 bytes. Each `Value` clone gets more expensive.
3. **Metadata wiring at compile time.** Today PortType is `Copy`.
   Arrow's `DataType` is `Clone` but not `Copy` (timestamps carry
   `Option<Arc<str>>` timezone, fields carry `Arc`s). Static
   compile paths that today pass `PortType` by value would need
   to switch to references — small ergonomic friction, no
   correctness issue.

**Co-opt strategy** (per Stance 1): introduce specific Arrow
types as NewType variants when they earn their keep. The first
ones to absorb:

```rust
pub enum PortType {
    // ... existing ...
    FixedVec(Box<PortType>, u32),     // wraps Arrow FixedSizeList semantically
    Timestamp(TimeUnit),               // wraps Arrow Timestamp; no tz today
    Duration(TimeUnit),                // wraps Arrow Duration
    Decimal(u8, i8),                   // wraps Arrow Decimal128
}
```

Each addition is one variant; storage cost is bounded; we don't
import Arrow's enum directly (which would let it grow under us);
we still talk Arrow's vocabulary when we serialise. This is the
"absorb pieces" framing.

**What we don't absorb yet:** `Dictionary` (we don't have a
categorical use case), `Struct` (we have `Json` for now), `Union`
(not on roadmap), the date / time / interval families that
overlap with what `Duration`/`Timestamp` already cover.

### 5. Cross-tool snapshot/replay — concrete consumer scenarios

What "Captures sent to Parquet are readable by anything in the
data stack" looks like in practice:

- **Per-phase Parquet artifact.** At phase teardown, write
  `logs/<session>/phase_<name>.parquet` with one row per cycle,
  columns = result-binding LHS values for that phase. Operator
  loads in Python: `pd.read_parquet(...)`, runs `describe()` /
  `value_counts()` for ad-hoc histogram comparisons across runs.
  Implementation: ~200 LOC using the `parquet` crate's writer.
- **Replay-debug from a CBOR cycle dump.** `nbrs replay
  --session logs/X --cycle 42` reads the cycle's bound input
  state from a captured CBOR file, replays the op-template
  kernel against it, prints every wire's typed value. Useful
  when a workload errored on cycle 42 and the operator wants
  to interactively inspect what fed it. Implementation: small,
  needs the Handle-registry hack from §1.
- **Arrow IPC streaming to a Polars / pandas notebook.** Long-
  running session emits per-cadence Arrow batches over a Unix
  socket; a notebook subscribes via `pyarrow.flight` and live-
  plots metric streams. Today the cadence-reporter writes to
  the SQLite db and notebooks poll that — works fine, but
  doesn't carry typed metadata or stream tail-of-data cleanly.
  Implementation: medium; depends on adopting `arrow` as a
  dependency.
- **Cross-language consumers.** A Julia / R analyst reads our
  Parquet outputs through their language's native reader. No
  nb-rs install needed. Today this requires SQLite-to-CSV
  detour and loses type info.

What this DOESN'T enable: live cross-tool introspection of
running cycles (that's what the inspector socket is for, and it
serves a different audience: operators who want now, not later).

## Candidates

### serde_json::Value (currently used for the `Json` variant)

```rust
pub enum Value {
    Null,
    Bool(bool),
    Number(Number),     // unified — runtime parse for u64/i64/f64
    String(String),
    Array(Vec<Value>),
    Object(Map<String, Value>),
}
```

- **Vocab:** narrow. No typed numerics (u64/i64/f64 collapsed to
  `Number`, runtime branch on read). No typed arrays. No binary. No
  extension tag.
- **Tooling:** broadest. jaq, jql, gron, JSON Schema (schemars),
  every HTTP/web framework, web inspectors.
- **Hot-path cost:** ~equal to GK Value for scalars (enum dispatch);
  Object/Array allocate via Vec + HashMap, so structured values
  cost more.
- **Typed-slice story:** none. `Vec<f32>` would become
  `Array(Vec<Number(F64(_))>)` — 24 bytes per element, no zero-copy.
- **Extension story:** none. Adapter types stringify or wrap in
  Object metadata.

### MessagePack — `rmpv::Value`

```rust
pub enum Value {
    Nil,
    Boolean(bool),
    Integer(Integer),    // u64 OR i64, distinguished
    F32(f32), F64(f64),  // typed!
    String(Utf8String),
    Binary(Vec<u8>),
    Array(Vec<Value>),
    Map(Vec<(Value, Value)>),
    Ext(i8, Vec<u8>),    // tagged extension!
}
```

- **Vocab:** close to GK's shape. Typed floats. Native binary.
  Tagged extension types via `(tag: i8, payload: Vec<u8>)`.
- **Tooling:** smaller than JSON. `msgpack-cli`, `msgpack-tools`,
  some IoT analyzers. Less web/dev-tool support.
- **Hot-path cost:** comparable to GK Value. Integer is tagged
  internally.
- **Typed-slice story:** would have to encode as `Ext` (with our own
  tag), then zero-copy reads need custom paths — same as today.
- **Extension story:** first-class, via `Ext(tag, bytes)`. UUIDs,
  handles, etc. fit naturally.
- **Serialization:** efficient binary format, well-defined.

### CBOR — `ciborium::Value`

```rust
pub enum Value {
    Integer(i128),
    Float(f64),
    Bytes(Vec<u8>),
    Text(String),
    Bool(bool),
    Null,
    Tag(u64, Box<Value>),     // tagged values — first-class!
    Array(Vec<Value>),
    Map(Vec<(Value, Value)>),
}
```

- **Vocab:** middle ground. `i128` ambitiously holds any integer
  size; floats are unified to `f64` (slight regression from GK's
  F32/F64 separation). Bytes native. Tagged values for extensions.
- **Tooling:** standards-blessed (RFC 8949, IETF). Growing — used
  in COSE, WebAuthn, IoT. `cbor-diag`, `cbor.me`. Less dev-tool
  presence than JSON.
- **Hot-path cost:** Integer-as-i128 is slightly larger; otherwise
  comparable.
- **Typed-slice story:** `Tag(u64, Array(...))` is the
  canonical pattern; standard tags exist for typed numeric arrays
  (RFC 8746). Less ergonomic than direct typed enums.
- **Extension story:** first-class via `Tag(u64, Box<Value>)`. Tag
  numbers are IANA-registered, so we'd pick an unassigned range or
  use private-use tags.
- **Serialization:** efficient binary; preserves type tags.

### BSON — `bson::Bson`

```rust
pub enum Bson {
    Double(f64),  Int32(i32),  Int64(i64),
    String(String),
    Document(Document),  Array(Array),
    Binary(Binary),      // with subtype byte
    Boolean(bool),       Null,
    ObjectId(ObjectId),  DateTime(DateTime),
    Decimal128(Decimal128),  Timestamp(Timestamp),
    RegularExpression(Regex),
    JavaScriptCode(...), JavaScriptCodeWithScope(...),
    MaxKey, MinKey, Undefined, Symbol(String),
    DbPointer(...),
}
```

- **Vocab:** richer scalars than JSON (Int32, Int64, Decimal128,
  ObjectId, DateTime). MongoDB-flavored — some variants
  (`JavaScriptCode`, `DbPointer`, `Symbol`) are irrelevant for GK.
- **Tooling:** MongoDB driver tooling. Less general-purpose than
  JSON/CBOR. mongoshell / Compass.
- **Hot-path cost:** Document is essentially a vec-of-pairs; access
  patterns acceptable.
- **Typed-slice story:** Binary with subtype tag; can encode typed
  arrays. Not zero-copy by default.
- **Extension story:** Binary subtype byte (256 user values) gives
  a tag space.
- **Serialization:** BSON format. Mongo-native.

### Apache Arrow — `arrow::array::*` + `DataType`

```rust
pub enum DataType {
    Null,
    Boolean,
    Int8, Int16, Int32, Int64,
    UInt8, UInt16, UInt32, UInt64,
    Float16, Float32, Float64,
    Timestamp(TimeUnit, Option<Arc<str>>),
    Date32, Date64,
    Time32(TimeUnit), Time64(TimeUnit),
    Duration(TimeUnit),
    Interval(IntervalUnit),
    Binary, FixedSizeBinary(i32), LargeBinary,
    BinaryView,
    Utf8, LargeUtf8, Utf8View,
    List(Field), FixedSizeList(Field, i32), LargeList(Field),
    ListView(Field), LargeListView(Field),
    Struct(Fields),
    Union(UnionFields, UnionMode),
    Dictionary(DataType, DataType),
    Decimal128(u8, i8), Decimal256(u8, i8),
    Map(Field, bool),
    RunEndEncoded(Field, Field),
}
```

- **Vocab:** by a wide margin the richest. Every numeric width,
  timestamps with timezone, decimals, dictionary-encoded
  categoricals, fixed-size lists (perfect for vector embeddings!),
  nullable everywhere, schemas as `Field`/`Schema` first-class
  artifacts.
- **Tooling:** Polars, DataFusion, Arrow Flight (zero-copy IPC), pyarrow,
  Parquet ↔ Arrow free, Plotters integration. Best analytical
  ecosystem in any language.
- **Hot-path cost:** the rub. Arrow is columnar — arrays are
  batched-up `Arc<dyn Array>` with typed buffers. **Scalar access
  is slower than enum dispatch**; per-cell reads involve downcasting
  to a typed Array then bounds-checking an index. Per-cycle scalar
  ops would regress. `Arrow Scalar` exists for single-cell access
  but it's `Arc<dyn Array>` of length 1 — heavy.
- **Typed-slice story:** zero-copy from day one. `Float32Array`
  exposes `as_slice()` directly. Best in class.
- **Extension story:** `DataType::Dictionary` for categoricals;
  custom extension types via Arrow extension-type metadata on
  Field. Heavyweight.
- **Serialization:** Arrow IPC, Parquet, Feather V2 — all free.
- **Dependency weight:** ~50 crate deps, several MB compiled.

### Polars-arrow (Polars' Arrow fork)

- Similar to Arrow, leaner. Polars-specific. Less columnar
  ecosystem reach.

### simd-json::Value

- Drop-in faster `serde_json::Value`. Same vocab, same trade-offs.
  ~3-5× faster parse on large JSON. Doesn't help if our hot path
  isn't JSON-parse-bound.

## Dimensions of fit

| Dimension | GK today | serde_json | rmpv (msgpack) | ciborium (cbor) | bson | Arrow |
|---|---|---|---|---|---|---|
| Hot-path scalar cost | ★★★ | ★★★ | ★★★ | ★★★ | ★★ | ★ |
| Typed-slice zero-copy | ★★★ | ✗ | tag | tag | tag | ★★★ |
| Type metadata richness | ★★ | ★ | ★★ | ★★ | ★★ | ★★★★ |
| Serialization coverage | bespoke | JSON | MsgPack | CBOR | BSON | many |
| Tooling ecosystem | none | ★★★★ | ★★ | ★★ | ★★ | ★★★★ |
| Extension story | Ext+Handle | ✗ | ★★★ | ★★★ | ★★ | ★★ |
| Implementation cost | — | small | medium | medium | medium | large |
| Schema-as-artifact | none | schemars | no | mixed | no | first-class |

Rough rubric: ★★★ ≈ first-class; ★★ ≈ acceptable; ★ ≈ awkward;
✗ ≈ doesn't apply.

**"tag" in the Typed-slice row** means the format supports
extension tags as a primitive (CBOR `Tag(u64, Box<Value>)`,
MessagePack `Ext(i8, Vec<u8>)`, BSON Binary subtype byte) and a
typed numeric slice can be encoded *through* that tag mechanism.
It's not zero-copy and it isn't standardised per-format — we'd
pick a private-use tag number, document our payload layout, and
implement the encode/decode ourselves. The contrast with Arrow's
★★★ rating: Arrow's `Float32Array` IS a zero-copy `&[f32]` view
of a buffer; CBOR-via-tag is "serialise to a tagged byte blob."
Different costs, both functional.

## Approach archetypes

### A — Keep GK Value; layer in serde

Add `Serialize` / `Deserialize` for `Value` and `PortType` via
serde, with a conventional JSON projection (e.g. `{type: "VecF32",
value: [...]}` for typed slices, `{type: "Ext", typename: "uuid",
display: "..."}` for extensions). Cost: small (one impl). Buys
free JSON snapshots, easy debugging, no runtime changes. Doesn't
change tooling story beyond JSON. **Recommended baseline.**

### B — Absorb Arrow types as NewType slots on our existing surface

Per Stance 1 (co-opt, don't surrender): keep `PortType` + `Value`
as the authoritative surface. Add specific Arrow-shaped variants
as NewType wrappers when their expressivity buys real ground.
The first four candidates:

```rust
pub enum PortType {
    // ... existing 14 variants ...
    FixedVec(Box<PortType>, u32),  // semantic match for Arrow FixedSizeList:
                                    // embedding vectors with declared dim
    Timestamp(TimeUnit),            // wraps Arrow Timestamp semantics
    Duration(TimeUnit),             // wraps Arrow Duration semantics
    Decimal(u8, i8),                // wraps Arrow Decimal128 (precision, scale)
}
```

Each variant is one entry in our enum; the `TimeUnit` / precision
/ scale fields are our own small enums + integers, NOT direct
imports of `arrow::datatypes::*`. We don't take Arrow as a
dependency for the runtime; we mirror its semantics in a shape
we control.

Where we use Arrow's actual types is at the **boundary** (export
paths to Parquet / Arrow IPC) — that's archetype E. Internally,
our `Decimal(p, s)` variant maps to Arrow's `Decimal128(p, s)`
when we serialize, but it isn't bound to Arrow's enum shape (so
Arrow can evolve their API without forcing a breaking change on
our static type system).

Cost: small-to-medium per absorbed variant. Each addition is one
match-arm in every `PortType`-handling site (the assembler, the
type-widening pass, the display impl, the registry). Concrete
items per variant:

- `FixedVec(Box<PortType>, u32)`: storage choice — either a new
  `Value::FixedVec(SliceArc<...>)` per element type or a single
  `Value::FixedVec(SliceArc<u8>)` with a runtime cast. Pick based
  on what dataset adapters produce.
- `Timestamp(TimeUnit)`: storage as `Value::U64` semantically;
  `TimeUnit` is metadata that tags how to render. No new runtime
  cost.
- `Duration(TimeUnit)`: same as Timestamp.
- `Decimal(p, s)`: storage as `Value::Bytes` (16 bytes) or a new
  `Value::Decimal(i128)` variant. Decision point.

What this is NOT: a wholesale port from Arrow's vocabulary into
ours. We absorb the four (or however many) types we identify a
genuine use for, and stop. Arrow stays at the export boundary.

### C — Adopt CBOR/MessagePack as the snapshot/replay format

Keep GK Value internal. For SNAPSHOTS (checkpoint state, replay
logs, metric persistence), serialize Value via ciborium (CBOR) or
rmpv (MessagePack). Use tagged extension types for VecF32 / VecI32
/ Handle / Ext (with documented private-use tag numbers). Cost:
medium. The benefit is the *wire format* — snapshots travel,
analytical tools can read them, IoT-style tools can introspect.
Runtime stays as-is.

### D — Adopt Arrow as the runtime storage too

The most ambitious. `Value` becomes `arrow::array::Scalar` or a
custom enum that wraps `ArrayRef` for the heavyweights. Per-cycle
hot path runs against Arrow types. Workload becomes a DataFusion-like
plan. Cost: very large (rewrite). Benefits: full ecosystem
integration, zero-copy across the data stack, schemas everywhere.
Risks: per-cell scalar access regression; downstream node API
becomes columnar-aware; major refactor.

### E — Hybrid: GK Value + Arrow interop trait

Keep current `Value` for hot path. Add `From<Value> for arrow::Scalar`
and `From<Value> for arrow::ArrayRef` (for the columnar shapes —
VecF32 → Float32Array, Bytes → BinaryArray, etc.). Add `to_arrow_schema`
on `PortType`. Adapters that want analytical export call into
these conversions at batch boundaries (snapshot, summary, plot).
Cost: small/medium. Buys the Arrow ecosystem on the export side
without paying the storage refactor.

## Recommendation framework

The question reduces to "which dimensions matter most for nb-rs's
near-term roadmap?"

If the goal is **debuggability and snapshot/replay** → **A + C**:
serde + CBOR. Free JSON for human-readable debugging; CBOR for
efficient binary captures. Both are small implementation cost.

If the goal is **analytical tooling for results** (Polars/Parquet
post-run analysis, cross-language scripting) → **E**: keep Value,
add Arrow interop on export paths. Best ROI for the cost.

If the goal is **schema as a first-class contract** (workload
authors get a typed schema for their data; downstream tools
consume it directly) → **B**: Arrow DataType as PortType. Bigger
implementation cost but the schema becomes a real artifact.

If the goal is **deep integration with the data ecosystem**
(treat each workload as a DataFusion pipeline; metric outputs are
Arrow-native; readouts are Parquet) → **D**: adopt Arrow as
runtime. Multi-month rewrite scope.

## My read

The hot path doesn't benefit from any of these — our enum dispatch
+ SliceArc for typed slices is already close to optimal for
scalar+vector hot loops. The benefits are at the *boundaries*:
serialization, debugging, schemas, cross-tool export.

**A + E is the practical bundle.** Serde for free snapshots;
Arrow interop on export for analytical tooling. Both incremental,
neither paints us into a corner.

**B is the architecturally interesting one.** Per the revised
framing: absorb specific Arrow-shaped variants (`Timestamp`,
`Duration`, `Decimal`, `FixedVec`) into our existing `PortType`
enum as NewType slots, without taking Arrow as a runtime
dependency. Each addition is a contained, justifiable expressivity
gain; the surface we control doesn't grow under us; we serialize
out to Arrow's vocabulary at the boundary (archetype E).

Note that we deliberately do NOT fold our `Lifecycle` / `WireCost`
into the absorbed types' metadata bag. Stance 2: those are GK's
own axes; they stay external as the logical layer that decorates
ports, not as opaque entries in someone else's metadata HashMap.

**D — full Arrow adoption — is parking lot.** The performance
unknowns and rewrite scope are too large for the current roadmap.
Revisit only when "this workload's per-cycle storage is its own
DataFusion plan" becomes a real ask.

**C — CBOR/MessagePack snapshots — depends on whether we want a
binary wire format.** If readout snapshots are user-facing artifacts
(human debug), JSON wins. If they're agent-to-agent / cross-tool
binary blobs, CBOR or MessagePack make sense. Today our snapshot
shape is SQLite-backed, so this is a longer conversation.

## Open questions

- What's the actual cost of `serde_json::Value`-style nested
  containers on our hot path? We use them only for `Json` body
  values today; if we ever projected them into kernel inputs
  per-cycle, the alloc churn would matter.
- Is there value in adopting `arrow::Field` for ports' full
  metadata (PortType + nullable + metadata-map)? That would let
  workload-author docs round-trip via Arrow Schema. ~Medium cost.
- Does adopting Arrow's `DataType` create a downstream pressure to
  also use `RecordBatch` for our cycle inputs/outputs? Probably
  yes if we're not careful; mitigated by treating Arrow DataType
  as static metadata only (option B) and not flowing arrays into
  Value.

## Open candidates not surveyed

- **rkyv** — zero-copy archive format. Niche; ergonomically
  different (uses derive macros and an "archived" view type).
- **prost / Protobuf** — codegen-based; not a runtime value type.
- **flatbuffers** — similar to rkyv, niche.
- **apache_avro** — schema-first; richer schema language than
  Arrow but less typed-vector story.
- **value-bag** — lightweight; designed for log records; too narrow.

If any of these change the rec significantly, flag and we'll dig.

## Next step

This is a study, not a decision. If we want to pick a direction,
worth knowing:

- Which boundary use cases matter most right now?
  (snapshot/replay; analytical export; cross-language; schema
  artifacts)
- What's the appetite for implementation cost? (A: hours;
  B/C/E: days; D: months)
- Are there specific tooling capabilities we'd lean on if we had
  them? (jq for inspection; Polars for ad-hoc analysis; Parquet
  for archival; Arrow Flight for live streaming to dashboards)

Tell me which of the above answer "yes," and I'll work the
implementation plan up from there.
