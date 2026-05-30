# Spec Implementation Plan — Polydat Design

**Subtitle:** Pushes to convert PARTIAL / PLANNED items
across the five spec docs into SHIPPED status.

**Status:** DRAFT — plan covers eight pushes (γ-1 through
γ-8) that, when complete, will convert every PARTIAL and
PLANNED item in the spec doc set into SHIPPED. Two
additional optional pushes (γ-9, γ-10) track lower-
priority enhancements named in the spec's open questions.

## Authoritative ownership declaration

This document is the **single authoritative reference**
for the staged implementation pushes needed to bring the
spec doc set to fully-SHIPPED status. Each push has:

- A purpose (what spec status changes).
- A dependencies list (which other pushes must land first).
- Affected files (where the code work happens).
- Code changes (high-level shape).
- Tests required (regression baseline + new coverage).
- Acceptance criteria (what "done" means).

When a push lands, the corresponding spec sections'
status markers update from PARTIAL/PLANNED to SHIPPED,
and this doc records the transition.

## Companion documents

The five spec docs whose PARTIAL/PLANNED items this plan
addresses:

- [Composition Substrate](composition_substrate.md) — S/T/L
  axioms. T2 PARTIAL targeted by γ-5 + γ-6.
- [Grammar](grammar.md) — G axioms. All SHIPPED; no
  pushes in this plan target G-axioms.
- [Graph Compiler](graph_compiler.md) — H/CF/NF axioms.
  All SHIPPED; no pushes in this plan target H/CF/NF
  axioms.
- [Runtime Model](runtime_model.md) — R/D axioms. D2
  PARTIAL targeted by γ-2.
- [Expression Engine](expression_engine.md) — E axioms +
  §5 Embedding System Contract. E7 + §5.1.3 + §5.3 +
  §5.4.2-3 + §5.6 targeted by γ-1 + γ-3 + γ-4 + γ-5 +
  γ-6 + γ-7 + γ-8.

The forcing question: **the spec set is 37/40 axioms
SHIPPED, with 3 PARTIAL/PLANNED axioms and 5
section-level PLANNED items in expression_engine.md.
What's the minimum staged push sequence that brings
everything to SHIPPED, and what dependencies constrain
the order?** This doc says: eight pushes (γ-1 through
γ-8), each independent or with at most one prerequisite,
landing in the order specified below.

---

## 1. The planned units

The PARTIAL/PLANNED items group into two coherent units
plus a third optional group:

### Unit 1 — Typed Embedding Upgrade

Six pushes that together convert E7 (typed error
ontology), §5.1.3 (opt-in strict contract), §5.3
(l-value typed surface), §5.4.2-3 (boundary adapter
polyfills), D2 (explicit purity declaration), and T2
(typed mismatches healed at boundary sites) from
PARTIAL/PLANNED to SHIPPED.

| Push | Targets | Spec impact |
|---|---|---|
| γ-1 | typed `EmbeddingError` enum introduction | E7 Phase A |
| γ-2 | `GkNode::purity()` explicit declaration | D2 SHIPPED |
| γ-3 | migrate surfaces to typed errors | E7 Phase B → SHIPPED |
| γ-4 | typed embedding surface (`eval_const_expr_typed::<T>`) | §5.3 SHIPPED |
| γ-5 | input-binding boundary adapters | §5.4.2 (half) SHIPPED |
| γ-6 | return-path boundary adapters | §5.4.2 (half) SHIPPED; T2 SHIPPED |
| γ-7 | strict-mode embedding flag | §5.1.3 SHIPPED |

### Unit 2 — Virtual-Wire Resolver

One push that converts §5.6 (host-mediated extern
resolver) from PLANNED to SHIPPED.

| Push | Targets | Spec impact |
|---|---|---|
| γ-8 | `register_extern_resolver` API + Context Fusion wiring | §5.6 SHIPPED |

### Unit 3 — Lower-priority enhancements (optional)

Three pushes named in spec open questions, deferred
until profile-driven need or specific consumer demand.

| Push | Targets | Spec impact |
|---|---|---|
| γ-9 | bulk-evaluation surface | §12.3 SHIPPED |
| γ-10 | lazy / suspended compilation | §12.5 SHIPPED |
| γ-11 | `HostText` provenance wrapper | §12.4 SHIPPED |

---

## 2. Push sequence and dependencies

```text
γ-1 (EmbeddingError enum) ────────┬───► γ-3 (migrate surfaces)
                                  │
                                  └───► γ-4 (typed surface) ──┬───► γ-6 (return-path adapters)
                                                              │
                                                              └───► γ-7 (strict mode)
γ-2 (GkNode::purity)  ─── independent
γ-5 (input-binding adapters) ─── independent
γ-8 (virtual-wire resolver) ─── independent

γ-9, γ-10, γ-11 ─── independent, deferred
```

**Recommended landing order** (each push is
independently testable; the order minimises rework):

1. **γ-1** — Foundation for Unit 1; introduces the typed
   enum without breaking any consumer.
2. **γ-2** — Independent of Unit 1; ships D2 quickly.
3. **γ-3** — Migrates surfaces to typed errors; consumer
   updates follow.
4. **γ-4** — Adds the typed embedding surface; no consumer
   migration yet (additive).
5. **γ-5** — Independent; ships the input-binding adapter
   site.
6. **γ-6** — Pairs with γ-4; ships the return-path adapter
   site, completes T2.
7. **γ-7** — Pairs with γ-4; ships the strict-mode flag.
8. **γ-8** — Independent of Unit 1; ships virtual wires.

After γ-8, every PARTIAL/PLANNED axiom and section in
the spec set is SHIPPED.

γ-9, γ-10, γ-11 land later if profile-driven or
consumer-driven need surfaces.

---

## 3. Per-push specifications

### Push γ-1 — Typed `EmbeddingError` enum introduction

**Purpose.** Land the `EmbeddingError` enum (10 variants
per [Expression Engine §6](expression_engine.md)) as a
new type at `polydat::dsl::compile::EmbeddingError`. Add
`From<EmbeddingError> for String` so existing
string-consumer call sites continue working unchanged.
No surfaces migrate yet.

**Spec impact.** E7 moves from PLANNED to "Phase A
landed" (additive); E7 stays PLANNED until γ-3.

**Dependencies.** None — foundational.

**Affected files.**

- `polydat/src/dsl/compile.rs` — add `EmbeddingError` enum
  + `From<EmbeddingError> for String` impl.
- Optional: re-export `EmbeddingError` at `polydat::dsl`
  for convenience.

**Code changes.**

```rust
pub enum EmbeddingError {
    Parse { source: String, message: String, position: Option<usize> },
    UnresolvedPlaceholder { name: String, source: String },
    LifecycleMismatch { source: String, dynamic_inputs: Vec<String> },
    UnknownNode { name: String, source: String, suggestion: Option<String> },
    TypeMismatch { from_node: String, from_type: PortType, to_node: String, to_type: PortType, source: String },
    NodeEvalPanic { node_name: String, message: String, source: String },
    ResultMissing { output_name: String, source: String },
    NonePropagated { accessor: &'static str, source: String },
    Timeout { source: String, elapsed_ms: u64, deadline_ms: u64 },
    RegistryNotInitialised { missing: Vec<String>, source: String },
}

impl std::fmt::Display for EmbeddingError { ... }
impl std::error::Error for EmbeddingError { ... }
impl From<EmbeddingError> for String { fn from(e: EmbeddingError) -> String { e.to_string() } }
```

**Tests required.**

- Round-trip: each variant `Display`s to a non-empty
  string and parses back via `Debug`.
- `From<EmbeddingError> for String` produces the same
  output as `format!("{}", e)`.

**Acceptance.**

- Enum compiles in `polydat` lib.
- All existing workspace tests pass unchanged
  (additive change, no surfaces migrated).
- Spec doc note: §6 variant guide table marked "ready
  for surface migration in γ-3".

### Push γ-2 — `GkNode::purity()` explicit declaration

**Purpose.** Add an explicit purity classification to
the `GkNode` trait via a `Purity` enum + `fn purity()`
method. Existing nodes default to inferred-via-JIT-level
for backward compatibility; diagnostic nodes (log_info,
log_debug, etc.) declare `SideChannel` explicitly.

**Spec impact.** D2 moves from PARTIAL to SHIPPED.

**Dependencies.** None.

**Affected files.**

- `polydat/src/ast.rs` — add `Purity` enum + `fn purity()`
  method on `GkNode` with default `Purity::Pure`.
- `polydat/src/library/diagnostic.rs` — declare
  `Purity::SideChannel { sink: SideChannelSink::Stderr }`
  on log_info, log_debug, log_warn, log_error.
- Optional: any other library nodes with known impurity
  (e.g., file-writing nodes if they exist).

**Code changes.**

```rust
pub enum Purity {
    /// Pure function — eval(inputs) → outputs, no side
    /// effects, byte-identical determinism.
    Pure,
    /// Has an observable side channel (logging, file I/O,
    /// network) but typed return is still a function of
    /// inputs.
    SideChannel { sink: SideChannelSink },
    /// Holds eval-call-spanning state (rare; mostly for
    /// stateful generators where polydat's caching model
    /// doesn't apply).
    Stateful { reason: &'static str },
}

pub enum SideChannelSink {
    Stderr,
    LogBuffer,
    File,
    Network,
}

pub trait GkNode: Send + Sync {
    // existing methods...

    /// Declare this node's purity for D2 (Runtime Model)
    /// and E3 (Expression Engine) determinism reasoning.
    /// Default: `Pure`. Override for nodes with observable
    /// side effects.
    fn purity(&self) -> Purity { Purity::Pure }
}
```

**Tests required.**

- `Purity` enum round-trips Debug → parse.
- Diagnostic nodes return non-`Pure` from `purity()`.
- All non-diagnostic library nodes return `Purity::Pure`
  by default (verified by a test that iterates registered
  nodes and asserts).

**Acceptance.**

- `GkNode::purity()` is callable on every registered
  node.
- Workspace tests pass.
- Spec doc note: runtime_model.md D2 status → SHIPPED;
  Expression Engine §12.2 open question resolved.

### Push γ-3 — Migrate surfaces to typed `EmbeddingError`

**Purpose.** Change the three host-facing surfaces to
return `Result<_, EmbeddingError>` instead of
`Result<_, String>`. Update every consumer call site
across nbrs-* and adapters/* to either pattern-match
on the enum or rely on the `From for String` shim.

**Spec impact.** E7 moves from "Phase A landed" to
SHIPPED.

**Dependencies.** γ-1.

**Affected files.**

- `polydat/src/dsl/compile.rs` — surface signature change:
  `eval_const_expr(source: &str) -> Result<Value, EmbeddingError>`.
- `polydat/src/kernel/interp.rs` — surface signature change:
  `interpolate_via_kernel(text: &str, kernel: &GkKernel) -> Result<String, EmbeddingError>`.
- `polydat/src/iteration/comprehension/eval.rs` — surface
  signature change: `evaluate_spec(...) -> Result<Vec<Value>, EmbeddingError>`.
- Consumer call sites (~10 across nbrs-activity, nbrs-workload,
  adapters/*): update to handle the typed enum, either via
  pattern match or by leaning on the `From for String`
  conversion at the use site.

**Code changes.**

Each surface's return-type signature updated; internal
error production uses the typed constructors. Consumer
sites typically need:

```rust
// Before:
let value = eval_const_expr(text).map_err(|s| MyError::Eval(s))?;

// After (using From for String):
let value = eval_const_expr(text).map_err(|e| MyError::Eval(e.to_string()))?;

// Or (using pattern match):
let value = eval_const_expr(text).map_err(|e| match e {
    EmbeddingError::Parse { .. } => MyError::Parse(e.to_string()),
    EmbeddingError::LifecycleMismatch { .. } => MyError::NeedsRuntime,
    _ => MyError::Eval(e.to_string()),
})?;
```

**Tests required.**

- Each surface returns the correct typed variant for
  each error class (parse failure → Parse;
  unresolved name → UnresolvedPlaceholder; etc.).
- Consumer tests still pass (string-form rendering
  preserved via `From for String`).

**Acceptance.**

- Surface signatures updated.
- All workspace tests pass.
- Spec doc note: §6.3 Phase B complete; E7 SHIPPED.

### Push γ-4 — Typed embedding surface

**Purpose.** Add `eval_const_expr_typed::<T>` (and its
kernel-bound counterpart `eval_kernel_bound_typed::<T>`)
that drive compilation against a host-declared target
type via a `HostType` trait.

**Spec impact.** §5.3 (L-value type inference) moves
from PLANNED to SHIPPED.

**Dependencies.** γ-1 (uses `EmbeddingError`).

**Affected files.**

- `polydat/src/dsl/compile.rs` — add `HostType` trait,
  impls for primitive Rust types, and the typed surfaces.
- `polydat/src/ast.rs` — possibly add helper conversions
  from `Value` to primitive Rust types behind the
  `HostType` trait.

**Code changes.**

```rust
pub trait HostType: Sized {
    fn target_port_type() -> PortType;
    fn from_value(v: Value) -> Result<Self, EmbeddingError>;
}

impl HostType for bool { /* PortType::Bool, accessor */ }
impl HostType for u64 { /* PortType::U64 */ }
impl HostType for i64 { /* ... */ }
impl HostType for f64 { /* ... */ }
impl HostType for String { /* ... */ }
impl HostType for Vec<f32> { /* PortType::VecF32 */ }
impl HostType for Vec<i32> { /* PortType::VecI32 */ }
impl HostType for Vec<String> { /* ... */ }

pub fn eval_const_expr_typed<T: HostType>(source: &str) -> Result<T, EmbeddingError> {
    let value = eval_const_expr(source)?;
    // Optional: invoke return-path adapter here (γ-6).
    T::from_value(value)
}

pub fn eval_kernel_bound_typed<T: HostType>(
    text: &str, kernel: &GkKernel,
) -> Result<T, EmbeddingError> {
    let interpolated = interpolate_via_kernel(text, kernel)?;
    eval_const_expr_typed::<T>(&interpolated)
}
```

**Tests required.**

- For each `HostType` impl: a happy-path test (type
  matches, returns Rust value).
- For each `HostType` impl: a type-mismatch test (returns
  `EmbeddingError::TypeMismatch`).
- Composition test: `eval_kernel_bound_typed::<bool>("{k} > 5", &kernel)`
  with `k = 10` returns `true`; with `k = 3` returns `false`.

**Acceptance.**

- Typed surfaces compile and pass per-type tests.
- Workspace tests pass.
- Spec doc note: §5.3 status → SHIPPED; §12.7 open
  question resolved.

### Push γ-5 — Input-binding boundary adapters

**Purpose.** Extend `materialize_wiring_from_outer` to
consult the adapter catalog when an outer-scope binding's
type differs from the inner kernel's extern slot's
declared type. Insert the catalog adapter at the
synthesis site; type-mismatch surfaces only when no
catalog entry exists.

**Spec impact.** §5.4.2 (input-binding sites) moves from
PLANNED to SHIPPED. Half of T2's PARTIAL gap closed.

**Dependencies.** None.

**Affected files.**

- `polydat/src/kernel/state.rs` —
  `materialize_wiring_from_outer` consults
  `library::convert` catalog at slot-fill time.
- `polydat/src/library/convert.rs` — possibly extend
  catalog with any missing conversions discovered
  during the implementation.

**Code changes.**

```rust
fn materialize_wiring_from_outer(&mut self, outer: &GkKernel) {
    // existing logic ...
    for input_def in self.input_defs.iter() {
        if input_def.kind == InputKind::Extern || input_def.kind == InputKind::IterationExtern {
            if let Some(value) = outer.lookup(&input_def.name) {
                if value.port_type() == input_def.typ {
                    // direct fill
                    self.set_input_value(input_def.name, value);
                } else if let Some(adapter) = library::convert::find_adapter(value.port_type(), input_def.typ) {
                    // apply adapter
                    let adapted = adapter.eval(value);
                    self.set_input_value(input_def.name, adapted);
                } else {
                    // typed error
                    return Err(EmbeddingError::TypeMismatch { ... });
                }
            }
        }
    }
}
```

**Tests required.**

- Outer scope binds `k: F64`, inner kernel declares
  `k: U64`: synthesis inserts `F64ToU64` adapter.
- Outer scope binds `k: Bytes`, inner kernel declares
  `k: U64`: no catalog adapter → typed error.
- Existing scope-init test suite passes unchanged
  for matching types.

**Acceptance.**

- Adapter insertion at synthesis fires for catalogued
  conversions.
- Typed error fires for non-catalogued mismatches.
- Workspace tests pass.

### Push γ-6 — Return-path boundary adapters

**Purpose.** When `eval_const_expr_typed::<T>` is called
and the expression's output type ≠ `T`'s declared
`PortType`, consult the adapter catalog. Insert the
adapter at the return-path; type-mismatch surfaces only
when no catalog entry exists.

**Spec impact.** §5.4.2 (return-path sites) moves from
PLANNED to SHIPPED. T2 fully SHIPPED.

**Dependencies.** γ-4 (typed surface).

**Affected files.**

- `polydat/src/dsl/compile.rs` — `eval_const_expr_typed`
  invokes adapter catalog before delegating to
  `HostType::from_value`.

**Code changes.**

```rust
pub fn eval_const_expr_typed<T: HostType>(source: &str) -> Result<T, EmbeddingError> {
    let value = eval_const_expr(source)?;
    let target_type = T::target_port_type();
    if value.port_type() == target_type {
        T::from_value(value)
    } else if let Some(adapter) = library::convert::find_adapter(value.port_type(), target_type) {
        let adapted = adapter.eval(value);
        T::from_value(adapted)
    } else {
        Err(EmbeddingError::TypeMismatch { ... })
    }
}
```

**Tests required.**

- `eval_const_expr_typed::<bool>("5 > 3")` returns
  `true` (the BinOp's `>` produces `U64`; `U64ToBool`
  adapter converts).
- `eval_const_expr_typed::<String>("42")` returns
  `"42"` via `U64ToString` adapter.
- Non-catalogued conversion returns typed error.

**Acceptance.**

- Return-path adapter insertion fires for catalogued
  conversions.
- Typed error fires for non-catalogued mismatches.
- Workspace tests pass.
- Spec doc notes: §5.4.2 SHIPPED; T2 SHIPPED.

### Push γ-7 — Strict-mode embedding flag

**Purpose.** Add a `StrictMode` flag (or a parallel
strict-typed surface) that rejects lossy conversions
even when the catalog allows them. Hosts opt in when
they want guaranteed lossless behavior.

**Spec impact.** §5.1.3 (opt-in strict contract) moves
from PLANNED to SHIPPED.

**Dependencies.** γ-4 (typed surface).

**Affected files.**

- `polydat/src/dsl/compile.rs` — add strict variant of
  the typed surface (e.g., `eval_const_expr_typed_strict::<T>`)
  or a `StrictMode` flag on the existing surface.
- `polydat/src/library/convert.rs` — adapter catalog
  entries declare lossless/lossy explicitly.

**Code changes.**

```rust
impl Adapter {
    pub fn is_lossless(&self) -> bool { ... }
}

pub fn eval_const_expr_typed_strict<T: HostType>(source: &str) -> Result<T, EmbeddingError> {
    // Same as eval_const_expr_typed but rejects lossy adapter conversions.
}
```

**Tests required.**

- Strict mode rejects lossy conversion (e.g., `F64 → U64`
  truncation): typed `TypeMismatch` error.
- Strict mode accepts lossless conversion (e.g., `U64 → F64`
  exact widening).
- Non-strict mode applies both.

**Acceptance.**

- Strict surface compiles and passes tests.
- Workspace tests pass.
- Spec doc note: §5.1.3 SHIPPED.

### Push γ-8 — Virtual-wire resolver

**Purpose.** Add a `register_extern_resolver` API on
`GkRuntime` that hosts use to provide values for extern
slots Context Fusion can't satisfy from the outer chain.
Resolver fires at scope-init time; result is frozen for
the scope's lifetime (per S3).

**Spec impact.** §5.6 (virtual wires) moves from PLANNED
to SHIPPED.

**Dependencies.** None — independent of Unit 1.

**Affected files.**

- `polydat/src/dsl/factories.rs` — add `register_extern_resolver`
  on `GkRuntime`.
- `polydat/src/kernel/state.rs` —
  `materialize_wiring_from_outer` consults registered
  resolvers as a fall-through after outer-chain lookup.

**Code changes.**

```rust
pub type ExternResolver = Box<dyn Fn(&str, &PortType, &GkKernel) -> Option<Value> + Send + Sync>;

impl GkRuntime {
    pub fn register_extern_resolver(&mut self, resolver: ExternResolver) {
        self.extern_resolvers.push(resolver);
    }
}
```

Context Fusion's S2 walk: for each extern slot, try
outer chain first; if no match, iterate registered
resolvers; if any returns `Some(value)`, fill the slot;
otherwise surface `EmbeddingError::UnresolvedPlaceholder`.

**Tests required.**

- A registered resolver fills a slot the outer chain
  doesn't provide.
- Multiple resolvers: first matching resolver wins
  (registration order).
- Resolver returns wrong type for the slot: typed
  `TypeMismatch` error.
- Resolver fires once at scope-init; not re-invoked
  per cycle.

**Acceptance.**

- API compiles and passes resolver-callback tests.
- Workspace tests pass.
- Spec doc note: §5.6 SHIPPED; §12.6 open question
  resolved.

### Push γ-9 — Bulk-evaluation surface (optional)

**Purpose.** Add `evaluate_many(&[&str], &GkKernel) ->
Vec<Result<T, EmbeddingError>>` for hosts that evaluate
N expressions against the same kernel context.

**Spec impact.** §12.3 (bulk-evaluation surface) →
SHIPPED.

**Dependencies.** γ-1.

**Status.** Profile-driven; defer until a measurable
need surfaces.

### Push γ-10 — Lazy / suspended compilation (optional)

**Purpose.** Add a "compile when first evaluated"
surface for expressions that may or may not be
evaluated (validation rules, assertions).

**Spec impact.** §12.5 → SHIPPED.

**Dependencies.** γ-1.

**Status.** Consumer-driven; defer until a host has
a specific pattern that benefits.

### Push γ-11 — `HostText` provenance wrapper (optional)

**Purpose.** Add a typed `HostText` wrapper that records
the provenance of submitted text (source file, line
number, host parsing context) for better cross-crate
error reporting.

**Spec impact.** §12.4 → SHIPPED.

**Dependencies.** γ-1, γ-3 (uses typed errors).

**Status.** UX-driven; defer until host-side error
quality becomes a focus area.

---

## 4. Acceptance criteria (all-pushes-shipped)

The plan is complete when:

- [ ] γ-1 through γ-8 all landed.
- [ ] Every PARTIAL/PLANNED axiom in the five spec docs
      is SHIPPED:
  - [ ] T2 SHIPPED
  - [ ] D2 SHIPPED
  - [ ] E7 SHIPPED
- [ ] Every section-level PLANNED in expression_engine.md
      is SHIPPED:
  - [ ] §5.1.3 (opt-in strict contract)
  - [ ] §5.3 (l-value type inference)
  - [ ] §5.4.2 (boundary adapter sites)
  - [ ] §5.4.3 (boundary polyfill rules)
  - [ ] §5.6 (virtual wires)
- [ ] Open-question entries §12.1, §12.2, §12.6, §12.7
      in expression_engine.md are resolved (removed from
      open-questions list; the relevant section is
      annotated SHIPPED with the push number).
- [ ] Workspace test count holds or grows; zero
      regressions on the existing baseline.
- [ ] Each spec doc's §0 status legend table is updated
      to reflect the new SHIPPED state.

---

## 5. Migration / cutover notes

**Backward compatibility.** Each push is designed to
preserve existing consumer behavior:

- γ-1 introduces the enum *additively*; `From for String`
  shim means existing string consumers see no behavior
  change.
- γ-3's surface migration breaks compilation at consumer
  sites that call the old signatures. The compile error
  is small (a `.map_err(|e| e.to_string())` insert) and
  fixable per-site.
- γ-2's `GkNode::purity()` has a default impl returning
  `Pure`; nodes that need to declare otherwise opt in.
- γ-4's typed surface is *additive*; existing untyped
  surfaces stay.
- γ-5 / γ-6 change error behavior at synthesis time:
  previously-failing type mismatches now succeed (with
  adapter insertion). This is a *strict improvement*
  — no host that was working before breaks.
- γ-7 is a new opt-in surface; default mode is unchanged.
- γ-8 is a new API; no existing consumer calls it.

**Strict-mode considerations.** γ-7 introduces strict
mode as opt-in. Hosts that want pre-γ-5 behavior (no
adapter insertion at synthesis) can use strict mode
where adapters would be lossy. Strict mode is not the
default — backward compat is the default — but it's
available for hosts that want guarantee-strict
contracts.

**Test-suite continuity.** Every push includes
regression validation: the existing workspace baseline
must pass before the push lands. New tests are added
per-push to cover the new behavior.

---

## 6. Cross-references to spec docs

### Spec status updates per push

| Push | Spec doc | Section / axiom | Status change |
|---|---|---|---|
| γ-1 | expression_engine.md | E7 | PLANNED → Phase A landed |
| γ-2 | runtime_model.md | D2 | PARTIAL → SHIPPED |
| γ-3 | expression_engine.md | E7 + §6.3 | Phase A → SHIPPED |
| γ-4 | expression_engine.md | §5.3 + §12.7 | PLANNED → SHIPPED + resolved |
| γ-5 | expression_engine.md + composition_substrate.md | §5.4.2 (half) + T2 | PLANNED → partially SHIPPED + PARTIAL |
| γ-6 | expression_engine.md + composition_substrate.md | §5.4.2 (other half) + T2 | partially SHIPPED → SHIPPED + PARTIAL → SHIPPED |
| γ-7 | expression_engine.md | §5.1.3 | PLANNED → SHIPPED |
| γ-8 | expression_engine.md | §5.6 + §12.6 | PLANNED → SHIPPED + resolved |

### Spec doc updates required per push

Each push, when it lands, must update:

1. The targeted spec doc's §0 status legend table
   (move row(s) from PARTIAL/PLANNED → SHIPPED).
2. The targeted axiom or section header
   (change `(PARTIAL)` or `(PLANNED)` to `(SHIPPED)`).
3. Any inline "PLANNED" or "PARTIAL" markers in the
   body text that the push obviates.
4. The §0 summary paragraph if the doc's overall
   shipped/planned ratio shifts substantially.

---

## 7. Open questions

### 7.1 Push γ-5 / γ-6 sequence

γ-5 (input-binding adapters) and γ-6 (return-path
adapters) both close T2 PARTIAL. The plan currently
lands them separately. An alternative would land them
as a single push covering both adapter sites. Tradeoff:
single push is more atomic for the T2 transition but
larger; separate pushes are smaller but T2 stays
PARTIAL for an intermediate window.

Recommendation: separate, in the order γ-5 then γ-6,
to keep each push small and reviewable.

### 7.2 γ-7 strict mode — flag vs. surface

The plan currently sketches strict mode as a separate
surface (`eval_const_expr_typed_strict::<T>`). An
alternative is a `StrictMode` enum parameter on the
existing surface (e.g.,
`eval_const_expr_typed_with::<T>(source, StrictMode::Strict)`).
The surface approach is more discoverable but doubles
the API count.

Recommendation: flag on the existing surface (avoid
API duplication), but await γ-4 implementation
experience before deciding.

### 7.3 γ-9 / γ-10 / γ-11 priority

The three deferred pushes are flagged as profile-
driven or consumer-driven. They should be revisited
when:

- A bulk-evaluation pattern surfaces in profiling
  (γ-9).
- A validation-rule or assertion pattern needs lazy
  compilation (γ-10).
- A host crate's error-quality work would benefit
  from typed text provenance (γ-11).

No specific schedule is committed; the pushes are
designed for when need arises.

### 7.4 Spec doc maintenance after lock-in

After γ-8 lands and the spec set is fully SHIPPED,
this doc itself becomes mostly historical (the pushes
are documentation of what happened). A revision could
collapse γ-1 through γ-8 into a single completion log
and keep §3 only for γ-9-11 (still-deferred work).

Recommendation: keep §3 entries for γ-1 through γ-8
until at least one round of post-implementation review,
since the implementation details are useful for
verifying the push delivered what the spec promised.

---

[Composition Substrate]: composition_substrate.md
[Grammar]: grammar.md
[Graph Compiler]: graph_compiler.md
[Runtime Model]: runtime_model.md
[Expression Engine]: expression_engine.md
