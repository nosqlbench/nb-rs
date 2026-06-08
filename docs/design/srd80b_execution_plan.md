# SRD-80b — Execution Plan

**Purpose:** Session-level plan to execute the SRD-80b
universal-authoring architecture and cut over the polydat
library to it. Each session has explicit entry preconditions,
work scope, and exit criteria so any session can be picked up
without re-reading the prior ones.

---

## Posture: greenfield

**This is greenfield work. No backwards compatibility, no
transitional aliases, no deprecation cycles, no parallel
old/new paths.** When a session changes a struct name, an API
shape, a workload syntax — every caller is updated in the
same PR. There is no "ship the new path alongside the old
path and migrate later." The macro is the contract; nothing
outside it gets preserved for compat reasons.

Concretely:

- **Renamed structs** — every caller in every crate updates
  in the same PR. No type aliases bridging old to new.
- **Changed `new(...)` signatures** — every call site is
  rewritten. No `#[deprecated]` overloads.
- **Removed code paths** (`polyfill_node!` macro, `ArgKind`
  enum variants, `wire_port_type_for` dispatch tables) — code
  gets deleted, not feature-flagged.
- **Workload-syntax breaks** (JsonObject, if we go that way)
  — workloads break and authors update them. No DSL-side
  compat shim.
- **Library `new(...)` API changes** — operators of the
  polydat crate are us; we update.

Sessions in this plan that *look like* they're about
managing compatibility (caller audits, simultaneous-update
expectations) are about **correctness** — the workspace must
still build and tests must still pass after the PR lands.
They are NOT about preserving the old surface in parallel.

### No "pre-existing issues"

**Every line of code in the library, every test result, every
runtime behaviour observed during this work IS OWNED BY THIS
DESIGN CHANGE.** When something breaks during a session,
fails an equivalence check, or produces an unexpected result
— the answer is not "that was pre-existing", "that's not
caused by my change", "git blame says someone else", or "this
was already broken." Those framings are out of scope; they
do not exist for this plan.

The committed posture is:

- **All code in the migration paths is yours.** If a node's
  eval behaviour changes during migration, the migration
  owns the new behaviour and either: (a) reproduces the
  old, or (b) explicitly states the new behaviour is intended
  in the session's exit notes.
- **All test results are yours.** A failing test during a
  session isn't a "flaky baseline" or "not my code" — it's a
  signal to investigate. If the failure is genuinely caused
  by something the session didn't touch, the session still
  owns the resolution (fix the broken test, fix the broken
  code, or document the deferral with a TODO that the next
  session inherits).
- **Equivalence-harness drift is yours.** If the equivalence
  harness starts producing different bytes between Phase 1
  and Phase 2 mid-session, the session investigates and
  resolves. "It was drifting before I started" is not an
  available defense.
- **CI failures introduced during a session are yours.** Even
  if the failing test is in a crate the session didn't touch,
  the session diagnoses why the change cascaded there. The
  Rust compiler doesn't lie about what depends on what.

This is sharper than the standard "own your changes" — it's
explicitly "own the *outcome* of the design change, including
behaviour you didn't write." Anyone picking up a session
inherits all the code and all the result-quality for the
slice they're working on.

If a session genuinely uncovers a defect that is wildly out
of scope (e.g. a corruption in `nbrs-metrics` discovered by
side-effect during S10 of the macro refactor), the right
move is **not** to ignore it as "not mine" — it's to file a
clear TODO, capture the diagnostic, and keep the session
focused while ensuring the defect is visible enough to be
addressed in a follow-up session. The exit notes mention it
explicitly.

### Complications require discussion, not drift

**If a session discovers that the design as written in
SRD-80b isn't crisp enough — or that a planned shape hits a
real feasibility wall — STOP and surface the complication.
Do not silently extend the macro, add an escape valve, or
introduce a special case to keep the session "on track."**

The architectural commitment is fragile in exactly one way:
each session can locally feel pressure to "just make this
one shape work somehow," and across 19 sessions those local
accommodations would erode the universal-set property that
makes SRD-80b worth doing. The escape valves we explicitly
rejected (NodeImpl trait, Value-slice fallback, per-shape
attributes) are easy to silently reintroduce under different
names. They mustn't be.

**Triggers that require discussion before proceeding**:

- A shape in SRD-80b's canonical-form table turns out to need
  a mechanism not contemplated by `Wire` / `ConstSource` /
  `#[poly_const]` (e.g. "this combinator wants to write to
  the outputs slice from inside extract", "this Wire impl
  needs to vary at runtime based on a sibling arg's type").
- A session needs to add a new attribute to the macro
  surface that isn't in the 13-attribute cap (10 macro-level
  + 3 arg-level) per SRD-80b §"What stays attribute-driven".
- A migration target genuinely can't be expressed through
  the macro at all — and the temptation is to leave a
  hand-written `impl PolydatNode` "just for this one".
- The macro source LOC budget (under 5000) is being violated
  to accommodate one specific node's quirks.
- A workload-syntax break would be required that doesn't
  fit cleanly into the universal-set shapes, and the
  workaround involves adding a special-case parser path.

**What "discussion" means concretely**:

- Stop the session. Don't merge what you have until the
  question is resolved.
- Write up the complication in the session's exit notes:
  what shape, what mechanism is missing or fighting the
  design, what the local-accommodation temptation is, what
  alternatives you can see.
- Surface it explicitly — open a discussion, ask for a
  call, file an issue. The form matters less than the
  visibility.
- The resolution is one of:
  1. **Sharpen SRD-80b** — the architectural doc gets an
     amendment that makes the new shape first-class. Macro
     evolves once, benefits every future node.
  2. **Defer the migration target** — this node stays on a
     parking-lot list; the macro doesn't grow for it; we
     decide later whether it's worth re-opening the
     architecture for that case.
  3. **Drop the migration target** — if the node turns out
     to be unnecessary or replaceable by composition, it's
     deleted from the library and the question dissolves.

The wrong resolution — the one this section exists to
prevent — is **"quietly extend the macro with a one-off
mechanism that handles this case and looks fine in isolation
but adds a new dimension to the combinatoric surface."**
That's the failure mode that turned SRD-80 into 12 buckets
in the first place.

**Operational rule**: if you find yourself writing a fourth
attribute to handle a shape that SRD-80b says should fit
through `Wire` and `ConstSource`, stop. The right move is to
discuss, not to ship the fourth attribute.

---

**Reading order:**

1. This doc (orientation + the session you're picking up).
2. [SRD-80b](../sysref/80b_macro_universal_authoring.md) §"Migration
   plan" for the architectural shape of the phase that session
   sits in.
3. The macro source (`polydat-derive/src/lib.rs`) and trait
   surface (`polydat/src/derive_support.rs`) for the current
   state.

**Cutover criteria** — "the new polydat system is live" when:

- Phase 2 has shipped (macro internally dispatches through the
  `Wire` / `ConstSource` traits; behaviour preserved across
  all existing migrations).
- Phase 4 has shipped (zero hand-written `impl PolydatNode for
  X` in `polydat/src/library/`).
- Phase 7 lint check is in place (CI fails if a new hand-
  written `impl PolydatNode` is introduced outside the macro's
  output).

Phases 5 and 6 land continuously after the cutover; they
expand the macro's surface and the fusion contract without
changing the "macro is the sole authoring path" commitment.

---

## Current state at the head of this plan

**Post SRD-80 PR B.15** (commit boundary at the time of
writing; counts verified against the source tree, not extrapolated):

- **47 `#[polydat_node]` attribute usages** across **17 files**
  in `polydat/src/library/` — these are the nodes already on the
  macro path. (Earlier drafts of this header said "~169
  migrated"; that number was aspirational or counted something
  else — possibly `FuncSig` entries including variadic arity
  expansions. The grepped attribute-usage count is the load-
  bearing baseline.)
- **110 hand-written `impl PolydatNode for X` blocks** across
  30 files in `polydat/src/library/`, **plus** ~83 `polydat::library::polyfill::polyfill_adapter!` macro
  expansions (each emits an `impl PolydatNode` of its own — they
  don't show up in a grep of source files but are real
  registrations). Effective hand-written-impl surface for the
  migration: **193 PolydatNode impls** across 31 files.
- Macro source: `polydat-derive/src/lib.rs` is **2,138 LOC**
  (single monolithic file) carrying explicit dispatch tables
  (5 ArgKind variants, 4 ConstShape variants, 7 WrapperWire
  variants, 3 JitType variants, 6 classify_* helpers, tuple-
  return special case).
- Workspace test count: **4262 passing**.

The migration order in Phase 4 below is set so each session
unblocks the next; you can also run them in parallel if you
trust independent branches.

---

## Session catalog

### Phase 1 — Trait foundation (one session)

#### S1 — Define `Wire` and `ConstSource` traits, populate with impls for every type the macro already recognises

**Entry preconditions:** PR B.15 merged; workspace 4262 tests
pass.

**Work scope:**

- Add `Wire` trait to `polydat/src/derive_support.rs` per
  SRD-80b §"Architecture (2)".
- Add `ConstSource` trait per §"Architecture (3)".
- Add `impl Wire for X` for every primitive, narrow integer,
  vector element type, Bytes shape, Json shape, Handle shape,
  `Value`. (Cross-reference the existing recognition tables in
  `polydat-derive/src/lib.rs` — the impl list should mirror
  what those tables accept today.)
- Add `impl ConstSource for X` for u64/f64/bool/String/&str.
- Add `Const<T: ConstSource>` wrapper if not already present
  in the new shape (the existing one works; just add the
  `T: ConstSource` bound).
- Each impl gets a round-trip unit test
  (`Wire::inject(v).extract() == v`).

**Exit criteria:**

- `cargo build -p polydat` succeeds.
- New trait impls have round-trip unit tests, all passing.
- **Workspace tests still 4262/4262** — traits are inert
  until Phase 2 wires the macro through them.
- No changes to `polydat-derive/src/lib.rs` in this session.

**Followups parked:**

- Any combinator impls (`Option<T>`, `&[(T1,T2)]`, etc.) —
  Phase 3.
- `Ext<T>` wrapper definition — Phase 3.

---

### Phase 2 — Refactor macro internals through the traits (3 sessions)

#### S2 — Replace `wire_port_type_for` and `classify_wrapper_wire` / `classify_vec_wire` with `Wire` trait dispatch

**Entry preconditions:** S1 merged.

**Work scope:**

- Delete `wire_port_type_for`'s match table; replace with
  `<#ty as polydat::Wire>::PORT` emission.
- Delete `classify_wrapper_wire`, `classify_vec_wire`,
  `classify_polywire`. Replace any structural matching with
  trait lookup (the macro just emits the trait path; rustc
  resolves it).
- Update `wire_type_to_jit_type` to read
  `<#ty as Wire>::JIT`.
- **Runtime semantics preserved** — every existing migration
  still produces the same `Value` for the same inputs. This
  is NOT about preserving the macro's internal API or
  attribute surface; it's about not breaking the equivalence
  harness. Internal cleanups are encouraged in the same PR.

**Exit criteria:**

- Workspace 4262/4262 still passing.
- `polydat-derive/src/lib.rs` is ~200-300 LOC smaller.
- Equivalence harness (Phase 1 eval vs Phase 2 compiled_u64)
  still passes for every JIT-eligible node.

**Risks:** Trait dispatch errors surface at the macro call
site (wrong place for the error message). Mitigation: emit
helper code that produces a useful error when `<#ty as Wire>`
fails to resolve (e.g. via a `where T: Wire` bound on a marker
fn whose error points at the function signature).

#### S3 — Collapse `ArgKind::Wire` / `PolyWire` / `Variadic` into one trait-dispatched path

**Entry preconditions:** S2 merged.

**Work scope:**

- The macro's `ArgKind` enum currently has 5 variants;
  `Wire`, `PolyWire`, and `Variadic` become one variant (or
  the enum is replaced with a struct that just carries the
  Rust type). All three paths go through `<T as Wire>::extract`.
- `PolyWire` was handled specially because the runtime port
  type was unknown at macro time. With Wire trait, it's just
  `<Value as Wire>` (where the impl returns a placeholder
  PORT and the macro fills in from a paired type parameter).
- `Variadic` recognition (`&[T]`) becomes `<&[T] as Wire>`
  via the blanket combinator impl (added in Phase 3, but the
  recognition arm can land now and stay broken until S5).
- Setup (`#[poly_const]`) stays attribute-based — separate
  path.
- Const (workload-literal) stays attribute-driven via
  `Const<T>` wrapper — uses `ConstSource` trait instead of
  `ConstShape` enum.

**Exit criteria:**

- `ArgKind` enum reduced to 2 variants (TypedArg + Setup) or
  becomes structural.
- Workspace 4262/4262 passing.
- Macro source is ~500-800 LOC smaller from PR B.15
  baseline.

**Risks:** Variadic codegen changes touch the `new()`
constructor's slot-extension loop. Triple-check the variadic
slot building stays correct.

#### S4 — Replace `ConstShape` enum with `ConstSource` trait dispatch

**Entry preconditions:** S3 merged.

**Work scope:**

- The macro's `ConstShape` enum (U64/F64/Bool/Str) gets
  removed; const dispatch goes through `<T as ConstSource>::extract`
  with `<T as ConstSource>::SLOT` providing the slot type.
- The build closure's `consts: &[ConstArg]` iteration uses
  trait extraction.
- `poly_default` attribute carries through — its expression is
  inlined into the build closure's fallback as before.

**Exit criteria:**

- `ConstShape` enum deleted from `polydat-derive/src/lib.rs`.
- Workspace 4262/4262 passing.
- Net macro LOC reduction from PR B.15 baseline: ~1000.

**Cutover checkpoint:** After S4, the macro is internally
trait-driven. Phase 2 complete; behaviour preserved.

---

### Phase 3 — Add combinator impls (3 sessions)

These sessions are independent and can run in parallel
branches.

#### S5 — `Option<T>` + `Ext<T>` combinator impls

**Entry preconditions:** S4 merged.

**Work scope:**

- Add `polydat::Ext<T: ReflectedValue + 'static>` wrapper to
  `polydat/src/derive_support.rs` per SRD-80b
  §"Architecture (2)".
- Add `impl<T: Wire> Wire for Option<T>`: `Value::None` →
  `None`; else `Some(T::extract)`.
- Add `impl<T: ReflectedValue + 'static> Wire for Ext<T>`:
  downcast via `value.as_ext_or_else(..)`. Inject upcasts
  through `Value::Ext(...)`.
- Update macro recognition: `Option<T>` and `Ext<T>` are
  recognised structurally; both reduce to `<T as Wire>` via
  the combinator impl.
- Add pilot tests in `polydat/tests/polydat_node_macro.rs`:
  one Option round-trip, one Ext downcast round-trip, both
  with eval + (if JIT-eligible) compiled_u64.

**Exit criteria:**

- Pilot tests pass.
- Workspace 4262/4262 + new pilot tests passing.

#### S6 — `&[(T1, T2)]` paired variadic

**Entry preconditions:** S4 merged.

**Work scope:**

- Add `impl<W1: Wire, W2: Wire> Wire for &[(W1, W2)]`:
  materialises a slice of pairs from the input slice (input
  count must be even; pair `i` reads `inputs[2*i]` and
  `inputs[2*i+1]`).
- Slot layout: `name_0a`, `name_0b`, `name_1a`, `name_1b`,
  ... with port types from `W1::PORT` and `W2::PORT`.
- Pilot test: `fn macro_pilot_paired(pairs: &[(bool, u64)]) -> u64`
  with selector-value semantics like PickN.

**Exit criteria:**

- Pilot tests pass.
- Workspace + pilot tests passing.

**Followup parked:**

- `&[(T1, T2, T3)]` triple-variadic — defer until a real
  use case lands.

#### S7 — `Vec<C: ConstSource>` workload-list combinator

**Entry preconditions:** S4 merged.

**Work scope:**

- Add `impl<C: ConstSource> ConstSource for Vec<C>`:
  materialises a Vec from the remaining const args (treating
  everything past this position as same-type list).
- Slot type: variadic const list.
- FuncSig arity emission: macro detects `Const<Vec<C>>` and
  emits `Arity::VariadicConsts { min_consts: 0 }`.
- Pilot test: `fn macro_pilot_allowed(x: u64, vs: Const<Vec<u64>>) -> u64`
  with allow-list check.

**Exit criteria:**

- Pilot tests pass.
- Workspace + pilot tests passing.

---

### Phase 4 — Migrate hand-written nodes (~7 sessions)

Run in any order after Phase 3 is complete. Each session
takes one file or one shape class.

#### S8 — Migrate partition.rs (Ext-typed nodes)

**Entry preconditions:** S5 merged.

**Work scope:** ~10 nodes in `polydat/src/library/partition.rs`.
All take `Ext<Partition>` and project to u64. Per the new
combinator, each becomes `fn cardinality(p: Ext<Partition>) -> u64`.

**Exit criteria:**

- `partition.rs` has 0 hand-written `impl PolydatNode`.
- Workspace + partition tests passing.

#### S9 — Migrate `Option<T>` nodes (RequiredU64, ThisOrU64)

**Entry preconditions:** S5 merged.

**Work scope:** 2 nodes in `polydat/src/library/param_helpers.rs`.
RequiredU64 → `fn required(input: Option<u64>, name: Const<&str>) -> u64`;
ThisOrU64 → `fn this_or(primary: Option<u64>, default: u64) -> u64`.

**Exit criteria:**

- Both nodes migrated.
- Existing `accepts_none_inputs` override semantics preserved
  (Option<T> wire makes the node None-tolerant by construction).

#### S10 — Migrate paired-variadic + workload-list nodes

**Entry preconditions:** S6 + S7 merged.

**Work scope:** PickN (`&[(bool, u64)]`), FixedValuesU64/F64/Str
(`Const<Vec<...>>`), IsOneOfU64 (`Const<Vec<u64>>`).

**Exit criteria:**

- 5 nodes migrated; pick.rs and fixed.rs and param_helpers.rs
  reach 0 hand-written impls for these specific shapes.

#### S11 — Migrate context.rs (capture-on-construction)

**Entry preconditions:** S17 merged (empty-source `#[poly_const]`).

**Work scope:** SessionStartMillis, ElapsedMillis, TmpDir,
CurrentEpochMillis, ThreadId.

Each uses `#[poly_const(fn, from = ())]` to capture state at
construction time.

**Exit criteria:**

- context.rs has 0 hand-written impls except `Env` (held
  back to S16).

#### S12 — Migrate JIT-heavy nodes (pcg.rs, sampling/icd.rs)

**Entry preconditions:** S4 merged (no combinators needed).

**Work scope:** Pcg, PcgStream, CycleWalk (pcg.rs); UnitInterval,
ClampF64, and other ICD nodes (sampling/icd.rs). Use
`compiled_u64_override` and `jit_constants_override`
attributes for the Phase 3 cases where the macro's auto-
inference doesn't suffice.

**Exit criteria:**

- pcg.rs and sampling/icd.rs migrated.
- JIT equivalence harness still byte-identical across phases.

#### S13 — Refactor + migrate weighted.rs (RefCell → atomic)

**Entry preconditions:** S4 merged.

**Work scope:** DynamicWeightedSelect currently uses RefCell
with `unsafe impl Sync`. Refactor to ArcSwap or atomic
double-buffer first, then migrate the now-Sync shape via
`#[polydat_node]` + `#[poly_const]` for the alias table.

**Exit criteria:**

- weighted.rs has 0 hand-written impls.
- No `unsafe impl Sync` in the migrated form.

**Risks:** Concurrency-correctness refactor; needs careful
review of the alias-table update path.

#### S14 — Migrate polyfill.rs adapter family (~83 type-conversion cells)

**Entry preconditions:** S4 merged.

**Work scope:** **Delete the `polyfill_adapter!` declarative
macro.** The current shape:
`polyfill_adapter!(U64ToI32, "__u64_to_i32", U64, I32, |v| {...})`
is one cell of the type-conversion matrix per
`polydat/docs/design/type_system.md` §3. There are **83 such
cells** in `polydat/src/library/polyfill.rs`. Each becomes a
standalone `#[polydat_node] fn ...`. The Wire trait's narrow-
int / vector / bytes / json impls make most adapter bodies
collapse to a single typed cast + range check, with the
PortType source/target inferred from the function signature
instead of declared positionally in the macro call.

The session is larger than the original "~16 nodes" framing
in early drafts of this plan — the count was wrong. 83 cells
× ~5 lines of body each is bulk, but uniform; an Edit pass per
cell is the right tool. The shape of every cell is the same
(one input wire, one output wire, panic on range-check fail);
no per-cell design decisions needed.

**Alternative considered**: introduce a new macro shape that
takes a pair of Wire types and emits the adapter automatically.
**Rejected for this session** — it's a third macro shape extension
(`#[polydat_adapter(from = X, to = Y)]`) and would expand the
attribute surface beyond the 13-attribute cap per SRD-80b
§"What stays attribute-driven". If the post-migration shape
turns out to fit a uniform fold over `(PortType, PortType)`
pairs, that's a separate post-cutover proposal — see SRD-80b
§"Open questions" parking lot. For this session: 83 individual
`#[polydat_node]` blocks, no special-case macro path.

Every direct caller of the `polyfill_adapter!`-exported struct
names (e.g. `U64ToI32`) is rewritten in the same PR to use the
new macro-generated struct names; no aliases. Per
greenfield, the type-system cell-naming convention can shift
too (e.g. `__u64_to_i32` → `u64_to_i32` if `__` prefix turns
out to be incidental) — flag at session entry whether to
rename or preserve.

**Exit criteria:**

- polyfill.rs has 0 hand-written `impl PolydatNode for X`.
- `polyfill_adapter!` macro is deleted from the source tree.
- All 83 cells migrated and emit through `#[polydat_node]`.
- `assembly::auto_adapter` / `boundary_adapter` callers still
  resolve to the correct nodes (this is the cross-crate caller
  audit; see Risk #5).
- Workspace builds and tests pass.

#### S14b — Migrate residual hand-written impls outside S8-S14 scope (~77 nodes across 22 files)

**Entry preconditions:** S5, S6, S7 merged (combinators
available); S15-S17 merged where their shapes are needed.

**Work scope:** The cutover criterion in §"Cutover criteria" is
"zero hand-written `impl PolydatNode for X` in
`polydat/src/library/`." S8-S14 explicitly cover 34 + 83 = 117
hand-written impls (5 specific files + polyfill.rs). The
remaining **~77 hand-written impls across ~22 files** must also
migrate before the cutover criterion is reachable. Inventory at
the head of this plan (verified 2026-06-05):

| File | Hand-written impls | Shape notes |
|---|---|---|
| `arithmetic.rs` | 4 | most likely already migratable via Wire scalar impls |
| `assertions.rs` | 2 | side-effecting; verify Purity expression |
| `bytebuf.rs` | 2 | Bytes wires |
| `datafile.rs` | 6 | likely Handle (Arc<Dataset>) or Ext shapes |
| `diagnostic.rs` | 1 | side-channel; verify Purity |
| `exactly_one.rs` | 1 | needs investigation |
| `format.rs` | 1 | Str wires |
| `hash.rs` | 3 | typed scalar ops + Bytes |
| `identity.rs` | 5 | overlaps with S15 (generic-over-Wire); ConstU64 is named in S15 |
| `json.rs` | 12 | Json wires; largest single-file residual |
| `lerp.rs` | 3 | F64 wires |
| `noise.rs` | 2 | F64 wires |
| `probability.rs` | 9 | F64 wires + sampling shapes |
| `random.rs` | 2 | typed scalar with state-bearing internals |
| `realer.rs` | 6 | F64 wires |
| `regex.rs` | 1 | `#[poly_const]` for compiled Regex |
| `string.rs` | 1 | Str wires |
| `vectors.rs` | 9 | typed-element vector wires |
| `sampling/alias.rs` | 1 | needs investigation |
| `sampling/histribution.rs` | 1 | needs investigation |
| `sampling/lut.rs` | 2 | needs investigation |
| `sampling/metashift.rs` | 2 | needs investigation |
| **Total** | **77** | — |

**Approach**: each file is its own sub-task; pick the smallest
first to validate the migration recipe before tackling
`json.rs` and `vectors.rs`. Most should fall through the
Wire concrete-type impls landed in S1 with no new macro shape.
Any node that genuinely doesn't fit the universal-set shapes
triggers the §"Complications require discussion, not drift"
process; do NOT silently extend the macro.

**Risk**: this session catalog entry is the only landed defense
against the cutover criterion being unreachable. If it's left
on the punch-list and never staffed, Phase 4 doesn't complete
and Phase 6/7 can't start. Schedule S14b explicitly — it can
parallelize with S8-S14 (different files, no shared edits).

**Exit criteria:**

- All 22 listed files have 0 hand-written `impl PolydatNode for X`.
- Per-file caller audits clean.
- Workspace builds and tests pass.
- Total `impl PolydatNode for X` in `polydat/src/library/` is
  exactly the count emitted by `#[polydat_node]` macro
  expansions (verified via grep on files NOT containing
  `#[polydat_node]` — the S19 lint check pattern).

**Followups parked:**

- `sampling/` sub-directory likely deserves its own design
  pass; sampling kernels have specific JIT requirements
  (icd.rs already in S12) that may surface during migration.
  Flag at session start which sampling shapes need a new
  combinator vs. fit existing ones.

---

### Phase 5 — Macro shape extensions for the residual (3 sessions)

These can interleave with Phase 4.

#### S15 — Generic-over-Wire instantiation (PortPassthrough, AssertType)

**Entry preconditions:** S4 merged.

**Work scope:** Resolve open question 1 in SRD-80b §"Open
questions" (instantiation policy). Implement chosen approach.
Migrate PortPassthrough (~5 instantiations across PortType
variants) and AssertType (one instantiation per PortType).

**Decision required at session entry:** Pick instantiation
policy from SRD-80b §"Open questions" item 1.

**Exit criteria:**

- PortPassthrough + AssertType migrated; identity::ConstU64
  also migrated using same machinery.
- Per-instantiation NodeRegistration generated; assembly's
  edge-adapter insertion sees the right concrete struct per
  type pair.

#### S16 — Fallible construction (Env)

**Entry preconditions:** S4 merged.

**Work scope:** Macro recognises `Result<T, E>` return on the
body of a node whose `new()` should be fallible. The macro
emits `fn try_new(...) -> Result<Self, String>`. The
PolydatNode factory dispatch propagates Err.

Note: this is **construction-time fallibility**, not eval-time.
Eval still panics on bad input by contract.

**Exit criteria:**

- Env migrated.
- Pilot test for fallible construction.

#### S17 — Empty-source `#[poly_const]`

**Entry preconditions:** S4 merged.

**Work scope:** Tiny extension to the `poly_const` attribute
parser: `from = ()` (an empty tuple) means "call the setup fn
with no args." The setup fn becomes `fn() -> T`. Pilot test
covers a node that captures a system-time value at
construction.

This unlocks S11 (context.rs migration).

**Exit criteria:**

- Macro parser accepts `from = ()`.
- Codegen emits no-arg call.
- Pilot test passes.

---

### Phase 6 — Enrich `NodeRegistration` for fusion (ongoing, starts with one session)

#### S18 — Define `NodeRegistration` shape; macro emits initial fusion metadata

**Entry preconditions:** Phase 4 complete (cutover done).

**Work scope:**

- Extend `polydat::dsl::registry::NodeRegistration` per
  SRD-80b §"NodeRegistration — the fusion contract".
- Macro emits the new fields (most have sensible defaults).
- Update consumers (`polydat::compile::fusion`,
  `polydat::compile::jit`) to read the new fields where ready.
- Set up the `decompose` attribute path:
  `#[polydat_node(decompose = path::to::fn)]`.

**Exit criteria:**

- `NodeRegistration` has the full field list from SRD-80b.
- Existing fusion logic reads the new fields (with defaults
  where it didn't previously have data).
- No regression in fusion test suite.

**Followups parked:** Each subsequent fusion capability that
needs new metadata is a separate session. They're additive,
local, and small — no upfront enumeration here.

---

### Phase 7 — Lock in the convention (one session)

#### S19 — Lint + docs + cleanup

**Entry preconditions:** Phase 4 complete (Phase 6 in
progress is fine).

**Work scope:**

- Add a lint or pre-commit check that fails when a new
  `impl PolydatNode for X` appears anywhere in
  `polydat/src/library/` outside the macro's expansion.
  (Approach options: clippy custom lint, syn-based scan in
  CI, or a simpler grep-based gate. Pick simplest viable.)
- Update `docs/sysref/12_polydat_stdlib.md` (or its successor)
  with the canonical-form table from SRD-80b §"Authoring
  patterns" as the operator-facing reference.
- Delete any retired infrastructure: the old `ArgKind` enum
  remnants, the old dispatch tables, the `polyfill_node!`
  macro source.
- Confirm `polydat-derive/src/lib.rs` is under 2000 LOC and
  builds in under 5 seconds (SRD-80b open question 4 budget).

**Exit criteria:**

- CI gates new hand-written `PolydatNode` impls.
- Docs reference SRD-80b's canonical-form table.
- Dead code removed.
- The "macro is the sole authoring path" invariant is now
  enforced, not just observed.

---

## Session dependency graph

```
S1
 ├─ S2 ─ S3 ─ S4 ───────────────────────────────┬─ S5 ─ S8 ────┐
 │                                              ├─ S5 ─ S9 ────┤
 │                                              ├─ S6, S7 ─ S10┤
 │                                              ├─ S12 ────────┤
 │                                              ├─ S13 ────────┤
 │                                              ├─ S14 ────────┤
 │                                              ├─ S15 ────────┤
 │                                              ├─ S16 ────────┤
 │                                              └─ S17 ─ S11 ──┤
 │                                                              │
 │                                                          S18 (Phase 6)
 │                                                              │
 │                                                          S19 (Phase 7)
```

S1 → S2 → S3 → S4 are strictly sequential (the macro refactor
is a single thread). After S4, the Phase 3 / 4 / 5 sessions
fan out and can run in parallel. S11 depends on S17. S18 and
S19 are tail-end.

---

## Risk register

1. **Macro error message regression.** Replacing dispatch
   tables with trait-call emission can move error sites away
   from the function signature toward generated code. Track
   error quality during S2-S4 and add `where T: Wire` marker
   bounds at the macro's recognition site if needed.

2. **JIT byte-equivalence drift.** Phase 1/2/3 equivalence
   harness must remain valid. Run it every session.

3. **Generic-over-Wire instantiation cost.** If we choose the
   "auto-instantiate all built-in Wire types" policy, compile
   times for nodes like PortPassthrough multiply by N (number
   of instantiations). Mitigation: prefer explicit
   instantiation in attribute, defer auto-instantiation until
   a real use case requires it.

4. **Inventory / linkme cross-version stability.** SRD-80b
   leaves the registration mechanism open. If `inventory` has
   issues at a future Rust stable, the macro's emission
   surface needs an alternative path. Keep registration
   abstracted behind a single macro-emission point.

5. **Caller updates across crates during Phase 4.** Migrating
   a node changes its `new(...)` Rust signature (e.g.
   `IsPositiveU64::new("name")` → `IsPositive::new("name".to_string())`).
   Every caller in every crate updates in the same PR (no
   aliases, no deprecation — greenfield). This isn't a risk
   so much as a workflow expectation: the session is not
   complete until the workspace builds. The "risk" framing
   only flags that if a session author misses a caller, the
   PR doesn't compile and the missing site is obvious.

6. **JsonObject's workload syntax is a real design call.**
   SRD-80b open question 2 — JsonObject's
   `(k1, v1, k2, v2)` interleaved workload syntax doesn't
   fit the universal-set shapes. Greenfield posture: the
   workload syntax breaks if that's the cleanest answer.
   Defer JsonObject migration to its own post-Phase 4
   session; pick the cleanest shape there (likely:
   `&[(&str, T)]` paired-variadic at the macro level,
   workload syntax becomes `[("k1", v1), ("k2", v2)]` or
   keeps the flat positional form via DSL-side desugar —
   either way, **the existing flat-positional syntax does not
   need to be preserved as a stable contract**).

7. **Lint enforcement complexity.** S19's "no new hand-written
   `impl PolydatNode`" lint is harder than it sounds because
   the macro's expansion contains the impl, so a naive grep
   catches the macro's own output. Mitigation: scan only
   files NOT containing `#[polydat_node]` for the impl
   pattern, OR use a syn-based AST scan that ignores
   macro-expanded code.

---

## Open meta-questions (decisions to make between sessions)

1. **Branching strategy.** S2-S4 are sequential and touch the
   macro; should they land on a single integration branch and
   merge once, or as separate PRs? Recommendation: separate
   PRs for review-ability; each is workspace-test-gated
   independently.

2. **Equivalence-harness depth.** Phase 1/2 (eval vs
   compiled_u64) is well-tested. Phase 3 (extern-inlined JIT)
   has narrower coverage. Should we expand the harness as
   part of S18 (NodeRegistration enrichment), or carry the
   current depth as the baseline?

3. **JsonObject syntax resolution timeline.** Defer to a
   post-Phase 4 session, or solve as part of S10 (paired-
   variadic migrations)?

4. **Documentation update cadence.** SRD-80b is the
   architectural truth. The operator-facing reference
   currently lives in `docs/sysref/12_polydat_stdlib.md` (and
   inline in source). Should every Phase 4 session update
   `12_polydat_stdlib.md`, or batch all doc updates into S19?

5. **(Resolved by greenfield posture)** Earlier drafts of
   this plan asked whether to keep legacy struct names as
   type aliases for one minor version when a migration
   renames them (`Hash64` → `Hash`). The answer is **no**.
   Atomic break, every caller updated in the same PR. This
   is the default for every renaming/reshaping decision in
   the plan; flag here only as a reminder for sessions where
   the caller count is large enough that someone might be
   tempted to argue for a shim.

---

## How to use this plan

When you sit down to work on SRD-80b:

1. Identify which session you're picking up (often the next
   one whose entry preconditions are met).
2. Read the session's section in this doc.
3. Read SRD-80b §"Migration plan" for the phase context.
4. Read the macro source (`polydat-derive/src/lib.rs`) and
   trait surface (`polydat/src/derive_support.rs`) for current
   state.
5. Execute the session. Update this doc's "Current state"
   header when you merge.

If a session uncovers a need to change the plan (e.g. an
unblocked-on-S17 finding emerges during S15), update the
relevant session's followups-parked list and note the change
in the risk register if it shifts risk.

This plan is a living document. Update it as decisions are
made; do NOT update SRD-80b (the architectural commitment) —
that's a stable anchor.

---

## Tooling discipline

**Prefer tools that operate robustly without per-step
supervision. Avoid tools whose correctness depends on
careful inline review of shell-variable elision, regex
anchoring, multi-line substitution, or approval-prompt
fatigue.**

This isn't a style preference — it's a defense against the
specific failure modes that show up during bulk migrations:
silent partial-success, confusing error messages from
half-expanded variables, and approval-friction pressure that
encourages "just say yes" on a hundred near-identical
prompts.

**Use** (robust, atomic, reviewable):

- `Edit` with `replace_all: true` per file for bulk renames
  across many sites. Atomic, visible, idempotent.
- `Write` for whole-file rewrites when a file is being
  meaningfully restructured.
- `grep -c`, `wc -l`, `grep -n`, `grep -rn` for counts and
  locations. Plain queries with plain output.
- `cargo nextest run --filterset` for targeted test runs.
- `cargo build -p <crate>` and `cargo test -p <crate>` for
  scoped verification.

**Avoid** (needs supervision, approval-friction, or has
known silent-failure modes):

- `sed -i` looped across files via shell variables. Variable
  elision, regex misfires, and silent partial-success make
  the failure mode invisible. The standing
  [[feedback_no_sed_bulk]] tripwire applies — use
  per-file `Edit` instead.
- `awk` for output filtering. Approval-friction makes every
  pipeline a checkpoint; the standing [[feedback_no_awk]]
  tripwire applies — use grep / wc / direct reading instead.
- Multi-shell-construction commands (`for f in $files; do
  sed -i ...; done`) — bash variable scoping plus
  command-substitution timing produces "no such file or
  directory" errors that look like environment problems but
  are actually the loop being malformed.
- Heredocs with embedded variable substitution unless the
  text is short enough to inspect inline.
- `find ... -exec ... {} \;` for bulk edits — same
  variable-elision issues as the sed loops.

**When in doubt**: write a single targeted `Edit` per file
instead of a single clever command across many files. The
extra tool calls are cheap; the recoverable-state property
they provide is not.

---
