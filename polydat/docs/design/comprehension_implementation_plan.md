# Comprehension Implementation Plan

Tracks the staged implementation of the polydat comprehension spec
(`polydat/docs/design/comprehension_forms.md`) in code. Each phase
has deliverables, test gates, and a checkpoint marker. Update the
checkpoint column as PRs land; everything else is the contract.

## Status overview

| PR | Phase | Status | Tests added | Total lib tests |
|---|---|---|---|---|
| PR 1 | 1 + 2: Foundation + Validation | ✅ landed | +42 | 1092 |
| PR 2 | 3: Metadata propagation + V4 lift | ✅ landed | +15 | 1107 |
| PR 3 | 4: Strategy implementations | ✅ landed | +52 | 1159 |
| PR 4 | 5: Predicate analyzer | ✅ landed | +26 lib +8 int | 1185 + 8 int |
| PR 5 | 6: Optimizer (R0a, R0b, R1-R7) | ✅ landed | +37 lib +9 int | 1222 + 17 int |
| PR 6 | 7: IR + compiler + interpreter | ✅ landed | +28 lib +11 int | 1250 + 28 int |
| PR 7 | 8: Consumption surfaces (§9.5) | ✅ landed | +12 lib +8 int | 1262 + 36 int |
| PR 8 | 9: Property-test integration suite | ✅ landed | +37 int | 1262 + 73 int |
| PR 9a | 10: Migration gate definition | ✅ landed | (baseline 93 nb-rs tests confirmed) | 1262 + 73 int |
| PR 9b | 10: KernelScope wiring + spec surface | ✅ landed | — | — |
| PR 9c-1a | 10: Parser routing through ComprehensionSpec | ✅ landed | — | — |
| PR 9c-1b | 10: Walker rewrite + synthesis dissolve | ✅ landed | — | — |
| PR 9c-2 | 10: Wire type switch to algebra::Comprehension | ✅ landed | — | — |
| PR 9c-3 | 10: interpolate_via_kernel → polydat::kernel::interp | ✅ landed | — | — |
| PR 9c-4 | 10: Executor migration to algebra runtime evaluator | ✅ landed | — | — |
| PR 9c-4b | 10: Delete iteration / order / bridge | ✅ landed | — | — |
| PR 9c-5 | 10: algebra::* → comprehension::* atomic rename | ✅ landed | — | 1276 + ~500 int |
| PR α | 11: EvaluatedSource contract — IndexFn is a runtime query | ✅ landed | +8 lib (eval_source + runtime regression) | 1277 |
| PR β | 11: Built-in generator registry — static-evaluable set | ⏳ planned | — | — |
| PR γ | 11: freeze_to_literal_ast — runtime → static AST snapshot | ⏳ planned (optional) | — | — |

## Constraints (from spec §13, §3.1, §10, §10.7.5, §9.1, §9.5.2, §14)

- **Staged migration** at cutover (PRs 9c-1a through 9c-5). PRs
  1–8 built the new algebra side-by-side in
  `polydat::comprehension::algebra::*`; legacy
  `polydat::comprehension::{ast, parse, eval, synthesis, ...}`
  kept working until the staged-cutover sequence reshaped the
  layout per
  [`comprehension_cutover_contact_surfaces.md`](comprehension_cutover_contact_surfaces.md).
  The algebra layer is now the canonical
  `polydat::comprehension::*`; the legacy flat-struct types
  live as parse-pipeline implementation details
  (`ast_legacy`, `parse`, `eval`, `spec/legacy_convert`).
- **Stream-first.** Sources are stream producers; no `Vec<Value>`
  materialization at clause level.
- **Required optimizer.** Spec §10 is part of the compilation
  contract, not optional.
- **Closed-enum discipline.** `IndexFn`, `NaturalOrder`,
  `Materialization`, `StrategyName` are closed; no callbacks, no
  registration hooks.
- **Public IR is immutable.** `polydat::comprehension::ir::Program`
  is `#[non_exhaustive]` and accessible by value only.
- **Independence contract.** Three consumption surfaces share IR
  but maintain independent dispense state.
- **Explicitly deferred** (no code built): R8/R9/R10 optimizer
  rules, continuous-coord predicate analysis, cross-streamer
  sharing, strategy extensibility surface, filter-cost-aware
  optimizer.

---

## PR 1 — Phases 1 + 2: Foundation + Validation ✅

**Goal:** Type system + V1-V9 axioms. No behavior beyond
validation; coexists with legacy `ast` module.

**Module:** `polydat/src/comprehension/algebra/`

### Phase 1 — Foundation types ✅

- [x] `ast.rs` — 6-variant `Comprehension` enum (clause,
  cartesian, zip, union, filter, order); construction helpers;
  `coordinate_names()`, `children()`, `node_count()`, `depth()`.
- [x] `source.rs` — `Source` enum: 4 discrete variants (Literal,
  IntRange, Generator, WorkloadParamList) + 2 continuous
  (ContinuousInterval, Distribution); `cardinality()`,
  `is_discrete()` / `is_continuous()`.
- [x] `cardinality.rs` — 6 `CardinalityClass` variants; `Interval`,
  `ProductMeasure`, `MeasureName`, `Hybrid`; V8 integrability
  check on measures.
- [x] `strategy.rs` — `StrategyName` (10 variants), `ZipMode` (3);
  classification helpers (`is_streaming`, `is_index_sampling`,
  `is_lattice_geometric`).

### Phase 2 — Validation ✅

- [x] `validate.rs` — `validate(c, mode)` entry; per-axiom checks
  V1-V9; `Mode::{Permissive, Strict}`; `ValidationError` typed
  variants; `ValidationWarning` for §5.8 degenerate forms.
- [x] V4 stub: structural check only (no metadata) — lifted in
  PR 2.

### Acceptance gates ✅

- [x] All 42 new tests pass.
- [x] `cargo build --lib` clean (no warnings).
- [x] Full lib regression: 1092 / 1092 pass.

---

## PR 2 — Phase 3: Metadata propagation + V4 lift ✅

**Goal:** Bottom-up metadata bundle per spec §10.7; lift V4 from
structural stub to per-strategy `IndexFn` check.

### Phase 3 — Metadata propagation ✅

- [x] `metadata.rs` — `Metadata` bundle (cardinality,
  index_addressable, natural_order, materialization).
- [x] `IndexFn` (6 variants: Lattice, Lockstep, Modular,
  Concatenation, Continuous, Hybrid).
- [x] `NaturalOrder` (5 variants: Lex, Lockstep, Sequential,
  Strategy, PendingSampling).
- [x] `Materialization` (Streaming, BoundedBarrier, UnboundedBarrier).
- [x] `Comprehension::metadata()` propagation entry.
- [x] Per-constructor propagation rules per spec §10.7.2:
  clause, cartesian (with dependent-source detection), zip
  (per-mode), union, filter, order (Lex inherits, non-Lex
  produces None at AST level + records barrier size).
- [x] Dependent-source detector — walks source expressions for
  `{name}` back-references to earlier-axis names.

### V4 lift ✅

- [x] Replace V4's structural-only stub with metadata-based
  per-strategy `IndexFn` check.
- [x] Lattice-geometric strategies require `IndexFn::Lattice`;
  Concatenation rejected; Lockstep/Modular warns
  (DegenerateGeometric); 1-axis Lattice warns.
- [x] Index-sampling strategies (Halton/Sobol/Lhs/Shuffle/ReverseLex)
  accept any non-None IndexFn; continuous acceptance per §3.6.
- [x] V5 one-filter look-through: metadata target is `child.child`
  when child is Filter; nested filters error (F1 / R6 needed).
- [x] V6 lifted from direct-source check to metadata cardinality.

### Acceptance gates ✅

- [x] All 15 new tests pass; total 57 algebra tests passing.
- [x] Existing PR 1 tests still pass.
- [x] `cargo build --lib` clean.
- [x] Full lib regression: 1107 / 1107 pass.

---

## PR 3 — Phase 4: Strategy implementations ✅

**Goal:** Algorithmic implementations of every named strategy.
Each strategy has two surfaces: naïve (materialize input + apply)
and indexed (R2 push-down with closed-form draws).

### Deliverables

- [x] `strategies/` submodule:
  - [x] `Strategy` trait — `naive_apply`, `indexed_apply`,
    `accepts_input(idx)`, `has_closed_form_for(idx)`.
  - [x] `lex.rs` — pass-through.
  - [x] `reverse_lex.rs` — N-1 down to 0.
  - [x] `shuffle.rs` — PRNG with captured seed.
  - [x] `halton.rs` — base-2 Halton (K-D + 1-D + continuous).
  - [x] `sobol.rs` — base-2 van-der-Corput per axis with
    per-axis offset (degenerate Sobol; Joe-Kuo direction
    numbers tracked as a follow-up for production-quality QMC).
  - [x] `lhs.rs` — per-axis stratified permutation.
  - [x] `extrema.rs` — 2^N lattice corners + distance sort.
  - [x] `shells.rs` — concentric shell partition.
  - [x] `diagonal.rs` / `antidiagonal.rs` — index-sum walks.
  - [x] `prng.rs` — thin stateful wrapper over polydat's
    PCG-RXS-M-XS (`nodes/pcg.rs::pcg_seek`). Single PRNG
    family across the crate; Phase 7's per-streamer seed
    wiring just passes through `(seed, stream)` to the same
    wrapper.

### Test gates

- [x] Per-strategy correctness: known-output tests for
  radical-inverse (Halton/Sobol building block), lattice
  enumeration (Diagonal/Antidiagonal), corner enumeration
  (Extrema), shell stratification (Shells).
- [x] Determinism: every strategy produces stable output
  given stable input.
- [x] Working-set size: indexed_apply outputs are O(output),
  not O(input) — verified via the per-strategy tests.
- [x] Continuous-input tests: Halton/Sobol/Lhs over `[0,1)^K`
  produce expected sequences.
- [x] Per-strategy `accepts_input` tests align with spec §3.6.

### Known follow-ups (deferred to a later refinement push)

- Sobol uses degenerate identity direction numbers; production-
  grade Sobol requires Joe-Kuo direction numbers per axis. Not
  blocking — the spec's per-strategy correctness contract is
  "stable + decidable permutation rule," which the current
  impl satisfies.
- PRNG choice: see "PRNG choice" discussion below.

### Actual surgery

~1700 lines new code + tests across 11 files.

---

## PR 4 — Phase 5: Predicate analyzer ✅

**Goal:** Structured analysis of GK boolean expressions used as
filter predicates per spec §10.9.

### Deliverables

- [x] `predicate/` submodule:
  - [x] `info.rs` — `PredicateInfo` struct, `Factorization`
    (4 variants), `Monotonicity`, `RangeConstraint`,
    `Determinism`, `OpaqueReason` (5 variants), `ConstValue`,
    `PerAxisMap<T>` insertion-order-preserving helper.
  - [x] `coordset.rs` — `CoordSet` with per-coord
    discrete/continuous classification;
    `CoordSet::from_metadata(coord_names, metadata)` builds
    from a comprehension's metadata bundle.
  - [x] `analyzer.rs` — `analyze(predicate, coords)` entry
    that delegates to the recognizer catalog.
  - [x] `recognizers.rs` — §10.9.5 pattern catalog:
    `{a} OP K` (6 ops, also reversed `K OP {a}`), `{a} OP {b}`,
    `p1 && p2` (recursive with PerAxis merge + range
    intersection), `p1 || p2` (recursive with Disjunctive
    fallback), `!p` (with monotonicity inversion),
    `K1 <= {a} && {a} <= K2` (range fold via conjunction),
    `{a} in [K1, K2, K3]` (discrete-set membership).
    Continuous-coord short-circuit → `Opaque(Continuous)`.
    Nondeterministic-function detection → `Determinism::Opaque`.

### Test gates

- [x] Per-recognizer unit tests on hand-crafted predicates
  (26 tests across info.rs, coordset.rs, recognizers.rs,
  analyzer.rs).
- [x] Soundness property tests (8 integration tests):
  randomized tuple generation via inline PCG-style RNG;
  for every asserted `RangeConstraint`, evaluating the
  original predicate against random tuples agrees with
  the asserted decomposition.
- [x] Conservative-incompleteness test: unknown patterns
  produce `Opaque(UnknownPattern)`.
- [x] Determinism test: same `(predicate, coords)` produces
  the same `PredicateInfo` on every call.
- [x] Cross-axis non-factoring: `{k} == {limit}` does NOT
  factor as PerAxis (the soundness-breaking case).

### Notes

- The recognizer catalog walks the predicate **string** rather
  than a pre-parsed GK AST. Pre-parsed AST input is a future
  extension; not needed for R5 (Phase 6) which only consults
  the structured factorization output.
- Nondeterministic-function detection is text-pattern-based
  (recognizes `random`, `pcg(`, `now(`, etc.). A future
  refinement could consult polydat's GK kernel
  `requires_seed` flags; today's text check is conservative
  (false positives mark `Opaque`, never false negatives).

### Actual surgery

~1500 lines new code + 8 integration tests.

---

## PR 5 — Phase 6: Optimizer (R0a, R0b, R1-R7) ✅

**Goal:** AST → AST rewriter producing canonical, push-down form.
Required pass upstream of compilation per spec §10.

### Deliverables

- [x] `optimize/` submodule:
  - [x] `mod.rs` — `optimize(c)` top-level + bottom-up walker
    + fixed-point loop. `analyze_reducibility(c)` is the
    §10.10 entry point.
  - [x] `finding.rs` — `ReducibilityFinding`, `Reduction`
    (Replace / Rewrite variants), `ComplexityDelta`,
    `RuleId` (12 variants including deferred R8/R9/R10),
    three-way `Ordering`.
  - [x] R-rule implementations:
    - [x] `r0a_identity.rs` — I1-I5 elimination.
    - [x] `r0b_flatten.rs` — A1, A2 flattening (union +
      cartesian); zip correctly excluded per A3.
    - **R1, R2 — not AST rewrites.** Metadata-driven IR
      compilation decisions; recorded in the reducibility
      catalog as IR-compilation eligibilities. Phase 7's IR
      compiler reads `metadata.materialization` and emits
      `ORDER_STREAMING` (R1) or indexed `ORDER_MATERIALIZE`
      (R2) accordingly. No AST shape change needed.
    - [x] `r3_commute.rs` — `order(filter, Lex, None)` ↔
      `filter(order, Lex, None)`.
    - [x] `r4_distribute.rs` — filter over union (D1).
    - [x] `r5_factorize.rs` — per-axis filter pushdown via
      predicate analyzer's PerAxis factorization.
    - [x] `r6_filter_fold.rs` — chained filter folding (F1).
    - [x] `r7_order_fold.rs` — order chain folding (O1)
      with O2 guard (inner truncation → no fire).
- [x] Fixed priority order: R0a → R0b → R3 → R4 → R5 → R6 →
  R7. R1 and R2 are metadata-realized.
- [x] Bottom-up walker rebuilds parent on child rewrite.
- [x] Defensive `node_count^2` step bound for loop
  termination.
- [x] §10.6 contract: total (no rejections), idempotent
  (verified via test), each rule strictly improves at least
  one complexity dimension.

### Test gates

- [x] Per-rule unit tests: 37 lib tests across the 6 AST-
  rewriting rules + 4 finding-type tests.
- [x] Idempotence test (optimize(optimize(C)) == optimize(C))
  on randomly-structured ASTs.
- [x] Integration tests (9 in `tests/optimizer_worked_examples.rs`):
  spec §11.5 filter-distribution example; R7 + R0a
  composition; R6 chained-filter folding; R0b cartesian
  flattening; idempotence on messy AST; empty finding on
  canonical input; ReducibilityFinding rule-tagging; coord-
  name preservation; zip non-flattening.
- [x] No regression in lib (1222 total).

### Notes

- The R5 implementation takes the predicate analyzer as a
  closure parameter so the optimizer module doesn't tightly
  couple to predicate internals. The default loop uses
  `predicate::analyze` per spec §10.9.
- §10.6 properties 1 (semantic-preserving) and 4 (bounds-
  improving) are verified at the per-rule level: each rule's
  rewrite is an instance of a §7 equivalence (so dispense
  sequence is preserved by construction), and the
  improvement vector is checked via the integration tests.
  Full §9.2 equivalence-against-naive will land in PR 8
  when the IR interpreter exists to execute both sides.

### Actual surgery

~1400 lines new code + tests across 9 files.

---

## PR 6 — Phase 7: IR + compiler + interpreter ✅

**Goal:** Compile optimized AST to the 8-opcode IR and execute it
per spec §9.1 + §9.2.

### Deliverables

- [x] `ir/` submodule:
  - [x] `op.rs` — 8 opcodes (PUSH_CLAUSE, CARTESIAN, ZIP, UNION,
    FILTER, ORDER_STREAMING, ORDER_MATERIALIZE, DISPENSE) +
    `OrderStreamingKind`, `Op::stack_effect()`,
    `Op::is_barrier()`.
  - [x] `program.rs` — `#[non_exhaustive] Program` wrapper;
    `stack_depth()`.
  - [x] `compile.rs` — bottom-up AST walker; R1 + R2 dispatch
    via metadata (`indexed: bool` on `OrderMaterialize`).
  - [x] `interpreter.rs` — `TupleStream` trait; ClauseStream,
    CartesianStream (lazy axis-0 + cached axes 1..N),
    ZipStream (Strict/Truncate streaming + Cycle barrier),
    UnionStream, FilterStream, OrderStreamingStream,
    OrderMaterializeStream. Predicate evaluator covering
    §10.9.5 catalog.
  - [x] `bounds.rs` — `check_bounds(program) -> ResourceBound`
    with per-barrier `Bound` entries; sum via
    `total_barrier_working_set()`.
- [x] Architecture design doc:
  `polydat/docs/design/ir_architecture.md`. Explains the
  stack-machine + stream-operand model for other devs; covers
  the R1/R2 IR-vs-AST boundary, materialization barriers,
  trait-object choice, and the "how to add a new opcode"
  procedure.

### Test gates

- [x] Per-opcode unit tests (Op + Program — 7 tests).
- [x] Per-stream-type tests in interpreter.rs (10 tests
  covering all 7 stream types).
- [x] Compilation round-trip tests in compile.rs (6 tests).
- [x] Resource-bound checker tests in bounds.rs (5 tests).
- [x] End-to-end integration tests
  (`tests/ir_end_to_end.rs`, 11 tests) including two §9.2
  equivalence tests: naïve vs optimized produce identical
  dispense sequences on filter+cartesian and nested-
  combinator ASTs.
- [x] Full lib regression passes (1250 total).

### Notes

- `OrderMaterialize` currently runs in the naïve form for
  both `indexed: true` and `indexed: false` cases. The
  indexed path's full closed-form `IndexFn` lookup is
  tracked as a follow-up — the strategy implementations
  (PR 3) already expose `indexed_apply`, but threading the
  source-evaluator and lookup function through the IR
  interpreter is a separate piece of work. The current
  impl is correct (produces the same dispense sequence) but
  doesn't achieve R2's O(output) working-set guarantee yet.
  Phase 9's property suite will catch any regression when
  the indexed-form wiring lands.
- Predicate evaluator is sufficient for §10.9.5 catalog
  patterns. Unknown predicates evaluate to `true`
  (conservative pass-through); production wiring to
  polydat's GK expression evaluator is a Phase 9 task.
- `Source::Generator` / `Source::WorkloadParamList` /
  continuous sources currently exhaust to `None` in the
  interpreter — they need runtime evaluator wiring that
  Phase 9 (nb-rs migration) provides.

### Actual surgery

~1500 lines new code + 11 integration tests + design doc.

---

## PR 7 — Phase 8: Consumption surfaces (§9.5) ✅

**Goal:** Three independent first-class consumption types over
the shared compiled IR.

### Deliverables

- [x] `surfaces/` submodule:
  - [x] `compiled.rs` — `CompiledComprehension` (entry point)
    holding `Arc<Program>`; factory methods for the three
    surfaces; `program()` accessor exposing the immutable IR.
  - [x] `coord_stream.rs` — `CoordinateStream` (first-order),
    per-streamer interpreter graph, implements `Iterator`.
  - [x] `scoped_stream.rs` — `ScopedKernelStream<K>` (second-
    order), wraps a `CoordinateStream` + parent kernel,
    implements `Iterator`.
  - [x] `scope_once.rs` — `scope_once<K>(&parent, &coords)`
    standalone function + `CompiledComprehension::scope_once`
    method (both pure, no cursor consulted).
  - [x] `instance.rs` — `KernelScope` trait (algebra-layer
    abstraction over "a thing that can be scoped to a coord
    tuple"); `ScopedKernelInstance<S>` carrying coords +
    scoped value.
  - [x] `mod.rs` — re-exports + `compile()` convenience
    function.

### Test gates

- [x] Independence test: two `CoordinateStream` from same
  comprehension advance independently (verified via partial-
  pull + sibling-still-full).
- [x] Cross-surface independence in both directions: pulling
  from `CoordinateStream` doesn't advance
  `ScopedKernelStream`, and vice versa.
- [x] `scope_once` consistency with `ScopedKernelStream` —
  same coords → same scoped instance.
- [x] Concurrent-pull test: 8 threads, each with its own
  streamer, no data races, all produce identical sequences.
- [x] IR-sharing test: 100 streamers share `Arc<Program>`,
  no recompilation (verified via `Arc::strong_count`).
- [x] `scope_once` doesn't advance any cursor (verified via
  stream-state-preserved test).

### Notes

- `KernelScope` is the algebra-layer abstraction; nb-rs's
  `GkKernel` implements it during the Phase 9 (PR 9)
  migration. Tests use a `MockKernel` to exercise the
  surfaces end-to-end without pulling in the full GK
  runtime.
- The `CompiledComprehension` type is the spec's
  "comprehension handle" (§9.5.2). It owns the
  `Arc<Program>`; factory methods clone the Arc for
  per-streamer state. This makes IR sharing structural
  (sibling streamers literally hold the same Arc).

### Actual surgery

~900 lines new code + 8 integration tests.

---

## PR 8 — Phase 9: Property-test integration suite ✅

**Goal:** Cross-phase confidence. Property tests that exercise
the full pipeline.

### Deliverables

- [x] `polydat/tests/equivalence_harness.rs` — random AST
  generator (PCG-based, V1-V9-valid) + naïve-vs-optimized
  pipeline comparison. 9 tests including 3 random-AST
  harnesses at depths 2/3/4 (~2000 cases total), plus
  idempotence, determinism, validation totality, and
  optimizer-no-rejection properties.
- [x] `polydat/tests/spec_section_11_worked_examples.rs` —
  16 tests, one per §11.x example (11.1 through 11.13 plus
  resource-bound sanity).
- [x] `polydat/tests/resource_bounds_verification.rs` —
  12 tests verifying §9.3 bound checker output matches
  actual program shape (streaming programs → no barriers;
  materializing programs → barrier with truncation-sized
  working set; chain-folding case verifies R7 reduces
  barrier count).
- Skipped (deliberate, with rationale below):
  - `comprehension_strategies.rs` — already covered by
    per-strategy unit tests in PR 3 (~52 tests).
  - `comprehension_concurrency.rs` (loom/shuttle) — the
    existing thread-based test in
    `surfaces_independence.rs::concurrent_pulls_no_data_races`
    catches data races. The §9.5.2 independence contract
    is structural (no shared mutable state); model
    checking would mostly confirm the design. Adding
    loom as a dep is a separate decision to make when
    the runtime concurrency surface (Phase 9 / PR 9)
    needs it.

### Test gates

- [x] Equivalence harness: ~2000 random ASTs validated +
  compared; zero §9.2 failures.
- [x] Worked-example tests: 16 spec §11 examples passing.
- [x] Bound checker: 12 verification tests passing.
- [x] No regression: full lib test suite passes.

### Notes

- The equivalence harness uses `"true"` predicates and `Lex`
  orderings so dispense sequences are deterministic and
  predictable across naïve and optimized pipelines. Random
  predicates and random strategies would require coord-aware
  generation; the current shape exercises R0a, R0b, and the
  identity-equivalences extensively (which is where most
  optimizer bugs would land).
- The depth-4 harness runs ~100 seconds; this is acceptable
  for CI but is a known long-tail test.

### Actual surgery

~1000 lines new test code (3 integration files).

---

## PR 9 — Phase 10: nb-rs migration + cutover ⏳

**Goal:** Atomic per spec §13. Replace every nb-rs consumer of
the old `Comprehension` shape with the new polydat surfaces.

### Pre-cutover audit

- [ ] `grep -rn 'polydat::comprehension' nbrs-*/` — list every
  call site.
- [ ] Test workloads catalogued for the regression gate below.

### Migration steps

- [ ] **Parser** (`nbrs-workload`): YAML parser produces new
  operator-tree `Comprehension` directly. Validation via
  `validate(_, Strict)` for the workload-load path.
  No continuous-source parser yet (SRD-18c follow-up
  deferred).
- [x] **Executor** (`nbrs-activity`): `runtime_iterate` calls
  `polydat::comprehension::runtime::evaluate_for_iteration`
  directly; per-iter kernel built via `GkKernel::for_iteration`.
  Two-branch Cartesian/Union dispatch collapsed into one
  algebra-native path (PR 9c-4).
- [x] **Scope-tree** (`nbrs-activity`):
  `ScenarioNode::Comprehension` field is
  `polydat::comprehension::Comprehension` (the algebra type)
  end-to-end (PR 9c-2).
- [x] **Cutover staged PRs (9c-1a through 9c-5)**:
  - [x] `polydat::comprehension::synthesis` dissolved (9c-1b).
  - [x] `polydat::comprehension::{iteration, order}` deleted
    (9c-4b).
  - [x] Algebra-to-legacy bridge deleted (9c-4b phase 2).
  - [x] `polydat::comprehension::algebra::*` promoted to
    `polydat::comprehension::*` via file moves (9c-5). Legacy
    flat-struct types live in `ast_legacy`, `parse`, `eval`,
    `spec/legacy_convert` as parse-pipeline implementation
    details.
  - [x] Public API exports match spec §3, §9.5, §10.7.

### Workload regression gate (load-bearing)

- [x] 93-test baseline regression gate defined in
  [`comprehension_migration_gate.md`](comprehension_migration_gate.md).
- [x] Re-run under the new shape (post-migration) at every push.
- [x] **All 93 baseline tests stayed green at every push**, plus
  2727 unit tests across polydat / activity / workload.

### Post-cutover sweep

- [ ] Full workspace test suite passes.
- [ ] Benchmark suite: no >10% regression on parameter-sweep /
  iteration-heavy workloads.

### Estimated surgery

~1500 lines changes (mostly deletions + thin wrappers).

---

## PR α / β / γ — Phase 11: EvaluatedSource + registry + freeze ⏳

**Origin.** Surfaced after running
`full_cql_vector_sweep` (PR 9c-5 cutover, 2026-05-28). The
`Generator` source variant is opaque post-evaluation —
strategies receive `IndexFn::None` and any non-Lex strategy
(Diagonal, Extrema, Shells, Halton, Sobol, Lhs) fails V4 at
strategy-invocation time even when the generator's expansion
is a perfectly valid `Lattice` / `Continuous`. The fix is the
unified design recorded in spec §10.7.0 / §10.7.6 / §10.7.7
/ §10.7.8: lift `IndexFn` from a static AST property to a
contextual query that fires at evaluation time, layer a
built-in generator registry that recognizes name+args as
known shapes (so static evaluation works for the common
cases), and provide a one-way `freeze_to_literal_ast`
serialization helper for the cases that need a paper-trail
of "what was actually expanded".

The three pushes compose; α is foundational and must land
first.

### PR α — Phase 11a: EvaluatedSource contract (load-bearing) ✅

**Goal.** Stop asking the AST "what is your IndexFn?" Ask
the source at evaluation time, with context.

- [x] New `polydat::comprehension::eval_source` module:
  - [x] `pub struct EvaluatedSource { values: Vec<Value>,
        cardinality: u64, index_fn: IndexFn }` (spec
        §10.7.6).
  - [x] `pub enum EvalError { NeedsContext, EvalFailed { ... } }`.
  - [x] `pub enum EvalClass { Static, ContextRequired,
        Distribution }` (spec §10.7.0).
  - [x] `pub trait SourceEval { fn eval_class(&self) -> EvalClass;
        fn evaluate(&self, ctx: Option<&EvalContext>)
        -> Result<EvaluatedSource, EvalError>; }`.
- [x] Implement `SourceEval` for every AST source variant
  (`Literal`, `IntRange`, `ContinuousInterval`,
  `WorkloadParamList`, `Generator`, `Distribution`).
- [x] **`Generator::evaluate` naive path** (no registry yet):
  expand-then-classify via `classify_observed_values`, emits
  `Lattice { axis_sizes: [N] }` from observed length. Cheap
  correctness floor; PR β replaces it for registry-recognized
  cases with declared-without-expansion shapes.
- [x] **Strategy invocation contract** (spec §10.7.8): rewired
  the runtime evaluator (`runtime::evaluate_for_iteration`)
  so each node threads `Option<IndexFn>` alongside the
  materialized tuples; the Order node builds an
  `EvaluatedInput` (spec §10.7.8 strategy-input carrier) and
  calls `Strategy::apply`. V4 fires here, definitively, via
  `RuntimeError::StrategyRejectsInput`.
- [x] **Retired the `naive_apply` / `indexed_apply` split**.
  Single `Strategy::apply(input: &EvaluatedInput,
  truncation: Option<u64>)` method on the trait; internal
  helpers (`*_multi_indices`, etc.) live as private functions
  in each strategy module. Default-impl dispatch in `apply`
  routes to indexed-form via `multi_index_to_flat` (lookup
  against `input.tuples`) when `index_fn_supports_lookup`,
  else per-strategy fallback over the materialized tuples.
- [x] IR `OrderMaterialize` opcode enriched with
  `input_index_fn: Option<IndexFn>`; the IR compiler grabs it
  from the upstream child's metadata at compile time, so the
  IR interpreter passes the actual combined index_fn into
  `Strategy::apply` (not just a 1-D length stand-in).
- [x] Compile-time V4 best-effort: existing `validate::visit_order`
  flow already fires V4 against static AST metadata; the
  module-level doc on `validate.rs` now documents the two-
  tier contract (static best-effort + runtime definitive).
  No new compile-time fire needed for PR α — static and
  post-eval IndexFn agree for every supported case until
  PR β's registry promotion changes that.

**Test gates landed.**
- [x] `eval_source::tests` — 7 cases: Literal / IntRange
  evaluate without context, Generator + WorkloadParamList
  error without context, Continuous / Distribution yield
  Continuous IndexFn, Generator with context evaluates to
  Lattice.
- [x] `runtime::tests::extrema_over_cartesian_uses_indexed_form`
  — bug regression: Extrema over a 3x3 cartesian product
  yields the 4 corners (was: first/last/prefix-filler via the
  old `naive_apply`). This is the load-bearing fix for the
  `full_cql_vector_sweep` workload's `order: extrema/1` bug.
- [x] Existing 5 runtime walker tests + every strategy
  module's per-strategy test still passes.
- [x] Workspace `cargo test --workspace`: 1849 tests across
  60 test groups, 0 failures. Polydat lib: 1269 → 1277
  (delta = 8 new tests in eval_source + runtime regression).

### PR β — Phase 11b: Built-in generator registry

**Goal.** Recognize the common `Generator` patterns as known
shapes *without* running them. Promotes them from
`ContextRequired` to `Static`.

Depends on PR α (registry consumers go through `Source::evaluate`).

- [ ] New `polydat::comprehension::generators::registry`
  module (spec §10.7.7):
  - [ ] `pub trait BuiltinGenerator { fn evaluate(&self,
        args: &[Value]) -> Result<EvaluatedSource, EvalError>; }`.
  - [ ] Inventory-registered impls for: `range`, `fib`,
    `pow2`, `linear_steps`, `geometric`, `concat`,
    `partitions`, `subdivide`.
  - [ ] Each impl declares its produced `IndexFn` from
    args without expanding (e.g., `range(0, 100, 5)` →
    `Lattice { stride: 5, count: 20 }`).
- [ ] `Generator::eval_class` returns `Static` iff registry
  recognizes the generator name *and* args are themselves
  static.
- [ ] Wire IR planner's compile-time V4 fire to cover
  registry-recognized generators.

**Test gates.**
- [ ] Per-generator table-driven test: known input args →
  known `EvaluatedSource`.
- [ ] Mixed AST: `Cartesian(range(0,100,5), my_workload_param)`
  — the range half is `Static`, the param half is
  `ContextRequired`; whole comprehension classifies as
  `ContextRequired` (worst-class wins).
- [ ] V4 compile-time fire surfaces strategy/source
  mismatches at parse for fully-registry workloads.

### PR γ — Phase 11c: freeze_to_literal_ast (optional)

**Goal.** Serialization helper. Given an `EvaluatedSource`,
produce a `Source::Literal { values }` AST node so the
evaluated result can be persisted, diffed, or pasted back
into a workload. Strictly one-way.

- [ ] `pub fn freeze_to_literal_ast(eval: &EvaluatedSource)
      -> Source` returning `Source::Literal`.
- [ ] Idempotence test: `freeze(literal_source.evaluate(None))
      == literal_source`.

**Optionality.** γ is not load-bearing for any current
workload; it exists for tooling / audit / "what did this
runtime-generator actually expand to last night?" use cases.
Defer if scope pressure.

### Acceptance for Phase 11

- [ ] `full_cql_vector_sweep` (and any other workload that
  pairs a `Generator` source with a non-Lex `order:` strategy)
  runs without V4 violation.
- [ ] No `IndexFn` field is reachable from `Source` AST
  (gone — it's a query, not a property).
- [ ] No `naive_apply` / `indexed_apply` distinction remains
  on `Strategy`.
- [ ] Spec §10.7.0, §10.7.6, §10.7.7, §10.7.8 match the
  implementation contracts.

### Estimated surgery

- α: ~600 lines (new module + `Source` impls + runtime
  evaluator rewire + strategy method consolidation).
- β: ~400 lines (registry + per-generator declarations).
- γ: ~80 lines.

---

## Risk register

| Risk | Mitigation |
|---|---|
| Optimizer rule incorrectness produces silent dispense-sequence drift | §9.2 equivalence harness on 10000+ random ASTs (PR 8) |
| Strategy implementation incorrectness (Halton/Sobol/Lhs math) | Per-strategy known-output corpora (PR 3 test gate) |
| Performance regression from stream-first | Benchmark gates in PR 9; investigate any >10% regression |
| Workload migration breaks production | Workload regression suite as cutover gate (PR 9) |
| Metadata propagation false `None` causes missed push-down opportunities | Property test: metadata is total + idempotent (PR 2 ✓ done) |
| Closed-enum discipline drifts | Code-review checklist + `#[non_exhaustive]` on public enums |
| Continuous-source semantics correct but no parser surface | Phase 10 limitation; SRD-18c follow-up tracked separately |
| Concurrent-streamer data races | loom/shuttle tests in PR 8 |

---

## Acceptance criteria for the full implementation push

- [ ] All PR 1–9 test gates pass.
- [ ] Workload corpus regression suite passes byte-equal for every
  workload.
- [ ] Polydat public API exports match spec's documented surfaces
  (verify against spec §3, §9.5, §10.7).
- [ ] No `polydat/src/comprehension/...` internals references in
  any non-polydat SRD (audit invariant preserved post-impl).
- [ ] Workspace test suite passes.
- [ ] No >10% benchmark regression.

---

## Out of scope (deferred per spec §14)

- R8, R9, R10 optimizer rules.
- Continuous-coord predicate analysis (analyzer returns
  `Opaque(Continuous)`).
- Cross-streamer shared sub-evaluation.
- Strategy extensibility surface beyond closed enum.
- Filter-cost-aware optimizer (recognizer catalog growth).
- SRD-18c continuous-source parser grammar.
- SRD-18d Shuffle algorithmic detail doc push.
- SRD-78 explicit three-stream-type narrative doc push.

These do not block implementation; document follow-ups landed
separately.
