# SRD-13f Wire-Classification True-Up — Implementation Plan

**Spec:** [SRD-13f §"Wire-reference classification (synthesizer
rule)"](../sysref/13f_cross_scope_wire_materialization.md#wire-reference-classification-synthesizer-rule).

**Status:** in progress — planning artifact for the synthesizer
+ kernel changes that move the code to the SRD's classification
rule. Tracks the migration from the workaround-driven
intermediate state (compile_from_scope source-append; `build_scope`
workload-source param; auto-extern emission on op-field refs) to
the canonical four-case synthesizer.

## Ground rules

- Stage gate = code change shipped *and* its test surface green.
- Stage produces no regression in workspace tests (modulo the
  staircase / shared_cells / coverage_matrix / detect_dialect
  surfaces explicitly tested at the relevant stage).
- The four cases (promoted-final, authored-`extern`,
  local-inclusion, unresolved-validation-error) are the canonical
  surface — any code path that violates them in a stage's
  deliverable is a stage failure, not a follow-up.

---

## Stage 1 — Synthesizer rewrite

**Deliverable:** `nbrs-activity/src/scope.rs::build_scope`,
`nbrs-variates/src/comprehension/synthesis.rs::synthesize_for_each_scope`,
`build_phase_scope_kernel`, and `build_op_template_scope_kernel`
apply the four-case rule to every wire reference they encounter.

**Foundational primitive — AST-as-metadata on `GkProgram`:**

A binding's "matter" is not contiguous in source: a binding's
RHS may invoke helpers, module functions, or other named
bindings that live elsewhere in the file. Source-text slicing
captures only the binding's surface declaration, not its
graph-structural neighbourhood. Therefore the AST itself
(`Arc<GkFile>`) is retained on every compiled `GkProgram` as
live metadata.

- `nbrs-variates/src/kernel/program.rs` — add `ast: Arc<GkFile>`
  field to `GkProgram`. Populated by every compile entry point
  in `compile.rs` (`compile_gk_*`, `compile_ast*`, `Compiler::compile`).
- Accessors:
  - `pub fn ast(&self) -> &Arc<GkFile>` — the full retained AST.
  - `pub fn binding_ast_for(&self, name: &str) -> Option<&Statement>`
    — find the `InitBinding` / `CycleBinding` whose output name
    matches `name` (or whose tuple targets include `name` for
    destructuring bindings).
- These are read-only metadata accessors; the AST is never
  mutated post-compile.

**Synthesizer responsibilities (per scope):**

1. Walk the subscope's authored body (op fields, condition,
   delay, metric values, result wires, plus any authored
   `bindings:` block at this scope) and collect the set of
   referenced names.
2. Walk the authored matter's `extern` declarations and collect
   the explicit cascade set.
3. For each referenced name N that isn't satisfied locally:
   - If N is `final`/`init`/`const` upstream (resolve through the
     parent kernel's program-modifier table / folded-output
     buffer) → emit `final N := <value>` in the synthesized
     matter.
   - Else if N is in the authored extern set → emit `extern N: T`
     (T resolved from parent manifest).
   - Else if N is found as a binding in any ancestor's program →
     query parent program's `binding_ast_for(N)`, integrate the
     `Statement` into the child's synthesized AST, then walk the
     binding's `Expr` tree (via `collect_references` or
     equivalent) to find transitive references and process them
     by the same four-case rule recursively.
   - Else → return a `WireResolutionError` with scope-tree path,
     field-path provenance, and the in-scope name list.
4. Coord inputs (`cycle` and any additional declared at workload
   level) are owned by every scope that needs to set them. The
   synthesizer emits `inputs := (cycle, …)` on the subscope.

**AST-mode synthesis path:**

The existing synthesizer concatenates `.gk` source text and
re-parses. To consume the AST primitive, the synthesizer needs
either:

- (a) An AST pretty-printer that re-emits `Statement` →
  text and stays faithful to grammar (modifiers, tuple targets,
  interpolated strings). Risk: drift between parser and printer.
- (b) A direct AST-construction path that builds a `GkFile`
  by composing the subscope's own AST with the inherited
  `Statement`s and feeds it to `compile_ast_*` (which already
  exists — see `compile.rs:458`). No round-trip through text.
  Cleaner; matches the user's "AST as kernel metadata" framing.

Stage 1 commits to (b). Synthesizers move from string
concatenation to `GkFile` assembly. The compile entry point
already accepts `GkFile`, so the change is local to the
synthesizers — no compiler surgery.

**Files touched:**

- `nbrs-variates/src/kernel/program.rs` — add `ast` field +
  accessors.
- `nbrs-variates/src/dsl/compile.rs` — every compile entry
  point passes the parsed `GkFile` Arc through to the
  assembler, which forwards it to `GkProgram` at construction.
- `nbrs-variates/src/dsl/assembler.rs` — accept and forward
  the `Arc<GkFile>`.
- `nbrs-activity/src/scope.rs` — `build_scope`,
  `build_phase_scope_kernel`, `build_op_template_scope_kernel`
  switch to AST assembly. Auto-extern emission for op-field
  references **retires** (replaced by recursive local-inclusion
  walk and validation error).
- `nbrs-variates/src/comprehension/synthesis.rs::synthesize_for_each_scope`
  — same AST assembly migration.
- `nbrs-workload/src/parse.rs` — no change.

**Gate (Stage 1 test surface):**

- New unit test: `synthesizer_emits_local_inclusion_for_referenced_workload_binding`
  — assert phase synthesizer pulls in workload-level binding
  AST as local matter for a referenced non-final non-extern
  wire.
- New unit test: `synthesizer_emits_promoted_final_for_workload_param`
  — assert `final dataset := "sift1m"` ends up in the phase
  scope's emitted source.
- New unit test: `synthesizer_emits_extern_for_authored_extern`
  — assert authored `extern X: T` survives unchanged.
- New unit test: `synthesizer_rejects_unresolved_reference`
  — assert the synthesizer surfaces a `WireResolutionError`
  with path + in-scope names for a typoed reference.
- Existing workload integration tests continue to pass under
  Stage 1's synthesizer alone (Stage 2/3 won't have landed
  yet, but the synthesizer's output should already be the
  correct shape — Stages 2 and 3 swap the kernel-layer
  primitive without changing what matter the synthesizer
  emits).

---

## Stage 2 — Kernel parent-ref + cascade-on-read primitive

**Deliverable:** `GkKernel` carries an optional `parent`
reference. `materialize_wiring_from_outer` (formerly
`bind_outer_scope`) records cascade routes for each `extern X`
slot; reads on those slots delegate to `parent.pull(X)`.
Shared-cell attachment stays unchanged.

**Specific changes:**

- `nbrs-variates/src/kernel/gkkernel.rs::GkKernel` gains
  `parent: Option<Arc<GkKernel>>`. Set by
  `materialize_subscope` / `adopt_subscope` at construction.
- `nbrs-variates/src/kernel/engines.rs::EngineCore::read_input`
  gains a cascade-route slot per input. When the slot's route
  is `CascadeFromParent(name)`, `read_input` calls
  `parent.pull(name)` instead of reading the local buffer.
  Routes are set up at construction by
  `materialize_wiring_from_outer`.
- `materialize_wiring_from_outer` (in `gkkernel.rs`): for each
  input slot whose program-side declaration is `extern X: T`
  with the explicit-extern marker (Stage 1 emits this), record
  the cascade route to `parent.pull(X)`. Shared cells continue
  to attach via Arc cell sharing. Promoted-final and
  local-included names need no wiring — they're in the program
  directly.

**Gate (Stage 2 test surface):**

- New unit test: `kernel_cascade_route_delegates_to_parent_pull`
  — set up a parent with `volatile X := side_effect_node(...)`,
  a child with `extern X: u64` and the explicit-extern marker,
  pull X on the child, assert the side effect fires on the
  parent and the child receives the value.
- New unit test: `kernel_local_binding_evaluates_against_local_state`
  — set up a child with `volatile Y := add(cycle, 1)` as
  local matter, set cycle=10 on the child, pull Y, assert
  result is 11. Parent's cycle is 0; child's local eval
  doesn't consult parent.

---

## Stage 3 — Revert workarounds

**Deliverable:** every patch-around accumulated during the
Push D / Push E / detect_dialect investigations is removed.
The synthesizer (Stage 1) and the cascade primitive (Stage 2)
together cover the cases those patches were patching.

**Specific changes:**

- `nbrs-activity/src/bindings.rs::compile_from_scope` — drop
  the `workload_level_gk: Option<&str>` parameter and the
  source-append block. The phase scope's matter (from Stage
  1's synthesizer) is complete.
- `nbrs-activity/src/scope.rs::build_scope` — drop the
  `workload_level_gk` parameter and the reference-scan block
  that fed the source-append.
- `nbrs-activity/src/executor.rs::ExecCtx` — drop the
  `workload_level_gk: Option<String>` field. Drop the
  threading at the `ExecCtx` construction site.
- `nbrs-variates/src/comprehension/synthesis.rs::synthesize_for_each_scope`
  — drop the `phase_bindings: Option<&str>` parameter and
  the source-append block (Push E append-mechanism). Phase
  bindings now arrive via the synthesizer's local-inclusion
  walk over the for_each scope's body.
- `nbrs-variates/src/dsl/compile.rs::compile_filtered` — drop
  the special-case "always preserve volatile outputs" block
  added during staircase debugging (the synthesizer's
  local-inclusion handles the `trip` wire being needed; the
  volatile-output-preservation is no longer load-bearing).
  The volatile-lifecycle-marking in `fold_init_constants_impl`
  stays — that's the proper architectural fix for `volatile`.

**Gate (Stage 3 test surface):**

- `grep -rn "workload_level_gk" nbrs-activity nbrs-variates`
  zero hits in src/ trees (test fixtures may retain literal
  uses where they verify the synthesizer's output).
- `cargo build --workspace --tests` zero warnings.

---

## Stage 4 — Test gates

**Deliverable:** every previously-broken scenario passes
through the new mechanism. New tests cover the
unresolved-reference validation error surface.

**Test surface:**

- `staircase_failures_resume_correctly` — phase scope inlines
  `volatile trip` and `volatile threshold` as local matter
  (Stage 1). `trip` evaluates per-fiber per-cycle on the
  phase's main_kernel; `threshold`'s side effect fires once
  (process-cache contract of the testkit fixture, not the
  kernel layer). Resume sequence advances correctly across
  runs.
- `shared_cells_basic_emits_all_four_types` — phase scope
  inlines `scaled := mul(budget_u64, 2)` as local matter.
  `budget_u64` is `shared` upstream → cell-attach (existing
  mechanism). `scaled` evaluates on the per-fiber kernel
  against the cell-aware `budget_u64` slot.
- `coverage_matrix_derived_binding_consumes_shared_cell` —
  same pattern as shared_cells.
- `bare_file_invocation` (maze.yaml) — workload param
  `cols` is referenced from a workload-level binding
  (`row := char_buf(hash(cycle), "╱╲", cols)`). The
  `cols` workload param is `final` upstream → promoted-final.
  `row`'s binding inlines as local matter on each phase
  scope that references it.
- `phase_with_for_each_and_bindings_folds_bindings_into_loop_scope`
  (Push E) — for_each scope inlines phase bindings as local
  matter alongside iter-var coord declaration.
- `full_cql_vector::detect_dialect` — workload binding
  `profiles := matching_profiles("{dataset}", "{prefix}")`
  references `dataset` (workload-param `final`) and `prefix`
  (workload-param `final`). Both promote to local `final`
  injections on each scope that references them.
  `profiles` itself inlines as local matter where referenced.
- **New test:** `synthesizer_rejects_unresolved_wire_reference`
  — workload with `stmt: "x={tirp}"` while the workload
  binding is named `trip` triggers a workload-load-time error
  naming the field path, the typoed name, and the in-scope
  name list. Assert on the error message structure.

**Completion gate:**

- `cargo test --workspace --tests` green (modulo any
  pre-existing flakes that aren't this SRD's concern).
- All five named integration tests pass.
- The new unresolved-reference unit test passes.

---

## Risk surface

- **AST retention memory cost** — every compiled program now
  carries an `Arc<GkFile>`. Programs are reused across fibers
  (Arc-shared), and `GkFile` is small (a `Vec<Statement>` of
  enum nodes), so the cost is bounded. Negligible against
  per-fiber state buffers. The Arc means no clone on share.
- **Module/extern resolution in inherited AST** — when the
  synthesizer integrates a parent binding's `Statement` whose
  RHS references a module function (e.g. `char_buf(...)`),
  the module definition must also be reachable in the child's
  AST. Plan: copy `Statement::ModuleDef` entries from the
  parent's AST when the binding's RHS references a module
  symbol. Compile-time validation handles unresolved cases.
- **Recursive inclusion termination** — the inclusion walk
  terminates at case 1 (promoted-final), case 2 (authored
  extern), case 4 (unresolved). A workload with circular
  refs across scopes would loop, but GK's existing
  acyclic-DAG constraint prevents that statically.
- **Side-effecting nodes** — under local inclusion, a
  side-effecting nullary node like
  `side_effect_sequence_next_cycling` is replicated into
  every subscope that references it transitively. Each
  per-fiber kernel construction would fire the side effect
  once at first pull. The node's own caching contract
  (process-wide path-keyed cache for the testkit fixture)
  handles cross-kernel idempotence — explicit responsibility
  of the node, not of the synthesizer.

## Out of scope for this plan

- Per-fiber instancing of upstream kernels. The tree's
  provenance + sharing model stays as is (one shared kernel
  per scope at the canonical/template level; per-fiber
  state is materialised by `materialize_subscope`).
- Eval-with-caller-state for cascade-on-read. Case 2
  (`extern`) cascades use the parent's state during eval.
  Workload-authors marking a wire `extern` accept
  parent-state semantics; if they need per-fiber semantics
  they use case 3 (local inclusion via reference) instead.

---

## Status

- Stage 1: **landed (cases 1+3)** 2026-05-11.
  - AST retained on `GkProgram` (`ast` field +
    `binding_ast_for`, `local_inclusion_chain`).
  - AST-mode synthesizer wired into `build_scope` via a new
    `parent_kernel: Option<&GkKernel>` parameter (carries both
    the program's retained AST and the folded constant state).
  - Pretty-printer covers Statement / Expr / modifier / arg /
    binop / escapes (12 round-trip tests).
  - **Case 1 (promoted-final):** when a referenced name is FINAL
    in the outer manifest, read the value from
    `parent_kernel.get_constant(name)` and emit
    `final name := <literal>` inline (U64/F64/Bool/Str types;
    falls back to extern cascade for other types).
  - **Case 2 (authored extern):** unchanged (existing
    extern-cascade behavior).
  - **Case 3 (local matter inclusion):** wired through AST-mode
    inclusion pass; pretty-prints transitive-closure of cycle
    bindings as Inherited scope ingestion.
  - **Case 4 (unresolved → validation error):** **landed**
    2026-05-11. Initial false-positive was because the
    executor's `build_scope` call was using
    `current_parent_kernel` (workload root) rather than the
    phase scope kernel from the scope tree as the classifier
    parent — the phase scope kernel (carrying `phase.bindings`)
    wasn't visible. Wired `classifier_kernel` via the same
    `scope_tree.phase_node_by_name` lookup the placeholder
    validator uses, with a fallback to `current_parent_kernel`.
    Synthesizer now rejects typos like `{tirp}` (when the
    binding is `trip`) at workload-load time with structured
    `unresolved wire reference(s) [...]: ...` errors listing
    the visible names. GK-function tokens (`mod`, `hash`, etc.)
    are filtered out via `registry::lookup`.
- Stage 2: **deferred indefinitely**. The cascade-on-read
  primitive's value was to handle parent-state mutations
  visible from the child mid-execution. After the case-3 +
  case-1 landing, the situations where this would matter no
  longer arise in practice:
  - FINAL wires: case 1 inlines the literal; no mutation
    possible.
  - SHARED wires: `SharedCell` mechanism already provides
    Arc-shared cross-kernel synchronisation.
  - Non-final cycle bindings: case 3 evaluates locally in the
    child kernel; the parent's state is irrelevant.
  - Authored EXTERN wires: rare; bind-at-init is sufficient
    for documented patterns.
  Revisit only when a concrete workload exposes a behaviour
  gap. Until then the architectural cleanup isn't worth the
  semantic-change risk.
- Stage 3: **landed** 2026-05-11.
  - `build_scope::workload_level_gk` parameter + scan block: **removed**.
  - `compile_from_scope::workload_level_gk` parameter + source-append + `inputs := (cycle)` fallback: **removed**.
  - `compile_bindings_with_libs_excluding`: kept the
    workload_level_gk parameter but switched its internal
    handling from post-emit source-append to
    `scope.ingest_gk_source(..., Inherited)` before emit.
  - `ExecCtx::workload_level_gk` field: **removed** (no readers
    left after the build_scope/compile_from_scope retirements).
  - Phase-scope coord propagation: synthesizer now emits
    `inputs := (...)` from the parent program's coord names.
  - All four hardcoded `name == "cycle"` special-cases retired
    in scope.rs and synthesis.rs (replaced with generic
    coord-set detection via `parent_program.coord_count()`).
  - Case-3 AST-mode inclusion now also runs in
    `build_phase_scope_kernel` and `synthesize_for_each_scope`
    alongside their existing extern cascade — names with
    non-final cycle bindings in the parent AST get inlined as
    local matter when the scope references them.
  - **`synthesize_for_each_scope::phase_bindings` parameter
    stays.** Reclassified — it's not a workaround. It carries
    phase-level YAML bindings into the for_each scope, which
    has no compiled-program alternative (phase.bindings is YAML
    metadata, not part of an upstream kernel's AST).
- Stage 4: **landed** 2026-05-11.
  - All 34 workload-example integration tests pass after the
    full synthesizer migration: staircase_failures_resume_correctly,
    shared_cells_*, coverage_matrix_*, feature_showcase_*,
    full_cql_vector tests, lifecycle event tests.
  - Unit tests landed:
    - `promoted_final_emits_inline_literal_for_str` /
      `..._for_u64` (case 1).
    - `unresolved_wire_reference_surfaces_validation_error`
      (case 4).
    - 4 ast_metadata_tests (binding_ast_for + local_inclusion_chain).
    - 12 pprint round-trip tests.
  - Workspace: 3151 tests pass, 0 failures.
