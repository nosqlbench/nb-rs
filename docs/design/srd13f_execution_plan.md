# SRD-13f Execution Plan

**Working tracking document.** This is the plan I committed to in
conversation. The success criteria are gates; I do not advance to
the next stage without satisfying the gate at the current stage.
If I find a design contradiction inside a stage, I stop and ask
rather than ship a workaround.

Reference spec: [SRD-13f Cross-Scope Wire Materialization](../sysref/13f_cross_scope_wire_materialization.md).

---

## Ground rules

- The SRD-13f spec is the success criterion, not "tests pass."
- If I hit a design contradiction, I stop and surface it — not
  paper over with refresh loops, "extras" parameters, or partial
  reverts.
- "Bindings are bindings." No special-case path for phase vs.
  workload vs. op anywhere in the runtime layer. If I find myself
  writing "phase binding" handling, that's a signal I'm off-spec.
- The kernel knows its scope. Validators, runners, dispensers
  ask the kernel; no external "in-scope" lists threaded around.
- One per-fiber chain. The structural cache (per-scope-tree-node
  *program* cache) is allowed; parallel per-fiber kernel
  lineages are not.
- Iter-vars are `final const` matter in the comprehension's
  inner scope — produced by the comprehension synthesizer, not
  a separate category.

---

## Stage status legend

- ⏳ *not started*
- 🟡 *in progress*
- ✅ *gate passed*
- 🔴 *blocked — design clarification needed*

---

## Stage 0 — Revert the bandages   ✅

**Gate results:**
- grep for forbidden symbols in non-test files: ✅ clean
  (single hit was a substring match against
  `resolve_with_extras` in a comment, unrelated)
- `cargo build --workspace`: ✅ succeeds with no errors
- Test failures catalogued: 1 test —
  `for_each_union_across_subspaces` in
  `nbrs/tests/m3_dependent_for_each.rs`. This test's
  workload declares scenario iter-vars `x` and `y` and an
  op stmt that references them via `{x}` and `{y}`. The
  failure is exactly the case Stage 1 is built to solve
  (iter-vars as `final const` matter in the comprehension
  inner scope; per-fiber chain carries them through
  via the standard local-kernel mechanism).



**Deliverable:** every contract violation shipped this session
is reverted. Codebase contains only design-conformant code;
tests fail where Stage 1+ will restore correctness.

**Specific removals:**

1. `nbrs-activity/src/scope.rs::validate_placeholders_via_kernel_with_extra`
   deletes. `validate_placeholders_via_kernel(ops, kernel)` is
   the only entry.
2. `nbrs-activity/src/scope.rs::scan_locally_declared_idents`
   visibility returns to private.
3. `nbrs-activity/src/executor.rs::run_phase` reverts to single
   `validate_placeholders_via_kernel(ops, parent_kernel)` call.
   `phase_binding_names` block deletes.
4. `nbrs-activity/src/runner.rs` `workload_root_excludes`
   collection (iter-var walker + phase-binding LHS walker)
   deletes. `compile_bindings_with_libs_excluding` called with
   `&[]` exclude.
5. `nbrs-activity/src/synthesis.rs::FiberBuilder::refresh_per_op_externs_from_main`
   and call sites delete.
6. `nbrs-activity/src/runner.rs::collect_param_references`
   workload-binding and phase-binding scans I added delete.
   Walker reverts.
7. `nbrs-activity/src/bindings.rs::compile_bindings_with_libs_excluding`
   `workload_level_gk_map` parameter deletes. Internal
   `all_bindings` extraction reverts.
8. `nbrs-activity/src/runner.rs::workload_level_gk_map` local
   deletes.

**Completion gate:**

- `grep -rn "_with_extra\|workload_level_gk_map\|workload_root_excludes\|refresh_per_op_externs_from_main\|phase_binding_names" nbrs-activity nbrs-workload` returns zero hits in non-test files.
- `cargo build --workspace` succeeds.
- Test failures are catalogued; not fixed.

**Reporting at gate:** test failures listed, grep output shown.
Do not proceed to Stage 1 until gate is signed off.

---

## Stage 1 — Single per-fiber chain   ✅

**Status:** chain restructure + cell mechanism + iter-var-
as-final-const synthesizer all shipped. Stage 1 absorbed
what was originally Stage 2 (B.2). All three gates pass.

**Shipped:**

1. `attach_dispenser_kernels` builds per-op kernels as
   subscopes of `fiber.main_kernel`. Single per-fiber
   lineage; no shared-kernel parents in the per-fiber chain.
2. Validator (`executor.rs::run_phase`) uses the phase
   scope-tree kernel as the validation kernel — the kernel
   that owns the ops' scope.
3. Workload-root compile's missing-binding check on
   op-field references retired. Op-field references
   resolve at the op-template scope, not workload-root.
4. `bind_outer_scope` Step 2 cell-attaches outer's
   computed output cells to inner's matching input slots.
   Passthrough names (outputs backed by outer input slots)
   continue through the existing input-slot path. Cycle/
   coord inputs handled via the explicit `set_inputs`
   propagation.
5. `EngineCore::pull` writes through to the output's cell
   when one is attached. Outer's per-cycle eval populates
   the cell automatically.
6. `GkKernel::advance_broadcasts` — outer-side operation
   that pulls every output with an attached cell. Called
   from `FiberBuilder::set_source_item` so per-fiber
   `main_kernel` advances its broadcasts at every cycle
   boundary. The operation lives on the kernel's own
   surface; the dispatch layer does not reach into
   descendant kernel state.
7. `synthesize_for_each_iteration` (new in
   `nbrs-variates/src/comprehension/synthesis.rs`):
   emits `final <var> := <literal>` for each iter-var
   and compiles a fresh per-iteration program. Wired
   through `ScopeIterations::with_iteration_kernel_fn`
   from `executor::runtime_iterate`. Iter-vars become
   folded const matter on the inner kernel — no input
   slot, no cell.

**Gate results:**

- **Gate 1 (lineage invariant test):** ✅ passes.
  `synthesis::tests::per_fiber_chain_is_linear_and_unshared`.
- **Gate 2 (iter-var-as-final-const test):** ✅ passes.
  `comprehension::synthesis::tests::iter_var_as_final_const`
  — asserts `kernel.get_constant("x") == Some(Value::U64(1))`
  and `kernel.program().find_input("x") == None` for two
  distinct iter-values; confirms per-iteration recompile
  with literal substitution.
- **Gate 3 (workload integration tests):** ✅ passes
  (whole workspace; one unrelated `nbrs-adapter-stdout`
  observer-log singleton flake under parallel runs —
  passes solo).

**Gate 2 — iter-var-as-final-const:**

The user's design clarification: "comprehensions which
produce scope coordinates are meant to anchor the
coordinates as final values within their scopes." Per
that, the comprehension synthesizer's emitted source must
classify iter-vars as `final <var> := <literal>` matter.

Today's `nbrs_variates::comprehension::synthesis::synthesize_for_each_scope`
emits `extern <var>: <type>` (line 148), with values
injected per iteration via `set_input`. Functionally
equivalent for reads, but the matter AST classifies them
as runtime input slots, not `final const`s.

Making them literal `final const` requires per-iteration
recompile of the inner-scope program: per iteration,
substitute the literal value into the source and compile.
That's a structural change to:

- `synthesize_for_each_scope`'s contract: takes per-
  iteration values, returns a compiled program per
  iteration (or accepts a "template + values" form and
  defers compile to dispatch).
- `GkKernel::for_iteration`'s contract: stops re-using
  `canonical.program()` and instead receives the
  iteration-specific program.
- Dispatch in `dispatch_comprehension`: per-iteration
  recompile or template substitution.

**Decision:** Gate 2 lands within Stage 1. Per-iteration
recompile so iter-vars are literal `final const` matter,
matching the design intent.



**Deliverable:** per-fiber kernel construction walks the scope
tree once per fiber, building each scope's kernel as
`build_subscope` of the per-fiber outer kernel. No shared
kernels in the per-fiber chain. Structural caches on scope-tree
nodes hold *programs*, not kernels.

**Specific changes:**

- `nbrs-activity/src/synthesis.rs::OpBuilder`: retains program
  cache; drops shared `source_kernel` and shared
  `canonical_kernel_for_op` as parents of per-fiber kernels.
- `nbrs-activity/src/synthesis.rs::FiberBuilder`: owns one
  per-fiber kernel per scope it executes in. Replaces
  `main_kernel` + `per_op_kernels` with a linear chain.
- `nbrs-activity/src/executor.rs::run_phase`: builds the
  per-fiber chain from the scope tree.
- `nbrs-activity/src/activity.rs` cycle dispatch:
  `CycleWires::new(per_op)` against the per-fiber op-template
  kernel from the chain.
- Comprehension synthesizers: emit `final <iter_var> := <literal>`
  matter for the inner-scope kernel. Per iteration, fresh
  per-fiber instance from the cached program with the
  iteration's literal substituted. Iter-var is folded
  constant on inner kernel — no input slot, no cell.

**Completion gates:**

1. **Lineage invariant test** — new unit test asserting:
   ```rust
   #[test]
   fn per_fiber_chain_is_linear_and_unshared() {
       let (fiber, _) = build_test_fiber(...);
       let chain = fiber.scope_kernel_chain();
       for k in &chain { assert!(fiber.owns(k)); }
       for w in chain.windows(2) {
           assert!(w[1].is_parent_of(&w[0]));
       }
   }
   ```
2. **Iter-var-as-final-const test** — new unit test:
   - For for_each scope with `x in 1, 2`, take per-iteration
     kernel for iter 0; assert `kernel.get_constant("x") ==
     Some(Value::U64(1))`, `kernel.program().find_input("x") ==
     None`, `kernel.program().output_names().contains("x")`.
3. **Workload integration tests pass.**

If integration tests pass but iter-var-as-final-const fails:
that's a comprehension-synthesizer correction *within
Stage 1*, not a separate stage.

---

## Stage 2 — B.2 bind-time cell attachment   ✅ *(absorbed into Stage 1)*

**Deliverable:** bind step in `nbrs-variates` attaches outer's
output cells to inner's matching input slots, completing the
cell-on-outputs mechanism SRD-13f specifies. No per-cycle
refresh outside the GK eval engine.

**Specific changes:**

- `nbrs-variates/src/kernel/gkkernel.rs::bind_outer_scope`
  Step 2: cell-attach using
  `outer.state.core.output_cell(name)`. Passthrough exclusion
  driven by matter classification, not name-overlap heuristics.
- Matter-AST: extends classification if needed for the
  read-only-cell case (SRD-13f §"Materialization gradient");
  *if the schema shape isn't already in place from SRD-13e,
  stop and ask*.

**Completion gates:**

1. **Read invariant unit test:**
   ```rust
   #[test]
   fn read_invariant_for_cycle_derived_outer_binding() {
       let (outer, inner) = build_per_fiber_pair_with_outer_binding(
           "load := add(cycle, 1)", "load"
       );
       outer.set_inputs(&[0]);
       let v0_outer = outer.pull("load");
       let v0_inner = inner.lookup("load").unwrap();
       assert_eq!(v0_inner, v0_outer);
       outer.set_inputs(&[7]);
       let v7_outer = outer.pull("load");
       let v7_inner = inner.lookup("load").unwrap();
       assert_eq!(v7_inner, v7_outer);
   }
   ```
2. **Workload integration tests pass with no per-cycle refresh
   code anywhere outside `nbrs-variates/src/kernel/`.** Grep
   confirms.

---

## Stage 3 — Push D full retirement of parser merge   ✅

**Deliverable:** Cross-scope parser merge retired across
workload + phase scopes. Block-level YAML `bindings:` was
clarified (with user) as syntactic sugar — not a GK scope —
so its parser-time inlining into op-level bindings remains
under a clearer name (`inline_block_sugar_into_op`).
Workload-level and phase-level `bindings:` live only on
their scopes; descendants resolve them through the GK
kernel chain.

**Shipped:**

1. `nbrs-workload/src/parse.rs`: `merge_bindings` renamed to
   `inline_block_sugar_into_op` with explicit-sugar framing;
   workload-level and phase-level threads through
   `parse_phases` / `parse_blocks` / `parse_single_block`
   retired; the only remaining caller is the block→op
   sugar expansion inside `normalize_op_object`.
2. `nbrs-activity/src/runner.rs`: `workload_level_gk`
   extended to handle Map-form workload bindings via a
   new `bindings::legacy_chain_map_to_gk_lines` helper
   (workload-level Map-form bindings translate to GK
   source instead of going through op.bindings merge);
   `collect_param_references` scans `workload.bindings`
   and `phase.bindings` directly so the unused-param
   validator no longer relies on parser-time merge.
3. `nbrs-activity/src/bindings.rs`: `compile_bindings_with_libs_excluding`'s
   `scope_already_has_gk` gating simplified to
   "always append `workload_level_gk` when non-empty"
   since the duplication risk it was hedging against
   (workload bindings appearing twice via merge + direct
   param) is now structurally impossible.
4. `nbrs-activity/src/executor.rs`: `ExecCtx.workload_level_gk`
   threaded through; phase-scope compile (`compile_from_scope`)
   now appends workload-level GK source as local matter so
   fiber.main_kernel evaluates dynamic workload bindings
   on its own state per cycle (no shared workload-root
   ticking, which would race across fibers).
   `validate_placeholders_via_kernel` switched to the
   phase scope-tree kernel as the validation root so
   placeholders referencing phase-level bindings (`{c}`
   where `c := (cycle)` is declared at phase scope)
   resolve correctly without the legacy parser merge.
5. `nbrs-activity/src/bindings.rs::compile_from_scope`:
   coordinates-declaration shim ensures every phase-
   scope program declares `inputs := (cycle)` so per-
   cycle `set_inputs` propagation finds a Coordinate
   slot to write to. Conflicting extern declarations
   (auto-emitted from the manifest cascade) are stripped
   before workload-level GK is appended.
6. `nbrs-activity/src/synthesis.rs::FiberBuilder::set_source_item`:
   `advance_broadcasts` gated on per-op kernel having a
   *different* program from main_kernel. When the
   per-op kernel reuses main_kernel's program (flattened
   path — same workload+phase bindings inlined),
   pre-evaluating on main_kernel is both redundant and
   harmful (side-effecting nodes like `throw_at` would
   fire outside the per-op cascade surface). When the
   per-op kernel has its own program, advance_broadcasts
   pushes cell-attached values for it.

**Completion gates:**

- `grep -rn "merge_bindings" nbrs-workload nbrs-activity`
  zero hits ✅.
- Workload integration tests pass for:
  - `shared_cells_basic_emits_all_four_types` ✅
  - `coverage_matrix_derived_binding_consumes_shared_cell` ✅
  - `feature_showcase_*` ✅
  - All other workload_examples tests ✅.
- All lib tests (~1.7K) pass ✅.

**Determinism cleanup (also shipped):**

The staircase regression surfaced TWO determinism gaps
that the pre-Push-D parser-merge had masked. Both are now
addressed at the GK-compiler layer (not patched at the
workload layer):

1. **Workload-params iteration order** — `compile_bindings_with_libs_excluding`
   walked `workload_params: &HashMap` to collect required
   outputs; HashMap iteration is per-process randomised so
   the resulting `required_outputs: Vec<String>` order
   varied across processes, which drove `output_order` in
   the compiled program. Fixed by sorting the workload-
   param keys before pushing them into `scope_required`.

2. **`volatile` not respected by the fold pass** — pre-Push-D,
   workload-root never compiled the dynamic workload
   bindings (`throw_at(cycle, threshold, "staircase")`)
   because `merge_bindings` had landed them on op.bindings
   and the workload-root compile only saw phase ops with
   their own bindings; DCE dropped the whole chain at the
   workload-root level. Post-Push-D the workload-root
   compile DOES see them (phase ops have empty bindings,
   so they flow into the workload-root compile), and the
   compile-time fold replaced `side_effect_sequence_next_cycling`
   with a literal — different value per invocation,
   different `canonical_hash`, broken resume-skip
   identity. The author's `volatile` marker was supposed
   to prevent exactly this ("exclude these wires' values
   from `hash_const`"), but the fold pass only consulted
   `output_modifiers` to suppress the non-deterministic
   *warning*; it didn't change the lifecycle classifier.

   **Fix (proper architectural — `volatile` is the marker,
   not name-prefix):**
   - `nbrs-variates/src/kernel/gkkernel.rs`: thread
     `output_modifiers` through `GkKernel::new_with_inputs`
     / `new_strict_with_inputs` / `new_impl` and install
     them on the program BEFORE `fold_init_constants`
     runs (the old out-of-band `set_output_modifiers`
     accessor ran *after* fold, which was too late).
   - `nbrs-variates/src/kernel/program.rs::fold_init_constants_impl`:
     in the lifecycle pre-classification, mark any node
     whose `output_map` entry has a `volatile` modifier
     as `EvalLifecycle::Dynamic`. The propagation walk
     then carries that downstream so consumers also stay
     unfolded.
   - `nbrs-variates/src/dsl/compile.rs::compile_filtered`:
     preserve volatile bindings as outputs from DCE
     even when the caller's `required_outputs` list
     doesn't reference them — the author's `volatile`
     declaration is the explicit "this stays in the
     program" intent, and without the output entry the
     fold pass's volatile check has no `output_map`
     row to match against.

   Net effect: `volatile threshold := side_effect_…`
   keeps its side-effect node as-is in the compiled
   program. The `canonical_hash` sees the node type +
   wiring shape but never the per-invocation value.
   Workload identity is stable across processes; the
   resume-skip path matches as designed.

---

## Stage 5 — Push E: combined `for_each:` + `bindings:` phase support   ✅

**Deliverable:** a phase declaring BOTH `for_each:` AND
`bindings:` folds its bindings into the for_each scope
kernel. One kernel per phase scope, carrying both iter-var
declarations AND phase-level bindings; descendants chain
from there. Pre-Push-E the install loop's `for_each`
branch silently dropped the phase's `bindings:`; the
parser-merge fallback kept such workloads working
incidentally — Push D retired that fallback.

**Shipped:**

1. `synthesize_for_each_scope` (nbrs-variates) accepts an
   optional `phase_bindings: Option<&str>` parameter and
   appends the source after the extern cascade. Phase
   bindings can reference iter vars (now externs) and
   any cascaded parent name; name-collision with an iter
   var surfaces as a `duplicate node name` compile error
   from the assembler — preferred over a silent shadow.
2. `InstallSpec::ForComprehension` (nbrs-activity) gained
   a `phase_bindings: BindingsDef` field. Pure-comprehension
   scope-tree nodes (scenario-level `for_each:`) pass
   `BindingsDef::default()`. Phase scope-tree nodes that
   declare both `for_each:` and `bindings:` pass
   `phase.bindings.clone()`.
3. Executor's `ForComprehension` handler translates the
   `BindingsDef` to GK source (GkSource verbatim, Map →
   `name := expr` lines) and threads it to
   `synthesize_for_each_scope`.
4. Test added in `nbrs/tests/m3_dependent_for_each.rs`:
   `phase_with_for_each_and_bindings_folds_bindings_into_loop_scope`
   — a phase with `for_each: "k in 1, 2, 3"` and
   `bindings: |\n doubled := mul(k, 2)` emits per-
   iteration `k=N doubled=2N` lines for all three
   iter-var values.

**Completion gate:** workspace tests pass ✅, new test
passes ✅, no regressions across the existing for_each /
do_while / coverage_matrix integration tests.

---

## Stage 4 — Push F: rename `bind_outer_scope`   ✅

**Deliverable:** operation renamed to reflect matter-
interpretation responsibility. SRD-13f §"The cross-scope
wiring operation is matter-AST-driven at construction" is the
spec for the new name.

**Shipped:** chose **`materialize_wiring_from_outer`** —
direction-explicit (FROM outer), short, matches the
sibling `materialize_subscope` vocabulary, doesn't
redundantly name the receiver (which IS the subcontext).
Function and all references updated; tests pass.

**Completion gate:**

- `grep -rn "bind_outer_scope" nbrs-variates/src nbrs-activity/src`
  zero hits in source ✅.
- `cargo test --workspace --tests` green ✅ (modulo the
  pre-existing stdout observer-log singleton flake, which
  is order-dependent on parallel runs and passes solo).

---

## Drift-catch protocol

At each completion gate, deliverable + test/grep are both
concrete. If test fails or grep is non-empty, stage isn't
complete; don't progress. Report back at every gate before
moving on — specific results, not narrative.

If inside a stage a design clarification is needed (e.g.,
matter-AST schema for Stage 2), stop *inside the stage* and
surface the question. No "shipping a partial as a checkpoint."
