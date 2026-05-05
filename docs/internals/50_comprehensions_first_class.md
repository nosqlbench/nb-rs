# Comprehensions as a first-class GK concept

Migration plan to lift the comprehension model out of
`nbrs-activity` / `nbrs-workload` and into `nbrs-variates`,
making `Comprehension` a peer of `GkProgram` / `GkKernel` /
`ScopeCoord` in the GK API surface.

Status: **planning → execution**. Phases land independently;
each phase keeps the workspace green.

---

## Where comprehension code lives today

```
nbrs-workload
├── model.rs              ScenarioNode { ForEach, ForCombinations,
│                                        ForEachUnion, IncludedScenario,
│                                        DoWhile, DoUntil, Phase }
├── parse.rs              YAML→ScenarioNode
│                         + parse_one_clause, split_respecting_parens,
│                         + parse_combination_specs

nbrs-activity
├── scope_tree.rs         ScopeKind (mirrors ScenarioNode 1:1) + ScopeTree
├── scope.rs              build_scope (the big one): synthesises GK source
│                         for each comprehension scope;
│                         pre_evaluate_clause, evaluate_spec,
│                         parse_list_with_types,
│                         format_workload_param_as_gk_literal,
│                         value_to_gk_type_name,
│                         collect_string_interp_refs
├── executor.rs           TupleComprehension::{new, enumerate},
│                         dispatch_comprehension, run_one_iteration,
│                         TerminalAction
└── interpolate.rs        interpolate_via_kernel — reads GK kernel
                          for `{name}` substitution inside specs

nbrs-variates             carries InputKind::IterationExtern, ScopeCoord,
                          GkKernel::scope_coordinates() — but doesn't
                          know about comprehensions as a structural concept.
```

Three near-identical representations of the same shape:
`ScenarioNode::ForEach/ForCombinations/ForEachUnion`,
`ScopeKind::*`, and `TupleComprehension`'s in-memory clause
list. The classification rule that produced
`scope_coordinates()` is structural
(`InputKind::IterationExtern` + not inherited) but the *shape*
that defines those coordinates (which clauses, in what mode,
against what values) is invisible to GK.

---

## Proposed boundaries

```
nbrs-variates                                   ← becomes the home of the model
├── kernel/
│   ├── scope_coords.rs   ScopeCoord (already there)
│   └── gkkernel.rs       scope_coordinates() invariant (already there)
└── comprehension/                              ← NEW MODULE
    ├── ast.rs            Clause { var, expr },
    │                     Comprehension { mode },
    │                     ComprehensionMode { Cartesian, Union(Vec<Vec<Clause>>) }
    ├── parse.rs          parse_comprehension_spec(text) → Comprehension
    │                     (string-form parser + paren-aware splitter +
    │                      union-vs-cartesian detection)
    ├── eval.rs           evaluate_clause(parent_kernel, clause) → Vec<Value>
    │                     enumerate_tuples(canonical, parent, comprehension, strict)
    │                       → Vec<Vec<(String, Value)>>
    └── synthesis.rs      synthesize_scope_source(comprehension,
                            parent_manifest, workload_params, pragmas)
                          → GkSourceFragments

nbrs-activity                                   ← orchestration only
├── scope_tree.rs         ScopeKind variants now hold
│                            comprehension: Comprehension
│                         instead of bespoke (var, spec) shapes
├── executor.rs           dispatch_comprehension calls
│                            comprehension::enumerate_tuples(...)
└── (scope.rs largely retires; what's left is the composition glue —
    "given a comprehension scope's GK source, compile it, install
    the kernel, run lifecycle." Pure activity concerns.)

nbrs-workload                                   ← parser only
├── model.rs              ScenarioNode variants carry
│                            comprehension: Comprehension
│                         (parsed once at YAML-read time). DoWhile/
│                         DoUntil keep their condition strings —
│                         those aren't comprehensions.
└── parse.rs              YAML → ScenarioNode delegates clause parsing
                          to comprehension::parse_comprehension_spec
```

---

## API surface (new in `nbrs-variates`)

```rust
pub mod comprehension {
    pub struct Clause { pub var: String, pub expr: String }

    pub enum ComprehensionMode {
        Cartesian(Vec<Clause>),
        Union(Vec<Vec<Clause>>),  // each inner Vec is one sub-space
    }

    pub struct Comprehension {
        pub mode: ComprehensionMode,
    }

    impl Comprehension {
        pub fn coordinate_names(&self) -> Vec<&str>;
        pub fn flat_clauses(&self) -> Vec<&Clause>;
    }

    pub fn parse_comprehension_spec(text: &str) -> Result<Comprehension, String>;

    pub fn evaluate_clause(
        kernel: &GkKernel,
        clause: &Clause,
    ) -> Result<Vec<Value>, String>;

    pub fn enumerate_tuples(
        canonical: &Arc<GkKernel>,
        parent: &Arc<GkKernel>,
        comprehension: &Comprehension,
        strict: bool,
    ) -> Result<Vec<Vec<(String, Value)>>, String>;

    pub fn synthesize_scope_source(
        comprehension: &Comprehension,
        parent_manifest: &[ManifestEntry],
        workload_params: &IndexMap<String, String>,
        pragmas: &PragmaSet,
    ) -> GkSourceFragments;
}
```

`GkSourceFragments` is a structured value (extern decls, final
injections, cascade externs, body) rather than a raw String —
keeps the public surface stable as the synthesis details
evolve. The string form stays as a debug rendering only.

---

## Why this shape

- **Single source of truth.** `Comprehension` becomes the one
  type that everyone — parser, scope tree, scope synthesis,
  executor, presentation — consults.
- **Scope-coordinates invariant tightens.** Today
  `ScopeCoord` is populated post-hoc from
  `InputKind::IterationExtern`. With `Comprehension`
  first-class, the synthesiser declares "coordinate X" once,
  and the kernel's coord set is provably the comprehension's
  clause LHS set — no classification round-trip.
- **GK gets a self-contained iteration model.** A future
  `for_each` keyword in the GK DSL, programmatic GK consumers,
  unit tests — none need to drag in workload/activity to
  iterate.
- **Reduces nbrs-activity by ~600 LoC.** scope.rs's
  interpolation/evaluation/synthesis machinery and
  executor.rs's TupleComprehension move out, leaving activity
  with the orchestration it owns.

---

## Migration plan

Each phase lands cleanly, keeping the workspace green.

### Status (as of 2026-05-01)

- **Phase A — Lift the AST.** ✅ Shipped. `nbrs-variates::comprehension::ast` carries `Clause`, `Comprehension`, `ComprehensionMode`.
- **Phase B — Parser.** ✅ Shipped. `parse_clause`, `parse_clause_list`, `comprehension_from_subspaces`, `split_respecting_parens` live in `comprehension::parse`. Workload parser delegates.
- **Phase C — Evaluation.** ✅ Shipped. `evaluate_spec`, `pre_evaluate_clause`, `parse_list_with_types`, `value_to_gk_type_name`, `collect_string_interp_refs`, `interpolate_via_kernel`, `interpolate_with_lookup`, `enumerate_tuples` all in `comprehension::eval`. `nbrs-activity::interpolate` deleted.
- **Phase D — Synthesis.** ✅ Shipped. `synthesize_for_each_scope`, `propagate_parent_inputs`, `collect_leaf_placeholders`, `scan_one`, `workload_param_type_name`, `format_workload_param_as_gk_literal` in `comprehension::synthesis`. `ManifestEntry`/`extract_manifest` moved to `nbrs-variates::kernel::manifest`. `iterate(comprehension, parent, …) → ComprehensionIter` is the headline ergonomic.
- **Phase E — Drop duplicated representations.** ✅ Shipped. `ScenarioNode::ForEach` / `ForCombinations` / `ForEachUnion` collapsed into one `ScenarioNode::Comprehension { comprehension, children }`; same on `ScopeKind`. `find_comprehension_scope` replaces the trio of find methods. Bespoke `spec` / `specs` / `sets` fields gone.
- **Phase F — SRD pass.** ✅ Shipped (this commit). SRD-18b §"The Comprehension model" plus the canonical-traversal table now describe the unified shape.

---

### Phase A — Lift the AST

1. Create `nbrs-variates/src/comprehension/{mod.rs, ast.rs}`.
   Define `Clause`, `ComprehensionMode`, `Comprehension`.
   No behavior moved yet — pure type addition.
2. Update `ScenarioNode` and `ScopeKind` to embed
   `comprehension: Option<Comprehension>` alongside the
   current bespoke fields (additive). Existing code paths
   keep working.

**Risk: low.** Pure type addition. Tests stay green.

### Phase B — Move the parser

3. Move `parse_one_clause`, `split_respecting_parens`,
   `parse_combination_specs` from
   `nbrs-workload/src/parse.rs` to
   `nbrs-variates/comprehension::parse`. Add
   `parse_comprehension_spec` as the one entry point. Keep
   the YAML-shape detection (string vs list vs object) in
   nbrs-workload — it's YAML-shaped, not GK-shaped.
4. nbrs-workload's parser populates the new `comprehension:`
   field by calling into nbrs-variates.

**Risk: low.** Mechanical moves.

### Phase C — Move evaluation

5. Move `evaluate_spec`, `parse_list_with_types`,
   `pre_evaluate_clause`, `value_to_gk_type_name`,
   `collect_string_interp_refs` from `scope.rs` into
   `comprehension::eval`. Re-export from scope.rs for the
   migration window.
6. `interpolate_via_kernel` moves with them — it only reads
   GK kernel state, no activity dependencies. (Or stays in
   activity with a callback shape; pick simpler.)
7. Update `TupleComprehension::enumerate` to call
   `comprehension::enumerate_tuples`.

**Risk: medium.** Threading the kernel handle (already an
Arc) through the new API is mechanical; the
interpolate-cross-call decision is the only design choice.

### Phase D — Move synthesis

8. Lift the chunk of `scope::build_scope` that emits the
   comprehension scope's GK source (extern declarations,
   final injections for workload params, cascade externs).
   ~200 LoC. Lands as
   `comprehension::synthesize_scope_source`.
9. `scope::build_scope` becomes a thin caller — composes
   synthesis + compile + install.

**Pre-step:** write golden-output tests against
`build_scope`'s current output so the refactor preserves
byte-for-byte.

**Risk: medium-high.** `build_scope` is intricate (manifest
resolution, pragma cascade, cursor extents). The synthesis
sub-step has clear inputs/outputs — extracting it is
mechanical, but the call-site refactor needs care.

### Phase E — Drop the duplicated representations

10. Remove the old `ScenarioNode::ForEach { spec: String,
    children }` variant fields once everyone reads from the
    embedded `Comprehension`. Same for `ScopeKind`.
11. Remove `TupleComprehension` — replaced by direct calls
    into `comprehension::enumerate_tuples`.

**Risk: low** (after D). Pure removal.

### Phase F — SRD pass

12. Update SRD-18b §"Iteration variables as scope outputs" +
    new section on the `Comprehension` model.
13. Update SRD-10 §"GK Language" to mention that
    comprehensions are part of the GK API surface (even if
    the DSL doesn't have a `for_each` keyword today, the
    model is GK-owned).

---

## Open questions / explicitly not in scope

- **DoWhile / DoUntil** are not comprehensions in the
  cartesian/union sense. They stay as
  `ScenarioNode::DoWhile { condition, counter, ... }` with
  their own evaluation path. Out of scope.
- **`for_each_union`'s sub-space-preservation** — the current
  shape (`Vec<Vec<(var, expr)>>` where each inner Vec is
  one sub-space) round-trips through
  `ComprehensionMode::Union` faithfully. Tested by existing
  cases in `nbrs-workload/tests/for_each_forms.rs`.
- **Workload-param interpolation in clause RHS**
  (`"limit in {k_{k}_limits}"` resolving the inner `{k}`
  against prior clauses' values): today done by
  `pre_evaluate_clause` building per-clause sub-kernels.
  Algorithm unchanged; logic moves to `comprehension::eval`.
- **A new `for_each` keyword in the GK DSL itself.**
  Possible follow-up; doesn't block this refactor.
- **Streaming evaluation** for very large iteration sets.
  Today everything's pre-enumerated into a Vec; fine for
  workload sizes we run. Future limit if iteration counts
  grow beyond ~1M.
- **Stability of synthesis output.** Synthesised string
  becomes structured `GkSourceFragments` so byte-level
  changes aren't a public-API concern.

---

## Cross-references

- [SRD 18b](../sysref/18b_scenario_tree_and_scheduler.md)
  §"Scope coordinates" — the existing kernel-side invariant
  this plan builds on.
- [SRD 11](../sysref/11_gk_evaluation.md)
  §"Effectively-Const Nodes" — defines `IterationExtern`,
  the structural marker comprehensions emit.
- [SRD 13b](../sysref/13b_gk_combination_modes.md) —
  semantics of for_combinations / for_each_union.
- [SRD 13c](../sysref/13c_gk_scope_model.md) — how scope
  composition (`bind_outer_scope`, manifest extraction)
  interacts with what synthesis emits.
