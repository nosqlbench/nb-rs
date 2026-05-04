# SRD / sysref cross-reference

Single entry point for the architectural design references. When in
doubt about a system shape, **read here first**, then jump to the
specific SRD.

The detailed index (every SRD, by subsystem) is at
[`sysref/00_index.md`](sysref/00_index.md). The historical
design-discussion docs are under [`design/`](design/). What this
file does is keep a small, opinionated map of the *load-bearing*
documents in front of any reader — including AI assistants whose
context starts cold each session.

The source of truth is the code; SRDs explain the *intent* behind
it. Where the code has drifted from the SRD, the SRD wins unless
a more recent SRD revision says otherwise. Drift is not evidence
against the design.

---

## Architectural rules that everything else builds on

These are the axioms. If a design proposal contradicts one of
these, the proposal is wrong, not the rule.

| Rule | Where it's stated |
|------|-------------------|
| **GK kernels are the canonical state holder for scope, binding, and name resolution.** Multiple sources of resolvable values is the documented anti-pattern. | [SRD 16](sysref/16_gk_scoping.md), [SRD 18b](sysref/18b_scenario_tree_and_scheduler.md) |
| **One scope per non-trivial scenario node.** Workload → Scenario → for_each → … → Phase. Iteration variables are scope outputs, not text substitutions. Leaf phases compile once. | [SRD 18b](sysref/18b_scenario_tree_and_scheduler.md) §"Iteration variables as scope outputs", §"M3 — per-scope kernel composition" |
| **Auto-extern + `bind_outer_scope` is how layering works.** Inner kernel sees outer values as pre-populated input slots. Caller-side scope-tree walking for name resolution is wrong; the kernel encapsulates it. | [SRD 16](sysref/16_gk_scoping.md) §"How It Works: Plugging Graphs Together", §"Per-Scope Canonical Kernel Cache" |
| **Multi-clause `for_each` is a single-scope dependent tuple comprehension.** `"k in {k_values}, limit in {k_{k}_limits}"` is one scope; lex-order clause binding; clause N's spec sees clauses 0..N-1's values via interpolation against the scope's kernel. Total tuples = sum, not Cartesian. | [SRD 18b](sysref/18b_scenario_tree_and_scheduler.md) §"M3 — per-scope kernel composition" (M3.3) |
| **Native types as the general rule.** Iter var types come from the spec's pre-evaluated value type — `u64` / `f64` / `bool` / `String`. JIT optimizes scalar fast paths; capability is not f64/u64-only. Conversion shadows for string-based accessors live at consumer boundaries, not at the kernel surface. | [SRD 18b](sysref/18b_scenario_tree_and_scheduler.md) §"M3 — per-scope kernel composition" (M3.2) |
| **GK `Value` is type-flexible.** Str, Bool, U64, F64, VecF32, VecI32, … JIT optimizes scalar fast paths; capability is not f64/u64-only. | [SRD 10](sysref/10_gk_language.md), [SRD 11](sysref/11_gk_evaluation.md) |
| **GK kernel = `Arc<GkProgram>` + `GkState`.** Program is the compiled DAG, state is per-instance. State cloning per fiber happens at the phase boundary only; intermediate scopes are single-instance. | [SRD 11](sysref/11_gk_evaluation.md), [SRD 18b](sysref/18b_scenario_tree_and_scheduler.md) |
| **Three evaluation lifecycles, not two.** *Compile-const* (resolved at GK compile), *scope-init* (resolved once per scope activation, after `bind_outer_scope`), *dynamic* (resolved per pull at execution time). `for_each` / `for_combinations` iteration externs are **effectively-const** for the duration of one activation; `do_while`/`do_until` counters and graph inputs are not. | [SRD 11](sysref/11_gk_evaluation.md) §"Three Evaluation Lifecycles", §"Effectively-Const Nodes" |
| **`init` is a const-like constraint, not a hint.** An `init` binding's upstream wire chain must be entirely effectively-const at scope-init time. Compile-time check (Plan A) catches structural violations; scope-activation check (Plan B) catches runtime materialization failures. No soft fall-through to dynamic eval. | [SRD 11](sysref/11_gk_evaluation.md) §"Init Binding Contract" |
| **Scope coordinates are a kernel invariant.** Every initialised `GkKernel` exposes a leaf-first `scope_coordinates()` path: one ordered name→value `ScopeCoord` per enclosing comprehension scope. Populated by construction + `bind_outer_scope` — consumers don't walk the scope tree. Classification is structural: `InputKind::IterationExtern` AND not `is_inherited`. | [SRD 18b](sysref/18b_scenario_tree_and_scheduler.md) §"Scope coordinates", `nbrs-variates::kernel::scope_coords` |
| **`GkKernel::from_program(Arc<GkProgram>)` is the cache-and-rebind primitive.** Compile once, instantiate fresh per execution context. Documented for SRD-18b. | `nbrs-variates::kernel::gkkernel` (docstring) |
| **Strict mode is opt-in but cumulative.** Promotes warnings to errors at the boundary they fire. Default is loose-and-warning. | [SRD 15](sysref/15_strict_mode.md) |

---

## Most-load-bearing SRDs by topic

Topics show up here when an architectural change touching them
should be preceded by re-reading the SRD in full.

### Scope, binding, composition, iteration

- [SRD 16: GK Scope Model](sysref/16_gk_scoping.md) — `bind_outer_scope`, `scope_values`, auto-extern, manifest extraction, shared/final modifiers, the scope-composition contract.
- [SRD 18b: Scenario Tree, Scope Hierarchy, Scheduler](sysref/18b_scenario_tree_and_scheduler.md) — one scope per scenario node, iteration vars as scope outputs, leaf-phase-compiles-once, pragma chain along the scope tree, scheduler abstraction with `schedule=<level0>/<level1>/...`.
- [SRD 18c: Comprehension Syntax](sysref/18c_comprehension_syntax.md) — layered grammar of clause expressions: literal lists, ranges, named generators, `where` filter, SI suffixes, tuple LHS (parallel-iter + destructure), bucket/concat/interval LUT expansions.
- [SRD 18d: Comprehension Traversal Order](sysref/18d_comprehension_traversal_order.md) — emission order of tuples: lex, diagonal, extrema-first, concentric shells, space-filling (Halton/Sobol/LHS), custom; composes with `where` filter; truncation as part of the ordering declaration.
- [SRD 13b: GK Combination Modes](sysref/13b_gk_combination_modes.md) — for_combinations / for_each_union semantics; the multi-clause dependent tuple rules.

### GK kernel internals

- [SRD 10: GK Language and Compilation](sysref/10_gk_language.md) — DSL, compiler pipeline, type system, GK as the unified runtime-state surface.
- [SRD 11: GK Evaluation Model](sysref/11_gk_evaluation.md) — kernel/state split, input spaces, three lifecycles (compile-const / scope-init / dynamic), effectively-const classification, init-binding contract (Plan A compile-time + Plan B scope-activation checks), constant folding.
- [SRD 12: GK Standard Library](sysref/12_gk_stdlib.md) — node catalog with type signatures.
- [SRD 16: GK Engines](sysref/16_gk_engines.md) (note: file name collision with scope doc — distinct numbering convention) — provenance push/pull, engine variants, auto-selection.
- [SRD 16b: GK JIT Wiring](sysref/16_gk_jit.md) — Cranelift boundary, `invoke_with_catch`, setjmp/longjmp.

### Workload model and parameters

- [SRD 20: Workload Model](sysref/20_workload_model.md) — YAML → ParsedOp → blocks/tags/normalization.
- [SRD 21: Parameters and Bind Points](sysref/21_parameters.md) — param resolution, scope hierarchy.
- [SRD 23: Dynamic Controls](sysref/23_dynamic_controls.md) — runtime-mutable parameters via the component tree.
- [SRD 24: Component Lookup](sysref/24_component_lookup.md) — selector grammar, dimensional-label predicates.

### Execution and adapters

- [SRD 30: Adapter Interface](sysref/30_adapter_interface.md) — DriverAdapter/OpDispenser contract.
- [SRD 31: Op Pipeline](sysref/31_op_pipeline.md) — resolve → wrap → execute → metrics.
- [SRD 32: Dispenser Wrappers](sysref/32_wrappers.md) — TraversingDispenser, ValidatingDispenser, composition order.
- [SRD 33: Result Validation](sysref/33_result_validation.md) — relevancy, ground truth, binding visibility.
- [SRD 34: Capture Points](sysref/34_capture_points.md) — inter-op data flow.

### Metrics and observability

- [SRD 40: Metrics Framework](sysref/40_metrics.md) — instruments, frames, delta semantics, reporters.
- [SRD 42: Windowed Metrics Access](sysref/42_windowed_metrics.md) — user-specified cadences, auto-intermediate buckets.
- [SRD 46: Reports](sysref/46_reports.md) — unified `report:` block; plots and tables; figure enumeration; CLI surface; style language.

### Concurrency, errors, strict mode

- [SRD 02: Concurrency Model](sysref/02_concurrency_model.md) — async fibers, tokio runtime, no blocking primitives in async, cycle source, rate limiting.
- [SRD 03: Error Handling](sysref/03_error_handling.md) — error scoping, retry, silent-failure policy.
- [SRD 15: Strict Mode](sysref/15_strict_mode.md) — compile-time enforcement of explicit declarations.

### TUI / CLI / personas

- [SRD 60: CLI Structure](sysref/60_cli.md), [SRD 61: Personas](sysref/61_personas.md), [SRD 62: TUI Layout](sysref/62_tui_layout.md).

---

## Deferred / future work

- [SRD 98: Deferred work](sysref/98_todo_deferred.md) — features whose design is settled but implementation is parked: Tier 2 cursor-state snapshot (SRD-44), `verify:` op runtime check, do-loop checkpointing.

## Design-discussion docs

Living design notes, often more discursive than the consolidated
SRDs. Useful when you need the *why* behind a decision and the SRD
states the *what*.

- [`design/binding_scope_model.md`](design/binding_scope_model.md) — typed-provenance binding scope (`BindingOrigin::IterationVar`, etc.).
- [`design/data_driven_workloads.md`](design/data_driven_workloads.md) — the entity-scoped op-instancing pattern.
- [`design/metrics_architecture.md`](design/metrics_architecture.md) — component tree as canonical name index, GK-aware component scopes.
- [`design/tui_status_display.md`](design/tui_status_display.md) — display surface decoupling.

---

## How to use this document

1. **AI assistants and new readers**: when starting work on
   *anything* touching scope, binding, iteration, GK kernels, or
   composition, read the rules table at the top, then jump to the
   relevant SRD section. Do not propose changes from "what the
   surrounding code does" without first checking what the SRD
   commits to. Drift in code is not evidence; the SRD is the
   commitment.

2. **Authors**: when adding a new SRD or revising an existing
   one, update this file's most-load-bearing section if the new
   content materially affects the architectural rules. The
   detailed [00_index.md](sysref/00_index.md) lists everything;
   this file lists what people should *prioritize*.
