# SRD-83 Workload-Shell Evaluation — Handoff

**Date:** 2026-06-09  **Branch:** polydat_brainswap  **Status:** ✅ **LANDED + green.**
The executor subsystem described below is now wired (`nbrs-activity/src/workload_shell.rs`
+ `ExecCtx.workload_shell`/`workload_stop_when` + `run_phase` feeding + the
`run_siblings_concurrently` walk-stop check + the extended per-phase `each` filter +
the runner-side shell build). Tests: 3 unit (`workload_shell.rs`) + e2e
`nbrs/tests/workload_shell_e2e.rs`. Suites green: nbrs-activity 752, nbrs-workload 278,
polydat 1359, nbrs e2e all pass.

**Implementation notes / deviations from the plan below:**
- The shell accumulator is **shared atomics behind an `Arc`** (not plain `u64` on a
  cloned `ExecCtx`) — `ExecCtx` is cloned per concurrent task, so the `children_*`/op/error
  counters must be shared state for a phase finishing anywhere in the tree to feed the same
  shell. `Arc<WorkloadShell>` clones by reference.
- **Feed + evaluate live in `run_phase`** (where the outcome is built), not in
  `execute_tree_at`. The walker's role is reduced to **consulting `should_stop()`** before
  each sibling — the clean split: `run_phase` reports the outcome to the shell handler, the
  walker enacts the halt. This handles nested phases (any depth) for free.
- **No default error-rate condition at the workload level** (`None` passed to
  `build_for_phase`) — the error-rate breach stays a per-phase concern; a workload-wide
  aggregate error rate across phases of different sizes would be a surprising new semantic.
- A walk-stop returns **`Ok`** (graceful stop) unless a task `Err`'d; session-level
  `Validity` rides on the failing phase's own `Err`. The two-axis `Outcome` effect mapping
  (fail vs stop) remains the SRD-83 step-4 follow-up.

The original task brief follows (kept for the rationale + integration-point map). Read the
canonical SRDs first: `docs/sysref/82_uniform_execution_shells.md`, `83_stop_conditions.md`,
`84_grammar_safe_matter.md`, and **`18b_scenario_tree_and_scheduler.md` §"Single Walker
Contract"** (the walk is tripwire-protected — see Gotchas).

---

## The immediate task

Evaluate **workload-level** stop conditions at the workload shell: aggregate child phase
outcomes into the `children_*` runtime-state wires, evaluate the workload's `stop_when`
predicates after each phase, and halt the remaining walk on a trip (the scenario
stop-on-error default).

Scenarios are deferred (they're step-sequences — `HashMap<String, Vec<ScenarioStep>>` — with
no struct home for `stop_when`; the **workload** is the clean first shell: one `Workload`
struct, one `ScopeKind::Workload` node with a `cached_kernel`).

### Three structural facts that shape it

1. **`children_*` wires are greenfield.** `RuntimeState { children_total, children_failed,
   children_done }` exist in `nbrs-activity/src/stop_conditions.rs` but **nothing populates
   them**. The aggregation is new.
2. **Phase outcomes don't surface to the parent walk.** `run_phase(ctx, phase_name) ->
   Result<(), String>` (executor.rs ~1856) records the `PhaseOutcome` *internally*
   (sqlite/readout); a failed phase returns `Ok(())`. The parent walk never sees
   Failed/Succeeded. The accumulator must be fed where `run_phase` builds the outcome
   (`PhaseOutcome::failed(...)` ~executor.rs:3556, and the success path nearby).
3. **Walk-stop is the stop-on-error.** A tripped workload condition must halt the *remaining*
   walk — a `walk_stop` flag checked in `execute_tree_at` before each child.

### The plan

- **`ExecCtx`** (executor.rs, struct ~line 200–260; `current_scope_idx` is at ~251) gains:
  - a workload accumulator: `children_total/failed/done: u64` + `total_op_count/total_error_count: u64`;
  - a workload `StopConditionSet` (`Option`, built lazily/once);
  - a `walk_stop: bool` flag.
- **Build the workload set once** against the `ScopeKind::Workload` node's `cached_kernel`
  (`ctx.scope_tree` has the node; get its idx, then `nodes[idx].cached_kernel.get().cloned()`).
  Source = `workload.stop_when` filtered to `each ∋ {SelfScope, Workload}` + (optionally) the
  global `error_rate_max` desugar. Reuse `StopConditionSet::build_for_phase(kernel,
  error_rate_max, &when_strings)` — it's level-agnostic despite the name (consider renaming to
  `build_for_shell`).
- **`run_phase`** increments `ctx`'s accumulator from each `PhaseOutcome` it builds
  (children_total++, children_failed++ on Failed else children_done++; add the phase's op/error
  totals from `activity.metrics`).
- **`execute_tree_at`** after each phase: build a `RuntimeState` from the accumulator, call
  `set.evaluate(&state)`; on `Some(reason)` set `ctx.walk_stop = true` (+ record a stop reason /
  workload-failure). Check `ctx.walk_stop` before walking each child; if set, stop descending.
- **Completeness:** extend the *per-phase* `each` filter (executor.rs, the `ActivityConfig`
  build ~2899, `stop_when:` field) to ALSO gather workload-level `each: phase` declarations —
  today it only gathers the phase's *own* `stop_when`. (i.e. `workload.stop_when.filter(each ∋
  phase)` ∪ `phase.stop_when.filter(each ∋ {self, phase})`.)

---

## What's already landed (the chain below this task) — all green

### Polydat (`polydat/src/dsl/`)
- **SRD-84 P1 `&&`/`||`** (eager, truthiness-normalizing desugar), **P1b `as` cast**
  (`Expr::Cast`, atom-binding postfix, alignment-only fusion; **no lossy numeric narrowing** —
  `f64→u64` errors, pointing to `f64_to_u64`/`round_to_u64`/`floor_to_u64`/`ceil_to_u64`),
  parens, the f64-extern infer fix.
- **SRD-84 shapes (`polydat/src/dsl/stub.rs`):**
  - **`ExprStub`** — `parse(name, src)` (boundary parse), `returning::<T: Wire>()` (wraps in the
    `as` cast to `T::PORT`), `volatile()`, `into_statement()`.
  - **`GraphMatter`** (shape 1) — `extern_wire::<T: Wire>(name)` (constructs `Statement::ExternPort`,
    not parsed), `bind(ExprStub)`, `into_statements() -> Vec<Statement>`.
  - **`ScopedExpr`** (shape 2) — `bind(parent: &PolydatKernel, output, GraphMatter)` builds a
    sub-context of the parent; `set(name, Value)` / `dataflow()` inject; `eval()->Value` /
    `is_true()->bool` (truthiness = `U64 != 0`, NOT `as_bool` which panics on non-Bool).
  - `BodyFragment::Statements` (the grammar-safe matter form) already existed (SRD-67 Decision 4).

### Stop conditions (`nbrs-activity/src/stop_conditions.rs`)
- `RuntimeState { op_count, error_count, elapsed_ms, children_* }` + `error_rate()` +
  `inject_into<D: Dataflow>` + `trips(&mut ScopedExpr) -> bool`.
- `wire::*` name constants; `extern_matter() -> GraphMatter` (the runtime-state externs, typed).
- `compile_stop_condition(phase_kernel, idx, when) -> Result<ScopedExpr>` — binds a
  u64-truthiness predicate stub against the kernel (NOT baked into its matter).
- **`StopConditionSet`** — `build_for_phase(kernel, error_rate_max, &[String]) -> Result<Self>`
  (default error-rate condition `op_count >= 50 && error_rate > {max}` — the 50-op floor
  preserves the retired `AggregateGuard`'s behavior — plus each declared predicate),
  `evaluate(&RuntimeState) -> Option<String>` (first trip's reason), `is_empty()`, `empty()`.

### Phase-level executor wiring (landed)
- `Activity` gained `phase_kernel: Option<Arc<PolydatKernel>>` (the phase node's `cached_kernel`);
  `with_params_and_sigdigs` takes it; `run_phase` passes
  `ctx.scope_tree.phase_node_by_name(phase_name).and_then(|i| nodes[i].cached_kernel.get().cloned())`.
- The activity **drain loop** builds `StopConditionSet::build_for_phase(&activity.phase_kernel, …)`
  against the **native phase kernel** (the conjured-root stopgap is deleted) and evaluates it per
  Tick → trip → fail + `stop_flag`.
- `ActivityConfig` gained `error_rate_max` + `stop_when: Vec<String>` (the `each`-filtered phase
  predicates).
- **`AggregateGuard` retired** — deleted `nbrs-errorhandler/src/aggregate.rs` + its lib.rs export;
  removed `ErrorPolicy.guard` (field/construction/import/test). `ErrorPolicy` keeps `router` +
  `derived` breadth cache.

### The `each:` distribution model (landed in `nbrs-workload/src/model.rs`)
- `ScopeLevel { SelfScope("self"), Op, Phase, Scenario, Workload }` (aligned to `ScopeKind`).
- `StopConditionSpec.each: Vec<ScopeLevel>` — scalar-or-list via `de_each` (untagged
  `OneOrMany`), default `[SelfScope]`. Distribution rule: `node.level ∈ each ∧ in declaring
  subtree` — **purely structural, never inferred from the predicate's content**.
- **`Workload.stop_when: Vec<StopConditionSpec>`** committed THIS session + top-level YAML parse
  (`parse.rs` reads `obj.get("stop_when")`, same shape as the phase block); 2 inline.rs literals
  default empty. (The phase block parse is `parse.rs` ~1160.)

---

## The user-canonical design principles (do not violate)

- **No scope inferencing for placement.** Distribution is the *declared* `each:` selector,
  matched *structurally* over the scope tree. Never inspect a predicate's wires to decide where
  it lives. This was an explicit, emphatic correction.
- **Native scope, not a conjured root.** Predicates bind to the scope they're declared for (its
  `cached_kernel`), riding along with the matter walk — never a throwaway kernel.
- **Two shapes, not conflated.** GraphMatter = matter the compiler builds *into* a kernel;
  ScopedExpr = an expression *bound to* a kernel scope, callable. (SRD-84 Part 3.)
- **Build-cost ⟂ static-debt/modularity-soundness.** Judge end-state design, not how many sites a
  change touches.

## Gotchas / environment

- **One-Walker tripwire** (memory `feedback_one_walker`): pre-map / dryrun / runtime are ONE
  walker (`execute_tree_at`) at different depths; serial/concurrent are ONE harness. Do NOT add a
  parallel walker or a "mode" flag. Structural work (scope-kernel construction via
  `build_subscope`, SceneTree push, iter-step) ALWAYS runs; the depth discriminant gates
  executional work. Read SRD-18b §"Single Walker Contract" + SRD-02 §"One Concurrency Path".
- **No blocking in async** (memory `feedback_no_blocking_in_async`): the drain loop is async;
  `std::sync::Mutex` is used there but only **uncontended** (lock/act/unlock, no await held).
- Paths: `PolydatKernel` is re-exported at `polydat::kernel::PolydatKernel` (the inner
  `subcontext::kernel` module is private). `PolydatMatter` is `polydat::kernel::subcontext::PolydatMatter`.
  `Span` is `polydat::dsl::lexer::Span` (NOT `dsl::ast`). For `find_input` you need
  `polydat::kernel::Metadata` in scope (supertrait of `Dataflow`).
- **Persona = greenfield** (`local/persona_active.md`): most-correct + architecturally consistent
  change; effort and backwards-compat are NOT factors.
- **No git ops from Claude** — the user runs git; leave changes in the working tree.
- **Project TMPDIR** — `.cargo/config.toml` redirects to `target/test-tmp`; never hardcode `/tmp/foo`.

## Test commands (all green at handoff)

```
cargo test -p polydat --lib                 # 1359
cargo test -p nbrs-workload --lib           # 278
cargo test -p nbrs-activity --lib           # 749  (stop_conditions 4, error_policy 4)
cargo build -p nbrs --bin nbrs              # builds
```

## Pointers into the canonical record

- Full running narrative: memory `project_srd82_execution_shells.md` (the SRD-82/83/84 note —
  has every landed step + the workload-shell plan in detail).
- SRDs: `docs/sysref/82_uniform_execution_shells.md`, `83_stop_conditions.md` (the `each:`
  selector section + the firing-event Part 3), `84_grammar_safe_matter.md` (the two shapes +
  the `as` narrowing policy).
- Still pending after this task: scenario-level (step-sequence model home); the real
  `FiringEvent` enum (PhaseStart/PhaseEnd + settle backoff daemon) — currently only the
  drain-loop Tick; two-axis `Outcome` effect mapping (a trip is currently always Failed+stop).
