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
| **Polydat kernels are the canonical state holder for scope, binding, and name resolution.** Multiple sources of resolvable values is the documented anti-pattern. | [SRD 13c](sysref/13c_polydat_scope_model.md), [SRD 18b](sysref/18b_scenario_tree_and_scheduler.md) |
| **One scope per non-trivial scenario node.** Workload → Scenario → for_each → … → Phase. Iteration variables are scope outputs, not text substitutions. Leaf phases compile once. | [SRD 18b](sysref/18b_scenario_tree_and_scheduler.md) §"Iteration variables as scope outputs", §"M3 — per-scope kernel composition" |
| **Auto-extern + `bind_outer_scope` is how layering works.** Inner kernel sees outer values as pre-populated input slots. Caller-side scope-tree walking for name resolution is wrong; the kernel encapsulates it. | [SRD 13c](sysref/13c_polydat_scope_model.md) §"How It Works: Plugging Graphs Together", §"Per-Scope Canonical Kernel Cache" |
| **Multi-clause `for_each` is a single-scope dependent tuple comprehension.** `"k in {k_values}, limit in {k_{k}_limits}"` is one scope; lex-order clause binding; clause N's spec sees clauses 0..N-1's values via interpolation against the scope's kernel. Total tuples = sum, not Cartesian. | [SRD 18b](sysref/18b_scenario_tree_and_scheduler.md) §"M3 — per-scope kernel composition" (M3.3) |
| **Native types as the general rule.** Iter var types come from the spec's pre-evaluated value type — `u64` / `f64` / `bool` / `String`. JIT optimizes scalar fast paths; capability is not f64/u64-only. Conversion shadows for string-based accessors live at consumer boundaries, not at the kernel surface. | [SRD 18b](sysref/18b_scenario_tree_and_scheduler.md) §"M3 — per-scope kernel composition" (M3.2) |
| **Polydat `Value` is type-flexible.** Str, Bool, U64, F64, VecF32, VecI32, … JIT optimizes scalar fast paths; capability is not f64/u64-only. | [SRD 10](sysref/10_polydat_language.md), [SRD 11](sysref/11_polydat_evaluation.md) |
| **Polydat kernel = `Arc<GkProgram>` + `PolydatState`.** Program is the compiled DAG, state is per-instance. State cloning per fiber happens at the phase boundary only; intermediate scopes are single-instance. | [SRD 11](sysref/11_polydat_evaluation.md), [SRD 18b](sysref/18b_scenario_tree_and_scheduler.md) |
| **Two evaluation lifecycles: effectively-const and dynamic.** *Effectively-const* covers everything materialised once and frozen for a scope's lifetime — compile-time fold when the wire chain is pure, scope-init materialisation when it depends on iteration externs / params populated by `materialize_wiring_from_outer`. Both paths share the `const` modifier, the same buffer, and the same downstream guarantees. *Dynamic* is everything resolved per pull at execution time. `for_each` / `for_combinations` iteration externs are effectively-const for one activation; `do_while`/`do_until` counters and graph inputs are dynamic. | [SRD 11](sysref/11_polydat_evaluation.md) §"Two Evaluation Lifecycles", §"Effectively-Const Nodes" |
| **`const` is a contract, not a hint.** A `const` binding's upstream wire chain must be entirely effectively-const at scope-init time. Compile-time check (Plan A) catches structural violations (wires reaching `cycle`, capture ports, or non-deterministic sources); scope-activation pull (Plan B) materialises the value once and freezes it. No soft fall-through to dynamic eval. | [SRD 11](sysref/11_polydat_evaluation.md) §"Const Binding Contract" |
| **Scope coordinates are a kernel invariant.** Every initialised `PolydatKernel` exposes a leaf-first `scope_coordinates()` path: one ordered name→value `ScopeCoord` per enclosing comprehension scope. Populated by construction + `bind_outer_scope` — consumers don't walk the scope tree. Classification is structural: `InputKind::IterationExtern` AND not `is_inherited`. | [SRD 18b](sysref/18b_scenario_tree_and_scheduler.md) §"Scope coordinates", `polydat::kernel::scope_coords` |
| **`PolydatKernel::from_program(Arc<GkProgram>)` is the cache-and-rebind primitive.** Compile once, instantiate fresh per execution context. Documented for SRD-18b. | `polydat::kernel::polydatkernel` (docstring) |
| **Strict mode is opt-in but cumulative.** Promotes warnings to errors at the boundary they fire. Default is loose-and-warning. | [SRD 15](sysref/15_strict_mode.md) |
| **The console belongs to the adapter; every nbrs system signal routes through the observer/sink.** Only adapter output (stdout fields, plotter canvas), user-requested reports, and fatal errors write to the console directly. A `println!`/`eprintln!` of a system signal is a bug — it bypasses the sink, `session.log`, and the TUI. Banners are `Debug` (log-only); completion is `RunLifecycle`; user readouts stay visible. A console-owning adapter (`DisplayPreference::Off`) on an interactive TTY reserves the console for itself. | [SRD 41](sysref/41_logging.md) §"Output Routing", [SRD 30](sysref/30_adapter_interface.md) §"Display Preference" |
| **Every execution level is the same shell; results are two orthogonal axes; error handling and stop conditions are two orthogonal subsystems.** Scenario graph, phase, stanza, and op are one recursive execution shell differing only in granularity and configured policy — never in mechanism. A result is `(Disposition: Completed/Interrupted/Skipped, Validity: Succeeded/Failed)`, never a single status mixing "how much ran" with "is it usable." **Error handling** (`ErrorPolicy` — a per-op router: count/warn/retry/stop/fail, resolved by depth-inherit/breadth-share) is orthogonal to **stop conditions** (per-shell **polydat predicates** over runtime-state wires, evaluated at **triggers**, with an Outcome `effect`). The error-rate breach and "stop on error" are stop conditions, not router rules. A `stop` carries a cause (`Interrupt` vs `Fault`). | [SRD 82](sysref/82_uniform_execution_shells.md), [SRD 83](sysref/83_stop_conditions.md) (DRAFT), extend [SRD 03](sysref/03_error_handling.md), [SRD 76](sysref/76_phase_outcome_disposition.md) |

---

## Most-load-bearing SRDs by topic

Topics show up here when an architectural change touching them
should be preceded by re-reading the SRD in full.

### Scope, binding, composition, iteration

The composition family (13 / 13b / 13c) covers how GK
programs combine; read in order if you're new to it.

- [SRD 13: Polydat Modules](sysref/13_polydat_modules.md) — file-based modules, inlining resolution, compiler diagnostic event stream.
- [SRD 13b: Polydat Combination Modes](sysref/13b_polydat_combination_modes.md) — taxonomy of four orthogonal modes: inline, scope composition, subgraph, reification. Read this before reaching for terminology.
- [SRD 13c: Polydat Scope Model](sysref/13c_polydat_scope_model.md) — the scope-composition mode in depth: `bind_outer_scope`, `scope_values`, auto-extern, manifest extraction, shared/const modifiers, `for_each` lifecycle.
- [SRD 13d: Op-template Polydat Scope Layer](sysref/13d_op_template_scope.md) — extends 13c so the op template is a scope of its own; `HasPolydatMatter` classification + scope-flattening; the realisation-lifecycle staged pipeline; `dryrun=op` diagnostic level.
- [SRD 13e: Scope-as-Module Refinement](sysref/13e_scope_as_module.md) — *(design)* sub-scopes become formal `ScopeModule` values with typed import/export contracts; `instance_under(parent)` is the single typed attach operation; kernel-bound handles make cross-kernel mis-routes ill-typed; absorbs the SRD-13d Phase 9 followup architectural fixes into automatic consequences.
- [SRD 13f: Cross-Scope Wire Materialization](sysref/13f_cross_scope_wire_materialization.md) — *(normative; A/B.1/C/D.1 shipped, B.2 partial-storage-only, D.2/E/F pending; D.2/F depend on B.2)* read invariant: inner reads of cross-scope wires return what reading on the owning kernel returns; matter-AST classifies each wire and the interpreter materializes per gradient (inlined constant / value-only cell / read-write shared cell); `shared` is purely a write-permission flag; retires SRD-13c's "Default: Immutable Propagation" snapshot rule; wires layer takes a single kernel handle (no fallback composition outside Polydat API); parser-level phase-into-op binding merge retired in favor of Polydat scope-chain wiring; B.2 full integration blocked on a per-fiber kernel-chain restructure documented in the SRD's "Architectural challenge" section.
- [SRD 18b: Scenario Tree, Scope Hierarchy, Scheduler](sysref/18b_scenario_tree_and_scheduler.md) — one scope per scenario node, iteration vars as scope outputs, leaf-phase-compiles-once, pragma chain along the scope tree, scheduler abstraction with `schedule=<level0>/<level1>/...`.
- [SRD 18c: Comprehension Syntax](sysref/18c_comprehension_syntax.md) — layered grammar of clause expressions: literal lists, ranges, named generators, `where` filter, SI suffixes, tuple LHS (parallel-iter + destructure), bucket/concat/interval LUT expansions.
- [SRD 18d: Comprehension Traversal Order](sysref/18d_comprehension_traversal_order.md) — emission order of tuples: lex, diagonal, extrema-first, concentric shells, space-filling (Halton/Sobol/LHS), custom; composes with `where` filter; truncation as part of the ordering declaration.
- [SRD 18e: Comprehension Canonical Reference](sysref/18e_comprehension_canonical_reference.md) — the implementer contract: full AST in one place, Cartesian/Union detection, coordinate-set contract, index-space ordering rule, Union+non-lex rejection, `where` predicate semantics, Layer 7 extension path with `Value::Tuple` dependency. Read after 18c/18d, before writing or auditing comprehension code.
- [SRD 18f: Comprehension Source Forms — List & String Comprehensions](sysref/18f_comprehension_source_forms.md) *(DRAFT)* — the source position (right of `in`): peel-exactly-one-level invariant; **list comprehension sugar** `[S]` (no-peel) / `[S…]` (destructure) / `[a,b,c]`; **string comprehension** double-quoted = iterable (token-strip on `, ; ws`, colons retained) vs single-quoted = atomic, *positional* (only in the source slot); relaxed `x in S` ≡ infer between `[S…]`/`[S]` via the canonical `iteration_interior` predicate; list elements parsed by the core expression grammar (bare = wire ref, quoted = string — retires bare-word→string coercion); interpolation declared orthogonal to quote-kind. Supersedes the ad-hoc peel/wrap in `evaluate_spec_internal`.

### Polydat kernel internals

The language / evaluation / stdlib triplet (10 / 11 / 12)
specifies the kernel itself; the engines pair (16 / 16b)
specifies how the kernel runs.

- [SRD 10: Polydat Language and Compilation](sysref/10_polydat_language.md) — DSL syntax, compiler pipeline, type system, op-level bindings, cursor declarations, Polydat as the unified runtime-state surface.
- [SRD 11: Polydat Evaluation Model](sysref/11_polydat_evaluation.md) — kernel/state split, input spaces, two lifecycles (effectively-const / dynamic), effectively-const classification, provenance-based invalidation, const-binding contract.
- [SRD 12: Polydat Standard Library](sysref/12_polydat_stdlib.md) — node catalog with type signatures, P3 JIT eligibility, fusion patterns.
- [SRD 14: Polydat Config Expressions](sysref/14_polydat_config_expressions.md) — `{...}` form for init-time constants in activity config.
- [SRD 16: Polydat Engines](sysref/16_polydat_engines.md) — compilation levels P1/P2/P3, provenance push/pull, engine variants, auto-selection.
- [SRD 16b: Polydat JIT Wiring](sysref/16b_polydat_jit.md) — Cranelift boundary, `invoke_with_catch`, setjmp/longjmp.
- [SRD 84: Grammar-Safe Matter & Boolean Operators](sysref/84_grammar_safe_matter.md) *(DRAFT)* — moves synthesized matter off `BodyFragment::PolydatSource(String)` onto a grammar-safe (AST) form (SRD-67); a caller-native expression-stub API **generic over `T: Wire`** (SRD-80b — call sites bind the return type at compile time) with a truthy/falsy default for indeterminate predicates; `&&`/`||` as **eager** truthiness combinators at lowest precedence (short-circuit deferred — needs conditional-pull); a uniform `<expr> as <type>` **alignment-only, idempotent** type-fusion cast into the SRD-79 layer. Comparisons yield `U64` 1/0 (truthiness), not `Bool`. Synthesizers (metrics, poll, scope, SRD-83 stop conditions) re-base off string concatenation. Forward-pointers in SRD 10 / 11 / 67.

### Workload model and parameters

- [SRD 20: Workload Model](sysref/20_workload_model.md) — YAML → ParsedOp → blocks/tags/normalization.
- [SRD 21: Parameters and Bind Points](sysref/21_parameters.md) — param resolution, scope hierarchy.
- [SRD 72: Workload `extends:`](sysref/72_workload_extends.md) *(DESIGN — not yet implemented)* — single-parent composition for sibling workloads.
- [SRD 23: Dynamic Controls](sysref/23_dynamic_controls.md) — runtime-mutable parameters via the component tree.
- [SRD 24: Component Lookup](sysref/24_component_lookup.md) — selector grammar, dimensional-label predicates.

### Execution and adapters

- [SRD 30: Adapter Interface](sysref/30_adapter_interface.md) — DriverAdapter/OpDispenser contract.
- [SRD 31: Op Pipeline](sysref/31_op_pipeline.md) — resolve → wrap → execute → metrics.
- [SRD 32: Dispenser Wrappers](sysref/32_wrappers.md) — TraversingDispenser, ValidatingDispenser, composition order.
- [SRD 33: Result Validation](sysref/33_result_validation.md) — relevancy, ground truth, binding visibility.
- [SRD 34: Capture Points](sysref/34_capture_points.md) — inter-op data flow via `[name]` bind-point syntax.
- [SRD 69: Capture Semantics](sysref/69_capture_semantics.md) *(DRAFT — parked for future)* — unified contract across the four capture sources (bind-points, result-bindings, magic externs, adapter-direct); sink contract via `ctx.wires.write`; slot allocation via the closure-binding economy; collision rules; `ResultBody::capture(name)` adapter-side hook; multi-row column projection with per-element-transform paths (A: pre-baked nodes / B: capture-pipeline declaration / C: Polydat closures).
- [SRD 70: Capture via JSON Path Expressions](sysref/70_capture_paths.md) *(DRAFT — first-wave)* — practical-shipping shape: extends the existing result-binding path-expr form with `[*]` wildcard for column projection; auto-types to `VecI32` / `VecF32` / `Vec<Str>` for uniform-typed columns; validator reads `evaluations.relevancy.actual:` as a wire reference; no adapter API extension or new YAML shapes. Feeds the use cases SRD-69 catalogued.
- [SRD 35: Driver Resource Lifecycle and Sharing](sysref/35_driver_resources.md) *(DESIGN; Push A/B implemented)* — generic adapter-shell vs driver-instance split; `ResourceKey` value-equality identity; instance-shaping vs shell-shaping param partition (normative); `ShareCapability` driver-declared + `ResourceSharePolicy` user-elevatable; resource-API trait pair `can_share()` (capability declaration: thread-safe + designed for sharing) and `can_support_more_load()` (live capacity: `true` = route another caller here, `false` = saturated, spawn a sibling); pool-level guard catches `quiescent-decline` driver bugs; pre-map-driven multi-generation refcount lifecycle with explicit async `close()`; debug `resource.{attach,init,share.spawn,detach,close}` event surface. CQL is the prototype consumer.
- [SRD 73: Op Field Modifiers](sysref/73_op_field_modifiers.md) *(DRAFT — not yet implemented)* — generic `OpFieldModifier<T>` + `ModifierChain<T>` + `ModifierTraceSink` in `nbrs-activity`; monadic-compose enhancer pattern ported from upstream NB Java; initializer-time Polydat scope resolution via existing `PolydatKernel::lookup` (no new resolution machinery); JSON-only lazy diagnostic gating; CQL universal field superset (`consistency`, `serial_consistency`, `request_timeout_ms`, `page_size`, `cql_trace`) with per-engine modifier impls bridging into scylla and cassandra-cpp per-statement APIs; terminology disambiguation: CQL query-tracing subsystem vs Rust `tracing` crate vs nb-rs event-log emission.
- [SRD 74: None Propagation](sysref/74_none_propagation.md) *(NORMATIVE — Rules 1 & 3 + conditional-shadow `const` SHIPPED; Rule 2 explicit-optionality syntax — DESIGN)* — `Value::None` propagates through every operation that consumes a value, never silently coerced to `""` or `"None"`.
- [SRD 75: Phase-Level Poll](sysref/75_phase_poll.md) *(DRAFT — design for the synchronizer pattern)* — `poll:` block on a `WorkloadPhase` loops the phase's cycle execution until a Polydat predicate over captures returns true; captures land on the phase scope as `shared` wires (per SRD-13c / SRD-67 Rule 1) so the predicate and `if:` conditions read them through the canonical kernel chain; one walker contract preserved (per-phase runtime concept at `depth=Cycle`, not a new walker).
- [SRD 77: Working Sessions, Executions, and `refine`](sysref/77_working_sessions_and_refine.md) *(DRAFT)* — Two-tier entity model: persistent Session containing 1..N Executions. New `refine` verb (topology metaphor: refining an open cover) layers a new execution onto an existing session, running only phases that are new or whose chain-hash differs from any prior execution. Universal `--new-session` / `--resume-session` / `--session` flag trio across every nbrs verb; strict mode forbids the bare ambiguous form. `sessions/` directory rename (was `logs/`). Phase fingerprint is the existing `GkProgram::instance_hash` from SRD-44.
- [SRD 76: Phase Outcome Disposition](sysref/76_phase_outcome_disposition.md) *(DRAFT)* — structured `PhaseOutcome { status: PhaseStatus, errors: Vec<PhaseErrorDetail>, duration_secs }` recorded per phase, carried on the SceneTree and persisted to `phase_outcomes` + `phase_errors` SQLite tables. `PhaseStatus` (`Completed`/`Failed`/`Skipped`/`CursorSuspended`) is the per-phase terminal state; `SessionDisposition` (`Success`/`Failure`) is the binary projection at the session level — single source for process exit code, terminating status line, and `nbrs replay` header. Two new Readouts (`phase_outcome`, `phase_errors`) bound to `on_phase_end` render the data; `nbrs replay` reads the structured store and re-runs the same Readout binder so realtime + replay use one projection. Closes the gap where failed phases don't surface in `nbrs replay`. Three layers in place: (1) `Printf::eval` propagates None through string interpolation; (2) `const NAME := <expr>` (whose RHS references at least one name) auto-externs NAME so the two-tier `lookup` falls through to outer-scope bindings when the const folds to None, with `materialize_wiring_from_outer` using value-copy via `outer.lookup` (preserves the SRD-13f read invariant transitively across scope chains); (3) `substitute_via_wires` uses `Value::to_display_strict` and errors loudly on any unresolved bind-point reaching wire-protocol render. Pure-literal consts are exempt from auto-extern (Gate 2 preserved). Workload authors get conditional-shadow semantics from `set:` for free — the desugar is unchanged canonical GK. Resolves the cndb `source_model 'NONE'` / `'source_model': ''` corruption class from SRD-73's `set:` integration.

### Metrics and observability

- [SRD 40: Metrics Framework](sysref/40_metrics.md) — instruments, frames, delta semantics, reporters.
- [SRD 42: Windowed Metrics Access](sysref/42_windowed_metrics.md) — user-specified cadences, auto-intermediate buckets.
- [SRD 46: Reports](sysref/46_reports.md) — unified `report:` block; plots and tables; figure enumeration; CLI surface; style language.
- [SRD 65: Plot Multi-Axis](sysref/65_plot_multi_axis.md) *(PROPOSED)* — extends plots from `y` + `y2` to `y` + `y2`/`y3`/`y4` with the full per-axis directive surface (range / scale / ticks / label); right-rail stacking layout; axis-projection renderer for axis 3+.
- [SRD 47: MetricsQL Streaming Aggregation](sysref/47_metricsql_streaming.md) — `Reducer` algebra (distributive / algebraic / holistic), `StreamingPlan` compiler, ingest+snapshot, equivalence property test as the load-bearing artifact, holistic-function and sliding-window deferred decisions.
- [SRD 48: MetricsQL Continuous-Query Runtime](sysref/48_metricsql_continuous_query.md) — Orchestration layer over SRD-47: plan registry, pull/push/watchable sample feeds, actor+ArcSwap concurrency (mirrors SRD-40 lock-free pattern), lifecycle, window framing policies (tumbling / grid), TUI+web binding, memory bounds. First push is contained; followups mapped explicitly.

### Concurrency, errors, strict mode

- [SRD 02: Concurrency Model](sysref/02_concurrency_model.md) — async fibers, tokio runtime, no blocking primitives in async, cycle source, rate limiting.
- [SRD 03: Error Handling](sysref/03_error_handling.md) — error scoping, retry, silent-failure policy.
- [SRD 82: Uniform Execution Shells](sysref/82_uniform_execution_shells.md) *(DRAFT)* — the canonical model for outcomes and error handling across every execution level. Scenario graph / phase / stanza / op are one recursive **shell** (body + policy + outcome). Two-axis `Outcome` (`Disposition` × `Validity`) replaces the single `PhaseStatus`. Error handling is an `ErrorPolicy` — the per-op router plus its own resolver: a child inherits its parent's policy (depth) unless it overrides, and equal overrides share one instance by value-equality (breadth), bound once at scope-init. A `stop` carries `Interrupt` vs `Fault` so partial results are re-used or discarded deliberately. Extends SRD-03 and SRD-76. **Stop conditions are orthogonal — see SRD 83.**
- [SRD 83: Stop Conditions](sysref/83_stop_conditions.md) *(DRAFT)* — the predicate-triggered, daemon-scheduled stop-condition system, **orthogonal to** SRD-82 error handling. A stop condition is `(when: polydat predicate over runtime-state wires, trigger: a firing event or named backoff, effect: a two-axis Outcome)`. Predicates (`op_count > 50 && error_rate > 0.1`) compile in the shell's scope and read volatile state wires; evaluation is triggered, not continuous — a standard `settle` daemon fires eagerly then backs off as a shell settles. The error-rate breach becomes a default condition (`effect: fail`); "stop on error" becomes `children_failed > 0` (`effect: stop`, Fault cause). Retires the `AggregateGuard`-in-`ErrorPolicy` stopgap. Builds on SRD-82's abort + SRD-18b's per-scope kernel + SRD-11's volatile lifecycle.

### TUI / CLI / build

- [SRD 60: CLI Structure](sysref/60_cli.md), [SRD 61: Single Binary, Feature-Gated Drivers](sysref/61_single_binary.md), [SRD 62: TUI Layout](sysref/62_tui_layout.md), [SRD 64: Report CLI](sysref/64_report_cli.md) *(DRAFT)*.
- [SRD 41: Logging and Diagnostics](sysref/41_logging.md) — output routing: the console belongs to the adapter, every system signal goes through the observer/sink; level (Debug banners vs Info completion/readouts) + category (`RunLifecycle` completion vs Diagnostic readouts) determine console visibility; console-owning adapters reserve an interactive TTY; surface-by-mode (TUI panel / stderr / `session.log`).
- [SRD 81: Event-Sourced Display Projections](sysref/81_event_sourced_display.md) *(DRAFT — design)* — every display surface (terminal scrollback, TUI tree/log/panels, `session.log`, replay) is a **projection** of one typed, ordered **event stream**; the `RunState` snapshot is a **fold** of that stream. No surface consumes a pre-rendered string built for another surface — rendering always goes through the single `ReadoutSink` seam (`StringSink` for terminal/log, a new ratatui `SpanSink` for the TUI). Fixes the SRD-63 conflation where the per-phase `✓` outcome is `diag!`'d into the log plane as an ANSI string (garbles `draw_log`, pollutes `session.log`). Builds on existing machinery (`readouts::Event`, `CheckpointEvent` JSONL fold per SRD-44a, `PhaseSummary` per SRD-76, actor+ArcSwap per SRD-02). Load-bearing artifacts: the `ReadoutSink` seam + a sink-agreement property test. Pushes: (1) de-conflate + typed events ring; (2) `SpanSink` + native TUI projection; (3) persistence/replay reconciliation.

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
   *anything* touching scope, binding, iteration, Polydat kernels, or
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
