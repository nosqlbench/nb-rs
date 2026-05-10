# nb-rs System Reference (SRDv2)

Consolidated system reference for nb-rs. Replaces the layered SRD
documents in `docs/design/` which remain as historical record.

This reference is organized by subsystem, not by chronological
discovery order. Each document is self-contained and
cross-referenced. The source of truth is the code; this reference
explains the design intent behind it.

---

## Documents

### 1. System Architecture

| # | Document | Scope |
|---|----------|-------|
| 01 | [System Overview](01_system_overview.md) | Crate map, data flow, persona model, build structure |
| 02 | [Concurrency Model](02_concurrency_model.md) | Async fibers, tokio runtime, cycle source, rate limiting |
| 03 | [Error Handling](03_error_handling.md) | Error scoping, routing, retry semantics, silent failure policy |

### 2. GK Kernel (nbrs-variates)

| # | Document | Scope |
|---|----------|-------|
| 10 | [GK Language and Compilation](10_gk_language.md) | DSL syntax, compiler pipeline, node wiring, type system, op-level bindings, cursor declarations, **GK as the unified access surface for runtime state** |
| 11 | [GK Evaluation Model](11_gk_evaluation.md) | Kernel/state split, input spaces, three lifecycles (compile-const / scope-init / dynamic), provenance-based invalidation, init-binding contract |
| 12 | [GK Standard Library](12_gk_stdlib.md) | Node catalog, type signatures, P3 JIT eligibility, runtime context nodes |
| 13 | [GK Modules](13_gk_modules.md) | File-based modules, inlining resolution, compiler diagnostic event stream |
| 13b | [GK Combination Modes](13b_gk_combination_modes.md) | Four-mode taxonomy: inline, scope composition, subgraph, reification |
| 13c | [GK Scope Model](13c_gk_scope_model.md) | Scope hierarchy, visibility / mutability rules, `for_each` lifecycle, auto-extern composition, the scope-composition mode in depth |
| 13d | [Op-template GK Scope Layer](13d_op_template_scope.md) *(SKETCH — not yet implemented)* | Op templates as GK scopes; **`HasGkMatter` trait** declarative classification (None / Readonly / Definitions) every workload-AST node implements; **scope flattening** (trait short-circuit + program-hash equivalence) collapses trivial scopes into the parent; **subcontext symbol redefinition forbidden by default**; staged **realisation lifecycle** (Source → AST → resolvable AST → program → hash check → instance) with two cache layers and three short-circuits; **walking parent-kernel reference** (`nearest_materialised`) and **logical kernel names** for diagnostics; `dryrun=op` diagnostic level + `nbrs describe gk` integration; dedicated proving-out test suite. Prerequisite for SRD-40b |
| 13e | [Scope-as-Module Refinement](13e_scope_as_module.md) *(DESIGN — not yet implemented)* | Promotes every sub-scope (phase, op-template, comprehension, do-loop) to a formal `ScopeModule` with typed import / export contracts; `ScopeModule::instance_under(parent)` is the single typed attach operation that replaces ad-hoc string-concat synthesis + `bind_outer_scope` + manual scope-value reapply; `ScopeContract` issues kernel-bound `ImportHandle<M>` / `ExportHandle<M>` so cross-kernel mis-routes are ill-typed at compile; refinement of 13c/13d. Migration plan in §5; absorbs the SRD-13d Phase 9 followup architectural fixes (Coordinate skip, workload-param precedence, owning-phase resolution, post-bind init pull) into automatic consequences of the typed contract. |
| 14 | [GK Config Expressions](14_gk_config_expressions.md) | `{...}` form: init-time constants flowing into activity config |
| 15 | [Strict Mode](15_strict_mode.md) | Compile-time enforcement: config wire promotion, explicit declarations, no silent coercions |
| 16 | [GK Engines](16_gk_engines.md) | Compilation levels P1/P2/P3, provenance push/pull, engine variants, auto-selection heuristic |
| 16b | [GK JIT Wiring](16b_gk_jit.md) | Cranelift ↔ Rust call boundary, setjmp/longjmp for catchable predicate violations, hybrid-kernel wrapping, `invoke_with_catch` contract |

### 3. Workload Specification (nbrs-workload)

| # | Document | Scope |
|---|----------|-------|
| 18 | [Control Flow](18_control_flow.md) | Scenario tree, scopes, iteration shapes, loop counters |
| 18b | [Scenario Tree and Scheduler](18b_scenario_tree_and_scheduler.md) | Two-tree model, comprehension AST, kernel composition, find-by-comprehension lookup |
| 18c | [Comprehension Syntax](18c_comprehension_syntax.md) | Layered grammar — literal lists, ranges, generators, `where` filter, SI suffixes, tuple LHS, sequencer-style LUT expansions |
| 18d | [Comprehension Traversal Order](18d_comprehension_traversal_order.md) | Tuple emission order — lex, diagonal, extrema-first, concentric shells, low-discrepancy (Halton/Sobol/LHS), custom |
| 18e | [Comprehension Canonical Reference](18e_comprehension_canonical_reference.md) | The contract: full AST, mode detection, coordinate-set, filter+order pipeline, index-space contract, Union+ordering rule, `where` semantics, Layer 7 extension path, per-strategy implementation status |
| 20 | [Workload Model](20_workload_model.md) | YAML structure, ParsedOp, blocks, tags, normalization |
| 21 | [Parameters and Bind Points](21_parameters.md) | Param resolution, bind point syntax, workload/CLI/env scoping |
| 22 | [Op Sequencing](22_op_sequencing.md) | Stanza model, sequencer types, weighted ratios, cycle mapping |
| 23 | [Dynamic Controls](23_dynamic_controls.md) | Runtime-mutable per-component parameters (concurrency, rates, log level), confirmed-apply writes, enumerable declaration, reification as gauges |
| 24 | [Component Lookup](24_component_lookup.md) | Finding components by dimensional-label predicates — the selector grammar and lookup API used by dynamic controls, metrics selection, and scripted orchestration |

### 4. Execution Engine (nbrs-activity)

| # | Document | Scope |
|---|----------|-------|
| 30 | [Adapter Interface](30_adapter_interface.md) | DriverAdapter/OpDispenser contract, ResolvedFields, ResultBody |
| 31 | [Op Execution Pipeline](31_op_pipeline.md) | Resolve → wrap → execute → metrics flow, stanza concurrency |
| 32 | [Dispenser Wrappers](32_wrappers.md) | TraversingDispenser, ValidatingDispenser, composition order |
| 32a | [Op Wrapper Registry, Field Ownership, and Stacking Order](32a_wrapper_registry.md) *(DESIGN — not yet implemented)* | Refines SRD-32: every wrapper has a stable name, exclusive owned op-template fields, numeric stack rank, trigger predicate; default order matches today's hand-rolled cascade; workload-level (`wrappers: { order: [...] }`) and CLI (`--wrap-order`) overrides; parse-time validation of field ownership (catches misplaced `poll_interval_ms` etc.); compatibility constraints (`metrics` outermost, `traverse` innermost, `if` outside `poll` by default); `nbrs describe wrappers` / `nbrs describe op` for discoverability |
| 33 | [Result Validation](33_result_validation.md) | Assertions, relevancy metrics, ground truth, binding visibility |
| 34 | [Capture Points](34_capture_points.md) | Inter-op data flow, GK ports, capture extraction |
| 35 | [Driver Resource Lifecycle and Sharing](35_driver_resources.md) *(DESIGN — Push A/B implemented)* | Two-layer split (shell vs instance); `ResourceKey` value-equality identity; instance-shaping vs shell-shaping param partition; `ShareCapability` (driver-declared, planning-time) + `ResourceSharePolicy` (user-elevatable); paired live-instance trait methods `can_share()` (capability: thread-safe + designed for sharing) and `can_support_more_load()` (live capacity: can the instance take another caller right now? `true` = yes, route here; `false` = saturated, spawn a sibling) — driver decides the criterion, no canonical shape imposed; pool-level guard catches `quiescent-decline` driver bugs (saturation reported at zero load); pre-map-driven multi-generation refcount lifecycle, explicit async `close()` with bounded teardown, debug `resource.{attach,init,share.spawn,detach,close}` event surface with stable `generation` field; CQL adapter is the prototype consumer |
| 68 | [Dispenser-Owned GK Context and Single-Surface Resolution](68_dispenser_owned_gk_context.md) *(DESIGN — Push 1+ in flight)* | Dispenser owns its canonical GK kernel; one resolution surface per dispenser; narrow `WireSource` trait walls adapter code off from kernel internals; `map_op` takes a `SubcontextBuilder` and uses SRD-67 two-phase materialisation; per-fiber kernel fan-out via `build_subscope` from canonical kernels (no `op_template_kernels` LUT); workload-load pre-flight is non-mutating; CQL prepared compilation is dispenser-init-time work using the canonical kernel via `WireSource`; collapses several existing parallel structures (`OpBuilder::op_template_kernels`/`op_template_programs`, `synthesis::substitute_bind_points*`, `resolve_placeholders_via_kernel`'s mutation half) into the standard subcontext mechanism |

### 5. Metrics and Observability (nbrs-metrics)

| # | Document | Scope |
|---|----------|-------|
| 40 | [Metrics Framework](40_metrics.md) | Instruments, frames, delta semantics, reporters, scheduling |
| 41 | [Logging and Diagnostics](41_logging.md) | Conventions, GK compiler events, --explain mode |
| 42 | [Windowed Metrics Access](42_windowed_metrics.md) | User-specified cadences, auto-intermediate buckets, non-draining `now`, arbitrary past-duration queries |
| 44 | [Workload Checkpointing](44_workload_checkpointing.md) | Phase-boundary + cursor-state resume, per-phase identity hashing, durability ordering, invocation-agnostic error handling |
| 44a | [Checkpoint Persistence: JSONL Event Log](44a_checkpoint_jsonl.md) *(DESIGN — not yet implemented)* | Refines SRD-44 storage: append-only JSONL replaces whole-document JSON rewrites; typed-event surface (`type` discriminator) covers `session_start` / `session_end` / `phase_declared` / `scope_enter` / `scope_exit` / `phase_started` / `phase_progress` / `phase_completed` / `phase_failed`; resume planner folds the stream; truncated-tail crash recovery; future-additive event types (metric_sample / error_record / control_change) |
| 45 | [Sessions](45_sessions.md) | Session id + directory resolution, `SESSION_DIRECTORY` env / `--session-dir`, reuse policy (`error`/`restart`/`resume`), lifecycle cleanup (`--sessions-max`, `--sessions-shelflife`), resume hint on exit |
| 47 | [MetricsQL Streaming Aggregation](47_metricsql_streaming.md) | `Reducer` algebra (distributive / algebraic / holistic), `StreamingPlan` compiler, ingest + snapshot data path, equivalence property test, holistic-function and sliding-window deferred decisions |
| 48 | [MetricsQL Continuous-Query Runtime](48_metricsql_continuous_query.md) | Plan registry, sample feed model (pull / push / watchable), actor + ArcSwap concurrency, lifecycle (register / tick / reset / unregister), window framing policy (tumbling / grid), TUI / web binding model, memory bounds |

### 6. Adapters

| # | Document | Scope |
|---|----------|-------|
| 50 | [CQL Adapter](50_cql_adapter.md) | Statement modes, CqlResultBody, prepared/raw dispatch, vector workloads |
| 51 | [HTTP Adapter](51_http_adapter.md) | Request templates, method/URL/body mapping |
| 52 | [Stdout and Model Adapters](52_stdout_model.md) | Format modes, field rendering, diagnostic output |
| 53 | [Vector Data Integration](53_vectordata.md) | Dataset nodes, catalog resolution, caching, metadata/predicates |

### 7. CLI and Personas

| # | Document | Scope |
|---|----------|-------|
| 60 | [CLI Structure](60_cli.md) | Command tree, completions, workload discovery, bench command |
| 61 | [Single Binary, Feature-Gated Drivers](61_personas.md) | nbrs binary, Cargo features, adapter selection, future drivers |
| 62 | [TUI Layout](62_tui_layout.md) | Tree-centric layout, per-phase detail blocks, dynamic Focus LOD, 120-col baseline |
| 63 | [Status Readout Templates](63_status_readouts.md) *(DRAFT)* | Component-based template engine for status / summary lines, pre-baked render-step lists, compactness levels, layout ↔ content separation |
| 64 | [Report CLI](64_report_cli.md) *(DRAFT)* | `nbrs report` command family, dynamic completion with full SRD-46 grammar parity, scratch-rendering against active session, `--add`/`--contextual`/`--replace` promotion to workload YAML |
| 49 | [MetricsQL Supported Scope](49_metricsql_supported_scope.md) | Canonical reference for what nb-rs supports as MetricsQL: parser corpus, evaluator dispatch, and tooling registry — with drift-detection tests pinning the link. Covers the [`MetricCatalog`] trait + OpenMetrics types + supported aggregate / rollup / binary-op surface |
| 40a | [Metrics Data Model](40a_metrics_model.md) | Consolidated mechanical reference: entity-relationship model + types + naming + ABNF + lookup conventions across `nbrs-metrics::snapshot`, OpenMetrics 1.0, MetricsQL selectors, and the SQLite schema. Pins identity rules, value-type representations, label/name character grammars, and round-trip invariants. Includes a §8 gap audit against the model |
| 40b | [Synthetic Metrics from GK](40b_synthetic_metrics_from_gk.md) *(SKETCH — not yet implemented; depends on SRD-13d)* | Cross-cutting mechanism for op templates declaring `metrics:` — formula-driven synthetic metric families published per-cycle through the standard metrics pipeline. Schema (full mapping + bare-string + list-with-wire-expression sugar). Value is always GK; **`result:` declaration** exposes capture / return-body fields as GK named wires so result-derived metrics share the same one-path evaluation. Dispenser-as-component (with `op` label) owns the instrument; duplicate `(family, label-cell)` collisions surface against the component's instrument set at init. `format:` is a generation-time numeric sanitiser (Excel hash patterns). `unit:` flows into both the family-name suffix and the `metric_family.unit` column. **`scope_close` cadence-streamer flush signal** (§11) so short-phase metrics never get lost between cadence pulses — generic, applies to every component. Op-template GK scope layering + flattening live in SRD-13d. Workload-specific demo plan: [`docs/design/synthetic_metrics_cql_vector_demo.md`](../design/synthetic_metrics_cql_vector_demo.md) |

---

## SRDv1 → SRDv2 Mapping

| SRDv2 | Source SRDs (v1) |
|-------|-----------------|
| 01 System Overview | 01, 20, 37, 39 |
| 02 Concurrency | 21, 40 (concurrency section) |
| 03 Error Handling | 41 |
| 10 GK Language | 05, 06, 07, 14, 24 |
| 11 GK Evaluation | 02, 10, 12, 13, 26, 44 |
| 12 GK Stdlib | 03, 04, 08, 09, 11, 25, 30 |
| 13 GK Modules | 27, 36, 44, 45 |
| 14 GK Config | 48 |
| 20 Workload Model | 17, 22, 35 |
| 21 Parameters | 42 |
| 22 Op Sequencing | 22 |
| 30 Adapter Interface | 38 |
| 31 Op Pipeline | 33, 40 |
| 32 Wrappers | 33, 34, 47 (wrapper section) |
| 33 Result Validation | 47 |
| 34 Capture Points | 28 |
| 40 Metrics | 15, 16 |
| 41 Logging | 43, 45 |
| 50 CQL Adapter | 46, 50 (from code) |
| 51 HTTP Adapter | (from code) |
| 52 Stdout/Model | 29 |
| 53 Vectordata | 46, (vectordata nodes) |
| 60 CLI | 23, 32, 35 |
| 61 Personas | 37 |

## Known Tensions — resolved

The v1 SRDs flagged seven cross-cutting tensions that v2 had
to answer. All seven have now been folded into the relevant
SRD sections. The pointers below exist so anyone arriving
from a v1 reference can jump to the current authoritative
home of each decision.

| # | v1 tension | Current home |
|---|------------|--------------|
| 1 | Binding visibility scope | SRD 10 §"GK as the unified access surface" (GK owns all runtime-value resolution; no separate "extras" pathway). |
| 2 | `{gk:name}` qualifier for GK constants | SRD 10 §"GK as the unified access surface" (subsumed: every name resolves through the GK graph, no separate qualifier needed). |
| 3 | Per-phase config override | SRD 21 §"Parameter Resolution" (block-level `params:`, closest-wins; GK helpers `this_or` / `required` / predicates for explicit layering). |
| 4 | `cycles=train_count` resolution | SRD 10 §"GK as the unified access surface" + SRD 21 §"Explicit layering with GK helpers". `cycles` is not special; cursors are arbitrary names; `train_count` is a GK-folded constant reified into the local or workload scope. |
| 5 | Adapter vs core op-field boundary | SRD 30 §"Core-first field processing" (core consumes its fields first; adapters see only their own fields; unknown fields are errors). |
| 6 | `inputs := (cycle)` boilerplate | SRD 10 §"Input Declaration" (inputs inferred when the declaration is omitted; `cycle` is not a magic identifier). |
| 7 | Result extraction model | SRD 33 §"Result Extraction" (universal JSON access + typed accessors / traversers as an opt-in hot-path optimization). |

For outstanding unresolved items, see
[`99_open_tensions_memo.md`](99_open_tensions_memo.md) (kept
for historical reference; empty as of its resolution).
