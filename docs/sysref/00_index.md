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

### 2. GK Kernel (nb-variates)

| # | Document | Scope |
|---|----------|-------|
| 10 | [GK Language and Compilation](10_gk_language.md) | DSL syntax, compiler pipeline, node wiring, type system, **GK as the unified access surface for runtime state** |
| 11 | [GK Evaluation Model](11_gk_evaluation.md) | Kernel/state split, input spaces, init vs cycle, constant folding |
| 12 | [GK Standard Library](12_gk_stdlib.md) | Node catalog, type signatures, P3 JIT eligibility, runtime context nodes |
| 13 | [GK Modules and Composition](13_gk_modules.md) | Module imports, kernel composition, diagnostic event stream |
| 14 | [GK Config Expressions](14_gk_config_expressions.md) | Init-time constants flowing into activity config |
| 15 | [Strict Mode](15_strict_mode.md) | Compile-time enforcement: config wire promotion, explicit declarations, no silent coercions |
| 16 | [GK Engines](16_gk_engines.md) | Compilation levels, provenance push/pull, engine variants, auto-selection heuristic |
| 16b | [GK JIT Wiring](16_gk_jit.md) | Cranelift ↔ Rust call boundary, setjmp/longjmp for catchable predicate violations, hybrid-kernel wrapping, `invoke_with_catch` contract |

### 3. Workload Specification (nb-workload)

| # | Document | Scope |
|---|----------|-------|
| 20 | [Workload Model](20_workload_model.md) | YAML structure, ParsedOp, blocks, tags, normalization |
| 21 | [Parameters and Bind Points](21_parameters.md) | Param resolution, bind point syntax, workload/CLI/env scoping |
| 22 | [Op Sequencing](22_op_sequencing.md) | Stanza model, sequencer types, weighted ratios, cycle mapping |
| 23 | [Dynamic Controls](23_dynamic_controls.md) | Runtime-mutable per-component parameters (concurrency, rates, log level), confirmed-apply writes, enumerable declaration, reification as gauges |
| 24 | [Component Lookup](24_component_lookup.md) | Finding components by dimensional-label predicates — the selector grammar and lookup API used by dynamic controls, metrics selection, and scripted orchestration |

### 4. Execution Engine (nb-activity)

| # | Document | Scope |
|---|----------|-------|
| 30 | [Adapter Interface](30_adapter_interface.md) | DriverAdapter/OpDispenser contract, ResolvedFields, ResultBody |
| 31 | [Op Execution Pipeline](31_op_pipeline.md) | Resolve → wrap → execute → metrics flow, stanza concurrency |
| 32 | [Dispenser Wrappers](32_wrappers.md) | TraversingDispenser, ValidatingDispenser, composition order |
| 33 | [Result Validation](33_result_validation.md) | Assertions, relevancy metrics, ground truth, binding visibility |
| 34 | [Capture Points](34_capture_points.md) | Inter-op data flow, GK ports, capture extraction |

### 5. Metrics and Observability (nb-metrics)

| # | Document | Scope |
|---|----------|-------|
| 40 | [Metrics Framework](40_metrics.md) | Instruments, frames, delta semantics, reporters, scheduling |
| 41 | [Logging and Diagnostics](41_logging.md) | Conventions, GK compiler events, --explain mode |
| 42 | [Windowed Metrics Access](42_windowed_metrics.md) | User-specified cadences, auto-intermediate buckets, non-draining `now`, arbitrary past-duration queries |

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
| 61 | [Persona Model](61_personas.md) | cassnbrs, opennbrs, build structure, adapter selection |
| 62 | [TUI Layout](62_tui_layout.md) | Tree-centric layout, per-phase detail blocks, dynamic Focus LOD, 120-col baseline |

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
