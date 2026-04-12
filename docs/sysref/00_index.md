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
| 10 | [GK Language and Compilation](10_gk_language.md) | DSL syntax, compiler pipeline, node wiring, type system |
| 11 | [GK Evaluation Model](11_gk_evaluation.md) | Kernel/state split, input spaces, init vs cycle, constant folding |
| 12 | [GK Standard Library](12_gk_stdlib.md) | Node catalog, type signatures, P3 JIT eligibility |
| 13 | [GK Modules and Composition](13_gk_modules.md) | Module imports, kernel composition, diagnostic event stream |
| 14 | [GK Config Expressions](14_gk_config_expressions.md) | Init-time constants flowing into activity config |
| 15 | [Strict Mode](15_strict_mode.md) | Compile-time enforcement: config wire promotion, explicit declarations, no silent coercions |
| 16 | [GK Engines](16_gk_engines.md) | Compilation levels, provenance push/pull, engine variants, auto-selection heuristic |

### 3. Workload Specification (nb-workload)

| # | Document | Scope |
|---|----------|-------|
| 20 | [Workload Model](20_workload_model.md) | YAML structure, ParsedOp, blocks, tags, normalization |
| 21 | [Parameters and Bind Points](21_parameters.md) | Param resolution, bind point syntax, workload/CLI/env scoping |
| 22 | [Op Sequencing](22_op_sequencing.md) | Stanza model, sequencer types, weighted ratios, cycle mapping |

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

## Known Tensions to Resolve

These are design tensions identified across the v1 SRDs that the
v2 documents should address explicitly:

1. **Binding visibility scope** — Op fields vs params vs validation
   needs. Who declares which GK outputs get compiled? Currently:
   binding compiler scans both op fields and params. The v2 should
   define a clear principle for what triggers GK output inclusion.

2. **Activity config from GK constants** — SRD 48 proposes `{name}`
   syntax in params for GK constant references. Open question:
   should there be a qualifier (`{gk:name}`) to disambiguate from
   workload params? The v2 should commit to one resolution model.

3. **Per-phase concurrency** — Workload-level `concurrency: "100"`
   applies to all phases. Schema DDL needs `concurrency=1`. Current
   workaround: CLI override. The v2 should define block-level
   activity config override semantics.

4. **Default cycles from data** — Vector workloads want
   `cycles=train_count` but this requires GK constant → config
   flow (SRD 48, not yet implemented). The v2 should specify the
   resolution chain clearly.

5. **Statement mode dispatch** — CQL adapter uses op field names
   (`raw:`, `prepared:`, `stmt:`) for dispatch. This is
   adapter-specific, not in core. But the workload parser's
   `activity_params` list must know about `relevancy:`, `verify:`,
   `strict:` to route them to params instead of op fields. The
   boundary between "adapter concern" and "core concern" for op
   field routing needs a clear principle.

6. **Input declaration redundancy** — `inputs := (cycle)`
   in every GK source vs implicit single-input default. The
   v2 should decide: always explicit, or default to `(cycle)`.

7. **Result extraction model** — SRD 47 uses JSON fallback for
   all adapters. Native downcast via `as_any()` is available but
   not used in the validation path (JSON with `json_field_as_i64`
   coercion suffices). Should the v2 commit to JSON-only extraction,
   or define when native downcast is warranted?
