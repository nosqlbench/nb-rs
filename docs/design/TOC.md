# SRD Table of Contents

## Foundation

| # | File | Scope |
|---|------|-------|
| 00 | [assay_summary](00_assay_summary.md) | Initial survey of the Java nosqlbench codebase to inform nb-rs design |
| 01 | [subsystem_scope](01_subsystem_scope.md) | Priority subsystems for nb-rs and elaboration plan |
| 18 | [project_status](18_project_status.md) | Implementation status snapshot of nb-rs |

Establishes the project's origin, scope, and current state. SRD 00
catalogs what exists in Java nosqlbench. SRD 01 identifies the three
priority subsystems (variate generation, metrics, workload spec).
SRD 18 tracks what has been implemented.

## Generation Kernel (GK)

| # | File | Scope |
|---|------|-------|
| 02 | [variate_generation](02_variate_generation.md) | Generation kernel (GK) architecture: DAG-based deterministic data generation |
| 03 | [virtdata_function_catalog](03_virtdata_function_catalog.md) | Inventory of 526 Java virtdata @ThreadSafeMapper functions |
| 04 | [gk_function_library](04_gk_function_library.md) | Standard function library for GK, derived from virtdata assessment |
| 07 | [gk_rust_model](07_gk_rust_model.md) | Mapping of GK design to Rust types, traits, and runtime structures |
| 08 | [binding_recipe_catalog](08_binding_recipe_catalog.md) | Catalog of 838 binding expressions across 107 Java workload files |

Defines the GK as a pure-function DAG that transforms input
coordinates into named output variates. SRD 02 is the core
architecture. SRDs 03/04/08 scope the function library by surveying
what Java nosqlbench uses in practice. SRD 07 maps the design to
concrete Rust types (`GkNode` trait, `Value` enum, `NodeMeta`,
`GkAssembler`, `GkKernel`).

## GK Language

| # | File | Scope |
|---|------|-------|
| 05 | [gk_dsl](05_gk_dsl.md) | Early design exploration of GK DSL syntax (superseded by SRD 14) |
| 06 | [gk_language_layers](06_gk_language_layers.md) | Surface DSL to kernel IR normalization pipeline and sugar rules |
| 12 | [init_vs_cycle_state](12_init_vs_cycle_state.md) | Init-time vs cycle-time state separation and `init` keyword |
| 13 | [init_objects](13_init_objects.md) | Non-scalar init objects: types, lifecycle, and internal mechanics |
| 14 | [unified_dsl_syntax](14_unified_dsl_syntax.md) | Consolidated GK DSL syntax reference (final draft) |

Defines the `.gk` source language. SRD 14 is the canonical syntax
reference. SRDs 05/06 are earlier explorations that led to it.
SRDs 12/13 define the init/cycle split — `init` keyword for
assembly-time constants (LUTs, tables) vs `:=` for cycle-time
bindings. Together these enable the two-phase compilation model
where init objects are frozen before any cycle executes.

## GK Compilation

| # | File | Scope |
|---|------|-------|
| 10 | [aot_compilation](10_aot_compilation.md) | Ahead-of-time compiled kernel path and trade-offs |
| 24 | [compilation_levels](24_compilation_levels.md) | P1/P2/P3/Hybrid compilation, shared buffer, thread scalability |

Defines four compilation levels for the GK runtime. Phase 1 is a
pull-through interpreter (~70ns/node). Phase 2 compiles to u64
closures (~4.5ns/node). Phase 3 generates Cranelift JIT native
code (~0.2ns/node). Hybrid mode lets each node run at its optimal
level in the same kernel. SRD 24 also covers the thread scalability
model: shared immutable code via Arc, per-thread mutable buffers,
zero-contention evaluation.

## GK Extensions

| # | File | Scope |
|---|------|-------|
| 25 | [pcg_rng](25_pcg_rng.md) | PCG-RXS-M-XS RNG nodes: seekable, stream-independent, pure-function |
| 26 | [coordinate_spaces](26_coordinate_spaces.md) | Coordinate decomposition from cycle via mixed_radix, inference rules |
| 27 | [gk_modules](27_gk_modules.md) | Reusable .gk modules with inferred interfaces, resolution, strict mode |

Extends the GK with higher-level capabilities. SRD 25 adds PCG
random number generation with pure-function seek (no shared state
between threads). SRD 26 defines how a flat cycle counter decomposes
into multi-dimensional coordinate spaces, with automatic inference
of coordinate inputs from unbound wire references. SRD 27 adds a
module system — `.gk` files that are automatically loaded when
referenced as unknown functions, with interfaces inferred from
wire connectivity. Also defines `--strict` mode, dead code
elimination, and the `result` field for model adapter integration.

## Sampling and Distributions

| # | File | Scope |
|---|------|-------|
| 09 | [alias_method](09_alias_method.md) | O(1) weighted sampling via Vose's alias method |
| 11 | [icd_sampling](11_icd_sampling.md) | Inverse CDF sampling from continuous and discrete distributions |

Defines the two main sampling strategies. Alias method gives O(1)
weighted selection from discrete probability tables (built at init,
queried at cycle time). Inverse CDF uses precomputed LUTs for
continuous distributions (normal, exponential, Pareto, etc.) with
O(1) interpolating lookup. Both are init-time builds + cycle-time
pure-function queries, fitting cleanly into the GK model.

## Metrics

| # | File | Scope |
|---|------|-------|
| 15 | [metrics_assay](15_metrics_assay.md) | Study of Java nosqlbench metrics to inform nb-rs design |
| 16 | [metrics_design](16_metrics_design.md) | Frame-based metrics capture with delta HDR histograms and coalescing |
| 20 | [component_tree](20_component_tree.md) | Component hierarchy for consistent dimensional metric labeling |

Defines the metrics collection and reporting layer. Frame-based
capture produces immutable snapshots (delta HDR histograms,
counters) that can be coalesced hierarchically for multi-cadence
reporters. SRD 20 defines how component hierarchy drives
dimensional labels (session → activity → op), ensuring metrics
are consistently tagged without per-instrument configuration.

## Workload Specification

| # | File | Scope |
|---|------|-------|
| 17 | [workload_spec_assay](17_workload_spec_assay.md) | Analysis of nosqlbench YAML workload specification format |
| 22 | [op_sequencing](22_op_sequencing.md) | Stanza-based op sequencing: bucket, interval, concat strategies |
| 23 | [op_synthesis_diagnostics](23_op_synthesis_diagnostics.md) | Op assembly pipeline, dry-run mode, and diagnostic tooling |
| 35 | [inline_workload](35_inline_workload.md) | Inline `op=` workload synthesis from command line without YAML |

Defines how YAML workload files are parsed, normalized, and
translated into runtime operations. SRD 22 defines stanza-based
sequencing — ops have ratios, the sequencer builds a stanza
pattern, and the executor repeats it. Three strategies (bucket,
interval, concat) control op ordering within a stanza. SRD 23
covers the op synthesis pipeline: GK evaluation → bind point
substitution → assembled op, plus dry-run and diagnostic modes.
SRD 35 adds inline workload synthesis — `op=` on the command
line synthesizes a `Workload` struct without a YAML file, with
`{{expr}}` inline bindings compiled to GK outputs.

## Execution Engine

| # | File | Scope |
|---|------|-------|
| 19 | [rate_limiter](19_rate_limiter.md) | Token-bucket rate limiter with burst recovery, async-ready |
| 21 | [execution_layer](21_execution_layer.md) | Async activity engine composing workloads, variates, metrics, rate limiting |

Defines the runtime execution model. The activity is the unit of
concurrent execution — it owns a cycle source, op sequence, GK
kernel, metrics, rate limiters, and error router. Executor tasks
(tokio async) pull cycles, evaluate the GK kernel, assemble ops,
and execute them through the adapter. Dual rate limiting (per-cycle
and per-stanza) with token-bucket semantics and burst recovery.

## Inter-Op Data Flow

| # | File | Scope |
|---|------|-------|
| 28 | [capture_points](28_capture_points.md) | Inter-op data flow, stanza-scoped captures, qualified bind points, serialization |
| 29 | [model_adapter](29_model_adapter.md) | Simulated op execution with configurable results, latency, error injection |
| 30 | [stdlib](30_stdlib.md) | GK standard library: embedded modules, resolution chain, stdlib contents |
| 31 | [node_factories](31_node_factories.md) | External node providers: NodeFactory trait, unified registry, fiber safety |
| 32 | [web_ui](32_web_ui.md) | Axum + htmx web dashboard: live metrics, function browser, DAG viewer |
| 33 | [op_pipeline](33_op_pipeline.md) | Op decorator stack: dry-run, capture, assert, print, metrics middleware |

Defines how data flows between operations within a stanza and how
to prototype without real infrastructure. SRD 28 introduces capture
points (`[name]` syntax), external input ports (volatile/sticky with
defaults), qualified bind points (`{coord:name}`, `{capture:name}`,
`{bind:name}`), and the stanza-scoped capture context. The GK stays
pure — captures are an external input source, not a graph mutation.
SRD 29 defines the model adapter: the `result` op field provides
simulated results (static map or GK kernel), `result-*` fields for
latency/error injection (static or GK-driven), `{{...}}` anonymous
inline bindings, and probability modeling nodes (fair_coin, select,
one_of, etc.). SRD 30 defines the standard library — `.gk` module
files embedded in the binary, resolved after workload-local and user
library, providing reusable patterns (latency models, identity
generators, time series helpers, service modeling). SRD 31 defines
node factories — external crates implement `NodeFactory` to provide
GK nodes that are registered into the unified registry at startup,
indistinguishable from built-in nodes in describe output, category
grouping, type checking, and compilation levels.
