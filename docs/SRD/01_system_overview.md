# 01: System Overview

nb-rs is a workload generation and testing engine. It produces
deterministic, high-throughput request streams against database and
service targets using composable data generation kernels.

---

## Crate Map

```
┌───────────────────────────────────────────────────────────┐
│                       nbrs (binary)                       │
│  Single CLI; protocol drivers gated by Cargo features     │
├───────────────────────────────────────────────────────────┤
│                     Adapter Crates                        │
│  nbrs-adapter-stdout  ·  nbrs-adapter-http                │
│  nbrs-adapter-testkit ·  nbrs-adapter-plotter             │
│  nbrs-adapter-cql  (engine-scylla / engine-cassandra-cpp) │
│  nbrs-adapter-openapi (openapi feature)                   │
├───────────────────────────────────────────────────────────┤
│                       nbrs-activity                         │
│  Activity engine: executor, op synthesis, sequencing,     │
│  validation, dispenser wrappers                           │
├───────────────┬─────────────────────┬─────────────────────┤
│  nbrs-workload  │  polydat        │  nbrs-metrics         │
│  YAML parsing │  Polydat kernel, nodes,  │  Timers, counters,  │
│  ParsedOp     │  DSL compiler,      │  HDR histograms,    │
│  tag filters  │  constant folding   │  frame capture      │
├───────────────┴─────────────────────┴─────────────────────┤
│  nbrs-rate          nbrs-errorhandler       nbrs-web  ·  nbrs-tui │
│  Token bucket     Error routing         Web UI    Term UI │
│  rate limiter     spec parser           API       status  │
└───────────────────────────────────────────────────────────┘
```

### Dependency Rules

The crate/module dependency rules — the 8-layer DAG, the per-crate Contract
Registry, polydat-standalone, no-upward-imports, no-cross-adapter edges — are
specified and **CI-enforced** in [SRD 05 — Dependency Rules](05_dependency_rules.md)
(D1–D7, gate: `nbrs/tests/architecture_rules.rs`). In one line: dependencies flow
strictly downward, `polydat` depends only on `polydat-derive`, `nbrs-activity` is the
integration hub above the foundation crates, adapters implement the
`nbrs_activity::adapter` contract, and `nbrs` is the composition root.

### Workspace Structure

```
nb-rs/
├── nbrs/                   single user-facing binary
├── polydat/               Polydat kernel and node library (standalone, extractable)
├── polydat-derive/        #[polydat_node] proc-macro
├── nbrs-workload/         YAML workload parser
├── nbrs-activity/         execution engine
├── nbrs-metrics/          metrics instruments and reporters
├── nbrs-metricsql/        MetricsQL parser / evaluator (standalone)
├── nbrs-rate/             rate limiter
├── nbrs-errorhandler/     error routing
├── nbrs-web/              web UI
├── nbrs-tui/              terminal UI + TuiObserver
├── adapters/
│   ├── stdout/            text output
│   ├── http/              HTTP client
│   ├── testkit/           simulation/diagnostic
│   ├── plotter/           live terminal plots
│   ├── cql/               CQL adapter (scylla + cassandra-cpp engines,
│   │                      common surface, workloads/, build.sh,
│   │                      Dockerfiles, sysroot/)
│   └── openapi/           OpenAPI 3.x workload synthesis
├── workloads/             shared workload examples
├── polydat/docs/          polydat's own substrate design (axiom + mechanism)
└── docs/
    ├── SRD/               the nb-rs system reference (this doc set)
    │   ├── notes/         living design rationale (discursive, Pillar-3 mechanism)
    │   └── history/       superseded notes + shipped implementation-plans (archive)
    └── guide/             user-facing documentation
```

The polydat substrate design lives in `polydat/docs/` because polydat is an
independently extractable crate; `docs/SRD` carries only the nbrs-side
integration plus the polydat contract surface. See
[SRD 05 §Dependency Rules](05_dependency_rules.md) and the
[Subsystem Treatment Standard](00b_subsystem_standard.md).

The cassandra-cpp engine isn't on crates.io and needs a system
toolchain; build it via `adapters/cql/build.sh` (Docker-based
sysroot) and link it with `cargo build -p nbrs --features
engine-cassandra-cpp`.

---

## Data Flow

```
Workload YAML ──▶ nbrs-workload ──▶ ParsedOp[]
                                      │
                                      ├──▶ polydat (compile Polydat bindings)
                                      │        │
                                      │        ▼
                                      │    GkProgram (immutable, shared Arc)
                                      │        │
CLI params ─────────────────────────┐ │        │
                                    ▼ ▼        ▼
                                nbrs-activity
                              ┌────────────────────┐
                              │  Activity           │
                              │  ├── OpSequence     │
                              │  ├── CycleSource    │
                              │  ├── Dispensers[]    │
                              │  ├── Metrics        │
                              │  └── ErrorRouter    │
                              └────────┬───────────┘
                                       │
                          ┌────────────┼────────────┐
                          ▼            ▼            ▼
                       Fiber 0     Fiber 1     Fiber N
                       (tokio)     (tokio)     (tokio)
                          │
                    ┌─────┴──────┐
                    │ Per cycle: │
                    │ 1. Rate    │
                    │ 2. Select  │
                    │ 3. Resolve │──▶ Polydat eval (per-fiber state)
                    │ 4. Execute │──▶ Adapter (CQL, HTTP, ...)
                    │ 5. Metrics │──▶ Timer, Counter
                    │ 6. Capture │──▶ GkState (ports)
                    └────────────┘
```

---

## Core Invariant

For a given `(cycle, template)` pair, the Polydat kernel always produces
the same field values. This makes workloads reproducible: the same
cycle number generates the same request payload regardless of
concurrency, timing, or execution order.

Protocol execution (network I/O, server state) is inherently
non-deterministic. But the input side — what we send — is fully
deterministic from the cycle input.

---

## Single Binary, Feature-Gated Drivers

`nbrs` is the single user-facing binary. Lightweight universal
adapters (stdout, HTTP, model) are always linked in. Protocol
drivers that need heavy or non-portable build dependencies are
gated behind Cargo features so users compile in only what they
need:

- **engine-scylla** (default) — pure-Rust ScyllaDB driver
- **engine-cassandra-cpp** (opt-in) — Apache Cassandra C++
  driver via `adapters/cql/build.sh`-built sysroot
- **all-engines** — both CQL engines, runtime-selected via
  `cqldriver=`
- **openapi** — OpenAPI 3.x workload synthesis (adds
  `describe-openapi` / `run-openapi` subcommands)

See [SRD 61](61_single_binary.md) for the full feature-gating
model and adapter selection.

---

## Contract Boundaries

| Boundary | Type | Direction |
|----------|------|-----------|
| Workload → Activity | `ParsedOp` | Parsed ops, bindings, params, tags |
| Variates → Activity | `PolydatProgram` + `PolydatState` | Immutable program (includes globals) shared via Arc; per-fiber mutable state |
| Activity → Adapter | `DriverAdapter` / `OpDispenser` | Scope-init template analysis, dynamic per-cycle execution |
| Activity → Adapter | `ExecCtx` (`ResolvedFields` + `ResolvedPulls`) | Op-field bind values for the inner adapter, plus wrapper-side handle-indexed pulls (SRD 32) |
| Adapter → Activity | `OpResult` / `ExecutionError` | Result body + captures, or scoped error |
| Activity → Metrics | `ActivityMetrics` | Timers, counters, gauges |
| Metrics → Reporters | `MetricsFrame` | Immutable snapshots at capture intervals |
