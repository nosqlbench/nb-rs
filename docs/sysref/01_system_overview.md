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
│  nbrs-workload  │  nbrs-variates        │  nbrs-metrics         │
│  YAML parsing │  GK kernel, nodes,  │  Timers, counters,  │
│  ParsedOp     │  DSL compiler,      │  HDR histograms,    │
│  tag filters  │  constant folding   │  frame capture      │
├───────────────┴─────────────────────┴─────────────────────┤
│  nbrs-rate          nbrs-errorhandler       nbrs-web  ·  nbrs-tui │
│  Token bucket     Error routing         Web UI    Term UI │
│  rate limiter     spec parser           API       status  │
└───────────────────────────────────────────────────────────┘
```

### Dependency Rules

1. Dependencies flow downward only — no reverse dependencies
2. `nbrs-variates` is fully standalone (no workload, adapter, or
   activity dependency). It can be extracted and used in other
   projects for deterministic data generation.
3. `nbrs-workload` is standalone (parses YAML to `ParsedOp`)
4. `nbrs-activity` depends on all three foundation crates
   (nbrs-workload, nbrs-variates, nbrs-metrics) and defines the
   adapter trait contract
5. Adapter crates implement `DriverAdapter` / `OpDispenser`
   from nbrs-activity
6. The `nbrs` binary is the composition root — it depends on
   every adapter (some optionally via Cargo features) and
   wires them to the activity engine

### Workspace Structure

```
nb-rs/
├── nbrs/                    single user-facing binary
├── nbrs-variates/           GK kernel and node library
├── nbrs-workload/           YAML workload parser
├── nbrs-activity/           execution engine
├── nbrs-metrics/            metrics instruments and reporters
├── nbrs-rate/               rate limiter
├── nbrs-errorhandler/       error routing
├── nbrs-web/                web UI
├── nbrs-tui/                terminal UI + TuiObserver
├── adapters/
│   ├── stdout/              text output
│   ├── http/                HTTP client
│   ├── testkit/             simulation/diagnostic
│   ├── plotter/             live terminal plots
│   ├── cql/                 CQL adapter (scylla + cassandra-cpp engines,
│   │                        common surface, workloads/, build.sh,
│   │                        Dockerfiles, sysroot/)
│   └── openapi/             OpenAPI 3.x workload synthesis
├── workloads/               shared workload examples
└── docs/
    ├── design/              SRDv1 (historical)
    ├── sysref/              SRDv2 (this reference)
    ├── memos/               numbered technical memos
    └── guide/               user-facing documentation
```

The cassandra-cpp engine isn't on crates.io and needs a system
toolchain; build it via `adapters/cql/build.sh` (Docker-based
sysroot) and link it with `cargo build -p nbrs --features
engine-cassandra-cpp`.

---

## Data Flow

```
Workload YAML ──▶ nbrs-workload ──▶ ParsedOp[]
                                      │
                                      ├──▶ nbrs-variates (compile GK bindings)
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
                    │ 3. Resolve │──▶ GK eval (per-fiber state)
                    │ 4. Execute │──▶ Adapter (CQL, HTTP, ...)
                    │ 5. Metrics │──▶ Timer, Counter
                    │ 6. Capture │──▶ GkState (ports)
                    └────────────┘
```

---

## Core Invariant

For a given `(cycle, template)` pair, the GK kernel always produces
the same field values. This makes workloads reproducible: the same
cycle number generates the same request payload regardless of
concurrency, timing, or execution order.

Protocol execution (network I/O, server state) is inherently
non-deterministic. But the input side — what we send — is fully
deterministic from the cycle input.

---

## Persona Model

The core `nbrs` binary includes lightweight universal adapters
(stdout, HTTP, model). Protocol-specific testing requires native
drivers that bring heavy dependencies (C++ libraries, system
packages). These are built as separate **persona** binaries.

`nbrs` is the single user-facing binary. Protocol drivers that
need heavy or non-portable build dependencies are gated behind
Cargo features so users compile in only what they need:

- **engine-scylla** (default) — pure-Rust ScyllaDB driver
- **engine-cassandra-cpp** (opt-in) — Apache Cassandra C++
  driver via `adapters/cql/build.sh`-built sysroot
- **all-engines** — both CQL engines, runtime-selected via
  `cqldriver=`
- **openapi** — OpenAPI 3.x workload synthesis (adds
  `describe-openapi` / `run-openapi` subcommands)

See [SRD 61](61_personas.md) for the feature-gating model and
the rationale for retiring the earlier persona-binary approach.

---

## Contract Boundaries

| Boundary | Type | Direction |
|----------|------|-----------|
| Workload → Activity | `ParsedOp` | Parsed ops, bindings, params, tags |
| Variates → Activity | `GkProgram` + `GkState` | Immutable program (includes globals) shared via Arc; per-fiber mutable state |
| Activity → Adapter | `DriverAdapter` / `OpDispenser` | Scope-init template analysis, dynamic per-cycle execution |
| Activity → Adapter | `ExecCtx` (`ResolvedFields` + `ResolvedPulls`) | Op-field bind values for the inner adapter, plus wrapper-side handle-indexed pulls (SRD 32) |
| Adapter → Activity | `OpResult` / `ExecutionError` | Result body + captures, or scoped error |
| Activity → Metrics | `ActivityMetrics` | Timers, counters, gauges |
| Metrics → Reporters | `MetricsFrame` | Immutable snapshots at capture intervals |
