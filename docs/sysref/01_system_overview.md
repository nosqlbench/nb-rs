# 01: System Overview

nb-rs is a workload generation and testing engine. It produces
deterministic, high-throughput request streams against database and
service targets using composable data generation kernels.

---

## Crate Map

```
┌───────────────────────────────────────────────────────────┐
│                     Persona Binaries                      │
│  cassnbrs (CQL)  ·  opennbrs (OpenSearch)  ·  nbrs (core)│
├───────────────────────────────────────────────────────────┤
│                     Adapter Crates                        │
│  nb-adapter-stdout  ·  nb-adapter-http  ·  nb-adapter-model│
│  cassnbrs-adapter-cql (external, static-linked)           │
├───────────────────────────────────────────────────────────┤
│                       nb-activity                         │
│  Activity engine: executor, op synthesis, sequencing,     │
│  validation, dispenser wrappers                           │
├───────────────┬─────────────────────┬─────────────────────┤
│  nb-workload  │  nb-variates        │  nb-metrics         │
│  YAML parsing │  GK kernel, nodes,  │  Timers, counters,  │
│  ParsedOp     │  DSL compiler,      │  HDR histograms,    │
│  tag filters  │  constant folding   │  frame capture      │
├───────────────┴─────────────────────┴─────────────────────┤
│  nb-rate          nb-errorhandler       nb-web  ·  nb-tui │
│  Token bucket     Error routing         Web UI    Term UI │
│  rate limiter     spec parser           API       status  │
└───────────────────────────────────────────────────────────┘
```

### Dependency Rules

1. Dependencies flow downward only — no reverse dependencies
2. `nb-variates` is fully standalone (no workload, adapter, or
   activity dependency). It can be extracted and used in other
   projects for deterministic data generation.
3. `nb-workload` is standalone (parses YAML to `ParsedOp`)
4. `nb-activity` depends on all three foundation crates
   (nb-workload, nb-variates, nb-metrics) and defines the
   adapter trait contract
5. Adapter crates implement `DriverAdapter` / `OpDispenser`
   from nb-activity
6. Persona binaries are composition roots — they depend on
   everything and wire adapters to the activity engine

### Workspace Structure

```
nb-rs/
├── nb-rs/                   main binary (stdout, http, model)
├── nb-variates/             GK kernel and node library
├── nb-workload/             YAML workload parser
├── nb-activity/             execution engine
├── nb-metrics/              metrics instruments and reporters
├── nb-rate/                 rate limiter
├── nb-errorhandler/         error routing
├── nb-web/                  web UI (optional)
├── nb-tui/                  terminal UI (optional)
├── adapters/
│   ├── nb-adapter-stdout/   text output
│   ├── nb-adapter-http/     HTTP client
│   └── nb-adapter-model/    simulation/diagnostic
├── personas/
│   ├── cassnbrs/            Cassandra persona (excluded from workspace)
│   │   └── adapter/         CQL adapter crate (cassandra-cpp)
│   └── opennbrs/            OpenSearch persona
├── workloads/               shared workload examples
└── docs/
    ├── design/              SRDv1 (historical)
    ├── sysref/              SRDv2 (this reference)
    ├── memos/               numbered technical memos
    └── guide/               user-facing documentation
```

`cassnbrs` is excluded from the main workspace because it requires
the Apache Cassandra C++ driver. It builds separately via
`personas/cassnbrs/build.sh` which manages the driver sysroot.

---

## Data Flow

```
Workload YAML ──▶ nb-workload ──▶ ParsedOp[]
                                      │
                                      ├──▶ nb-variates (compile GK bindings)
                                      │        │
                                      │        ▼
                                      │    GkProgram (immutable, shared Arc)
                                      │        │
CLI params ─────────────────────────┐ │        │
                                    ▼ ▼        ▼
                                nb-activity
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
                    │ 6. Capture │──▶ CaptureContext
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

Each persona is a thin composition root:
1. Reuses the full nb-rs execution engine
2. Adds one or more protocol-specific adapters
3. Provides adapter-specific help text and defaults

Current personas:
- **cassnbrs** — Cassandra/CQL via Apache C++ driver
- **opennbrs** — OpenAPI-driven HTTP services (also usable for
  OpenSearch and similar REST APIs)

The persona model prevents dependency bloat in the core binary
while allowing each persona to statically link its driver for
single-binary deployment.

---

## Contract Boundaries

| Boundary | Type | Direction |
|----------|------|-----------|
| Workload → Activity | `ParsedOp` | Parsed ops, bindings, params, tags |
| Variates → Activity | `GkProgram` + `GkState` | Immutable program (includes globals) shared via Arc; per-fiber mutable state |
| Activity → Adapter | `DriverAdapter` / `OpDispenser` | Init-time template analysis, cycle-time execution |
| Activity → Adapter | `ResolvedFields` | Typed values + lazy strings per cycle |
| Adapter → Activity | `OpResult` / `ExecutionError` | Result body + captures, or scoped error |
| Activity → Metrics | `ActivityMetrics` | Timers, counters, gauges |
| Metrics → Reporters | `MetricsFrame` | Immutable snapshots at capture intervals |
