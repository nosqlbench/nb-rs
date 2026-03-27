# NoSQLBench Java — Assay Summary

Initial survey of the Java nosqlbench codebase (`links/nosqlbench`) to inform
the design of nb-rs.

---

## What NoSQLBench Is

A programmable load-testing engine for databases (NoSQL and beyond). Users
define workloads in YAML, specifying operations with parameterized templates
and data-generation bindings. The engine dispatches those operations across
threads at controlled rates, measuring latency and throughput with HDR
histograms.

---

## Architecture Layers

1. **API / Foundation** — cross-cutting types, annotations, expression SPI
2. **Virtual Data (virtdata)** — 11 modules for deterministic, cycle-driven
   data generation via composable function pipelines
3. **Adapters API** — plugin contract:
   `DriverAdapter` → `OpMapper` → `OpDispenser` → `CycleOp`
4. **Engine Core** — `Activity` → `ActivityExecutor` → `Motor` (threads) →
   `Action` (stride execution), plus rate limiting and state machines
5. **26+ Adapter Implementations** — CQL, MongoDB, HTTP, DynamoDB, vector DBs,
   message queues, etc.
6. **CLI / Scenario Orchestration** — two-pass CLI parsing, YAML scenario
   definitions, GraalVM polyglot scripting
7. **Metrics & Reporting** — component-tree labeled instruments, delta HDR
   histograms, fan-out to console/CSV/SQLite/Prometheus

---

## Key Design Patterns

- **Cycle-deterministic** — the cycle number is the universal coordinate; the
  same cycle always produces the same op (enables retries, reproducibility)
- **Dispenser pattern** — expensive initialization at startup, lightweight
  per-cycle dispensing on the hot path
- **Three-phase op pipeline** — mapping (init-time) → dispensing (per-cycle) →
  execution (hot path)
- **Hierarchical config inheritance** — document → block → op level for
  bindings, params, and tags
- **Token-bucket rate limiting** — with burst recovery, virtual-thread
  compatible
- **SPI-based plugin discovery** — for adapter registration and loading

---

## Priority Subsystems for nb-rs

The following three subsystems have been identified as the initial focus areas.
Each requires detailed design discussion before implementation.

### 1. Variate Generation (virtdata)

The Java version has 11 modules with hundreds of composable functions
(statistical distributions, hashing, realistic data, vectors, HDF5). The core
idea: a binding expression like `Mod(1000); ToString()` is a composable
pipeline of `LongFunction` / `LongUnaryOperator` that maps a cycle number to a
value deterministically.

**Observations for nb-rs:**
- Function composition, zero-cost abstractions, and trait-based dispatch are
  natural fits in Rust.
- The deterministic cycle → value contract is the essential invariant.
- Scope of the function library is a key design decision — the Java version
  has hundreds of functions; nb-rs need not replicate all of them.

### 2. Metrics Collection & Processing

The Java version uses Dropwizard Metrics with custom delta HDR histogram
reservoirs, a component-tree labeling system, a snapshot scheduler with
hierarchical cadence aggregation, and fan-out to multiple reporters.

**Observations for nb-rs:**
- OpenMetrics alignment and label-based dimensional model are the important
  architectural choices.
- The delta-histogram approach (capturing only the interval, not cumulative)
  is critical for accurate percentile reporting under load.
- Reporter fan-out (console, CSV, SQLite, Prometheus push) is a design
  decision about which sinks to support initially.

### 3. Workload Description Language (Uniform Workload Specification)

YAML-based with a layered inheritance model (document → block → op), template
expressions with bind-point substitution (`{binding_name}`), tag-based op
filtering, scenario definitions that compose activity phases, and a
`TEMPLATE(key, default)` macro system for parameterization.

**Observations for nb-rs:**
- The three-level inheritance (document → block → op) for bindings, params,
  and tags is a core structural decision.
- Bind-point resolution connects this subsystem directly to the variate
  generation layer.
- Tag-based filtering determines which ops execute in a given scenario phase.
- The YAML format itself is a design choice worth revisiting — it could remain
  YAML or evolve.

---

## Open Questions

- Which variate functions are essential for an initial release?
- Should the workload format remain YAML, or is there a better fit for Rust
  tooling?
- What is the minimum viable set of metric reporters?
- How should the adapter plugin model work in Rust (traits, dynamic loading,
  compile-time features)?
- What concurrency model should nb-rs use (tokio async, OS threads, hybrid)?
- How much of the scenario scripting layer is needed, and in what form?
