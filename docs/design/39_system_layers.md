# SRD 39: System Layers, Encapsulation, and Contract Boundaries

## Overview

nb-rs is organized into crate-level layers with explicit dependency
direction and contract boundaries. Each crate encapsulates a concern
and exposes a minimal public interface. Cross-layer communication uses
trait objects and typed data structures — never raw strings or untyped
maps at layer boundaries.

## Layer Diagram

```
┌───────────────────────────────────────────────────────────┐
│                     Persona Binaries                      │
│  (cassnbrs, opennbrs, nbrs)                               │
│  Wire adapters to the activity engine. CLI entry point.   │
├───────────────────────────────────────────────────────────┤
│                     Adapter Crates                        │
│  (nb-adapter-stdout, nb-adapter-http, nb-adapter-model)   │
│  Implement DriverAdapter + OpDispenser.                   │
│  Protocol-specific. No knowledge of GK internals.         │
├───────────────────────────────────────────────────────────┤
│                     nb-activity                           │
│  Activity engine: executor, op synthesis, sequencing.     │
│  Defines DriverAdapter / OpDispenser / ResolvedFields.    │
│  Owns the run loop, metrics, rate limiting, error routing.│
├───────────────┬─────────────────────┬─────────────────────┤
│  nb-workload  │  nb-variates        │  nb-metrics         │
│  YAML parsing │  GK kernel, nodes,  │  Timers, counters,  │
│  ParsedOp     │  DSL compiler,      │  HDR histograms,    │
│  tag filters  │  JIT, fusion        │  frame capture      │
├───────────────┴─────────────────────┴─────────────────────┤
│  nb-rate        nb-errorhandler       nb-web (optional)   │
│  Token bucket   Error routing         Web UI, graph       │
│  rate limiter   spec parser           editor, API         │
└───────────────────────────────────────────────────────────┘
```

## Dependency Rules

1. **Downward only.** Higher layers depend on lower layers, never the
   reverse. `nb-variates` never imports from `nb-activity`.

2. **Adapter crates depend on `nb-activity` for traits and types.**
   They implement `DriverAdapter` and `OpDispenser` defined in
   `nb-activity::adapter`. They do NOT depend on `nb-variates`
   directly (except for `Value` type used in `ResolvedFields`).

3. **Persona binaries depend on everything.** They're the composition
   root — they wire adapter crates to the activity engine.

4. **`nb-workload` is standalone.** It parses YAML/JSON workload specs
   into `ParsedOp` structures. No dependency on the GK kernel.

5. **`nb-variates` is standalone.** The generation kernel has no
   knowledge of workloads, adapters, or the activity engine.

## Contract Boundaries

### Workload → Activity

The `ParsedOp` struct is the contract. It carries:
- `name: String` — op identity
- `op: HashMap<String, serde_json::Value>` — field templates with `{bind_point}` placeholders
- `bindings: BindingsDef` — GK source or legacy binding chain
- `params: HashMap<String, serde_json::Value>` — adapter-specific configuration
- `tags: HashMap<String, String>` — filtering metadata

### Activity → Adapter

The `DriverAdapter` / `OpDispenser` traits are the contract:
- `DriverAdapter::map_op(template: &ParsedOp) → Box<dyn OpDispenser>`
  - Init-time: adapter inspects the template and pre-processes it
- `OpDispenser::execute(cycle: u64, fields: &ResolvedFields) → Result<OpResult, AdapterError>`
  - Cycle-time: dispenser receives resolved field values, executes

### Variates → Activity

The `GkProgram` and `GkState` are the contract:
- `GkProgram`: immutable compiled DAG, shared via `Arc`
- `GkState`: per-fiber mutable evaluation state
- `FiberBuilder::resolve(template) → ResolvedFields`: bridge from GK to adapter

### Metrics → Activity

`ActivityMetrics` instruments are the contract:
- `Timer` (HDR histogram) for service/wait/response time
- `Counter` for cycles, errors, stanzas
- `MetricsFrame` snapshots for reporters

## Encapsulation Principles

- **No leaking internals.** Node implementations, wiring details, and
  kernel state are `pub(crate)` in nb-variates. External code accesses
  outputs only through `GkKernel::pull()` or `GkState::pull()`.

- **Typed boundaries.** `ResolvedFields` uses `Vec<Value>` (typed) +
  `Vec<String>` (rendered), not `HashMap<String, String>`. Adapters
  get both typed access (for native binding) and string access (for
  text rendering).

- **Init vs cycle separation.** Heavy work (parsing, compilation,
  prepared statements, table construction) happens at init time.
  Cycle-time code paths are allocation-free where possible.

- **No shared mutable state across fibers.** Each fiber has its own
  `FiberBuilder` (owns `GkState` + `CaptureContext`). The shared
  `GkProgram` is immutable. Dispensers are shared but thread-safe.
