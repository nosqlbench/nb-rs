# 30: Adapter Interface

The adapter interface is the contract between the execution engine
and protocol-specific drivers. A tiered pipeline separates
scope-init template analysis from dynamic per-cycle execution
(see [SRD 11](11_polydat_evaluation.md) for the lifecycle vocabulary).

---

## Adapter subsystem — Contract & Axioms

This SRD is the front door for the **adapter cluster** (`adapters/{stdout, http,
plotter, cql, openapi, testkit}`, layer L5/L6) per the
[Subsystem Treatment Standard](00b_subsystem_standard.md).

**Contract.** Adapters **export nothing** — they implement `DriverAdapter` / `OpDispenser`
from `nmbrs_runtime::adapter` and register via the inventory pattern (`nmbrs_runtime::adapters`).
Their *inbound* contract is the trait surface defined here. Allowed edges:
`nmbrs-runtime`, `nmbrs-workload`, `polydat` (+ `nmbrs-metrics` / vendored `cassandra-cpp`
for CQL). No adapter depends on another adapter except `testkit → stdout`
([SRD 05 D4](05_dependency_rules.md)).

**Axioms.**
- **AD1 — Core-first field processing.** The core consumes its own fields first; an
  adapter sees only its own fields; an unknown field is an error, never a silent discard.
  (Detailed below.)
- **AD2 — The console belongs to the adapter.** Only adapter output (stdout fields,
  plotter canvas) writes to the console directly; a console-owning adapter
  (`DisplayPreference::Off`) on an interactive TTY reserves it. Everything else routes
  through the observer/sink. [SRD 41 §Output Routing](41_logging.md).
- **AD3 — Deterministic input, non-deterministic protocol.** The op the adapter receives
  is fully determined by `(cycle, template)`; only the network/server side is
  non-deterministic. [SRD 01 §Core Invariant](01_system_overview.md).

Per-adapter mechanism: [50 CQL](50_cql_adapter.md), [51 HTTP](51_http_adapter.md),
[52 Stdout/Model](52_stdout_model.md), [53 Vectordata](53_vectordata.md).

---

## Core-first field processing

Op templates carry two kinds of fields in one YAML map: fields
the core runtime interprets (wrapper directives, validation
specs, rate-limit overrides, capture declarations) and fields
the adapter interprets (`prepared:`, `raw:`, `method:`, `url:`,
etc.). The boundary is strict and one-directional:

1. **Core consumes its own fields first.** The parsed op
   template passes through the core pipeline — wrapper
   resolution, validation-spec extraction, capture-point
   registration, rate-limit configuration, diagnostic-mode
   checks — and core **removes** every field it recognizes
   from the template before passing the remainder to the
   adapter's `map_op`.
2. **The adapter sees only its own fields.** When
   `DriverAdapter::map_op(template)` runs, the template
   contains exclusively adapter-specific fields. The adapter
   has no need — and no permission — to understand anything
   else.
3. **Unknown fields are errors.** If `map_op` encounters a
   field name it doesn't recognize, that is a hard error,
   not a warning or a silent pass-through. The diagnostic
   names the field and the adapter so the user can tell
   whether they misspelled an adapter field or misplaced a
   core-level directive.

The motivation is separation of concerns. Core concerns
(op wrappers, result validation, capture declarations) are
orthogonal to adapter concerns (how to dispatch a CQL
prepared statement vs. a raw query). Mixing them in the
adapter layer couples every adapter to every wrapper; layering
them here keeps each layer's vocabulary small and auditable.

The principle extends to `params:` routing: fields that look
like adapter fields but are actually activity params
(`relevancy:`, `verify:`, `strict:`) are core concerns.
They're consumed by the core parser into `params` and never
appear in the template the adapter sees. Adapters that need
their own param-like fields (e.g. an `http_timeout:`) register
them via the adapter-level field inventory, documented in
each adapter's SRD.

This is the rule that keeps the adapter surface narrow: an
adapter is a **field mapper plus a runtime driver**, not a
workload-schema participant.

---

## Two Phases

```
     INIT TIME                              CYCLE TIME
  ┌───────────────┐                       ┌──────────────┐
  │ DriverAdapter │                       │ OpDispenser  │
  │               │                       │              │
  │ map_op(       │                       │ execute(     │
  │   template    │──▶ Box<OpDispenser>──▶│   cycle,     │
  │ )             │                       │   fields     │
  │               │                       │ )            │
  │ Analyzes      │                       │ Binds + runs │
  │ template,     │                       │ via driver   │
  │ prepares      │                       │              │
  │ statements    │                       └──────┬───────┘
  └───────────────┘                              │
                                       Result<OpResult,
                                              ExecutionError>
```

---

## Core Traits

```rust
pub trait DriverAdapter: Send + Sync + 'static {
    fn name(&self) -> &str;
    fn map_op(&self, template: &ParsedOp)
        -> Result<Box<dyn OpDispenser>, String>;
}

pub trait OpDispenser: Send + Sync {
    fn execute<'a>(
        &'a self,
        cycle: u64,
        ctx: &'a ExecCtx<'a>,
    ) -> Pin<Box<dyn Future<Output = Result<OpResult, ExecutionError>>
             + Send + 'a>>;
}
```

`DriverAdapter` is constructed once per activity and shared via
`Arc`. `OpDispenser` is created per unique template at init time
and shared across fibers.

`ExecCtx` bundles two parallel dynamic-pull views of Polydat state:

- `ctx.fields: &ResolvedFields` — op-field substitution view for
  this dispenser. Adapters consume *only* this.
- `ctx.pulls: &ResolvedPulls` — wrapper-side handle-indexed view
  used by validation / conditional / throttle wrappers higher in
  the chain. Adapters ignore this.

See [SRD 32 §"`ExecCtx` — dynamic-pull bundle"](32_wrappers.md) for
the design and [SRD 31 §"Pull plan vs bind plan"](31_op_pipeline.md)
for the contract that keeps the two views distinct.

---

## ResolvedFields

Typed values from the Polydat kernel, with lazy string rendering:

```rust
pub struct ResolvedFields {
    pub names: Vec<String>,
    pub values: Vec<Value>,
    strings: OnceLock<Vec<String>>,  // rendered on first access
}
```

Access paths:
- `get_value(name)` — typed `Value` (no string cost)
- `get_str(name)` — triggers lazy string rendering
- `strings()` — all strings at once

CQL uses `get_value()` for native type binding. HTTP/stdout use
`get_str()`. Diagnostics use `to_json()`.

---

## OpResult

```rust
pub struct OpResult {
    pub body: Option<Box<dyn ResultBody>>,
    pub captures: HashMap<String, Value>,
}
```

No `success` or `status` fields. If you have an `OpResult`, the
operation succeeded. Protocol-specific status codes belong inside
the adapter's `ResultBody`.

---

## ResultBody

Adapters define native result types:

```rust
pub trait ResultBody: Send + Sync + Debug {
    fn to_json(&self) -> serde_json::Value;
    fn as_any(&self) -> &dyn Any;
    fn element_count(&self) -> u64 { 1 }
    fn byte_count(&self) -> Option<u64> { None }
}
```

- `to_json()` — universal representation for logging, capture,
  validation
- `as_any()` — downcast to adapter-native type
- `element_count()` / `byte_count()` — traversal metrics

Implementations:
- `CqlResultBody` — typed row data from CQL queries
- `TextBody(String)` — simple text (stdout, model)
- Future: `HttpResultBody` with status, headers, body

---

## ExecutionError

```rust
pub enum ExecutionError {
    Op(AdapterError),       // per-op, retryable
    Adapter(AdapterError),  // connection-wide, not retryable
}

pub struct AdapterError {
    pub error_name: String,
    pub message: String,
    pub retryable: bool,
}
```

See [03: Error Handling](03_error_handling.md) for routing and
retry semantics.

---

## Adapter-Specific Metrics and Status

The `OpDispenser` trait includes three optional methods that
enable adapter-specific metrics and status line integration:

### `adapter_metrics()`

```rust
fn adapter_metrics(&self) -> Vec<Sample> { ... }
```

Called by the metrics scheduler alongside standard activity
metrics. Returns additional `Sample`s (timers, counters) that
represent adapter-internal state. These appear in the summary
report and are delivered to all reporters (SQLite, CSV, etc.).

Default: delegates to `inner_dispenser()` if this is a wrapper,
otherwise returns empty.

### `status_counters()`

```rust
fn status_counters(&self) -> Vec<(&str, u64)> { ... }
```

Returns `(display_name, cumulative_count)` pairs for the
activity status line. Unlike `adapter_metrics()` which snapshots
delta timers, status counters are cumulative and safe to read
from the progress thread without interfering with the metrics
pipeline.

Default: delegates to `inner_dispenser()` if this is a wrapper,
otherwise returns empty.

### `inner_dispenser()`

```rust
fn inner_dispenser(&self) -> Option<&dyn OpDispenser> { None }
```

Returns the wrapped inner dispenser if this is a wrapper.
Enables delegation chains for `adapter_metrics()` and
`status_counters()` through wrapper layers
(`TraversingDispenser`, `ConditionalDispenser`,
`PollingDispenser`, `EmitDispenser`, etc.). Each wrapper
implements `inner_dispenser()` to point to its wrapped
dispenser, so metrics calls propagate to the adapter's
concrete dispenser at the bottom of the chain.

### `default_status_metrics()` on DriverAdapter

```rust
fn default_status_metrics(&self) -> Vec<StatusMetric> { Vec::new() }
```

Declares which adapter-specific metrics should appear on the
status line by default. Each entry specifies a metric name,
display label, and render mode (`Rate`, `Count`, or `Latency`).
Workloads can override this via a `status:` field on phases.

---

## CQL Batch Support

The CQL adapter provides `CqlBatchDispenser` for grouping
multiple bound statements into a single CQL BATCH call.

```rust
struct CqlBatchDispenser {
    session: SessionHandle,
    stmt_text: String,
    bind_names: Vec<String>,
    prepared: Mutex<Option<Arc<PreparedStatement>>>,
    batch_type: BatchType,          // logged | unlogged | counter
    rows_timer: Timer,              // amortized per-row latency
    rows_total: AtomicU64,          // cumulative row counter
}
```

**Batch budget model**: The executor advances the cursor
repeatedly, evaluates the Polydat graph per position, binds each
statement, and accumulates rows. The batch is executed as one
CQL BATCH call. `rows_timer` records amortized latency
(batch_nanos / row_count) per row for throughput reporting.
`rows_total` is a cumulative counter surfaced via
`status_counters()` for the progress line.

`CqlBatchDispenser` implements both `adapter_metrics()`
(returning the rows timer snapshot and rows_inserted_total
counter) and `status_counters()` (returning the cumulative
`rows_inserted` count).

---

## No Space Concept

Java nosqlbench had `Space` for many-to-many client/server
topology testing. Dropped in nmbrs. Rust native drivers handle
connection pooling internally. The adapter owns one driver
instance; fibers share it via `Arc<dyn DriverAdapter>`.

---

## Display Preference

Each adapter declares a `DisplayPreference` (registration-level,
checked before construction, **params-aware**):

| Value | Meaning |
|-------|---------|
| `Auto` | TUI-compatible — the dashboard may run alongside (most adapters) |
| `Off`  | The adapter writes its own output to the console; the dashboard must not run, or it would overwrite that output |

The stdout adapter is `Off` when writing to the **console** (default /
`filename=stdout`) and `Auto` when `filename=` redirects to a **file**
(the console is then free). The plotter is always `Off`. When the
resolved adapter is `Off`, the run collapses to `tui=off`; on an
*interactive* TTY it further reserves the console for the adapter's
output (status line, run-completion notices, and post-run summary go to
`session.log` only). The full routing contract is
[SRD 41 §Output Routing](41_logging.md).
