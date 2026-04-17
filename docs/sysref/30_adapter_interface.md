# 30: Adapter Interface

The adapter interface is the contract between the execution engine
and protocol-specific drivers. A tiered pipeline separates
init-time template analysis from cycle-time execution.

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
        fields: &'a ResolvedFields,
    ) -> Pin<Box<dyn Future<Output = Result<OpResult, ExecutionError>>
             + Send + 'a>>;
}
```

`DriverAdapter` is constructed once per activity and shared via
`Arc`. `OpDispenser` is created per unique template at init time
and shared across fibers.

---

## ResolvedFields

Typed values from the GK kernel, with lazy string rendering:

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
repeatedly, evaluates the GK graph per position, binds each
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
topology testing. Dropped in nb-rs. Rust native drivers handle
connection pooling internally. The adapter owns one driver
instance; fibers share it via `Arc<dyn DriverAdapter>`.
