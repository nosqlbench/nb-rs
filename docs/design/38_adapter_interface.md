# SRD 38: Adapter Interface — Standard Contract for Protocol Drivers

Defines the adapter abstraction layer for nb-rs: the contract between
the workload execution engine and protocol-specific drivers. A tiered
pipeline separates init-time template analysis from cycle-time
execution, with lazy field rendering, native result types, and
delaminated error scoping.

---

## Background

### Java nosqlbench (what we're keeping)

The Java adapter architecture had a three-tier pipeline:

```
ParsedOp (template)
  → OpMapper        init-time, once per template
    → OpDispenser    per-template state (prepared stmt, bind plan)
      → CycleOp      cycle-time, immutable, retryable
```

**What worked well:** init-time template analysis (prepare once,
execute many), per-template metrics, clean separation of concerns.

**What we're dropping:** The `Space` concept. In Java nosqlbench,
Spaces enabled many-to-many client/server topology testing — N
client instances talking to M server targets with metadata-driven
session routing. This vastly complicated the implementation and
is not needed for nb-rs's current scope (one target at a time).

Most Rust native drivers (cassandra-cpp, reqwest, sqlx, tonic)
already handle connection pooling and concurrency internally. The
adapter owns a single driver instance and fibers share it via `Arc`.
If a specific driver needs per-fiber state, the adapter handles that
internally — it's not an interface concern.

---

## Design

### Two Phases

```
     INIT TIME                              CYCLE TIME
  ┌───────────────┐                       ┌──────────────┐
  │ DriverAdapter │                       │ OpDispenser  │
  │               │                       │              │
  │ map_op(       │    cycle (u64)        │ execute(     │
  │   template    │    resolved fields    │   cycle,     │
  │ ) ────────────┼──▶ OpDispenser ──────▶│   fields     │
  │               │                       │ )            │
  │ Analyzes      │                       │ Binds + runs │
  │ template,     │                       │ via driver   │
  │ prepares      │                       │              │
  │ statements    │                       └──────┬───────┘
  └───────────────┘                              │
                                       Result<OpResult,
                                              ExecutionError>
```

### Core Traits

```rust
/// A protocol-specific driver adapter. Constructed once per activity
/// with connection parameters. Shared across fibers via Arc.
///
/// The adapter owns the driver connection (session, client, pool)
/// and provides OpDispensers that pre-process each op template.
pub trait DriverAdapter: Send + Sync + 'static {
    /// Map an op template into a dispenser. Called once per unique
    /// op template at activity startup — before any cycles execute.
    ///
    /// This is where expensive init-time work happens:
    /// - Parse the `stmt` / `method` / `query` field
    /// - Determine the operation type (INSERT, SELECT, GET, POST)
    /// - Prepare statements (CQL) or compile request templates (HTTP)
    /// - Pre-compute bind-point resolution order
    /// - Validate field names against expected parameters
    /// - Attach per-template metrics and labels
    fn map_op(&self, template: &ParsedOp) -> Result<Box<dyn OpDispenser>, String>;

    /// Human-readable name for this adapter (e.g., "cql", "http").
    fn name(&self) -> &str;
}

/// A per-template op factory. Created at init time by the adapter,
/// called per-cycle to bind values and execute operations.
///
/// The dispenser captures all template-specific state (prepared
/// statement handle, field names, bind-point indices, metrics) so
/// that the per-cycle path is minimal: bind values and execute.
///
/// Dispensers are shared across fibers (via the dispenser array in
/// the executor). They must be thread-safe. The underlying driver
/// connection is accessed through the adapter (which the dispenser
/// holds a reference to).
pub trait OpDispenser: Send + Sync {
    /// Execute an operation for the given cycle.
    ///
    /// - `cycle` — the cycle coordinate (for logging/debugging)
    /// - `fields` — fully-resolved typed values from the GK kernel
    ///
    /// The dispenser owns the full driver call path: bind the
    /// resolved values into the pre-analyzed template, execute
    /// via the driver, and return the result.
    fn execute<'a>(
        &'a self,
        cycle: u64,
        fields: &'a ResolvedFields,
    ) -> Pin<Box<dyn Future<Output = Result<OpResult, ExecutionError>> + Send + 'a>>;
}
```

### ResolvedFields — Lazy String Rendering

The GK synthesis pipeline produces resolved values for each bind
point. The dispenser receives typed values; string rendering is
deferred until an adapter actually needs it.

```rust
/// Resolved field values for a single cycle.
///
/// Typed values are always available. String representations are
/// lazily computed on first access — adapters that bind typed
/// values natively (e.g., CQL prepared statements) never pay
/// the string rendering cost.
pub struct ResolvedFields {
    /// Field names in op template declaration order.
    pub names: Vec<String>,
    /// Typed values, parallel to `names`.
    pub values: Vec<Value>,
    /// Lazily rendered strings (computed once on first access).
    strings: OnceLock<Vec<String>>,
}

impl ResolvedFields {
    /// Create with names and typed values. Strings are not rendered.
    pub fn new(names: Vec<String>, values: Vec<Value>) -> Self;

    /// Access lazily-rendered string representations.
    pub fn strings(&self) -> &[String];

    /// Get a field value by name as a string (triggers lazy rendering).
    pub fn get_str(&self, name: &str) -> Option<&str>;

    /// Get a field value by name as a typed Value (no string rendering).
    pub fn get_value(&self, name: &str) -> Option<&Value>;
}
```

Adapters choose which access path to use:
- **CQL:** `get_value()` for native type binding — never renders strings
- **HTTP/stdout:** `get_str()` or `strings()` for text rendering
- **JSON:** either, depending on target format

### ResultBody — Native Result Types

The adapter defines its own result type and implements the
`ResultBody` trait. External consumers call `.to_json()` for a
universal view; adapter-internal code can downcast via `.as_any()`
to access the native type.

```rust
/// Trait for adapter-specific result bodies.
///
/// The adapter defines its own concrete result type and implements
/// this. Internal code can downcast via `as_any()`. External
/// consumers call `to_json()` for logging, capture, verification.
pub trait ResultBody: Send + Sync + Debug {
    fn to_json(&self) -> serde_json::Value;
    fn as_any(&self) -> &dyn Any;
    /// Count of logical elements (rows, records). Default 1.
    fn element_count(&self) -> u64 { 1 }
    /// Size in bytes, if known. Used for throughput metrics.
    fn byte_count(&self) -> Option<u64> { None }
}

/// Simple text result body (convenience impl).
pub struct TextBody(pub String);
```

Adapter-specific result types (implemented):

- **CQL:** `CqlResultBody { rows: Vec<HashMap<String, serde_json::Value>> }` —
  typed column values per row, extracted from `CassResult` via
  `LendingIterator`. Supports `as_any()` downcast for native column
  access (e.g., validation extracts integer indices directly).
  `to_json()` returns a JSON array of row objects.
- **HTTP:** `HttpResultBody` — carries status, headers, body
- **Model/stdout:** `TextBody(rendered_text)` — simple text

### OpResult

```rust
/// The result of a successful operation.
///
/// If you have an OpResult, the operation succeeded. Failure is
/// represented by ExecutionError, not by flags on the result.
/// Protocol-specific status codes (HTTP, CQL) live inside the
/// adapter's ResultBody implementation, not on the generic result.
pub struct OpResult {
    /// Adapter-specific response body. Native type via `as_any()`,
    /// universal JSON via `to_json()`.
    /// `None` for operations with no meaningful result (e.g., DDL).
    pub body: Option<Box<dyn ResultBody>>,
    /// Captured values from the result (populated by adapters that
    /// support capture points). Key = capture alias name.
    pub captures: HashMap<String, Value>,
}
```

Note: no `success` or `status` fields. `Ok(OpResult)` means success.
Protocol-specific status codes belong in the adapter's `ResultBody`.


### ExecutionError — Delaminated Error Scoping

Errors are classified by scope to drive different handling:

```rust
/// Execution error with scope delamination.
pub enum ExecutionError {
    /// Per-op failure: this specific operation failed.
    /// Template-specific, may be retried with the same resolved fields.
    Op(AdapterError),

    /// Adapter-level failure: the driver connection or session is
    /// degraded. Affects all ops. The activity may need to pause or stop.
    Adapter(AdapterError),
}

pub struct AdapterError {
    /// Error category name for the error handler (e.g.,
    /// "cql_write_timeout", "http_503", "connection_refused").
    pub error_name: String,
    pub message: String,
    /// Hint to the executor: is this worth retrying?
    pub retryable: bool,
}
```

**Op-level errors** (query timeout, validation failure, HTTP 4xx/5xx)
are per-template and retryable. The executor retries with the same
resolved fields up to `max_retries`.

**Adapter-level errors** (connection refused, auth failure, driver
crash) affect all ops. The executor does NOT retry — the error router
decides whether to pause or stop the activity.

The executor's retry logic:

```rust
match dispenser.execute(cycle, &fields).await {
    Ok(result) => { /* record metrics, store captures */ }
    Err(e) => {
        let inner = e.error();
        let detail = error_router.handle_error(&inner.error_name, ...);
        if !e.is_adapter_level() && detail.is_retryable() && retries < max_retries {
            retries += 1;
            continue;
        }
        // else: record failure, advance
    }
}
```

---

## Concrete Example: CQL Adapter

### DriverAdapter

```rust
pub struct CqlDriverAdapter {
    session: cassandra_cpp::Session,
    consistency: cassandra_cpp::Consistency,
}

impl DriverAdapter for CqlDriverAdapter {
    fn name(&self) -> &str { "cql" }

    fn map_op(&self, template: &ParsedOp) -> Result<Box<dyn OpDispenser>, String> {
        let stmt_text = template.op.get("stmt")
            .and_then(|v| v.as_str())
            .ok_or("CQL op requires a 'stmt' field")?;

        // Extract bind-point names from the statement
        let bind_names: Vec<String> = template.op.keys()
            .filter(|k| k.as_str() != "stmt")
            .cloned().collect();

        if bind_names.is_empty() {
            // DDL or simple query — no bind points
            Ok(Box::new(CqlSimpleDispenser { ... }))
        } else {
            // Has bind points — prepare lazily on first execute
            Ok(Box::new(CqlPreparedDispenser {
                stmt_text: stmt_text.to_string(),
                bind_names,
                prepared: Mutex::new(None),
                ...
            }))
        }
    }
}
```

### OpDispenser

```rust
impl OpDispenser for CqlPreparedDispenser {
    fn execute<'a>(&'a self, _cycle: u64, fields: &'a ResolvedFields)
        -> Pin<Box<dyn Future<Output = Result<OpResult, ExecutionError>> + Send + 'a>>
    {
        Box::pin(async move {
            let prepared = self.get_prepared().await?;
            let mut stmt = prepared.bind();

            // Bind typed values — no string rendering needed
            for name in &self.bind_names {
                if let Some(value) = fields.get_value(name) {
                    match value {
                        Value::U64(v) => stmt.bind_int64_by_name(name, *v as i64),
                        Value::F64(v) => stmt.bind_double_by_name(name, *v),
                        Value::Str(v) => stmt.bind_string_by_name(name, v),
                        Value::Bytes(v) => stmt.bind_bytes_by_name(name, v.clone()),
                        _ => stmt.bind_string_by_name(name, &value.to_display_string()),
                    }.map_err(|e| ExecutionError::Op(AdapterError {
                        error_name: "bind_error".into(),
                        message: format!("bind '{name}': {e}"),
                        retryable: false,
                    }))?;
                }
            }

            let result = stmt.execute().await
                .map_err(|e| ExecutionError::Op(AdapterError {
                    error_name: "cql_error".into(),
                    message: format!("{e}"),
                    retryable: false,
                }))?;

            Ok(OpResult {
                success: true,
                status: 0,
                body: Some(Box::new(CqlResultBody::from(result))),
                captures: HashMap::new(),
            })
        })
    }
}
```

---

## All Adapters Use This Interface

There is no separate "flat" adapter trait. All adapters — stdout,
http, model, cql, grpc — implement `DriverAdapter` and provide
`OpDispenser` instances. This is the only adapter interface.

For simple adapters (stdout), `map_op()` returns a dispenser that
just formats and prints. The init-time/cycle-time split still
applies: the dispenser captures the output format at init time.

### Mixed Adapters Within a Stanza

Different op templates within the same stanza may use different
adapters. For example, an INSERT via CQL followed by a GET via HTTP
to verify the data. The adapter for each template is resolved at init
time based on the template's `adapter` param. Each template's
dispenser may be backed by a different adapter instance, but all share
the same GK program and field resolution pipeline. See SRD 40
(Adapter Resolution) for details.

---

## Metrics Integration

Each OpDispenser attaches per-template metrics at init time:

```rust
impl CqlPreparedDispenser {
    fn new(template: &ParsedOp, ...) -> Self {
        let timer = metrics::timer("op", &[
            ("name", &template.name),
            ("type", "cql_prepared"),
        ]);
        Self { timer, ... }
    }
}

impl OpDispenser for CqlPreparedDispenser {
    async fn execute(&self, ...) -> Result<OpResult, ExecutionError> {
        let _guard = self.timer.start();
        // ... execute ...
    }
}
```

Per-template latency histograms, automatically labeled.

---

## Error Handling

Dispensers classify errors by scope and category:

```rust
// Op-level: this query failed
ExecutionError::Op(AdapterError {
    error_name: "cql_write_timeout",
    message: "WriteTimeoutException: ...",
    retryable: true,
})

// Adapter-level: connection is down
ExecutionError::Adapter(AdapterError {
    error_name: "connection_refused",
    message: "Connection refused: 127.0.0.1:9042",
    retryable: false,
})
```

The executor's error router matches `error_name` against the user's
error spec:

- `errors=stop` — any error stops the activity
- `errors=count` — count and continue
- `errors="Timeout:retry,warn;.*:count"` — retry timeouts, count rest
- `errors=warn` — log and continue

Op-level errors are retryable (up to `max_retries`). Adapter-level
errors skip retry and go directly to the error router.

---

## Activity Execution Loop

```rust
async fn executor_task(
    dispensers: &[Arc<dyn OpDispenser>],
    program: &GkProgram,
    fiber: &mut FiberBuilder,
    cycle_source: &CycleSource,
    op_sequence: &OpSequence,
    rate_limiter: Option<&RateLimiter>,
    metrics: &ActivityMetrics,
) {
    let stanza_len = op_sequence.stanza_length();

    while let Some(base_cycle) = cycle_source.next_n(stanza_len) {
        fiber.reset_captures(base_cycle);

        for offset in 0..stanza_len {
            let cycle = base_cycle + offset;

            if offset > 0 { fiber.apply_captures(); }

            // Rate limit
            if let Some(rl) = rate_limiter { rl.acquire().await; }

            // Select template and resolve fields
            let (idx, template) = op_sequence.get_with_index(cycle);
            fiber.set_coordinates(&[cycle]);
            let fields = fiber.resolve(template);

            // Execute via dispenser
            let start = Instant::now();
            match dispensers[idx].execute(cycle, &fields).await {
                Ok(result) => {
                    metrics.record_success(start.elapsed());
                    for (name, value) in result.captures {
                        fiber.capture(&name, value);
                    }
                }
                Err(e) => {
                    let inner = e.error();
                    let detail = error_router.handle_error(
                        &inner.error_name, &inner.message, cycle, ...
                    );
                    if !e.is_adapter_level() && detail.is_retryable() {
                        // retry with same fields
                    }
                }
            }
        }
    }
}
```

---

## Why No Space

Java nosqlbench used `Space` for multi-tenant topology testing: N
client instances talking to M server targets with metadata-driven
routing. This vastly complicated the adapter implementation and is
not needed for nb-rs's current scope (single target per activity).

Rust native drivers (cassandra-cpp, reqwest, sqlx, tonic) handle
connection pooling and concurrency internally. The adapter owns one
driver instance; fibers share it via `Arc<dyn DriverAdapter>`. If a
specific driver needs per-fiber state, the adapter handles that as
an internal implementation detail (e.g., `thread_local!` or a
fiber-ID-keyed pool) — it's not exposed in the interface.

If multi-tenant topology testing is needed in the future, it can be
added as a layer above `DriverAdapter` without changing the core
interface.

---

## Relationship to Other SRDs

- **SRD 21 (Execution Layer)**: The executor fiber loop uses
  dispensers with stanza-scoped capture flow.
- **SRD 28 (Capture Points)**: Capture extraction is performed by the
  dispenser, which populates `OpResult.captures` from the native
  result type via `ResultBody.as_any()`.
- **SRD 33 (Op Pipeline)**: Decorators wrap the dispenser.
- **SRD 37 (Client Personas)**: Each persona provides a
  `DriverAdapter`. The persona binary wires it into the executor.
- **SRD 39 (System Layers)**: The adapter contract is the typed
  boundary between the execution engine and protocol-specific code.
- **SRD 41 (Error Handling)**: `ExecutionError::Op` vs `::Adapter`
  drives the retry and routing behavior in the error pipeline.
