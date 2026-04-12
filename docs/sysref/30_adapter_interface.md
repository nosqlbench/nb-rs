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

## No Space Concept

Java nosqlbench had `Space` for many-to-many client/server
topology testing. Dropped in nb-rs. Rust native drivers handle
connection pooling internally. The adapter owns one driver
instance; fibers share it via `Arc<dyn DriverAdapter>`.
