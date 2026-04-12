# 03: Error Handling

All errors are handled explicitly. No errors swallowed. No silent
failures. The `ignore` action is an explicit user choice, never
a default.

---

## Error Scoping

Errors are classified by scope to drive different handling:

```rust
pub enum ExecutionError {
    /// Per-op: template-specific, retryable
    Op(AdapterError),
    /// Adapter-level: connection-wide, not retryable
    Adapter(AdapterError),
}

pub struct AdapterError {
    pub error_name: String,   // routing key: "Timeout", "cql_error"
    pub message: String,
    pub retryable: bool,      // adapter's hint to executor
}
```

Three scopes capture all failure modes:

**Op-level errors** (query timeout, HTTP 4xx/5xx, bind failure):
per-template, retryable with the same resolved fields.

**Adapter-level errors** (connection refused, auth failure):
affect all ops, never retried.

**Stanza-level errors** (upstream capture failure, dependency
chain break): an op failed and downstream ops in the same
linearized chain cannot proceed. The executor skips the
dependent ops and records errors for the entire chain. This is
not a separate `ExecutionError` variant — it's the executor's
response to an Op error in a dependency chain (see
[02: Concurrency Model](02_concurrency_model.md), Linearization
and Error Handling).

---

## Error Router

Matches error names against rules and applies response actions.

### Spec Syntax

```
errors="<pattern>:<actions>[;<pattern>:<actions>]*"
```

Patterns are regex. Actions are comma-separated. First match wins.

### Actions

| Action | Effect |
|--------|--------|
| `retry` | Allow executor to retry this op |
| `warn` | Log warning to stderr |
| `count` | Increment error counter (always happens) |
| `stop` | Halt the activity |
| `ignore` | Suppress logging (error still counted) |

### Examples

```bash
# Default
errors=".*:warn,count"

# Retry timeouts, stop on auth, warn on rest
errors="Timeout:retry,warn;Auth.*:stop;.*:warn,count"

# Ignore 404s, retry 503s
errors="HttpStatus404:ignore;HttpStatus503:retry,warn;.*:warn,count"
```

---

## Retry Semantics

```
execute(cycle, fields) → Err(Op(AdapterError))
  → router.handle_error(error_name, ...) → retryable?
    → YES and retries < max_retries: retry with SAME fields
    → NO or max_retries exceeded: record failure, continue

execute(cycle, fields) → Err(Adapter(AdapterError))
  → router.handle_error(...) → NEVER retry
```

Key properties:
- Same fields: GK state not re-evaluated on retry
- Same dispenser: same prepared statement, same bind plan
- Bounded: `max_retries` default 3
- Scope-gated: only Op errors retryable

---

## Error Taxonomy by Phase

| Phase | Error Source | Handling |
|-------|-------------|----------|
| Parse | YAML syntax | Process exits with message |
| Compile | GK DSL errors | Process exits with diagnostic |
| Init | `map_op()` returns `Err` | Activity aborts |
| Cycle | `ExecutionError::Op` | Router decides; retryable |
| Cycle | `ExecutionError::Adapter` | Router decides; not retryable |
| Validation | Missing ground truth | Hard error (`ExecutionError::Op`) |

---

## Adapter Error Naming

Adapters choose error names that are specific, stable, and
routable:

| Adapter | Error Names |
|---------|------------|
| CQL | `cql_error`, `prepare_error`, `bind_error`, `missing_field` |
| HTTP | `Timeout`, `ConnectionRefused`, `HttpStatus{code}` |
| Stdout | `IoError` |
| Validation | `relevancy_error`, `validation_failed` |

---

## Unrecognized Parameters

Hard error. User passes unknown `key=value` → activity refuses
to start. Prevents silent misconfiguration from typos.

```
error: unrecognized parameter(s): 'trhreads'. Check for typos.
```

---

## Silent Failure Policy

The system must never silently discard errors. Specific rules:

- **Fallible results**: Every `Result` must be matched or
  propagated. `let _ = fallible()` is prohibited on cycle-time
  code paths. `.ok()` must not be used to discard errors.
- **Mutex access**: Cycle-time mutex acquisition uses
  `unwrap_or_else(|e| e.into_inner())` (recover from poison),
  not `.unwrap()` (panics the fiber).
- **Missing values**: Every `Option::None` that indicates a
  problem (missing field, unresolved binding, empty result)
  produces a diagnostic or error — never silently returns a
  default.
- **Error counting**: The `ignore` action in the error router
  suppresses log output but never suppresses error counting.
  All errors are always counted.
- **Panics**: Reserved for invariant violations (programmer
  errors). Operational errors always return `Err(...)`.
  Panicking on bad user input or network failures is a bug.
