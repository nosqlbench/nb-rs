# SRD 41: Error Handling

## Overview

nb-rs classifies, routes, and responds to operational errors using a
two-stage pipeline: adapters classify errors by scope and name, and
the error router matches those names against user-specified rules to
determine the response action.

## General Error Handling Requirements

All possible error conditions in the nb-rs crates must be handled
explicitly. No errors may be swallowed or ignored. All possible
"silent failure" scenarios are categorically disallowed.

Specifically:
- Every `Result` must be matched or propagated. No `let _ = fallible()`.
- Every `Option::None` in a context where it indicates a problem must
  produce a diagnostic message, not silently continue.
- Panics are reserved for invariant violations (programmer errors),
  never for operational errors. All operational failures flow through
  `ExecutionError` or `Result<_, String>`.
- The `ignore` action in the error router is an explicit user choice,
  not a silent default.

## Error Scoping

Errors are delaminated by scope:

```rust
pub enum ExecutionError {
    /// Per-op failure: this specific operation failed.
    /// Template-specific, may be retried with same resolved fields.
    Op(AdapterError),

    /// Adapter-level failure: the driver connection or session is
    /// degraded. Affects all ops. The activity may need to pause/stop.
    Adapter(AdapterError),
}
```

**Op-level errors** are template-specific and retryable: query timeout,
validation failure, HTTP 4xx/5xx, missing field. The executor can
retry the same op with the same resolved fields.

**Adapter-level errors** affect all ops: connection refused, auth
failure, driver crash. The executor does NOT retry — the error router
decides whether to pause or stop the activity.

## Error Classification

Every adapter error carries a classification name:

```rust
pub struct AdapterError {
    pub error_name: String,    // "Timeout", "ConnectionRefused", "cql_error"
    pub message: String,       // human-readable detail
    pub retryable: bool,       // adapter's hint: is this worth retrying?
}
```

The `error_name` is the routing key. Adapters choose names that are:
- **Specific enough** to route differently (e.g., `Timeout` vs `ConnectionRefused`)
- **Stable** across runs (not dependent on timestamps or transient data)
- **Hierarchical** where useful (e.g., `HttpStatus503` → matches `HttpStatus.*`)

### Adapter Error Names

| Adapter | Error Names |
|---------|------------|
| HTTP | `Timeout`, `ConnectionRefused`, `RequestError`, `HttpError`, `HttpStatus{code}` |
| CQL | `cql_error`, `prepare_error`, `bind_error`, `missing_field` |
| Stdout | `IoError` |
| Model | `ModelError` (configurable via `result-error-name`) |

### Error Scope Assignment

| Error | Scope | Rationale |
|-------|-------|-----------|
| Connection refused | `Adapter` | All ops will fail |
| Auth failure | `Adapter` | Session is invalid |
| Query timeout | `Op` | Other queries may succeed |
| HTTP 503 | `Op` | Retryable, server-side |
| HTTP 404 | `Op` | This specific request |
| Bind/parse error | `Op` | Template-specific |
| IO error (stdout) | `Op` | May be transient |

## Error Router

The error router (`nb-errorhandler` crate) matches error names against
rules and applies response actions. Rules are specified via the `errors`
activity parameter.

### Spec Syntax

```
errors="<pattern>:<actions>[;<pattern>:<actions>]*"
```

Patterns are regex. Actions are comma-separated. Rules are tried in
order; first match wins.

### Actions

| Action | Effect |
|--------|--------|
| `retry` | Mark as retryable (allow executor to retry) |
| `warn` | Log a warning to stderr |
| `count` | Increment the error counter (always happens) |
| `stop` | Halt the activity |
| `ignore` | Suppress logging (error is still counted) |

Note: `ignore` is an explicit user choice to suppress log noise for
expected errors. It does NOT suppress error counting — all errors are
always counted. There is no way to make errors truly invisible.

### Examples

```bash
# Retry timeouts, warn on everything else, stop on auth failures
errors="Timeout:retry,warn;Auth.*:stop;.*:warn,count"

# Default: warn and count all errors
errors=".*:warn,count"

# Strict mode: stop on any error
errors=".*:stop"

# Ignore 404s, retry 503s, warn on everything else
errors="HttpStatus404:ignore;HttpStatus503:retry,warn;.*:warn,count"
```

### Default Behavior

When `errors` is not specified, the default is `".*:warn,counter"` —
all errors are logged and counted but execution continues.

## Retry Flow

```
dispenser.execute(cycle, &fields)
  → Err(ExecutionError::Op(AdapterError { retryable: true, ... }))
    → error_router.handle_error(name, message, cycle, duration)
      → ErrorDetail { is_retryable: true } (if "retry" action matched)
        → retries += 1; if retries < max_retries: retry same op
        → else: record failure, advance to next cycle
  → Err(ExecutionError::Adapter(AdapterError { ... }))
    → error_router.handle_error(name, message, cycle, duration)
      → NO retry regardless of router config
      → router decides: warn, count, stop
```

Key properties:
- **Same fields:** Retries use the same resolved field values. The GK
  state is not re-evaluated.
- **Same dispenser:** The same OpDispenser handles the retry.
- **Bounded:** `max_retries` (default 3) caps the retry count.
- **Scope-gated:** Only `Op`-level errors are retryable. `Adapter`-level
  errors skip retry entirely.
- **Adapter hint:** `retryable: bool` is the adapter's suggestion. The
  error router can override — a `retry` action retries even if the
  adapter said `retryable: false`, and absence of `retry` action means
  no retry even if the adapter said `retryable: true`.

## Error Metrics

Every error increments `errors_total`. The error name and message are
available for log analysis. The cycle number and duration at failure
are passed to the error router for context-aware logging.

Service time is still recorded for failed operations — the time spent
on a failed attempt is real resource consumption.

## Adapter Error Design Guidelines

When implementing a new adapter:

1. **Use specific error names.** `Timeout` not `Error`. `HttpStatus429`
   not `HttpError`.

2. **Classify scope correctly.** Connection failures → `Adapter`.
   Query failures → `Op`. When in doubt, use `Op` (more granular).

3. **Set `retryable` conservatively.** Timeouts and 503s are retryable.
   Auth failures and 400s are not. Parse errors are not.

4. **Include useful detail in `message`.** The URL, the error class,
   the first 200 chars of the response body. Not a full stack trace.

5. **Don't panic.** All adapter failures must return
   `Err(ExecutionError::Op/Adapter(AdapterError { ... }))`, never
   panic. Panics in a tokio task kill the fiber and lose cycles.

6. **Don't swallow errors.** If a driver call can fail, propagate the
   error. If a parse can fail, propagate the error. Never `unwrap()`
   on fallible operations in cycle-time code.

## Unrecognized Parameters

Unrecognized parameters are not allowed anywhere. If a user passes
`key=value` on the command line and no component recognizes that key,
it is an error. This prevents silent misconfiguration from typos
(e.g., `cyclse=1000` instead of `cycles=1000`).

The parameter validation pipeline:

1. **Runner** consumes known activity params: `adapter`, `driver`,
   `cycles`, `threads`, `rate`, `stanzarate`, `errors`, `seq`,
   `tags`, `workload`, `op`, `format`, `filename`,
   `stanza_concurrency`, `sc`
2. **Adapter** consumes driver-specific params at `map_op` time
   (e.g., `hosts`, `port`, `keyspace`, `consistency` for CQL)
3. After all consumers have claimed their params, any remaining
   unclaimed params produce a warning or error

This is enforced at init time. The activity does not start if
unrecognized parameters are present.

## Error Taxonomy by Phase

| Phase | Error Type | Handling |
|-------|-----------|----------|
| **Compile time** | GK DSL compilation errors | Process exits. Bindings are invalid. |
| **Init time** | `DriverAdapter::map_op()` returns `Err(String)` | Activity aborts. Template is invalid. |
| **Cycle time** | `ExecutionError::Op(AdapterError)` | Error router decides. Retryable. |
| **Cycle time** | `ExecutionError::Adapter(AdapterError)` | Error router decides. Not retryable. |
