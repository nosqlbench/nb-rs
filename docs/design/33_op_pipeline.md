# SRD 33 — Op Execution Pipeline and Decorator Stack

## Background: Java nosqlbench Architecture

The Java nosqlbench op pipeline has four distinct phases:

1. **Mapping** (init time): `OpMapper` interprets a YAML template
   and produces an `OpDispenser` — a factory that can create concrete
   ops for any cycle number.

2. **Dispensing** (per cycle): `OpDispenser.getOp(cycle)` creates a
   concrete `CycleOp<RESULT>` with all template bindings resolved.

3. **Decoration** (per op type): Optional wrappers are conditionally
   layered around the dispenser at init time:
   - `CapturingOpDispenser` — extracts result values into flow context
   - `AssertingOpDispenser` — validates results against conditions
   - `DryrunOpDispenser` — synthesizes but doesn't execute
   - `ResultPrintingOpDispenser` — prints results to stdout
   - `PollingOp` — re-executes until a condition is met

4. **Execution** (per cycle): `StandardAction.runCycle(cycle)` invokes
   the (possibly decorated) op, handles retries, metrics, and
   verification.

The wrapper stack is assembled in a fixed order by
`OpFunctionComposition.wrapOptionally()`:

```
dryrun( assert( print( capture( real_op ) ) ) )
```

Each layer is conditionally applied — if no captures are declared,
`CapturingOpDispenser` is skipped entirely.

## Design for nb-rs

### What to Keep

- **Decorator pattern**: wrapping op execution with optional layers
  is clean, composable, and doesn't require modifying adapters
- **Conditional application**: only add layers that are needed
- **Fixed ordering**: the stack order matters (capture must see real
  results; assertions validate before dry-run hides execution)
- **Flow context threading**: captured values available to subsequent
  ops via the capture context (already implemented in SRD 28)

### What to Change

- **No OpDispenser/OpMapper split**: In nb-rs, the GK kernel handles
  all binding resolution (the "dispensing" role). The adapter just
  executes assembled ops. There's no per-adapter mapper — the
  workload parser + GK compiler do all template interpretation.

- **No per-cycle op allocation**: Java creates a new `CycleOp` object
  per cycle via the dispenser. In nb-rs, `AssembledOp` is a simple
  struct with a `HashMap<String, String>` — cheap to create, no
  heap allocation beyond the strings.

- **Decorators as middleware, not wrappers**: Instead of wrapping the
  dispenser (which we don't have), decorators wrap the adapter's
  `execute()` call. This is closer to Tower middleware than to the
  Java wrapper pattern.

### Op Pipeline in nb-rs

```
OpBuilder.build(cycle, template)
    ↓
AssembledOp { name, fields }
    ↓
[Decorator stack]
    ↓
Adapter.execute(op) → OpResult
    ↓
[Post-execution hooks]
```

## Decorator Trait

```rust
/// A decorator that wraps adapter execution with additional behavior.
///
/// Decorators are composed into a stack at activity init time.
/// Each decorator can:
/// - Inspect or modify the assembled op before execution
/// - Short-circuit execution (e.g., dry-run)
/// - Inspect or modify the result after execution
/// - Perform side effects (e.g., capture, print, assert)
pub trait OpDecorator: Send + Sync {
    /// Called before adapter execution. Return None to proceed,
    /// or Some(result) to short-circuit.
    fn before_execute(
        &self,
        op: &AssembledOp,
        cycle: u64,
        captures: &CaptureContext,
    ) -> Option<Result<OpResult, AdapterError>> {
        None // default: proceed to adapter
    }

    /// Called after successful adapter execution.
    fn after_success(
        &self,
        op: &AssembledOp,
        result: &OpResult,
        cycle: u64,
        captures: &mut CaptureContext,
    ) {}

    /// Called after adapter error (before retry decision).
    fn after_error(
        &self,
        op: &AssembledOp,
        error: &AdapterError,
        cycle: u64,
    ) {}
}
```

## Built-in Decorators

### DryRunDecorator

Short-circuits execution — the op is assembled (bindings resolved)
but never sent to the adapter.

```rust
struct DryRunDecorator {
    mode: DryRunMode,
}

enum DryRunMode {
    /// Assemble but don't execute. Return success with no body.
    Op,
    /// Assemble, don't execute, print the assembled op to stdout.
    Emit,
}
```

Activated by `--dry-run` or `--dry-run=emit` CLI flag, or
`dryrun: op|emit` in workload params.

### CaptureDecorator

Extracts values from the adapter result and writes them to the
capture context. Wired from `[name]` declarations in op templates.

```rust
struct CaptureDecorator {
    /// Per-op capture declarations, keyed by op name.
    captures: HashMap<String, Vec<CaptureDecl>>,
}
```

After successful execution, calls the adapter's `CaptureExtractor`
to extract values and writes them to the capture context via
`captures.set(name, value)`.

### AssertDecorator

Validates results against expected conditions. Conditions are
declared in op template params:

```yaml
ops:
  query:
    stmt: "SELECT count(*) FROM users"
    assert-status: 200
    assert-body-contains: "count"
```

```rust
struct AssertDecorator {
    assertions: HashMap<String, Vec<Assertion>>,
}

enum Assertion {
    StatusEquals(i32),
    BodyContains(String),
    BodyMatches(regex::Regex),
    Custom(Box<dyn Fn(&OpResult) -> bool + Send + Sync>),
}
```

After successful execution, runs each assertion. If any fails,
converts the success into an error (for retry/error-handler routing).

### PrintDecorator

Prints the assembled op and/or result to stderr for debugging.
Activated by `--diagnose` or `instrument: print` in op params.

```rust
struct PrintDecorator {
    print_op: bool,
    print_result: bool,
}
```

### MetricsDecorator

Records per-op-name timing into the metrics system. This is what
currently lives inline in the executor loop — extracting it into
a decorator makes it composable and optional.

```rust
struct MetricsDecorator {
    service_timer: Timer,
    op_timers: HashMap<String, Timer>,
}
```

## Decorator Stack Assembly

The stack is assembled at activity init time based on workload
configuration:

```rust
fn build_decorator_stack(config: &ActivityConfig, ops: &[ParsedOp]) -> Vec<Box<dyn OpDecorator>> {
    let mut stack: Vec<Box<dyn OpDecorator>> = Vec::new();

    // Innermost first (closest to adapter execution)
    // 1. Metrics (always)
    stack.push(Box::new(MetricsDecorator::new(&config.metrics)));

    // 2. Capture (if any op declares captures)
    if ops.iter().any(|op| has_capture_points(op)) {
        stack.push(Box::new(CaptureDecorator::from_ops(ops)));
    }

    // 3. Assert (if any op declares assertions)
    if ops.iter().any(|op| has_assertions(op)) {
        stack.push(Box::new(AssertDecorator::from_ops(ops)));
    }

    // 4. Print (if diagnose mode)
    if config.diagnose {
        stack.push(Box::new(PrintDecorator { print_op: true, print_result: true }));
    }

    // 5. DryRun (outermost — must short-circuit before everything)
    if config.dry_run != DryRunMode::None {
        stack.push(Box::new(DryRunDecorator { mode: config.dry_run }));
    }

    stack
}
```

## Execution with Decorators

The executor applies the stack in order:

```rust
async fn execute_with_decorators(
    adapter: &dyn Adapter,
    op: &AssembledOp,
    cycle: u64,
    decorators: &[Box<dyn OpDecorator>],
    captures: &mut CaptureContext,
) -> Result<OpResult, AdapterError> {
    // Before: check for short-circuit (outermost to innermost)
    for dec in decorators.iter().rev() {
        if let Some(result) = dec.before_execute(op, cycle, captures) {
            return result;
        }
    }

    // Execute
    let result = adapter.execute(op).await;

    // After: apply post-hooks (innermost to outermost)
    match &result {
        Ok(op_result) => {
            for dec in decorators.iter() {
                dec.after_success(op, op_result, cycle, captures);
            }
        }
        Err(error) => {
            for dec in decorators.iter() {
                dec.after_error(op, error, cycle);
            }
        }
    }

    result
}
```

## Comparison with Java nosqlbench

| Aspect | Java nosqlbench | nb-rs |
|--------|----------------|-------|
| Op creation | OpDispenser creates per-cycle object | OpBuilder assembles struct per cycle |
| Decoration target | Wraps the OpDispenser (factory) | Wraps the execute() call (middleware) |
| Composition | Nested dispenser wrappers | Flat decorator stack |
| Ordering | Fixed in OpFunctionComposition | Fixed in build_decorator_stack |
| Conditional | wrapOptionally() pattern | if-has-feature push pattern |
| Flow context | FlowContextAwareOp interface | CaptureContext passed to decorators |
| Verification | Groovy-based verifier scripts | Rust Assertion enum |
| Dry-run | DryrunOp short-circuits apply() | DryRunDecorator short-circuits before_execute() |

The key simplification: nb-rs doesn't need the Dispenser abstraction
because the GK kernel already handles all binding resolution. The
decorator stack is flatter and more explicit — no nested wrapper
objects, just a list of hooks called in order.

## Future Extensions

- **PollingDecorator**: Re-execute until a condition on the result
  is met (equivalent to Java's PollingOp)
- **ThrottleDecorator**: Per-op-type rate limiting (separate from
  the activity-level rate limiter)
- **TraceDecorator**: OpenTelemetry span creation per op
- **Custom decorators via plugins**: External crates implement
  OpDecorator and register via NodeFactory-like mechanism
