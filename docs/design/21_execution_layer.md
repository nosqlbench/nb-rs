# Execution Layer — Design Discussion

The engine core: how workloads, variates, metrics, rate limiting, and
the HTTP adapter are composed into a running system.

This is a fresh design, not a port of nosqlbench's Activity/Motor/
StrideAction hierarchy.

---

## What nosqlbench Had (and what we're moving away from)

In Java nosqlbench:
- **Activity** — owns config, workload, adapter, metrics, rate limiters
- **ActivityExecutor** — creates N Motor threads
- **Motor** — per-thread loop: get cycle segment → for each cycle: get op → execute
- **StrideAction** — processes a "stride" (batch of cycles)
- **OpSequence** — round-robin LUT mapping cycle → op dispenser
- **OpDispenser** — cycle → immutable CycleOp
- **CycleOp** — executes the actual operation

**What was good:** clean separation of op dispensing (init-time) from
execution (cycle-time). The OpSequence LUT. Rate limiter integration.

**What was complex/rigid:** deep class hierarchy (Activity →
StandardActivity → SimpleActivity), the Motor/StrideAction split,
thread-per-slot model that didn't compose well with async, scenario
scripting layer (Polyglot) bolted on top.

---

## Design Goals for nb-rs

1. **Async-native.** The command layer is async (tokio). Operations
   dispatch as async tasks. The rate limiter is already async.

2. **Flat, not deep.** Minimal type hierarchy. Compose behaviors
   through traits and closures, not inheritance chains.

3. **HTTP-first.** Design around what HTTP needs. Don't abstract for
   hypothetical adapters yet.

4. **Observable.** Every operation is timed and metricated through the
   metrics frame pipeline. Service time, wait time, response time.

5. **Configurable concurrency.** Not "N threads with one motor each"
   but "N concurrent async tasks." The runtime manages the concurrency
   level, not the user managing threads.

6. **Workload-driven.** The workload YAML drives everything: what ops
   to run, how many, at what rate, with what data.

---

## Proposed Architecture

```
CLI
 │
 ▼
Session
 │  owns: config, metrics scheduler
 │
 ├── Activity "write"
 │   │  owns: workload ops, GK kernel, rate limiter, adapter
 │   │
 │   └── Executor (tokio task pool)
 │       ├── task 0: loop { acquire_rate → get_op(cycle) → dispatch → record }
 │       ├── task 1: loop { ... }
 │       └── task N: loop { ... }
 │
 └── Activity "read"
     └── Executor
         └── ...
```

### Session

The root. Owns:
- Session ID and labels
- Metrics snapshot scheduler (dedicated thread)
- Activities (started/stopped by scenario commands)

### Activity

A named unit of work. Owns:
- Workload ops (filtered by tags from scenario step)
- GK kernel (compiled from workload bindings)
- Rate limiter (if configured)
- HTTP adapter (or future adapter)
- Metrics (timers, counters attached to component tree)
- Concurrency config (number of async tasks)
- Cycle range (start..end)

### Executor

The async dispatch loop. Not a type — just a set of tokio tasks:

```rust
async fn executor_task(
    activity: Arc<Activity>,
    cycle_source: Arc<CycleSource>,
) {
    loop {
        // Get next cycle (or batch of cycles)
        let Some(cycle) = cycle_source.next() else { break };

        // Rate limit
        if let Some(rl) = &activity.rate_limiter {
            rl.acquire().await;
        }
        let wait_nanos = ...; // measure

        // Generate variates
        activity.kernel.set_coordinates(&[cycle]);

        // Build the operation from the op template + variates
        let op = activity.build_op(cycle);

        // Execute
        let start = Instant::now();
        let result = activity.adapter.execute(&op).await;
        let service_nanos = start.elapsed().as_nanos() as u64;

        // Record metrics
        activity.timers.service_time.record(service_nanos);
        activity.timers.wait_time.record(wait_nanos);
        activity.timers.response_time.record(service_nanos + wait_nanos);
        activity.counters.total.inc();
        if result.is_err() {
            activity.counters.errors.inc();
        }
    }
}
```

### CycleSource

Distributes cycles to tasks. Thread-safe atomic counter:

```rust
struct CycleSource {
    current: AtomicU64,
    end: u64,
}

impl CycleSource {
    fn next(&self) -> Option<u64> {
        let cycle = self.current.fetch_add(1, Ordering::Relaxed);
        if cycle < self.end { Some(cycle) } else { None }
    }
}
```

No stride batching needed — each task grabs one cycle at a time.
The atomic counter is the coordination mechanism. Simple.

### OpSequence

Maps cycles to op templates via round-robin LUT (same as nosqlbench):

```rust
struct OpSequence {
    ops: Vec<ParsedOp>,
    ratios: Vec<u64>,
    lut: Vec<usize>,  // cycle % lut.len() → op index
}
```

### Adapter Trait

```rust
#[async_trait]
pub trait Adapter: Send + Sync {
    type Op: Send;
    type Result: Send;

    /// Build an operation from an op template and current variates.
    fn build_op(&self, template: &ParsedOp, variates: &VariateContext) -> Self::Op;

    /// Execute the operation.
    async fn execute(&self, op: &Self::Op) -> Result<Self::Result, AdapterError>;
}
```

For HTTP:
- `Op` = `reqwest::Request`
- `Result` = `reqwest::Response`
- `build_op` constructs the request from method, URL, headers, body
  (all templated from variates)
- `execute` sends the request via `reqwest::Client`

---

## Scenario Execution

A scenario is a sequence of commands:

```yaml
scenarios:
  default:
    schema: run driver=http tags==block:schema cycles=10 threads=1
    main: run driver=http tags==block:main cycles=1M threads=auto rate=5000
```

The session executes steps sequentially. Each step:
1. Parse the command string into activity config
2. Filter ops by tags
3. Compile the GK kernel from bindings
4. Create the activity with rate limiter, adapter, metrics
5. Spawn executor tasks
6. Wait for completion (all cycles consumed)

`threads=auto` means "use available CPU cores as concurrency level."

---

## Error Handling

Modular, composable error handler inspired by nosqlbench's design gem.
Errors are classified by type and routed through a chain of handlers.

### Configuration Syntax

```
errors="TimeoutException:retry,warn,counter;.*:error,meter"
```

- Error patterns are regex on exception/error type name
- Handlers are comma-separated, execute in chain order
- Multiple patterns are semicolon-separated
- Default pattern `.*` catches everything

### Handler Modes (composable)

| Mode | Effect |
|------|--------|
| `stop` | Halt execution immediately |
| `warn` | Log warning, continue |
| `error` | Log error, continue |
| `ignore` | Silent pass-through |
| `retry` | Mark as retryable, re-execute the op |
| `counter` | Increment per-error-type counter metric |
| `meter` | Record in per-error-type rate metric |

Handlers compose: `retry,warn,counter` means retry the op, log a
warning, and count it. Each handler in the chain receives and
transforms an `ErrorDetail` that carries retryability, result code,
and context.

### Integration

```rust
let result = adapter.execute(&op).await;
if let Err(e) = result {
    let detail = error_handler.handle(&e, cycle, elapsed_nanos);
    if detail.retryable {
        continue; // retry loop
    }
    // record error metrics, apply result code
}
```

---

## Resolved Questions

- **Q31:** Top-level control loop runs serially (scenario steps in
  order). Each step can `run` (blocking) or `start` (background)
  async tasks. The serial controller manages async activity lifecycles.

- **Q32:** Modular composable error handler (see above). Classified
  by error type, routed through handler chain. Modes: stop, warn,
  retry, counter, meter, ignore.

- **Q33:** GK kernels should be shared where possible. If the kernel
  is Phase 1 (mutable pull-through), each task needs its own instance
  (clone or per-task construction). If Phase 2 (compiled, stateless
  eval), a single `Arc<CompiledKernel>` can be shared.

- **Q34:** One `reqwest::Client` shared across all tasks (Arc). This
  is reqwest's recommended usage — the client manages an internal
  connection pool with keep-alive.

- **Q35:** Defer `recycles` for now. Use `cycles` only. The
  (recycles, cycles) pair maps conceptually to a 2D GK coordinate
  tuple — the bridge between recycles and the coordinate space can
  be designed when needed.

---

## Naming

**Activity** — same as nosqlbench. Well-understood, carries the right
semantics. The unit of concurrent execution that owns a workload,
adapter, rate limiter, GK kernel, and metrics.
