# SRD 40: Op Execution Semantics

## Overview

This document defines the behavioral contract for op execution in
nb-rs: what happens when a cycle runs, how operations are selected,
how fields are resolved, what success and failure mean, and how
operations compose within a stanza.

## Cycle Lifecycle

A single cycle follows this path:

```
cycle (u64)
  │
  ├── rate limiter: acquire (blocks until budget available)
  │
  ├── template selection: op_sequence.get_with_index(cycle) → (idx, template)
  │
  ├── field resolution: fiber.set_coordinates([cycle]) → fiber.resolve(template) → ResolvedFields
  │     └── for each template field:
  │           bind points {name} → GK pull → typed Value
  │           capture points {capture:name} → CaptureContext lookup
  │           static text → passed through verbatim
  │           (string rendering is deferred until adapter requests it)
  │
  ├── execution: dispenser.execute(cycle, &fields) → Result<OpResult, ExecutionError>
  │     └── adapter-specific: HTTP request, CQL query, stdout print, etc.
  │
  ├── capture: store OpResult.captures in FiberBuilder.CaptureContext
  │
  ├── metrics: record service_time, wait_time, response_time, cycles_total
  │
  └── error handling: if Err, route through ErrorRouter → retry/warn/stop/count
```

## Stanza Semantics

A stanza is one full pass through all op templates in their sequenced
order. The stanza length is the sum of all op ratios.

**Stanza isolation:** The `CaptureContext` resets at stanza boundaries.
Captures from op N are available to op N+1 within the same stanza but
do not leak to the next stanza. This provides deterministic isolation.

**Cycle → stanza mapping:** `base_cycle = cycle_source.next_n(stanza_len)`.
All ops in a stanza share a contiguous cycle range. Op selection is
`cycle % stanza_len → LUT index`.

### Stanza Concurrency

`stanza_concurrency=N` (default 1, alias `sc=N`) controls how many
ops within a stanza execute concurrently. A single executor handles
all cases — no dual code path.

The executor processes stanza ops in **windows** of size N:
1. Apply captures from the previous window
2. Resolve all N ops in the window (sequential, uses GK state)
3. Execute all N ops concurrently (`join_all`)
4. Record metrics and store captures from results

With `stanza_concurrency=1`: each window is one op. Resolve one,
execute one, capture, next. This is sequential execution with capture
flow between every op — the default for workloads with inter-op data
dependencies (e.g., INSERT then SELECT to verify).

With `stanza_concurrency=N>1`: windows of N ops resolve and then
execute in parallel. Captures flow between windows but not within
them. Use for workloads where ops within a stanza are independent and
throughput matters more than ordering.

With `stanza_concurrency=stanza_length`: all ops in the stanza fire
concurrently. Maximum throughput, no inter-op capture flow.

## Adapter Resolution

The adapter for each op template is resolved at init time, not at
cycle time. Different ops within a stanza may use different adapters.
For example, a verification workload might INSERT via CQL then
read-back via HTTP to confirm the data is visible through the API.

The op synthesis pipeline inspects each template's `adapter` or
`driver` param (if present) and dispatches `map_op()` to the
appropriate adapter. Templates without an explicit adapter use the
activity's default. This produces a per-template dispenser array
where each dispenser may be backed by a different adapter.

```yaml
ops:
  insert:
    adapter: cql
    stmt: "INSERT INTO t (id, v) VALUES ({id}, {val})"
  verify:
    adapter: http
    method: GET
    uri: "http://api:8080/items/{id}"
```

Both ops share the same GK bindings and cycle space. The executor
resolves fields identically for both and dispatches to the correct
dispenser based on the template index.

## Template Selection

The `OpSequence` maps `cycle → ParsedOp` using a precomputed lookup
table (LUT). Three sequencing strategies:

- **Bucket** (default): interleaved. A 3:1 read:write ratio gives
  `R R R W R R R W ...` with even spacing.
- **Interval**: similar to bucket but uses fractional positioning for
  more uniform distribution.
- **Concat**: sequential blocks. `R R R W W` then repeat.

The LUT is built at init time and indexed at O(1) per cycle.

## Field Resolution

Template fields are `serde_json::Value` entries in `ParsedOp.op`.
String-typed fields may contain bind points: `{name}`, `{bind:name}`,
`{capture:name}`, `{coord:name}`.

Resolution order for unqualified `{name}`:
1. GK binding output
2. Capture context
3. Coordinate value (raw u64)

The resolved result is `ResolvedFields`:
- `names: Vec<String>` — field names
- `values: Vec<Value>` — typed GK values (preserving u64/f64/bool/string/bytes)
- `strings()` — lazily rendered on first access
- `to_json()` — serialize all fields as JSON for diagnostic/logging use

Adapters choose which access path to use:
- CQL: `get_value()` for native type binding — never triggers string rendering
- HTTP/stdout: `get_str()` or `strings()` for text rendering
- Diagnostics: `to_json()` for human-readable output

## Result Processing

A successful op is not merely "the driver returned OK." The result
data must be fully consumed — all rows iterated, all bytes read, all
response body received. This is the semantic contract: when the
executor records an op as complete, the data has been verified as
received.

### Result Traversal Wrapper

Result consumption is implemented as a composable op behavior
(decorator/wrapper) around the core dispenser execution:

```
dispenser.execute(cycle, fields)
  → native result (e.g., CQL ResultSet, HTTP Response)
    → ResultTraverser wrapper (always on, highest priority)
      → traverses/consumes the native result
      → counts rows, bytes, elements for metrics
      → if captures needed: retains a representation
      → if no captures: discards data after counting
    → OpResult { body, captures }
```

The result traverser is:
- **Always on by default.** It is the first wrapper behavior by
  priority for any operation.
- **Runtime configurable.** Can be disabled for dry-run or diagnostic
  modes where result consumption is unnecessary.
- **Non-interfering.** Errors from core op execution flow through
  unchanged. The traverser only wraps successful results.

### Consume vs Retain

Traversal may or may not retain the result data:

- **Consume-only** (no captures, no verification): The traverser
  iterates through the result, counts elements (rows, bytes,
  records), and discards the data. Metrics reflect the full
  consumption cost. No allocation for result retention.

- **Retain for capture** (downstream ops need fields): The traverser
  produces a retained representation that capture extraction can
  read from. For CQL this might be a `Vec<CqlRow>`. For HTTP this
  might be the parsed JSON body.

The adapter's `ResultBody` implementation decides the retained form.
The traverser calls the appropriate consumption method based on
whether captures are declared for this op template.

### Field Capture from Results

Capture extraction should use the native result type when possible.
Each adapter implements capture in the way appropriate for its
protocol — CQL reads typed column values directly, HTTP extracts
JSON fields, etc.

A **naive fallback** is provided for any adapter that implements
`ResultBody::to_json()`: the framework can extract captures using
JSON path syntax on the serialized result. This gives every adapter
basic capture support for free, even if not optimized for the
protocol's native types.

```
Native capture:   result.as_any() → downcast → read typed fields
Naive capture:    result.to_json() → json_path("$.rows[0].user_id") → Value
```

### Op Wrapping Selection

It is up to the op synthesis pipeline to choose an appropriate set of
op wrapping behaviors to support downstream requirements. The pipeline
inspects the op template's declarations (capture points, verification
assertions, diagnostic flags) and composes the correct decorator
stack. For example:

- Template with `[name]` capture points → result traverser retains
  data + capture extractor wrapper
- Template with no captures → result traverser consume-only (count,
  discard)
- Dry-run mode → no result traverser at all
- Verification assertions → result traverser + assertion wrapper

The wrapping decision is made at init time (`map_op`), not per-cycle.

## Success and Failure

An op execution returns `Result<OpResult, ExecutionError>`.

**Success** (`Ok(OpResult)`):
- `body: Option<Box<dyn ResultBody>>` — adapter-specific result, `.to_json()` for
  universal access, `.as_any()` for adapter-internal downcast
- `captures: HashMap<String, Value>` — extracted capture values

There is no `success: bool` or `status: i32` on OpResult. If you have
an `Ok(OpResult)`, the operation succeeded. Protocol-specific status
codes (HTTP 200, CQL success) are internal to the adapter's
`ResultBody` implementation — they are not a concern of the execution
engine. Status-to-metric mapping, if needed, is handled via the op
decorator pipeline (SRD 33), not on the generic result type.

**Failure** (`Err(ExecutionError)`):
- `ExecutionError::Op(AdapterError)` — per-op, template-specific, retryable
- `ExecutionError::Adapter(AdapterError)` — adapter-wide, connection-level

The adapter decides what constitutes an error vs success. HTTP 4xx/5xx
are errors. CQL query failures are errors. IO failures are errors.

## Retry Semantics

When an op fails with an `Op`-level error and the error router allows
retry, the executor re-executes with the **same resolved fields**.
Fields are not re-resolved — the GK state for this cycle is frozen.

`Adapter`-level errors are never retried — they indicate a
connection-wide problem that won't be fixed by retrying the same op.

Maximum retries: `ActivityConfig.max_retries` (default 3).

After exhausting retries, the cycle is counted as failed and the
executor moves to the next cycle.

## Default Cycles

When `cycles` is not specified, it defaults to one full stanza
(the sum of all op ratios). This ensures every op template executes
at least once.

## Concurrency Model

Two independent concurrency knobs:

- **`concurrency=N`** (fiber-level): N tokio tasks, each owning a
  `FiberBuilder`. Tasks claim stanza-sized cycle ranges atomically
  from `CycleSource`. Fully concurrent across stanzas.

- **`stanza_concurrency=M`** (op-level): within each fiber, ops in a
  stanza execute in windows of M. Single executor, no dual code path.

Total in-flight ops at peak = `concurrency × stanza_concurrency`.

The parameter is called `concurrency`, not `threads` — it controls
async fibers, not OS threads. Workloads can set a default via
`params: { concurrency: "100" }`; CLI always overrides.

For I/O-bound workloads (CQL, HTTP), set `concurrency` high — 100,
200, or more. Each fiber is a lightweight tokio task (~300 bytes),
not an OS thread. There is no performance penalty for high values.

### Why Not Stride

Java nosqlbench used a `stride` parameter to pipeline multiple
cycles within a single thread. This was necessary because system
threads were expensive, and even virtual threads had scheduling
overhead that limited the practical fiber count. Stride let a small
number of threads achieve high request concurrency by batching.

nb-rs does not need stride. Tokio tasks are as cheap as Java virtual
threads. The idiomatic Rust approach: increase `concurrency` to get
more in-flight requests. Each fiber independently claims stanzas
from the atomic cycle source and executes them. No batching, no
stride, no complexity — just more tasks.

```
# Java nosqlbench (stride was needed):
#   threads=4 stride=64  →  256 in-flight requests
#
# nb-rs (no stride needed):
#   concurrency=256       →  256 in-flight requests
```

### Tokio Worker Threads

The tokio multi-threaded runtime uses `num_cpus` OS worker threads
by default. These are the actual OS threads that poll and execute
async tasks. With `concurrency=200`, all 200 fibers are multiplexed
across these worker threads.

For I/O-bound workloads, the default is almost always correct: tasks
spend most of their time waiting on network I/O, so a few OS threads
can service hundreds of tasks efficiently. The GK resolve step is
CPU-bound but takes microseconds — negligible per cycle.

If a workload has unusually heavy per-cycle CPU work (complex GK
programs), the tokio worker count can be tuned at runtime creation.
This is not exposed as a user parameter — the default suffices for
all current workloads.

## Determinism

For the same `(cycle, template)` pair:
- GK resolution is deterministic (same coordinate → same output)
- Field resolution is deterministic (same bind points → same values)
- Adapter execution may be non-deterministic (network, timing)

Deterministic field generation means workloads are reproducible.
The same cycle number always produces the same request payload.
