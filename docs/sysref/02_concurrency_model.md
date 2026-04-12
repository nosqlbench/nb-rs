# 02: Concurrency Model

nb-rs is async-native. All execution uses tokio tasks (fibers),
not OS threads. The concurrency model has two independent knobs
and no stride.

---

## Fiber Architecture

```
tokio runtime (num_cpus OS worker threads)
  │
  ├── Fiber 0 ── FiberBuilder (own GkState, CaptureContext)
  ├── Fiber 1 ── FiberBuilder
  ├── Fiber 2 ── FiberBuilder
  │   ...
  └── Fiber N ── FiberBuilder
        │
        │  Each fiber independently:
        │  1. Claims stanza from CycleSource (atomic)
        │  2. Resolves fields (sequential, uses GK state)
        │  3. Executes ops (concurrent within stanza window)
        │  4. Records metrics
        └── loop until cycles exhausted
```

Each fiber is a tokio task (~300 bytes). No shared mutable state
between fibers. The `GkProgram` is immutable and shared via `Arc`;
each fiber owns its own `GkState` and `CaptureContext`.

---

## Concurrency Knobs

### `concurrency=N` (fiber-level)

Number of tokio tasks executing stanzas concurrently. Each fiber
claims stanza-sized cycle ranges atomically from `CycleSource`
and processes them independently.

For I/O-bound workloads (CQL, HTTP), set this high: 100, 200,
or more. There is no performance penalty — tokio tasks are as
cheap as Java virtual threads.

The parameter is called `concurrency`, not `threads`. It controls
async fibers, not OS threads. Workloads set a default via
`params: { concurrency: "100" }`; CLI always overrides.

### `stanza_concurrency=M` (op-level)

Controls the maximum number of ops within a stanza that may
execute concurrently. Default: unlimited (all independent ops
fire concurrently).

### Linearization

Ops within a stanza may have **data dependencies**: op B
references a value captured from op A's result. These
dependencies create **linearization constraints** — op A must
complete before op B can resolve its fields.

The executor determines linearization from the op templates at
init time by analyzing capture declarations and capture
references:

```
ops:
  read_user:
    prepared: "SELECT [username] FROM users WHERE id = {id}"
    # declares capture: username

  update_user:
    prepared: "UPDATE users SET name = {capture:username} WHERE id = {id}"
    # references capture: username → must execute after read_user
```

The executor partitions ops into **dependency groups**: sets of
ops that can execute concurrently because they have no
inter-dependencies. Groups execute in sequence; ops within a
group execute concurrently.

```
Group 1: [read_user]           ← produces capture:username
  (barrier — apply captures)
Group 2: [update_user]         ← consumes capture:username
```

If no ops have capture dependencies, all ops in the stanza
execute concurrently regardless of `stanza_concurrency`. The
setting acts as an additional cap on group size, not as a
substitute for dependency analysis.

**Design status:** The current implementation uses
`stanza_concurrency` as a blunt window size. Automatic
linearization from capture analysis is the target design — it
eliminates the need for users to manually set `sc=1` for
dependent stanzas.

### Linearization and Error Handling

When an upstream op in a linearized chain fails:

- If the error is retried and succeeds: downstream ops proceed
  normally with the captured values
- If the error exhausts retries: downstream ops that depend on
  the failed capture cannot resolve their fields. The executor
  skips them and records errors for the entire dependency chain.
- Adapter-level errors halt the entire stanza (all ops affected)

This means a failed `read_user` produces errors for both
`read_user` and `update_user` — the downstream op is not
attempted with missing captures.

### Total In-Flight

```
max in-flight ops = concurrency × max_group_size
```

Typical configurations:
- Rampup (single INSERT): `concurrency=200` → 200 in-flight
- Read-then-write stanza: `concurrency=100` → 100 in-flight
  (groups of 1 due to capture dependency)
- Independent multi-op: `concurrency=50`, 4 independent ops
  → 200 in-flight

---

## Why Not Stride

Java nosqlbench used a `stride` parameter to pipeline multiple
cycles within a single thread. This was necessary because system
threads were expensive, and even virtual threads had scheduling
overhead limiting the practical fiber count.

nb-rs does not need stride. Tokio tasks are as cheap as Java
virtual threads. The idiomatic approach: increase `concurrency`
to get more in-flight requests.

```
# Java nosqlbench (stride was needed):
#   threads=4 stride=64  →  256 in-flight requests
#
# nb-rs (no stride needed):
#   concurrency=256       →  256 in-flight requests
```

---

## Cycle Source

`CycleSource` distributes cycle numbers to fibers via atomic
counter. Lock-free, zero contention on the hot path.

```rust
pub struct CycleSource {
    current: AtomicU64,
    end: u64,
}

impl CycleSource {
    pub fn next_n(&self, n: u64) -> Option<u64> {
        let base = self.current.fetch_add(n, Relaxed);
        if base + n <= self.end { Some(base) } else { None }
    }
}
```

Each fiber claims `stanza_length` cycles at a time. No batching,
no stride — just atomic increment.

---

## Rate Limiting

Two rate limiters operate independently:

- **`rate=N`** — cycle-level. Each op acquire before execution.
- **`stanzarate=N`** — stanza-level. Each stanza acquire before
  starting its ops.

Implementation: hybrid token/leaky bucket with three pools
(active, waiting, burst). Time-scaling trick keeps semaphore
within 32-bit bounds at any rate. 10ms refill interval.

Wait time from rate limiting is recorded as `wait_time` metric,
separate from `service_time` (adapter execution). The sum is
`response_time`.

---

## Tokio Worker Threads

`tokio::runtime::Runtime::new()` creates `num_cpus` OS worker
threads by default. These are the actual OS threads that poll and
drive async tasks. With `concurrency=200`, all 200 fibers
multiplex across these workers.

The `num_cpus` default is appropriate for mixed I/O + CPU
workloads: enough threads to saturate available cores during GK
resolution bursts, but not so many that context switching overhead
dominates. On high-core-count machines (32+ cores), the default
may be more than needed for purely I/O-bound workloads, but the
overhead of idle worker threads is negligible (each sleeps on
epoll).

For workloads with heavy per-cycle CPU (complex GK programs), all
cores contribute to resolution throughput. For pure I/O workloads,
a smaller worker count (4-8) would suffice but provides no
measurable benefit over the default.

The tokio worker count should be exposed as an optional
`worker_threads` parameter for tuning, but the default of
`num_cpus` is the right starting point for most workloads.

---

## Executor Task Structure

```rust
async fn executor_task(activity, dispensers, extra_bindings, dep_groups, program, ...) {
    let mut fiber = FiberBuilder::new(program);
    // Globals (workload params) are on the program — no separate params arg

    loop {
        let base_cycle = activity.cycle_source.next_n(stanza_len)?;

        // Stanza rate limit
        stanza_rl.acquire().await;

        fiber.reset_captures(base_cycle);

        // Process ops in windows of stanza_concurrency
        let mut offset = 0;
        while offset < stanza_len {
            let window = offset..min(offset + sc, stanza_len);

            if offset > 0 { fiber.apply_captures(); }

            // Phase 1: Resolve (sequential — mutable GK state)
            for i in window {
                fiber.set_inputs(&[base_cycle + i]);
                let fields = fiber.resolve_with_extras(template, &extras[idx]);
                batch.push((cycle, idx, fields, wait_nanos));
            }

            // Phase 2: Execute (concurrent — join_all)
            let results = join_all(batch.map(|op| dispenser.execute(...))).await;

            // Phase 3: Record metrics and captures
            for result in results { ... }

            offset = window.end;
        }
    }
}
```
