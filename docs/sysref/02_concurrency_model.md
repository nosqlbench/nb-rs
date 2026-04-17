# 02: Concurrency Model

nb-rs is async-native. All execution uses tokio tasks (fibers),
not OS threads. Concurrency is controlled by a single knob:
`concurrency=N` (number of fibers in flight).

---

## Fiber Architecture

```
tokio runtime (num_cpus OS worker threads)
  │
  ├── Fiber 0 ── FiberBuilder (own GkState)
  ├── Fiber 1 ── FiberBuilder
  ├── Fiber 2 ── FiberBuilder
  │   ...
  └── Fiber N ── FiberBuilder
        │
        │  Each fiber independently:
        │  1. Reserves a stride from the source (one atomic CAS)
        │  2. Renders + resolves each op (fiber-local GK state)
        │  3. Executes each op (sequential, declaration order)
        │  4. Records metrics
        └── loop until source exhausted
```

Each fiber is a tokio task (~300 bytes). No shared mutable state
between fibers during rendering or execution. The `GkProgram` is
immutable and shared via `Arc`; each fiber owns its own `GkState`.

---

## Source Dispatch: Reserve / Render / Execute

The source dispatch model has three distinct phases per stanza.
The critical design property: **shared state is touched only in
Phase 1**, and only via a single atomic operation. Phases 2 and 3
are entirely fiber-local — fibers never block on each other.

### Phase 1: Reserve (shared state — one CAS)

```rust
let range = source.reserve(stanza_len);  // fetch_add on AtomicU64
```

The source factory holds a shared `AtomicU64` cursor. Each fiber
calls `reserve(stride)` which does one `fetch_add` to atomically
claim a range of ordinals. This is the **only** shared-state
interaction per stanza — lock-free, ~10ns on x86.

The cursor lives on the `DataSourceFactory`, not on GK nodes:

```
Activity
  └── source_factory: Arc<dyn DataSourceFactory>
        └── cursor: Arc<AtomicU64>     ← the CAS volatile

Fiber 0                            Fiber 1
  └── reader: Box<dyn DataSource>    └── reader: Box<dyn DataSource>
        └── cursor: Arc<AtomicU64>         └── cursor: Arc<AtomicU64>
            (same Arc)                         (same Arc)
```

The factory creates per-fiber readers via `create_reader()`. Each
reader holds an `Arc` clone pointing to the same atomic cursor.
The factory itself lives on the `Activity` struct, which is
`Arc`-shared across all fibers.

**The GK graph does not own the cursor.** The GK `cursor` keyword
declares a source with a name and extent, but the runtime owns the
atomic state. GK nodes receive rendered `SourceItem` values — they
never interact with the atomic cursor directly.

### Phase 2: Render (fiber-local — no shared state)

```rust
let item = source.render_item(ordinal);   // fiber-local
fiber.set_source_item(&item);             // feed into GK state
let fields = fiber.resolve_with_extras(template, &extras);
```

For each ordinal in the reserved range, the fiber produces a
`SourceItem`. For range sources, this is trivial (the ordinal IS
the data). For dataset sources, this reads vector/metadata from
mmap'd storage — a fiber-local operation that touches no shared
mutable state.

The rendered item is fed into the fiber's GK state via
`set_source_item`, then the GK graph resolves all bindings for
the op template. The `FiberBuilder` owns its `GkState` — no
sharing, no locking.

### Phase 3: Execute (fiber-local — async I/O)

```rust
let result = dispenser.execute(cycle, &fields).await;
```

The resolved fields are dispatched to the adapter. For CQL, this
sends a prepared statement or batch. The `.await` yields the fiber
to tokio while waiting for I/O — other fibers run on the same
worker thread during this time.

### Sequential Stanza Execution

Ops within a stanza execute **sequentially in declaration order**.
A stanza is an inseparable dispatch unit:

```
Stanza (3 ops: create_keyspace, create_table, create_index):
  reserve [0..3) from source
  for ordinal in 0..3:
    render → resolve → execute → record metrics
```

Each op completes before the next begins. Captures from earlier
ops are available to later ops in the same stanza. This ordering
guarantee means DDL phases (where CREATE TABLE must precede
CREATE INDEX) work correctly without explicit dependency
annotations.

Concurrency happens **between fibers**, not within stanzas. Each
fiber processes its own stanzas sequentially; multiple fibers
process different stanzas concurrently.

---

## Stride and Work Distribution

The **stride** is the stanza length — the number of source items
a fiber acquires as an atomic unit. One stride = one stanza = one
pass through the op sequence.

All fibers pull strides from the same shared cursor:

```
1M items, concurrency=100, stanza_len=4

Time →
  Fiber  0: reserve [0..4)   → render → execute → reserve [400..404)   → ...
  Fiber  1: reserve [4..8)   → render → execute → reserve [404..408)   → ...
  Fiber  2: reserve [8..12)  → render → execute → reserve [408..412)   → ...
  ...
  Fiber 99: reserve [396..400) → render → execute → reserve [796..800) → ...
```

Because all fibers pull from the same counter, they naturally
interleave through the ordinal space in monotonic-similar order.
No fiber gets far ahead of others. This is important for workloads
where ordinal locality matters (e.g., time-series data where
adjacent ordinals represent adjacent timestamps).

Work stealing is not needed for the shared-cursor model — there
are no per-fiber partitions to exhaust. When dataset sources with
locality benefits (mmap prefetch) arrive, partitioned allocation
with stride-granularity work stealing can be implemented behind
the same `DataSource` trait.

---

## Concurrency Knob

### `concurrency=N` (fiber count)

Number of tokio tasks executing stanzas concurrently. Each fiber
claims strides atomically and processes them independently.

For I/O-bound workloads (CQL, HTTP), set this high: 100, 200,
or more. There is no performance penalty — tokio tasks are as
cheap as Java virtual threads.

Workloads set a default via `params: { concurrency: "100" }`.
CLI always overrides. Phases can override per-phase:
`concurrency: "{concurrency}"` or `concurrency: 1` (for DDL).

### Total In-Flight

```
max in-flight ops = concurrency
```

Each fiber has exactly one op in-flight at a time (sequential
stanza execution). With `concurrency=100`, there are at most
100 CQL statements or HTTP requests in flight simultaneously.

---

## Why Not Stride

Java nosqlbench used a `stride` parameter to pipeline multiple
cycles within a single thread. This was necessary because system
threads were expensive, and even virtual threads had scheduling
overhead limiting the practical fiber count.

nb-rs does not need stride as a user-facing parameter. Tokio tasks
are as cheap as Java virtual threads. The idiomatic approach:
increase `concurrency` to get more in-flight requests.

```
# Java nosqlbench (stride was needed):
#   threads=4 stride=64  →  256 in-flight requests
#
# nb-rs (no stride needed):
#   concurrency=256       →  256 in-flight requests
```

The stride concept still exists internally as the stanza length,
but it is determined by the workload's op count, not by a user
parameter. It is the indivisible unit of dispatch — not a
throughput multiplier.

---

## Rate Limiting

Two rate limiters operate independently:

- **`rate=N`** — cycle-level. Each op acquires before execution.
- **`stanzarate=N`** — stanza-level. Each stanza acquires before
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
workloads. For workloads with heavy per-cycle CPU (complex GK
programs), all cores contribute to resolution throughput. For pure
I/O workloads, a smaller worker count would suffice but provides
no measurable benefit over the default.

---

## Executor Task Structure

```rust
async fn executor_task(activity, dispensers, extra_bindings, program, ...) {
    let mut fiber = FiberBuilder::new(program);
    let mut source = activity.source_factory.create_reader();

    loop {
        // Phase 1: Reserve (one atomic op — shared state)
        let range = source.reserve(stanza_len)?;

        stanza_rl.acquire().await;
        fiber.reset_captures();

        // Phase 2+3: Render + Execute (sequential, fiber-local)
        for ordinal in range {
            let item = source.render_item(ordinal);
            fiber.set_source_item(&item);

            let (idx, template) = op_sequence.get_with_index(ordinal);
            let fields = fiber.resolve_with_extras(template, &extras[idx]);

            // Execute — yields to tokio during I/O
            let result = dispensers[idx].execute(ordinal, &fields).await;

            // Record metrics, store captures
            record_metrics(&activity.metrics, &result);
            fiber.apply_captures(&result.captures);
        }
    }
}
```
