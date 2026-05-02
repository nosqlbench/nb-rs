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
let fields = fiber.resolve_with_field_pulls(template, &field_pulls[idx]);
let pulls  = fiber.resolve_pulls(&pull_plans[idx]);
let ctx    = ExecCtx::new(&fields, &pulls);
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
async fn executor_task(activity, dispensers, field_pulls, pull_plans, program, ...) {
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
            // Two parallel index-driven materializations from the
            // same GkState — fields for the inner adapter, pulls for
            // wrapper-side reads (validation, conditional, throttle).
            // SRD 31 §"Pull plan vs bind plan".
            let fields = fiber.resolve_with_field_pulls(template, &field_pulls[idx]);
            let pulls  = fiber.resolve_pulls(&pull_plans[idx]);
            let ctx    = ExecCtx::new(&fields, &pulls);

            // Execute — yields to tokio during I/O
            let result = dispensers[idx].execute(ordinal, &ctx).await;

            // Record metrics, store captures
            record_metrics(&activity.metrics, &result);
            fiber.apply_captures(&result.captures);
        }
    }
}
```

---

## No Blocking Primitives in Async Contexts

A hard rule: **code reachable from a tokio worker (every
`async fn`, `tokio::spawn`, anything `.await`-driven) must never
block the OS thread on a synchronous primitive.**

### Forbidden in async paths

- `std::sync::Mutex::lock()`, `RwLock::read()` / `.write()` —
  any sync lock acquisition. The lock itself isn't the problem;
  *holding it across an `.await`* parks the worker for arbitrary
  time, and `std::sync::RwLock` on Linux is writer-preferring,
  so even nested same-thread reads deadlock when a writer is
  queued.
- `std::sync::mpsc::Receiver::recv()`,
  `crossbeam_channel::Receiver::recv()`,
  `flume::Receiver::recv()` — any sync `recv` blocks the OS
  thread. Use `tokio::sync::oneshot` / `mpsc` / `broadcast` and
  `.await` the receiver.
- `std::thread::sleep` — use `tokio::time::sleep(...).await`.
- `SyncSender::send` on a full bounded channel — use `try_send`
  with a non-blocking-drop policy, or a tokio channel + `.await`.

### Why the runtime can hang

The tokio worker pool has N threads (typically `num_cpus`).
Each worker runs the scheduler in a loop: pick a runnable task,
poll it, repeat. When a task `.await`s an async primitive that
isn't ready, the worker sets the task aside, registers a
wakeup, and picks another runnable task.

A sync block doesn't yield to the scheduler. The worker thread
sits in a syscall or cmpxchg loop until the OS unblocks it.
While stuck, the worker runs no other tasks.

If many async tasks block on sync primitives at once, *all*
workers can be stuck simultaneously. The runtime has no thread
free to drive the timer wheel or process I/O readiness. Tasks
that should wake on a deadline (e.g., `sleep(5ms).await`) never
get polled — the wakeup comes from the timer driver, but the
timer driver runs on the same worker pool. The runtime appears
deadlocked. From outside, the process is at 0% CPU with all
worker threads parked on `futex_wait_queue` / `epoll_wait`.

### The case that motivated this rule

`CadenceReporter::ingest()` was previously implemented as
"send command to actor + wait on ack via
`crossbeam_channel::Receiver::recv()`". Each per-phase ingest
pinned one tokio worker for a round-trip. With 100+ fibers
logging `fiber exit (normal)` through the same observer write
path during phase teardown, plus the phase boundary doing
ingest/close_path, all workers were pinned. The activity's
drain-loop `tokio::time::sleep(5ms).await` never got polled —
its wakeup needs the timer driver, which needs an idle worker.

The fix: `ingest` and `close_path` are fire-and-forget. The
actor processes commands in FIFO order, so a follow-up call
(next phase's ingest, shutdown_flush, etc.) sees prior effects
naturally. Deterministic publication at session shutdown is
provided by `shutdown_flush`, which is intentionally allowed
to block — it's called once at end of run, after all tokio
tasks have completed.

### Design rules for new code

1. **The fast path must not block.** Use an actor channel, write
   through an atomic, or `.await` a tokio primitive. Never
   sync `recv()`, never `lock()` across `.await`, never
   `thread::sleep` on a tokio worker.
2. **Synchronous semantics are a debugging affordance, not the
   default.** If tests need to observe an in-flight effect,
   expose a `flush_for_tests()` helper (documented
   non-production) that uses a sync ack.
3. **One blocking call per session is the ceiling, not the
   target.** `shutdown_flush` is the single sanctioned exception
   in nb-rs's metrics path. New components must not add another
   without a matching SRD justification.

### What's safe

- `parking_lot::Mutex` / `parking_lot::RwLock` for short, never-
  -across-`.await` critical sections. Both allow nested same-
  thread reads, so the writer-preferring deadlock pattern
  doesn't apply. They still block the OS thread; don't hold them
  across an `.await`.
- `tokio::sync::Mutex` / `RwLock` when the critical section
  *must* span an `.await`.
- Atomics (`AtomicU64`, `AtomicBool`, etc.) for hot-path
  counters and flags.
- `arc_swap::ArcSwap<T>` for lock-free atomic snapshot
  publication of read-mostly state — the canonical pattern for
  cross-component shared data (see SRD-42 §"Lock-free
  consolidation lifecycle").
- `crossbeam_channel` for cross-thread queuing as long as the
  *async side* uses `try_send` and the *sync side* (the actor
  thread) uses `recv()`. Never `recv()` from an async context.

### Watchlist for code review

- Any `recv()`, `lock()`, `read()`, `write()`, or
  `thread::sleep` inside an `async fn` or under `tokio::spawn`
  is a red flag.
- Any new public method that uses `crossbeam_channel` or
  `std::sync::mpsc` for cross-thread coordination must verify
  it doesn't `.recv()` from an async context.
- A `parking_lot::Mutex/RwLock` is fine but its `.read()` /
  `.write()` still block the OS thread. Use only when (a) the
  critical section is short and (b) it cannot run inside an
  `.await`. For state shared with async, prefer the actor +
  `arc-swap` pattern (SRD-42).

---

## Display and Diagnostic Decoupling

A second hard rule, orthogonal to async/blocking but in the same
spirit: **the display and diagnostic plane must be a strict actor
relative to the core machine. The two planes share no mutable
state. They communicate only through immutable snapshots
(downstream) and typed commands (upstream).**

The display plane includes: TUI, console progress lines, web
status pages, log file sinks, and any out-of-band introspection
endpoint. The diagnostic plane includes: `diag!()` writes,
session.log, panic-route hooks. Treat all of these the same way.

### Why this is a separate rule

The async/blocking rule keeps the runtime alive. This rule keeps
*observability* alive. They fail differently:

- A blocking-primitive bug parks tokio workers; the user sees a
  process at 0% CPU and asks "is it stuck?".
- A display-coupling bug freezes the UI alongside the core, so
  even when the user *knows* it's stuck, the very tool meant to
  show them why is frozen too. Diagnostics that go silent during
  a hang are the wrong shape of diagnostic.

The first rule is satisfied by the standard tokio playbook. The
second requires an explicit architectural choice — one most
projects accidentally violate by sharing an `Arc<RwLock<State>>`
between the renderer and the writer.

### The actor shape

```
        ┌─────────────────┐                  ┌────────────────┐
        │   Core machine  │                  │  Display plane │
        │ (RunState owner │                  │   (TUI / web / │
        │     actor)      │                  │  log sink / …) │
        │                 │   Arc<ArcSwap<   │                │
        │                 │     Snapshot>>   │                │
        │                 │ ───────────────► │  store.load()  │
        │                 │   (snapshot pub) │  (zero-wait)   │
        │                 │                  │                │
        │   inbox.recv()  │  mpsc::Sender    │                │
        │  (typed command │ ◄─────────────── │  inbox.send(   │
        │   match exhaust)│   <UiCommand>    │     UiCommand) │
        └─────────────────┘                  └────────────────┘
```

- **Downstream (core → display):** the core owns a private
  mutable `RunState`. On every change, it publishes
  `Arc::new(snapshot)` into a shared `arc_swap::ArcSwap<Snapshot>`.
  The display reads via `store.load()` — a single atomic
  operation that *cannot wait*. It always returns the most
  recently published `Arc<Snapshot>`. If the core is stalled,
  the display continues to render the last-good snapshot; the
  freshness staleness is itself a useful diagnostic signal.

- **Upstream (display → core):** every action the display can
  initiate is a variant of a typed `UiCommand` enum, sent over a
  bounded `mpsc::Sender<UiCommand>` into the core's actor
  inbox. The display uses `try_send` and surfaces `Full` /
  `Disconnected` in the snapshot ("core is backed up", "core
  exited"). It never blocks waiting for the core to drain.

- **No shared mutable state in either direction.** No
  `Arc<RwLock<RunState>>` shared between core and UI. No
  `Arc<Mutex<…>>` that both sides take. Type-safety falls out:
  the actor's `match cmd` is exhaustive, so a new UI capability
  cannot be added without the core handling it.

### Existing reference: the dynamic-controls path

The TUI's `e` keybinding routes through
`Control<T>::set(value, ControlOrigin::Tui).await` (SRD-23).
That path is already an actor: the control's applier runs on a
dedicated task, the `set` returns once the apply is queued, and
the display reads the post-apply value through the control's
ArcSwap-backed reify path. **Generalize this shape to every
upstream action**, don't reinvent it. New display capabilities
(request shutdown, pin selection, request stack dump, request
fiber-pool snapshot) become new `UiCommand` variants, never new
shared-state fields.

### The case that motivates this rule

The TUI froze together with a stalled core during an ann_query
phase teardown. Diagnostic instrumentation revealed the cause —
the renderer holds `Arc<RwLock<RunState>>::read()` to draw each
frame; the executor holds `Arc<RwLock<RunState>>::write()` from
its `RunObserver` callbacks. When the executor's worker stalled
mid-callback (waiting on a downstream actor), every TUI render
queued behind the held write guard. The very tool meant to show
the user the state of the machine was the first thing to go
silent. The fix is structural: replace the shared `RwLock` with
`(ArcSwap<RunState>, mpsc::Sender<UiCommand>)`, and route every
`RunObserver` event through the same actor inbox the UI sends to.

### Diagnostic sinks belong on a dedicated thread

`diag!()` writes and session.log appends must not run inline on
a tokio worker. A slow filesystem (NFS, a full disk, a
fsync-heavy snapshot) can stall the writer for seconds; in line
with the async/blocking rule, that stall pins a worker. Route
diagnostic writes through an mpsc channel into a dedicated
single-thread sink. The sink is the only thread that touches
the file. Producers `try_send` and drop on overflow with a
visible "log dropped" counter — never block waiting for the
sink to catch up.

### Out-of-band introspection

The "poke and prod the machine while it's stuck" surface (an
HTTP endpoint, Unix socket, or signal handler) must run on a
dedicated OS thread, not as a tokio task. If tokio is wedged,
the introspection thread is still alive. It interacts with the
core only through the same `UiCommand` channel and the
`ArcSwap<Snapshot>` reader — so it produces the same snapshots
the TUI sees, plus on-demand dumps (fiber-pool census, pending
inbox depth, last `RunObserver` event) that don't depend on the
runtime making forward progress.

### Design rules for new code

1. **Never share a `Mutex` or `RwLock` across the core/display
   boundary.** If you find yourself reaching for one, the
   correct shape is `(ArcSwap<Snapshot>, mpsc::Sender<Command>)`.
2. **Every UI→core action is a typed `Command` variant.** No
   "convenience" mutation paths through shared state, even when
   "it's just a flag".
3. **Display reads must be `O(load)`.** A snapshot read takes
   exactly one atomic load. If a render path needs a lock, the
   shape is wrong.
4. **Diagnostic writes don't run on tokio workers.** Route to a
   dedicated thread sink.
5. **An OOB introspection surface is a first-class deliverable.**
   It runs on its own OS thread and consumes the same
   snapshot/command channels as the UI.

### Watchlist for code review

- Any `Arc<RwLock<…>>` or `Arc<Mutex<…>>` reachable from both
  core code and display/diagnostic code is a red flag.
- A `RunObserver` impl that calls `.write()` on shared display
  state (instead of sending a command into an actor inbox) is a
  red flag.
- A `diag!` / log emission that writes synchronously to a file
  from a tokio worker is a red flag.
- A render path that takes more than one atomic operation to
  obtain its data is a red flag — it's almost certainly waiting
  on something it shouldn't.
