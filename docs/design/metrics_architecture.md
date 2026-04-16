# Design Memo: Metrics Architecture for nb-rs

## Problem

The current nb-rs metrics system has three fundamental defects:

1. **No accumulation** — Timer histograms use delta snapshots that reset every capture interval. The summary report queries the last sample, which can be empty if the phase ended between capture ticks. This caused the 0.00ns latency bug.

2. **No component tree** — Metrics are flat. There is no hierarchical structure linking instruments to the phases/activities that own them. Dimensional labels are assembled ad-hoc at construction time and cannot be queried structurally.

3. **No configurable time windows** — There is one capture cadence (1 second). Reporters cannot request different aggregation windows. There is no "total lifetime" accumulator.

NoSQLBench solved all three with a component-tree-rooted metrics architecture. This memo proposes an equivalent for nb-rs, adapted to Rust's ownership model and the GK/workload structure.

## Reference: How NoSQLBench Does It

### Component Tree as Name Index

Every runtime object (session, scenario, activity, phase) is an `NBComponent` with parent/child relationships. Each component carries its own `ConcurrentHashMap<String, NBMetric>`. Metrics inherit parent labels automatically — an activity's timer gets session + scenario + activity labels without explicit wiring.

### Hierarchical Snapshot Scheduling

`MetricsSnapshotScheduler` sits at the session root. It captures delta snapshots at a base interval (e.g., 100ms), then feeds them through a **divisor tree** of `ScheduleNode`s:

>> 100ms is WAY to high of a rate. The view of metrics in these windows is expected to be some minimum number of seconds at least. Further, we need to understand the accumulation modes of different snapshot durations. Is it true that we can numerically accumulate snapshots of differing durations, or do we need to enforce a geometric uniformity across accumulation levels?

**Resolution**: Base interval should be 1s minimum (default 1s). HDR histogram merging via `histogram.add()` is mathematically exact regardless of constituent interval uniformity — it sums raw bucket counts, so merging a 1s delta into a 3s delta produces the same result as three 1s deltas. Geometric uniformity is therefore NOT required for correct HDR accumulation. However, the weighted quantile fallback (when HDR payloads are unavailable) weights by observation count, not interval duration, which is correct for quantile estimation across non-uniform windows. The NoSQLBench divisor tree enforces that consumer intervals are exact multiples of the base interval, which simplifies bookkeeping without being a mathematical requirement.

```
base (1s) ─► sqlite (1s)
          ─► console (5s)   ── accumulates 5 base snapshots
          ─► csv (10s)      ── accumulates 2 console emissions
```

Each node accumulates via `MetricsView.combine()` — merging HDR histograms, weighted-averaging quantiles, summing counters, computing combined statistics with proper variance formulas.

### MetricsView: Immutable Snapshot with Merge

`MetricsView.capture(metrics, interval)` produces an immutable snapshot of all instruments. `MetricsView.combine(views)` merges multiple snapshots using type-specific accumulators:

- **Counters**: summed
- **Gauges**: weighted average by interval duration
- **Summaries (timers/histograms)**: HDR histogram merge when available, weighted quantile averaging as fallback, proper pooled variance computation

### Alive Gating

Components have lifecycle states (STARTING → RUNNING → CLOSING → STOPPED). The scheduler collects metrics by traversing the live component tree — dead components are detached, and their metrics disappear from the next capture automatically.

## Proposed Design for nb-rs

### 1. Component Tree

```rust
/// A node in the runtime component tree.
/// Every runtime entity (session, scenario, activity, phase) is a component.
pub struct Component {
    /// This component's own labels (e.g., phase="rampup").
    labels: Labels,
    /// Effective labels = parent labels + own labels. Computed on attach.
    effective_labels: Labels,
    /// Metrics instruments owned by this component.
    metrics: HashMap<String, Arc<dyn Instrument>>,
    /// Child components. Populated at runtime as phases start.
    children: Vec<Arc<RwLock<Component>>>,
    /// Lifecycle state. Instruments on STOPPED components are excluded from capture.
    state: ComponentState,
}

enum ComponentState { Starting, Running, Stopping, Stopped }
```

The component tree is the **canonical name index** for all metrics. Label-pattern queries traverse the tree, matching against effective labels. No separate metric registry needed.

>> We need to be specific about which types of components will be promoted to components in the tree. As long as the runtime cost is minimal it can span from the session all the way to the dispenser layer. But it would be too wasteful to make ops or stanzas components.

**Resolution**: Every GK context layer gets a component. This means phases ARE components, since each phase has its own GK compilation scope. The component tree spans:

```
Session
 └─ Scenario
     └─ Phase (one per phase execution, carries dimensional labels, owns GK context)
         └─ Dispenser (adapter execution layer)
```

Ops and stanzas (individual cycles) are NOT components — they are the hot path and should be zero-overhead. The phase is the natural boundary because it's where GK bindings are compiled and where dimensional labels (profile, k, etc.) are established. The dispenser hangs off the phase for adapter-specific instrumentation.

### 2. Instruments and Accumulation

Instruments are simple — they only produce delta snapshots:

```rust
pub trait Instrument: Send + Sync {
    /// Record a value (nanoseconds for timers, count for counters).
    fn record(&self, value: u64);

    /// Delta snapshot: data since last delta call. Resets the delta accumulator.
    fn delta_snapshot(&self) -> Snapshot;
}
```

The **cumulative (total) accumulator** lives in the **reporting layer**, not on each instrument. The scheduler maintains a running `MetricsView` per component that merges each delta snapshot into a cumulative view via `MetricsView.combine()`. This keeps instruments cheap (single accumulator) and centralizes the merging logic.

Two access modes for consumers:

- **Last window (addend)**: The most recent delta snapshot — what happened in the last capture interval. Cached after each capture tick so it's always available without re-snapshotting.
- **Total accumulator**: The merged cumulative view across the component's entire lifetime. Built by combining all deltas since the component started.

Both views are maintained by the scheduler, not the instruments themselves.

### 3. Snapshot Pipeline and Reporting Channels

All metrics consumers — external reporters (SQLite, CSV, console) and in-process readers (GK, summary report, TUI) — are `SnapshotConsumer` implementations on the same pipeline. There is no separate bypass path for in-process access.

```rust
/// A consumer of periodic metrics snapshots.
trait SnapshotConsumer: Send {
    /// Called each time the consumer's cadence interval is reached.
    fn on_snapshot(&mut self, view: &MetricsView);

    /// Whether this consumer needs HDR histogram payloads in the view.
    fn requires_hdr_payload(&self) -> bool { false }
}
```

One of these consumers is the **in-process metrics store** — a built-in consumer that maintains the queryable state for GK, the summary report, and any other in-process reader:

```rust
/// Built-in consumer that maintains queryable metrics state.
/// Registered at the base cadence so it sees every delta.
struct InProcessMetricsStore {
    /// Per-component cumulative views. Keyed by effective label set.
    cumulative: HashMap<Labels, MetricsView>,
    /// Per-component last window (most recent delta). Cached, never re-snapshots.
    last_window: HashMap<Labels, MetricsView>,
}

impl SnapshotConsumer for InProcessMetricsStore {
    fn on_snapshot(&mut self, view: &MetricsView) {
        // For each component's metrics in the view:
        // 1. Cache the delta as `last_window`
        // 2. Merge the delta into `cumulative` via MetricsView::combine()
    }

    fn requires_hdr_payload(&self) -> bool { true }
}

impl InProcessMetricsStore {
    /// Query cumulative views matching a label pattern.
    fn query_cumulative(&self, pattern: &TagFilter) -> Vec<(Labels, &MetricsView)> { ... }

    /// Query last window views matching a label pattern.
    fn query_last_window(&self, pattern: &TagFilter) -> Vec<(Labels, &MetricsView)> { ... }

    /// Flush a retiring component's final delta into its cumulative view.
    fn flush_component(&mut self, labels: &Labels, final_delta: MetricsView) { ... }
}
```

The in-process store is the **only** place cumulative and last-window state is maintained. External reporters (SQLite, CSV) receive their cadence-appropriate snapshots and persist them — they don't maintain queryable state. The summary report, GK `metric()`/`metric_window()` functions, and any future TUI or web dashboard all read from the in-process store.

This avoids duplicating accumulation state. The in-process store does the same `combine()` work that the schedule tree does for longer-cadence reporters, but it holds on to the result rather than discarding it after emission.

### 4. MetricsView: Capture and Combine

```rust
/// Immutable snapshot of all metrics at a point in time.
pub struct MetricsView {
    captured_at: Instant,
    interval: Duration,
    families: Vec<MetricFamily>,
}

impl MetricsView {
    /// Capture delta snapshots from all instruments on the component tree.
    fn capture(root: &Component, interval: Duration) -> Self { ... }

    /// Merge multiple views using type-specific accumulation.
    /// Counters: summed. Gauges: weighted average. Summaries: HDR merge.
    fn combine(views: &[MetricsView]) -> Self { ... }
}
```

The combine logic mirrors NoSQLBench's `SummarySampleAccumulator`:
- HDR histograms merged via `histogram.add()` when both sides have raw histograms
- Fallback: weighted quantile averaging when HDR payload unavailable
- Variance computed via pooled sum-of-squares formula
- Min/max preserved across merges

### 5. Accumulator Tree for Cadenced Reporters

```rust
struct SnapshotScheduler {
    base_interval: Duration,
    /// The in-process store. Always receives at base cadence.
    in_process: Arc<RwLock<InProcessMetricsStore>>,
    /// Cadence tree for external reporters.
    root: ScheduleNode,
}

struct ScheduleNode {
    interval: Duration,
    pending: Option<MetricsView>,
    accumulated: Duration,
    consumers: Vec<Box<dyn SnapshotConsumer>>,
    children: Vec<ScheduleNode>,
}
```

On each base tick:
1. Traverse the component tree, collect delta snapshots from all RUNNING instruments.
2. Build an immutable `MetricsView`.
3. Feed to the in-process store (always, at base cadence).
4. Feed into the `ScheduleNode` tree for cadenced external reporters.

The in-process store and the cadence tree share the same `MetricsView` input — no duplication of the capture step. The cadence tree exists solely for reporters that need accumulated windows at slower frequencies.

### 6. Alive Gating and Lifecycle Flush

When a component's scope ends (phase completes, activity tears down), the lifecycle retirement sequence is:

1. **Flush**: Take a final delta snapshot from all instruments on the component. Feed it to the in-process store (which merges it into the cumulative view) and to the cadence tree. This ensures no data is lost between the last capture tick and the actual completion.
2. **Transition**: Component state moves to `Stopped`.
3. **Visibility**: Stopped components remain in the tree and their cumulative views remain queryable in the in-process store. They are excluded from future delta captures. The component tree is the name index — stopped-but-present components are visible to label-pattern queries (e.g., the summary report).
4. **Detachment**: When no consumers hold references to a stopped component's cumulative view, the component is detached from the tree. Its metrics are gone.

### 7. In-Process Access (GK, Summary Report, TUI)

All in-process metrics reading goes through the `InProcessMetricsStore`:

```rust
// Summary report at end of run
let cumulative = store.query_cumulative(&tag_filter);

// GK metric() node — reads cumulative
let p99 = store.query_cumulative(&pattern)
    .first()
    .and_then(|(_, view)| view.summary("cycles_servicetime").map(|s| s.p99));

// GK metric_window() node — reads last delta
let current_rate = store.query_last_window(&pattern)
    .first()
    .and_then(|(_, view)| view.summary("cycles_servicetime").map(|s| s.rate));
```

The store is `Arc<RwLock<...>>` — reads are concurrent, writes happen only on the scheduler thread at base cadence. GK evaluation and reporters never contend with each other; they both read the store's immutable view references.

### 8. Configurable Cadences

Reporters register at construction with their desired cadence:

```yaml
# In workload YAML or CLI
metrics:
  base_interval: 1s
  reporters:
    - type: sqlite
      interval: 1s
    - type: console
      interval: 5s
    - type: csv
      interval: 1s
      fields: "cycles_total,cycles_servicetime"
```

The scheduler builds the divisor tree automatically. The in-process store is always present at base cadence regardless of what external reporters are configured.

## Migration Path

1. **Phase 1**: Introduce the Component struct with props, labels, lifecycle states. Attach to Session → Scenario → Phase → Dispenser hierarchy. Every GK context layer becomes a component.

2. **Phase 2**: Build the SnapshotScheduler with per-component cumulative views and last-window caching. Implement MetricsView capture and combine. Add lifecycle flush on component retirement.

3. **Phase 3**: Route reporters (SQLite, console, CSV) through the snapshot pipeline as consumers at configurable cadences. Summary report queries scheduler's cumulative views. SQLite becomes optional archival, not source of truth.

4. **Phase 4**: Add GK `metric()` and `metric_window()` node functions. Enable live metrics access for reactive control, reporting expressions, and interactive side-effects on the component tree.

## Key Differences from NoSQLBench

| Aspect | NoSQLBench (Java) | nb-rs (Rust) |
|--------|-------------------|--------------|
| Concurrency | ConcurrentHashMap, synchronized blocks | Arc<RwLock<Component>>, lock-free atomics for hot path |
| Histogram | Codahale Metrics + HdrHistogram Recorder | hdrhistogram crate, delta-only instruments; cumulative maintained by scheduler via combine() |
| Component lifecycle | GC handles detachment | Explicit Arc reference counting, detach on stop |
| Snapshot immutability | Defensive copies | Owned values, no interior mutability |
| View merging | `MetricsView.combine()` with accumulators | Same pattern, but `Snapshot` is an enum not a trait object |

>> We should set the minimum hdr histogram significant digits to 3 or 4 and make this an easily configurable setting within the scope of a component tree. For example if it is set on the session as 4 (the session is the root of the component tree) then all hdr histograms created should have that setting. Look at the decorator services within the NBComponent types in links/nosqlbench to see how this works. Find a way to do it in nbrs which is appropriate for this code base

**Resolution**: Component properties with walk-up inheritance. NoSQLBench uses `getComponentProp(name)` which checks the local node's `props` map, then walks up to the parent. We adopt the same pattern:

```rust
impl Component {
    /// Get a property, walking up the tree until found.
    fn get_prop(&self, name: &str) -> Option<String> {
        self.props.get(name).cloned()
            .or_else(|| self.parent.as_ref()?.read().ok()?.get_prop(name))
    }
}
```

HDR significant digits default to 3, configurable via `hdr_digits` property on any component. Set it on the session root and all descendant histograms inherit it:

```yaml
params:
  hdr_digits: "4"    # All histograms in this workload use 4 significant digits
```

At histogram construction: `let digits = component.get_prop("hdr_digits").and_then(|s| s.parse().ok()).unwrap_or(3);`

This same walk-up mechanism supports any inheritable configuration: base interval, error routing, adapter defaults, etc.

## Open Questions

1. **Cumulative view memory**: The scheduler maintains a merged cumulative `MetricsView` per component. Each view holds HDR histogram payloads from `combine()`. Per activity: 4 timer histograms. With 24 activities, that's 96 cumulative histograms in the scheduler (~3MB at 3 digits, ~12MB at 4 digits). This is in the scheduler, not duplicated per instrument, so it's one copy. Acceptable?

2. **Lock contention on component tree**: The hot path (record) should be lock-free. Only the capture path needs to read the tree. Should instruments be `AtomicU64` counters and lock-free histogram recorders?
>> The component tree will likely become an active signaling pathway in the future for components to send message (both sync and async messaging semantics) to each other, set options on each other, accumulate sparse events, and so on. It will be moderately active, but the rate of change in the component tree is meant to be low in general. It shall not be a high-bandwidth nervous system, just one that supports the non-hot path for configuration, metrics, and so on. In other words, while metrics may be very active, the underlying component tree that they hang off of will change relatively infrequently compared to metrics collection, etc.

**Resolution**: Two-tier design. The component tree uses `RwLock` (read-heavy, write-rare — structure changes only at phase boundaries). Instrument recording uses lock-free atomics and the hdrhistogram crate's `Recorder` (which provides a lock-free `record()` + synchronized `snapshot()`). The tree is the slow control plane; instruments are the fast data plane. Future signaling (sync/async messages, option propagation, sparse events) operates at tree-change frequency, not per-op frequency, so `RwLock` is appropriate.

3. **GK integration**: Should GK nodes be able to read live metrics from the component tree? This would enable expressions like `mean_latency := metric("cycles_servicetime", "phase=rampup").mean` for adaptive workloads.

>> Yes, we should be able to use gk as a metrics scripting and expression language for the purposes of live analysis and reporting expressions, etc. There will be a live evaluation context for some GK instances which are used in the future for reactive and interactive side-effects within the component tree, like signaling the session to shut down when an error condition is found by an asynchronous polling component, for example.

**Resolution**: GK gets two node functions for metrics access — one for each view:

- **`metric(pattern, stat)`** — reads the **cumulative** view. Total lifetime data for the matched component. Use for summary expressions, final reports, threshold checks.
- **`metric_window(pattern, stat)`** — reads the **last non-cumulative window** (most recent delta). Use for rate-of-change detection, live dashboards, adaptive control.

The last window is always cached by the scheduler after each capture tick, so `metric_window()` never triggers a re-snapshot — it reads the cached value.

This enables:

- **Reporting expressions**: `mean_latency := metric("cycles_servicetime", "phase=rampup").p99` for computed summary columns.
- **Reactive control**: `do_until` condition like `metric("errors_total").count > 100` that polls the cumulative view each iteration.
- **Rate detection**: `metric_window("cycles_servicetime", "phase=rampup").rate` to see current throughput in the last capture interval.
- **Live GK evaluation context**: Some GK programs (watchdogs, polling conditions, adaptive rate limiters) run on a separate evaluation cadence, reading the component tree as input. These are not per-cycle bindings — they're component-level reactive expressions attached to the tree.

Both are read-only non-deterministic nodes (like `elapsed_millis`) — they must be explicitly acknowledged in strict mode.