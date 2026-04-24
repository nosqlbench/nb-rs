# 40: Metrics Framework

nb-metrics provides labeled instruments with delta-snapshot
semantics. Reporters consume immutable frames at configurable
intervals.

---

## Instruments

### Timer

HDR histogram recording nanosecond-precision durations.
Delta semantics: each `snapshot()` returns data since the
last snapshot and resets the accumulator.

```rust
pub struct Timer {
    labels: Labels,
    histogram: Histogram,  // HDR, delta-snapshotting
}
```

Used for: `service_time`, `wait_time`, `response_time`.

#### HDR significant digits — subtree-scoped setting

The precision of an HDR histogram is fixed at construction
time (bucket layout depends on `sigdigs`). Rather than
hard-coding the value or repeating it at every instrument
declaration, `sigdigs` is a **subtree-scoped setting**
resolved through the component tree:

1. **Session-root typed property.** At session bootstrap,
   the runner sets `hdr.sigdigs` as a typed
   `Component::property` on the session root, before any
   instrument is constructed. CLI / workload configuration
   feeds the initial value; absent override, the runner
   picks the project default.
2. **Walk-up resolution at instrument construction.** Every
   `Timer` / histogram resolves `hdr.sigdigs` by walking up
   the component tree from its own node until it finds the
   declared property — matching the nosqlbench pattern. No
   duplicate declarations needed down the tree.
3. **Branch-scoped control after bootstrap.** The same name
   is also declared as a branch-scoped control (SRD 23
   §"Branch-scoped and final controls") rooted at the
   session. A runtime write updates the committed value so
   **newly instantiated reservoirs** pick up the change;
   existing reservoirs keep their fixed bucket layout.
4. **No live reservoir swap.** Reconfiguring `hdr.sigdigs`
   mid-run does not reconstruct in-flight histograms. That
   would either lose accumulated data or force a lossy
   re-bucket. The contract is "future reservoirs see the
   new value" — operators who need the new precision
   applied to already-running instruments restart the
   owning component.

This resolves the tension between "I want to configure HDR
precision once at the session level" (property walk-up) and
"I want to adjust it during a long run" (branch-scoped
control) without inventing a separate mechanism for either.

### Counter

Monotonic `AtomicU64`. Read at snapshot time.

```rust
pub struct Counter {
    labels: Labels,
    value: AtomicU64,
}
```

Used for: `cycles_total`, `errors_total`, `stanzas_total`,
`result_elements`, `result_bytes`.

### Gauge

Instantaneous value, either settable (`ValueGauge`) or
function-based (`FnGauge`):

```rust
pub struct ValueGauge { labels: Labels, bits: AtomicU64 }
pub struct FnGauge { labels: Labels, f: Box<dyn Fn() -> f64> }
```

Used for: relevancy score means, system metrics.

---

## Summaries

Downstream of the primary instruments above, **summaries**
(`nb_metrics::summaries::*`) are retained, transforming views
of instrument data. A summary sits between a source (a primary
instrument, a scheduler-emitted snapshot, an observer callback)
and a reader (usually a display), and holds whatever
representation the reader needs for on-demand lookup.

Key distinctions vs. primary instruments:

- **Off the hot path.** Summaries are fed from outside the
  op-execution loop. `record`/`update` calls on a summary
  happen at scheduler-tick granularity or observer-callback
  rate, not per-op.
- **Retained across drains.** A primary histogram resets on
  `capture_delta`; a summary holds whatever representation it
  has built up (bounded by its capacity or window).
- **Transforming.** Each summary decides how to compact the
  stream — pairwise averaging, sliding-window aggregation,
  lossless sorted accumulation.
- **Read on demand.** Snapshots are non-mutating clones; a
  display can poll arbitrarily without perturbing the summary.
- **Scope-bounded lifetime.** Usually attached to a UI view,
  a phase, or an analysis window. When the source stops
  feeding it, the contents freeze naturally.

Current summaries:

| Type                          | Transform                                       | Used for                                         |
|-------------------------------|-------------------------------------------------|--------------------------------------------------|
| `BinomialSummary`             | bounded ring + pairwise-averaging on overflow   | per-phase throughput sparkline in the TUI        |
| `Ewma`                        | time-weighted exponentially-weighted average    | single-scalar "current rate / current value" readouts that shouldn't flicker |
| `F64Stats`                    | lossless sorted-vec accumulator + percentiles   | relevancy score accumulation (recall, precision) |
| `HdrSummary`                  | retained (non-draining) HDR histogram           | lossless latency percentiles over a scope (phase, run, report cell) |
| `LiveWindowHistogram`         | N-slot sliding-window HDR with lazy reset       | TUI 1 s rolling latency peek on `Timer`          |
| `PeakTracker`                 | monotonic-deque rolling max/min over a window   | 5 s / 10 s peak cross-bar markers on latency rows; any "high-water mark over last N" display |

See SRD 62 §"Design notes → Per-phase sparkline" for the
architectural position.

---

## Labels

Dimensional key-value pairs on every instrument:

```rust
pub struct Labels(Vec<(String, String)>);

impl Labels {
    pub fn of(key: &str, value: &str) -> Self;
    pub fn with(&self, key: &str, value: &str) -> Self;
}
```

Labels propagate: activity labels inherit session labels, timer
labels inherit activity labels plus metric name.

---

## MetricsFrame

Immutable snapshot of all instruments for one capture interval:

```rust
pub struct MetricsFrame {
    pub captured_at: Instant,
    pub interval: Duration,
    pub samples: Vec<Sample>,
}

pub enum Sample {
    Counter { labels: Labels, value: u64 },
    Gauge { labels: Labels, value: f64 },
    Timer { labels: Labels, count: u64, histogram: HdrHistogram<u64> },
}
```

### Coalescing

Multiple frames merge via `MetricsFrame::coalesce()`:
- Counters: summed
- Gauges: weighted average by interval duration
- Timers: histograms merged via `Histogram::add()` for accurate
  quantiles across the combined interval

For sample-weighted access to these coalesced windows from
consumers (TUI panels, reports, programmatic queries) see
[SRD 42 — Windowed Metrics Access](42_windowed_metrics.md).

### Standard Quantiles

`QUANTILES = [0.5, 0.75, 0.90, 0.95, 0.98, 0.99, 0.999]`

---

## Reporters

| Reporter | Output |
|----------|--------|
| Console | Periodic stderr summary |
| CSV | Per-interval CSV rows |
| OpenMetrics | Prometheus exposition format |
| SQLite | Persistent metric store |
| VictoriaMetrics | Remote push |

Reporters receive coalesced `MetricsFrame` at their configured
interval. Multiple reporters can run simultaneously at different
intervals.

---

## ActivityMetrics

Standard metrics created per activity:

```rust
pub struct ActivityMetrics {
    pub service_time: Timer,
    pub wait_time: Timer,
    pub response_time: Timer,
    pub cycles_total: Counter,
    pub errors_total: Counter,
    pub stanzas_total: Counter,
    pub result_elements: Counter,
    pub result_bytes: Counter,
}
```

Validation metrics (pass/fail counters, relevancy histograms) are
managed separately by `ValidationMetrics` and summarized at
activity completion.

---

## Component Tree

Every runtime layer is a `Component` in a parent-child tree.
Labels inherit downward; properties walk upward. The scheduler
captures delta snapshots from all RUNNING components.

```
Session (root)
  └── Scenario
        └── Phase (has InstrumentSet → ActivityMetrics)
              └── Dispenser (optional instruments)
```

### Component and ComponentState

```rust
pub struct Component {
    labels: Labels,              // own labels (e.g., phase="rampup")
    effective_labels: Labels,    // all ancestor labels merged with own
    props: HashMap<String, String>,  // inheritable properties
    parent: Option<Weak<RwLock<Component>>>,
    children: Vec<Arc<RwLock<Component>>>,
    state: ComponentState,
    instruments: Option<Arc<dyn InstrumentSet>>,
}

pub enum ComponentState {
    Starting,  // being initialized
    Running,   // actively captured by scheduler
    Stopping,  // final flush pending
    Stopped,   // no longer captured; cumulative view remains
}
```

### Attach / Detach

`attach(parent, child)` computes the child's effective labels
by extending the parent's effective labels with the child's own
labels. Sets the parent reference and adds the child to the
parent's children list.

`detach(parent, child)` removes the child from the parent's
children list and clears the child's parent reference.

### Property Walk-Up

`component.get_prop("hdr_digits")` checks the component's
own props first, then walks up to each ancestor until found.
Used for `hdr_digits`, `base_interval`, and other inheritable
configuration that affects instrument construction.

### InstrumentSet

The `InstrumentSet` trait abstracts over concrete instrument
collections. The component tree does not know about specific
instrument types — it only asks for a frame of delta samples:

```rust
pub trait InstrumentSet: Send + Sync {
    fn capture_delta(&self, interval: Duration) -> MetricsFrame;
}
```

`ActivityMetrics` implements this in nb-activity. The
`capture_delta` call resets internal delta accumulators
(histograms, F64Stats) and emits counter changes since the
last call.

---

## InProcessMetricsStore

In-process queryable metrics state. Fed by the scheduler at
every base tick. Maintains two views per component:

- **Cumulative**: merged total across the component's entire
  lifetime. Counters are summed, timer histograms are merged.
  Gauges are replaced with the latest value.
- **Last window**: the most recent delta snapshot. Replaced
  on every ingest.

```rust
pub struct InProcessMetricsStore {
    cumulative: HashMap<u64, (Labels, MetricsFrame)>,
    last_window: HashMap<u64, (Labels, MetricsFrame)>,
}
```

Keyed by label identity hash for O(1) lookup. All in-process
readers (GK `metric()` / `metric_window()`, summary report,
status line) query this store via read locks. External
reporters (SQLite, CSV) are separate consumers on the scheduler
pipeline — they do not maintain queryable state.

### Lifecycle Flush

When a phase completes, the executor calls
`flush_component(labels, final_delta)` on the store before
transitioning the component to `Stopped`. This ensures no
data is lost between the last scheduler tick and actual
completion. The flush merges the final delta into the
cumulative view.

---

## Scheduler

A dedicated thread captures frames at the base interval
(default 1s) from the component tree. The scheduler walks
the tree via `capture_tree()`, collecting delta frames from
all RUNNING components that have instruments.

```
capture_tree(root)
  → for each RUNNING component with instruments:
      capture_delta(interval) → (effective_labels, MetricsFrame)
```

### In-Process Store Feed

At every base tick, each component's delta is fed to the
`InProcessMetricsStore` via `ingest_delta()`. This maintains
per-component cumulative and last-window views.

### Hierarchical Frame Coalescing

External reporters are registered at their own intervals
(must be exact multiples of the base interval). The scheduler
uses a tree of `ScheduleNode`s to accumulate and coalesce
frames for slower reporters:

```
Root (1s base) → SQLite reporter (1s)
  └── Child (10s) → CSV reporter (10s)
        └── Child (60s) → Summary reporter (60s)
```

Frames flow from root to children. Each node accumulates
frames until its interval is satisfied, then coalesces and
emits. `MetricsFrame::coalesce` sums counters, merges timer
histograms, and weight-averages gauges.

### Shutdown

On stop, the scheduler performs a final capture and delivers
it to all reporters, then calls `flush()` on each reporter
for any buffered data.

The `StopHandle` also exposes `report_frame()` for the
executor to deliver lifecycle flush frames directly to
reporters outside the tick loop.

---

## GK Metric Functions

GK programs can read live metrics via two node functions:

- `metric(label_pattern, stat)` — reads the **cumulative** view
- `metric_window(label_pattern, stat)` — reads the **last window**

Both are non-deterministic context nodes (excluded from
constant folding). The store reference is captured at node
construction from a global static set by the runner.

### Label Pattern

Comma-separated `key=value` or `key~substring` filters. All
conditions must match:

```
metric("phase=rampup", "p99")
metric_window("phase~search", "rate")
```

### Stat Accessors

| Stat | Source | Description |
|------|--------|-------------|
| `"cycles"` | Counter `cycles_total` | Total cycles |
| `"errors"` | Counter `errors_total` | Total errors |
| `"rate"` | Counter `cycles_total` / interval | Ops/sec |
| `"p50"` | Timer `cycles_servicetime` | 50th percentile latency (ns) |
| `"p99"` | Timer `cycles_servicetime` | 99th percentile latency (ns) |
| `"mean"` | Timer `cycles_servicetime` | Mean latency (ns) |

These functions enable GK-driven control flow based on live
metrics — for example, a `do_while` condition that runs until
throughput stabilizes.
