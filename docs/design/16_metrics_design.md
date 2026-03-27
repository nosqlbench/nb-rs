# Metrics Subsystem — Design Sketch

Design for the nb-rs metrics collection and reporting layer.

---

## Design Principles

1. **No framework dependency.** Own the instruments: atomic counters,
   HDR histograms, rate computation from deltas.

2. **Frame-based capture.** All instruments are snapshotted atomically
   in a single pass per base interval, producing an immutable frame.
   Reporters consume frames, never live instruments.

3. **Hierarchical frame coalescing.** Reporters at different intervals
   share the same captured frames. Slower reporters receive frames
   that are the deterministic coalescing of faster frames — not
   re-snapshots. A 30s reporter sees the merged result of 30 × 1s
   frames, with HDR histograms merged for accurate quantiles.

4. **Dimensional labels.** Composable, hierarchic, OpenMetrics-aligned.
   Every metric carries labels from its component tree ancestry.

5. **Three-time model.** service_time, wait_time, response_time.
   Surfaces coordinated omission.

6. **Delta rates.** Rates are computed as deltas over the reporting
   interval. No EWMA — unambiguous interpretation.

7. **Dedicated scheduler thread.** Capture runs on its own thread.
   Each downstream reporting channel processes concurrently — no
   serializing multiple reporters on the same thread.

---

## Frame Capture Model

This is the core design element, carried forward from nosqlbench.

### Capture

One thread, one base tick (configurable, default 1s):

```
tick →
  1. Discover all metrics from component tree (single traversal)
  2. Snapshot every instrument in one pass:
     - Counters: read current value
     - Timers/Histograms: swap delta interval (HdrHistogram Recorder)
     - Gauges: call closure or read value
  3. Package into an immutable MetricsFrame
  4. Feed frame into the schedule tree
```

The frame is immutable once constructed. No reporter can mutate it
or affect live instruments through it.

### MetricsFrame

```rust
pub struct MetricsFrame {
    /// When this frame was captured.
    pub captured_at: Instant,
    /// Duration this frame covers (base interval).
    pub interval: Duration,
    /// All metric samples, grouped by family.
    pub families: Vec<MetricFamily>,
}
```

A `MetricFamily` groups samples by name and type (OpenMetrics
convention):

```rust
pub struct MetricFamily {
    pub name: String,
    pub family_type: FamilyType,  // Counter, Gauge, Summary
    pub unit: Option<String>,
    pub help: Option<String>,
    pub samples: Vec<Sample>,
}

pub enum Sample {
    Counter {
        labels: Labels,
        value: u64,
    },
    Gauge {
        labels: Labels,
        value: f64,
    },
    Summary {
        labels: Labels,
        count: u64,
        sum: f64,
        min: f64,
        max: f64,
        mean: f64,
        stddev: f64,
        quantiles: Vec<(f64, f64)>,  // (quantile, value)
        /// The raw HDR histogram for this interval, if available.
        hdr_histogram: Option<hdrhistogram::Histogram<u64>>,
    },
}
```

### Hierarchical Coalescing

Reporters register at different intervals. The scheduler builds a
tree where each node accumulates frames and coalesces them:

```
Base: 1s (capture)
  ├── Console reporter: 1s (receives every frame directly)
  ├── SQLite reporter: 5s
  │   └── accumulates 5 × 1s frames, coalesces, emits
  └── Prometheus reporter: 30s
      └── accumulates 6 × 5s coalesced frames, coalesces, emits
```

**Constraint:** all intervals must be exact multiples of the base.
This enables clean frame alignment.

**Concurrent delivery:** after coalescing, frames are delivered to
reporters concurrently — each reporter processes on its own thread.
A slow SQLite write never delays the console output.

### Frame Coalescing Rules

When combining N frames into one coalesced frame:

| Sample Type | Coalescing Rule |
|------------|-----------------|
| **Counter** | Sum the values (counts are additive) |
| **Gauge** | Weighted average by interval duration |
| **Summary count** | Sum |
| **Summary sum** | Sum |
| **Summary min** | Minimum across all frames |
| **Summary max** | Maximum across all frames |
| **Summary quantiles** | Merge HDR histograms via `Histogram::add()`, recompute quantiles from merged histogram |
| **Summary mean/stddev** | Recompute from merged count, sum, and sum-of-squares |
| **Delta rates** | Weighted average by interval duration |

The HDR histogram merge is critical: a 30s reporter gets quantiles
computed from **all 30 seconds of observations**, not an average of
per-second quantiles. This is what makes percentile reporting
accurate at coarser intervals.

When HDR payloads are not available (e.g., a reporter that doesn't
request them), quantiles are approximated via count-weighted
averaging of the per-frame quantile values.

### Key Guarantee

Multiple reporters at the same cadence receive the **exact same
frame object** (shared via Arc). No duplication, no re-snapshotting.
A 1s console reporter and a 1s HDR log writer both get the same
`Arc<MetricsFrame>`.

---

## Instrument Types

### Counter

```rust
pub struct Counter {
    labels: Labels,
    value: AtomicU64,
}
```

Monotonic. Rates computed by reporters from frame deltas.

### Histogram (HDR, delta-based)

```rust
pub struct Histogram {
    labels: Labels,
    recorder: Mutex<hdrhistogram::SyncHistogram<u64>>,
}
```

- `record(value_nanos)` — lock-free write to SyncHistogram
- `snapshot()` — swap interval, return delta histogram (all data
  since last snapshot)
- 3 significant digits (0.1% error), ~4KB per histogram
- The delta swap is the key operation: the Recorder gives us the
  interval histogram and resets its internal accumulator atomically

### Timer

Histogram + Counter. Records both a latency distribution and an
operation count per interval.

### Gauge

Function-based or settable. Sampled once per capture tick.

---

## Labels

```rust
pub struct Labels {
    pairs: Arc<Vec<(String, String)>>,
}
```

Immutable, `Arc`-shared for cheap cloning. Hierarchic composition
via the component tree:

```
Session:   {session="abc123"}
Activity:  {session="abc123", activity="write", driver="cql"}
Metric:    {session="abc123", activity="write", driver="cql", name="cycles_servicetime"}
```

### Labeling Contexts (from nosqlbench devdocs)

The component tree determines which labels a metric inherits:

- **Process level:** `appname="nosqlbench"`
- **Session level:** `session="<id>"`, `workload`, `scenario`
- **Activity level:** `activity="<alias>"`, `driver="<type>"`
- **Metric level:** `name="<metric_name>"`

### Categories

Semantic tags for metric filtering:
- **Core** — essential activity metrics (ops, latency)
- **Progress** — cycle tracking
- **Errors** — exception counts and rates
- **Driver** — adapter/client health
- **Internals** — CPU, memory, scheduling
- **Verification** — test validity
- **Config** — runtime configuration mirroring

---

## Component Tree

```rust
pub trait MetricSource: Send + Sync {
    fn labels(&self) -> &Labels;
    fn metrics(&self) -> Vec<Arc<dyn Metric>>;
    fn children(&self) -> Vec<Arc<dyn MetricSource>>;
}
```

```
Session
  ├── Activity "write"
  │   ├── Timer: cycles_servicetime
  │   ├── Timer: cycles_waittime
  │   ├── Timer: cycles_responsetime
  │   ├── Counter: cycles_total
  │   └── Counter: errors_total
  └── Activity "read"
      └── ...
```

Adapters register additional metrics (connection pools, queue depths)
through their activity's component node. Labels flow from parent to
child — an adapter metric inherits session + activity labels
automatically.

---

## Per-Operation Timing

```rust
// In the execution loop:
rate_limiter.acquire();  // may block
let wait_nanos = ...;

let service_start = Instant::now();
let result = op.execute();
let service_nanos = service_start.elapsed().as_nanos() as u64;

timers.service_time.record(service_nanos);
timers.wait_time.record(wait_nanos);
timers.response_time.record(service_nanos + wait_nanos);
counters.total.inc();
if result.is_err() { counters.errors.inc(); }
```

Standard quantiles: `[0.5, 0.75, 0.90, 0.95, 0.98, 0.99, 0.999]`

---

## Reporters

### Console

Delta rates and percentiles per interval:

```
── write (5.2s) ────────────────────
  cycles_servicetime  rate=10,068/s  count=52,341
    p50=1.2ms  p90=3.4ms  p99=12.1ms  p999=45.2ms  max=89.3ms
  errors_total  0
```

### CSV

One file per metric. Timestamp + stats columns.

### SQLite

Normalized schema aligned with OpenMetrics conventions:

```sql
CREATE TABLE metric_family (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    type TEXT NOT NULL,      -- counter, gauge, summary
    unit TEXT,
    help TEXT
);

CREATE TABLE label_key (id INTEGER PRIMARY KEY, key TEXT UNIQUE);
CREATE TABLE label_value (id INTEGER PRIMARY KEY, value TEXT UNIQUE);
CREATE TABLE label_set (
    id INTEGER PRIMARY KEY,
    hash INTEGER NOT NULL UNIQUE
);
CREATE TABLE label_set_entry (
    set_id INTEGER REFERENCES label_set(id),
    key_id INTEGER REFERENCES label_key(id),
    value_id INTEGER REFERENCES label_value(id)
);

CREATE TABLE metric_instance (
    id INTEGER PRIMARY KEY,
    family_id INTEGER REFERENCES metric_family(id),
    label_set_id INTEGER REFERENCES label_set(id),
    UNIQUE(family_id, label_set_id)
);

CREATE TABLE sample_value (
    instance_id INTEGER REFERENCES metric_instance(id),
    timestamp_ms INTEGER NOT NULL,
    interval_ms INTEGER NOT NULL,
    count INTEGER,
    sum REAL,
    min REAL,
    max REAL,
    mean REAL,
    stddev REAL,
    p50 REAL, p75 REAL, p90 REAL, p95 REAL,
    p98 REAL, p99 REAL, p999 REAL,
    hdr_payload TEXT  -- optional base64-encoded HDR histogram
);

CREATE TABLE session_metadata (
    key TEXT PRIMARY KEY,
    value TEXT
);
```

Label set deduplication via hash. Prepared statements for insertion
efficiency. Compatible with MetricsQL queries for post-run analysis.

### Prometheus Push

OpenMetrics exposition format, HTTP POST to push gateway.

### HDR Histogram Log

Standard `.hlog` format for offline analysis.

---

## Metric Filtering

```rust
pub struct MetricFilter {
    includes: Vec<LabelPattern>,
    excludes: Vec<LabelPattern>,
    default_accept: bool,
}
```

Exclude wins over include. Patterns match name regex or label
key=value pairs. Same semantics as nosqlbench.

---

## Crate Structure

```
nb-metrics/
├── src/
│   ├── lib.rs
│   ├── instruments/
│   │   ├── counter.rs
│   │   ├── histogram.rs
│   │   ├── timer.rs
│   │   └── gauge.rs
│   ├── labels.rs
│   ├── frame.rs          // MetricsFrame, Sample, coalescing
│   ├── scheduler.rs      // Capture thread, schedule tree
│   ├── component.rs      // MetricSource trait, tree traversal
│   ├── filter.rs
│   └── reporters/
│       ├── console.rs
│       ├── csv.rs
│       ├── sqlite.rs
│       ├── prometheus.rs
│       └── hdr_log.rs
└── Cargo.toml
```

Dependencies: `hdrhistogram`, `rusqlite` (optional), `reqwest`
(optional).

---

## Resolved Questions

- **Q26:** Dedicated scheduler thread. Each downstream reporter
  processes concurrently on its own thread — no serialization.
- **Q27:** Frames contain owned HDR histogram data (from Recorder
  interval swap). Multiple reporters at same cadence share the same
  `Arc<MetricsFrame>`. Coalescing merges histograms via
  `Histogram::add()` for accurate quantiles at coarser intervals.
- **Q28:** SQLite schema follows the nosqlbench normalized model,
  aligned with OpenMetrics conventions. See schema above.
- **Q29:** Adapters register metrics through the component tree,
  inheriting labels from their parent activity node.
- **Q30:** Delta rates — count change / interval duration.
- **Q31:** In-process automatons (optimizers, control loops) observe
  metrics by registering as consumers on the frame-based feed at 1s
  intervals — same mechanism as reporters. They are just another
  consumer node in the schedule tree. If 1s proves too expensive at
  scale, we can revisit with a direct instrument peek API that reads
  without disturbing the delta state. But start simple.
