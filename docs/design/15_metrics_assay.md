# Metrics Subsystem — Assay of Java nosqlbench

Study of the Java nosqlbench metrics system to inform the design of
nb-rs metrics. This documents what exists, what worked, what was
complex, and what we should carry forward.

---

## Architecture Overview

The Java metrics system has five layers:

```
Instruments (timers, counters, gauges, histograms, meters)
      │
      ▼
Component Tree (NBComponent hierarchy, label composition)
      │
      ▼
Snapshot Pipeline (MetricsView normalization, cadence scheduling)
      │
      ▼
Reporters (console, CSV, SQLite, Prometheus push, HDR log)
      │
      ▼
Post-Run Analysis (MQL queries against SQLite)
```

Built on Dropwizard Metrics (Codahale) with significant custom
extensions, primarily around HDR Histogram integration and delta-based
snapshot management.

---

## 1. Instrument Types

Five core types, all implementing an `NBMetric` interface that adds
dimensional labels, descriptions, units, and categories.

| Type | What It Measures | Underlying |
|------|-----------------|------------|
| **Counter** | Monotonic event count | Codahale Counter |
| **Timer** | Latency distribution + rate | Codahale Timer + Delta HDR Histogram |
| **Histogram** | Value distribution (no rate) | Codahale Histogram + Delta HDR Histogram |
| **Meter** | Event rate (EWMA) | Codahale Meter |
| **Gauge** | Instantaneous value | Lambda or settable value |

The Timer is the workhorse — it records per-operation latency and
provides percentiles (p50, p75, p90, p95, p98, p99, p999), mean,
min, max, stddev, count, and rate (1m, 5m, 15m moving averages).

---

## 2. HDR Histogram Integration (the key extension)

### Why Delta Snapshots

Standard Dropwizard histograms use exponentially-decaying reservoirs
that lose precision for recent data. nosqlbench replaces this with
**Delta HDR Histogram Reservoirs** (`DeltaHdrHistogramReservoir`):

- Each update records to an HdrHistogram `Recorder` (lock-free write)
- On snapshot: swap the interval histogram (atomic), return the delta
  since last snapshot
- Multiple reporters can snapshot independently without competing
- Preserves full HDR precision (configurable significant digits)

### Mirror Attachment Pattern

A timer can have "mirror" histograms attached — copies that receive
the same updates but maintain independent snapshot state. This allows
the HDR interval logger to consume its own delta without interfering
with the console reporter's delta.

### What This Means for nb-rs

Delta HDR histograms are essential for accurate percentile reporting
under load. The Rust `hdrhistogram` crate provides the same core
functionality. The key design to port: the delta snapshot + multiple
independent consumer model.

---

## 3. Dimensional Labels (NBLabels)

Every metric carries a set of key-value labels for identification:

```
{session="abc123", activity="write", name="cycles_servicetime"}
```

Labels are:
- **Immutable** after construction
- **Hierarchic** — child components inherit parent labels
- **Composable** — `parent.labels().and("name", "cycles_servicetime")`
- **Renderable** — multiple serialization modes (dot-separated, OpenMetrics `{key="val"}`, etc.)

### Categories

Metrics are tagged with semantic categories:
- **Core** — essential activity metrics (ops, latency)
- **Progress** — cycle/progress tracking
- **Errors** — exception counts and rates
- **Driver** — external client health
- **Internals** — CPU, scheduling, memory
- **Verification** — test result validity
- **Config** — runtime configuration mirroring

Categories enable filtering: "show me only error metrics" or "exclude internals from console output."

---

## 4. Snapshot Pipeline

### MetricsSnapshotScheduler

The central scheduling mechanism. Reporters register at different
intervals (console every 1s, SQLite every 5s, Prometheus every 30s).
The scheduler builds a tree that captures at the base interval and
aggregates upward:

```
Base: 1s capture
  ├── Console: 1s (every capture)
  ├── SQLite: 5s (aggregate 5 captures)
  └── Prometheus: 30s (aggregate 30 captures)
```

**Constraints**: all intervals must be exact multiples of the base.
The scheduler rebuilds the tree if a new reporter needs a finer base.

### MetricsView (normalization boundary)

Raw NBMetric instruments are converted into OpenMetrics-aligned
`MetricFamily` structures:

- Counters → counter type with `_total` suffix
- Timers → summary type with quantile samples
- Meters → gauge type with rate suffixes
- Sanitized names (valid OpenMetrics identifiers)
- Label filtering (remove internal labels from output)

The MetricsView is immutable — reporters receive a frozen snapshot
they can process without locking.

---

## 5. Reporters

| Reporter | Output | Cadence | Special |
|----------|--------|---------|---------|
| Console | stdout/stderr | Human-readable | ASCII art alignment |
| CSV | Directory of CSVs | Per-metric files | Time-series columns |
| SQLite | Local .db file | Normalized schema | MQL queryable |
| Prometheus Push | HTTP endpoint | Push gateway | Bearer token auth |
| HDR Interval Log | .hlog file | HdrHistogram format | Full distribution |
| HDR Stats Log | .csv file | Stats per interval | Percentile columns |

### SQLite Schema

Fully normalized relational schema:
- `metric_family` — types and metadata
- `sample_family` — sample names per family
- `label_key/value/set` — normalized dimensions with deduplication
- `metric_instance` — instances bound to label sets
- `sample_value` — time-series data points

This enables SQL-based post-run analysis: "what was the p99 latency
for writes in the last 5 minutes of the run?"

---

## 6. Per-Operation Timing

The engine instruments each operation through the `BaseOpDispenser`:

```
start timer
  → dispense op from template
  → execute op
  → record result
stop timer

If error:
  → increment error counter by exception type
  → increment error meter (for rate tracking)
```

Key metrics per activity:
- `cycles_servicetime` — wall-clock execution time per cycle
- `cycles_responsetime` — includes rate-limiter wait delay
- `cycles_waittime` — accumulated scheduling delay
- `strides_servicetime` — per-stride (batch of cycles)
- `bind`, `execute`, `result` — sub-phase timers

---

## 7. Rate Limiting Metrics

The rate limiter (token bucket) exposes:
- **waittime** — time blocked waiting for a token
- **servicetime** — actual operation time
- **responsetime** — waittime + servicetime (user-perceived)

These three form the "coordinated omission" response: if the system
can't keep up with the target rate, waittime grows and responsetime
diverges from servicetime.

---

## 8. Metric Filtering

Tri-state filter system for reporters:
- **Default**: allow all (or deny all with `default=false`)
- **Include**: regex on name or label patterns
- **Exclude**: `!pattern` or `-pattern`
- **Precedence**: exclude wins over include

Example: `default=false;name=op_latency;!stat=mean` — only show
op_latency metrics, but hide the mean statistic.

---

## 9. Post-Run Analysis (MQL)

SQL-based query language against the SQLite metrics database:
- `summary` — comprehensive session report
- `quantile` — percentile analysis
- `topk` — find highest-value metrics
- `rate` — throughput analysis
- `aggregate` — custom aggregations

The SQLite file is portable — can be queried with any SQL tool after
the run.

---

## 10. What Worked Well

1. **Delta HDR histograms** — essential for accurate percentile
   reporting. The delta snapshot model cleanly separates producers
   from consumers.

2. **Dimensional labels** — flexible, composable, hierarchic. Enables
   rich querying without predefined naming conventions.

3. **SQLite persistence** — queryable post-run analysis without
   external infrastructure. Portable, self-contained.

4. **Metric categories** — semantic filtering without knowing specific
   metric names.

5. **OpenMetrics alignment** — compatible with Prometheus ecosystem
   without adaptation layers.

## 11. What Was Complex or Problematic

1. **Dropwizard dependency weight** — pulls in a large Java library
   for what amounts to counters, rates, and histograms. Many features
   go unused.

2. **Cadence scheduler complexity** — the hierarchical interval tree
   with exact-multiple constraints is hard to reason about. A simpler
   "tick at base, emit when due" approach might suffice.

3. **Mirror histogram management** — the attachment pattern is
   powerful but tricky to use correctly. Lifecycle management of
   mirrors is error-prone.

4. **SQLite write contention** — at high reporting frequencies
   (<100ms), SQLite becomes a bottleneck. Buffering helps but adds
   latency.

5. **Snapshot allocation** — each MetricsView snapshot allocates new
   collections. At high frequency this generates GC pressure.

6. **MQL scope** — the full SQL query language is powerful but rarely
   used beyond basic summary. Most users just want "show me the p99."

---

## 12. Implications for nb-rs

### Must have
- HDR histograms with delta snapshots (use `hdrhistogram` crate)
- Dimensional labels (lightweight, composable)
- Timer, counter, gauge instrument types
- Console and file reporters
- Per-operation timing with service/response/wait time decomposition

### Should have
- SQLite persistence for post-run analysis
- Prometheus push/exposition support
- Metric categories for filtering
- HDR histogram log output

### Could simplify
- Drop EWMA rate meters — compute rates from counter deltas instead
  (simpler, no exponential decay confusion)
- Simplify cadence scheduling — single base tick, reporters check
  their own interval
- Skip mirror histogram pattern — use a single shared snapshot with
  reference counting
- Lean on Rust ownership model instead of Dropwizard's thread-safety
  patterns
