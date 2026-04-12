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

>> If the significant digits can be configured, this should be exposed as a component level option.
>> In nosqlbench, it was stored on the component tree as a property so any component in the runtime could trace back towards the trunck to find its hdr digits, even if it was already at the end of a branch in the op template.
>> We need a good way to achieve a similar result here, and the component tree might be it.

```rust
pub struct Timer {
    labels: Labels,
    histogram: Histogram,  // HDR, delta-snapshotting
}
```

Used for: `service_time`, `wait_time`, `response_time`.

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
