# Cumulative-counter model (Prometheus/VM-aligned) — change set

**Status:** IMPLEMENTED 2026-06-18, then simplified to **CUMULATIVE-ONLY** the same day.
`CounterValue` carries a SINGLE `cumulative` field (the monotonic running total). The
short-lived "counter-carries-both" `total` (per-window delta) field was **deleted** — it was
a second semantic alongside the canonical cumulative and the root of a bug class (the
`total`-summing query-side time-fold overcount; the `delta`-in-`component.rs`
vs `absolute`-in-`activity.rs::snapshot` capture inconsistency). A counter is now just its
cumulative; **per-interval deltas are DERIVED at the consumer** by differencing samples
(the metricsql engine, the sqlite `_rate` = `count − LAG(count)`, windowed-throughput
readers off the finest ring). `combine_into` keeps `CombineMode {Coalesce, Aggregate}` but
with nothing to reconcile: counter `cumulative` is **latest-by-timestamp** on `Coalesce`
(the time dimension — the cadence cascade AND the query-side `session_lifetime` /
`increase_over` / `distribution_over`) and **summed** on `Aggregate` (cross-component).
Capture is always the
instrument's absolute (`Counter::get()`); the `drain` flag governs only histogram/timer
reservoirs. The finest-cadence ring (cap 32 ≥ max fan-in 20) is retained independently of
the cascade fold, so a continuous/sliding lookback up to the next cadence's interval is
always derivable from the running totals. `optimizer_metricsql.yaml` uses
`sum(rate(errors_total[3s]))` (best [2]). All suites + 44 workload examples green.

> NOTE: the "carries both" framing below is HISTORICAL. The dual `total`/`cumulative`
> design was superseded by cumulative-only — read it as the path taken, not the end state.

**Histogram/timer count closed 2026-06-18.** The same model now covers the histogram/timer
*count*: the instrument carries a monotonic lifetime `total()` (the HDR reservoir still
drains per window for the distribution), capture stamps it onto
`HistogramValue::cumulative_count`, `combine` splits it Coalesce=last / Aggregate=sum like the
counter, and both backends expose it (live `value_to_f64` → `cumulative_count`; sqlite reporter
writes it as the stored count). So `rate()`/`increase` over a histogram count is PromQL-correct
on either backend. The per-window *distribution* (percentiles) stays windowed — Prometheus's
cumulative-*bucket*-counter alignment remains a separate follow-on (nb-rs uses HDR reservoirs).

Scopes the move from delta-source to cumulative-source counters so the stored form (sqlite) is numerically a
Prometheus/VictoriaMetrics-style series, MetricsQL aggregate rules are exact, and
the **same metricsql expression works on either backend (live or sqlite)**.

## Consumer-side derivation surface — total vs increase vs distribution

Because a counter is now only its running total, every change-over-time view is a
**consumer-side derivation off the running totals**, and the `MetricsQuery` read surface is
named so the method identifier says which kind of value it returns:

| Reader | Returns | PromQL stem |
|---|---|---|
| `now` / `cadence_window` / `session_lifetime` | running **total** (cumulative snapshot) | — |
| `increase_over(span, sel)` | counter **increase** over the span, `cum[now] − cum[now−span]` (counters only; values are deltas) | `increase` |
| `distribution_over(span, sel)` | merged latency/value **distribution** over the span (HDR reservoir; histograms only) | `*_over_time` quantiles |
| the `rate` stat | derived `increase / span` | `rate` |

`recent_window` (which had returned a *mixed* snapshot — counter increases AND histogram
distributions in one call) was **split** into `increase_over` + `distribution_over` so each
method emits one kind and the identifier carries the semantic. Both pick the **finest**
cadence whose retained ring covers the span (`finest_cadence_covering`), so the lookback is a
continuous/sliding window at the finest available resolution (e.g. "the last 10 s" off the
1 s ring), unaligned to coarser cadence boundaries. The counter increase is
`insert_counter_increase_into`'s baseline subtraction (`cum[end] − cum[before span]`).

The two metric-reader nodes follow the same split: `metric(...)` reads `session_lifetime`
(session **totals** + session-average `rate`); `metric_window(...)` reads `increase_over`
for `cycles`/`errors`/`rate` (per-window **increase**) and `distribution_over` for
`p50`/`p99`/`mean`.

## Decision

A **counter is cumulative-as-source-of-truth** (monotonic running total, 0 at session
start), exactly like Prometheus / VM / OpenTelemetry-default / Micrometer & Dropwizard
instruments. **Delta is derived** (`cum[t] − cum[t−1]`) only where consumed. This
supersedes the earlier "convert at the read junction" idea (Option B), which left
storage as delta and could not give Prometheus-schematic storage *or* drop-resilient
correctness.

Scope: **counters, plus the histogram/timer count** (the rate-aggregate concern). Gauges
already last-value. The histogram/timer *count* now follows the same cumulative model (the
instrument's monotonic lifetime total); the per-window *distribution* (HDR reservoir,
windowed-merge) keeps its model — Prometheus's cumulative-*bucket*-counter alignment is a
separate follow-on (nb-rs uses HDR reservoirs, which don't window cumulatively).

## Why cumulative is the more correct source (the correctness case)

- **Authoritative, not reconstructed → drop-resilient.** Store `capture_current`
  (`component.rs:363`, the instrument's true running total). A dropped cadence sample
  then loses *resolution*, not data (the next sample carries the true total).
  Reconstructing cumulative from a delta stream is drop-*sensitive*: one missed delta
  permanently offsets every later value (OTel: "delta dropped = data loss").
- **Backend-independence by construction.** `MetricAccess` returns Prometheus-semantic
  series (cumulative counters, gauges as-is). sqlite stores cumulative; the live backend
  reads the same captured cumulative. Identical metricsql expression → identical data →
  identical result, with **no** per-backend conversion code.
- **One source of truth, no drift.** Delta derived where needed; no two parallel
  representations to disagree.
- **Engine unchanged.** rate()/increase/`*_over_time` already assume cumulative (which is
  why they're wrong over today's delta data); reset detection + `rate()`-before-`sum()`
  become correct for free.

## The load-bearing subtlety: combine has TWO semantics for cumulative counters

The delta model uses one `combine_into` (sum) everywhere because summing deltas is
correct for *both* time-coalesce and cross-component aggregate. Cumulative counters
split:

| junction | call site | delta (today) | cumulative (target) |
|---|---|---|---|
| **time-coalesce** (consecutive windows of one series — the cadence cascade/prebuffer, **and the query-side reconstructions** `session_lifetime` / `recent_window`) | `cadence_reporter.rs` cascade; `metrics_query.rs` per-component fold → `Coalesce` | sum | **last value** (monotonic ⇒ window-end cumulative) |
| **cross-component aggregate** (same family+labels across components) | `metrics_query.rs::insert_metric_into` → `Aggregate` | sum | **sum** (totals across series) |

So `combine_into` splits into `combine_coalesce` (counter = last) for the cascade and
`combine_aggregate` (counter = sum) for MetricsQuery. Gauges/histograms identical in both.

**Correction (2026-06-18) — the query-side time-folds were missed initially.**
`session_lifetime` and `recent_window` reconstruct a whole-/recent-span value by folding
**multiple windows of one series over time** (the lifetime accumulator + every cascade
prebuffer; or N consecutive ring windows) and *then* aggregating across components. That
inner fold is **time-coalesce**, not cross-component aggregate — but both originally went
through `insert_metric_into` (`Aggregate`). In the delta model summing was correct (disjoint
slices); under cumulative counters each source carries the full running `cumulative`, so
summing **multiplied** it (a single counter read 84 for an actual 42 — overcounted by the
number of cascade sources, compounded by a `now`/closed-window top-up re-reading a promoted
window). Fix: a `coalesce_component_windows` helper folds a component's windows with
`Coalesce` (sum disjoint `total`s, keep latest `cumulative`, merge reservoirs); the result is
then `Aggregate`d across components. The `now` top-up was dropped (it re-read an
already-promoted window). The rule: **fold the time dimension with `Coalesce` wherever it
appears — the cadence cascade AND the query-side reconstructions (`session_lifetime`, and the
`increase_over` / `distribution_over` that later replaced `recent_window`) — and use
`Aggregate` only across distinct components.**

## Change set (ordered; counters)

1. **Capture → cumulative.** The scheduler's `capture_tree` path captures
   `drain=false` (absolute totals, `capture_current` semantics) for counters instead of
   `capture_delta`. Counter baselines are no longer advanced by the cadence tick.
2. **Split combine.** `snapshot::combine_into` → `combine_coalesce` (counter: last by
   timestamp, like gauge) + `combine_aggregate` (counter: sum). Point the cadence cascade
   at `combine_coalesce`; point `metrics_query`'s cross-component fold at
   `combine_aggregate`. The prebuffer holds the latest cumulative, not an accumulating sum.
3. **Derive delta** for today's delta consumers, from cumulative differences:
   - the sqlite `_rate` suffix (`cycles_total_rate` = `Δcum/interval`);
   - `metric("…","rate")` (`Δcum/duration`);
   - summary per-window throughput; TUI live rate.
4. **Backends expose cumulative.** sqlite reporter writes cumulative samples; the live
   backend (`queryapi::live`) reads the cumulative windows directly — **no** accumulation
   in `select_range` (the reverted Option-B code stays reverted).
5. **Tests.** Update the delta-assuming tests (`combine_into` sum tests, the `_rate`
   suffix tests' expectations, capture tests); add: cumulative storage is monotonic; the
   same `rate(...)` expression gives equal results from live vs sqlite (backend
   independence); rate() value is the true per-second increase.

## Correctness invariants

- **Reset = session restart.** A session is a process: counter 0 at start, monotonic
  within; a resume/restart is a reset (back to 0) that rate() detects/corrects — same as
  a Prometheus process restart.
- **rate() before sum().** Per-series cumulative → rate → aggregate; the engine's
  `sum(rate(...))` shape preserves this.
- **Optimizer per-eval scoping (CLOSED 2026-06-18).** The counter is
  session-cumulative across coordinates (like Prometheus running a workload N times in one
  process). Per-coordinate rate = `rate()` over a window **scoped to the coordinate's time
  range** — the settle sizes/anchors the window, no storage trick. Resolved via the
  settle pipeline (not an engine clamp): the objective is settled across the run over the
  cadence feed, and the settle's **viability gate** is sized to the objective's widest rollup
  window (`PolydatProgram::max_temporal_window_ms`, fed by the metricsql reader node's
  `temporal_window_ms`), so a `rate(...[W])` read can't settle until its window has cleared
  the prior coordinate. See SRD-86 §6 step 2. The bounded windowed readers (`metricsql_*`,
  `metric_window`) are gate-scopeable; the session-cumulative `metric(...)` reader
  (`session_lifetime`) has **no bounded window** and so cannot be gated — the settle *warns*
  that such an objective won't isolate per-coordinate rather than scoping it.

## Out of scope (this change)

Gauges (already last-value). The histogram/timer per-window **distribution** (HDR reservoir
windowed-merge — percentiles stay windowed; Prometheus cumulative-*bucket*-counter alignment
is a separate follow-on). The histogram/timer **count** is now in scope (closed 2026-06-18).
(The optimizer per-eval window scoping — previously deferred here — is now CLOSED, see
the correctness invariant above.)
