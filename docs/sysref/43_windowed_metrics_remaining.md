# 43: Windowed Metrics — Comment Resolution & Migration Plan

This doc previously held a parking lot of remaining work items
for review. Those comments have been folded back into SRD-42 as
the unified `MetricsQuery` design and the Phase 6–10 migration.
This file stays as a record of what changed and why.

---

## What moved to SRD-42

| Comment thread | Where it landed in SRD-42 |
|---|---|
| Standard in-process query interface with cadence selector + ordered enumerator + duration-spec composition | §"MetricsQuery — the unified read interface" |
| Three named query modes (`now`, `cadence_window`, `recent_window`) with per-metric ephemeral merge | §"Query modes" + §"Cost rule for `recent_window`" |
| Algebraic uniformity — combine reservoirs, derive percentiles as properties of the combined result | §"Combine semantics — algebraic uniformity" |
| Selection by full label set OR by label filter within a component subtree; "specific metric" callers hard-error on ≠1 match | §"Selection" |
| Reports use the global session-lifetime accumulator by default; cadences are opt-in extra columns; no fixed `ActivityRow` schema | §"Reports — lifetime accumulator + opt-in cadences" |
| Cadence reporter walks the component tree at the smallest-cadence tick, captures an OpenMetrics-shaped snapshot, owns the windowed store | §"Wire-Up → Cadence reporter — single writer" |
| SQLite is near-time persistence, not the canonical query surface; default 10s minimum write cadence | §"SQLite — near-time persistence, not a query surface" |
| Cross-component visibility is intrinsic to the tree walk — no separate composition layer needed | §"Open Questions" (resolved) |
| Cadence validation is a plan-time hard error, not parse-time; remove `from_unchecked` | §"Open Questions" + Phase 10 |
| Streaming coalesce — close, notify, fold into next prebuffer; never batch a larger cadence's coalesce | §"Streaming coalesce semantics" |
| Force-close + flush partials on shutdown; trailing snapshots carry actual `interval < cadence` | §"Streaming coalesce semantics → Shutdown" |
| Lifetime emerges as a top-of-cascade prebuffer that never promotes mid-session; peeked, not awaited | §"Streaming coalesce semantics → Why this matters" + §"Open Questions" (resolved) |
| Notification dispatch is async with per-subscriber timeout; timeout escalates via error callback | §"Streaming coalesce semantics → Notification dispatch" |
| Pull (`cadence_window`) returns latest closed snapshot immediately, even mid-session; push subscribers see only future closes | §"Wire-Up → Subscription model — pull vs push" |
| `session_lifetime` query mode — walks the cascade down at read time (lifetime prebuffer + in-flight smaller prebuffers + live `peek_snapshot`) so canonical session totals never lag in-flight data; subsumes `InProcessMetricsStore` cumulative views | §"Streaming coalesce semantics → Canonical session-lifetime queries" + §"Query modes" table |

## What stayed open

| Item | Status |
|---|---|
| `DEFAULT_MAX_FAN_IN = 20` reframed | Resolved (reasoning, no profiling needed). With streaming coalesce, K barely affects per-tick cost or steady-state memory. What it actually controls: (a) in-flight data loss on crash (`K × layers × base ≈ 2 min` at K=20 / 6 layers / 1s); (b) `recent_window` granularity — lower K inserts more hidden layers, giving the query finer tiles; (c) INFO-log noise. **Keep K=20 default**; SRD-42 should reframe the tuning advice as "how granular do you want windowed-history queries?" not "how fast do you want coalesce to run?". |
| TUI `past(span)` ad-hoc lookback overlay | Resolved (design). Trigger: `p` opens a one-line `past span:` input prompt; accepts `Cadences::parse` syntax (`5m`, `1h30m`, …). Render: extra row on the latency bar chart between `now` and smallest cadence, labeled `past <span>`, refreshed via `MetricsQuery::recent_window(span).p99`. Single pinned span; in-memory per-TUI-session; `p` again re-opens with current value pre-filled; empty input clears. Invalid spans surface a one-line error preserving the existing pin. Phase 7 follow-on, or interim against per-component `past()` if needed sooner. |
| `InProcessMetricsStore` coexistence with the cadence reporter store | Resolved (architecture). Cumulative views are served by `session_lifetime` over the unified store (lifetime prebuffer + in-flight cascade peek + live now). Last-window views are the smallest cadence's most-recently-closed snapshot. Phase 6 may keep `InProcessMetricsStore` running as a parallel sink during migration, but it ceases to be the source of truth for any read path. Phase 10 deletes it. |
| Subscriber timeout default + escalation path | Resolved (architecture). Default timeout is `2 × cadence_interval`; subscribers may override per-subscription. Escalation routes through a cadence-manager-registered error callback receiving `(cadence, subscriber_id, snapshot_age, consecutive_timeouts)`; the manager owns the action. Default callback (no manager registered) emits `diag::warn` on first timeout and `diag::error` on subsequent — bounded noise, never silent. |
| Snapshot data shape | Resolved (architecture). In-memory model mirrors OpenMetrics 1:1 — `MetricSet` → `MetricFamily` → `Metric` (LabelSet) → `MetricPoint` variants (`CounterValue`, `GaugeValue`, `HistogramValue`, …). Identity = `(MetricFamily.name, LabelSet)` per spec §4.5.1. Histograms internally store the HDR reservoir for combine fidelity; OpenMetrics cumulative buckets are derived on demand. Suffix conventions (`_total`, `_count`, etc.) are exposition-time concerns, not stored. Component context lives in labels. See SRD-42 §"Snapshot data model". |
| Exemplars (OpenMetrics §4.6.1) | Resolved (architecture). Optional `Exemplar { labels, value, timestamp }` on `CounterValue` and per-`Bucket` within `HistogramValue`/`GaugeHistogramValue`. Recording-side sampling: per-bucket O(1) "skip if bucket already has one for the current open window". Combine: most-recent-wins by `MetricPoint.timestamp`. Cost: O(buckets × cascade depth) `Arc<LabelSet>` clones per smallest-cadence tick — negligible. New recording APIs: `Counter::increment_with_exemplar` / `Histogram::record_with_exemplar`. See SRD-42 §"Snapshot data model → Exemplars". |

## Migration plan summary

SRD-42 §"Incremental Rollout" lists Phases 6–9 — direct
replacement, no compat layer. Each phase is a single change set:
every caller of the type/path being deleted gets rewritten in
the same diff. Brief recap:

- **Phase 6** — `snapshot.rs` types are the only data model.
  Delete `Sample` and rewrite producers/consumers to speak
  `MetricPoint`. `MetricsFrame` → `MetricSet`.
- **Phase 7** — Cadence reporter is the only store;
  `MetricsQuery` is the only reader. Delete
  `WindowedMetrics` and `InProcessMetricsStore`; rewrite TUI,
  summary, SQLite, and GK metric nodes against `MetricsQuery`
  in the same diff.
- **Phase 8** — Streaming coalesce + async notification dispatch
  replace the current per-tick scheduler coalescing. Delete
  `set_live_source`; the reporter owns the live capture path.
- **Phase 9** — Cleanup. Plan-time cadence validation;
  delete `from_unchecked`; surface `past(span)` in the TUI; sweep
  for orphans.

The bootstrap phases (1–5) shipped a single-metric per-component
slice that proved every mechanism the unified design needs
(cadence tree, auto-intermediate buckets, peek_snapshot,
`past(span)`). They are scaffolding for the unification, not a
public API surface — everything they introduced gets replaced
or absorbed by the unification phases.
