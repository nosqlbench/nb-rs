# 39: Metrics & Observability — Contract & Axioms (nbrs-metrics)

Front door for **nbrs-metrics** (layer L2): the component tree, instruments, cadence
reporter, and the read-side query surface. A well-factored crate — almost every module is
a deliberate public surface. Pillars 1+2 of the
[Subsystem Treatment Standard](00b_subsystem_standard.md).

## Contract

- **Public surface:** `component` (the hierarchical tree), `instruments` (counters /
  gauges / histograms / timers), `labels`, `selector`, `controls`, `snapshot`, `cadence`,
  `cadence_reporter`, `scheduler`, `summaries`, `metrics_query` (the read-side handle),
  `reporters` (SQLite / VictoriaMetrics sinks), `diag`, `polydat_nodes`.
- **Internal** (declared `pub`, unconsumed): `validation`.
- **Inbound contract:** `polydat` (`ast`, `kernel`) for metric-reading nodes.
- **Allowed edges:** `polydat`. See [SRD 05 §Contract Registry](05_dependency_rules.md).

## Axioms

- **M1 — Lock-free reporter.** `CadenceReporter` uses an **actor + `ArcSwap`**, never a
  `RwLock`. Preserve this in any change.
- **M2 — Nanoseconds internal.** All internal time values are tracked as nanoseconds.
- **M3 — The component tree is the canonical metric index.** Instruments hang on
  components; lookup is by dimensional-label selector, never a sidecar map. [SRD 24](24_component_lookup.md).
- **M4 — Immutable frame snapshots.** Metrics are captured as immutable snapshots at
  cadence intervals (delta semantics); readers never see torn state. [SRD 40](40_metrics.md).
- **M5 — Controls are reified gauges + a confirmed-apply write path.** Dynamic controls
  are enumerable on the tree and applied transactionally. [SRD 23](23_dynamic_controls.md).

## Mechanism (Pillar 3)
[SRD 40](40_metrics.md) (framework), [SRD 40a](40a_metrics_model.md) (data model),
[SRD 40b](40b_synthetic_metrics_from_polydat.md) (synthetic metrics), [SRD 42](42_windowed_metrics.md)
(windowed access), [SRD 24](24_component_lookup.md) (lookup). MetricsQL is its own crate — [SRD 08](08_metricsql.md).

## See also
`nbrs-metrics/src/lib.rs`; [SRD 40](40_metrics.md); [SRD 40a](40a_metrics_model.md); [SRD 42](42_windowed_metrics.md).
