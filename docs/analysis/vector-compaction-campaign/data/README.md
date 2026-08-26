# Data for the vector-compaction analysis

These CSVs are **frozen evidence** from a completed analysis, not live tooling.
They were extracted once from Cassandra's text logs and are kept as the record
behind the numbers in `../README.md`.

## Why there are no extraction scripts here any more

There used to be three Python scrapers in this directory and two more under
`docs/captures/`. They recomputed medians, quantiles and windowed rates by hand
— which is what `nbrs metrics` and a workload `report:` block already do, and
they added an undeclared Python toolchain to a Rust repo. They have been
deleted.

The nb-rs-visible analysis now lives where it belongs: the
`report:` block at the end of `adapters/cql/workloads/compaction_demo_derived.yaml`
renders pool pressure, SSTable growth, the windowed failure rates the abort
backstops read, and the servo's response — all as metricsql over metrics the
workload itself publishes. Run it with:

    nbrs report all --session-dir <session> workload=adapters/cql/workloads/compaction_demo_derived

## What could NOT move into the workload, and why

Four families of measurement in this analysis exist **only in jvector's text
log**, which nb-rs cannot see:

| measurement | log line |
|---|---|
| segment build rate (cells/s) | `Flushed segment with N cells ... in M ms` |
| merge progress | `Compaction I/O progress: N/M batches ... (X/Y ordinals)` |
| source pretouch cost | `Source pretouch: warmed N ordinals ... in M ms` |
| cluster-path cost | `Cluster path cost: certified N/M ..., R exact rescores` |

Cassandra exposes compaction progress through `system_views.sstable_tasks`
(already read by the watch daemon), but its `token range parts` counter is
**unreliable** — it implied 10.3 days for work three other measures put at
2.5–7 h — so it is not a substitute.

Making these first-class would mean emitting them from Cassandra as metrics
rather than log lines. Until then, extracting them is a one-off shell task
against the logs, not checked-in tooling — which is the distinction that was
got wrong the first time.

## Files

| file | contents |
|---|---|
| `provenance.csv` | run → jvector commit → Cassandra commit → flags, recovered from JVM args |
| `provenance-boots.csv` | per-boot flag surface |
| `history-merges.csv` | 480 merges, 2026-08-21 → 08-24, with size class and regime |
| `history-pretouch.csv` | 82 pretouch calls with µs/ordinal |
| `cycle{3,4}-segments.csv` | per-segment cells/s |
| `cycle{3,4}-pretouch.csv` | per-call pretouch cost |
| `cycle{3,4}-large-merges.csv` | ≥25k-batch merges with rate and completion |
| `configurations.csv`, `headline-results.csv` | config matrix and cross-cycle comparison |
