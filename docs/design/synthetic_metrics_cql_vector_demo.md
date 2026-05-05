# Design Memo: Synthetic-metric demo in `full_cql_vector.yaml`

**Status:** memo (proposes a concrete demo on top of SRD-40b's
mechanism). Mortal — intended to be subsumed by the implementation
PR's commit messages once the demo lands.

**Companion SRD:** [SRD-40b — Synthetic metrics declared by op
templates](../sysref/40b_synthetic_metrics_from_gk.md). This memo
assumes the SRD has been implemented (phases A–E); it covers only
the workload-author surface needed to demonstrate the mechanism.

---

## What we want to demonstrate

The CQL vector workload has a measured-latency phase that sweeps
`(k, limit, optimize_for, profile, …)` and emits real
`recall@K_*` metrics. We want to add a synthetic phase that
produces an analytical curve for the same dimensional cells, so
the SRD-46 plot directives can render the curve alongside the
measured data on one chart.

The two analytical formulas:

```
LATENCY: f(limit) = 0.979 + 4.021 * pow(limit, -0.761)
                    f(1) ≈ 5.0    f(100) ≈ 1.1   f(1000) ≈ 1.0

RECALL:  f(limit) = 0.509 + 9.491 * pow(limit, -0.402)
                    f(1) ≈ 10.0   f(100) ≈ 2.0   f(1000) ≈ 1.1
```

Both are functions of `limit` only; `optimize_for` selects which
formula applies. No other dimensions feed the value.

---

## Phase shape

A new phase, `pvs_predict`, that mirrors the existing query
phase's iteration so dimensional alignment (SRD-40b §5) is exact.
The query phase reads workload-level GK names — `k_values`, the
per-`k` `k_<n>_limits` lists, and `optimize_for_modes` — to
produce one op per `(k, limit, optimize_for)` cell. The predict
phase reads the **same** names.

```yaml
phases:
  pvs_predict:
    # Re-read the workload-level GK comprehension. Conceptual
    # shape; the exact for_each chaining follows SRD-18b.
    for_each:
      - "k in k_values"
      - "limit in k_n_limits[k]"
      - "optimize_for in optimize_for_modes"
    cycles: 1

    # Phase-level GK module: the two formulas, named so the
    # `value:` expression below is one if(...) call.
    bindings: |
      latency_factor := 0.979 + 4.021 * pow(limit, -0.761)
      recall_factor  := 0.509 + 9.491 * pow(limit, -0.402)

    ops:
      predict:
        adapter: stdout
        stdout: eventlog                # SRD-40b §6: suppress terminal noise
        stmt: "/* synthetic */"

        metrics:
          optimize_for_factor:           # distinct family name (SRD-40b §4)
            value: |
              if(optimize_for == "LATENCY", latency_factor, recall_factor)
            unit: ms
```

Notes:

- **Family name** `optimize_for_factor`. Distinct from any
  measured family. SRD-40b §4 forbids reuse on the same
  dimensional cell.
- **`value:` is one expression**. The branching by
  `optimize_for` happens in GK, not in YAML conditionals — keeps
  the metric declaration tight, and the formula library
  (`bindings:` block) reusable if other phases want it.
- **No new comprehension**. The `for_each` block reads the same
  GK names the query phase reads. If the workload reorganises
  `k_values` or `k_n_limits[k]`, both phases follow.

---

## Scenario

A new named scenario that runs only the predict phase, so users
can populate the synthetic family without re-running the
expensive measured phase:

```yaml
scenarios:
  predict_only: pvs_predict
```

The default scenario stays whatever the workload already declares.
Users running the standard scenario get measured + (when desired)
synthetic in the same session by chaining: `nbrs run … scenario=
default && nbrs run … scenario=predict_only`. Both write to the
same `metrics.db` (per SRD-45 session continuity), so the plot
directive sees both families in one query.

---

## Plot directive

A plot under the workload's `report:` block, structurally
identical to plots over measured families:

```yaml
report:
  optimize_for_curves: |
    plot optimize_for_factor_curves
      query: avg(optimize_for_factor_mean) by (limit, optimize_for)
      x: limit
      series: optimize_for
      label "Predicted latency factor by optimize_for vs limit"
      xscale: bin
      yscale: dec
```

The plot reads the synthetic family the same way it would read a
measured family. SRD-40b §7 guarantees parity at the read side; no
synthetic-aware logic exists in the plot path.

To overlay measured + synthetic on one chart, a future iteration
can use a multi-query plot directive (SRD-46 vocab permitting) or
two separate plots stacked in the report.

---

## Sequence to land the demo

1. **SRD-40b implementation.** Phases A–E from the SRD must land
   first. The workload YAML changes don't compile against
   anything until `metrics:` is parsed and the wrapper exists.
2. **Workload patch** to `full_cql_vector.yaml`:
   - New `pvs_predict` phase block.
   - New `predict_only` scenario.
   - New plot directive in `report:`.
3. **Smoke test.** `nbrs run workload=… scenario=predict_only`
   should populate `optimize_for_factor` rows in `metrics.db`,
   and `nbrs report plot optimize_for_factor_curves` should
   produce a chart with two lines (LATENCY, RECALL) over the
   limit axis.

---

## Why this stays in a memo, not the SRD

Everything above is workload-shaped: the formulas, the phase
name, the `k_values × k_n_limits[k]` comprehension, the plot
labels. None of it is normative for nb-rs as a system — it's a
concrete consumer of SRD-40b's mechanism. The cross-cutting rules
(schema, wrapper, registration, alignment principle, output-
channel convention) live in SRD-40b; this memo records *one
application* of those rules.

When the demo lands, this memo can be deleted or shrunk to a
pointer at the workload YAML. The SRD persists.
