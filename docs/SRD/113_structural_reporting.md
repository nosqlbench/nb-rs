# SRD-113 — Structural reporting: key-metric designations and execution-graph anchors

**Status:** design agreed 2026-08-08; implementation staged below
**Owner:** nmbrs-workload (designation + anchor model), nmbrs (synthesis + renderer)
**Cross-refs:** SRD-46 (reports; the synthesized surface), SRD-64/65 (report
CLI / plot axes), SRD-77 (structural labels — the row join), SRD-42/40a
(gauge last-write-wins, amended 2026-08-08 as this SRD's storage
prerequisite), SRD-91 (outcome instruments — the spine), SRD-101
(`continue_if` — anchor-adjacent sweep gating)

---

## Why

Hand-written report definitions for a structured workload reconstruct, by
hand and per report, facts the workload already states: which phases exist,
which loops produce per-iteration series, which labels join a metric to the
scope that emitted it. The reconstruction fails in characteristic ways —
label-join guesswork, silently implied aggregates (a bare `avg()` reading
one instance-latest sample and rendering as if it were a time-average),
phase blindness (a table filtered to one phase name renders empty for the
sibling flow), and coverage gaps that nobody notices because nothing checks
the report against the workload's shape.

The fix inverts the relationship: **the workload's own structure is the
report definition.** Phases designate their key metrics; the scenario
layout designates row anchors; a report is synthesized from those fixtures.
Explicit `report:` sections remain the escape hatch for hand-tuned figures
— and the synthesized form is expressed in exactly that grammar, so the
escape hatch is "dump and edit," not a parallel system.

## The model

1. **Every phase is meaningful.** Every execution node appears in the
   report at least through its SRD-91 outcome instruments. Zero
   designations is a valid designation.
2. **Rows are execution-graph nodes.** A table coalesces a subset of the
   execution graph onto each line. The scene/scope tree — scenario nodes,
   comprehension iterations, phase activations — is the row space; the
   SRD-77 structural labels carried by every metric instance are the join.
3. **Anchors choose the row altitude, and it can vary.** An anchor cue on
   a scenario-layout node declares "one table line per activation of this
   node." Different tables may anchor at different altitudes.
4. **Default closure: scenario-level nodes.** With no anchors declared,
   the synthesized report has one spine table whose rows are the
   scenario-level nodes active in the executed scenario — `teardown` and a
   tier loop are peers on it.
5. **Cardinality below an anchor is load-bearing.** The graph tells the
   renderer the cardinality of every sub-scope beneath an anchor. A
   designated family whose owning node activates N>1 times per row must
   have either a declared structural aggregate or a break-out — silence is
   an error, not a flatten.
6. **Break-outs are opt-in inset rows.** An anchor may request inset rows
   for each interior node activation (per-iteration, per-phase) beneath a
   row.
7. **Designation ⇒ row attachment.** Designating key metrics on a node
   attaches that node's measurables to the row of its nearest enclosing
   anchor (or the spine).

## Designations

Phase-level (op-template-level reserved for later):

```yaml
phases:
  load_increment_adaptive:
    key_metrics:
      rows:        last(result_success)
      failures:    last(result_failure)
      rows_per_s:  rate(result_success)
      insert_p99:  max(result_success_p99)
      weight:      avg(load_weight)
      weight_min:  min(load_weight)
      pressure:    avg(servo_pressure)
```

- Every entry is `column_name: agg(family)`. The aggregate is **mandatory**
  — `rows: result_success` is a parse error naming the family and listing
  the aggregate vocabulary.
- **Aggregate vocabulary (time dimension):** `min`, `max`, `avg`, `last`,
  `first`, `median`, `stddev`, `sum`, `count` — defined over the stored
  samples of one instance within the row scope's activation window, which
  under the SRD-42 amendment are last-write-wins point samples (the
  PromQL contract; `avg(load_weight)` is the average of last-written
  values, exactly what a PromQL user expects).
- **Derived measures (graph-aware):** `rate(F)` = mean delivery rate while
  active (per-window increase over the window's own `interval_ms`, averaged);
  `span()` = total active time (sum of sample intervals, max across the row's
  instances). Both ride the per-sample active-window bookkeeping
  (`F_rate` / `F_interval_ns` virtual stats), NOT first/last timestamp
  arithmetic — a phase shorter than the durable write cadence leaves one
  sample, whose interval is still exact. `delta(F)` = last − first. These
  are explicit declarations, so the no-implied rule holds.
- **Structural aggregates** (across activations under one row, needed only
  when cardinality > 1): declared at the anchor, per column, from the same
  vocabulary minus order-dependent members (`last`/`first` are
  structural errors). Example: `rollup: {weight: avg, rows: sum}`.

## Anchors

```yaml
scenarios:
  stcs_adaptive:
    - ...
    - for: "part in profile_partitions('{dataset}', '{profile_pattern}')"
      anchor: tier            # one row per iteration; view named "tier"
      breakout: none          # none (default) | phases | all
      phases:
        - compaction_watch
        - scenario: tier_ingest_adaptive
        - recall_check
```

- `anchor: <view>` names the table the rows belong to. Multiple nodes may
  share a view name only if their coordinate label sets are identical
  (else: error).
- The synthesized table's row key is the anchor node's coordinate label
  (`part`, `recall_round`, …); its columns are the union of designations
  attached beneath it, prefixed on collision by phase name.
- The spine table is always synthesized; anchored sub-trees appear on the
  spine as a one-line reference to their view.

## The spine contract

The SRD-91 outcome instruments carry **contract-level default
designations** — declared here, in the instrument contract, so the spine
is not an implied aggregate: `count: last(result_success)`,
`failures: last(result_failure)`, `wall: span()`, `p99: max(result_success_p99)`.
A workload needs zero designations to get a complete, honest spine.

## Synthesis — the affine mechanism

When a report is requested for a workload with **no explicit `report:`
block**, the renderer synthesizes one: a report section in the SRD-46
grammar (groups, `table`/`plot` items, metricsql query bodies), generated
from the designations + anchors + spine contract, then fed through the
existing parse → evaluate → render pipeline. Consequences, by design:

- **Affinity.** Anything the synthesis produces is expressible by hand;
  anything hand-written uses the same machinery. `nmbrs report synth`
  (name TBD) dumps the synthesized section verbatim — the migration path
  to hand-tuning is copy, paste, edit.
- **Suppression.** A workload with an explicit `report:` block gets no
  synthesis (the dump verb still works). Mixing comes later, if ever.
- **One query language.** Synthesized bodies are ordinary metricsql over
  canonical family names and structural labels — `avg(avg_over_time(
  load_weight{phase="load_increment_adaptive"}[<span>])) by (part)` — the
  same expressions an operator would write, visible in the dump.

## Well-formedness — errors at report time

Report rendering (synthesized or explicit) validates against the workload
structure and **fails** on:

1. **Unqualified aggregate** — a designation without `agg(...)` (caught at
   workload parse), or an explicit-section query whose coalescing is not
   derivable from a declared aggregate.
2. **Unknown family** — a designation naming a family no op/phase emits.
3. **Silent flatten** — a designated family with cardinality > 1 under its
   row anchor and no declared structural aggregate or break-out.
4. **Order-dependent structural aggregate** — `last`/`first` across
   sibling activations.

Every error names the node, the family, and offers the vocabulary:
`min, max, avg, last, first, median, stddev, sum, count` (+ derived
`rate, span, delta`). No error is a warning; a report that renders is a
report whose aggregations were all chosen by a human.

## Staging

1. **Model** — parse + store `key_metrics` (phases) and `anchor`/
   `breakout` (scenario nodes); agg-qualification enforced at parse with
   the suggestion list. Unknown-family validation needs the op walk.
2. **Synthesis** — spine table + per-anchor tables from designations;
   dump verb; suppression rule.
3. **Error contract** — cardinality analysis from the scene tree;
   errors 2–4.
4. **Break-outs** — inset rows.
5. **Plots** — synthesized progression plots (paired `(progress, y)` form)
   for designations opting in (`plot: true` on a designation), after
   tables prove the model.

## Resolved prerequisites

- Gauge storage is last-write-wins (2026-08-08, SRD-42 amendment): the
  time-aggregate vocabulary operates on point samples with PromQL
  semantics, so designations need no storage coupling. Summarization is
  a query-point concern, period.

## Open questions

- Unit/scale hints on designations (`insert_p99: max(result_success_p99) as ms`)
  vs renderer auto-scaling — deferred; auto-scaling has been adequate.
- Column ordering control; per-view labels beyond the anchor coordinate.
- Whether `median`/`stddev` need sample-count floors to render honestly.
