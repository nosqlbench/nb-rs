# Optimizer — Workload-Author Guide

An `optimize:` block turns a phase's `for_each` sweep into a **search**: instead
of visiting every coordinate, an optimizer proposes coordinates, reads an
**objective** you name, and maximizes it. This guide is task-oriented; the full
design is [SRD-86](../SRD/86_optimization.md). Every form below has a runnable
example under `examples/workloads/optimizer_*.yaml`.

## The one-minute version

A phase becomes a search node by adding `optimize:` alongside its `for_each`:

```yaml
phases:
  sweep:
    cycles: 1
    for_each: "ef in 1.0,2.0,3.0,4.0,5.0"   # the search axis (auto-gathered)
    optimize:
      objective: score      # a wire to MAXIMIZE
      max_evals: 10
    bindings: |
      score := 0 - (ef - 3) * (ef - 3)      # peaked at ef=3
    ops:
      probe: { adapter: stdout, stmt: "ef={ef}" }
```

`nbrs run workload=...` reports `best [3] → score=0`. Two things you do **not**
write: the axes (auto-gathered from `for_each`), and — with no `method:` — the
optimizer: it defaults to **`sweep`** (the identity: evaluate every coordinate,
report the best). Add `method: cmaes` (etc.) to search adaptively instead.

## Pick the axis kind by how you write the `for_each`

| Axis kind | `for_each` form | Notes | Example |
|---|---|---|---|
| **Discrete** | `"ef in 1.0,2.0,3.0"` or `"x in [1.0,2.0,3.0]"` | numeric detents. **Use float literals** (`1.0`, not `1`) — an integer axis types the iter-var `u64`, and signed objective math like `0 - (x-3)^2` then underflows. | `optimizer_sweep.yaml` |
| **Continuous** | `"ef in 1.0 .. 5.0"` | a real interval — *sampled* by a metric-space solver, not enumerated. | `optimizer_continuous.yaml` |
| **Categorical** | `'mode in ["a","b","c"]'` | named options, no order/distance. **Quote the labels** — a bare word is a wire reference (SRD-18f). | `optimizer_categorical.yaml` |

### One axis or several

A multi-clause `for_each` searches several axes jointly:

```yaml
for_each: "x in 0.0 .. 6.0, y in 0.0 .. 8.0"   # 2-D continuous search
optimize: { method: cmaes, objective: score, max_evals: 60 }
```

Discrete and continuous axes both work at any cardinality. See
`optimizer_multiaxis.yaml`.

## Choose a method

`nbrs describe optimizers` lists them. The two classes:

- **Identity** — `sweep` (the **default**, used when `method:` is omitted) visits
  every coordinate in order and reports the best. Use it for small grids and
  categorical choices.
- **Adaptive** — `nelder_mead`, `cmaes`, `hooke_jeeves`, `bobyqa`, `bayes_opt`,
  `hyperband`, `centroid_variant`, `cost_greedy_traversal`. These propose
  coordinates from feedback and converge on a manifold without visiting
  everything. `nelder_mead` (simplex) and `cmaes` (population) are good defaults
  for continuous numeric tuning. Set `seed:` for reproducibility.

## Choose the objective

`objective:` names a wire to maximize. Two sources:

- **Synthetic** — a `bindings:` expression that is a pure function of the
  coordinate (deterministic, no backend). Good for learning and for tuning
  against a known manifold.
- **Run-produced** — a live metric the phase *generates*, read through a windowed
  reader (`metricsql_scalar("sum(rate(errors_total[3s]))")` or
  `metric_window(...)`). The value exists only *after* the phase runs, so the
  optimizer settles it across the run (the cadence-fed detector holds the
  windowed value until it stabilizes). See `optimizer_saturation.yaml` /
  `optimizer_metricsql.yaml`. Maximize, so negate a cost: `score := 0 - err_rate`.

## Coordinate vs control: stepping through vs steering

How a value is enacted has two modes:

- **Coordinate** (the **default** for every axis) — the value is **stepped
  through by re-running the phase** at each setting.
- **Control** — a live SRD-23 control (`concurrency`) **retargeted without
  restarting the phase**, steered by a daemon on one continuous phase.

You get control by **naming it in `steer:`**. Two equivalent ways to name it:

```yaml
# Direct — the axis IS the control (no bind wire). The only way to steer `rate`.
for_each: "concurrency in 32, 16, 2"
optimize: { objective: score, steer: concurrency }
```
```yaml
# Indirect — a plain var bound into a control, when you want a different axis name.
concurrency: "{conc}"
for_each: "conc in 32, 16, 2"
optimize: { objective: score, steer: conc }
```

`steer:` accepts one name (`steer: concurrency`) or a list (`steer: [concurrency,
rate]`). The optimizer steers the control per setting and settles the windowed
objective at each. Because the phase dwells at each setting, a control sweep that
explores a saturating region must **not** carry a tripping error guard — set
`error_rate_max: 1.0` (which compiles to `error_rate > 1.0`, i.e. "allow 100%").
See `optimizer_control.yaml`.

**`steer:` is validated, not guessed.** A steered var must (a) be a search axis,
(b) resolve to a live control — directly (its name is `concurrency`/`rate`) or via
a `{var}` bind — and (c) have a windowed objective the steerer can settle. Miss
any of these and you get a **clear error** — never a silent downgrade. So if you
wire a control but use a session-cumulative
`metric(...)` objective (which can't isolate a live setting), don't `steer:` it —
just step it through (the default), as `optimizer_saturation.yaml` does.

> Which names are live controls? Run **`nbrs describe controls`** — it lists every
> control this binary can steer (core `concurrency`/`rate` plus any adapter knobs
> like `cql_trace_rate`), with the condition under which each appears (SRD-23).

### Mixing step-through and steering in one node (hybrid)

A single node can do both — name only the steered vars:

```yaml
for_each: "batch in [1,2], conc in [16,2]"
optimize:
  objective: score
  max_evals: 10
  steer: conc                       # batch steps through; conc steers
```

`batch` (not steered) forms the **outer re-run grid**; for each `batch` cell the
phase reruns and the daemon steers `conc` **interior** to it. A coordinate axis is
realized *only* by iterating its scope (re-run) — never set ineffectually. See
`optimizer_hybrid.yaml`.

## How a search renders

A search node is a **Search**, not a sweep: dryrun/describe and the scene tree
show its spec and budget (`search · cmaes · maximize score · {x, y} · ≤60 evals`)
and *evaluations* as children — never a pre-listed coordinate set. The final line
reports `best [...] → objective=value after N evals`.

## Example index

| Example | Form it illustrates |
|---|---|
| `optimizer_sweep.yaml` | discrete axis, default `sweep` (identity) + best-selection, synthetic |
| `optimizer_continuous.yaml` | continuous axis, `nelder_mead`, synthetic |
| `optimizer_multiaxis.yaml` | multi-axis continuous, `cmaes` |
| `optimizer_categorical.yaml` | categorical (label) axis |
| `optimizer_saturation.yaml` | run-produced objective (metric reader), settled |
| `optimizer_metricsql.yaml` | run-produced objective (MetricsQL reader), settled |
| `optimizer_control.yaml` | control axis — live-retarget steering daemon |
| `optimizer_hybrid.yaml` | mixed coordinate + control in one node |
