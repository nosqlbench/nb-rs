# nbrs Examples

## workloads/

Example workload YAML files. All support `#!/usr/bin/env nbrs`
shebangs — make them executable and run directly.

### workloads/getting_started/
- `basic_workload.yaml` — Minimal multi-op workload
- `polydat_bindings.yaml` — Native Polydat DAG syntax for bindings
- `inline_ops.yaml` — Inline `op=` expressions and ratio prefixes

### workloads/
- `math_and_bitwise.yaml` — All infix operators, auto-widening
- `cartesian_space.yaml` — Mixed-radix coordinate decomposition
- `op_inline_forms.yaml` — How `op=` is detected and de-sugared (Polydat block vs text template), with the explicit YAML equivalent for each CLI form
- `conditional_ops.yaml` — `if:` field for per-cycle op skipping
- `feature_showcase.yaml` — Phases, scenarios, params, conditions
- `service_model.yaml` — Multi-table, ratios, `delay:` latency injection

### workloads/ — optimizer (SRD-86; see [docs/guide/optimizer.md](../docs/guide/optimizer.md))
- `optimizer_sweep.yaml` — discrete axis, default `sweep` (identity) + best-selection, synthetic objective
- `optimizer_continuous.yaml` — continuous (float-range) axis sampled by `nelder_mead`
- `optimizer_inline_objective.yaml` — objective written as an inline polydat expression (no `bindings:` entry)
- `optimizer_multiaxis.yaml` — multi-axis continuous search with `cmaes`
- `optimizer_categorical.yaml` — categorical (named-label) axis
- `optimizer_saturation.yaml` — run-produced objective (metric reader), settled across the run
- `optimizer_metricsql.yaml` — run-produced objective via a MetricsQL reader, settled
- `optimizer_control.yaml` — `servo:` a live control — retargeted by a daemon on one continuous phase (no rerun)
- `optimizer_multiservo.yaml` — `servo: [concurrency, rate]` — two live controls retargeted together over a 2-D grid
- `optimizer_hybrid.yaml` — mixed coordinate + control axes in one node

### workloads/signals/
- `fourier_analysis.yaml` — FFT analysis of fractal noise
- `lfsr.yaml` — Galois LFSR with bitwise Polydat ops

### workloads/visual/
- `maze.yaml` — Classic random slash maze pattern

## modules/

GK module files (`.polydat`) and workloads that exercise them.

- `hashed_id.polydat` — Example Polydat module (deterministic hashed ID)
- `euler_circuit.polydat` — Euler circuit Polydat module
- `module_test.yaml` — Adjacent `.polydat` file resolution
- `stdlib_test.yaml` — Embedded stdlib module resolution
