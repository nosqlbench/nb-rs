# nbrs Examples

## workloads/

Example workload YAML files. All support `#!/usr/bin/env nbrs`
shebangs — make them executable and run directly.

### workloads/getting_started/
- `basic_workload.yaml` — Minimal multi-op workload
- `gk_bindings.yaml` — Native GK DAG syntax for bindings
- `inline_ops.yaml` — Inline `op=` expressions and ratio prefixes

### workloads/
- `math_and_bitwise.yaml` — All infix operators, auto-widening
- `cartesian_space.yaml` — Mixed-radix coordinate decomposition
- `conditional_ops.yaml` — `if:` field for per-cycle op skipping
- `feature_showcase.yaml` — Phases, scenarios, params, conditions
- `service_model.yaml` — Multi-table, ratios, `delay:` latency injection

### workloads/signals/
- `fourier_analysis.yaml` — FFT analysis of fractal noise
- `lfsr.yaml` — Galois LFSR with bitwise GK ops

### workloads/visual/
- `maze.yaml` — Classic random slash maze pattern

## modules/

GK module files (`.gk`) and workloads that exercise them.

- `hashed_id.gk` — Example GK module (deterministic hashed ID)
- `euler_circuit.gk` — Euler circuit GK module
- `module_test.yaml` — Adjacent `.gk` file resolution
- `stdlib_test.yaml` — Embedded stdlib module resolution
