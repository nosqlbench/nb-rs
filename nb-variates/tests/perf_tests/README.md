# GK Performance Tests

Canonical benchmark scenarios for the GK evaluation engine.
Each `.gk` file is a self-describing test that specifies the
graph under test, how its inputs are driven, and which outputs
are pulled.

## Running

```bash
# Full suite with provenance comparison
nbrs bench gk "nb-variates/tests/perf_tests/*.gk" --compare iters=5

# Single scenario
nbrs bench gk nb-variates/tests/perf_tests/a2_stable_params.gk --compare iters=7

# Without provenance (raw engine baseline)
nbrs bench gk nb-variates/tests/perf_tests/a1_single_input.gk --no-provenance iters=5

# With flamegraph profiling
nbrs bench gk nb-variates/tests/perf_tests/c5_multi_root.gk --profile
```

## File Format

Each `.gk` file has three sections marked by `/// @` headers.

### @driver

A GK program (commented with `///`) that generates input values
for the test graph. The bench harness strips the `///` prefix,
prepends `inputs := (meta)`, compiles it, and evaluates it per
cycle with `meta = cycle_number`.

Each output name maps positionally to the test graph's declared
inputs. Constants produce stable inputs; expressions involving
`meta` produce changing inputs.

```
/// @driver
/// cycle := meta              ← changes every eval
/// ks := 42                   ← constant (stable)
/// rate := mod(meta, 100)     ← changes every 100 evals
```

The driver's per-cycle cost is measured independently and
subtracted from all reported timings, so results reflect only
the test graph's evaluation cost.

### @selector / @pull_weights

Controls which outputs are pulled each cycle. Two formats:

**@pull_weights** maps output names to frequency weights.
The bench selects one output per cycle by weighted random
(deterministic from cycle number via hash).

```
/// @pull_weights
/// hot_out: 80
/// warm_out: 15
/// cold_out: 5
```

**@selector** is a GK program (not yet compiled by the harness —
currently informational). Future: the harness will compile and
run it to select outputs dynamically.

### @graph

The actual GK program under test. Standard `.gk` syntax.
Everything after `/// @graph` (or the first uncommented line)
is the test graph.

```
/// @graph
inputs := (cycle, ks, rate)
h := hash(cycle)
...
out := mod(h, 997)
```

## Test Scenarios

### Group A: Provenance Sensitivity

| File | Inputs | Stable | Nodes | Tests |
|------|--------|--------|-------|-------|
| a1_single_input | 1 | 0% | 50 | Provenance overhead baseline |
| a2_stable_params | 3 | 67% | 49 | Provenance payoff with stable inputs |
| a3_all_changing | 3 | 0% | 49 | All inputs change every cycle |
| a4_mostly_stable | 5 | 80% | 101 | Best case for provenance |
| a5_mixed_rates | 4 | varies | 80 | Inputs change at different cadences |

### Group B: Output Access Patterns

| File | Outputs | Pattern | Nodes | Tests |
|------|---------|---------|-------|-------|
| b1_hot_cold_outputs | many | 80/15/5 weighted | 38 | Uneven output access |
| b2_selective_cone | 2 | 90/10 weighted | 100 | Small hot cone, large cold cone |

### Group C: Topology Scaling

| File | Topology | Nodes | Tests |
|------|----------|-------|-------|
| c1_deep_chain | linear chain | 200 | Depth scaling |
| c5_multi_root | 4 separate trees | 201 | Per-input subgraph caching |

### Group D: Realistic Workloads

| File | Pattern | Nodes | Tests |
|------|---------|-------|-------|
| d1_keyvalue | INSERT bindings | 9 | Typical small workload |
| d4_multi_table | 4-table stanza | 44 | Multi-op workload |

## Interpreting Results

The bench outputs min/median/p99 ns per cycle across multiple
iterations. With `--compare`, each engine level shows both
raw and provenance-enabled variants:

```
  Level            min ns  median ns   p99 ns  Speedup     ops/s
  ---------------------------------------------------------------
  P1               329.4     338.0     341.3     1.0x   3036219
  P1/prov           87.0      87.1      87.9     3.8x  11493436
  P2               434.0     434.1     435.6     0.2x   2304207
  P2/prov          201.0     201.1     201.2     0.4x   4975273
  P3               191.9     196.8     198.0     0.5x   5210930
  P3/prov          142.4     144.6     148.8     0.6x   7021444
```

**Speedup** is relative to P1 (first row). Values > 1.0 mean
faster than baseline.

**Driver overhead** is reported and subtracted automatically.
The harness runs the @driver graph alone first, measures its
per-cycle cost, and subtracts it from all results.

## Adding Scenarios

1. Create a `.gk` file with `@driver`, `@pull_weights`, and
   `@graph` sections
2. The driver outputs must match the graph's input names in
   order
3. Run with `nbrs bench gk <file> --compare` to verify
4. Commit to this directory

The driver is real GK — any expression the compiler supports
can be used to generate input patterns.
