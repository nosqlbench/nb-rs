# 22: Op Sequencing

The op sequence maps cycle numbers to op templates. It defines
the stanza structure and controls the ratio of different
operations in the workload.

---

### Design Note: Dynamic and Non-Uniform Stanzas

The current model (fixed ratio LUT, uniform stanzas) is
deterministic, efficient, and understandable. However it does not
support:

- **Dynamic op injection**: adding ops to the stanza at runtime
  based on results or conditions (e.g., "if SELECT returns empty,
  inject an INSERT")
- **Non-uniform stanzas**: stanzas that vary in structure across
  iterations (e.g., "first 1000 stanzas do setup, then switch to
  steady-state mix")
- **Conditional branching**: choosing between op paths based on
  a GK-evaluated condition

These capabilities are needed for realistic service simulation
workloads. The design should generalize the stanza contract to
allow non-uniform execution while preserving the current model as
the default (simple, deterministic) case. See
`docs/memos/05_dynamic_stanza_execution.md` for the design.

---

## Stanza Model

A **stanza** is one complete pass through all active op templates.
The stanza length is the sum of all op ratios.

```yaml
ops:
  read:                    # ratio 4 (default or explicit)
    tags: { phase: main }
    prepared: "SELECT ..."
  write:                   # ratio 1
    tags: { phase: main }
    prepared: "INSERT ..."

# Stanza length = 4 + 1 = 5 cycles
# Each stanza: 4 reads + 1 write (interleaved by sequencer)
```

### Stanza Isolation

- Capture inputs reset at stanza boundaries
- Captures flow within a stanza (op A's output feeds op B)
- Captures do NOT leak across stanza boundaries
- Each fiber processes one stanza at a time

---

## Cycle-to-Template Mapping

`OpSequence` maps cycle numbers to template indices via a
precomputed lookup table (LUT). O(1) per cycle.

```rust
pub struct OpSequence {
    templates: Vec<ParsedOp>,
    lut: Vec<usize>,  // cycle % lut.len() → template index
}

impl OpSequence {
    pub fn get_with_index(&self, cycle: u64) -> (usize, &ParsedOp) {
        let idx = self.lut[(cycle % self.lut.len() as u64) as usize];
        (idx, &self.templates[idx])
    }

    pub fn stanza_length(&self) -> usize {
        self.lut.len()
    }
}
```

---

## Sequencer Types

### Bucket (default)

Interleaved distribution. With ratios 3:1 (read:write):

```
R R R W R R R W R R R W ...
```

Distributes ops as evenly as possible across the stanza.

### Interval

Similar to bucket but with fractional positioning. Produces
slightly different interleaving patterns for non-power-of-two
ratios.

### Concat

Sequential blocks. Same 3:1 ratio:

```
R R R W W R R R W W ...
```

All reads, then all writes, per stanza. Useful when operation
order within a stanza matters (e.g., setup then verify).

---

## Default Cycles

When `cycles` is not specified on CLI or in workload params:

1. Check workload params for `cycles` key
2. Default to one stanza length (sum of all op ratios)

One stanza ensures every op template executes at least once.

---

## Tag Filtering

Before sequencing, ops are filtered by tag expressions:

```
tags=phase:rampup         → only ops with phase=rampup
tags=phase:main,type:read → ops matching BOTH conditions
```

After filtering, only the matching ops form the op sequence.
This is how workloads define separate phases (schema, rampup,
search) in a single YAML file, selected at runtime.

Filtered ops retain their original ratios. A 4:1 read:write
workload filtered to `type:read` produces a sequence of all
reads with no writes.

---

## Phased Execution

A workload can define multiple **phases** — sequential execution
stages with independent cycle counts, concurrency, and op sets.
Phases share a single compiled GK program.

### YAML Structure

```yaml
params:
  dataset: sift1m

bindings: |
  inputs := (cycle)
  train_count := vector_count("{dataset}")
  dim := vector_dim("{dataset}")

scenarios:
  default:
    - schema
    - rampup
    - search

phases:
  schema:
    cycles: 1
    concurrency: 1
    ops:
      create_table:
        raw: "CREATE TABLE IF NOT EXISTS ..."

  rampup:
    cycles: "{train_count}"
    concurrency: 100
    ops:
      insert:
        stmt: "INSERT INTO t (id, vec) VALUES ({cycle}, {base_vec})"

  search:
    cycles: 10000
    concurrency: 50
    rate: 5000
    ops:
      ann_query:
        stmt: "SELECT id FROM t ORDER BY vec ANN OF {query_vec} LIMIT 100"
```

### Scenarios

A scenario is a named ordered list of phase names:

```yaml
scenarios:
  default:    [schema, rampup, search]
  quick:      [schema, rampup_small]
```

CLI: `nbrs run workload.yaml scenario=quick`. Default is
`default`. If no `scenarios:` section exists, all phases run
in YAML definition order.

### Phase Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `cycles` | u64 or `"{gk_const}"` | stanza length | Total stanzas to execute |
| `concurrency` | usize | 1 | Async fibers |
| `rate` | f64 | unlimited | Ops/sec rate limit |
| `adapter` | string | inherited from CLI | Override adapter |
| `errors` | string | `".*:warn,counter"` | Error routing spec |
| `tags` | string | — | Tag filter to select from top-level ops |
| `ops` | map | — | Inline ops for this phase |

Phases can define ops **inline** or select from top-level ops
via **tag filter**. Inline ops take precedence.

### GK Program Sharing

All phases share one compiled GK program. Benefits:
- Dataset handles loaded once
- Constant folding computed once
- Output namespace unified across phases
- GK config expressions (`cycles: "{train_count}"`) resolve
  from the shared program's folded constants

### Execution Model

```
for phase_name in scenario:
    phase = phases[phase_name]
    ops = resolve_ops(phase)        // inline or tag-filtered
    config = ActivityConfig { cycles, concurrency, rate, ... }
    activity = Activity::new(config, ops)
    activity.run_with_driver(adapter, program.clone()).await
    // blocks until phase completes
```

Each phase runs to completion before the next starts. Each
phase gets its own fiber pool, cycle counter, and rate limiter.

### Stanza Isolation Across Phases

Capture inputs reset at stanza boundaries within a phase.
Between phases, all state resets — each phase starts with
fresh fiber builders from the shared program.

### Backward Compatibility

Workloads without `phases:` or `scenarios:` run as a single
implicit phase with all ops — identical to the non-phased model.

---

## Cursor-Driven Sequencing

When a phase declares `cursor` bindings in its GK graph, the
sequencing model shifts from counter-driven to cursor-driven:

- **Phase extent** is determined by cursor exhaustion, not `cycles:`
- **Cycle counter** is replaced by cursor ordinals
- **LUT dispatch** is preserved when `ratio:` is present on ops
- **Stanza scheduling** becomes one of several strategies:
  - **LUT/Bucket**: classic weighted ratios (when `ratio:` present)
  - **Sequential**: declaration order (for capture dependency chains)
  - **Cursor-driven**: one cursor advance per dispatch

The executor infers the scheduler from the op configuration.
No user configuration needed.

### Cursors Provenance Tracing

At phase setup, the executor builds a `Cursors` instance by
tracing GK provenance from the op template's referenced fields
back to root cursor nodes. Each cursor target is a `DataSource`
reader paired with its GK input index. Only cursors whose
ordinals transitively feed the requested output fields are
advanced on each iteration. This means:

- A phase with two cursors (e.g., `base` and `queries`) where
  an op only uses `base`-derived fields will only advance the
  `base` cursor per cycle.
- Phase completion occurs when any targeted cursor is exhausted.

### Batch Budget

Cursor-driven phases support budget-based batching via
`CqlBatchDispenser`:

```yaml
ops:
  insert:
    batch:
      max_rows: 100
      type: unlogged
    prepared: "INSERT INTO ..."
```

The executor advances the cursor repeatedly, evaluating the
GK graph per position, rendering and binding each statement,
and accumulating rows until the batch budget is reached. The
batch is then executed as a single CQL BATCH call. Batch size
is controlled by `max_rows` on the op template. Each batch
execution records amortized per-row latency for throughput
reporting.

The `CqlBatchDispenser` uses `ResolvedFields::batch_fields`
to receive the expanded field sets — one per cursor advance
within the batch window. The base `ResolvedFields` contains
the fields from the first cycle in the batch.
