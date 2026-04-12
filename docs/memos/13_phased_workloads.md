# Memo 13: Phased Workloads

How nb-rs should support multi-phase workload execution
(schema → rampup → main). Based on study of nosqlbench's
approach, with departures where GK makes things simpler.

---

## What nosqlbench does

### YAML structure

```yaml
scenarios:
  default:
    schema: run driver=cql tags==block:schema threads==1 cycles==UNDEF
    rampup: run driver=cql tags==block:rampup cycles===TEMPLATE(rampup-cycles,10000000) threads=auto
    main:   run driver=cql tags==block:"main.*" cycles===TEMPLATE(main-cycles,10000000) threads=auto

blocks:
  schema:
    params:
      prepared: false
    ops:
      create_keyspace: |
        create keyspace if not exists ...
      create_table: |
        create table if not exists ...

  rampup:
    params:
      cl: LOCAL_QUORUM
    ops:
      rampup_insert: |
        insert into ... (key, value) values ({seq_key},{seq_value});

  main_read:
    params:
      ratio: 5
      cl: LOCAL_QUORUM
    ops:
      main_select: |
        select * from ... where key={rw_key};

  main_write:
    params:
      ratio: 5
      cl: LOCAL_QUORUM
    ops:
      main_insert: |
        insert into ... (key, value) values ({rw_key}, {rw_value});
```

### Key mechanisms

1. **Scenarios** are named sequences of steps. Each step is a
   `run` command string with phase-specific overrides for driver,
   tags, cycles, threads, error handling.

2. **Blocks** group ops by purpose. Each block has its own params
   and tags. The automatic `block` tag lets phases select which
   ops to run.

3. **Tag filtering** (`tags==block:schema`) selects ops from
   specific blocks. Regex supported (`block:"main.*"` matches
   `main_read` and `main_write`).

4. **Ratio** weighting: `main_read` ratio 5 + `main_write`
   ratio 5 = 50/50 read/write mix within the `main` phase.

5. **Parameter operators**: `=` (overridable), `==` (locked
   silent), `===` (locked error). Schema always runs single-
   threaded (`threads==1`), user can't override.

6. **TEMPLATE()** for parameterized defaults:
   `TEMPLATE(rampup-cycles,10000000)` defaults to 10M but
   user can override with `rampup-cycles=1000`.

7. **UNDEF** removes a parameter entirely: `cycles==UNDEF`
   means "run until ops are exhausted" (one stanza).

8. **Each phase is a separate Activity**: independent thread
   pool, cycle counter, rate limiter, metrics scope. Phases
   run sequentially. No state carries between phases except
   what's in the database.

---

## What nb-rs should do differently

### Don't replicate the command-string DSL

nosqlbench's scenario steps are mini command lines embedded in
YAML strings. This is powerful but opaque — the syntax is
effectively a fourth language (YAML + CQL + binding DSL +
scenario command strings). nb-rs should keep phase configuration
as structured YAML, not embedded command strings.

### Use GK for everything that nosqlbench uses bindings for

nosqlbench has separate bindings per block. nb-rs has one GK
program per workload. Different phases select different GK
outputs — the GK program compiles once, phases are just
different views into it.

### Use GK config expressions for computed phase params

nosqlbench needs scripting (Groovy/JS) to compute phase params
from data. nb-rs has `cycles={train_count}` — GK folded
constants flow into activity config directly.

---

## Proposed YAML structure

```yaml
params:
  dataset: sift1m

bindings: |
  inputs := (cycle)
  train_count := vector_count("{dataset}")
  dim := vector_dim("{dataset}")
  base_vec := vector_at(cycle, "{dataset}")
  query_vec := query_vector_at(cycle, "{dataset}")

scenarios:
  default:
    - schema
    - rampup
    - main

phases:
  schema:
    tags: block:schema
    cycles: 1
    concurrency: 1
    ops:
      create_keyspace:
        raw: |
          CREATE KEYSPACE IF NOT EXISTS test
          WITH replication = {'class': 'SimpleStrategy',
          'replication_factor': 1}
      create_table:
        raw: |
          CREATE TABLE IF NOT EXISTS test.vectors (
            id bigint PRIMARY KEY,
            vec vector<float, {dim}>
          )

  rampup:
    tags: block:rampup
    cycles: "{train_count}"
    concurrency: 100
    ops:
      insert_vector:
        stmt: |
          INSERT INTO test.vectors (id, vec)
          VALUES ({cycle}, {base_vec})

  main:
    tags: block:main
    cycles: 10000
    concurrency: 50
    rate: 5000
    ops:
      ann_query:
        stmt: |
          SELECT id FROM test.vectors
          ORDER BY vec ANN OF {query_vec} LIMIT 100
```

### How this differs from nosqlbench

| Aspect | nosqlbench | nb-rs |
|--------|-----------|-------|
| Phase definition | Command string: `run driver=cql tags==block:schema threads==1` | Structured YAML map: `concurrency: 1` |
| Phase sequencing | Scenario steps are ordered strings | Scenario is a list of phase names |
| Bindings | Per-block, Java binding chain syntax | One GK program, shared across phases |
| Computed params | Groovy scripting | GK config expressions: `cycles: "{train_count}"` |
| Op selection | Tag regex filter | Phase-level ops (inline) or tag filter |
| Ratio weighting | Block param `ratio: 5` | Same, or use GK `weighted_pick` |
| Parameter locking | `==` / `===` operators | Not needed — YAML structure is explicit |
| TEMPLATE defaults | `TEMPLATE(name, default)` | `params:` section with CLI override |

### Scenarios

A scenario is just a named list of phase names that run in
order:

```yaml
scenarios:
  default:
    - schema
    - rampup
    - main
  quick_check:
    - schema
    - rampup_small
    - main_short
```

CLI: `nbrs workload.yaml default` or `nbrs workload.yaml quick_check`.
Default scenario is `default` if not specified.

### Phases

Each phase is a named section with:

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `cycles` | u64 or `"{gk_const}"` | stanza length | Total ops to execute |
| `concurrency` | usize | 1 | Async fibers |
| `rate` | f64 | unlimited | Ops/sec rate limit |
| `adapter` | string | inherited | Override adapter for this phase |
| `errors` | string | `".*:warn,counter"` | Error routing spec |
| `tags` | string | — | Tag filter to select ops from blocks |
| `ops` | map | — | Inline ops for this phase |

Phases can define ops inline OR select from top-level `blocks:`
via tags. If both `ops` and `tags` are present, only `ops` is
used.

### GK program sharing

All phases share the same compiled GK program. The program is
compiled once from the workload's `bindings:` section. Each
phase creates its own fiber pool from the shared program.

Benefits:
- Dataset handles loaded once (train vectors, query vectors)
- Constant folding computed once
- Output namespace is unified across phases

### Phase execution model

```
for phase_name in scenario:
    phase = phases[phase_name]
    adapter = resolve_adapter(phase)
    ops = resolve_ops(phase, blocks)
    config = ActivityConfig {
        cycles: resolve_gk_config(phase.cycles),
        concurrency: phase.concurrency,
        rate: phase.rate,
        ...
    }
    activity = Activity::new(config, ops)
    activity.run_with_driver(adapter, program.clone()).await
    // blocks until phase completes
```

Each phase is a sequential blocking call. The next phase starts
only after the previous one finishes.

---

## Blocks (optional, for op reuse)

For workloads where phases share op definitions or where ops are
grouped by function, use top-level `blocks:`:

```yaml
blocks:
  schema:
    ops:
      create_ks: ...
      create_table: ...
  rampup:
    ops:
      insert: ...
  read:
    params:
      ratio: 8
    ops:
      select: ...
  write:
    params:
      ratio: 2
    ops:
      insert: ...

phases:
  schema:
    tags: block:schema
    concurrency: 1
    cycles: 1
  rampup:
    tags: block:rampup
    cycles: "{train_count}"
    concurrency: 100
  main:
    tags: "block:read|block:write"
    cycles: 1000000
    concurrency: 50
```

The `main` phase selects ops from both `read` and `write`
blocks. Their `ratio` params (8:2) determine the read/write
mix within the stanza.

---

## What this solves

The `vector_inspect.yaml` problem: summary repeats because all
ops are in one stanza. With phases:

```yaml
phases:
  summary:
    cycles: 1
    concurrency: 1
    ops:
      header: { stmt: "=== Dataset: {dataset} ===" }
      dims:   { stmt: "  dimensions: {dim}" }
      count:  { stmt: "  vectors: {base_count}" }

  iterate:
    cycles: "{mi_len}"
    concurrency: 1
    ops:
      entry: { stmt: "  match[{cycle}] = base[{base_ordinal}]" }
```

Summary runs once. Iteration runs {mi_len} times. Each is a
separate phase with its own cycle count.

---

## Implementation sketch

### YAML parsing

Add to `nb-workload/src/parse.rs`:

```rust
pub struct WorkloadPhase {
    pub name: String,
    pub cycles: Option<String>,      // literal or "{gk_const}"
    pub concurrency: Option<usize>,
    pub rate: Option<f64>,
    pub adapter: Option<String>,
    pub errors: Option<String>,
    pub tags: Option<String>,
    pub ops: Option<Vec<ParsedOp>>,  // inline ops
}

pub struct Scenario {
    pub name: String,
    pub phases: Vec<String>,         // phase names in order
}
```

### Runner changes

`nb-rs/src/run.rs` becomes a phase loop:

```rust
let scenario = resolve_scenario(&workload, scenario_name);
for phase_name in &scenario.phases {
    let phase = &workload.phases[phase_name];
    let ops = resolve_phase_ops(phase, &workload.blocks);
    let cycles = resolve_gk_config(phase.cycles, &kernel);
    let config = build_activity_config(phase, cycles);
    let activity = Activity::new(config, ops);
    activity.run_with_driver(adapter.clone(), program.clone()).await;
}
```

### Backward compatibility

Workloads without `phases:` or `scenarios:` continue to work
exactly as today — single implicit phase with all ops.

---

## Open questions

1. **Adapter per phase?** Schema DDL needs CQL. Should each
   phase declare its adapter, or inherit from CLI?

2. **Metrics scoping?** Should each phase have its own metrics
   namespace (`schema.cycles`, `rampup.cycles`, `main.cycles`)?

3. **Error escalation?** If schema fails, should rampup be
   skipped? Currently each phase would just fail independently.

4. **Block-level bindings?** nosqlbench allows per-block
   bindings. nb-rs has one GK program. Should blocks be able
   to add GK bindings, or is the shared program sufficient?

5. **Parameterized phase config?** Should phases support
   TEMPLATE-like defaults? Or is `params:` + CLI override
   enough?
