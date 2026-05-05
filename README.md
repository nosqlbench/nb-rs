# nbrs

High-performance workload generation and database testing in Rust.

nbrs generates deterministic, reproducible request streams at scale.
Every value is derived from a cycle number through a composable DAG
of functions — same cycle, same output, every time. This makes
workloads debuggable, cacheable, and exactly reproducible across runs.

Part of the [nosqlbench](https://github.com/nosqlbench/nosqlbench) project.

This system was derived from things learned building nosqlbench, and shares many of its concepts. However, some things were kept and some removed. This will be a much leaner and meaner version of what nosqlbench is. It will do some things differently.

## Quick Start

```
$ nbrs run op='INSERT INTO t (id, name) VALUES ({{mod(hash(cycle), 1000000)}}, "{{number_to_words(cycle)}}")' cycles=5

INSERT INTO t (id, name) VALUES (527897, "zero")
INSERT INTO t (id, name) VALUES (460078, "one")
INSERT INTO t (id, name) VALUES (564547, "two")
INSERT INTO t (id, name) VALUES (960189, "three")
INSERT INTO t (id, name) VALUES (862456, "four")
```

Or from a workload file:

```yaml
#!/usr/bin/env nbrs
# service.yaml

params:
  keyspace: demo
  table: users
  user_count: "100000"

bindings: |
  inputs := (cycle)
  user_id := mod(hash(cycle), {user_count})
  user_name := number_to_words(mod(hash(hash(cycle)), 1000))
  is_write := mod(cycle, 5)

ops:
  read_user:
    ratio: 4
    stmt: "SELECT * FROM {keyspace}.{table} WHERE id={user_id}"
  write_user:
    ratio: 1
    if: is_write
    stmt: "INSERT INTO {keyspace}.{table} (id, name) VALUES ({user_id}, '{user_name}')"
```

```
$ chmod +x service.yaml
$ ./service.yaml cycles=100 threads=4 rate=1000
```

## Features

**Generation Kernel (GK)** — A DAG-based data generation engine with:
- Infix operators (`+`, `-`, `*`, `/`, `%`, `**`, `&`, `|`, `^`, `<<`, `>>`)
- 100+ node functions: hash, distributions, noise, strings, vectors, CSV/JSONL
- Type-aware dispatch with auto-widening (u64/f64/string)
- Constant folding, provenance-based invalidation, JIT compilation
- Module system with composable `.gk` files and stdlib

**Workload Engine** — Flexible execution with:
- Phased workloads (schema → rampup → steady-state)
- Conditional ops (`if:` field skips per-cycle)
- Latency injection (`delay:` field for GK-driven think time)
- Ratio-weighted op sequencing
- Capture flow between ops within a stanza
- GK expressions in config (`cycles="{vector_count("example")}"`)

**Adapters** — Protocol drivers for:
- stdout (debugging, dry-run, format=json/csv/stmt)
- HTTP (REST APIs, configurable timeouts)
- CQL (Cassandra/ScyllaDB via nbrs persona)
- Model (simulated service latency)

**Observability** — Built-in metrics and dashboards:
- HDR histograms for latency percentiles
- OpenMetrics push to Prometheus/VictoriaMetrics
- Live TUI dashboard (`--tui`)
- Web dashboard (`nbrs web`)

## Build

```
cargo build --release
```

Enable shell completions:
```
eval "$(nbrs completions)"
```

## Commands

```
nbrs run workload=file.yaml cycles=1M threads=8 rate=10000
nbrs run op='hello {{hash(cycle)}}' cycles=10
nbrs bench gk 'hash(cycle)' --compare-modes iters=5
nbrs plot gk 'sin(to_f64(cycle) * 0.01)' cycles=1000
nbrs describe gk functions
nbrs web --daemon
```

## Examples

See [`examples/`](examples/) for categorized workload examples:
- `getting_started/` — First workloads, GK bindings, inline ops
- `gk_language/` — Operators, bitwise, coordinate decomposition
- `workloads/` — Phases, conditions, delays, scenarios
- `signals/` — FFT analysis, LFSR, fractal noise
- `visual/` — Random maze generator
- `modules/` — GK module system

## Architecture

```
nbrs-variates     GK engine: DAG compilation, node functions, JIT, provenance
nbrs-workload     YAML parsing, bind points, inline expressions, phasing
nbrs-activity     Async execution engine, dispenser wrappers, capture flow
nbrs-metrics      HDR histograms, frame capture, OpenMetrics export
nbrs-rate         Async token bucket rate limiter
nbrs-errorhandler Composable error routing
nb-rs           CLI binary (nbrs), bench, plot, web dashboard
nbrs-tui          Terminal UI for live monitoring
nbrs-web          Web dashboard with Axum + HTMX
```

## Functional Areas

Each of these areas has a distinctive design which has evolved from its nosqlbench form. This represents a capsule-form view of the user-facing elements which compose together to make a whole system.

### GK Modules

### Op Templates

#### Op Synthesis

### Scenarios

### Phases



## License

Apache-2.0
