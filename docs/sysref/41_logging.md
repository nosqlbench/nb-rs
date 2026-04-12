# 41: Logging and Diagnostics

Diagnostic output uses stderr for operational messages and
structured event streams for compiler transparency.

---

## Operational Logging

All operational messages go to stderr:

```
vectordata: resolved 'glove-25-angular' → https://...
1 ops, 100 cycles, concurrency=10, adapter=cql
cassnbrs: connecting to 127.0.0.1 (keyspace: <none>)
cassnbrs: done.
```

### Conventions

- Prefix with subsystem: `vectordata:`, `cassnbrs:`, `validation:`
- Warnings: `warning: <message>`
- Errors: `error: <message>` followed by context
- No timestamps in operational messages (the activity has metrics
  for timing)
- Adapter output goes to stdout; diagnostics to stderr

---

## GK Compiler Diagnostics

The compiler emits structured `CompileEvent` values explaining
each compilation step:

```rust
pub enum CompileEvent {
    Parsed { node: String, function: String },
    BindingResolved { name: String, source: String },
    ModuleInlined { module: String, prefix: String },
    TypeAdapterInserted { from: String, to: String, node: String },
    ConstantFolded { node: String, value: String },
    FusionApplied { pattern: String, nodes: Vec<String> },
    CompileLevelSelected { node: String, level: String },
    OutputSelected { name: String, consumers: Vec<String> },
}
```

### --explain Mode

`nbrs bench --explain <expr>` dumps the event stream to stderr:

```
$ nbrs bench --explain "hash(cycle)" cycles=1
[parsed]    cycle → graph input #0
[parsed]    hash  → Hash64 node
[wired]     hash.input[0] ← input:cycle
[output]    hash  → selected as program output
[compiled]  1 node, 1 output, 0 constants folded
```

Shows parsing, wiring, type adaptation, constant folding, fusion,
and output selection decisions.

### GK Compiler Events

| Event | Level | Description |
|-------|-------|-------------|
| Parsed | Info | AST created |
| BindingResolved | Info | Name → node |
| ModuleInlined | Info | Module expanded |
| TypeAdapterInserted | Advisory | Auto-coercion |
| TypeWidening | Advisory | u64→f64 promotion |
| ConstantFolded | Info | Init-time eval |
| FusionApplied | Info | DAG rewrite |
| ConfigWireCycleWarning | Warning | Config wire perf |
| Warning | Warning | General |

Query advisories: `nbrs bench gk file.gk --explain`

---

## Validation Diagnostics

### End-of-Run Summary

When validation is active, a summary prints after all fibers
complete:

```
  recall@100: mean=0.9385 p50=0.9503 p99=1.0000 min=0.7800 max=1.0000 (n=100)
  precision@100: mean=0.9385 ...
validation: 100 passed, 0 failed
```

### Hard Errors

Missing ground truth, empty result extraction, and similar
validation setup problems are hard errors (not silent zeros):

```
error: [op] [relevancy_error] relevancy: no ground truth for
'ground_truth'. Available fields: ["prepared"].
```

### Extraction Warnings

First occurrence of empty result extraction logs a warning with
a result preview:

```
warning: relevancy: no values extracted for field 'key' from result
  result preview: [{"key":"abc",...}]
```

---

## Error Router Logging

The error router controls which errors produce log output:

- `warn` action: logs to stderr with error name, message, cycle
- `ignore` action: suppresses logging (error still counted)
- `stop` action: logs and halts activity
- All errors always counted regardless of logging config
