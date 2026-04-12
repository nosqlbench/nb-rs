# 31: Op Execution Pipeline

The execution pipeline flows from workload parsing through GK
compilation, dispenser creation, wrapping, and per-cycle execution.

---

## Init-Time Pipeline

```
ParsedOp[]
  │
  ├── Compile GK bindings ──▶ GkKernel ──▶ GkProgram (Arc)
  │     (scans op fields AND params for {name} references)
  │
  ├── Build OpSequence ──▶ cycle → template LUT
  │
  └── For each unique template:
        │
        ├── adapter.map_op(template) ──▶ raw OpDispenser
        │
        ├── TraversingDispenser::wrap(raw, template, stats)
        │     (element/byte counting, capture extraction)
        │
        ├── ValidatingDispenser::wrap(traversed, template, labels)
        │     (assertions, relevancy — only if declared)
        │
        ├── Collect extra_bindings for validation needs
        │
        └── Final dispenser stored in dispensers[]
```

### Wrapping Order

Wrappers compose outside-in. The executor calls the outermost:

```
ValidatingDispenser  (outermost — runs after inner completes)
  └── TraversingDispenser  (counts elements/bytes, extracts captures)
        └── raw OpDispenser  (adapter-specific execution)
```

Traversal completes before validation sees the result. Captures
are populated before assertions check them.

---

## Cycle-Time Pipeline

Per cycle within a fiber:

```
1. Rate limit    ── acquire token (if rate= configured)
2. Select        ── op_sequence.get_with_index(cycle) → (idx, template)
3. Resolve       ── fiber.set_inputs([cycle])
                    fiber.resolve_with_extras(template, extras[idx])
                    → ResolvedFields (from GK outputs, captures, globals)
4. Execute       ── dispenser.execute(cycle, &fields)
                    → ValidatingDispenser
                      → TraversingDispenser
                        → adapter dispenser (CQL/HTTP/stdout)
                    → Result<OpResult, ExecutionError>
5. Metrics       ── service_time, wait_time, response_time
6. Captures      ── store result.captures in fiber.capture_context
7. Error         ── route through ErrorRouter if Err
```

**Design note:** `resolve_with_extras` exists because validation
needs GK outputs not referenced in op fields. When the GK kernel
becomes the unified state holder (sysref 10), all outputs would
be available through a single resolution path, eliminating
the "extras" mechanism.

### Dependency Group Processing

Ops within a stanza are partitioned into **dependency groups** at
init time based on capture analysis (see `linearize.rs`). Groups
execute sequentially with capture application between them; ops
within a group execute concurrently.

```
Group 0: [read_user, insert_log]  ← independent, concurrent
  (apply captures)
Group 1: [update_user]            ← depends on read_user's capture
  (apply captures)
Group 2: [verify]                 ← depends on update_user's capture
```

If an upstream group fails and its captures are not produced,
downstream groups that require those captures are **skipped** —
recorded as `upstream_capture_missing` errors with the missing
capture names. This prevents executing ops with known-missing
inputs.

---
ø
## Extra Bindings

>> This section needs to be updated or removed depending on our recent consolidation work with GK

Validation and other non-adapter consumers may need GK outputs
that aren't referenced in op fields. The pipeline handles this:

1. **Init time**: `validation::extra_bindings(template)` returns
   binding names needed (e.g., `["ground_truth"]` from
   `relevancy.expected: "{ground_truth}"`)
2. **Compile time**: Binding compiler scans both `op` fields AND
   `params` for `{name}` references, ensuring all needed bindings
   are compiled into the GK program
3. **Cycle time**: `resolve_with_extras(template, extras)` pulls
   extra GK outputs into `ResolvedFields` alongside op fields
4. **Execution**: Validation wrapper reads from `ResolvedFields`
   by name — adapter never sees the extra fields

**Design note:** The extra bindings mechanism is interim. The
GK-as-unified-state-holder design (sysref 10) would make all
GK outputs available through the standard resolution path,
eliminating the need for a separate "extras" list.

---

## Activity Metrics

Recorded per cycle by the executor:

| Metric | Type | Description |
|--------|------|-------------|
| `service_time` | Timer | Adapter execution duration (all ops) |
| `result_success_time` | Timer | Execution time for successful ops only |
| `wait_time` | Timer | Rate limiter wait |
| `response_time` | Timer | service + wait |
| `tries_histogram` | Histogram | Try count distribution (1=first try, 2+=retried) |
| `cycles_total` | Counter | Successful cycles |
| `errors_total` | Counter | Failed cycles (all error types) |
| `errors.{name}` | Counter | Per-error-type counters (created on demand) |
| `stanzas_total` | Counter | Stanzas started |
| `result_elements` | Counter | Elements from traversal |
| `result_bytes` | Counter | Bytes from traversal |

These metrics match nosqlbench's core instrumentation:
- `result_success_time` = nosqlbench `resultSuccessTimer`
- `tries_histogram` = nosqlbench `triesHistogram`
- `errors.{name}` = nosqlbench per-type error counters
