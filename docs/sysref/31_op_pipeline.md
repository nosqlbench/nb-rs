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
ConditionalDispenser   (outermost — skips if `if:` is falsy)
  └── ValidatingDispenser  (assertions, relevancy checks)
        └── TraversingDispenser  (counts elements/bytes, extracts captures)
              └── raw OpDispenser  (adapter-specific execution)
```

`ConditionalDispenser` is only applied when the op template
declares an `if:` field. Ops without conditions skip this
wrapper entirely.

Traversal completes before validation sees the result. Captures
are populated before assertions check them.

---

## Cycle-Time Pipeline

Per cycle within a fiber:

```
1. Rate limit    ── acquire token (if rate= configured)
2. Select        ── op_sequence.get_with_index(cycle) → (idx, template)
3. Set inputs    ── fiber.set_inputs([cycle])
4. Resolve       ── fiber.resolve_with_extras(template, extras[idx])
                    → ResolvedFields (from GK outputs, captures)
5. Execute       ── dispenser.execute(cycle, &fields)
                    → ConditionalDispenser (checks `if:`, may skip)
                      → ValidatingDispenser (assertions, relevancy)
                        → TraversingDispenser (counts, captures)
                          → adapter dispenser (CQL/HTTP/stdout)
                    → Result<OpResult, ExecutionError>
6. Metrics       ── service_time, wait_time, response_time
                    (skipped ops: only cycles_total + skips_total)
7. Captures      ── store result.captures via fiber.capture()
8. Error         ── route through ErrorRouter if Err
```

**Design note:** `resolve_with_extras` exists because validation
needs GK outputs not referenced in op fields. When the GK kernel
becomes the unified state holder (sysref 10), all outputs would
be available through a single resolution path, eliminating
the "extras" mechanism.

### Conditional Op Execution

An op template can declare an `if` field that names a GK
binding. The `ConditionalDispenser` wrapper evaluates the
condition from the resolved fields and skips the op if falsy.

```yaml
ops:
  insert_default:
    if: should_insert
    stmt: "INSERT INTO t (id, val) VALUES ({id}, 'default')"
```

**Implementation:** The condition is resolved as part of normal
GK evaluation (included in `extra_bindings` for the op). The
`ConditionalDispenser` checks `fields.get_value(condition_name)`
and returns `OpResult::skipped()` if falsy. The condition field
is stripped from resolved fields before the inner dispensers see
it, so adapters never see internal condition values.

**Truthiness:** A value is truthy unless it is `0` (u64),
`0.0` (f64), `false` (bool), an empty string, or `None`.

**Metrics:** `cycles_total = success + skipped + errors`.
Skipped ops increment both `cycles_total` and `skips_total`.
No timing metrics (service/wait/response) recorded for skips.

**Captures:** A skipped op produces no captures. Downstream ops
that depend on captures from a skipped op will see the default
input values (or whatever was captured by earlier ops).

**Zero cost when unused:** Ops without an `if` field don't get
a `ConditionalDispenser` wrapper — no branch, no overhead.

**Use cases:**
- Conditional insert: `if: is_empty` (skip if SELECT found rows)
- Periodic check: `if: is_hundredth` where
  `is_hundredth := mod(cycle, 100)` (runs every 100 cycles,
  skips when mod=0)
- Feature flag: `if: enable_writes` (workload param controls
  whether writes execute)

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
