# 31: Op Execution Pipeline

The execution pipeline flows from workload parsing through GK
compilation, dispenser creation, wrapping, and per-cycle execution.

---

## Init-Time Pipeline

```
ParsedOp[]
  │
  ├── Compile GK bindings ──▶ GkKernel ──▶ GkProgram (Arc)
  │     (scans op fields AND params for {name} references — see SRD 13c
  │      §"Auto-Extern Generation"; bind point scanner already walks
  │      the full template, so the kernel knows every name referenced
  │      by any consumer)
  │
  ├── Build OpSequence ──▶ cycle → template LUT
  │
  └── For each unique template:
        │
        ├── adapter.map_op(template) ──▶ raw OpDispenser
        │
        ├── ScopeFixture::new(kernel)        ◀── consumer self-registration
        │     │
        │     ├── TraversingDispenser::fixture(template, &mut fx)
        │     ├── ValidatingDispenser::fixture(template, &mut fx)
        │     ├── ConditionalDispenser::fixture(template, &mut fx)
        │     ├── ThrottleDispenser::fixture(template, &mut fx)
        │     │   (each consumer registers names it reads at cycle time;
        │     │    unknown names ⇒ Err, no silent drops)
        │     │
        │     └── fixture.seal() ──▶ PullPlan (ordered, deduplicated)
        │
        └── Final wrapped dispenser stored in dispensers[]
              (each wrapper holds the PullHandles its fixture returned)
```

The fixture is the **net product of all consumers' scope-init
preparation**. There is no top-level coordinator that gathers names
"on behalf of" a consumer — each consumer is responsible for its
own scoping against the GK context. Activity construction iterates
the registered consumer set and seals the fixture at the end. See
[32: Dispenser Wrappers](32_wrappers.md) §"Init-Time Fixture and
Consumer Self-Registration" for the trait contract.

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
4. Resolve       ── two parallel index-driven materializations from
                    GkState; both are O(plan_size), no name hashing:
                    ├── BindPlan::resolve(state)  → ResolvedFields
                    │     (op-field substitution; positional view for
                    │      the inner adapter — prepared-statement order
                    │      matters here)
                    └── PullPlan::resolve(state)  → ResolvedPulls
                          (name-free handle access for outer wrappers;
                           PullHandle is a Copy index into the plan)
5. Execute       ── dispenser.execute(cycle, &ExecCtx { fields, pulls })
                    → ConditionalDispenser (reads `if` via PullHandle)
                      → ValidatingDispenser (reads ground truth via
                                             PullHandle; assertions
                                             on result body)
                        → TraversingDispenser (counts, captures)
                          → adapter dispenser (CQL/HTTP/stdout — sees
                                               only `fields`, never
                                               `pulls`)
                    → Result<OpResult, ExecutionError>
6. Metrics       ── service_time, wait_time, response_time
                    (skipped ops: only cycles_total + skips_total)
7. Captures      ── store result.captures via fiber.capture()
8. Error         ── route through ErrorRouter if Err
```

### Pull plan vs bind plan

The two dynamic-pull plans look similar but serve different
contracts and must not be conflated:

| | `BindPlan` → `ResolvedFields` | `PullPlan` → `ResolvedPulls` |
|---|---|---|
| Scope | Names referenced in op fields (`op.values()`) | Names registered by wrappers via `ScopeFixture` |
| Access shape | Positional, by column order in the prepared statement | By `PullHandle` (Copy, index into plan) |
| Consumer | Innermost adapter dispenser | Outer wrappers (validation, conditional, throttle, …) |
| Why distinct | Adapters need slot-ordered typed values that match the prepared statement; consumers need by-name handles resolved at init | A wrapper's read should not depend on whether the same name happens to appear in op fields |

Both are populated from the same `GkState` per cycle; the GK
kernel remains the single canonical source of values. A name
that appears in both an op field and a wrapper config is pulled
once from the kernel and observed via two independent
materializations of the cycle's value snapshot. Eventual
unification (slice α.7+) is open work; the contracts above are
load-bearing and should be preserved by any future merge.

**Historical note:** `resolve_with_extras` and the
`extra_bindings` side channel — formerly threaded from the
activity layer into a single `ResolvedFields` for both adapter
and wrapper reads — are removed by slice α.4. The two-plan
shape above replaces them. SRD 13c §"Open Design Issue" has
related context on the kernel's read-API consolidation;
`PullPlan` here sits *above* the kernel's `lookup` and is
agnostic to the kernel's internal storage-strategy split.

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

**Implementation:** `ConditionalDispenser::fixture` registers the
condition name with the scope fixture and stores the returned
`PullHandle`. At cycle time, the wrapper reads
`ctx.pulls.get(self.condition_handle)` and returns
`OpResult::skipped()` if falsy. The condition is invisible to the
inner adapter — adapters see only `ctx.fields`.

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
