# SRD 29 — Model Adapter

## Purpose

The model adapter simulates operation execution for workload
prototyping, capture point testing, and full stanza flow validation
without requiring a real backend. It produces deterministic,
configurable results that feed into the capture system.

The model adapter **extends** the stdout adapter — it prints the
assembled op (like stdout) AND produces simulated results that
populate the capture context. This makes it the primary tool for
developing and debugging workloads before connecting to real
infrastructure.

## Result Definition via the `result` Op Field

The reserved op field `result` defines what a simulated operation
returns. Two forms:

### Map form (static results)

When `result` is a YAML mapping, it provides literal name-value
pairs that populate the capture context:

```yaml
ops:
  read_user:
    stmt: "SELECT [user_id], [balance] FROM users WHERE id={id};"
    result:
      user_id: 42
      balance: 1234.56
```

After executing this op, the capture context contains
`user_id=42` and `balance=1234.56`. These are available to
subsequent ops via `{capture:user_id}` or `{capture:balance}`.

Static results are useful for fixed test fixtures and simple
prototyping.

### GK form (computed results)

When `result` is a string (YAML block scalar), it is parsed as
GK grammar. The kernel generates the simulated result
deterministically from the op's inputs and the capture context:

```yaml
ops:
  read_user:
    stmt: "SELECT [user_id], [balance] FROM users WHERE id={id};"
    result: |
      user_id := id
      balance_h := hash(id)
      balance := scale_range(balance_h, 0.0, 10000.0)
```

The result GK kernel has access to:
- **Coordinates** (cycle, etc.)
- **Bind point values** from the op's primary GK kernel (e.g., `id`)
- **Capture context** from previous ops (volatile and sticky ports)

This means simulated results can depend on prior captures,
enabling full stanza-level flow modeling:

```yaml
ops:
  get_account:
    stmt: "SELECT [account_id], [tier] FROM accounts WHERE user={user_id};"
    result: |
      account_id := hash(user_id)
      tier := mod(hash(account_id), 3)

  get_balance:
    stmt: "SELECT [balance] FROM ledger WHERE account={capture:account_id};"
    result: |
      // account_id comes from previous op's capture
      balance := scale_range(hash(account_id), 0.0, 50000.0)
```

In strict mode, all result GK inputs must be fully qualified
(`capture:account_id`, `coord:cycle`, etc.).

## Result-Affecting Op Fields

All op fields that modify the model adapter's result behavior use
the `result-` prefix. The primary `result` field stands alone.

### `result-latency` — Latency Simulation

Configurable per-op latency to model realistic response times:

```yaml
ops:
  fast_read:
    stmt: "SELECT ..."
    result-latency: 2ms

  slow_query:
    stmt: "SELECT ..."
    result-latency: 50ms

  variable_latency:
    stmt: "SELECT ..."
    result-latency: normal(5.0, 1.0)   # ms, normally distributed
```

| Format | Meaning |
|--------|---------|
| `Nms` | Fixed delay of N milliseconds |
| `Nus` | Fixed delay of N microseconds |
| `normal(mean, stddev)` | Normally distributed delay (ms) |
| `uniform(min, max)` | Uniformly distributed delay (ms) |
| `0` or absent | No simulated latency (instant) |

Latency is injected after the result GK kernel evaluates and before
the result is returned to the executor. The delay appears in the
service time metric, modeling realistic adapter overhead.

### `result-error-rate`, `result-error-name`, `result-error-message` — Error Injection

Configurable per-op error rates for testing error handling paths:

```yaml
ops:
  flaky_write:
    stmt: "INSERT INTO ..."
    result-error-rate: 0.05              # 5% random failure
    result-error-name: "WriteTimeout"    # error class for error router
    result-error-message: "connection reset"
```

| Field | Default | Meaning |
|-------|---------|---------|
| `result-error-rate` | 0.0 | Fraction of ops that fail (0.0–1.0) |
| `result-error-name` | "ModelError" | Error name for the error router |
| `result-error-message` | "simulated error" | Error detail message |

Error injection is deterministic — seeded by cycle number so the
same cycle always produces (or doesn't produce) an error. This
enables reproducible failure testing.

### `result-rows` — Result Cardinality

For ops that model multi-row results (e.g., range queries):

```yaml
ops:
  scan:
    stmt: "SELECT [item_id], [price] FROM catalog WHERE category={cat};"
    result-rows: 10
    result: |
      // Evaluated once per row, with 'row_index' as additional coordinate
      item_id := hash(interleave(cat, row_index))
      price := scale_range(hash(item_id), 1.0, 999.0)
```

When `result-rows > 1`, the result GK kernel is evaluated N times
with `row_index` injected as an additional coordinate (0..N-1).
Captures from multi-row results are arrays — downstream ops can
reference individual elements or iterate.

For the initial implementation, `result-rows` defaults to 1 and
multi-row support is deferred.

### Summary of result fields

| Field | Type | Default | Purpose |
|-------|------|---------|---------|
| `result` | map or string | (none) | Simulated result values or GK kernel |
| `result-latency` | duration or distribution | 0 | Injected delay |
| `result-error-rate` | float | 0.0 | Fraction of ops that fail |
| `result-error-name` | string | "ModelError" | Error class for routing |
| `result-error-message` | string | "simulated error" | Error detail |
| `result-rows` | integer | 1 | Number of result rows (future) |

## Anonymous Inline Bindings: `{{...}}`

Op templates support anonymous inline GK expressions using double
braces. Each `{{expr}}` is desugared into an anonymous binding in
the GK kernel:

```yaml
ops:
  insert:
    stmt: "INSERT INTO t (id, name) VALUES ({{mod(hash(cycle), 1000000)}}, {{combinations(hash(hash(cycle)), 'A-Za-z', 8)}});"
```

Desugars to:

```
__inline_0 := mod(hash(cycle), 1000000)
__inline_1 := combinations(hash(hash(cycle)), 'A-Za-z', 8)
```

with the template becoming:

```
INSERT INTO t (id, name) VALUES ({__inline_0}, {__inline_1});
```

Inline bindings use GK expression syntax (nested composition),
not the legacy semicolon chain syntax. They can reference
coordinates, captures, and any named binding.

Inline bindings are convenience sugar. They are equivalent to
declaring named bindings — the compiler makes no distinction
after desugaring.

## GK-Driven Result Fields

The `result-*` fields accept either static literals or references
to named outputs from the `result` GK kernel:

| Form | Example | Meaning |
|------|---------|---------|
| Literal | `result-latency: 5ms` | Same value every cycle |
| GK reference | `result-latency: delay` | Per-cycle, computed by result GK |

When a `result-*` field names a GK output, that output is evaluated
per cycle alongside the capture values. All per-cycle computation
lives in one GK kernel — the `result-*` fields are wiring
declarations, not computation sites.

### Example: Gaussian latency

Named output from the result GK:

```yaml
ops:
  read:
    stmt: "SELECT [user_id] FROM users WHERE id={id};"
    result: |
      user_id := id
      latency := icd_normal(unit_interval(hash(cycle)), 5.0, 1.2)
    result-latency: latency
```

Or, when the latency computation is independent of the result
captures, an anonymous GK expression directly in the field:

```yaml
ops:
  read:
    stmt: "SELECT [user_id] FROM users WHERE id={id};"
    result:
      user_id: 42
    result-latency: "{{icd_normal(unit_interval(hash(cycle)), 5.0, 1.2)}}"
```

Both produce the same behavior: every cycle gets a different
latency drawn from N(5.0, 1.2) ms, deterministically seeded by
cycle. The first form keeps all GK computation in one kernel. The
second is more concise when the latency is independent of the
result values.

### Example: Bimodal latency with probabilistic error

```yaml
ops:
  read:
    stmt: "SELECT [user_id] FROM users WHERE id={id};"
    result: |
      user_id := id

      // Latency: 90% fast cache hits, 10% slow disk reads
      fast := icd_normal(unit_interval(hash(cycle)), 2.0, 0.5)
      slow := icd_normal(unit_interval(hash(hash(cycle))), 30.0, 5.0)
      latency := select(unfair_coin(cycle, 0.1), slow, fast)

      // Error rate: 2% of ops fail, weighted toward slow ops
      err := unfair_coin(cycle, 0.02)
    result-latency: latency
    result-error-rate: err
```

This uses `unfair_coin` and `select` from the probability modeling
nodes (see below).

## Diagnostic Output

The model adapter supports a verbose diagnostic mode that shows
the full data flow for each cycle:

```
nbrs run workload=example.yaml driver=model cycles=1 --diagnose
```

```
cycle 0:
  coords: cycle=0
  op1 [read_user]:
    bind: id=527897
    stmt: SELECT [user_id], [balance] FROM users WHERE id=527897;
    result: user_id=527897 balance=4823.17
    capture: user_id → volatile[0] balance → volatile[1]
    latency: 2.1ms (model)
  op2 [update_balance]:
    bind: target=527897 amount=4823.17
    stmt: UPDATE accounts SET balance=4823.17 WHERE user=527897;
    result: (none)
    latency: 0ms
```

This shows coordinates, bind point resolution, the statement, the
simulated result, which captures were written to which port slots,
and the modeled latency.

## Capture Validation

At compile time, the model adapter validates that every `[capture]`
declared in an op template has a corresponding entry in the
`result` definition (map or GK output). Missing captures are:

- **Default mode**: Warning — the capture will be unset at runtime
- **Strict mode**: Error — all captures must be covered by the result

This catches typos and missing result fields before any ops execute.

## Relationship to stdout Adapter

| Feature | stdout | model |
|---------|--------|-------|
| Print assembled op | Yes | Yes |
| Produce results | No | Yes (`result`) |
| Populate captures | No | Yes |
| Latency simulation | No | Yes (`result-latency`) |
| Error injection | No | Yes (`result-error-*`) |
| Multi-row results | No | Future (`result-rows`) |
| Diagnostic output | No | Yes (`--diagnose`) |

The model adapter is a superset of stdout. When no `result` field
is present on an op, the model adapter behaves identically to
stdout — print the assembled statement and return success with no
captures.

## Probability Modeling Nodes

The following GK node functions support modeling probabilistic
behavior in result GK kernels. They are general-purpose — usable
anywhere in a GK graph, not only in result kernels — but their
primary motivation is enabling realistic model adapter behavior.

All nodes are deterministic: same input always produces the same
output. "Random" behavior comes from hashing the input, not from
a stateful RNG.

### Coin Flips

| Node | Signature | Description |
|------|-----------|-------------|
| `fair_coin(input)` | u64 → u64 (0 or 1) | 50/50 binary outcome |
| `unfair_coin(input, p)` | u64, f64 → u64 (0 or 1) | Outcome with probability `p` of returning 1 |

`fair_coin` is equivalent to `mod(hash(input), 2)`.
`unfair_coin` hashes the input to a unit interval and compares
against `p`. The `p` parameter can be init-time (constant) or
wire (dynamic).

```gk
// 10% chance of being "slow"
is_slow := unfair_coin(cycle, 0.1)
```

### Selection

| Node | Signature | Description |
|------|-----------|-------------|
| `select(cond, if_true, if_false)` | u64, T, T → T | Binary conditional: returns `if_true` when cond ≠ 0, else `if_false` |
| `one_of(input, values...)` | u64, T... → T | Uniform selection from N values |
| `one_of_weighted(input, spec)` | u64, init → T | Weighted selection from a spec string |

`select` is the core conditional — it picks between two wire
inputs based on a boolean (u64: 0 = false, nonzero = true).
All three inputs are evaluated (no short-circuit), since GK
is a DAG, not a control flow graph.

```gk
// Pick fast or slow latency based on coin flip
latency := select(is_slow, slow_latency, fast_latency)
```

`one_of` uniformly picks from N values. The input is hashed
and mod'd by N:

```gk
// Uniform pick from 4 data centers
dc := one_of(cycle, "us-east", "us-west", "eu-west", "ap-south")
```

`one_of_weighted` uses the alias method (SRD 09) for weighted
selection from a spec string:

```gk
// 60% reads, 30% writes, 10% deletes
action := one_of_weighted(cycle, "read:60, write:30, delete:10")
```

### N-of-M Selection

| Node | Signature | Description |
|------|-----------|-------------|
| `n_of(input, n, m)` | u64, init, init → u64 (0 or 1) | Returns 1 for exactly `n` out of every `m` inputs |

Deterministic fractional selection: exactly `n` out of every `m`
consecutive inputs return 1, the rest return 0. Which specific
inputs are selected is determined by a hash-based shuffle, so the
pattern isn't simply the first `n`.

```gk
// Exactly 3 out of every 10 cycles are "special"
is_special := n_of(cycle, 3, 10)
```

This differs from `unfair_coin` which is probabilistic (each
input independently has probability p). `n_of` guarantees exact
counts over each window of m inputs.

### Probability Ranges

| Node | Signature | Description |
|------|-----------|-------------|
| `chance(input, p)` | u64, f64 → f64 (0.0 or 1.0) | Like unfair_coin but returns f64 for direct math |
| `blend(input, a, b, mix)` | u64, f64, f64, f64 → f64 | `a * (1 - mix) + b * mix`, with mix from unit_interval(hash(input)) when wired |

`chance` is like `unfair_coin` but returns f64 (0.0 or 1.0) for
direct use in arithmetic without type conversion:

```gk
// Add a 5% surcharge: base_price * (1.0 + 0.05 * chance(cycle, 0.3))
surcharge := mul(chance(cycle, 0.3), 0.05)
```

`blend` mixes two values with a ratio. When `mix` comes from a
wire, it acts as a continuous interpolation. When `mix` is a
constant, it's a fixed weighted average:

```gk
// 70% fast baseline, 30% slow baseline
latency := blend(cycle, fast_mean, slow_mean, 0.3)
```

### Composition Example: Realistic Service Model

```gk
// Model a service with:
//   - 90% fast (2ms ± 0.5), 10% slow (30ms ± 5)
//   - 1% overall error rate, 5% error rate on slow path
//   - Timeout after 100ms

is_slow := unfair_coin(cycle, 0.1)
fast_t := icd_normal(unit_interval(hash(cycle)), 2.0, 0.5)
slow_t := icd_normal(unit_interval(hash(hash(cycle))), 30.0, 5.0)
raw_latency := select(is_slow, slow_t, fast_t)
latency := clamp_f64(raw_latency, 0.1, 100.0)

// Error rate depends on path
fast_err := unfair_coin(hash(cycle), 0.01)
slow_err := unfair_coin(hash(cycle), 0.05)
is_error := select(is_slow, slow_err, fast_err)
```

This produces deterministic, reproducible latency and error
distributions that model a realistic service with cache hit/miss
behavior and path-dependent error rates.
