# 21: Parameters and Bind Points

Parameters configure workloads without editing YAML. Bind points
connect parameters and GK bindings to op templates.

> **Parameters vs runtime reads.** This document covers *launch-
> time* parameter resolution (CLI → workload → env) and the bind-
> point syntax that names parameter and GK values in op fields.
> *Runtime-mutable* values — anything a workload reads that can
> change mid-run — are accessed as GK bindings too, not as a
> parallel mechanism. See SRD 10 §"GK as the unified access
> surface" for the general principle and SRD 12 §"Runtime context
> nodes" for the node catalog (`control`, `metric`, `rate`,
> `concurrency`, `phase`).

---

## Parameter Resolution

Resolution is **closest-scope-wins**, layered from innermost
(per-op) outward to outermost (CLI):

1. **Op-local `params:`** on an individual op template.
2. **Block-local `params:`** on the containing phase / stanza
   block.
3. **Workload `params:`** at the top-level of the YAML.
4. **Environment** via the `env:VAR_NAME` shorthand in any
   of the above.
5. **CLI** `key=value` overrides.

The closest definition wins, so block-level and op-local
`params:` are the canonical way to say "this phase needs a
different value from the rest of the workload". CLI overrides
take the outermost layer: they ride on top and replace values
for the keys they name, which is the right scoping for the
"operator nudged it from the command line" use case.

```yaml
params:
  keyspace: baselines              # workload default
  concurrency: "100"               # workload default

blocks:
  - name: ddl
    params:
      concurrency: "1"             # block override for schema phase
    ops:
      create_table:
        prepared: "CREATE TABLE ..."
  - name: rampup
    ops:
      insert:
        prepared: "INSERT INTO ..."
```

CLI override: `cassnbrs run ... concurrency=200` replaces the
effective concurrency for every block that hasn't overridden
it locally.

### Explicit layering with GK helpers

Closest-wins covers the common case, but workloads sometimes
need to say "use this value if it's defined, otherwise fall
back to *that* one" — across scopes, with explicit ordering.
The GK layer exposes three helpers for that:

- **`this_or(primary, default)`** — returns `primary` if it
  resolves to a defined value, otherwise `default`. Use when
  a block's value should override the workload default only
  when explicitly set. Both arguments are ordinary GK wires,
  so `default` can itself be another `this_or`, a literal, a
  capture from a prior op, or another param lookup.
- **`required(name)`** — asserts that `name` resolves to a
  defined, non-empty value at compile time; fails with a
  clear error if not. Use to catch missing-parameter bugs
  before any cycle runs. Equivalent to a compile-time
  precondition on the workload.
- **Value predicates** — a family of assertion nodes that
  constrain a resolved value: `is_positive`, `in_range(lo,
  hi)`, `matches(regex)`, `is_one_of(a, b, c)`, etc. Each
  returns the value unchanged on pass and raises a compile
  (or init) error on fail. Predicates stack — the same value
  can carry several — and are evaluated at the earliest time
  the input is known.

```yaml
params:
  concurrency: "{this_or({param:concurrency}, 100)}"
  rate: "{required(rate)}"
  timeout_ms: "{in_range({param:timeout_ms}, 10, 60_000)}"
```

These helpers live in the GK stdlib (SRD 12); workload param
values go through the same reification path as every other
runtime value (SRD 10), so predicates and layering compose
with bind points, capture references, and runtime context
nodes uniformly.

Parameters are string-typed at the YAML layer. Numeric
parsing happens at the consumer (runner parses `cycles`,
`concurrency`; adapters parse `port`, `timeout`).

### Interaction with dynamic controls

Every param that is also declared as a [dynamic control](23_dynamic_controls.md)
uses the layered resolution above as its initial value
(seeded with `ControlOrigin::Launch`). Runtime writers may
advance the control past that seed unless the declaring scope
marks it `final` — see SRD 23 §"Branch-scoped and final
controls" for the override-rejection semantics.

---

## Bind Point Syntax

Op fields use `{qualifier:name}` to reference values from
specific sources, or `{name}` as shorthand that resolves by
precedence.

### Qualified References (preferred in strict mode)

| Syntax | Source | Description |
|--------|--------|-------------|
| `{bind:name}` | GK binding | Output of a GK kernel node |
| `{capture:name}` | Capture context | Value captured from a prior op's result |
| `{input:name}` | Graph input | External input value (e.g., cycle) |
| `{param:name}` | Workload params | Workload parameter value |

All four qualifiers resolve at cycle time from their respective
sources. Qualified references are unambiguous — each names exactly
one source. Required in strict mode (sysref 15).

`{param:name}` provides explicit access to workload parameters in
op fields. Without it, params are only available via pre-compile
string substitution in GK source. With it, a param can be
referenced directly in an op field without relying on the
pre-compile expansion phase.

### Unqualified Shorthand

| Syntax | Resolution Order |
|--------|-----------------|
| `{name}` | GK binding → capture → input → param (first match wins) |

Convenient for simple workloads. Produces a warning if `name`
exists in multiple namespaces (could resolve to the wrong one).

### Example

```yaml
ops:
  read:
    prepared: "SELECT [username] FROM users WHERE id = {bind:user_id}"
  update:
    prepared: "UPDATE users SET tag = {capture:username} WHERE id = {bind:user_id}"
```

### Workload Params vs GK Bindings

Workload params are expanded **before** GK compilation. GK
bind points are resolved **per cycle** during execution.

```yaml
params:
  dataset: glove-25-angular     # expanded pre-compile

bindings: |
  dim := vector_dim("{dataset}")  # {dataset} → "glove-25-angular" before GK sees it
  user_id := hash(cycle)          # {user_id} resolved per cycle
```

The expansion order matters: `{dataset}` in GK source is a param
substitution (string replacement pre-compile). `{user_id}` in an
op field is a bind point (resolved per cycle from GK output).

---

## Activity Config from Params

Activity settings resolve from CLI > workload params, with GK
constant substitution for values like `cycles: "{train_count}"`:

```rust
fn resolve_param_with_gk(cli, workload, kernel, key) -> Option<String> {
    // CLI always wins (no substitution)
    if let Some(v) = cli.get(key) { return Some(v); }
    // Workload params: substitute {name} from GK folded constants
    if let Some(v) = workload.get(key) { return Some(resolve_gk_refs(v, kernel)); }
    None
}
```

Workload params referenced in op templates are injected into the
GK source as constant bindings before compilation. They resolve
as normal GK outputs at cycle time — no separate globals mechanism.

Settings resolved this way:
- `cycles` — total cycle count
- `concurrency` — async fiber count
- `stanza_concurrency` / `sc` — ops per window
- `rate` — cycle rate limit
- `stanzarate` — stanza rate limit

This means workloads can set sensible defaults:

```yaml
params:
  concurrency: "100"
  rate: "10000"
```

And users override on CLI: `concurrency=200 rate=50000`

---

## Param Expansion in GK Source

Before GK compilation, `{param}` references in binding source
strings are expanded from resolved workload params:

```yaml
params:
  dataset: glove-25-angular

bindings: |
  dim := vector_dim("{dataset}")
  # After expansion: dim := vector_dim("glove-25-angular")
```

This is pure string substitution — the GK compiler never sees
`{dataset}`, only the expanded value. This is necessary because
GK node constructors (like `vector_dim`) need the dataset name
at compile time, not cycle time.

Additionally, params referenced in op templates (e.g., `{dataset}`
in a stmt field) are injected as standalone GK constant bindings
(e.g., `dataset := "glove-25-angular"`) before compilation. This
makes them available as normal GK outputs at cycle time, eliminating
the need for a separate globals mechanism on `GkProgram`.
