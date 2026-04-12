# 21: Parameters and Bind Points

Parameters configure workloads without editing YAML. Bind points
connect parameters and GK bindings to op templates.

---

## Parameter Resolution

Resolution order (highest priority wins):

1. **CLI**: `key=value` on the command line
2. **Workload params**: `params:` section defaults
3. **Environment**: `env:VAR_NAME` syntax in defaults

```yaml
params:
  keyspace: baselines              # default value
  dataset: env:NB_DATASET          # from environment
  concurrency: "100"               # overridable from CLI
```

CLI override: `cassnbrs run ... keyspace=production`

Parameters are string-typed. Numeric parsing happens at the
consumer (runner parses `cycles`, `concurrency`; adapters parse
`port`, `timeout`).

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

Resolved workload params are stored as globals on `GkProgram`
after compilation. Fibers read them via `program.globals()` —
no separate params map per fiber.

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

After compilation, the resolved params are stored as globals on
`GkProgram`. Fibers read them via `program.globals()` at cycle
time for `{param:name}` resolution and op field substitution.
The params are resolved once, stored once, never re-read from
external sources.
