# 20: Workload Model

Workloads are YAML files that define what operations to execute,
how to generate their data, and how to configure the activity.

---

## Structure

```yaml
description: "Human-readable workload description"

params:
  keyspace: baselines
  concurrency: "100"

bindings: |
  inputs := (cycle)
  user_id := mod(hash(cycle), 1000000)

ops:
  insert_user:
    tags:
      phase: write
    prepared: "INSERT INTO {keyspace}.users (id) VALUES ({user_id})"

  read_user:
    tags:
      phase: read
    prepared: "SELECT * FROM {keyspace}.users WHERE id = {user_id}"
```

### Top-Level Fields

| Field | Type | Description |
|-------|------|-------------|
| `description` | string | Optional human-readable description |
| `params` | map | Workload parameters with defaults |
| `bindings` | string or map | GK source or legacy binding chains |
| `ops` | map or list | Operation templates |
| `blocks` | map | Named groups of ops with local overrides |
| `scenarios` | map | Named command sequences (future) |

---

## ParsedOp

The canonical normalized op template consumed by adapters:

```rust
pub struct ParsedOp {
    pub name: String,
    pub description: Option<String>,
    pub op: HashMap<String, serde_json::Value>,
    pub bindings: BindingsDef,
    pub params: HashMap<String, serde_json::Value>,
    pub tags: HashMap<String, String>,
}
```

- `op` — the adapter payload (statement, method, URL, etc.)
- `params` — adapter config and activity-level settings
- `tags` — filtering metadata
- `bindings` — GK source or legacy binding map

### Normalization

The parser normalizes all YAML shorthand forms into `ParsedOp`:

```yaml
# String shorthand → op.stmt = "SELECT ..."
ops:
  - "SELECT * FROM users"

# Named map → name from key, fields from value
ops:
  my_query:
    prepared: "SELECT * FROM users WHERE id = {id}"

# Explicit op field
ops:
  my_query:
    op:
      prepared: "SELECT * FROM users"
      id: "{user_id}"
```

---

## Blocks

Blocks group ops with shared bindings, params, and tags.
Block-level settings merge with and override workload-level:

```yaml
params:
  keyspace: baselines

blocks:
  schema:
    params:
      concurrency: "1"
    ops:
      create_table:
        raw: "CREATE TABLE {keyspace}.users ..."

  rampup:
    ops:
      insert:
        prepared: "INSERT INTO {keyspace}.users ..."
```

Merge rules: block params override workload params. Block
bindings override workload bindings. Block tags merge with
workload tags.

---

## Tags and Filtering

Ops carry string tags for phase selection:

```yaml
ops:
  insert:
    tags:
      phase: rampup
      type: write
```

Filter on CLI: `tags=phase:rampup` selects only matching ops.
Tag filter syntax: `key:value` pairs, AND-combined. Values can
be regex patterns.

Auto-generated tags: `name` (op name), `op` (op name), `block`
(containing block name).

---

## Op Field Routing

When the parser normalizes an op object, fields are routed:

| Destination | Fields |
|-------------|--------|
| `op` (adapter payload) | All fields not in reserved/activity lists |
| `params` (activity config) | `ratio`, `driver`, `verify`, `relevancy`, `strict`, `space`, `instrument`, `labels` |
| `tags` | `tags` field contents |
| `bindings` | `bindings` field contents |
| Reserved (consumed by parser) | `name`, `description`, `desc` |

This routing means `relevancy:` on an op goes to `params` (for
the validation wrapper), while `prepared:` goes to `op` (for the
CQL adapter).

### Audit Against nosqlbench Op Fields

| nosqlbench field | nb-rs | Status |
|---|---|---|
| `driver` / `space` | `params` | Implemented |
| `ratio` | `params` | Implemented |
| `labels` | `params` | **Needs implementation** — per-op metric labels for dashboard breakdown |
| `instrument` | `params` | Routed but not acted on — per-op metrics enable/disable |
| `start-timers` / `stop-timers` | — | Not implemented — named timer control across ops |
| `verifier` / `verifier-init` | `verify:` / `relevancy:` | Implemented (declarative syntax, no Groovy) |
| `expected-result` | `verify: [{field: x, eq: y}]` | Implemented |
| `hdr_digits` | — | Not exposed — HDR histogram significant digits |

The most impactful gap is **`labels`**: per-op labels attached to
metrics, enabling per-template metric breakdown in dashboards
(e.g., separate latency histograms for "read" vs "write" ops).

---

## Inline Workloads

Single-op workloads can be specified inline on the CLI:

```
nbrs run adapter=stdout op="hello {cycle}" cycles=5
```

Synthesized into a minimal `Workload` with one `ParsedOp`.
