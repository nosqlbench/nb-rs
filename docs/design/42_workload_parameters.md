# SRD 42: Workload Parameters and Expression Layer

## Background

### Java nosqlbench: Groovy preprocessing

Java nosqlbench has a two-phase preprocessing pipeline that runs
before YAML parsing:

1. **TemplateRewriter**: Converts `TEMPLATE(key, default)` and
   `${key:default}` syntax into `{{= paramOr('key', 'default') }}`
2. **GroovyExpressionProcessor**: Evaluates full Groovy expressions
   within `{{ }}` delimiters — conditionals, loops, string operations,
   math, library functions

This gives workload authors tremendous flexibility:

```yaml
# Java nosqlbench workload with Groovy preprocessing
bindings:
  user_id: Mod(TEMPLATE(keycount,1000000))
ops:
  write:
    stmt: |
      INSERT INTO TEMPLATE(keyspace,baselines).TEMPLATE(table,keyvalue)
      (key, value) VALUES ({key}, {value})
```

**What works:** Powerful parameterization. Workloads are reusable
templates that adapt to different targets, schemas, and scales.

**What doesn't:**
- Full Groovy runtime is ~30MB of dependencies and ~200ms startup
- Pre-parse text substitution is fragile — can produce invalid YAML
- Two separate preprocessing phases with different syntax
- Groovy expressions are opaque to the workload model — no
  validation, no introspection, no composition with the GK kernel
- The expression context (variable scope, parameter tracking) is a
  parallel state system that duplicates what GK already provides

### What nb-rs needs

The core requirement is the same: workload files must be
parameterizable. Users need to write one workload and run it against
different targets, key counts, table names, concurrency levels, etc.

But nb-rs has an advantage Java nosqlbench didn't: the GK kernel is
already an expression evaluation engine with named inputs, typed
outputs, and a compilation pipeline. Rather than building a second
expression language on the side, parameters can be first-class inputs
to the system that already exists.

---

## Design: Parameters as GK Upstream State

### Core idea

Workload parameters are **named inputs to the GK program**, resolved
at compile time and available to the entire workload through the
normal bind-point mechanism. No separate expression language. No
pre-parse text substitution. The GK DSL is the expression layer.

```
CLI args:  keycount=1000000 keyspace=baselines table=keyvalue
                │
                ▼
        ┌────────────────┐
        │  GK Program    │
        │                │
        │  params:       │  ← named parameter inputs
        │    keycount    │
        │    keyspace    │
        │    table       │
        │                │
        │  coordinates:  │  ← per-cycle inputs
        │    cycle       │
        │                │
        │  bindings:     │  ← computed outputs
        │    user_id     │
        │    key         │
        │    value       │
        └────────────────┘
                │
                ▼
        Op templates use {keyspace}, {table}, {user_id}
        via the same bind-point resolution
```

### Parameter declarations

Workload files declare parameters with types and defaults:

```yaml
params:
  keycount:
    type: u64
    default: 1000000
    description: Number of unique keys
  keyspace:
    type: string
    default: baselines
  table:
    type: string
    default: keyvalue
  rf:
    type: u64
    default: 1
    description: Replication factor
```

Or in compact form:

```yaml
params:
  keycount: 1000000
  keyspace: baselines
  table: keyvalue
  rf: 1
```

Parameters are resolved at workload load time from (in priority order):
1. CLI `key=value` args (highest)
2. Workload `params:` defaults (lowest)

### Parameters in op templates

Parameters are available as bind points in op template fields, using
the same `{name}` syntax as GK bindings:

```yaml
ops:
  schema:
    stmt: |
      CREATE KEYSPACE IF NOT EXISTS {keyspace}
      WITH replication = {{'class': 'SimpleStrategy',
      'replication_factor': {rf}}}
  write:
    stmt: |
      INSERT INTO {keyspace}.{table} (key, value)
      VALUES ({key}, {value})
```

There is no separate template syntax. `{keyspace}` is resolved
the same way `{user_id}` is — through the GK bind-point pipeline.
The difference is that `keyspace` is a parameter (constant per run),
while `user_id` is a binding (varies per cycle).

### Parameters in GK bindings

Parameters are available as constant inputs to GK expressions:

```yaml
bindings: |
  coordinates := (cycle)
  user_id := mod(hash(cycle), {keycount})
  key := format_u64(user_id, 10)
  value := combinations(hash(cycle), "a-z0-9", 64)
```

Here `{keycount}` is a parameter, resolved to a constant at compile
time. The GK compiler injects it as a `ConstU64` node. The resulting
DAG is fully optimized — the parameter is a compile-time constant,
not a per-cycle lookup.

### Implementation: parameter resolution in the GK program

Parameters become part of the GK program's input contract:

```rust
/// A workload parameter: name, type, default, resolved value.
pub struct WorkloadParam {
    pub name: String,
    pub param_type: ParamType,
    pub default: Option<String>,
    pub value: String,  // resolved from CLI or default
}

pub enum ParamType {
    U64,
    F64,
    String,
    Bool,
}
```

At compile time, the GK assembler receives the resolved parameter
values and creates constant nodes for each:

```
Param "keycount" = 1000000
  → ConstU64 node named "keycount" with value 1000000
  → Available as WireRef::node("keycount") in the GK DAG
  → Available as {keycount} in op template bind points
```

This means parameters participate in the full GK optimization
pipeline: fusion, constant folding, JIT compilation. A binding like
`mod(hash(cycle), {keycount})` compiles to a single fused node with
the modulus baked in as a constant.

---

## Computed Parameters: GK Expressions

For cases where a parameter needs computation (not just a literal
value), the GK DSL provides the expression power:

```yaml
params:
  keycount: 1000000
  partition_count: 256

bindings: |
  coordinates := (cycle)
  keys_per_partition := div({keycount}, {partition_count})
  partition := mod(hash(cycle), {partition_count})
  key_in_partition := mod(hash(cycle), keys_per_partition)
```

This replaces what Java nosqlbench used Groovy for: computing derived
values from parameters. But instead of a separate expression language,
it's the same GK DSL that generates cycle-time data — just operating
on constant inputs.

---

## Conditional Content

Java nosqlbench used Groovy conditionals to include/exclude sections:

```yaml
# Java: Groovy conditional
{{#if (param('lwt') == 'true')}}
ops:
  write_lwt:
    stmt: "INSERT ... IF NOT EXISTS"
{{/if}}
```

In nb-rs, this can be handled through **tag-based filtering** (already
implemented) rather than template conditionals:

```yaml
ops:
  write:
    tags:
      mode: standard
    stmt: "INSERT INTO {keyspace}.{table} (k, v) VALUES ({key}, {val})"
  write_lwt:
    tags:
      mode: lwt
    stmt: "INSERT INTO {keyspace}.{table} (k, v) VALUES ({key}, {val}) IF NOT EXISTS"
```

```bash
# Select which ops to run via tag filter
nbrs run workload.yaml tags=mode:lwt keycount=1000000
```

This is more explicit than conditional template expansion and
composes cleanly with the existing tag filter system. Every op is
always visible in the workload file; the filter selects which ones
run. No hidden conditional logic.

For truly dynamic workload structure (rare), a higher-level workload
composition tool can generate YAML programmatically.

---

## String Interpolation in Parameters

Parameters used in string contexts (table names, keyspace names)
need string interpolation in op templates. This already works through
the existing bind-point resolution:

```yaml
params:
  keyspace: baselines
  table: keyvalue

ops:
  write:
    stmt: "INSERT INTO {keyspace}.{table} (k, v) VALUES ({key}, {val})"
```

At resolve time, `{keyspace}` resolves to the string "baselines",
`{table}` to "keyvalue", and `{key}` to the cycle-time GK output.
All through the same pipeline.

---

## Comparison

| Feature | Java nosqlbench | nb-rs |
|---------|----------------|-------|
| Literal params | `TEMPLATE(k, v)` | `params: k: v` + `{k}` |
| Default values | `TEMPLATE(k, default)` | `params: k: default` |
| CLI override | `key=value` | `key=value` (same) |
| Computed params | Groovy `{{= expr }}` | GK DSL in `bindings:` |
| Conditionals | Groovy `{{#if}}` | Tag filters |
| String ops | Groovy functions | GK string nodes |
| Math | Groovy expressions | GK arithmetic nodes |
| Env vars | `env("NAME")` | `env:NAME` param source |
| Preprocessing | 2-phase text rewrite | None — resolved at compile |

### What's gained

- **No separate expression language.** The GK DSL is the expression
  layer. One syntax, one compiler, one optimization pipeline.
- **Type safety.** Parameters are typed and validated at compile time.
  Groovy expressions could produce any type at runtime.
- **Optimization.** Parameters are compile-time constants. The JIT
  can fold them into native code. Groovy values were runtime strings.
- **Introspection.** All parameters are visible in the workload model.
  Tools can enumerate them, validate them, generate help text. Groovy
  expressions were opaque strings.
- **No runtime dependency.** No Groovy runtime, no scripting engine,
  no classpath issues.

### What's traded

- **No arbitrary computation in templates.** You can't write a for
  loop that generates ops. This is intentional — workload structure
  should be static and inspectable, not computed at load time.
- **No conditional template expansion.** Tag filters replace this
  with a more explicit model, but it requires all variants to be
  present in the workload file.

---

## Migration Path

Java nosqlbench workloads using `TEMPLATE()` translate directly:

```yaml
# Java
stmt: "INSERT INTO TEMPLATE(keyspace,baselines).TEMPLATE(table,kv) ..."

# nb-rs
params:
  keyspace: baselines
  table: kv
ops:
  write:
    stmt: "INSERT INTO {keyspace}.{table} ..."
```

Workloads using Groovy for computed values translate to GK bindings:

```yaml
# Java
bindings:
  key: Mod(TEMPLATE(keycount,1000000)); ToString()

# nb-rs
params:
  keycount: 1000000
bindings: |
  key := format_u64(mod(hash(cycle), {keycount}), 10)
```

---

## Design Decisions

1. **Param scoping:** Global by default, with activity-level override
   via `activity.param=value` syntax in multi-activity scenarios.

2. **No param validation constraints.** No min/max/enum schemas.
   Parameters are simple typed values. Validation happens naturally
   through GK compilation errors and adapter-level checks.

3. **Environment variable access:** Supported. `env:NAME` syntax in
   parameter defaults or as a resolution source. Useful for paths,
   credentials, and CI/CD integration.

4. **No file inclusion.** Out of scope for this SRD. Workload
   composition is a separate concern.