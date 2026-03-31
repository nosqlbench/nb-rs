# SRD 35 — Inline Workload

## Purpose

Allow quick workload execution from the command line without a YAML
file. The `op=` parameter synthesizes a single-op workload on the
fly, resolving inline bindings against the GK function registry.

This is the primary onboarding path — a user can verify their
install, explore functions, and prototype binding expressions
without creating any files.

## Syntax

```
nbrs run adapter=stdout op='<template>' [cycles=N] [rate=R] [threads=T] ...
```

The `op` value is a string template. Bind points use the same
`{name}` and `{{expr}}` syntax as YAML workload templates:

- `{name}` — reference a named binding (requires a `bindings`
  parameter or coordinate name like `cycle`)
- `{{expr}}` — inline GK expression, compiled at init time
  into the kernel and invoked per cycle

### Examples

```bash
# Inline binding: number spelled as English words
nbrs run adapter=stdout op='{{number_to_words(cycle)}} <- cycle {{cycle}}' cycles=10

# Multiple inline bindings
nbrs run adapter=stdout op='id={{mod(hash(cycle), 100000)}} name={{number_to_words(cycle)}}' cycles=100

# With rate limiting
nbrs run adapter=stdout op='tick {{cycle}}' cycles=1000 rate=100

# JSON output format
nbrs run adapter=stdout op='user={{mod(hash(cycle), 1000)}}' cycles=5 format=json

# Chained functions
nbrs run adapter=stdout op='{{clamp_f64(icd_normal(unit_interval(hash(cycle)), 22.0, 3.0), 0.0, 50.0)}}' cycles=20

# Using stdlib modules (resolved from embedded stdlib)
nbrs run adapter=stdout op='device={{hashed_id(input: cycle, bound: 50000)}}' cycles=10

# Multiple ops (semicolon-separated)
nbrs run adapter=stdout op='read {{cycle}};write {{mod(cycle, 100)}}' cycles=100

# With the HTTP adapter
nbrs run adapter=http op='GET http://localhost:8080/api/item/{{mod(hash(cycle), 10000)}}' cycles=100 rate=50
```

## Relationship to `{{expr}}` — Inline Definitions

In Java nosqlbench, `{{expr}}` inside an op template is an
**inline binding definition** — a VirtData function chain that is
evaluated per cycle with the cycle number as input. The binding
is anonymous (unnamed) and resolved at the point of use.

In nb-rs, `{{expr}}` is a GK expression. The inline workload
compiler extracts all `{{expr}}` occurrences from the op template,
generates a GK source block with named outputs for each, and
compiles them into the kernel. Each expression receives `cycle`
as the implicit input coordinate.

### Compilation Example

Given:
```
op='id={{mod(hash(cycle), 100000)}} name={{number_to_words(cycle)}}'
```

The inline compiler generates:

**GK source:**
```
coordinates := (cycle)
__inline_0 := mod(hash(cycle), 100000)
__inline_1 := number_to_words(cycle)
```

**Op template (rewritten):**
```
stmt: "id={__inline_0} name={__inline_1}"
```

**Synthesized workload (equivalent YAML):**
```yaml
bindings: |
  coordinates := (cycle)
  __inline_0 := mod(hash(cycle), 100000)
  __inline_1 := number_to_words(cycle)
ops:
  inline:
    stmt: "id={__inline_0} name={__inline_1}"
```

The `{cycle}` reference is special-cased: it resolves to the raw
cycle coordinate without going through a GK output. This is
already handled by the existing bind point resolution logic
(SRD 23).

## Multiple Ops

A single `op=` value can contain multiple ops separated by
semicolons. Each segment becomes a separate op in the synthesized
workload with equal ratio:

```
op='read {{cycle}};write {{mod(cycle, 100)}}'
```

Becomes:
```yaml
ops:
  inline_0:
    stmt: "read {cycle}"
  inline_1:
    stmt: "write {__inline_0}"
```

Ratios can be specified with a prefix `N:`:

```
op='3:read {{cycle}};1:write {{mod(cycle, 100)}}'
```

Becomes:
```yaml
ops:
  inline_0:
    ratio: 3
    stmt: "read {cycle}"
  inline_1:
    ratio: 1
    stmt: "write {__inline_0}"
```

## Parameter Interaction

| Parameter | With `op=` | With `workload=` |
|-----------|-----------|-----------------|
| `adapter=` | Optional (defaults to stdout) | Optional (can be in YAML) |
| `cycles=` | Works normally | Works normally |
| `rate=` | Works normally | Works normally |
| `threads=` | Works normally | Works normally |
| `format=` | Works normally (stdout adapter) | Works normally |
| `tags=` | Ignored (single op, no filtering) | Works normally |
| `seq=` | Only relevant with multiple ops | Works normally |
| `--tui` | Works normally | Works normally |
| `--dry-run` | Works normally | Works normally |
| `--report-openmetrics-to=` | Works normally | Works normally |

`op=` and `workload=` are **mutually exclusive**. If both are
provided, `op=` takes precedence with a warning.

## Default Adapter

When `op=` is provided and `driver=` is omitted, the default
adapter is `stdout` with `format=statement`. This makes the
simplest invocation just:

```
nbrs run op='hello {{cycle}}' cycles=5
```

Which prints:
```
hello 0
hello 1
hello 2
hello 3
hello 4
```

## Implementation Strategy

### Where the Work Happens

The inline workload synthesis lives in `nb-workload` as a pure
function that takes the `op=` string and returns a `Workload`
struct — the same type that `parse_workload()` returns from YAML.
From there, the existing pipeline (binding compilation, op
sequencing, activity construction) works unchanged.

```rust
/// Synthesize a Workload from an inline `op=` string.
///
/// Extracts `{{expr}}` inline bindings, generates GK source,
/// rewrites the template to use named bind points, and returns
/// a Workload with one or more ParsedOps.
pub fn synthesize_inline_workload(op_template: &str) -> Result<Workload, String>
```

### Processing Steps

1. **Split** on unquoted `;` to get individual op segments
2. **Extract ratios** from `N:` prefixes
3. For each segment, **scan for `{{expr}}`** patterns
4. **Generate GK source** with `coordinates := (cycle)` and a
   named output for each unique expression
5. **Rewrite** each segment: replace `{{expr}}` with `{__inline_N}`
6. **Build** `Vec<ParsedOp>` with `BindingsDef::GkSource(gk_source)`
7. Return `Workload { ops, .. }`

### Call Site in `main.rs`

In `run_command()`, before the existing workload loading:

```rust
let workload = if let Some(op_str) = param("op") {
    if param("workload").is_some() {
        eprintln!("nbrs: warning: op= overrides workload=");
    }
    synthesize_inline_workload(op_str)?
} else if let Some(path) = param("workload") {
    let source = std::fs::read_to_string(path)?;
    parse_workload(&source, &params)?
} else {
    print_usage();
    return;
};
```

Everything downstream (binding compilation, op sequencing,
adapter selection, execution) remains unchanged.

## Non-Goals

- No support for structured ops (JSON fields, headers, etc.)
  from the inline form. Use YAML for complex op structures.
- No inline `bindings=` parameter for named bindings separate
  from the op template. All bindings are inline `{{expr}}`.
- No workload-level params, scenarios, or blocks. Inline is
  a single flat list of ops.

## Relationship to Other Components

- **nb-workload (SRD 17)**: `synthesize_inline_workload()` returns
  the same `Workload` type. The YAML parser and inline synthesizer
  are parallel entry points to the same downstream pipeline.
- **GK kernel (SRD 02/07)**: Inline expressions are compiled to
  the same GK DAG. Function resolution, module imports (stdlib),
  and compilation levels all apply.
- **Op synthesis (SRD 23)**: Bind point substitution works
  identically — `{__inline_N}` is resolved from GK outputs,
  `{cycle}` from coordinates.
- **Dry-run (SRD 34)**: `--dry-run=emit` with inline ops is the
  fastest way to verify a binding expression works.
- **Stdout adapter**: Default adapter for inline workloads.
  Combined with `format=json`, useful for generating sample data.
