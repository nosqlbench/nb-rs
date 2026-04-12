# Memo 15: Const Expressions — Pure GK Evaluation Without Phases

## Problem

Today, using a computed value for `cycles` requires either:
1. A folded GK binding referenced as `cycles={name}` — needs a
   named binding in the workload's `bindings:` section
2. A separate phase that computes a value, then a downstream
   phase that uses it

Both are heavyweight for what is fundamentally a pure expression
evaluation: "compute this value from known constants, give me
the result." The user should be able to write:

```bash
nbrs run workload.yaml 'cycles={vector_count("sift1m") / 10}'
```

No bindings section. No named outputs. No phases. Just evaluate
a GK expression in a context with no external variables and
return the scalar result.

## Design: Const Expression Evaluation

A **const expression** is a GK expression that:
1. Has no input dependencies (no `cycle`, no coordinates)
2. References only literals, function calls, and other const
   expressions
3. Produces a single scalar value (u64, f64, or string)

These can be evaluated at compile time — they're the pure
functional subset of GK.

### Syntax: `{...}` is always an expression

`{...}` is the universal expression boundary. Everywhere it
appears — config values, CLI params, op templates — the content
is evaluated as a GK expression.

**No ambiguity.** `{...}` is never a literal brace. To include
a literal `{` in a string, escape it: `\{`. This follows the
same rule as Rust's `format!()`.

```yaml
params:
  dataset: sift1m
  half_dataset: "{vector_count('{dataset}') / 2}"    # → 500000
  batch_size: "{1000 * 1000}"                        # → 1000000
  query_idx: "{mod(hash(42), 10000)}"                # → deterministic index
  literal_braces: "this has \{no expressions\}"      # → literal braces

phases:
  rampup:
    cycles: "{vector_count('{dataset}')}"
    concurrency: "{min(100, vector_count('{dataset}') / 1000)}"
```

On the command line:

```bash
nbrs run workload.yaml 'cycles={4 * 4}'
nbrs run workload.yaml 'concurrency={min(100, 50 * 2)}'
nbrs run workload.yaml 'cycles={vector_count("sift1m") / 10}'
```

In op templates:

```yaml
ops:
  show:
    stmt: "bucket_count={1 << 16} sqrt={sqrt(to_f64(1000000))}"
```

### Resolution order

1. If content matches a known GK binding name → use binding value
2. Otherwise try const expression evaluation
3. If evaluation fails → **error** (not silent fallback)

If the user writes `{bad_func(42)}` and it doesn't compile,
that's an error. The full GK compilation error is shown —
syntax errors, unknown functions, type mismatches, missing
datasets. The user needs enough detail to fix it themselves.

### Implementation

#### Core: `eval_const_expr(source: &str) -> Result<Value, String>`

```rust
pub fn eval_const_expr(source: &str) -> Result<Value, String> {
    let wrapped = format!("inputs := ()\nout := {source}");
    let kernel = compile_gk(&wrapped)?;
    kernel.get_constant("out")
        .cloned()
        .ok_or_else(|| format!(
            "not a const expression: '{source}' depends on runtime inputs"
        ))
}
```

Note: `inputs := ()` declares zero inputs. Minor parser change
needed to accept this.

#### Runner integration

Extend the existing `resolve_gk_config` to try const expression
evaluation when a `{...}` reference doesn't match a known
binding:

```rust
fn resolve_config_value(raw: &str, kernel: &GkKernel) -> Option<u64> {
    if raw.starts_with('{') && raw.ends_with('}') {
        let inner = &raw[1..raw.len()-1];
        // Try as a named GK constant first
        if let Some(v) = kernel.get_constant(inner) {
            return Some(v.as_u64());
        }
        // Otherwise try as an inline const expression
        match eval_const_expr(inner) {
            Ok(v) => Some(v.as_u64()),
            Err(e) => { eprintln!("error: {e}"); None }
        }
    } else {
        raw.parse().ok()
    }
}
```

#### Param resolution

Workload params can contain `{...}` expressions. Resolve after
param substitution (so `{dataset}` is already expanded):

```yaml
params:
  dataset: sift1m
  num_queries: "{query_count('{dataset}')}"
```

Resolution order:
1. CLI params override YAML defaults (existing)
2. String substitution of `{param}` in GK source (existing)
3. `{...}` expressions evaluated after GK compilation
4. Results available for config resolution

#### Numeric results

All values render as strings at the workload boundary. A const
expression that returns `Value::U64(42)` renders as `"42"` in
an op template. `Value::F64(3.14)` renders as `"3.14"`. No
special numeric-to-string conversion needed — the existing
`to_display_string()` handles it.

For config values that require integers (`cycles`, `concurrency`),
the resolver calls `.as_u64()` on the result. If the expression
returns a string, that's a type error.

### What This Enables

```yaml
# No phases needed — cycles computed inline
params:
  dataset: sift1m

bindings: |
  inputs := (cycle)
  vec := vector_at(cycle, "{dataset}")

ops:
  insert:
    stmt: "INSERT INTO t (id, vec) VALUES ({cycle}, {vec})"

cycles: "{vector_count('{dataset}')}"
concurrency: 100
```

```bash
# Simple arithmetic
nbrs run workload.yaml 'cycles={1000 * 1000}'

# Dataset metadata
nbrs run workload.yaml 'cycles={vector_count("sift1m")}'

# Computed from metadata
nbrs run workload.yaml 'cycles={vector_count("sift1m") / 10}'
```

### Relationship to Phases

Const expressions do NOT replace phases. Phases are for:
- Different adapters per phase (schema DDL vs data load)
- Different concurrency/rate per phase
- Sequential dependencies (schema before rampup)
- Different op sets per phase

Const expressions are for:
- Computing a single scalar from known constants
- No runtime state, no cycle iteration
- Anywhere a scalar value is needed at compile time

Both compose naturally:

```yaml
phases:
  rampup:
    cycles: "{vector_count('{dataset}')}"
    concurrency: 100
    ops: ...
  main:
    cycles: "{vector_count('{dataset}') * 10}"
    concurrency: 50
    ops: ...
```

### Relationship to Constant Folding

Const expressions ARE constant folding — exposed to the user.
The same fold pass that evaluates `dim := vector_dim("sift1m")`
also evaluates `{vector_dim("sift1m")}`. Same mechanism,
different surface syntax.

### What Changes

| Component | Change |
|-----------|--------|
| Parser | Accept `inputs := ()` (zero inputs) |
| Compiler | `eval_const_expr()` public API |
| Runner | `{...}` fallback to const eval in config values |
| Param resolver | `{...}` in YAML param values |
| Bind point resolver | `{expr}` inline in op templates |

### What Doesn't Change

- The GK DAG model
- The kernel evaluation path
- The constant folding algorithm
- The provenance system
- Phased execution (orthogonal)
