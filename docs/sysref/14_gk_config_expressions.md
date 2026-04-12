# 14: GK Config Expressions

GK expressions can appear anywhere a value is needed — config
fields, CLI params, op templates. The `{...}` syntax is the
universal expression boundary.

---

## Expression Syntax

`{...}` always denotes a GK expression. It is never a literal
brace. Use `\{` for a literal `{` character.

```yaml
cycles: "{vector_count('{dataset}')}"       # const expression
concurrency: "{min(100, 50 * 2)}"           # inline arithmetic
batch_size: "{1000 * 1000}"                 # pure arithmetic
literal: "this has \{no expressions\}"      # escaped braces
```

On the command line:

```bash
nbrs run workload.yaml 'cycles={vector_count("sift1m") / 10}'
nbrs run workload.yaml 'cycles={4 * 4}'
nbrs run workload.yaml 'concurrency={min(100, 50 * 2)}'
```

In op templates:

```yaml
ops:
  show:
    stmt: "count={vector_count('{dataset}')} dim={vector_dim('{dataset}')}"
```

---

## Resolution Order

When the resolver encounters `{...}`:

1. **Named binding** — if the content matches a GK output name
   from the workload's `bindings:`, use the binding value.
2. **Const expression** — otherwise, compile the content as a
   standalone GK program with zero inputs. If all nodes fold to
   constants, use the result.
3. **Error** — if compilation fails or the expression depends on
   runtime inputs, report the full error. Never silently fall
   back to a literal string.

```
{train_count}                → named binding lookup
{vector_count("sift1m")}     → const expression eval
{bad_func(42)}               → error with full diagnostic
```

---

## Const Expression Evaluation

A const expression is any GK expression with no input
dependencies. It evaluates at compile time via the same
constant folding pass used for named bindings.

### API

```rust
pub fn eval_const_expr(source: &str) -> Result<Value, String> {
    let wrapped = format!("inputs := ()\nout := {source}");
    let kernel = compile_gk(&wrapped)?;
    kernel.get_constant("out")
        .cloned()
        .ok_or_else(|| "expression depends on runtime inputs".into())
}
```

### What works as a const expression

- Literals: `{42}`, `{3.14}`, `{"hello"}`
- Arithmetic: `{1000 * 1000}`, `{4 ** 0.5}`
- Function calls with constant args: `{hash(42)}`, `{mod(hash(42), 100)}`
- Dataset metadata: `{vector_count("sift1m")}`, `{vector_dim("glove-25")}`
- Nested: `{vector_count("{dataset}") / 10}` (after param substitution)

### What does NOT work

- References to cycle inputs: `{hash(cycle)}` → error
- References to undefined names: `{undefined_var}` → error
- Non-deterministic functions: `{counter()}` → error

---

## Config Value Types

For config fields that expect integers (`cycles`, `concurrency`),
the result must be numeric. The resolver calls `.as_u64()`.
If the expression returns a string, that's a type error.

For string contexts (op templates, param values), all values
render via `to_display_string()`:
- `Value::U64(42)` → `"42"`
- `Value::F64(3.14)` → `"3.14"`
- `Value::Str(s)` → `s`

---

## Param Substitution Interaction

Params are substituted BEFORE const expression evaluation.
This enables:

```yaml
params:
  dataset: sift1m

cycles: "{vector_count('{dataset}')}"
# After param substitution: "{vector_count('sift1m')}"
# After const eval: 1000000
```

Order:
1. CLI params override YAML defaults
2. `{param}` substitution in GK source and config values
3. GK compilation + constant folding
4. `{expr}` const expression evaluation for remaining references

---

## Error Handling

Errors show the full GK compilation diagnostic — syntax errors,
unknown functions, type mismatches, missing datasets:

```
error: const expression failed: '{bad_func(42)}'
  unknown function: 'bad_func'
  This function is not registered in the GK function library.
  Use 'nbrs describe gk functions' to see all available functions.
```

```
error: const expression failed: '{hash(cycle)}'
  not a const expression: depends on runtime input 'cycle'
```

---

## Implementation State

Named binding references (`{train_count}`) are implemented.
The runner resolves them from `GkKernel::get_constant(name)`.

Inline const expressions (`{4 * 4}`, `{vector_count("sift1m")}`)
need: `eval_const_expr()` API, parser support for zero-input
programs, and the fallback-to-const-eval path in the resolver.

---

## What This Replaces

Java nosqlbench used Groovy scripting:

```javascript
var train_count = dataset.getBaseCount();
scenario.run("rampup", "cycles=" + train_count);
```

nb-rs replaces this with GK const expressions — the same
language, the same compiler, no scripting runtime.
