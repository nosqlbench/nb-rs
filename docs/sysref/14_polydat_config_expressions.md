# 14: Polydat Config Expressions — nbrs-side framing

The substrate half of this SRD (the `{...}` expression syntax,
the const-expression evaluation API, config-value type
rendering, and the typed-error surface) has been hoisted into
the axiom-level polydat design:

- [polydat/docs/design/expression_engine.md §3.1](../../polydat/docs/design/expression_engine.md)
  — `eval_const_expr` API + works/doesn't-work catalog +
  EmbeddingError variants. Hoisted 2026-05-30 as part of the
  reconciliation pass (see [docs/polydat_srd_audit.md](../polydat_srd_audit.md))

This file retains the nbrs-side resolution order, param
substitution interaction with SRD-21, implementation state,
and historical context.

---

## Resolution Order

When the resolver encounters `{...}`:

1. **Named binding** — if the content matches a Polydat output name
   from the workload's `bindings:`, use the binding value.
2. **Const expression** — otherwise, compile the content as a
   standalone Polydat program with zero inputs. If all nodes fold to
   constants, use the result.
3. **Error** — if compilation fails or the expression depends on
   runtime inputs, report the full error. Never silently fall
   back to a literal string.

```
{train_count}                → named binding lookup
{vector_count("example")}     → const expression eval
{bad_func(42)}               → error with full diagnostic
```

---

## Param Substitution Interaction

Params are substituted BEFORE const expression evaluation.
This enables:

```yaml
params:
  dataset: example

cycles: "{vector_count('{dataset}')}"
# After param substitution: "{vector_count('example')}"
# After const eval: 1000000
```

Order:
1. CLI params override YAML defaults
2. `{param}` substitution in Polydat source and config values
3. Polydat compilation + constant folding
4. `{expr}` const expression evaluation for remaining references

The full parameter-precedence and bind-point ownership spec
lives in [SRD 21 Parameters](21_parameters.md).

---

## Implementation State

Named binding references (`{train_count}`) are implemented.
The runner resolves them from `PolydatKernel::get_constant(name)`.

Inline const expressions (`{4 * 4}`, `{vector_count("example")}`)
need: `eval_const_expr()` API, parser support for zero-input
programs, and the fallback-to-const-eval path in the resolver.

---

## What This Replaces

Java nosqlbench used Groovy scripting:

```javascript
var train_count = dataset.getBaseCount();
scenario.run("rampup", "cycles=" + train_count);
```

nb-rs replaces this with Polydat const expressions — the same
language, the same compiler, no scripting runtime.
