# 14: GK Config Expressions

Init-time GK constants can flow into activity configuration,
replacing the need for a separate scripting language.

---

## Problem

Activity settings like `cycles` and `concurrency` are resolved
from CLI params or workload `params:`. But some settings depend
on data only known after GK compilation:

```yaml
bindings: |
  train_count := vector_count("{dataset}")

# How does rampup know cycles=1183514?
# Currently: user must hardcode it or pass on CLI.
```

`train_count` is an init-time constant — evaluated during constant
folding before any cycles execute. The workload has the information
but can't flow it into config.

---

## Design

### Two-Phase Resolution

```
Phase 1: Compile GK, fold constants
  train_count → 1183514
  dim → 25

Phase 2: Resolve config from folded constants
  cycles: "{train_count}" → 1183514
  concurrency: "100"      → 100 (literal, unchanged)
```

### Syntax

Config values reference GK constants with `{name}` braces:

```yaml
params:
  dataset: glove-25-angular

bindings: |
  train_count := vector_count("{dataset}")

blocks:
  rampup:
    params:
      cycles: "{train_count}"       # GK constant reference
      concurrency: "100"            # literal — no braces
```

### Disambiguation

| Value | Interpretation |
|-------|---------------|
| `"100"` | Literal string |
| `"{train_count}"` | GK constant reference |
| `"{dataset}"` | Workload param reference (already expanded) |
| `"prefix_{dim}_suffix"` | Inline substitution |

Resolution order: workload params first (expanded pre-compilation),
then GK constants (expanded post-compilation).

**Open question**: Should a `{gk:name}` qualifier disambiguate
when a workload param and GK binding share the same name?

---

## Error Handling

Reference to a non-foldable binding (depends on inputs):
```
error: config 'cycles' references '{user_count}', but user_count
depends on cycle inputs. Only init-time constants can be used.
```

Reference to a non-existent binding:
```
error: config 'cycles' references '{train_count}', but no binding
named 'train_count' exists.
```

---

## Current State

`GkKernel::get_constant(name)` is implemented — returns the value
of a folded constant output. The full config resolution pipeline
(Phase 2 above) is designed but not yet wired into the runner.
Currently, workload params resolve from CLI > workload `params:`
with no GK constant substitution.

---

## What This Replaces

Java nosqlbench used Groovy scripting for computed config:

```javascript
var train_count = dataset.getBaseCount();
scenario.run("rampup", "cycles=" + train_count);
```

nb-rs replaces this with GK — no separate language, no scripting
runtime, no Groovy stack traces. Init-time constants computed by
the same GK kernel that generates workload data.
