# SRD 48: GK Config Expressions — Init-Time Constants in Activity Config

Replace the nosqlbench Groovy-based "named scenario" scripting layer
with GK init-time constants flowing into activity configuration.
No new language, no scripting engine — just GK constants resolved
at compile time and made available to activity settings.

---

## Problem

Activity configuration settings like `cycles`, `concurrency`, and
`rate` are currently string literals resolved from CLI params or
workload `params:`. But some settings depend on data that's only
known after GK compilation:

```yaml
params:
  dataset: glove-25-angular

bindings: |
  train_count := vector_count("{dataset}")

# PROBLEM: How does the rampup phase know cycles=1183514?
# The user must hardcode it or pass it on the CLI.
```

`train_count` is a GK init-time constant — it evaluates during
constant folding before any cycles execute. The workload *has* the
information. It just can't flow it into activity config.

### Java nosqlbench's approach

Java nosqlbench solved this with a Groovy scripting layer:

```javascript
// Named scenario script (Groovy)
var train_count = dataset.getBaseCount();
scenario.run("rampup", "cycles=" + train_count);
```

**What was wrong with it:**
- Separate language (Groovy) with its own runtime and error modes
- Scripting context was implicit and poorly documented
- Script errors were opaque (Groovy stack traces)
- Two independent expression systems (bindings vs. scripts)

### What nb-rs should do

Use GK for everything. GK already evaluates init-time constants.
The missing piece: a resolution step that makes folded constants
available as activity config values.

---

## Design

### Two-Phase Resolution

```
  Phase 1: Compile GK program
  ┌─────────────────────────────────┐
  │  GK source + workload params    │
  │  ────────────────────────────   │
  │  train_count := vector_count()  │──▶ fold constants
  │  dim := vector_dim()            │──▶ train_count = 1183514
  └─────────────────────────────────┘    dim = 25

  Phase 2: Resolve config from folded constants
  ┌─────────────────────────────────┐
  │  Activity config                │
  │  ────────────────────────────   │
  │  cycles: train_count            │──▶ cycles = 1183514
  │  concurrency: 100               │    (literal, unchanged)
  └─────────────────────────────────┘
```

After GK compilation and constant folding, any activity config
value that names a folded GK constant gets its value substituted.
Literal values pass through unchanged.

### Syntax and Disambiguation

Config values use the same `{name}` syntax as bind points in op
fields. The distinction between literal `"100"` and GK reference
`"{train_count}"` is syntactic: braces mean "resolve this name."

```yaml
params:
  dataset: glove-25-angular

bindings: |
  coordinates := (cycle)
  train_count := vector_count("{dataset}")
  dim := vector_dim("{dataset}")

blocks:
  schema:
    params:
      concurrency: "1"              # literal — no braces
    ops:
      create_table:
        raw: "CREATE TABLE t (key text, value vector<float, {dim}>)"

  rampup:
    params:
      cycles: "{train_count}"       # GK constant reference
      concurrency: "100"            # literal
    ops:
      insert:
        prepared: "INSERT INTO t (key, value) VALUES ..."
```

**Disambiguation rules:**

| Value | Interpretation |
|-------|---------------|
| `"100"` | Literal string — passed through as-is |
| `"{train_count}"` | GK constant reference — resolved after folding |
| `"{dataset}"` | Workload param reference — already resolved today |
| `"prefix_{dim}_suffix"` | Mixed — inline substitution (param or GK) |

This is the same syntax that already works in op fields and GK
binding source strings. The only new behavior: after GK compilation,
the runner applies one more substitution pass over activity config
values (cycles, concurrency, rate, etc.) using folded constants.

**Open question:** Should GK constant references use a distinct
qualifier syntax like `{gk:train_count}` to make the source
explicit? The current `{name}` syntax is ambiguous when a workload
param and a GK binding share the same name. Resolution order would
be: workload param first (already expanded pre-compilation), then
GK constant. This matches the existing bind point resolution order
in op fields. A qualifier would add clarity but break consistency.

Resolution order for a config value like `cycles: "{train_count}"`:
1. CLI param `cycles=N` — always wins, no substitution
2. Workload/block param with `{name}` — GK constant substitution
3. Workload/block param literal — used as-is
4. Default — one stanza length

### Error Handling

If a config value references a GK binding that was NOT folded to a
constant (i.e., it depends on coordinates), that's a hard error:

```
error: config param 'cycles' references '{user_count}', but
user_count depends on cycle coordinates and cannot be resolved
at init time. Only init-time constants can be used in config.
```

If the referenced name doesn't exist in the GK program at all:

```
error: config param 'cycles' references '{train_count}', but
no binding named 'train_count' exists. Check your bindings.
```

### What Can Be Referenced

Only GK bindings that are **init-time foldable** — no coordinate
dependencies. This includes:

- `vector_count(dataset)` — dataset size
- `vector_dim(dataset)` — dimension count
- `dataset_distance_function(dataset)` — similarity metric name
- Any arithmetic/string function on other constants
- Literal values (`42`, `"hello"`)

This does NOT include:
- `hash(cycle)` — depends on coordinates
- `vector_at(cycle, dataset)` — depends on coordinates
- Any binding that transitively depends on a coordinate

---

## Implementation

### Step 1: `GkKernel::get_constant(name)`

Already implemented. Returns the value of a named output that was
folded during init-time constant folding.

### Step 2: Config resolution in the runner

After GK compilation:

```rust
fn resolve_config_value(
    cli: &HashMap<String, String>,
    workload: &HashMap<String, String>,
    kernel: &GkKernel,
    key: &str,
) -> Option<String> {
    // CLI always wins
    if let Some(v) = cli.get(key) {
        return Some(v.clone());
    }
    // Workload param with possible GK constant reference
    if let Some(v) = workload.get(key) {
        return Some(resolve_gk_references(v, kernel));
    }
    None
}

fn resolve_gk_references(value: &str, kernel: &GkKernel) -> String {
    // Simple case: entire value is a single {name} reference
    let trimmed = value.trim();
    if trimmed.starts_with('{') && trimmed.ends_with('}')
        && !trimmed.starts_with("{{") {
        let name = &trimmed[1..trimmed.len()-1];
        if let Some(val) = kernel.get_constant(name) {
            return val.to_display_string();
        }
    }
    // Otherwise: substitute {name} references inline
    // (same pattern as workload param expansion)
    value.to_string()
}
```

### Step 3: Per-phase config

The block-level params should be resolvable too. When the runner
processes a block, block params override workload params, and GK
constant references are resolved in both.

---

## Binding Visibility for Non-Op Consumers

### The Problem

GK bindings are currently compiled only if they're referenced by
op fields. If a binding is needed only by the validation layer
(e.g., `ground_truth` for relevancy measurement), it won't be
compiled into the GK program.

### Current Solution

The binding compiler scans both `op` fields AND `params` for
`{name}` references. This ensures that bindings referenced in
`relevancy.expected: "{ground_truth}"` are included in the GK
program even though no adapter op field uses them.

At resolve time, `FiberBuilder::resolve_with_extras()` accepts
a list of extra GK output names to pull alongside the op fields.
The validation wrapper declares which extras it needs at init time.

### Design Principle

The GK program should include **all bindings that any consumer
needs**, not just those referenced by adapter op fields. Consumers
include:
- Adapter op fields (CQL bind points, HTTP body templates)
- Validation (ground truth, expected values)
- Config expressions (cycles, rate from constants)
- Future: metric labels, conditional logic

The binding compiler achieves this by scanning all `{name}`
references across all op and param values, not just op fields.

---

## Relationship to Other SRDs

**SRD 44 (GK Constant Folding):** The init-time constant folding
mechanism is the foundation. Config expressions consume the same
folded values that optimize the cycle-time evaluation path.

**SRD 47 (Result Validation):** The `relevancy.expected` field
drove the discovery that bindings need to be visible beyond op
fields. The extra-bindings mechanism and param scanning solve this.

**SRD 42 (Workload Parameters):** Config expressions extend the
parameter resolution chain: CLI > workload param (with GK constant
substitution) > default. The `{name}` syntax is consistent with
existing bind point references.
