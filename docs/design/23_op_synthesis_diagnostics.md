# Op Synthesis, Dry-Run, and Diagnostics

How operations are assembled from templates + variates, and how
users inspect, debug, and validate their workloads before running
them for real.

---

## Op Synthesis Pipeline

The path from a YAML op template to an executed operation:

```
ParsedOp (from workload YAML)
    │  name, op fields, bindings, params, tags
    │
    ▼
GK Kernel (compiled from bindings)
    │  cycle → named output variates
    │
    ▼
Op Assembly (per cycle)
    │  Substitute {bindpoint} references with variate values
    │  Resolve static fields (no bind points) as constants
    │  Result: AssembledOp with all fields fully resolved
    │
    ▼
Adapter (execute)
    │  HTTP: build request from fields (method, url, headers, body)
    │  Stdout: render fields as text
    │
    ▼
Result + Metrics
```

### The build_op Closure

The bridge between the GK kernel and the adapter:

```rust
fn build_op(cycle: u64, template: &ParsedOp, kernel: &mut GkKernel) -> AssembledOp {
    // Set the coordinate context for this cycle
    kernel.set_coordinates(&[cycle]);

    // Resolve each op field
    let mut fields = HashMap::new();
    for (key, value) in &template.op {
        let resolved = resolve_field(value, kernel);
        fields.insert(key.clone(), resolved);
    }

    AssembledOp { name: template.name.clone(), fields }
}

fn resolve_field(value: &serde_json::Value, kernel: &mut GkKernel) -> String {
    match value {
        Value::String(s) => {
            // Replace {bindpoint} references with kernel outputs
            substitute_bind_points(s, kernel)
        }
        other => other.to_string(),
    }
}

fn substitute_bind_points(template: &str, kernel: &mut GkKernel) -> String {
    // For each {name} in the template, pull the named variate
    // from the kernel and substitute it.
    // ... (reuse bindpoints::extract_bind_points logic)
}
```

### Static vs Dynamic Fields

At assembly time (before cycles run), fields are classified:

- **Static fields** — no bind points. The value is constant across
  all cycles. Can be resolved once and reused.
- **Dynamic fields** — contain `{bindpoint}` references. Must be
  resolved per-cycle from the GK kernel.

The assembly step can short-circuit static fields for efficiency.

---

## Dry-Run Mode

**Purpose:** Show the user what ops will be generated without
actually executing them. Uses the stdout adapter internally.

### Usage

```
nb-rs dryrun workload.yaml cycles=10
```

Or in the workload YAML:
```yaml
scenarios:
  dryrun:
    preview: run driver=stdout tags==block:main cycles=10 format=json
```

### What It Shows

For each cycle, the dry-run renders the fully assembled op:

```
cycle=0: {"method":"POST","url":"http://api.example.com/users","body":"{\"name\":\"Alice\",\"age\":42}"}
cycle=1: {"method":"POST","url":"http://api.example.com/users","body":"{\"name\":\"Bob\",\"age\":37}"}
```

This verifies:
- Bind points resolve correctly
- Variate generation produces expected values
- The op template produces valid operations
- Ratios and sequencing are correct

### Diagnostic Format

The stdout adapter's `diag` format (future) would show the
decomposed view:

```
--- cycle 0 ---
  op: write_user
  template: POST /users body={user_json}
  bindings:
    user_json: {"name":"Alice","age":42}
  resolved:
    method: POST
    url: http://api.example.com/users
    body: {"name":"Alice","age":42}
  tags: {block=main, name=write_user, op=write_user}
```

---

## Diagnostics and Validation

### Workload Validation (before any cycles)

Run at activity init time:

1. **Parse check** — does the YAML parse? (spectest already validates)
2. **Binding resolution** — can all `{bindpoint}` references be
   resolved from the declared bindings?
3. **GK compilation** — does the GK kernel compile from the bindings?
4. **Op template completeness** — does each op have the required
   fields for the target adapter?
5. **Tag filter check** — does the tag filter select at least one op?
   (Error if zero ops selected.)

### Per-Cycle Diagnostics

Available via the `inspect` GK node or adapter instrumentation:

- **Variate preview** — show the GK kernel output for a specific
  cycle without executing
- **Timing decomposition** — service time, wait time, response time
  per op (already in metrics)
- **Error classification** — which error handler chain fired, what
  was the ErrorDetail result

### Binding Verification

The `diag` output format for the stdout adapter shows bindings
alongside their resolved values, helping users verify that:
- Bindings produce the expected type
- Hash provenance is correct
- Distribution shapes look right (e.g., values cluster as expected)

### Op Sequence Visualization

Show the stanza pattern to verify ratio correctness:

```
Stanza (length=6):
  cycle 0: read   (ratio 3)
  cycle 1: write  (ratio 2)
  cycle 2: delete (ratio 1)
  cycle 3: read
  cycle 4: write
  cycle 5: read
```

---

## Integration Points

### Stdout Adapter for Dry-Run

The stdout adapter (`driver=stdout`) IS the dry-run mechanism.
No separate dry-run mode needed — just set `driver=stdout` and
the assembled ops are rendered instead of executed.

Formats available:
- `assignments` — `key=value, key=value`
- `json` — inline JSON object
- `csv` — comma-separated values
- `statement` — just the stmt field

### GK Diagnostic Nodes

The `inspect` node logs values to stderr while passing them
through. Users can wire it into the GK kernel to see what
variates are being produced:

```gk
coordinates := (cycle)
hashed := inspect(hash(cycle))  // logs to stderr
result := mod(hashed, 1000)
```

### Error Handler Diagnostics

The `warn` and `error` handlers log to stderr with cycle number
and error details. The `counter` handler provides per-error-type
counts accessible via the metrics pipeline.
