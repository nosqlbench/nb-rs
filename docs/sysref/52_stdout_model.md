# 52: Stdout and Model Adapters

Lightweight adapters included in all binaries. Used for
workload development, debugging, and diagnostic output.

---

## Stdout Adapter

Renders resolved fields to a file or stdout.

### Configuration

```rust
pub struct StdoutConfig {
    pub filename: String,       // "stdout" or file path
    pub newline: bool,
    pub format: StdoutFormat,
    pub fields_filter: Vec<String>,  // empty = all fields
}
```

### Formats

| Format | Output |
|--------|--------|
| `Assignments` | `field1=value1, field2=value2` |
| `Json` | `{"field1":"value1","field2":"value2"}` |
| `Csv` | `value1,value2,value3` |
| `Statement` | All fields, newline-separated |

Select via `format=json` on CLI or in workload params.

### Field Rendering

The stdout adapter renders ALL fields in `ResolvedFields`, not
just a `stmt` field. This was a deliberate design decision:
adapter payloads may have multiple fields, and stdout should
show them all for diagnostic purposes.

The `fields_filter` parameter restricts output to named fields
when specified.

---

## Model Adapter

Simulation adapter for testing workload structure without a
live target. Renders operations like stdout but with additional
diagnostic capabilities.

### Configuration

```rust
pub struct ModelConfig {
    pub stdout: StdoutConfig,
    pub diagnose: bool,
}
```

When `diagnose=true`, the model adapter logs additional
information about field resolution, bind point substitution,
and template selection.

---

## Use Cases

- **Workload development**: `adapter=stdout format=json` to
  verify field resolution
- **Dry-run verification**: See what would be sent without a
  live target
- **GK testing**: Verify data generation patterns
- **CI validation**: Ensure workloads parse and resolve correctly
