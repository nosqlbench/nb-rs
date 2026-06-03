# 13: Polydat Modules — nbrs-side framing

The module-as-source-file system (file-based modules, inlining
mechanics, resolution chain, strict mode) has moved into the
polydat crate:

- [polydat/docs/design/module_system.md](../../polydat/docs/design/module_system.md)
  — moved 2026-05-30 as part of the import-first reorganization
  (see [docs/polydat_srd_audit.md](../polydat_srd_audit.md))

This file retains the nbrs-activity-facing diagnostic API.

> Composition mechanics — how modules combine with the host
> program and with other Polydat kernels — live in
> [SRD 13b: Polydat Combination Modes](13b_polydat_combination_modes.md).

---

## Compiler Diagnostic Event Stream

The compiler emits structured events explaining each step
of compilation. This is the canonical introspection surface
for "why did the compiler do that?" questions.

```rust
pub enum CompileEvent {
    Parsed { node: String, function: String },
    BindingResolved { name: String, source: String },
    ModuleInlined { module: String, prefix: String },
    TypeAdapterInserted { from: String, to: String, node: String },
    ConstantFolded { node: String, value: String },
    FusionApplied { pattern: String, nodes: Vec<String> },
    CompileLevelSelected { node: String, level: String },
    OutputSelected { name: String, consumers: Vec<String> },
}
```

### Accessing Diagnostics

- **CLI**: `nbrs bench --explain <expr>` dumps the event stream
  to stderr with formatted explanations
- **Web UI**: event stream available via API for visual inspection
- **Programmatic**: `PolydatKernel::new_with_log()` accepts an
  `Option<&mut CompileEventLog>`

### Example Output

```
$ nbrs bench --explain "hash(cycle)" cycles=1
[parsed]    cycle → graph input #0
[parsed]    hash  → Hash64 node
[wired]     hash.input[0] ← input:cycle
[output]    hash  → selected as program output
[compiled]  1 node, 1 output, 0 constants folded
```
