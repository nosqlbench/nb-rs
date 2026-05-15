# 13: GK Modules

File-based GK modules: how reusable `.gk` source files are
discovered, inlined, and tracked through the compiler's
diagnostic event stream.

> Composition mechanics — how modules combine with the host
> program and with other GK kernels — live in
> [SRD 13b: GK Combination Modes](13b_gk_combination_modes.md).
> This file covers only the module-as-source-file system.

---

## Module System

### File-Based Modules

A `.gk` file is a module. Interface is inferred:
- **Inputs**: unbound references (names not defined in the file)
- **Outputs**: all terminal bindings (names defined but not
  consumed by other bindings in the file)

```
// user_generator.gk
input cycle: u64
user_id := mod(hash(cycle), 1000000)
username := format_u64(user_id, 8)
email := "{username}@example.com"
```

### Module Inlining

When a host program references a module, the compiler:
1. Parses the module source
2. Prefixes all internal node names to avoid collision
3. Wires module inputs to host node outputs
4. Exposes module outputs as host bindings

```
// host workload bindings
use "user_generator.gk"
full_name := weighted_strings(hash(cycle), "names.csv")
```

The combined program is a single DAG: shared input
namespace, merged output map, dead-code elimination pruning
unreferenced bindings, one topological sort across the
merged result. This is the **inline** combination mode (mode
1 in SRD 13b's taxonomy); the module's nodes become
indistinguishable from the host's once compiled.

### Resolution Chain

Module files are resolved in order:
1. Workload directory (same directory as the `.yaml`)
2. `--gk-lib` paths (CLI argument)
3. Bundled stdlib (compiled into `nbrs-variates`)
4. Error if not found

### Strict Mode

Modules can opt into strict compilation:
- All inputs must be declared explicitly
- All function arguments must be named (no positional)
- Unresolved references are errors, not warnings

Strict mode is for library modules intended for reuse. Workload
bindings default to relaxed mode for convenience. See SRD 15
for the full strict-mode contract.

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
- **Programmatic**: `GkKernel::new_with_log()` accepts an
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
