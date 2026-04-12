# 13: GK Modules and Composition

GK modules enable reusable subgraphs and kernel composition.
The compiler diagnostic event stream provides full visibility
into the compilation process.

---

## Module System

### File-Based Modules

A `.gk` file is a module. Interface is inferred:
- **Inputs**: unbound references (names not defined in the file)
- **Outputs**: all terminal bindings (names defined but not
  consumed by other bindings in the file)

```
// user_generator.gk
inputs := (cycle)
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

### Resolution Chain

Module files are resolved in order:
1. Workload directory (same directory as the `.yaml`)
2. `--gk-lib` paths (CLI argument)
3. Bundled stdlib (compiled into `nb-variates`)
4. Error if not found

---

## Kernel Composition

GK kernels compose at two levels:

### Init → Cycle Staging

Init-time constants from one evaluation feed into cycle-time
computation as folded constants:

```
// These evaluate at init time (no input dependency):
dim := vector_dim("glove-25-angular")
train_count := vector_count("glove-25-angular")

// This uses dim as a baked-in constant at cycle time:
vector := vector_at(cycle, "glove-25-angular")
// dim was folded → vector_at sees a literal, not a node output
```

### Module Embedding

Multiple `.gk` files merge into a single program:
- Shared input namespace
- Merged output map
- Dead code elimination prunes unreferenced bindings
- Single topological sort across the merged DAG

---

## Compiler Diagnostic Event Stream

The compiler emits structured events explaining each step:

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

---

## Strict Mode

Modules can opt into strict compilation:
- All inputs must be declared explicitly
- All function arguments must be named (no positional)
- Unresolved references are errors, not warnings

Strict mode is for library modules intended for reuse. Workload
bindings default to relaxed mode for convenience.
