# SRD 45: GK Compiler Diagnostic Event Stream

## Overview

The GK compiler produces a typed event stream that explains each step
of DSL resolution, optimization, and compilation. This stream is the
single source of truth for understanding what the compiler did and why.
It is available for diagnostic display when the user requests it.

## Event Types

```rust
pub enum GkCompileEvent {
    /// DSL source parsed into AST.
    Parsed { source_len: usize, statements: usize },

    /// A binding was resolved from DSL to node.
    BindingResolved { name: String, node_type: String, inputs: Vec<String> },

    /// A module was loaded and inlined.
    ModuleInlined { name: String, source: String, nodes_added: usize },

    /// A legacy binding chain was translated to GK.
    LegacyTranslated { name: String, chain: String, gk_source: String },

    /// Type adapter inserted between mismatched ports.
    TypeAdapterInserted { from_node: String, from_type: String, to_node: String, to_type: String },

    /// Init-time constant folded (SRD 44).
    ConstantFolded { node: String, value: String },

    /// Fusion pattern matched and applied (SRD 36).
    FusionApplied { pattern: String, replaced_nodes: Vec<String>, fused_node: String },

    /// Output declared.
    OutputDeclared { name: String, node: String, port: usize },

    /// Compilation level selected for a node.
    CompileLevelSelected { node: String, level: String },

    /// Workload parameter injected as constant.
    ParamInjected { name: String, value: String },

    /// Warning: unresolved bind point left as placeholder.
    UnresolvedBindPoint { name: String },

    /// Error: compilation failed.
    CompileError { message: String },
}
```

## Usage

The event stream is collected during compilation and optionally
displayed to the user:

```bash
# Show compilation diagnostics
nbrs run workload.yaml --gk-explain

# Show in the web UI graph editor
# (events are sent via websocket to the frontend)
```

### Example Output

```
gk: parsed 12 statements from 340 bytes
gk: resolved binding 'user_id' → mod(hash(cycle), 1000000)
gk: resolved binding 'key' → format_u64(user_id, 10)
gk: param injected: keyspace = "baselines"
gk: param injected: table = "keyvalue"
gk: type adapter inserted: hash[U64] → unit_interval[U64→F64]
gk: constant folded: base (42 → ConstU64)
gk: constant folded: seed (hash(base) → ConstU64(15395265915043915720))
gk: fusion applied: hash+mod → HashMod (2 nodes → 1)
gk: compile level: user_id → P3 (JIT)
gk: compile level: key → P2 (compiled closure)
gk: 8 nodes, 3 outputs, 2 constants folded, 1 fusion applied
```

## Implementation

The compiler accumulates events in a `Vec<GkCompileEvent>` during
compilation. The events are returned alongside the compiled kernel:

```rust
pub struct GkCompileResult {
    pub kernel: GkKernel,
    pub events: Vec<GkCompileEvent>,
}
```

Callers that don't need diagnostics ignore the events. Callers that
want diagnostics (CLI `--gk-explain`, web UI) format and display them.

## Relationship to Other SRDs

- **SRD 24 (Compilation Levels):** Events report which level each
  node compiled to.
- **SRD 36 (Node Fusion):** Events report which fusions were applied.
- **SRD 44 (GK Composition):** Events report constant folding results.
- **SRD 43 (Logging):** Diagnostic events are not logged by default.
  They are only displayed when explicitly requested.
