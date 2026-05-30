# Module System

The mechanism for file-based GK modules: how reusable
`.gk` source files are discovered, inlined into the host
program, and tracked through the resolution chain. This
doc extends three axiom-level statements:

- [grammar.md §2.4 (module definition syntax)](grammar.md) — the surface form.
- [composition_substrate.md S2 (single chokepoint)](composition_substrate.md) — where module composition fits in the substrate.
- [scope_model.md (Combination Modes)](scope_model.md) — modules use the "inline" composition mode.

The nbrs-side compiler-diagnostic event stream stays in
[SRD-13](module_system.md).

> Composition mechanics — how modules combine with the host
> program and with other GK kernels — live in
> [SRD 13b: GK Combination Modes](../../../docs/sysref/13b_gk_combination_modes.md).
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
3. Bundled stdlib (compiled into `polydat`)
4. Error if not found

### Strict Mode

Modules can opt into strict compilation:
- All inputs must be declared explicitly
- All function arguments must be named (no positional)
- Unresolved references are errors, not warnings

Strict mode is for library modules intended for reuse. Workload
bindings default to relaxed mode for convenience. See
[SRD 15](../../../docs/sysref/15_strict_mode.md) for the full
strict-mode contract.
