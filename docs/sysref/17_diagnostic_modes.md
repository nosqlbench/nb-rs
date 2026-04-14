# 17: Diagnostic Modes

How nb-rs workloads are inspected, validated, and explained
without (or with controlled) execution.

---

## Principles

1. **Diagnostics use the same pipeline as execution.** There
   is no separate "describe" code path. The runner compiles
   scopes, resolves for_each, wires auto-externs, and builds
   kernels identically in diagnostic and execution modes.
   Diagnostics branch at the activity boundary — after all
   compilation is complete.

2. **Two orthogonal axes.** Diagnostic behavior is controlled
   by execution depth (how far to go) and diagnostic output
   (what to explain). These are independent.

3. **The GK subsystem is the sole authority.** Provenance,
   data flow, const-folding, scope composition — all
   explanations come from the compiled GK kernel's
   introspection APIs, not from re-analysis of source text.

---

## Execution Depth

Controls how far through the pipeline to execute.

| Depth | Compiles? | Resolves for_each? | Runs cycles? | Creates adapters? |
|-------|-----------|-------------------|--------------|-------------------|
| `phase` | Yes | Yes | No | No |
| `cycle` | Yes | Yes | Yes (dry-run adapter) | Dry-run only |
| `full` | Yes | Yes | Yes | Yes (normal) |

- **`phase`**: compiles all kernels, resolves all scope
  composition, validates all bind points. Stops before
  creating adapters or running cycles. Use for compile-time
  validation and GK explanation.

- **`cycle`**: runs cycles with a dry-run adapter that
  prints resolved op text (or silently discards). Tests the
  full per-cycle pipeline including GK evaluation, field
  resolution, and op template rendering.

- **`full`**: normal execution (default).

---

## Diagnostic Output

Controls what explanations are emitted during execution.

| Flag | Output |
|------|--------|
| `gk` | GK kernel analysis: inputs, outputs, provenance, const-folding, scope composition, modifiers |

Future flags may include `ops` (resolved op templates),
`adapters` (adapter mapping), `metrics` (live metric names).

---

## CLI Syntax

```
# Compile and explain GK data flow, stop before cycles
nbrs run workload=file.yaml dryrun=phase,gk

# Compile and explain, with dry-run cycle execution
nbrs run workload=file.yaml dryrun=cycle,gk

# Just compile validation, no output
nbrs run workload=file.yaml dryrun=phase

# Normal execution with GK explanations interleaved
nbrs run workload=file.yaml dryrun=gk
```

The `dryrun` parameter is a comma-separated list of flags.
Execution depth flags (`phase`, `cycle`) are mutually
exclusive — last one wins. Diagnostic output flags (`gk`)
are additive.

When no execution depth is specified, `full` is assumed
(normal execution with diagnostic output interleaved).

The `describe` subcommand is shorthand for `dryrun=phase,gk`:
```
nbrs describe workload=file.yaml
# equivalent to:
nbrs run workload=file.yaml dryrun=phase,gk
```

---

## Implementation

The runner parses `dryrun` into a `DiagnosticConfig`:

```rust
struct DiagnosticConfig {
    /// How far to execute: Phase, Cycle, or Full.
    depth: ExecDepth,
    /// Whether to emit GK provenance analysis.
    explain_gk: bool,
}
```

The config is threaded through the runner. At the activity
boundary (after kernel compilation, before cycle dispatch):

- If `explain_gk`: call `describe::print_kernel_analysis()`
  with the compiled program.
- If `depth == Phase`: skip activity creation and cycle
  dispatch. Continue to next phase/iteration.
- If `depth == Cycle`: create activity with dry-run adapter.
- If `depth == Full`: create activity with real adapter.

This is a single conditional at a coarse-grained flow point.
No performance impact on normal execution.

---

## What GK Explain Shows

For each scope (workload, phase, for_each iteration):

- **Inputs**: coordinate vs extern, with index
- **Bindings**: name, type, modifier (shared/final/none)
  - Const-folded: value shown, computed once at compile time
  - Per-cycle: input dependencies listed, node function shown
  - Init-time constant: no wire inputs, computed once
- **Node wiring**: upstream connections for each binding
- **Scope composition**: which names are auto-externed from
  outer scope, which are shadowed

The explanation comes from the compiled `GkProgram`'s
introspection APIs — the same data structures used at runtime.
