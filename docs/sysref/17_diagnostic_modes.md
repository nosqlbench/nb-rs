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

Controls how far through the pipeline to execute. Depth is a
**single discriminant** read by **the same walker** every level
of diagnosis and execution shares — there is no parallel
"describe" walker or "pre-map" walker that re-implements
traversal at a different fidelity. The walker descends the
scenario tree, performing each layer's *structural* work
unconditionally; the depth value chooses the deepest layer whose
*executional* work runs for real vs. via an observer / dryrun
wrapper.

Pre-map (the SceneTree-and-scope-build pass that the TUI and
checkpoint subsystem consume) is **the walker at `depth=phase`**;
full execution is **the walker at `depth=cycle` or `depth=full`**.
They are not different code. See SRD 18b §"Single Walker
Contract" for the load-bearing version of this commitment.

| Depth | Compiles? | Resolves for_each? | Builds SceneTree? | Runs cycles? | Creates adapters? |
|-------|-----------|-------------------|-------------------|--------------|-------------------|
| `phase` | Yes | Yes | Yes | No | No |
| `cycle` | Yes | Yes | Yes | Yes (dry-run adapter) | Dry-run only |
| `full` | Yes | Yes | Yes | Yes | Yes (normal) |

- **`phase`**: compiles all kernels, resolves all scope
  composition, materialises every comprehension iteration's
  bound kernel (polydat comprehension consumption per
  `polydat/docs/design/comprehension_forms.md` §9.5),
  validates all bind points, populates the SceneTree. Stops
  before creating adapters or running cycles. This is the
  depth pre-map runs at. Use for compile-time validation, GK
  explanation, and TUI plan-preview.

- **`cycle`**: enters `run_phase`, builds the wrapper stack with
  the **dryrun wrapper** at the leaf instead of the real adapter
  wrapper, runs cycles through it. Exercises the full per-cycle
  pipeline (GK evaluation, field resolution, op-template render,
  wrapper composition) without contacting the real backend.

- **`full`**: same walk, real adapter wrapper at the leaf.

### Visitor framing

Each scope-tree layer (scenario, comprehension, bindings, phase,
op, cycle) is a visitor with two responsibilities: *structural*
(always runs at all depths ≥ that layer) and *executional*
(runs only when depth equals or exceeds that layer's "real"
threshold; lower depth substitutes an observer). New depth
values extend this enum along one axis; they don't introduce new
code paths.

---

## Diagnostic Output

Controls what explanations are emitted during execution.

| Flag | Output |
|------|--------|
| `wiring` | Value-provenance / wiring view: how each named wire was computed, what inputs it depends on, where those inputs came from (const-folding, scope composition, modifiers). Needs depth ≥ Op — a bare `dryrun=wiring` auto-bumps depth to `Op` so kernels exist to render. |
| `labels` | Dimensional labels for all phases — the coordinate-tuple labelling that scope-tree nodes carry. |

Future flags may include `ops` (resolved op templates),
`adapters` (adapter mapping), `metrics` (live metric names).

---

## CLI Syntax

```
# Compile and dump the wiring view, stop before cycles
nbrs run workload=file.yaml dryrun=op,wiring

# Compile and dump wiring, with dry-run cycle execution
nbrs run workload=file.yaml dryrun=cycle,wiring

# Plan dump only (phase structure, no kernels)
nbrs run workload=file.yaml dryrun=phase

# Bare wiring — auto-bumps depth to Op so kernels exist
nbrs run workload=file.yaml dryrun=wiring
```

The `dryrun` parameter is a comma-separated list of flags.
Execution depth flags (`phase`, `op`, `cycle`, `full`) are
mutually exclusive — last one wins. Output-filter flags
(`wiring`, `labels`) are additive.

When no execution depth is specified, `phase` is assumed
(plan dump). The `wiring` flag is special-cased: if no depth
is set, it bumps to `Op` so kernels exist to render.

---

## Implementation

The runner parses `dryrun` into a `DiagnosticConfig`:

```rust
struct DiagnosticConfig {
    /// How far to execute: Phase, Op, Cycle, or Full.
    depth: ExecDepth,
    /// Emit value-provenance / wiring view.
    show_wiring: bool,
    /// Emit dimensional labels for all phases.
    show_labels: bool,
    /// Walk the post-construction component tree, dump every
    /// declared dynamic control, exit.
    list_controls: bool,
}
```

The config is threaded through the runner. At the activity
boundary (after kernel compilation, before cycle dispatch):

- If `show_wiring`: call `describe::print_wiring_analysis()`
  with the compiled program (requires depth ≥ Op).
- If `depth == Phase`: skip activity creation and cycle
  dispatch. Continue to next phase/iteration.
- If `depth == Op`: run op-template kernel synthesis +
  `map_op` + metric registration; dump scope-flattening
  summary; exit before cycles.
- If `depth == Cycle`: create activity with dry-run adapter.
- If `depth == Full`: create activity with real adapter.

This is a single conditional at a coarse-grained flow point.
No performance impact on normal execution.

---

## What Wiring Shows

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
