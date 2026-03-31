# Pending SRD Implementations

Ordered by dependency and impact. Items marked [BLOCKED] depend on
an earlier item.

## Tier 1 — Core Runtime Capabilities

These enable the model adapter and inter-op data flow.

- [x] **Probability nodes** (SRD 29) — DONE
  `select`, `fair_coin`, `unfair_coin`, `chance`, `n_of` implemented.
  Remaining: `one_of`, `one_of_weighted`, `blend`.

- [x] **Model adapter** (SRD 29) — DONE (basic)
  `driver=model` works, `--diagnose` flag works, `result` map form
  parsed. Remaining: GK-computed results, latency injection at
  runtime, error injection at runtime, `result-rows`.

- [ ] **Capture context runtime** (SRD 28)
  Stanza-scoped `CaptureContext` with volatile/sticky ports,
  default values, reset on set_coordinates(). Adapter extraction
  trait. Wiring captures to next op's GK kernel.
  Unblocks: multi-op workload flows.

- [ ] **External input ports** (SRD 28)
  Volatile and sticky buffer regions in GK kernel. `extern` port
  declarations in GK grammar. Buffer layout: coords | volatile |
  sticky | node outputs.
  [BLOCKED by capture context]

## Tier 2 — Language & Tooling

- [ ] **Qualified bind points** (SRD 28)
  `{coord:name}`, `{capture:name}`, `{bind:name}`, `{port:name}`
  parsing in bindpoints.rs. Resolution order: bindings → captures →
  coordinates. Ambiguity warnings (default) / errors (strict).

- [x] **--strict mode** (SRD 27) — DONE
  Enforces: explicit coordinates, named-only module args, all module
  inputs provided. 3 new tests.

- [x] **Dead code elimination** (SRD 27) — DONE
  Assembler traces backward from outputs, prunes unreachable nodes.
  compile_bindings passes referenced bind points as required outputs.
  4 new tests.

- [ ] **--gk-lib flag** (SRD 30)
  CLI flag for user library directories. Multiple paths, searched
  between workload dir and embedded stdlib.

- [x] **describe gk stdlib** (SRD 30) — DONE
  `nbrs describe gk stdlib` lists all embedded modules with typed
  signatures and descriptions.

- [ ] **describe gk modules** (SRD 30)
  CLI command listing modules in current workload directory +
  user library.

## Tier 3 — PCG & Advanced Generation

- [ ] **PCG-RXS-M-XS 64/64 nodes** (SRD 25)
  `pcg(seed, stream)`, `pcg_stream(seed)`, `pcg_dyn`,
  `pcg_n(seed, stream, count)`. Pure-function seek model.
  Extern call JIT (P3). Sequential access memoization.

- [ ] **cycle_walk node** (SRD 25/27)
  Bijective permutation via PCG cycle-walking. Auto-period
  selection. Enables euler_circuit stdlib module.

## Tier 4 — Serialization & Visualization

- [ ] **Serialization formats** (SRD 28)
  CBOR wire format, CDDL schema language, JSONL capture event logs.
  Requirements study for format selection.

- [x] **DAG visualization** (new) — DONE
  `gk_to_dot()`, `gk_to_mermaid()`, `gk_to_svg()` in nb-variates/src/viz.rs.
  Pure-Rust SVG via petgraph + layout-rs. Remaining: wire into
  `nbrs describe gk dag` CLI command.
