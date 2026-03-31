# Pending SRD Implementations

## Fully Implemented

- [x] Probability nodes (SRD 29) — fair_coin, unfair_coin, select, chance, n_of, one_of, one_of_weighted, blend
- [x] Model adapter (SRD 29) — driver=model, --diagnose, result map, runtime latency/error injection
- [x] Capture context runtime (SRD 28) — CaptureContext, volatile/sticky ports, set/reset/apply
- [x] External input ports (SRD 28) — WireSource::VolatilePort/StickyPort, PortDef, buffer layout
- [x] Qualified bind points (SRD 28) — {coord:name}, {capture:name}, {bind:name}, {port:name}
- [x] Adapter capture extraction trait (SRD 28) — CaptureDecl, CaptureExtractor
- [x] --strict mode (SRD 27) — coordinates, named args, all inputs
- [x] Dead code elimination (SRD 27) — assembler prunes unreachable nodes
- [x] Coordinate inference (SRD 26) — unbound refs become coordinates
- [x] GK modules (SRD 27) — resolution, inlining, formal sigs, type checking
- [x] Embedded stdlib (SRD 30) — 8 category files, 17 modules
- [x] describe gk functions/stdlib/dag (SRD 30)
- [x] PCG nodes (SRD 25) — pcg, pcg_stream, cycle_walk
- [x] DAG visualization — DOT with ports, Mermaid, SVG via layout-rs
- [x] GkProgram/GkState split (SRD 24) — fiber-safe sharing
- [x] NodeFactory trait (SRD 31) — GkRuntime, unified registry
- [x] FuncCategory enum — type-safe categories in registry
- [x] Variadic nodes — sum, product, min, max with identity elements
- [x] Printf node — variadic formatting
- [x] Web UI (SRD 32) — nb-web crate, Axum + htmx

## Remaining — Small/Medium Items

- [ ] **--gk-lib flag** (SRD 30)
  CLI flag for user library paths. Threading through to compiler.

- [ ] **describe gk modules** (SRD 30)
  List modules in workload directory + user library.

- [ ] **`extern volatile`/`extern sticky` GK grammar** (SRD 28)
  Parser support for declaring external ports in .gk source.
  The runtime supports them; the grammar doesn't parse them yet.

- [ ] **Ambiguity warnings for bind points** (SRD 28)
  Warn when an unqualified {name} exists in multiple namespaces.
  Error in --strict mode.

- [ ] **GK-computed results for model adapter** (SRD 29)
  `result: |` string form parsed as GK kernel for computed results.

- [ ] **result-rows for model adapter** (SRD 29)
  Multi-row result simulation.

- [ ] **Stanza-level executor loop** (SRD 28)
  Currently each cycle processes one op. For captures to flow between
  ops, the executor needs a stanza-aware mode that processes all ops
  in sequence and applies captures between them.

- [ ] **Update bimodal stdlib modules** (SRD 29/30)
  modeling.gk stubs can now use select/unfair_coin — update them.

## Remaining — Larger Items

- [ ] **Serialization formats** (SRD 28)
  CBOR wire format, CDDL schema language, JSONL capture logs.

- [ ] **WebSocket live metrics** (SRD 32)
  htmx WS extension for real-time dashboard updates from MetricsFrame.

- [ ] **Activity control API** (SRD 32)
  Start/pause/stop activities from the web UI.

- [ ] **HTTP adapter** (first real adapter)
  The first non-stdout/non-model adapter for actual workloads.

- [ ] **Built-in nodes as NodeFactory** (SRD 31)
  Refactor build_node hardcoded match into a BuiltinsFactory for
  fully uniform dispatch.
