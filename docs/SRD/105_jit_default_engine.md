# SRD-105 — JIT as the Default Engine: Auto-Mixed Cone Compilation

**Status:** Push 1 implemented 2026-07-09 (extraction + cone node +
  `JitMode` config, default `off`); Pushes 2-4 pending
**Owner:** polydat (cone extraction, cone node, config surface)
**Implementation target:** `polydat/src/compile/assembly.rs` (cone
  extraction pass), a new `polydat/src/compile/cone.rs` (cone node +
  marshalling), `polydat/src/kernel/engines.rs` (unchanged contract,
  new cone-aware enrichment metadata), `polydat/tests/` (differential
  battery)
**Cross-refs:** `polydat/docs/design/jit_boundary.md` (JIT ABI, longjmp
  violation transport), `polydat/docs/design/type_system_alignment.md`
  §8.3-8.4 (slot layers, slice-slot pending), `polydat/docs/design/engines.md`
  (P1/P2/P3 tiers and selection heuristics), `polydat/docs/design/scope_model.md`
  §"Type stability" (cell contract; point 4 panic surfacing),
  SRD-80b (`#[polydat_node]` authoring — the JIT lowering surface),
  SRD 18b + SRD 02 (One Walker — the constraint this design preserves)

---

## What this SRD is for

Polydat compiles every program to the fastest execution form it can,
**by default, with no user configuration**. Nodes with JIT lowerings run
as native code; nodes without them run on the typed interpreter path;
the mix is decided per-node, automatically, at assembly time. This is
the completion of the standing "JIT by default" authoring policy: nodes
have been required to ship P3 lowerings since the initial implementation,
but the production compile path has never executed them.

## The gap this closes (verified 2026-07-09)

The production path hardcodes the interpreter: `compile_filtered` →
`compile_strict` → `PolydatKernel::new_with_inputs`
(`dsl/compile.rs`, `compile/assembly.rs:536-603`). The engine-selection
machinery (`auto_compile_p2/p3`, `select.rs::GraphAnalysis`) is reachable
only from `nbrs/src/bench.rs`. The `jit` cargo feature is already in
polydat's default set — Cranelift ships in every build; only the wiring
is absent.

Whole-kernel P2/P3 replacement is **not** the mechanism, because the
compiled engines are architecturally incompatible with the runtime
contract:

- raw `u64` slot buffers vs `Value` enums (Str/Bytes/JSON/Handle/U128
  and all vectors cannot cross; F64 rides as bits, Bool as 0/1);
- slot indices vs named wires (scope chains, `lookup`,
  `materialize_wiring_from_outer` assume names);
- no SharedCell participation, no per-cycle `set_input`, no captures,
  no runtime-context reads (`cycle`/`control`/`rate`/`phase` nodes);
- externs baked as immediates.

## Decision

**Cone-level JIT inside the interpreter kernel.** At assembly time,
extract maximal JIT-eligible cones — connected subgraphs whose nodes
all carry JIT lowerings and whose boundary ports are scalar-representable
(U64/I64/U32/I32/F64/F32/Bool) — compile each cone to a `JitKernelRaw`,
and replace it in the program with **one synthetic cone node**. The cone
node is an ordinary `PolydatNode`: `eval` marshals boundary `Value`s into
the cone's u64 slots (types fixed at compile time, so marshalling is a
static per-port operation, not a dynamic funnel), runs the native code,
and writes outputs back as `Value`s.

Everything load-bearing is preserved *by construction*:

1. **One Walker** (SRD 18b/02): the walker never learns about JIT. A
   cone is a node. Pre-map, dryrun, and runtime traverse the same graph.
2. **Named I/O, scope chains, cells, captures, per-cycle writes**: all
   remain interpreter concerns at cone boundaries. Cell-bound wires stay
   interpreter input slots, so `check_cell_clean` cross-fiber revision
   invalidation sees cone inputs exactly as it sees any node's inputs.
3. **Type stability** (scope_model.md): cone boundary types are declared
   `PortType`s; the same write-site validation applies. No new coercion
   surface is introduced.
4. **Panic contract**: cone eval runs inside `eval_node`'s
   suppress-enrich-reraise wrap. Predicate violations inside native code
   already transport via longjmp → `invoke_with_catch` → Rust panic
   (jit_boundary.md); the enriched message must attribute the *interior*
   node, not the synthetic cone — see "Cone diagnostics" below.
5. **SRD-74 None propagation**: every JIT-able node is in the
   None-propagating class, so the kernel-level None guard applies to the
   cone node uniformly: any `None` boundary input → all cone outputs
   `None`, native code not invoked.

## Mechanism

### Eligibility (per node)

A node joins a cone iff:
- it has a JIT classification (`compile/jit/codegen.rs::classify_node`);
- all its ports in the cone-local dataflow are scalar-representable;
- it is stateless and side-effect-free (stateful RNG, accumulators,
  runtime-context reads, dataset/resource accessors are interpreter-only
  by classification already);
- it does not `accepts_none_inputs` (coalesce-style nodes stay outside
  so the uniform None guard holds);
- it classifies **Dynamic** under SRD 11's three-lifecycle analysis
  (re-derived at extraction). CompileConst and ScopeInit subgraphs
  belong to the fold passes, which evaluate them ONCE and replace
  them with literals; fusing them would demote them to per-pull
  native evaluation and — because `fold_init_constants` only
  replaces single-output nodes — break `get_constant` consumers
  such as `eval_const_expr`. Per-cycle fusion is only profitable
  for per-cycle work anyway; Dynamic is exactly that set.

### Cone construction

- Maximal cones under the eligibility relation, split so no cone exceeds
  64 boundary inputs (one provenance word).
- An interior port that is also a named program output, or that feeds a
  non-JIT node, becomes an additional cone boundary output (the JIT
  `output_map` already supports multi-output kernels).
- Assembler-known immediates bake into the native code via the existing
  `jit_constants()` surface. Values produced by const *nodes* (e.g.
  resource/dataset accessors) enter as boundary inputs — never baked —
  so const-freeze order and SRD-67 lifecycles are untouched.
- Cost model: a cone must clear a minimum fused-node threshold (initial
  value 2; single-node cones pay boundary marshalling for zero fusion
  win). `force` mode sets the threshold to 1 for test coverage.

### Invalidation

The cone node participates in `node_clean` like any node: the
interpreter's dirty tracking operates at cone granularity. Interior
re-evaluation is unconditional (`JitKernelRaw`) — correct because the
interpreter only invokes the cone when its boundary changed. Adopting
Push/Pull JIT variants *inside* cones is a measured follow-up, not part
of the initial landing (the interpreter already provides the coarse
guard the variants duplicate).

### Cone diagnostics

The cone node carries metadata: member node names, their source
bindings, and the slot→member provenance map
(`compute_jit_slot_provenance`). A violation payload from the
`jit_*_fail` externs names the predicate; cone enrichment prepends the
member attribution so the enriched panic reads as if the interior node
were interpreter-evaluated. The differential battery asserts message
parity for the predicate-violation cases.

### Config surface

`kernel.jit: auto | off | force`, default **`auto`**.
- `auto` — cone extraction with the cost model. The default; no user
  action required.
- `off` — pure interpreter. Escape hatch and the differential baseline.
- `force` — every eligible node joins a cone (threshold 1). Used by the
  battery; useful for isolating marshalling regressions.

One canonical name; no aliases. The setting is a kernel-compile
parameter, not a per-phase toggle.

## Correctness gate

The default flips to `auto` only behind a green **differential
battery**: every workload in the thematic coverage suite runs
`force` vs `off` and compares outputs cycle-for-cycle — polydat's
determinism makes the comparison exact, including F64 (transcendentals
call the same Rust functions via extern symbols; Cranelift scalar ops
are IEEE-identical). Predicate-violation workloads assert panic-message
parity. The battery is a permanent suite, not a one-time check.

## Pushes

1. **Cone extraction + cone node + config surface** — mechanism lands
   with default `off`. Bench harness gains cone-aware reporting.
2. **Differential battery + panic parity** — coverage-suite sweep,
   force-vs-off, violation attribution tests.
3. **Default flip to `auto`** — one-line change, gated on Push 2 green
   in CI. This is part of the same effort, not a deferred aspiration.
4. **Measured extensions** (each its own decision, driven by bench
   evidence): slice-slot `(ptr,len)` vector ABI
   (type_system_alignment.md §8.3 phases 5-6 — unblocks vector-heavy
   workloads, the largest expected win); Push/Pull cone variants;
   extern-param slots (avoid cone rebuild on dynamic-control change);
   U128 two-slot ABI.

## Known consequence: program identity across modes

Extraction rewrites the node list, so `canonical_hash` differs
between a `jit: off` and `jit: auto` compile of the same source.
Within one process configuration the hash is deterministic (same
mode → same cones → same hash), but resume-skip identity matching
across mode changes will see distinct programs. Before the Push 3
default flip, either compute program identity from the
pre-extraction graph or accept and document the mode-change
boundary.

## Rejected alternatives

- **Whole-kernel P2/P3 selection** (`auto_compile_p2/p3` promoted to the
  production path): incompatible with `Value`, named wires, cells, and
  per-cycle writes — a runtime rewrite with no proportional payoff. The
  bench-only entry points remain for measurement.
- **Hybrid kernels owning the graph** (`compile_hybrid`): benchmarked as
  strictly dominated by P3 segments, and it constitutes a second walker.
- **Runtime Value↔u64 funnel at every port**: defeats the JIT's purpose
  and violates the type-stability decision (no dynamic coercion
  surface).
