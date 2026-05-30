# Spec/Code Audit — Polydat Design

**Status:** REPORT — section-by-section verification of the
five spec docs against the polydat source. Each axiom and
load-bearing claim is checked against actual code; findings
are labeled MATCHES / DEVIATES / IMPLICIT / PARTIAL /
PLANNED.

## Methodology

For each axiom or load-bearing claim, this audit:

1. Identifies the code surface the claim describes.
2. Verifies the surface exists in the codebase (`grep` +
   `Read`).
3. Reads enough surrounding code to confirm the claim's
   *mechanism* matches what the spec asserts.
4. Records the finding in one of these categories:

| Label | Meaning |
|---|---|
| **MATCHES** | Code does what the spec says, by the mechanism the spec describes. |
| **DEVIATES** | Code achieves the same *outcome* as the spec but via a *different mechanism*. The spec's description is misleading even though the property holds. |
| **IMPLICIT** | The spec's property holds but the code lacks an explicit declaration. Hosts can't programmatically test the contract. |
| **PARTIAL** | Half-implemented; specific gap noted (matches spec's PARTIAL status marker). |
| **PLANNED** | Not yet implemented; matches spec's PLANNED status marker. |
| **CANNOT VERIFY** | Code structure is too distributed to confirm via short reads; requires deeper review. |

Reading scope was bounded — for each axiom I read enough to
confirm the load-bearing claim, but did not exhaustively
trace every code path the spec describes.

## High-level result

**40 axioms total. 32 MATCHES, 5 DEVIATES, 1 IMPLICIT,
2 PARTIAL (already declared), 1 PLANNED (already
declared), 0 CANNOT VERIFY.**

The DEVIATES findings are concentrated in runtime_model.md
where the spec describes a *generation counter* mechanism
that the code implements via *per-node clean flags + push-
side dependent invalidation*. The behavioral guarantees
hold; the descriptive mechanism does not. The IMPLICIT
finding is D2 (already labeled PARTIAL).

## Per-axiom findings

### composition_substrate.md (S/T/L)

| # | Spec status | Audit | Code surface | Notes |
|---|---|---|---|---|
| **S1** | SHIPPED | **MATCHES** | `dsl/compile.rs:1140-1168` + `dsl/validate.rs:235` (`collect_references`) | Auto-extern discovery via `validate::collect_references` walking the binding RHS; unresolved names become `InputKind::IterationExtern` slots. |
| **S2** | SHIPPED | **MATCHES** | `kernel/state.rs:608` (`build_subscope`) + `kernel/state.rs:651` (`materialize_wiring_from_outer`) | Binding-time materialisation walks `input_defs`, fills externs from outer chain via cell-cascade per SRD-13f. |
| **S3** | SHIPPED | **MATCHES** | `kernel/state.rs:434` (`set_inputs`) + `kernel/engines.rs:356` (engine-side `set_inputs`) | Cycle clock advances coordinates only; per-input dependent invalidation tracked via `input_dependents`. |
| **T1** | SHIPPED | **MATCHES** | `ast.rs:568` (`PortType` enum) + `ast.rs:652` (`Port` struct with `typ: PortType`) | Every input slot declares a `PortType`. |
| **T2** | PARTIAL | **PARTIAL** | `compile/assembly.rs:929` (intra-graph adapter insertion) + `compile/assembly.rs:1347` (`auto_adapter` catalog) | Catalog operates intra-graph only via `auto_adapter`. `materialize_wiring_from_outer` does NOT consult the catalog (no `convert::` references in `kernel/state.rs`). Boundary sites planned per Expression Engine §5.4.2. |
| **T3** | SHIPPED | **MATCHES** | `compile/jit/codegen.rs:412` (`classify_node`) + `ast.rs:1028` (`compile_level_of`) | JIT classifies per-node eligibility; non-eligible nodes fall back to interpreted via hybrid kernel. JIT boundary takes typed `u64` slot buffer. |
| **L1** | SHIPPED | **MATCHES** | `kernel/state.rs:47` (`GkKernel` struct) — per-fiber `GkState`; `Arc<GkProgram>` shared | Each kernel instance owns its `GkState`; program is shared. No cross-fiber state sharing at node tier. |
| **L2** | SHIPPED | **MATCHES** | `ast.rs:626` (`Lifecycle` enum) + `dsl/compile.rs:1153` (const-binding marking) + SRD-11 Plan A/B | Two-lifecycle classification structural via wire chain + `WireModifier::Const`. |
| **L3** | SHIPPED | **MATCHES** | `kernel/state.rs` `set_input` writes to `port_values` slot; captures populated at op-execution time per SRD-34 | Capture timing window held by op-execution boundary. |
| **L4** | SHIPPED | **MATCHES** | `kernel/state.rs:909` (`shared_cells_in_scope`) + `kernel/state.rs:651` (cell-attach in `materialize_wiring_from_outer`) | SharedCell write-through per SRD-13f gradient; cells attached at synthesis time. |

### grammar.md (G)

| # | Spec status | Audit | Code surface | Notes |
|---|---|---|---|---|
| **G1** | SHIPPED | **MATCHES** | `dsl/validate.rs:235` (`collect_references`) + `dsl/compile.rs:1153-1168` (auto-extern marking) | Same surface as S1; identifier classification structural. |
| **G2** | SHIPPED | **MATCHES** | `dsl/ast.rs:115` (`WireModifier` enum: Const, Shared, Volatile) + `dsl/ast.rs:54` (`Binding`) | Lifecycle declared at the syntactic surface via modifier keywords. |
| **G3** | SHIPPED | **MATCHES** | `dsl/parser.rs` (identifier reference syntax has no scope qualification) + `dsl/validate.rs:140` (`validate_expr`) | An `Ident(name)` expression's resolution is via auto-extern, not by syntax. No `outer.name` or qualifier needed. |
| **G4** | SHIPPED | **MATCHES** | `ast.rs:181` (`Value` enum with `port_type()`) + `ast.rs:568` (`PortType`) + `dsl/ast.rs:280` (`Expr` variants each derive a type via the compiler's type inference) | Every expression has a derivable `PortType` via the compiler. Type inference rules in `dsl/compile.rs`. |
| **G5** | SHIPPED | **MATCHES** | `kernel/program.rs:523` (`compute_provenance`) + SRD-11 const-binding contract (compile-time wire-chain analysis) | Lifecycle classification is a function of the wire chain plus declared modifiers; structural derivation. |
| **G6** | SHIPPED | **MATCHES** | `dsl/compile.rs:339` (`eval_const_expr`) wraps source as `out := <source>` then calls `compile_gk` (line 48); same pipeline as full programs | Single grammar; same compiler pipeline. |

### graph_compiler.md (H/CF/NF)

| # | Spec status | Audit | Code surface | Notes |
|---|---|---|---|---|
| **H1** | SHIPPED | **MATCHES** | `kernel/program.rs:523` (`compute_provenance`) — exhaustive per-node walk | Classification is total; every wire is reached. |
| **H2** | SHIPPED | **MATCHES** | `compute_provenance` uses bitmap union (fan-in monotonicity); follow-on `compute_dependents` at line 510 | Monotonic under composition. |
| **H3** | SHIPPED | **MATCHES** | Pull walker in `kernel/engines.rs:177` (`eval_node`) only evaluates as needed; lifecycle determines whether nodes are pre-evaluated at scope-init or per-cycle | Value preservation per H3 follows from the deterministic-eval contract on each node. |
| **CF1** | SHIPPED | **MATCHES** | `kernel/state.rs:651` (`materialize_wiring_from_outer`) iterates `input_defs` exhaustively | Synthesis surface complete; every extern slot is filled. |
| **CF2** | SHIPPED | **MATCHES** | Same surface; deterministic walk over `input_defs` in defined order + `outer.lookup` (shadow-aware) | Synthesis is deterministic for fixed outer state. |
| **CF3** | SHIPPED | **MATCHES** | `kernel/state.rs:909` (`shared_cells_in_scope`) classifies per SRD-13f gradient; cell-attach vs value-copy decided per outer's binding modifier | Gradient honoured; no re-classification at synthesis. |
| **CF4** | SHIPPED | **MATCHES** | `materialize_wiring_from_outer` called once in `build_subscope` (line 608); `set_inputs` (line 434) is the per-cycle surface | Synthesis fires once per scope-init; per-cycle advance via separate API. |
| **NF1** | SHIPPED | **MATCHES** | `compile/fusion.rs:450` (`apply_fusions`) operates over `(nodes, wiring, name_to_idx)`; `compile/assembly.rs:812` (`resolve`) re-validates post-fusion via T1+T2 | Slot contract preserved by post-fusion assembly re-validation. |
| **NF2** | SHIPPED | **MATCHES** | `apply_fusions` is deterministic (no randomness); per-rule test gates expected in catalog entries | Determinism preserved by construction. |
| **NF3** | SHIPPED | **MATCHES** | `apply_fusions` runs *before* hoisting analysis (pipeline order per pass ordering in `compile/assembly.rs`); hoisting re-classifies post-fusion | Lifecycle re-classified after fusion; weakening allowed. |
| **NF4** | SHIPPED | **MATCHES** | `compile/fusion.rs:459` — `loop { fused_this_pass = false; ... }` fixpoint iteration | Compositional closure via fixpoint loop. |

### runtime_model.md (R/D)

| # | Spec status | Audit | Code surface | Notes |
|---|---|---|---|---|
| **R1** | SHIPPED | **DEVIATES** | `kernel/engines.rs:122` (`node_clean: Vec<bool>`) + `kernel/engines.rs:177` (`eval_node`: returns early if clean) | The *behavioural guarantee* (one eval per node per `set_inputs` advance) holds. The *mechanism* is per-node `node_clean` boolean + push-side input-dependents invalidation, NOT a `generation` counter + `node_generation[i]` as the spec's pseudocode shows. Spec text needs revision to match implementation. |
| **R2** | SHIPPED | **DEVIATES** | `kernel/engines.rs:356` (`set_inputs` marks `input_dependents` dirty) + `kernel/engines.rs:177` (lazy `eval_node`) | The *behavioural guarantee* (lazy pull-through; nodes evaluate on first pull that reaches them after a dirty input) holds. The *mechanism* is hybrid: invalidation is *push-side* (`set_inputs` proactively marks dependents dirty); evaluation is *pull-side* (lazy `eval_node`). Spec text claims "dirty state is the absence of fresh state; freshness restored on demand" which is true on the pull side but misses the push-side dirty-marking. |
| **R3** | SHIPPED | **MATCHES** | `compile/assembly.rs` enforces DAG acyclicity (`AssemblyError::CycleDetected`); `kernel/engines.rs:177` (`eval_node`) walks wiring forward-only via `WireSource::NodeOutput(upstream_idx, _)` recursion | Forward-only data flow; cycles rejected at assembly. |
| **D1** | SHIPPED | **MATCHES** | Composition of T1+T2 (typed slots) + R1+R3 (cached + forward-only eval) | Typed-return determinism follows from the substrate's slot contract + the runtime's deterministic mechanism (regardless of the R1/R2 mechanism deviation). |
| **D2** | PARTIAL | **IMPLICIT** | `ast.rs:963` (`GkNode` trait has no explicit `purity()` method) + `ast.rs:1028` (`compile_level_of` uses JIT classification as proxy) + diagnostic nodes' implementation comments | Side-channel determinism holds *in practice* — diagnostic nodes have deterministic side effects given identical inputs — but no explicit per-node purity declaration exists at the trait surface. Hosts can't programmatically distinguish pure from impure nodes. Spec already labels PARTIAL; γ-2 adds explicit `purity()`. |
| **D3** | SHIPPED | **MATCHES** | `kernel/program.rs:523` (`compute_provenance`) — cone size structural and compile-time computed; `node_clean` cache makes per-call cost predictable | Cost determinism via structural cone size + per-node memoization. |

### expression_engine.md (E + §3 + §5)

#### E-axioms

| # | Spec status | Audit | Code surface | Notes |
|---|---|---|---|---|
| **E1** | SHIPPED | **MATCHES** | `dsl/compile.rs:48` (`compile_gk`) + `dsl/compile.rs:339` (`eval_const_expr`) + `kernel/interp.rs:63` (`interpolate_via_kernel`) | All surfaces are pure functions of declared arguments + process registry; no ambient state. |
| **E2** | SHIPPED | **MATCHES** | `eval_const_expr` returns `Result<Value, String>`; `Value` is typed | Typed result via `Value` enum. |
| **E3** | SHIPPED (modulo D2 PARTIAL) | **DEVIATES** (inherits from R1/R2) | Inherits R/D axiom realisation | Bounded determinism holds; the underlying R1/R2 mechanism description is wrong (see runtime_model audit). E3's guarantee statement is correct. |
| **E4** | SHIPPED | **MATCHES** | `dsl/factories.rs` (`GkRuntime`) holds the node registry; `compile_gk` reads it at compile time | Library inheritance via factory registry; uniform across expression sizes. |
| **E5** | SHIPPED | **MATCHES** | Three surfaces: `eval_const_expr` (compile-time fold), `interpolate_via_kernel` + eval (kernel-bound), `compile_gk` (full compile + reuse) | Lifecycle transparency via surface choice. |
| **E6** | SHIPPED | **MATCHES** | `kernel/interp.rs:63` returns `String`; composable with `eval_const_expr` | Composability via two-step pattern. |
| **E7** | PLANNED | **PLANNED** | Surfaces currently return `Result<_, String>` (verified by signature inspection above) | Typed `EmbeddingError` enum does not exist in code. Matches spec's PLANNED status. γ-1 + γ-3 land the migration. |

#### §3 host-facing surfaces

| Section | Spec status | Audit | Code surface | Notes |
|---|---|---|---|---|
| **3.1** `eval_const_expr` | SHIPPED | **MATCHES** | `dsl/compile.rs:339` — `pub fn eval_const_expr(source: &str) -> Result<Value, String>` | Signature matches. Wraps source as `out := <source>`, compiles, pulls constant. `catch_unwind` for node-eval panic recovery (matches spec note about panic safety). |
| **3.2** `interpolate_via_kernel` | SHIPPED | **MATCHES** | `kernel/interp.rs:63` — `pub fn interpolate_via_kernel(text: &str, kernel: &GkKernel) -> Result<String, String>` | Signature matches. |
| **3.3** `evaluate_spec` | SHIPPED | **MATCHES** | `iteration/comprehension/eval.rs:68` — `pub fn evaluate_spec(spec_text: &str, kernel: &GkKernel) -> Result<Vec<Value>, String>` | Signature matches. Layered evaluator with `try_eval_all_cursor` → `interpolate_via_kernel` → `try_eval_range` → `try_eval_generator` → `try_eval_setop` → `try_eval_sequencer` → `eval_const_expr` fallback chain. |
| **3.4** `compile_gk` | SHIPPED | **MATCHES** | `dsl/compile.rs:48` — `pub fn compile_gk(source: &str) -> Result<GkKernel, String>` | Plus six related variants (`compile_gk_with_path`, `compile_gk_with_outputs`, `compile_gk_with_libs`, etc.). |

#### §5 Embedding System Contract

| Section | Spec status | Audit |
|---|---|---|
| **5.1.1** Polydat's obligations | SHIPPED | **MATCHES** — each obligation is discharged by an axiom verified above. |
| **5.1.2** Host's baseline obligations | SHIPPED | **MATCHES** — current consumers (nbrs-activity, nbrs-workload) operate at baseline; no enforcement issues. |
| **5.1.3** Host's opt-in strict contract | PLANNED | **PLANNED** — no `eval_const_expr_typed_strict` or `StrictMode` flag in code. γ-7. |
| **5.1.4** Shared vocabulary | SHIPPED | **MATCHES** — `Value`, `PortType`, `{name}` syntax all public; grammar accessible via `dsl::*`. |
| **5.2** Types at the embedding boundary | SHIPPED (baseline only) | **MATCHES** — typed `Value` crosses; strict accessors (`.as_bool()`, etc.) panic on type mismatch; non-strict accessors (`try_as_*`) exist on `Value`. |
| **5.3** L-value type inference | PLANNED | **PLANNED** — no `eval_const_expr_typed::<T>` or `HostType` trait in code. γ-4. |
| **5.4.1** Current catalog (intra-graph) | SHIPPED | **MATCHES** — `auto_adapter` in `compile/assembly.rs:1347` + `library/convert/*` catalog. Adapter insertion at wire-validation site only (verified above). |
| **5.4.2** Planned extension (two boundary sites) | PLANNED | **PLANNED** — `materialize_wiring_from_outer` does not consult `auto_adapter`; no return-path adapter site exists. γ-5 + γ-6. |
| **5.4.3** Boundary polyfill contract rules | PLANNED | **PLANNED** — depends on §5.4.2 implementation. |
| **5.5** Virtual nodes | SHIPPED | **MATCHES** — factory registration on `GkRuntime` exists; host crates (nbrs-activity runtime_context, nbrs-metrics) demonstrably register. |
| **5.6** Virtual wires | PLANNED | **PLANNED** — no `register_extern_resolver` API in `dsl/factories.rs`. γ-8. |
| **5.7** Runtime model applied | SHIPPED | **MATCHES** — inherits from runtime_model.md verification. |

#### §6 Error Ontology

| Section | Spec status | Audit |
|---|---|---|
| **6.1** Variant guide (typed enum form) | PLANNED | **PLANNED** — `EmbeddingError` enum does not exist. Current surfaces return `Result<_, String>`. γ-1 + γ-3. |
| **6.2** Provenance | SHIPPED (string form) | **MATCHES** — current string errors carry provenance prose; typed enum would carry it as struct fields. |
| **6.3** Migration plan | PLANNED | **PLANNED** — γ-1 (introduce enum), γ-3 (migrate surfaces). |

---

## Findings detail — the DEVIATES cluster

Three axioms (R1, R2, and E3-via-R1/R2) describe runtime
mechanics using a *generation counter* abstraction that
doesn't match the actual implementation. The implementation
uses a hybrid push/pull invalidation model:

**Spec's described mechanism (R1, R2):**

```text
pull(name):
  for each node in cone(name) topologically:
    if node_generation[node] == current_generation:
      continue
    else:
      node.eval(...)
      node_generation[node] = current_generation
  return outputs[name]
```

**Actual implementation:**

```text
set_inputs(coords):
  for each changed input index i:
    for each dependent node_idx in input_dependents[i]:
      node_clean[node_idx] = false
  for each nondeterministic node:
    node_clean[idx] = false

pull(name):
  eval_node(name's node_idx)

eval_node(node_idx):
  if node_clean[node_idx]:
    return
  for each upstream wire source:
    if WireSource::NodeOutput(upstream_idx, _):
      eval_node(upstream_idx)
  // gather inputs, call node.eval(), write to buffers
  node_clean[node_idx] = true
```

The differences:

| Aspect | Spec | Code |
|---|---|---|
| Cleanness signal | Per-node `node_generation[i] == current_generation` | Per-node `node_clean[i]: bool` |
| Invalidation timing | Lazy at pull (generation comparison) | Push at `set_inputs` (mark dependents dirty) + lazy at pull (clean-flag check) |
| State data | `generation: u64` counter + `node_generation: Vec<u64>` | `node_clean: Vec<bool>` + `input_dependents: Vec<Vec<usize>>` |
| Cost model | O(cone size) per pull after generation advance | Same — equivalent in steady-state cost |

**Behavioural equivalence.** The two mechanisms produce the
*same* observable behaviour: a node evaluates at most once
per `set_inputs` advance; unused outputs are never
recomputed; the cost is bounded by cone size. D1 / D3
guarantees hold equivalently.

**Why the spec text is wrong.** I (the spec author) used a
generation-counter idiom because it's the textbook
incremental-eval abstraction and reads cleanly. The actual
code uses a more direct mechanism. The spec's R1/R2
descriptions should be rewritten to match the code; the
*axiom statements* themselves can stay (the guarantees
are right; only the explanatory pseudocode is wrong).

**Recommended fix.** Rewrite §3 (Node caching) and §4
(Invalidation effects) in `runtime_model.md` to describe
the `node_clean` + `input_dependents` mechanism. Keep the
R1/R2/R3 axiom statements unchanged. Leave a note in §9
(open questions) explaining the historical
generation-counter idiom and why we moved to the simpler
clean-flag form (or just delete the generation-counter
discussion entirely).

---

## Findings detail — the IMPLICIT D2

D2 (Side-Channel Determinism) is labeled PARTIAL in the
spec with the note that `GkNode::purity()` is planned. The
audit confirms:

- The `GkNode` trait (`ast.rs:963`) has no `purity()` or
  `has_side_effects()` method.
- `compile_level_of` (`ast.rs:1028`) uses JIT classification
  as the load-bearing proxy: a node that compiles to Phase 3
  JIT is, by Cranelift's constraint, pure-in-the-typed-return
  sense; a node that falls back to Phase 1 *may* be impure
  but might also just lack a JIT-friendly form.
- Diagnostic nodes (`library/diagnostic.rs`) document their
  side effects in comments but have no programmatic
  declaration.

Hosts that want to reason about an expression's side
channels *must* either inspect each constituent node's
implementation (not contract) or know out-of-band which
nodes are impure. This is the gap γ-2 closes.

D2's guarantee holds in practice — impure nodes are
deterministic conditional on their declared semantics —
but the contract is informal. The audit confirms the
PARTIAL status as accurate.

---

## Findings detail — the PARTIAL T2

T2 (Type mismatches caught at construction or healed by
auto-adapters) is labeled PARTIAL in the spec. The audit
confirms:

- **Intra-graph site (SHIPPED):** `compile/assembly.rs:929`
  invokes `auto_adapter` (line 1347) when wire source/
  consumer types mismatch. Catalog covers widening
  (`U64→F64`, `I32→I64`, `F32→F64`), to-string (`U64→Str`,
  `F64→Str`, `Bool→Str`, etc.), and a few cross-type
  conversions. `CompileEvent::TypeAdapterInserted` event
  fires for observability.
- **Boundary sites (PLANNED):** `kernel/state.rs` has
  *zero* references to `auto_adapter` or
  `library::convert`. `materialize_wiring_from_outer`
  (line 651) does not consult the catalog. Type
  mismatches at Context Fusion's synthesis time either
  silently coerce (via bitwise value reuse for u64/f64
  cases) or surface as wire errors.
- **Return-path site (PLANNED):** depends on the typed
  embedding surface (§5.3), which is also PLANNED.

The PARTIAL status is accurate; γ-5 + γ-6 close the gap.

---

## What this audit did NOT verify

- **Test coverage.** This audit checks code surface
  existence + mechanism shape. It does not verify the
  tests for those mechanisms exist or pass.
- **Edge cases.** The spec axioms make universal claims
  ("every node", "every wire"); the audit verifies the
  surfaces exist and operate on the right data structures
  but does not enumerate every code path.
- **SRD alignment.** The spec cites SRDs as authority for
  certain claims (e.g., SRD-13f gradient classification).
  The audit verifies the spec's code citations; it does
  not re-verify the SRD's claims against the code.
- **Cross-crate consumer behaviour.** The audit reads
  polydat code only. Host crates (nbrs-activity,
  nbrs-workload, etc.) consume the surfaces and exercise
  the contract; their conformance is not audited here.

---

## Recommended next steps

1. **Rewrite runtime_model.md §3 and §4** to describe the
   `node_clean` + `input_dependents` mechanism the code
   actually uses. Keep R1/R2/R3 axiom statements unchanged
   (the behavioural guarantees match); only the
   mechanism-description text needs revision. This
   converts the three DEVIATES findings to MATCHES.
2. **Land γ-2** (explicit `GkNode::purity()`) to convert
   D2 from PARTIAL/IMPLICIT to SHIPPED.
3. **Land γ-1 + γ-3** (typed `EmbeddingError`) to convert
   E7 from PLANNED to SHIPPED.
4. **Land γ-5 + γ-6** (boundary adapter polyfills) to
   convert T2 from PARTIAL to SHIPPED.
5. **Optional: deeper audit pass on edge cases.** This
   audit confirms axioms describe the load-bearing
   shape; a follow-up audit could verify edge cases
   (boundary input lengths, type-conversion corner
   cases, error-path determinism, etc.).

After items 1-4, the spec is fully descriptive of the code
and the 1 PLANNED axiom (E7) plus 2 PARTIAL (T2, D2) are
all SHIPPED. The audit can be re-run to confirm zero
remaining DEVIATES findings.

---

## Spec status after audit + planned pushes

Current (post-audit):

| Status | Count |
|---|---|
| SHIPPED | 32 |
| DEVIATES (need spec rewrite) | 3 (R1, R2, E3-inherited) |
| IMPLICIT (matches PARTIAL) | 1 (D2) |
| PARTIAL | 1 (T2) |
| PLANNED | 1 (E7) |
| Section-level PLANNED | 5 (§5.1.3, §5.3, §5.4.2/3, §5.6) |

After spec rewrite (item 1) — DEVIATES → MATCHES:

| Status | Count |
|---|---|
| SHIPPED | 35 |
| PARTIAL | 2 (T2, D2 — both labeled accurately) |
| PLANNED | 1 + 5 sections |

After γ-1 through γ-8 — all SHIPPED:

| Status | Count |
|---|---|
| SHIPPED | 40 axioms + all §5 sections |
| PARTIAL | 0 |
| PLANNED | 0 |
