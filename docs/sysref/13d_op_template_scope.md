# 13d: Op-template GK scope layer

**Status:** normative (sketch — not yet implemented)
**Owner:** nbrs-variates (kernel/program API), nbrs-activity
  (scope-tree pre-walk, op-dispenser construction, dryrun
  diagnostics)
**Cross-refs:** SRD-13 (GK modules), SRD-13b (combination modes),
  SRD-13c (scope model — superset; this SRD extends it),
  SRD-18b (scenario tree), SRD-40b (synthetic metrics — primary
  consumer of this scope layer)

---

## What this SRD covers

This document extends [SRD-13c — GK Scope Model](13c_gk_scope_model.md)
with the op-template scope layer. It specifies:

- That an op template *is* a GK scope, distinct from its enclosing
  phase scope.
- **Scope flattening** — the optimisation by which the compiler
  detects that an op-template scope is materially equivalent to
  its parent and **flattens** the layer away, so descendants bind
  directly to the parent without an intervening kernel.
- How the flattening decision propagates through scope-tree
  pre-walk so the runtime never instantiates an unnecessary
  kernel, and how it's proven correct via a dedicated test
  suite (§4 below).
- A new `dryrun=op` diagnostic level that exercises every step
  of op-template scope construction without running cycles.

Without this SRD the existing pipeline merges every op-template's
bindings into one phase-level program. That model worked when ops
had no per-template GK content of their own; it loses to
diagnostics quality, cache granularity, and naming hygiene as soon
as they do.

---

## 1. Op templates are scopes

Per the SRD-13c "scope per non-trivial node" rule, an op template
is a scope:

```
workload kernel
   ↑ bind_outer_scope
phase kernel
   ↑ bind_outer_scope
op-template kernel        ← this layer
```

The op-template kernel's GK content is whatever the op declares:

- Its `bindings:` block.
- Wire expressions auto-injected from sugared `metrics:` forms
  (SRD-40b §2.2).
- Inline `{{<expr>}}` rewrites in op fields (current pre-compile
  pass at `crate::scope::rewrite_inline_exprs`).

The kernel auto-externs every name it doesn't declare locally;
those externs resolve up through the phase kernel and beyond per
the standard SRD-13c chain.

---

## 2. Two-step compile: validate at workload-init, instantiate at premap

This section captures the high-level "validate early,
instantiate late" rule. The full staged pipeline — every cache
boundary, every short-circuit — is in §4.

### 2.1 Validation: every kernel compiles at workload load

At workload-init time, the compiler walks every op template,
collects its GK matter, and **compiles the kernel program** to
prove the GK source is valid. This is type-checking, not
instancing — no `GkState`, no per-cycle execution, no scope
binding. The output of this pass is the program AST + program
hash for each scope node.

Failures here are workload errors (bad GK source, unresolved
names, type mismatches) and surface before any cycle runs.

### 2.2 Instancing: at premap time, scope-by-scope

Per-iteration kernel instances continue to be created at premap
time (SRD-18b §"M3 — per-scope kernel composition"), descending
the scenario tree as the runner enumerates concrete iterations.
Premap **must descend to op-template level** for this SRD to be
consistent — the existing premap depth (which stops at phase) is
extended one tier so op-template kernels are part of the same
walk.

Each instance comes from the op template's compiled program via
`GkKernel::from_program(program.clone())` plus
`bind_outer_scope(parent_kernel)`, identical machinery to every
other scope layer — no new mechanism. The instance cache (SRD-13c
§"Per-Scope Canonical Kernel Cache") is consulted before
instancing — see §4.6 for the full sequence.

### 2.3 `dryrun=op` diagnostic level

Existing depths are `phase`, `cycle`, `full` (SRD-04 / runner
`DiagnosticConfig`). This SRD adds `op` between `phase` and
`cycle`:

| `dryrun=` | Stops after                                                |
|-----------|------------------------------------------------------------|
| `phase`   | Scenario tree walk; phase-level kernels instanced; no ops. |
| `op`      | **NEW.** Phase walk + op-template kernels instanced;      |
|           | adapter `map_op` called; metric instruments registered    |
|           | (duplicate-family collisions from                         |
|           | `Component::register_instrument` surface here); no cycles. |
| `cycle`   | One cycle per op runs through the silent adapter.          |
| `full`    | Standard run.                                              |

`op` is the right level for SRD-40b validation — it exercises the
op-template kernel and family-registration paths without paying
adapter execution cost.

---

## 3. Scope flattening

Compiling a per-template kernel for every op is wasteful when the
template adds nothing to its parent's GK content. The compiler
detects this case and **flattens** the per-template scope into
its parent — descendants `bind_outer_scope` directly to the
phase kernel, the op-template kernel layer simply doesn't exist
at runtime.

"Flattening" rather than "eliding" or "skipping" because the
operation is structural: two adjacent scope tiers collapse into
one when their content is materially identical. It's a property
of the scope tree, computed once at workload load, applied
uniformly thereafter.

The flattening decision proceeds in two stages: the **trait
classification** (§3.1) is the cheap, declarative path that
answers most cases; the **program-hash check** (§3.2) is the
refinement that catches definitions which happen to be identical
to a parent's. The pre-walk (§3.3) wires them together.

### 3.1 The `HasGkMatter` trait

Every node in the construction tree (components, phases, op
templates, scenario-tree nodes) implements a single trait
declaring its GK usage:

```rust
pub trait HasGkMatter {
    /// Classify this node's contribution to GK content. The
    /// scope walker uses this to decide whether the node needs
    /// its own kernel context or can reference the parent's.
    fn gk_matter(&self) -> GkMatter;
}

pub enum GkMatter {
    /// No GK references at all. Trivial node — no `bindings:`,
    /// no `metrics:`, no inline `{{<expr>}}`, no GK-typed
    /// fields. Walker skips kernel construction entirely.
    None,

    /// References parent-scope GK names but **defines nothing
    /// new**. Examples:
    /// - `metrics:` declarations whose `value:` is a bare name
    ///   that resolves to a parent binding.
    /// - Inline `{{<name>}}` substitution where `<name>` is a
    ///   parent binding.
    /// - Op fields that bind to parent-scope wires without
    ///   declaring new ones.
    /// Walker skips kernel construction; reads thread through
    /// the parent's kernel state directly.
    Readonly,

    /// Declares new bindings, wire expressions, or constants
    /// that the parent doesn't supply. Walker materialises a
    /// kernel for this node — possibly subject to hash-check
    /// flattening (§3.2) if the new content turns out to be
    /// equivalent to the parent's.
    Definitions,
}
```

The trait is implemented by **workload-model AST types** —
the static description of the workload as parsed from YAML.
Runtime objects (the `Component` tree, fibers, dispensers)
*consume* the marks the trait produced; they don't implement
it themselves. GK content lives on the AST, not on runtime
state.

| Type                            | `gk_matter()` derives from                                                        |
|---------------------------------|------------------------------------------------------------------------------------|
| `Workload` (root)               | Top-level `bindings:` block                                                        |
| `WorkloadPhase`                 | Phase-level `bindings:`; `for_each:` clauses                                       |
| `ParsedOp`                      | Op-level `bindings:`; `metrics:` (sugared forms in SRD-40b §2.2); inline `{{<expr>}}` |
| `ScenarioNode` (for_each / do)  | Iteration clauses (always `Definitions` when present — they bind iteration vars)   |

The classification is computed at workload-parse time from the
node's AST, before any kernel compilation. It is the **first
question** the scope-tree pre-walker asks at every node:

- `None` → no kernel; descendants bind to parent directly.
- `Readonly` → no kernel; descendants bind to parent directly.
  (Distinguished from `None` only for diagnostics — the
  workload-validation surface can show "node X reads but
  defines nothing.")
- `Definitions` → candidate for materialisation; proceed to the
  hash check (§3.2).

This is the cheap path: 90%+ of op templates classify as `None`
or `Readonly` and never reach the compiler. The hash check is
reserved for nodes that genuinely declare new GK content.

### 3.2 The hash check (refinement for `Definitions` nodes)

When a node classifies as `Definitions`, it might still be
flattenable if its content happens to be identical to the
parent's. The hash check answers that question without forcing
materialisation.

GK programs already carry a `program_hash` (SRD-13c §"Per-Scope
Canonical Kernel Cache"). The hash is over the program's AST +
constants — no instance state required. Two scopes with identical
`program_hash` are functionally equivalent at compile time; their
runtime instances would differ only by parent-bound values, which
is exactly what `bind_outer_scope` already handles.

A new helper on the GK API exposes this without forcing
materialisation:

```rust
impl GkProgram {
    /// Hash-compare two programs for AST/constant equivalence.
    /// Cheap; doesn't allocate state.
    pub fn is_equivalent_to(&self, other: &GkProgram) -> bool;

    /// Hash-compare a program against its potential parent.
    /// True when the inner program adds no bindings, no
    /// constants, and no graph nodes the parent doesn't already
    /// supply. This is the "can flatten?" predicate for nodes
    /// that classified as `GkMatter::Definitions`.
    pub fn is_subset_of(&self, parent: &GkProgram) -> bool;
}
```

(Exact API TBD during implementation; the contract is "does this
scope add anything new?" answered without instancing.)

### 3.3 Pre-walk: trait first, hash second

The scope-tree pre-walk visits every node bottom-up. At each
node:

1. Ask `node.gk_matter()`.
2. **If `None` or `Readonly`** — mark as **flattened**. No
   kernel will be compiled for this scope. Done; no further
   work.
3. **If `Definitions`** — compile the candidate kernel program
   (validation pass, §2.1).
   1. Check `op_program.is_subset_of(parent_program)`.
   2. If yes, mark as **flattened** (rare — the node's
      definitions duplicated the parent's; usually a refactor
      tell, but not an error).
   3. If no, mark as **materialised**. The dispenser owns its
      own kernel, instanced at premap (§2.2).

The mark is a property of the scope-tree node, set once per
workload load. Premap and runtime read the mark; neither
recomputes it.

The trait + hash-check separation means most workloads pay
**zero compile cost** for op-template scope construction:
trivial ops short-circuit at the trait check; only ops with
real GK content compile a candidate program.

The mechanism generalises beyond op templates: any scope-tree
node whose `gk_matter()` is `None`/`Readonly` (or whose
`Definitions` content collapses by hash) can be flattened by the
same pre-walk. SRD-40b's op-template surface is the first
concrete consumer; future for_each / do-loop optimisations may
piggyback on the same mechanism.

### 3.4 Determinism

The flattening decision is **deterministic over the workload
AST**. A workload that compiles with op X flattened will compile
that way on every run; adding a single binding to op X may flip
its `gk_matter()` from `Readonly` to `Definitions` (and the bit
to materialised), but the result is stable for any fixed
workload text. Diagnostics (`dryrun=op` with verbose) print the
per-node `gk_matter()` value and the resulting flatten/
materialise mark.

---

## 4. Realisation lifecycle phases

Scope flattening, the canonical-kernel cache, and the
materialise-or-reuse decision all depend on a staged
compilation pipeline. This section names the stages
explicitly so each subsystem (parser, GK compiler, scope-tree
walker, premap, runtime) has a clear input/output contract
with the next.

The principle: **don't instance a kernel until you've ruled
out every cheaper alternative** (cached reuse, scope flatten,
trait classification). Each stage filters; only nodes that
survive all filters reach the materialisation step.

### 4.1 Stage A — Source → AST

**Input:** YAML / inline GK source text.
**Output:** AST with location metadata for diagnostics.
**Cost:** Single parse pass per source string; trivial.
**Failure mode:** YAML syntax error, GK parse error.
**Cache:** None (source is the cache key for everything
downstream; you can't cache before parsing).

The AST is the canonical structural representation. Every
later stage consumes the AST or a derivative. Whitespace,
comments, and identifier order may or may not be normalised
here — see §6.1 open question.

### 4.2 Stage B — AST + param context → resolvable AST

**Input:** AST + workload param map + parent-scope binding
catalogue.
**Output:** AST with `{name}` placeholders rewritten,
inline `{{<expr>}}` constructs hoisted to `__expr_N := …`
bindings (current `crate::scope::rewrite_inline_exprs`),
extern slots identified.
**Cost:** Linear in AST size.
**Failure mode:** unresolved `{name}` placeholder, malformed
inline expression.
**Cache:** Could cache `(ast_hash, param_set_hash) →
resolvable_ast` but not currently done; the resolvable AST is
small and the rewrites are cheap, so re-running them per
workload load is acceptable.

The output of this stage is what the trait classification
(§3.1) inspects. `gk_matter()` reads the resolvable AST, not
the raw source — placeholder rewrites can promote a `None`
node to `Definitions` (e.g. an inline `{{<expr>}}` becomes a
`__expr_N := …` binding).

### 4.3 Stage C — Trait classification

**Input:** Resolvable AST node.
**Output:** `GkMatter::{None, Readonly, Definitions}`.
**Cost:** O(1) — declarative property derived from the
node's structure.
**Failure mode:** None (always returns a value).
**Cache:** The classification is a pure function of the AST;
cache by AST identity if the workload-load path benefits.

This is the **first filter**. `None` and `Readonly` short-
circuit the entire downstream pipeline — no compilation, no
hash check, no instancing. Most op templates classify here.

### 4.4 Stage D — Compile to `GkProgram`

**Input:** Resolvable AST + parent program (for auto-extern
slot resolution).
**Output:** `GkProgram` (compiled, immutable, carries
`program_hash`).
**Cost:** Moderate. Type-checking, name resolution against
the parent chain, auto-extern wiring.
**Failure mode:** Workload error (unresolved name, type
mismatch, output-modifier conflict).
**Cache:** **Yes — keyed on `(ast_hash, parent_ast_hash)`**.
Two scopes whose AST and parent's AST hash identically
produce the same `GkProgram`; the compile work runs once per
unique pair.

This stage runs only for nodes that classified as
`Definitions` in §4.3. The cache means a workload with 50
ops sharing the same `bindings:` block compiles **one**
program, not 50.

### 4.5 Stage E — Hash check (subset / equivalence)

**Input:** `GkProgram` + parent `GkProgram`.
**Output:** "flatten" / "materialise" decision.
**Cost:** Hash compare (cheap) plus, if hashes differ, an
optional `is_subset_of` walk (still cheap — it's a structural
comparison, not state instancing).
**Failure mode:** None.
**Cache:** The hash check itself is the cache key; results
are persistent for the workload-load lifetime.

When the program is a subset of its parent (the §3.2
predicate), the scope flattens: no kernel will be instanced
for this node. The decision is recorded on the scope-tree
node (§3.3 mark) and frozen for the workload load.

### 4.6 Stage F — Instance materialisation

**Input:** `GkProgram` + parent `GkKernel` instance + bound
parent values.
**Output:** `GkKernel` (`from_program` + `bind_outer_scope`
+ `set_input`).
**Cost:** Small per instance — clones the program reference,
copies parent values into extern slots.
**Failure mode:** Bind-time error (extern slot not supplied
by parent).
**Cache:** Per-iteration. The
canonical-kernel cache (SRD-13c §"Per-Scope Canonical Kernel
Cache") keyed on `(program_hash, parent_instance_hash)`
returns an existing kernel when both match. Iterations whose
parent-bound values match a prior iteration share the kernel.

This is where actual kernel objects come into existence. By
the time we reach this stage, the trait check, compile-cache
hit, and hash flatten check have all said "yes, you really
do need a new instance here."

### 4.7 Stage G — Per-cycle execution

**Input:** `GkKernel` instance + cycle coordinates.
**Output:** Bound wire values.
**Cost:** Hot path — runs every cycle.
**Failure mode:** Per-cycle GK error (typically caught by
strict-mode validation).
**Cache:** None at this layer; the hot path is the kernel's
own per-fiber state.

### 4.8 Stage summary

```
   Stage A: Source → AST
       |
   Stage B: AST + params → resolvable AST
       |
   Stage C: gk_matter()? ──┬── None / Readonly → flatten, done
       |                   └── Definitions
       v
   Stage D: compile → GkProgram   [cached by (ast_hash, parent_ast_hash)]
       |
   Stage E: is_subset_of(parent)? ──┬── yes → flatten, done
       |                            └── no
       v
   Stage F: instance               [cached by (program_hash, parent_instance_hash)]
       |
   Stage G: per-cycle execution
```

Two cache layers (compile cache, instance cache) and three
short-circuits (`None`, `Readonly`, hash subset). The
per-cycle layer never sees these decisions — by the time
execution starts, every flatten/materialise call has been
made and recorded.

### 4.9 Diagnostic surface

`dryrun=op` (§2.3) walks stages A–F for every op template
and prints, per node:

- The `gk_matter()` classification.
- The compile-cache hit/miss and the program_hash.
- The hash-check decision (when applicable).
- The materialise-or-flatten mark.
- The instance-cache hit/miss at premap.

This makes the staged pipeline auditable per op without
running cycles. Test-suite assertions (§5) hook the same
diagnostic stream.

---

## 5. Walking parent-kernel reference and logical kernel names

When a scope is flattened, descendants that need a parent
kernel (for `bind_outer_scope`, for hash-keyed cache lookups,
for diagnostics) can't just point at "my immediate parent" —
the immediate parent didn't materialise. They need a reference
that walks past flattened tiers to the **last materialised
ancestor**. The GK subcontextual API exposes this directly so
no caller has to walk the scope tree by hand.

### 5.1 The walking reference

A new accessor on the scope-tree node:

```rust
impl ScopeNode {
    /// The nearest ancestor (or self) whose `materialised` mark
    /// is true. For materialised nodes, returns self. For
    /// flattened nodes, walks parent links until a materialised
    /// node is reached. Workload-root is always materialised
    /// (it's the workload-params kernel — never flattened) so
    /// this terminates.
    pub fn nearest_materialised(&self) -> &ScopeNode;
}
```

Every consumer of "give me the parent kernel for binding /
caching / diagnostics" routes through this accessor. It's the
single point that knows about flattening; nothing else does.

### 5.2 What this fixes

- **Cache invariants.** The canonical-kernel cache key
  `(program_hash, parent_instance_hash)` (SRD-13c §"Per-Scope
  Canonical Kernel Cache") now uses the *materialised* parent's
  instance hash, not the flattened immediate parent's
  (which has no instance). Cache lookups stay coherent under
  flattening — no special case at every call site.
- **Bind-outer-scope.** Materialising a kernel calls
  `bind_outer_scope(node.nearest_materialised().kernel())`
  uniformly. Whether the immediate parent flattened or not is
  invisible to the call site.
- **Diagnostics.** When `dryrun=op` reports an op as
  materialised, it can name the *actual* outer scope it bound
  to — not "phase X" if the phase flattened, but "workload" or
  whatever the closest materialised ancestor is. Walking is
  cheap and produces a structurally honest diagnostic.

### 5.3 Logical kernel names

For diagnostics to be readable, every materialised kernel
carries a **logical name** assigned at scope-tree
construction. Names follow a stable convention:

| Scope                          | Logical name                                  |
|--------------------------------|-----------------------------------------------|
| Workload-params kernel         | `workload`                                    |
| Top-level workload bindings    | `workload.bindings`                           |
| Phase                          | `phase.<phase_name>`                          |
| `for_each` / `do` scope        | `phase.<phase_name>.for_each.<var>` etc.     |
| Op template                    | `phase.<phase_name>.op.<op_name>`             |

Flattened scopes don't get their own name in the runtime — they
*inherit* the nearest-materialised ancestor's name in
diagnostics, since they don't exist as kernels. `dryrun=op`
output reads:

```
op pvs_query.predict   gk_matter=Definitions  flatten=false
   logical_name=phase.pvs_query.op.predict
   binds_outer=phase.pvs_query
   program_hash=…  cache=miss
   instance=new

op pvs_query.bare      gk_matter=None         flatten=true
   logical_name=phase.pvs_query        ← inherited; no own kernel
```

Logical names are also the right surface for `nbrs describe
gk` (resolves Q2 in §7) — when the user inspects the workload's
GK structure, every kernel has a stable, human-readable name
independent of compile-cache hits or flattening decisions.

### 5.4 Pre-walk integration

The pre-walk (§3.3) sets the materialise/flatten mark and the
logical name on every scope-tree node. By the time premap or
runtime walks the tree, both are queryable in O(1). The
walking reference (§5.1) is a thin accessor over those marks.

---

## 6. Proving-out test suite

Scope flattening is an optimisation that changes runtime
structure (kernel instance count, parent-binding chains) without
changing observable behaviour. That delta has to be tested
explicitly — without proofs, "flattened == materialised in every
observable way" is a hope, not an invariant.

The test suite lives in `nbrs-variates/tests/scope_flattening.rs`
and / or `nbrs-activity/tests/scope_flattening.rs` and proves:

### 6.1 Equivalence under flattening

For each fixture workload, run twice:

1. **Baseline:** force-materialise every op-template scope
   (flatten disabled; one kernel per op).
2. **Flattened:** standard pre-walk (flatten where eligible).

Assert that for both runs:

- Every metric `metric_instance` row is bit-identical (same
  family, same labels, same recorded values across all sample
  rows).
- Every captured GK output (per cycle, per op) compares equal.
- Side-effect logs (op execution order, throttle delays,
  emit-dispenser output) are identical.

If the two runs disagree on any of these, flattening has
introduced a semantic delta — fail the test loudly with the
first divergence.

### 6.2 Flatten-eligibility cases (positive)

Workloads where flattening must succeed (and be observable in
diagnostics):

- Op template with no `bindings:`, no `metrics:`, no inline
  `{{...}}`. (The trivial case.)
- Op template whose only `metrics:` declarations use bare-name
  `value:` references that resolve to phase bindings. (No new
  GK content; flatten safe.)
- Op template whose `bindings:` block is byte-identical to the
  phase's `bindings:` block. (Exact program-hash equality.)

### 6.3 Materialise-required cases (negative)

Workloads where flattening must NOT happen:

- Op template with a non-empty `bindings:` declaring new names.
- Op template with a `metrics:` wire-expression list entry
  (SRD-40b §2.2).
- Op template with an inline `{{<expr>}}` rewrite.

For each, assert that `dryrun=op` reports the op as
materialised, not flattened. A flattened op here would be a
correctness bug.

### 6.4 Hash-API contract tests

Direct unit tests on `GkProgram::is_equivalent_to` and
`is_subset_of`:

- Two programs from identical sources: `is_equivalent_to == true`.
- Programs differing only in identifier order in the source
  (whitespace/comments): `is_equivalent_to == true`.
  (Open question §5.1 — depends on whether normalisation runs
  before hashing.)
- Inner program adds one binding: `is_subset_of(parent) == false`.
- Inner program is empty / parent-equivalent:
  `is_subset_of(parent) == true`.

### 6.5 Performance smoke test

Compare the canonical-kernel cache hit rate and total compile
time between baseline and flattened modes for a workload with
many trivial op templates (e.g. 50+ ops with no GK matter). Flat
mode should reduce kernel instances by approximately the
trivial-op count and reduce compile time proportionally. Not
strict thresholds; the test is a regression guard against future
changes that accidentally disable flattening.

---

## 7. Structural rules carried forward from SRD-13c

The following rules from SRD-13c apply unchanged at the
op-template layer:

- The op-template kernel auto-externs names it doesn't define;
  resolution proceeds up the parent chain (SRD-13c §"Auto-extern").
- `final` / `shared` modifiers on op-level bindings interact with
  outer scopes per SRD-13c §"Output Modifiers".
- The canonical-kernel cache keyed on
  `(program_hash, parent_instance_hash)` works at this layer
  too — multiple op templates with the same GK content share a
  canonical kernel; per-instance state diverges only on bound
  values.

This SRD adds the scope tier and the elision pre-walk; everything
else flows from SRD-13c.

---

## 8. Resolutions and remaining open questions

### 8.1 Resolved

1. **Subcontext symbol redefinition — verboten by default.** A
   child scope MAY NOT redefine a symbol declared in any
   ancestor scope. The pre-walk's `is_subset_of` check
   (§3.2) treats redefinition of a parent symbol as a workload
   error, not as fertile ground for materialisation. If a future
   workload genuinely needs to shadow, it will require an
   explicit safety switch — out of scope for this SRD; raise it
   when first needed. §6.4's hash-API contract tests pin the
   refusal at the API level.

2. **`nbrs describe gk` flatten/materialise display.** Yes —
   the diagnostic surface in `nbrs describe gk` (when details
   are turned on) shows, for every scope-tree node:
   - The materialise/flatten bit.
   - The logical kernel name (§5.3).
   - The walking-parent reference (§5.1) when flattened.

   Implementation lands alongside the `dryrun=op` diagnostics
   work (§9 phase 5).

3. **Cache invariants under flattening.** Resolved by §5.1's
   walking parent-kernel reference. The canonical-kernel cache
   key uses the *materialised* parent's instance hash (via
   `nearest_materialised()`), not the flattened immediate
   parent's. No special-case logic at any call site; the
   walking accessor encapsulates the rule. §6.1's equivalence-
   under-flattening test still guards against regressions.

### 8.2 Decided defaults

The two items below were tracked as open questions in earlier
drafts. They're recorded here as the working defaults — the
SRD freezes them as decisions, not pending.

4. **Hash normalisation: verbatim, no normalisation.** AST
   hashes are computed structurally over the parsed nodes
   without normalising whitespace, comment positions, or
   identifier ordering. Two programs whose source differs
   only in formatting hash to **different** values and produce
   different scope-tree marks. The decision optimises for
   compiler simplicity and predictability over flattening
   coverage; the cost is that a hand-edited workload that
   reformats one op without changing semantics may flip its
   flatten/materialise mark.

   **Revisit only if** benchmarking shows a workload-class
   where reformatting churn dominates compile cost; current
   flattening already short-circuits the trivial case via the
   `HasGkMatter::None`/`Readonly` trait check (§3.1), so the
   verbatim hash mostly matters for the `Definitions` minority
   path.

5. **Logical-name format: full path, no truncation.** Logical
   kernel names (§5.3) are the fully-qualified scope-tree
   path: `phase.<phase_name>.for_each.<var>.op.<op_name>`.
   Deep workloads produce long names; that's accepted —
   diagnostics print the full string. There is no truncation
   rule, no automatic shortening, no implicit ellipsis.

   **Revisit only if** a real workload's diagnostic output
   becomes unreadable. The mitigation in that case will be a
   render-time truncation knob in the diagnostic surface
   (e.g. `dryrun=op` width parameter), not a change to the
   logical-name format itself — names remain stable across
   tools, only the rendering varies.

---

## 9. Implementation phases

| Phase | What                                                                          | Where                                      |
|-------|-------------------------------------------------------------------------------|--------------------------------------------|
| 1     | `HasGkMatter` trait + impls on `WorkloadPhase`, `ParsedOp`, `ScenarioNode`    | `nbrs-workload/src/model.rs`               |
| 2     | `GkProgram::is_equivalent_to` / `is_subset_of`; redefinition-forbidden check  | `nbrs-variates/src/kernel.rs` (or sibling) |
| 3     | Workload-init validation walk over op templates (Stage A–D, §4)               | `nbrs-activity/src/runner.rs`              |
| 4     | Scope-tree node carries `materialised: bool` + `logical_name`; pre-walk sets  | `nbrs-activity/src/scope_tree.rs`          |
| 5     | `nearest_materialised()` walking accessor (§5.1)                              | `nbrs-activity/src/scope_tree.rs`          |
| 6     | Premap descends to op level when `materialised`                               | `nbrs-activity/src/scope_tree.rs`          |
| 7     | `dryrun=op` depth + per-stage diagnostics (§4.9, §5.3)                        | `nbrs-activity/src/runner.rs`              |
| 8     | `nbrs describe gk` flatten/materialise/logical-name display                   | `nbrs/src/describe.rs`                     |
| 9     | Op-dispenser holds (or doesn't hold) its own kernel handle                    | `nbrs-activity/src/activity.rs`            |

Phases 1–2 are independently testable in isolation. Phases 3–5
build the scope-tree marks; phases 6–9 are the runtime
consumers. Once this SRD lands, SRD-40b's §3 becomes a thin
reference to it — the metrics mechanism just consumes the
op-template scope layer.

### Phase 9 status (2026-05)

Phases 1–9 are landed. The runner's install loop synthesizes
per-op-template kernels for materialised scopes (via
`build_op_template_scope_kernel` in `nbrs-activity/src/scope.rs`)
and installs them on `cached_kernel` slots. `OpBuilder` carries
per-op-template programs, `FiberBuilder` instances one
`GkKernel` per template at fiber creation via the canonical
`from_program` + `bind_outer_scope` recipe (SRD-13c §"Per-Scope
Canonical Kernel Cache"), and `resolve_pulls_for_op` routes
wrapper-side reads to the right state. `MetricsDispenser`
resolves through the GK pull plan against the op-template
kernel's program.

`MetricsDispenser` still requires `value:` to be a bare
binding name. Non-bare expressions (`value: mul(load, 2)`)
error at wrap-time. The `GkProgram::compile_expr` follow-up
that lifts that restriction, plus the cross-scope per-cycle
value contract and value-correctness tests, are tracked in
`docs/design/srd13d_phase9_followups.md`.
