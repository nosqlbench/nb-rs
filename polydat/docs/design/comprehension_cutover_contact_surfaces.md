# Comprehension Cutover — Contact Surfaces

**Status:** Shipped. The cutover this doc plans completed across
pushes PR 9c-1a through 9c-5; algebra is the canonical
comprehension layer at `polydat::comprehension::*`. The
per-surface determinations below are kept for historical
reference; the "Shipped state" section at the end summarises
the actual outcome at each surface.

**Companion to:** [comprehension_forms.md](comprehension_forms.md),
[comprehension_implementation_plan.md](comprehension_implementation_plan.md),
[comprehension_migration_gate.md](comprehension_migration_gate.md)

---

## Shipped state (post-cutover, end of PR 9c-5)

The algebra layer (formerly `polydat::comprehension::algebra::*`)
is now the canonical comprehension layer at
`polydat::comprehension::*`. The legacy flat-struct AST
modules (`ast_legacy`, `parse`, `eval`, `spec/legacy_convert`)
remain as **parse-pipeline implementation details** — they
take YAML text through the existing grammar parser, then
[`spec::ComprehensionSpec::into_algebra`] converts to the
canonical algebra AST via [`spec::legacy_to_algebra`]. The
legacy types are not exposed at the top-level
`polydat::comprehension::*` namespace; the algebra types are.

Per-surface outcome:

| # | Surface | Shipped state |
|---|---------|---------------|
| 1 | Text → AST parsing | `ComprehensionSpec` / `parse_text` is the public surface (PR 9c-1a). Legacy `parse` module retained internally; output converted to algebra via `legacy_to_algebra`. |
| 2 | AST data types | Workload model and scope tree hold `algebra::Comprehension` (now `polydat::comprehension::Comprehension`) — PR 9c-2. |
| 3 | Polydat source synthesis | Shared cascade walker in `nbrs-activity/src/scope_synth/` drives the polydat `SubcontextBuilder`. Three of four sister scope builders (phase / do_loop / for_each) refactored onto the shared walker; op_template kept its narrow-cascade policy. `polydat::comprehension::synthesis` dissolved — PR 9c-1b. |
| 4 | Iteration driver | Executor calls `polydat::comprehension::runtime::evaluate_for_iteration` directly. Algebra IR's static interpreter (`ir/interpreter.rs`) serves the §9.5 consumption surfaces; the runtime evaluator handles dependent-product semantics for executor use. `iteration` module deleted — PR 9c-4. |
| 5 | String interpolation | Relocated to `polydat::kernel::interp` — PR 9c-3. |
| 6 | Order application | Algebra strategies own ordering. `order` module deleted — PR 9c-4b. |
| 7 | Polydat literal formatters | `nbrs-activity/src/scope_synth/helpers.rs` — PR 9c-1b. |

Plus a spec amendment ([comprehension_forms.md](comprehension_forms.md)
§3.2): Cartesian was reformulated as dependent product (Σ),
with classical independent cross-product (Π) as the
degenerate case. V4 carries a new constraint that non-`Lex`
strategies require independent Cartesian. This let the flat
`for_each: [...]` shape with implicit cross-clause dependencies
("Form B") fit the algebra natively.

Final test count at completion: 93 baseline regression tests +
2727 unit tests across polydat / activity / workload, zero
failures at every intermediate push.

---

## Why this doc existed

PR 9a and PR 9b landed the **algebra layer**: the operator-tree AST,
strategies, optimizer, IR, consumption surfaces, the `PolydatKernelScope`
adapter, and the `ComprehensionSpec` friendly surface. The algebra
layer is feature-complete as a *data model* and *streaming consumer*.

PR 9c was originally scoped as "atomic cutover: switch executor +
parser to algebra path, delete legacy `polydat::comprehension::{ast,
parse, eval, synthesis, order, iteration}`, promote `algebra::*` to
`comprehension::*`." A pre-cutover audit revealed that scoping was
too compact:

- ~5,000 lines live in the legacy modules.
- ~48 external call sites across 7 files in `nbrs-activity` and
  `nbrs-workload`.
- The legacy modules carry responsibilities the algebra layer does
  **not** yet replicate (notably **Polydat source synthesis for child
  kernels** — 2,288 lines in `synthesis.rs`).

A monolithic delete is not viable. Before we can stage the cutover
into incremental pushes, we need a per-surface decision on where each
responsibility lives **after** the dust settles. This document
enumerates the contact surfaces, makes that determination for each,
and derives the push sequence from the contracts.

The audience is reviewers of the cutover plan: someone needs to
agree (or push back) on each API-line determination before
implementation begins.

---

## Surface inventory (overview)

| # | Surface | Current owner | Post-cutover owner | Public form |
|---|---------|---------------|--------------------|-------------|
| 1 | Text → AST parsing | polydat (legacy) | polydat | `ComprehensionSpec` + `parse_text` |
| 2 | AST data types | polydat (legacy) | polydat | `algebra::Comprehension` (operator-tree) |
| 3 | Polydat source synthesis for child kernels | polydat (legacy) | **shared** | polydat: `SubcontextBuilder`; activity: scope-walker |
| 4 | Iteration driver | polydat (legacy) | polydat | `algebra::surfaces::ScopedKernelStream<PolydatKernelScope>` |
| 5 | String interpolation against a kernel | polydat (legacy) | polydat (relocated) | `polydat::kernel::interp::*` |
| 6 | Order application | polydat (internal only) | — (deleted) | (absorbed into algebra strategies) |
| 7 | Polydat literal formatters | polydat (legacy) | **nbrs-activity** | walker-side helpers |

Surface **#3** is the shared-responsibility surface: polydat owns the
builder, activity owns the walker that drives it (see Surface 3 below
for the new-scope-walk pattern). Surface **#7** moves to activity-
side helpers because the formatters are walker concerns. Everything
else either stays in polydat (possibly relocated) or is internal-only
and deletes.

---

## Surface 1 — Text → AST parsing

### What it does

Turns the textual comprehension grammar — `var in expr`, `where`,
`order` — into an AST. Three entry points get external use:

- `parse_clause` / `parse_clause_list` — one or more clauses, comma-
  separated with paren-depth-aware splitting.
- `parse_order_spec` — the `order` clause's RHS (`lex`, `halton/50`,
  `shells(origin=center, depth=3)`, etc.).
- `parse_comprehension_text` — full `for ... where ... order ...`
  in one string.

`comprehension_from_subspaces` is the structural-detection rule
(repeated names ⇒ Union; otherwise Cartesian).

### Where it lives now

`polydat::comprehension::parse` (1,213 lines). Re-exported at
`polydat::comprehension::{parse_*, comprehension_from_subspaces}`.

### Callers

- `nbrs-workload/src/parse.rs` — the YAML loader. Detects YAML shape
  (string vs list vs list-of-lists) itself, then calls
  `parse_clause_list` on the entries and `comprehension_from_subspaces`
  to fold them.
- `nbrs-activity/src/runner.rs` — a one-off call to
  `parse_comprehension_text` for an inline-text form.
- `nbrs-activity/src/scope_tree.rs` (tests only) — `parse_clause_list`
  for test setup.

### Recommendation: **polydat owns; surface is `ComprehensionSpec` + `parse_text`**

The legacy text parsers stop being public API after the cutover.
External consumers reach the parser through either:

- `ComprehensionSpec` (serde-deserializable) — for callers that
  already have YAML- or JSON-shaped input. nbrs-workload's YAML loader
  is the primary consumer.
- `parse_text(&str)` — for callers that have a text block (REPL,
  inline-text shapes in scenario files). nbrs-activity's runner is
  the primary consumer.

The structural-detection rule (`comprehension_from_subspaces`)
stays internal — it's an implementation detail of
`ComprehensionSpec::into_algebra`. The shape-detection logic in
nbrs-workload (YAML `string vs list vs list-of-lists`) **stays in
nbrs-workload**: that's a YAML-grammar concern, not a comprehension-
grammar concern. The hand-off is "nbrs-workload classifies the YAML
shape, builds a `ComprehensionSpec`, calls `.into_algebra()`."

This was already largely landed in PR 9b. The cutover work here is
**removing** the external use of `parse_clause_list` /
`parse_comprehension_text` / `comprehension_from_subspaces` from
nbrs-workload and nbrs-activity by re-routing through
`ComprehensionSpec`.

### Risk

Low. The replacement is in place; the work is rerouting ~7 call sites
through the spec surface and confirming no semantic drift (the
parity smoke test in `polydat/tests/spec_surface_parity.rs` is the
gate).

---

## Surface 2 — AST data types

### What it does

The data structures that represent a comprehension. Eight legacy
types are publicly used:

- `Comprehension` (flat struct: `mode`, `filter`, `order`)
- `ComprehensionMode::{Cartesian, Union}`
- `Clause` (with `vars: Vec<String>` and `source: ClauseSource`)
- `ClauseSource::{Single(String), Parallel { mode, exprs }}`
- `Subspace` (a `Vec<Clause>` newtype)
- `TraversalOrder` (10 variants, identical-in-spirit to algebra's
  `StrategyName`)
- `ZipMode::{Strict, Truncate, Cycle}`
- `ShellOrigin::{Outer, Center, Corner}`

### Where it lives now

`polydat::comprehension::ast` (889 lines). Re-exported at the module
root.

### Callers

This is the most-distributed surface. ~25 call sites across:

- `nbrs-workload/src/model.rs` — the workload model holds
  `Comprehension` as a field.
- `nbrs-activity/src/scope_tree.rs` — match arms over
  `ComprehensionMode`, constructs `Comprehension::cartesian(...)` in
  tests, reads `Clause` vars.
- `nbrs-activity/src/scope.rs` — reads clause vars for binding origin
  determination.
- `nbrs-activity/src/scope_flattening.rs` — constructs empty
  `Comprehension::cartesian(vec![])` for flattening edge cases.
- `nbrs-activity/src/executor.rs` — receives `&[Clause]`, holds
  `IterationStep` referring to clauses.
- `nbrs-activity/src/runner.rs` — match arms over
  `ComprehensionMode` and `ClauseSource`.

### Recommendation: **polydat owns; surface is `algebra::Comprehension`**

The flat-struct legacy AST retires. `algebra::Comprehension`
(operator tree: `Clause | Cartesian | Zip | Union | Filter | Order`)
becomes the canonical wire type across the boundary. The workload
model holds `algebra::Comprehension`; every inspection site shifts
to operator-tree pattern-matching.

This is the **load-bearing conceptual move** of the cutover.
Everything else is mechanical; this one rewires the mental model.
Reviewers should focus their attention here.

#### Notes on the move

The operator-tree shape carries strictly more information than the
flat struct (filter and order are first-class nodes; zip is its own
constructor instead of an inline `ClauseSource::Parallel`). So legacy
→ algebra is total (already implemented in
`spec::legacy_convert::legacy_to_algebra`). The reverse direction is
not needed.

Match-arm migration patterns:

| Legacy shape | Algebra shape |
|---|---|
| `ComprehensionMode::Cartesian(clauses)` | `Comprehension::Cartesian { children }` |
| `ComprehensionMode::Union(subspaces)` | `Comprehension::Union { children }` |
| `Clause::Single(expr)` source | `Comprehension::Clause { name, source }` (leaf) |
| `Clause::Parallel { mode, exprs }` source | `Comprehension::Zip { children, mode }` |
| `comp.filter.is_some()` | wrapping `Comprehension::Filter { predicate, child }` node |
| `comp.order.is_some()` | wrapping `Comprehension::Order { strategy, truncation, child }` node |

Inspection sites that currently look at "what variables does this
comprehension bind?" can use `Comprehension::bound_vars()` (already
exists on the algebra AST) instead of walking `Clause::vars` lists.

### Risk

Medium. The match-arm rewrites are mechanical but tedious — and
they're spread across the activity crate's hot paths (scope_tree,
runner, executor). Each inspection site needs a careful read to
preserve semantics. Mitigation: do this push immediately **after**
the synthesis relocation (surface #3) so the synthesis call sites are
already isolated to one crate, narrowing the search radius.

---

## Surface 3 — Polydat source synthesis for child kernels

### What it does

Generates the Polydat source text for a child kernel that participates in
a comprehension. For each iteration variable, emits an extern
declaration with the right type; for each workload parameter the
comprehension interpolates, emits a `final` injection; propagates
parent-scope inputs through the chain; resolves placeholder
references; selects integration-vs-final binding form based on
binding origin.

Public functions today:

- `synthesize_for_each_scope` (11 external call sites — the big one)
- `synthesize_for_each_iteration` (per-iter materialization)
- `propagate_parent_inputs` (chain walking)
- `emit_workload_param_chain_aware` (3 external sites)
- `collect_leaf_placeholders`, `scan_one` (placeholder discovery)

This module is **2,288 lines** — about 45% of all legacy code.

### Where it lives now

`polydat::comprehension::synthesis`. Re-exported at the module root.

### Callers

`nbrs-activity` exclusively. ~14 sites across `scope.rs`,
`scope_tree.rs`, `runner.rs`. **No `nbrs-workload` or `polydat`-
internal callers.**

### Recommendation: **separate walking, building, and synthesis**

The substance of `synthesize_for_each_scope` conflates three distinct
operations that have a strict temporal order:

1. **Walking** — traversal of the environment to discover *what*
   matter is needed. The environment determines this: the
   comprehension AST contributes iter vars and referenced
   placeholders; the parent kernel's program contributes cascade-
   extern candidates and local-inclusion chains; the workload params
   contribute referenced values.
2. **Building** — accumulating matter into substance. The walker
   hands its discoveries to a builder, which records imports,
   exports, body fragments, and consumers. Substance is what you
   have once walking has driven enough builder calls.
3. **Synthesis** — turning substance into a typed kernel instance.
   `SubcontextBuilder::finalize()` does this: Rule 1 / Rule 2
   rewrites, type checks, lowering.

You cannot synthesize without substance. You cannot have substance
without having walked the environment and built matter from what
you found. Treating these as one operation — as the legacy
`synthesize_for_each_scope` does — hides ownership and conflates
concerns.

#### Ownership by operation

| Operation | Activity | Polydat |
|---|---|---|
| **Walking** | comprehension-AST walk (iter vars, type detection, placeholder scan); workload-param reference walk; scope-tree traversal | the *query primitives* the walker uses to inspect a parent kernel (`output_names`, `output_modifier`, `local_inclusion_chain`, `input_port_type`, etc.) |
| **Building** | drives the builder: which imports, which body fragments, which export specs, in what order | the builder itself (`SubcontextBuilder`) + its declaration methods. May grow sugar methods (`cascade_extern`, `import_typed`) only when the rewrite reveals a real need. |
| **Synthesis** | nothing | `SubcontextBuilder::finalize()` — already complete. |

The bulk of the 2,288-line synthesis module is **walking** logic
wrapped around a small amount of building, terminated by a polydat
synthesis call. Walking is the contested ground; most of it
dissolves into activity's walker, which uses polydat's query
primitives to inspect parent state.

#### This pattern is already partially implemented

The builder is `polydat::subcontext::SubcontextBuilder`. It is gated
by `ScopeKernel::subcontext_builder` (the parent is the only way in
— walled-off-API), and exposes `import` / `export` / `body` /
`add_result_bindings` / `with_compile_options` / `register_pull` /
`finalize`. SRD-67 Phases 1-5 already migrated other synthesisers
onto this surface; comprehension synthesis is the holdout. The
builder API's expressive sufficiency for the building step is
**presumed**; gaps are handled when they surface, not preempted with
speculative methods.

The query primitives the walker needs (`output_names`,
`output_modifier`, `local_inclusion_chain`, `input_port_type`,
`coord_count`, `input_names`, etc.) already exist on `PolydatKernel` and
`Program`. Some are `pub(crate)` or undocumented because the legacy
synthesis function was the only caller; promoting them to a
deliberate public surface is part of this push.

The cache-and-rehydrate primitive (`from_program` / `for_iteration`
on `PolydatKernel`) is the existing pattern that makes "compile once,
hydrate many per-instance contexts" work. It functions today; what's
missing is making it a first-class documented surface rather than a
pattern synthesisers stumble into.

#### What the cutover actually does

**Dissolve**, not delete. The substance of `synthesis.rs`
redistributes across walking / building / synthesis layers — most
goes to activity, some relocates within polydat to a more
appropriate layer, some dissolves entirely. Per-function mapping:

| Function | External callers | Destination |
|---|---|---|
| `synthesize_for_each_scope` | 12 (1 prod, 11 test) | **Dissolves** into (a) the shared cascade walker (activity, new) + (b) for-each-specific iter-var emission in a new `build_for_each_scope_kernel` (activity) |
| `synthesize_for_each_iteration` | 0 | **Dissolves entirely** (no external callers) |
| `propagate_parent_inputs` | 3 | **Stays in polydat** but relocates to a `PolydatKernel` method (`outer.propagate_inputs_into(&mut child)`). It's a kernel-chain operation, not a comprehension concern. |
| `emit_workload_param_chain_aware` | 3 | **Splits**: chain-walking part → polydat kernel query method; source-emission part → activity-side cascade-walker helper (Surface #7) |
| `workload_param_type_name` | 3 | **Activity** (Surface #7, walker helper) |
| `collect_leaf_placeholders` | 1 | **Activity** (walker helper) |
| `scan_one` | 0 (internal) | **Activity** (walker helper, with `collect_leaf_placeholders`) |
| `format_value_as_polydat_literal` | 1 | **Activity** (Surface #7) |
| `format_workload_param_as_polydat_literal` | 2 | **Activity** (Surface #7) |
| `value_to_param_string` | 3 | **Activity** (Surface #7) |
| `iterate` | 3 | **Activity** as transitional bridge; replaced by algebra surfaces (Surface #4) in PR 9c-3 |
| `ComprehensionIter` | 0 | **Dissolves with `iterate`** |

Net effect for polydat: loses ~2,000 lines of synthesis substance to
activity (walker + Surface #7); gains a handful of `PolydatKernel`
methods that lift kernel-chain operations out of the synthesis
module to where they actually belong; gains rustdoc on existing
query primitives that the walker consumes.

Net effect for activity: gains a `synthesis/` module containing the
shared cascade walker, the Surface #7 helpers, and per-scope-kind
builders. Importantly, **the existing three sister builders**
(`build_phase_scope_kernel`, `build_do_loop_scope_kernel`,
`build_op_template_scope_kernel`) **also refactor** to consume the
shared cascade walker. They currently duplicate the same cascade
logic; the dissolve work factors that duplication out, not just for
for-each.

#### What stays where in polydat after dissolve

1. **Builder** (`SubcontextBuilder`): unchanged unless the rewrite
   surfaces an inelegant building pattern, in which case sugar
   methods are added then. No speculative additions.
2. **Synthesis** (`SubcontextBuilder::finalize`): unchanged.
3. **Query API** (clean-up): the parent-program inspection
   primitives the cascade walker needs become deliberate public
   surface with rustdoc. No new primitives expected — they already
   exist; just exposing them cleanly.
4. **Kernel-chain operations**: `propagate_parent_inputs` and the
   chain-walking parts of `emit_workload_param_chain_aware` land on
   `PolydatKernel` as methods. Same substance, more honest layer.
5. **Cache-and-rehydrate** (`from_program` / `for_iteration`):
   gain comprehensive rustdoc naming the pattern, documenting the
   use case, and linking from this cutover doc.

### Risk

Medium. Risks specific to this approach:

- The walker rewrite is more invasive than a relocation — every
  call site is touched, not just its import statement. Mitigation:
  rewrite incrementally call-site-by-call-site, with the regression
  gate green after each. The builder's existing test coverage
  protects the finalize/compile path; the walker's correctness is
  checked by the same end-to-end regression gate the legacy code
  was checked against.
- Synthesis sub-cases the legacy code handles ad-hoc (parent-chain
  walking, placeholder discovery, workload-param interpolation
  emission) may not map one-to-one onto current builder methods. If
  a real gap surfaces, we add a builder method then — assumed
  sufficient until proven otherwise.
- The 11 `synthesize_for_each_scope` call sites are spread across
  three files; each is its own walker context. Some may share
  enough structure to factor into a reusable activity-side helper;
  some won't. Don't pre-factor — let the rewrite reveal the natural
  shape.

### Alternatives considered and rejected

**Relocate synthesis wholesale.** Earliest draft recommended moving
`synthesis.rs` to `nbrs-activity/src/synthesis/` as a self-contained
pipeline. Rejected because it preserves the monolithic
"synthesis function" shape rather than separating walking, building,
and synthesis; it bypasses `SubcontextBuilder` (the existing shared-
responsibility surface) and leaves comprehension as the only scope-
construction path going its own way; and it carries forward
implementation decisions made before the builder existed.

**Delete synthesis and rewrite call sites against the unchanged
builder.** Intermediate draft. Rejected because it implies the
caller must reinvent the parent-program cascade-discovery walks that
the synthesis function already encodes — work that's polydat
query-API territory, not walker policy. The "delete" framing
conflates removing a *module* with discarding its *substance*; the
substance must redistribute, not vanish.

---

## Surface 4 — Iteration driver

### What it does

Drives the per-iteration step sequence for a comprehension at runtime.
The legacy public surface:

- `iterate_scope(comp, kernel) -> ScopeIterations` — entry point.
- `IterationStep { var_bindings, ... }` — what the executor consumes.
- `ComprehensionIter` (separate type in `synthesis.rs`) — used for
  enumeration in synthesis.

### Where it lives now

`polydat::comprehension::iteration` (201 lines) + parts of
`synthesis.rs`.

### Callers

- `nbrs-activity/src/executor.rs` — `iterate_scope` is the runtime
  driver; `IterationStep` is the executor's per-iteration record.

### Recommendation: **polydat owns; surface is `algebra::surfaces::ScopedKernelStream<PolydatKernelScope>`**

The algebra layer already has the canonical replacement:

- `CoordinateStream` — first-order surface yielding coordinate tuples.
- `ScopedKernelStream<K>` — second-order surface yielding scoped
  kernel instances; `K: KernelScope` chooses the kernel-adaptation
  policy.
- `PolydatKernelScope` — the implementor that calls
  `PolydatKernel::for_iteration` to produce a per-iteration child kernel.

The executor consumes a `ScopedKernelStream<PolydatKernelScope>`. Per-
iteration, it receives a `ScopedKernelInstance` whose `kernel: Arc<PolydatKernel>`
field is the child kernel ready to use. The synthesis pre-stage
(now in nbrs-activity) produces the parent kernel; per-iter
`PolydatKernelScope` does the rebind via the existing
`PolydatKernel::for_iteration` primitive.

The legacy `IterationStep` type retires. The executor's per-iter
record becomes a `ScopedKernelInstance` (algebra type) or a thin
wrapper around it.

### Risk

Medium. The executor's iteration loop is performance-sensitive and
threads through several abstractions (`ResultDispenser`,
`MetricsDispenser`, `TraversingDispenser`, etc.). The streaming
surface is API-equivalent but the call shape differs — `next()` on a
stream vs a step-collection. Mitigation: do this push **last** (PR
9c-3), after surfaces #2 and #3 are settled; the algebra surfaces
already have parity tests so the API contract is solid.

---

## Surface 5 — String interpolation against a kernel

### What it does

`{var}`-style template interpolation where placeholders are resolved
by reading from a Polydat kernel. Used by:

- `interpolate_via_kernel(text, &kernel)` — looks up `{var}` against
  the kernel's bindings.
- `interpolate_with_lookup(text, |name| value)` — generic form with
  a caller-supplied lookup.
- `collect_string_interp_refs(text) -> Vec<String>` — extracts the
  placeholder names from a template.

### Where it lives now

`polydat::comprehension::eval` (2,419 lines — most of which is
internal helpers, not this surface).

### Callers

- `nbrs-activity/src/executor.rs` — 1 external call to
  `interpolate_via_kernel`.
- Internal: synthesis uses these heavily.

### Recommendation: **polydat owns; relocate to `polydat::kernel::interp`**

String interpolation against a kernel is **not** a comprehension
concern. It's a general GK-kernel facility that the comprehension
runtime *happens* to use, and that synthesis *happens* to need, and
that the executor *happens* to call. Putting it under
`polydat::comprehension::eval` was incidental — it's where it was
born, not where it belongs.

Relocate the three public functions to a new module
`polydat::kernel::interp`. Re-export `interpolate_via_kernel` at the
crate root for compatibility ergonomics.

The remaining ~2,400 lines of `eval.rs` are internal helpers (spec
evaluation, list-with-types parsing, tuple enumeration). Most of
this dies when the algebra surfaces replace it:

- `enumerate_tuples` → replaced by `CoordinateStream`.
- `evaluate_spec` → replaced by algebra optimizer + IR.
- `parse_list_with_types` → replaced by `Source::Literal` typing.
- `pre_evaluate_clause` → replaced by algebra metadata propagation.

What survives is small, polydat-internal, and gets relocated to
wherever the algebra interpreter needs it (likely inside
`algebra::ir::interpreter`).

### Risk

Low for the relocation itself (3 functions, 1 external caller, clear
new home). The internal-helper deletion has more surface area but
all of it is bounded by "code only legacy modules call." When the
legacy modules retire, the helpers retire with them.

---

## Surface 6 — Order application

### What it does

Applies a `TraversalOrder` to a tuple sequence — implements the 10
ordering strategies (Lex, ReverseLex, Diagonal, Antidiagonal,
Extrema, Shells, Halton, Sobol, Lhs, Custom).

Public symbols: `apply_order`, `Tuple`.

### Where it lives now

`polydat::comprehension::order` (937 lines).

### Callers

**Zero external callers.** All internal to `synthesis.rs` and
`eval.rs`.

### Recommendation: **delete after surfaces #3 and #4 retire**

The algebra layer's `strategies/` module already implements all 10
strategies (PR 3 of the algebra implementation plan) under the
algebra AST's `Order` constructor. The legacy `apply_order` exists
solely to serve the legacy iteration/synthesis paths.

When those paths retire (PR 9c-3), `order.rs` deletes cleanly. No
preservation needed; the algebra strategies are the canonical
implementation and have their own test coverage.

Special note: legacy `TraversalOrder::Custom { function }`
(user-supplied Polydat function as the ordering) is **removed** per spec
§3.6 — already handled by `legacy_to_algebra` which raises
`ConvertError::CustomOrderingRemoved`. Custom orderings are not
forward-supported; documented as a breaking change.

### Risk

None — pure deletion of internal code with no external callers.

---

## Surface 7 — Polydat literal formatters

### What it does

Small utility functions for emitting Polydat source literals:

- `format_value_as_polydat_literal(&Value) -> String` — generic Value → GK
  literal.
- `format_workload_param_as_polydat_literal(&str) -> String` — workload-
  param-specific formatter (handles string quoting rules).
- `value_to_param_string(&Value) -> Option<String>` — for inserting
  values into placeholder slots.
- `workload_param_type_name(&str) -> &'static str` — type-name
  inference for emitting extern declarations.

### Where it lives now

`polydat::comprehension::synthesis` (mixed in with the synthesis
module).

### Callers

100% nbrs-activity (same distribution as surface #3).

### Recommendation: **nbrs-activity owns as walker-side helpers**

These are Polydat code-generator details — what the walker (Surface #3)
needs to emit `import` / `body` declarations into the builder. They
live in `nbrs-activity` alongside the scope walker that calls them.
Activity-side because the output shape is governed by
nbrs-activity's Polydat source conventions, not by polydat's algebra; and
because the only callers are the walker and its peers.

If a polydat-internal need for "format a Value as a Polydat literal"
later emerges (e.g., for diagnostic dumps), polydat can grow its
own simple formatter — the rules are well-defined and the
implementation is small.

### Risk

Low. These are small, well-defined utility functions with a single
caller-class (the walker). They land naturally next to the walker
during the Surface #3 rewrite.

---

## Derived cutover sequence

The contracts above induce a three-push sequence with clean
dependencies between pushes.

### PR 9c-1a: Surface 1 (parser routing) — **shipped**

Already landed. External parser calls route through
`algebra::spec`; nbrs-workload's YAML loader builds
`ComprehensionSpec` and calls `.into_legacy()`; the
`comprehension_from_subspaces` pipeline is no longer reached from
outside polydat. 93 baseline tests green.

### PR 9c-1b: Dissolve synthesis into walker + builder + query API

- **Walker (activity, new):** build a walker in `nbrs-activity/src/synthesis/`
  that performs the comprehension-AST walk, the workload-param
  reference walk, and the parent-program walk (via polydat's public
  query API). The walker drives `SubcontextBuilder` to accumulate
  matter and calls `finalize()` at the end.
- **Polydat query API (clean-up):** promote whatever `PolydatKernel` /
  `Program` query methods the walker needs from `pub(crate)` /
  undocumented to deliberate public surface with rustdoc. No new
  primitives expected — these already exist; just exposing them
  cleanly.
- **Builder (no changes unless required):** if the rewrite surfaces
  an inelegant building pattern, add sugar methods then. Otherwise
  the builder stays as-is.
- **Cache-and-rehydrate rustdoc:** add comprehensive rustdoc to
  `from_program` / `for_iteration` naming the pattern, documenting
  the use case, and linking from this cutover doc.
- **Migrate call sites:** production first (runner.rs:1985 — the
  single non-test caller), then test fixtures. Each migration
  preserves semantics; the walker output must be byte-equivalent
  to the legacy `synthesize_for_each_scope` output for the same
  inputs, since downstream consumers depend on the Polydat source shape.
- **Remove** `polydat::comprehension::synthesis` once the walker
  has taken over all call sites.
- Initially the walker consumes the **legacy** `Comprehension` AST
  (the algebra wire-type switch is 9c-2).
- Acceptance: regression gate (93 baseline tests) green; polydat
  sheds the synthesis module; comprehension joins the other
  scope-construction paths on the builder.

### PR 9c-2: Switch the wire type (surface #2)

- Workload model holds `algebra::Comprehension` instead of legacy
  `Comprehension`.
- All inspection sites in nbrs-activity (scope.rs, scope_tree.rs,
  runner.rs, executor.rs, scope_flattening.rs) migrate match arms
  from legacy AST shape to algebra operator-tree shape.
- The walker (from 9c-1b) becomes an algebra-AST walker.
- `ComprehensionSpec::into_legacy()` consumers switch to
  `.into_algebra()`; the legacy-AST adapter retires.
- After this push, the legacy AST module has **no users**.
- Acceptance: regression gate (93 baseline tests) green; all match-
  arm rewrites preserve semantics; spec-surface parity tests
  extended to cover walker output equivalence.

### PR 9c-3: Final delete (surfaces #1, #4, #5, #6)

- Replace executor's `iterate_scope` calls with
  `ScopedKernelStream<PolydatKernelScope>`.
- Move `interpolate_via_kernel` to `polydat::kernel::interp`.
- Delete legacy modules: `ast`, `parse`, `eval`, `order`,
  `iteration`.
- Promote `algebra::*` to `comprehension::*` (atomic rename;
  `algebra::Comprehension` becomes `comprehension::Comprehension`,
  etc.).
- Run the full regression gate plus the spec-surface parity suite.
- Acceptance: regression gate green; legacy modules gone; polydat
  `comprehension::` module is just the algebra layer with its
  natural name.

### Why this ordering

- **9c-1a first** (done): isolates parser routing as the chokepoint;
  removes the smallest, lowest-risk legacy surface first.
- **9c-1b second**: rewrites the largest legacy chunk against the
  existing builder pattern. Done before the wire-type switch so
  the walker is exercising the legacy AST it already understands —
  one variable at a time. Rewriting against a new AST AND a new
  builder pattern simultaneously would muddy debug context if
  anything broke.
- **9c-2 third**: switches the AST type system-wide. By this point
  the walker is the only structural consumer of comprehension
  semantics; switching it is the system-wide change.
- **9c-3 last**: depends on the executor having migrated off
  `iterate_scope`, which is easiest once the walker no longer needs
  the legacy iteration types.

Each push is independently reviewable, has its own acceptance bar
(regression gate), and leaves the tree in a working state.

---

## Resolved decisions

1. **Walker location in nbrs-activity (Surface #3).** The walker
   rewrite lives in a new `nbrs-activity/src/synthesis/` module,
   broken into submodules for the walker itself, the Surface #7
   formatters, and the comprehension-walking helpers. Clean
   separation from existing scope code; easy to test in isolation;
   scales as the walker grows.

2. **Builder-API sufficiency (Surface #3).** Presumed sufficient.
   Gaps are addressed when discovered during the rewrite, not
   preempted with speculative methods.

3. **Cache-and-rehydrate documentation (Surface #3).** PR 9c-1b
   adds comprehensive rustdoc to `from_program` / `for_iteration`
   naming the pattern, explaining the use case, and linking from
   this cutover doc. No new SRD — the primitives are the natural
   docs surface, and an SRD is reserved for cases where contributors
   need deeper architectural framing than rustdoc can carry.

4. **Surface #5 (interpolation relocation).** The relocated
   functions land at `polydat::kernel::interp`. Interpolation IS
   against a Polydat kernel (`interpolate_via_kernel` takes
   `&PolydatKernel`); putting it under `kernel::` makes the coupling
   visible in the path.

5. **Push 9c-3 atomicity.** Bundled: 9c-3 ships executor migration
   + interp move + legacy delete + algebra→comprehension rename
   together. The rename is search-and-replace once everything else
   lands; bundling avoids a churn-only intermediate state.
