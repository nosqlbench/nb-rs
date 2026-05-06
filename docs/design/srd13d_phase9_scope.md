# SRD-13d Phase 9 Scope Assessment

**Status:** scoped, **not implemented**. This memo defines the
work to make Phase 9 actionable. Reviewer: Jonathan.

## What Phase 9 is

The last row of SRD-13d §9 (Implementation phases):

> **Phase 9** — *Op-dispenser holds (or doesn't hold) its own kernel handle.*

The runtime consumer of phases 1-8: now that scope-tree nodes
are marked `materialised: bool` with a `cached_kernel` slot,
op-dispensers should pick up the right kernel handle and use
it to evaluate per-cycle GK expressions.

The motivating concrete pain point lives in
`nbrs-activity/src/wrappers.rs::MetricsDispenser`:

```rust
/// SRD-13d Phase 9 will route this through a real GK eval.
value_expr: String,
```

Today the wrapper treats `value_expr` as a bare-binding-name
lookup against `OpResult.captures`. Non-bare expressions
(e.g. `value: mul(latency_curve, 2)` in
`synthetic_metrics.yaml`'s mapping form) silently degrade to
`None` and the slot warns + skips. With a kernel handle and
real eval, those expressions would just work.

## Current state (post-consolidation, 2026-05)

What's already in place:

- **Scope-tree plumbing** (Phases 1-7): every `ScopeNode`
  carries `materialised: Option<bool>`, `logical_name: String`,
  and `cached_kernel: OnceLock<Arc<GkKernel>>`. The
  scope-flattening pre-walk
  (`nbrs-activity/src/scope_flattening.rs`) sets the marks.
- **`nearest_materialised()` accessor** is documented in SRD-13d
  §5.1 and in the scope_tree docs; the tree's `OpTemplate`
  scope kind is wired.
- **`Component` per op dispenser** holds the metrics
  registry (post-consolidation). The op-dispenser
  construction site (`activity.rs::run_with_adapters`,
  ~line 1014-1042) creates `dispenser_component` and calls
  `MetricsDispenser::wrap(inner, metrics, &mut component)`.
- **`MetricsDispenser` slot** carries `value_expr: String`,
  ready to receive a kernel-eval upgrade.

What's NOT in place:

- **No kernel handle reaches `MetricsDispenser::wrap`.** The
  op-template scope's kernel lives in the scope tree;
  `wrap` only sees the metrics decl + the component.
- **No expression-string GK eval API.** `nbrs-variates`
  exposes `GkAssembler` + `GkKernel`, but compiling and
  evaluating a free-form expression like `mul(latency_curve, 2)`
  in the context of an existing kernel's outputs is not a
  one-call surface today. The closest infrastructure is
  `ScopeFixture` + `PullPlan` (pre-declared bindings) — built
  for the bind-time path, not for per-cycle ad-hoc eval.
- **No scope-tree ↔ op-dispenser lookup.** When constructing
  an op-dispenser inside `run_with_adapters`, the activity
  has no direct handle on the corresponding scope-tree
  `OpTemplate` node. Today it iterates `templates: &[ParsedOp]`
  by-index against op-config, never crossing into scope-tree
  state.

## Scope of the work

### Cross-crate plumbing

| Step | Crate | Surgery |
|------|-------|---------|
| 1 | `nbrs-activity::scope_tree` | Expose a `find_op_template(name) -> Option<&ScopeNode>` accessor. Currently the tree knows about op-templates internally; the runtime needs a way to look one up by name + parent-phase context. |
| 2 | `nbrs-activity::activity` | At op-dispenser construction (line ~1014), look up the scope-tree node for the template, walk to `nearest_materialised()`, get its `cached_kernel.get_or_init(...)` handle. Thread an `Option<Arc<GkKernel>>` into `MetricsDispenser::wrap`. |
| 3 | `nbrs-activity::wrappers` | `MetricsDispenser::wrap` signature gets the kernel arg. Slot stores it (or shares one across slots if uniform). |
| 4 | `nbrs-variates` | New API: `GkKernel::eval_expr(expr_str, inputs) -> Result<Value, EvalError>`. Compiles the expression string against the kernel's wire vocabulary, instances on demand (or reuses cached compiled fragments), runs, returns the scalar. |
| 5 | `nbrs-activity::wrappers` | Per-cycle path in `MetricsDispenser::execute`: if `value_expr` parses as a bare name → captures-lookup (existing). Else → `kernel.eval_expr(value_expr, fiber_inputs)`. |

Estimated LOC: ~150 in `nbrs-variates` (the `eval_expr` API
is the load-bearing piece — needs to handle expression
parsing, type inference against the parent kernel's wire
types, single-output extraction, and error reporting), ~80 in
`nbrs-activity` (lookup + plumbing), ~20 in `wrappers.rs`
(slot dispatch).

### Risk surface

- **GK expression-string eval is a new public API.** The current
  GK consumer model is "compile a program from declared bindings,
  instance, run." Free-form expression eval at runtime is
  conceptually different — it's a REPL-style operation. The
  existing `nbrs-variates::dsl` parser can probably be reused,
  but binding the expression's free wires to the parent
  kernel's outputs is novel work.
- **Caching strategy.** Naive: re-parse + re-compile `value_expr`
  every cycle. Real: cache the compiled expression on the
  slot (it's static across cycles). Real-real: also cache
  the JIT-specialised form. Phase-9-good-enough = parse +
  compile once at slot-init, eval per cycle.
- **Type coercion.** `value_expr` outputs may be `U64` / `F64` /
  `Bool` / `Str`. The current `value_to_f64` helper handles
  numeric variants; the eval path needs to feed through that
  same coercion plus emit the same warning/skip semantics
  for non-numeric outputs.

### Out-of-scope (intentional)

- **Closures over outer-scope state.** SRD-13d Phase 9 is
  about op-template scopes. Expressions referencing wires
  from the enclosing phase scope work today via
  `bind_outer_scope` (the kernel handle covers it). No new
  multi-scope coordination needed for Phase 9 itself.
- **GK kernel sharing across op-templates.** Every
  materialised op-template gets its own kernel (per the
  flattening rules). Phase 9 doesn't change that — it just
  picks up whatever the pre-walk decided.
- **Reactive / control-driven `value_expr`.** Phase 9 evaluates
  the expression at metric-record time per cycle. SRD-23
  dynamic controls don't enter the picture; if a wire's value
  changes due to a control mutation, that's already visible
  through the kernel's normal input wiring.

## Recommendation

**Defer to a dedicated push.** The work is contained in
scope (one feature, three well-bounded crate changes), but
the GK `eval_expr` API design merits standalone attention
— it touches the public GK surface and deserves its own SRD
note + design conversation before code.

A reasonable two-step landing:

1. **Step 1 (small):** Add a `GkProgram::compile_expr(expr_str)
   -> Result<CompiledExpr, _>` API that produces a
   single-output sub-program against the parent program's
   wire vocabulary. Test with a unit test that compiles
   `mul(load, 2)` against a kernel that exposes `load`.
2. **Step 2 (medium):** Wire it through to `MetricsDispenser`,
   add an integration test on `synthetic_metrics.yaml` with
   a non-bare `value:` (e.g., `value: mul(latency_curve, 2)`).

Until then: bare-name lookup keeps working (covers the
canonical SRD-40b §1 form), non-bare expressions warn and
skip with the existing telemetry. The current `value_expr:
String` field is forward-compatible — the upgrade is
additive, not a schema change.

## What lands now

This memo. Plus an updated SRD-13d §9 row noting Phase 9 is
scoped + parked behind the standalone `eval_expr` design.

Nothing else changes.
