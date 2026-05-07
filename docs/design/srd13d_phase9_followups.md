# SRD-13d Phase 9 Follow-ups

**Status:** design memo. Phase 9's core (per-op-template kernel
synthesis + install + per-fiber instancing + dispenser routing)
shipped 2026-05-06; this memo tracks the four remaining
sub-tasks the original scoping memo
(`docs/design/srd13d_phase9_scope.md`) flagged.

> **Architectural note (2026-05-06).** The runtime fixes called
> out in the "Real bug surfaced and fixed during §4 implementation"
> section below — Coordinate-input cascade emit, workload-param
> precedence over manifest cascade, owning-phase ambiguity in
> the install loop, post-`bind_outer_scope` init re-pull on
> op-template kernels, name-keyed `scope_values` to defeat
> cross-kernel index mis-routing — are each correct in isolation
> but share a single root cause: the parent/child kernel contract
> is implicit. **SRD-13e (Scope-as-Module Refinement)** specifies
> the unification: every sub-scope is a typed `ScopeModule` with
> explicit import/export contracts, `instance_under(parent)` is
> the single typed attach operation, and the bug shapes that
> drove these fixes become structurally impossible. Read SRD-13e
> before working on this memo's punch list — most items collapse
> into the migration plan there.

Cross-refs:
- SRD-13b (combination modes — inline / scope composition /
  subgraph / reification).
- SRD-13c (scope model — extern wiring, default immutable
  propagation, `shared` modifier and SharedCell).
- SRD-13d (op-template scope layer; this memo's parent).
- SRD-13e (scope-as-module refinement; supersedes the
  architectural fixes documented in this memo).
- SRD-40b (synthetic metrics, primary consumer).

---

## 1. Non-bare metric `value:` expressions

### Status today

`MetricsDispenser` requires `value:` to be a bare binding
name. Anything with operators / parens / function calls
(e.g. `value: mul(load, 2)`) errors at wrap-time:

```
metric '<family>' value '<expr>' is not a bare binding name —
non-bare expressions are deferred to SRD-13d Phase 9
(op-dispenser kernel handle).
```

The original scoping memo expected this to need a new
`GkProgram::compile_expr` runtime API. Re-reading the GK
design surfaces a simpler path.

### What's already there

The list-form sugar in `metrics:` already handles non-bare
expressions:

```yaml
metrics:
  - forecast_low := mul(latency_curve, 0.9)
  - forecast_high := mul(latency_curve, 1.1)
```

Parser path: `nbrs-workload/src/parse.rs::inject_wire_into_bindings`
splices each `<wire> := <expr>` entry into the op's `bindings:`
block. The wire becomes an op-level binding, gets compiled
into the op-template kernel as a regular output, and the
dispenser pulls it by name. The metric registered for that
slot has `value: <wire>` — a bare name.

The mapping form doesn't auto-inject:

```yaml
metrics:
  forecast_low:
    value: mul(latency_curve, 0.9)   # error today
```

### Proposal: parse-time auto-injection (no new runtime API)

Extend the mapping-form parser (`parse_metric_spec_value` in
`parse.rs`) to detect non-bare `value:` and inject a synthetic
binding the same way the list form does:

1. Detect non-bare `value:` (whitespace / operators / parens /
   non-ident chars).
2. Generate a synthetic wire name — `__metric_<family>` or
   the metric's map key with a reserved prefix.
3. Inject `<synthetic_wire> := <value_expr>` into the op's
   `bindings:` (`inject_wire_into_bindings`).
4. Replace the metric's `value:` with `<synthetic_wire>` so
   the dispenser pulls the bare name.

The op-template kernel naturally compiles the new binding;
runtime resolution goes through the existing
`MetricsDispenser` GK pull plan. No `compile_expr` runtime
API needed. SRD-13b §"Inline" is the relevant combination
mode — the expression splices into the op-template kernel's
DAG at compile time, exactly like a user-written binding.

### Why this is better than a runtime `compile_expr`

- **Same path for both forms.** List-form `:=` and
  mapping-form `value:` both end up as op-template kernel
  bindings; the difference is purely surface syntax.
- **Constant folding survives.** A new runtime-compile path
  would build a sub-program at wrap-time, missing the
  workload-init-time fold pass on the parent kernel.
  Parse-time injection runs the expression through the same
  pipeline as every other binding.
- **Provenance preserved.** Errors point at the user's
  source line (the `value:` text), not a synthesised
  sub-program with no location.
- **Diagnostics consistent.** `dryrun=op` and `nbrs describe
  gk` already render op-level bindings; injected ones show up
  identically.

### Implementation notes

- `parse_metric_spec_value` returns `MetricSpec`. The current
  signature has no access to the op's mutable bindings; the
  caller (`parse_metrics_field`) does. Either thread `&mut
  op_bindings` through, or have the parser return both the
  rewritten spec and the binding to inject.
- Naming: pick an unambiguous prefix (`__metric_<family>`
  is fine — the `__` prefix already conventionally marks
  compiler-generated wires per the `__anon_*` pattern).
- Collision check: if the synthetic name already exists in
  `op_bindings`, error at parse-time with a helpful
  message (the user wrote a binding named `__metric_*`,
  which collides with the reserved prefix).
- Preserve the original `value:` text in the diagnostic
  surface so error messages still quote what the user
  wrote.

### Test surface

- `parse_metrics_mapping_with_non_bare_value_injects_binding`
  — round-trip a mapping form, assert the op's bindings
  gained `__metric_<family> := <expr>`.
- Workload integration: extend `synthetic_metrics.yaml` with
  a non-bare `value:` and assert the computed value lands in
  `metric_family`.

---

## 2. Cross-scope per-cycle value contract

### The trap

SRD-13c §"Default: Immutable Propagation" is unambiguous:

> Outer scope values are propagated to the inner scope as
> **immutable snapshot inputs**. The inner scope reads them
> but cannot modify them. The values are copied once at
> inner scope creation, not per-cycle.

For op-template scopes, scope creation is **per-fiber**, not
per-cycle. An op-template kernel's `bind_outer_scope` pulls
parent outputs once when the fiber starts, then never again.

The shipped synthetic_metrics workload steps right into the
trap:

```yaml
phases:
  predict:
    bindings: |
      load := add(cycle, 1)
      latency_curve := mul(load, 2)
      forecast_low := mul(latency_curve, 0.9)   # phase-level, fine
```

Phase-level bindings are in scope for all ops in the phase
(they're outputs of the phase kernel, externs on the
op-template kernel). Per-cycle freshness works because the
op-template's `cycle` input is set per cycle (FiberBuilder
propagates), and the op-template kernel re-evaluates its own
externs through bind_outer_scope's value-copy path.

Wait — re-read carefully. SRD-13c says values are copied
once at scope creation. Op-template scope creation is at
fiber start. So `latency_curve` in the op-template kernel's
extern slot is whatever it was at fiber start — `cycle = 0`
(or whatever cycle the fiber was constructed at). It does
NOT update per cycle.

In practice today this works for the existing test only
because the test asserts row-existence, not values. A test
that checks `forecast_low == latency_curve_at_cycle_N * 0.9`
for several Ns would fail.

### The contract

Per SRD-13c:

- **Default (no modifier)**: parent output value is copied
  to inner extern at scope creation; inner sees the
  snapshot, not subsequent updates.
- **`shared` modifier**: parent output is backed by a
  `SharedCell` (an `Arc<Mutex<Value>>`); inner's input slot
  attaches the same cell; reads go through the cell on
  every access. Per-cycle freshness via this mechanism.

For op-templates, this means:

- `cycle` is an INPUT (coord) on every kernel; FiberBuilder
  writes it per-cycle to every kernel directly. ✓
- Bindings derived only from `cycle` (the op-template's own
  bindings like `step := 1`, `observation := mod(cycle, 100)`)
  evaluate fresh per cycle. ✓
- Bindings derived from parent OUTPUTS that aren't `shared`
  see a stale snapshot. ✗

### Proposal: workload-author guidance + compile-time check

Two complementary moves:

**Author-level contract** (added to SRD-40b §"value: contract"
or similar): when a metric `value:` references a wire from an
ancestor scope (not declared locally), the wire must be one
of:

1. A scope INPUT on every layer of the chain (e.g., `cycle`).
   Per-cycle freshness intrinsic.
2. A `shared`-modified output in the declaring scope. Live
   linkage via SharedCell.
3. A scope-init binding (compile-time constant). Snapshot is
   the final value; no per-cycle change matters.

Anything else gets a stale snapshot — error at compile time.

**Compile-time check**: extend the auto-extern pass in
`build_op_template_scope_kernel` (or a new validation pass)
to walk each op-template binding's referenced names and
check the manifest entry's modifier:

```rust
for ref_name in op.referenced_names() {
    if op.bindings.declares(ref_name) { continue; }                    // local
    let entry = parent_manifest.get(ref_name).ok_or(...)?;
    match entry.kind {
        ParentRefKind::Input => {}                                     // (1) cycle, source coord
        ParentRefKind::SharedOutput => {}                              // (2) shared modifier
        ParentRefKind::ConstantOutput => {}                            // (3) folded constant
        ParentRefKind::DynamicOutput => return Err(format!(
            "op-template '{op_name}' references parent wire '{ref_name}' \
             which is a per-cycle-changing output without `shared` \
             modifier. Either: (a) mark it `shared` in the parent, \
             (b) rebind it locally in this op's bindings, or (c) move \
             the metric value to the parent scope."
        )),
    }
}
```

`ParentRefKind::ConstantOutput` and `ParentRefKind::DynamicOutput`
require the manifest to track folded-constant status. The
GkProgram already has `output_modifiers` and `get_constant`;
the manifest entry just needs to expose both.

### Implementation notes

- `ManifestEntry` already exists in `nbrs-activity::runner`
  with `name`, `port_type`, `modifier`. Extend with a
  `kind: ParentRefKind` enum.
- The check fires at workload-init time, alongside the
  scope-flattening pre-walk. Gating output: a new error
  variant in `compile_bindings_with_libs_excluding`'s
  return.
- Strict-mode integration: in `strict` mode, the check is a
  hard error. In permissive mode, it's a warning logged
  once per op-template (so workloads that knowingly rely on
  snapshot semantics can opt in).
- Parse-time auto-injected metric bindings (see §1) are
  subject to the same check — if the user writes
  `value: mul(latency_curve, 2)` and `latency_curve` isn't
  shared, the check fires.

### Test surface

- `op_template_referencing_unshared_parent_output_errors`
  — workload with `value: mul(load, 2)` where `load` isn't
  `shared` errors at workload-init time.
- `op_template_referencing_shared_parent_output_compiles`
  — same workload with `shared load := …` compiles + values
  flow live per cycle.

---

## 3. Value-correctness integration test

### Status today

`synthetic_metrics_workload_populates_metric_family`
(in `nbrs/tests/workload_examples.rs`) only asserts that
`metric_family` has the expected rows — it doesn't check
that the recorded values are right. With Phase 9 routing
the dispenser's pulls through op-template kernels, value
correctness is the bigger semantic guarantee.

### Proposal

Add a sibling test
`synthetic_metrics_workload_records_correct_values` that:

1. Runs the workload with a small, predictable cycle count
   (say 12).
2. Reads `metric_family` + `metric_instance` + `sample_value`
   from `metrics.db`.
3. For each cycle's recorded value, asserts the value
   matches the formula. E.g. for `latency_curve_ms` at
   cycle N: assert recorded gauge ≈ `(N+1) * 2`.
4. Repeats for every metric in the workload (load,
   forecast_low, forecast_high, step_counter_ops,
   observation_dist).

### Implementation notes

- `metric_instance` rows tie a family to a label set;
  `sample_value` rows hold per-tick samples. The schema is
  defined in `nbrs-metrics/src/reporters/sqlite.rs`.
- For histogram families (observation_dist), assert against
  reasonable distribution properties (max, count, sum) since
  histograms aggregate.
- For counters (step_counter_ops), assert monotonic
  increase by 1 per cycle.
- Run the workload with a fixed `cycle_count` to avoid
  flake from cadence-reporter timing.

### What this test will surface

- The §2 cross-scope per-cycle gap, if any metric depends
  on a non-shared parent output.
- Off-by-one errors in cycle propagation.
- Per-fiber kernel instancing bugs (e.g., kernel state not
  resetting between stanza boundaries).

---

## 4. Other op-level wrappers under materialised op-templates

### Status today

`MetricsDispenser` was the explicit Phase 9 consumer. The
other op-level wrappers — `ValidatingDispenser`,
`ConditionalDispenser`, `ThrottleDispenser` — also register
pulls into the per-op-template `ScopeFixture` (since I
changed the fixture's program to
`op_builder.program_for_op(template.name)`). They build OK,
but no integration tests exercise them under workloads with
materialised op-templates.

### Proposal

Add smoke tests for each wrapper under a materialised
op-template scope:

- `validation_under_materialised_op_template` — workload
  with `relevancy:` block on an op that has its own
  `bindings:`, assert recall is computed correctly.
- `conditional_under_materialised_op_template` — workload
  with `if:` whose condition is an op-level binding,
  assert the op skips correctly when condition is false.
- `throttle_under_materialised_op_template` — workload
  with `delay:` whose value is an op-level binding,
  assert the throttle reads the right value per cycle.

Each test mirrors the synthetic_metrics shape: build a
workload, run it through the stdout adapter, inspect
metrics.db (or stdout for skip behavior).

### Implementation notes

- `examples/workloads/` already has fixtures for each
  wrapper feature in isolation; combining them with op-level
  bindings is the new shape.
- The wrappers' `wrap()` calls `register_pull` on `fx`
  identically to MetricsDispenser; they should "just work"
  if the fixture's program matches the actual fiber state
  at resolve time. The new tests verify that.

### Risk / what could surface

- A wrapper that registers a pull for a name not in the
  op-template program (e.g., a phase-level binding the op
  needs) would fail at register_pull. The compile-time
  check from §2 would catch this in advance.
- Per-fiber kernel instancing has `set_inputs` and
  `set_source_item` propagating to all kernels; bugs in
  that propagation would manifest as wrappers reading stale
  values.

---

## Order of operations

A reasonable landing order:

1. **§2 first** (cross-scope contract). Lock the contract
   before workloads grow around the wrong assumption. The
   compile-time check is the load-bearing piece — once it's
   in place, the other follow-ups can rely on the
   guarantees.
2. **§3** (value-correctness test). Validates §2 from the
   workload-author side; would fail today against the
   stale-snapshot behaviour, pass after §2's check guides
   workload changes.
3. **§1** (auto-inject for mapping-form `value:`). Lifts
   the bare-name restriction, completing SRD-40b §1's
   user-facing contract.
4. **§4** (other wrapper smoke checks). Quick verification
   pass; surfaces nothing if §2 + §3 are clean.

Each is independent enough to land in its own commit; they
don't share much code surface beyond `parse.rs` (§1) and
`scope.rs` / `runner.rs` (§2).

---

## Open questions

- **Should `cycle` itself be classified as `Input` or
  `SharedOutput` for the §2 check?** It's a coord input on
  every kernel, set per-cycle by FiberBuilder. The check
  needs to know that referencing `cycle` in an op-template
  binding is fine. Cleanest: have the check skip names that
  are inputs on EVERY kernel in the chain.

- **Does the §2 check apply transitively?** E.g., op-template
  references `forecast_low`, which references `latency_curve`,
  which references `load`. If only `load` is `shared`, the
  intermediates are stale snapshots. Probably yes — walk the
  parent program's DAG and check every transitively-referenced
  output.

- **Should §1's auto-inject support op-level capture
  references?** Mapping `value: mul({capture:rows}, 2)` where
  `{capture:rows}` is a result-body extraction. Today the
  capture flow is post-execute via `OpResult.captures`; for
  op-template kernel injection, the capture would need to
  route as a kernel input. That's a separate design question.

---

## Pre-existing bug surfaced during §2 implementation

`HasGkMatter::gk_matter()` for `ParsedOp` reads
`self.bindings`, which is the **post-parser-merge** view —
the parser splices each phase's `bindings:` block into every
op's `bindings` field at parse time (the legacy
`parse_phases` behaviour, documented on
`WorkloadPhase::bindings`'s doc comment). Result: every op
ends up with non-empty `bindings`, so `gk_matter()` returns
`Definitions` even for ops that have no own GK content. The
scope-flattening pre-walk then marks every op-template
scope as materialised, the install loop builds a kernel for
each, and the contract check from §2 passes vacuously
because every reference resolves to a locally-merged
binding. The redundant op-template kernels are inert at
runtime (the dispenser still gets correct values via the
GK pull plan against its own kernel), but they're wasted
compile work and they prevent the flatten optimisation from
firing the way SRD-13d §3 designed it.

The SRD-13d §3.1 docstring on `gk_matter.rs` already calls
this out:

> Today's parser also legacy-merges this into per-op
> bindings; the phase still owns the structural fact that
> it declared the binding (SRD-13d §3.1's classification
> operates on the AST, not on the post-merge runtime view).

The fix:

1. Track the op's *own* bindings separately from the merged
   view. Either (a) keep `ParsedOp.bindings` as the merged
   view and add a new `own_bindings: BindingsDef` carrying
   only what the YAML declared on the op, or (b) un-merge
   at parse time — keep ops' bindings own-only and have
   the activity-construction code resolve phase bindings
   from the phase AST instead.
2. Update `HasGkMatter for ParsedOp` to read the own-only
   field.
3. Audit the rest of the codebase for sites that currently
   rely on the merged `bindings` field; either route them
   through the merged view explicitly or migrate them to
   resolve from the phase AST.

(b) is the cleaner end state because the post-merge view is
genuinely a backward-compat shim. The merge only matters
because `compile_bindings_with_libs_excluding` consumes a
flat ops list and doesn't have phase context — once Phase 9
is fully wired, the per-op-template kernel synthesis takes
the op's own bindings and the parent kernel handles the
inherited ones via `bind_outer_scope`. The legacy merge
becomes dead code.

This is bigger than Phase 9 — it touches the parser, the
workload model, and several call sites in `bindings.rs` and
`scope.rs`. Tracked here because §2 surfaced it; the fix
properly belongs to a separate follow-up SRD note (e.g. a
"SRD-13d §3 own-bindings split" memo) rather than landing
under Phase 9 follow-ups.

---

## Pre-existing bug surfaced during §3 implementation

`mul(x, 0.9)` (and friends — any registry-arity arithmetic
function whose factor port declares `SlotType::ConstU64`)
silently truncates a float-literal const argument to `0`
when the wire-typed input is u64. Discovered while running
`synthetic_metrics_workload_records_correct_values`: the
example workload had

```yaml
- forecast_low := mul(latency_curve, 0.9)
```

with `latency_curve` carrying a `u64` value. The kernel
compiled cleanly, but at runtime `forecast_low` came out as
`0` for every cycle. Root cause: the `mul` registry signature
binds `factor: ConstU64`; the const-coercion path narrows
`0.9` to `0` instead of erroring.

The example workload was rewritten to use the `*` operator
(`latency_curve * 0.9`), which routes through
`BinOpKind::Mul`'s widening rule in `dsl/binding.rs` and
correctly emits `f64_mul` with a `to_f64(latency_curve)`
upstream. That unblocks §3.

The underlying behaviour is the bug worth tracking
independently of Phase 9:

- `mul(u64_wire, 0.9)` should either reject at compile time
  (type mismatch — the registry says ConstU64, the literal
  is FloatLit) or auto-route through `f64_mul` with
  `to_f64(...)` widening on the wire side, mirroring what
  `*` does.
- Silent narrowing to `0` is the worst of both: no error at
  compile time, no diagnostic at run time, just a wrong
  number in production telemetry.

Fix scope is in `dsl/registry.rs` and the const-arg coercion
in the function-call lowering pass. Tracked here because §3's
test surfaced it; belongs to a separate "DSL: tighten const-
type coercion" follow-up rather than landing under Phase 9.

---

## Real bug surfaced and fixed during §4 implementation

`build_op_template_scope_kernel` was emitting an explicit
`extern <name>: <type>` line for every parent-output name the
op referenced. For `cycle` — which is the parent's *Coordinate*
input — that explicit declaration forced the inner kernel to
classify `cycle` as `IterationExtern` (the kind for declared
externs without a default), giving the inner kernel
`coord_count == 0`. `FiberBuilder::set_inputs` skips
propagation for kernels with `coord_count == 0`, so the
op-template kernel's `cycle` slot stayed at `Value::None` for
every cycle and any reference (`mod(cycle, 2)`, `add(cycle,
1)`, etc.) that flowed through it either panicked
("expected U64, got U64", because `Value::None.port_type()`
reports U64 as a placeholder) or evaluated against the
default — silently wrong values.

The synthetic-metrics tests didn't surface this because their
op-templates' parents (the workload root kernel) had no inputs
at all — `cycle` wasn't in the manifest, so the cascade fell
through and let the inner kernel auto-extern `cycle` as
`Coordinate`. The bug only fires when the parent kernel has
`cycle` as a coord input, which is the typical phase-scope
shape.

**Fix:** the cascade now skips the explicit `extern` emit for
any parent-input that's classified as `Coordinate`, leaving
the inner kernel's auto-extern path to re-classify it as
`Coordinate` too — matching the parent's input kind so
`set_inputs` propagates per cycle. The name still goes into
`inherited_names` so `mark_inherited_outputs` covers it; the
behaviour at runtime is identical except that propagation now
works.

(Landed in the same commit as §4; `nbrs-activity/src/scope.rs`
`build_op_template_scope_kernel`.)

### Open: adapter bind-point rendering under op-template scope

While verifying the §4 conditional smoke test, I observed
that `stmt: "ran cycle={cycle}"` rendered `ran cycle=0` on
every execution even though the conditional wrapper (reading
`local_pred = mod(cycle, 2)` from the same op-template
kernel) was firing on the correct truthy cycles. The
op-template kernel's `cycle` propagates per cycle (the
conditional proves it), but the adapter's `{cycle}` bind-
point resolution appears to read from a different state —
likely the main fiber kernel via the
`resolve_with_field_pulls` path rather than
`resolve_pulls_for_op`. The §4 test was rewritten to assert
via the metrics.db `cycles_total` / `skips_total` counters
rather than parsing stdout, so the test passes; the
bind-point routing question is deferred to a follow-up
"adapter pulls under Phase 9" memo.
