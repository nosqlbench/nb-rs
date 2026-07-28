# Metric dimensions from query data

*Proposal — not yet an SRD. Written 2026-07-28.*

## What we want

A workload reads rows whose values name instances of a dimension — a compaction
tier, a keyspace, a node — and wants one series per instance of a single
measurement:

```
compaction_bytes_out{tier="24"}
compaction_bytes_out{tier="25"}
```

Today the label values a metric can carry are fixed at component-attach time
(`component.effective_labels()`, read in `wrappers/metrics.rs:277`). Nothing in
`MetricSpec` (`nbrs-workload/src/model.rs:2234`: `value`, `family`, `kind`,
`unit`, `format`) can bind a label to captured data.

## What already works, and is not the problem

Verified on 2026-07-28, so the design does not need to solve any of it:

- `Str` is a first-class `PortType` (`polydat/src/ast.rs:1036`).
- Nodes take **runtime** (non-`Const`) string arguments — `str_eq(a: &str, b:
  &str)` (`polydat/src/library/compare.rs:118`) is a real node over two String
  wires.
- A query's text column captures into a `Str` wire and flows into a node
  argument. Verified end to end against a live node: `extern kname: str = ""` +
  `capture: kname: "/0/keyspace_name"` → `str_eq(kname, "system")` → `1`, with
  the negative comparison returning `0`.
- Storage already models one instance per label set — `metric_instance` +
  `instance_label` (`nbrs-metrics/src/reporters/sqlite.rs:368,400`).

The metrics/metricsql nodes use `Const<&str>` **by choice** — they pre-parse the
pattern via `#[poly_const(...)]` — not because polydat lacks runtime strings.

## The invariant this must not weaken

`Component::register_instrument_with_unit` (`nbrs-metrics/src/component.rs:232`)
rejects a second registration of the same family on one component:

> The component's `effective_labels` define the dimensional cell; the same
> family on a different component is a different cell and produces no collision.

That is metric identity, not an obstacle. **A per-value series is a per-value
cell** — a child component carrying the dimension value, with the family
registered exactly once on it. The uniqueness check is untouched and keeps
catching the error it exists to catch: two different instruments claiming one
name in one dimensional context.

`Component::attach` (`component.rs:805`) enforces the other half:

> component label-ownership violation: child re-declares label `{k}` already
> owned by an ancestor. Each label name must be set on exactly one tier and
> inherited downward.

So a dimension **name** is a structural declaration made at exactly one tier;
only the **value** varies per cell.

### The two legitimate patterns

| pattern | mechanism in tree today | correct when |
|---|---|---|
| new family, same cell | `errors.<type>` — `Counter::new(labels.with("name", format!("errors.{error_name}")))` (`activity.rs:566`) | the values name **different measurements** |
| same family, new cell | one component per `k` / `profile` sweep value | the values are **instances of one dimension** |

This proposal is the second. `compaction_bytes_out` is one measurement and
`tier` is a dimension; `compaction_bytes_out.tier24` would be the first pattern
misapplied.

### Why not `DynamicCapture`

`DynamicCapture` (`component.rs:102`) emits samples straight into the
`MetricSet` from `capture_into`, carrying labels on the instrument itself. It is
how per-error counters reach the wire — and it **bypasses the component registry
entirely**, which is how it sidesteps cell identity.

That is acceptable for an engine-internal concern with a fixed shape. As the
general mechanism for workload-authored dimensions it would put an unbounded,
uncheckable namespace outside the type system: label sets assembled by runtime
string formatting, invisible to the compiler, unvalidated. It is the "not
reified" design, and it is the argument against itself.

## Design

### 1. Declaration — the dimension is compiled matter

A dimension is declared once, at the tier that owns the label name, beside
`bindings:` / `metrics:`:

```yaml
finalize_index:
  dimensions:
    tier: { type: str }
```

The declaration names the label key and types its value wire. It does **not**
enumerate values — those come from data.

### 2. Placement — a metric names the cell it lands in

`MetricSpec` grows one optional field, `cell:`, mapping dimension name to an
**expression** (same grammar as `value:`, not a bare wire name):

```yaml
ops:
  read_compaction_history:
    capture:
      tier_name: "/0/tier"
    metrics:
      compaction_bytes_out:
        kind: gauge
        value: history_bytes_out
        cell: { tier: tier_name }
```

Read as: *this measurement, in the cell where `tier` = the captured value.*

### 3. Compiled representation — the reification

Scope synthesis emits a typed binding per cell coordinate, exactly as phase
metrics already emit `volatile __metric_<name> := <expr>`
(`nbrs-runtime/src/scope.rs`):

```
volatile __cell_tier := tier_name       # typed Str by the declaration
```

So a cell coordinate is a **compiled kernel output**, type-checked and pulled
through the normal wire path — the same standing as a metric value. The
program's manifest carries the declared dimension names, which is what makes
every check below possible.

### 4. Runtime

At attach time the declaring tier's component records the dimension **name**
(no values).

Per cycle, the metrics wrapper pulls `__cell_<dim>` alongside
`__metric_<name>`, then:

1. resolve-or-create the child cell for that value — memoized, so the same value
   maps to the same cell for the life of the activity;
2. on first materialization only, register the family on the new cell — the
   uniqueness check runs there, per cell, which is where it belongs;
3. steady state: hash lookup on the value → cell → instrument handle →
   `gauge.set()`. No registry write and no component write lock after first
   sight.

The cache is per-activity on the wrapper, not global. Cell resolution must
target the **fiber's own** component, reusing `control_set`'s resolution path
(`runtime_context.rs:195` — its comment flags that as load-bearing: a write must
hit the component a subsequent read sees).

### 5. Compile-time checks

Because the dimension is in the program, validation moves ahead of execution —
the same class of check that just caught the per-op program bug in
`validate_bind_points` (`synthesis.rs:354`):

1. a metric's `cell:` names a dimension declared at some enclosing tier;
2. a dimension name is declared at exactly **one** tier in the chain — this
   turns `Component::attach`'s runtime **panic** into a compile error;
3. the cell expression types as `Str`, by ordinary polydat type checking once
   the binding is synthesized.

### 6. Report side

No change. The value lands in `instance_label` exactly as a sweep label does, so
`group_by: tier` works as it does for `k` / `profile`. The manifest additionally
lets a report distinguish *"dimension declared, no data yet"* from *"typo"* —
today a `group_by:` over a label that never materialized silently yields
nothing.

## Explicitly not changing

- **`register_instrument_with_unit` keeps rejecting duplicate families per
  cell.** No intern-or-return, no relaxation.
- **No cardinality cap.** The number of series is a modeling decision the author
  makes by declaring a dimension, exactly as a sweep over `k` is. A cap would
  also silently discard data.
- **`DynamicCapture` stays** for engine-internal dynamic metrics.

## Open questions

1. **Cell lifetime.** Under the phase component (dies with the phase) or the
   activity/session (persists across phases)? Recommend phase-scoped, matching
   where the values are produced — but that ends a tier's series when its phase
   ends, which changes what a trailing report query sees.
2. **Multi-dimension cells.** `cell: {a: x, b: y}` should resolve to **one**
   child carrying the whole coordinate rather than nested children, since
   nesting would impose an arbitrary order on co-equal dimensions.
3. **Which tiers may declare.** Phase-level is clearly needed. Op-level
   declaration would let one op own a dimension its siblings never see — useful,
   or a footgun?

## Unverified

The pieces are each proven (above), but the synthesized `__cell_<dim>` form
specifically has not been compiled yet — that is the first thing to build and
the first place this design could be wrong.
