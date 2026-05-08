# SRD-32a — Op Wrapper Registry, Field Ownership, and Stacking Order

**Status:** Pushes 1–4 shipped 2026-05-07, including
`--wrap-order` and `--wrap-default-order` CLI flags
(env-equivalent `NBRS_WRAP_ORDER` / `NBRS_WRAP_DEFAULT_ORDER`
auto-derived per SRD-04). Registry + resolver + cascade
replacement + Info-level assignment logging (Push 1);
parse-time field ownership / misplaced-field guard (Push 2);
workload-root + per-op + CLI order overrides (Push 3);
`nbrs describe wrappers` / `nbrs describe op` (Push 4).
Refinement of SRD-32.
**Owner:** runtime / executor / wrappers
**Implementation target:** `nbrs-activity/src/wrappers.rs` (registry
  surface), `nbrs-activity/src/wrappers/registry.rs` (new),
  `nbrs-activity/src/activity.rs` (composition loop),
  `nbrs-workload/src/model.rs` (field-ownership routing)
**Cross-refs:** SRD-32 (dispenser wrappers — load-bearing),
  SRD-30 (adapter interface — core/adapter field split),
  SRD-31 (op pipeline), SRD-13d Phase 9 (op-template scope)

---

## What this SRD refines

SRD-32 specifies *how a wrapper plugs in* — the `OpDispenser`
trait, the scope-init `ScopeFixture` registration contract, the
per-cycle pull path. That part is sound. What SRD-32 leaves
implicit is **which wrappers exist**, **which op-template fields
each wrapper owns**, and **what order they stack in**. Today
those facts live as inline if-let chains inside
`activity.rs::executor_task`, with the order baked into the
sequence of `let varname = if … wrap … else …` clauses:

```rust
let traversed   = TraversingDispenser::wrap(adapter_dispenser, …);
let throttled   = ThrottleDispenser::wrap(traversed, …);
let validated   = ValidatingDispenser::wrap(throttled, …);
let polled      = PollingDispenser::wrap(validated, …);
let conditional = ConditionalDispenser::wrap(polled, …);
let emitted     = EmitDispenser::wrap(conditional, …);
let with_result = ResultDispenser::wrap(emitted, …);
let final_disp  = MetricsDispenser::wrap(with_result, …);
```

Three problems:

1. **Field ownership is ambient.** Adding a new template field
   (e.g. `verify:`, `poll_interval_ms`, `if:`) means scattering
   `template.params.get("…")` calls across whichever wrapper
   handles it. There's no single source of truth saying "this
   field belongs to this wrapper" — the parser permits any
   field on any op, and the wrapper that happens to look it up
   first wins.
2. **Stacking order is invisible.** Operators reading the YAML
   can't tell from the workload alone that polling sits inside
   `if:`, that validation runs before polling, or that
   `MetricsDispenser` is outermost. A change like
   "swap polling and conditional" (the recent fix) is a code
   change, not a config knob.
3. **Order is non-discoverable.** A user who wants
   "throttle outside the conditional" has no surface to
   configure that, and no way to inspect the current order
   from `nbrs describe`.

This SRD specifies a **wrapper registry** that names every
wrapper, declares the op-template fields each wrapper owns,
codifies the default stacking order as a numeric rank, and
opens a workload-level / CLI knob for users to override it.
The default order matches today's behaviour exactly; the
registry just makes it explicit and configurable.

The load-bearing rule:

> **Every op wrapper has a name, owns a closed set of
> op-template fields, and applies at a registered rank. The
> stack is the rank-sorted projection of every wrapper whose
> trigger fires. Override is by name.**

---

## Vocabulary

- **Wrapper.** A `dyn OpDispenser` implementation that wraps
  an inner dispenser, adding a behaviour (validation,
  polling, conditional gating, …). Synonymous with SRD-32's
  "decorator."
- **Trigger.** A predicate over an op template's fields that
  decides whether the wrapper applies to that op. Typically
  "presence of a marker field" (`if`/`poll`/`emit`) but can
  be richer (e.g. `verify` + nested predicates).
- **Owned field.** An op-template field whose only legitimate
  consumer is one specific wrapper. Misplaced fields (e.g.
  `poll_interval_ms` on an op that has no `poll:`) are caught
  by the registry's parse-time check rather than silently
  ignored.
- **Constraint.** A typed relationship a wrapper declares
  against other named wrappers, expressed in terms of
  composition position rather than absolute rank:
  - **`requires_inner: [name, …]`** — these wrappers MUST
    be present and composed inside this one (closer to the
    adapter). If they aren't already triggered by the op
    template, they auto-activate as a *transitive
    activation* of this wrapper. Models the
    `validate`-needs-`traverse` shape: if `validate`
    triggers (because the op declared a `verify:` field),
    `traverse` is pulled in even though no traverse field
    was declared.
  - **`forbids_outer: [name, …]`** — these wrappers MUST
    NOT be composed outside this one. Hard error at
    resolve time when the constraint graph would allow
    one to slip through.
  - **`mutually_exclusive_with: [name, …]`** — at most one
    of this wrapper and any name in the list may be
    triggered for a given op. Both triggering is a hard
    error.
- **Transitive activation.** When a wrapper triggers, the
  closure of `requires_inner` is computed and every
  wrapper in the closure is also marked triggered. The
  user doesn't have to know that asking for `validate`
  implies `traverse` — the constraint graph encodes that.
- **Loose contract.** The constraint graph specifies
  *position* and *presence*. It does NOT type-check the
  data flowing between wrappers. If `validate` reads a
  field that `traverse` is supposed to populate but the
  shape mismatches, that's a wrapper-author bug, not a
  framework error. The framework guarantees `traverse`
  exists and is inside `validate`; the wrapper authors
  agree what `traverse` puts in `ResolvedFields` /
  `ScopeFixture` for `validate` to consume.
---

## Wrapper registry

One entry per wrapper, consolidated in
`nbrs-activity/src/wrappers/registry.rs`. The registry is the
single source of truth for "what wrappers exist, which fields
they own, when they apply, and where they stack."

```rust
/// Stable identifier for a wrapper. Used in workload override
/// directives, CLI flags, and registry lookups.
pub struct WrapperName(&'static str);

pub struct WrapperRegistration {
    /// Stable name (`"validate"`, `"poll"`, `"throttle"`,
    /// `"if"`, `"emit"`, `"result"`, `"metrics"`).
    pub name: WrapperName,
    /// Op-template field names this wrapper exclusively
    /// owns. Listed for parse-time validation: a misplaced
    /// field like `poll_interval_ms: 5000` on an op without
    /// `poll:` becomes a hard error pointing at THIS
    /// registration, not an opaque "unknown param".
    pub owned_fields: &'static [&'static str],
    /// Predicate over the op template: "does this wrapper
    /// apply to this op?" Default: any owned field present.
    /// Wrappers with no owned fields (e.g. `result`, which
    /// applies whenever the op has any `result:` wires)
    /// override this.
    pub triggers: fn(&ParsedOp) -> bool,

    // ── Constraint surface — see "Vocabulary" ───────────
    /// Wrappers that MUST sit inside this one (closer to
    /// the adapter, called *after* this one per cycle).
    /// Activates transitively: triggering `validate` pulls
    /// in `traverse` whether or not a traverse field was
    /// declared.
    pub requires_inner: &'static [WrapperName],
    /// Wrappers that MUST NOT sit outside this one. Hard
    /// error when the constraint graph permits any of the
    /// listed wrappers to wrap this one.
    pub forbids_outer: &'static [WrapperName],
    /// Wrappers that cannot coexist with this one on a
    /// given op. Triggering both is a hard error.
    pub mutually_exclusive_with: &'static [WrapperName],

    /// Factory that builds the wrapped dispenser given the
    /// inner one and the per-op context.
    pub wrap: fn(WrapperBuildCtx<'_>) -> Result<WrapperOutput, String>,
}

inventory::collect!(WrapperRegistration);
```

There is no `rank` field. Composition order is the
topological sort of the constraint graph (see
§"Wrapper Resolver"); when two wrappers have no relative
constraint, their order is determined by the session-level
default-order configuration (§"Default ordering"), not by an
implicit numeric rank. The graph encodes the load-bearing
relationships explicitly; the session config supplies the
tiebreaker for the rest.

`WrapperBuildCtx` carries the inner `Box<dyn OpDispenser>`,
the parsed op template, the kernel program, the activity
labels, and the mutable `ScopeFixture` (per SRD-32). The
return type bundles the wrapped dispenser plus any
"side-products" (validation metrics handle, polling-metrics
handle) that the executor needs to retain.

---

## Default registry contents

| name | owned fields | trigger | requires_inner | forbids_outer | mutually_exclusive_with |
|------|--------------|---------|----------------|---------------|-------------------------|
| `traverse` | (none) | always | — | — | — |
| `throttle` | `rate`, `rate_limiter` | rate set | `traverse` | — | — |
| `validate` | `verify`, `relevancy`, `strict` | any owned field present | `traverse` | — | — |
| `poll` | `poll`, `poll_interval_ms`, `timeout_ms`, `poll_max_error_retries`, `poll_metric_name` | `poll:` set | `traverse` | — | — |
| `if` | `if` | `if:` set | — | — | — |
| `emit` | `emit` | `emit: true` | — | — | — |
| `result` | (none — reads `result:` wires) | template has `result:` wires | `traverse` | — | — |
| `metrics` | (none) | always | — | (everything) | — |

Read the table relationally:

- **`metrics`** declares `forbids_outer: <every other
  wrapper>`, which is the registry's way of saying it MUST
  be outermost. The validator surfaces this as "no wrapper
  may be composed outside `metrics`" rather than as a
  special-case "must_be_outermost: true" boolean — same
  semantic, expressed in the constraint vocabulary so the
  override machinery can reason about it uniformly.
- **`traverse`** has no constraints of its own, but every
  wrapper that depends on op-field values declares
  `requires_inner: ["traverse"]`. The graph computes
  "traverse must be innermost" as a *consequence* of
  every consumer asking for it, rather than as a
  hand-coded "must_be_innermost: true". A future wrapper
  that doesn't need traverse can sit inside it without
  surgery on traverse's record.
- **`emit`** declares no `requires_inner`. The cascade
  composes `result` *outside* `emit` (innermost-first list
  ends `..., emit, result, metrics`), so an
  `emit.requires_inner = [result]` declaration would
  invert the cascade and break the byte-identical-output
  bar in §"Migration". `result` is always-on (wraps
  unconditionally, no-op when its wire map is empty), so
  emit can rely on its presence without an explicit
  constraint. Pre-`result` emit (raw adapter output) is
  reserved as a future variant (`emit.raw`) if needed.
- **`if`** has no `requires_inner`. A false condition
  short-circuits before any inner wrapper fires; that's
  load-bearing for the recent fix that pulled `poll` to
  the inside of `if`. The constraint says "I don't depend
  on anyone being inside me"; the topological resolver
  combined with the session-level default order places
  `if` outside `poll` by default.

Adding a new wrapper is a single `inventory::submit!` block:
declare the name, the owned fields, the trigger, and the
relationships. No rank to negotiate, no renumbering to
ripple through.

---

## Field ownership and parse-time validation

The workload parser (SRD-30 §"Core-first field processing")
already separates *core* fields from *adapter* fields. With
this registry, **core fields further split into per-wrapper
field families**:

```text
op template
├── adapter fields     → DriverAdapter::map_op (SRD-30)
└── core fields
    ├── traverse fields → unowned (always-on)
    ├── throttle fields → ThrottleDispenser
    ├── validate fields → ValidatingDispenser
    ├── poll fields     → PollingDispenser
    ├── if fields       → ConditionalDispenser
    ├── emit fields     → EmitDispenser
    ├── result fields   → ResultDispenser
    └── metrics fields  → MetricsDispenser
```

The parser walks every op template's core fields and routes
each to its owning wrapper based on the registry. Two new
parse-time errors land:

- **Unknown field.** A field that no wrapper owns and no
  adapter declared. The diagnostic names the closest match
  (Levenshtein-1) so a typo like `poll_interval` (missing
  `_ms`) is caught at parse, not as a silent default at
  runtime.
- **Misplaced field.** An owned field whose owning wrapper
  isn't triggered. Today
  `poll_interval_ms: 5000` on an op without `poll:` is
  silently ignored. Under this SRD, the parser errors with
  "field `poll_interval_ms` is owned by wrapper `poll`, but
  the trigger field `poll:` is absent — either remove
  `poll_interval_ms` or add `poll:`."

  Operators can quiet this with `--strict-fields=warn` if
  they have a workload that legitimately leaves
  ghost-knobs around (rare; mostly during a refactor).

---

## Wrapper Resolver

A dedicated trait, `WrapperResolver`, owns the logic that
turns a parsed op template + the wrapper registry + the
session-level default order into a concrete composition
plan. This is the single place where wrapper composition
happens; the executor calls it once per op template at
phase init and applies the returned plan.

```rust
/// Resolves a `WrapperPlan` for a parsed op template,
/// honouring registry constraints, transitive activation,
/// and the session-level default ordering. Implementations
/// are stateless apart from the session config they hold;
/// repeated calls with the same input produce identical
/// plans.
pub trait WrapperResolver: Send + Sync + 'static {
    /// Resolve the wrapper composition for one op template.
    fn resolve(
        &self,
        template: &nbrs_workload::model::ParsedOp,
        registry: &WrapperRegistry,
    ) -> Result<WrapperPlan, ResolveError>;
}

pub struct WrapperPlan {
    /// Wrappers in composition order — innermost first
    /// (built first; called last per cycle), outermost
    /// last. The executor applies them in this order to
    /// construct the final dispenser chain.
    pub stack: Vec<&'static WrapperRegistration>,
    /// Diagnostic record of which wrappers triggered
    /// directly vs. via transitive activation. Used by
    /// `nbrs describe op` to explain why each wrapper
    /// is present.
    pub provenance: Vec<WrapperActivation>,
}

pub enum WrapperActivation {
    /// Triggered directly by an owned field on the op
    /// template (e.g. `validate` because `verify:` was
    /// declared).
    OwnedField { wrapper: WrapperName, field: &'static str },
    /// Pulled in transitively by another wrapper's
    /// `requires_inner`. Captures the chain so the
    /// operator can see "validate triggered → traverse
    /// transitively activated".
    TransitiveFrom { wrapper: WrapperName, requested_by: WrapperName },
    /// Always-on wrapper (e.g. `metrics`).
    AlwaysOn { wrapper: WrapperName },
}

pub enum ResolveError {
    /// `forbids_outer` violation — wrapper A declared B
    /// must not wrap it, but the resolved order placed B
    /// outside A.
    ForbiddenOuter { inner: WrapperName, outer: WrapperName },
    /// `mutually_exclusive_with` violation — both
    /// triggered for the same op.
    MutuallyExclusive { a: WrapperName, b: WrapperName,
                        a_reason: WrapperActivation,
                        b_reason: WrapperActivation },
    /// Constraint graph contains a cycle (`A.requires_inner
    /// = [B]` and `B.requires_inner = [A]`). Almost always
    /// a registry-author bug; surface immediately at
    /// session start, not at op-resolve time.
    ConstraintCycle { cycle: Vec<WrapperName> },
    /// Override referenced an unknown wrapper name. Surfaces
    /// the closest registered name as a typo suggestion.
    UnknownWrapper { name: String, suggestion: Option<&'static str> },
}
```

### Algorithm

The resolver runs four passes in order:

1. **Trigger fan-out.** Collect every wrapper whose
   `triggers(template)` returns true; tag each with
   `OwnedField` provenance.
2. **Transitive closure.** For every triggered wrapper,
   walk `requires_inner` and add any wrapper not already
   triggered, tagging the addition with `TransitiveFrom`
   provenance. Repeat until no new wrappers are added.
   `requires_inner` is finite per registration; the closure
   converges in O(N) over total wrappers.
3. **Constraint validation.** Check
   `mutually_exclusive_with` (any triggered pair where
   both names appear → `MutuallyExclusive` error) and
   detect cycles in `requires_inner` over the triggered
   set (DFS with grey/black coloring →
   `ConstraintCycle` error).
4. **Topological order.** Sort the triggered set so
   `requires_inner` comes before its requirer (innermost
   first). When the partial order leaves multiple valid
   orders, the session-level default-order list breaks
   ties (§"Default ordering"). After sorting, scan for
   `forbids_outer` violations — for each wrapper, walk
   the items above it in the list and reject if any are
   in its `forbids_outer` set.

Total cost: O(N²) where N is the number of triggered
wrappers (typically ≤ 8). Negligible per op template.

### Resolver invariants

- **Determinism.** Same input → same plan. The session-
  level default order is the only tiebreaker for
  unconstrained pairs.
- **Diagnostic completeness.** Every wrapper in the plan
  carries a `WrapperActivation` so `nbrs describe op` can
  explain the presence of any wrapper without re-running
  the resolver.
- **Single responsibility.** The resolver does NOT build
  dispensers. It produces a `WrapperPlan`; the executor
  iterates the plan's `stack` and calls each registration's
  `wrap` factory in order. This keeps the resolver
  testable in isolation against synthetic registry
  fixtures.

---

## Default ordering

The default order is a session-component-scoped configuration:
an explicit list of wrapper names in innermost-to-outermost
order, used by the resolver as a tiebreaker when constraints
leave a choice. The runtime ships a built-in default that
matches today's behaviour:

```
[traverse, throttle, validate, poll, if, emit, result, metrics]
```

This list is loaded onto the session component at startup
under the same path that holds dynamic controls and
component metadata (SRD-19 / SRD-23). The user can override
it via:

- `--wrap-default-order <list>` on the CLI (SRD-04 umbrella)
- `wrappers: { default_order: [...] }` at workload root
- `NBRS_WRAP_DEFAULT_ORDER=<list>` env

The override is **validated against the constraint graph
at startup** before any phase runs:

- Every name in the list must be a registered wrapper.
  Unknown names → hard error with a typo suggestion.
- The list MUST be consistent with every registration's
  `requires_inner` and `forbids_outer`. A reordered list
  that, say, places `metrics` before `result` violates
  metrics' implicit "outermost" constraint and is rejected
  immediately at session start with an explanation:

  ```
  error: wrap-default-order is inconsistent with the
  wrapper registry:
    'metrics' forbids 'result' from being outside it,
    but the configured order places 'result' after 'metrics'.
  Suggested fix: ensure 'metrics' is the last entry in the
  list, e.g.
    [traverse, throttle, validate, poll, if, emit, result, metrics]
  ```

The validator runs against the registry's constraint graph,
not against any specific op template — so the configured
order is checked once per session, regardless of how many
ops eventually trigger which wrappers.

---

## Composition — default and override

### Default

The resolver runs the four-pass algorithm against each op
template, using the session-level default order as the
tiebreaker. The default-order shipped today produces the
same chain as the current hand-rolled if-let cascade:

```text
Adapter base
  └─ traverse (always)
     └─ throttle  (when rate set)
        └─ validate (when verify/relevancy)
           └─ poll      (when poll: set)
              └─ if      (when if: set)
                 └─ emit  (when emit: true)
                    └─ result  (always; no-op without wires)
                       └─ metrics (always)
```

Read top-to-bottom for execute-time call order: `metrics` is
called first per cycle, runs its work, calls `result`, …
`adapter base` last.

### Workload-level override

A workload may set a `wrappers:` directive at the workload
root or at an op-template level:

```yaml
# Workload root: applies to every op template.
wrappers:
  order: [traverse, throttle, validate, poll, if, emit, result, metrics]

phases:
  await_index:
    ops:
      indexes_built_cass:
        # Per-op override: pin this op's order, ignore root.
        wrappers:
          order: [traverse, validate, poll, if, metrics]
        if: "cql_dialect == 'cass5'"
        poll: await_empty
        verify: …
```

Rules:

- The `order:` list MUST be a permutation of the wrappers
  triggered on this op (after transitive activation).
  Listing a wrapper whose trigger doesn't fire is a hard
  error (silently dropping it would mask typos). Omitting
  one whose trigger does fire is a hard error (skipping a
  wrapper changes semantics).
- The override MUST satisfy every registered constraint
  (`requires_inner`, `forbids_outer`, `mutually_exclusive_with`)
  for the triggered set. The resolver checks each
  constraint against the override exactly as it does
  against the default order — same code path, same error
  shapes.
- Per-op `wrappers:` shadows root-level entirely; no
  cascading merge. Mixing styles is intentional — operators
  that want one global override use root-level; those that
  want pinpoint control use per-op.
- The list is `[innermost, …, outermost]`.

### CLI override

```
--wrap-order <list>
NBRS_WRAP_ORDER=<list>
```

Comma-separated. Applies to every op template in the run.
Same validation as workload-root — must be a permutation of
the triggered wrappers. A workload-level or per-op override
takes precedence over CLI (config-locality wins; see SRD-04
Rule 5).

---

## Lifecycle and data bubbling

The wrapper trait surface has THREE lifecycle phases. All
three are part of the wrapper contract; wrapper authors that
override one are expected to know how the other two compose
in the surrounding chain.

### 1. Build (init time)

When the resolver's `WrapperPlan` is applied, each
registration's `wrap` factory runs in innermost-to-outermost
order. The factory:

- Receives the inner `Box<dyn OpDispenser>` plus the
  per-op `WrapperBuildCtx` (parsed template, kernel
  program, activity labels, mutable `ScopeFixture`).
- Self-registers any name dependencies into the
  `ScopeFixture` per SRD-32 §"Init-Time Fixture and
  Consumer Self-Registration".
- Returns the wrapped dispenser. May also return
  side-products (validation metrics handle, polling
  metrics handle) the executor needs to retain.

**Build-time invariant:** the factory MUST NOT pull GK
values, fire any I/O, or assume the inner dispenser has
been "warmed". It composes a layered closure; the actual
work happens in phase 2.

### 2. Per-cycle invoke (cycle time)

The executor calls `dispenser.execute(cycle, &ExecCtx)` on
the outermost wrapper. Each wrapper:

- Decides whether to delegate inward (call the inner
  dispenser's `execute`) or short-circuit. `if` short-
  circuits when its condition is false; `validate` always
  delegates and inspects the result; `poll` may delegate
  many times in a loop.
- Receives `ExecCtx` (per SRD-32) carrying the resolved
  field set and the wrapper-pull view. Outer wrappers
  may extend `ExecCtx` for inner wrappers; the
  extension contract is per-wrapper-pair.
- Returns a `Result<OpResult, ExecutionError>` to its
  caller (the wrapper above it, or the executor for the
  outermost).

**Per-cycle invariant:** every wrapper's `execute` MUST
either return a result (success, skip, or failure) or
propagate the inner result unchanged. Wrappers that need
to *replace* the inner result (e.g. `result` capturing
fields, `emit` adding side-effects) do so by mutating the
returned `OpResult` before returning it upward.

### 3. Side-effect emission (during invoke)

A wrapper may produce side effects that flow OUTWARD to
later wrappers and the surrounding runtime:

- **Captures.** `traverse` populates `OpResult.captures`
  from declared `result:` wires. `result` consumes those
  captures and exposes them to the GK scope so subsequent
  wrappers (and the next cycle's bind plan) can read them.
- **Metrics.** `validate` writes to its `ValidationMetrics`
  handle (count of passes / failures). `poll` writes
  poll-attempt counts. These flow to the activity-level
  metrics scheduler via the Arc handles returned at build
  time.
- **Stop signals.** A wrapper that wants to halt the
  phase (validation strict-mode failure, polling timeout)
  returns `ExecutionError::Op` with a `should_stop`
  payload; the activity loop captures that and surfaces
  it as the phase's stop reason (per the recent
  diagnostic enrichment).

**Bubbling rule (outward):** every side-effect a wrapper
produces is observable to wrappers above it via the
`OpResult` it returns or via shared Arc handles wired at
build time. The wrapper above MUST NOT poke into a wrapper
below by name; the data path is the typed return value.

**Bubbling rule (inward):** a wrapper above passes context
DOWN by extending `ExecCtx` (typed extensions, not generic
type-erased blobs). When `validate` needs to know about
relevancy ground-truth, it pulls that into `ExecCtx` from
the `ScopeFixture` and the inner adapter dispenser sees
the extended view. Inner wrappers do NOT shop for values
they weren't given.

### Why this matters for constraint design

The `requires_inner` constraint is what authorises
inward-bubbling assumptions. When `validate` declares
`requires_inner: ["traverse"]`, it's telling the resolver
"I depend on `traverse` having populated
`OpResult.captures` before my `execute` runs." The
constraint graph guarantees `traverse` is composed inside
`validate`; the wrapper authors have already agreed
*what* `traverse` puts where. The framework doesn't type-
check the data shape — it ensures the wrappers that have
agreed to a contract are actually present and ordered
correctly.

A wrapper that wants to extend `ExecCtx` for inner wrappers
declares it in its design notes; consumers of the
extension declare `requires_inner` on the producer and
agree on the typed extension type. The constraint graph
is the load-bearing presence/order guarantee; the typed
extension is the data contract that rides on top.

---

## Discoverability

### `nbrs describe wrappers`

```
$ nbrs describe wrappers
NAME       RANK  OWNED FIELDS                                                                        TRIGGER
traverse   -100  (none — always-on)                                                                  always
throttle      0  rate, rate_limiter                                                                  rate set
validate    100  verify, relevancy, strict                                                           verify/relevancy
poll        200  poll, poll_interval_ms, timeout_ms, poll_max_error_retries, poll_metric_name        poll: set
if          300  if                                                                                  if: set
emit        400  emit                                                                                emit: true
result      500  (none — reads result: wires)                                                        always (no-op when empty)
metrics     600  (none — applies to every op)                                                        always
```

### `nbrs describe op <workload> <op>`

Renders the actual stack that *will* be applied to the op
under the current workload + CLI override:

```
$ nbrs describe op full_cql_vector.yaml indexes_built_cass
op 'indexes_built_cass' (phase: await_index)
  wrapper stack (innermost → outermost):
    1. traverse
    2. validate (verify: min_rows ≥ 1)
    3. poll (interval=5000ms, timeout=600000ms)
    4. if (cql_dialect == 'cass5')
    5. metrics
```

### Workload validation

`nbrs check <workload>` walks every op template and validates
field ownership per the registry. Misplaced or unknown fields
are reported with file/line locations, same shape as adapter-
field validation today.

---

## Worked examples — what the constraint graph rejects

The constraint vocabulary covers every previous "must be
outermost / innermost / before / after" rule expressed in
terms of the same uniform machinery. A few examples make
the shape concrete:

- **`metrics` outermost.** The registration declares
  `forbids_outer: <every other registered wrapper>`. Any
  override that places another wrapper outside `metrics`
  is rejected by the resolver's `forbids_outer` pass with
  a message naming the violating wrapper.
- **`traverse` innermost (consequence, not declaration).**
  Most other wrappers declare `requires_inner:
  ["traverse"]`. The topological sort places `traverse`
  before any of them. There's no special "must be
  innermost" flag — innermost-ness emerges from the
  pattern of `requires_inner` declarations on the
  consumers.
- **`if` outside `poll` (default, not constraint).**
  Neither `if` nor `poll` declares a constraint about the
  other; they're independently triggered. The session-
  level default-order list places `if` after `poll` (in
  innermost-to-outermost order), which the resolver
  honours as the tiebreaker. A workload that wants
  `poll` outside `if` (poll until a runtime condition
  flips) overrides per-op with `wrappers: { order:
  [traverse, if, poll, …] }`. Both orders are valid
  topologically; the override picks.
- **`validate` outside `throttle` (default, not
  constraint).** Same shape — independent triggers, the
  default order is the tiebreaker, and the override
  surface lets users invert it.
- **Hypothetical `validate` + `validate.strict-only`
  conflict.** If a future variant `validate.strict-only`
  is introduced that conflicts with the regular
  `validate`, it declares `mutually_exclusive_with:
  ["validate"]`. An op template that triggers both is
  rejected at resolve time with both activations cited in
  the error.

The system gives wrappers narrowly-scoped functionality
that rides on top of others: a future `result.compact`
might require `result` and `traverse`, refining what
gets persisted; a `metrics.histogram-detail` might
require `metrics` and contribute extra label dimensions.
The constraint graph captures these layered relationships
without growing the special-case validator surface.

---

## Migration

### Push 1 — registry surface + resolver, no behaviour change

- Add `WrapperRegistration` struct + `inventory::collect`.
- Submit one entry per existing wrapper with its
  constraint declarations (`requires_inner`,
  `forbids_outer`, `mutually_exclusive_with`).
- Implement the `WrapperResolver` trait with the four-pass
  algorithm (trigger fan-out, transitive closure,
  constraint validation, topological order with default-
  order tiebreaker).
- Add the session-component `wrap-default-order` config
  with the built-in default list. Validate the loaded
  default against the constraint graph at startup.
- Replace the hand-rolled if-let cascade in
  `activity.rs::executor_task` with a resolver call +
  plan application. Confirm byte-identical output via
  existing test suite.

### Push 2 — parse-time field ownership

- Walk every op template at workload load. Route fields to
  owners; error on unknown / misplaced.
- Existing workloads that pass today should continue to pass
  (every field already has a home, just implicit). Workloads
  that have *latent* misplacements (e.g. `poll_interval_ms`
  on an op without `poll:`) get a clean diagnostic instead
  of silent ignore.
- Add `--strict-fields={error|warn|off}` for the migration
  window. Default `error`; mass-rename / refactor sessions
  can drop to `warn`.

### Push 3 — workload + CLI overrides

- Parse `wrappers: { order: […] }` at workload root and
  per-op.
- Add `--wrap-order` CLI flag. SRD-04 umbrella precedence.
- Validate permutation rules (no missing / extra wrappers
  vs. triggered set).

### Push 4 — discoverability

- `nbrs describe wrappers` — registry dump.
- `nbrs describe op <workload> <op>` — effective stack
  rendering, including overrides.
- `nbrs check <workload>` — walk every op, validate field
  ownership and order.

---

## Composition telemetry

Wrapper composition is an **init-time concern, decided
once per op template**. The resolver runs at phase
construction; the resulting `WrapperPlan` is materialised
into a dispenser stack that owns the per-cycle cascade
thereafter. There is no per-cycle re-resolution, no
late-bound rewiring, no "did we apply this wrapper this
time" branch in the hot path.

Two log levels surface around that one-shot decision:

- **Debug — assembly process.** The resolver's internal
  passes (trigger fan-out, transitive activation, cycle
  detection, topological sort, default-order tiebreaker
  resolution) emit per-step records suitable for
  troubleshooting a malformed registry or override.
  Operators rarely need this; build maintainers do.
- **Info — wrapper assignment.** Once the plan is
  finalised, each wrapper in the stack emits one line at
  Info naming what it'll do to the op — produced via a
  per-wrapper `describe_assignment()` method on
  `WrapperRegistration`. This is the operator-facing
  signal: it tells you, at session start, that a given
  op is being polled at 5s intervals, validated against
  `min_rows ≥ 1`, gated by `cql_dialect == "cass5"`, and
  metric-emitted under `await_index`.

```rust
pub struct WrapperRegistration {
    // ... existing fields ...

    /// One-line summary of what this wrapper, configured
    /// for the given op template, will do at runtime.
    /// Emitted at Info level once per op-template
    /// activation, alongside the other wrappers in the
    /// resolved plan.
    ///
    /// Examples:
    /// - `validate`: `"validate: min_rows ≥ 1 (strict)"`
    /// - `poll`:     `"poll: every 5s, timeout 600s,
    ///                on `await_empty`"`
    /// - `if`:       `"if: cql_dialect == 'cass5'"`
    /// - `metrics`:  `"metrics: emits cycles,
    ///                latency_*, recall (when present)"`
    ///
    /// Distinct from `OpDispenser::describe()` (SRD-30
    /// extension) which describes the *runtime op* —
    /// e.g. the CQL statement text — for error-context
    /// dumps. `describe_assignment` describes the
    /// *wrapper's contribution* for init-time
    /// diagnostics.
    pub describe_assignment: fn(&ParsedOp) -> Option<String>,
}
```

Per-op output looks like:

```text
INF op 'indexes_built_cass' wrappers (innermost → outermost):
INF   1. traverse
INF   2. validate: min_rows ≥ 1 (strict)
INF   3. poll: every 5s, timeout 600s, on `await_empty`
INF   4. if: cql_dialect == "cass5"
INF   5. metrics: emits cycles, latency_*
```

`describe_assignment` returns `Option` so trivial wrappers
that have nothing useful to say (e.g. an always-on
`traverse` with no per-op configuration) can return
`None` and skip the Info line entirely. Operators see
the wrappers that actually shape behaviour; the
boilerplate stays at Debug.

A workload's full assignment surface for every op is
fixed by the parsed templates — running the resolver in
"plan only" mode (`nbrs describe op …`) reproduces the
same Info output without firing the run, so
post-hoc inspection matches what operators saw at start.

---

## Decisions made

- **Telemetry verbosity at scale** — one Info-level
  assignment line per wrapper per op, full stop. No
  collapsing, no summarising, no truncation when the
  workload has hundreds of op templates. The operator
  authored those op templates deliberately, and the
  per-op wrapper assignment is what tells them how each
  op is actually shaped at runtime — that's exactly the
  kind of signal a verbose-by-default surface should
  preserve. Don't second-guess the operator. Operators
  who want the noise compressed have `loglevel=warn` and
  `nbrs describe op <name>` on demand.
- **Per-phase overrides** — not in scope for the initial
  push. Workload-root + per-op covers every concrete use
  case raised so far; phase-level overrides can be added
  later as a non-breaking refinement if a workload
  pattern emerges that genuinely needs them.
- **Adapter-internal wrappers** — explicitly out of
  scope. CQL's batch dispenser composes its own
  wrapper-shaped logic INSIDE the adapter dispenser; this
  SRD's "innermost is `traverse`" rule applies above the
  adapter boundary. Adapter-internal composition is each
  adapter's concern.
- **Cross-field trigger validation** — dropped from open
  questions. The `triggers: fn(&ParsedOp) -> bool`
  predicate already handles "this wrapper fires when any
  of its owned fields are present"; the speculative
  failure mode (a triggered owned field but the wrapper
  somehow not activating) doesn't have a concrete repro
  path through the resolver's trigger fan-out. If a real
  example shows up, reopen.

---

## Out of scope

- Wrappers that *generate* op templates (synthetic ops, batch
  expansion, parallel dispatch) — these aren't simple
  decorators; they reshape the op-sequence and live above
  this layer.
- Multi-instance wrappers (the same wrapper applied twice on
  one op). The registry rejects duplicates by name. If a
  legitimate use case appears (two `validate` blocks for
  pre-/post-result checks), the right surface is naming
  variants explicitly (`validate.before`, `validate.after`)
  not duplicating one entry.
- Adapter-internal composition. Each adapter owns its own
  shape inside the `DriverAdapter::map_op` boundary; this
  registry sits above that boundary.
- Per-phase override surface. Root + per-op are the only
  override scopes in v1.
- Per-cycle wrapper re-resolution. Wrapper plans are
  fixed at op-template construction; the dispenser stack
  owns the per-cycle cascade after that point. There is
  no late-binding rewiring.
- Runtime hot-swap of wrapper order. The registry is sealed
  at activity construction; mid-phase changes aren't
  supported.
