# 32: Dispenser Wrappers

Wrappers are composable decorators around `OpDispenser`. They add
cross-cutting behavior without modifying adapter code.

Each wrapper that reads GK values dynamically (per cycle, in the
op-pipeline pull path) is responsible for its own scope-init
scoping against the GK context — see
§"Init-Time Fixture and Consumer Self-Registration" below.
("Init-time" here is shorthand for "scope-init" per
[SRD 11](11_gk_evaluation.md): the fixture is sealed once per
phase activation, before the first dynamic pull.)

---

## Init-Time Fixture and Consumer Self-Registration

### Principle

The GK kernel is the single canonical state holder for scope, binding,
and name resolution (SRD 16 §"Architectural rules"). Wrappers must
not gain access to GK values via a side channel — no top-level
coordinator may "manually notate" name dependencies on a wrapper's
behalf, and no wrapper may shop for values through `ResolvedFields`
that were placed there for a different consumer.

Instead, **each wrapper self-registers its name dependencies into a
shared `ScopeFixture` at init time.** The fixture's accumulated set
of registered names *is* the runtime view of the GK context for the
op template — the net product of all consumers doing their own
scoping. There is no other source.

### `ScopeFixture` — scope-init accumulator

```rust
pub struct ScopeFixture {
    program: Arc<GkProgram>,                // the per-template canonical program
    handles: HashMap<String, PullHandle>,   // dedup by name
    plan:    Vec<PlanEntry>,                // ordered by registration
}

enum PlanEntry {
    Output { name: String, output_idx: usize },
    Input  { name: String, input_idx: usize },
}

impl ScopeFixture {
    /// Open a fixture against the per-template canonical program.
    /// The Arc clone is cheap; the activity construction loop
    /// already holds the program for adapter dispatch and bind-plan
    /// synthesis.
    pub fn new(program: Arc<GkProgram>) -> Self;

    /// Register a name. Resolves it against the program's output
    /// map (folded constants) first, then against `find_input`
    /// (extern slots, capture inputs, coordinates). Returns a
    /// memoized handle. The program must already know the name —
    /// bind-point scanning over the *whole* op template (op fields
    /// + params) is the GK compiler's responsibility (SRD 16
    /// §"Auto-Extern Generation"; SRD 31 §"Init-Time Pipeline").
    /// An unknown name here means the consumer is reading something
    /// the compiler has not provisioned — that is a workload bug,
    /// not a fixture bug, and must error.
    pub fn register_pull(&mut self, name: &str) -> Result<PullHandle, String>;

    /// Seal and yield the immutable plan. Called once after every
    /// consumer has registered. Idempotent name registrations
    /// (same name across consumers) collapse to a single plan entry
    /// and a single shared handle is returned each time. The plan
    /// owns its own `Arc<GkProgram>` clone, so dynamic
    /// `resolve` only needs the per-fiber state.
    pub fn seal(self) -> PullPlan;
}
```

The fixture borrows nothing from the per-fiber state; it operates
purely against the program's name maps. `Arc<GkProgram>` is stored
(rather than `&GkProgram` borrowed) so the lifetime ergonomics at
the activity construction site stay simple — sealing the fixture
hands ownership of the cloned Arc to the resulting [`PullPlan`].

### `PullPlan` and `PullHandle`

```rust
pub struct PullPlan {
    program: Arc<GkProgram>,    // moved in from the sealed fixture
    entries: Vec<PlanEntry>,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct PullHandle(usize);  // opaque newtype, index into plan.entries

pub struct ResolvedPulls { values: Vec<Value> }

impl ResolvedPulls {
    pub fn get(&self, h: PullHandle) -> &Value { &self.values[h.0] }
}

impl PullPlan {
    /// Dynamic-time materialization. Walks each plan entry once,
    /// pulling its current value from GkState by index — no name
    /// hashing on the hot path. Output entries go through
    /// `state.pull_by_index` (eval cone if dirty); input entries
    /// go through `state.read_input_value` (cell-aware read).
    pub fn resolve(&self, state: &mut GkState) -> ResolvedPulls;
}
```

`PullPlan::resolve` takes only the per-fiber `GkState`; the
program reference comes from the plan itself. `FiberBuilder` exposes
a thin convenience wrapper, `fiber.resolve_pulls(plan)`, used at
the executor's per-cycle dispatch site.

`PullHandle` is opaque (just a `Copy` newtype). The only way to
materialize a value is `PullPlan::resolve` followed by
`ResolvedPulls::get(handle)`. Wrappers store handles next to their
parsed config; dynamic access is one indexed dereference plus
one `Value::clone`-or-borrow.

### `OpConsumer` trait

```rust
pub trait OpConsumer: Sized {
    /// Inspect the template, register every name this consumer
    /// will read dynamically (per cycle, per pull), and return
    /// a fully-configured
    /// instance holding handles + parsed (strict) config.
    ///
    /// Failure modes (all must be `Err`):
    /// - Unknown sub-key in the consumer's own param block
    ///   (closed-vocabulary check).
    /// - Required field missing.
    /// - Bad value shape (e.g. non-numeric `k`).
    /// - Referenced name unknown to the kernel.
    fn fixture(template: &ParsedOp, fx: &mut ScopeFixture<'_>)
        -> Result<Self, String>;
}
```

The trait is the single contract between the activity construction
loop and every cross-cutting concern that wants GK values at cycle
time. Strict parsing of each consumer's slice of `op.params` lives
inside `fixture` — no late-binding kernel pulls, no silent defaults,
no extras side channel.

### `ExecCtx` — dynamic-pull bundle

The `OpDispenser::execute` signature receives an `ExecCtx` instead
of a bare `ResolvedFields`:

```rust
pub struct ExecCtx<'a> {
    /// Op-field substitution view for the inner adapter.
    pub fields: &'a ResolvedFields,
    /// Wrapper-facing handle-indexed view of GK values.
    pub pulls:  &'a ResolvedPulls,
}

pub trait OpDispenser {
    fn execute<'a>(&'a self, cycle: u64, ctx: &'a ExecCtx<'a>)
        -> Pin<Box<dyn Future<Output = Result<OpResult, ExecutionError>>
            + Send + 'a>>;
}
```

Adapters use `ctx.fields` exclusively; wrappers read `ctx.pulls` via
their stored handles and forward `ctx` unchanged to the inner
dispenser. Bundling rather than passing two parameters is
forward-compatible: per-cycle component context, streaming-capture
hooks, and diagnostic taps are all natural future fields on
`ExecCtx`.

---

## TraversingDispenser

Always applied (unless dry-run). Wraps every raw dispenser.

**Responsibilities:**
1. Count `element_count()` and `byte_count()` from `ResultBody`
2. Extract capture points from result JSON
3. Record traversal metrics

```rust
pub struct TraversingDispenser {
    inner: Arc<dyn OpDispenser>,
    stats: Arc<TraversalStats>,
    captures: Vec<CaptureSpec>,  // parsed at init time
}
```

`fixture()` parses `[name]` capture points from the template's op
fields and pre-registers nothing — captures *write* into GK ports;
they don't *read* from the kernel.

### Capture Extraction

Capture points are declared in op field strings with `[name]`
syntax:

```yaml
ops:
  read_user:
    prepared: "SELECT [username], [age as user_age] FROM users WHERE id = {id}"
```

At init time, capture specs are parsed from the template. At
cycle time, the traverser extracts named fields from the result's
`to_json()` representation:

```rust
fn extract_captures_from_json(body: &dyn ResultBody, specs: &[CaptureSpec])
    -> HashMap<String, Value>
{
    let json = body.to_json();
    for spec in specs {
        if let Some(val) = json.get(&spec.source) {
            captures.insert(spec.alias.clone(), json_to_value(val));
        }
    }
}
```

---

## ValidatingDispenser

Applied only when the template declares `verify:` or `relevancy:`
blocks. Zero overhead for templates without validation.

**Responsibilities:**
1. Check field assertions against result
2. Compute relevancy metrics (recall, precision, etc.)
3. Record pass/fail counters and score histograms
4. Hard error on missing ground truth

```rust
pub struct ValidatingDispenser {
    inner: Arc<dyn OpDispenser>,
    assertions: Vec<AssertionSpec>,
    relevancy: Option<RelevancyConfig>,
    /// Memoized handle for `relevancy.expected` (the ground-truth
    /// binding). Returned by `ScopeFixture::register_pull` at init.
    /// `None` when no `relevancy:` block is declared.
    expected_handle: Option<PullHandle>,
    metrics: Arc<ValidationMetrics>,
    strict: bool,
}
```

`fixture()` strict-parses the `relevancy:` block (closed
vocabulary, required `actual`/`expected`/`k`, `r >= k`, valid
function names) and registers `expected` against the kernel.
The kernel must already know the name — bind-point scanning over
`op.params` happens at GK compile time; the consumer's call to
`register_pull` is a *resolution* against an already-provisioned
output, not a discovery step. An unknown name here is an error.

**Design note:** Relevancy is not a cross-cutting concern — it
applies only to vector search workloads. It should be a
specialized validator plugin registered via the `relevancy:`
declaration, not a built-in field on `ValidatingDispenser`.
The target design: `ValidatingDispenser` holds a list of
validator implementations (field assertions, relevancy, future
custom validators), each activated by its own YAML block.

### Field Assertions

```yaml
verify:
  - field: name
    is: not_null
  - field: balance
    gte: 0
  - field: status
    eq: "active"
```

Predicates: `eq`, `not_null`, `is_null`, `gte`, `lte`, `contains`.

Assertion failures increment `validations_failed`. With
`strict: true`, failures become `ExecutionError::Op`.

### Relevancy Metrics

```yaml
relevancy:
  actual: key              # column to extract from result
  expected: "{ground_truth}" # GK binding for ground truth
  k: 100
  functions:
    - recall
    - precision
    - f1
    - reciprocal_rank
    - average_precision
```

Metric names include the k value: `recall@100`, `precision@10`,
etc. This matches nosqlbench's naming convention where each
function/k combination produces a distinct metric.

See [33: Result Validation](33_result_validation.md) for details.

---

## Error router attachment points (planned)

> **Status: design only — not yet implemented.** Two things are
> proposed:
>
> 1. A new wrapper type, `ErrorRouterDispenser`, sitting in the
>    **existing** op-dispenser wrapper stack. This is purely
>    additive — same trait, same composition rules, same
>    `ExecCtx` interface as the wrappers documented above.
>    Not new architecture.
>
> 2. A **new** wrapper layer for phase execution itself, mirroring
>    the op-dispenser wrapper architecture but at phase scope.
>    See §"Phase-execution wrappers" below for that. This *is*
>    new architecture — phase execution today is a bare
>    `Result<(), String>` propagating function; the proposal is
>    to make it wrappable with the same trait + decorator pattern
>    that op dispatch already uses.
>
> Both proposals reuse the SRD-03 `errors:` syntax with no new
> error-handling vocabulary.

### Default is strict; wrappers relax

The architectural rule:

> **The underlying code paths — bare `OpDispenser::execute`,
> bare phase-execution loop — are conservative and defensive.
> Any error propagates immediately, fails the op, fails the
> phase, stops the run. Wrappers can *relax* this default
> (enable retries, enable warn-and-continue, enable
> ignored-but-counted error names), but they never tighten
> an already-strict default.**

Concretely: an op or phase with no `errors:` declaration
anywhere in its cascade chain has no relaxation wrapper
attached, and any `ExecutionError` from inner code propagates
all the way up to the workload exit. This is the fail-fast
contract that SRD-03 §"Status-Determination Invariant"
already requires for fixture-verification, expressed
architecturally as the property of the wrapper composition.

The current activity-level `errors:` router is a relaxation
wrapper at the coarsest scope. Existing workloads that rely
on `.*:warn,count` semantics declare it explicitly (or,
during the migration period, the runner inserts the legacy
default for unmodified workloads — see "Backwards-compat
during rollout" below). The strict-default framing is a
property of the architecture, not a behaviour change forced
on existing workloads.

#### Backwards-compat during rollout

The current activity dispatcher applies a `.*:warn,counter`
fallback when no `errors:` is declared at activity level. To
preserve existing-workload behaviour while introducing the
new attachment points, the runner continues to inject the
legacy fallback **at activity scope only** when no activity-
level `errors:` is declared. New phase-level and op-level
attachment points have **no inherited relaxation** — declaring
`errors:` on a phase or op opts in narrowly, omitting it
keeps that scope strict-by-default.

The longer-term direction is to deprecate the activity-scope
fallback once workloads have migrated to explicit declarations.
That deprecation is a separate decision, not a precondition
for this design.

### Same syntax, multiple attachment points

The SRD-03 `errors:` router (regex pattern → action list) was
introduced as activity-scope only. To enforce SRD-03's
§"Status-Determination Invariant" at the per-op level — and to
let workloads declare different policies for measurement
loads vs. fixture-verification ops — the **same `errors:`
syntax** is allowed at additional attachment points along the
op pipeline AND at the phase-execution layer.

There is no new vocabulary. The actions stay
`retry,warn,count,stop,ignore`. The pattern syntax stays
regex. What changes is: where in the YAML you can write
`errors:` and how they cascade.

### Attachment points

| Scope | Attachment | Cascade rank | Wrapper layer |
|-------|------------|--------------|---------------|
| Activity (workload root) | `errors:` at workload level | fallback (lowest) | wraps the activity's run loop |
| Phase | `errors:` inside a phase definition | overrides activity | wraps **phase execution** |
| Op template | `errors:` inside an op definition | overrides phase | wraps the op dispenser stack |

Both scopes — phase execution and op dispatch — are wrappable
by the same mechanism. See §"Phase-execution wrappers" below
for the phase-layer architecture; the op-layer architecture is
the rest of this document.

```yaml
# Activity-level — applies to anything not overridden
errors: "Timeout:retry,warn;.*:warn,count"

phases:
  rampup:
    # No phase-level override; ops inherit activity policy.
    ops:
      insert:
        # No op-level override; inherits phase, then activity.
        prepared: "INSERT INTO ..."

  await_index:
    # Phase-level override: status-determination invariant.
    # Any error stops the run. Retries the spec.
    errors: "Timeout:retry,warn;.*:warn,stop"
    ops:
      wait_for_index:
        # Op-level override: this specific op stops on any error,
        # no retries (the polling wrapper has its own retry budget
        # via `poll_max_error_retries`).
        errors: ".*:warn,stop"
        raw: "SELECT ..."
```

Cascade is **per-error-name first-match-wins**. The router walks
op-level rules first; if no rule matches the error name, falls
through to phase-level; then activity-level; then the default
`.*:warn,count`. This means each level can declare narrow rules
without re-stating the broader fallthrough.

### Implementation: a new wrapper in the existing stack

The op-side change is purely additive: a new
`ErrorRouterDispenser` wrapper type joining
`TraversingDispenser`, `ValidatingDispenser`,
`PollingDispenser`, and friends in the established
op-dispenser stack. It uses the same `OpDispenser` trait, the
same `ExecCtx` interface, the same `OpConsumer::fixture`
init-time scoping, and the same composition rules documented
in this file's earlier sections. There is no architectural
extension at op scope — the slot for "another wrapper type"
already exists; we're filling it.

```rust
pub struct ErrorRouterDispenser {
    inner: Arc<dyn OpDispenser>,
    /// Compiled rule list, op-level rules first then phase
    /// then activity. Each rule is a (regex, action_set).
    rules: Vec<ErrorRule>,
    metrics: Arc<ErrorRouterMetrics>,
    max_retries: u32,
}
```

It wraps an inner dispenser, runs the inner's `execute`, and on
`Err` consults its `errors:` rule list (op-level + phase-level
+ activity-level, cascade order baked in at construction). The
result is one of: retry-and-loop, return-as-failure, or
return-as-success-with-count (the `ignore` action).

### Composition

Stack position determines which inner errors the router sees.
Default placement (when `errors:` appears on an op template)
is between `ValidatingDispenser` and `ConditionalDispenser` —
i.e. the router sees errors from validation, polling, and the
adapter, but a falsy `if:` short-circuits past it entirely.

```
ConditionalDispenser
  ErrorRouterDispenser (rules: op + phase + activity)
    ValidatingDispenser
      PollingDispenser
        ThrottleDispenser
          TraversingDispenser
            adapter
```

When no `errors:` is declared at op or phase level, no
`ErrorRouterDispenser` is inserted at op scope — the
activity-level router (existing behaviour) handles every
error from this op. Backwards-compatible.

### Metrics

```rust
pub struct ErrorRouterMetrics {
    pub errors_observed:  AtomicU64,
    pub errors_ignored:   AtomicU64,
    pub errors_warned:    AtomicU64,
    pub errors_retried:   AtomicU64,
    pub errors_stopped:   AtomicU64,
    pub retries_consumed: AtomicU64,
}
```

Per-op-template (one `Arc<ErrorRouterMetrics>` per attached
wrapper). Surface in the post-run summary alongside
`ProgressMetrics`.

### Re-using the same router for resume retry

Because the `errors:` policy is what governs "what happens
when this op / phase errors", and a checkpoint resume is just
a fresh execution of the failed phase with a fresh retry
budget, **the same policy applies to resume**. No new keys
needed for the checkpointing feature; see
`docs/design/workload_checkpointing.md` §"Phase-failed resume
reuses `error_policy`, no new syntax".

### Open questions

- **Composition order with the activity router.** When an op-
  level `errors:` declares `:stop` on an error name, the
  wrapper converts the error to non-retryable so the activity
  router doesn't second-guess. The activity router still owns
  the global `errors_total` count and the standard log
  messages — the wrapper observes + classifies + propagates,
  but doesn't bypass the router's bookkeeping.
- **Per-stanza vs per-op grouping.** A stanza of fixture ops
  (schema_check, await_index, verify_recall) sharing the same
  `errors:` policy can be expressed as phase-level `errors:`
  with no per-op override. No new mechanism needed; the
  cascade handles it.

---

## Phase-execution wrappers (planned)

> **Status: design only — not yet implemented.** This *is* a
> new architectural layer: phase execution today is bare
> function-call composition; the proposal is to make it
> wrappable via the same trait + decorator pattern that op
> dispatch already uses. Op-side wrapper architecture is the
> reference; phase-side adopts it at a coarser scope.

### Why a parallel layer

The op-dispenser stack catches errors **inside** a single
op's execution: adapter calls, validation, polling, throttle.
Things that fail at coarser granularity — phase setup,
fiber-pool initialisation, cursor-source construction,
post-phase metrics finalisation — happen *outside* the
dispenser stack. Today they're handled with bare `Result<(),
String>` propagation in `executor::run_phase`.

To express policies like "if this phase's setup fails, retry
the whole phase up to N times before giving up" or "warn-but-
continue if `await_index` times out", the phase executor
itself needs a wrapper layer. This is the new architectural
piece — extending an *existing* wrapper pattern (op-side) to
a scope where it doesn't yet apply (phase-side). Same trait
shape, same decorator composition, same fixture-init story —
new attachment point.

### Trait surface

```rust
pub trait PhaseExecutor: Send + Sync {
    fn execute<'a>(
        &'a self,
        ctx: &'a mut PhaseExecCtx<'a>,
    ) -> Pin<Box<dyn Future<Output = Result<PhaseOutcome, ExecutionError>>
                 + Send + 'a>>;
}
```

The "raw" executor is `RunPhaseExecutor` — what `run_phase`
does today, repackaged as a trait impl. Wrappers decorate it:

```rust
pub struct ErrorRouterPhaseExecutor {
    inner: Arc<dyn PhaseExecutor>,
    rules: Vec<ErrorRule>,
    metrics: Arc<ErrorRouterMetrics>,
    max_retries: u32,
}
```

`ErrorRouterPhaseExecutor::execute` runs `inner.execute(ctx)`,
inspects the `ExecutionError` on `Err`, consults the cascaded
rule list (op rules first then phase then activity), and
either retries the whole phase, returns failure, or returns
success-with-counted-error per the matching action.

### Default-strict contract

The bare `RunPhaseExecutor` propagates every error. With no
wrappers attached, that's the phase's behaviour — fail loud,
fail fast, stop the run.

When the workload has `errors:` at phase or activity scope,
the runner wraps `RunPhaseExecutor` with
`ErrorRouterPhaseExecutor` carrying the cascaded rules. The
wrapper is the *relaxation*. Without `errors:`, no wrapper,
strict default in effect.

### Composition example

```yaml
phases:
  await_index:
    errors: "Timeout:retry,warn;.*:stop"   # phase-level rules
    ops:
      wait_for_index_meta:
        errors: ".*:stop"                  # op-level: even stricter
        raw: "SELECT ..."
```

Phase-execution chain:

```
ErrorRouterPhaseExecutor (rules: phase + activity)
  RunPhaseExecutor
    Activity (per-op stack, including ErrorRouterDispenser
              with rules: op + phase + activity)
      adapter
```

The op-level `errors:` and phase-level `errors:` are merged
into a cascaded rule list at op-stack construction; the same
rules' phase-level subset is the input to the phase-execution
wrapper. They share data, not state.

### Resume semantics

The phase-execution wrapper is also where checkpoint resume
attaches: a `ResumeSkipPhaseExecutor` decorates `RunPhaseExecutor`
and short-circuits when the resume plan says this phase is
already complete (see `docs/design/workload_checkpointing.md`).
The wrapper stack at resume time looks like:

```
ResumeSkipPhaseExecutor       ← checkpoint plan consultation
  ErrorRouterPhaseExecutor    ← cascaded rules
    RunPhaseExecutor          ← raw phase execution
```

If the phase is in the resume plan's "already complete" set,
the outermost wrapper short-circuits with `PhaseOutcome::Skipped`
and nothing inside runs. If the phase is not in the plan
(or the plan says re-run), the call cascades inward and the
phase runs normally — through its error-router policy, which
governs both first-run and resume behaviour identically.

### Open questions

- **Wrapper-creation site.** Op-stack wrappers are constructed
  in `Activity::run_with_driver` from per-template configs.
  Phase-stack wrappers want to be constructed in the runner /
  scheduler before phase dispatch, since they govern the
  whole phase including its activity construction. Lean
  toward a `PhaseExecutorBuilder` mirroring `ActivityConfig`'s
  shape.
- **Fiber-pool errors.** Errors in fiber-pool spawn / resize
  are infrastructural, not workload-classified. Should they
  bypass the error-router wrapper entirely (always fatal) or
  flow through it as a distinguished error name (`fiber_pool`)
  that operators can route? Lean toward the latter — gives
  operators visibility and control.
- **Activity-level error-router migration.** The current
  activity-level `errors:` policy is implemented inline in
  the activity dispatcher, not as a wrapper. Migrating it to
  the new phase-wrapper architecture means the activity loop
  becomes the raw `RunPhaseExecutor` and the policy is the
  wrapper around it. Tractable, but requires care to preserve
  exact existing behaviour for workloads that already declare
  activity-level `errors:`.

---

## Composition Order

```
executor calls →
  ConditionalDispenser.execute(cycle, &ctx)        ← reads `if` via handle
    → ValidatingDispenser.execute(cycle, &ctx)     ← reads ground truth via handle
      → ThrottleDispenser.execute(cycle, &ctx)     ← reads delay via handle
        → TraversingDispenser.execute(cycle, &ctx) ← no GK reads (writes captures)
          → adapter OpDispenser.execute(cycle, &ctx.fields)
          ← OpResult (body + captures)
        ← element/byte counting, capture extraction done
      ← assertions checked, relevancy computed
    ← throttle decision applied
  ← short-circuit if `if` evaluated falsy
← metrics recorded
```

The innermost (adapter) dispenser executes first and sees only
`ctx.fields` — never `ctx.pulls`. Each wrapper above the adapter
reads any GK values it needs through its stored `PullHandle`s
against `ctx.pulls`, and forwards `ctx` unchanged inward.

---

## Dry-Run Mode

When `dry_run=true`, the traversing wrapper is replaced with a
no-op wrapper that skips adapter execution entirely. Fields are
still resolved (GK runs), but no protocol call is made.

Useful for validating workload syntax, GK bindings, and field
resolution without a live target.

### Diagnostic Visibility Levels

The system should support multiple levels of diagnostic
inspection, each revealing progressively deeper pipeline state:

| Level | Shows |
|-------|-------|
| `--show templates` | Parsed op templates after normalization |
| `--show sequence` | Op sequence with ratios and stanza layout |
| `--show dispensers` | Dispenser types selected per template |
| `--show resolve` | Resolved fields for sample cycles |
| `--show execute` | Full execution with wrapper chain visible |
| `--show wrappers` | Wrapper configuration (validators, capture specs) |

Each level should be implemented via **diagnostic wrappers** that
intercept and display pipeline state at the appropriate point,
not via conditional branches scattered throughout the code.
Diagnostic wrappers are assembled at init time based on the
requested visibility level — the same composition pattern used
for traversal and validation.
