# SRD 71: Cursor Partitioning and the `cursor` Parameter

*(DRAFT — design memo, not yet implemented)*

## Motivation

Operators routinely want to run the same workload over a fraction of
its full domain (a quick smoke test against the first 1% of vectors)
or sweep the same workload across several disjoint slices of that
domain (warm cache on the first 10%, then steady-state on the
remainder). The current surface has no direct way to express either
without modifying the workload — per-phase `bindings:` rewrites can
narrow a cursor's range, but only at workload-author time, not
operator-runtime.

This SRD specifies a single operator surface — the `cursor` parameter
— that projects the active cursor's domain into one or more
contiguous sub-ranges, and a comprehension protocol that lets the
workload iterate those sub-ranges explicitly.

Three orthogonal pieces:

1. **CLI quote elision** — `'key=value'`, `key='value'`, `key="value"`,
   bare `key=value` all parse identically. General CLI parsing fix
   that this SRD depends on but doesn't own outright.
2. **Cursor partition specs** — a small spec language for declaring
   partition lists relative to a cursor's domain.
3. **Cursor metadata wires** — the `<cursor>.cursor.*` projection
   that exposes partition state to GK matter and op templates, plus
   an explicit comprehension form that drives partition iteration.

## Naming: `cursor`, not `limit`

The parameter is named **`cursor`**. The earlier informal name
`limit` collides with SQL/CQL `LIMIT N` clauses and `evaluations.
relevancy.r` "search depth" semantics. Workloads using CQL frequently
have a `{limit}` workload param meaning the ANN candidate ceiling;
overloading would be confusing.

`cursor` matches the language — the thing being partitioned is the
cursor's domain — and avoids the collision.

## Surface

### CLI parsing — quote elision

These all resolve to the same `(name="cursor", value="0..53%")`
pair:

```
cursor=0..53%
cursor='0..53%'
cursor="0..53%"
'cursor=0..53%'
"cursor=0..53%"
cursor='[0..53%)'
cursor='[0%..53%)'
cursor='[0..53)%'
```

Rule: if the *entire argument* is wrapped in matching single or
double quotes, strip them. Then if the value (the part after the
first `=`) is wrapped in matching quotes, strip those. The quote
characters are never part of the parsed name or value.

This is a general CLI parsing rule, applied to every named
parameter — not just `cursor`. Same elision applies to params that
already exist (`dataset`, `keyspace`, `concurrency`, etc.). Any
construction that produced a quote-wrapped value (a wrapper script
forwarding `"$@"`, a `--arg="value"` form that double-passed through
a shell) becomes idempotent.

Open question: do we treat backtick or other quote-like characters?
Proposed answer: no — single and double quotes only. Backtick has
shell-evaluation semantics that don't survive into our argv.

### Cursor partition specs

A spec resolves to a non-empty **contiguous** list of partitions.
Each partition is a half-open ordinal range `[start_ord, end_ord)`
within the cursor's declared extent `[base_start, base_end)`.

#### Number forms

Each numeric value in a spec is one of three forms; the form is
determined unambiguously from the literal's shape, never from
context. Forms may be **mixed within a single spec**.

| Form         | Shape                                    | Meaning                                            |
|--------------|------------------------------------------|----------------------------------------------------|
| Percentage   | digits + `%` (e.g. `53%`, `0.5%`)        | A fraction of the cursor's extent, in `[0, 100]`.  |
| Fraction     | number containing a `.`, in `[0.0, 1.0]` | Same as percentage scaled by 100. `0.53` == `53%`. |
| Literal      | bare integer (no `.`, no `%`)            | An absolute cursor ordinal (u64).                  |

`0.5` and `50%` are interchangeable. `100` is the literal ordinal
100. A decimal number with leading digits ≥ 1 (e.g. `1.5`) is
rejected at parse time with a diagnostic — either the operator
meant `1.5%` (percentage) or `0.015` (fraction) or `15`
(literal), and forcing them to disambiguate avoids a class of
"why did this run for ages" surprises.

Resolution to absolute ordinals happens at phase setup, against
the cursor's known base extent. Percentages and fractions are
multiplied; literals are clamped to `[base_start, base_end]` with
a diagnostic if the spec walks outside the extent. The
`<wire>.cursor.start_pct` / `.end_pct` / `.start_ordinal` /
`.end_ordinal` projection wires report both views post-resolution
regardless of which form the operator typed.

#### Form 1 — single sub-range

```
0..53%
[0..53%]
[0%..53%)
[0..53)%
0%..53%
0..0.53                          # fraction form, same as above
100..1000                        # literal ordinals — first 1000 rows starting at 100
0..1000                          # literal end — first 1000 rows
0.05..0.5                        # 5% to 50%
100..50%                         # ordinal 100 to 50% of extent (mixed)
0.10..10000                      # 10% of extent to ordinal 10000 (mixed)
```

All parse to a single-partition list. Bracket placement and
closure markers (`[ ] ( )`) are accepted but advisory — closure
is always treated as `[start, end)` (left-closed, right-open) so
adjacent partitions don't double-count the boundary.

The `%` sign may appear after each number, after the closing
bracket, or once at the end. All forms parse the same way. The
endpoint type is determined per-endpoint independently; mixed
endpoints (`100..50%`) resolve as expected.

#### Form 2 — contiguous partition list

Each entry is a **delta** from the running start. Entry types
work the same way as in Form 1: percentages, fractions, and
literals can be mixed.

```
2%,10%,*%
0.02,0.10,*                      # fraction equivalents
[2%,10%,*%]
1000,5000,*                      # literal deltas — first 1000, next 5000, remainder
1000,10%,*                       # mixed — first 1000, next 10% of extent, remainder
20%,30%                          # short list — partitions [(0%,20%), (20%,50%)], trailing 50% dropped
```

The literal `*` (or `*%` — the `%` is decorative here) is the
"remainder" token; exactly one entry may be `*`, and it absorbs
whatever ordinals are needed for the list to span the cursor's
full extent. A list summing **exactly** to the extent doesn't
need `*`. A list summing **less than** the extent without `*`
drops the trailing gap. A list summing **more than** the extent
is rejected at resolution time (parse time can't catch mixed
literal/percentage lists because the extent isn't known until
phase setup).

A `*` entry in a list of all-percentage / all-fractional entries
absorbs the missing percentage. A `*` in a list containing any
literal absorbs whatever absolute-ordinal remainder is left
after resolving the percentages and fractions against the actual
extent.

#### Form 3 — pre-baked ratio expansions

A `name:args` form expands to a partition list via a built-in
recipe. Weights from the recipe are normalised to sum to 100% and
laid out left-to-right.

| Spec                   | Weights produced                              | Notes |
|------------------------|-----------------------------------------------|-------|
| `linear:N`             | `1,1,…,1` (N copies)                          | Uniform N-way split. |
| `ratios:a,b,c,…`       | The literal weights                           | Explicit override; weights normalised. |
| `mul:R`                | `1, R, R², R³, …`                             | One per term, terminate at the first weight whose contribution rounds to < 0.1%. |
| `mul:S,R`              | `S, S·R, S·R², …`                             | Same termination rule, scaled. |
| `bin:N`                | `C(N-1,0), C(N-1,1), …, C(N-1,N-1)`           | Coefficients of the binomial expansion `(1+x)^(N-1)` — exactly N terms. Not the binomial distribution PMF. |
| `fib:N`                | `F(1), F(2), …, F(N)` (first N Fibonacci)     | Distinct terms only; F(1)=1, F(2)=2, F(3)=3, …  Skips the redundant leading `1,1`. |
| `ln:N`                 | `ln(1+1), ln(1+2), …, ln(1+N)`                | Slow growth; useful for log-spaced workload phases. |
| `geom:N,R`             | `1, R, R², …, R^(N-1)`                        | Like `mul` but with a fixed term count instead of a tail-off rule. |
| `zipf:s,N`             | `1/1^s, 1/2^s, …, 1/N^s`                      | Zipfian access pattern (s>0); heavy head. |
| `pareto:alpha,N`       | `(1/n)^alpha` for n in `1..N`                 | Pareto-style heavy-tail. |
| `front_heavy:N`        | `N, N-1, …, 1`                                | Linear declining — front partitions cover a larger fraction of the cursor extent. Useful for warm-then-coast. |
| `back_heavy:N`         | `1, 2, …, N`                                  | Linear growing — front partitions cover smaller fractions; tail partitions cover larger ones. |

All weight-list forms produce contiguous partitions covering exactly
0..100%.

`bin:5` example: weights `1,4,6,4,1` (= `C(4,k)` for `k∈0..4`)
→ five partitions of 6.25%, 25%, 37.5%, 25%, 6.25%.

`fib:7` example: weights `1,2,3,5,8,13,21` → seven partitions
summing to 53; normalised → 1.89%, 3.77%, 5.66%, 9.43%, 15.09%,
24.53%, 39.62% (approximately).

`mul:2.3` example: 1, 2.3, 5.29, 12.17, 27.98, 64.36, … — terms
continue until each new term's contribution < 0.1% of the running
total. Useful for "exponential ramp" testing.

#### Parser

The spec parser is shared between CLI and YAML param-value
contexts. Whitespace is ignored. Numbers accept integers and decimals.
Brackets and `%` placement are forgiving per Form 1's examples.

### Cursor metadata wires

Every cursor declaration `cursor q = <expr>` exposes a metadata
namespace on the wire `q`:

| Name                       | Type   | Meaning |
|----------------------------|--------|---------|
| `q.cursor.partition_count` | u64    | Number of partitions in the active spec. 1 when no spec is supplied. |
| `q.cursor.idx`             | u64    | 0-based index of the active partition. 0 when no spec / no iteration. |
| `q.cursor.start_pct`       | f64    | Start of the active partition, [0.0, 100.0). |
| `q.cursor.end_pct`         | f64    | End of the active partition, (0.0, 100.0]. |
| `q.cursor.start_ordinal`   | u64    | Absolute ordinal at the partition's start (inclusive). |
| `q.cursor.end_ordinal`     | u64    | Absolute ordinal at the partition's end (exclusive). |
| `q.cursor.partitions`      | list   | Full partition list for iteration. Elements are tuples `(idx, start_pct, end_pct, start_ord, end_ord)`. |

These wires resolve through the standard GK scope chain. They are
visible to bindings, op-template fields, evaluations, and metric
labels alike — anywhere `{q.cursor.idx}` or `{q.cursor.end_pct}`
interpolates.

`q.cursor.partitions` is the only one that returns a list; the rest
are scalars reflecting the current iteration step.

### Comprehension syntax for partition iteration

Iteration is **explicit and named**. The workload author opts in
at two sites that must agree on a name:

1. The scenario-tree `for:` clause iterates the parameter's
   partition projection wire (`<param>.partitions`) and binds
   an iter-var.
2. The phase's cursor declaration names that iter-var via an
   `over <name>` clause.

```yaml
scenarios:
  sweep:
    - for: "p in cursor.partitions"
      phases:
        - my_phase

phases:
  my_phase:
    bindings: |
      cursor q = range(0, N) over p
```

Each `for:` iteration materialises a fresh `q` narrowed to
`p`'s sub-range; the `q.cursor.*` scalars reflect the current
partition. Inside `my_phase`, op templates can interpolate
`{p.idx}` (iter-var fields) or `{q.cursor.idx}` (cursor wire
fields — same values, but the cursor wire also carries absolute
ordinals computed against `q`'s extent).

Tuple-destructuring works the same way as for other
comprehensions:

```yaml
- for: "(idx, start_pct, end_pct, _, _) in cursor.partitions"
  phases:
    - my_phase
```

The cursor decl can also name the parameter's projection
directly (skipping the `for:` scaffold) when the spec is single-
partition:

```yaml
phases:
  my_phase:
    bindings: |
      cursor q = range(0, N) over cursor.partitions
```

The `over cursor.partitions` form follows whatever the
parameter's current state is. With a single-partition spec, `q`
narrows directly. With a multi-partition spec and no enclosing
iteration, the cursor declaration is a startup error — the
diagnostic names the cursor and the missing `for:` clause.

A cursor declared without `over` ignores `cursor=...` entirely;
its extent is whatever its constructor expression evaluates to.
That's the intentional cost of explicit opt-in.

### Workload param surface

`cursor=...` on the CLI applies to **every cursor-bearing phase** in
the workload. The spec is stored as a workload-level setting and
consulted whenever a phase resolves its cursor's effective extent.

The operator can scope an override to a **specific** phase by name:

```
mytestphase42.cursor=fib:7
```

`mytestphase42` is matched against the phase name (the YAML key in
the `phases:` block, post-comprehension-expansion). For phases
synthesised by scenario-tree iteration (e.g. `pvs_query` running
inside `for: "table in vec_{profile}, ..."`), the *base* phase
name is matched.

The phase-name part may be a **glob** (`*`, `?`, `[abc]` per
fnmatch):

```
phase42_*.cursor=fib:7         # all phases starting with phase42_
*_query.cursor=0..10%          # all *_query phases narrowed to first 10%
```

Resolution precedence (highest wins) at each phase:

1. Phase-explicit glob match from CLI (most specific match wins
   ties; ambiguous tie is a fatal error at startup).
2. Workload-wide `cursor=...` from CLI.
3. Phase's `params: { cursor: "..." }` block.
4. Workload-level `params:` block default for `cursor`.
5. No spec — cursor uses its declared extent unmodified.

The user can **reify the operator surface** via a custom param
name. The workload declares its own param name in `params:`, then
each phase's `params:` references it:

```yaml
params:
  warmup_cursor: "0..5%"
  steady_cursor: "5%..100%"

phases:
  warmup:
    params:
      cursor: "{warmup_cursor}"
    # ...

  steady:
    params:
      cursor: "{steady_cursor}"
    # ...
```

The operator overrides via `warmup_cursor=0..1%` (the workload's
public surface) without having to know which phase consumes it.

### Partition iteration: connecting scenarios to phase cursors

The connection between a scenario-tree iteration and a phase's
cursor declaration is **explicit and named**. The scenario-tree
`for:` clause binds an iter-var; the cursor declaration names
that iter-var as its partition source. There is no implicit
"ambient narrowing" — a cursor narrows if and only if its
declaration says so.

#### Explicit binding at the cursor declaration

A new `over <name>` clause attaches a cursor to a partition
source. The clause names a wire that resolves through the
standard scope chain to either:

- An iter-var bound by an enclosing `for:` clause, or
- A cursor parameter's `<param>.partitions` projection wire
  (when the cursor is meant to follow the parameter directly
  without an iteration scaffold — only valid for single-partition
  specs).

```
cursor q = range(0, query_count(prebuffered)) over p
```

This declares: "`q` is a cursor over the range, narrowed by the
partition named `p` resolved in this scope." The narrowing is
applied at cursor materialisation time.

Without an `over` clause, a cursor uses its full declared
extent — no narrowing applies, regardless of what partitions
are in scope. Operator overrides like `cursor=...` can't reach
a cursor that wasn't declared `over` something.

#### Driving an iteration

A declared cursor parameter — the default `cursor` or a
custom-named one like `warmup_cursor` — surfaces a sibling wire
`<param>.partitions` at workload scope, carrying the resolved
partition list as `(idx, start_pct, end_pct)` tuples. The
scenario tree iterates that list with a standard `for:` clause,
and the cursor names the iter-var:

```yaml
params:
  cursor: "2%,10%,*%"

scenarios:
  sweep:
    - for: "p in cursor.partitions"
      phases:
        - ann_query

phases:
  ann_query:
    bindings: |
      const prebuffered := dataset_prebuffer("{dataset}:{profile}")
      cursor q = range(0, query_count(prebuffered)) over p
      query_vector := query_vector_at(prebuffered, q)
```

The connection is direct: `p` is named in the `for:` clause
(scenario tree) and in the `over p` clause (phase bindings).
With `cursor=2%,10%,*%` the iteration runs three times; on each
iteration `q`'s effective extent is the corresponding sub-range,
and `q.cursor.*` scalars reflect the current partition. Inside
the phase, the workload author can interpolate either form —
`{p.idx}` reads the iteration variable; `{q.cursor.start_ordinal}`
reads the absolute ordinal resolved against `q`'s base extent
(a projection that's only well-defined on the cursor wire,
because absolute ordinals depend on the cursor's extent).

For phases without a cursor declaration carrying `over`, the
iteration has no effect on that phase — `p` is still bound, but
nothing consumes it. That's the intentional cost of explicit
opt-in: a workload that hasn't been wired up to use `cursor`
won't be partitioned silently.

#### Single-partition / no-iteration form

When the spec is a single partition and no scenario-level
iteration is needed, the cursor names the parameter's projection
wire directly:

```yaml
phases:
  ann_query:
    bindings: |
      cursor q = range(0, query_count(prebuffered)) over cursor.partitions
```

`over cursor.partitions` means "follow the workload's `cursor`
parameter's partition state, whatever it is." If the parameter
is single-partition (`cursor=0..1%`), `q` narrows directly. A
multi-partition spec without an enclosing `for:` clause is a
startup error that names the missing iteration and points the
operator at the `over p` form.

This is the form a "smoke-test friendly" workload uses — the
operator can pass `cursor=0..1%` and any cursor declared `over
cursor.partitions` narrows automatically, without the workload
needing a `for:` scaffold for a single iteration.

#### Reified parameter names

Custom-named cursor parameters work the same way. The
`<param>.partitions` projection wire follows the parameter name:

```yaml
params:
  warmup_cursor: "0..5%"
  steady_cursor: "5%..100%"

scenarios:
  warmup_then_steady:
    - for: "wp in warmup_cursor.partitions"
      phases:
        - warmup
    - for: "sp in steady_cursor.partitions"
      phases:
        - steady

phases:
  warmup:
    bindings: |
      cursor q = range(0, query_count(prebuffered)) over wp
      # ...

  steady:
    bindings: |
      cursor q = range(0, query_count(prebuffered)) over sp
      # ...
```

Each phase's cursor names the iter-var bound by its enclosing
`for:`. There's no ambient state — every connection is a named
wire reference visible at both the iteration site and the cursor
declaration.

#### Nesting

Multiple `for:` clauses can nest; the inner cursor declarations
name whichever iter-var they want:

```yaml
- for: "wp in warmup_cursor.partitions"
  phases:
    - for: "sp in steady_cursor.partitions"
      phases:
        - mixed_phase    # cursor inside names wp OR sp explicitly
```

There is no "innermost wins" rule, because there is no implicit
binding — the cursor declaration names exactly which partition
source it follows. Standard SRD 13c name resolution applies to
the `over <name>` lookup itself; if a nested `for:` shadows an
outer iter-var by reusing the same name, the inner binding
wins — same rule comprehensions already follow for any other
iter-var.

## Internal model

### Partition list resolution

The partition spec is a **value type** — a literal string like
`"2%,10%,*%"` that flows through the standard parameter chain.
There is no shared partition-list instance held anywhere; every
cursor scope instantiates its own list freshly from the same
input spec. Cursor lifecycle, including the partition list it
follows, is owned by the cursor's declared scope. Two unrelated
scopes that read the same parameter each resolve a private list
against their own cursor extent. No cross-scope contention is
possible, and the spec itself can't drift mid-run because
parameter resolution is effectively-const for the scope
activation (per SRD 11).

At phase setup, the runtime walks the phase's cursor declarations.
For each cursor:

1. Resolve the active spec via the precedence chain above.
2. Parse the spec into a partition list of
   `(start_pct, end_pct)` pairs.
3. Compute absolute ordinals from the cursor's base extent
   `[base_start, base_end)`:
   - `start_ord = base_start + floor(start_pct * (base_end - base_start) / 100)`
   - `end_ord   = base_start + floor(end_pct   * (base_end - base_start) / 100)`
4. Install the partition list as the value of the
   `<wire>.cursor.partitions` output (and seed the scalar
   `.cursor.*` outputs with the partition-0 values).

For cursors with **open extents** (`until_elapsed(...)` and friends),
percentage-based partitioning has no obvious target. Two options:

- **Reject** — passing `cursor=...` to a phase with an open cursor
  is a startup error.
- **Project onto base** — interpret the percentages as fractions of
  the `base` (per-pass chunk size). Partition `[0..50%)` over
  `base=10000` becomes ordinals `[0, 5000)`. The open-end policy
  still extends from there.

Proposed default: **reject** (with a clear diagnostic). The
projection-onto-base interpretation can be added later if a real
use case appears — operators today don't have anything close to it,
so the constrained surface is the safer floor.

### Cursor metadata wire materialisation

`q.cursor.*` lookups resolve via the standard scope chain. The
runtime synthesises a scope node above the cursor declaration that
publishes the `partitions` list (effectively-const for the phase's
lifetime) and the `idx` / `start_pct` / `end_pct` / `start_ordinal`
/ `end_ordinal` scalars (effectively-const for the partition
iteration's lifetime — they update at each iteration boundary).

The `<wire>.cursor.<field>` lookup syntax piggybacks on the
existing dot-form (already used by `prebuffered.something` field
projections in SRD 53). No new lexer / parser surface required —
the resolver just needs to know that any wire of cursor-source
shape has a `.cursor` projection.

### Workload-level vs phase-level / glob storage

CLI parsing builds two maps:

- `workload_params: HashMap<String, String>` — entries with no `.`.
- `phase_overrides: Vec<(GlobPattern, String, String)>` — entries
  with a `.` in the name part: `(glob, param_name, value)`.

At each phase's parameter resolution, the runtime:

1. Walks `phase_overrides` and collects matching `(param, value)`
   pairs. Ambiguous matches (two distinct globs match this phase
   for the same param name) → fatal error at startup with a clear
   diagnostic naming both patterns.
2. Falls back to `workload_params`.
3. Falls back to the phase's own `params:` block.
4. Falls back to the workload-level `params:` block.

Resolution happens once per phase scope-tree-resolve. The result is
the same string-valued spec that gets passed to the partition
parser.

### Interaction with existing cursor surface

`range(start, end)` cursors: percentage projection is well-defined
against `[start, end)`. Partition ordinals resolve directly.

`until_elapsed`, `until_passes`, `until_count`, and the `_and_` /
`_or_` composites: when a partition is named via `over`, the
partition's cardinality (its `end_ord - start_ord`) becomes the
hard upper bound the policy converges toward. The reservation
walks within the partition's range; the extension policy still
makes its time / pass / count decisions but terminates as soon as
the partition is exhausted, whether or not the time / pass / count
target was reached. See `## Partition as a first-class GK type`
below for the type that carries this from spec resolution into
the cursor's policy.

The existing `RangeSource` / `ExtendingRangeSource` factories
already accept `start` and `end` parameters; the partition
narrowing just adjusts them at construction. No new source factory
required.

## Partition as a first-class GK type

The partition spec language above lives at the operator surface
(CLI / YAML strings). Past the parser, partitions flow through GK
wires as **two first-class value types** that GK nodes can consume
and produce the same way they handle `U64`, `F64`, `Str`, or
`VecF32`. This is what lets `until_elapsed` accept a partition's
cardinality, lets modulo operations stay inside a partition's
range, and lets the workload author derive one partition from
another via standard composition.

### Value types

**`PartitionSpec`** — the parsed-but-unresolved form. Carries the
operator's spec literal as a structured value:

```
PartitionSpec {
    entries: Vec<PartitionEntry>,
}

PartitionEntry {
    idx:        u64,        // 0-based position in the list
    start:      Bound,      // Pct(f64) | Frac(f64) | Ord(u64)
    end:        Bound,      // same; `*` resolves to extent at materialisation
}
```

`PartitionSpec` is what `<param>.partitions` returns at workload
scope before any cursor has bound it.

**`Partition`** — a single resolved partition with concrete
absolute ordinals. Materialised against a known base extent:

```
Partition {
    idx:         u64,       // 0-based position
    start_ord:   u64,       // absolute, inclusive
    end_ord:     u64,       // absolute, exclusive
    start_pct:   f64,       // [0.0, 100.0)
    end_pct:     f64,       // (0.0, 100.0]
    base_extent: u64,       // the extent it was resolved against
}
```

`Partition.cardinality` = `end_ord - start_ord` is a derived
projection, not a stored field.

Both types ride inside the `Value` enum the same way `Str` or
`VecF32` already do (per the [[GK Types Are Flexible]] rule).
`Vec<Partition>` is the natural list shape for the resolved
partition list of a cursor.

### Where each type appears

| Wire / expression                  | Value type                       |
|------------------------------------|----------------------------------|
| `<param>.partitions`               | `PartitionSpec`                  |
| `<param>.partitions[i]`            | `PartitionEntry` (sub-projection)|
| Iter-var `p` in `for: "p in <param>.partitions"` | `PartitionEntry`   |
| `q.cursor` (a cursor wire's partition projection) | `Partition`       |
| `q.cursor.partitions`              | `Vec<Partition>` resolved against `q`'s extent |
| `q.cursor.idx` / `.start_ord` / …  | scalar projections of `q.cursor` |

The distinction is concrete: `PartitionEntry` is a description that
hasn't been pinned to an extent; `Partition` is a fully resolved
range. The runtime promotes `PartitionEntry → Partition` at cursor
materialisation time when the cursor names a partition via `over`
— the cursor's base extent is the resolution context.

### Functions that consume partitions

A small set of stdlib node functions operates on partition values
as their primary argument. Each is a first-class GK node — same
P3 JIT eligibility rules as the rest of the stdlib.

| Function                       | Signature                                       | Meaning |
|--------------------------------|-------------------------------------------------|---------|
| `cardinality(p)`               | `Partition → u64`                               | `p.end_ord - p.start_ord`. |
| `start_of(p)`                  | `Partition → u64`                               | `p.start_ord`. |
| `end_of(p)`                    | `Partition → u64`                               | `p.end_ord` (exclusive). |
| `idx_of(p)`                    | `Partition → u64`                               | `p.idx`. |
| `mod_in(n, p)`                 | `u64, Partition → u64`                          | `p.start_ord + (n mod cardinality(p))`. Maps an arbitrary integer into the partition's range, wrapping. |
| `at(p, i)`                     | `Partition, u64 → u64`                          | `p.start_ord + i`. Errors at evaluation if `i ≥ cardinality(p)`. |
| `clamp_in(n, p)`               | `u64, Partition → u64`                          | `max(p.start_ord, min(n, p.end_ord - 1))`. Saturating projection rather than modulo. |
| `random_in(p, seed)`           | `Partition, u64 → u64`                          | `p.start_ord + hash(seed) mod cardinality(p)`. Deterministic per seed. |
| `subdivide(p, n)`              | `Partition, u64 → Vec<Partition>`               | Splits `p` into `n` equal sub-partitions. Indices restart at 0; `base_extent` propagates. |
| `resolve(spec, extent)`        | `PartitionSpec, u64 → Vec<Partition>`           | Promotes a raw spec to a list of resolved partitions against an explicit extent. Useful when the workload wants to use a spec without binding it to a cursor first. |

Cursor constructors (`range`, `until_elapsed`, `until_passes`,
`until_count`, and the composites) also accept partition-typed
inputs as their narrowing source — the `over` clause is the
syntactic sugar for this; the underlying lowering passes the
partition into the constructor.

### `until_elapsed` over a computed partition

With a `Partition` flowing into `until_elapsed`, the policy has a
hard upper bound (the partition's cardinality) alongside its
time bound (`min_ms`). The reservation loop walks within
`[p.start_ord, p.end_ord)`; the extension policy projects
remaining cycles from the observed rate the same way it does
today, but capped at `cardinality(p)` so the cursor terminates
the moment either the time or the partition is exhausted.

```yaml
params:
  cursor: "2%,10%,*%"

scenarios:
  sweep:
    - for: "p in cursor.partitions"
      phases:
        - timed_per_partition

phases:
  timed_per_partition:
    bindings: |
      const prebuffered := dataset_prebuffer("{dataset}:{profile}")
      # Resolve `p` against the dataset's vector count first; the
      # resulting `Partition` is what `until_elapsed` consumes.
      part := resolve(p, vector_count(prebuffered))[0]
      cursor q = until_elapsed(100, 10000) over part
      query_vector := query_vector_at(prebuffered, q)
```

For each of the three iterations, `q` reserves up to one
`vector_count`-fraction of ordinals from `prebuffered`,
terminating when either 10 seconds elapses or the partition is
fully consumed. The rate-projection math from
[`UntilElapsedPolicy`](../../nbrs-variates/src/source.rs) gets
the partition's cardinality as a natural ceiling — it converges
geometrically on the time target but doesn't overshoot the
partition.

A shorter sugar form, when the cursor's base extent is the
natural resolution target:

```
cursor q = until_elapsed(100, 10000) over p
```

Here the `over p` lowering resolves `p` against `until_elapsed`'s
declared base or against a phase-level "extent" wire if one is
declared. If the cursor has no natural extent (no `range` portion
to draw from), the spec must be in literal-ordinal form or routed
through `resolve(...)` first.

### Modulo and other index-arithmetic compositions

`mod_in` is the canonical "pick an ordinal inside a partition"
function. Combined with `cycle` as the input, it gives the
workload author a deterministic per-cycle ordinal selector
that's guaranteed to stay inside the active partition:

```yaml
phases:
  ann_query:
    bindings: |
      const prebuffered := dataset_prebuffer("{dataset}:{profile}")
      cursor q = range(0, query_count(prebuffered)) over p
      # Pick a query vector index from inside the active partition,
      # wrapping if the cycle count exceeds the partition's size.
      qi := mod_in(cycle, q.cursor)
      query_vector := query_vector_at(prebuffered, qi)
```

`at(p, i)` is the bounds-checked variant — useful when iteration
is meant to consume each ordinal exactly once and the workload
wants a hard error rather than wrap-around.

`subdivide(p, n)` lets the workload create nested partitions
without re-parsing a spec. A coarse-grained outer iteration
followed by a fine-grained inner iteration of each outer
partition:

```yaml
scenarios:
  hierarchical_sweep:
    - for: "outer in cursor.partitions"
      phases:
        - for: "inner in subdivide(outer, 10)"
          phases:
            - ann_query
```

`outer` is a `PartitionEntry`; `subdivide` is overloaded to
accept either a `Partition` or a `PartitionEntry` (the latter
needs an extent — falls back to the consuming cursor's extent,
same as `over`).

### Composing partitions across cursors

Because `Partition` is a regular GK value, one cursor's partition
can drive another:

```yaml
phases:
  windowed_load:
    bindings: |
      const prebuffered := dataset_prebuffer("{dataset}:{profile}")
      cursor q1 = range(0, vector_count(prebuffered)) over p
      # q2 walks the same partition with a time-budget policy
      cursor q2 = until_elapsed(100, 10000) over q1.cursor
      v := vector_at(prebuffered, q1)
      meta := metadata_value_at(prebuffered, q2)
```

`q1.cursor` is a `Partition` resolved against `vector_count`.
`q2`'s `over q1.cursor` consumes that already-resolved partition
directly — no re-resolution needed. The two cursors share the
same window of ordinals; each has its own iteration policy
within it.

## Worked examples

### Smoke test against first 1% of vectors

Workload declares its cursors with `over cursor.partitions` so
they follow the operator-set parameter without needing a
scenario-level iteration:

```yaml
phases:
  rampup:
    bindings: |
      cursor row = range(0, vector_count(prebuffered)) over cursor.partitions
      # ...
  ann_query:
    bindings: |
      cursor q = range(0, query_count(prebuffered)) over cursor.partitions
      # ...
```

Operator runs:

```
nbrs run workload=full_cql_vector.yaml scenario=test_oracles cursor=0..1%
```

Each cursor declared `over cursor.partitions` narrows to the
first 1% of its base extent. Cursors not declared `over` anything
keep their full extent. Phases without cursors (`schema`,
`teardown`, `jolokia_*`) run unchanged.

### Three-stage workload sweep

```
nbrs run workload=ann_sweep.yaml cursor=2%,10%,*%
```

Workload declares:

```yaml
scenarios:
  sweep:
    - for: "p in cursor.partitions"
      phases:
        - ann_query

phases:
  ann_query:
    bindings: |
      const prebuffered := dataset_prebuffer("{dataset}:{profile}")
      cursor q = range(0, query_count(prebuffered)) over p
      query_vector := query_vector_at(prebuffered, q)
    ops:
      select_ann:
        prepared: "SELECT key FROM ... ANN OF {query_vector} LIMIT 10"
```

The scenario tree iterates `cursor.partitions` and binds the
iter-var `p`. The cursor `q` in `ann_query`'s bindings declares
`over p` — the explicit name match wires the iteration to the
cursor. With `cursor=2%,10%,*%` the iteration runs three times;
each call materialises `q` with a different sub-range:
`[0, 0.02 * query_count)`, `[0.02 * query_count, 0.12 *
query_count)`, `[0.12 * query_count, query_count)`.

Op-template fields and metric labels inside `ann_query` can
interpolate either the iteration variable (`{p.idx}`,
`{p.start_pct}`) or the cursor's own resolved projection
(`{q.cursor.idx}`, `{q.cursor.start_ordinal}`). The cursor-wire
form is the one to use when the absolute-ordinal value matters
(metrics labelled by partition row range, etc.).

### Reified operator surface

A workload that wants to expose `warmup_cursor` and
`steady_cursor` as the operator-facing knobs:

```yaml
params:
  warmup_cursor: "0..5%"
  steady_cursor: "5%..100%"

phases:
  warmup:
    bindings: |
      cursor q = range(0, query_count(prebuffered)) over warmup_cursor.partitions
      # ...

  steady:
    bindings: |
      cursor q = range(0, query_count(prebuffered)) over steady_cursor.partitions
      # ...
```

The operator overrides via `warmup_cursor=0..1%` — and the
phase's cursor (named `over warmup_cursor.partitions`) follows
that parameter without needing a `for:` scaffold. Both phases
are independently controlled because their cursors name distinct
parameters.

### Glob-scoped override

```
nbrs run workload=full_cql_vector.yaml *_query.cursor=fib:7
```

Every phase whose name ends in `_query` (e.g. `ann_query`,
`pvs_query`, `pvs_metadata_query`) gets partitioned by the
`fib:7` spec. Non-query phases unchanged.

## Phased delivery

**P1 — Foundation.** CLI quote elision + cursor partition spec
parser + `cursor` workload param + explicit `over <name>` clause
on cursor declarations. Single-partition specs only;
multi-partition is a parse-error pending P2. Phase-level
`params: { cursor: ... }` plumbing. No glob support.

**P2 — Partition iteration and type system.** Multi-partition
specs accepted. `Partition` and `PartitionSpec` GK value types
added; `<param>.partitions` projection wire exposed at workload
scope. `for: "p in <param>.partitions"` comprehension form;
phase-local cursors bind via `over p`. `<wire>.cursor.*`
projections on cursor wires expose the resolved `Partition`.
Stdlib partition functions: `cardinality`, `start_of`, `end_of`,
`idx_of`, `mod_in`, `at`, `clamp_in`, `random_in`, `subdivide`,
`resolve`. Pre-baked recipes (`linear`, `ratios`, `mul`, `bin`,
`fib`, `ln`). `until_elapsed` / `until_passes` / `until_count`
accept a `Partition` as the bound and converge within it.

**P3 — Operator-surface conveniences.** Phase-scoped CLI overrides
(`phase.cursor=...`). Glob matching for phase scoping. Remaining
pre-baked recipes (`geom`, `zipf`, `pareto`, `front_heavy`,
`back_heavy`).

### Status / report integration

The phase-status banner reflects partition iteration for any phase
whose active scope has an in-scope cursor partition. Format:

```
partition <idx>/<count> [<start>..<end>)
```

Where `<idx>` is 1-based for display, `<count>` is the total
partition count, and `[<start>..<end>)` is the condensed effective
range. The range uses ordinal form when the spec was literal /
mixed-literal, percentage form when the spec was pure-percentage
or pure-fraction. Examples:

```
partition 3/7 [12000..18000)
partition 3/7 [12%..18%)
```

For phases that run without an iteration (single-partition spec
or no spec at all), the banner is suppressed.

Metric labels can carry the same projection via the cursor wire's
`q.cursor.idx` / `q.cursor.start_ordinal` / `.end_ordinal` /
`.start_pct` / `.end_pct` fields; labelling is workload-author
choice and orthogonal to the banner.

### Partition list filtering

The comprehension expression accepts the standard SRD 18c `where`
clause, so partition iteration can be filtered inline:

```yaml
- for: "p in cursor.partitions where p.idx > 0"
  phases:
    - my_phase
```

No new surface — the `where` clause already works against
arbitrary list-valued comprehension sources; partition lists slot
in naturally.

## Open questions

- **Percentage specs against cursors with no natural extent.**
  When `until_elapsed(base, min_ms)` is declared `over <p>` and
  `p` is a `PartitionEntry` in pct/fraction form, there's no
  obvious extent to resolve `p` against (the cursor itself has
  no closed extent until the policy terminates). The workload
  author has two recourses: (a) use literal-ordinal form in the
  spec, or (b) route through `resolve(p, N)` against an
  explicit reference extent (typically a dataset count). Open
  question: should the runtime auto-resolve against a single
  declared phase-level "reference extent" wire if one exists,
  to spare authors the `resolve(...)` call? Proposed: no — the
  explicit `resolve` keeps the dependency visible at the
  declaration site.

## Non-goals

- **Non-contiguous partition lists.** Operators can chain workload
  invocations for `0..10% then 60..70%`. The contiguous restriction
  keeps the source-factory machinery simple (one `[start, end)`
  range per partition iteration step).
- **Mid-activation partition resampling.** Partitions are
  effectively-const for the lifetime of one scope activation. The
  partition list itself never changes inside a single phase run.
  A future "dynamic partition" feature would need its own design.
- **Touching other phase shape parameters.** This SRD modulates
  the cursor's extent and nothing else. Anything else that reads
  from a cursor's extent keeps reading whatever the (now
  narrowed) cursor exposes — there's no new code path; no field
  receives special handling.

## See also

- [SRD 18b](18b_scenario_tree_and_scheduler.md) — scenario tree
  and `for:` comprehension semantics.
- [SRD 18c](18c_comprehension_syntax.md) — clause expression
  grammar; partition lists slot in as a new clause-expression
  source via the cursor-metadata wire.
- [SRD 60](60_cli.md) — CLI parameter parsing; quote-elision
  rule applies workload-wide, not just to `cursor`.
