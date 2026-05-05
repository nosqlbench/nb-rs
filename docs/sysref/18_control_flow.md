# 18: Uniform Control Flow Primitives

How `for_each`, `do_while`, and `do_until` work as composable,
nestable control flow constructs at any level of the scenario tree.

---

## Principles

1. **`cycle` is immutable.** It's the activity-assigned ordinal —
   the identity of the work unit. Interior loops do not mutate it.
   All ops within a cycle's stanza are part of that cycle's lineage,
   regardless of how many loops they pass through.

2. **Loop counters are explicit.** If a loop needs an iteration
   index, it declares a named `counter` variable. This is a GK
   scope value — visible to all children via the standard scope
   composition mechanism (auto-externs, `shared`/`final`).

3. **Three constructs, one shape.** `for_each`, `do_while`, and
   `do_until` are all tree nodes that wrap children. They differ
   only in termination logic:
   - `for_each` — pre-resolved value list, iterate all
   - `do_while` — evaluate condition after each iteration, continue while true
   - `do_until` — evaluate condition after each iteration, stop when true

4. **Nestable to arbitrary depth.** Any construct can contain any
   other construct. A `for_each` can contain a `do_until` which
   contains another `for_each`. The execution plan is flattened
   recursively.

5. **Uniform at all levels.** The same constructs work at:
   - Scenario level (wrapping phases)
   - Phase level (on the phase's `for_each` field)
   - Op level (future: wrapping individual ops within a stanza)
   The GK scope model (SRD 13c) handles variable resolution
   at every level.

---

## Cycle Identity and Data Lineage

A cycle is the fundamental unit of work in an activity. When a
cycle spawns additional operations through loop constructs, those
operations are part of the same cycle's **causal lineage**:

```
cycle 42 → stanza ops → [do_until loop → op A, op B] → op C
```

All of op A, op B (across loop iterations), and op C share
cycle 42. The loop counter is a separate variable — it doesn't
replace or shadow `cycle`.

If an inner op needs a unique ordinal (e.g., for deterministic
data generation within a loop), it derives it from the loop
counter variable combined with `cycle`:

```
inner_id := hash(cycle + attempt * 1000000)
```

This preserves determinism: same cycle + same attempt = same result.

---

## Syntax

### for_each (scenario level)

`for_each` accepts six syntactic shapes that collapse to three
runtime variants — `ForEach` (one var, single value list),
`ForCombinations` (Cartesian product of distinct dims), and
`ForEachUnion` (concatenation of multiple Cartesian sub-spaces).
The shape is auto-detected from the YAML structure plus the
variable-name reuse pattern; see §"Detection rule" below.

#### Form 1 — single var (string)

```yaml
- for_each: "profile in matching_profiles('{dataset}', '{prefix}')"
  phases:
    - drop_table
    - create_table
```

One variable, one value list. The simplest form.

#### Form 2 — multi-var inline (string, distinct vars)

```yaml
- for_each: "profile in {profiles}, k in {k_values}"
  phases:
    - search
```

Cartesian product of distinct vars. Equivalent to nested
`for_each`s in iteration order, but flatter syntactically.

#### Form 3 — multi-var array (list of single-clause strings, distinct vars)

```yaml
- for_each:
    - "profile in {profiles}"
    - "k in {k_values}"
  phases:
    - search
```

Identical semantics to Form 2 — cartesian over distinct vars.
Use when individual specs are long enough that the inline form
hurts readability.

#### Form 4 — multi-var array, single entry (multi-clause)

```yaml
- for_each:
    - "profile in {profiles}, k in {k_values}, limit in {limits}"
  phases:
    - search
```

A single multi-clause sub-space — equivalent to Form 2 with
the same clauses inlined. Most useful as a starting shape that
can grow into Form 6 by adding more entries.

#### Form 5 — repeated var (union by clause grouping)

```yaml
- for_each: "x in 1, x in 2, x in 3"
  phases: [step]
```

```yaml
- for_each:
    - "x in 1"
    - "x in 2"
    - "x in 3"
  phases: [step]
```

When the *same* variable name appears in multiple positions,
each position is its own single-var sub-space and the iteration
enumerates the union. The two forms above are equivalent and
both produce three iterations: x=1, x=2, x=3. (This is also
equivalent to `for_each: "x in 1,2,3"` in the simple case;
it's most useful when the values come from different sources.)

#### Form 6 — multi-var array with repeated vars across entries (the main use case)

```yaml
- for_each:
    - "index_state in building, k in 10,  limit in 10,20,30"
    - "index_state in building, k in 100, limit in 100,200,300"
  phases:
    - ann_query
```

Each list entry is its own multi-dim Cartesian sub-space; the
iteration enumerates the **concatenation** of those products.
Variable names typically repeat across entries so children
see a uniform binding shape.

Use this when only certain combinations of multiple dimensions
are valid — e.g. `k=10` only makes sense paired with limits in
one range while `k=100` needs a different range. The full
Cartesian k×limit would visit invalid corners; the union form
skips them by construction.

The example above produces 6 tuples
(`{(k=10, limit=10), (k=10, limit=20), (k=10, limit=30),
(k=100, limit=100), (k=100, limit=200), (k=100, limit=300)}`)
instead of the 12 tuples a Cartesian k×limit would generate.

### Detection rule

The parser collects every `(var, expr)` pair across the spec
(string clauses or array entries become structural sub-spaces).

- **No variable name appears more than once** ⇒ `ForCombinations`
  (Cartesian) — or `ForEach` if there is exactly one pair.
- **Any variable name appears more than once** ⇒ `ForEachUnion`,
  with structural sub-spaces preserved (one sub-space per
  top-level clause for string form; one per array entry for
  array form).

The clause splitter respects parens, brackets, braces, and
recognizes a clause boundary only when a top-level comma is
followed by `<ident> in `. This means values containing commas
(`limit in 10,20,30`) and function calls
(`matching_profiles('a', 'b')`) survive the split correctly.

### Empty iteration sources

A `for_each` whose expression resolves to zero values, a
`for_combinations` dimension that resolves to zero values, or a
`for_each_union` sub-space whose Cartesian product is empty —
all produce zero iterations of their children.

| Mode | Behavior |
|------|----------|
| Default | Warning to stderr / session log, naming the offending dimension and (for `for_each_union`) which sub-space (`N/M`) collapsed. Run continues with the other sub-spaces. |
| `--strict` | Hard error — fails the run with the same diagnostic, before any of that sub-space's children execute. |

This is sometimes intentional — toggling a sub-space off by
setting one of its dimensions to an empty value list — and
sometimes a workload-config error (typo, missing parameter,
filter that didn't match anything). The default-mode warning
gives operators visibility either way; `--strict` is the
correct setting for CI / production where silent zero
iteration is unacceptable. See SRD-15 §"Empty Iteration
Sources" for the canonical contract.

### Comparison: cartesian vs. union

For a workload with two dimensions, three values each:

| Form | Iterations |
|---|---|
| `for_combinations: "x in 1,2,3, y in a,b,c"` | 9 (3×3 Cartesian) |
| `for_each: ["x in 1, y in a", "x in 2, y in b", "x in 3, y in c"]` | 3 (union — three matched pairs) |

Pick `for_combinations` (or distinct-var `for_each`) when every
combination is meaningful. Pick the union form when the valid
combinations are a sparse subset of the full Cartesian product.

### do_while (scenario level)

```yaml
scenarios:
  default:
    - do_while: "remaining_profiles > 0"
      counter: batch
      phases:
        - process_batch
```

### do_until (scenario level)

```yaml
scenarios:
  default:
    - load_data
    - do_until: "empty"
      counter: attempt
      phases:
        - await_compaction
```

### scenario: <name> (logical inclusion)

Wherever a phase name can appear in a scenario tree — at the
top level of a scenario, in a `phases:` list under any of the
iteration constructs above — an entry of the form
`scenario: <name>` includes the named scenario at that point.
The included scenario's nodes are spliced in logically; the
include wrapper is preserved in the parsed tree so the scope
tree retains the include hierarchy and the renderer can show
the operator which group of phases came from which scenario.

```yaml
scenarios:
  smoke:
    - schema
    - rampup

  bench:
    - scenario: smoke
    - for_each: "k in 10,100"
      phases:
        - scenario: smoke   # may also appear nested
        - search
```

#### scenarios: [list] (plural form)

When you're stitching several named scenarios together at one
point in the tree, the singular form is repetitive
(`- scenario: a`, `- scenario: b`, …). The plural list form
expands to one `scenario: <name>` per element:

```yaml
scenarios:
  rampup_fknn:
    - schema
    - rampup
    - await_index

  query_fknn:
    - for_each: "k in {k_values}"
      phases:
        - search

  test_fknn:
    - scenarios:
        - rampup_fknn
        - query_fknn
```

Each list entry can be a bare scenario name (string) or any
other scenario-node shape (`{ scenario: name }`,
`{ for_each: …, phases: … }`, etc.) — entries are
heterogeneous and the parser routes each one through the
standard `parse_scenario_nodes` path. The two forms are
equivalent; pick whichever reads better at the call site.

Resolution runs once after the YAML parse, with cycle
detection. `A` referencing `B` referencing `A` (transitively or
directly) errors at parse time with the cycle path in the
message; an unknown scenario name errors with a list of the
known scenarios.

The diamond case — `A` reaches `C` via two distinct paths but
never recursively from itself — is allowed and resolves once
per occurrence.

At runtime the wrapper is **transparent**: there is no extra
binding scope, no extra label, no extra concurrency boundary.
Iteration variables and parameter bindings flow through it
unchanged. The wrapper exists only for diagnostic clarity in
the scope tree, `dryrun=phase` output, and TUI rendering.

### Phase-level for_each (lifted into tree)

```yaml
phases:
  search:
    for_each: "k in {k_values}"
    ops:
      select_ann: ...
```

This is equivalent to a `ForEach` node wrapping the phase in
the scenario tree. The runner lifts it during plan building.

---

## Condition Evaluation

### for_each

The expression is evaluated once at loop entry. The result is
a comma-separated string. Each value drives one iteration.
The iteration variable is injected as a GK init constant.

### do_while / do_until

The condition is a GK expression or a result body check:

- **GK expression**: evaluated after each iteration using the
  current scope state. Returns a boolean (or u64 where 0=false).
- **`"empty"`**: special keyword — checks if the last op's result
  body has zero rows (same as poll wrapper's await_empty).

The condition is evaluated AFTER each iteration (guarantee at
least one execution), matching do-while/do-until semantics in
most languages.

### Counter variable

When `counter: name` is specified, the variable starts at 0 and
increments by 1 per iteration. It's available to all children as
a GK scope value via `shared` semantics — inner scopes see it,
and it carries across iterations.

---

## Implementation

### ScenarioNode enum

```rust
enum ScenarioNode {
    Phase(String),
    ForEach { spec: String, children: Vec<ScenarioNode> },
    /// Cartesian product of distinct dimensions.
    ForCombinations { specs: Vec<(String, String)>, children: Vec<ScenarioNode> },
    /// Union (concatenation) of multiple Cartesian sub-spaces.
    /// Each inner `Vec` is one sub-space; the runtime walks each
    /// in turn and yields the concatenation of their tuples.
    ForEachUnion { sets: Vec<Vec<(String, String)>>, children: Vec<ScenarioNode> },
    /// Logical inclusion of another scenario by name. `children`
    /// is populated post-parse from the referenced scenario;
    /// transparent at runtime, structural in the scope tree.
    IncludedScenario { name: String, children: Vec<ScenarioNode> },
    DoWhile { condition: String, counter: Option<String>, children: Vec<ScenarioNode> },
    DoUntil { condition: String, counter: Option<String>, children: Vec<ScenarioNode> },
}
```

### Plan flattening

`for_each` is pre-resolved into a flat list of `(phase, bindings)`.
`do_while` and `do_until` cannot be pre-resolved — they're
evaluated at runtime. The execution plan has two types of entries:

1. **Static entries** (from `Phase` and `ForEach`): fully resolved
   at plan time, executed sequentially.
2. **Dynamic entries** (from `DoWhile`/`DoUntil`): evaluated
   at runtime, looping until the condition is met.

### GK scope integration

Loop variables (iteration values, counters) are injected as GK
init constants into the inner scope. This uses the same mechanism
as `for_each` — `init {var} = "{value}"` prepended to the GK
source before compilation. The variable is then available as a
GK output for op field resolution, relevancy config, and all
other GK-dependent paths.

No side-channel text substitution. All values flow through GK.

---

## Interaction with Other Features

- **`shared`/`final`**: Loop counters use `shared` semantics.
  `final` variables from outer scopes cannot be modified by loops.
- **Scope modes** (`loop_scope`/`iter_scope`): Apply to `for_each`.
  `do_while`/`do_until` always inherit from the enclosing scope.
- **Diagnostics** (`dryrun=phase,gk`): `for_each` shows all
  iterations. `do_while`/`do_until` show one iteration (the first).
- **Metrics**: Each loop iteration's ops contribute to the same
  activity's metrics (same cycle). The counter variable can be
  used as a dimensional label for per-iteration tracking.
