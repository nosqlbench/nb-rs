# 18c: Comprehension Syntax

The full surface of the comprehension grammar — the language
that turns a clause-based iteration spec into a typed tuple
stream. Companion to SRD-18b §"The Comprehension model" (which
covers the AST + execution plumbing). This SRD covers
**syntax** — what users write — and how each form lowers to
the same canonical [`nbrs_variates::comprehension::Comprehension`]
AST.

> **Status:** Layer 1 (literal lists), Layer 4 (`where`
> predicate), Layer 7 (tuple LHS — parallel form), and the
> `for` alias key are shipped. Other layers are designed but
> not yet implemented; this SRD is the spec they land against.

---

## Why this exists

Today's clause grammar is just `var in expr` with comma-list
literal sources. That covers a small fraction of what users
actually want when expressing parameter sweeps:

- Numeric ranges (`1..1000 step 50`)
- Geometric / Fibonacci / log-spaced sequences
- Subdivision of an interval
- Sizes with SI suffixes (`1G`, `1Mi`)
- Weighted sequences for LUTs (3:A, 1:B → AAAB)
- Tuple-paired iteration (zip)
- Predicate filters

The design choice here: keep the **comprehension AST**
unchanged in shape, and grow expressiveness in the **clause
expression language**. Every clause's `<expr>` evaluates to
`Vec<Value>` via `evaluate_spec`; whether it came from a
literal, a range, a generator function, or a set composition
is invisible to the executor. New syntactic shapes lower to
new GK expression-language constructs (range operators,
literal suffixes, stdlib functions) — not to new comprehension
machinery.

The same machinery doubles as a **LUT facility**: a clause
expression that emits `[A, A, A, B]` is equally usable for
weighted op sequencing, sample-with-replacement test data, or
deterministic dispatch tables. SRD-22 §"Op Sequencing"
already does this for op templates; the comprehension grammar
generalizes it.

---

## Layered grammar

The grammar has seven independent layers. Each layer is
optional and orthogonal — you can mix any subset, and adding
one doesn't change the others.

| Layer | Adds | Where it lives |
|--|--|--|
| 1 | Literal comma lists | `parse_clause_list` (shipped) |
| 2 | Range operator `a..b`, `a..=b`, `a..b..s` | GK lexer + AST (planned) |
| 3 | Named generators (`fib`, `pow2`, `geometric`, …) | GK stdlib (planned) |
| 4 | `where <predicate>` filter | `Comprehension::filter` (shipped) |
| 5 | Set operators (`concat`, `unique`, `interleave`, …) | GK stdlib (planned) |
| 6 | SI suffix literals (`1K`, `1Mi`) | GK lexer (planned) |
| 7 | Tuple LHS / parallel-iter form | `Clause::Parallel` (planned)¹ |

¹ The AST extension is staged but not yet wired through to the
parser — see §"Tuple LHS form" below.

---

## Layer 1: Literal lists (shipped)

The simplest source: a comma-separated list of literal values.

```text
x in 1, 2, 3
x in alice, bob, carol
x in true, false
```

Per-element type detection: `1` → `U64`, `1.5` → `F64`, `true` /
`false` → `Bool`, anything else → `Str`. Mixed types in one
list are allowed; each element gets its own native type.

Whitespace around commas is irrelevant. Commas inside parens
or brackets are protected (so `f(a, b)` stays one item).

---

## Layer 2: Range operator (planned)

Borrows the Rust / Kotlin / Python convention. Half-open by
default; `..=` for closed; trailing `..step` for non-1 stride.

```text
x in 1..10                # half-open: 1, 2, ..., 9
x in 1..=10               # closed:    1, 2, ..., 10
x in 0..100..10           # 0, 10, 20, ..., 90
x in 0..=1.0..0.1         # float ranges:    0.0, 0.1, ..., 1.0
x in 10..1..-1            # descending (negative step)
```

**Subdivision** — divide a range into N equal parts — is
expressed via the step:

```text
x in 0..100..(100/5)      # 0, 20, 40, 60, 80   (5 buckets)
x in 0..=100..(100/4)     # 0, 25, 50, 75, 100  (5 endpoints)
```

Or via a `subdivide(start, end, n)` stdlib helper for
readability:

```text
x in subdivide(0, 100, 5)
```

**Semantics:**
- Range bounds and step are GK const expressions; they're
  evaluated against the parent kernel at scope-init time.
- Empty ranges (`5..5`, `5..2..1`) produce empty lists, which
  triggers the `on_empty_clause` policy (warn or strict, per
  SRD-18b).
- Negative step requires `start > end`; the inverse is an empty
  list, not an error.

**Lowering:** the range operator desugars to a stdlib function
returning `Vec<Value>` — `1..10` becomes `range(1, 10)`,
`1..=10..2` becomes `range_step_inclusive(1, 10, 2)`. The
clause-expr evaluator sees a normal function call.

---

## Layer 3: Named generators (planned)

When the source isn't a range, a stdlib function takes its
place. Each returns a `Vec<Value>` like a literal list.

| Function | Produces | Notes |
|--|--|--|
| `all(<cursor>)` | Half-open ordinal range `[start, end)` from the named cursor's resolved extent | **Shipped.** Reads `__cursor_extent_<name>_{start,end}` from the parent kernel; cursor must be declared at or above this scope and have init-resolvable range arguments. |
| `fib(n)` | First `n` Fibonacci numbers (1, 1, 2, 3, 5, …) | `n` ≥ 0 |
| `fib_until(max)` | Fibs up to and including `max` | Stops at the term `> max` |
| `pow2(n)` | `1, 2, 4, …, 2^(n-1)` | |
| `pow2_until(max)` | Powers of two ≤ `max` | |
| `geometric(start, factor, n)` | `start, start*factor, …` (n terms) | Float-friendly |
| `geometric_until(start, factor, max)` | `start, start*factor, …` ≤ `max` | |
| `binomial(n)` | `C(n,0), C(n,1), …, C(n,n)` | |
| `subdivide(start, end, n)` | Sugar for half-open range with computed step | |
| `subdivide_inclusive(start, end, n)` | Sugar for closed range with computed step | |
| `log_steps(start, end, n)` | `n` log-spaced points from `start` to `end` | |
| `linear_steps(start, end, n)` | Sugar for `subdivide_inclusive` | |

### `all(<cursor>)` — iterate a cursor's full extent

A cursor declared in GK as `cursor name = Cursor(start, end)` carries a known extent — the half-open ordinal range it spans. `all(<cursor>)` lowers that extent into a clause value list at scope-init time:

```yaml
bindings:
  cursor row = Cursor(0, 10000)

scenarios:
  test:
    - for: "xval in all(row)"
      phases: [run]
```

Each iteration of the comprehension binds `xval` to one ordinal in `[0, 10000)`. The phase that consumes `{xval}` projects it however it wants — typically by feeding it back through cursor-aware stdlib functions:

```yaml
ops:
  load:
    cql: "INSERT INTO data (id) VALUES (?)"
    args: [{ "{xval}" }]
```

**Resolvability:** the cursor's range must be **init-resolvable** (literal or compile-const). Per-cycle-dynamic ranges aren't supported — the comprehension needs the extent at scope-init time. This matches the existing cursor-extent discovery rule.

**Scope visibility:** the cursor must be declared at or above the comprehension scope. Workload-level cursors are universally visible; scenario-level cursors are visible to scenario-tree comprehensions; phase-level cursors aren't visible to enclosing scenario-level `for` clauses.

**Composes naturally** with the rest of the comprehension grammar:

```yaml
- for: "xval in all(row) where {xval} % 100 == 0"      # every 100th ordinal
  phases: [run]

- for: "xval in all(row) order halton/64"              # 64 well-spread (when halton ships)
  phases: [run]

- for: "xval in all(row) order lex/100"                # first 100
  phases: [run]
```

These are pure functions — same input always gives same
output, no kernel state mutation. They register in the GK
stdlib like every other node and compose with const-folding.

**Composition** with ranges and other expressions:

```text
x in concat(1..10, fib(8))
x in unique(pow2(10), 1..1000..100)
x in fib(50) where {x} > 1000 and {x} < 1_000_000
```

---

## Layer 4: `where` filter (shipped)

A single GK predicate evaluated against each emitted tuple,
*after* the cross-product (or zip, or union) is built. The
predicate uses `{name}` interpolation — clause-bound names and
inherited scope names appear as placeholders, expanded against
the per-tuple kernel before `eval_const_expr` runs.

```text
k in 10, 100, limit in 10, 20, 30 where {k} * {limit} < 1000
```

Predicate result must be Bool, U64, or F64 — `0` / `0.0` /
`false` filter out, anything else keeps. (Same truthiness rule
as `do_while` / `do_until` conditions per SRD-18.)

The filter is **mode-uniform** — Cartesian and Union both
honor the same single predicate, applied to every emitted
tuple regardless of which sub-space produced it.

YAML form:

```yaml
- for: "k in 10, 100, limit in 10, 20, 30"
  where: "{k} * {limit} < 1000"
  phases: [search]
```

GK-text form:

```text
k in 10, 100, limit in 10, 20, 30 where {k} * {limit} < 1000
```

Both lower to the same `Comprehension { mode: Cartesian(...),
filter: Some("{k} * {limit} < 1000") }`.

---

## Layer 5: Set operators (planned)

Stdlib functions that take lists and return lists. They live
in the same namespace as Layer-3 generators, but their input
shape is `Vec<Value>` rather than scalars.

| Function | Behavior |
|--|--|
| `concat(a, b, ...)` | Concatenate in argument order |
| `unique(a, b, ...)` | Concatenate, then dedup preserving first-occurrence order |
| `intersect(a, b, ...)` | Values appearing in every input |
| `subtract(a, b)` | Values in `a` not in `b` |
| `interleave(a, b, ...)` | Round-robin: `a₀, b₀, c₀, a₁, b₁, c₁, …` |
| `cycle(a, n)` | `a` repeated `n` times |
| `reverse(a)` | Reverse order |
| `take(a, n)` | First `n` |
| `skip(a, n)` | All but first `n` |

These compose cleanly because every clause expression is just
"something that evaluates to a `Vec<Value>`".

---

## Layer 6: SI suffix literals (planned)

Recognized in any numeric literal position. Two suffix
families per the IEC convention:

| Suffix | Multiplier | Example |
|--|--|--|
| `K`, `M`, `G`, `T`, `P` | 10³, 10⁶, 10⁹, 10¹², 10¹⁵ | `1K = 1_000` |
| `Ki`, `Mi`, `Gi`, `Ti`, `Pi` | 2¹⁰, 2²⁰, 2³⁰, 2⁴⁰, 2⁵⁰ | `1Ki = 1_024` |
| `m`, `u`, `n` | 10⁻³, 10⁻⁶, 10⁻⁹ | `5m = 0.005` |

Suffixes apply to integer literals (`100K` → U64) and float
literals (`1.5G` → U64 if integral, F64 otherwise; resolution
follows the existing literal type-detection rules). The lexer
handles them; the rest of the language is unchanged.

```text
x in 1K, 1M, 1G                   # U64 list
x in 1K..1M..100K                 # range with SI bounds and step
x in 0.5G..2G..0.25G              # float range
x in 1Ki, 1Mi, 1Gi                # binary multipliers
```

Suffixes never appear in identifier positions; `Kilometers`
remains a valid identifier.

---

## Layer 7: Tuple LHS — parallel iteration

For clauses where multiple variables advance in lockstep
rather than as a cross product. Bundles `var₁, …, varₙ` on
the LHS and `expr₁, …, exprₙ` on the RHS.

### 7a. Parallel sources (zip)

```text
(x, y) in (1..10, 100..1000..100)
(host, port, weight) in (hosts(), ports(), 1..=10)
```

Zip semantics — sources advance in lockstep, stopping at the
shortest. Different from cross-product:

| Form | Tuples |
|--|--|
| `x in 1..3, y in a..c`         | (1,a) (1,b) (1,c) (2,a) (2,b) (2,c) — 6 |
| `(x, y) in (1..3, a..c)`       | (1,a) (2,b) — 2 |

Composes freely with other clauses (cross-product across
parallel groups):

```text
(x, y) in (1..10, 100..1000..100), z in 5..50..5
```

Bundle `(x, y)` parallel-iter, cross-product with `z`.

**Length-mismatch policy:** strict by default — the comprehension
errors at scope-init if `len(expr_i)` differ across the parallel
group. Truncate-to-shortest and cycle-the-shorter behaviors are
available via stdlib helpers (`zip_truncate(a, b)`,
`zip_cycle(a, b)`) which the clause writer applies explicitly.

### 7b. Tuple destructure (single source produces tuples)

```text
(host, port) in pairs_from_csv("hosts.csv")
(k, expected_recall) in test_dataset()
```

Single RHS source produces a list of tuples; LHS unpacks
positionally. Useful for stdlib functions that emit paired or
N-ary data.

**Implementation note:** destructure requires either a
`Value::Tuple` variant in GK or a `Vec<Vec<Value>>` return
shape from the clause evaluator. Parallel form (7a) ships
first since it doesn't need new GK type machinery.

### AST representation

```rust
pub struct Clause {
    pub vars: Vec<String>,         // ≥1 names
    pub source: ClauseSource,
}

pub enum ClauseSource {
    /// Single expression yields Vec<Value> (when vars.len() == 1)
    /// or Vec<Vec<Value>> (when vars.len() > 1, destructure form).
    Single(String),
    /// One expression per var; sources zip in lockstep.
    /// vars.len() == exprs.len() (≥ 2).
    Parallel(Vec<String>),
}
```

Single-var clauses (the existing common case) are
`Clause { vars: vec!["k"], source: Single("…") }` — same data,
just lifted into the new shape. Existing AST consumers see no
behavior change.

---

## Sequencer-style expansions (LUT facility)

Stdlib functions that lift the SRD-22 op sequencer into the
clause-expression layer. They take ratio-weighted items and
produce a list whose length and element distribution matches
the chosen sequencing strategy.

| Function | Equivalent SRD-22 strategy | Output shape |
|--|--|--|
| `bucket(items, ratios)` | Round-robin from buckets | Interleaved by ratio |
| `concat_seq(items, ratios)` | All of first, then all of second, … | Contiguous runs |
| `interval_seq(items, ratios)` | Evenly spaced by frequency | Spread across stanza |

Each takes parallel lists — `items[i]` paired with `ratios[i]`
— and returns a `Vec<Value>` of length `sum(ratios)`. The
algorithms are exactly those in `nbrs-activity::opseq` (see
`build_bucket_lut`, `build_concat_lut`, `build_interval_lut`);
the comprehension stdlib reuses them so behavior is
guaranteed-identical to the executor's op-sequencing path.

### Examples

**Weighted comprehension (LUT use case):**

```text
profile in bucket(["ann", "scan", "fetch"], [3, 1, 2])
```

Yields `["ann", "scan", "fetch", "ann", "scan", "fetch", "ann", "scan"]`
— 6 elements (= 3+1+2), interleaved round-robin from
ratio-sized buckets. Same algorithm as the op sequencer's
default mode.

**Concat — contiguous runs:**

```text
phase in concat_seq(["warmup", "bench", "cooldown"], [10, 100, 5])
```

Yields 10 warmups, then 100 benches, then 5 cooldowns —
useful as a phase-replay LUT.

**Interval — evenly spaced:**

```text
op in interval_seq(["read", "write"], [3, 1])
```

Yields a length-4 list with `write` evenly spread among the
three `read`s.

### YAML sugar for ratio:item form

The bucket / concat / interval functions accept the
nosqlbench-classic `"3:ann, 1:scan, 2:fetch"` shape too, via
a sibling helper:

```text
profile in bucket("3:ann, 1:scan, 2:fetch")
```

Internally `bucket(text)` parses the ratio-prefix shorthand
into the parallel `(items, ratios)` form. The dual form lets
authors choose: explicit lists (programmatic, easy to compose
with ranges) or the colon shorthand (terse, familiar to
nosqlbench users).

### As a LUT — pairing with the cycle index

The sequencer functions become a true LUT when paired with a
parent counter — typically a do-loop's counter or an outer
for-each iter var:

```yaml
- do_while: "{i} < 1000"
  counter: i
  phases:
    - for: "op in bucket('3:read, 1:write')"
      where: "{i} % 4 == {cycle_idx}"
      phases: [run]
```

Or more naturally — the comprehension itself emits the LUT
once and every iteration of the outer loop indexes into it
(future feature: `lookup(LUT, i)` stdlib function). For
today's grammar, post-filter on `{i}` against the LUT-emitted
positions is the workaround.

---

## Canonical text form vs YAML sugar

The text form passed to
[`parse_comprehension_text`](../../nbrs-variates/src/comprehension/parse.rs)
is the canonical surface. YAML accepts two interchangeable
shapes; both lower to the same `Comprehension` AST.

### Inline (one-liner) form

`for:` / `for_each:` carries the full GK comprehension text —
clauses plus any `where` predicate plus any `order` spec — in
one string. Most concise; useful for short specs.

```yaml
- for: "k in 1..10"
- for: "k in 1..10 where {k} > 5"
- for: "k in 1..10 where {k} > 5 order extrema/1"
- for: "k in 1..100, l in 1..100 where {k}*{l} <= 1000 order halton/64"
```

### Sibling-key form

Separate `where:` and `order:` keys live alongside `for:`,
which then carries only the clause list. Useful for long
predicates, multi-clause orders, or when a key benefits from
keyword arguments expressed as a YAML object.

```yaml
- for: "k in 1..10"
  where: "{k} > 5"
  order: extrema/1
```

### Equivalence and precedence

The two forms produce the same AST. If both inline tokens
(`where`, `order`) and explicit keys appear in the same node,
the explicit keys win — so misconfigured workloads don't
silently mix or merge. The parser merges:

```yaml
- for: "k in 1..10 order halton/50"
# is exactly equivalent to:
- for: "k in 1..10"
  order: halton/50
```

### Common YAML shape mappings

| YAML | Equivalent GK text |
|--|--|
| `for: "k in 1..10"` | `k in 1..10` |
| `for_each: "k in 1..10"` | `k in 1..10` (synonym) |
| `for: "k in 1..10", where: "{k} > 5"` | `k in 1..10 where {k} > 5` |
| `for: "k in 1..10", order: extrema/1` | `k in 1..10 order extrema/1` |
| `for: "k in 1..10 where {k} > 5 order extrema/1"` | same (inline) |
| `for_each: ["k in 10, l in 10,20", "k in 100, l in 100,200"]` | `k in 10, l in 10,20, k in 100, l in 100,200` (Union mode) |
| `for_combinations: { k: 1..10, l: 1..3 }` | `k in 1..10, l in 1..3` |

YAML-shape detection (string vs list vs object) lives in
`nbrs-workload`; the GK comprehension parser only sees text.
Both paths produce the same `Comprehension` AST.

---

## Worked end-to-end example

```yaml
phases:
  search:
    adapter: cql
    cycles: 100
    ops:
      query:
        cql: "SELECT * FROM ks.{table} WHERE k = ? LIMIT {limit}"
scenarios:
  bench:
    - for: "(table, k_dim) in (vector_tables(), 1..=5),
            limit in pow2(8),
            profile in bucket('3:ann, 1:exact')"
      where: "{k_dim} * {limit} <= 1024"
      phases: [search]
```

Reads as:

1. `(table, k_dim)` zip: each table paired with its k-dimension
   index 1..5.
2. Cross-product with `limit` (pow2: 1, 2, 4, …, 128).
3. Cross-product with `profile` (bucket-weighted: ann appears
   3× more often than exact, length-4 list).
4. Filter to tuples whose `k_dim * limit ≤ 1024`.
5. For each surviving tuple, run the `search` phase 100 cycles.

Lowered Comprehension AST (logical):

```rust
Comprehension {
    mode: Cartesian(vec![
        // (table, k_dim) parallel
        Clause { vars: vec!["table", "k_dim"], source: Parallel(vec!["vector_tables()", "1..=5"]) },
        // limit
        Clause { vars: vec!["limit"], source: Single("pow2(8)") },
        // profile
        Clause { vars: vec!["profile"], source: Single("bucket('3:ann, 1:exact')") },
    ]),
    filter: Some("{k_dim} * {limit} <= 1024".to_string()),
}
```

Every layer is independently optional and orthogonal:

- Drop the `where` → no filter, all tuples pass.
- Drop the parallel `(table, k_dim)` → cross-product instead of
  zip.
- Replace `pow2(8)` with `1, 2, 4, 8, 16` → identical behavior,
  literal list instead of generator.
- Replace `bucket(...)` with a uniform list `'ann', 'exact'` →
  weighting goes away.

---

## Why this shape

**One AST, many surfaces.** The comprehension AST is the
fixed point. Everything above — YAML sugar, GK text grammar,
stdlib functions, range operator, SI suffixes — lowers to the
same `Comprehension { mode, filter }`. New surface forms add
parser code or stdlib nodes; they don't add comprehension
semantics. This keeps the executor, scope tree, and
synthesis paths stable as the grammar grows.

**Composition over special cases.** Ranges, generators, set
operators, sequencers, and SI literals all evaluate to
`Vec<Value>`. Any expression-language feature that produces a
list is automatically a clause source — no per-feature
plumbing through the comprehension layer. The same `bucket()`
function works in a clause expression, in a regular GK binding,
in a const-folded final, and in a workload param.

**LUT facility for free.** The sequencer functions (bucket,
concat, interval) reuse `nbrs-activity::opseq`'s algorithms
unchanged. SRD-22's op-sequencing strategies become available
to any clause; weighted dispatch tables, replay sequences, and
deterministic-but-non-uniform sweeps all compose cleanly.

**Filter as a first-class layer.** `where` is single-predicate
by design — chained filtering is a single boolean expression
(`and`, `or`, `not`). This keeps the AST shape simple and
makes `Comprehension` round-trip cleanly through serde for
diagnostic output. The expression grammar inside `where` is
fully GK, so users get the full power of GK's stdlib (string
ops, regex, range checks, etc.) without the comprehension
layer needing to know about any of it.

---

## Implementation order

1. **Layer 7a — parallel form.** Pure parser + AST extension;
   reuses existing `enumerate_tuples` with a zip step. Largest
   user-visible win for the smallest grammar addition.
2. **Layer 2 — range operator.** Lexer + AST + desugar to
   stdlib `range(...)`. Needs no comprehension changes.
3. **Layer 6 — SI suffixes.** Lexer-only. Orthogonal,
   safe to land any time after lexer support is in.
4. **Layer 3 — named generators** (`fib`, `pow2`, `geometric`,
   `binomial`, `subdivide`, `log_steps`). Each is one stdlib
   node + a short test. Land in any order.
5. **Sequencer expansions** (`bucket`, `concat_seq`,
   `interval_seq`). Reuse `nbrs-activity::opseq` algorithms.
6. **Layer 5 — set operators.** Trivial once lists are
   first-class clause sources.
7. **Layer 7b — destructure form.** Requires `Value::Tuple` or
   tuple-list return shape; biggest type-system change.

Each item lands as its own SRD-update + tests + example. The
parking-lot ordering is intentional: high-payoff syntactic wins
first, larger surgery last.

---

## Cross-references

- [SRD-18 — Control Flow](18_control_flow.md): the larger
  control-flow grammar (`if`, `do_while`, `do_until`, phases).
- [SRD-18b — Scenario Tree and Scheduler](18b_scenario_tree_and_scheduler.md):
  the AST + execution model the syntax lowers into.
- [SRD-18d — Comprehension Traversal Order](18d_comprehension_traversal_order.md):
  emission order — peer of `where`, applied after filter, to
  control which tuples come out first (extrema, shells,
  space-filling) and how many.
- [SRD-10 — GK Language](10_gk_language.md): the expression
  grammar that clause expressions and the `where` predicate
  both use.
- [SRD-22 — Op Sequencing](22_op_sequencing.md): the bucket /
  concat / interval algorithms that the sequencer-style
  expansions reuse.
- `docs/internals/50_comprehensions_first_class.md`: the
  migration plan that built the canonical `Comprehension` model.
- `examples/workloads/for_each_forms.yaml`: every shipped form
  with side-by-side YAML + GK text + iteration shape.
