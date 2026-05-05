# Implementation Plan: Comprehension Grammar (SRD-18c / 18d / 18e)

Companion to:
- [SRD 18c — Comprehension Syntax](../sysref/18c_comprehension_syntax.md)
- [SRD 18d — Comprehension Traversal Order](../sysref/18d_comprehension_traversal_order.md)
- [SRD 18e — Comprehension Canonical Reference](../sysref/18e_comprehension_canonical_reference.md)
- [Comprehensions Open Items](comprehensions_open_items.md) — post-audit residue (deferred trait refactors, gated features, stylistic items)

The SRDs specify *what* the grammar is. This plan specifies
*how and when* the unfinished pieces land. Each push is
independently mergeable, leaves the engine in a working
state, and ends with a green CI.

> **Status**: punch list in flight. Keep the table at the
> top current as work lands.

---

## Progress checklist

| Push | Title | Status |
|------|-------|--------|
| 1 | AST + Cartesian + Union + filter + lex/reverse_lex/diagonal/antidiagonal/extrema/shells orderings | ✅ shipped (baseline) |
| 2 | Layer 7a — parallel-iter clauses (AST migration to `vars` + `ClauseSource`) | ✅ shipped — `Clause { vars, source: ClauseSource::{Single,Parallel} }`, parser recognises `(a, b) in (e1, e2)` form, `enumerate_tuples` zips parallel groups in lockstep with strict length-mismatch error, parallel groups count as one axis for `clause_sizes` (Cartesian × parallel-axis-cardinality, not N axes); 9 parse tests + 3 synthesis-iterate tests |
| 3 | Layer 2 — range operator (`a..b`, `a..=b`, `a..b..s`) | ✅ shipped — comprehension-layer expansion + 13 tests |
| 4 | Layer 6 — SI suffix literals (`1K`, `1Mi`, `5m`) | ✅ shipped — lexer + 10 tests |
| 5 | Halton ordering | ✅ shipped — 5 tests |
| 5b | Sobol ordering | ⏳ deferred — needs Joe-Kuo direction-number table |
| 5c | LHS ordering (Latin Hypercube with seed) | ✅ shipped — Fisher-Yates seeded permutation + 6 tests |
| 7 | Layer 3 — named generators (`fib`, `pow2`, `geometric`, `subdivide`, `log_steps`, `binomial`) | ✅ shipped — comprehension-layer dispatcher + 11 tests |
| 8 | Sequencer expansions (`bucket`, `concat_seq`, `interval_seq`) — reuses `nbrs-activity::opseq` algorithms | ✅ shipped — both parallel-list and ratio-prefix shorthand forms + 4 tests |
| 9 | Layer 5 — set operators (`concat`, `unique`, `interleave`, `intersect`, `subtract`, `cycle`, `reverse`, `take`, `skip`) | ✅ shipped — comprehension-layer dispatcher with recursive arg eval + 10 tests |
| 10 | Index-space ordering rejection on Union mode (`validate_order_for_mode`) | ✅ shipped — 7 tests |
| 11 | Layer 7b — destructure clauses (`(host, port) in pairs_csv()`) — depends on `Value::Tuple` | ⏳ deferred — gated on GK type extension |
| 12 | `order: custom(<gk-fn>)` — depends on Push 11 | ⏳ deferred — same `Value::Tuple` dep |

Push 10 is small and orthogonal; can land at any time
between pushes 1 and 11.

---

## Audit-driven gap status

Tracks the design gaps SRD-18e closes vs. the implementation
of those gaps. SRD-18e itself ships as the design artifact;
the implementation work is the rest of this plan.

### Spec gaps closed by SRD-18e

| # | Gap | Status |
|---|-----|--------|
| C1 | No single canonical AST view across 18c+18d | ✅ SRD-18e §"The canonical AST" |
| C2 | Cartesian-vs-Union detection rule undocumented | ✅ SRD-18e §"Mode detection" |
| C3 | Coordinate-set contract (what children see) undocumented | ✅ SRD-18e §"Coordinate-set contract" |
| C4 | Filter+order composition for index-space orderings underspecified | ✅ SRD-18e §"Index-space contract for orderings" |
| C5 | Union mode + non-lex orderings undefined | ✅ SRD-18e §"Union mode + non-lex orderings" |
| C6 | `where` predicate semantics on missing names + types | ✅ SRD-18e §"`where` predicate semantics" |
| C7 | Layer 7a vs 7b split, `Value::Tuple` dependency | ✅ SRD-18e §"Layer 7 extension path" |

### Implementation gaps to close (per push table above)

| # | Gap | Lands in |
|---|-----|----------|
| I1 | Halton / Sobol / Lhs / Custom orderings parse but error at eval | Pushes 5/6/12 |
| I2 | `Clause` is single-var; can't express parallel-iter | ✅ Push 2 (shipped) |
| I3 | Range operator not lexed | Push 3 |
| I4 | SI suffixes not lexed | Push 4 |
| I5 | Named generators not in stdlib (`fib`, `pow2`, …) — only `all(<cursor>)` ships | Push 7 |
| I6 | Set operators not in stdlib | Push 9 |
| I7 | Sequencer expansions not in clause-expression layer | Push 8 |
| I8 | Index-space orderings on Union pass-through (should reject) | Push 10 |
| I9 | Destructure form not parsed; `Value::Tuple` not in GK | Push 11 |

---

## Push 1 — completed

The current shipping baseline (visible in
`nbrs-variates/src/comprehension/`):

- [x] Full `Comprehension` AST (mode + filter + order)
- [x] `ComprehensionMode::{Cartesian, Union}` with name-
      repeat detection
- [x] `Clause { var, expr }` (single-var)
- [x] `parse_comprehension_text` parses `<clauses> [where
      <pred>] [order <spec>]`
- [x] `parse_order_spec` parses all 10 strategies (terse
      `name/N` + keyword `name(...)`)
- [x] 6 ordering implementations: `lex`, `reverse_lex`,
      `diagonal`, `antidiagonal`, `extrema`, `shells`
- [x] `coordinate_names()` dedup'd first-occurrence order
- [x] `flat_clauses()` for Union expansion
- [x] Filter predicate: bool-result contract, identifier
      interpolation
- [x] `all(<cursor>)` clause source (cursor-extent lift)

---

## Push 2 — Layer 7a parallel-iter clauses *(closes I2)*

Most-leverage syntactic addition; user-visible win for
zip iteration. Pure parser + AST extension.

- [x] Migrate `Clause { var, expr }` →
      `Clause { vars: Vec<String>, source: ClauseSource }`.
      Single-var construction sites use
      `Clause::new(v, e)` (now constructs
      `Clause { vars: vec![v], source: ClauseSource::Single(e) }`).
- [x] Add `ClauseSource::{Single(String), Parallel(Vec<String>)}`
      (exported from `nbrs_variates::comprehension`).
- [x] Extend `parse_clause` to recognise `(var₁, var₂,
      ...) in (expr₁, expr₂, ...)`. Required: parens on
      both sides — single-side paren produces a
      `parentheses on both sides` error; bare comma
      separation rejected via the existing splitter's
      `<ident> in ` boundary rule. `is_clause_boundary`
      extended to also accept `(<idents>) in ` so a
      parallel-iter clause can follow another clause in a
      list.
- [x] Extend `enumerate_tuples` to zip Parallel sources.
      Strict length-mismatch (error at iteration time);
      mismatch message names the offending expression
      index and points at `zip_truncate(...)` /
      `zip_cycle(...)` as the explicit-opt-in alternatives.
- [x] `coordinate_names()` accommodates multi-name
      clauses — iterates `clause.vars`, deduplicating in
      first-occurrence order.
- [x] Index-space orderings treat a parallel group as one
      lattice axis. `compute_clause_sizes` reports
      `min(len(expr_i))` per parallel clause (one entry,
      not N), so the lattice cardinality stays correct
      under halton/extrema/shells/etc.
- [x] Tests: 9 parse tests (2-var, 3-var, function-call
      RHS, paren-only-one-side rejection, count
      mismatch, single-var-in-parens rejection, parallel
      clause boundary in list, mixed parallel + single,
      invalid var name) + 3 synthesis-iterate tests
      (lockstep zip, length-mismatch error, parallel +
      cross-product mix). Activity-side runtime/premap
      iterate paths take `&[Clause]` directly — no
      `(String, String)` flattening that would lose the
      parallel-vs-single distinction.

**Migration cost (actual)**: ~25 field-access sites across
`nbrs-variates`, `nbrs-workload`, `nbrs-activity`. The
`var()` / `expr()` accessor methods on `Clause` enabled a
mechanical `c.var` → `c.var()` rewrite for all single-var
call sites, with `enumerate_tuples` /
`compute_clause_sizes` / `iterate_scope` /
`runtime_iterate` / `premap_iterate` widening from
`&[(String, String)]` to `&[Clause]` to carry the
parallel-vs-single distinction end-to-end.
Mechanical refactor. The `Clause::new(v, e)` constructor
preserved for backward-compat (now expanding to
`Clause { vars: vec![v.into()], source:
ClauseSource::Single(e.into()) }`).

---

## Push 3 — Layer 2 range operator *(closes I3)*

Lexer + AST + desugar to stdlib `range(...)`. Independent of
any other push.

- [ ] GK lexer recognises `..`, `..=`, and trailing
      `..step`.
- [ ] Lexer disambiguates `1..10` (range) from `1.0..10`
      (float `1.0` followed by `..10`) — the half-open
      range operator wins; no float can end with `.`
      followed by `.`.
- [ ] AST extension: new `Expr::Range { start, end,
      inclusive, step }` or desugar at parse-time directly
      to `range(start, end[, step])` / `range_inclusive(...)`.
      The latter is simpler.
- [ ] Stdlib functions `range(a, b)`,
      `range_inclusive(a, b)`, `range_step(a, b, s)`,
      `range_step_inclusive(a, b, s)` — each returns
      `Vec<Value>`. Empty for `start == end` (half-open) or
      sign-mismatched step.
- [ ] Float ranges supported (`0..=1.0..0.1`) — int/float
      type stays consistent across all elements.
- [ ] Negative step requires `start > end`; inverse is
      empty (not error).
- [ ] Tests: half-open / closed / step / negative step /
      empty / float bounds / SI-suffix bounds (cross-
      reference Push 4).

---

## Push 4 — Layer 6 SI suffix literals *(closes I4)*

Lexer-only orthogonal addition. Safe to land alongside or
after Push 3.

- [ ] Lexer recognises `K`, `M`, `G`, `T`, `P` (decimal),
      `Ki`, `Mi`, `Gi`, `Ti`, `Pi` (binary IEC),
      `m`, `u`, `n` (sub-unit) on numeric literal trailers.
- [ ] Suffix only recognised after a valid numeric literal
      with no whitespace before it. `1K` → `1000`;
      `1 K` is identifier `K` after literal `1`.
- [ ] Type promotion follows existing literal-detection
      rules: integer literal stays `U64` if integral after
      multiplication; float literal stays `F64`.
- [ ] `Kilometers`, `Megaparsecs`, etc. are still valid
      identifiers — suffixes never match in identifier
      position.
- [ ] Tests: every suffix, integer + float bases, large
      values (`1Pi`), negative values, range integration
      (`1K..1M..100K`).

---

## Push 5 — Halton + Sobol orderings *(closes I1 partial)*

Both replace existing `Err("not yet implemented")` stubs in
`comprehension::order::apply_order`.

- [ ] Implement Halton sequence: per-axis prime base, fixed
      sequence, deterministic. Map each tuple's per-clause
      index to `[0, 1)`; walk the Halton sequence; for each
      Halton point, find the closest unemitted lattice
      tuple (L₂ in fraction space). Emit, mark, repeat.
- [ ] Implement Sobol sequence: similar walk, default
      direction numbers. Library check first
      (`sobol_burley`, `quasirandom`) before hand-rolling.
- [ ] Both honour the SRD-18e index-space contract: walk
      uses original lattice indices; filter-rejected tuples
      are silently skipped during emission (the closest-
      unemitted search advances past them).
- [ ] `count: N` truncates after N emissions; if survivor
      set has fewer than N, emit all and stop without
      error (per SRD-18d edge cases).
- [ ] Reject on Union mode (per SRD-18e — see Push 10).
- [ ] Tests: 2-D and 5-D lattices, count truncation,
      determinism (same input → same output across runs),
      filter interaction (count satisfied past holes).

---

## Push 6 — Lhs ordering *(closes I1 partial)*

- [ ] Implement Latin Hypercube: stratify each axis into
      `count` strata; pick one sample per stratum per axis;
      pair samples deterministically using `seed` (default
      seed = 0). Snap each chosen point to closest lattice
      tuple.
- [ ] Reject on Union mode.
- [ ] `seed` option from the parser carries through to the
      stratum-permutation RNG. Halton/Sobol with `seed=N`
      warn (the sequences are fixed; `seed` is ignored).
- [ ] Tests: 2-D / 5-D, deterministic with seed, different
      seeds produce different orderings, filter survivor
      set with N greater than survivors.

---

## Push 7 — Layer 3 named generators *(closes I5)*

Each is one stdlib node + a short test. Order doesn't
matter; pick a batch and land them.

- [ ] `fib(n)` — first n Fibonacci numbers (1, 1, 2, 3, 5, ...)
- [ ] `fib_until(max)` — Fibs ≤ max
- [ ] `pow2(n)` — 1, 2, 4, …, 2^(n-1)
- [ ] `pow2_until(max)`
- [ ] `geometric(start, factor, n)`
- [ ] `geometric_until(start, factor, max)`
- [ ] `binomial(n)` — `C(n,0), …, C(n,n)`
- [ ] `subdivide(start, end, n)` — half-open subdivision
      sugar over Push 3's `range_step`
- [ ] `subdivide_inclusive(start, end, n)`
- [ ] `log_steps(start, end, n)` — log-spaced
- [ ] `linear_steps(start, end, n)` — alias for
      `subdivide_inclusive`
- [ ] Each registers as a normal GK stdlib node;
      composes with const folding.
- [ ] Tests: typical sizes, `n=0`, large `n`, float
      vs integer bases, composition with Push 3 ranges
      (`concat(1..10, fib(8))`).

---

## Push 8 — Sequencer expansions *(closes I7)*

LUT facility. Each function reuses `nbrs-activity::opseq`
algorithms unchanged.

- [ ] `bucket(items, ratios)` → `build_bucket_lut`
- [ ] `concat_seq(items, ratios)` → `build_concat_lut`
- [ ] `interval_seq(items, ratios)` → `build_interval_lut`
- [ ] `bucket("3:ann, 1:scan, 2:fetch")` colon-shorthand
      parser — splits and dispatches to the parallel-list
      form internally.
- [ ] Tests: every algorithm matches its op-sequencing
      counterpart byte-for-byte; ratio sums; mixed-type
      items.

---

## Push 9 — Layer 5 set operators *(closes I6)*

Each takes lists and returns lists. Trivial once Push 7
lands (lists are first-class clause sources).

- [ ] `concat(a, b, ...)` — concat in argument order
- [ ] `unique(a, b, ...)` — concat then dedup (first-
      occurrence preserved)
- [ ] `intersect(a, b, ...)` — values in every input
- [ ] `subtract(a, b)` — values in `a` not in `b`
- [ ] `interleave(a, b, ...)` — round-robin
- [ ] `cycle(a, n)` — `a` repeated n times
- [ ] `reverse(a)`
- [ ] `take(a, n)` — first n
- [ ] `skip(a, n)` — all but first n
- [ ] Tests per function; composition tests
      (`unique(pow2(10), 1..1000..100)`).

---

## Push 10 — Index-space ordering rejection on Union *(closes I8)*

Tiny, independent.

- [ ] Add `validate_order_for_mode(mode, order)` to
      `comprehension::ast` (or `parse`).
- [ ] Wire into `parse_comprehension_text` after both
      mode and order are parsed; surface the error with
      the SRD-18e §"Union mode + non-lex orderings" message.
- [ ] Lex and Custom remain valid for Union mode.
- [ ] Tests: each rejection path emits the right message
      pointing at the user's options.

---

## Push 11 — Layer 7b destructure *(closes I9 partial)*

Big lift: requires `Value::Tuple` (or `Vec<Vec<Value>>`
return shape). Deferred until the GK side is ready.

- [ ] Decide between (a) `Value::Tuple(Vec<Value>)` variant,
      or (b) clause-evaluator-returns-`Vec<Vec<Value>>` shape.
      Option (b) is smaller surgery; (a) is more
      composable. Decide before writing code.
- [ ] Parse `(var₁, var₂, ...) in single_expr_returning_tuples`.
- [ ] `enumerate_tuples` unpacks the returned tuples
      positionally into `vars`.
- [ ] Tests: 2-tuple destructure, 3-tuple, mismatch
      between `vars.len()` and tuple arity (load-time error).

---

## Push 12 — `order: custom(<gk-fn>)` *(closes I9 partial)*

Same `Value::Tuple` dependency as Push 11.

- [ ] Parser already accepts `custom(<fn>)`; eval path
      currently errors. Wire to GK function lookup.
- [ ] GK function signature: `fn(tuples: List<Tuple>) ->
      List<Tuple>`. Validation at scope-init (function
      exists, signature matches).
- [ ] Tests: custom ordering reorders, returns subset,
      returns empty, returns invalid type (rejected at
      eval).

---

## Deviations from SRD-18c §"Implementation order"

SRD-18c lists the original order as: 7a → 2 → 6 → 3 →
sequencer → 5 → 7b. This plan tightens it:

- **Inserted Push 5/6 (Halton/Sobol/Lhs orderings)
  earlier** — the SRD already designs them, the parser
  already accepts them, and they're the highest-impact
  user-visible win for early-stop coverage on big sweeps
  (the SRD-18d motivating use case).
- **Push 10 (Union+ordering rejection) added** — closes
  spec gap C5 with a one-match-arm fix.
- **Pushes 11 + 12 explicitly gated on `Value::Tuple`** —
  18c §7b was vague about this; SRD-18e calls it out.

---

## Cross-references

- [SRD 18b](../sysref/18b_scenario_tree_and_scheduler.md)
  — execution plumbing
- [SRD 18c](../sysref/18c_comprehension_syntax.md) — user-
  facing syntax
- [SRD 18d](../sysref/18d_comprehension_traversal_order.md)
  — order taxonomy
- [SRD 18e](../sysref/18e_comprehension_canonical_reference.md)
  — canonical AST + contracts (this plan's spec
  companion)
- `nbrs-variates/src/comprehension/{ast,parse,eval,order,
  iteration,synthesis}.rs` — implementation modules
