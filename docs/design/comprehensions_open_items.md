# Comprehensions — Open Items After 2026-05-05 Audit

Companion to:
- [Comprehensions Grammar Plan](comprehensions_grammar_plan.md)
- [SRD 18e — Comprehension Canonical Reference](../sysref/18e_comprehension_canonical_reference.md)

This memo captures the residue from the well-formed / stable /
testable audit on 2026-05-05. Everything below is either
deferred with a forcing function, gated on a documented
dependency, or stylistic residue too small to justify a
dedicated push right now. Revisit when a concrete forcing
function shows up; don't burn cycles speculatively.

---

## 1. Trait refactors deferred until a third strategy lands

### 1a. `OrderingStrategy` trait for `apply_order`

**Today.** `apply_order(tuples, sizes, order)` is a single match
on `TraversalOrder`. New strategies require:
- a new variant in the enum,
- a new arm in the match,
- a new helper function,
- and tests.

There's no extensibility for user-supplied orderings (the
planned `Custom { function }` is an enum variant, not a hook),
and tests can't inject mock orderings without spelunking
through the enum.

**Sketch.**

```rust
pub trait OrderingStrategy {
    fn apply(
        &self,
        tuples: Vec<Tuple>,
        sizes: &[usize],
    ) -> Result<Vec<Tuple>, String>;
}

// Each variant becomes a small impl. Custom dispatches to
// a registered `fn(...) -> Vec<Tuple>` from a name registry.
```

**Why deferred.** Three users force a pattern; we have eight
variants today but they're all internal — the enum is the
right shape until either:
- `order: custom(<gk-fn>)` actually ships (Push 12, gated on
  `Value::Tuple` — the user-visible plug-in point that
  benefits from a trait), OR
- a fourth distinct dispatch site appears (e.g. dryrun-time
  ordering preview) that wants to swap strategies cheaply.

Both are concrete future events. Do this then, not before.

### 1b. `SpecExpander` trait for `evaluate_spec`

**Today.** `evaluate_spec` runs through five `try_eval_*`
dispatchers in a fixed order:

```rust
if let Some(v) = try_eval_all_cursor(...)? { return Ok(v); }
if let Some(v) = try_eval_range(...)? { return Ok(v); }
if let Some(v) = try_eval_generator(...)? { return Ok(v); }
if let Some(v) = try_eval_setop(...)? { return Ok(v); }
if let Some(v) = try_eval_sequencer(...)? { return Ok(v); }
// fallback: const-eval or list-parse
```

Two parallel paths exist (`evaluate_spec` for runtime,
`pre_evaluate_clause` for synthesis-time). A new layer must
land in **both** chains. Order is implicit; conflicts are
silent.

**Sketch.**

```rust
pub trait SpecExpander {
    /// Try to expand the (already-interpolated) spec. Return
    /// `Ok(None)` to defer to the next expander in the chain.
    fn try_expand(
        &self,
        text: &str,
        ctx: &dyn SpecContext,
    ) -> Result<Option<Vec<Value>>, String>;
}

pub fn evaluate_spec(text: &str, ctx: &dyn SpecContext) -> Result<Vec<Value>, String> {
    for expander in registered_expanders() {
        if let Some(v) = expander.try_expand(text, ctx)? {
            return Ok(v);
        }
    }
    fallback_const_eval(text)
}
```

The `SpecContext` abstraction is the load-bearing piece — it
needs to expose enough of the kernel for runtime use AND
enough probes / params for synthesis-time use. That's the
real design work.

**Why deferred.** Five expanders works as a hand-built chain.
The forcing function is:
- a sixth expander (current count is at the "rule of three"
  threshold but the cost of adding one more inline is still
  small), OR
- a runtime/synthesis path divergence bug — if a layer lands
  in `evaluate_spec` and someone forgets `pre_evaluate_clause`,
  we'll feel it. That bug is the cheapest forcing function;
  wait for it.

---

## 2. Features gated on declared dependencies

These are documented in SRDs as deferred. Listed here so
this memo is exhaustive.

### 2a. Push 5b — Sobol ordering
**Blocker.** Joe-Kuo direction-number tables (public domain
but not yet bundled).
**Disposition.** `TraversalOrder::Sobol { .. }` parses; the
runtime errors with a message pointing at `halton/N` or
`lhs/N seed=K` as in-tree alternatives.
**Effort estimate.** Tabulate first ~25 dims of Joe-Kuo,
implement the bit-reversal recurrence, port the existing
`order_halton` test pattern. Bounded scope.

### 2b. Push 11 — Layer 7b destructure clauses
**Blocker.** GK `Value::Tuple` (or equivalent
`Vec<Vec<Value>>` shape).
**Disposition.** Parser today accepts only the parallel-iter
LHS form `(a, b) in (e1, e2)`; destructure form
`(host, port) in pairs_csv()` parse-rejects with a clear
"requires Value::Tuple" message.
**Effort estimate.** Bigger than 2a — it crosses the GK type
boundary. Probably worth a dedicated SRD push when scheduled.

### 2c. Push 12 — `order: custom(<gk-fn>)`
**Blocker.** Same `Value::Tuple` dependency as 2b — the
user's GK function takes `List<Tuple>` and returns
`List<Tuple>`.
**Disposition.** Parser accepts; evaluator errors with the
same Value::Tuple-pointing message.
**Note.** Landing this is the natural forcing function for
1a (the `OrderingStrategy` trait): once user functions are
real, the registry / dispatch shape benefits from a trait.

---

## 3. Stylistic residue (discretionary)

Tiny items that are clean to land in passing during related
work, but don't justify a dedicated change.

### 3a. `comprehension_from_subspaces(Vec<Vec<Clause>>)` shape
Still takes the bare-Vec shape rather than `Vec<Subspace>`.
Internal workload-parse plumbing; no external callers.
Update next time `parse.rs` is open for other reasons.

### 3b. Deprecated `validate_order_for_mode` shim
Kept in `parse.rs` with `#[deprecated]` for one-cycle
compatibility. No external callers found in this repo. Safe
to remove on the next `parse.rs` change.

### 3c. Dual Union constructors
`Comprehension::union(Vec<Vec<Clause>>)` and
`Comprehension::union_from(Vec<Subspace>)` co-exist.
Intentional convenience pair — `union(...)` for callers with
bare clause lists, `union_from(...)` for callers carrying
metadata. Not duplication; document if the surface looks
confusing in code review later.

---

## 4. Genuinely-residual gaps not attempted

These are real, but the cost/benefit isn't right today.

### 4a. Filter expression parse-time validation

**Today.** `comprehension.filter: Option<String>` is checked
only at iteration time — a typo or undefined-name reference
errors per-tuple, not at workload load.

**Why we haven't done it.** The filter is a GK predicate
expression. Pre-validating means exposing GK's expression
parser to the comprehension layer, which inverts the
dependency direction (comprehension currently depends on
GK eval; this would add a parse-time hop the other way).
The right home is probably a `gk::expr::parse_predicate`
API that the comprehension validator can call without
exposing the rest of GK. That's its own design exercise.

**Workaround today.** `dryrun` mode catches filter errors
on the first iteration of each comprehension scope, which
is good enough for the current "did my workload load"
checkpoint. Real validation happens whenever the predicate
first runs.

### 4b. Property-based parser testing

**Today.** Round-trip tests
(`Comprehension::Display ↔ parse_comprehension_text`) cover
hand-picked inputs across all the major shapes. The parser
has bespoke string walkers (`split_respecting_parens`,
`is_clause_boundary`, `split_paren_group`,
`strip_zip_mode_prefix`) — these are the kinds of routines
that proptest catches edge cases in.

**Why we haven't done it.** No parser bug has motivated the
machinery yet. `proptest` adds a build-time dependency, and
the round-trip tests we have catch the obvious shape errors.

**Trigger to revisit.** Any parser bug from real workloads.
The harness pays for itself when the first user-reported
edge case lands.

---

## 5. What success looks like

When does this memo close out?

- 1a/1b retire when their forcing functions arrive (Push 12
  for 1a; a sixth expander or a path-divergence bug for 1b).
- 2a/2b/2c are scheduled work — they'll get their own pushes.
- 3a/3b/3c get cleaned up as drive-bys.
- 4a closes if/when a `gk::expr::parse_predicate` lands; if
  it doesn't, this stays as documented residue.
- 4b closes the first time a parser edge case shows up in a
  user workload.

If 6+ months pass with none of these triggering, the right
answer is probably "delete the deferred items from this memo
because they aren't load-bearing." Don't carry indefinite
backlogs; revisit and prune.
