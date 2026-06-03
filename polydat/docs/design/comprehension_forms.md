# Comprehension Forms — Polydat Design

**Status:** DRAFT — design for the comprehension algebra. Pins
down the constructors, the closure properties under composition,
the validity axioms, and the compilation model.

## Authoritative ownership declaration

This document is the **single authoritative reference** for the
comprehension algebra — its constructors, validity axioms,
metadata propagation rules, optimizer rewrites, IR opcodes, and
consumption surfaces. Any non-polydat SRD discussing
comprehensions does so **strictly to describe how that SRD's
subject integrates with polydat-owned comprehensions**; non-
polydat SRDs do not redefine, extend, or shadow polydat-owned
material. Apparent contradictions between a non-polydat SRD and
this document resolve in favor of this document. §15 below
names each touching SRD's role under this declaration.

## Companion documents

- [SRD-18c](../../../docs/sysref/18c_comprehension_syntax.md) —
  parser-layer surface grammar (clause sources, ranges,
  generators, SI suffixes). Owns parsing; does not own
  semantics.
- [SRD-18d](../../../docs/sysref/18d_comprehension_traversal_order.md)
  — per-strategy algorithmic detail (Halton recurrence, Sobol
  direction numbers, Lhs construction). Owns mathematical
  implementation detail; §3.6 below owns compositional behavior
  and per-strategy input requirements.
- [SRD-78](../../../docs/sysref/78_polystreamer.md) —
  PolyStreamer runtime that hosts the §9.5 consumption surfaces
  (`CoordinateStream`, `ScopedKernelStream<K>`, `scope_once`).
  Owns runtime implementation; this document owns the contracts
  it implements.

The forcing question: **given filtering, ordering, unioning,
parallel-zipping, cartesian-multiplying, and bounded/unbounded
source distinctions, can every meaningful combination be written
in one regular grammar, validated by a small axiom set, and
compiled to a small operator language that runs in bounded memory
beyond unavoidable combinatoric tracking?** This doc says yes,
and shows how.

---

## 1. What a comprehension is

A **comprehension** is a value-producing expression whose value is
an ordered stream of **named tuples**. A tuple is one
`Vec<(String, Value)>` — a finite set of `(name, value)` pairs.
Across a single comprehension's dispense, every tuple carries the
**same set of names** in the same order; only the values vary.

Comprehensions are *first-class*: the same constructors apply
whether the comprehension appears in a user-authored expression,
is bound to a wire and reused, is held as a runtime
[`PolyStreamer`](../../../docs/sysref/78_polystreamer.md), or is
the subject of a static optimization pass.

### 1.1 Universe of values

Every operation in this algebra has the **same input and output
type**: comprehension. There is no `clause` type that's separate
from `comprehension`; a clause is a comprehension. There is no
"filtered comprehension" type distinct from "comprehension"; a
filter applied to a comprehension yields another comprehension.
This is the closure property that makes the algebra compose.

The value-level type is `comprehension`. The compile-time
metadata each comprehension carries (its tuple shape, its
boundedness, its materialization-need) is **derived** from the
constructor and its operands, not declared independently.

---

## 2. The four feature stems

Polydat comprehensions cover four logically independent feature
stems. Each stem is one **constructor** in the algebra; combining
them is composition, not special-casing.

| Stem | Constructor | What it does |
|---|---|---|
| **Sources** | `clause` | A single named binding `var := source-expr`, where `source-expr` evaluates to a tuple stream of single-name tuples. |
| **Combination** | `cartesian`, `zip`, `union` | Combine N comprehensions into one with declared combination semantics. |
| **Selection** | `filter` | Drop tuples that fail a predicate. |
| **Permutation** | `order` | Reorder (and optionally truncate) the dispense stream. |

Every other comprehension feature — sequences, ranges, bounded /
unbounded sources, parallel-zip, destructure, where-chains, order-
chains — is expressible as a use of one or more of these four
constructors. The constructors are the **minimal closed set**
under composition.

### 2.1 Why exactly four

- **Sources** are leaves of the operator tree. Without them, the
  tree has nothing to compose over.
- **Combination** is the multi-operand combinator family. The
  three combination shapes (cartesian product, zip, concatenation)
  are mutually irreducible — each describes a distinct way to
  fuse N tuple streams into one. None is expressible as a
  composition of the others. (Cartesian is N-dim; zip is 1-dim
  diagonal of an N-dim cartesian, so technically reducible to
  `filter(cartesian(...), is-diagonal)` — but that costs O(N^k)
  evaluation for a 1/N^(k-1) survival rate. Keeping zip as its
  own constructor is a performance contract, not just a
  convenience.)
- **Selection** filters; it has no expressive substitute. You can
  emulate `filter` by `cartesian` against a predicate-derived
  source, but that's a re-encoding, not a derivation.
- **Permutation** reorders. It's the only operator that touches
  the *sequence* of dispense without touching the set of dispensed
  tuples (when un-truncated) or their values (ever).

---

## 3. Constructors in detail

The algebra has **six constructor forms**: one source, three
combinators, two modifiers. Every comprehension AST is a tree
whose nodes are one of these six.

### 3.1 `clause(name, source)` — leaf source

```text
clause(k, 1..10)
```

Produces a **stream** of single-name tuples
`(k=1), (k=2), ..., (k=9)`.

- `name`: a string identifier; becomes the tuple key.
- `source`: an expression compiled to either a **discrete
  stream producer** (yielding one `Value` per `advance()`) or
  a **continuous measure** (a bounded or unbounded real
  interval with a defined measure, sampled rather than
  enumerated). Discrete sources cover literal comma lists, GK
  integer/string ranges, generator functions, stdlib helpers,
  workload-param references, set operators, etc. (SRD-18c
  Layers 1-6.) Continuous sources cover bounded real intervals
  (`0.0..1.0`, `-π..π`) and unbounded ones (`0.0..` for the
  non-negative reals), plus continuous-distribution objects
  (uniform, normal, exponential — sampled as their respective
  measures). Discrete sources that are inherently finite expose
  their cardinality up front; computed-on-demand discrete
  sources expose `Unbounded` or an `at-most` hint; continuous
  sources expose their measure class (see §6.1).

**Streaming is the default.** A clause does NOT pre-materialize
its source. Discrete sources are stream producers; a clause's
per-tuple cost is one `advance()` call plus the cost of
materializing one `Value`. Continuous sources are measures, not
streams — a clause over a continuous source cannot enumerate
on its own; it must be wrapped in a sampling order (V8 in §5)
that turns the continuous measure into a finite ordered
traversal. This is the load-bearing model choice for the whole
algebra — see §6 for the propagation rules that make every
composition stream by default and §10 for the optimizer
rewrites that preserve streaming through combinators that
would naïvely materialize.

**Tuple shape:** `[(name, Value)]` — one entry, one name.
**Cardinality:** equals the source's declared cardinality; may
be `Bounded(n)`, `BoundedAtMost(n)`, `Unbounded`, or
`Continuous { interval, measure }` (see §6.1).
**Per-tuple memory:** O(1) above the source's own per-tuple
state. No buffering at the clause level.

### 3.2 `cartesian(c1, c2, ..., cN)` — dependent product combinator

```text
cartesian(
    clause(k, 1..10),
    clause(profile, {profiles}),
)
```

Sequential composition of N children, evaluated **left-to-right**
with each child's source evaluated in a context that carries
prior children's bindings. The classical independent cross-
product is the **degenerate case** when no child's source
references a prior child's variable.

- Children must have **disjoint name sets**. (If `clause(k, ...)`
  and `clause(k, ...)` both appear under one `cartesian`, the
  parser rejects with a duplicate-name error — there's no
  meaningful interpretation.)
- **Dependent sources are allowed**: clause N's source
  expression may reference clause M's variable for M < N
  (e.g. `cartesian(clause(outer, [a,b,c]), clause(inner, pre_{outer}))`).
  Evaluation is left-to-right dependent enumeration —
  mathematically a dependent product (Σ in type theory), not
  the free cross-product (Π). The classical Π semantics emerge
  automatically when children's sources contain no
  cross-references.

**Tuple shape:** disjoint union of children's tuple shapes, in
declaration order.

**Cardinality:** depends on whether any child references prior
children's variables:

- **Independent** (no cross-references): cardinality is the
  classical **product** of children's cardinalities
  (`bounded × bounded = bounded`; `bounded × unbounded = unbounded`;
  etc.). This is the special case the static IR interpreter can
  optimize via cached per-child enumeration.
- **Dependent** (any cross-reference): cardinality is a
  **dependent sum** — for each outer-clause value, the inner-
  clause cardinality may differ. The total cardinality is
  `Σ_{outer ∈ C_outer} |C_inner(outer)|`. Reduces to the
  product when `|C_inner(outer)|` is constant.

**Independence detection** is a compile-time pass: examine each
child's source for `{name}` references to any prior child's
name. The pass produces an `is_dependent` flag the interpreter
and the optimizer consult.

**Why dependent semantics by default.** The classical
independent cross-product is a useful but special case; the
algebra's general operator is the dependent product. Treating
dependency as the general case (with independence as an
optimization) lets workload authors write the flat clause
shape regardless of whether their clauses happen to reference
each other — the algebra figures out the right enumeration.

### 3.3 `zip(c1, c2, ..., cN, mode)` — lockstep combinator

```text
zip([
    clause(x, 1..10),
    clause(y, 11..20),
], mode = Strict)
```

Produces the diagonal of the cartesian — tuple K binds each
child's K-th value. Strict mode errors on length mismatch;
`Truncate` truncates to the shortest; `Cycle` cycles shorter to
longest.

- Children must have **disjoint name sets** (same as cartesian).
- Children must be **bounded** under `Strict` and `Truncate`
  (cardinality must be known to compute the diagonal endpoint).
  `Cycle` permits one unbounded child with the others bounded;
  cardinality is the unbounded child's cardinality.

**Tuple shape:** disjoint union of children's tuple shapes.
**Cardinality:** `min`, `min`, or `max` of children's
cardinalities respectively per mode.

### 3.4 `union(c1, c2, ..., cN)` — concatenation combinator

```text
union(
    cartesian(clause(k, 10), clause(limit, 10..20)),
    cartesian(clause(k, 100), clause(limit, 100..200)),
)
```

Produces the concatenation of children's dispense streams in
declaration order.

- Children must have the **same tuple shape** (same set of names,
  in the same order). A consumer pulling from a union always
  receives a tuple with one consistent shape; this is the
  load-bearing contract that lets downstream consumers
  destructure by name without knowing which sub-space the tuple
  came from.
- Children's tuple-name set order is checked structurally; if
  child A produces `(k, limit)` and child B produces
  `(limit, k)`, the parser rejects (despite the same name set).

**Tuple shape:** equal to (and inherited from) any child's tuple
shape.
**Cardinality:** sum of children's cardinalities. Unbounded if
any child is unbounded; if a child appearing before another is
unbounded, the later children are unreachable (compile warning).

### 3.5 `filter(c, predicate)` — selection modifier

```text
filter(
    cartesian(clause(k, 1..10), clause(limit, 1..10)),
    "{k} * {limit} <= 50",
)
```

Produces the substream of tuples for which `predicate` evaluates
to `true`. Tuples that evaluate to `false` are dropped; tuples
where the predicate fails to evaluate (missing name, type error,
etc.) are runtime errors per SRD-18e §"`where` predicate
semantics".

- Predicate is a Polydat boolean expression; names referenced must be
  in the comprehension's tuple shape or inherited from the
  enclosing parent scope.
- Predicate evaluates **per-tuple**, not per-batch — `filter` is
  inherently streaming.

**Tuple shape:** equal to the input comprehension's tuple shape.
**Cardinality:** `BoundedAtMost(N)` if input was `Bounded(N)`;
`Unbounded` if input was `Unbounded`. Actual count is a runtime
fact dependent on predicate evaluation against actual values.

### 3.6 `order(c, strategy, truncation?)` — permutation modifier

```text
order(
    cartesian(clause(k, 1..10), clause(limit, 1..10)),
    Halton, truncation = Some(20),
)
```

Produces a permutation of the input stream. Optionally truncates
to the first N tuples in the new order. Each named strategy is a
stable, decidable permutation rule whose closed-form description
is known to the optimizer (§10). User-supplied `Custom(fn)`
orderings are deliberately excluded — they cannot be analyzed for
push-down and would force the input to materialize in full at
every use site. If a workload need cannot be expressed by the
named strategies, the spec is the right place to add a named
strategy, not a user-callback escape hatch.

The strategy taxonomy is SRD-18d's set plus `Shuffle`. Each
strategy declares its **input requirement** in terms of the
input's `IndexFn` (per §10.7's metadata algebra) and its
behavior over discrete vs. Continuous inputs. The validator
(V4) accepts only inputs whose `IndexFn` satisfies the
strategy's requirement; V8 additionally requires Continuous
inputs to be wrapped in a strategy that supports them.

**Strategy invocation surface.** Strategies have a single
entry point, `apply(evaluated: &[EvaluatedSource],
truncation: Option<u64>)`, where each [`EvaluatedSource`]
(##10.7.6) carries its values, cardinality, and `IndexFn`.
There is no split between "metadata-bearing" and
"metadata-naive" apply paths — the strategy reads the
`IndexFn` from the evaluated inputs and routes accordingly.
This is what makes V4 enforceable at strategy-invocation
time independent of how the source was authored (literal,
range, registry-recognized generator, or workload-param):
the shape is *always* known by the time `apply` runs.

| Strategy | Input requirement | Discrete behavior | Continuous behavior |
|---|---|---|---|
| `Lex` | any (incl. `None`) | Natural enumeration order — pass-through (§10.2 R1) | Rejected — Continuous has no natural enumeration; use a sampling strategy |
| `ReverseLex` | any non-`None` discrete | Reverse the input's index range | Rejected — same reason |
| `Shuffle` | any non-`None` | Random permutation of the input's index range (PRNG seed captured at materialization) | n PRNG draws from the measure, returned in shuffle order |
| `Halton` | any non-`None` | K-D Halton over Lattice; 1-D Halton sequence over Lockstep / Modular / Concatenation | **Native** — K-D Halton over `[0,1)^K` mapped to the input's interval(s); the canonical use case |
| `Sobol` | any non-`None` | K-D Sobol over Lattice; 1-D Sobol over single-axis index spaces | **Native** — same shape as Halton, Sobol generator |
| `Lhs` | any non-`None` | K-D stratified per-axis permutation over Lattice; uniform random over single-axis index spaces (degenerate) | **Native** — K-D Latin Hypercube over `[0,1)^K` mapped to the input's interval(s) |
| `Extrema` | any non-`None` | K-D lattice corners over Lattice with N≥2 axes; {first, last} over 1-D (degenerate) | K-D box corners over a Continuous Lattice (interval endpoints on each axis); 2K extrema for K-D |
| `Shells` | any non-`None` discrete | Concentric shells around lattice center; concentric pairs over 1-D (degenerate) | Rejected — "shells" in continuous space is ill-defined without a discretization parameter |
| `Diagonal` / `Antidiagonal` | any non-`None` discrete | Diagonal walk over Lattice with N≥2 axes; trivial over 1-D (degenerate) | Rejected — continuous diagonal would emit uncountably many points; no canonical "step" |

Strategies marked "Native" for Continuous (Halton, Sobol, Lhs)
are the canonical sampling strategies for continuous parameter
sweeps — these are precisely the low-discrepancy quasi-random
sequences whose mathematical definition lives in continuous
space, and discretization is the special case. Workloads that
sample real-valued ranges should default to one of these three.

**Degenerate compositions are not errors.** A strategy applied
to an input that mathematically satisfies its requirement but
collapses to a trivial form (e.g. `Extrema` over a 1-D index
space yielding `{first, last}`) is well-formed and compiles. The
spec's posture is **don't police mathematically valid recipes**
— automated workload generation, exploratory testing, and
parameter-sweep tooling often produce technically-degenerate
forms that are nonetheless correct. See §5.8 for the validation-
mode mechanism that flags these as warnings (default) or errors
(strict mode).

- Strategies split into **streaming** (`Lex` only) and
  **materializing** (every other strategy). See §6.
- Per-strategy validity constraints — see V4 in §5.

**Tuple shape:** equal to the input comprehension's tuple shape.
**Cardinality:** equal to input cardinality if `truncation` is
`None`; `min(input, truncation)` otherwise.

---

## 4. Closure and identity axioms

### 4.1 Closure

**Axiom C1 (uniform return type).** Every constructor returns a
`comprehension`. Every constructor accepts only `comprehension`s
(plus the constructor-specific scalar parameters: predicate
string, strategy, truncation, zip mode, source expr). There is
no auxiliary value type the user can mistakenly substitute.

**Axiom C2 (compositional well-formedness).** A comprehension is
well-formed iff every node in its AST passes its constructor-
specific shape check (§3) against its children's
**already-well-formed** shapes. Well-formedness is decidable in
one bottom-up pass over the AST.

### 4.2 Identity

**Axiom I1 (singleton union is identity).** `union(c)` ≡ `c`.

**Axiom I2 (singleton cartesian is identity).** `cartesian(c)`
≡ `c`. (A one-child cartesian is just the child.)

**Axiom I3 (singleton zip is identity).** `zip([c], _mode)` ≡
`c`. The mode is irrelevant when there's one input — nothing to
synchronize against.

**Axiom I4 (trivially-true filter is identity).** `filter(c,
"true")` ≡ `c`. The parser MAY fold this away; the runtime MUST
behave identically.

**Axiom I5 (lex order with no truncation is identity).**
`order(c, Lex, None)` ≡ `c`. (`Lex` IS the default emission
order; un-truncated reorder produces the original stream.)

These five identities make the AST canonicalizable: any well-
formed AST has a unique normal form under repeated identity
elimination.

---

## 5. Validity axioms

Not every syntactically-composable AST is semantically valid.
The validity rules are **type-level constraints** the well-
formedness check (Axiom C2) enforces. There are nine of them
(V1 – V9).

**Axiom V1 (cartesian / zip disjoint names).** All children of
a `cartesian` or `zip` must have disjoint tuple name sets.
*Reason:* a merged tuple with two values for the same key is
ambiguous; the algebra refuses to define which wins.

**Axiom V2 (union shape uniformity).** All children of a
`union` must have **identical** tuple shapes — same names, same
declaration order. *Reason:* downstream consumers destructure
union output by name; allowing variable shapes would break the
consumer's ability to write one expression that handles all
sub-spaces.

**Axiom V3 (filter name closure).** Every name referenced by a
filter predicate must be present in the filtered comprehension's
tuple shape OR in the enclosing parent scope's coordinate set.
*Reason:* a predicate that names something neither bound nor
inherited can never evaluate. The check happens at parse time
(static names from the AST) plus at link time (parent-scope
names from the binding context).

**Axiom V4 (strategy input-shape contract).** Each named
ordering strategy declares its accepted input `IndexFn` shape in
§3.6. The validator computes the immediately-ordered
comprehension's `IndexFn` per the metadata algebra (§10.7) and
rejects when the input's variant does not satisfy the strategy's
declared requirement. *Reason:* every named strategy is a
permutation of an index space; its mathematical definition
requires *some* closed-form addressing of the input. Inputs
whose `IndexFn` is `None` (e.g. a raw `filter` output —
filter destroys index addressability) have no index space for
the strategy to permute, so non-`Lex` strategies over them are
mathematically undefined.

The §3.6 table is the per-strategy reference. The summary:

- `Lex` accepts any input, including `None` (it's a pass-through
  over the input's natural enumeration).
- Every other named strategy requires the input's `IndexFn` to
  be non-`None` — i.e. the input must be a `cartesian`, a `zip`
  (any mode), a `union` of index-addressable children, or a
  `clause` (which is a 1-axis `Lattice`).
- V5 carries forward: a `filter` wrapping an index-addressable
  input does NOT destroy the underlying addressability for V4's
  purposes. V4's check looks through one filter layer.

V4 is **not** a usefulness filter — degenerate-but-defined
compositions (e.g. `Extrema` over a 1-D `Lattice` yielding
`{first, last}`) pass V4 and are flagged by §5.8's warning
mechanism, not rejected.

**V4 enforcement timing (per §10.7.8).** V4 fires at
strategy-invocation time against the
[`EvaluatedSource`](##10.7.6) the strategy receives. For
comprehensions whose sources are all statically evaluable
(per §10.7.0's eval-class partitioning — `Literal` /
`IntRange` / `ContinuousInterval` / registry-recognized
generators per §10.7.7), the compile-time IR planner runs
evaluation and fires V4 early as a usability nicety —
malformed shapes error at parse time. For context-required
sources, the early fire is skipped; runtime fire at strategy
invocation is the load-bearing check. Either way, the axiom
is the same.

**V4 + dependent Cartesian.** When a `cartesian`'s children
have cross-references (dependent product per §3.2), the
result is *not* a regular n-D lattice — the "shape" of the
inner clause varies per outer-clause value. Named strategies
other than `Lex` are mathematically undefined over a
dependent product (extrema of what corners? Halton samples in
what unit cube?). V4 rejects non-`Lex` strategies over a
dependent Cartesian. `Lex` still works because it's a
pass-through over whatever natural enumeration the dependent
walk produces. *Reason:* the strategies' geometric
interpretations require an independent product space; the
dependent case has no such space to permute over. Workloads
that need strategy-driven sampling over a parameterized space
must structure the parameterization as an outer scope (nested
for_each), with the inner `order(cartesian(...), strategy, n)`
operating over an independent inner product.

**Axiom V5 (filter is transparent to V4's input-shape check).**
`order(filter(c, p), strategy, t)` is valid whenever
`order(c, strategy, t)` is valid. The intermediate `filter`
node has `index_addressable: None` per §10.7's propagation
rules, but V4 looks through one filter layer when computing
the strategy's input `IndexFn`. The ordering operates on the
filter-surviving tuples while reasoning about their *original*
index-space positions (per SRD-18e §"Index-space contract").
*Reason:* extrema-of-survivors and shells-of-survivors are
meaningful when the user wrote "extrema of (k, limit) where
k*limit ≤ 50"; the surviving corners are still corners of the
original (k, limit) lattice. Without this transparency rule,
every `filter` would force the user to reorder before
filtering, losing N1's filter-then-order vs. order-then-filter
distinction.

V5's transparency is **one filter layer**, not arbitrarily deep.
`order(filter(filter(c, p), q), strategy, t)` requires F1
folding (§7.2) to collapse to one filter first, then V4's
look-through applies.

V5 applies **uniformly across cardinality classes**: a filter
wrapping a `Continuous` or `Hybrid` input also has its underlying
`IndexFn::Continuous` / `IndexFn::Hybrid` surfaced for V4's
check, even though the filter itself produces `ContinuousAtMost`
/ `Hybrid` (still cardinality-reduced) with `index_addressable
= None`. So `order(filter(c_continuous, p), Halton, Some(n))`
fires R2 push-down (Halton draws over the underlying continuous
box) and applies the predicate per drawn sample. The
"extrema-of-survivors" semantic carries to the continuous case
as "samples-from-the-original-box that happen to satisfy the
predicate."

**Axiom V6 (discrete-bounded-required operations on unbounded
discrete inputs).** `order` with a materializing strategy
(every strategy except `Lex`) and `zip` under `Strict` or
`Truncate` mode require their discrete input to have known
finite cardinality (`Bounded(n)` or `BoundedAtMost(n)`).
Applying these to an `Unbounded` discrete comprehension is
invalid; the parser rejects with a clear "cannot
reorder/zip an unbounded stream" message. *Reason:*
materialization assumes the stream fits in memory; we
refuse to enable a runtime OOM.

V6 is the **discrete unboundedness** rejection. The companion
rejection for `Continuous` / `ContinuousAtMost` inputs is V8 —
continuous-without-sampling is invalid because it has no
canonical enumeration order at all (not just because it
wouldn't fit in memory). The two axioms partition the
"non-finite-discrete" space cleanly: V6 catches unbounded
discrete streams reaching a barrier; V8 catches continuous
measures reaching the outermost dispense without an enclosing
sampling order.

**Axiom V7 (zip cardinality contract).** A `zip` under `Strict`
mode requires all children's cardinalities equal. Mismatch is
a load-time validation error (catches some via static analysis)
or a runtime error at the first cardinality measurement (covers
the cases where cardinality is data-dependent). Additionally,
**all children of a `zip` (under any mode) must be discrete**
(`Bounded`, `BoundedAtMost`, or `Unbounded`). Continuous or
mixed-class children are rejected at parse time. *Reason:*
zip's lockstep semantics require an integer "i-th element"
notion; continuous sources have no integer index. Authors who
want two continuous coordinates paired together should either
(a) sample each first and zip the discretized outputs —
`zip(order(c1, halton/n, Some(n)), order(c2, halton/n,
Some(n)))` — or (b) compose into a cartesian and sample the
joint space — `order(cartesian(c1, c2), halton, Some(n))`.
Form (b) is usually what's intended.

**Axiom V8 (continuous requires explicit sampling + integrable
measure).** A comprehension whose cardinality is `Continuous` or
`ContinuousAtMost` cannot be a `PolyStreamer`'s dispense source
directly. It MUST be wrapped in an `order(_, strategy, Some(n))`
where the strategy accepts a Continuous input (per §3.6's
per-strategy input table) and `n` is finite.

Additionally, every `Continuous` source must declare an
**integrable measure** — a measure with finite total mass that
can be normalized to a probability distribution. The check
happens at parse time on the source:

| Source shape | Integrable? | V8 verdict |
|---|---|---|
| Bounded real interval + `Uniform` (e.g. `0.0..1.0`) | Yes (Lebesgue measure scaled by interval width) | Accepted |
| Unbounded real interval + named distribution (Normal, Exponential, Pareto, Beta on `[0,1]`, ...) | Yes (these distributions have proper probability measures even with unbounded support) | Accepted |
| Unbounded real interval + `Uniform` (e.g. `0.0..` with no distribution) | No (uniform on unbounded support has no normalizable density) | Rejected at parse |
| Bounded interval + `Named(D)` where `D`'s support is incompatible with the interval | Case-dependent | Validator consults the distribution's declared support |

*Reasons:* a continuous measure has no canonical enumeration
order — the mathematical object is a measure, not a sequence.
The order + truncation pair is what selects a finite,
deterministic, named traversal from the measure. Sampling
strategies (Halton, Sobol, Lhs) work by inverse-CDF mapping or
density-weighted point selection; both require a normalizable
density. Unbounded-uniform has no density, so no sampling
strategy can produce well-defined draws — rejecting at parse
prevents a runtime sampling failure with confusing root cause.

V8 is checked at the comprehension's outermost level. Continuous
subexpressions deep inside an AST are fine as long as some
enclosing `order(_, _, Some(n))` produces a finite traversal
before the AST reaches a `PolyStreamer`. The integrability check
fires at parse on every continuous source independently of where
it sits in the AST. Example:
`order(cartesian(clause(k, 1..10), clause(theta, 0.0..2π)),
Halton, Some(100))` is valid — the outer order samples the
hybrid discrete × Continuous space into 100 tuples, and the
continuous source `0.0..2π` is bounded so its uniform measure
is integrable.

**Axiom V9 (union class uniformity).** All children of a
`union` must be discrete (`Bounded`, `BoundedAtMost`, or
`Unbounded`). Continuous or mixed-class children are rejected
at parse time. *Reason:* union concatenates dispense sequences,
and continuous sources have no sequence to concatenate — the
mathematical object is a measure, not a stream. Authors who want
"samples from interval A or interval B" should sample each
continuous sub-space first and union the discrete outputs:
`union(order(c_a, halton/n_a, Some(n_a)), order(c_b, halton/n_b,
Some(n_b)))`. V9 is the union analogue of V7's zip restriction;
both keep the discrete-combinators free of the measure-theoretic
complexity that arises only when continuous sources are first-
class operands.

### 5.8 Validation modes

V1 – V9 are about **mathematical validity** — failing any of
them produces an AST that has no defined meaning, and the
validator rejects unconditionally. There is a separate class of
*degenerate but defined* compositions that the validator
flags rather than rejects. These are mathematically well-formed
(the operator's definition applies, the dispense sequence is
deterministic, the resource bounds compute) but produce trivial
or surprising output that suggests the author may have meant
something else.

Two validation modes:

- **Permissive (default).** V1 – V9 enforced as errors;
  degenerate compositions emit a `ValidationWarning` carrying
  the location, the degeneracy reason, and a suggested
  alternative if one exists. The comprehension compiles and
  runs. Warnings surface through the validator's structured
  output; consumers (workload loader, REPL, tooling) decide
  whether to print or filter them.
- **Strict (`polydat::validate::Mode::Strict`).** Promotes
  every `ValidationWarning` to a hard error. Used by
  workload-loading paths that want a clean bill of health.

The degenerate-composition catalog (initial):

| Pattern | Reason | Suggested alternative |
|---|---|---|
| `Extrema` / `Shells` / `Diagonal` / `Antidiagonal` over a 1-axis input | Strategy collapses to `{first, last}` or trivial walk | Use `Lex` truncation if the goal is "first and last"; restate over a multi-axis cartesian |
| `Lhs` over a 1-axis input | Equivalent to `Shuffle`; two names for one behavior | Use `Shuffle` explicitly |
| Single-element clauses chained in cartesian | `cartesian(clause(a, [K]))` is just `clause(a, K)` | Replace with the scalar source; the cartesian wrapper adds nothing |
| `filter(c, "false")` | Empty dispense sequence | If intentional, use an empty literal source; if not, the predicate is bug-shaped |
| `zip([c], _)` / `cartesian(c)` / `union(c)` | Singleton variant is identity (I1 – I3) | The optimizer's R0 elides these; the warning surfaces the redundancy at the source AST |

The catalog grows by coordinated addition (new entry + a
suggestion + a test that exercises both permissive and strict
modes). The point is not exhaustive coverage of every possibly-
unwise form; it's catching the cases that experience shows
authors stumble into.

**Why this isn't a V-axiom.** V-axioms are about defining what
"comprehension" means. Validation modes are about *author
ergonomics* — calling attention to defined-but-likely-unintended
shapes without blocking them. Test automation, exploratory
sweeps, and code generation legitimately produce these shapes;
hard rejection would force defensive wrapping at the call site.

---

## 6. Boundedness and materialization

Every comprehension AST has two compile-time-derivable
properties: its **cardinality class** and its **memory
footprint class**. The algebra preserves these properties
predictably under composition, which is what lets the
"reasonable resource use" axiom hold.

### 6.1 Cardinality class

Polydat distinguishes six cardinality classes:

- `Bounded(n)` — discrete, exactly n tuples.
- `BoundedAtMost(n)` — discrete, between 0 and n tuples.
- `Unbounded` — discrete, no known upper bound (e.g. a
  generator or live stream).
- `Continuous { intervals, measure }` — a fully-continuous
  source whose value space is one or more real intervals with
  a defined **integrable** product measure (uniform on a
  bounded interval, named distribution with proper density,
  etc.). Cannot enumerate; must be sampled via an enclosing
  `order(_, strategy, Some(n))` per V8. The integrability
  constraint is V8's responsibility: unbounded interval +
  Uniform is rejected at parse (no normalizable density);
  unbounded interval + a named distribution like Normal or
  Exponential is accepted (these are proper probability
  measures). Note `intervals` is a vector: a clause over a
  scalar range produces `Continuous { intervals: [single],
  measure: ... }`; a cartesian of continuous clauses produces
  `Continuous { intervals: [...K-D...], measure:
  Product([...]) }`.
- `ContinuousAtMost { intervals, measure_at_most }` — a
  filtered continuous source whose measure has been reduced
  by the predicate. Still cannot enumerate without sampling.
- `Hybrid { discrete_axes, continuous_axes, measure }` — a
  cartesian whose children mix discrete and continuous
  classes. The discrete part is enumerable; the continuous
  part is measure-based. Like `Continuous`, requires an
  enclosing sampling `order(_, _, Some(n))` per V8.

Each constructor's cardinality is a function of its inputs:

| Constructor | Cardinality |
|---|---|
| `clause(_, source)` | `source.cardinality()` (any of the five classes) |
| `cartesian(c1, ..., cN)` | When **independent** (no cross-references between clause sources): discrete × discrete = product (Unbounded if any Unbounded); all-Continuous = `Continuous { intervals: [...K-D...], measure: Product([...]) }`; mixed discrete + Continuous = `Hybrid { discrete_axes, continuous_axes, measure }`. When **dependent** (any clause source references a prior clause's variable): cardinality is a dependent sum `Σ_{outer} |C_inner(outer)|` rather than a product. The independence pass (§3.2) determines which rule applies. |
| `zip(c1, ..., cN, Strict)` | common cardinality (load-error if mismatch). All children must be discrete; continuous or mixed-class children rejected by V7. |
| `zip(c1, ..., cN, Truncate)` | `min` of children's cardinalities. All children must be discrete (V7). |
| `zip(c1, ..., cN, Cycle)` | `max` of children's cardinalities (Unbounded if any are). All children must be discrete (V7). |
| `union(c1, ..., cN)` | sum for discrete (Unbounded if any Unbounded). Continuous or mixed-class children rejected by V9. |
| `filter(c, _)` | discrete in → `BoundedAtMost`/`Unbounded` (existing rules); `Continuous` in → `ContinuousAtMost` with measure ≤ input measure; `Hybrid` in → `Hybrid` with reduced measure on the continuous axes and the same discrete-axis shape (filter cannot grow the space, only shrink the realized subset) |
| `order(c, _, None)` | `c.cardinality` (continuous stays continuous, unsampled) |
| `order(c, _, Some(n))` | for discrete: `min(c.cardinality, n)`; for Continuous / ContinuousAtMost: `Bounded(n)` — the sampling strategy materializes n discrete samples |

Operations propagate cardinality classes predictably. Four
upgrade/downgrade rules:

- `Bounded → BoundedAtMost` requires an explicit filter.
- `Continuous → ContinuousAtMost` requires an explicit filter.
- `Continuous → Bounded` requires an explicit `order(_, _,
  Some(n))` (this is V8 — the only way to enumerate a
  continuous source).
- `Hybrid → Bounded` requires an explicit `order(_, _,
  Some(n))` (V8 applies to Hybrid as well — the continuous
  axes still need sampling). The order's strategy must accept
  `IndexFn::Hybrid` per §3.6 (Halton, Sobol, Lhs, Extrema).

The Continuous classes are what make the algebra useful for
parameter sweeps over real-valued coordinates. Halton, Sobol,
and Lhs are *natively* continuous-domain strategies; over a
Continuous input they sample the underlying measure directly,
which is the strategies' mathematical definition (§10.2 R2).

### 6.2 Memory footprint class

Footprint reflects the **stream-first model**: every constructor
runs in per-tuple steady-state memory by default. The only nodes
that hold more than per-tuple state are the explicit
materialization barriers documented below.

| Constructor | Footprint | Justification |
|---|---|---|
| `clause` | O(1) above source's per-tuple state | Source is a stream producer (§3.1); one Value in flight per active position |
| `cartesian` | O(N) for an N-child node | One position cursor per child; one tuple in flight at the output |
| `zip` (Strict/Truncate) | O(N) for an N-child node | Lockstep walk; one tuple per child in flight |
| `zip` (Cycle) | O(N) + O(cycled-child's cardinality) per child that's not the longest | Cycling re-emits earlier values; the shorter children must replay, so they hold their own buffered values. The longest child still streams. |
| `union` | O(active child's footprint) | One child active at a time; previous children released before next starts |
| `filter` | O(child's footprint) + O(1) per-tuple | Stream the child; evaluate predicate per tuple; emit or drop |
| `order` with `Lex` (un-truncated) | O(child's footprint) | Lex IS the enumeration order; identity I5 applies; no buffering |
| `order` with `Lex` truncated `Some(n)` | O(child's footprint) + O(1) counter | Stream first n then halt |
| `order` with any other strategy | **Materialization barrier**; see §6.3 | All other strategies must inspect a working set to reorder |

Critical consequence: **a well-formed AST's per-tuple steady-
state memory is constant in the dispensed-tuples count**, except
at non-Lex `order` nodes which are explicit materialization
points. The compiler can statically classify each AST as
"fully streaming" or "streaming-with-N-explicit-barriers"; the
total memory budget is the sum of the barriers' working sets
plus per-tuple constant overhead.

This is the load-bearing model property: a user who constructs
a comprehension AST and pulls one tuple from the resulting
streamer pays O(1) steady-state cost per pull, plus whatever
the explicit barriers in their AST declare. The "ability to
compute the first tuple validates the steady-state overhead"
contract from the user's feedback is exactly this property.

### 6.3 The materialization barrier

A **materialization barrier** is a node that must inspect more
than one tuple of its input at a time to produce one tuple of
its output. There are exactly two natural barriers in the
algebra and both are bounded by their input's working set, not
the full upstream:

1. **`order` with any non-`Lex` strategy** — the strategy reads
   a working set, applies its permutation, then emits.
2. **`zip(Cycle)` for the shorter children** — cycling requires
   replaying earlier values; the shorter children buffer their
   own cardinality.

The barrier's working-set size is **not necessarily the full
input cardinality**. It is the smallest set the strategy needs
to produce correct output:

- `order(c, Lex, Some(n))` — no barrier; emit first n.
- `order(c, halton/n, Some(n))` — barrier of size `n` if
  push-down (§10) compiles the halton sequence to direct
  index selection over the cartesian lattice. Naïve unfused
  compilation has barrier of size `|c|`.
- `order(c, extrema/k)` — barrier of size `O(k * d)` where d
  is cartesian dimensionality, IF push-down compiles "extrema
  enumeration" against the lattice index space; full input
  size otherwise.
- `zip(Cycle)` shorter children — barrier of size = each
  shorter child's cardinality.

Push-down (§10) is **the** mechanism that keeps these working
sets small. Naïve compilation produces correct output but at
full-input-cardinality cost; the optimizer's job is to recognize
patterns where a closed-form lattice-index computation produces
the same result with O(output-size) memory. This is why §10
is a **required** pass, not an optional one — for many user-
authored expressions the naïve compilation makes streaming
illusory.

**Validity V6 carries forward unchanged**: a materialization
barrier requires bounded input. Unbounded input passing through
a non-Lex order is a load-time validation failure regardless
of whether push-down would have shrunk the working set.

---

## 7. Algebraic equivalences

These are the **rewrite rules** a normalizing pass or an
optimizer may apply. They are equivalences — the rewritten AST
produces a structurally-identical dispense stream (same tuples,
same order) from the same source values. Each rule's direction
of preference (left → right or right → left) is documented
where it matters for canonicalization or for performance.

### 7.1 Associativity

**A1 — union associativity:**
`union(union(a, b), c)` ≡ `union(a, union(b, c))` ≡
`union(a, b, c)`.
*Canonical form:* flat n-ary `union(a, b, c, ...)`.

**A2 — cartesian associativity:**
`cartesian(cartesian(a, b), c)` ≡ `cartesian(a, cartesian(b, c))`
≡ `cartesian(a, b, c)`.
*Canonical form:* flat n-ary `cartesian(a, b, c, ...)`.

**A3 — zip is NOT associative.** `zip(zip(a, b), c)` ≢
`zip(a, zip(b, c))` in general (the inner zip's tuple shape
becomes part of the outer's name set, changing what zips against
what). Flat `zip(a, b, c, modes...)` is the only canonical form;
nested zips are a parse error.

### 7.2 Filter conjunction

**F1 — chained filters fold:**
`filter(filter(c, p), q)` ≡ `filter(c, p && q)`.
*Canonical form:* one filter with a conjoined predicate. The
user-surface chain `... where p where q` exists for readability;
the AST stores one node.

### 7.3 Filter distribution

**D1 — filter distributes over union:**
`filter(union(a, b), p)` ≡ `union(filter(a, p), filter(b, p))`.
*Canonical form:* filter on top (left side). Distribution
(right side) is the per-sub-space form the user can write
explicitly; canonicalization folds it back to the outer form
when the predicate doesn't depend on which sub-space the tuple
came from. The right form is preserved by canonicalization if
the predicates differ per sub-space.

The distribution is unconditionally safe because **Axiom V2
requires identical tuple shape across union children** (§5.2):
every name `p` could reference is bound by every child, so
"this predicate makes sense against child a but not child b"
is structurally impossible. V2 is what makes D1 a rewrite the
optimizer can fire without any cross-child analysis; loosening
V2 would loosen D1 in lockstep.

**D2 — filter does NOT distribute over cartesian.**
`filter(cartesian(a, b), p)` ≢ `cartesian(filter(a, q),
filter(b, r))` in general (cross-cutting predicates can't be
split into per-axis predicates without loss). When `p`
factorizes as `pa(vars-of-a) && pb(vars-of-b)` the optimizer
MAY push down to the cartesian children, but it's an
optimization, not an equivalence at the operator level.

**D3 — filter does NOT distribute over zip.** Same reason as
D2.

### 7.4 Order chaining

**O1 — outer order wins, modulo truncation:**
`order(order(c, s1, None), s2, t)` ≡ `order(c, s2, t)`.
The inner full-permutation is wasted; the outer redoes it.
The optimizer SHOULD fold.

**O2 — outer order over truncated inner is NOT redundant:**
`order(order(c, s1, Some(n)), s2, t)` is meaningful: the inner
truncates to n in `s1` order; the outer reorders those n
survivors by `s2` and possibly re-truncates. The two-stage form
is the canonical way to express "the first n in this order, then
permuted that way."

**O3 — order does NOT distribute over union.** Reordering the
concatenation is a different operation than reordering each
piece independently. Both forms are user-writable and have
distinct semantics; the optimizer does not rewrite between them.

**O4 — order does NOT distribute over cartesian.** Same reason.
A cartesian's `Lex` order is "rightmost varies fastest"; an
imposed `order halton` mixes positions across axes in a way that
can't be factored back to per-axis permutations.

### 7.5 Filter ↔ Order non-commutativity

**N1 — filter then order ≠ order then filter (with truncation):**
- `order(filter(c, p), strategy, n)`: filter survivors get
  reordered, then first n picked. Always emits ≤ n tuples; the
  count depends on filter survival.
- `filter(order(c, strategy, n), p)`: first n in order picked,
  then filtered. Emits between 0 and n tuples; tuples that
  passed the truncation but fail the predicate are dropped,
  leaving a final count that may be much less than n.

Both forms are valid, distinct, and useful. The optimizer does
NOT rewrite between them; the user's authored form is the
intended semantic.

**N2 — filter then order = order then filter (without
truncation):** `order(filter(c, p), strategy, None)` ≡
`filter(order(c, strategy, None), p)`. Without truncation,
permutation and selection commute. The optimizer MAY prefer
the filter-first form (smaller working set for the order
materialization).

---

## 8. The syntactic surface

The algebra is what the compiler manipulates; the user writes a
flatter surface that desugars to the algebra. The surface
preserves regularity by mapping each surface form to exactly
one algebraic constructor (or a small fixed chain).

### 8.1 The single `for` keyword

Polydat expression form uses one keyword: `for`. The RHS shape
disambiguates which constructor:

```text
for var in source                              → clause(var, source)
for var1 in src1, var2 in src2, ...            → cartesian(clause(var1, src1), ...)
for (var1, var2) in (src1, src2)               → zip([clause(var1, src1), clause(var2, src2)], Strict)
for (var1, var2) in zip_truncate(src1, src2)   → zip([...], Truncate)
for (var1, var2) in zip_cycle(src1, src2)      → zip([...], Cycle)
for [ comprehension1, comprehension2, ... ]    → union(c1, c2, ...)
```

The trailing modifiers append `filter` and `order` nodes:

```text
<for-expression> where <predicate>             → filter(<for-expression>, <predicate>)
<for-expression> order <strategy>[/<n>]        → order(<for-expression>, <strategy>, <truncation>)
```

`where` and `order` chain in source order, producing nested
filter/order nodes per F1/O1 chaining rules.

### 8.2 Recursive composition

Wherever the surface allows a `<comprehension>`, the full set
of for-forms applies. This is the regularity property:

```text
for [
    for k in 10, limit in 10..20 where {limit} > 15,
    for k in 100, limit in 100..200 order halton/5,
]
```

The strings-as-sub-spaces shorthand (`for ["k in 10, ..."]`)
remains valid as a parsing convenience — each string is parsed
as a comprehension expression. The bracketed-comprehension form
and the bracketed-string form are equivalent at the AST level.

### 8.3 Comprehensions as named values

A comprehension binds to a wire of `PortType::Streamer`. Bound
wires reference comprehensions by name; the same comprehension
may participate in multiple derived expressions:

```text
base := for k in 1..100, limit in 1..100

// Three derived streamers from one base.
boundary := base where {k} == 1 || {k} == 100 || {limit} == 1 || {limit} == 100
sampled  := base order halton/50
fast_corner := boundary order extrema/1
```

`base`, `boundary`, `sampled`, `fast_corner` are four distinct
ASTs; the compiler is free to share `base`'s evaluation across
the three derivatives or to recompute per-`PolyStreamer`
instantiation. SRD-78's "one streamer per Arc" semantic means
each wire-bound comprehension has its own dispense cursor.

### 8.4 Inferred union (special case)

Today's parser supports `for k in 10, limit in ..., k in 100,
limit in ...` with repeated names inferring `union`. Under the
regular algebra this is a parser convenience — the disambiguation
runs after clause-list parsing and lifts to `union(cartesian(...),
cartesian(...))`. The convenience persists for backward
compatibility; the canonical form is the explicit bracketed
union.

---

## 9. The compilation model

The algebra compiles to a small operator language suitable for
either a stream-fusion compiler or a stack-machine interpreter.
This section is the **executable contract** behind the
"resource-bounded execution" requirement.

### 9.1 Operator IR

Every well-formed comprehension AST compiles to a finite
sequence of operators from this set. **Every operator is a
stream transducer.** Operands flow as `Stream<Tuple>` —
`advance() -> Option<Tuple>` — never as `Vec<Tuple>`. The two
exceptions (the only places memory is held above per-tuple
state) are called out explicitly as materialization barriers.

```text
PUSH_CLAUSE(name, source)
    Push a single-name tuple stream produced by `source`.
    Sources are themselves stream producers: their advance()
    yields one Value per call. The source is bound at runtime
    by the streamer's outer scope (SRD-78), not eagerly
    enumerated here. Per-pull cost: O(1) plus source.advance().
    Steady-state memory: O(1).

CARTESIAN(N)
    Replace the top-N stream operands with one stream that
    enumerates their cross product in lexicographic order. The
    operator maintains one position cursor per child and one
    output tuple buffer; it lazily advances the slowest cursor
    each pull. Per-pull cost: O(1) amortized. Steady-state
    memory: O(N) for the cursors plus one tuple.

ZIP(N, mode)
    Replace the top-N stream operands with their lockstep
    diagonal. Under Strict and Truncate the operator pulls one
    tuple from each child per output pull; per-pull cost O(N),
    steady-state memory O(N). Under Cycle the operator buffers
    each non-longest child's full output once and replays from
    the buffer thereafter; buffer size per child equals that
    child's cardinality.

UNION(N)
    Replace the top-N stream operands with a stream that
    concatenates them in operand order. The operator drains
    operand i fully before pulling from operand i+1; only one
    operand is active at a time. Steady-state memory: O(active
    operand's working set).

FILTER(predicate)
    Wrap the top operand with a per-tuple predicate check. On
    each output pull the operator pulls from its child and
    re-pulls on rejection; emits the first accepted tuple. The
    operator holds no per-tuple state of its own. Streaming.

ORDER_STREAMING(Lex, truncation)
    A no-op pass-through (un-truncated) or a counter wrapper
    (truncated to `Some(n)`) that halts after n pulls. Lex IS
    the cartesian enumeration order, so the strategy adds
    nothing. Streaming.

ORDER_MATERIALIZE(strategy, truncation)
    MATERIALIZATION BARRIER. On first pull the operator builds
    a working set sufficient to satisfy the strategy: either
    (a) the strategy's full domain (input cardinality) if the
    naïve form is emitted, or (b) a closed-form working set
    of size determined by the strategy and truncation if §10's
    push-down optimizer fused the strategy into the input's
    enumeration. Once built, the operator emits permuted
    tuples (then truncated) from the working set as a stream;
    subsequent pulls are O(log size) for selection-strategy
    cases, O(1) for permutation-strategy cases.

    Allowed strategies: the full §3.6 taxonomy minus `Lex`
    (Lex compiles to `ORDER_STREAMING`, not this opcode) —
    `ReverseLex`, `Shuffle` (PRNG seed captured at
    materialization), `Halton`, `Sobol`, `Lhs`, `Extrema`,
    `Shells`, `Diagonal`, `Antidiagonal`. Per-strategy input-
    shape requirements and continuous behavior are documented
    in §3.6's strategy table. There is no user-defined
    "Custom" strategy in the algebra — user-authored ordering
    compiles to one of the named strategies or is rejected at
    parse time.

DISPENSE
    Bind the top stream as the comprehension's result. The
    consumer (PolyStreamer per SRD-78) pulls from this stream.
```

Compilation from AST to IR is a bottom-up tree walk: each AST
node emits its children's IR sequences in left-to-right order,
then its own operator(s). `cartesian`, `zip`, `union` use
N-arity opcodes; `filter`, `order` use unary wrappers.

**IR as immutable public API.** The compiled IR sequence is
exposed via `polydat::comprehension::ir::Program` as a
`#[non_exhaustive]` `Vec<Op>` accessible by value. Consumers
may inspect the sequence (e.g. for cost estimation, tracing,
or alternative backends) but cannot mutate it post-compile —
the optimizer (§10) is the only path from AST to IR, and the
resulting program is frozen. This is the user feedback on
§14's "expose IR or not" question: expose it, immutable.

### 9.2 Correctness contract

For any well-formed AST C, the compiled IR sequence Op(C),
executed by either:

(a) A stack-machine interpreter that maintains a stream stack
    and processes opcodes left-to-right, OR
(b) A stream-fusion compiler that rewrites the IR sequence into
    a single nested generator function,

produces **identical dispense sequences** (same tuples in the
same order). Differences are limited to memory layout and
throughput; observable behavior at the dispense interface is
the same.

### 9.3 Resource bounds

For any well-formed AST C, the compiled IR's peak memory usage
is a closed-form sum over the program — **no term grows with
the dispensed-tuple count for streaming nodes**:

```
memory(C) ≤
    O(depth(C))                                 // operator stack
  + Σ (per-operator steady-state, see §6.2)     // O(1) for streaming ops
  + Σ (zip(Cycle) shorter-child cardinality)    // barrier 1
  + Σ (ORDER_MATERIALIZE working-set size)      // barrier 2
```

The first two terms are O(1) in the source cardinalities; the
operator stack is bounded by AST depth (constant for flat
composition, logarithmic for balanced trees), and each
streaming opcode holds O(N-arity) cursors plus one tuple. The
remaining two terms are the only places memory scales with
source size, and both are explicit in the AST.

The barrier working-set sizes are:

- `zip(Cycle)` shorter child: that child's full cardinality
  (it must replay).
- `ORDER_MATERIALIZE` without push-down: input cardinality
  (the strategy needs to inspect everything).
- `ORDER_MATERIALIZE` with push-down (§10): the strategy-
  specific minimum — for halton/n over a cartesian, O(n); for
  extrema/k, O(k·d); for shuffle/n with cartesian input,
  O(n) index draws.

There are NO hidden buffering, copy, or fan-out terms. Every
opcode either streams (O(operator-local state) per pull) or
declares its materialization at compile time. The "basic
combinatoric tracking data" budget the user asks for is exactly
this closed-form sum.

The compile-time bound checker computes this expression
symbolically from the AST. A consumer can ask: "what is the
maximum memory this comprehension will hold at steady state?"
and get a numeric answer (when sources are bounded) or a
symbolic Unbounded with the barrier identified (when sources
are unbounded but the barrier-policy permits it via push-down).

### 9.4 Compile-time guarantees

The well-formedness check (Axiom C2) plus the validity axioms
(V1-V9) plus the boundedness propagation (§6.1) collectively
imply:

1. Every AST that passes validation compiles successfully — no
   "valid AST that won't compile" cases.
2. Every compiled IR has a closed-form peak-memory bound
   derivable at compile time from source cardinalities.
3. Every compiled IR has a closed-form dispense-cardinality bound
   derivable at compile time, returning `Bounded(n)`,
   `BoundedAtMost(n)`, `Unbounded`, `Continuous`, or
   `ContinuousAtMost` per §6.1.
4. Every IR that the bound-checker proves unbounded-cardinality
   is reachable as a `PolyStreamer` only via the unbounded-
   variant queue (SRD-78 §"Unbounded sources") — bounded-only
   consumers can refuse to instantiate the streamer.
5. Every IR with `ORDER_MATERIALIZE` over an `Unbounded` discrete
   input fails validation (Axiom V6); every IR whose outermost
   cardinality is `Continuous` or `ContinuousAtMost` fails
   validation (Axiom V8) — neither reaches compile.

All five guarantees assume the IR was produced from an
*optimized* AST. §10's post-parse optimizer is a required pass
upstream of §9.1's compilation — it converts user-authored
expressions into the push-down forms whose closed-form working
sets the bounds above describe. The un-optimized AST is the
correctness reference, not the runtime input.

### 9.5 Consumption surfaces

A compiled comprehension IR is consumed at two distinct
**orders**, with three concrete surface forms. The orders are
not levels in a single pipeline — they are independent,
first-class consumption modes that share the compiled IR but
maintain separate dispense state.

#### 9.5.1 The two orders

**First-order: coordinate tuples.** A `CoordinateStream` pulls
one `Vec<(String, Value)>` per `advance()` — the named
coordinate tuple, nothing more. This is what §9.1's `DISPENSE`
opcode produces directly. The consumer interprets the tuple
however they want: inspection, logging, exporting to another
system, feeding a non-polydat computation.

**Second-order: scoped kernel instances.** A
`ScopedKernelStream<K>` pulls one **scoped polydat kernel
instance** per `advance()` — a `ScopedKernelInstance<K>` whose
scope already has the coordinate tuple's values bound as scope
variables. The consumer invokes the kernel as if it were a
standalone kernel; the coordinate binding is transparent.

The second order is a **functor** over the first: every
`ScopedKernelStream` is conceptually the image of a
`CoordinateStream` under the mapping `coords ↦
parent_kernel.scope(coords)`. But the spec does **not** model
the second-order stream as a Rust-style `.map()` view over the
first. The two are independent first-class streams (see §9.5.2).

#### 9.5.2 Independence contract

The three consumption surfaces are produced by three independent
factory functions on the comprehension handle:

```text
trait Comprehension {
    fn coordinate_stream(&self) -> CoordinateStream;
    fn scoped_kernel_stream<K>(&self, parent: &K) -> ScopedKernelStream<K>;
    fn scope_once<K>(&self, parent: &K, coords: &CoordTuple) -> ScopedKernelInstance<K>;
}
```

Each call to `coordinate_stream` or `scoped_kernel_stream`
returns a **fresh streamer** with its own dispense cursor. The
streamers share the underlying compiled IR (immutable per §9.1)
but allocate their own per-streamer state: cursor positions,
barrier working sets, PRNG draws for shuffle/halton/sobol/lhs.

The independence is **structural, not performance-driven**:

- Two `CoordinateStream` instances from the same comprehension
  produce identical dispense sequences but advance independently
  — pulling from one does NOT advance the other.
- A `CoordinateStream` and a `ScopedKernelStream` from the
  same comprehension produce dispense sequences whose
  coordinate tuples are identical (same order, same values) but
  advance independently.
- Two `ScopedKernelStream` instances from the same comprehension
  produce identical dispense sequences of scoped kernel
  instances and advance independently.

This is the SRD-78 "one streamer per Arc, each with its own
dispense cursor" semantic, generalized across the two orders.

#### 9.5.3 The one-shot map function

`scope_once(parent, coords)` is the **non-streamed** form: a
pure function that takes a single coordinate tuple (obtained
from anywhere — typically a `CoordinateStream` snapshot, a
replay log, or a manually-constructed tuple) and produces a
single scoped kernel instance. It does not advance any stream
and does not consult any dispense cursor.

This is the surface workload-replay and debugging tooling uses:
"Replay the kernel for the coordinate tuple captured in this
log entry," "Run the kernel for this specific point in the
parameter sweep," "Construct a kernel instance for the tuple
my UI just emitted." None of these need a stream; they need a
point query.

`scope_once` is also the **unit of work** that
`ScopedKernelStream::advance()` performs internally — pull one
coordinate tuple from the underlying IR, apply `scope_once` to
produce one `ScopedKernelInstance<K>`, return. The function is
exposed publicly so callers can perform the same operation
without going through a streamer at all.

#### 9.5.4 Why two orders, not one

The temptation is to expose only `ScopedKernelStream` and treat
the coordinate stream as an internal implementation detail.
That conflates two concerns:

- **Coordinate enumeration** is a polydat concern — it's what
  the algebra in §3–§10 specifies. The coordinate tuple is the
  observable contract.
- **Kernel instantiation** is a polydat kernel concern — it's
  how a coordinate tuple becomes runnable. It involves scope
  binding, kernel cloning, possibly resource allocation. None
  of this is comprehension-spec material.

Separating the surfaces lets:

- Inspection / logging / export tooling work over coordinate
  tuples without paying the kernel-instantiation cost.
- Workload runners work over scoped kernel instances without
  the consumer having to write the `coords ↦ kernel.scope(...)`
  glue.
- Replay tooling pull a tuple from a log and call
  `scope_once` directly — no streamer needed.
- The polydat optimizer and metadata algebra stay focused on
  coordinate-tuple semantics; kernel concerns don't leak in.

#### 9.5.5 What this means for SRD-78

SRD-78 (PolyStreamer) is the runtime that hosts both stream
surfaces. The compiled IR (§9.1) is the shared resource;
SRD-78's streamer instances are the per-surface dispense state.
The first-order / second-order split is a polydat-side
architectural decision; SRD-78 implements both as concrete
streamer types over the same IR.

The "lock-free shared cursor" semantic SRD-78 documents applies
**per streamer instance**, not across streamers from the same
comprehension. Two `CoordinateStream` instances from one
comprehension have two independent lock-free cursors, not one
shared cursor.

---

## 10. Post-parse optimizer

The §6.2 footprint table and the §9.3 resource bound only hold
if the runtime sees AST shapes that the streaming-with-explicit-
barriers model can actually realize cheaply. A user-authored
expression like

```text
for k in 1..1_000_000, limit in 1..1_000_000 order halton/30
```

is well-formed (passes validity) and compiles to an `ORDER_
MATERIALIZE(Halton, Some(30))` over a 10¹²-tuple cartesian.
Naïvely executed, it allocates a 10¹²-element working set to
extract 30 tuples. That is not "bounded by the AST's declared
barriers" in any useful sense — the bound exists but is
catastrophic.

The fix is **required**, not optional: a post-parse pass that
rewrites the AST into a form whose materialization barriers are
sized by the *output*, not the *input*. This pass is the
optimizer. It is part of the compilation contract — running it
is mandatory before §9.1's IR compilation.

### 10.1 What "push-down" means here

A push-down rewrite **fuses a strategy into its input's
enumeration**. Instead of:

1. enumerate every tuple of the input,
2. apply the strategy to the materialized buffer,
3. truncate and emit,

the rewriter produces:

1. compute a strategy-specific selection function over the
   input's *index space* (without enumerating it),
2. emit selected tuples by lattice-index lookup against the
   input.

The barrier's working set shrinks from O(input cardinality) to
O(selection size). The output tuples are identical (the strategy
is a function of index space; what changes is the order of
operations).

This only works when the input's enumeration is *index-
addressable* — that is, when there's a closed-form bijection
from `0..cardinality` to the input's tuples. Cartesian
satisfies this (the lattice indices are the addresses). Zip
satisfies it. Filter does not (the bijection from index to
surviving tuple requires evaluating the predicate against
candidates). Union satisfies it (concatenation arithmetic).

### 10.2 Required rewrites

The optimizer fires the following rules unconditionally. Each
rule preserves the dispense sequence per §7's equivalences and
shrinks the worst-case barrier working set. The guards are
predicates over the **metadata algebra** specified in §10.7;
each rule's "when does it fire?" reduces to a pattern match on
the node's metadata bundle plus its operator and a small fixed
set of structural shapes. Rules that need predicate-shape
information (R5 below) consult the predicate analyzer in §10.9
and never read AST internals directly.

The catalog opens with two canonicalization rules (R0a, R0b)
that put the AST into a normal form so downstream rules don't
have to handle equivalent variants. Every AST that enters the
optimizer is canonicalized first; every R-rule from R1 onward
runs against the canonical form.

**R0a — identity elimination** (Axioms I1–I5 from §4.2):

- `union(c)` → `c` (singleton union)
- `cartesian(c)` → `c` (singleton cartesian)
- `zip([c], _mode)` → `c` (singleton zip)
- `filter(c, "true")` → `c` (trivially-true predicate, when
  the predicate analyzer's `PredicateInfo` proves this)
- `order(c, Lex, None)` → `c` (un-truncated Lex)

Each rewrite strictly decreases AST node count and never
inflates any metadata field. Without R0a, downstream guards
would have to enumerate variants (R7's "two adjacent orders"
guard would have to also handle "one order over a singleton
union containing an order").

**R0b — associativity flattening** (Axioms A1, A2 from §7.1):

- `union(union(a, b), c)` → `union(a, b, c)` (flatten left)
- `union(a, union(b, c))` → `union(a, b, c)` (flatten right)
- `cartesian(cartesian(a, b), c)` → `cartesian(a, b, c)`
- `cartesian(a, cartesian(b, c))` → `cartesian(a, b, c)`

`zip` is NOT flattened per A3 (zip is not associative — nested
zips are a parse error and never appear in valid input).

Each flatten strictly decreases AST node count and produces a
canonical n-ary form that downstream rules can match without
enumerating nestings. After R0b a `union` or `cartesian` is
either a leaf (single-operand, eliminated by R0a) or a flat
n-ary node with no `union`/`cartesian` of the same type as a
direct child.

R0a and R0b run to a fixed point before R1 fires. The combined
fixed point is the AST's *canonical form* — the unique
representative within its identity- and associativity-equivalence
class. The optimizer's idempotence (§10.6.2) and the
reducibility catalog's confluence (§10.10.5) both rely on this
canonical form existing and being reachable in finite time.

**R1 — `order(c, lex, t)` → `truncated_lex(c, t)`.** When the
strategy is `Lex` and the input enumerates in Lex order
(cartesian, zip Strict/Truncate), the order operator is a
counter wrapper, not a barrier. Compiles to `ORDER_STREAMING`,
not `ORDER_MATERIALIZE`.

**R2 — `order(c, strategy, Some(n))` → `indexed_order(c,
strategy, n)`** when `c.index_addressable.is_some()` and
`strategy` has a closed-form rule over `c`'s `IndexFn`. Fires
over cartesian, zip, union, and any other index-addressable
input — V4 (§5) already constrained the strategy to require a
non-`None` IndexFn, so the optimizer's job is just to recognize
the closed-form lookup and emit the indexed opcode. Per-strategy
push-down rules:

- **Halton**: for discrete `Lattice`, emit n Halton-sequence
  indices over `[0, |c1|) × [0, |c2|) × ... × [0, |cN|)`, look
  up each multi-index in the cartesian's enumeration. For
  `Continuous`, emit n K-D Halton points in `[0,1)^K` and map
  each to the input's intervals (affine for Uniform measure;
  inverse-CDF for named measures). For `Hybrid`, mix the two:
  discrete axes get integer Halton lookups, continuous axes
  get interval-mapped points. Working set: O(n) draws plus
  per-axis cursors. Halton is deterministic and self-
  correlating; the K-D continuous form is the strategy's
  *native* mathematical definition.
- **Sobol**: same shape as Halton, with a Sobol generator —
  also native to continuous, also handles Lattice / Continuous
  / Hybrid uniformly.
- **Lhs**: for discrete, pre-stratify the n samples by axis
  (one permutation of `0..n` per axis), then emit n tuples by
  zipping the per-axis permutations. For `Continuous`,
  stratify each axis's interval into n equal-measure bins,
  draw one sample per bin, zip them; this is the classical
  Latin Hypercube design over a real K-D box. For Hybrid,
  combine the two per axis. Working set: O(n · N).
- **Extrema** (k corners): for discrete `Lattice`, enumerate
  the 2^N lattice corner positions, sort by the strategy's
  distance metric (per §3.6's named-strategy semantics), emit
  the top k. Working set: O(2^N · log(2^N)) = O(N · 2^N),
  independent of input cardinality. For N>20 the optimizer
  keeps a heap of size k instead of materializing all corners.
  For Continuous, the corners are the 2^N tuples formed from
  each axis's interval endpoints (with appropriate open/closed
  treatment); same selection logic.
- **Diagonal / Antidiagonal**: discrete `Lattice` only —
  per §3.6, these strategies reject Continuous inputs (no
  canonical step in a continuous space). Emit tuples whose
  index sum matches the strategy's diagonal walk. Working
  set: O(N) per emitted tuple.
- **Shells**: discrete only — per §3.6, "shells" in a
  continuous space is ill-defined without a discretization
  parameter, so Continuous inputs are V4-rejected. Emit
  tuples whose index falls on each shell in turn (per the
  strategy's shell partition). Working set: O(N) per
  emitted tuple plus a small per-shell counter.

For strategies in this list, R2 collapses `ORDER_MATERIALIZE`
to a strategy-aware streaming source. The barrier disappears.

**R3 — `order(filter(c, p), lex, None)` → `filter(order(c, lex,
None), p)`** by N2: when un-truncated, filter and Lex order
commute. The Lex-then-filter form is equivalent and cheaper to
emit (Lex is streaming; filter wraps it).

**R4 — `filter(union(a, b), p)` → `union(filter(a, p), filter(b,
p))`** by D1 (§7.3 + V2). Each child becomes its own filtered
sub-pipeline; downstream barriers (if any) get smaller inputs.

**R5 — `cartesian(c1, ..., cN) where {var-of-ci} == K` →
`cartesian(c1, ..., filter(ci, {var-of-ci} == K), ..., cN)`**
when the predicate factorizes per-axis. Per-axis filters are
applied before the cartesian sees the surviving rows, shrinking
the lattice the cartesian enumerates over.

**R6 — chained filter folding** (F1): `filter(filter(c, p), q)`
→ `filter(c, p && q)`. One predicate is one expression evaluation
per tuple instead of two.

**R7 — order chain folding** (O1): `order(order(c, s1, None),
s2, t)` → `order(c, s2, t)`. The inner full-permutation is
wasted; drop it.

### 10.3 Worked example: zip with computed permutations

The user feedback specifically called out **zip with computed
permutations**. Here is what that means and how the optimizer
makes it tractable.

A user writes:

```text
for (k, limit) in zip_cycle(1..1_000_000, [10, 50, 100])
  order halton/100
```

Surface intent: a zip-cycle stream of one million `(k, limit)`
tuples (cycling the three-element `limit` list), then 100
Halton-permuted samples.

Naïve compilation:
1. ZIP(Cycle) enumerates 1,000,000 tuples (cycling the colors).
2. ORDER_MATERIALIZE(Halton, 100) materializes all 1,000,000.
3. DISPENSE emits 100.

Working set: 1,000,000 tuples. Heap pressure for a 100-tuple
output. This is exactly the user's "ability to compute the first
tuple validates the steady-state overhead" concern.

The optimizer recognizes:
- `zip_cycle(c1, c2)` has **a closed-form index addressing
  function**: tuple at index i is `(c1[i mod |c1|],
  c2[i mod |c2|])`. The zip's index space is `0..max(|c1|,
  |c2|)` when one operand is finite and the other unbounded;
  `0..max(|c1|, |c2|)` when both finite under Cycle.
- Halton over an index space of size N emits draws by
  computing `floor(N · halton_k(i))` for the i-th Halton
  point.

So R2 generalizes to zip-with-Cycle: emit 100 Halton draws over
`0..1_000_000`, look each draw up against the zip's index
function. Working set: 100 indices + 3 buffered colors (for the
zip's shorter-child barrier from §6.3). Per-pull cost: one
Halton draw + two modulo operations.

This is the load-bearing case the user named. The optimizer's
existence (not its merely being present, but its *running before
compilation*) is what makes `zip · order(halton)` express
"sample 100 tuples from a billion-tuple cycle" without holding
a billion tuples. V4's per-strategy input-shape contract (§5)
admits the composition: Halton accepts any non-`None` `IndexFn`,
and `zip_cycle` publishes `IndexFn::Modular` per §10.7.

The same pattern extends to other strategies whose input
requirement Halton shares:
- `zip_strict(...) order halton/n` — index space is the common
  length (`IndexFn::Lockstep`); emit n Halton-indexed tuples.
- `zip_cycle(...) order shuffle/n` — emit n PRNG-shuffled
  indices, look up against the Modular index function.
- `zip_strict(...) order sobol/n` — same shape, Sobol generator.

Strategies that require a multi-axis `Lattice` (Extrema,
Shells, Diagonal, Antidiagonal, multi-axis Lhs) do not fire R2
over a zip — the zip's `IndexFn` is 1-D, and per §3.6 these
strategies in 1-D are *degenerate*. §5.8's warning surfaces at
parse, and the form compiles to a trivially-correct degenerate
output (e.g. Extrema over 1-D zip yields the first and last
tuples).

### 10.4 Worked example: halton over cartesian

The motivating case at the top of this section:

```text
for k in 1..1_000_000, limit in 1..1_000_000 order halton/30
```

After R2 fires:
1. PUSH_CLAUSE k, source `1..1_000_000`.
2. PUSH_CLAUSE limit, source `1..1_000_000`.
3. CARTESIAN(2) — produces an index-addressable stream of size
   10¹², with one cursor per axis.
4. ORDER_MATERIALIZE(Halton, 30) → rewritten to indexed_halton
   over the cartesian's lattice. Emit 30 Halton draws in
   `[0, 1_000_000) × [0, 1_000_000)`, look up each.
5. DISPENSE.

Working set: 30 draws + 2 cursors. The barrier remains a barrier
in spirit (we still don't emit tuples until the strategy decides
on the indices), but its size is O(output), not O(input).

### 10.5 Worked example: filter distribution shrinking a barrier

```text
for [
  for k in 1..1000, x in 1..1000,
  for k in 1..1000, x in 1001..2000,
]
where {k} > 500
order halton/50
```

After R4 (filter distributes over union):

```text
for [
  for k in 1..1000, x in 1..1000 where {k} > 500,
  for k in 1..1000, x in 1001..2000 where {k} > 500,
]
order halton/50
```

After R5 (per-axis filter pushed into each cartesian):

```text
for [
  for k in 501..1000, x in 1..1000,
  for k in 501..1000, x in 1001..2000,
]
order halton/50
```

Each filtered sub-cartesian has cardinality 500 × 1000 = 500_000
tuples (the k-axis shrunk from 1000 to 500 elements via the
range-narrowed source). The union's combined index space is
therefore `Concatenation { segment_sizes: [500_000, 500_000] }`,
totaling 1_000_000.

After R2 (halton over the resulting union of cartesians):

Halton accepts any non-`None` `IndexFn` per §3.6, and a union of
two index-addressable cartesians has `IndexFn::Concatenation
{ segment_sizes: [500_000, 500_000] }`. R2 fires: emit 50 Halton
draws over `[0, 1_000_000)`, map each through the Concatenation
index function to either `cartesian_0[i]` (i < 500_000) or
`cartesian_1[i - 500_000]` (i ≥ 500_000), then descend into the
chosen cartesian's lattice.

Working set after all four rewrites (R4 → R5 → R2): 50 Halton
draws + per-axis cursors. Sources stream. Total per-pull O(1)
above the cursor count.

If the author's intent was per-sub-space Halton (15 spread points
per sub-space rather than 50 spread across the combined
concatenation), N1 says they must author that form explicitly:

```text
for [
  for k in 501..1000, x in 1..1000 order halton/25,
  for k in 501..1000, x in 1001..2000 order halton/25,
]
```

The two forms emit different tuple sets; the optimizer never
rewrites between them. The point of this worked example is to
show R4 + R5 + R2 composing: filter distribution → per-axis
filter pushdown → indexed-halton over the resulting union.

### 10.6 Optimizer contract

The optimizer is a thin loop over the **reducibility analyzer**
(§10.10): ask the analyzer for a `ReducibilityFinding` on the
current AST, apply the finding's witness if non-empty, repeat
until the empty finding comes back. All the load-bearing
intelligence lives in the analyzer; the optimizer just drives
the loop and re-propagates metadata after each application.

The optimizer is a function `Ast → Ast` with these properties:

1. **Semantic-preserving.** For every input AST C, the rewritten
   AST C' produces the same dispense sequence per §7.
2. **Idempotent.** `optimize(optimize(C)) ≡ optimize(C)`.
3. **Decidable termination.** Rewrites strictly decrease a
   well-founded measure (a tuple of (max barrier working-set
   size, AST node count, filter depth)) or leave the AST
   unchanged; the optimizer halts.
4. **Bounds-improving.** For every C, `peak_memory(optimize(C))
   ≤ peak_memory(C)` per §9.3's formula.
5. **No rejections.** The optimizer never rejects an AST; rules
   that don't apply are simply skipped. Validity is decided
   pre-optimizer (V1-V9 per §5); rejections happen there.

Properties 1 and 4 together are the "tightly and strictly
verified for correctness and the ability to implement cleanly"
property the user named: the optimizer can only shrink memory,
never inflate it, and the dispense sequence is invariant.

Property 5 means there is no "user wrote something pessimal,
optimizer reports it" path — the optimizer silently improves
what it can and leaves the rest. Diagnostics about expressions
that compile but produce degenerate or surprising output (e.g.
Extrema over a 1-D zip) are §5.8's `ValidationWarning` channel,
not the optimizer's concern.

### 10.7 Metadata algebra

The R-rules in §10.2 are guards over a small **metadata bundle**
carried by every well-formed AST node. The optimizer never
consults the AST for anything else (no global walks, no late-
binding callbacks, no analysis hooks). Metadata propagates
bottom-up as a monoid; each constructor's metadata is a total
function of its children's metadata and its own scalar
parameters.

#### 10.7.0 Timing — metadata is contextual, not statically-only

A source's metadata (cardinality, IndexFn) is **a property of
the evaluated source in a kernel context**, not a property of
the AST in isolation. The rules in §10.7.1–§10.7.5 below
specify *what* every constructor's metadata is; this section
specifies *when* it becomes knowable.

Sources are partitioned into three **eval classes**:

- **Statically evaluable**: `Literal { values }`,
  `IntRange { lo, hi, step }`, `ContinuousInterval { … }`,
  and any `Generator` polydat recognizes as built-in (the set
  enumerated in §10.7.6). For these, evaluation succeeds with
  no kernel context — `source.evaluate(None)` returns
  `EvaluatedSource` — and the compile-time planner computes
  the full metadata bundle during parse / R0–R7 optimization.
- **Context-required**: `WorkloadParamList { name }` and
  `Generator` outside the built-in set. For these,
  `source.evaluate(None)` returns `NeedsContext`; the runtime
  evaluator supplies a kernel context at evaluation time.
  The metadata becomes knowable then — same rules,
  later firing.
- **Distribution**: not enumerated until an enclosing
  `Order(_, sampling-strategy, Some(n))` discharges V8;
  evaluation is the sampling pass itself.

The propagation rules in §10.7.1–§10.7.5 are unchanged. What
the rules describe is what holds *once the source has been
evaluated*. For statically-evaluable sources, "once" is
compile time; for context-required sources, "once" is
strategy-invocation time inside the runtime evaluator.

A statically-evaluable source whose evaluator returns
`NeedsContext` is a polydat bug, not a workload-author error.
A context-required source whose evaluator fails because
required context is missing surfaces as a runtime
evaluation error attributed to the clause.

#### 10.7.1 The bundle

```text
Metadata {
  cardinality:        CardinalityClass,   // §6.1
  index_addressable:  Option<IndexFn>,    // closed-form bijection 0..|c| → tuple
  natural_order:      NaturalOrder,       // how this node enumerates
  materialization:    Materialization,    // streaming or sized barrier
}

enum IndexFn {                            // closed-form addressing schemes only
  Lattice       { axis_sizes: Vec<usize> },           // cartesian (discrete)
  Lockstep      { length: usize },                    // zip(Strict|Truncate)
  Modular       { axis_sizes: Vec<usize> },           // zip(Cycle)
  Concatenation { segment_sizes: Vec<usize> },        // union
  Continuous    { intervals: Vec<Interval>,           // cartesian-of-continuous (K-D box)
                  measure: ProductMeasure },          //   1-D scalar range when intervals.len() == 1
  Hybrid        { discrete_axes: Vec<usize>,          // mixed discrete × continuous cartesian
                  continuous_axes: Vec<Interval>,
                  measure: ProductMeasure },
}

struct Interval { lo: f64, hi: f64, lo_open: bool, hi_open: bool }

enum ProductMeasure {
  Uniform,                                 // Lebesgue measure on the box
  Named(MeasureName),                      // e.g. Normal, Exponential, Beta
  Product(Vec<ProductMeasure>),            // per-axis product of measures
}

enum NaturalOrder {
  Lex,                             // cartesian, clause, order(Lex,_)
  Lockstep,                        // zip
  Sequential,                      // union (operand 0 fully, then 1, ...)
  Strategy(StrategyName),          // order(non-Lex, _)
}

enum Materialization {
  Streaming,
  BoundedBarrier { working_set_size: usize },
  UnboundedBarrier,                // currently always V6-rejected
}
```

Every field is either a closed enum (capability bit) or a
closed-form numeric/symbolic descriptor. `IndexFn` is a typed
enum, never an opaque callback. `StrategyName` is the §3.6
named-strategy enum; adding a new strategy is a coordinated
type extension (§14), not an open registration point.

#### 10.7.2 Propagation rules

One rule per constructor; constant-time per node; no rule can
fail or be partial.

`clause(name, source)`:
- cardinality: `source.cardinality`
- index_addressable:
  - `Some(Lattice { axis_sizes: [n] })` if source is `Bounded(n)`;
  - `Some(Continuous { intervals: [source.interval], measure: source.measure })` if source is `Continuous`;
  - `None` for `BoundedAtMost`, `Unbounded`, or `ContinuousAtMost` (filter destroys addressability uniformly)
- natural_order: `Lex` for discrete; `Strategy(<pending sampling>)` for continuous (V8 requires an enclosing sampling order before dispense)
- materialization: `Streaming`

`cartesian(c1, ..., cN)`:
- cardinality: product per §6.1
- index_addressable:
  - **dependent sources** (§3.2 — clause N's source references
    clause M's variable for M < N): always `None`. The axis
    sizes are runtime-dependent on prior axes' values, so no
    compile-time closed-form bijection exists. The cartesian
    is still well-formed and dispenses correctly, but R2
    push-down cannot fire — non-Lex orderings over a dependent
    cartesian materialize naïvely.
  - all discrete + addressable + non-dependent: `Some(Lattice { axis_sizes: [c1.|c|, ..., cN.|c|] })`
  - all continuous + non-dependent: `Some(Continuous { intervals: ..., measure: Product([c1.measure, ..., cN.measure]) })`
  - mixed discrete + continuous + non-dependent: `Some(Hybrid { discrete_axes, continuous_axes, measure })`
  - any child not addressable: `None`
- natural_order: `Lex` for fully-discrete; otherwise inherited as `Strategy(<pending sampling>)`
- materialization: `Streaming`

Dependency detection happens at parse: the cartesian builder
walks each child's source expression for references to earlier
clauses' coordinate names. If any reference is found, the
cartesian is tagged `dependent` and the metadata propagation
short-circuits to `None`. This is the only place where
metadata propagation consults child-internal information beyond
their published metadata bundles — and it does so once at
construction, not in a guard predicate.

`zip(c1, ..., cN, mode)` (children must all be discrete per V7):
- cardinality: per §6.1
- index_addressable: `Some(Lockstep { length: |c| })` for Strict/Truncate when every child is addressable; `Some(Modular { axis_sizes })` for Cycle when at least one child is bounded; else `None`
- natural_order: `Lockstep`
- materialization: `Streaming` for Strict/Truncate; `BoundedBarrier { working_set_size: Σ non-longest child cardinalities }` for Cycle

`union(c1, ..., cN)` (children must all be discrete per V9):
- cardinality: sum per §6.1
- index_addressable: `Some(Concatenation { segment_sizes })` if every child is addressable; else `None`
- natural_order: `Sequential`
- materialization: `Streaming` (one child active at a time)

`filter(c, predicate)`:
- cardinality:
  - `c.cardinality == Bounded(n)` or `BoundedAtMost(n)` → `BoundedAtMost(n)`
  - `c.cardinality == Unbounded` → `Unbounded`
  - `c.cardinality == Continuous { interval, measure }` or `ContinuousAtMost` → `ContinuousAtMost { interval, measure_at_most: c.measure }`
- index_addressable: **`None`** — filter destroys the bijection; the surviving-tuple index isn't a closed-form function of the original index without predicate evaluation
- natural_order: inherited from `c`
- materialization: `c.materialization`

`order(c, Lex, t)`:
- cardinality: per §6.1
- index_addressable: inherited from `c` (Lex doesn't reshape the index space)
- natural_order: `Lex`
- materialization: `c.materialization` (counter wrapper at most)

`order(c, strategy, t)` with strategy ≠ Lex:
- cardinality: per §6.1 — note that `order(Continuous, sampling-strategy, Some(n))` produces `Bounded(n)` (V8's discharge mechanism: sampling materializes a continuous measure into n discrete points)
- index_addressable: **`None`** at the AST level. R2 (§10.2) rewrites this node into an `indexed_order` IR opcode that *is* index-addressable through the strategy's draw function, but the AST-level metadata stops here. If a parent operator chains over this output, it sees `None` and falls back to streaming consumption.
- natural_order: `Strategy(strategy_name)`
- materialization: `BoundedBarrier { working_set_size: strategy.working_set_for(c.index_addressable, t) }` per §6.3's strategy-specific sizing. For Continuous input + sampling strategy + `Some(n)`, the working set is O(n) — the n drawn sample points, not the (uncountable) input measure.

#### 10.7.3 Closure property

Every propagation rule terminates in O(N-arity) time per node
with no recursion beyond the bottom-up walk that built the
children. The whole algebra is a monoid: metadata composes
under constructor composition, and there is no metadata field
whose value depends on an analysis that might fail, time out,
or be partial.

A consequence: the optimizer (§10.6's properties) inherits
decidability from the metadata algebra. Termination, idempotence,
and bounds-improvement all reduce to "each R-rule either
strictly decreases a metadata-derived measure or leaves the AST
unchanged," and the measure is a finite tuple of metadata
values.

#### 10.7.4 How each R-rule consults metadata

| Rule | Guard |
|---|---|
| R0a (identity elimination) | structural — match one of the I1–I5 shapes; safety from §4.2 identity axioms; metadata reread after firing |
| R0b (associativity flattening) | structural — `node.op ∈ {Union, Cartesian} && some child has the same op`; safety from A1, A2; metadata reread after firing |
| R1 (`order(Lex)` is a counter wrapper) | `node.op == Order(Lex, _) && child.natural_order == Lex` |
| R2 (push-down to `indexed_order`) | `node.op == Order(strategy, Some(n)) && child.index_addressable.is_some() && strategy.has_closed_form_for(child.index_addressable)` |
| R3 (Lex / filter commute, untruncated) | `node.op == Order(Lex, None) && child.op == Filter` — structural; safety from N2 |
| R4 (filter distributes over union) | `node.op == Filter && child.op == Union` — structural; safety from D1 + V2 |
| R5 (per-axis filter pushdown) | `node.op == Filter && child.op == Cartesian` (structural) + `PredicateInfo.factorization == PerAxis(_)` from the predicate analyzer (§10.9). Metadata is not consulted. |
| R6 (filter chain folding) | structural; safety from F1 |
| R7 (order chain folding) | `node.op == Order && child.op == Order && child.truncation == None` |

R0a, R0b, R3, R4, R6, and R7 are structural patterns whose
safety is guaranteed by §4.2 identities or §7's equivalences.
R1 and R2 are pure metadata predicates. R5 is structural for
the AST shape (filter wrapping cartesian) and consults the
predicate analyzer (§10.9) for the factorization decision — no
metadata field is read.

#### 10.7.5 Non-goals (what stays OUT of metadata)

- **Predicate shape.** Whether `{a} == K` factorizes is a
  property of a Polydat expression, not of the comprehension AST.
  Predicate analysis lives in a separate analyzer (§10.9) and
  is consumed by R5; its results are never stored in node
  metadata.
- **Cost models.** Rules fire unconditionally per §10.2; there
  is no cost-comparison logic peeking into metadata to decide.
  If two rules apply, they apply in fixed priority order.
- **Source-evaluation timing.** Whether a clause's source is
  eager- or lazy-evaluated at runtime is a PolyStreamer
  concern (SRD-78). Metadata records *declared* cardinality,
  not evaluation state.
- **User-defined extensions.** `IndexFn`, `NaturalOrder`,
  `Materialization`, and `StrategyName` are closed enums. New
  values land as coordinated type extensions, not registration
  points. This is what keeps the algebra closed.

#### 10.7.6 The `EvaluatedSource` contract

The single surface every consumer (IR interpreter, strategies,
V4) uses to read a source's enumerated form. Produced by
`Source::evaluate(Option<&Context>)`.

```text
struct EvaluatedSource {
  /// Concrete typed values the source dispenses, in
  /// declaration / enumeration order.
  values: Vec<polydat::node::Value>,

  /// Cardinality — same enum as §6.1's CardinalityClass, but
  /// `Bounded(values.len())` for any successfully-evaluated
  /// discrete source. `Continuous` retained for distribution-
  /// like sources awaiting an enclosing sampler (V8).
  cardinality: CardinalityClass,

  /// Closed-form addressing scheme — same enum as §10.7.1's
  /// IndexFn. For evaluated discrete sources this is
  /// always `Lattice { axis_sizes: [values.len()] }` at the
  /// clause level; combinators compose it per §10.7.2's
  /// rules.
  index_fn: Option<IndexFn>,
}

enum EvalError {
  /// Source's eval class is context-required, but
  /// evaluate(None) was called. Caller is expected to
  /// supply a kernel context.
  NeedsContext,
  /// Source evaluation against the provided context
  /// failed (interpolation error, eval_const_expr
  /// failure, registry-unknown generator, etc.).
  EvalFailed { spec_text: String, reason: String },
}

trait Source {
  fn eval_class(&self) -> EvalClass;
  fn evaluate(&self, ctx: Option<&Context>) -> Result<EvaluatedSource, EvalError>;
}

enum EvalClass { Static, ContextRequired, Distribution }
```

Calling `evaluate(None)` on a `Static` source always succeeds
(or surfaces a polydat bug). Calling `evaluate(None)` on a
`ContextRequired` source always returns `NeedsContext` —
the type signature is *honest* about whether context is needed.
The runtime evaluator supplies a context when needed.

`Distribution` sources (`Source::Distribution { … }`) evaluate
only inside the sampling discharge of an enclosing
`Order(_, sampling-strategy, Some(n))` — the strategy
produces n drawn points which become the EvaluatedSource's
`values`. Bare Distribution clauses (no enclosing sampler) are
V8-rejected at compile time.

#### 10.7.7 Built-in generator registry

The polydat-owned set of generators whose `evaluate(None)`
succeeds without a kernel context. These move from
`ContextRequired` to `Static` for planning purposes.

| Generator | Signature | Cardinality |
|---|---|---|
| `range(lo, hi)` | int literals | `⌈(hi-lo)⌉` |
| `range(lo, hi, step)` | int literals | `⌈(hi-lo)/step⌉` |
| `fib(n)` | int literal | `n` |
| `pow2(n)` | int literal | `n` |
| `linear_steps(n)` | int literal | `n` |
| `geometric(n, base, ratio)` | int + numeric literals | `n` |
| `concat(s₁, s₂, …, sₖ)` | each sᵢ is itself a registered generator or static source | `Σᵢ |sᵢ|` |
| `partitions("linear:N")` | literal spec | `N` |
| `partitions("hash:N")` | literal spec | `N` |
| `subdivide(lo, hi, n)` | numeric literals | `n` |
| `bucket(...)`, `concat_seq(...)`, `interval_seq(...)` | literal args | per definition |

A generator is "registry-recognized" when (a) its name matches
a registry entry and (b) its argument expressions are all
literal-resolvable without context (recursively for `concat`).
Mixed cases (`concat({workload_param}, fib(8))`) are
`ContextRequired` — the recursion bottoms out at a non-static
piece.

Workload authors do not interact with the registry directly;
it's polydat-internal. Registry completeness is a polydat
quality target — every built-in generator polydat ships should
have a registry entry. Generators *outside* the registry
(notably the activity-side or adapter-defined ones) remain
context-required.

#### 10.7.8 Strategy invocation contract

Strategies (§3.6) implement one method:

```text
trait Strategy {
  fn apply(
    &self,
    evaluated: &EvaluatedSource,
    truncation: Option<u64>,
  ) -> Result<Vec<Tuple>, StrategyError>;
}
```

The strategy queries `evaluated.index_fn` and
`evaluated.values` to compute its permutation. The earlier
`naive_apply` / `indexed_apply` split retires — there's one
method, and the question "does the strategy have lattice
metadata to work with?" is answered by inspecting the
`EvaluatedSource` it receives.

**§V4 enforcement timing.** V4 ("non-`Lex` strategies require
the input's `IndexFn` to be non-`None`") fires at
strategy-invocation time, against the `EvaluatedSource`. The
compile-time IR planner may *additionally* fire V4 early as a
usability nicety — when an AST's sources are all statically
evaluable, the planner runs evaluation and surfaces V4
failures at compile time. For ASTs with context-required
sources, the early fire is skipped; runtime fire is the load-
bearing one. Either way, V4 is the same axiom; only the
*when* changes.

#### 10.7.9 Why this matters

The R-rules become pure pattern matches over metadata + AST
shape — no global analysis, no out-of-tree consultations, no
late-binding callbacks. The optimizer's contract (§10.6) is a
direct consequence of monoidicity: each rule strictly decreases
a metadata-derived measure (`BoundedBarrier.working_set_size`,
or transition from `BoundedBarrier` to `Streaming` via R1/R2)
without inflating any other field.

The metadata algebra also serves as the **public introspection
surface** alongside the immutable IR (§9.1). A consumer asking
"is this comprehension index-addressable?" or "what is the
worst-case working set?" reads metadata, not IR. The two
surfaces together let external tooling reason about
comprehension cost without recompiling.

The eval-class partitioning (§10.7.0) plus the registry
(§10.7.7) keep the static-evaluable subset broad without
introducing a static / runtime semantic split: it's one
metadata algebra, run twice for context-required cases (once
optimistically at compile time, once definitively at strategy
invocation). The runtime second-fire produces the same
metadata bundle the static path would have, only with values
the planner didn't know yet.

### 10.8 What the optimizer doesn't do

- It does NOT change the user-authored semantic. N1 holds:
  filter-then-order and order-then-filter remain distinct.
- It does NOT eagerly materialize sources. PUSH_CLAUSE stays
  a lazy stream producer per §9.1.
- It does NOT inline cross-AST sharing. The §8.3 derived-
  streamers case (`base`, `sampled`, `boundary`) compiles each
  AST independently; sharing across PolyStreamers is SRD-78's
  job, not the optimizer's.
- It does NOT add new operators to the IR. The eight §9.1
  opcodes are sufficient; the optimizer's "indexed_halton"
  family is an `ORDER_MATERIALIZE` strategy parameterization,
  not a new opcode.
- It does NOT consult anything beyond the metadata bundle
  (§10.7) and the predicate analyzer's structured output (§10.9).
  No ad-hoc tree walks, no cross-cutting analyses inlined.

### 10.9 Predicate analyzer

R5 (per-axis filter pushdown) and any future rule that depends
on predicate shape need a structured view of the Polydat expression
that `filter`'s predicate carries. The **predicate analyzer**
is the single component that provides that view. It is
specified separately from the metadata algebra (§10.7) because
predicates are Polydat expressions, not comprehension AST nodes —
they live in a different value space and deserve their own
analysis surface.

The analyzer takes `(GkExpr, CoordSet) → PredicateInfo` and
operates on one predicate at a time. Its output feeds R5 and
the predicate-aware rules in the deferred R8–R10 set. The
whole-AST reducibility component that drives the optimizer's
rewrite loop is a separate analyzer specified in §10.10; the
two analyzers share the predicate-shape information but have
distinct inputs, outputs, and scopes.

This section pins down what the predicate analyzer accepts,
what it produces, and which properties it asserts. Everything
here is the planned design; the analyzer ships alongside R5.

#### 10.9.1 Scope

**In scope:**

- Polydat boolean expressions used as `filter` predicates.
- Coordinate references `{name}` where `name` is a coordinate
  produced by the wrapped comprehension's tuple shape.
- Constants, named Polydat kernel inputs (bound at scope-init),
  arithmetic and comparison operators, boolean combinators
  (`&&`, `||`, `!`), and the small fixed set of Polydat builtins
  that produce deterministic scalar values.

**Out of scope:**

- Non-deterministic Polydat expressions (e.g. PRNG draws inside the
  predicate). Detected by inspecting the expression's GK
  kernel for `requires_seed` flags; if present, the analyzer
  returns `Opaque` and no R-rule that needs structured info
  fires. The predicate is still evaluated correctly per-tuple
  at runtime — only the *optimizer* skips push-down.
- Cross-tuple state. Predicates that depend on
  previously-emitted tuples (none exist today, but the
  analyzer should reject them rather than silently accept).
- Side-effecting expressions. Same rejection path as
  non-deterministic.
- **Continuous-coord predicates.** Any predicate whose `coords`
  set includes one or more continuous-cardinality axes is
  marked `Opaque` with a dedicated `OpaqueReason::Continuous`
  tag (see §10.9.3). Continuous-space predicate analysis is a
  separate problem (interval arithmetic, measure-preserving
  factorization) that the initial cut deliberately defers.
  The predicate still runs correctly per-sample at runtime —
  only the optimizer skips push-down rules whose correctness
  would depend on continuous-aware factorization.

#### 10.9.2 What the analyzer wraps

The analyzer is invoked as a function:

```text
analyze(predicate: &GkExpr, coords: &CoordSet) -> PredicateInfo
```

`coords` is the coordinate name set of the comprehension the
predicate filters (e.g. `{k, limit}` for a two-axis cartesian).
The analyzer never sees the full comprehension AST — only its
coordinate name set. This isolation is intentional: the
analyzer's job is "what does this expression say *about these
names*?", not "what does this expression do in context?"

`PredicateInfo` is a pure data record with the assertions
below. It is the only artifact R5 (and any future predicate-
shape-aware rule) reads.

#### 10.9.3 Assertable properties

For every predicate the analyzer wraps, `PredicateInfo` carries:

```text
PredicateInfo {
  factorization:      Factorization,
  monotonicity:       PerAxisMap<Monotonicity>,
  range_constraint:   PerAxisMap<RangeConstraint>,
  determinism:        Determinism,
  coords_referenced:  CoordSet,
}

enum Factorization {
  PerAxis(PerAxisMap<GkExpr>),       // p ≡ p1({a}) && p2({b}) && ...
  Conjunctive(Vec<GkExpr>),          // p ≡ q1 && q2 && ... where each qi may still cross-cut
  Disjunctive(Vec<GkExpr>),          // p ≡ q1 || q2 || ...
  Opaque(OpaqueReason),              // analyzer can't structurally decompose
}

enum OpaqueReason {
  UnknownPattern,                    // shape not in §10.9.5 recognizer catalog
  NonDeterministic,                  // requires_seed flag, PRNG draws, etc.
  CrossTupleState,                   // depends on previously-emitted tuples
  SideEffecting,                     // side-effecting Polydat expression
  Continuous,                        // predicate references continuous-cardinality coord(s)
}

enum Monotonicity {
  Increasing,                        // larger axis value → predicate stays true once true
  Decreasing,                        // larger axis value → predicate stays true once true going down
  None,
}

enum RangeConstraint {
  Bounded { lo: Option<Value>, hi: Option<Value>, inclusive: (bool, bool) },
  Discrete(Vec<Value>),              // p ≡ {a} ∈ {1, 7, 42}
  None,
}

enum Determinism {
  Deterministic,                     // same coords → same boolean, always
  Opaque,                            // any of the out-of-scope conditions
}
```

The five properties are independent assertions — a predicate
may have `PerAxis` factorization but no monotonicity, or have
strong monotonicity on each axis but be `Opaque` overall (e.g.
`{a} < f({b})` where `f` is a non-trivial Polydat kernel call).

#### 10.9.4 Goals and correctness contract

The analyzer is a function `(GkExpr, CoordSet) → PredicateInfo`
with these properties:

1. **Sound.** Every assertion in `PredicateInfo` is *true* of
   the predicate. If `factorization` says `PerAxis(p1, p2)`,
   then for every tuple `(a, b)` the original predicate
   evaluates to exactly `p1(a) && p2(b)`. Soundness is
   load-bearing for R5: the optimizer rewrites based on
   asserted facts and would emit incorrect output if any
   assertion were false.
2. **Conservatively incomplete.** The analyzer is allowed (and
   expected) to under-report. A predicate that *is*
   factorizable but uses an expression the analyzer doesn't
   recognize may return `Opaque`. Missing an optimization is
   acceptable; asserting a false property is not.
3. **Total.** Every well-formed Polydat boolean expression produces
   a `PredicateInfo`. The trivial bundle (everything `None` /
   `Opaque`) is the worst case but never a failure.
4. **Deterministic.** Same `(predicate, coords)` always
   produces the same `PredicateInfo`. The analyzer is itself
   referentially transparent.
5. **Constant-time per node.** The analyzer walks the GK
   expression tree once, with constant work per node. No
   fixed-point iteration, no SMT solver, no expression
   rewriting.

Property 1 (soundness) is verified by property-based tests in
the analyzer's own crate: for every `PredicateInfo` field
assertion, generate random tuples in the coord space, evaluate
both the original predicate and the asserted decomposition,
require equality. The test corpus covers each `Factorization`
variant, each `Monotonicity` direction, and each
`RangeConstraint` shape.

Property 2 (conservative incompleteness) is the design
principle that keeps the analyzer simple and the metadata
algebra closed. Anything the analyzer can't structurally
recognize is `Opaque`; nothing falls back to "run the predicate
to see what happens."

#### 10.9.5 Recognized patterns (initial set)

The analyzer ships with recognizers for a small, fixed pattern
catalog. The catalog grows by coordinated extension (new
recognizer = one new function + property tests); workload-
shape pressure drives which patterns get added.

| Pattern | Factorization | Monotonicity | Range |
|---|---|---|---|
| `{a} OP K` for OP ∈ {==, !=, <, <=, >, >=} | `PerAxis({a}: <expr>)` | direction-of-OP on `a` | bounded for `<`/`<=`/`>`/`>=`; discrete for `==` |
| `{a} OP {b}` (cross-axis) | `Conjunctive([self])` | None | None |
| `p1 && p2` recursively | factorization of children, joined by coord-disjointness | per-axis intersection | per-axis intersection |
| `p1 \|\| p2` recursively | `Disjunctive(children)` if each is `Conjunctive`/`PerAxis`; else `Opaque` | None | per-axis union |
| `!p` | inverted factorization where invertible; else `Opaque` | inverted monotonicity | inverted range |
| `K1 <= {a} && {a} <= K2` | `PerAxis({a}: K1..=K2)` (folded) | None | `Bounded { lo: K1, hi: K2, inclusive: (true, true) }` |
| `{a} in [K1, K2, K3]` (GK `in` builtin) | `PerAxis({a}: in)` | None | `Discrete([K1, K2, K3])` |

Patterns NOT in the initial set return `Opaque` (or partial
factorization where safe). Examples: predicates that call
user-defined Polydat kernels, predicates over computed coordinates
that haven't been simplified, predicates with `if(...)`
branches.

#### 10.9.6 What R5 does with `PredicateInfo`

R5's full guard, expanded:

```text
fire R5 on filter(cartesian(c1, ..., cN), predicate) iff
    let info = analyze(predicate, coords_of(cartesian));
    info.determinism == Deterministic &&
    info.factorization matches PerAxis(per_axis_preds)
```

When R5 fires, each per-axis predicate becomes a `filter` node
wrapping its child, and the outer filter disappears:

```text
filter(cartesian(c1, ..., cN), p) →
    cartesian(c1, ..., filter(ci, per_axis_preds[name_of(ci)]), ..., cN)
```

If `factorization` is `Conjunctive` rather than `PerAxis`, R5
fires partially: each `qi` that turns out to be `PerAxis` on
its own is pushed down, and the residual `qi`s reassemble into
an outer filter. The push-down is monotonic in the analyzer's
information (more `PerAxis` recognition → strictly more
pushdown), never inflating.

If `factorization` is `Opaque` or `Disjunctive` (when not all
disjuncts are themselves `PerAxis`), R5 does not fire. The
predicate stays at the outer filter and runs per-tuple as
before. This is the "conservatively incomplete" path: correct,
but no working-set shrinkage.

#### 10.9.7 Future rules built on this surface

The same `PredicateInfo` enables additional optimizer rules
without re-analyzing predicates. Each ships as a coordinated
addition (new R-rule + property tests + recognizer extensions
if needed):

- **R8 (range-narrowing into cartesian).** When a per-axis
  predicate is a `RangeConstraint::Bounded`, rewrite the child
  cartesian's clause-source to the narrowed range directly.
  `filter(cartesian(clause(a, 1..1000)), {a} >= 500)` →
  `cartesian(clause(a, 500..1000))`. The filter disappears
  entirely; the source's `BoundedInt` clause-source narrows
  to the constrained range. Working set shrinks at the
  source, not just at the cartesian.
- **R9 (discrete-set substitution).** When a per-axis
  predicate is `RangeConstraint::Discrete`, rewrite the child
  clause-source to a literal list of the discrete values.
  `filter(cartesian(clause(a, 1..1_000_000)), {a} in [7, 42])`
  → `cartesian(clause(a, [7, 42]))`. Same shape as R8.
- **R10 (monotonic-cutoff truncation).** When a per-axis
  predicate is `Monotonicity::Increasing` and the cartesian
  enumerates in Lex order, the predicate's first false value
  on that axis can short-circuit the axis's enumeration.
  Useful for `where {a} < K` patterns that today require
  evaluating the predicate against every later value.

These rules are deliberately deferred. They land when workload
pressure justifies them; the analyzer's `PredicateInfo` is
already shaped to carry the assertions they need.

#### 10.9.8 What the predicate analyzer is NOT (per-predicate scope)

- **Not an SMT solver.** No symbolic reasoning, no quantifier
  handling, no inequality chains beyond the recognizer
  patterns. Workloads needing that complexity should restate
  the comprehension explicitly (split into per-sub-space
  unions).
- **Not a constant folder.** Polydat's expression layer already
  folds constant subexpressions before the analyzer sees the
  predicate. The analyzer assumes folded input.
- **Not coupled to the comprehension AST** at the per-
  predicate layer. The §10.9.1 – §10.9.7 surface operates on
  `(GkExpr, CoordSet)` only; the comprehension's structure is
  the reducibility analyzer's concern (§10.10).
- **Not extensible by callback.** Recognizer patterns live in
  a closed Rust enum, mirroring §10.7's design discipline for
  the metadata algebra. A workload that needs a new pattern
  upstreams a recognizer; it doesn't register one at runtime.

### 10.10 Reducibility analyzer

The reducibility analyzer is the second analyzer in §10's
toolchain, distinct from §10.9's predicate analyzer. It operates
on whole AST nodes (not predicates), takes the metadata bundle
(§10.7) as its primary input, and produces the
`ReducibilityFinding` values that drive the optimizer's rewrite
loop (§10.6).

The two analyzers share information — the reducibility analyzer
consults the predicate analyzer's `PredicateInfo` when its rules
need predicate-shape facts (R5 is the current case) — but their
scopes are disjoint: the predicate analyzer never sees AST
context, and the reducibility analyzer never reaches inside a
predicate.

The reducibility analyzer answers a single, sharp question for
any AST `C`:

> Given `C` and the propagated metadata `M(C)` (per §10.7),
> does there exist an AST `C'` — built from the six §3
> constructors — such that `C'` produces the same dispense
> sequence as `C` and `cost(C') < cost(C)` in compute order,
> memory order, or both?

If yes, the analyzer returns a `ReducibilityFinding` carrying
the witness `C'` and the strict-improvement vector. If no, it
returns the empty finding. The optimizer (§10.6) is a thin loop
that asks the analyzer for findings on the current AST, applies
the first finding, and re-asks until the empty finding comes
back.

#### 10.10.1 Inputs

```text
analyze_reducibility(c: &Ast, m: &Metadata) -> ReducibilityFinding
```

- `c`: the AST node under consideration (and, recursively, its
  children).
- `m`: the metadata bundle for `c` propagated per §10.7. The
  analyzer reads `m` exhaustively but **never reads `c`
  beyond what metadata surfaces** — that is, no peeking into
  Polydat expression internals, source-expression internals, or
  any field of `c` that §10.7's propagation rules did not
  publish into metadata. For predicate-shape facts the
  analyzer consults the predicate analyzer (§10.9) and treats
  the returned `PredicateInfo` as part of its input.

This discipline is what keeps the reducibility analysis closed
over the metadata algebra: any property that influences
reducibility decisions must be surfaced into metadata or
`PredicateInfo` first. New reducibility rules don't get an
escape hatch into AST internals.

#### 10.10.2 Output

```text
ReducibilityFinding {
  reduction:   Option<Reduction>,
  improvement: ComplexityDelta,
}

enum Reduction {
  Replace { with: Ast },                    // C → C', whole-tree swap
  Rewrite { rule: RuleId, witness: Ast },   // tagged with R-rule
}

struct ComplexityDelta {
  compute_order: Ordering,    // Less means C' is asymptotically cheaper
  memory_order:  Ordering,    // same
  rationale:     &'static str,
}
```

A non-empty finding requires `improvement` to be strictly
better in at least one dimension and non-worse in the other.
Findings where both dimensions are `Equal` (no asymptotic
change) are not produced — the optimizer would loop on them.

The `witness` field carries the proposed replacement AST. The
optimizer applies it directly; there is no separate "compile
the finding" step. This is what lets the analyzer run **before
stack-machine materialization** — its output is itself an AST,
not an IR fragment.

#### 10.10.3 The reducibility catalog

The R-rules in §10.2 are the **enumerated reducibility
catalog**. Each rule, when its guard fires, is the analyzer
producing a `ReducibilityFinding` whose `Reduction::Rewrite`
points to the rule's identity and whose `witness` is the
rewritten AST. The catalog grows by adding R-rules; the
analyzer's structure does not change.

The improvement vector per rule:

| Rule | Compute Δ | Memory Δ | Why |
|---|---|---|---|
| R0a (identity elimination) | Less (skips a no-op operator per pull) | Equal | Removes structurally redundant nodes; AST shrinks |
| R0b (associativity flattening) | Less (one n-ary cursor walk replaces nested 2-ary walks) | Equal | Removes structurally redundant nesting; AST shrinks |
| R1 (`order(Lex)` → counter) | Equal | Less (barrier → streaming) | Removes a non-Lex materialization that wasn't actually needed |
| R2 (push-down to `indexed_order`) | Less or Equal | Less | Barrier working-set shrinks from O(input) to O(output) |
| R3 (Lex/filter commute) | Equal | Equal in worst case, Less when downstream barrier exists | Reorders work; pure equivalence under §7 N2 |
| R4 (filter distributes over union) | Equal | Less when downstream barriers exist | Each child sees a smaller filtered input |
| R5 (per-axis filter pushdown) | Less | Less | Cartesian enumerates fewer rows |
| R6 (filter chain folding) | Less (one eval not two) | Equal | Pure compute reduction |
| R7 (order chain folding) | Less | Less | Removes redundant interior materialization |

R0a and R0b are the canonicalization rules; they always
strictly decrease AST node count and never inflate metadata, so
their `improvement` vector is non-trivial even though the
runtime semantics are unchanged at the dispense interface. They
are the lower-bound case of "strictly improving" — the
improvement is in AST size, not in runtime cost — but they earn
their place in the catalog because downstream rules' guards are
expressed against the canonical form.

R3's "Equal compute" case is the rule's lower bound; in
practice the analyzer fires R3 only when the downstream context
shows a barrier whose input would shrink, so the realized
improvement is `Less` in memory. Rules that show `Equal` in
both columns under any context never fire — see §10.6's
"bounds-improving" property.

§5.8's degenerate-composition catalog overlaps with R0a: every
"singleton variant" entry in §5.8 (single-element clauses
chained, `zip([c], _)`, `cartesian(c)`, `union(c)`) is a form
that R0a elides at the optimizer. §5.8 surfaces the warning at
parse so the author sees the redundancy in their source; R0a
ensures the runtime never pays for it. The two layers are
complementary, not duplicate — warnings teach the author,
rewrites keep the IR clean.

#### 10.10.4 Correctness contract

For every `ReducibilityFinding` the analyzer returns:

1. **Dispense-sequence preserving (§9.2).** The witness AST
   `C'` produces the identical tuple sequence as `C` when
   compiled to IR and executed. This is enforced by:
   - For catalog rules: each R-rule is an instance of a §7
     equivalence (A/I/F/D/O/N axioms), which is itself
     defined as dispense-sequence-preserving.
   - For the catalog as a whole: a property-based test that
     generates random ASTs, applies a non-empty finding,
     compiles both sides, and asserts identical dispense
     sequences over a configurable tuple prefix.
2. **Strictly improving.** `improvement.compute_order` and
   `improvement.memory_order` together strictly dominate the
   pre-rewrite cost (one strictly Less, the other ≤). No
   finding is produced for break-even rewrites.
3. **Metadata-closed.** The analyzer's decision uses only
   `c`'s shape (operator + children's recursive shapes),
   `m`'s fields, and `PredicateInfo` for any contained
   predicate. No reach into AST internals; no consultation of
   runtime state, evaluation environment, or kernel internals
   beyond what metadata published.
4. **Pre-materialization.** The analyzer runs on the AST,
   not on the IR. The IR is the *result* of applying the
   findings followed by compilation (§9.1). This ordering is
   load-bearing: rewriting an AST is cheap and structural;
   rewriting an IR after compilation would require re-running
   the metadata propagation through a different formalism.
5. **Total and terminating.** Every well-formed AST produces
   a `ReducibilityFinding`; the empty finding is the worst
   case but never a failure. The analyzer's per-AST work is
   bounded by node count × catalog size. The optimizer's
   loop terminates by §10.6.3 (each applied finding strictly
   decreases the metadata-derived measure).

#### 10.10.5 Composing findings across the AST

The analyzer is invoked at every AST node during a bottom-up
walk; it emits findings local to that node (with the rest of
the AST as context via metadata). The optimizer applies
findings in fixed priority order: **R0a → R0b → R1 → R2 → R3 →
R4 → R5 → R6 → R7**, then the deferred rules (R8, R9, R10) in
their landing order. R0a and R0b run first and to a fixed point
before any other rule fires — this puts the AST into canonical
form so the structural guards on R1 – R7 don't have to
enumerate identity-equivalent variants. After applying any
finding, the optimizer re-propagates metadata (or, equivalently,
recomputes `m` for the affected subtree) and re-asks the
analyzer until the empty finding comes back.

The order matters for confluence but not for correctness: each
individual finding is dispense-sequence-preserving, so any
sequence of findings is also dispense-sequence-preserving by
composition. Different application orders may reach different
fixed points (different `C'` results), but every reached fixed
point is sound and strictly better than the input.

The optimizer's idempotence (§10.6.2) follows: re-running the
analyzer on a fixed point produces the empty finding (no
strict improvement available), so `optimize(optimize(C)) ≡
optimize(C)`.

#### 10.10.6 What the reducibility analyzer is NOT

- **Not a cost-comparison engine over IR variants.** It does
  not compile two ASTs to IR and compare. It compares
  asymptotic complexity using the closed-form bounds the
  metadata algebra already published. Numeric cost estimation
  is downstream (potentially in tooling that consumes the
  immutable IR per §9.1) and never feeds back into
  reducibility decisions.
- **Not an AST synthesizer.** It does not invent new ASTs from
  whole cloth. Every witness is the application of a catalog
  rule whose rewrite shape is published in §10.2. If a
  reduction is possible but not in the catalog, the analyzer
  returns the empty finding; the reduction lands as a new
  R-rule entry in a future spec push, not as an ad-hoc
  synthesis at runtime.
- **Not a recipient of runtime feedback.** It does not adapt
  based on observed dispense behavior, source values, or
  predicate selectivity. The findings are static facts about
  the AST under the metadata algebra; they are valid at
  compile time without any runtime probing.

---

## 11. Examples gallery

Each example shows the surface form, the desugared AST, and
notable properties (cardinality, footprint, validity hits).

### 11.1 Single Cartesian

```text
for k in 1..10, profile in {profiles}
```

AST: `cartesian(clause(k, 1..10), clause(profile, {profiles}))`

- Cardinality: `Bounded(10 × len(profiles))`.
- Footprint: O(1) per cursor (sources stream per §3.1); the
  cartesian holds two cursors and one in-flight output tuple.
  Total per-pull steady state: O(1). No materialization
  barriers in the AST.
- IR: two `PUSH_CLAUSE` + `CARTESIAN(2)` + `DISPENSE`.
- Metadata: `cardinality = Bounded(10 × len(profiles))`;
  `index_addressable = Some(Lattice { axis_sizes: [10,
  len(profiles)] })`; `natural_order = Lex`;
  `materialization = Streaming`.

### 11.2 Cartesian with filter and order

```text
for k in 1..100, limit in 1..100
  where {k} * {limit} <= 1000
  order extrema/5
```

AST: `order(filter(cartesian(clause(k, 1..100), clause(limit, 1..100)), "{k} * {limit} <= 1000"), Extrema, Some(5))`

- Cardinality: `BoundedAtMost(5)` (truncation cap + filter
  survival).
- Validity: V4 passes — Extrema requires a non-`None` Lattice
  with ≥2 axes; V5's look-through rule lets the filter sit
  between Extrema and its cartesian input without breaking the
  check. The strategy reasons about original lattice positions.
- Footprint (naïve, pre-optimizer): O(1) per cursor for sources;
  the filter holds no state; `ORDER_MATERIALIZE(Extrema, 5)`
  inspects all surviving tuples to find extrema, so the barrier
  holds up to 10,000 candidates in the worst case.
- Footprint (post-R2): Extrema over a 2-axis Lattice has a
  closed-form push-down — enumerate the 2² = 4 lattice corners,
  apply the strategy's distance metric, emit the top 5
  (truncates to 4 since only 4 corners exist). Working set:
  O(2^N · log 2^N) = O(N · 2^N) = O(2 · 4) = ~8 cells,
  independent of input size. The filter still runs per
  emitted-candidate tuple (V5 transparency), but the candidate
  set is the small corner set, not the 10,000-element filtered
  survivors.
- IR (after R2): `PUSH_CLAUSE k` + `PUSH_CLAUSE limit` +
  `CARTESIAN(2)` + `FILTER("{k} * {limit} <= 1000")` +
  `ORDER_MATERIALIZE(IndexedExtrema, 5)` + `DISPENSE`.

### 11.3 Union of differently-modified sub-spaces

```text
for [
    for k in 10, limit in 1..50 where {limit} > 10,
    for k in 100, limit in 1..500 order halton/20,
]
```

AST: `union(filter(cartesian(clause(k, 10), clause(limit, 1..50)), "{limit} > 10"), order(cartesian(clause(k, 100), clause(limit, 1..500)), Halton, Some(20)))`

- Cardinality: `BoundedAtMost(40 + 20)` = up to 60 tuples.
- Validity: per-sub-space `where` and `order` are independent
  operators inside the union's children. V4 passes for the
  inner Halton — Halton accepts any non-`None` `IndexFn` and
  the wrapped cartesian publishes `Lattice`. V5 not invoked
  (no filter between the inner halton and its cartesian).
- Footprint (naïve): O(1) per cursor for sources; the first
  sub-space's filter holds no state; the second sub-space's
  `ORDER_MATERIALIZE(Halton, 20)` would inspect all 500
  cartesian tuples in the naïve form.
- Footprint (post-R2): Halton over a 2-axis Lattice has a
  closed-form push-down. The inner halton/20 emits 20 Halton
  draws over the `[0, 100) × [0, 500)` index space, looking
  each up against the cartesian's enumeration. Working set:
  20 indices + per-axis cursors. The union activates one
  sub-space at a time, so the first sub-space's state is
  released before the second begins.
- IR (after R2): `PUSH_CLAUSE k` (=10) + `PUSH_CLAUSE limit`
  (1..50) + `CARTESIAN(2)` + `FILTER("{limit} > 10")` + sub-
  space-A end → `PUSH_CLAUSE k` (=100) + `PUSH_CLAUSE limit`
  (1..500) + `CARTESIAN(2)` + `ORDER_MATERIALIZE(IndexedHalton,
  20)` + sub-space-B end → `UNION(2)` + `DISPENSE`.

### 11.4 Union with outer reordering

```text
for [
    for k in 10, limit in 1..50,
    for k in 100, limit in 1..50,
] order lex/30
```

AST: `order(union(cartesian(clause(k, 10), clause(limit, 1..50)), cartesian(clause(k, 100), clause(limit, 1..50))), Lex, Some(30))`

- Cardinality: `Bounded(min(100, 30))` = 30.
- Validity: `Lex` accepts any input including unions per §3.6;
  this form is the union-and-truncate idiom.
- Footprint: O(1) per cursor (sources stream); the union holds
  one active sub-space at a time; Lex with truncation compiles
  to `ORDER_STREAMING` (a counter wrapper, R1) — no barrier.
  Total per-pull steady state: O(1) plus the operator stack.
- IR (after R1): two cartesian sub-trees + `UNION(2)` +
  `ORDER_STREAMING(Lex, 30)` + `DISPENSE`.

### 11.5 Halton over a union (combined index space)

```text
for [
    for k in 10, limit in 1..50,
    for k in 100, limit in 1..50,
] order halton/30
```

AST: `order(union(cartesian(clause(k, 10), clause(limit, 1..50)), cartesian(clause(k, 100), clause(limit, 1..50))), Halton, Some(30))`

- Validity: V4 passes. Halton accepts any non-`None` `IndexFn`
  per §3.6; the union's metadata publishes `IndexFn::Concatenation
  { segment_sizes: [50, 50] }` (each sub-space contributes 50
  tuples). The strategy reasons over the combined 100-element
  index space.
- Cardinality: `Bounded(30)` (truncation).
- Footprint: O(1) per cursor (sources stream); O(30) for the
  Halton draws after R2 push-down. Total per-pull steady state
  ~O(1).
- Per-pull behavior: each Halton draw produces an index in
  `[0, 100)`; the Concatenation index function maps it to either
  `cartesian_0[i]` (when i < 50) or `cartesian_1[i - 50]`
  (when i ≥ 50), and the lookup descends into the chosen
  cartesian's lattice.
- Note: this samples the *concatenated* index space, not each
  sub-space independently. The geometry is heterogeneous —
  Halton spreads 30 points smoothly across `0..100`, which lands
  ≈15 points in each cartesian on average. If per-sub-space
  Halton is the intent (15 Halton-spread points within each
  sub-space's own lattice), author it explicitly per N1's "the
  author's form is the intended semantic":

```text
for [
    for k in 10, limit in 1..50 order halton/15,
    for k in 100, limit in 1..50 order halton/15,
]
```

### 11.6 Filter then order vs order then filter

```text
// Form A — order, then filter
fast_corner := (for k in 1..10, limit in 1..10 order extrema/4)
               where {k} * {limit} > 50

// Form B — filter, then order
corner_of_high := (for k in 1..10, limit in 1..10 where {k} * {limit} > 50)
                  order extrema/4
```

AST A: `filter(order(cartesian(...), Extrema, Some(4)), "{k} * {limit} > 50")`
AST B: `order(filter(cartesian(...), "{k} * {limit} > 50"), Extrema, Some(4))`

- Form A: pick the 4 extrema (corners) of (k, limit), then drop
  those whose product ≤ 50. Could emit 0-4 tuples depending on
  which corners survive.
- Form B: filter to high-product tuples first, then pick the
  4 extrema of *those*. The "corners" are computed relative to
  the surviving set (which still uses original lattice
  positions per V5, but the survivors are a different set).
- Both are valid. They emit different tuples. The user's
  authored form is the intended semantic; N1 says the optimizer
  does NOT rewrite.

### 11.7 Bounded zip

```text
for (x, y) in (1..10, 100..200..10)
```

AST: `zip([clause(x, 1..10), clause(y, 100..200..10)], Strict)`

- Cardinality: `Bounded(10)` (both children cardinality 10, V7
  passes).
- Footprint: O(1) per cursor (sources stream); the zip holds
  one cursor per child and one in-flight output tuple. Total
  per-pull steady state: O(N-arity) = O(2). No barriers.
- Metadata: `index_addressable = Some(Lockstep { length: 10 })`;
  `natural_order = Lockstep`; `materialization = Streaming`.

### 11.8 Cycle zip with one unbounded child

```text
for (cycle, color) in zip_cycle({cycle_stream}, [red, green, blue])
```

AST: `zip([clause(cycle, {cycle_stream}), clause(color, [red, green, blue])], Cycle)`

- Cardinality: `Unbounded` (cycle_stream is unbounded;
  color repeats indefinitely).
- Footprint: O(3) for the colors buffer (cycling requires re-
  emit); O(1) for the unbounded `cycle` stream's per-tuple
  state.
- Validity: V6 satisfied — no materializing order applied to
  this; zip Cycle accepts one unbounded child.

### 11.9 Derived streamers from one base

```text
base       := for k in 1..100, limit in 1..100
sampled    := base order halton/50
boundary   := base where {k} == 1 || {k} == 100 || {limit} == 1 || {limit} == 100
hot_corner := boundary order extrema/1
```

ASTs:
- `base`: `cartesian(clause(k, 1..100), clause(limit, 1..100))`
- `sampled`: `order(<base>, Halton, Some(50))`
- `boundary`: `filter(<base>, "{k} == 1 || {k} == 100 || {limit} == 1 || {limit} == 100")`
- `hot_corner`: `order(<boundary>, Extrema, Some(1))`

Each becomes a distinct `PolyStreamer` per SRD-78. The
compiler MAY share evaluation of `<base>` across the three
derivatives — `<base>` itself streams (sources don't
materialize per §3.1; the cartesian holds two cursors). If a
downstream consumer pulls from multiple derived streamers
simultaneously, the runtime may cache `<base>`'s emitted tuples
or recompute per-streamer; see §14 for the open question on
cross-streamer sharing as a polydat-internal or SRD-78-runtime
concern.

### 11.10 Continuous parameter sweep

```text
for alpha in 0.0..1.0, beta in 0.0..1.0 order halton/100
```

AST: `order(cartesian(clause(alpha, 0.0..1.0), clause(beta, 0.0..1.0)), Halton, Some(100))`

The two continuous coordinates form a 2-D `cartesian`, not a
`zip` — V7 rejects continuous zip per §5, and "Halton over a
2-D box" is the cartesian semantic. The outer `order(_, Halton,
Some(100))` discharges V8 by sampling 100 quasi-random points
from the box.

Hybrid discrete-and-continuous variant — vary `k` over a small
integer set and `theta` over a continuous angle:

```text
for k in [1, 2, 4, 8], theta in 0.0..2*pi order lhs/50
```

AST: `order(cartesian(clause(k, [1, 2, 4, 8]), clause(theta, 0.0..2*pi)), Lhs, Some(50))`

- Cardinality (first form): `Bounded(100)` — V8 discharged by
  the outer `order(_, Halton, Some(100))`; Halton over a
  Continuous 2-D box samples 100 quasi-random points.
- Cardinality (hybrid form): `Bounded(50)` — V8 discharged by
  the outer `order(_, Lhs, Some(50))`; Lhs over a Hybrid
  4-element × continuous-interval space stratifies both axes
  and emits 50 paired samples.
- Validity: V4 passes (Halton/Lhs both accept Continuous and
  Hybrid `IndexFn` per §3.6); V7 not invoked (no zip in either
  AST); V8 passes (both wrap their continuous coordinates in
  an `order(_, _, Some(n))`).
- Footprint: O(1) per cursor (clause sources stream — for
  continuous, the "cursor" is the sampling-strategy's draw
  state, not a buffer of the interval); `ORDER_MATERIALIZE`
  barrier of size O(100) (or O(50)) for the drawn samples
  after R2 push-down. The 2-D continuous interval itself is
  never materialized.
- IR (first form, after R2): `PUSH_CLAUSE alpha (0.0..1.0)` +
  `PUSH_CLAUSE beta (0.0..1.0)` + `CARTESIAN(2)` +
  `ORDER_MATERIALIZE(IndexedHalton, 100)` + `DISPENSE`. The
  IndexedHalton variant draws 100 K-D Halton points in
  `[0,1)^2` and emits each as a `(alpha, beta)` tuple by
  affine mapping the unit square onto the input intervals
  (which are also `[0,1)`, so the mapping is identity).
- Metadata (first form): `cardinality = Continuous {
  intervals: [(0,1), (0,1)], measure: Uniform }` *before*
  the outer order, `Bounded(100)` after; `index_addressable =
  Some(Continuous { ... })` before, `None` at the order's
  output (per §10.7); `natural_order = Lex` before (cartesian's
  natural order), `Strategy(Halton)` after.

This is the canonical continuous-parameter-sweep idiom. Most
real workloads with continuous coordinates take this shape:
declare the parameter ranges as continuous, wrap in a sampling
order with a finite truncation, dispense `Bounded(n)` tuples.
If two continuous coordinates need to be paired in lockstep
rather than swept jointly, see §11.11.

### 11.11 Sample-then-zip (lockstep continuous pairing)

Two continuous coordinates that should advance **in lockstep**
(coordinate-i of A paired with coordinate-i of B for some
externally-determined ordering of i) cannot use direct
`zip(continuous_a, continuous_b)` — V7 (§5) rejects continuous
zip because lockstep needs an integer "i-th element" and
continuous sources have none. The form that does work is
sample-each-first then zip the discrete outputs:

```text
samples_alpha := for alpha in 0.0..1.0 order halton/100
samples_beta  := for beta in 0.0..1.0 order halton/100
paired        := for (alpha, beta) in (samples_alpha, samples_beta)
```

ASTs:
- `samples_alpha`: `order(clause(alpha, 0.0..1.0), Halton, Some(100))` → cardinality `Bounded(100)`
- `samples_beta`: same shape, different binding name
- `paired`: `zip([samples_alpha, samples_beta], Strict)`

- Cardinality (paired): `Bounded(100)` (zip-Strict requires equal cardinalities; both children are Bounded(100) so V7 passes).
- Validity: V8 discharged at each sub-comprehension by the inner Halton sampling; V7 accepts because both zip children are now discrete (Bounded). The lockstep pairing is well-defined: the i-th alpha-sample paired with the i-th beta-sample.
- Footprint: each inner `order(_, Halton, 100)` is a O(100) barrier producing 100 discrete tuples; the outer `zip(Strict)` is streaming O(N-arity). Total: two O(100) barriers + streaming zip = O(200) working set.
- Semantic contrast: `paired` produces 100 specific `(alpha_i, beta_i)` tuples where alpha_i is the i-th Halton draw on [0,1) and beta_i is the *independent* i-th Halton draw on [0,1). This is **not** the same as `order(cartesian(...), Halton, Some(100))` from §11.10, which produces 100 specific (alpha, beta) pairs drawn jointly from the 2-D Halton sequence over `[0,1)²`. The joint form has 2-D low-discrepancy properties (no clustering in the box); the paired form has 1-D low-discrepancy on each axis but the *joint* distribution is two independent 1-D Halton draws, which is **not** 2-D low-discrepancy.
- When to use which: joint form when you want uniform coverage of a 2-D space; paired form when alpha and beta represent two parameters that are externally constrained to vary together (e.g. "the i-th iteration's alpha and beta come from independent draws"). The user's authored form is the intended semantic; N1 says the optimizer does NOT rewrite between them.

### 11.12 Dependent-source cartesian

§3.2 allows clause N's source to reference clause M's variable
for M < N. The cartesian is still well-formed and dispenses
correctly, but the metadata algebra (§10.7) cannot publish a
closed-form `IndexFn`:

```text
for k in 1..10, replicas in 1..(2 * {k})
```

AST: `cartesian(clause(k, 1..10), clause(replicas, 1..(2 * {k})))`

- Cardinality: depends on `k`. For each `k` value the second axis has `2*k - 1` elements (range 1 to 2*k exclusive). Total: Σ(i=1..9) (2i-1) = 81 tuples. The compiler reports `Bounded(81)` if it can statically evaluate the dependent expression; otherwise `BoundedAtMost(...)` with a conservative upper bound, or `Unbounded` if even the bound isn't computable.
- Validity: V1 (disjoint names) passes; the dependent source is permitted by §3.2.
- Metadata: `index_addressable = None` because the axis sizes are runtime-dependent on prior axes' values (per the dependent-source rule in §10.7.2). No closed-form bijection from `0..81` to the dispensed tuples exists — computing the i-th tuple requires walking the dependent enumeration.
- Footprint: O(1) per cursor (each clause streams); the cartesian holds two cursors and one in-flight tuple. Same shape as a non-dependent cartesian.
- Optimizer: R2 push-down does NOT fire over a dependent cartesian. `order(this, Halton, Some(20))` would materialize all 81 tuples and then apply Halton selection — no closed-form index lookup possible. If the workload needs Halton-spread sampling over this space, the user should either restate as a non-dependent shape (e.g. `cartesian(clause(k, 1..10), clause(rep, 1..18)) where {rep} <= 2 * {k}` — which is a wider cartesian + filter, and IS index-addressable) or accept the materialization cost.
- IR: `PUSH_CLAUSE k (1..10)` + `PUSH_CLAUSE replicas (1..(2*{k}))` (with `{k}` resolved per-pull from the cursor) + `CARTESIAN(2)` + `DISPENSE`. The cartesian opcode handles the dependent enumeration by resolving the second clause's source against the current k-cursor's value each time the k-cursor advances.

### 11.13 Two consumption surfaces from one comprehension

A single comprehension can be consumed at either order (§9.5):
as a stream of coordinate tuples (first-order) or as a stream
of scoped kernel instances (second-order). Each surface
maintains its own dispense cursor; pulling from one does not
advance the other.

```text
sweep := for k in 1..10, profile in {profiles}
```

AST: `cartesian(clause(k, 1..10), clause(profile, {profiles}))` — same as §11.1.

Three consumption patterns from the same `sweep`:

```rust
// First-order: a stream of coordinate tuples.
let coords: CoordinateStream = sweep.coordinate_stream();
while let Some(tuple) = coords.advance() {
    log::info!("coords: {tuple:?}");
}

// Second-order: a stream of scoped kernel instances.
let kernels: ScopedKernelStream<MyKernel> =
    sweep.scoped_kernel_stream(&parent_kernel);
while let Some(scoped) = kernels.advance() {
    let result = scoped.run();
    record(result);
}

// One-shot: scope a specific coordinate tuple into a kernel instance.
let replay_coords = load_from_log("entry-42");
let scoped = sweep.scope_once(&parent_kernel, &replay_coords);
let result = scoped.run();
```

Properties illustrated:

- The three surfaces are constructed by three independent
  factory calls on `sweep`. Each returns a fresh handle with
  its own dispense state.
- `coords` and `kernels` produced from the same `sweep`
  advance independently. Pulling 5 tuples from `coords` does
  not move `kernels`'s cursor; `kernels` still emits its full
  sequence starting from the beginning.
- Two `CoordinateStream` instances would also advance
  independently — there is no "the cursor" for a comprehension,
  only "this streamer's cursor."
- `scope_once` does not consult or advance any cursor. It's
  the pure-function form of "coordinate tuple → scoped kernel
  instance," useful for replay, debugging, point queries, and
  anywhere a stream isn't the right abstraction.
- The compiled IR is shared across all three surfaces (immutable
  per §9.1) — instantiating multiple streamers does not
  re-compile the comprehension, it only allocates per-streamer
  state (cursors, barrier working sets, PRNG draws if any).

---

## 12. What this design lets us claim

After this document is law:

1. **Composition is the only special case.** There are six
   constructors. Anything else is composition. No "this form
   is a parsing exception" carve-outs.
2. **Validity is decidable in one bottom-up pass.** Axioms C1-C2
   plus V1-V9 are all local rules at one AST node given its
   children's properties. No global analysis.
3. **Memory and dispense bounds are derivable at compile time.**
   §6.1 propagates cardinality; §6.2 propagates footprint. The
   peak memory of any AST is computable without execution.
4. **The runtime is a small operator language.** §9.1's eight
   opcodes are enough to execute every well-formed AST. New
   user-visible features (new clause sources, new ordering
   strategies) don't add opcodes — they parameterize existing
   ones.
5. **Algebraic equivalences are documented and direction-
   tagged.** §7's rewrite rules tell the optimizer (§10) which
   transformations preserve semantics. §7's equivalences are
   the *correctness* anchor; §10's rewrites are *required* for
   tractable resource bounds. A bare interpreter on the
   un-optimized AST is correct but potentially catastrophic in
   working-set size — see §10's motivating example.
6. **Streaming is the default, materialization is explicit.**
   §6.2's footprint table and §9.3's resource bound together
   guarantee that the only memory above per-operator constants
   is at named barriers (non-Lex `order`, `zip(Cycle)`'s
   shorter children). The optimizer's push-down rules shrink
   those barriers further; nothing inflates them.

---

## 13. Migration relative to current code

Today's `polydat::comprehension::ast::Comprehension` carries
`{mode, filter, order}` flat on one struct. The shift to this
algebra:

- `Comprehension` becomes an enum: `Clause`, `Cartesian`, `Zip`,
  `Union`, `Filter`, `Order`. Each variant carries its operands
  and constructor-specific scalars.
- The current `ComprehensionMode::Cartesian(Vec<Clause>)` is
  the `Cartesian` variant; `ComprehensionMode::Union(Vec<Vec<Clause>>)`
  is the `Union` variant with `Cartesian` children.
- The current `filter: Option<String>` and `order: Option<TraversalOrder>`
  fields on `Comprehension` retire — they become `Filter` and
  `Order` AST nodes wrapping the comprehension they apply to.
- `coordinate_names()` becomes a method on every variant,
  computed recursively.

The parser changes scope to recursive: wherever it currently
parses a clause list, it now parses a comprehension expression.
The bracketed-string union form parses each string as a
comprehension recursively.

The evaluator's existing `enumerate_tuples` becomes a per-
variant `evaluate` method on the new enum, with each variant
calling its children's `evaluate` and combining results per its
operator's semantics. The pipeline (enumerate → filter → order
→ materialize) collapses into the operator tree's bottom-up
evaluation.

Two new layers ship as part of the migration:

- A **post-parse optimizer** (§10) that rewrites the parsed AST
  before compilation. This is mandatory, not optional — without
  it, perfectly valid user expressions allocate catastrophic
  working sets.
- An **immutable IR surface** (§9.1) exposed as
  `polydat::comprehension::ir::Program` so external tooling can
  inspect compiled programs without recompiling them.

The current `TraversalOrder` enum loses its user-callback escape
hatch (§3.6); existing callers must select a named strategy.
The migration includes an audit of in-tree call sites; out-of-
tree consumers (there are none today, but the public crate is
shipping) get a deprecation note in CHANGELOG.

The migration is a single push (no incremental valid-but-
partial state — the operator tree replaces the flat struct
atomically), but the changes are mechanical given the
correspondence above. PolyStreamer (SRD-78) consumes the new
operator-tree comprehension type via its compiled IR.

---

## 14. Planned deferrals

This section is the **deferral roster** — items the spec
deliberately does not address in its current form. Each entry
names what's deferred, the rationale (why deferring is the
correct call now, not just convenient), the workaround until
the item lands, and the condition for revisiting. The roster is
a plan, not an open-questions list — every entry below has been
considered and explicitly punted, not left unresolved.

### 14.1 Deferred R-rules (R8 – R10)

**Status:** PLANNED — predicate analyzer infrastructure ready;
rules land when workload pressure justifies.

§10.9.7 enumerates three predicate-aware optimizer rules
already designed against the `PredicateInfo` surface
(§10.9.3):

- **R8 — range-narrowing into cartesian.** Rewrites
  `filter(cartesian(clause(a, 1..1000)), {a} >= 500)` →
  `cartesian(clause(a, 500..1000))`. The filter disappears;
  the clause-source narrows. Shrinks the cartesian's input
  space at the source.
- **R9 — discrete-set substitution.** Rewrites
  `filter(cartesian(clause(a, 1..1_000_000)), {a} in [7, 42])`
  → `cartesian(clause(a, [7, 42]))`. Same shape as R8.
- **R10 — monotonic-cutoff truncation.** When a per-axis
  predicate is `Monotonicity::Increasing` and the cartesian
  enumerates in Lex order, short-circuit the axis at the first
  false value.

**Rationale for deferral:** Each rule requires a corresponding
source-side rewrite (BoundedInt narrowing, literal-list
substitution, axis-short-circuit). The plumbing isn't free.
R5 alone covers the most common filter-pushdown cases;
R8/R9/R10 are refinements whose benefit depends on workload-
specific predicate shapes. Until R5 lands and workload usage
patterns surface, ranking these three by expected payoff is
guesswork.

**Workaround:** None needed — the un-optimized form is
correct, just less efficient. R5 still pushes the filter into
each cartesian child; the filter then runs per-tuple against
the un-narrowed source. Authors who need the narrowing today
can manually restate as `cartesian(clause(a, 500..1000))`.

**Revisit when:** R5 has landed and at least one workload
shows measurable benefit from R8/R9/R10 in its hot path.

### 14.2 Continuous-coord predicate analysis

**Status:** PLANNED — `OpaqueReason::Continuous` explicitly
marks the dead-end; analyzer extension is a separate design
problem.

§10.9.1's out-of-scope list and §10.9.3's `OpaqueReason`
enum document that any predicate touching a continuous-
cardinality coord is `Opaque`. R5 doesn't fire on
continuous-axis filters; the filter still runs per-sample at
the sampled output.

**Rationale for deferral:** Continuous-space predicate analysis
is a distinct problem class — interval arithmetic, measure-
preserving factorization, density-aware push-down. The
techniques don't transfer from the discrete recognizer
catalog. Designing the continuous-coord analyzer is a
significant additional surface that doesn't share infrastructure
with the discrete case beyond the `PredicateInfo` carrier type.

**Workaround:** Author-side restatement. A filter over a
continuous coord that factorizes per-axis can be expressed by
narrowing the source's interval directly. For
`order(cartesian(clause(theta, 0.0..2π), clause(r, 0.0..1.0)),
Halton, Some(100)) where {theta} < pi`, restate as
`order(cartesian(clause(theta, 0.0..pi), clause(r, 0.0..1.0)),
Halton, Some(100))` — the interval narrowing is purely
syntactic.

**Revisit when:** At least one workload has continuous-coord
filtering whose author-side restatement is awkward and whose
predicate shape fits a small recognizable pattern (e.g.
"polynomial constraint", "ellipsoidal region").

### 14.3 Cross-streamer shared sub-evaluation

**Status:** PLANNED — leaning toward SRD-78 (runtime) as
owning concern, not polydat.

§8.3's derived-streamers case (`base`, `sampled`, `boundary`,
`hot_corner`) — and now §11.13's two-surfaces case — raise
the question of whether multiple streamers from related or
identical comprehensions should share evaluation work. The
optimizer (§10) intentionally compiles per-AST; SRD-78 may or
may not cache cross-streamer.

**Rationale for deferral:** Sharing is a runtime memoization
concern, not a comprehension-algebra concern. The metadata
algebra (§10.7) and the reducibility analyzer (§10.10) operate
on single ASTs in isolation; folding cross-AST analysis into
either layer would break the closure-over-one-AST discipline
that makes them clean. The decision space is "where in the
stack does cross-streamer caching live?" — polydat-internal
(optimizer recognizes shared sub-ASTs, emits a shared IR
fragment) or SRD-78-runtime (streamers consult a sub-evaluation
cache keyed by IR hash). The current lean is SRD-78 because
caching policy depends on runtime memory pressure and
workload-shape information that polydat doesn't have at
compile time.

**Workaround:** Acceptable — derived streamers re-evaluate
their `base` per instantiation. For small `base` cardinalities
this is cheap; for large `base` the user can manually
materialize the base into a discrete literal source and bind
that as a name.

**Revisit when:** SRD-78 surfaces a concrete cross-streamer
caching design or measurement shows derived-streamer
re-evaluation is a hot-path concern.

### 14.4 Strategy extensibility surface

**Status:** PLANNED — leaning toward internal-only (closed
enum extension), no out-of-tree hook.

§3.6's strategy taxonomy is a closed enum. Adding a new named
strategy (e.g. `Sobol2D`, `Latin/k`, `LowDiscrepancyCustom`)
requires coordinated changes: parser keyword, §3.6 table entry,
§10.2 R2 push-down rule, per-strategy `IndexFn` requirement.

**Rationale for deferral:** The lean is internal-only because
(a) strategies are small in number — adding one is a focused
PR, not a heavy ceremony — and (b) each strategy's push-down
rule is non-trivial Rust code (lattice-index arithmetic,
PRNG state management, measure mapping for continuous cases).
A registration hook for out-of-tree strategies would expose
internal optimizer surfaces and constrain refactoring. The
deferral is "no escape hatch" rather than "we'll add an
escape hatch later."

**Workaround:** New strategies land as PRs against this spec
+ the polydat crate. The crate's `StrategyName` enum is
`#[non_exhaustive]` (per the §9.1 immutability discipline), so
adding variants is a minor-version change that doesn't break
downstream consumers' match arms.

**Revisit when:** A workload need genuinely cannot be expressed
by the existing strategy set AND the new strategy is not
generally useful enough to upstream. Both conditions must hold;
satisfying only one means the strategy upstreams.

### 14.5 Filter-cost-aware optimizer (R5 catalog depth)

**Status:** PLANNED — wait for R5 to land + workload pressure.

R5's guard depends on the predicate analyzer recognizing
factorization. §10.9.5's initial recognizer catalog covers
simple patterns (`{a} OP K`, conjunction with coord-disjointness,
range constraints, discrete-set membership). Deeper patterns
(polynomial factorization, conditional expressions, Polydat kernel
calls with known semantics) are not in the initial catalog.

**Rationale for deferral:** Recognizer development is
"speculative without measurement." Each pattern added to the
catalog is dead code unless real workloads exercise it. The
analyzer's "conservatively incomplete" property (§10.9.4
property 2) means missing optimizations are acceptable; the
catalog can grow as workload patterns appear in practice.

**Workaround:** Workloads with predicates the analyzer doesn't
recognize get correct execution but no push-down — the filter
runs per-tuple at the outer level. Authors can restructure to
hit a recognized pattern (e.g. split a complex predicate into
a per-axis conjunction).

**Revisit when:** A specific predicate pattern shows up in
≥3 workloads and the per-tuple cost is measurable.

---

## 15. Relationship to the SRDs

The following SRDs touch comprehension material. This section
names each SRD's role relative to this document.

### 15.1 Sibling specifications (polydat-adjacent)

- **SRD-18c (Comprehension Syntax)** — **owns the parser-layer
  surface grammar** that produces this document's ASTs. SRD-18c
  defines how source text becomes `clause` source values
  (literal lists, integer/string ranges, generators, SI
  suffixes, etc.). It does **not** own comprehension semantics;
  semantics are this document. SRD-18c also needs an extension
  push to define continuous-source grammar (`0.0..2π` real
  intervals, distribution-object sources per §3.1); until that
  push lands, this document is the reference for continuous-
  source text representation and SRD-18c is the parser
  reference only for discrete sources.

- **SRD-18d (Traversal Order)** — **owns per-strategy
  algorithmic detail** (Halton recurrence, Sobol direction
  numbers, Lhs stratification construction, etc.). SRD-18d
  does **not** own compositional behavior or per-strategy input
  requirements; §3.6 is authoritative on those. SRD-18d carries
  no `Custom(fn)` escape hatch and includes `Shuffle` alongside
  the other named strategies.

- **SRD-18e (Canonical Reference)** — **superseded by this
  document.** Retired to a redirect stub; cross-references to
  SRD-18e should target this document.

- **SRD-78 (PolyStreamer)** — **owns the runtime that hosts
  this document's consumption surfaces.** SRD-78 implements
  `CoordinateStream`, `ScopedKernelStream<K>`, and
  `scope_once` (defined here in §9.5) as concrete types over
  the shared compiled IR (§9.1). The "lock-free shared cursor"
  semantic SRD-78 owns applies *per streamer instance* — two
  streamers from one comprehension have two independent
  cursors (per §9.5.2's independence contract). SRD-78 also
  owns the unbounded-variant queue used to accept `Unbounded`
  cardinality streamers.

### 15.2 Consumer specifications (workload + control flow)

- **SRD-18 (Control Flow)** — defines the user-facing
  control-flow shapes (`ForCombinations`, `ForEachUnion`, the
  `for_each` family) that desugar to polydat constructors per
  §8. SRD-18 frames each shape as "this control-flow construct
  desugars to [polydat constructor]" and carries no inline
  semantics.

- **SRD-18b (Scenario Tree and Scheduler)** — defines the
  scenario tree's `ScenarioNode::Comprehension { comprehension,
  children }` wrapper variant, where `comprehension` is a
  reference to a `polydat::comprehension::Comprehension`
  value. SRD-18b owns the scenario-tree integration (how
  scenario nodes dispatch and find-by-comprehension lookup);
  this document owns the comprehension type SRD-18b wraps.

- **SRD-71 (Cursor Partitions)** — owns the cursor-partition
  surface (partition-spec language, `cursor.partitions`
  projection wire, cursor-declaration `over <iter-var>` syntax,
  CLI workload-param surface). The comprehension iteration
  pattern `for: "p in cursor.partitions"` is standard polydat
  clause-over-list semantics per §3.1 and §8.1 — SRD-71 does
  **not** extend polydat's algebra. Polydat sees a plain
  list-source comprehension; the cursor narrowing is SRD-71's
  concern resolved against the iter-var at cursor
  materialization time.

### 15.3 Integration touch points (kernel scope, lifecycle)

- **SRD-13e (Scope-as-Module Refinement)** — names
  `ComprehensionModule` as one of the typed scope modules.
  The module's content is polydat-defined; SRD-13e owns the
  typed scope-module protocol that wraps it.

- **SRD-13f (Cross-Scope Wire Materialization)** — describes
  cross-scope wire flow that crosses comprehension scope
  boundaries. References polydat's public synthesis surface
  (this document §9.5) rather than internals paths.

- **SRD-67 (GK Subcontext Construction)** — describes
  comprehension scope synthesis as one of the subcontext-
  construction paths, via polydat's public synthesis surface.

- **SRD-68 (Dispenser-Owned Polydat Context)** — mentions
  `for_each` comprehensions positionally in the scope-tree
  ownership model; integration-level only.

- **SRD-74 (None Propagation)** — references polydat's
  comprehension synthesis test suite for the Gate 2
  regression guard; no semantic claims.

### 15.4 Passing-mention SRDs

These SRDs name "comprehension" as a background concept without
making semantic claims; each carries a one-line cross-reference
to this document on first mention:

- **SRD-00 (Index)** — table-of-contents entries.
- **SRD-02 (Concurrency Model)** — comprehension iter-steps in
  the concurrency context.
- **SRD-11 (GK Evaluation)** — enclosing-comprehension
  advancing in the scope-init pull context.
- **SRD-17 (Diagnostic Modes)** — comprehension iteration
  logging.
- **SRD-40b (Synthetic Metrics from GK)** — example syntax
  using a `comprehension_var`.
- **SRD-44 (Workload Checkpointing)** — comprehensions
  enumerate distinct tuples for checkpoint ordering.

### 15.5 Ownership invariant

The following invariant holds across the SRD corpus:

> Every comprehension-semantic claim in the SRD corpus either
> appears in this document, or is a one-line reference to a
> section of this document. No two SRDs make different claims
> about the same comprehension behavior.

Verification is a `grep` sweep: every passage in any non-polydat
SRD mentioning a comprehension constructor, axiom, strategy, or
optimizer rule must either (a) be an integration description, or
(b) be a cross-reference to this document.
