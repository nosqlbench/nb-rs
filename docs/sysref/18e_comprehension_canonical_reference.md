# 18e: Comprehension Canonical Reference

The consolidating spec for the comprehension grammar. SRD-18c
covers the syntactic surface (clause forms, layered grammar).
SRD-18d covers traversal order (which tuples come out first).
This SRD pins down the parts neither one fully specifies on
its own:

- The full canonical AST in one place
- Mode detection (Cartesian vs Union)
- The coordinate-set contract (what children see)
- The execution pipeline (`enumerate → filter → order →
  materialize`)
- Index-space semantics for orderings (Cartesian lattice
  is the source of truth, even after filter)
- Union mode's interaction with non-lex orderings
- `where` predicate semantics (interpolation, types,
  missing names)
- The Layer 7 extension path and its `Value::Tuple`
  dependency
- Per-strategy implementation status

Read this **after** 18c (syntax) and 18d (order); read it
**before** writing or auditing comprehension code. The two
syntax / order SRDs are user-facing. This one is the contract
the implementer reads.

> **Status:** AST + Cartesian + Union + filter + 6 of 10
> orderings shipped (`nbrs-variates::comprehension::*`).
> Halton / Sobol / Lhs / Custom strategies parse-and-error
> until implemented. Layer-7 parallel + Layer-7 destructure
> are AST-extension work; this SRD specifies the migration
> path.

---

## The canonical AST

Every comprehension lowers to a single struct:

```rust
pub struct Comprehension {
    pub mode: ComprehensionMode,
    pub filter: Option<String>,        // GK predicate; missing = no filter
    pub order: Option<TraversalOrder>, // None = lex (default)
}

pub enum ComprehensionMode {
    /// Single ordered list of clauses; iteration emits the
    /// cross product. Single-clause Cartesian collapses to
    /// `for_each var in expr`.
    Cartesian(Vec<Clause>),
    /// List of sub-spaces. Each sub-space is its own
    /// Cartesian; iteration concatenates each sub-space's
    /// product in declaration order.
    Union(Vec<Vec<Clause>>),
}

pub struct Clause {
    pub var: String,
    pub expr: String,
}

pub enum TraversalOrder {
    Lex          { count: Option<usize> },
    ReverseLex   { count: Option<usize> },
    Diagonal     { count: Option<usize> },
    Antidiagonal { count: Option<usize> },
    Extrema      { strata: Option<usize> },
    Shells       { origin: ShellOrigin, depth: Option<usize> },
    Halton       { count: Option<usize> },
    Sobol        { count: Option<usize> },
    Lhs          { count: Option<usize>, seed: Option<u64> },
    Custom       { function: String },
}

pub enum ShellOrigin { Outer, Center, Corner }
```

This is the source of truth. SRD-18c's `ClauseSource::Single
| Parallel` and 18c's `Clause { vars: Vec<String> }` are
**aspirational** — they describe the Layer-7 extension path
(below). The shipped code uses single-var `Clause` and
single-clause `ComprehensionMode` variants.

**Storage**: every string field is owned (`String`, not
`&str`). The AST gets parked on long-lived scope-tree /
scenario-tree nodes; sharing references back into source
text would force lifetimes through every consumer.

**Serde-ready**: `#[derive(Serialize, Deserialize)]` on
every variant. Round-trips cleanly through YAML, JSON,
sqlite blob — used by the snapshot store and replay
tooling.

---

## Mode detection: Cartesian vs Union

The parser decides Cartesian vs Union based on **variable-
name repetition** across declared sub-spaces, not a
keyword:

- All distinct variable names → `Cartesian(Vec<Clause>)`
- Any variable name repeats → `Union(Vec<Vec<Clause>>)`

```text
k in 10, 100, limit in 10, 20, 30          → Cartesian
k in 10, limit in 10, 20, k in 100         → Union
```

The repetition is the signal that the user wanted parallel
sub-spaces (children see the same binding shape regardless
of which sub-space the current tuple came from), not a
cross product (which would over-bind `k`).

YAML carries this through two interchangeable shapes —
**list of clause-lists** is always Union; **flat clause
list** is Cartesian unless name-repetition triggers Union
inference:

```yaml
# Cartesian (distinct names)
- for: "k in 10, 100, limit in 10, 20, 30"

# Union (list of strings == list of sub-spaces, declared)
- for_each: ["k in 10, limit in 10,20,30", "k in 100, limit in 100,200"]

# Union (inferred from repeated `k`)
- for: "k in 10, limit in 10,20, k in 100, limit in 100,200"
```

The third form is the inference path — the parser detects
repetition, splits at the second `k in …`, and emits Union
with the two sub-spaces.

---

## Coordinate-set contract

Every iteration of a comprehension scope publishes a
**scope-coordinate set**: one `(name, value)` pair per
**unique** variable name across all sub-spaces. Children
see one extern per unique name regardless of how many
sub-spaces the comprehension declares.

Method on the AST:

```rust
impl Comprehension {
    pub fn coordinate_names(&self) -> Vec<&str> {
        // Dedup, preserving first-occurrence order.
    }
}
```

For Cartesian mode this is just the LHS of each clause.
For Union mode the names typically repeat across sub-
spaces; dedup gives the operator-visible coordinate set.

This is the contract that lets Union mode work for
children: a `do_while` or phase under a Union comprehension
sees `{k}` and `{limit}` regardless of which sub-space the
current iteration came from.

**Order is first-occurrence**: `k in 10, limit in 100, k
in 200` produces coordinates `[k, limit]`, not `[limit,
k]`. Authoring intent wins over alphabetical sorting.

---

## Execution pipeline

Every comprehension execution follows a fixed pipeline:

```
clause specs
    │
    ▼
enumerate_tuples           ← cross-product / union concat / parallel-zip (Layer 7)
    │  default: lex order, rightmost varies fastest
    ▼
apply filter               ← GK predicate per tuple (SRD-18c §"Layer 4")
    │  filter operates on tuple values
    ▼
apply order                ← reorder (and possibly truncate) (SRD-18d)
    │  order operates on tuple positions in the Cartesian lattice
    ▼
materialize child kernel
    │
    ▼
run children per tuple
```

**Filter then order**, never the reverse. Reversing would
mean `count: N` truncation could be wiped out by the filter,
leaving fewer than N tuples. Filter-then-order with
truncation gives "the first N tuples in this order from
the filtered set", which is what users expect.

**Pure transforms**: each step takes a `Vec<Tuple>` and
returns a `Vec<Tuple>` (with possible truncation). No
side effects, no kernel mutation. The next step sees only
what the previous emitted.

**Tuple shape**: `Vec<(String, Value)>` — name + value
pair per coordinate. Value uses GK's existing `Value` enum
(see SRD-10), not a separate type.

---

## Index-space contract for orderings

All non-lex orderings (`reverse_lex`, `diagonal`,
`antidiagonal`, `extrema`, `shells`, `halton`, `sobol`,
`lhs`) operate on the **Cartesian lattice's index space**,
**not** post-filter projected indices.

Concrete rule:

1. Enumerate the full Cartesian product, generating each
   tuple's per-clause **lattice index** alongside its value.
2. Apply the filter against tuple values. Surviving tuples
   keep their original lattice indices.
3. Apply the ordering against the surviving tuples'
   original lattice indices.

**Why this rule**: orderings like `extrema` and `shells`
reason about *position in the parameter space*. A tuple
that filter retains because its value passes the predicate
is still at index `(0, 5, 2)` in the original lattice —
its corner-ness or shell-distance is a property of where
it lives in the space, not of which other tuples survived.

**Concrete consequence**: filter-induced gaps in the
emitted tuple stream are visible. `for: "k in 1..10, limit
in 1..10 where {k} * {limit} <= 50 order extrema"` emits
the surviving corners (some corners may have been filtered
out — those don't appear), then surviving edges, etc. It
does NOT redefine "corner" relative to the survivor set.

This contract is **reflected in the function signatures**:

```rust
fn order_extrema(tuples: Vec<Tuple>, sizes: &[usize], ...) -> Vec<Tuple>
```

`sizes` is the **original Cartesian lattice's per-clause
cardinality**, not a post-filter projection. Each tuple
carries its lattice index implicitly via the order it came
out of `enumerate_tuples`.

**Edge case — empty strata**: when filter removes all
tuples in the stratum that `extrema/N` would have kept,
the emission proceeds to the next stratum. No padding, no
error.

---

## Union mode + non-lex orderings

Union sub-spaces have potentially different shapes (different
clause counts, different per-clause cardinalities). There is
no single Cartesian lattice for the union, so index-space
orderings have no natural meaning.

**Resolution**: index-space orderings (`reverse_lex`,
`diagonal`, `antidiagonal`, `extrema`, `shells`, `halton`,
`sobol`, `lhs`) **error at parse-time** when applied to a
Union-mode comprehension. The parser emits:

```
order: <strategy> requires Cartesian comprehension; got Union.
For per-sub-space ordering, split into separate comprehensions
and order each. For uniform ordering across the union, use
`order: lex` or `order: custom(<gk-fn>)`.
```

**`lex` and `custom` are the exceptions**:

- `lex` works because it's a stable enumeration order, not
  a geometric reasoning step. The Union concatenates each
  sub-space's lex stream in declaration order. `count: N`
  truncates the concatenated stream.
- `custom(<gk-fn>)` receives the full tuple list as input;
  the function decides what ordering means for the user's
  Union shape. The escape hatch.

**Implementation contract** (in `comprehension::eval`):

```rust
fn validate_order_for_mode(
    mode: &ComprehensionMode,
    order: &Option<TraversalOrder>,
) -> Result<(), String> {
    match (mode, order) {
        (ComprehensionMode::Union(_), Some(TraversalOrder::ReverseLex { .. }))
        | (ComprehensionMode::Union(_), Some(TraversalOrder::Diagonal { .. }))
        | (ComprehensionMode::Union(_), Some(TraversalOrder::Antidiagonal { .. }))
        | (ComprehensionMode::Union(_), Some(TraversalOrder::Extrema { .. }))
        | (ComprehensionMode::Union(_), Some(TraversalOrder::Shells { .. }))
        | (ComprehensionMode::Union(_), Some(TraversalOrder::Halton { .. }))
        | (ComprehensionMode::Union(_), Some(TraversalOrder::Sobol { .. }))
        | (ComprehensionMode::Union(_), Some(TraversalOrder::Lhs { .. }))
            => Err("…".to_string()),
        _ => Ok(()),
    }
}
```

This runs at workload-load (before the comprehension is
ever evaluated), so the rejection is loud and early.

---

## `where` predicate semantics

The `where` clause is a single GK predicate evaluated
against each tuple. The predicate's source string is a
**string-interpolation expression**: `{name}` placeholders
are expanded against the per-tuple kernel; the rest is a
const-evaluable GK expression.

### Interpolation contract

For each tuple, the executor:

1. Builds a per-tuple **sub-kernel** with one binding per
   clause-bound name (`k`, `limit`, …) plus inherited
   parent-scope bindings (workload params, outer
   for-each vars).
2. Walks the predicate string looking for `{<ident>}`
   placeholders. Each placeholder's identifier must
   resolve to a binding in the sub-kernel.
3. Substitutes each placeholder with its bound value's
   string representation. Quoting follows the same rule
   as workload-param interpolation: numeric values become
   bare numbers, strings get wrapped in quotes.
4. Runs `eval_const_expr` on the substituted string.
5. The result must be `Value::Bool`. Anything else is a
   runtime error (not a tuple drop).

### Missing names

A `{var}` referencing a name that's neither a clause var
nor an inherited binding is a **load-time** error caught
by static analysis (the parser walks the predicate text,
extracts identifier references, and validates against the
declared clause names + the resolved parent scope's
manifest). Strict-mode default: error. Loose mode: warn,
treat as `Bool::false` at eval time (so tuples filter
through cleanly without crashing the run).

### Type contract

The predicate's result must be `Value::Bool`. Numeric
truthiness rules (`0` / `0.0` → false, others → true) from
SRD-18 `do_while` / `do_until` do **not** apply here; the
filter requires explicit boolean output. Per-tuple errors
("predicate returned non-bool") are reported once with
the offending tuple's coords for diagnosis.

### Strict-mode interaction

SRD-15 strict mode promotes the loose-mode warnings:
- Missing-name reference: error
- Non-Bool predicate result: error
- Type-mismatch in placeholder substitution
  (e.g., `{k}` is Str but predicate expects U64): error

Loose mode (default) continues with warning + skip-tuple
behavior so a single bad predicate doesn't kill a long run.

---

## Layer 7 extension path

SRD-18c §7 promises tuple-LHS comprehensions
(`(x, y) in (1..10, 100..1000)` parallel-zip and
`(host, port) in pairs_csv()` destructure). Stage 1
(parallel-iter) is shipped; Stage 2 (destructure) is
gated on `Value::Tuple` and remains deferred.

### Stage 1 — parallel-iter (Layer 7a) — ✅ shipped

Each clause LHS may be a **parallel group** of names
whose RHS is a **parallel group of expressions** that
zip in lockstep.

**Shipped AST** (additive, doesn't break existing
single-var clauses):

```rust
pub struct Clause {
    pub vars: Vec<String>,           // length 1 = single-var; ≥ 2 = parallel
    pub source: ClauseSource,
}

pub enum ClauseSource {
    /// Single expression yields Vec<Value> (when vars.len() == 1)
    /// or Vec<Vec<Value>> (when vars.len() > 1, destructure form — Stage 2).
    Single(String),
    /// One expression per var; sources zip in lockstep per `mode`.
    /// vars.len() == exprs.len() ≥ 2.
    Parallel { mode: ZipMode, exprs: Vec<String> },
}

pub enum ZipMode { Strict, Truncate, Cycle }  // default = Strict
```

Single-var clauses are constructed via
`Clause::new("k", "1..10")` (which produces
`Clause { vars: vec!["k"], source: ClauseSource::Single("1..10") }`).
Parallel-iter clauses use `Clause::parallel(vars, exprs)`
or `Clause::parallel_with_mode(mode, vars, exprs)`.

**Shipped textual syntax** (recognised by `parse_clause`
and surfaced through the workload-YAML `for_each` parser):

```text
(x, y) in (e1, e2)               # strict zip — default
(x, y) in zip_truncate(e1, e2)   # truncate to shortest
(x, y) in zip_cycle(e1, e2)      # cycle to longest
```

Parens on both sides are required; one-sided paren is
rejected with a clear error. The clause-list splitter
(`split_respecting_parens` / `is_clause_boundary`) accepts
parallel-iter clauses as boundary tokens, so they may
follow or precede single-var clauses in a comma-separated
list:

```text
(x, y) in (xs, ys), z in zs      # 2 axes (zip-step × z)
```

**Parallel-zip semantics**: sources advance in lockstep,
with [`ZipMode`] choosing length policy:

- **Strict** (default): every expression must produce the
  same number of values. Mismatch is a hard error at
  iteration time, with the message naming the offending
  expression index.
- **Truncate** (`zip_truncate(...)`): the zip step count is
  `min(len(expr_i))`; longer columns are truncated.
- **Cycle** (`zip_cycle(...)`): the zip step count is
  `max(len(expr_i))`; shorter columns repeat
  (`col[step % col.len()]`). An empty column under Cycle
  routes through the empty-clause callback (no value to
  repeat).

`zip_truncate` and `zip_cycle` are **syntax modifiers** on
the parallel-iter form, not standalone stdlib functions —
their semantics live in the comprehension evaluator and
don't require `Value::Tuple`.

**Lattice index** for a parallel group: one `usize` per
group (the zip step), not per-name. `compute_clause_sizes`
reports one cardinality per parallel clause (the zip-step
count under the selected mode), so index-space orderings
(halton / extrema / shells / lhs / etc.) see the lattice
correctly: a `(x, y) in (...)` + `z in zs` comprehension
is a 2-axis lattice, not 3-axis.

**Activity-side wiring**: `enumerate_tuples`,
`compute_clause_sizes`, `iterate_scope`, `runtime_iterate`,
and `premap_iterate` all carry `&[Clause]` end-to-end —
no `(String, String)` flattening loses the
parallel-vs-single distinction between layers.

### Stage 2 — destructure (Layer 7b)

Single-RHS source emits tuples; LHS unpacks positionally.

```text
(host, port) in pairs_from_csv("hosts.csv")
(k, expected_recall) in test_dataset()
```

**Hard dependency**: `Value::Tuple` (or equivalent
list-of-lists shape) on the GK side. The clause expression
must return `Vec<Vec<Value>>` (a list of N-tuples) instead
of the usual `Vec<Value>`. Until `Value::Tuple` lands,
destructure is parse-rejected with a clear message
pointing at this dependency.

Stage 2 is **not blocking** Stage 1. Parallel-iter is
shipped on its own; destructure waits on `Value::Tuple`.

### `custom` ordering — same dependency

`TraversalOrder::Custom { function }` calls a GK function
of signature `fn(tuples: List<Tuple>) -> List<Tuple>`. The
input requires `Value::Tuple`. Until Stage 2 lands, the
parser accepts `order: custom(...)` and the evaluator
errors at runtime with:

```
order: custom(...) requires GK Value::Tuple support
(SRD-18c Layer 7b). Use a named ordering strategy or
defer custom ordering until Layer 7b lands.
```

This matches the shipped `comprehension::order::apply_order`
behavior today.

---

## Implementation status

### Shipped (in `nbrs-variates/src/comprehension/`)

| Surface | Module | Notes |
|---|---|---|
| `Comprehension` AST | `ast.rs` | Full struct with `mode`/`filter`/`order` |
| `ComprehensionMode::Cartesian` | `ast.rs` + `eval.rs` | Single-clause and multi-clause |
| `ComprehensionMode::Union` | `ast.rs` + `eval.rs` | Variable-name-repeat detection |
| `Clause { var, expr }` (single-var) | `ast.rs` | Stage 0 — current shipping shape |
| `filter: Option<String>` predicate | `eval.rs` | Bool-result contract |
| `parse_comprehension_text` | `parse.rs` | Layer 1 + 4 + order parse |
| `parse_order_spec` (10 strategies) | `parse.rs` | All 10 parse; not all evaluate |
| `coordinate_names()` dedup | `ast.rs` | First-occurrence order |
| `order: lex` (with `count`) | `order.rs` | Default emission order |
| `order: reverse_lex` | `order.rs` | |
| `order: diagonal` | `order.rs` | Index-sum sorting |
| `order: antidiagonal` | `order.rs` | Mirror of diagonal |
| `order: extrema` (with `strata`) | `order.rs` | Interior-count stratification |
| `order: shells` (3 origins, with `depth`) | `order.rs` | L∞ stratification |

### Planned — not yet implemented

| Surface | Status | Blocking dep |
|---|---|---|
| Layer 2 — range operator (`..`, `..=`, `..step`) | Parse-and-error | None |
| Layer 3 — named generators (`fib`, `pow2`, `geometric`, …) | Stdlib stubs | None |
| Layer 5 — set ops (`concat`, `unique`, `interleave`, …) | Stdlib stubs | Useful only after Layer 3 |
| Layer 6 — SI suffix literals (`1K`, `1Mi`, …) | Lexer extension | None |
| Layer 7a — parallel-iter clauses | AST extension + parser | `Clause` migration |
| Layer 7b — destructure clauses | AST + parser + GK type | `Value::Tuple` |
| `order: halton` | parse-OK / eval-err | None |
| `order: sobol` | parse-OK / eval-err | None |
| `order: lhs` | parse-OK / eval-err | None |
| `order: custom(<fn>)` | parse-OK / eval-err | `Value::Tuple` (Stage 2) |
| Sequencer expansions (`bucket`, `concat_seq`, `interval_seq`) | Stdlib | None — uses `nbrs-activity::opseq` |
| Index-space orderings reject Union | TBD | Spec landed; impl simple |

### Implementation order (per SRD-18c §"Implementation order", refined)

1. **Layer 7a — parallel-iter** (AST migration). One
   refactor sweep across `nbrs-variates`. Lowest grammar
   addition; highest user-visible win.
2. **Layer 2 — range operator**. Lexer + AST + desugar to
   stdlib `range(...)`. No comprehension changes.
3. **Layer 6 — SI suffixes**. Lexer-only.
4. **Halton + Sobol orderings**. The two big space-filling
   strategies. `Lhs` follows naturally with a stratum-
   permutation primitive.
5. **Layer 3 — named generators**. Each is one stdlib
   node + a short test. Land in any order.
6. **Sequencer expansions** (`bucket`, `concat_seq`,
   `interval_seq`). Reuse `nbrs-activity::opseq`.
7. **Layer 5 — set operators**. Trivial once Layer 3
   ships (lists are first-class clause sources).
8. **Index-space-orderings-on-Union rejection**. One
   match arm in `validate_order_for_mode`; can land
   independently.
9. **Layer 7b — destructure form** + **`order: custom`**.
   Both gated on `Value::Tuple`.

---

## Worked example — every layer

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
      order: halton/64
      phases: [search]
```

What happens at workload-load:

1. **Parse**. Yaml → `Comprehension {
     mode: Cartesian([
       Clause { vars: ["table", "k_dim"], source: Parallel([..., "1..=5"]) },
       Clause { vars: ["limit"], source: Single("pow2(8)") },
       Clause { vars: ["profile"], source: Single("bucket('3:ann, 1:exact')") },
     ]),
     filter: Some("{k_dim} * {limit} <= 1024".into()),
     order: Some(TraversalOrder::Halton { count: Some(64) }),
   }`.
2. **Validate**. Mode (Cartesian) compatible with `halton`
   ordering → OK. Variable names distinct → Cartesian
   confirmed. Predicate's identifiers (`k_dim`, `limit`)
   match clause names → OK.
3. **Coordinate-set publication**: this comprehension
   declares coordinates `[table, k_dim, limit, profile]`
   for children to bind against.

What happens at scope-init (per outer iteration):

1. **enumerate_tuples** — produces every `(table, k_dim,
   limit, profile)` Cartesian product (with `(table,
   k_dim)` zipped). Cardinalities: `len(vector_tables())`
   × 1 (zip with k_dim) × 8 × 4 = N tuples.
2. **filter** — drops tuples where `k_dim * limit > 1024`.
   Survivors keep their original lattice indices.
3. **order: halton/64** — walks the Halton sequence on
   the full lattice (rejecting tuples not in the survivor
   set), emits the first 64 hits. Determinism: same input
   produces same 64 tuples every run.
4. **materialize** — the executor binds each tuple's
   coordinates into the child scope and runs `phases:
   [search]`.

---

## Cross-references

- [SRD 18b — Scenario Tree and Scheduler](18b_scenario_tree_and_scheduler.md): the AST + execution
  plumbing this SRD ties off (`enumerate_tuples`,
  `iterate()`, scope-coordinate publication).
- [SRD 18c — Comprehension Syntax](18c_comprehension_syntax.md): the user-facing layered grammar.
  Use this when authoring workloads.
- [SRD 18d — Comprehension Traversal Order](18d_comprehension_traversal_order.md): the order
  taxonomy. This SRD pins the index-space contract that
  18d's strategies operate against.
- [SRD 10 — GK Language](10_gk_language.md): the
  expression grammar that clause expressions and the
  `where` predicate both use.
- [SRD 22 — Op Sequencing](22_op_sequencing.md): the
  bucket / concat / interval algorithms that the
  sequencer-style expansions reuse.
- `nbrs-variates/src/comprehension/{ast,parse,eval,order}.rs`:
  the implementation modules.
- `docs/design/comprehensions_grammar_plan.md`: push-by-
  push implementation plan.

---

## Why this shape

**One AST, many surfaces** (carried over from 18c):
everything lowers to `Comprehension { mode, filter, order
}`. New surface forms add parser code or stdlib nodes;
they don't add comprehension semantics.

**Index-space contract for orderings** (this SRD): every
non-lex strategy reasons about position in the Cartesian
lattice, not value. A 10-element float clause has indices
0..9 regardless of its values. This decouples the ordering
algorithms from value types and keeps the geometric
semantics clean.

**Filter then order** (this SRD): index-space orderings
operate on the filtered set's positions in the original
lattice. Filter-rejected tuples are skipped during
emission, not projected away. This avoids the "redefine
'corner' relative to survivor set" ambiguity that 18d
left open.

**Strict Union+ordering rejection** (this SRD): non-lex
orderings on Union mode have no natural lattice; rather
than papering over with concatenation tricks, the parser
errors and points the user at split-comprehensions or
`custom`. Loud-and-early beats wrong-and-quiet.

**Layered AST extension path** (this SRD): Layer 7a
parallel can land without `Value::Tuple`; Layer 7b
destructure and `custom` ordering wait. The current single-
var `Clause` migrates to `vars: Vec<String> + source:
ClauseSource` with no behavior change; existing
construction sites get a one-line update.

**Explicit predicate contract** (this SRD): `where` takes
a string-interpolation expression that produces `Value::
Bool`; missing names error at load (strict) or warn-and-
skip (loose); type mismatches surface clearly. SRD-15
strict mode promotes warnings to errors uniformly.
