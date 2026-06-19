# The Expression Engine — Polydat Design

**Subtitle:** Polydat as host-embeddable evaluation utility.

Formalises the host-facing evaluation surface that emerges
from polydat's grammar. Names the embedding contract,
catalogs the surfaces, and shows how the substrate and
graph compiler are re-used at expression scale to give host
crates a typed, deterministic, library-rich evaluation
engine for free.

## Authoritative ownership declaration

This document is the **single authoritative reference** for
polydat's role as an embedded expression engine for host
crates. It owns the embedding contract (E-axioms), the
catalog of evaluation surfaces, the composition pattern
(interpolation → evaluation), and the host-design
implications. Where SRD-14 (Config Expressions) and SRD-10
(GK Language) describe specific surfaces, this doc names the
unifying capability the surfaces collectively provide.

## Companion documents

- [Composition Substrate](composition_substrate.md) — the
  S/T/L pillars and the slot contract. The expression
  engine's typed-result guarantee follows directly from T1
  + T2.
- [The Graph Compiler](graph_compiler.md) — the construction
  pipeline. Embedded expression evaluation is the *same
  compiler* operating on smaller input — no separate
  evaluator exists.
- [The Runtime Model](runtime_model.md) — the R-axioms
  (data flow, caching, invalidation) and D-axioms
  (determinism guarantees). E3's bounded-determinism claim
  is the realisation of D1/D2/D3 at expression scale.
- [The Polydat Grammar](grammar.md) — G-axioms. The
  grammar-level commitments that underwrite this doc's
  E-axioms. G3 (scope-chain transparency) + G6 (single
  grammar for expressions and full programs) compose
  into E1 (self-contained submission) + E4 (library
  inheritance) + the expression-as-kernel correspondence
  in §2.
- [SRD-10: Polydat Language and Compilation](language_spec.md)
  — DSL syntax. Owns the language grammar; this doc shows
  how the grammar's full expressivity becomes a host
  utility.
- [SRD-11: Polydat Evaluation Model](evaluation_model.md)
  — two-lifecycle classification. Owns the const-binding
  contract; this doc shows how `eval_const_expr` is the
  const-binding contract at single-expression scale.
- [SRD-14: Polydat Config Expressions](expression_engine.md)
  — the `{...}` config expression surface. Owns one of the
  evaluation surfaces; this doc places it in the broader
  embedding catalog.
- The host's parameter / bind-point resolution owns the parameter
  substitution surface that pairs with embedded evaluation.

The forcing question: **polydat was designed to compile
workloads — full programs over typed coordinate streams. So
why does it also turn out to be a near-zero-cost embedded
expression engine for every host crate in the project? What
contract does that emergence rest on, and what does the host
agree to in exchange?** This doc says: the contract emerges
from the substrate + compiler operating uniformly across all
input sizes; the host agrees to submit self-contained text
and receive typed values; the cost is the substrate's
ordinary slot-contract overhead, which is small when the
expression is small.

---

## 1. The claim

Polydat's grammar is its own expression engine. The same
machinery that compiles a 200-line workload kernel compiles
a four-character expression like `"k+1"`. The substrate's
slot contract holds at every scale; the compiler's passes
fire uniformly; the result is a typed value the host
consumes.

> **For any text the polydat grammar accepts, the host crate
> can ask polydat to evaluate it against an optional context
> and receive a typed `Value` (or a typed list of `Value`).
> The host inherits the full node library, the lifecycle
> classification, the typed slot contract, and the
> deterministic-evaluation guarantee — without writing a
> parser, an evaluator, or a type checker. The host's
> obligation is the text and (when needed) the context; the
> output is a typed `Value` answer.**

This is what was informally described as "polydat doubles as
an embedded expression engine." This doc names it as the
**Embedding Contract** — the host-facing utility surface
that emerges from the substrate + compiler.

The capability is not bolted on. It is the substrate
operating at small scale. No additional infrastructure
beyond what already exists for workload kernels is required;
the expression engine *is* the workload engine, just with
shorter input.

---

## 2. The expression-as-kernel correspondence

Every text input the host submits compiles to a
`PolydatProgram` — same shape, same compilation pipeline, same
slot contract, as a full workload kernel. The size of the
program is proportional to the input's complexity; an
expression like `"k * 2 + 1"` compiles to a three-node
program (two ops + a const-fold output binding).

```text
host text input            polydat compile pipeline
──────────────────         ──────────────────────────────────────
"k * 2 + 1"                Parse → Bind → Node Fusion → Topological
                           Sort → Hoisting analysis → Engine select
                           → Emit GkProgram (3 ops, 1 output)

                                            │
                                            ▼

host context (binding         materialize_subscope / set_inputs
for "k" as scope-input)       ─────────────────────────────────
                                            │
                                            ▼

                              evaluate via Context Fusion + cycle
                              clock → Value::U64(<result>)

                                            │
                                            ▼

                              host receives Value
```

The correspondence is total: every expression has a kernel
representation, and every kernel can be reduced to a
single-expression case if its body is a single binding. The
distinction between "expression" and "workload" is
quantitative (lines of input, count of bindings), not
qualitative (no different machinery).

This correspondence is what makes the expression engine
*free* — there's no separate engine to maintain. Any
improvement to the substrate or graph compiler improves the
expression engine automatically.

---

## 3. The host-facing surfaces

Three public surfaces, one for each evaluation depth.

### 3.1 `eval_const_expr` — const-fold at compile time

Location: [`crate::dsl::compile::eval_const_expr`].

Signature:

```rust
pub fn eval_const_expr(source: &str) -> Result<Value, EmbeddingError>
```

Semantics: compile the text wrapped as a single output
binding (`out := <source>`); pull the constant from the
compiled kernel; return it. The compilation succeeds iff
the expression is statically foldable — its upstream cone
reaches *no* dynamic inputs. The grammar's full surface is
available — node calls, literals, arithmetic, string ops —
but the expression's lifecycle must be Effectively-const
(per H1 / H2 / H3).

Hosts that want the host's Rust type back directly should
use `eval_const_expr_typed::<T>` (see §3 + §5.3); the
typed surface returns `Result<T, EmbeddingError>` and
removes the Value enum from the boundary.

Use case: host-side `{...}` config expressions, where the
host has a small expression and a guarantee it should
resolve at activity-construction time (no `cycle` reference,
no external-write inputs). The host wraps the result in its config-value
contract — typically calling `.as_u64()` for numeric fields
or `to_display_string()` for string contexts; the projection
is host policy, not part of the embedding contract.

**What works as a const expression:**

- Literals: `{42}`, `{3.14}`, `{"hello"}`
- Arithmetic: `{1000 * 1000}`, `{4 ** 0.5}`
- Function calls with constant args: `{hash(42)}`, `{mod(hash(42), 100)}`
- Catalog-registered metadata accessors (e.g.
  `{vector_count("example")}`)
- Nested: `{vector_count("{dataset}") / 10}` (after the host's
  param substitution pass — itself outside the embedding
  contract; see §6.4)

**What does NOT work:**

- References to cycle inputs: `{hash(cycle)}` → error
- References to undefined names: `{undefined_var}` → error
- Non-deterministic functions: `{counter()}` → error

Cost: one full compile + scope-init evaluation, ~ms scale.
The compile dominates; once compiled, the value is folded
into the program's static state and reading it is free.

Failure modes (returned as typed `Err(EmbeddingError)` per
§6 Error Ontology):
- `Parse` — parse error in the text.
- `LifecycleMismatch` — the expression's upstream reaches a
  dynamic input (the surface promises const-only; the chain
  returns "depends on runtime inputs").
- `NodeEvalPanic` — a node `eval` panics during scope-init
  (caught via `catch_unwind` and surfaced as a node-eval-panic
  error).

### 3.2 `interpolate_via_kernel` + evaluation — kernel-bound dynamic evaluation

#### 3.2.1 Why interpolation is text-level

The substrate holds scope state in typed slots accessible
to nodes inside a kernel's program. But host expression
text is *outside* any specific kernel program — it's text
the host is about to submit for compilation. To bridge
"text the host has" with "values the kernel holds,"
polydat exposes a **text-level** interpolation surface:
slot values are rendered to their display strings and
substituted into the text in place of `{name}` placeholders.
The output is text; the next step is ordinary
`eval_const_expr`.

This deliberately is **not** value-level injection
(where the host programmatically builds an expression tree
with bound values pre-substituted). Three reasons for the
text-level choice:

- **Grammar preservation.** Interpolation produces valid
  expression text; the eval step compiles ordinary text;
  the grammar is the contract at every stage. There's no
  separate "expression-with-bound-values" intermediate
  representation to maintain.
- **Decoupling of interpolation from eval.** A host that
  just wants placeholder substitution (e.g., to render a
  label string with kernel values) uses interpolation
  alone. A host with already-resolved text (no
  placeholders) uses eval alone. The two compose only when
  needed.
- **Lifecycle-gating naturally falls out at eval time.**
  Interpolation is type-erased (everything becomes a
  display string); the typed-and-lifecycle-checked
  evaluation happens at the eval step over the resulting
  text. If the post-interpolation text reaches a dynamic
  input that wasn't substituted away, `eval_const_expr`
  rejects it with a typed error.

The `{name}` surface is the *contract* for textual
embedding of slot values. Hosts can author expression text
freely with `{...}` placeholders; the kernel chain's slot
contract is the source of substitution; the eval step is
the typed result producer.

#### 3.2.2 The surfaces

Location: [`crate::kernel::interp::interpolate_via_kernel`].

Signature:

```rust
pub fn interpolate_via_kernel(
    text: &str, kernel: &PolydatKernel,
) -> Result<String, String>
```

Semantics: replace `{name}` placeholders in `text` with the
display string of `kernel.lookup(name)`. Returns the
substituted text.

The canonical two-step composition:

```rust
let interpolated = interpolate_via_kernel(text, &kernel)?;
let value = eval_const_expr(&interpolated)?;
let truth = value.as_bool();
```

Step 1 (interpolate) brings the kernel's bound values into
the text. Step 2 (eval) compiles the now-bound text and
folds it. The lifecycle gating is preserved at step 2: if
the post-interpolation text still reaches a dynamic input,
the eval rejects it; if every name was substituted to a
static value, the fold succeeds.

Use case: nbrs-runtime's predicate evaluation — the host
has a text like `"{k} > 5"` (where `{k}` is an iter-var
bound in the calling kernel) and needs a boolean answer.
The two-step composition resolves the placeholder and
folds the resulting `5 > 5` (or `7 > 5`, etc.) to a
boolean.

Cost: interpolation is O(name lookups + text length); the
follow-on `eval_const_expr` is a compile + fold (~ms
scale).

#### 3.2.3 Interpolation alone — text rendering without evaluation

Interpolation is useful as a standalone operation when the
host needs the *rendered text* but not an evaluated value.
The output is host-domain text (a filesystem path, an SQL
fragment, a log line, a keyspace name) — text whose
consumer is not polydat. The host calls
`interpolate_via_kernel` and uses the returned string
directly; no `eval_const_expr` follows.

**Worked example: rendering a per-iteration data path.**

Suppose the host has a path template that depends on the
current scope's iter-vars:

```text
"data/{dataset}/k{k}_limit{limit}.bin"
```

and the current kernel's bindings are
`dataset = "sift1m"`, `k = 10`, `limit = 100` (typical
post-Context-Fusion state in a `for_each (k, limit)` scope
running over a configured dataset). The host's code:

```rust
use polydat::kernel::interp::interpolate_via_kernel;

let template = "data/{dataset}/k{k}_limit{limit}.bin";
let path = interpolate_via_kernel(template, &kernel)?;
// path = "data/sift1m/k10_limit100.bin"

let bytes = std::fs::read(&path)?;
// host proceeds with the resolved path; no polydat
// evaluation needed.
```

Three things to notice:

- **The result is text, not a `Value`.** The host gets a
  `String` and uses it for a host-domain purpose
  (filesystem read). Polydat is the *renderer*, not the
  *consumer*.
- **No expression grammar required.** The template is not
  a polydat expression — it's a string with `{placeholder}`
  syntax. Polydat doesn't try to parse `"data/.../"` as a
  Polydat expression; the `{...}` form is the only syntactic
  surface interpolation cares about.
- **Lifecycle gating doesn't apply.** Since no eval
  follows, there's no `LifecycleMismatch` to fire. If a
  placeholder is unresolved, `interpolate_via_kernel`
  returns `UnresolvedPlaceholder` (per §6); the host
  surfaces it as a missing-binding diagnostic.

**Other standalone interpolation patterns:**

| Pattern | Template | Consumer |
|---|---|---|
| SQL fragment | `"SELECT * FROM {table} WHERE k = {k}"` | Adapter sending the SQL to a database; the host isn't asking polydat what `k = {k}` evaluates to, only what `{k}` renders as. |
| Log line | `"phase {phase_name} cycle {cycle} k={k}"` | Stderr or log buffer; the renderer's role is per-cycle string emission. |
| Cassandra keyspace name | `"recall_{model_lc}_{dim}"` | The schema-create op; the host uses the rendered name as the keyspace identifier. |
| Cache key | `"vec_{dim}/{partition}"` | A keyed lookup against a host-owned store. |

The composability principle (E6) says interpolation and
evaluation compose only when both are needed. Standalone
interpolation is the half of that composition that uses
just the substitution.

### 3.3 `evaluate_spec` — list-yielding evaluation against kernel

Location:
[`crate::iteration::comprehension::eval::evaluate_spec`].

Signature:

```rust
pub fn evaluate_spec(
    spec_text: &str, kernel: &PolydatKernel,
) -> Result<Vec<Value>, String>
```

Semantics: a layered evaluator that recognises multiple
clause-source forms — `all(cursor)`, range expressions
(`a..b`, `a..=b..s`), named generators
(`concat(...)`, `bucket(...)`, etc.), set operators on
lists, sequencer LUTs, and plain `eval_const_expr` as the
last fallback. Returns a vector of values per the
recognised form's expansion.

Use case: comprehension clause-source expansion (the source
of every `for_each k in <text>` clause). The host text can
declare a *list* of values, not just a single value, and
`evaluate_spec` does the expansion against `kernel`
context.

Cost: dominated by the recognition cascade (~us per cheap
form) + an `eval_const_expr` fallback for the literal-list
case (~ms).

### 3.4 The underlying surface: `compile_polydat`

Location: [`crate::dsl::compile::compile_polydat`].

Signature:

```rust
pub fn compile_polydat(source: &str) -> Result<PolydatKernel, String>
```

The full compilation entry point: text → compiled +
instanced `PolydatKernel`. The three higher-level surfaces above
are all built on this; the host can also reach for
`compile_polydat` directly when it wants a kernel rather than
just a value.

Use case: host crates that pre-compile expressions for
repeated evaluation. The kernel can be cached via
`Arc<GkProgram>` and re-instanced per fiber via SRD-67's
`from_program`.

Cost: one full compile (~ms scale for small expressions).
Subsequent re-instances are fast (the program is shared,
state is per-fiber).

---

## 4. The Embedding Contract — E-axioms

The host gets six guarantees in exchange for submitting
self-contained text. Each is a substrate / compiler
property at expression scale.

### Axiom E1 — Self-contained submission

**A host submits self-contained text (and optionally a
`&PolydatKernel` for context). Polydat does not reach for
ambient state, global registries-not-named-in-the-call, or
thread-local context. The submission is the input; the
return is the output; there is no third channel.**

Enforcement: the public function signatures themselves —
each is a pure function of its declared arguments + the
process-level node library (registered at startup, fixed
thereafter).

### Axiom E2 — Typed result

**The returned `Value` (or each element of a returned
`Vec<Value>`) carries a declared type per T1. The host
reads the type via `Value`'s typed accessors (`as_u64`,
`as_f64`, `as_str`, `as_bool`, etc.) or via pattern matching.
There is no untyped result.**

Enforcement: T1 (every slot typed) flows through the entire
compiler pipeline; the output binding's slot is typed; the
returned value's type is the slot's declared type. The
typed `Err(String)` for failure modes is symmetric — even
errors are typed (one variant of the `Result`).

### Axiom E3 — Bounded determinism via the Runtime Model

**For a fixed expression text, a fixed context, and a
fixed node registry, embedded evaluation produces a
deterministic typed return value (D1), with deterministic
side channels conditional on per-node metadata (D2), and
structurally bounded cost (D3). The full mechanism — data
flow, dependency tracking, node caching, invalidation, and
the state-layering contracts that compose them — is owned
by the [Runtime Model](runtime_model.md); §5 records how
those properties specialise to embedded eval.**

Enforcement: composition of the Runtime Model's R1–R3
(memoization, lazy pull-through, forward-only flow) with
the substrate's S/T/L axioms and the Graph Compiler's
H-axioms.

### Axiom E4 — Library inheritance

**Every node registered in `polydat::dsl::factories::GkRuntime`
(the default registry plus host-extension factories) is
available to embedded expressions. The host inherits the
full node catalog — hash, arithmetic, string, math,
distributions, datetime, noise, vector ops — without
declaring per-expression node availability.**

Enforcement: the compiler reads the runtime registry at
compile time. Host crates that extend the registry (e.g.,
nbrs-runtime registers runtime-context nodes; nbrs-metrics
registers Polydat metric nodes) make those nodes available to all
embedded expression evaluation.

### Axiom E5 — Lifecycle transparency

**The host chooses the evaluation depth that matches its
need: const-fold via `eval_const_expr` (the expression must
be Effectively-const), kernel-bound dynamic via
`interpolate_via_kernel` + eval (the expression sees the
kernel's bound state), or full compile via `compile_polydat` (the
host owns the resulting kernel for repeated evaluation).
Each surface preserves the substrate's lifecycle
classification — they differ in *which* lifecycle window
they evaluate against.**

Enforcement: the surfaces are distinct entry points with
distinct contracts. `eval_const_expr` rejects expressions
that reach dynamic inputs (typed error). The two-step
interpolate-then-eval composition handles dynamic-via-
kernel cases. `compile_polydat` exposes the full kernel for any
remaining use case.

### Axiom E6 — Composability via interpolation

**The interpolation surface (`interpolate_via_kernel`) +
evaluation surface compose. The host can use them as a
pipeline: text → interpolation → resolved text → evaluation
→ value. The composition's invariants are: interpolation
preserves text grammar (substitutions are syntactically
sound); evaluation operates on the post-interpolation text
under the same E1–E5 guarantees.**

Enforcement: interpolation's contract is text-to-text
(no semantic transformation; just placeholder replacement
via `kernel.lookup` + `Value::to_display_string`).
Evaluation's contract is text-to-Value. The two compose
naturally; the pipeline is the canonical host pattern.

### Axiom E7 — Typed error ontology

**Every failure mode the embedding surface produces is
classified into a typed `EmbeddingError` variant per the
error ontology in §6. The host pattern-matches on the
error to drive UX, recovery, or logging — there is no
stringly-typed escape hatch as part of the standard. The
current implementation returns `Result<_, String>` and the
ontology is being progressively retrofitted as a typed
enum at the surface; until that migration completes, hosts
may parse the string forms, but the contract reads from §6.**

Enforcement: §6 enumerates the variants exhaustively;
implementations of the surfaces are mandated to produce
errors that map to one variant. Migration to the typed
enum at the surface is tracked in §12.1.

---

## 5. The Embedding System Contract

This section is the canonical reference for the contract
between polydat and host crates that embed expression
evaluation. It establishes:

- what host and polydat each provide (§5.1)
- how types cross the boundary (§5.2)
- l-value type inference at the embedding surface (§5.3)
- type-matching adapter polyfills at the boundary (§5.4)
- virtual nodes — host-registered factory contributions
  (§5.5)
- virtual wires — context-fusion-conditioned host bindings
  (§5.6)
- how the [Runtime Model] applies to embedded expressions
  specifically (§5.7)

The contract is what makes the embedding capability load-
bearing: it's not "polydat happens to be usable as an
expression engine" but "polydat and the host share a typed,
mechanised contract whose terms are explicit."

### 5.1 The contract — what host and polydat each provide

The Embedding System Contract is bidirectional and has
**two engagement levels**: a baseline contract every host
must satisfy to use the surfaces at all, and an opt-in
strict contract a host can engage for stronger
compile-time type alignment.

#### 5.1.1 Polydat's obligations

Polydat's obligations to every host, regardless of
engagement level:

| Obligation | Discharged by |
|---|---|
| Typed result | T1+T2 (substrate) → E2 |
| Deterministic typed return | D1 (runtime model) → E3 |
| Library inheritance | E4 — every registered node is available |
| Lifecycle transparency | E5 — three surfaces for three depths |
| Typed error ontology | E7 + §6's `EmbeddingError` enum |
| Forward-only data flow | R3 (runtime model) — no surprise side channels |

These are unconditional. A host using only the baseline
contract gets all of these. The strict opt-in adds
guarantees on top; it does not remove any.

#### 5.1.2 Host's baseline obligations

The minimum a host must do to use the surfaces:

| Obligation | Required surface |
|---|---|
| Self-contained text | A `&str` submitted to one of §3's surfaces |
| Context (when needed) | A `&PolydatKernel` for kernel-bound evaluation |
| Registry contributions (when needed) | Factory registration before evaluation |

That's it. A baseline-only host calls a surface, receives
a `Value` (or `Result<Value, EmbeddingError>`), and
handles the value however it likes — typed accessor,
pattern match, or even string-display rendering. The host
takes responsibility for any type expectations it imposes
on the result (accessor panics, mismatch handling).

Two host crates in the workspace operate at this level
today: nbrs-runtime's predicate evaluation reaches for
`.as_bool()` post-hoc; nbrs-workload's parameter
evaluation reaches for `.as_u64()` post-hoc. Both work
correctly because the host has out-of-band knowledge of
the expected type.

#### 5.1.3 Host's opt-in strict contract

A host that wants polydat to enforce type alignment at
*kernel compile time* engages additional obligations in
exchange for additional guarantees. The opt-in surface
is the typed embedding API (§5.3, planned):

| Opt-in obligation | What polydat guarantees in return |
|---|---|
| Declare the expected return type via `eval_const_expr_typed::<T>` | Compile-time check: expression's output type matches `T` or is healable via the catalog (§5.4) |
| Use the typed accessor on the unwrapped Rust value | No accessor panic risk — the result is a Rust `T`, not a `Value` |
| Treat `TypeMismatch` errors as compile-time signals | Error variant fires at embed-call rather than at downstream use |

The opt-in is a *contract upgrade*, not a separate
contract. A host can use the baseline surfaces alongside
the opt-in surfaces in the same crate — different sites
can engage different levels.

**Why opt-in, not mandatory.** Some hosts have legitimate
reasons to operate at the baseline:

- Hosts that compose expressions whose return type
  varies across calls (e.g., a generic configuration
  evaluator that may return `U64`, `Str`, or `Bool`
  depending on the configuration key).
- Hosts that already have their own type-coercion layer
  and just want polydat's value as input.
- Hosts wrapping polydat for an interpreted-language
  binding (e.g., a Python embedding) where Rust's static
  typing isn't the boundary.

The opt-in keeps these hosts welcome at the baseline
while letting Rust-native hosts that want stricter
compile-time guarantees opt into them.

#### 5.1.4 Shared vocabulary

Both engagement levels rest on a shared vocabulary:

| Shared element | Role |
|---|---|
| `Value` enum | The carrier type for all typed return values |
| `PortType` enum | The type vocabulary for slot declarations and value classifications |
| `{name}` placeholder syntax | The textual surface for interpolation |
| The grammar | The expression-text language both produce/consume |

This shared vocabulary is the contract's *currency*. A
host wishing to speak the substrate's type system — at
baseline or strict level — uses these exact types and
syntaxes. Polydat exports them; host crates depend on
the polydat crate and import them directly. There is no
opaque value, no host-side type that polydat treats as
a black box, no syntactic surface other than what the
grammar declares.

Hosts that want to speak Polydat's `Value` type system
deeply (e.g., constructing `Value`s programmatically,
pattern-matching exhaustively, contributing virtual
nodes per §5.5 that produce typed values) are explicitly
*allowed and supported*. The substrate's type vocabulary
is public; deep host integration is a first-class
pattern, not a workaround.

### 5.2 Types at the embedding boundary

Every value crossing the boundary is typed. There is no
untyped slot, no untyped return, no untyped error in the
contract. The type vocabulary is `PortType` (declarations)
and `Value` (runtime carrier); the two are isomorphic in
the sense that every `Value` has a `port_type()` method
returning the matching `PortType` and every `PortType` has
a non-empty set of `Value` variants that satisfy it.

**Boundary type checks:**

- **Inputs (host → polydat):** the host's submitted text
  must be parseable as expression text whose result wire
  has a `PortType`. The compiler infers this from the
  expression's structure (T1+T2). If the host supplies a
  kernel context with bindings whose types are wrong for
  the slots the expression declares (e.g., slot expects
  `U64`, binding is `Str`), Node Fusion's adapter insertion
  (NF + T2) heals it if an adapter exists; otherwise the
  compiler emits `EmbeddingError::TypeMismatch`.

- **Outputs (polydat → host):** the typed `Value`
  returned to the host carries its `PortType` via the enum
  variant. The host accesses it through typed accessors
  (`Value::as_u64`, `Value::as_f64`, etc.) or
  pattern-matching. Strict accessors panic on type
  mismatch; non-strict accessors (`try_as_*`) return
  `Option`.

- **Errors (polydat → host):** the `EmbeddingError` enum
  (§6) is itself typed — every error class is a
  discriminable variant, not a stringly-typed message.

The boundary is type-strict in both directions. The
shared `Value` / `PortType` vocabulary makes the strictness
implementable without per-call negotiation.

### 5.3 L-value type inference

The current embedding surfaces are *result-typed*: polydat
returns a `Value`, and the host applies a typed accessor
post-hoc:

```rust
let result_value = eval_const_expr("k > 5")?;
let truth = result_value.as_bool();  // post-hoc accessor
```

The host's *expected* type (`bool` in the example) is not
visible to polydat at compile time. Polydat compiles the
expression, returns whatever `Value` variant the
expression produces, and the host coerces via accessor.

**The contract's planned extension — l-value-driven
inference:**

A future revision of the embedding surfaces adds a typed
entry point:

```rust
let truth: bool = eval_const_expr_typed::<bool>("k > 5")?;
```

Here the type parameter `bool` drives compilation: polydat
knows the host expects a `Value::Bool`, the compiler
enforces that the expression's output `PortType` matches
(or is healable to) `Bool`, and the return is a Rust
`bool` (not a `Value`). Type-mismatch surfaces as
`EmbeddingError::TypeMismatch` at compile, not as a
runtime panic from `.as_bool()`.

The mechanism for inference:

- The host's type parameter selects a target `PortType` via
  a `HostType` trait (one impl per Rust type that has a
  natural polydat correspondence).
- Compilation runs as ordinary; the resulting output
  wire's `PortType` is compared against the target.
- If they match: return the unwrapped Rust value.
- If they mismatch but a return-path adapter exists (per
  §5.4): apply the adapter, return.
- Otherwise: typed error.

L-value type inference moves the type contract from
*runtime accessor panic risk* to *compile-time check at
embedding*. The current accessor pattern remains supported
(for hosts that want the `Value` for other reasons), but
the typed surface becomes the recommended path.

### 5.4 Type-matching adapter polyfills at the boundary

The substrate's T2 axiom says type mismatches between
adjacent wires are healed by auto-inserted edge adapters.
The Graph Compiler's Node Fusion pass (§5.3 of
[graph_compiler.md]) implements this via the catalog of
known conversions in [`library::convert`]:
`U64ToString`, `F64ToString`, `U64ToF64`, `JsonToStr`,
etc. Each catalog entry is itself a `PolydatNode` with declared
input and output `PortType`s; Node Fusion's
adapter-insertion rule inserts the appropriate adapter
node when a wire's source type differs from its
consumer's expectation in a way the catalog can heal.

#### 5.4.1 The current catalog — intra-graph only

The catalog operates at exactly one site today:
**intra-graph wire validation during assembly**. The
assembler (`compile::assembly::resolve`) walks each wire,
checks the source's output `PortType` against the
consumer's input `PortType`, and:

- If they match exactly → no adapter needed.
- If they mismatch but a catalog adapter exists → insert
  the adapter as an intermediate node, rewriting the wire
  to pass through it.
- If they mismatch and no catalog adapter exists → fail
  with `AssemblyError::TypeMismatch`.

This site is the *only* one the catalog currently
supplies. Every other tier of polydat operation (Context
Fusion, embedding boundary, return-path coercion) either
doesn't exist as a tier yet or operates without
adapter-catalog support, surfacing type mismatches as
errors rather than healing them.

#### 5.4.2 Planned extension — two additional polyfill sites

The substrate-consistent move: extend the same catalog to
operate at two additional sites that match Context
Fusion's structural boundaries.

**Input-binding adapters (planned).** When the host's
context kernel has a binding `k: F64` and the expression's
extern slot declares `k: U64`, the boundary should insert
the catalog's `F64ToU64` adapter at the synthesis site.
This is **Context Fusion's slot-filling step (S2) with
type-coercion**: instead of failing on type mismatch, the
chain consults the catalog and applies the same rule the
intra-graph case uses.

Current state: Context Fusion's `materialize_wiring_from_outer`
does not invoke the catalog. A binding-type mismatch
either silently coerces via the value's bitwise
representation (for u64/f64 cases) or surfaces as a wire
error at first read. The planned change: at the synthesis
boundary, the chain consults the catalog and either
applies the adapter or surfaces a typed `TypeMismatch`
error before the kernel is fully bound.

**Return-path adapters (planned, pairs with §5.3's
l-value-typed surface).** When the host calls
`eval_const_expr_typed::<bool>` and the expression
produces `U64`, the boundary applies the catalog's
`U64ToBool` rule. The host's contract receives `bool`
without an accessor panic risk.

Current state: no return-path adapters exist. The host
calls `.as_bool()` post-hoc and accepts panic risk on
mismatch (or pattern-matches the `Value` directly). The
planned change: when the typed embedding surface is added
(§5.3), the boundary consults the catalog at return time
and applies the adapter that brings the expression's
output type to the host's target type.

#### 5.4.3 The contract's rules for boundary polyfills

Across both planned sites and the existing intra-graph
site, the rules are uniform:

- **Only catalog adapters apply.** No silent generic
  coercion. `U64` → `Str` uses `U64ToString` (in the
  catalog); `Bytes` → `Str` is not in the catalog and
  surfaces as `TypeMismatch`. The host knows what's
  healable by reading the catalog.
- **Lossy conversions are explicit.** The catalog
  declares per-entry whether the conversion preserves
  value identity (e.g., `U64` → `Str` is lossless;
  display-round-trippable) or is lossy (e.g., `F64` →
  `U64` truncates). Hosts can opt out of lossy
  conversions via a strict-mode embedding flag (planned,
  paired with the typed surface).
- **Polyfill insertion is observable.** The compiled
  program records which adapters were inserted and at
  which sites. Hosts that want to diagnose unexpected
  coercion can query the program's adapter-insertion log
  (currently exposed by the assembler; not yet wired to
  boundary sites).
- **The catalog is the single source of truth.** New
  conversion needs are added to the catalog *once*. After
  registration, the new conversion is available at every
  adapter site uniformly — intra-graph today, plus the
  two planned boundary sites.

#### 5.4.4 What this means for the spec

The §5.4 contract is the spec's normative position. The
current implementation supports the intra-graph site
only; the two boundary sites are planned. Treating the
boundary extension as the spec means:

- New host code can assume the boundary will heal type
  mismatches via the catalog and write against that
  expectation; until the boundary sites land, hosts hit
  `TypeMismatch` errors and treat them as
  not-yet-implemented.
- Catalog additions should serve all three sites by
  construction (input-binding, intra-graph, return-path).
  A new adapter that's only valid at one site is a
  catalog-design smell.
- §12.4 ("Embedded compilation of host source text") and
  §12.7 ("L-value-typed embedding surface") are
  prerequisites; the boundary-adapter extension can land
  alongside them.

### 5.5 Virtual nodes — host-registered factory contributions

**Status: shipped.** Host crates contribute `PolydatNode`
implementations to the runtime registry; from the
substrate's perspective these are indistinguishable from
built-in nodes and obey the full slot contract.

The host registers a factory contribution with
[`PolydatRuntime`](../../src/dsl/factories.rs) before
embedding evaluation begins. Each contribution declares
one or more `PolydatNode` implementations whose `eval`
delegates to host code:

```rust
pub trait HostFactory {
    fn nodes(&self) -> Vec<(NodeName, Box<dyn GkNode>)>;
}

// At process start:
runtime.register_factory(MyHostFactory { … });
```

From the substrate's perspective, host-contributed nodes
are *indistinguishable* from built-ins — they have
declared `PortType`s, declared `commutativity`, declared
JIT eligibility, and their `eval` is invoked through the
ordinary trait surface. The slot contract (T1+T2) and the
runtime model (R-axioms) hold uniformly.

Examples of current host contributions:

| Host crate | Virtual node | Purpose |
|---|---|---|
| nbrs-runtime | `runtime_context` family | Surfaces per-cycle activity state (current op name, scope path, etc.) as typed input values to Polydat expressions. |
| nbrs-metrics | Polydat metric nodes | Surfaces metric values (counters, gauges) to Polydat predicates without leaking metric infrastructure into polydat. |
| Adapters | Driver-aware nodes | A CQL adapter might register a `cql_table_exists` predicate node usable in workload expressions. |

The host's only obligation: register the factory before
the first evaluation. After that, the contributed nodes
are part of the substrate's vocabulary uniformly. The
runtime model's R-axioms apply to host-contributed nodes
exactly as they do to built-ins — same per-generation
memoization, same forward-only flow, same determinism
classification per the node's declared metadata.

This is the host's *shallow* integration point — extending
the node vocabulary. Virtual wires (§5.6) are the *deeper*
integration point, extending the synthesis vocabulary.

### 5.6 Virtual wires — context-fusion-conditioned bindings

Where virtual nodes (§5.5) extend the *node vocabulary*,
virtual wires extend the *synthesis vocabulary*. The host
interposes at Context Fusion's slot-filling step: when
polydat's auto-extern discovers a slot the outer scope
can't satisfy from its direct bindings, the host's
resolver fires and may provide the value.

The host registers a resolver via
[`crate::dsl::factories::register_extern_resolver`]:

```rust
register_extern_resolver(Box::new(|slot_name, slot_type| {
    // Host-mediated resolution; returns Option<Value>.
    // If Some, Context Fusion fills the slot with the
    // returned value; if None, falls through to ordinary
    // resolution (typed error if no binding exists).
}));
```

Use case: the host might want `{cluster_metadata.region}`
references in workload expressions to resolve via a
host-side configuration lookup, not via the kernel chain.
A virtual-wire resolver does this — at scope-init, polydat
sees the extern slot, the resolver fires, the slot is
filled with the host-computed value, the expression
evaluates as ordinary.

#### 5.6.1 Why this is a distinct integration tier

Virtual wires differ from virtual nodes in three ways
that matter for the substrate:

- **Timing.** Virtual nodes fire at *evaluation* time
  (their `eval` runs per cycle, like any node). Virtual
  wires fire at *synthesis* time (the resolver runs at
  scope-init, the result is frozen for the scope's
  lifetime per S3).
- **Surface.** Virtual nodes appear in the expression
  *text* (the workload author writes `runtime_context()`
  somewhere). Virtual wires appear as *bindings* the
  expression text references via `{...}` — the resolution
  is invisible to the expression author.
- **Substrate role.** Virtual nodes are consumers of the
  slot contract (they read inputs, write outputs).
  Virtual wires are *contributors* to the slot contract
  (they fill slots that auto-extern declared).

This is why virtual wires are a deeper integration point
— the host becomes a *participant* in S2 (binding-time
materialisation), not just a consumer of S1 (auto-extern
discovery). The substrate's S1 axiom currently assumes
the synthesis source is the outer kernel's chain alone;
virtual wires extend this to "outer chain *or*
host-resolver."

#### 5.6.2 The planned contract for virtual-wire resolvers

The resolver's contract must preserve every substrate
axiom for the slot it fills:

- **T1, T2 (typed slots).** The resolver MUST return a
  typed `Value` matching the slot's declared `PortType`,
  or `None` to fall through. Returning a value of the
  wrong type is a typed error
  (`EmbeddingError::TypeMismatch`).
- **S3 (cycle clock).** The resolver fires at scope-init
  time only. It cannot re-condition the slot per cycle.
  If the host needs per-cycle resolution, a virtual node
  (§5.5) is the correct surface.
- **L1 (each layer owns its state).** The resolver does
  not see other layers' state outside the kernel context
  passed as its argument. The context is the synthesis
  envelope at this scope-init.
- **D1 (typed-return determinism).** The resolver MUST
  be deterministic in its inputs (slot name + type +
  context). Polydat's D1 holds conditional on this; a
  non-deterministic resolver breaks D1 for any expression
  that consumes its slot.

#### 5.6.3 Composition with virtual nodes

Virtual wires fill slots; virtual nodes consume them.
The composition is uniform: a virtual node's `eval`
sees its input slots filled per the ordinary contract —
whether the slot was filled by the outer chain, by a
virtual wire resolver, or by an ordinary binding, the
node consumes a typed `Value` from the slot. The slot
contract is the abstraction barrier.

#### 5.6.4 What this enables

The host's deepest integration point: the host becomes a
*participant* in the substrate's synthesis surface, not
just a consumer of its output. Concrete capabilities the
planned mechanism would enable:

- **Host configuration injection.** Workload expressions
  reference `{config.region}` or `{config.dataset_path}`;
  the resolver looks up host configuration and fills the
  slot.
- **External-system bindings.** Workload expressions
  reference `{environment.kafka_brokers}`; the resolver
  reads environment / mounted-config / discovery service
  and fills the slot.
- **Lazy / on-demand resolution.** The resolver can defer
  expensive lookups until the slot is actually needed
  by the expression (Context Fusion only invokes the
  resolver for slots auto-extern discovered).

These are use cases that *currently* require workload
authors to thread configuration through workload params
explicitly — the planned mechanism lets the host
inject them at the substrate boundary instead.

### 5.7 The runtime model applied to embedded expressions

The mechanism by which embedded expressions execute — data
flow along wires, dependency tracking, per-generation node
caching, lazy pull-through invalidation, and the
determinism guarantees the runtime delivers — is the
subject of the [Runtime Model](runtime_model.md). That doc
owns the R-axioms (R1 memoization, R2 lazy pull-through,
R3 forward-only flow) and the D-axioms (D1 typed-return
determinism, D2 side-channel determinism, D3 cost
determinism). E3's guarantee is the realisation of
D1/D2/D3 at expression scale.

#### 5.7.1 The embedded expression's kernel is its own scope tier

Per the Runtime Model's L1 realisation (per-fiber `PolydatState`),
an embedded expression's kernel is its own scope tier
owned by the host call. The kernel's `PolydatState` is not
shared with the host's other state; the kernel's program
is `Arc<GkProgram>`, sharable across fibers if the host
caches it.

The host context (passed as `&PolydatKernel`) is the **outer
scope** for the embedded expression. Context Fusion (per
the Graph Compiler) populates the expression kernel's
extern slots from the context kernel's bindings at
scope-init — including any virtual-wire resolutions per
§5.6 (planned).

#### 5.7.2 Cone size is small — cost stays small

D3 (cost determinism) gives the host a structural cost
prediction: cone size × node-eval cost per generation. For
embedded expressions, cone size is typically small —
single-digit nodes for a one-line expression, low-double-
digits for a complex predicate. This is what makes the
embedding cost predictable in practice: a host pattern of
"compile-once, evaluate-many" pays a one-time compile cost
plus per-evaluation cost bounded by a small cone.

#### 5.7.3 External-write-aware embedding patterns

Per the Composition Substrate's S4 (external-write
synthesis as the open-granularity fill path), an embedded
expression that consumes externally-written wire values
must be evaluated *after* the host has written the value
into the slot. The host pattern:

```rust
// host writes the value into the slot
state.set_port_value("recall_at_k", recall_value);

// NOW the expression sees the written value
let value = eval_const_expr_against(&kernel, "{recall_at_k} >= 0.8")?;
```

If the host calls eval **before** the slot is written, the
slot holds `Value::None` (or the slot's default); per the
None-propagation contract, None propagates through the
expression and produces a `NonePropagated` error on the
host's strict accessor (per §6's ontology).

Hosts that consume this pattern give their write events
host-specific names — nbrs's *capture* is one such pattern
that writes op-result values into externally-written slots
between op executions — but the polydat embedding contract
sees only the generic external-write mechanism.

#### 5.7.4 Cross-host determinism — what hosts share

Two host crates evaluating the same expression text against
the same kernel context get the same typed return value
(D1). This is the load-bearing property that lets
expression evaluation be a shared utility across the
workspace: nbrs-runtime and nbrs-workload calling
`eval_const_expr` on `"{k} * 2 + 1"` with the same kernel
get identical `Value::U64`s, every time, in every fiber.

D2 (side-channel determinism) is more nuanced: if the
expression includes a diagnostic node (`log_info`,
`log_debug`), the resulting log output is deterministic
per the impure node's declared semantics. Hosts that
share a sink for diagnostic output observe deterministic
emission *per node*, with combined output ordering
governed by the diagnostic node's per-eval invocation
order — which is itself deterministic from R3 (forward-
only flow along the wire chain).

For the canonical formal statement of these properties,
see [Runtime Model §6 (D-axioms)](runtime_model.md).

---

## 6. The Error Ontology

The embedding surface produces errors in eight distinct
classes. Each carries enough context for the host to render
a meaningful diagnostic and (where applicable) suggest
remediation. The ontology is normative for the standard;
the current `Result<_, String>` surface uses descriptive
prefixes that map to these variants until the typed enum
lands.

```rust
pub enum EmbeddingError {
    /// Text could not be parsed as polydat expression source.
    /// The lexer or parser rejected the input before any
    /// semantic analysis.
    Parse {
        source: String,
        message: String,
        position: Option<usize>,
    },

    /// A `{name}` placeholder in the text had no matching
    /// binding in the kernel chain. Produced by
    /// `interpolate_via_kernel` only.
    UnresolvedPlaceholder {
        name: String,
        source: String,
    },

    /// The expression's upstream cone reaches a dynamic
    /// input, but the requested evaluation surface requires
    /// effectively-const lifecycle. Produced by
    /// `eval_const_expr` (directly or via the two-step
    /// composition).
    LifecycleMismatch {
        source: String,
        dynamic_inputs: Vec<String>,
    },

    /// A node mentioned in the expression is not registered
    /// in the runtime. Includes a Levenshtein-suggested
    /// alternative when the name is close to a known node.
    UnknownNode {
        name: String,
        source: String,
        suggestion: Option<String>,
    },

    /// The expression's wire chain has a type mismatch that
    /// auto-adapters cannot heal. Produced by the assembly
    /// pass during compilation.
    TypeMismatch {
        from_node: String,
        from_type: PortType,
        to_node: String,
        to_type: PortType,
        source: String,
    },

    /// A node's `eval` panicked during scope-init evaluation.
    /// The kernel's `catch_unwind` boundary captured the
    /// panic; the message is the panic payload's
    /// human-readable form.
    NodeEvalPanic {
        node_name: String,
        message: String,
        source: String,
    },

    /// Compilation succeeded but the requested output name
    /// could not be resolved in the resulting kernel.
    /// Indicates an internal compiler issue or a mismatch
    /// between the wrapper template and the compiler's output
    /// naming.
    ResultMissing {
        output_name: String,
        source: String,
    },

    /// A `Value::None` propagated to the expression's output
    /// when the host called a strict accessor (`as_bool` on
    /// `Value::None`, etc.). Produced at the host's
    /// accessor call, not by polydat directly — but
    /// classified here because the propagation is a polydat
    /// semantic. See SRD-74.
    NonePropagated {
        accessor: &'static str,
        source: String,
    },

    /// Evaluation exceeded a host-specified time budget.
    /// Currently produced only by deadline-accepting
    /// surfaces (none of the existing surfaces accept one;
    /// reserved for the bulk-evaluation surface §12.3 and
    /// for adapter-specific embedding paths that wrap
    /// the standard surfaces with their own deadline).
    Timeout {
        source: String,
        elapsed_ms: u64,
        deadline_ms: u64,
    },

    /// The runtime node registry (`GkRuntime`) is in a state
    /// where required factories were not registered before
    /// the embedding call. Indicates a host-side
    /// initialisation-order bug (a factory that should have
    /// been registered at process start wasn't). Includes
    /// the list of node names that the expression referenced
    /// but couldn't resolve due to registry incompleteness.
    RegistryNotInitialised {
        missing: Vec<String>,
        source: String,
    },
}
```

### 6.1 Variant guide

| Variant | When it fires | Host remediation |
|---|---|---|
| `Parse` | Lexer/parser rejects the input. | Surface the parse position to the user; the input is malformed expression text. |
| `UnresolvedPlaceholder` | `{name}` has no binding in the kernel chain. | Check the kernel's `scope_coordinates()`; suggest declaring the name as a workload param or fixing the spelling. |
| `LifecycleMismatch` | `eval_const_expr` was called on text reaching a dynamic input. | Either: (a) use the two-step interpolate-then-eval pattern to resolve dynamic names, or (b) accept the expression must be evaluated per-cycle via `compile_polydat` + cycle dispatch. |
| `UnknownNode` | A node call uses a name not in the registry. | If `suggestion` is `Some`, render it; otherwise tell the user to check the available node catalog. Host crates that register custom nodes should ensure registration happens before evaluation. |
| `TypeMismatch` | Wire types incompatible and no auto-adapter exists. | Surface the from/to node and types; suggest inserting an explicit conversion (e.g., `u64_to_str(x)`) or using a different node. |
| `NodeEvalPanic` | A node panicked during scope-init. | Surface the panic message; this is typically a node-internal contract violation (invalid argument range, etc.). Forwarded to the user with provenance. |
| `ResultMissing` | The compiler completed but the wrapper output isn't reachable. | Internal — report as a bug. Should not occur under correct surface usage. |
| `NonePropagated` | Host called a strict accessor on `Value::None`. | Either use a non-strict accessor (`try_as_*`) or surface the None to the user with context about which input was missing. See SRD-74. |
| `Timeout` | Evaluation exceeded host-specified deadline. | Surface the elapsed/deadline to the user. Either widen the deadline or refactor the expression to reduce work; investigate whether the embedded expression's complexity is unexpectedly large. |
| `RegistryNotInitialised` | Expression references nodes not present in the runtime registry. | Internal — usually indicates a host initialisation-order bug. Ensure the relevant factory is registered before evaluation; if the missing nodes are surprising, audit the registry construction sequence at process start. |

### 6.2 Provenance

Every variant carries the source text that produced the
error. Host crates that wrap embedded evaluation should
*also* record (a) the file or YAML key the source text
came from, and (b) the calling host context (e.g.,
"workload `query` op, field `where`"). Together these
form the diagnostic chain: polydat owns the polydat-layer
error variant; the host owns the host-layer location and
naming.

---

## 7. Use case catalog

Current host consumers, mapped to the surfaces they reach
for:

| Host site | Surface | Pattern |
|---|---|---|
| **nbrs-runtime** `executor.rs:1433` | `interpolate_via_kernel` + `eval_const_expr` | Predicate evaluation for `if:` / `where:` conditions in op fields. Text is e.g. `"{recall_at_k} >= 0.8"`; kernel is the phase scope. |
| **nbrs-runtime** `executor.rs:2351` | `eval_const_expr` directly | Inline constant evaluation in op-template field text (SRD-14 `{...}` form). |
| **nbrs-runtime** `runner.rs:3661` | `eval_const_expr` | Parameter expression evaluation in workload context (e.g., a `--params` overlay containing arithmetic). |
| **nbrs-runtime** `scope.rs:776` | Same two-step pattern | Children inherit counter bindings; the host text references them via `{...}` and the pattern evaluates against the scope's kernel. |
| **polydat** `iteration::comprehension::eval::evaluate_spec` | List-yielding evaluator | Comprehension clause-source expansion. The host text (e.g., `"1, 2, 4, 8"` or `"{kvs}"`) becomes a value list. |
| **polydat** `iteration::comprehension::eval::pre_evaluate_clause` | `evaluate_spec` | Pre-iteration evaluation of clause sources for early validation / dryrun. |

The pattern across consumers: small text in, typed value
(or value list) out. The host writes neither parser nor
evaluator; polydat handles both.

---

## 8. Why this works — substrate + compiler reused

The expression engine is not a separate system. It is the
substrate's slot contract holding at small scale and the
graph compiler's passes firing on small input. Every
guarantee in §4 is a substrate or compiler property already
formalised in the companion docs; this section names which
property each E-axiom rests on.

| E-axiom | Underlying substrate / compiler property |
|---|---|
| E1 (Self-contained submission) | Compiler entry-point design — pure functions of declared inputs + the process-level node registry. |
| E2 (Typed result) | Substrate T1 (every slot typed) → compiler emits typed output → value carries declared type. |
| E3 (Deterministic evaluation) | Substrate T1+L2 (typed deterministic lifecycle) + compiler H3 (hoisting preserves value) + NF2 (fusion preserves determinism). |
| E4 (Library inheritance) | Compiler reads the runtime registry; registry extension via `PolydatRuntime` is process-level and uniform. |
| E5 (Lifecycle transparency) | Substrate L2 (two-lifecycle classification) + compiler hoisting analysis (§3 in graph_compiler) → surfaces match each lifecycle window. |
| E6 (Composability via interpolation) | Substrate S1+S2 (synthesis surface) + compiler Context Fusion → interpolation is text-level access to bound slot values. |

Every property the host depends on for embedded evaluation
is a property the substrate or compiler already provides for
workloads. The "free expression engine" property is the
substrate's claim that the contract holds at every scale.

---

## 9. The composition pattern

The canonical host pattern for kernel-bound evaluation:

```rust
use polydat::kernel::interp::interpolate_via_kernel;
use polydat::dsl::compile::eval_const_expr;

fn evaluate_predicate(
    text: &str, kernel: &PolydatKernel,
) -> Result<bool, String> {
    let resolved = interpolate_via_kernel(text, kernel)?;
    let value = eval_const_expr(&resolved)?;
    Ok(value.as_bool())
}
```

The two-step composition has these properties:

- **Interpolation is text-preserving.** `{name}` becomes the
  display string of `kernel.lookup(name)`. The
  post-interpolation text remains grammatically valid as a
  polydat expression.
- **Evaluation is interpolation-agnostic.** `eval_const_expr`
  doesn't know the text was interpolated; it just compiles
  what it receives.
- **Lifecycle gating moves to the eval step.** If the
  post-interpolation text still references a dynamic input
  (e.g., interpolation didn't substitute everything, or the
  remaining names are dynamic-bound), eval returns a clean
  error.
- **The two steps are reusable independently.** A host that
  wants raw interpolation (text → text) calls just the
  first; a host with already-resolved text calls just the
  second.

The pattern is what gives the host the *full* expressive
range without compromising the substrate's deterministic-
evaluation guarantee.

---

## 10. SRD cross-references and roles

| SRD / doc | Role under this declaration |
|---|---|
| [Composition Substrate](composition_substrate.md) | The slot contract that flows through expression evaluation. T1 + L2 are E2 + E3's enforcement layer. |
| [Graph Compiler](graph_compiler.md) | The compilation pipeline. Embedded evaluation IS this compiler with smaller input. Every E-axiom inherits from the compiler's per-pass guarantees. |
| [SRD-10](language_spec.md) | Grammar surface. The expression engine's full expressivity is SRD-10's grammar — nothing is held back. |
| [SRD-11](evaluation_model.md) | Const-binding contract. `eval_const_expr` (E5, §3.1) is the const-binding contract operating on a single output binding. |
| [SRD-14](expression_engine.md) | `{...}` config expression surface. One specific embedding pattern; this doc places it in the broader catalog. |
| [SRD-67](subcontext_construction.md) | `from_program` cache-and-rebind primitive. Hosts caching compiled expressions for repeated evaluation use this. |

---

## 11. Why this matters — host design implications

### 11.1 Hosts don't reinvent expression evaluation

Every host crate in the project that needs to evaluate
small expressions reaches for the polydat surfaces. None
build their own parser, evaluator, or type system. This is
a recurring cost saving — and a recurring consistency win.
Two host crates using the same evaluation surface produce
identical typed values for identical inputs; there's no
"my expression dialect differs from yours" mismatch.

### 11.2 Type safety propagates from substrate to host

T1 (typed slots) guarantees that any value returned to the
host carries a known type. Hosts that consume `Value` know
they're holding a typed datum, not an opaque string.
Pattern-matching on `Value` is exhaustive (the enum is
closed); type errors at the host boundary are compile-time
errors, not runtime mysteries.

### 11.3 Library extensions are shared across the workspace

When nbrs-runtime registers runtime-context nodes, every
embedded expression in every host crate gains access to
them automatically. There's no per-host opt-in; the
factory registry is process-level. This makes adding new
host capabilities (a new node, a new function category) a
single-touch operation that fans out to every consumer.

### 11.4 The cost is predictable

A compile + scope-init evaluation for a small expression is
~ms scale (dominated by compile). Compiling once + caching
via `from_program` reduces repeated evaluation to per-fiber
state setup + per-cycle dispatch — sub-microsecond per
evaluation for an interpreted (P1) or closure (P2) kernel.
Hosts that care about cost choose `compile_polydat` + cache;
hosts that don't care reach for `eval_const_expr` per call.

---

## 12. Open questions

### 12.1 Bulk-evaluation surface

When a host has N expressions over the same kernel context,
it currently issues N compile + eval cycles. A bulk
surface — `evaluate_many(&[&str], &PolydatKernel) ->
Vec<Result<Value, EmbeddingError>>` — could amortise some
compilation work (shared parser state, shared node lookups,
etc.). Profile-driven: only worth specifying if bulk
patterns dominate a measurable cost.

### 12.2 Embedded compilation of host source text

Several host consumers parse host-side data (YAML, JSON,
TOML) into strings that get passed through polydat
evaluation. The boundary between "host data parser" and
"polydat compiler" is informal. A future revision could
specify a typed `HostText` wrapper that records the
provenance of submitted text (source file, line number,
host parsing context) for better cross-crate error
reporting; this dovetails with §6.2's provenance
discussion.

### 12.3 Lazy / suspended compilation

The current surfaces compile eagerly. A "compile when first
evaluated" surface would let hosts cache compiled programs
for expressions that may or may not be evaluated. Specific
patterns where this would help: lazily-evaluated assertion
expressions, validation rules that fire only on specific
result shapes.

---

[`crate::ast`]: ../../src/ast.rs
[`crate::kernel`]: ../../src/kernel/mod.rs
[`crate::dsl::compile::eval_const_expr`]: ../../src/dsl/compile.rs
[`crate::dsl::compile::compile_polydat`]: ../../src/dsl/compile.rs
[`crate::dsl::factories::GkRuntime`]: ../../src/dsl/factories.rs
[`crate::kernel::interp::interpolate_via_kernel`]: ../../src/kernel/interp.rs
[`crate::iteration::comprehension::eval::evaluate_spec`]: ../../src/iteration/comprehension/eval.rs
