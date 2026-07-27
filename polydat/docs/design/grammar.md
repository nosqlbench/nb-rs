# The Polydat Grammar — Design

> **Superseded as the entry point (2026-06-28).** The definitive,
> read-top-to-bottom specification and guide for the Polydat surface
> language — with worked, machine-verified examples and a programmatic
> construction companion — is now
> [`polydat_grammar.md`](polydat_grammar.md). Start there. This document
> is **retained as the detailed formal appendix**: the verbatim EBNF
> productions, the complete type-inference rules, the full G-axiom
> statements, and the G-axiom composition diagram. `polydat_grammar.md`
> summarizes this material and cross-references back here for the formal
> detail; it is authoritative on any grammar-structural conflict.

**Subtitle:** The Grammar Substrate.

Formalises polydat's Polydat grammar as a substrate the other
three design docs depend on. Where SRD-10 describes the
language *prosaically*, this doc states the grammar's
productions formally and identifies the distinctive
properties that make the other docs' axioms possible. Names
the G-axioms: structural commitments the grammar makes that
the substrate, compiler, runtime, and embedding contracts
all rest on.

## Authoritative ownership declaration

This document is the **single authoritative reference** for
polydat's Polydat grammar's *structural properties* — the
formal productions, the type inference rules, and the
distinctive commitments (G-axioms) that make the
[Composition Substrate], [Graph Compiler], [Runtime Model],
and [Expression Engine] docs' axioms achievable at the
language level. SRD-10 owns the prosaic specification (the
DSL syntax, the parser pipeline, the type system); this
doc owns the *grammar-level invariants* the SRD's syntax
preserves. Apparent contradictions between SRD-10 and this
document resolve in favor of this document on grammar-
structural matters; SRD-10 remains authoritative on
specific syntax forms and on rejection rules.

## Companion documents

- [The Composition Substrate](composition_substrate.md) —
  S/T/L axioms over the slot contract. The substrate's
  axioms are achievable because the grammar guarantees
  G1 (auto-extern discovery), G3 (scope-chain
  transparency), and G4 (port-typed expressions).
- [The Graph Compiler](graph_compiler.md) — H/CF/NF
  axioms for construction. The compiler's hoisting analysis
  works because the grammar guarantees G5 (structural
  lifecycle classification); Context Fusion works because
  the grammar guarantees G1 (auto-extern).
- [The Runtime Model](runtime_model.md) — R/D axioms
  for runtime mechanics + determinism. D1 (typed return
  determinism) follows from G4 (port-typed expressions)
  composed with T1 (typed slots).
- [The Expression Engine](expression_engine.md) — E-axioms
  for embedding. E4 (library inheritance) works because
  the grammar guarantees G6 (single grammar for
  expressions and full programs).
- [SRD-10: Polydat Language and Compilation](language_spec.md)
  — prosaic specification. This doc complements SRD-10
  by formalising the grammar-level invariants SRD-10's
  syntax assumes.
- [SRD-11: Polydat Evaluation Model](evaluation_model.md)
  — two-lifecycle classification. G5 names this as a
  grammar-level commitment SRD-11's lifecycle taxonomy
  builds on.
- [SRD-13c: Polydat Scope Model](scope_model.md)
  — auto-extern + scope-chain composition. G1 and G3 are
  the grammar-level commitments SRD-13c's scope mechanism
  rests on.

The forcing question: **the substrate, compiler, runtime,
and embedding docs each make load-bearing claims that
ultimately reduce to "the grammar makes this possible."
What is the grammar's actual shape, what does it commit
to structurally, and what makes it an unusually capable
substrate for those four docs' axioms?** This doc says:
the grammar has six distinctive properties (the G-axioms),
each a structural commitment, and together they compose
into a small grammar that does an unusually large amount of
load-bearing work.

---

## 1. The claim

Polydat's Polydat grammar is unusual. Not in its individual
features — function calls, named arguments, lifecycle
modifiers, and field access are commonplace — but in the
*combination* of features and the *structural commitments*
that combination implies:

- Identifier references are *automatically* classified as
  local or outer-scope (G1).
- Lifecycle (effectively-const vs dynamic) is *declared at
  the syntactic surface* via wire modifiers (G2).
- Outer-scope references use the *same syntax* as
  local references (G3).
- Every expression has a *type-derivable output port*
  classifiable at compile time (G4).
- Lifecycle classification is *structural* — derivable
  from the wire chain alone (G5).
- A single expression and a full kernel program are
  *the same grammar* (G6).

These six commitments compose into the substrate that
makes the other three docs' axioms achievable. The
G-axioms are not optimisations; they are load-bearing
properties without which the substrate / compiler /
runtime / embedding stories would collapse.

The grammar is small (one expression type with seven
constructors, six statement types) but does an unusual
amount of work. This is the "unusually capable substrate"
the focal-point treatment names.

---

## 2. The grammar productions

The grammar in formal (EBNF-ish) form. Where SRD-10
describes each form prosaically, this section lays the
productions out for cross-reference.

### 2.1 Top-level structure

```ebnf
polydat_file   ::= statement*

statement      ::= input_decl
                |  binding
                |  module_def
                |  extern_port
                |  cursor_decl
                |  pragma
```

A polydat `.polydat` source is a sequence of statements. The
grammar does not commit to a particular ordering between
statement kinds at the syntactic level; lifecycle and
dependency analyses (per [Graph Compiler]'s passes) order
the resulting graph.

### 2.2 Inputs and externs

```ebnf
input_decl     ::= "input" ident (":" type)?
                |  "input" "(" (ident (":" type)?)+ ")"

extern_port    ::= "extern" ident ":" type ("=" expr)?
```

Inputs are per-cycle kernel input slots (driven by
`set_inputs`). Externs are slots populated by the
chain (manifest values from outer scopes) or written by
external producers (per composition_substrate S4).

The distinction is structural: an `input` declares a slot
that *advances* per cycle (the cycle clock S3 of the
substrate); an `extern` declares a slot that is *fixed*
per scope-init via Context Fusion (S1+S2).

### 2.3 Bindings

```ebnf
binding        ::= modifier* ident ":=" expr
                |  modifier* "(" ident ("," ident)* ")" ":=" expr

modifier       ::= "const"
                |  "shared"
                |  "volatile"
```

The bare form (no modifier) is per-cycle dynamic. The
modifiers declare lifecycle (`const`) or sharing
semantics (`shared`, `volatile`).

The tuple form (`(a, b) := expr`) is destructuring sugar:
the expression must produce a tuple-typed value; each
identifier binds the corresponding element.

### 2.4 Module definitions

```ebnf
module_def     ::= ident "(" typed_param_list ")"
                   ("->" "(" typed_param_list ")")?
                   ":=" "{" statement* "}"

typed_param    ::= ident ":" type
```

A module is a named, typed reusable computation unit. Its
parameters are declared input slots; its outputs are
declared output ports. The body is a nested sequence of
statements.

### 2.5 Expressions

```ebnf
expr           ::= ident
                |  int_literal
                |  float_literal
                |  string_literal
                |  array_literal
                |  call_expr
                |  bin_op_expr
                |  unary_expr
                |  field_access

call_expr      ::= ident "(" arg_list? ")"
arg_list       ::= arg ("," arg)*
arg            ::= expr                       (* positional *)
                |  ident ":" expr             (* named *)

bin_op_expr    ::= expr bin_op expr
bin_op         ::= "+" | "-" | "*" | "/" | "%" | "**"
                |  "&" | "|" | "^" | "<<" | ">>"
                |  "==" | "!=" | "<" | ">" | "<=" | ">="

unary_expr     ::= "-" expr                   (* arithmetic neg *)
                |  "!" expr                   (* bitwise NOT *)

field_access   ::= ident "." ident            (* source field projection *)

string_literal ::= "\"" ( char | "{" ident "}" )* "\""

array_literal  ::= "[" (expr ("," expr)*)? "]"
```

Seven expression constructors. Six lifecycle-typed kinds
(`Ident`, `IntLit`, `FloatLit`, `StringLit`, `ArrayLit`,
`Call`) plus three sugar-only kinds (`BinOp`, `UnaryNeg`,
`UnaryBitNot`) that desugar to `Call`.

`FieldAccess` (the seventh non-sugar form) is a source
field projection — reads a field from a typed source
binding. Its semantics depend on the source's declared
type and are part of the type-inference rules (§3).

### 2.6 Pragmas

```ebnf
pragma         ::= "pragma" ident
```

Module-level compile-time directives. Unknown pragmas are
forward-compatible (warning, not error).

---

## 3. Type inference rules

The grammar's type system is `PortType`:
`U64`, `F64`, `Bool`, `Str`, `Bytes`, `Json`, `VecF32`,
`VecI32`, plus extension types via `Ext`. Every well-
formed expression has an output type derivable from its
structure.

### 3.1 Literal rules

```text
T-IntLit:    n ∈ integer literal           ⊢  n  :  U64
T-FloatLit:  n ∈ float literal             ⊢  n  :  F64
T-StringLit: s ∈ string literal            ⊢  s  :  Str
T-BoolLit:   b ∈ {true, false}             ⊢  b  :  Bool
T-ArrayLit:  e_1 : T, ..., e_n : T         ⊢  [e_1, ..., e_n]  :  Vec<T>
                                              (all elements same type)
```

Polydat does not distinguish signed vs unsigned at the
literal tier; integer literals are `U64`. Signed values
appear via explicit conversion nodes.

### 3.2 Identifier rules

```text
T-LocalIdent:    ident is declared in this scope by `input`,
                 `binding`, `cursor`, or `extern`
                 with declared type T
                 ⊢  ident  :  T

T-OuterIdent:    ident is not declared in this scope but
                 is declared in some outer scope with type T
                 (discovered via auto-extern at compile time)
                 ⊢  ident  :  T

T-Unknown:       ident not declared anywhere reachable
                 ⊢  fail at compile time with typed error
```

The T-OuterIdent rule is the formal expression of G1
(auto-extern as syntactic discovery): an unresolved
identifier in the local scope is *automatically* searched
in outer scopes; if found, an extern slot is synthesised
in the current scope's program.

### 3.3 Function call rules

```text
T-Call:    func is declared with signature
              (param_1: T_1, ..., param_k: T_k) -> (out: U)
           args : (T_1, ..., T_k)
           ⊢  func(args)  :  U

T-Call-OverloadResolve:
           func has multiple registered signatures S_1, ..., S_m
           where exactly one S_i matches args' types
           ⊢  func(args)  :  U_i
```

The overload-resolution rule reflects the registry's
common pattern: arithmetic operators like `add` exist as
both `u64_add` and `f64_add`; the parser desugars `a + b`
to one or the other based on `a` and `b`'s types.

### 3.4 BinOp rules

BinOps desugar to function calls. The desugaring rule:

```text
T-BinOp-Add:    a : U64, b : U64    ⊢  a + b  ≡  u64_add(a, b)  :  U64
                a : F64, b : F64    ⊢  a + b  ≡  f64_add(a, b)  :  F64
                a : U64, b : F64    ⊢  a + b  ≡  f64_add(u64_to_f64(a), b) : F64
                                                (adapter from §5.4 of expression_engine)
```

Cross-type BinOps trigger adapter insertion per the
adapter catalog. Comparison operators (`==`, `!=`, `<`,
etc.) produce `Bool`.

### 3.5 Field access rules

```text
T-FieldAccess:    source is declared with type Source_T
                  Source_T has a field-projection rule
                    "field" : field_T
                  ⊢  source.field  :  field_T
```

The Source_T's field-projection rules are declared per
source kind (e.g., a vector-dataset source provides
`vector`, `ordinal`, and other fields). The grammar
itself does not enumerate these; they come from the
source binding's declared type.

### 3.6 Type totality

```text
G4 follows from these rules: every well-formed expression
in the grammar has a derivable output type. The rules
cover every expression constructor; there is no
"untyped" expression form.
```

---

## 4. The G-axioms — distinctive structural commitments

Six commitments the grammar makes. Each is a structural
property the other docs' axioms depend on.

### Axiom G1 — Auto-extern as syntactic discovery

**Every identifier reference in an expression is
classified by the grammar as local or outer-scope.
Identifier references are not syntactically distinguished
by their resolution scope — the syntax is the same — but
the resolution rule (T-LocalIdent then T-OuterIdent) is
total and deterministic. An identifier not found locally
is automatically searched in the outer chain and, if
found, synthesised as an extern slot in the current
scope's program.**

What this enables:

- The substrate's S1 (auto-extern as synthesis surface
  discovery). The compiler walks the body, sees
  unresolved identifiers, classifies them per T-OuterIdent,
  and synthesises the slot.
- The compiler's CF1 (synthesis surface completeness) —
  every extern slot is discovered structurally, not
  declared manually.
- The expression engine's `{name}` interpolation — the
  same identifier-as-extern rule applies to interpolation
  placeholders, so the host's `{k}` references the same
  binding that a bare `k` reference would.

What breaks without G1: workload authors would have to
manually declare every cross-scope dependency, doubling
the surface area and creating a maintenance trap.

### Axiom G2 — Lifecycle declared at the syntactic surface

**The `const` modifier on a binding declares
effectively-const lifecycle at the syntactic surface.
The compiler does not infer lifecycle from the
expression's contents alone; the author's declaration is
authoritative, and the compiler verifies the declaration
against the wire chain.**

What this enables:

- The substrate's L2 (two-lifecycle classification
  bridges layers). The grammar exposes the classification
  at the surface; the compiler enforces it.
- SRD-11's const-binding contract. Plan A (compile-time
  structural check) verifies the declaration; Plan B
  (scope-init pull) materialises the value once.
- The runtime model's R1 (per-generation memoization).
  Effectively-const wires are computed once at scope-init
  and never re-evaluated within the scope.

What breaks without G2: lifecycle would have to be
inferred per call, the compiler would need a richer
analysis, and the wire-chain check would lack a
declarative anchor.

### Axiom G3 — Scope-chain transparency

**An identifier referencing an outer-scope binding uses
the same syntax as one referencing a local binding. There
is no "outer keyword," no "parent scope" qualifier, no
syntactic ceremony. The author writes `k` regardless of
which scope owns the `k` binding.**

What this enables:

- The substrate's L1 (each layer owns its state) at the
  syntactic level. The grammar does not expose the layer
  structure to the author; the substrate handles it.
- The composition substrate's slot-contract abstraction —
  authors write expressions without knowing which layer
  each value lives in; the chain does the lookup.
- The expression engine's E1 (self-contained submission) —
  embedded expressions consume host-provided context
  without syntactic adaptation.

What breaks without G3: outer-scope references would
require explicit qualifications, expressions would not
be embeddable as-is, and the substrate's layered design
would leak through the syntax.

### Axiom G4 — Port-typed expressions

**Every well-formed expression in the grammar has a
derivable output `PortType`. The type inference rules
(§3) are total — every expression-constructor case has
a rule; the rules compose; there is no expression form
without a declared output type.**

What this enables:

- The substrate's T1 (every slot is typed) at the
  expression layer. Expressions produce typed wires.
- The substrate's T2 (type mismatches caught at
  construction or healed by auto-adapters) — type-checking
  happens at every wire boundary because every wire is
  typed at both ends.
- The runtime model's D1 (typed-return determinism). The
  output type is structural; the deterministic return
  follows from it.
- The expression engine's E2 (typed result). The
  returned `Value` carries its declared type.

What breaks without G4: untyped expressions would
require runtime type checking, the substrate's slot
contract would lose its compile-time enforcement, and
the embedding contract's typed-result guarantee would
become a runtime concern.

### Axiom G5 — Two-lifecycle structural classification

**Every wire in a well-formed program is classifiable
into Effectively-const or Dynamic by structural
analysis of the wire chain alone. The classification is
a function of the wire's upstream cone (lifecycle is the
join of upstream lifecycles per H2) plus the bindings'
declared modifiers (per G2). It does not depend on
runtime state or evaluation history.**

What this enables:

- The substrate's L2 (two-lifecycle classification
  bridges layers) — the substrate names two classes; the
  grammar guarantees every wire belongs to one.
- The compiler's H1 (classification totality) and H2
  (classification monotonicity under fan-in).
- The runtime model's R1 + the scope-init buffer
  partition. Hoisted (Effectively-const) wires live in
  a separate buffer evaluated once at scope-init.

What breaks without G5: lifecycle would have to be
discovered at evaluation time, the compiler couldn't
emit partitioned scope-init/per-cycle code paths, and
the runtime cost-determinism guarantee (D3) would
lose its structural basis.

### Axiom G6 — Single grammar for expressions and full programs

**The grammar treats a single expression and a full
multi-binding program uniformly: an expression is a
program of one anonymous output binding; a program is a
sequence of zero or more named bindings whose final
binding is reachable. There is no "expression grammar"
vs "program grammar"; one grammar covers both.**

What this enables:

- The expression engine's claim that the same compiler
  compiles a four-character expression as a 200-line
  workload. The grammar's uniformity makes this true
  without any expression-mode special case.
- The expression engine's E4 (library inheritance) —
  every node available to a workload is available to an
  embedded expression because the grammar does not
  distinguish.
- The host's free composition pattern — embedding,
  interpolation, and full kernel construction all use
  the same grammar.

What breaks without G6: hosts would have to choose
between two grammars, the compiler would have two
pipelines to maintain, and the embedding contract would
lose its uniformity claim.

### Sub-axiom G6.i — Compiler intrinsics are grammar-level desugars

**A small fixed set of expression forms that look like
function calls or value expressions are *intrinsics* —
the parser recognises them and emits desugared graph
shapes rather than ordinary function-call nodes. The
intrinsic set is closed: each intrinsic rewrites to a
combination of registered library nodes (or constant-node
emission for literals), so the post-desugar graph is
expressible in the grammar's ordinary surface. Nothing
flows through the compiler that an author could not write
by hand using only registered nodes.**

The intrinsic catalog is part of the grammar's
*structural commitment* — these forms exist at parse
time and are not extensible by library code. The specific
catalog (currently `if(cond, a, b)` and its block spelling
`if cond { a } else { b }`, literal promotion in
wire position, and string-interpolation desugar to
`printf(...)`) is delegated to
[language_spec.md §"Conditional Selection" + §"Literal
Promotion" + §"String Interpolation"](language_spec.md).

`if` is a soft keyword: the lexer emits it as an ordinary
identifier, and only the token that follows decides the parse
(`(` keeps the call form; anything else opens the block form).
This is the same treatment `over` and `input` receive, and it
is why adding the block form required no lexer change and
broke no existing kernel.

What this enables:

- Closure under composition: an embedded expression that
  uses `"x={y}"` interpolation compiles to the same
  printf-call graph the author could write directly. The
  embedding contract sees nothing magic.
- Library inheritance (E4): every intrinsic resolves to
  library-registered nodes, so a host that exposes the
  standard library exposes every intrinsic.
- Compiler simplicity: the intrinsic set is small and
  fixed; the compiler's main loop sees only registered
  library nodes after parse.

What breaks without G6.i: parse-time desugars would have
to live in library code (every library node would have to
declare itself "intrinsic" via a registration mechanism),
or the grammar would have to grow special-case constructs
the compiler treats opaquely.

### Sub-axiom G6.p — Infix operators have a stable precedence

**Infix operators in the grammar follow a stable
Rust-like precedence ordering. Parse-tree shape is
determined by this precedence table; the table is a
grammar-structural commitment, not an implementation
detail. The canonical table lives in
[language_spec.md §"Infix Operators"](language_spec.md);
the commitment that *some* stable precedence exists and
that authors can rely on it for parse-tree shape is the
grammar's axiom here.**

What this enables:

- Authors can write `a + b * c < d & e` and predict the
  parse without consulting docs every time.
- The compiler's type-inference pass (per §3 type rules)
  operates on a determinate AST shape.
- Tooling that walks or rewrites the AST can rely on
  precedence-derived structure.

What breaks without G6.p: every operator would need
explicit parenthesisation, OR the parse would become
implementation-defined and tooling would need
implementation-specific knowledge to walk the tree.

---

## 5. The G-axioms compose

```text
            Grammar
            G1 G2 G3 G4 G5 G6
              │
              ▼
        slot contract       ← G1+G4 → S1+T1
              │
              ▼
        compiler passes     ← G2+G5 → H1+CF1
              │
              ▼
        runtime model       ← G4+G5 → R1+D1
              │
              ▼
        embedding           ← G3+G6 → E1+E4
```

Each G-axiom is the substrate / compiler / runtime /
embedding axioms' grammar-level basis. The other docs
make claims like "the substrate's S1 holds"; this doc
identifies G1 as the grammar-level commitment that makes
S1 achievable. Without G1, S1 would have to be a runtime
discovery surface; without G2, L2 would have to be a
runtime classification; without G3, embedding would
require syntactic adaptation.

The G-axioms are *load-bearing for the architecture*, not
optimisations or conveniences. The substrate is small
because the grammar's commitments do load-bearing work
the substrate doesn't have to repeat.

---

## 6. SRD cross-references and roles

| SRD / doc | Role under this declaration |
|---|---|
| [Composition Substrate](composition_substrate.md) | S/T/L axioms. G1+G4 underwrite S1+T1; G3+G4 underwrite L1+L2; G5 underwrites L2's structural classification. |
| [Graph Compiler](graph_compiler.md) | H/CF/NF axioms. G2+G5 underwrite H1+H2; G1 underwrites CF1; G4 underwrites NF1. |
| [Runtime Model](runtime_model.md) | R/D axioms. G4 underwrites D1; G5 underwrites R1+D3; G3 underwrites L1's runtime realisation. |
| [Expression Engine](expression_engine.md) | E-axioms. G3+G6 underwrite E1+E4; G4 underwrites E2; G6 underwrites the expression-as-kernel correspondence. |
| [SRD-10](language_spec.md) | DSL syntax. This doc's productions (§2) formalise the syntax SRD-10 describes prosaically. |
| [SRD-11](evaluation_model.md) | Two-lifecycle classification. G2+G5 are the grammar-level commitments SRD-11's lifecycle taxonomy rests on. |
| [SRD-13c](scope_model.md) | Scope-composition mechanism. G1+G3 are the grammar-level commitments SRD-13c's auto-extern + `bind_outer_scope` rest on. |
| [SRD-13f](wire_materialization.md) | Cross-scope read/write. G1's auto-extern discovery is what SRD-13f's gradient classification operates over. |

---

## 7. What this document does NOT specify

- **Lexer-level concerns.** Tokenisation, whitespace,
  comments, and string-literal escape rules live in
  `dsl/lexer.rs`. Not formalised here.
- **Per-node semantics.** What `hash` or `mod` does is the
  per-node implementation's responsibility (and the
  library catalog's documentation). The grammar names the
  call form; the semantics live elsewhere.
- **Parse-error recovery.** The parser's error reporting
  + recovery strategies are implementation concerns. SRD-10
  describes them prosaically; not formalised here.
- **Source modules and includes.** The module-resolution
  pipeline (`dsl/modules.rs`) is implementation; this doc
  treats modules as a `ModuleDef` statement form and stops.
- **Pragma semantics.** Specific pragma names and their
  semantics are documented per-pragma in `dsl/pragmas.rs`
  and SRD-15. The grammar treats pragmas as forward-
  compatible name-bearing statements.

---

## 8. Open questions

### 8.1 Formal type-inference soundness statement

The type-inference rules (§3) are presented as derivation
rules but no formal soundness theorem is stated (e.g.,
"every well-typed expression evaluates to a value of its
declared type"). A future revision should either state
the soundness theorem explicitly or note that it follows
trivially from G4 + the per-node implementation's
correctness.

### 8.2 String interpolation as expression form

The grammar treats `{name}` in string literals as a
substitution at parse-time-or-eval-time (depending on
context). This is currently described prosaically in
SRD-10 and used uniformly in expression embedding (E6).
A future revision could elevate interpolation to a
first-class expression form (`Interp(template, vars)`)
with its own type-inference rule. The current treatment
works but loses some compositional clarity.

### 8.3 Cross-statement type inference

The current type-inference rules (§3) operate on single
expressions. Cross-statement inference (e.g., a binding's
RHS type informs the binding's declared type for
downstream uses) happens during compilation but is not
formalised here. A future revision should add an §3.7
covering the cross-statement flow.

### 8.4 Type-erased extension points

`PortType::Ext` is the catch-all for types beyond the
built-in set (Partition, Json, dataset handles, etc.).
The grammar treats `Ext` as opaque; the per-extension
type implements its own field-projection rules. A future
revision should specify the contract `Ext` types must
satisfy to participate in G4 (port-typed expressions)
and G5 (lifecycle classification).

---

[Composition Substrate]: composition_substrate.md
[Graph Compiler]: graph_compiler.md
[Runtime Model]: runtime_model.md
[Expression Engine]: expression_engine.md
[`dsl::ast`]: ../../src/dsl/ast.rs
[`dsl::parser`]: ../../src/dsl/parser.rs
[`dsl::compile`]: ../../src/dsl/compile.rs
