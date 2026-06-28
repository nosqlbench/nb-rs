# The Polydat Grammar — Definitive Specification & Guide

**Subtitle:** The complete, verified reference for the Polydat surface
language.

> **Planned (SRD-84):** the `&&` / `||` boolean operators (eager
> truthiness combinators; short-circuit deferred) and the uniform
> `<expr> as <type>` cast described below have shipped. They occupy the
> lowest precedence band (`&&`/`||`) and the tightest postfix position
> (`as`) respectively. See [SRD-84](../../../docs/SRD/84_grammar_safe_matter.md)
> Parts 1 + 1b for the original framing.

<a id="sec-authority"></a>
## 0. Authority and supersession

This document is the **single authoritative reference** for the Polydat
surface language: its lexical grammar, its statement and expression
productions, its type-naming vocabulary, its desugaring and projection
behaviour, and its rejection rules. It is written to be read top to
bottom — every construct is introduced **one at a time, with worked
examples** — and to be **machine-verified**: the examples in this file
are extracted and round-tripped by
[`polydat/tests/doc_examples_test.rs`](../../tests/doc_examples_test.rs)
on every `cargo test` run.

It **supersedes and replaces**:

- [`grammar.md`](grammar.md) — *The Grammar Substrate.* Its formal
  productions, type-inference rules, and the six **G-axioms** are carried
  forward here in summary (§[17](#sec-gaxioms)); `grammar.md` carries a
  supersession banner pointing here as the definitive entry point and is
  retained as the detailed formal appendix this spec cross-references.
- [`docs/SRD/10_polydat_language.md`](../../../docs/SRD/10_polydat_language.md)
  — *nbrs-side framing.* Its grammar-facing material (cursor
  declarations, modifiers, output selection as it touches syntax) is
  carried forward here; its host-integration framing remains in SRD-10,
  which now points here for all grammar matters.

It **does not** supersede:

- [`language_spec.md`](language_spec.md) — the compilation-pipeline /
  node-contract substrate. This spec owns *what the syntax is*;
  `language_spec.md` owns *how it is compiled and run*.
- [`comprehension_forms.md`](comprehension_forms.md) — the comprehension
  **algebra** (constructors, validity axioms, optimizer rewrites, IR).
  The comprehension `for:`/`where:`/`order:` surface is a *sibling*
  mini-grammar parsed by the `iteration` subsystem, **not** part of the
  `.polydat` statement language. §[16](#sec-comprehension) describes its
  surface and defers to `comprehension_forms.md` for all semantics.
- [`type_system.md`](type_system.md) — the storage-class / adapter
  machinery behind the type *keywords* this spec names.

**Conflict resolution.** On any grammar-structural matter (productions,
precedence, type-naming, projection), this document is final. Where a
companion doc describes runtime or host behaviour, it remains
authoritative for that.

<a id="sec-roundtrip"></a>
### 0.1 How the examples are verified — the round-trip contract

Polydat ships a projector,
[`polydat::dsl::pprint::pp_file`](../../src/dsl/pprint.rs), that turns a
parsed AST back into canonical `.polydat` source. Throughout this spec,
**“the syntax the runtime gives back”** means the output of `pp_file`.

The projector's guarantee is **idempotence**, not textual fidelity:

```text
let p1 = pp_file(parse(src));
let p2 = pp_file(parse(p1));
assert_eq!(p1, p2);            // second-pass print == first-pass print
```

Projection is *canonicalizing*. It deliberately differs from the input
text in three ways, each of which you will see in the examples below:

1. **Every `BinOp` is fully parenthesized.** `y := x + 1` projects as
   `y := (x + 1)`. This is uniformly safe and makes precedence explicit.
2. **String interpolation is desugared at parse time.** `name := "{a}-{b}"`
   parses to a `printf` call and projects as
   `name := printf("{}-{}", a, b)` (see §[10](#sec-interpolation)).
3. **Integer-valued finite floats gain a trailing `.0`.** `60.0` stays
   `60.0`; `6.283185307179586` is preserved exactly.

Code blocks tagged <code>```polydat</code> in this document are parsed
and checked for idempotent round-trip. Blocks additionally tagged
<code>```polydat compile</code> are also compiled (and must succeed).
Blocks tagged <code>```text</code> are illustrative only — EBNF,
token tables, rejected forms, and the sibling comprehension surface —
and are **not** parsed.

A second document,
[`polydat_grammar_programmatic.md`](polydat_grammar_programmatic.md),
builds a selection of these same kernels **programmatically** via the
public AST types, and the test proves the two construction paths project
to **identical** canonical syntax
(`pp_file(builder_ast) == pp_file(parse(grammar_src))`). Those paired
examples are flagged **[↔ programmatic]** below.

---

<a id="sec-lexical"></a>
## 1. Lexical basics

Polydat source is a sequence of **statements**. It is
**newline-insensitive and has no statement terminators** — statement
boundaries are determined entirely by the parser, not by line breaks or
semicolons. Identifiers are ASCII `[A-Za-z_][A-Za-z0-9_]*`.

<a id="sec-comments"></a>
### 1.1 Comments

Three comment forms exist; all are stripped by the lexer:

- `#` to end of line
- `//` (and `///`) to end of line
- `/* … */` block comments — **non-nesting**

```text
# a hash comment, to end of line
x := 1   // a line comment after a statement
y := 2   /* a block comment */
/* block comments do /* not */ nest — this trailing text is live code
```

Note that `pragma` (§[15](#sec-pragmas)) is a real first-class statement,
**not** a comment, despite looking directive-like.

<a id="sec-keywords"></a>
### 1.2 Keywords

There are exactly **eight hard keyword tokens**:

```text
const   input   extern   shared   volatile   cursor   over   pragma
```

Everything else — including `module`, `as`, `range`, and every type name
(`u64`, `f64`, …) — is an ordinary identifier. Three of the eight are
**soft** in expression position and may be used as names: `input`,
`cursor`, `over`. The other five (`const`, `extern`, `shared`,
`volatile`, `pragma`) cannot be used as names. `as` is a *soft postfix
keyword*: it is a cast only in `<expr> as <type>` position and is
otherwise an ordinary identifier (it is also the conventional module
parameter name).

<a id="sec-operators"></a>
### 1.3 Operator and punctuation tokens

```text
+  -  *  /  %  **          arithmetic ( ** = power )
&  |  ^  <<  >>            bitwise / shift
&&  ||  !                  logical and/or (eager), bitwise NOT
==  !=  <  >  <=  >=       comparison
:=                         binding
=                          only after `extern …: T` and `cursor …`
:  ->  .  ,                annotation, module arrow, field access, separator
( ) [ ] { }               grouping, arrays, module bodies
```

Two tokens are easy to confuse:

- `:=` (`ColonEq`) is the **binding** operator, used by *every* binding
  form. `=` (`Eq`) is used **only** by `extern …: T = default` and
  `cursor … = constructor`. `x = 1` is **not** a binding.
- `-` is always the minus token; there is no negative-literal token.
  `-5` is unary negation applied to `5` (§[8](#sec-unary)).

There is **no `..` token**: `..` lexes as two `.` tokens, and `range(a, b)`
is an ordinary function call, not special syntax.

---

<a id="sec-literals"></a>
## 2. Literals

<a id="sec-int-literals"></a>
### 2.1 Integer literals

Decimal and `0x`/`0X` hexadecimal only (no binary or octal). Integer
literals have type `u64`.

```polydat
decimal := 1000
hexa := 0xFF
big := 4294967296
```

> **Gotcha — no underscore separators.** `1_000` does **not** lex as
> `1000`; it lexes as `1` followed by the identifier `_000`. Write
> `1000`.

<a id="sec-float-literals"></a>
### 2.2 Float literals

A digit run becomes a float only when a `.` is **followed by a digit**.
This is what disambiguates a float from a field access: `1.0` is the
float one; `1.foo` is `1` `.` `foo`; `.5` is `.` then `5`; `1.` is `1`
then `.`. Scientific notation (`1e10`, `2.5e3`, `3.14e-2`) is supported,
with or without a decimal point. Float literals have type `f64`.

```polydat
pi := 3.14
scaled := 1e10
small := 3.14e-2
tau := 6.283185307179586
```

<a id="sec-si-suffixes"></a>
### 2.3 SI suffixes on numbers

A numeric literal may carry an SI suffix, applied only when the
character after the suffix is not identifier-continuation:

| Class | Suffixes | Multiplier |
|---|---|---|
| Decimal | `K` `M` `G` `T` `P` | 10³ … 10¹⁵ |
| Binary | `Ki` `Mi` `Gi` `Ti` `Pi` | 2¹⁰ … 2⁵⁰ |
| Sub-unit | `m` `u` `n` | 10⁻³, 10⁻⁶, 10⁻⁹ |

The two-character binary forms are checked first (so `K` does not eat the
`K` of `Ki`). An SI value that is exactly integral collapses to a `u64`;
a sub-unit suffix promotes to `f64`.

```polydat
thousand := 1K
kibi := 1Ki
half_milli := 5m
integral := 1.5K
not_a_suffix := 1
distance_label := one_Kilometer
```

Here `1K` → `1000` (`u64`), `1Ki` → `1024`, `5m` → `0.005` (`f64`),
`1.5K` → `1500` (`u64`, integral). `1Kilometers` would lex as `1`
followed by the identifier `Kilometers` — the suffix only applies when
nothing identifier-like follows it.

<a id="sec-string-literals"></a>
### 2.4 String literals

Delimited by `"…"` or `'…'`; both kinds are ordinary string literals
(single quotes are **not** character literals — they are commonly used
for charset specs like `'0-9A-Za-z'`). Recognized escapes are `\n`,
`\t`, `\\`, and the active quote character; any other `\x` keeps the
backslash literally.

```polydat
greeting := "hello world"
charset := '0-9A-Za-z'
escaped := "line one\nline two"
quote := "she said \"hi\""
```

`{…}` placeholders inside a string are **interpolation**, desugared to a
`printf` call — see §[10](#sec-interpolation).

<a id="sec-array-literals"></a>
### 2.5 Array literals

`[expr, …]`, possibly empty.

```polydat
weights := [60.0, 20.0, 15.0, 5.0]
empty := []
```

<a id="sec-bool"></a>
### 2.6 `true` and `false`

`true` and `false` are **plain identifiers**, not a distinct boolean
literal token. Comparison and logical operators yield a `u64` `0`/`1`,
not a separate bool value at the wire level (§[7](#sec-comparison)).

---

<a id="sec-bindings"></a>
## 3. Bindings  **[↔ programmatic]**

The fundamental statement is a **binding**: a name `:=` an expression.
A bare binding is **per-cycle** (re-evaluated every cycle).

```polydat compile
input cycle: u64
hashed := hash(cycle)
user_id := mod(hashed, 1000000)
```

This is the minimal idiom: a cycle coordinate in, a bounded id out.
Projected, it is byte-for-byte the same (no `BinOp`, no interpolation, no
float to canonicalize). It is verified end-to-end — built both from this
source and from a hand-constructed AST — in
[the programmatic guide](polydat_grammar_programmatic.md#p-minimal).

<a id="sec-destructuring"></a>
### 3.1 Tuple-destructuring bindings  **[↔ programmatic]**

A multi-output node call binds to a parenthesized target list. The
binding operator is still `:=`.

```polydat compile
input cycle: u64
(region, store, tx) := mixed_radix(cycle, 50, 200, 0)
region_id := mod(hash(region), 10000)
store_id := mod(hash(interleave(region, store)), 100000)
```

`mixed_radix` decomposes a flat cycle into positional digits; a trailing
radix of `0` means unbounded. Paired AST builder:
[programmatic guide](polydat_grammar_programmatic.md#p-destructure).

---

<a id="sec-inputs"></a>
## 4. Inputs

`input name: type` declares a per-cycle coordinate slot driven by
`set_inputs`. The type annotation is **optional and advisory** — every
coordinate input rides `u64` at runtime regardless of the annotation;
the annotation aids inference and documents intent.

```polydat
input cycle: u64
input thread
```

<a id="sec-input-tuple"></a>
### 4.1 Tuple input form

`input (a: T, b: T, …)` is sugar that **desugars at parse time** into N
separate `InputDecl`s. The empty form `input ()` is rejected.

```polydat compile
input (cycle: u64, thread: u64)
combined := interleave(cycle, thread)
row_key := mod(hash(combined), 1000000)
```

> **Projection note.** Because the tuple form desugars to N decls, this
> program projects back as two lines —
> `input cycle: u64` / `input thread: u64` — not as the tuple. That is
> the canonical form, and the round-trip is still idempotent. The
> [programmatic builder](polydat_grammar_programmatic.md#p-tuple-input)
> constructs the two `InputDecl`s directly, matching the projection.

---

<a id="sec-modifiers"></a>
## 5. Binding modifiers — `const`, `shared`, `volatile`

A binding may be prefixed by one or more **wire modifiers**, in any
order. They declare *lifecycle* at the syntactic surface (this is
G-axiom **G2**, §[17](#sec-gaxioms)) — the compiler verifies the
declaration against the wire chain, it does not infer it.

| Modifier | Meaning |
|---|---|
| `const` | Effectively-const: materialised once per scope activation, then frozen. Cannot be shadowed by an inner scope. |
| `shared` | A mutable cell that propagates upward to the enclosing scope after a `for_each`. Last-write-wins by default. |
| `volatile` | Per-cycle, but forced Dynamic: excluded from compile-time folding and from program-identity hashing. |

```polydat compile
input cycle: u64
const base := 42
const seed := hash(base)
shared error_budget := 100
volatile attempt := mod(hash(cycle), 8)
user_id := mod(hash(cycle), 1000000)
```

`base` and `seed` have no cycle dependency and const-fold to literals;
`user_id` varies per cycle.

<a id="sec-modifier-combos"></a>
### 5.1 Valid and rejected combinations

`shared const` and `shared volatile` are valid combinations. The
combination **`const volatile` is rejected at parse time** (the two are
contradictory — one freezes, the other excludes from folding), as is a
**duplicate modifier**.

```text
const volatile x := 1    # REJECTED: contradictory modifiers
const const y := 2       # REJECTED: duplicate modifier
shared const z := 100    # OK: a shared cell whose initial value folds
```

> The retired `init` / `final` keyword pair is **gone** — `const`
> subsumes both. Do not reintroduce them.

---

<a id="sec-expressions"></a>
## 6. Expressions and operators

There is one expression grammar, used identically for a four-character
expression and a two-hundred-line kernel (G-axiom **G6**). The non-sugar
constructors are: identifier, integer literal, float literal, string
literal, array literal, call, and field access. Three **sugar** forms —
binary operator, unary negation (`-`), and bitwise NOT (`!`) — desugar
to calls.

<a id="sec-precedence"></a>
### 6.1 Precedence and associativity

Loosest to tightest; all left-associative except `**`:

```text
||                     (lowest)
&&
== !=
< > <= >=
|
^
&
<< >>
+ -
* / %
**                     (RIGHT-associative)
unary - !              (- = arithmetic negate; ! = bitwise NOT)
postfix  as <type>     (tightest; binds to the atom; chains left-to-right)
```

So `a > b && c > d` parses as `(a > b) && (c > d)`; `a + b < c * d` as
`(a + b) < (c * d)`; `a + b as u64` as `a + (b as u64)`; and `2 ** 3 ** 2`
as `2 ** (3 ** 2)` (right-associative).

<a id="sec-arithmetic"></a>
### 6.2 Arithmetic

`+ - * / %` choose `u64_*` when **both** operands are `u64`, otherwise
the `f64_*` form with the `u64` side widened by an inserted `to_f64`
adapter. `**` is always `pow` (`f64`). Bitwise/shift operators
(`& | ^ << >>`) are `u64`.

```polydat compile
input cycle: u64
a := hash(cycle)
sum := a + 7
masked := a & 0xFF
shifted := a >> 3
power := 2 ** 10
```

Projected, the binops are fully parenthesized: `sum := (a + 7)`,
`masked := (a & 255)`, `power := (2 ** 10)`. (Note `0xFF` projects as its
decimal value `255` — the projector emits a canonical integer.)

<a id="sec-unary"></a>
### 6.3 Unary `-` and `!`

`-x` desugars to `f64_sub(0.0, x)`; `!x` desugars to bitwise NOT
(`u64_not`), **not** logical negation.

```polydat
delta := -100.0
bits := !0
```

`delta` projects as `(-100.0)` and `bits` as `(!0)`.

---

<a id="sec-comparison"></a>
## 7. Comparisons and logical operators

Comparison operators (`== != < > <= >=`) select `str_*`/`f64_*`/`u64_*`
by operand family (string beats float beats u64) and produce a `u64`
`0`/`1`. Ordered comparisons (`< > <= >=`) on **strings are a compile
error**; only `==`/`!=` are defined for strings.

The logical operators `&&` and `||` are **eager** (no short-circuit —
both sides always evaluate): each operand is reduced to truthiness
(`x != 0`) and combined with `u64_and`/`u64_or`.

```polydat compile
input cycle: u64
x := hash(cycle)
big := x > 1000000
in_band := (x > 100) && (x < 1000000)
flag := (x == 0) || big
```

`big` projects as `(x > 1000000)`; `in_band` as
`((x > 100) && (x < 1000000))`; `flag` as `((x == 0) || big)`.

The built-in `if(cond, a, b)` is a compiler intrinsic (a closed
parse-time form, G-axiom **G6.i**) lowering to `select_u64/f64/str`;
like the logical operators it evaluates **both** branches.

```polydat compile
input cycle: u64
optimize_for := "LATENCY"
latency_factor := 1.5
recall_factor := 9.5
overscan := if(optimize_for == "LATENCY", latency_factor, recall_factor)
```

---

<a id="sec-calls"></a>
## 8. Function calls

`name(arg, …)`, where each argument is either **positional** (`expr`) or
**named** (`ident: expr`). Positional and named may be mixed (positional
first by convention); the empty call `name()` is allowed. The soft
keyword `input` is accepted as an argument name.

```polydat compile
input cycle: u64
h := hash(cycle)
u := unit_interval(h)
raw := icd_normal(u, 100.0, 15.0)
clamped := clamp_f64(raw, -50.0, 50.0)
```

Named-argument form (common for stdlib module calls):

```polydat
v := dist_normal(mean: 72.0, stddev: 5.0)
weights := combinations(seed: 0, charset: "A-Z0-9", length: 8)
```

---

<a id="sec-interpolation"></a>
## 9. String interpolation

A string literal containing `{…}` placeholders is **desugared at parse
time** into a `printf` call. The placeholder body is parsed as a full
expression; `{{`/`}}` are literal braces; a malformed placeholder (e.g. a
printf format spec like `{:05}`) silently stays a plain `StringLit`.

```polydat compile
input cycle: u64
(tenant, device, reading) := mixed_radix(cycle, 100, 1000, 0)
tenant_code := mod(hash(tenant), 10000)
device_seq := mod(hash(interleave(tenant, device)), 100000)
device_id := "{tenant_code}-{device_seq}"
label := "sensor_reading"
```

> **Projection note (important).** `device_id := "{tenant_code}-{device_seq}"`
> parses to a `printf` call, so it **projects back as**
> `device_id := printf("{}-{}", tenant_code, device_seq)`. The
> placeholder-free `label` stays `"sensor_reading"`. This is the clearest
> demonstration of projection-as-canonicalization: the runtime gives
> back the desugared form, and the round-trip is idempotent from there.

---

<a id="sec-casts"></a>
## 10. `as` casts

`<expr> as <type>` is an **alignment-only** cast. The defined coercions
are:

- `u64 as f64` → widening (`to_f64`)
- `str as u64` → `StrToU64`
- same type → no-op passthrough

`f64 as u64` is a **hard compile error** — narrowing under `as` is
disallowed; choose an explicit rounding node (`f64_to_u64`,
`round_to_u64`, `floor_to_u64`, `ceil_to_u64`). Any other pair is also an
error. Casts may chain (`x as u64 as f64`).

```polydat compile
input cycle: u64
x := hash(cycle)
ratio := (x as f64) / 4294967296.0
```

`ratio` projects as `((x as f64) / 4294967296.0)`. The cast binds tighter
than `/`, so the parenthesization here matches what you wrote.

```text
narrowed := some_f64 as u64    # REJECTED: use round_to_u64 / f64_to_u64 / …
```

---

<a id="sec-fields-cursors"></a>
## 11. Field access and cursors  **[↔ programmatic]**

<a id="sec-field-access"></a>
### 11.1 Field access

`base.field` projects a field from a source-typed base. The base must be
a bare identifier (or soft keyword); field access does **not** apply to
call results or parenthesized groups. **Chained** dots are accepted and
flattened with `__`: `q.cursor.idx` becomes
`FieldAccess { source: "q__cursor", field: "idx" }`, reading wire
`q__cursor__idx`.

<a id="sec-cursors"></a>
### 11.2 Cursors

A **cursor** is a named `u64` ordinal position tracker driving data
access. Its declaration uses `=` (not `:=`):
`cursor <name> = <constructor> [over <expr>]`. A cursor has no fields or
schema of its own; data is read via accessor functions that take the
cursor's ordinal. The optional `over <expr>` clause (SRD-71) supplies a
partition source.

```polydat
cursor users = range(0, 1000000)
```

```polydat
cursor q = range(0, 100) over p
i := q.cursor.idx
ratio := (i as f64) / 100.0
```

> The `over` clause is part of the canonical projection — `pp_cursor`
> emits it, so a cursor with `over` round-trips faithfully. (This was a
> projection gap closed alongside this spec.) The paired AST builder is
> in the [programmatic guide](polydat_grammar_programmatic.md#p-cursor-over).

The **constructor** is an ordinary expression. `range(start, end)` is the
finite-ordinal form. The vectordata cursor-sugar forms rewrite to a
synthetic `range(…)` plus auxiliary projections at compile time (the
cursor-sugar registry is **open**, extensible by node modules via
`inventory::submit!`):

```polydat
cursor row = vectordata_base("example", "label_00")
```

(`vectordata_base`/`_query`/`_source` parse without any feature flag but
only *compile* under the `vectordata` feature, so this block is
round-tripped, not compiled, by the test harness.)

---

<a id="sec-externs"></a>
## 12. Externs  **[↔ programmatic]**

`extern name: type [= default]` declares a slot fixed per scope-init
(via Context Fusion) rather than advancing per cycle like an `input`. The
default is optional.

```polydat compile
input cycle: u64
extern scale: u64
result := cycle * scale
```

`result` projects as `(cycle * scale)`. Externs are the typed,
host-written counterpart to coordinate inputs; the paired builder is in
the [programmatic guide](polydat_grammar_programmatic.md#p-extern).

```polydat
extern balance: f64 = 0.0
extern session_id: u64 = 0
```

---

<a id="sec-modules"></a>
## 13. Module definitions  **[↔ programmatic]**

A module is a named, typed, reusable computation unit:
`name(params) -> (outputs) := { body }`. Parameters are input slots,
outputs are output ports, the body is a statement block. Modules infer
inputs from unbound references and outputs from terminal bindings; the
explicit signature pins the contract.

```polydat compile
sine_wave(input: u64, period: u64) -> (value: f64) := {
    pos := to_f64(input % period)
    per := to_f64(period)
    value := sin((pos / per) * 6.283185307179586)
}
```

The body uses `input` (a soft keyword) as a parameter name and type-aware
infix that resolves to `f64` once operands are `f64`. The paired builder
is in the [programmatic guide](polydat_grammar_programmatic.md#p-module).

> **Projection note.** Module bodies project with four-space-indented
> statements inside `{ … }`, and inner binops are parenthesized while
> calls are not — so the body projects as
> `pos := to_f64((input % period))` / `per := to_f64(period)` /
> `value := sin(((pos / per) * 6.283185307179586))`. Idempotent from there.

---

<a id="sec-pragmas"></a>
## 14. Pragmas

`pragma <name>` is a first-class, module-level compile-time directive
(bare name only, no arguments). Recognized pragmas include
`strict_types`, `strict_values`, and `strict`. **Unknown pragmas are
forward-compatible** — a warning, not an error.

```polydat
pragma strict_types
```

---

<a id="sec-types"></a>
## 15. Types nameable in syntax

The complete set of type keywords usable in `input x: T`, `extern x: T`,
`<expr> as T`, and module signatures is fixed by `PortType::from_keyword`
(`polydat/src/ast.rs`):

```text
u64 f64  u32 i32  i64 f32  u8 i8  u16 i16  f16  u128 i128
reg128  reg_i8x16 reg_i16x8 reg_i32x4 reg_i64x2 reg_f16x8 reg_f32x4 reg_f64x2
bool
str | Str | String           (three spellings; Display renders "String")
bytes
json | Json
ext | Ext
handle
vec_f32 vec_i32 vec_f64 vec_i64 vec_f16 vec_i16 vec_i8
```

> **Spelling gotchas.** Vector keywords are **underscored**: `vec_f32`,
> not `vecf32`. Register-lane keywords are `reg_i8x16` … `reg_f64x2` plus
> raw `reg128`. There is **no `f128`** (stable Rust cannot carry it) and
> **no `none`** keyword (`None` is a runtime `Value` sentinel, not a
> type). An unknown keyword is a loud diagnostic, never a silent default.

```polydat
input cycle: u64
extern threshold: f64 = 0.5
extern tag: String
extern embedding: vec_f32
```

---

<a id="sec-comprehension"></a>
## 16. Sibling surface: the comprehension `for:`/`where:`/`order:` grammar

> **Scope boundary.** The comprehension surface is **not** part of the
> `.polydat` statement language and does **not** round-trip through
> `pp_file`. It is a sibling mini-grammar owned by the `iteration`
> subsystem and authored in workload YAML/JSON, embedding the core
> Polydat expression grammar only for scalar sub-expressions. All
> semantics — constructors, validity axioms, optimizer rewrites, IR —
> are owned by [`comprehension_forms.md`](comprehension_forms.md). This
> section documents only the **surface shape**, in `text` blocks (never
> parsed by the round-trip harness).

A comprehension is a text block with three keys:

```text
for:   "k in 1..10, limit in [10, 100]"
where: "{k} > 0"
order: "halton/20"
```

- **`for:`** is a comma-separated list of `var in source` clauses
  (cartesian product), or a list-of-lists for a union. A depth-0 comma
  starts a new clause only when followed by `<ident> in` — so value-list
  and function-argument commas stay inside one clause.
- **Ranges** in source position: `a..b` (half-open), `a..=b`
  (inclusive), `a..b step c` (or legacy `a..b..c`). Integer endpoints →
  discrete `IntRange`; float endpoints → a continuous interval. SI
  suffixes apply (`0..1K..200` → 0, 200, 400, 600, 800).
- **Tuple clauses** `(a, b) in (…)` zip in parallel; `zip_truncate(…)`
  and `zip_cycle(…)` choose the zip mode.
- **Source quote-kind** selects iteration interior: `"a, b; c"`
  (double) token-strips into `[a, b, c]`; `'a, b; c'` (single) is one
  atomic element. A **bare identifier** is a wire/param reference, not a
  string. Spread is `…` (U+2026) or `...`.
- **`order:`** keywords are exactly `lex`, `reverse_lex`, `diagonal`,
  `antidiagonal`, `extrema`, `shells`, `halton`, `sobol`, `lhs`,
  `custom`, plus the meta-form `space_filling(<halton|sobol|lhs>, …)`.
  Forms: bare `name`, terse `name/N`, keyword `name(arg=val, …)`.

```text
for k in 1..1000000, limit in 1..1000000 order halton/30
(x, y) in (1..10, 100..1000..100)
for (k, limit) in zip_cycle(1..1000000, [10, 50, 100])
k in 10,100, limit in 10,20,30 where k * limit < 1000
order: "shells(origin=center, depth=3)"
```

See [`comprehension_forms.md`](comprehension_forms.md) for the algebra
(`Clause`, `Cartesian`, `Zip`, `Union`, `Filter`, `Order`), the validity
axioms (V1–V9), and the legacy↔algebra bridge (note: `order: "shuffle"`
and `order: "custom(fn)"` are **not** reachable from workload text).

---

<a id="sec-gaxioms"></a>
## 17. Carried-forward foundations: the six G-axioms

The grammar is small but does an unusual amount of load-bearing work.
These six structural commitments (from the superseded `grammar.md`) are
the basis the substrate, compiler, runtime, and embedding docs rest on.
They are **not** optimizations — without them those layers' contracts
would not hold.

- **G1 — Auto-extern as syntactic discovery.** An identifier reference is
  classified local-or-outer with the *same* syntax; an unresolved local
  reference is searched up the scope chain and synthesised as an extern
  slot. (Type rules `T-LocalIdent`, then `T-OuterIdent`.)
- **G2 — Lifecycle declared at the surface.** `const`/`shared`/`volatile`
  declare lifecycle; the compiler verifies, it does not infer
  (§[5](#sec-modifiers)).
- **G3 — Scope-chain transparency.** An outer-scope reference uses the
  same syntax as a local one — no `outer` keyword or qualifier.
- **G4 — Port-typed expressions.** Every well-formed expression has a
  compile-time-derivable `PortType`; the type rules are total
  (§[15](#sec-types)).
- **G5 — Two-lifecycle structural classification.** Every wire is
  classifiable Effectively-const or Dynamic from the wire chain alone,
  independent of runtime state.
- **G6 — One grammar for expressions and programs.** An expression is a
  program of one anonymous output; a program is a sequence of named
  bindings. Sub-axiom **G6.i**: compiler intrinsics (`if(…)`, literal
  promotion, interpolation→`printf`) are a **closed** parse-time set, not
  extensible by library code. Sub-axiom **G6.p**: infix precedence is a
  stable, grammar-structural commitment (§[6.1](#sec-precedence)).

This section summarizes the G-axioms. The full type-inference rules
(`T-IntLit`, `T-Call-OverloadResolve`, `T-BinOp-Add`, `T-FieldAccess`, …),
the G-axiom composition diagram, and the downstream cross-reference map
remain in the retained [`grammar.md`](grammar.md) formal appendix, which
this spec supersedes as the reading entry point but cross-references for
the complete formal statements.

---

<a id="sec-rejections"></a>
## 18. Rejection rules (summary)

The parser/compiler reject, with diagnostics:

- `const volatile` together, and any duplicate modifier
  (§[5.1](#sec-modifier-combos)).
- `input ()` — the empty input tuple (§[4.1](#sec-input-tuple)).
- `f64 as u64` and any undefined `as` coercion
  (§[10](#sec-casts)) — narrowing requires an explicit rounding node.
- Ordered comparison (`< > <= >=`) on strings
  (§[7](#sec-comparison)).
- A bare expression that is not a complete statement (every top-level
  construct must be a statement).
- Undefined wire / unknown function / forward reference (at validate).

> There is **no** “reserved word used as a name” error beyond the five
> hard-unusable keywords, and **no** “chained dot rejected” error —
> chained field access is accepted (§[11.1](#sec-field-access)). Do not
> assume rejections this spec does not list.

---

<a id="sec-projection-summary"></a>
## 19. Appendix: projection canonicalization, at a glance

What “the syntax the runtime gives back” changes, relative to your input:

| You write | It projects as | Why |
|---|---|---|
| `y := x + 1` | `y := (x + 1)` | BinOps fully parenthesized |
| `name := "{a}-{b}"` | `name := printf("{}-{}", a, b)` | interpolation desugared at parse |
| `m := 0xFF` | `m := 255` | canonical integer rendering |
| `f := 60` (into f64 ctx) / `60.0` | `60.0` | integral floats keep `.0` |
| `input (a: u64, b: u64)` | `input a: u64` / `input b: u64` | tuple input desugared |
| `cursor q = range(0,1) over p` | `cursor q = range(0, 1) over p` | `over` retained |

Everything in this table is exercised by
[`doc_examples_test.rs`](../../tests/doc_examples_test.rs), which extracts
every <code>```polydat</code> block above, asserts idempotent round-trip,
compiles the <code>compile</code>-tagged ones, and proves the
**[↔ programmatic]** examples project identically to their hand-built
ASTs in [`polydat_grammar_programmatic.md`](polydat_grammar_programmatic.md).
