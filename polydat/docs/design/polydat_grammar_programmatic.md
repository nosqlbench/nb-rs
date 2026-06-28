# The Polydat Grammar — Programmatic Construction Guide

**Companion to** [`polydat_grammar.md`](polydat_grammar.md).

This guide shows, for a selection of the spec's examples, how to build
the **same kernel** in Rust through the public AST types — without ever
writing a source string. Each example here is cross-linked to its spec
section, and every one is **machine-verified**: the test
[`polydat/tests/doc_examples_test.rs`](../../tests/doc_examples_test.rs)
asserts that the hand-built AST and the spec's grammar source **project
to identical canonical syntax**:

```text
pp_file(builder_ast) == pp_file(parse(grammar_src))
```

That equality is the proof that two independent construction paths — the
grammar (parsed) and the builder (hand-assembled) — converge on one
canonical kernel definition. The same test also confirms the builder
snippets below appear verbatim in this file, so the documentation cannot
silently drift from the code that runs.

> **Why AST builders rather than a “use polydat from Rust” tour?** The
> goal is to prove *the same kernel definition* via *the projected
> syntax the runtime gives back*. Building a `PolydatFile` and projecting
> it with `pp_file` is the most direct expression of that: it compares
> construction paths at the canonical-syntax layer. For driving a
> compiled kernel (`set_inputs`/`pull`, the typed `Dataflow` path), see
> §[7](#sec-driving) at the end.

<a id="sec-setup"></a>
## 1. Imports and helpers

All AST types are public with public fields. Synthesized nodes use
`Span { line: 0, col: 0 }` (the in-crate convention). These small helpers
keep the builders readable and are used by every example below:

```rust
use polydat::dsl::ast::{
    Arg, Binding, BindingModifier, BinOpKind, CallExpr, CursorDecl, Expr,
    ExternPort, InputDecl, ModuleDef, PolydatFile, Statement, TypedParam,
};
use polydat::dsl::lexer::Span;
use polydat::ast::PortType;

fn sp() -> Span { Span { line: 0, col: 0 } }
fn input(name: &str, ty: &str) -> Statement {
    Statement::InputDecl(InputDecl { name: name.into(), ty: Some(ty.into()), span: sp() })
}
fn id(n: &str) -> Expr { Expr::Ident(n.into(), sp()) }
fn int(n: u64) -> Expr { Expr::IntLit(n, sp()) }
fn flt(n: f64) -> Expr { Expr::FloatLit(n, sp()) }
fn call(func: &str, args: Vec<Expr>) -> Expr {
    Expr::Call(CallExpr {
        func: func.into(),
        args: args.into_iter().map(Arg::Positional).collect(),
        span: sp(),
    })
}
fn binop(l: Expr, op: BinOpKind, r: Expr) -> Expr {
    Expr::BinOp(Box::new(l), op, Box::new(r))
}
fn bind(target: &str, value: Expr) -> Statement {
    Statement::Binding(Binding {
        targets: vec![target.into()], value,
        modifier: BindingModifier::NONE, span: sp(),
    })
}
fn bind_multi(targets: &[&str], value: Expr) -> Statement {
    Statement::Binding(Binding {
        targets: targets.iter().map(|s| s.to_string()).collect(), value,
        modifier: BindingModifier::NONE, span: sp(),
    })
}
fn file(statements: Vec<Statement>) -> PolydatFile { PolydatFile { statements } }
```

To project any built file to canonical source, call
`polydat::dsl::pprint::pp_file(&file)`. To compile it directly (no source
string), call `polydat::dsl::compile::compile_ast(&file)` — note that
`compile_ast` is reachable only via its full module path, as it is not in
the `dsl` re-export list.

<a id="p-minimal"></a>
## 2. Minimal kernel — input, hash, mod

Mirrors [spec §3 “Bindings”](polydat_grammar.md#sec-bindings):

```text
input cycle: u64
hashed := hash(cycle)
user_id := mod(hashed, 1000000)
```

```rust
fn build_minimal() -> PolydatFile {
    file(vec![
        input("cycle", "u64"),
        bind("hashed", call("hash", vec![id("cycle")])),
        bind("user_id", call("mod", vec![id("hashed"), int(1_000_000)])),
    ])
}
```

`pp_file(build_minimal())` is byte-for-byte the spec's source — there is
no binop, interpolation, or float to canonicalize.

<a id="p-destructure"></a>
## 3. Tuple-destructuring binding

Mirrors [spec §3.1](polydat_grammar.md#sec-destructuring). A multi-target
binding uses `bind_multi`; the target order is the projection order.

```rust
fn build_destructure() -> PolydatFile {
    file(vec![
        input("cycle", "u64"),
        bind_multi(&["region", "store", "tx"], call("mixed_radix", vec![id("cycle"), int(50), int(200), int(0)])),
        bind("region_id", call("mod", vec![call("hash", vec![id("region")]), int(10_000)])),
        bind("store_id", call("mod", vec![call("hash", vec![call("interleave", vec![id("region"), id("store")])]), int(100_000)])),
    ])
}
```

<a id="p-tuple-input"></a>
## 4. Tuple input — building the desugared form

Mirrors [spec §4.1](polydat_grammar.md#sec-input-tuple). The grammar's
`input (cycle: u64, thread: u64)` desugars at parse time to **two**
`InputDecl`s, so the builder constructs the two decls directly — matching
the canonical projection rather than the surface tuple.

```rust
fn build_tuple_input() -> PolydatFile {
    file(vec![
        input("cycle", "u64"),
        input("thread", "u64"),
        bind("combined", call("interleave", vec![id("cycle"), id("thread")])),
        bind("row_key", call("mod", vec![call("hash", vec![id("combined")]), int(1_000_000)])),
    ])
}
```

<a id="p-extern"></a>
## 5. Extern in an arithmetic expression

Mirrors [spec §12 “Externs”](polydat_grammar.md#sec-externs). An
`ExternPort` with `default: None`; the `*` becomes a `BinOpKind::Mul`
that projects fully parenthesized as `(cycle * scale)`.

```rust
fn build_extern() -> PolydatFile {
    file(vec![
        input("cycle", "u64"),
        Statement::ExternPort(ExternPort { name: "scale".into(), typ: "u64".into(), default: None, span: sp() }),
        bind("result", binop(id("cycle"), BinOpKind::Mul, id("scale"))),
    ])
}
```

<a id="p-cursor-over"></a>
## 6. Cursor with an `over` clause and field access

Mirrors [spec §11.2 “Cursors”](polydat_grammar.md#sec-cursors). The
`over` clause is `Some(id("p"))`; chained field access `q.cursor.idx` is
already flattened in the AST to a single `FieldAccess` whose source is
`q__cursor`. The cast binds tighter than `/`.

```rust
fn build_cursor_over() -> PolydatFile {
    file(vec![
        Statement::Cursor(CursorDecl {
            name: "q".into(),
            constructor: call("range", vec![int(0), int(100)]),
            over: Some(id("p")),
            span: sp(),
        }),
        bind("i", Expr::FieldAccess { source: "q__cursor".into(), field: "idx".into(), span: sp() }),
        bind("ratio", binop(Expr::Cast(Box::new(id("i")), PortType::F64, sp()), BinOpKind::Div, flt(100.0))),
    ])
}
```

This is the example the `pp_cursor` fix was made for: before it, the
`over p` clause was dropped on projection and this builder could not have
matched the grammar source.

<a id="p-module"></a>
## 7. Module definition

Mirrors [spec §13 “Module definitions”](polydat_grammar.md#sec-modules).
A `ModuleDef` carries typed `params`/`outputs` and a `body` of
statements; the projector indents the body four spaces inside `{ … }`.

```rust
fn build_module() -> PolydatFile {
    let body = vec![
        bind("pos", call("to_f64", vec![binop(id("input"), BinOpKind::Mod, id("period"))])),
        bind("per", call("to_f64", vec![id("period")])),
        bind("value", call("sin", vec![binop(binop(id("pos"), BinOpKind::Div, id("per")), BinOpKind::Mul, flt(std::f64::consts::TAU))])),
    ];
    file(vec![Statement::ModuleDef(ModuleDef {
        name: "sine_wave".into(),
        params: vec![
            TypedParam { name: "input".into(), typ: "u64".into() },
            TypedParam { name: "period".into(), typ: "u64".into() },
        ],
        outputs: vec![TypedParam { name: "value".into(), typ: "f64".into() }],
        body,
        span: sp(),
    })])
}
```

<a id="sec-driving"></a>
## 8. Driving a compiled kernel (reference)

The builders above are verified at the *syntax* layer. To verify at the
*behaviour* layer, compile and drive a kernel. The simplest path is from
source; the typed, non-panicking path is the `Dataflow` trait.

```rust
use polydat::dsl::compile_polydat;
let mut kernel = compile_polydat(
    "input cycle: u64\nhashed := hash(cycle)\nuser_id := mod(hashed, 1000000)\n",
).unwrap();
kernel.set_inputs(&[42]);                 // writes the `cycle` coordinate
let user_id = kernel.pull("user_id").as_u64();   // panics on unknown name/type
assert!(user_id < 1_000_000);
```

```rust
use polydat::dsl::compile_polydat;
use polydat::kernel::Dataflow;
use polydat::ast::Value;
let mut k = compile_polydat("input cycle: u64\nextern n: u64\nout := n\n").unwrap();
k.set_wire("n", Value::U64(5)).expect("typed write");   // u64->f64 slots auto-adapt
assert_eq!(k.get_wire("n"), Some(Value::U64(5)));        // None on unknown wire, never panics
```

A grammar snippet's source and its hand-built AST compile to kernels that
produce identical `pull` results — the behaviour-layer counterpart to the
projection equality this guide is built around. The
[verification test](../../tests/doc_examples_test.rs) checks the syntax
equality directly; compiling both paths and comparing `pull` outputs is
the natural extension when a behavioural guarantee is needed.
