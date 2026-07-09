// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Workspace-level verification for the definitive grammar spec.
//!
//! Guards three properties of
//! [`docs/design/polydat_grammar.md`](../docs/design/polydat_grammar.md)
//! and its programmatic companion
//! [`polydat_grammar_programmatic.md`](../docs/design/polydat_grammar_programmatic.md):
//!
//! 1. **Every ` ```polydat ` block parses and round-trips idempotently.**
//!    `pp_file(parse(src))` re-parses and re-projects to itself — the
//!    documented projection contract (`pprint.rs`). A doc example that
//!    is not real, parseable, canonical Polydat fails here.
//! 2. **Every ` ```polydat compile ` block compiles.** The
//!    `compile`-tagged subset is run through `compile_polydat`. Blocks
//!    additionally tagged `vectordata` are compiled only when that
//!    feature is on (parsing is feature-independent; compiling the
//!    vectordata cursor sugar is not).
//! 3. **The programmatic examples build the same kernel.** For each
//!    paired example, a hand-assembled `PolydatFile` projects to exactly
//!    the same canonical syntax as the spec's grammar source
//!    (`pp_file(builder) == pp_file(parse(grammar_src))`), the example
//!    appears as a ` ```polydat ` block in the spec, and the builder
//!    code appears verbatim in the programmatic doc (so the doc cannot
//!    silently drift from the verified code).
//!
//! The docs are read with `include_str!` (cwd-independent), mirroring
//! `adapter_catalog_invariants.rs`'s doc-as-spec pattern.

use polydat::ast::PortType;
use polydat::dsl::ast::{
    Arg, Binding, BindingModifier, BinOpKind, CallExpr, CursorDecl, Expr,
    ExternPort, InputDecl, ModuleDef, PolydatFile, Statement, TypedParam,
};
use polydat::dsl::compile_polydat;
use polydat::dsl::lexer::Span;
use polydat::dsl::pprint::pp_file;
use polydat::dsl::{lexer, parser};

const GRAMMAR_DOC: &str = include_str!("../docs/design/polydat_grammar.md");
const PROG_DOC: &str = include_str!("../docs/design/polydat_grammar_programmatic.md");

// ── Fenced-block extraction ───────────────────────────────────────

struct Block {
    /// Info-string directives after the language token (e.g. `compile`).
    directives: Vec<String>,
    /// The block body (fence lines excluded), newline-terminated.
    body: String,
    /// 1-based line where the opening fence sits, for diagnostics.
    start_line: usize,
}

/// Extract every fenced block whose info string's first token equals
/// `lang`. No line of block *content* in these docs begins with a fence,
/// so a `trim_start` test on ```` ``` ```` reliably toggles in/out.
fn extract_blocks(md: &str, lang: &str) -> Vec<Block> {
    let mut out = Vec::new();
    let mut lines = md.lines().enumerate();
    while let Some((idx, line)) = lines.next() {
        let trimmed = line.trim_start();
        if !trimmed.starts_with("```") {
            continue;
        }
        let info = trimmed.trim_start_matches('`');
        let mut tokens = info.split([' ', ',']).filter(|t| !t.is_empty());
        let is_match = tokens.next() == Some(lang);
        let directives: Vec<String> = tokens.map(|t| t.to_string()).collect();
        // Consume the body up to the closing fence.
        let mut body = String::new();
        let mut closed = false;
        for (_, body_line) in lines.by_ref() {
            if body_line.trim_start().starts_with("```") {
                closed = true;
                break;
            }
            body.push_str(body_line);
            body.push('\n');
        }
        debug_assert!(closed, "unterminated fence at line {}", idx + 1);
        if is_match {
            out.push(Block { directives, body, start_line: idx + 1 });
        }
    }
    out
}

fn parse(src: &str) -> Result<PolydatFile, String> {
    let tokens = lexer::lex(src)?;
    parser::parse(tokens)
}

fn project(src: &str, label: &str) -> String {
    let ast = parse(src)
        .unwrap_or_else(|e| panic!("{label} failed to parse: {e}\n--- source ---\n{src}"));
    pp_file(&ast)
}

// ── Property 1: idempotent round-trip ─────────────────────────────

#[test]
fn polydat_blocks_round_trip_idempotently() {
    let blocks = extract_blocks(GRAMMAR_DOC, "polydat");
    assert!(
        !blocks.is_empty(),
        "no ```polydat blocks found in polydat_grammar.md — the spec must \
         carry at least one verifiable example"
    );
    for b in &blocks {
        let loc = format!("```polydat block at line {}", b.start_line);
        let p1 = project(&b.body, &loc);
        let p2 = project(&p1, &format!("re-projection of {loc}"));
        assert_eq!(
            p1, p2,
            "{loc} is not idempotent under projection.\n\
             --- original ---\n{}\n--- first projection ---\n{p1}\n\
             --- second projection ---\n{p2}",
            b.body
        );
    }
}

// ── Property 2: compile-tagged blocks compile ─────────────────────

#[test]
fn polydat_compile_blocks_compile() {
    let mut compiled = 0;
    for b in extract_blocks(GRAMMAR_DOC, "polydat") {
        if !b.directives.iter().any(|d| d == "compile") {
            continue;
        }
        if b.directives.iter().any(|d| d == "vectordata") && !cfg!(feature = "vectordata") {
            continue;
        }
        compile_polydat(&b.body).unwrap_or_else(|e| {
            panic!(
                "```polydat compile block at line {} failed to compile: {e}\n\
                 --- source ---\n{}",
                b.start_line, b.body
            )
        });
        compiled += 1;
    }
    assert!(
        compiled > 0,
        "expected at least one ```polydat compile block to compile"
    );
}

// ── Property 3: programmatic builders match the grammar ───────────

/// A paired example: an anchor, the spec grammar source, a verbatim
/// snippet that must appear in the programmatic doc, and the builder.
struct Paired {
    anchor: &'static str,
    grammar_src: &'static str,
    /// A distinctive line of the builder that must appear verbatim in
    /// `polydat_grammar_programmatic.md` (drift guard).
    doc_snippet: &'static str,
    build: fn() -> PolydatFile,
}

#[test]
fn programmatic_examples_match_grammar() {
    // Canonical projections of every spec ```polydat block, so we can
    // confirm each paired example actually appears in the spec.
    let spec_projections: Vec<String> = extract_blocks(GRAMMAR_DOC, "polydat")
        .iter()
        .map(|b| project(&b.body, "spec block"))
        .collect();

    let examples = paired_examples();
    assert!(!examples.is_empty());

    for p in &examples {
        let canonical = project(p.grammar_src, p.anchor);

        // (a) The builder converges on the same canonical syntax.
        let built = pp_file(&(p.build)());
        assert_eq!(
            built, canonical,
            "builder `{}` does not project to its grammar source.\n\
             --- builder projection ---\n{built}\n--- grammar projection ---\n{canonical}",
            p.anchor
        );

        // (b) The example is actually present in the spec.
        assert!(
            spec_projections.iter().any(|s| s == &canonical),
            "paired example `{}` is not present as a ```polydat block in \
             polydat_grammar.md",
            p.anchor
        );

        // (c) The programmatic doc anchors and shows the builder.
        assert!(
            PROG_DOC.contains(p.anchor),
            "anchor `{}` missing from polydat_grammar_programmatic.md",
            p.anchor
        );
        assert!(
            PROG_DOC.contains(p.doc_snippet),
            "builder snippet for `{}` not found verbatim in \
             polydat_grammar_programmatic.md — doc and code have drifted.\n\
             expected substring:\n{}",
            p.anchor, p.doc_snippet
        );
    }
}

// ── Builders (must stay identical to the snippets in the prog doc) ──

fn sp() -> Span {
    Span { line: 0, col: 0 }
}
fn input(name: &str, ty: &str) -> Statement {
    Statement::InputDecl(InputDecl { name: name.into(), ty: Some(ty.into()), span: sp() })
}
fn id(n: &str) -> Expr {
    Expr::Ident(n.into(), sp())
}
fn int(n: u64) -> Expr {
    Expr::IntLit(n, sp())
}
fn flt(n: f64) -> Expr {
    Expr::FloatLit(n, sp())
}
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
        targets: vec![target.into()],
        value,
        modifier: BindingModifier::NONE,
        type_annotation: None,
        span: sp(),
    })
}
fn bind_multi(targets: &[&str], value: Expr) -> Statement {
    Statement::Binding(Binding {
        targets: targets.iter().map(|s| s.to_string()).collect(),
        value,
        modifier: BindingModifier::NONE,
        type_annotation: None,
        span: sp(),
    })
}
fn file(statements: Vec<Statement>) -> PolydatFile {
    PolydatFile { statements }
}

fn build_minimal() -> PolydatFile {
    file(vec![
        input("cycle", "u64"),
        bind("hashed", call("hash", vec![id("cycle")])),
        bind("user_id", call("mod", vec![id("hashed"), int(1_000_000)])),
    ])
}

fn build_destructure() -> PolydatFile {
    file(vec![
        input("cycle", "u64"),
        bind_multi(&["region", "store", "tx"], call("mixed_radix", vec![id("cycle"), int(50), int(200), int(0)])),
        bind("region_id", call("mod", vec![call("hash", vec![id("region")]), int(10_000)])),
        bind("store_id", call("mod", vec![call("hash", vec![call("interleave", vec![id("region"), id("store")])]), int(100_000)])),
    ])
}

fn build_tuple_input() -> PolydatFile {
    file(vec![
        input("cycle", "u64"),
        input("thread", "u64"),
        bind("combined", call("interleave", vec![id("cycle"), id("thread")])),
        bind("row_key", call("mod", vec![call("hash", vec![id("combined")]), int(1_000_000)])),
    ])
}

fn build_extern() -> PolydatFile {
    file(vec![
        input("cycle", "u64"),
        Statement::ExternPort(ExternPort { name: "scale".into(), typ: "u64".into(), default: None, span: sp() }),
        bind("result", binop(id("cycle"), BinOpKind::Mul, id("scale"))),
    ])
}

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

fn paired_examples() -> Vec<Paired> {
    vec![
        Paired {
            anchor: "p-minimal",
            grammar_src: "input cycle: u64\nhashed := hash(cycle)\nuser_id := mod(hashed, 1000000)\n",
            doc_snippet: "bind(\"user_id\", call(\"mod\", vec![id(\"hashed\"), int(1_000_000)])),",
            build: build_minimal,
        },
        Paired {
            anchor: "p-destructure",
            grammar_src: "input cycle: u64\n(region, store, tx) := mixed_radix(cycle, 50, 200, 0)\nregion_id := mod(hash(region), 10000)\nstore_id := mod(hash(interleave(region, store)), 100000)\n",
            doc_snippet: "bind_multi(&[\"region\", \"store\", \"tx\"], call(\"mixed_radix\", vec![id(\"cycle\"), int(50), int(200), int(0)])),",
            build: build_destructure,
        },
        Paired {
            anchor: "p-tuple-input",
            grammar_src: "input (cycle: u64, thread: u64)\ncombined := interleave(cycle, thread)\nrow_key := mod(hash(combined), 1000000)\n",
            doc_snippet: "bind(\"row_key\", call(\"mod\", vec![call(\"hash\", vec![id(\"combined\")]), int(1_000_000)])),",
            build: build_tuple_input,
        },
        Paired {
            anchor: "p-extern",
            grammar_src: "input cycle: u64\nextern scale: u64\nresult := cycle * scale\n",
            doc_snippet: "ExternPort { name: \"scale\".into(), typ: \"u64\".into(), default: None, span: sp() }",
            build: build_extern,
        },
        Paired {
            anchor: "p-cursor-over",
            grammar_src: "cursor q = range(0, 100) over p\ni := q.cursor.idx\nratio := (i as f64) / 100.0\n",
            doc_snippet: "over: Some(id(\"p\")),",
            build: build_cursor_over,
        },
        Paired {
            anchor: "p-module",
            grammar_src: "sine_wave(input: u64, period: u64) -> (value: f64) := {\n    pos := to_f64(input % period)\n    per := to_f64(period)\n    value := sin((pos / per) * 6.283185307179586)\n}\n",
            doc_snippet: "name: \"sine_wave\".into(),",
            build: build_module,
        },
    ]
}
