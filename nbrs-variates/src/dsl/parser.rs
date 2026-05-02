// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Recursive descent parser for the GK DSL.
//!
//! Parses a token stream (from the lexer) into an AST.
//! Infix arithmetic expressions (`+`, `-`, `*`, `/`, `%`, `^`) are
//! handled by a Pratt (precedence-climbing) parser that produces
//! `Expr::BinOp` nodes, later desugared by the compiler into
//! function calls.
//!
//! ## String interpolation
//!
//! Per SRD 10 §"String Interpolation", string literals containing
//! `{ … }` placeholders are desugared to a `printf` call over
//! the placeholder bodies. The bodies are parsed as full GK
//! expressions via [`parse_expression`] — same entry the rest of
//! the language uses — so anything that can appear on a binding
//! right-hand side can appear inside a placeholder.
//!
//! Examples:
//!
//! - `"hello"` — no placeholders → `Expr::StringLit("hello")`.
//! - `"{name}"` — bare identifier → `printf("{}", name)`.
//! - `"x={a + b}"` — infix expression → `printf("x={}", a + b)`.
//! - `"{format_u64(hash(cycle), 10)}@example.com"` — nested call
//!   → `printf("{}@example.com", format_u64(hash(cycle), 10))`.
//! - `"{row.id}"` — field access → `printf("{}", row.id)`.
//! - `"{{literal braces}}"` — escaped → stays a `StringLit` (printf
//!   emits `{` / `}` from `{{` / `}}` at format time).
//! - `"x={:05}"` — printf format spec, not a GK expression → stays
//!   a `StringLit`; the user is calling printf by hand.
//! - `"missing close {abc"` — unterminated placeholder → stays a
//!   `StringLit`.
//!
//! The desugaring is pure syntactic sugar. The resulting `printf`
//! call goes through the standard binding/assembly path: each
//! placeholder expression compiles to a node, the printf node
//! ingests their outputs as wires, and at evaluation time
//! `Value::to_display_string()` renders each input into its slot.
//! No special runtime support is needed beyond `printf`.

use crate::dsl::ast::*;
use crate::dsl::lexer::{Token, TokenKind, Span};

/// Parser state.
struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    fn new(tokens: Vec<Token>) -> Self {
        Self { tokens, pos: 0 }
    }

    fn peek(&self) -> &TokenKind {
        &self.tokens[self.pos].kind
    }

    fn span(&self) -> Span {
        self.tokens[self.pos].span
    }

    fn advance(&mut self) -> &Token {
        let tok = &self.tokens[self.pos];
        if self.pos < self.tokens.len() - 1 {
            self.pos += 1;
        }
        tok
    }

    fn expect(&mut self, expected: &TokenKind) -> Result<&Token, String> {
        if self.peek() == expected {
            Ok(self.advance())
        } else {
            Err(format!(
                "expected {:?}, got {:?} at line {}, col {}",
                expected, self.peek(), self.span().line, self.span().col
            ))
        }
    }

    fn expect_ident(&mut self) -> Result<String, String> {
        match self.peek().clone() {
            TokenKind::Ident(name) => {
                self.advance();
                Ok(name)
            }
            _ => Err(format!(
                "expected identifier, got {:?} at line {}, col {}",
                self.peek(), self.span().line, self.span().col
            )),
        }
    }

    fn at_eof(&self) -> bool {
        matches!(self.peek(), TokenKind::Eof)
    }
}

/// Parse a token stream into a GkFile AST.
pub fn parse(tokens: Vec<Token>) -> Result<GkFile, String> {
    let mut parser = Parser::new(tokens);
    let mut statements = Vec::new();

    while !parser.at_eof() {
        statements.push(parse_statement(&mut parser)?);
    }

    Ok(GkFile { statements })
}

/// Parse a token stream as a single GK expression.
///
/// Used by string-interpolation desugaring to compile placeholder
/// bodies (`{ … }` inside string literals) the same way any
/// other binding right-hand side is compiled. Identifiers,
/// nested function calls, infix arithmetic, and field access
/// all work uniformly because this is the same `parse_expr`
/// entry the compiler uses elsewhere.
///
/// ```ignore
/// // "{format_u64(hash(cycle), 10)}" → printf("{}", format_u64(hash(cycle), 10))
/// // "{a + b}"                       → printf("{}", a + b)
/// ```
///
/// Returns an error if the tokens don't form a single complete
/// expression, or if there are trailing tokens after the
/// expression ends.
pub fn parse_expression(tokens: Vec<Token>) -> Result<Expr, String> {
    let mut parser = Parser::new(tokens);
    let expr = parse_expr(&mut parser)?;
    if !parser.at_eof() {
        let span = parser.span();
        return Err(format!(
            "expected end of expression at line {}, col {}, got {:?}",
            span.line, span.col, parser.peek()
        ));
    }
    Ok(expr)
}

fn parse_statement(p: &mut Parser) -> Result<Statement, String> {
    match p.peek() {
        TokenKind::Pragma => parse_pragma(p),
        TokenKind::Inputs => parse_inputs(p),
        TokenKind::Init => parse_init_binding(p),
        TokenKind::Extern => parse_extern_port(p),
        TokenKind::Cursor => parse_cursor_decl(p),
        TokenKind::Shared | TokenKind::Final => parse_modified_binding(p),
        TokenKind::LParen => parse_destructuring_binding(p),
        TokenKind::Ident(_) => {
            // Lookahead to distinguish:
            //   name := expr              → cycle binding
            //   name(p: type) -> ... := { → module def
            if is_module_def(p) {
                parse_module_def(p)
            } else {
                parse_cycle_binding(p)
            }
        }
        _ => Err(format!(
            "unexpected token {:?} at line {}, col {}",
            p.peek(), p.span().line, p.span().col
        )),
    }
}

/// `pragma <name>` — first-class module directive. The pragma name
/// is a bare identifier; arguments are not currently supported (the
/// recognised set in SRD 15 has none, and adding them later is
/// non-breaking). See SRD 15 §"Module-Level Pragmas".
fn parse_pragma(p: &mut Parser) -> Result<Statement, String> {
    let span = p.span();
    p.expect(&TokenKind::Pragma)?;
    let name = p.expect_ident()?;
    Ok(Statement::Pragma { name, span })
}

/// Lookahead: is this a module def? Pattern: ident ( ident : ident ...
fn is_module_def(p: &Parser) -> bool {
    // Need at least: ident ( ident : type
    if p.pos + 4 >= p.tokens.len() { return false; }
    matches!(&p.tokens[p.pos].kind, TokenKind::Ident(_))
        && matches!(&p.tokens[p.pos + 1].kind, TokenKind::LParen)
        && matches!(&p.tokens[p.pos + 2].kind, TokenKind::Ident(_))
        && matches!(&p.tokens[p.pos + 3].kind, TokenKind::Colon)
}

/// `name(param: type, ...) -> (output: type, ...) := { body }`
fn parse_module_def(p: &mut Parser) -> Result<Statement, String> {
    let span = p.span();
    let name = p.expect_ident()?;

    // Parse params: (name: type, ...)
    p.expect(&TokenKind::LParen)?;
    let mut params = Vec::new();
    while !matches!(p.peek(), TokenKind::RParen) {
        let pname = p.expect_ident()?;
        p.expect(&TokenKind::Colon)?;
        let ptype = p.expect_ident()?;
        params.push(TypedParam { name: pname, typ: ptype });
        if matches!(p.peek(), TokenKind::Comma) {
            p.advance();
        }
    }
    p.expect(&TokenKind::RParen)?;

    // Parse -> (output: type, ...)
    p.expect(&TokenKind::Arrow)?;
    p.expect(&TokenKind::LParen)?;
    let mut outputs = Vec::new();
    while !matches!(p.peek(), TokenKind::RParen) {
        let oname = p.expect_ident()?;
        p.expect(&TokenKind::Colon)?;
        let otype = p.expect_ident()?;
        outputs.push(TypedParam { name: oname, typ: otype });
        if matches!(p.peek(), TokenKind::Comma) {
            p.advance();
        }
    }
    p.expect(&TokenKind::RParen)?;

    // Parse := { body }
    p.expect(&TokenKind::ColonEq)?;
    p.expect(&TokenKind::LBrace)?;

    let mut body = Vec::new();
    while !matches!(p.peek(), TokenKind::RBrace | TokenKind::Eof) {
        body.push(parse_statement(p)?);
    }
    p.expect(&TokenKind::RBrace)?;

    Ok(Statement::ModuleDef(ModuleDef {
        name,
        params,
        outputs,
        body,
        span,
    }))
}

/// `extern name: type = default`
fn parse_extern_port(p: &mut Parser) -> Result<Statement, String> {
    let span = p.span();
    p.advance(); // consume 'extern'

    let name = p.expect_ident()?;
    p.expect(&TokenKind::Colon)?;
    let typ = p.expect_ident()?;

    // Optional default: = expr
    let default = if matches!(p.peek(), TokenKind::Eq) {
        p.advance(); // consume '='
        Some(parse_expr(p)?)
    } else {
        None
    };

    Ok(Statement::ExternPort(ExternPort { name, typ, default, span }))
}

/// `inputs := (name1, name2, ...)` or `inputs := ()` (zero inputs)
fn parse_inputs(p: &mut Parser) -> Result<Statement, String> {
    let span = p.span();
    p.advance(); // consume 'coordinates' / 'inputs'
    p.expect(&TokenKind::ColonEq)?;
    p.expect(&TokenKind::LParen)?;

    // Allow empty input list: `inputs := ()`
    if matches!(p.peek(), TokenKind::RParen) {
        p.advance(); // consume ')'
        return Ok(Statement::Inputs(vec![], span));
    }

    let mut names = Vec::new();
    loop {
        names.push(p.expect_ident()?);
        if matches!(p.peek(), TokenKind::Comma) {
            p.advance();
        } else {
            break;
        }
    }
    p.expect(&TokenKind::RParen)?;

    Ok(Statement::Inputs(names, span))
}

/// `init name = expr`
fn parse_init_binding(p: &mut Parser) -> Result<Statement, String> {
    parse_init_binding_with_modifier(p, BindingModifier::None)
}

fn parse_init_binding_with_modifier(p: &mut Parser, modifier: BindingModifier) -> Result<Statement, String> {
    let span = p.span();
    p.advance(); // consume 'init'
    let name = p.expect_ident()?;
    p.expect(&TokenKind::Eq)?;
    let value = parse_expr(p)?;

    Ok(Statement::InitBinding(InitBinding { name, value, modifier, span }))
}

/// `cursor name = Cursor()` or `cursor name = expr`
fn parse_cursor_decl(p: &mut Parser) -> Result<Statement, String> {
    let span = p.span();
    p.advance(); // consume 'cursor'
    let name = p.expect_ident()?;
    p.expect(&TokenKind::Eq)?;
    let constructor = parse_expr(p)?;
    Ok(Statement::Cursor(CursorDecl { name, constructor, span }))
}

/// `shared name := expr` or `final name := expr`
fn parse_modified_binding(p: &mut Parser) -> Result<Statement, String> {
    let modifier = match p.peek() {
        TokenKind::Shared => BindingModifier::Shared,
        TokenKind::Final => BindingModifier::Final,
        _ => unreachable!(),
    };
    p.advance(); // consume 'shared' or 'final'

    match p.peek() {
        TokenKind::Init => parse_init_binding_with_modifier(p, modifier),
        TokenKind::Ident(_) => parse_cycle_binding_with_modifier(p, modifier),
        _ => Err(format!(
            "expected binding after {:?} at line {}, col {}",
            modifier, p.span().line, p.span().col
        )),
    }
}

/// `name := expr`
fn parse_cycle_binding(p: &mut Parser) -> Result<Statement, String> {
    parse_cycle_binding_with_modifier(p, BindingModifier::None)
}

fn parse_cycle_binding_with_modifier(p: &mut Parser, modifier: BindingModifier) -> Result<Statement, String> {
    let span = p.span();
    let name = p.expect_ident()?;
    p.expect(&TokenKind::ColonEq)?;
    let value = parse_expr(p)?;

    Ok(Statement::CycleBinding(CycleBinding {
        targets: vec![name],
        value,
        modifier,
        span,
    }))
}

/// `(a, b, c) := expr`
fn parse_destructuring_binding(p: &mut Parser) -> Result<Statement, String> {
    let span = p.span();
    p.advance(); // consume '('
    let mut targets = Vec::new();
    loop {
        targets.push(p.expect_ident()?);
        if matches!(p.peek(), TokenKind::Comma) {
            p.advance();
        } else {
            break;
        }
    }
    p.expect(&TokenKind::RParen)?;
    p.expect(&TokenKind::ColonEq)?;
    let value = parse_expr(p)?;

    Ok(Statement::CycleBinding(CycleBinding {
        targets,
        value,
        modifier: BindingModifier::None,
        span,
    }))
}

/// Parse an expression with operator precedence (Pratt parsing).
///
/// Handles infix arithmetic operators (`+`, `-`, `*`, `/`, `%`, `^`)
/// with correct precedence and associativity. Atoms are literals,
/// identifiers, function calls, parenthesized groups, and unary negation.
fn parse_expr(p: &mut Parser) -> Result<Expr, String> {
    parse_expr_bp(p, 0)
}

/// Pratt parser core: parse expression with minimum binding power.
///
/// Precedence levels (lowest to highest):
///   Level 1: `==` `!=`               — bp (1, 2)
///   Level 2: `<` `>` `<=` `>=`       — bp (3, 4)
///   Level 3: `|`  (BitOr)            — bp (5, 6)
///   Level 4: `^`  (BitXor)           — bp (7, 8)
///   Level 5: `&`  (BitAnd)           — bp (9, 10)
///   Level 6: `<<` `>>` (Shl/Shr)     — bp (11, 12)
///   Level 7: `+` `-` (Add/Sub)       — bp (13, 14)
///   Level 8: `*` `/` `%`             — bp (15, 16)
///   Level 9: `**` (Pow, right)       — bp (18, 17)
///   Level 10: `-` `!` (unary, in parse_atom)
///
/// Comparison ops sit below all arithmetic/bitwise so
/// `a + b < c * d` parses as `(a + b) < (c * d)`. Equality is
/// below relational so `a < b == c` parses as `(a < b) == c`,
/// matching C/Rust convention.
fn parse_expr_bp(p: &mut Parser, min_bp: u8) -> Result<Expr, String> {
    let mut lhs = parse_atom(p)?;

    loop {
        let op = match p.peek() {
            TokenKind::EqEq      => Some((BinOpKind::Eq,    1,  2)),
            TokenKind::BangEq    => Some((BinOpKind::Ne,    1,  2)),
            TokenKind::Lt        => Some((BinOpKind::Lt,    3,  4)),
            TokenKind::Gt        => Some((BinOpKind::Gt,    3,  4)),
            TokenKind::LtEq      => Some((BinOpKind::Le,    3,  4)),
            TokenKind::GtEq      => Some((BinOpKind::Ge,    3,  4)),
            TokenKind::Pipe      => Some((BinOpKind::BitOr,  5,  6)),
            TokenKind::Caret     => Some((BinOpKind::BitXor, 7,  8)),
            TokenKind::Ampersand => Some((BinOpKind::BitAnd, 9, 10)),
            TokenKind::ShiftLeft => Some((BinOpKind::Shl,   11, 12)),
            TokenKind::ShiftRight=> Some((BinOpKind::Shr,   11, 12)),
            TokenKind::Plus      => Some((BinOpKind::Add,   13, 14)),
            TokenKind::Minus     => Some((BinOpKind::Sub,   13, 14)),
            TokenKind::Star      => Some((BinOpKind::Mul,   15, 16)),
            TokenKind::Slash     => Some((BinOpKind::Div,   15, 16)),
            TokenKind::Percent   => Some((BinOpKind::Mod,   15, 16)),
            TokenKind::StarStar  => Some((BinOpKind::Pow,   18, 17)), // right-associative
            _ => None,
        };

        let Some((op_kind, l_bp, r_bp)) = op else { break; };
        if l_bp < min_bp { break; }

        p.advance(); // consume operator token
        let rhs = parse_expr_bp(p, r_bp)?;
        lhs = Expr::BinOp(Box::new(lhs), op_kind, Box::new(rhs));
    }

    Ok(lhs)
}

/// Parse an atomic expression: literal, identifier, function call,
/// parenthesized group, or unary negation.
fn parse_atom(p: &mut Parser) -> Result<Expr, String> {
    let span = p.span();

    match p.peek().clone() {
        TokenKind::Minus => {
            // Unary negation: `-expr`
            p.advance();
            let inner = parse_atom(p)?;
            Ok(Expr::UnaryNeg(Box::new(inner), span))
        }
        TokenKind::Bang => {
            // Unary bitwise NOT: `!expr`
            p.advance();
            let inner = parse_atom(p)?;
            Ok(Expr::UnaryBitNot(Box::new(inner), span))
        }
        TokenKind::LParen => {
            // Parenthesized grouping (not a function call — that is
            // handled inside the Ident branch below).
            p.advance(); // consume '('
            let inner = parse_expr(p)?;
            p.expect(&TokenKind::RParen)?;
            Ok(inner)
        }
        TokenKind::StringLit(s) => {
            p.advance();
            Ok(parse_interpolated_string(s, span))
        }
        TokenKind::IntLit(v) => {
            p.advance();
            Ok(Expr::IntLit(v, span))
        }
        TokenKind::FloatLit(v) => {
            p.advance();
            Ok(Expr::FloatLit(v, span))
        }
        TokenKind::LBracket => {
            parse_array_lit(p)
        }
        TokenKind::Ident(name) => {
            p.advance();
            if matches!(p.peek(), TokenKind::LParen) {
                // Function call: name(args...)
                parse_call(p, name, span)
            } else if matches!(p.peek(), TokenKind::Dot) {
                // Source field projection: base.ordinal
                p.advance(); // consume '.'
                let field = p.expect_ident()?;
                Ok(Expr::FieldAccess { source: name, field, span })
            } else {
                Ok(Expr::Ident(name, span))
            }
        }
        _ => Err(format!(
            "expected expression, got {:?} at line {}, col {}",
            p.peek(), span.line, span.col
        )),
    }
}

/// Desugar a string literal that contains `{ … }` placeholders
/// into a `printf` call.
///
/// SRD 10 §"String Interpolation": `{name}` references resolve
/// to other bindings or workload parameters; the compiler
/// splits the template into a format string and the placeholder
/// expressions, then wires them into a `Printf` node that
/// formats at evaluation time. This is pure syntactic sugar —
/// no special runtime support beyond the standard node path.
///
/// Implementation: each placeholder body is lexed and parsed as
/// a full GK expression via the same `parse_expression` entry
/// the rest of the language uses, so nesting, function calls,
/// arithmetic, and field access all work uniformly:
///
/// | Input                                            | Result                                     |
/// |--------------------------------------------------|--------------------------------------------|
/// | `"hello"`                                        | `Expr::StringLit("hello")`                 |
/// | `"hello {name}"`                                 | `printf("hello {}", name)`                 |
/// | `"{a}-{b}"`                                      | `printf("{}-{}", a, b)`                    |
/// | `"{format_u64(hash(cycle), 10)}@example.com"`    | `printf("{}@example.com", format_u64(hash(cycle), 10))` |
/// | `"x={a + b}"`                                    | `printf("x={}", a + b)`                    |
/// | `"{x:05}"`                                       | `Expr::StringLit("{x:05}")` (format spec — left to printf) |
/// | `"{{literal}}"`                                  | `Expr::StringLit("{{literal}}")` (escaped braces) |
///
/// The placeholder scan is brace- and string-aware: `}` inside
/// a quoted string or inside nested parentheses doesn't
/// terminate the placeholder. `{{` and `}}` keep printf's escape
/// semantics for emitting literal braces in output. A
/// placeholder body that fails to parse as a complete
/// expression makes the whole literal stay as `StringLit` — the
/// user's intent was likely a printf format spec written by
/// hand, or an unbalanced brace, neither of which we should
/// interpret further.
fn parse_interpolated_string(s: String, span: Span) -> Expr {
    let segments = match scan_interpolation_segments(&s) {
        Some(segs) => segs,
        None => return Expr::StringLit(s, span), // unbalanced — leave alone
    };

    if !segments.iter().any(|seg| matches!(seg, Segment::Placeholder(_))) {
        return Expr::StringLit(s, span);
    }

    // Build the printf format string and gather the placeholder
    // expressions, parsing each via the standard expression
    // parser so nested calls / arithmetic / field access all
    // work uniformly.
    let mut format_str = String::with_capacity(s.len());
    let mut placeholder_exprs: Vec<Expr> = Vec::new();
    for seg in segments {
        match seg {
            Segment::Literal(text) => format_str.push_str(&text),
            Segment::Placeholder(body) => {
                let expr = match parse_placeholder_body(&body, span) {
                    Ok(e) => e,
                    // Unparseable body → bail out, keep the
                    // string literal untouched. The user may
                    // have written a printf format spec or
                    // some other non-GK content.
                    Err(_) => return Expr::StringLit(s, span),
                };
                placeholder_exprs.push(expr);
                format_str.push_str("{}");
            }
        }
    }

    let mut args: Vec<Arg> = Vec::with_capacity(placeholder_exprs.len() + 1);
    args.push(Arg::Positional(Expr::StringLit(format_str, span)));
    for e in placeholder_exprs {
        args.push(Arg::Positional(e));
    }
    Expr::Call(CallExpr { func: "printf".into(), args, span })
}

/// One piece of an interpolated string after segmentation.
enum Segment {
    /// Literal text to copy into the format string. Includes
    /// printf's own `{{` / `}}` escapes verbatim — printf's
    /// `parse_format` pass turns them into single-brace output.
    Literal(String),
    /// A `{ … }` placeholder body, with the surrounding braces
    /// stripped. Will be lexed + parsed as a GK expression.
    Placeholder(String),
}

/// Walk the input, splitting at each `{` that opens a
/// placeholder (i.e. not part of `{{`). Brace and string
/// awareness: nested `(`/`[`/`{` increase depth, the matching
/// closer decreases it, and `}` only terminates a placeholder
/// when at depth zero and not inside a `"…"` string literal.
///
/// Returns `None` if a placeholder is unterminated — the caller
/// treats the whole input as a non-interpolated literal.
fn scan_interpolation_segments(s: &str) -> Option<Vec<Segment>> {
    let chars: Vec<char> = s.chars().collect();
    let mut segments: Vec<Segment> = Vec::new();
    let mut literal = String::new();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        // Escaped braces: keep verbatim in the literal so printf
        // emits a single-brace output.
        if c == '{' && i + 1 < chars.len() && chars[i + 1] == '{' {
            literal.push_str("{{");
            i += 2;
            continue;
        }
        if c == '}' && i + 1 < chars.len() && chars[i + 1] == '}' {
            literal.push_str("}}");
            i += 2;
            continue;
        }
        if c == '{' {
            if !literal.is_empty() {
                segments.push(Segment::Literal(std::mem::take(&mut literal)));
            }
            let body_start = i + 1;
            let body_end = match find_placeholder_end(&chars, body_start) {
                Some(end) => end,
                None => return None,
            };
            let body: String = chars[body_start..body_end].iter().collect();
            segments.push(Segment::Placeholder(body));
            i = body_end + 1; // skip the `}`
            continue;
        }
        literal.push(c);
        i += 1;
    }
    if !literal.is_empty() {
        segments.push(Segment::Literal(literal));
    }
    Some(segments)
}

/// Find the index of the `}` that closes the placeholder
/// starting at `start`. Tracks paren/bracket/brace depth and
/// double-quoted string state so unbalanced sub-expressions
/// inside a placeholder body don't terminate it prematurely.
fn find_placeholder_end(chars: &[char], start: usize) -> Option<usize> {
    let mut depth: i32 = 0;
    let mut in_string = false;
    let mut i = start;
    while i < chars.len() {
        let c = chars[i];
        if in_string {
            if c == '\\' && i + 1 < chars.len() {
                // Skip the escape sequence (eg \" or \\). One
                // char of lookahead is enough — we only need to
                // avoid mistaking the next char for a string
                // terminator.
                i += 2;
                continue;
            }
            if c == '"' {
                in_string = false;
            }
            i += 1;
            continue;
        }
        match c {
            '"' => in_string = true,
            '(' | '[' | '{' => depth += 1,
            ')' | ']' => depth -= 1,
            '}' => {
                if depth == 0 {
                    return Some(i);
                }
                depth -= 1;
            }
            _ => {}
        }
        i += 1;
    }
    None
}

/// Lex and parse a placeholder body as a single GK expression.
fn parse_placeholder_body(body: &str, _span: Span) -> Result<Expr, String> {
    let body = body.trim();
    if body.is_empty() {
        return Err("empty placeholder".into());
    }
    let tokens = crate::dsl::lexer::lex(body)?;
    parse_expression(tokens)
}

/// Parse `name(args...)` — the name has already been consumed.
fn parse_call(p: &mut Parser, func: String, span: Span) -> Result<Expr, String> {
    p.advance(); // consume '('
    let mut args = Vec::new();

    if !matches!(p.peek(), TokenKind::RParen) {
        loop {
            args.push(parse_arg(p)?);
            if matches!(p.peek(), TokenKind::Comma) {
                p.advance();
            } else {
                break;
            }
        }
    }

    p.expect(&TokenKind::RParen)?;
    Ok(Expr::Call(CallExpr { func, args, span }))
}

/// Parse a single argument: either `name: expr` (named) or `expr` (positional).
fn parse_arg(p: &mut Parser) -> Result<Arg, String> {
    // Lookahead: if it's `Ident Colon`, it's a named arg.
    if let TokenKind::Ident(name) = p.peek().clone()
        && p.pos + 1 < p.tokens.len() && matches!(p.tokens[p.pos + 1].kind, TokenKind::Colon) {
            let name = name.clone();
            p.advance(); // consume ident
            p.advance(); // consume ':'
            let value = parse_expr(p)?;
            return Ok(Arg::Named(name, value));
        }
    let expr = parse_expr(p)?;
    Ok(Arg::Positional(expr))
}

/// Parse `[expr, expr, ...]`
fn parse_array_lit(p: &mut Parser) -> Result<Expr, String> {
    let span = p.span();
    p.advance(); // consume '['
    let mut elements = Vec::new();

    if !matches!(p.peek(), TokenKind::RBracket) {
        loop {
            elements.push(parse_expr(p)?);
            if matches!(p.peek(), TokenKind::Comma) {
                p.advance();
            } else {
                break;
            }
        }
    }

    p.expect(&TokenKind::RBracket)?;
    Ok(Expr::ArrayLit(elements, span))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dsl::lexer::lex;

    fn parse_str(s: &str) -> GkFile {
        let tokens = lex(s).unwrap();
        parse(tokens).unwrap()
    }

    #[test]
    fn parse_inputs() {
        let f = parse_str("inputs := (cycle, thread)");
        assert_eq!(f.statements.len(), 1);
        match &f.statements[0] {
            Statement::Inputs(names, _) => {
                assert_eq!(names, &["cycle", "thread"]);
            }
            _ => panic!("expected coordinates"),
        }
    }

    #[test]
    fn parse_inputs_empty() {
        // Zero-input declaration for const expression programs
        let f = parse_str("inputs := ()");
        assert_eq!(f.statements.len(), 1);
        match &f.statements[0] {
            Statement::Inputs(names, _) => {
                assert_eq!(names.len(), 0, "expected empty input list");
            }
            _ => panic!("expected coordinates"),
        }
    }

    #[test]
    fn parse_init_binding() {
        let f = parse_str("init lut = dist_normal(72.0, 5.0)");
        assert_eq!(f.statements.len(), 1);
        match &f.statements[0] {
            Statement::InitBinding(b) => {
                assert_eq!(b.name, "lut");
                match &b.value {
                    Expr::Call(c) => {
                        assert_eq!(c.func, "dist_normal");
                        assert_eq!(c.args.len(), 2);
                    }
                    _ => panic!("expected call"),
                }
            }
            _ => panic!("expected init binding"),
        }
    }

    #[test]
    fn parse_cycle_binding() {
        let f = parse_str("seed := hash(cycle)");
        match &f.statements[0] {
            Statement::CycleBinding(b) => {
                assert_eq!(b.targets, vec!["seed"]);
                match &b.value {
                    Expr::Call(c) => {
                        assert_eq!(c.func, "hash");
                        assert_eq!(c.args.len(), 1);
                    }
                    _ => panic!("expected call"),
                }
            }
            _ => panic!("expected cycle binding"),
        }
    }

    #[test]
    fn parse_destructuring() {
        let f = parse_str("(tenant, device, reading) := mixed_radix(cycle, 100, 1000, 0)");
        match &f.statements[0] {
            Statement::CycleBinding(b) => {
                assert_eq!(b.targets, vec!["tenant", "device", "reading"]);
                match &b.value {
                    Expr::Call(c) => {
                        assert_eq!(c.func, "mixed_radix");
                        assert_eq!(c.args.len(), 4);
                    }
                    _ => panic!("expected call"),
                }
            }
            _ => panic!("expected cycle binding"),
        }
    }

    #[test]
    fn parse_named_args() {
        let f = parse_str("init lut = dist_normal(mean: 72.0, stddev: 5.0)");
        match &f.statements[0] {
            Statement::InitBinding(b) => {
                match &b.value {
                    Expr::Call(c) => {
                        assert!(matches!(&c.args[0], Arg::Named(n, _) if n == "mean"));
                        assert!(matches!(&c.args[1], Arg::Named(n, _) if n == "stddev"));
                    }
                    _ => panic!("expected call"),
                }
            }
            _ => panic!("expected init"),
        }
    }

    #[test]
    fn parse_string_lit_plain() {
        // Bare strings without `{name}` placeholders stay as
        // `Expr::StringLit`.
        let f = parse_str(r#"id := "static text""#);
        match &f.statements[0] {
            Statement::CycleBinding(b) => match &b.value {
                Expr::StringLit(s, _) => assert_eq!(s, "static text"),
                _ => panic!("expected string lit"),
            },
            _ => panic!("expected binding"),
        }
    }

    #[test]
    fn parse_string_lit_interpolated() {
        // Strings containing `{ident}` placeholders compile to a
        // `printf(fmt, idents...)` call so the named idents flow
        // as wires from the surrounding scope.
        let f = parse_str(r#"id := "{code}-{seq}""#);
        match &f.statements[0] {
            Statement::CycleBinding(b) => match &b.value {
                Expr::Call(c) => {
                    assert_eq!(c.func, "printf");
                    assert_eq!(c.args.len(), 3);
                    match &c.args[0] {
                        Arg::Positional(Expr::StringLit(s, _)) => assert_eq!(s, "{}-{}"),
                        _ => panic!("expected format string as first arg"),
                    }
                    match &c.args[1] {
                        Arg::Positional(Expr::Ident(n, _)) => assert_eq!(n, "code"),
                        _ => panic!("expected ident `code`"),
                    }
                    match &c.args[2] {
                        Arg::Positional(Expr::Ident(n, _)) => assert_eq!(n, "seq"),
                        _ => panic!("expected ident `seq`"),
                    }
                }
                other => panic!("expected printf call, got {other:?}"),
            },
            _ => panic!("expected binding"),
        }
    }

    #[test]
    fn parse_string_lit_format_spec_left_alone() {
        // printf format specs (`{:05}`, `{:x}`, `{:.3}`) and
        // empty positional placeholders (`{}`) aren't valid GK
        // expressions, so the literal is preserved untouched
        // for printf's own parser.
        let f = parse_str(r#"id := "x={:05}""#);
        match &f.statements[0] {
            Statement::CycleBinding(b) => match &b.value {
                Expr::StringLit(s, _) => assert_eq!(s, "x={:05}"),
                _ => panic!("expected literal"),
            },
            _ => panic!("expected binding"),
        }
    }

    #[test]
    fn parse_string_lit_nested_call() {
        // SRD 10 example: function calls inside placeholders
        // parse as full expressions and become printf args.
        let f = parse_str(r#"email := "{format_u64(hash(cycle), 10)}@example.com""#);
        let call = match &f.statements[0] {
            Statement::CycleBinding(b) => match &b.value {
                Expr::Call(c) => c,
                other => panic!("expected printf call, got {other:?}"),
            },
            _ => panic!("expected binding"),
        };
        assert_eq!(call.func, "printf");
        assert_eq!(call.args.len(), 2);
        match &call.args[0] {
            Arg::Positional(Expr::StringLit(s, _)) => assert_eq!(s, "{}@example.com"),
            other => panic!("expected format string, got {other:?}"),
        }
        match &call.args[1] {
            Arg::Positional(Expr::Call(inner)) => {
                assert_eq!(inner.func, "format_u64");
                assert_eq!(inner.args.len(), 2);
                match &inner.args[0] {
                    Arg::Positional(Expr::Call(h)) => assert_eq!(h.func, "hash"),
                    other => panic!("expected hash(...) call, got {other:?}"),
                }
                match &inner.args[1] {
                    Arg::Positional(Expr::IntLit(10, _)) => {}
                    other => panic!("expected literal 10, got {other:?}"),
                }
            }
            other => panic!("expected format_u64 call, got {other:?}"),
        }
    }

    #[test]
    fn parse_string_lit_arithmetic_in_placeholder() {
        // Infix arithmetic inside placeholders parses via the
        // standard Pratt expression path.
        let f = parse_str(r#"id := "x={a + b * 2}""#);
        let call = match &f.statements[0] {
            Statement::CycleBinding(b) => match &b.value {
                Expr::Call(c) => c,
                other => panic!("expected call, got {other:?}"),
            },
            _ => panic!("expected binding"),
        };
        assert_eq!(call.func, "printf");
        match &call.args[1] {
            Arg::Positional(Expr::BinOp(_, BinOpKind::Add, _)) => {}
            other => panic!("expected addition, got {other:?}"),
        }
    }

    #[test]
    fn parse_string_lit_field_access() {
        // Field access (`base.ordinal`) inside placeholders.
        let f = parse_str(r#"k := "row {row.id}""#);
        let call = match &f.statements[0] {
            Statement::CycleBinding(b) => match &b.value {
                Expr::Call(c) => c,
                other => panic!("expected call, got {other:?}"),
            },
            _ => panic!("expected binding"),
        };
        assert_eq!(call.func, "printf");
        match &call.args[1] {
            Arg::Positional(Expr::FieldAccess { source, field, .. }) => {
                assert_eq!(source, "row");
                assert_eq!(field, "id");
            }
            other => panic!("expected field access, got {other:?}"),
        }
    }

    #[test]
    fn parse_string_lit_escaped_braces() {
        // Doubled braces (`{{`, `}}`) keep printf's escape
        // semantics — they emit literal `{` / `}` at format time
        // and don't open a placeholder.
        let f = parse_str(r#"k := "{{not a placeholder}} but {real}""#);
        let call = match &f.statements[0] {
            Statement::CycleBinding(b) => match &b.value {
                Expr::Call(c) => c,
                other => panic!("expected call, got {other:?}"),
            },
            _ => panic!("expected binding"),
        };
        match &call.args[0] {
            Arg::Positional(Expr::StringLit(s, _)) => {
                assert_eq!(s, "{{not a placeholder}} but {}");
            }
            other => panic!("expected fmt string, got {other:?}"),
        }
        match &call.args[1] {
            Arg::Positional(Expr::Ident(n, _)) => assert_eq!(n, "real"),
            other => panic!("expected ident `real`, got {other:?}"),
        }
    }

    #[test]
    fn parse_string_lit_unterminated_falls_back() {
        // An unterminated `{` makes the whole string stay literal.
        let f = parse_str(r#"k := "missing close {abc""#);
        match &f.statements[0] {
            Statement::CycleBinding(b) => match &b.value {
                Expr::StringLit(s, _) => assert_eq!(s, "missing close {abc"),
                other => panic!("expected literal, got {other:?}"),
            },
            _ => panic!("expected binding"),
        }
    }

    #[test]
    fn parse_string_lit_parens_in_placeholder() {
        // Function-call parens inside a placeholder don't
        // confuse the brace scanner; the matching `}` is found
        // at depth zero.
        let f = parse_str(r#"k := "{abs(x - y)}""#);
        let call = match &f.statements[0] {
            Statement::CycleBinding(b) => match &b.value {
                Expr::Call(c) => c,
                other => panic!("expected call, got {other:?}"),
            },
            _ => panic!("expected binding"),
        };
        assert_eq!(call.func, "printf");
        match &call.args[1] {
            Arg::Positional(Expr::Call(inner)) => assert_eq!(inner.func, "abs"),
            other => panic!("expected abs call, got {other:?}"),
        }
    }

    #[test]
    fn parse_array_lit() {
        let f = parse_str("init weights = [60.0, 20.0, 15.0, 5.0]");
        match &f.statements[0] {
            Statement::InitBinding(b) => {
                match &b.value {
                    Expr::ArrayLit(elems, _) => assert_eq!(elems.len(), 4),
                    _ => panic!("expected array lit"),
                }
            }
            _ => panic!("expected init"),
        }
    }

    #[test]
    fn parse_nested_call() {
        let f = parse_str("x := hash(interleave(a, b))");
        match &f.statements[0] {
            Statement::CycleBinding(b) => {
                match &b.value {
                    Expr::Call(c) => {
                        assert_eq!(c.func, "hash");
                        assert_eq!(c.args.len(), 1);
                        match &c.args[0] {
                            Arg::Positional(Expr::Call(inner)) => {
                                assert_eq!(inner.func, "interleave");
                                assert_eq!(inner.args.len(), 2);
                            }
                            _ => panic!("expected nested call"),
                        }
                    }
                    _ => panic!("expected call"),
                }
            }
            _ => panic!("expected binding"),
        }
    }

    #[test]
    fn parse_full_program() {
        let src = r#"
            // Init
            init temp_lut = dist_normal(mean: 72.0, stddev: 5.0)
            init weights = [60.0, 20.0, 15.0]

            // Cycle
            inputs := (cycle)
            (tenant, device) := mixed_radix(cycle, 100, 0)
            tenant_h := hash(tenant)
            code := mod(tenant_h, 10000)
            device_id := "{code}-{seq}"
        "#;
        let f = parse_str(src);
        assert_eq!(f.statements.len(), 7);
    }

    #[test]
    fn parse_mixed_positional_named() {
        let f = parse_str("init lut = dist_normal(72.0, 5.0, resolution: 2000)");
        match &f.statements[0] {
            Statement::InitBinding(b) => {
                match &b.value {
                    Expr::Call(c) => {
                        assert!(matches!(&c.args[0], Arg::Positional(_)));
                        assert!(matches!(&c.args[1], Arg::Positional(_)));
                        assert!(matches!(&c.args[2], Arg::Named(n, _) if n == "resolution"));
                    }
                    _ => panic!("expected call"),
                }
            }
            _ => panic!("expected init"),
        }
    }

    #[test]
    fn parse_simple_addition() {
        let f = parse_str("y := a + b");
        match &f.statements[0] {
            Statement::CycleBinding(b) => {
                match &b.value {
                    Expr::BinOp(lhs, BinOpKind::Add, rhs) => {
                        assert!(matches!(**lhs, Expr::Ident(ref s, _) if s == "a"));
                        assert!(matches!(**rhs, Expr::Ident(ref s, _) if s == "b"));
                    }
                    _ => panic!("expected BinOp Add, got {:?}", b.value),
                }
            }
            _ => panic!("expected cycle binding"),
        }
    }

    #[test]
    fn parse_precedence_mul_over_add() {
        // `a + b * c` should parse as `a + (b * c)`
        let f = parse_str("y := a + b * c");
        match &f.statements[0] {
            Statement::CycleBinding(b) => {
                match &b.value {
                    Expr::BinOp(lhs, BinOpKind::Add, rhs) => {
                        assert!(matches!(**lhs, Expr::Ident(ref s, _) if s == "a"));
                        match &**rhs {
                            Expr::BinOp(rl, BinOpKind::Mul, rr) => {
                                assert!(matches!(**rl, Expr::Ident(ref s, _) if s == "b"));
                                assert!(matches!(**rr, Expr::Ident(ref s, _) if s == "c"));
                            }
                            _ => panic!("expected inner Mul"),
                        }
                    }
                    _ => panic!("expected outer Add"),
                }
            }
            _ => panic!("expected cycle binding"),
        }
    }

    #[test]
    fn parse_parenthesized_grouping() {
        // `(a + b) * c` — parens override precedence
        let f = parse_str("y := (a + b) * c");
        match &f.statements[0] {
            Statement::CycleBinding(b) => {
                match &b.value {
                    Expr::BinOp(lhs, BinOpKind::Mul, rhs) => {
                        match &**lhs {
                            Expr::BinOp(_, BinOpKind::Add, _) => {} // correct
                            _ => panic!("expected inner Add in lhs"),
                        }
                        assert!(matches!(**rhs, Expr::Ident(ref s, _) if s == "c"));
                    }
                    _ => panic!("expected outer Mul"),
                }
            }
            _ => panic!("expected cycle binding"),
        }
    }

    #[test]
    fn parse_unary_negation() {
        let f = parse_str("y := -x");
        match &f.statements[0] {
            Statement::CycleBinding(b) => {
                match &b.value {
                    Expr::UnaryNeg(inner, _) => {
                        assert!(matches!(**inner, Expr::Ident(ref s, _) if s == "x"));
                    }
                    _ => panic!("expected UnaryNeg"),
                }
            }
            _ => panic!("expected cycle binding"),
        }
    }

    #[test]
    fn parse_func_call_with_infix_arg() {
        // `sin(cycle * 0.25)` — infix inside function args
        let f = parse_str("y := sin(cycle * 0.25)");
        match &f.statements[0] {
            Statement::CycleBinding(b) => {
                match &b.value {
                    Expr::Call(c) => {
                        assert_eq!(c.func, "sin");
                        assert_eq!(c.args.len(), 1);
                        match &c.args[0] {
                            Arg::Positional(Expr::BinOp(_, BinOpKind::Mul, _)) => {}
                            _ => panic!("expected Mul inside sin() arg"),
                        }
                    }
                    _ => panic!("expected call"),
                }
            }
            _ => panic!("expected cycle binding"),
        }
    }

    #[test]
    fn parse_power_right_associative() {
        // `a ** b ** c` should parse as `a ** (b ** c)` (right-associative)
        let f = parse_str("y := a ** b ** c");
        match &f.statements[0] {
            Statement::CycleBinding(b) => {
                match &b.value {
                    Expr::BinOp(lhs, BinOpKind::Pow, rhs) => {
                        assert!(matches!(**lhs, Expr::Ident(ref s, _) if s == "a"));
                        match &**rhs {
                            Expr::BinOp(rl, BinOpKind::Pow, rr) => {
                                assert!(matches!(**rl, Expr::Ident(ref s, _) if s == "b"));
                                assert!(matches!(**rr, Expr::Ident(ref s, _) if s == "c"));
                            }
                            _ => panic!("expected inner Pow"),
                        }
                    }
                    _ => panic!("expected outer Pow"),
                }
            }
            _ => panic!("expected cycle binding"),
        }
    }

    #[test]
    fn parse_negate_function_call() {
        // `-sin(x)` — unary negation of a function call
        let f = parse_str("y := -sin(x)");
        match &f.statements[0] {
            Statement::CycleBinding(b) => {
                match &b.value {
                    Expr::UnaryNeg(inner, _) => {
                        match &**inner {
                            Expr::Call(c) => assert_eq!(c.func, "sin"),
                            _ => panic!("expected Call inside UnaryNeg"),
                        }
                    }
                    _ => panic!("expected UnaryNeg"),
                }
            }
            _ => panic!("expected cycle binding"),
        }
    }

    #[test]
    fn parse_all_operators() {
        // Ensure all operators parse without error.
        let f = parse_str("y := a + b - c * d / e % f ** g");
        match &f.statements[0] {
            Statement::CycleBinding(_) => {} // just checking it parses
            _ => panic!("expected cycle binding"),
        }
    }

    #[test]
    fn parse_star_star_power() {
        // `x ** 2.0` parses as BinOp(x, Pow, 2.0)
        let f = parse_str("y := x ** 2.0");
        match &f.statements[0] {
            Statement::CycleBinding(b) => {
                match &b.value {
                    Expr::BinOp(lhs, BinOpKind::Pow, rhs) => {
                        assert!(matches!(**lhs, Expr::Ident(ref s, _) if s == "x"));
                        assert!(matches!(**rhs, Expr::FloatLit(v, _) if v == 2.0));
                    }
                    _ => panic!("expected BinOp Pow, got {:?}", b.value),
                }
            }
            _ => panic!("expected cycle binding"),
        }
    }

    #[test]
    fn parse_caret_is_xor() {
        // `a ^ b` parses as BinOp(a, BitXor, b)
        let f = parse_str("y := a ^ b");
        match &f.statements[0] {
            Statement::CycleBinding(b) => {
                match &b.value {
                    Expr::BinOp(lhs, BinOpKind::BitXor, rhs) => {
                        assert!(matches!(**lhs, Expr::Ident(ref s, _) if s == "a"));
                        assert!(matches!(**rhs, Expr::Ident(ref s, _) if s == "b"));
                    }
                    _ => panic!("expected BinOp BitXor, got {:?}", b.value),
                }
            }
            _ => panic!("expected cycle binding"),
        }
    }

    #[test]
    fn parse_bitand_binds_tighter_than_bitor() {
        // `a & b | c` should parse as `(a & b) | c`
        let f = parse_str("y := a & b | c");
        match &f.statements[0] {
            Statement::CycleBinding(b) => {
                match &b.value {
                    Expr::BinOp(lhs, BinOpKind::BitOr, rhs) => {
                        match &**lhs {
                            Expr::BinOp(_, BinOpKind::BitAnd, _) => {} // correct
                            _ => panic!("expected inner BitAnd in lhs"),
                        }
                        assert!(matches!(**rhs, Expr::Ident(ref s, _) if s == "c"));
                    }
                    _ => panic!("expected outer BitOr"),
                }
            }
            _ => panic!("expected cycle binding"),
        }
    }

    #[test]
    fn parse_shift_left() {
        // `a << 4` parses as BinOp(a, Shl, 4)
        let f = parse_str("y := a << 4");
        match &f.statements[0] {
            Statement::CycleBinding(b) => {
                match &b.value {
                    Expr::BinOp(lhs, BinOpKind::Shl, rhs) => {
                        assert!(matches!(**lhs, Expr::Ident(ref s, _) if s == "a"));
                        assert!(matches!(**rhs, Expr::IntLit(4, _)));
                    }
                    _ => panic!("expected BinOp Shl, got {:?}", b.value),
                }
            }
            _ => panic!("expected cycle binding"),
        }
    }

    #[test]
    fn parse_unary_bitnot() {
        // `!x` parses as UnaryBitNot(x)
        let f = parse_str("y := !x");
        match &f.statements[0] {
            Statement::CycleBinding(b) => {
                match &b.value {
                    Expr::UnaryBitNot(inner, _) => {
                        assert!(matches!(**inner, Expr::Ident(ref s, _) if s == "x"));
                    }
                    _ => panic!("expected UnaryBitNot, got {:?}", b.value),
                }
            }
            _ => panic!("expected cycle binding"),
        }
    }
}
