// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Recursive descent parser for the GK DSL.
//!
//! Parses a token stream (from the lexer) into an AST.
//! Infix arithmetic expressions (`+`, `-`, `*`, `/`, `%`, `^`) are
//! handled by a Pratt (precedence-climbing) parser that produces
//! `Expr::BinOp` nodes, later desugared by the compiler into
//! function calls.

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

/// `coordinates := (name1, name2, ...)` or `inputs := ()` (zero inputs)
fn parse_inputs(p: &mut Parser) -> Result<Statement, String> {
    let span = p.span();
    p.advance(); // consume 'coordinates' / 'inputs'
    p.expect(&TokenKind::ColonEq)?;
    p.expect(&TokenKind::LParen)?;

    // Allow empty input list: `inputs := ()`
    if matches!(p.peek(), TokenKind::RParen) {
        p.advance(); // consume ')'
        return Ok(Statement::Coordinates(vec![], span));
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

    Ok(Statement::Coordinates(names, span))
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
            Ok(Expr::StringLit(s, span))
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
        let f = parse_str("coordinates := (cycle, thread)");
        assert_eq!(f.statements.len(), 1);
        match &f.statements[0] {
            Statement::Coordinates(names, _) => {
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
            Statement::Coordinates(names, _) => {
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
    fn parse_string_lit() {
        let f = parse_str(r#"id := "{code}-{seq}""#);
        match &f.statements[0] {
            Statement::CycleBinding(b) => {
                match &b.value {
                    Expr::StringLit(s, _) => assert_eq!(s, "{code}-{seq}"),
                    _ => panic!("expected string lit"),
                }
            }
            _ => panic!("expected binding"),
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
            coordinates := (cycle)
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
