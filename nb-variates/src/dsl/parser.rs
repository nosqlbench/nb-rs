// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! Recursive descent parser for the GK DSL.
//!
//! Parses a token stream (from the lexer) into an AST.

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
        TokenKind::Coordinates => parse_coordinates(p),
        TokenKind::Init => parse_init_binding(p),
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

/// `coordinates := (name1, name2, ...)`
fn parse_coordinates(p: &mut Parser) -> Result<Statement, String> {
    let span = p.span();
    p.advance(); // consume 'coordinates'
    p.expect(&TokenKind::ColonEq)?;
    p.expect(&TokenKind::LParen)?;

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
    let span = p.span();
    p.advance(); // consume 'init'
    let name = p.expect_ident()?;
    p.expect(&TokenKind::Eq)?;
    let value = parse_expr(p)?;

    Ok(Statement::InitBinding(InitBinding { name, value, span }))
}

/// `name := expr`
fn parse_cycle_binding(p: &mut Parser) -> Result<Statement, String> {
    let span = p.span();
    let name = p.expect_ident()?;
    p.expect(&TokenKind::ColonEq)?;
    let value = parse_expr(p)?;

    Ok(Statement::CycleBinding(CycleBinding {
        targets: vec![name],
        value,
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
        span,
    }))
}

/// Parse an expression.
fn parse_expr(p: &mut Parser) -> Result<Expr, String> {
    let span = p.span();

    match p.peek().clone() {
        TokenKind::StringLit(s) => {
            p.advance();
            Ok(Expr::StringLit(s, span))
        }
        TokenKind::IntLit(v) => {
            p.advance();
            // Check if this is followed by `.` making it a float
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
            // Is this a function call?
            if matches!(p.peek(), TokenKind::LParen) {
                parse_call(p, name, span)
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
    if let TokenKind::Ident(name) = p.peek().clone() {
        if p.pos + 1 < p.tokens.len() && matches!(p.tokens[p.pos + 1].kind, TokenKind::Colon) {
            let name = name.clone();
            p.advance(); // consume ident
            p.advance(); // consume ':'
            let value = parse_expr(p)?;
            return Ok(Arg::Named(name, value));
        }
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
    fn parse_coordinates() {
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
}
