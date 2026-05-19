// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Lexer for the GK DSL.
//!
//! Tokenizes `.gk` source text into a flat token stream. The grammar
//! is line-oriented but the lexer doesn't enforce line structure —
//! that's the parser's job.

/// A source location for error reporting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Span {
    pub line: usize,
    pub col: usize,
}

/// A token with its source location.
#[derive(Debug, Clone, PartialEq)]
pub struct Token {
    pub kind: TokenKind,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TokenKind {
    /// A bare identifier: `cycle`, `hash`, `temp_lut`
    Ident(String),
    /// `const` keyword — declares an effectively-const binding.
    /// Replaces the former `final` / `init` distinction; both
    /// surfaces collapsed into one. The value is materialized at
    /// the earliest opportunity (compile-time fold if possible,
    /// scope-init pull otherwise) and is then immutable for the
    /// scope's lifetime. Independent of provenance: a `const`
    /// binding's RHS may reference workload params, iter-vars
    /// bound by an enclosing comprehension, or other in-scope
    /// names — whatever the GK compiler can resolve.
    Const,
    /// `input` keyword — declares one per-cycle kernel input slot.
    /// Surface: `input <name>[: <type>]` (single) or
    /// `input (<name>[: <type>], ...)` (tuple sugar). Mirrors the
    /// module-signature param-list shape.
    Input,
    /// `extern` keyword
    Extern,
    /// `shared` keyword
    Shared,
    /// `volatile` keyword (SRD-44 + design memo
    /// `resumable_test_fixture.md`). Wire-coloring modifier
    /// excluding the binding's value from `hash_const`.
    Volatile,
    /// `cursor` keyword
    Cursor,
    /// `pragma` keyword (module-level directive opening, SRD 15
    /// §"Module-Level Pragmas"). Followed by an `Ident` naming the
    /// pragma. Distinct from line comments so the parser sees
    /// pragmas as first-class statements rather than scraping them
    /// out of `// @pragma:` text.
    Pragma,
    /// `.` (field access: `base.ordinal`)
    Dot,
    /// Integer literal: `1000`, `0xFF`
    IntLit(u64),
    /// Float literal: `72.0`, `3.14`
    FloatLit(f64),
    /// String literal (contents only, no quotes): `"hello {name}"`
    StringLit(String),
    /// `:=` (binding operator, used by every binding shape:
    /// cycle bindings, `const`, `shared`, `volatile`)
    ColonEq,
    /// `=` (reserved for future use in expression-level
    /// comparisons; not currently emitted by any binding form)
    Eq,
    /// `(`
    LParen,
    /// `)`
    RParen,
    /// `[`
    LBracket,
    /// `]`
    RBracket,
    /// `{`
    LBrace,
    /// `}`
    RBrace,
    /// `,`
    Comma,
    /// `:`
    Colon,
    /// `->`
    Arrow,
    /// `+`
    Plus,
    /// `-` (both binary subtract and unary negate)
    Minus,
    /// `*`
    Star,
    /// `/`
    Slash,
    /// `%`
    Percent,
    /// `^` (bitwise XOR)
    Caret,
    /// `**` (power)
    StarStar,
    /// `<<` (shift left)
    ShiftLeft,
    /// `>>` (shift right)
    ShiftRight,
    /// `&` (bitwise AND)
    Ampersand,
    /// `|` (bitwise OR)
    Pipe,
    /// `!` (unary bitwise NOT)
    Bang,
    /// `<` (less-than comparison)
    Lt,
    /// `>` (greater-than comparison)
    Gt,
    /// `==` (equal-to comparison)
    EqEq,
    /// `!=` (not-equal-to comparison)
    BangEq,
    /// `<=` (less-than-or-equal comparison)
    LtEq,
    /// `>=` (greater-than-or-equal comparison)
    GtEq,
    /// End of input
    Eof,
}

/// Lex source text into tokens.
pub fn lex(source: &str) -> Result<Vec<Token>, String> {
    let mut tokens = Vec::new();
    let chars: Vec<char> = source.chars().collect();
    let mut pos = 0;
    let mut line = 1;
    let mut col = 1;

    while pos < chars.len() {
        let c = chars[pos];

        // Skip whitespace (but track line/col)
        if c == '\n' {
            line += 1;
            col = 1;
            pos += 1;
            continue;
        }
        if c.is_ascii_whitespace() {
            col += 1;
            pos += 1;
            continue;
        }

        // Skip hash comments (# to end of line, YAML-style)
        if c == '#' {
            while pos < chars.len() && chars[pos] != '\n' {
                pos += 1;
            }
            continue;
        }

        // Skip comments (// and /* */)
        if c == '/' && pos + 1 < chars.len() {
            if chars[pos + 1] == '/' {
                // Line comment (// or ///) — skip to end of line
                while pos < chars.len() && chars[pos] != '\n' {
                    pos += 1;
                }
                continue;
            }
            if chars[pos + 1] == '*' {
                // Block comment /* ... */ — skip to closing */
                pos += 2;
                col += 2;
                while pos + 1 < chars.len() {
                    if chars[pos] == '\n' {
                        line += 1;
                        col = 1;
                    }
                    if chars[pos] == '*' && chars[pos + 1] == '/' {
                        pos += 2;
                        col += 2;
                        break;
                    }
                    pos += 1;
                    col += 1;
                }
                continue;
            }
        }

        // Arithmetic operator `/` (not a comment start)
        if c == '/' {
            let span = Span { line, col };
            tokens.push(Token { kind: TokenKind::Slash, span });
            pos += 1;
            col += 1;
            continue;
        }

        let span = Span { line, col };

        // Two-character operators
        if c == ':' && pos + 1 < chars.len() && chars[pos + 1] == '=' {
            tokens.push(Token { kind: TokenKind::ColonEq, span });
            pos += 2;
            col += 2;
            continue;
        }
        if c == '-' && pos + 1 < chars.len() && chars[pos + 1] == '>' {
            tokens.push(Token { kind: TokenKind::Arrow, span });
            pos += 2;
            col += 2;
            continue;
        }

        // Number literal (integer or float).
        // The `-` character is always lexed as `Minus`; negative values
        // are handled by the parser via `UnaryNeg`.
        if c.is_ascii_digit() {
            let start = pos;

            // Check for hex: 0x...
            if pos + 1 < chars.len() && chars[pos] == '0' && (chars[pos + 1] == 'x' || chars[pos + 1] == 'X') {
                pos += 2;
                col += 2;
                let hex_start = pos;
                while pos < chars.len() && chars[pos].is_ascii_hexdigit() {
                    pos += 1;
                    col += 1;
                }
                let hex: String = chars[hex_start..pos].iter().collect();
                let val = u64::from_str_radix(&hex, 16)
                    .map_err(|e| format!("invalid hex literal at line {}, col {}: {e}", span.line, span.col))?;
                tokens.push(Token { kind: TokenKind::IntLit(val), span });
                continue;
            }

            while pos < chars.len() && chars[pos].is_ascii_digit() {
                pos += 1;
                col += 1;
            }

            // Check for float
            let mut is_float = false;
            if pos < chars.len() && chars[pos] == '.' && pos + 1 < chars.len() && chars[pos + 1].is_ascii_digit() {
                is_float = true;
                pos += 1;
                col += 1;
                while pos < chars.len() && chars[pos].is_ascii_digit() {
                    pos += 1;
                    col += 1;
                }
                // Scientific notation
                if pos < chars.len() && (chars[pos] == 'e' || chars[pos] == 'E') {
                    pos += 1;
                    col += 1;
                    if pos < chars.len() && (chars[pos] == '+' || chars[pos] == '-') {
                        pos += 1;
                        col += 1;
                    }
                    while pos < chars.len() && chars[pos].is_ascii_digit() {
                        pos += 1;
                        col += 1;
                    }
                }
            } else if pos < chars.len() && (chars[pos] == 'e' || chars[pos] == 'E') {
                // Scientific notation without a decimal point: 1e10, 2E-3
                is_float = true;
                pos += 1;
                col += 1;
                if pos < chars.len() && (chars[pos] == '+' || chars[pos] == '-') {
                    pos += 1;
                    col += 1;
                }
                while pos < chars.len() && chars[pos].is_ascii_digit() {
                    pos += 1;
                    col += 1;
                }
            }

            // SRD-18c Layer 6 / SRD-18e Push 4: SI suffix
            // literals. Suffix attaches to the just-lexed
            // numeric literal when the next 1-2 chars form
            // a known suffix AND the char after the suffix
            // isn't an identifier-continuation character
            // (so `Kilometers` stays a valid identifier
            // following an unrelated `1`).
            let suffix_consumed = match peek_si_suffix(&chars, pos) {
                Some((multiplier, len, is_subunit)) => {
                    pos += len;
                    col += len;
                    Some((multiplier, is_subunit))
                }
                None => None,
            };

            if is_float {
                let num: String = chars[start..pos - suffix_len_consumed(suffix_consumed)]
                    .iter().collect();
                let mut val: f64 = num.parse()
                    .map_err(|e| format!("invalid float at line {}, col {}: {e}", span.line, span.col))?;
                if let Some((mult, is_sub)) = suffix_consumed {
                    if is_sub {
                        val /= mult as f64;
                    } else {
                        val *= mult as f64;
                    }
                }
                // SI-promoted floats become U64 when the
                // result is exactly integral (`1.5G` →
                // 1_500_000_000 → U64) — matches SRD-18c
                // §"Layer 6" type-resolution rule.
                if suffix_consumed.is_some() && val.fract() == 0.0
                    && val >= 0.0 && val <= u64::MAX as f64 {
                    tokens.push(Token {
                        kind: TokenKind::IntLit(val as u64),
                        span,
                    });
                } else {
                    tokens.push(Token { kind: TokenKind::FloatLit(val), span });
                }
            } else {
                // Skip trailing underscore separators in int literals
                let num_end = pos - suffix_len_consumed(suffix_consumed);
                let num: String = chars[start..num_end].iter()
                    .filter(|c| **c != '_').collect();
                let val: u64 = num.parse()
                    .map_err(|e| format!("invalid integer at line {}, col {}: {e}", span.line, span.col))?;
                match suffix_consumed {
                    Some((mult, true)) => {
                        // Sub-unit suffix on integer →
                        // float (`5m` = 0.005).
                        let f = val as f64 / mult as f64;
                        tokens.push(Token {
                            kind: TokenKind::FloatLit(f),
                            span,
                        });
                    }
                    Some((mult, false)) => {
                        let v = val.checked_mul(mult).ok_or_else(|| format!(
                            "integer literal with SI suffix overflows u64 at line {}, col {}",
                            span.line, span.col))?;
                        tokens.push(Token {
                            kind: TokenKind::IntLit(v),
                            span,
                        });
                    }
                    None => {
                        tokens.push(Token { kind: TokenKind::IntLit(val), span });
                    }
                }
            }
            continue;
        }

        // Single-character tokens
        match c {
            '(' => { tokens.push(Token { kind: TokenKind::LParen, span }); pos += 1; col += 1; continue; }
            ')' => { tokens.push(Token { kind: TokenKind::RParen, span }); pos += 1; col += 1; continue; }
            '[' => { tokens.push(Token { kind: TokenKind::LBracket, span }); pos += 1; col += 1; continue; }
            ']' => { tokens.push(Token { kind: TokenKind::RBracket, span }); pos += 1; col += 1; continue; }
            '{' => { tokens.push(Token { kind: TokenKind::LBrace, span }); pos += 1; col += 1; continue; }
            '}' => { tokens.push(Token { kind: TokenKind::RBrace, span }); pos += 1; col += 1; continue; }
            ',' => { tokens.push(Token { kind: TokenKind::Comma, span }); pos += 1; col += 1; continue; }
            '=' => {
                if pos + 1 < chars.len() && chars[pos + 1] == '=' {
                    tokens.push(Token { kind: TokenKind::EqEq, span });
                    pos += 2; col += 2;
                } else {
                    tokens.push(Token { kind: TokenKind::Eq, span });
                    pos += 1; col += 1;
                }
                continue;
            }
            ':' => { tokens.push(Token { kind: TokenKind::Colon, span }); pos += 1; col += 1; continue; }
            '+' => { tokens.push(Token { kind: TokenKind::Plus, span }); pos += 1; col += 1; continue; }
            '-' => { tokens.push(Token { kind: TokenKind::Minus, span }); pos += 1; col += 1; continue; }
            '*' => {
                if pos + 1 < chars.len() && chars[pos + 1] == '*' {
                    tokens.push(Token { kind: TokenKind::StarStar, span });
                    pos += 2; col += 2;
                } else {
                    tokens.push(Token { kind: TokenKind::Star, span });
                    pos += 1; col += 1;
                }
                continue;
            }
            '%' => { tokens.push(Token { kind: TokenKind::Percent, span }); pos += 1; col += 1; continue; }
            '^' => { tokens.push(Token { kind: TokenKind::Caret, span }); pos += 1; col += 1; continue; }
            '<' => {
                if pos + 1 < chars.len() && chars[pos + 1] == '<' {
                    tokens.push(Token { kind: TokenKind::ShiftLeft, span });
                    pos += 2; col += 2;
                } else if pos + 1 < chars.len() && chars[pos + 1] == '=' {
                    tokens.push(Token { kind: TokenKind::LtEq, span });
                    pos += 2; col += 2;
                } else {
                    tokens.push(Token { kind: TokenKind::Lt, span });
                    pos += 1; col += 1;
                }
                continue;
            }
            '>' => {
                if pos + 1 < chars.len() && chars[pos + 1] == '>' {
                    tokens.push(Token { kind: TokenKind::ShiftRight, span });
                    pos += 2; col += 2;
                } else if pos + 1 < chars.len() && chars[pos + 1] == '=' {
                    tokens.push(Token { kind: TokenKind::GtEq, span });
                    pos += 2; col += 2;
                } else {
                    tokens.push(Token { kind: TokenKind::Gt, span });
                    pos += 1; col += 1;
                }
                continue;
            }
            '.' => { tokens.push(Token { kind: TokenKind::Dot, span }); pos += 1; col += 1; continue; }
            '&' => { tokens.push(Token { kind: TokenKind::Ampersand, span }); pos += 1; col += 1; continue; }
            '|' => { tokens.push(Token { kind: TokenKind::Pipe, span }); pos += 1; col += 1; continue; }
            '!' => {
                if pos + 1 < chars.len() && chars[pos + 1] == '=' {
                    tokens.push(Token { kind: TokenKind::BangEq, span });
                    pos += 2; col += 2;
                } else {
                    tokens.push(Token { kind: TokenKind::Bang, span });
                    pos += 1; col += 1;
                }
                continue;
            }
            _ => {}
        }

        // String literal (double-quoted or single-quoted)
        if c == '"' || c == '\'' {
            let quote = c;
            pos += 1;
            col += 1;
            let mut s = String::new();
            while pos < chars.len() && chars[pos] != quote {
                if chars[pos] == '\\' && pos + 1 < chars.len() {
                    pos += 1;
                    col += 1;
                    match chars[pos] {
                        'n' => s.push('\n'),
                        't' => s.push('\t'),
                        '\\' => s.push('\\'),
                        c if c == quote => s.push(c),
                        other => { s.push('\\'); s.push(other); }
                    }
                } else {
                    s.push(chars[pos]);
                }
                pos += 1;
                col += 1;
            }
            if pos < chars.len() {
                pos += 1; // skip closing quote
                col += 1;
            } else {
                return Err(format!("unterminated string at line {}, col {}", span.line, span.col));
            }
            tokens.push(Token { kind: TokenKind::StringLit(s), span });
            continue;
        }

        // Identifier or keyword
        if c.is_ascii_alphabetic() || c == '_' {
            let start = pos;
            while pos < chars.len() && (chars[pos].is_ascii_alphanumeric() || chars[pos] == '_') {
                pos += 1;
                col += 1;
            }
            let word: String = chars[start..pos].iter().collect();
            let kind = match word.as_str() {
                "const" => TokenKind::Const,
                "input" => TokenKind::Input,
                "extern" => TokenKind::Extern,
                "shared" => TokenKind::Shared,
                "volatile" => TokenKind::Volatile,
                "cursor" => TokenKind::Cursor,
                "pragma" => TokenKind::Pragma,
                _ => TokenKind::Ident(word),
            };
            tokens.push(Token { kind, span });
            continue;
        }

        return Err(format!("unexpected character '{}' at line {}, col {}", c, line, col));
    }

    tokens.push(Token { kind: TokenKind::Eof, span: Span { line, col } });
    Ok(tokens)
}

/// SRD-18c Layer 6 / SRD-18e Push 4: peek for an SI suffix
/// at `pos`. Returns `Some((multiplier, len, is_subunit))`
/// on a match, `None` otherwise.
///
/// Two-char binary suffixes (`Ki`, `Mi`, `Gi`, `Ti`, `Pi`)
/// are checked first to avoid `K` greedy-eating the `K` of
/// `Ki`. Decimal suffixes (`K`, `M`, `G`, `T`, `P`) and sub-
/// unit suffixes (`m`, `u`, `n`) are length-1.
///
/// A match requires the char *after* the suffix to NOT be
/// an identifier-continuation character — so `1Kilometers`
/// stays as IntLit(1) + Ident("Kilometers"), not `1000000`.
fn peek_si_suffix(chars: &[char], pos: usize) -> Option<(u64, usize, bool)> {
    if pos >= chars.len() {
        return None;
    }
    // Try 2-char binary suffixes first.
    if pos + 1 < chars.len() && chars[pos + 1] == 'i' {
        let mult = match chars[pos] {
            'K' => 1u64 << 10,
            'M' => 1u64 << 20,
            'G' => 1u64 << 30,
            'T' => 1u64 << 40,
            'P' => 1u64 << 50,
            _ => 0,
        };
        if mult > 0 {
            // Check that the suffix isn't part of an identifier.
            let next = chars.get(pos + 2);
            if !next.is_some_and(|c| c.is_ascii_alphanumeric() || *c == '_') {
                return Some((mult, 2, false));
            }
        }
    }
    // 1-char decimal suffixes.
    let (mult, is_subunit) = match chars[pos] {
        'K' => (1_000u64, false),
        'M' => (1_000_000u64, false),
        'G' => (1_000_000_000u64, false),
        'T' => (1_000_000_000_000u64, false),
        'P' => (1_000_000_000_000_000u64, false),
        'm' => (1_000u64, true),         // 10⁻³
        'u' => (1_000_000u64, true),     // 10⁻⁶
        'n' => (1_000_000_000u64, true), // 10⁻⁹
        _ => return None,
    };
    let next = chars.get(pos + 1);
    if !next.is_some_and(|c| c.is_ascii_alphanumeric() || *c == '_') {
        Some((mult, 1, is_subunit))
    } else {
        None
    }
}

/// Length consumed for the SI suffix, or 0 when no suffix
/// was applied. Used by the numeric-literal branch to
/// recover the digit-only end-position when slicing the
/// literal text out for parsing.
fn suffix_len_consumed(suffix: Option<(u64, bool)>) -> usize {
    match suffix {
        // We applied either a 1-char or 2-char suffix; the
        // caller only stored the multiplier and is_subunit
        // pair, so we re-derive: binary multipliers (powers
        // of 2 from 2^10 up) take 2 chars, others take 1.
        Some((m, _)) => {
            if m.is_power_of_two() && m >= (1 << 10) { 2 } else { 1 }
        }
        None => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lex_cycle_binding() {
        let tokens = lex("seed := hash(cycle)").unwrap();
        assert!(matches!(tokens[0].kind, TokenKind::Ident(ref s) if s == "seed"));
        assert!(matches!(tokens[1].kind, TokenKind::ColonEq));
        assert!(matches!(tokens[2].kind, TokenKind::Ident(ref s) if s == "hash"));
        assert!(matches!(tokens[3].kind, TokenKind::LParen));
        assert!(matches!(tokens[4].kind, TokenKind::Ident(ref s) if s == "cycle"));
        assert!(matches!(tokens[5].kind, TokenKind::RParen));
    }

    #[test]
    fn lex_const_binding() {
        let tokens = lex("const lut := dist_normal(72.0, 5.0)").unwrap();
        assert!(matches!(tokens[0].kind, TokenKind::Const));
        assert!(matches!(tokens[1].kind, TokenKind::Ident(ref s) if s == "lut"));
        assert!(matches!(tokens[2].kind, TokenKind::ColonEq));
        assert!(matches!(tokens[3].kind, TokenKind::Ident(ref s) if s == "dist_normal"));
        assert!(matches!(tokens[5].kind, TokenKind::FloatLit(v) if v == 72.0));
        assert!(matches!(tokens[7].kind, TokenKind::FloatLit(v) if v == 5.0));
    }

    #[test]
    fn lex_input_keyword_tuple() {
        let tokens = lex("input (cycle: u64, thread: u64)").unwrap();
        assert!(matches!(tokens[0].kind, TokenKind::Input));
        assert!(matches!(tokens[1].kind, TokenKind::LParen));
        assert!(matches!(tokens[2].kind, TokenKind::Ident(ref s) if s == "cycle"));
    }

    #[test]
    fn lex_destructuring() {
        let tokens = lex("(a, b, c) := mixed_radix(cycle, 100, 1000, 0)").unwrap();
        assert!(matches!(tokens[0].kind, TokenKind::LParen));
        assert!(matches!(tokens[1].kind, TokenKind::Ident(ref s) if s == "a"));
    }

    #[test]
    fn lex_string_with_interpolation() {
        let tokens = lex(r#"id := "{code}-{seq}""#).unwrap();
        // id(0) :=(1) string(2)
        assert!(matches!(tokens[2].kind, TokenKind::StringLit(ref s) if s == "{code}-{seq}"));
    }

    #[test]
    fn lex_named_args() {
        let tokens = lex("dist_normal(mean: 72.0, stddev: 5.0)").unwrap();
        assert!(matches!(tokens[2].kind, TokenKind::Ident(ref s) if s == "mean"));
        assert!(matches!(tokens[3].kind, TokenKind::Colon));
        assert!(matches!(tokens[4].kind, TokenKind::FloatLit(v) if v == 72.0));
    }

    #[test]
    fn lex_array_literal() {
        let tokens = lex("[60.0, 20.0, 15.0, 5.0]").unwrap();
        assert!(matches!(tokens[0].kind, TokenKind::LBracket));
        assert!(matches!(tokens[1].kind, TokenKind::FloatLit(v) if v == 60.0));
        assert!(matches!(tokens[8].kind, TokenKind::RBracket));
    }

    #[test]
    fn lex_comments_stripped() {
        let tokens = lex("// this is a comment\nseed := hash(cycle)").unwrap();
        assert!(matches!(tokens[0].kind, TokenKind::Ident(ref s) if s == "seed"));
    }

    #[test]
    fn lex_arrow() {
        let tokens = lex("(x: u64) -> (y: u64)").unwrap();
        // ( x : u64 ) -> ...
        // 0 1 2  3  4  5
        assert!(matches!(tokens[5].kind, TokenKind::Arrow));
    }

    #[test]
    fn lex_large_int() {
        let tokens = lex("1710000000000").unwrap();
        assert!(matches!(tokens[0].kind, TokenKind::IntLit(1710000000000)));
    }

    #[test]
    fn lex_hex_int() {
        let tokens = lex("0xFF").unwrap();
        assert!(matches!(tokens[0].kind, TokenKind::IntLit(255)));
    }

    #[test]
    fn lex_block_comment() {
        let tokens = lex("a := /* skip this */ hash(b)").unwrap();
        assert!(matches!(tokens[0].kind, TokenKind::Ident(ref s) if s == "a"));
        assert!(matches!(tokens[1].kind, TokenKind::ColonEq));
        assert!(matches!(tokens[2].kind, TokenKind::Ident(ref s) if s == "hash"));
    }

    #[test]
    fn lex_block_comment_multiline() {
        let tokens = lex("a := 42\n/* this\nis\na\nblock */\nb := 7").unwrap();
        assert!(matches!(tokens[0].kind, TokenKind::Ident(ref s) if s == "a"));
        assert!(matches!(tokens[2].kind, TokenKind::IntLit(42)));
        assert!(matches!(tokens[3].kind, TokenKind::Ident(ref s) if s == "b"));
    }

    #[test]
    fn lex_doc_comment() {
        // Triple-slash doc comments are stripped like line comments
        let tokens = lex("/// doc comment\nseed := hash(cycle)").unwrap();
        assert!(matches!(tokens[0].kind, TokenKind::Ident(ref s) if s == "seed"));
    }

    #[test]
    fn lex_input_keyword_bare() {
        let tokens = lex("input cycle: u64").unwrap();
        assert!(matches!(tokens[0].kind, TokenKind::Input));
        assert!(matches!(tokens[1].kind, TokenKind::Ident(ref s) if s == "cycle"));
        assert!(matches!(tokens[2].kind, TokenKind::Colon));
        assert!(matches!(tokens[3].kind, TokenKind::Ident(ref s) if s == "u64"));
    }

    #[test]
    fn lex_arithmetic_operators() {
        let tokens = lex("a + b * 2.0 - c / d % e ^ f").unwrap();
        assert!(matches!(tokens[0].kind, TokenKind::Ident(ref s) if s == "a"));
        assert!(matches!(tokens[1].kind, TokenKind::Plus));
        assert!(matches!(tokens[2].kind, TokenKind::Ident(ref s) if s == "b"));
        assert!(matches!(tokens[3].kind, TokenKind::Star));
        assert!(matches!(tokens[4].kind, TokenKind::FloatLit(v) if v == 2.0));
        assert!(matches!(tokens[5].kind, TokenKind::Minus));
        assert!(matches!(tokens[6].kind, TokenKind::Ident(ref s) if s == "c"));
        assert!(matches!(tokens[7].kind, TokenKind::Slash));
        assert!(matches!(tokens[8].kind, TokenKind::Ident(ref s) if s == "d"));
        assert!(matches!(tokens[9].kind, TokenKind::Percent));
        assert!(matches!(tokens[10].kind, TokenKind::Ident(ref s) if s == "e"));
        assert!(matches!(tokens[11].kind, TokenKind::Caret));
        assert!(matches!(tokens[12].kind, TokenKind::Ident(ref s) if s == "f"));
    }

    #[test]
    fn lex_minus_binary_vs_negative_literal() {
        // After an identifier, `-` is a binary Minus, not a negative literal.
        let tokens = lex("x - 3").unwrap();
        assert!(matches!(tokens[0].kind, TokenKind::Ident(ref s) if s == "x"));
        assert!(matches!(tokens[1].kind, TokenKind::Minus));
        assert!(matches!(tokens[2].kind, TokenKind::IntLit(3)));
    }

    #[test]
    fn lex_negative_via_minus_token() {
        // `-3.0` is lexed as Minus + FloatLit(3.0); the parser
        // handles negation via UnaryNeg.
        let tokens = lex("-3.0").unwrap();
        assert!(matches!(tokens[0].kind, TokenKind::Minus));
        assert!(matches!(tokens[1].kind, TokenKind::FloatLit(v) if v == 3.0));

        // `-3` is Minus + IntLit(3).
        let tokens = lex("-3").unwrap();
        assert!(matches!(tokens[0].kind, TokenKind::Minus));
        assert!(matches!(tokens[1].kind, TokenKind::IntLit(3)));
    }

    #[test]
    fn lex_scientific_notation() {
        let tokens = lex("1e10").unwrap();
        assert!(matches!(&tokens[0].kind, TokenKind::FloatLit(v) if (*v - 1e10).abs() < 1e5));
    }

    #[test]
    fn lex_scientific_notation_negative_exponent() {
        let tokens = lex("1e-10").unwrap();
        assert!(matches!(&tokens[0].kind, TokenKind::FloatLit(v) if *v > 0.0 && *v < 1e-5));
    }

    #[test]
    fn lex_scientific_notation_with_decimal() {
        let tokens = lex("2.5e3").unwrap();
        assert!(matches!(&tokens[0].kind, TokenKind::FloatLit(v) if (*v - 2500.0).abs() < 0.1));
    }

    #[test]
    fn lex_scientific_notation_positive_exponent() {
        let tokens = lex("3E+5").unwrap();
        assert!(matches!(&tokens[0].kind, TokenKind::FloatLit(v) if (*v - 3e5).abs() < 1.0));
    }

    #[test]
    fn lex_sci_uppercase_e() {
        let t = lex("2.5E3").unwrap();
        assert!(matches!(&t[0].kind, TokenKind::FloatLit(v) if (*v - 2500.0).abs() < 0.1));
    }

    #[test]
    fn lex_sci_explicit_positive_exp() {
        let t = lex("1e+10").unwrap();
        assert!(matches!(&t[0].kind, TokenKind::FloatLit(v) if (*v - 1e10).abs() < 1e5));
    }

    #[test]
    fn lex_sci_decimal_negative_exp() {
        let t = lex("3.14e-2").unwrap();
        assert!(matches!(&t[0].kind, TokenKind::FloatLit(v) if (*v - 0.0314).abs() < 0.001));
    }

    #[test]
    fn lex_sci_zero_exponent() {
        let t = lex("0.5e0").unwrap();
        assert!(matches!(&t[0].kind, TokenKind::FloatLit(v) if (*v - 0.5).abs() < 0.001));
    }

    #[test]
    fn lex_sci_very_small() {
        let t = lex("1e-300").unwrap();
        assert!(matches!(&t[0].kind, TokenKind::FloatLit(v) if *v > 0.0 && *v < 1e-299));
    }

    #[test]
    fn lex_sci_uppercase_no_decimal() {
        let t = lex("1E10").unwrap();
        assert!(matches!(&t[0].kind, TokenKind::FloatLit(v) if (*v - 1e10).abs() < 1e5));
    }

    #[test]
    fn lex_slash_vs_comment() {
        // Single `/` is Slash, `//` is a comment.
        let tokens = lex("a / b // comment").unwrap();
        assert!(matches!(tokens[0].kind, TokenKind::Ident(ref s) if s == "a"));
        assert!(matches!(tokens[1].kind, TokenKind::Slash));
        assert!(matches!(tokens[2].kind, TokenKind::Ident(ref s) if s == "b"));
        assert!(matches!(tokens[3].kind, TokenKind::Eof));
    }

    // ── SRD-18c Layer 6 / SRD-18e Push 4: SI suffixes ──

    #[test]
    fn lex_si_decimal_k_m_g_t_p() {
        let tokens = lex("1K 1M 1G 1T 1P").unwrap();
        assert!(matches!(tokens[0].kind, TokenKind::IntLit(1_000)));
        assert!(matches!(tokens[1].kind, TokenKind::IntLit(1_000_000)));
        assert!(matches!(tokens[2].kind, TokenKind::IntLit(1_000_000_000)));
        assert!(matches!(tokens[3].kind, TokenKind::IntLit(1_000_000_000_000)));
        assert!(matches!(tokens[4].kind, TokenKind::IntLit(1_000_000_000_000_000)));
    }

    #[test]
    fn lex_si_binary_ki_mi_gi_ti_pi() {
        let tokens = lex("1Ki 1Mi 1Gi 1Ti 1Pi").unwrap();
        assert!(matches!(tokens[0].kind, TokenKind::IntLit(1024)));
        assert!(matches!(tokens[1].kind, TokenKind::IntLit(1_048_576)));
        assert!(matches!(tokens[2].kind, TokenKind::IntLit(1_073_741_824)));
        assert!(matches!(tokens[3].kind, TokenKind::IntLit(1_099_511_627_776)));
        assert!(matches!(tokens[4].kind, TokenKind::IntLit(1_125_899_906_842_624)));
    }

    #[test]
    fn lex_si_subunit_m_u_n() {
        // Sub-unit suffixes promote integer to float (5m = 0.005).
        let tokens = lex("5m 5u 5n").unwrap();
        assert!(matches!(tokens[0].kind, TokenKind::FloatLit(v) if (v - 0.005).abs() < 1e-12));
        assert!(matches!(tokens[1].kind, TokenKind::FloatLit(v) if (v - 0.000_005).abs() < 1e-15));
        assert!(matches!(tokens[2].kind, TokenKind::FloatLit(v) if (v - 0.000_000_005).abs() < 1e-18));
    }

    #[test]
    fn lex_si_float_base_with_decimal_suffix() {
        // 1.5K = 1500, integral → IntLit per SRD-18c §"Layer 6"
        let tokens = lex("1.5K").unwrap();
        assert!(matches!(tokens[0].kind, TokenKind::IntLit(1_500)),
            "1.5K should be IntLit(1500), got {:?}", tokens[0].kind);
    }

    #[test]
    fn lex_si_float_base_non_integral_stays_float() {
        // 1.5G = 1_500_000_000.0 — integral → IntLit
        // 1.5K = 1500 — integral → IntLit
        // 1.25K = 1250 — integral → IntLit
        // To force float: an irrational-result combination.
        // 0.001K = 1.0 → still integral → IntLit. Hmm. Let's
        // try a sub-unit float: 1.5m = 0.0015, non-integral.
        let tokens = lex("1.5m").unwrap();
        assert!(matches!(tokens[0].kind, TokenKind::FloatLit(v) if (v - 0.0015).abs() < 1e-12));
    }

    #[test]
    fn lex_si_kilometers_stays_identifier() {
        // The K of `Kilometers` is followed by an
        // identifier-cont char; suffix doesn't apply.
        let tokens = lex("1 Kilometers").unwrap();
        assert!(matches!(tokens[0].kind, TokenKind::IntLit(1)));
        assert!(matches!(tokens[1].kind, TokenKind::Ident(ref s) if s == "Kilometers"));
    }

    #[test]
    fn lex_si_overflow_errors_loud() {
        // 1P × 1000 doesn't overflow but multiplying by P
        // a value already at u64::MAX/1000 + 1 would. Use
        // a value that 1P would overflow: 100000P = 10^20 > u64::MAX.
        let err = lex("100000P").unwrap_err();
        assert!(err.contains("overflows u64"), "{err}");
    }

    #[test]
    fn lex_si_in_range_expression() {
        // SI literals work in range positions (SRD-18c
        // example `1K..1M..100K`). The lexer doesn't know
        // about ranges yet, but should produce the right
        // numeric tokens.
        let tokens = lex("1K..1M..100K").unwrap();
        assert!(matches!(tokens[0].kind, TokenKind::IntLit(1_000)));
        // Token 1 is `..` (still parsed as Dot Dot in
        // pre-Push-3 lexer, but the numeric values are right).
        // We just verify the IntLits at positions 0, 3, 6.
        // (Positions depend on how `..` is tokenized today;
        // we walk the IntLits.)
        let int_lits: Vec<u64> = tokens.iter().filter_map(|t| match &t.kind {
            TokenKind::IntLit(v) => Some(*v),
            _ => None,
        }).collect();
        assert_eq!(int_lits, vec![1_000, 1_000_000, 100_000]);
    }

    #[test]
    fn lex_si_disambiguation_two_char_first() {
        // `1Ki` should match the 2-char binary suffix, NOT
        // 1-char `K` followed by `i` identifier.
        let tokens = lex("1Ki").unwrap();
        assert!(matches!(tokens[0].kind, TokenKind::IntLit(1024)));
        // No leftover identifier after the suffix.
        assert!(matches!(tokens[1].kind, TokenKind::Eof));
    }

    #[test]
    fn lex_si_followed_by_operator_applies_suffix() {
        // `1K+5` — `K` followed by `+` (not ident-cont) → suffix applies.
        let tokens = lex("1K+5").unwrap();
        assert!(matches!(tokens[0].kind, TokenKind::IntLit(1000)));
        assert!(matches!(tokens[1].kind, TokenKind::Plus));
        assert!(matches!(tokens[2].kind, TokenKind::IntLit(5)));
    }
}
