// Copyright 2024-2026 nosqlbench contributors
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
    /// `init` keyword
    Init,
    /// `coordinates` keyword
    Inputs,
    /// `extern` keyword
    Extern,
    /// `volatile` keyword
    Volatile,
    /// `sticky` keyword
    Sticky,
    /// Integer literal: `1000`, `0xFF`
    IntLit(u64),
    /// Float literal: `72.0`, `3.14`
    FloatLit(f64),
    /// String literal (contents only, no quotes): `"hello {name}"`
    StringLit(String),
    /// `:=` (cycle-time binding operator)
    ColonEq,
    /// `=` (init-time binding operator, used after `init`)
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

        // Skip comments
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

        // Single-character tokens
        match c {
            '(' => { tokens.push(Token { kind: TokenKind::LParen, span }); pos += 1; col += 1; continue; }
            ')' => { tokens.push(Token { kind: TokenKind::RParen, span }); pos += 1; col += 1; continue; }
            '[' => { tokens.push(Token { kind: TokenKind::LBracket, span }); pos += 1; col += 1; continue; }
            ']' => { tokens.push(Token { kind: TokenKind::RBracket, span }); pos += 1; col += 1; continue; }
            '{' => { tokens.push(Token { kind: TokenKind::LBrace, span }); pos += 1; col += 1; continue; }
            '}' => { tokens.push(Token { kind: TokenKind::RBrace, span }); pos += 1; col += 1; continue; }
            ',' => { tokens.push(Token { kind: TokenKind::Comma, span }); pos += 1; col += 1; continue; }
            '=' => { tokens.push(Token { kind: TokenKind::Eq, span }); pos += 1; col += 1; continue; }
            ':' => { tokens.push(Token { kind: TokenKind::Colon, span }); pos += 1; col += 1; continue; }
            _ => {}
        }

        // String literal
        if c == '"' {
            pos += 1;
            col += 1;
            let mut s = String::new();
            while pos < chars.len() && chars[pos] != '"' {
                if chars[pos] == '\\' && pos + 1 < chars.len() {
                    pos += 1;
                    col += 1;
                    match chars[pos] {
                        'n' => s.push('\n'),
                        't' => s.push('\t'),
                        '\\' => s.push('\\'),
                        '"' => s.push('"'),
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

        // Number literal (integer or float)
        if c.is_ascii_digit() || (c == '-' && pos + 1 < chars.len() && chars[pos + 1].is_ascii_digit()) {
            let start = pos;
            if c == '-' { pos += 1; col += 1; }

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
            if pos < chars.len() && chars[pos] == '.' && pos + 1 < chars.len() && chars[pos + 1].is_ascii_digit() {
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
                let num: String = chars[start..pos].iter().collect();
                let val: f64 = num.parse()
                    .map_err(|e| format!("invalid float at line {}, col {}: {e}", span.line, span.col))?;
                tokens.push(Token { kind: TokenKind::FloatLit(val), span });
            } else {
                // Skip trailing underscore separators in int literals
                let num: String = chars[start..pos].iter().filter(|c| **c != '_').collect();
                let val: u64 = num.parse()
                    .map_err(|e| format!("invalid integer at line {}, col {}: {e}", span.line, span.col))?;
                tokens.push(Token { kind: TokenKind::IntLit(val), span });
            }
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
                "init" => TokenKind::Init,
                "coordinates" | "inputs" => TokenKind::Inputs,
                "extern" => TokenKind::Extern,
                "volatile" => TokenKind::Volatile,
                "sticky" => TokenKind::Sticky,
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
    fn lex_init_binding() {
        let tokens = lex("init lut = dist_normal(72.0, 5.0)").unwrap();
        assert!(matches!(tokens[0].kind, TokenKind::Init));
        assert!(matches!(tokens[1].kind, TokenKind::Ident(ref s) if s == "lut"));
        assert!(matches!(tokens[2].kind, TokenKind::Eq));
        assert!(matches!(tokens[3].kind, TokenKind::Ident(ref s) if s == "dist_normal"));
        assert!(matches!(tokens[5].kind, TokenKind::FloatLit(v) if v == 72.0));
        assert!(matches!(tokens[7].kind, TokenKind::FloatLit(v) if v == 5.0));
    }

    #[test]
    fn lex_coordinates() {
        let tokens = lex("coordinates := (cycle, thread)").unwrap();
        assert!(matches!(tokens[0].kind, TokenKind::Inputs));
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
    fn lex_inputs_keyword() {
        let tokens = lex("inputs := (cycle)").unwrap();
        assert!(matches!(tokens[0].kind, TokenKind::Inputs));
    }
}
