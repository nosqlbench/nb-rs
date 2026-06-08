// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Source-string grammar parser — turns the user-facing
//! source expression (e.g. `"1..10"`, `"[a, b, c]"`,
//! `"fib(8)"`) into a typed [`Source`] value.
//!
//! Polydat owns the source-string grammar per the audit
//! resolution + this design pass: SRD-18c covers the parser-
//! layer surface conceptually, but the actual parsing lives
//! here so all polydat consumers share one canonical
//! source-grammar implementation.
//!
//! Recognized forms:
//!
//! | Source text | Produces |
//! |---|---|
//! | `1..10` | `IntRange { lo: 1, hi: 10, step: 1 }` |
//! | `1..=10` | `IntRange { lo: 1, hi: 11, step: 1 }` (inclusive end) |
//! | `1..10 step 2` | `IntRange { lo: 1, hi: 10, step: 2 }` |
//! | `[a, b, c]` | `Literal { values: [Str, Str, Str] }` |
//! | `[1, 2, 3]` | `Literal { values: [Int, Int, Int] }` |
//! | `[1.0, 2.5]` | `Literal { values: [Float, Float] }` |
//! | `[true, false]` | `Literal { values: [Bool, Bool] }` |
//! | `{name}` | `WorkloadParamList { name: "name", len_hint: None }` |
//! | `fib(8)` (or any `ident(...)`) | `Generator { expr, cardinality_hint: None }` |
//! | `0.0..1.0` | `ContinuousInterval { interval, measure: Uniform }` |
//!
//! Unknown shapes return `SourceParseError::Unrecognized`. The
//! consumer treats this as a parse error to surface to the
//! workload author.

use crate::iteration::comprehension::cardinality::{Interval, ProductMeasure};
use crate::iteration::comprehension::source::{LiteralValue, Source};

/// Parse a source-expression string into a typed [`Source`].
pub fn parse_source(text: &str) -> Result<Source, SourceParseError> {
    let trimmed = text.trim();

    // Workload-param reference: `{name}` — accepts both the
    // simple form (`{foo}`) and the dynamic form
    // (`{a_{b}_c}`). Dynamic placeholders surface as the
    // outer name with `_` separators; the runtime interpolator
    // resolves the nesting before lookup.
    if let Some(name) = strip_curly(trimmed) {
        return Ok(Source::WorkloadParamList {
            name,
            len_hint: None,
        });
    }
    if let Some(dyn_text) = strip_dynamic_curly(trimmed) {
        return Ok(Source::WorkloadParamList {
            name: dyn_text,
            len_hint: None,
        });
    }

    // SRD-18f string comprehension: a wholly-quoted string in
    // source position. Quote-kind selects the iteration interior:
    //   - double `"…"` → iterable: token-strip (comma/semicolon/
    //     whitespace; colons etc. retained) into a literal list.
    //   - single `'…'` → atomic: one whole-string element.
    // (Outside the source slot a quoted token is a plain string
    // literal; this branch only runs because we're parsing a
    // comprehension source.)
    if trimmed.len() >= 2 && trimmed.starts_with('"') && trimmed.ends_with('"') {
        let inner = &trimmed[1..trimmed.len() - 1];
        let values = super::super::source::split_string_comprehension(inner)
            .into_iter()
            .map(|t| parse_literal_value(t))
            .collect();
        return Ok(Source::Literal { values });
    }
    if trimmed.len() >= 2 && trimmed.starts_with('\'') && trimmed.ends_with('\'') {
        let inner = &trimmed[1..trimmed.len() - 1];
        return Ok(Source::Literal { values: vec![LiteralValue::String(inner.to_string())] });
    }

    // List comprehension sugar `[…]` (SRD-18f Stage 2).
    //   - Pure-literal list (numbers / bools / quoted strings,
    //     no spread, no bare references) → `Source::Literal`,
    //     baked at parse time with a static cardinality (the
    //     historical fast path, unchanged).
    //   - Otherwise — any bare-identifier *reference* element or
    //     a `…`/`...` spread — defers to `Source::Generator`
    //     carrying the bracket text verbatim, so the runtime
    //     evaluator (`eval::try_eval_bracket_list`) resolves each
    //     element against the kernel and applies spread peeling.
    if trimmed.starts_with('[') && trimmed.ends_with(']') {
        let inner = &trimmed[1..trimmed.len() - 1];
        if bracket_is_pure_literal(inner) {
            return parse_literal_list(inner);
        }
        return Ok(Source::Generator {
            expr: trimmed.to_string(),
            cardinality_hint: None,
        });
    }

    // Range: contains `..` and starts with a number-ish.
    if let Some(idx) = find_top_level(trimmed, "..") {
        return parse_range(trimmed, idx);
    }

    // Function-call shape: `ident(...)` → Generator
    if looks_like_function_call(trimmed) {
        return Ok(Source::Generator {
            expr: trimmed.to_string(),
            cardinality_hint: None,
        });
    }

    // Bare scalar literal: `10`, `"hello"`, `true`, `3.14` →
    // single-element Literal. This matches the legacy
    // grammar's `k in 10` shape, where the RHS is a single
    // literal value (the comprehension dispenses exactly one
    // tuple).
    if let Some(value) = try_parse_bare_scalar(trimmed) {
        return Ok(Source::Literal { values: vec![value] });
    }

    // Bare comma-separated list — the legacy grammar accepts
    // `k in 1,2,3` and `y in a,b,c` without brackets. Treat
    // it as a Literal list. The check is conservative:
    // require a top-level comma and that no element contains
    // syntax that would suggest a more complex expression
    // (parens, brackets, braces, operators).
    if trimmed.contains(',') && looks_like_bare_value_list(trimmed) {
        return parse_literal_list(trimmed);
    }

    // Fallback: treat as a Generator expression. The legacy
    // grammar accepts arbitrary expression text (e.g.
    // `pre_{outer}`, `mod_in(cycle, p)`, `range(0, {n})`)
    // that the runtime evaluator resolves via the Polydat Kernel
    // chain. The algebra-layer typing for these is generator
    // (cardinality_hint=None); the bridge back to legacy
    // round-trips them verbatim.
    Ok(Source::Generator {
        expr: trimmed.to_string(),
        cardinality_hint: None,
    })
}

/// Conservative bare-comma-list detector. The legacy form
/// `k in 1,2,3` (no brackets) is a literal list; this matches
/// it without misclassifying expression-like text. Same shape
/// as the legacy `looks_like_literal_list` in
/// `polydat::iteration::comprehension::eval`.
fn looks_like_bare_value_list(text: &str) -> bool {
    !text.chars().any(|c| matches!(
        c,
        '(' | ')' | '[' | ']' | '{' | '}' | '\'' | '"'
        | '+' | '*' | '/' | '%' | '=' | '<' | '>' | '!' | '&' | '|' | '~' | '^' | '?'
    ))
}

/// Detect dynamic-placeholder text like `{a_{b}_c}` (nested
/// braces). Returns the contained text as the name; the runtime
/// interpolator handles the nesting at lookup time.
fn strip_dynamic_curly(s: &str) -> Option<String> {
    let s = s.trim();
    if !s.starts_with('{') || !s.ends_with('}') {
        return None;
    }
    let inner = &s[1..s.len() - 1];
    // Must contain at least one nested `{` — distinguishes
    // dynamic from the simple `{name}` form `strip_curly`
    // already handled.
    if !inner.contains('{') {
        return None;
    }
    Some(inner.to_string())
}

/// Parse a comma-separated literal list. Determines element
/// type from the first element; mixed-type lists currently
/// fall back to string.
fn parse_literal_list(inner: &str) -> Result<Source, SourceParseError> {
    let parts: Vec<&str> = inner
        .split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .collect();

    if parts.is_empty() {
        return Ok(Source::Literal { values: Vec::new() });
    }

    let values: Vec<LiteralValue> = parts
        .iter()
        .map(|s| parse_literal_value(s))
        .collect();

    Ok(Source::Literal { values })
}

/// True when every element of a bracket list is a pure literal
/// (integer, float, bool, or quoted string) and there is no
/// spread (`…`/`...`). Such lists bake to `Source::Literal` at
/// parse time. A bare-identifier element (a reference) or a
/// spread makes the list eval-time (`Source::Generator`).
/// SRD-18f Stage 2.
fn bracket_is_pure_literal(inner: &str) -> bool {
    let elems: Vec<&str> = inner.split(',').map(str::trim).filter(|s| !s.is_empty()).collect();
    if elems.is_empty() {
        return true; // `[]` is a (degenerate) literal list
    }
    elems.iter().all(|e| {
        if e.ends_with('…') || e.ends_with("...") {
            return false; // spread → eval-time
        }
        e.eq_ignore_ascii_case("true")
            || e.eq_ignore_ascii_case("false")
            || ((e.starts_with('"') && e.ends_with('"'))
                || (e.starts_with('\'') && e.ends_with('\'')))
            || e.parse::<i64>().is_ok()
            || e.parse::<f64>().is_ok()
    })
}

fn parse_literal_value(s: &str) -> LiteralValue {
    let s = s.trim();
    if s.eq_ignore_ascii_case("true") {
        return LiteralValue::Bool(true);
    }
    if s.eq_ignore_ascii_case("false") {
        return LiteralValue::Bool(false);
    }
    // Quoted string
    if (s.starts_with('"') && s.ends_with('"'))
        || (s.starts_with('\'') && s.ends_with('\''))
    {
        let inner = &s[1..s.len() - 1];
        return LiteralValue::String(inner.to_string());
    }
    // Integer
    if let Ok(n) = s.parse::<i64>() {
        return LiteralValue::Int(n);
    }
    // Float
    if let Ok(f) = s.parse::<f64>() {
        return LiteralValue::Float(f);
    }
    // Bare identifier → string literal
    LiteralValue::String(s.to_string())
}

/// Parse a range expression starting at `dotdot_idx` (the
/// position of `..`).
fn parse_range(text: &str, dotdot_idx: usize) -> Result<Source, SourceParseError> {
    let lo_str = text[..dotdot_idx].trim();
    let after = &text[dotdot_idx + 2..];

    // `..=` inclusive form
    let (inclusive_end, after) = if let Some(rest) = after.strip_prefix('=') {
        (true, rest)
    } else {
        (false, after)
    };

    // Optional ` step N` suffix OR legacy three-segment form
    // `..N` (e.g. `1..10..2`, `1..=10..2`). Both are step
    // suffixes; the legacy form predates the keyword. Check
    // ` step ` first since it's the documented form.
    let (rhs, step) = if let Some(step_pos) = after.find(" step ") {
        let rhs = after[..step_pos].trim();
        let step_str = after[step_pos + 6..].trim();
        let step: i64 = step_str
            .parse()
            .map_err(|_| SourceParseError::InvalidRange(text.to_string()))?;
        (rhs, step)
    } else if let Some(step_pos) = after.find("..") {
        // Legacy `lo..hi..step` shape — the second `..` is
        // the step separator.
        let rhs = after[..step_pos].trim();
        let step_str = after[step_pos + 2..].trim();
        let step: i64 = step_str
            .parse()
            .map_err(|_| SourceParseError::InvalidRange(text.to_string()))?;
        (rhs, step)
    } else {
        (after.trim(), 1)
    };

    // Try parsing both endpoints as integers first.
    if let (Ok(lo_i), Ok(hi_i)) = (lo_str.parse::<i64>(), rhs.parse::<i64>()) {
        let hi = if inclusive_end { hi_i + 1 } else { hi_i };
        return Ok(Source::IntRange { lo: lo_i, hi, step });
    }
    // Otherwise try as floats → continuous interval.
    if let (Ok(lo_f), Ok(hi_f)) = (lo_str.parse::<f64>(), rhs.parse::<f64>()) {
        let interval = Interval {
            lo: lo_f,
            hi: hi_f,
            lo_open: false,
            hi_open: !inclusive_end,
        };
        return Ok(Source::ContinuousInterval {
            interval,
            measure: ProductMeasure::Uniform,
        });
    }

    Err(SourceParseError::InvalidRange(text.to_string()))
}

fn strip_curly(s: &str) -> Option<String> {
    let s = s.trim();
    if s.starts_with('{') && s.ends_with('}') {
        let inner = &s[1..s.len() - 1];
        let trimmed = inner.trim();
        if !trimmed.is_empty()
            && trimmed.chars().all(|c| c.is_alphanumeric() || c == '_')
        {
            return Some(trimmed.to_string());
        }
    }
    None
}

/// Try to parse `s` as a bare scalar literal — int, float,
/// bool, or quoted string. Returns `None` if `s` is not a
/// well-formed scalar (e.g., a bare identifier without
/// quotes); bare identifiers ambiguously could be names rather
/// than string literals, so we don't accept them here.
fn try_parse_bare_scalar(s: &str) -> Option<LiteralValue> {
    if s.eq_ignore_ascii_case("true") {
        return Some(LiteralValue::Bool(true));
    }
    if s.eq_ignore_ascii_case("false") {
        return Some(LiteralValue::Bool(false));
    }
    if (s.starts_with('"') && s.ends_with('"'))
        || (s.starts_with('\'') && s.ends_with('\''))
    {
        let inner = &s[1..s.len() - 1];
        return Some(LiteralValue::String(inner.to_string()));
    }
    if let Ok(n) = s.parse::<i64>() {
        return Some(LiteralValue::Int(n));
    }
    if let Ok(f) = s.parse::<f64>() {
        return Some(LiteralValue::Float(f));
    }
    None
}

fn looks_like_function_call(s: &str) -> bool {
    let Some(open) = s.find('(') else { return false; };
    if !s.ends_with(')') {
        return false;
    }
    let name = &s[..open];
    !name.is_empty() && name.chars().all(|c| c.is_alphanumeric() || c == '_')
}

/// Find the first top-level occurrence of `needle`,
/// respecting `(`, `[`, `{` nesting.
fn find_top_level(s: &str, needle: &str) -> Option<usize> {
    let bytes = s.as_bytes();
    let needle_bytes = needle.as_bytes();
    let mut depth = 0i64;
    let mut i = 0;
    while i + needle_bytes.len() <= bytes.len() {
        match bytes[i] {
            b'(' | b'[' | b'{' => depth += 1,
            b')' | b']' | b'}' => depth -= 1,
            _ => {}
        }
        if depth == 0 && &bytes[i..i + needle_bytes.len()] == needle_bytes {
            return Some(i);
        }
        i += 1;
    }
    None
}

/// Errors that can arise during source-string parsing.
#[derive(Debug, Clone, PartialEq)]
pub enum SourceParseError {
    /// The source text doesn't match any recognized shape.
    Unrecognized(String),
    /// A range expression couldn't be parsed (bad endpoint
    /// types, malformed step suffix, etc.).
    InvalidRange(String),
}

impl std::fmt::Display for SourceParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SourceParseError::Unrecognized(s) => {
                write!(f, "unrecognized source expression: {s:?}")
            }
            SourceParseError::InvalidRange(s) => {
                write!(f, "invalid range expression: {s:?}")
            }
        }
    }
}

impl std::error::Error for SourceParseError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn int_range_exclusive() {
        let s = parse_source("1..10").unwrap();
        assert!(matches!(s, Source::IntRange { lo: 1, hi: 10, step: 1 }));
    }

    #[test]
    fn double_quoted_source_is_string_comprehension_striped() {
        // SRD-18f §3.2: double-quoted source → token-strip.
        let s = parse_source(r#""rerank_def, rerank_1x, rerank_2x""#).unwrap();
        match s {
            Source::Literal { values } => {
                assert_eq!(values, vec![
                    LiteralValue::String("rerank_def".into()),
                    LiteralValue::String("rerank_1x".into()),
                    LiteralValue::String("rerank_2x".into()),
                ]);
            }
            other => panic!("expected striped Literal, got {other:?}"),
        }
    }

    #[test]
    fn single_quoted_source_is_atomic() {
        // SRD-18f §3.2: single-quoted source → one whole element.
        let s = parse_source("'rerank_def, rerank_1x'").unwrap();
        match s {
            Source::Literal { values } => {
                assert_eq!(values, vec![LiteralValue::String("rerank_def, rerank_1x".into())]);
            }
            other => panic!("expected atomic Literal, got {other:?}"),
        }
    }

    #[test]
    fn int_range_inclusive() {
        let s = parse_source("1..=10").unwrap();
        assert!(matches!(s, Source::IntRange { lo: 1, hi: 11, step: 1 }));
    }

    #[test]
    fn int_range_with_step() {
        let s = parse_source("0..100 step 10").unwrap();
        assert!(matches!(s, Source::IntRange { lo: 0, hi: 100, step: 10 }));
    }

    #[test]
    fn literal_int_list() {
        let s = parse_source("[1, 2, 3]").unwrap();
        match s {
            Source::Literal { values } => {
                assert_eq!(values.len(), 3);
                assert_eq!(values[0], LiteralValue::Int(1));
                assert_eq!(values[2], LiteralValue::Int(3));
            }
            other => panic!("expected Literal, got {other:?}"),
        }
    }

    #[test]
    fn bracket_bare_words_are_references_not_strings() {
        // SRD-18f Stage 2: bare-word bracket elements are wire
        // *references*, not string literals — so the list defers
        // to a Generator (resolved at eval time) rather than
        // baking `["a","b","c"]`. To get string literals, quote
        // them (see `literal_quoted_strings`).
        let s = parse_source("[a, b, c]").unwrap();
        match s {
            Source::Generator { expr, .. } => assert_eq!(expr, "[a, b, c]"),
            other => panic!("expected deferred Generator, got {other:?}"),
        }
    }

    #[test]
    fn bracket_with_spread_defers_to_generator() {
        let s = parse_source("[xs…]").unwrap();
        assert!(matches!(s, Source::Generator { .. }), "spread list must defer: {s:?}");
    }

    #[test]
    fn literal_quoted_strings() {
        let s = parse_source(r#"["hello", "world"]"#).unwrap();
        match s {
            Source::Literal { values } => {
                assert_eq!(values[0], LiteralValue::String("hello".into()));
                assert_eq!(values[1], LiteralValue::String("world".into()));
            }
            other => panic!("expected Literal, got {other:?}"),
        }
    }

    #[test]
    fn literal_float_list() {
        let s = parse_source("[1.5, 2.5, 3.5]").unwrap();
        match s {
            Source::Literal { values } => {
                assert_eq!(values[0], LiteralValue::Float(1.5));
            }
            other => panic!("expected Literal, got {other:?}"),
        }
    }

    #[test]
    fn workload_param_ref() {
        let s = parse_source("{profiles}").unwrap();
        match s {
            Source::WorkloadParamList { name, .. } => assert_eq!(name, "profiles"),
            other => panic!("expected WorkloadParamList, got {other:?}"),
        }
    }

    #[test]
    fn generator_function_call() {
        let s = parse_source("fib(8)").unwrap();
        match s {
            Source::Generator { expr, .. } => assert_eq!(expr, "fib(8)"),
            other => panic!("expected Generator, got {other:?}"),
        }
    }

    #[test]
    fn continuous_interval_via_floats() {
        let s = parse_source("0.0..1.0").unwrap();
        match s {
            Source::ContinuousInterval { interval, measure } => {
                assert_eq!(interval.lo, 0.0);
                assert_eq!(interval.hi, 1.0);
                assert!(matches!(measure, ProductMeasure::Uniform));
            }
            other => panic!("expected ContinuousInterval, got {other:?}"),
        }
    }

    #[test]
    fn continuous_interval_inclusive() {
        let s = parse_source("0.0..=1.0").unwrap();
        match s {
            Source::ContinuousInterval { interval, .. } => {
                assert!(!interval.hi_open);
            }
            other => panic!("expected ContinuousInterval, got {other:?}"),
        }
    }

    #[test]
    fn unrecognized_source_falls_back_to_generator() {
        // Previously: returned Err(Unrecognized). The legacy
        // grammar accepts arbitrary expression text and the
        // runtime evaluator resolves it via the Polydat Kernel
        // chain, so unrecognized shapes pass through as a
        // Generator expression rather than failing the parse.
        let s = parse_source("totally nonsense").unwrap();
        match s {
            Source::Generator { expr, .. } => assert_eq!(expr, "totally nonsense"),
            other => panic!("expected Generator, got {other:?}"),
        }
    }

    #[test]
    fn empty_literal_list() {
        let s = parse_source("[]").unwrap();
        match s {
            Source::Literal { values } => assert!(values.is_empty()),
            other => panic!("expected empty Literal, got {other:?}"),
        }
    }
}
