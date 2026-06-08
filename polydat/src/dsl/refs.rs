// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Grammar-based free-name extraction for Polydat expression text.
//!
//! Consumers that need to know "which wire / param / coordinate
//! names does this expression reference?" — workload validators,
//! comprehension-source analysis, the YAML→Polydat fusion layer —
//! MUST go through this module rather than byte-scanning the
//! source text for `{...}` or identifier-shaped runs. Byte
//! scanning misclassifies bare wire references (`concat(foo)`),
//! function names, and string-literal contents; the grammar
//! resolves all three correctly because it parses the same
//! tokens the kernel compiler does.
//!
//! The extractor is built on the canonical lexer + expression
//! parser ([`crate::dsl::lexer::lex`] +
//! [`crate::dsl::parser::parse_expression`]) so a reference is
//! recognised exactly when the compiler would treat it as one.
//! `FieldAccess` (`base.vector`) contributes its `source` name;
//! `StringLit` contributes the `{name}` interpolation references
//! it carries; function callee names are NOT references (they
//! resolve in the function registry, not the wire scope).

use std::collections::BTreeSet;

use crate::dsl::ast::{Arg, Expr};

/// Parse `text` as a single Polydat expression and return the
/// set of free names it references — wire/param/coordinate
/// identifiers, `FieldAccess` source names, and `{name}`
/// interpolation references inside string literals. Function
/// callee names are excluded (they resolve in the function
/// registry).
///
/// Returns an empty set when `text` does not lex/parse as a
/// single expression. Callers that need to distinguish "no
/// references" from "not an expression" should use
/// [`try_referenced_names`].
pub fn referenced_names(text: &str) -> BTreeSet<String> {
    try_referenced_names(text).unwrap_or_default()
}

/// Like [`referenced_names`] but returns `Err` with the
/// lexer/parser diagnostic when `text` is not a single valid
/// Polydat expression. Use when a parse failure should surface
/// to the operator rather than silently yield no references.
pub fn try_referenced_names(text: &str) -> Result<BTreeSet<String>, String> {
    let tokens = crate::dsl::lexer::lex(text)?;
    let expr = crate::dsl::parser::parse_expression(tokens)?;
    let mut out = BTreeSet::new();
    collect_expr_refs(&expr, &mut out);
    Ok(out)
}

/// Walk a parsed [`Expr`], inserting every free name reference
/// into `out`. The traversal mirrors
/// [`crate::dsl::validate::validate_expr`]'s reference-collection
/// arm minus the diagnostics, so the two stay in lockstep about
/// what counts as a reference.
pub fn collect_expr_refs(expr: &Expr, out: &mut BTreeSet<String>) {
    match expr {
        Expr::Ident(name, _) => {
            // The lexer has no BoolLit variant — `true` / `false`
            // arrive as `Expr::Ident`. They are keyword literals,
            // not wire references (every typed evaluator special-
            // cases them), so they must not count as references —
            // matches `crate::dsl::validate::collect_references`.
            if name != "true" && name != "false" {
                out.insert(name.clone());
            }
        }
        Expr::Call(call) => {
            // The callee name is a function-registry lookup, not
            // a wire reference — skip it. Arguments are walked.
            for arg in &call.args {
                let inner = match arg {
                    Arg::Positional(e) => e,
                    Arg::Named(_, e) => e,
                };
                collect_expr_refs(inner, out);
            }
        }
        Expr::BinOp(lhs, _, rhs) => {
            collect_expr_refs(lhs, out);
            collect_expr_refs(rhs, out);
        }
        Expr::UnaryNeg(inner, _) | Expr::UnaryBitNot(inner, _)
        | Expr::Cast(inner, _, _) => {
            collect_expr_refs(inner, out);
        }
        Expr::ArrayLit(elems, _) => {
            for e in elems {
                collect_expr_refs(e, out);
            }
        }
        Expr::FieldAccess { source, .. } => {
            // `base.vector` references the source wire `base`.
            out.insert(source.clone());
        }
        Expr::StringLit(s, _) => {
            collect_string_interpolation_refs(s, out);
        }
        Expr::IntLit(..) | Expr::FloatLit(..) => {}
    }
}

/// Extract `{name}` interpolation references from a string
/// literal's contents. Skips format specifiers (`{:05}`,
/// `{:.2}`) and non-identifier bodies. This is the
/// string-interpolation grammar — the only place `{name}` is a
/// reference inside a parsed Polydat expression. (Raw YAML
/// template fields like an op's `prepared:` use the same
/// interpolation grammar but never reach the Polydat parser; the
/// YAML-fusion layer applies [`collect_string_interpolation_refs`]
/// to those directly.)
pub fn collect_string_interpolation_refs(s: &str, out: &mut BTreeSet<String>) {
    let chars: Vec<char> = s.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        if chars[i] != '{' {
            i += 1;
            continue;
        }
        // Balance nested braces so composite templates
        // (`{a_{b}_c}`) are spanned as a unit, then recurse into
        // the body to pick up the inner leaf names.
        let body_start = i + 1;
        let mut depth = 1;
        let mut j = body_start;
        while j < chars.len() && depth > 0 {
            match chars[j] {
                '{' => depth += 1,
                '}' => depth -= 1,
                _ => {}
            }
            if depth == 0 { break; }
            j += 1;
        }
        if depth != 0 {
            break; // unmatched `{` — treat the rest as literal
        }
        let body: String = chars[body_start..j].iter().collect();
        if body.contains('{') {
            // Composite — recurse to collect the inner leaves.
            collect_string_interpolation_refs(&body, out);
        } else if is_plain_ident(&body) {
            out.insert(body);
        } else {
            // Expression-bodied placeholder (`{is_one_of(x, "y")}`,
            // `{mod(hash(cycle), 100)}`) — parse it with the
            // expression grammar and collect its free names.
            if let Ok(inner) = try_referenced_names(&body) {
                out.extend(inner);
            }
        }
        i = j + 1;
    }
}

/// True when `s` is a single plain identifier: starts with a
/// letter or `_`, followed by alphanumerics / `_`.
fn is_plain_ident(s: &str) -> bool {
    let mut chars = s.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names(text: &str) -> Vec<String> {
        referenced_names(text).into_iter().collect()
    }

    #[test]
    fn bare_identifier_is_a_reference() {
        assert_eq!(names("eh_values"), vec!["eh_values"]);
    }

    #[test]
    fn function_call_args_are_references_callee_is_not() {
        // `concat` is a function name (registry), not a wire ref;
        // `nbo_v_values` is the argument and IS a reference.
        assert_eq!(names("concat(nbo_v_values)"), vec!["nbo_v_values"]);
    }

    #[test]
    fn nested_calls_collect_all_arg_idents() {
        let got = names("mod(hash(cycle), p)");
        assert_eq!(got, vec!["cycle", "p"]);
    }

    #[test]
    fn string_literal_interpolation_refs() {
        let got = names(r#""fknn_oat_{sm_lc}_m{mnc}""#);
        assert_eq!(got, vec!["mnc", "sm_lc"]);
    }

    #[test]
    fn string_literal_plain_text_has_no_refs() {
        assert!(names(r#""just text""#).is_empty());
    }

    #[test]
    fn field_access_references_source_wire() {
        assert_eq!(names("base.vector"), vec!["base"]);
    }

    #[test]
    fn arithmetic_operands_are_references() {
        let got = names("a + b * c");
        assert_eq!(got, vec!["a", "b", "c"]);
    }

    #[test]
    fn numeric_and_bool_literals_are_not_references() {
        assert!(names("1000").is_empty());
        assert!(names("3.14").is_empty());
        // `true` / `false` lex as keyword literals, not idents.
        assert!(names("true").is_empty());
    }

    #[test]
    fn unparseable_text_yields_no_refs_via_lenient_api() {
        // The lenient API swallows parse errors.
        assert!(names("((((").is_empty());
        // The strict API surfaces them.
        assert!(try_referenced_names("((((").is_err());
    }
}
