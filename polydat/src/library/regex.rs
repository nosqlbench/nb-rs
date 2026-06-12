// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Regex processing nodes.

use regex::Regex;

impl crate::derive_support::PolydatSetup for Regex {}

/// Regex replace: substitute all matches of a pattern with the
/// replacement string. SRD-80b Phase E migration via two
/// Const<&str> args + cached compiled Regex.
#[crate::polydat_node(category = Regex)]
fn regex_replace(
    input: &str,
    pattern: crate::derive_support::Const<&str>,
    replacement: crate::derive_support::Const<&str>,
    #[poly_const(compile_regex, from = pattern)]
    re: &Regex,
) -> String {
    let _ = pattern;
    re.replace_all(input, replacement.0).into_owned()
}

/// Build a Regex from a pattern. Panics on invalid pattern;
/// the const-arg constraint (registered in the FuncSig the
/// macro emits is forthcoming) catches malformed patterns at
/// workload-compile-time, so the panic here is a true bug
/// indicator only.
fn compile_regex(pattern: &str) -> Regex {
    Regex::new(pattern).expect("invalid regex")
}

/// Regex match: test if input matches a pattern.
/// SRD-80 PR B.6 migration.
#[crate::polydat_node(category = Regex)]
fn regex_match(
    input: &str,
    pattern: crate::derive_support::Const<&str>,
    #[poly_const(compile_regex, from = pattern)]
    re: &Regex,
) -> bool {
    let matched = re.is_match(input);
    if crate::library::debug_nodes_enabled() {
        let snippet: String = input.chars().take(200).collect();
        let ellipsis = if input.len() > snippet.len() { "…" } else { "" };
        crate::library::support::audit::debug(&format!(
            "regex_match: pattern={:?} input.len={} matched={matched} input.snippet={:?}{ellipsis}",
            re.as_str(), input.len(), snippet,
        ));
    }
    matched
}

/// Regex extract: extract the first capture group (or full match).
/// SRD-80 PR B.6 migration.
#[crate::polydat_node(category = Regex)]
fn regex_extract(
    input: &str,
    pattern: crate::derive_support::Const<&str>,
    #[poly_const(compile_regex, from = pattern)]
    re: &Regex,
) -> String {
    if let Some(caps) = re.captures(input) {
        caps.get(1)
            .or_else(|| caps.get(0))
            .map(|m| m.as_str().to_string())
            .unwrap_or_default()
    } else {
        String::new()
    }
}

// ---------------------------------------------------------------------------
// Signature declarations for the DSL registry
// ---------------------------------------------------------------------------

use crate::dsl::registry::{Arity, FuncCategory, FuncSig, ParamSpec};
use crate::ast::SlotType;

/// Signatures for regex nodes.
pub fn signatures() -> &'static [FuncSig] {
    use FuncCategory as C;
    &[
        FuncSig {
            name: "regex_replace", category: C::Regex,
            outputs: 1, description: "regex substitution",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true, example: "cycle", constraint: None },
                ParamSpec { name: "pattern", slot_type: SlotType::ConstStr, required: true, example: "\"[a-z]+\"",
                    constraint: Some(crate::dsl::const_constraints::ConstConstraint::StrParser(validate_regex_pattern)) },
                ParamSpec { name: "replacement", slot_type: SlotType::ConstStr, required: true, example: "\"X\"", constraint: None },
            ],
            arity: Arity::Fixed,
            commutativity: crate::ast::Commutativity::Positional,
            help: "Substitute all matches of a regex pattern in the input string.\nThe regex is compiled at init time for fast cycle-time evaluation.\nParameters:\n  input       — String wire input\n  pattern     — regex pattern (Rust regex syntax)\n  replacement — replacement string ($1, $2 for capture groups)\nExample: regex_replace(name, \"[^a-zA-Z]\", \"_\")",
            default_resolver: None,
            output_type: crate::dsl::registry::OutputType::Fixed,
            // Hand registration: no static return-port declaration;
            // type inference falls back to the name heuristic.
            output_port: None,
        },
        // `regex_match` migrated to `#[polydat_node]` per
        // SRD-80 PR B.6. Const-constraint registration is
        // forthcoming via a future macro attribute pass.
    ]
}

/// Try to build a regex node from a function name and const args.
///
/// Returns `None` if the name is not handled by this module.
pub(crate) fn build_node(_name: &str, _wires: &[crate::compile::assembly::WireRef], _wire_types: &[crate::ast::PortType], _consts: &[crate::dsl::factory::ConstArg]) -> Option<Result<Box<dyn crate::ast::PolydatNode>, String>> {
    // All regex nodes route via proc-macro-emitted NodeRegistration
    // (regex_match, regex_extract, regex_replace).
    None
}


fn validate_regex_pattern(pattern: &str) -> Result<(), String> {
    regex::Regex::new(pattern)
        .map(|_| ())
        .map_err(|e| format!("invalid regex '{pattern}': {e}"))
}

crate::register_nodes!(signatures, build_node);
#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{PolydatNode, Value};

    #[test]
    fn regex_replace_basic() {
        let node = RegexReplace::new(r"\d+".to_string(), "NUM".to_string());
        let mut out = [Value::None];
        node.eval(&[Value::Str("abc 123 def 456".into())], &mut out);
        assert_eq!(out[0].as_str(), "abc NUM def NUM");
    }

    #[test]
    fn regex_replace_no_match() {
        let node = RegexReplace::new(r"\d+".to_string(), "NUM".to_string());
        let mut out = [Value::None];
        node.eval(&[Value::Str("no numbers here".into())], &mut out);
        assert_eq!(out[0].as_str(), "no numbers here");
    }

    #[test]
    fn regex_match_true() {
        let node = RegexMatch::new(r"^\d{3}-\d{4}$".to_string());
        let mut out = [Value::None];
        node.eval(&[Value::Str("123-4567".into())], &mut out);
        assert!(out[0].as_bool());
    }

    #[test]
    fn regex_match_false() {
        let node = RegexMatch::new(r"^\d{3}-\d{4}$".to_string());
        let mut out = [Value::None];
        node.eval(&[Value::Str("hello".into())], &mut out);
        assert!(!out[0].as_bool());
    }

    #[test]
    fn regex_extract_capture_group() {
        let node = RegexExtract::new(r"name=(\w+)".to_string());
        let mut out = [Value::None];
        node.eval(&[Value::Str("name=Alice age=30".into())], &mut out);
        assert_eq!(out[0].as_str(), "Alice");
    }

    #[test]
    fn regex_extract_no_group() {
        let node = RegexExtract::new(r"\d+".to_string());
        let mut out = [Value::None];
        node.eval(&[Value::Str("abc 42 def".into())], &mut out);
        assert_eq!(out[0].as_str(), "42");
    }

    #[test]
    fn regex_extract_no_match() {
        let node = RegexExtract::new(r"\d+".to_string());
        let mut out = [Value::None];
        node.eval(&[Value::Str("no digits".into())], &mut out);
        assert_eq!(out[0].as_str(), "");
    }
}
