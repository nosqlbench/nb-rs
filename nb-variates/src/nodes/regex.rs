// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Regex processing nodes.

use crate::node::{GkNode, NodeMeta, Port, PortType, Slot, Value};
use regex::Regex;

/// Regex replace: substitute all matches of a pattern.
///
/// Signature: `(input: String) -> (String)`
/// Init params: `pattern`, `replacement`
pub struct RegexReplace {
    meta: NodeMeta,
    re: Regex,
    replacement: String,
}

impl RegexReplace {
    pub fn new(pattern: &str, replacement: &str) -> Self {
        Self {
            meta: NodeMeta {
                name: "regex_replace".into(),
                outs: vec![Port::new("output", PortType::Str)],
                ins: vec![Slot::Wire(Port::new("input", PortType::Str))],
            },
            re: Regex::new(pattern).expect("invalid regex"),
            replacement: replacement.to_string(),
        }
    }
}

impl GkNode for RegexReplace {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let result = self.re.replace_all(inputs[0].as_str(), &self.replacement);
        outputs[0] = Value::Str(result.into_owned());
    }
}

/// Regex match: test if input matches a pattern.
///
/// Signature: `(input: String) -> (bool)`
pub struct RegexMatch {
    meta: NodeMeta,
    re: Regex,
}

impl RegexMatch {
    pub fn new(pattern: &str) -> Self {
        Self {
            meta: NodeMeta {
                name: "regex_match".into(),
                outs: vec![Port::bool("output")],
                ins: vec![Slot::Wire(Port::new("input", PortType::Str))],
            },
            re: Regex::new(pattern).expect("invalid regex"),
        }
    }
}

impl GkNode for RegexMatch {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::Bool(self.re.is_match(inputs[0].as_str()));
    }
}

/// Regex extract: extract the first capture group (or full match).
///
/// Signature: `(input: String) -> (String)`
pub struct RegexExtract {
    meta: NodeMeta,
    re: Regex,
}

impl RegexExtract {
    pub fn new(pattern: &str) -> Self {
        Self {
            meta: NodeMeta {
                name: "regex_extract".into(),
                outs: vec![Port::new("output", PortType::Str)],
                ins: vec![Slot::Wire(Port::new("input", PortType::Str))],
            },
            re: Regex::new(pattern).expect("invalid regex"),
        }
    }
}

impl GkNode for RegexExtract {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let s = inputs[0].as_str();
        let result = if let Some(caps) = self.re.captures(s) {
            caps.get(1)
                .or_else(|| caps.get(0))
                .map(|m| m.as_str().to_string())
                .unwrap_or_default()
        } else {
            String::new()
        };
        outputs[0] = Value::Str(result);
    }
}

// ---------------------------------------------------------------------------
// Signature declarations for the DSL registry
// ---------------------------------------------------------------------------

use crate::dsl::registry::{Arity, FuncCategory, FuncSig, ParamSpec};
use crate::node::SlotType;

/// Signatures for regex nodes.
pub fn signatures() -> &'static [FuncSig] {
    use FuncCategory as C;
    &[
        FuncSig {
            name: "regex_replace", category: C::Regex,
            outputs: 1, description: "regex substitution",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true, example: "cycle" },
                ParamSpec { name: "pattern", slot_type: SlotType::ConstStr, required: true, example: "\"[a-z]+\"" },
                ParamSpec { name: "replacement", slot_type: SlotType::ConstStr, required: true, example: "\"X\"" },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Substitute all matches of a regex pattern in the input string.\nThe regex is compiled at init time for fast cycle-time evaluation.\nParameters:\n  input       — String wire input\n  pattern     — regex pattern (Rust regex syntax)\n  replacement — replacement string ($1, $2 for capture groups)\nExample: regex_replace(name, \"[^a-zA-Z]\", \"_\")",
        },
        FuncSig {
            name: "regex_match", category: C::Regex,
            outputs: 1, description: "test if string matches regex",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true, example: "cycle" },
                ParamSpec { name: "pattern", slot_type: SlotType::ConstStr, required: true, example: "\"[a-z]+\"" },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Test if a string matches a regex pattern. Returns 1 (match) or 0 (no match).\nThe regex is compiled at init time. Tests for a partial match\n(use ^...$ anchors for a full match).\nParameters:\n  input   — String wire input\n  pattern — regex pattern (Rust regex syntax)\nExample: regex_match(email, \"^[^@]+@[^@]+$\")",
        },
    ]
}

/// Try to build a regex node from a function name and const args.
///
/// Returns `None` if the name is not handled by this module.
pub(crate) fn build_node(name: &str, _wires: &[crate::assembly::WireRef], consts: &[crate::dsl::factory::ConstArg]) -> Option<Result<Box<dyn crate::node::GkNode>, String>> {
    match name {
        "regex_replace" => Some(Ok(Box::new(RegexReplace::new(
            consts.first().map(|c| c.as_str()).unwrap_or(""),
            consts.get(1).map(|c| c.as_str()).unwrap_or(""),
        )))),
        "regex_match" => Some(Ok(Box::new(RegexMatch::new(
            consts.first().map(|c| c.as_str()).unwrap_or(".*"),
        )))),
        _ => None,
    }
}


crate::register_nodes!(signatures, build_node);
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn regex_replace_basic() {
        let node = RegexReplace::new(r"\d+", "NUM");
        let mut out = [Value::None];
        node.eval(&[Value::Str("abc 123 def 456".into())], &mut out);
        assert_eq!(out[0].as_str(), "abc NUM def NUM");
    }

    #[test]
    fn regex_replace_no_match() {
        let node = RegexReplace::new(r"\d+", "NUM");
        let mut out = [Value::None];
        node.eval(&[Value::Str("no numbers here".into())], &mut out);
        assert_eq!(out[0].as_str(), "no numbers here");
    }

    #[test]
    fn regex_match_true() {
        let node = RegexMatch::new(r"^\d{3}-\d{4}$");
        let mut out = [Value::None];
        node.eval(&[Value::Str("123-4567".into())], &mut out);
        assert!(out[0].as_bool());
    }

    #[test]
    fn regex_match_false() {
        let node = RegexMatch::new(r"^\d{3}-\d{4}$");
        let mut out = [Value::None];
        node.eval(&[Value::Str("hello".into())], &mut out);
        assert!(!out[0].as_bool());
    }

    #[test]
    fn regex_extract_capture_group() {
        let node = RegexExtract::new(r"name=(\w+)");
        let mut out = [Value::None];
        node.eval(&[Value::Str("name=Alice age=30".into())], &mut out);
        assert_eq!(out[0].as_str(), "Alice");
    }

    #[test]
    fn regex_extract_no_group() {
        let node = RegexExtract::new(r"\d+");
        let mut out = [Value::None];
        node.eval(&[Value::Str("abc 42 def".into())], &mut out);
        assert_eq!(out[0].as_str(), "42");
    }

    #[test]
    fn regex_extract_no_match() {
        let node = RegexExtract::new(r"\d+");
        let mut out = [Value::None];
        node.eval(&[Value::Str("no digits".into())], &mut out);
        assert_eq!(out[0].as_str(), "");
    }
}
