// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! Regex processing nodes.

use crate::node::{Commutativity, GkNode, NodeMeta, Port, PortType, Value};
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
                inputs: vec![Port::new("input", PortType::Str)],
                outputs: vec![Port::new("output", PortType::Str)],
                commutativity: Commutativity::Positional,
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
                inputs: vec![Port::new("input", PortType::Str)],
                outputs: vec![Port::bool("output")],
                commutativity: Commutativity::Positional,
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
                inputs: vec![Port::new("input", PortType::Str)],
                outputs: vec![Port::new("output", PortType::Str)],
                commutativity: Commutativity::Positional,
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
