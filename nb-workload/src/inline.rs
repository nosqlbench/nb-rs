// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! Inline workload synthesis from the `op=` command-line parameter.
//!
//! Parses an inline op template string into a [`Workload`] — the same
//! type that [`parse_workload()`](crate::parse::parse_workload) returns
//! from YAML. Inline `{{expr}}` bindings are extracted, assigned
//! synthetic GK output names, and compiled to a GK source block.
//!
//! See SRD 35 for design details.

use std::collections::HashMap;

use crate::model::{BindingsDef, ParsedOp, Workload};

/// Synthesize a [`Workload`] from an inline `op=` string.
///
/// # Inline binding syntax
///
/// - `{{expr}}` — inline GK expression. Compiled into the GK
///   kernel at init time, then invoked per cycle like any other
///   GK output. Extracted and replaced with `{__inline_N}`.
/// - `{name}` — reference bind point, resolved by the standard
///   bind point pipeline (GK output, coordinate, capture).
///
/// # Multiple ops
///
/// Semicolons separate multiple ops. An optional `N:` prefix sets
/// the ratio:
///
/// ```text
/// "3:read {{cycle}};1:write {{mod(cycle, 100)}}"
/// ```
///
/// # Examples
///
/// ```
/// use nb_workload::inline::synthesize_inline_workload;
///
/// let w = synthesize_inline_workload("hello {{cycle}}").unwrap();
/// assert_eq!(w.ops.len(), 1);
/// assert_eq!(w.ops[0].name, "inline_0");
/// ```
pub fn synthesize_inline_workload(op_template: &str) -> Result<Workload, String> {
    if op_template.trim().is_empty() {
        return Err("op= value is empty".into());
    }

    // Split on unquoted semicolons into individual op segments.
    let segments = split_ops(op_template);

    // Collect all inline expressions across all segments to build
    // a single shared GK source block.
    let mut inline_exprs: Vec<String> = Vec::new();
    let mut expr_index: HashMap<String, usize> = HashMap::new();

    // First pass: discover all {{expr}} across all segments.
    for seg in &segments {
        for expr in extract_inline_exprs(&seg.template) {
            if !expr_index.contains_key(&expr) {
                let idx = inline_exprs.len();
                expr_index.insert(expr.clone(), idx);
                inline_exprs.push(expr);
            }
        }
    }

    // Build GK source from collected expressions.
    let gk_source = if inline_exprs.is_empty() {
        String::new()
    } else {
        let mut src = String::from("coordinates := (cycle)\n");
        for (i, expr) in inline_exprs.iter().enumerate() {
            src.push_str(&format!("__inline_{i} := {expr}\n"));
        }
        src
    };

    // Second pass: rewrite templates and build ParsedOps.
    let mut ops = Vec::with_capacity(segments.len());
    for (i, seg) in segments.iter().enumerate() {
        let rewritten = rewrite_template(&seg.template, &expr_index);

        let mut op = ParsedOp::simple(&format!("inline_{i}"), &rewritten);

        if seg.ratio != 1 {
            op.params.insert(
                "ratio".to_string(),
                serde_json::Value::Number(serde_json::Number::from(seg.ratio)),
            );
        }

        op.tags.insert("name".to_string(), op.name.clone());
        op.tags.insert("op".to_string(), op.name.clone());
        op.tags.insert("block".to_string(), "inline".to_string());

        if !gk_source.is_empty() {
            op.bindings = BindingsDef::GkSource(gk_source.clone());
        }

        ops.push(op);
    }

    Ok(Workload {
        description: Some("inline workload".into()),
        scenarios: HashMap::new(),
        ops,
    })
}

// ─── Internal Types ─────────────────────────────────────────

struct OpSegment {
    template: String,
    ratio: u64,
}

// ─── Helpers ────────────────────────────────────────────────

/// Split an op string on unquoted semicolons, extracting optional
/// ratio prefixes (`3:template`).
fn split_ops(input: &str) -> Vec<OpSegment> {
    let mut segments = Vec::new();
    let mut current = String::new();
    let mut in_braces = 0u32;

    for c in input.chars() {
        match c {
            '{' => {
                in_braces += 1;
                current.push(c);
            }
            '}' => {
                in_braces = in_braces.saturating_sub(1);
                current.push(c);
            }
            ';' if in_braces == 0 => {
                let seg = current.trim().to_string();
                if !seg.is_empty() {
                    segments.push(parse_segment(&seg));
                }
                current.clear();
            }
            _ => current.push(c),
        }
    }
    let seg = current.trim().to_string();
    if !seg.is_empty() {
        segments.push(parse_segment(&seg));
    }
    segments
}

/// Parse a single segment, extracting an optional `N:` ratio prefix.
fn parse_segment(s: &str) -> OpSegment {
    // Look for `N:` at the start, but don't confuse with `{{...}}`.
    if let Some(colon_pos) = s.find(':') {
        let prefix = &s[..colon_pos];
        // Only treat as ratio if prefix is all digits.
        if !prefix.is_empty() && prefix.chars().all(|c| c.is_ascii_digit()) {
            if let Ok(ratio) = prefix.parse::<u64>() {
                return OpSegment {
                    template: s[colon_pos + 1..].trim().to_string(),
                    ratio,
                };
            }
        }
    }
    OpSegment {
        template: s.to_string(),
        ratio: 1,
    }
}

/// Extract all `{{expr}}` occurrences from a template string.
fn extract_inline_exprs(template: &str) -> Vec<String> {
    let mut exprs = Vec::new();
    let bytes = template.as_bytes();
    let len = bytes.len();
    let mut i = 0;

    while i + 1 < len {
        if bytes[i] == b'{' && bytes[i + 1] == b'{' {
            // Find matching }}.
            let start = i + 2;
            let mut depth = 1u32;
            let mut j = start;
            while j + 1 < len {
                if bytes[j] == b'{' && bytes[j + 1] == b'{' {
                    depth += 1;
                    j += 2;
                } else if bytes[j] == b'}' && bytes[j + 1] == b'}' {
                    depth -= 1;
                    if depth == 0 {
                        let expr = template[start..j].trim().to_string();
                        if !expr.is_empty() {
                            exprs.push(expr);
                        }
                        i = j + 2;
                        break;
                    }
                    j += 2;
                } else {
                    j += 1;
                }
            }
            if depth > 0 {
                // Unmatched {{ — skip past it.
                i += 2;
            }
        } else {
            i += 1;
        }
    }
    exprs
}

/// Rewrite a template by replacing `{{expr}}` with `{__inline_N}`.
fn rewrite_template(template: &str, expr_index: &HashMap<String, usize>) -> String {
    let mut result = String::with_capacity(template.len());
    let bytes = template.as_bytes();
    let len = bytes.len();
    let mut i = 0;

    while i < len {
        if i + 1 < len && bytes[i] == b'{' && bytes[i + 1] == b'{' {
            let start = i + 2;
            let mut depth = 1u32;
            let mut j = start;
            while j + 1 < len {
                if bytes[j] == b'{' && bytes[j + 1] == b'{' {
                    depth += 1;
                    j += 2;
                } else if bytes[j] == b'}' && bytes[j + 1] == b'}' {
                    depth -= 1;
                    if depth == 0 {
                        let expr = template[start..j].trim().to_string();
                        if let Some(&idx) = expr_index.get(&expr) {
                            result.push_str(&format!("{{__inline_{idx}}}"));
                        } else {
                            // Should not happen, but preserve original.
                            result.push_str(&template[i..j + 2]);
                        }
                        i = j + 2;
                        break;
                    }
                    j += 2;
                } else {
                    j += 1;
                }
            }
            if depth > 0 {
                result.push_str(&template[i..]);
                break;
            }
        } else {
            result.push(bytes[i] as char);
            i += 1;
        }
    }
    result
}

// ─── Tests ──────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_inline_binding() {
        let w = synthesize_inline_workload("hello {{cycle}}").unwrap();
        assert_eq!(w.ops.len(), 1);
        assert_eq!(w.ops[0].name, "inline_0");
        let stmt = w.ops[0].op.get("stmt").unwrap().as_str().unwrap();
        assert_eq!(stmt, "hello {__inline_0}");
        match &w.ops[0].bindings {
            BindingsDef::GkSource(src) => {
                assert!(src.contains("coordinates := (cycle)"));
                assert!(src.contains("__inline_0 := cycle"));
            }
            _ => panic!("expected GkSource bindings"),
        }
    }

    #[test]
    fn multiple_inline_bindings() {
        let w = synthesize_inline_workload(
            "id={{mod(hash(cycle), 100000)}} name={{number_to_words(cycle)}}"
        ).unwrap();
        assert_eq!(w.ops.len(), 1);
        let stmt = w.ops[0].op.get("stmt").unwrap().as_str().unwrap();
        assert_eq!(stmt, "id={__inline_0} name={__inline_1}");
        match &w.ops[0].bindings {
            BindingsDef::GkSource(src) => {
                assert!(src.contains("__inline_0 := mod(hash(cycle), 100000)"));
                assert!(src.contains("__inline_1 := number_to_words(cycle)"));
            }
            _ => panic!("expected GkSource bindings"),
        }
    }

    #[test]
    fn no_inline_bindings_plain_text() {
        let w = synthesize_inline_workload("hello world").unwrap();
        assert_eq!(w.ops.len(), 1);
        let stmt = w.ops[0].op.get("stmt").unwrap().as_str().unwrap();
        assert_eq!(stmt, "hello world");
        assert!(w.ops[0].bindings.is_empty());
    }

    #[test]
    fn reference_bind_points_preserved() {
        let w = synthesize_inline_workload("value={cycle}").unwrap();
        assert_eq!(w.ops.len(), 1);
        let stmt = w.ops[0].op.get("stmt").unwrap().as_str().unwrap();
        assert_eq!(stmt, "value={cycle}");
        assert!(w.ops[0].bindings.is_empty());
    }

    #[test]
    fn semicolon_split_multiple_ops() {
        let w = synthesize_inline_workload("read {{cycle}};write {{mod(cycle, 100)}}").unwrap();
        assert_eq!(w.ops.len(), 2);
        assert_eq!(w.ops[0].name, "inline_0");
        assert_eq!(w.ops[1].name, "inline_1");
    }

    #[test]
    fn ratio_prefix() {
        let w = synthesize_inline_workload("3:read {{cycle}};1:write {{cycle}}").unwrap();
        assert_eq!(w.ops.len(), 2);
        assert_eq!(
            w.ops[0].params.get("ratio").unwrap().as_u64().unwrap(),
            3
        );
        // ratio=1 is the default, so it's not stored explicitly.
        assert!(!w.ops[1].params.contains_key("ratio"));
    }

    #[test]
    fn ratio_one_not_stored() {
        let w = synthesize_inline_workload("hello {{cycle}}").unwrap();
        assert!(!w.ops[0].params.contains_key("ratio"));
    }

    #[test]
    fn duplicate_expressions_share_output() {
        let w = synthesize_inline_workload(
            "a={{hash(cycle)}};b={{hash(cycle)}}"
        ).unwrap();
        // Both ops should reference the same __inline_0.
        let stmt0 = w.ops[0].op.get("stmt").unwrap().as_str().unwrap();
        let stmt1 = w.ops[1].op.get("stmt").unwrap().as_str().unwrap();
        assert_eq!(stmt0, "a={__inline_0}");
        assert_eq!(stmt1, "b={__inline_0}");
        match &w.ops[0].bindings {
            BindingsDef::GkSource(src) => {
                // Only one output for hash(cycle).
                let count = src.matches("__inline_").count();
                assert_eq!(count, 1);
            }
            _ => panic!("expected GkSource"),
        }
    }

    #[test]
    fn empty_op_is_error() {
        assert!(synthesize_inline_workload("").is_err());
        assert!(synthesize_inline_workload("   ").is_err());
    }

    #[test]
    fn mixed_reference_and_inline() {
        let w = synthesize_inline_workload(
            "id={{mod(hash(cycle), 1000)}} raw={cycle}"
        ).unwrap();
        let stmt = w.ops[0].op.get("stmt").unwrap().as_str().unwrap();
        assert_eq!(stmt, "id={__inline_0} raw={cycle}");
    }
}
