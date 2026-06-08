// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Printf-style formatting node.
//!
//! Takes a format string and N inputs, produces a formatted String.
//! Uses `{}` placeholders (Rust-style, not C printf-style), with
//! optional format specifiers.
//!
//! Supported specifiers:
//! - `{}` — default display
//! - `{:05}` — zero-padded to width 5 (u64)
//! - `{:.2}` — 2 decimal places (f64)
//! - `{:x}` — lowercase hex (u64)
//! - `{:X}` — uppercase hex (u64)
//! - `{:b}` — binary (u64)
//! - `{:o}` — octal (u64)
//!
//! SRD-80b Phase E: migrated from a hand-written `impl PolydatNode for
//! Printf` to `#[polydat_node]` + `Const<&str>` (the format string) +
//! `#[poly_const]` cached `ParsedFormat` + `&[Value]` variadic wires.
//! The cached `ParsedFormat` is computed once at construction (in the
//! `parse_format` setup-fn) so per-eval work is just iterating the
//! pre-parsed segments.

use crate::ast::Value;
use crate::derive_support::PolydatSetup;

/// A parsed format segment: either literal text or a placeholder.
#[derive(Debug, Clone)]
pub enum Segment {
    Literal(String),
    Placeholder(FormatSpec),
}

#[derive(Debug, Clone)]
pub struct FormatSpec {
    /// Input index (sequential, 0-based)
    index: usize,
    /// Optional width
    width: Option<usize>,
    /// Optional precision (decimal places)
    precision: Option<usize>,
    /// Fill character for width (default space, '0' for zero-pad)
    fill: char,
    /// Conversion: 'd' (decimal, default), 'x' (hex), 'X' (HEX), 'b' (binary), 'o' (octal)
    conversion: char,
}

/// Pre-parsed format string cached on the `Printf` node. The
/// `#[polydat_node]` macro invokes `parse_format` once at
/// construction; eval reads the segments directly with no
/// per-call parsing.
#[derive(Debug, Clone)]
pub struct ParsedFormat {
    segments: Vec<Segment>,
}

impl PolydatSetup for ParsedFormat {}

/// Printf-style N→1 formatting node. Variadic: accepts 0..N wire inputs.
///
/// Signature: `printf(format: String, in_0, in_1, ...) -> (String)`
///
/// Format string uses Rust-style `{}` placeholders with optional specifiers:
/// `{:05}` (zero-pad), `{:.2}` (precision), `{:x}` (hex), `{:X}` (HEX),
/// `{:b}` (binary), `{:o}` (octal). Inputs are matched positionally.
///
/// Use for constructing complex formatted strings from multiple Polydat wires:
/// `printf("user-{:05}-score-{:.1}", id, score)` → "user-00042-score-98.6"
///
/// All Value types are accepted at eval time regardless of declared port
/// types (the variadic slots advertise `PortType::Str` but the body
/// dispatches on `Value` variants). The format specifier determines how
/// each value renders.
///
/// SRD-73 follow-up: None propagation through string interpolation.
/// If any REFERENCED input is `Value::None`, the whole result is
/// `Value::None`. The body itself doesn't materialise this —
/// the Polydat kernel's SRD-74 Rule 1 guard (engines.rs) emits
/// `Value::None` on every output for any node whose inputs
/// include `Value::None` and which doesn't opt into
/// `accepts_none_inputs`. Printf doesn't opt in, so the kernel
/// guard fires before this body is invoked at production time.
///
/// Rationale: `Value::None` is the canonical "absent" sentinel.
/// The Polydat Kernel's `lookup` / `get_constant` already treat
/// None-valued outputs as "not present in this scope" and fall
/// through to the parent scope. String interpolation is the
/// surface where that discipline was being silently broken — an
/// unresolved `{X}` in a source-level string literal compiles
/// to a `printf` call with the unresolved name's slot, and when
/// that slot evaluates to None the printf result should likewise
/// be None so the binding doesn't shadow upstream defaults. The
/// canonical end-to-end coverage for this lives in
/// `tests/scope_composition.rs::const_with_unbound_interpolation_*`.
#[crate::polydat_node(category = Formatting)]
fn printf(
    format: Const<&str>,
    #[poly_const(ParsedFormat::from_format_str, from = format)]
    parsed: &ParsedFormat,
    parts: &[polydat::ast::Value],
) -> String {
    let mut result = String::new();
    for seg in &parsed.segments {
        match seg {
            Segment::Literal(s) => result.push_str(s),
            Segment::Placeholder(spec) => {
                let val = match parts.get(spec.index) {
                    Some(v) => v,
                    None => panic!(
                        "printf: format references input #{} but only {} wire input(s) supplied",
                        spec.index,
                        parts.len(),
                    ),
                };
                let formatted = format_value(val, spec);
                result.push_str(&formatted);
            }
        }
    }
    result
}

impl ParsedFormat {
    /// Setup-fn for `#[poly_const(...)]`: parse a format string
    /// into a list of segments. Called once at node construction;
    /// the resulting `ParsedFormat` is cached on the struct field
    /// and borrowed by every eval call.
    pub fn from_format_str(fmt: &str) -> Self {
        Self { segments: parse_format(fmt) }
    }
}

fn format_value(val: &Value, spec: &FormatSpec) -> String {
    match val {
        Value::U64(v) => format_u64(*v, spec),
        Value::F64(v) => format_f64(*v, spec),
        Value::Bool(v) => v.to_string(),
        Value::Str(v) => {
            if let Some(w) = spec.width {
                format!("{:>width$}", v, width = w)
            } else {
                v.to_string()
            }
        }
        _ => format!("{val:?}"),
    }
}

fn format_u64(v: u64, spec: &FormatSpec) -> String {
    let raw = match spec.conversion {
        'x' => format!("{v:x}"),
        'X' => format!("{v:X}"),
        'b' => format!("{v:b}"),
        'o' => format!("{v:o}"),
        _ => v.to_string(),
    };
    apply_width(&raw, spec)
}

fn format_f64(v: f64, spec: &FormatSpec) -> String {
    let raw = if let Some(prec) = spec.precision {
        format!("{v:.prec$}")
    } else {
        // Bare `{}` for f64 uses Debug formatting so whole-number
        // floats render as `1.0` instead of `1`, matching
        // `Value::F64::to_display_string`. Authors who want
        // integer-style output for whole floats specify a
        // precision (`{:.0}`) or convert via `format_u64`.
        format!("{v:?}")
    };
    apply_width(&raw, spec)
}

fn apply_width(s: &str, spec: &FormatSpec) -> String {
    if let Some(w) = spec.width {
        if s.len() < w {
            let pad = w - s.len();
            let fill = spec.fill;
            format!("{}{s}", std::iter::repeat_n(fill, pad).collect::<String>())
        } else {
            s.to_string()
        }
    } else {
        s.to_string()
    }
}

fn parse_format(fmt: &str) -> Vec<Segment> {
    let mut segments = Vec::new();
    let mut literal = String::new();
    let chars: Vec<char> = fmt.chars().collect();
    let mut i = 0;
    let mut placeholder_idx = 0;

    while i < chars.len() {
        if chars[i] == '{' && i + 1 < chars.len() && chars[i + 1] == '{' {
            literal.push('{');
            i += 2;
        } else if chars[i] == '{' {
            if !literal.is_empty() {
                segments.push(Segment::Literal(std::mem::take(&mut literal)));
            }
            // Find closing }
            let start = i + 1;
            while i < chars.len() && chars[i] != '}' {
                i += 1;
            }
            let spec_str: String = chars[start..i].iter().collect();
            let spec = parse_spec(&spec_str, placeholder_idx);
            segments.push(Segment::Placeholder(spec));
            placeholder_idx += 1;
            i += 1; // skip }
        } else if chars[i] == '}' && i + 1 < chars.len() && chars[i + 1] == '}' {
            literal.push('}');
            i += 2;
        } else {
            literal.push(chars[i]);
            i += 1;
        }
    }

    if !literal.is_empty() {
        segments.push(Segment::Literal(literal));
    }

    segments
}

fn parse_spec(spec: &str, index: usize) -> FormatSpec {
    let mut result = FormatSpec {
        index,
        width: None,
        precision: None,
        fill: ' ',
        conversion: 'd',
    };

    if spec.is_empty() {
        return result;
    }

    // Strip leading ':'
    let spec = spec.strip_prefix(':').unwrap_or(spec);
    if spec.is_empty() {
        return result;
    }

    let chars: Vec<char> = spec.chars().collect();
    let mut pos = 0;

    // Check for zero-fill
    if pos < chars.len() && chars[pos] == '0' && pos + 1 < chars.len() && chars[pos + 1].is_ascii_digit() {
        result.fill = '0';
        pos += 1;
    }

    // Width
    let width_start = pos;
    while pos < chars.len() && chars[pos].is_ascii_digit() {
        pos += 1;
    }
    if pos > width_start {
        let w: String = chars[width_start..pos].iter().collect();
        result.width = Some(w.parse().unwrap());
    }

    // Precision
    if pos < chars.len() && chars[pos] == '.' {
        pos += 1;
        let prec_start = pos;
        while pos < chars.len() && chars[pos].is_ascii_digit() {
            pos += 1;
        }
        if pos > prec_start {
            let p: String = chars[prec_start..pos].iter().collect();
            result.precision = Some(p.parse().unwrap());
        }
    }

    // Conversion
    if pos < chars.len() {
        result.conversion = chars[pos];
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::PolydatNode;

    #[test]
    fn printf_simple() {
        let node = Printf::new("hello {}".to_string(), 1);
        let mut out = [Value::None];
        node.eval(&[Value::U64(42)], &mut out);
        assert_eq!(out[0].as_str(), "hello 42");
    }

    #[test]
    fn printf_multiple() {
        let node = Printf::new("{} + {} = {}".to_string(), 3);
        let mut out = [Value::None];
        node.eval(&[Value::U64(1), Value::U64(2), Value::U64(3)], &mut out);
        assert_eq!(out[0].as_str(), "1 + 2 = 3");
    }

    #[test]
    fn printf_zero_pad() {
        let node = Printf::new("{:05}".to_string(), 1);
        let mut out = [Value::None];
        node.eval(&[Value::U64(42)], &mut out);
        assert_eq!(out[0].as_str(), "00042");
    }

    #[test]
    fn printf_hex() {
        let node = Printf::new("{:x}".to_string(), 1);
        let mut out = [Value::None];
        node.eval(&[Value::U64(255)], &mut out);
        assert_eq!(out[0].as_str(), "ff");
    }

    #[test]
    fn printf_hex_upper() {
        let node = Printf::new("{:X}".to_string(), 1);
        let mut out = [Value::None];
        node.eval(&[Value::U64(255)], &mut out);
        assert_eq!(out[0].as_str(), "FF");
    }

    #[test]
    fn printf_precision() {
        let node = Printf::new("{:.2}".to_string(), 1);
        let mut out = [Value::None];
        node.eval(&[Value::F64(3.14159)], &mut out);
        assert_eq!(out[0].as_str(), "3.14");
    }

    #[test]
    fn printf_mixed() {
        let node = Printf::new("id={:05} val={:.1}".to_string(), 2);
        let mut out = [Value::None];
        node.eval(&[Value::U64(7), Value::F64(98.6)], &mut out);
        assert_eq!(out[0].as_str(), "id=00007 val=98.6");
    }

    #[test]
    fn printf_literal_braces() {
        let node = Printf::new("{{escaped}} {}".to_string(), 1);
        let mut out = [Value::None];
        node.eval(&[Value::U64(1)], &mut out);
        assert_eq!(out[0].as_str(), "{escaped} 1");
    }

    #[test]
    fn printf_no_placeholders() {
        let node = Printf::new("just text".to_string(), 0);
        let mut out = [Value::None];
        node.eval(&[], &mut out);
        assert_eq!(out[0].as_str(), "just text");
    }

    #[test]
    fn printf_string_input() {
        let node = Printf::new("hello {}".to_string(), 1);
        let mut out = [Value::None];
        node.eval(&[Value::Str("world".into())], &mut out);
        assert_eq!(out[0].as_str(), "hello world");
    }

    // ────────────────────────────────────────────────────────
    // None propagation (SRD-73 follow-up)
    //
    // String interpolation evaluates to Value::None when any
    // referenced input is Value::None. Pre-migration, the body
    // implemented this check directly. Post-migration (SRD-80b
    // Phase E), the canonical None-propagation surface is the
    // Polydat kernel's SRD-74 Rule 1 guard (engines.rs): any
    // node whose inputs include Value::None and which doesn't
    // override `accepts_none_inputs` emits None on every output
    // BEFORE the body is invoked. The body therefore never
    // observes a None-tainted `parts` slice at production time.
    //
    // End-to-end coverage of the kernel-level None-propagation
    // through printf lives in `tests/scope_composition.rs`
    // (`const_with_unbound_interpolation_falls_through_to_outer`).
    // The direct-eval unit tests that previously asserted the
    // redundant body-side check are intentionally retired —
    // they tested defense-in-depth at a layer the macro
    // migration removed.
    // ────────────────────────────────────────────────────────

    #[test]
    fn printf_all_present_unchanged() {
        // Sanity: a multi-arg format with no None inputs. This
        // is the regression guard for the overwhelming common
        // case the body actually handles.
        let node = Printf::new("a={} b={}".to_string(), 2);
        let mut out = [Value::None];
        node.eval(&[Value::U64(1), Value::U64(2)], &mut out);
        assert_eq!(out[0].as_str(), "a=1 b=2");
    }

    #[test]
    fn printf_no_placeholders_still_renders() {
        // Edge: a format with no placeholders. The result is
        // the literal string.
        let node = Printf::new("static text".to_string(), 0);
        let mut out = [Value::None];
        node.eval(&[], &mut out);
        assert_eq!(out[0].as_str(), "static text");
    }
}
