// Copyright 2024-2026 nosqlbench contributors
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

use crate::node::{GkNode, NodeMeta, Port, PortType, Slot, Value};

/// A parsed format segment: either literal text or a placeholder.
#[derive(Debug, Clone)]
enum Segment {
    Literal(String),
    Placeholder(FormatSpec),
}

#[derive(Debug, Clone)]
struct FormatSpec {
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

/// Printf-style N→1 formatting node. Variadic: accepts 0..N wire inputs.
///
/// Signature: `printf(format: String, in_0, in_1, ...) -> (String)`
///
/// Format string uses Rust-style `{}` placeholders with optional specifiers:
/// `{:05}` (zero-pad), `{:.2}` (precision), `{:x}` (hex), `{:X}` (HEX),
/// `{:b}` (binary), `{:o}` (octal). Inputs are matched positionally.
///
/// Use for constructing complex formatted strings from multiple GK wires:
/// `printf("user-{:05}-score-{:.1}", id, score)` → "user-00042-score-98.6"
///
/// All Value types are accepted at eval time regardless of declared port
/// types. The format specifier determines how each value renders.
///
/// JIT level: P1 (String output).
pub struct Printf {
    meta: NodeMeta,
    segments: Vec<Segment>,
}

impl Printf {
    /// Create from a format string with explicit input port types.
    pub fn new(fmt: &str, input_types: &[PortType]) -> Self {
        let segments = parse_format(fmt);
        let placeholder_count = segments.iter().filter(|s| matches!(s, Segment::Placeholder(_))).count();
        assert_eq!(
            placeholder_count, input_types.len(),
            "format string has {placeholder_count} placeholders but {n} input types provided",
            n = input_types.len()
        );

        let inputs: Vec<Port> = input_types
            .iter()
            .enumerate()
            .map(|(i, &typ)| Port::new(format!("in_{i}"), typ))
            .collect();
        let slots: Vec<Slot> = inputs.iter().map(|p| Slot::Wire(p.clone())).collect();

        Self {
            meta: NodeMeta {
                name: "printf".into(),
                outs: vec![Port::new("output", PortType::Str)],
                ins: slots,
            },
            segments,
        }
    }

    /// Create from a format string with N wire inputs, all typed as u64.
    ///
    /// For variadic DSL use. Port types are declared as u64 but the eval
    /// method accepts any Value type — the assembler skips type checking
    /// for printf inputs.
    pub fn variadic(fmt: &str, wire_count: usize) -> Self {
        let segments = parse_format(fmt);
        let inputs: Vec<Port> = (0..wire_count)
            .map(|i| Port::new(format!("in_{i}"), PortType::U64))
            .collect();
        let slots: Vec<Slot> = inputs.iter().map(|p| Slot::Wire(p.clone())).collect();
        Self {
            meta: NodeMeta {
                name: "printf".into(),
                outs: vec![Port::new("output", PortType::Str)],
                ins: slots,
            },
            segments,
        }
    }
}

impl GkNode for Printf {
    fn meta(&self) -> &NodeMeta {
        &self.meta
    }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let mut result = String::new();
        for seg in &self.segments {
            match seg {
                Segment::Literal(s) => result.push_str(s),
                Segment::Placeholder(spec) => {
                    let val = &inputs[spec.index];
                    let formatted = format_value(val, spec);
                    result.push_str(&formatted);
                }
            }
        }
        outputs[0] = Value::Str(result);
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
                v.clone()
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
        v.to_string()
    };
    apply_width(&raw, spec)
}

fn apply_width(s: &str, spec: &FormatSpec) -> String {
    if let Some(w) = spec.width {
        if s.len() < w {
            let pad = w - s.len();
            let fill = spec.fill;
            format!("{}{s}", std::iter::repeat(fill).take(pad).collect::<String>())
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

    #[test]
    fn printf_simple() {
        let node = Printf::new("hello {}", &[PortType::U64]);
        let mut out = [Value::None];
        node.eval(&[Value::U64(42)], &mut out);
        assert_eq!(out[0].as_str(), "hello 42");
    }

    #[test]
    fn printf_multiple() {
        let node = Printf::new("{} + {} = {}", &[PortType::U64, PortType::U64, PortType::U64]);
        let mut out = [Value::None];
        node.eval(&[Value::U64(1), Value::U64(2), Value::U64(3)], &mut out);
        assert_eq!(out[0].as_str(), "1 + 2 = 3");
    }

    #[test]
    fn printf_zero_pad() {
        let node = Printf::new("{:05}", &[PortType::U64]);
        let mut out = [Value::None];
        node.eval(&[Value::U64(42)], &mut out);
        assert_eq!(out[0].as_str(), "00042");
    }

    #[test]
    fn printf_hex() {
        let node = Printf::new("{:x}", &[PortType::U64]);
        let mut out = [Value::None];
        node.eval(&[Value::U64(255)], &mut out);
        assert_eq!(out[0].as_str(), "ff");
    }

    #[test]
    fn printf_hex_upper() {
        let node = Printf::new("{:X}", &[PortType::U64]);
        let mut out = [Value::None];
        node.eval(&[Value::U64(255)], &mut out);
        assert_eq!(out[0].as_str(), "FF");
    }

    #[test]
    fn printf_precision() {
        let node = Printf::new("{:.2}", &[PortType::F64]);
        let mut out = [Value::None];
        node.eval(&[Value::F64(3.14159)], &mut out);
        assert_eq!(out[0].as_str(), "3.14");
    }

    #[test]
    fn printf_mixed() {
        let node = Printf::new("id={:05} val={:.1}", &[PortType::U64, PortType::F64]);
        let mut out = [Value::None];
        node.eval(&[Value::U64(7), Value::F64(98.6)], &mut out);
        assert_eq!(out[0].as_str(), "id=00007 val=98.6");
    }

    #[test]
    fn printf_literal_braces() {
        let node = Printf::new("{{escaped}} {}", &[PortType::U64]);
        let mut out = [Value::None];
        node.eval(&[Value::U64(1)], &mut out);
        assert_eq!(out[0].as_str(), "{escaped} 1");
    }

    #[test]
    fn printf_no_placeholders() {
        let node = Printf::new("just text", &[]);
        let mut out = [Value::None];
        node.eval(&[], &mut out);
        assert_eq!(out[0].as_str(), "just text");
    }

    #[test]
    fn printf_string_input() {
        let node = Printf::new("hello {}", &[PortType::Str]);
        let mut out = [Value::None];
        node.eval(&[Value::Str("world".into())], &mut out);
        assert_eq!(out[0].as_str(), "hello world");
    }
}
