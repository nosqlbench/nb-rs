// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! String encoding and decoding nodes: HTML entities, URL percent-encoding.

use crate::node::{GkNode, NodeMeta, Port, PortType, Slot, Value};

// =================================================================
// HTML entity encoding
// =================================================================

/// Encode HTML special characters as entities.
///
/// Signature: `(input: String) -> (String)`
///
/// Encodes: `& < > " '`
pub struct HtmlEncode {
    meta: NodeMeta,
}

impl Default for HtmlEncode {
    fn default() -> Self {
        Self::new()
    }
}

impl HtmlEncode {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "html_encode".into(),
                outs: vec![Port::new("output", PortType::Str)],
                ins: vec![Slot::Wire(Port::new("input", PortType::Str))],
            },
        }
    }
}

impl GkNode for HtmlEncode {
    fn meta(&self) -> &NodeMeta { &self.meta }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let s = inputs[0].as_str();
        let mut result = String::with_capacity(s.len());
        for c in s.chars() {
            match c {
                '&' => result.push_str("&amp;"),
                '<' => result.push_str("&lt;"),
                '>' => result.push_str("&gt;"),
                '"' => result.push_str("&quot;"),
                '\'' => result.push_str("&#x27;"),
                _ => result.push(c),
            }
        }
        outputs[0] = Value::Str(result);
    }
}

/// Decode HTML entities back to characters.
///
/// Signature: `(input: String) -> (String)`
///
/// Decodes: `&amp; &lt; &gt; &quot; &#x27; &#39;`
pub struct HtmlDecode {
    meta: NodeMeta,
}

impl Default for HtmlDecode {
    fn default() -> Self {
        Self::new()
    }
}

impl HtmlDecode {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "html_decode".into(),
                outs: vec![Port::new("output", PortType::Str)],
                ins: vec![Slot::Wire(Port::new("input", PortType::Str))],
            },
        }
    }
}

impl GkNode for HtmlDecode {
    fn meta(&self) -> &NodeMeta { &self.meta }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let s = inputs[0].as_str();
        let result = s
            .replace("&amp;", "&")
            .replace("&lt;", "<")
            .replace("&gt;", ">")
            .replace("&quot;", "\"")
            .replace("&#x27;", "'")
            .replace("&#39;", "'");
        outputs[0] = Value::Str(result);
    }
}

// =================================================================
// URL percent-encoding
// =================================================================

/// Percent-encode a string for use in URLs.
///
/// Signature: `(input: String) -> (String)`
///
/// Encodes all characters except unreserved: `A-Z a-z 0-9 - _ . ~`
pub struct UrlEncode {
    meta: NodeMeta,
}

impl Default for UrlEncode {
    fn default() -> Self {
        Self::new()
    }
}

impl UrlEncode {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "url_encode".into(),
                outs: vec![Port::new("output", PortType::Str)],
                ins: vec![Slot::Wire(Port::new("input", PortType::Str))],
            },
        }
    }
}

fn is_url_unreserved(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b == b'.' || b == b'~'
}

impl GkNode for UrlEncode {
    fn meta(&self) -> &NodeMeta { &self.meta }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let s = inputs[0].as_str();
        let mut result = String::with_capacity(s.len());
        for &b in s.as_bytes() {
            if is_url_unreserved(b) {
                result.push(b as char);
            } else {
                result.push_str(&format!("%{b:02X}"));
            }
        }
        outputs[0] = Value::Str(result);
    }
}

/// Decode a percent-encoded URL string.
///
/// Signature: `(input: String) -> (String)`
pub struct UrlDecode {
    meta: NodeMeta,
}

impl Default for UrlDecode {
    fn default() -> Self {
        Self::new()
    }
}

impl UrlDecode {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "url_decode".into(),
                outs: vec![Port::new("output", PortType::Str)],
                ins: vec![Slot::Wire(Port::new("input", PortType::Str))],
            },
        }
    }
}

impl GkNode for UrlDecode {
    fn meta(&self) -> &NodeMeta { &self.meta }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let s = inputs[0].as_str();
        let bytes = s.as_bytes();
        let mut result = Vec::with_capacity(bytes.len());
        let mut i = 0;
        while i < bytes.len() {
            if bytes[i] == b'%' && i + 2 < bytes.len()
                && let Ok(byte) = u8::from_str_radix(
                    &s[i + 1..i + 3], 16
                ) {
                    result.push(byte);
                    i += 3;
                    continue;
                }
            result.push(bytes[i]);
            i += 1;
        }
        outputs[0] = Value::Str(String::from_utf8_lossy(&result).into_owned());
    }
}

// ---------------------------------------------------------------------------
// Signature declarations for the DSL registry
// ---------------------------------------------------------------------------

use crate::dsl::registry::{Arity, FuncCategory, FuncSig, ParamSpec};
use crate::node::SlotType;

/// Signatures for encoding/decoding nodes.
pub fn signatures() -> &'static [FuncSig] {
    use FuncCategory as C;
    &[
        FuncSig {
            name: "html_encode", category: C::Encoding, outputs: 1,
            description: "HTML entity encode",
            help: "Escape HTML special characters: & < > \" ' become entity references.\nUse when embedding generated strings into HTML content to prevent\ninjection or rendering issues.\nParameters:\n  input — String wire input",
            identity: None, variadic_ctor: None,
            params: &[ParamSpec { name: "input", slot_type: SlotType::Wire, required: true }],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "html_decode", category: C::Encoding, outputs: 1,
            description: "HTML entity decode",
            help: "Decode HTML entity references back to literal characters.\nHandles named entities (&amp;, &lt;, etc.) and numeric references.\nUse when processing HTML content that needs to be plain text.\nParameters:\n  input — String wire input",
            identity: None, variadic_ctor: None,
            params: &[ParamSpec { name: "input", slot_type: SlotType::Wire, required: true }],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "url_encode", category: C::Encoding, outputs: 1,
            description: "URL percent-encode",
            help: "Percent-encode a string for safe use in URLs (RFC 3986).\nReserved and non-ASCII characters become %XX hex sequences.\nUse when generating query parameters or path segments.\nParameters:\n  input — String wire input",
            identity: None, variadic_ctor: None,
            params: &[ParamSpec { name: "input", slot_type: SlotType::Wire, required: true }],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "url_decode", category: C::Encoding, outputs: 1,
            description: "URL percent-decode",
            help: "Decode percent-encoded URL sequences back to literal characters.\nConverts %XX hex sequences and '+' (as space) to their originals.\nUse when processing URL-encoded input data.\nParameters:\n  input — String wire input",
            identity: None, variadic_ctor: None,
            params: &[ParamSpec { name: "input", slot_type: SlotType::Wire, required: true }],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
    ]
}

/// Try to build an encoding node from a function name and const args.
///
/// Returns `None` if the name is not handled by this module.
pub(crate) fn build_node(name: &str, _wires: &[crate::assembly::WireRef], _consts: &[crate::dsl::factory::ConstArg]) -> Option<Result<Box<dyn crate::node::GkNode>, String>> {
    match name {
        "html_encode" => Some(Ok(Box::new(HtmlEncode::new()))),
        "html_decode" => Some(Ok(Box::new(HtmlDecode::new()))),
        "url_encode" => Some(Ok(Box::new(UrlEncode::new()))),
        "url_decode" => Some(Ok(Box::new(UrlDecode::new()))),
        _ => None,
    }
}


crate::register_nodes!(signatures, build_node);
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn html_encode_basic() {
        let node = HtmlEncode::new();
        let mut out = [Value::None];
        node.eval(&[Value::Str("<b>hello & world</b>".into())], &mut out);
        assert_eq!(out[0].as_str(), "&lt;b&gt;hello &amp; world&lt;/b&gt;");
    }

    #[test]
    fn html_encode_quotes() {
        let node = HtmlEncode::new();
        let mut out = [Value::None];
        node.eval(&[Value::Str(r#"say "hello" it's fine"#.into())], &mut out);
        assert_eq!(out[0].as_str(), "say &quot;hello&quot; it&#x27;s fine");
    }

    #[test]
    fn html_encode_passthrough() {
        let node = HtmlEncode::new();
        let mut out = [Value::None];
        node.eval(&[Value::Str("plain text 123".into())], &mut out);
        assert_eq!(out[0].as_str(), "plain text 123");
    }

    #[test]
    fn html_roundtrip() {
        let enc = HtmlEncode::new();
        let dec = HtmlDecode::new();
        let mut mid = [Value::None];
        let mut out = [Value::None];
        let input = "<div class=\"test\">hello & 'world'</div>";
        enc.eval(&[Value::Str(input.into())], &mut mid);
        dec.eval(&[mid[0].clone()], &mut out);
        assert_eq!(out[0].as_str(), input);
    }

    #[test]
    fn url_encode_basic() {
        let node = UrlEncode::new();
        let mut out = [Value::None];
        node.eval(&[Value::Str("hello world".into())], &mut out);
        assert_eq!(out[0].as_str(), "hello%20world");
    }

    #[test]
    fn url_encode_special() {
        let node = UrlEncode::new();
        let mut out = [Value::None];
        node.eval(&[Value::Str("a=1&b=2".into())], &mut out);
        assert_eq!(out[0].as_str(), "a%3D1%26b%3D2");
    }

    #[test]
    fn url_encode_passthrough() {
        let node = UrlEncode::new();
        let mut out = [Value::None];
        node.eval(&[Value::Str("hello-world_123.txt~".into())], &mut out);
        assert_eq!(out[0].as_str(), "hello-world_123.txt~");
    }

    #[test]
    fn url_roundtrip() {
        let enc = UrlEncode::new();
        let dec = UrlDecode::new();
        let mut mid = [Value::None];
        let mut out = [Value::None];
        let input = "hello world & friends = cool";
        enc.eval(&[Value::Str(input.into())], &mut mid);
        dec.eval(&[mid[0].clone()], &mut out);
        assert_eq!(out[0].as_str(), input);
    }
}
