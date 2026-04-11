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
