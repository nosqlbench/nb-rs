// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! String encoding and decoding nodes: HTML entities, URL percent-encoding.

#[cfg(test)]
use crate::ast::Value;

// =================================================================
// HTML entity encoding
// =================================================================

// SRD-80 PR B.4 — encoding/decoding nodes migrated to
// `#[polydat_node]`. The struct names HtmlEncode / HtmlDecode /
// UrlEncode / UrlDecode are emitted by the macro's
// snake_case → PascalCase rule, matching the existing names
// the tests below reference.

/// Encode HTML special characters as entities (`& < > " '`).
#[crate::polydat_node(category = Encoding)]
fn html_encode(input: String) -> String {
    let mut result = String::with_capacity(input.len());
    for c in input.chars() {
        match c {
            '&' => result.push_str("&amp;"),
            '<' => result.push_str("&lt;"),
            '>' => result.push_str("&gt;"),
            '"' => result.push_str("&quot;"),
            '\'' => result.push_str("&#x27;"),
            _ => result.push(c),
        }
    }
    result
}

/// Decode HTML entities back to characters.
#[crate::polydat_node(category = Encoding)]
fn html_decode(input: String) -> String {
    input
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&#x27;", "'")
        .replace("&#39;", "'")
}

fn is_url_unreserved(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b == b'.' || b == b'~'
}

/// Percent-encode a string for use in URLs (RFC 3986).
#[crate::polydat_node(category = Encoding)]
fn url_encode(input: String) -> String {
    let mut result = String::with_capacity(input.len());
    for &b in input.as_bytes() {
        if is_url_unreserved(b) {
            result.push(b as char);
        } else {
            result.push_str(&format!("%{b:02X}"));
        }
    }
    result
}

/// Decode a percent-encoded URL string.
#[crate::polydat_node(category = Encoding)]
fn url_decode(input: String) -> String {
    let bytes = input.as_bytes();
    let mut result = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len()
            && let Ok(byte) = u8::from_str_radix(
                &input[i + 1..i + 3], 16
            ) {
                result.push(byte);
                i += 3;
                continue;
            }
        result.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&result).into_owned()
}

// ---------------------------------------------------------------------------
// Signature declarations for the DSL registry
// ---------------------------------------------------------------------------

// SRD-80 PR B.4: FuncSig / ParamSpec / SlotType / Arity /
// FuncCategory imports retired with the manual signatures()
// function — the proc-macro emits the equivalent types
// internally via its `polydat::dsl::registry::...` paths.

// SRD-80 PR B.4 — every node in this module is registered
// link-time via `#[polydat_node]`'s NodeRegistration emission.
// No `register_nodes!` call needed; no manual signatures()/
// build_node() to maintain.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::PolydatNode;

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
