// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! Cryptographic digest and base encoding nodes.

use crate::node::{GkNode, NodeMeta, Port, PortType, Slot, Value};
use sha2::{Sha256, Digest as Sha2Digest};
use md5::Md5;

/// SHA-256 digest of a byte buffer.
///
/// Signature: `(input: bytes) -> (bytes)`
/// Output is always 32 bytes.
pub struct DigestSha256 {
    meta: NodeMeta,
}

impl Default for DigestSha256 {
    fn default() -> Self {
        Self::new()
    }
}

impl DigestSha256 {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "sha256".into(),
                outs: vec![Port::new("output", PortType::Bytes)],
                ins: vec![Slot::Wire(Port::new("input", PortType::Bytes))],
            },
        }
    }
}

impl GkNode for DigestSha256 {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let mut hasher = Sha256::new();
        hasher.update(inputs[0].as_bytes());
        outputs[0] = Value::Bytes(hasher.finalize().to_vec());
    }
}

/// MD5 digest of a byte buffer.
///
/// Signature: `(input: bytes) -> (bytes)`
/// Output is always 16 bytes.
pub struct DigestMd5 {
    meta: NodeMeta,
}

impl Default for DigestMd5 {
    fn default() -> Self {
        Self::new()
    }
}

impl DigestMd5 {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "md5".into(),
                outs: vec![Port::new("output", PortType::Bytes)],
                ins: vec![Slot::Wire(Port::new("input", PortType::Bytes))],
            },
        }
    }
}

impl GkNode for DigestMd5 {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let mut hasher = Md5::new();
        hasher.update(inputs[0].as_bytes());
        outputs[0] = Value::Bytes(hasher.finalize().to_vec());
    }
}

/// Base64 encode bytes to string.
///
/// Signature: `(input: bytes) -> (String)`
pub struct ToBase64 {
    meta: NodeMeta,
}

impl Default for ToBase64 {
    fn default() -> Self {
        Self::new()
    }
}

impl ToBase64 {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "to_base64".into(),
                outs: vec![Port::new("output", PortType::Str)],
                ins: vec![Slot::Wire(Port::new("input", PortType::Bytes))],
            },
        }
    }
}

impl GkNode for ToBase64 {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        use base64::Engine;
        outputs[0] = Value::Str(base64::engine::general_purpose::STANDARD.encode(inputs[0].as_bytes()));
    }
}

/// Base64 decode string to bytes.
///
/// Signature: `(input: String) -> (bytes)`
pub struct FromBase64 {
    meta: NodeMeta,
}

impl Default for FromBase64 {
    fn default() -> Self {
        Self::new()
    }
}

impl FromBase64 {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "from_base64".into(),
                outs: vec![Port::new("output", PortType::Bytes)],
                ins: vec![Slot::Wire(Port::new("input", PortType::Str))],
            },
        }
    }
}

impl GkNode for FromBase64 {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        use base64::Engine;
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(inputs[0].as_str())
            .unwrap_or_default();
        outputs[0] = Value::Bytes(bytes);
    }
}

/// Base32 encode bytes to string.
///
/// Signature: `(input: bytes) -> (String)`
pub struct ToBase32 {
    meta: NodeMeta,
}

impl Default for ToBase32 {
    fn default() -> Self {
        Self::new()
    }
}

impl ToBase32 {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "to_base32".into(),
                outs: vec![Port::new("output", PortType::Str)],
                ins: vec![Slot::Wire(Port::new("input", PortType::Bytes))],
            },
        }
    }
}

impl GkNode for ToBase32 {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::Str(data_encoding::BASE32.encode(inputs[0].as_bytes()));
    }
}

/// Base32 decode string to bytes.
///
/// Signature: `(input: String) -> (bytes)`
pub struct FromBase32 {
    meta: NodeMeta,
}

impl Default for FromBase32 {
    fn default() -> Self {
        Self::new()
    }
}

impl FromBase32 {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "from_base32".into(),
                outs: vec![Port::new("output", PortType::Bytes)],
                ins: vec![Slot::Wire(Port::new("input", PortType::Str))],
            },
        }
    }
}

impl GkNode for FromBase32 {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let bytes = data_encoding::BASE32
            .decode(inputs[0].as_str().as_bytes())
            .unwrap_or_default();
        outputs[0] = Value::Bytes(bytes);
    }
}

// ---------------------------------------------------------------------------
// Signature declarations for the DSL registry
// ---------------------------------------------------------------------------

use crate::dsl::registry::{Arity, FuncCategory, FuncSig, ParamSpec};
use crate::node::SlotType;

/// Signatures for digest and encoding nodes.
pub fn signatures() -> &'static [FuncSig] {
    use FuncCategory as C;
    &[
        FuncSig {
            name: "sha256", category: C::Digest,
            outputs: 1, description: "SHA-256 digest",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Compute the SHA-256 cryptographic digest of a byte buffer.\nOutput is always 32 bytes. Use with to_hex or to_base64 for string output.\nParameters:\n  input — bytes wire input\nExample: sha256(bytes_from_hash(cycle, 64)) -> to_hex(...)",
        },
        FuncSig {
            name: "md5", category: C::Digest,
            outputs: 1, description: "MD5 digest",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Compute the MD5 digest of a byte buffer.\nOutput is always 16 bytes. Not cryptographically secure — use for\nchecksums, deduplication keys, or legacy compatibility only.\nParameters:\n  input — bytes wire input\nExample: md5(u64_to_bytes(hash(cycle))) -> to_hex(...)",
        },
        FuncSig {
            name: "to_base64", category: C::Digest, outputs: 1,
            description: "base64 encode",
            help: "Encode a byte buffer as a standard base64 string (RFC 4648).\nUse after digest functions for compact, printable output.\nExample: sha256(...) -> to_base64(...)\nParameters:\n  input — Bytes wire input",
            identity: None, variadic_ctor: None,
            params: &[ParamSpec { name: "input", slot_type: SlotType::Wire, required: true }],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "from_base64", category: C::Digest, outputs: 1,
            description: "base64 decode",
            help: "Decode a standard base64 string back to a byte buffer.\nAccepts standard base64 (RFC 4648) with optional padding.\nUse when processing base64-encoded input data.\nParameters:\n  input — String wire input (base64-encoded)",
            identity: None, variadic_ctor: None,
            params: &[ParamSpec { name: "input", slot_type: SlotType::Wire, required: true }],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
    ]
}

/// Try to build a digest or base64 node from a function name and const args.
///
/// Returns `None` if the name is not handled by this module.
pub(crate) fn build_node(name: &str, _wires: &[crate::assembly::WireRef], _consts: &[crate::dsl::factory::ConstArg]) -> Option<Result<Box<dyn crate::node::GkNode>, String>> {
    match name {
        "sha256" => Some(Ok(Box::new(DigestSha256::new()))),
        "md5" => Some(Ok(Box::new(DigestMd5::new()))),
        "to_base64" => Some(Ok(Box::new(ToBase64::new()))),
        "from_base64" => Some(Ok(Box::new(FromBase64::new()))),
        _ => None,
    }
}


crate::register_nodes!(signatures, build_node);
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sha256_known() {
        let node = DigestSha256::new();
        let mut out = [Value::None];
        // SHA-256 of empty string
        node.eval(&[Value::Bytes(vec![])], &mut out);
        let bytes = out[0].as_bytes();
        assert_eq!(bytes.len(), 32);
        // Known hash of empty: e3b0c44298fc1c14...
        assert_eq!(bytes[0], 0xe3);
        assert_eq!(bytes[1], 0xb0);
    }

    #[test]
    fn sha256_deterministic() {
        let node = DigestSha256::new();
        let mut out1 = [Value::None];
        let mut out2 = [Value::None];
        let input = Value::Bytes(b"hello world".to_vec());
        node.eval(&[input.clone()], &mut out1);
        node.eval(&[input], &mut out2);
        assert_eq!(out1[0].as_bytes(), out2[0].as_bytes());
    }

    #[test]
    fn md5_known() {
        let node = DigestMd5::new();
        let mut out = [Value::None];
        node.eval(&[Value::Bytes(vec![])], &mut out);
        let bytes = out[0].as_bytes();
        assert_eq!(bytes.len(), 16);
        // Known MD5 of empty: d41d8cd98f00b204...
        assert_eq!(bytes[0], 0xd4);
    }

    #[test]
    fn base64_roundtrip() {
        let enc = ToBase64::new();
        let dec = FromBase64::new();
        let mut mid = [Value::None];
        let mut out = [Value::None];
        let input = vec![0xDE, 0xAD, 0xBE, 0xEF, 0x42];
        enc.eval(&[Value::Bytes(input.clone())], &mut mid);
        dec.eval(&[mid[0].clone()], &mut out);
        assert_eq!(out[0].as_bytes(), &input);
    }

    #[test]
    fn base32_roundtrip() {
        let enc = ToBase32::new();
        let dec = FromBase32::new();
        let mut mid = [Value::None];
        let mut out = [Value::None];
        let input = vec![0xDE, 0xAD, 0xBE, 0xEF, 0x42];
        enc.eval(&[Value::Bytes(input.clone())], &mut mid);
        dec.eval(&[mid[0].clone()], &mut out);
        assert_eq!(out[0].as_bytes(), &input);
    }
}
