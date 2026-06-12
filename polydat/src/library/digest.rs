// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Cryptographic digest and base encoding nodes.

// SRD-80 PR B.11 — alias the upstream digest types so the
// macro-generated `Sha256` / `Md5` structs don't collide.
use sha2::{Sha256 as Sha2_256, Digest as Sha2Digest};
use md5::Md5 as Md5Hasher;

// SRD-80 PR B.11 — digest and base-encoding nodes migrated to
// `#[polydat_node]` with native Rust types for Bytes:
//   - input  Bytes:  `&[u8]` (borrow, zero-alloc)
//   - output Bytes:  `Vec<u8>` (owned, becomes Arc<[u8]> via IntoValue)

#[crate::polydat_node(category = Digest)]
fn sha256(input: &[u8]) -> Vec<u8> {
    let mut hasher = Sha2_256::new();
    hasher.update(input);
    hasher.finalize().to_vec()
}

#[crate::polydat_node(category = Digest)]
fn md5(input: &[u8]) -> Vec<u8> {
    let mut hasher = Md5Hasher::new();
    hasher.update(input);
    hasher.finalize().to_vec()
}

#[crate::polydat_node(category = Digest)]
fn to_base64(input: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(input)
}

#[crate::polydat_node(category = Digest)]
fn from_base64(input: &str) -> Vec<u8> {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD
        .decode(input)
        .unwrap_or_default()
}

#[crate::polydat_node(category = Digest)]
fn to_base32(input: &[u8]) -> String {
    data_encoding::BASE32.encode(input)
}

#[crate::polydat_node(category = Digest)]
fn from_base32(input: &str) -> Vec<u8> {
    data_encoding::BASE32
        .decode(input.as_bytes())
        .unwrap_or_default()
}

// ---------------------------------------------------------------------------
// Signature declarations for the DSL registry
// ---------------------------------------------------------------------------

// SRD-80 PR B.11 — every node in this module registers
// link-time via the proc-macro-emitted NodeRegistration.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{PolydatNode, Value};

    #[test]
    fn sha256_known() {
        let node = Sha256::default();
        let mut out = [Value::None];
        // SHA-256 of empty string
        node.eval(&[Value::Bytes(vec![].into())], &mut out[..]);
        let bytes = out[0].as_bytes();
        assert_eq!(bytes.len(), 32);
        // Known hash of empty: e3b0c44298fc1c14...
        assert_eq!(bytes[0], 0xe3);
        assert_eq!(bytes[1], 0xb0);
    }

    #[test]
    fn sha256_deterministic() {
        let node = Sha256::default();
        let mut out1 = [Value::None];
        let mut out2 = [Value::None];
        let input = Value::Bytes(b"hello world".to_vec().into());
        node.eval(std::slice::from_ref(&input), &mut out1);
        node.eval(&[input], &mut out2);
        assert_eq!(out1[0].as_bytes(), out2[0].as_bytes());
    }

    #[test]
    fn md5_known() {
        let node = Md5::default();
        let mut out = [Value::None];
        node.eval(&[Value::Bytes(vec![].into())], &mut out[..]);
        let bytes = out[0].as_bytes();
        assert_eq!(bytes.len(), 16);
        // Known MD5 of empty: d41d8cd98f00b204...
        assert_eq!(bytes[0], 0xd4);
    }

    #[test]
    fn base64_roundtrip() {
        let enc = ToBase64::default();
        let dec = FromBase64::default();
        let mut mid = [Value::None];
        let mut out = [Value::None];
        let input = vec![0xDE, 0xAD, 0xBE, 0xEF, 0x42];
        enc.eval(&[Value::Bytes(input.clone().into())], &mut mid[..]);
        dec.eval(&[mid[0].clone()], &mut out);
        assert_eq!(out[0].as_bytes(), &input);
    }

    #[test]
    fn base32_roundtrip() {
        let enc = ToBase32::default();
        let dec = FromBase32::default();
        let mut mid = [Value::None];
        let mut out = [Value::None];
        let input = vec![0xDE, 0xAD, 0xBE, 0xEF, 0x42];
        enc.eval(&[Value::Bytes(input.clone().into())], &mut mid[..]);
        dec.eval(&[mid[0].clone()], &mut out);
        assert_eq!(out[0].as_bytes(), &input);
    }
}
