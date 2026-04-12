// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Byte buffer and character buffer nodes.
//!
//! Two composition patterns from nosqlbench:
//!
//! 1. **Direct hash fill**: generate N bytes from a seed by chaining
//!    hashes. Fresh per cycle. Simple but slower for large buffers.
//!
//! 2. **Image extraction**: pre-fill a large static buffer at init
//!    time, then extract variable-length slices at cycle time using
//!    hash-based offset selection. Fast hot path — just a memcpy.

use crate::node::{GkNode, NodeMeta, Port, PortType, Slot, Value};
use xxhash_rust::xxh3::xxh3_64;

// =================================================================
// Direct byte generation
// =================================================================

/// Convert a u64 to 8 bytes (little-endian).
///
/// Signature: `(input: u64) -> (bytes)`
pub struct U64ToBytes {
    meta: NodeMeta,
}

impl Default for U64ToBytes {
    fn default() -> Self {
        Self::new()
    }
}

impl U64ToBytes {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "u64_to_bytes".into(),
                outs: vec![Port::new("output", PortType::Bytes)],
                ins: vec![Slot::Wire(Port::u64("input"))],
            },
        }
    }
}

impl GkNode for U64ToBytes {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::Bytes(inputs[0].as_u64().to_le_bytes().to_vec());
    }
}

/// Generate N deterministic bytes from a u64 seed via chained hashing.
///
/// Signature: `(input: u64) -> (bytes)`
/// Param: `size: usize`
///
/// Each 8-byte chunk is `hash(seed + chunk_index)`. The buffer is
/// fresh per cycle — no image caching.
pub struct BytesFromHash {
    meta: NodeMeta,
    size: usize,
}

impl BytesFromHash {
    pub fn new(size: usize) -> Self {
        Self {
            meta: NodeMeta {
                name: "bytes_from_hash".into(),
                outs: vec![Port::new("output", PortType::Bytes)],
                ins: vec![Slot::Wire(Port::u64("input"))],
            },
            size,
        }
    }
}

impl GkNode for BytesFromHash {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let seed = inputs[0].as_u64();
        let mut result = Vec::with_capacity(self.size);
        let chunks = self.size.div_ceil(8);
        for i in 0..chunks {
            let h = xxh3_64(&(seed.wrapping_add(i as u64)).to_le_bytes());
            let take = (self.size - result.len()).min(8);
            result.extend_from_slice(&h.to_le_bytes()[..take]);
        }
        outputs[0] = Value::Bytes(result);
    }
}

// =================================================================
// Image-based extraction (init-time buffer, cycle-time slice)
// =================================================================

/// A pre-filled byte image for fast cycle-time extraction.
///
/// Built at init time by hash-filling a large buffer. At cycle time,
/// a hash-based offset selects where to extract a variable-length
/// slice. The extraction is just a memcpy — no per-byte computation.
pub struct ByteImage {
    image: Vec<u8>,
}

impl ByteImage {
    /// Build a byte image of `image_size` bytes from a seed.
    pub fn new(image_size: usize, seed: u64) -> Self {
        let mut image = Vec::with_capacity(image_size);
        let chunks = image_size.div_ceil(8);
        for i in 0..chunks {
            let h = xxh3_64(&(seed.wrapping_add(i as u64)).to_le_bytes());
            let take = (image_size - image.len()).min(8);
            image.extend_from_slice(&h.to_le_bytes()[..take]);
        }
        Self { image }
    }

    /// Extract a slice at the given hash-based offset.
    pub fn extract(&self, hash_val: u64, slice_size: usize) -> &[u8] {
        let max_offset = self.image.len().saturating_sub(slice_size);
        let offset = if max_offset > 0 {
            (hash_val as usize) % (max_offset + 1)
        } else {
            0
        };
        let end = (offset + slice_size).min(self.image.len());
        &self.image[offset..end]
    }
}

/// Extract a fixed-size byte slice from a pre-built image.
///
/// Signature: `(input: u64) -> (bytes)`
/// Init params: `image_size`, `slice_size`, `seed`
///
/// The image is built at init time. Each cycle, the input u64 selects
/// the extraction offset via modular arithmetic.
pub struct ByteImageExtract {
    meta: NodeMeta,
    image: ByteImage,
    slice_size: usize,
}

impl ByteImageExtract {
    pub fn new(image_size: usize, slice_size: usize, seed: u64) -> Self {
        Self {
            meta: NodeMeta {
                name: "byte_image_extract".into(),
                outs: vec![Port::new("output", PortType::Bytes)],
                ins: vec![Slot::Wire(Port::u64("input"))],
            },
            image: ByteImage::new(image_size, seed),
            slice_size,
        }
    }
}

impl GkNode for ByteImageExtract {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let slice = self.image.extract(inputs[0].as_u64(), self.slice_size);
        outputs[0] = Value::Bytes(slice.to_vec());
    }
}

/// A pre-filled character image for fast text extraction.
///
/// Built at init time by cycling through a character set to fill a
/// buffer. At cycle time, a hash-based offset extracts a substring.
/// This is the Rust equivalent of nosqlbench's `CharBufImage`.
pub struct CharImage {
    image: String,
}

impl CharImage {
    /// Build a character image by repeating `charset` to fill `size` chars.
    pub fn new(charset: &str, size: usize) -> Self {
        let chars: Vec<char> = parse_charset(charset);
        assert!(!chars.is_empty(), "charset must not be empty");
        let mut image = String::with_capacity(size);
        let mut idx = 0;
        for _ in 0..size {
            image.push(chars[idx % chars.len()]);
            idx += 1;
        }
        Self { image }
    }

    /// Build a character image by hashing into the charset.
    pub fn hashed(charset: &str, size: usize, seed: u64) -> Self {
        let chars: Vec<char> = parse_charset(charset);
        assert!(!chars.is_empty(), "charset must not be empty");
        let mut image = String::with_capacity(size);
        for i in 0..size {
            let h = xxh3_64(&(seed.wrapping_add(i as u64)).to_le_bytes());
            image.push(chars[(h as usize) % chars.len()]);
        }
        Self { image }
    }

    fn extract(&self, hash_val: u64, slice_len: usize) -> &str {
        let chars: Vec<(usize, char)> = self.image.char_indices().collect();
        let max_start = chars.len().saturating_sub(slice_len);
        let start_idx = if max_start > 0 {
            (hash_val as usize) % (max_start + 1)
        } else {
            0
        };
        let end_idx = (start_idx + slice_len).min(chars.len());
        let byte_start = chars[start_idx].0;
        let byte_end = if end_idx < chars.len() {
            chars[end_idx].0
        } else {
            self.image.len()
        };
        &self.image[byte_start..byte_end]
    }
}

/// Extract a text slice from a pre-built character image.
///
/// Signature: `(input: u64) -> (String)`
/// Init params: `charset`, `image_size`, `slice_size`
///
/// Equivalent to nosqlbench's `CharBufImage`. The image is filled
/// from the charset at init time. Each cycle extracts a substring.
pub struct CharImageExtract {
    meta: NodeMeta,
    image: CharImage,
    slice_size: usize,
}

impl CharImageExtract {
    pub fn new(charset: &str, image_size: usize, slice_size: usize) -> Self {
        Self {
            meta: NodeMeta {
                name: "char_image_extract".into(),
                outs: vec![Port::new("output", PortType::Str)],
                ins: vec![Slot::Wire(Port::u64("input"))],
            },
            image: CharImage::hashed(charset, image_size, 0),
            slice_size,
        }
    }

    pub fn with_seed(charset: &str, image_size: usize, slice_size: usize, seed: u64) -> Self {
        Self {
            meta: NodeMeta {
                name: "char_image_extract".into(),
                outs: vec![Port::new("output", PortType::Str)],
                ins: vec![Slot::Wire(Port::u64("input"))],
            },
            image: CharImage::hashed(charset, image_size, seed),
            slice_size,
        }
    }
}

impl GkNode for CharImageExtract {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let text = self.image.extract(inputs[0].as_u64(), self.slice_size);
        outputs[0] = Value::Str(text.to_string());
    }
}

// =================================================================
// Byte slice and hex conversion
// =================================================================

/// Extract a sub-range from a byte buffer.
///
/// Signature: `(input: bytes) -> (bytes)`
pub struct ByteSlice {
    meta: NodeMeta,
    offset: usize,
    length: usize,
}

impl ByteSlice {
    pub fn new(offset: usize, length: usize) -> Self {
        Self {
            meta: NodeMeta {
                name: "byte_slice".into(),
                outs: vec![Port::new("output", PortType::Bytes)],
                ins: vec![Slot::Wire(Port::new("input", PortType::Bytes))],
            },
            offset,
            length,
        }
    }
}

impl GkNode for ByteSlice {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let bytes = inputs[0].as_bytes();
        let end = (self.offset + self.length).min(bytes.len());
        let start = self.offset.min(end);
        outputs[0] = Value::Bytes(bytes[start..end].to_vec());
    }
}

/// Encode bytes as lowercase hexadecimal string.
///
/// Signature: `(input: bytes) -> (String)`
pub struct ToHex {
    meta: NodeMeta,
}

impl Default for ToHex {
    fn default() -> Self {
        Self::new()
    }
}

impl ToHex {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "to_hex".into(),
                outs: vec![Port::new("output", PortType::Str)],
                ins: vec![Slot::Wire(Port::new("input", PortType::Bytes))],
            },
        }
    }
}

impl GkNode for ToHex {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let hex: String = inputs[0].as_bytes().iter().map(|b| format!("{b:02x}")).collect();
        outputs[0] = Value::Str(hex);
    }
}

/// Decode a hexadecimal string to bytes.
///
/// Signature: `(input: String) -> (bytes)`
pub struct FromHex {
    meta: NodeMeta,
}

impl Default for FromHex {
    fn default() -> Self {
        Self::new()
    }
}

impl FromHex {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "from_hex".into(),
                outs: vec![Port::new("output", PortType::Bytes)],
                ins: vec![Slot::Wire(Port::new("input", PortType::Str))],
            },
        }
    }
}

impl GkNode for FromHex {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let s = inputs[0].as_str();
        let bytes: Vec<u8> = (0..s.len())
            .step_by(2)
            .filter_map(|i| s.get(i..i + 2).and_then(|h| u8::from_str_radix(h, 16).ok()))
            .collect();
        outputs[0] = Value::Bytes(bytes);
    }
}

// --- charset parser (shared with string::Combinations) ---

fn parse_charset(spec: &str) -> Vec<char> {
    let mut chars = Vec::new();
    let spec_chars: Vec<char> = spec.chars().collect();
    let mut i = 0;
    while i < spec_chars.len() {
        if i + 2 < spec_chars.len() && spec_chars[i + 1] == '-' {
            for c in spec_chars[i]..=spec_chars[i + 2] {
                chars.push(c);
            }
            i += 3;
        } else {
            chars.push(spec_chars[i]);
            i += 1;
        }
    }
    chars
}

// ---------------------------------------------------------------------------
// Signature declarations for the DSL registry
// ---------------------------------------------------------------------------

use crate::dsl::registry::{Arity, FuncCategory, FuncSig, ParamSpec};
use crate::node::SlotType;

/// Signatures for byte buffer nodes.
pub fn signatures() -> &'static [FuncSig] {
    use FuncCategory as C;
    &[
        FuncSig {
            name: "u64_to_bytes", category: C::ByteBuffers, outputs: 1,
            description: "convert u64 to 8 bytes LE",
            help: "Convert a u64 to an 8-byte little-endian byte buffer.\nThis is the bridge from the integer domain to the bytes domain.\nFeed the result into sha256, md5, to_hex, or to_base64.\nParameters:\n  input — u64 wire input",
            identity: None, variadic_ctor: None,
            params: &[ParamSpec { name: "input", slot_type: SlotType::Wire, required: true }],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "bytes_from_hash", category: C::ByteBuffers,
            outputs: 1, description: "generate N deterministic bytes",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true },
                ParamSpec { name: "size", slot_type: SlotType::ConstU64, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Generate N deterministic bytes from a u64 seed via chained hashing.\nEach 8-byte chunk is hash(seed + chunk_index). Fresh per cycle.\nParameters:\n  input — u64 wire input (seed value)\n  size  — number of bytes to generate (u64)\nExample: bytes_from_hash(hash(cycle), 32)  // 32 pseudo-random bytes",
        },
        FuncSig {
            name: "to_hex", category: C::ByteBuffers, outputs: 1,
            description: "encode bytes as hex string",
            help: "Encode a byte buffer as a lowercase hexadecimal string.\nEach byte becomes two hex digits: [0xDE, 0xAD] -> \"dead\".\nUse after sha256/md5/u64_to_bytes for human-readable output.\nParameters:\n  input — Bytes wire input",
            identity: None, variadic_ctor: None,
            params: &[ParamSpec { name: "input", slot_type: SlotType::Wire, required: true }],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "from_hex", category: C::ByteBuffers, outputs: 1,
            description: "decode hex string to bytes",
            help: "Decode a hexadecimal string to a byte buffer.\nAccepts uppercase or lowercase hex digits. The string length\nmust be even (two hex chars per byte).\nParameters:\n  input — String wire input (hex-encoded)",
            identity: None, variadic_ctor: None,
            params: &[ParamSpec { name: "input", slot_type: SlotType::Wire, required: true }],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
    ]
}

/// Try to build a byte-buffer node from a function name and const args.
///
/// Returns `None` if the name is not handled by this module.
pub(crate) fn build_node(name: &str, _wires: &[crate::assembly::WireRef], consts: &[crate::dsl::factory::ConstArg]) -> Option<Result<Box<dyn crate::node::GkNode>, String>> {
    match name {
        "bytes_from_hash" => Some(Ok(Box::new(BytesFromHash::new(
            consts.first().map(|c| c.as_u64()).unwrap_or(16) as usize,
        )))),
        "u64_to_bytes" => Some(Ok(Box::new(U64ToBytes::new()))),
        "to_hex" => Some(Ok(Box::new(ToHex::new()))),
        "from_hex" => Some(Ok(Box::new(FromHex::new()))),
        _ => None,
    }
}


crate::register_nodes!(signatures, build_node);
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn u64_to_bytes_roundtrip() {
        let node = U64ToBytes::new();
        let mut out = [Value::None];
        node.eval(&[Value::U64(0xDEADBEEF)], &mut out);
        let bytes = out[0].as_bytes();
        assert_eq!(bytes.len(), 8);
        assert_eq!(u64::from_le_bytes(bytes.try_into().unwrap()), 0xDEADBEEF);
    }

    #[test]
    fn bytes_from_hash_size() {
        let node = BytesFromHash::new(32);
        let mut out = [Value::None];
        node.eval(&[Value::U64(42)], &mut out);
        assert_eq!(out[0].as_bytes().len(), 32);
    }

    #[test]
    fn bytes_from_hash_deterministic() {
        let node = BytesFromHash::new(16);
        let mut out1 = [Value::None];
        let mut out2 = [Value::None];
        node.eval(&[Value::U64(42)], &mut out1);
        node.eval(&[Value::U64(42)], &mut out2);
        assert_eq!(out1[0].as_bytes(), out2[0].as_bytes());
    }

    #[test]
    fn byte_image_extract_consistent_size() {
        let node = ByteImageExtract::new(10000, 100, 0);
        let mut out = [Value::None];
        for i in 0..100u64 {
            node.eval(&[Value::U64(i)], &mut out);
            assert_eq!(out[0].as_bytes().len(), 100);
        }
    }

    #[test]
    fn byte_image_extract_deterministic() {
        let node = ByteImageExtract::new(10000, 50, 0);
        let mut out1 = [Value::None];
        let mut out2 = [Value::None];
        node.eval(&[Value::U64(42)], &mut out1);
        node.eval(&[Value::U64(42)], &mut out2);
        assert_eq!(out1[0].as_bytes(), out2[0].as_bytes());
    }

    #[test]
    fn char_image_extract_size() {
        let node = CharImageExtract::new("A-Za-z0-9", 10000, 50);
        let mut out = [Value::None];
        node.eval(&[Value::U64(42)], &mut out);
        assert_eq!(out[0].as_str().len(), 50);
    }

    #[test]
    fn char_image_extract_charset() {
        let node = CharImageExtract::new("A-Z", 1000, 20);
        let mut out = [Value::None];
        node.eval(&[Value::U64(42)], &mut out);
        assert!(out[0].as_str().chars().all(|c| c.is_ascii_uppercase()));
    }

    #[test]
    fn char_image_extract_varied() {
        let node = CharImageExtract::new("A-Za-z0-9", 10000, 30);
        let mut out1 = [Value::None];
        let mut out2 = [Value::None];
        node.eval(&[Value::U64(0)], &mut out1);
        node.eval(&[Value::U64(999)], &mut out2);
        assert_ne!(out1[0].as_str(), out2[0].as_str());
    }

    #[test]
    fn byte_slice_basic() {
        let node = ByteSlice::new(2, 3);
        let mut out = [Value::None];
        node.eval(&[Value::Bytes(vec![10, 20, 30, 40, 50])], &mut out);
        assert_eq!(out[0].as_bytes(), &[30, 40, 50]);
    }

    #[test]
    fn hex_roundtrip() {
        let to = ToHex::new();
        let from = FromHex::new();
        let mut mid = [Value::None];
        let mut out = [Value::None];
        let input = vec![0xDE, 0xAD, 0xBE, 0xEF];
        to.eval(&[Value::Bytes(input.clone())], &mut mid);
        assert_eq!(mid[0].as_str(), "deadbeef");
        from.eval(&[mid[0].clone()], &mut out);
        assert_eq!(out[0].as_bytes(), &input);
    }
}
