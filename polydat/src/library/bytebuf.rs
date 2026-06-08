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

use xxhash_rust::xxh3::xxh3_64;
#[cfg(test)]
use crate::ast::{PolydatNode, Value};

// =================================================================
// Direct byte generation
// =================================================================

/// Convert a u64 to 8 bytes (little-endian).
/// SRD-80 PR B.13 migration.
#[crate::polydat_node(category = ByteBuffers)]
fn u64_to_bytes(input: u64) -> Vec<u8> {
    input.to_le_bytes().to_vec()
}

/// Generate N deterministic bytes from a u64 seed via chained hashing.
/// Each 8-byte chunk is `hash(seed + chunk_index)`. Buffer is fresh
/// per cycle. SRD-80 PR B.13 migration.
#[crate::polydat_node(category = ByteBuffers)]
fn bytes_from_hash(
    input: u64,
    #[poly_default(8u64)] size: crate::derive_support::Const<u64>,
) -> Vec<u8> {
    let sz = *size as usize;
    let mut result = Vec::with_capacity(sz);
    let chunks = sz.div_ceil(8);
    for i in 0..chunks {
        let h = xxh3_64(&(input.wrapping_add(i as u64)).to_le_bytes());
        let take = (sz - result.len()).min(8);
        result.extend_from_slice(&h.to_le_bytes()[..take]);
    }
    result
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

/// Setup function: build the byte image from `image_size` + `seed`.
/// Single-call construction-time invocation per node instance.
fn build_byte_image(image_size: u64, seed: u64) -> ByteImage {
    ByteImage::new(image_size as usize, seed)
}

/// Extract a fixed-size byte slice from a pre-built image.
///
/// Signature: `byte_image_extract(input: u64) -> (output: bytes)`
/// Const: `image_size: u64`, `slice_size: u64`, `seed: u64`
///
/// The image is built at init time from `image_size` + `seed`. Each
/// cycle, the input u64 selects the extraction offset via modular
/// arithmetic and a `slice_size`-long span is copied out.
///
/// SRD-80b Phase E: migrated to `#[polydat_node]` via multi-source
/// `#[poly_const(... from = (image_size, seed))]`. `slice_size` stays
/// a per-node `Const<u64>` consumed in the body.
#[crate::polydat_node(category = ByteBuffers)]
fn byte_image_extract(
    input: u64,
    image_size: crate::derive_support::Const<u64>,
    slice_size: crate::derive_support::Const<u64>,
    seed: crate::derive_support::Const<u64>,
    #[poly_const(build_byte_image, from = (image_size, seed))]
    image: &ByteImage,
) -> Vec<u8> {
    let _ = image_size; // captured in `image`; field kept for workload-author surface
    let _ = seed;
    image.extract(input, *slice_size as usize).to_vec()
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

/// Setup function: build the character image from `charset` +
/// `image_size` + `seed`. Single-call construction-time invocation.
fn build_char_image(charset: &str, image_size: u64, seed: u64) -> CharImage {
    CharImage::hashed(charset, image_size as usize, seed)
}

/// Extract a text slice from a pre-built character image.
///
/// Signature: `char_image_extract(input: u64) -> (output: Str)`
/// Const: `charset: Str`, `image_size: u64`, `slice_size: u64`,
///        `seed: u64` (default 0)
///
/// Equivalent to nosqlbench's `CharBufImage`. The image is filled
/// from the charset at init time. Each cycle extracts a substring.
///
/// SRD-80b Phase E: migrated to `#[polydat_node]` via multi-source
/// `#[poly_const(... from = (charset, image_size, seed))]`.
#[crate::polydat_node(category = ByteBuffers)]
fn char_image_extract(
    input: u64,
    charset: crate::derive_support::Const<&str>,
    image_size: crate::derive_support::Const<u64>,
    slice_size: crate::derive_support::Const<u64>,
    #[poly_default(0u64)] seed: crate::derive_support::Const<u64>,
    #[poly_const(build_char_image, from = (charset, image_size, seed))]
    image: &CharImage,
) -> String {
    let _ = charset;
    let _ = image_size;
    let _ = seed;
    image.extract(input, *slice_size as usize).to_string()
}

// =================================================================
// Byte slice and hex conversion
// =================================================================

/// Extract a sub-range from a byte buffer.
/// SRD-80 PR B.13 migration.
#[crate::polydat_node(category = ByteBuffers)]
fn byte_slice(
    input: &[u8],
    #[poly_default(0u64)] offset: crate::derive_support::Const<u64>,
    #[poly_default(8u64)] length: crate::derive_support::Const<u64>,
) -> Vec<u8> {
    let off = *offset as usize;
    let len = *length as usize;
    let end = (off + len).min(input.len());
    let start = off.min(end);
    input[start..end].to_vec()
}

/// Encode bytes as lowercase hexadecimal string.
/// SRD-80 PR B.13 migration.
#[crate::polydat_node(category = ByteBuffers)]
fn to_hex(input: &[u8]) -> String {
    input.iter().map(|b| format!("{b:02x}")).collect()
}

/// Decode a hexadecimal string to bytes.
/// SRD-80 PR B.13 migration.
#[crate::polydat_node(category = ByteBuffers)]
fn from_hex(input: &str) -> Vec<u8> {
    (0..input.len())
        .step_by(2)
        .filter_map(|i| input.get(i..i + 2).and_then(|h| u8::from_str_radix(h, 16).ok()))
        .collect()
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

// All `#[polydat_node]`-authored byte-buffer nodes — including
// `ByteImageExtract` and `CharImageExtract` (SRD-80b Phase E
// multi-source `#[poly_const]` migration) — are auto-registered
// via inventory.
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
        let node = CharImageExtract::new("A-Za-z0-9".to_string(), 10000, 50, 0);
        let mut out = [Value::None];
        node.eval(&[Value::U64(42)], &mut out);
        assert_eq!(out[0].as_str().len(), 50);
    }

    #[test]
    fn char_image_extract_charset() {
        let node = CharImageExtract::new("A-Z".to_string(), 1000, 20, 0);
        let mut out = [Value::None];
        node.eval(&[Value::U64(42)], &mut out);
        assert!(out[0].as_str().chars().all(|c| c.is_ascii_uppercase()));
    }

    #[test]
    fn char_image_extract_varied() {
        let node = CharImageExtract::new("A-Za-z0-9".to_string(), 10000, 30, 0);
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
        node.eval(&[Value::Bytes(vec![10u8, 20, 30, 40, 50].into())], &mut out[..]);
        assert_eq!(out[0].as_bytes(), &[30, 40, 50]);
    }

    #[test]
    fn hex_roundtrip() {
        let to = ToHex::new();
        let from = FromHex::new();
        let mut mid = [Value::None];
        let mut out = [Value::None];
        let input = vec![0xDE, 0xAD, 0xBE, 0xEF];
        to.eval(&[Value::Bytes(input.clone().into())], &mut mid[..]);
        assert_eq!(mid[0].as_str(), "deadbeef");
        from.eval(&[mid[0].clone()], &mut out);
        assert_eq!(out[0].as_bytes(), &input);
    }
}
