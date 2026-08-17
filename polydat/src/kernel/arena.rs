// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Thread-local and cycle-scoped bump allocator and handle encoding
//! for non-scalar types in JIT execution frames (SRD 111).
//!
//! ## 64-bit Handle Format
//!
//! Non-scalar values (`String`, `Vec<u8>`, `serde_json::Value`, `&str`)
//! are encoded as 64-bit integer values in JIT buffer slots:
//!
//! - **Tag (bits 62..64)**:
//!   - `0b00`: Static string interner handle (`[Tag: 2][Unused: 30][InternerId: 32]`)
//!   - `0b01`: Dynamic cycle arena slice (`[Tag: 2][Offset: 31][Length: 31]`)
//!   - `0b10`: Resource handle / dataset index (`[Tag: 2][Type: 30][ResourceId: 32]`)
//!
//! This enables non-scalar data to flow through flat 64-bit slot registers
//! without per-operation heap allocations or pointer invalidation risks.

use std::cell::RefCell;
use std::sync::RwLock;

/// Handle Tag constants
pub const TAG_STATIC: u64 = 0b00 << 62;
pub const TAG_ARENA: u64  = 0b01 << 62;
pub const TAG_RES: u64    = 0b10 << 62;
pub const TAG_MASK: u64   = 0b11 << 62;

/// Default initial size for thread-local cycle bump arena (64KB).
const DEFAULT_ARENA_CAPACITY: usize = 64 * 1024;

/// Thread-local cycle bump arena for dynamic strings, byte buffers, and JSON.
pub struct CycleArena {
    buffer: Vec<u8>,
    cursor: usize,
}

impl Default for CycleArena {
    fn default() -> Self {
        Self::new()
    }
}

impl CycleArena {
    pub fn new() -> Self {
        Self {
            buffer: vec![0u8; DEFAULT_ARENA_CAPACITY],
            cursor: 0,
        }
    }

    /// Reset cursor in 1 instruction at cycle boundaries.
    #[inline(always)]
    pub fn reset(&mut self) {
        self.cursor = 0;
    }

    /// Allocate raw bytes in the arena and return the mutable slice.
    #[inline]
    pub fn alloc_bytes(&mut self, len: usize) -> &mut [u8] {
        if self.cursor + len > self.buffer.len() {
            let new_cap = (self.buffer.len() * 2).max(self.cursor + len);
            self.buffer.resize(new_cap, 0);
        }
        let start = self.cursor;
        self.cursor += len;
        &mut self.buffer[start..self.cursor]
    }

    /// Copy a byte slice into the arena and return its 64-bit handle.
    #[inline]
    pub fn put_bytes(&mut self, bytes: &[u8]) -> u64 {
        let len = bytes.len();
        let offset = self.cursor;
        let dest = self.alloc_bytes(len);
        dest.copy_from_slice(bytes);
        encode_arena_handle(offset as u32, len as u32)
    }

    /// Copy a string into the arena and return its 64-bit handle.
    #[inline]
    pub fn put_str(&mut self, s: &str) -> u64 {
        self.put_bytes(s.as_bytes())
    }

    /// Resolve a string from a 64-bit handle (static or arena).
    #[inline]
    pub fn resolve_str(&self, handle: u64) -> &str {
        match handle & TAG_MASK {
            TAG_STATIC => StaticInterner::resolve(handle as u32),
            TAG_ARENA => {
                let (offset, len) = decode_arena_handle(handle);
                let bytes = &self.buffer[offset as usize..(offset + len) as usize];
                unsafe { std::str::from_utf8_unchecked(bytes) }
            }
            _ => "",
        }
    }

    /// Resolve bytes from a 64-bit handle (static or arena).
    #[inline]
    pub fn resolve_bytes(&self, handle: u64) -> &[u8] {
        match handle & TAG_MASK {
            TAG_STATIC => StaticInterner::resolve(handle as u32).as_bytes(),
            TAG_ARENA => {
                let (offset, len) = decode_arena_handle(handle);
                &self.buffer[offset as usize..(offset + len) as usize]
            }
            _ => &[],
        }
    }
}

/// Encode arena offset and length into a 64-bit handle (Tag = `0b01`).
#[inline(always)]
pub fn encode_arena_handle(offset: u32, len: u32) -> u64 {
    TAG_ARENA | ((offset as u64 & 0x7FFF_FFFF) << 31) | (len as u64 & 0x7FFF_FFFF)
}

/// Decode arena handle into `(offset, length)`.
#[inline(always)]
pub fn decode_arena_handle(handle: u64) -> (u32, u32) {
    let offset = ((handle >> 31) & 0x7FFF_FFFF) as u32;
    let len = (handle & 0x7FFF_FFFF) as u32;
    (offset, len)
}

thread_local! {
    /// Thread-local cycle arena.
    pub static THREAD_CYCLE_ARENA: RefCell<CycleArena> = RefCell::new(CycleArena::new());
}

/// Helper to execute a closure with the thread-local cycle arena.
#[inline]
pub fn with_cycle_arena<R>(f: impl FnOnce(&mut CycleArena) -> R) -> R {
    THREAD_CYCLE_ARENA.with(|arena| f(&mut arena.borrow_mut()))
}

/// Resolve a string from a 64-bit handle using the thread-local cycle arena.
#[inline]
pub fn resolve_thread_str(handle: u64) -> &'static str {
    THREAD_CYCLE_ARENA.with(|arena| {
        let a = arena.borrow();
        match handle & TAG_MASK {
            TAG_STATIC => StaticInterner::resolve(handle as u32),
            TAG_ARENA => {
                let (offset, len) = decode_arena_handle(handle);
                let bytes = &a.buffer[offset as usize..(offset + len) as usize];
                unsafe { std::mem::transmute::<&str, &'static str>(std::str::from_utf8_unchecked(bytes)) }
            }
            _ => "",
        }
    })
}

/// Resolve bytes from a 64-bit handle using the thread-local cycle arena.
#[inline]
pub fn resolve_thread_bytes(handle: u64) -> &'static [u8] {
    THREAD_CYCLE_ARENA.with(|arena| {
        let a = arena.borrow();
        match handle & TAG_MASK {
            TAG_STATIC => StaticInterner::resolve(handle as u32).as_bytes(),
            TAG_ARENA => {
                let (offset, len) = decode_arena_handle(handle);
                let bytes = &a.buffer[offset as usize..(offset + len) as usize];
                unsafe { std::mem::transmute::<&[u8], &'static [u8]>(bytes) }
            }
            _ => &[],
        }
    })
}

/// Put a string into the thread-local cycle arena and return its handle.
#[inline]
pub fn put_thread_str(s: &str) -> u64 {
    THREAD_CYCLE_ARENA.with(|arena| arena.borrow_mut().put_str(s))
}

/// Put bytes into the thread-local cycle arena and return its handle.
#[inline]
pub fn put_thread_bytes(b: &[u8]) -> u64 {
    THREAD_CYCLE_ARENA.with(|arena| arena.borrow_mut().put_bytes(b))
}

/// Global static string interner for workload-compile-time constants.
pub struct StaticInterner;

static STATIC_STRINGS: RwLock<Vec<&'static str>> = RwLock::new(Vec::new());

impl StaticInterner {
    /// Intern a string literal or leaked string and return its static handle.
    pub fn intern(s: &str) -> u64 {
        let mut table = STATIC_STRINGS.write().unwrap();
        if let Some((idx, _)) = table.iter().enumerate().find(|&(_, entry)| *entry == s) {
            return TAG_STATIC | (idx as u64);
        }
        let leaked: &'static str = Box::leak(s.to_string().into_boxed_str());
        let idx = table.len();
        table.push(leaked);
        TAG_STATIC | (idx as u64)
    }

    /// Resolve a static handle back to string slice.
    pub fn resolve(id: u32) -> &'static str {
        let table = STATIC_STRINGS.read().unwrap();
        if let Some(&s) = table.get(id as usize) {
            s
        } else {
            ""
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arena_alloc_and_resolve() {
        let mut arena = CycleArena::new();
        let h1 = arena.put_str("hello");
        let h2 = arena.put_str("world");
        assert_eq!(arena.resolve_str(h1), "hello");
        assert_eq!(arena.resolve_str(h2), "world");

        arena.reset();
        let h3 = arena.put_str("fresh");
        assert_eq!(arena.resolve_str(h3), "fresh");
    }

    #[test]
    fn static_interner_roundtrip() {
        let h1 = StaticInterner::intern("test_constant");
        let h2 = StaticInterner::intern("test_constant");
        assert_eq!(h1, h2, "interning same string must return identical handle");

        let arena = CycleArena::new();
        assert_eq!(arena.resolve_str(h1), "test_constant");
    }
}
