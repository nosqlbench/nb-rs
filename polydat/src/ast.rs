// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Core types for Polydat nodes: values, ports, metadata, and the evaluation trait.
//!
//! The Polydat type system has three layers:
//!
//! 1. **Runtime values** ([`Value`]) — the enum that flows through
//!    the DAG at evaluation time. Every buffer slot holds a `Value`.
//!
//! 2. **Port types** ([`PortType`]) — compile-time type tags on
//!    node input/output ports. The assembler validates that wiring
//!    connects compatible types and auto-inserts adapters when not.
//!
//! 3. **Slot types** ([`SlotType`]) — distinguishes wire inputs
//!    (cycle-time values) from constant parameters (baked at
//!    construction). The DSL compiler uses these to decide whether
//!    a literal in a function call is a wire promotion or a const arg.
//!
//! The [`PolydatNode`] trait is what every node function implements.
//! A node declares its port metadata via [`NodeMeta`] and evaluates
//! via `eval(&[Value], &mut [Value])`.

use std::fmt;
use std::sync::Arc;
use std::ops::Deref;

/// Arc-managed typed slice. Holds a borrow into a parent Arc'd
/// owner — typically either an owned backing buffer (`Arc<[T]>`)
/// or a long-lived resource like an mmap'd dataset. Cloning is
/// one `Arc::clone` (atomic increment, zero allocations); the
/// owner is type-erased as `Arc<dyn Any + Send + Sync>` so the
/// same `SliceArc<T>` shape covers both modes.
///
/// Used by [`Value::VecF32`] / [`Value::VecI32`] to flow vector
/// data on wires from accessors to native-binding adapters with:
///   - zero per-cycle allocation when the source supports
///     zero-copy reads (mmap-backed `VectorReader::get_slice`),
///   - exactly one allocation when it doesn't (a `Vec<T>` from
///     `VectorReader::get`, wrapped into an `Arc<[T]>`).
///
/// See SRD 53 §"Native Vector Binding".
pub struct SliceArc<T: 'static> {
    /// Keeps the storage alive. For owned data this is an
    /// `Arc<OwnedSlice<T>>`; for mmap-backed data this is an
    /// `Arc<UniformDataset<T>>` (or any other type whose Arc
    /// keeps the underlying memory mapped).
    _owner: Arc<dyn std::any::Any + Send + Sync>,
    ptr: *const T,
    len: usize,
}

// Send/Sync: the raw pointer is treated as a borrow into memory
// owned by `_owner`, which is itself Send+Sync. T must be
// Send+Sync for the slice contents to be safely shared.
unsafe impl<T: Send + Sync + 'static> Send for SliceArc<T> {}
unsafe impl<T: Send + Sync + 'static> Sync for SliceArc<T> {}

/// Type-erasable wrapper for an owned `Arc<[T]>`. Used as the
/// owner when the source isn't zero-copy — `Arc<[T]>` is unsized
/// so it can't be cast to `Arc<dyn Any>` directly, but
/// `OwnedSlice<T>` is sized and the cast works.
// Field is unused at the type level — its only job is to keep the
// Arc<[T]> reference count alive while the SliceArc holds the raw
// pointer into the buffer. Hence the `dead_code` allow.
#[allow(dead_code)]
pub(crate) struct OwnedSlice<T: 'static>(pub(crate) Arc<[T]>);

impl<T: Send + Sync + 'static> SliceArc<T> {
    /// Build from an owned `Vec<T>`. One heap allocation
    /// (`Vec → Arc<[T]>`); cloning the resulting `SliceArc<T>` is
    /// one atomic increment.
    pub fn from_vec(v: Vec<T>) -> Self {
        let arc: Arc<[T]> = Arc::from(v);
        let ptr = arc.as_ptr();
        let len = arc.len();
        let owner: Arc<dyn std::any::Any + Send + Sync> = Arc::new(OwnedSlice(arc));
        Self { _owner: owner, ptr, len }
    }

    /// Build from a `&[T]` borrowed from `owner`'s data.
    ///
    /// SAFETY: `slice` must point into memory owned by `owner` and
    /// remain valid for at least as long as `owner` (i.e., until
    /// the last clone of this Arc is dropped). The caller asserts
    /// this — typical use is mmap-backed readers where the slice
    /// is a view into a memory-mapped page kept alive by the
    /// dataset Arc.
    pub unsafe fn from_borrowed(
        owner: Arc<dyn std::any::Any + Send + Sync>,
        slice: &[T],
    ) -> Self {
        Self {
            _owner: owner,
            ptr: slice.as_ptr(),
            len: slice.len(),
        }
    }

}

impl<T: 'static> SliceArc<T> {
    /// Borrow as `&[T]`. The borrow lives as long as `&self`.
    /// Defined here without Send+Sync bounds so it's reachable
    /// from `Deref`/`PartialEq`/`Debug` impls that don't carry
    /// those bounds.
    pub fn as_slice(&self) -> &[T] {
        // SAFETY: `_owner` keeps the storage alive; `ptr`/`len`
        // were validated at construction. The returned reference
        // is bounded by `&self`'s lifetime.
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
}

impl<T: Send + Sync + 'static> Clone for SliceArc<T> {
    fn clone(&self) -> Self {
        Self {
            _owner: self._owner.clone(),
            ptr: self.ptr,
            len: self.len,
        }
    }
}

impl<T: 'static> Deref for SliceArc<T> {
    type Target = [T];
    fn deref(&self) -> &[T] {
        // SAFETY: identical reasoning to as_slice().
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
}

impl<T: PartialEq + 'static> PartialEq for SliceArc<T> {
    fn eq(&self, other: &Self) -> bool {
        // Pointer-equal pair → trivially equal (zero-copy from the
        // same source). Otherwise compare contents — two unrelated
        // SliceArcs may hold equal data.
        if std::ptr::eq(self.ptr, other.ptr) && self.len == other.len {
            return true;
        }
        self.as_slice() == other.as_slice()
    }
}

impl<T: fmt::Debug + 'static> fmt::Debug for SliceArc<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SliceArc")
            .field("len", &self.len)
            .field("first", &self.as_slice().first())
            .finish_non_exhaustive()
    }
}

/// A value flowing through the DAG at runtime.
///
/// This is the universal runtime representation for all Polydat data.
/// Every node input and output is a `Value`. The variant determines
/// the data type:
///
/// | Variant | Rust type | Polydat DSL type | Usage |
/// |---------|-----------|-------------|-------|
/// | `U64` | `u64` | `u64` | Cycle counters, hashes, IDs, bitwise ops |
/// | `F64` | `f64` | `f64` | Floating point math, distributions, noise |
/// | `Bool` | `bool` | `bool` | Conditions, flags |
/// | `Str` | `String` | `String` | Names, formatted output, templates |
/// | `Bytes` | `Vec<u8>` | `bytes` | Raw binary data, digests |
/// | `Json` | `serde_json::Value` | `json` | Structured data, vectors |
/// | `Ext` | `Box<dyn ReflectedValue>` | adapter-specific | CQL UUIDs, timestamps, etc. |
/// | `None` | — | — | Uninitialized buffer slot (never flows through wiring) |
///
/// The assembly phase validates type correctness at compile time.
/// Runtime code can safely use `as_u64()`, `as_f64()`, etc. — a
/// type mismatch is a compiler bug, not a user error.
///
/// `Ext` enables adapter-contributed types (e.g., `uuid::Uuid` from
/// the CQL adapter) to flow through the DAG without the kernel
/// knowing the concrete type. Any consumer can display, serialize,
/// or inspect an Ext value via [`ReflectedValue`]. The producing
/// adapter can downcast via `as_any()`.
#[derive(Debug, Clone)]
pub enum Value {
    /// Unsigned 64-bit integer. The workhorse type for deterministic
    /// data generation: hash outputs, modular arithmetic, bit
    /// manipulation, cycle counters, primary keys.
    U64(u64),
    /// IEEE 754 double-precision float. Used for distributions,
    /// noise functions, trigonometry, interpolation, and any
    /// computation that needs fractional precision.
    F64(f64),
    /// Boolean. Used for conditional ops (`if:` field), selection
    /// nodes, and flag computation.
    Bool(bool),
    /// Shared, immutable UTF-8 string. Used for formatted output,
    /// weighted string selection, template interpolation, and any
    /// value that will appear directly in an op statement. Backed
    /// by `Arc<str>` so cloning is one atomic increment with no
    /// allocation — the per-cycle reads that materialize a `final`
    /// or `init` string into op-template substitution are
    /// pointer-share, not heap-copy.
    Str(Arc<str>),
    /// Shared, immutable raw byte buffer. Used for cryptographic
    /// digests, binary encoding/decoding, and byte-level data
    /// generation. Backed by `Arc<[u8]>` so cloning is one atomic
    /// increment.
    Bytes(Arc<[u8]>),
    /// Shared, immutable structured JSON value. Used for
    /// vector representations (JSON arrays), complex structured
    /// data, and JSON merge ops. Backed by `Arc<serde_json::Value>`
    /// so cloning is one atomic increment — the per-cycle reads
    /// of result-body JSON wires (capture extraction, recall
    /// evaluation, column projection) share the underlying
    /// allocation rather than deep-cloning the tree. Consumers
    /// that need an owned `serde_json::Value` (mutation,
    /// serialization sinks) explicitly deep-clone via
    /// `(*v).clone()` at the consume site.
    Json(Arc<serde_json::Value>),
    /// Adapter-contributed reflected value. Carries type info and
    /// standard access methods (display, JSON, string, bytes).
    /// Enables protocol-native types (UUIDs, timestamps, inet
    /// addresses) to flow through Polydat without boxing to strings.
    Ext(Box<dyn ReflectedValue>),
    /// Type-erased Arc handle to a resolved resource (dataset,
    /// prepared statement, ...). Cloning during input gather is one
    /// `Arc::clone` — a single atomic increment, zero allocations.
    /// Produced by resolver nodes (e.g. `dataset_open`) and consumed
    /// by reader nodes that downcast to the concrete type. See
    /// SRD 53 §"Dataset Handles" for the canonical use case.
    Handle(Arc<dyn std::any::Any + Send + Sync>),
    /// Typed `f32` vector carrier. Flows from vector accessors to
    /// native-binding adapters without string formatting or byte
    /// serialization on the cycle path. Cloning is one `Arc::clone`,
    /// zero allocations. The underlying [`SliceArc`] supports both
    /// owned (allocated `Arc<[f32]>`) and zero-copy (borrow into a
    /// long-lived owner like an mmap'd dataset) storage modes.
    /// `to_display_string()` renders as JSON array.
    VecF32(SliceArc<f32>),
    /// Typed `i32` vector carrier (e.g. neighbor indices). Same
    /// shape as VecF32 — typed slice on the wire.
    VecI32(SliceArc<i32>),
    /// Typed `f64` vector carrier (`Arc<[f64]>`). Same shape as
    /// VecF32. Used for double-precision embeddings / dense
    /// numeric features bound to CQL `vector<double, N>` etc.
    VecF64(SliceArc<f64>),
    /// Typed `i64` vector carrier (`Arc<[i64]>`). 64-bit integer
    /// vectors for CQL `vector<bigint, N>`.
    VecI64(SliceArc<i64>),
    /// Typed half-precision float vector (`Arc<[half::f16]>`).
    /// 16-bit float carrier — stays at f16 on the wire so
    /// embeddings stored as half-precision aren't widened on the
    /// kernel side.
    VecF16(SliceArc<half::f16>),
    /// Typed `i16` vector carrier (`Arc<[i16]>`). 16-bit signed
    /// integer vectors for CQL `vector<smallint, N>`.
    VecI16(SliceArc<i16>),
    /// Sentinel for uninitialized buffer slots. Never appears in
    /// wiring — only in freshly allocated state buffers before
    /// first evaluation.
    None,
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::U64(a), Value::U64(b)) => a == b,
            (Value::F64(a), Value::F64(b)) => a == b,
            (Value::Bool(a), Value::Bool(b)) => a == b,
            // Arc-backed variants: pointer-eq fast path before
            // any content compare. Hot per-cycle callers
            // (notably `PolydatState::reset_inputs_from`'s
            // "still at default?" probe) typically test a slot
            // against a value that was Arc-cloned from the same
            // source — `Arc::ptr_eq` is O(1) and lets the deep
            // compare drop out of the per-cycle path.
            (Value::Str(a), Value::Str(b)) => Arc::ptr_eq(a, b) || a == b,
            (Value::Bytes(a), Value::Bytes(b)) => Arc::ptr_eq(a, b) || a == b,
            (Value::Json(a), Value::Json(b)) => Arc::ptr_eq(a, b) || a == b,
            (Value::None, Value::None) => true,
            (Value::Ext(a), Value::Ext(b)) => {
                a.type_name() == b.type_name() && a.display() == b.display()
            }
            (Value::Handle(a), Value::Handle(b)) => Arc::ptr_eq(a, b),
            (Value::VecF32(a), Value::VecF32(b)) => a == b,
            (Value::VecI32(a), Value::VecI32(b)) => a == b,
            (Value::VecF64(a), Value::VecF64(b)) => a == b,
            (Value::VecI64(a), Value::VecI64(b)) => a == b,
            (Value::VecF16(a), Value::VecF16(b)) => a == b,
            (Value::VecI16(a), Value::VecI16(b)) => a == b,
            _ => false,
        }
    }
}

/// Trait for adapter-contributed value types.
///
/// Any type that flows through the Polydat Kernel as `Value::Ext` must
/// implement this. It provides standard access patterns that work
/// across adapter boundaries — stdout can display it, HTTP can
/// serialize it, model adapter can capture it — without needing
/// the concrete type.
///
/// The producing adapter can downcast via `as_any()` when it needs
/// native protocol access (e.g., CQL binding a `uuid::Uuid`).
pub trait ReflectedValue: Send + Sync + std::fmt::Debug {
    /// Type name for diagnostics and describe output.
    fn type_name(&self) -> &str;

    /// Human-readable string representation.
    /// Used by stdout adapter, logging, and diagnostics.
    fn display(&self) -> String;

    /// JSON representation for serialization and HTTP bodies.
    fn to_json_value(&self) -> serde_json::Value {
        serde_json::Value::String(self.display())
    }

    /// Try to represent as a string. Many types have a canonical
    /// string form (UUIDs, timestamps, IP addresses).
    fn try_as_str(&self) -> Option<String> {
        Some(self.display())
    }

    /// Try to represent as u64.
    fn try_as_u64(&self) -> Option<u64> { None }

    /// Try to represent as f64.
    fn try_as_f64(&self) -> Option<f64> { None }

    /// Try to represent as bytes.
    fn try_as_bytes(&self) -> Option<&[u8]> { None }

    /// Downcast to the concrete type. Only works when the consuming
    /// code has the concrete type in scope (same crate or shared dep).
    fn as_any(&self) -> &dyn std::any::Any;

    /// Clone into a new boxed trait object.
    fn clone_reflected(&self) -> Box<dyn ReflectedValue>;
}

impl Clone for Box<dyn ReflectedValue> {
    fn clone(&self) -> Self {
        self.clone_reflected()
    }
}

impl Value {
    pub fn as_u64(&self) -> u64 {
        match self {
            Value::U64(v) => *v,
            _ => panic!("expected U64, got {:?}", self.port_type()),
        }
    }

    pub fn as_f64(&self) -> f64 {
        match self {
            Value::F64(v) => *v,
            _ => panic!("expected F64, got {:?}", self.port_type()),
        }
    }

    pub fn as_bool(&self) -> bool {
        match self {
            Value::Bool(v) => *v,
            _ => panic!("expected Bool, got {:?}", self.port_type()),
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            Value::Str(v) => v,
            _ => panic!("expected Str, got {:?}", self.port_type()),
        }
    }

    pub fn as_bytes(&self) -> &[u8] {
        match self {
            Value::Bytes(v) => v,
            _ => panic!("expected Bytes, got {:?}", self.port_type()),
        }
    }

    pub fn as_json(&self) -> &serde_json::Value {
        match self {
            Value::Json(v) => v,
            _ => panic!("expected Json, got {:?}", self.port_type()),
        }
    }

    /// Borrow the inner `Arc<serde_json::Value>` from a
    /// `Value::Json` variant. Use when a consumer wants to
    /// share the JSON tree across kernels without deep-cloning
    /// the structure — e.g. capture extraction that writes the
    /// same JSON wire to multiple downstream slots. Panics on
    /// type mismatch.
    pub fn as_json_arc(&self) -> &Arc<serde_json::Value> {
        match self {
            Value::Json(v) => v,
            _ => panic!("expected Json, got {:?}", self.port_type()),
        }
    }

    /// Return the `PortType` corresponding to this value's variant.
    pub fn port_type(&self) -> PortType {
        match self {
            Value::U64(_) => PortType::U64,
            Value::F64(_) => PortType::F64,
            Value::Bool(_) => PortType::Bool,
            Value::Str(_) => PortType::Str,
            Value::Bytes(_) => PortType::Bytes,
            Value::Json(_) => PortType::Json,
            Value::Ext(_) => PortType::Ext,
            Value::Handle(_) => PortType::Handle,
            Value::VecF32(_) => PortType::VecF32,
            Value::VecI32(_) => PortType::VecI32,
            Value::VecF64(_) => PortType::VecF64,
            Value::VecI64(_) => PortType::VecI64,
            Value::VecF16(_) => PortType::VecF16,
            Value::VecI16(_) => PortType::VecI16,
            Value::None => PortType::U64, // placeholder
        }
    }

    /// Borrow a `VecF32` value as `&[f32]`. Panics on type mismatch.
    pub fn as_vec_f32(&self) -> &[f32] {
        match self {
            Value::VecF32(arc) => arc,
            _ => panic!("expected VecF32, got {:?}", self.port_type()),
        }
    }

    /// Test whether this value's runtime variant is acceptable
    /// to a slot declaring `slot_type`. `port_type() == slot_type`
    /// is the strict case; this method also accepts the
    /// **bit-stuffing equivalences** documented in
    /// `polydat/docs/design/type_system.md` §1:
    ///
    /// - `Value::U64` is the runtime storage for `PortType` `U64`,
    ///   `U32`, `I64`, and `I32` (narrow integers carry their
    ///   bits in the low part of the u64; sign-extension for
    ///   `I32` is part of the producer convention).
    /// - `Value::F64` is the runtime storage for `PortType` `F64`
    ///   and `F32` (`F32` carries its bits in the low 32 via
    ///   `f32::to_bits() as u64`-style stuffing — but float
    ///   stuffing uses `Value::F64` for the materialised float
    ///   value, not the bit pattern).
    /// - `Value::None` is acceptable for every slot type
    ///   (SRD-74 absent sentinel).
    ///
    /// Used at the typed-write residual check
    /// (`Dataflow::set_wire_idx`) AFTER the boundary adapter has
    /// already converted/validated the value — see
    /// `polydat/src/kernel/api_impl.rs`. The pre-adapter check in
    /// `adapt_boundary_value` stays strict (`port_type ==
    /// slot_type`) so an unadapted Value::U64 can never silently
    /// truncate into a narrower slot.
    pub fn satisfies_slot(&self, slot_type: PortType) -> bool {
        if matches!(self, Value::None) {
            return true;
        }
        let value_type = self.port_type();
        if value_type == slot_type {
            return true;
        }
        matches!(
            (value_type, slot_type),
            (PortType::U64, PortType::U32 | PortType::I64 | PortType::I32)
                | (PortType::F64, PortType::F32)
        )
    }

    /// Borrow a `VecI32` value as `&[i32]`. Panics on type mismatch.
    pub fn as_vec_i32(&self) -> &[i32] {
        match self {
            Value::VecI32(arc) => arc,
            _ => panic!("expected VecI32, got {:?}", self.port_type()),
        }
    }

    /// Borrow a `VecF64` value as `&[f64]`. Panics on type mismatch.
    pub fn as_vec_f64(&self) -> &[f64] {
        match self {
            Value::VecF64(arc) => arc,
            _ => panic!("expected VecF64, got {:?}", self.port_type()),
        }
    }

    /// Borrow a `VecI64` value as `&[i64]`. Panics on type mismatch.
    pub fn as_vec_i64(&self) -> &[i64] {
        match self {
            Value::VecI64(arc) => arc,
            _ => panic!("expected VecI64, got {:?}", self.port_type()),
        }
    }

    /// Borrow a `VecF16` value as `&[half::f16]`. Panics on type mismatch.
    pub fn as_vec_f16(&self) -> &[half::f16] {
        match self {
            Value::VecF16(arc) => arc,
            _ => panic!("expected VecF16, got {:?}", self.port_type()),
        }
    }

    /// Borrow a `VecI16` value as `&[i16]`. Panics on type mismatch.
    pub fn as_vec_i16(&self) -> &[i16] {
        match self {
            Value::VecI16(arc) => arc,
            _ => panic!("expected VecI16, got {:?}", self.port_type()),
        }
    }

    /// Downcast a Handle value to a borrowed reference of its concrete
    /// type. Panics if the variant isn't `Handle` or the type doesn't
    /// match. Used by reader nodes that consume a typed-handle wire
    /// produced by a resolver node (see SRD 53 §"Dataset Handles").
    ///
    /// The borrow lasts as long as `self` (the buffer slot's `Value`
    /// is what holds the `Arc`). For per-cycle reads this is the
    /// expected pattern — call methods on the borrowed dataset, then
    /// return.
    pub fn as_handle<T: std::any::Any + Send + Sync>(&self) -> &T {
        match self {
            Value::Handle(arc) => arc.downcast_ref::<T>().unwrap_or_else(|| {
                panic!(
                    "Handle downcast failed: expected {}",
                    std::any::type_name::<T>()
                )
            }),
            _ => panic!("expected Handle, got {:?}", self.port_type()),
        }
    }

    /// Construct a `Value::Handle` from a typed `Arc<T>`. Convenience
    /// wrapper that performs the type-erasure to `Arc<dyn Any + Send + Sync>`.
    pub fn handle<T: std::any::Any + Send + Sync>(arc: Arc<T>) -> Self {
        Value::Handle(arc as Arc<dyn std::any::Any + Send + Sync>)
    }

    /// Best-effort string representation for any value.
    /// Works across all variants including Ext.
    pub fn to_display_string(&self) -> String {
        match self {
            Value::U64(v) => v.to_string(),
            // `{v:?}` (Rust Debug) for f64 always includes at
            // least one fractional digit, so whole-number floats
            // render as `1.0` instead of `1` — distinguishing
            // them from integers in CQL OPTIONS strings, plot
            // labels, and other surfaces where the type matters.
            // Display-formatted (`v.to_string()`) strips the
            // trailing zero, conflating ints with whole-number
            // floats. Both forms produce identical output for
            // non-whole floats (`1.5 → "1.5"`).
            Value::F64(v) => format!("{v:?}"),
            Value::Bool(v) => v.to_string(),
            Value::Str(v) => v.to_string(),
            Value::Bytes(v) => v.iter().map(|b| format!("{b:02x}")).collect(),
            Value::Json(v) => v.to_string(),
            Value::Ext(v) => v.display(),
            Value::Handle(arc) => format!("<handle:{:?}>", arc.type_id()),
            Value::VecF32(arc) => {
                // JSON-array text. Per-element format-write into a
                // pre-sized String avoids the intermediate Vec<String>.
                // Debug formatter (`{v:?}`) matches the F64 element
                // rule above: whole-number floats render as `1.0`
                // so VecF32 stays distinguishable from VecI32 at the
                // display surface.
                let mut s = String::with_capacity(arc.len() * 8 + 2);
                s.push('[');
                let mut first = true;
                for v in arc.iter() {
                    if !first { s.push(','); }
                    first = false;
                    use std::fmt::Write;
                    let _ = write!(&mut s, "{v:?}");
                }
                s.push(']');
                s
            }
            Value::VecI32(arc) => {
                let mut s = String::with_capacity(arc.len() * 4 + 2);
                s.push('[');
                let mut first = true;
                for v in arc.iter() {
                    if !first { s.push(','); }
                    first = false;
                    use std::fmt::Write;
                    let _ = write!(&mut s, "{v}");
                }
                s.push(']');
                s
            }
            Value::VecF64(arc) => {
                let mut s = String::with_capacity(arc.len() * 8 + 2);
                s.push('[');
                let mut first = true;
                for v in arc.iter() {
                    if !first { s.push(','); }
                    first = false;
                    use std::fmt::Write;
                    let _ = write!(&mut s, "{v:?}");
                }
                s.push(']');
                s
            }
            Value::VecI64(arc) => {
                let mut s = String::with_capacity(arc.len() * 4 + 2);
                s.push('[');
                let mut first = true;
                for v in arc.iter() {
                    if !first { s.push(','); }
                    first = false;
                    use std::fmt::Write;
                    let _ = write!(&mut s, "{v}");
                }
                s.push(']');
                s
            }
            Value::VecF16(arc) => {
                let mut s = String::with_capacity(arc.len() * 6 + 2);
                s.push('[');
                let mut first = true;
                for v in arc.iter() {
                    if !first { s.push(','); }
                    first = false;
                    use std::fmt::Write;
                    // Render as the f32 widening so the JSON form
                    // is the standard "1.0" / "1.5" surface — f16
                    // Display has its own form but it isn't valid
                    // JSON, so widening makes the array shape
                    // parseable downstream.
                    let _ = write!(&mut s, "{:?}", v.to_f32());
                }
                s.push(']');
                s
            }
            Value::VecI16(arc) => {
                let mut s = String::with_capacity(arc.len() * 4 + 2);
                s.push('[');
                let mut first = true;
                for v in arc.iter() {
                    if !first { s.push(','); }
                    first = false;
                    use std::fmt::Write;
                    let _ = write!(&mut s, "{v}");
                }
                s.push(']');
                s
            }
            Value::None => String::new(),
        }
    }

    /// Strict-render variant of [`Self::to_display_string`] for use
    /// at wire-protocol render sites (op-template substitution,
    /// adapter byte-emission paths).
    ///
    /// Returns `None` for [`Value::None`] instead of converting it
    /// to `""`. The empty-string mapping in `to_display_string` is
    /// convenient for diagnostic / log contexts but lethal at the
    /// wire boundary — it silently coerces "absent" into "present
    /// but empty," corrupting downstream bytes (e.g. sending
    /// `'source_model': ''` to a CQL cluster when the intended
    /// shadow didn't bind). Render paths use this primitive and
    /// surface a clear error when an unresolved bind-point reaches
    /// them. See [SRD 74](../../../docs/sysref/74_none_propagation.md)
    /// §"Rule 3 — Op-template render refuses silent None".
    pub fn to_display_strict(&self) -> Option<String> {
        match self {
            Value::None => None,
            other => Some(other.to_display_string()),
        }
    }

    /// JSON representation for any value. Works across all variants.
    pub fn to_json_value(&self) -> serde_json::Value {
        match self {
            Value::U64(v) => serde_json::Value::from(*v),
            Value::F64(v) => serde_json::json!(*v),
            Value::Bool(v) => serde_json::Value::from(*v),
            Value::Str(v) => serde_json::Value::from(&**v),
            Value::Bytes(v) => serde_json::Value::from(v.iter().map(|b| format!("{b:02x}")).collect::<String>()),
            Value::Json(v) => (**v).clone(),
            Value::Ext(v) => v.to_json_value(),
            Value::Handle(_) => serde_json::Value::Null,
            Value::VecF32(arc) => serde_json::Value::Array(
                arc.iter().map(|f| serde_json::json!(*f)).collect()
            ),
            Value::VecI32(arc) => serde_json::Value::Array(
                arc.iter().map(|i| serde_json::Value::from(*i)).collect()
            ),
            Value::VecF64(arc) => serde_json::Value::Array(
                arc.iter().map(|f| serde_json::json!(*f)).collect()
            ),
            Value::VecI64(arc) => serde_json::Value::Array(
                arc.iter().map(|i| serde_json::Value::from(*i)).collect()
            ),
            Value::VecF16(arc) => serde_json::Value::Array(
                arc.iter().map(|f| serde_json::json!(f.to_f32())).collect()
            ),
            Value::VecI16(arc) => serde_json::Value::Array(
                arc.iter().map(|i| serde_json::Value::from(*i as i32)).collect()
            ),
            Value::None => serde_json::Value::Null,
        }
    }
}

/// Compile-time type tag for a port on a Polydat node.
///
/// **Narrow types and runtime storage:**
///
/// `PortType` includes narrow integer and float variants (U32, I32,
/// I64, F32) that have no corresponding `Value` variant. At runtime,
/// narrow values are stored inside `Value::U64` (for integers) or
/// `Value::F64` (for f32), with the assumption that the bits fit:
///
/// - `u32` → zero-extended in `Value::U64`
/// - `i32` → sign-extended or bit-reinterpreted in `Value::U64`
/// - `i64` → bit-reinterpreted in `Value::U64`
/// - `f32` → losslessly widened in `Value::F64`
///
/// The narrow `PortType` variants exist for compile-time type
/// checking and auto-adapter insertion (`U32ToU64`, `F32ToF64`).
/// P2/P3 compiled kernels use flat u64 buffers where this packing
/// is natural. The `Value` enum stays small — no combinatorial
/// explosion of narrow variant types.
///
/// Every input and output port declares its `PortType`. The assembler
/// uses these to validate wiring and auto-insert type adapters (e.g.,
/// `u64 → f64` widening). At runtime, the corresponding [`Value`]
/// variant is used.
///
/// **Widening rules** (auto-inserted by the assembler):
/// - `U32 → U64`, `I32 → I64`, `F32 → F64` (lossless widening)
/// - `U64 → F64` (lossless for values < 2^53)
/// - `Bool → U64` (true=1, false=0)
/// - Any type → `Str` (via display conversion)
///
/// **Narrowing** is never implicit — use explicit cast functions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PortType {
    /// 64-bit unsigned integer. The primary numeric type.
    U64,
    /// 64-bit IEEE 754 float. Used for math, distributions, noise.
    F64,
    /// 32-bit unsigned integer. Widens to U64 automatically.
    U32,
    /// 32-bit signed integer. Widens to I64 automatically.
    I32,
    /// 64-bit signed integer.
    I64,
    /// 32-bit IEEE 754 float. Widens to F64 automatically.
    F32,
    /// Boolean (true/false). Widens to U64 (1/0).
    Bool,
    /// Heap-allocated string. Any type auto-converts to Str.
    Str,
    /// Raw byte buffer.
    Bytes,
    /// Structured JSON value.
    Json,
    /// Adapter-contributed reflected type (e.g., CQL UUID).
    Ext,
    /// Type-erased Arc handle to a resolved resource (dataset,
    /// prepared statement, ...). The producer node populates an
    /// `Arc<dyn Any + Send + Sync>`; the consumer node downcasts to
    /// the concrete type via `Value::as_handle::<T>()`.
    Handle,
    /// Typed `f32` vector slice (`Arc<[f32]>`). Bound natively by
    /// adapters that understand `[f32]` (CQL `vector<float, N>`).
    VecF32,
    /// Typed `i32` vector slice (`Arc<[i32]>`).
    VecI32,
    /// Typed `f64` vector slice (`Arc<[f64]>`). Bound natively
    /// for CQL `vector<double, N>`.
    VecF64,
    /// Typed `i64` vector slice (`Arc<[i64]>`). Bound natively
    /// for CQL `vector<bigint, N>`.
    VecI64,
    /// Typed half-precision float vector (`Arc<[half::f16]>`).
    /// Bound natively for CQL `vector<half_float, N>`-style
    /// columns; stays at f16 on the wire so embeddings stored
    /// as 16-bit floats don't widen to f32 at the boundary.
    VecF16,
    /// Typed `i16` vector slice (`Arc<[i16]>`). Bound natively
    /// for CQL `vector<smallint, N>`.
    VecI16,
}

impl fmt::Display for PortType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PortType::U64 => write!(f, "u64"),
            PortType::F64 => write!(f, "f64"),
            PortType::U32 => write!(f, "u32"),
            PortType::I32 => write!(f, "i32"),
            PortType::I64 => write!(f, "i64"),
            PortType::F32 => write!(f, "f32"),
            PortType::Bool => write!(f, "bool"),
            PortType::Str => write!(f, "String"),
            PortType::Bytes => write!(f, "bytes"),
            PortType::Json => write!(f, "json"),
            PortType::Ext => write!(f, "ext"),
            PortType::Handle => write!(f, "handle"),
            PortType::VecF32 => write!(f, "vec_f32"),
            PortType::VecI32 => write!(f, "vec_i32"),
            PortType::VecF64 => write!(f, "vec_f64"),
            PortType::VecI64 => write!(f, "vec_i64"),
            PortType::VecF16 => write!(f, "vec_f16"),
            PortType::VecI16 => write!(f, "vec_i16"),
        }
    }
}

impl PortType {
    /// The canonical lowercase keyword for this `PortType`.
    ///
    /// This is the single source of truth for the str↔PortType
    /// mapping used by every synthesizer and parser in the
    /// workspace — synthesized polydat source (`extern <name>:
    /// <keyword>`), the workload-author `{name:<keyword>}` lvalue
    /// spec, and reverse parsing via [`Self::from_keyword`].
    /// Inverse of [`Self::from_keyword`].
    ///
    /// Exhaustive over the enum — adding a new `PortType` variant
    /// is a compile error here, forcing the addition of its
    /// canonical keyword and the round-trip closure to update.
    pub fn to_keyword(&self) -> &'static str {
        match self {
            Self::U64    => "u64",
            Self::F64    => "f64",
            Self::U32    => "u32",
            Self::I32    => "i32",
            Self::I64    => "i64",
            Self::F32    => "f32",
            Self::Bool   => "bool",
            Self::Str    => "str",
            Self::Bytes  => "bytes",
            Self::Json   => "json",
            Self::Ext    => "ext",
            Self::Handle => "handle",
            Self::VecF32 => "vec_f32",
            Self::VecI32 => "vec_i32",
            Self::VecF64 => "vec_f64",
            Self::VecI64 => "vec_i64",
            Self::VecF16 => "vec_f16",
            Self::VecI16 => "vec_i16",
        }
    }

    /// Parse a polydat type keyword into a `PortType`.
    ///
    /// Inverse of [`Self::to_keyword`]: accepts every keyword
    /// that `to_keyword` emits, plus a small set of legacy aliases
    /// (`"String"`, `"Json"`, `"Ext"`) that survive in older
    /// hand-written workload source. Returns `None` for any
    /// unrecognized keyword so callers can surface a loud
    /// diagnostic rather than silently coercing to a default.
    ///
    /// Used by the DSL `extern <name>: <keyword>` parser
    /// (`polydat/src/dsl/compile.rs`). Round-trips cleanly with
    /// any source `to_keyword` emits.
    pub fn from_keyword(name: &str) -> Option<Self> {
        match name {
            "u64"                 => Some(Self::U64),
            "f64"                 => Some(Self::F64),
            "u32"                 => Some(Self::U32),
            "i32"                 => Some(Self::I32),
            "i64"                 => Some(Self::I64),
            "f32"                 => Some(Self::F32),
            "bool"                => Some(Self::Bool),
            "str" | "Str" | "String" => Some(Self::Str),
            "bytes"               => Some(Self::Bytes),
            "json" | "Json"       => Some(Self::Json),
            "ext"  | "Ext"        => Some(Self::Ext),
            "handle"              => Some(Self::Handle),
            "vec_f32"             => Some(Self::VecF32),
            "vec_i32"             => Some(Self::VecI32),
            "vec_f64"             => Some(Self::VecF64),
            "vec_i64"             => Some(Self::VecI64),
            "vec_f16"             => Some(Self::VecF16),
            "vec_i16"             => Some(Self::VecI16),
            _ => None,
        }
    }

    /// Workload-author-facing parser for the `{name:<keyword>}`
    /// lvalue-spec surface. Strict subset of [`Self::from_keyword`]
    /// — `handle`, `ext`, and `none` are rejected because they're
    /// internal-only types a workload author should never assert.
    ///
    /// Returns `None` for any unrecognized name; the caller
    /// surfaces the unknown spec as a workload-shape diagnostic.
    pub fn from_workload_name(name: &str) -> Option<Self> {
        match Self::from_keyword(name)? {
            Self::Handle | Self::Ext => None,
            pt => Some(pt),
        }
    }
}

/// The lifecycle of a port's value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Lifecycle {
    /// Cycle-time: value changes per evaluation.
    Cycle,
    /// Init-time: value is frozen at assembly, immutable at runtime.
    /// Wiring a cycle-time value to an init port is an assembly error.
    Init,
}

/// Cost class for an input wire, indicating how expensive it is
/// to change the value on this port.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WireCost {
    /// Data wire: cheap per-cycle input. The node's primary
    /// computation path. Default for most ports.
    #[default]
    Data,
    /// Config wire: changing this input invalidates expensive
    /// internal state (LUT, distribution table). Expected to be
    /// wired to init-time constants or rarely-changing values.
    /// The compiler warns when a config wire connects to a
    /// cycle-time binding.
    Config,
}

/// Descriptor for a single input or output port on a node.
#[derive(Debug, Clone)]
pub struct Port {
    pub name: String,
    pub typ: PortType,
    pub lifecycle: Lifecycle,
    /// Cost class for input ports. Ignored for output ports.
    pub wire_cost: WireCost,
    /// Optional value contract this wire must satisfy at runtime
    /// (SRD 15 §"Strict Wire Mode"). The compiler uses this to
    /// decide whether to auto-insert a value assertion when the
    /// upstream source can't statically be proven to deliver a
    /// satisfying value. `None` = no constraint declared.
    ///
    /// Constraints reuse the same vocabulary as
    /// [`crate::dsl::const_constraints::ConstConstraint`] — the
    /// difference is just where the value comes from (a literal
    /// for `ConstU64`, a wire for `Slot::Wire`).
    pub constraint: Option<crate::dsl::const_constraints::ConstConstraint>,
}

impl Port {
    pub fn new(name: impl Into<String>, typ: PortType) -> Self {
        Self {
            name: name.into(),
            typ,
            lifecycle: Lifecycle::Cycle,
            wire_cost: WireCost::Data,
            constraint: None,
        }
    }

    /// Create a port with explicit lifecycle.
    pub fn with_lifecycle(name: impl Into<String>, typ: PortType, lifecycle: Lifecycle) -> Self {
        Self {
            name: name.into(),
            typ,
            lifecycle,
            wire_cost: WireCost::Data,
            constraint: None,
        }
    }

    pub fn u64(name: impl Into<String>) -> Self {
        Self::new(name, PortType::U64)
    }

    pub fn f64(name: impl Into<String>) -> Self {
        Self::new(name, PortType::F64)
    }

    pub fn str(name: impl Into<String>) -> Self {
        Self::new(name, PortType::Str)
    }

    pub fn bool(name: impl Into<String>) -> Self {
        Self::new(name, PortType::Bool)
    }

    pub fn json(name: impl Into<String>) -> Self {
        Self::new(name, PortType::Json)
    }

    pub fn handle(name: impl Into<String>) -> Self {
        Self::new(name, PortType::Handle)
    }

    pub fn vec_f32(name: impl Into<String>) -> Self {
        Self::new(name, PortType::VecF32)
    }

    pub fn vec_i32(name: impl Into<String>) -> Self {
        Self::new(name, PortType::VecI32)
    }

    /// Create an init-time port (frozen at assembly).
    pub fn init(name: impl Into<String>, typ: PortType) -> Self {
        Self::with_lifecycle(name, typ, Lifecycle::Init)
    }

    /// Attach a value constraint. Used by node authors that want
    /// to declare "this wire must satisfy X" so strict-wire-mode
    /// can auto-insert the right value assertion. See SRD 15
    /// §"Strict Wire Mode".
    pub fn with_constraint(mut self, c: crate::dsl::const_constraints::ConstConstraint) -> Self {
        self.constraint = Some(c);
        self
    }

    /// Mark this port as a config wire (expensive to change).
    pub fn config(mut self) -> Self {
        self.wire_cost = WireCost::Config;
        self
    }
}

// ---------------------------------------------------------------------------
// Unified slot model (SRD 36 §Variadic)
// ---------------------------------------------------------------------------

/// The type discriminant for a slot: wire or typed constant.
///
/// This is the shared vocabulary between `FuncSig` (static registry)
/// and `NodeMeta` (owned instance). It replaces the former `ParamKind`,
/// `ConstType`, and `SlotKind` enums with a single type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SlotType {
    /// A runtime wire input carrying a value each cycle.
    Wire,
    /// A u64 constant literal.
    ConstU64,
    /// An f64 constant literal.
    ConstF64,
    /// A string constant literal.
    ConstStr,
    /// A `Vec<u64>` constant (from array literal).
    ConstVecU64,
    /// A `Vec<f64>` constant (from array literal).
    ConstVecF64,
}

impl SlotType {
    /// Whether this is a constant (not a wire).
    pub fn is_const(self) -> bool {
        !matches!(self, SlotType::Wire)
    }

    /// Whether this is a wire (not a constant).
    pub fn is_wire(self) -> bool {
        matches!(self, SlotType::Wire)
    }
}

/// A concrete constant value stored in node metadata.
///
/// Assembly-time values baked into the node at construction. The
/// variant determines the `SlotType` — no separate type discriminant
/// is needed.
#[derive(Debug, Clone, PartialEq)]
pub enum ConstValue {
    U64(u64),
    F64(f64),
    Str(String),
    VecU64(Vec<u64>),
    VecF64(Vec<f64>),
}

impl ConstValue {
    /// Return the `SlotType` for this value.
    pub fn slot_type(&self) -> SlotType {
        match self {
            ConstValue::U64(_) => SlotType::ConstU64,
            ConstValue::F64(_) => SlotType::ConstF64,
            ConstValue::Str(_) => SlotType::ConstStr,
            ConstValue::VecU64(_) => SlotType::ConstVecU64,
            ConstValue::VecF64(_) => SlotType::ConstVecF64,
        }
    }

    /// Encode to the JIT's u64 representation.
    pub fn to_jit_u64s(&self) -> Vec<u64> {
        match self {
            ConstValue::U64(v) => vec![*v],
            ConstValue::F64(v) => vec![v.to_bits()],
            ConstValue::Str(_) => vec![],
            ConstValue::VecU64(v) => v.clone(),
            ConstValue::VecF64(v) => v.iter().map(|f| f.to_bits()).collect(),
        }
    }
}

/// A single logical input to a node: either a runtime wire or an
/// assembly-time constant. The positional order in `NodeMeta.slots`
/// matches the function call syntax in the DSL.
#[derive(Debug, Clone)]
pub enum Slot {
    /// A runtime wire input carrying a value each cycle.
    Wire(Port),
    /// An assembly-time constant, baked into the node at construction.
    Const {
        name: String,
        value: ConstValue,
    },
}

impl Slot {
    /// Return the `SlotType` discriminant for this slot.
    pub fn slot_type(&self) -> SlotType {
        match self {
            Slot::Wire(_) => SlotType::Wire,
            Slot::Const { value, .. } => value.slot_type(),
        }
    }

    /// Create a wire slot.
    pub fn wire(port: Port) -> Self { Slot::Wire(port) }

    /// Create a u64 constant slot.
    pub fn const_u64(name: impl Into<String>, v: u64) -> Self {
        Slot::Const { name: name.into(), value: ConstValue::U64(v) }
    }

    /// Create an f64 constant slot.
    pub fn const_f64(name: impl Into<String>, v: f64) -> Self {
        Slot::Const { name: name.into(), value: ConstValue::F64(v) }
    }

    /// Create a string constant slot.
    pub fn const_str(name: impl Into<String>, v: impl Into<String>) -> Self {
        Slot::Const { name: name.into(), value: ConstValue::Str(v.into()) }
    }

    /// Create a `Vec<u64>` constant slot.
    pub fn const_vec_u64(name: impl Into<String>, v: Vec<u64>) -> Self {
        Slot::Const { name: name.into(), value: ConstValue::VecU64(v) }
    }

    /// Create a `Vec<f64>` constant slot.
    pub fn const_vec_f64(name: impl Into<String>, v: Vec<f64>) -> Self {
        Slot::Const { name: name.into(), value: ConstValue::VecF64(v) }
    }
}

/// Declares which inputs of a node are interchangeable.
///
/// Used by the fusion pattern matcher to recognize equivalent
/// subgraphs regardless of operand order, and by future passes
/// (e.g., canonical ordering, common subexpression elimination).
#[derive(Debug, Clone, PartialEq, Eq)]
#[derive(Default)]
pub enum Commutativity {
    /// Input order matters. No permutations attempted during
    /// pattern matching. This is the default for unary nodes and
    /// any node where operand order affects the result.
    ///
    /// Examples: `mod(dividend, divisor)`, `div(x, K)`,
    /// `concat(left, right)`, `sub(a, b)`.
    #[default]
    Positional,

    /// All inputs are interchangeable, including variadic.
    /// For small arity (2-3), the matcher tries all permutations.
    /// For larger arity, it uses set-matching.
    ///
    /// Examples: `sum(a, b, ..., n)`, `product(a, b, ..., n)`,
    /// `min(a, b, ..., n)`, `max(a, b, ..., n)`.
    AllCommutative,

    /// Specific groups of input port indices are interchangeable
    /// within each group. Inputs not listed in any group are
    /// positional.
    ///
    /// Example: `fma(x, y, z) = x + y * z`
    /// The multiplicands `y` (index 1) and `z` (index 2) commute,
    /// but the addend `x` (index 0) does not.
    /// `Groups(vec![vec![1, 2]])`
    Groups(Vec<Vec<usize>>),
}


/// Metadata describing a node's interface: its input slots and output ports.
///
/// Generated per-node-type and queryable at runtime for assembly-time
/// validation, compilation, optimization passes, and describe output.
///
/// Wire inputs are `Slot::Wire(Port)`. Constants are `Slot::Const { name, value }`.
/// Use `wire_inputs()` to extract just the wire ports.
#[derive(Debug, Clone)]
pub struct NodeMeta {
    pub name: String,
    /// All inputs in positional order: wires and constants.
    pub ins: Vec<Slot>,
    pub outs: Vec<Port>,
}

impl NodeMeta {
    /// Wire-only input ports extracted from `ins`.
    pub fn wire_inputs(&self) -> Vec<&Port> {
        self.ins.iter().filter_map(|s| match s {
            Slot::Wire(p) => Some(p),
            Slot::Const { .. } => None,
        }).collect()
    }

    /// Constant names and values extracted from `ins`.
    pub fn const_slots(&self) -> Vec<(&str, &ConstValue)> {
        self.ins.iter().filter_map(|s| match s {
            Slot::Const { name, value } => Some((name.as_str(), value)),
            Slot::Wire(_) => None,
        }).collect()
    }

    /// Encode all constants from `ins` to JIT u64 representation.
    pub fn jit_constants_from_slots(&self) -> Vec<u64> {
        self.const_slots().iter()
            .flat_map(|(_, v)| v.to_jit_u64s())
            .collect()
    }
}

/// A compiled u64-only evaluation step.
///
/// The closure captures all assembly-time parameters. At runtime it
/// reads from input slots and writes to output slots in a flat `[u64]`
/// buffer — no `Value` enum, no virtual dispatch.
pub type CompiledU64Op = Box<dyn Fn(&[u64], &mut [u64]) + Send + Sync>;

/// Per-node purity classification per
/// [`runtime_model.md`'s D2 axiom][spec] and
/// [`composition_substrate.md`'s T1+T2 axioms][substrate].
///
/// Every node declares its purity status via
/// [`PolydatNode::purity`]. The default is [`Purity::Pure`]; nodes
/// with observable side channels (logging, file I/O, network)
/// or eval-call-spanning state override to declare
/// [`Purity::SideChannel`] or [`Purity::Nondeterministic`].
///
/// **D1 (Typed Return Determinism) holds for every purity
/// class.** The slot contract carries only typed return
/// values; impure nodes still produce typed-deterministic
/// returns. What varies between purity classes is the
/// *observable side channels* (D2): pure nodes have none;
/// SideChannel nodes have declared side channels; Stateful
/// nodes additionally have internal eval-call-spanning state
/// that affects future evaluations.
///
/// [spec]: ../docs/design/runtime_model.md
/// [substrate]: ../docs/design/composition_substrate.md
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Purity {
    /// Pure function — `eval(inputs)` is a function of inputs,
    /// no observable side effects, byte-identical determinism
    /// across calls with identical inputs.
    Pure,

    /// Has an observable side channel (logging, file I/O,
    /// network, etc.) but the typed return value is still a
    /// function of inputs. Hosts that care about side-channel
    /// observability examine the `sink` to know what
    /// observable surface this node writes to.
    SideChannel { sink: SideChannelSink },

    /// The typed return value is not a function of declared
    /// inputs alone — it depends on external sources (system
    /// clock, entropy, thread identity, environment) or on
    /// eval-call-spanning internal state mutated by prior calls.
    /// In either case, the runtime's `node_clean` caching model
    /// must opt the node out of within-cycle memoization
    /// suppression; the lifecycle classifier marks the node as
    /// nondeterministic per `kernel/engines.rs::nondeterministic_nodes`.
    /// The `reason` string documents the source of
    /// non-determinism (e.g., "reads system clock",
    /// "monotonic counter incremented per call",
    /// "accumulates signal buffer across calls").
    ///
    /// This is the intrinsic-volatility marker referenced by
    /// runtime_model.md R1.v: certain library nodes declare
    /// themselves volatile via this variant; no user opt-in is
    /// required, and the workload author cannot remove the
    /// marker. User-opt-in volatility via the `volatile`
    /// modifier is a separate surface that produces the same
    /// runtime effect (see R1.v).
    Nondeterministic { reason: &'static str },
}

/// Where a [`Purity::SideChannel`] node writes its observable
/// side effects. Hosts reasoning about side-channel
/// determinism (D2) pattern-match on this to know what
/// observable surface to expect.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SideChannelSink {
    /// Writes to the process's stderr.
    Stderr,
    /// Writes to the process's stdout.
    Stdout,
    /// Writes to a log buffer (e.g. tracing/log crate sink).
    LogBuffer,
    /// Writes to a file path determined at construction time.
    File,
    /// Writes to a network endpoint determined at
    /// construction time.
    Network,
    /// Writes to an observable surface not covered by the
    /// other variants. The host should consult the node's
    /// documentation for the specific contract.
    Other,
}

/// Runtime evaluation interface for a Polydat node.
///
/// Phase 1: called via `dyn PolydatNode` (dynamic dispatch with `Value` enum).
/// Phase 2: if all nodes in the DAG are u64-only and provide a
/// `compiled_u64` implementation, the assembly phase compiles the DAG
/// into a flat buffer evaluator with direct function calls.
pub trait PolydatNode: Send + Sync {
    /// Return this node's metadata (port names and types).
    fn meta(&self) -> &NodeMeta;

    /// Evaluate the node: read from `inputs`, write to `outputs`.
    ///
    /// The assembly phase guarantees that `inputs` and `outputs` have
    /// the correct length and types matching `meta()`.
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]);

    /// Declare which inputs are interchangeable for this node.
    ///
    /// Override for commutative operations like `sum`, `product`,
    /// `min`, `max`. The default is `Positional` (order matters).
    fn commutativity(&self) -> Commutativity {
        Commutativity::Positional
    }

    /// True iff this node should receive `Value::None` inputs
    /// directly rather than have the kernel propagate None through
    /// it. Default: false — most nodes follow SRD-74 Rule 1
    /// (None in → None out, no eval invocation).
    ///
    /// Override to true for nodes whose semantics explicitly
    /// consume None: coalesce-style fallbacks (`default_or`),
    /// optional/maybe handlers, anything that distinguishes
    /// "present" from "absent" as part of its contract.
    /// Override-true nodes are responsible for handling
    /// `Value::None` in their own `eval` implementation.
    ///
    /// See [SRD 74](../../../docs/sysref/74_none_propagation.md)
    /// §"Rule 1 — String-interpolation propagates None" — the
    /// rule is general (lifted to the kernel level) rather than
    /// per-node; this flag is the opt-out for legitimate None-
    /// aware operators.
    fn accepts_none_inputs(&self) -> bool {
        false
    }

    /// Return a compiled u64-only evaluation closure, if this node
    /// operates entirely in u64 space.
    ///
    /// The closure reads from an input slice and writes to an output
    /// slice, both `&[u64]` / `&mut [u64]`. Assembly-time parameters
    /// are captured in the closure.
    ///
    /// Return `None` if the node has non-u64 ports or cannot be
    /// compiled. The assembly phase will fall back to Phase 1.
    fn compiled_u64(&self) -> Option<CompiledU64Op> {
        None
    }

    /// Return assembly-time constants for JIT compilation.
    ///
    /// Nodes with baked-in constants (Mod's modulus, Add's addend, etc.)
    /// override this to expose their constants to the JIT compiler.
    /// Returns a list of u64 constants in the order the JIT expects.
    ///
    /// Default: empty (no constants to expose).
    fn jit_constants(&self) -> Vec<u64> {
        Vec::new()
    }

    /// Declare this node's purity status per the
    /// [`runtime_model.md`'s D2 axiom][spec]. Default:
    /// [`Purity::Pure`]. Override to declare an observable
    /// side channel ([`Purity::SideChannel`]) or
    /// eval-call-spanning state ([`Purity::Nondeterministic`]).
    ///
    /// **What this affects:**
    ///
    /// - The runtime's `node_clean` cache (R1) holds for
    ///   `Purity::Pure` and `Purity::SideChannel`. The
    ///   typed return value is cached after one eval;
    ///   subsequent pulls with identical inputs reuse the
    ///   cache. For `SideChannel` nodes, this means the
    ///   side channel fires once per dirty-to-clean
    ///   transition (not on every pull).
    /// - `Purity::Nondeterministic` nodes opt out of `node_clean`
    ///   caching at the construction tier (assembly marks
    ///   them as nondeterministic per
    ///   `kernel/engines.rs::nondeterministic_nodes`).
    /// - Hosts inspecting an expression's determinism
    ///   profile via D2 read this declaration to know
    ///   whether the constituent node has side channels.
    ///
    /// Default: `Purity::Pure`. Most nodes are pure
    /// functions over their inputs.
    ///
    /// [spec]: ../docs/design/runtime_model.md
    fn purity(&self) -> Purity {
        Purity::Pure
    }
}

/// Determine the compile level of a node (works on trait objects).
pub fn compile_level_of(node: &dyn PolydatNode) -> CompileLevel {
    #[cfg(feature = "jit")]
    {
        let jit_op = crate::compile::jit::classify_node(node);
        if !matches!(jit_op, crate::compile::jit::JitOp::Fallback) {
            return CompileLevel::Phase3;
        }
    }

    if node.compiled_u64().is_some() {
        CompileLevel::Phase2
    } else {
        CompileLevel::Phase1
    }
}

/// The maximum compilation level a node supports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompileLevel {
    /// Runtime interpreter: `dyn PolydatNode` + `Value` enum.
    Phase1,
    /// Compiled closure: `Box<dyn Fn(&[u64], &mut [u64])>`.
    Phase2,
    /// JIT native code via Cranelift.
    Phase3,
}

#[cfg(test)]
mod purity_tests {
    use super::*;

    /// A minimal pure node — defaults to `Purity::Pure` via
    /// the trait default impl.
    struct DefaultPureNode {
        meta: NodeMeta,
    }

    impl PolydatNode for DefaultPureNode {
        fn meta(&self) -> &NodeMeta { &self.meta }
        fn eval(&self, _inputs: &[Value], outputs: &mut [Value]) {
            outputs[0] = Value::U64(42);
        }
    }

    /// A node that explicitly declares a side channel.
    struct SideChannelNode {
        meta: NodeMeta,
    }

    impl PolydatNode for SideChannelNode {
        fn meta(&self) -> &NodeMeta { &self.meta }
        fn eval(&self, _inputs: &[Value], _outputs: &mut [Value]) {}
        fn purity(&self) -> Purity {
            Purity::SideChannel { sink: SideChannelSink::Stderr }
        }
    }

    /// A node that explicitly declares stateful behaviour.
    struct StatefulNode {
        meta: NodeMeta,
    }

    impl PolydatNode for StatefulNode {
        fn meta(&self) -> &NodeMeta { &self.meta }
        fn eval(&self, _inputs: &[Value], _outputs: &mut [Value]) {}
        fn purity(&self) -> Purity {
            Purity::Nondeterministic { reason: "test fixture" }
        }
    }

    fn empty_meta() -> NodeMeta {
        NodeMeta {
            name: "test".into(),
            ins: vec![],
            outs: vec![Port::u64("out")],
        }
    }

    #[test]
    fn default_purity_is_pure() {
        let n = DefaultPureNode { meta: empty_meta() };
        assert_eq!(n.purity(), Purity::Pure);
    }

    #[test]
    fn side_channel_declaration_is_observable() {
        let n = SideChannelNode { meta: empty_meta() };
        match n.purity() {
            Purity::SideChannel { sink } => assert_eq!(sink, SideChannelSink::Stderr),
            other => panic!("expected SideChannel, got {other:?}"),
        }
    }

    #[test]
    fn stateful_declaration_is_observable() {
        let n = StatefulNode { meta: empty_meta() };
        match n.purity() {
            Purity::Nondeterministic { reason } => assert_eq!(reason, "test fixture"),
            other => panic!("expected Stateful, got {other:?}"),
        }
    }

    #[test]
    fn inspect_node_declares_stderr_side_channel() {
        let n = crate::library::diagnostic::Inspect::new(PortType::U64, "x".to_string());
        match n.purity() {
            Purity::SideChannel { sink } => assert_eq!(sink, SideChannelSink::Stderr),
            other => panic!("inspect should declare Stderr SideChannel, got {other:?}"),
        }
    }

    #[test]
    fn log_passthrough_declares_log_buffer_side_channel() {
        let n = crate::library::log_levels::LogInfo::new(PortType::U64);
        match n.purity() {
            Purity::SideChannel { sink } => assert_eq!(sink, SideChannelSink::LogBuffer),
            other => panic!("log_passthrough should declare LogBuffer SideChannel, got {other:?}"),
        }
    }
}
