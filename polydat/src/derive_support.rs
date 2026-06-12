// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Trait surface that the `#[polydat_node]` proc-macro
//! (`polydat-derive`) calls into for boxing / unboxing wire
//! values.
//!
//! ## Canonical trait surface (SRD-80b)
//!
//! - [`Wire`] — `Sized + 'static` Rust-type ↔ [`Value`] bridge.
//!   Owned types only; the macro recognises borrow shapes
//!   (`&str`, `&[u8]`, `&[T]`, `&serde_json::Value`)
//!   syntactically and emits direct `match`-on-`Value`
//!   extraction at the eval call site — no trait dispatch, no
//!   `unsafe` lifetime transmute.
//! - [`ConstSource`] — `Sized + 'static` typed-extraction from
//!   [`ConstArg`] for owned `Const<T>` positions.
//!
//! Combinator [`Wire`] impls cover [`Option<T>`] (`None`-aware
//! pass-through) and [`Ext<T>`] (downcast through
//! [`ReflectedValue`]); `ConstSource for Vec<C: ConstSource>`
//! handles workload-list constants.
//!
//! ## Why the trait surface lives here
//!
//! `polydat-derive` is a proc-macro crate — it can't define
//! traits that are visible at the call site, only emit token
//! streams referencing traits defined elsewhere. The macro
//! emits `<T as polydat::derive_support::Wire>::extract(...)`
//! and `<T as polydat::derive_support::ConstSource>::extract(...)`
//! paths; this module is what those paths resolve to.

use std::sync::Arc;

use crate::ast::{JitType, PortType, ReflectedValue, SliceArc, SlotType, Value};
use crate::dsl::factory::ConstArg;

// =====================================================================
// Wire — Rust-type ↔ Value bridge (owned types only)
// =====================================================================

/// Rust-type ↔ `Value` bridge.
///
/// Every owned Rust type the macro accepts in a wire position
/// implements this trait. `PORT` is the static [`PortType`] the
/// DSL type-checker uses to route a wire to this slot; `JIT`
/// tags the type as ridable on the Phase-2 `u64` buffer (or
/// `None` if it stays on the Phase-1 typed-eval path).
///
/// Borrow shapes (`&str`, `&[u8]`, `&[T]`,
/// `&serde_json::Value`) and polymorphic `Value`-typed wires
/// are NOT covered here — the macro recognises them
/// syntactically and emits direct `match`-on-`Value` extraction
/// at the eval call site. This keeps the trait surface free of
/// lifetime parameters.
///
/// `extract` panics on type mismatch — the DSL type-checker is
/// responsible for routing well-typed `Value`s to each slot
/// before `eval` runs. A panic here is a "type-checker was
/// lied to" bug, not a normal path.
pub trait Wire: Sized + 'static {
    /// Static port type for the DSL type-checker.
    const PORT: PortType;

    /// JIT carrier classification. `Some(_)` means the type
    /// rides the Phase-2 `u64` buffer; `None` means typed-eval
    /// only.
    const JIT: Option<JitType>;

    /// SRD-53 §"Source-string call-site sugar" — auto-resolver
    /// for `Str`-typed upstream wires feeding this slot. `None`
    /// (the default) disables auto-promotion; the workload must
    /// supply the wire's actual port type directly. Set via the
    /// [`Resolved<R, T>`] marker wrapper.
    const RESOLVER: Option<crate::dsl::registry::DefaultResolver> = None;

    /// SRD-15 §"WireCost::Config" — cost class for this wire.
    /// Defaults to [`WireCost::Data`] (cheap per-cycle input).
    /// Set to [`WireCost::Config`] via the [`Config<T>`] marker
    /// wrapper to signal that the wire is rarely-changing and
    /// the compiler should warn on cycle-time binding.
    const WIRE_COST: crate::ast::WireCost = crate::ast::WireCost::Data;

    /// Pull a typed value out of a `Value` wire.
    fn extract(v: &Value) -> Self;

    /// Push a typed value back into the `Value` outputs stream.
    fn inject(self) -> Value;
}

// ── Scalar primitives ─────────────────────────────────────────

impl Wire for u64 {
    const PORT: PortType = PortType::U64;
    const JIT: Option<JitType> = Some(JitType::U64);
    fn extract(v: &Value) -> Self { v.as_u64() }
    fn inject(self) -> Value { Value::U64(self) }
}

impl Wire for u32 {
    const PORT: PortType = PortType::U32;
    const JIT: Option<JitType> = Some(JitType::U64);
    fn extract(v: &Value) -> Self { v.as_u64() as u32 }
    fn inject(self) -> Value { Value::U64(self as u64) }
}

impl Wire for i32 {
    const PORT: PortType = PortType::I32;
    const JIT: Option<JitType> = Some(JitType::I64);
    // Lenient extract: honest `Value::I64` (sign-extended I32
    // storage convention) plus the legacy bit-stuffed `Value::U64`
    // form during the alignment migration — same precedent as
    // `Wire<bool>` accepting `U64(n != 0)`.
    fn extract(v: &Value) -> Self { v.as_i64() as i32 }
    fn inject(self) -> Value { Value::I64(self as i64) }
}

impl Wire for i64 {
    const PORT: PortType = PortType::I64;
    const JIT: Option<JitType> = Some(JitType::I64);
    // Lenient extract: see `Wire<i32>` note above.
    fn extract(v: &Value) -> Self { v.as_i64() }
    fn inject(self) -> Value { Value::I64(self) }
}

impl Wire for u8 {
    const PORT: PortType = PortType::U8;
    const JIT: Option<JitType> = Some(JitType::U64);
    fn extract(v: &Value) -> Self { v.as_u64() as u8 }
    fn inject(self) -> Value { Value::U64(self as u64) }
}

impl Wire for u16 {
    const PORT: PortType = PortType::U16;
    const JIT: Option<JitType> = Some(JitType::U64);
    fn extract(v: &Value) -> Self { v.as_u64() as u16 }
    fn inject(self) -> Value { Value::U64(self as u64) }
}

impl Wire for i8 {
    const PORT: PortType = PortType::I8;
    const JIT: Option<JitType> = Some(JitType::I64);
    // Lenient extract through as_i64 (honest I64 or legacy
    // stuffed U64), narrowed by truncation — sign survives
    // because the storage convention is sign-extension.
    fn extract(v: &Value) -> Self { v.as_i64() as i8 }
    fn inject(self) -> Value { Value::I64(self as i64) }
}

impl Wire for i16 {
    const PORT: PortType = PortType::I16;
    const JIT: Option<JitType> = Some(JitType::I64);
    fn extract(v: &Value) -> Self { v.as_i64() as i16 }
    fn inject(self) -> Value { Value::I64(self as i64) }
}

impl Wire for u128 {
    const PORT: PortType = PortType::U128;
    // Interpreter-only: a 128-bit value cannot ride the one-u64
    // JIT slot; the two-slot ABI is a Phase-5 concern
    // (type_system_alignment.md §8.1).
    const JIT: Option<JitType> = None;
    fn extract(v: &Value) -> Self { v.as_u128() }
    fn inject(self) -> Value { Value::U128(crate::ast::Bits128::from_u128(self)) }
}

impl Wire for i128 {
    const PORT: PortType = PortType::I128;
    const JIT: Option<JitType> = None;
    fn extract(v: &Value) -> Self { v.as_i128() }
    fn inject(self) -> Value { Value::I128(crate::ast::Bits128::from_i128(self)) }
}

impl Wire for f64 {
    const PORT: PortType = PortType::F64;
    const JIT: Option<JitType> = Some(JitType::F64);
    fn extract(v: &Value) -> Self { v.as_f64() }
    fn inject(self) -> Value { Value::F64(self) }
}

impl Wire for f32 {
    const PORT: PortType = PortType::F32;
    const JIT: Option<JitType> = Some(JitType::U64);
    fn extract(v: &Value) -> Self { f32::from_bits(v.as_u64() as u32) }
    fn inject(self) -> Value { Value::U64(self.to_bits() as u64) }
}

impl Wire for half::f16 {
    const PORT: PortType = PortType::F16;
    const JIT: Option<JitType> = Some(JitType::U64);
    // Same bit-stuffing convention as f32: the binary16 pattern
    // rides the low 16 bits of the u64 carrier.
    fn extract(v: &Value) -> Self { half::f16::from_bits(v.as_u64() as u16) }
    fn inject(self) -> Value { Value::U64(self.to_bits() as u64) }
}

impl Wire for bool {
    const PORT: PortType = PortType::Bool;
    const JIT: Option<JitType> = Some(JitType::Bool);
    fn extract(v: &Value) -> Self {
        match v {
            Value::Bool(b) => *b,
            Value::U64(n) => *n != 0,
            other => panic!(
                "Wire<bool>::extract: type-checker routed {other:?} \
                 to a Bool slot"),
        }
    }
    fn inject(self) -> Value { Value::Bool(self) }
}

impl Wire for String {
    const PORT: PortType = PortType::Str;
    const JIT: Option<JitType> = None;
    fn extract(v: &Value) -> Self {
        // SRD-80b: panic on shape mismatch — the type-checker is
        // responsible for routing well-typed values to each slot,
        // and a non-Str input here is a "type system was lied to"
        // bug, not a coercion opportunity. Nodes that want a
        // display rendering of an arbitrary `Value` take a
        // `Value`-typed (PolyWire) arg instead.
        match v {
            Value::Str(s) => s.to_string(),
            other => panic!(
                "Wire<String>::extract: expected Str, got {other:?}"),
        }
    }
    fn inject(self) -> Value { Value::Str(self.into()) }
}

/// `Arc<str>` — zero-copy shared string handle. Reading
/// extracts the existing `Arc<str>` from `Value::Str` (refcount
/// bump only); injecting wraps directly. Nodes whose hot path
/// emits the same string per cycle (lookup table outputs,
/// fixed-value selectors) should use this instead of `String`
/// to avoid the per-cycle `to_string()` allocation.
impl Wire for std::sync::Arc<str> {
    const PORT: PortType = PortType::Str;
    const JIT: Option<JitType> = None;
    fn extract(v: &Value) -> Self {
        match v {
            Value::Str(s) => s.clone(),
            other => panic!(
                "Wire<Arc<str>>::extract: expected Str, got {other:?}"),
        }
    }
    fn inject(self) -> Value { Value::Str(self) }
}

/// `Arc<dyn Any + Send + Sync>` — opaque Handle wire. The body
/// receives the runtime-typed handle directly; downcast is the
/// operator's responsibility. Use [`Resolved<R, T>`] when the
/// node wants a typed Handle with SRD-53 source-string
/// auto-promotion sugar; use this raw shape when the body
/// needs to handle multiple inner types via runtime dispatch.
impl Wire for std::sync::Arc<dyn std::any::Any + Send + Sync> {
    const PORT: PortType = PortType::Handle;
    const JIT: Option<JitType> = None;
    fn extract(v: &Value) -> Self {
        match v {
            Value::Handle(arc) => arc.clone(),
            other => panic!(
                "Wire<Arc<dyn Any>>::extract: expected Handle, got {other:?}"),
        }
    }
    fn inject(self) -> Value { Value::Handle(self) }
}

/// `Box<dyn ReflectedValue>` — Ext (adapter-typed) wire with
/// dynamic downcast left to the body. Use [`Ext<T>`] when the
/// inner type is known at codegen; use this when a node needs
/// to dispatch on the runtime ReflectedValue::type_name.
impl Wire for Box<dyn ReflectedValue> {
    const PORT: PortType = PortType::Ext;
    const JIT: Option<JitType> = None;
    fn extract(v: &Value) -> Self {
        match v {
            Value::Ext(b) => b.clone_reflected(),
            other => panic!(
                "Wire<Box<dyn ReflectedValue>>::extract: expected Ext, got {other:?}"),
        }
    }
    fn inject(self) -> Value { Value::Ext(self) }
}

// ── Bytes ──────────────────────────────────────────────────────

impl Wire for Arc<[u8]> {
    const PORT: PortType = PortType::Bytes;
    const JIT: Option<JitType> = None;
    fn extract(v: &Value) -> Self {
        match v {
            Value::Bytes(b) => b.clone(),
            other => panic!("Wire<Arc<[u8]>>::extract: expected Bytes, got {other:?}"),
        }
    }
    fn inject(self) -> Value { Value::Bytes(self) }
}

impl Wire for Vec<u8> {
    const PORT: PortType = PortType::Bytes;
    const JIT: Option<JitType> = None;
    fn extract(v: &Value) -> Self {
        match v {
            Value::Bytes(b) => b.to_vec(),
            other => panic!("Wire<Vec<u8>>::extract: expected Bytes, got {other:?}"),
        }
    }
    fn inject(self) -> Value { Value::Bytes(self.into()) }
}

// ── Json ───────────────────────────────────────────────────────

impl Wire for Arc<serde_json::Value> {
    const PORT: PortType = PortType::Json;
    const JIT: Option<JitType> = None;
    fn extract(v: &Value) -> Self {
        match v {
            Value::Json(j) => j.clone(),
            other => panic!("Wire<Arc<Json>>::extract: expected Json, got {other:?}"),
        }
    }
    fn inject(self) -> Value { Value::Json(self) }
}

// ── Typed-element vectors ──────────────────────────────────────

macro_rules! impl_wire_vec {
    ($elem:ty, $variant:ident, $port:ident) => {
        impl Wire for SliceArc<$elem> {
            const PORT: PortType = PortType::$port;
            const JIT: Option<JitType> = None;
            fn extract(v: &Value) -> Self {
                match v {
                    Value::$variant(arc) => arc.clone(),
                    other => panic!(
                        concat!("Wire<SliceArc<", stringify!($elem),
                                ">>::extract: expected ", stringify!($variant),
                                ", got {:?}"),
                        other),
                }
            }
            fn inject(self) -> Value { Value::$variant(self) }
        }

        impl Wire for Vec<$elem> {
            const PORT: PortType = PortType::$port;
            const JIT: Option<JitType> = None;
            fn extract(v: &Value) -> Self {
                match v {
                    Value::$variant(arc) => arc.as_slice().to_vec(),
                    other => panic!(
                        concat!("Wire<Vec<", stringify!($elem),
                                ">>::extract: expected ", stringify!($variant),
                                ", got {:?}"),
                        other),
                }
            }
            fn inject(self) -> Value {
                Value::$variant(SliceArc::from_vec(self))
            }
        }
    };
}

impl_wire_vec!(f32, VecF32, VecF32);
impl_wire_vec!(i32, VecI32, VecI32);
impl_wire_vec!(f64, VecF64, VecF64);
impl_wire_vec!(i64, VecI64, VecI64);
impl_wire_vec!(half::f16, VecF16, VecF16);
impl_wire_vec!(i16, VecI16, VecI16);
impl_wire_vec!(i8, VecI8, VecI8);

// ── Phase C combinators ────────────────────────────────────────

/// None-aware wire combinator. Macro auto-emits
/// `accepts_none_inputs() -> true` when any arg is `Option<_>`.
impl<T: Wire> Wire for Option<T> {
    const PORT: PortType = T::PORT;
    const JIT: Option<JitType> = None;
    fn extract(v: &Value) -> Self {
        match v {
            Value::None => None,
            _ => Some(T::extract(v)),
        }
    }
    fn inject(self) -> Value {
        match self {
            None => Value::None,
            Some(t) => t.inject(),
        }
    }
}

/// Operator-side wrapper for adapter-typed wire arguments.
/// `Ext<T>` signals "this arg comes from `Value::Ext(Box<dyn
/// ReflectedValue>)`; downcast it to `T`." Implements `Deref` /
/// `DerefMut` like [`Const<T>`] so the body can use `.method()`
/// directly.
pub struct Ext<T>(pub T);

impl<T> std::ops::Deref for Ext<T> {
    type Target = T;
    fn deref(&self) -> &T { &self.0 }
}

impl<T> std::ops::DerefMut for Ext<T> {
    fn deref_mut(&mut self) -> &mut T { &mut self.0 }
}

impl<T: ReflectedValue + Clone + 'static> Wire for Ext<T> {
    const PORT: PortType = PortType::Ext;
    const JIT: Option<JitType> = None;
    fn extract(v: &Value) -> Self {
        match v {
            Value::Ext(boxed) => {
                let any = boxed.as_any();
                match any.downcast_ref::<T>() {
                    Some(t) => Ext(t.clone()),
                    None => panic!(
                        "Wire<Ext<{}>>::extract: ReflectedValue downcast failed; \
                         got runtime type {:?}",
                        std::any::type_name::<T>(),
                        boxed.type_name()),
                }
            }
            other => panic!("Wire<Ext>::extract: expected Ext, got {other:?}"),
        }
    }
    fn inject(self) -> Value {
        Value::Ext(Box::new(self.0))
    }
}

// ── DynamicOutputs<T> — variable output port count ────────────

/// Marker wrapper for node return types whose output port
/// COUNT is determined at construction time from a
/// `Const<Vec<C>>` arg's length, not at codegen time.
/// SRD-80b shape extension covering nodes like `mixed_radix`
/// that emit one output per radix where `radix` count is a
/// workload-supplied list.
///
/// Operator writes:
///
/// ```ignore
/// #[polydat_node(category = Arithmetic)]
/// fn mixed_radix(
///     value: u64,
///     radixes: Const<Vec<u64>>,
/// ) -> DynamicOutputs<u64> {
///     // body returns DynamicOutputs(Vec<u64>) with len == radixes.len()
/// }
/// ```
///
/// The macro emits one output port per element (named `d0`,
/// `d1`, ...) at construction time using the `Const<Vec<C>>`
/// arg's length. `FuncSig.outputs` is `0` signalling dynamic.
/// Requires exactly one `Const<Vec<C>>` arg per function; the
/// macro errors at compile time otherwise.
pub struct DynamicOutputs<T>(pub Vec<T>);

impl<T> std::ops::Deref for DynamicOutputs<T> {
    type Target = Vec<T>;
    fn deref(&self) -> &Vec<T> { &self.0 }
}

// ── Config<T> — wire arg marked as config-cost ────────────────

/// Marker wrapper signalling that the wrapped wire is a
/// configuration input — expensive to change because the node
/// keeps internal state (LUTs, alias tables, parsed specs)
/// derived from it. The macro emits the matching slot with
/// `Port::config()` (SRD 15 §"WireCost::Config") so the
/// compiler warns on cycle-time binding.
///
/// In-spirit replacement for a `#[wire_cost(Config)]` arg-level
/// attribute — operator declares the cost intent via the type
/// system. Body unwraps with `.0` or via `Deref`.
pub struct Config<T>(pub T);

impl<T> std::ops::Deref for Config<T> {
    type Target = T;
    fn deref(&self) -> &T { &self.0 }
}

impl<T: Wire> Wire for Config<T> {
    const PORT: PortType = T::PORT;
    const JIT: Option<JitType> = T::JIT;
    const RESOLVER: Option<crate::dsl::registry::DefaultResolver> = T::RESOLVER;
    const WIRE_COST: crate::ast::WireCost = crate::ast::WireCost::Config;
    fn extract(v: &Value) -> Self { Config(T::extract(v)) }
    fn inject(self) -> Value { self.0.inject() }
}

// ── Resolved<R, T> — Handle wire with SRD-53 auto-resolver ────

/// SRD-80b in-spirit replacement for the `default_resolver`
/// attribute. The `R` parameter (a [`ResolverKind`] impl) carries
/// the auto-resolver kind; the `T` parameter is the concrete
/// `Handle`-inner type the body sees.
///
/// Operators write:
///
/// ```ignore
/// fn matching_profiles(
///     group: Resolved<GroupResolver, vectordata::TestDataGroup>,
///     prefix: &str,
/// ) -> Vec<String> {
///     let group: &vectordata::TestDataGroup = &group;
///     // ... use group methods directly
/// }
/// ```
///
/// The macro reads `<Resolved<GroupResolver, T> as Wire>::RESOLVER`
/// at codegen time and emits the matching `FuncSig.default_resolver`.
/// No `#[polydat_node(default_resolver = ...)]` attribute is
/// involved — the resolver information lives in the function
/// signature where it belongs.
pub struct Resolved<R: ResolverKind, T: 'static + Send + Sync> {
    inner: std::sync::Arc<T>,
    _r: std::marker::PhantomData<fn() -> R>,
}

impl<R: ResolverKind, T: 'static + Send + Sync> std::ops::Deref for Resolved<R, T> {
    type Target = T;
    fn deref(&self) -> &T { &self.inner }
}

impl<R: ResolverKind, T: 'static + Send + Sync> Resolved<R, T> {
    /// Construct from a pre-resolved Arc — useful for tests
    /// and programmatic graph assembly that bypasses the DSL
    /// auto-resolver.
    pub fn from_arc(inner: std::sync::Arc<T>) -> Self {
        Self { inner, _r: std::marker::PhantomData }
    }
    /// Borrow the inner Arc.
    pub fn as_arc(&self) -> &std::sync::Arc<T> { &self.inner }
}

/// Marker trait that names a kind of source-string auto-resolver
/// for [`Resolved<R, T>`] wire args. The variants here mirror
/// [`crate::dsl::registry::DefaultResolver`]; each impl picks
/// one of them.
pub trait ResolverKind: 'static {
    const RESOLVER: crate::dsl::registry::DefaultResolver;
}

/// Splice `dataset_group_open(<source>)` upstream when the wire
/// source is a `Str` (SRD-53 `DefaultResolver::Group`).
pub struct GroupResolver;
impl ResolverKind for GroupResolver {
    const RESOLVER: crate::dsl::registry::DefaultResolver =
        crate::dsl::registry::DefaultResolver::Group;
}

impl<R: ResolverKind, T: 'static + Send + Sync> Wire for Resolved<R, T> {
    const PORT: PortType = PortType::Handle;
    const JIT: Option<JitType> = None;
    const RESOLVER: Option<crate::dsl::registry::DefaultResolver> =
        Some(<R as ResolverKind>::RESOLVER);
    fn extract(v: &Value) -> Self {
        match v {
            Value::Handle(arc) => {
                let inner = arc.clone().downcast::<T>()
                    .unwrap_or_else(|_| panic!(
                        "Wire<Resolved<_, {}>>::extract: Handle downcast failed",
                        std::any::type_name::<T>()));
                Resolved { inner, _r: std::marker::PhantomData }
            }
            other => panic!(
                "Wire<Resolved>::extract: expected Handle, got {other:?}"),
        }
    }
    fn inject(self) -> Value { Value::Handle(self.inner) }
}

// =====================================================================
// ConstSource — ConstArg → typed extraction (owned types only)
// =====================================================================

/// `ConstArg` → typed-Rust-value bridge for owned types in
/// `Const<T>` position.
///
/// Borrow shapes (`Const<&str>`) are handled by the macro at
/// codegen — it stores `String` via `ConstSource for String`
/// and emits `Const(self.field.as_str())` at the eval call site
/// to satisfy the operator-side `Const<&str>` signature.
pub trait ConstSource: Sized + 'static {
    const SLOT: SlotType;
    fn extract(arg: &ConstArg) -> Self;
}

impl ConstSource for u64 {
    const SLOT: SlotType = SlotType::ConstU64;
    fn extract(arg: &ConstArg) -> Self {
        match arg {
            ConstArg::Int(v) => *v,
            other => panic!("ConstSource<u64>::extract: expected Int, got {other:?}"),
        }
    }
}

impl ConstSource for f64 {
    const SLOT: SlotType = SlotType::ConstF64;
    fn extract(arg: &ConstArg) -> Self {
        match arg {
            ConstArg::Float(v) => *v,
            ConstArg::Int(v) => *v as f64,
            other => panic!("ConstSource<f64>::extract: expected Float or Int, got {other:?}"),
        }
    }
}

impl ConstSource for bool {
    const SLOT: SlotType = SlotType::ConstU64;
    fn extract(arg: &ConstArg) -> Self {
        match arg {
            ConstArg::Int(v) => *v != 0,
            other => panic!("ConstSource<bool>::extract: expected Int, got {other:?}"),
        }
    }
}

impl ConstSource for String {
    const SLOT: SlotType = SlotType::ConstStr;
    fn extract(arg: &ConstArg) -> Self {
        match arg {
            ConstArg::Str(s) => s.clone(),
            other => panic!("ConstSource<String>::extract: expected Str, got {other:?}"),
        }
    }
}

/// SRD-80b Phase C — workload-list const combinator. Used by the
/// macro when it sees `Const<Vec<C>>` in an operator's signature
/// (e.g. `Const<Vec<u64>>`, `Const<Vec<String>>`). The macro
/// emits one `Slot::Const { name, slot_type: ConstVec, .. }`
/// for the position and packages the trailing `consts[..]`
/// slice into a `ConstArg::List` at build time; this impl
/// walks the list, dispatching `C::extract` per element.
impl<C: ConstSource> ConstSource for Vec<C> {
    const SLOT: SlotType = SlotType::ConstVec;
    fn extract(arg: &ConstArg) -> Self {
        match arg {
            ConstArg::List(items) => items.iter().map(C::extract).collect(),
            other => panic!(
                "ConstSource<Vec<_>>::extract: expected List, got {other:?}"),
        }
    }
}

// FromValue / IntoValue retired 2026-06-05 — the `#[polydat_node]`
// macro now dispatches every owned type through `<T as Wire>::extract`
// / `::inject` and emits direct `match`-on-`Value` extraction for
// borrow shapes (`&str`, `&[u8]`, `&[T]`, `&serde_json::Value`).
// Per SRD-80b Phase B; the old trait pair plus their borrow-impls'
// `unsafe { transmute }` lifetime-extension hack are gone.
//
// [PLACEHOLDER_PHASE_B_DELETE]

/// SRD-80 PR B.5 — marker wrapper for const arguments in
/// `#[polydat_node]` function signatures.
///
/// Use in arg position to signal that the value is captured at
/// node-construction time (assembly-time) rather than read
/// per-cycle from a wire. The macro detects `Const<T>` in arg
/// position and:
///
/// - Emits `Slot::Const { ... }` (not `Slot::Wire`) in the
///   node's NodeMeta.
/// - Emits `SlotType::ConstU64` / `ConstF64` / `ConstStr` in
///   the corresponding `FuncSig.params` entry (the const
///   variant matching `T`).
/// - Adds a struct field to hold the captured value.
/// - Generates a `new(const_values...)` constructor.
/// - Wires the build closure to pull from `consts: &[ConstArg]`
///   and pass values to `new()`.
/// - Constructs a `Const<T>(...)` wrapper around the struct
///   field at eval time so the user's function body sees the
///   wrapped type matching its signature.
///
/// Body code accesses the wrapped value via `.0` or via the
/// `Deref` impl below:
///
/// ```ignore
/// #[polydat_node(category = String)]
/// fn combinations(input: u64, pattern: Const<&str>) -> String {
///     apply(input, pattern.0)  // pattern.0 is &str
/// }
/// ```
///
/// Type-shape dispatch table:
///
/// | `Const<T>` form | `SlotType` variant | Struct field type | ConstArg accessor |
/// |---|---|---|---|
/// | `Const<u64>`  | `ConstU64`  | `u64`    | `as_u64()`  |
/// | `Const<f64>`  | `ConstF64`  | `f64`    | `as_f64()`  |
/// | `Const<bool>` | `ConstU64`  | `bool`   | `as_u64() != 0` |
/// | `Const<&str>` | `ConstStr`  | `String` | `as_str().to_string()` |
pub struct Const<T>(pub T);

impl<T> std::ops::Deref for Const<T> {
    type Target = T;
    fn deref(&self) -> &T { &self.0 }
}

impl<T> std::ops::DerefMut for Const<T> {
    fn deref_mut(&mut self) -> &mut T { &mut self.0 }
}

/// SRD-80 PR B.6 — construction-time setup contract for nodes
/// that derive a pre-computed runtime state from their const
/// args (e.g. `combinations` parsing a charset pattern into
/// segments + modulus, `regex_match` compiling a pattern,
/// `histribution` parsing a distribution spec).
///
/// **The contract**: the operator-provided setup function is
/// called EXACTLY ONCE per node instance, at construction time
/// (`new()`). Its result is stored in a struct field; eval-
/// time access is a plain `&T` borrow.
///
/// **Type-level enforcement**: the `#[poly_const(...)]`
/// attribute on a `&T` argument tells the macro to generate
/// this construction pattern. The macro is the sole party
/// emitting `setup_fn(...)` calls and it generates the call
/// exactly once inside `new()`. The contract is inviolable
/// because no other code path can reach the setup function —
/// the macro hides it inside the constructor.
///
/// In effect, the function pointer behaves as `FnOnce` —
/// invoked one time, by one site, never again. The FnOnce
/// semantics aren't expressed as a trait bound because they
/// don't need to be: the macro is the only caller, and the
/// macro respects single-call by construction.
///
/// Library author idiom:
///
/// ```ignore
/// pub struct ParsedPattern {
///     pub segments: Vec<Segment>,
///     pub modulus: u64,
/// }
///
/// impl ParsedPattern {
///     /// Single-call setup. Macro invokes once in `new()`.
///     fn from_pattern(pattern: &str) -> Self { /* parse */ }
/// }
///
/// #[polydat_node(category = String)]
/// fn combinations(
///     input: u64,
///     pattern: Const<&str>,
///     #[poly_const(ParsedPattern::from_pattern, from = pattern)]
///     parsed: &ParsedPattern,
/// ) -> String {
///     // parsed is a borrow of the cached struct field —
///     // no recomputation, no clone, no ceremony at the call site.
///     let mut r = input % parsed.modulus;
///     /* ... */
/// }
/// ```
///
/// Marker trait — purely a documentation handle for types
/// intended to be polydat-setup targets. The macro doesn't
/// dispatch on this; the attribute is the dispatch surface.
/// Implementing the trait gives library authors a way to
/// signal intent and improve `cargo doc` discoverability.
pub trait PolydatSetup {}

// SRD-80 PR B.2/B.3 — macro-generated nodes register through
// the existing `NodeRegistration` inventory channel
// (`polydat::dsl::registry::NodeRegistration`), the same
// channel `register_nodes!` already uses. The proc-macro
// emits a `NodeRegistration` per `#[polydat_node]` site, so
// every consumer that already iterates the registry
// (`registry()`, `lookup()`, the compile pipeline's
// `factory::build_node`) sees macro-generated nodes
// automatically — no parallel collection, no separate dispatch
// surface. See `polydat::dsl::registry` for the load-bearing
// data structures.
