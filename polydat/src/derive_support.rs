// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Trait surface that the `#[polydat_node]` proc-macro
//! (`polydat-derive`) calls into for boxing / unboxing wire
//! values.
//!
//! Every primitive `PortType` has matching `FromValue` and
//! `IntoValue` impls. The macro-generated `eval` function uses
//! them to convert between the `Value`-typed wire stream the
//! polydat runtime carries and the typed Rust arguments the
//! workload-author's function body operates on.
//!
//! ## Scope (PR B.1)
//!
//! Primitive scalar conversions only:
//!
//! - `u64` / `U64`
//! - `f64` / `F64`
//! - `bool` / `Bool`
//! - `&str` / `Str` (borrowed from the `Value::Str(Arc<str>)`)
//! - `String` / `Str` (owned clone of the same)
//!
//! Vector types, `Json`, `Handle`, `Ext` and any compound shapes
//! are deferred to later PRs. The simple case is enough to
//! validate the macro pipeline end-to-end with a pilot node.
//!
//! ## Why the trait surface lives here
//!
//! `polydat-derive` is a proc-macro crate — it can't define
//! traits that are visible at the call site, only emit token
//! streams referencing traits defined elsewhere. The macro
//! emits `<T as polydat::derive_support::FromValue>::from_value(...)`
//! paths; this module is what those paths resolve to.

use crate::ast::Value;

/// Pull a typed value out of a `Value` wire. Panics if the
/// runtime value doesn't match the declared type — the
/// type-checker is responsible for routing well-typed values
/// to each slot before `eval` runs, so the panic is a real
/// "the type system was lied to" bug rather than a normal
/// path.
pub trait FromValue: Sized {
    fn from_value(v: &Value) -> Self;
}

impl FromValue for u64 {
    fn from_value(v: &Value) -> Self { v.as_u64() }
}

impl FromValue for f64 {
    fn from_value(v: &Value) -> Self { v.as_f64() }
}

impl FromValue for bool {
    fn from_value(v: &Value) -> Self {
        match v {
            Value::Bool(b) => *b,
            Value::U64(n) => *n != 0,
            other => panic!(
                "FromValue<bool> called with {:?}; type-checker should have \
                 prevented this", other),
        }
    }
}

impl FromValue for String {
    fn from_value(v: &Value) -> Self {
        match v {
            Value::Str(s) => s.to_string(),
            other => other.to_display_string(),
        }
    }
}

// SRD-80 PR B.14 — narrow integer and f32 types. No
// dedicated Value variants; they ride on Value::U64 /
// Value::F64 with width-narrowing casts at the boundary.
// This matches the hand-written edge-adapter convention in
// convert.rs.

impl FromValue for u32 {
    fn from_value(v: &Value) -> Self { v.as_u64() as u32 }
}
impl IntoValue for u32 {
    fn into_value(self) -> Value { Value::U64(self as u64) }
}

impl FromValue for i32 {
    fn from_value(v: &Value) -> Self { v.as_u64() as i32 }
}
impl IntoValue for i32 {
    fn into_value(self) -> Value { Value::U64(self as u64) }
}

impl FromValue for i64 {
    fn from_value(v: &Value) -> Self { v.as_u64() as i64 }
}
impl IntoValue for i64 {
    fn into_value(self) -> Value { Value::U64(self as u64) }
}

// f32 follows the same narrow-width convention as u32/i32/i64:
// the runtime stores the value as bits in Value::U64's payload
// (low 32 = f32 bit-pattern). This matches the hand-written
// edge adapters (F32ToF64, F32ToString) which read via
// `f32::from_bits(as_u64() as u32)`.
impl FromValue for f32 {
    fn from_value(v: &Value) -> Self {
        f32::from_bits(v.as_u64() as u32)
    }
}
impl IntoValue for f32 {
    fn into_value(self) -> Value { Value::U64(self.to_bits() as u64) }
}

impl<'a> FromValue for &'a str {
    fn from_value(v: &Value) -> Self {
        // SAFETY: This borrow is scoped to the eval call's
        // duration. The runtime guarantees the input `Value`
        // outlives the eval; the borrow checker can't see
        // through the proc-macro-generated tuple-destructure,
        // so we transmute the lifetime. This is the same
        // pattern the hand-written `.as_str()` calls already
        // rely on across the polydat library.
        let s: &str = match v {
            Value::Str(s) => s.as_ref(),
            _ => panic!(
                "FromValue<&str> called with non-Str value; type-checker \
                 should have prevented this"),
        };
        unsafe { std::mem::transmute::<&str, &'a str>(s) }
    }
}

/// Push a typed value back into the `Value` outputs stream.
/// Inverse of `FromValue` — every type with a `FromValue` impl
/// has a matching `IntoValue` impl so the round-trip type
/// holds.
pub trait IntoValue {
    fn into_value(self) -> Value;
}

impl IntoValue for u64 {
    fn into_value(self) -> Value { Value::U64(self) }
}

impl IntoValue for f64 {
    fn into_value(self) -> Value { Value::F64(self) }
}

impl IntoValue for bool {
    fn into_value(self) -> Value { Value::Bool(self) }
}

impl IntoValue for String {
    fn into_value(self) -> Value { Value::Str(self.into()) }
}

impl<'a> IntoValue for &'a str {
    fn into_value(self) -> Value { Value::Str(self.into()) }
}

/// SRD-80 PR B.8 — `Value` as a wire-arg type. Polydat already
/// uses `Value` as the runtime carrier across every wire; using
/// it in `#[polydat_node]` signatures lets the macro represent
/// type-tunneling passthrough nodes (`identity`, `log_*`,
/// `inspect`) and value-as-debug nodes (`debug_repr`, `type_of`)
/// without inventing a new wrapper type. Clone-through both ways.
impl FromValue for Value {
    fn from_value(v: &Value) -> Self { v.clone() }
}

impl IntoValue for Value {
    fn into_value(self) -> Value { self }
}

// SRD-80 PR B.11 — wrapper type recognition. The macro reads
// `Arc<[u8]>` / `Vec<u8>` / `&[u8]` as PortType::Bytes,
// `Arc<serde_json::Value>` / `&serde_json::Value` as
// PortType::Json, and any other `Arc<T>` as PortType::Handle
// with downcast. The trait impls below are for the
// non-Handle (concrete-type) carriers; Handle dispatches
// inline in the macro because a blanket `impl FromValue for
// Arc<T>` would overlap with the Json impl.

// ── Bytes (`Value::Bytes(Arc<[u8]>)`) ─────────────────────

impl FromValue for std::sync::Arc<[u8]> {
    fn from_value(v: &Value) -> Self {
        match v {
            Value::Bytes(b) => b.clone(),
            other => panic!("FromValue<Arc<[u8]>>: expected Bytes, got {other:?}"),
        }
    }
}

impl IntoValue for std::sync::Arc<[u8]> {
    fn into_value(self) -> Value { Value::Bytes(self) }
}

impl FromValue for Vec<u8> {
    fn from_value(v: &Value) -> Self {
        match v {
            Value::Bytes(b) => b.to_vec(),
            other => panic!("FromValue<Vec<u8>>: expected Bytes, got {other:?}"),
        }
    }
}

impl IntoValue for Vec<u8> {
    fn into_value(self) -> Value { Value::Bytes(self.into()) }
}

impl<'a> FromValue for &'a [u8] {
    fn from_value(v: &Value) -> Self {
        let s: &[u8] = match v {
            Value::Bytes(b) => b,
            other => panic!("FromValue<&[u8]>: expected Bytes, got {other:?}"),
        };
        // Same lifetime-extending transmute as &str. The borrow
        // is valid for the eval() call's duration; the runtime
        // guarantees the input `Value` outlives the eval.
        unsafe { std::mem::transmute::<&[u8], &'a [u8]>(s) }
    }
}

// ── Json (`Value::Json(Arc<serde_json::Value>)`) ─────────

impl FromValue for std::sync::Arc<serde_json::Value> {
    fn from_value(v: &Value) -> Self {
        match v {
            Value::Json(j) => j.clone(),
            other => panic!(
                "FromValue<Arc<serde_json::Value>>: expected Json, got {other:?}"),
        }
    }
}

impl IntoValue for std::sync::Arc<serde_json::Value> {
    fn into_value(self) -> Value { Value::Json(self) }
}

impl<'a> FromValue for &'a serde_json::Value {
    fn from_value(v: &Value) -> Self {
        let j: &serde_json::Value = match v {
            Value::Json(j) => j,
            other => panic!(
                "FromValue<&serde_json::Value>: expected Json, got {other:?}"),
        };
        unsafe { std::mem::transmute::<&serde_json::Value, &'a serde_json::Value>(j) }
    }
}

// SRD-80 PR B.13 — typed vector wires. Six element types
// (f32 / i32 / f64 / i64 / f16 / i16) map to PortType::Vec*.
// Three Rust input shapes are supported per element type:
//   - `SliceArc<T>`  zero-copy clone, body takes ownership of
//                    the Arc (one atomic increment).
//   - `&[T]`         borrow into the SliceArc's slice for the
//                    eval call (zero alloc, zero clone).
//   - `Vec<T>`       owned copy (allocates per call — use only
//                    when the body must mutate or outlive the
//                    eval call).
// Three Rust output shapes per element type:
//   - `SliceArc<T>`  pass-through, no allocation.
//   - `Vec<T>`       one heap alloc through SliceArc::from_vec.

/// Codegen helper: expand a single FromValue<&[T]> impl. Same
/// lifetime-extending transmute as &str and &[u8].
macro_rules! impl_from_borrowed_slice {
    ($elem:ty, $variant:ident) => {
        impl<'a> FromValue for &'a [$elem] {
            fn from_value(v: &Value) -> Self {
                let s: &[$elem] = match v {
                    Value::$variant(arc) => arc.as_slice(),
                    other => panic!(
                        concat!("FromValue<&[", stringify!($elem),
                                "]>: expected ", stringify!($variant),
                                ", got {:?}"),
                        other),
                };
                unsafe { std::mem::transmute::<&[$elem], &'a [$elem]>(s) }
            }
        }
    };
}

macro_rules! impl_vec_wire {
    ($elem:ty, $variant:ident) => {
        impl FromValue for crate::ast::SliceArc<$elem> {
            fn from_value(v: &Value) -> Self {
                match v {
                    Value::$variant(arc) => arc.clone(),
                    other => panic!(
                        concat!("FromValue<SliceArc<", stringify!($elem),
                                ">>: expected ", stringify!($variant),
                                ", got {:?}"),
                        other),
                }
            }
        }
        impl IntoValue for crate::ast::SliceArc<$elem> {
            fn into_value(self) -> Value { Value::$variant(self) }
        }

        impl FromValue for Vec<$elem> {
            fn from_value(v: &Value) -> Self {
                match v {
                    Value::$variant(arc) => arc.as_slice().to_vec(),
                    other => panic!(
                        concat!("FromValue<Vec<", stringify!($elem),
                                ">>: expected ", stringify!($variant),
                                ", got {:?}"),
                        other),
                }
            }
        }
        impl IntoValue for Vec<$elem> {
            fn into_value(self) -> Value {
                Value::$variant(crate::ast::SliceArc::from_vec(self))
            }
        }

        impl_from_borrowed_slice!($elem, $variant);
    };
}

impl_vec_wire!(f32, VecF32);
impl_vec_wire!(i32, VecI32);
impl_vec_wire!(f64, VecF64);
impl_vec_wire!(i64, VecI64);
impl_vec_wire!(half::f16, VecF16);
impl_vec_wire!(i16, VecI16);

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
