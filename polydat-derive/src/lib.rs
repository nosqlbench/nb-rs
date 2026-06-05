// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! `polydat-derive` — proc-macro implementation of
//! [`#[polydat_node]`](polydat_node).
//!
//! See [`docs/sysref/80_node_function_macro_collapse.md`](https://github.com/jshook/nb-rs/blob/main/docs/sysref/80_node_function_macro_collapse.md)
//! for the design, the 8 open design questions this proc-macro
//! is closing one-at-a-time, and the migration plan against
//! existing polydat library nodes.
//!
//! ## Current scope (PR B.1)
//!
//! This is the SCAFFOLDING pass. The macro recognizes the
//! simplest case only:
//!
//! - A standalone `fn` (no `impl` block, no struct).
//! - All wire input arguments are PRIMITIVES with implementations
//!   of polydat's `FromValue` trait — concretely: `u64`, `f64`,
//!   `bool`, `&str` (or owned `String`).
//! - Return type is a PRIMITIVE with a `IntoValue` implementation —
//!   same set.
//! - No state, no const args, no JIT hooks, no variadic shapes,
//!   no polymorphism.
//!
//! Out of scope (deferred to later PR B.* batches):
//!
//! - State-bearing nodes (probability PRNG, vectors readers).
//! - JIT-eligible nodes (the `compiled_u64` hooks).
//! - Const-arg parameters with `ConstConstraint`.
//! - Variadic shapes (`Variadic<T>`, `&[T]`).
//! - Polymorphic outputs (`SameAsInput`).
//! - Ext-typed args / returns (adapter-contributed types).
//!
//! ## Generated output (for the simple case)
//!
//! Input:
//!
//! ```ignore
//! #[polydat_node]
//! fn str_eq(a: &str, b: &str) -> u64 {
//!     if a == b { 1 } else { 0 }
//! }
//! ```
//!
//! Generated:
//!
//! ```ignore
//! pub struct StrEq { meta: polydat::ast::NodeMeta }
//! impl Default for StrEq { fn default() -> Self { Self::new() } }
//! impl StrEq {
//!     pub fn new() -> Self {
//!         Self {
//!             meta: polydat::ast::NodeMeta {
//!                 name: "str_eq".into(),
//!                 ins: vec![
//!                     polydat::ast::Slot::Wire(polydat::ast::Port::new(
//!                         "a", polydat::ast::PortType::Str)),
//!                     polydat::ast::Slot::Wire(polydat::ast::Port::new(
//!                         "b", polydat::ast::PortType::Str)),
//!                 ],
//!                 outs: vec![polydat::ast::Port::new(
//!                     "output", polydat::ast::PortType::U64)],
//!             },
//!         }
//!     }
//! }
//! impl polydat::ast::PolydatNode for StrEq {
//!     fn meta(&self) -> &polydat::ast::NodeMeta { &self.meta }
//!     fn eval(
//!         &self,
//!         inputs: &[polydat::ast::Value],
//!         outputs: &mut [polydat::ast::Value],
//!     ) {
//!         let a = <&str as polydat::derive_support::FromValue>::from_value(&inputs[0]);
//!         let b = <&str as polydat::derive_support::FromValue>::from_value(&inputs[1]);
//!         let result: u64 = if a == b { 1 } else { 0 };
//!         outputs[0] = <u64 as polydat::derive_support::IntoValue>::into_value(result);
//!     }
//! }
//! ```
//!
//! The original `fn str_eq` is consumed by the macro — only the
//! struct + impl is emitted. The body of `str_eq` becomes the
//! body of the `eval` method (with parameter rebinding via
//! `FromValue::from_value`).
//!
//! FuncSig registration via inventory or similar is deferred to
//! PR B.2 — for now the macro just generates the struct + impl
//! so we can validate the boxing/unboxing path with a pilot
//! node.

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{quote, format_ident};
use syn::{
    parse_macro_input, FnArg, Ident, ItemFn, Meta, Pat, ReturnType, Token, Type,
    parse::Parser, punctuated::Punctuated,
};

/// `#[polydat_node]` — derive a polydat node from a typed Rust
/// function signature.
///
/// See the crate docs for the supported surface and what's
/// out of scope for this scaffolding pass.
///
/// ## Attribute parameters (PR B.2)
///
/// - `category = <ident>` — the polydat `FuncCategory` variant
///   the node belongs to (`Comparison`, `Math`, `String`, etc.).
///   Defaults to `Misc` when unspecified.
#[proc_macro_attribute]
pub fn polydat_node(attr: TokenStream, item: TokenStream) -> TokenStream {
    let func = parse_macro_input!(item as ItemFn);

    let attrs = match parse_attrs(attr.into()) {
        Ok(a) => a,
        Err(e) => return e.to_compile_error().into(),
    };

    match generate(func, attrs) {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

/// Parsed `#[polydat_node(...)]` attribute parameters.
struct NodeAttrs {
    /// `FuncCategory` variant name — required (no default).
    /// Forcing the operator to declare the category keeps the
    /// `describe` / help / categorization surface coherent.
    category: Ident,
    /// SRD-80 PR B.7 — opt out of JIT (Phase-2/Phase-3) emission
    /// even when the type signature qualifies. Use when body
    /// has side effects the operator doesn't want JIT-dispatched
    /// or when hand-written hooks override the macro version.
    no_jit: bool,
    /// SRD-80 PR B.7 — override path for `compiled_u64()`. When
    /// set, the macro emits `compiled_u64(&self) -> Some(Box::new(<path>))`
    /// instead of building the closure from the body. Free-fn
    /// signature: `fn(&[u64], &mut [u64])`. Escape hatch for
    /// hand-tuned SIMD / FFI / unusual carriers.
    compiled_u64_override: Option<syn::ExprPath>,
    /// SRD-80 PR B.7 — override path for `jit_constants()`.
    /// Free-fn signature: `fn(&Node) -> Vec<u64>`. Macro emits
    /// `jit_constants(&self) -> <path>(self)`.
    jit_constants_override: Option<syn::ExprPath>,
    /// SRD-80 PR B.7 — declared `Purity` (Pure / SideChannel /
    /// Nondeterministic). Defaults to `Pure` (the trait
    /// default). Macro emits `fn purity(&self) -> Purity::<expr>`
    /// when present.
    ///
    /// Two attribute forms recognized:
    ///
    /// - `purity = Nondeterministic` (path) — emits `Purity::Nondeterministic`.
    /// - `purity = SideChannel(LogBuffer)` (call) — emits the
    ///   struct-variant form `Purity::SideChannel { sink:
    ///   SideChannelSink::LogBuffer }`. The call-form variant
    ///   makes the struct-variant inline attribute parse-able
    ///   (Rust attribute grammar doesn't accept inline `{ ... }`
    ///   struct literals as attribute values).
    purity: Option<syn::Expr>,
    /// SRD-80 PR B.9 — variadic node identity value (the result
    /// when called with zero inputs). Emitted into
    /// `FuncSig.identity: Option<u64>`. Required for variadic
    /// numeric reductions whose group has an identity (sum=0,
    /// product=1, min=u64::MAX, max=0). Skip for variadics with
    /// no meaningful identity (str_concat — empty list yields "").
    identity: Option<syn::Expr>,
    /// SRD-80 PR B.9 — `Commutativity` variant. Defaults to
    /// `Positional`. Variadic reductions typically pass
    /// `AllCommutative` (sum/product/min/max all hold regardless
    /// of input order).
    commutativity: Option<Ident>,
    /// SRD-80 PR B.9 — minimum required wire count for variadic
    /// nodes. Defaults to 0 (callable with zero inputs).
    variadic_min: Option<syn::LitInt>,
    /// SRD-80 PR B.10 — names for the elements of a tuple
    /// return type, paired positionally with the tuple
    /// elements. Defaults to `out_0`, `out_1`, ... when
    /// absent. Length must match tuple arity — operator gets a
    /// compile error otherwise.
    output_names: Option<Vec<Ident>>,
}

fn parse_attrs(attr: TokenStream2) -> syn::Result<NodeAttrs> {
    if attr.is_empty() {
        return Err(syn::Error::new(
            proc_macro2::Span::call_site(),
            "#[polydat_node] requires `category = <FuncCategory variant>`. \
             Example: #[polydat_node(category = Comparison)]",
        ));
    }

    let parser = Punctuated::<Meta, Token![,]>::parse_terminated;
    let items = parser.parse2(attr)?;

    let mut category: Option<Ident> = None;
    let mut no_jit = false;
    let mut compiled_u64_override: Option<syn::ExprPath> = None;
    let mut jit_constants_override: Option<syn::ExprPath> = None;
    let mut purity: Option<syn::Expr> = None;
    let mut identity: Option<syn::Expr> = None;
    let mut commutativity: Option<Ident> = None;
    let mut variadic_min: Option<syn::LitInt> = None;
    let mut output_names: Option<Vec<Ident>> = None;

    for item in items {
        match item {
            Meta::Path(p) => {
                let key = p.get_ident()
                    .ok_or_else(|| syn::Error::new_spanned(
                        &p,
                        "#[polydat_node] flag keys must be bare identifiers",
                    ))?
                    .clone();
                match key.to_string().as_str() {
                    "no_jit" => { no_jit = true; }
                    other => {
                        return Err(syn::Error::new_spanned(
                            &key,
                            format!(
                                "#[polydat_node] does not recognize flag `{other}`. \
                                 PR B.7 flags: `no_jit`.",
                            ),
                        ));
                    }
                }
            }
            Meta::NameValue(nv) => {
                let key = nv.path.get_ident()
                    .ok_or_else(|| syn::Error::new_spanned(
                        &nv.path,
                        "#[polydat_node] parameter keys must be bare identifiers",
                    ))?
                    .clone();
                match key.to_string().as_str() {
                    "category" => {
                        let syn::Expr::Path(p) = &nv.value else {
                            return Err(syn::Error::new_spanned(
                                &nv.value,
                                "`category` value must be a bare identifier \
                                 (a polydat `FuncCategory` variant name).",
                            ));
                        };
                        category = Some(p.path.get_ident()
                            .ok_or_else(|| syn::Error::new_spanned(
                                &nv.value,
                                "`category` value must be a single identifier.",
                            ))?
                            .clone());
                    }
                    "compiled_u64" => {
                        let syn::Expr::Path(p) = &nv.value else {
                            return Err(syn::Error::new_spanned(
                                &nv.value,
                                "`compiled_u64` value must be a path to a free \
                                 function with signature `fn(&[u64], &mut [u64])`.",
                            ));
                        };
                        compiled_u64_override = Some(p.clone());
                    }
                    "jit_constants" => {
                        let syn::Expr::Path(p) = &nv.value else {
                            return Err(syn::Error::new_spanned(
                                &nv.value,
                                "`jit_constants` value must be a path to a free \
                                 function with signature `fn(&Node) -> Vec<u64>`.",
                            ));
                        };
                        jit_constants_override = Some(p.clone());
                    }
                    "purity" => {
                        // Accept either:
                        //   purity = Nondeterministic         (path)
                        //   purity = SideChannel(LogBuffer)   (call)
                        // The codegen dispatches on the shape.
                        match &nv.value {
                            syn::Expr::Path(_) | syn::Expr::Call(_) => {
                                purity = Some(nv.value.clone());
                            }
                            _ => {
                                return Err(syn::Error::new_spanned(
                                    &nv.value,
                                    "`purity` value must be a Purity variant: \
                                     `Pure`, `Nondeterministic`, or \
                                     `SideChannel(<sink>)` where `<sink>` is a \
                                     `SideChannelSink` variant ident.",
                                ));
                            }
                        }
                    }
                    "identity" => {
                        // SRD-80 PR B.9 — variadic identity element.
                        // Any constant-evaluable expression is fine.
                        identity = Some(nv.value.clone());
                    }
                    "commutativity" => {
                        let syn::Expr::Path(p) = &nv.value else {
                            return Err(syn::Error::new_spanned(
                                &nv.value,
                                "`commutativity` value must be a `Commutativity` \
                                 variant ident (Positional / AllCommutative / ...).",
                            ));
                        };
                        commutativity = Some(p.path.get_ident()
                            .ok_or_else(|| syn::Error::new_spanned(
                                &nv.value,
                                "`commutativity` value must be a single identifier.",
                            ))?
                            .clone());
                    }
                    "variadic_min" => {
                        let syn::Expr::Lit(syn::ExprLit { lit: syn::Lit::Int(n), .. }) = &nv.value else {
                            return Err(syn::Error::new_spanned(
                                &nv.value,
                                "`variadic_min` value must be an integer literal.",
                            ));
                        };
                        variadic_min = Some(n.clone());
                    }
                    other => {
                        return Err(syn::Error::new_spanned(
                            &key,
                            format!(
                                "#[polydat_node] does not recognize parameter `{other}`. \
                                 PR B.2 keys: `category = ...`. PR B.7 keys: \
                                 `no_jit`, `compiled_u64 = ...`, \
                                 `jit_constants = ...`, `purity = ...`. \
                                 PR B.9 keys: `identity = ...`, \
                                 `commutativity = ...`, `variadic_min = ...`.",
                            ),
                        ));
                    }
                }
            }
            Meta::List(list) => {
                let key = list.path.get_ident()
                    .ok_or_else(|| syn::Error::new_spanned(
                        &list.path,
                        "#[polydat_node] list-form keys must be bare identifiers",
                    ))?
                    .clone();
                match key.to_string().as_str() {
                    "output_names" => {
                        let names: Punctuated<Ident, Token![,]> =
                            list.parse_args_with(Punctuated::parse_terminated)?;
                        if names.is_empty() {
                            return Err(syn::Error::new_spanned(
                                &list,
                                "`output_names(...)` requires at least one name.",
                            ));
                        }
                        output_names = Some(names.into_iter().collect());
                    }
                    other => {
                        return Err(syn::Error::new_spanned(
                            &key,
                            format!(
                                "#[polydat_node] does not recognize list-form key `{other}`. \
                                 PR B.10 list keys: `output_names(...)`.",
                            ),
                        ));
                    }
                }
            }
        }
    }

    let category = category.ok_or_else(|| syn::Error::new(
        proc_macro2::Span::call_site(),
        "#[polydat_node] requires `category = <FuncCategory variant>`.",
    ))?;

    Ok(NodeAttrs {
        category,
        no_jit,
        compiled_u64_override,
        jit_constants_override,
        purity,
        identity,
        commutativity,
        variadic_min,
        output_names,
    })
}

/// One classified function argument. Drives every downstream
/// piece of the generated output: NodeMeta slot, FuncSig
/// param, struct field (for consts), build closure const
/// extraction, eval-time wrapper construction.
struct ClassifiedArg {
    name: syn::Ident,
    /// Original Rust type from the function signature.
    declared_ty: Type,
    /// Whether the arg was declared as `Const<T>`.
    kind: ArgKind,
    /// For const args: optional default value expression parsed
    /// from `#[poly_default(VAL)]`. Present → the const is
    /// optional in FuncSig and the build closure falls back to
    /// the default when the consts slice doesn't supply one.
    default_value: Option<syn::Expr>,
    /// SRD-80 PR B.14 — `#[constraint(<Variant>)]` on a wire
    /// arg. The variant name maps to `ConstConstraint::*`; the
    /// emitted `Port` carries the constraint so strict-wire
    /// mode can auto-insert upstream assertion nodes.
    wire_constraint: Option<Ident>,
}

#[derive(Clone)]
enum ArgKind {
    Wire,
    Const(ConstShape),
    /// `&T` argument with `#[poly_setup(<fn_path>, from = <arg>)]`.
    /// Generates a struct field of type `T`, computed once in
    /// `new()` by calling `<fn_path>(<source>)` where `<source>`
    /// is the field-access expression for the named `from` arg.
    Setup(SetupSpec),
    /// SRD-80 PR B.8 — `Value` argument. Polymorphic wire whose
    /// port type is resolved at construction (`new()` takes a
    /// runtime `PortType`). Body sees a cloned `Value`; eval
    /// box/unboxes via the trivial `FromValue<Value>` impl.
    /// Triggers `OutputType::SameAsInput(<this idx>)` when the
    /// return type is also `Value`.
    PolyWire,
    /// SRD-80 PR B.9 — `&[T]` argument (variadic wire). Construction
    /// is runtime-arity (`new(n_wires)`); the macro emits N wire
    /// slots, an `Arity::VariadicWires { min_wires }` FuncSig
    /// entry, and a `variadic_ctor` thunk that builds with `n`
    /// at compile time.
    Variadic(VariadicElement),
}

/// Element type of a `&[T]` variadic arg. Determines the
/// per-element port type, whether the node stays JIT-eligible,
/// and how `eval()` materialises the slice for the body call.
#[derive(Clone, Copy, PartialEq, Eq)]
enum VariadicElement {
    U64,
    F64,
    Bool,
    BorrowedStr,
    OwnedString,
    /// `&[Value]` — polymorphic per-element type. The body sees
    /// each element as the polydat runtime carrier; type
    /// inspection / coercion is the body's responsibility.
    Value,
}

impl VariadicElement {
    fn port_type_tokens(self) -> TokenStream2 {
        // For Value variadics we declare the per-slot port type
        // as Str (the most common stringy use case — printf,
        // str_concat). The body deals with type coercion via
        // its own dispatch on the Value variant.
        match self {
            VariadicElement::U64           => quote!(polydat::ast::PortType::U64),
            VariadicElement::F64           => quote!(polydat::ast::PortType::F64),
            VariadicElement::Bool          => quote!(polydat::ast::PortType::Bool),
            VariadicElement::BorrowedStr   => quote!(polydat::ast::PortType::Str),
            VariadicElement::OwnedString   => quote!(polydat::ast::PortType::Str),
            VariadicElement::Value         => quote!(polydat::ast::PortType::Str),
        }
    }

    /// JIT-eligible elements that fit the u64 buffer. Str/String/
    /// Value can't ride the JIT path.
    fn as_jit_element(self) -> Option<JitType> {
        match self {
            VariadicElement::U64  => Some(JitType::U64),
            VariadicElement::F64  => Some(JitType::F64),
            VariadicElement::Bool => Some(JitType::Bool),
            VariadicElement::BorrowedStr
            | VariadicElement::OwnedString
            | VariadicElement::Value => None,
        }
    }

    /// Expression that converts a single `&Value` to the body's
    /// element type. Used to build the per-call slice in eval().
    fn extract_from_value(self) -> TokenStream2 {
        match self {
            VariadicElement::U64           => quote!(|v: &polydat::ast::Value| v.as_u64()),
            VariadicElement::F64           => quote!(|v: &polydat::ast::Value| v.as_f64()),
            VariadicElement::Bool          => quote!(|v: &polydat::ast::Value| v.as_bool()),
            VariadicElement::BorrowedStr   => quote!(|v: &polydat::ast::Value| v.as_str()),
            VariadicElement::OwnedString   => quote!(|v: &polydat::ast::Value| v.as_str().to_string()),
            VariadicElement::Value         => quote!(|v: &polydat::ast::Value| v.clone()),
        }
    }
}

#[derive(Clone)]
struct SetupSpec {
    /// `T` — the type the field stores (inner type of `&T`).
    inner_ty: Type,
    /// Operator-provided constructor path, e.g.
    /// `ParsedPattern::from_pattern`.
    setup_fn: syn::Expr,
    /// Name of the const arg whose field-value is passed to
    /// `setup_fn`. Single-source only; nodes needing derived
    /// state from multiple consts inline-compute in the body
    /// (see InvLerp / Remap as examples).
    source_arg: syn::Ident,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ConstShape {
    U64,
    F64,
    Bool,
    Str,
}

impl ConstShape {
    /// Token stream for the `SlotType::Const*` variant.
    fn slot_type_tokens(self) -> TokenStream2 {
        match self {
            ConstShape::U64  => quote!(polydat::ast::SlotType::ConstU64),
            ConstShape::F64  => quote!(polydat::ast::SlotType::ConstF64),
            ConstShape::Bool => quote!(polydat::ast::SlotType::ConstU64),
            ConstShape::Str  => quote!(polydat::ast::SlotType::ConstStr),
        }
    }

    /// Token stream for the struct field type that stores the
    /// captured const value. `Const<&str>` → `String` (owned
    /// backing store). Other shapes are Copy and stored
    /// directly.
    fn field_type_tokens(self) -> TokenStream2 {
        match self {
            ConstShape::U64  => quote!(u64),
            ConstShape::F64  => quote!(f64),
            ConstShape::Bool => quote!(bool),
            ConstShape::Str  => quote!(String),
        }
    }

    /// Token stream that extracts a value from a `ConstArg`.
    /// `c` is the `ConstArg` binding in scope at the call site.
    fn extract_from_const_arg(self, c: TokenStream2) -> TokenStream2 {
        match self {
            ConstShape::U64  => quote!(#c.as_u64()),
            ConstShape::F64  => quote!(#c.as_f64()),
            ConstShape::Bool => quote!(#c.as_u64() != 0),
            ConstShape::Str  => quote!(#c.as_str().to_string()),
        }
    }

    /// Token stream that wraps a struct-field expression as
    /// `Const<T>` for handoff into the user's function body.
    /// `field_ref` is the borrow / value expression for the
    /// stored field (e.g. `&self.pattern` or `self.seed`).
    fn wrap_as_const(self, field_ref: TokenStream2) -> TokenStream2 {
        match self {
            ConstShape::U64  => quote!(polydat::derive_support::Const(#field_ref)),
            ConstShape::F64  => quote!(polydat::derive_support::Const(#field_ref)),
            ConstShape::Bool => quote!(polydat::derive_support::Const(#field_ref)),
            ConstShape::Str  => quote!(polydat::derive_support::Const(#field_ref.as_str())),
        }
    }
}

/// SRD-80 PR B.7 — primitive types that fit the JIT u64 buffer.
/// A node is Phase-2 eligible iff every wire arg / const arg /
/// return type maps to a `JitType` and no `Setup<T>` arg is
/// declared (Setup carries non-primitive derived state).
#[derive(Clone, Copy, PartialEq, Eq)]
enum JitType {
    U64,
    F64,
    Bool,
}

impl JitType {
    /// Tokens reading a typed value from the Phase-2 u64 buffer
    /// at position `idx`. f64/bool are bit-reinterpreted from
    /// the u64 carrier (the buffer-level convention shared with
    /// every existing hand-written `compiled_u64`).
    fn read_from_u64_buffer(self, idx: usize) -> TokenStream2 {
        let i = syn::Index::from(idx);
        match self {
            JitType::U64  => quote!(inputs[#i]),
            JitType::F64  => quote!(f64::from_bits(inputs[#i])),
            JitType::Bool => quote!(inputs[#i] != 0),
        }
    }

    /// Tokens writing a typed value back into the Phase-2 u64
    /// output buffer at `outputs[0]`. Inverse of the read.
    fn write_to_u64_buffer(self, result: TokenStream2) -> TokenStream2 {
        match self {
            JitType::U64  => quote!(outputs[0] = #result;),
            JitType::F64  => quote!(outputs[0] = (#result).to_bits();),
            JitType::Bool => quote!(outputs[0] = if #result { 1 } else { 0 };),
        }
    }

    /// Tokens encoding the captured Copy value of a const field
    /// as a `u64` for `jit_constants()` (Phase-3 classifier).
    fn const_field_as_u64(self, field_ref: TokenStream2) -> TokenStream2 {
        match self {
            JitType::U64  => quote!(#field_ref),
            JitType::F64  => quote!((#field_ref).to_bits()),
            JitType::Bool => quote!(if #field_ref { 1 } else { 0 }),
        }
    }
}

/// Map a `ConstShape` to its JIT-compatible primitive carrier,
/// or `None` if the shape can't live in the u64 buffer.
fn const_shape_to_jit_type(s: ConstShape) -> Option<JitType> {
    match s {
        ConstShape::U64  => Some(JitType::U64),
        ConstShape::F64  => Some(JitType::F64),
        ConstShape::Bool => Some(JitType::Bool),
        ConstShape::Str  => None,
    }
}

/// Map a wire arg's declared Rust type to its JIT carrier, or
/// `None` for types that can't fit in the buffer.
fn wire_type_to_jit_type(ty: &Type) -> Option<JitType> {
    let s = type_to_string(ty);
    match s.as_str() {
        "u64"  => Some(JitType::U64),
        "f64"  => Some(JitType::F64),
        "bool" => Some(JitType::Bool),
        _      => None,
    }
}

/// Detect `Const<T>` in arg-type position. Returns `Some(shape)`
/// for recognized inner types; `None` for bare types (wire) or
/// unrecognized shapes. The recognition is structural — matches
/// the last segment of the path as `Const` with a single
/// generic argument resolving to a primitive type the macro
/// supports.
fn classify_type(ty: &Type) -> Option<ConstShape> {
    let syn::Type::Path(p) = ty else { return None; };
    let last = p.path.segments.last()?;
    if last.ident != "Const" { return None; }
    let syn::PathArguments::AngleBracketed(args) = &last.arguments else { return None; };
    let inner = args.args.iter().find_map(|a| {
        if let syn::GenericArgument::Type(t) = a { Some(t) } else { None }
    })?;
    let s = type_to_string(inner);
    match s.as_str() {
        "u64"            => Some(ConstShape::U64),
        "f64"            => Some(ConstShape::F64),
        "bool"           => Some(ConstShape::Bool),
        "& str" | "&str" => Some(ConstShape::Str),
        _ => None,
    }
}

/// Extract a `#[poly_default(EXPR)]` attribute from an arg's
/// outer attributes, if present. Returns the inner expression
/// token stream so the build closure can use it as the
/// fallback when the runtime `consts` slice is shorter than
/// the declared param list.
fn parse_poly_default(attrs: &[syn::Attribute]) -> syn::Result<Option<syn::Expr>> {
    for attr in attrs {
        if !attr.path().is_ident("poly_default") { continue; }
        let expr: syn::Expr = attr.parse_args()?;
        return Ok(Some(expr));
    }
    Ok(None)
}

/// Extract a `#[constraint(<Variant>)]` attribute. SRD-80 PR
/// B.14 — wire-arg constraint metadata. The variant name
/// matches `ConstConstraint::*` (e.g. `NonZeroU64`,
/// `PositiveFiniteF64`). Strict-wire mode reads this metadata
/// to auto-insert assertion nodes upstream.
fn parse_wire_constraint(attrs: &[syn::Attribute]) -> syn::Result<Option<Ident>> {
    for attr in attrs {
        if !attr.path().is_ident("constraint") { continue; }
        let variant: Ident = attr.parse_args()?;
        return Ok(Some(variant));
    }
    Ok(None)
}

/// Extract a `#[poly_const(<fn_expr>, from = <arg>)]` attribute
/// from an arg's outer attributes, if present. Returns the
/// constructor expression and the source-arg identifier.
///
/// SRD-80 PR B.12 — `poly_setup` retired in favour of
/// `poly_const`. Conceptually, both primitive `Const<T>` args
/// and `&T` args with `#[poly_const]` are workload-compile-
/// time-known values; the umbrella name is "Const".
///
/// Single-source only. Nodes that derive state from multiple
/// consts inline-compute in the body (see InvLerp / Remap),
/// because the multi-source machinery covered a scenario no
/// current node actually needs.
fn parse_poly_const(attrs: &[syn::Attribute]) -> syn::Result<Option<(syn::Expr, syn::Ident)>> {
    for attr in attrs {
        if !attr.path().is_ident("poly_const") { continue; }
        let parser = |input: syn::parse::ParseStream| -> syn::Result<(syn::Expr, syn::Ident)> {
            let fn_expr: syn::Expr = input.parse()?;
            let _comma: Token![,] = input.parse()?;
            let from_kw: syn::Ident = input.parse()?;
            if from_kw != "from" {
                return Err(syn::Error::new_spanned(
                    from_kw,
                    "#[poly_const(...)] requires a `from = <arg>` clause",
                ));
            }
            let _eq: Token![=] = input.parse()?;
            let source_arg: syn::Ident = input.parse()?;
            Ok((fn_expr, source_arg))
        };
        let parsed = attr.parse_args_with(parser)?;
        return Ok(Some(parsed));
    }
    Ok(None)
}

/// Detect `&T` for some `T` in arg-type position. Returns
/// `Some(inner_t)` on match, `None` otherwise. Used for the
/// PR B.6 setup-arg dispatch.
fn classify_borrowed(ty: &Type) -> Option<Type> {
    let syn::Type::Reference(r) = ty else { return None; };
    if r.mutability.is_some() { return None; }
    Some((*r.elem).clone())
}

/// Detect `Value` in arg-type position. SRD-80 PR B.8 —
/// polymorphic wire dispatch. Matches the last path segment
/// being `Value`, so both `Value` and `polydat::ast::Value`
/// (and any other fully-qualified path ending in `Value`) work.
fn classify_polywire(ty: &Type) -> bool {
    let syn::Type::Path(p) = ty else { return false; };
    p.path.segments.last().map(|s| s.ident == "Value").unwrap_or(false)
}

/// SRD-80 PR B.11/B.13 — structural classifier for the
/// wrapper-typed wire arg shapes. Returns the matching wire
/// kind, or `None` if the type isn't one of the recognised
/// wrapper shapes.
#[derive(Clone, Copy, PartialEq, Eq)]
enum WrapperWire {
    Bytes,
    Json,
    /// `Arc<T>` for some T that isn't `[u8]` or `serde_json::Value`.
    /// Inline-downcast in arg_bindings; inline-upcast in
    /// result_to_outputs. Handle dispatch.
    Handle,
    /// One of the six typed vector variants: `VecF32` / `VecI32`
    /// / `VecF64` / `VecI64` / `VecF16` / `VecI16`. The macro
    /// emits the matching `PortType::Vec*`; the FromValue /
    /// IntoValue impls in derive_support are autogenerated from
    /// a macro_rules! expansion per element type.
    VecF32, VecI32, VecF64, VecI64, VecF16, VecI16,
}

fn classify_wrapper_wire(ty: &Type) -> Option<WrapperWire> {
    // SRD-80 PR B.13 — typed vectors. Check first to catch
    // `Vec<f32>` etc. before they fall into Handle territory
    // (which is the catch-all for Arc<T>).
    if let Some(kind) = classify_vec_wire(ty) {
        return Some(kind);
    }

    // `Arc<[u8]>` — Arc with [u8] generic.
    if let Some(inner) = strip_arc(ty)
        && let syn::Type::Slice(slc) = inner
        && let syn::Type::Path(p) = &*slc.elem
        && p.path.is_ident("u8")
    {
        return Some(WrapperWire::Bytes);
    }
    // `Arc<serde_json::Value>` / `Arc<Value>` (last segment).
    if let Some(inner) = strip_arc(ty)
        && let syn::Type::Path(p) = inner
        && last_segment_is(p, "Value")
        && path_contains_segment(p, "serde_json")
    {
        return Some(WrapperWire::Json);
    }
    // Any other `Arc<T>` is a Handle.
    if strip_arc(ty).is_some() {
        return Some(WrapperWire::Handle);
    }
    // `Vec<u8>`.
    if let syn::Type::Path(p) = ty
        && let Some(last) = p.path.segments.last()
        && last.ident == "Vec"
        && let syn::PathArguments::AngleBracketed(args) = &last.arguments
        && let Some(syn::GenericArgument::Type(syn::Type::Path(elem))) = args.args.first()
        && elem.path.is_ident("u8")
    {
        return Some(WrapperWire::Bytes);
    }
    // `&[u8]` — borrowed bytes.
    if let syn::Type::Reference(r) = ty
        && r.mutability.is_none()
        && let syn::Type::Slice(slc) = &*r.elem
        && let syn::Type::Path(p) = &*slc.elem
        && p.path.is_ident("u8")
    {
        return Some(WrapperWire::Bytes);
    }
    // `&serde_json::Value`.
    if let syn::Type::Reference(r) = ty
        && r.mutability.is_none()
        && let syn::Type::Path(p) = &*r.elem
        && last_segment_is(p, "Value")
        && path_contains_segment(p, "serde_json")
    {
        return Some(WrapperWire::Json);
    }
    None
}

fn strip_arc(ty: &Type) -> Option<&Type> {
    let syn::Type::Path(p) = ty else { return None; };
    let last = p.path.segments.last()?;
    if last.ident != "Arc" { return None; }
    let syn::PathArguments::AngleBracketed(args) = &last.arguments else { return None; };
    args.args.iter().find_map(|a| match a {
        syn::GenericArgument::Type(t) => Some(t),
        _ => None,
    })
}

fn last_segment_is(p: &syn::TypePath, name: &str) -> bool {
    p.path.segments.last().map(|s| s.ident == name).unwrap_or(false)
}

fn path_contains_segment(p: &syn::TypePath, name: &str) -> bool {
    p.path.segments.iter().any(|s| s.ident == name)
}

/// For a `Handle` arg, extract the inner T (the downcast target).
fn extract_handle_inner(ty: &Type) -> Option<Type> {
    strip_arc(ty).cloned()
}

/// SRD-80 PR B.13 — typed-vector classifier. Recognises three
/// input shapes per element type: `SliceArc<T>`, `Vec<T>`,
/// `&[T]`. The element type's last path segment selects the
/// `WrapperWire::Vec*` variant.
fn classify_vec_wire(ty: &Type) -> Option<WrapperWire> {
    // Try to extract the element type from any of the three shapes:
    let elem: Type = if let Some(elem) = strip_vec(ty) {
        elem.clone()
    } else if let Some(elem) = strip_slice_arc(ty) {
        elem.clone()
    } else if let Some(elem) = strip_borrowed_slice(ty) {
        elem.clone()
    } else {
        return None;
    };

    let syn::Type::Path(p) = &elem else { return None; };
    let last = p.path.segments.last()?;
    // f16 lives in the `half` crate, so the element path can
    // be `f16`, `half::f16`, etc. — match by last segment.
    match last.ident.to_string().as_str() {
        "f32" => Some(WrapperWire::VecF32),
        "i32" => Some(WrapperWire::VecI32),
        "f64" => Some(WrapperWire::VecF64),
        "i64" => Some(WrapperWire::VecI64),
        "f16" => Some(WrapperWire::VecF16),
        "i16" => Some(WrapperWire::VecI16),
        _ => None,
    }
}

fn strip_vec(ty: &Type) -> Option<&Type> {
    let syn::Type::Path(p) = ty else { return None; };
    let last = p.path.segments.last()?;
    if last.ident != "Vec" { return None; }
    let syn::PathArguments::AngleBracketed(args) = &last.arguments else { return None; };
    args.args.iter().find_map(|a| match a {
        syn::GenericArgument::Type(t) => Some(t),
        _ => None,
    })
}

fn strip_slice_arc(ty: &Type) -> Option<&Type> {
    let syn::Type::Path(p) = ty else { return None; };
    let last = p.path.segments.last()?;
    if last.ident != "SliceArc" { return None; }
    let syn::PathArguments::AngleBracketed(args) = &last.arguments else { return None; };
    args.args.iter().find_map(|a| match a {
        syn::GenericArgument::Type(t) => Some(t),
        _ => None,
    })
}

fn strip_borrowed_slice(ty: &Type) -> Option<&Type> {
    let syn::Type::Reference(r) = ty else { return None; };
    if r.mutability.is_some() { return None; }
    let syn::Type::Slice(slc) = &*r.elem else { return None; };
    Some(&slc.elem)
}

/// Detect `&[T]` (variadic) in arg-type position. SRD-80 PR B.9.
/// Returns the recognised element type for the supported primitive
/// element set; `None` otherwise (bare reference, non-slice, or
/// unsupported element type). Structural match — works regardless
/// of how the inner type is written (`Value` / `polydat::ast::Value`).
fn classify_variadic(ty: &Type) -> Option<VariadicElement> {
    let syn::Type::Reference(r) = ty else { return None; };
    if r.mutability.is_some() { return None; }
    let syn::Type::Slice(s) = &*r.elem else { return None; };

    // `&[&str]` — element is a Type::Reference to a path "str".
    if let syn::Type::Reference(inner_r) = &*s.elem
        && inner_r.mutability.is_none()
        && let syn::Type::Path(p) = &*inner_r.elem
        && p.path.is_ident("str")
    {
        return Some(VariadicElement::BorrowedStr);
    }

    // Bare-path element types — match by last path segment ident.
    let syn::Type::Path(p) = &*s.elem else { return None; };
    let last = p.path.segments.last()?;
    if !last.arguments.is_empty() { return None; }
    match last.ident.to_string().as_str() {
        "u64"    => Some(VariadicElement::U64),
        "f64"    => Some(VariadicElement::F64),
        "bool"   => Some(VariadicElement::Bool),
        "String" => Some(VariadicElement::OwnedString),
        "Value"  => Some(VariadicElement::Value),
        _ => None,
    }
}

fn generate(func: ItemFn, attrs: NodeAttrs) -> syn::Result<TokenStream2> {
    let fn_name = &func.sig.ident;
    // SRD-80 PR B.7: strip `r#` from raw identifiers (`fn r#mod`,
    // `fn r#type`, etc.) so the DSL function name and the
    // PascalCase struct identifier both come out clean.
    let fn_name_raw = fn_name.to_string();
    let func_name_str = fn_name_raw
        .strip_prefix("r#")
        .unwrap_or(&fn_name_raw)
        .to_string();
    let struct_name = format_ident!("{}", to_camel_case(&func_name_str));
    let category = &attrs.category;

    // Classify each function arg: wire or const? Reject any
    // unsupported pattern (self, complex destructuring, bare-
    // type wires the macro doesn't recognize).
    let mut args: Vec<ClassifiedArg> = Vec::new();
    for input in &func.sig.inputs {
        match input {
            FnArg::Receiver(r) => {
                return Err(syn::Error::new_spanned(
                    r,
                    "#[polydat_node] does not support `self` parameters yet; \
                     state-bearing nodes are deferred to a later PR.",
                ));
            }
            FnArg::Typed(pat_ty) => {
                let ident = match &*pat_ty.pat {
                    Pat::Ident(p) => p.ident.clone(),
                    other => {
                        return Err(syn::Error::new_spanned(
                            other,
                            "#[polydat_node] requires plain identifier parameters; \
                             pattern matching in argument position isn't supported.",
                        ));
                    }
                };
                let declared_ty = (*pat_ty.ty).clone();
                let default_value = parse_poly_default(&pat_ty.attrs)?;
                let setup_attr = parse_poly_const(&pat_ty.attrs)?;
                let wire_constraint = parse_wire_constraint(&pat_ty.attrs)?;
                let is_polywire = classify_polywire(&declared_ty);
                let variadic_elem = classify_variadic(&declared_ty);

                let kind = if let Some(elem) = variadic_elem {
                    if default_value.is_some() || setup_attr.is_some() || is_polywire {
                        return Err(syn::Error::new_spanned(
                            pat_ty,
                            "variadic `&[T]` args don't combine with \
                             #[poly_default(...)], #[poly_const(...)], or `Value`.",
                        ));
                    }
                    ArgKind::Variadic(elem)
                } else if is_polywire {
                    if default_value.is_some() || setup_attr.is_some() {
                        return Err(syn::Error::new_spanned(
                            pat_ty,
                            "`Value` args (PolyWire) don't combine with \
                             #[poly_default(...)] or #[poly_const(...)]; \
                             the runtime port type comes from the upstream wire \
                             at construction time.",
                        ));
                    }
                    ArgKind::PolyWire
                } else if let Some((setup_fn, source_arg)) = setup_attr {
                    // `#[poly_const(...)]` requires `&T` arg type.
                    let inner_ty = classify_borrowed(&declared_ty)
                        .ok_or_else(|| syn::Error::new_spanned(
                            &declared_ty,
                            "#[poly_const(...)] requires the argument type to be \
                             a borrow `&T` — the macro stores the computed `T` \
                             in a struct field and hands the body a borrow each \
                             eval.",
                        ))?;
                    if default_value.is_some() {
                        return Err(syn::Error::new_spanned(
                            pat_ty,
                            "#[poly_default(...)] cannot combine with \
                             #[poly_const(...)]; defaults belong on the source \
                             Const arg, not on the derived setup arg.",
                        ));
                    }
                    ArgKind::Setup(SetupSpec { inner_ty, setup_fn, source_arg })
                } else {
                    match classify_type(&declared_ty) {
                        Some(shape) => ArgKind::Const(shape),
                        None => {
                            if default_value.is_some() {
                                return Err(syn::Error::new_spanned(
                                    pat_ty,
                                    "#[poly_default(...)] only applies to const args \
                                     (`Const<T>`); bare-type wire args don't have \
                                     assembly-time defaults.",
                                ));
                            }
                            ArgKind::Wire
                        }
                    }
                };
                args.push(ClassifiedArg { name: ident, declared_ty, kind, default_value, wire_constraint });
            }
        }
    }

    // Map a bare wire-arg type to a PortType expression.
    let wire_port_type_for = |ty: &Type| -> syn::Result<TokenStream2> {
        // SRD-80 PR B.11: wrapper types via structural match.
        if let Some(kind) = classify_wrapper_wire(ty) {
            return Ok(match kind {
                WrapperWire::Bytes  => quote!(polydat::ast::PortType::Bytes),
                WrapperWire::Json   => quote!(polydat::ast::PortType::Json),
                WrapperWire::Handle => quote!(polydat::ast::PortType::Handle),
                WrapperWire::VecF32 => quote!(polydat::ast::PortType::VecF32),
                WrapperWire::VecI32 => quote!(polydat::ast::PortType::VecI32),
                WrapperWire::VecF64 => quote!(polydat::ast::PortType::VecF64),
                WrapperWire::VecI64 => quote!(polydat::ast::PortType::VecI64),
                WrapperWire::VecF16 => quote!(polydat::ast::PortType::VecF16),
                WrapperWire::VecI16 => quote!(polydat::ast::PortType::VecI16),
            });
        }
        let s = type_to_string(ty);
        match s.as_str() {
            "u64"          => Ok(quote!(polydat::ast::PortType::U64)),
            "f64"          => Ok(quote!(polydat::ast::PortType::F64)),
            // SRD-80 PR B.14 — narrow integer + f32 widths.
            "u32"          => Ok(quote!(polydat::ast::PortType::U32)),
            "i32"          => Ok(quote!(polydat::ast::PortType::I32)),
            "i64"          => Ok(quote!(polydat::ast::PortType::I64)),
            "f32"          => Ok(quote!(polydat::ast::PortType::F32)),
            "bool"         => Ok(quote!(polydat::ast::PortType::Bool)),
            "& str" | "&str" | "String"
                           => Ok(quote!(polydat::ast::PortType::Str)),
            other => Err(syn::Error::new_spanned(
                ty,
                format!(
                    "#[polydat_node] does not yet support wire-arg type `{other}`. \
                     Supported wire types: u64, f64, bool, &str/String, \
                     Arc<[u8]>/Vec<u8>/&[u8] (Bytes), \
                     Arc<serde_json::Value>/&serde_json::Value (Json), \
                     Arc<T> (Handle). \
                     For const args, wrap as `Const<T>`. Ext arrives in a later PR.",
                ),
            )),
        }
    };

    // Build the NodeMeta `ins` slot list — one entry per arg,
    // dispatched by kind. Wire args get `Slot::Wire(...)`;
    // const args get `Slot::Const { ... }` populated with the
    // captured field value at construction time.
    let mut slot_exprs: Vec<TokenStream2> = Vec::new();
    for a in &args {
        let name_str = a.name.to_string();
        match &a.kind {
            ArgKind::Wire => {
                let pt = wire_port_type_for(&a.declared_ty)?;
                // SRD-80 PR B.14: optional `#[constraint(Variant)]`.
                let port_expr = if let Some(variant) = &a.wire_constraint {
                    quote! {
                        polydat::ast::Port::new(#name_str, #pt)
                            .with_constraint(
                                polydat::dsl::const_constraints::ConstConstraint::#variant)
                    }
                } else {
                    quote!(polydat::ast::Port::new(#name_str, #pt))
                };
                slot_exprs.push(quote! {
                    polydat::ast::Slot::Wire(#port_expr)
                });
            }
            ArgKind::Const(shape) => {
                let field_name = &a.name;
                let const_value_ctor = match shape {
                    ConstShape::U64  => quote!(polydat::ast::ConstValue::U64(#field_name)),
                    ConstShape::F64  => quote!(polydat::ast::ConstValue::F64(#field_name)),
                    ConstShape::Bool => quote!(polydat::ast::ConstValue::U64(if #field_name { 1 } else { 0 })),
                    ConstShape::Str  => quote!(polydat::ast::ConstValue::Str(#field_name.clone())),
                };
                slot_exprs.push(quote! {
                    polydat::ast::Slot::Const {
                        name: #name_str.into(),
                        value: #const_value_ctor,
                    }
                });
            }
            ArgKind::Setup(_) => {
                // Setup args don't appear in NodeMeta.ins —
                // they're derived state, not declared params.
                // The source Const arg already carries the
                // introspectable value.
            }
            ArgKind::PolyWire => {
                // Port type is the `<argname>_type` parameter
                // passed to `new()`; the variable is in scope
                // because the macro emits it as a `new()` param.
                let pt_param = format_ident!("{}_type", a.name);
                slot_exprs.push(quote! {
                    polydat::ast::Slot::Wire(polydat::ast::Port::new(
                        #name_str, #pt_param))
                });
            }
            ArgKind::Variadic(_) => {
                // Variadic emits per-element slots at construction.
                // The macro generates `extend` into the slot vec
                // from a 0..n_wires loop. Each slot is named
                // `<argname>_<i>` to keep the meta diff-friendly.
                // (Handled in the new() body via a separate pass —
                // see `variadic_slot_extends` below.)
            }
        }
    }
    // For each variadic arg, also emit a runtime loop that
    // appends N slots to the `Slot` vec.
    let variadic_slot_extends: Vec<TokenStream2> = args.iter()
        .filter_map(|a| match &a.kind {
            ArgKind::Variadic(elem) => {
                let name_str = a.name.to_string();
                let pt = elem.port_type_tokens();
                Some(quote! {
                    for __i in 0..n_wires {
                        ins.push(polydat::ast::Slot::Wire(
                            polydat::ast::Port::new(
                                format!("{}_{__i}", #name_str),
                                #pt,
                            )));
                    }
                })
            }
            _ => None,
        })
        .collect();

    // Build the FuncSig.params static slice — one ParamSpec
    // per declared arg (Wire and Const). Setup args don't
    // appear in the FuncSig surface — they're macro-internal
    // derived state.
    let param_specs: Vec<TokenStream2> = args.iter()
        .filter_map(|a| {
            let name_str = a.name.to_string();
            // Variadic args declare `required: false` — they accept
            // any count from `variadic_min` (default 0) upward.
            let required = match &a.kind {
                ArgKind::Variadic(_) => false,
                _ => a.default_value.is_none(),
            };
            let slot_type = match &a.kind {
                ArgKind::Wire | ArgKind::PolyWire | ArgKind::Variadic(_) => quote!(polydat::ast::SlotType::Wire),
                ArgKind::Const(shape) => shape.slot_type_tokens(),
                ArgKind::Setup(_) => return None,
            };
            Some(quote! {
                polydat::dsl::registry::ParamSpec {
                    name: #name_str,
                    slot_type: #slot_type,
                    required: #required,
                    example: #name_str,
                    constraint: None,
                }
            })
        })
        .collect();

    // Output type. The simple case requires a concrete return
    // type (-> T); unit / unspecified isn't supported.
    let ret_ty = match &func.sig.output {
        ReturnType::Default => {
            return Err(syn::Error::new_spanned(
                &func.sig,
                "#[polydat_node] requires an explicit return type; \
                 nodes always produce a value.",
            ));
        }
        ReturnType::Type(_, t) => (**t).clone(),
    };
    let ret_is_polywire = classify_polywire(&ret_ty);

    // SRD-80 PR B.10: detect tuple-typed return for multi-output.
    let tuple_ret_elems: Option<Vec<Type>> = match &ret_ty {
        syn::Type::Tuple(t) => Some(t.elems.iter().cloned().collect()),
        _ => None,
    };

    if tuple_ret_elems.is_some() && ret_is_polywire {
        // Type::Tuple isn't Type::Path so this is impossible, but
        // belt-and-suspenders for future return-shape changes.
        return Err(syn::Error::new_spanned(
            &ret_ty,
            "tuple return + PolyWire don't compose (SameAsInput is a \
             single-output dispatch).",
        ));
    }

    // SRD-80 PR B.8: when the return type is `Value`, the
    // output port type tracks the first PolyWire arg's runtime
    // port type (SameAsInput). Otherwise it's the primitive's
    // fixed PortType.
    let first_polywire_idx: Option<usize> = args.iter()
        .enumerate()
        .find(|(_, a)| matches!(a.kind, ArgKind::PolyWire))
        .map(|(i, _)| i);

    // Per-output port-type token streams, indexed positionally.
    // Single-output → 1-element vec; tuple → N elements.
    let output_port_types: Vec<TokenStream2> = if let Some(elems) = &tuple_ret_elems {
        elems.iter()
            .map(wire_port_type_for)
            .collect::<syn::Result<Vec<_>>>()?
    } else if ret_is_polywire {
        let polywire_arg = args.iter().find(|a| matches!(a.kind, ArgKind::PolyWire))
            .ok_or_else(|| syn::Error::new_spanned(
                &ret_ty,
                "function returns `Value` but has no `Value` arg — the macro \
                 needs at least one PolyWire arg to source the runtime port \
                 type for the output (SameAsInput dispatch).",
            ))?;
        let pt_ident = format_ident!("{}_type", polywire_arg.name);
        vec![quote!(#pt_ident)]
    } else {
        vec![wire_port_type_for(&ret_ty)?]
    };

    // SRD-80 PR B.10: output names. Operator-supplied via
    // `output_names(a, b, c)`; falls back to `out_0`, `out_1`, ...
    // for tuple returns; just "output" for single returns.
    let output_names_strs: Vec<String> = match (&tuple_ret_elems, &attrs.output_names) {
        (Some(elems), Some(names)) => {
            if names.len() != elems.len() {
                return Err(syn::Error::new_spanned(
                    &ret_ty,
                    format!(
                        "tuple return has {} elements but `output_names(...)` \
                         lists {}; lengths must match.",
                        elems.len(), names.len(),
                    ),
                ));
            }
            names.iter().map(|n| n.to_string()).collect()
        }
        (Some(elems), None) => (0..elems.len()).map(|i| format!("out_{i}")).collect(),
        (None, Some(names)) if names.len() != 1 => {
            return Err(syn::Error::new_spanned(
                &ret_ty,
                "single-output return doesn't accept multi-name `output_names(...)`.",
            ));
        }
        (None, Some(names)) => vec![names[0].to_string()],
        (None, None) => vec!["output".to_string()],
    };

    let output_count = output_port_types.len();
    let output_count_lit = syn::LitInt::new(&output_count.to_string(), proc_macro2::Span::call_site());

    let output_type_tokens: TokenStream2 = if ret_is_polywire {
        let idx = first_polywire_idx.unwrap();
        let i = syn::Index::from(idx);
        quote!(polydat::dsl::registry::OutputType::SameAsInput(#i))
    } else {
        quote!(polydat::dsl::registry::OutputType::Fixed)
    };

    // Struct fields. Wire/PolyWire/Variadic → no field (arity
    // reflected in `meta.ins.len()`); Const → owned-typed field;
    // Setup → field of the borrowed inner type.
    let struct_fields: Vec<TokenStream2> = args.iter()
        .filter_map(|a| match &a.kind {
            ArgKind::Wire | ArgKind::PolyWire | ArgKind::Variadic(_) => None,
            ArgKind::Const(shape) => {
                let n = &a.name;
                let ft = shape.field_type_tokens();
                Some(quote!(pub #n: #ft))
            }
            ArgKind::Setup(spec) => {
                let n = &a.name;
                let ty = &spec.inner_ty;
                Some(quote!(pub #n: #ty))
            }
        })
        .collect();

    // `new(<polywire_types..>, <consts..>)` constructor params, in
    // declaration order. Const args contribute their owned-typed
    // value; PolyWire args contribute a `<argname>_type: PortType`
    // parameter that names the runtime port type the assembler
    // resolved for the upstream wire. Setup args are computed
    // inside new(), not parameters.
    let new_params: Vec<TokenStream2> = args.iter()
        .filter_map(|a| match &a.kind {
            ArgKind::Wire => None,
            ArgKind::Const(shape) => {
                let n = &a.name;
                let ft = shape.field_type_tokens();
                Some(quote!(#n: #ft))
            }
            ArgKind::Setup(_) => None,
            ArgKind::PolyWire => {
                let n = format_ident!("{}_type", a.name);
                Some(quote!(#n: polydat::ast::PortType))
            }
            // Variadic args don't add their OWN per-arg param —
            // the variadic-arity is supplied via a SINGLE
            // `n_wires: usize` parameter appended once at the end
            // (see `variadic_n_wires_param` below).
            ArgKind::Variadic(_) => None,
        })
        .collect();

    // SRD-80 PR B.9: append a single `n_wires: usize` parameter
    // to `new()` when the function declares any variadic arg.
    // Only one variadic arg is supported in this PR.
    let has_variadic = args.iter().any(|a| matches!(a.kind, ArgKind::Variadic(_)));
    let new_params: Vec<TokenStream2> = if has_variadic {
        let mut v = new_params;
        v.push(quote!(n_wires: usize));
        v
    } else {
        new_params
    };

    // Build a lookup from arg name → ConstShape so the Setup
    // pre-compute step can dispatch on the source's shape to
    // produce the right access expression.
    let const_shape_by_name: std::collections::HashMap<String, ConstShape> = args.iter()
        .filter_map(|a| match &a.kind {
            ArgKind::Const(shape) => Some((a.name.to_string(), *shape)),
            _ => None,
        })
        .collect();

    // Setup pre-compute lines, emitted at the top of `new()`
    // BEFORE `Self { ... }` so they can borrow the const
    // locals before those values are moved into self.
    let setup_precomputes: Vec<TokenStream2> = args.iter()
        .filter_map(|a| match &a.kind {
            ArgKind::Wire | ArgKind::Const(_) | ArgKind::PolyWire | ArgKind::Variadic(_) => None,
            ArgKind::Setup(spec) => {
                let n = &a.name;
                let setup_fn = &spec.setup_fn;
                let src = &spec.source_arg;
                let src_shape = const_shape_by_name.get(&src.to_string());
                let src_expr = match src_shape {
                    Some(ConstShape::Str)  => quote!(#src.as_str()),
                    Some(_)                => quote!(#src),
                    None => {
                        return Some(syn::Error::new(
                            src.span(),
                            format!(
                                "#[poly_const(... from = {src})] — `{src}` \
                                 is not declared as a `Const<T>` arg in the \
                                 same function signature."),
                        ).to_compile_error());
                    }
                };
                Some(quote! {
                    let #n = #setup_fn(#src_expr);
                })
            }
        })
        .collect();

    // Self { ... } field-init list. Const args use field-name
    // shorthand; Setup args use the local computed above.
    // Wire/PolyWire contribute nothing (no field).
    let new_field_inits: Vec<TokenStream2> = args.iter()
        .filter_map(|a| match &a.kind {
            ArgKind::Wire | ArgKind::PolyWire | ArgKind::Variadic(_) => None,
            ArgKind::Const(_) | ArgKind::Setup(_) => {
                let n = &a.name;
                Some(quote!(#n))
            }
        })
        .collect();

    // Per-arg bindings the eval body sees. Wire args unbox via
    // FromValue; const args wrap the struct field as `Const<T>`
    // so the user's body code sees the wrapper type matching
    // its function signature.
    let mut wire_idx = 0usize;
    let arg_bindings: Vec<TokenStream2> = args.iter()
        .map(|a| {
            let n = &a.name;
            match &a.kind {
                ArgKind::Wire => {
                    let idx = syn::Index::from(wire_idx);
                    wire_idx += 1;
                    let ty = &a.declared_ty;
                    // SRD-80 PR B.11: `Arc<T>` Handle args
                    // bypass FromValue (no blanket impl —
                    // would conflict with the Json
                    // Arc<serde_json::Value> impl). Emit the
                    // downcast inline.
                    if classify_wrapper_wire(ty) == Some(WrapperWire::Handle) {
                        let inner = extract_handle_inner(ty)
                            .expect("Handle classification implies Arc<T> shape");
                        quote! {
                            let #n: std::sync::Arc<#inner> = match &inputs[#idx] {
                                polydat::ast::Value::Handle(arc) => arc.clone()
                                    .downcast::<#inner>()
                                    .expect("Handle type mismatch — wiring bug"),
                                other => panic!("expected Handle, got {other:?}"),
                            };
                        }
                    } else {
                        quote! {
                            let #n = <#ty as polydat::derive_support::FromValue>::from_value(&inputs[#idx]);
                        }
                    }
                }
                ArgKind::Const(shape) => {
                    let wrap = shape.wrap_as_const(quote!(self.#n));
                    quote! {
                        let #n = #wrap;
                    }
                }
                ArgKind::Setup(_) => {
                    // Setup arg: body sees a borrow of the
                    // construction-time computed field. No
                    // wrapping needed — the field is the
                    // user's named type and `&T` matches the
                    // function-signature borrow.
                    quote! {
                        let #n = &self.#n;
                    }
                }
                ArgKind::PolyWire => {
                    // SRD-80 PR B.8: PolyWire — clone the
                    // `Value` directly into a local. Body sees
                    // an owned `Value`.
                    let idx = syn::Index::from(wire_idx);
                    wire_idx += 1;
                    quote! {
                        let #n: polydat::ast::Value = inputs[#idx].clone();
                    }
                }
                ArgKind::Variadic(elem) => {
                    // SRD-80 PR B.9: variadic — materialise
                    // a Vec<T> from the inputs slice (per-element
                    // extraction), then bind the body local as
                    // `&[T]`. Allocates a Vec per call on the
                    // Phase 1 path; the JIT path bypasses this.
                    let extractor = elem.extract_from_value();
                    let owned = format_ident!("__{}_owned", a.name);
                    quote! {
                        let #owned: Vec<_> = inputs.iter().map(#extractor).collect();
                        let #n: &[_] = #owned.as_slice();
                    }
                }
            }
        })
        .collect();

    // Build closure const-extraction logic. For each const arg
    // (in declaration order), pull from `consts: &[ConstArg]`
    // by index; fall back to the `poly_default` value if the
    // slice is shorter than the const arg list.
    let mut const_idx_for_extract = 0usize;
    let const_extracts: Vec<TokenStream2> = args.iter()
        .filter_map(|a| match &a.kind {
            ArgKind::Wire | ArgKind::Setup(_) | ArgKind::PolyWire | ArgKind::Variadic(_) => None,
            ArgKind::Const(shape) => {
                let n = &a.name;
                let i = const_idx_for_extract;
                const_idx_for_extract += 1;
                let i_lit = syn::Index::from(i);
                let extract_present = shape.extract_from_const_arg(quote!(c));
                let fallback = match &a.default_value {
                    Some(default_expr) => {
                        // Default is an expression evaluating to
                        // the field type (`u64`, `f64`, `bool`,
                        // `String`). For Str: the expression
                        // should produce a `&str` or `String`; we
                        // call `.to_string()` to land on owned.
                        match shape {
                            ConstShape::Str => quote!((#default_expr).to_string()),
                            _ => quote!(#default_expr),
                        }
                    }
                    None => {
                        let msg = format!(
                            "missing required const arg '{n}' for function '{func_name_str}'");
                        quote!(return Some(Err(#msg.to_string())))
                    }
                };
                Some(quote! {
                    let #n: _ = match consts.get(#i_lit) {
                        Some(c) => #extract_present,
                        None => #fallback,
                    };
                })
            }
        })
        .collect();

    // Names to pass to `Self::new(...)` from the build closure,
    // in declaration order. Const → `<name>`; PolyWire →
    // `<name>_type` (the local extracted from `wire_types`).
    let mut new_call_args: Vec<TokenStream2> = args.iter()
        .filter_map(|a| match &a.kind {
            ArgKind::Wire | ArgKind::Setup(_) | ArgKind::Variadic(_) => None,
            ArgKind::Const(_) => {
                let n = &a.name;
                Some(quote!(#n))
            }
            ArgKind::PolyWire => {
                let n = format_ident!("{}_type", a.name);
                Some(quote!(#n))
            }
        })
        .collect();
    if has_variadic {
        new_call_args.push(quote!(n_wires));
    }

    // SRD-80 PR B.9: when the function has a variadic arg,
    // extract `n_wires` from the `_wires: &[WireRef]` slice in
    // the build closure. The whole `_wires.len()` is the variadic
    // count (this PR supports one variadic arg only — when
    // multi-variadic lands, this extraction needs the per-arg
    // split logic).
    let variadic_n_wires_extract: TokenStream2 = if has_variadic {
        quote! { let n_wires: usize = _wires.len(); }
    } else {
        quote!()
    };

    // SRD-80 PR B.8: extract resolved PolyWire port types from
    // the `wire_types: &[PortType]` slice the assembler hands
    // the build closure. Wire/PolyWire share the same slot
    // counter (both consume a wire input position); we count
    // through args in declaration order.
    let polywire_extracts: Vec<TokenStream2> = {
        let mut wire_idx = 0usize;
        let mut out = Vec::new();
        for a in &args {
            match &a.kind {
                ArgKind::Wire => { wire_idx += 1; }
                ArgKind::Variadic(_) => {
                    // Variadic args consume the REMAINDER of the
                    // wire slots. Only one variadic arg supported
                    // in this PR.
                    wire_idx += 0;  // no positional increment
                }
                ArgKind::PolyWire => {
                    let pt_ident = format_ident!("{}_type", a.name);
                    let i = syn::Index::from(wire_idx);
                    let n_str = a.name.to_string();
                    let err = format!(
                        "polywire arg '{n_str}' for '{func_name_str}': assembler \
                         did not resolve a port type at wire index {wire_idx}");
                    out.push(quote! {
                        let #pt_ident: polydat::ast::PortType = match _wire_types.get(#i) {
                            Some(t) => *t,
                            None => return Some(Err(#err.to_string())),
                        };
                    });
                    wire_idx += 1;
                }
                ArgKind::Const(_) | ArgKind::Setup(_) => {}
            }
        }
        out
    };

    let block = &func.block;

    // Emit `Default` only when there are no const args AND no
    // setup args. Both require captured values to construct.
    let has_non_wire = args.iter().any(|a| !matches!(a.kind, ArgKind::Wire));
    let default_impl = if has_non_wire {
        quote!()
    } else {
        quote! {
            impl Default for #struct_name {
                fn default() -> Self { Self::new() }
            }
        }
    };

    // ── SRD-80 PR B.7 — JIT eligibility + hook emission ──
    //
    // A node is Phase-2 eligible when every arg + return maps
    // to a `JitType` and no `Setup<T>` arg is declared (Setup
    // carries non-primitive derived state that can't fit a u64
    // buffer). Override attributes (`compiled_u64 = ...`,
    // `jit_constants = ...`) bypass eligibility — they win
    // unconditionally. `no_jit` blocks macro emission when no
    // override is present.

    let has_setup = args.iter().any(|a| matches!(a.kind, ArgKind::Setup(_)));
    let ret_jit_type = wire_type_to_jit_type(&ret_ty);

    let arg_jit_types: Option<Vec<JitType>> = if has_setup {
        None
    } else {
        args.iter()
            .map(|a| match &a.kind {
                ArgKind::Wire => wire_type_to_jit_type(&a.declared_ty),
                ArgKind::Const(shape) => const_shape_to_jit_type(*shape),
                ArgKind::Setup(_) | ArgKind::PolyWire => None,
                // SRD-80 PR B.9: variadic JIT — only `&[u64]`
                // rides the Phase 2 closure cleanly (the buffer
                // IS the slice). For f64/bool/Str variadics
                // the closure would need a per-call Vec
                // allocation to bit-reinterpret; skip in this PR.
                ArgKind::Variadic(elem) => match elem {
                    VariadicElement::U64 => Some(JitType::U64),
                    _ => None,
                },
            })
            .collect()
    };

    // SRD-80 PR B.10/B.15: tuple return becomes JIT-eligible
    // when every element is JIT-eligible. The compiled_u64
    // closure destructures the result and writes each element
    // to its `outputs[i]` slot via the matching JitType.
    let tuple_ret_jit_types: Option<Vec<JitType>> = tuple_ret_elems.as_ref()
        .and_then(|elems| {
            elems.iter()
                .map(wire_type_to_jit_type)
                .collect::<Option<Vec<_>>>()
        });

    let jit_eligible = arg_jit_types.is_some()
        && (ret_jit_type.is_some() || tuple_ret_jit_types.is_some());

    let emit_compiled_u64 = attrs.compiled_u64_override.is_some()
        || (jit_eligible && !attrs.no_jit);
    let emit_jit_constants = attrs.jit_constants_override.is_some()
        || (jit_eligible && !attrs.no_jit);

    // Body sharing: extract the function body into a private
    // associated fn `__polydat_body` when JIT is emitted. Both
    // `eval()` (Value boxing path) and `compiled_u64()` (u64
    // buffer path) call it. Single source of truth.
    //
    // When JIT is not emitted, the body stays inlined inside
    // `eval()`'s current `#[allow(unused_variables)]` block
    // (Setup-bearing nodes need this — their body references
    // setup-derived locals via `let n = &self.n` bindings).

    let use_shared_body = jit_eligible && (emit_compiled_u64 || !attrs.no_jit);

    // Body-fn parameter list — every arg in its DECLARED form
    // (wire as bare type, const as `Const<T>`, setup as `&T`).
    let body_params: Vec<TokenStream2> = args.iter()
        .map(|a| {
            let n = &a.name;
            let t = &a.declared_ty;
            quote!(#n: #t)
        })
        .collect();

    let body_fn_def: TokenStream2 = if use_shared_body {
        quote! {
            #[inline(always)]
            #[allow(unused_variables)]
            fn __polydat_body( #( #body_params ),* ) -> #ret_ty #block
        }
    } else {
        quote!()
    };

    // Helper: emit `outputs[idx] = <conversion>(value)` for a
    // given element type. SRD-80 PR B.11: Handle (`Arc<T>` for
    // non-special T) bypasses IntoValue (no blanket impl) and
    // upcasts inline via `Value::handle`.
    let output_assign = |idx_lit: TokenStream2, elem_ty: &Type, local: TokenStream2| -> TokenStream2 {
        if classify_wrapper_wire(elem_ty) == Some(WrapperWire::Handle) {
            quote! {
                outputs[#idx_lit] = polydat::ast::Value::handle(#local);
            }
        } else {
            quote! {
                outputs[#idx_lit] = <#elem_ty as polydat::derive_support::IntoValue>::into_value(#local);
            }
        }
    };

    // SRD-80 PR B.10/B.11: result → outputs translation. For
    // single-output, write `outputs[0] = ...(result)`. For
    // tuple-output, destructure and per-element write.
    let result_to_outputs: TokenStream2 = if let Some(elems) = &tuple_ret_elems {
        let locals: Vec<Ident> = (0..elems.len())
            .map(|i| format_ident!("__r_{}", i))
            .collect();
        let writes: Vec<TokenStream2> = elems.iter().enumerate()
            .map(|(i, elem_ty)| {
                let local = &locals[i];
                let idx = syn::Index::from(i);
                output_assign(quote!(#idx), elem_ty, quote!(#local))
            })
            .collect();
        quote! {
            let ( #( #locals ),* ) = result;
            #( #writes )*
        }
    } else {
        output_assign(quote!(0), &ret_ty, quote!(result))
    };

    // Eval-path arg bindings + body-call. When JIT is emitted,
    // eval() unboxes from Values and calls `__polydat_body`.
    // When JIT is not emitted, the body stays inline in
    // `eval()` for back-compat with Setup-bearing nodes.
    let eval_body: TokenStream2 = if use_shared_body {
        let arg_names: Vec<&syn::Ident> = args.iter().map(|a| &a.name).collect();
        quote! {
            #[allow(unused_variables)]
            {
                #( #arg_bindings )*
                let result: #ret_ty = Self::__polydat_body( #( #arg_names ),* );
                #result_to_outputs
            }
        }
    } else {
        quote! {
            #[allow(unused_variables)]
            {
                #( #arg_bindings )*
                let result: #ret_ty = (|| #block)();
                #result_to_outputs
            }
        }
    };

    // compiled_u64() emission. Three cases:
    //   (a) Override path supplied → call it.
    //   (b) JIT eligible and not opted out → emit closure that
    //       reads from u64 buffer, captures const fields by
    //       Copy, calls __polydat_body, writes back.
    //   (c) Otherwise → don't override the trait default
    //       (returns None).
    let compiled_u64_impl: TokenStream2 = if let Some(path) = &attrs.compiled_u64_override {
        quote! {
            fn compiled_u64(&self) -> Option<polydat::ast::CompiledU64Op> {
                Some(Box::new(#path))
            }
        }
    } else if jit_eligible && !attrs.no_jit {
        // Per-arg jit handling. Wire args read from inputs at
        // the next sequential index. Const args capture by Copy
        // from self at closure-creation time, then re-wrap as
        // `Const<T>` inside the closure for handoff to body.
        let jit_types = arg_jit_types.as_ref().unwrap();
        let mut wire_buf_idx = 0usize;

        let captures: Vec<TokenStream2> = args.iter()
            .filter_map(|a| match &a.kind {
                ArgKind::Wire | ArgKind::Variadic(_) => None,
                ArgKind::Const(_) => {
                    let n = &a.name;
                    Some(quote!(let #n = self.#n;))
                }
                ArgKind::Setup(_) | ArgKind::PolyWire => {
                    unreachable!("setup/polywire excludes JIT eligibility")
                }
            })
            .collect();

        let arg_reads: Vec<TokenStream2> = args.iter().zip(jit_types.iter())
            .map(|(a, jt)| {
                let n = &a.name;
                let _ = jt;
                match &a.kind {
                    ArgKind::Wire => {
                        let read = jt.read_from_u64_buffer(wire_buf_idx);
                        wire_buf_idx += 1;
                        quote!(let #n = #read;)
                    }
                    ArgKind::Const(_) => {
                        // `n` is already captured by Copy above;
                        // wrap as the body's `Const<T>` form.
                        quote!(let #n = polydat::derive_support::Const(#n);)
                    }
                    ArgKind::Variadic(_) => {
                        // SRD-80 PR B.9: u64 variadic — pass the
                        // whole `inputs: &[u64]` buffer directly
                        // to the body. Zero allocation, zero conversion.
                        // (Non-u64 variadics aren't JIT-eligible —
                        // this branch is only reached for u64 elems.)
                        quote!(let #n: &[u64] = inputs;)
                    }
                    ArgKind::Setup(_) | ArgKind::PolyWire => unreachable!(),
                }
            })
            .collect();

        let arg_names: Vec<&syn::Ident> = args.iter().map(|a| &a.name).collect();
        // SRD-80 PR B.15: multi-output write. For single-output
        // ret, `write` emits `outputs[0] = bits(result)`. For
        // tuple-output, destructure into locals and emit a
        // per-element write line.
        let write = if let Some(tuple_jits) = &tuple_ret_jit_types {
            let locals: Vec<Ident> = (0..tuple_jits.len())
                .map(|i| format_ident!("__jit_r_{}", i))
                .collect();
            let writes: Vec<TokenStream2> = tuple_jits.iter().enumerate()
                .map(|(i, jt)| {
                    let local = &locals[i];
                    let mut out_idx = jt.write_to_u64_buffer(quote!(#local));
                    // `write_to_u64_buffer` always emits `outputs[0] = ...`.
                    // For multi-output we need to rewrite to `outputs[i] = ...`.
                    // Rebuild from the JitType's conversion directly:
                    let idx = syn::Index::from(i);
                    out_idx = match jt {
                        JitType::U64 => quote!(outputs[#idx] = #local;),
                        JitType::F64 => quote!(outputs[#idx] = #local.to_bits();),
                        JitType::Bool => quote!(outputs[#idx] = if #local { 1 } else { 0 };),
                    };
                    out_idx
                })
                .collect();
            quote! {
                let ( #( #locals ),* ) = result;
                #( #writes )*
            }
        } else {
            let ret_jit = ret_jit_type.unwrap();
            ret_jit.write_to_u64_buffer(quote!(result))
        };

        quote! {
            fn compiled_u64(&self) -> Option<polydat::ast::CompiledU64Op> {
                #( #captures )*
                Some(Box::new(move |inputs: &[u64], outputs: &mut [u64]| {
                    #( #arg_reads )*
                    let result: #ret_ty = Self::__polydat_body( #( #arg_names ),* );
                    #write
                }))
            }
        }
    } else {
        quote!()
    };

    // jit_constants() emission. Three cases:
    //   (a) Override path supplied → call it with `&self`.
    //   (b) JIT eligible and not opted out → emit a Vec<u64>
    //       built from const fields in declaration order,
    //       bit-reinterpreting f64 and 0/1-encoding bool.
    //   (c) Otherwise → don't override the trait default.
    let jit_constants_impl: TokenStream2 = if let Some(path) = &attrs.jit_constants_override {
        quote! {
            fn jit_constants(&self) -> Vec<u64> {
                #path(self)
            }
        }
    } else if emit_jit_constants {
        let const_encodings: Vec<TokenStream2> = args.iter()
            .filter_map(|a| match &a.kind {
                ArgKind::Const(shape) => {
                    let jt = const_shape_to_jit_type(*shape)?;
                    let n = &a.name;
                    Some(jt.const_field_as_u64(quote!(self.#n)))
                }
                _ => None,
            })
            .collect();

        quote! {
            fn jit_constants(&self) -> Vec<u64> {
                vec![ #( #const_encodings ),* ]
            }
        }
    } else {
        quote!()
    };

    // purity() emission — only when attribute is set; otherwise
    // the trait default (`Pure`) is used.
    //
    // Two attribute shapes:
    //   - `Expr::Path` (e.g. `Nondeterministic`)
    //     → `Purity::Nondeterministic`
    //   - `Expr::Call` (e.g. `SideChannel(LogBuffer)`)
    //     → `Purity::SideChannel { sink: SideChannelSink::LogBuffer }`
    let purity_impl: TokenStream2 = match &attrs.purity {
        None => quote!(),
        Some(syn::Expr::Path(p)) => {
            let variant = &p.path;
            quote! {
                fn purity(&self) -> polydat::ast::Purity {
                    polydat::ast::Purity::#variant
                }
            }
        }
        Some(syn::Expr::Call(c)) => {
            // SRD-80 PR B.7/B.11: dispatch on the variant head.
            //   SideChannel(<SideChannelSink variant>) →
            //     Purity::SideChannel { sink: SideChannelSink::<arg> }
            //   Nondeterministic(<&'static str reason>) →
            //     Purity::Nondeterministic { reason: <arg> }
            let syn::Expr::Path(head_path) = &*c.func else {
                return Err(syn::Error::new_spanned(
                    &c.func,
                    "purity call-form expects a Purity variant ident as the head.",
                ));
            };
            let head_ident = head_path.path.get_ident().ok_or_else(|| {
                syn::Error::new_spanned(
                    &c.func,
                    "purity call-form head must be a single Purity variant ident.",
                )
            })?;
            let arg = c.args.first().ok_or_else(|| {
                syn::Error::new_spanned(
                    c,
                    "purity call-form requires one argument.")
            })?;
            match head_ident.to_string().as_str() {
                "SideChannel" => quote! {
                    fn purity(&self) -> polydat::ast::Purity {
                        polydat::ast::Purity::SideChannel {
                            sink: polydat::ast::SideChannelSink::#arg,
                        }
                    }
                },
                "Nondeterministic" => quote! {
                    fn purity(&self) -> polydat::ast::Purity {
                        polydat::ast::Purity::Nondeterministic { reason: #arg }
                    }
                },
                other => return Err(syn::Error::new_spanned(
                    head_ident,
                    format!(
                        "purity call-form head `{other}` not recognized. \
                         Use `SideChannel(<sink>)` or `Nondeterministic(<reason>)`."),
                )),
            }
        }
        Some(other) => {
            return Err(syn::Error::new_spanned(
                other,
                "purity attribute must be a Purity variant path or call form",
            ));
        }
    };

    let _ = emit_compiled_u64; // referenced via the conditionals above

    // SRD-80 PR B.9: conditional FuncSig fields.
    let identity_field: TokenStream2 = if let Some(expr) = &attrs.identity {
        quote!(Some(#expr))
    } else {
        quote!(None)
    };

    // `variadic_ctor` only emitted for pure-variadic nodes (no
    // const args, no PolyWire). Const+variadic mixing would need
    // the ctor to thread the const values through — defer to a
    // future PR.
    let has_const_arg = args.iter().any(|a| matches!(a.kind, ArgKind::Const(_)));
    let has_polywire = args.iter().any(|a| matches!(a.kind, ArgKind::PolyWire));
    let variadic_ctor_field: TokenStream2 = if has_variadic && !has_const_arg && !has_polywire {
        quote!(Some(|n| Box::new(#struct_name::new(n))))
    } else {
        quote!(None)
    };

    let arity_field: TokenStream2 = if has_variadic {
        let min_wires = attrs.variadic_min.as_ref()
            .map(|v| quote!(#v))
            .unwrap_or_else(|| quote!(0));
        quote!(polydat::dsl::registry::Arity::VariadicWires { min_wires: #min_wires })
    } else {
        quote!(polydat::dsl::registry::Arity::Fixed)
    };

    let commutativity_field: TokenStream2 = if let Some(c) = &attrs.commutativity {
        quote!(polydat::ast::Commutativity::#c)
    } else {
        quote!(polydat::ast::Commutativity::Positional)
    };

    let result = quote! {
        pub struct #struct_name {
            meta: polydat::ast::NodeMeta,
            #( #struct_fields, )*
        }

        #default_impl

        impl #struct_name {
            pub fn new( #( #new_params ),* ) -> Self {
                // SRD-80 PR B.6: setup pre-computes (FnOnce-
                // equivalent — emitted once by the macro,
                // never reachable by any other code path).
                #( #setup_precomputes )*
                // Build the `ins` slot list. Const args and
                // singleton wires already appear in `slot_exprs`;
                // variadic args append N slots per `n_wires`.
                let mut ins: Vec<polydat::ast::Slot> = vec![ #( #slot_exprs ),* ];
                #( #variadic_slot_extends )*
                let outs = vec![ #(
                    polydat::ast::Port::new(#output_names_strs, #output_port_types)
                ),* ];
                Self {
                    meta: polydat::ast::NodeMeta {
                        name: #func_name_str.into(),
                        ins,
                        outs,
                    },
                    #( #new_field_inits, )*
                }
            }

            // SRD-80 PR B.7: shared `__polydat_body` extracted
            // when the node is JIT-eligible. Both `eval()` and
            // `compiled_u64()` call it. Empty token stream when
            // JIT is not emitted (body stays inlined in eval).
            #body_fn_def
        }

        impl polydat::ast::PolydatNode for #struct_name {
            fn meta(&self) -> &polydat::ast::NodeMeta { &self.meta }

            fn eval(
                &self,
                inputs: &[polydat::ast::Value],
                outputs: &mut [polydat::ast::Value],
            ) {
                #eval_body
            }

            #compiled_u64_impl
            #jit_constants_impl
            #purity_impl
        }

        // SRD-80 PR B.2/B.3/B.5 — link-time registration via
        // the existing `NodeRegistration` inventory channel.
        // The build closure pulls const args from the runtime
        // `consts` slice, falling back to per-arg
        // `#[poly_default(...)]` values if the slice is short.
        const _: () = {
            static SIGS: &[polydat::dsl::registry::FuncSig] = &[
                polydat::dsl::registry::FuncSig {
                    name: #func_name_str,
                    category: polydat::dsl::registry::FuncCategory::#category,
                    outputs: #output_count_lit,
                    description: "",
                    help: "",
                    identity: #identity_field,
                    variadic_ctor: #variadic_ctor_field,
                    params: &[ #( #param_specs ),* ],
                    arity: #arity_field,
                    commutativity: #commutativity_field,
                    default_resolver: None,
                    output_type: #output_type_tokens,
                },
            ];

            fn signatures() -> &'static [polydat::dsl::registry::FuncSig] { SIGS }

            fn build(
                name: &str,
                _wires: &[polydat::compile::assembly::WireRef],
                _wire_types: &[polydat::ast::PortType],
                consts: &[polydat::dsl::factory::ConstArg],
            ) -> Option<Result<Box<dyn polydat::ast::PolydatNode>, String>> {
                if name != #func_name_str { return None; }
                #( #const_extracts )*
                #( #polywire_extracts )*
                #variadic_n_wires_extract
                Some(Ok(Box::new(#struct_name::new( #( #new_call_args ),* ))))
            }

            ::polydat::inventory::submit! {
                polydat::dsl::registry::NodeRegistration {
                    signatures,
                    build,
                    validate: None,
                }
            }
        };
    };

    Ok(result)
}

/// `snake_case` → `PascalCase` (for the generated struct name).
fn to_camel_case(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut up = true;
    for c in s.chars() {
        if c == '_' { up = true; continue; }
        if up { out.extend(c.to_uppercase()); up = false; }
        else { out.push(c); }
    }
    out
}

/// Stringify a `syn::Type` minimally — used for primitive-type
/// dispatch. Not a robust pretty-printer; only handles the
/// shapes the simple-case allows (bare path, `&str`, `String`).
fn type_to_string(ty: &Type) -> String {
    use quote::ToTokens;
    let mut s = String::new();
    for t in ty.to_token_stream() {
        s.push_str(&t.to_string());
        s.push(' ');
    }
    s.trim().to_string()
}
