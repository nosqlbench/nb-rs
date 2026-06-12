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

    // SRD-80b Phase D1 — generic-over-Wire fanout. When the
    // operator declares `instantiate(T1, T2, ...)`, the macro
    // emits one full registration per type (per-instantiation
    // struct + impl + NodeRegistration). The DSL function name
    // stays shared; the Rust struct names get type-derived
    // suffixes (`PassthroughU64`, `PassthroughF64`, ...).
    if attrs.instantiate.is_empty() {
        return match generate(func, attrs, None) {
            Ok(ts) => ts.into(),
            Err(e) => e.to_compile_error().into(),
        };
    }
    match instantiate_and_generate(func, attrs) {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

/// SRD-80b Phase D1 — fan out a generic-over-Wire function into
/// one full instantiation per concrete type listed in
/// `instantiate(...)`. Requires exactly one type parameter on
/// the function; substitutes that parameter throughout args /
/// return / body and emits a generate() call per instantiation
/// with a type-derived struct-name suffix.
fn instantiate_and_generate(
    func: ItemFn,
    attrs: NodeAttrs,
) -> syn::Result<TokenStream2> {
    let generics = &func.sig.generics;
    // Exactly one type parameter is required. (Lifetimes and
    // const params are not supported for instantiation.)
    let type_params: Vec<&syn::TypeParam> = generics.type_params().collect();
    if type_params.len() != 1 {
        return Err(syn::Error::new_spanned(
            &func.sig,
            format!(
                "#[polydat_node(instantiate(...))] requires exactly one type \
                 parameter on the function (got {}). Declare the function as \
                 `fn name<T: Wire>(...) -> ...` and list concrete `Wire`-impl \
                 types in the `instantiate(...)` clause.",
                type_params.len(),
            ),
        ));
    }
    let type_param_ident = type_params[0].ident.clone();
    let dsl_name = func.sig.ident.to_string();

    let mut out = TokenStream2::new();
    // Clone attrs minus the `instantiate` clause so the
    // downstream generate() doesn't try to fan out again.
    let mut shared_attrs = attrs.clone();
    let instantiations = std::mem::take(&mut shared_attrs.instantiate);

    for concrete in instantiations {
        let mut inst_func = func.clone();
        // Strip the generic parameter — the substituted form is
        // no longer generic.
        inst_func.sig.generics.params.clear();
        inst_func.sig.generics.where_clause = None;
        // Substitute T -> concrete throughout the function.
        let mut subst = TypeSubst {
            type_param: type_param_ident.clone(),
            concrete: concrete.clone(),
        };
        syn::visit_mut::VisitMut::visit_item_fn_mut(&mut subst, &mut inst_func);
        // Rename to a per-instantiation Rust identifier so the
        // generated struct name carries the type suffix. The
        // operator-facing DSL name stays `dsl_name`, passed
        // through generate()'s name_override.
        let suffix = type_suffix(&concrete);
        let new_ident = syn::Ident::new(
            &format!("{dsl_name}_{}", suffix.to_lowercase()),
            inst_func.sig.ident.span(),
        );
        inst_func.sig.ident = new_ident;
        let emit = generate(inst_func, shared_attrs.clone(), Some(dsl_name.clone()))?;
        out.extend(emit);
    }
    Ok(out)
}

/// Derive a struct-name suffix from a Rust type. Used by Phase
/// D1 instantiate to disambiguate the per-instantiation struct
/// names. `u64` → "U64"; `String` → "String"; `Arc<[u8]>` →
/// "ArcU8"; `SliceArc<f32>` → "SliceArcF32". The strategy
/// strips angle brackets / refs / punctuation and uppercases
/// each segment's first character.
fn type_suffix(ty: &Type) -> String {
    let raw = type_to_string(ty);
    let mut out = String::new();
    let mut capitalize_next = true;
    for c in raw.chars() {
        if c.is_alphanumeric() {
            if capitalize_next {
                out.extend(c.to_uppercase());
                capitalize_next = false;
            } else {
                out.push(c);
            }
        } else {
            capitalize_next = true;
        }
    }
    if out.is_empty() { "Inst".to_string() } else { out }
}

/// syn visitor that substitutes a single named type parameter
/// with a concrete type throughout an item function. Used by
/// the SRD-80b Phase D1 fanout to produce per-instantiation
/// copies of a generic-over-Wire function.
struct TypeSubst {
    type_param: syn::Ident,
    concrete: Type,
}

impl syn::visit_mut::VisitMut for TypeSubst {
    fn visit_type_mut(&mut self, ty: &mut Type) {
        if let Type::Path(p) = ty {
            if p.qself.is_none() && p.path.is_ident(&self.type_param) {
                *ty = self.concrete.clone();
                return;
            }
        }
        syn::visit_mut::visit_type_mut(self, ty);
    }
}

// Make NodeAttrs cloneable for the Phase D1 fanout (we need a
// copy per instantiation; the original parsed-once Attrs is the
// shared template).

/// Parsed `#[polydat_node(...)]` attribute parameters.
#[derive(Clone)]
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
    /// SRD-80b Phase F (S18) — `decompose = path`. When set, the
    /// macro emits `impl FusedNode for <Struct>` whose
    /// `decomposed(&self)` delegates to the named free function.
    /// Free-fn signature: `fn(&Self) -> DecomposedGraph`. The
    /// fusion compiler reaches the equivalent unfused subgraph
    /// through this path. Operators with bespoke fusion logic
    /// can still `impl FusedNode` by hand alongside the macro
    /// emission — the attribute is the canonical sugar for the
    /// "decompose by calling one free fn" case.
    decompose: Option<syn::ExprPath>,
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
    /// SRD-80b Phase D1 — generic-over-Wire instantiation policy
    /// (SRD-80b §"Open questions" item 1). For a function
    /// declared `fn pp<T: Wire>(input: T) -> T`, the macro emits
    /// one full instantiation per type listed here (per-instance
    /// struct + impl + NodeRegistration). The DSL function name
    /// is shared across instantiations; per-instantiation build
    /// closures guard on `<T as Wire>::PORT` so only the matching
    /// one claims the call.
    instantiate: Vec<Type>,
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
    let mut decompose: Option<syn::ExprPath> = None;
    let mut purity: Option<syn::Expr> = None;
    let mut identity: Option<syn::Expr> = None;
    let mut commutativity: Option<Ident> = None;
    let mut variadic_min: Option<syn::LitInt> = None;
    let mut output_names: Option<Vec<Ident>> = None;
    let mut instantiate: Vec<Type> = Vec::new();

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
                    "decompose" => {
                        let syn::Expr::Path(p) = &nv.value else {
                            return Err(syn::Error::new_spanned(
                                &nv.value,
                                "`decompose` value must be a path to a free \
                                 function with signature \
                                 `fn(&Self) -> DecomposedGraph`.",
                            ));
                        };
                        decompose = Some(p.clone());
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
                    "instantiate" => {
                        let types: Punctuated<Type, Token![,]> =
                            list.parse_args_with(Punctuated::parse_terminated)?;
                        if types.is_empty() {
                            return Err(syn::Error::new_spanned(
                                &list,
                                "`instantiate(...)` requires at least one type. \
                                 List the concrete `Wire`-impl types that should \
                                 get their own per-instantiation registrations.",
                            ));
                        }
                        instantiate = types.into_iter().collect();
                    }
                    other => {
                        return Err(syn::Error::new_spanned(
                            &key,
                            format!(
                                "#[polydat_node] does not recognize list-form key `{other}`. \
                                 Recognised: `output_names(...)`, `instantiate(...)`.",
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
        decompose,
        purity,
        identity,
        commutativity,
        variadic_min,
        output_names,
        instantiate,
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
    /// SRD-80b Phase C — `Const<Vec<C>>` workload-list const.
    /// Inner ConstShape gives the element type (u64/f64/bool/Str).
    /// The macro emits ONE ParamSpec in the FuncSig with the
    /// inner element's slot type, sets `Arity::VariadicConsts`,
    /// and at build time collects every matching ConstArg from
    /// the tail of `consts[..]` into a `Vec<inner>` field.
    /// Eval hands the body a `Const(self.field.clone())`.
    ConstVec(ConstShape),
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
    /// Names of the const args whose field-values are passed to
    /// `setup_fn`. Empty when declared as `from = ()` — the
    /// setup fn takes no arguments and captures session-static
    /// state (env, system clock, etc.). Length 1 for the common
    /// single-source case (`from = ident`); length N for
    /// multi-source `from = (a, b, c)` per SRD-80b amendment.
    source_args: Vec<syn::Ident>,
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
    I64,
    F64,
    Bool,
    // Narrow widths (alignment §8.1): each rides the u64 slot per
    // its Wire storage convention — unsigned zero-extended, signed
    // sign-extended (through the i64 carrier), floats bit-stuffed.
    // The variant carries enough width information for the buffer
    // read/write tokens to emit the exact narrowing/widening casts.
    U8,
    U16,
    U32,
    I8,
    I16,
    I32,
    F32,
    F16,
}

impl JitType {
    /// Buffer slots this carrier occupies (alignment §8.4
    /// layer 1): 1 — everything rides a single u64.
    fn width(self) -> usize {
        1
    }

    /// Tokens reading a typed value from the Phase-2 u64 buffer
    /// at slot offset `idx` (the prefix sum of the widths of all
    /// preceding wire args). f64/bool are bit-reinterpreted from
    /// the u64 carrier (the buffer-level convention shared with
    /// every existing hand-written `compiled_u64`).
    fn read_from_u64_buffer(self, idx: usize) -> TokenStream2 {
        let i = syn::Index::from(idx);
        match self {
            JitType::U64  => quote!(inputs[#i]),
            JitType::I64  => quote!(inputs[#i] as i64),
            JitType::F64  => quote!(f64::from_bits(inputs[#i])),
            JitType::Bool => quote!(inputs[#i] != 0),
            JitType::U8   => quote!(inputs[#i] as u8),
            JitType::U16  => quote!(inputs[#i] as u16),
            JitType::U32  => quote!(inputs[#i] as u32),
            JitType::I8   => quote!((inputs[#i] as i64) as i8),
            JitType::I16  => quote!((inputs[#i] as i64) as i16),
            JitType::I32  => quote!((inputs[#i] as i64) as i32),
            JitType::F32  => quote!(f32::from_bits(inputs[#i] as u32)),
            JitType::F16  => quote!(polydat::half::f16::from_bits(inputs[#i] as u16)),
        }
    }

    /// Tokens writing a typed value into the Phase-2 u64 output
    /// buffer at slot offset `base`. Inverse of the read.
    fn write_to_u64_buffer_at(self, base: usize, result: TokenStream2) -> TokenStream2 {
        let o = syn::Index::from(base);
        match self {
            JitType::U64  => quote!(outputs[#o] = #result;),
            JitType::I64  => quote!(outputs[#o] = (#result) as u64;),
            JitType::F64  => quote!(outputs[#o] = (#result).to_bits();),
            JitType::Bool => quote!(outputs[#o] = if #result { 1 } else { 0 };),
            JitType::U8 | JitType::U16 | JitType::U32
                => quote!(outputs[#o] = (#result) as u64;),
            JitType::I8 | JitType::I16 | JitType::I32
                => quote!(outputs[#o] = ((#result) as i64) as u64;),
            JitType::F32  => quote!(outputs[#o] = (#result).to_bits() as u64;),
            JitType::F16  => quote!(outputs[#o] = (#result).to_bits() as u64;),
        }
    }

    /// Single-return write at offset 0.
    fn write_to_u64_buffer(self, result: TokenStream2) -> TokenStream2 {
        self.write_to_u64_buffer_at(0, result)
    }

    /// Tokens encoding the captured Copy value of a const field
    /// as a `u64` for `jit_constants()` (Phase-3 classifier).
    fn const_field_as_u64(self, field_ref: TokenStream2) -> TokenStream2 {
        match self {
            JitType::U64  => quote!(#field_ref),
            JitType::I64  => quote!((#field_ref) as u64),
            JitType::F64  => quote!((#field_ref).to_bits()),
            JitType::Bool => quote!(if #field_ref { 1 } else { 0 }),
            JitType::U8 | JitType::U16 | JitType::U32
                => quote!((#field_ref) as u64),
            JitType::I8 | JitType::I16 | JitType::I32
                => quote!(((#field_ref) as i64) as u64),
            JitType::F32 | JitType::F16
                => quote!((#field_ref).to_bits() as u64),
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
        "i64"  => Some(JitType::I64),
        "f64"  => Some(JitType::F64),
        "bool" => Some(JitType::Bool),
        "u8"   => Some(JitType::U8),
        "u16"  => Some(JitType::U16),
        "u32"  => Some(JitType::U32),
        "i8"   => Some(JitType::I8),
        "i16"  => Some(JitType::I16),
        "i32"  => Some(JitType::I32),
        "f32"  => Some(JitType::F32),
        // type_to_string joins every token with a space
        // (`half : : f16`, `[ f32 ; 4 ]`), so the path/array
        // forms compare whitespace-stripped. The two-slot types
        // ride limb pairs per alignment §8.4 layer 1.
        _ => {
            let flat: String = s.split_whitespace().collect();
            match flat.as_str() {
                "half::f16" | "f16" => Some(JitType::F16),
                _ => None,
            }
        }
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

/// SRD-80b Phase C — detect `Const<Vec<T>>` in arg position.
/// Returns the inner element shape on match. Distinct path
/// from [`classify_type`]: the macro recognises the variadic-
/// const shape before the scalar `Const<T>` shape, so a
/// signature using `Const<Vec<u64>>` doesn't get misclassified.
fn classify_const_vec(ty: &Type) -> Option<ConstShape> {
    // Outer must be Const<...>.
    let syn::Type::Path(p) = ty else { return None; };
    let last = p.path.segments.last()?;
    if last.ident != "Const" { return None; }
    let syn::PathArguments::AngleBracketed(args) = &last.arguments else { return None; };
    let inner = args.args.iter().find_map(|a| {
        if let syn::GenericArgument::Type(t) = a { Some(t) } else { None }
    })?;
    // Inner must be Vec<X>.
    let syn::Type::Path(vp) = inner else { return None; };
    let vlast = vp.path.segments.last()?;
    if vlast.ident != "Vec" { return None; }
    let syn::PathArguments::AngleBracketed(vargs) = &vlast.arguments else { return None; };
    let velem = vargs.args.iter().find_map(|a| {
        if let syn::GenericArgument::Type(t) = a { Some(t) } else { None }
    })?;
    let s = type_to_string(velem);
    match s.as_str() {
        "u64"            => Some(ConstShape::U64),
        "f64"            => Some(ConstShape::F64),
        "bool"           => Some(ConstShape::Bool),
        "String"         => Some(ConstShape::Str),
        "& str" | "&str" => Some(ConstShape::Str),
        _ => None,
    }
}

/// SRD-80b dynamic-output shape — detect
/// `DynamicOutputs<T>` in return position. Returns the inner
/// element type `T` on match. The macro pairs this with the
/// function's `Const<Vec<C>>` arg to compute the output port
/// count at construction time.
fn classify_dynamic_outputs(ty: &Type) -> Option<Type> {
    let syn::Type::Path(p) = ty else { return None; };
    let last = p.path.segments.last()?;
    if last.ident != "DynamicOutputs" { return None; }
    let syn::PathArguments::AngleBracketed(args) = &last.arguments else { return None; };
    args.args.iter().find_map(|a| {
        if let syn::GenericArgument::Type(t) = a { Some(t.clone()) } else { None }
    })
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

/// Extract a `#[poly_const(<fn_expr>, from = <source>)]` attribute
/// from an arg's outer attributes, if present. Returns the
/// constructor expression and the source identifiers.
///
/// SRD-80b — `from` accepts three shapes:
///   - `from = ()` — empty source. Setup fn takes no args;
///     captures session-static state (env, system clock).
///   - `from = ident` — single source. Setup fn called as
///     `setup_fn(ident_value)`.
///   - `from = (a, b, c)` — multi-source (SRD-80b amendment).
///     Setup fn called as `setup_fn(a_value, b_value, c_value)`.
///     Order matches the tuple. Each name must reference a
///     `Const<T>` arg declared in the same function signature.
fn parse_poly_const(attrs: &[syn::Attribute]) -> syn::Result<Option<(syn::Expr, Vec<syn::Ident>)>> {
    for attr in attrs {
        if !attr.path().is_ident("poly_const") { continue; }
        let parser = |input: syn::parse::ParseStream| -> syn::Result<(syn::Expr, Vec<syn::Ident>)> {
            let fn_expr: syn::Expr = input.parse()?;
            let _comma: Token![,] = input.parse()?;
            let from_kw: syn::Ident = input.parse()?;
            if from_kw != "from" {
                return Err(syn::Error::new_spanned(
                    from_kw,
                    "#[poly_const(...)] requires a `from = <source>` clause. \
                     Supported shapes: `from = ()` (empty), `from = ident` \
                     (single), `from = (a, b, c)` (multi-source).",
                ));
            }
            let _eq: Token![=] = input.parse()?;
            // Parenthesised forms: `from = ()` or `from = (a, b, c)`.
            if input.peek(syn::token::Paren) {
                let inner;
                let _paren = syn::parenthesized!(inner in input);
                if inner.is_empty() {
                    return Ok((fn_expr, Vec::new()));
                }
                let parsed: Punctuated<syn::Ident, Token![,]> =
                    Punctuated::parse_terminated(&inner)?;
                if parsed.is_empty() {
                    return Err(syn::Error::new_spanned(
                        from_kw,
                        "#[poly_const(..., from = (...))] — the parenthesised \
                         form expects a comma-separated list of source-arg \
                         identifiers, or an empty `()` for session-static \
                         setup.",
                    ));
                }
                return Ok((fn_expr, parsed.into_iter().collect()));
            }
            // Bare `from = ident` — single source.
            let source: syn::Ident = input.parse()?;
            Ok((fn_expr, vec![source]))
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
    // `Arc<str>` — Str port via the dedicated Wire impl. Don't
    // route through Handle (str isn't Sized so the Handle's
    // `Value::handle<T: Sized>` constructor would reject it).
    if let Some(inner) = strip_arc(ty)
        && let syn::Type::Path(p) = inner
        && p.path.is_ident("str")
    {
        return None;
    }
    // `Arc<dyn Any + Send + Sync>` — Handle via the dedicated
    // Wire impl. Fall through to trait dispatch rather than
    // the structural Handle path (which expects a concrete
    // Arc<ConcreteT> for the downcast).
    if let Some(inner) = strip_arc(ty)
        && matches!(inner, syn::Type::TraitObject(_))
    {
        return None;
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

/// `Option<T>` recognition. Returns `true` if the type's last
/// path segment is `Option` with a single generic argument. Used
/// to decide whether to auto-emit `accepts_none_inputs() -> true`
/// — the runtime kernel's SRD-74 Rule 1 short-circuits `Value::None`
/// inputs on opt-in nodes; `Option<T>` wires are the canonical
/// opt-in shape.
fn is_option_arg(ty: &Type) -> bool {
    let syn::Type::Path(p) = ty else { return false; };
    let Some(last) = p.path.segments.last() else { return false; };
    if last.ident != "Option" { return false; }
    matches!(&last.arguments,
        syn::PathArguments::AngleBracketed(args)
            if args.args.iter().any(|a| matches!(a, syn::GenericArgument::Type(_))))
}

/// Borrow-shape detection for SRD-80b Wire cutover. The macro
/// dispatches owned types through `<T as Wire>::extract` / `::inject`;
/// borrow shapes are recognised syntactically and emitted as
/// direct `match`-on-`Value` extraction at the eval call site.
/// This keeps the [`Wire`] trait bound at `Sized + 'static` without
/// needing lifetime parameters.
///
/// Returns the matched `Value::<Variant>(inner)` pattern and the
/// accessor expression that yields the body's expected borrow.
#[derive(Clone)]
enum BorrowWire {
    /// `&str`  → `Value::Str(arc)` → `arc.as_ref()` (`&str`).
    Str,
    /// `&[u8]` → `Value::Bytes(arc)` → `arc.as_ref()` (`&[u8]`).
    Bytes,
    /// `&serde_json::Value` → `Value::Json(j)` → `j.as_ref()`.
    Json,
    /// `&[T]` for T in {f32, i32, f64, i64, f16, i16} — typed
    /// vector borrow. Variant tracked separately so we can emit
    /// the right `Value::Vec*` arm; element type is recovered
    /// from the syntactic recognition.
    Vec(&'static str /* variant name */, TokenStream2 /* PortType expr */),
}

fn is_borrow_wire_shape(ty: &Type) -> Option<BorrowWire> {
    let syn::Type::Reference(r) = ty else { return None; };
    if r.mutability.is_some() { return None; }
    match &*r.elem {
        // `&str`
        syn::Type::Path(p) if p.path.is_ident("str") => Some(BorrowWire::Str),
        // `&[T]` — bytes (T=u8) and typed vectors.
        syn::Type::Slice(slc) => {
            if let syn::Type::Path(p) = &*slc.elem {
                if p.path.is_ident("u8") {
                    return Some(BorrowWire::Bytes);
                }
                let elem_name = p.path.segments.last()?.ident.to_string();
                let (variant, port_expr) = match elem_name.as_str() {
                    "f32" => ("VecF32", quote!(polydat::ast::PortType::VecF32)),
                    "i32" => ("VecI32", quote!(polydat::ast::PortType::VecI32)),
                    "f64" => ("VecF64", quote!(polydat::ast::PortType::VecF64)),
                    "i64" => ("VecI64", quote!(polydat::ast::PortType::VecI64)),
                    "f16" => ("VecF16", quote!(polydat::ast::PortType::VecF16)),
                    "i16" => ("VecI16", quote!(polydat::ast::PortType::VecI16)),
                    "i8" => ("VecI8", quote!(polydat::ast::PortType::VecI8)),
                    _ => return None,
                };
                return Some(BorrowWire::Vec(variant, port_expr));
            }
            None
        }
        // `&serde_json::Value` — recognise by last segment `Value`
        // alongside `serde_json` somewhere in the path.
        syn::Type::Path(p) if last_segment_is(p, "Value")
            && path_contains_segment(p, "serde_json") => Some(BorrowWire::Json),
        _ => None,
    }
}

/// Token stream for extracting a borrow-shape wire from
/// `&inputs[idx]`. The macro emits this directly (no trait
/// dispatch) so the borrow's lifetime is bound to the eval
/// call's `&inputs` borrow naturally — no `unsafe transmute`.
fn borrow_extract_tokens(shape: BorrowWire, input_expr: TokenStream2) -> TokenStream2 {
    match shape {
        BorrowWire::Str => quote! {
            match #input_expr {
                polydat::ast::Value::Str(__arc) => __arc.as_ref(),
                __other => panic!("expected Str wire, got {__other:?}"),
            }
        },
        BorrowWire::Bytes => quote! {
            match #input_expr {
                polydat::ast::Value::Bytes(__arc) => __arc.as_ref(),
                __other => panic!("expected Bytes wire, got {__other:?}"),
            }
        },
        BorrowWire::Json => quote! {
            match #input_expr {
                polydat::ast::Value::Json(__arc) => __arc.as_ref(),
                __other => panic!("expected Json wire, got {__other:?}"),
            }
        },
        BorrowWire::Vec(variant, _port) => {
            let v = syn::Ident::new(variant, proc_macro2::Span::call_site());
            quote! {
                match #input_expr {
                    polydat::ast::Value::#v(__arc) => __arc.as_slice(),
                    __other => panic!(
                        concat!("expected ", stringify!(#v), " wire, got {:?}"),
                        __other),
                }
            }
        }
    }
}

/// Token stream for the static `PortType` of a borrow-shape wire.
fn borrow_port_type(shape: &BorrowWire) -> TokenStream2 {
    match shape {
        BorrowWire::Str  => quote!(polydat::ast::PortType::Str),
        BorrowWire::Bytes => quote!(polydat::ast::PortType::Bytes),
        BorrowWire::Json => quote!(polydat::ast::PortType::Json),
        BorrowWire::Vec(_, port_expr) => port_expr.clone(),
    }
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

/// SRD-80b Phase 5 S16 — detect `Result<T, E>` return type for
/// fallible-construction nodes. Returns `Some(T)` (the Ok type)
/// when the return is a `Result<T, _>`; `None` otherwise. Matches
/// any path ending in `Result` so both bare `Result` and fully
/// qualified `std::result::Result` work.
///
/// The Err arm is consumed for its `Into<String>` projection at
/// emission time, so we don't pin its shape here — any E that
/// satisfies `Into<String>` (including `String` itself) is fine.
fn classify_result_return(ty: &Type) -> Option<Type> {
    let syn::Type::Path(p) = ty else { return None; };
    let last = p.path.segments.last()?;
    if last.ident != "Result" { return None; }
    let syn::PathArguments::AngleBracketed(args) = &last.arguments else { return None; };
    // Two args expected: <Ok, Err>. Tolerate `Result<T>` (rare alias)
    // by requiring at least one type arg.
    let mut tys = args.args.iter().filter_map(|a| match a {
        syn::GenericArgument::Type(t) => Some(t.clone()),
        _ => None,
    });
    tys.next()
}

fn generate(
    func: ItemFn,
    attrs: NodeAttrs,
    dsl_name_override: Option<String>,
) -> syn::Result<TokenStream2> {
    let fn_name = &func.sig.ident;
    // SRD-80 PR B.7: strip `r#` from raw identifiers (`fn r#mod`,
    // `fn r#type`, etc.) so the Rust struct name comes out clean.
    let fn_name_raw = fn_name.to_string();
    let rust_name_str = fn_name_raw
        .strip_prefix("r#")
        .unwrap_or(&fn_name_raw)
        .to_string();
    let struct_name = format_ident!("{}", to_camel_case(&rust_name_str));
    // SRD-80b Phase D1 — when instantiating a generic-over-Wire
    // function, the per-instantiation copies have suffixed Rust
    // names (`passthrough_u64`, `passthrough_f64`) but share a
    // single DSL function name from the original declaration.
    let is_instantiation = dsl_name_override.is_some();
    let func_name_str = dsl_name_override.unwrap_or_else(|| rust_name_str.clone());
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
                } else if let Some((setup_fn, source_args)) = setup_attr {
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
                    ArgKind::Setup(SetupSpec { inner_ty, setup_fn, source_args })
                } else if let Some(inner) = classify_const_vec(&declared_ty) {
                    // SRD-80b Phase C — `Const<Vec<C>>` variadic
                    // workload-list. `poly_default` doesn't apply
                    // (the empty list IS the default); other
                    // attributes don't compose.
                    if default_value.is_some() {
                        return Err(syn::Error::new_spanned(
                            pat_ty,
                            "#[poly_default(...)] cannot combine with \
                             `Const<Vec<C>>`; the empty Vec IS the implicit \
                             default. Use `Const<C>` with a poly_default \
                             literal for a single-value default instead.",
                        ));
                    }
                    if setup_attr.is_some() {
                        return Err(syn::Error::new_spanned(
                            pat_ty,
                            "`Const<Vec<C>>` doesn't combine with \
                             #[poly_const(...)]; route the derived state \
                             from a scalar `Const<C>` source instead.",
                        ));
                    }
                    ArgKind::ConstVec(inner)
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

    // SRD-80b Phase C — `Const<Vec<C>>` consumes the tail of
    // `consts[..]` at build time, so at most one ConstVec arg is
    // allowed per node and it must be the last const arg in
    // declaration order. Validate before emission.
    {
        let const_vec_positions: Vec<usize> = args.iter().enumerate()
            .filter_map(|(i, a)| if matches!(a.kind, ArgKind::ConstVec(_)) { Some(i) } else { None })
            .collect();
        if const_vec_positions.len() > 1 {
            return Err(syn::Error::new_spanned(
                &args[const_vec_positions[1]].declared_ty,
                "#[polydat_node] supports at most one `Const<Vec<C>>` arg \
                 per function; the variadic-const surface consumes the \
                 tail of the consts slice and a second one would have no \
                 entries to claim.",
            ));
        }
        if let Some(&pos) = const_vec_positions.first() {
            // Any Const(_) declared AFTER the ConstVec would never
            // bind (its index ≥ ConstVec's tail-start).
            for later in &args[pos + 1..] {
                if matches!(later.kind, ArgKind::Const(_)) {
                    return Err(syn::Error::new_spanned(
                        &later.declared_ty,
                        "scalar `Const<T>` arg declared after a \
                         `Const<Vec<C>>` arg is unreachable — the variadic \
                         consumes everything from its position to the end \
                         of the consts slice. Move the scalar consts BEFORE \
                         the `Const<Vec<C>>` in the function signature.",
                    ));
                }
            }
        }
    }

    // Map a bare wire-arg type to a PortType expression.
    //
    // SRD-80b: the canonical answer is `<#ty as Wire>::PORT` —
    // any owned type that impls [`Wire`] is admitted, and adding
    // a new wire type means adding one Wire impl (no macro
    // source change). Three exceptions stay structural because
    // they can't be expressed through the trait:
    //
    //   1. Borrow shapes (`&str`, `&[u8]`, `&[T]`,
    //      `&serde_json::Value`) — `Wire` is `Sized + 'static`
    //      so borrowed refs can't impl it. The macro emits the
    //      literal `PortType` here and direct `match`-on-`Value`
    //      extraction elsewhere.
    //
    //   2. `Arc<T>` Handle (non-special T) — would conflict with
    //      the concrete `Arc<[u8]>` / `Arc<serde_json::Value>`
    //      impls if expressed as a blanket. Kept as inline
    //      downcast at the extract site; port type is the static
    //      `Handle`.
    //
    //   3. PolyWire (`Value`-typed wire) — polymorphic at
    //      runtime; no static `PortType`. The `ArgKind::PolyWire`
    //      path handles this independently of `wire_port_type_for`.
    //
    // Everything else — including `Option<T>`, `Ext<T>`, and any
    // future combinator added by impl'ing `Wire` — flows through
    // trait dispatch.
    let wire_port_type_for = |ty: &Type| -> syn::Result<TokenStream2> {
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
        if let Some(borrow) = is_borrow_wire_shape(ty) {
            return Ok(borrow_port_type(&borrow));
        }
        // Fall through to trait dispatch — `<T as Wire>::PORT` is
        // a const associated, evaluable at codegen time. Types
        // without a `Wire` impl produce a clean E0277 at the
        // function's call site, naming the missing trait bound.
        Ok(quote!(<#ty as polydat::derive_support::Wire>::PORT))
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
                let ty = &a.declared_ty;
                // SRD-80 PR B.14: optional `#[constraint(Variant)]`.
                let constraint_chain = if let Some(variant) = &a.wire_constraint {
                    quote! {
                        .with_constraint(
                            polydat::dsl::const_constraints::ConstConstraint::#variant)
                    }
                } else {
                    quote!()
                };
                // SRD-80b in-spirit — `Wire::WIRE_COST` is read
                // from the trait at codegen. Owned/non-borrow
                // wire types route here; borrow shapes don't
                // impl Wire so they get the default Data cost
                // (the WireCost::Config opt-in only applies to
                // owned types wrapped in `Config<T>`).
                let cost_chain = if is_borrow_wire_shape(ty).is_none()
                    && classify_wrapper_wire(ty) != Some(WrapperWire::Handle)
                {
                    quote! {
                        .with_cost(<#ty as polydat::derive_support::Wire>::WIRE_COST)
                    }
                } else {
                    quote!()
                };
                slot_exprs.push(quote! {
                    polydat::ast::Slot::Wire(
                        polydat::ast::Port::new(#name_str, #pt)
                            #constraint_chain
                            #cost_chain
                    )
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
            ArgKind::ConstVec(inner) => {
                // SRD-80b — `Const<Vec<C>>` emits a `Slot::Const`
                // entry when the inner element has a matching
                // `ConstValue::Vec*` variant (u64, f64). This
                // makes the captured list visible to JIT slot-
                // walkers and introspection (`jit_constants_from_slots`).
                // For element types without a parallel
                // `ConstValue` variant (bool, Str), no slot is
                // emitted; the FuncSig's `Arity::VariadicConsts`
                // tracks the surface and the stored Vec<C> field
                // is the canonical storage.
                let field_name = &a.name;
                match inner {
                    ConstShape::U64 => slot_exprs.push(quote! {
                        polydat::ast::Slot::Const {
                            name: #name_str.into(),
                            value: polydat::ast::ConstValue::VecU64(#field_name.clone()),
                        }
                    }),
                    ConstShape::F64 => slot_exprs.push(quote! {
                        polydat::ast::Slot::Const {
                            name: #name_str.into(),
                            value: polydat::ast::ConstValue::VecF64(#field_name.clone()),
                        }
                    }),
                    _ => {}
                }
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
            // `ConstVec` follows the same pattern (empty is valid).
            let required = match &a.kind {
                ArgKind::Variadic(_) | ArgKind::ConstVec(_) => false,
                _ => a.default_value.is_none(),
            };
            let slot_type = match &a.kind {
                ArgKind::Wire | ArgKind::PolyWire | ArgKind::Variadic(_) => quote!(polydat::ast::SlotType::Wire),
                ArgKind::Const(shape) => shape.slot_type_tokens(),
                ArgKind::ConstVec(inner) => inner.slot_type_tokens(),
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
    let declared_ret_ty = match &func.sig.output {
        ReturnType::Default => {
            return Err(syn::Error::new_spanned(
                &func.sig,
                "#[polydat_node] requires an explicit return type; \
                 nodes always produce a value.",
            ));
        }
        ReturnType::Type(_, t) => (**t).clone(),
    };
    // SRD-80b Phase 5 S16 — fallible construction. When the body
    // returns `Result<T, E>`, the macro treats T as the effective
    // node-output type and emits a `try_new(...) -> Result<Self,
    // String>` constructor that runs the body once at
    // construction, caches the Ok value, and propagates Err. Only
    // valid for nodes with no wire/polywire inputs — the body has
    // to be fully resolvable at construction.
    let fallible_inner_ty: Option<Type> = classify_result_return(&declared_ret_ty);
    let is_fallible = fallible_inner_ty.is_some();
    let ret_ty = fallible_inner_ty.clone().unwrap_or_else(|| declared_ret_ty.clone());
    let ret_is_polywire = classify_polywire(&ret_ty);

    if is_fallible {
        // Wire / polywire / variadic inputs are not supported in
        // fallible mode: the body executes once at construction,
        // not per-eval. Const args are fine — they're all known
        // by the time `try_new` runs.
        for a in &args {
            match &a.kind {
                ArgKind::Wire | ArgKind::PolyWire | ArgKind::Variadic(_) => {
                    return Err(syn::Error::new_spanned(
                        &a.declared_ty,
                        "fallible-construction nodes (-> Result<T, E>) must \
                         have only Const args. Wire/PolyWire/variadic inputs \
                         can't be evaluated at construction time. Use the \
                         #[poly_const(setup_fn, from = ...)] shape instead \
                         when per-eval inputs are needed.",
                    ));
                }
                ArgKind::Setup(_) | ArgKind::Const(_) | ArgKind::ConstVec(_) => {}
            }
        }
    }

    // SRD-80 PR B.10: detect tuple-typed return for multi-output.
    let tuple_ret_elems: Option<Vec<Type>> = match &ret_ty {
        syn::Type::Tuple(t) => Some(t.elems.iter().cloned().collect()),
        _ => None,
    };

    // SRD-80b dynamic-output shape — detect `DynamicOutputs<T>`
    // return and locate the `Const<Vec<C>>` arg whose length
    // drives the output port count at construction.
    let dynamic_outputs_inner: Option<Type> = classify_dynamic_outputs(&ret_ty);
    let dynamic_outputs_count_arg: Option<syn::Ident> = if dynamic_outputs_inner.is_some() {
        let const_vec_args: Vec<&syn::Ident> = args.iter()
            .filter_map(|a| match &a.kind {
                ArgKind::ConstVec(_) => Some(&a.name),
                _ => None,
            })
            .collect();
        if const_vec_args.len() != 1 {
            return Err(syn::Error::new_spanned(
                &ret_ty,
                format!(
                    "`DynamicOutputs<T>` return requires exactly one \
                     `Const<Vec<C>>` arg to drive the output port count \
                     (got {}). Declare one `Const<Vec<C>>` arg whose length \
                     determines the number of output ports.",
                    const_vec_args.len(),
                ),
            ));
        }
        Some(const_vec_args[0].clone())
    } else {
        None
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
        // Prefer a singleton PolyWire arg for SameAsInput
        // dispatch; fall back to a variadic `&[Value]` arg
        // (split-halves shape) whose runtime element types
        // drive the output polymorphism. The static slot
        // gets a `PortType::U64` placeholder (assembler skips
        // type-check for these); eval enforces uniformity.
        if let Some(polywire_arg) = args.iter().find(|a| matches!(a.kind, ArgKind::PolyWire)) {
            let pt_ident = format_ident!("{}_type", polywire_arg.name);
            vec![quote!(#pt_ident)]
        } else if args.iter().any(|a| matches!(&a.kind, ArgKind::Variadic(VariadicElement::Value))) {
            vec![quote!(polydat::ast::PortType::U64)]
        } else {
            return Err(syn::Error::new_spanned(
                &ret_ty,
                "function returns `Value` but has no `Value` arg — the macro \
                 needs at least one PolyWire (`Value`) arg or a `&[Value]` \
                 variadic to source the runtime port type for the output.",
            ));
        }
    } else if let Some(inner) = &dynamic_outputs_inner {
        // Single per-element port type for the dynamic case.
        // The count is determined at construction time; this
        // entry is used by the codegen as the port type each
        // output port carries.
        vec![wire_port_type_for(inner)?]
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

    let output_count = if dynamic_outputs_inner.is_some() { 0 } else { output_port_types.len() };
    // SRD-80b: `0` in the FuncSig signals "dynamic, determined at
    // compile time" (existing FuncSig convention from the doc).
    let output_count_lit = syn::LitInt::new(&output_count.to_string(), proc_macro2::Span::call_site());

    // When return is `Value`, prefer SameAsInput dispatch
    // against a singleton PolyWire arg; for the split-halves
    // `&[Value]` case there's no singleton to point at, so
    // fall back to OutputType::Fixed (the static slot's
    // placeholder PortType is used and eval enforces type
    // uniformity).
    let output_type_tokens: TokenStream2 = match (ret_is_polywire, first_polywire_idx) {
        (true, Some(idx)) => {
            let i = syn::Index::from(idx);
            quote!(polydat::dsl::registry::OutputType::SameAsInput(#i))
        }
        _ => quote!(polydat::dsl::registry::OutputType::Fixed),
    };

    // Struct fields. Wire/PolyWire/Variadic → no field (arity
    // reflected in `meta.ins.len()`); Const → owned-typed field;
    // ConstVec → Vec<inner>; Setup → field of the borrowed
    // inner type.
    let struct_fields: Vec<TokenStream2> = args.iter()
        .filter_map(|a| match &a.kind {
            ArgKind::Wire | ArgKind::PolyWire | ArgKind::Variadic(_) => None,
            ArgKind::Const(shape) => {
                let n = &a.name;
                let ft = shape.field_type_tokens();
                Some(quote!(pub #n: #ft))
            }
            ArgKind::ConstVec(inner) => {
                let n = &a.name;
                let ft = inner.field_type_tokens();
                Some(quote!(pub #n: Vec<#ft>))
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
            ArgKind::ConstVec(inner) => {
                let n = &a.name;
                let ft = inner.field_type_tokens();
                Some(quote!(#n: Vec<#ft>))
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
    // SRD-80b split-halves variadic: TWO variadics in succession
    // share a single `n_wires` param (interpreted as "count per
    // half"). The macro emits 2*n_wires wire slots and slices
    // the inputs at the midpoint at eval time. Used by `pick`'s
    // `(b0,...,bN,v0,...,vN)` workload syntax per SRD-66.
    let has_variadic = args.iter().any(|a| matches!(a.kind, ArgKind::Variadic(_)));
    let variadic_count = args.iter().filter(|a| matches!(a.kind, ArgKind::Variadic(_))).count();
    if variadic_count > 2 {
        return Err(syn::Error::new_spanned(
            &func.sig,
            "`#[polydat_node]` supports at most two variadic `&[T]` args (split-halves shape). \
             Functions declaring more than two are not expressible in any SRD-80b shape.",
        ));
    }
    let is_split_halves = variadic_count == 2;
    // Positional index of each Variadic arg in declaration
    // order, used by `arg_bindings` to slice `inputs` at the
    // midpoint in split-halves mode.
    let variadic_positions: std::collections::HashMap<String, usize> = args.iter()
        .filter(|a| matches!(a.kind, ArgKind::Variadic(_)))
        .enumerate()
        .map(|(i, a)| (a.name.to_string(), i))
        .collect();
    let new_params: Vec<TokenStream2> = if has_variadic {
        let mut v = new_params;
        v.push(quote!(n_wires: usize));
        v
    } else {
        new_params
    };

    // Build a lookup from arg name → const-shape category so the
    // Setup pre-compute step can dispatch on the source's shape
    // to produce the right access expression.
    #[derive(Clone, Copy)]
    enum ConstSourceShape {
        /// Scalar `Const<u64>` / `Const<f64>` / `Const<bool>`.
        ScalarValue,
        /// `Const<&str>` / `Const<String>` — backing field is
        /// `String`; setup fn typically wants `&str`.
        ScalarStr,
        /// `Const<Vec<C>>` — backing field is `Vec<C>`; setup fn
        /// typically wants `&Vec<C>` or `&[C]`.
        VecValues,
    }
    let const_shape_by_name: std::collections::HashMap<String, ConstSourceShape> = args.iter()
        .filter_map(|a| match &a.kind {
            ArgKind::Const(ConstShape::Str) => Some((a.name.to_string(), ConstSourceShape::ScalarStr)),
            ArgKind::Const(_)               => Some((a.name.to_string(), ConstSourceShape::ScalarValue)),
            ArgKind::ConstVec(_)            => Some((a.name.to_string(), ConstSourceShape::VecValues)),
            _ => None,
        })
        .collect();

    // Setup pre-compute lines, emitted at the top of `new()`
    // BEFORE `Self { ... }` so they can borrow the const
    // locals before those values are moved into self.
    let setup_precomputes: Vec<TokenStream2> = args.iter()
        .filter_map(|a| match &a.kind {
            ArgKind::Wire | ArgKind::Const(_) | ArgKind::ConstVec(_) | ArgKind::PolyWire | ArgKind::Variadic(_) => None,
            ArgKind::Setup(spec) => {
                let n = &a.name;
                let setup_fn = &spec.setup_fn;
                // SRD-80b amendment — `source_args` may be empty
                // (session-static setup), single (the common
                // case), or multi (joint derivation). Per-source
                // access dispatch reads each named const's
                // shape and emits the right body-side expression.
                let mut src_exprs: Vec<TokenStream2> = Vec::new();
                let mut err: Option<TokenStream2> = None;
                for src in &spec.source_args {
                    let shape = const_shape_by_name.get(&src.to_string());
                    let expr = match shape {
                        Some(ConstSourceShape::ScalarStr)   => quote!(#src.as_str()),
                        Some(ConstSourceShape::ScalarValue) => quote!(#src),
                        // ConstVec source: pass a borrow of the
                        // Vec. Setup fn signatures like
                        // `fn build(w: &Vec<f64>)` or
                        // `fn build(w: &[f64])` both work via
                        // Deref / unsized coercion.
                        Some(ConstSourceShape::VecValues)   => quote!(&#src),
                        None => {
                            err = Some(syn::Error::new(
                                src.span(),
                                format!(
                                    "#[poly_const(... from = ... {src} ...)] — \
                                     `{src}` is not declared as a `Const<T>` \
                                     arg in the same function signature."),
                            ).to_compile_error());
                            break;
                        }
                    };
                    src_exprs.push(expr);
                }
                if let Some(e) = err { return Some(e); }
                let call = quote!(#setup_fn( #( #src_exprs ),* ));
                Some(quote! {
                    let #n = #call;
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
            ArgKind::Const(_) | ArgKind::ConstVec(_) | ArgKind::Setup(_) => {
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
                    // SRD-80b Phase B — dispatch:
                    //   1. `Arc<T>` Handle (non-special T) → inline
                    //      downcast (no blanket impl works).
                    //   2. Borrow shape (`&str`, `&[u8]`, `&[T]`,
                    //      `&serde_json::Value`) → direct
                    //      `match`-on-`Value`. Lifetime is naturally
                    //      `&inputs[i]`'s; no `unsafe` transmute.
                    //   3. Otherwise → `<#ty as Wire>::extract`.
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
                    } else if let Some(borrow) = is_borrow_wire_shape(ty) {
                        let extract = borrow_extract_tokens(borrow, quote!(&inputs[#idx]));
                        quote! {
                            let #n = #extract;
                        }
                    } else {
                        quote! {
                            let #n = <#ty as polydat::derive_support::Wire>::extract(&inputs[#idx]);
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
                    // SRD-80 PR B.9 + SRD-80b split-halves —
                    // materialise a Vec<T> from the inputs
                    // slice (per-element extraction), then bind
                    // the body local as `&[T]`. In single-
                    // variadic mode, the slice is `inputs` (all
                    // of them after the leading wires consumed
                    // their indices). In split-halves mode, the
                    // first variadic gets `inputs[0..n_wires]`
                    // and the second gets `inputs[n_wires..]`.
                    let extractor = elem.extract_from_value();
                    let owned = format_ident!("__{}_owned", a.name);
                    // Split-halves divides `inputs` at the
                    // midpoint at eval time. `inputs.len() / 2`
                    // is the per-half count; first variadic
                    // gets the low half, second gets the high.
                    let slice_expr = if is_split_halves {
                        let pos = variadic_positions[&a.name.to_string()];
                        if pos == 0 {
                            quote!({ let __half = inputs.len() / 2; &inputs[..__half] })
                        } else {
                            quote!({ let __half = inputs.len() / 2; &inputs[__half..] })
                        }
                    } else {
                        quote!(inputs)
                    };
                    quote! {
                        let #owned: Vec<_> = #slice_expr.iter().map(#extractor).collect();
                        let #n: &[_] = #owned.as_slice();
                    }
                }
                ArgKind::ConstVec(_) => {
                    // SRD-80b Phase C — `Const<Vec<C>>` body view:
                    // clone the cached Vec and wrap in `Const`.
                    // (Per-cycle clone matches the Wire-trait
                    // convention; JIT-ineligible by design.)
                    quote! {
                        let #n = polydat::derive_support::Const(self.#n.clone());
                    }
                }
            }
        })
        .collect();

    // Build closure const-extraction logic. For each const arg
    // (in declaration order), pull from `consts: &[ConstArg]`
    // by index; fall back to the `poly_default` value if the
    // slice is shorter than the const arg list.
    //
    // For `ConstVec` args, collect every remaining entry from
    // `consts[i..]` into a `Vec<inner>` via the inner shape's
    // extractor — this consumes the tail of the consts slice
    // (only one ConstVec arg per function, enforced earlier).
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
            ArgKind::ConstVec(inner) => {
                let n = &a.name;
                let i = const_idx_for_extract;
                // ConstVec consumes everything from index `i`
                // onward. const_idx_for_extract is intentionally
                // NOT bumped — by construction (validated below)
                // there's at most one ConstVec arg and it must be
                // the last arg, so no subsequent Const reads need
                // a higher base index.
                let i_lit = syn::LitInt::new(&i.to_string(), proc_macro2::Span::call_site());
                let extract_one = inner.extract_from_const_arg(quote!(c));
                Some(quote! {
                    let #n: Vec<_> = consts[#i_lit..].iter()
                        .map(|c| #extract_one)
                        .collect();
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
            ArgKind::Const(_) | ArgKind::ConstVec(_) => {
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
        // Split-halves: assembler hands TOTAL wires; new() takes
        // the per-half count, so divide by 2 here too (matches
        // the variadic_ctor field's `n / 2`).
        if is_split_halves {
            quote! { let n_wires: usize = _wires.len() / 2; }
        } else {
            quote! { let n_wires: usize = _wires.len(); }
        }
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
                ArgKind::Const(_) | ArgKind::ConstVec(_) | ArgKind::Setup(_) => {}
            }
        }
        out
    };

    let block = &func.block;

    // SRD-80b in-spirit `default_resolver` emission. Each wire
    // arg's `Wire::RESOLVER` const exposes the auto-resolver
    // intent at codegen time; the cascade picks the first
    // non-None among the wire-typed args. Non-Resolved wire
    // types contribute `None` (the trait default), so this
    // collapses cleanly to a no-resolver FuncSig for the
    // overwhelming majority of nodes.
    let default_resolver_field: TokenStream2 = {
        // Borrow shapes (`&str`, `&[u8]`, ...) don't impl `Wire`,
        // and `PolyWire` is excluded by ArgKind; only the
        // owned-type wire args contribute resolver intent.
        let wire_tys: Vec<&Type> = args.iter()
            .filter_map(|a| match &a.kind {
                ArgKind::Wire if is_borrow_wire_shape(&a.declared_ty).is_none()
                    && classify_wrapper_wire(&a.declared_ty) != Some(WrapperWire::Handle)
                    => Some(&a.declared_ty),
                _ => None,
            })
            .collect();
        if wire_tys.is_empty() {
            quote!(None)
        } else {
            // Build a right-to-left match cascade so the first
            // wire arg with a Some(_) resolver wins. Each step:
            //   match <ty as Wire>::RESOLVER { Some(r) => Some(r), None => <rest> }
            let mut acc = quote!(None);
            for ty in wire_tys.iter().rev() {
                acc = quote! {
                    match <#ty as polydat::derive_support::Wire>::RESOLVER {
                        Some(__r) => Some(__r),
                        None => #acc,
                    }
                };
            }
            acc
        }
    };

    // SRD-80b Phase D1 — when multiple instantiations share a
    // DSL function name, each per-instantiation build closure
    // must claim only the call that matches its own concrete
    // wire types. The guard checks `wire_types[i]` against
    // `<#ty as Wire>::PORT` for every wire-position arg. The
    // factory walks all matching registrations and the first to
    // accept (return `Some(Ok(...))`) wins; mismatches fall
    // through to the next instantiation.
    let port_guard: TokenStream2 = if is_instantiation {
        let mut wi: usize = 0;
        let mut checks: Vec<TokenStream2> = Vec::new();
        for a in &args {
            match &a.kind {
                ArgKind::Wire | ArgKind::PolyWire => {
                    let i = syn::Index::from(wi);
                    let ty = &a.declared_ty;
                    // PolyWire stays opaque (Value isn't a Wire impl);
                    // skip it from the guard.
                    if !classify_polywire(ty) {
                        checks.push(quote! {
                            if _wire_types.get(#i) != Some(&<#ty as polydat::derive_support::Wire>::PORT) {
                                return None;
                            }
                        });
                    }
                    wi += 1;
                }
                ArgKind::Variadic(_) => {
                    // Variadic — claims any tail; instantiation
                    // selection on variadic generic-over-Wire
                    // isn't supported in this pass.
                }
                _ => {}
            }
        }
        quote! { #( #checks )* }
    } else {
        quote!()
    };

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

    // SRD-80b Phase F (S18) — `#[polydat_node(decompose =
    // path)]` emits the FusedNode impl by delegating to the
    // named free function. Operators with bespoke fusion
    // logic (e.g. WeightedPick whose `decomposed()` body
    // builds a spec string) can still write their own
    // `impl FusedNode` block alongside the macro emission;
    // both compose because `decompose` is opt-in.
    let fused_node_impl: TokenStream2 = if let Some(path) = &attrs.decompose {
        quote! {
            impl polydat::compile::fusion::FusedNode for #struct_name {
                fn decomposed(&self) -> polydat::compile::fusion::DecomposedGraph {
                    #path(self)
                }
            }
        }
    } else {
        quote!()
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
                // ConstVec is JIT-ineligible (the JIT u64 buffer
                // has no slot shape for a variable-length list).
                ArgKind::Setup(_) | ArgKind::PolyWire | ArgKind::ConstVec(_) => None,
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

    let body_fn_def: TokenStream2 = if is_fallible {
        // SRD-80b Phase 5 S16 — fallible body. Body returns the
        // declared Result<T, E>; try_new runs it once at
        // construction and propagates Err as String via Into.
        quote! {
            #[inline(always)]
            #[allow(unused_variables)]
            fn __polydat_body( #( #body_params ),* ) -> #declared_ret_ty #block
        }
    } else if use_shared_body {
        quote! {
            #[inline(always)]
            #[allow(unused_variables)]
            fn __polydat_body( #( #body_params ),* ) -> #ret_ty #block
        }
    } else {
        quote!()
    };

    // Helper: emit `outputs[idx] = <conversion>(value)` for a
    // given element type. SRD-80b Phase B — owned types route
    // through `<T as Wire>::inject`; Handle keeps its inline
    // upcast (no blanket impl works). Returning a borrow shape
    // (`&str`, `&[u8]`, etc.) from a node body is unusual but
    // supported: the borrow's `into()` already exists for the
    // canonical `Value` constructor; we emit that directly.
    let output_assign = |idx_lit: TokenStream2, elem_ty: &Type, local: TokenStream2| -> TokenStream2 {
        if classify_wrapper_wire(elem_ty) == Some(WrapperWire::Handle) {
            quote! {
                outputs[#idx_lit] = polydat::ast::Value::handle(#local);
            }
        } else if classify_polywire(elem_ty) {
            // PolyWire return: body returns `Value` directly, move
            // it into the outputs slot. No trait dispatch — Value
            // has no static port type (it's polymorphic at runtime).
            quote! {
                outputs[#idx_lit] = #local;
            }
        } else if let Some(borrow) = is_borrow_wire_shape(elem_ty) {
            // Borrow-typed returns: construct the matching Value
            // variant from the borrow via the existing
            // `Into<Value>` / Arc::from path. `&str` →
            // `Value::Str(arc)`; `&[u8]` → `Value::Bytes(arc)`;
            // typed-vec borrows → `Value::Vec*(SliceArc::from(slice))`.
            match borrow {
                BorrowWire::Str => quote! {
                    outputs[#idx_lit] = polydat::ast::Value::Str((#local).into());
                },
                BorrowWire::Bytes => quote! {
                    outputs[#idx_lit] = polydat::ast::Value::Bytes((#local).into());
                },
                BorrowWire::Json => quote! {
                    outputs[#idx_lit] = polydat::ast::Value::Json(::std::sync::Arc::new((#local).clone()));
                },
                BorrowWire::Vec(variant, _) => {
                    let v = syn::Ident::new(variant, proc_macro2::Span::call_site());
                    quote! {
                        outputs[#idx_lit] = polydat::ast::Value::#v(polydat::ast::SliceArc::from_vec((#local).to_vec()));
                    }
                }
            }
        } else {
            quote! {
                outputs[#idx_lit] = <#elem_ty as polydat::derive_support::Wire>::inject(#local);
            }
        }
    };

    // SRD-80b `DynamicOutputs<T>` — build the `outs:` vec at
    // construction from the driving `Const<Vec<C>>` arg's
    // length. Used by both the infallible `new()` and the
    // fallible `try_new()` paths below.
    let outs_build: TokenStream2 = if let (Some(inner), Some(count_arg)) =
        (&dynamic_outputs_inner, &dynamic_outputs_count_arg)
    {
        quote! {
            let outs: Vec<polydat::ast::Port> = (0..#count_arg.len())
                .map(|__i| polydat::ast::Port::new(
                    format!("d{}", __i),
                    <#inner as polydat::derive_support::Wire>::PORT,
                ))
                .collect();
        }
    } else {
        quote! {
            let outs = vec![ #(
                polydat::ast::Port::new(#output_names_strs, #output_port_types)
            ),* ];
        }
    };

    // SRD-80 PR B.10/B.11: result → outputs translation. For
    // single-output, write `outputs[0] = ...(result)`. For
    // tuple-output, destructure and per-element write. For
    // SRD-80b `DynamicOutputs<T>`, iterate the returned Vec
    // and inject each element via the inner type's Wire impl.
    let result_to_outputs: TokenStream2 = if let Some(inner) = &dynamic_outputs_inner {
        let inject_one = if classify_polywire(inner) {
            quote!(__elem)
        } else if let Some(borrow) = is_borrow_wire_shape(inner) {
            match borrow {
                BorrowWire::Str => quote!(polydat::ast::Value::Str((__elem).into())),
                BorrowWire::Bytes => quote!(polydat::ast::Value::Bytes((__elem).into())),
                BorrowWire::Json => quote!(polydat::ast::Value::Json(::std::sync::Arc::new((__elem).clone()))),
                BorrowWire::Vec(variant, _) => {
                    let v = syn::Ident::new(variant, proc_macro2::Span::call_site());
                    quote!(polydat::ast::Value::#v(polydat::ast::SliceArc::from_vec((__elem).to_vec())))
                }
            }
        } else {
            quote!(<#inner as polydat::derive_support::Wire>::inject(__elem))
        };
        quote! {
            for (__i, __elem) in result.0.into_iter().enumerate() {
                outputs[__i] = #inject_one;
            }
        }
    } else if let Some(elems) = &tuple_ret_elems {
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
        // SRD-80b in-spirit refinement — pass `&self` to the
        // override fn so setup-derived state (round_keys,
        // half_bits, etc.) is reachable. The override fn
        // signature is now `fn(&Self) -> CompiledU64Op`.
        quote! {
            fn compiled_u64(&self) -> Option<polydat::ast::CompiledU64Op> {
                Some(#path(self))
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
                ArgKind::Setup(_) | ArgKind::PolyWire | ArgKind::ConstVec(_) => {
                    unreachable!("setup/polywire/constvec excludes JIT eligibility")
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
                        wire_buf_idx += jt.width();
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
                    ArgKind::Setup(_) | ArgKind::PolyWire | ArgKind::ConstVec(_) => unreachable!(),
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
            // Per-element write at the element's slot OFFSET (the
            // prefix sum of preceding element widths — §8.4 L1).
            let mut out_off = 0usize;
            let writes: Vec<TokenStream2> = tuple_jits.iter().enumerate()
                .map(|(i, jt)| {
                    let local = &locals[i];
                    let w = jt.write_to_u64_buffer_at(out_off, quote!(#local));
                    out_off += jt.width();
                    w
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
        // Split-halves: assembler passes TOTAL wire count; the
        // struct's `new()` takes per-half count, so divide by 2.
        if is_split_halves {
            quote!(Some(|n| Box::new(#struct_name::new(n / 2))))
        } else {
            quote!(Some(|n| Box::new(#struct_name::new(n))))
        }
    } else {
        quote!(None)
    };

    // SRD-80b Phase C — `Option<T>` arg auto-emits
    // `accepts_none_inputs() -> true`. The runtime kernel's
    // SRD-74 Rule 1 propagation short-circuits `Value::None`
    // inputs by default; `Option<T>` is the canonical opt-in
    // shape that wants None routed to the body instead.
    // SRD-80b in-spirit rule — `Option<T>` wire args declare
    // None-tolerance via the type system; PolyWire (`Value`) args
    // ARE inherently None-tolerant (`Value::None` is just one of
    // the polymorphic variants). Both opt the node out of the
    // kernel-Rule-1 short-circuit.
    let has_none_aware_arg = args.iter().any(|a| match &a.kind {
        ArgKind::Wire => is_option_arg(&a.declared_ty),
        ArgKind::PolyWire => true,
        _ => false,
    });
    let accepts_none_impl: TokenStream2 = if has_none_aware_arg {
        quote! {
            fn accepts_none_inputs(&self) -> bool { true }
        }
    } else {
        quote!()
    };

    // SRD-80b Phase C — `Const<Vec<C>>` implies
    // `Arity::VariadicConsts`. Mutually exclusive with the
    // wire-variadic case (the macro rejects mixing them earlier).
    let has_const_vec = args.iter().any(|a| matches!(a.kind, ArgKind::ConstVec(_)));
    let arity_field: TokenStream2 = if has_variadic {
        // SRD-80b split-halves: `variadic_min` is interpreted
        // as PAIRS count; the FuncSig advertises 2× as total
        // wires so the assembler enforces the right floor.
        let min_wires = match (&attrs.variadic_min, is_split_halves) {
            (Some(v), true)  => quote!(2 * (#v)),
            (Some(v), false) => quote!(#v),
            (None, _)        => quote!(0),
        };
        quote!(polydat::dsl::registry::Arity::VariadicWires { min_wires: #min_wires })
    } else if has_const_vec {
        // min_consts = 0 by default; the workload-list shape
        // permits empty lists. Authors who want a minimum
        // declare it via `#[poly_default]` on the inner type or
        // by validating in the body.
        quote!(polydat::dsl::registry::Arity::VariadicConsts { min_consts: 0 })
    } else {
        quote!(polydat::dsl::registry::Arity::Fixed)
    };

    let commutativity_field: TokenStream2 = if let Some(c) = &attrs.commutativity {
        quote!(polydat::ast::Commutativity::#c)
    } else {
        quote!(polydat::ast::Commutativity::Positional)
    };

    // SRD-80b Phase 5 S16 — fallible-mode emission. When the body
    // returns Result<T, E>, the macro:
    //   * adds a cached `__polydat_cached: T` struct field,
    //   * replaces `new(...)` with `try_new(...) -> Result<Self, String>`,
    //   * runs the body once inside try_new, captures Ok into the
    //     cache, propagates Err via Into<String>,
    //   * makes eval read the cached value (no per-eval body call).
    let (ctor_emission, eval_emission, build_call_emission): (TokenStream2, TokenStream2, TokenStream2) = if is_fallible {
        // body-arg pass list. In try_new() Const args arrive as
        // their `field_type_tokens()` form (String for Str, raw
        // primitive otherwise) and need wrapping as `Const<T>` for
        // the body's declared signature. Setup args are locals
        // produced by `setup_precomputes` — body takes `&local`.
        let body_arg_passes: Vec<TokenStream2> = args.iter()
            .map(|a| {
                let n = &a.name;
                match &a.kind {
                    ArgKind::Const(shape) => shape.wrap_as_const(quote!(#n)),
                    ArgKind::Setup(_) => quote!(&#n),
                    // Wire / PolyWire / Variadic are rejected
                    // earlier for fallible nodes — unreachable.
                    _ => quote!(#n),
                }
            })
            .collect();
        // Local wrapping: each Const arg comes in as the wrapper
        // (matching new_params), so we forward it directly. The
        // body receives `Const<T>` and unwraps via .0 or .as_str()
        // in its own code.
        let try_new = quote! {
            pub fn try_new( #( #new_params ),* ) -> ::std::result::Result<Self, String> {
                #( #setup_precomputes )*
                let mut ins: Vec<polydat::ast::Slot> = vec![ #( #slot_exprs ),* ];
                #( #variadic_slot_extends )*
                #outs_build
                // Invoke the body once; propagate Err as String.
                let __polydat_cached = match Self::__polydat_body( #( #body_arg_passes ),* ) {
                    Ok(v) => v,
                    Err(e) => return Err(Into::<String>::into(e)),
                };
                Ok(Self {
                    meta: polydat::ast::NodeMeta {
                        name: #func_name_str.into(),
                        ins,
                        outs,
                    },
                    #( #new_field_inits, )*
                    __polydat_cached,
                })
            }
        };
        // eval reads the cached value; no body call.
        let out_assign = output_assign(quote!(0), &ret_ty, quote!(self.__polydat_cached.clone()));
        let ev = quote! {
            #[allow(unused_variables)]
            { #out_assign }
        };
        // build closure: call try_new and propagate Err.
        let bc = quote! {
            Some(match #struct_name::try_new( #( #new_call_args ),* ) {
                Ok(n) => Ok(Box::new(n) as Box<dyn polydat::ast::PolydatNode>),
                Err(e) => Err(e),
            })
        };
        (try_new, ev, bc)
    } else {
        let ctor = quote! {
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
                #outs_build
                Self {
                    meta: polydat::ast::NodeMeta {
                        name: #func_name_str.into(),
                        ins,
                        outs,
                    },
                    #( #new_field_inits, )*
                }
            }
        };
        let ev = quote!(#eval_body);
        // Wrap `new()` in `catch_unwind` so that panics from
        // `#[poly_const]` setup functions (Regex parse failures,
        // file-not-found from filename consts, "value:weight"
        // parse failures, etc.) surface as build-closure `Err`
        // values rather than unwinding through the compile path.
        // The runtime sees `name` here as the DSL-registered
        // function name; the message is prefixed for traceability.
        let bc = quote! {
            Some(match ::std::panic::catch_unwind(
                ::std::panic::AssertUnwindSafe(|| #struct_name::new( #( #new_call_args ),* ))
            ) {
                Ok(node) => Ok(Box::new(node) as Box<dyn polydat::ast::PolydatNode>),
                Err(panic) => {
                    let msg = panic.downcast_ref::<&str>().copied()
                        .or_else(|| panic.downcast_ref::<String>().map(|s| s.as_str()))
                        .unwrap_or("<non-string panic>");
                    Err(format!("{}: construction failed: {}", #func_name_str, msg))
                }
            })
        };
        (ctor, ev, bc)
    };

    // Cached field for fallible mode. T = `ret_ty` (the Ok inner).
    let cached_field: TokenStream2 = if is_fallible {
        quote!(__polydat_cached: #ret_ty,)
    } else {
        quote!()
    };

    let result = quote! {
        pub struct #struct_name {
            meta: polydat::ast::NodeMeta,
            #( #struct_fields, )*
            #cached_field
        }

        #default_impl

        #fused_node_impl

        impl #struct_name {
            #ctor_emission

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
                #eval_emission
            }

            #compiled_u64_impl
            #jit_constants_impl
            #purity_impl
            #accepts_none_impl
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
                    default_resolver: #default_resolver_field,
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
                #port_guard
                #( #const_extracts )*
                #( #polywire_extracts )*
                #variadic_n_wires_extract
                #build_call_emission
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
