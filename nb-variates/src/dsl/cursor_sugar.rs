// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Cursor-constructor sugar registry.
//!
//! A *cursor sugar* lets a node module (vectordata, future
//! tabular sources, etc.) recognize a non-standard `cursor x =
//! foo(...)` form and rewrite it into:
//!
//! 1. A synthetic constructor expression (typically `range(...)`)
//!    that the standard extent-resolution path can drive, and
//! 2. Zero or more auxiliary bindings to emit after the cursor's
//!    input ports are wired — typical examples are an
//!    init-time prebuffer call and per-field projection bindings
//!    (`<cursor>__vector := vector_at_bytes(<cursor>__ordinal,
//!    "ds:profile")`).
//!
//! The core compiler stays agnostic: it walks the inventory of
//! registered handlers, picks the first match, and applies the
//! rewrite. Adding a new sugar form is a single
//! `inventory::submit!` plus a handler function — no compile.rs
//! change required.

use crate::dsl::ast::Expr;
use crate::node::PortType;

/// A handler that recognizes one or more cursor-constructor
/// shapes and produces a [`CursorSugar`] rewrite. Return:
/// - `Ok(Some(s))` when the handler matched and the rewrite
///   should be applied.
/// - `Ok(None)` when this handler isn't responsible for the
///   given constructor — the dispatch loop continues on to the
///   next handler.
/// - `Err(msg)` when the handler matched the *name* but the
///   arguments don't validate. The caller surfaces the error
///   directly (with the cursor name prepended).
pub type CursorSugarFn = fn(source_name: &str, constructor: &Expr)
    -> Result<Option<CursorSugar>, String>;

/// One inventory entry. Handlers self-name for diagnostic
/// listings (`describe gk cursor-sugar`, future) and so the
/// dispatcher can attribute errors to the right module.
pub struct CursorSugarRegistration {
    pub handler: CursorSugarFn,
    /// Short identifier of the sugar family for diagnostics
    /// (e.g. `"vectordata"`).
    pub name: &'static str,
}

inventory::collect!(CursorSugarRegistration);

/// The rewrite returned by a sugar handler.
pub struct CursorSugar {
    /// Replacement for the user's constructor. The standard
    /// extent-resolution path runs against this — typically a
    /// `range(0, <count_function>(...))` call.
    pub effective_constructor: Expr,
    /// Auxiliary bindings the compiler should emit after the
    /// cursor's input ports are wired. Order is preserved.
    pub aux_bindings: Vec<AuxBinding>,
}

/// One binding emitted by sugar after cursor input wiring.
///
/// If `projection` is `Some`, the binding's output wire is
/// promoted to a cursor projection — both registered on the
/// `SourceSchema.projections` list and added as a kernel output
/// the runtime can read.
pub struct AuxBinding {
    pub name: String,
    pub value: Expr,
    pub projection: Option<(String, PortType)>,
}

/// Walk the inventory and dispatch to the first handler that
/// matches `constructor`. Returns the rewrite to apply, `None`
/// if no handler matches, or the handler's error.
pub fn dispatch(
    source_name: &str,
    constructor: &Expr,
) -> Result<Option<CursorSugar>, String> {
    for reg in inventory::iter::<CursorSugarRegistration> {
        match (reg.handler)(source_name, constructor) {
            Ok(Some(s)) => return Ok(Some(s)),
            Ok(None) => continue,
            Err(e) => return Err(e),
        }
    }
    Ok(None)
}
