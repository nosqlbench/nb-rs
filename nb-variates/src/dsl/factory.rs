// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Node factory: maps GK function names to runtime node instances.
//!
//! `build_node` is the single dispatch point used by the compiler's
//! `compile_binding` to turn a parsed call expression into a `Box<dyn GkNode>`.
//! `ConstArg` captures assembly-time constant arguments extracted from the AST.
//!
//! Dispatch is decentralized: each node module exposes its own `build_node`
//! function returning `Option<Result<...>>`.  The top-level `build_node` here
//! tries each module in turn and falls back to the registry for variadic nodes.

use crate::assembly::WireRef;
use crate::node::GkNode;
use crate::nodes::identity::ConstU64;

use crate::dsl::registry;

/// Constant arguments extracted from the AST.
///
/// Holds assembly-time values (integers, floats, strings, float arrays)
/// that are baked into node constructors rather than passed as wire inputs.
///
/// `pub` visibility is required so that `NodeRegistration::build` function
/// pointers (which are `pub` fields) can name this type.
pub enum ConstArg {
    Int(u64),
    Float(f64),
    Str(String),
    FloatArray(#[allow(dead_code)] Vec<f64>),
}

impl ConstArg {
    /// Return the value as a `u64`, or 0 if incompatible.
    pub fn as_u64(&self) -> u64 {
        match self { ConstArg::Int(v) => *v, _ => 0 }
    }

    /// Return the value as an `f64`, or 0.0 if incompatible.
    ///
    /// Integer literals are widened to f64.
    pub fn as_f64(&self) -> f64 {
        match self { ConstArg::Float(v) => *v, ConstArg::Int(v) => *v as f64, _ => 0.0 }
    }

    /// Return the value as a `&str`, or `""` if incompatible.
    pub fn as_str(&self) -> &str {
        match self { ConstArg::Str(s) => s, _ => "" }
    }

    /// Return the value as a float slice, or `&[]` if incompatible.
    #[allow(dead_code)]
    pub fn as_float_array(&self) -> &[f64] {
        match self { ConstArg::FloatArray(v) => v, _ => &[] }
    }
}

/// Build a node from a function name and its arguments.
///
/// `wires` are the cycle-time wire inputs.
/// `consts` are the assembly-time constant arguments.
///
/// Dispatch order:
/// 1. Per-module `build_node` functions (one per node module)
/// 2. Sampling functions not covered by a node module
/// 3. Registry variadic fallback
/// Source-binding attribution, set by the compiler before each
/// `build_node` call and read by factories that want to record
/// which DSL binding caused the node to exist. The
/// `control_set` factory is the canonical consumer:
/// `rate_adj := control_set("rate", target)` calls the factory
/// with `current_binding()` returning `"rate_adj"`, which the
/// node stores for runtime attribution in
/// `ControlOrigin::Gk { binding }`.
pub mod compile_ctx {
    use std::cell::RefCell;
    thread_local! {
        static BINDING: RefCell<Option<String>> = const { RefCell::new(None) };
    }

    /// Install the current binding name for the duration of a
    /// single `build_node` call. Returns a guard that clears
    /// the thread-local on drop so nested compilation can't
    /// leak attribution across callers.
    pub fn scoped_binding(name: &str) -> BindingScope {
        BINDING.with(|b| *b.borrow_mut() = Some(name.to_string()));
        BindingScope(())
    }

    /// Read the current binding attribution. Returns `None`
    /// when called outside a [`scoped_binding`] scope (e.g.
    /// ad-hoc tests that call [`super::build_node`] directly).
    pub fn current_binding() -> Option<String> {
        BINDING.with(|b| b.borrow().clone())
    }

    /// RAII guard that clears the binding slot on drop.
    pub struct BindingScope(());
    impl Drop for BindingScope {
        fn drop(&mut self) {
            BINDING.with(|b| *b.borrow_mut() = None);
        }
    }
}

pub(crate) fn build_node(
    func: &str,
    wires: &[WireRef],
    consts: &[ConstArg],
) -> Result<Box<dyn GkNode>, String> {
    // --- Per-module dispatch via inventory ---

    use crate::dsl::registry::NodeRegistration;
    for reg in inventory::iter::<NodeRegistration> {
        // Only run this module's validator if it owns `func`.
        // Signatures are the authoritative "does this module
        // handle this name" list — probing `build` first would
        // invert the ordering (construction before validation)
        // and give an opt-in validator no chance to reject bad
        // constants before the constructor panics.
        let sigs = (reg.signatures)();
        let owning_sig = sigs.iter().find(|s| s.name == func);
        if owning_sig.is_none() { continue; }
        let sig = owning_sig.unwrap();

        // Pass 1: walk declared `ParamSpec.constraint`s and run
        // each per-param check. Constraints declared on individual
        // params cover the bulk of "must be in [0,1]" /
        // "must be one of {2,8,10,16}" / "spec must parse" cases.
        // SRD 15 §"Const Constraint Metadata".
        if let Err(msg) = check_param_constraints(sig, consts) {
            return Err(format!("bad constant {func}: {msg}"));
        }

        // Pass 2: per-module imperative validator for relational
        // and cross-param rules (e.g. `n_of`'s n ≤ m). Eventually
        // migrates onto a `FuncSig.validator` field; until then,
        // each module declares its relational constraint here.
        if let Some(validator) = reg.validate
            && let Err(reason) = validator(func, consts)
        {
            return Err(format!("bad constant {func}: {reason}"));
        }

        if let Some(result) = (reg.build)(func, wires, consts) {
            return result;
        }
    }

    // --- Sampling functions without a dedicated node module ---
    match func {
        "identity" => return Ok(Box::new(crate::nodes::identity::Identity::new())),

        "lut_sample" | "icd_normal" => {
            use crate::sampling::icd::IcdSample;
            return Ok(Box::new(IcdSample::normal(
                consts.first().map(|c| c.as_f64()).unwrap_or(0.0),
                consts.get(1).map(|c| c.as_f64()).unwrap_or(1.0),
            )));
        }
        "icd_exponential" | "dist_exponential" => {
            use crate::sampling::icd::IcdSample;
            return Ok(Box::new(IcdSample::exponential(
                consts.first().map(|c| c.as_f64()).unwrap_or(1.0),
            )));
        }
        "dist_normal" => {
            use crate::sampling::icd::IcdSample;
            return Ok(Box::new(IcdSample::normal(
                consts.first().map(|c| c.as_f64()).unwrap_or(0.0),
                consts.get(1).map(|c| c.as_f64()).unwrap_or(1.0),
            )));
        }
        "dist_uniform" => {
            use crate::sampling::icd::IcdSample;
            return Ok(Box::new(IcdSample::uniform(
                consts.first().map(|c| c.as_f64()).unwrap_or(0.0),
                consts.get(1).map(|c| c.as_f64()).unwrap_or(1.0),
            )));
        }
        "dist_pareto" => {
            use crate::sampling::icd::IcdSample;
            return Ok(Box::new(IcdSample::pareto(
                consts.first().map(|c| c.as_f64()).unwrap_or(1.0),
                consts.get(1).map(|c| c.as_f64()).unwrap_or(1.0),
            )));
        }
        "dist_zipf" => {
            use crate::sampling::icd::IcdSample;
            return Ok(Box::new(IcdSample::zipf(
                consts.first().map(|c| c.as_u64()).unwrap_or(100),
                consts.get(1).map(|c| c.as_f64()).unwrap_or(1.0),
            )));
        }
        "histribution" => {
            // Assembly-time spec validation — the node's constructor
            // calls `parse_histribution` which asserts on malformed
            // input; we catch it here first so the builder stays
            // infallible (SRD 15 §"Const Constraint Metadata").
            let spec = consts.first().map(|c| c.as_str()).unwrap_or("1");
            if let Err(e) = validate_histribution_spec(spec) {
                return Err(format!("bad constant histribution: spec: {e}"));
            }
            return Ok(Box::new(crate::sampling::histribution::Histribution::new(spec)));
        }
        "dist_empirical" => {
            let spec = consts.first().map(|c| c.as_str()).unwrap_or("0.0 1.0");
            if let Err(e) = validate_dist_empirical_spec(spec) {
                return Err(format!("bad constant dist_empirical: spec: {e}"));
            }
            return Ok(Box::new(crate::sampling::lut::EmpiricalSample::from_spec(spec)));
        }
        _ => {}
    }

    // --- Registry variadic fallback ---
    if let Some(sig) = registry::lookup(func)
        && sig.is_variadic() {
            if wires.is_empty() {
                if let Some(id) = sig.identity {
                    return Ok(Box::new(ConstU64::new(id)));
                }
                return Err(format!("variadic function '{func}' requires at least one input"));
            }
            if let Some(ctor) = sig.variadic_ctor {
                return Ok(ctor(wires.len()));
            }
        }

    let mut msg = format!("unknown function: '{func}'\n");
    if let Some(suggestion) = registry::suggest_function(func) {
        msg.push_str(&format!("\n  Did you mean '{suggestion}'?"));
    }
    msg.push_str("\n\n  This function is not registered in the GK function library.");
    msg.push_str("\n  Use 'nbrs describe gk functions' to see all available functions.");
    Err(msg)
}

/// Walk `sig.params`, applying every declared `ConstConstraint`
/// to the corresponding positional `ConstArg`. Parameters with no
/// constraint are skipped; missing optional arguments are skipped
/// (the `required` flag handles mandatory presence elsewhere).
fn check_param_constraints(
    sig: &crate::dsl::registry::FuncSig,
    consts: &[ConstArg],
) -> Result<(), String> {
    use crate::node::SlotType;
    // Const args appear in positional order, but `sig.params`
    // mixes wire and const slots. Walk both in lockstep, pulling
    // const args from a separate counter.
    let mut const_idx = 0usize;
    for spec in sig.params {
        if matches!(spec.slot_type, SlotType::Wire) {
            continue;
        }
        if let Some(constraint) = &spec.constraint
            && let Some(arg) = consts.get(const_idx)
        {
            constraint.check(arg, spec.name)?;
        }
        const_idx += 1;
    }
    Ok(())
}

/// Validate a `histribution` spec string at assembly time.
///
/// Mirrors the semantics of
/// [`crate::sampling::histribution::parse_histribution`] but returns
/// a structured error instead of panicking, so the factory can reject
/// malformed specs before the node is ever constructed.
fn validate_histribution_spec(spec: &str) -> Result<(), String> {
    let labeled = spec.contains(':');
    let mut any = false;
    for elem in spec.split([' ', ',', ';']) {
        let elem = elem.trim();
        if elem.is_empty() { continue; }
        if labeled {
            let parts: Vec<&str> = elem.splitn(2, ':').collect();
            if parts.len() != 2 {
                return Err(format!("all elements must be labeled: '{elem}'"));
            }
            parts[0].parse::<u64>()
                .map_err(|_| format!("invalid label '{}'", parts[0]))?;
            parts[1].parse::<f64>()
                .map_err(|_| format!("invalid weight '{}'", parts[1]))?;
        } else {
            elem.parse::<f64>()
                .map_err(|_| format!("invalid weight '{elem}'"))?;
        }
        any = true;
    }
    if !any {
        return Err("spec must not be empty".into());
    }
    Ok(())
}

/// Validate a `dist_empirical` spec string at assembly time.
fn validate_dist_empirical_spec(spec: &str) -> Result<(), String> {
    let mut count = 0usize;
    for tok in spec.split([' ', ',', ';']) {
        let tok = tok.trim();
        if tok.is_empty() { continue; }
        tok.parse::<f64>()
            .map_err(|_| format!("invalid data point '{tok}'"))?;
        count += 1;
    }
    if count < 2 {
        return Err(format!("needs at least 2 data points, got {count}"));
    }
    Ok(())
}
