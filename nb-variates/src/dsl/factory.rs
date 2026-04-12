// Copyright 2024-2026 nosqlbench contributors
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
pub(crate) fn build_node(
    func: &str,
    wires: &[WireRef],
    consts: &[ConstArg],
) -> Result<Box<dyn GkNode>, String> {
    // --- Per-module dispatch via inventory ---

    use crate::dsl::registry::NodeRegistration;
    for reg in inventory::iter::<NodeRegistration> {
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
            return Ok(Box::new(crate::sampling::histribution::Histribution::new(
                consts.first().map(|c| c.as_str()).unwrap_or("1"),
            )));
        }
        "dist_empirical" => {
            return Ok(Box::new(crate::sampling::lut::EmpiricalSample::from_spec(
                consts.first().map(|c| c.as_str()).unwrap_or("0.0 1.0"),
            )));
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
