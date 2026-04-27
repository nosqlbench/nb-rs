// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Function registry: known function signatures for DSL validation.
//!
//! Each registered function declares its name, category, expected wire
//! inputs, constant parameters, output count, and variadic behavior.
//! The compiler uses this to validate calls at parse time and to
//! generically dispatch variadic functions.
//!
//! Categories are a type-safe enum — every function must declare one.
//! The `describe gk functions` command groups by category automatically.
//! Stdlib modules declare their category via `// @category: Name`
//! comment syntax.
//!
//! Signatures are owned by their respective node modules. This file
//! defines the shared types and the collector function.

pub use crate::node::CompileLevel;
use crate::assembly::WireRef;

/// A node module's registration: signatures + builder.
///
/// Each node module submits one of these at link time via `inventory::submit!`.
/// The runtime collects all submissions to build the function registry and
/// dispatch table without any explicit module list.
pub struct NodeRegistration {
    /// Returns the static slice of `FuncSig` entries for this module.
    pub signatures: fn() -> &'static [FuncSig],
    /// Attempts to build a node for the given function name.
    ///
    /// Returns `None` if the name is not handled by this module,
    /// or `Some(Ok(node))` / `Some(Err(msg))` if it is.
    pub build: fn(&str, &[WireRef], &[crate::dsl::factory::ConstArg]) -> Option<Result<Box<dyn crate::node::GkNode>, String>>,
    /// Optional assembly-time validator for this module's constants.
    ///
    /// The factory calls this **before** `build` whenever the name
    /// matches one of this module's functions. Returning `Err` makes
    /// the compile fail with a structured `bad constant` error, so
    /// the node itself never sees a malformed literal and can keep
    /// its constructor and `eval()` branch-free. See SRD 15 §"Const
    /// Constraint Metadata" for the contract.
    pub validate: Option<crate::dsl::const_constraints::NodeValidator>,
}

inventory::collect!(NodeRegistration);

/// Register a node module's signatures and builder with the GK runtime.
///
/// Place this call at module scope in each node module. The inventory crate
/// arranges for the registration to run before `main` so that `registry()`
/// and `build_node()` see all entries.
///
/// Two forms:
///
/// - `register_nodes!(signatures, build_node)` — no assembly-time
///   validation. The builder is responsible for handling any bad
///   input itself (usually by trusting the caller or panicking).
/// - `register_nodes!(signatures, build_node, validate_node)` — the
///   factory calls `validate_node(name, consts)` before `build_node`.
///   Use this to declare [`ConstConstraint`]-style checks so
///   constructors can stay infallible.
///
/// [`ConstConstraint`]: crate::dsl::const_constraints::ConstConstraint
#[macro_export]
macro_rules! register_nodes {
    ($sigs:expr, $builder:expr) => {
        inventory::submit! {
            $crate::dsl::registry::NodeRegistration {
                signatures: $sigs,
                build: $builder,
                validate: None,
            }
        }
    };
    ($sigs:expr, $builder:expr, $validator:expr) => {
        inventory::submit! {
            $crate::dsl::registry::NodeRegistration {
                signatures: $sigs,
                build: $builder,
                validate: Some($validator),
            }
        }
    };
}

/// Functional category for a GK node function.
///
/// Every native node and stdlib module belongs to exactly one category.
/// Categories drive the `describe gk functions` grouping and provide
/// semantic organization for documentation and discovery.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FuncCategory {
    /// Core deterministic hashing.
    Hashing,
    /// Integer arithmetic with constant parameters.
    Arithmetic,
    /// Comparison and selection: ==, !=, <, >, <=, >=, if(...).
    /// Comparison nodes return u64 truth values (0 or 1); select
    /// nodes pick between two operand values based on a u64 cond.
    Comparison,
    /// Variadic N-ary operations (sum, product, min, max).
    Variadic,
    /// Type conversions between u64, f64, String, etc.
    Conversions,
    /// Statistical distribution LUT builders and samplers.
    Distributions,
    /// Date and time generation and decomposition.
    Datetime,
    /// HTML, URL, hex, base64 encoding/decoding.
    Encoding,
    /// Linear interpolation, range mapping, quantization.
    Interpolation,
    /// Trigonometric and mathematical functions (sin, cos, sqrt, etc.).
    Math,
    /// Probability modeling: coins, selection, conditionals.
    Probability,
    /// Weighted categorical selection.
    Weighted,
    /// Printf-style and structured string formatting.
    Formatting,
    /// String generation: combinations, number words.
    String,
    /// JSON construction, serialization, merging.
    Json,
    /// Byte buffer construction and manipulation.
    ByteBuffers,
    /// Cryptographic and non-cryptographic digests.
    Digest,
    /// Coherent noise: Perlin, simplex.
    Noise,
    /// Regular expression matching and substitution.
    Regex,
    /// Bijective permutations and shuffles.
    Permutation,
    /// Real-world data: names, places, codes.
    RealData,
    /// Non-deterministic context: wall clock, counters.
    Context,
    /// Debugging and introspection.
    Diagnostic,
    /// File-based data access: CSV, JSONL, text files.
    Data,
}

impl FuncCategory {
    /// Display name for the category (used in describe output).
    pub fn display_name(&self) -> &'static str {
        match self {
            Self::Hashing => "Hashing",
            Self::Arithmetic => "Arithmetic",
            Self::Comparison => "Comparison",
            Self::Variadic => "Variadic",
            Self::Conversions => "Conversions",
            Self::Distributions => "Distributions",
            Self::Datetime => "Datetime",
            Self::Encoding => "Encoding",
            Self::Interpolation => "Interpolation",
            Self::Math => "Math",
            Self::Probability => "Probability",
            Self::Weighted => "Weighted",
            Self::Formatting => "Formatting",
            Self::String => "String",
            Self::Json => "JSON",
            Self::ByteBuffers => "Byte Buffers",
            Self::Digest => "Digest",
            Self::Noise => "Noise",
            Self::Regex => "Regex",
            Self::Permutation => "Permutation",
            Self::RealData => "Real Data",
            Self::Context => "Context",
            Self::Diagnostic => "Diagnostic",
            Self::Data => "Data",
        }
    }

    /// Parse a category name from a string (case-insensitive).
    /// Used for `// @category: Name` syntax in stdlib modules.
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_lowercase().as_str() {
            "hashing" => Some(Self::Hashing),
            "arithmetic" => Some(Self::Arithmetic),
            "comparison" | "compare" => Some(Self::Comparison),
            "variadic" => Some(Self::Variadic),
            "conversions" | "conversion" => Some(Self::Conversions),
            "distributions" | "distribution" => Some(Self::Distributions),
            "datetime" | "date" | "time" => Some(Self::Datetime),
            "encoding" => Some(Self::Encoding),
            "interpolation" | "lerp" => Some(Self::Interpolation),
            "math" | "trig" | "trigonometry" => Some(Self::Math),
            "probability" => Some(Self::Probability),
            "weighted" => Some(Self::Weighted),
            "formatting" | "format" | "printf" => Some(Self::Formatting),
            "string" | "strings" => Some(Self::String),
            "json" => Some(Self::Json),
            "byte buffers" | "bytebuffers" | "bytes" => Some(Self::ByteBuffers),
            "digest" => Some(Self::Digest),
            "noise" => Some(Self::Noise),
            "regex" => Some(Self::Regex),
            "permutation" | "shuffle" => Some(Self::Permutation),
            "real data" | "realdata" | "realer" => Some(Self::RealData),
            "context" => Some(Self::Context),
            "diagnostic" | "diagnostics" | "debug" => Some(Self::Diagnostic),
            "data" | "datafile" | "csv" | "jsonl" => Some(Self::Data),
            _ => None,
        }
    }

    /// Canonical ordering for display (same order as the enum definition).
    pub fn display_order() -> &'static [Self] {
        &[
            Self::Hashing, Self::Arithmetic, Self::Comparison, Self::Variadic,
            Self::Conversions, Self::Distributions, Self::Datetime,
            Self::Encoding, Self::Interpolation, Self::Math, Self::Probability,
            Self::Weighted, Self::Formatting, Self::String,
            Self::Json, Self::ByteBuffers, Self::Digest, Self::Noise,
            Self::Regex, Self::Permutation, Self::RealData,
            Self::Context, Self::Diagnostic, Self::Data,
        ]
    }
}

// ---------------------------------------------------------------------------
// Unified parameter specification (SRD 36 §Variadic)
// ---------------------------------------------------------------------------

use crate::node::SlotType;

/// Describes one parameter in a function's call signature.
///
/// A "slot template" — the type-level version of a `Slot` without
/// a concrete value. Parameters are listed in positional order
/// matching the DSL syntax.
#[derive(Debug, Clone, Copy)]
pub struct ParamSpec {
    /// Parameter name (for error messages and describe output).
    pub name: &'static str,
    /// Wire or constant, and if constant, what type.
    pub slot_type: SlotType,
    /// Whether this parameter must be provided.
    pub required: bool,
    /// Example value for this parameter, used for probing compile
    /// level and for documentation. Wire params use `"cycle"`,
    /// const params use a representative value that passes validation.
    pub example: &'static str,
    /// Optional assembly-time validation rule (SRD 15 §"Const
    /// Constraint Metadata"). The factory enforces this before
    /// `build_node` so node constructors can stay infallible and
    /// branch-free at runtime. `None` = no constraint declared
    /// (default for wires and unconstrained constants).
    pub constraint: Option<crate::dsl::const_constraints::ConstConstraint>,
}

impl ParamSpec {
    /// Convenience: chainable on a literal to attach a constraint.
    /// Used by node modules that want to keep the literal compact.
    pub const fn with_constraint(mut self, c: crate::dsl::const_constraints::ConstConstraint) -> Self {
        self.constraint = Some(c);
        self
    }
}

/// Arity specification for a function signature.
///
/// Describes which parts of the parameter list are fixed vs repeatable.
#[derive(Debug, Clone)]
#[derive(Default)]
pub enum Arity {
    /// Exactly the parameters declared in `params`.
    #[default]
    Fixed,
    /// Trailing wire parameters repeat (sum, product, min, max).
    VariadicWires { min_wires: usize },
    /// Trailing constant parameters repeat (mixed_radix).
    VariadicConsts { min_consts: usize },
    /// A repeating group of slot types (weighted_sum).
    VariadicGroup {
        group: &'static [SlotType],
        min_repeats: usize,
    },
}


/// Description of a registered function's signature.
pub struct FuncSig {
    /// Function name as used in the DSL.
    pub name: &'static str,
    /// Functional category.
    pub category: FuncCategory,
    /// Number of output ports (0 = dynamic, determined at compile time).
    pub outputs: usize,
    /// Short description for help/error messages.
    pub description: &'static str,
    /// Detailed help text: theory, usage examples, parameter meanings.
    /// Displayed in the graph editor help panel.
    pub help: &'static str,
    /// For variadic functions: the identity element for zero inputs.
    pub identity: Option<u64>,
    /// Factory for variadic nodes: takes wire count, returns node.
    pub variadic_ctor: Option<fn(usize) -> Box<dyn crate::node::GkNode>>,
    /// Positional parameter list: wires and constants in call order.
    pub params: &'static [ParamSpec],
    /// Arity specification.
    pub arity: Arity,
    /// Input commutativity for this function.
    pub commutativity: crate::node::Commutativity,
}

impl FuncSig {
    /// Number of wire inputs in the fixed parameter list.
    pub fn wire_input_count(&self) -> usize {
        self.params.iter().filter(|p| p.slot_type.is_wire()).count()
    }

    /// Whether this function accepts variadic arguments.
    pub fn is_variadic(&self) -> bool {
        !matches!(self.arity, Arity::Fixed)
    }

    /// Constant parameter names and whether they're required.
    pub fn const_param_info(&self) -> Vec<(&'static str, bool)> {
        self.params.iter()
            .filter(|p| p.slot_type.is_const())
            .map(|p| (p.name, p.required))
            .collect()
    }
}

impl std::fmt::Debug for FuncSig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FuncSig")
            .field("name", &self.name)
            .field("category", &self.category)
            .field("params", &self.params)
            .field("arity", &self.arity)
            .finish()
    }
}

impl Clone for FuncSig {
    fn clone(&self) -> Self {
        Self {
            name: self.name,
            category: self.category,
            outputs: self.outputs,
            description: self.description,
            help: self.help,
            identity: self.identity,
            variadic_ctor: self.variadic_ctor,
            params: self.params,
            arity: self.arity.clone(),
            commutativity: self.commutativity.clone(),
        }
    }
}

/// Return the full registry of known functions.
///
/// Iterates all `NodeRegistration` entries submitted via `inventory::submit!`
/// at link time. No explicit module list is required here — each node module
/// registers itself by calling `register_nodes!` at module scope.
pub fn registry() -> Vec<FuncSig> {
    let mut funcs = Vec::new();
    for reg in inventory::iter::<NodeRegistration> {
        funcs.extend_from_slice((reg.signatures)());
    }
    funcs
}

/// Return functions grouped by category in display order.
pub fn by_category() -> Vec<(FuncCategory, Vec<FuncSig>)> {
    let reg = registry();
    let mut groups: std::collections::HashMap<FuncCategory, Vec<FuncSig>> = std::collections::HashMap::new();
    for sig in reg {
        groups.entry(sig.category).or_default().push(sig);
    }
    FuncCategory::display_order().iter()
        .filter_map(|cat| {
            groups.remove(cat).map(|funcs| (*cat, funcs))
        })
        .collect()
}

/// Find the closest function name to a misspelling.
pub fn suggest_function(name: &str) -> Option<&'static str> {
    let reg = registry();
    let mut best: Option<(&str, usize)> = None;
    for sig in &reg {
        let dist = edit_distance(name, sig.name);
        if dist <= 3
            && (best.is_none() || dist < best.unwrap().1) {
                best = Some((sig.name, dist));
            }
    }
    best.map(|(name, _)| name)
}

/// Find a registered function by name.
pub fn lookup(name: &str) -> Option<&'static FuncSig> {
    let reg = registry();
    for sig in &reg {
        if sig.name == name {
            return Some(Box::leak(Box::new(sig.clone())));
        }
    }
    None
}

fn edit_distance(a: &str, b: &str) -> usize {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    let mut matrix = vec![vec![0usize; b.len() + 1]; a.len() + 1];
    for i in 0..=a.len() { matrix[i][0] = i; }
    for j in 0..=b.len() { matrix[0][j] = j; }
    for i in 1..=a.len() {
        for j in 1..=b.len() {
            let cost = if a[i - 1] == b[j - 1] { 0 } else { 1 };
            matrix[i][j] = (matrix[i - 1][j] + 1)
                .min(matrix[i][j - 1] + 1)
                .min(matrix[i - 1][j - 1] + cost);
        }
    }
    matrix[a.len()][b.len()]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn suggest_close_match() {
        assert_eq!(suggest_function("hsh"), Some("hash"));
        assert_eq!(suggest_function("hahs"), Some("hash"));
        assert_eq!(suggest_function("interleav"), Some("interleave"));
    }

    #[test]
    fn suggest_no_match() {
        assert_eq!(suggest_function("zzzzzzzzz"), None);
    }

    #[test]
    fn lookup_exists() {
        let sig = lookup("hash").unwrap();
        assert_eq!(sig.wire_input_count(), 1);
        assert_eq!(sig.outputs, 1);
        assert!(!sig.is_variadic());
        assert_eq!(sig.category, FuncCategory::Hashing);
    }

    #[test]
    fn lookup_missing() {
        assert!(lookup("nonexistent").is_none());
    }

    #[test]
    fn lookup_variadic() {
        let sig = lookup("sum").unwrap();
        assert!(sig.is_variadic());
        assert_eq!(sig.identity, Some(0));
        assert_eq!(sig.category, FuncCategory::Variadic);
    }

    #[test]
    fn every_function_has_category() {
        let reg = registry();
        for sig in &reg {
            // Just verify the category display name is non-empty
            assert!(!sig.category.display_name().is_empty(),
                "function '{}' has no category display name", sig.name);
        }
    }

    #[test]
    fn by_category_covers_all() {
        let grouped = by_category();
        let total: usize = grouped.iter().map(|(_, funcs)| funcs.len()).sum();
        let reg = registry();
        assert_eq!(total, reg.len(), "by_category must cover all registered functions");
    }

    #[test]
    fn category_parse_roundtrip() {
        for cat in FuncCategory::display_order() {
            let name = cat.display_name();
            let parsed = FuncCategory::parse(name);
            assert_eq!(parsed, Some(*cat), "failed to parse category '{name}'");
        }
    }

    #[test]
    fn registry_has_entries() {
        let reg = registry();
        assert!(reg.len() > 50, "registry should have 50+ functions");
    }

    // --- Unified param model tests ---

    #[test]
    fn arithmetic_params_populated() {
        // Verify key arithmetic functions have params.
        for name in &["add", "mul", "div", "mod", "clamp", "interleave", "mixed_radix"] {
            let sig = lookup(name).unwrap_or_else(|| panic!("missing '{name}'"));
            assert!(!sig.params.is_empty(),
                "function '{}' should have params populated", name);
        }
    }

    #[test]
    fn variadic_params_populated() {
        for name in &["sum", "product", "min", "max"] {
            let sig = lookup(name).unwrap_or_else(|| panic!("missing '{name}'"));
            assert!(matches!(sig.arity, Arity::VariadicWires { .. }),
                "function '{}' should have VariadicWires arity", name);
        }
    }

    #[test]
    fn mixed_radix_is_variadic_consts() {
        let sig = lookup("mixed_radix").unwrap();
        assert!(matches!(sig.arity, Arity::VariadicConsts { min_consts: 1 }),
            "mixed_radix should be VariadicConsts");
        assert_eq!(sig.params.len(), 1); // just the wire input
        assert!(matches!(sig.params[0].slot_type, SlotType::Wire));
    }

    #[test]
    fn printf_has_const_str_param() {
        let sig = lookup("printf").unwrap();
        assert_eq!(sig.params.len(), 1);
        assert!(matches!(sig.params[0].slot_type, SlotType::ConstStr));
        assert!(matches!(sig.arity, Arity::VariadicWires { .. }));
    }
}
