// Copyright 2024-2026 nosqlbench contributors
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

pub use crate::node::CompileLevel;
use crate::nodes::arithmetic::{SumN, ProductN, MinN, MaxN};

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
}

impl FuncCategory {
    /// Display name for the category (used in describe output).
    pub fn display_name(&self) -> &'static str {
        match self {
            Self::Hashing => "Hashing",
            Self::Arithmetic => "Arithmetic",
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
        }
    }

    /// Parse a category name from a string (case-insensitive).
    /// Used for `// @category: Name` syntax in stdlib modules.
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_lowercase().as_str() {
            "hashing" => Some(Self::Hashing),
            "arithmetic" => Some(Self::Arithmetic),
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
            _ => None,
        }
    }

    /// Canonical ordering for display (same order as the enum definition).
    pub fn display_order() -> &'static [Self] {
        &[
            Self::Hashing, Self::Arithmetic, Self::Variadic,
            Self::Conversions, Self::Distributions, Self::Datetime,
            Self::Encoding, Self::Interpolation, Self::Math, Self::Probability,
            Self::Weighted, Self::Formatting, Self::String,
            Self::Json, Self::ByteBuffers, Self::Digest, Self::Noise,
            Self::Regex, Self::Permutation, Self::RealData,
            Self::Context, Self::Diagnostic,
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

use FuncCategory as C;

/// Return the full registry of known functions.
pub fn registry() -> Vec<FuncSig> {
    #[allow(unused_mut)]
    let mut funcs = vec![
        // --- Hashing ---
        FuncSig {
            name: "hash", category: C::Hashing, outputs: 1,
            description: "64-bit xxHash3",
            help: "Deterministic 64-bit hash using xxHash3.\nThis is the fundamental entropy source: feed a cycle counter in,\nget pseudo-random bits out. Hash before mod/lerp to avoid patterns.\nParameters:\n  input — any u64 value (typically a cycle ordinal)\nExample: hash(cycle) -> mod(1000)\nTheory: xxHash3 is a non-cryptographic hash with excellent\navalanche properties and very high throughput.",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },

        // --- Variadic arithmetic ---
        FuncSig {
            name: "sum", category: C::Variadic, outputs: 1,
            description: "sum of N inputs (wrapping); identity = 0",
            help: "Wrapping addition of N wire inputs. With zero inputs returns 0.\nUseful for combining multiple independently generated components.\nParameters:\n  input... — any number of u64 wire inputs\nExample: sum(hash(cycle), hash(add(cycle, 1000)))\nIdentity element is 0. Overflow wraps at 2^64.",
            identity: Some(0),
            variadic_ctor: Some(|n| Box::new(SumN::new(n))),
            params: &[ParamSpec { name: "input", slot_type: SlotType::Wire, required: false }],
            arity: Arity::VariadicWires { min_wires: 0 },
            commutativity: crate::node::Commutativity::AllCommutative,
        },
        FuncSig {
            name: "product", category: C::Variadic, outputs: 1,
            description: "product of N inputs (wrapping); identity = 1",
            help: "Wrapping multiplication of N wire inputs. With zero inputs returns 1.\nUseful for combining independent scaling factors.\nParameters:\n  input... — any number of u64 wire inputs\nExample: product(hash(cycle), mod(cycle, 10))\nIdentity element is 1. Overflow wraps at 2^64.",
            identity: Some(1),
            variadic_ctor: Some(|n| Box::new(ProductN::new(n))),
            params: &[ParamSpec { name: "input", slot_type: SlotType::Wire, required: false }],
            arity: Arity::VariadicWires { min_wires: 0 },
            commutativity: crate::node::Commutativity::AllCommutative,
        },
        FuncSig {
            name: "min", category: C::Variadic, outputs: 1,
            description: "minimum of N inputs; identity = u64::MAX",
            help: "Returns the smallest of N wire inputs. With zero inputs returns u64::MAX.\nUseful for clamping to the lowest of several generated bounds.\nParameters:\n  input... — any number of u64 wire inputs\nExample: min(hash(cycle), mod(cycle, 1000))\nIdentity element is u64::MAX.",
            identity: Some(u64::MAX),
            variadic_ctor: Some(|n| Box::new(MinN::new(n))),
            params: &[ParamSpec { name: "input", slot_type: SlotType::Wire, required: false }],
            arity: Arity::VariadicWires { min_wires: 0 },
            commutativity: crate::node::Commutativity::AllCommutative,
        },
        FuncSig {
            name: "max", category: C::Variadic, outputs: 1,
            description: "maximum of N inputs; identity = 0",
            help: "Returns the largest of N wire inputs. With zero inputs returns 0.\nUseful for selecting the highest of several generated values.\nParameters:\n  input... — any number of u64 wire inputs\nExample: max(hash(cycle), mod(cycle, 500))\nIdentity element is 0.",
            identity: Some(0),
            variadic_ctor: Some(|n| Box::new(MaxN::new(n))),
            params: &[ParamSpec { name: "input", slot_type: SlotType::Wire, required: false }],
            arity: Arity::VariadicWires { min_wires: 0 },
            commutativity: crate::node::Commutativity::AllCommutative,
        },

        // --- Formatting ---
        FuncSig {
            name: "printf", category: C::Formatting,
            outputs: 1, description: "printf-style formatting: printf(fmt, a, b, ...) -> String",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "format", slot_type: SlotType::ConstStr, required: true },
            ],
            arity: Arity::VariadicWires { min_wires: 0 },
            commutativity: crate::node::Commutativity::Positional,
            help: "Printf-style string formatting with positional wire inputs.\nFormat string uses Rust-style {} placeholders with optional specifiers:\n  {:05} zero-pad, {:.2} precision, {:x} hex, {:X} HEX, {:b} binary, {:o} octal.\nParameters:\n  format   — format string constant (e.g. \"user-{:05}-score-{:.1}\")\n  input... — wire inputs matched positionally to placeholders\nExample: printf(\"id={:08x} val={:.2}\", hash(cycle), score)",
        },

        // --- Arithmetic ---
        FuncSig {
            name: "add", category: C::Arithmetic,
            outputs: 1, description: "add a constant (wrapping)",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true },
                ParamSpec { name: "addend", slot_type: SlotType::ConstU64, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Add a constant to a u64 value using wrapping arithmetic.\nUseful for offsetting ranges or shifting cycle ordinals.\nParameters:\n  input  — u64 wire input\n  addend — constant to add (wraps at 2^64)\nExample: add(hash(cycle), 1000000)",
        },
        FuncSig {
            name: "mul", category: C::Arithmetic,
            outputs: 1, description: "multiply by a constant (wrapping)",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true },
                ParamSpec { name: "factor", slot_type: SlotType::ConstU64, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Multiply a u64 value by a constant using wrapping arithmetic.\nUseful for scaling counters or spreading values across a stride.\nParameters:\n  input  — u64 wire input\n  factor — constant multiplier (wraps at 2^64)\nExample: mul(cycle, 7)",
        },
        FuncSig {
            name: "div", category: C::Arithmetic,
            outputs: 1, description: "divide by a constant",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true },
                ParamSpec { name: "divisor", slot_type: SlotType::ConstU64, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Integer division by a constant (truncating toward zero).\nUseful for coarsening values — e.g., grouping cycles into blocks.\nParameters:\n  input   — u64 wire input\n  divisor — constant divisor (must be > 0)\nExample: div(cycle, 100)  // groups into blocks of 100",
        },
        FuncSig {
            name: "mod", category: C::Arithmetic,
            outputs: 1, description: "modulo by a constant",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true },
                ParamSpec { name: "modulus", slot_type: SlotType::ConstU64, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Modular reduction: output = input % modulus, producing [0, K).\nThe most common operation after hash — bounds a hashed value\ninto a usable integer range.\nParameters:\n  input   — u64 wire input (typically hashed)\n  modulus — upper bound (exclusive, must be > 0)\nExample: mod(hash(cycle), 1000)  // yields 0..999",
        },
        FuncSig {
            name: "clamp", category: C::Arithmetic,
            outputs: 1, description: "clamp u64 to [min, max]",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true },
                ParamSpec { name: "min", slot_type: SlotType::ConstU64, required: true },
                ParamSpec { name: "max", slot_type: SlotType::ConstU64, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Saturating clamp: values below min become min, above max become max.\nUnlike mod (which wraps), clamp preserves relative ordering within\nthe valid range. Use when you need hard bounds without wrap-around.\nParameters:\n  input — u64 wire input\n  min   — lower bound (inclusive)\n  max   — upper bound (inclusive)\nExample: clamp(hash(cycle), 10, 500)",
        },
        FuncSig {
            name: "interleave", category: C::Arithmetic,
            outputs: 1, description: "interleave bits of two u64 values",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "a", slot_type: SlotType::Wire, required: true },
                ParamSpec { name: "b", slot_type: SlotType::Wire, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Interleave the bits of two u64 values into a single u64 (Morton code).\nBit 0 of a goes to bit 0, bit 0 of b goes to bit 1, bit 1 of a to bit 2, etc.\nUseful for combining two independent coordinates into one value\nthat preserves spatial locality.\nParameters:\n  a — first u64 wire input (even bits in output)\n  b — second u64 wire input (odd bits in output)\nExample: hash(interleave(x_coord, y_coord))",
        },
        FuncSig {
            name: "mixed_radix", category: C::Arithmetic, outputs: 0,
            description: "decompose into mixed-radix digits (output count = number of radixes)",
            help: "Decompose a single u64 into multiple coordinate digits, like\nnested loops unrolled into a flat index. Each radix defines the\nmodulus for that digit; radix=0 means unbounded (captures remainder).\nProduces one output port per radix.\nParameters:\n  input    — u64 wire input\n  radix... — one or more u64 constants (variadic)\nExample: mixed_radix(cycle, 10, 26, 0)  // 3 outputs: d0 in [0,10), d1 in [0,26), d2 unbounded\nTheory: mixed-radix decomposition generalizes base conversion;\neach position can have a different base.",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true },
            ],
            arity: Arity::VariadicConsts { min_consts: 1 },
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "identity", category: C::Arithmetic, outputs: 1,
            description: "passthrough",
            help: "Passes the input value through unchanged.\nUseful for debugging, naming intermediate values, or as a\nplaceholder during graph construction.\nParameters:\n  input — any wire value\nExample: identity(hash(cycle))  // same as hash(cycle)",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },

        // --- Conversions ---
        FuncSig {
            name: "unit_interval", category: C::Conversions, outputs: 1,
            description: "normalize u64 to f64 in [0, 1)",
            help: "Convert a u64 to an f64 in [0.0, 1.0) by dividing by 2^64.\nBridges the integer hash domain into the probability domain.\nFeed the result to lerp, distribution samplers, or coin flips.\nParameters:\n  input — u64 wire input (typically hashed)\nExample: unit_interval(hash(cycle)) -> lerp(0.0, 100.0)",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "clamp_f64", category: C::Conversions,
            outputs: 1, description: "clamp f64 to [min, max]",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true },
                ParamSpec { name: "min", slot_type: SlotType::ConstF64, required: true },
                ParamSpec { name: "max", slot_type: SlotType::ConstF64, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Clamp an f64 value to [min, max].\nUse after distributions with unbounded tails (normal, Cauchy)\nto enforce domain constraints, or to guard against edge values.\nParameters:\n  input — f64 wire input\n  min   — lower bound (inclusive, f64)\n  max   — upper bound (inclusive, f64)\nExample: clamp_f64(icd_normal(hash(cycle), 50.0, 10.0), 0.0, 100.0)",
        },
        FuncSig {
            name: "f64_to_u64", category: C::Conversions, outputs: 1,
            description: "truncate f64 to u64 (lossy)",
            help: "Truncate an f64 to u64 by dropping the fractional part toward zero.\nNegative values and NaN produce 0. Values above u64::MAX saturate.\nUse when you need a raw integer from a float without rounding.\nParameters:\n  input — f64 wire input",
            identity: None, variadic_ctor: None,
            params: &[ParamSpec { name: "input", slot_type: SlotType::Wire, required: true }],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "round_to_u64", category: C::Conversions, outputs: 1,
            description: "round f64 to nearest u64",
            help: "Round an f64 to the nearest u64 (half-to-even / banker's rounding).\nPreferred over truncation when you want the closest integer.\nNegative values and NaN produce 0.\nParameters:\n  input — f64 wire input",
            identity: None, variadic_ctor: None,
            params: &[ParamSpec { name: "input", slot_type: SlotType::Wire, required: true }],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "floor_to_u64", category: C::Conversions, outputs: 1,
            description: "floor f64 to u64",
            help: "Floor an f64 to the next lower u64 (round toward negative infinity).\nFor positive values, equivalent to truncation. Negative values yield 0.\nUse when you want consistent downward rounding.\nParameters:\n  input — f64 wire input",
            identity: None, variadic_ctor: None,
            params: &[ParamSpec { name: "input", slot_type: SlotType::Wire, required: true }],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "ceil_to_u64", category: C::Conversions, outputs: 1,
            description: "ceil f64 to u64",
            help: "Ceiling of an f64 to u64 (round toward positive infinity).\nAlways rounds up: 2.1 becomes 3. Negative values yield 0.\nUse when you need the next integer above a continuous value.\nParameters:\n  input — f64 wire input",
            identity: None, variadic_ctor: None,
            params: &[ParamSpec { name: "input", slot_type: SlotType::Wire, required: true }],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "discretize", category: C::Conversions,
            outputs: 1, description: "bin f64 into N equal-width buckets",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true },
                ParamSpec { name: "range", slot_type: SlotType::ConstU64, required: true },
                ParamSpec { name: "buckets", slot_type: SlotType::ConstU64, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Bin a continuous f64 into N equal-width integer buckets.\nInput is an f64 in [0, range); output is a u64 bucket index in [0, buckets).\nOut-of-range inputs are clamped to the first or last bucket.\nParameters:\n  input   — f64 wire input\n  range   — upper bound of the input domain (u64, cast to f64)\n  buckets — number of output bins (u64)\nExample: discretize(scale_range(hash(cycle), 0.0, 100.0), 100, 10)",
        },
        FuncSig {
            name: "format_u64", category: C::Conversions,
            outputs: 1, description: "format u64 as string (decimal/hex/octal/binary)",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true },
                ParamSpec { name: "radix", slot_type: SlotType::ConstU64, required: false },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Format a u64 as a string in the specified radix.\nRadix: 10=decimal (default), 16=hex (0x prefix), 8=octal (0o),\n2=binary (0b). Omit radix for plain decimal.\nParameters:\n  input — u64 wire input\n  radix — optional base (2, 8, 10, or 16; default 10)\nExample: format_u64(hash(cycle), 16)  // \"0x1a2b3c4d\"",
        },
        FuncSig {
            name: "format_f64", category: C::Conversions,
            outputs: 1, description: "format f64 with decimal precision",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true },
                ParamSpec { name: "precision", slot_type: SlotType::ConstU64, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Format an f64 with a fixed number of decimal places.\nPrecision 0 rounds to the nearest integer string.\nParameters:\n  input     — f64 wire input\n  precision — number of decimal digits (u64)\nExample: format_f64(scale_range(hash(cycle), 0.0, 100.0), 2)  // \"73.41\"",
        },
        FuncSig {
            name: "zero_pad_u64", category: C::Conversions,
            outputs: 1, description: "zero-pad u64 to fixed width string",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true },
                ParamSpec { name: "width", slot_type: SlotType::ConstU64, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Zero-pad a u64 to a fixed-width decimal string.\nShorter numbers are left-padded with zeros; longer numbers pass through.\nUseful for fixed-width identifiers, partition keys, or filenames.\nParameters:\n  input — u64 wire input\n  width — minimum string width (u64)\nExample: zero_pad_u64(mod(hash(cycle), 10000), 8)  // \"00004217\"",
        },

        // --- Distributions ---
        FuncSig {
            name: "dist_normal", category: C::Distributions,
            outputs: 1, description: "build normal distribution LUT",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "mean", slot_type: SlotType::ConstF64, required: true },
                ParamSpec { name: "stddev", slot_type: SlotType::ConstF64, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Build a normal (Gaussian) distribution lookup table.\nThe output is a LUT node — feed it into lut_sample to draw values.\nParameters:\n  mean   — center of the distribution\n  stddev — standard deviation (must be > 0)\nExample: dist_normal(50.0, 10.0) -> lut_sample(hash(cycle))\nTheory: pre-computes the inverse CDF into a table for O(1) sampling.",
        },
        FuncSig {
            name: "dist_exponential", category: C::Distributions,
            outputs: 1, description: "build exponential distribution LUT",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "rate", slot_type: SlotType::ConstF64, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Build an exponential distribution lookup table.\nModels time between events (inter-arrival times, latencies).\nThe output is a LUT node — feed it into lut_sample to draw values.\nParameters:\n  rate — rate parameter lambda (mean = 1/rate, must be > 0)\nExample: dist_exponential(0.5) -> lut_sample(hash(cycle))",
        },
        FuncSig {
            name: "dist_uniform", category: C::Distributions,
            outputs: 1, description: "build uniform distribution LUT",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "min", slot_type: SlotType::ConstF64, required: true },
                ParamSpec { name: "max", slot_type: SlotType::ConstF64, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Build a uniform distribution lookup table over [min, max].\nEvery value in the range is equally likely.\nThe output is a LUT node — feed it into lut_sample to draw values.\nParameters:\n  min — lower bound (inclusive, f64)\n  max — upper bound (exclusive, f64)\nExample: dist_uniform(0.0, 1000.0) -> lut_sample(hash(cycle))",
        },
        FuncSig {
            name: "dist_pareto", category: C::Distributions,
            outputs: 1, description: "build Pareto distribution LUT",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "scale", slot_type: SlotType::ConstF64, required: true },
                ParamSpec { name: "shape", slot_type: SlotType::ConstF64, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Build a Pareto (power-law) distribution lookup table.\nModels heavy-tailed phenomena: wealth, file sizes, city populations.\nThe output is a LUT node — feed it into lut_sample to draw values.\nParameters:\n  scale — minimum value (x_m, must be > 0)\n  shape — tail index (alpha, larger = thinner tail)\nExample: dist_pareto(1.0, 2.0) -> lut_sample(hash(cycle))",
        },
        FuncSig {
            name: "dist_zipf", category: C::Distributions,
            outputs: 1, description: "build Zipf distribution LUT",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "n", slot_type: SlotType::ConstU64, required: true },
                ParamSpec { name: "exponent", slot_type: SlotType::ConstF64, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Build a Zipf distribution lookup table over ranks [1, n].\nModels rank-frequency phenomena: word frequency, cache access patterns.\nThe output is a LUT node — feed it into lut_sample to draw values.\nParameters:\n  n        — number of elements (u64, must be > 0)\n  exponent — Zipf exponent s (f64, typically 1.0-2.0; higher = more skewed)\nExample: dist_zipf(1000, 1.07) -> lut_sample(hash(cycle))",
        },
        FuncSig {
            name: "lut_sample", category: C::Distributions,
            outputs: 1, description: "interpolating lookup table sample",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Sample from a precomputed lookup table via linear interpolation.\nInput is an f64 in [0, 1]; output is the interpolated table value.\nThis is the runtime half of distribution sampling: build the table\nwith dist_normal/dist_zipf/etc, then sample with lut_sample.\nParameters:\n  input — f64 wire in [0.0, 1.0] (typically from unit_interval)\nExample: lut_sample(unit_interval(hash(cycle)))  // wired to a dist_* LUT",
        },
        FuncSig {
            name: "icd_normal", category: C::Distributions,
            outputs: 1, description: "sample from normal distribution",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true },
                ParamSpec { name: "mean", slot_type: SlotType::ConstF64, required: true },
                ParamSpec { name: "stddev", slot_type: SlotType::ConstF64, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "One-step normal distribution sampling (builds LUT + samples internally).\nConvenience wrapper: equivalent to dist_normal -> lut_sample but\ncombined into a single node for simpler graph construction.\nParameters:\n  input  — u64 wire input (typically hashed)\n  mean   — center of the distribution (f64)\n  stddev — standard deviation (f64, must be > 0)\nExample: icd_normal(hash(cycle), 100.0, 15.0)",
        },
        FuncSig {
            name: "icd_exponential", category: C::Distributions,
            outputs: 1, description: "sample from exponential distribution",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true },
                ParamSpec { name: "rate", slot_type: SlotType::ConstF64, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "One-step exponential distribution sampling (builds LUT + samples).\nConvenience wrapper for modeling inter-arrival times and latencies.\nParameters:\n  input — u64 wire input (typically hashed)\n  rate  — rate parameter lambda (f64, mean = 1/rate)\nExample: icd_exponential(hash(cycle), 0.1)  // mean = 10.0",
        },

        FuncSig {
            name: "histribution", category: C::Distributions,
            outputs: 1, description: "discrete histogram distribution",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true },
                ParamSpec { name: "spec", slot_type: SlotType::ConstStr, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Sample from a discrete histogram distribution.\nParse an inline frequency spec into an alias table at init time.\nTwo formats:\n  Implicit labels: histribution(hash(cycle), \"50 25 13 12\") → outcomes 0-3\n  Explicit labels: histribution(hash(cycle), \"234:50 33:25 17:13 3:12\")\nDelimiters: space, comma, or semicolon.\nOutput is a u64 label.",
        },
        FuncSig {
            name: "dist_empirical", category: C::Distributions,
            outputs: 1, description: "empirical distribution from data points",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true },
                ParamSpec { name: "data", slot_type: SlotType::ConstStr, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Sample from an empirical distribution defined by observed data points.\nThe data string is a space/comma/semicolon-separated list of f64 values.\nAt init time, values are sorted and used as the inverse CDF directly.\nThe input is an f64 in [0,1] (from unit_interval); output is interpolated.\nExample: dist_empirical(unit_interval(hash(cycle)), \"1.2 3.5 5.0 7.8 12.1\")",
        },

        // --- Datetime ---
        FuncSig {
            name: "epoch_scale", category: C::Datetime,
            outputs: 1, description: "scale u64 to epoch millis",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true },
                ParamSpec { name: "factor", slot_type: SlotType::ConstU64, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Scale a u64 to epoch milliseconds by multiplying by a factor.\nUse to convert a counter in coarse units to millisecond timestamps.\nParameters:\n  input  — u64 wire input (e.g., a cycle counter)\n  factor — milliseconds per input unit (u64)\nExample: epoch_scale(cycle, 1000)  // treat input as seconds -> millis",
        },
        FuncSig {
            name: "epoch_offset", category: C::Datetime,
            outputs: 1, description: "add base epoch offset",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true },
                ParamSpec { name: "base", slot_type: SlotType::ConstU64, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Add a base epoch offset (milliseconds) to a u64 value.\nShifts a relative millisecond value into absolute epoch time.\nParameters:\n  input — u64 wire input (relative millis)\n  base  — epoch milliseconds to add (e.g., 1704067200000 for 2024-01-01)\nExample: epoch_offset(epoch_scale(cycle, 1000), 1704067200000)",
        },
        FuncSig {
            name: "to_timestamp", category: C::Datetime,
            outputs: 1, description: "format epoch millis as ISO-8601",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Format an epoch-millis u64 as an ISO-8601 timestamp string.\nProduces: \"YYYY-MM-DDThh:mm:ss.mmmZ\" (UTC, no timezone conversion).\nParameters:\n  input — u64 epoch milliseconds\nExample: to_timestamp(epoch_offset(epoch_scale(cycle, 1000), 1704067200000))",
        },
        FuncSig {
            name: "date_components", category: C::Datetime, outputs: 7,
            description: "decompose epoch to year/month/day/hour/min/sec/ms",
            help: "Decompose epoch milliseconds into 7 output ports:\nyear, month (1-12), day (1-31), hour (0-23), minute (0-59),\nsecond (0-59), millisecond (0-999). All values are UTC.\nUse when you need individual date/time fields for structured output.\nParameters:\n  input — u64 epoch milliseconds",
            identity: None, variadic_ctor: None,
            params: &[ParamSpec { name: "input", slot_type: SlotType::Wire, required: true }],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },

        // --- Encoding ---
        FuncSig {
            name: "html_encode", category: C::Encoding, outputs: 1,
            description: "HTML entity encode",
            help: "Escape HTML special characters: & < > \" ' become entity references.\nUse when embedding generated strings into HTML content to prevent\ninjection or rendering issues.\nParameters:\n  input — String wire input",
            identity: None, variadic_ctor: None,
            params: &[ParamSpec { name: "input", slot_type: SlotType::Wire, required: true }],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "html_decode", category: C::Encoding, outputs: 1,
            description: "HTML entity decode",
            help: "Decode HTML entity references back to literal characters.\nHandles named entities (&amp;, &lt;, etc.) and numeric references.\nUse when processing HTML content that needs to be plain text.\nParameters:\n  input — String wire input",
            identity: None, variadic_ctor: None,
            params: &[ParamSpec { name: "input", slot_type: SlotType::Wire, required: true }],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "url_encode", category: C::Encoding, outputs: 1,
            description: "URL percent-encode",
            help: "Percent-encode a string for safe use in URLs (RFC 3986).\nReserved and non-ASCII characters become %XX hex sequences.\nUse when generating query parameters or path segments.\nParameters:\n  input — String wire input",
            identity: None, variadic_ctor: None,
            params: &[ParamSpec { name: "input", slot_type: SlotType::Wire, required: true }],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "url_decode", category: C::Encoding, outputs: 1,
            description: "URL percent-decode",
            help: "Decode percent-encoded URL sequences back to literal characters.\nConverts %XX hex sequences and '+' (as space) to their originals.\nUse when processing URL-encoded input data.\nParameters:\n  input — String wire input",
            identity: None, variadic_ctor: None,
            params: &[ParamSpec { name: "input", slot_type: SlotType::Wire, required: true }],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },

        // --- Interpolation ---
        FuncSig {
            name: "lerp", category: C::Interpolation,
            outputs: 1, description: "linear interpolation with fixed endpoints",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true },
                ParamSpec { name: "a", slot_type: SlotType::ConstF64, required: true },
                ParamSpec { name: "b", slot_type: SlotType::ConstF64, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Linear interpolation: output = a + t * (b - a).\nInput must be an f64 in [0,1] (the interpolation parameter t).\nParameters:\n  input — f64 wire in [0.0, 1.0] (e.g., from unit_interval)\n  a     — start value (when t=0)\n  b     — end value (when t=1)\nExample: lerp(unit_interval(hash(cycle)), -50.0, 50.0)",
        },
        FuncSig {
            name: "scale_range", category: C::Interpolation,
            outputs: 1, description: "map u64 to f64 range",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true },
                ParamSpec { name: "min", slot_type: SlotType::ConstF64, required: true },
                ParamSpec { name: "max", slot_type: SlotType::ConstF64, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Maps a u64 directly to an f64 in [min, max). Equivalent to\nlerp(unit_interval(input), min, max) but fused into one node.\nParameters:\n  input — u64 wire input (typically hashed)\n  min   — lower bound of output range (inclusive)\n  max   — upper bound of output range (exclusive)\nExample: scale_range(hash(cycle), 0.0, 100.0)",
        },
        FuncSig {
            name: "quantize", category: C::Interpolation,
            outputs: 1, description: "round to nearest multiple of step",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true },
                ParamSpec { name: "step", slot_type: SlotType::ConstF64, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Round an f64 to the nearest multiple of a step size.\nOutput remains f64 at the grid point (unlike discretize which returns a bucket index).\nUseful for snapping coordinates to a tile grid or binning to fixed intervals.\nParameters:\n  input — f64 wire input\n  step  — grid spacing (f64, must be > 0)\nExample: quantize(scale_range(hash(cycle), 0.0, 100.0), 5.0)  // 0, 5, 10, ..., 100",
        },

        // --- Math (trig & elementary functions) ---
        FuncSig {
            name: "sin", category: C::Math,
            outputs: 1, description: "sine (radians)",
            help: "Sine of an f64 value in radians.\nOutput oscillates between -1 and 1.\n\nExample: sin(scale_range(hash(cycle), 0.0, 6.2832))",
            identity: None, variadic_ctor: None,
            params: &[ParamSpec { name: "input", slot_type: SlotType::Wire, required: true }],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "cos", category: C::Math,
            outputs: 1, description: "cosine (radians)",
            help: "Cosine of an f64 value in radians.\nOutput oscillates between -1 and 1.",
            identity: None, variadic_ctor: None,
            params: &[ParamSpec { name: "input", slot_type: SlotType::Wire, required: true }],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "tan", category: C::Math,
            outputs: 1, description: "tangent (radians)",
            help: "Tangent of an f64 value in radians.\nUnbounded output — has poles at odd multiples of pi/2.",
            identity: None, variadic_ctor: None,
            params: &[ParamSpec { name: "input", slot_type: SlotType::Wire, required: true }],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "asin", category: C::Math,
            outputs: 1, description: "arc sine (inverse sin)",
            help: "Arc sine: input in [-1, 1], output in [-pi/2, pi/2] radians.",
            identity: None, variadic_ctor: None,
            params: &[ParamSpec { name: "input", slot_type: SlotType::Wire, required: true }],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "acos", category: C::Math,
            outputs: 1, description: "arc cosine (inverse cos)",
            help: "Arc cosine: input in [-1, 1], output in [0, pi] radians.",
            identity: None, variadic_ctor: None,
            params: &[ParamSpec { name: "input", slot_type: SlotType::Wire, required: true }],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "atan", category: C::Math,
            outputs: 1, description: "arc tangent",
            help: "Arc tangent: output in (-pi/2, pi/2) radians.",
            identity: None, variadic_ctor: None,
            params: &[ParamSpec { name: "input", slot_type: SlotType::Wire, required: true }],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "atan2", category: C::Math,
            outputs: 1, description: "two-argument arc tangent",
            help: "atan2(y, x): angle in radians from positive x-axis to point (x,y).\nOutput in (-pi, pi]. Use for Cartesian-to-polar conversion.",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "y", slot_type: SlotType::Wire, required: true },
                ParamSpec { name: "x", slot_type: SlotType::Wire, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "sqrt", category: C::Math,
            outputs: 1, description: "square root",
            help: "Square root of an f64 value.\nReturns NaN for negative inputs.",
            identity: None, variadic_ctor: None,
            params: &[ParamSpec { name: "input", slot_type: SlotType::Wire, required: true }],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "abs_f64", category: C::Math,
            outputs: 1, description: "absolute value (f64)",
            help: "Absolute value of an f64. Always non-negative.",
            identity: None, variadic_ctor: None,
            params: &[ParamSpec { name: "input", slot_type: SlotType::Wire, required: true }],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "ln", category: C::Math,
            outputs: 1, description: "natural logarithm",
            help: "Natural logarithm (base e).\nReturns -inf for 0, NaN for negative inputs.",
            identity: None, variadic_ctor: None,
            params: &[ParamSpec { name: "input", slot_type: SlotType::Wire, required: true }],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "exp", category: C::Math,
            outputs: 1, description: "exponential (e^x)",
            help: "Exponential function: e raised to the power of input.\nexp(0) = 1, exp(1) ≈ 2.718.",
            identity: None, variadic_ctor: None,
            params: &[ParamSpec { name: "input", slot_type: SlotType::Wire, required: true }],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "pow", category: C::Math,
            outputs: 1, description: "power (base^exponent)",
            help: "Raise base to the power of exponent.\npow(2, 10) = 1024. Both inputs are f64 wires.",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "base", slot_type: SlotType::Wire, required: true },
                ParamSpec { name: "exponent", slot_type: SlotType::Wire, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },

        // --- Probability ---
        FuncSig {
            name: "fair_coin", category: C::Probability,
            outputs: 1, description: "50/50 binary outcome (0 or 1)",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Fair coin flip: deterministically returns 0 or 1 with 50/50 probability.\nEquivalent to mod(hash(input), 2). Use for simple binary decisions\nlike choosing between two data centers or two code paths.\nParameters:\n  input — u64 wire input (hashed internally)\nExample: fair_coin(cycle)  // 0 or 1",
        },
        FuncSig {
            name: "unfair_coin", category: C::Probability,
            outputs: 1, description: "biased coin: 1 with probability p, else 0",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true },
                ParamSpec { name: "p", slot_type: SlotType::ConstF64, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Biased coin: returns 1 with probability p, else 0.\nThe input is hashed to [0,1) and compared against p.\nUse for modeling probabilistic events: error injection, cache miss rates.\nParameters:\n  input — u64 wire input (hashed internally)\n  p     — probability of returning 1 (f64 in [0.0, 1.0])\nExample: unfair_coin(cycle, 0.1)  // 10% chance of 1",
        },
        FuncSig {
            name: "select", category: C::Probability,
            outputs: 1, description: "binary conditional: if_true when cond != 0, else if_false",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "cond", slot_type: SlotType::Wire, required: true },
                ParamSpec { name: "if_true", slot_type: SlotType::Wire, required: true },
                ParamSpec { name: "if_false", slot_type: SlotType::Wire, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Ternary conditional: returns if_true when cond != 0, else if_false.\nAll three inputs are always evaluated (no short-circuit — this is a DAG).\nCombine with fair_coin/unfair_coin/n_of for the condition wire.\nParameters:\n  cond     — u64 condition (0 = false, nonzero = true)\n  if_true  — value returned when cond != 0\n  if_false — value returned when cond == 0\nExample: select(unfair_coin(cycle, 0.1), slow_path, fast_path)",
        },
        FuncSig {
            name: "chance", category: C::Probability,
            outputs: 1, description: "like unfair_coin but returns f64 (0.0 or 1.0)",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true },
                ParamSpec { name: "p", slot_type: SlotType::ConstF64, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Like unfair_coin but returns 0.0 or 1.0 as f64.\nUse when the result feeds directly into f64 arithmetic\nwithout needing an explicit type conversion step.\nParameters:\n  input — u64 wire input (hashed internally)\n  p     — probability of returning 1.0 (f64 in [0.0, 1.0])\nExample: chance(cycle, 0.3)  // 30% chance of 1.0, else 0.0",
        },
        FuncSig {
            name: "n_of", category: C::Probability,
            outputs: 1, description: "exactly n of every m inputs return 1",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true },
                ParamSpec { name: "n", slot_type: SlotType::ConstU64, required: true },
                ParamSpec { name: "m", slot_type: SlotType::ConstU64, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Deterministic fractional selection: exactly n out of every m inputs return 1.\nUnlike unfair_coin (probabilistic), n_of guarantees exact counts\nover each window of m consecutive inputs.\nParameters:\n  input — u64 wire input\n  n     — number of selected inputs per window (u64, n <= m)\n  m     — window size (u64, must be > 0)\nExample: n_of(cycle, 3, 10)  // exactly 3 of every 10 cycles are 1",
        },
        FuncSig {
            name: "one_of", category: C::Probability,
            outputs: 1, description: "uniform selection from N constant values",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true },
                ParamSpec { name: "values", slot_type: SlotType::ConstStr, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Uniform selection from a comma-separated list of string values.\nHashes the input, picks one value with equal probability.\nUse for simple categorical selection when all outcomes are equally likely.\nParameters:\n  input  — u64 wire input (hashed internally)\n  values — comma-separated string values\nExample: one_of(cycle, \"red,green,blue\")",
        },
        FuncSig {
            name: "one_of_weighted", category: C::Probability,
            outputs: 1, description: "weighted selection from 'val:weight,...' spec",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true },
                ParamSpec { name: "spec", slot_type: SlotType::ConstStr, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Weighted selection from a \"value:weight,...\" spec string.\nWeights are relative and do not need to sum to 1.\nUse for unequal-probability categorical selection.\nParameters:\n  input — u64 wire input (hashed internally)\n  spec  — comma-separated value:weight pairs\nExample: one_of_weighted(cycle, \"200:80,404:10,500:5,503:5\")",
        },
        FuncSig {
            name: "blend", category: C::Probability,
            outputs: 1, description: "weighted mix: a*(1-mix) + b*mix",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "a", slot_type: SlotType::Wire, required: true },
                ParamSpec { name: "b", slot_type: SlotType::Wire, required: true },
                ParamSpec { name: "mix", slot_type: SlotType::ConstF64, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Weighted linear blend of two f64 wire inputs.\nResult = a * (1 - mix) + b * mix. At mix=0 you get pure a, at mix=1 pure b.\nParameters:\n  a   — first f64 wire input\n  b   — second f64 wire input\n  mix — blend factor (f64 in [0.0, 1.0])\nExample: blend(fast_latency, slow_latency, 0.3)  // 70% fast, 30% slow",
        },

        // --- Weighted ---
        FuncSig {
            name: "weighted_strings", category: C::Weighted,
            outputs: 1, description: "weighted string selection from inline spec",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true },
                ParamSpec { name: "spec", slot_type: SlotType::ConstStr, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Weighted string selection from a compact spec string.\nSpec format: \"value:weight,value:weight,...\" — weights are relative.\nParameters:\n  input — u64 wire input (typically hashed)\n  spec  — comma-separated value:weight pairs\nExample: weighted_strings(hash(cycle), \"red:3,green:2,blue:1\")",
        },
        FuncSig {
            name: "weighted_u64", category: C::Weighted,
            outputs: 1, description: "weighted u64 selection from inline spec",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true },
                ParamSpec { name: "spec", slot_type: SlotType::ConstStr, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Weighted u64 selection from a compact spec string.\nSpec format: \"value:weight,value:weight,...\" — values are parsed as u64.\nParameters:\n  input — u64 wire input (typically hashed)\n  spec  — comma-separated value:weight pairs (e.g. \"10:0.5,20:0.3,30:0.2\")\nExample: weighted_u64(hash(cycle), \"100:5,200:3,300:2\")",
        },

        FuncSig {
            name: "weighted_pick", category: C::Weighted,
            outputs: 1, description: "weighted u64 selection from inline weight/value pairs",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true },
                ParamSpec { name: "weight", slot_type: SlotType::ConstF64, required: true },
                ParamSpec { name: "value", slot_type: SlotType::ConstU64, required: true },
            ],
            arity: Arity::VariadicGroup {
                group: &[SlotType::ConstF64, SlotType::ConstU64],
                min_repeats: 1,
            },
            commutativity: crate::node::Commutativity::Positional,
            help: "Weighted categorical selection from inline weight/value pairs.\nUses the alias method for O(1) lookup after initialization.\nParameters:\n  input      — u64 wire input (typically hashed)\n  weight,val — repeating pairs: f64 weight, u64 value\nWeights are relative (need not sum to 1).\nExample: weighted_pick(hash(cycle), 3.0, 100, 1.0, 200, 1.0, 300)\nTheory: the alias method pre-computes a table so each lookup is\nconstant-time regardless of the number of categories.",
        },

        // --- String ---
        FuncSig {
            name: "combinations", category: C::String,
            outputs: 1, description: "mixed-radix character set mapping",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true },
                ParamSpec { name: "pattern", slot_type: SlotType::ConstStr, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Map a u64 to a string via mixed-radix indexing into character sets.\nPattern is semicolon-delimited character set specs per position.\nEach spec uses ranges (A-Z, 0-9) or literal characters.\nA single literal (like -) is emitted as-is without consuming a radix digit.\nParameters:\n  input   — u64 wire input\n  pattern — semicolon-separated charset specs\nExample: combinations(cycle, \"0-9;0-9;0-9;-;0-9;0-9;0-9;0-9\")  // \"372-8419\"",
        },
        FuncSig {
            name: "number_to_words", category: C::String, outputs: 1,
            description: "spell out number in English",
            help: "Convert a u64 to its English word representation.\nExample: 42 becomes \"forty-two\", 1000 becomes \"one thousand\".\nUseful for generating human-readable labels or test data.\nParameters:\n  input — u64 wire input",
            identity: None, variadic_ctor: None,
            params: &[ParamSpec { name: "input", slot_type: SlotType::Wire, required: true }],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },

        // --- Diagnostic ---
        FuncSig {
            name: "type_of", category: C::Diagnostic, outputs: 1,
            description: "emit type name as string",
            help: "Returns the runtime type name of the input value as a String.\nOutputs: \"U64\", \"F64\", \"Str\", \"Bool\", \"Bytes\", \"Json\", etc.\nUseful for debugging type mismatches in complex graphs.\nParameters:\n  input — any wire value",
            identity: None, variadic_ctor: None,
            params: &[ParamSpec { name: "input", slot_type: SlotType::Wire, required: true }],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "debug_repr", category: C::Diagnostic, outputs: 1,
            description: "emit Debug representation as string",
            help: "Returns the Rust Debug representation of the input value as a String.\nShows internal structure: U64(42), Str(\"hello\"), Bytes([0x01, ...]).\nMore detailed than type_of — use for inspecting actual values.\nParameters:\n  input — any wire value",
            identity: None, variadic_ctor: None,
            params: &[ParamSpec { name: "input", slot_type: SlotType::Wire, required: true }],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "inspect", category: C::Diagnostic, outputs: 1,
            description: "passthrough with stderr logging",
            help: "Passes the input value through unchanged while logging it to stderr.\nThe value is printed with its type and Debug repr on every evaluation.\nUse for live debugging during graph development — remove before production.\nParameters:\n  input — any wire value (passed through unmodified)",
            identity: None, variadic_ctor: None,
            params: &[ParamSpec { name: "input", slot_type: SlotType::Wire, required: true }],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },

        // --- Byte buffer ---
        FuncSig {
            name: "u64_to_bytes", category: C::ByteBuffers, outputs: 1,
            description: "convert u64 to 8 bytes LE",
            help: "Convert a u64 to an 8-byte little-endian byte buffer.\nThis is the bridge from the integer domain to the bytes domain.\nFeed the result into sha256, md5, to_hex, or to_base64.\nParameters:\n  input — u64 wire input",
            identity: None, variadic_ctor: None,
            params: &[ParamSpec { name: "input", slot_type: SlotType::Wire, required: true }],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "bytes_from_hash", category: C::ByteBuffers,
            outputs: 1, description: "generate N deterministic bytes",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true },
                ParamSpec { name: "size", slot_type: SlotType::ConstU64, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Generate N deterministic bytes from a u64 seed via chained hashing.\nEach 8-byte chunk is hash(seed + chunk_index). Fresh per cycle.\nParameters:\n  input — u64 wire input (seed value)\n  size  — number of bytes to generate (u64)\nExample: bytes_from_hash(hash(cycle), 32)  // 32 pseudo-random bytes",
        },
        FuncSig {
            name: "to_hex", category: C::ByteBuffers, outputs: 1,
            description: "encode bytes as hex string",
            help: "Encode a byte buffer as a lowercase hexadecimal string.\nEach byte becomes two hex digits: [0xDE, 0xAD] -> \"dead\".\nUse after sha256/md5/u64_to_bytes for human-readable output.\nParameters:\n  input — Bytes wire input",
            identity: None, variadic_ctor: None,
            params: &[ParamSpec { name: "input", slot_type: SlotType::Wire, required: true }],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "from_hex", category: C::ByteBuffers, outputs: 1,
            description: "decode hex string to bytes",
            help: "Decode a hexadecimal string to a byte buffer.\nAccepts uppercase or lowercase hex digits. The string length\nmust be even (two hex chars per byte).\nParameters:\n  input — String wire input (hex-encoded)",
            identity: None, variadic_ctor: None,
            params: &[ParamSpec { name: "input", slot_type: SlotType::Wire, required: true }],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },

        // --- Digest ---
        FuncSig {
            name: "sha256", category: C::Digest,
            outputs: 1, description: "SHA-256 digest",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Compute the SHA-256 cryptographic digest of a byte buffer.\nOutput is always 32 bytes. Use with to_hex or to_base64 for string output.\nParameters:\n  input — bytes wire input\nExample: sha256(bytes_from_hash(cycle, 64)) -> to_hex(...)",
        },
        FuncSig {
            name: "md5", category: C::Digest,
            outputs: 1, description: "MD5 digest",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Compute the MD5 digest of a byte buffer.\nOutput is always 16 bytes. Not cryptographically secure — use for\nchecksums, deduplication keys, or legacy compatibility only.\nParameters:\n  input — bytes wire input\nExample: md5(u64_to_bytes(hash(cycle))) -> to_hex(...)",
        },
        FuncSig {
            name: "to_base64", category: C::Digest, outputs: 1,
            description: "base64 encode",
            help: "Encode a byte buffer as a standard base64 string (RFC 4648).\nUse after digest functions for compact, printable output.\nExample: sha256(...) -> to_base64(...)\nParameters:\n  input — Bytes wire input",
            identity: None, variadic_ctor: None,
            params: &[ParamSpec { name: "input", slot_type: SlotType::Wire, required: true }],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "from_base64", category: C::Digest, outputs: 1,
            description: "base64 decode",
            help: "Decode a standard base64 string back to a byte buffer.\nAccepts standard base64 (RFC 4648) with optional padding.\nUse when processing base64-encoded input data.\nParameters:\n  input — String wire input (base64-encoded)",
            identity: None, variadic_ctor: None,
            params: &[ParamSpec { name: "input", slot_type: SlotType::Wire, required: true }],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },

        // --- JSON ---
        FuncSig {
            name: "to_json", category: C::Json,
            outputs: 1, description: "promote value to JSON",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Promote a scalar value to a JSON value.\nU64 -> JSON number, F64 -> JSON number, Bool -> JSON bool,\nStr -> JSON string, Json -> passed through unchanged.\nParameters:\n  input — any wire value\nExample: to_json(hash(cycle))  // JSON number",
        },
        FuncSig {
            name: "json_to_str", category: C::Json,
            outputs: 1, description: "serialize JSON to compact string",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Serialize a JSON value to a compact string representation.\nProduces minified JSON with no extra whitespace.\nParameters:\n  input — JSON wire input\nExample: json_to_str(to_json(hash(cycle)))  // \"42\"",
        },
        FuncSig {
            name: "escape_json", category: C::Json, outputs: 1,
            description: "escape string for JSON embedding",
            help: "Escape a string for safe embedding inside a JSON string literal.\nBackslashes, quotes, control characters, and unicode are escaped.\nUse when building JSON by hand via printf rather than to_json.\nParameters:\n  input — String wire input",
            identity: None, variadic_ctor: None,
            params: &[ParamSpec { name: "input", slot_type: SlotType::Wire, required: true }],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "json_merge", category: C::Json, outputs: 1,
            description: "shallow merge two JSON objects",
            help: "Shallow-merge two JSON objects: keys in b override keys in a.\nBoth inputs must be JSON objects. Non-object inputs produce an error.\nUse to combine independently generated JSON fragments.\nParameters:\n  a — JSON object wire input (base)\n  b — JSON object wire input (overrides)",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "a", slot_type: SlotType::Wire, required: true },
                ParamSpec { name: "b", slot_type: SlotType::Wire, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },

        // --- Context ---
        FuncSig {
            name: "current_epoch_millis", category: C::Context, outputs: 1,
            description: "current wall-clock time (non-deterministic)",
            help: "Returns the current wall-clock time as epoch milliseconds.\nNON-DETERMINISTIC: returns a different value on each evaluation.\nUse for real-time timestamps in generated records.\nTakes no wire inputs.",
            identity: None, variadic_ctor: None,
            params: &[],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "counter", category: C::Context, outputs: 1,
            description: "monotonic counter (non-deterministic)",
            help: "Returns a monotonically increasing u64 counter.\nNON-DETERMINISTIC: increments on each evaluation across all threads.\nUse for sequence numbers, unique IDs, or ordering guarantees.\nTakes no wire inputs.",
            identity: None, variadic_ctor: None,
            params: &[],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "session_start_millis", category: C::Context, outputs: 1,
            description: "session start time (frozen at init)",
            help: "Returns the epoch milliseconds when the session was initialized.\nFrozen at init time — returns the same value on every evaluation.\nUse as a stable base timestamp for relative time calculations.\nTakes no wire inputs.",
            identity: None, variadic_ctor: None,
            params: &[],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },

        FuncSig {
            name: "elapsed_millis", category: C::Context, outputs: 1,
            description: "elapsed milliseconds since session start",
            help: "Returns elapsed milliseconds since the session was initialized.\nNon-deterministic: grows monotonically over the session.\nUse for relative time offsets in generated records.\nTakes no wire inputs.",
            identity: None, variadic_ctor: None,
            params: &[],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "thread_id", category: C::Context, outputs: 1,
            description: "current OS thread numeric ID",
            help: "Returns the current OS thread's numeric identifier as u64.\nNon-deterministic: different fibers may run on different threads.\nUseful for partitioning or sharding in multi-threaded workloads.\nTakes no wire inputs.",
            identity: None, variadic_ctor: None,
            params: &[],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },

        // --- Noise ---
        FuncSig {
            name: "perlin_1d", category: C::Noise,
            outputs: 1, description: "1D Perlin noise",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true },
                ParamSpec { name: "seed", slot_type: SlotType::ConstU64, required: true },
                ParamSpec { name: "frequency", slot_type: SlotType::ConstF64, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "1D Perlin noise: coherent pseudo-random f64 in [-1, 1].\nThe u64 input is scaled to the float domain by frequency.\nNearby inputs produce smoothly varying outputs (spatial correlation).\nParameters:\n  input     — u64 wire input\n  seed      — permutation table seed (u64)\n  frequency — spatial frequency (f64; higher = more detail)\nExample: perlin_1d(cycle, 42, 0.01)  // slow-varying noise",
        },
        FuncSig {
            name: "perlin_2d", category: C::Noise,
            outputs: 1, description: "2D Perlin noise",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "x", slot_type: SlotType::Wire, required: true },
                ParamSpec { name: "y", slot_type: SlotType::Wire, required: true },
                ParamSpec { name: "seed", slot_type: SlotType::ConstU64, required: true },
                ParamSpec { name: "frequency", slot_type: SlotType::ConstF64, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "2D Perlin noise: coherent pseudo-random f64 in [-1, 1].\nTwo u64 coordinate inputs are scaled by frequency.\nProduces spatially correlated values for terrain, textures, etc.\nParameters:\n  x         — u64 x-coordinate wire input\n  y         — u64 y-coordinate wire input\n  seed      — permutation table seed (u64)\n  frequency — spatial frequency (f64)\nExample: perlin_2d(row, col, 42, 0.05)",
        },
        FuncSig {
            name: "simplex_2d", category: C::Noise,
            outputs: 1, description: "2D simplex noise",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "x", slot_type: SlotType::Wire, required: true },
                ParamSpec { name: "y", slot_type: SlotType::Wire, required: true },
                ParamSpec { name: "seed", slot_type: SlotType::ConstU64, required: true },
                ParamSpec { name: "frequency", slot_type: SlotType::ConstF64, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "2D simplex noise: faster than Perlin for 2D+ with fewer directional artifacts.\nOutput is f64 in [-1, 1]. Uses a simplex grid instead of a square grid.\nParameters:\n  x         — u64 x-coordinate wire input\n  y         — u64 y-coordinate wire input\n  seed      — permutation table seed (u64)\n  frequency — spatial frequency (f64)\nExample: simplex_2d(row, col, 99, 0.1)",
        },

        // --- Regex ---
        FuncSig {
            name: "regex_replace", category: C::Regex,
            outputs: 1, description: "regex substitution",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true },
                ParamSpec { name: "pattern", slot_type: SlotType::ConstStr, required: true },
                ParamSpec { name: "replacement", slot_type: SlotType::ConstStr, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Substitute all matches of a regex pattern in the input string.\nThe regex is compiled at init time for fast cycle-time evaluation.\nParameters:\n  input       — String wire input\n  pattern     — regex pattern (Rust regex syntax)\n  replacement — replacement string ($1, $2 for capture groups)\nExample: regex_replace(name, \"[^a-zA-Z]\", \"_\")",
        },
        FuncSig {
            name: "regex_match", category: C::Regex,
            outputs: 1, description: "test if string matches regex",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true },
                ParamSpec { name: "pattern", slot_type: SlotType::ConstStr, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Test if a string matches a regex pattern. Returns 1 (match) or 0 (no match).\nThe regex is compiled at init time. Tests for a partial match\n(use ^...$ anchors for a full match).\nParameters:\n  input   — String wire input\n  pattern — regex pattern (Rust regex syntax)\nExample: regex_match(email, \"^[^@]+@[^@]+$\")",
        },

        // --- PCG RNG ---
        FuncSig {
            name: "pcg", category: C::Permutation,
            outputs: 1, description: "PCG-RXS-M-XS seekable RNG (pure function)",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true },
                ParamSpec { name: "seed", slot_type: SlotType::ConstU64, required: true },
                ParamSpec { name: "stream", slot_type: SlotType::ConstU64, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Seekable pseudo-random generator: a pure function from position.\nPCG-RXS-M-XS variant — given the same input, seed, and stream,\nalways returns the same output. Stateless alternative to hash.\nParameters:\n  input  — u64 position (cycle ordinal)\n  seed   — initialization constant\n  stream — stream selector (different streams = independent sequences)\nExample: pcg(cycle, 42, 0)\nTheory: PCG uses a linear congruential core with permuted output;\nthe RXS-M-XS variant supports O(1) seeking to any position.",
        },
        FuncSig {
            name: "pcg_stream", category: C::Permutation,
            outputs: 1, description: "PCG with dynamic stream ID from wire",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true },
                ParamSpec { name: "stream", slot_type: SlotType::Wire, required: true },
                ParamSpec { name: "seed", slot_type: SlotType::ConstU64, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "PCG with a runtime (wire) stream ID instead of a fixed constant.\nUse when the stream identity is data-dependent (e.g., derived from\na partition key) and cannot be fixed at assembly time.\nParameters:\n  input  — u64 position (cycle ordinal)\n  stream — u64 wire input selecting the stream\n  seed   — initialization constant (u64)\nExample: pcg_stream(cycle, partition_id, 42)",
        },
        FuncSig {
            name: "cycle_walk", category: C::Permutation,
            outputs: 1, description: "bijective permutation via Feistel + cycle-walking",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true },
                ParamSpec { name: "range", slot_type: SlotType::ConstU64, required: true },
                ParamSpec { name: "seed", slot_type: SlotType::ConstU64, required: true },
                ParamSpec { name: "stream", slot_type: SlotType::ConstU64, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Bijective permutation of [0, range) via a Feistel network + cycle-walking.\nEvery input maps to a unique output (and vice versa) within [0, range).\nUse to visit every row exactly once in pseudo-random order, or to\ngenerate unique IDs without a tracking structure.\nParameters:\n  input  — u64 wire input (position in [0, range))\n  range  — domain size (u64, must be > 0)\n  seed   — Feistel round key seed (u64)\n  stream — stream selector (u64)\nExample: cycle_walk(cycle, 1000000, 42, 0)",
        },

        // --- Permutation ---
        FuncSig {
            name: "shuffle", category: C::Permutation,
            outputs: 1, description: "bijective LFSR permutation",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true },
                ParamSpec { name: "min", slot_type: SlotType::ConstU64, required: false },
                ParamSpec { name: "size", slot_type: SlotType::ConstU64, required: true },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Bijective permutation: maps [0, size) to [0, size) with no collisions.\nEvery input maps to a unique output and vice versa — a perfect shuffle.\nParameters:\n  input — u64 wire input\n  min   — optional offset added to output (default 0)\n  size  — range size (required)\nExample: shuffle(cycle, 0, 10000)  // permute 0..9999\nTheory: uses a maximal-length LFSR (linear feedback shift register)\nto generate a permutation without storing a full table.",
        },

        // --- Real-world data ---
        FuncSig {
            name: "first_names", category: C::RealData, outputs: 1,
            description: "Census first name (weighted)",
            help: "Select a first name from US Census data, weighted by frequency.\nMore common names appear proportionally more often.\nUse for realistic person-name generation in test data.\nParameters:\n  input — u64 wire input (typically hashed)",
            identity: None, variadic_ctor: None,
            params: &[ParamSpec { name: "input", slot_type: SlotType::Wire, required: true }],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "full_names", category: C::RealData, outputs: 1,
            description: "full name (first + last)",
            help: "Generate a full name (first + last) from Census data.\nFirst and last names are selected independently, both weighted\nby frequency. Produces realistic \"Jane Smith\" style names.\nParameters:\n  input — u64 wire input (typically hashed)",
            identity: None, variadic_ctor: None,
            params: &[ParamSpec { name: "input", slot_type: SlotType::Wire, required: true }],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "state_codes", category: C::RealData, outputs: 1,
            description: "US state abbreviation",
            help: "Select a US state abbreviation (e.g., \"CA\", \"NY\", \"TX\").\nAll 50 states plus DC are included with equal probability.\nUse for generating realistic US address data.\nParameters:\n  input — u64 wire input (typically hashed)",
            identity: None, variadic_ctor: None,
            params: &[ParamSpec { name: "input", slot_type: SlotType::Wire, required: true }],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "country_names", category: C::RealData, outputs: 1,
            description: "country name",
            help: "Select a country name from the full ISO list.\nAll countries are included with equal probability.\nUse for generating geographic diversity in test data.\nParameters:\n  input — u64 wire input (typically hashed)",
            identity: None, variadic_ctor: None,
            params: &[ParamSpec { name: "input", slot_type: SlotType::Wire, required: true }],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
    ];

    // Vectordata nodes (feature-gated)
    #[cfg(feature = "vectordata")]
    {
        let vd = vec![
            FuncSig {
                name: "vector_at", category: C::RealData, outputs: 1,
                description: "access base vector by index (string)",
                help: "Look up a base vector by index from a loaded dataset.\nReturns the vector as a JSON array string: [0.1,0.2,...].\nThe index wraps modulo the dataset size.\nRequires a dataset loaded at init time.\nExample: vector_at(mod(cycle, vector_count), dataset)",
                identity: None, variadic_ctor: None,
                params: &[
                    ParamSpec { name: "index", slot_type: SlotType::Wire, required: true },
                    ParamSpec { name: "source", slot_type: SlotType::ConstStr, required: true },
                ],
                arity: Arity::Fixed,
                commutativity: crate::node::Commutativity::Positional,
            },
            FuncSig {
                name: "vector_at_bytes", category: C::RealData, outputs: 1,
                description: "access base vector by index (bytes)",
                help: "Look up a base vector by index, returning raw f32 little-endian bytes.\nSuitable for CQL blob columns or binary protocols.",
                identity: None, variadic_ctor: None,
                params: &[
                    ParamSpec { name: "index", slot_type: SlotType::Wire, required: true },
                    ParamSpec { name: "source", slot_type: SlotType::ConstStr, required: true },
                ],
                arity: Arity::Fixed,
                commutativity: crate::node::Commutativity::Positional,
            },
            FuncSig {
                name: "query_vector_at", category: C::RealData, outputs: 1,
                description: "access query vector by index (string)",
                help: "Look up a query vector by index from a loaded dataset.\nReturns the vector as a JSON array string.",
                identity: None, variadic_ctor: None,
                params: &[
                    ParamSpec { name: "index", slot_type: SlotType::Wire, required: true },
                    ParamSpec { name: "source", slot_type: SlotType::ConstStr, required: true },
                ],
                arity: Arity::Fixed,
                commutativity: crate::node::Commutativity::Positional,
            },
            FuncSig {
                name: "query_vector_at_bytes", category: C::RealData, outputs: 1,
                description: "access query vector by index (bytes)",
                help: "Look up a query vector by index, returning raw f32 little-endian bytes.",
                identity: None, variadic_ctor: None,
                params: &[
                    ParamSpec { name: "index", slot_type: SlotType::Wire, required: true },
                    ParamSpec { name: "source", slot_type: SlotType::ConstStr, required: true },
                ],
                arity: Arity::Fixed,
                commutativity: crate::node::Commutativity::Positional,
            },
            FuncSig {
                name: "neighbor_indices_at", category: C::RealData, outputs: 1,
                description: "ground-truth neighbor indices for a query",
                help: "Look up ground-truth k-nearest neighbor indices for a query.\nReturns indices as a JSON array string: [42,17,99,...].\nUsed for recall verification in vector search workloads.",
                identity: None, variadic_ctor: None,
                params: &[
                    ParamSpec { name: "index", slot_type: SlotType::Wire, required: true },
                    ParamSpec { name: "source", slot_type: SlotType::ConstStr, required: true },
                ],
                arity: Arity::Fixed,
                commutativity: crate::node::Commutativity::Positional,
            },
            FuncSig {
                name: "neighbor_distances_at", category: C::RealData, outputs: 1,
                description: "ground-truth neighbor distances for a query",
                help: "Look up ground-truth distances for a query's k-nearest neighbors.\nReturns distances as a JSON array string.",
                identity: None, variadic_ctor: None,
                params: &[
                    ParamSpec { name: "index", slot_type: SlotType::Wire, required: true },
                    ParamSpec { name: "source", slot_type: SlotType::ConstStr, required: true },
                ],
                arity: Arity::Fixed,
                commutativity: crate::node::Commutativity::Positional,
            },
            FuncSig {
                name: "filtered_neighbor_indices_at", category: C::RealData, outputs: 1,
                description: "filtered ground-truth neighbor indices",
                help: "Look up filtered ground-truth neighbor indices for a query.\nUsed for filtered ANN recall verification.",
                identity: None, variadic_ctor: None,
                params: &[
                    ParamSpec { name: "index", slot_type: SlotType::Wire, required: true },
                    ParamSpec { name: "source", slot_type: SlotType::ConstStr, required: true },
                ],
                arity: Arity::Fixed,
                commutativity: crate::node::Commutativity::Positional,
            },
            FuncSig {
                name: "filtered_neighbor_distances_at", category: C::RealData, outputs: 1,
                description: "filtered ground-truth neighbor distances",
                help: "Look up filtered ground-truth distances for a query.\nUsed for filtered ANN recall verification.",
                identity: None, variadic_ctor: None,
                params: &[
                    ParamSpec { name: "index", slot_type: SlotType::Wire, required: true },
                    ParamSpec { name: "source", slot_type: SlotType::ConstStr, required: true },
                ],
                arity: Arity::Fixed,
                commutativity: crate::node::Commutativity::Positional,
            },
            FuncSig {
                name: "dataset_distance_function", category: C::RealData, outputs: 1,
                description: "dataset distance/similarity function name",
                help: "Returns the distance function declared in the dataset metadata\n(e.g., 'cosine', 'euclidean', 'dot_product').\nConstant per dataset.\nExample: dataset_distance_function(\"glove-25-angular\")",
                identity: None, variadic_ctor: None,
                params: &[ParamSpec { name: "source", slot_type: SlotType::ConstStr, required: true }],
                arity: Arity::Fixed,
                commutativity: crate::node::Commutativity::Positional,
            },
            FuncSig {
                name: "vector_dim", category: C::RealData, outputs: 1,
                description: "dataset vector dimensionality",
                help: "Returns the dimensionality of vectors in the loaded dataset.\nConstant per dataset — evaluated once at init time.\nExample: vector_dim(\"glove-100\")",
                identity: None, variadic_ctor: None,
                params: &[ParamSpec { name: "source", slot_type: SlotType::ConstStr, required: true }],
                arity: Arity::Fixed,
                commutativity: crate::node::Commutativity::Positional,
            },
            FuncSig {
                name: "vector_count", category: C::RealData, outputs: 1,
                description: "dataset vector count",
                help: "Returns the number of vectors in the loaded dataset.\nConstant per dataset — evaluated once at init time.\nExample: vector_count(\"glove-100\")",
                identity: None, variadic_ctor: None,
                params: &[ParamSpec { name: "source", slot_type: SlotType::ConstStr, required: true }],
                arity: Arity::Fixed,
                commutativity: crate::node::Commutativity::Positional,
            },
        ];
        funcs.extend(vd);
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
