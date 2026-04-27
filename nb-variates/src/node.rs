// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Core types for GK nodes: values, ports, metadata, and the evaluation trait.
//!
//! The GK type system has three layers:
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
//! The [`GkNode`] trait is what every node function implements.
//! A node declares its port metadata via [`NodeMeta`] and evaluates
//! via `eval(&[Value], &mut [Value])`.

use std::fmt;

/// A value flowing through the DAG at runtime.
///
/// This is the universal runtime representation for all GK data.
/// Every node input and output is a `Value`. The variant determines
/// the data type:
///
/// | Variant | Rust type | GK DSL type | Usage |
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
    /// Heap-allocated string. Used for formatted output, weighted
    /// string selection, template interpolation, and any value
    /// that will appear directly in an op statement.
    Str(String),
    /// Raw byte buffer. Used for cryptographic digests, binary
    /// encoding/decoding, and byte-level data generation.
    Bytes(Vec<u8>),
    /// Structured JSON value. Used for vector representations
    /// (JSON arrays), complex structured data, and JSON merge ops.
    Json(serde_json::Value),
    /// Adapter-contributed reflected value. Carries type info and
    /// standard access methods (display, JSON, string, bytes).
    /// Enables protocol-native types (UUIDs, timestamps, inet
    /// addresses) to flow through GK without boxing to strings.
    Ext(Box<dyn ReflectedValue>),
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
            (Value::Str(a), Value::Str(b)) => a == b,
            (Value::Bytes(a), Value::Bytes(b)) => a == b,
            (Value::Json(a), Value::Json(b)) => a == b,
            (Value::None, Value::None) => true,
            (Value::Ext(a), Value::Ext(b)) => {
                a.type_name() == b.type_name() && a.display() == b.display()
            }
            _ => false,
        }
    }
}

/// Trait for adapter-contributed value types.
///
/// Any type that flows through the GK kernel as `Value::Ext` must
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
            Value::None => PortType::U64, // placeholder
        }
    }

    /// Best-effort string representation for any value.
    /// Works across all variants including Ext.
    pub fn to_display_string(&self) -> String {
        match self {
            Value::U64(v) => v.to_string(),
            Value::F64(v) => v.to_string(),
            Value::Bool(v) => v.to_string(),
            Value::Str(v) => v.clone(),
            Value::Bytes(v) => v.iter().map(|b| format!("{b:02x}")).collect(),
            Value::Json(v) => v.to_string(),
            Value::Ext(v) => v.display(),
            Value::None => String::new(),
        }
    }

    /// JSON representation for any value. Works across all variants.
    pub fn to_json_value(&self) -> serde_json::Value {
        match self {
            Value::U64(v) => serde_json::Value::from(*v),
            Value::F64(v) => serde_json::json!(*v),
            Value::Bool(v) => serde_json::Value::from(*v),
            Value::Str(v) => serde_json::Value::from(v.as_str()),
            Value::Bytes(v) => serde_json::Value::from(v.iter().map(|b| format!("{b:02x}")).collect::<String>()),
            Value::Json(v) => v.clone(),
            Value::Ext(v) => v.to_json_value(),
            Value::None => serde_json::Value::Null,
        }
    }
}

/// Compile-time type tag for a port on a GK node.
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

/// Runtime evaluation interface for a GK node.
///
/// Phase 1: called via `dyn GkNode` (dynamic dispatch with `Value` enum).
/// Phase 2: if all nodes in the DAG are u64-only and provide a
/// `compiled_u64` implementation, the assembly phase compiles the DAG
/// into a flat buffer evaluator with direct function calls.
pub trait GkNode: Send + Sync {
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
}

/// Determine the compile level of a node (works on trait objects).
pub fn compile_level_of(node: &dyn GkNode) -> CompileLevel {
    #[cfg(feature = "jit")]
    {
        let jit_op = crate::jit::classify_node(node);
        if !matches!(jit_op, crate::jit::JitOp::Fallback) {
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
    /// Runtime interpreter: `dyn GkNode` + `Value` enum.
    Phase1,
    /// Compiled closure: `Box<dyn Fn(&[u64], &mut [u64])>`.
    Phase2,
    /// JIT native code via Cranelift.
    Phase3,
}
