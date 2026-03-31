// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! Core types for GK nodes: values, ports, metadata, and the evaluation trait.

use std::fmt;

/// A value flowing through the DAG at runtime.
///
/// Phase 1 uses this enum for dynamic value representation.
/// The assembly phase guarantees type correctness, so runtime
/// code can safely unwrap to the expected variant.
///
/// The `Ext` variant carries adapter-contributed types via the
/// `ReflectedValue` trait. Any consumer can display, serialize,
/// or inspect an Ext value. The producing adapter (or any code
/// with the concrete type in scope) can downcast via `as_any()`.
#[derive(Debug, Clone)]
pub enum Value {
    U64(u64),
    F64(f64),
    Bool(bool),
    Str(String),
    Bytes(Vec<u8>),
    Json(serde_json::Value),
    /// Adapter-contributed reflected value. Carries type info and
    /// standard access methods (display, JSON, string, bytes).
    Ext(Box<dyn ReflectedValue>),
    /// Sentinel for uninitialized buffer slots.
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

/// The type of a port on a GK node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PortType {
    U64,
    F64,
    Bool,
    Str,
    Bytes,
    Json,
    /// Adapter-contributed reflected type.
    Ext,
}

impl fmt::Display for PortType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PortType::U64 => write!(f, "u64"),
            PortType::F64 => write!(f, "f64"),
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

/// Descriptor for a single input or output port on a node.
#[derive(Debug, Clone)]
pub struct Port {
    pub name: String,
    pub typ: PortType,
    pub lifecycle: Lifecycle,
}

impl Port {
    pub fn new(name: impl Into<String>, typ: PortType) -> Self {
        Self { name: name.into(), typ, lifecycle: Lifecycle::Cycle }
    }

    /// Create a port with explicit lifecycle.
    pub fn with_lifecycle(name: impl Into<String>, typ: PortType, lifecycle: Lifecycle) -> Self {
        Self { name: name.into(), typ, lifecycle }
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
}

/// Descriptor for an assembly-time parameter on a node.
#[derive(Debug, Clone)]
pub struct Param {
    pub name: String,
    pub value: ParamValue,
}

/// Assembly-time parameter values, baked into the node at construction.
#[derive(Debug, Clone)]
pub enum ParamValue {
    U64(u64),
    F64(f64),
    Str(String),
    VecU64(Vec<u64>),
}

/// Metadata describing a node's interface: its ports and parameters.
///
/// Generated per-node-type and queryable at runtime for assembly-time
/// validation and (Phase 2) compilation.
#[derive(Debug, Clone)]
pub struct NodeMeta {
    pub name: String,
    pub inputs: Vec<Port>,
    pub outputs: Vec<Port>,
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
