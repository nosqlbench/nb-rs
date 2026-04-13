// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! JSON construction, serialization, and manipulation nodes.
//!
//! JSON is a first-class `Value` type in the GK. Nodes can produce
//! and consume `Value::Json(serde_json::Value)` directly, avoiding
//! serialization/deserialization round-trips when passing structured
//! data between nodes or to adapters that consume JSON natively.

use crate::node::{GkNode, NodeMeta, Port, PortType, Slot, Value};
use serde_json::json;

// =================================================================
// Construction: build JSON values from inputs
// =================================================================

/// Build a JSON object from N named key-value pairs.
///
/// Signature: `(val_0: any, val_1: any, ...) -> (json)`
///
/// Keys are specified at init time. Values come from cycle-time wires.
/// Each input is converted to its JSON representation:
///   - U64 → JSON number
///   - F64 → JSON number
///   - Bool → JSON bool
///   - Str → JSON string
///   - Json → nested as-is
pub struct JsonObject {
    meta: NodeMeta,
    keys: Vec<String>,
}

impl JsonObject {
    /// Create with field names. Input count must match key count.
    pub fn new(keys: Vec<String>, input_types: Vec<PortType>) -> Self {
        assert_eq!(keys.len(), input_types.len(),
            "key count must match input count");
        let inputs: Vec<Port> = keys.iter().zip(input_types.iter())
            .map(|(k, &t)| Port::new(k.clone(), t))
            .collect();
        let slots: Vec<Slot> = inputs.iter().map(|p| Slot::Wire(p.clone())).collect();
        Self {
            meta: NodeMeta {
                name: "json_object".into(),
                outs: vec![Port::json("output")],
                ins: slots,
            },
            keys,
        }
    }
}

impl GkNode for JsonObject {
    fn meta(&self) -> &NodeMeta { &self.meta }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let mut map = serde_json::Map::new();
        for (i, key) in self.keys.iter().enumerate() {
            map.insert(key.clone(), value_to_json(&inputs[i]));
        }
        outputs[0] = Value::Json(serde_json::Value::Object(map));
    }
}

/// Build a JSON array from N inputs.
///
/// Signature: `(elem_0: any, elem_1: any, ...) -> (json)`
pub struct JsonArray {
    meta: NodeMeta,
}

impl JsonArray {
    pub fn new(input_types: Vec<PortType>) -> Self {
        let inputs: Vec<Port> = input_types.iter().enumerate()
            .map(|(i, &t)| Port::new(format!("elem_{i}"), t))
            .collect();
        let slots: Vec<Slot> = inputs.iter().map(|p| Slot::Wire(p.clone())).collect();
        Self {
            meta: NodeMeta {
                name: "json_array".into(),
                outs: vec![Port::json("output")],
                ins: slots,
            },
        }
    }
}

impl GkNode for JsonArray {
    fn meta(&self) -> &NodeMeta { &self.meta }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let arr: Vec<serde_json::Value> = inputs.iter().map(value_to_json).collect();
        outputs[0] = Value::Json(serde_json::Value::Array(arr));
    }
}

/// Wrap a single value as a JSON value.
///
/// Signature: `(input: any) -> (json)`
///
/// Useful for promoting a scalar to JSON for further composition.
pub struct ToJson {
    meta: NodeMeta,
}

impl ToJson {
    pub fn new(input_type: PortType) -> Self {
        Self {
            meta: NodeMeta {
                name: "to_json".into(),
                outs: vec![Port::json("output")],
                ins: vec![Slot::Wire(Port::new("input", input_type))],
            },
        }
    }
}

impl GkNode for ToJson {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::Json(value_to_json(&inputs[0]));
    }
}

/// Merge two JSON objects into one (shallow merge, right wins).
///
/// Signature: `(left: json, right: json) -> (json)`
pub struct JsonMerge {
    meta: NodeMeta,
}

impl Default for JsonMerge {
    fn default() -> Self {
        Self::new()
    }
}

impl JsonMerge {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "json_merge".into(),
                outs: vec![Port::json("output")],
                ins: vec![Slot::Wire(Port::json("left")), Slot::Wire(Port::json("right"))],
            },
        }
    }
}

impl GkNode for JsonMerge {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let left = inputs[0].as_json();
        let right = inputs[1].as_json();
        let mut result = left.clone();
        if let (serde_json::Value::Object(base), serde_json::Value::Object(overlay)) =
            (&mut result, right)
        {
            for (k, v) in overlay {
                base.insert(k.clone(), v.clone());
            }
        }
        outputs[0] = Value::Json(result);
    }
}

// =================================================================
// Serialization: JSON ↔ String
// =================================================================

/// Serialize a JSON value to a compact string.
///
/// Signature: `(input: json) -> (String)`
///
/// This is also the auto-adapter for Json → Str.
pub struct JsonToStr {
    meta: NodeMeta,
}

impl Default for JsonToStr {
    fn default() -> Self {
        Self::new()
    }
}

impl JsonToStr {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "__json_to_string".into(),
                outs: vec![Port::new("output", PortType::Str)],
                ins: vec![Slot::Wire(Port::json("input"))],
            },
        }
    }
}

impl GkNode for JsonToStr {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::Str(inputs[0].as_json().to_string());
    }
}

/// Serialize a JSON value to a pretty-printed string.
///
/// Signature: `(input: json) -> (String)`
pub struct JsonToStrPretty {
    meta: NodeMeta,
}

impl Default for JsonToStrPretty {
    fn default() -> Self {
        Self::new()
    }
}

impl JsonToStrPretty {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "json_to_str_pretty".into(),
                outs: vec![Port::new("output", PortType::Str)],
                ins: vec![Slot::Wire(Port::json("input"))],
            },
        }
    }
}

impl GkNode for JsonToStrPretty {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::Str(
            serde_json::to_string_pretty(inputs[0].as_json()).unwrap_or_default()
        );
    }
}

/// Parse a JSON string into a JSON value.
///
/// Signature: `(input: String) -> (json)`
pub struct StrToJson {
    meta: NodeMeta,
}

impl Default for StrToJson {
    fn default() -> Self {
        Self::new()
    }
}

impl StrToJson {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "str_to_json".into(),
                outs: vec![Port::json("output")],
                ins: vec![Slot::Wire(Port::new("input", PortType::Str))],
            },
        }
    }
}

impl GkNode for StrToJson {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let parsed = serde_json::from_str(inputs[0].as_str())
            .unwrap_or(serde_json::Value::Null);
        outputs[0] = Value::Json(parsed);
    }
}

/// Escape a string for safe embedding in a JSON string value.
///
/// Signature: `(input: String) -> (String)`
///
/// Escapes `"`, `\`, control characters, etc. Does NOT add
/// surrounding quotes — the result is the interior of a JSON string.
pub struct EscapeJson {
    meta: NodeMeta,
}

impl Default for EscapeJson {
    fn default() -> Self {
        Self::new()
    }
}

impl EscapeJson {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "escape_json".into(),
                outs: vec![Port::new("output", PortType::Str)],
                ins: vec![Slot::Wire(Port::new("input", PortType::Str))],
            },
        }
    }
}

impl GkNode for EscapeJson {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        // serde_json::to_string adds quotes; strip them for interior-only
        let json_str = serde_json::to_string(inputs[0].as_str()).unwrap_or_default();
        // Remove leading and trailing quote
        let interior = &json_str[1..json_str.len() - 1];
        outputs[0] = Value::Str(interior.to_string());
    }
}

// =================================================================
// Field access
// =================================================================

/// Extract a field from a JSON object by key.
///
/// Signature: `(input: json) -> (json)`
/// Param: `key: String`
pub struct JsonField {
    meta: NodeMeta,
    key: String,
}

impl JsonField {
    pub fn new(key: &str) -> Self {
        Self {
            meta: NodeMeta {
                name: format!("json_field[{key}]"),
                outs: vec![Port::json("output")],
                ins: vec![Slot::Wire(Port::json("input"))],
            },
            key: key.to_string(),
        }
    }
}

impl GkNode for JsonField {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let val = inputs[0].as_json();
        outputs[0] = Value::Json(
            val.get(&self.key).cloned().unwrap_or(serde_json::Value::Null)
        );
    }
}

// =================================================================
// Helpers
// =================================================================

fn value_to_json(v: &Value) -> serde_json::Value {
    match v {
        Value::U64(n) => json!(*n),
        Value::F64(n) => json!(*n),
        Value::Bool(b) => json!(*b),
        Value::Str(s) => json!(s),
        Value::Bytes(b) => {
            use base64::Engine;
            json!(base64::engine::general_purpose::STANDARD.encode(b))
        }
        Value::Json(j) => j.clone(),
        Value::Ext(v) => v.to_json_value(),
        Value::None => serde_json::Value::Null,
    }
}

// ---------------------------------------------------------------------------
// Signature declarations for the DSL registry
// ---------------------------------------------------------------------------

use crate::dsl::registry::{Arity, FuncCategory, FuncSig, ParamSpec};
use crate::node::SlotType;

/// Signatures for JSON construction and serialization nodes.
pub fn signatures() -> &'static [FuncSig] {
    use FuncCategory as C;
    &[
        FuncSig {
            name: "to_json", category: C::Json,
            outputs: 1, description: "promote value to JSON",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true, example: "cycle" },
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
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true, example: "cycle" },
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
            params: &[ParamSpec { name: "input", slot_type: SlotType::Wire, required: true, example: "cycle" }],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "json_merge", category: C::Json, outputs: 1,
            description: "shallow merge two JSON objects",
            help: "Shallow-merge two JSON objects: keys in b override keys in a.\nBoth inputs must be JSON objects. Non-object inputs produce an error.\nUse to combine independently generated JSON fragments.\nParameters:\n  a — JSON object wire input (base)\n  b — JSON object wire input (overrides)",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "a", slot_type: SlotType::Wire, required: true, example: "cycle" },
                ParamSpec { name: "b", slot_type: SlotType::Wire, required: true, example: "cycle" },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "array_len", category: C::Json, outputs: 1,
            description: "count elements in a bracket-encoded array",
            help: "Parse [a,b,c,...] and return the element count.\nWorks on JSON arrays and bracket-format vectors.\nReturns 0 for empty or non-array input.\nExample: array_len(metadata_indices_at(cycle, \"sift1m\"))",
            identity: None, variadic_ctor: None,
            params: &[ParamSpec { name: "array", slot_type: SlotType::Wire, required: true, example: "cycle" }],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "array_at", category: C::Json, outputs: 1,
            description: "access element at index in bracket-encoded array",
            help: "Return the element at a given index from [a,b,c,...].\nIndex wraps modulo array length.\nReturns empty string for empty arrays.\nExample: array_at(neighbor_indices_at(0, \"sift1m\"), cycle)",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "array", slot_type: SlotType::Wire, required: true, example: "cycle" },
                ParamSpec { name: "index", slot_type: SlotType::Wire, required: true, example: "cycle" },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "normalize_vector", category: C::Json, outputs: 1,
            description: "L2-normalize a bracket-encoded float vector string",
            help: "Parse [x,y,z,...], compute L2 norm, return normalized vector string.\nPasses through unchanged if input is not bracket-encoded or norm is zero.\nParameters:\n  vector — Str wire input\nExample: normalize_vector(random_vector(seed, 128))",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "vector", slot_type: SlotType::Wire, required: true, example: "cycle" },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "random_vector", category: C::Json, outputs: 1,
            description: "generate deterministic float vector as bracket-encoded string",
            help: "Generate a deterministic vector of `dim` f64 values in [min, max).\nSeed and dim are cycle-time wires; min and max are consts (default 0.0, 1.0).\nUses xxHash3 for each element — same seed always produces the same vector.\nParameters:\n  seed — u64 wire input\n  dim  — u64 wire input\n  min  — f64 const (default 0.0)\n  max  — f64 const (default 1.0)\nExample: random_vector(hash(cycle), 128, 0.0, 1.0)",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "seed", slot_type: SlotType::Wire, required: true, example: "cycle" },
                ParamSpec { name: "dim", slot_type: SlotType::Wire, required: true, example: "cycle" },
                ParamSpec { name: "min", slot_type: SlotType::ConstF64, required: false, example: "0.0" },
                ParamSpec { name: "max", slot_type: SlotType::ConstF64, required: false, example: "1.0" },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
    ]
}

// =================================================================
// Vector operations: normalize and random generation
// =================================================================

/// L2-normalize a bracket-encoded float vector string `[1.0,2.0,3.0]`.
///
/// Parses the bracket-format vector, computes the L2 norm, and returns
/// a normalized vector in the same bracket format. Passes through
/// unchanged if the input is not bracket-encoded or the norm is
/// effectively zero.
///
/// Signature: `normalize_vector(vector: Str) -> (output: Str)`
pub struct NormalizeVector {
    meta: NodeMeta,
}

impl Default for NormalizeVector {
    fn default() -> Self {
        Self::new()
    }
}

impl NormalizeVector {
    /// Create a new NormalizeVector node.
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "normalize_vector".into(),
                outs: vec![Port::new("output", PortType::Str)],
                ins: vec![Slot::Wire(Port::new("vector", PortType::Str))],
            },
        }
    }
}

impl GkNode for NormalizeVector {
    fn meta(&self) -> &NodeMeta { &self.meta }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let s = inputs[0].as_str();
        let trimmed = s.trim();
        if !trimmed.starts_with('[') || !trimmed.ends_with(']') {
            outputs[0] = Value::Str(s.to_string());
            return;
        }
        let inner = &trimmed[1..trimmed.len()-1];
        let values: Vec<f64> = inner.split(',')
            .filter_map(|v| v.trim().parse::<f64>().ok())
            .collect();
        let norm = values.iter().map(|v| v * v).sum::<f64>().sqrt();
        if norm < 1e-15 {
            outputs[0] = Value::Str(s.to_string());
            return;
        }
        let normalized: Vec<String> = values.iter()
            .map(|v| format!("{}", v / norm))
            .collect();
        outputs[0] = Value::Str(format!("[{}]", normalized.join(",")));
    }
}

/// Generate a deterministic f64 vector as a bracket-encoded JSON array string.
///
/// Uses xxHash3 to derive pseudo-random values in `[min, max)` for each
/// dimension. The seed and dimension are provided at cycle time; `min`
/// and `max` are constants set at construction.
///
/// Signature: `random_vector(seed: u64, dim: u64) -> (output: Str)`
/// Consts: `min: f64 = 0.0`, `max: f64 = 1.0`
pub struct RandomVector {
    meta: NodeMeta,
    min: f64,
    max: f64,
}

impl RandomVector {
    /// Create a new RandomVector node with the given value range.
    pub fn new(min: f64, max: f64) -> Self {
        Self {
            meta: NodeMeta {
                name: "random_vector".into(),
                outs: vec![Port::new("output", PortType::Str)],
                ins: vec![
                    Slot::Wire(Port::u64("seed")),
                    Slot::Wire(Port::u64("dim")),
                ],
            },
            min,
            max,
        }
    }
}

impl GkNode for RandomVector {
    fn meta(&self) -> &NodeMeta { &self.meta }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let seed = inputs[0].as_u64();
        let dim = inputs[1].as_u64() as usize;
        let range = self.max - self.min;
        let mut h = seed;
        let mut values = Vec::with_capacity(dim);
        for _ in 0..dim {
            h = xxhash_rust::xxh3::xxh3_64(&h.to_le_bytes());
            let unit = (h as f64) / (u64::MAX as f64); // [0, 1)
            values.push(format!("{}", self.min + range * unit));
        }
        outputs[0] = Value::Str(format!("[{}]", values.join(",")));
    }
}

// =================================================================
// Array inspection: operate on bracket-encoded arrays like [1,2,3]
// =================================================================

/// Return the number of elements in a bracket-encoded array string.
///
/// Parses `[a,b,c,...]` and counts elements. Returns 0 for empty
/// arrays or non-array input.
pub struct ArrayLen {
    meta: NodeMeta,
}

impl ArrayLen {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "array_len".into(),
                outs: vec![Port::u64("output")],
                ins: vec![Slot::Wire(Port::new("input", PortType::Str))],
            },
        }
    }
}

impl GkNode for ArrayLen {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let s = inputs[0].as_str();
        let trimmed = s.trim();
        if trimmed == "[]" || trimmed.is_empty() {
            outputs[0] = Value::U64(0);
        } else if trimmed.starts_with('[') && trimmed.ends_with(']') {
            let inner = &trimmed[1..trimmed.len() - 1];
            outputs[0] = Value::U64(inner.split(',').count() as u64);
        } else {
            outputs[0] = Value::U64(0);
        }
    }
}

/// Return the element at a given index from a bracket-encoded array.
///
/// `array_at(array_str, index)` → string element at position.
/// Index wraps modulo array length. Returns "" for empty arrays.
pub struct ArrayAt {
    meta: NodeMeta,
}

impl ArrayAt {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "array_at".into(),
                outs: vec![Port::new("output", PortType::Str)],
                ins: vec![
                    Slot::Wire(Port::new("array", PortType::Str)),
                    Slot::Wire(Port::u64("index")),
                ],
            },
        }
    }
}

impl GkNode for ArrayAt {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let s = inputs[0].as_str();
        let idx = inputs[1].as_u64() as usize;
        let trimmed = s.trim();
        if trimmed.starts_with('[') && trimmed.ends_with(']') {
            let inner = &trimmed[1..trimmed.len() - 1];
            let elements: Vec<&str> = inner.split(',').map(|e| e.trim()).collect();
            if elements.is_empty() || (elements.len() == 1 && elements[0].is_empty()) {
                outputs[0] = Value::Str(String::new());
            } else {
                outputs[0] = Value::Str(elements[idx % elements.len()].to_string());
            }
        } else {
            outputs[0] = Value::Str(String::new());
        }
    }
}

/// Try to build a JSON node from a function name and const args.
///
/// Returns `None` if the name is not handled by this module.
pub(crate) fn build_node(name: &str, _wires: &[crate::assembly::WireRef], consts: &[crate::dsl::factory::ConstArg]) -> Option<Result<Box<dyn crate::node::GkNode>, String>> {
    match name {
        "to_json" => Some(Ok(Box::new(ToJson::new(crate::node::PortType::U64)))),
        "json_to_str" => Some(Ok(Box::new(JsonToStr::new()))),
        "escape_json" => Some(Ok(Box::new(EscapeJson::new()))),
        "json_merge" => Some(Ok(Box::new(JsonMerge::new()))),
        "array_len" => Some(Ok(Box::new(ArrayLen::new()))),
        "array_at" => Some(Ok(Box::new(ArrayAt::new()))),
        "normalize_vector" => Some(Ok(Box::new(NormalizeVector::new()))),
        "random_vector" => Some(Ok(Box::new(RandomVector::new(
            consts.first().map(|c| c.as_f64()).unwrap_or(0.0),
            consts.get(1).map(|c| c.as_f64()).unwrap_or(1.0),
        )))),
        _ => None,
    }
}


crate::register_nodes!(signatures, build_node);
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_object_basic() {
        let node = JsonObject::new(
            vec!["name".into(), "age".into(), "active".into()],
            vec![PortType::Str, PortType::U64, PortType::Bool],
        );
        let mut out = [Value::None];
        node.eval(
            &[Value::Str("Alice".into()), Value::U64(30), Value::Bool(true)],
            &mut out,
        );
        let j = out[0].as_json();
        assert_eq!(j["name"], "Alice");
        assert_eq!(j["age"], 30);
        assert_eq!(j["active"], true);
    }

    #[test]
    fn json_object_nested() {
        let inner = JsonObject::new(
            vec!["x".into(), "y".into()],
            vec![PortType::U64, PortType::U64],
        );
        let mut inner_out = [Value::None];
        inner.eval(&[Value::U64(10), Value::U64(20)], &mut inner_out);

        let outer = JsonObject::new(
            vec!["point".into()],
            vec![PortType::Json],
        );
        let mut out = [Value::None];
        outer.eval(&[inner_out[0].clone()], &mut out);
        let j = out[0].as_json();
        assert_eq!(j["point"]["x"], 10);
        assert_eq!(j["point"]["y"], 20);
    }

    #[test]
    fn json_array_basic() {
        let node = JsonArray::new(vec![PortType::U64, PortType::Str, PortType::F64]);
        let mut out = [Value::None];
        node.eval(
            &[Value::U64(1), Value::Str("two".into()), Value::F64(3.0)],
            &mut out,
        );
        let j = out[0].as_json();
        let arr = j.as_array().unwrap();
        assert_eq!(arr.len(), 3);
        assert_eq!(arr[0], 1);
        assert_eq!(arr[1], "two");
        assert_eq!(arr[2], 3.0);
    }

    #[test]
    fn json_to_str_compact() {
        let node = JsonToStr::new();
        let mut out = [Value::None];
        let input = Value::Json(json!({"a": 1, "b": "hello"}));
        node.eval(&[input], &mut out);
        let s = out[0].as_str();
        assert!(s.contains("\"a\":1") || s.contains("\"a\": 1"));
        assert!(s.contains("\"b\":\"hello\"") || s.contains("\"b\": \"hello\""));
    }

    #[test]
    fn str_to_json_roundtrip() {
        let to_str = JsonToStr::new();
        let from_str = StrToJson::new();
        let original = Value::Json(json!({"key": [1, 2, 3]}));
        let mut mid = [Value::None];
        let mut out = [Value::None];
        to_str.eval(&[original.clone()], &mut mid);
        from_str.eval(&[mid[0].clone()], &mut out);
        assert_eq!(out[0].as_json(), original.as_json());
    }

    #[test]
    fn escape_json_basic() {
        let node = EscapeJson::new();
        let mut out = [Value::None];
        node.eval(&[Value::Str("hello \"world\"\nline2".into())], &mut out);
        let s = out[0].as_str();
        assert!(s.contains("\\\""));
        assert!(s.contains("\\n"));
        assert!(!s.starts_with('"'));
    }

    #[test]
    fn json_merge_basic() {
        let node = JsonMerge::new();
        let mut out = [Value::None];
        let left = Value::Json(json!({"a": 1, "b": 2}));
        let right = Value::Json(json!({"b": 99, "c": 3}));
        node.eval(&[left, right], &mut out);
        let j = out[0].as_json();
        assert_eq!(j["a"], 1);
        assert_eq!(j["b"], 99); // right wins
        assert_eq!(j["c"], 3);
    }

    #[test]
    fn json_field_basic() {
        let node = JsonField::new("name");
        let mut out = [Value::None];
        node.eval(&[Value::Json(json!({"name": "Alice", "age": 30}))], &mut out);
        assert_eq!(out[0].as_json(), &json!("Alice"));
    }

    #[test]
    fn json_field_missing() {
        let node = JsonField::new("missing");
        let mut out = [Value::None];
        node.eval(&[Value::Json(json!({"name": "Alice"}))], &mut out);
        assert!(out[0].as_json().is_null());
    }

    #[test]
    fn to_json_from_u64() {
        let node = ToJson::new(PortType::U64);
        let mut out = [Value::None];
        node.eval(&[Value::U64(42)], &mut out);
        assert_eq!(out[0].as_json(), &json!(42));
    }

    #[test]
    fn json_pretty_print() {
        let node = JsonToStrPretty::new();
        let mut out = [Value::None];
        node.eval(&[Value::Json(json!({"a": 1}))], &mut out);
        let s = out[0].as_str();
        assert!(s.contains('\n'), "pretty print should have newlines");
    }
}
