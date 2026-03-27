// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! JSON construction, serialization, and manipulation nodes.
//!
//! JSON is a first-class `Value` type in the GK. Nodes can produce
//! and consume `Value::Json(serde_json::Value)` directly, avoiding
//! serialization/deserialization round-trips when passing structured
//! data between nodes or to adapters that consume JSON natively.

use crate::node::{GkNode, NodeMeta, Port, PortType, Value};
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
        Self {
            meta: NodeMeta {
                name: "json_object".into(),
                inputs,
                outputs: vec![Port::json("output")],
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
        Self {
            meta: NodeMeta {
                name: "json_array".into(),
                inputs,
                outputs: vec![Port::json("output")],
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
                inputs: vec![Port::new("input", input_type)],
                outputs: vec![Port::json("output")],
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

impl JsonMerge {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "json_merge".into(),
                inputs: vec![Port::json("left"), Port::json("right")],
                outputs: vec![Port::json("output")],
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

impl JsonToStr {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "__json_to_string".into(),
                inputs: vec![Port::json("input")],
                outputs: vec![Port::new("output", PortType::Str)],
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

impl JsonToStrPretty {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "json_to_str_pretty".into(),
                inputs: vec![Port::json("input")],
                outputs: vec![Port::new("output", PortType::Str)],
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

impl StrToJson {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "str_to_json".into(),
                inputs: vec![Port::new("input", PortType::Str)],
                outputs: vec![Port::json("output")],
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

impl EscapeJson {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "escape_json".into(),
                inputs: vec![Port::new("input", PortType::Str)],
                outputs: vec![Port::new("output", PortType::Str)],
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
                inputs: vec![Port::json("input")],
                outputs: vec![Port::json("output")],
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
        Value::None => serde_json::Value::Null,
    }
}

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
