// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! JSON construction, serialization, and manipulation nodes.
//!
//! JSON is a first-class `Value` type in the GK. Nodes can produce
//! and consume `Value::Json(std::sync::Arc::new(serde_json::Value))` directly, avoiding
//! serialization/deserialization round-trips when passing structured
//! data between nodes or to adapters that consume JSON natively.
//!
//! SRD-80b Phase E migration: every node in this module routes
//! through `#[polydat_node]`. `JsonObject`'s historical "interleaved
//! key/value pairs" gap (SRD-80b §"open question 2") is resolved
//! compositionally: `json_with(key, value)` produces a single-pair
//! partial Json, and the variadic `json_object(parts...)` merges
//! them. Workload syntax:
//!
//! ```text
//! record := json_object(
//!     json_with("name", name_wire),
//!     json_with("age", age_wire),
//! )
//! ```

use crate::ast::Value;
use serde_json::json;

// =================================================================
// Construction: build JSON values from inputs
// =================================================================

/// Build a single-pair partial JSON object: `{ key: value }`.
///
/// Signature: `(value: any) -> (json)` with const `key: &str`.
///
/// The compositional building block for `json_object`. Each
/// `json_with` produces a one-entry Json Object that the variadic
/// `json_object` merger flattens. The value is converted via
/// `value_to_json`, so any `Value` variant works as the input:
///   - U64 / F64 → JSON number
///   - Bool → JSON bool
///   - Str → JSON string
///   - Json → nested as-is
///
/// SRD-80b Phase E migration — resolves the historical "interleaved
/// key/value pairs" gap (open question 2) by splitting the operator:
/// const key paired with a single PolyWire value, no per-slot
/// type modelling needed.
#[crate::polydat_node(category = Json)]
fn json_with(
    key: crate::derive_support::Const<&str>,
    value: Value,
) -> std::sync::Arc<serde_json::Value> {
    let mut m = serde_json::Map::new();
    m.insert(key.0.to_string(), value_to_json(&value));
    std::sync::Arc::new(serde_json::Value::Object(m))
}

/// Merge N partial Json Objects into a single Json Object.
///
/// Signature: `(parts_0: json, parts_1: json, ...) -> (json)`
///
/// Variadic merger over Json inputs. Each input is expected to be a
/// Json Object (typically produced by `json_with`); non-Object
/// variants are silently skipped — keeping the operator
/// composable with conditional `if(...)` branches that may yield
/// `null`. Later parts shadow earlier on key collision.
///
/// SRD-80b Phase E migration — variadic `&[Value]`. Replaces the
/// hand-written `JsonObject` which couldn't express interleaved
/// const keys + per-slot wires in the workload DSL.
#[crate::polydat_node(category = Json, variadic_min = 0)]
fn json_object(parts: &[Value]) -> std::sync::Arc<serde_json::Value> {
    let mut merged = serde_json::Map::new();
    for v in parts {
        if let Value::Json(arc) = v
            && let serde_json::Value::Object(map) = arc.as_ref() {
                for (k, val) in map {
                    merged.insert(k.clone(), val.clone());
                }
            }
    }
    std::sync::Arc::new(serde_json::Value::Object(merged))
}

/// Build a JSON array from N inputs.
///
/// Signature: `(elem_0: any, elem_1: any, ...) -> (json)`
///
/// SRD-80b Phase E migration — `&[Value]` variadic. Per-element
/// type fidelity isn't surfaced at eval (the body walks the
/// elements through `value_to_json`, which dispatches on the
/// `Value` variant), so the macro-emitted variadic shape is
/// behaviourally equivalent to the hand-written per-slot-typed
/// version. The macro emits `JsonArray::new(n_wires)` for the
/// programmatic-construction signature.
#[crate::polydat_node(category = Json, variadic_min = 0)]
fn json_array(elems: &[Value]) -> std::sync::Arc<serde_json::Value> {
    let arr: Vec<serde_json::Value> = elems.iter().map(value_to_json).collect();
    std::sync::Arc::new(serde_json::Value::Array(arr))
}

/// Wrap a single value as a JSON value.
///
/// Signature: `(input: any) -> (json)`
///
/// Useful for promoting a scalar to JSON for further composition.
/// SRD-80b Phase E — PolyWire input, Json output. The macro's
/// SameAsInput dispatch isn't applicable here (return type is
/// Json, not Value), so the body coerces each variant through
/// `value_to_json`.
#[crate::polydat_node(category = Json)]
fn to_json(input: Value) -> std::sync::Arc<serde_json::Value> {
    std::sync::Arc::new(value_to_json(&input))
}

/// Merge two JSON objects into one (shallow merge, right wins).
///
/// Signature: `(left: json, right: json) -> (json)`
///
/// SRD-80b Phase E migration — `&serde_json::Value` inputs,
/// `Arc<serde_json::Value>` output.
#[crate::polydat_node(category = Json)]
fn json_merge(
    left: &serde_json::Value,
    right: &serde_json::Value,
) -> std::sync::Arc<serde_json::Value> {
    let mut result = left.clone();
    if let (serde_json::Value::Object(base), serde_json::Value::Object(overlay)) =
        (&mut result, right)
    {
        for (k, v) in overlay {
            base.insert(k.clone(), v.clone());
        }
    }
    std::sync::Arc::new(result)
}

// =================================================================
// Serialization: JSON ↔ String
// =================================================================

/// Serialize a JSON value to a compact string.
///
/// Signature: `json_to_str(input: json) -> (String)`
///
/// Workload-callable AND the auto-adapter the assembly phase inserts
/// on Json → Str boundaries. SRD-80b Phase E migration —
/// `&serde_json::Value` input. The struct name `JsonToStr`
/// follows the snake_case → PascalCase rule and is what
/// `compile::assembly` constructs for the auto-adapter slot.
#[crate::polydat_node(category = Conversions)]
fn json_to_str(input: &serde_json::Value) -> String {
    input.to_string()
}

/// Serialize a JSON value to a pretty-printed string.
/// SRD-80 PR B.11 migration — `&serde_json::Value` input.
#[crate::polydat_node(category = Json)]
fn json_to_str_pretty(input: &serde_json::Value) -> String {
    serde_json::to_string_pretty(input).unwrap_or_default()
}

/// Parse a JSON string into a JSON value.
/// SRD-80 PR B.11 migration — `Arc<serde_json::Value>` output.
#[crate::polydat_node(category = Json)]
fn str_to_json(input: &str) -> std::sync::Arc<serde_json::Value> {
    let parsed = serde_json::from_str(input).unwrap_or(serde_json::Value::Null);
    std::sync::Arc::new(parsed)
}

/// Escape a string for safe embedding in a JSON string value.
///
/// Signature: `(input: String) -> (String)`
///
/// Escape a string for embedding inside JSON. Escapes `"`,
/// `\`, control characters, etc. Does NOT add surrounding
/// quotes — the result is the interior of a JSON string.
///
/// SRD-80 PR B.4 migration.
#[crate::polydat_node(category = Json)]
fn escape_json(input: String) -> String {
    // serde_json::to_string adds quotes; strip them for interior-only.
    let json_str = serde_json::to_string(&input).unwrap_or_default();
    json_str[1..json_str.len() - 1].to_string()
}

// =================================================================
// Field access
// =================================================================

/// Extract a field from a JSON object by key.
///
/// Extract a single field by key from a JSON object input.
/// Returns the field's value (or `null` if missing).
/// SRD-80b Phase E migration via scalar Const + borrow Json input.
#[crate::polydat_node(category = Json)]
fn json_field(
    input: &serde_json::Value,
    key: crate::derive_support::Const<&str>,
) -> std::sync::Arc<serde_json::Value> {
    std::sync::Arc::new(
        input.get(*key).cloned().unwrap_or(serde_json::Value::Null)
    )
}

// =================================================================
// Helpers
// =================================================================

fn value_to_json(v: &Value) -> serde_json::Value {
    match v {
        // Bytes uses base64 here (JSON-payload convention) instead of
        // the hex form `Value::to_json_value` returns, so the Bytes
        // arm stays local. Everything else delegates to the
        // canonical typed-to-JSON projection on Value itself —
        // adding a new Value variant doesn't require a parallel
        // arm here anymore.
        Value::Bytes(b) => {
            use base64::Engine;
            json!(base64::engine::general_purpose::STANDARD.encode(b))
        }
        other => other.to_json_value(),
    }
}

/// Flatten a JSON tree into a single newline-separated text by
/// concatenating every leaf value's textual form.
///
/// Walks the tree depth-first; for each leaf:
///   - Strings emit their text verbatim (newlines inside the
///     string survive — important when the JSON carries
///     multi-line content like CQL `create_statement`).
///   - Numbers / booleans emit their natural string form.
///   - Nulls are skipped.
/// Successive leaves are joined with `\n`.
///
/// Use case: probe-phase regex matches over a multi-row body.
/// `regex_match(json_text(body), "(?im)^TABLE …")` lets the
/// regex see the actual newlines inside `create_statement`-shape
/// columns; the previous `regex_match(exactly_one_value(body), …)`
/// shape silently degrades when the body isn't unary AND when
/// the upstream wire is forced through a `JsonToStr` adapter
/// that escapes newlines as `\n` literals.
///
/// SRD-80b Phase E migration — PolyWire input so non-Json values
/// fall through their display form (a Str input is already
/// textual; numeric/bool scalars render naturally), useful when
/// the upstream wire is heterogeneous (e.g. a body extern that
/// sometimes carries a string, sometimes a JSON value).
#[crate::polydat_node(category = Json)]
fn json_text(input: Value) -> String {
    match &input {
        Value::Json(j) => {
            let mut buf = String::new();
            walk_json_leaves(j, &mut buf);
            buf
        }
        other => other.to_display_string(),
    }
}

fn walk_json_leaves(j: &serde_json::Value, out: &mut String) {
    use serde_json::Value as J;
    match j {
        J::String(s) => {
            if !out.is_empty() { out.push('\n'); }
            out.push_str(s);
        }
        J::Number(n) => {
            if !out.is_empty() { out.push('\n'); }
            out.push_str(&n.to_string());
        }
        J::Bool(b) => {
            if !out.is_empty() { out.push('\n'); }
            out.push_str(if *b { "true" } else { "false" });
        }
        J::Null => {}
        J::Array(arr) => {
            for item in arr { walk_json_leaves(item, out); }
        }
        J::Object(obj) => {
            for value in obj.values() { walk_json_leaves(value, out); }
        }
    }
}

/// `body_column_i32(body, "name")` — extract the named column from
/// every row of a JSON result body, parse each as i32, and return
/// the values as a `VecI32` wire.
///
/// This is the canonical capture path for tabular result data into
/// typed-vector wires. Adapter result bodies that already serialize
/// to `[{ "key": 1, ... }, { "key": 2, ... }, ...]`-shaped JSON can
/// expose per-column wires for downstream readers (the recall
/// evaluator, custom metrics, etc.) without forcing string
/// round-trips through the metric reader.
///
/// Robust extraction rules:
/// - Body shape `[{...}, {...}, ...]`: walks each row, looks up the
///   column by name, parses as i32 via `json_value_as_i32`.
/// - Body shape `{ "rows": [...] }`: walks `rows`; same per-row
///   extraction as above. Matches common envelope formats (Jolokia,
///   HTTP wrappers).
/// - Body shape `{ "key": value, ... }` (single row at top level):
///   produces a single-element vector.
/// - Non-JSON input: empty vector. This preserves the "no values
///   extracted" diagnostic at the evaluator instead of panicking
///   here.
///
/// Rows whose column is absent / null / unparseable contribute
/// nothing (no zero-fill, no error). Mirrors the legacy host-side
/// `extract_indices_from_json` JSON-walk so workloads can swap to
/// this typed-wire path without recall-value drift.
///
/// SRD-80b Phase E migration — PolyWire body so the non-Json
/// fallback ("empty vector, no panic") stays intact; `column`
/// is a const string with no default (required workload arg).
#[crate::polydat_node(category = Json)]
fn body_column_i32(
    body: Value,
    column: crate::derive_support::Const<&str>,
) -> Vec<i32> {
    let json = match &body {
        Value::Json(j) => j,
        _ => return Vec::new(),
    };
    extract_column_i32(json, column.0)
}

/// Walk a JSON value extracting `column` from every row. Handles
/// top-level array, `{ rows: [...] }` envelope, and bare object
/// forms — the same shapes adapter `ResultBody::to_json()` produces
/// across CQL / HTTP / stdout drivers.
fn extract_column_i32(json: &serde_json::Value, column: &str) -> Vec<i32> {
    match json {
        serde_json::Value::Array(rows) => {
            rows.iter()
                .filter_map(|row| json_value_as_i32(row.get(column)?))
                .collect()
        }
        serde_json::Value::Object(obj) => {
            // Two shapes can land here: an envelope object with a
            // `rows` array (preferred), or a single row object
            // whose column we extract as a one-element vector.
            // Try envelope first to match the common
            // `{rows: [...]}` shape adapters use.
            if let Some(serde_json::Value::Array(rows)) = obj.get("rows") {
                return rows.iter()
                    .filter_map(|row| json_value_as_i32(row.get(column)?))
                    .collect();
            }
            obj.get(column)
                .and_then(json_value_as_i32)
                .map(|n| vec![n])
                .unwrap_or_default()
        }
        _ => Vec::new(),
    }
}

/// Parse a JSON value as i32 with the same tolerance the legacy
/// `json_field_as_i64` extractor used: number→cast, string→parse,
/// bool/null/object/array→skip. Out-of-range numerics saturate to
/// the closest i32 boundary; preserves the legacy "best-effort"
/// behaviour rather than silently dropping rows.
fn json_value_as_i32(v: &serde_json::Value) -> Option<i32> {
    match v {
        serde_json::Value::Number(n) => {
            n.as_i64().map(|i| i.clamp(i32::MIN as i64, i32::MAX as i64) as i32)
                .or_else(|| n.as_u64().map(|u| u.min(i32::MAX as u64) as i32))
                .or_else(|| n.as_f64().and_then(|f| {
                    if f.is_finite() { Some(f as i32) } else { None }
                }))
        }
        serde_json::Value::String(s) => s.trim().parse::<i32>().ok(),
        _ => None,
    }
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
///
/// SRD-80b Phase E migration.
#[crate::polydat_node(category = Json)]
fn normalize_vector(vector: &str) -> String {
    let trimmed = vector.trim();
    if !trimmed.starts_with('[') || !trimmed.ends_with(']') {
        return vector.to_string();
    }
    let inner = &trimmed[1..trimmed.len()-1];
    let values: Vec<f64> = inner.split(',')
        .filter_map(|v| v.trim().parse::<f64>().ok())
        .collect();
    let norm = values.iter().map(|v| v * v).sum::<f64>().sqrt();
    if norm < 1e-15 {
        return vector.to_string();
    }
    let normalized: Vec<String> = values.iter()
        .map(|v| format!("{}", v / norm))
        .collect();
    format!("[{}]", normalized.join(","))
}

/// Generate a deterministic f64 vector as a bracket-encoded JSON array string.
///
/// Uses xxHash3 to derive pseudo-random values in `[min, max)` for each
/// dimension. The seed and dimension are provided at cycle time; `min`
/// and `max` are constants set at construction.
///
/// Signature: `random_vector(seed: u64, dim: u64) -> (output: Str)`
/// Consts: `min: f64 = 0.0`, `max: f64 = 1.0`
///
/// SRD-80b Phase E migration.
#[crate::polydat_node(category = Json)]
fn random_vector(
    seed: u64,
    dim: u64,
    #[poly_default(0.0f64)] min: crate::derive_support::Const<f64>,
    #[poly_default(1.0f64)] max: crate::derive_support::Const<f64>,
) -> String {
    let min_v = *min;
    let max_v = *max;
    let range = max_v - min_v;
    let dim = dim as usize;
    let mut h = seed;
    let mut values = Vec::with_capacity(dim);
    for _ in 0..dim {
        h = xxhash_rust::xxh3::xxh3_64(&h.to_le_bytes());
        let unit = (h as f64) / (u64::MAX as f64); // [0, 1)
        values.push(format!("{}", min_v + range * unit));
    }
    format!("[{}]", values.join(","))
}

// =================================================================
// Array inspection: operate on bracket-encoded arrays like [1,2,3]
// =================================================================

/// Return the number of elements in a bracket-encoded array string.
///
/// Parses `[a,b,c,...]` and counts elements. Returns 0 for empty
/// arrays or non-array input.
///
/// SRD-80b Phase E migration.
#[crate::polydat_node(category = Json)]
fn array_len(input: &str) -> u64 {
    let trimmed = input.trim();
    if trimmed == "[]" || trimmed.is_empty() {
        0
    } else if trimmed.starts_with('[') && trimmed.ends_with(']') {
        let inner = &trimmed[1..trimmed.len() - 1];
        inner.split(',').count() as u64
    } else {
        0
    }
}

/// Return the element at a given index from a bracket-encoded array.
///
/// `array_at(array_str, index)` → string element at position.
/// Index wraps modulo array length. Returns "" for empty arrays.
///
/// SRD-80b Phase E migration.
#[crate::polydat_node(category = Json)]
fn array_at(array: &str, index: u64) -> String {
    let trimmed = array.trim();
    if trimmed.starts_with('[') && trimmed.ends_with(']') {
        let inner = &trimmed[1..trimmed.len() - 1];
        let elements: Vec<&str> = inner.split(',').map(|e| e.trim()).collect();
        if elements.is_empty() || (elements.len() == 1 && elements[0].is_empty()) {
            String::new()
        } else {
            elements[(index as usize) % elements.len()].to_string()
        }
    } else {
        String::new()
    }
}

// SRD-80b Phase E: `signatures()` / `build_node()` / `register_nodes!`
// retired — every node above registers via the proc-macro's
// `NodeRegistration` inventory submission. `JsonObject` is now
// macro-emitted variadic; the historical hand-written struct is
// gone.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{PolydatNode, PortType, Value};

    #[test]
    fn body_column_i32_extracts_array_of_rows() {
        // Standard CQL SELECT shape: top-level array of row objects.
        let body = Value::Json(std::sync::Arc::new(serde_json::json!([
            { "key": 4, "value": 0.5 },
            { "key": 17, "value": 0.4 },
            { "key": 42, "value": 0.3 },
        ])));
        let node = BodyColumnI32::new(PortType::Json, "key".to_string());
        let mut out = [Value::None];
        node.eval(&[body], &mut out);
        let Value::VecI32(slice) = &out[0] else { panic!("expected VecI32, got {:?}", out[0]) };
        assert_eq!(slice.as_slice(), &[4, 17, 42]);
    }

    #[test]
    fn body_column_i32_extracts_envelope_rows() {
        // Envelope shape: { "rows": [...] }.
        let body = Value::Json(std::sync::Arc::new(serde_json::json!({
            "rows": [
                { "id": 1 },
                { "id": 7 },
                { "id": 13 },
            ],
            "metadata": "ignored",
        })));
        let node = BodyColumnI32::new(PortType::Json, "id".to_string());
        let mut out = [Value::None];
        node.eval(&[body], &mut out);
        let Value::VecI32(slice) = &out[0] else { panic!("expected VecI32") };
        assert_eq!(slice.as_slice(), &[1, 7, 13]);
    }

    #[test]
    fn body_column_i32_skips_rows_missing_column() {
        // Robustness: rows without the column don't zero-fill.
        let body = Value::Json(std::sync::Arc::new(serde_json::json!([
            { "key": 1 },
            { "other": 2 },     // skipped
            { "key": "not_an_int" },  // skipped
            { "key": 3 },
        ])));
        let node = BodyColumnI32::new(PortType::Json, "key".to_string());
        let mut out = [Value::None];
        node.eval(&[body], &mut out);
        let Value::VecI32(slice) = &out[0] else { panic!("expected VecI32") };
        assert_eq!(slice.as_slice(), &[1, 3]);
    }

    #[test]
    fn body_column_i32_string_numeric_parses() {
        // Stringified numbers parse — common for some adapters
        // that don't preserve native numeric typing.
        let body = Value::Json(std::sync::Arc::new(serde_json::json!([
            { "key": "42" },
            { "key": "-7" },
        ])));
        let node = BodyColumnI32::new(PortType::Json, "key".to_string());
        let mut out = [Value::None];
        node.eval(&[body], &mut out);
        let Value::VecI32(slice) = &out[0] else { panic!("expected VecI32") };
        assert_eq!(slice.as_slice(), &[42, -7]);
    }

    #[test]
    fn body_column_i32_empty_body_produces_empty_vec() {
        let body = Value::Json(std::sync::Arc::new(serde_json::json!([])));
        let node = BodyColumnI32::new(PortType::Json, "key".to_string());
        let mut out = [Value::None];
        node.eval(&[body], &mut out);
        let Value::VecI32(slice) = &out[0] else { panic!("expected VecI32") };
        assert!(slice.as_slice().is_empty());
    }

    #[test]
    fn body_column_i32_non_json_input_produces_empty_vec() {
        // Defensive: non-JSON input shouldn't panic; the node's
        // type contract still holds. The evaluator surfaces a
        // "no values extracted" diagnostic downstream.
        let node = BodyColumnI32::new(PortType::Json, "key".to_string());
        let mut out = [Value::None];
        node.eval(&[Value::Str("not json".into())], &mut out);
        let Value::VecI32(slice) = &out[0] else { panic!("expected VecI32") };
        assert!(slice.as_slice().is_empty());
    }

    /// `json_text` flattens a multi-row describe-keyspace body
    /// to newline-joined leaves. The actual newlines INSIDE
    /// `create_statement` strings survive verbatim, so a
    /// line-anchored regex can match the table-declaration
    /// line. This is the workload-of-record probe shape:
    /// `regex_match(json_text(body), "(?im)^…TABLE foo\(…")`.
    #[test]
    fn json_text_flattens_multirow_describe_for_regex() {
        let body = Value::Json(std::sync::Arc::new(serde_json::json!([
            {
                "keyspace_name": "system_views",
                "type": "table",
                "name": "sai_column_indexes",
                "create_statement": "CREATE TABLE system_views.sai_column_indexes (\n    keyspace_name text,\n    table_name text\n);"
            },
            {
                "keyspace_name": "system_views",
                "type": "table",
                "name": "indexes",
                "create_statement": "CREATE VIRTUAL TABLE system_views.indexes (\n    keyspace_name text\n);"
            },
        ])));

        let node = JsonText::new(PortType::Json);
        let mut out = [Value::None];
        node.eval(&[body], &mut out);

        let text = match &out[0] {
            Value::Str(s) => s.clone(),
            other => panic!("expected Str, got {other:?}"),
        };

        // The actual schema-text newlines are intact (not the
        // `\n` literal escape sequences a JSON-stringification
        // would produce).
        assert!(text.contains("CREATE TABLE system_views.sai_column_indexes (\n"));
        assert!(text.contains("CREATE VIRTUAL TABLE system_views.indexes (\n"));

        // The workload's intended regex (with the CREATE-prefix
        // fix) matches the flattened text.
        let pat = regex::Regex::new(
            r"(?im)^\s*(?:CREATE\s+)?(?:VIRTUAL\s+)?TABLE\s+system_views\.sai_column_indexes\s*\("
        ).unwrap();
        assert!(pat.is_match(&text), "regex should match the flattened schema text");
    }

    /// Helper: drive `json_with(key, value)` through one eval and
    /// return the resulting `Value::Json` partial-object. The
    /// `value_pt` is the wire-type the test wants attached to the
    /// PolyWire input port (`json_with` accepts any Value variant).
    fn jw(key: &str, value_pt: PortType, value: Value) -> Value {
        let node = JsonWith::new(key.to_string(), value_pt);
        let mut out = [Value::None];
        node.eval(&[value], &mut out);
        std::mem::replace(&mut out[0], Value::None)
    }

    #[test]
    fn json_object_basic() {
        // Compositional form: three json_with parts merged.
        let name = jw("name", PortType::Str, Value::Str("Alice".into()));
        let age = jw("age", PortType::U64, Value::U64(30));
        let active = jw("active", PortType::Bool, Value::Bool(true));

        let node = JsonObject::new(3);
        let mut out = [Value::None];
        node.eval(&[name, age, active], &mut out);
        let j = out[0].as_json();
        assert_eq!(j["name"], "Alice");
        assert_eq!(j["age"], 30);
        assert_eq!(j["active"], true);
    }

    #[test]
    fn json_object_nested() {
        // Inner object: { x: 10, y: 20 }.
        let inner_x = jw("x", PortType::U64, Value::U64(10));
        let inner_y = jw("y", PortType::U64, Value::U64(20));
        let inner_node = JsonObject::new(2);
        let mut inner_out = [Value::None];
        inner_node.eval(&[inner_x, inner_y], &mut inner_out);

        // Outer: { point: <inner> }.
        let point = jw(
            "point",
            PortType::Json,
            std::mem::replace(&mut inner_out[0], Value::None),
        );
        let outer = JsonObject::new(1);
        let mut out = [Value::None];
        outer.eval(&[point], &mut out);
        let j = out[0].as_json();
        assert_eq!(j["point"]["x"], 10);
        assert_eq!(j["point"]["y"], 20);
    }

    #[test]
    fn json_object_later_part_wins_on_collision() {
        // Documents the merge-order semantic: later json_with shadows
        // earlier, mirroring how json_merge treats right-wins.
        let first = jw("k", PortType::U64, Value::U64(1));
        let second = jw("k", PortType::U64, Value::U64(2));
        let node = JsonObject::new(2);
        let mut out = [Value::None];
        node.eval(&[first, second], &mut out);
        assert_eq!(out[0].as_json()["k"], 2);
    }

    #[test]
    fn json_object_skips_non_json_parts() {
        // Composability with conditional branches that may yield
        // non-Json (e.g. `if(cond, json_with(...), null)`).
        let valid = jw("k", PortType::U64, Value::U64(1));
        let node = JsonObject::new(2);
        let mut out = [Value::None];
        node.eval(&[valid, Value::None], &mut out);
        let j = out[0].as_json();
        assert_eq!(j["k"], 1);
        assert_eq!(j.as_object().unwrap().len(), 1);
    }

    #[test]
    fn json_array_basic() {
        let node = JsonArray::new(3);
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
        let input = Value::Json(std::sync::Arc::new(json!({"a": 1, "b": "hello"})));
        node.eval(&[input], &mut out);
        let s = out[0].as_str();
        assert!(s.contains("\"a\":1") || s.contains("\"a\": 1"));
        assert!(s.contains("\"b\":\"hello\"") || s.contains("\"b\": \"hello\""));
    }

    #[test]
    fn str_to_json_roundtrip() {
        let to_str = JsonToStr::new();
        let from_str = StrToJson::default();
        let original = Value::Json(std::sync::Arc::new(json!({"key": [1, 2, 3]})));
        let mut mid = [Value::None];
        let mut out = [Value::None];
        to_str.eval(std::slice::from_ref(&original), &mut mid);
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
        let left = Value::Json(std::sync::Arc::new(json!({"a": 1, "b": 2})));
        let right = Value::Json(std::sync::Arc::new(json!({"b": 99, "c": 3})));
        node.eval(&[left, right], &mut out);
        let j = out[0].as_json();
        assert_eq!(j["a"], 1);
        assert_eq!(j["b"], 99); // right wins
        assert_eq!(j["c"], 3);
    }

    #[test]
    fn json_field_basic() {
        let node = JsonField::new("name".to_string());
        let mut out = [Value::None];
        node.eval(&[Value::Json(std::sync::Arc::new(json!({"name": "Alice", "age": 30})))], &mut out);
        assert_eq!(out[0].as_json(), &json!("Alice"));
    }

    #[test]
    fn json_field_missing() {
        let node = JsonField::new("missing".to_string());
        let mut out = [Value::None];
        node.eval(&[Value::Json(std::sync::Arc::new(json!({"name": "Alice"})))], &mut out);
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
        let node = JsonToStrPretty::default();
        let mut out = [Value::None];
        node.eval(&[Value::Json(std::sync::Arc::new(json!({"a": 1})))], &mut out);
        let s = out[0].as_str();
        assert!(s.contains('\n'), "pretty print should have newlines");
    }
}
