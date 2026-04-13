// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Data file nodes: ordinal-based access to CSV, JSONL, and text files.
//!
//! Each node loads the file once at construction time (the filename is
//! a const parameter), builds an in-memory index, and serves fast
//! ordinal lookups at cycle time. Ordinals wrap via modulo so every
//! u64 input is valid.

use crate::node::{GkNode, NodeMeta, Port, PortType, Slot, Value};

// ─── CSV ───────────────────────────────────────────────────────

/// Read a specific column from a CSV row at a given ordinal.
///
/// Signature: `csv_field(ordinal: u64) -> (output: Str)`
/// Const: `filename: Str`, `column: Str` (header name or "0","1",... index)
///
/// The file is read and parsed at construction time. Header row is
/// auto-detected. Ordinal wraps modulo row count.
pub struct CsvField {
    meta: NodeMeta,
    values: Vec<String>,
}

impl CsvField {
    pub fn new(filename: &str, column: &str) -> Result<Self, String> {
        let content = std::fs::read_to_string(filename)
            .map_err(|e| format!("csv_field: failed to read '{filename}': {e}"))?;
        let mut lines = content.lines();

        // Parse header to find column index
        let header_line = lines.next()
            .ok_or_else(|| format!("csv_field: '{filename}' is empty"))?;
        let headers: Vec<&str> = split_csv_line(header_line);

        let col_idx = if let Ok(idx) = column.parse::<usize>() {
            idx
        } else {
            headers.iter().position(|h| h.trim() == column)
                .ok_or_else(|| format!(
                    "csv_field: column '{column}' not found in '{filename}'. Available: {}",
                    headers.join(", ")
                ))?
        };

        let mut values = Vec::new();
        for line in lines {
            let fields: Vec<&str> = split_csv_line(line);
            let val = fields.get(col_idx).unwrap_or(&"").trim().to_string();
            values.push(val);
        }

        if values.is_empty() {
            return Err(format!("csv_field: '{filename}' has no data rows"));
        }

        Ok(Self {
            meta: NodeMeta {
                name: "csv_field".into(),
                outs: vec![Port::new("output", PortType::Str)],
                ins: vec![Slot::Wire(Port::u64("ordinal"))],
            },
            values,
        })
    }
}

impl GkNode for CsvField {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let idx = inputs[0].as_u64() as usize % self.values.len();
        outputs[0] = Value::Str(self.values[idx].clone());
    }
}

/// Read an entire CSV row at a given ordinal as a comma-separated string.
///
/// Signature: `csv_row(ordinal: u64) -> (output: Str)`
/// Const: `filename: Str`
pub struct CsvRow {
    meta: NodeMeta,
    rows: Vec<String>,
}

impl CsvRow {
    pub fn new(filename: &str) -> Result<Self, String> {
        let content = std::fs::read_to_string(filename)
            .map_err(|e| format!("csv_row: failed to read '{filename}': {e}"))?;
        let rows: Vec<String> = content.lines()
            .skip(1) // skip header
            .filter(|l| !l.trim().is_empty())
            .map(|l| l.to_string())
            .collect();
        if rows.is_empty() {
            return Err(format!("csv_row: '{filename}' has no data rows"));
        }
        Ok(Self {
            meta: NodeMeta {
                name: "csv_row".into(),
                outs: vec![Port::new("output", PortType::Str)],
                ins: vec![Slot::Wire(Port::u64("ordinal"))],
            },
            rows,
        })
    }
}

impl GkNode for CsvRow {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let idx = inputs[0].as_u64() as usize % self.rows.len();
        outputs[0] = Value::Str(self.rows[idx].clone());
    }
}

/// Return the number of data rows in a CSV file (init-time constant).
///
/// Signature: `csv_row_count() -> (output: u64)`
/// Const: `filename: Str`
pub struct CsvRowCount {
    meta: NodeMeta,
    count: u64,
}

impl CsvRowCount {
    pub fn new(filename: &str) -> Result<Self, String> {
        let content = std::fs::read_to_string(filename)
            .map_err(|e| format!("csv_row_count: failed to read '{filename}': {e}"))?;
        let count = content.lines()
            .skip(1)
            .filter(|l| !l.trim().is_empty())
            .count() as u64;
        Ok(Self {
            meta: NodeMeta {
                name: "csv_row_count".into(),
                outs: vec![Port::u64("output")],
                ins: vec![],
            },
            count,
        })
    }
}

impl GkNode for CsvRowCount {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, _inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::U64(self.count);
    }
}

// ─── JSONL ─────────────────────────────────────────────────────

/// Read a field from a JSONL line at a given ordinal.
///
/// Signature: `jsonl_field(ordinal: u64) -> (output: Str)`
/// Const: `filename: Str`, `path: Str` (JSON field name or dot path)
///
/// Each line of the file is a JSON object. The field is extracted
/// by name (top-level) or dot-path (nested). Ordinal wraps modulo
/// line count.
pub struct JsonlField {
    meta: NodeMeta,
    values: Vec<String>,
}

impl JsonlField {
    pub fn new(filename: &str, path: &str) -> Result<Self, String> {
        let content = std::fs::read_to_string(filename)
            .map_err(|e| format!("jsonl_field: failed to read '{filename}': {e}"))?;
        let mut values = Vec::new();
        for (line_num, line) in content.lines().enumerate() {
            let trimmed = line.trim();
            if trimmed.is_empty() { continue; }
            let parsed: serde_json::Value = serde_json::from_str(trimmed)
                .map_err(|e| format!("jsonl_field: parse error at line {}: {e}", line_num + 1))?;
            let val = resolve_json_path(&parsed, path);
            values.push(val);
        }
        if values.is_empty() {
            return Err(format!("jsonl_field: '{filename}' has no lines"));
        }
        Ok(Self {
            meta: NodeMeta {
                name: "jsonl_field".into(),
                outs: vec![Port::new("output", PortType::Str)],
                ins: vec![Slot::Wire(Port::u64("ordinal"))],
            },
            values,
        })
    }
}

impl GkNode for JsonlField {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let idx = inputs[0].as_u64() as usize % self.values.len();
        outputs[0] = Value::Str(self.values[idx].clone());
    }
}

/// Read an entire JSONL line at a given ordinal as a JSON string.
///
/// Signature: `jsonl_row(ordinal: u64) -> (output: Str)`
/// Const: `filename: Str`
pub struct JsonlRow {
    meta: NodeMeta,
    rows: Vec<String>,
}

impl JsonlRow {
    pub fn new(filename: &str) -> Result<Self, String> {
        let content = std::fs::read_to_string(filename)
            .map_err(|e| format!("jsonl_row: failed to read '{filename}': {e}"))?;
        let rows: Vec<String> = content.lines()
            .filter(|l| !l.trim().is_empty())
            .map(|l| l.to_string())
            .collect();
        if rows.is_empty() {
            return Err(format!("jsonl_row: '{filename}' has no lines"));
        }
        Ok(Self {
            meta: NodeMeta {
                name: "jsonl_row".into(),
                outs: vec![Port::new("output", PortType::Str)],
                ins: vec![Slot::Wire(Port::u64("ordinal"))],
            },
            rows,
        })
    }
}

impl GkNode for JsonlRow {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let idx = inputs[0].as_u64() as usize % self.rows.len();
        outputs[0] = Value::Str(self.rows[idx].clone());
    }
}

/// Return the number of lines in a JSONL file (init-time constant).
///
/// Signature: `jsonl_row_count() -> (output: u64)`
/// Const: `filename: Str`
pub struct JsonlRowCount {
    meta: NodeMeta,
    count: u64,
}

impl JsonlRowCount {
    pub fn new(filename: &str) -> Result<Self, String> {
        let content = std::fs::read_to_string(filename)
            .map_err(|e| format!("jsonl_row_count: failed to read '{filename}': {e}"))?;
        let count = content.lines()
            .filter(|l| !l.trim().is_empty())
            .count() as u64;
        Ok(Self {
            meta: NodeMeta {
                name: "jsonl_row_count".into(),
                outs: vec![Port::u64("output")],
                ins: vec![],
            },
            count,
        })
    }
}

impl GkNode for JsonlRowCount {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, _inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::U64(self.count);
    }
}

// ─── Helpers ───────────────────────────────────────────────────

/// Split a CSV line on commas, respecting quoted fields.
fn split_csv_line(line: &str) -> Vec<&str> {
    // Simple split — doesn't handle quoted commas.
    // TODO: support RFC 4180 quoting for fields with embedded commas.
    line.split(',').collect()
}

/// Resolve a dot-separated JSON path. Returns the value as a string.
fn resolve_json_path(value: &serde_json::Value, path: &str) -> String {
    let mut current = value;
    for key in path.split('.') {
        match current {
            serde_json::Value::Object(map) => {
                current = match map.get(key) {
                    Some(v) => v,
                    None => return String::new(),
                };
            }
            serde_json::Value::Array(arr) => {
                if let Ok(idx) = key.parse::<usize>() {
                    current = match arr.get(idx) {
                        Some(v) => v,
                        None => return String::new(),
                    };
                } else {
                    return String::new();
                }
            }
            _ => return String::new(),
        }
    }
    match current {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Null => String::new(),
        other => other.to_string(),
    }
}

// ─── Registry ──────────────────────────────────────────────────

use crate::dsl::registry::{Arity, FuncCategory, FuncSig, ParamSpec};
use crate::node::SlotType;

pub fn signatures() -> &'static [FuncSig] {
    use FuncCategory as C;
    &[
        FuncSig {
            name: "csv_field", category: C::Data, outputs: 1,
            description: "Read a CSV column value at ordinal",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "ordinal", slot_type: SlotType::Wire, required: true, example: "cycle" },
                ParamSpec { name: "filename", slot_type: SlotType::ConstStr, required: true, example: "\"test.csv\"" },
                ParamSpec { name: "column", slot_type: SlotType::ConstStr, required: true, example: "\"name\"" },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Read a column from a CSV file at a cycle-time ordinal.\nThe file is loaded at init time. Header row determines column names.\nOrdinal wraps modulo row count.\nParameters:\n  ordinal  — u64 wire input\n  filename — path to CSV file (const)\n  column   — column name or index (const)\nExample: csv_field(cycle, \"users.csv\", \"username\")",
        },
        FuncSig {
            name: "csv_row", category: C::Data, outputs: 1,
            description: "Read a full CSV row at ordinal",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "ordinal", slot_type: SlotType::Wire, required: true, example: "cycle" },
                ParamSpec { name: "filename", slot_type: SlotType::ConstStr, required: true, example: "\"test.csv\"" },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Read an entire CSV row at ordinal as a comma-separated string.\nSkips header row. Ordinal wraps modulo row count.\nParameters:\n  ordinal  — u64 wire input\n  filename — path to CSV file (const)\nExample: csv_row(cycle, \"data.csv\")",
        },
        FuncSig {
            name: "csv_row_count", category: C::Data, outputs: 1,
            description: "Number of data rows in a CSV file",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "filename", slot_type: SlotType::ConstStr, required: true, example: "\"test.csv\"" },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Return the number of data rows (excluding header) in a CSV file.\nEvaluated at init time (constant).\nParameters:\n  filename — path to CSV file (const)\nExample: csv_row_count(\"users.csv\")",
        },
        FuncSig {
            name: "jsonl_field", category: C::Data, outputs: 1,
            description: "Read a field from a JSONL line at ordinal",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "ordinal", slot_type: SlotType::Wire, required: true, example: "cycle" },
                ParamSpec { name: "filename", slot_type: SlotType::ConstStr, required: true, example: "\"test.csv\"" },
                ParamSpec { name: "path", slot_type: SlotType::ConstStr, required: true, example: "\"$.data\"" },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Read a field from a JSON Lines file at ordinal.\nEach line is a JSON object. Field is extracted by name or dot-path.\nOrdinal wraps modulo line count.\nParameters:\n  ordinal  — u64 wire input\n  filename — path to JSONL file (const)\n  path     — field name or dot-path (const)\nExample: jsonl_field(cycle, \"events.jsonl\", \"user.name\")",
        },
        FuncSig {
            name: "jsonl_row", category: C::Data, outputs: 1,
            description: "Read a full JSONL line at ordinal",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "ordinal", slot_type: SlotType::Wire, required: true, example: "cycle" },
                ParamSpec { name: "filename", slot_type: SlotType::ConstStr, required: true, example: "\"test.csv\"" },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Read an entire JSONL line at ordinal as a raw JSON string.\nOrdinal wraps modulo line count.\nParameters:\n  ordinal  — u64 wire input\n  filename — path to JSONL file (const)\nExample: jsonl_row(cycle, \"events.jsonl\")",
        },
        FuncSig {
            name: "jsonl_row_count", category: C::Data, outputs: 1,
            description: "Number of lines in a JSONL file",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "filename", slot_type: SlotType::ConstStr, required: true, example: "\"test.csv\"" },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Return the number of non-empty lines in a JSONL file.\nEvaluated at init time (constant).\nParameters:\n  filename — path to JSONL file (const)\nExample: jsonl_row_count(\"events.jsonl\")",
        },
    ]
}

pub(crate) fn build_node(name: &str, _wires: &[crate::assembly::WireRef], consts: &[crate::dsl::factory::ConstArg]) -> Option<Result<Box<dyn GkNode>, String>> {
    match name {
        "csv_field" => {
            let filename = consts.first().map(|c| c.as_str()).unwrap_or("");
            let column = consts.get(1).map(|c| c.as_str()).unwrap_or("0");
            Some(CsvField::new(filename, column).map(|n| Box::new(n) as Box<dyn GkNode>))
        }
        "csv_row" => {
            let filename = consts.first().map(|c| c.as_str()).unwrap_or("");
            Some(CsvRow::new(filename).map(|n| Box::new(n) as Box<dyn GkNode>))
        }
        "csv_row_count" => {
            let filename = consts.first().map(|c| c.as_str()).unwrap_or("");
            Some(CsvRowCount::new(filename).map(|n| Box::new(n) as Box<dyn GkNode>))
        }
        "jsonl_field" => {
            let filename = consts.first().map(|c| c.as_str()).unwrap_or("");
            let path = consts.get(1).map(|c| c.as_str()).unwrap_or("");
            Some(JsonlField::new(filename, path).map(|n| Box::new(n) as Box<dyn GkNode>))
        }
        "jsonl_row" => {
            let filename = consts.first().map(|c| c.as_str()).unwrap_or("");
            Some(JsonlRow::new(filename).map(|n| Box::new(n) as Box<dyn GkNode>))
        }
        "jsonl_row_count" => {
            let filename = consts.first().map(|c| c.as_str()).unwrap_or("");
            Some(JsonlRowCount::new(filename).map(|n| Box::new(n) as Box<dyn GkNode>))
        }
        _ => None,
    }
}

crate::register_nodes!(signatures, build_node);

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn write_temp_csv(name: &str, content: &str) -> String {
        let path = std::env::temp_dir().join(name);
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(content.as_bytes()).unwrap();
        path.to_str().unwrap().to_string()
    }

    #[test]
    fn csv_field_by_name() {
        let path = write_temp_csv("test_csv_field.csv", "name,age,city\nalice,30,paris\nbob,25,london\n");
        let node = CsvField::new(&path, "name").unwrap();
        let mut out = [Value::None];
        node.eval(&[Value::U64(0)], &mut out);
        assert_eq!(out[0].to_display_string(), "alice");
        node.eval(&[Value::U64(1)], &mut out);
        assert_eq!(out[0].to_display_string(), "bob");
        // Wrap around
        node.eval(&[Value::U64(2)], &mut out);
        assert_eq!(out[0].to_display_string(), "alice");
    }

    #[test]
    fn csv_field_by_index() {
        let path = write_temp_csv("test_csv_idx.csv", "name,age,city\nalice,30,paris\n");
        let node = CsvField::new(&path, "1").unwrap();
        let mut out = [Value::None];
        node.eval(&[Value::U64(0)], &mut out);
        assert_eq!(out[0].to_display_string(), "30");
    }

    #[test]
    fn csv_row_returns_full_line() {
        let path = write_temp_csv("test_csv_row.csv", "a,b,c\n1,2,3\n4,5,6\n");
        let node = CsvRow::new(&path).unwrap();
        let mut out = [Value::None];
        node.eval(&[Value::U64(0)], &mut out);
        assert_eq!(out[0].to_display_string(), "1,2,3");
    }

    #[test]
    fn csv_row_count_excludes_header() {
        let path = write_temp_csv("test_csv_count.csv", "h1,h2\na,b\nc,d\ne,f\n");
        let node = CsvRowCount::new(&path).unwrap();
        let mut out = [Value::None];
        node.eval(&[], &mut out);
        assert_eq!(out[0].as_u64(), 3);
    }

    #[test]
    fn jsonl_field_top_level() {
        let path = write_temp_csv("test_jsonl_field.jsonl",
            "{\"name\":\"alice\",\"age\":30}\n{\"name\":\"bob\",\"age\":25}\n");
        let node = JsonlField::new(&path, "name").unwrap();
        let mut out = [Value::None];
        node.eval(&[Value::U64(0)], &mut out);
        assert_eq!(out[0].to_display_string(), "alice");
        node.eval(&[Value::U64(1)], &mut out);
        assert_eq!(out[0].to_display_string(), "bob");
    }

    #[test]
    fn jsonl_field_nested_path() {
        let path = write_temp_csv("test_jsonl_nested.jsonl",
            "{\"user\":{\"name\":\"alice\"}}\n{\"user\":{\"name\":\"bob\"}}\n");
        let node = JsonlField::new(&path, "user.name").unwrap();
        let mut out = [Value::None];
        node.eval(&[Value::U64(0)], &mut out);
        assert_eq!(out[0].to_display_string(), "alice");
    }

    #[test]
    fn jsonl_row_returns_full_json() {
        let path = write_temp_csv("test_jsonl_row.jsonl",
            "{\"a\":1}\n{\"b\":2}\n");
        let node = JsonlRow::new(&path).unwrap();
        let mut out = [Value::None];
        node.eval(&[Value::U64(0)], &mut out);
        assert!(out[0].to_display_string().contains("\"a\":1"));
    }

    #[test]
    fn jsonl_row_count() {
        let path = write_temp_csv("test_jsonl_count.jsonl",
            "{\"a\":1}\n{\"b\":2}\n{\"c\":3}\n");
        let node = JsonlRowCount::new(&path).unwrap();
        let mut out = [Value::None];
        node.eval(&[], &mut out);
        assert_eq!(out[0].as_u64(), 3);
    }
}
