// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Native scylla `QueryResult` body for nb-rs validation /
//! capture. Mirrors the cassandra-cpp adapter's `CqlResultBody`
//! shape so downstream wrappers see the same row/column surface
//! regardless of which engine produced the result.

use std::any::Any;
use std::collections::HashMap;
use nbrs_activity::adapter::ResultBody;
use scylla::response::query_result::QueryResult;
use scylla::value::{CqlValue, Row};

/// Engine-agnostic result body. Holds resolved row data as
/// rows × column-name maps. Each cell is converted to
/// `serde_json::Value` for uniform downstream handling — the
/// trait surface is `to_json` + `as_any`, just like every other
/// `ResultBody` implementer.
#[derive(Debug)]
pub(super) struct ScyllaResultBody {
    /// Row data: each row is a column-name → JSON value map.
    pub rows: Vec<HashMap<String, serde_json::Value>>,
}

impl ScyllaResultBody {
    pub fn from_query_result(result: QueryResult) -> Self {
        // DDL results / non-row results: empty body. Mirrors the
        // cassandra-cpp adapter's behavior.
        let rows = match result.into_rows_result() {
            Ok(rows_result) => {
                let cols: Vec<String> = rows_result.column_specs()
                    .iter()
                    .map(|spec| spec.name().to_string())
                    .collect();
                let mut row_maps: Vec<HashMap<String, serde_json::Value>> = Vec::new();
                if let Ok(iter) = rows_result.rows::<Row>() {
                    for row_result in iter {
                        let Ok(row) = row_result else { continue };
                        let mut row_map = HashMap::new();
                        for (idx, cell) in row.columns.iter().enumerate() {
                            let name = cols.get(idx).cloned().unwrap_or_default();
                            row_map.insert(name, cql_to_json(cell.as_ref()));
                        }
                        row_maps.push(row_map);
                    }
                }
                row_maps
            }
            Err(_) => Vec::new(),
        };
        Self { rows }
    }
}

impl ResultBody for ScyllaResultBody {
    fn to_json(&self) -> serde_json::Value {
        serde_json::Value::Array(
            self.rows.iter()
                .map(|row| serde_json::Value::Object(row.clone().into_iter().collect()))
                .collect(),
        )
    }

    fn as_any(&self) -> &dyn Any { self }

    fn element_count(&self) -> u64 { self.rows.len() as u64 }
}

/// Convert a CqlValue cell to `serde_json::Value`. Same projection
/// rules the cassandra-cpp adapter uses, so downstream validation
/// / captures see the same shape regardless of engine.
fn cql_to_json(value: Option<&CqlValue>) -> serde_json::Value {
    use serde_json::Value as J;
    let Some(v) = value else { return J::Null; };
    match v {
        CqlValue::Boolean(b) => J::Bool(*b),
        CqlValue::TinyInt(n)  => J::from(*n),
        CqlValue::SmallInt(n) => J::from(*n),
        CqlValue::Int(n)      => J::from(*n),
        CqlValue::BigInt(n)   => J::from(*n),
        CqlValue::Counter(c)  => J::from(c.0),
        CqlValue::Float(f)    => serde_json::Number::from_f64(*f as f64).map(J::Number).unwrap_or(J::Null),
        CqlValue::Double(f)   => serde_json::Number::from_f64(*f).map(J::Number).unwrap_or(J::Null),
        CqlValue::Text(s) | CqlValue::Ascii(s) => J::String(s.clone()),
        CqlValue::Uuid(u)     => J::String(u.to_string()),
        CqlValue::Timeuuid(u) => J::String(u.to_string()),
        CqlValue::Inet(ip)    => J::String(ip.to_string()),
        CqlValue::Blob(b)     => J::String(hex_encode(b)),
        CqlValue::List(items) | CqlValue::Set(items) | CqlValue::Vector(items) => {
            J::Array(items.iter().map(|v| cql_to_json(Some(v))).collect())
        }
        CqlValue::Map(entries) => {
            let mut obj = serde_json::Map::new();
            for (k, v) in entries {
                let key = match k {
                    CqlValue::Text(s) | CqlValue::Ascii(s) => s.clone(),
                    other => format!("{other:?}"),
                };
                obj.insert(key, cql_to_json(Some(v)));
            }
            J::Object(obj)
        }
        CqlValue::Tuple(items) => {
            J::Array(items.iter().map(|opt| cql_to_json(opt.as_ref())).collect())
        }
        CqlValue::Empty => J::Null,
        other => J::String(format!("{other:?}")),
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}
