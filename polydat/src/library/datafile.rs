// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Data file nodes: ordinal-based access to CSV, JSONL, and text files.
//!
//! Each node loads the file once at construction time (the filename is
//! a const parameter), builds an in-memory index, and serves fast
//! ordinal lookups at cycle time. Ordinals wrap via modulo so every
//! u64 input is valid.
//!
//! SRD-80b Phase E: all six data-file nodes are authored via
//! `#[polydat_node]`. The single-source nodes (`csv_row`,
//! `csv_row_count`, `jsonl_row`, `jsonl_row_count`) use
//! `#[poly_const(setup, from = filename)]`. The two two-source nodes
//! (`csv_field`, `jsonl_field`) use multi-source
//! `#[poly_const(setup, from = (filename, column|path))]` to build
//! the per-ordinal value table at construction time.
//!
//! Fallibility shift: hand-written `new()` returned
//! `Result<Self, String>`. The macro-emitted `new()` is infallible;
//! the setup function panics with the original formatted error on
//! bad file content. The build closure carries the panic upward —
//! workload compile still surfaces the same diagnostic text, just
//! via panic propagation instead of structured `Err`. This matches
//! the precedent in `library/regex.rs`'s
//! `compile_regex(...).expect("invalid regex")` setup.

#[cfg(test)]
use crate::ast::{PolydatNode, Value};

// ─── Workload-relative path resolution ─────────────────────────
//
// Data-file paths are read at construction time relative to the
// process CWD. That breaks when a workload is run from anywhere but
// the directory its relative paths assume. So — mirroring how
// `extends:` resolves relative targets against the workload's own
// directory — a compile sets the workload's directory as a base, and a
// relative path that doesn't resolve against the CWD is retried
// against it. Absolute paths and CWD-resolvable paths are untouched,
// so existing workloads are unaffected.

thread_local! {
    static DATA_BASE_DIR: std::cell::RefCell<Option<std::path::PathBuf>> =
        const { std::cell::RefCell::new(None) };
}

/// Set the base directory relative data-file paths resolve against
/// (the workload's own directory). Returns the previous value so the
/// caller can restore it — compiles nest. The compile entry points
/// bracket their (synchronous) body with this; concurrent compiles on
/// different threads keep independent values.
pub fn set_data_base_dir(dir: Option<std::path::PathBuf>) -> Option<std::path::PathBuf> {
    DATA_BASE_DIR.with(|d| d.replace(dir))
}

/// Resolve a data-file path. Absolute paths and paths that already
/// resolve against the CWD are returned verbatim; otherwise, if a
/// workload base dir is set and `<base>/<path>` exists, that absolute
/// path is returned. Falls back to the original so a genuine
/// not-found error names the path the author wrote.
fn resolve_data_path(filename: &str) -> std::borrow::Cow<'_, str> {
    let p = std::path::Path::new(filename);
    if p.is_absolute() || p.exists() {
        return std::borrow::Cow::Borrowed(filename);
    }
    DATA_BASE_DIR.with(|d| {
        if let Some(base) = d.borrow().as_ref() {
            let candidate = base.join(filename);
            if candidate.exists() {
                return std::borrow::Cow::Owned(candidate.to_string_lossy().into_owned());
            }
        }
        std::borrow::Cow::Borrowed(filename)
    })
}

// ─── CSV ───────────────────────────────────────────────────────

/// Setup function for `csv_field`: read the named column from the
/// CSV file and return one entry per data row. Panics on read or
/// column-not-found error (workload-compile diagnostic path).
fn read_csv_column(filename: &str, column: &str) -> Vec<String> {
    let content = std::fs::read_to_string(resolve_data_path(filename).as_ref())
        .unwrap_or_else(|e| panic!("csv_field: failed to read '{filename}': {e}"));
    let mut lines = content.lines();

    let header_line = lines.next()
        .unwrap_or_else(|| panic!("csv_field: '{filename}' is empty"));
    let headers: Vec<&str> = split_csv_line(header_line);

    let col_idx = if let Ok(idx) = column.parse::<usize>() {
        idx
    } else {
        headers.iter().position(|h| h.trim() == column)
            .unwrap_or_else(|| panic!(
                "csv_field: column '{column}' not found in '{filename}'. Available: {}",
                headers.join(", ")
            ))
    };

    let mut values = Vec::new();
    for line in lines {
        let fields: Vec<&str> = split_csv_line(line);
        let val = fields.get(col_idx).unwrap_or(&"").trim().to_string();
        values.push(val);
    }

    if values.is_empty() {
        panic!("csv_field: '{filename}' has no data rows");
    }
    values
}

/// Read a specific column from a CSV row at a given ordinal.
///
/// Signature: `csv_field(ordinal: u64) -> (output: Str)`
/// Const: `filename: Str`, `column: Str` (header name or "0","1",... index)
///
/// The file is read and the named column extracted at construction
/// time. Header row is auto-detected. Ordinal wraps modulo row count.
///
/// SRD-80b Phase E: migrated to `#[polydat_node]` via multi-source
/// `#[poly_const(... from = (filename, column))]`.
#[crate::polydat_node(category = Data)]
fn csv_field(
    ordinal: u64,
    filename: crate::derive_support::Const<&str>,
    column: crate::derive_support::Const<&str>,
    #[poly_const(read_csv_column, from = (filename, column))]
    values: &Vec<String>,
) -> String {
    let _ = filename;
    let _ = column;
    let idx = ordinal as usize % values.len();
    values[idx].clone()
}

/// Read all CSV data rows (skipping the header) as raw lines.
/// Panics on read failure or empty file — workload-compile error
/// path (preserves the original error-message format).
fn read_csv_data_rows(filename: &str) -> Vec<String> {
    let content = std::fs::read_to_string(resolve_data_path(filename).as_ref())
        .unwrap_or_else(|e| panic!("csv_row: failed to read '{filename}': {e}"));
    let rows: Vec<String> = content.lines()
        .skip(1) // skip header
        .filter(|l| !l.trim().is_empty())
        .map(|l| l.to_string())
        .collect();
    if rows.is_empty() {
        panic!("csv_row: '{filename}' has no data rows");
    }
    rows
}

/// Read a CSV file and return its data-row count (excluding header).
/// Panics on read failure — workload-compile error path.
fn read_csv_row_count(filename: &str) -> u64 {
    let content = std::fs::read_to_string(resolve_data_path(filename).as_ref())
        .unwrap_or_else(|e| panic!("csv_row_count: failed to read '{filename}': {e}"));
    content.lines()
        .skip(1)
        .filter(|l| !l.trim().is_empty())
        .count() as u64
}

/// Read an entire CSV row at a given ordinal as a comma-separated string.
///
/// Signature: `csv_row(ordinal: u64) -> (output: Str)`
/// Const: `filename: Str`
///
/// SRD-80b Phase E: migrated to `#[polydat_node]`. Construction-time
/// failure (missing file, empty file) panics via the setup function;
/// the build closure propagates the panic message.
#[crate::polydat_node(category = Data)]
fn csv_row(
    ordinal: u64,
    filename: crate::derive_support::Const<&str>,
    #[poly_const(read_csv_data_rows, from = filename)]
    rows: &Vec<String>,
) -> String {
    let idx = ordinal as usize % rows.len();
    rows[idx].clone()
}

/// Return the number of data rows in a CSV file (init-time constant).
///
/// Signature: `csv_row_count() -> (output: u64)`
/// Const: `filename: Str`
///
/// SRD-80b Phase E: migrated to `#[polydat_node]`. The count is
/// computed once at setup time; `eval` reads the cached value.
#[crate::polydat_node(category = Data)]
fn csv_row_count(
    filename: crate::derive_support::Const<&str>,
    #[poly_const(read_csv_row_count, from = filename)]
    count: &u64,
) -> u64 {
    *count
}

// ─── JSONL ─────────────────────────────────────────────────────

/// Setup function for `jsonl_field`: read the JSONL file, extract
/// the named field per line, return one entry per non-empty line.
/// Panics on read or JSON-parse error (workload-compile diagnostic
/// path).
fn read_jsonl_field(filename: &str, path: &str) -> Vec<String> {
    let content = std::fs::read_to_string(resolve_data_path(filename).as_ref())
        .unwrap_or_else(|e| panic!("jsonl_field: failed to read '{filename}': {e}"));
    let mut values = Vec::new();
    for (line_num, line) in content.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() { continue; }
        let parsed: serde_json::Value = serde_json::from_str(trimmed)
            .unwrap_or_else(|e| panic!(
                "jsonl_field: parse error at line {}: {e}", line_num + 1));
        let val = resolve_json_path(&parsed, path);
        values.push(val);
    }
    if values.is_empty() {
        panic!("jsonl_field: '{filename}' has no lines");
    }
    values
}

/// Read a field from a JSONL line at a given ordinal.
///
/// Signature: `jsonl_field(ordinal: u64) -> (output: Str)`
/// Const: `filename: Str`, `path: Str` (JSON field name or dot path)
///
/// Each line of the file is a JSON object. The field is extracted
/// by name (top-level) or dot-path (nested) at construction time.
/// Ordinal wraps modulo line count.
///
/// SRD-80b Phase E: migrated to `#[polydat_node]` via multi-source
/// `#[poly_const(... from = (filename, path))]`.
#[crate::polydat_node(category = Data)]
fn jsonl_field(
    ordinal: u64,
    filename: crate::derive_support::Const<&str>,
    path: crate::derive_support::Const<&str>,
    #[poly_const(read_jsonl_field, from = (filename, path))]
    values: &Vec<String>,
) -> String {
    let _ = filename;
    let _ = path;
    let idx = ordinal as usize % values.len();
    values[idx].clone()
}

/// Read all non-empty lines of a JSONL file. Panics on read failure
/// or empty file (workload-compile error path).
fn read_jsonl_lines(filename: &str) -> Vec<String> {
    let content = std::fs::read_to_string(resolve_data_path(filename).as_ref())
        .unwrap_or_else(|e| panic!("jsonl_row: failed to read '{filename}': {e}"));
    let rows: Vec<String> = content.lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| l.to_string())
        .collect();
    if rows.is_empty() {
        panic!("jsonl_row: '{filename}' has no lines");
    }
    rows
}

/// Count non-empty lines in a JSONL file. Panics on read failure
/// (workload-compile error path).
fn read_jsonl_row_count(filename: &str) -> u64 {
    let content = std::fs::read_to_string(resolve_data_path(filename).as_ref())
        .unwrap_or_else(|e| panic!("jsonl_row_count: failed to read '{filename}': {e}"));
    content.lines()
        .filter(|l| !l.trim().is_empty())
        .count() as u64
}

/// Read an entire JSONL line at a given ordinal as a JSON string.
///
/// Signature: `jsonl_row(ordinal: u64) -> (output: Str)`
/// Const: `filename: Str`
///
/// SRD-80b Phase E: migrated to `#[polydat_node]`. Lines are
/// captured at setup time.
#[crate::polydat_node(category = Data)]
fn jsonl_row(
    ordinal: u64,
    filename: crate::derive_support::Const<&str>,
    #[poly_const(read_jsonl_lines, from = filename)]
    rows: &Vec<String>,
) -> String {
    let idx = ordinal as usize % rows.len();
    rows[idx].clone()
}

/// Return the number of lines in a JSONL file (init-time constant).
///
/// Signature: `jsonl_row_count() -> (output: u64)`
/// Const: `filename: Str`
///
/// SRD-80b Phase E: migrated to `#[polydat_node]`.
#[crate::polydat_node(category = Data)]
fn jsonl_row_count(
    filename: crate::derive_support::Const<&str>,
    #[poly_const(read_jsonl_row_count, from = filename)]
    count: &u64,
) -> u64 {
    *count
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

// SRD-80b Phase E: all six data-file nodes register via the
// `#[polydat_node]` macro's inventory emission. No explicit
// FuncSig / build_node table needed.

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
        let node = CsvField::new(path, "name".to_string());
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
    fn relative_path_resolves_against_data_base_dir() {
        // A file in a subdirectory, referenced by basename only —
        // the workload-relative resolution mirrors how `extends:`
        // resolves relative targets against the workload's directory.
        let dir = std::env::temp_dir().join("nbrs_datafile_base_test");
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("base_rows.jsonl");
        std::fs::write(&file, "{\"v\": 7}\n").unwrap();

        // No base dir: a bare basename does not resolve to the file.
        let prev = set_data_base_dir(None);
        assert_eq!(resolve_data_path("base_rows.jsonl").as_ref(), "base_rows.jsonl");

        // Base dir set: the basename resolves to the absolute file, and
        // the reader honours it.
        set_data_base_dir(Some(dir.clone()));
        assert_eq!(resolve_data_path("base_rows.jsonl").as_ref(), file.to_string_lossy());
        assert_eq!(read_jsonl_field("base_rows.jsonl", "v"), vec!["7".to_string()]);

        // Absolute paths pass through untouched even with a base set.
        let abs = file.to_string_lossy().into_owned();
        assert_eq!(resolve_data_path(&abs).as_ref(), abs);

        set_data_base_dir(prev);
    }

    #[test]
    fn csv_field_by_index() {
        let path = write_temp_csv("test_csv_idx.csv", "name,age,city\nalice,30,paris\n");
        let node = CsvField::new(path, "1".to_string());
        let mut out = [Value::None];
        node.eval(&[Value::U64(0)], &mut out);
        assert_eq!(out[0].to_display_string(), "30");
    }

    #[test]
    fn csv_row_returns_full_line() {
        let path = write_temp_csv("test_csv_row.csv", "a,b,c\n1,2,3\n4,5,6\n");
        // Macro-emitted: `CsvRow::new(filename: String) -> Self` (panics on
        // bad file via `read_csv_data_rows`).
        let node = CsvRow::new(path);
        let mut out = [Value::None];
        node.eval(&[Value::U64(0)], &mut out);
        assert_eq!(out[0].to_display_string(), "1,2,3");
    }

    #[test]
    fn csv_row_count_excludes_header() {
        let path = write_temp_csv("test_csv_count.csv", "h1,h2\na,b\nc,d\ne,f\n");
        let node = CsvRowCount::new(path);
        let mut out = [Value::None];
        node.eval(&[], &mut out);
        assert_eq!(out[0].as_u64(), 3);
    }

    #[test]
    fn jsonl_field_top_level() {
        let path = write_temp_csv("test_jsonl_field.jsonl",
            "{\"name\":\"alice\",\"age\":30}\n{\"name\":\"bob\",\"age\":25}\n");
        let node = JsonlField::new(path, "name".to_string());
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
        let node = JsonlField::new(path, "user.name".to_string());
        let mut out = [Value::None];
        node.eval(&[Value::U64(0)], &mut out);
        assert_eq!(out[0].to_display_string(), "alice");
    }

    #[test]
    fn jsonl_row_returns_full_json() {
        let path = write_temp_csv("test_jsonl_row.jsonl",
            "{\"a\":1}\n{\"b\":2}\n");
        let node = JsonlRow::new(path);
        let mut out = [Value::None];
        node.eval(&[Value::U64(0)], &mut out);
        assert!(out[0].to_display_string().contains("\"a\":1"));
    }

    #[test]
    fn jsonl_row_count() {
        let path = write_temp_csv("test_jsonl_count.jsonl",
            "{\"a\":1}\n{\"b\":2}\n{\"c\":3}\n");
        let node = JsonlRowCount::new(path);
        let mut out = [Value::None];
        node.eval(&[], &mut out);
        assert_eq!(out[0].as_u64(), 3);
    }
}
