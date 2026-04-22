// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Stdout adapter: writes resolved ops to stdout or a file.
//!
//! Formats:
//!   stmt        — statement field only (default for `nbrs run`)
//!   readout     — aligned name=value pairs, one per line per op
//!   assignments — compact name=value pairs on one line
//!   json        — JSON object per op (proper types, not all strings)
//!   jsonl       — same as json (alias for JSON Lines workflows)
//!   csv         — comma-separated values (optional header row)
//!   tsv         — tab-separated values
//!   raw         — values only, custom separator
//!
//! Options:
//!   filename=<path>    — output file (default: stdout)
//!   format=<name>      — output format (default: stmt)
//!   separator=<str>    — field separator for raw/csv formats
//!   header=true        — emit header row (csv/tsv formats)
//!   fields=a,b,c       — only include named fields

use std::io::{self, Write, BufWriter};
use std::fs::File;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, Ordering};

use nb_activity::adapter::{
    AdapterError, DriverAdapter, ExecutionError, OpDispenser, OpResult, ResolvedFields, TextBody,
};
use nb_workload::model::ParsedOp;

/// Output target for the stdout adapter.
enum OutputTarget {
    Stdout(BufWriter<io::Stdout>),
    File(BufWriter<File>),
}

impl Write for OutputTarget {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            OutputTarget::Stdout(w) => w.write(buf),
            OutputTarget::File(w) => w.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            OutputTarget::Stdout(w) => w.flush(),
            OutputTarget::File(w) => w.flush(),
        }
    }
}

/// Configuration for the stdout adapter.
pub struct StdoutConfig {
    /// Output filename, or "stdout" for console output.
    pub filename: String,
    /// Whether to append a newline after each op.
    pub newline: bool,
    /// Output format.
    pub format: StdoutFormat,
    /// Optional field filter: only display these fields.
    /// Empty means display all fields.
    pub fields_filter: Vec<String>,
    /// Field separator for Raw format. Default: ","
    pub separator: String,
    /// Whether to emit a header row (for CSV/TSV/Raw).
    pub header: bool,
    /// Whether to process `{red}`, `{reset}`, etc. ANSI color tokens
    /// in rendered output. Enable with `--color` or `color=true`.
    pub color: bool,
}

impl Default for StdoutConfig {
    fn default() -> Self {
        Self {
            filename: "stdout".into(),
            newline: true,
            format: StdoutFormat::Assignments,
            fields_filter: Vec::new(),
            separator: ",".into(),
            header: false,
            color: false,
        }
    }
}

impl StdoutConfig {
    /// Construct a config from CLI/workload params.
    pub fn from_params(params: &std::collections::HashMap<String, String>) -> Self {
        let format = params.get("format")
            .map(|s| StdoutFormat::parse(s).unwrap_or(StdoutFormat::Assignments))
            .unwrap_or(StdoutFormat::Statement);
        Self {
            filename: params.get("filename").cloned().unwrap_or("stdout".into()),
            newline: true,
            format,
            fields_filter: Vec::new(),
            separator: params.get("separator").cloned().unwrap_or(",".into()),
            header: params.get("header").map(|s| s == "true" || s == "1" || s == "yes").unwrap_or(false),
            color: params.get("color").map(|s| s == "true" || s == "1" || s == "yes").unwrap_or(false),
        }
    }
}

/// How to render the resolved op fields.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum StdoutFormat {
    /// The `stmt` field only. Default for `nbrs run`.
    Statement,
    /// Aligned `name = value` pairs, one per line. Readable output
    /// for debugging multi-field ops.
    Readout,
    /// Compact `name=value, name=value` on one line.
    Assignments,
    /// JSON object with proper types (numbers stay numbers).
    Json,
    /// Comma-separated values. Use with `header=true` for column names.
    Csv,
    /// Tab-separated values. Use with `header=true` for column names.
    Tsv,
    /// Values only with a custom separator (`separator=|`).
    Raw,
}

impl StdoutFormat {
    pub fn parse(s: &str) -> Result<Self, String> {
        match s.to_lowercase().as_str() {
            "statement" | "stmt" => Ok(Self::Statement),
            "readout" => Ok(Self::Readout),
            "assignments" | "assign" => Ok(Self::Assignments),
            "json" | "inlinejson" | "jsonl" => Ok(Self::Json),
            "csv" => Ok(Self::Csv),
            "tsv" => Ok(Self::Tsv),
            "raw" => Ok(Self::Raw),
            other => Err(format!(
                "unknown format: '{other}'. Available: stmt, readout, assignments, json, csv, tsv, raw"
            )),
        }
    }

    /// Render resolved fields to a string.
    pub fn render(&self, fields: &ResolvedFields, separator: &str) -> String {
        match self {
            Self::Statement => {
                // Statement mode: just the stmt field value, or all fields joined by newlines
                if let Some(stmt) = fields.get_str("stmt") {
                    stmt.to_string()
                } else if let Some(raw) = fields.get_str("raw") {
                    raw.to_string()
                } else if let Some(prepared) = fields.get_str("prepared") {
                    prepared.to_string()
                } else {
                    fields.strings().join("\n")
                }
            }
            Self::Readout => {
                // Aligned name = value, one per line
                let max_name_len = fields.names.iter().map(|n| n.len()).max().unwrap_or(0);
                fields.names.iter().zip(fields.strings().iter())
                    .map(|(k, v)| format!("  {:width$} = {v}", k, width = max_name_len))
                    .collect::<Vec<_>>()
                    .join("\n")
            }
            Self::Assignments => {
                fields.names.iter().zip(fields.strings().iter())
                    .map(|(k, v): (&String, &String)| format!("{k}={v}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            }
            Self::Json => {
                // Proper JSON with typed values (numbers, bools, not all strings)
                let obj = fields.to_json();
                obj.to_string()
            }
            Self::Csv => {
                // CSV: quote fields that contain commas or quotes
                fields.strings().iter()
                    .map(|v| csv_escape(v, ','))
                    .collect::<Vec<_>>()
                    .join(",")
            }
            Self::Tsv => {
                // TSV: escape tabs in values
                fields.strings().iter()
                    .map(|v| v.replace('\t', "\\t"))
                    .collect::<Vec<_>>()
                    .join("\t")
            }
            Self::Raw => {
                fields.strings().join(separator)
            }
        }
    }

    /// Render a header row (field names) for tabular formats.
    fn render_header(&self, fields: &ResolvedFields, separator: &str) -> String {
        match self {
            Self::Csv => fields.names.join(","),
            Self::Tsv => fields.names.join("\t"),
            Self::Raw => fields.names.join(separator),
            _ => String::new(),
        }
    }

    /// Whether this format supports a header row.
    fn supports_header(&self) -> bool {
        matches!(self, Self::Csv | Self::Tsv | Self::Raw)
    }
}

/// Replace `{color}` tokens with ANSI escape sequences.
/// Supported: {red}, {green}, {yellow}, {blue}, {magenta}, {cyan},
/// {white}, {bold}, {dim}, {reset}, {underline}.
fn apply_ansi_colors(text: &str) -> String {
    text.replace("{red}", "\x1b[31m")
        .replace("{green}", "\x1b[32m")
        .replace("{yellow}", "\x1b[33m")
        .replace("{blue}", "\x1b[34m")
        .replace("{magenta}", "\x1b[35m")
        .replace("{cyan}", "\x1b[36m")
        .replace("{white}", "\x1b[37m")
        .replace("{bold}", "\x1b[1m")
        .replace("{dim}", "\x1b[2m")
        .replace("{underline}", "\x1b[4m")
        .replace("{reset}", "\x1b[0m")
}

/// Escape a value for CSV output (RFC 4180).
fn csv_escape(value: &str, delimiter: char) -> String {
    if value.contains(delimiter) || value.contains('"') || value.contains('\n') {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

/// The stdout adapter: writes ops to stdout or a file.
pub struct StdoutAdapter {
    writer: Arc<Mutex<OutputTarget>>,
    newline: bool,
    format: StdoutFormat,
    config_fields_filter: Vec<String>,
    separator: String,
    header: bool,
    header_emitted: Arc<AtomicBool>,
    color: bool,
}

impl StdoutAdapter {
    /// Create with default config (stdout, newlines, assignments format).
    pub fn new() -> Self {
        Self::with_config(StdoutConfig::default())
    }

    /// Create with explicit config.
    pub fn with_config(config: StdoutConfig) -> Self {
        let writer = if config.filename.eq_ignore_ascii_case("stdout") {
            OutputTarget::Stdout(BufWriter::new(io::stdout()))
        } else {
            let file = File::create(&config.filename)
                .unwrap_or_else(|e| panic!("failed to create output file '{}': {e}", config.filename));
            OutputTarget::File(BufWriter::new(file))
        };
        Self {
            writer: Arc::new(Mutex::new(writer)),
            newline: config.newline,
            config_fields_filter: config.fields_filter,
            format: config.format,
            separator: config.separator,
            header: config.header,
            header_emitted: Arc::new(AtomicBool::new(false)),
            color: config.color,
        }
    }
}

impl DriverAdapter for StdoutAdapter {
    fn name(&self) -> &str { "stdout" }

    fn map_op(&self, _template: &ParsedOp) -> Result<Box<dyn OpDispenser>, String> {
        Ok(Box::new(StdoutDispenser {
            writer: self.writer.clone(),
            format: self.format,
            newline: self.newline,
            fields_filter: self.config_fields_filter.clone(),
            separator: self.separator.clone(),
            header: self.header,
            header_emitted: self.header_emitted.clone(),
            color: self.color,
        }))
    }
}

/// Op dispenser for the stdout adapter.
pub struct StdoutDispenser {
    writer: Arc<Mutex<OutputTarget>>,
    format: StdoutFormat,
    newline: bool,
    fields_filter: Vec<String>,
    separator: String,
    header: bool,
    header_emitted: Arc<AtomicBool>,
    color: bool,
}

impl OpDispenser for StdoutDispenser {
    fn execute<'a>(
        &'a self,
        _cycle: u64,
        fields: &'a ResolvedFields,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<OpResult, ExecutionError>> + Send + 'a>> {
        Box::pin(async move {
            // Apply field filter if configured
            let filtered;
            let render_fields = if self.fields_filter.is_empty() {
                fields
            } else {
                let mut names = Vec::new();
                let mut values = Vec::new();
                for (i, name) in fields.names.iter().enumerate() {
                    if self.fields_filter.iter().any(|f| f == name) {
                        names.push(name.clone());
                        values.push(fields.values[i].clone());
                    }
                }
                filtered = ResolvedFields::new(names, values);
                &filtered
            };

            let raw_text = self.format.render(render_fields, &self.separator);
            let text = if self.color {
                apply_ansi_colors(&raw_text)
            } else {
                raw_text
            };

            let result = {
                let mut writer = self.writer.lock()
                    .unwrap_or_else(|e| e.into_inner());

                // Emit header row once for tabular formats
                if self.header && self.format.supports_header()
                    && !self.header_emitted.swap(true, Ordering::Relaxed)
                {
                    let header = self.format.render_header(render_fields, &self.separator);
                    if !header.is_empty() {
                        let _ = writeln!(writer, "{header}");
                    }
                }

                let write_result = if self.newline {
                    writeln!(writer, "{text}")
                } else {
                    write!(writer, "{text}")
                };
                if let Err(e) = writer.flush() {
                    return Err(ExecutionError::Op(AdapterError {
                        error_name: "FlushError".into(),
                        message: e.to_string(),
                        retryable: false,
                    }));
                }
                write_result
            };

            match result {
                Ok(()) => Ok(OpResult {
                    body: Some(Box::new(TextBody(text))),
                    captures: std::collections::HashMap::new(),
                    skipped: false,
                }),
                Err(e) => Err(ExecutionError::Op(AdapterError {
                    error_name: "IoError".into(),
                    message: e.to_string(),
                    retryable: false,
                })),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_fields(fields: &[(&str, &str)]) -> ResolvedFields {
        ResolvedFields::new(
            fields.iter().map(|(k, _)| k.to_string()).collect(),
            fields.iter().map(|(_, v)| nb_variates::node::Value::Str(v.to_string())).collect(),
        )
    }

    fn typed_fields() -> ResolvedFields {
        ResolvedFields::new(
            vec!["name".into(), "age".into(), "score".into()],
            vec![
                nb_variates::node::Value::Str("alice".into()),
                nb_variates::node::Value::U64(30),
                nb_variates::node::Value::F64(3.14),
            ],
        )
    }

    #[test]
    fn format_statement_prefers_stmt_field() {
        let f = StdoutFormat::Statement;
        let fields = test_fields(&[("stmt", "SELECT 1"), ("other", "ignored")]);
        assert_eq!(f.render(&fields, ","), "SELECT 1");
    }

    #[test]
    fn format_statement_falls_back_to_raw() {
        let f = StdoutFormat::Statement;
        let fields = test_fields(&[("raw", "CREATE TABLE t")]);
        assert_eq!(f.render(&fields, ","), "CREATE TABLE t");
    }

    #[test]
    fn format_readout_aligned() {
        let f = StdoutFormat::Readout;
        let fields = test_fields(&[("name", "alice"), ("age", "30"), ("x", "1")]);
        let rendered = f.render(&fields, ",");
        // All '=' should be aligned
        let lines: Vec<&str> = rendered.lines().collect();
        assert_eq!(lines.len(), 3);
        assert!(lines[0].contains("name = alice"));
        assert!(lines[1].contains("age  = 30"));
        assert!(lines[2].contains("x    = 1"));
    }

    #[test]
    fn format_assignments() {
        let f = StdoutFormat::Assignments;
        let fields = test_fields(&[("a", "1"), ("b", "2")]);
        assert_eq!(f.render(&fields, ","), "a=1, b=2");
    }

    #[test]
    fn format_json_typed_values() {
        let f = StdoutFormat::Json;
        let fields = typed_fields();
        let rendered = f.render(&fields, ",");
        let parsed: serde_json::Value = serde_json::from_str(&rendered).unwrap();
        assert_eq!(parsed["name"], "alice");
        assert_eq!(parsed["age"], 30);
        assert!((parsed["score"].as_f64().unwrap() - 3.14).abs() < 0.001);
    }

    #[test]
    fn format_csv_escapes_commas() {
        let f = StdoutFormat::Csv;
        let fields = test_fields(&[("a", "hello, world"), ("b", "plain")]);
        let rendered = f.render(&fields, ",");
        assert_eq!(rendered, "\"hello, world\",plain");
    }

    #[test]
    fn format_csv_header() {
        let f = StdoutFormat::Csv;
        let fields = test_fields(&[("name", "alice"), ("age", "30")]);
        let header = f.render_header(&fields, ",");
        assert_eq!(header, "name,age");
    }

    #[test]
    fn format_tsv() {
        let f = StdoutFormat::Tsv;
        let fields = test_fields(&[("a", "1"), ("b", "2")]);
        assert_eq!(f.render(&fields, "\t"), "1\t2");
    }

    #[test]
    fn format_raw_custom_separator() {
        let f = StdoutFormat::Raw;
        let fields = test_fields(&[("a", "1"), ("b", "2"), ("c", "3")]);
        assert_eq!(f.render(&fields, "|"), "1|2|3");
    }

    #[test]
    fn format_parse_all() {
        assert!(matches!(StdoutFormat::parse("stmt").unwrap(), StdoutFormat::Statement));
        assert!(matches!(StdoutFormat::parse("readout").unwrap(), StdoutFormat::Readout));
        assert!(matches!(StdoutFormat::parse("assignments").unwrap(), StdoutFormat::Assignments));
        assert!(matches!(StdoutFormat::parse("json").unwrap(), StdoutFormat::Json));
        assert!(matches!(StdoutFormat::parse("jsonl").unwrap(), StdoutFormat::Json));
        assert!(matches!(StdoutFormat::parse("csv").unwrap(), StdoutFormat::Csv));
        assert!(matches!(StdoutFormat::parse("tsv").unwrap(), StdoutFormat::Tsv));
        assert!(matches!(StdoutFormat::parse("raw").unwrap(), StdoutFormat::Raw));
        assert!(StdoutFormat::parse("bogus").is_err());
    }

    #[tokio::test]
    async fn dispenser_writes_to_file() {
        let path = std::env::temp_dir().join("nb_stdout_test.txt");
        let adapter = StdoutAdapter::with_config(StdoutConfig {
            filename: path.to_str().unwrap().into(),
            newline: true,
            format: StdoutFormat::Assignments,
            ..Default::default()
        });
        let template = ParsedOp::simple("test", "key={key}");
        let dispenser = adapter.map_op(&template).unwrap();
        let fields = test_fields(&[("key", "value42")]);
        dispenser.execute(0, &fields).await.unwrap();

        let content = std::fs::read_to_string(&path).unwrap();
        assert!(content.contains("key=value42"));
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn dispenser_csv_with_header() {
        let path = std::env::temp_dir().join("nb_csv_header_test.txt");
        let adapter = StdoutAdapter::with_config(StdoutConfig {
            filename: path.to_str().unwrap().into(),
            newline: true,
            format: StdoutFormat::Csv,
            header: true,
            ..Default::default()
        });
        let template = ParsedOp::simple("test", "test");
        let dispenser = adapter.map_op(&template).unwrap();

        let fields1 = test_fields(&[("name", "alice"), ("age", "30")]);
        let fields2 = test_fields(&[("name", "bob"), ("age", "25")]);
        dispenser.execute(0, &fields1).await.unwrap();
        dispenser.execute(1, &fields2).await.unwrap();

        let content = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines[0], "name,age", "first line should be header");
        assert_eq!(lines[1], "alice,30");
        assert_eq!(lines[2], "bob,25");
        let _ = std::fs::remove_file(&path);
    }
}

// =========================================================================
// Adapter Registration (inventory-based, link-time)
// =========================================================================

inventory::submit! {
    nb_activity::adapter::AdapterRegistration {
        names: || &["stdout"],
        known_params: || &["filename", "format", "separator", "header", "color", "fields"],
        display_preference: || nb_activity::adapter::DisplayPreference::Auto,
        create: |params| Box::pin(async move {
            Ok(std::sync::Arc::new(StdoutAdapter::with_config(StdoutConfig::from_params(&params)))
                as std::sync::Arc<dyn nb_activity::adapter::DriverAdapter>)
        }),
    }
}
