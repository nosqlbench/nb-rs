// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! Stdout adapter: writes resolved ops to stdout or a file.
//!
//! This is the "just show me what the ops look like" adapter.
//! Useful for debugging workload templates, verifying variate
//! generation, and quick prototyping without a real database.
//!
//! Supports output to stdout (default) or a file.
//! Appends a newline after each op unless configured otherwise.

use std::io::{self, Write, BufWriter};
use std::fs::File;
use std::sync::{Arc, Mutex};

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
    /// Output format: how to render the op fields.
    pub format: StdoutFormat,
    /// Optional field filter: only display these fields.
    /// Empty means display all fields. Set via `fields=name1,name2`.
    pub fields_filter: Vec<String>,
}

impl Default for StdoutConfig {
    fn default() -> Self {
        Self {
            filename: "stdout".into(),
            newline: true,
            format: StdoutFormat::Assignments,
            fields_filter: Vec::new(),
        }
    }
}

/// How to render the resolved op fields.
#[derive(Debug, Clone, Copy)]
pub enum StdoutFormat {
    /// `field1=value1, field2=value2` (default)
    Assignments,
    /// `{"field1":"value1","field2":"value2"}`
    Json,
    /// `value1,value2,value3`
    Csv,
    /// The `stmt` field only (if present)
    Statement,
}

impl StdoutFormat {
    pub fn parse(s: &str) -> Result<Self, String> {
        match s.to_lowercase().as_str() {
            "assignments" | "assign" => Ok(Self::Assignments),
            "json" | "inlinejson" => Ok(Self::Json),
            "csv" => Ok(Self::Csv),
            "statement" | "stmt" => Ok(Self::Statement),
            other => Err(format!("unknown stdout format: '{other}'")),
        }
    }

    /// Render resolved fields to a string.
    pub fn render(&self, fields: &ResolvedFields) -> String {
        match self {
            Self::Assignments => {
                fields.names.iter().zip(fields.strings().iter())
                    .map(|(k, v): (&String, &String)| format!("{k}={v}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            }
            Self::Json => {
                let pairs: Vec<String> = fields.names.iter().zip(fields.strings().iter())
                    .map(|(k, v): (&String, &String)| {
                        let escaped = v.replace('\\', "\\\\").replace('"', "\\\"");
                        format!("\"{k}\":\"{escaped}\"")
                    })
                    .collect();
                format!("{{{}}}", pairs.join(","))
            }
            Self::Csv => {
                fields.strings().join(",")
            }
            Self::Statement => {
                fields.strings().join("\n")
            }
        }
    }
}

/// The stdout adapter: writes ops to stdout or a file.
pub struct StdoutAdapter {
    writer: Arc<Mutex<OutputTarget>>,
    newline: bool,
    format: StdoutFormat,
    config_fields_filter: Vec<String>,
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
        }))
    }
}

/// Op dispenser for the stdout adapter. Captures format at init time,
/// shares the writer via Arc.
pub struct StdoutDispenser {
    writer: Arc<Mutex<OutputTarget>>,
    format: StdoutFormat,
    newline: bool,
    fields_filter: Vec<String>,
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
            let text = self.format.render(render_fields);

            let result = {
                let mut writer = self.writer.lock()
                    .unwrap_or_else(|e| e.into_inner());
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

    #[test]
    fn format_assignments() {
        let f = StdoutFormat::Assignments;
        let fields = test_fields(&[("a", "1"), ("b", "2")]);
        let rendered = f.render(&fields);
        assert!(rendered.contains("a=1"));
        assert!(rendered.contains("b=2"));
    }

    #[test]
    fn format_json() {
        let f = StdoutFormat::Json;
        let fields = test_fields(&[("name", "alice")]);
        let rendered = f.render(&fields);
        assert!(rendered.contains("\"name\":\"alice\""));
        assert!(rendered.starts_with('{'));
        assert!(rendered.ends_with('}'));
    }

    #[test]
    fn format_csv() {
        let f = StdoutFormat::Csv;
        let fields = test_fields(&[("a", "1"), ("b", "2")]);
        let rendered = f.render(&fields);
        assert_eq!(rendered, "1,2");
    }

    #[test]
    fn format_statement() {
        let f = StdoutFormat::Statement;
        // Statement mode renders all fields, newline-separated
        let fields = test_fields(&[("stmt", "SELECT * FROM t;"), ("other", "ignored")]);
        assert_eq!(f.render(&fields), "SELECT * FROM t;\nignored");
    }

    #[test]
    fn format_statement_fallback() {
        let f = StdoutFormat::Statement;
        let fields = test_fields(&[("body", "the body text")]);
        assert_eq!(f.render(&fields), "the body text");
    }

    #[tokio::test]
    async fn stdout_dispenser_executes() {
        let dir = std::env::temp_dir();
        let path = dir.join("nb_stdout_test.txt");
        let adapter = StdoutAdapter::with_config(StdoutConfig {
            filename: path.to_str().unwrap().into(),
            newline: true,
            format: StdoutFormat::Assignments,
            fields_filter: Vec::new(),
        });
        let template = nb_workload::model::ParsedOp::simple("test", "key={key}");
        let dispenser = adapter.map_op(&template).unwrap();
        let fields = test_fields(&[("key", "value42")]);
        let _result = dispenser.execute(0, &fields).await.unwrap();

        let content = std::fs::read_to_string(&path).unwrap();
        assert!(content.contains("key=value42"));
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn stdout_dispenser_json_format() {
        let dir = std::env::temp_dir();
        let path = dir.join("nb_stdout_json_test.txt");
        let adapter = StdoutAdapter::with_config(StdoutConfig {
            filename: path.to_str().unwrap().into(),
            newline: true,
            format: StdoutFormat::Json,
            fields_filter: Vec::new(),
        });
        let template = nb_workload::model::ParsedOp::simple("test", "test");
        let dispenser = adapter.map_op(&template).unwrap();
        let fields = test_fields(&[("user", "alice"), ("age", "30")]);
        dispenser.execute(0, &fields).await.unwrap();

        let content = std::fs::read_to_string(&path).unwrap();
        assert!(content.contains("\"user\":\"alice\""));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn format_parse() {
        assert!(matches!(StdoutFormat::parse("json").unwrap(), StdoutFormat::Json));
        assert!(matches!(StdoutFormat::parse("CSV").unwrap(), StdoutFormat::Csv));
        assert!(matches!(StdoutFormat::parse("stmt").unwrap(), StdoutFormat::Statement));
        assert!(matches!(StdoutFormat::parse("assignments").unwrap(), StdoutFormat::Assignments));
        assert!(StdoutFormat::parse("bogus").is_err());
    }
}
