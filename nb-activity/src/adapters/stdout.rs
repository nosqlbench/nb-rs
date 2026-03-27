// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! Stdout adapter: writes assembled ops to stdout or a file.
//!
//! This is the "just show me what the ops look like" adapter.
//! Useful for debugging workload templates, verifying variate
//! generation, and quick prototyping without a real database.
//!
//! Supports output to stdout (default) or a file.
//! Appends a newline after each op unless configured otherwise.

use std::io::{self, Write, BufWriter};
use std::fs::File;
use std::sync::Mutex;

use crate::adapter::{Adapter, AdapterError, AssembledOp, OpResult};

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
}

impl Default for StdoutConfig {
    fn default() -> Self {
        Self {
            filename: "stdout".into(),
            newline: true,
            format: StdoutFormat::Assignments,
        }
    }
}

/// How to render the assembled op.
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

    fn render(&self, op: &AssembledOp) -> String {
        match self {
            Self::Assignments => {
                op.fields.iter()
                    .map(|(k, v)| format!("{k}={v}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            }
            Self::Json => {
                let pairs: Vec<String> = op.fields.iter()
                    .map(|(k, v)| {
                        let escaped = v.replace('\\', "\\\\").replace('"', "\\\"");
                        format!("\"{k}\":\"{escaped}\"")
                    })
                    .collect();
                format!("{{{}}}", pairs.join(","))
            }
            Self::Csv => {
                op.fields.values()
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(",")
            }
            Self::Statement => {
                op.fields.get("stmt")
                    .cloned()
                    .unwrap_or_else(|| {
                        // Fall back to first field value
                        op.fields.values().next().cloned().unwrap_or_default()
                    })
            }
        }
    }
}

/// The stdout adapter: writes ops to stdout or a file.
pub struct StdoutAdapter {
    writer: Mutex<OutputTarget>,
    newline: bool,
    format: StdoutFormat,
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
            writer: Mutex::new(writer),
            newline: config.newline,
            format: config.format,
        }
    }
}

impl Adapter for StdoutAdapter {
    fn execute(&self, op: &AssembledOp) -> impl std::future::Future<Output = Result<OpResult, AdapterError>> + Send {
        let text = self.format.render(op);
        let newline = self.newline;

        // Write synchronously under the lock (stdout is fast)
        let result = {
            let mut writer = self.writer.lock().unwrap();
            let write_result = if newline {
                writeln!(writer, "{text}")
            } else {
                write!(writer, "{text}")
            };
            let _ = writer.flush();
            write_result
        };

        async move {
            match result {
                Ok(()) => Ok(OpResult {
                    success: true,
                    status: 0,
                    body: Some(text),
                }),
                Err(e) => Err(AdapterError {
                    error_name: "IoError".into(),
                    message: e.to_string(),
                }),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn test_op(fields: &[(&str, &str)]) -> AssembledOp {
        AssembledOp {
            name: "test".into(),
            fields: fields.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
        }
    }

    #[test]
    fn format_assignments() {
        let f = StdoutFormat::Assignments;
        let op = test_op(&[("a", "1"), ("b", "2")]);
        let rendered = f.render(&op);
        assert!(rendered.contains("a=1"));
        assert!(rendered.contains("b=2"));
    }

    #[test]
    fn format_json() {
        let f = StdoutFormat::Json;
        let op = test_op(&[("name", "alice")]);
        let rendered = f.render(&op);
        assert!(rendered.contains("\"name\":\"alice\""));
        assert!(rendered.starts_with('{'));
        assert!(rendered.ends_with('}'));
    }

    #[test]
    fn format_csv() {
        let f = StdoutFormat::Csv;
        let op = test_op(&[("a", "1"), ("b", "2")]);
        let rendered = f.render(&op);
        // HashMap order isn't guaranteed, but values should be comma-separated
        assert!(rendered.contains(','));
    }

    #[test]
    fn format_statement() {
        let f = StdoutFormat::Statement;
        let op = test_op(&[("stmt", "SELECT * FROM t;"), ("other", "ignored")]);
        assert_eq!(f.render(&op), "SELECT * FROM t;");
    }

    #[test]
    fn format_statement_fallback() {
        let f = StdoutFormat::Statement;
        let op = test_op(&[("body", "the body text")]);
        // No stmt field — falls back to first value
        let rendered = f.render(&op);
        assert!(!rendered.is_empty());
    }

    #[tokio::test]
    async fn stdout_adapter_executes() {
        // Write to a temp file instead of actual stdout
        let dir = std::env::temp_dir();
        let path = dir.join("nb_stdout_test.txt");
        let config = StdoutConfig {
            filename: path.to_str().unwrap().into(),
            newline: true,
            format: StdoutFormat::Assignments,
        };
        let adapter = StdoutAdapter::with_config(config);
        let op = test_op(&[("key", "value42")]);
        let result = adapter.execute(&op).await.unwrap();
        assert!(result.success);

        let content = std::fs::read_to_string(&path).unwrap();
        assert!(content.contains("key=value42"));
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn stdout_adapter_json_format() {
        let dir = std::env::temp_dir();
        let path = dir.join("nb_stdout_json_test.txt");
        let config = StdoutConfig {
            filename: path.to_str().unwrap().into(),
            newline: true,
            format: StdoutFormat::Json,
        };
        let adapter = StdoutAdapter::with_config(config);
        let op = test_op(&[("user", "alice"), ("age", "30")]);
        adapter.execute(&op).await.unwrap();

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
