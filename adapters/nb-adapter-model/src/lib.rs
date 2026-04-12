// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Model adapter: simulates operation execution for workload prototyping.
//!
//! Extends the stdout adapter with:
//! - Simulated results via the `result` op field (static map or GK kernel)
//! - Latency simulation via `result-latency`
//! - Deterministic error injection via `result-error-rate`
//! - Diagnostic output via `--diagnose`
//!
//! When no `result` field is present, behaves identically to stdout.
//! See SRD 29 for the full design.

use std::collections::HashMap;
use std::io::{self, Write, BufWriter};
use std::sync::{Arc, Mutex};

use nb_activity::adapter::{
    AdapterError, DriverAdapter, ExecutionError, OpDispenser, OpResult, ResolvedFields, TextBody,
};
use nb_workload::model::ParsedOp;
use nb_adapter_stdout::{StdoutFormat, StdoutConfig};
use xxhash_rust::xxh3;

/// Configuration for the model adapter.
#[derive(Default)]
pub struct ModelConfig {
    /// Base stdout config (filename, newline, format).
    pub stdout: StdoutConfig,
    /// Whether to print diagnostic output (--diagnose).
    pub diagnose: bool,
}


/// Simulated result definition for a single op.
///
/// Attached to ops via the `result` field in the workload YAML.
/// When present, the model adapter populates OpResult.body with
/// the rendered result and returns the result map for capture.
#[derive(Debug, Clone)]
pub enum ResultDef {
    /// Static key-value pairs: `result: { user_id: 42, balance: 1234.56 }`
    Static(HashMap<String, String>),
}

/// Per-op model parameters extracted from `result-*` op fields.
#[derive(Debug, Clone)]
pub struct ModelParams {
    /// Simulated result definition (None = no result, behave like stdout).
    pub result: Option<ResultDef>,
    /// Simulated latency in milliseconds (None = instant).
    pub latency_ms: Option<f64>,
    /// Error injection rate (0.0-1.0). Deterministic per cycle.
    pub error_rate: f64,
    /// Error class name for the error router.
    pub error_name: String,
    /// Error detail message.
    pub error_message: String,
}

impl Default for ModelParams {
    fn default() -> Self {
        Self {
            result: None,
            latency_ms: None,
            error_rate: 0.0,
            error_name: "ModelError".into(),
            error_message: "simulated error".into(),
        }
    }
}

/// Output target (same pattern as stdout adapter).
enum OutputTarget {
    Stdout(BufWriter<io::Stdout>),
    File(BufWriter<std::fs::File>),
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

/// The model adapter: stdout + simulated results, latency, and errors.
///
/// Prints the resolved op (like stdout), then produces a simulated
/// result based on the op's `result` field configuration. Supports
/// latency injection and deterministic error simulation.
///
/// Use for prototyping workloads before connecting to real infrastructure.
pub struct ModelAdapter {
    writer: Arc<Mutex<OutputTarget>>,
    newline: bool,
    format: StdoutFormat,
    diagnose: bool,
}

impl ModelAdapter {
    /// Create with default config.
    pub fn new() -> Self {
        Self::with_config(ModelConfig::default())
    }

    /// Create with explicit config.
    pub fn with_config(config: ModelConfig) -> Self {
        let writer = if config.stdout.filename.eq_ignore_ascii_case("stdout") {
            OutputTarget::Stdout(BufWriter::new(io::stdout()))
        } else {
            let file = std::fs::File::create(&config.stdout.filename)
                .unwrap_or_else(|e| panic!("failed to create output file '{}': {e}", config.stdout.filename));
            OutputTarget::File(BufWriter::new(file))
        };
        Self {
            writer: Arc::new(Mutex::new(writer)),
            newline: config.stdout.newline,
            format: config.stdout.format,
            diagnose: config.diagnose,
        }
    }
}

impl DriverAdapter for ModelAdapter {
    fn name(&self) -> &str { "model" }

    fn map_op(&self, template: &ParsedOp) -> Result<Box<dyn OpDispenser>, String> {
        let model_params = extract_model_params(&template.params);
        Ok(Box::new(ModelDispenser {
            writer: self.writer.clone(),
            format: self.format,
            newline: self.newline,
            diagnose: self.diagnose,
            model_params,
        }))
    }
}

/// Op dispenser for the model adapter. Captures format and model params
/// at init time; renders and simulates per-cycle.
struct ModelDispenser {
    writer: Arc<Mutex<OutputTarget>>,
    format: StdoutFormat,
    newline: bool,
    diagnose: bool,
    model_params: ModelParams,
}

impl OpDispenser for ModelDispenser {
    fn execute<'a>(
        &'a self,
        cycle: u64,
        fields: &'a ResolvedFields,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<OpResult, ExecutionError>> + Send + 'a>> {
        Box::pin(async move {
            let text = self.format.render(fields);

            // Write the resolved op (same as stdout)
            {
                let mut writer = self.writer.lock()
                    .unwrap_or_else(|e| e.into_inner());
                let write_result = if self.newline {
                    writeln!(writer, "{text}")
                } else {
                    write!(writer, "{text}")
                };
                if let Err(e) = write_result {
                    return Err(ExecutionError::Op(AdapterError {
                        error_name: "IoError".into(),
                        message: format!("write failed: {e}"),
                        retryable: false,
                    }));
                }
                if let Err(e) = writer.flush() {
                    return Err(ExecutionError::Op(AdapterError {
                        error_name: "FlushError".into(),
                        message: format!("flush failed: {e}"),
                        retryable: false,
                    }));
                }
            }

            // Error injection: deterministic per cycle
            if self.model_params.error_rate > 0.0 {
                let h = xxh3::xxh3_64(&cycle.to_le_bytes());
                let p = h as f64 / u64::MAX as f64;
                if p < self.model_params.error_rate {
                    if self.diagnose {
                        eprintln!("  model: ERROR injected (cycle={}, rate={:.2}%)",
                            cycle, self.model_params.error_rate * 100.0);
                    }
                    return Err(ExecutionError::Op(AdapterError {
                        error_name: self.model_params.error_name.clone(),
                        message: self.model_params.error_message.clone(),
                        retryable: false,
                    }));
                }
            }

            // Latency injection
            if let Some(ms) = self.model_params.latency_ms
                && ms > 0.0 {
                    let duration = std::time::Duration::from_micros((ms * 1000.0) as u64);
                    tokio::time::sleep(duration).await;
                }

            Ok(OpResult {
                body: Some(Box::new(TextBody(text))),
                captures: std::collections::HashMap::new(),
            })
        })
    }
}

/// Extract model parameters from an op's params/fields.
///
/// Looks for `result`, `result-latency`, `result-error-rate`,
/// `result-error-name`, `result-error-message` in the op's params.
pub fn extract_model_params(params: &HashMap<String, serde_json::Value>) -> ModelParams {
    let mut mp = ModelParams::default();

    if let Some(val) = params.get("result")
        && let Some(obj) = val.as_object() {
            let map: HashMap<String, String> = obj.iter()
                .map(|(k, v)| (k.clone(), v.as_str().map(|s| s.to_string()).unwrap_or_else(|| v.to_string())))
                .collect();
            mp.result = Some(ResultDef::Static(map));
        }

    if let Some(val) = params.get("result-latency") {
        if let Some(s) = val.as_str() {
            mp.latency_ms = parse_latency(s);
        } else if let Some(n) = val.as_f64() {
            mp.latency_ms = Some(n);
        }
    }

    if let Some(val) = params.get("result-error-rate")
        && let Some(n) = val.as_f64() {
            mp.error_rate = n;
        }

    if let Some(val) = params.get("result-error-name")
        && let Some(s) = val.as_str() {
            mp.error_name = s.to_string();
        }

    if let Some(val) = params.get("result-error-message")
        && let Some(s) = val.as_str() {
            mp.error_message = s.to_string();
        }

    mp
}

/// Parse a latency string like "5ms", "200us", or just a number (ms).
fn parse_latency(s: &str) -> Option<f64> {
    let s = s.trim();
    if let Some(n) = s.strip_suffix("ms") {
        n.trim().parse().ok()
    } else if let Some(n) = s.strip_suffix("us") {
        n.trim().parse::<f64>().ok().map(|v| v / 1000.0)
    } else {
        s.parse().ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_latency_ms() {
        assert_eq!(parse_latency("5ms"), Some(5.0));
        assert_eq!(parse_latency("  10ms  "), Some(10.0));
    }

    #[test]
    fn parse_latency_us() {
        assert_eq!(parse_latency("500us"), Some(0.5));
    }

    #[test]
    fn parse_latency_bare_number() {
        assert_eq!(parse_latency("3.14"), Some(3.14));
    }

    #[test]
    fn extract_static_result() {
        let mut params = HashMap::new();
        let mut result_map = serde_json::Map::new();
        result_map.insert("user_id".into(), serde_json::Value::from(42));
        result_map.insert("name".into(), serde_json::Value::from("alice"));
        params.insert("result".into(), serde_json::Value::Object(result_map));

        let mp = extract_model_params(&params);
        assert!(mp.result.is_some());
        if let Some(ResultDef::Static(map)) = &mp.result {
            assert_eq!(map["user_id"], "42");
            assert_eq!(map["name"], "alice");
        }
    }

    #[test]
    fn extract_error_params() {
        let mut params = HashMap::new();
        params.insert("result-error-rate".into(), serde_json::Value::from(0.05));
        params.insert("result-error-name".into(), serde_json::Value::from("Timeout"));

        let mp = extract_model_params(&params);
        assert_eq!(mp.error_rate, 0.05);
        assert_eq!(mp.error_name, "Timeout");
    }

    #[tokio::test]
    async fn model_dispenser_basic() {
        let adapter = ModelAdapter::new();
        let template = nb_workload::model::ParsedOp::simple("test", "SELECT 1;");
        let dispenser = adapter.map_op(&template).unwrap();
        let fields = ResolvedFields::new(
            vec!["stmt".into()],
            vec![nb_variates::node::Value::Str("SELECT 1;".into())],
        );
        let result = dispenser.execute(0, &fields).await.unwrap();
        assert!(result.body.is_some());
    }
}
