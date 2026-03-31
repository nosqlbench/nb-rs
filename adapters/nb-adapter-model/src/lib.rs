// Copyright 2024-2026 nosqlbench contributors
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
use std::sync::Mutex;
use nb_activity::adapter::{Adapter, AdapterError, AssembledOp, OpResult};
use nb_adapter_stdout::{StdoutFormat, StdoutConfig};
use xxhash_rust::xxh3;

/// Configuration for the model adapter.
pub struct ModelConfig {
    /// Base stdout config (filename, newline, format).
    pub stdout: StdoutConfig,
    /// Whether to print diagnostic output (--diagnose).
    pub diagnose: bool,
}

impl Default for ModelConfig {
    fn default() -> Self {
        Self {
            stdout: StdoutConfig::default(),
            diagnose: false,
        }
    }
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
    // Future: GkSource(String) — GK kernel for computed results
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
/// Prints the assembled op (like stdout), then produces a simulated
/// result based on the op's `result` field configuration. Supports
/// latency injection and deterministic error simulation.
///
/// Use for prototyping workloads before connecting to real infrastructure.
pub struct ModelAdapter {
    writer: Mutex<OutputTarget>,
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
            writer: Mutex::new(writer),
            newline: config.stdout.newline,
            format: config.stdout.format,
            diagnose: config.diagnose,
        }
    }
}

impl ModelAdapter {
    /// Execute with model parameters for latency/error injection.
    ///
    /// Call this from the executor when model params have been extracted
    /// from the op template's `result-*` fields.
    pub async fn execute_with_params(
        &self,
        op: &AssembledOp,
        params: &ModelParams,
        cycle: u64,
    ) -> Result<OpResult, AdapterError> {
        let text = self.format.render(op);

        // Write the assembled op (same as stdout)
        {
            let mut writer = self.writer.lock().unwrap();
            if self.newline {
                let _ = writeln!(writer, "{text}");
            } else {
                let _ = write!(writer, "{text}");
            }
            let _ = writer.flush();
        }

        // Error injection: deterministic per cycle
        if params.error_rate > 0.0 {
            // Hash the cycle to get a deterministic float in [0, 1)
            let h = xxh3::xxh3_64(&cycle.to_le_bytes());
            let p = h as f64 / u64::MAX as f64;
            if p < params.error_rate {
                if self.diagnose {
                    eprintln!("  model [{}]: ERROR injected (cycle={}, rate={:.2}%)",
                        op.name, cycle, params.error_rate * 100.0);
                }
                return Err(AdapterError {
                    error_name: params.error_name.clone(),
                    message: params.error_message.clone(),
                });
            }
        }

        // Latency injection
        if let Some(ms) = params.latency_ms {
            if ms > 0.0 {
                let duration = std::time::Duration::from_micros((ms * 1000.0) as u64);
                tokio::time::sleep(duration).await;
                if self.diagnose {
                    eprintln!("  model [{}]: latency {:.1}ms injected", op.name, ms);
                }
            }
        }

        // Diagnostic output
        if self.diagnose {
            let result_desc = match &params.result {
                Some(ResultDef::Static(map)) => {
                    let pairs: Vec<String> = map.iter()
                        .map(|(k, v)| format!("{k}={v}"))
                        .collect();
                    pairs.join(", ")
                }
                None => "(none)".into(),
            };
            eprintln!("  model [{}]: result=[{}]", op.name, result_desc);
        }

        Ok(OpResult {
            success: true,
            status: 0,
            body: Some(text),
        })
    }
}

impl Adapter for ModelAdapter {
    fn execute(&self, op: &AssembledOp) -> impl std::future::Future<Output = Result<OpResult, AdapterError>> + Send {
        let text = self.format.render(op);
        let newline = self.newline;
        let diagnose = self.diagnose;
        let op_name = op.name.clone();

        // Write the assembled op (same as stdout)
        {
            let mut writer = self.writer.lock().unwrap();
            if newline {
                let _ = writeln!(writer, "{text}");
            } else {
                let _ = write!(writer, "{text}");
            }
            let _ = writer.flush();
        }

        if diagnose {
            eprintln!("  model [{}]: stmt rendered, {} fields", op_name, op.fields.len());
        }

        async move {
            Ok(OpResult {
                success: true,
                status: 0,
                body: Some(text),
            })
        }
    }
}

/// Extract model parameters from an op's params/fields.
///
/// Looks for `result`, `result-latency`, `result-error-rate`,
/// `result-error-name`, `result-error-message` in the op's params.
pub fn extract_model_params(params: &HashMap<String, serde_json::Value>) -> ModelParams {
    let mut mp = ModelParams::default();

    if let Some(val) = params.get("result") {
        if let Some(obj) = val.as_object() {
            let map: HashMap<String, String> = obj.iter()
                .map(|(k, v)| (k.clone(), v.as_str().map(|s| s.to_string()).unwrap_or_else(|| v.to_string())))
                .collect();
            mp.result = Some(ResultDef::Static(map));
        }
        // Future: if val.is_string(), parse as GK source
    }

    if let Some(val) = params.get("result-latency") {
        if let Some(s) = val.as_str() {
            mp.latency_ms = parse_latency(s);
        } else if let Some(n) = val.as_f64() {
            mp.latency_ms = Some(n);
        }
    }

    if let Some(val) = params.get("result-error-rate") {
        if let Some(n) = val.as_f64() {
            mp.error_rate = n;
        }
    }

    if let Some(val) = params.get("result-error-name") {
        if let Some(s) = val.as_str() {
            mp.error_name = s.to_string();
        }
    }

    if let Some(val) = params.get("result-error-message") {
        if let Some(s) = val.as_str() {
            mp.error_message = s.to_string();
        }
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
    async fn model_adapter_basic() {
        let adapter = ModelAdapter::new();
        let op = AssembledOp {
            name: "test".into(),
            fields: {
                let mut m = HashMap::new();
                m.insert("stmt".into(), "SELECT 1;".into());
                m
            },
        };
        let result = adapter.execute(&op).await.unwrap();
        assert!(result.success);
        assert!(result.body.is_some());
    }
}
