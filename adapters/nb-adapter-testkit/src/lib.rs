// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Model adapter: simulates operation execution for workload prototyping.
//!
//! Extends the stdout adapter with:
//! - Simulated results via the `result` op field (static map or GK kernel)
//! - Latency simulation via `result-latency`
//! - Deterministic error injection via `result-error-rate`
//! - Backend saturation simulation via `result-capacity` / `result-overload`
//! - Diagnostic output via `--diagnose`
//!
//! When no `result` field is present, behaves identically to stdout.
//! See SRD 29 for the full design.
//!
//! ## Oversaturation modeling
//!
//! Two op-level knobs let a workload simulate a capacity-limited
//! backend so operators can *see* the effect of pushing concurrency
//! past what the simulated server can absorb:
//!
//! * `result-capacity = N` — soft cap implemented as a
//!   [`tokio::sync::Semaphore`] with `N` permits. Ops acquire a
//!   permit before the latency sleep and release on completion, so
//!   up to `N` ops are serviced concurrently; the rest queue. As the
//!   caller's `concurrency` control climbs past `N`, the per-op
//!   wait time climbs with it (classic M/M/c queueing behavior)
//!   while throughput tops out at `N / result-latency`.
//!
//! * `result-overload = M` — hard threshold. If the number of
//!   in-flight ops (after acquiring a permit) exceeds `M`, the op is
//!   rejected with an `Overload` error instead of being serviced.
//!   Use alongside `capacity` to model a backend that queues up to a
//!   point and then starts returning errors — which is what the
//!   `dynamic_controls` example's feedback loop watches for so it
//!   can throttle the rate back.
//!
//! Both are per-op: different ops simulate independent backends.

use std::collections::HashMap;
use std::io::{self, Write, BufWriter};
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Semaphore;

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
    /// Simulated backend concurrency cap. `Some(n)` installs an n-permit
    /// [`Semaphore`] the op must acquire before its latency sleep, so ops
    /// above `n` queue — throughput caps at `n / latency_ms` and per-op
    /// latency grows with caller concurrency. `None` = unlimited.
    pub capacity: Option<usize>,
    /// In-flight threshold above which the op is rejected with an
    /// `Overload` error instead of being serviced. Checked *after* a
    /// permit is acquired, so this represents "too many already
    /// being served." `None` = no overload rejection.
    pub overload: Option<usize>,
}

impl Default for ModelParams {
    fn default() -> Self {
        Self {
            result: None,
            latency_ms: None,
            error_rate: 0.0,
            error_name: "ModelError".into(),
            error_message: "simulated error".into(),
            capacity: None,
            overload: None,
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
    fn name(&self) -> &str { "testkit" }

    fn map_op(&self, template: &ParsedOp) -> Result<Box<dyn OpDispenser>, String> {
        // The yaml parser routes unknown top-level op keys into
        // `template.op`, while a nested `params:` block lands in
        // `template.params`. Both are valid ways to declare
        // `result-*` fields, so merge them — `params` wins on
        // collision because an explicit `params:` block is the
        // stronger user intent.
        let mut merged = template.op.clone();
        for (k, v) in &template.params {
            merged.insert(k.clone(), v.clone());
        }
        let model_params = extract_model_params(&merged);
        // Per-op semaphore: independent ops simulate independent
        // backends, each with their own capacity ceiling.
        let semaphore = model_params.capacity.map(|n| Arc::new(Semaphore::new(n)));
        Ok(Box::new(ModelDispenser {
            writer: self.writer.clone(),
            format: self.format,
            newline: self.newline,
            diagnose: self.diagnose,
            model_params,
            semaphore,
            in_flight: Arc::new(AtomicUsize::new(0)),
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
    /// Simulated-backend permit pool. `Some` when `result-capacity` is
    /// set; held for the full service time so waiting ops observe
    /// queueing delay. `None` = no capacity limit.
    semaphore: Option<Arc<Semaphore>>,
    /// Count of ops currently being serviced (after permit acquire,
    /// before latency-sleep completion). Used for the overload check
    /// and diagnostic output.
    in_flight: Arc<AtomicUsize>,
}

/// RAII guard that decrements the in-flight counter on drop, so the
/// count stays accurate across every exit path (success, error
/// injection, overload rejection, panic).
struct InFlightGuard(Arc<AtomicUsize>);

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

impl OpDispenser for ModelDispenser {
    fn execute<'a>(
        &'a self,
        cycle: u64,
        fields: &'a ResolvedFields,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<OpResult, ExecutionError>> + Send + 'a>> {
        Box::pin(async move {
            let text = self.format.render(fields, ",");

            // Write the resolved op (same as stdout). Done before
            // any saturation simulation so the trace reflects the
            // op the caller issued, regardless of how it resolved.
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

            // Track occupancy (waiting + serving) BEFORE taking a
            // permit so `overload` can measure the real pressure on
            // the simulated backend — including ops currently stuck
            // in the queue, not just the ones being serviced.
            let current = self.in_flight.fetch_add(1, Ordering::AcqRel) + 1;
            let _guard = InFlightGuard(self.in_flight.clone());

            // Overload rejection: models a backend that accepts a
            // bounded queue and rejects anything beyond it. The
            // check runs before acquiring a permit so overloaded
            // requests never consume service capacity — they fail
            // fast, which matches what real backends do when their
            // inbound buffer fills. Marked retryable so error
            // handlers can back off instead of surfacing every
            // rejection.
            if let Some(threshold) = self.model_params.overload
                && current > threshold {
                if self.diagnose {
                    eprintln!("  testkit: OVERLOAD cycle={cycle} in_flight={current} threshold={threshold}");
                }
                return Err(ExecutionError::Op(AdapterError {
                    error_name: "Overload".into(),
                    message: format!("simulated overload: in_flight={current} > {threshold}"),
                    retryable: true,
                }));
            }

            // Simulated-backend admission. Any await here counts
            // toward the caller's observed latency, which is the
            // point — queueing delay is what the caller sees when
            // the backend is oversaturated.
            let _permit = match &self.semaphore {
                Some(sem) => Some(sem.clone().acquire_owned().await
                    .map_err(|e| ExecutionError::Op(AdapterError {
                        error_name: "SemaphoreClosed".into(),
                        message: format!("testkit semaphore closed: {e}"),
                        retryable: false,
                    }))?),
                None => None,
            };

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

            // Service time. Happens *while holding the permit*, so
            // later ops wait in the semaphore queue rather than
            // racing through.
            if let Some(ms) = self.model_params.latency_ms
                && ms > 0.0 {
                    let duration = std::time::Duration::from_micros((ms * 1000.0) as u64);
                    tokio::time::sleep(duration).await;
                }

            Ok(OpResult {
                body: Some(Box::new(TextBody(text))),
                captures: std::collections::HashMap::new(),
                skipped: false,
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

    if let Some(n) = params.get("result-capacity").and_then(parse_usize_param)
        && n > 0 {
            mp.capacity = Some(n);
        }

    if let Some(n) = params.get("result-overload").and_then(parse_usize_param)
        && n > 0 {
            mp.overload = Some(n);
        }

    mp
}

/// Parse a positive integer from a JSON value, accepting either a
/// native number or a numeric string (so YAML `"4"` and `4` both work).
fn parse_usize_param(v: &serde_json::Value) -> Option<usize> {
    if let Some(n) = v.as_u64() {
        return usize::try_from(n).ok();
    }
    if let Some(s) = v.as_str() {
        return s.trim().parse().ok();
    }
    None
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

    #[test]
    fn extract_saturation_params() {
        let mut params = HashMap::new();
        params.insert("result-capacity".into(), serde_json::Value::from(4));
        params.insert("result-overload".into(), serde_json::Value::from("16"));

        let mp = extract_model_params(&params);
        assert_eq!(mp.capacity, Some(4));
        assert_eq!(mp.overload, Some(16));
    }

    #[test]
    fn saturation_zero_disables() {
        // A literal zero means "no limit," so the user doesn't have
        // to remove the field to unset it.
        let mut params = HashMap::new();
        params.insert("result-capacity".into(), serde_json::Value::from(0));
        params.insert("result-overload".into(), serde_json::Value::from(0));

        let mp = extract_model_params(&params);
        assert_eq!(mp.capacity, None);
        assert_eq!(mp.overload, None);
    }

    #[tokio::test]
    async fn overload_rejects_when_in_flight_exceeds_threshold() {
        // Capacity 1, overload 2: exactly 2 ops fit (1 serving + 1
        // queued). A third concurrent op must reject with Overload.
        let adapter = ModelAdapter::new();
        let mut op = nb_workload::model::ParsedOp::simple("test", "SELECT 1;");
        op.params.insert("result-latency".into(), serde_json::Value::from("50ms"));
        op.params.insert("result-capacity".into(), serde_json::Value::from(1));
        op.params.insert("result-overload".into(), serde_json::Value::from(2));

        let dispenser: Arc<dyn OpDispenser> = Arc::from(adapter.map_op(&op).unwrap());
        let fields = Arc::new(ResolvedFields::new(
            vec!["stmt".into()],
            vec![nb_variates::node::Value::Str("SELECT 1;".into())],
        ));

        let mut handles = Vec::new();
        for cycle in 0..3u64 {
            let d = dispenser.clone();
            let f = fields.clone();
            handles.push(tokio::spawn(async move {
                d.execute(cycle, &f).await
            }));
        }
        let mut overload_count = 0usize;
        for h in handles {
            let res = h.await.expect("task panicked");
            if let Err(ExecutionError::Op(e)) = &res
                && e.error_name == "Overload" {
                overload_count += 1;
            }
        }
        assert_eq!(overload_count, 1,
            "expected exactly one op to be rejected with Overload, got {overload_count}");
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

// =========================================================================
// Adapter Registration (inventory-based, link-time)
// =========================================================================

inventory::submit! {
    nb_activity::adapter::AdapterRegistration {
        names: || &["testkit"],
        known_params: || &[
            "result", "result-latency",
            "result-error-rate", "result-error-name", "result-error-message",
            "result-capacity", "result-overload",
        ],
        display_preference: || nb_activity::adapter::DisplayPreference::Auto,
        create: |params| Box::pin(async move {
            Ok(std::sync::Arc::new(ModelAdapter::with_config(ModelConfig {
                stdout: StdoutConfig::from_params(&params),
                diagnose: false,
            })) as std::sync::Arc<dyn nb_activity::adapter::DriverAdapter>)
        }),
    }
}
