// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Diagnostic and debugging nodes.
//!
//! These are development aids, not hot-path nodes. They let users
//! inspect types and values flowing through the DAG.
//!
//! SRD-80b S8: `fft_analyze` migrated from a hand-written `impl
//! PolydatNode for FftAnalyzer` to `#[polydat_node]` form. The
//! cross-call eval state (buffer + lazy-open output file) lives
//! in struct fields derived via `#[poly_const]` setup functions —
//! one buffer setup keyed on `window_size`, one output setup
//! keyed on `filename`. Lazy file open is preserved so
//! describe/probe/dryrun paths that never feed samples leave
//! nothing behind.

use crate::ast::Value;

/// Emit the type name of the input value as a string.
///
/// Signature: `(input: any) -> (String)`
///
/// Returns "u64", "f64", "bool", "String", or "bytes".
/// Return the input's port type as a string. SRD-80 PR B.8 —
/// PolyWire input, Fixed Str output.
#[crate::polydat_node(category = Diagnostic)]
fn type_of(input: Value) -> String {
    input.port_type().to_string()
}

/// Emit the Rust Debug representation of the input value.
#[crate::polydat_node(category = Diagnostic)]
fn debug_repr(input: Value) -> String {
    format!("{input:?}")
}

/// Passthrough that prints the value (with a const label) to
/// stderr. SameAsInput output — runtime port type preserved.
#[crate::polydat_node(category = Diagnostic, purity = SideChannel(Stderr))]
fn inspect(
    input: Value,
    #[poly_default("inspect")] label: crate::derive_support::Const<&str>,
) -> Value {
    eprintln!("[inspect:{}] {input:?}", label.0);
    input
}

// ---------------------------------------------------------------------------
// FFT / DFT analysis node
// ---------------------------------------------------------------------------

/// Wraps the lazily-opened output file plus the path it was
/// configured with. Construction stores the path; the file is
/// opened on the first window emit so probes / dryruns that
/// never feed samples don't leave empty artifacts behind.
struct FftOutput {
    path: String,
    writer: Option<std::io::BufWriter<std::fs::File>>,
    open_attempted: bool,
}

impl crate::derive_support::PolydatSetup for std::sync::Mutex<Vec<f64>> {}
impl crate::derive_support::PolydatSetup for std::sync::Mutex<FftOutput> {}

/// Build the per-window signal buffer for the given window size.
/// `window_size` is clamped to a minimum of 2 (DFT below that is
/// degenerate). Returned as a `Mutex<Vec<f64>>` so the eval body
/// can mutate across calls while remaining Send+Sync.
fn fft_buffer(window_size: u64) -> std::sync::Mutex<Vec<f64>> {
    let cap = window_size.max(2) as usize;
    std::sync::Mutex::new(Vec::with_capacity(cap))
}

/// Stash the output path without opening the file. Lazy-open
/// happens on the first window emit so describe/probe/dryrun
/// paths that construct the node without ever feeding samples
/// leave nothing behind.
fn fft_output(filename: &str) -> std::sync::Mutex<FftOutput> {
    std::sync::Mutex::new(FftOutput {
        path: filename.to_string(),
        writer: None,
        open_attempted: false,
    })
}

/// Collect values over N cycles and write DFT analysis to a JSONL file.
///
/// Signature: `fft_analyze(signal: f64, filename: str, window_size: u64) -> (u64)`
///
/// This is a diagnostic node with side effects (file I/O). It buffers
/// N f64 signal values, computes a discrete Fourier transform when the
/// buffer fills, writes one JSONL line with magnitudes, phases, DC
/// component, and fundamental frequency, then clears the buffer.
///
/// The output is a passthrough of the current buffer length (how many
/// samples have been collected in the current window).
///
/// Declared `Nondeterministic` — the per-cycle signal buffer
/// accumulates across calls (return value depends on prior
/// history), and every window emit writes a JSONL line to the
/// configured file path. The eval-spanning state is the
/// load-bearing aspect.
#[crate::polydat_node(
    category = Diagnostic,
    purity = Nondeterministic("accumulates signal buffer across calls; writes JSONL on window emit"),
)]
fn fft_analyze(
    signal: f64,
    #[poly_default("fft.jsonl")] filename: crate::derive_support::Const<&str>,
    #[poly_default(256u64)] window_size: crate::derive_support::Const<u64>,
    #[poly_const(fft_buffer, from = window_size)]
    buffer: &std::sync::Mutex<Vec<f64>>,
    #[poly_const(fft_output, from = filename)]
    output: &std::sync::Mutex<FftOutput>,
) -> u64 {
    let _ = filename; // const value stashed in `output` at setup; not used at eval
    let window = (*window_size).max(2) as usize;

    let mut buf = buffer.lock().unwrap();
    let current_len = buf.len() as u64;

    buf.push(signal);

    if buf.len() >= window {
        // Compute DFT
        let n = buf.len();
        let mut magnitudes = Vec::with_capacity(n / 2 + 1);
        let mut phases = Vec::with_capacity(n / 2 + 1);

        for k in 0..=(n / 2) {
            let mut re = 0.0f64;
            let mut im = 0.0f64;
            for (i, &x) in buf.iter().enumerate() {
                let angle = -2.0 * std::f64::consts::PI * (k as f64) * (i as f64) / (n as f64);
                re += x * angle.cos();
                im += x * angle.sin();
            }
            magnitudes.push((re * re + im * im).sqrt() / n as f64);
            phases.push(im.atan2(re));
        }

        // Write JSONL line. The output file is lazy-opened
        // here on first use; a previous open failure (bad
        // path, permissions) is sticky for the lifetime of
        // the node so we don't retry on every window.
        if let Ok(mut out) = output.lock() {
            if !out.open_attempted {
                out.open_attempted = true;
                out.writer = std::fs::File::create(&out.path).ok()
                    .map(std::io::BufWriter::new);
            }
            if let Some(ref mut writer) = out.writer {
                use std::io::Write;
                let json = serde_json::json!({
                    "window_size": n,
                    "magnitudes": magnitudes,
                    "phases": phases,
                    "dc": magnitudes.first().copied().unwrap_or(0.0),
                    "fundamental": magnitudes.get(1).copied().unwrap_or(0.0),
                });
                let _ = writeln!(writer, "{}", json);
                let _ = writer.flush();
            }
        }

        buf.clear();
    }

    current_len
}

// SRD-80 PR B.8 / SRD-80b S8 — every node in this module is
// registered link-time via the proc-macro-emitted
// `NodeRegistration`. The hand-maintained
// `signatures()` / `build_node()` / `register_nodes!` plumbing
// is retired.

#[cfg(test)]
mod tests {
    use super::*;

    use crate::ast::{PolydatNode, PortType};

    #[test]
    fn type_of_u64() {
        let node = TypeOf::new(PortType::U64);
        let mut out = [Value::None];
        node.eval(&[Value::U64(42)], &mut out);
        assert_eq!(out[0].as_str(), "u64");
    }

    #[test]
    fn type_of_f64() {
        let node = TypeOf::new(PortType::F64);
        let mut out = [Value::None];
        node.eval(&[Value::F64(3.14)], &mut out);
        assert_eq!(out[0].as_str(), "f64");
    }

    #[test]
    fn type_of_str() {
        let node = TypeOf::new(PortType::Str);
        let mut out = [Value::None];
        node.eval(&[Value::Str("hello".into())], &mut out);
        assert_eq!(out[0].as_str(), "String");
    }

    #[test]
    fn debug_repr_u64() {
        let node = DebugRepr::new(PortType::U64);
        let mut out = [Value::None];
        node.eval(&[Value::U64(42)], &mut out);
        assert_eq!(out[0].as_str(), "U64(42)");
    }

    #[test]
    fn debug_repr_str() {
        let node = DebugRepr::new(PortType::Str);
        let mut out = [Value::None];
        node.eval(&[Value::Str("hello".into())], &mut out);
        assert!(out[0].as_str().contains("hello"));
    }

    #[test]
    fn inspect_passthrough() {
        let node = Inspect::new(PortType::U64, "test".to_string());
        let mut out = [Value::None];
        node.eval(&[Value::U64(42)], &mut out);
        assert_eq!(out[0].as_u64(), 42);
    }

    #[test]
    fn fft_analyzer_collects_and_writes() {
        let tmp = std::env::temp_dir().join("test_fft_diag.jsonl");
        let path = tmp.to_str().unwrap();
        let node = FftAnalyze::new(path.to_string(), 4u64);
        let mut out = [Value::None];

        // Feed 4 samples: a simple DC signal of 1.0
        for i in 0..4 {
            node.eval(&[Value::F64(1.0)], &mut out);
            // Output is the buffer length before this push
            assert_eq!(out[0].as_u64(), i as u64);
        }

        // After 4 samples, buffer should have been flushed
        // Next eval should show buffer len 0 again
        node.eval(&[Value::F64(1.0)], &mut out);
        assert_eq!(out[0].as_u64(), 0);

        // Verify the JSONL file was written
        let contents = std::fs::read_to_string(path).unwrap();
        assert!(!contents.is_empty(), "JSONL file should not be empty");
        let line: serde_json::Value = serde_json::from_str(contents.lines().next().unwrap()).unwrap();
        assert_eq!(line["window_size"], 4);
        // DC component of constant 1.0 signal should be ~1.0/4 * 4 = 1.0
        // Actually our normalization divides by n, so DC = sum/n = 1.0
        let dc = line["dc"].as_f64().unwrap();
        assert!((dc - 1.0).abs() < 0.001, "DC component of constant signal should be ~1.0, got {dc}");

        // Clean up
        let _ = std::fs::remove_file(path);
    }
}
