// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Diagnostic and debugging nodes.
//!
//! These are development aids, not hot-path nodes. They let users
//! inspect types and values flowing through the DAG.

use crate::node::{GkNode, NodeMeta, Port, PortType, Slot, Value};

/// Emit the type name of the input value as a string.
///
/// Signature: `(input: any) -> (String)`
///
/// Returns "u64", "f64", "bool", "String", or "bytes".
pub struct TypeOf {
    meta: NodeMeta,
    input_type: PortType,
}

impl TypeOf {
    pub fn for_u64() -> Self { Self::new(PortType::U64) }
    pub fn for_f64() -> Self { Self::new(PortType::F64) }
    pub fn for_str() -> Self { Self::new(PortType::Str) }
    pub fn for_bool() -> Self { Self::new(PortType::Bool) }

    pub fn new(input_type: PortType) -> Self {
        Self {
            meta: NodeMeta {
                name: "type_of".into(),
                outs: vec![Port::new("output", PortType::Str)],
                ins: vec![Slot::Wire(Port::new("input", input_type))],
            },
            input_type,
        }
    }
}

impl GkNode for TypeOf {
    fn meta(&self) -> &NodeMeta { &self.meta }

    fn eval(&self, _inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::Str(self.input_type.to_string());
    }
}

/// Emit the Rust Debug representation of the input value as a string.
///
/// Signature: `(input: any) -> (String)`
pub struct DebugRepr {
    meta: NodeMeta,
    _input_type: PortType,
}

impl DebugRepr {
    pub fn for_u64() -> Self { Self::new(PortType::U64) }
    pub fn for_f64() -> Self { Self::new(PortType::F64) }
    pub fn for_str() -> Self { Self::new(PortType::Str) }

    pub fn new(input_type: PortType) -> Self {
        Self {
            meta: NodeMeta {
                name: "debug_repr".into(),
                outs: vec![Port::new("output", PortType::Str)],
                ins: vec![Slot::Wire(Port::new("input", input_type))],
            },
            _input_type: input_type,
        }
    }
}

impl GkNode for DebugRepr {
    fn meta(&self) -> &NodeMeta { &self.meta }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::Str(format!("{:?}", inputs[0]));
    }
}

/// Passthrough that prints the value to stderr (for development).
///
/// Signature: `(input: u64) -> (u64)` (or any matching type)
///
/// The value passes through unchanged. A side-effect log line is
/// emitted to stderr with the node name, cycle value, and type.
pub struct Inspect {
    meta: NodeMeta,
    label: String,
}

impl Inspect {
    pub fn u64(label: impl Into<String>) -> Self {
        Self::new(label, PortType::U64)
    }

    pub fn f64(label: impl Into<String>) -> Self {
        Self::new(label, PortType::F64)
    }

    pub fn str(label: impl Into<String>) -> Self {
        Self::new(label, PortType::Str)
    }

    pub fn new(label: impl Into<String>, typ: PortType) -> Self {
        let label = label.into();
        Self {
            meta: NodeMeta {
                name: format!("inspect[{label}]"),
                outs: vec![Port::new("output", typ)],
                ins: vec![Slot::Wire(Port::new("input", typ))],
            },
            label,
        }
    }
}

impl GkNode for Inspect {
    fn meta(&self) -> &NodeMeta { &self.meta }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        eprintln!("[inspect:{}] {:?}", self.label, inputs[0]);
        outputs[0] = inputs[0].clone();
    }
}

// ---------------------------------------------------------------------------
// FFT / DFT analysis node
// ---------------------------------------------------------------------------

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
/// Not deterministic: uses interior mutability and file I/O.
pub struct FftAnalyzer {
    meta: NodeMeta,
    window_size: usize,
    buffer: std::sync::Mutex<Vec<f64>>,
    file: std::sync::Mutex<Option<std::io::BufWriter<std::fs::File>>>,
}

impl FftAnalyzer {
    /// Create a new FFT analyzer node.
    ///
    /// - `filename`: path to the JSONL output file (created/truncated on construction)
    /// - `window_size`: number of samples per DFT window (minimum 2)
    pub fn new(filename: &str, window_size: usize) -> Self {
        let window_size = window_size.max(2);
        let file = std::fs::File::create(filename).ok()
            .map(std::io::BufWriter::new);
        Self {
            meta: NodeMeta {
                name: "fft_analyze".into(),
                outs: vec![Port::u64("output")],
                ins: vec![Slot::Wire(Port::f64("signal"))],
            },
            window_size,
            buffer: std::sync::Mutex::new(Vec::with_capacity(window_size)),
            file: std::sync::Mutex::new(file),
        }
    }
}

impl GkNode for FftAnalyzer {
    fn meta(&self) -> &NodeMeta { &self.meta }

    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let signal = inputs[0].as_f64();

        let mut buf = self.buffer.lock().unwrap();
        let current_len = buf.len() as u64;
        outputs[0] = Value::U64(current_len);

        buf.push(signal);

        if buf.len() >= self.window_size {
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

            // Write JSONL line
            if let Ok(mut file_guard) = self.file.lock() {
                if let Some(ref mut writer) = *file_guard {
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
    }
}

// ---------------------------------------------------------------------------
// Signature declarations for the DSL registry
// ---------------------------------------------------------------------------

use crate::dsl::registry::{Arity, FuncCategory, FuncSig, ParamSpec};
use crate::node::SlotType;

/// Signatures for diagnostic and introspection nodes.
pub fn signatures() -> &'static [FuncSig] {
    use FuncCategory as C;
    &[
        FuncSig {
            name: "type_of", category: C::Diagnostic, outputs: 1,
            description: "emit type name as string",
            help: "Returns the runtime type name of the input value as a String.\nOutputs: \"U64\", \"F64\", \"Str\", \"Bool\", \"Bytes\", \"Json\", etc.\nUseful for debugging type mismatches in complex graphs.\nParameters:\n  input — any wire value",
            identity: None, variadic_ctor: None,
            params: &[ParamSpec { name: "input", slot_type: SlotType::Wire, required: true }],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "debug_repr", category: C::Diagnostic, outputs: 1,
            description: "emit Debug representation as string",
            help: "Returns the Rust Debug representation of the input value as a String.\nShows internal structure: U64(42), Str(\"hello\"), Bytes([0x01, ...]).\nMore detailed than type_of — use for inspecting actual values.\nParameters:\n  input — any wire value",
            identity: None, variadic_ctor: None,
            params: &[ParamSpec { name: "input", slot_type: SlotType::Wire, required: true }],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "inspect", category: C::Diagnostic, outputs: 1,
            description: "passthrough with stderr logging",
            help: "Passes the input value through unchanged while logging it to stderr.\nThe value is printed with its type and Debug repr on every evaluation.\nUse for live debugging during graph development — remove before production.\nParameters:\n  input — any wire value (passed through unmodified)",
            identity: None, variadic_ctor: None,
            params: &[ParamSpec { name: "input", slot_type: SlotType::Wire, required: true }],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
        FuncSig {
            name: "fft_analyze", category: C::Diagnostic, outputs: 1,
            description: "write FFT analysis of signal to JSONL file",
            help: "Collect N samples of an f64 signal, compute DFT magnitudes\nand phases, write to a JSONL file. Each line is one window.\nMore windows = higher confidence in frequency content.\nParams: filename (ConstStr), window_size (ConstU64, default 256)",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "signal", slot_type: SlotType::Wire, required: true },
                ParamSpec { name: "filename", slot_type: SlotType::ConstStr, required: true },
                ParamSpec { name: "window_size", slot_type: SlotType::ConstU64, required: false },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
    ]
}

/// Try to build a diagnostic node from a function name and const args.
///
/// Returns `None` if the name is not handled by this module.
pub(crate) fn build_node(name: &str, _wires: &[crate::assembly::WireRef], consts: &[crate::dsl::factory::ConstArg]) -> Option<Result<Box<dyn crate::node::GkNode>, String>> {
    match name {
        "type_of" => Some(Ok(Box::new(TypeOf::for_u64()))),
        "inspect" => Some(Ok(Box::new(Inspect::u64("inspect")))),
        "debug_repr" => Some(Ok(Box::new(DebugRepr::new(crate::node::PortType::U64)))),
        "fft_analyze" => {
            let filename = consts.first().map(|c| c.as_str()).unwrap_or("fft.jsonl");
            let window = consts.get(1).map(|c| c.as_u64()).unwrap_or(256) as usize;
            Some(Ok(Box::new(FftAnalyzer::new(filename, window))))
        }
        _ => None,
    }
}


crate::register_nodes!(signatures, build_node);
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn type_of_u64() {
        let node = TypeOf::for_u64();
        let mut out = [Value::None];
        node.eval(&[Value::U64(42)], &mut out);
        assert_eq!(out[0].as_str(), "u64");
    }

    #[test]
    fn type_of_f64() {
        let node = TypeOf::for_f64();
        let mut out = [Value::None];
        node.eval(&[Value::F64(3.14)], &mut out);
        assert_eq!(out[0].as_str(), "f64");
    }

    #[test]
    fn type_of_str() {
        let node = TypeOf::for_str();
        let mut out = [Value::None];
        node.eval(&[Value::Str("hello".into())], &mut out);
        assert_eq!(out[0].as_str(), "String");
    }

    #[test]
    fn debug_repr_u64() {
        let node = DebugRepr::for_u64();
        let mut out = [Value::None];
        node.eval(&[Value::U64(42)], &mut out);
        assert_eq!(out[0].as_str(), "U64(42)");
    }

    #[test]
    fn debug_repr_str() {
        let node = DebugRepr::for_str();
        let mut out = [Value::None];
        node.eval(&[Value::Str("hello".into())], &mut out);
        assert!(out[0].as_str().contains("hello"));
    }

    #[test]
    fn inspect_passthrough() {
        let node = Inspect::u64("test");
        let mut out = [Value::None];
        node.eval(&[Value::U64(42)], &mut out);
        assert_eq!(out[0].as_u64(), 42);
    }

    #[test]
    fn fft_analyzer_collects_and_writes() {
        let tmp = std::env::temp_dir().join("test_fft_diag.jsonl");
        let path = tmp.to_str().unwrap();
        let node = FftAnalyzer::new(path, 4);
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
