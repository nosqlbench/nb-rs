// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Structural type-round-trip lint (C6b; governing principle:
//! `feedback_no_auto_string_conversion`).
//!
//! Detects DAG paths where a value's type is modulated to another type
//! and then restored — `T → Y → … → T` — purely through conversion /
//! formatting machinery during synthesis. Native types must stay
//! native through data passing; a round trip means some hand-off point
//! was expressed in a foreign type (usually text) and re-parsed, which
//! loses type fidelity, costs work, and turns downstream type checks
//! into liars. The canonical instance is the retired identity-shadow
//! bug (`VecF32 → printf → Str → parse → VecF32`); this lint subsumes
//! that string case in one structural rule.
//!
//! **Classification is derived from the adapter catalogs themselves**
//! ([`boundary_adapter`] enumerated over the closed `PortType`
//! surface), so the conversion-node registry cannot drift from the
//! catalog. Formatting combinators (`printf`, `str_concat`,
//! `select_str`) and `identity` are *carriers*: walks pass through
//! them to find the typed value that entered the text domain.
//!
//! **Sanctioned intermediaries are exempt**: a restore FROM `Json` is
//! by-design (JSON is a declared text hand-off format), so
//! `T → Json → T` is never reported.
//!
//! Severity: one [`RoundTripFinding`] per restoring node; the caller
//! (assembly `resolve`) reports findings as compile warnings by
//! default and as a hard error under strict-values mode.

use std::collections::{HashMap, HashSet};

use crate::ast::{PolydatNode, PortType};
use crate::compile::assembly::boundary_adapter;
use crate::kernel::{InputDef, WireSource};

/// One detected round trip: `restored` left the native domain through
/// `departure_node` (or entered a formatting carrier natively) and was
/// restored by `restore_node` via the `via` type.
#[derive(Debug, Clone)]
pub struct RoundTripFinding {
    pub restored: PortType,
    pub via: PortType,
    pub departure_node: String,
    pub restore_node: String,
}

impl RoundTripFinding {
    /// Operator-facing message: names both ends and the principle.
    pub fn message(&self) -> String {
        format!(
            "type round trip: a {restored:?} value is modulated to {via:?} \
             (at '{dep}') and restored to {restored:?} (at '{res}') — native \
             types must stay native through data passing; render to text only \
             at presentation points, or hand off via a by-design intermediary \
             (JSON)",
            restored = self.restored,
            via = self.via,
            dep = self.departure_node,
            res = self.restore_node,
        )
    }
}

/// The closed `PortType` surface the catalogs cover (SIMD register
/// types carry no adapters and are excluded). Adding a variant here is
/// only ever a lint-coverage improvement — omission under-lints, never
/// mis-lints, because classification still comes from the catalog.
const LINTABLE_TYPES: [PortType; 26] = [
    PortType::U64, PortType::F64, PortType::U32, PortType::I32,
    PortType::I64, PortType::F32, PortType::U8, PortType::I8,
    PortType::U16, PortType::I16, PortType::F16, PortType::U128,
    PortType::I128, PortType::Bool, PortType::Str, PortType::Bytes,
    PortType::Json, PortType::Ext, PortType::Handle,
    PortType::VecF32, PortType::VecI32, PortType::VecF64,
    PortType::VecI64, PortType::VecF16, PortType::VecI16, PortType::VecI8,
];

/// Conversion-node registry: node meta-name → (from, to), enumerated
/// once from the boundary catalog (the superset — every auto adapter
/// is also a boundary adapter). Cached process-wide; the catalog is
/// static.
fn conversion_registry() -> &'static HashMap<String, (PortType, PortType)> {
    static REG: std::sync::OnceLock<HashMap<String, (PortType, PortType)>> =
        std::sync::OnceLock::new();
    REG.get_or_init(|| {
        let mut m = HashMap::new();
        for from in LINTABLE_TYPES {
            for to in LINTABLE_TYPES {
                if from == to {
                    continue;
                }
                if let Some(node) = boundary_adapter(from, to) {
                    m.insert(node.meta().name.clone(), (from, to));
                }
            }
        }
        m
    })
}

/// Formatting / passthrough carriers the walk looks through. These
/// nodes move a value between edges without changing its *information
/// identity* (identity) or combine typed values into text
/// (formatters) — the walk continues into their inputs to find the
/// native value that entered the modulated domain.
fn is_carrier(name: &str) -> bool {
    matches!(name, "printf" | "str_concat" | "select_str" | "identity")
}

/// Run the lint over a resolved DAG (topologically sorted nodes +
/// wiring, as built at the end of assembly `resolve`). Returns one
/// finding per restoring conversion node that closes a round trip.
/// `pub(crate)`: the only sanctioned caller is assembly `resolve`
/// (walled-off chokepoint); hosts observe findings as compile
/// warnings / strict errors, never by re-running the pass.
pub(crate) fn lint_type_round_trips(
    nodes: &[Box<dyn PolydatNode>],
    wiring: &[Vec<WireSource>],
    input_defs: &[InputDef],
) -> Vec<RoundTripFinding> {
    let registry = conversion_registry();
    let mut findings = Vec::new();

    for (i, node) in nodes.iter().enumerate() {
        let Some(&(via, restored)) = registry.get(&node.meta().name) else {
            continue;
        };
        // A restore FROM Json is a by-design hand-off — sanctioned.
        if via == PortType::Json {
            continue;
        }
        // Walk upstream from the restorer's wire input, through
        // conversion chains and carriers, looking for the same type
        // leaving the native domain.
        let mut visited: HashSet<usize> = HashSet::new();
        let mut stack: Vec<&WireSource> = wiring[i].iter().collect();
        let mut departure: Option<String> = None;
        while let Some(ws) = stack.pop() {
            let WireSource::NodeOutput(up, _) = ws else {
                continue; // a raw input is an origin, not a modulation
            };
            if !visited.insert(*up) {
                continue;
            }
            let up_meta = nodes[*up].meta();
            if let Some(&(dep_from, _dep_to)) = registry.get(&up_meta.name) {
                if dep_from == restored {
                    // The same type left the native domain upstream —
                    // the chain between is pure conversion machinery.
                    departure = Some(up_meta.name.clone());
                    break;
                }
                // A different conversion in the chain: keep walking
                // through it (multi-hop modulation, e.g. T→Y→Y'→T).
                stack.extend(wiring[*up].iter());
            } else if is_carrier(&up_meta.name) {
                // A formatter/passthrough: if any of its inputs is
                // natively the restored type, the carrier is where
                // the value entered the modulated domain.
                for cw in &wiring[*up] {
                    let t = source_type(cw, nodes, input_defs);
                    if t == Some(restored) {
                        departure = Some(up_meta.name.clone());
                        break;
                    }
                }
                if departure.is_some() {
                    break;
                }
                stack.extend(wiring[*up].iter());
            }
            // Any other node kind is semantic computation — the walk
            // stops there; a value produced by real computation in the
            // via-type domain is not a round trip.
        }
        if let Some(dep) = departure {
            findings.push(RoundTripFinding {
                restored,
                via,
                departure_node: dep,
                restore_node: node.meta().name.clone(),
            });
        }
    }
    findings
}

/// The static type of a wire source.
fn source_type(
    ws: &WireSource,
    nodes: &[Box<dyn PolydatNode>],
    input_defs: &[InputDef],
) -> Option<PortType> {
    match ws {
        WireSource::Input(c) => input_defs.get(*c).map(|d| d.port_type),
        WireSource::NodeOutput(n, p) => {
            nodes.get(*n).and_then(|nd| nd.meta().outs.get(*p)).map(|o| o.typ)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::Value;
    use crate::compile::assembly::{AssemblyError, PolydatAssembler, WireRef};
    use crate::kernel::InputKind;

    fn conv(from: PortType, to: PortType) -> Box<dyn PolydatNode> {
        boundary_adapter(from, to).expect("catalog pair")
    }

    /// U64 → Str → U64 through pure conversions is the canonical
    /// mechanical round trip: strict-values mode fails the compile
    /// with a message naming the modulation.
    #[test]
    fn strict_mode_rejects_scalar_string_round_trip() {
        let mut asm = PolydatAssembler::new(vec![]);
        asm.set_strict_wires(false, true);
        asm.add_input("x", Value::U64(0), PortType::U64, InputKind::Coordinate);
        asm.add_node("to_text", conv(PortType::U64, PortType::Str),
            vec![WireRef::Input("x".into())]);
        asm.add_node("back", conv(PortType::Str, PortType::U64),
            vec![WireRef::Node("to_text".into(), 0)]);
        asm.add_output("y", WireRef::node("back"));
        match asm.compile() {
            Err(AssemblyError::Other(msg)) => {
                assert!(msg.contains("type round trip"), "got: {msg}");
                assert!(msg.contains("U64") && msg.contains("Str"), "got: {msg}");
            }
            other => panic!("expected strict round-trip rejection, got {other:?}"),
        }
    }

    /// The same graph without strict mode compiles (warning only).
    #[test]
    fn default_mode_warns_but_compiles() {
        let mut asm = PolydatAssembler::new(vec![]);
        asm.add_input("x", Value::U64(0), PortType::U64, InputKind::Coordinate);
        asm.add_node("to_text", conv(PortType::U64, PortType::Str),
            vec![WireRef::Input("x".into())]);
        asm.add_node("back", conv(PortType::Str, PortType::U64),
            vec![WireRef::Node("to_text".into(), 0)]);
        asm.add_output("y", WireRef::node("back"));
        asm.compile().expect("non-strict compile must succeed");
    }

    /// T → Json → T is a by-design hand-off: clean even under strict.
    #[test]
    fn json_intermediary_is_sanctioned() {
        let mut asm = PolydatAssembler::new(vec![]);
        asm.set_strict_wires(false, true);
        asm.add_input("x", Value::U64(0), PortType::U64, InputKind::Coordinate);
        asm.add_node("to_json", conv(PortType::U64, PortType::Json),
            vec![WireRef::Input("x".into())]);
        asm.add_node("back", conv(PortType::Json, PortType::U64),
            vec![WireRef::Node("to_json".into(), 0)]);
        asm.add_output("y", WireRef::node("back"));
        asm.compile().expect("Json hand-off must be sanctioned");
    }

    /// A parser fed from a genuine text ORIGIN (a Str input) is not a
    /// round trip — nothing left the native domain. Clean under strict.
    #[test]
    fn parse_from_text_origin_is_clean() {
        let mut asm = PolydatAssembler::new(vec![]);
        asm.set_strict_wires(false, true);
        asm.add_input("s", Value::Str("1".into()), PortType::Str, InputKind::Coordinate);
        asm.add_node("parse", conv(PortType::Str, PortType::U64),
            vec![WireRef::Input("s".into())]);
        asm.add_output("y", WireRef::node("parse"));
        asm.compile().expect("parsing a text origin is legitimate");
    }
}
