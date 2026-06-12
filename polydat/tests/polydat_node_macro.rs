// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! SRD-80 PR B.1 — proc-macro `#[polydat_node]` validation
//! tests. Defines a few pilot nodes inline using the macro and
//! verifies their generated struct + NodeMeta + eval round-
//! trip correctly.
//!
//! These tests exercise the SIMPLE case only: primitive
//! scalar arguments and return types, no state, no JIT, no
//! const args. The pilot is what proves the boxing/unboxing
//! path works end-to-end before any library migration begins.

use polydat::ast::{PolydatNode, PortType, Slot, SlotType, Value};

// Pilot 1 — single-arg u64 → u64 identity-ish node.
#[polydat::polydat_node(category = Math)]
fn macro_pilot_double(x: u64) -> u64 {
    x * 2
}

// Pilot 2 — two-arg Str / Str → u64 (mirrors str_eq's shape).
#[polydat::polydat_node(category = Comparison)]
fn macro_pilot_str_eq(a: &str, b: &str) -> u64 {
    if a == b { 1 } else { 0 }
}

// Pilot 3 — mixed types (Str + u64 → Str).
#[polydat::polydat_node(category = String)]
fn macro_pilot_format(prefix: &str, n: u64) -> String {
    format!("{prefix}{n}")
}

// Pilot 4 — bool input + bool output.
#[polydat::polydat_node(category = Diagnostic)]
fn macro_pilot_not(b: bool) -> bool {
    !b
}

#[test]
fn macro_generates_struct_with_correct_meta_for_single_arg_u64() {
    let node = MacroPilotDouble::new();
    let meta = node.meta();
    assert_eq!(meta.name.as_str(), "macro_pilot_double");
    assert_eq!(meta.ins.len(), 1);
    assert!(matches!(meta.ins[0], Slot::Wire(ref p) if p.typ == PortType::U64));
    assert_eq!(meta.outs.len(), 1);
    assert_eq!(meta.outs[0].typ, PortType::U64);
}

#[test]
fn macro_generates_struct_with_correct_meta_for_two_str_args() {
    let node = MacroPilotStrEq::new();
    let meta = node.meta();
    assert_eq!(meta.name.as_str(), "macro_pilot_str_eq");
    assert_eq!(meta.ins.len(), 2);
    assert!(matches!(meta.ins[0], Slot::Wire(ref p) if p.typ == PortType::Str));
    assert!(matches!(meta.ins[1], Slot::Wire(ref p) if p.typ == PortType::Str));
    assert_eq!(meta.outs[0].typ, PortType::U64);
}

#[test]
fn macro_generates_struct_with_correct_meta_for_mixed_args() {
    let node = MacroPilotFormat::new();
    let meta = node.meta();
    assert_eq!(meta.name.as_str(), "macro_pilot_format");
    assert!(matches!(meta.ins[0], Slot::Wire(ref p) if p.typ == PortType::Str));
    assert!(matches!(meta.ins[1], Slot::Wire(ref p) if p.typ == PortType::U64));
    assert_eq!(meta.outs[0].typ, PortType::Str);
}

#[test]
fn macro_generates_struct_with_correct_meta_for_bool() {
    let node = MacroPilotNot::new();
    let meta = node.meta();
    assert!(matches!(meta.ins[0], Slot::Wire(ref p) if p.typ == PortType::Bool));
    assert_eq!(meta.outs[0].typ, PortType::Bool);
}

#[test]
fn macro_eval_round_trips_through_value_boxing_for_single_arg_u64() {
    let node = MacroPilotDouble::new();
    let inputs = [Value::U64(21)];
    let mut outputs = [Value::None];
    node.eval(&inputs, &mut outputs);
    assert_eq!(outputs[0], Value::U64(42));
}

#[test]
fn macro_eval_round_trips_through_value_boxing_for_two_str_args() {
    let node = MacroPilotStrEq::new();

    let inputs_eq = [Value::Str("foo".into()), Value::Str("foo".into())];
    let mut outputs = [Value::None];
    node.eval(&inputs_eq, &mut outputs);
    assert_eq!(outputs[0], Value::U64(1));

    let inputs_ne = [Value::Str("foo".into()), Value::Str("bar".into())];
    let mut outputs = [Value::None];
    node.eval(&inputs_ne, &mut outputs);
    assert_eq!(outputs[0], Value::U64(0));
}

#[test]
fn macro_eval_round_trips_through_value_boxing_for_mixed_args() {
    let node = MacroPilotFormat::new();
    let inputs = [Value::Str("count=".into()), Value::U64(7)];
    let mut outputs = [Value::None];
    node.eval(&inputs, &mut outputs);
    assert_eq!(outputs[0], Value::Str("count=7".into()));
}

#[test]
fn macro_eval_round_trips_through_value_boxing_for_bool() {
    let node = MacroPilotNot::new();
    let inputs = [Value::Bool(true)];
    let mut outputs = [Value::None];
    node.eval(&inputs, &mut outputs);
    assert_eq!(outputs[0], Value::Bool(false));
}

#[test]
fn macro_slot_type_is_wire_for_every_generated_input() {
    // Catches any future drift where the macro might emit a
    // ConstU64 / ConstStr slot by mistake (Phase B.1 only
    // supports wire inputs).
    for slot in MacroPilotStrEq::new().meta().ins.iter() {
        assert!(matches!(slot.slot_type(), SlotType::Wire));
    }
}

// ── PR B.2 — link-time FuncSig registration via NodeRegistration ──

#[test]
fn macro_registered_node_appears_in_runtime_registry() {
    let sigs = polydat::dsl::registry::registry();
    let names: Vec<&'static str> = sigs.iter().map(|s| s.name).collect();
    assert!(names.contains(&"macro_pilot_double"),
        "macro-registered `macro_pilot_double` MUST appear in registry");
    assert!(names.contains(&"macro_pilot_str_eq"),
        "macro-registered `macro_pilot_str_eq` MUST appear in registry");
    assert!(names.contains(&"macro_pilot_format"));
    assert!(names.contains(&"macro_pilot_not"));
}

#[test]
fn macro_registered_node_carries_attribute_specified_category() {
    use polydat::dsl::registry::FuncCategory;
    let sigs = polydat::dsl::registry::registry();
    let find = |n: &str| sigs.iter().find(|s| s.name == n).cloned();
    assert_eq!(find("macro_pilot_double").unwrap().category, FuncCategory::Math);
    assert_eq!(find("macro_pilot_str_eq").unwrap().category, FuncCategory::Comparison);
    assert_eq!(find("macro_pilot_format").unwrap().category, FuncCategory::String);
    assert_eq!(find("macro_pilot_not").unwrap().category, FuncCategory::Diagnostic);
}

#[test]
fn macro_registered_node_resolves_via_registry_lookup() {
    let sig = polydat::dsl::registry::lookup("macro_pilot_double")
        .expect("macro-registered node MUST be discoverable via registry::lookup");
    assert_eq!(sig.name, "macro_pilot_double");
    assert_eq!(sig.params.len(), 1);
}

#[test]
fn macro_registered_funcsig_params_match_function_signature() {
    let sigs = polydat::dsl::registry::registry();
    let sig = sigs.iter().find(|s| s.name == "macro_pilot_str_eq")
        .expect("macro_pilot_str_eq registered");
    assert_eq!(sig.params.len(), 2, "two args ⇒ two ParamSpec entries");
    assert_eq!(sig.params[0].name, "a");
    assert_eq!(sig.params[1].name, "b");
    assert!(matches!(sig.params[0].slot_type, SlotType::Wire));
    assert!(matches!(sig.params[1].slot_type, SlotType::Wire));
    assert_eq!(sig.outputs, 1);
}

// ── PR B.5 — Const<T> args + #[poly_default(VAL)] ──

use polydat::derive_support::Const;

// Pilot 5 — single Const<u64> with no default. Wire u64 input
// gets shifted by the captured const offset.
#[polydat::polydat_node(category = Math)]
fn macro_pilot_shift(input: u64, offset: Const<u64>) -> u64 {
    input + *offset
}

// Pilot 6 — Const<&str> wire+const mix. Returns a string by
// prefixing the captured const.
#[polydat::polydat_node(category = String)]
fn macro_pilot_prefix(value: u64, prefix: Const<&str>) -> String {
    format!("{}{}", prefix.0, value)
}

// Pilot 7 — multiple consts + default. Optional `scale` falls
// back to `1` when the workload doesn't supply it.
#[polydat::polydat_node(category = Math)]
fn macro_pilot_affine(
    x: u64,
    intercept: Const<u64>,
    #[poly_default(1)] scale: Const<u64>,
) -> u64 {
    (x * *scale) + *intercept
}

// Pilot 8 — Const<f64> + Const<bool>.
#[polydat::polydat_node(category = Math)]
fn macro_pilot_scale_or_zero(x: u64, factor: Const<f64>, enable: Const<bool>) -> f64 {
    if *enable { x as f64 * *factor } else { 0.0 }
}

#[test]
fn macro_const_arg_slot_type_in_funcsig() {
    let sigs = polydat::dsl::registry::registry();
    let sig = sigs.iter().find(|s| s.name == "macro_pilot_shift")
        .expect("macro_pilot_shift registered");
    assert_eq!(sig.params.len(), 2, "wire + const = 2 params");
    assert!(matches!(sig.params[0].slot_type, SlotType::Wire));
    assert!(matches!(sig.params[1].slot_type, SlotType::ConstU64));
    assert!(sig.params[0].required);
    assert!(sig.params[1].required, "no default → required");
}

#[test]
fn macro_const_str_slot_type_in_funcsig() {
    let sigs = polydat::dsl::registry::registry();
    let sig = sigs.iter().find(|s| s.name == "macro_pilot_prefix").unwrap();
    assert!(matches!(sig.params[0].slot_type, SlotType::Wire));
    assert!(matches!(sig.params[1].slot_type, SlotType::ConstStr));
}

#[test]
fn macro_poly_default_marks_param_optional() {
    let sigs = polydat::dsl::registry::registry();
    let sig = sigs.iter().find(|s| s.name == "macro_pilot_affine").unwrap();
    assert_eq!(sig.params.len(), 3);
    assert!(sig.params[0].required, "x is a required wire");
    assert!(sig.params[1].required, "intercept has no default → required");
    assert!(!sig.params[2].required, "scale has poly_default → optional");
}

#[test]
fn macro_const_node_constructed_via_runtime_factory_with_const_value() {
    use polydat::dsl::factory::ConstArg;
    use polydat::dsl::registry::registry;
    use polydat::ast::PortType;

    // Walk the inventory'd NodeRegistration entries until we
    // find the one that handles "macro_pilot_shift", invoke
    // its build closure with a single const arg of value 100,
    // then exercise eval.
    let reg = registry();
    let _ = reg; // ensure registry call works
    for entry in inventory::iter::<polydat::dsl::registry::NodeRegistration>() {
        if let Some(result) = (entry.build)(
            "macro_pilot_shift",
            &[],
            &[] as &[PortType],
            &[ConstArg::Int(100)],
        ) {
            let node = result.expect("build succeeds");
            let mut out = [Value::None];
            node.eval(&[Value::U64(5)], &mut out);
            assert_eq!(out[0], Value::U64(105), "5 + 100 = 105");
            return;
        }
    }
    panic!("macro_pilot_shift not found in inventory");
}

#[test]
fn macro_const_str_node_uses_captured_value_at_eval_time() {
    use polydat::dsl::factory::ConstArg;
    use polydat::ast::PortType;
    for entry in inventory::iter::<polydat::dsl::registry::NodeRegistration>() {
        if let Some(result) = (entry.build)(
            "macro_pilot_prefix",
            &[],
            &[] as &[PortType],
            &[ConstArg::Str("ID-".to_string())],
        ) {
            let node = result.expect("build succeeds");
            let mut out = [Value::None];
            node.eval(&[Value::U64(42)], &mut out);
            assert_eq!(out[0], Value::Str("ID-42".into()));
            return;
        }
    }
    panic!("macro_pilot_prefix not found in inventory");
}

#[test]
fn macro_poly_default_fallback_used_when_const_absent() {
    use polydat::dsl::factory::ConstArg;
    use polydat::ast::PortType;
    for entry in inventory::iter::<polydat::dsl::registry::NodeRegistration>() {
        if let Some(result) = (entry.build)(
            "macro_pilot_affine",
            &[],
            &[] as &[PortType],
            &[ConstArg::Int(10)], // intercept=10, scale defaulted to 1
        ) {
            let node = result.expect("build succeeds with one fewer const");
            let mut out = [Value::None];
            node.eval(&[Value::U64(3)], &mut out);
            assert_eq!(out[0], Value::U64(13), "3*1 + 10 = 13 (scale defaulted)");
            return;
        }
    }
    panic!("macro_pilot_affine not found in inventory");
}

#[test]
fn macro_poly_default_overridden_when_const_supplied() {
    use polydat::dsl::factory::ConstArg;
    use polydat::ast::PortType;
    for entry in inventory::iter::<polydat::dsl::registry::NodeRegistration>() {
        if let Some(result) = (entry.build)(
            "macro_pilot_affine",
            &[],
            &[] as &[PortType],
            &[ConstArg::Int(5), ConstArg::Int(7)], // intercept=5, scale=7
        ) {
            let node = result.expect("build succeeds");
            let mut out = [Value::None];
            node.eval(&[Value::U64(3)], &mut out);
            assert_eq!(out[0], Value::U64(26), "3*7 + 5 = 26");
            return;
        }
    }
    panic!("macro_pilot_affine not found in inventory");
}

#[test]
fn macro_missing_required_const_returns_error() {
    use polydat::ast::PortType;
    for entry in inventory::iter::<polydat::dsl::registry::NodeRegistration>() {
        if let Some(result) = (entry.build)(
            "macro_pilot_shift",
            &[],
            &[] as &[PortType],
            &[], // no consts at all — should error on missing required `offset`
        ) {
            let err = match result {
                Ok(_) => panic!("missing required const ⇒ Err expected"),
                Err(e) => e,
            };
            assert!(err.contains("offset"), "error must mention missing arg name");
            return;
        }
    }
    panic!("macro_pilot_shift not found in inventory");
}

#[test]
fn macro_mixed_const_types_evaluate_correctly() {
    use polydat::dsl::factory::ConstArg;
    use polydat::ast::PortType;
    for entry in inventory::iter::<polydat::dsl::registry::NodeRegistration>() {
        if let Some(result) = (entry.build)(
            "macro_pilot_scale_or_zero",
            &[],
            &[] as &[PortType],
            &[ConstArg::Float(2.5), ConstArg::Int(1)], // factor=2.5, enable=true
        ) {
            let node = result.expect("build succeeds");
            let mut out = [Value::None];
            node.eval(&[Value::U64(4)], &mut out);
            assert_eq!(out[0], Value::F64(10.0), "4 * 2.5 = 10.0");
            return;
        }
    }
    panic!("macro_pilot_scale_or_zero not found in inventory");
}

// ── PR B.6 — Setup<T> via #[poly_const(...)] ──

#[derive(Debug)]
pub struct PrecomputedScale {
    pub factor: u64,
    pub label: String,
}

impl PrecomputedScale {
    /// Single-call setup. Macro invokes once in new().
    pub fn from_seed(seed: &str) -> Self {
        // Pretend this is expensive — parse seed into derived state.
        let factor: u64 = seed.parse().unwrap_or(0);
        Self {
            factor: factor * 10,
            label: format!("scale-{}", factor),
        }
    }
}

#[polydat::polydat_node(category = Math)]
fn macro_pilot_setup(
    input: u64,
    seed: polydat::derive_support::Const<&str>,
    #[poly_const(PrecomputedScale::from_seed, from = seed)]
    scaled: &PrecomputedScale,
) -> u64 {
    input * scaled.factor
}

// ── PR B.14 — Narrow integer + f32 widths ──

#[polydat::polydat_node(category = Conversions)]
fn macro_pilot_u32_round_trip(input: u32) -> u32 {
    input.wrapping_mul(2)
}

#[polydat::polydat_node(category = Conversions)]
fn macro_pilot_i32_passthrough(input: i32) -> i32 {
    -input
}

#[polydat::polydat_node(category = Conversions)]
fn macro_pilot_f32_passthrough(input: f32) -> f32 {
    input + 1.0
}

#[test]
fn macro_u32_round_trip() {
    let node = MacroPilotU32RoundTrip::default();
    let mut out = [Value::None];
    node.eval(&[Value::U64(7)], &mut out);
    assert_eq!(out[0].as_u64(), 14);
    if let Slot::Wire(ref p) = node.meta().ins[0] {
        assert_eq!(p.typ, PortType::U32);
    } else { panic!(); }
}

#[test]
fn macro_i32_negation() {
    let node = MacroPilotI32Passthrough::default();
    let mut out = [Value::None];
    // Legacy bit-stuffed input still extracts via the lenient
    // Wire<i32>; the output is the honest signed carrier
    // (type_system_alignment.md §5).
    node.eval(&[Value::U64(5_u32 as u64)], &mut out);
    assert_eq!(out[0], Value::I64(-5));
    // Honest input form produces the same result.
    node.eval(&[Value::I64(5)], &mut out);
    assert_eq!(out[0], Value::I64(-5));
}

#[test]
fn macro_f32_addition() {
    let node = MacroPilotF32Passthrough::default();
    let mut out = [Value::None];
    // f32 narrow-width: input/output value is U64 carrying
    // f32 bit pattern in low 32 bits.
    let in_bits = (2.5f32).to_bits() as u64;
    node.eval(&[Value::U64(in_bits)], &mut out);
    let out_bits = out[0].as_u64() as u32;
    let out_val = f32::from_bits(out_bits);
    assert!((out_val - 3.5).abs() < 0.0001);
}

// ── PR B.13 — Typed vector wires ──

#[polydat::polydat_node(category = Diagnostic)]
fn macro_pilot_vec_f32_passthrough(input: &[f32]) -> Vec<f32> {
    input.iter().map(|x| x * 2.0).collect()
}

#[polydat::polydat_node(category = Diagnostic)]
fn macro_pilot_vec_i32_slicearc_in_out(input: polydat::ast::SliceArc<i32>) -> polydat::ast::SliceArc<i32> {
    input
}

#[test]
fn macro_vec_f32_borrow_to_owned_round_trip() {
    use polydat::ast::SliceArc;
    let node = MacroPilotVecF32Passthrough::default();
    if let Slot::Wire(ref p) = node.meta().ins[0] {
        assert_eq!(p.typ, PortType::VecF32);
    } else { panic!(); }
    assert_eq!(node.meta().outs[0].typ, PortType::VecF32);
    let payload: SliceArc<f32> = SliceArc::from_vec(vec![1.0, 2.0, 3.0]);
    let mut out = [Value::None];
    node.eval(&[Value::VecF32(payload)], &mut out);
    if let Value::VecF32(arc) = &out[0] {
        assert_eq!(arc.as_slice(), &[2.0, 4.0, 6.0]);
    } else { panic!(); }
}

#[test]
fn macro_vec_i32_slicearc_zero_copy() {
    use polydat::ast::SliceArc;
    let node = MacroPilotVecI32SlicearcInOut::default();
    let payload: SliceArc<i32> = SliceArc::from_vec(vec![10, 20, 30]);
    let mut out = [Value::None];
    node.eval(&[Value::VecI32(payload)], &mut out);
    if let Value::VecI32(arc) = &out[0] {
        assert_eq!(arc.as_slice(), &[10, 20, 30]);
    } else { panic!(); }
}

#[test]
fn macro_vec_disables_jit() {
    assert!(MacroPilotVecF32Passthrough::default().compiled_u64().is_none());
}

// ── PR B.11 — Wrapper types (Bytes, Json, Handle) ──

use std::sync::Arc;

// Bytes — both Arc<[u8]> and &[u8] inputs, Vec<u8> output.
#[polydat::polydat_node(category = Digest)]
fn macro_pilot_bytes_passthrough(input: Arc<[u8]>) -> Arc<[u8]> {
    input
}

#[polydat::polydat_node(category = Digest)]
fn macro_pilot_bytes_borrow(input: &[u8]) -> Vec<u8> {
    input.to_vec()
}

// Json
#[polydat::polydat_node(category = Json)]
fn macro_pilot_json_to_string(input: &serde_json::Value) -> String {
    serde_json::to_string(input).unwrap_or_default()
}

// Handle — Arc<TestHandle> in, Arc<TestHandle> out
pub struct TestHandle {
    pub value: u64,
}

#[polydat::polydat_node(category = Diagnostic)]
fn macro_pilot_handle_passthrough(input: Arc<TestHandle>) -> Arc<TestHandle> {
    input
}

#[test]
fn macro_bytes_arg_emits_bytes_porttype() {
    use polydat::ast::PolydatNode;
    use polydat::ast::PortType;
    let node = MacroPilotBytesPassthrough::default();
    if let polydat::ast::Slot::Wire(ref p) = node.meta().ins[0] {
        assert_eq!(p.typ, PortType::Bytes);
    } else { panic!(); }
    assert_eq!(node.meta().outs[0].typ, PortType::Bytes);
}

#[test]
fn macro_bytes_arc_passthrough_eval() {
    use polydat::ast::PolydatNode;
    let node = MacroPilotBytesPassthrough::default();
    let payload: Arc<[u8]> = vec![1, 2, 3, 4].into();
    let mut out = [Value::None];
    node.eval(&[Value::Bytes(payload.clone())], &mut out);
    if let Value::Bytes(b) = &out[0] {
        assert_eq!(&**b, &[1, 2, 3, 4]);
    } else { panic!(); }
}

#[test]
fn macro_bytes_borrow_and_vec_round_trip() {
    use polydat::ast::PolydatNode;
    let node = MacroPilotBytesBorrow::default();
    let payload: Arc<[u8]> = vec![10, 20, 30].into();
    let mut out = [Value::None];
    node.eval(&[Value::Bytes(payload)], &mut out);
    if let Value::Bytes(b) = &out[0] {
        assert_eq!(&**b, &[10, 20, 30]);
    } else { panic!(); }
}

#[test]
fn macro_json_borrow_input_to_string_output() {
    use polydat::ast::PolydatNode;
    use polydat::ast::PortType;
    let node = MacroPilotJsonToString::default();
    if let polydat::ast::Slot::Wire(ref p) = node.meta().ins[0] {
        assert_eq!(p.typ, PortType::Json);
    } else { panic!(); }
    let j = Arc::new(serde_json::json!({"hello": "world"}));
    let mut out = [Value::None];
    node.eval(&[Value::Json(j)], &mut out);
    assert!(out[0].as_str().contains("hello"));
}

#[test]
fn macro_handle_passthrough_with_downcast() {
    use polydat::ast::PolydatNode;
    use polydat::ast::PortType;
    let node = MacroPilotHandlePassthrough::default();
    if let polydat::ast::Slot::Wire(ref p) = node.meta().ins[0] {
        assert_eq!(p.typ, PortType::Handle);
    } else { panic!(); }
    let h: Arc<TestHandle> = Arc::new(TestHandle { value: 42 });
    let mut out = [Value::None];
    node.eval(&[Value::Handle(h.clone())], &mut out);
    if let Value::Handle(arc) = &out[0] {
        let downcast: Arc<TestHandle> = arc.clone().downcast().unwrap();
        assert_eq!(downcast.value, 42);
    } else { panic!(); }
}

#[test]
fn macro_wrapper_types_disable_jit() {
    use polydat::ast::PolydatNode;
    assert!(MacroPilotBytesPassthrough::default().compiled_u64().is_none());
    assert!(MacroPilotJsonToString::default().compiled_u64().is_none());
    assert!(MacroPilotHandlePassthrough::default().compiled_u64().is_none());
}

// ── PR B.10 — Multi-output via tuple return ──

// Positional default names: out_0, out_1, out_2.
#[polydat::polydat_node(category = Math)]
fn macro_pilot_divmod(a: u64, b: u64) -> (u64, u64) {
    (a / b, a % b)
}

// Named outputs via attribute.
#[polydat::polydat_node(
    category = Math,
    output_names(sum, product),
)]
fn macro_pilot_sum_and_product(a: u64, b: u64) -> (u64, u64) {
    (a.wrapping_add(b), a.wrapping_mul(b))
}

// Mixed primitive output types.
#[polydat::polydat_node(
    category = Math,
    output_names(quotient, has_remainder),
)]
fn macro_pilot_div_with_flag(a: u64, b: u64) -> (u64, bool) {
    (a / b, a % b != 0)
}

#[test]
fn macro_tuple_return_emits_n_output_ports() {
    use polydat::ast::PolydatNode;
    let node = MacroPilotDivmod::default();
    assert_eq!(node.meta().outs.len(), 2);
    assert_eq!(node.meta().outs[0].name.as_str(), "out_0");
    assert_eq!(node.meta().outs[1].name.as_str(), "out_1");
}

#[test]
fn macro_tuple_return_with_named_outputs() {
    use polydat::ast::PolydatNode;
    let node = MacroPilotSumAndProduct::default();
    assert_eq!(node.meta().outs.len(), 2);
    assert_eq!(node.meta().outs[0].name.as_str(), "sum");
    assert_eq!(node.meta().outs[1].name.as_str(), "product");
}

#[test]
fn macro_tuple_return_writes_each_output_in_order() {
    use polydat::ast::PolydatNode;
    let node = MacroPilotDivmod::default();
    let mut out = [Value::None, Value::None];
    node.eval(&[Value::U64(17), Value::U64(5)], &mut out);
    assert_eq!(out[0], Value::U64(3));   // 17 / 5
    assert_eq!(out[1], Value::U64(2));   // 17 % 5
}

#[test]
fn macro_tuple_return_with_mixed_types() {
    use polydat::ast::PolydatNode;
    use polydat::ast::PortType;
    let node = MacroPilotDivWithFlag::default();
    assert_eq!(node.meta().outs[0].typ, PortType::U64);
    assert_eq!(node.meta().outs[1].typ, PortType::Bool);
    let mut out = [Value::None, Value::None];
    node.eval(&[Value::U64(20), Value::U64(4)], &mut out);
    assert_eq!(out[0], Value::U64(5));
    assert_eq!(out[1], Value::Bool(false));
    node.eval(&[Value::U64(21), Value::U64(4)], &mut out);
    assert_eq!(out[0], Value::U64(5));
    assert_eq!(out[1], Value::Bool(true));
}

#[test]
fn macro_tuple_return_funcsig_outputs_count_matches_arity() {
    use polydat::dsl::registry::registry;
    let sigs = registry();
    let sig = sigs.iter().find(|s| s.name == "macro_pilot_divmod").unwrap();
    assert_eq!(sig.outputs, 2);
    let sig2 = sigs.iter().find(|s| s.name == "macro_pilot_div_with_flag").unwrap();
    assert_eq!(sig2.outputs, 2);
}

#[test]
fn macro_tuple_return_jit_round_trip() {
    use polydat::ast::PolydatNode;
    // SRD-80 PR B.15: multi-output gained Phase 2 closure
    // support. divmod is (u64, u64) — both JIT-eligible —
    // so a `compiled_u64()` closure is now emitted.
    let node = MacroPilotDivmod::default();
    let compiled = node.compiled_u64()
        .expect("u64×u64→(u64,u64) is JIT-eligible after PR B.15");
    let mut out = [0u64; 2];
    compiled(&[17, 5], &mut out);
    assert_eq!(out[0], 3);
    assert_eq!(out[1], 2);
}

// ── PR B.9 — Variadics via &[T] arg ──

#[polydat::polydat_node(
    category = Variadic,
    identity = 0u64,
    commutativity = AllCommutative,
)]
fn macro_pilot_sum(values: &[u64]) -> u64 {
    values.iter().fold(0u64, |a, b| a.wrapping_add(*b))
}

#[polydat::polydat_node(
    category = Variadic,
    identity = 1u64,
    commutativity = AllCommutative,
)]
fn macro_pilot_product(values: &[u64]) -> u64 {
    values.iter().fold(1u64, |a, b| a.wrapping_mul(*b))
}

#[polydat::polydat_node(category = Variadic)]
fn macro_pilot_strconcat(parts: &[&str]) -> String {
    parts.concat()
}

#[test]
fn macro_variadic_construction_with_zero_inputs() {
    use polydat::ast::PolydatNode;
    let node = MacroPilotSum::new(0);
    assert_eq!(node.meta().ins.len(), 0);
    let mut out = [Value::None];
    node.eval(&[], &mut out);
    assert_eq!(out[0], Value::U64(0));
}

#[test]
fn macro_variadic_construction_with_three_inputs() {
    use polydat::ast::PolydatNode;
    let node = MacroPilotSum::new(3);
    assert_eq!(node.meta().ins.len(), 3);
    let mut out = [Value::None];
    node.eval(&[Value::U64(1), Value::U64(2), Value::U64(3)], &mut out);
    assert_eq!(out[0], Value::U64(6));
}

#[test]
fn macro_variadic_product_identity_1() {
    use polydat::ast::PolydatNode;
    let mut out = [Value::None];
    MacroPilotProduct::new(0).eval(&[], &mut out);
    assert_eq!(out[0], Value::U64(1));
    MacroPilotProduct::new(3).eval(
        &[Value::U64(2), Value::U64(3), Value::U64(4)], &mut out);
    assert_eq!(out[0], Value::U64(24));
}

#[test]
fn macro_variadic_str_concat_jit_ineligible() {
    use polydat::ast::PolydatNode;
    let node = MacroPilotStrconcat::new(3);
    assert!(node.compiled_u64().is_none(), "&[&str] is not JIT-eligible");
    let mut out = [Value::None];
    node.eval(
        &[Value::Str("a".into()), Value::Str("b".into()), Value::Str("c".into())],
        &mut out,
    );
    assert_eq!(out[0].as_str(), "abc");
}

#[test]
fn macro_variadic_u64_jit_works_via_compiled_u64() {
    use polydat::ast::PolydatNode;
    let node = MacroPilotSum::new(4);
    let compiled = node.compiled_u64().expect("u64 variadic IS JIT-eligible");
    let mut out = [0u64];
    compiled(&[10, 20, 30, 40], &mut out);
    assert_eq!(out[0], 100);
}

#[test]
fn macro_variadic_funcsig_declares_variadic_arity() {
    use polydat::dsl::registry::{Arity, registry};
    let sigs = registry();
    let sig = sigs.iter().find(|s| s.name == "macro_pilot_sum")
        .expect("variadic sum registered");
    assert!(matches!(sig.arity, Arity::VariadicWires { .. }));
    assert_eq!(sig.identity, Some(0));
    assert_eq!(sig.params.len(), 1);
    assert!(!sig.params[0].required, "variadic param is required:false");
}

#[test]
fn macro_variadic_funcsig_carries_variadic_ctor() {
    use polydat::dsl::registry::registry;
    let sigs = registry();
    let sig = sigs.iter().find(|s| s.name == "macro_pilot_sum").unwrap();
    let ctor = sig.variadic_ctor.expect("pure-variadic emits a ctor");
    let node = ctor(5);
    assert_eq!(node.meta().ins.len(), 5);
}

#[test]
fn macro_variadic_constructs_via_runtime_factory_with_wires_slice() {
    use polydat::compile::assembly::WireRef;
    use polydat::ast::PortType;
    let wires = [WireRef::input("a"), WireRef::input("b"), WireRef::input("c")];
    for entry in inventory::iter::<polydat::dsl::registry::NodeRegistration>() {
        if let Some(result) = (entry.build)(
            "macro_pilot_sum",
            &wires,
            &[PortType::U64; 3],
            &[],
        ) {
            let node = result.expect("build succeeds with 3 wires");
            assert_eq!(node.meta().ins.len(), 3);
            return;
        }
    }
    panic!("macro_pilot_sum not registered");
}

// ── PR B.8 — PolyWire via Value arg ──

// Passthrough Value→Value: SameAsInput output.
#[polydat::polydat_node(category = Diagnostic)]
fn macro_pilot_polywire_passthrough(value: Value) -> Value {
    value
}

// Value→primitive: declared input type is runtime-known,
// output is fixed Str.
#[polydat::polydat_node(category = Diagnostic)]
fn macro_pilot_polywire_to_type(input: Value) -> String {
    input.port_type().to_string()
}

#[test]
fn macro_polywire_arg_emits_runtime_typed_meta() {
    use polydat::ast::PortType;
    let node = MacroPilotPolywirePassthrough::new(PortType::U64);
    let meta = node.meta();
    assert!(matches!(meta.ins[0], Slot::Wire(ref p) if p.typ == PortType::U64));
    assert_eq!(meta.outs[0].typ, PortType::U64, "SameAsInput → output tracks input");
}

#[test]
fn macro_polywire_passthrough_eval_clones_value() {
    use polydat::ast::PortType;
    let node = MacroPilotPolywirePassthrough::new(PortType::Str);
    let inputs = [Value::Str("hello".into())];
    let mut outputs = [Value::None];
    node.eval(&inputs, &mut outputs);
    assert_eq!(outputs[0], Value::Str("hello".into()));
}

#[test]
fn macro_polywire_construction_with_different_runtime_types() {
    use polydat::ast::PortType;
    for pt in [PortType::U64, PortType::F64, PortType::Bool, PortType::Str] {
        let node = MacroPilotPolywirePassthrough::new(pt);
        assert_eq!(node.meta().ins[0].slot_type(), SlotType::Wire);
        // Find the wire's actual port type via the meta.
        if let Slot::Wire(ref port) = node.meta().ins[0] {
            assert_eq!(port.typ, pt, "port type tracks construction-time runtime type");
        }
    }
}

#[test]
fn macro_polywire_to_primitive_return_fixes_output_type() {
    use polydat::ast::PortType;
    let node = MacroPilotPolywireToType::new(PortType::F64);
    assert_eq!(node.meta().outs[0].typ, PortType::Str,
        "primitive return → fixed PortType regardless of input");
    let mut out = [Value::None];
    node.eval(&[Value::F64(3.14)], &mut out);
    assert_eq!(out[0], Value::Str("f64".into()));
}

#[test]
fn macro_polywire_funcsig_output_type_is_same_as_input() {
    use polydat::dsl::registry::{OutputType, registry};
    let sigs = registry();
    let sig = sigs.iter().find(|s| s.name == "macro_pilot_polywire_passthrough")
        .expect("PolyWire passthrough registered");
    assert!(matches!(sig.output_type, OutputType::SameAsInput(0)),
        "FuncSig.output_type must be SameAsInput(0) for Value → Value");
}

#[test]
fn macro_polywire_funcsig_output_type_is_fixed_when_return_is_primitive() {
    use polydat::dsl::registry::{OutputType, registry};
    let sigs = registry();
    let sig = sigs.iter().find(|s| s.name == "macro_pilot_polywire_to_type")
        .expect("PolyWire → primitive registered");
    assert!(matches!(sig.output_type, OutputType::Fixed),
        "FuncSig.output_type must be Fixed when return is a primitive");
}

#[test]
fn macro_polywire_node_disables_jit() {
    use polydat::ast::PolydatNode;
    use polydat::ast::PortType;
    let node = MacroPilotPolywirePassthrough::new(PortType::U64);
    assert!(node.compiled_u64().is_none(),
        "PolyWire is not a u64-buffer carrier; JIT must be disabled");
    assert_eq!(node.jit_constants(), Vec::<u64>::new());
}

#[test]
fn macro_polywire_constructs_via_runtime_factory_with_wire_types() {
    use polydat::ast::PortType;
    for entry in inventory::iter::<polydat::dsl::registry::NodeRegistration>() {
        if let Some(result) = (entry.build)(
            "macro_pilot_polywire_passthrough",
            &[],
            &[PortType::Bool],   // assembler-resolved upstream wire type
            &[],
        ) {
            let node = result.expect("build succeeds with wire type provided");
            let mut out = [Value::None];
            node.eval(&[Value::Bool(true)], &mut out);
            assert_eq!(out[0], Value::Bool(true));
            // Verify the constructed node carries the right port type.
            assert_eq!(node.meta().ins[0].slot_type(), SlotType::Wire);
            if let Slot::Wire(ref p) = node.meta().ins[0] {
                assert_eq!(p.typ, PortType::Bool);
            }
            return;
        }
    }
    panic!("macro_pilot_polywire_passthrough not registered");
}

#[test]
fn macro_setup_arg_does_not_appear_in_funcsig() {
    let sigs = polydat::dsl::registry::registry();
    let sig = sigs.iter().find(|s| s.name == "macro_pilot_setup")
        .expect("macro_pilot_setup registered");
    assert_eq!(sig.params.len(), 2,
        "wire + const = 2 params; setup arg is macro-internal and absent");
    assert_eq!(sig.params[0].name, "input");
    assert_eq!(sig.params[1].name, "seed");
}

// ── PR B.7 — JIT (Phase 2/3) emission ──

// JIT-eligible: bare u64 + Const<u64> → u64. Should emit
// compiled_u64 and jit_constants.
#[polydat::polydat_node(category = Math)]
fn macro_pilot_add_const(input: u64, addend: Const<u64>) -> u64 {
    input + *addend
}

// JIT-eligible f64 + bool variants.
#[polydat::polydat_node(category = Math)]
fn macro_pilot_f64_lt(a: f64, b: f64) -> bool {
    a < b
}

// JIT-eligible: bool input, bool return, with f64 const.
#[polydat::polydat_node(category = Math)]
fn macro_pilot_f64_scaled(input: f64, scale: Const<f64>) -> f64 {
    input * *scale
}

// JIT-INELIGIBLE: String wire arg. Macro must NOT emit
// compiled_u64 (else the closure won't compile against u64
// buffers).
#[polydat::polydat_node(category = String)]
fn macro_pilot_string_passthrough(s: String) -> String {
    s
}

// JIT-INELIGIBLE: Setup<T> arg. Macro must skip JIT emission.
pub struct SetupPilotState { pub doubled: u64 }

impl SetupPilotState {
    pub fn from_seed(seed: u64) -> Self { Self { doubled: seed * 2 } }
}

#[polydat::polydat_node(category = Math)]
fn macro_pilot_with_setup(
    input: u64,
    seed: Const<u64>,
    #[poly_const(SetupPilotState::from_seed, from = seed)]
    state: &SetupPilotState,
) -> u64 {
    input + state.doubled
}

// no_jit opt-out: type signature qualifies but the operator
// declared `no_jit`. Macro must skip JIT emission.
#[polydat::polydat_node(category = Math, no_jit)]
fn macro_pilot_no_jit(input: u64) -> u64 {
    input + 7
}

#[test]
fn macro_jit_eligible_node_emits_compiled_u64() {
    use polydat::ast::PolydatNode;
    let node = MacroPilotAddConst::new(100);
    let compiled = node.compiled_u64()
        .expect("Phase-2 closure must be emitted for u64+Const<u64>→u64");
    let inputs = [42u64];
    let mut outputs = [0u64];
    compiled(&inputs, &mut outputs);
    assert_eq!(outputs[0], 142, "42 + 100 via compiled_u64");
}

#[test]
fn macro_jit_eligible_node_emits_jit_constants_in_decl_order() {
    use polydat::ast::PolydatNode;
    let node = MacroPilotAddConst::new(100);
    let consts = node.jit_constants();
    assert_eq!(consts, vec![100u64], "single const arg of value 100");
}

#[test]
fn macro_jit_f64_wire_args_bit_reinterpret_through_u64_buffer() {
    use polydat::ast::PolydatNode;
    let node = MacroPilotF64Lt::default();
    let compiled = node.compiled_u64()
        .expect("f64+f64→bool must emit Phase-2 closure");
    // Pack f64 inputs as their u64 bit reps.
    let inputs = [3.5f64.to_bits(), 7.5f64.to_bits()];
    let mut outputs = [0u64];
    compiled(&inputs, &mut outputs);
    assert_eq!(outputs[0], 1, "3.5 < 7.5 ⇒ bool true ⇒ u64 1");

    let inputs2 = [10.0f64.to_bits(), 2.0f64.to_bits()];
    compiled(&inputs2, &mut outputs);
    assert_eq!(outputs[0], 0, "10 < 2 false ⇒ u64 0");
}

#[test]
fn macro_jit_const_f64_encoded_as_bits_in_jit_constants() {
    use polydat::ast::PolydatNode;
    let node = MacroPilotF64Scaled::new(2.5);
    let consts = node.jit_constants();
    assert_eq!(consts, vec![2.5f64.to_bits()],
        "f64 const must be bit-reinterpreted for Phase-3 classifier");
}

#[test]
fn macro_jit_f64_return_writes_bit_pattern_to_u64_buffer() {
    use polydat::ast::PolydatNode;
    let node = MacroPilotF64Scaled::new(2.5);
    let compiled = node.compiled_u64().expect("f64 return ⇒ JIT eligible");
    let inputs = [4.0f64.to_bits()];
    let mut outputs = [0u64];
    compiled(&inputs, &mut outputs);
    assert_eq!(f64::from_bits(outputs[0]), 10.0, "4.0 * 2.5 = 10.0 via JIT buffer");
}

#[test]
fn macro_jit_ineligible_string_arg_node_skips_compiled_u64() {
    use polydat::ast::PolydatNode;
    let node = MacroPilotStringPassthrough::default();
    assert!(node.compiled_u64().is_none(),
        "String arg makes node JIT-ineligible; compiled_u64 must NOT be emitted");
    assert_eq!(node.jit_constants(), Vec::<u64>::new(),
        "JIT-ineligible node returns empty jit_constants");
}

#[test]
fn macro_jit_ineligible_setup_arg_node_skips_compiled_u64() {
    use polydat::ast::PolydatNode;
    let node = MacroPilotWithSetup::new(5);
    assert!(node.compiled_u64().is_none(),
        "Setup<T> arg makes node JIT-ineligible (derived state not in u64 buffer)");
    // Const-only arg, but JIT is disabled due to Setup presence,
    // so jit_constants should NOT expose state through that path.
    assert_eq!(node.jit_constants(), Vec::<u64>::new(),
        "Setup-bearing node also skips jit_constants (no Phase-3 dispatch)");
}

#[test]
fn macro_no_jit_attr_blocks_emission_even_when_types_qualify() {
    use polydat::ast::PolydatNode;
    let node = MacroPilotNoJit::default();
    assert!(node.compiled_u64().is_none(),
        "no_jit attr blocks Phase-2 emission even for qualifying type signature");
    assert_eq!(node.jit_constants(), Vec::<u64>::new(),
        "no_jit attr blocks jit_constants emission too");
}

#[test]
fn macro_jit_path_and_eval_path_produce_same_result() {
    use polydat::ast::PolydatNode;
    let node = MacroPilotAddConst::new(50);
    // eval() path through Value boxing
    let mut eval_out = [Value::None];
    node.eval(&[Value::U64(7)], &mut eval_out);
    // compiled_u64() path through u64 buffer
    let compiled = node.compiled_u64().unwrap();
    let mut jit_out = [0u64];
    compiled(&[7], &mut jit_out);
    assert_eq!(eval_out[0], Value::U64(jit_out[0]),
        "eval and compiled_u64 MUST be observationally identical");
}

#[test]
fn macro_setup_arg_node_eval_still_works_with_inline_body() {
    use polydat::ast::PolydatNode;
    let node = MacroPilotWithSetup::new(5);
    let mut out = [Value::None];
    node.eval(&[Value::U64(10)], &mut out);
    assert_eq!(out[0], Value::U64(20), "10 + (5*2) = 20 — eval path works for JIT-ineligible");
}

#[test]
fn macro_setup_computed_once_at_construction_and_borrowed_at_eval() {
    use polydat::dsl::factory::ConstArg;
    use polydat::ast::PortType;
    for entry in inventory::iter::<polydat::dsl::registry::NodeRegistration>() {
        if let Some(result) = (entry.build)(
            "macro_pilot_setup",
            &[],
            &[] as &[PortType],
            &[ConstArg::Str("5".to_string())],
        ) {
            let node = result.expect("build succeeds");
            let mut out = [Value::None];
            node.eval(&[Value::U64(3)], &mut out);
            // seed "5" → from_seed returns factor=50; 3*50 = 150
            assert_eq!(out[0], Value::U64(150));
            // Re-eval — setup MUST NOT recompute (same instance).
            node.eval(&[Value::U64(4)], &mut out);
            assert_eq!(out[0], Value::U64(200), "4*50 = 200, setup still cached");
            return;
        }
    }
    panic!("macro_pilot_setup not found in inventory");
}

#[test]
fn macro_const_bool_false_takes_disable_branch() {
    use polydat::dsl::factory::ConstArg;
    use polydat::ast::PortType;
    for entry in inventory::iter::<polydat::dsl::registry::NodeRegistration>() {
        if let Some(result) = (entry.build)(
            "macro_pilot_scale_or_zero",
            &[],
            &[] as &[PortType],
            &[ConstArg::Float(2.5), ConstArg::Int(0)], // enable=false
        ) {
            let node = result.expect("build succeeds");
            let mut out = [Value::None];
            node.eval(&[Value::U64(4)], &mut out);
            assert_eq!(out[0], Value::F64(0.0));
            return;
        }
    }
    panic!("macro_pilot_scale_or_zero not found");
}

// ── Narrow-width Phase-2 eligibility (alignment §8.3 follow-up) ──
//
// The macro's buffer tokens are width-aware: narrow scalar wires
// (u8/i8/u16/i16/u32/i32/f32/f16) ride the u64 slots per their
// Wire storage conventions and the generated `compiled_u64`
// closure must be observationally identical to typed eval.

#[polydat::polydat_node(category = Diagnostic)]
fn macro_pilot_i16_negation(input: i16) -> i16 {
    -input
}

#[polydat::polydat_node(category = Diagnostic)]
fn macro_pilot_u8_double(input: u8) -> u8 {
    input.wrapping_mul(2)
}

#[polydat::polydat_node(category = Diagnostic)]
fn macro_pilot_f16_add_one(input: half::f16) -> half::f16 {
    half::f16::from_f32(input.to_f32() + 1.0)
}

#[test]
fn macro_narrow_widths_are_phase2_eligible_and_equivalent() {
    // i16: sign-extension must survive the buffer round trip.
    let node = MacroPilotI16Negation::default();
    let compiled = node.compiled_u64().expect("i16 wire is Phase-2 eligible");
    let mut out = [0u64; 1];
    compiled(&[5u64], &mut out);
    assert_eq!(out[0] as i64, -5, "i16 negation via compiled_u64");
    // The buffer form matches the honest Wire storage (I64 carrier):
    let mut tv = [Value::None];
    node.eval(&[Value::I64(5)], &mut tv);
    assert_eq!(tv[0], Value::I64(-5));
    assert_eq!(out[0], (-5i64) as u64, "buffer bits == sign-extended i64");

    // u8: zero-extended.
    let node = MacroPilotU8Double::default();
    let compiled = node.compiled_u64().expect("u8 wire is Phase-2 eligible");
    let mut out = [0u64; 1];
    compiled(&[100u64], &mut out);
    assert_eq!(out[0], 200);
    // Narrowing happens before the body: 0x1F0 reads as u8 0xF0.
    compiled(&[0x1F0u64], &mut out);
    assert_eq!(out[0], (0xF0u8.wrapping_mul(2)) as u64);

    // f16: bit-stuffed like f32.
    let node = MacroPilotF16AddOne::default();
    let compiled = node.compiled_u64().expect("f16 wire is Phase-2 eligible");
    let mut out = [0u64; 1];
    let in_bits = half::f16::from_f32(1.5).to_bits() as u64;
    compiled(&[in_bits], &mut out);
    assert_eq!(
        half::f16::from_bits(out[0] as u16),
        half::f16::from_f32(2.5),
        "f16 add-one via compiled_u64"
    );

    // 128-bit stays typed-eval only (no single-slot ride).
    // (No macro pilot node — u128 wires are interpreter-only by
    // the wire_type_to_jit_type table.)
}
