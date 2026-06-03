// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Typed-binding contracts between adapters and the polydat runtime.
//!
//! ## What a binder is
//!
//! A [`Binder`] is an adapter's declaration, at op-template
//! construction time, of one typed-parameter binding shape it
//! expects: which wires supply values, and what *lvalue type*
//! each value will be assigned into at the wire-protocol layer
//! (CQL column type, RPC parameter type, HTTP body schema, etc.).
//!
//! Polydat consumes the binder to:
//!
//! 1. **Verify wiring sanity at construction time** — for each
//!    slot, check that the wire's rvalue type satisfies the
//!    declared lvalue type. Concretely, the load-bearing rule is:
//!    *a string detour (rvalue → `to_display_string` → bytes →
//!    cluster parser → typed wire form) is permitted only when
//!    the lvalue is itself a string-natural type.* Anywhere else,
//!    text-rendering a typed wire to feed a typed parameter is
//!    the workload-shape defect the binder API exists to catch.
//!
//! 2. **Plan typed pass-through at compile time** — when the
//!    kernel knows a wire's value will flow only into typed
//!    slots, it can skip string-form codegen entirely and
//!    materialise the value as the adapter's expected lvalue
//!    type directly. (Compile-time use of binder metadata is a
//!    follow-on; for now polydat consumes the binder only for
//!    verification.)
//!
//! ## What a binder is NOT
//!
//! - It is **not** a workload-author syntax. Workload authors
//!   keep writing their adapter-native op templates (e.g. a CQL
//!   `prepared:` statement with `{wire}` references); the
//!   adapter constructs binders internally during its op-template
//!   processing (CQL: prepare the statement, read parameter
//!   metadata, build the binder from that).
//!
//! - It is **not** compulsory for polydat callers in general.
//!   Polydat works without any binders submitted; the binder API
//!   is an opt-in surface for callers that have typed-parameter
//!   intent to declare.
//!
//! - It IS compulsory for **adapters** via the
//!   `DriverAdapter::binders_for` trait method — every adapter
//!   has to either declare its binders or explicitly acknowledge
//!   it has none. Default no-op declarations are forbidden by
//!   trait design so the question can't be skipped silently.
//!
//! ## Pre-canned binder patterns
//!
//! Three [`Binder`] variants cover the protocol shapes seen so
//! far: positional (CQL `?` placeholders, ordinal RPC params),
//! named (CQL `:name`, JSON-RPC), and single-value (one whole
//! field bound as one typed value).

use crate::ast::PortType;
use std::collections::BTreeMap;

/// One adapter-declared binding shape for one op-template field.
///
/// Three variants cover the common protocol shapes. The `field`
/// names the op-template field this binder describes — surfaced
/// in error diagnostics so the operator can locate the slot
/// quickly.
#[derive(Debug, Clone)]
pub enum Binder {
    /// Positional binding (slot index = position in vec).
    ///
    /// Example: a CQL prepared statement with three `?`
    /// placeholders → three positional slots. The slot at index 0
    /// binds to the first `?`, etc.
    Positional {
        field: String,
        slots: Vec<BinderSlot>,
    },

    /// Named binding (slot key = parameter name).
    ///
    /// Example: a CQL prepared statement with `:id` / `:val`
    /// named parameters → two named slots keyed by `id` / `val`.
    /// `BTreeMap` for deterministic diagnostic ordering.
    Named {
        field: String,
        slots: BTreeMap<String, BinderSlot>,
    },

    /// Single-value binding (the whole field IS one typed value).
    ///
    /// Example: an HTTP body bound as one JSON object, an MQTT
    /// payload bound as Bytes. The slot supplies the field's
    /// entire value with the lvalue type declared by the
    /// protocol.
    Single {
        field: String,
        slot: BinderSlot,
    },
}

/// One slot in a binder.
///
/// `wire` names the polydat wire that supplies the value at
/// execute time (bare wire name, resolved through the kernel's
/// GK context). `lvalue_type` is what the protocol expects at
/// this binding site — typically obtained from adapter-side
/// introspection (CQL: column type from prepared-statement
/// metadata).
///
/// `allow_fusion` is the per-slot policy bit the caller sets
/// when it has license — usually from a workload-author opt-in
/// like the bind-point `:*` wildcard syntax — to accept polydat
/// type fusion at this slot. When `true`, the verifier skips
/// the strict rvalue→lvalue rule for this slot and accepts any
/// rvalue (the wire-existence check still fires; an unknown
/// wire is always a violation). When `false` (the default), the
/// strict rule applies: string-detour into a non-text-natural
/// lvalue is rejected.
///
/// The `lvalue_type` field stays honest in both cases —
/// callers don't fake the type to Str to bypass the check;
/// they keep the real cluster-reported type AND set
/// `allow_fusion: true`. That way downstream consumers
/// (compile-time analysis, diagnostics, future runtime-typed
/// bind paths) see the true protocol-side type.
#[derive(Debug, Clone)]
pub struct BinderSlot {
    pub wire: String,
    pub lvalue_type: PortType,
    pub allow_fusion: bool,
}

impl Binder {
    /// Op-template field this binder describes.
    pub fn field(&self) -> &str {
        match self {
            Binder::Positional { field, .. } => field,
            Binder::Named { field, .. } => field,
            Binder::Single { field, .. } => field,
        }
    }

    /// Walk every slot in this binder for verification or
    /// codegen. Yields `(slot_label, slot)` pairs where
    /// `slot_label` is a human-readable position hint
    /// (`"[0]"` for positional, `":name"` for named, `""` for
    /// single).
    pub fn slots(&self) -> Vec<(String, &BinderSlot)> {
        match self {
            Binder::Positional { slots, .. } => slots.iter().enumerate()
                .map(|(i, s)| (format!("[{i}]"), s))
                .collect(),
            Binder::Named { slots, .. } => slots.iter()
                .map(|(name, s)| (format!(":{name}"), s))
                .collect(),
            Binder::Single { slot, .. } => vec![(String::new(), slot)],
        }
    }
}

/// One wire-type-vs-lvalue-type mismatch found by
/// [`verify_binders`].
#[derive(Debug, Clone)]
pub struct BinderViolation {
    pub field: String,
    pub slot_label: String,
    pub wire: String,
    /// Wire's rvalue type as resolved from the program, or
    /// `None` if the wire wasn't declared in the program at all
    /// (a separate error — the binder names a wire the kernel
    /// doesn't know).
    pub rvalue_type: Option<PortType>,
    pub lvalue_type: PortType,
    pub message: String,
}

/// Verify each binder slot's rvalue (wire) type against its
/// declared lvalue type, using the lookup closure to resolve
/// wire names to their `PortType`. The closure returns `None`
/// for unknown wires.
///
/// Returns every violation found (not just the first) so the
/// operator fixes them in one pass.
///
/// Why a closure instead of `&GkProgram`: the verifier
/// shouldn't be coupled to one program/kernel surface. Callers
/// supply whatever lookup matches their wire-resolution
/// context — kernel program output table, scope-init constants,
/// auto-externed parent-scope wires, or test fixtures.
/// Verify binders against a [`crate::kernel::GkKernel`] directly,
/// returning `Ok(())` when every slot's rvalue→lvalue check
/// passes and `Err(Vec<BinderViolation>)` listing every failure
/// otherwise.
///
/// Convenience wrapper around [`verify_binders`] for the common
/// case where the wire-type lookup comes from a polydat kernel
/// the adapter was handed during dispenser init. Adapters call
/// this inline inside their `map_op` as part of completing the
/// currying stack — see `nbrs_activity::adapter::DriverAdapter::map_op`
/// for the per-op compulsion to verify any typed binders before
/// returning the constructed dispenser.
///
/// Looks up each binder slot's wire as either an output or an
/// input of the kernel's program (outputs are checked first;
/// inputs are the fallback for coordinate/extern wires the
/// op-template kernel auto-externs from outer scope). A wire
/// that resolves to neither surfaces as a "not declared" binder
/// violation rather than silent passthrough.
pub fn verify_against_kernel(
    binders: &[Binder],
    kernel: &crate::kernel::GkKernel,
) -> Result<(), Vec<BinderViolation>> {
    use crate::kernel::Metadata;
    let violations = verify_binders(binders, |name: &str| {
        // Output first (most common: phase-scope bindings,
        // op-template binding outputs). Fall back to input
        // (coordinate / extern wires the program declared as
        // typed input slots).
        kernel.output_port_type(name)
            .or_else(|| kernel.input_port_type(name))
    });
    if violations.is_empty() {
        Ok(())
    } else {
        Err(violations)
    }
}

pub fn verify_binders(
    binders: &[Binder],
    wire_type: impl Fn(&str) -> Option<PortType>,
) -> Vec<BinderViolation> {
    let mut violations = Vec::new();
    for binder in binders {
        for (slot_label, slot) in binder.slots() {
            let rvalue = wire_type(&slot.wire);
            if let Some(msg) =
                check_compatibility(rvalue, slot.lvalue_type, &slot.wire, slot.allow_fusion)
            {
                violations.push(BinderViolation {
                    field: binder.field().to_string(),
                    slot_label,
                    wire: slot.wire.clone(),
                    rvalue_type: rvalue,
                    lvalue_type: slot.lvalue_type,
                    message: msg,
                });
            }
        }
    }
    violations
}

/// The load-bearing rule, stated once:
///
/// > A string detour (rvalue → `to_display_string` → bytes →
/// > parser → typed wire form) is permitted only when the lvalue
/// > is itself a string-natural type.
///
/// Applied as: if the rvalue type is `Str` and the lvalue type
/// is *not* text-natural, that's a string detour into a non-
/// string lvalue — the bug class the binder API exists to catch.
///
/// For non-string-detour cases, this function applies a
/// conservative match: same type → OK, structurally-compatible
/// vector type → OK, otherwise → reject. The intent is to catch
/// the obviously-wrong cases (VecF32 wire into a text lvalue,
/// Str wire into a vector lvalue) without trying to express
/// every coercion polydat's [`crate::compile::assembly::auto_adapter`]
/// would happily insert. Refinement against that adapter table
/// is a follow-on.
fn check_compatibility(
    rvalue: Option<PortType>,
    lvalue: PortType,
    wire: &str,
    allow_fusion: bool,
) -> Option<String> {
    let Some(rv) = rvalue else {
        // Unknown-wire violation is structural — always fires,
        // independent of `allow_fusion`. A binder that names a
        // non-existent wire is broken regardless of whether the
        // caller would tolerate type fusion at that slot.
        return Some(format!(
            "binder names wire `{wire}` (lvalue type {lvalue}) but the \
             wire is not declared in the kernel's GK context — this \
             binder names a wire that doesn't exist."));
    };

    if rv == lvalue { return None; }

    // Caller-licensed fusion: the slot was tagged `allow_fusion`.
    // The rvalue→lvalue strict rule is intentionally skipped for
    // this slot — polydat trusts the caller's judgement that any
    // protocol-side coercion / text-detour the adapter performs
    // at bind time is acceptable for this position.
    //
    // The text-natural-lvalue auto-permit that used to live in
    // this file moved out to the caller side: an adapter whose
    // protocol can text-coerce anything to its string parameter
    // type is expected to set `allow_fusion: true` on slots
    // with `Str`-lvalue (in non-strict mode). That makes the
    // fusion policy explicit and visible on the binder slot
    // rather than implicit in polydat's rule. Strict-mode
    // adapters that DO want polydat to reject `Str + non-Str`
    // simply leave `allow_fusion: false`.
    if allow_fusion { return None; }

    // Strict path. The only way the rvalue is OK is if it
    // structurally matches the lvalue. Bail with a clear
    // diagnostic pointing at both types.
    if structurally_compatible(rv, lvalue) {
        return None;
    }
    Some(format!(
        "wire `{wire}` holds {rv} but the binder declares an lvalue \
         type of {lvalue} — strict binder verification (no \
         `allow_fusion`) rejects the rvalue/lvalue pair. Bind a wire \
         whose type matches the lvalue directly, change the lvalue \
         type at the adapter side, or — if the workload author \
         intends to license polydat to fuse types at this slot — \
         spell the bind-point with the `:*` wildcard suffix \
         (e.g. `{{{wire}:*}}` in place of `{{{wire}}}`) so the binder \
         slot's `allow_fusion` flag is set."))
}

/// Conservative structural match for the non-text-natural case.
/// Same type matches; vector types match by element type. The
/// intent is to catch the obvious mismatches without trying to
/// be the full polydat auto-adapter table — that's a follow-on
/// once we have call sites that need finer-grained rules.
fn structurally_compatible(rv: PortType, lv: PortType) -> bool {
    match (rv, lv) {
        (PortType::VecF32, PortType::VecF32) => true,
        (PortType::VecI32, PortType::VecI32) => true,
        (PortType::Bytes,  PortType::Bytes)  => true,
        // Numeric widening — both sides are scalar numerics
        // (none of these is text-natural, so this branch is
        // about within-numeric compatibility).
        (PortType::U32, PortType::U64) => true,
        (PortType::I32, PortType::I64) => true,
        (PortType::F32, PortType::F64) => true,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn wire_lookup(types: &[(&str, PortType)]) -> impl Fn(&str) -> Option<PortType> {
        let map: HashMap<String, PortType> = types.iter()
            .map(|(n, t)| (n.to_string(), *t))
            .collect();
        move |name: &str| map.get(name).copied()
    }

    #[test]
    fn positional_binder_matches_types_cleanly() {
        let binder = Binder::Positional {
            field: "prepared".into(),
            slots: vec![
                BinderSlot { wire: "id".into(),  lvalue_type: PortType::Str, allow_fusion: false },
                BinderSlot { wire: "vec".into(), lvalue_type: PortType::VecF32, allow_fusion: false },
            ],
        };
        let v = verify_binders(&[binder], wire_lookup(&[
            ("id",  PortType::Str),
            ("vec", PortType::VecF32),
        ]));
        assert!(v.is_empty(), "matched-type binder should verify clean: {v:?}");
    }

    /// The load-bearing case: a wire holding VecF32 is bound
    /// to a non-text lvalue of a different type → reject. This
    /// is the equivalent of "decimal-stringify the 128 f32s and
    /// hope the cluster parses them back" → no.
    #[test]
    fn vec_f32_wire_into_non_text_non_vector_lvalue_is_rejected() {
        let binder = Binder::Positional {
            field: "prepared".into(),
            slots: vec![
                BinderSlot { wire: "vec".into(), lvalue_type: PortType::Bytes, allow_fusion: false },
            ],
        };
        let v = verify_binders(&[binder], wire_lookup(&[
            ("vec", PortType::VecF32),
        ]));
        assert_eq!(v.len(), 1);
        assert!(v[0].message.contains("vec_f32"),
            "diagnostic should name rvalue: {}", v[0].message);
        assert!(v[0].message.contains("bytes") || v[0].message.contains("Bytes"),
            "diagnostic should name lvalue: {}", v[0].message);
        assert_eq!(v[0].rvalue_type, Some(PortType::VecF32));
        assert_eq!(v[0].lvalue_type, PortType::Bytes);
    }

    /// Strict mode rejects rvalue→Str-lvalue mismatches. The
    /// text-natural auto-permit USED to live in polydat
    /// (`is_text_natural`); it has moved to the caller side as
    /// an explicit `allow_fusion: true` policy bit. With
    /// `allow_fusion: false`, even a Str lvalue rejects a
    /// non-Str rvalue — and the diagnostic points at the `:*`
    /// opt-in.
    #[test]
    fn strict_rejects_non_str_rvalue_into_str_lvalue() {
        let strict_binder = Binder::Positional {
            field: "prepared".into(),
            slots: vec![
                BinderSlot { wire: "vec".into(), lvalue_type: PortType::Str, allow_fusion: false },
            ],
        };
        let v = verify_binders(&[strict_binder], wire_lookup(&[
            ("vec", PortType::VecF32),
        ]));
        assert_eq!(v.len(), 1,
            "strict (allow_fusion=false) should reject VecF32→Str: {v:?}");
    }

    /// Caller-side opt-in: the same rvalue/lvalue pair is
    /// accepted when the slot carries `allow_fusion: true`.
    /// The adapter sets this for `Str`-lvalue slots in
    /// non-strict mode; the workload-author can also opt in
    /// per-slot via the `:*` wildcard syntax.
    #[test]
    fn allow_fusion_accepts_non_str_rvalue_into_str_lvalue() {
        let fusing_binder = Binder::Positional {
            field: "prepared".into(),
            slots: vec![
                BinderSlot { wire: "vec".into(), lvalue_type: PortType::Str, allow_fusion: true },
                BinderSlot { wire: "num".into(), lvalue_type: PortType::Json, allow_fusion: true },
            ],
        };
        let v = verify_binders(&[fusing_binder], wire_lookup(&[
            ("vec", PortType::VecF32),
            ("num", PortType::F64),
        ]));
        assert!(v.is_empty(),
            "allow_fusion=true should accept any rvalue into any lvalue: {v:?}");
    }

    /// Unknown wire → loud error, no silent skipping. A binder
    /// that names a wire the kernel doesn't know is a bug (the
    /// adapter built the binder against an out-of-date wire
    /// list, or a workload typo'd the field).
    #[test]
    fn unknown_wire_in_binder_is_loud_error() {
        let binder = Binder::Positional {
            field: "prepared".into(),
            slots: vec![
                BinderSlot { wire: "nonexistent".into(), lvalue_type: PortType::Str, allow_fusion: false },
            ],
        };
        let v = verify_binders(&[binder], wire_lookup(&[]));
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].rvalue_type, None);
        assert!(v[0].message.contains("not declared"),
            "diagnostic should call out the unknown wire: {}", v[0].message);
    }

    /// Named binder: same rules, keyed-by-name diagnostics.
    #[test]
    fn named_binder_violations_carry_name_label() {
        let mut slots = BTreeMap::new();
        slots.insert("vec_param".into(),
            BinderSlot { wire: "v".into(), lvalue_type: PortType::I64, allow_fusion: false });
        let binder = Binder::Named { field: "prepared".into(), slots };
        let v = verify_binders(&[binder], wire_lookup(&[
            ("v", PortType::VecF32),
        ]));
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].slot_label, ":vec_param",
            "named-slot diagnostic should carry the name: {:?}", v[0]);
    }

    /// Single-value binder: one slot, slot label empty.
    #[test]
    fn single_binder_violation_is_locatable() {
        let binder = Binder::Single {
            field: "body".into(),
            slot: BinderSlot { wire: "payload".into(), lvalue_type: PortType::Bytes, allow_fusion: false },
        };
        let v = verify_binders(&[binder], wire_lookup(&[
            ("payload", PortType::VecF32),
        ]));
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].slot_label, "",
            "single-slot label is empty: {:?}", v[0]);
        assert_eq!(v[0].field, "body");
    }

    /// Numeric widening within the non-text-natural side is
    /// allowed. U32 wire → U64 lvalue, etc.
    #[test]
    fn numeric_widening_is_accepted() {
        let binder = Binder::Positional {
            field: "prepared".into(),
            slots: vec![
                BinderSlot { wire: "a".into(), lvalue_type: PortType::U64, allow_fusion: false },
                BinderSlot { wire: "b".into(), lvalue_type: PortType::F64, allow_fusion: false },
            ],
        };
        let v = verify_binders(&[binder], wire_lookup(&[
            ("a", PortType::U32),
            ("b", PortType::F32),
        ]));
        assert!(v.is_empty(), "widening U32→U64 / F32→F64 should verify clean: {v:?}");
    }

    /// Per-slot `allow_fusion: true` skips the strict
    /// rvalue→lvalue check. A wire holding `Str` bound to an
    /// `I32` lvalue normally fails (the load-bearing
    /// string-detour-into-non-text rule), but with the slot
    /// tagged `allow_fusion: true` polydat accepts it — the
    /// caller has licensed type fusion at this position.
    #[test]
    fn allow_fusion_skips_strict_check_for_wired_slot() {
        let strict_binder = Binder::Positional {
            field: "prepared".into(),
            slots: vec![
                BinderSlot { wire: "x".into(), lvalue_type: PortType::I32, allow_fusion: false },
            ],
        };
        let fusing_binder = Binder::Positional {
            field: "prepared".into(),
            slots: vec![
                BinderSlot { wire: "x".into(), lvalue_type: PortType::I32, allow_fusion: true },
            ],
        };
        let lookup = wire_lookup(&[("x", PortType::Str)]);
        // Strict: rejected (Str into I32, not text-natural).
        let strict = verify_binders(&[strict_binder], &lookup);
        assert_eq!(strict.len(), 1, "strict slot should reject: {strict:?}");
        // Fusing: accepted; same rvalue/lvalue pair.
        let fusing = verify_binders(&[fusing_binder], &lookup);
        assert!(fusing.is_empty(),
            "allow_fusion=true should skip strict rule: {fusing:?}");
    }

    /// `allow_fusion: true` does NOT silence the unknown-wire
    /// violation. Naming a non-existent wire is a structural
    /// bug regardless of how the caller wants typed binding
    /// applied at that slot.
    #[test]
    fn allow_fusion_still_reports_unknown_wires() {
        let binder = Binder::Positional {
            field: "prepared".into(),
            slots: vec![
                BinderSlot { wire: "ghost".into(), lvalue_type: PortType::I32, allow_fusion: true },
            ],
        };
        let v = verify_binders(&[binder], wire_lookup(&[]));
        assert_eq!(v.len(), 1, "unknown wire must fire even with fusion: {v:?}");
        assert!(v[0].message.contains("not declared"),
            "expected unknown-wire diagnostic: {}", v[0].message);
    }

    /// Multiple violations across binders are all reported.
    #[test]
    fn all_violations_across_binders_are_reported() {
        let b1 = Binder::Positional {
            field: "f1".into(),
            slots: vec![
                BinderSlot { wire: "a".into(), lvalue_type: PortType::Bytes, allow_fusion: false },
            ],
        };
        let b2 = Binder::Positional {
            field: "f2".into(),
            slots: vec![
                BinderSlot { wire: "b".into(), lvalue_type: PortType::VecF32, allow_fusion: false },
            ],
        };
        let v = verify_binders(&[b1, b2], wire_lookup(&[
            ("a", PortType::VecF32),  // VecF32 → Bytes : reject
            ("b", PortType::Str),     // Str → VecF32 : reject (str-detour-into-non-text)
        ]));
        assert_eq!(v.len(), 2, "both violations expected: {v:?}");
    }
}
