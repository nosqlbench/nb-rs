// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Exported-wire type fidelity (the `query_vector` VecF32→Str
//! regression class, `local/CARE_PACKAGE_query_vector_typing.md`).
//!
//! Governing principle (`feedback_no_auto_string_conversion`): native
//! types stay native through data passing; string rendering happens
//! only at presentation points. A typed wire consumed by BOTH a typed
//! sink and a text-formatting sink must keep its native exported type
//! — the formatting consumer gets its rendering on its own edge, never
//! by rewiring the export.
//!
//! These tests pin the invariant at the compiled-kernel surface the
//! binder consults (`output_port_type`), using the exact shape of the
//! `pvs_query_sweep` phase kernel (a dataset accessor producing
//! VecF32, flattened op scope, no shadow).

use polydat::ast::PortType;
use polydat::dsl::compile::compile_polydat;
use polydat::kernel::Metadata;

/// The sweep-phase shape: a VecF32-producing dataset accessor bound to
/// a name. The exported wire's type — what the CQL binder reads as the
/// rvalue — must be VecF32.
#[test]
fn dataset_accessor_export_is_native_vec_f32() {
    let src = r#"
input prebuffered: handle
input q: u64
query_vector := query_vector_at(prebuffered, q)
"#;
    let kernel = compile_polydat(src).expect("compile");
    assert_eq!(
        kernel.output_port_type("query_vector"),
        Some(PortType::VecF32),
        "the exported dataset-accessor wire must keep its native type"
    );
}

/// Care-package step 4 — the two-consumer kernel: the same VecF32 wire
/// feeds a typed downstream consumer AND a text-formatting consumer
/// (`printf`, the lowering of every string-interpolation template).
/// The export must STAY VecF32; the formatting consumer's Str lives on
/// its own output only.
#[test]
fn text_consumer_does_not_downgrade_the_export() {
    let src = r#"
input prebuffered: handle
input q: u64
query_vector := query_vector_at(prebuffered, q)
normed := vec_norm(query_vector)
rendered := printf("ann of {}", query_vector)
"#;
    let kernel = compile_polydat(src).expect("compile");
    assert_eq!(
        kernel.output_port_type("query_vector"),
        Some(PortType::VecF32),
        "a formatting consumer must not rewire the export through a \
         Str conversion (per-consumer-edge adapters only)"
    );
    assert_eq!(
        kernel.output_port_type("normed"),
        Some(PortType::VecF32),
        "typed consumers downstream of the shared wire stay native"
    );
    assert_eq!(
        kernel.output_port_type("rendered"),
        Some(PortType::Str),
        "the formatting consumer's own output is Str — presentation \
         happens on its edge, not on the source wire"
    );
}
