// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Invariant guard for the adapter catalogs (`type_system.md` §3).
//!
//! The catalog hand-mirroring that let the doc matrix drift (two
//! cells were wrong before the §3 rework) is the failure mode this
//! file closes. Three properties are asserted directly against the
//! real `auto_adapter` / `boundary_adapter` functions — no separate
//! copy of the table to keep in sync:
//!
//! 1. **Widening totality** — every conversion the class-A *spec*
//!    requires (lossless widening, canonical `int → f64`,
//!    `bool ↔ numeric`, `int → bool`) is present in `auto_adapter`.
//!    A hole here is a surprise for authors ("why won't `u8` flow
//!    into an `i32` slot?"), so the spec is encoded as a predicate
//!    and checked exhaustively.
//! 2. **`auto ⊆ boundary`** — every intra-graph adapter is also a
//!    boundary adapter (the boundary catalog is a strict superset).
//! 3. **Doc matrix is current** — the grid in `type_system.md` §3
//!    is re-derived from the catalog and compared cell-by-cell, so
//!    a catalog change that isn't reflected in the doc fails CI.

use polydat::ast::PortType;
use polydat::compile::assembly::{auto_adapter, boundary_adapter};

// ── The class-A spec, as a predicate ──────────────────────────────

/// Numeric classification used by the widening predicate.
#[derive(Clone, Copy)]
enum Num {
    /// Unsigned integer of N bits.
    U(u32),
    /// Signed integer of N bits.
    S(u32),
    /// Float with N bits of exact-integer precision (f16=11,
    /// f32=24, f64=53).
    F(u32),
    Bool,
}

fn classify(p: PortType) -> Option<Num> {
    use PortType as P;
    Some(match p {
        P::U8 => Num::U(8),
        P::U16 => Num::U(16),
        P::U32 => Num::U(32),
        P::U64 => Num::U(64),
        P::U128 => Num::U(128),
        P::I8 => Num::S(8),
        P::I16 => Num::S(16),
        P::I32 => Num::S(32),
        P::I64 => Num::S(64),
        P::I128 => Num::S(128),
        P::F16 => Num::F(11),
        P::F32 => Num::F(24),
        P::F64 => Num::F(53),
        P::Bool => Num::Bool,
        _ => return None,
    })
}

/// An integer of `width` bits (signed or not) fits exactly in a
/// float with `exact` bits of integer precision iff its magnitude
/// needs no more than `exact` bits.
fn int_fits_float(signed: bool, width: u32, exact: u32) -> bool {
    let needed = if signed { width - 1 } else { width };
    needed <= exact
}

/// The class-A specification: conversions that must **always** be
/// available because they never panic and are either lossless or a
/// privileged canonical widening. This is the invariant the matrix
/// promises; `auto_adapter` must implement every pair it admits.
fn class_a_spec(from: PortType, to: PortType) -> bool {
    if from == to {
        return false;
    }
    let (Some(f), Some(t)) = (classify(from), classify(to)) else {
        return false;
    };
    use Num::{Bool, F, S, U};
    match (f, t) {
        (Bool, Bool) => false,
        // bool widens to any numeric (1/0); any numeric reduces to
        // bool (nonzero test). Both directions are total.
        (Bool, _) | (_, Bool) => true,
        // int → int: lossless range widening only.
        (U(a), U(b)) => b >= a,
        (U(a), S(b)) => b > a, // unsigned needs a strictly-wider signed
        (S(a), S(b)) => b >= a,
        (S(_), U(_)) => false, // negatives are unrepresentable
        // int → float: `→ f64` is the canonical (always-A, may
        // round) widening; `→ f32`/`→ f16` are class A only when
        // the integer range fits the float's exact-int window.
        (U(a), F(eb)) => to == PortType::F64 || int_fits_float(false, a, eb),
        (S(a), F(eb)) => to == PortType::F64 || int_fits_float(true, a, eb),
        // float → float: widen by precision rank (f16 < f32 < f64).
        (F(a), F(b)) => b > a,
        // float → int is never class A (lossy / range-checked).
        (F(_), _) => false,
    }
}

// ── Type lists ────────────────────────────────────────────────────

/// Every conversion type the matrix lists (rows == columns). Order
/// matches the doc grid so the drift check lines up.
const MATRIX_TYPES: &[(PortType, &str)] = &[
    (PortType::U8, "u8"),
    (PortType::I8, "i8"),
    (PortType::U16, "u16"),
    (PortType::I16, "i16"),
    (PortType::F16, "f16"),
    (PortType::U32, "u32"),
    (PortType::I32, "i32"),
    (PortType::F32, "f32"),
    (PortType::U64, "u64"),
    (PortType::I64, "i64"),
    (PortType::F64, "f64"),
    (PortType::U128, "u12"),
    (PortType::I128, "i12"),
    (PortType::Bool, "bl"),
    (PortType::Str, "str"),
    (PortType::Bytes, "by"),
    (PortType::Json, "js"),
    (PortType::VecF32, "vF"),
    (PortType::VecI32, "vI"),
    (PortType::VecF64, "vD"),
    (PortType::VecI64, "vL"),
    (PortType::VecF16, "vH"),
    (PortType::VecI16, "vS"),
    (PortType::VecI8, "v8"),
];

/// Scalars (incl. `Bool`) and vectors never inter-convert: a
/// scalar has no canonical vector length, a vector no canonical
/// scalar reduction. Every *other* pair is meaningful and must
/// have at least a boundary adapter (`use vec_len`/`vec_first`/…
/// for the deliberate vec→scalar reductions).
fn is_scalar(p: PortType) -> bool {
    classify(p).is_some()
}
fn is_vector(p: PortType) -> bool {
    matches!(
        p,
        PortType::VecF32 | PortType::VecI32 | PortType::VecF64 | PortType::VecI64
            | PortType::VecF16 | PortType::VecI16 | PortType::VecI8
    )
}
fn is_meaningful(from: PortType, to: PortType) -> bool {
    if from == to {
        return false;
    }
    if (is_scalar(from) && is_vector(to)) || (is_vector(from) && is_scalar(to)) {
        return false;
    }
    true
}

fn cell(from: PortType, to: PortType) -> char {
    if from == to {
        '─'
    } else if auto_adapter(from, to).is_some() {
        'A'
    } else if boundary_adapter(from, to).is_some() {
        'B'
    } else {
        '·'
    }
}

// ── 1. Widening totality ──────────────────────────────────────────

#[test]
fn class_a_spec_is_fully_covered() {
    let mut missing = Vec::new();
    for &(f, _) in MATRIX_TYPES {
        for &(t, _) in MATRIX_TYPES {
            if class_a_spec(f, t) && auto_adapter(f, t).is_none() {
                missing.push(format!("{f:?}→{t:?}"));
            }
        }
    }
    assert!(
        missing.is_empty(),
        "auto_adapter is missing {} class-A (always-defined) conversion(s) \
         the widening invariant requires (type_system.md §3.3). Add the node \
         + catalog arm + `known` mirror entry for each:\n  {}",
        missing.len(),
        missing.join("\n  "),
    );
}

// ── 2. auto ⊆ boundary ────────────────────────────────────────────

#[test]
fn auto_adapter_is_subset_of_boundary() {
    let mut violations = Vec::new();
    for &(f, _) in MATRIX_TYPES {
        for &(t, _) in MATRIX_TYPES {
            if auto_adapter(f, t).is_some() && boundary_adapter(f, t).is_none() {
                violations.push(format!("{f:?}→{t:?}"));
            }
        }
    }
    assert!(
        violations.is_empty(),
        "boundary_adapter must be a superset of auto_adapter, but these \
         intra-graph adapters have no boundary entry:\n  {}",
        violations.join("\n  "),
    );
}

// ── 2b. Completeness — every meaningful pair is healable ──────────

#[test]
fn every_meaningful_pair_has_an_adapter() {
    let mut missing = Vec::new();
    for &(f, _) in MATRIX_TYPES {
        for &(t, _) in MATRIX_TYPES {
            if is_meaningful(f, t) && boundary_adapter(f, t).is_none() {
                missing.push(format!("{f:?}→{t:?}"));
            }
        }
    }
    assert!(
        missing.is_empty(),
        "the conversion matrix has {} unfilled meaningful cell(s) — every \
         scalar↔scalar, scalar↔container, container↔container, vec↔vec, and \
         vec↔container pair must have an adapter (only scalar↔vec stays `·`):\n  {}",
        missing.len(),
        missing.join("\n  "),
    );
}

// ── 3. Doc matrix is current ──────────────────────────────────────

/// Render the coverage grid exactly as `type_system.md` §3 shows it.
fn render_matrix() -> String {
    let mut out = String::from("from╲to");
    for &(_, ab) in MATRIX_TYPES {
        out.push_str(&format!("{ab:>4}"));
    }
    out.push('\n');
    for &(f, fab) in MATRIX_TYPES {
        out.push_str(&format!("{fab:<7}"));
        for &(t, _) in MATRIX_TYPES {
            out.push_str(&format!("{:>4}", cell(f, t)));
        }
        out.push('\n');
    }
    out
}

#[test]
fn doc_matrix_matches_catalog() {
    let doc = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/docs/design/type_system.md"
    ))
    .expect("type_system.md should be readable from the crate dir");

    // Extract the fenced block whose header row starts with "from╲to".
    let block: String = {
        let mut lines = doc.lines().peekable();
        let mut found = None;
        while let Some(line) = lines.next() {
            if line.trim_start().starts_with("```") {
                // peek the next line for the matrix header
                if let Some(next) = lines.peek()
                    && next.starts_with("from╲to")
                {
                    let mut grid = Vec::new();
                    for body in lines.by_ref() {
                        if body.trim_start().starts_with("```") {
                            break;
                        }
                        // stop at the legend (blank line then "A = …")
                        if body.trim_start().starts_with("A =") || body.trim().is_empty() {
                            break;
                        }
                        grid.push(body);
                    }
                    found = Some(grid.join("\n"));
                    break;
                }
            }
        }
        found.expect("matrix grid (header `from╲to`) not found in type_system.md")
    };

    let expected = render_matrix();
    let expected_grid = expected.trim_end();
    assert_eq!(
        block.trim_end(),
        expected_grid,
        "\nThe matrix in type_system.md §3 is stale. Replace the grid with the \
         catalog-derived version below:\n\n{expected_grid}\n",
    );
}
