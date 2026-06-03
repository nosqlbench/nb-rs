// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! CQL parameter-type → polydat `PortType` mapping for the
//! cassandra-cpp driver.
//!
//! Same shape and policy as `super::super::scylla::binder_meta` —
//! the C++ driver exposes `cass::ValueType` instead of scylla's
//! Rust-native `ColumnType<'frame>`, so the lookup table is
//! mechanically distinct but the policy decisions match exactly.
//! See the scylla module's docs for the per-type rationale.
//!
//! Fallback slots return their CQL type label as the second
//! tuple element of [`Mapping`] so `map_op` can WARN about which
//! positions lost typed verification.

use cassandra_cpp as cass;
use polydat::ast::PortType;

/// Result of mapping one CQL `cass::ValueType` to a polydat
/// `PortType`. The `Option<String>` carries the CQL-type label
/// when this slot fell back to a permissive mapping (currently
/// always `PortType::Str`). `None` means the mapping is exact.
pub type Mapping = (PortType, Option<String>);

/// Map a cassandra-cpp `cass::ValueType` to a polydat
/// `PortType`, plus an optional CQL-type-label for the fallback
/// warning.
///
/// Common cases map directly; long-tail types fall back to `Str`
/// per the agreed permissive policy. Fallback slots are surfaced
/// via `Some("<cql-type-label>")` so the caller (`map_op`) can
/// emit a WARN identifying which `?` positions are no longer
/// being type-checked precisely.
///
/// For `CUSTOM` types specifically, the `class_name` parameter
/// (when supplied) is parsed for the cluster's class-name
/// format. The cassandra
/// `org.apache.cassandra.db.marshal.VectorType(<ElementType>, N)`
/// element → polydat `PortType` mapping:
///
/// | Cassandra element type | Polydat type    |
/// |------------------------|-----------------|
/// | `FloatType`            | `VecF32`        |
/// | `IntType`              | `VecI32`        |
/// | `DoubleType`           | `VecF64`        |
/// | `LongType`             | `VecI64`        |
/// | `ShortType`            | `VecI16`        |
/// | `HalfFloatType` /      | `VecF16`        |
/// |  `Float16Type`         |                 |
///
/// Unknown vector element types (and any other CUSTOM class)
/// fall back to `Str` with the class name labelled in the warning.
///
/// Callers pass `class_name: None` for non-CUSTOM types
/// (no introspection needed) or `Some(name)` when they've
/// already extracted it from a `ConstDataType` via
/// [`super::get_const_data_type_class_name`].
pub fn cass_to_polydat(t: cass::ValueType, class_name: Option<&str>) -> Mapping {
    match t {
        // Exact mappings.
        cass::ValueType::BOOLEAN  => (PortType::Bool, None),
        cass::ValueType::INT      => (PortType::I32, None),
        cass::ValueType::BIGINT   => (PortType::I64, None),
        cass::ValueType::COUNTER  => (PortType::I64, None),
        cass::ValueType::FLOAT    => (PortType::F32, None),
        cass::ValueType::DOUBLE   => (PortType::F64, None),
        cass::ValueType::ASCII    => (PortType::Str, None),
        cass::ValueType::TEXT     => (PortType::Str, None),
        cass::ValueType::VARCHAR  => (PortType::Str, None),
        cass::ValueType::BLOB     => (PortType::Bytes, None),
        // Widened mappings (smaller-than-32 signed integers).
        cass::ValueType::SMALL_INT => (PortType::I32, None),
        cass::ValueType::TINY_INT  => (PortType::I32, None),
        // CUSTOM: try to introspect the class name. Cassandra
        // vectors come back as CUSTOM with a Java FQN like
        // `org.apache.cassandra.db.marshal.VectorType(...)`.
        cass::ValueType::CUSTOM => match class_name.and_then(parse_vector_class_name) {
            Some(VectorElement::Float)  => (PortType::VecF32, None),
            Some(VectorElement::Int)    => (PortType::VecI32, None),
            Some(VectorElement::Double) => (PortType::VecF64, None),
            Some(VectorElement::Long)   => (PortType::VecI64, None),
            Some(VectorElement::Short)  => (PortType::VecI16, None),
            Some(VectorElement::Half)   => (PortType::VecF16, None),
            Some(VectorElement::Other(elem)) => (
                PortType::Str,
                Some(format!("CUSTOM<VectorType<{elem}>>")),
            ),
            None => match class_name {
                Some(cn) if !cn.is_empty() =>
                    (PortType::Str, Some(format!("CUSTOM<{cn}>"))),
                _ => (PortType::Str, Some("CUSTOM".to_string())),
            },
        },
        // TODO(precise mapping): remaining long-tail types fall
        // back to Str per the agreed permissive policy. Concrete
        // mappings can land per workload need (UUID, TIMESTAMP,
        // DATE, TIME, etc.).
        other => (PortType::Str, Some(format!("{other:?}"))),
    }
}

/// Result of parsing the element type out of a Cassandra
/// `VectorType(...)` class name. Only the two element types
/// with precise polydat counterparts get a typed variant;
/// everything else goes through `Other` so the caller can
/// label the slot specifically in its diagnostic.
/// Decoded element type from a `VectorType(...)` class name.
/// Public so `make_binder` in this module's sibling can dispatch
/// per element type and build a specialised typed-vector closure.
#[derive(Debug)]
pub enum VectorElement {
    Float,
    Int,
    Double,
    Long,
    Short,
    /// Half-precision float (Cassandra 5.x experimental short name
    /// `HalfFloatType` and Datastax/Apache variants `Float16Type`).
    Half,
    Other(String),
}

/// Parse a Cassandra Java FQN class name of the form
/// `*.VectorType(*.<ElementType>, <N>)` and return the element
/// short-name. Returns `None` when the class name doesn't match
/// the VectorType shape.
///
/// Examples accepted:
/// - `org.apache.cassandra.db.marshal.VectorType(org.apache.cassandra.db.marshal.FloatType, 128)`
///   → `VectorElement::Float`
/// - `org.apache.cassandra.db.marshal.VectorType(org.apache.cassandra.db.marshal.IntType, 4)`
///   → `VectorElement::Int`
/// - Same shape with any other element type → `VectorElement::Other("XxxType")`
/// Sibling-module public alias for [`parse_vector_class_name`].
/// Exposed under a stable name so `make_binder` can dispatch per
/// element type without depending on internal naming.
pub fn parse_vector_element(cn: &str) -> Option<VectorElement> {
    parse_vector_class_name(cn)
}

fn parse_vector_class_name(cn: &str) -> Option<VectorElement> {
    let open = cn.find(".VectorType(")?;
    let after_open = &cn[open + ".VectorType(".len()..];
    let close = after_open.rfind(')')?;
    let inner = &after_open[..close];
    let (element_fqn, _dims) = inner.split_once(',')?;
    let element_short = element_fqn.trim().rsplit('.').next()?.trim();
    match element_short {
        "FloatType"     => Some(VectorElement::Float),
        "IntType"       => Some(VectorElement::Int),
        "DoubleType"    => Some(VectorElement::Double),
        "LongType"      => Some(VectorElement::Long),
        "ShortType"     => Some(VectorElement::Short),
        "HalfFloatType" | "Float16Type" => Some(VectorElement::Half),
        other           => Some(VectorElement::Other(other.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The cluster format we actually observed: a vector<float, 128>
    /// reports as `org.apache.cassandra.db.marshal.VectorType(org.apache.cassandra.db.marshal.FloatType, 128)`.
    #[test]
    fn parses_observed_vector_float_class_name() {
        let cn = "org.apache.cassandra.db.marshal.VectorType(\
                  org.apache.cassandra.db.marshal.FloatType, 128)";
        assert!(matches!(parse_vector_class_name(cn), Some(VectorElement::Float)));
    }

    #[test]
    fn parses_vector_int_class_name() {
        let cn = "org.apache.cassandra.db.marshal.VectorType(\
                  org.apache.cassandra.db.marshal.IntType, 4)";
        assert!(matches!(parse_vector_class_name(cn), Some(VectorElement::Int)));
    }

    #[test]
    fn parses_vector_long_class_name() {
        let cn = "org.apache.cassandra.db.marshal.VectorType(\
                  org.apache.cassandra.db.marshal.LongType, 16)";
        assert!(matches!(parse_vector_class_name(cn), Some(VectorElement::Long)));
    }

    #[test]
    fn parses_vector_double_class_name() {
        let cn = "org.apache.cassandra.db.marshal.VectorType(\
                  org.apache.cassandra.db.marshal.DoubleType, 64)";
        assert!(matches!(parse_vector_class_name(cn), Some(VectorElement::Double)));
    }

    #[test]
    fn parses_vector_short_class_name() {
        let cn = "org.apache.cassandra.db.marshal.VectorType(\
                  org.apache.cassandra.db.marshal.ShortType, 32)";
        assert!(matches!(parse_vector_class_name(cn), Some(VectorElement::Short)));
    }

    #[test]
    fn parses_vector_half_float_class_name() {
        for name in ["HalfFloatType", "Float16Type"] {
            let cn = format!(
                "org.apache.cassandra.db.marshal.VectorType(\
                 org.apache.cassandra.db.marshal.{name}, 128)"
            );
            assert!(matches!(parse_vector_class_name(&cn), Some(VectorElement::Half)),
                "{name} should parse as Half");
        }
    }

    #[test]
    fn unknown_vector_element_falls_to_other() {
        let cn = "org.apache.cassandra.db.marshal.VectorType(\
                  org.apache.cassandra.db.marshal.DecimalType, 16)";
        match parse_vector_class_name(cn) {
            Some(VectorElement::Other(e)) => assert_eq!(e, "DecimalType"),
            other => panic!("expected Other(DecimalType), got {other:?}"),
        }
    }

    #[test]
    fn non_vector_custom_class_name_returns_none() {
        assert!(parse_vector_class_name("com.example.OpaqueType").is_none());
        assert!(parse_vector_class_name("").is_none());
    }

    /// End-to-end mapping for the observed cluster shape:
    /// CUSTOM + the Cassandra vector class name yields a precise
    /// VecF32 lvalue and no fallback warning.
    #[test]
    fn cass_to_polydat_maps_observed_vector_to_vec_f32() {
        let cn = "org.apache.cassandra.db.marshal.VectorType(\
                  org.apache.cassandra.db.marshal.FloatType, 128)";
        let (pt, fallback) = cass_to_polydat(cass::ValueType::CUSTOM, Some(cn));
        assert_eq!(pt, PortType::VecF32);
        assert!(fallback.is_none(),
            "precise vector mapping should not fire fallback: {fallback:?}");
    }

    /// CUSTOM with no class name falls back to Str with a
    /// generic `CUSTOM` label.
    #[test]
    fn cass_to_polydat_custom_no_class_name_falls_back() {
        let (pt, fallback) = cass_to_polydat(cass::ValueType::CUSTOM, None);
        assert_eq!(pt, PortType::Str);
        assert_eq!(fallback.as_deref(), Some("CUSTOM"));
    }
}
