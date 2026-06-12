// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Datetime and epoch function nodes.

/// Scale a u64 to epoch milliseconds by multiplying by a factor.
///
/// Signature: `(input: u64) -> (u64)`
/// Param: `factor: u64` — milliseconds per input unit.
///
/// Example: `EpochScale(1000)` treats input as seconds → millis.
/// Scale a u64 to epoch milliseconds. SRD-80 PR B.13 migration.
#[crate::polydat_node(category = Datetime)]
fn epoch_scale(
    input: u64,
    #[poly_default(1u64)] factor: crate::derive_support::Const<u64>,
) -> u64 {
    input.wrapping_mul(*factor)
}

impl EpochScale {
    pub fn millis() -> Self { Self::new(1) }
    pub fn seconds() -> Self { Self::new(1_000) }
    pub fn minutes() -> Self { Self::new(60_000) }
    pub fn hours() -> Self { Self::new(3_600_000) }
}

/// Add a base epoch offset to a u64 value. SRD-80 PR B.13 migration.
#[crate::polydat_node(category = Datetime)]
fn epoch_offset(
    input: u64,
    #[poly_default(0u64)] base_epoch_ms: crate::derive_support::Const<u64>,
) -> u64 {
    input.wrapping_add(*base_epoch_ms)
}

impl EpochOffset {
    /// 2024-01-01T00:00:00Z in epoch millis.
    pub fn from_2024() -> Self { Self::new(1_704_067_200_000) }
    /// 2025-01-01T00:00:00Z in epoch millis.
    pub fn from_2025() -> Self { Self::new(1_735_689_600_000) }
}

/// Format an epoch-millis u64 as an ISO-8601-like timestamp string.
///
/// Signature: `(input: u64) -> (String)`
///
/// Produces: `"YYYY-MM-DDThh:mm:ss.mmmZ"`
/// Uses a simple arithmetic calendar (no timezone, no leap second handling).
/// Convert u64 epoch millis to ISO-8601 timestamp string.
/// SRD-80 PR B.6 migration.
#[crate::polydat_node(category = Datetime)]
fn to_timestamp(input: u64) -> String {
    epoch_ms_to_iso(input)
}

/// Decompose epoch millis into date/time components.
/// SRD-80 PR B.10 — tuple-return multi-output.
#[crate::polydat_node(
    category = Datetime,
    output_names(year, month, day, hour, minute, second, millis),
)]
fn date_components(input: u64) -> (u64, u64, u64, u64, u64, u64, u64) {
    decompose_epoch_ms(input)
}

// --- Calendar arithmetic (simplified, no leap seconds) ---

const MILLIS_PER_SEC: u64 = 1_000;
#[allow(dead_code)]
const MILLIS_PER_MIN: u64 = 60_000;
#[allow(dead_code)]
const MILLIS_PER_HOUR: u64 = 3_600_000;
#[allow(dead_code)]
const MILLIS_PER_DAY: u64 = 86_400_000;

fn is_leap_year(y: u64) -> bool {
    (y.is_multiple_of(4) && !y.is_multiple_of(100)) || y.is_multiple_of(400)
}

fn days_in_month(y: u64, m: u64) -> u64 {
    match m {
        1 => 31, 2 => if is_leap_year(y) { 29 } else { 28 },
        3 => 31, 4 => 30, 5 => 31, 6 => 30,
        7 => 31, 8 => 31, 9 => 30, 10 => 31, 11 => 30, 12 => 31,
        _ => 30,
    }
}

fn decompose_epoch_ms(epoch_ms: u64) -> (u64, u64, u64, u64, u64, u64, u64) {
    let mut remaining = epoch_ms;
    let ms = remaining % MILLIS_PER_SEC;
    remaining /= MILLIS_PER_SEC;
    let sec = remaining % 60;
    remaining /= 60;
    let min = remaining % 60;
    remaining /= 60;
    let hour = remaining % 24;
    let mut days = remaining / 24;

    // Convert days since epoch (1970-01-01) to y/m/d
    let mut year = 1970u64;
    loop {
        let days_in_year = if is_leap_year(year) { 366 } else { 365 };
        if days < days_in_year { break; }
        days -= days_in_year;
        year += 1;
    }
    let mut month = 1u64;
    loop {
        let dim = days_in_month(year, month);
        if days < dim { break; }
        days -= dim;
        month += 1;
    }
    let day = days + 1;

    (year, month, day, hour, min, sec, ms)
}

fn epoch_ms_to_iso(epoch_ms: u64) -> String {
    let (y, mo, d, h, mi, s, ms) = decompose_epoch_ms(epoch_ms);
    format!("{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}:{s:02}.{ms:03}Z")
}

// ---------------------------------------------------------------------------
// Signature declarations for the DSL registry
// ---------------------------------------------------------------------------

use crate::dsl::registry::FuncSig;

/// Signatures for datetime nodes.
pub fn signatures() -> &'static [FuncSig] {
    
    &[
        // `epoch_scale` migrated to `#[polydat_node]` per SRD-80 PR B.13.
        // `epoch_offset` migrated to `#[polydat_node]` per SRD-80 PR B.13.
        // `to_timestamp` migrated to `#[polydat_node]` per SRD-80 PR B.6.
        // `date_components` migrated to `#[polydat_node]` per SRD-80 PR B.10.
    ]
}

/// Try to build a datetime node from a function name and const args.
///
/// Returns `None` if the name is not handled by this module.
pub(crate) fn build_node(_name: &str, _wires: &[crate::compile::assembly::WireRef], _wire_types: &[crate::ast::PortType], _consts: &[crate::dsl::factory::ConstArg]) -> Option<Result<Box<dyn crate::ast::PolydatNode>, String>> {
    None
}


crate::register_nodes!(signatures, build_node);
#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{PolydatNode, Value};

    #[test]
    fn epoch_scale_seconds() {
        let node = EpochScale::seconds();
        let mut out = [Value::None];
        node.eval(&[Value::U64(5)], &mut out);
        assert_eq!(out[0].as_u64(), 5000);
    }

    #[test]
    fn epoch_offset_basic() {
        let node = EpochOffset::new(1_000_000);
        let mut out = [Value::None];
        node.eval(&[Value::U64(500)], &mut out);
        assert_eq!(out[0].as_u64(), 1_000_500);
    }

    #[test]
    fn to_timestamp_epoch_zero() {
        let node = ToTimestamp::new();
        let mut out = [Value::None];
        node.eval(&[Value::U64(0)], &mut out);
        assert_eq!(out[0].as_str(), "1970-01-01T00:00:00.000Z");
    }

    #[test]
    fn to_timestamp_known_date() {
        let node = ToTimestamp::new();
        let mut out = [Value::None];
        // 2024-01-01T00:00:00.000Z = 1704067200000
        node.eval(&[Value::U64(1_704_067_200_000)], &mut out);
        assert_eq!(out[0].as_str(), "2024-01-01T00:00:00.000Z");
    }

    #[test]
    fn date_components_epoch_zero() {
        let node = DateComponents::new();
        let mut out = vec![Value::None; 7];
        node.eval(&[Value::U64(0)], &mut out);
        assert_eq!(out[0].as_u64(), 1970);
        assert_eq!(out[1].as_u64(), 1);
        assert_eq!(out[2].as_u64(), 1);
        assert_eq!(out[3].as_u64(), 0);
        assert_eq!(out[4].as_u64(), 0);
        assert_eq!(out[5].as_u64(), 0);
        assert_eq!(out[6].as_u64(), 0);
    }

    #[test]
    fn date_components_known() {
        let node = DateComponents::new();
        let mut out = vec![Value::None; 7];
        // 2024-03-15T14:30:45.123Z
        // Manually: days from epoch to 2024-03-15 = 19797
        // 19797 * 86400000 + 14*3600000 + 30*60000 + 45*1000 + 123
        let epoch = 19797u64 * MILLIS_PER_DAY + 14 * MILLIS_PER_HOUR
            + 30 * MILLIS_PER_MIN + 45 * MILLIS_PER_SEC + 123;
        node.eval(&[Value::U64(epoch)], &mut out);
        assert_eq!(out[0].as_u64(), 2024);
        assert_eq!(out[1].as_u64(), 3);
        assert_eq!(out[2].as_u64(), 15);
        assert_eq!(out[3].as_u64(), 14);
        assert_eq!(out[4].as_u64(), 30);
        assert_eq!(out[5].as_u64(), 45);
        assert_eq!(out[6].as_u64(), 123);
    }

    #[test]
    fn epoch_scale_compiled() {
        let node = EpochScale::seconds();
        let op = node.compiled_u64().unwrap();
        let mut out = [0u64];
        op(&[5], &mut out);
        assert_eq!(out[0], 5000);
    }
}
