// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Datetime and epoch function nodes.

use crate::node::{CompiledU64Op, GkNode, NodeMeta, Port, PortType, Slot, Value};

/// Scale a u64 to epoch milliseconds by multiplying by a factor.
///
/// Signature: `(input: u64) -> (u64)`
/// Param: `factor: u64` — milliseconds per input unit.
///
/// Example: `EpochScale(1000)` treats input as seconds → millis.
pub struct EpochScale {
    meta: NodeMeta,
    factor: u64,
}

impl EpochScale {
    pub fn millis() -> Self { Self::new(1) }
    pub fn seconds() -> Self { Self::new(1_000) }
    pub fn minutes() -> Self { Self::new(60_000) }
    pub fn hours() -> Self { Self::new(3_600_000) }

    pub fn new(factor: u64) -> Self {
        Self {
            meta: NodeMeta {
                name: "epoch_scale".into(),
                outs: vec![Port::u64("output")],
                ins: vec![Slot::Wire(Port::u64("input"))],
            },
            factor,
        }
    }
}

impl GkNode for EpochScale {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::U64(inputs[0].as_u64().wrapping_mul(self.factor));
    }
    fn compiled_u64(&self) -> Option<CompiledU64Op> {
        let factor = self.factor;
        Some(Box::new(move |inputs, outputs| {
            outputs[0] = inputs[0].wrapping_mul(factor);
        }))
    }
}

/// Add a base epoch offset to a u64 value (milliseconds).
///
/// Signature: `(input: u64) -> (u64)`
/// Param: `base_epoch_ms: u64`
///
/// Convenience: combines with a reading counter to produce timestamps.
pub struct EpochOffset {
    meta: NodeMeta,
    base: u64,
}

impl EpochOffset {
    pub fn new(base_epoch_ms: u64) -> Self {
        Self {
            meta: NodeMeta {
                name: "epoch_offset".into(),
                outs: vec![Port::u64("output")],
                ins: vec![Slot::Wire(Port::u64("input"))],
            },
            base: base_epoch_ms,
        }
    }

    /// 2024-01-01T00:00:00Z in epoch millis.
    pub fn from_2024() -> Self { Self::new(1_704_067_200_000) }
    /// 2025-01-01T00:00:00Z in epoch millis.
    pub fn from_2025() -> Self { Self::new(1_735_689_600_000) }
}

impl GkNode for EpochOffset {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::U64(inputs[0].as_u64().wrapping_add(self.base));
    }
    fn compiled_u64(&self) -> Option<CompiledU64Op> {
        let base = self.base;
        Some(Box::new(move |inputs, outputs| {
            outputs[0] = inputs[0].wrapping_add(base);
        }))
    }
}

/// Format an epoch-millis u64 as an ISO-8601-like timestamp string.
///
/// Signature: `(input: u64) -> (String)`
///
/// Produces: `"YYYY-MM-DDThh:mm:ss.mmmZ"`
/// Uses a simple arithmetic calendar (no timezone, no leap second handling).
pub struct ToTimestamp {
    meta: NodeMeta,
}

impl Default for ToTimestamp {
    fn default() -> Self {
        Self::new()
    }
}

impl ToTimestamp {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "to_timestamp".into(),
                outs: vec![Port::new("output", PortType::Str)],
                ins: vec![Slot::Wire(Port::u64("input"))],
            },
        }
    }
}

impl GkNode for ToTimestamp {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        outputs[0] = Value::Str(epoch_ms_to_iso(inputs[0].as_u64()));
    }
}

/// Decompose epoch millis into date/time components.
///
/// Signature: `(input: u64) -> (year: u64, month: u64, day: u64, hour: u64, minute: u64, second: u64, millis: u64)`
pub struct DateComponents {
    meta: NodeMeta,
}

impl Default for DateComponents {
    fn default() -> Self {
        Self::new()
    }
}

impl DateComponents {
    pub fn new() -> Self {
        Self {
            meta: NodeMeta {
                name: "date_components".into(),
                outs: vec![
                    Port::u64("year"), Port::u64("month"), Port::u64("day"),
                    Port::u64("hour"), Port::u64("minute"), Port::u64("second"),
                    Port::u64("millis"),
                ],
                ins: vec![Slot::Wire(Port::u64("input"))],
            },
        }
    }
}

impl GkNode for DateComponents {
    fn meta(&self) -> &NodeMeta { &self.meta }
    fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
        let (y, mo, d, h, mi, s, ms) = decompose_epoch_ms(inputs[0].as_u64());
        outputs[0] = Value::U64(y);
        outputs[1] = Value::U64(mo);
        outputs[2] = Value::U64(d);
        outputs[3] = Value::U64(h);
        outputs[4] = Value::U64(mi);
        outputs[5] = Value::U64(s);
        outputs[6] = Value::U64(ms);
    }
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

use crate::dsl::registry::{Arity, FuncCategory, FuncSig, ParamSpec};
use crate::node::SlotType;

/// Signatures for datetime nodes.
pub fn signatures() -> &'static [FuncSig] {
    use FuncCategory as C;
    &[
        FuncSig {
            name: "epoch_scale", category: C::Datetime,
            outputs: 1, description: "scale u64 to epoch millis",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true, example: "cycle" },
                ParamSpec { name: "factor", slot_type: SlotType::ConstU64, required: true, example: "10" },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Scale a u64 to epoch milliseconds by multiplying by a factor.\nUse to convert a counter in coarse units to millisecond timestamps.\nParameters:\n  input  — u64 wire input (e.g., a cycle counter)\n  factor — milliseconds per input unit (u64)\nExample: epoch_scale(cycle, 1000)  // treat input as seconds -> millis",
        },
        FuncSig {
            name: "epoch_offset", category: C::Datetime,
            outputs: 1, description: "add base epoch offset",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true, example: "cycle" },
                ParamSpec { name: "base", slot_type: SlotType::ConstU64, required: true, example: "1000000" },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Add a base epoch offset (milliseconds) to a u64 value.\nShifts a relative millisecond value into absolute epoch time.\nParameters:\n  input — u64 wire input (relative millis)\n  base  — epoch milliseconds to add (e.g., 1704067200000 for 2024-01-01)\nExample: epoch_offset(epoch_scale(cycle, 1000), 1704067200000)",
        },
        FuncSig {
            name: "to_timestamp", category: C::Datetime,
            outputs: 1, description: "format epoch millis as ISO-8601",
            identity: None, variadic_ctor: None,
            params: &[
                ParamSpec { name: "input", slot_type: SlotType::Wire, required: true, example: "cycle" },
            ],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
            help: "Format an epoch-millis u64 as an ISO-8601 timestamp string.\nProduces: \"YYYY-MM-DDThh:mm:ss.mmmZ\" (UTC, no timezone conversion).\nParameters:\n  input — u64 epoch milliseconds\nExample: to_timestamp(epoch_offset(epoch_scale(cycle, 1000), 1704067200000))",
        },
        FuncSig {
            name: "date_components", category: C::Datetime, outputs: 7,
            description: "decompose epoch to year/month/day/hour/min/sec/ms",
            help: "Decompose epoch milliseconds into 7 output ports:\nyear, month (1-12), day (1-31), hour (0-23), minute (0-59),\nsecond (0-59), millisecond (0-999). All values are UTC.\nUse when you need individual date/time fields for structured output.\nParameters:\n  input — u64 epoch milliseconds",
            identity: None, variadic_ctor: None,
            params: &[ParamSpec { name: "input", slot_type: SlotType::Wire, required: true, example: "cycle" }],
            arity: Arity::Fixed,
            commutativity: crate::node::Commutativity::Positional,
        },
    ]
}

/// Try to build a datetime node from a function name and const args.
///
/// Returns `None` if the name is not handled by this module.
pub(crate) fn build_node(name: &str, _wires: &[crate::assembly::WireRef], consts: &[crate::dsl::factory::ConstArg]) -> Option<Result<Box<dyn crate::node::GkNode>, String>> {
    match name {
        "epoch_scale" => Some(Ok(Box::new(EpochScale::new(consts.first().map(|c| c.as_u64()).unwrap_or(1))))),
        "epoch_offset" => Some(Ok(Box::new(EpochOffset::new(consts.first().map(|c| c.as_u64()).unwrap_or(0))))),
        "to_timestamp" => Some(Ok(Box::new(ToTimestamp::new()))),
        "date_components" => Some(Ok(Box::new(DateComponents::new()))),
        _ => None,
    }
}


crate::register_nodes!(signatures, build_node);
#[cfg(test)]
mod tests {
    use super::*;

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
