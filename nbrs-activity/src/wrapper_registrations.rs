// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! SRD-32a — Built-in wrapper registrations.
//!
//! One `inventory::submit!` block per shipped wrapper. The
//! data declared here is the contract the resolver consumes:
//! which fields each wrapper owns, when it triggers, what it
//! requires inside it, what it forbids outside, what it can't
//! coexist with, and how it describes its assignment for
//! init-time logging.
//!
//! The wrapper *implementations* live in `wrappers.rs` and
//! `validation.rs`; the cascade in `activity.rs` reads the
//! resolved plan and dispatches to the appropriate
//! `XxxDispenser::wrap()` factory by name. New wrappers add
//! one entry here plus their factory at the call site.

use nbrs_workload::model::ParsedOp;

use crate::wrapper_registry::{WrapperName, WrapperRegistration};

// =====================================================
// Wrapper-name constants — single declaration site so the
// resolver, the cascade, and the constraint declarations
// all agree on spelling.
// =====================================================

pub const TRAVERSE: WrapperName = WrapperName::new("traverse");
pub const THROTTLE: WrapperName = WrapperName::new("throttle");
pub const VALIDATE: WrapperName = WrapperName::new("validate");
pub const POLL: WrapperName = WrapperName::new("poll");
pub const IF_COND: WrapperName = WrapperName::new("if");
pub const EMIT: WrapperName = WrapperName::new("emit");
pub const RESULT: WrapperName = WrapperName::new("result");
pub const METRICS: WrapperName = WrapperName::new("metrics");
pub const MEMO: WrapperName = WrapperName::new("memo");

// =====================================================
// Trigger predicates
// =====================================================

fn always(_: &ParsedOp) -> bool { true }

fn trigger_throttle(template: &ParsedOp) -> bool {
    template.delay.is_some()
}

fn trigger_validate(template: &ParsedOp) -> bool {
    template.params.contains_key("verify")
        || template.params.contains_key("relevancy")
}

fn trigger_poll(template: &ParsedOp) -> bool {
    // `poll:` may be either a bare string (mode-only, defaults
    // for everything else) or a map carrying the full config.
    // Either form turns the wrapper on.
    template
        .params
        .get("poll")
        .map(|v| v.is_string() || v.is_object())
        .unwrap_or(false)
}

fn trigger_if(template: &ParsedOp) -> bool {
    template.condition.is_some()
}

fn trigger_emit(template: &ParsedOp) -> bool {
    template
        .params
        .get("emit")
        .map(|v| {
            v.as_bool().unwrap_or_else(|| {
                v.as_str().map(|s| s == "true").unwrap_or(false)
            })
        })
        .unwrap_or(false)
}

fn trigger_metrics(template: &ParsedOp) -> bool {
    !template.metrics.is_empty()
}

fn trigger_memo(template: &ParsedOp) -> bool {
    // `memo:` is either a bare string (shorthand — same
    // template for before+after) or a map with `before:` and/or
    // `after:` keys. Either form turns the wrapper on. A null /
    // empty-map value does nothing.
    template
        .params
        .get("memo")
        .map(|v| v.is_string() || v.is_object())
        .unwrap_or(false)
}

// =====================================================
// describe_assignment — one-line summaries for the Info-
// level wrapper-stack log emitted at phase init.
//
// Returns `None` for trivial wrappers that have nothing
// to add beyond their name. The init logger then skips
// the line entirely.
// =====================================================

fn describe_traverse(_: &ParsedOp) -> Option<String> {
    None
}

fn describe_throttle(template: &ParsedOp) -> Option<String> {
    template.delay.as_ref().map(|name| {
        let trimmed = trim_braces(name);
        format!("throttle: delay binding `{trimmed}`")
    })
}

fn describe_validate(template: &ParsedOp) -> Option<String> {
    let strict = template
        .params
        .get("strict")
        .and_then(|v| v.as_bool().or_else(|| v.as_str().map(|s| s == "true")))
        .unwrap_or(false);
    let mut parts: Vec<String> = Vec::new();
    if let Some(v) = template.params.get("verify") {
        parts.push(format!("verify={}", short_value(v)));
    }
    if let Some(v) = template.params.get("relevancy") {
        parts.push(format!("relevancy={}", short_value(v)));
    }
    if parts.is_empty() {
        return None;
    }
    let body = parts.join(", ");
    Some(if strict {
        format!("validate: {body} (strict)")
    } else {
        format!("validate: {body}")
    })
}

fn describe_poll(template: &ParsedOp) -> Option<String> {
    let poll_val = template.params.get("poll")?;
    // Two shapes: bare string (mode only) or map (full config).
    let (mode, interval, timeout): (String, u64, u64) = match poll_val {
        v if v.is_string() => (
            v.as_str().unwrap().to_string(),
            1000,
            300_000,
        ),
        v if v.is_object() => {
            let m = v.as_object().unwrap();
            let mode = m
                .get("mode")
                .and_then(|x| x.as_str())
                .unwrap_or("await_empty")
                .to_string();
            let interval = m.get("interval_ms").and_then(json_to_u64).unwrap_or(1000);
            let timeout = m.get("timeout_ms").and_then(json_to_u64).unwrap_or(300_000);
            (mode, interval, timeout)
        }
        _ => return None,
    };
    Some(format!(
        "poll: every {}ms, timeout {}ms, on `{mode}`",
        interval, timeout
    ))
}

fn describe_if(template: &ParsedOp) -> Option<String> {
    template.condition.as_ref().map(|cond| {
        let trimmed = trim_braces(cond);
        format!("if: {trimmed}")
    })
}

fn describe_emit(_: &ParsedOp) -> Option<String> {
    Some("emit: rendered op text to stdout".into())
}

fn describe_result(template: &ParsedOp) -> Option<String> {
    let spec = template.result.as_ref()?;
    if spec.is_empty() {
        return None;
    }
    let mut names: Vec<String> = Vec::new();
    spec.walk_fragments(|frag| match frag {
        nbrs_workload::model::ResultFragment::Named { name, .. } => {
            names.push(name.to_string());
        }
        nbrs_workload::model::ResultFragment::Source(source) => {
            for line in source.lines() {
                let line = line.trim();
                if let Some((name, _)) = line.split_once(":=") {
                    names.push(name.trim().to_string());
                }
            }
        }
    });
    if names.is_empty() {
        return None;
    }
    names.sort();
    names.dedup();
    Some(format!("result: captures {}", names.join(", ")))
}

fn describe_metrics(template: &ParsedOp) -> Option<String> {
    if template.metrics.is_empty() {
        return None;
    }
    let mut names: Vec<&str> = template.metrics.keys().map(|s| s.as_str()).collect();
    names.sort();
    Some(format!("metrics: emits {}", names.join(", ")))
}

fn describe_memo(template: &ParsedOp) -> Option<String> {
    let v = template.params.get("memo")?;
    if let Some(s) = v.as_str() {
        if s.is_empty() { return None; }
        Some(format!("memo: \"{s}\" (before+after)"))
    } else if let Some(obj) = v.as_object() {
        let before = obj.get("before").and_then(|x| x.as_str());
        let after  = obj.get("after").and_then(|x| x.as_str());
        match (before, after) {
            (Some(b), Some(a)) => Some(format!("memo: before \"{b}\" / after \"{a}\"")),
            (Some(b), None)    => Some(format!("memo: before \"{b}\"")),
            (None, Some(a))    => Some(format!("memo: after \"{a}\"")),
            (None, None)       => None,
        }
    } else {
        None
    }
}

// =====================================================
// Helpers
// =====================================================

fn trim_braces(s: &str) -> &str {
    let t = s.trim();
    t.strip_prefix('{')
        .and_then(|s| s.strip_suffix('}'))
        .unwrap_or(t)
}

fn json_to_u64(v: &serde_json::Value) -> Option<u64> {
    v.as_u64()
        .or_else(|| v.as_str().and_then(|s| s.parse::<u64>().ok()))
}

/// Render a JSON value as a short, single-line string for
/// init-time diagnostics. Long objects/arrays are abbreviated.
fn short_value(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => format!("\"{s}\""),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Null => "null".into(),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            let s = v.to_string();
            if s.len() > 60 {
                format!("{}…", &s[..60])
            } else {
                s
            }
        }
    }
}

// =====================================================
// `metrics` forbids_outer list — every other registered
// wrapper. Encoded as a const slice (rather than computed
// at startup) because the set of built-in wrappers is
// fixed at compile time.
// =====================================================

const METRICS_FORBIDS_OUTER: &[WrapperName] = &[
    TRAVERSE, THROTTLE, VALIDATE, POLL, IF_COND, EMIT, RESULT,
];

// =====================================================
// inventory submissions — one per built-in wrapper.
// =====================================================

inventory::submit! {
    WrapperRegistration {
        name: TRAVERSE,
        owned_fields: &[],
        triggers: always,
        requires_inner: &[],
        forbids_outer: &[],
        mutually_exclusive_with: &[],
        describe_assignment: describe_traverse,
    }
}

inventory::submit! {
    WrapperRegistration {
        name: THROTTLE,
        // `delay` is the field carried at the top level of
        // ParsedOp; the legacy aliases `rate` / `rate_limiter`
        // are documented in the SRD but never landed as
        // first-class fields. Listing the actual storage key
        // keeps parse-time validation honest.
        owned_fields: &["delay"],
        triggers: trigger_throttle,
        requires_inner: &[TRAVERSE],
        forbids_outer: &[],
        mutually_exclusive_with: &[],
        describe_assignment: describe_throttle,
    }
}

inventory::submit! {
    WrapperRegistration {
        name: VALIDATE,
        owned_fields: &["verify", "relevancy", "strict"],
        triggers: trigger_validate,
        requires_inner: &[TRAVERSE],
        forbids_outer: &[],
        mutually_exclusive_with: &[],
        describe_assignment: describe_validate,
    }
}

inventory::submit! {
    WrapperRegistration {
        name: POLL,
        owned_fields: &[
            // `poll:` is the single discriminant for the poll
            // wrapper; every knob (interval_ms, timeout_ms,
            // min_rows, max_rows, json_path, metric_name,
            // max_error_retries) lives under it as a map. The
            // flat `poll_*`-prefix surface was retired.
            "poll",
        ],
        triggers: trigger_poll,
        requires_inner: &[TRAVERSE],
        forbids_outer: &[],
        mutually_exclusive_with: &[],
        describe_assignment: describe_poll,
    }
}

inventory::submit! {
    WrapperRegistration {
        name: IF_COND,
        owned_fields: &["if"],
        triggers: trigger_if,
        requires_inner: &[],
        forbids_outer: &[],
        mutually_exclusive_with: &[],
        describe_assignment: describe_if,
    }
}

inventory::submit! {
    WrapperRegistration {
        name: EMIT,
        owned_fields: &["emit"],
        triggers: trigger_emit,
        // SRD-32a's table lists `emit.requires_inner =
        // [result]`, but the cascade composes `result`
        // OUTSIDE `emit` (innermost-first list ends
        // `..., emit, result, metrics`). The SRD's
        // intention — "emit reflects post-result-capture
        // state" — is satisfied by `result` being always-
        // on rather than by an inner-position constraint.
        // Declaring `requires_inner = [result]` would
        // make `result` innermore than `emit`, which
        // contradicts the cascade and breaks the byte-
        // identical-output test bar in §"Migration".
        requires_inner: &[],
        forbids_outer: &[],
        mutually_exclusive_with: &[],
        describe_assignment: describe_emit,
    }
}

inventory::submit! {
    WrapperRegistration {
        name: RESULT,
        // `result:` is parsed into ParsedOp.result, not into
        // params, so this wrapper has no owned `params`-keys
        // to declare — the trigger always fires (the cascade
        // wraps unconditionally, no-op when result map is
        // empty). Listed in the SRD as "always (no-op when
        // empty)" for the same reason.
        owned_fields: &[],
        triggers: always,
        requires_inner: &[TRAVERSE],
        forbids_outer: &[],
        mutually_exclusive_with: &[],
        describe_assignment: describe_result,
    }
}

inventory::submit! {
    WrapperRegistration {
        name: METRICS,
        // `metrics:` is parsed into ParsedOp.metrics, not
        // into params, so there's no owned `params`-key.
        // Trigger fires only when the metrics map is non-
        // empty — matches the cascade's
        // `!template.metrics.is_empty()` guard. The SRD's
        // "always" note describes intent, but the wrapper
        // factory needs a non-empty map to do anything
        // useful.
        owned_fields: &[],
        triggers: trigger_metrics,
        requires_inner: &[],
        forbids_outer: METRICS_FORBIDS_OUTER,
        mutually_exclusive_with: &[],
        describe_assignment: describe_metrics,
    }
}

inventory::submit! {
    WrapperRegistration {
        name: MEMO,
        // `memo:` is the sole discriminant — string shorthand
        // or `{before, after}` map. No inner/outer constraints:
        // memo publication is independent of every other
        // wrapper's behaviour; it sees the same wires every
        // wrapper sees and writes to its own atomic.
        owned_fields: &["memo"],
        triggers: trigger_memo,
        requires_inner: &[TRAVERSE],
        forbids_outer: &[],
        mutually_exclusive_with: &[],
        describe_assignment: describe_memo,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wrapper_registry::WrapperRegistry;
    use crate::wrapper_resolver::{WrapperResolver, WrapperActivation};
    use nbrs_workload::model::ParsedOp;

    fn empty_template(name: &str) -> ParsedOp {
        ParsedOp::simple(name, "")
    }

    #[test]
    fn registry_collects_all_eight_wrappers() {
        let r = WrapperRegistry::from_inventory();
        let names: Vec<&str> = r.iter().map(|reg| reg.name.as_str()).collect();
        for expected in [
            "traverse", "throttle", "validate", "poll",
            "if", "emit", "result", "metrics",
        ] {
            assert!(
                names.contains(&expected),
                "registry missing `{expected}`; got {names:?}",
            );
        }
    }

    #[test]
    fn default_order_passes_constraint_validation() {
        let r = WrapperRegistry::from_inventory();
        let resolver = WrapperResolver::with_default_order(&r);
        assert!(resolver.is_ok(),
            "default order should validate: {:?}", resolver.err());
    }

    #[test]
    fn empty_template_resolves_to_traverse_and_result() {
        let r = WrapperRegistry::from_inventory();
        let resolver = WrapperResolver::with_default_order(&r).unwrap();
        let plan = resolver.resolve(&empty_template("noop"), &r).unwrap();
        let names: Vec<&str> = plan.stack.iter().map(|reg| reg.name.as_str()).collect();
        // traverse + result both unconditionally fire.
        assert_eq!(names, vec!["traverse", "result"]);
    }

    #[test]
    fn validate_pulls_in_traverse_transitively() {
        let r = WrapperRegistry::from_inventory();
        let resolver = WrapperResolver::with_default_order(&r).unwrap();
        let mut t = empty_template("v_op");
        t.params.insert("verify".into(),
            serde_json::Value::String("min_rows >= 1".into()));
        let plan = resolver.resolve(&t, &r).unwrap();

        // Traverse must be present and must precede validate.
        let names: Vec<&str> = plan.stack.iter().map(|reg| reg.name.as_str()).collect();
        let i_traverse = names.iter().position(|n| *n == "traverse").unwrap();
        let i_validate = names.iter().position(|n| *n == "validate").unwrap();
        assert!(i_traverse < i_validate,
            "traverse must be inside validate: {names:?}");

        // Activation provenance distinguishes triggered vs transitive.
        let validate_act = plan.activation(VALIDATE).unwrap();
        assert!(matches!(validate_act,
            WrapperActivation::OwnedField { field: "verify", .. }),
            "validate should be OwnedField(verify): {validate_act:?}");
    }

    #[test]
    fn override_must_include_every_triggered_wrapper() {
        let r = WrapperRegistry::from_inventory();
        let resolver = WrapperResolver::with_default_order(&r).unwrap();
        let mut t = empty_template("v_op");
        t.params.insert("verify".into(),
            serde_json::Value::String("min_rows >= 1".into()));
        // Override forgets `traverse` — even though traverse is
        // transitively activated by validate, the override must
        // list it.
        match resolver.resolve_with_order(&t, &r, &["validate", "result"]) {
            Err(crate::wrapper_resolver::ResolveError::OverridePermutationMismatch { missing: Some(_), .. }) => {}
            Err(other) => panic!("expected missing-wrapper error; got {other:?}"),
            Ok(_) => panic!("override missing a triggered wrapper must error"),
        }
    }

    #[test]
    fn override_must_not_include_non_triggered_wrappers() {
        let r = WrapperRegistry::from_inventory();
        let resolver = WrapperResolver::with_default_order(&r).unwrap();
        let t = empty_template("noop");
        // `poll` is not triggered (no `poll:` field). Listing it
        // is a hard error — silently dropping would mask a typo.
        match resolver.resolve_with_order(&t, &r, &["traverse", "poll", "result"]) {
            Err(crate::wrapper_resolver::ResolveError::OverridePermutationMismatch { extra: Some(_), .. }) => {}
            Err(other) => panic!("expected extra-wrapper error; got {other:?}"),
            Ok(_) => panic!("override naming a non-triggered wrapper must error"),
        }
    }

    #[test]
    fn override_with_unknown_name_suggests_typo() {
        let r = WrapperRegistry::from_inventory();
        let resolver = WrapperResolver::with_default_order(&r).unwrap();
        let t = empty_template("noop");
        match resolver.resolve_with_order(&t, &r,
            &["traverse", "validatte", "result"])
        {
            Err(crate::wrapper_resolver::ResolveError::UnknownWrapper { name, suggestion }) => {
                assert_eq!(name, "validatte");
                assert_eq!(suggestion, Some("validate"));
            }
            Err(other) => panic!("expected UnknownWrapper; got {other:?}"),
            Ok(_) => panic!("unknown name must error"),
        }
    }

    #[test]
    fn cli_default_order_replaces_built_in_tiebreaker() {
        // SRD-32a Push 3 — `--wrap-default-order` builds the
        // resolver from a custom innermost-to-outermost list.
        // Without any per-op or workload override, an op that
        // triggers two independently-ordered wrappers should
        // resolve to the CLI tiebreaker, not the built-in one.
        let r = WrapperRegistry::from_inventory();
        // Custom default puts validate AFTER throttle (built-in
        // already does that), and explicitly orders if BEFORE
        // poll — built-in has poll BEFORE if. This list flips
        // the if/poll relationship at the default-order level.
        let custom = vec![
            "traverse", "throttle", "validate", "if", "poll",
            "emit", "result", "metrics",
        ];
        let resolver = WrapperResolver::from_names(&custom, &r).unwrap();
        let mut t = empty_template("flexible");
        t.params.insert("poll".into(),
            serde_json::Value::String("await_empty".into()));
        t.condition = Some("flag".into());
        let plan = resolver.resolve(&t, &r).unwrap();
        let names: Vec<&str> = plan.stack.iter()
            .map(|reg| reg.name.as_str()).collect();
        let i_if = names.iter().position(|n| *n == "if").unwrap();
        let i_poll = names.iter().position(|n| *n == "poll").unwrap();
        assert!(i_if < i_poll,
            "custom default should place if INSIDE poll: {names:?}");
    }

    #[test]
    fn override_can_invert_default_tiebreaker_order() {
        // SRD-32a §"Workload-level override" — the override
        // is allowed to flip independently-triggered wrappers
        // (no requires_inner / forbids_outer between them).
        // The default has `validate, poll, if`; this test
        // confirms an override can reorder validate after
        // poll.
        let r = WrapperRegistry::from_inventory();
        let resolver = WrapperResolver::with_default_order(&r).unwrap();
        let mut t = empty_template("flexible");
        t.params.insert("verify".into(),
            serde_json::Value::String("ok".into()));
        t.params.insert("poll".into(),
            serde_json::Value::String("await_empty".into()));
        // Order: traverse, poll, validate, result. validate
        // OUTER than poll — both reasonable, no constraint
        // between them.
        let plan = resolver.resolve_with_order(&t, &r,
            &["traverse", "poll", "validate", "result"]).unwrap();
        let names: Vec<&str> = plan.stack.iter()
            .map(|reg| reg.name.as_str()).collect();
        assert_eq!(names, vec!["traverse", "poll", "validate", "result"]);
    }

    #[test]
    fn strict_without_verify_is_misplaced() {
        let r = WrapperRegistry::from_inventory();
        let mut t = empty_template("noop");
        t.params.insert("strict".into(), serde_json::Value::Bool(true));
        let violations = r.misplaced_fields(&t,
            |f| t.params.contains_key(f));
        let names: Vec<(&str, &str)> = violations.iter()
            .map(|(w, f)| (w.as_str(), *f))
            .collect();
        assert!(names.contains(&("validate", "strict")),
            "strict alone must be misplaced; got {names:?}");
    }

    #[test]
    fn poll_as_map_triggers_wrapper() {
        // The new contract: poll config is a nested map under
        // the single `poll:` key. Any flat `poll_*` prefix keys
        // were retired (so `poll_interval_ms` at op level is
        // just an unrecognized field, not a misplaced one).
        let r = WrapperRegistry::from_inventory();
        let mut t = empty_template("polled");
        let mut cfg = serde_json::Map::new();
        cfg.insert("mode".into(), serde_json::Value::String("await_empty".into()));
        cfg.insert("interval_ms".into(), serde_json::Value::Number(5000.into()));
        cfg.insert("timeout_ms".into(), serde_json::Value::Number(600_000.into()));
        t.params.insert("poll".into(), serde_json::Value::Object(cfg));
        // poll: <map> still triggers the wrapper.
        assert!(trigger_poll(&t));
        // And `poll:` is the wrapper's only owned field, so it's
        // never misplaced.
        let violations = r.misplaced_fields(&t,
            |f| t.params.contains_key(f));
        assert!(violations.is_empty(), "got {violations:?}");
    }

    #[test]
    fn full_workload_default_order_matches_cascade() {
        // Op declaring every trigger — the resolved order
        // must match the cascade's hand-rolled order:
        // traverse, throttle, validate, poll, if, emit,
        // result, metrics.
        let r = WrapperRegistry::from_inventory();
        let resolver = WrapperResolver::with_default_order(&r).unwrap();
        let mut t = empty_template("full");
        t.delay = Some("rate".into());
        t.params.insert("verify".into(),
            serde_json::Value::String("ok".into()));
        t.params.insert("poll".into(),
            serde_json::Value::String("await_empty".into()));
        t.condition = Some("flag".into());
        t.params.insert("emit".into(), serde_json::Value::Bool(true));
        t.metrics.insert("recall".into(),
            nbrs_workload::model::MetricSpec {
                value: "recall_value".into(),
                family: None,
                kind: None,
                unit: None,
                format: None,
            });
        let plan = resolver.resolve(&t, &r).unwrap();
        let names: Vec<&str> = plan.stack.iter().map(|reg| reg.name.as_str()).collect();
        assert_eq!(names, vec![
            "traverse", "throttle", "validate", "poll",
            "if", "emit", "result", "metrics",
        ]);
    }
}
