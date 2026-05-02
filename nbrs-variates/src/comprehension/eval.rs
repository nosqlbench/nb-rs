// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Comprehension spec evaluation — text → typed value list.
//!
//! ## What this module does
//!
//! A comprehension clause `var in expr` ships its `expr` as
//! free-form workload-author text. At runtime, the executor
//! needs to turn that text into a list of typed values to
//! enumerate over. That's what [`evaluate_spec`] does, given a
//! GK kernel that holds the in-scope name space (own outputs +
//! inherited externs from `bind_outer_scope`).
//!
//! ## Pipeline
//!
//! ```text
//!   spec_text
//!       │
//!       ▼
//!   interpolate_via_kernel  ← {name} → kernel.lookup(name)
//!       │
//!       ▼
//!   eval_const_expr         ← optional: GK expression eval
//!       │
//!       ▼
//!   parse_list_with_types   ← comma-split, per-element type
//!       │
//!       ▼
//!   Vec<Value>              ← what the executor enumerates
//! ```
//!
//! ## Where this used to live
//!
//! Pre-Phase-C this code lived in `nbrs-activity::scope` and
//! `nbrs-activity::interpolate`. The lift was driven by
//! `docs/internals/50_comprehensions_first_class.md`: GK is the
//! canonical owner of what a comprehension *means*, including
//! how its spec strings resolve. Activity now consumes this
//! API rather than implementing it.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::kernel::GkKernel;
use crate::node::Value;

const ROUND_WARN: usize = 100;
const ROUND_HARD: usize = 1000;

/// Evaluate a comprehension clause's spec text against a kernel.
///
/// Steps:
///  1. [`interpolate_via_kernel`] resolves `{name}` placeholders
///     against the kernel's in-scope name space (own outputs +
///     inherited extern values).
///  2. Try `dsl::compile::eval_const_expr` on the result. On
///     success with a `Str` value, re-parse as a comma-separated
///     list with per-element type detection. Other typed
///     variants become a single-element typed list.
///  3. On eval failure (most common case for literal lists like
///     `"1, 10"` which aren't valid GK const expressions), fall
///     back to [`parse_list_with_types`] on the interpolated
///     text — `1` → `U64`, `1.5` → `F64`, `true` → `Bool`,
///     anything else → `Str`.
///
/// Errors propagate from interpolation (unresolved placeholder,
/// runaway round count, etc.) — those are the user-facing
/// actionable diagnostics.
pub fn evaluate_spec(
    spec_text: &str,
    kernel: &GkKernel,
) -> Result<Vec<Value>, String> {
    if let Some(values) = try_eval_all_cursor(spec_text, kernel)? {
        return Ok(values);
    }
    let interpolated = interpolate_via_kernel(spec_text, kernel)?;
    let value_str = match crate::dsl::compile::eval_const_expr(&interpolated) {
        Ok(Value::Str(s)) => s.to_string(),
        Ok(other) => return Ok(vec![other]),
        Err(_) => interpolated,
    };
    Ok(parse_list_with_types(&value_str))
}

/// Pre-evaluate a clause's spec text at synthesis time, using
/// `probes` for prior clauses' first values and `workload_params`
/// as a fallback source for names not yet promoted to workload-
/// kernel `init` bindings.
///
/// The runtime dispatcher uses [`evaluate_spec`] directly because
/// (a) the runtime kernel has prior-clause values as real input
/// slots, not text probes, and (b) by then workload params are
/// already injected as final bindings on the for_each scope's
/// kernel via the synthesis path.
pub fn pre_evaluate_clause(
    spec_text: &str,
    parent_kernel: &GkKernel,
    workload_params: &HashMap<String, String>,
    probes: &HashMap<String, String>,
) -> Result<Vec<Value>, String> {
    // The `all(<cursor>)` form resolves cursor extents from the
    // parent kernel's auxiliary outputs; it doesn't fit the
    // const-eval pipeline (which returns a single Value), so it's
    // intercepted here too — same as `evaluate_spec`.
    if let Some(values) = try_eval_all_cursor(spec_text, parent_kernel)? {
        return Ok(values);
    }
    let mut text = spec_text.to_string();
    for (var, probe_value) in probes {
        text = text.replace(&format!("{{{var}}}"), probe_value);
    }

    let interpolated = interpolate_with_lookup(
        &text,
        |name| {
            parent_kernel.get_constant(name)
                .or_else(|| parent_kernel.get_input(name))
                .filter(|v| !matches!(v, Value::None))
                .map(|v| v.to_display_string())
                .or_else(|| workload_params.get(name).cloned())
        },
    )?;

    let value_str = match crate::dsl::compile::eval_const_expr(&interpolated) {
        Ok(Value::Str(s)) => s.to_string(),
        Ok(other) => return Ok(vec![other]),
        Err(_) => interpolated,
    };
    Ok(parse_list_with_types(&value_str))
}

/// Parse a comma-separated text list, detecting each element's
/// native type. SRD-18b's "native types as the general rule":
/// `"1, 10"` → `[U64(1), U64(10)]`, `"1.5, 2.5"` → `[F64(...)]`,
/// mixed → each element gets its own native type.
pub fn parse_list_with_types(text: &str) -> Vec<Value> {
    text.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| {
            if let Ok(n) = s.parse::<u64>() {
                Value::U64(n)
            } else if let Ok(n) = s.parse::<f64>() {
                Value::F64(n)
            } else if s == "true" {
                Value::Bool(true)
            } else if s == "false" {
                Value::Bool(false)
            } else {
                Value::Str(s.to_string())
            }
        })
        .collect()
}

/// Recognize the comprehension-level `all(<cursor>)` clause form
/// and resolve it against the parent kernel's cursor extent
/// auxiliary outputs.
///
/// Cursors declared via the GK `cursor name = Cursor(start, end)`
/// shape compile to two well-known auxiliary outputs on the
/// kernel: `__cursor_extent_<name>_start` and
/// `__cursor_extent_<name>_end`. Reading those gives the cursor's
/// resolved extent at scope-init time. `all(<cursor>)` lowers to
/// the half-open ordinal range `[start, end)` as a `Vec<Value::U64>`.
///
/// Returns:
/// - `Ok(Some(values))` if `spec_text` matches the `all(<ident>)`
///   shape and the cursor's extent resolved successfully.
/// - `Ok(None)` if `spec_text` doesn't match — caller continues
///   with the normal interpolation + const-eval pipeline.
/// - `Err(...)` if the form matched but the cursor's extent
///   couldn't be resolved (cursor not in scope, extent wires
///   missing, etc.) — surfaced as a clause-level diagnostic.
fn try_eval_all_cursor(
    spec_text: &str,
    kernel: &GkKernel,
) -> Result<Option<Vec<Value>>, String> {
    let trimmed = spec_text.trim();
    let Some(stripped) = trimmed.strip_prefix("all(") else { return Ok(None); };
    let Some(arg) = stripped.strip_suffix(')') else { return Ok(None); };
    let cursor_name = arg.trim();
    if cursor_name.is_empty() || !is_valid_ident(cursor_name) {
        return Ok(None);
    }

    let start_key = format!("__cursor_extent_{cursor_name}_start");
    let end_key = format!("__cursor_extent_{cursor_name}_end");
    let start = kernel.lookup(&start_key)
        .and_then(|v| match v { Value::U64(n) => Some(n), _ => None })
        .ok_or_else(|| format!(
            "all({cursor_name}): cursor '{cursor_name}' has no resolvable extent — \
             check that the cursor is declared at or above this scope and that \
             its range arguments are init-resolvable. Looked for output '{start_key}'."
        ))?;
    let end = kernel.lookup(&end_key)
        .and_then(|v| match v { Value::U64(n) => Some(n), _ => None })
        .ok_or_else(|| format!(
            "all({cursor_name}): missing auxiliary output '{end_key}' on the parent kernel."
        ))?;

    if end < start {
        return Err(format!(
            "all({cursor_name}): cursor extent end={end} is less than start={start} — \
             cannot enumerate a negative-extent range."
        ));
    }
    Ok(Some((start..end).map(Value::U64).collect()))
}

fn is_valid_ident(s: &str) -> bool {
    let mut chars = s.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Map a `Value` variant to the GK extern type name string the
/// parser accepts (`u64`, `f64`, `bool`, `String`).
///
/// The GK parser today accepts only `u64`, `f64`, and
/// anything-else-as-Str for extern declarations, so Bytes /
/// Json / Ext / Handle / VecF32 / VecI32 / Str / None all map
/// to "String". The runtime spec evaluator surfaces the actual
/// typed value via interpolation regardless.
pub fn value_to_gk_type_name(v: &Value) -> &'static str {
    match v {
        Value::U64(_) => "u64",
        Value::F64(_) => "f64",
        Value::Bool(_) => "bool",
        _ => "String",
    }
}

/// Scan a GK source string for `{name}` placeholders nested
/// inside string-literal bodies (the GK string-interpolation
/// surface — see SRD-10 §"String Interpolation"). A body that
/// starts with a quote (CQL map / JSON literal) is treated as
/// literal content and skipped — same disambiguation rule the
/// binding compiler applies (see `string_lit_has_real_placeholder`
/// in `nbrs-variates/src/dsl/binding.rs`).
pub fn collect_string_interp_refs(src: &str, refs: &mut HashSet<String>) {
    let chars: Vec<char> = src.chars().collect();
    let mut i = 0;
    let mut in_str: Option<char> = None;
    while i < chars.len() {
        let c = chars[i];
        match in_str {
            Some(quote) if c == quote => { in_str = None; i += 1; }
            Some(_) if c == '\\' && i + 1 < chars.len() => { i += 2; }
            Some(_) if c == '{' => {
                let body_start = i + 1;
                let mut body_end = body_start;
                while body_end < chars.len() && chars[body_end] != '}' {
                    body_end += 1;
                }
                let body: String = chars[body_start..body_end].iter().collect();
                let trimmed = body.trim();
                if !trimmed.is_empty()
                    && !trimmed.starts_with('\'')
                    && !trimmed.starts_with('"')
                    && trimmed.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
                    && !trimmed.bytes().next().unwrap().is_ascii_digit()
                {
                    refs.insert(trimmed.to_string());
                }
                i = body_end + 1;
            }
            Some(_) => { i += 1; }
            None if c == '"' || c == '\'' => { in_str = Some(c); i += 1; }
            None => { i += 1; }
        }
    }
}

/// Enumerate the typed tuples a Cartesian comprehension produces.
///
/// Walks the dependent-tuple tree depth-first using fresh
/// per-branch kernels. Each branch installs the prior clauses'
/// typed values as inputs on a fresh kernel
/// ([`GkKernel::from_program`] + [`GkKernel::bind_outer_scope`]),
/// then evaluates the next clause's spec against that kernel.
/// This is the kernel-per-logical-subspace rule from SRD-18b
/// §"Dependent Tuple Iteration".
///
/// `filter`, when provided, is evaluated against each fully-bound
/// tuple — the predicate text is interpolated against a kernel
/// with all clause values installed, then `eval_const_expr` runs
/// it to a `Value::Bool`. Tuples where the predicate is `false`
/// are skipped. Predicate evaluation errors (non-Bool result,
/// unresolved name, etc.) abort enumeration. See
/// [`Comprehension::filter`](super::ast::Comprehension::filter).
///
/// Empty-clause handling is delegated to `on_empty_clause`: the
/// caller decides whether to propagate as a hard error (strict
/// mode) or warn-and-skip (relaxed mode). The callback receives
/// `(var, spec_text)` and returns `Result<(), String>` —
/// returning `Err` aborts enumeration, `Ok(())` skips the
/// branch.
pub fn enumerate_tuples<F>(
    canonical: &Arc<GkKernel>,
    parent: &Arc<GkKernel>,
    clauses: &[(String, String)],
    filter: Option<&str>,
    mut on_empty_clause: F,
) -> Result<Vec<Vec<(String, Value)>>, String>
where
    F: FnMut(&str, &str) -> Result<(), String>,
{
    let mut out = Vec::new();
    enumerate_into(
        canonical, parent, clauses, filter, 0, &Vec::new(), &mut out,
        &mut on_empty_clause,
    )?;
    Ok(out)
}

#[allow(clippy::too_many_arguments)]
fn enumerate_into<F>(
    canonical: &Arc<GkKernel>,
    parent: &Arc<GkKernel>,
    clauses: &[(String, String)],
    filter: Option<&str>,
    idx: usize,
    prefix: &[(String, Value)],
    out: &mut Vec<Vec<(String, Value)>>,
    on_empty_clause: &mut F,
) -> Result<(), String>
where
    F: FnMut(&str, &str) -> Result<(), String>,
{
    if idx == clauses.len() {
        // Apply the filter, if any, against a fresh kernel with
        // every tuple value installed. If the predicate evaluates
        // to false, skip this tuple; if true (or filter absent),
        // emit it.
        if let Some(predicate) = filter {
            let mut kernel = GkKernel::from_program(canonical.program().clone());
            kernel.bind_outer_scope(parent);
            for (var, value) in prefix {
                if let Some(slot) = kernel.program().find_input(var) {
                    kernel.state().set_input(slot, value.clone());
                }
            }
            let interpolated = interpolate_via_kernel(predicate, &kernel)
                .map_err(|e| format!("comprehension filter '{predicate}': {e}"))?;
            let result = crate::dsl::compile::eval_const_expr(&interpolated)
                .map_err(|e| format!("comprehension filter '{predicate}': {e}"))?;
            // GK comparison operators return U64 (0/1); accept
            // any truthy/falsy scalar uniformly, matching the
            // do-loop condition handler.
            let keep = match result {
                Value::Bool(b) => b,
                Value::U64(n) => n != 0,
                Value::F64(n) => n != 0.0,
                other => return Err(format!(
                    "comprehension filter '{predicate}': expected bool/u64/f64, got {other:?}"
                )),
            };
            if keep { out.push(prefix.to_vec()); }
        } else {
            out.push(prefix.to_vec());
        }
        return Ok(());
    }
    let mut kernel = GkKernel::from_program(canonical.program().clone());
    kernel.bind_outer_scope(parent);
    for (var, value) in prefix {
        if let Some(slot) = kernel.program().find_input(var) {
            kernel.state().set_input(slot, value.clone());
        }
    }

    let (var, spec_text) = &clauses[idx];
    let values = evaluate_spec(spec_text, &kernel)
        .map_err(|e| format!("for_each clause '{var} in {spec_text}': {e}"))?;

    if values.is_empty() {
        on_empty_clause(var, spec_text)?;
        return Ok(());
    }

    for value in values {
        let mut next_prefix = prefix.to_vec();
        next_prefix.push((var.clone(), value));
        enumerate_into(
            canonical, parent, clauses, filter, idx + 1, &next_prefix, out,
            on_empty_clause,
        )?;
    }
    Ok(())
}

/// Expand `{name}` placeholders in `text`, resolving each leaf
/// placeholder against `kernel`'s in-scope name space.
///
/// Lookup goes through `GkKernel::lookup`, which checks own
/// outputs (folded constants) first then extern input slots
/// populated by `bind_outer_scope` from a parent kernel, or by
/// the dependent-tuple dispatcher's per-clause `set_input`
/// writes. Inner shadows outer per SRD-16 §"Visibility Rules:
/// Shadowing".
///
/// `Value::None` (an unset extern slot) doesn't match — so a
/// slot that hasn't been populated falls through to the
/// unresolved-name error path at the fixed point.
///
/// Iterative + escape-aware + round-cap algorithm; see
/// [`interpolate_with_lookup`] for the engine.
pub fn interpolate_via_kernel(
    text: &str,
    kernel: &GkKernel,
) -> Result<String, String> {
    interpolate_with_lookup(text, |name| {
        kernel.lookup(name).map(|v| v.to_display_string())
    })
}

/// Iterative leaf-placeholder substitution with escape handling,
/// round cap, and final unresolved-name check. The `lookup`
/// closure decides where each leaf's value comes from.
///
/// Public so callers like the synthesis-time clause probe can
/// compose their own lookup (parent kernel + workload params +
/// clause probes) without reimplementing the iterative loop.
pub fn interpolate_with_lookup<F>(text: &str, lookup: F) -> Result<String, String>
where
    F: Fn(&str) -> Option<String>,
{
    let mut s = text.to_string();
    let mut warned = false;
    for round in 1..=ROUND_HARD {
        if round == ROUND_WARN && !warned {
            eprintln!(
                "interpolation: '{text}' has run {ROUND_WARN} substitution rounds — likely cyclic"
            );
            warned = true;
        }
        let progress = one_pass(&mut s, &lookup)?;
        if !progress { break; }
        if round == ROUND_HARD {
            return Err(format!(
                "interpolation: '{text}' did not stabilize in {ROUND_HARD} rounds — \
                 cyclic placeholders?"
            ));
        }
    }
    if let Some(unresolved) = first_unresolved(&s) {
        return Err(format!(
            "interpolation: unresolved placeholder '{{{unresolved}}}' in '{text}' — \
             not bound by any outer for_each var or workload param. \
             Use \\{{ \\}} to write literal braces."
        ));
    }
    Ok(unescape(&s))
}

/// One sweep over `s`: replaces every **leaf** placeholder
/// (`{NAME}` whose body contains no `{` or `}`) with its
/// resolved value via the supplied `lookup` closure. Returns
/// `Ok(true)` if any replacement happened, `Ok(false)` if the
/// string is at a fixed point. Nested placeholders
/// (`{a_{b}_c}`) are skipped this pass and revisited on the
/// next round once the inner is resolved.
fn one_pass<F>(s: &mut String, lookup: &F) -> Result<bool, String>
where
    F: Fn(&str) -> Option<String>,
{
    let bytes = s.as_bytes();
    let n = bytes.len();
    let mut out = String::with_capacity(n);
    let mut i = 0;
    let mut replaced_any = false;

    while i < n {
        let c = bytes[i];
        if c == b'\\' && i + 1 < n && (bytes[i + 1] == b'{' || bytes[i + 1] == b'}') {
            out.push('\\');
            out.push(bytes[i + 1] as char);
            i += 2;
            continue;
        }
        if c == b'{' {
            let mut j = i + 1;
            let mut has_inner_open = false;
            let mut end: Option<usize> = None;
            while j < n {
                let cj = bytes[j];
                if cj == b'\\' && j + 1 < n && (bytes[j + 1] == b'{' || bytes[j + 1] == b'}') {
                    j += 2;
                    continue;
                }
                if cj == b'{' { has_inner_open = true; break; }
                if cj == b'}' { end = Some(j); break; }
                j += 1;
            }
            if has_inner_open {
                out.push('{');
                i += 1;
                continue;
            }
            let Some(end_idx) = end else {
                return Err(format!(
                    "interpolation: unmatched '{{' in '{s}' starting at byte {i} — \
                     write \\{{ for a literal opening brace"
                ));
            };
            let name = std::str::from_utf8(&bytes[i + 1..end_idx])
                .map_err(|e| format!("interpolation: non-utf8 placeholder in '{s}': {e}"))?
                .to_string();
            if name.is_empty() {
                return Err(format!(
                    "interpolation: empty placeholder '{{}}' in '{s}' — \
                     write \\{{\\}} for literal braces"
                ));
            }
            let value = lookup(&name);
            let Some(value) = value else {
                out.push_str(&s[i..=end_idx]);
                i = end_idx + 1;
                continue;
            };
            out.push_str(&value);
            i = end_idx + 1;
            replaced_any = true;
            continue;
        }
        out.push(c as char);
        i += 1;
    }
    *s = out;
    Ok(replaced_any)
}

/// Locate the first unresolved leaf placeholder name (after
/// fixed-point iteration) for the diagnostic message. Returns
/// `None` if every `{...}` is escaped or already resolved.
fn first_unresolved(s: &str) -> Option<String> {
    let bytes = s.as_bytes();
    let n = bytes.len();
    let mut i = 0;
    while i < n {
        if bytes[i] == b'\\' && i + 1 < n && (bytes[i + 1] == b'{' || bytes[i + 1] == b'}') {
            i += 2;
            continue;
        }
        if bytes[i] == b'{' {
            let mut j = i + 1;
            while j < n {
                if bytes[j] == b'\\' && j + 1 < n
                    && (bytes[j + 1] == b'{' || bytes[j + 1] == b'}')
                {
                    j += 2;
                    continue;
                }
                if bytes[j] == b'}' {
                    return Some(s[i + 1..j].to_string());
                }
                if bytes[j] == b'{' { break; }
                j += 1;
            }
        }
        i += 1;
    }
    None
}

/// Strip `\{` → `{` and `\}` → `}`. Other escapes pass through
/// untouched so the substituted text doesn't gain newlines or
/// other surprises the user didn't ask for.
fn unescape(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = String::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'\\' && i + 1 < bytes.len()
            && (bytes[i + 1] == b'{' || bytes[i + 1] == b'}')
        {
            out.push(bytes[i + 1] as char);
            i += 2;
            continue;
        }
        out.push(bytes[i] as char);
        i += 1;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn h(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    fn interpolate(
        text: &str,
        bindings: &HashMap<String, String>,
        workload_params: &HashMap<String, String>,
    ) -> Result<String, String> {
        interpolate_with_lookup(text, |name| {
            bindings.get(name).or_else(|| workload_params.get(name)).cloned()
        })
    }

    #[test]
    fn flat_substitution() {
        let params = h(&[("dataset", "example"), ("prefix", "label")]);
        let out = interpolate("matching('{dataset}', '{prefix}')", &h(&[]), &params).unwrap();
        assert_eq!(out, "matching('example', 'label')");
    }

    #[test]
    fn bindings_shadow_params() {
        let params = h(&[("profile", "default")]);
        let bindings = h(&[("profile", "label_07")]);
        let out = interpolate("vec_{profile}", &bindings, &params).unwrap();
        assert_eq!(out, "vec_label_07");
    }

    #[test]
    fn nested_placeholder_resolves_inside_out() {
        let params = h(&[
            ("k_1_limits", "1,2,4,8"),
            ("k_10_limits", "10,20,30"),
        ]);
        let bindings = h(&[("k", "1")]);
        let out = interpolate("{k_{k}_limits}", &bindings, &params).unwrap();
        assert_eq!(out, "1,2,4,8");
    }

    #[test]
    fn deeply_nested() {
        let params = h(&[("a_b_c", "WIN")]);
        let bindings = h(&[("x", "a"), ("y", "b"), ("z", "c")]);
        let out = interpolate("{{x}_{y}_{z}}", &bindings, &params).unwrap();
        assert_eq!(out, "WIN");
    }

    #[test]
    fn escape_emits_literal_brace() {
        let out = interpolate("\\{not_a_var\\}", &h(&[]), &h(&[])).unwrap();
        assert_eq!(out, "{not_a_var}");
    }

    #[test]
    fn escape_inside_otherwise_resolved_text() {
        let params = h(&[("x", "1")]);
        let out = interpolate("a={x} literal=\\{x\\}", &h(&[]), &params).unwrap();
        assert_eq!(out, "a=1 literal={x}");
    }

    #[test]
    fn unresolved_is_hard_error() {
        let err = interpolate("hello {nope}", &h(&[]), &h(&[])).unwrap_err();
        assert!(err.contains("unresolved"));
        assert!(err.contains("nope"));
    }

    #[test]
    fn empty_placeholder_rejected() {
        let err = interpolate("a{}b", &h(&[]), &h(&[])).unwrap_err();
        assert!(err.contains("empty"));
    }

    #[test]
    fn unmatched_brace_rejected() {
        let err = interpolate("a {x", &h(&[]), &h(&[])).unwrap_err();
        assert!(err.contains("unmatched"));
    }

    #[test]
    fn idempotent_when_no_placeholders() {
        let out = interpolate("plain text", &h(&[]), &h(&[])).unwrap();
        assert_eq!(out, "plain text");
    }

    #[test]
    fn resolved_value_with_braces_does_not_re_expand() {
        let params = h(&[("greeting", "hello {planet}")]);
        let err = interpolate("{greeting}", &h(&[]), &params).unwrap_err();
        assert!(err.contains("planet"));
    }

    #[test]
    fn cyclic_placeholders_hit_round_cap() {
        let params = h(&[("a", "{b}"), ("b", "{a}")]);
        let err = interpolate("{a}", &h(&[]), &params).unwrap_err();
        assert!(err.contains("did not stabilize") || err.contains("rounds"));
    }

    #[test]
    fn kernel_resolves_via_get_constant() {
        let kernel = crate::dsl::compile::compile_gk(
            "final dataset := \"example\"\n"
        ).unwrap();
        let out = interpolate_via_kernel("path/{dataset}/data", &kernel).unwrap();
        assert_eq!(out, "path/example/data");
    }

    #[test]
    fn kernel_resolves_via_get_input() {
        let parent = crate::dsl::compile::compile_gk(
            "final k_values := \"1, 10\"\n"
        ).unwrap();
        let mut child = crate::dsl::compile::compile_gk(
            "extern k_values: String\n"
        ).unwrap();
        child.bind_outer_scope(&parent);
        let out = interpolate_via_kernel("values={k_values}", &child).unwrap();
        assert_eq!(out, "values=1, 10");
    }

    #[test]
    fn kernel_unresolved_name_errors() {
        let kernel = crate::dsl::compile::compile_gk(
            "final x := 1\n"
        ).unwrap();
        let err = interpolate_via_kernel("hello {nope}", &kernel).unwrap_err();
        assert!(err.contains("unresolved"));
        assert!(err.contains("nope"));
    }

    #[test]
    fn kernel_nested_template_iterates_to_fixed_point() {
        let kernel = crate::dsl::compile::compile_gk(
            "final k := \"1\"\nfinal k_1_limits := \"1, 2, 4, 8\"\n"
        ).unwrap();
        let out = interpolate_via_kernel("{k_{k}_limits}", &kernel).unwrap();
        assert_eq!(out, "1, 2, 4, 8");
    }

    #[test]
    fn parse_list_native_types() {
        let v = parse_list_with_types("1, 10, 100");
        assert_eq!(v, vec![Value::U64(1), Value::U64(10), Value::U64(100)]);
    }

    #[test]
    fn parse_list_mixed_types() {
        let v = parse_list_with_types("1, 1.5, true, hello");
        assert_eq!(v, vec![
            Value::U64(1),
            Value::F64(1.5),
            Value::Bool(true),
            Value::Str("hello".to_string()),
        ]);
    }

    #[test]
    fn all_cursor_returns_extent_range() {
        // Simulate a cursor declaration at the parent scope by
        // exposing the auxiliary extent outputs as folded
        // constants. The real cursor compiler emits these via
        // `__cursor_extent_<name>_{start,end}` outputs; for this
        // test we synthesize them directly.
        let kernel = crate::dsl::compile::compile_gk(
            "final __cursor_extent_row_start := 0\n\
             final __cursor_extent_row_end := 5\n"
        ).unwrap();
        let values = evaluate_spec("all(row)", &kernel).unwrap();
        assert_eq!(values, vec![
            Value::U64(0), Value::U64(1), Value::U64(2), Value::U64(3), Value::U64(4),
        ]);
    }

    #[test]
    fn all_cursor_non_zero_start() {
        let kernel = crate::dsl::compile::compile_gk(
            "final __cursor_extent_data_start := 100\n\
             final __cursor_extent_data_end := 103\n"
        ).unwrap();
        let values = evaluate_spec("all(data)", &kernel).unwrap();
        assert_eq!(values, vec![Value::U64(100), Value::U64(101), Value::U64(102)]);
    }

    #[test]
    fn all_cursor_missing_extent_errors() {
        let kernel = crate::dsl::compile::compile_gk(
            "final unrelated := 1\n"
        ).unwrap();
        let err = evaluate_spec("all(no_such_cursor)", &kernel).unwrap_err();
        assert!(err.contains("all(no_such_cursor)"));
        assert!(err.contains("no resolvable extent"));
    }

    #[test]
    fn all_cursor_only_matches_exact_shape() {
        // `all(<ident>)` is the only matched shape; anything more
        // complex falls through to the normal eval path (which
        // for now treats unrecognized text as a comma-list of
        // string values).
        let kernel = crate::dsl::compile::compile_gk(
            "final __cursor_extent_row_start := 0\n\
             final __cursor_extent_row_end := 5\n"
        ).unwrap();
        // `all(row, 5)` doesn't match the strict shape — comma
        // breaks the bare-ident requirement. Fall-through path
        // splits on commas, producing two string fragments.
        let result = evaluate_spec("all(row, 5)", &kernel).unwrap();
        // Not the cursor's range — the form didn't match.
        assert_ne!(result, vec![
            Value::U64(0), Value::U64(1), Value::U64(2), Value::U64(3), Value::U64(4),
        ]);
    }

    #[test]
    fn literal_cursor_exposes_extent_auxiliaries() {
        // Real cursor declaration with literal extent — verifies
        // the compiler-side change that emits
        // __cursor_extent_<name>_{start,end} as final bindings
        // even in the literal-args case.
        let kernel = crate::dsl::compile::compile_gk(
            "cursor row = range(0, 50)\n"
        ).unwrap();
        let start = kernel.lookup("__cursor_extent_row_start");
        let end = kernel.lookup("__cursor_extent_row_end");
        assert_eq!(start, Some(Value::U64(0)),
            "expected start=0, got {start:?}");
        assert_eq!(end, Some(Value::U64(50)),
            "expected end=50, got {end:?}");
    }

    #[test]
    fn all_cursor_with_real_cursor_decl_works() {
        let kernel = crate::dsl::compile::compile_gk(
            "cursor row = range(0, 5)\n"
        ).unwrap();
        let values = evaluate_spec("all(row)", &kernel).unwrap();
        assert_eq!(values, vec![
            Value::U64(0), Value::U64(1), Value::U64(2), Value::U64(3), Value::U64(4),
        ]);
    }

    #[test]
    fn all_cursor_ignores_whitespace() {
        let kernel = crate::dsl::compile::compile_gk(
            "final __cursor_extent_row_start := 0\n\
             final __cursor_extent_row_end := 3\n"
        ).unwrap();
        let values = evaluate_spec("  all( row )  ", &kernel).unwrap();
        assert_eq!(values.len(), 3);
    }

    #[test]
    fn evaluate_spec_resolves_against_kernel() {
        let kernel = crate::dsl::compile::compile_gk(
            "final k_values := \"1, 10, 100\"\n"
        ).unwrap();
        let v = evaluate_spec("{k_values}", &kernel).unwrap();
        assert_eq!(v, vec![Value::U64(1), Value::U64(10), Value::U64(100)]);
    }
}
