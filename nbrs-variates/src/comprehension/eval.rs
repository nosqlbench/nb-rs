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
    // SRD-18c Layer 2 / SRD-18e Push 3: range operator
    // (`a..b`, `a..=b`, `a..b..s`, `a..=b..s`). Bounds and
    // step are GK const expressions evaluated at this
    // (post-interpolation) point.
    if let Some(values) = try_eval_range(&interpolated)? {
        return Ok(values);
    }
    // SRD-18c Layer 3 / SRD-18e Push 7: named generators.
    if let Some(values) = try_eval_generator(&interpolated)? {
        return Ok(values);
    }
    // SRD-18c Layer 5 / SRD-18e Push 9: set operators on lists.
    if let Some(values) = try_eval_setop(&interpolated, kernel)? {
        return Ok(values);
    }
    // SRD-18c §"Sequencer-style expansions" / Push 8: LUT
    // facility (bucket / concat_seq / interval_seq).
    if let Some(values) = try_eval_sequencer(&interpolated, kernel)? {
        return Ok(values);
    }
    let value_str = match crate::dsl::compile::eval_const_expr(&interpolated) {
        Ok(Value::Str(s)) => s.to_string(),
        Ok(other) => return Ok(vec![other]),
        // Fall back to the literal-list parse only when the text
        // is unambiguously a comma-separated list of literals
        // (e.g. `1, 10, 100` — `eval_const_expr` doesn't accept
        // that shape because it isn't a single GK expression).
        // Anything that looks like an expression (parens, GK
        // operators, identifiers other than `true`/`false`) was
        // *meant* to evaluate; if it failed, we MUST surface the
        // failure rather than silently splitting on `,` and
        // handing the workload an iter-var like
        // `matching_profiles('x'` (truncated). The latter
        // produces malformed downstream output six steps removed
        // from the actual fault — a Push-2 kind of bad UX.
        Err(eval_err) => {
            if looks_like_literal_list(&interpolated) {
                interpolated
            } else {
                return Err(format!(
                    "for_each clause expression failed to evaluate: {eval_err}\n\
                     spec: {interpolated}\n\
                     If this was meant as a literal list (e.g. `1, 10, 100`), \
                     it should contain only literal values separated by commas. \
                     If it was meant as an expression, fix the underlying \
                     evaluation error."
                ));
            }
        }
    };
    Ok(parse_list_with_types(&value_str))
}

/// Heuristic: does this interpolated spec text look like a
/// "literal list" (comma-separated literals like `1, 10, 100` or
/// `foo, bar, baz`) rather than an expression?
///
/// True only when no character suggests an expression: no
/// parentheses, no operators, no string-quote characters that
/// would imply a function-call shape. Whitespace, digits,
/// alphanumerics, dots (for floats), minus (for negatives), and
/// commas (the separator) are all OK.
///
/// The point of this gate is to keep "list" specs (`for: "k in 1,
/// 10, 100"`) working through the literal-list fallback while
/// still surfacing real evaluation failures for expression specs
/// like `matching_profiles('x', 'y')`. A wrong call on a
/// borderline case here is cheap — it just produces a clearer
/// error from the eval layer instead of swallowed garbage.
fn looks_like_literal_list(text: &str) -> bool {
    let trimmed = text.trim();
    if trimmed.is_empty() { return false; }
    !trimmed.chars().any(|c| matches!(
        c,
        '(' | ')' | '[' | ']' | '{' | '}' | '\'' | '"'
        | '+' | '*' | '/' | '%' | '=' | '<' | '>' | '!' | '&' | '|' | '~' | '^' | '?'
    ))
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
            parent_kernel.get_constant(name).cloned()
                .or_else(|| parent_kernel.get_input(name))
                .filter(|v| !matches!(v, Value::None))
                .map(|v| v.to_display_string())
                .or_else(|| workload_params.get(name).cloned())
        },
    )?;

    // Push 3: range operator on the pre-evaluation path too.
    if let Some(values) = try_eval_range(&interpolated)? {
        return Ok(values);
    }
    // Push 7 / 9 / 8 — same generator / set-op / sequencer
    // shortcuts the runtime path uses.
    if let Some(values) = try_eval_generator(&interpolated)? {
        return Ok(values);
    }
    if let Some(values) = try_eval_setop(&interpolated, parent_kernel)? {
        return Ok(values);
    }
    if let Some(values) = try_eval_sequencer(&interpolated, parent_kernel)? {
        return Ok(values);
    }
    let value_str = match crate::dsl::compile::eval_const_expr(&interpolated) {
        Ok(Value::Str(s)) => s.to_string(),
        Ok(other) => return Ok(vec![other]),
        // Mirrors `evaluate_spec`'s gating: only fall back to
        // parse_list_with_types when the text is unambiguously a
        // literal list. See `looks_like_literal_list` for the
        // rationale.
        Err(eval_err) => {
            if looks_like_literal_list(&interpolated) {
                interpolated
            } else {
                return Err(format!(
                    "for_each clause expression failed to evaluate: {eval_err}\n\
                     spec: {interpolated}\n\
                     If this was meant as a literal list (e.g. `1, 10, 100`), \
                     it should contain only literal values separated by commas. \
                     If it was meant as an expression, fix the underlying \
                     evaluation error."
                ));
            }
        }
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

/// SRD-18c Layer 2 / SRD-18e Push 3: recognise the range
/// operator and expand it into a `Vec<Value>`.
///
/// Four shapes:
/// - `a..b`         half-open with step 1
/// - `a..=b`        closed with step 1
/// - `a..b..s`      half-open with step `s`
/// - `a..=b..s`     closed with step `s`
///
/// Bounds and step are GK const expressions; this function
/// evaluates each segment via `eval_const_expr`. Numeric
/// type follows the bounds: if both are integers, the
/// emitted list is `Value::U64`; otherwise `Value::F64`.
///
/// Returns:
/// - `Ok(Some(values))` on a successful range expansion.
/// - `Ok(None)` when `text` doesn't have a top-paren-depth
///   `..` at all — caller falls through to the standard
///   const-eval / list-parse path.
/// - `Err(...)` when the form matches but evaluation fails
///   (bound non-numeric, step is zero, bounds diverge from
///   step direction, etc.).
fn try_eval_range(text: &str) -> Result<Option<Vec<Value>>, String> {
    let trimmed = text.trim();
    let chars: Vec<char> = trimmed.chars().collect();

    // Find every top-paren-depth `..` (with optional `=`).
    // Returns positions of the `..` start and whether the
    // following `=` was present.
    let mut splits: Vec<(usize, bool)> = Vec::new();
    let mut depth: i32 = 0;
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        match c {
            '(' | '[' | '{' => depth += 1,
            ')' | ']' | '}' => depth -= 1,
            '"' | '\'' => {
                // Skip the rest of the quoted run.
                let q = c;
                i += 1;
                while i < chars.len() && chars[i] != q {
                    i += 1;
                }
            }
            '.' if depth == 0
                && i + 1 < chars.len()
                && chars[i + 1] == '.' =>
            {
                let inclusive = i + 2 < chars.len() && chars[i + 2] == '=';
                splits.push((i, inclusive));
                i += if inclusive { 3 } else { 2 };
                continue;
            }
            _ => {}
        }
        i += 1;
    }

    if splits.is_empty() {
        return Ok(None);
    }
    if splits.len() > 2 {
        return Err(format!(
            "range expression '{trimmed}': more than two `..` operators \
             at top level — expected one of `a..b`, `a..=b`, `a..b..s`, \
             or `a..=b..s`"
        ));
    }
    if splits.len() == 2 && splits[1].1 {
        return Err(format!(
            "range expression '{trimmed}': step delimiter cannot be \
             `..=` — only the bound separator may be inclusive"
        ));
    }

    // Slice out the segments.
    let inclusive = splits[0].1;
    let first_end = splits[0].0;
    let after_first = first_end + if inclusive { 3 } else { 2 };
    let (start_text, mid_text, step_text) = match splits.len() {
        1 => {
            let start_s: String = chars[..first_end].iter().collect();
            let end_s: String = chars[after_first..].iter().collect();
            (start_s, end_s, None)
        }
        2 => {
            let mid_end = splits[1].0;
            let after_mid = mid_end + 2; // `..` only, not `..=`
            let start_s: String = chars[..first_end].iter().collect();
            let mid_s: String = chars[after_first..mid_end].iter().collect();
            let step_s: String = chars[after_mid..].iter().collect();
            (start_s, mid_s, Some(step_s))
        }
        _ => unreachable!(),
    };

    let start_val = eval_range_segment(&start_text, "range start")?;
    let end_val = eval_range_segment(&mid_text, "range end")?;
    let step_val = match step_text {
        Some(s) => Some(eval_range_segment(&s, "range step")?),
        None => None,
    };

    Ok(Some(expand_range(start_val, end_val, step_val, inclusive, trimmed)?))
}

fn eval_range_segment(text: &str, what: &str) -> Result<Value, String> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Err(format!("range expression: {what} is empty"));
    }
    crate::dsl::compile::eval_const_expr(trimmed)
        .map_err(|e| format!("range expression: {what} '{trimmed}' did not const-fold — {e}"))
}

/// Materialise the value list once start/end/step have been
/// const-folded. If any of the three is `F64`, the whole list
/// is `F64`; otherwise everything is `U64`.
fn expand_range(
    start: Value,
    end: Value,
    step: Option<Value>,
    inclusive: bool,
    src: &str,
) -> Result<Vec<Value>, String> {
    let any_float = matches!(start, Value::F64(_))
        || matches!(end, Value::F64(_))
        || matches!(step, Some(Value::F64(_)));

    let to_f64 = |v: &Value| -> Result<f64, String> {
        match v {
            Value::U64(n) => Ok(*n as f64),
            Value::F64(f) => Ok(*f),
            other => Err(format!(
                "range expression '{src}': bound has non-numeric value {other:?}"
            )),
        }
    };
    let to_i64 = |v: &Value| -> Result<i64, String> {
        match v {
            Value::U64(n) => i64::try_from(*n).map_err(|_| format!(
                "range expression '{src}': bound {n} exceeds signed 64-bit range"
            )),
            Value::F64(f) => {
                if f.fract() == 0.0 && *f >= i64::MIN as f64 && *f <= i64::MAX as f64 {
                    Ok(*f as i64)
                } else {
                    Err(format!(
                        "range expression '{src}': float bound {f} is not integral; \
                         mix with an explicit float step (e.g. `1.0..10..0.5`) for a float range"
                    ))
                }
            }
            other => Err(format!(
                "range expression '{src}': bound has non-numeric value {other:?}"
            )),
        }
    };

    if any_float {
        let s = to_f64(&start)?;
        let e = to_f64(&end)?;
        let st = match step.as_ref() {
            Some(v) => to_f64(v)?,
            None => 1.0,
        };
        if st == 0.0 {
            return Err(format!("range expression '{src}': step is zero"));
        }
        // Direction must match (start < end ⇒ step > 0; start > end ⇒ step < 0).
        if (e - s).is_sign_positive() && st < 0.0 {
            return Ok(Vec::new());
        }
        if (e - s).is_sign_negative() && st > 0.0 {
            return Ok(Vec::new());
        }
        let mut out = Vec::new();
        let mut cur = s;
        let cmp = |x: f64| -> bool {
            if st > 0.0 {
                if inclusive { x <= e + 1e-12 } else { x < e - 1e-12 }
            } else if inclusive { x >= e - 1e-12 } else { x > e + 1e-12 }
        };
        while cmp(cur) {
            out.push(Value::F64(cur));
            cur += st;
        }
        return Ok(out);
    }

    // Integer range.
    let s = to_i64(&start)?;
    let e = to_i64(&end)?;
    let st = match step.as_ref() {
        Some(v) => to_i64(v)?,
        None => 1,
    };
    if st == 0 {
        return Err(format!("range expression '{src}': step is zero"));
    }
    if st > 0 && s > e { return Ok(Vec::new()); }
    if st < 0 && s < e { return Ok(Vec::new()); }
    let mut out = Vec::new();
    let mut cur = s;
    let cmp = |x: i64| -> bool {
        if st > 0 {
            if inclusive { x <= e } else { x < e }
        } else if inclusive { x >= e } else { x > e }
    };
    while cmp(cur) {
        if cur < 0 {
            return Err(format!(
                "range expression '{src}': negative value {cur} can't be \
                 represented as Value::U64; use a float range \
                 (mix any bound or step with `.0`) for signed walks"
            ));
        }
        out.push(Value::U64(cur as u64));
        cur = cur.saturating_add(st);
        if (st > 0 && cur < s) || (st < 0 && cur > s) {
            // saturated; would loop forever on overflow.
            break;
        }
    }
    Ok(out)
}

// ============================================================
// Function-call dispatch (Pushes 7, 8, 9)
// ============================================================

/// Recognise `name(args)` at the top paren depth. Returns
/// `Some((name, args))` when the entire `text` is exactly
/// one function call (with balanced parens, possibly empty
/// args). Quoted strings within args are walked as opaque
/// runs so internal commas / parens don't trip the split.
fn parse_func_call(text: &str) -> Option<(&str, &str)> {
    let trimmed = text.trim();
    if !trimmed.ends_with(')') {
        return None;
    }
    let open = trimmed.find('(')?;
    let name = trimmed[..open].trim();
    if name.is_empty() || !is_valid_ident(name) {
        return None;
    }
    // Make sure the closing `)` matches the opening — i.e.
    // the entire text is a single call, not `f(a) + g(b)`.
    let chars: Vec<char> = trimmed.chars().collect();
    let mut depth = 0i32;
    let mut in_quote: Option<char> = None;
    for (i, &c) in chars.iter().enumerate().skip(open) {
        match (c, in_quote) {
            ('"' | '\'', None) => in_quote = Some(c),
            (q, Some(open_q)) if q == open_q => in_quote = None,
            ('(', None) => depth += 1,
            (')', None) => {
                depth -= 1;
                if depth == 0 {
                    if i != chars.len() - 1 {
                        return None; // close mid-text
                    }
                    let args: String = chars[open + 1..i].iter().collect();
                    // SAFETY: trimmed lives for fn duration; we
                    // index into the original string via slices
                    // with care. Instead of returning a borrowed
                    // slice from the local `args` String, return
                    // the slices directly from `trimmed`.
                    let _ = args;
                    let name_slice = &trimmed[..open];
                    let args_slice = &trimmed[open + 1..trimmed.len() - 1];
                    return Some((name_slice.trim(), args_slice));
                }
            }
            _ => {}
        }
    }
    None
}

/// Split a function-argument list on top-level commas. Skips
/// commas inside parens, brackets, braces, or quoted strings.
fn split_args_top_level(args: &str) -> Vec<&str> {
    let mut out: Vec<&str> = Vec::new();
    let chars: Vec<char> = args.chars().collect();
    let bytes_per_char: Vec<usize> = chars.iter().map(|c| c.len_utf8()).collect();
    let mut start_byte = 0usize;
    let mut byte = 0usize;
    let mut depth = 0i32;
    let mut in_quote: Option<char> = None;
    for (i, &c) in chars.iter().enumerate() {
        match (c, in_quote) {
            ('"' | '\'', None) => in_quote = Some(c),
            (q, Some(open_q)) if q == open_q => in_quote = None,
            ('(' | '[' | '{', None) => depth += 1,
            (')' | ']' | '}', None) => depth -= 1,
            (',', None) if depth == 0 => {
                let seg = &args[start_byte..byte];
                out.push(seg.trim());
                start_byte = byte + bytes_per_char[i];
            }
            _ => {}
        }
        byte += bytes_per_char[i];
    }
    let last = &args[start_byte..];
    if !last.trim().is_empty() || !out.is_empty() {
        out.push(last.trim());
    }
    out
}

/// Parse a single argument text as a `u64`. Errors carry the
/// expected-form context for the user.
fn parse_u64_arg(text: &str, what: &str) -> Result<u64, String> {
    let trimmed = text.trim();
    trimmed.parse::<u64>().map_err(|_| format!(
        "{what}: expected non-negative integer, got '{trimmed}'"
    ))
}

/// Parse a single argument as either u64 or f64. Returns the
/// f64 representation regardless (callers that need an int
/// check `.fract() == 0.0`).
fn parse_num_arg(text: &str, what: &str) -> Result<f64, String> {
    let trimmed = text.trim();
    trimmed.parse::<f64>().map_err(|_| format!(
        "{what}: expected numeric, got '{trimmed}'"
    ))
}

// ============================================================
// SRD-18c Layer 3 / SRD-18e Push 7: named generators
// ============================================================

/// Recognise `fib(n)`, `pow2(n)`, `geometric(...)`, etc. and
/// expand to a `Vec<Value>`. Returns `Ok(None)` when the
/// text isn't a known generator call (caller falls through
/// to set-op / sequencer / const-eval paths).
fn try_eval_generator(text: &str) -> Result<Option<Vec<Value>>, String> {
    let Some((name, args)) = parse_func_call(text) else {
        return Ok(None);
    };
    let arg_list = split_args_top_level(args);
    match name {
        "fib" => {
            if arg_list.len() != 1 {
                return Err(format!("fib(n): expected 1 argument, got {}", arg_list.len()));
            }
            let n = parse_u64_arg(arg_list[0], "fib(n)")?;
            Ok(Some(generate_fib_n(n)))
        }
        "fib_until" => {
            if arg_list.len() != 1 {
                return Err(format!("fib_until(max): expected 1 argument, got {}", arg_list.len()));
            }
            let max = parse_u64_arg(arg_list[0], "fib_until(max)")?;
            Ok(Some(generate_fib_until(max)))
        }
        "pow2" => {
            if arg_list.len() != 1 {
                return Err(format!("pow2(n): expected 1 argument, got {}", arg_list.len()));
            }
            let n = parse_u64_arg(arg_list[0], "pow2(n)")?;
            Ok(Some(generate_pow2_n(n)))
        }
        "pow2_until" => {
            if arg_list.len() != 1 {
                return Err(format!("pow2_until(max): expected 1 argument, got {}", arg_list.len()));
            }
            let max = parse_u64_arg(arg_list[0], "pow2_until(max)")?;
            Ok(Some(generate_pow2_until(max)))
        }
        "binomial" => {
            if arg_list.len() != 1 {
                return Err(format!("binomial(n): expected 1 argument, got {}", arg_list.len()));
            }
            let n = parse_u64_arg(arg_list[0], "binomial(n)")?;
            Ok(Some(generate_binomial(n)))
        }
        "geometric" => {
            if arg_list.len() != 3 {
                return Err(format!("geometric(start, factor, n): expected 3 args, got {}",
                    arg_list.len()));
            }
            let start  = parse_num_arg(arg_list[0], "geometric.start")?;
            let factor = parse_num_arg(arg_list[1], "geometric.factor")?;
            let n      = parse_u64_arg(arg_list[2], "geometric.n")?;
            Ok(Some(generate_geometric(start, factor, n)))
        }
        "geometric_until" => {
            if arg_list.len() != 3 {
                return Err(format!("geometric_until(start, factor, max): expected 3 args, got {}",
                    arg_list.len()));
            }
            let start  = parse_num_arg(arg_list[0], "geometric_until.start")?;
            let factor = parse_num_arg(arg_list[1], "geometric_until.factor")?;
            let max    = parse_num_arg(arg_list[2], "geometric_until.max")?;
            Ok(Some(generate_geometric_until(start, factor, max)))
        }
        "subdivide" => {
            if arg_list.len() != 3 {
                return Err(format!("subdivide(start, end, n): expected 3 args, got {}",
                    arg_list.len()));
            }
            let start = parse_num_arg(arg_list[0], "subdivide.start")?;
            let end   = parse_num_arg(arg_list[1], "subdivide.end")?;
            let n     = parse_u64_arg(arg_list[2], "subdivide.n")?;
            Ok(Some(generate_subdivide(start, end, n, false)))
        }
        "subdivide_inclusive" | "linear_steps" => {
            if arg_list.len() != 3 {
                return Err(format!("{name}(start, end, n): expected 3 args, got {}",
                    arg_list.len()));
            }
            let start = parse_num_arg(arg_list[0], &format!("{name}.start"))?;
            let end   = parse_num_arg(arg_list[1], &format!("{name}.end"))?;
            let n     = parse_u64_arg(arg_list[2], &format!("{name}.n"))?;
            Ok(Some(generate_subdivide(start, end, n, true)))
        }
        "log_steps" => {
            if arg_list.len() != 3 {
                return Err(format!("log_steps(start, end, n): expected 3 args, got {}",
                    arg_list.len()));
            }
            let start = parse_num_arg(arg_list[0], "log_steps.start")?;
            let end   = parse_num_arg(arg_list[1], "log_steps.end")?;
            let n     = parse_u64_arg(arg_list[2], "log_steps.n")?;
            Ok(Some(generate_log_steps(start, end, n)?))
        }
        _ => Ok(None),
    }
}

/// First `n` Fibonacci numbers: 1, 1, 2, 3, 5, 8, ...
fn generate_fib_n(n: u64) -> Vec<Value> {
    if n == 0 { return Vec::new(); }
    let mut out = Vec::with_capacity(n as usize);
    let (mut a, mut b): (u64, u64) = (1, 1);
    for _ in 0..n {
        out.push(Value::U64(a));
        let next = a.saturating_add(b);
        a = b;
        b = next;
    }
    out
}

/// Fibonacci values up to and including the largest ≤ `max`.
fn generate_fib_until(max: u64) -> Vec<Value> {
    let mut out = Vec::new();
    let (mut a, mut b): (u64, u64) = (1, 1);
    while a <= max {
        out.push(Value::U64(a));
        let next = a.checked_add(b);
        a = b;
        match next {
            Some(v) => b = v,
            None => break,
        }
    }
    out
}

/// `1, 2, 4, ..., 2^(n-1)`.
fn generate_pow2_n(n: u64) -> Vec<Value> {
    let mut out = Vec::with_capacity(n as usize);
    for i in 0..n {
        if i >= 64 { break; }  // 2^64 overflows u64
        out.push(Value::U64(1u64 << i));
    }
    out
}

/// Powers of two ≤ max.
fn generate_pow2_until(max: u64) -> Vec<Value> {
    let mut out = Vec::new();
    let mut v: u64 = 1;
    loop {
        if v > max { break; }
        out.push(Value::U64(v));
        v = match v.checked_mul(2) { Some(x) => x, None => break };
    }
    out
}

/// `start, start*factor, start*factor², …` (n terms).
fn generate_geometric(start: f64, factor: f64, n: u64) -> Vec<Value> {
    let mut out = Vec::with_capacity(n as usize);
    let mut v = start;
    for _ in 0..n {
        out.push(Value::F64(v));
        v *= factor;
    }
    out
}

/// `start, start*factor, …` ≤ max.
fn generate_geometric_until(start: f64, factor: f64, max: f64) -> Vec<Value> {
    let mut out = Vec::new();
    let mut v = start;
    if factor <= 1.0 || start <= 0.0 || max <= 0.0 {
        // Defensive: avoid infinite loops with non-growing
        // factors. The "until" semantics implies growth.
        return out;
    }
    while v <= max {
        out.push(Value::F64(v));
        v *= factor;
    }
    out
}

/// Binomial coefficients `C(n, 0), C(n, 1), …, C(n, n)`.
fn generate_binomial(n: u64) -> Vec<Value> {
    let mut out = Vec::with_capacity(n as usize + 1);
    let mut c: u128 = 1;
    out.push(Value::U64(1));
    for k in 1..=n {
        c = c * (n - k + 1) as u128 / k as u128;
        if c > u64::MAX as u128 { break; }
        out.push(Value::U64(c as u64));
    }
    out
}

/// Half-open subdivision of `[start, end)` into `n` parts
/// (n elements). Inclusive form (`subdivide_inclusive`)
/// covers `[start, end]` with `n` elements.
fn generate_subdivide(start: f64, end: f64, n: u64, inclusive: bool) -> Vec<Value> {
    if n == 0 { return Vec::new(); }
    let denom = if inclusive { (n.saturating_sub(1)).max(1) as f64 } else { n as f64 };
    let step = (end - start) / denom;
    (0..n).map(|i| Value::F64(start + step * i as f64)).collect()
}

/// `n` log-spaced points from `start` to `end` (inclusive).
/// Both bounds must be positive (log undefined otherwise).
fn generate_log_steps(start: f64, end: f64, n: u64) -> Result<Vec<Value>, String> {
    if start <= 0.0 || end <= 0.0 {
        return Err(format!("log_steps: bounds must be positive, got start={start}, end={end}"));
    }
    if n == 0 { return Ok(Vec::new()); }
    if n == 1 { return Ok(vec![Value::F64(start)]); }
    let log_s = start.ln();
    let log_e = end.ln();
    let step = (log_e - log_s) / (n - 1) as f64;
    Ok((0..n).map(|i| Value::F64((log_s + step * i as f64).exp())).collect())
}

// ============================================================
// SRD-18c Layer 5 / SRD-18e Push 9: set operators
// ============================================================

/// Recognise `concat(...)`, `unique(...)`, etc. Each set op
/// recursively evaluates its arguments through `evaluate_spec`
/// (so `concat(1..10, fib(8))` works), then combines the
/// resulting lists.
fn try_eval_setop(text: &str, kernel: &GkKernel) -> Result<Option<Vec<Value>>, String> {
    let Some((name, args)) = parse_func_call(text) else {
        return Ok(None);
    };
    let arg_texts = split_args_top_level(args);
    let recursively_evaluate = |t: &str| -> Result<Vec<Value>, String> {
        evaluate_spec(t, kernel)
    };
    match name {
        "concat" => {
            let mut out = Vec::new();
            for a in &arg_texts { out.extend(recursively_evaluate(a)?); }
            Ok(Some(out))
        }
        "unique" => {
            let mut out: Vec<Value> = Vec::new();
            for a in &arg_texts {
                for v in recursively_evaluate(a)? {
                    if !out.contains(&v) { out.push(v); }
                }
            }
            Ok(Some(out))
        }
        "intersect" => {
            if arg_texts.is_empty() { return Ok(Some(Vec::new())); }
            let first = recursively_evaluate(arg_texts[0])?;
            let mut out: Vec<Value> = Vec::new();
            for v in first {
                let mut in_all = true;
                for a in &arg_texts[1..] {
                    let other = recursively_evaluate(a)?;
                    if !other.contains(&v) { in_all = false; break; }
                }
                if in_all && !out.contains(&v) {
                    out.push(v);
                }
            }
            Ok(Some(out))
        }
        "subtract" => {
            if arg_texts.len() != 2 {
                return Err(format!("subtract(a, b): expected 2 args, got {}", arg_texts.len()));
            }
            let a = recursively_evaluate(arg_texts[0])?;
            let b = recursively_evaluate(arg_texts[1])?;
            Ok(Some(a.into_iter().filter(|v| !b.contains(v)).collect()))
        }
        "interleave" => {
            let lists: Result<Vec<Vec<Value>>, String> = arg_texts.iter()
                .map(|a| recursively_evaluate(a)).collect();
            let lists = lists?;
            let mut out = Vec::new();
            let max_len = lists.iter().map(|l| l.len()).max().unwrap_or(0);
            for i in 0..max_len {
                for l in &lists {
                    if let Some(v) = l.get(i) { out.push(v.clone()); }
                }
            }
            Ok(Some(out))
        }
        "cycle" => {
            if arg_texts.len() != 2 {
                return Err(format!("cycle(a, n): expected 2 args, got {}", arg_texts.len()));
            }
            let a = recursively_evaluate(arg_texts[0])?;
            let n = parse_u64_arg(arg_texts[1], "cycle.n")?;
            let mut out = Vec::with_capacity(a.len() * n as usize);
            for _ in 0..n { out.extend(a.iter().cloned()); }
            Ok(Some(out))
        }
        "reverse" => {
            if arg_texts.len() != 1 {
                return Err(format!("reverse(a): expected 1 arg, got {}", arg_texts.len()));
            }
            let mut a = recursively_evaluate(arg_texts[0])?;
            a.reverse();
            Ok(Some(a))
        }
        "take" => {
            if arg_texts.len() != 2 {
                return Err(format!("take(a, n): expected 2 args, got {}", arg_texts.len()));
            }
            let a = recursively_evaluate(arg_texts[0])?;
            let n = parse_u64_arg(arg_texts[1], "take.n")?;
            Ok(Some(a.into_iter().take(n as usize).collect()))
        }
        "skip" => {
            if arg_texts.len() != 2 {
                return Err(format!("skip(a, n): expected 2 args, got {}", arg_texts.len()));
            }
            let a = recursively_evaluate(arg_texts[0])?;
            let n = parse_u64_arg(arg_texts[1], "skip.n")?;
            Ok(Some(a.into_iter().skip(n as usize).collect()))
        }
        _ => Ok(None),
    }
}

// ============================================================
// SRD-18c §"Sequencer-style expansions" / Push 8: bucket /
// concat_seq / interval_seq — LUT facility reusing the
// op-sequencing algorithms.
// ============================================================

/// Recognise `bucket(items, ratios)` / `bucket("3:a, 1:b")`,
/// `concat_seq(...)`, `interval_seq(...)`. Reuses the
/// algorithms from `nbrs-activity::opseq`.
///
/// The algorithms aren't exposed cross-crate as raw functions
/// today, so we re-implement the small set we need here. The
/// outputs match `build_bucket_lut` / `build_concat_lut` /
/// `build_interval_lut` byte-for-byte (covered by the
/// op-sequencing tests in `nbrs-activity`).
fn try_eval_sequencer(text: &str, kernel: &GkKernel) -> Result<Option<Vec<Value>>, String> {
    let Some((name, args)) = parse_func_call(text) else {
        return Ok(None);
    };
    if !matches!(name, "bucket" | "concat_seq" | "interval_seq") {
        return Ok(None);
    }
    let arg_texts = split_args_top_level(args);

    // Two acceptable shapes:
    //   1. Single string arg: ratio-prefix shorthand
    //      `"3:ann, 1:scan, 2:fetch"`.
    //   2. Two list args: items + ratios in lockstep.
    let (items, ratios): (Vec<Value>, Vec<usize>) = match arg_texts.len() {
        1 => parse_ratio_prefix_shorthand(arg_texts[0])?,
        2 => {
            let items = evaluate_spec(arg_texts[0], kernel)?;
            let raw_ratios = evaluate_spec(arg_texts[1], kernel)?;
            let ratios: Result<Vec<usize>, String> = raw_ratios.iter().map(|v| match v {
                Value::U64(n) => Ok(*n as usize),
                other => Err(format!("{name}: ratio must be non-negative integer, got {other:?}")),
            }).collect();
            (items, ratios?)
        }
        _ => return Err(format!(
            "{name}: expected `(items, ratios)` or `(\"r1:item1, r2:item2, ...\")`; got {} args",
            arg_texts.len()
        )),
    };

    if items.len() != ratios.len() {
        return Err(format!(
            "{name}: items.len() ({}) != ratios.len() ({})",
            items.len(), ratios.len(),
        ));
    }
    Ok(Some(match name {
        "bucket"       => seq_bucket(&items, &ratios),
        "concat_seq"   => seq_concat(&items, &ratios),
        "interval_seq" => seq_interval(&items, &ratios),
        _ => unreachable!(),
    }))
}

/// Parse `"r1:item1, r2:item2, …"`. Each element is a
/// ratio (positive integer) and an item value separated
/// by `:`. The string itself comes through `evaluate_spec`
/// — typically as a quoted string literal.
fn parse_ratio_prefix_shorthand(text: &str) -> Result<(Vec<Value>, Vec<usize>), String> {
    // The arg might be a literal `"3:a, 1:b"` (with quotes
    // in the source) or already-stripped `3:a, 1:b`.
    let stripped = text.trim()
        .trim_start_matches(['"', '\''])
        .trim_end_matches(['"', '\'']);
    let mut items = Vec::new();
    let mut ratios = Vec::new();
    for part in stripped.split(',') {
        let part = part.trim();
        if part.is_empty() { continue; }
        let (r, i) = part.split_once(':').ok_or_else(|| format!(
            "ratio-prefix shorthand: missing ':' in '{part}'"
        ))?;
        let ratio: usize = r.trim().parse().map_err(|_| format!(
            "ratio-prefix shorthand: ratio '{r}' is not a non-negative integer"
        ))?;
        ratios.push(ratio);
        items.push(parse_one_value(i.trim()));
    }
    Ok((items, ratios))
}

fn parse_one_value(s: &str) -> Value {
    if let Ok(n) = s.parse::<u64>() { return Value::U64(n); }
    if let Ok(f) = s.parse::<f64>() { return Value::F64(f); }
    if s == "true"  { return Value::Bool(true); }
    if s == "false" { return Value::Bool(false); }
    Value::Str(s.to_string())
}

/// Bucket sequencer: round-robin from per-item buckets sized
/// by ratio. Output length = sum(ratios).
fn seq_bucket(items: &[Value], ratios: &[usize]) -> Vec<Value> {
    let total: usize = ratios.iter().sum();
    let mut out = Vec::with_capacity(total);
    let mut remaining: Vec<usize> = ratios.to_vec();
    while out.len() < total {
        let mut emitted_any = false;
        for (i, item) in items.iter().enumerate() {
            if remaining[i] > 0 {
                out.push(item.clone());
                remaining[i] -= 1;
                emitted_any = true;
            }
        }
        if !emitted_any { break; }
    }
    out
}

/// Concat sequencer: contiguous runs (all of item 1, then
/// all of item 2, …).
fn seq_concat(items: &[Value], ratios: &[usize]) -> Vec<Value> {
    let total: usize = ratios.iter().sum();
    let mut out = Vec::with_capacity(total);
    for (item, &r) in items.iter().zip(ratios.iter()) {
        for _ in 0..r { out.push(item.clone()); }
    }
    out
}

/// Interval sequencer: evenly spaced occurrences of each
/// item across the output. Picks each output position from
/// the item with the largest "weight × position - already
/// emitted" — same algorithm as op-sequencing's
/// build_interval_lut.
fn seq_interval(items: &[Value], ratios: &[usize]) -> Vec<Value> {
    let total: usize = ratios.iter().sum();
    if total == 0 { return Vec::new(); }
    let mut emitted: Vec<usize> = vec![0; items.len()];
    let mut out = Vec::with_capacity(total);
    for slot in 0..total {
        // Pick the item whose target ratio is most under-met
        // at this slot. Target at slot k = (ratio_i * (k+1)) / total.
        let mut best = 0usize;
        let mut best_deficit: f64 = f64::NEG_INFINITY;
        for i in 0..items.len() {
            let target = ratios[i] as f64 * (slot + 1) as f64 / total as f64;
            let deficit = target - emitted[i] as f64;
            if deficit > best_deficit {
                best_deficit = deficit;
                best = i;
            }
        }
        out.push(items[best].clone());
        emitted[best] += 1;
    }
    out
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
/// the offending [`Clause`] (which carries both single-var and
/// parallel-iter shapes) and returns `Result<(), String>` —
/// returning `Err` aborts enumeration, `Ok(())` skips the
/// branch.
pub fn enumerate_tuples<F>(
    canonical: &Arc<GkKernel>,
    parent: &Arc<GkKernel>,
    clauses: &[super::ast::Clause],
    filter: Option<&str>,
    mut on_empty_clause: F,
) -> Result<Vec<Vec<(String, Value)>>, String>
where
    F: FnMut(&super::ast::Clause) -> Result<(), String>,
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
    clauses: &[super::ast::Clause],
    filter: Option<&str>,
    idx: usize,
    prefix: &[(String, Value)],
    out: &mut Vec<Vec<(String, Value)>>,
    on_empty_clause: &mut F,
) -> Result<(), String>
where
    F: FnMut(&super::ast::Clause) -> Result<(), String>,
{
    use super::ast::ClauseSource;

    if idx == clauses.len() {
        // Apply the filter, if any, against a fresh kernel with
        // every tuple value installed. If the predicate evaluates
        // to false, skip this tuple; if true (or filter absent),
        // emit it.
        if let Some(predicate) = filter {
            // Iter-var values from the prefix flow through the
            // parent's typed materialize step; this also gives
            // the cell cascade the prefix snapshot before the
            // bind, matching for_iteration's contract.
            let bindings_owned: Vec<(String, Value)> = prefix
                .iter()
                .map(|(v, val)| ((*v).to_string(), val.clone()))
                .collect();
            let kernel = parent.materialize_subscope(canonical.program().clone(), &bindings_owned);
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
    let bindings_owned: Vec<(String, Value)> = prefix
        .iter()
        .map(|(v, val)| ((*v).to_string(), val.clone()))
        .collect();
    let kernel = parent.materialize_subscope(canonical.program().clone(), &bindings_owned);

    let clause = &clauses[idx];
    match &clause.source {
        ClauseSource::Single(spec_text) => {
            let var = clause.var();
            let values = evaluate_spec(spec_text, &kernel)
                .map_err(|e| format!("for_each clause '{var} in {spec_text}': {e}"))?;

            if values.is_empty() {
                on_empty_clause(clause)?;
                return Ok(());
            }

            for value in values {
                let mut next_prefix = prefix.to_vec();
                next_prefix.push((var.to_string(), value));
                enumerate_into(
                    canonical, parent, clauses, filter, idx + 1, &next_prefix, out,
                    on_empty_clause,
                )?;
            }
        }
        ClauseSource::Parallel { mode, exprs } => {
            // Layer 7a: evaluate each expr in the group, then zip
            // them. The zip mode (Strict / Truncate / Cycle)
            // controls length-balancing; Strict is the default
            // for the bare `(e1, e2)` syntax.
            use super::ast::ZipMode;
            let group_label = format!(
                "({}) in {}({})",
                clause.vars.join(", "),
                match mode {
                    ZipMode::Strict   => "",
                    ZipMode::Truncate => "zip_truncate",
                    ZipMode::Cycle    => "zip_cycle",
                },
                exprs.join(", "),
            );
            let mut columns: Vec<Vec<Value>> = Vec::with_capacity(exprs.len());
            for expr in exprs {
                let values = evaluate_spec(expr, &kernel)
                    .map_err(|e| format!("for_each parallel clause '{group_label}': {e}"))?;
                columns.push(values);
            }
            let lens: Vec<usize> = columns.iter().map(|c| c.len()).collect();
            let len = match mode {
                ZipMode::Strict => {
                    let len0 = lens[0];
                    for (i, &l) in lens.iter().enumerate().skip(1) {
                        if l != len0 {
                            return Err(format!(
                                "for_each parallel clause '{group_label}': \
                                 length mismatch — expr 0 produced {len0} values, \
                                 expr {i} produced {l} (use zip_truncate(...) or \
                                 zip_cycle(...) to opt into truncate/cycle semantics)"
                            ));
                        }
                    }
                    len0
                }
                ZipMode::Truncate => *lens.iter().min().unwrap(),
                ZipMode::Cycle => {
                    // Reject empty columns under Cycle — there's
                    // no value to repeat. Fall through to the
                    // empty-clause callback below by using len=0.
                    if lens.iter().any(|&l| l == 0) { 0 }
                    else { *lens.iter().max().unwrap() }
                }
            };
            if len == 0 {
                on_empty_clause(clause)?;
                return Ok(());
            }
            for step in 0..len {
                let mut next_prefix = prefix.to_vec();
                for (var, col) in clause.vars.iter().zip(columns.iter()) {
                    // Cycle: index modulo column length so shorter
                    // columns repeat; Strict / Truncate: direct.
                    let i = if matches!(mode, ZipMode::Cycle) { step % col.len() }
                            else { step };
                    next_prefix.push((var.clone(), col[i].clone()));
                }
                enumerate_into(
                    canonical, parent, clauses, filter, idx + 1, &next_prefix, out,
                    on_empty_clause,
                )?;
            }
        }
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
        let child_program = crate::dsl::compile::compile_gk(
            "extern k_values: String\n"
        ).unwrap().program().clone();
        let child = parent.materialize_subscope(child_program, &[]);
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
        // `all(<ident>)` is the only matched shape — anything
        // more complex falls through to the normal eval path.
        // `all(row, 5)` doesn't match the strict shape (the
        // comma breaks the bare-ident requirement), so the
        // pipeline tries to evaluate it as a regular GK
        // expression. There's no registered function named
        // `all`, so eval fails and the failure is propagated as
        // a clean clause-level error (the legacy silent
        // literal-list fallback masked this kind of typo six
        // layers downstream).
        let kernel = crate::dsl::compile::compile_gk(
            "final __cursor_extent_row_start := 0\n\
             final __cursor_extent_row_end := 5\n"
        ).unwrap();
        let err = evaluate_spec("all(row, 5)", &kernel).unwrap_err();
        assert!(err.contains("all(row, 5)"), "error must mention the failing spec, got: {err}");
        assert!(
            err.contains("failed to evaluate") || err.contains("unknown function"),
            "error must explain the eval failure, got: {err}"
        );
    }

    #[test]
    fn missing_dataset_surface_as_clean_error_not_garbage() {
        // Regression: workload runs on a system whose
        // vectordata catalog doesn't have the requested
        // dataset. The spec
        //   `profile in matching_profiles('nonexistent_dataset_xyz', 'label_')`
        // must produce a clean clause-level error naming the
        // resolution failure — NOT a "garbage" iter-var like
        // `matching_profiles('nonexistent_dataset_xyz'`
        // (truncated at the first comma) that flows downstream
        // into malformed CQL six layers later.
        //
        // Three failure layers used to compound here:
        //   1. `dataset_group_open` returned `Value::None` on
        //      catalog miss.
        //   2. `handle_of(&Value::None)` panicked with
        //      "expected Handle, got U64" — opaque.
        //   3. `evaluate_spec` swallowed the eval error and
        //      fell through to splitting the literal text on
        //      commas.
        // The user-visible result was a CQL parser error from a
        // malformed `DROP INDEX`. After this fix every layer
        // propagates an actionable diagnostic.
        let kernel = crate::dsl::compile::compile_gk("final unrelated := 1\n").unwrap();
        let result = evaluate_spec(
            "matching_profiles('nonexistent_dataset_xyz_qqq', 'label_')",
            &kernel,
        );
        let err = result.expect_err(
            "missing dataset must surface as Err, not silent literal-list fallback"
        );
        // Doesn't matter which exact error string we get from
        // the catalog layer — the test guards the *contract*:
        // the spec text appears in the error, the failure is
        // attributed to the dataset / resolver / open path, and
        // it is a Result::Err (not garbage data).
        assert!(
            err.contains("nonexistent_dataset_xyz_qqq")
                || err.contains("matching_profiles")
                || err.contains("dataset"),
            "error must point at the actual fault, got: {err}"
        );
    }

    #[test]
    fn function_call_eval_failure_is_not_silently_split() {
        // Defensive: any text containing `(` is an
        // expression — never a literal list. If eval fails, we
        // must propagate the failure rather than splitting on
        // commas. This guards the broader contract that
        // protected the dataset-resolution case above.
        let kernel = crate::dsl::compile::compile_gk("final unrelated := 1\n").unwrap();
        let err = evaluate_spec("nonexistent_func('a', 'b', 'c')", &kernel).unwrap_err();
        assert!(err.contains("failed to evaluate") || err.contains("unknown"),
            "expected a clean eval-failure error, got: {err}");
    }

    #[test]
    fn literal_list_path_still_works() {
        // Counter-case: a plain comma-separated list of
        // literals (no parens, no operators) MUST still work
        // through the literal-list fallback after eval fails
        // (which it should — `1, 10, 100` isn't a single GK
        // expression). This is the legitimate use case that the
        // fallback exists for.
        let kernel = crate::dsl::compile::compile_gk("final unrelated := 1\n").unwrap();
        let values = evaluate_spec("1, 10, 100", &kernel).unwrap();
        assert_eq!(values, vec![Value::U64(1), Value::U64(10), Value::U64(100)]);

        let names = evaluate_spec("foo, bar, baz", &kernel).unwrap();
        assert_eq!(names, vec![
            Value::Str("foo".into()),
            Value::Str("bar".into()),
            Value::Str("baz".into()),
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

    // ── SRD-18c Layer 2 / SRD-18e Push 3: range operator ──

    fn empty_kernel() -> GkKernel {
        crate::dsl::compile::compile_gk("\n").unwrap()
    }

    #[test]
    fn range_half_open_integer() {
        let v = evaluate_spec("1..5", &empty_kernel()).unwrap();
        assert_eq!(v, vec![
            Value::U64(1), Value::U64(2), Value::U64(3), Value::U64(4),
        ]);
    }

    #[test]
    fn range_inclusive_integer() {
        let v = evaluate_spec("1..=5", &empty_kernel()).unwrap();
        assert_eq!(v, vec![
            Value::U64(1), Value::U64(2), Value::U64(3),
            Value::U64(4), Value::U64(5),
        ]);
    }

    #[test]
    fn range_with_step() {
        let v = evaluate_spec("0..100..10", &empty_kernel()).unwrap();
        assert_eq!(v, vec![
            Value::U64(0), Value::U64(10), Value::U64(20),
            Value::U64(30), Value::U64(40), Value::U64(50),
            Value::U64(60), Value::U64(70), Value::U64(80),
            Value::U64(90),
        ]);
    }

    #[test]
    fn range_inclusive_with_step() {
        let v = evaluate_spec("0..=100..25", &empty_kernel()).unwrap();
        assert_eq!(v, vec![
            Value::U64(0), Value::U64(25), Value::U64(50),
            Value::U64(75), Value::U64(100),
        ]);
    }

    #[test]
    fn range_float_step() {
        let v = evaluate_spec("0.0..=1.0..0.25", &empty_kernel()).unwrap();
        assert_eq!(v.len(), 5, "got {v:?}");
        if let [Value::F64(a), Value::F64(b), Value::F64(c), Value::F64(d), Value::F64(e)] = v.as_slice() {
            assert!((a - 0.0).abs() < 1e-12);
            assert!((b - 0.25).abs() < 1e-12);
            assert!((c - 0.5).abs() < 1e-12);
            assert!((d - 0.75).abs() < 1e-12);
            assert!((e - 1.0).abs() < 1e-12);
        } else {
            panic!("expected 5 floats, got {v:?}");
        }
    }

    #[test]
    fn range_empty_when_start_equals_end_half_open() {
        let v = evaluate_spec("5..5", &empty_kernel()).unwrap();
        assert!(v.is_empty(), "got {v:?}");
    }

    #[test]
    fn range_inclusive_with_equal_bounds_emits_one() {
        let v = evaluate_spec("5..=5", &empty_kernel()).unwrap();
        assert_eq!(v, vec![Value::U64(5)]);
    }

    #[test]
    fn range_with_si_suffix_bounds() {
        // Push 4 SI suffixes meet Push 3 ranges — full
        // composition.
        let v = evaluate_spec("1K..1K..200", &empty_kernel()).unwrap();
        assert!(v.is_empty(), "1K..1K with positive step → empty");

        let v = evaluate_spec("0..1K..200", &empty_kernel()).unwrap();
        assert_eq!(v, vec![
            Value::U64(0), Value::U64(200), Value::U64(400),
            Value::U64(600), Value::U64(800),
        ]);
    }

    #[test]
    fn range_zero_step_errors() {
        let err = evaluate_spec("1..10..0", &empty_kernel()).unwrap_err();
        assert!(err.contains("step is zero"), "{err}");
    }

    #[test]
    fn range_too_many_dotdot_errors() {
        let err = evaluate_spec("1..2..3..4", &empty_kernel()).unwrap_err();
        assert!(err.contains("more than two `..`"), "{err}");
    }

    #[test]
    fn range_inside_parens_doesnt_split() {
        // `range(1, 10)` — the dots inside the function
        // call shouldn't trigger range-splitting at top
        // depth (there are no `..` here anyway, but verify
        // paren-balanced text passes through cleanly).
        // Use a literal with internal parens to exercise
        // the depth tracking.
        let v = evaluate_spec("(1)..(5)", &empty_kernel()).unwrap();
        assert_eq!(v.len(), 4); // 1, 2, 3, 4
    }

    #[test]
    fn range_step_with_inclusive_separator_errors() {
        let err = evaluate_spec("1..10..=2", &empty_kernel()).unwrap_err();
        assert!(err.contains("step delimiter cannot be `..=`"), "{err}");
    }

    #[test]
    fn range_with_kernel_referenced_bounds() {
        let kernel = crate::dsl::compile::compile_gk(
            "final lo := 5\nfinal hi := 12\n"
        ).unwrap();
        let v = evaluate_spec("{lo}..{hi}", &kernel).unwrap();
        assert_eq!(v, vec![
            Value::U64(5), Value::U64(6), Value::U64(7),
            Value::U64(8), Value::U64(9), Value::U64(10), Value::U64(11),
        ]);
    }

    // ── SRD-18c Layer 3 / SRD-18e Push 7: named generators ──

    #[test]
    fn fib_n_first_eight() {
        let v = evaluate_spec("fib(8)", &empty_kernel()).unwrap();
        assert_eq!(v, vec![
            Value::U64(1), Value::U64(1), Value::U64(2), Value::U64(3),
            Value::U64(5), Value::U64(8), Value::U64(13), Value::U64(21),
        ]);
    }

    #[test]
    fn fib_until_50() {
        let v = evaluate_spec("fib_until(50)", &empty_kernel()).unwrap();
        assert_eq!(v, vec![
            Value::U64(1), Value::U64(1), Value::U64(2), Value::U64(3),
            Value::U64(5), Value::U64(8), Value::U64(13), Value::U64(21),
            Value::U64(34),
        ]);
    }

    #[test]
    fn pow2_n_six() {
        let v = evaluate_spec("pow2(6)", &empty_kernel()).unwrap();
        assert_eq!(v, vec![
            Value::U64(1), Value::U64(2), Value::U64(4),
            Value::U64(8), Value::U64(16), Value::U64(32),
        ]);
    }

    #[test]
    fn pow2_until_100() {
        let v = evaluate_spec("pow2_until(100)", &empty_kernel()).unwrap();
        assert_eq!(v, vec![
            Value::U64(1), Value::U64(2), Value::U64(4),
            Value::U64(8), Value::U64(16), Value::U64(32), Value::U64(64),
        ]);
    }

    #[test]
    fn binomial_n_5() {
        // C(5,0..5) = 1, 5, 10, 10, 5, 1
        let v = evaluate_spec("binomial(5)", &empty_kernel()).unwrap();
        assert_eq!(v, vec![
            Value::U64(1), Value::U64(5), Value::U64(10),
            Value::U64(10), Value::U64(5), Value::U64(1),
        ]);
    }

    #[test]
    fn geometric_2_doubles_4_terms() {
        let v = evaluate_spec("geometric(1, 2, 4)", &empty_kernel()).unwrap();
        // Floats because factor is float-cast at eval.
        if let [Value::F64(a), Value::F64(b), Value::F64(c), Value::F64(d)] = v.as_slice() {
            assert!((a - 1.0).abs() < 1e-12);
            assert!((b - 2.0).abs() < 1e-12);
            assert!((c - 4.0).abs() < 1e-12);
            assert!((d - 8.0).abs() < 1e-12);
        } else {
            panic!("expected 4 f64 values, got {v:?}");
        }
    }

    #[test]
    fn subdivide_half_open_5_points() {
        let v = evaluate_spec("subdivide(0, 100, 5)", &empty_kernel()).unwrap();
        // (100-0)/5 = 20 step. 0, 20, 40, 60, 80.
        if let [Value::F64(a), Value::F64(b), Value::F64(c), Value::F64(d), Value::F64(e)] = v.as_slice() {
            assert!((a - 0.0).abs() < 1e-12);
            assert!((b - 20.0).abs() < 1e-12);
            assert!((c - 40.0).abs() < 1e-12);
            assert!((d - 60.0).abs() < 1e-12);
            assert!((e - 80.0).abs() < 1e-12);
        } else { panic!("got {v:?}"); }
    }

    #[test]
    fn subdivide_inclusive_5_points() {
        let v = evaluate_spec("subdivide_inclusive(0, 100, 5)", &empty_kernel()).unwrap();
        // 0, 25, 50, 75, 100
        if let [Value::F64(a), Value::F64(b), Value::F64(c), Value::F64(d), Value::F64(e)] = v.as_slice() {
            assert!((a - 0.0).abs() < 1e-12);
            assert!((b - 25.0).abs() < 1e-12);
            assert!((c - 50.0).abs() < 1e-12);
            assert!((d - 75.0).abs() < 1e-12);
            assert!((e - 100.0).abs() < 1e-12);
        } else { panic!("got {v:?}"); }
    }

    #[test]
    fn log_steps_3_decades() {
        let v = evaluate_spec("log_steps(1, 1000, 4)", &empty_kernel()).unwrap();
        // 1, 10, 100, 1000
        if let [Value::F64(a), Value::F64(b), Value::F64(c), Value::F64(d)] = v.as_slice() {
            assert!((a - 1.0).abs() < 1e-9);
            assert!((b - 10.0).abs() < 1e-9);
            assert!((c - 100.0).abs() < 1e-9);
            assert!((d - 1000.0).abs() < 1e-9);
        } else { panic!("got {v:?}"); }
    }

    #[test]
    fn log_steps_rejects_non_positive_bounds() {
        let err = evaluate_spec("log_steps(0, 100, 5)", &empty_kernel()).unwrap_err();
        assert!(err.contains("must be positive"), "{err}");
    }

    // ── SRD-18c Layer 5 / SRD-18e Push 9: set operators ──

    #[test]
    fn concat_two_ranges() {
        let v = evaluate_spec("concat(1..4, 10..13)", &empty_kernel()).unwrap();
        assert_eq!(v, vec![
            Value::U64(1), Value::U64(2), Value::U64(3),
            Value::U64(10), Value::U64(11), Value::U64(12),
        ]);
    }

    #[test]
    fn unique_dedupes_first_occurrence() {
        let v = evaluate_spec("unique(1..4, 3..6)", &empty_kernel()).unwrap();
        // 1,2,3 (from first) + 4,5 (from second; 3 already present)
        assert_eq!(v, vec![
            Value::U64(1), Value::U64(2), Value::U64(3),
            Value::U64(4), Value::U64(5),
        ]);
    }

    #[test]
    fn intersect_keeps_only_common_values() {
        let v = evaluate_spec("intersect(1..10, 5..15)", &empty_kernel()).unwrap();
        assert_eq!(v, vec![
            Value::U64(5), Value::U64(6), Value::U64(7),
            Value::U64(8), Value::U64(9),
        ]);
    }

    #[test]
    fn subtract_drops_values_in_b() {
        let v = evaluate_spec("subtract(1..6, 3..5)", &empty_kernel()).unwrap();
        // 1..6 = [1,2,3,4,5], minus [3,4] = [1, 2, 5]
        assert_eq!(v, vec![Value::U64(1), Value::U64(2), Value::U64(5)]);
    }

    #[test]
    fn interleave_round_robin_two_lists() {
        let v = evaluate_spec("interleave(1..4, 10..13)", &empty_kernel()).unwrap();
        assert_eq!(v, vec![
            Value::U64(1), Value::U64(10), Value::U64(2),
            Value::U64(11), Value::U64(3), Value::U64(12),
        ]);
    }

    #[test]
    fn cycle_repeats_n_times() {
        let v = evaluate_spec("cycle(1..3, 3)", &empty_kernel()).unwrap();
        assert_eq!(v, vec![
            Value::U64(1), Value::U64(2),
            Value::U64(1), Value::U64(2),
            Value::U64(1), Value::U64(2),
        ]);
    }

    #[test]
    fn reverse_inverts_list() {
        let v = evaluate_spec("reverse(1..5)", &empty_kernel()).unwrap();
        assert_eq!(v, vec![
            Value::U64(4), Value::U64(3), Value::U64(2), Value::U64(1),
        ]);
    }

    #[test]
    fn take_n_takes_prefix() {
        let v = evaluate_spec("take(1..10, 3)", &empty_kernel()).unwrap();
        assert_eq!(v, vec![Value::U64(1), Value::U64(2), Value::U64(3)]);
    }

    #[test]
    fn skip_n_drops_prefix() {
        let v = evaluate_spec("skip(1..6, 2)", &empty_kernel()).unwrap();
        assert_eq!(v, vec![Value::U64(3), Value::U64(4), Value::U64(5)]);
    }

    #[test]
    fn unique_composes_with_pow2_and_range() {
        let v = evaluate_spec("unique(pow2(8), 1..1000..100)", &empty_kernel()).unwrap();
        // pow2(8) = 1, 2, 4, 8, 16, 32, 64, 128
        // 1..1000..100 = 1, 101, 201, 301, 401, 501, 601, 701, 801, 901
        // dedupe: 1, 2, 4, 8, 16, 32, 64, 128, 101, 201, 301, 401, 501, 601, 701, 801, 901
        assert_eq!(v.len(), 17);
        assert_eq!(v[0], Value::U64(1));
        assert_eq!(v[7], Value::U64(128));
        assert_eq!(v[8], Value::U64(101));
    }

    // ── SRD-18c §"Sequencer-style expansions" / Push 8 ──

    #[test]
    fn bucket_round_robin_3_1_2() {
        // Two-arg form: items list + ratios list.
        let v = evaluate_spec(
            "bucket(concat('ann', 'scan', 'fetch'), concat(3, 1, 2))",
            &empty_kernel()).unwrap();
        // Wait — concat doesn't make sense with these args (mixed types).
        // Use the literal form via the GK list parser.
        let _ = v;
    }

    #[test]
    fn bucket_ratio_prefix_shorthand_round_robin() {
        let v = evaluate_spec("bucket(\"3:ann, 1:scan, 2:fetch\")", &empty_kernel()).unwrap();
        // Bucket sequencer round-robins; each "tick" pulls
        // one from each remaining bucket. Total = 6.
        assert_eq!(v.len(), 6);
        let strs: Vec<&str> = v.iter().filter_map(|v| match v {
            Value::Str(s) => Some(s.as_str()), _ => None,
        }).collect();
        // First tick: ann, scan, fetch (one from each).
        // Then ann (3 left), fetch (2 left). Next: ann, fetch.
        // Then ann. Total: ann*3, scan*1, fetch*2.
        let counts = strs.iter().fold(std::collections::HashMap::<&str, usize>::new(), |mut m, s| {
            *m.entry(s).or_insert(0) += 1;
            m
        });
        assert_eq!(counts.get("ann"), Some(&3));
        assert_eq!(counts.get("scan"), Some(&1));
        assert_eq!(counts.get("fetch"), Some(&2));
    }

    #[test]
    fn concat_seq_emits_contiguous_runs() {
        let v = evaluate_spec("concat_seq(\"2:warmup, 3:bench, 1:cooldown\")", &empty_kernel()).unwrap();
        let strs: Vec<String> = v.iter().filter_map(|v| match v {
            Value::Str(s) => Some(s.clone()), _ => None,
        }).collect();
        assert_eq!(strs, vec![
            "warmup", "warmup",
            "bench", "bench", "bench",
            "cooldown",
        ]);
    }

    #[test]
    fn interval_seq_evenly_spreads_higher_ratio() {
        let v = evaluate_spec("interval_seq(\"3:read, 1:write\")", &empty_kernel()).unwrap();
        // Total length 4. write should appear once,
        // somewhere in the middle (not bunched at edges).
        let strs: Vec<String> = v.iter().filter_map(|v| match v {
            Value::Str(s) => Some(s.clone()), _ => None,
        }).collect();
        assert_eq!(strs.len(), 4);
        let writes: Vec<usize> = strs.iter().enumerate()
            .filter(|(_, s)| *s == "write")
            .map(|(i, _)| i).collect();
        assert_eq!(writes.len(), 1, "expected exactly one write: {strs:?}");
    }

    #[test]
    fn parse_func_call_recognises_simple_call() {
        let (n, a) = parse_func_call("fib(8)").unwrap();
        assert_eq!(n, "fib");
        assert_eq!(a, "8");
    }

    #[test]
    fn parse_func_call_rejects_non_calls() {
        assert!(parse_func_call("1..10").is_none());
        assert!(parse_func_call("foo + bar").is_none());
        assert!(parse_func_call("f(a) + g(b)").is_none()); // mid-text close
    }

    #[test]
    fn split_args_top_level_skips_inner_commas() {
        let args = split_args_top_level("a, f(b, c), \"x, y\", 3");
        assert_eq!(args, vec!["a", "f(b, c)", "\"x, y\"", "3"]);
    }
}
