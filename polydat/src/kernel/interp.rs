// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! `{name}`-style template interpolation against a Polydat Kernel.
//!
//! Surface #5 home (per `polydat/docs/design/comprehension_cutover_contact_surfaces.md`).
//! Previously lived in `polydat::iteration::comprehension::eval`; relocated
//! to the kernel module because the operation is general
//! GK-kernel facility, not a comprehension concern. The
//! comprehension runtime uses it; synthesisers use it; the
//! executor uses it; but it doesn't depend on comprehension AST
//! shape.
//!
//! ## Functions
//!
//! - [`interpolate_via_kernel`] — looks up `{name}` placeholders
//!   against the kernel's chain-aware bindings via
//!   [`PolydatKernel::lookup`].
//! - [`interpolate_with_lookup`] — the generic engine; the
//!   `lookup` closure decides where each leaf's value comes
//!   from. Used by callers that compose their own lookup over
//!   the kernel plus workload params plus synthesis-time
//!   probes.
//! - [`collect_string_interp_refs`] — extracts the placeholder
//!   names from a text without doing substitution.
//!
//! ## Semantics
//!
//! Iterative leaf-placeholder substitution with escape handling
//! and a round cap:
//!
//! - **Leaf**: `{name}` whose body contains no further `{`. The
//!   dynamic form `{a_{b}_c}` is resolved by first substituting
//!   `{b}`, then re-scanning for the resulting `{a_<b-value>_c}`
//!   as a leaf.
//! - **Escape**: `\{` and `\}` pass through as literal `{` /
//!   `}` and are removed from the final string.
//! - **Round cap**: if substitution doesn't stabilize in
//!   [`ROUND_HARD`] iterations, returns Err (the input had
//!   cyclic placeholders).
//! - **Unresolved name**: any `{name}` that survives the
//!   substitution rounds errors with a diagnostic naming the
//!   missing binding.

use std::collections::HashSet;

use crate::kernel::PolydatKernel;

/// Round count at which we warn about possible cycles in the
/// substitution stream.
const ROUND_WARN: usize = 100;

/// Hard round-count limit. Errors out if substitution doesn't
/// stabilize in this many iterations.
const ROUND_HARD: usize = 1000;

/// Interpolate `{name}` placeholders against `kernel`.
///
/// `{name}` resolves to `kernel.lookup(name).map(|v| v.to_display_string())`.
/// `Value::None` (an unset extern slot) doesn't match — falls
/// through to the unresolved-name error path at the fixed
/// point.
///
/// Returns a typed [`crate::dsl::compile::EmbeddingError`] per
/// E7 of the spec; the underlying string-form
/// [`interpolate_with_lookup`] is kept for callers that
/// compose their own lookup and don't want the
/// typed-error overhead.
pub fn interpolate_via_kernel(
    text: &str,
    kernel: &PolydatKernel,
) -> Result<String, crate::dsl::compile::EmbeddingError> {
    interpolate_with_lookup(text, |name| {
        kernel.lookup(name).map(|v| v.to_display_string())
    })
    .map_err(|msg| classify_interpolate_error(text, msg))
}

fn classify_interpolate_error(
    text: &str,
    msg: String,
) -> crate::dsl::compile::EmbeddingError {
    // "interpolation: unresolved placeholder '{name}' in '...'"
    if let Some(rest) = msg.strip_prefix("interpolation: unresolved placeholder '{")
        && let Some(end) = rest.find('}') {
            let name = rest[..end].to_string();
            return crate::dsl::compile::EmbeddingError::UnresolvedPlaceholder {
                name,
                source: text.to_string(),
            };
        }
    // Cyclic placeholder fall-through: classify as Parse since
    // the text didn't stabilise.
    crate::dsl::compile::EmbeddingError::Parse {
        source: text.to_string(),
        message: msg,
        position: None,
    }
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
        if !progress {
            break;
        }
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

/// Extract every leaf `{name}` placeholder mentioned inside
/// string-literal contexts in `src` into `refs`.
///
/// Used by the synthesiser to discover names the body
/// references via `{name}` interpolation that don't appear as
/// bare identifiers in the Polydat source. The detection is
/// quote-aware: leading non-identifier chars (`'`, `"`) skip
/// the placeholder, matching the binding compiler's
/// `string_lit_has_real_placeholder` disambiguation.
pub fn collect_string_interp_refs(src: &str, refs: &mut HashSet<String>) {
    let chars: Vec<char> = src.chars().collect();
    let mut i = 0;
    let mut in_str: Option<char> = None;
    while i < chars.len() {
        let c = chars[i];
        match in_str {
            Some(quote) if c == quote => {
                in_str = None;
                i += 1;
            }
            Some(_) if c == '\\' && i + 1 < chars.len() => {
                i += 2;
            }
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
            Some(_) => {
                i += 1;
            }
            None if c == '"' || c == '\'' => {
                in_str = Some(c);
                i += 1;
            }
            None => {
                i += 1;
            }
        }
    }
}

/// One sweep over `s`: replaces every **leaf** placeholder
/// (`{NAME}` whose body contains no `{` or `}`) with its
/// resolved value via the supplied `lookup` closure. Returns
/// `Ok(true)` if any replacement happened, `Ok(false)` if the
/// pass was a no-op (fixed point reached).
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
                if cj == b'{' {
                    has_inner_open = true;
                    break;
                }
                if cj == b'}' {
                    end = Some(j);
                    break;
                }
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
        // Passthrough. ASCII bytes copy directly; a non-ASCII
        // lead byte starts a multi-byte UTF-8 char that must be
        // copied whole (`c as char` would split it into mojibake).
        // `i` is always at a char boundary here — the scanner only
        // advances past ASCII specials (`{` `}` `\`) or whole
        // placeholders.
        if c < 0x80 {
            out.push(c as char);
            i += 1;
        } else {
            let ch = s[i..].chars().next().expect("byte index at char boundary");
            out.push(ch);
            i += ch.len_utf8();
        }
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
                if bytes[j] == b'{' {
                    break;
                }
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
    // Char-based, not byte-based: `bytes[i] as char` would split
    // any multi-byte UTF-8 sequence (e.g. `…` U+2026) into
    // mojibake. Only `\{` and `\}` are unescaped; every other
    // character — ASCII or not — passes through intact.
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\\'
            && let Some(&next) = chars.peek()
                && (next == '{' || next == '}') {
                    out.push(next);
                    chars.next();
                    continue;
                }
        out.push(c);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn h(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    #[test]
    fn interpolate_with_lookup_resolves_leaves() {
        let m = h(&[("name", "Alice"), ("count", "42")]);
        let s = interpolate_with_lookup("hello {name}, you have {count} items", |n| {
            m.get(n).cloned()
        })
        .unwrap();
        assert_eq!(s, "hello Alice, you have 42 items");
    }

    #[test]
    fn interpolate_with_lookup_handles_escapes() {
        let m = h(&[("x", "1")]);
        let s = interpolate_with_lookup("\\{literal\\} and {x}", |n| m.get(n).cloned()).unwrap();
        assert_eq!(s, "{literal} and 1");
    }

    #[test]
    fn interpolate_with_lookup_resolves_dynamic_via_iteration() {
        // `{a_{b}_c}` resolves by first substituting {b} = "X",
        // then re-scanning to find `{a_X_c}` as a leaf.
        let m = h(&[("b", "X"), ("a_X_c", "RESULT")]);
        let s = interpolate_with_lookup("got {a_{b}_c}", |n| m.get(n).cloned()).unwrap();
        assert_eq!(s, "got RESULT");
    }

    #[test]
    fn interpolate_with_lookup_errors_on_unresolved() {
        let m = h(&[]);
        let err = interpolate_with_lookup("missing: {nope}", |n| m.get(n).cloned()).unwrap_err();
        assert!(err.contains("unresolved placeholder"));
    }

    #[test]
    fn collect_string_interp_refs_picks_quoted_placeholders() {
        let mut refs = HashSet::new();
        collect_string_interp_refs(r#"do "x = {var}" and "{another}""#, &mut refs);
        assert!(refs.contains("var"));
        assert!(refs.contains("another"));
    }

    #[test]
    fn collect_string_interp_refs_skips_outside_strings() {
        let mut refs = HashSet::new();
        collect_string_interp_refs("bare {not_picked} and \"yes {picked}\"", &mut refs);
        assert!(refs.contains("picked"));
        assert!(!refs.contains("not_picked"));
    }
}
