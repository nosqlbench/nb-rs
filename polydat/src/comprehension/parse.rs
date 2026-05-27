// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Comprehension spec parser — text → AST.
//!
//! The textual form is `var in expr` per clause, comma-separated
//! at clause boundaries, with paren-respecting splitting so
//! function-call argument commas and multi-value list commas
//! aren't mistaken for clause separators.
//!
//! ## Two entry points
//!
//! - [`parse_clause_list`] turns one comma-separated string into
//!   `Vec<Clause>`. Each entry from the YAML's array form, or the
//!   top-level entries of the YAML's string form, calls this.
//! - [`comprehension_from_subspaces`] takes the parsed sub-spaces
//!   (each sub-space is a `Vec<Clause>` — a Cartesian list) and
//!   decides between [`ComprehensionMode::Cartesian`] and
//!   [`ComprehensionMode::Union`]. This is the structural
//!   detection rule: any variable name repeating across the
//!   sub-spaces' flat clause set ⇒ Union; otherwise Cartesian
//!   over the flattened list.
//!
//! YAML-shape detection (string vs list vs object) stays in
//! `nbrs-workload` — it's YAML-shaped, not GK-shaped. The
//! workload parser builds `Vec<Vec<Clause>>` from the YAML
//! using these primitives, then calls
//! [`comprehension_from_subspaces`].

use std::collections::HashMap;

use super::ast::{Clause, Comprehension, ShellOrigin, TraversalOrder, ZipMode};

/// Parse a single clause.
///
/// Two shapes are recognised:
///
/// - **Single-var** (Layers 1–6): `var in expr`. The lone
///   variable on the LHS binds successive values from the
///   single source on the RHS. This is the historical shape
///   and remains the common case.
/// - **Parallel-iter** (SRD-18c Layer 7a): `(a, b, …) in
///   (e1, e2, …)`. Each variable on the LHS binds the
///   corresponding source on the RHS; the sources advance in
///   lockstep ("zip"). Length-mismatch across the group is a
///   strict-mode error at scope-init.
///
/// Returns `Err` for malformed input — the caller decides
/// whether to keep going with whatever did parse cleanly. The
/// error message names the clause text so it's surfaceable as
/// a diagnostic.
pub fn parse_clause(s: &str) -> Result<Clause, String> {
    // Find the first top-paren-depth-0 ` in ` separator. The
    // LHS may be a parenthesised group `(a, b)` whose internal
    // commas are at depth ≥ 1; the RHS likewise. Walking with
    // depth-aware lookahead is the only reliable split.
    let bytes = s.as_bytes();
    let mut depth: i32 = 0;
    let mut i: usize = 0;
    while i + 4 <= bytes.len() {
        let ch = bytes[i];
        match ch {
            b'(' | b'[' | b'{' => { depth += 1; i += 1; }
            b')' | b']' | b'}' => { depth -= 1; i += 1; }
            b' ' if depth == 0
                && bytes.get(i + 1) == Some(&b'i')
                && bytes.get(i + 2) == Some(&b'n')
                && bytes.get(i + 3) == Some(&b' ') =>
            {
                let lhs = s[..i].trim();
                let rhs = s[i + 4..].trim();
                return parse_clause_from_sides(lhs, rhs, s);
            }
            _ => { i += 1; }
        }
    }
    Err(format!("invalid for_each clause: '{s}' (expected 'var in expr')"))
}

/// Build a `Clause` from already-split `lhs` and `rhs` text.
///
/// LHS shape decides single-var vs parallel-iter:
/// - `var` (bare identifier) → single-var clause.
/// - `(a, b, ...)` → parallel-iter clause.
///
/// For parallel-iter, RHS is one of three forms (see
/// [`ZipMode`]):
/// - `(e1, e2, ...)` — strict zip (default).
/// - `zip_truncate(e1, e2, ...)` — truncate to shortest.
/// - `zip_cycle(e1, e2, ...)` — cycle to longest.
///
/// `whole` is only used to enrich error messages.
fn parse_clause_from_sides(lhs: &str, rhs: &str, whole: &str) -> Result<Clause, String> {
    let lhs_paren = is_paren_wrapped(lhs);
    let (rhs_inner, mode) = match strip_zip_mode_prefix(rhs) {
        Some((inner, m)) => (inner, m),
        None => (rhs.to_string(), ZipMode::Strict),
    };
    let rhs_paren = is_paren_wrapped(&rhs_inner);
    let rhs_explicit_paren = mode != ZipMode::Strict || rhs_paren;
    if lhs_paren || rhs_explicit_paren {
        if !(lhs_paren && rhs_explicit_paren) {
            return Err(format!(
                "parallel-iter clause '{whole}' requires parentheses on both sides \
                 (e.g. '(a, b) in (e1, e2)' or '(a, b) in zip_truncate(e1, e2)')"
            ));
        }
        let vars = split_paren_group(lhs);
        // For zip_truncate(...) / zip_cycle(...), `rhs_inner`
        // is the parenthesised arg list and parses identically
        // to the bare `(e1, e2)` form.
        let exprs = split_paren_group(&rhs_inner);
        for v in &vars {
            if !is_simple_ident(v) {
                return Err(format!(
                    "parallel-iter clause '{whole}': '{v}' is not a valid variable name"
                ));
            }
        }
        if vars.len() < 2 {
            return Err(format!(
                "parallel-iter clause '{whole}' requires ≥ 2 variables \
                 (use 'var in expr' for the single-var form)"
            ));
        }
        if vars.len() != exprs.len() {
            return Err(format!(
                "parallel-iter clause '{whole}': {} variables but {} expressions",
                vars.len(), exprs.len()
            ));
        }
        Ok(Clause::parallel_with_mode(mode, vars, exprs))
    } else {
        Ok(Clause::new(lhs, rhs))
    }
}

/// Strip a leading `zip_truncate(...)` / `zip_cycle(...)`
/// wrapper from `rhs` and return `(inner, mode)` where `inner`
/// is the parenthesised argument list (still wrapped in
/// parens) and `mode` is the corresponding [`ZipMode`].
/// Returns `None` if `rhs` isn't a recognised zip-mode form.
fn strip_zip_mode_prefix(rhs: &str) -> Option<(String, ZipMode)> {
    for (prefix, mode) in [
        ("zip_truncate", ZipMode::Truncate),
        ("zip_cycle",    ZipMode::Cycle),
    ] {
        if let Some(rest) = rhs.strip_prefix(prefix) {
            let trimmed = rest.trim_start();
            if trimmed.starts_with('(') && is_paren_wrapped(trimmed) {
                return Some((trimmed.to_string(), mode));
            }
        }
    }
    None
}

/// True if `s` starts with `(` and the matching close-paren is
/// the final character (no trailing text). Whitespace inside is
/// fine; whitespace outside is the caller's job to trim.
fn is_paren_wrapped(s: &str) -> bool {
    let bytes = s.as_bytes();
    if bytes.first() != Some(&b'(') || bytes.last() != Some(&b')') {
        return false;
    }
    let mut depth: i32 = 0;
    for (i, &b) in bytes.iter().enumerate() {
        match b {
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return i == bytes.len() - 1;
                }
            }
            _ => {}
        }
    }
    false
}

/// Split a `(a, b, c)` group on its top-paren-depth-1 commas.
/// Caller must ensure the input passes [`is_paren_wrapped`].
fn split_paren_group(s: &str) -> Vec<String> {
    let inner = &s[1..s.len() - 1];
    let bytes = inner.as_bytes();
    let mut parts: Vec<String> = Vec::new();
    let mut start: usize = 0;
    let mut depth: i32 = 0;
    let mut i: usize = 0;
    while i < bytes.len() {
        let ch = bytes[i];
        match ch {
            b'(' | b'[' | b'{' => { depth += 1; i += 1; }
            b')' | b']' | b'}' => { depth -= 1; i += 1; }
            b',' if depth == 0 => {
                parts.push(inner[start..i].trim().to_string());
                start = i + 1;
                i += 1;
            }
            _ => { i += 1; }
        }
    }
    let tail = inner[start..].trim();
    if !tail.is_empty() {
        parts.push(tail.to_string());
    }
    parts
}

fn is_simple_ident(s: &str) -> bool {
    let mut chars = s.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Parse the full GK comprehension text grammar:
/// `<clause_list> [where <predicate>]`.
///
/// The clause list is a comma-separated sequence of `var in expr`
/// clauses (paren-respecting; see [`parse_clause_list`]). The
/// optional `where` keyword at top-paren-depth-0 ends the
/// clause list and starts a single GK predicate expression that
/// runs to end-of-string. The predicate is **not** parsed here —
/// it's stored as text and evaluated at iteration time against
/// the per-tuple kernel.
///
/// Mode (Cartesian vs Union) is decided by
/// [`comprehension_from_subspaces`] from the clause list. The
/// filter, if present, attaches uniformly to both modes — one
/// predicate per emitted tuple.
///
/// Examples:
/// ```text
/// k in 10,100, limit in 10,20,30
/// k in 10,100 where k > 5
/// k in 10,100, limit in 10,20,30,50,100,200,300 where k * limit < 1000
/// ```
pub fn parse_comprehension_text(text: &str) -> Result<Comprehension, String> {
    // Split off the optional `order <spec>` first (it's the
    // outermost clause), then split off the optional
    // `where <pred>` from what remains.
    let (head, order_text) = split_at_order(text);
    let (clause_text, filter) = split_at_where(&head);
    let clauses = parse_clause_list(&clause_text)?;
    // String form: each clause is its own sub-space so the
    // detection rule (repeated names ⇒ Union) sees per-clause
    // boundaries. Same convention `nbrs-workload` uses.
    let subspaces: Vec<Vec<Clause>> = clauses.into_iter()
        .map(|c| vec![c])
        .collect();
    let mut comp = comprehension_from_subspaces(subspaces);
    if let Some(predicate) = filter {
        comp = comp.with_filter(predicate);
    }
    if let Some(spec) = order_text {
        comp = comp.with_order(parse_order_spec(&spec)?);
    }
    // Single-source invariant check: structural shape +
    // index-space-ordering vs. Union compatibility.
    comp.validate().map_err(|errs| errs.join("; "))?;
    Ok(comp)
}

/// Backward-compat shim — call [`Comprehension::validate`]
/// instead. Kept so external `nbrs-workload` callers don't
/// need a same-day update; will be retired once those move.
#[deprecated(note = "use Comprehension::validate() — single source of truth for AST invariants")]
pub fn validate_order_for_mode(
    mode: &super::ast::ComprehensionMode,
    order: &Option<TraversalOrder>,
) -> Result<(), String> {
    super::ast::check_order_for_mode(mode, order)
}

/// Parse an order spec string into a [`TraversalOrder`].
///
/// Three syntactic shapes (per SRD-18d §"GK text grammar"):
///
/// - **Bare name**: `lex`, `extrema`, `shells`, `sobol`, …
///   No truncation; uses the strategy's defaults.
/// - **Terse `name/N`**: `extrema/1`, `shells/2`, `halton/64`,
///   `lex/100`. The `/N` suffix is the strategy's natural
///   truncation parameter (count, strata, or depth).
/// - **Keyword form `name(arg=val, …)`**: full parameter
///   surface for strategies with multiple knobs.
///   `shells(origin=center, depth=3)`,
///   `lhs(count=20, seed=42)`,
///   `space_filling(sobol, count=64)`.
pub fn parse_order_spec(text: &str) -> Result<TraversalOrder, String> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Err("order spec is empty".to_string());
    }

    // Check for keyword form: `name(arg=val, ...)`
    if let Some(open) = trimmed.find('(') {
        if !trimmed.ends_with(')') {
            return Err(format!(
                "order spec '{trimmed}': unbalanced parens — expected `name(...)`"
            ));
        }
        let name = trimmed[..open].trim();
        let body = &trimmed[open + 1..trimmed.len() - 1];
        return build_order_from_keyword(name, body);
    }

    // Terse form `name/N` or bare `name`.
    let (name, n_opt) = match trimmed.find('/') {
        Some(slash) => {
            let n_text = trimmed[slash + 1..].trim();
            let n = n_text.parse::<usize>().map_err(|_| format!(
                "order spec '{trimmed}': '/N' suffix must be a non-negative integer, got '{n_text}'"
            ))?;
            (trimmed[..slash].trim(), Some(n))
        }
        None => (trimmed, None),
    };

    build_order_from_terse(name, n_opt)
}

fn build_order_from_terse(name: &str, n: Option<usize>) -> Result<TraversalOrder, String> {
    match name {
        "lex" => Ok(TraversalOrder::Lex { count: n }),
        "reverse_lex" => Ok(TraversalOrder::ReverseLex { count: n }),
        "diagonal" => Ok(TraversalOrder::Diagonal { count: n }),
        "antidiagonal" => Ok(TraversalOrder::Antidiagonal { count: n }),
        "extrema" => Ok(TraversalOrder::Extrema { strata: n }),
        "shells" => Ok(TraversalOrder::Shells {
            origin: ShellOrigin::Outer,
            depth: n,
        }),
        "halton" => Ok(TraversalOrder::Halton { count: n }),
        "sobol" => Ok(TraversalOrder::Sobol { count: n }),
        "lhs" => Ok(TraversalOrder::Lhs { count: n, seed: None }),
        "custom" => Err(format!(
            "order spec 'custom': use 'custom(<function>)' to name the GK function"
        )),
        other => Err(format!(
            "order spec: unknown strategy '{other}' — \
             expected one of lex/reverse_lex/diagonal/antidiagonal/extrema/shells/halton/sobol/lhs/custom"
        )),
    }
}

fn build_order_from_keyword(name: &str, body: &str) -> Result<TraversalOrder, String> {
    let args = parse_keyword_args(body)?;
    let count = args.iter().find_map(|(k, v)| (k == "count").then(|| v.parse::<usize>().ok()).flatten());
    let depth = args.iter().find_map(|(k, v)| (k == "depth").then(|| v.parse::<usize>().ok()).flatten());
    let strata = args.iter().find_map(|(k, v)| (k == "strata").then(|| v.parse::<usize>().ok()).flatten());
    let seed = args.iter().find_map(|(k, v)| (k == "seed").then(|| v.parse::<u64>().ok()).flatten());

    match name {
        "lex" => Ok(TraversalOrder::Lex { count }),
        "reverse_lex" => Ok(TraversalOrder::ReverseLex { count }),
        "diagonal" => Ok(TraversalOrder::Diagonal { count }),
        "antidiagonal" => Ok(TraversalOrder::Antidiagonal { count }),
        "extrema" => Ok(TraversalOrder::Extrema { strata }),
        "shells" => {
            let origin = match args.iter().find_map(|(k, v)| (k == "origin").then_some(v.as_str())) {
                Some("outer") | None => ShellOrigin::Outer,
                Some("center") => ShellOrigin::Center,
                Some("corner") => ShellOrigin::Corner,
                Some(other) => return Err(format!(
                    "order shells: unknown origin '{other}' — expected outer/center/corner"
                )),
            };
            Ok(TraversalOrder::Shells { origin, depth })
        }
        "halton" => Ok(TraversalOrder::Halton { count }),
        "sobol" => Ok(TraversalOrder::Sobol { count }),
        "lhs" => Ok(TraversalOrder::Lhs { count, seed }),
        "space_filling" => {
            // `space_filling(strategy, count=N, seed=N)` —
            // strategy is the first positional arg.
            let strategy = args.iter()
                .find(|(k, _)| k.is_empty())
                .map(|(_, v)| v.as_str())
                .ok_or_else(|| "space_filling: missing strategy name (halton/sobol/lhs)".to_string())?;
            match strategy {
                "halton" => Ok(TraversalOrder::Halton { count }),
                "sobol" => Ok(TraversalOrder::Sobol { count }),
                "lhs" => Ok(TraversalOrder::Lhs { count, seed }),
                other => Err(format!(
                    "space_filling: unknown strategy '{other}' — expected halton/sobol/lhs"
                )),
            }
        }
        "custom" => {
            let function = args.iter()
                .find(|(k, _)| k.is_empty())
                .map(|(_, v)| v.clone())
                .ok_or_else(|| "custom: missing function name".to_string())?;
            Ok(TraversalOrder::Custom { function })
        }
        other => Err(format!(
            "order spec: unknown strategy '{other}'"
        )),
    }
}

/// Parse a keyword/positional argument body — `arg, key=val, key2=val2`.
/// Positional args are returned with an empty key. Splits on commas
/// at top paren-depth (function-call commas inside arg values are
/// preserved). Strips matching surrounding quotes from values.
fn parse_keyword_args(body: &str) -> Result<Vec<(String, String)>, String> {
    let mut out = Vec::new();
    let bytes = body.as_bytes();
    let n = bytes.len();
    let mut start = 0;
    let mut i = 0;
    let mut depth: u32 = 0;
    let push = |s: &str, out: &mut Vec<(String, String)>| {
        let trimmed = s.trim();
        if trimmed.is_empty() { return; }
        let (k, v) = if let Some(eq) = trimmed.find('=') {
            (trimmed[..eq].trim().to_string(), trimmed[eq + 1..].trim().to_string())
        } else {
            (String::new(), trimmed.to_string())
        };
        let v = strip_quotes(&v);
        out.push((k, v));
    };
    while i < n {
        let ch = bytes[i];
        match ch {
            b'(' | b'[' | b'{' => { depth = depth.saturating_add(1); i += 1; }
            b')' | b']' | b'}' => { depth = depth.saturating_sub(1); i += 1; }
            b',' if depth == 0 => {
                push(&body[start..i], &mut out);
                start = i + 1;
                i += 1;
            }
            _ => { i += 1; }
        }
    }
    push(&body[start..], &mut out);
    Ok(out)
}

fn strip_quotes(s: &str) -> String {
    let s = s.trim();
    if (s.starts_with('"') && s.ends_with('"') && s.len() >= 2)
        || (s.starts_with('\'') && s.ends_with('\'') && s.len() >= 2)
    {
        s[1..s.len() - 1].to_string()
    } else {
        s.to_string()
    }
}

/// Split a comprehension text on the top-level ` order `
/// keyword. Returns `(head, Some(order_spec))` if an order
/// clause is present at paren-depth 0, or `(text, None)`
/// otherwise. The `order` keyword is consumed; the spec is
/// trimmed.
pub fn split_at_order(text: &str) -> (String, Option<String>) {
    const KEYWORD: &str = " order ";
    let bytes = text.as_bytes();
    let n = bytes.len();
    let mut depth: u32 = 0;
    let mut i: usize = 0;
    while i < n {
        let ch = bytes[i];
        match ch {
            b'(' | b'[' | b'{' => { depth = depth.saturating_add(1); i += 1; }
            b')' | b']' | b'}' => { depth = depth.saturating_sub(1); i += 1; }
            b' ' if depth == 0 && text.is_char_boundary(i)
                && text[i..].starts_with(KEYWORD) =>
            {
                let head = text[..i].to_string();
                let spec = text[i + KEYWORD.len()..].trim().to_string();
                if spec.is_empty() {
                    return (text.to_string(), None);
                }
                return (head, Some(spec));
            }
            _ => { i += 1; }
        }
    }
    (text.to_string(), None)
}

/// Split a comprehension text on the top-level ` where `
/// keyword. Returns `(clause_text, Some(predicate))` if a
/// `where` clause is present at paren-depth 0, or
/// `(text, None)` otherwise. The predicate is trimmed; the
/// `where` keyword itself is consumed.
///
/// "Top-level" means paren-depth 0 — `where` substrings inside
/// `(...)`, `[...]`, or `{...}` are ignored, so a clause
/// expression like `f(where_clause('foo'))` survives intact.
pub fn split_at_where(text: &str) -> (String, Option<String>) {
    const KEYWORD: &str = " where ";
    let bytes = text.as_bytes();
    let n = bytes.len();
    let mut depth: u32 = 0;
    let mut i: usize = 0;
    while i < n {
        let ch = bytes[i];
        match ch {
            b'(' | b'[' | b'{' => { depth = depth.saturating_add(1); i += 1; }
            b')' | b']' | b'}' => { depth = depth.saturating_sub(1); i += 1; }
            b' ' if depth == 0 && text.is_char_boundary(i)
                && text[i..].starts_with(KEYWORD) =>
            {
                let prefix = text[..i].to_string();
                let suffix = text[i + KEYWORD.len()..].trim().to_string();
                if suffix.is_empty() {
                    return (text.to_string(), None);
                }
                return (prefix, Some(suffix));
            }
            _ => { i += 1; }
        }
    }
    (text.to_string(), None)
}

/// Parse a comma-separated clause list — the textual content
/// of one comprehension sub-space.
///
/// Splits on commas at paren-depth 0 that are followed (after
/// whitespace) by an `<ident> in ` token. This splits real
/// clause boundaries while leaving:
///
/// - Function-call inner commas:
///   `matching_profiles('a', 'b')` stays one expression.
/// - Multi-value inner commas:
///   `limit in 10,20,30` stays one clause whose expression is
///   `10,20,30`.
///
/// Each entry that doesn't parse as `var in expr` produces an
/// error in the result; the function returns the first error
/// encountered.
pub fn parse_clause_list(text: &str) -> Result<Vec<Clause>, String> {
    let mut out = Vec::new();
    for part in split_respecting_parens(text) {
        let trimmed = part.trim();
        if trimmed.is_empty() { continue; }
        out.push(parse_clause(trimmed)?);
    }
    Ok(out)
}

/// Build a [`Comprehension`] from a list of pre-parsed
/// sub-spaces. Each `subspaces[i]` is one Cartesian clause
/// list (the output of [`parse_clause_list`] for one of the
/// YAML's array-form entries, or one entry for the YAML's
/// map / string forms).
///
/// **Detection rule**: if any variable name appears more than
/// once across the flat list of all sub-spaces' clauses,
/// emit [`ComprehensionMode::Union`] (preserving sub-space
/// boundaries). Otherwise — every var name distinct — flatten
/// into a single [`ComprehensionMode::Cartesian`] list.
///
/// This collapses the YAML's string form (which produces one
/// sub-space per top-level clause) into the natural
/// Cartesian shape when names are distinct, while still
/// detecting repeats as a Union signal. Same rule the
/// pre-refactor workload parser applied — see
/// `nbrs-workload/src/parse.rs::parse_scenario_nodes`.
pub fn comprehension_from_subspaces(subspaces: Vec<Vec<Clause>>) -> Comprehension {
    let mut counts: HashMap<&str, usize> = HashMap::new();
    for set in &subspaces {
        for clause in set {
            for v in &clause.vars {
                *counts.entry(v.as_str()).or_insert(0) += 1;
            }
        }
    }
    let any_repeat = counts.values().any(|c| *c > 1);

    if any_repeat {
        Comprehension::union(subspaces)
    } else {
        let flat: Vec<Clause> = subspaces.into_iter().flatten().collect();
        Comprehension::cartesian(flat)
    }
}

/// Split a comma-separated clause list on clause boundaries.
///
/// A clause boundary is a comma at paren-depth 0 followed
/// (after whitespace) by an `<ident> in ` token, where
/// `<ident>` is a Rust-style identifier. The lookahead is the
/// only reliable signal — the YAML grammar above doesn't
/// supply any other syntactic boundary, so a comma might be a
/// new clause OR a comma inside a value list / function call.
///
/// Returns the parts as owned `String`s so callers don't
/// thread the input lifetime through every step. Empty parts
/// (consecutive commas, leading/trailing whitespace) are
/// preserved here and dropped in [`parse_clause_list`].
pub fn split_respecting_parens(s: &str) -> Vec<String> {
    let bytes = s.as_bytes();
    let mut parts: Vec<String> = Vec::new();
    let mut start: usize = 0;
    let mut i: usize = 0;
    let mut depth: u32 = 0;
    while i < bytes.len() {
        let ch = bytes[i];
        match ch {
            b'(' | b'[' | b'{' => { depth = depth.saturating_add(1); i += 1; }
            b')' | b']' | b'}' => { depth = depth.saturating_sub(1); i += 1; }
            b',' if depth == 0 => {
                if is_clause_boundary(&s[i + 1..]) {
                    parts.push(s[start..i].to_string());
                    start = i + 1;
                    i += 1;
                } else {
                    // Comma is inside a value list — keep walking.
                    i += 1;
                }
            }
            _ => { i += 1; }
        }
    }
    let tail = &s[start..];
    if !tail.trim().is_empty() {
        parts.push(tail.to_string());
    }
    parts
}

/// True if `tail` begins (after optional whitespace) with
/// either:
/// - an identifier followed by ` in ` (single-var clause), or
/// - a `(<ident>, <ident>, ...)` group followed by ` in `
///   (parallel-iter clause, SRD-18c Layer 7a).
///
/// Used by [`split_respecting_parens`] to recognise a clause
/// boundary.
fn is_clause_boundary(tail: &str) -> bool {
    let trimmed = tail.trim_start();
    if trimmed.starts_with('(') {
        // Parallel-iter LHS: walk to matching close-paren.
        let bytes = trimmed.as_bytes();
        let mut depth: i32 = 0;
        for (i, &b) in bytes.iter().enumerate() {
            match b {
                b'(' => depth += 1,
                b')' => {
                    depth -= 1;
                    if depth == 0 {
                        let after = &trimmed[i + 1..];
                        return after.starts_with(" in ");
                    }
                }
                _ => {}
            }
        }
        return false;
    }
    let mut ident_end = 0;
    for (i, c) in trimmed.char_indices() {
        if i == 0 {
            if !(c.is_ascii_alphabetic() || c == '_') { return false; }
        } else if !(c.is_ascii_alphanumeric() || c == '_') {
            ident_end = i;
            break;
        }
        ident_end = i + c.len_utf8();
    }
    if ident_end == 0 { return false; }
    let after = &trimmed[ident_end..];
    after.starts_with(" in ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_single_clause() {
        let c = parse_clause("k in {k_values}").unwrap();
        assert_eq!(c.var(), "k");
        assert_eq!(c.expr(), "{k_values}");
    }

    #[test]
    fn parse_clause_rejects_malformed() {
        let err = parse_clause("not a clause").unwrap_err();
        assert!(err.contains("invalid for_each clause"));
    }

    #[test]
    fn split_respects_parens_in_function_call() {
        // The inner `'{dataset}', '{prefix}'` commas live inside
        // the matching_profiles() call — must not split.
        let s = "profile in matching_profiles('{dataset}', '{prefix}')";
        let parts = split_respecting_parens(s);
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0], s);
    }

    #[test]
    fn split_respects_inner_value_list_commas() {
        // The `10,20,30` commas form a value list, not new
        // clauses — there's no `<ident> in ` after them.
        let s = "k in 10, limit in 10,20,30";
        let parts = split_respecting_parens(s);
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0], "k in 10");
        assert_eq!(parts[1].trim(), "limit in 10,20,30");
    }

    #[test]
    fn split_handles_multiple_real_boundaries() {
        let s = "a in 1, b in 2, c in 3";
        let parts = split_respecting_parens(s);
        assert_eq!(parts.len(), 3);
    }

    #[test]
    fn parse_clause_list_distinct_names() {
        let clauses = parse_clause_list("k in {k_values}, limit in {k_{k}_limits}").unwrap();
        assert_eq!(clauses.len(), 2);
        assert_eq!(clauses[0].var(), "k");
        assert_eq!(clauses[1].var(), "limit");
    }

    #[test]
    fn parse_clause_list_paren_safe() {
        // The function-call inner comma is preserved; only one clause.
        let clauses = parse_clause_list(
            "profile in matching_profiles('{dataset}', '{prefix}')"
        ).unwrap();
        assert_eq!(clauses.len(), 1);
        assert_eq!(clauses[0].var(), "profile");
        assert_eq!(clauses[0].expr(),
            "matching_profiles('{dataset}', '{prefix}')");
    }

    #[test]
    fn comprehension_from_subspaces_distinct_names_flattens_to_cartesian() {
        // Each clause in its own sub-space, distinct names
        // ⇒ flatten into one Cartesian list. This is the
        // string-form path: `"k in 10, limit in 20"` yields
        // `[[(k, 10)], [(limit, 20)]]` from the parser, and
        // here we collapse to `Cartesian([(k,10),(limit,20)])`.
        let subspaces = vec![
            vec![Clause::new("k", "10")],
            vec![Clause::new("limit", "20")],
        ];
        let c = comprehension_from_subspaces(subspaces);
        assert!(c.is_cartesian());
        assert_eq!(c.coordinate_names(), vec!["k", "limit"]);
        assert_eq!(c.flat_clauses().len(), 2);
    }

    #[test]
    fn comprehension_from_subspaces_repeated_names_yields_union() {
        // `k` appears in both sub-spaces ⇒ Union (preserves
        // sub-space boundaries).
        let subspaces = vec![
            vec![Clause::new("k", "10"), Clause::new("limit", "10,20,30")],
            vec![Clause::new("k", "100"), Clause::new("limit", "100,200,300")],
        ];
        let c = comprehension_from_subspaces(subspaces);
        assert!(c.is_union());
        // Union dedups names for the operator-visible set.
        assert_eq!(c.coordinate_names(), vec!["k", "limit"]);
        // Flat clause count preserves repetition.
        assert_eq!(c.flat_clauses().len(), 4);
    }

    #[test]
    fn split_at_where_simple() {
        let (clauses, filter) = split_at_where("k in 10,100 where k > 5");
        assert_eq!(clauses, "k in 10,100");
        assert_eq!(filter, Some("k > 5".to_string()));
    }

    #[test]
    fn split_at_where_no_predicate() {
        let (clauses, filter) = split_at_where("k in 10,100, limit in 10,20,30");
        assert_eq!(clauses, "k in 10,100, limit in 10,20,30");
        assert_eq!(filter, None);
    }

    #[test]
    fn split_at_where_inside_parens_is_ignored() {
        // `where` inside a function call shouldn't split.
        let (clauses, filter) = split_at_where(
            "p in pick(profiles, where='ann') where p == 'x'"
        );
        assert_eq!(clauses, "p in pick(profiles, where='ann')");
        assert_eq!(filter, Some("p == 'x'".to_string()));
    }

    #[test]
    fn parse_comprehension_text_no_filter() {
        let comp = parse_comprehension_text("k in 10,100, limit in 10,20,30").unwrap();
        assert!(comp.is_cartesian());
        assert_eq!(comp.coordinate_names(), vec!["k", "limit"]);
        assert_eq!(comp.filter, None);
    }

    #[test]
    fn parse_comprehension_text_with_filter() {
        let comp = parse_comprehension_text(
            "k in 10,100, limit in 10,20,30 where k * limit < 1000"
        ).unwrap();
        assert!(comp.is_cartesian());
        assert_eq!(comp.coordinate_names(), vec!["k", "limit"]);
        assert_eq!(comp.filter, Some("k * limit < 1000".to_string()));
    }

    #[test]
    fn parse_comprehension_text_repeated_var_yields_union_with_filter() {
        let comp = parse_comprehension_text(
            "k in 1, k in 2 where k > 0"
        ).unwrap();
        assert!(comp.is_union());
        assert_eq!(comp.filter, Some("k > 0".to_string()));
    }

    #[test]
    fn split_at_order_simple() {
        let (head, order) = split_at_order("k in 1..10 order extrema/1");
        assert_eq!(head, "k in 1..10");
        assert_eq!(order, Some("extrema/1".to_string()));
    }

    #[test]
    fn split_at_order_no_order() {
        let (head, order) = split_at_order("k in 1..10 where {k} > 5");
        assert_eq!(head, "k in 1..10 where {k} > 5");
        assert_eq!(order, None);
    }

    #[test]
    fn split_at_order_inside_parens_is_ignored() {
        let (head, order) = split_at_order(
            "p in pick(profiles, order='ann') order lex"
        );
        assert_eq!(head, "p in pick(profiles, order='ann')");
        assert_eq!(order, Some("lex".to_string()));
    }

    #[test]
    fn parse_order_spec_bare() {
        match parse_order_spec("lex").unwrap() {
            TraversalOrder::Lex { count: None } => {}
            other => panic!("expected Lex, got {other:?}"),
        }
        match parse_order_spec("extrema").unwrap() {
            TraversalOrder::Extrema { strata: None } => {}
            other => panic!("expected Extrema, got {other:?}"),
        }
        match parse_order_spec("shells").unwrap() {
            TraversalOrder::Shells { origin: ShellOrigin::Outer, depth: None } => {}
            other => panic!("expected Shells outer/None, got {other:?}"),
        }
    }

    #[test]
    fn parse_order_spec_terse() {
        match parse_order_spec("extrema/1").unwrap() {
            TraversalOrder::Extrema { strata: Some(1) } => {}
            other => panic!("expected Extrema strata=1, got {other:?}"),
        }
        match parse_order_spec("shells/2").unwrap() {
            TraversalOrder::Shells { origin: ShellOrigin::Outer, depth: Some(2) } => {}
            other => panic!("expected Shells outer/2, got {other:?}"),
        }
        match parse_order_spec("halton/64").unwrap() {
            TraversalOrder::Halton { count: Some(64) } => {}
            other => panic!("expected Halton count=64, got {other:?}"),
        }
        match parse_order_spec("lex/100").unwrap() {
            TraversalOrder::Lex { count: Some(100) } => {}
            other => panic!("expected Lex count=100, got {other:?}"),
        }
    }

    #[test]
    fn parse_order_spec_keyword() {
        match parse_order_spec("shells(origin=center, depth=3)").unwrap() {
            TraversalOrder::Shells { origin: ShellOrigin::Center, depth: Some(3) } => {}
            other => panic!("expected Shells center/3, got {other:?}"),
        }
        match parse_order_spec("lhs(count=20, seed=42)").unwrap() {
            TraversalOrder::Lhs { count: Some(20), seed: Some(42) } => {}
            other => panic!("expected Lhs count=20 seed=42, got {other:?}"),
        }
        match parse_order_spec("space_filling(sobol, count=64)").unwrap() {
            TraversalOrder::Sobol { count: Some(64) } => {}
            other => panic!("expected Sobol count=64, got {other:?}"),
        }
    }

    #[test]
    fn parse_order_spec_unknown_strategy_errors() {
        let err = parse_order_spec("zigzag").unwrap_err();
        assert!(err.contains("unknown strategy"), "got: {err}");
    }

    #[test]
    fn parse_comprehension_text_with_order() {
        let comp = parse_comprehension_text(
            "k in 1..10 where {k} > 3 order extrema/1"
        ).unwrap();
        assert_eq!(comp.filter, Some("{k} > 3".to_string()));
        match comp.order {
            Some(TraversalOrder::Extrema { strata: Some(1) }) => {}
            other => panic!("expected Extrema strata=1, got {other:?}"),
        }
    }

    #[test]
    fn parse_comprehension_text_order_only() {
        let comp = parse_comprehension_text(
            "k in 1..10, l in 1..10 order halton/50"
        ).unwrap();
        assert_eq!(comp.filter, None);
        assert!(matches!(comp.order, Some(TraversalOrder::Halton { count: Some(50) })));
    }

    #[test]
    fn comprehension_from_subspaces_string_form_repeated_var_yields_union() {
        // The string form `"k in 1, k in 2"` is one
        // top-level clause list with a repeated var name —
        // each clause becomes its own sub-space, then the
        // detection rule sees the repetition.
        let subspaces = vec![
            vec![Clause::new("k", "1")],
            vec![Clause::new("k", "2")],
        ];
        let c = comprehension_from_subspaces(subspaces);
        assert!(c.is_union());
        assert_eq!(c.coordinate_names(), vec!["k"]);
    }

    // ── SRD-18e Push 10: Union + non-lex ordering rejection ──

    #[test]
    fn union_plus_extrema_is_rejected() {
        let err = parse_comprehension_text(
            "k in 10, k in 100 order extrema/1"
        ).unwrap_err();
        assert!(err.contains("'extrema'") && err.contains("Union"),
            "wrong message: {err}");
        assert!(err.contains("Cartesian") || err.contains("lex"),
            "should hint at remedy: {err}");
    }

    #[test]
    fn union_plus_halton_is_rejected() {
        let err = parse_comprehension_text(
            "k in 10, l in 100, k in 200, l in 400 order halton/64"
        ).unwrap_err();
        assert!(err.contains("'halton'") && err.contains("Union"), "{err}");
    }

    #[test]
    fn union_plus_shells_is_rejected() {
        let err = parse_comprehension_text(
            "k in 10, k in 100 order shells/2"
        ).unwrap_err();
        assert!(err.contains("'shells'") && err.contains("Union"), "{err}");
    }

    #[test]
    fn union_plus_lex_is_accepted() {
        // lex is a stable enumeration order with no
        // geometric reasoning — works fine on Union.
        let comp = parse_comprehension_text(
            "k in 10, k in 100 order lex"
        ).unwrap();
        assert!(comp.is_union());
        assert!(matches!(comp.order, Some(TraversalOrder::Lex { count: None })));
    }

    #[test]
    fn union_plus_custom_is_accepted() {
        // custom is the escape hatch — the user's function
        // decides what ordering means for their Union shape.
        let comp = parse_comprehension_text(
            "k in 10, k in 100 order custom(my_fn)"
        ).unwrap();
        assert!(comp.is_union());
        assert!(matches!(comp.order, Some(TraversalOrder::Custom { .. })));
    }

    #[test]
    fn cartesian_plus_extrema_remains_valid() {
        // The rejection is Union-specific; Cartesian +
        // index-space orderings have always been valid.
        let comp = parse_comprehension_text(
            "k in 1..10, l in 1..10 order extrema/1"
        ).unwrap();
        assert!(comp.is_cartesian());
        assert!(matches!(comp.order, Some(TraversalOrder::Extrema { strata: Some(1) })));
    }

    #[test]
    fn validate_rejects_each_index_space_strategy_on_union() {
        // Routes through Comprehension::validate (the canonical
        // invariant entry point) — verifies every named
        // index-space strategy is named in the error.
        for (label, ord) in [
            ("reverse_lex",  TraversalOrder::ReverseLex   { count: None }),
            ("diagonal",     TraversalOrder::Diagonal     { count: None }),
            ("antidiagonal", TraversalOrder::Antidiagonal { count: None }),
            ("extrema",      TraversalOrder::Extrema      { strata: None }),
            ("shells",       TraversalOrder::Shells {
                origin: ShellOrigin::Outer, depth: None }),
            ("halton",       TraversalOrder::Halton       { count: None }),
            ("sobol",        TraversalOrder::Sobol        { count: None }),
            ("lhs",          TraversalOrder::Lhs          { count: None, seed: None }),
        ] {
            let comp = Comprehension::union(vec![
                vec![Clause::new("k", "10")],
                vec![Clause::new("k", "20")],
            ]).with_order(ord);
            let errs = comp.validate().unwrap_err();
            assert!(errs.iter().any(|e| e.contains(label)),
                "{label}: error should name the strategy: {errs:?}");
        }
    }

    // ---- Push 2: Layer 7a parallel-iter clauses --------------

    #[test]
    fn parse_clause_parallel_two_vars() {
        let c = parse_clause("(x, y) in (1..10, 100..1000..100)").unwrap();
        assert!(c.is_parallel());
        assert_eq!(c.vars, vec!["x".to_string(), "y".to_string()]);
        match &c.source {
            super::super::ast::ClauseSource::Parallel { exprs, .. } => {
                assert_eq!(exprs, &vec!["1..10".to_string(), "100..1000..100".to_string()]);
            }
            _ => panic!("expected Parallel source"),
        }
    }

    #[test]
    fn parse_clause_parallel_three_vars() {
        let c = parse_clause("(a, b, c) in (1..3, 10..30..10, 100..300..100)").unwrap();
        assert!(c.is_parallel());
        assert_eq!(c.vars, vec!["a".to_string(), "b".to_string(), "c".to_string()]);
    }

    #[test]
    fn parse_clause_parallel_with_function_call_rhs() {
        // Nested parens inside the parallel-RHS group must not
        // confuse the splitter — the comma between fib(8) and
        // pow2(8) is at depth 1.
        let c = parse_clause("(x, y) in (fib(8), pow2(8))").unwrap();
        assert!(c.is_parallel());
        match &c.source {
            super::super::ast::ClauseSource::Parallel { exprs, .. } => {
                assert_eq!(exprs, &vec!["fib(8)".to_string(), "pow2(8)".to_string()]);
            }
            _ => panic!("expected Parallel source"),
        }
    }

    #[test]
    fn parse_clause_paren_only_one_side_is_rejected() {
        let err = parse_clause("(x, y) in 1..10").unwrap_err();
        assert!(err.contains("parentheses on both sides"), "got: {err}");
    }

    #[test]
    fn parse_clause_parallel_count_mismatch_is_rejected() {
        let err = parse_clause("(x, y, z) in (1..10, 1..20)").unwrap_err();
        assert!(err.contains("3 variables but 2 expressions"), "got: {err}");
    }

    #[test]
    fn parse_clause_parallel_single_var_is_rejected() {
        // `(x) in (1..10)` is malformed parallel-iter (≥ 2 vars
        // required); use the single-var form instead.
        let err = parse_clause("(x) in (1..10)").unwrap_err();
        assert!(err.contains("≥ 2 variables"), "got: {err}");
    }

    #[test]
    fn split_respects_parallel_clause_boundaries() {
        // A parallel-iter clause followed by a single-var clause
        // — the boundary detector must accept `(<ident>, ...) in `
        // as the start of a new clause.
        let s = "(x, y) in (1..2, 10..20..10), z in 100..200..100";
        let parts = split_respecting_parens(s);
        assert_eq!(parts.len(), 2);
    }

    #[test]
    fn parse_clause_list_mixed_parallel_and_single() {
        let clauses = parse_clause_list(
            "(x, y) in (1..2, 10..20..10), z in 100..200..100"
        ).unwrap();
        assert_eq!(clauses.len(), 2);
        assert!(clauses[0].is_parallel());
        assert!(!clauses[1].is_parallel());
        assert_eq!(clauses[1].var(), "z");
    }

    #[test]
    fn parse_clause_parallel_invalid_var_name_is_rejected() {
        let err = parse_clause("(x, 9b) in (1..10, 1..10)").unwrap_err();
        assert!(err.contains("not a valid variable name"), "got: {err}");
    }

    // ---- Round-trip: Display → parse → equal AST ------------

    fn roundtrip_clause(c: Clause) {
        let text = c.to_string();
        let reparsed = parse_clause(&text)
            .unwrap_or_else(|e| panic!("re-parse failed for '{text}': {e}"));
        assert_eq!(c, reparsed,
            "round-trip diverged: original={c:?}\n  text='{text}'\n  reparsed={reparsed:?}");
    }

    #[test]
    fn round_trip_single_var_clause() {
        roundtrip_clause(Clause::new("k", "1..10"));
        roundtrip_clause(Clause::new("limit", "fib(8)"));
    }

    #[test]
    fn round_trip_parallel_strict() {
        use super::super::ast::ZipMode;
        roundtrip_clause(Clause::parallel(["x", "y"], ["fib(8)", "pow2(8)"]));
        roundtrip_clause(Clause::parallel_with_mode(
            ZipMode::Strict, ["a", "b", "c"], ["1..3", "10..30..10", "100..300..100"]
        ));
    }

    #[test]
    fn round_trip_parallel_truncate_and_cycle() {
        use super::super::ast::ZipMode;
        roundtrip_clause(Clause::parallel_with_mode(
            ZipMode::Truncate, ["x", "y"], ["fib(8)", "pow2(4)"]
        ));
        roundtrip_clause(Clause::parallel_with_mode(
            ZipMode::Cycle, ["x", "y"], ["fib(4)", "pow2(8)"]
        ));
    }

    fn roundtrip_comprehension_text(text: &str) {
        let parsed = parse_comprehension_text(text)
            .unwrap_or_else(|e| panic!("parse failed for '{text}': {e}"));
        let rendered = parsed.to_string();
        let reparsed = parse_comprehension_text(&rendered)
            .unwrap_or_else(|e| panic!("re-parse failed for '{rendered}' (from '{text}'): {e}"));
        assert_eq!(parsed, reparsed,
            "round-trip diverged for '{text}':\n  rendered='{rendered}'");
    }

    #[test]
    fn round_trip_cartesian_comprehension() {
        roundtrip_comprehension_text("k in 1..10");
        roundtrip_comprehension_text("k in 1..10, limit in fib(8)");
        roundtrip_comprehension_text("k in 1..10 where {k} > 3");
        roundtrip_comprehension_text("k in 1..10 order extrema/2");
        roundtrip_comprehension_text("k in 1..10, l in 1..10 where {k} != {l} order extrema/1");
    }

    #[test]
    fn round_trip_parallel_iter_through_full_comprehension_text() {
        roundtrip_comprehension_text("(x, y) in (fib(5), pow2(5))");
        roundtrip_comprehension_text("(x, y) in zip_truncate(fib(8), pow2(4))");
        roundtrip_comprehension_text(
            "(x, y) in (fib(4), pow2(4)), z in 1..3 order extrema/1"
        );
    }

    #[test]
    fn parse_clause_parallel_zip_truncate_mode() {
        use super::super::ast::{ClauseSource, ZipMode};
        let c = parse_clause("(x, y) in zip_truncate(1..10, fib(8))").unwrap();
        match &c.source {
            ClauseSource::Parallel { mode, exprs } => {
                assert_eq!(*mode, ZipMode::Truncate);
                assert_eq!(exprs, &vec!["1..10".to_string(), "fib(8)".to_string()]);
            }
            _ => panic!("expected Parallel source, got {:?}", c.source),
        }
    }

    #[test]
    fn parse_clause_parallel_zip_cycle_mode() {
        use super::super::ast::{ClauseSource, ZipMode};
        let c = parse_clause("(x, y) in zip_cycle(1..10, 100..1000..100)").unwrap();
        match &c.source {
            ClauseSource::Parallel { mode, .. } => {
                assert_eq!(*mode, ZipMode::Cycle);
            }
            _ => panic!("expected Parallel source"),
        }
    }

    #[test]
    fn parse_clause_parallel_default_mode_is_strict() {
        use super::super::ast::{ClauseSource, ZipMode};
        let c = parse_clause("(x, y) in (1..10, 100..1000..100)").unwrap();
        match &c.source {
            ClauseSource::Parallel { mode, .. } => {
                assert_eq!(*mode, ZipMode::Strict);
            }
            _ => panic!("expected Parallel source"),
        }
    }
}
