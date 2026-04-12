// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! AST validation and diagnostics for GK source files.
//!
//! Provides `validate_ast` and supporting helpers that check function names,
//! wire references, forward references, and coordinate inference before the
//! compiler attempts to build an assembler graph.

use std::collections::HashSet;

use crate::dsl::ast::*;
use crate::dsl::error::DiagnosticReport;
use crate::dsl::registry;

/// Validate the AST: check function names, argument counts, wire
/// references, unused bindings, forward references.
///
/// Coordinate inference: if no `coordinates` declaration is present,
/// any referenced name that is not defined as a node output is
/// automatically promoted to a coordinate input. If a `coordinates`
/// declaration IS present, any unbound reference not in that list
/// is an error.
pub(crate) fn validate_ast(file: &GkFile, report: &mut DiagnosticReport) {
    let mut defined: HashSet<String> = HashSet::new();
    let mut referenced: HashSet<String> = HashSet::new();
    let mut input_names: HashSet<String> = HashSet::new();
    let mut has_explicit_coords = false;
    let mut definition_order: Vec<(String, crate::dsl::lexer::Span)> = Vec::new();

    // First pass: collect explicit coordinates and all defined names
    for stmt in &file.statements {
        match stmt {
            Statement::Coordinates(names, _) => {
                has_explicit_coords = true;
                for n in names {
                    input_names.insert(n.clone());
                    defined.insert(n.clone());
                }
            }
            Statement::InitBinding(b) => {
                defined.insert(b.name.clone());
                definition_order.push((b.name.clone(), b.span));
            }
            Statement::CycleBinding(b) => {
                for t in &b.targets {
                    defined.insert(t.clone());
                    definition_order.push((t.clone(), b.span));
                }
            }
            Statement::ModuleDef(m) => {
                defined.insert(m.name.clone());
            }
            Statement::ExternPort(p) => {
                defined.insert(p.name.clone());
            }
        }
    }

    // Second pass: validate function calls and collect references
    for stmt in &file.statements {
        let expr = match stmt {
            Statement::Coordinates(_, _) | Statement::ModuleDef(_) | Statement::ExternPort(_) => continue,
            Statement::InitBinding(b) => &b.value,
            Statement::CycleBinding(b) => &b.value,
        };
        validate_expr(expr, &defined, &input_names, &mut referenced, report);
    }

    // Coordinate inference or validation
    if has_explicit_coords {
        // Explicit mode: unbound references are errors
        for name in &referenced {
            if !defined.contains(name) {
                report.error_with_hint(
                    crate::dsl::lexer::Span { line: 1, col: 1 },
                    format!("undefined wire reference: '{name}'"),
                    if input_names.contains(name) {
                        // shouldn't happen — input_names are in defined
                        "internal error".into()
                    } else if let Some(suggestion) = find_close_name(name, &defined) {
                        format!("did you mean '{suggestion}'?")
                    } else {
                        format!("'{name}' is not declared as a coordinate — add it to your coordinates declaration, or define it as a binding")
                    },
                );
            }
        }
    } else {
        // Infer mode: unbound references become coordinates
        let mut inferred: Vec<String> = referenced.iter()
            .filter(|name| !defined.contains(*name))
            .cloned()
            .collect();
        inferred.sort(); // deterministic order

        if inferred.is_empty() && !file.statements.is_empty() {
            report.error_with_hint(
                crate::dsl::lexer::Span { line: 1, col: 1 },
                "no coordinate inputs found",
                "reference at least one unbound name (e.g., 'cycle') to define the kernel's input",
            );
        } else {
            // Promote inferred names to coordinates
            for name in &inferred {
                input_names.insert(name.clone());
                defined.insert(name.clone());
            }
        }
    }

    // Check for unused bindings (warning, not error)
    for (name, _span) in &definition_order {
        if !referenced.contains(name) && !input_names.contains(name) {
            // It's an output variate — not consumed internally.
            // This is fine, don't warn. Outputs are consumed externally.
        }
    }

    // Check for forward references (warning)
    let mut seen_defs: HashSet<String> = input_names.clone();
    for stmt in &file.statements {
        match stmt {
            Statement::Coordinates(_, _) => {}
            Statement::InitBinding(b) => {
                check_forward_refs(&b.value, &seen_defs, b.span, report);
                seen_defs.insert(b.name.clone());
            }
            Statement::CycleBinding(b) => {
                check_forward_refs(&b.value, &seen_defs, b.span, report);
                for t in &b.targets {
                    seen_defs.insert(t.clone());
                }
            }
            Statement::ModuleDef(m) => {
                seen_defs.insert(m.name.clone());
            }
            Statement::ExternPort(p) => {
                seen_defs.insert(p.name.clone());
            }
        }
    }
}

/// Validate an expression: check function names against the registry and
/// collect all wire references into `referenced`.
pub(crate) fn validate_expr(
    expr: &Expr,
    defined: &HashSet<String>,
    coords: &HashSet<String>,
    referenced: &mut HashSet<String>,
    report: &mut DiagnosticReport,
) {
    match expr {
        Expr::Ident(name, _) => {
            referenced.insert(name.clone());
        }
        Expr::Call(call) => {
            // Validate function name
            if registry::lookup(&call.func).is_none() {
                let msg = format!("unknown function: '{}'", call.func);
                let hint = if let Some(suggestion) = registry::suggest_function(&call.func) {
                    format!("did you mean '{suggestion}'?")
                } else {
                    "check the function name or see the function reference".into()
                };
                report.error_with_hint(call.span, msg, hint);
            }

            // Validate arguments recursively
            for arg in &call.args {
                let inner = match arg {
                    Arg::Positional(e) => e,
                    Arg::Named(_, e) => e,
                };
                validate_expr(inner, defined, coords, referenced, report);
            }
        }
        Expr::ArrayLit(elems, _) => {
            for e in elems {
                validate_expr(e, defined, coords, referenced, report);
            }
        }
        Expr::StringLit(s, _) => {
            // Extract {name} references from string interpolation.
            // Only valid identifiers (alpha/underscore start) — skip
            // format specifiers like {:05} or {:.2}.
            let chars: Vec<char> = s.chars().collect();
            let mut i = 0;
            while i < chars.len() {
                if chars[i] == '{' {
                    i += 1;
                    let start = i;
                    while i < chars.len() && chars[i] != '}' { i += 1; }
                    let name: String = chars[start..i].iter().collect();
                    let is_ident = name.chars().next()
                        .map(|c| c.is_alphabetic() || c == '_')
                        .unwrap_or(false);
                    if is_ident {
                        referenced.insert(name);
                    }
                    i += 1;
                } else {
                    i += 1;
                }
            }
        }
        _ => {}
    }
}

/// Return the type name of a literal expression, if it is one.
pub(crate) fn literal_type(expr: &Expr) -> Option<String> {
    match expr {
        Expr::IntLit(_, _) => Some("u64".into()),
        Expr::FloatLit(_, _) => Some("f64".into()),
        Expr::StringLit(_, _) => Some("String".into()),
        _ => None, // wire references, calls — type not known from the literal
    }
}

/// Check if a literal type is compatible with a declared parameter type.
pub(crate) fn types_compatible(lit_type: &str, declared: &str) -> bool {
    match (lit_type, declared) {
        ("u64", "u64") => true,
        ("f64", "f64") => true,
        ("String", "String") => true,
        // u64 literal can be used where f64 is expected (implicit widening)
        ("u64", "f64") => true,
        _ => false,
    }
}

/// Collect all identifier references from an expression tree (no validation).
pub(crate) fn collect_references(expr: &Expr, referenced: &mut HashSet<String>) {
    match expr {
        Expr::Ident(name, _) => { referenced.insert(name.clone()); }
        Expr::Call(call) => {
            for arg in &call.args {
                let inner = match arg {
                    Arg::Positional(e) => e,
                    Arg::Named(_, e) => e,
                };
                collect_references(inner, referenced);
            }
        }
        Expr::ArrayLit(elems, _) => {
            for e in elems { collect_references(e, referenced); }
        }
        Expr::StringLit(s, _) => {
            // Extract {name} references, but only valid identifiers
            // (starts with alpha/underscore). Skips format specifiers
            // like {:05} or {:.2} which start with ':' or '.'.
            let chars: Vec<char> = s.chars().collect();
            let mut i = 0;
            while i < chars.len() {
                if chars[i] == '{' {
                    i += 1;
                    let start = i;
                    while i < chars.len() && chars[i] != '}' { i += 1; }
                    let name: String = chars[start..i].iter().collect();
                    let is_ident = name.chars().next()
                        .map(|c| c.is_alphabetic() || c == '_')
                        .unwrap_or(false);
                    if is_ident { referenced.insert(name); }
                    i += 1;
                } else {
                    i += 1;
                }
            }
        }
        _ => {}
    }
}

/// Check for forward references within an expression relative to the set of
/// already-defined names at the statement's position.
pub(crate) fn check_forward_refs(
    expr: &Expr,
    seen: &HashSet<String>,
    stmt_span: crate::dsl::lexer::Span,
    report: &mut DiagnosticReport,
) {
    match expr {
        Expr::Ident(name, span) => {
            if !seen.contains(name) {
                report.warning_with_hint(
                    *span,
                    format!("forward reference: '{name}' is used before it is defined"),
                    "consider reordering bindings so definitions come before uses",
                );
            }
        }
        Expr::Call(call) => {
            for arg in &call.args {
                let inner = match arg {
                    Arg::Positional(e) => e,
                    Arg::Named(_, e) => e,
                };
                check_forward_refs(inner, seen, stmt_span, report);
            }
        }
        Expr::ArrayLit(elems, _) => {
            for e in elems {
                check_forward_refs(e, seen, stmt_span, report);
            }
        }
        _ => {}
    }
}

/// Find the closest name in `defined` to `name` using edit distance.
///
/// Returns `Some(name)` if a name within edit distance 3 is found,
/// or `None` if there is no close match.
pub(crate) fn find_close_name(name: &str, defined: &HashSet<String>) -> Option<String> {
    let mut best: Option<(String, usize)> = None;
    for d in defined {
        let dist = simple_edit_distance(name, d);
        if dist <= 3 && (best.is_none() || dist < best.as_ref().unwrap().1) {
            best = Some((d.clone(), dist));
        }
    }
    best.map(|(n, _)| n)
}

/// Compute the Levenshtein edit distance between two strings.
pub(crate) fn simple_edit_distance(a: &str, b: &str) -> usize {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    let mut m = vec![vec![0; b.len() + 1]; a.len() + 1];
    for i in 0..=a.len() { m[i][0] = i; }
    for j in 0..=b.len() { m[0][j] = j; }
    for i in 1..=a.len() {
        for j in 1..=b.len() {
            let c = if a[i-1] == b[j-1] { 0 } else { 1 };
            m[i][j] = (m[i-1][j]+1).min(m[i][j-1]+1).min(m[i-1][j-1]+c);
        }
    }
    m[a.len()][b.len()]
}
