// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Workload parameters as a Polydat module.
//!
//! Workload params arrive as `(name → string)` pairs from
//! the YAML `params:` block plus any CLI overrides. Rather
//! than text-substituting their values into op bindings or
//! patching them as folded constants on every kernel that
//! happens to need them, we compile them once into a
//! standalone Polydat Kernel — the **workload-params kernel** —
//! which sits at the root of the scope chain. Every kernel
//! built downstream (workload-level bindings, phase ops,
//! comprehensions, leaf phases) `materialize_wiring_from_outer`s through
//! it, so `{name}` references in any descendant resolve via
//! standard Polydat name resolution.
//!
//! ## Why a kernel instead of a string-substitution pass
//!
//! Text replacement of `{name}` placeholders into op binding
//! sources is fundamentally ambiguous: a placeholder can sit
//! inside a string literal (`"{dataset}:{profile}"`) where
//! it's Polydat string-interpolation, or as a standalone expression
//! where it's an identifier reference. A blind text pass
//! rewrites both, double-quotes the string-literal cases, and
//! produces broken Polydat source.
//!
//! The params-kernel approach is unambiguous — `final name :=
//! <literal>` is just a normal Polydat binding. Polydat's parser knows
//! string-interpolation from identifier reference; both
//! resolve correctly.
//!
//! ## Type detection
//!
//! Since workload params arrive as strings, we infer Polydat types
//! the same way the legacy [`crate::scope::format_workload_param_as_polydat_literal`]
//! does:
//!
//! - Integer-parseable → `u64`
//! - Float-parseable → `f64`
//! - `"true"` / `"false"` → `bool`
//! - Anything else → `String` (with proper quote / escape handling)
//!
//! Native typing matters because descendant-scope synthesis
//! relies on the parent's manifest port types when emitting
//! cascade externs (see SRD-18b §"Cascade externs"). A param
//! presented as `"100"` becomes `u64` in the manifest, not
//! `String`.

use std::collections::HashMap;

use polydat::dsl::compile::compile_polydat;
use polydat::kernel::PolydatKernel;

/// Build the workload-params kernel from a params map. The
/// resulting kernel exposes one `final <name> := <literal>`
/// binding per param; consumers `materialize_wiring_from_outer` to it to
/// inherit every workload param at once.
///
/// Empty params produces a kernel with a single
/// `const __empty := 0` placeholder so descendant scopes can
/// always `materialize_wiring_from_outer` to it without a "no kernel"
/// special case.
pub fn build_workload_params_kernel(
    params: &HashMap<String, String>,
) -> Result<PolydatKernel, String> {
    let source = render_workload_params_source(params);
    compile_polydat(&source).map_err(|e| format!(
        "workload params kernel: {e}\n--- generated source ---\n{source}"
    ))
}

/// Render the Polydat source for the workload-params kernel. Public
/// so tests and diagnostics can inspect the synthesized module
/// without compiling it.
pub fn render_workload_params_source(
    params: &HashMap<String, String>,
) -> String {
    if params.is_empty() {
        return "const __empty := 0\n".to_string();
    }
    // Sort by name so the generated source is deterministic
    // across runs — matters for cache keys, diagnostic output,
    // and golden-output tests.
    let mut keys: Vec<&String> = params.keys().collect();
    keys.sort();
    let mut out = String::new();
    for name in keys {
        let value = &params[name];
        let literal = format_value_as_polydat_literal(value);
        out.push_str(&format!("const {name} := {literal}\n"));
    }
    out
}

/// Format a workload-param string as a Polydat literal, detecting
/// the natural type. Integers parse as `IntLit`, floats as
/// `FloatLit`; everything else is emitted as a quoted string
/// literal so the Polydat lexer always has a token kind to read.
///
/// `true` / `false` are NOT special-cased: Polydat's lexer has no
/// boolean token kind, so a bare `false` would parse as an
/// identifier (wire reference) and fail kernel compilation.
/// Workload params carrying boolean-looking strings are
/// emitted as `"true"` / `"false"`; downstream consumers can
/// `str_eq(x, "true")` if they need a real comparison, and the
/// CQL `WITH OPTIONS` interpolation path already wants string
/// values inside the single-quoted clause.
///
/// Mirrors `crate::scope::format_workload_param_as_polydat_literal`
/// — kept private here for the params-kernel path so this
/// module is self-contained and the legacy text-substitution
/// pass can eventually be retired without affecting it.
fn format_value_as_polydat_literal(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.parse::<u64>().is_ok() {
        return trimmed.to_string();
    }
    if trimmed.parse::<f64>().is_ok() {
        return trimmed.to_string();
    }
    // Embed as a quoted string. Escape any embedded backslash
    // and quote so the Polydat source remains parsable. The original
    // (un-trimmed) value is preserved — leading/trailing space
    // can be meaningful for some param values.
    let escaped = value.replace('\\', "\\\\").replace('"', "\\\"");
    format!("\"{escaped}\"")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn h(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    #[test]
    fn renders_empty_params_as_placeholder() {
        let src = render_workload_params_source(&HashMap::new());
        assert_eq!(src, "const __empty := 0\n");
    }

    #[test]
    fn renders_typed_params_with_native_literals() {
        let src = render_workload_params_source(&h(&[
            ("dataset", "sift1m"),
            ("k_values", "1,10"),
            ("count", "100"),
            ("ratio", "0.95"),
            ("strict", "true"),
        ]));
        // Sorted by name: count, dataset, k_values, ratio, strict.
        // Numbers pass through as bare literals; booleans round-
        // trip as quoted strings because Polydat has no bool token kind
        // (a bare `true` would parse as an identifier).
        let expected = "const count := 100\n\
                        const dataset := \"sift1m\"\n\
                        const k_values := \"1,10\"\n\
                        const ratio := 0.95\n\
                        const strict := \"true\"\n";
        assert_eq!(src, expected);
    }

    #[test]
    fn boolean_strings_compile_as_string_consts() {
        // Regression: workload params like
        //   enable_hierarchy: "false"
        // used to emit `const enable_hierarchy := false`, which
        // Polydat rejected as an unknown wire. They must round-trip as
        // quoted strings so the params-kernel compiles.
        let kernel = build_workload_params_kernel(&h(&[
            ("enable_hierarchy", "false"),
            ("debug_mode", "true"),
        ])).unwrap();
        let eh = kernel.lookup("enable_hierarchy")
            .expect("enable_hierarchy must resolve");
        let dm = kernel.lookup("debug_mode")
            .expect("debug_mode must resolve");
        assert_eq!(eh.to_display_string(), "false");
        assert_eq!(dm.to_display_string(), "true");
    }

    #[test]
    fn escapes_quotes_in_string_values() {
        let src = render_workload_params_source(&h(&[
            ("replication", r#"{'class': 'SimpleStrategy'}"#),
            ("with_quote", r#"a"b"#),
        ]));
        assert!(src.contains(r#"const replication := "{'class': 'SimpleStrategy'}""#),
            "unexpected: {src}");
        assert!(src.contains(r#"const with_quote := "a\"b""#),
            "embedded double-quote not escaped: {src}");
    }

    #[test]
    fn deterministic_ordering_across_runs() {
        let p = h(&[
            ("z_last", "1"), ("a_first", "2"), ("m_middle", "3"),
        ]);
        let s1 = render_workload_params_source(&p);
        let s2 = render_workload_params_source(&p);
        assert_eq!(s1, s2);
        // Names appear alphabetically.
        let a_pos = s1.find("a_first").unwrap();
        let m_pos = s1.find("m_middle").unwrap();
        let z_pos = s1.find("z_last").unwrap();
        assert!(a_pos < m_pos && m_pos < z_pos);
    }

    #[test]
    fn compiles_to_valid_kernel() {
        let kernel = build_workload_params_kernel(&h(&[
            ("dataset", "sift1m"),
            ("count", "100"),
        ])).unwrap();
        // Both params are reachable as Polydat constants.
        let dataset = kernel.lookup("dataset")
            .expect("dataset must resolve");
        let count = kernel.lookup("count")
            .expect("count must resolve");
        assert_eq!(dataset.to_display_string(), "sift1m");
        assert_eq!(count.as_u64(), 100);
    }

    #[test]
    fn compiles_with_no_params_using_placeholder() {
        let kernel = build_workload_params_kernel(&HashMap::new()).unwrap();
        // `__empty` is folded — kernel compiles and is
        // materialize_wiring_from_outer-eligible. We don't assert the
        // placeholder is queryable since callers should
        // ignore it.
        let _ = kernel;
    }

    #[test]
    fn boolean_values_emit_quoted_strings() {
        // Polydat's lexer has no boolean token kind, so bare `true` /
        // `false` would parse as identifiers (wire references)
        // and fail kernel compilation. The formatter therefore
        // emits boolean-looking strings as quoted string
        // literals; downstream consumers that need a real
        // boolean comparison can `str_eq(x, "true")`.
        let src = render_workload_params_source(&h(&[
            ("flag_t", "true"),
            ("flag_f", "false"),
        ]));
        assert!(src.contains("const flag_f := \"false\"\n"));
        assert!(src.contains("const flag_t := \"true\"\n"));
    }
}
