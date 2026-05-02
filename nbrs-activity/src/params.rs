// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Workload parameters as a GK module.
//!
//! Workload params arrive as `(name → string)` pairs from
//! the YAML `params:` block plus any CLI overrides. Rather
//! than text-substituting their values into op bindings or
//! patching them as folded constants on every kernel that
//! happens to need them, we compile them once into a
//! standalone GK kernel — the **workload-params kernel** —
//! which sits at the root of the scope chain. Every kernel
//! built downstream (workload-level bindings, phase ops,
//! comprehensions, leaf phases) `bind_outer_scope`s through
//! it, so `{name}` references in any descendant resolve via
//! standard GK name resolution.
//!
//! ## Why a kernel instead of a string-substitution pass
//!
//! Text replacement of `{name}` placeholders into op binding
//! sources is fundamentally ambiguous: a placeholder can sit
//! inside a string literal (`"{dataset}:{profile}"`) where
//! it's GK string-interpolation, or as a standalone expression
//! where it's an identifier reference. A blind text pass
//! rewrites both, double-quotes the string-literal cases, and
//! produces broken GK source.
//!
//! The params-kernel approach is unambiguous — `final name :=
//! <literal>` is just a normal GK binding. GK's parser knows
//! string-interpolation from identifier reference; both
//! resolve correctly.
//!
//! ## Type detection
//!
//! Since workload params arrive as strings, we infer GK types
//! the same way the legacy [`crate::scope::format_workload_param_as_gk_literal`]
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

use nbrs_variates::dsl::compile::compile_gk;
use nbrs_variates::kernel::GkKernel;

/// Build the workload-params kernel from a params map. The
/// resulting kernel exposes one `final <name> := <literal>`
/// binding per param; consumers `bind_outer_scope` to it to
/// inherit every workload param at once.
///
/// Empty params produces a kernel with a single
/// `final __empty := 0` placeholder so descendant scopes can
/// always `bind_outer_scope` to it without a "no kernel"
/// special case.
pub fn build_workload_params_kernel(
    params: &HashMap<String, String>,
) -> Result<GkKernel, String> {
    let source = render_workload_params_source(params);
    compile_gk(&source).map_err(|e| format!(
        "workload params kernel: {e}\n--- generated source ---\n{source}"
    ))
}

/// Render the GK source for the workload-params kernel. Public
/// so tests and diagnostics can inspect the synthesized module
/// without compiling it.
pub fn render_workload_params_source(
    params: &HashMap<String, String>,
) -> String {
    if params.is_empty() {
        return "final __empty := 0\n".to_string();
    }
    // Sort by name so the generated source is deterministic
    // across runs — matters for cache keys, diagnostic output,
    // and golden-output tests.
    let mut keys: Vec<&String> = params.keys().collect();
    keys.sort();
    let mut out = String::new();
    for name in keys {
        let value = &params[name];
        let literal = format_value_as_gk_literal(value);
        out.push_str(&format!("final {name} := {literal}\n"));
    }
    out
}

/// Format a workload-param string as a GK literal, detecting
/// the natural type. Numbers and booleans pass through; any
/// other string becomes a quoted string literal with embedded
/// quotes / backslashes properly escaped.
///
/// Mirrors `crate::scope::format_workload_param_as_gk_literal`
/// — kept private here for the params-kernel path so this
/// module is self-contained and the legacy text-substitution
/// pass can eventually be retired without affecting it.
fn format_value_as_gk_literal(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.parse::<u64>().is_ok() {
        return trimmed.to_string();
    }
    if trimmed.parse::<f64>().is_ok() {
        return trimmed.to_string();
    }
    if trimmed == "true" || trimmed == "false" {
        return trimmed.to_string();
    }
    // Embed as a quoted string. Escape any embedded backslash
    // and quote so the GK source remains parsable. The original
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
        assert_eq!(src, "final __empty := 0\n");
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
        let expected = "final count := 100\n\
                        final dataset := \"sift1m\"\n\
                        final k_values := \"1,10\"\n\
                        final ratio := 0.95\n\
                        final strict := true\n";
        assert_eq!(src, expected);
    }

    #[test]
    fn escapes_quotes_in_string_values() {
        let src = render_workload_params_source(&h(&[
            ("replication", r#"{'class': 'SimpleStrategy'}"#),
            ("with_quote", r#"a"b"#),
        ]));
        assert!(src.contains(r#"final replication := "{'class': 'SimpleStrategy'}""#),
            "unexpected: {src}");
        assert!(src.contains(r#"final with_quote := "a\"b""#),
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
        // Both params are reachable as GK constants.
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
        // bind_outer_scope-eligible. We don't assert the
        // placeholder is queryable since callers should
        // ignore it.
        let _ = kernel;
    }

    #[test]
    fn boolean_values_emit_unquoted_literals() {
        // GK's bool literal handling lives in the DSL compiler;
        // here we just verify the source we emit matches the
        // legacy `format_workload_param_as_gk_literal`
        // convention. Names are arbitrary — `flag_t` and
        // `flag_f` are placeholders chosen to make the
        // true/false correspondence obvious in the assertion.
        let src = render_workload_params_source(&h(&[
            ("flag_t", "true"),
            ("flag_f", "false"),
        ]));
        assert!(src.contains("final flag_f := false\n"));
        assert!(src.contains("final flag_t := true\n"));
    }
}
