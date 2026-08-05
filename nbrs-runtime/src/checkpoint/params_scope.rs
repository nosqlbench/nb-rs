// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! SRD-107 — the consumed-params derivation.
//!
//! Which workload params does a phase's scope actually consume?
//! The answer is DERIVED, never declared (a declaration surface
//! would drift from the programs and reintroduce stale-skip bugs
//! by hand):
//!
//! - **GK backward closure** — seed with the non-coordinate input
//!   names of the phase's own program and its op-template
//!   programs, then resolve upward through each ancestor scope
//!   program: a name the ancestor outputs is replaced by that
//!   output's own extern slice ([`polydat::kernel::PolydatProgram
//!   ::extern_closure`] — per-output dataflow, so sibling outputs'
//!   params are NOT dragged in); a name it doesn't output stays
//!   unresolved and continues up. Names still unresolved past the
//!   workload root that match declared params are consumed from
//!   the params module. Aliasing through upstream rebindings
//!   (`alias := run_tag` at the root, phase reads `alias`)
//!   resolves correctly with no textual guessing.
//! - **Textual union** — `{name}` interpolation sites in the
//!   phase's canonical config (op statement text, governance
//!   fields like `cycles:`/`timeout:`) never enter compiled
//!   programs; a substring scan over the canonical config text
//!   covers them. False positives only over-invalidate, and only
//!   when that specific param changes — the safe direction.
//!
//! The result maps each consumed name to a digest of its CURRENT
//! raw value; equality of stored vs freshly computed digests is
//! the per-param leg of SRD-107's three-way skip-validity check.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use polydat::kernel::{PolydatKernel, PolydatProgram};

/// SHA-256 hex of a param's raw string value — the stored
/// representation is a digest, never the value itself, so rows
/// stay uniform and value contents (hosts, credentials-adjacent
/// strings) don't land in provenance stores.
pub(crate) fn value_digest(value: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(b"nbrs-param-value-v1\n");
    h.update(value.as_bytes());
    h.finalize().iter().map(|b| format!("{b:02x}")).collect()
}

/// Derive the phase's consumed-params map: `name -> value_digest`
/// over exactly the params the phase's scope consumes.
///
/// `ancestors_below_session` is innermost-first (immediate parent
/// … workload root), the params module EXCLUDED — the same shape
/// [`crate::scope_tree::ScopeTree::ancestor_kernels_split`]
/// returns. `phase_config_text` is the canonical config
/// serialization ([`super::phase_config_canonical_text`]), shared
/// with the config digest so the two legs read one surface.
pub(crate) fn consumed_params(
    own_program: &PolydatProgram,
    op_template_programs: &[Arc<PolydatProgram>],
    ancestors_below_session: &[Arc<PolydatKernel>],
    phase_config_text: &str,
    params: &HashMap<String, String>,
) -> BTreeMap<String, String> {
    // Seed: every outer-satisfied slot of the phase's own scope.
    let mut unresolved: BTreeSet<String> = own_program
        .outer_input_names()
        .into_iter()
        .collect();
    for prog in op_template_programs {
        unresolved.extend(prog.outer_input_names());
    }

    // Resolve upward. Innermost ancestor wins a name (GK scope
    // shadowing); a passthrough re-export (`x := x` wired from
    // the input of the same name) removes and re-adds the name,
    // which is exactly "keep walking up".
    for kernel in ancestors_below_session {
        if unresolved.is_empty() {
            break;
        }
        let prog = kernel.program();
        let outputs: BTreeSet<&str> = prog.output_names().into_iter().collect();
        let produced: Vec<String> = unresolved.iter()
            .filter(|n| outputs.contains(n.as_str()))
            .cloned()
            .collect();
        if produced.is_empty() {
            continue;
        }
        let produced_refs: Vec<&str> =
            produced.iter().map(String::as_str).collect();
        let closure = prog.extern_closure(&produced_refs);
        for name in &produced {
            unresolved.remove(name);
        }
        unresolved.extend(closure);
    }

    // Terminal intersection + textual union.
    let mut out = BTreeMap::new();
    for (name, value) in params {
        let gk = unresolved.contains(name);
        let textual = phase_config_text.contains(&format!("{{{name}}}"));
        if gk || textual {
            out.insert(name.clone(), value_digest(value));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use polydat::dsl::compile::compile_polydat;

    fn kernel(source: &str) -> Arc<PolydatKernel> {
        Arc::new(compile_polydat(source).expect("compile test kernel"))
    }

    fn params(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs.iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn names(map: &BTreeMap<String, String>) -> Vec<&str> {
        map.keys().map(String::as_str).collect()
    }

    #[test]
    fn direct_extern_consumption() {
        let phase = kernel("extern p1: String\nout := p1\n");
        let got = consumed_params(
            &phase.program(), &[], &[], "",
            &params(&[("p1", "a"), ("p2", "b")]),
        );
        assert_eq!(names(&got), vec!["p1"]);
    }

    #[test]
    fn alias_rebinding_resolves_through_ancestor() {
        // Root rebinds `alias := run_tag`; the phase reads `alias`.
        // The closure must land on run_tag — the aliasing case that
        // defeats textual scanning.
        let root = kernel("extern run_tag: String\nalias := run_tag\n");
        let phase = kernel("extern alias: String\nout := alias\n");
        let got = consumed_params(
            &phase.program(), &[], &[root], "",
            &params(&[("run_tag", "a"), ("other", "b")]),
        );
        assert_eq!(names(&got), vec!["run_tag"]);
    }

    #[test]
    fn sibling_outputs_do_not_drag_their_params_in() {
        // THE precision test: the root produces `a` from p1 and
        // `b` from p2; a phase consuming only `a` must consume
        // only p1 — whole-program over-approximation would pull
        // p2 and reintroduce whole-module invalidation.
        let root = kernel(
            "extern p1: String\nextern p2: String\na := p1\nb := p2\n");
        let phase = kernel("extern a: String\nout := a\n");
        let got = consumed_params(
            &phase.program(), &[], &[root], "",
            &params(&[("p1", "x"), ("p2", "y")]),
        );
        assert_eq!(names(&got), vec!["p1"]);
    }

    #[test]
    fn textual_interpolation_site_is_consumed() {
        let phase = kernel("out := 1\n");
        let got = consumed_params(
            &phase.program(), &[], &[],
            r#"{"ops":{"q":{"stmt":"SELECT * FROM {keyspace}.t"}}}"#,
            &params(&[("keyspace", "ks"), ("unrelated", "z")]),
        );
        assert_eq!(names(&got), vec!["keyspace"]);
    }

    #[test]
    fn op_template_externs_seed_the_walk() {
        let root = kernel("extern p1: String\nfield := p1\n");
        let phase = kernel("out := 1\n");
        let op_template = kernel("extern field: String\nrow := field\n");
        let got = consumed_params(
            &phase.program(),
            &[op_template.program().clone()],
            &[root], "",
            &params(&[("p1", "x"), ("p2", "y")]),
        );
        assert_eq!(names(&got), vec!["p1"]);
    }

    #[test]
    fn iteration_var_resolved_by_scope_is_not_a_param() {
        // A comprehension scope produces `section` from a literal;
        // the phase's `section` extern resolves there and never
        // reaches the params module.
        let comprehension = kernel("section := \"b\"\n");
        let phase = kernel("extern section: String\nout := section\n");
        let got = consumed_params(
            &phase.program(), &[], &[comprehension], "",
            &params(&[("run_tag", "a")]),
        );
        assert!(got.is_empty(), "got: {got:?}");
    }

    #[test]
    fn coordinates_are_excluded_and_empty_set_is_empty() {
        let phase = kernel("input cycle: u64\nout := cycle\n");
        let got = consumed_params(
            &phase.program(), &[], &[], "",
            &params(&[("p1", "a")]),
        );
        assert!(got.is_empty(), "got: {got:?}");
    }

    #[test]
    fn value_digest_tracks_the_value() {
        assert_eq!(value_digest("a"), value_digest("a"));
        assert_ne!(value_digest("a"), value_digest("b"));
    }
}
