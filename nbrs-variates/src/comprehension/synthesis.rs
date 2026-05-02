// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Comprehension scope GK source synthesis.
//!
//! Given the static shape of an iteration (a [`Comprehension`])
//! and the parent kernel, this module emits the GK source for
//! the comprehension scope's own kernel — the kernel that holds
//! every name visible at this scope so spec interpolation,
//! child `bind_outer_scope` chains, and dynamic-control resolution
//! all answer through one canonical state holder
//! (SRD-16 §"Single Name-Resolution Surface").
//!
//! ## What gets emitted
//!
//! For a comprehension with iter-vars `iter_vars` and clause
//! expressions `spec_exprs`:
//!
//! ```text
//!   extern <referenced_name>: <type>     # one per name any clause
//!                                        # spec mentions, typed
//!                                        # from the parent manifest
//!   extern <iter_var>: <native_type>     # one per clause's iter
//!                                        # var, typed via probe
//!                                        # pre-evaluation
//!   extern <cascade_name>: <type>        # one per parent kernel
//!                                        # name not yet covered —
//!                                        # the layer-passthrough
//!                                        # cascade SRD-18b §"Scope
//!                                        # Composition"
//! ```
//!
//! The `extern` declarations install passthrough nodes so each
//! name is both an input slot (bound by `bind_outer_scope` on
//! children) and an output (visible to descendants). Iter vars
//! get their slots populated per-iteration by the executor's
//! `set_input` writes; everything else flows in once at scope
//! activation via the standard composition primitives.
//!
//! ## Where this used to live
//!
//! Pre-Phase-D this code lived in `nbrs-activity::scope` as
//! `build_for_each_scope_kernel`. The lift was driven by
//! `docs/internals/50_comprehensions_first_class.md`: GK owns
//! "what a comprehension means" end-to-end, including the GK
//! source it expands to.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::kernel::{extract_manifest, GkKernel, ManifestEntry};
use crate::node::{PortType, Value};

use super::ast::{Comprehension, ComprehensionMode};
use super::eval::{enumerate_tuples, pre_evaluate_clause, value_to_gk_type_name};

/// Synthesize and compile a GK kernel for a comprehension scope.
///
/// `iter_vars` are the clause variable names in declaration
/// order; `spec_exprs[i]` is the expression for `iter_vars[i]`.
/// `parent_manifest` describes the parent kernel's typed outputs
/// (use [`crate::kernel::extract_manifest`] on the parent's
/// program). `parent_kernel` provides the in-scope name space
/// for clause pre-evaluation. `workload_params` is the
/// fallback string-substitute source for names not yet promoted
/// to kernel `init` bindings.
///
/// Returns a kernel with:
/// - One output per name visible at this scope (via the extern
///   passthroughs).
/// - `bind_outer_scope(parent)` already called.
/// - Parent's input-slot values propagated (so cascade names
///   reach this kernel even when the parent inherited them
///   itself rather than declaring them).
///
/// The caller's responsibility: per-iteration, install the
/// tuple's typed values on this kernel's input slots before
/// evaluating children. See [`super::iterate`] for the
/// one-call ergonomic that handles iteration plus binding.
pub fn synthesize_for_each_scope(
    iter_vars: &[String],
    spec_exprs: &[String],
    parent_manifest: &[ManifestEntry],
    parent_kernel: &GkKernel,
    workload_params: &HashMap<String, String>,
    gk_lib_paths: Vec<std::path::PathBuf>,
    workload_dir: Option<&std::path::Path>,
    strict: bool,
    context: &str,
) -> Result<GkKernel, String> {
    let referenced = collect_leaf_placeholders(spec_exprs);

    let manifest_by_name: HashMap<&str, &ManifestEntry> =
        parent_manifest.iter().map(|e| (e.name.as_str(), e)).collect();

    let mut source = String::new();
    let mut emitted_externs: HashSet<String> = HashSet::new();
    // Names that are declared on this scope only because they
    // cascade through from a parent — not because they're this
    // scope's own iter coords. Filled below as we walk
    // `referenced` against `manifest_by_name`, then again when
    // we walk every workload param + parent extern through
    // `cascade_external`. Passed to `mark_inherited_outputs`
    // before the kernel binds, so `compute_own_coordinates`
    // correctly excludes them when reporting this scope's own
    // iteration position (SRD 18b §"Iteration variables as
    // scope outputs").
    let mut inherited_names: Vec<String> = Vec::new();

    for name in &referenced {
        if iter_vars.iter().any(|v| v == name) { continue; }
        if let Some(entry) = manifest_by_name.get(name.as_str()) {
            let type_name = port_type_to_extern_name(entry.port_type);
            source.push_str(&format!("extern {name}: {type_name}\n"));
            emitted_externs.insert(name.clone());
            // Manifest-sourced names are inherited from the
            // parent scope's program — this scope is just
            // wiring the value through so its clause
            // interpolation can resolve it. Without the mark,
            // `compute_own_coordinates` would report the
            // outer scope's iter coord (e.g. `profile`) as
            // belonging to this inner scope, doubling it up
            // in the striated coord display.
            inherited_names.push(name.clone());
        } else if let Some(value) = workload_params.get(name) {
            let literal = format_workload_param_as_gk_literal(value);
            source.push_str(&format!("final {name} := {literal}\n"));
            emitted_externs.insert(name.clone());
        }
    }

    let mut probes: HashMap<String, String> = HashMap::new();
    let mut all_referenced: HashSet<String> = referenced.clone();
    for (idx, var) in iter_vars.iter().enumerate() {
        if emitted_externs.contains(var) { continue; }
        let spec_text = spec_exprs.get(idx).map(String::as_str).unwrap_or("");
        let values = pre_evaluate_clause(spec_text, parent_kernel, workload_params, &probes)
            .unwrap_or_default();
        let detected_type = values.first()
            .map(value_to_gk_type_name)
            .unwrap_or("String");
        source.push_str(&format!("extern {var}: {detected_type}\n"));
        emitted_externs.insert(var.clone());

        for v in &values {
            let v_str = v.to_display_string();
            for next_idx in (idx + 1)..spec_exprs.len() {
                let mut substituted = spec_exprs[next_idx].clone();
                substituted = substituted.replace(&format!("{{{var}}}"), &v_str);
                let mut emergent = HashSet::new();
                scan_one(&substituted, &mut emergent);
                all_referenced.extend(emergent);
            }
        }

        if let Some(first) = values.into_iter().next() {
            probes.insert(var.clone(), first.to_display_string());
        }
    }

    for name in &all_referenced {
        if emitted_externs.contains(name) { continue; }
        if iter_vars.iter().any(|v| v == name) { continue; }
        if let Some(entry) = manifest_by_name.get(name.as_str()) {
            let type_name = port_type_to_extern_name(entry.port_type);
            source.push_str(&format!("extern {name}: {type_name}\n"));
            emitted_externs.insert(name.clone());
            // Same coordinate-attribution fix as the
            // `referenced` loop above: names that flow in
            // from the parent scope's manifest are
            // **inherited** at this level, not own iter
            // coords. Without the mark,
            // `compute_own_coordinates` reports cascaded
            // workload params (e.g. `k_1_limits` reached
            // via the `{k_{k}_limits}` substitution in a
            // dependent for_each clause) as if they
            // belonged to this scope, and they leak into
            // the leaf coord stratum on every status / log
            // line.
            inherited_names.push(name.clone());
        } else if let Some(value) = workload_params.get(name) {
            let literal = format_workload_param_as_gk_literal(value);
            source.push_str(&format!("final {name} := {literal}\n"));
            emitted_externs.insert(name.clone());
        }
    }

    for (name, value) in workload_params {
        if emitted_externs.contains(name) { continue; }
        if iter_vars.iter().any(|v| v == name) { continue; }
        let type_name = workload_param_type_name(value);
        source.push_str(&format!("extern {name}: {type_name}\n"));
        emitted_externs.insert(name.clone());
        inherited_names.push(name.clone());
    }

    let parent_program = parent_kernel.program();
    let cascade_external = |emitted: &HashSet<String>,
                            iter_vars: &[String],
                            name: &str|
        -> bool
    {
        if emitted.contains(name) { return false; }
        if iter_vars.iter().any(|v| v == name) { return false; }
        if name == "cycle" { return false; }
        // Internal compiler-generated names skip the cascade,
        // EXCEPT cursor extent auxiliaries — those are read by
        // the comprehension `all(<cursor>)` form to enumerate
        // a cursor's full range, so they must flow through to
        // descendant scopes that consume them.
        if name.starts_with("__") && !name.starts_with("__cursor_extent_") {
            return false;
        }
        true
    };
    for name in parent_program.output_names() {
        let owned = name.to_string();
        if !cascade_external(&emitted_externs, iter_vars, &owned) { continue; }
        let (node_idx, port_idx) = parent_program.resolve_output_by_index(
            parent_program.output_index(&owned).unwrap()
        );
        let port_type = parent_program.node_meta(node_idx).outs[port_idx].typ;
        let type_name = port_type_to_extern_name(port_type);
        source.push_str(&format!("extern {owned}: {type_name}\n"));
        emitted_externs.insert(owned.clone());
        inherited_names.push(owned);
    }
    for name in parent_program.input_names() {
        if !cascade_external(&emitted_externs, iter_vars, &name) { continue; }
        let port_type = parent_program.input_port_type(&name)
            .unwrap_or(PortType::Str);
        let type_name = port_type_to_extern_name(port_type);
        source.push_str(&format!("extern {name}: {type_name}\n"));
        emitted_externs.insert(name.clone());
        inherited_names.push(name);
    }

    if source.is_empty() {
        source.push_str("final __empty := 0\n");
    }

    let mut kernel = crate::dsl::compile::compile_gk_with_libs(
        &source,
        workload_dir,
        gk_lib_paths,
        &[],
        strict,
        context,
    ).map_err(|e| format!("{context}: for_each scope synthesis: {e}"))?;

    kernel.mark_inherited_outputs(inherited_names);

    kernel.bind_outer_scope(parent_kernel);
    propagate_parent_inputs(&mut kernel, parent_kernel);

    Ok(kernel)
}

/// Copy parent kernel's currently-set input-slot values into the
/// inner kernel's input slots by name. Companion to
/// [`GkKernel::bind_outer_scope`] which only walks parent
/// outputs; this extends the chain so cascade-extern'd inputs
/// propagate too. Without it, names that the parent kernel
/// inherited from *its* parent would stop at the parent and
/// never reach the grandchild's matching slot.
pub fn propagate_parent_inputs(
    inner: &mut GkKernel,
    outer: &GkKernel,
) {
    let names = outer.program().input_names();
    for name in names {
        let Some(outer_value) = outer.get_input(&name) else { continue };
        if matches!(outer_value, Value::None) { continue; }
        let cloned = outer_value.clone();
        let Some(inner_idx) = inner.program().find_input(&name) else { continue };
        inner.state().set_input(inner_idx, cloned);
    }
}

/// Pick the GK port type for a workload-param string value.
/// Numeric values widen to `u64` / `f64`; `true`/`false` →
/// `bool`; everything else → `String`.
pub fn workload_param_type_name(value: &str) -> &'static str {
    let trimmed = value.trim();
    if trimmed.parse::<u64>().is_ok() {
        "u64"
    } else if trimmed.parse::<f64>().is_ok() {
        "f64"
    } else if trimmed == "true" || trimmed == "false" {
        "bool"
    } else {
        "String"
    }
}

/// Format a workload-param string as a GK literal (for emission
/// as `final <name> := <literal>`). Numbers and booleans pass
/// through; everything else becomes a quoted string with escape
/// handling so the GK source stays parsable.
pub fn format_workload_param_as_gk_literal(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.parse::<u64>().is_ok() || trimmed.parse::<f64>().is_ok() {
        trimmed.to_string()
    } else if trimmed == "true" || trimmed == "false" {
        trimmed.to_string()
    } else {
        let escaped = value.replace('\\', "\\\\").replace('"', "\\\"");
        format!("\"{escaped}\"")
    }
}

/// Map a GK [`PortType`] to the extern declaration's type name
/// (`u64`, `f64`, `bool`, `String`). Anything not in the four
/// scalar variants widens to `String` — the GK extern grammar
/// only accepts those four type names.
fn port_type_to_extern_name(t: PortType) -> &'static str {
    match t {
        PortType::U64 => "u64",
        PortType::F64 => "f64",
        PortType::Str => "String",
        PortType::Bool => "bool",
        _ => "String",
    }
}

/// Collect every leaf `{name}` placeholder from a list of clause
/// spec texts. "Leaf" means a `{...}` whose body contains no
/// further `{` — the dynamic case (`{a_{b}_c}`) is handled at
/// runtime by the iterative interpolator. Honors `\{` / `\}`
/// escapes (the same escape syntax `interpolate` uses).
pub fn collect_leaf_placeholders(texts: &[String]) -> HashSet<String> {
    let mut out = HashSet::new();
    for text in texts {
        scan_one(text, &mut out);
    }
    out
}

/// One-call ergonomic: turn a [`Comprehension`] + parent kernel
/// into an iterator of fully-bound child contexts.
///
/// Each yielded [`GkKernel`] is:
/// - Compiled from the comprehension's synthesized GK source
///   (one canonical program shared across iterations via
///   `Arc<GkProgram>`).
/// - Wired to `parent` via [`GkKernel::bind_outer_scope`] plus
///   [`propagate_parent_inputs`] so cascade names reach this
///   layer regardless of where they were declared.
/// - Populated with the iteration's typed coordinate values on
///   the matching input slots.
///
/// Caller drives child evaluation directly off the yielded
/// kernel: `child.lookup(name)`, `child.pull(name)`, etc.
///
/// ## Mode handling
///
/// - [`ComprehensionMode::Cartesian`]: one canonical, one
///   tuple stream from `enumerate_tuples` over the flat clause
///   list.
/// - [`ComprehensionMode::Union`]: one canonical (deduplicated
///   iter-var set, first-occurrence specs feed synthesis-time
///   typing), tuple stream is the concatenation of each
///   sub-space's enumeration.
///
/// ## Empty-clause policy
///
/// If any clause produces no values, this returns `Err`
/// (strict mode). Use [`enumerate_tuples`] directly with a
/// custom callback for warn-and-skip behavior.
pub fn iterate(
    comprehension: &Comprehension,
    parent: &Arc<GkKernel>,
    workload_params: &HashMap<String, String>,
    gk_lib_paths: Vec<std::path::PathBuf>,
    workload_dir: Option<&std::path::Path>,
    strict: bool,
    context: &str,
) -> Result<ComprehensionIter, String> {
    // Representative iter_vars + spec_exprs for synthesis.
    // Cartesian: the flat clause list as authored.
    // Union: dedup'd by var name with first-occurrence spec —
    // the synthesizer only consults specs for type detection,
    // and any sub-space's representative spec works.
    let representative: Vec<(String, String)> = match &comprehension.mode {
        ComprehensionMode::Cartesian(clauses) => {
            clauses.iter().map(|c| (c.var.clone(), c.expr.clone())).collect()
        }
        ComprehensionMode::Union(subspaces) => {
            let mut seen: HashSet<String> = HashSet::new();
            let mut out = Vec::new();
            for sub in subspaces {
                for c in sub {
                    if seen.insert(c.var.clone()) {
                        out.push((c.var.clone(), c.expr.clone()));
                    }
                }
            }
            out
        }
    };
    let iter_vars: Vec<String> = representative.iter().map(|(v, _)| v.clone()).collect();
    let spec_exprs: Vec<String> = representative.iter().map(|(_, e)| e.clone()).collect();

    let parent_manifest = extract_manifest(parent.program());
    let canonical_kernel = synthesize_for_each_scope(
        &iter_vars, &spec_exprs, &parent_manifest, parent,
        workload_params, gk_lib_paths, workload_dir, strict, context,
    )?;
    let canonical = Arc::new(canonical_kernel);

    let strict_empty = |var: &str, spec_text: &str| -> Result<(), String> {
        Err(format!(
            "comprehension clause '{var} in {spec_text}' produced no values"
        ))
    };

    let filter = comprehension.filter.as_deref();
    let (mut tuples, clause_sizes): (Vec<Vec<(String, Value)>>, Vec<usize>) = match &comprehension.mode {
        ComprehensionMode::Cartesian(clauses) => {
            let pairs: Vec<(String, String)> = clauses.iter()
                .map(|c| (c.var.clone(), c.expr.clone())).collect();
            // Compute per-clause sizes for the order layer.
            // For Cartesian mode the canonical lattice cardinality
            // equals the product of clause cardinalities.
            let sizes = compute_clause_sizes(parent, &pairs, workload_params)?;
            let tuples = enumerate_tuples(&canonical, parent, &pairs, filter, strict_empty)?;
            (tuples, sizes)
        }
        ComprehensionMode::Union(subspaces) => {
            let mut all = Vec::new();
            let mut max_sub_sizes: Vec<usize> = Vec::new();
            for sub in subspaces {
                let pairs: Vec<(String, String)> = sub.iter()
                    .map(|c| (c.var.clone(), c.expr.clone())).collect();
                let sizes = compute_clause_sizes(parent, &pairs, workload_params)?;
                if sizes.iter().product::<usize>() > max_sub_sizes.iter().product::<usize>() {
                    max_sub_sizes = sizes;
                }
                let mut t = enumerate_tuples(&canonical, parent, &pairs, filter, strict_empty)?;
                all.append(&mut t);
            }
            (all, max_sub_sizes)
        }
    };

    // Apply ordering, if any. Default (None) preserves the
    // lex-ordered tuple stream from `enumerate_tuples`.
    if let Some(order) = comprehension.order.as_ref() {
        tuples = super::order::apply_order(tuples, &clause_sizes, order)?;
    }

    Ok(ComprehensionIter {
        canonical,
        parent: parent.clone(),
        tuples: tuples.into_iter(),
    })
}

/// Compute the per-clause cardinality used as `clause_sizes`
/// input to `apply_order`. Each clause's spec is pre-evaluated
/// against `parent` (with prior-clause first values substituted
/// as probes — same convention as
/// [`synthesize_for_each_scope`]).
///
/// Empty / unevaluatable clauses are reported as size 1 so the
/// ordering layer doesn't fail in degenerate cases — the actual
/// tuple count from `enumerate_tuples` is still authoritative
/// for the order operation; sizes here just inform the
/// geometric reasoning.
fn compute_clause_sizes(
    parent: &GkKernel,
    clauses: &[(String, String)],
    workload_params: &HashMap<String, String>,
) -> Result<Vec<usize>, String> {
    let mut probes: HashMap<String, String> = HashMap::new();
    let mut sizes = Vec::with_capacity(clauses.len());
    for (var, spec_text) in clauses {
        let values = pre_evaluate_clause(spec_text, parent, workload_params, &probes)
            .unwrap_or_default();
        sizes.push(values.len().max(1));
        if let Some(first) = values.into_iter().next() {
            probes.insert(var.clone(), first.to_display_string());
        }
    }
    Ok(sizes)
}

/// Iterator yielded by [`iterate`]. Each `next()` returns a
/// fresh per-iteration child kernel with the tuple's coordinate
/// values installed and parent-scope bindings already wired.
pub struct ComprehensionIter {
    canonical: Arc<GkKernel>,
    parent: Arc<GkKernel>,
    tuples: std::vec::IntoIter<Vec<(String, Value)>>,
}

impl ComprehensionIter {
    /// Number of tuples remaining.
    pub fn len(&self) -> usize {
        self.tuples.len()
    }

    /// True if no more tuples remain.
    pub fn is_empty(&self) -> bool {
        self.tuples.len() == 0
    }
}

impl Iterator for ComprehensionIter {
    type Item = GkKernel;

    fn next(&mut self) -> Option<Self::Item> {
        let tuple = self.tuples.next()?;
        let mut child = GkKernel::from_program(self.canonical.program().clone());
        child.bind_outer_scope(&self.parent);
        propagate_parent_inputs(&mut child, &self.parent);
        for (var, value) in tuple {
            if let Some(slot) = child.program().find_input(&var) {
                child.state().set_input(slot, value);
            }
        }
        Some(child)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let n = self.tuples.len();
        (n, Some(n))
    }
}

impl ExactSizeIterator for ComprehensionIter {}

/// Single-text version of [`collect_leaf_placeholders`]. Used
/// during clause-by-clause emergent-name discovery in the
/// synthesizer.
pub fn scan_one(text: &str, out: &mut HashSet<String>) {
    let bytes = text.as_bytes();
    let n = bytes.len();
    let mut i = 0;
    while i < n {
        if bytes[i] == b'\\' && i + 1 < n
            && (bytes[i + 1] == b'{' || bytes[i + 1] == b'}')
        {
            i += 2;
            continue;
        }
        if bytes[i] == b'{' {
            let mut j = i + 1;
            let mut nested = false;
            while j < n {
                if bytes[j] == b'\\' && j + 1 < n
                    && (bytes[j + 1] == b'{' || bytes[j + 1] == b'}')
                {
                    j += 2;
                    continue;
                }
                if bytes[j] == b'{' { nested = true; break; }
                if bytes[j] == b'}' { break; }
                j += 1;
            }
            if !nested && j < n && bytes[j] == b'}' {
                let name = &text[i + 1..j];
                if !name.is_empty() {
                    out.insert(name.to_string());
                }
                i = j + 1;
                continue;
            }
            i += 1;
            continue;
        }
        i += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::ast::Clause;

    #[test]
    fn iterate_cartesian_yields_bound_child_kernels() {
        // Parent holds a final string-list workload param.
        let parent = Arc::new(crate::dsl::compile::compile_gk(
            "final k_values := \"1, 10, 100\"\n"
        ).unwrap());

        let comp = Comprehension::cartesian(vec![
            Clause::new("k", "{k_values}"),
        ]);

        let mut iter = iterate(
            &comp, &parent, &HashMap::new(),
            Vec::new(), None, false, "test",
        ).unwrap();
        assert_eq!(iter.len(), 3);

        let yielded: Vec<u64> = (&mut iter)
            .map(|child| child.lookup("k").unwrap().as_u64())
            .collect();
        assert_eq!(yielded, vec![1, 10, 100]);
    }

    #[test]
    fn iterate_union_concatenates_subspaces() {
        // Two sub-spaces, each its own values for `k`.
        let parent = Arc::new(crate::dsl::compile::compile_gk(
            "final small_k := \"1, 2\"\nfinal big_k := \"100, 200\"\n"
        ).unwrap());

        let comp = Comprehension::union(vec![
            vec![Clause::new("k", "{small_k}")],
            vec![Clause::new("k", "{big_k}")],
        ]);

        let iter = iterate(
            &comp, &parent, &HashMap::new(),
            Vec::new(), None, false, "test",
        ).unwrap();

        let yielded: Vec<u64> = iter
            .map(|child| child.lookup("k").unwrap().as_u64())
            .collect();
        assert_eq!(yielded, vec![1, 2, 100, 200]);
    }

    #[test]
    fn iterate_cartesian_two_clauses_emits_cross_product() {
        let parent = Arc::new(crate::dsl::compile::compile_gk(
            "final ks := \"1, 2\"\nfinal limits := \"10, 20\"\n"
        ).unwrap());

        let comp = Comprehension::cartesian(vec![
            Clause::new("k", "{ks}"),
            Clause::new("limit", "{limits}"),
        ]);

        let iter = iterate(
            &comp, &parent, &HashMap::new(),
            Vec::new(), None, false, "test",
        ).unwrap();

        let yielded: Vec<(u64, u64)> = iter
            .map(|child| (
                child.lookup("k").unwrap().as_u64(),
                child.lookup("limit").unwrap().as_u64(),
            ))
            .collect();
        assert_eq!(yielded, vec![(1, 10), (1, 20), (2, 10), (2, 20)]);
    }

    #[test]
    fn iterate_with_all_cursor_yields_extent_range() {
        // Parent kernel synthesizes the `__cursor_extent_*`
        // auxiliary outputs that a real cursor declaration
        // would produce. The comprehension `for xval in all(row)`
        // resolves them and emits the half-open ordinal range.
        let parent = Arc::new(crate::dsl::compile::compile_gk(
            "final __cursor_extent_row_start := 0\n\
             final __cursor_extent_row_end := 50\n"
        ).unwrap());
        let comp = Comprehension::cartesian(vec![
            Clause::new("xval", "all(row)"),
        ]);

        let iter = iterate(
            &comp, &parent, &HashMap::new(),
            Vec::new(), None, false, "test",
        ).unwrap();

        let yielded: Vec<u64> = iter
            .map(|child| child.lookup("xval").unwrap().as_u64())
            .collect();
        let expected: Vec<u64> = (0..50).collect();
        assert_eq!(yielded, expected);
    }

    #[test]
    fn iterate_with_all_cursor_and_filter_truncates() {
        let parent = Arc::new(crate::dsl::compile::compile_gk(
            "final __cursor_extent_row_start := 0\n\
             final __cursor_extent_row_end := 20\n"
        ).unwrap());
        let comp = Comprehension::cartesian(vec![
            Clause::new("xval", "all(row)"),
        ]).with_filter("{xval} % 5 == 0");

        let iter = iterate(
            &comp, &parent, &HashMap::new(),
            Vec::new(), None, false, "test",
        ).unwrap();

        let yielded: Vec<u64> = iter
            .map(|child| child.lookup("xval").unwrap().as_u64())
            .collect();
        // Every 5th ordinal in [0, 20).
        assert_eq!(yielded, vec![0, 5, 10, 15]);
    }

    #[test]
    fn iterate_order_extrema_first_visits_corners() {
        let parent = Arc::new(crate::dsl::compile::compile_gk(
            "final ks := \"1, 5, 10\"\nfinal limits := \"1, 5, 10\"\n"
        ).unwrap());

        let comp = Comprehension::cartesian(vec![
            Clause::new("k", "{ks}"),
            Clause::new("limit", "{limits}"),
        ]).with_order(super::super::ast::TraversalOrder::Extrema { strata: Some(1) });

        let iter = iterate(
            &comp, &parent, &HashMap::new(),
            Vec::new(), None, false, "test",
        ).unwrap();

        let yielded: Vec<(u64, u64)> = iter
            .map(|child| (
                child.lookup("k").unwrap().as_u64(),
                child.lookup("limit").unwrap().as_u64(),
            ))
            .collect();
        // Strata-1 = corners only (where every coord is at min or max).
        // 3x3 has 4 corners: (1,1) (1,10) (10,1) (10,10).
        assert_eq!(yielded.len(), 4);
        assert!(yielded.contains(&(1, 1)));
        assert!(yielded.contains(&(1, 10)));
        assert!(yielded.contains(&(10, 1)));
        assert!(yielded.contains(&(10, 10)));
    }

    #[test]
    fn iterate_order_with_lex_count_truncates() {
        let parent = Arc::new(crate::dsl::compile::compile_gk(
            "final xs := \"1, 2, 3, 4, 5\"\n"
        ).unwrap());
        let comp = Comprehension::cartesian(vec![
            Clause::new("x", "{xs}"),
        ]).with_order(super::super::ast::TraversalOrder::Lex { count: Some(3) });

        let iter = iterate(
            &comp, &parent, &HashMap::new(),
            Vec::new(), None, false, "test",
        ).unwrap();

        let yielded: Vec<u64> = iter
            .map(|child| child.lookup("x").unwrap().as_u64())
            .collect();
        assert_eq!(yielded, vec![1, 2, 3]);
    }

    #[test]
    fn iterate_filter_skips_tuples() {
        // Cartesian product `(k, limit)` over 2x4 = 8 tuples,
        // filter `k * limit < 1000` keeps only those whose
        // product is below the threshold.
        let parent = Arc::new(crate::dsl::compile::compile_gk(
            "final ks := \"10, 100\"\nfinal limits := \"1, 10, 50, 100\"\n"
        ).unwrap());

        let comp = Comprehension::cartesian(vec![
            Clause::new("k", "{ks}"),
            Clause::new("limit", "{limits}"),
        ]).with_filter("{k} * {limit} < 1000");

        let iter = iterate(
            &comp, &parent, &HashMap::new(),
            Vec::new(), None, false, "test",
        ).unwrap();

        let yielded: Vec<(u64, u64)> = iter
            .map(|child| (
                child.lookup("k").unwrap().as_u64(),
                child.lookup("limit").unwrap().as_u64(),
            ))
            .collect();

        // Full 8 tuples cross-product:
        //   (10,1)=10  (10,10)=100  (10,50)=500   (10,100)=1000
        //   (100,1)=100 (100,10)=1000 (100,50)=5000 (100,100)=10000
        // Filter k*limit < 1000 keeps the four whose product is
        // strictly below 1000.
        assert_eq!(yielded, vec![
            (10, 1), (10, 10), (10, 50),
            (100, 1),
        ]);
    }

    #[test]
    fn iterate_filter_on_union_applies_per_tuple() {
        // Union of two sub-spaces with the same filter applied
        // uniformly to every tuple regardless of which sub-space
        // produced it.
        let parent = Arc::new(crate::dsl::compile::compile_gk(
            "final s1 := \"1, 2, 3\"\nfinal s2 := \"10, 20, 30\"\n"
        ).unwrap());

        let comp = Comprehension::union(vec![
            vec![Clause::new("k", "{s1}")],
            vec![Clause::new("k", "{s2}")],
        ]).with_filter("{k} > 2");

        let iter = iterate(
            &comp, &parent, &HashMap::new(),
            Vec::new(), None, false, "test",
        ).unwrap();

        let yielded: Vec<u64> = iter
            .map(|child| child.lookup("k").unwrap().as_u64())
            .collect();
        // Sub-space 1: [1, 2, 3] → keeps 3
        // Sub-space 2: [10, 20, 30] → keeps all
        assert_eq!(yielded, vec![3, 10, 20, 30]);
    }

    #[test]
    fn iterate_propagates_inherited_names_to_child() {
        // A parent-scope name (workload param expressed as final)
        // must be visible from inside the child kernel.
        let parent = Arc::new(crate::dsl::compile::compile_gk(
            "final dataset := \"glove-100\"\nfinal ks := \"1, 2\"\n"
        ).unwrap());

        let comp = Comprehension::cartesian(vec![
            Clause::new("k", "{ks}"),
        ]);

        let mut iter = iterate(
            &comp, &parent, &HashMap::new(),
            Vec::new(), None, false, "test",
        ).unwrap();

        let first = iter.next().unwrap();
        // The inherited `dataset` resolves through the standard
        // lookup since synthesize_for_each_scope cascaded it as
        // an extern.
        assert_eq!(first.lookup("dataset").unwrap().to_display_string(), "glove-100");
        assert_eq!(first.lookup("k").unwrap().as_u64(), 1);
    }

    #[test]
    fn collect_leaf_placeholders_skips_nested() {
        let texts = vec!["{flat}".to_string(), "{outer_{inner}_tail}".to_string()];
        let names = collect_leaf_placeholders(&texts);
        // `flat` is a leaf; the outer `{outer_..._tail}` is not
        // (its body has another `{`); the inner `{inner}` is
        // a leaf at its own depth.
        assert!(names.contains("flat"));
        assert!(names.contains("inner"));
        assert!(!names.contains("outer_{inner}_tail"));
    }

    #[test]
    fn collect_leaf_placeholders_honors_escapes() {
        let texts = vec!["\\{not_a_var\\}".to_string(), "{real}".to_string()];
        let names = collect_leaf_placeholders(&texts);
        assert!(names.contains("real"));
        assert!(!names.contains("not_a_var"));
    }

    #[test]
    fn workload_param_type_name_classification() {
        assert_eq!(workload_param_type_name("42"), "u64");
        assert_eq!(workload_param_type_name("1.5"), "f64");
        assert_eq!(workload_param_type_name("true"), "bool");
        assert_eq!(workload_param_type_name("false"), "bool");
        assert_eq!(workload_param_type_name("hello"), "String");
        assert_eq!(workload_param_type_name("1, 2, 3"), "String");
    }

    #[test]
    fn format_workload_param_quotes_strings() {
        assert_eq!(format_workload_param_as_gk_literal("42"), "42");
        assert_eq!(format_workload_param_as_gk_literal("1.5"), "1.5");
        assert_eq!(format_workload_param_as_gk_literal("true"), "true");
        assert_eq!(format_workload_param_as_gk_literal("hello"), "\"hello\"");
        assert_eq!(format_workload_param_as_gk_literal("a\"b"), "\"a\\\"b\"");
    }
}
