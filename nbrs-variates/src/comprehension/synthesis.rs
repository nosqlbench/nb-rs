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
    bindings: &[(String, String)],
    parent_manifest: &[ManifestEntry],
    parent_kernel: &GkKernel,
    workload_params: &HashMap<String, String>,
    gk_lib_paths: Vec<std::path::PathBuf>,
    workload_dir: Option<&std::path::Path>,
    strict: bool,
    context: &str,
) -> Result<GkKernel, String> {
    // `bindings` is `[(var, spec_expr)]` per scalar variable.
    // Parallel-iter clauses contribute one entry per
    // `vars[i] = exprs[i]`; single-var clauses contribute one
    // entry. Pairing the var with its spec eliminates the
    // parallel-array hazard the previous signature carried.
    let iter_vars: Vec<String> = bindings.iter().map(|(v, _)| v.clone()).collect();
    let spec_exprs: Vec<String> = bindings.iter().map(|(_, e)| e.clone()).collect();
    let referenced = collect_leaf_placeholders(&spec_exprs);

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
        if !cascade_external(&emitted_externs, &iter_vars, &owned) { continue; }
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
        if !cascade_external(&emitted_externs, &iter_vars, &name) { continue; }
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

    // SRD-67 Phase 3 — route through the
    // `SubcontextBuilder` bridge so finalize handles import /
    // export validation, Rule 2 collision detection, and
    // `mark_inherited_outputs` in one place. The for_each
    // synthesiser threads `gk_lib_paths` / `workload_dir` /
    // `strict` through `CompileOptions` so the underlying
    // `compile_gk_with_libs` invocation is byte-identical to
    // the legacy direct call.
    let compile_options = crate::subcontext::CompileOptions {
        workload_dir: workload_dir.map(|p| p.to_path_buf()),
        gk_lib_paths,
        strict,
        required_outputs: Vec::new(),
        context_label: Some(context.to_string()),
        cursor_limit: None,
    };
    let matter = crate::subcontext::GkMatter::builder()
        .label(context)
        .source(source)
        .inherited_outputs(inherited_names)
        .options(compile_options)
        .build()
        .map_err(|e| format!("{context}: for_each scope synthesis: {e}"))?;
    let mut kernel = parent_kernel
        .build_subscope(matter)
        .map_err(|e| format!("{context}: for_each scope synthesis: {e}"))?;

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
    // Cartesian: every (var, expr) pair from the flat clause
    // list, expanded for parallel groups so each variable in
    // the group contributes its own (name, expr) entry.
    // Union: dedup'd by var name with first-occurrence spec —
    // the synthesizer only consults specs for type detection,
    // and any sub-space's representative spec works.
    let representative: Vec<(String, String)> = match &comprehension.mode {
        ComprehensionMode::Cartesian(clauses) => {
            clauses.iter()
                .flat_map(|c| c.scalar_bindings())
                .map(|(v, e)| (v.to_string(), e.to_string()))
                .collect()
        }
        ComprehensionMode::Union(subspaces) => {
            let mut seen: HashSet<String> = HashSet::new();
            let mut out = Vec::new();
            for sub in subspaces {
                for c in sub.iter() {
                    for (v, e) in c.scalar_bindings() {
                        if seen.insert(v.to_string()) {
                            out.push((v.to_string(), e.to_string()));
                        }
                    }
                }
            }
            out
        }
    };
    let parent_manifest = extract_manifest(parent.program());
    let canonical_kernel = synthesize_for_each_scope(
        &representative, &parent_manifest, parent,
        workload_params, gk_lib_paths, workload_dir, strict, context,
    )?;
    let canonical = Arc::new(canonical_kernel);

    let strict_empty = |clause: &super::ast::Clause| -> Result<(), String> {
        Err(format!("comprehension clause '{clause}' produced no values"))
    };

    let filter = comprehension.filter.as_deref();
    let (mut tuples, clause_sizes): (Vec<Vec<(String, Value)>>, Vec<usize>) = match &comprehension.mode {
        ComprehensionMode::Cartesian(clauses) => {
            // Compute per-axis cardinality for the order layer.
            // For Cartesian mode the canonical lattice cardinality
            // equals the product of axis cardinalities — a parallel
            // group is *one* axis (zip-step count), not N.
            let sizes = compute_clause_sizes(parent, clauses, workload_params)?;
            let tuples = enumerate_tuples(&canonical, parent, clauses, filter, strict_empty)?;
            (tuples, sizes)
        }
        ComprehensionMode::Union(subspaces) => {
            let mut all = Vec::new();
            let mut max_sub_sizes: Vec<usize> = Vec::new();
            for sub in subspaces {
                let sizes = compute_clause_sizes(parent, sub, workload_params)?;
                if sizes.iter().product::<usize>() > max_sub_sizes.iter().product::<usize>() {
                    max_sub_sizes = sizes;
                }
                let mut t = enumerate_tuples(&canonical, parent, sub, filter, strict_empty)?;
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
/// Per-axis cardinality used as `clause_sizes` input to
/// [`super::order::apply_order`]. Each clause contributes one
/// axis; a parallel-iter clause is **one** axis (the zip step
/// count under its [`super::ast::ZipMode`]), not N. See SRD-18e
/// Push 2.
///
/// Made `pub(crate)` so the contract — "parallel group is one
/// axis" — has a direct unit-test surface independent of the
/// full iterate path.
pub(crate) fn compute_clause_sizes(
    parent: &GkKernel,
    clauses: &[super::ast::Clause],
    workload_params: &HashMap<String, String>,
) -> Result<Vec<usize>, String> {
    use super::ast::ClauseSource;
    let mut probes: HashMap<String, String> = HashMap::new();
    let mut sizes = Vec::with_capacity(clauses.len());
    for clause in clauses {
        match &clause.source {
            ClauseSource::Single(spec_text) => {
                let values = pre_evaluate_clause(spec_text, parent, workload_params, &probes)
                    .unwrap_or_default();
                sizes.push(values.len().max(1));
                if let Some(first) = values.into_iter().next() {
                    probes.insert(clause.var().to_string(), first.to_display_string());
                }
            }
            ClauseSource::Parallel { mode, exprs } => {
                // Zip-axis cardinality depends on the zip mode:
                // Strict / Truncate use min(len(expr_i)) (the
                // strict case errors at iteration time if
                // lengths actually differ); Cycle uses
                // max(len(expr_i)).
                use super::ast::ZipMode;
                let mut lens: Vec<usize> = Vec::with_capacity(exprs.len());
                for (var, expr) in clause.vars.iter().zip(exprs.iter()) {
                    let values = pre_evaluate_clause(expr, parent, workload_params, &probes)
                        .unwrap_or_default();
                    lens.push(values.len());
                    if let Some(first) = values.into_iter().next() {
                        probes.insert(var.clone(), first.to_display_string());
                    }
                }
                let card = match mode {
                    ZipMode::Strict | ZipMode::Truncate =>
                        lens.iter().copied().min().unwrap_or(1),
                    ZipMode::Cycle =>
                        lens.iter().copied().max().unwrap_or(1),
                };
                sizes.push(card.max(1));
            }
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
        let bindings: Vec<(String, crate::node::Value)> = tuple.into_iter().collect();
        let mut child = self
            .parent
            .materialize_subscope(self.canonical.program().clone(), &bindings);
        propagate_parent_inputs(&mut child, &self.parent);
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

    // ---- Push 2: Layer 7a parallel-iter ----------------------

    #[test]
    fn iterate_parallel_zips_two_axes_in_lockstep() {
        let parent = Arc::new(crate::dsl::compile::compile_gk(
            "final xs := \"1, 2, 3\"\nfinal ys := \"10, 20, 30\"\n"
        ).unwrap());
        let comp = Comprehension::cartesian(vec![
            Clause::parallel(["x", "y"], ["{xs}", "{ys}"]),
        ]);
        let iter = iterate(
            &comp, &parent, &HashMap::new(),
            Vec::new(), None, false, "test",
        ).unwrap();
        let yielded: Vec<(u64, u64)> = iter
            .map(|child| (
                child.lookup("x").unwrap().as_u64(),
                child.lookup("y").unwrap().as_u64(),
            ))
            .collect();
        // Lockstep: (1,10),(2,20),(3,30) — NOT a 3×3 product.
        assert_eq!(yielded, vec![(1, 10), (2, 20), (3, 30)]);
    }

    #[test]
    fn iterate_parallel_length_mismatch_errors() {
        // Parallel-iter is strict-by-default for length: every
        // expr in the group must produce the same number of
        // values. This is independent of the `strict` flag,
        // which controls empty-clause vs warn-and-skip policy.
        let parent = Arc::new(crate::dsl::compile::compile_gk(
            "final xs := \"1, 2, 3\"\nfinal ys := \"10, 20\"\n"
        ).unwrap());
        let comp = Comprehension::cartesian(vec![
            Clause::parallel(["x", "y"], ["{xs}", "{ys}"]),
        ]);
        let err = match iterate(
            &comp, &parent, &HashMap::new(),
            Vec::new(), None, false, "test",
        ) {
            Err(e) => e,
            Ok(_) => panic!("expected error, got Ok"),
        };
        assert!(err.contains("length mismatch"), "got: {err}");
    }

    #[test]
    fn iterate_parallel_zip_truncate_cuts_to_shortest() {
        use super::super::ast::ZipMode;
        let parent = Arc::new(crate::dsl::compile::compile_gk(
            "final xs := \"1, 2, 3, 4, 5\"\nfinal ys := \"10, 20, 30\"\n"
        ).unwrap());
        let comp = Comprehension::cartesian(vec![
            Clause::parallel_with_mode(ZipMode::Truncate, ["x", "y"], ["{xs}", "{ys}"]),
        ]);
        let iter = iterate(
            &comp, &parent, &HashMap::new(),
            Vec::new(), None, false, "test",
        ).unwrap();
        let yielded: Vec<(u64, u64)> = iter
            .map(|child| (
                child.lookup("x").unwrap().as_u64(),
                child.lookup("y").unwrap().as_u64(),
            ))
            .collect();
        // ys has 3 values; xs gets truncated to match.
        assert_eq!(yielded, vec![(1, 10), (2, 20), (3, 30)]);
    }

    #[test]
    fn iterate_parallel_zip_cycle_repeats_shorter() {
        use super::super::ast::ZipMode;
        let parent = Arc::new(crate::dsl::compile::compile_gk(
            "final xs := \"1, 2, 3, 4\"\nfinal ys := \"10, 20\"\n"
        ).unwrap());
        let comp = Comprehension::cartesian(vec![
            Clause::parallel_with_mode(ZipMode::Cycle, ["x", "y"], ["{xs}", "{ys}"]),
        ]);
        let iter = iterate(
            &comp, &parent, &HashMap::new(),
            Vec::new(), None, false, "test",
        ).unwrap();
        let yielded: Vec<(u64, u64)> = iter
            .map(|child| (
                child.lookup("x").unwrap().as_u64(),
                child.lookup("y").unwrap().as_u64(),
            ))
            .collect();
        // xs has 4 values; ys cycles 10,20,10,20.
        assert_eq!(yielded, vec![(1, 10), (2, 20), (3, 10), (4, 20)]);
    }

    // ---- compute_clause_sizes contract tests ----------------

    #[test]
    fn clause_sizes_parallel_strict_uses_min_len() {
        let parent = Arc::new(crate::dsl::compile::compile_gk(
            "final xs := \"1, 2, 3, 4, 5\"\nfinal ys := \"10, 20, 30\"\n"
        ).unwrap());
        let clauses = vec![
            Clause::parallel(["x", "y"], ["{xs}", "{ys}"]),
        ];
        let sizes = compute_clause_sizes(&parent, &clauses, &HashMap::new()).unwrap();
        // One axis, cardinality min(5, 3) = 3.
        assert_eq!(sizes, vec![3]);
    }

    #[test]
    fn clause_sizes_parallel_truncate_uses_min_len() {
        use super::super::ast::ZipMode;
        let parent = Arc::new(crate::dsl::compile::compile_gk(
            "final xs := \"1, 2, 3, 4\"\nfinal ys := \"10, 20\"\n"
        ).unwrap());
        let clauses = vec![
            Clause::parallel_with_mode(ZipMode::Truncate, ["x", "y"], ["{xs}", "{ys}"]),
        ];
        let sizes = compute_clause_sizes(&parent, &clauses, &HashMap::new()).unwrap();
        assert_eq!(sizes, vec![2]);
    }

    #[test]
    fn clause_sizes_parallel_cycle_uses_max_len() {
        use super::super::ast::ZipMode;
        let parent = Arc::new(crate::dsl::compile::compile_gk(
            "final xs := \"1, 2, 3, 4\"\nfinal ys := \"10, 20\"\n"
        ).unwrap());
        let clauses = vec![
            Clause::parallel_with_mode(ZipMode::Cycle, ["x", "y"], ["{xs}", "{ys}"]),
        ];
        let sizes = compute_clause_sizes(&parent, &clauses, &HashMap::new()).unwrap();
        // Cycle = max(4, 2) = 4.
        assert_eq!(sizes, vec![4]);
    }

    #[test]
    fn clause_sizes_parallel_then_single_two_axes() {
        let parent = Arc::new(crate::dsl::compile::compile_gk(
            "final xs := \"1, 2, 3\"\nfinal ys := \"10, 20, 30\"\nfinal zs := \"100, 200\"\n"
        ).unwrap());
        let clauses = vec![
            Clause::parallel(["x", "y"], ["{xs}", "{ys}"]),
            Clause::new("z", "{zs}"),
        ];
        let sizes = compute_clause_sizes(&parent, &clauses, &HashMap::new()).unwrap();
        // Parallel = one axis (3); z = second axis (2). NOT
        // 3 axes — that would mean the parallel group leaked
        // its internal vars into the lattice.
        assert_eq!(sizes, vec![3, 2]);
    }

    // ---- Lex-default contract --------------------------------

    #[test]
    fn lex_default_emits_rightmost_varies_fastest() {
        // `enumerate_tuples` is documented to emit Cartesian
        // products in lex order with rightmost clause varying
        // fastest. The whole `order:` layer assumes this — if
        // it ever changed silently, halton/extrema/etc. would
        // miscount lattice positions. This test pins the
        // contract.
        let parent = Arc::new(crate::dsl::compile::compile_gk(
            "final xs := \"1, 2\"\nfinal ys := \"10, 20\"\nfinal zs := \"100, 200\"\n"
        ).unwrap());
        let comp = Comprehension::cartesian(vec![
            Clause::new("x", "{xs}"),
            Clause::new("y", "{ys}"),
            Clause::new("z", "{zs}"),
        ]);
        let iter = iterate(
            &comp, &parent, &HashMap::new(),
            Vec::new(), None, false, "test",
        ).unwrap();
        let yielded: Vec<(u64, u64, u64)> = iter
            .map(|child| (
                child.lookup("x").unwrap().as_u64(),
                child.lookup("y").unwrap().as_u64(),
                child.lookup("z").unwrap().as_u64(),
            ))
            .collect();
        // Expected: x outer, y middle, z innermost.
        assert_eq!(yielded, vec![
            (1, 10, 100), (1, 10, 200),
            (1, 20, 100), (1, 20, 200),
            (2, 10, 100), (2, 10, 200),
            (2, 20, 100), (2, 20, 200),
        ]);
    }

    #[test]
    fn iterate_parallel_inside_union_subspace() {
        // Union with two sub-spaces, each containing a
        // parallel-iter clause. Sub-space 0 zips small values
        // (3 steps), sub-space 1 zips big ones (2 steps).
        // Result: 5 tuples = 3 + 2, each emitting both x and y
        // bound from the corresponding sub-space's lockstep.
        use super::super::ast::Subspace;
        let parent = Arc::new(crate::dsl::compile::compile_gk(
            concat!(
                "final small_x := \"1, 2, 3\"\n",
                "final small_y := \"10, 20, 30\"\n",
                "final big_x := \"100, 200\"\n",
                "final big_y := \"1000, 2000\"\n",
            )
        ).unwrap());
        let comp = Comprehension::union_from(vec![
            Subspace::new(vec![
                Clause::parallel(["x", "y"], ["{small_x}", "{small_y}"]),
            ]),
            Subspace::new(vec![
                Clause::parallel(["x", "y"], ["{big_x}", "{big_y}"]),
            ]),
        ]);
        let iter = iterate(
            &comp, &parent, &HashMap::new(),
            Vec::new(), None, false, "test",
        ).unwrap();
        let yielded: Vec<(u64, u64)> = iter
            .map(|child| (
                child.lookup("x").unwrap().as_u64(),
                child.lookup("y").unwrap().as_u64(),
            ))
            .collect();
        assert_eq!(yielded, vec![
            (1, 10), (2, 20), (3, 30),       // sub-space 0
            (100, 1000), (200, 2000),         // sub-space 1
        ]);
    }

    #[test]
    fn iterate_parallel_with_extrema_ordering_treats_group_as_one_axis() {
        // Two-axis lattice: parallel group `(x, y)` = 4 zip
        // steps (one axis), `z` = 3 values (second axis). With
        // `Extrema` ordering, the lattice is 4×3 and the
        // first stratum (corners) covers the four coordinate
        // extremes — proving the parallel group counts as one
        // axis, not two. If parallel were two axes the lattice
        // would be 4×4×3 with 8 corners.
        use super::super::ast::TraversalOrder;
        let parent = Arc::new(crate::dsl::compile::compile_gk(
            "final xs := \"1, 2, 3, 4\"\nfinal ys := \"10, 20, 30, 40\"\nfinal zs := \"100, 200, 300\"\n"
        ).unwrap());
        let comp = Comprehension::cartesian(vec![
            Clause::parallel(["x", "y"], ["{xs}", "{ys}"]),
            Clause::new("z", "{zs}"),
        ]).with_order(TraversalOrder::Extrema { strata: Some(1) });
        let iter = iterate(
            &comp, &parent, &HashMap::new(),
            Vec::new(), None, false, "test",
        ).unwrap();
        let yielded: Vec<(u64, u64, u64)> = iter
            .map(|child| (
                child.lookup("x").unwrap().as_u64(),
                child.lookup("y").unwrap().as_u64(),
                child.lookup("z").unwrap().as_u64(),
            ))
            .collect();
        // Strata=1 ⇒ corners only on the 2-axis (zip-step,
        // z-step) lattice: 4 corners (2 axes × 2 endpoints).
        // (1,10) and (4,40) are the parallel-group extremes;
        // 100 and 300 are the z extremes.
        assert_eq!(yielded.len(), 4, "got {yielded:?}");
        let yielded_set: std::collections::HashSet<_> = yielded.into_iter().collect();
        let expected: std::collections::HashSet<_> = vec![
            (1, 10, 100), (1, 10, 300),
            (4, 40, 100), (4, 40, 300),
        ].into_iter().collect();
        assert_eq!(yielded_set, expected);
    }

    #[test]
    fn iterate_parallel_then_single_emits_cross_product_of_axes() {
        // Parallel group `(x, y)` is one axis; `z` is another.
        // Result is the 3-step zip × 2-step single = 6 tuples.
        let parent = Arc::new(crate::dsl::compile::compile_gk(
            "final xs := \"1, 2, 3\"\nfinal ys := \"10, 20, 30\"\nfinal zs := \"100, 200\"\n"
        ).unwrap());
        let comp = Comprehension::cartesian(vec![
            Clause::parallel(["x", "y"], ["{xs}", "{ys}"]),
            Clause::new("z", "{zs}"),
        ]);
        let iter = iterate(
            &comp, &parent, &HashMap::new(),
            Vec::new(), None, false, "test",
        ).unwrap();
        let yielded: Vec<(u64, u64, u64)> = iter
            .map(|child| (
                child.lookup("x").unwrap().as_u64(),
                child.lookup("y").unwrap().as_u64(),
                child.lookup("z").unwrap().as_u64(),
            ))
            .collect();
        assert_eq!(yielded, vec![
            (1, 10, 100), (1, 10, 200),
            (2, 20, 100), (2, 20, 200),
            (3, 30, 100), (3, 30, 200),
        ]);
    }
}
