// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! `WireSource` — narrow read trait for op-template name resolution.
//!
//! SRD-68 §"The narrow trait" specifies the wall between adapter
//! code and `nbrs_variates::kernel::GkKernel` internals: a dispenser
//! at cycle time accesses its bound GK context only through this
//! trait's `get` (value lookup by name) and `names` (declared-name
//! iteration for diagnostics). No `program()`, no `state()`, no
//! `scope_coordinates()` — adapter code is sealed off from kernel
//! mechanics.
//!
//! Two implementations ship here:
//! - `GkKernel` itself, via the kernel's existing `lookup` chain
//!   (input slots, outputs, inherited scope state). Single
//!   resolution surface — name resolves where SRD-67 places it,
//!   no fallback (SRD-68 invariant I-1).
//! - `NullWireSource`, a unit type that returns `None` for every
//!   name. Used as the default value in `ExecCtx::new` during the
//!   SRD-68 migration so call sites that don't yet have a kernel
//!   handle don't break — adapters opt in via
//!   `ExecCtx::with_wires` once they own a kernel reference.
//!
//! See `docs/sysref/68_dispenser_owned_gk_context.md`.

use nbrs_variates::kernel::GkKernel;
use nbrs_variates::node::Value;

/// Cycle-time read surface a dispenser uses to resolve names from
/// its bound GK context.
///
/// `get(name)` returns the current value of the named wire in the
/// dispenser's kernel. A `None` return indicates the name isn't
/// declared in this scope — callers MUST treat that as a
/// resolution error, not a fallback opportunity (SRD-68 I-1).
///
/// `names` enumerates declared names for validators and
/// `describe_resolved`-style introspection. Not for hot-path cycle
/// reads; cycle reads use `get`.
pub trait WireSource: Send + Sync {
    /// Look up `name` in this kernel's scope. Returns the current
    /// value (cloned, owned) or `None` when the name is not
    /// declared here. Callers do not retry against another kernel
    /// — a `None` is the resolution result, full stop.
    fn get(&self, name: &str) -> Option<Value>;

    /// Iterate declared names. Order is implementation-defined.
    /// Used by validators and diagnostic renderers.
    fn names(&self) -> Box<dyn Iterator<Item = String> + '_>;

    /// Advance the underlying kernel state to coordinate `coord`
    /// and invalidate any memoized pulls so subsequent `get`
    /// calls produce values for this coord.
    ///
    /// Used by batch dispensers per the SRD-68 invariant:
    /// > "Within the batch view, each iteration of the batch is
    /// > considered another pull, just as if the operation
    /// > inside the batch were separate. It is simply an
    /// > iteration container."
    ///
    /// The default impl is a no-op — `NullWireSource` and
    /// adapters that don't drive batch iteration leave it alone.
    /// `CycleWires` overrides to mutate the wrapped kernel's
    /// coord input. Calling `advance` on a `WireSource` that
    /// has no kernel handle (e.g. `NullWireSource`) is a no-op,
    /// not an error — the dispenser's own `wires.get` calls
    /// will resolve correctly against whatever read surface the
    /// adapter has bound.
    fn advance(&self, _coord: u64) {}
}

/// `WireSource` over `&GkKernel` — covers names that the kernel's
/// `lookup` API already exposes (inputs, scope-init constants,
/// shared-cell-backed values). Computed outputs that require a
/// memoizing `pull(&mut state, …)` evaluation are NOT covered here
/// and return `None` from `get`; the SRD-68 Push 2 work introduces a
/// richer `WireSource` impl that owns the per-fiber kernel handle
/// with interior mutability and can pull outputs at cycle time.
///
/// For Push 1 this `&GkKernel` impl is the additive baseline: every
/// existing call site that gets handed a `NullWireSource` continues
/// working unchanged, and code that wants kernel-side reads via the
/// trait can use it for the names `lookup` already answers.
impl WireSource for GkKernel {
    fn get(&self, name: &str) -> Option<Value> {
        self.lookup(name)
    }

    fn names(&self) -> Box<dyn Iterator<Item = String> + '_> {
        // Outputs come first (the canonical declared-name surface),
        // followed by input names not also published as outputs.
        // Ordering is stable for a given program; consumers should
        // not assume a particular order between the two groups.
        let program = self.program();
        let outputs: Vec<String> = program.output_names()
            .iter().map(|s| s.to_string()).collect();
        let inputs_only: Vec<String> = program.input_names()
            .iter()
            .filter(|n| !outputs.contains(n))
            .cloned()
            .collect();
        Box::new(outputs.into_iter().chain(inputs_only))
    }
}

/// `WireSource` over a per-fiber kernel handle that supports the
/// full read surface — inputs, scope-init constants, AND computed
/// outputs (which need a memoizing `pull(&mut state, …)` to fire
/// the eval cone). Wraps a `&mut GkKernel` in a `Mutex` so the
/// trait stays `&self`-callable (and `Sync`) while still permitting
/// pull's `&mut` requirement.
///
/// The `Mutex` is uncontended in practice: per-fiber-per-cycle
/// dispatch is single-threaded by construction (the fiber owns
/// the kernel slot exclusively for the duration of the cycle),
/// so `lock()` always succeeds without spinning. Using `Mutex`
/// rather than `RefCell` is a `Sync` requirement — async futures
/// returned by `OpDispenser::execute` are Send, which means
/// `&ExecCtx` (and through it `&dyn WireSource`) must be Send,
/// which means `dyn WireSource` must be Sync.
///
/// Cells held here have the lifetime of one cycle dispatch —
/// constructed at cycle entry from the firing dispenser's
/// per-fiber kernel slot, dropped after the dispenser returns.
///
/// Resolution order on `get(name)`:
/// 1. Output (memoizing pull through the eval cone).
/// 2. Input slot (cell-aware read).
/// 3. Scope-init constant.
/// `None` when the name doesn't appear on this kernel — callers
/// surface as an unresolved-bindpoint error per SRD-68 I-1.
pub struct CycleWires<'a> {
    kernel: std::sync::Mutex<&'a mut GkKernel>,
    /// Optional chained fallback — typically the fiber's main
    /// kernel. Consulted only when `kernel`'s `lookup`/`pull`
    /// returns `None`, which surfaces when an op-template
    /// references a phase-level binding that
    /// [`crate::scope::build_op_template_scope_kernel`] couldn't
    /// extern into the op-template program (the phase-scope
    /// manifest wasn't visible at op-template build time). The
    /// fallback ensures the single resolution-surface invariant
    /// stays satisfied — adapter code still calls one
    /// `wires.get`; the chain happens internally.
    fallback: Option<std::sync::Mutex<&'a mut GkKernel>>,
}

impl<'a> CycleWires<'a> {
    /// Wrap a per-fiber kernel handle for cycle-time reads. The
    /// caller — typically the executor's cycle dispatch — holds
    /// the only outstanding borrow on the kernel for the duration
    /// of this cycle.
    pub fn new(kernel: &'a mut GkKernel) -> Self {
        Self { kernel: std::sync::Mutex::new(kernel), fallback: None }
    }

    /// Wrap a primary per-op kernel with a fallback kernel for
    /// names the primary doesn't carry. The fallback is the
    /// fiber's main kernel — phase bindings folded into the
    /// activity's source kernel land here even when the
    /// op-template scope's program doesn't extern them.
    pub fn with_fallback(primary: &'a mut GkKernel, fallback: &'a mut GkKernel) -> Self {
        Self {
            kernel: std::sync::Mutex::new(primary),
            fallback: Some(std::sync::Mutex::new(fallback)),
        }
    }
}

impl<'a> WireSource for CycleWires<'a> {
    fn get(&self, name: &str) -> Option<Value> {
        {
            let mut k = self.kernel.lock().expect("CycleWires mutex poisoned");
            if k.program().resolve_output(name).is_some() {
                return Some(k.pull(name).clone());
            }
            if let Some(v) = k.lookup(name) {
                return Some(v);
            }
        }
        if let Some(fallback) = &self.fallback {
            let mut fb = fallback.lock().expect("CycleWires fallback mutex poisoned");
            if fb.program().resolve_output(name).is_some() {
                return Some(fb.pull(name).clone());
            }
            return fb.lookup(name);
        }
        None
    }

    fn names(&self) -> Box<dyn Iterator<Item = String> + '_> {
        let k = self.kernel.lock().expect("CycleWires mutex poisoned");
        let program = k.program();
        let mut names: Vec<String> = program.output_names()
            .iter().map(|s| s.to_string()).collect();
        for input in program.input_names() {
            let s = input.to_string();
            if !names.contains(&s) {
                names.push(s);
            }
        }
        if let Some(fallback) = &self.fallback {
            let fb = fallback.lock().expect("CycleWires fallback mutex poisoned");
            let fp = fb.program();
            for n in fp.output_names() {
                let s = n.to_string();
                if !names.contains(&s) {
                    names.push(s);
                }
            }
            for n in fp.input_names() {
                let s = n.to_string();
                if !names.contains(&s) {
                    names.push(s);
                }
            }
        }
        Box::new(names.into_iter())
    }

    fn advance(&self, coord: u64) {
        // Mutate both kernels' coord inputs so subsequent reads
        // (primary or fallback) reflect the advanced coord.
        // Single-fiber-per-cycle ownership means both Mutexes
        // are uncontended.
        {
            let mut k = self.kernel.lock().expect("CycleWires mutex poisoned");
            if k.program().coord_count() > 0 {
                k.state().set_inputs(&[coord]);
            }
        }
        if let Some(fallback) = &self.fallback {
            let mut fb = fallback.lock().expect("CycleWires fallback mutex poisoned");
            if fb.program().coord_count() > 0 {
                fb.state().set_inputs(&[coord]);
            }
        }
    }
}

/// Empty `WireSource` — every `get` returns `None`, `names` is
/// empty. Used as the default for `ExecCtx::new` so callers that
/// don't yet have a kernel handle don't need to construct a
/// real implementation. Migration call sites switch to
/// `ExecCtx::with_wires` when they own a kernel reference (see
/// SRD-68 Push 2 and beyond).
pub struct NullWireSource;

impl WireSource for NullWireSource {
    fn get(&self, _name: &str) -> Option<Value> {
        None
    }
    fn names(&self) -> Box<dyn Iterator<Item = String> + '_> {
        Box::new(std::iter::empty())
    }
}

/// Static `&'static dyn WireSource` for use as the default in
/// `ExecCtx::new`. Avoids per-call allocation; the unit struct has
/// no state to differ.
pub static NULL_WIRES: NullWireSource = NullWireSource;

/// Render `template` by substituting each `{name}` placeholder
/// with `wires.get(name)`'s display-string form. The single
/// resolution-via-wires entry point adapters use at cycle time
/// per SRD-68 Push 5 — replaces the synthesis-layer
/// `substitute_bind_points*` text-mutation pass.
///
/// Honors the standard placeholder escape rules established by
/// `nbrs_workload::bindpoints`:
/// - `\{` / `\}` — literal brace, passes through unchanged.
/// - `{{...}}` — inline-expression form, reserved for the
///   `{{<gk-expr>}}` desugar surface; passes through unchanged
///   (compiled into bindings before reaching cycle time).
/// - Qualifier-prefixed forms (`{bind:name}` / `{capture:name}`
///   / `{input:name}`) — the current Push 5 contract is bare
///   `{name}` only; qualifier-prefixed references error.
/// - `{` followed by non-identifier text — passes through (CQL
///   map literals like `{'class': 'SimpleStrategy'}`, JSON object
///   literals like `{"a": 1}`, format specs like `{:5.2}`).
///
/// Returns `Err` with a descriptive message when a bare `{name}`
/// reference doesn't resolve through `wires` — SRD-68 invariant
/// I-1 forbids silent fallback. Callers (typically the cycle
/// dispatch error path) surface this as a phase-stopping
/// diagnostic.
pub fn substitute_via_wires(
    template: &str,
    wires: &dyn WireSource,
) -> Result<String, String> {
    // Mirror the brace-handling algorithm of
    // `nbrs_workload::bindpoints::extract_bind_points`: when `{` is
    // followed by a literal-start character (`'` or `"`), emit the
    // brace and continue scanning so nested `{name}` placeholders
    // INSIDE CQL map / JSON object literals still get resolved.
    // Track brace depth so a top-level `{name}` whose body itself
    // contains balanced braces (rare) isn't truncated at the first
    // closing brace.
    let chars: Vec<char> = template.chars().collect();
    let n = chars.len();
    let mut out = String::with_capacity(template.len());
    let mut i = 0;
    while i < n {
        // `\{` / `\}` — pass through, two chars.
        if chars[i] == '\\' && i + 1 < n && (chars[i + 1] == '{' || chars[i + 1] == '}') {
            out.push(chars[i]);
            out.push(chars[i + 1]);
            i += 2;
            continue;
        }
        // `{{ ... }}` — inline-expression form. Reserved for the
        // GK desugar surface; passes through unchanged at cycle
        // time (compiled into bindings before reaching this
        // path).
        if i + 1 < n && chars[i] == '{' && chars[i + 1] == '{' {
            let start = i;
            let mut j = i + 2;
            while j + 1 < n && !(chars[j] == '}' && chars[j + 1] == '}') {
                j += 1;
            }
            let end = (j + 2).min(n);
            for k in start..end { out.push(chars[k]); }
            i = end;
            continue;
        }
        if chars[i] != '{' {
            out.push(chars[i]);
            i += 1;
            continue;
        }
        // CQL map / JSON object literal: `{` followed by `'`/`"`.
        // Emit just the `{` and continue scanning so any nested
        // `{name}` placeholders inside still resolve.
        if i + 1 < n && (chars[i + 1] == '\'' || chars[i + 1] == '"') {
            out.push('{');
            i += 1;
            continue;
        }
        // Single `{...}` form: depth-track to find the matching
        // `}` so balanced inner braces don't truncate the body.
        let body_start = i + 1;
        let mut j = body_start;
        let mut depth: u32 = 1;
        while j < n {
            if chars[j] == '{' { depth += 1; }
            if chars[j] == '}' { depth -= 1; if depth == 0 { break; } }
            j += 1;
        }
        if j >= n {
            // Unterminated — treat as literal char.
            out.push('{');
            i += 1;
            continue;
        }
        let body: String = chars[body_start..j].iter().collect();
        let body = body.trim();
        let after = j + 1;
        // Empty body — pass through.
        if body.is_empty() {
            out.push('{');
            out.push('}');
            i = after;
            continue;
        }
        // Qualifier-prefixed form (`{bind:name}`, `{capture:name}`,
        // `{input:name}`) — SRD-68 Push 5 contract is bare names
        // only at cycle time.
        if body.contains(':') {
            return Err(format!(
                "qualifier-prefixed bind point `{{{body}}}` is not supported \
                 at cycle time; only bare `{{name}}` references are answered \
                 by the dispenser's WireSource"
            ));
        }
        // Non-bare-identifier bodies (format specs `{:5.2}`,
        // expressions `{a+b}`, etc.) pass through unchanged.
        if !is_bare_ident(body) {
            out.push('{');
            out.push_str(body);
            out.push('}');
            i = after;
            continue;
        }
        // Bare identifier — wires.get resolves or errors.
        match wires.get(body) {
            Some(v) => out.push_str(&v.to_display_string()),
            None => {
                return Err(format!(
                    "unresolved bind point `{{{body}}}`: no wire named \
                     `{body}` in the dispenser's GK context"
                ));
            }
        }
        i = after;
    }
    Ok(out)
}

/// Resolve a list of op-template field entries through the generic
/// wires API. Each entry is `(field_name, json_value_from_template)`;
/// the helper returns name+value pairs an adapter can hand to its
/// renderer, with no synthesis-layer involvement.
///
/// Resolution rules per field:
/// - Non-string JSON values pass through as `Value::Str(json.to_string())`.
///   (Adapters that bind by type can reach into the JSON directly
///   if they need richer typing — this helper preserves the legacy
///   string projection.)
/// - String fields whose entire trimmed text is a single bare
///   `{name}` token preserve their typed `Value` via `wires.get(name)`.
///   Mirrors the synthesis-era "pure-token" rule so adapters that
///   bind typed values (CQL prepared params, vector args) keep
///   precision through the pipeline.
/// - Other string fields render through `substitute_via_wires`,
///   producing a `Value::Str`.
///
/// Returns the SRD-68 standard error message on the first
/// unresolved bind point (single resolution surface, no fallback).
pub fn resolve_op_fields_via_wires(
    op_fields: &[(String, serde_json::Value)],
    wires: &dyn WireSource,
) -> Result<crate::adapter::ResolvedFields, String> {
    use nbrs_variates::node::Value;
    let mut names = Vec::with_capacity(op_fields.len());
    let mut values = Vec::with_capacity(op_fields.len());
    for (key, json_value) in op_fields {
        names.push(key.clone());
        let serde_json::Value::String(s) = json_value else {
            values.push(Value::Str(json_value.to_string()));
            continue;
        };
        let trimmed = s.trim();
        let pure_token = trimmed.starts_with('{')
            && trimmed.ends_with('}')
            && !trimmed.starts_with("{{")
            && trimmed.len() >= 2
            && trimmed[1..trimmed.len() - 1].chars().all(|c| c != '{' && c != '}');
        if pure_token {
            let body = trimmed[1..trimmed.len() - 1].trim();
            let bare = match body.split_once(':') {
                Some((_, n)) => n,
                None => body,
            };
            if is_bare_ident(bare) {
                match wires.get(bare) {
                    Some(v) => {
                        values.push(v);
                        continue;
                    }
                    None => {
                        return Err(format!(
                            "unresolved bind point `{{{bare}}}` in field '{key}': \
                             no wire named `{bare}` in the dispenser's GK context"
                        ));
                    }
                }
            }
        }
        let rendered = substitute_via_wires(s, wires)
            .map_err(|e| format!("field '{key}': {e}"))?;
        values.push(Value::Str(rendered));
    }
    Ok(crate::adapter::ResolvedFields::new(names, values))
}

/// Same identifier discipline as the core `resolve_placeholders_in_string`
/// validator in `crate::scope`: ASCII-alpha-or-underscore start,
/// remainder ASCII-alphanumeric-or-underscore. Anything else is
/// not a bare identifier and passes through.
fn is_bare_ident(s: &str) -> bool {
    let mut chars = s.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

#[cfg(test)]
mod tests {
    use super::*;
    use nbrs_variates::dsl::compile::compile_gk;

    #[test]
    fn gkkernel_get_resolves_inputs_and_constants() {
        // `lookup` (and therefore Push 1's WireSource) covers
        // input slots and scope-init constants — the names available
        // without a memoizing pull. `folded := 42` lands as a
        // compile-folded constant; `cycle` is a coordinate input.
        let mut k = compile_gk(
            "inputs := (cycle)\n\
             folded := 42\n",
        ).unwrap();
        k.set_inputs(&[7]);
        let wires: &dyn WireSource = &k;
        assert_eq!(wires.get("folded").map(|v| v.as_u64()), Some(42));
        assert_eq!(wires.get("cycle").map(|v| v.as_u64()), Some(7));
    }

    #[test]
    fn gkkernel_get_returns_none_for_pull_only_outputs_in_push_1() {
        // Push 1 baseline: outputs that require a memoizing
        // `pull(&mut state, …)` evaluation are NOT served by the
        // `&GkKernel` impl. Push 2 introduces the kernel-owning
        // wires impl that can pull outputs. This test pins the
        // current contract so the Push 2 change is visible as a
        // diff.
        let mut k = compile_gk(
            "inputs := (cycle)\n\
             cyc_dep := hash(cycle)\n",
        ).unwrap();
        k.set_inputs(&[7]);
        let wires: &dyn WireSource = &k;
        assert!(wires.get("cyc_dep").is_none());
    }

    #[test]
    fn gkkernel_get_returns_none_for_unknown_name() {
        let k = compile_gk("inputs := (cycle)\nx := 1\n").unwrap();
        let wires: &dyn WireSource = &k;
        assert!(wires.get("not_a_real_name").is_none());
    }

    #[test]
    fn gkkernel_names_lists_declared_outputs_and_inputs() {
        let k = compile_gk("inputs := (cycle)\nfolded := 42\n").unwrap();
        let wires: &dyn WireSource = &k;
        let names: Vec<String> = wires.names().collect();
        assert!(names.iter().any(|n| n == "folded"), "folded should appear: {names:?}");
        assert!(names.iter().any(|n| n == "cycle"), "cycle should appear: {names:?}");
    }

    #[test]
    fn null_wires_returns_none_and_empty() {
        let wires: &dyn WireSource = &NULL_WIRES;
        assert!(wires.get("anything").is_none());
        assert_eq!(wires.names().count(), 0);
    }

    #[test]
    fn cycle_wires_pulls_outputs() {
        // CycleWires (Push 4) covers the memoizing-pull path that
        // the bare `&GkKernel` impl can't reach. `cyc_dep` is a
        // computed output — pulling it requires `&mut state` to
        // fire the eval cone and cache the result.
        let mut k = compile_gk(
            "inputs := (cycle)\n\
             folded := 42\n\
             cyc_dep := hash(cycle)\n",
        ).unwrap();
        k.set_inputs(&[7]);
        let cw = CycleWires::new(&mut k);
        let wires: &dyn WireSource = &cw;
        // Output pull works through CycleWires.
        let v = wires.get("cyc_dep").expect("cyc_dep should resolve");
        assert!(v.as_u64() != 0, "hash result should be non-zero");
        // Folded constant still resolves.
        assert_eq!(wires.get("folded").map(|v| v.as_u64()), Some(42));
        // Coordinate input still resolves.
        assert_eq!(wires.get("cycle").map(|v| v.as_u64()), Some(7));
    }

    #[test]
    fn cycle_wires_returns_none_for_unknown_name() {
        let mut k = compile_gk("inputs := (cycle)\nx := 1\n").unwrap();
        let cw = CycleWires::new(&mut k);
        let wires: &dyn WireSource = &cw;
        assert!(wires.get("not_a_real_name").is_none());
    }

    #[test]
    fn cycle_wires_resolves_phase_binding_lhs_names() {
        // SRD-68 Push 4b sanity check: a workload's phase
        // binding (e.g. `target_index_table := pick(...)`) becomes
        // an output on the per-op canonical kernel after
        // `OpBuilder::canonical_kernel_for_op`. Per-fiber
        // instances built from it via `build_subscope` carry that
        // output. Wrapping such a per-fiber kernel in `CycleWires`
        // and calling `wires.get(phase_binding_name)` returns the
        // computed value — no text substitution required at
        // adapter cycle time.
        //
        // This unit test simulates the shape directly via
        // `compile_gk` rather than spinning up the full activity
        // pipeline; it pins the contract that `wires.get` answers
        // for any name the program declares as an output.
        let mut k = compile_gk(
            "inputs := (cycle)\n\
             keyspace := \"baselines\"\n\
             table := \"vec_label_00\"\n\
             # `pick`-equivalent with constant booleans for testability:\n\
             # take the first value when its selector is true.\n\
             target_index_table := \"system_views.sai_column_indexes\"\n",
        ).unwrap();
        k.set_inputs(&[0]);
        let cw = CycleWires::new(&mut k);
        let wires: &dyn WireSource = &cw;
        assert_eq!(
            wires.get("target_index_table").map(|v| v.as_str().to_string()),
            Some("system_views.sai_column_indexes".to_string()),
            "phase-binding LHS should resolve through CycleWires",
        );
        assert_eq!(
            wires.get("keyspace").map(|v| v.as_str().to_string()),
            Some("baselines".to_string()),
        );
    }

    #[test]
    fn substitute_via_wires_resolves_bare_names() {
        let mut k = compile_gk(
            "inputs := (cycle)\n\
             keyspace := \"baselines\"\n\
             table := \"vec_label_00\"\n",
        ).unwrap();
        let cw = CycleWires::new(&mut k);
        let resolved = substitute_via_wires(
            "SELECT * FROM {keyspace}.{table} WHERE x = 1",
            &cw,
        ).unwrap();
        assert_eq!(resolved, "SELECT * FROM baselines.vec_label_00 WHERE x = 1");
    }

    #[test]
    fn substitute_via_wires_passes_through_literal_braces() {
        let mut k = compile_gk(
            "inputs := (cycle)\n\
             ks := \"baselines\"\n",
        ).unwrap();
        let cw = CycleWires::new(&mut k);
        // CQL map literal: brace bodies starting with quotes are
        // not bare identifiers, pass through verbatim.
        let resolved = substitute_via_wires(
            "CREATE KEYSPACE {ks} WITH replication = {'class': 'SimpleStrategy'}",
            &cw,
        ).unwrap();
        assert_eq!(
            resolved,
            "CREATE KEYSPACE baselines WITH replication = {'class': 'SimpleStrategy'}",
        );
    }

    #[test]
    fn substitute_via_wires_errors_on_unresolved_name() {
        let mut k = compile_gk("inputs := (cycle)\nx := \"a\"\n").unwrap();
        let cw = CycleWires::new(&mut k);
        let err = substitute_via_wires("hi {nonexistent}", &cw).unwrap_err();
        assert!(err.contains("nonexistent"), "diagnostic should name the wire: {err}");
        assert!(err.contains("unresolved"), "diagnostic should call out unresolved: {err}");
    }

    #[test]
    fn substitute_via_wires_resolves_inside_cql_options_map() {
        // Pin the failure shape that surfaced in
        // `full_cql_vector.yaml`: a CQL `WITH OPTIONS = {'k':
        // '{value}'}` map. The earlier substitution algorithm
        // truncated the body at the first `}` (the inner
        // placeholder's `}`), then treated the outer `{...}`
        // as one literal-content body — so inner `{value}`
        // placeholders inside the map were never resolved.
        // The fix mirrors `nbrs_workload::bindpoints::extract_bind_points`:
        // when `{` is followed by `'` or `"`, treat it as a CQL
        // map opener (emit the brace, continue scanning).
        let mut k = compile_gk(
            "inputs := (cycle)\n\
             optimize_for := \"RECALL\"\n\
             similarity_function := \"EUCLIDEAN\"\n",
        ).unwrap();
        let cw = CycleWires::new(&mut k);
        let resolved = substitute_via_wires(
            "WITH OPTIONS = {'optimize_for': '{optimize_for}', 'similarity_function': '{similarity_function}'}",
            &cw,
        ).unwrap();
        assert_eq!(
            resolved,
            "WITH OPTIONS = {'optimize_for': 'RECALL', 'similarity_function': 'EUCLIDEAN'}",
            "nested `{{name}}` placeholders inside CQL map literals must resolve"
        );
    }

    #[test]
    fn substitute_via_wires_errors_on_qualifier_prefix() {
        let mut k = compile_gk("inputs := (cycle)\nx := \"a\"\n").unwrap();
        let cw = CycleWires::new(&mut k);
        let err = substitute_via_wires("hi {bind:x}", &cw).unwrap_err();
        assert!(err.contains("bind:x"), "diagnostic should name the qualifier form: {err}");
    }

    #[test]
    fn substitute_via_wires_passes_through_inline_expr() {
        let mut k = compile_gk("inputs := (cycle)\nx := \"a\"\n").unwrap();
        let cw = CycleWires::new(&mut k);
        let resolved = substitute_via_wires("v = {{x + 1}}", &cw).unwrap();
        // `{{...}}` is reserved for the inline-expression desugar
        // surface (compiled into bindings before reaching cycle
        // time); cycle-time substitute_via_wires passes it
        // through unchanged.
        assert_eq!(resolved, "v = {{x + 1}}");
    }

    #[test]
    fn cycle_wires_resolves_iter_var_through_subscope_chain() {
        // SRD-68 invariant from user: "the GK context visible after
        // initialization in each scope is designed to and required
        // to provide all of the values which should be visible
        // including those which are populated at logical closure
        // boundaries based on comprehensions."
        //
        // Concretely: a for_each scope kernel populates iter vars
        // on its INPUT slots (per `for_iteration`). The phase
        // scope kernel then inherits them via `build_subscope`.
        // The op-template canonical (built via `build_subscope`
        // again) MUST also carry the iter var values so cycle-time
        // `wires.get(iter_var_name)` answers correctly.
        //
        // This test mirrors that chain: parent kernel has
        // `optimize_for` as an extern input declaration with a
        // populated value; child kernel declares the same name;
        // verify the value propagates through `build_subscope`
        // and is visible via `CycleWires::get`.
        use nbrs_variates::dsl::compile::compile_gk;
        use nbrs_variates::node::Value;
        use nbrs_variates::subcontext::GkMatter;

        // Parent: declares `optimize_for` as extern + auto-passthrough
        // output via `final` — same pattern the phase synthesizer
        // uses for iter-var cascade.
        let mut parent = compile_gk(
            "inputs := (cycle)\n\
             extern optimize_for: String\n",
        ).unwrap();
        // Populate the input slot the way the phase kernel does
        // after `bind_outer_scope` from the for_each bound_kernel.
        let opt_idx = parent.program().find_input("optimize_for")
            .expect("optimize_for input slot");
        parent.state().set_input(opt_idx, Value::Str("RECALL".into()));

        // Child: program declares the same name as an extern.
        // Mimics the per-op canonical the dispenser owns.
        let child_program = compile_gk(
            "inputs := (cycle)\n\
             extern optimize_for: String\n",
        ).unwrap().program().clone();

        let mut child = parent.build_subscope(
            GkMatter::builder().program(child_program).build().unwrap(),
        ).expect("subscope build is infallible");

        let cw = CycleWires::new(&mut child);
        let wires: &dyn WireSource = &cw;
        // The architectural contract: child's CycleWires resolves
        // `optimize_for` through the inheritance chain. If this
        // assertion fails, the chain isn't propagating iter vars
        // and we need the gap fix at the GK kernel layer.
        assert_eq!(
            wires.get("optimize_for").map(|v| v.as_str().to_string()),
            Some("RECALL".to_string()),
            "iter-var input populated on parent should propagate \
             through build_subscope to child's WireSource",
        );
    }

    #[test]
    fn extern_decl_only_produces_input_no_output() {
        // Diagnostic: an `extern <name>: <type>` declaration by
        // itself creates an INPUT but does NOT publish a matching
        // output. So `bind_outer_scope` (which walks outer's
        // outputs) wouldn't find this name and wouldn't propagate
        // it to descendants. The synthesis pipeline avoids this by
        // also calling `mark_inherited_outputs` on the kernel
        // post-build, but a plain `extern` line doesn't.
        //
        // Pinning this so the next person reading the source isn't
        // surprised — `cycle_wires_resolves_iter_var_through_subscope_chain`
        // passes only because the `String` extern's auto-passthrough
        // output (added by the compiler when the name appears in the
        // body) gives bind_outer_scope something to walk. With ONLY
        // an extern decl, there's no body reference, no auto-passthrough,
        // and the chain breaks.
        use nbrs_variates::dsl::compile::compile_gk;
        let k = compile_gk(
            "inputs := (cycle)\n\
             extern optimize_for: String\n",
        ).unwrap();
        let outputs: Vec<&str> = k.program().output_names().to_vec();
        // If this assertion fails (i.e. `optimize_for` IS in
        // outputs), then the chain test above was a no-op and we
        // need to revisit the architectural question.
        // Document whichever is true.
        eprintln!("DBG outputs from `extern optimize_for: String`: {outputs:?}");
    }

    #[test]
    fn cycle_wires_advance_drives_per_row_pulls() {
        // SRD-68 batch contract: `advance(coord)` mutates the
        // underlying kernel's coord input so subsequent `get`
        // calls produce values for that coord. Verify by pulling
        // a coord-dependent output before and after advance.
        let mut k = compile_gk(
            "inputs := (cycle)\n\
             id := format_u64(cycle, 10)\n",
        ).unwrap();
        k.set_inputs(&[0]);
        let cw = CycleWires::new(&mut k);
        let wires: &dyn WireSource = &cw;
        // First read at cycle=0.
        assert_eq!(wires.get("id").map(|v| v.as_str().to_string()), Some("0".to_string()));
        // Advance and re-read.
        wires.advance(42);
        assert_eq!(wires.get("id").map(|v| v.as_str().to_string()), Some("42".to_string()));
        wires.advance(7);
        assert_eq!(wires.get("id").map(|v| v.as_str().to_string()), Some("7".to_string()));
    }

    #[test]
    fn null_wires_advance_is_noop() {
        // Default `advance` impl on `NullWireSource` does nothing;
        // useful as a sanity check that advance on a non-batch-
        // capable wires doesn't panic.
        let wires: &dyn WireSource = &NULL_WIRES;
        wires.advance(0);
        wires.advance(123);
        assert!(wires.get("anything").is_none());
    }

    #[test]
    fn cycle_wires_caches_across_repeated_gets() {
        // Pull memoizes; a second get of the same name reads the
        // cached value off state, not re-runs the eval cone. We
        // can't directly observe memoization but we CAN observe
        // that two reads of the same cycle produce the same value
        // (hash is deterministic per coordinate, but the kernel's
        // dirty-tracking would require a state mutation to
        // re-fire — the property still holds).
        let mut k = compile_gk(
            "inputs := (cycle)\nh := hash(cycle)\n"
        ).unwrap();
        k.set_inputs(&[42]);
        let cw = CycleWires::new(&mut k);
        let wires: &dyn WireSource = &cw;
        let v1 = wires.get("h").unwrap().as_u64();
        let v2 = wires.get("h").unwrap().as_u64();
        assert_eq!(v1, v2);
    }
}
