// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! GkProgram: the immutable compiled DAG shared across all fibers.

use std::collections::HashMap;
use std::sync::Arc;

use crate::node::{GkNode, Value};
use super::{WireSource, InputDef};
use super::engines::{GkState, RawState, ProvScanState, EngineCore};
use crate::dsl::ast::{GkFile, Statement};

/// Evaluation lifecycle classification used by the init-binding
/// contract (see [SRD 11 §"Three Evaluation Lifecycles"](../../../../docs/sysref/11_gk_evaluation.md)).
///
/// The variants are *ordered* — `Dynamic > ScopeInit > CompileConst`
/// — so propagation along wires is a `max()` operation: a node's
/// lifecycle is the most-dynamic of its own seed and every upstream
/// node's lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum EvalLifecycle {
    /// Foldable at GK compile time. No dependency on extern slots
    /// or cycle inputs.
    CompileConst,
    /// Foldable at scope activation, after `materialize_wiring_from_outer`
    /// populates iteration externs. Effectively-const for the
    /// duration of one activation.
    ScopeInit,
    /// Re-evaluated on each pull at execution time. Reaches a
    /// graph input (cycle / capture port) or a non-deterministic
    /// source.
    Dynamic,
}

/// Build a diagnostic phrase pinpointing the first wire on a
/// dynamic init-binding's upstream chain that broke the
/// effectively-const contract. Walks one step deep into the
/// node's wiring; for transitive cases the message points at the
/// nearest dynamic source. Best-effort — an unresolvable wire
/// returns a generic message.
fn first_dynamic_wire(
    nodes: &[Box<dyn GkNode>],
    wiring: &[Vec<WireSource>],
    lifecycle: &[EvalLifecycle],
    input_defs: &[InputDef],
    node_idx: usize,
) -> String {
    use crate::kernel::InputKind;
    let owner = nodes[node_idx].meta().name.clone();
    for source in &wiring[node_idx] {
        match source {
            WireSource::Input(idx) => {
                let def = match input_defs.get(*idx) {
                    Some(d) => d,
                    None => continue,
                };
                match def.kind {
                    InputKind::Coordinate => {
                        return format!(
                            "wire on node '{owner}' reaches coordinate input '{}' \
                             (dynamic; changes every cycle)",
                            def.name);
                    }
                    InputKind::CapturePort => {
                        return format!(
                            "wire on node '{owner}' reaches capture port '{}' \
                             (dynamic; mutated by op execution)",
                            def.name);
                    }
                    InputKind::IterationExtern => {} // not the offender
                }
            }
            WireSource::NodeOutput(upstream, _) => {
                if lifecycle[*upstream] == EvalLifecycle::Dynamic {
                    let upstream_name = nodes[*upstream].meta().name.clone();
                    // Detect non-deterministic seed nodes.
                    if wiring[*upstream].is_empty()
                        && (upstream_name == "counter"
                            || upstream_name == "current_epoch_millis"
                            || upstream_name == "session_start_millis"
                            || upstream_name == "elapsed_millis"
                            || upstream_name == "thread_id")
                    {
                        return format!(
                            "wire on node '{owner}' reaches non-deterministic \
                             source '{upstream_name}' (dynamic by construction)");
                    }
                    return format!(
                        "wire on node '{owner}' reaches dynamic node \
                         '{upstream_name}' upstream");
                }
            }
        }
    }
    format!("node '{owner}' is dynamic but the offending wire could not be isolated")
}

/// The immutable compiled DAG. Shared across fibers via `Arc`.
pub struct GkProgram {
    /// Node instances in topological order.
    pub(crate) nodes: Vec<Box<dyn GkNode>>,
    /// For each node, the wiring of its input ports.
    pub(crate) wiring: Vec<Vec<WireSource>>,
    /// All input definitions (coordinates first, then captures).
    input_defs: Vec<InputDef>,
    /// Original source text that produced this program. Arc-shared
    /// so multiple references (diagnostics, describe, debugger) don't
    /// duplicate the string. Empty if constructed programmatically.
    source: Arc<String>,
    /// Diagnostic context describing where this program came from
    /// (e.g., "workload.yaml bindings", "phase rampup (pname=label-1)").
    /// Required on all construction paths — no silent empty contexts.
    context: Arc<String>,
    /// How many of the inputs are coordinate inputs (set via set_inputs(&[u64])).
    /// Inputs at indices [0..coord_count) are coordinates.
    /// Inputs at indices [coord_count..) are capture inputs.
    coord_count: usize,
    /// Map from output variate name to `(node_index, output_port_index)`.
    pub(crate) output_map: HashMap<String, (usize, usize)>,
    /// Outputs in declaration order: (name, node_index, port_index).
    /// Stable ordering for positional access.
    output_list: Vec<(String, usize, usize)>,
    /// Per-node input provenance bitmask. Bit i is set if the node
    /// transitively depends on graph input i.
    pub(crate) input_provenance: Vec<u64>,
    /// Per-input dependent node lists. For each input, the list of
    /// node indices that transitively depend on it.
    input_dependents: Vec<Vec<usize>>,
    /// Output binding modifiers: `shared` or `final`.
    /// Only populated for outputs that have a modifier; absent = default.
    output_modifiers: HashMap<String, crate::dsl::ast::BindingModifier>,
    /// Names exposed by this program *only* to pass them through
    /// the scope chain — not because the scope's own bindings or
    /// specs reference them. Set by intermediate-scope synthesis
    /// (for_each / for_combinations / do-loop) when auto-cascading
    /// workload params or other inherited values: an `extern` is
    /// declared so `materialize_wiring_from_outer` can wire the value, but the
    /// scope itself doesn't *own* the name. Display layers
    /// (scenario tree pre-map, TUI per-scope listing) use this
    /// to distinguish "names defined here" from "names visible
    /// here through inheritance."
    inherited_outputs: std::collections::HashSet<String>,
    /// Source schemas declared in the GK program. The runtime queries
    /// these to discover data sources and their extents.
    cursor_schemas: Vec<crate::source::SourceSchema>,
    /// Names declared with the `init` keyword in the source. Subject
    /// to the init-binding contract (SRD 11 §"Init Binding Contract"):
    /// every name listed here must reach exactly one effectively-const
    /// value at scope-init time. Plan A (compile-time) and Plan B
    /// (scope-activation) checks both consult this set.
    pub(crate) init_outputs: std::collections::HashSet<String>,
    /// Rule 2 write-through bindings produced when this program
    /// was synthesized by the SRD-67 builder's finalize step.
    /// Each entry pairs an export name (a cell-bound input slot
    /// on this program) with the synthetic `__write_<name>`
    /// source output the rewrite emitted.
    ///
    /// Carried on the program — not just on the kernel — so any
    /// kernel built from this program automatically inherits the
    /// bindings. Without this, the per-fiber rebuild path
    /// (`bind_program_under_parent` from a cached program) would
    /// produce a kernel with empty write-throughs and the
    /// per-cycle commit would silently no-op.
    pub(crate) write_throughs: Vec<crate::kernel::KernelWriteThrough>,
    /// Retained AST that produced this program. Live metadata —
    /// read by the subscope synthesizer (SRD-13f §"Wire-reference
    /// classification") to integrate parent bindings' matter
    /// into child scopes. A binding's graph structure may not be
    /// contiguous in source text, so the AST is the canonical
    /// view of what defines each binding. `None` only for
    /// legacy / programmatic construction paths that bypass the
    /// parser; the DSL entry points always populate this.
    pub(crate) ast: Option<Arc<GkFile>>,
}

unsafe impl Send for GkProgram {}
unsafe impl Sync for GkProgram {}

impl std::fmt::Debug for GkProgram {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GkProgram")
            .field("nodes", &self.nodes.len())
            .field("inputs", &self.input_names())
            .field("coord_count", &self.coord_count)
            .finish()
    }
}

impl GkProgram {
    /// Create a program from pre-validated, topologically-sorted components.
    /// All inputs are treated as coordinates.
    #[allow(dead_code)]
    pub(crate) fn new(
        nodes: Vec<Box<dyn GkNode>>,
        wiring: Vec<Vec<WireSource>>,
        input_names: Vec<String>,
        output_map: HashMap<String, (usize, usize)>,
        source: &str,
        context: &str,
    ) -> Self {
        let coord_count = input_names.len();
        let input_defs: Vec<InputDef> = input_names.into_iter()
            .map(|name| InputDef {
                name,
                default: Value::U64(0),
                port_type: crate::node::PortType::U64,
                kind: crate::kernel::InputKind::Coordinate,
            })
            .collect();
        let input_provenance = Self::compute_provenance(&nodes, &wiring);
        let input_dependents = Self::compute_dependents(&input_provenance, input_defs.len());
        // No declaration order available — fall back to sorted by node index
        let fallback_order: Vec<String> = {
            let mut v: Vec<(String, usize, usize)> = output_map.iter()
                .map(|(n, &(ni, pi))| (n.clone(), ni, pi)).collect();
            v.sort_by_key(|(_, ni, pi)| (*ni, *pi));
            v.into_iter().map(|(n, _, _)| n).collect()
        };
        let output_list = Self::build_output_list(&fallback_order, &output_map);
        Self {
            nodes, wiring, input_defs, coord_count, output_map, output_list,
            input_provenance, input_dependents,
            source: Arc::new(source.to_string()),
            context: Arc::new(context.to_string()),
            output_modifiers: HashMap::new(),
            inherited_outputs: std::collections::HashSet::new(),
            cursor_schemas: Vec::new(),
            init_outputs: std::collections::HashSet::new(),
            write_throughs: Vec::new(),
            ast: None,
        }
    }

    /// Create a program with explicit input definitions and output ordering.
    #[allow(dead_code)]
    pub(crate) fn with_inputs(
        nodes: Vec<Box<dyn GkNode>>,
        wiring: Vec<Vec<WireSource>>,
        input_defs: Vec<InputDef>,
        coord_count: usize,
        output_map: HashMap<String, (usize, usize)>,
        output_order: Vec<String>,
        source: &str,
        context: &str,
    ) -> Self {
        let input_provenance = Self::compute_provenance(&nodes, &wiring);
        let input_dependents = Self::compute_dependents(&input_provenance, input_defs.len());
        let output_list = Self::build_output_list(&output_order, &output_map);
        Self {
            nodes, wiring, input_defs, coord_count, output_map, output_list,
            input_provenance, input_dependents,
            source: Arc::new(source.to_string()),
            context: Arc::new(context.to_string()),
            output_modifiers: HashMap::new(),
            inherited_outputs: std::collections::HashSet::new(),
            cursor_schemas: Vec::new(),
            init_outputs: std::collections::HashSet::new(),
            write_throughs: Vec::new(),
            ast: None,
        }
    }

    /// Mark a binding as declared with the `init` keyword. The
    /// init-binding contract (SRD 11) is checked against this set.
    pub(crate) fn mark_init_output(&mut self, name: &str) {
        self.init_outputs.insert(name.to_string());
    }

    /// Set the program's Rule 2 write-through bindings. Called
    /// once by the SRD-67 builder's finalize step right after
    /// compile, while the program Arc is still uniquely owned.
    /// Every kernel built from this program afterwards inherits
    /// the bindings via `from_program`'s automatic seeding.
    pub(crate) fn set_write_throughs(
        &mut self,
        write_throughs: Vec<crate::kernel::KernelWriteThrough>,
    ) {
        self.write_throughs = write_throughs;
    }

    /// Read this program's Rule 2 write-through bindings.
    /// Used by `GkKernel::from_program` to auto-seed the
    /// kernel's `write_throughs` field, so the per-fiber
    /// re-instance path picks them up without a side channel.
    pub(crate) fn write_throughs(&self) -> &[crate::kernel::KernelWriteThrough] {
        &self.write_throughs
    }

    /// Attach the parsed AST as live metadata. Called once by
    /// every DSL compile entry point right after assembly, while
    /// the program Arc is still uniquely owned.
    pub(crate) fn set_ast(&mut self, ast: Arc<GkFile>) {
        self.ast = Some(ast);
    }

    /// The retained AST that produced this program, if any.
    /// SRD-13f §"Wire-reference classification" — the subscope
    /// synthesizer queries this to integrate parent bindings'
    /// graph structure into child scopes. Returns `None` for
    /// programs built via programmatic (non-DSL) paths.
    pub fn ast(&self) -> Option<&Arc<GkFile>> {
        self.ast.as_ref()
    }

    /// Find the `Statement` that defines binding `name` in this
    /// program's retained AST. Matches both single-target
    /// `InitBinding`/`CycleBinding` and tuple-target destructuring
    /// bindings (where `name` is one of several targets). Returns
    /// `None` if no AST is retained or no binding defines `name`.
    pub fn binding_ast_for(&self, name: &str) -> Option<&Statement> {
        let ast = self.ast.as_ref()?;
        ast.statements.iter().find(|stmt| match stmt {
            Statement::InitBinding(b) => b.name == name,
            Statement::CycleBinding(b) => b.targets.iter().any(|t| t == name),
            _ => false,
        })
    }

    /// Compute the transitive closure of bindings needed to
    /// materialise `name` locally in a descendant scope.
    /// SRD-13f §"Wire-reference classification" — case 3 (local
    /// matter inclusion).
    ///
    /// Starting from the binding that defines `name`, recursively
    /// walk the RHS expression tree following `Ident` references.
    /// For each referenced name, if it's defined by another
    /// binding in this program's AST AND is not effectively final
    /// (the four-case rule treats final as a separate cascade),
    /// include that binding too and recurse.
    ///
    /// Termination boundaries:
    /// - `final` / `shared` outputs (effectively const upstream;
    ///   caller emits as promoted-final in case 1)
    /// - `extern` ports (caller handles as case 2 cascade)
    /// - Input slots (`cycle`, etc.)
    /// - Names defined nowhere (will surface as unresolved at
    ///   compile time of the child scope)
    ///
    /// Returns the bindings in topological order (dependencies
    /// first). Names already in `excluded` are not re-walked,
    /// letting callers express "stop here — this name is locally
    /// defined / coordinated / already collected".
    pub fn local_inclusion_chain<'a>(
        &'a self,
        name: &str,
        excluded: &std::collections::HashSet<String>,
    ) -> Vec<&'a Statement> {
        let mut out: Vec<&'a Statement> = Vec::new();
        let mut visited: std::collections::HashSet<String> = excluded.clone();
        self.collect_chain_into(name, &mut out, &mut visited);
        out
    }

    fn collect_chain_into<'a>(
        &'a self,
        name: &str,
        out: &mut Vec<&'a Statement>,
        visited: &mut std::collections::HashSet<String>,
    ) {
        if !visited.insert(name.to_string()) {
            return;
        }
        // `final` / `shared` bindings stop the walk: they're case 1
        // (promoted-final or shared-cell) at the call site, not
        // case 3. Skip silently.
        let modifier = self.output_modifier(name);
        if modifier == crate::dsl::ast::BindingModifier::FINAL
            || modifier == crate::dsl::ast::BindingModifier::SHARED
        {
            return;
        }
        let Some(stmt) = self.binding_ast_for(name) else { return };
        let value = match stmt {
            Statement::CycleBinding(b) => &b.value,
            Statement::InitBinding(b) => &b.value,
            _ => return,
        };
        // Recurse into dependencies first, then push this stmt —
        // produces topo order (deps before dependents).
        let mut refs = std::collections::HashSet::new();
        crate::dsl::validate::collect_references(value, &mut refs);
        let mut refs_sorted: Vec<String> = refs.into_iter().collect();
        refs_sorted.sort();
        for r in refs_sorted {
            self.collect_chain_into(&r, out, visited);
        }
        out.push(stmt);
    }

    /// Read the set of names declared with the `init` keyword.
    pub fn init_outputs(&self) -> &std::collections::HashSet<String> {
        &self.init_outputs
    }

    /// Read the input classification for slot `idx`.
    pub fn input_kind(&self, idx: usize) -> Option<crate::kernel::InputKind> {
        self.input_defs.get(idx).map(|d| d.kind)
    }

    /// Look up `name` in the output map, returning `(node_idx, port_idx)`.
    /// Public surface for the scope-init pass and other consumers
    /// outside the kernel module.
    pub fn output_map_lookup(&self, name: &str) -> Option<&(usize, usize)> {
        self.output_map.get(name)
    }

    /// Iterate every (output-name, (node_idx, port_idx)) pair.
    /// Used by the eval-panic enricher to reverse-resolve which
    /// output(s) a given node feeds when reporting which binding
    /// the panic originated from.
    pub fn output_map_iter(&self) -> impl Iterator<Item = (&String, &(usize, usize))> {
        self.output_map.iter()
    }

    /// Set the binding modifier for a named output.
    pub(crate) fn set_output_modifier(&mut self, name: &str, modifier: crate::dsl::ast::BindingModifier) {
        if modifier != crate::dsl::ast::BindingModifier::NONE {
            self.output_modifiers.insert(name.to_string(), modifier);
        }
    }

    /// Query the binding modifier for a named output.
    pub fn output_modifier(&self, name: &str) -> crate::dsl::ast::BindingModifier {
        self.output_modifiers.get(name).copied()
            .unwrap_or(crate::dsl::ast::BindingModifier::NONE)
    }

    /// Return all output names that have the `shared` modifier.
    pub fn shared_outputs(&self) -> Vec<&str> {
        self.output_modifiers.iter()
            .filter(|(_, m)| **m == crate::dsl::ast::BindingModifier::SHARED)
            .map(|(n, _)| n.as_str())
            .collect()
    }

    /// Mark `name` as an inherited (cascade-propagated) output —
    /// declared on this program only to flow the value through
    /// to descendants via `materialize_wiring_from_outer`, not because this
    /// scope's own bindings or specs reference it.
    pub fn mark_inherited(&mut self, name: &str) {
        self.inherited_outputs.insert(name.to_string());
    }

    /// Is `name` an inherited (cascade-propagated) output? See
    /// [`Self::mark_inherited`].
    pub fn is_inherited(&self, name: &str) -> bool {
        self.inherited_outputs.contains(name)
    }

    /// Return only the outputs *owned* by this program — names
    /// the scope's own bindings, externs, or specs declared,
    /// excluding inherited cascade-propagation outputs. Used by
    /// the scenario tree pre-map and TUI to render per-scope
    /// "what's defined here" without listing every inherited
    /// name. Output order matches `output_names`.
    pub fn own_output_names(&self) -> Vec<&str> {
        self.output_names().into_iter()
            .filter(|name| !self.inherited_outputs.contains(*name))
            .collect()
    }

    /// Return all output names that have the `final` modifier.
    pub fn final_outputs(&self) -> Vec<&str> {
        self.output_modifiers.iter()
            .filter(|(_, m)| **m == crate::dsl::ast::BindingModifier::FINAL)
            .map(|(n, _)| n.as_str())
            .collect()
    }

    /// The original source text that produced this program.
    pub fn source(&self) -> &str { &self.source }

    /// Diagnostic context (e.g., "workload.yaml bindings").
    pub fn context(&self) -> &str { &self.context }

    /// Source schemas declared in this program. The runtime queries
    /// these to discover data sources, their extents, and projections.
    pub fn cursor_schemas(&self) -> &[crate::source::SourceSchema] {
        &self.cursor_schemas
    }

    /// Set source schemas (called by the compiler after processing source declarations).
    pub(crate) fn set_cursor_schemas(&mut self, schemas: Vec<crate::source::SourceSchema>) {
        self.cursor_schemas = schemas;
    }

    /// Build ordered output list from declaration order and the output map.
    fn build_output_list(
        output_order: &[String],
        output_map: &HashMap<String, (usize, usize)>,
    ) -> Vec<(String, usize, usize)> {
        // Use declaration order from the assembler
        let mut list: Vec<(String, usize, usize)> = output_order.iter()
            .filter_map(|name| {
                output_map.get(name).map(|&(ni, pi)| (name.clone(), ni, pi))
            })
            .collect();
        // Add any outputs not in the declaration order (shouldn't happen,
        // but defensive against manual assembler use). Sort the
        // tail by name so the ordering is deterministic across
        // processes — HashMap iteration is per-process-randomised,
        // and a deterministic tail keeps the canonical-program
        // identity (and therefore checkpoint phase-hash) stable
        // across resume invocations.
        let mut tail: Vec<(&String, &(usize, usize))> = output_map.iter()
            .filter(|(name, _)| !output_order.contains(*name))
            .collect();
        tail.sort_by(|a, b| a.0.cmp(b.0));
        for (name, &(ni, pi)) in tail {
            list.push((name.clone(), ni, pi));
        }
        list
    }

    /// Invert provenance into per-input dependent node lists.
    pub(crate) fn compute_dependents(provenance: &[u64], num_inputs: usize) -> Vec<Vec<usize>> {
        let mut deps = vec![Vec::new(); num_inputs];
        for (node_idx, &prov) in provenance.iter().enumerate() {
            for input_idx in 0..num_inputs {
                if prov & (1u64 << input_idx) != 0 {
                    deps[input_idx].push(node_idx);
                }
            }
        }
        deps
    }

    /// Compute per-node input provenance bitmask from the DAG wiring.
    pub(crate) fn compute_provenance(
        nodes: &[Box<dyn GkNode>],
        wiring: &[Vec<WireSource>],
    ) -> Vec<u64> {
        let n = nodes.len();
        let mut prov = vec![0u64; n];
        for i in 0..n {
            for source in &wiring[i] {
                match source {
                    WireSource::Input(idx) => {
                        prov[i] |= 1u64 << idx;
                    }
                    WireSource::NodeOutput(upstream, _) => {
                        prov[i] |= prov[*upstream];
                    }
                }
            }
        }
        prov
    }

    /// Build an EngineCore (shared by all state constructors).
    fn build_engine_core(&self) -> EngineCore {
        let buffers: Vec<Vec<Value>> = self.nodes
            .iter().map(|n| vec![Value::None; n.meta().outs.len()]).collect();
        let node_count = self.nodes.len();
        let inputs: Vec<Value> = self.input_defs.iter()
            .map(|d| d.default.clone()).collect();
        let input_defaults = inputs.clone();
        let max_inputs = self.wiring.iter().map(|w| w.len()).max().unwrap_or(0);
        let input_count = inputs.len();
        EngineCore {
            buffers,
            node_clean: vec![false; node_count],
            inputs, input_defaults,
            shared_cells: vec![None; input_count],
            // SRD-13f Push B.2: cells allocated lazily by
            // `seed_output_cells` (called from kernel
            // constructors). Start with an empty Vec — the
            // seed pass sizes it to match output count.
            output_cells: Vec::new(),
            input_scratch: vec![Value::None; max_inputs],
        }
    }

    /// Create a new evaluation state for this program.
    pub fn create_state(&self) -> GkState {
        let buffers: Vec<Vec<Value>> = self.nodes
            .iter()
            .map(|n| vec![Value::None; n.meta().outs.len()])
            .collect();
        let node_count = self.nodes.len();

        let inputs: Vec<Value> = self.input_defs.iter()
            .map(|d| d.default.clone()).collect();
        let input_defaults = inputs.clone();

        let max_inputs = self.wiring.iter()
            .map(|w| w.len())
            .max()
            .unwrap_or(0);

        let nondeterministic_nodes: Vec<usize> = self.nodes.iter().enumerate()
            .filter(|(i, node)| self.wiring[*i].is_empty() && node.meta().ins.is_empty())
            .map(|(i, _)| i)
            .collect();

        let input_count = inputs.len();
        let core = EngineCore {
            buffers,
            node_clean: vec![false; node_count],
            inputs, input_defaults,
            shared_cells: vec![None; input_count],
            // SRD-13f Push B.2: cells allocated lazily by
            // `seed_output_cells` (called from kernel
            // constructors). Start with an empty Vec — the
            // seed pass sizes it to match output count.
            output_cells: Vec::new(),
            input_scratch: vec![Value::None; max_inputs],
        };

        GkState::from_parts(core, self.input_dependents.clone(), nondeterministic_nodes)
    }

    /// Create a raw state (no provenance). For benchmarking.
    pub fn create_raw_state(&self) -> RawState {
        RawState { core: self.build_engine_core() }
    }

    /// Create the provenance-scan engine state (for benchmarking).
    pub fn create_provscan_state(&self) -> ProvScanState {
        let core = self.build_engine_core();
        let nondeterministic_nodes: Vec<usize> = self.nodes.iter().enumerate()
            .filter(|(i, node)| self.wiring[*i].is_empty() && node.meta().ins.is_empty())
            .map(|(i, _)| i)
            .collect();
        ProvScanState::from_parts(core, self.input_provenance.clone(), nondeterministic_nodes)
    }

    /// Return the names of all inputs.
    pub fn input_names(&self) -> Vec<String> {
        self.input_defs.iter().map(|d| d.name.clone()).collect()
    }

    /// Return the number of coordinate inputs.
    pub fn coord_count(&self) -> usize {
        self.coord_count
    }

    /// Find an input by name. Returns its index.
    pub fn find_input(&self, name: &str) -> Option<usize> {
        self.input_defs.iter().position(|d| d.name == name)
    }

    /// Lookup the declared port type of a named input.
    /// Returns `None` if the name isn't an input of this program.
    pub fn input_port_type(&self, name: &str) -> Option<crate::node::PortType> {
        self.input_defs.iter().find(|d| d.name == name).map(|d| d.port_type)
    }

    /// Number of declared outputs.
    pub fn output_count(&self) -> usize {
        self.output_list.len()
    }

    /// Output name at index (declaration order).
    pub fn output_name(&self, idx: usize) -> &str {
        &self.output_list[idx].0
    }

    /// Return all output names in declaration order.
    pub fn output_names(&self) -> Vec<&str> {
        self.output_list.iter().map(|(n, _, _)| n.as_str()).collect()
    }

    /// Resolve an output name to its (node_index, port_index).
    pub fn resolve_output(&self, name: &str) -> Option<(usize, usize)> {
        self.output_map.get(name).copied()
    }

    /// Resolve an output index to its (node_index, port_index).
    pub fn resolve_output_by_index(&self, idx: usize) -> (usize, usize) {
        let (_, ni, pi) = &self.output_list[idx];
        (*ni, *pi)
    }

    /// Find the output index for a name (for building memoized getters).
    pub(crate) fn output_list(&self) -> &[(String, usize, usize)] {
        &self.output_list
    }

    pub fn output_index(&self, name: &str) -> Option<usize> {
        self.output_list.iter().position(|(n, _, _)| n == name)
    }

    /// Get the provenance bitmask for a node by index.
    pub fn input_provenance_for(&self, node_idx: usize) -> u64 {
        self.input_provenance.get(node_idx).copied().unwrap_or(0)
    }

    /// SRD-13d §3.2: hash-compare two programs for AST /
    /// constant equivalence. Two programs that produce the
    /// same `canonical_hash` are functionally equivalent at
    /// compile time; their runtime instances would differ
    /// only by parent-bound values, which `materialize_wiring_from_outer`
    /// handles. Cheap (one hash compare); doesn't allocate
    /// state. The pre-walker uses this to flatten one scope
    /// into another that materialises identical content.
    pub fn is_equivalent_to(&self, other: &GkProgram) -> bool {
        self.canonical_hash() == other.canonical_hash()
    }

    /// SRD-13d §3.2: "can-flatten?" predicate. Returns true
    /// when this program adds no GK content the parent
    /// program doesn't already supply — i.e. when the inner
    /// scope's contribution is structurally a subset of the
    /// parent's. The pre-walker uses this for nodes that
    /// classified as `GkMatter::Definitions` to detect cases
    /// where the new content turns out to be parent-equivalent
    /// (rare, but correct: a binding that duplicates a parent
    /// declaration is structurally a no-op).
    ///
    /// Current implementation: structural — true when the
    /// inner program has zero outputs and zero inputs beyond
    /// what the parent already exposes. The semantic-
    /// equivalence form (new bindings whose definitions equal
    /// parent bindings) is documented as future work in
    /// SRD-13d §8.2 item 4 (hash normalisation depth).
    pub fn is_subset_of(&self, parent: &GkProgram) -> bool {
        // Equivalent programs flatten trivially.
        if self.is_equivalent_to(parent) {
            return true;
        }
        // The inner program contributes new content if it
        // declares outputs the parent doesn't, or constants
        // / nodes the parent doesn't carry. Cheapest check:
        // an inner program with no outputs of its own and
        // every input also declared by the parent is a
        // structural no-op.
        if !self.output_list.is_empty() {
            return false;
        }
        // Inputs: every name declared by `self` must be
        // declared by `parent` (parent supplies the value).
        // Inner program might have empty input_defs entirely
        // — that's the "trivial wrapper" case and trivially
        // a subset.
        let parent_inputs: std::collections::HashSet<&str> = parent
            .input_defs
            .iter()
            .map(|d| d.name.as_str())
            .collect();
        for d in &self.input_defs {
            if !parent_inputs.contains(d.name.as_str()) {
                return false;
            }
        }
        true
    }

    /// Canonical content-addressable hash of this program.
    ///
    /// SHA-256 over a deterministic byte sequence describing
    /// every node's kind + constant slots, every wiring edge,
    /// and the named input / output declarations. Stable
    /// across compilations of equivalent input — two programs
    /// produced from identical source + identical workload-
    /// scope state hash to the same value, and a change that
    /// affects what the program actually computes (a renamed
    /// output, a new node, a const-slot value change, a
    /// re-routed wire) shifts the hash.
    ///
    /// Used by checkpointing (SRD-44 §"Why hash the compiled
    /// program, not the YAML body") for per-phase identity:
    /// the resume planner skips a phase only when the saved
    /// hash matches the freshly-compiled program's hash, so a
    /// `{dataset}` change that ripples into a phase's
    /// compiled form correctly invalidates that phase's
    /// saved status, while phases whose programs are
    /// unaffected stay skip-eligible.
    ///
    /// ## Determinism contract
    ///
    /// - Outputs are emitted in alphabetical order (not the
    ///   compiler's declaration order, which can shuffle
    ///   slightly across compilation passes).
    /// - For each output, the producing node and its
    ///   transitive input chain are walked in deterministic
    ///   order — wire-source list iterated in port-position
    ///   order, recursion uses the producer's stable
    ///   (already-canonical) hash as the wire reference.
    /// - Const slots are iterated in `NodeMeta.ins` order,
    ///   which is the DSL-declared positional order and is
    ///   compiler-invariant.
    /// - `Input(idx)` wires are translated to the input's
    ///   *name* (stable across runs) rather than its index
    ///   (a compile-time positional choice).
    /// - Floating-point constants hash via their bit
    ///   representation, so 0.0 vs -0.0 hash differently and
    ///   NaNs are distinguishable from each other only by
    ///   their bit pattern (rare but consistent).
    /// Aggregate identity over this program **plus** an outer
    /// chain of ancestor programs (innermost first; the
    /// workload-root program is last). The result is a
    /// SHA-256 over each program's `canonical_hash` in
    /// declaration order, prefixed with a versioned tag so
    /// future reshapings can be detected.
    ///
    /// **Use this when callers need "did anything in scope
    /// change?"** — including upstream bindings that feed
    /// in via auto-extern. `canonical_hash` (the per-program
    /// flavour) covers only this program's own AST and
    /// cannot detect a workload-param edit that lands in a
    /// parent kernel's const slots.
    ///
    /// `canonical_hash` stays a pure local operation (no
    /// kernel-chain dependency); GK refuses to walk parent
    /// scopes inside a per-program hash. The runtime owns
    /// the parent-chain walk and feeds the resulting program
    /// chain here. Callers are responsible for ensuring every
    /// piece of state that should affect identity lives in
    /// some attached GK module — e.g. nbrs injects workload
    /// `params:` as a synthetic root module
    /// (`build_workload_params_kernel`) whose `final` bindings
    /// land in const slots `canonical_hash` covers.
    pub fn instance_hash(&self, ancestors: &[&GkProgram]) -> [u8; 32] {
        use sha2::{Sha256, Digest};
        let mut h = Sha256::new();
        h.update(b"GkProgram-instance-v1\n");
        h.update(self.canonical_hash());
        for a in ancestors {
            h.update(a.canonical_hash());
        }
        let mut out = [0u8; 32];
        out.copy_from_slice(&h.finalize());
        out
    }

    pub fn canonical_hash(&self) -> [u8; 32] {
        use sha2::{Sha256, Digest};
        let mut h = Sha256::new();
        h.update(b"GkProgram-v1\n");

        // Inputs: emit name + kind + port type. Sorted by name
        // for stability — input declaration order is set by
        // the compiler's traversal of the source, which is
        // stable for a given source but can drift across
        // compiler revisions.
        let mut inputs: Vec<(usize, &InputDef)> = self.input_defs.iter().enumerate().collect();
        inputs.sort_by(|a, b| a.1.name.cmp(&b.1.name));
        for (_, def) in &inputs {
            h.update(b"in:");
            h.update(def.name.as_bytes());
            h.update(b":");
            h.update(format!("{:?}", def.port_type).as_bytes());
            h.update(b":");
            h.update(format!("{:?}", def.kind).as_bytes());
            h.update(b"\n");
        }

        // Outputs: alphabetical. For each output, walk the
        // producing node and its input chain depth-first
        // through `node_canonical_hash` (memoised). The
        // stream of (output-name, node-hash) tuples is the
        // canonical "what does this program produce?" form.
        let mut outputs: Vec<&(String, usize, usize)> = self.output_list.iter().collect();
        outputs.sort_by(|a, b| a.0.cmp(&b.0));
        let mut node_hashes: HashMap<usize, [u8; 32]> = HashMap::new();
        for (name, ni, pi) in &outputs {
            let nh = self.node_canonical_hash(*ni, &mut node_hashes);
            h.update(b"out:");
            h.update(name.as_bytes());
            h.update(b":port:");
            h.update(pi.to_le_bytes().as_ref());
            h.update(b":");
            h.update(&nh);
            h.update(b"\n");
            // Output modifier flags (`final`, `shared`,
            // `volatile`) — affect semantic identity. A
            // `shared` slot reads differently than a `final`
            // slot even with the same producing node; a
            // `volatile` mark is part of the workload's
            // identity-decision intent. Emitting individual
            // flag bytes (not Debug-format) so the hash stays
            // stable under struct-field reordering.
            if let Some(m) = self.output_modifiers.get(name.as_str()) {
                h.update(b"  mod:");
                h.update(if m.is_final()    { b"F" } else { b"-" });
                h.update(if m.is_shared()   { b"S" } else { b"-" });
                h.update(if m.is_volatile() { b"V" } else { b"-" });
                h.update(b"\n");
            }
        }

        // Inherited-output set: marks names that pass through
        // this scope without "owning" them. Affects
        // compute_own_coordinates → scope-coordinate
        // attribution → potentially affects observable
        // identity (e.g. label-set keys in metrics).
        let mut inherited: Vec<&String> = self.inherited_outputs.iter().collect();
        inherited.sort();
        for name in inherited {
            h.update(b"inh:");
            h.update(name.as_bytes());
            h.update(b"\n");
        }

        // Init-output set: every name whose producing node is
        // expected to fold to a constant at scope-init time
        // (per SRD-11 §"Init Binding Contract"). A workload
        // edit that promotes a binding from `final` to `init`
        // (or vice versa) changes the eval-lifecycle of the
        // node graph — distinct programs.
        let mut init_outs: Vec<&String> = self.init_outputs.iter().collect();
        init_outs.sort();
        for name in init_outs {
            h.update(b"init:");
            h.update(name.as_bytes());
            h.update(b"\n");
        }

        // Cursor schemas: source declarations carry into the
        // program's compile-time identity (different source
        // bounds = different program).
        for schema in &self.cursor_schemas {
            h.update(b"cursor:");
            h.update(schema.name.as_bytes());
            h.update(b":");
            h.update(format!("{:?}", schema.extent).as_bytes());
            h.update(b"\n");
        }

        h.finalize().into()
    }

    /// Recursive helper: hash a single node's canonical form,
    /// memoising on node index. The hash incorporates the
    /// node's kind (`meta.name`), every const slot's value,
    /// and every wire input — wires to other nodes resolve to
    /// those nodes' canonical hashes, so the result is a
    /// Merkle-tree summary of the producer's full transitive
    /// dependency cone.
    fn node_canonical_hash(
        &self,
        ni: usize,
        memo: &mut HashMap<usize, [u8; 32]>,
    ) -> [u8; 32] {
        if let Some(h) = memo.get(&ni) {
            return *h;
        }
        // Insert a sentinel to handle the (theoretical)
        // cycle case — GK DAGs aren't supposed to cycle, but
        // guarding against an infinite recursion if a future
        // node graph violates that is cheap insurance.
        memo.insert(ni, [0u8; 32]);

        use sha2::{Sha256, Digest};
        let mut h = Sha256::new();
        let meta = self.nodes[ni].meta();
        h.update(b"node:");
        h.update(meta.name.as_bytes());
        h.update(b"\n");

        // Output ports: name + type, in declaration order.
        for port in &meta.outs {
            h.update(b"  outp:");
            h.update(port.name.as_bytes());
            h.update(b":");
            h.update(format!("{:?}", port.typ).as_bytes());
            h.update(b"\n");
        }

        // Input slots in declaration order. For Wire slots,
        // pull the wire-source for that port and resolve it.
        // Const slots inline their value's bytes.
        let wires = &self.wiring[ni];
        let mut wire_idx = 0;
        for slot in &meta.ins {
            match slot {
                crate::node::Slot::Wire(port) => {
                    h.update(b"  wirep:");
                    h.update(port.name.as_bytes());
                    h.update(b":");
                    h.update(format!("{:?}", port.typ).as_bytes());
                    h.update(b":");
                    if let Some(src) = wires.get(wire_idx) {
                        canonical_wire_source(src, self, memo, &mut h);
                    } else {
                        h.update(b"unwired");
                    }
                    h.update(b"\n");
                    wire_idx += 1;
                }
                crate::node::Slot::Const { name, value } => {
                    h.update(b"  const:");
                    h.update(name.as_bytes());
                    h.update(b":");
                    canonical_const_value(value, &mut h);
                    h.update(b"\n");
                }
            }
        }

        let result: [u8; 32] = h.finalize().into();
        memo.insert(ni, result);
        result
    }

    /// Number of nodes in the program.
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Total wire count (sum of all node input edges).
    pub fn wire_count(&self) -> usize {
        self.wiring.iter().map(|w| w.len()).sum()
    }

    /// Average in-degree (wires per node).
    pub fn avg_degree(&self) -> f64 {
        let n = self.nodes.len();
        if n == 0 { return 0.0; }
        self.wire_count() as f64 / n as f64
    }

    /// Access a node's metadata by index.
    pub fn node_meta(&self, idx: usize) -> &crate::node::NodeMeta {
        self.nodes[idx].meta()
    }

    /// Access the wiring for a node by index.
    /// Returns the list of `WireSource`s feeding this node's inputs.
    pub fn node_wiring(&self, idx: usize) -> &[super::WireSource] {
        &self.wiring[idx]
    }

    /// Probe the compile level of a node by index.
    pub fn node_compile_level(&self, idx: usize) -> crate::node::CompileLevel {
        crate::node::compile_level_of(self.nodes[idx].as_ref())
    }

    /// Probe the compile level of the last node.
    pub fn last_node_compile_level(&self) -> crate::node::CompileLevel {
        if self.nodes.is_empty() {
            return crate::node::CompileLevel::Phase1;
        }
        self.node_compile_level(self.nodes.len() - 1)
    }

    /// Fold init-time constant nodes.
    ///
    /// Returns `Err` only when the init-binding contract (SRD 11
    /// §"Init Binding Contract" Plan A) is violated; non-fatal
    /// warnings (config-wire / non-determinism / implicit coercion)
    /// continue to be log-emitted and don't surface here.
    pub fn fold_init_constants(&mut self) -> Result<usize, String> {
        self.fold_init_constants_impl(None, false)
    }

    /// Fold init-time constants, emitting diagnostic events to the log.
    /// Returns `Err` for init-binding contract violations (Plan A).
    pub fn fold_init_constants_with_log(&mut self, log: Option<&mut crate::dsl::events::CompileEventLog>) -> Result<usize, String> {
        self.fold_init_constants_impl(log, false)
    }

    /// Fold init-time constants with strict mode.
    pub fn fold_init_constants_strict(
        &mut self,
        log: Option<&mut crate::dsl::events::CompileEventLog>,
        strict: bool,
    ) -> Result<usize, String> {
        self.fold_init_constants_impl(log, strict)
    }

    fn fold_init_constants_impl(
        &mut self,
        mut log: Option<&mut crate::dsl::events::CompileEventLog>,
        strict: bool,
    ) -> Result<usize, String> {
        use crate::nodes::identity::{ConstU64, ConstStr, ConstHandle};
        use crate::nodes::fixed::ConstF64;
        use crate::node::Value;

        let n = self.nodes.len();
        if n == 0 { return Ok(0); }

        // Phase 1: Classify each node by its evaluation lifecycle.
        // Per SRD 11 §"Three Evaluation Lifecycles": every node is
        // CompileConst, ScopeInit, or Dynamic; the three are
        // ordered (Dynamic dominates ScopeInit dominates
        // CompileConst) and `max()`-propagate downstream.
        //
        // CompileConst: foldable now (no extern / cycle dependencies).
        // ScopeInit:    not foldable now, but will be at scope
        //               activation (depends on iteration externs).
        // Dynamic:      depends on cycle inputs, capture ports, or
        //               non-deterministic sources.
        use crate::kernel::InputKind;
        let mut lifecycle: Vec<EvalLifecycle> = vec![EvalLifecycle::CompileConst; n];

        for (i, wiring) in self.wiring.iter().enumerate() {
            for source in wiring {
                match source {
                    WireSource::Input(idx) => {
                        let kind = self.input_defs.get(*idx).map(|d| d.kind)
                            .unwrap_or(InputKind::Coordinate);
                        let lc = match kind {
                            InputKind::IterationExtern => EvalLifecycle::ScopeInit,
                            InputKind::Coordinate | InputKind::CapturePort => EvalLifecycle::Dynamic,
                        };
                        if lc > lifecycle[i] {
                            lifecycle[i] = lc;
                        }
                    }
                    WireSource::NodeOutput(_, _) => {}
                }
            }
            if wiring.is_empty() {
                let name = &self.nodes[i].meta().name;
                if name == "current_epoch_millis" || name == "session_start_millis"
                    || name == "elapsed_millis" || name == "counter"
                    || name == "thread_id"
                {
                    lifecycle[i] = EvalLifecycle::Dynamic;
                }
            }

            // SRD-13f Push D / SRD-44: `volatile` is the
            // author's explicit declaration that this wire's
            // value is non-deterministic across invocations
            // (e.g. a sequence-next fixture, an env-derived
            // value) and MUST NOT be const-folded into the
            // workload's identity. Mark the producing node as
            // Dynamic so the fold pass leaves it alone — the
            // canonical_hash then sees the node-type + wiring
            // shape but never the value, keeping the workload
            // identity stable across processes (resume-skip
            // identity matching depends on this).
            //
            // Walk output_modifiers directly (not output_list)
            // so volatile bindings DCE-pruned out of the
            // exposed output list still mark their producing
            // node Dynamic — the modifier was the author's
            // intent at the source layer, independent of
            // whether the binding survived DCE as a kernel
            // output.
            if self.output_modifiers.iter().any(|(name, m)| {
                m.is_volatile()
                    && self.output_map.get(name).map(|(ni, _)| *ni == i).unwrap_or(false)
            }) {
                lifecycle[i] = EvalLifecycle::Dynamic;
            }
        }

        // Propagate: a node's lifecycle is the max of its own seed
        // and every upstream NodeOutput's lifecycle. Iterate to
        // fixed point.
        let mut changed = true;
        while changed {
            changed = false;
            for i in 0..n {
                for source in &self.wiring[i] {
                    if let WireSource::NodeOutput(upstream, _) = source
                        && lifecycle[*upstream] > lifecycle[i]
                    {
                        lifecycle[i] = lifecycle[*upstream];
                        changed = true;
                    }
                }
            }
        }

        // is_init is the compile-const subset. Subsequent fold
        // phases below only operate on CompileConst nodes; ScopeInit
        // nodes are deferred to the scope-activation pass.
        let mut is_init: Vec<bool> = lifecycle.iter()
            .map(|lc| *lc == EvalLifecycle::CompileConst)
            .collect();

        // ─── Plan A: Init-Binding Contract (compile-time) ──────────
        //
        // SRD 11 §"Init Binding Contract": every binding declared
        // `init` must reach a single effectively-const value at
        // scope-init time. At compile time, that means: the
        // binding's owning node must classify as CompileConst or
        // ScopeInit — never Dynamic.
        //
        // A Dynamic classification on an init binding is a hard
        // structural error. The diagnostic names the binding and
        // the offending wire. There is no soft fall-through.
        if !self.init_outputs.is_empty() {
            for init_name in &self.init_outputs {
                let Some((node_idx, _)) = self.output_map.get(init_name) else { continue };
                if lifecycle[*node_idx] == EvalLifecycle::Dynamic {
                    let offending = first_dynamic_wire(&self.nodes, &self.wiring, &lifecycle, &self.input_defs, *node_idx);
                    return Err(format!(
                        "init binding '{init_name}' violates the init contract: \
                         {offending} \
                         (init bindings must be effectively-const at scope-init time \
                         per SRD 11 §\"Init Binding Contract\")"
                    ));
                }
            }
        }
        // ─────────────────────────────────────────────────────────────

        // Wire cost check
        for i in 0..n {
            let wire_inputs = self.nodes[i].meta().wire_inputs();
            for (port_idx, wire_source) in self.wiring[i].iter().enumerate() {
                if port_idx >= wire_inputs.len() { break; }
                if wire_inputs[port_idx].wire_cost != crate::node::WireCost::Config {
                    continue;
                }
                let source_is_cycle = match wire_source {
                    WireSource::Input(_) => true,
                    WireSource::NodeOutput(src_idx, _) => !is_init[*src_idx],
                };
                if source_is_cycle {
                    let node_name = &self.nodes[i].meta().name;
                    let port_name = &wire_inputs[port_idx].name;
                    if strict {
                        return Err(format!(
                            "strict mode: config wire '{port_name}' on node '{node_name}' \
                             is connected to a cycle-time source."
                        ));
                    }
                    crate::audit::warn(&format!(
                        "config wire '{port_name}' on node '{node_name}' is connected to a \
                         cycle-time source."
                    ));
                    if let Some(ref mut log) = log {
                        log.push(crate::dsl::events::CompileEvent::ConfigWireCycleWarning {
                            node: node_name.clone(),
                            port: port_name.clone(),
                        });
                    }
                }
            }
        }

        // Non-deterministic node check (per SRD-44 + design memo
        // `resumable_test_fixture.md`). Empty-wiring + not-init +
        // not-internal nodes are structurally-detected as
        // non-deterministic. The `volatile` keyword on a binding
        // wire is the author's explicit acknowledgment — when a
        // node's output feeds into a volatile output, suppress
        // both the strict-mode error and the audit warning.
        //
        // Direct-consumer check only: walks `output_list` looking
        // for outputs that map to this node and checks whether
        // the output's modifier carries `is_volatile`. Suffices
        // for the common pattern `volatile name := source_fn(...)`.
        // Full transitive volatile-taint propagation is a future
        // enhancement keyed to `hash_const`.
        for i in 0..n {
            let name = &self.nodes[i].meta().name;
            let is_nondeterministic = self.wiring[i].is_empty() && !is_init[i]
                && !name.starts_with("__");
            if !is_nondeterministic { continue; }
            let consumed_by_volatile = self.output_list.iter().any(|(out_name, node_idx, _port)| {
                *node_idx == i
                    && self.output_modifiers.get(out_name)
                        .map(|m| m.is_volatile())
                        .unwrap_or(false)
            });
            if consumed_by_volatile {
                continue;
            }
            let msg = format!(
                "non-deterministic node '{name}' used without explicit acknowledgment"
            );
            if strict {
                return Err(format!("strict mode: {msg}. Use a deterministic alternative."));
            }
            crate::audit::warn(&msg);
            if let Some(ref mut log) = log {
                log.push(crate::dsl::events::CompileEvent::Warning { message: msg });
            }
        }

        // Implicit type coercion check
        for i in 0..n {
            let name = &self.nodes[i].meta().name;
            if name.starts_with("__adapt_") {
                let msg = format!(
                    "implicit type coercion via '{name}'. Use explicit conversion function."
                );
                if strict {
                    return Err(format!("strict mode: {msg}"));
                }
                crate::audit::warn(&msg);
                if let Some(ref mut log) = log {
                    log.push(crate::dsl::events::CompileEvent::Warning { message: msg });
                }
            }
        }

        // Unused binding check
        let output_node_indices: std::collections::HashSet<usize> =
            self.output_map.values().map(|(idx, _)| *idx).collect();
        for i in 0..n {
            let name = &self.nodes[i].meta().name;
            if name.starts_with("__") { continue; }
            let is_output = output_node_indices.contains(&i);
            let is_consumed = (0..n).any(|j| {
                self.wiring[j].iter().any(|w| matches!(w, WireSource::NodeOutput(src, _) if *src == i))
            });
            if !is_output && !is_consumed {
                let msg = format!("binding '{name}' is never referenced");
                if strict {
                    return Err(format!("strict mode: {msg}. Remove it or mark as output."));
                }
                if !name.contains("__") {
                    crate::audit::warn(&msg);
                    if let Some(ref mut log) = log {
                        log.push(crate::dsl::events::CompileEvent::Warning { message: msg });
                    }
                }
            }
        }

        let init_count = is_init.iter().filter(|&&b| b).count();
        if init_count == 0 { return Ok(0); }

        // Phase 2: Evaluate init-time nodes.
        let mut state = self.create_state();
        let dummy_inputs = vec![0u64; self.coord_count];
        state.set_inputs(&dummy_inputs);

        for i in 0..n {
            if is_init[i] {
                if self.nodes[i].meta().outs.len() != 1 {
                    is_init[i] = false;
                    continue;
                }
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    state.eval_node_public(self, i);
                }));
                if result.is_err() {
                    let node_name = &self.nodes[i].meta().name;
                    crate::audit::warn(&format!(
                        "constant folding: node '{node_name}' panicked during init-time eval — skipping fold"));
                    is_init[i] = false;
                }
            }
        }

        // Phase 3: Replace init-time nodes with constants.
        let mut folded = 0;
        for i in 0..n {
            if !is_init[i] { continue; }

            let value = state.core.buffers[i][0].clone();
            if matches!(value, Value::None) { continue; }

            let const_node: Box<dyn crate::node::GkNode> = match &value {
                Value::U64(v) => Box::new(ConstU64::new(*v)),
                Value::F64(v) => Box::new(ConstF64::new(*v)),
                Value::Bool(v) => Box::new(ConstU64::new(if *v { 1 } else { 0 })),
                Value::Str(s) => Box::new(ConstStr::new(s.clone())),
                // Handles (e.g. `init prebuffered = dataset_prebuffer(...)`)
                // get a dedicated `ConstHandle` replacement so the original
                // side-effect-bearing node is removed from the program.
                // Without this, every fresh fiber's `GkState` walks the
                // dirty original on first pull and re-fires its eval —
                // producing a per-fiber stampede that exhausts process
                // thread limits when the eval spawns HTTP workers (the
                // exact failure mode that motivates this branch).
                Value::Handle(arc) => {
                    let original_name = self.nodes[i].meta().name.clone();
                    // Per-node compile-time mechanic; one
                    // line per `init` binding pollutes
                    // session output with no actionable
                    // signal for the operator. Demote to
                    // Debug — visible under `--log-level
                    // debug` for compiler-pipeline
                    // inspection, silent on the default
                    // INFO console.
                    crate::audit::debug(&format!(
                        "fold: replacing init node '{original_name}' with ConstHandle \
                         (Arc<dyn Any>) — eval will not re-fire post-fold"));
                    Box::new(ConstHandle::new(arc.clone()))
                }
                _ => continue,
            };

            let node_name = self.nodes[i].meta().name.clone();
            if let Some(ref mut log) = log {
                log.push(crate::dsl::events::CompileEvent::ConstantFolded {
                    node: node_name,
                    value: value.to_display_string(),
                });
            }
            self.nodes[i] = const_node;
            self.wiring[i] = Vec::new();
            folded += 1;
        }

        Ok(folded)
    }
}

/// Hash one [`super::WireSource`] in canonical form. Inputs
/// resolve to their *name* (stable identifier) rather than
/// their positional index. Node-output references recurse via
/// [`GkProgram::node_canonical_hash`].
fn canonical_wire_source(
    src: &super::WireSource,
    program: &GkProgram,
    memo: &mut HashMap<usize, [u8; 32]>,
    h: &mut sha2::Sha256,
) {
    use sha2::Digest;
    match src {
        super::WireSource::Input(idx) => {
            h.update(b"input:");
            if let Some(def) = program.input_defs.get(*idx) {
                h.update(def.name.as_bytes());
            } else {
                h.update(b"<oob>");
            }
        }
        super::WireSource::NodeOutput(ni, pi) => {
            h.update(b"node:");
            let nh = program.node_canonical_hash(*ni, memo);
            h.update(&nh);
            h.update(b":port:");
            h.update(pi.to_le_bytes().as_ref());
        }
    }
}

/// Hash one [`crate::node::ConstValue`] in canonical form.
/// Floats hash via their bit pattern so 0.0 vs -0.0 (and
/// distinct NaN payloads) are distinguishable. Strings and
/// vectors include explicit length tags so concatenation is
/// unambiguous.
fn canonical_const_value(v: &crate::node::ConstValue, h: &mut sha2::Sha256) {
    use crate::node::ConstValue;
    use sha2::Digest;
    match v {
        ConstValue::U64(x) => {
            h.update(b"u64:");
            h.update(x.to_le_bytes().as_ref());
        }
        ConstValue::F64(x) => {
            h.update(b"f64:");
            h.update(x.to_bits().to_le_bytes().as_ref());
        }
        ConstValue::Str(s) => {
            h.update(b"str:");
            h.update((s.len() as u64).to_le_bytes().as_ref());
            h.update(s.as_bytes());
        }
        ConstValue::VecU64(xs) => {
            h.update(b"vu64:");
            h.update((xs.len() as u64).to_le_bytes().as_ref());
            for x in xs {
                h.update(x.to_le_bytes().as_ref());
            }
        }
        ConstValue::VecF64(xs) => {
            h.update(b"vf64:");
            h.update((xs.len() as u64).to_le_bytes().as_ref());
            for x in xs {
                h.update(x.to_bits().to_le_bytes().as_ref());
            }
        }
    }
}

#[cfg(test)]
mod canonical_hash_tests {
    use crate::dsl::compile_gk;

    #[test]
    fn identical_source_produces_identical_hash() {
        let src = "final dataset := \"sift1m\"\nfinal count := 100\n";
        let k1 = compile_gk(src).expect("compile1");
        let k2 = compile_gk(src).expect("compile2");
        assert_eq!(k1.program().canonical_hash(), k2.program().canonical_hash());
    }

    #[test]
    fn different_const_value_changes_hash() {
        let a = compile_gk("final x := 100\n").expect("compile a");
        let b = compile_gk("final x := 101\n").expect("compile b");
        assert_ne!(a.program().canonical_hash(), b.program().canonical_hash(),
            "differing const value must change canonical hash");
    }

    #[test]
    fn different_string_value_changes_hash() {
        let a = compile_gk("final s := \"sift1m\"\n").expect("compile a");
        let b = compile_gk("final s := \"sift10m\"\n").expect("compile b");
        assert_ne!(a.program().canonical_hash(), b.program().canonical_hash(),
            "differing string value must change canonical hash");
    }

    #[test]
    fn renamed_output_changes_hash() {
        // Same RHS, different output name → different program
        // identity. The output map contributes to canonical
        // identity.
        let a = compile_gk("final foo := 42\n").expect("compile a");
        let b = compile_gk("final bar := 42\n").expect("compile b");
        assert_ne!(a.program().canonical_hash(), b.program().canonical_hash(),
            "renamed output must change canonical hash");
    }

    #[test]
    fn comment_only_change_does_not_change_hash() {
        let a = compile_gk("final x := 42\n").expect("compile a");
        let b = compile_gk("# explanatory comment\nfinal x := 42\n# trailing comment\n")
            .expect("compile b");
        assert_eq!(a.program().canonical_hash(), b.program().canonical_hash(),
            "comment-only edits should not affect canonical hash — \
             the AST is what's hashed, not the source bytes");
    }

    #[test]
    fn whitespace_change_does_not_change_hash() {
        let a = compile_gk("final x := 42\n").expect("compile a");
        let b = compile_gk("final  x  :=  42\n\n\n").expect("compile b");
        assert_eq!(a.program().canonical_hash(), b.program().canonical_hash(),
            "whitespace-only edits should not affect canonical hash");
    }

    #[test]
    fn additional_binding_changes_hash() {
        let a = compile_gk("final x := 1\n").expect("compile a");
        let b = compile_gk("final x := 1\nfinal y := 2\n").expect("compile b");
        assert_ne!(a.program().canonical_hash(), b.program().canonical_hash(),
            "added output must change canonical hash");
    }

    // -----------------------------------------------------------
    // instance_hash — aggregates over a parent-chain of programs
    // -----------------------------------------------------------

    #[test]
    fn const_inside_function_call_changes_hash() {
        // The integer literal in `mod(..., N)` lives in a const
        // slot that canonical_hash should cover — even when it's
        // an argument to a function call rather than a top-level
        // `final X := <literal>` binding.
        let a = compile_gk("inputs := (cycle)\nshard := mod(hash(cycle), 8)\n")
            .expect("a");
        let b = compile_gk("inputs := (cycle)\nshard := mod(hash(cycle), 16)\n")
            .expect("b");
        assert_ne!(a.program().canonical_hash(), b.program().canonical_hash(),
            "literal-arg const value must change canonical hash");
    }

    #[test]
    fn instance_hash_with_no_ancestors_differs_from_canonical_hash() {
        // The instance form prefixes a different domain tag, so
        // even with an empty ancestor chain the two flavours are
        // distinguishable. Prevents a caller from accidentally
        // comparing an instance_hash against a canonical_hash
        // and getting a coincidental match.
        let p = compile_gk("final x := 1\n").expect("compile");
        let prog = p.program();
        assert_ne!(prog.instance_hash(&[]), prog.canonical_hash());
    }

    #[test]
    fn instance_hash_changes_when_an_ancestor_program_changes() {
        // Parent A vs B differ only in a const-slot literal —
        // canonical_hash distinguishes them, so instance_hash
        // computed against the same child must distinguish too.
        let parent_a = compile_gk("final ds := \"v1\"\n").expect("a");
        let parent_b = compile_gk("final ds := \"v2\"\n").expect("b");
        let child = compile_gk("final y := 42\n").expect("child");
        let cp = child.program();
        let h_a = cp.instance_hash(&[parent_a.program().as_ref()]);
        let h_b = cp.instance_hash(&[parent_b.program().as_ref()]);
        assert_ne!(h_a, h_b,
            "ancestor const-slot edit must change instance_hash even \
             when the child program is byte-identical");
    }

    #[test]
    fn instance_hash_is_order_sensitive_in_the_chain() {
        // The chain order matters — different scope-tree paths
        // must map to different identities. The hash mixes
        // ancestor[i].canonical_hash() in chain order, so swapping
        // ancestors yields a different result.
        let g = compile_gk("final g := 1\n").expect("g");
        let p = compile_gk("final p := 2\n").expect("p");
        let c = compile_gk("final c := 3\n").expect("c");
        let cp = c.program();
        let chain1 = cp.instance_hash(&[p.program().as_ref(), g.program().as_ref()]);
        let chain2 = cp.instance_hash(&[g.program().as_ref(), p.program().as_ref()]);
        assert_ne!(chain1, chain2);
    }

    #[test]
    fn instance_hash_is_deterministic_across_rebuilds() {
        // Two independent compiles of the same source feeding
        // the same child must produce the same instance_hash.
        let parent_src = "final ds := \"sift1m\"\n";
        let p1 = compile_gk(parent_src).expect("p1");
        let p2 = compile_gk(parent_src).expect("p2");
        let child = compile_gk("final y := 42\n").expect("child");
        let cp = child.program();
        let h1 = cp.instance_hash(&[p1.program().as_ref()]);
        let h2 = cp.instance_hash(&[p2.program().as_ref()]);
        assert_eq!(h1, h2);
    }

    // ── SRD-13d §3.2: is_equivalent_to / is_subset_of ──

    #[test]
    fn is_equivalent_to_identical_programs() {
        let src = "final x := 100\n";
        let a = compile_gk(src).expect("a");
        let b = compile_gk(src).expect("b");
        assert!(a.program().is_equivalent_to(b.program()));
        assert!(b.program().is_equivalent_to(a.program())); // symmetric
    }

    #[test]
    fn is_equivalent_to_differs_when_const_differs() {
        let a = compile_gk("final x := 100\n").expect("a");
        let b = compile_gk("final x := 101\n").expect("b");
        assert!(!a.program().is_equivalent_to(b.program()));
    }

    #[test]
    fn is_subset_of_self_is_true() {
        let p = compile_gk("final x := 1\n").expect("p");
        // A program is trivially a subset of itself (the
        // equivalence shortcut at the top of is_subset_of).
        assert!(p.program().is_subset_of(p.program()));
    }

    #[test]
    fn is_subset_of_distinct_definitions_is_false() {
        // Inner declares a NEW output the parent doesn't —
        // structurally not a subset.
        let parent = compile_gk("final x := 1\n").expect("parent");
        let inner = compile_gk("final y := 2\n").expect("inner");
        assert!(!inner.program().is_subset_of(parent.program()));
    }
}

#[cfg(test)]
mod ast_metadata_tests {
    use crate::dsl::compile_gk;
    use crate::dsl::ast::Statement;

    #[test]
    fn retained_ast_is_present_after_compile() {
        let src = "final dataset := \"sift1m\"\ncount := 100\n";
        let k = compile_gk(src).expect("compile");
        assert!(k.program().ast().is_some(), "AST should be retained on program");
    }

    #[test]
    fn binding_ast_for_finds_init_binding() {
        let src = "init dataset = \"sift1m\"\nratio := 2.5\n";
        let k = compile_gk(src).expect("compile");
        let stmt = k.program().binding_ast_for("dataset")
            .expect("dataset binding should be retrievable");
        match stmt {
            Statement::InitBinding(b) => assert_eq!(b.name, "dataset"),
            other => panic!("expected InitBinding for 'dataset', got {other:?}"),
        }
    }

    #[test]
    fn binding_ast_for_finds_cycle_binding() {
        let src = "count := 42\n";
        let k = compile_gk(src).expect("compile");
        let stmt = k.program().binding_ast_for("count")
            .expect("count binding should be retrievable");
        match stmt {
            Statement::CycleBinding(b) => {
                assert!(b.targets.iter().any(|t| t == "count"),
                    "CycleBinding targets should include 'count'");
            }
            other => panic!("expected CycleBinding for 'count', got {other:?}"),
        }
    }

    #[test]
    fn binding_ast_for_unknown_name_returns_none() {
        let k = compile_gk("final x := 1\n").expect("compile");
        assert!(k.program().binding_ast_for("does_not_exist").is_none());
    }

    #[test]
    fn local_inclusion_chain_includes_transitive_deps() {
        // `bar` depends on `foo`. Both are cycle bindings.
        // The chain for `bar` should include `foo` first, `bar` second.
        let src = "\
foo := hash(cycle)
bar := mod(foo, 100)
";
        let k = compile_gk(src).expect("compile");
        let chain = k.program()
            .local_inclusion_chain("bar", &std::collections::HashSet::new());
        assert_eq!(chain.len(), 2, "expected 2 bindings in chain, got {}", chain.len());
        match chain[0] {
            Statement::CycleBinding(b) => assert!(b.targets.iter().any(|t| t == "foo")),
            _ => panic!("expected foo first"),
        }
        match chain[1] {
            Statement::CycleBinding(b) => assert!(b.targets.iter().any(|t| t == "bar")),
            _ => panic!("expected bar second"),
        }
    }

    #[test]
    fn local_inclusion_chain_stops_at_final() {
        // `seed` is `final` — should NOT appear in the chain
        // (case 1, promoted-final, is the caller's job).
        let src = "\
final seed := 12345
mixed := hash(seed)
";
        let k = compile_gk(src).expect("compile");
        let chain = k.program()
            .local_inclusion_chain("mixed", &std::collections::HashSet::new());
        // Just `mixed` — `seed` is final, walk stops.
        assert_eq!(chain.len(), 1);
        match chain[0] {
            Statement::CycleBinding(b) => assert!(b.targets.iter().any(|t| t == "mixed")),
            _ => panic!("expected mixed"),
        }
    }

    #[test]
    fn local_inclusion_chain_respects_excluded() {
        // If `foo` is already locally satisfied (excluded), walk
        // doesn't include it. `bar` alone should appear.
        let src = "\
foo := hash(cycle)
bar := mod(foo, 100)
";
        let k = compile_gk(src).expect("compile");
        let mut excluded = std::collections::HashSet::new();
        excluded.insert("foo".to_string());
        let chain = k.program()
            .local_inclusion_chain("bar", &excluded);
        assert_eq!(chain.len(), 1);
        match chain[0] {
            Statement::CycleBinding(b) => assert!(b.targets.iter().any(|t| t == "bar")),
            _ => panic!("expected bar"),
        }
    }

    #[test]
    fn local_inclusion_chain_unknown_name_is_empty() {
        let k = compile_gk("final x := 1\n").expect("compile");
        let chain = k.program()
            .local_inclusion_chain("missing", &std::collections::HashSet::new());
        assert!(chain.is_empty());
    }
}
