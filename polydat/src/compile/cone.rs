// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! SRD-105 — cone-level JIT inside the interpreter kernel.
//!
//! At assembly time, maximal cones of JIT-eligible nodes with
//! scalar boundaries collapse into one synthetic [`JitConeNode`]
//! each, compiled to native code via the existing P3 codegen. The
//! cone node is an ordinary `PolydatNode`: the walker, scope
//! chains, shared cells, None propagation, node_clean caching, and
//! the enrich-and-re-raise panic contract all see a plain node.
//!
//! Boundary marshalling is restricted to single-slot scalar port
//! types (U64 / F64 / Bool) in this push; interior fusion follows
//! whatever the P3 classifier accepts. Extraction is recoverable:
//! member nodes move into the cone only after codegen succeeds, so
//! any JIT failure leaves the graph exactly as the interpreter
//! would have compiled it.

use std::sync::atomic::{AtomicU8, Ordering};

/// Engine-mix selection for kernel compilation (SRD-105).
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum JitMode {
    /// Pure interpreter — the escape hatch and differential baseline.
    Off,
    /// Cone extraction with the cost model (fused cones of >= 2 nodes).
    Auto,
    /// Every eligible node joins a cone (threshold 1). Used by the
    /// differential battery and for isolating marshalling regressions.
    Force,
}

/// Process default for [`JitMode`]: `Auto` — polydat compiles
/// every program with the mixed engine by default (SRD-105 Push 3,
/// gated on the differential battery: expression-level in
/// `function_coverage`, workload-level in nbrs `jit_differential`,
/// identity invariance in `cone_tests`). `jit=off` remains the
/// escape hatch and differential baseline.
static DEFAULT_JIT_MODE: AtomicU8 = AtomicU8::new(1);

/// Set the process-default JIT mode used by kernel compiles that
/// carry no per-assembler override.
pub fn set_default_jit_mode(mode: JitMode) {
    let v = match mode {
        JitMode::Off => 0,
        JitMode::Auto => 1,
        JitMode::Force => 2,
    };
    DEFAULT_JIT_MODE.store(v, Ordering::Relaxed);
}

/// Read the process-default JIT mode.
pub fn default_jit_mode() -> JitMode {
    match DEFAULT_JIT_MODE.load(Ordering::Relaxed) {
        1 => JitMode::Auto,
        2 => JitMode::Force,
        _ => JitMode::Off,
    }
}

#[cfg(not(feature = "jit"))]
pub(crate) fn extract_jit_cones(
    _dag: &mut super::assembly::ResolvedDag,
    _mode: JitMode,
) {
}

#[cfg(feature = "jit")]
pub(crate) use jit_impl::extract_jit_cones;

#[cfg(feature = "jit")]
mod jit_impl {
    use super::JitMode;
    use crate::ast::{NodeMeta, PolydatNode, Port, PortType, Purity, Slot, Value};
    use crate::compile::assembly::{PolydatAssembler, ResolvedDag};
    use crate::compile::jit::{classify_node, JitOp};
    use crate::kernel::{InputDef, InputKind, WireSource};
    use std::collections::HashMap;

    /// Cranelift's `JITModule` owns the executable code memory the
    /// cone's function pointer targets; dropping it frees that
    /// memory, so the cone node must keep it alive for the life of
    /// the program. It is never accessed after finalization.
    struct ModuleHolder(#[allow(dead_code)] cranelift_jit::JITModule);
    // Safety: the module is write-once — finalized before the cone
    // node is constructed and never touched again; only the emitted
    // (reentrant) code runs concurrently. Same precedent as
    // `SimdKernels` in compile/jit/simd.rs.
    unsafe impl Send for ModuleHolder {}
    unsafe impl Sync for ModuleHolder {}

    thread_local! {
        /// Per-thread slot scratch shared by every cone node —
        /// programs (and their nodes) are `Arc`-shared across
        /// fibers, so eval must not serialize through per-node
        /// state. Capacity is retained across evals.
        static CONE_SCRATCH: std::cell::RefCell<Vec<u64>> =
            const { std::cell::RefCell::new(Vec::new()) };
    }

    /// A fused subgraph compiled to native code, standing in the
    /// program as one ordinary node (SRD-105).
    pub(crate) struct JitConeNode {
        meta: NodeMeta,
        code_fn: unsafe fn(*const u64, *mut u64),
        total_slots: usize,
        /// Buffer slot per output port, in `meta.outs` order.
        out_slots: Vec<usize>,
        in_types: Vec<PortType>,
        out_types: Vec<PortType>,
        /// The original member nodes — kept alive for the LUT /
        /// constant memory the native code references, and walked
        /// by identity hashing (`fusion_subgraph`).
        members: Vec<Box<dyn PolydatNode>>,
        /// Local member wiring (`Input(i)` = this node's i-th
        /// outer input; `NodeOutput(j, p)` = member j) — the
        /// stored subgraph identity hashing recurses through.
        sub_wiring: Vec<Vec<WireSource>>,
        /// Per output port: (local member index, member port).
        out_ports: Vec<(usize, usize)>,
        _module: ModuleHolder,
    }

    impl PolydatNode for JitConeNode {
        fn meta(&self) -> &NodeMeta {
            &self.meta
        }

        fn fusion_subgraph(&self) -> Option<crate::ast::FusionSubgraph<'_>> {
            Some(crate::ast::FusionSubgraph {
                members: &self.members,
                wiring: &self.sub_wiring,
                out_ports: &self.out_ports,
            })
        }

        fn eval(&self, inputs: &[Value], outputs: &mut [Value]) {
            CONE_SCRATCH.with(|cell| {
                let mut buf = cell.borrow_mut();
                buf.clear();
                buf.resize(self.total_slots, 0);
                for (i, v) in inputs.iter().enumerate() {
                    buf[i] = encode_boundary(v, self.in_types[i], i, &self.meta.name);
                }
                let code_fn = self.code_fn;
                let cp = buf.as_ptr();
                let mp = buf.as_mut_ptr();
                crate::compile::jit::invoke_with_catch(move || unsafe {
                    (code_fn)(cp, mp);
                });
                for (k, slot) in self.out_slots.iter().enumerate() {
                    outputs[k] = decode_boundary(buf[*slot], self.out_types[k]);
                }
            });
        }
    }

    /// `Value` → u64 slot bits at a cone boundary. The assembler
    /// proved the types; a mismatch here means a type-stability
    /// violation upstream, and the panic routes through the
    /// standard eval_node enrichment.
    fn encode_boundary(v: &Value, ty: PortType, port: usize, cone: &str) -> u64 {
        match v {
            Value::U64(x) => *x,
            Value::F64(x) => x.to_bits(),
            Value::Bool(b) => *b as u64,
            other => panic!(
                "cone `{cone}` boundary input [{port}] expected {ty:?}, \
                 got {:?}",
                other.port_type()
            ),
        }
    }

    /// u64 slot bits → `Value` by the declared boundary type.
    fn decode_boundary(bits: u64, ty: PortType) -> Value {
        match ty {
            PortType::F64 => Value::F64(f64::from_bits(bits)),
            PortType::Bool => Value::Bool(bits != 0),
            _ => Value::U64(bits),
        }
    }

    fn scalar_ok(ty: PortType) -> bool {
        matches!(ty, PortType::U64 | PortType::F64 | PortType::Bool)
            && ty.slot_width() == 1
    }

    /// A node may join a cone iff the P3 classifier can lower it,
    /// it is pure, it follows the SRD-74 None rule (so the kernel
    /// guard applies uniformly to the fused cone), and every wire
    /// port is a single-slot scalar this push can marshal.
    fn node_eligible(node: &dyn PolydatNode) -> bool {
        matches!(node.purity(), Purity::Pure)
            && !node.accepts_none_inputs()
            && !matches!(classify_node(node), JitOp::Fallback)
            && node.meta().outs.iter().all(|p| scalar_ok(p.typ))
            && node
                .meta()
                .wire_inputs()
                .iter()
                .all(|p| scalar_ok(p.typ))
    }

    /// SRD 11's three evaluation lifecycles, re-derived here so
    /// extraction can restrict fusion to per-cycle work. Const and
    /// scope-init subgraphs belong to the fold passes (which
    /// evaluate them once); fusing them would demote them to
    /// per-pull native evaluation and — for multi-output cones —
    /// block `fold_init_constants`' single-output replacement,
    /// breaking `get_constant` consumers like `eval_const_expr`.
    #[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
    enum Lc {
        CompileConst,
        ScopeInit,
        Dynamic,
    }

    fn classify_lifecycles(
        dag: &ResolvedDag,
        nodes: &[Box<dyn PolydatNode>],
    ) -> Vec<Lc> {
        let n = dag.wiring.len();
        let mut lc = vec![Lc::CompileConst; n];
        for i in 0..n {
            for src in &dag.wiring[i] {
                if let WireSource::Input(idx) = src {
                    let kind = dag
                        .input_defs
                        .get(*idx)
                        .map(|d| d.kind)
                        .unwrap_or(InputKind::Coordinate);
                    let seed = match kind {
                        InputKind::IterationExtern => Lc::ScopeInit,
                        InputKind::Coordinate | InputKind::ExternalWrite => Lc::Dynamic,
                    };
                    lc[i] = lc[i].max(seed);
                }
            }
            if matches!(nodes[i].purity(), Purity::Nondeterministic { .. }) {
                lc[i] = Lc::Dynamic;
            }
        }
        loop {
            let mut changed = false;
            for i in 0..n {
                for src in &dag.wiring[i] {
                    if let WireSource::NodeOutput(j, _) = src
                        && lc[*j] > lc[i]
                    {
                        lc[i] = lc[*j];
                        changed = true;
                    }
                }
            }
            if !changed {
                break;
            }
        }
        lc
    }

    /// Dedup/lookup key for a boundary wire source.
    fn src_key(src: &WireSource) -> (u8, usize, usize) {
        match src {
            WireSource::Input(i) => (0, *i, 0),
            WireSource::NodeOutput(j, p) => (1, *j, *p),
        }
    }

    struct ConePlan {
        /// Member node indices, ascending (inherits topo order).
        members: Vec<usize>,
        /// Boundary input sources, deduped, in first-use order.
        boundary_in: Vec<WireSource>,
        in_types: Vec<PortType>,
        /// Boundary output ports `(member_idx, port)`, first-use order.
        boundary_out: Vec<(usize, usize)>,
        out_types: Vec<PortType>,
    }

    /// Replace eligible cones in `dag` with compiled cone nodes.
    /// On any per-cone failure the cone's members stay interpreter
    /// nodes; the DAG is always left valid and topologically sorted.
    pub(crate) fn extract_jit_cones(dag: &mut ResolvedDag, mode: JitMode) {
        let min_members = match mode {
            JitMode::Off => return,
            JitMode::Auto => 2,
            JitMode::Force => 1,
        };
        let n = dag.nodes.len();
        if n == 0 {
            return;
        }

        let lifecycles = classify_lifecycles(dag, &dag.nodes);
        let eligible: Vec<bool> = dag
            .nodes
            .iter()
            .zip(&lifecycles)
            .map(|(nd, lc)| *lc == Lc::Dynamic && node_eligible(nd.as_ref()))
            .collect();

        // Connected components over eligible-to-eligible wires.
        let mut parent: Vec<usize> = (0..n).collect();
        fn find(parent: &mut [usize], mut i: usize) -> usize {
            while parent[i] != i {
                parent[i] = parent[parent[i]];
                i = parent[i];
            }
            i
        }
        for i in 0..n {
            if !eligible[i] {
                continue;
            }
            for src in &dag.wiring[i] {
                if let WireSource::NodeOutput(j, _) = src {
                    if eligible[*j] {
                        let (a, b) = (find(&mut parent, i), find(&mut parent, *j));
                        parent[a] = b;
                    }
                }
            }
        }
        let mut components: HashMap<usize, Vec<usize>> = HashMap::new();
        for i in 0..n {
            if eligible[i] {
                components.entry(find(&mut parent, i)).or_default().push(i);
            }
        }
        let mut roots: Vec<usize> = components.keys().copied().collect();
        roots.sort_unstable();

        let mut nodes_opt: Vec<Option<Box<dyn PolydatNode>>> =
            std::mem::take(&mut dag.nodes).into_iter().map(Some).collect();
        let mut cones: Vec<(ConePlan, JitConeNode)> = Vec::new();

        for root in roots {
            let members = &components[&root];
            if members.len() < min_members {
                continue;
            }
            let Some(plan) = plan_cone(dag, members, &nodes_opt) else {
                continue;
            };
            match build_cone(dag, &plan, &mut nodes_opt) {
                Ok(cone) => cones.push((plan, cone)),
                // Members were restored by build_cone; the cone
                // simply stays on the interpreter. Not silent:
                // rejected work is a routed compile event, not a
                // user-facing failure (SRD-105 fallback rule).
                Err(_e) => {}
            }
        }

        if cones.is_empty() {
            dag.nodes = nodes_opt.into_iter().map(Option::unwrap).collect();
            return;
        }
        rebuild(dag, nodes_opt, cones);
    }

    /// Compute the cone's boundaries; `None` rejects the component
    /// (dead outputs, oversized boundary, unmarshalable edge type).
    fn plan_cone(
        dag: &ResolvedDag,
        members: &[usize],
        nodes: &[Option<Box<dyn PolydatNode>>],
    ) -> Option<ConePlan> {
        let is_member = |j: usize| members.binary_search(&j).is_ok();

        let mut boundary_in: Vec<WireSource> = Vec::new();
        let mut in_types: Vec<PortType> = Vec::new();
        let mut seen_in: HashMap<(u8, usize, usize), usize> = HashMap::new();
        for &m in members {
            for src in &dag.wiring[m] {
                let intra = matches!(src, WireSource::NodeOutput(j, _) if is_member(*j));
                if intra {
                    continue;
                }
                let key = src_key(src);
                if seen_in.contains_key(&key) {
                    continue;
                }
                let ty = match src {
                    WireSource::Input(i) => dag.input_defs[*i].port_type,
                    WireSource::NodeOutput(j, p) => {
                        nodes[*j].as_ref()?.meta().outs[*p].typ
                    }
                };
                if !scalar_ok(ty) {
                    return None;
                }
                seen_in.insert(key, boundary_in.len());
                boundary_in.push(src.clone());
                in_types.push(ty);
            }
        }
        // SRD-105: cones are bounded at 64 boundary inputs (one
        // provenance word) so Pull-variant cones remain reachable
        // without a re-split.
        if boundary_in.len() > 64 {
            return None;
        }
        // A cone with no boundary inputs is a compile-time
        // constant: it would evaluate exactly once (node_clean)
        // and belongs to const folding, not per-cycle fusion.
        // It also breaks lifecycle analysis (a no-input node
        // claiming per-cycle outputs). Leave it interpreted.
        if boundary_in.is_empty() {
            return None;
        }

        let mut boundary_out: Vec<(usize, usize)> = Vec::new();
        let mut seen_out: HashMap<(usize, usize), usize> = HashMap::new();
        let mut note_out = |j: usize, p: usize| {
            if let std::collections::hash_map::Entry::Vacant(e) = seen_out.entry((j, p)) {
                e.insert(boundary_out.len());
                boundary_out.push((j, p));
            }
        };
        for (i, wiring) in dag.wiring.iter().enumerate() {
            if is_member(i) {
                continue;
            }
            for src in wiring {
                if let WireSource::NodeOutput(j, p) = src {
                    if is_member(*j) {
                        note_out(*j, *p);
                    }
                }
            }
        }
        for (j, p) in dag.output_map.values() {
            if is_member(*j) {
                note_out(*j, *p);
            }
        }
        if boundary_out.is_empty() {
            return None;
        }
        let out_types: Vec<PortType> = boundary_out
            .iter()
            .map(|(j, p)| nodes[*j].as_ref().map(|nd| nd.meta().outs[*p].typ))
            .collect::<Option<_>>()?;
        if out_types.iter().any(|t| !scalar_ok(*t)) {
            return None;
        }

        Some(ConePlan {
            members: members.to_vec(),
            boundary_in,
            in_types,
            boundary_out,
            out_types,
        })
    }

    fn default_for(ty: PortType) -> Value {
        match ty {
            PortType::F64 => Value::F64(0.0),
            PortType::Bool => Value::Bool(false),
            _ => Value::U64(0),
        }
    }

    /// Attempt native compilation of the planned cone. Codegen runs
    /// before the members leave the graph permanently: on any error
    /// they are restored and the caller keeps the interpreter form.
    fn build_cone(
        dag: &ResolvedDag,
        plan: &ConePlan,
        nodes: &mut [Option<Box<dyn PolydatNode>>],
    ) -> Result<JitConeNode, String> {
        let local: HashMap<usize, usize> = plan
            .members
            .iter()
            .enumerate()
            .map(|(l, &g)| (g, l))
            .collect();
        let in_pos: HashMap<(u8, usize, usize), usize> = plan
            .boundary_in
            .iter()
            .enumerate()
            .map(|(i, s)| (src_key(s), i))
            .collect();

        let sub_wiring: Vec<Vec<WireSource>> = plan
            .members
            .iter()
            .map(|&m| {
                dag.wiring[m]
                    .iter()
                    .map(|src| match src {
                        WireSource::NodeOutput(j, p) if local.contains_key(j) => {
                            WireSource::NodeOutput(local[j], *p)
                        }
                        other => WireSource::Input(in_pos[&src_key(other)]),
                    })
                    .collect()
            })
            .collect();
        let sub_input_defs: Vec<InputDef> = plan
            .in_types
            .iter()
            .enumerate()
            .map(|(i, ty)| InputDef {
                name: format!("c{i}"),
                default: default_for(*ty),
                port_type: *ty,
                kind: InputKind::Coordinate,
            })
            .collect();
        let mut sub_output_map: HashMap<String, (usize, usize)> = HashMap::new();
        let mut sub_output_order: Vec<String> = Vec::new();
        for (k, (j, p)) in plan.boundary_out.iter().enumerate() {
            let name = format!("o{k}");
            sub_output_map.insert(name.clone(), (local[j], *p));
            sub_output_order.push(name);
        }

        let taken: Vec<Box<dyn PolydatNode>> = plan
            .members
            .iter()
            .map(|&m| nodes[m].take().expect("cone member present"))
            .collect();
        let member_label = cone_label(&taken);

        let sub = ResolvedDag {
            nodes: taken,
            wiring: sub_wiring,
            input_defs: sub_input_defs,
            coord_count: plan.boundary_in.len(),
            output_map: sub_output_map,
            output_order: sub_output_order,
            source: String::new(),
            context: member_label.clone(),
            output_modifiers: HashMap::new(),
            const_outputs: std::collections::HashSet::new(),
        };

        let restore = |sub_nodes: Vec<Box<dyn PolydatNode>>,
                       nodes: &mut [Option<Box<dyn PolydatNode>>]| {
            for (&m, nd) in plan.members.iter().zip(sub_nodes) {
                nodes[m] = Some(nd);
            }
        };

        let layout = match PolydatAssembler::build_jit_layout(&sub) {
            Ok(l) => l,
            Err(e) => {
                restore(sub.nodes, nodes);
                return Err(e);
            }
        };
        let (coord_count, total_slots, jit_steps, jit_outputs) = layout;
        debug_assert_eq!(coord_count, plan.boundary_in.len());
        let compiled = crate::compile::jit::compile_jit_entry(&jit_steps);
        let (code_fn, module) = match compiled {
            Ok(pair) => pair,
            Err(e) => {
                restore(sub.nodes, nodes);
                return Err(e);
            }
        };

        let out_slots: Vec<usize> = (0..plan.boundary_out.len())
            .map(|k| jit_outputs[&format!("o{k}")])
            .collect();
        // Port metadata mirrors the fused subgraph rather than
        // being synthesized: outputs clone the member's original
        // port (lifecycle analysis and downstream diagnostics see
        // what the interpreter form would have declared); inputs
        // clone the source port where one exists (graph inputs are
        // per-cycle by definition).
        let meta = NodeMeta {
            name: member_label,
            ins: plan
                .boundary_in
                .iter()
                .zip(&plan.in_types)
                .enumerate()
                .map(|(i, (src, ty))| {
                    // Boundary producers are ineligible nodes by
                    // definition, so they are never cone members
                    // and always present in the slot vec.
                    let mut port = match src {
                        WireSource::NodeOutput(j, p) => nodes[*j]
                            .as_ref()
                            .map(|nd| nd.meta().outs[*p].clone())
                            .unwrap_or_else(|| Port::new("", *ty)),
                        WireSource::Input(_) => Port::new("", *ty),
                    };
                    port.name = format!("c{i}");
                    port.constraint = None;
                    Slot::Wire(port)
                })
                .collect(),
            outs: plan
                .boundary_out
                .iter()
                .enumerate()
                .map(|(k, (j, p))| {
                    let mut port = sub.nodes[local[j]].meta().outs[*p].clone();
                    port.name = format!("o{k}");
                    port.constraint = None;
                    port
                })
                .collect(),
        };
        let out_ports: Vec<(usize, usize)> = plan
            .boundary_out
            .iter()
            .map(|(j, p)| (local[j], *p))
            .collect();
        Ok(JitConeNode {
            meta,
            code_fn,
            total_slots,
            out_slots,
            in_types: plan.in_types.clone(),
            out_types: plan.out_types.clone(),
            members: sub.nodes,
            sub_wiring: sub.wiring,
            out_ports,
            _module: ModuleHolder(module),
        })
    }

    /// Diagnostic name carrying the fused members, so an enriched
    /// eval panic attributes the interior functions.
    fn cone_label(members: &[Box<dyn PolydatNode>]) -> String {
        const SHOWN: usize = 6;
        let names: Vec<&str> = members
            .iter()
            .take(SHOWN)
            .map(|n| n.meta().name.as_str())
            .collect();
        let suffix = if members.len() > SHOWN {
            format!("+{} more", members.len() - SHOWN)
        } else {
            String::new()
        };
        format!("jit_cone[{}{}]", names.join("+"), suffix)
    }

    /// Splice the compiled cones into the DAG and restore
    /// topological order.
    fn rebuild(
        dag: &mut ResolvedDag,
        nodes_opt: Vec<Option<Box<dyn PolydatNode>>>,
        cones: Vec<(ConePlan, JitConeNode)>,
    ) {
        let old_n = nodes_opt.len();
        // (old_idx, port) → (cone_ordinal, cone_out_port)
        let mut cone_port: HashMap<(usize, usize), (usize, usize)> = HashMap::new();
        for (ci, (plan, _)) in cones.iter().enumerate() {
            for (k, (j, p)) in plan.boundary_out.iter().enumerate() {
                cone_port.insert((*j, *p), (ci, k));
            }
        }

        let mut kept_map: HashMap<usize, usize> = HashMap::new();
        let mut new_nodes: Vec<Box<dyn PolydatNode>> = Vec::new();
        let mut new_wiring: Vec<Vec<WireSource>> = Vec::new();
        for (old, slot) in nodes_opt.into_iter().enumerate() {
            if let Some(node) = slot {
                kept_map.insert(old, new_nodes.len());
                new_nodes.push(node);
                new_wiring.push(dag.wiring[old].clone());
            }
        }
        let cone_base = new_nodes.len();
        let mut cone_plans: Vec<ConePlan> = Vec::with_capacity(cones.len());
        for (plan, cone) in cones {
            new_nodes.push(Box::new(cone));
            new_wiring.push(plan.boundary_in.clone());
            cone_plans.push(plan);
        }

        let remap = |src: &WireSource| -> WireSource {
            match src {
                WireSource::Input(i) => WireSource::Input(*i),
                WireSource::NodeOutput(j, p) => {
                    if let Some(&nj) = kept_map.get(j) {
                        WireSource::NodeOutput(nj, *p)
                    } else {
                        let (ci, k) = cone_port[&(*j, *p)];
                        WireSource::NodeOutput(cone_base + ci, k)
                    }
                }
            }
        };
        for wiring in new_wiring.iter_mut() {
            for src in wiring.iter_mut() {
                *src = remap(src);
            }
        }
        let mut new_output_map: HashMap<String, (usize, usize)> = HashMap::new();
        for (name, (j, p)) in dag.output_map.iter() {
            let (nj, np) = match remap(&WireSource::NodeOutput(*j, *p)) {
                WireSource::NodeOutput(a, b) => (a, b),
                WireSource::Input(_) => unreachable!("outputs map to nodes"),
            };
            new_output_map.insert(name.clone(), (nj, np));
        }

        // Kahn topo sort — consumers of cone interiors may sit at
        // indices below the spliced cone node.
        let m = new_nodes.len();
        let mut indegree = vec![0usize; m];
        let mut dependents: Vec<Vec<usize>> = vec![Vec::new(); m];
        for (i, wiring) in new_wiring.iter().enumerate() {
            let mut producers: Vec<usize> = wiring
                .iter()
                .filter_map(|s| match s {
                    WireSource::NodeOutput(j, _) => Some(*j),
                    WireSource::Input(_) => None,
                })
                .collect();
            producers.sort_unstable();
            producers.dedup();
            indegree[i] = producers.len();
            for j in producers {
                dependents[j].push(i);
            }
        }
        let mut order: Vec<usize> = Vec::with_capacity(m);
        let mut ready: std::collections::BinaryHeap<std::cmp::Reverse<usize>> = (0..m)
            .filter(|&i| indegree[i] == 0)
            .map(std::cmp::Reverse)
            .collect();
        while let Some(std::cmp::Reverse(i)) = ready.pop() {
            order.push(i);
            for &d in &dependents[i] {
                indegree[d] -= 1;
                if indegree[d] == 0 {
                    ready.push(std::cmp::Reverse(d));
                }
            }
        }
        assert_eq!(
            order.len(),
            m,
            "cone splice must not introduce a cycle (old_n={old_n})"
        );
        let mut pos = vec![0usize; m];
        for (new_idx, &i) in order.iter().enumerate() {
            pos[i] = new_idx;
        }

        let mut sorted_nodes: Vec<Option<Box<dyn PolydatNode>>> =
            new_nodes.into_iter().map(Some).collect();
        dag.nodes = order
            .iter()
            .map(|&i| sorted_nodes[i].take().expect("each node placed once"))
            .collect();
        dag.wiring = order
            .iter()
            .map(|&i| {
                new_wiring[i]
                    .iter()
                    .map(|s| match s {
                        WireSource::Input(k) => WireSource::Input(*k),
                        WireSource::NodeOutput(j, p) => WireSource::NodeOutput(pos[*j], *p),
                    })
                    .collect()
            })
            .collect();
        dag.output_map = new_output_map
            .into_iter()
            .map(|(name, (j, p))| (name, (pos[j], p)))
            .collect();
        let _ = cone_plans;
    }
}
