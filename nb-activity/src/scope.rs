// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Typed binding scope model for GK kernel compilation.
//!
//! A `BindingScope` is the structured intermediate representation
//! that replaces raw string manipulation for scope composition.
//! Every binding carries its provenance (`BindingOrigin`), its
//! modifier, and its definition text. Scope rules are checked
//! against this typed structure, and a single deduplicated GK
//! source string is emitted at the end.

use std::collections::{HashMap, HashSet};

use nb_workload::model::{BindingsDef, ParsedOp};

/// Where a binding was declared — its provenance in the scope chain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BindingOrigin {
    /// Declared at workload level, inherited by this phase via
    /// `merge_bindings` during YAML parsing.
    Inherited,
    /// Declared at phase level (the phase has its own `bindings:` block).
    Phase,
    /// Declared at op level (op-specific augmentation).
    Op(String),
    /// Injected as a `for_each` iteration variable.
    IterationVar,
    /// Generated as an `extern` from the outer scope manifest.
    AutoExtern,
    /// Injected from workload param expansion.
    ParamExpansion,
    /// Generated from inline expression extraction (`{{expr}}`).
    InlineExpr,
}

impl std::fmt::Display for BindingOrigin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Inherited => write!(f, "inherited"),
            Self::Phase => write!(f, "phase"),
            Self::Op(name) => write!(f, "op '{name}'"),
            Self::IterationVar => write!(f, "iteration variable"),
            Self::AutoExtern => write!(f, "auto-extern"),
            Self::ParamExpansion => write!(f, "param expansion"),
            Self::InlineExpr => write!(f, "inline expression"),
        }
    }
}

/// The modifier on a binding declaration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScopeModifier {
    None,
    Init,
    Shared,
    Final,
    Cursor,
}

/// A single binding declaration with provenance.
#[derive(Debug, Clone)]
pub struct ScopedBinding {
    /// The binding name (LHS of `:=`).
    pub name: String,
    /// The full declaration line as GK source text.
    /// For regular bindings: `"name := expr"`
    /// For init: `"init name = \"value\""`
    /// For externs: `"extern name: Type"`
    pub line: String,
    /// Where this binding came from.
    pub origin: BindingOrigin,
    /// Modifier on the declaration.
    pub modifier: ScopeModifier,
}

/// A typed extern declaration.
#[derive(Debug, Clone)]
pub struct ExternDecl {
    pub name: String,
    pub type_name: String,
}

/// Typed scope for a phase's GK kernel compilation.
///
/// Built by the executor from structured inputs, validated for
/// scope rules, then emitted as a single GK source string.
pub struct BindingScope {
    /// The coordinate declaration (e.g., `"inputs := (cycle)"`).
    coordinates: Option<String>,
    /// All bindings in insertion order.
    bindings: Vec<ScopedBinding>,
    /// Extern declarations.
    externs: Vec<ExternDecl>,
    /// Names referenced by op templates (for DCE).
    required_outputs: Vec<String>,
    /// Extra required outputs from config expressions.
    config_refs: Vec<String>,
}

impl BindingScope {
    /// Create an empty scope.
    pub fn new() -> Self {
        Self {
            coordinates: None,
            bindings: Vec::new(),
            externs: Vec::new(),
            required_outputs: Vec::new(),
            config_refs: Vec::new(),
        }
    }

    /// Ingest bindings from a `BindingsDef::GkSource`, classifying each
    /// line by the given origin. Extracts coordinates and handles all
    /// GK declaration forms (init, shared, final, cursor, extern, plain).
    pub fn ingest_gk_source(&mut self, source: &str, origin: BindingOrigin) {
        for line in source.lines() {
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with("//") || trimmed.starts_with('#') {
                continue;
            }

            // Cursor declarations use `=` not `:=`:
            //   cursor row = range(0, vector_count("example"))
            //   init prebuffer = dataset_prebuffer("example")
            // These are GK statements that the compiler handles directly.
            // Pass them through as bindings so they survive emission.
            if trimmed.starts_with("cursor ") || trimmed.starts_with("init ") {
                if let Some(eq_pos) = trimmed.find('=') {
                    // Check it's `=` not `:=`
                    let before_eq = &trimmed[..eq_pos];
                    if !before_eq.ends_with(':') {
                        // cursor/init with bare `=` — pass through as-is
                        let lhs = before_eq.trim();
                        let (modifier, name) = parse_modifier_and_name(lhs);
                        self.bindings.push(ScopedBinding {
                            name: name.to_string(),
                            line: trimmed.to_string(),
                            origin: origin.clone(),
                            modifier,
                        });
                        continue;
                    }
                }
            }

            if let Some(pos) = trimmed.find(":=") {
                let lhs = trimmed[..pos].trim();

                // Coordinate declarations
                if lhs == "inputs" || lhs == "coordinates" {
                    self.coordinates = Some(trimmed.to_string());
                    continue;
                }

                // Extern declarations
                if lhs.starts_with("extern") {
                    // Already handled via add_extern; skip inline externs
                    // from inherited sources
                    continue;
                }

                // Determine modifier and extract bare name
                let (modifier, name) = parse_modifier_and_name(lhs);

                self.bindings.push(ScopedBinding {
                    name: name.to_string(),
                    line: trimmed.to_string(),
                    origin: origin.clone(),
                    modifier,
                });
            } else if trimmed.starts_with("extern ") {
                // `extern name: Type` (no `:=`)
                if let Some(colon_pos) = trimmed.find(':') {
                    let name = trimmed["extern ".len()..colon_pos].trim();
                    let type_name = trimmed[colon_pos + 1..].trim();
                    self.externs.push(ExternDecl {
                        name: name.to_string(),
                        type_name: type_name.to_string(),
                    });
                }
            }
            // Lines that don't match any pattern are silently skipped.
            // Comments and blank lines are already filtered above.
        }
    }

    /// Add an iteration variable from `for_each`.
    pub fn add_iteration_var(&mut self, name: &str, value: &str) {
        self.bindings.push(ScopedBinding {
            name: name.to_string(),
            line: format!("init {name} = \"{value}\""),
            origin: BindingOrigin::IterationVar,
            modifier: ScopeModifier::Init,
        });
    }

    /// Add an auto-extern declaration from the outer scope manifest.
    pub fn add_extern(&mut self, name: &str, type_name: &str) {
        self.externs.push(ExternDecl {
            name: name.to_string(),
            type_name: type_name.to_string(),
        });
    }

    /// Add a workload param binding.
    pub fn add_param_binding(&mut self, name: &str, value: &str) {
        let line = if value.parse::<u64>().is_ok() || value.parse::<f64>().is_ok() {
            format!("{name} := {value}")
        } else {
            format!("{name} := \"{value}\"")
        };
        self.bindings.push(ScopedBinding {
            name: name.to_string(),
            line,
            origin: BindingOrigin::ParamExpansion,
            modifier: ScopeModifier::None,
        });
    }

    /// Add an inline expression binding.
    pub fn add_inline_expr(&mut self, name: &str, expr: &str) {
        self.bindings.push(ScopedBinding {
            name: name.to_string(),
            line: format!("{name} := {expr}"),
            origin: BindingOrigin::InlineExpr,
            modifier: ScopeModifier::None,
        });
    }

    /// Register a name referenced by an op template (for DCE).
    pub fn add_required_output(&mut self, name: &str) {
        if !self.required_outputs.contains(&name.to_string()) {
            self.required_outputs.push(name.to_string());
        }
    }

    /// Register a config expression reference (for DCE).
    pub fn add_config_ref(&mut self, name: &str) {
        if !self.config_refs.contains(&name.to_string()) {
            self.config_refs.push(name.to_string());
        }
    }

    /// All names defined in this scope (all origins).
    pub fn defined_names(&self) -> HashSet<String> {
        self.bindings.iter().map(|b| b.name.clone()).collect()
    }

    /// All extern names in this scope.
    pub fn extern_names(&self) -> HashSet<String> {
        self.externs.iter().map(|e| e.name.clone()).collect()
    }

    /// The combined required outputs (template refs + config refs).
    pub fn required_outputs(&self) -> Vec<String> {
        let mut all = self.required_outputs.clone();
        for name in &self.config_refs {
            if !all.contains(name) {
                all.push(name.clone());
            }
        }
        all
    }

    /// Validate scope rules. Returns `Ok(())` if valid, or a
    /// descriptive error explaining the violation and its provenance.
    pub fn validate(&self) -> Result<(), String> {
        // Build a map of name → first binding for each origin tier.
        // The tier order determines precedence: earlier tiers own the name.
        let mut owned: HashMap<String, &ScopedBinding> = HashMap::new();

        for binding in &self.bindings {
            if let Some(prior) = owned.get(&binding.name) {
                // Same name appears twice. Check if this is allowed.
                match (&prior.origin, &binding.origin) {
                    // Inherited + Inherited: duplicate from multiple ops
                    // sharing the same workload bindings. Allowed if
                    // definitions are identical.
                    (BindingOrigin::Inherited, BindingOrigin::Inherited) => {
                        if prior.line != binding.line {
                            return Err(format!(
                                "binding '{}' has conflicting inherited definitions:\n  \
                                 first:  {}\n  second: {}",
                                binding.name, prior.line, binding.line
                            ));
                        }
                        // Duplicate with same definition — will be deduplicated at emit
                    }

                    // Phase + Phase: same check as inherited
                    (BindingOrigin::Phase, BindingOrigin::Phase) => {
                        if prior.line != binding.line {
                            return Err(format!(
                                "binding '{}' has conflicting phase-level definitions:\n  \
                                 first:  {}\n  second: {}",
                                binding.name, prior.line, binding.line
                            ));
                        }
                    }

                    // Op overriding Inherited/Phase/IterationVar with a
                    // DIFFERENT definition: real shadow, error.
                    (BindingOrigin::Inherited | BindingOrigin::Phase | BindingOrigin::IterationVar,
                     BindingOrigin::Op(op_name)) => {
                        if prior.line != binding.line {
                            return Err(format!(
                                "op '{}' binding '{}' shadows a name from {} origin \
                                 with a different definition.\n  \
                                 scope: {}\n  op:    {}\n\
                                 Ops augment the scope DAG but cannot override it. \
                                 Use a separate phase for different bindings.",
                                op_name, binding.name, prior.origin,
                                prior.line, binding.line
                            ));
                        }
                        // Same definition from inheritance — dedup at emit
                    }

                    // Op redefining an op binding from a DIFFERENT op
                    (BindingOrigin::Op(prior_op), BindingOrigin::Op(this_op)) => {
                        if prior_op != this_op {
                            return Err(format!(
                                "op '{}' binding '{}' is already defined by op '{}'. \
                                 Each ride-along binding name must be unique across \
                                 all ops in the scope.",
                                this_op, binding.name, prior_op
                            ));
                        }
                        // Same op, same name — shouldn't happen, but tolerate if same def
                    }

                    // IterationVar replacing Inherited/Phase: iteration
                    // variables are injected before inherited bindings
                    // in the emission order, so GK sees them first.
                    // This is intentional — for_each vars override params.
                    (BindingOrigin::IterationVar, BindingOrigin::Inherited | BindingOrigin::Phase) |
                    (BindingOrigin::Inherited | BindingOrigin::Phase, BindingOrigin::IterationVar) => {
                        // Allowed: iteration vars intentionally override inherited names
                    }

                    // ParamExpansion: these are only added when the name
                    // is NOT already defined. The caller checks. But if
                    // somehow a duplicate appears, same-def is fine.
                    (_, BindingOrigin::ParamExpansion) | (BindingOrigin::ParamExpansion, _) => {
                        // Param expansion is additive; caller skips existing names
                    }

                    // InlineExpr: synthetic names (__expr_N), shouldn't collide
                    (_, BindingOrigin::InlineExpr) | (BindingOrigin::InlineExpr, _) => {
                        // Synthetic names from inline expressions
                    }

                    // AutoExtern: these go to the extern list, not bindings.
                    // Shouldn't appear here, but tolerate.
                    (_, BindingOrigin::AutoExtern) | (BindingOrigin::AutoExtern, _) => {}

                    // Inherited + Op(same def): already covered above.
                    // Any other combination: flag it.
                    _ => {
                        if prior.line != binding.line {
                            return Err(format!(
                                "binding '{}' conflicts: {} origin ({}) vs {} origin ({})",
                                binding.name,
                                prior.origin, prior.line,
                                binding.origin, binding.line
                            ));
                        }
                    }
                }
            } else {
                owned.insert(binding.name.clone(), binding);
            }
        }

        // Check final shadowing: no binding can redefine a name that
        // appears as Final in the extern list or prior bindings.
        let final_names: HashSet<String> = self.bindings.iter()
            .filter(|b| b.modifier == ScopeModifier::Final)
            .map(|b| b.name.clone())
            .collect();

        for binding in &self.bindings {
            if final_names.contains(&binding.name)
                && binding.modifier != ScopeModifier::Final
                && binding.origin != BindingOrigin::Inherited
            {
                return Err(format!(
                    "cannot shadow 'final' binding '{}' from outer scope",
                    binding.name
                ));
            }
        }

        Ok(())
    }

    /// Emit the validated scope as a single GK source string.
    ///
    /// Entries are ordered:
    /// 1. Coordinates declaration
    /// 2. Extern declarations
    /// 3. Init declarations (iteration variables)
    /// 4. Inherited/Phase bindings (deduplicated by name)
    /// 5. ParamExpansion bindings
    /// 6. InlineExpr bindings
    /// 7. Op-level bindings (new names only)
    ///
    /// Each name is emitted exactly once. The first occurrence wins;
    /// subsequent duplicates with the same definition are suppressed.
    pub fn emit(&self) -> String {
        let mut lines: Vec<String> = Vec::new();
        let mut emitted_names: HashSet<String> = HashSet::new();

        // 1. Coordinates
        if let Some(ref coords) = self.coordinates {
            lines.push(coords.clone());
            // Extract coordinate names into emitted set
            if let Some(pos) = coords.find(":=") {
                let rhs = coords[pos + 2..].trim();
                let inner = rhs.trim_start_matches('(').trim_end_matches(')');
                for name in inner.split(',') {
                    emitted_names.insert(name.trim().to_string());
                }
            }
        }

        // 2. Externs
        for ext in &self.externs {
            if !emitted_names.contains(&ext.name) {
                lines.push(format!("extern {}: {}", ext.name, ext.type_name));
                emitted_names.insert(ext.name.clone());
            }
        }

        // 3-7. Bindings in origin order
        let origin_order: &[fn(&BindingOrigin) -> bool] = &[
            |o| matches!(o, BindingOrigin::IterationVar),
            |o| matches!(o, BindingOrigin::Inherited),
            |o| matches!(o, BindingOrigin::Phase),
            |o| matches!(o, BindingOrigin::ParamExpansion),
            |o| matches!(o, BindingOrigin::InlineExpr),
            |o| matches!(o, BindingOrigin::Op(_)),
        ];

        for predicate in origin_order {
            for binding in &self.bindings {
                if predicate(&binding.origin) && !emitted_names.contains(&binding.name) {
                    lines.push(binding.line.clone());
                    emitted_names.insert(binding.name.clone());
                }
            }
        }

        lines.join("\n")
    }
}

/// Parse the modifier prefix and bare name from a GK LHS.
///
/// `"shared foo"` → `(Shared, "foo")`
/// `"init bar"` → `(Init, "bar")`
/// `"cursor baz"` → `(Cursor, "baz")`
/// `"final qux"` → `(Final, "qux")`
/// `"plain"` → `(None, "plain")`
fn parse_modifier_and_name(lhs: &str) -> (ScopeModifier, &str) {
    if let Some(rest) = lhs.strip_prefix("shared ") {
        (ScopeModifier::Shared, rest.trim())
    } else if let Some(rest) = lhs.strip_prefix("final ") {
        (ScopeModifier::Final, rest.trim())
    } else if let Some(rest) = lhs.strip_prefix("init ") {
        (ScopeModifier::Init, rest.trim())
    } else if let Some(rest) = lhs.strip_prefix("cursor ") {
        (ScopeModifier::Cursor, rest.trim())
    } else {
        (ScopeModifier::None, lhs)
    }
}

/// Build a `BindingScope` from phase ops and execution context.
///
/// This is the main entry point that replaces the string mutation
/// pipeline in the executor. It:
/// 1. Classifies each op's bindings by origin (Inherited vs Op)
/// 2. Adds iteration variables
/// 3. Generates auto-externs from the outer manifest
/// 4. Expands workload params
/// 5. Extracts inline expressions
/// 6. Collects required outputs from op templates
pub fn build_scope(
    ops: &[ParsedOp],
    iteration_vars: &HashMap<String, String>,
    outer_manifest: &[crate::runner::ManifestEntry],
    workload_params: &HashMap<String, String>,
    phases: &HashMap<String, nb_workload::model::WorkloadPhase>,
    phase_cycles: Option<&str>,
    exclude: &[String],
) -> Result<BindingScope, String> {
    let mut scope = BindingScope::new();

    // --- Step 1: Classify op bindings by origin ---
    //
    // The first op's GkSource is the "base" (inherited/phase-level).
    // Subsequent ops: if their GkSource matches the base exactly,
    // they're inherited duplicates. If different, they're op-level
    // augmentations carrying new bindings.
    let mut base_source: Option<String> = None;

    for op in ops {
        if let BindingsDef::GkSource(src) = &op.bindings {
            let src = src.trim();
            if src.is_empty() { continue; }

            match &base_source {
                None => {
                    // First op's source becomes the base — classified as Inherited
                    base_source = Some(src.to_string());
                    scope.ingest_gk_source(src, BindingOrigin::Inherited);
                }
                Some(base) => {
                    if src == base.as_str() {
                        // Identical to base — inherited duplicate, skip.
                        // validate() and emit() handle dedup.
                    } else {
                        // Different from base — this op has its own bindings.
                        // Ingest the DIFFERENCE (lines not in base) as Op origin,
                        // and the shared lines as Inherited.
                        let base_lines: HashSet<&str> = base.lines()
                            .map(|l| l.trim())
                            .filter(|l| !l.is_empty())
                            .collect();
                        for line in src.lines() {
                            let trimmed = line.trim();
                            if trimmed.is_empty() { continue; }
                            if base_lines.contains(trimmed) {
                                // This line is inherited — already ingested from base
                            } else {
                                // This line is op-specific
                                scope.ingest_gk_source(trimmed, BindingOrigin::Op(op.name.clone()));
                            }
                        }
                    }
                }
            }
        }
    }

    // --- Step 2: Add iteration variables ---
    for (var, val) in iteration_vars {
        scope.add_iteration_var(var, val);
    }

    // --- Step 3: Generate auto-externs ---
    //
    // Names referenced in op templates but not defined in the scope
    // need extern declarations to wire to the outer kernel.
    let defined = scope.defined_names();
    let extern_names = scope.extern_names();

    // Collect all referenced names from op templates
    let mut referenced: HashSet<String> = HashSet::new();
    for op in ops {
        for value in op.op.values() {
            if let Some(s) = value.as_str() {
                for name in nb_workload::bindpoints::referenced_bindings(s) {
                    referenced.insert(name);
                }
            }
        }
        if let Some(ref cond) = op.condition {
            let bare = cond.trim()
                .strip_prefix('{').and_then(|s| s.strip_suffix('}'))
                .unwrap_or(cond.trim());
            referenced.insert(bare.to_string());
        }
    }

    // Check for final shadowing violations
    for entry in outer_manifest {
        if entry.modifier == nb_variates::dsl::ast::BindingModifier::Final
            && defined.contains(&entry.name)
        {
            return Err(format!(
                "cannot shadow 'final' binding '{}' from outer scope",
                entry.name
            ));
        }
    }

    // Generate extern declarations for referenced-but-undefined names
    for entry in outer_manifest {
        let is_iter_var = iteration_vars.contains_key(&entry.name);
        if referenced.contains(&entry.name)
            && !defined.contains(&entry.name)
            && !extern_names.contains(&entry.name)
            && !is_iter_var
        {
            let type_name = match entry.port_type {
                nb_variates::node::PortType::U64 => "u64",
                nb_variates::node::PortType::F64 => "f64",
                nb_variates::node::PortType::Str => "String",
                nb_variates::node::PortType::Bool => "bool",
                _ => "String",
            };
            scope.add_extern(&entry.name, type_name);
        }
    }

    // --- Step 4: Workload param expansion ---
    //
    // Params that are referenced in op templates but not already defined
    // in the scope get injected as bindings.
    let defined = scope.defined_names(); // refresh after externs
    for name in &referenced {
        if workload_params.contains_key(name) && !defined.contains(name) {
            scope.add_param_binding(name, &workload_params[name]);
        }
    }
    // Also check phase config values for param refs
    for phase in phases.values() {
        if let Some(ref c) = phase.cycles {
            if c.starts_with('{') && c.ends_with('}') {
                let name = &c[1..c.len()-1];
                if workload_params.contains_key(name) && !scope.defined_names().contains(name) {
                    scope.add_param_binding(name, &workload_params[name]);
                }
            }
        }
    }

    // --- Step 5: Inline expression extraction ---
    //
    // {{expr}} in op templates becomes __expr_N bindings.
    // (Note: the op template rewriting happens separately in the caller
    // since it mutates op fields, not the scope.)
    let mut inline_idx = 0usize;
    let mut expr_to_name: HashMap<String, String> = HashMap::new();
    for op in ops {
        for value in op.op.values() {
            if let Some(s) = value.as_str() {
                for bp in nb_workload::bindpoints::extract_bind_points(s) {
                    if let nb_workload::bindpoints::BindPoint::InlineDefinition(ref expr) = bp {
                        if !expr_to_name.contains_key(expr) {
                            let name = format!("__expr_{inline_idx}");
                            inline_idx += 1;
                            expr_to_name.insert(expr.clone(), name);
                        }
                    }
                }
            }
        }
    }
    for (expr, name) in &expr_to_name {
        scope.add_inline_expr(name, expr);
    }

    // --- Step 6: Collect required outputs ---
    for op in ops {
        for value in op.op.values() {
            if let Some(s) = value.as_str() {
                for name in nb_workload::bindpoints::referenced_bindings(s) {
                    if !exclude.contains(&name) {
                        scope.add_required_output(&name);
                    }
                }
            }
        }
        if let Some(ref cond) = op.condition {
            let name = cond.trim()
                .strip_prefix('{').and_then(|s| s.strip_suffix('}'))
                .unwrap_or(cond.trim());
            if !name.is_empty() && !exclude.contains(&name.to_string()) {
                scope.add_required_output(name);
            }
        }
        crate::bindings::collect_param_bindings_into(&op.params, exclude, &mut scope.required_outputs);
    }

    // Config refs (from cycles={expr})
    if let Some(cycles_spec) = phase_cycles {
        if cycles_spec.starts_with('{') && cycles_spec.ends_with('}') {
            let mut inner = cycles_spec[1..cycles_spec.len()-1].to_string();
            for (v, val) in iteration_vars {
                inner = inner.replace(&format!("{{{v}}}"), val);
            }
            inner = crate::runner::expand_workload_params(&inner, workload_params);
            scope.add_config_ref(&inner);
        }
    }

    Ok(scope)
}

/// Apply `{var}` → `val` substitution to GK source text within ops
/// and op template strings. This is a pure text transform that must
/// happen before scope ingestion when iteration vars contain
/// placeholders in GK expressions (e.g., `vector_at(row, "{spec}")`).
pub fn substitute_iteration_vars(
    ops: &mut [ParsedOp],
    iteration_vars: &HashMap<String, String>,
) {
    for op in ops.iter_mut() {
        for (var, val) in iteration_vars {
            let ph = format!("{{{var}}}");
            if let BindingsDef::GkSource(ref mut src) = op.bindings {
                *src = src.replace(&ph, val);
            }
            // Also substitute in op template strings
            for value in op.op.values_mut() {
                if let Some(s) = value.as_str() {
                    if s.contains(&ph) {
                        *value = serde_json::Value::String(s.replace(&ph, val));
                    }
                }
            }
        }
    }
}

/// Apply workload param substitution to GK source text within ops.
pub fn substitute_workload_params(
    ops: &mut [ParsedOp],
    workload_params: &HashMap<String, String>,
) {
    if workload_params.is_empty() { return; }
    for op in ops.iter_mut() {
        if let BindingsDef::GkSource(ref mut src) = op.bindings {
            for (key, value) in workload_params {
                let placeholder = format!("{{{key}}}");
                if src.contains(&placeholder) {
                    *src = src.replace(&placeholder, value);
                }
            }
        }
        for value in op.op.values_mut() {
            if let Some(s) = value.as_str() {
                let mut rewritten = s.to_string();
                let mut changed = false;
                for (key, param_value) in workload_params {
                    let placeholder = format!("{{{key}}}");
                    if rewritten.contains(&placeholder) {
                        rewritten = rewritten.replace(&placeholder, param_value);
                        changed = true;
                    }
                }
                if changed {
                    *value = serde_json::Value::String(rewritten);
                }
            }
        }
    }
}

/// Rewrite inline expressions (`{{expr}}`) in op template strings to
/// use named binding references (`{__expr_N}`).
///
/// Returns the expression-to-name map for the caller to know what
/// was rewritten.
pub fn rewrite_inline_exprs(
    ops: &mut [ParsedOp],
) -> HashMap<String, String> {
    let mut inline_idx = 0usize;
    let mut expr_to_name: HashMap<String, String> = HashMap::new();

    // Collect unique expressions
    for op in ops.iter() {
        for value in op.op.values() {
            if let Some(s) = value.as_str() {
                for bp in nb_workload::bindpoints::extract_bind_points(s) {
                    if let nb_workload::bindpoints::BindPoint::InlineDefinition(ref expr) = bp {
                        if !expr_to_name.contains_key(expr) {
                            let name = format!("__expr_{inline_idx}");
                            inline_idx += 1;
                            expr_to_name.insert(expr.clone(), name);
                        }
                    }
                }
            }
        }
    }

    // Rewrite op templates
    if !expr_to_name.is_empty() {
        for op in ops.iter_mut() {
            for value in op.op.values_mut() {
                if let Some(s) = value.as_str() {
                    let mut rewritten = s.to_string();
                    for (expr, name) in &expr_to_name {
                        rewritten = rewritten.replace(
                            &format!("{{{{{expr}}}}}"),
                            &format!("{{{name}}}"),
                        );
                        rewritten = rewritten.replace(
                            &format!("{{:={expr}:=}}"),
                            &format!("{{{name}}}"),
                        );
                        rewritten = rewritten.replace(
                            &format!("{{:={expr}}}"),
                            &format!("{{{name}}}"),
                        );
                        rewritten = rewritten.replace(
                            &format!("{{{expr}}}"),
                            &format!("{{{name}}}"),
                        );
                    }
                    *value = serde_json::Value::String(rewritten);
                }
            }
        }
    }

    expr_to_name
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_gk_op(name: &str, stmt: &str, bindings: &str) -> ParsedOp {
        let mut op = ParsedOp::simple(name, stmt);
        op.bindings = BindingsDef::GkSource(bindings.to_string());
        op
    }

    #[test]
    fn inherited_bindings_dedup_across_ops() {
        let bindings = "inputs := (cycle)\nprofiles := matching_profiles(\"example\", \"label\")";
        let ops = vec![
            make_gk_op("op_a", "{profiles}", bindings),
            make_gk_op("op_b", "{profiles}", bindings),
        ];
        let scope = build_scope(
            &ops,
            &HashMap::new(),
            &[],
            &HashMap::new(),
            &HashMap::new(),
            None,
            &[],
        ).unwrap();
        scope.validate().unwrap();
        let emitted = scope.emit();
        // 'profiles' should appear exactly once
        let count = emitted.matches("profiles :=").count();
        assert_eq!(count, 1, "expected exactly 1 'profiles :=' in emitted scope, got {count}:\n{emitted}");
    }

    #[test]
    fn iteration_vars_dont_conflict_with_inherited() {
        let bindings = "inputs := (cycle)\nprofiles := matching_profiles(\"example\", \"label\")";
        let ops = vec![
            make_gk_op("op_a", "{profiles} {table}", bindings),
            make_gk_op("op_b", "{profiles} {table}", bindings),
        ];
        let mut iter_vars = HashMap::new();
        iter_vars.insert("table".to_string(), "vec_default".to_string());

        let scope = build_scope(
            &ops,
            &iter_vars,
            &[],
            &HashMap::new(),
            &HashMap::new(),
            None,
            &[],
        ).unwrap();
        scope.validate().unwrap();
        let emitted = scope.emit();
        assert!(emitted.contains("init table = \"vec_default\""),
            "expected init table in:\n{emitted}");
        assert!(emitted.contains("profiles :="),
            "expected profiles in:\n{emitted}");
    }

    #[test]
    fn op_augmentation_adds_new_names() {
        let base = "inputs := (cycle)\nfoo := hash(cycle)";
        let augmented = "inputs := (cycle)\nfoo := hash(cycle)\nbar := mod(cycle, 100)";
        let ops = vec![
            make_gk_op("op_a", "{foo}", base),
            make_gk_op("op_b", "{foo} {bar}", augmented),
        ];
        let scope = build_scope(
            &ops,
            &HashMap::new(),
            &[],
            &HashMap::new(),
            &HashMap::new(),
            None,
            &[],
        ).unwrap();
        scope.validate().unwrap();
        let emitted = scope.emit();
        assert!(emitted.contains("foo := hash(cycle)"), "missing foo");
        assert!(emitted.contains("bar := mod(cycle, 100)"), "missing bar");
    }

    #[test]
    fn real_shadow_is_caught() {
        let base = "inputs := (cycle)\nfoo := hash(cycle)";
        let shadow = "inputs := (cycle)\nfoo := mod(cycle, 100)";
        let ops = vec![
            make_gk_op("op_a", "{foo}", base),
            make_gk_op("op_b", "{foo}", shadow),
        ];
        let scope = build_scope(
            &ops,
            &HashMap::new(),
            &[],
            &HashMap::new(),
            &HashMap::new(),
            None,
            &[],
        ).unwrap();
        let result = scope.validate();
        assert!(result.is_err(), "expected shadow error");
        let err = result.unwrap_err();
        assert!(err.contains("shadows"), "expected 'shadows' in error: {err}");
        assert!(err.contains("op_b"), "expected op name in error: {err}");
    }

    #[test]
    fn original_bug_repro_no_false_shadow() {
        // The original bug: workload-level bindings with profiles,
        // inherited by a phase with for_each iteration vars.
        // Two ops share identical inherited bindings.
        // The init injection used to make them differ, causing false shadow.
        let bindings = "inputs := (cycle)\nprofiles := matching_profiles(\"example\", \"label\")";
        let ops = vec![
            make_gk_op("drop_metadata_index", "DROP INDEX {table}_meta_idx", bindings),
            make_gk_op("drop_vector_index", "DROP INDEX {table}_idx", bindings),
            make_gk_op("drop_table", "DROP TABLE {table}", bindings),
        ];
        let mut iter_vars = HashMap::new();
        iter_vars.insert("table".to_string(), "fknn_default".to_string());
        iter_vars.insert("spec".to_string(), "example:default".to_string());
        iter_vars.insert("optimize_for".to_string(), "RECALL".to_string());

        let scope = build_scope(
            &ops,
            &iter_vars,
            &[],
            &HashMap::new(),
            &HashMap::new(),
            None,
            &[],
        ).unwrap();
        // This was the bug: validate() used to fail with false shadow error
        scope.validate().unwrap();
        let emitted = scope.emit();
        assert!(emitted.contains("init table = \"fknn_default\""), "missing init table");
        assert!(emitted.contains("profiles :="), "missing profiles");
        // profiles should appear exactly once
        let count = emitted.matches("profiles :=").count();
        assert_eq!(count, 1, "profiles duplicated in:\n{emitted}");
    }
}
