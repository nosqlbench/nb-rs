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

use nbrs_workload::model::{BindingsDef, ParsedOp};
use nbrs_variates::comprehension::{
    collect_leaf_placeholders, collect_string_interp_refs,
    format_workload_param_as_gk_literal, propagate_parent_inputs,
    workload_param_type_name,
};

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

/// The modifier on a binding declaration. Mirrors the
/// `BindingModifier` flag set in nbrs-variates' GK AST plus
/// the binding-kind keywords (`init`, `cursor`) that
/// nbrs-activity's text-level scope assembly cares about.
///
/// Wire-coloring modifiers (`final`, `shared`, `volatile`)
/// could in principle combine, but the scope assembly path
/// historically only needs to know "is this final?" /
/// "is this shared?" for shadow checks; combinations get
/// reduced here to the most-distinctive single tag. The full
/// flag set is preserved in the eventual GK AST when the
/// scope-emitted source compiles.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScopeModifier {
    None,
    Init,
    Shared,
    Final,
    Volatile,
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
    ///
    /// A "line" here is a *logical* line: physical newlines inside
    /// unbalanced `()`/`[]`/`{}` or inside a string literal are
    /// absorbed into the current binding. This is what lets multi-line
    /// expressions like
    ///
    /// ```gk
    /// rate_adjust := control_set("rate",
    ///                            to_f64(control_u64("rate")) * 1.05)
    /// ```
    ///
    /// survive the later split-on-`\n` in [`Self::emit`]: we rejoin
    /// them onto one physical line so the downstream parser sees a
    /// complete expression.
    pub fn ingest_gk_source(&mut self, source: &str, origin: BindingOrigin) {
        for line in logical_lines(source) {
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

                // Input declaration (`inputs := (...)`)
                if lhs == "inputs" {
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
    ///
    /// Iteration variables are declared as `extern` ports rather
    /// than init-time bindings (SRD 18b §"Iteration variables as
    /// scope outputs"). The runtime sets the extern's value
    /// before the leaf kernel executes, so we no longer
    /// text-substitute literal values into the GK source. The
    /// type is inferred from the current iteration's value:
    /// numeric strings get `u64`/`f64`, anything else is `String`.
    pub fn add_iteration_var(&mut self, name: &str, value: &str) {
        let type_name = if value.parse::<u64>().is_ok() {
            "u64"
        } else if value.parse::<f64>().is_ok() {
            "f64"
        } else {
            "String"
        };
        self.externs.push(ExternDecl {
            name: name.to_string(),
            type_name: type_name.to_string(),
        });
    }

    /// Add an auto-extern declaration from the outer scope manifest.
    pub fn add_extern(&mut self, name: &str, type_name: &str) {
        self.externs.push(ExternDecl {
            name: name.to_string(),
            type_name: type_name.to_string(),
        });
    }

    /// Add a workload param binding as a `final` (compile-time
    /// constant) binding so the compiler folds the literal value
    /// and the assembler treats references as const args rather
    /// than wire inputs. The `final` modifier matches the M3.6
    /// contract: workload params are immutable for the run, so
    /// downstream nodes consume them as constants.
    ///
    /// String values are emitted as quoted GK string literals
    /// with embedded `"` and `\` escaped, so JSON-shaped param
    /// values (`{"a": 1}`, `{'class': 'SimpleStrategy'}`,
    /// arbitrary nested quotes) round-trip through GK
    /// compilation unchanged. Numeric and boolean values are
    /// emitted as bare literals.
    pub fn add_param_binding(&mut self, name: &str, value: &str) {
        let line = if value.parse::<u64>().is_ok() || value.parse::<f64>().is_ok() {
            format!("final {name} := {value}")
        } else if value == "true" || value == "false" {
            format!("final {name} := {value}")
        } else {
            let escaped = value.replace('\\', "\\\\").replace('"', "\\\"");
            format!("final {name} := \"{escaped}\"")
        };
        self.bindings.push(ScopedBinding {
            name: name.to_string(),
            line,
            origin: BindingOrigin::ParamExpansion,
            modifier: ScopeModifier::Final,
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

        // 1. Coordinates — emit only when an ingested source
        // declared them. The GK compiler auto-infers coordinates
        // from `cycle` references in non-strict mode, so a
        // workload-level scope built purely from injected
        // workload params (no op-supplied GK source, no `cycle`
        // references) compiles fine without a synthetic line.
        if let Some(ref coords) = self.coordinates {
            lines.push(coords.clone());
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
/// Split `source` into logical lines, treating newlines inside
/// unbalanced `()`, `[]`, `{}` or string literals as continuations.
///
/// A logical line ends at the first physical newline that sits at
/// bracket-depth 0 and outside any string. Each returned String has
/// its interior newlines collapsed to single spaces so the downstream
/// GK parser sees one-expression-per-line, which is all it supports.
fn logical_lines(source: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let mut buf = String::new();
    let mut depth: i32 = 0;
    let mut in_str: Option<char> = None;
    let mut chars = source.chars().peekable();
    while let Some(ch) = chars.next() {
        match in_str {
            Some(quote) => {
                buf.push(ch);
                if ch == '\\' {
                    // Preserve the escaped character verbatim.
                    if let Some(nx) = chars.next() {
                        buf.push(nx);
                    }
                } else if ch == quote {
                    in_str = None;
                }
            }
            None => match ch {
                '"' | '\'' => { in_str = Some(ch); buf.push(ch); }
                '(' | '[' | '{' => { depth += 1; buf.push(ch); }
                ')' | ']' | '}' => {
                    if depth > 0 { depth -= 1; }
                    buf.push(ch);
                }
                '\n' => {
                    if depth > 0 {
                        // Still inside brackets — collapse the
                        // physical newline so the stored line is a
                        // single-line expression.
                        buf.push(' ');
                    } else {
                        out.push(std::mem::take(&mut buf));
                    }
                }
                _ => buf.push(ch),
            },
        }
    }
    if !buf.is_empty() {
        out.push(buf);
    }
    out
}

fn parse_modifier_and_name(lhs: &str) -> (ScopeModifier, &str) {
    // Strip every recognised modifier prefix before classifying;
    // multi-modifier forms like `volatile final` or `final shared`
    // only see a single ScopeModifier tag at the scope-assembly
    // level (the most-distinctive one wins), but the bare name
    // still gets extracted correctly. The eventual GK compile
    // sees the full source line with all keywords intact.
    let mut rest = lhs;
    let mut tag = ScopeModifier::None;
    loop {
        let prev = rest;
        if let Some(r) = rest.strip_prefix("shared ") {
            rest = r.trim();
            // `final` / `init` / `cursor` are more distinctive
            // than `shared` for shadow / kind checks; only set
            // tag if we don't already have a stronger one.
            if matches!(tag, ScopeModifier::None | ScopeModifier::Volatile) {
                tag = ScopeModifier::Shared;
            }
        } else if let Some(r) = rest.strip_prefix("final ") {
            rest = r.trim();
            tag = ScopeModifier::Final;
        } else if let Some(r) = rest.strip_prefix("volatile ") {
            rest = r.trim();
            if matches!(tag, ScopeModifier::None) {
                tag = ScopeModifier::Volatile;
            }
        } else if let Some(r) = rest.strip_prefix("init ") {
            rest = r.trim();
            tag = ScopeModifier::Init;
            break;  // init is a kind-keyword; no modifiers come after it.
        } else if let Some(r) = rest.strip_prefix("cursor ") {
            rest = r.trim();
            tag = ScopeModifier::Cursor;
            break;
        }
        if rest == prev { break; }
    }
    (tag, rest)
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
// =========================================================================
// For-each / comprehension scope kernel synthesis (M3.2)
// =========================================================================
//
// The for_each / for_combinations / for_each_union synthesizer
// lives in `nbrs_variates::comprehension::synthesis::synthesize_for_each_scope`.
// Callers in this crate go directly to that entry point; this
// module retains only the do-loop synthesizer below, since
// do_while / do_until aren't comprehensions.


/// Build the per-scope GK kernel for a `do_while` / `do_until`
/// node (SRD 18b). Same composition contract as for_each
/// (every name visible at this scope resolves through standard
/// GK API on the synthesized kernel) — the difference is the
/// "scope output" is a `counter: u64` rather than tuple
/// iteration variables, and there's no value list to pre-eval.
///
/// Source shape:
///
/// ```text
///   extern <inherited_name>: <type>      # one per name
///                                        # referenced in
///                                        # `condition` text
///                                        # that exists in
///                                        # the parent
///                                        # manifest or
///                                        # workload params
///   final <inherited_param> := <literal> # workload-param
///                                        # injection (M3.3
///                                        # bridge until M3.6)
///   extern <counter>: u64                # only when counter
///                                        # is `Some`
/// ```
///
/// Per iteration the runtime sets `counter` (if present) via
/// `kernel.state().set_input` and evaluates the condition
/// expression against the kernel via `interpolate_via_kernel` +
/// `eval_const_expr`. Children inherit `counter` (and any
/// inherited names) through standard `bind_outer_scope`.
pub fn build_do_loop_scope_kernel(
    counter: Option<&str>,
    condition: &str,
    parent_manifest: &[crate::runner::ManifestEntry],
    parent_kernel: &nbrs_variates::kernel::GkKernel,
    workload_params: &HashMap<String, String>,
    gk_lib_paths: Vec<std::path::PathBuf>,
    workload_dir: Option<&std::path::Path>,
    strict: bool,
    context: &str,
) -> Result<nbrs_variates::kernel::GkKernel, String> {
    let referenced = collect_leaf_placeholders(&[condition.to_string()]);
    let manifest_by_name: HashMap<&str, &crate::runner::ManifestEntry> =
        parent_manifest.iter().map(|e| (e.name.as_str(), e)).collect();

    let mut source = String::new();
    let mut emitted: HashSet<String> = HashSet::new();
    let mut inherited_names: Vec<String> = Vec::new();

    for name in &referenced {
        if let Some(c) = counter
            && c == name
        {
            // Counter handled below.
            continue;
        }
        if let Some(entry) = manifest_by_name.get(name.as_str()) {
            let type_name = match entry.port_type {
                nbrs_variates::node::PortType::U64 => "u64",
                nbrs_variates::node::PortType::F64 => "f64",
                nbrs_variates::node::PortType::Str => "String",
                nbrs_variates::node::PortType::Bool => "bool",
                _ => "String",
            };
            source.push_str(&format!("extern {name}: {type_name}\n"));
            emitted.insert(name.clone());
            // Manifest-sourced names cascade in from the
            // parent scope; mark as inherited so this
            // do-loop scope's `compute_own_coordinates`
            // doesn't double up the parent's iter coord.
            // Same fix shape as `synthesize_for_each_scope`.
            inherited_names.push(name.clone());
        } else if let Some(value) = workload_params.get(name) {
            let literal = format_workload_param_as_gk_literal(value);
            source.push_str(&format!("final {name} := {literal}\n"));
            emitted.insert(name.clone());
        }
    }

    if let Some(c) = counter {
        if !emitted.contains(c) {
            source.push_str(&format!("extern {c}: u64\n"));
            emitted.insert(c.to_string());
        }
    }

    // Cascade every workload param through this do-loop scope
    // so descendants see them via bind_outer_scope. Declared as
    // `extern` (not `final`) so the value flows in from the
    // workload root via the standard GK scope chain — the
    // intermediate kernel doesn't re-declare the value at every
    // layer, it just provides a wire for it to pass through.
    for (name, value) in workload_params {
        if emitted.contains(name) { continue; }
        let type_name = workload_param_type_name(value);
        source.push_str(&format!("extern {name}: {type_name}\n"));
        emitted.insert(name.clone());
        inherited_names.push(name.clone());
    }

    // Cascade every name visible at the parent scope (outer iter
    // vars and any other ancestor-declared inputs *and* outputs)
    // — same chain-break story as `build_for_each_scope_kernel`.
    // See that function's comment for the motivating example.
    let parent_program = parent_kernel.program();
    let skip_cascade = |emitted: &HashSet<String>, name: &str| -> bool {
        emitted.contains(name) || name == "cycle" || name.starts_with("__")
    };
    for name in parent_program.output_names() {
        let owned = name.to_string();
        if skip_cascade(&emitted, &owned) { continue; }
        if let Some(c) = counter && c == owned { continue; }
        let (node_idx, port_idx) = parent_program.resolve_output_by_index(
            parent_program.output_index(&owned).unwrap()
        );
        let port_type = parent_program.node_meta(node_idx).outs[port_idx].typ;
        let type_name = match port_type {
            nbrs_variates::node::PortType::U64 => "u64",
            nbrs_variates::node::PortType::F64 => "f64",
            nbrs_variates::node::PortType::Str => "String",
            nbrs_variates::node::PortType::Bool => "bool",
            _ => "String",
        };
        source.push_str(&format!("extern {owned}: {type_name}\n"));
        emitted.insert(owned.clone());
        inherited_names.push(owned);
    }
    for name in parent_program.input_names() {
        if skip_cascade(&emitted, &name) { continue; }
        if let Some(c) = counter && c == name { continue; }
        let port_type = parent_program.input_port_type(&name)
            .unwrap_or(nbrs_variates::node::PortType::Str);
        let type_name = match port_type {
            nbrs_variates::node::PortType::U64 => "u64",
            nbrs_variates::node::PortType::F64 => "f64",
            nbrs_variates::node::PortType::Str => "String",
            nbrs_variates::node::PortType::Bool => "bool",
            _ => "String",
        };
        source.push_str(&format!("extern {name}: {type_name}\n"));
        emitted.insert(name.clone());
        inherited_names.push(name);
    }

    if source.is_empty() {
        source.push_str("final __empty := 0\n");
    }

    let mut kernel = nbrs_variates::dsl::compile::compile_gk_with_libs(
        &source,
        workload_dir,
        gk_lib_paths,
        &[],
        strict,
        context,
    ).map_err(|e| format!("{context}: do-loop scope synthesis: {e}"))?;
    kernel.mark_inherited_outputs(inherited_names);
    kernel.bind_outer_scope(parent_kernel);
    propagate_parent_inputs(&mut kernel, parent_kernel);
    Ok(kernel)
}

/// SRD-13d Phase 9 — synthesize a per-op-template kernel for a
/// materialised op-template scope.
///
/// Mirrors [`build_do_loop_scope_kernel`] but uses the op's
/// `bindings:` block (which already carries metric `:=` injections
/// per SRD-40b §1) as the body. The resulting kernel:
///
/// 1. Cascades every parent-visible name via `extern <name>: <type>`
///    so descendants reach parent values through the standard
///    `bind_outer_scope` chain.
/// 2. Emits the op's own bindings.
/// 3. Compiles + `bind_outer_scope`s to the parent kernel and
///    propagates inherited inputs.
///
/// Required outputs are computed from references in the op's
/// fields, condition, delay, metric values, and result wires —
/// the same surface `build_scope` uses for the workload-level
/// kernel.
pub fn build_op_template_scope_kernel(
    op: &nbrs_workload::model::ParsedOp,
    parent_manifest: &[crate::runner::ManifestEntry],
    parent_kernel: &nbrs_variates::kernel::GkKernel,
    workload_params: &HashMap<String, String>,
    gk_lib_paths: Vec<std::path::PathBuf>,
    workload_dir: Option<&std::path::Path>,
    strict: bool,
    context: &str,
) -> Result<nbrs_variates::kernel::GkKernel, String> {
    use nbrs_workload::model::BindingsDef;

    let manifest_by_name: HashMap<&str, &crate::runner::ManifestEntry> =
        parent_manifest.iter().map(|e| (e.name.as_str(), e)).collect();

    let mut source = String::new();
    let mut emitted: HashSet<String> = HashSet::new();
    let mut inherited_names: Vec<String> = Vec::new();

    // Collect names this op's body explicitly references via
    // textual placeholders (op fields, condition, delay) so the
    // extern emission below knows what types to declare. The
    // op-level `bindings:` block has its own type story handled
    // by the GK compiler downstream.
    let mut referenced: Vec<String> = Vec::new();
    for value in op.op.values() {
        if let Some(s) = value.as_str() {
            for n in nbrs_workload::bindpoints::referenced_bindings(s) {
                if !referenced.contains(&n) {
                    referenced.push(n);
                }
            }
        }
    }
    if let Some(ref s) = op.condition {
        let n = s.trim().trim_start_matches('{').trim_end_matches('}');
        if !n.is_empty() && !referenced.iter().any(|r| r == n) {
            referenced.push(n.to_string());
        }
    }
    if let Some(ref s) = op.delay {
        let n = s.trim().trim_start_matches('{').trim_end_matches('}');
        if !n.is_empty() && !referenced.iter().any(|r| r == n) {
            referenced.push(n.to_string());
        }
    }
    for spec in op.metrics.values() {
        let trimmed = spec.value.trim();
        let bare = !trimmed.is_empty()
            && trimmed.chars().all(|c| c.is_alphanumeric() || c == '_');
        if bare && !referenced.iter().any(|r| r == trimmed) {
            referenced.push(trimmed.to_string());
        }
    }

    for name in &referenced {
        if let Some(entry) = manifest_by_name.get(name.as_str()) {
            let type_name = match entry.port_type {
                nbrs_variates::node::PortType::U64 => "u64",
                nbrs_variates::node::PortType::F64 => "f64",
                nbrs_variates::node::PortType::Str => "String",
                nbrs_variates::node::PortType::Bool => "bool",
                _ => "String",
            };
            source.push_str(&format!("extern {name}: {type_name}\n"));
            emitted.insert(name.clone());
            inherited_names.push(name.clone());
        } else if let Some(value) = workload_params.get(name) {
            let literal = format_workload_param_as_gk_literal(value);
            source.push_str(&format!("final {name} := {literal}\n"));
            emitted.insert(name.clone());
        }
    }

    // Cascade workload params through this op-template scope so
    // descendants see them via bind_outer_scope. Same shape as
    // build_do_loop_scope_kernel.
    for (name, value) in workload_params {
        if emitted.contains(name) { continue; }
        let type_name = workload_param_type_name(value);
        source.push_str(&format!("extern {name}: {type_name}\n"));
        emitted.insert(name.clone());
        inherited_names.push(name.clone());
    }

    // Cascade every parent-scope name (outer iter vars, ancestor-
    // declared inputs and outputs).
    let parent_program = parent_kernel.program();
    let skip_cascade = |emitted: &HashSet<String>, name: &str| -> bool {
        emitted.contains(name) || name == "cycle" || name.starts_with("__")
    };
    for name in parent_program.output_names() {
        let owned = name.to_string();
        if skip_cascade(&emitted, &owned) { continue; }
        let (node_idx, port_idx) = parent_program.resolve_output_by_index(
            parent_program.output_index(&owned).unwrap()
        );
        let port_type = parent_program.node_meta(node_idx).outs[port_idx].typ;
        let type_name = match port_type {
            nbrs_variates::node::PortType::U64 => "u64",
            nbrs_variates::node::PortType::F64 => "f64",
            nbrs_variates::node::PortType::Str => "String",
            nbrs_variates::node::PortType::Bool => "bool",
            _ => "String",
        };
        source.push_str(&format!("extern {owned}: {type_name}\n"));
        emitted.insert(owned.clone());
        inherited_names.push(owned);
    }
    for name in parent_program.input_names() {
        if skip_cascade(&emitted, &name) { continue; }
        let port_type = parent_program.input_port_type(&name)
            .unwrap_or(nbrs_variates::node::PortType::Str);
        let type_name = match port_type {
            nbrs_variates::node::PortType::U64 => "u64",
            nbrs_variates::node::PortType::F64 => "f64",
            nbrs_variates::node::PortType::Str => "String",
            nbrs_variates::node::PortType::Bool => "bool",
            _ => "String",
        };
        source.push_str(&format!("extern {name}: {type_name}\n"));
        emitted.insert(name.clone());
        inherited_names.push(name);
    }

    // Now emit the op's own bindings as the body. These are
    // already in op.bindings — for the GkSource form we just
    // splice the source; for the Map form we serialize.
    let body_text: String = match &op.bindings {
        BindingsDef::GkSource(s) => s.clone(),
        BindingsDef::Map(m) => {
            let mut out = String::new();
            for (name, expr) in m {
                out.push_str(&format!("{name} := {expr}\n"));
            }
            out
        }
    };
    if body_text.trim().is_empty() {
        // No own bindings — the kernel just re-exports parent.
        // The flatten-elision logic upstream should have caught
        // this, but defensively keep the kernel non-empty.
        if source.is_empty() {
            source.push_str("final __empty := 0\n");
        }
    } else {
        if !source.ends_with('\n') && !source.is_empty() {
            source.push('\n');
        }
        source.push_str(&body_text);
        if !source.ends_with('\n') {
            source.push('\n');
        }
    }

    let mut kernel = nbrs_variates::dsl::compile::compile_gk_with_libs(
        &source,
        workload_dir,
        gk_lib_paths,
        &[],
        strict,
        context,
    ).map_err(|e| format!("{context}: op-template scope synthesis: {e}"))?;
    kernel.mark_inherited_outputs(inherited_names);
    kernel.bind_outer_scope(parent_kernel);
    propagate_parent_inputs(&mut kernel, parent_kernel);
    Ok(kernel)
}


pub fn build_scope(
    ops: &[ParsedOp],
    iteration_vars: &HashMap<String, String>,
    outer_manifest: &[crate::runner::ManifestEntry],
    workload_params: &HashMap<String, String>,
    phases: &HashMap<String, nbrs_workload::model::WorkloadPhase>,
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
                        // and the shared lines as Inherited. Compare by
                        // *logical* lines so multi-line expressions
                        // (function calls broken over several physical
                        // lines) stay intact instead of being split at
                        // the paren and ingested piecewise.
                        let base_logical: Vec<String> = logical_lines(base).into_iter()
                            .map(|l| l.trim().to_string())
                            .filter(|l| !l.is_empty())
                            .collect();
                        for line in logical_lines(src) {
                            let trimmed = line.trim();
                            if trimmed.is_empty() { continue; }
                            if base_logical.iter().any(|b| b == trimmed) {
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
    //
    // Iter vars (own + cascade-inherited) are part of the
    // **scope's contract** — they're names a phase or a child
    // scope is allowed to consume. Mark them required so the
    // compiler's DCE keeps the auto-passthrough output that
    // `extern <name>` produces. Without this, an iter var
    // referenced only from a deeper structure (e.g. a relevancy
    // `k:` field, or a downstream do-while condition) would be
    // pruned and pull-by-name would fail.
    for (var, val) in iteration_vars {
        scope.add_iteration_var(var, val);
        scope.add_required_output(var);
    }

    // --- Step 3: Generate auto-externs ---
    //
    // Names referenced in op templates but not defined in the scope
    // need extern declarations to wire to the outer kernel.
    let defined = scope.defined_names();
    let extern_names = scope.extern_names();

    // Collect all referenced names from op templates AND
    // binding source RHS. Names referenced in either need
    // extern declarations so the GK compile path can wire
    // them. Without scanning bindings, a phase whose binding
    // RHS uses `{outer_name}` (e.g. `dim := vector_dim(
    // "{dataset}:{profile}")`) wouldn't get extern declarations
    // for `dataset` and `profile`, leaving the GK string
    // interpolation desugar to fail at compile time.
    let mut referenced: HashSet<String> = HashSet::new();
    for op in ops {
        for value in op.op.values() {
            if let Some(s) = value.as_str() {
                for name in nbrs_workload::bindpoints::referenced_bindings(s) {
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
        // `delay:` accepts the same shapes as `if:` — a bare
        // wire name (`delay: think_time`) or a `{...}` inline
        // expression. Both consume a binding and need to land
        // in `referenced` so the auto-extern + DCE-keepalive
        // passes provision them.
        if let Some(ref delay) = op.delay {
            let bare = delay.trim()
                .strip_prefix('{').and_then(|s| s.strip_suffix('}'))
                .unwrap_or(delay.trim());
            referenced.insert(bare.to_string());
        }
        // Bindings: scan GK source for `{name}` placeholders
        // that the GK string-interpolation desugar will treat
        // as wire references.
        if let BindingsDef::GkSource(src) = &op.bindings {
            collect_string_interp_refs(src, &mut referenced);
        }
        // Op params (`evaluations:`/`relevancy:`/`verify:`/...
        // hoisted by the workload parser) carry `{name}` bind-
        // point references too — `relevancy: { k: "{k}" }` is
        // a real consumer of the wire `k`, even though `k`
        // isn't on the op-template wire path. Without scanning
        // these, an iter var referenced *only* from param
        // config would fail to auto-extern from the parent
        // manifest, and runtime `pull(name)` calls for that
        // wire (e.g. `parse_count_param` on relevancy `k:`)
        // would panic with "unknown output variate".
        let mut param_refs: Vec<String> = Vec::new();
        crate::bindings::collect_param_bindings_into(
            &op.params, &[], &mut param_refs,
        );
        for name in param_refs {
            referenced.insert(name);
        }
    }

    // Check for final shadowing violations
    for entry in outer_manifest {
        if entry.modifier == nbrs_variates::dsl::ast::BindingModifier::FINAL
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
                nbrs_variates::node::PortType::U64 => "u64",
                nbrs_variates::node::PortType::F64 => "f64",
                nbrs_variates::node::PortType::Str => "String",
                nbrs_variates::node::PortType::Bool => "bool",
                _ => "String",
            };
            scope.add_extern(&entry.name, type_name);
        }
    }

    // --- Step 4: Workload param expansion ---
    //
    // M3.6: Every workload param injects as a `final` binding,
    // regardless of whether it's referenced in this specific
    // scope's ops. The workload kernel becomes the single
    // canonical home for workload params; descendant scopes
    // auto-extern them via the manifest chain. Phase-level
    // build_scope callers pass an empty `workload_params`
    // (their workload params come via the parent-scope
    // kernel's manifest, not local injection).
    let defined = scope.defined_names(); // refresh after externs
    for (name, value) in workload_params {
        if !defined.contains(name) {
            scope.add_param_binding(name, value);
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
    let mut collect = |s: &str| {
        for bp in nbrs_workload::bindpoints::extract_bind_points(s) {
            if let nbrs_workload::bindpoints::BindPoint::InlineDefinition(ref expr) = bp {
                if !expr_to_name.contains_key(expr) {
                    let name = format!("__expr_{inline_idx}");
                    inline_idx += 1;
                    expr_to_name.insert(expr.clone(), name);
                }
            }
        }
    };
    for op in ops {
        for value in op.op.values() {
            if let Some(s) = value.as_str() {
                collect(s);
            }
        }
        // Inline expressions in `if:` and `delay:` count too —
        // those get hoisted out of `op.op` by the parser into
        // dedicated fields on `ParsedOp`.
        if let Some(s) = &op.condition { collect(s); }
        if let Some(s) = &op.delay { collect(s); }
    }
    for (expr, name) in &expr_to_name {
        scope.add_inline_expr(name, expr);
    }

    // --- Step 6: Collect required outputs ---
    for op in ops {
        for value in op.op.values() {
            if let Some(s) = value.as_str() {
                for name in nbrs_workload::bindpoints::referenced_bindings(s) {
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
        if let Some(ref delay) = op.delay {
            let name = delay.trim()
                .strip_prefix('{').and_then(|s| s.strip_suffix('}'))
                .unwrap_or(delay.trim());
            if !name.is_empty() && !exclude.contains(&name.to_string()) {
                scope.add_required_output(name);
            }
        }
        // SRD-40b §6: synthetic-metric `value:` references must
        // survive DCE so the dispenser's GK pull plan can resolve
        // them. Bare-name values (the SRD-40b §1 canonical form)
        // refer to a wire produced somewhere in scope; non-bare
        // expressions are deferred to Phase 9 elsewhere — for the
        // bare-name case we mark the wire required.
        for spec in op.metrics.values() {
            let trimmed = spec.value.trim();
            let bare = !trimmed.is_empty()
                && trimmed.chars().all(|c| c.is_alphanumeric() || c == '_');
            if bare && !exclude.contains(&trimmed.to_string()) {
                scope.add_required_output(trimmed);
            }
        }
        // SRD-40b §5 result-as-GK: each `result:` wire reads a
        // path expression off the response body and exposes it as
        // a GK wire. The wire's *name* is what subsequent
        // wrappers (metrics, validation) pull against — mark each
        // declared result wire as required so the kernel exposes
        // an extern slot for it on the post-execute write path.
        for wire in op.result.keys() {
            if !exclude.contains(wire) {
                scope.add_required_output(wire);
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

/// Apply iteration-variable substitution to *op-template
/// strings only* — `raw:`, `prepared:`, `stmt:`, etc.
///
/// Iter vars flow into GK binding source as wires (declared as
/// externs by `BindingScope::add_iteration_var` and bound at
/// runtime via the standard input mechanism), so this helper
/// **does not** touch `op.bindings`. It only rewrites the
/// op-template field values, where `{var}` placeholders refer
/// to structural elements (table names, keyspace names,
/// optimize-for hints) that adapters need as literal text —
/// not as bind variables. CQL's `prepared:` form, for example,
/// converts every remaining `{name}` in the statement to a `?`
/// bind marker; iter vars in structural positions (`INSERT
/// INTO ks.{table} ...`) must be substituted away before that
/// conversion runs, since CQL doesn't permit `?` for table
/// names.
/// Resolve every `{name}` placeholder in the op fields and
/// op-level params of `ops` against the populated parent
/// kernel — the **single name-resolution surface** per SRD-16.
///
/// All static-per-iteration values (iter vars, cascaded workload
/// params, ancestor scope outputs) reach the leaf phase via the
/// `bind_outer_scope` chain and are answered by
/// [`GkKernel::lookup`] uniformly. There is no parallel HashMap
/// of values; there is no fresh-state pull; there is no silent
/// default. A placeholder either resolves through the kernel,
/// is a per-cycle binding produced by this phase's own
/// `bindings:` block (in which case it stays in the string for
/// dispenser-time resolution), or is an error — caller surfaces
/// as a phase-level diagnostic.
///
/// Errors enumerate the unresolved placeholder, the field path,
/// and the names actually in scope at this kernel so the operator
/// can spot a typo or a missing cascade without instrumenting.
pub fn resolve_placeholders_via_kernel(
    ops: &mut [ParsedOp],
    kernel: &nbrs_variates::kernel::GkKernel,
) -> Result<(), String> {
    // Names produced by this phase's own `bindings:` block —
    // those are per-cycle wires the dispenser resolves at
    // execute time, so an unresolved placeholder for one of
    // them is *expected* and stays in the string. Anything
    // else has to come through the kernel.
    let per_cycle_names = collect_phase_binding_lhs_names(ops);

    let mut errors: Vec<String> = Vec::new();
    let in_scope = || -> Vec<String> {
        let prog = kernel.program();
        let mut names: Vec<String> = prog.output_names().iter().map(|s| s.to_string()).collect();
        for n in prog.input_names() {
            if !names.contains(&n) { names.push(n); }
        }
        names.sort();
        names
    };
    for op in ops.iter_mut() {
        let op_name = op.name.clone();
        for (key, value) in op.op.iter_mut() {
            let path = format!("op '{op_name}' field '{key}'");
            resolve_placeholders_in_json(
                value, kernel, &per_cycle_names, &path, &mut errors,
            );
        }
        for (key, value) in op.params.iter_mut() {
            // Params can legitimately reference per-cycle phase
            // bindings — `relevancy.expected: "{ground_truth}"`
            // is the canonical case (SRD 33 §"Ground Truth Flow").
            // The validation wrapper registers `ground_truth`
            // against the phase kernel (where it's an output) at
            // activity construction; until then, the placeholder
            // stays in the JSON value for the fixture/handle path
            // to consume. Apply the same `per_cycle_names` allow-
            // list as op fields — anything unresolved that *isn't*
            // a per-cycle binding remains a hard error.
            let path = format!("op '{op_name}' param '{key}'");
            resolve_placeholders_in_json(
                value, kernel, &per_cycle_names, &path, &mut errors,
            );
        }
    }

    if errors.is_empty() {
        return Ok(());
    }
    let in_scope_str = in_scope().join(", ");
    let mut out = String::from(
        "placeholder resolution failed (single read path: GK kernel lookup):\n"
    );
    for e in &errors {
        out.push_str("  - ");
        out.push_str(e);
        out.push('\n');
    }
    out.push_str(&format!(
        "  in-scope names at this kernel: [{in_scope_str}]"
    ));
    Err(out)
}

/// Scan `ops`' `bindings:` text for the LHS names that get
/// produced as per-cycle wires. The scan is intentionally
/// liberal (LHS can be `cursor q = …`, `init x = …`, `name :=
/// expr`, `extern n: type`, `final n := expr`, `shared n := expr`,
/// destructured `(a, b) := …`). Anything that survives the LHS
/// strip becomes a known name; the substitution path uses this
/// set to distinguish "per-cycle wire — leave for the dispenser"
/// from "typo or missing cascade — error."
fn collect_phase_binding_lhs_names(ops: &[ParsedOp]) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for op in ops {
        if let BindingsDef::GkSource(src) = &op.bindings {
            for line in logical_lines(src) {
                let trimmed = line.trim();
                if trimmed.is_empty() || trimmed.starts_with('#') { continue; }
                // Coordinates (`inputs := (cycle, ...)`) ARE
                // per-cycle names by definition — the runtime
                // sets them per iteration. Extract every name
                // inside the parentheses; without this, `{cycle}`
                // in op templates resolves at compile time
                // against the kernel's initial value (0) instead
                // of being deferred to per-iteration substitution.
                if let Some(rest) = trimmed.strip_prefix("inputs ")
                    .or_else(|| trimmed.strip_prefix("inputs:"))
                    .or_else(|| trimmed.strip_prefix("inputs"))
                {
                    let rest = rest.trim_start_matches(':').trim_start_matches('=').trim();
                    if let Some(inner) = rest.strip_prefix('(').and_then(|s| s.strip_suffix(')')) {
                        for piece in inner.split(',') {
                            let n = piece.trim();
                            if is_bare_ident(n) && !out.contains(&n.to_string()) {
                                out.push(n.to_string());
                            }
                        }
                    }
                    continue;
                }
                let lhs_end = trimmed.find(":=")
                    .or_else(|| trimmed.find('='))
                    .unwrap_or(trimmed.len());
                let mut lhs = &trimmed[..lhs_end];
                // Strip ALL leading wire-coloring modifiers in
                // any order. `volatile final` and `final shared`
                // both need their bare name extracted, and any
                // future modifier added to the SRD-10 set must
                // appear here too.
                loop {
                    let mut matched = false;
                    for prefix in ["cursor ", "init ", "extern ", "final ", "shared ", "volatile ", "private "] {
                        if let Some(stripped) = lhs.strip_prefix(prefix) {
                            lhs = stripped.trim();
                            matched = true;
                            break;
                        }
                    }
                    if !matched { break; }
                }
                let lhs = lhs.trim();
                // Destructured tuple LHS: (a, b, c) := ...
                if let Some(inner) = lhs.strip_prefix('(').and_then(|s| s.strip_suffix(')')) {
                    for piece in inner.split(',') {
                        let n = piece.trim().trim_end_matches(':').trim();
                        if is_bare_ident(n) && !out.contains(&n.to_string()) {
                            out.push(n.to_string());
                        }
                    }
                    continue;
                }
                // Type-annotated single LHS: `name: type`.
                let bare = lhs.split(':').next().unwrap_or(lhs).trim();
                if is_bare_ident(bare) && !out.contains(&bare.to_string()) {
                    out.push(bare.to_string());
                }
            }
        }
    }
    out
}

fn is_bare_ident(s: &str) -> bool {
    let mut chars = s.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Recursively walk a JSON value and resolve every `{name}`
/// placeholder via [`GkKernel::lookup`]. Non-resolving names
/// that are in the per-cycle binding set stay as-is (the
/// dispenser will resolve them at execute time); anything else
/// gets pushed onto `errors`.
///
/// Object keys are *not* rewritten — keys are the closed-vocab
/// field name surface, distinct from the value-bearing
/// placeholders.
fn resolve_placeholders_in_json(
    value: &mut serde_json::Value,
    kernel: &nbrs_variates::kernel::GkKernel,
    per_cycle_names: &[String],
    field_path: &str,
    errors: &mut Vec<String>,
) {
    match value {
        serde_json::Value::String(s) => {
            match resolve_placeholders_in_string(
                s, kernel, per_cycle_names, field_path,
            ) {
                Ok(out) => *value = serde_json::Value::String(out),
                Err(es) => errors.extend(es),
            }
        }
        serde_json::Value::Array(arr) => {
            for (i, v) in arr.iter_mut().enumerate() {
                let p = format!("{field_path}[{i}]");
                resolve_placeholders_in_json(v, kernel, per_cycle_names, &p, errors);
            }
        }
        serde_json::Value::Object(map) => {
            for (k, v) in map.iter_mut() {
                let p = format!("{field_path}.{k}");
                resolve_placeholders_in_json(v, kernel, per_cycle_names, &p, errors);
            }
        }
        _ => {}
    }
}

/// Walk one string, replace each `{name}` placeholder with the
/// kernel's `lookup` result for `name`. Bare-ident-only — the
/// qualified `{bind:…}` / `{capture:…}` / `{input:…}` /
/// `{param:…}` shapes and the inline `{{expr}}` shape pass
/// through untouched (consumed downstream by the dispenser /
/// inline-expression desugar). Names that fall through to the
/// per-cycle binding set also pass through. Anything else is
/// returned as an error in the `Err` Vec, with the field path
/// for context.
fn resolve_placeholders_in_string(
    s: &str,
    kernel: &nbrs_variates::kernel::GkKernel,
    per_cycle_names: &[String],
    field_path: &str,
) -> Result<String, Vec<String>> {
    let bytes = s.as_bytes();
    let n = bytes.len();
    let mut out = String::with_capacity(n);
    let mut errors: Vec<String> = Vec::new();
    let mut i = 0;
    while i < n {
        // `\{` and `\}` are escapes — passthrough one char.
        if bytes[i] == b'\\' && i + 1 < n
            && (bytes[i + 1] == b'{' || bytes[i + 1] == b'}')
        {
            out.push(bytes[i] as char);
            out.push(bytes[i + 1] as char);
            i += 2;
            continue;
        }
        // `{{ ... }}` is an inline expression — passthrough.
        if i + 1 < n && bytes[i] == b'{' && bytes[i + 1] == b'{' {
            // Find the matching `}}`.
            let start = i;
            let mut j = i + 2;
            while j + 1 < n && !(bytes[j] == b'}' && bytes[j + 1] == b'}') {
                j += 1;
            }
            let end = (j + 2).min(n);
            out.push_str(&s[start..end]);
            i = end;
            continue;
        }
        if bytes[i] != b'{' {
            out.push(bytes[i] as char);
            i += 1;
            continue;
        }
        // Find the matching `}` for this `{`.
        let body_start = i + 1;
        let mut j = body_start;
        while j < n && bytes[j] != b'}' {
            j += 1;
        }
        if j >= n {
            // Unterminated `{` — treat as literal char and move on.
            out.push('{');
            i += 1;
            continue;
        }
        let body = &s[body_start..j];
        let after = j + 1;

        // Qualified references stay as-is for downstream.
        if body.contains(':') {
            out.push('{');
            out.push_str(body);
            out.push('}');
            i = after;
            continue;
        }

        // Empty body — leave as-is, validator catches.
        if body.is_empty() {
            out.push('{');
            out.push_str(body);
            out.push('}');
            i = after;
            continue;
        }

        // Not a bare identifier — pass through (could be a format spec).
        if !is_bare_ident(body) {
            out.push('{');
            out.push_str(body);
            out.push('}');
            i = after;
            continue;
        }

        // Names that are per-cycle (coordinates declared via
        // `inputs := (cycle, ...)`, or LHS of phase bindings)
        // MUST be deferred to per-cycle resolution. Their value
        // varies per iteration; pre-resolving against the parent
        // kernel here would bake in iteration 0's value (0) and
        // every subsequent iteration would emit the same string.
        if per_cycle_names.iter().any(|n| n == body) {
            out.push('{');
            out.push_str(body);
            out.push('}');
            i = after;
            continue;
        }
        // Bare ident — try kernel lookup. Then error.
        match kernel.lookup(body) {
            Some(v) => out.push_str(&v.to_display_string()),
            None => {
                errors.push(format!(
                    "{field_path}: '{{{body}}}' did not resolve in scope and is \
                     not a per-cycle binding declared by this phase"
                ));
                // Still push the placeholder as-is so the rest of
                // the string remains parseable for further error
                // collection on the same field.
                out.push('{');
                out.push_str(body);
                out.push('}');
            }
        }
        i = after;
    }

    if errors.is_empty() {
        Ok(out)
    } else {
        Err(errors)
    }
}

/// Apply workload param substitution to GK source text within ops.
/// M3.6: Replace `{name}` placeholders in `bindings:` GK source
/// with the *literal value* of the workload param. Workload
/// params are session-static, so embedding the literal directly
/// lets the GK compiler treat the value as a folded constant
/// wherever the call site expects a `SlotType::ConstU64`/`ConstStr`
/// argument (e.g. `mod(hash(cycle), {user_count})` →
/// `mod(hash(cycle), 100000)` resolves the const-divisor slot
/// without a wire-vs-const ambiguity).
///
/// Numeric and boolean values inline as bare GK literals; string
/// values are emitted as quoted GK string literals with embedded
/// quotes/backslashes escaped.
///
/// Only touches `BindingsDef::GkSource`; op-template fields keep
/// their `{name}` placeholder syntax (rewritten to values at
/// runtime via the parent-kernel-derived iter-var-values map).
/// Replaces the pre-M3.6 `substitute_workload_params` text pass.
pub fn rewrite_workload_param_idents_in_bindings(
    ops: &mut [ParsedOp],
    workload_params: &HashMap<String, String>,
) {
    if workload_params.is_empty() { return; }
    for op in ops.iter_mut() {
        if let BindingsDef::GkSource(ref mut src) = op.bindings {
            for (key, value) in workload_params {
                let placeholder = format!("{{{key}}}");
                if src.contains(&placeholder) {
                    let literal = format_workload_param_as_gk_literal(value);
                    *src = src.replace(&placeholder, &literal);
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

    // Collect unique expressions from every op-bearing field.
    // This was previously op.op.values only; conditions
    // (`if:`) and delays (`delay:`) are also legal hosts of
    // inline-expression bind points (`{is_one_of(x, "y")}`),
    // but they're hoisted out of the op map by the parser so
    // they need explicit handling here.
    let collect_from = |s: &str, expr_to_name: &mut HashMap<String, String>, idx: &mut usize| {
        for bp in nbrs_workload::bindpoints::extract_bind_points(s) {
            if let nbrs_workload::bindpoints::BindPoint::InlineDefinition(ref expr) = bp {
                if !expr_to_name.contains_key(expr) {
                    let name = format!("__expr_{idx}");
                    *idx += 1;
                    expr_to_name.insert(expr.clone(), name);
                }
            }
        }
    };
    for op in ops.iter() {
        for value in op.op.values() {
            if let Some(s) = value.as_str() {
                collect_from(s, &mut expr_to_name, &mut inline_idx);
            }
        }
        if let Some(s) = &op.condition {
            collect_from(s, &mut expr_to_name, &mut inline_idx);
        }
        if let Some(s) = &op.delay {
            collect_from(s, &mut expr_to_name, &mut inline_idx);
        }
    }

    // Rewrite op templates + condition + delay.
    if !expr_to_name.is_empty() {
        let rewrite = |s: &str, expr_to_name: &HashMap<String, String>| -> String {
            let mut rewritten = s.to_string();
            for (expr, name) in expr_to_name {
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
            rewritten
        };
        for op in ops.iter_mut() {
            for value in op.op.values_mut() {
                if let Some(s) = value.as_str() {
                    let rewritten = rewrite(s, &expr_to_name);
                    *value = serde_json::Value::String(rewritten);
                }
            }
            if let Some(s) = &op.condition {
                op.condition = Some(rewrite(s, &expr_to_name));
            }
            if let Some(s) = &op.delay {
                op.delay = Some(rewrite(s, &expr_to_name));
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
        // Iteration variables now declare as `extern <name>:
        // <Type>` so the runtime can populate them per iteration
        // without recompiling (SRD 18b §"Iteration variables as
        // scope outputs"). Type is inferred from the value;
        // "vec_default" doesn't parse numerically → String.
        assert!(emitted.contains("extern table: String"),
            "expected extern table declaration in:\n{emitted}");
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
        // Iter vars now declare as extern (SRD 18b). The original
        // bug — false-shadow detection — is unchanged regardless
        // of how the iter var materialises.
        assert!(emitted.contains("extern table: String"), "missing extern table in:\n{emitted}");
        assert!(emitted.contains("profiles :="), "missing profiles");
        // profiles should appear exactly once
        let count = emitted.matches("profiles :=").count();
        assert_eq!(count, 1, "profiles duplicated in:\n{emitted}");
    }
}
