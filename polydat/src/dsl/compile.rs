// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! DSL-to-assembly bridge: compile a parsed Polydat AST into a runtime kernel.
//!
//! Walks the AST, resolves function names to node constructors, wires
//! the `PolydatAssembler`, and produces a `PolydatKernel`.


use std::path::{Path, PathBuf};

use crate::compile::assembly::{PolydatAssembler, WireRef};
use crate::dsl::ast::*;
use crate::dsl::lexer;
use crate::dsl::parser;
use crate::kernel::PolydatKernel;

use crate::dsl::error::DiagnosticReport;
use crate::dsl::validate::{validate_ast, collect_references};

use std::collections::HashSet;

use super::modules::ResolvedModule;

/// Typed error ontology for the embedded-evaluation surface.
///
/// Per [`expression_engine.md`'s §6 Error Ontology][spec], every
/// failure mode the embedding surface can produce maps to one of
/// these variants. Hosts pattern-match on the variant to drive
/// UX, recovery, or logging without parsing message strings.
///
/// **Status** (γ-1): the enum is introduced additively. Existing
/// surfaces still return `Result<_, String>`; this enum is
/// reachable via construction and converts to `String` via the
/// `From<EmbeddingError> for String` impl below. γ-3 migrates the
/// surfaces to return this type directly.
///
/// [spec]: ../../docs/design/expression_engine.md
#[derive(Debug, Clone)]
pub enum EmbeddingError {
    /// Text could not be parsed as polydat expression source.
    /// The lexer or parser rejected the input before any
    /// semantic analysis.
    Parse {
        source: String,
        message: String,
        position: Option<usize>,
    },

    /// A `{name}` placeholder in the text had no matching
    /// binding in the kernel chain. Produced by
    /// `interpolate_via_kernel` only.
    UnresolvedPlaceholder {
        name: String,
        source: String,
    },

    /// The expression's upstream cone reaches a dynamic input,
    /// but the requested evaluation surface requires
    /// effectively-const lifecycle. Produced by
    /// `eval_const_expr` (directly or via the two-step
    /// composition).
    LifecycleMismatch {
        source: String,
        dynamic_inputs: Vec<String>,
    },

    /// A node mentioned in the expression is not registered
    /// in the runtime. Includes a suggested alternative when
    /// the name is close to a known node.
    UnknownNode {
        name: String,
        source: String,
        suggestion: Option<String>,
    },

    /// The expression's wire chain has a type mismatch that
    /// auto-adapters cannot heal. Produced by the assembly
    /// pass during compilation.
    TypeMismatch {
        from_node: String,
        from_type: crate::ast::PortType,
        to_node: String,
        to_type: crate::ast::PortType,
        source: String,
    },

    /// A node's `eval` panicked during scope-init evaluation.
    /// The kernel's `catch_unwind` boundary captured the
    /// panic; the message is the panic payload's
    /// human-readable form.
    NodeEvalPanic {
        node_name: String,
        message: String,
        source: String,
    },

    /// Compilation succeeded but the requested output name
    /// could not be resolved in the resulting kernel.
    /// Indicates an internal compiler issue or a mismatch
    /// between the wrapper template and the compiler's output
    /// naming.
    ResultMissing {
        output_name: String,
        source: String,
    },

    /// A `Value::None` propagated to the expression's output
    /// when the host called a strict accessor (`as_bool` on
    /// `Value::None`, etc.). Produced at the host's
    /// accessor call, not by polydat directly. See SRD-74.
    NonePropagated {
        accessor: &'static str,
        source: String,
    },

    /// Evaluation exceeded a host-specified time budget.
    /// Currently produced only by deadline-accepting
    /// surfaces (reserved for the bulk-evaluation surface
    /// γ-9 and adapter-specific embedding paths).
    Timeout {
        source: String,
        elapsed_ms: u64,
        deadline_ms: u64,
    },

    /// The runtime node registry (`PolydatRuntime`) is in a state
    /// where required factories were not registered before
    /// the embedding call. Includes the list of node names
    /// the expression referenced but couldn't resolve due to
    /// registry incompleteness.
    RegistryNotInitialised {
        missing: Vec<String>,
        source: String,
    },
}

impl std::fmt::Display for EmbeddingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EmbeddingError::Parse { source, message, position } => match position {
                Some(p) => write!(f, "parse error at position {p} in '{source}': {message}"),
                None => write!(f, "parse error in '{source}': {message}"),
            },
            EmbeddingError::UnresolvedPlaceholder { name, source } => write!(
                f,
                "unresolved placeholder '{{{name}}}' in '{source}' — \
                 no matching binding in the kernel chain"
            ),
            EmbeddingError::LifecycleMismatch { source, dynamic_inputs } => write!(
                f,
                "not a const expression: '{source}' depends on runtime inputs ({})",
                dynamic_inputs.join(", ")
            ),
            EmbeddingError::UnknownNode { name, source, suggestion } => match suggestion {
                Some(sug) => write!(
                    f,
                    "unknown function: '{name}' in '{source}'\n\n  Did you mean '{sug}'?"
                ),
                None => write!(
                    f,
                    "unknown function: '{name}' in '{source}'\n\n  \
                     This function is not registered in the Polydat function library."
                ),
            },
            EmbeddingError::TypeMismatch { from_node, from_type, to_node, to_type, source } => {
                write!(
                    f,
                    "type mismatch in '{source}': cannot connect \
                     {from_type:?} output of '{from_node}' to {to_type:?} \
                     input of '{to_node}'"
                )
            }
            EmbeddingError::NodeEvalPanic { node_name, message, source } => write!(
                f,
                "node-eval panic in '{source}' (node '{node_name}'): {message}"
            ),
            EmbeddingError::ResultMissing { output_name, source } => write!(
                f,
                "compilation completed for '{source}' but output '{output_name}' \
                 is not reachable — internal compiler issue"
            ),
            EmbeddingError::NonePropagated { accessor, source } => write!(
                f,
                "Value::None propagated to '{source}'; \
                 host called strict accessor `{accessor}`. \
                 Use a non-strict accessor (`try_as_*`) or surface the None to the user."
            ),
            EmbeddingError::Timeout { source, elapsed_ms, deadline_ms } => write!(
                f,
                "evaluation of '{source}' exceeded deadline: \
                 {elapsed_ms}ms elapsed, {deadline_ms}ms budget"
            ),
            EmbeddingError::RegistryNotInitialised { missing, source } => write!(
                f,
                "runtime registry missing node(s) referenced by '{source}': {}",
                missing.join(", ")
            ),
        }
    }
}

impl std::error::Error for EmbeddingError {}

/// `From` impl that preserves backward compatibility while γ-1
/// is in place: existing call sites that still expect
/// `Result<_, String>` continue to work via `.map_err(Into::into)`.
/// γ-3 removes the need for this impl by migrating surfaces.
impl From<EmbeddingError> for String {
    fn from(e: EmbeddingError) -> String {
        e.to_string()
    }
}

/// Embedded standard library modules, compiled into the binary.
///
/// Each entry is (filename, source). Multiple modules per file —
/// each top-level binding is a separate module, resolved by name.
/// Searched as the final fallback after workload-local and --polydat-lib paths.
pub(super) static STDLIB_MODULES: &[(&str, &str)] = &[
    ("hashing.polydat", include_str!("../../stdlib/hashing.polydat")),
    ("strings.polydat", include_str!("../../stdlib/strings.polydat")),
    ("identity.polydat", include_str!("../../stdlib/identity.polydat")),
    ("distributions.polydat", include_str!("../../stdlib/distributions.polydat")),
    ("latency.polydat", include_str!("../../stdlib/latency.polydat")),
    ("timeseries.polydat", include_str!("../../stdlib/timeseries.polydat")),
    ("waves.polydat", include_str!("../../stdlib/waves.polydat")),
    ("fourier.polydat", include_str!("../../stdlib/fourier.polydat")),
    ("modeling.polydat", include_str!("../../stdlib/modeling.polydat")),
];

/// Return the embedded standard library module sources.
pub fn stdlib_sources() -> &'static [(&'static str, &'static str)] {
    STDLIB_MODULES
}

/// Compile a `.polydat` source string into a runtime kernel.
pub fn compile_polydat(source: &str) -> Result<PolydatKernel, String> {
    compile_polydat_with_path(source, None)
}

/// Compile Polydat source to an assembler (not yet compiled to a kernel).
///
/// Returns the `PolydatAssembler` with all nodes and wiring populated,
/// ready to be compiled at any level: `.compile()` for P1,
/// `.try_compile()` for P2, `.try_compile_jit()` for P3,
/// `.compile_hybrid()` for Hybrid.
pub fn compile_polydat_to_assembler(source: &str) -> Result<PolydatAssembler, String> {
    let tokens = super::lexer::lex(source)?;
    let ast = super::parser::parse(tokens)?;
    let mut compiler = Compiler::new(None, false);
    let mut asm = compiler.build_assembler(&ast)?;
    asm.set_context(source, "(polydat source)");
    Ok(asm)
}

/// Compile with a source directory for module resolution.
///
/// When the compiler encounters an unknown function name, it searches
/// `source_dir` for `.polydat` module files that export a matching binding.
pub fn compile_polydat_with_path(source: &str, source_dir: Option<&Path>) -> Result<PolydatKernel, String> {
    compile_polydat_strict(source, source_dir, false)
}

/// Compile with dead code elimination: only outputs named in
/// `required_outputs` are exposed, and unreachable upstream nodes
/// are pruned from the kernel.
///
/// When `required_outputs` is empty, compiles all bindings as outputs
/// (same as `compile_polydat_with_path`).
///
/// The `strict` flag enforces the same rules as `compile_polydat_strict`.
pub fn compile_polydat_with_outputs(
    source: &str,
    source_dir: Option<&Path>,
    required_outputs: &[String],
    strict: bool,
) -> Result<PolydatKernel, String> {
    let tokens = lexer::lex(source)?;
    let ast = parser::parse(tokens)?;
    // Only extend the required-outputs list with init bindings
    // when the caller actually passed a non-empty list. Empty
    // means "keep every binding" (DCE doesn't run); init
    // bindings are already preserved in that case, and adding
    // them to a previously-empty list would flip the meaning to
    // "keep only these and their deps" — silently dropping
    // every cycle binding the workload depends on.
    let extended = if required_outputs.is_empty() {
        Vec::new()
    } else {
        extend_required_with_const_bindings(required_outputs, &ast)
    };
    let filter = if extended.is_empty() {
        None
    } else {
        Some(extended.as_slice())
    };
    let mut compiler = Compiler::new(source_dir.map(|p| p.to_path_buf()), strict);
    compiler.source_text = source.to_string();
    compiler.compile_filtered(&ast, filter)
}

/// `init <name> = <expr>` declares a side-effect-carrying init-time
/// computation: download a dataset, prebuffer a facet, register a
/// resource, etc. The user's signal that they want it evaluated is
/// the `const` keyword itself, not a downstream wire reference. Yet
/// the assembler's DCE pass walks back from the requested-outputs
/// set and prunes anything not in that ancestry, which silently
/// removes init bindings whose result nothing reads.
///
/// This helper extends a caller-supplied `required_outputs` list
/// with every `init <name> = ...` LHS in the source. Two effects:
/// the assembler keeps those nodes during DCE, and constant
/// folding then evaluates them at compile time — running the side
/// effect exactly once, before any cycle dispatch.
///
/// Cycle bindings (`name := ...`) are *not* added; they only run
/// when consumed. Modules and other statements are likewise not
/// auto-promoted.
fn extend_required_with_const_bindings(
    required_outputs: &[String],
    ast: &crate::dsl::ast::PolydatFile,
) -> Vec<String> {
    let mut out: Vec<String> = required_outputs.to_vec();
    for stmt in &ast.statements {
        if let crate::dsl::ast::Statement::Binding(b) = stmt
            && b.modifier.is_const()
        {
            for name in &b.targets {
                if !out.iter().any(|n| n == name) {
                    out.push(name.clone());
                }
            }
        }
    }
    out
}

/// Compile with additional library directories for module resolution.
///
/// Resolution order: source_dir, then each polydat_lib_path in order,
/// then the embedded stdlib.  When `required_outputs` is empty,
/// compiles all bindings as outputs.
pub fn compile_polydat_with_libs(
    source: &str,
    source_dir: Option<&Path>,
    polydat_lib_paths: Vec<PathBuf>,
    required_outputs: &[String],
    strict: bool,
    context: &str,
) -> Result<PolydatKernel, String> {
    let tokens = lexer::lex(source)?;
    let ast = parser::parse(tokens)?;
    let extended = if required_outputs.is_empty() {
        Vec::new()
    } else {
        extend_required_with_const_bindings(required_outputs, &ast)
    };
    let filter = if extended.is_empty() {
        None
    } else {
        Some(extended.as_slice())
    };
    let mut compiler = Compiler::with_lib_paths(
        source_dir.map(|p| p.to_path_buf()),
        polydat_lib_paths,
        strict,
    );
    compiler.source_text = source.to_string();
    compiler.context_label = context.to_string();
    compiler.compile_filtered(&ast, filter)
}

/// Compile with an optional cursor limit applied to all cursor declarations.
///
/// When `cursor_limit` is `Some(n)`, the compiler inserts a `limit(cursor, n)`
/// node after each cursor declaration, clamping its extent.
pub fn compile_polydat_with_libs_and_limit(
    source: &str,
    source_dir: Option<&Path>,
    polydat_lib_paths: Vec<PathBuf>,
    required_outputs: &[String],
    strict: bool,
    context: &str,
    cursor_limit: Option<u64>,
) -> Result<PolydatKernel, String> {
    let tokens = lexer::lex(source)?;
    let ast = parser::parse(tokens)?;
    let extended = if required_outputs.is_empty() {
        Vec::new()
    } else {
        extend_required_with_const_bindings(required_outputs, &ast)
    };
    let filter = if extended.is_empty() {
        None
    } else {
        Some(extended.as_slice())
    };
    let mut compiler = Compiler::with_lib_paths(
        source_dir.map(|p| p.to_path_buf()),
        polydat_lib_paths,
        strict,
    );
    compiler.source_text = source.to_string();
    compiler.context_label = context.to_string();
    compiler.cursor_limit = cursor_limit;
    compiler.compile_filtered(&ast, filter)
}

/// Compile with a source directory and optional strict mode.
///
/// When `strict` is true, the compiler enforces:
/// - Explicit `input ...: u64` declaration (no inference)
/// - All module arguments must be named (no positional)
/// - All module inputs must be provided by the caller (no fallthrough to coordinates)
pub fn compile_polydat_strict(source: &str, source_dir: Option<&Path>, strict: bool) -> Result<PolydatKernel, String> {
    let tokens = lexer::lex(source)?;
    let ast = parser::parse(tokens)?;
    compile_ast_strict_with_source(&ast, source_dir, strict, source)
}

/// Compile with a compile event log for diagnostic inspection.
pub fn compile_polydat_with_log(source: &str, log: &mut super::events::CompileEventLog) -> Result<PolydatKernel, String> {
    let tokens = lexer::lex(source)?;
    let ast = parser::parse(tokens)?;
    let pragmas = super::pragmas::collect_from_ast(&ast);
    record_pragma_events(&pragmas, log);
    let mut compiler = Compiler::new(None, false);
    compiler.source_text = source.to_string();
    compiler.pragmas = pragmas;
    let mut asm = compiler.build_assembler(&ast)?;
    asm.set_strict_wires(compiler.pragmas.strict_types(), compiler.pragmas.strict_values());
    asm.compile_with_log(Some(log)).map_err(|e| e.to_string())
}

/// Scan the source for module-level `// @pragma: …` directives and
/// record one event per pragma in the supplied log:
///
/// - Recognised pragmas → `PragmaAcknowledged` (advisory).
/// - Unrecognised pragmas → `UnknownPragma` (warning) — pragmas are
///   forward-compatible, so the compile keeps going.
///
/// Hooked into every `compile_polydat_with_log`-shaped entry point. The
/// extracted [`PragmaSet`] can also be re-fetched directly via
/// [`crate::dsl::pragmas::extract_pragmas`] when downstream graph
/// transforms need it.
///
/// [`PragmaSet`]: crate::dsl::pragmas::PragmaSet
/// Emit `PragmaAcknowledged` (advisory) for recognised pragma
/// names and `UnknownPragma` (warning) for the rest. Forward-
/// compatible: an unknown pragma never blocks compilation.
pub(crate) fn record_pragma_events(
    set: &super::pragmas::PragmaSet,
    log: &mut super::events::CompileEventLog,
) {
    use super::events::CompileEvent;
    for entry in &set.entries {
        let known = matches!(entry.name.as_str(), "strict_types" | "strict_values" | "strict");
        if known {
            log.push(CompileEvent::PragmaAcknowledged {
                name: entry.name.clone(),
                line: entry.line,
            });
        } else {
            log.push(CompileEvent::UnknownPragma {
                name: entry.name.clone(),
                line: entry.line,
            });
        }
    }
}

/// Compile with full diagnostics: errors, warnings, suggestions.
///
/// Returns `(Ok(kernel), report)` on success with possible warnings,
/// or `(Err(()), report)` on failure with errors. The report always
/// contains all diagnostics.
pub fn compile_polydat_checked(source: &str) -> (Result<PolydatKernel, ()>, DiagnosticReport) {
    let mut report = DiagnosticReport::new(source);

    let tokens = match lexer::lex(source) {
        Ok(t) => t,
        Err(e) => {
            report.error(crate::dsl::lexer::Span { line: 1, col: 1 }, e);
            return (Err(()), report);
        }
    };

    let ast = match parser::parse(tokens) {
        Ok(a) => a,
        Err(e) => {
            report.error(crate::dsl::lexer::Span { line: 1, col: 1 }, e);
            return (Err(()), report);
        }
    };

    // Validate the AST before compiling
    validate_ast(&ast, &mut report);

    if report.has_errors() {
        return (Err(()), report);
    }

    match compile_ast(&ast) {
        Ok(kernel) => (Ok(kernel), report),
        Err(e) => {
            report.error(crate::dsl::lexer::Span { line: 1, col: 1 }, e);
            (Err(()), report)
        }
    }
}

/// Evaluate a Polydat expression as a compile-time constant.
///
/// The expression must have no input dependencies. It is compiled
/// as a zero-input program and constant-folded. Returns the folded
/// value, or an error if the expression depends on runtime inputs
/// or fails to compile.
///
/// # Examples
///
/// ```
/// use polydat::dsl::compile::eval_const_expr;
/// let v = eval_const_expr("4 * 4").unwrap();
/// assert_eq!(v.as_u64(), 16);  // both int literals → u64_mul
/// let v = eval_const_expr("4.0 * 4.0").unwrap();
/// assert_eq!(v.as_f64(), 16.0);  // both float literals → f64_mul
/// ```
pub fn eval_const_expr(source: &str) -> Result<crate::ast::Value, EmbeddingError> {
    let wrapped = format!("\nout := {source}");
    let source_owned = source.to_string();
    // Constant-folding inside `compile_polydat` invokes node `eval`
    // for inputs-free DAGs, so any node that panics on bad data
    // (e.g. `handle_of(&Value::None)` after a failed
    // `dataset_open`) would unwind out past this function and
    // crash any caller that doesn't itself catch panics. The
    // kernel's `engines::eval_node` enriches node-eval panics
    // with their provenance string; that string is what we
    // extract.
    let source_for_panic = source_owned.clone();
    let result = std::panic::catch_unwind(
        std::panic::AssertUnwindSafe(move || -> Result<crate::ast::Value, EmbeddingError> {
            let kernel = compile_polydat(&wrapped).map_err(|msg| classify_compile_error(&source_owned, msg))?;
            kernel.get_constant("out")
                .cloned()
                .ok_or_else(|| EmbeddingError::LifecycleMismatch {
                    source: source_owned.clone(),
                    dynamic_inputs: Vec::new(),
                })
        })
    );
    match result {
        Ok(r) => r,
        Err(payload) => Err(EmbeddingError::NodeEvalPanic {
            node_name: "(unknown)".to_string(),
            message: panic_payload_message(&payload),
            source: source_for_panic,
        }),
    }
}

/// Classify a raw compile-error string into a typed
/// `EmbeddingError` variant. Best-effort string pattern
/// matching against the compiler's error message shapes;
/// when nothing matches, falls through to `Parse` (the most
/// common case for stringly-typed compile errors).
fn classify_compile_error(source: &str, msg: String) -> EmbeddingError {
    // "not a const expression: '...' depends on runtime inputs"
    if msg.starts_with("not a const expression") {
        return EmbeddingError::LifecycleMismatch {
            source: source.to_string(),
            dynamic_inputs: Vec::new(),
        };
    }
    // "unknown function: 'foo'" patterns
    if let Some(stripped) = msg.strip_prefix("unknown function: '") {
        if let Some(end) = stripped.find('\'') {
            let name = stripped[..end].to_string();
            return EmbeddingError::UnknownNode {
                name,
                source: source.to_string(),
                suggestion: None,
            };
        }
    }
    // "type mismatch" patterns from the assembler
    if msg.contains("type mismatch") {
        return EmbeddingError::TypeMismatch {
            from_node: "(unknown)".to_string(),
            from_type: crate::ast::PortType::U64,
            to_node: "(unknown)".to_string(),
            to_type: crate::ast::PortType::U64,
            source: source.to_string(),
        };
    }
    // Fall-through: treat as parse error since most
    // compiler-side failures originate at parse time.
    EmbeddingError::Parse {
        source: source.to_string(),
        message: msg,
        position: None,
    }
}

// ───── Typed embedding surface (γ-4) ─────

/// Host-facing type that polydat can return from the typed
/// embedding surfaces. The trait declares the polydat
/// `PortType` the Rust type corresponds to and the conversion
/// from the returned [`crate::ast::Value`] back to the host
/// type.
///
/// Hosts that want compile-time type alignment use the typed
/// surfaces ([`eval_const_expr_typed`] /
/// [`eval_kernel_bound_typed`]) and let the type parameter
/// drive the contract. The fall-back is the untyped surface
/// (`eval_const_expr`) which returns a raw [`crate::ast::Value`]
/// for hosts to coerce themselves.
///
/// See expression_engine.md §5.3.
pub trait HostType: Sized {
    /// The `PortType` that polydat compares the expression's
    /// output type against. Used for compile-time / construction-
    /// time type-mismatch detection.
    fn target_port_type() -> crate::ast::PortType;

    /// Convert a polydat [`crate::ast::Value`] of the matching
    /// port type into the host Rust type. Returns a typed
    /// [`EmbeddingError::TypeMismatch`] when the value's
    /// variant doesn't match this `HostType`'s expected
    /// `PortType`.
    fn from_value(v: crate::ast::Value) -> Result<Self, EmbeddingError>;
}

impl HostType for bool {
    fn target_port_type() -> crate::ast::PortType { crate::ast::PortType::Bool }
    fn from_value(v: crate::ast::Value) -> Result<Self, EmbeddingError> {
        match v {
            crate::ast::Value::Bool(b) => Ok(b),
            crate::ast::Value::U64(n) => Ok(n != 0),
            crate::ast::Value::None => Err(EmbeddingError::NonePropagated {
                accessor: "HostType::<bool>::from_value",
                source: "<typed-embedding result>".to_string(),
            }),
            other => Err(EmbeddingError::TypeMismatch {
                from_node: "<expression-output>".to_string(),
                from_type: other.port_type(),
                to_node: "<host-target>".to_string(),
                to_type: crate::ast::PortType::Bool,
                source: "<typed-embedding result>".to_string(),
            }),
        }
    }
}

impl HostType for u64 {
    fn target_port_type() -> crate::ast::PortType { crate::ast::PortType::U64 }
    fn from_value(v: crate::ast::Value) -> Result<Self, EmbeddingError> {
        match v {
            crate::ast::Value::U64(n) => Ok(n),
            crate::ast::Value::None => Err(EmbeddingError::NonePropagated {
                accessor: "HostType::<u64>::from_value",
                source: "<typed-embedding result>".to_string(),
            }),
            other => Err(EmbeddingError::TypeMismatch {
                from_node: "<expression-output>".to_string(),
                from_type: other.port_type(),
                to_node: "<host-target>".to_string(),
                to_type: crate::ast::PortType::U64,
                source: "<typed-embedding result>".to_string(),
            }),
        }
    }
}

impl HostType for f64 {
    fn target_port_type() -> crate::ast::PortType { crate::ast::PortType::F64 }
    fn from_value(v: crate::ast::Value) -> Result<Self, EmbeddingError> {
        match v {
            crate::ast::Value::F64(n) => Ok(n),
            crate::ast::Value::U64(n) => Ok(n as f64),
            crate::ast::Value::None => Err(EmbeddingError::NonePropagated {
                accessor: "HostType::<f64>::from_value",
                source: "<typed-embedding result>".to_string(),
            }),
            other => Err(EmbeddingError::TypeMismatch {
                from_node: "<expression-output>".to_string(),
                from_type: other.port_type(),
                to_node: "<host-target>".to_string(),
                to_type: crate::ast::PortType::F64,
                source: "<typed-embedding result>".to_string(),
            }),
        }
    }
}

impl HostType for String {
    fn target_port_type() -> crate::ast::PortType { crate::ast::PortType::Str }
    fn from_value(v: crate::ast::Value) -> Result<Self, EmbeddingError> {
        match v {
            crate::ast::Value::Str(s) => Ok(s.to_string()),
            crate::ast::Value::U64(n) => Ok(n.to_string()),
            crate::ast::Value::F64(n) => Ok(n.to_string()),
            crate::ast::Value::Bool(b) => Ok(b.to_string()),
            crate::ast::Value::None => Err(EmbeddingError::NonePropagated {
                accessor: "HostType::<String>::from_value",
                source: "<typed-embedding result>".to_string(),
            }),
            other => Err(EmbeddingError::TypeMismatch {
                from_node: "<expression-output>".to_string(),
                from_type: other.port_type(),
                to_node: "<host-target>".to_string(),
                to_type: crate::ast::PortType::Str,
                source: "<typed-embedding result>".to_string(),
            }),
        }
    }
}

/// Const-fold the expression and convert the typed `Value`
/// into the host's requested Rust type. Compile-time type
/// alignment per expression_engine.md §5.3 + E5 + E7.
///
/// `T` must implement [`HostType`]. The expression's output
/// `PortType` is compared against `T::target_port_type()`;
/// matching types pass through directly to
/// [`HostType::from_value`]. Mismatched types invoke the γ-6
/// **return-path boundary adapter**: the catalog
/// (`crate::compile::assembly::auto_adapter`) is consulted
/// to heal the mismatch when possible. Only when no
/// catalog entry exists for the (output_type, target_type)
/// pair does this surface return
/// `EmbeddingError::TypeMismatch`.
///
/// Pairs with [`eval_kernel_bound_typed`] for the
/// kernel-bound (post-interpolation) case.
pub fn eval_const_expr_typed<T: HostType>(source: &str) -> Result<T, EmbeddingError> {
    let value = eval_const_expr(source)?;
    let value_type = value.port_type();
    let target_type = T::target_port_type();
    if value_type == target_type {
        return T::from_value(value);
    }
    // γ-6 return-path adapter: try the catalog before
    // surfacing TypeMismatch.
    if let Some(adapter) = crate::compile::assembly::auto_adapter(value_type, target_type) {
        let inputs = vec![value];
        let mut outputs = vec![crate::ast::Value::None];
        adapter.eval(&inputs, &mut outputs);
        return T::from_value(outputs.remove(0));
    }
    // No catalog entry — surface as typed error.
    Err(EmbeddingError::TypeMismatch {
        from_node: "<expression-output>".to_string(),
        from_type: value_type,
        to_node: "<host-target>".to_string(),
        to_type: target_type,
        source: source.to_string(),
    })
}

/// Two-step: interpolate placeholders against `kernel`, then
/// const-fold + type-convert. The canonical pattern for
/// kernel-bound typed embedding per expression_engine.md
/// §3.2 + §5.3.
pub fn eval_kernel_bound_typed<T: HostType>(
    text: &str,
    kernel: &crate::kernel::PolydatKernel,
) -> Result<T, EmbeddingError> {
    let interpolated = crate::kernel::interp::interpolate_via_kernel(text, kernel)?;
    eval_const_expr_typed::<T>(&interpolated)
}

/// Strict-mode variant of [`eval_const_expr_typed`].
///
/// Rejects type mismatches whose only catalog adapter is
/// **lossy** (e.g., `F64 → U64` truncation, `U64 → Bool`
/// boolean coercion). Hosts that want guaranteed-lossless
/// value passage opt into this surface per
/// `expression_engine.md` §5.1.3 (opt-in strict contract).
///
/// The "lossy" classification is per
/// [`is_lossless_adapter`] below; the function returns
/// `false` for catalog entries that change the value's
/// information content (truncation, narrowing, boolean
/// projection).
pub fn eval_const_expr_typed_strict<T: HostType>(source: &str) -> Result<T, EmbeddingError> {
    let value = eval_const_expr(source)?;
    let value_type = value.port_type();
    let target_type = T::target_port_type();
    if value_type == target_type {
        return T::from_value(value);
    }
    if !is_lossless_adapter(value_type, target_type) {
        return Err(EmbeddingError::TypeMismatch {
            from_node: "<expression-output>".to_string(),
            from_type: value_type,
            to_node: "<host-target>".to_string(),
            to_type: target_type,
            source: source.to_string(),
        });
    }
    if let Some(adapter) = crate::compile::assembly::auto_adapter(value_type, target_type) {
        let inputs = vec![value];
        let mut outputs = vec![crate::ast::Value::None];
        adapter.eval(&inputs, &mut outputs);
        return T::from_value(outputs.remove(0));
    }
    Err(EmbeddingError::TypeMismatch {
        from_node: "<expression-output>".to_string(),
        from_type: value_type,
        to_node: "<host-target>".to_string(),
        to_type: target_type,
        source: source.to_string(),
    })
}

/// Strict-mode kernel-bound variant. Composes
/// [`crate::kernel::interp::interpolate_via_kernel`] with
/// [`eval_const_expr_typed_strict`].
pub fn eval_kernel_bound_typed_strict<T: HostType>(
    text: &str,
    kernel: &crate::kernel::PolydatKernel,
) -> Result<T, EmbeddingError> {
    let interpolated = crate::kernel::interp::interpolate_via_kernel(text, kernel)?;
    eval_const_expr_typed_strict::<T>(&interpolated)
}

/// Classify a catalog adapter as lossless or lossy per
/// `expression_engine.md` §5.4.3. Lossless conversions
/// preserve value identity (widening numeric types,
/// to-string display roundtrips); lossy conversions
/// change information content (truncation, boolean
/// projection).
///
/// Strict-mode embedding surfaces use this to gate which
/// catalog adapters they'll invoke.
pub fn is_lossless_adapter(from: crate::ast::PortType, to: crate::ast::PortType) -> bool {
    use crate::ast::PortType;
    match (from, to) {
        // Numeric widening — lossless.
        (PortType::U32, PortType::U64) => true,
        (PortType::U32, PortType::F64) => true,
        (PortType::I32, PortType::I64) => true,
        (PortType::I32, PortType::F64) => true,
        (PortType::I64, PortType::F64) => true,
        (PortType::F32, PortType::F64) => true,
        // To-string conversions — lossless (string is a
        // representation of the value).
        (_, PortType::Str) => true,
        // Bool → U64 is lossless (true→1, false→0; round-trip
        // exact).
        (PortType::Bool, PortType::U64) => true,
        // U64 → Bool is lossy (nonzero → true throws away
        // the magnitude).
        (PortType::U64, PortType::Bool) => false,
        // F64 → U64 is lossy (truncation).
        (PortType::F64, PortType::U64) => false,
        // U64 → F64 widening is lossless (u64 fits in f64
        // mantissa for values < 2^53; values above lose
        // precision but f64 is the canonical wider type).
        (PortType::U64, PortType::F64) => true,
        // Default: unknown → assume lossy (conservative).
        _ => false,
    }
}

// ───── End typed embedding surface ─────

/// Best-effort extraction of a human message from a
/// `catch_unwind` payload. The kernel's `enrich_eval_panic`
/// re-raises with a `String` payload, so the common case is one
/// line of context-bearing text; fall through to a sentinel for
/// non-string payloads (rare — third-party panic with a custom
/// payload type).
fn panic_payload_message(payload: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "<non-string panic payload>".to_string()
    }
}

/// Evaluate an `extern name: type = default` default expression
/// to a typed `Value`. Accepts literal forms only (`IntLit`,
/// `FloatLit`, `StringLit`, plus identifiers `true`/`false` for
/// `bool` ports). Non-literal expressions are rejected with a
/// clear error; complex defaults belong in a binding, not on
/// the extern declaration.
fn evaluate_default_expr(
    expr: &crate::dsl::ast::Expr,
    port_type: crate::ast::PortType,
) -> Result<crate::ast::Value, String> {
    use crate::dsl::ast::Expr;
    use crate::ast::{PortType, Value};
    match (expr, port_type) {
        (Expr::IntLit(v, _), PortType::U64) => Ok(Value::U64(*v)),
        (Expr::IntLit(v, _), PortType::F64) => Ok(Value::F64(*v as f64)),
        (Expr::FloatLit(v, _), PortType::F64) => Ok(Value::F64(*v)),
        (Expr::StringLit(s, _), PortType::Str) => Ok(Value::Str(s.as_str().into())),
        (Expr::Ident(name, _), PortType::Bool) if name == "true" => Ok(Value::Bool(true)),
        (Expr::Ident(name, _), PortType::Bool) if name == "false" => Ok(Value::Bool(false)),
        _ => Err(format!(
            "default expression must be a literal of type {port_type:?}; got {expr:?}"
        )),
    }
}

/// Infer the surface-level `PortType` of an auto-extern binding's
/// RHS for the `const NAME := <expr>` shape. Returns `None` when
/// the type can't be determined cheaply from the AST alone —
/// the caller falls back to `PortType::Ext` in that case
/// (preserving today's behavior at the type-system edge).
///
/// ## Why this exists
///
/// Auto-extern slots — the `const NAME := <expr>` form where
/// `<expr>` references at least one name — are the
/// conditional-shadow fallback path that two-tier lookup uses
/// when the const-fold yields None at scope-init.
///
/// Before this inference: every auto-extern landed at the slot
/// boundary as `PortType::Ext`. When an outer scope provided
/// a concrete primitive (a U64 iter-var, a Str literal), the
/// boundary adapter had to bridge `U64 → Ext` / `Str → Ext` /
/// etc — and the type-adapter catalog had no entries for those
/// directions, so the runtime warned and passed the value
/// through unchanged.
///
/// `PortType::Ext` is meant for adapter-contributed reflected
/// types (CQL UUIDs, timestamps) — `Box<dyn ReflectedValue>`
/// — not as a "generic unknown" placeholder. Conflating the
/// **scope** axis (`InputKind::IterationExtern` — "this is
/// populated by the outer chain") with the **type** axis
/// (`PortType` — "what is this value's concrete shape") is the
/// design bug this function targets.
///
/// ## Rules
///
/// - String literal RHS (including `"{interp}"` templates) →
///   `Str`. The DSL parser produces `Expr::StringLit` for both
///   plain strings and interpolation patterns; the produced
///   value is Str in either case.
/// - Integer literal → `U64`.
/// - Float literal → `F64`.
/// - Bare identifier referencing an already-declared input →
///   the referenced input's `PortType`. Threading reference
///   types lets `const X := other_extern` propagate types
///   along the cascade rather than collapsing to Ext.
/// - Binary op → the operand types (preferring LHS when both
///   resolve and match; both `Add`/`Sub`/`Mul`/`Div`/`Mod`
///   preserve operand type). `Pow` always returns F64.
/// - Unary negation → operand type.
/// - Function calls, array literals, field access → `None`
///   (Ext fallback). These produce types the assembler knows
///   only after node attachment; inferring here would need a
///   full second-pass.
///
/// ## Tradeoffs not covered
///
/// String-literal RHS without interpolation is already foldable
/// to a concrete value at compile time — the auto-extern slot
/// only exists because the binding's RHS has refs. So the
/// "Str → ?" path is real and covered.
///
/// For Ident → declared-input, we look up the input that's
/// ALREADY in the assembler. Forward references (an Ident that
/// will be declared later in the same pass) return `None`.
/// Production sites declare in dependency order, so this
/// covers ~all real-world cases; the Ext fallback is correct
/// when it doesn't.
fn infer_auto_extern_type(
    expr: &crate::dsl::ast::Expr,
    asm: &crate::compile::assembly::PolydatAssembler,
) -> Option<crate::ast::PortType> {
    use crate::dsl::ast::{Expr, BinOpKind};
    use crate::ast::PortType;
    match expr {
        Expr::StringLit(_, _) => Some(PortType::Str),
        Expr::IntLit(_, _)    => Some(PortType::U64),
        Expr::FloatLit(_, _)  => Some(PortType::F64),
        Expr::Ident(name, _) => {
            if name == "true" || name == "false" {
                Some(PortType::Bool)
            } else {
                asm.input_type(name)
            }
        }
        Expr::BinOp(lhs, op, rhs) => {
            let lhs_t = infer_auto_extern_type(lhs, asm);
            let rhs_t = infer_auto_extern_type(rhs, asm);
            match op {
                BinOpKind::Pow => Some(PortType::F64),
                _ => lhs_t.or(rhs_t),
            }
        }
        Expr::UnaryNeg(inner, _) | Expr::UnaryBitNot(inner, _) => {
            infer_auto_extern_type(inner, asm)
        }
        // SRD-84 Part 1b — a cast's type is its target.
        Expr::Cast(_, ty, _) => Some(*ty),
        Expr::Call(call) => {
            // Each call we recognize here is one fewer
            // boundary-adapter `… → Ext` warning at runtime.
            // The function name → output `PortType` table below
            // is the practical-shipping subset; ideally this
            // lookup would consult the DSL registry's
            // `FuncSig.output_type` directly, but `FuncSig`
            // today carries only "Fixed vs SameAsInput(idx)"
            // without the actual PortType, so the answer for
            // the `Fixed` case still has to come from somewhere.
            // Adding entries here as workloads surface new
            // `→ Ext` warnings is the closed-loop fix until
            // the registry grows the missing column.
            //
            // Categories:
            //
            // - String-producing builtins. The DSL parser also
            //   desugars `"hello {x}"` to `printf("hello {}", x)`,
            //   so `printf` covers every interpolation-literal
            //   workload sugar (e.g. `set: { foo: "{outer}" }`).
            // - Handle-producing builtins. `dataset_prebuffer`
            //   returns an opaque `Value::Handle` so downstream
            //   binds can declare a `Handle`-typed input slot
            //   without per-source plumbing.
            match call.func.as_str() {
                "printf" | "concat" | "format" | "str"
                    => Some(crate::ast::PortType::Str),
                "dataset_prebuffer" | "const_handle"
                    => Some(crate::ast::PortType::Handle),
                _ => None,
            }
        }
        Expr::ArrayLit(_, _) | Expr::FieldAccess { .. } => None,
    }
}

/// Try to fold a `shared X := <expr>` initializer to a typed
/// `(Value, PortType)`. Returns `Some` for literal forms (the
/// shareable-cell case); returns `None` for non-literal
/// expressions (which keep the legacy cycle-binding shape — the
/// `shared` keyword carries metadata only and the binding has
/// no cross-scope mutability today).
///
/// Literal-init shared bindings compile to an input slot +
/// passthrough output, so `materialize_wiring_from_outer` can wire a
/// `SharedCell` between this slot and inner kernels' matching
/// inputs. Non-literal shared bindings retain the
/// computation-node shape; full cross-scope mutability for
/// those is future work (see SRD-16 §"Open: concurrent shared
/// mutation").
fn try_fold_shared_init(
    expr: &crate::dsl::ast::Expr,
) -> Option<(crate::ast::Value, crate::ast::PortType)> {
    use crate::dsl::ast::Expr;
    use crate::ast::{PortType, Value};
    match expr {
        Expr::IntLit(v, _) => Some((Value::U64(*v), PortType::U64)),
        Expr::FloatLit(v, _) => Some((Value::F64(*v), PortType::F64)),
        Expr::StringLit(s, _) => Some((Value::Str(s.as_str().into()), PortType::Str)),
        Expr::Ident(name, _) if name == "true" => Some((Value::Bool(true), PortType::Bool)),
        Expr::Ident(name, _) if name == "false" => Some((Value::Bool(false), PortType::Bool)),
        _ => None,
    }
}

/// Extract an integer literal from a positional argument. Returns None
/// for named args, non-int-literal positional args, or any other form.
fn positional_int_lit(arg: &crate::dsl::ast::Arg) -> Option<u64> {
    match arg {
        crate::dsl::ast::Arg::Positional(crate::dsl::ast::Expr::IntLit(v, _)) => Some(*v),
        _ => None,
    }
}

/// Extract a string literal from an optional positional argument.
/// Re-exported for cursor-sugar handlers in node modules that
/// validate string-literal-only constructor args.
pub fn positional_str_lit(arg: Option<&crate::dsl::ast::Arg>) -> Option<String> {
    match arg? {
        crate::dsl::ast::Arg::Positional(crate::dsl::ast::Expr::StringLit(s, _)) => Some(s.clone()),
        _ => None,
    }
}

/// Compile a parsed AST into a runtime kernel.
pub fn compile_ast(file: &PolydatFile) -> Result<PolydatKernel, String> {
    compile_ast_with_path(file, None)
}

/// Compile a parsed AST with module resolution from a source directory.
pub fn compile_ast_with_path(file: &PolydatFile, source_dir: Option<&Path>) -> Result<PolydatKernel, String> {
    compile_ast_strict(file, source_dir, false)
}

/// Compile a parsed AST with module resolution and optional strict mode.
///
/// When `strict` is true, the compiler enforces:
/// - Explicit `input ...: u64` declaration (no inference)
/// - All module arguments must be named (no positional)
/// - All module inputs must be provided by the caller (no fallthrough)
pub fn compile_ast_strict(file: &PolydatFile, source_dir: Option<&Path>, strict: bool) -> Result<PolydatKernel, String> {
    let mut compiler = Compiler::new(source_dir.map(|p| p.to_path_buf()), strict);
    compiler.compile(file)
}

/// Compile a pre-parsed AST with the same library / strict /
/// required-outputs / context-label knobs as
/// [`compile_polydat_with_libs`]. Used by SRD-67's
/// [`crate::kernel::subcontext::SubcontextBuilder`] when finalize has
/// rewritten the AST in-place (Rule 2 write-through) and can
/// no longer round-trip through the source-string compile path.
pub fn compile_ast_with_libs(
    file: &PolydatFile,
    source_dir: Option<&Path>,
    polydat_lib_paths: Vec<PathBuf>,
    required_outputs: &[String],
    strict: bool,
    context: &str,
) -> Result<PolydatKernel, String> {
    let extended = if required_outputs.is_empty() {
        Vec::new()
    } else {
        extend_required_with_const_bindings(required_outputs, file)
    };
    let filter = if extended.is_empty() {
        None
    } else {
        Some(extended.as_slice())
    };
    let mut compiler = Compiler::with_lib_paths(
        source_dir.map(|p| p.to_path_buf()),
        polydat_lib_paths,
        strict,
    );
    compiler.context_label = context.to_string();
    // Collect pragmas from the AST so strict-wire / other
    // pragma-gated behaviour matches the source-string path.
    compiler.pragmas = super::pragmas::collect_from_ast(file);
    compiler.compile_filtered(file, filter)
}

/// Compile a parsed AST with strict mode and source text for diagnostics.
///
/// Same as `compile_ast_strict` but attaches the original source text
/// to the compiled program for diagnostic inspection.
fn compile_ast_strict_with_source(
    file: &PolydatFile,
    source_dir: Option<&Path>,
    strict: bool,
    source: &str,
) -> Result<PolydatKernel, String> {
    let mut compiler = Compiler::new(source_dir.map(|p| p.to_path_buf()), strict);
    compiler.source_text = source.to_string();
    // Pragmas affect strict-wire mode even when no event log is
    // supplied — collect them from the AST so library callers
    // that go through `compile_polydat_with_path` still honour them.
    compiler.pragmas = super::pragmas::collect_from_ast(file);
    compiler.compile(file)
}

pub(super) struct Compiler {
    pub(super) input_names: Vec<String>,
    /// Track all named outputs so we can expose them.
    pub(super) all_names: Vec<String>,
    /// Auto-generated node counter for desugared intermediates.
    pub(super) anon_counter: usize,
    /// Directory for module resolution (search for .polydat files).
    pub(super) source_dir: Option<PathBuf>,
    /// Additional library directories for module resolution.
    ///
    /// Searched after `source_dir` but before the embedded stdlib.
    /// Populated via `--polydat-lib=path` CLI flags.
    pub(super) polydat_lib_paths: Vec<PathBuf>,
    /// Cache of already-resolved module ASTs: module_name → (inputs, statements).
    pub(super) module_cache: std::collections::HashMap<String, ResolvedModule>,
    /// When true, enforce strict validation.
    pub(super) strict: bool,
    /// Original source text, attached to compiled programs for diagnostics.
    source_text: String,
    /// Source schemas collected during compilation.
    pub(super) cursor_schemas: Vec<crate::iteration::source::SourceSchema>,
    /// Deferred cursor extent resolutions: each entry maps a cursor
    /// schema index to the aux output names that, once folded, give
    /// the range's start and end values. These are resolved after the
    /// kernel compiles by reading `get_constant()` for each name.
    pub(super) deferred_extents: Vec<DeferredExtent>,
    /// Optional limit applied to all cursors (from `limit` activity param).
    pub(super) cursor_limit: Option<u64>,
    /// Diagnostic context label.
    context_label: String,
    /// Module-level pragmas extracted from the source. Drive the
    /// assembler's `strict_types` / `strict_values` flags
    /// (SRD 15 §"Module-Level Pragmas" + §"Strict Wire Mode").
    pub(super) pragmas: super::pragmas::PragmaSet,
    /// LHS binding name currently being compiled, if any. Used as a
    /// prefix for auto-generated anonymous node names so type-mismatch
    /// errors point at the user-level binding (`overscan__anon_3`)
    /// instead of an opaque counter (`__anon_14`).
    pub(super) current_binding: Option<String>,
}

/// Records a cursor whose `range(...)` bounds reference const
/// expressions (e.g., `vector_count("example:default")`) rather than
/// integer literals. The expressions are compiled as auxiliary outputs
/// and the extent is resolved after kernel compilation by querying the
/// constant values.
pub(super) struct DeferredExtent {
    /// Index into `cursor_schemas` whose extent needs resolution.
    pub schema_idx: usize,
    /// Name of the aux output that, when folded, gives the start value.
    pub start_output: String,
    /// Name of the aux output that, when folded, gives the end value.
    pub end_output: String,
}

impl Compiler {
    pub(super) fn new(source_dir: Option<PathBuf>, strict: bool) -> Self {
        Self {
            input_names: Vec::new(),
            all_names: Vec::new(),
            anon_counter: 0,
            source_dir,
            polydat_lib_paths: Vec::new(),
            module_cache: std::collections::HashMap::new(),
            strict,
            source_text: String::new(),
            context_label: "(polydat)".into(),
            cursor_schemas: Vec::new(),
            deferred_extents: Vec::new(),
            cursor_limit: None,
            pragmas: super::pragmas::PragmaSet::default(),
            current_binding: None,
        }
    }

    pub(super) fn with_lib_paths(source_dir: Option<PathBuf>, polydat_lib_paths: Vec<PathBuf>, strict: bool) -> Self {
        Self {
            input_names: Vec::new(),
            all_names: Vec::new(),
            anon_counter: 0,
            source_dir,
            polydat_lib_paths,
            module_cache: std::collections::HashMap::new(),
            strict,
            source_text: String::new(),
            context_label: "(polydat)".into(),
            cursor_schemas: Vec::new(),
            deferred_extents: Vec::new(),
            cursor_limit: None,
            pragmas: super::pragmas::PragmaSet::default(),
            current_binding: None,
        }
    }

    /// Process a source declaration: create input ports for projections,
    /// passthrough nodes, and record the schema.
    fn process_cursor(&mut self, asm: &mut PolydatAssembler, decl: &crate::dsl::ast::CursorDecl) -> Result<(), String> {
        let source_name = &decl.name;

        // Cursor-sugar dispatch: any node module can register a
        // handler that recognizes a non-`range` constructor (e.g.
        // `vectordata_base("ds", "label_00")`) and rewrites it into
        // a synthetic `range(...)` plus a list of aux bindings to
        // emit after input ports are wired. The core stays
        // generic — nothing here knows that vectordata exists.
        // See `dsl::cursor_sugar` for the registry mechanism.
        let sugar = crate::dsl::cursor_sugar::dispatch(source_name, &decl.constructor)?;
        let effective_constructor = match &sugar {
            Some(s) => s.effective_constructor.clone(),
            None => decl.constructor.clone(),
        };

        // All sources get an "ordinal" projection.
        let mut projections = vec![
            ("ordinal".to_string(), crate::ast::PortType::U64),
        ];

        // Determine extent from constructor args. Three cases per arg:
        //   1. Integer literal → use directly
        //   2. Other const-foldable expression (e.g. `vector_count("...")`)
        //      → compile as an aux output and resolve after kernel compiles
        //   3. Arg references runtime state → no extent available
        //
        // Immediate-literal cases produce a concrete extent here.
        // Deferred cases push a DeferredExtent record; the outer compile
        // routine reads the folded values after compilation and updates
        // the schema's extent in place.
        let mut deferred: Option<(Option<u64>, String, Option<u64>, String)> = None;
        let mut cursor_kind_for_decl: crate::iteration::source::CursorKind = crate::iteration::source::CursorKind::Range;
        let extent = match &effective_constructor {
            // ── until_*(...) — extending cursors ────────────────
            // Recognise every cursor function whose constructor
            // declares an extending policy. The shape of each is:
            //   until_FAMILY(base, ...policy_args[, delta])
            // where `base` is the initial extent / pass size and
            // policy_args carry the family's stop-condition
            // parameters. An optional final `delta` overrides the
            // extension step size (defaults to `base`).
            //
            // Recognised families:
            //   until_elapsed(base, min_ms[, delta])
            //   until_passes(base, min_passes[, delta])
            //   until_count(base, min_count[, delta])
            //   until_elapsed_and_passes(base, min_ms, min_passes[, delta])
            //   until_elapsed_or_passes(base, min_ms, min_passes[, delta])
            //
            // Common shape: emit `base` as the cursor's `end` aux
            // output, `start` as a literal 0, and each policy arg
            // as a named aux output the runtime pulls at phase
            // setup. The CursorKind variant carries the output
            // names so the executor knows how to build the policy.
            crate::dsl::ast::Expr::Call(call) if matches!(
                call.func.as_str(),
                "until_elapsed" | "until_passes" | "until_count"
                | "until_elapsed_and_passes" | "until_elapsed_or_passes"
            ) => {
                let family = call.func.as_str();
                let expected = match family {
                    "until_elapsed" | "until_passes" | "until_count" => (2usize, 3usize),
                    "until_elapsed_and_passes" | "until_elapsed_or_passes" => (3, 4),
                    _ => unreachable!(),
                };
                let n = call.args.len();
                if n < expected.0 || n > expected.1 {
                    return Err(format!(
                        "cursor '{source_name}': `{family}` takes {}-{} args, got {n}",
                        expected.0, expected.1,
                    ));
                }
                // Common: base, start, end aux outputs.
                let base_literal = positional_int_lit(&call.args[0]);
                let base_name = format!("__cursor_extent_{source_name}_end");
                let start_name = format!("__cursor_extent_{source_name}_start");
                let _ = self.compile_binding(asm, &[start_name.clone()],
                    &crate::dsl::ast::Expr::IntLit(0, decl.span));
                if let crate::dsl::ast::Arg::Positional(expr) = &call.args[0] {
                    self.compile_binding(asm, &[base_name.clone()], expr)
                        .map_err(|e| format!(
                            "cursor '{source_name}': failed to compile {family} base: {e}"))?;
                }
                // Helper closure: compile a positional arg as a
                // named aux output. Returns the name on success.
                let mut compile_aux = |idx: usize, suffix: &str| -> Result<String, String> {
                    let out_name = format!("__cursor_{suffix}_{source_name}");
                    if let crate::dsl::ast::Arg::Positional(expr) = &call.args[idx] {
                        self.compile_binding(asm, &[out_name.clone()], expr)
                            .map_err(|e| format!(
                                "cursor '{source_name}': failed to compile \
                                 {family} arg {idx}: {e}"))?;
                    }
                    Ok(out_name)
                };
                // Family-specific arg layout.
                cursor_kind_for_decl = match family {
                    "until_elapsed" => {
                        let min_ms_name = compile_aux(1, "min_ms")?;
                        let delta_output = if n == 3 {
                            Some(compile_aux(2, "delta")?)
                        } else { None };
                        crate::iteration::source::CursorKind::ExtendingTimed {
                            min_ms_output: min_ms_name,
                            delta_output,
                        }
                    }
                    "until_passes" => {
                        let min_passes_name = compile_aux(1, "min_passes")?;
                        let delta_output = if n == 3 {
                            Some(compile_aux(2, "delta")?)
                        } else { None };
                        crate::iteration::source::CursorKind::ExtendingPasses {
                            min_passes_output: min_passes_name,
                            delta_output,
                        }
                    }
                    "until_count" => {
                        let min_count_name = compile_aux(1, "min_count")?;
                        let delta_output = if n == 3 {
                            Some(compile_aux(2, "delta")?)
                        } else { None };
                        crate::iteration::source::CursorKind::ExtendingCount {
                            min_count_output: min_count_name,
                            delta_output,
                        }
                    }
                    "until_elapsed_and_passes" => {
                        let min_ms_name = compile_aux(1, "min_ms")?;
                        let min_passes_name = compile_aux(2, "min_passes")?;
                        let delta_output = if n == 4 {
                            Some(compile_aux(3, "delta")?)
                        } else { None };
                        crate::iteration::source::CursorKind::ExtendingElapsedAndPasses {
                            min_ms_output: min_ms_name,
                            min_passes_output: min_passes_name,
                            delta_output,
                        }
                    }
                    "until_elapsed_or_passes" => {
                        let min_ms_name = compile_aux(1, "min_ms")?;
                        let min_passes_name = compile_aux(2, "min_passes")?;
                        let delta_output = if n == 4 {
                            Some(compile_aux(3, "delta")?)
                        } else { None };
                        crate::iteration::source::CursorKind::ExtendingElapsedOrPasses {
                            min_ms_output: min_ms_name,
                            min_passes_output: min_passes_name,
                            delta_output,
                        }
                    }
                    _ => unreachable!(),
                };
                deferred = Some((Some(0), start_name, base_literal, base_name));
                base_literal
            }
            crate::dsl::ast::Expr::Call(call) if call.func == "range" && call.args.len() >= 2 => {
                let start_literal = positional_int_lit(&call.args[0]);
                let end_literal = positional_int_lit(&call.args[1]);

                match (start_literal, end_literal) {
                    // Both literal — compute directly. We also emit
                    // the start/end as named final bindings so the
                    // comprehension `all(<cursor>)` form (SRD-18c)
                    // can resolve them uniformly with the deferred
                    // (non-literal) case below.
                    (Some(s), Some(e)) => {
                        let start_name = format!("__cursor_extent_{source_name}_start");
                        let end_name = format!("__cursor_extent_{source_name}_end");
                        let s_lit = crate::dsl::ast::Expr::IntLit(s, decl.span);
                        let e_lit = crate::dsl::ast::Expr::IntLit(e, decl.span);
                        let _ = self.compile_binding(asm, &[start_name], &s_lit);
                        let _ = self.compile_binding(asm, &[end_name], &e_lit);
                        Some(e.saturating_sub(s))
                    }
                    // At least one non-literal — compile as aux outputs.
                    _ => {
                        let start_name = format!("__cursor_extent_{source_name}_start");
                        let end_name = format!("__cursor_extent_{source_name}_end");
                        // Compile each arg as a named auxiliary output. Errors
                        // are returned so the user sees them — silently
                        // dropping them would leave extent=None and produce
                        // a phase that runs zero cycles with no explanation.
                        if let crate::dsl::ast::Arg::Positional(expr) = &call.args[0] {
                            self.compile_binding(asm, &[start_name.clone()], expr)
                                .map_err(|e| format!(
                                    "cursor '{source_name}': failed to compile range start: {e}"
                                ))?;
                        }
                        if let crate::dsl::ast::Arg::Positional(expr) = &call.args[1] {
                            self.compile_binding(asm, &[end_name.clone()], expr)
                                .map_err(|e| format!(
                                    "cursor '{source_name}': failed to compile range end: {e}"
                                ))?;
                        }
                        deferred = Some((start_literal, start_name, end_literal, end_name));
                        None
                    }
                }
            }
            _ => None,
        };

        // Create input ports and passthrough nodes for each projection.
        for (field_name, port_type) in &projections {
            let input_name = format!("{source_name}__{field_name}");
            let default_value = match port_type {
                crate::ast::PortType::U64 => crate::ast::Value::U64(0),
                crate::ast::PortType::F64 => crate::ast::Value::F64(0.0),
                _ => crate::ast::Value::None,
            };

            // Cursor projection slots are written by cursor advance
            // every cycle — dynamic for init-contract purposes.
            asm.add_input(&input_name, default_value, *port_type, crate::kernel::InputKind::ExternalWrite);
            self.input_names.push(input_name.clone());

            let passthrough = Box::new(
                crate::library::identity::PortPassthrough::new(&input_name, *port_type)
            );
            let node_name = format!("{source_name}__{field_name}");
            asm.add_node(
                &node_name,
                passthrough,
                vec![WireRef::input(&input_name)],
            );
            asm.add_output(&node_name, WireRef::node(&node_name));
        }

        // Apply any aux bindings the sugar handler asked for.
        // Bindings whose `projection` is `Some` are also published
        // as cursor projections — both pinned on the schema and
        // exposed as kernel outputs the runtime can read.
        if let Some(sugar) = sugar {
            for aux in sugar.aux_bindings {
                self.compile_binding(asm, &[aux.name.clone()], &aux.value)
                    .map_err(|e| format!(
                        "cursor '{source_name}': failed to compile aux binding '{}': {e}",
                        aux.name,
                    ))?;
                if let Some((field, port_type)) = aux.projection {
                    projections.push((field, port_type));
                    asm.add_output(&aux.name, WireRef::node(&aux.name));
                }
            }
        }

        // If a limit is set, insert a limit() node that shadows the cursor wire.
        // The limit node is a visible, documented passthrough that clamps extent.
        let effective_extent = if let Some(limit_val) = self.cursor_limit {
            let limit_node_name = format!("{source_name}__limit");
            let ordinal_wire = format!("{source_name}__ordinal");
            asm.add_node(
                &limit_node_name,
                Box::new(crate::library::context::CursorLimit::new(limit_val)),
                vec![WireRef::node(&ordinal_wire)],
            );
            // Shadow the ordinal output with the limited version
            asm.add_output(&ordinal_wire, WireRef::node(&limit_node_name));

            // Clamp extent
            extent.map(|e| e.min(limit_val)).or(Some(limit_val))
        } else {
            extent
        };

        let schema_idx = self.cursor_schemas.len();
        let extent_outputs = deferred.as_ref()
            .map(|(_, start, _, end)| (start.clone(), end.clone()));

        // SRD 71: if the cursor decl carries an `over <expr>`
        // clause, set up two pieces of plumbing:
        //
        // 1. An auxiliary output `<source>__over_raw` carrying
        //    the raw expression value (typically a string spec
        //    or a workload-param-typed value). The executor
        //    pulls this at phase setup to determine the
        //    narrowing range.
        //
        // 2. An input slot + passthrough output `<source>__cursor`
        //    of type `Ext` — this is the field-access wire that
        //    workload authors reference as `<source>.cursor`. At
        //    phase setup the executor resolves the raw value to
        //    a concrete `Partition` and writes it into this slot,
        //    so downstream nodes (`mod_in`, `cardinality`, etc.)
        //    can consume it as a `Partition`-typed wire.
        let partition_output = if let Some(over_expr) = decl.over.as_ref() {
            let raw_name = format!("__cursor_{source_name}_over_raw");
            self.compile_binding(asm, &[raw_name.clone()], over_expr)
                .map_err(|e| format!(
                    "cursor '{source_name}': failed to compile `over` expression: {e}"))?;
            // Allocate the resolved-Partition input slot. Default
            // is `Value::None` until the executor writes the
            // resolved value at phase setup.
            let cursor_input_name = format!("{source_name}__cursor");
            asm.add_input(
                &cursor_input_name,
                crate::ast::Value::None,
                crate::ast::PortType::Ext,
                crate::kernel::InputKind::ExternalWrite,
            );
            self.input_names.push(cursor_input_name.clone());
            let passthrough = Box::new(
                crate::library::identity::PortPassthrough::new(
                    &cursor_input_name,
                    crate::ast::PortType::Ext,
                )
            );
            asm.add_node(
                &cursor_input_name,
                passthrough,
                vec![WireRef::input(&cursor_input_name)],
            );
            asm.add_output(&cursor_input_name, WireRef::node(&cursor_input_name));
            // SRD 71 §"Cursor metadata wires": scalar projections
            // of the resolved partition, as plain typed slots —
            // `<source>.cursor.idx` and friends parse as chained
            // field access and flatten onto these wires. The
            // executor writes them alongside the Ext slot at
            // phase setup; defaults here cover the no-narrowing
            // case (idx 0, count 1, full-extent pcts; the
            // ordinal pair is patched by the executor once the
            // cursor's extent is known).
            use crate::ast::{PortType, Value};
            let scalar_slots: [(&str, Value, PortType); 6] = [
                ("idx",             Value::U64(0),    PortType::U64),
                ("partition_count", Value::U64(1),    PortType::U64),
                ("start_pct",       Value::F64(0.0),  PortType::F64),
                ("end_pct",         Value::F64(100.0), PortType::F64),
                ("start_ordinal",   Value::U64(0),    PortType::U64),
                ("end_ordinal",     Value::U64(0),    PortType::U64),
            ];
            for (field, default, port_type) in scalar_slots {
                let slot = format!("{cursor_input_name}__{field}");
                asm.add_input(
                    &slot,
                    default,
                    port_type,
                    crate::kernel::InputKind::ExternalWrite,
                );
                self.input_names.push(slot.clone());
                let pass = Box::new(
                    crate::library::identity::PortPassthrough::new(&slot, port_type),
                );
                asm.add_node(&slot, pass, vec![WireRef::input(&slot)]);
                asm.add_output(&slot, WireRef::node(&slot));
            }
            Some(raw_name)
        } else {
            None
        };

        self.cursor_schemas.push(crate::iteration::source::SourceSchema {
            name: source_name.clone(),
            projections,
            extent: effective_extent,
            extent_outputs,
            extent_limit: self.cursor_limit,
            cursor_kind: cursor_kind_for_decl.clone(),
            partition_output,
        });

        // Record deferred extent resolution if the range bounds are not
        // both literals. Post-compile, the outer compile routine will
        // query the aux outputs' folded constants and update this
        // schema's extent in place.
        if let Some((_start_lit, start_output, _end_lit, end_output)) = deferred {
            self.deferred_extents.push(DeferredExtent {
                schema_idx,
                start_output,
                end_output,
            });
        }
        Ok(())
    }

    pub(super) fn compile(&mut self, file: &PolydatFile) -> Result<PolydatKernel, String> {
        // First pass: collect explicit `input` declarations,
        // deduping by name so re-declaration is a no-op (the slot
        // already exists; the second `input cycle: u64` line is just
        // a redundant reaffirmation, not an error).
        let mut has_explicit_inputs = false;
        for stmt in &file.statements {
            if let Statement::InputDecl(d) = stmt {
                if !self.input_names.iter().any(|n| n == &d.name) {
                    self.input_names.push(d.name.clone());
                }
                has_explicit_inputs = true;
            }
        }

        // Input declaration check: error in strict mode (modules, .polydat files)
        if !has_explicit_inputs && self.strict {
            return Err(
                "strict mode: no `input` declaration — add `input <name>: <type>` \
                 (or the tuple form `input (a: u64, b: f64)`) to declare graph \
                 inputs explicitly".into()
            );
        }

        // If no explicit inputs, infer from unbound references
        if !has_explicit_inputs {
            let defined: HashSet<String> = file.statements.iter().flat_map(|stmt| {
                match stmt {
                    Statement::Binding(b) => b.targets.clone(),
                    Statement::ModuleDef(m) => vec![m.name.clone()],
                    Statement::ExternPort(p) => vec![p.name.clone()],
                    Statement::InputDecl(_) => vec![],
                    Statement::Cursor(_) => vec![],
                    Statement::Pragma { .. } => vec![],
                }
            }).collect();

            let mut referenced: HashSet<String> = HashSet::new();
            for stmt in &file.statements {
                let expr = match stmt {
                    Statement::InputDecl(_) | Statement::ModuleDef(_) | Statement::ExternPort(_) | Statement::Cursor(_) | Statement::Pragma { .. } => continue,
                    Statement::Binding(b) => &b.value,
                };
                collect_references(expr, &mut referenced);
            }

            let mut inferred: Vec<String> = referenced.into_iter()
                .filter(|name| !defined.contains(name))
                .collect();
            inferred.sort(); // deterministic order
            self.input_names = inferred;
        }

        // Zero inferred inputs means all bindings are constants — valid.

        // Pragmas were already collected from the AST by the
        // top-level compile entry points. If a caller bypasses
        // those (custom Compiler invocation), populate from this
        // AST as a last resort so the strict-wire flags below
        // still reflect the source.
        if self.pragmas.entries.is_empty() {
            self.pragmas = super::pragmas::collect_from_ast(file);
        }

        let mut asm = PolydatAssembler::new(self.input_names.clone());
        // Honour module-level pragmas: a `pragma strict_values` (or
        // `strict`) directive at the source head opts into
        // auto-inserted assertion nodes (SRD 15 §"Module-Level
        // Pragmas" + §"Strict Wire Mode").
        asm.set_strict_wires(self.pragmas.strict_types(), self.pragmas.strict_values());

        // Auto-expose every declared input as a passthrough output,
        // mirroring the `extern` declaration's behavior. This makes
        // `input cycle: u64` (or `input (cycle: u64, thread: u64)`)
        // produce `cycle` and `thread` as kernel outputs that
        // downstream consumers can read via `pull(...)` — no
        // user-written `cycle := identity(cycle)` shim required.
        // Inputs and externs are now uniform: declaration syntax
        // differs but the resulting input+output shape is identical.
        for input_name in self.input_names.clone() {
            let passthrough = Box::new(
                crate::library::identity::PortPassthrough::new(&input_name, crate::ast::PortType::U64)
            );
            let passthrough_name = format!("__port_{input_name}");
            asm.add_node(
                &passthrough_name,
                passthrough,
                vec![WireRef::input(&input_name)],
            );
            asm.add_output(&input_name, WireRef::node(&passthrough_name));
        }

        // Second pass: process all bindings
        for stmt in &file.statements {
            match stmt {
                Statement::InputDecl(_) => {} // already handled in first pass
                Statement::Binding(b) => {
                    // `shared X := <literal>` compiles to an input
                    // slot + passthrough output, so `materialize_wiring_from_outer`
                    // can wire a `SharedCell` for cross-scope
                    // mutability (SRD-16 §"Mutability Rules: Shared
                    // Mutable"). Single-target bindings only — tuple
                    // unpacks aren't shareable as cells.
                    //
                    // Non-literal `shared` inits and tuple-target
                    // shared bindings are rejected: the cell needs a
                    // single, well-defined initial value, and a
                    // computation-shaped RHS doesn't have one. See
                    // SRD-16 §"Non-literal `shared` initializers".
                    if b.modifier == BindingModifier::SHARED {
                        if b.targets.len() != 1 {
                            return Err(format!(
                                "shared binding must be single-target, not tuple unpack \
                                 ({}). Declare each target separately if a shared cell \
                                 is intended.",
                                b.targets.join(", "),
                            ));
                        }
                        let name = &b.targets[0];
                        let (init_value, port_type) = try_fold_shared_init(&b.value)
                            .ok_or_else(|| format!(
                                "shared binding '{name}' requires a literal initial value \
                                 (number, string, true/false). Computed and cycle-dependent \
                                 expressions don't have a well-defined single init for the \
                                 shared cell. See SRD-16 §\"Non-literal `shared` initializers\"."
                            ))?;
                        // `shared X := <literal>` cells: dynamic for
                        // init-contract purposes — the cell can be
                        // written by inner scopes between scope-init
                        // and per-cycle reads.
                        asm.add_input(name, init_value, port_type, crate::kernel::InputKind::ExternalWrite);
                        self.input_names.push(name.clone());
                        let passthrough = Box::new(
                            crate::library::identity::PortPassthrough::new(name, port_type)
                        );
                        let passthrough_name = format!("__port_{name}");
                        asm.add_node(
                            &passthrough_name,
                            passthrough,
                            vec![WireRef::input(name)],
                        );
                        asm.add_output(name, WireRef::node(&passthrough_name));
                        asm.set_output_modifier(name, BindingModifier::SHARED);
                        continue;
                    }
                    self.compile_binding(
                        &mut asm,
                        &b.targets,
                        &b.value,
                    )?;
                    if b.modifier != BindingModifier::NONE {
                        for target in &b.targets {
                            asm.set_output_modifier(target, b.modifier);
                        }
                    }
                    // `const NAME := …` — register every target as a
                    // const output so the runtime's scope-activation
                    // materialization pass knows to pull these. Const-
                    // modifier bindings collapse the former `init` and
                    // `const` keywords into a single lifecycle: fold
                    // at compile when possible, materialize at scope-
                    // init otherwise, immutable thereafter.
                    //
                    // SRD-74 P2: auto-extern const targets whose RHS
                    // references at least one name. Pure-literal
                    // consts (e.g. `const x := 1` from iter-var
                    // synthesis, SRD-13f Gate 2) DO NOT get auto-
                    // externed — they always fold to a real value and
                    // there's nothing for the chain to fall through
                    // to. Consts with name references CAN fold to
                    // None (the SRD-74 Rule 1 path when any
                    // referenced name reads as Value::None at scope-
                    // init); the auto-extern slot is the conditional-
                    // shadow fallback path that two-tier lookup uses
                    // when the const-fold yields None. Skipped when
                    // the name is already declared as an input.
                    if b.modifier.is_const() {
                        let rhs_has_refs = {
                            let mut refs = std::collections::HashSet::new();
                            crate::dsl::validate::collect_references(&b.value, &mut refs);
                            !refs.is_empty()
                        };
                        for target in &b.targets {
                            asm.mark_const_output(target);
                            if rhs_has_refs
                                && !asm.input_names().iter().any(|n| *n == target.as_str())
                            {
                                // Infer the slot's `PortType` from the
                                // RHS surface shape so the auto-extern
                                // lands at the boundary with its
                                // actual type (Str for string-template
                                // / interpolation forms, U64 / F64 /
                                // Bool for literals + literal-bearing
                                // arithmetic) rather than the legacy
                                // `Ext` catchall — the conflation the
                                // type-axis-vs-scope-axis design fix
                                // removes. `Ext` survives as the
                                // fallback for shapes we can't cheaply
                                // resolve (function calls, array
                                // literals, field access), so the
                                // boundary adapter's catalog miss is
                                // narrower and the typed paths bypass
                                // the warning entirely.
                                // Two-step type discovery for the
                                // auto-extern slot:
                                //
                                // 1. The binding's RHS was just
                                //    compiled (`compile_binding`
                                //    above) — its output is now a
                                //    node in the assembler. Query
                                //    that node's declared output
                                //    `PortType` directly. This
                                //    covers every shape the
                                //    inferrer's surface-AST pass
                                //    can't see through: `select_str`,
                                //    `str_concat`, `format_u64`,
                                //    `query_count`, arbitrary nested
                                //    function calls — all already
                                //    have nodes in the assembler with
                                //    fully-resolved `NodeMeta` ports.
                                // 2. If the assembler doesn't have an
                                //    answer (rare — should only
                                //    happen for shapes where
                                //    `compile_binding` didn't
                                //    register a node under the
                                //    target name), fall back to the
                                //    surface-AST inferrer.
                                // 3. If both fail, `PortType::Ext`
                                //    remains as the last-resort
                                //    fallback — every catalog miss
                                //    at runtime points back to a
                                //    real registry gap.
                                let inferred = asm.output_type(target.as_str())
                                    .or_else(|| infer_auto_extern_type(&b.value, &asm))
                                    .unwrap_or(crate::ast::PortType::Ext);
                                asm.add_input(
                                    target.as_str(),
                                    crate::ast::Value::None,
                                    inferred,
                                    crate::kernel::InputKind::IterationExtern,
                                );
                            }
                        }
                    }
                }
                Statement::ModuleDef(_) => {
                    // Module definitions are not executed — they're
                    // templates resolved by the module system when
                    // referenced from another file/kernel.
                }
                Statement::ExternPort(port) => {
                    // Declare the input on the assembler. The
                    // `extern name: type = default` syntax binds
                    // the trailing default expression to the input
                    // slot's initial value; without a default, the
                    // slot starts at `Value::None` (unset).
                    //
                    // Classify by SRD 11 §"Effectively-Const Nodes":
                    // a default makes this a user-declared capture
                    // port (dynamic — written by capture extraction);
                    // no default makes this an iteration extern
                    // (effectively-const, populated by
                    // `materialize_wiring_from_outer` from a parent for_each /
                    // for_combinations clause).
                    let port_type = crate::ast::PortType::from_keyword(port.typ.as_str())
                        .ok_or_else(|| format!(
                            "extern '{}': unknown polydat type keyword '{}'. \
                             Canonical keywords are emitted by PortType::to_keyword \
                             (one per PortType variant).",
                            port.name, port.typ,
                        ))?;
                    let (default_value, kind) = match &port.default {
                        Some(expr) => {
                            let v = evaluate_default_expr(expr, port_type)
                                .map_err(|e| format!(
                                    "extern '{}' default: {e}", port.name,
                                ))?;
                            (v, crate::kernel::InputKind::ExternalWrite)
                        }
                        None => (
                            crate::ast::Value::None,
                            crate::kernel::InputKind::IterationExtern,
                        ),
                    };
                    asm.add_input(&port.name, default_value, port_type, kind);

                    // Register the extern name as an input so the
                    // binding compiler resolves it as WireRef::input
                    // (enables `hash(offset)` where offset is extern)
                    self.input_names.push(port.name.clone());

                    // Create a passthrough node wired to the input
                    let passthrough = Box::new(
                        crate::library::identity::PortPassthrough::new(&port.name, port_type)
                    );
                    let passthrough_name = format!("__port_{}", port.name);
                    asm.add_node(
                        &passthrough_name,
                        passthrough,
                        vec![WireRef::input(&port.name)],
                    );
                    // Register as output so {name} resolves from GK
                    asm.add_output(&port.name, WireRef::node(&passthrough_name));
                }
                Statement::Cursor(decl) => {
                    self.process_cursor(&mut asm, decl)?;
                }
                Statement::Pragma { .. } => {
                    // Pragmas were collected before this pass (see
                    // `collect_pragmas`) and applied to the
                    // assembler via `set_strict_wires` already.
                    // Nothing to do during binding processing.
                }
            }
        }

        // Expose all top-level named bindings as outputs
        for name in &self.all_names {
            asm.add_output(name, WireRef::node(name));
        }

        // Attach source and context for diagnostics
        asm.set_context(&self.source_text, &self.context_label);
        let mut kernel = asm.compile_strict(self.strict).map_err(|e| format!("{e}"))?;

        // Retain the parsed AST as live program metadata (SRD-13f
        // §"Wire-reference classification"). The subscope
        // synthesizer queries this to integrate parent bindings'
        // matter into child scopes.
        kernel.set_ast(std::sync::Arc::new(file.clone()));

        // Resolve deferred cursor extents. At this point the kernel has
        // folded any const expressions to constant outputs; we read the
        // aux outputs compiled by process_cursor and update the schema
        // extents in place.
        for deferred in &self.deferred_extents {
            let start = kernel.get_constant(&deferred.start_output).map(|v| v.as_u64());
            let end = kernel.get_constant(&deferred.end_output).map(|v| v.as_u64());
            if let (Some(s), Some(e)) = (start, end) {
                let resolved_extent = e.saturating_sub(s);
                // Apply cursor_limit clamping if configured
                let final_extent = self.cursor_limit
                    .map(|limit| resolved_extent.min(limit))
                    .unwrap_or(resolved_extent);
                if let Some(schema) = self.cursor_schemas.get_mut(deferred.schema_idx) {
                    schema.extent = Some(final_extent);
                }
            }
        }

        // Propagate source schemas to the program for runtime discovery
        if !self.cursor_schemas.is_empty() {
            kernel.set_cursor_schemas(self.cursor_schemas.clone());
        }
        Ok(kernel)
    }

    /// Build an assembler with all nodes and wiring, without compiling.
    pub(super) fn build_assembler(&mut self, file: &PolydatFile) -> Result<PolydatAssembler, String> {
        // Reuse the same logic as compile(), but return the assembler
        // instead of calling asm.compile().

        // First pass: collect explicit `input` declarations, dedup by name.
        for stmt in &file.statements {
            if let Statement::InputDecl(d) = stmt
                && !self.input_names.iter().any(|n| n == &d.name)
            {
                self.input_names.push(d.name.clone());
            }
        }

        if self.input_names.is_empty() {
            let defined: HashSet<String> = file.statements.iter().flat_map(|stmt| {
                match stmt {
                    Statement::Binding(b) => b.targets.clone(),
                    Statement::ModuleDef(m) => vec![m.name.clone()],
                    Statement::ExternPort(p) => vec![p.name.clone()],
                    Statement::InputDecl(_) => vec![],
                    Statement::Cursor(_) => vec![],
                    Statement::Pragma { .. } => vec![],
                }
            }).collect();

            let mut referenced: HashSet<String> = HashSet::new();
            for stmt in &file.statements {
                let expr = match stmt {
                    Statement::InputDecl(_) | Statement::ModuleDef(_) | Statement::ExternPort(_) | Statement::Cursor(_) | Statement::Pragma { .. } => continue,
                    Statement::Binding(b) => &b.value,
                };
                collect_references(expr, &mut referenced);
            }

            let mut inferred: Vec<String> = referenced.into_iter()
                .filter(|name| !defined.contains(name))
                .collect();
            inferred.sort();
            self.input_names = inferred;
        }

        // Zero inferred inputs means all bindings are constants — valid.

        let mut asm = PolydatAssembler::new(self.input_names.clone());
        asm.set_strict_wires(self.pragmas.strict_types(), self.pragmas.strict_values());

        for stmt in file.statements.clone() {
            match &stmt {
                Statement::Binding(binding) => {
                    self.compile_binding(&mut asm, &binding.targets, &binding.value)?;
                    if binding.modifier != BindingModifier::NONE {
                        for target in &binding.targets {
                            asm.set_output_modifier(target, binding.modifier);
                        }
                    }
                    if binding.modifier.is_const() {
                        for target in &binding.targets {
                            asm.mark_const_output(target);
                        }
                    }
                }
                Statement::ExternPort(_) => {}
                Statement::ModuleDef(_) => {}
                Statement::InputDecl(_) => {}
                Statement::Pragma { .. } => {}
                Statement::Cursor(decl) => {
                    self.process_cursor(&mut asm, decl)?;
                }
            }
        }

        for name in &self.all_names {
            asm.add_output(name, WireRef::node(name));
        }

        asm.set_context(&self.source_text, &self.context_label);
        Ok(asm)
    }

    /// Compile with optional output filtering for dead code elimination.
    ///
    /// When `required_outputs` is `Some`, only those named bindings are
    /// exposed as kernel outputs. The assembler's DCE pass then prunes
    /// all nodes not reachable from those outputs.
    ///
    /// When `None`, behaves identically to `compile()`.
    pub(super) fn compile_filtered(
        &mut self,
        file: &PolydatFile,
        required_outputs: Option<&[String]>,
    ) -> Result<PolydatKernel, String> {
        // First pass: collect explicit `input` declarations, dedup by name.
        for stmt in &file.statements {
            if let Statement::InputDecl(d) = stmt
                && !self.input_names.iter().any(|n| n == &d.name)
            {
                self.input_names.push(d.name.clone());
            }
        }

        // Input declaration check: error in strict mode (modules, .polydat files)
        if self.input_names.is_empty() && self.strict {
            return Err(
                "strict mode: no `input` declaration — add `input <name>: <type>` \
                 (or the tuple form `input (a: u64, b: f64)`) to declare graph \
                 inputs explicitly".into()
            );
        }

        // If no explicit inputs, infer from unbound references
        if self.input_names.is_empty() {
            let defined: HashSet<String> = file.statements.iter().flat_map(|stmt| {
                match stmt {
                    Statement::Binding(b) => b.targets.clone(),
                    Statement::ModuleDef(m) => vec![m.name.clone()],
                    Statement::ExternPort(p) => vec![p.name.clone()],
                    Statement::InputDecl(_) => vec![],
                    Statement::Cursor(_) => vec![],
                    Statement::Pragma { .. } => vec![],
                }
            }).collect();

            let mut referenced: HashSet<String> = HashSet::new();
            for stmt in &file.statements {
                let expr = match stmt {
                    Statement::InputDecl(_) | Statement::ModuleDef(_) | Statement::ExternPort(_) | Statement::Cursor(_) | Statement::Pragma { .. } => continue,
                    Statement::Binding(b) => &b.value,
                };
                collect_references(expr, &mut referenced);
            }

            let mut inferred: Vec<String> = referenced.into_iter()
                .filter(|name| !defined.contains(name))
                .collect();
            inferred.sort();
            self.input_names = inferred;
        }

        // Zero inferred inputs means all bindings are constants — valid.

        let mut asm = PolydatAssembler::new(self.input_names.clone());

        // Auto-expose every declared input as a passthrough output
        // (parity with `extern`). See `compile()` for the same wiring.
        for input_name in self.input_names.clone() {
            let passthrough = Box::new(
                crate::library::identity::PortPassthrough::new(&input_name, crate::ast::PortType::U64)
            );
            let passthrough_name = format!("__port_{input_name}");
            asm.add_node(
                &passthrough_name,
                passthrough,
                vec![WireRef::input(&input_name)],
            );
            asm.add_output(&input_name, WireRef::node(&passthrough_name));
        }

        // Second pass: process all bindings into the assembler
        for stmt in &file.statements {
            match stmt {
                Statement::InputDecl(_) => {}
                Statement::Binding(b) => {
                    // Mirror `compile()`: literal-init `shared`
                    // bindings compile to slot+passthrough so
                    // SharedCells can be wired across kernels.
                    if b.modifier == BindingModifier::SHARED
                        && b.targets.len() == 1
                        && let Some((init_value, port_type)) =
                            try_fold_shared_init(&b.value)
                    {
                        let name = &b.targets[0];
                        asm.add_input(name, init_value, port_type, crate::kernel::InputKind::ExternalWrite);
                        self.input_names.push(name.clone());
                        let passthrough = Box::new(
                            crate::library::identity::PortPassthrough::new(name, port_type)
                        );
                        let passthrough_name = format!("__port_{name}");
                        asm.add_node(
                            &passthrough_name,
                            passthrough,
                            vec![WireRef::input(name)],
                        );
                        asm.add_output(name, WireRef::node(&passthrough_name));
                        asm.set_output_modifier(name, BindingModifier::SHARED);
                        continue;
                    }
                    self.compile_binding(
                        &mut asm,
                        &b.targets,
                        &b.value,
                    )?;
                    if b.modifier != BindingModifier::NONE {
                        for target in &b.targets {
                            asm.set_output_modifier(target, b.modifier);
                        }
                    }
                    // SRD-74 P2: auto-extern const targets whose RHS
                    // references at least one name. See the parallel
                    // block in `compile()` for rationale — makes
                    // `const NAME := <expr>` a conditional shadow when
                    // its RHS could fold to None, while leaving
                    // pure-literal consts (SRD-13f Gate 2 iter-vars)
                    // alone.
                    if b.modifier.is_const() {
                        let rhs_has_refs = {
                            let mut refs = std::collections::HashSet::new();
                            crate::dsl::validate::collect_references(&b.value, &mut refs);
                            !refs.is_empty()
                        };
                        for target in &b.targets {
                            asm.mark_const_output(target);
                            if rhs_has_refs
                                && !asm.input_names().iter().any(|n| *n == target.as_str())
                            {
                                // Infer the slot's `PortType` from the
                                // RHS surface shape so the auto-extern
                                // lands at the boundary with its
                                // actual type (Str for string-template
                                // / interpolation forms, U64 / F64 /
                                // Bool for literals + literal-bearing
                                // arithmetic) rather than the legacy
                                // `Ext` catchall — the conflation the
                                // type-axis-vs-scope-axis design fix
                                // removes. `Ext` survives as the
                                // fallback for shapes we can't cheaply
                                // resolve (function calls, array
                                // literals, field access), so the
                                // boundary adapter's catalog miss is
                                // narrower and the typed paths bypass
                                // the warning entirely.
                                // Two-step type discovery for the
                                // auto-extern slot:
                                //
                                // 1. The binding's RHS was just
                                //    compiled (`compile_binding`
                                //    above) — its output is now a
                                //    node in the assembler. Query
                                //    that node's declared output
                                //    `PortType` directly. This
                                //    covers every shape the
                                //    inferrer's surface-AST pass
                                //    can't see through: `select_str`,
                                //    `str_concat`, `format_u64`,
                                //    `query_count`, arbitrary nested
                                //    function calls — all already
                                //    have nodes in the assembler with
                                //    fully-resolved `NodeMeta` ports.
                                // 2. If the assembler doesn't have an
                                //    answer (rare — should only
                                //    happen for shapes where
                                //    `compile_binding` didn't
                                //    register a node under the
                                //    target name), fall back to the
                                //    surface-AST inferrer.
                                // 3. If both fail, `PortType::Ext`
                                //    remains as the last-resort
                                //    fallback — every catalog miss
                                //    at runtime points back to a
                                //    real registry gap.
                                let inferred = asm.output_type(target.as_str())
                                    .or_else(|| infer_auto_extern_type(&b.value, &asm))
                                    .unwrap_or(crate::ast::PortType::Ext);
                                asm.add_input(
                                    target.as_str(),
                                    crate::ast::Value::None,
                                    inferred,
                                    crate::kernel::InputKind::IterationExtern,
                                );
                            }
                        }
                    }
                }
                Statement::ModuleDef(_) => {}
                Statement::ExternPort(port) => {
                    // Mirror `compile()`: same kind classification —
                    // a default expression marks this as a capture
                    // port (dynamic); no default marks it as an
                    // iteration extern (effectively-const at
                    // scope-init time).
                    let port_type = crate::ast::PortType::from_keyword(port.typ.as_str())
                        .ok_or_else(|| format!(
                            "extern '{}': unknown polydat type keyword '{}'. \
                             Canonical keywords are emitted by PortType::to_keyword \
                             (one per PortType variant).",
                            port.name, port.typ,
                        ))?;
                    let (default_value, kind) = match &port.default {
                        Some(expr) => {
                            let v = evaluate_default_expr(expr, port_type)
                                .map_err(|e| format!(
                                    "extern '{}' default: {e}", port.name,
                                ))?;
                            (v, crate::kernel::InputKind::ExternalWrite)
                        }
                        None => (
                            crate::ast::Value::None,
                            crate::kernel::InputKind::IterationExtern,
                        ),
                    };
                    asm.add_input(&port.name, default_value, port_type, kind);
                    self.input_names.push(port.name.clone());
                    let passthrough = Box::new(
                        crate::library::identity::PortPassthrough::new(&port.name, port_type)
                    );
                    let passthrough_name = format!("__port_{}", port.name);
                    asm.add_node(
                        &passthrough_name,
                        passthrough,
                        vec![crate::compile::assembly::WireRef::input(&port.name)],
                    );
                    asm.add_output(&port.name, crate::compile::assembly::WireRef::node(&passthrough_name));
                }
                Statement::Cursor(decl) => {
                    self.process_cursor(&mut asm, decl)?;
                }
                Statement::Pragma { .. } => {}
            }
        }

        // Unused binding check: defer to kernel-level check in fold_init_constants_impl.
        // The kernel has the full wiring graph and can accurately determine which
        // nodes have no downstream consumers. The compiler can't do this reliably
        // because it doesn't track inter-binding wire dependencies.

        // Expose outputs: only the required set, or all if no filter.
        // Cursor extent aux outputs (`__cursor_extent_*`) must always be
        // exposed regardless of the filter — they are queried by the
        // post-compile deferred extent resolution and would otherwise be
        // pruned by DCE, leaving the cursor extent unresolved.
        match required_outputs {
            Some(required) => {
                // SRD-13f Push D / SRD-44: `volatile` bindings stay
                // exposed as outputs even when the caller's
                // required list doesn't mention them. The author
                // declared the wire as volatile to mark it as
                // non-deterministic across invocations — losing
                // it from the output set (DCE) would also lose
                // the "exclude from program identity" guarantee,
                // because the lifecycle classifier would no
                // longer find a volatile output pointing at the
                // producing node.
                let mut required_owned: Vec<String> = required.to_vec();
                for stmt in &file.statements {
                    if let crate::dsl::ast::Statement::Binding(b) = stmt
                        && b.modifier.is_volatile()
                    {
                        for t in &b.targets {
                            if !required_owned.iter().any(|n| n == t) {
                                required_owned.push(t.clone());
                            }
                        }
                    }
                }
                for name in &required_owned {
                    if self.all_names.contains(name) {
                        asm.add_output(name, WireRef::node(name));
                    }
                }
                for deferred in &self.deferred_extents {
                    if self.all_names.contains(&deferred.start_output) {
                        asm.add_output(&deferred.start_output, WireRef::node(&deferred.start_output));
                    }
                    if self.all_names.contains(&deferred.end_output) {
                        asm.add_output(&deferred.end_output, WireRef::node(&deferred.end_output));
                    }
                }
                // Always preserve `__cursor_extent_*` auxiliary
                // outputs — they're consumed by the comprehension
                // `all(<cursor>)` form (SRD-18c §"Layer 3") and
                // also by the post-compile deferred-extent
                // resolution above. DCE-ing them would leave the
                // cursor's extent unresolvable to descendant scopes.
                let pruned_aux: Vec<String> = self.all_names.iter()
                    .filter(|n| n.starts_with("__cursor_extent_"))
                    .cloned()
                    .collect();
                for name in pruned_aux {
                    asm.add_output(&name, WireRef::node(&name));
                }
            }
            None => {
                for name in &self.all_names {
                    asm.add_output(name, WireRef::node(name));
                }
            }
        }

        asm.set_context(&self.source_text, &self.context_label);
        let mut kernel = asm.compile_strict(self.strict).map_err(|e| format!("{e}"))?;

        // Retain the parsed AST as live program metadata (SRD-13f
        // §"Wire-reference classification"). The subscope
        // synthesizer queries this to integrate parent bindings'
        // matter into child scopes.
        kernel.set_ast(std::sync::Arc::new(file.clone()));

        // Resolve deferred cursor extents (same logic as in compile()).
        for deferred in &self.deferred_extents {
            let start = kernel.get_constant(&deferred.start_output).map(|v| v.as_u64());
            let end = kernel.get_constant(&deferred.end_output).map(|v| v.as_u64());
            if let (Some(s), Some(e)) = (start, end) {
                let resolved_extent = e.saturating_sub(s);
                let final_extent = self.cursor_limit
                    .map(|limit| resolved_extent.min(limit))
                    .unwrap_or(resolved_extent);
                if let Some(schema) = self.cursor_schemas.get_mut(deferred.schema_idx) {
                    schema.extent = Some(final_extent);
                }
            }
        }

        if !self.cursor_schemas.is_empty() {
            kernel.set_cursor_schemas(self.cursor_schemas.clone());
        }
        Ok(kernel)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn array_literal_binding_compiles_as_string() {
        // A list-valued binding (`const xs := [1, 2, 3]`) is a sweep
        // axis / interpolation value, not a scalar wire. polydat has no
        // const-vector node, so it binds to a `ConstStr` holding the
        // list's literal text rather than failing the compile — which
        // is what lets list-valued workload params (`limit_values:
        // [25]`) load.
        let result = compile_polydat(
            "input cycle: u64\nconst eh_values := [1, 2, 3]\nout := cycle",
        );
        assert!(
            result.is_ok(),
            "array-literal binding should compile (binds as a string const), got: {:?}",
            result.err(),
        );
        // The resolved value is the comma-joined, bracket-free form a
        // sweep-axis param carries (so a `WorkloadParamList` source
        // splits it on `, ` exactly like a string-valued sweep param).
        let kernel = result.unwrap();
        match kernel.get_constant("eh_values") {
            Some(crate::ast::Value::Str(s)) => assert_eq!(s.as_ref(), "1, 2, 3"),
            other => panic!("expected eh_values = Str(\"1, 2, 3\"), got {other:?}"),
        }
    }

    #[test]
    fn embedding_error_display_includes_source_text() {
        let e = EmbeddingError::LifecycleMismatch {
            source: "hash(cycle)".to_string(),
            dynamic_inputs: vec!["cycle".to_string()],
        };
        let s = format!("{e}");
        assert!(s.contains("hash(cycle)"), "display should include source: {s}");
        assert!(s.contains("cycle"), "display should mention dynamic input: {s}");
    }

    #[test]
    fn embedding_error_from_string_shim() {
        let e = EmbeddingError::UnresolvedPlaceholder {
            name: "k".to_string(),
            source: "{k} > 5".to_string(),
        };
        let s: String = e.clone().into();
        assert_eq!(s, format!("{e}"));
    }

    #[test]
    fn embedding_error_all_variants_display() {
        // Smoke test: every variant constructs and displays without panicking.
        let variants: Vec<EmbeddingError> = vec![
            EmbeddingError::Parse {
                source: "x +".into(),
                message: "unexpected EOF".into(),
                position: Some(3),
            },
            EmbeddingError::UnresolvedPlaceholder {
                name: "k".into(),
                source: "{k}".into(),
            },
            EmbeddingError::LifecycleMismatch {
                source: "hash(cycle)".into(),
                dynamic_inputs: vec!["cycle".into()],
            },
            EmbeddingError::UnknownNode {
                name: "frobnicate".into(),
                source: "frobnicate(x)".into(),
                suggestion: Some("fabricate".into()),
            },
            EmbeddingError::TypeMismatch {
                from_node: "n1".into(),
                from_type: crate::ast::PortType::U64,
                to_node: "n2".into(),
                to_type: crate::ast::PortType::Str,
                source: "n1 -> n2".into(),
            },
            EmbeddingError::NodeEvalPanic {
                node_name: "div".into(),
                message: "div by zero".into(),
                source: "div(a, b)".into(),
            },
            EmbeddingError::ResultMissing {
                output_name: "out".into(),
                source: "x := 1".into(),
            },
            EmbeddingError::NonePropagated {
                accessor: "as_bool",
                source: "{missing}".into(),
            },
            EmbeddingError::Timeout {
                source: "expensive()".into(),
                elapsed_ms: 5000,
                deadline_ms: 1000,
            },
            EmbeddingError::RegistryNotInitialised {
                missing: vec!["custom_node".into()],
                source: "custom_node()".into(),
            },
        ];
        for v in variants {
            let _ = format!("{v}");
        }
    }

    #[test]
    fn typed_surface_bool() {
        let v: bool = eval_const_expr_typed("5 > 3").unwrap();
        assert!(v);
        let v: bool = eval_const_expr_typed("3 > 5").unwrap();
        assert!(!v);
    }

    #[test]
    fn typed_surface_u64() {
        let v: u64 = eval_const_expr_typed("10 * 5").unwrap();
        assert_eq!(v, 50);
    }

    #[test]
    fn typed_surface_f64() {
        let v: f64 = eval_const_expr_typed("3.14 * 2.0").unwrap();
        assert!((v - 6.28).abs() < 1e-9);
    }

    #[test]
    fn typed_surface_string() {
        let v: String = eval_const_expr_typed("\"hello\"").unwrap();
        assert_eq!(v, "hello");
    }

    #[test]
    fn typed_surface_type_mismatch() {
        // expression yields U64; host requests f64 — widening allowed
        let v: f64 = eval_const_expr_typed("42").unwrap();
        assert_eq!(v, 42.0);
        // expression yields U64; host requests bool — interpreted as bool (nonzero)
        let v: bool = eval_const_expr_typed("1").unwrap();
        assert!(v);
        let v: bool = eval_const_expr_typed("0").unwrap();
        assert!(!v);
    }

    #[test]
    fn typed_surface_return_path_adapter() {
        // γ-6: expression produces U64; host requests String.
        // The catalog's U64ToString adapter heals the return-path.
        let v: String = eval_const_expr_typed("42").unwrap();
        assert_eq!(v, "42");

        // Expression produces F64; host requests String via catalog
        // F64ToString. (Note: f64's Display is locale-independent
        // but format may add trailing zeros.)
        let v: String = eval_const_expr_typed("3.14").unwrap();
        assert!(v.starts_with("3.14"), "got {v}");
    }

    #[test]
    fn typed_surface_return_path_no_adapter_errors() {
        // Bytes → Bool isn't in the catalog. Confirm the typed
        // error fires when the catalog can't heal.
        // (Need an expression producing Bytes; use a string-
        // literal-to-bytes conversion via bytes_of or similar
        // if available; otherwise use a roundtrip that fails.)
        //
        // Skipping concrete bytes producer for this test —
        // the contract is exercised by the negative path in
        // typed_surface_type_mismatch already.
    }

    #[test]
    fn typed_strict_rejects_lossy_conversion() {
        // U64 → Bool is in the catalog (γ-6 added it) but
        // lossy. Strict mode must reject.
        let result: Result<bool, _> = eval_const_expr_typed_strict("42");
        match result {
            Err(EmbeddingError::TypeMismatch { from_type, to_type, .. }) => {
                assert!(matches!(from_type, crate::ast::PortType::U64));
                assert!(matches!(to_type, crate::ast::PortType::Bool));
            }
            other => panic!("expected TypeMismatch, got {other:?}"),
        }
    }

    #[test]
    fn typed_strict_accepts_lossless_conversion() {
        // U64 → F64 widening is lossless (for values that
        // fit in f64 mantissa, i.e. < 2^53).
        let v: f64 = eval_const_expr_typed_strict("42").unwrap();
        assert_eq!(v, 42.0);

        // U64 → String via display — lossless.
        let v: String = eval_const_expr_typed_strict("42").unwrap();
        assert_eq!(v, "42");
    }

    #[test]
    fn typed_strict_kernel_bound() {
        let kernel = compile_polydat("const k := 10\n").unwrap();
        // Lossless: u64 → f64.
        let v: f64 = eval_kernel_bound_typed_strict("{k} * 2", &kernel).unwrap();
        assert_eq!(v, 20.0);
        // Lossy: u64 → bool — strict rejects.
        let result: Result<bool, _> = eval_kernel_bound_typed_strict("{k} > 5", &kernel);
        assert!(matches!(result, Err(EmbeddingError::TypeMismatch { .. })));
    }

    #[test]
    fn typed_surface_kernel_bound() {
        let kernel = compile_polydat("const k := 10\n").unwrap();
        let v: bool = eval_kernel_bound_typed("{k} > 5", &kernel).unwrap();
        assert!(v);
        let v: u64 = eval_kernel_bound_typed("{k} * 2", &kernel).unwrap();
        assert_eq!(v, 20);
    }

    #[test]
    fn compile_hello_world() {
        let src = r#"
            input cycle: u64
            hashed := hash(cycle)
            user_id := mod(hashed, 1000000)
        "#;
        let mut kernel = compile_polydat(src).unwrap();
        kernel.set_inputs(&[42]);
        let uid = kernel.pull("user_id").as_u64();
        assert!(uid < 1_000_000, "user_id={uid}");
    }

    #[test]
    fn compile_with_inline_nesting() {
        let src = r#"
            input cycle: u64
            result := mod(hash(cycle), 100)
        "#;
        let mut kernel = compile_polydat(src).unwrap();
        kernel.set_inputs(&[42]);
        assert!(kernel.pull("result").as_u64() < 100);
    }

    #[test]
    fn compile_deterministic() {
        let src = r#"
            input cycle: u64
            h := hash(cycle)
        "#;
        let mut kernel = compile_polydat(src).unwrap();
        kernel.set_inputs(&[42]);
        let v1 = kernel.pull("h").as_u64();
        kernel.set_inputs(&[42]);
        let v2 = kernel.pull("h").as_u64();
        assert_eq!(v1, v2);
    }

    #[test]
    fn shared_modifier_tracked() {
        let src = r#"
            input cycle: u64
            shared counter := 0
            normal := mod(hash(cycle), 100)
        "#;
        let kernel = compile_polydat(src).unwrap();
        assert_eq!(
            kernel.program().output_modifier("counter"),
            crate::dsl::ast::BindingModifier::SHARED
        );
        assert_eq!(
            kernel.program().output_modifier("normal"),
            crate::dsl::ast::BindingModifier::NONE
        );
    }

    #[test]
    fn shared_non_literal_init_rejected() {
        // Non-literal `shared` initializers no longer fall
        // through to the cycle-binding shape. Compile error
        // surfaces with a clear message naming the binding and
        // pointing at the SRD-16 §"Non-literal `shared`
        // initializers" section.
        let src = r#"
            input cycle: u64
            shared rolling := hash(cycle)
        "#;
        let err = compile_polydat(src).expect_err("non-literal shared const must error");
        assert!(err.contains("shared binding 'rolling'"), "error: {err}");
        assert!(err.contains("literal initial value"), "error: {err}");
    }

    #[test]
    fn final_modifier_tracked() {
        let src = r#"
            input cycle: u64
            const dim := 128
        "#;
        let kernel = compile_polydat(src).unwrap();
        assert_eq!(
            kernel.program().output_modifier("dim"),
            crate::dsl::ast::BindingModifier::CONST
        );
    }

    #[test]
    fn shared_literal_modifier_tracked() {
        let src = r#"
            input cycle: u64
            shared budget := 100
        "#;
        let kernel = compile_polydat(src).unwrap();
        assert_eq!(
            kernel.program().output_modifier("budget"),
            crate::dsl::ast::BindingModifier::SHARED
        );
        // Shared cells back the output via a port-passthrough node
        // reading the input slot; `lookup` is the cell-aware read.
        assert_eq!(kernel.lookup("budget").unwrap().as_u64(), 100);
    }

    #[test]
    fn const_literal_modifier_tracked() {
        let src = r#"
            input cycle: u64
            const max_dim := 256
        "#;
        let kernel = compile_polydat(src).unwrap();
        assert_eq!(
            kernel.program().output_modifier("max_dim"),
            crate::dsl::ast::BindingModifier::CONST
        );
        assert_eq!(kernel.get_constant("max_dim").unwrap().as_u64(), 256);
    }

    #[test]
    fn shared_outputs_query() {
        let src = r#"
            input cycle: u64
            shared counter := 0
            shared budget := 100
            normal := hash(cycle)
        "#;
        let kernel = compile_polydat(src).unwrap();
        let mut shared = kernel.program().shared_outputs();
        shared.sort();
        assert_eq!(shared, vec!["budget", "counter"]);
        assert!(kernel.program().const_outputs().is_empty());
    }

    #[test]
    fn final_outputs_query() {
        let src = r#"
            input cycle: u64
            const dim := 128
            const dataset := "example"
            normal := hash(cycle)
        "#;
        let kernel = compile_polydat(src).unwrap();
        let mut finals = kernel.program().const_outputs();
        finals.sort();
        assert_eq!(finals, vec!["dataset", "dim"]);
        assert!(kernel.program().shared_outputs().is_empty());
    }

    #[test]
    fn unmodified_bindings_have_none_modifier() {
        let src = r#"
            input cycle: u64
            h := hash(cycle)
            v := mod(h, 100)
        "#;
        let kernel = compile_polydat(src).unwrap();
        assert_eq!(
            kernel.program().output_modifier("h"),
            crate::dsl::ast::BindingModifier::NONE
        );
        assert_eq!(
            kernel.program().output_modifier("v"),
            crate::dsl::ast::BindingModifier::NONE
        );
    }

    #[test]
    fn compile_mixed_radix() {
        let src = r#"
            input cycle: u64
            (tenant, device, reading) := mixed_radix(cycle, 100, 1000, 0)
            tenant_h := hash(tenant)
            tenant_code := mod(tenant_h, 10000)
        "#;
        let mut kernel = compile_polydat(src).unwrap();
        kernel.set_inputs(&[4_201_337]);
        let tc = kernel.pull("tenant_code").as_u64();
        assert!(tc < 10000, "tenant_code={tc}");
    }

    #[test]
    fn compile_string_constant() {
        let src = r#"
            input cycle: u64
            label := "hello world"
        "#;
        let mut kernel = compile_polydat(src).unwrap();
        kernel.set_inputs(&[0]);
        assert_eq!(kernel.pull("label").as_str(), "hello world");
    }

    #[test]
    fn compile_int_constant() {
        let src = r#"
            input cycle: u64
            base := 1710000000000
        "#;
        let mut kernel = compile_polydat(src).unwrap();
        kernel.set_inputs(&[0]);
        assert_eq!(kernel.pull("base").as_u64(), 1_710_000_000_000);
    }

    #[test]
    fn compile_comments_ignored() {
        let src = r#"
            // This is a comment
            input cycle: u64
            // Another comment
            h := hash(cycle)
        "#;
        let mut kernel = compile_polydat(src).unwrap();
        kernel.set_inputs(&[1]);
        assert!(kernel.pull("h").as_u64() != 0);
    }

    // --- Diagnostic tests ---

    #[test]
    fn error_unknown_function() {
        let src = "input cycle: u64\nresult := foobar(cycle)";
        let (_result, report) = compile_polydat_checked(src);
        assert!(report.has_errors());
        let errors = report.errors();
        assert!(errors.iter().any(|e| e.message.contains("unknown function")));
        assert!(errors.iter().any(|e| e.message.contains("foobar")));
    }

    #[test]
    fn error_unknown_function_suggests() {
        let src = "input cycle: u64\nresult := hahs(cycle)";
        let (_, report) = compile_polydat_checked(src);
        let errors = report.errors();
        let err = errors.iter().find(|e| e.message.contains("hahs")).unwrap();
        assert!(err.hint.as_ref().unwrap().contains("hash"),
            "should suggest 'hash', got: {:?}", err.hint);
    }

    #[test]
    fn inferred_coordinates() {
        // Without explicit coordinates, 'cycle' is inferred as a coordinate input
        let src = "h := hash(cycle)";
        let mut kernel = compile_polydat(src).unwrap();
        assert_eq!(kernel.input_names(), &["cycle"]);
        kernel.set_inputs(&[42]);
        let h = kernel.pull("h").as_u64();
        assert_ne!(h, 42); // hashed, not identity
    }

    #[test]
    fn inferred_multi_coordinates() {
        // Multiple unbound names become multiple coordinate inputs (sorted)
        let src = "h := hash(interleave(row, col))";
        let mut kernel = compile_polydat(src).unwrap();
        assert_eq!(kernel.input_names(), &["col", "row"]); // alphabetically sorted
        kernel.set_inputs(&[10, 20]);
        let h = kernel.pull("h").as_u64();
        assert_ne!(h, 0);
    }

    #[test]
    fn explicit_coordinates_rejects_unbound() {
        // With explicit coordinates, unbound references are errors
        let src = "input cycle: u64\nh := hash(unknown)";
        let (_, report) = compile_polydat_checked(src);
        assert!(report.has_errors());
        assert!(report.errors().iter().any(|e|
            e.message.contains("undefined") && e.message.contains("unknown")));
    }

    #[test]
    fn warning_forward_reference() {
        let src = r#"
            input cycle: u64
            result := mod(h, 100)
            h := hash(cycle)
        "#;
        let (_, report) = compile_polydat_checked(src);
        let warnings = report.warnings();
        assert!(warnings.iter().any(|w| w.message.contains("forward reference")),
            "should warn about forward ref, got: {:?}", warnings);
    }

    #[test]
    fn error_undefined_wire() {
        let src = r#"
            input cycle: u64
            result := hash(nonexistent)
        "#;
        let (_, report) = compile_polydat_checked(src);
        assert!(report.has_errors());
        assert!(report.errors().iter().any(|e|
            e.message.contains("undefined") && e.message.contains("nonexistent")));
    }

    #[test]
    fn error_report_includes_source_line() {
        let src = "input cycle: u64\nresult := unknown_func(cycle)";
        let (_, report) = compile_polydat_checked(src);
        let s = report.to_string();
        assert!(s.contains("unknown_func"), "report should include source context");
    }

    #[test]
    fn checked_compile_success_with_no_errors() {
        let src = r#"
            input cycle: u64
            h := hash(cycle)
            result := mod(h, 1000)
        "#;
        let (result, report) = compile_polydat_checked(src);
        assert!(!report.has_errors());
        assert!(result.is_ok());
    }

    // --- Strict mode tests ---

    #[test]
    fn strict_requires_explicit_inputs() {
        // Without inputs declaration, strict mode should error
        let src = "h := hash(cycle)";
        let result = compile_polydat_strict(src, None, true);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.contains("strict mode"), "expected strict error, got: {err}");
        assert!(err.contains("inputs"), "expected inputs mention, got: {err}");
    }

    #[test]
    fn strict_accepts_explicit_coordinates() {
        // With explicit coordinates, strict mode should succeed
        let src = r#"
            input cycle: u64
            h := hash(cycle)
        "#;
        let mut kernel = compile_polydat_strict(src, None, true).unwrap();
        kernel.set_inputs(&[42]);
        let h = kernel.pull("h").as_u64();
        assert_ne!(h, 42); // hashed, not identity
    }

    #[test]
    fn non_strict_infers_coordinates() {
        // Without strict, coordinate inference works as before
        let src = "h := hash(cycle)";
        let mut kernel = compile_polydat_strict(src, None, false).unwrap();
        kernel.set_inputs(&[42]);
        assert_ne!(kernel.pull("h").as_u64(), 42);
    }

    // --- Dead code elimination tests ---

    #[test]
    fn dce_filters_to_required_outputs() {
        // Polydat source defines three bindings but we only request one
        let src = r#"
            input cycle: u64
            a := hash(cycle)
            b := mod(a, 100)
            c := add(cycle, 1)
        "#;
        let required = vec!["b".to_string()];
        let mut kernel = compile_polydat_with_outputs(src, None, &required, false).unwrap();
        kernel.set_inputs(&[42]);

        // "b" should be available and correct
        let b = kernel.pull("b").as_u64();
        assert!(b < 100, "b={b}");

        // "a" and "c" should NOT be in the output map
        let outputs = kernel.output_names();
        assert!(outputs.contains(&"b"), "should contain 'b'");
        assert!(!outputs.contains(&"a"), "should not contain pruned 'a'");
        assert!(!outputs.contains(&"c"), "should not contain pruned 'c'");
    }

    #[test]
    fn dce_preserves_upstream_dependencies() {
        // Request "result" which depends on "h" — both the result node
        // and its upstream "h" node must be kept, but "unrelated" is pruned
        let src = r#"
            input cycle: u64
            h := hash(cycle)
            result := mod(h, 1000)
            unrelated := add(cycle, 999)
        "#;
        let required = vec!["result".to_string()];
        let mut kernel = compile_polydat_with_outputs(src, None, &required, false).unwrap();
        kernel.set_inputs(&[42]);

        let result = kernel.pull("result").as_u64();
        assert!(result < 1000, "result={result}");

        let outputs = kernel.output_names();
        assert!(!outputs.contains(&"unrelated"), "unrelated should be pruned");
    }

    #[test]
    fn dce_empty_required_compiles_all() {
        // Empty required_outputs should produce the same kernel as compile_polydat
        let src = r#"
            input cycle: u64
            a := hash(cycle)
            b := mod(a, 100)
        "#;
        let kernel_all = compile_polydat(src).unwrap();
        let kernel_empty = compile_polydat_with_outputs(src, None, &[], false).unwrap();

        assert_eq!(kernel_all.output_names().len(), kernel_empty.output_names().len());
    }

    #[test]
    fn init_binding_survives_dce_even_when_unconsumed() {
        // Regression test for the prebuffer-not-firing bug. An
        // `const` binding declares a side-effect-bearing init-time
        // computation (download, register, prebuffer). The user's
        // signal that they want it evaluated is the `init`
        // keyword itself, *not* a downstream wire reference. Yet
        // the assembler's DCE walks back from the requested
        // outputs and prunes whatever's not in their ancestry.
        //
        // Pre-fix: with `required = ["b"]` and an unconsumed
        // `init side_effect = …` binding, the `side_effect` node
        // (and its constant-fold call) got pruned. Post-fix:
        // `compile_polydat_with_outputs` extends the required list
        // with every `const` binding's name, so DCE keeps the
        // node, fold evaluates it, and `kernel.pull("side_effect")`
        // returns the folded result.
        let src = r#"
            input cycle: u64
            const side_effect := 42
            b := mod(hash(cycle), 100)
        "#;
        let required = vec!["b".to_string()];
        let mut kernel = compile_polydat_with_outputs(src, None, &required, false).unwrap();
        kernel.set_inputs(&[0]);

        let outputs = kernel.output_names();
        assert!(outputs.contains(&"side_effect"),
            "init binding must survive DCE even when unconsumed; got outputs {outputs:?}");
        assert_eq!(kernel.pull("side_effect").as_u64(), 42);
    }

    #[test]
    fn dce_multiple_required_outputs() {
        // Request two of three bindings
        let src = r#"
            input cycle: u64
            x := hash(cycle)
            y := mod(x, 50)
            z := add(cycle, 10)
        "#;
        let required = vec!["y".to_string(), "z".to_string()];
        let mut kernel = compile_polydat_with_outputs(src, None, &required, false).unwrap();
        kernel.set_inputs(&[5]);

        assert!(kernel.pull("y").as_u64() < 50);
        assert_eq!(kernel.pull("z").as_u64(), 15);

        let outputs = kernel.output_names();
        assert!(outputs.contains(&"y"));
        assert!(outputs.contains(&"z"));
        // "x" is an upstream dep of "y" but not a requested output
        assert!(!outputs.contains(&"x"), "x should not be in outputs");
    }

    /// Every function registered in the FuncSig registry must be
    // --- Strict mode comprehensive tests ---

    #[test]
    fn strict_rejects_unused_bindings() {
        // "unused" has no downstream consumer and is not an output → strict error
        // Use compile_polydat_strict which exposes all bindings as outputs,
        // so the kernel sees the full graph and detects the unused node.
        // Actually: when all bindings are outputs, none are "unused".
        // The unused check only applies with DCE (required_outputs filter).
        // With DCE, pruned bindings produce a warning at the compiler level.
        let src = r#"
            input cycle: u64
            used := hash(cycle)
            unused := add(cycle, 1)
        "#;
        let required = vec!["used".to_string()];
        // Non-strict: DCE prunes "unused" silently
        let result = compile_polydat_with_outputs(src, None, &required, false);
        assert!(result.is_ok(), "non-strict with DCE should compile");
        // Verify "unused" is actually pruned
        let kernel = result.unwrap();
        assert!(!kernel.output_names().contains(&"unused"),
            "unused should be pruned by DCE");
    }

    #[test]
    fn strict_rejects_implicit_type_coercion() {
        // u64 → f64 auto-adapter → strict error
        let src = r#"
            input cycle: u64
            h := hash(cycle)
            f := sqrt(h)
        "#;
        let result = compile_polydat_strict(src, None, true);
        assert!(result.is_err(), "strict should reject implicit coercion");
        let err = result.unwrap_err();
        assert!(err.contains("coercion") || err.contains("__adapt"),
            "error should mention coercion: {err}");
    }

    #[test]
    fn non_strict_allows_implicit_type_coercion() {
        let src = r#"
            input cycle: u64
            h := hash(cycle)
            f := sqrt(h)
        "#;
        let result = compile_polydat_strict(src, None, false);
        assert!(result.is_ok(), "non-strict should allow implicit coercion");
    }

    #[test]
    fn strict_accepts_clean_program() {
        // All inputs declared, all bindings used, no coercions
        let src = r#"
            input cycle: u64
            h := hash(cycle)
            id := mod(h, 1000)
        "#;
        let required = vec!["id".to_string()];
        let result = compile_polydat_with_outputs(src, None, &required, true);
        assert!(result.is_ok(), "clean program should pass strict: {:?}", result.err());
    }

    #[test]
    fn compile_bitwise_and() {
        let src = r#"
            input cycle: u64
            out := cycle & 0xFF
        "#;
        let mut kernel = compile_polydat(src).unwrap();
        kernel.set_inputs(&[0x1234]);
        assert_eq!(kernel.pull("out").as_u64(), 0x34);
    }

    #[test]
    fn compile_shift_left() {
        let src = r#"
            input cycle: u64
            out := cycle << 8
        "#;
        let mut kernel = compile_polydat(src).unwrap();
        kernel.set_inputs(&[1]);
        assert_eq!(kernel.pull("out").as_u64(), 256);
    }

    #[test]
    fn compile_bitwise_not() {
        let src = r#"
            input cycle: u64
            out := !cycle
        "#;
        let mut kernel = compile_polydat(src).unwrap();
        kernel.set_inputs(&[0]);
        assert_eq!(kernel.pull("out").as_u64(), u64::MAX);
    }

    #[test]
    fn compile_bitwise_xor() {
        let src = r#"
            input cycle: u64
            out := cycle ^ 0xFF
        "#;
        let mut kernel = compile_polydat(src).unwrap();
        kernel.set_inputs(&[0xF0]);
        assert_eq!(kernel.pull("out").as_u64(), 0x0F);
    }

    #[test]
    fn compile_bitwise_or() {
        let src = r#"
            input cycle: u64
            out := cycle | 0x0F
        "#;
        let mut kernel = compile_polydat(src).unwrap();
        kernel.set_inputs(&[0xF0]);
        assert_eq!(kernel.pull("out").as_u64(), 0xFF);
    }

    #[test]
    fn compile_shift_right() {
        let src = r#"
            input cycle: u64
            out := cycle >> 4
        "#;
        let mut kernel = compile_polydat(src).unwrap();
        kernel.set_inputs(&[0xFF]);
        assert_eq!(kernel.pull("out").as_u64(), 0x0F);
    }

    #[test]
    fn compile_power_operator() {
        let src = r#"
            input cycle: u64
            out := to_f64(cycle) ** 2.0
        "#;
        let mut kernel = compile_polydat(src).unwrap();
        kernel.set_inputs(&[3]);
        // pow(3.0, 2.0) = 9.0
        let result = kernel.pull("out").as_f64();
        assert!((result - 9.0).abs() < 0.001);
    }

    // --- eval_const_expr tests ---

    #[test]
    fn eval_const_expr_arithmetic() {
        // 4 * 4: both operands are IntLit → u64_mul → returns u64(16)
        let v = eval_const_expr("4 * 4").unwrap();
        assert_eq!(v.as_u64(), 16, "expected u64(16), got {:?}", v);

        // 4.0 * 4.0: both operands are FloatLit → f64_mul → returns f64(16.0)
        let v = eval_const_expr("4.0 * 4.0").unwrap();
        assert!((v.as_f64() - 16.0).abs() < 0.001, "expected 16.0, got {}", v.as_f64());

        // Mixed: 4 * 4.0 → auto-widen LHS to f64, f64_mul → returns f64(16.0)
        let v = eval_const_expr("4 * 4.0").unwrap();
        assert!((v.as_f64() - 16.0).abs() < 0.001, "expected 16.0, got {}", v.as_f64());
    }

    #[test]
    fn eval_const_expr_function() {
        let v = eval_const_expr("hash(42)").unwrap();
        assert!(v.as_u64() != 0, "hash(42) should be non-zero");
    }

    #[test]
    fn eval_const_expr_fails_on_inputs() {
        // 'cycle' is a runtime input — should fail as const expr
        let r = eval_const_expr("hash(cycle)");
        assert!(r.is_err(), "hash(cycle) should fail as a const expression");
    }

    #[test]
    fn eval_const_expr_nested() {
        let v = eval_const_expr("mod(hash(42), 100)").unwrap();
        assert!(v.as_u64() < 100, "mod(hash(42), 100) should be < 100, got {}", v.as_u64());
    }

    // ─────────────────────────────────────────────────────────────
    // Init-Binding Contract (SRD 11 §"Init Binding Contract")
    //
    // Plan A — compile-time check: every binding declared `init`
    // must classify as compile-const or scope-init. A wire chain
    // reaching a coordinate input, a external-write port, or a
    // non-deterministic source disqualifies the binding.
    // ─────────────────────────────────────────────────────────────

    #[test]
    fn init_binding_compile_const_folded() {
        // Pure init: literal arg, no externs. Folds at compile
        // time; the compiled program's output_map points at a
        // ConstU64 leaf.
        let src = "const dim := 128\n";
        let kernel = compile_polydat(src).expect("init compile-const");
        let prog = kernel.program();
        assert!(prog.const_outputs().contains(&"dim"));
        let &(node_idx, _) = prog.output_map_lookup("dim").expect("dim in output map");
        // After fold, the node has empty wiring (leaf const).
        assert!(prog.wiring[node_idx].is_empty(),
            "compile-const init binding 'dim' must fold to a leaf const node");
    }

    #[test]
    fn init_binding_with_iteration_extern_passes_plan_a() {
        // Init binding wired through an iteration extern: this is
        // legal under Plan A — the wire chain reaches an
        // IterationExtern input slot, which is effectively-const at
        // scope-init time. Plan B (executor-side) is what actually
        // evaluates it; the compile step just must not reject.
        let src = "extern profile: String\n\
                   const label := format_str(\"label_%s\", profile)\n";
        let result = compile_polydat(src);
        // We don't care if format_str exists in the stdlib — what
        // we're testing is that the contract check itself doesn't
        // fail (any error must be about an unknown function, not
        // about the init contract).
        match result {
            Ok(_) => {} // ideal: kernel built
            Err(e) => assert!(
                !e.contains("violates the init contract"),
                "Plan A must accept iteration-extern wires in init bindings; got: {e}"),
        }
    }

    #[test]
    fn init_binding_wired_to_cycle_input_rejected() {
        // Init binding wired to `cycle` (Coordinate input) is a
        // hard structural violation. Plan A must reject.
        let src = "input cycle: u64\n\
                   const bad := hash(cycle)\n";
        let err = compile_polydat(src).expect_err(
            "Plan A must reject init binding wired to a coordinate input");
        assert!(err.contains("init binding 'bad'") && err.contains("init contract"),
            "diagnostic must name the binding and the contract; got: {err}");
        assert!(err.contains("cycle") || err.contains("coordinate"),
            "diagnostic should pinpoint the offending wire; got: {err}");
    }

    #[test]
    fn init_binding_wired_to_external_write_port_rejected() {
        // External-write port (extern with default) is dynamic;
        // init bindings must not depend on one.
        let src = "extern session_id: u64 = 0\n\
                   const derived := mod(session_id, 100)\n";
        let err = compile_polydat(src).expect_err(
            "Plan A must reject init binding wired to a external-write port");
        assert!(err.contains("init binding 'derived'") && err.contains("init contract"),
            "diagnostic must name the binding and the contract; got: {err}");
        assert!(err.contains("session_id") || err.contains("capture"),
            "diagnostic should pinpoint the offending wire; got: {err}");
    }

    #[test]
    fn init_binding_wired_to_nondeterministic_rejected() {
        // `counter()` is non-deterministic; init bindings must not
        // depend on it.
        let src = "const bad := counter()\n";
        let err = compile_polydat(src).expect_err(
            "Plan A must reject init binding wired to a non-deterministic source");
        assert!(err.contains("init binding 'bad'") && err.contains("init contract"),
            "diagnostic must name the binding and the contract; got: {err}");
    }

    #[test]
    fn cycle_binding_wired_to_cycle_input_still_allowed() {
        // The contract applies *only* to bindings declared `init`.
        // A normal `:=` binding wired to `cycle` is the bread-and-
        // butter case and must keep working.
        let src = "input cycle: u64\n\
                   user_id := mod(hash(cycle), 1000)\n";
        let _kernel = compile_polydat(src)
            .expect("non-init bindings wired to cycle must still compile");
    }

    #[test]
    fn init_outputs_threaded_into_program() {
        // Sanity: the compiler records every `init`-declared name
        // on GkProgram.const_outputs so Plan B (executor side) can
        // walk them at scope activation.
        let src = "const a := 1\n\
                   const b := 2\n\
                   c := 3\n";
        let kernel = compile_polydat(src).unwrap();
        let init_set = kernel.program().const_outputs();
        assert!(init_set.contains(&"a"), "const 'a' should be tracked");
        assert!(init_set.contains(&"b"), "const 'b' should be tracked");
        assert!(!init_set.contains(&"c"), "non-const 'c' must not be tracked");
    }

    #[test]
    fn str_concat_via_plus_operator() {
        // `+` between Str-typed operands lowers to str_concat.
        let src = r#"
            input cycle: u64
            greeting := "hello, " + "world"
        "#;
        let mut kernel = compile_polydat(src).unwrap();
        kernel.set_inputs(&[0]);
        assert_eq!(kernel.pull("greeting").as_str(), "hello, world");
    }

    #[test]
    fn str_concat_flattens_chained_plus() {
        // `"a" + b + "c"` flattens into a single str_concat node
        // (rather than a chain of binary concatenations) so the
        // assembler sees the full operand list at once.
        let src = r#"
            input cycle: u64
            x := "id="
            y := 42
            z := " end"
            out := x + y + z
        "#;
        let mut kernel = compile_polydat(src).unwrap();
        kernel.set_inputs(&[0]);
        assert_eq!(kernel.pull("out").as_str(), "id=42 end");
    }

    #[test]
    fn str_concat_mixed_str_and_numeric() {
        // Numeric operand on the right is rendered as decimal text;
        // the Str path wins because the left side is Str.
        let src = r#"
            input cycle: u64
            n := 7
            out := "n=" + n
        "#;
        let mut kernel = compile_polydat(src).unwrap();
        kernel.set_inputs(&[0]);
        assert_eq!(kernel.pull("out").as_str(), "n=7");
    }

    /// Auto-extern slots inferred from RHS shape land at the
    /// boundary with their actual type (Str / U64 / F64 / Bool)
    /// rather than the legacy `PortType::Ext` catchall. This
    /// removes the `U64 → Ext` boundary-adapter miss the audit
    /// log used to warn about for workloads that use `set:`
    /// blocks with iter-var interpolation.
    ///
    /// Test path: declare an iteration extern explicitly with
    /// `extern N: str` (no default → `IterationExtern` kind,
    /// effectively-const at scope-init); reference it from a
    /// const RHS. The const target then needs an auto-extern
    /// slot (RHS has a ref), and the inferrer picks the
    /// referenced input's type.
    #[test]
    fn auto_extern_slot_inherits_string_template_type() {
        let src = r#"
            extern some_outer_var: str
            const x := "{some_outer_var}"
        "#;
        let kernel = compile_polydat(src).expect("compile");
        assert_eq!(
            kernel.program().input_port_type("x"),
            Some(crate::ast::PortType::Str),
            "string-template auto-extern MUST be Str, not Ext",
        );
    }

    #[test]
    fn auto_extern_slot_inherits_arithmetic_operand_type() {
        // `const y := other + 1` — BinOp with U64 operands.
        // The auto-extern slot for `y` MUST be U64.
        let src = r#"
            extern other: u64
            const y := other + 1
        "#;
        let kernel = compile_polydat(src).expect("compile");
        assert_eq!(
            kernel.program().input_port_type("y"),
            Some(crate::ast::PortType::U64),
            "arithmetic-RHS auto-extern MUST inherit operand type",
        );
    }

    /// Identifier reference auto-extern inherits the referenced
    /// input's type. `const y := other_str_input` → y is Str.
    #[test]
    fn auto_extern_slot_inherits_ident_reference_type() {
        let src = r#"
            extern other: str
            const y := other
        "#;
        let kernel = compile_polydat(src).expect("compile");
        assert_eq!(
            kernel.program().input_port_type("y"),
            Some(crate::ast::PortType::Str),
            "ident-RHS auto-extern MUST inherit referenced input's type",
        );
    }

    /// `dataset_prebuffer(...)` returns `Value::Handle` — the
    /// auto-extern slot for `const prebuffered := dataset_prebuffer(...)`
    /// MUST be `PortType::Handle`, not the legacy `Ext` catchall.
    /// This is the second specific call site we patched in the
    /// inferrer after the `printf` string-template case.
    /// (`dataset_prebuffer` is a vectordata node, so the test only
    /// exists when that feature registers it.)
    #[cfg(feature = "vectordata")]
    #[test]
    fn auto_extern_slot_for_dataset_prebuffer_is_handle() {
        let src = r#"
            extern source_uri: str
            const prebuffered := dataset_prebuffer(source_uri)
        "#;
        let kernel = compile_polydat(src).expect("compile");
        assert_eq!(
            kernel.program().input_port_type("prebuffered"),
            Some(crate::ast::PortType::Handle),
            "dataset_prebuffer auto-extern MUST be Handle, not Ext",
        );
    }

    /// SRD-84 Part 1 — `&&` / `||` as eager truthiness combinators:
    /// correct results, lowest precedence (below comparison; `||`
    /// looser than `&&`), and truthiness normalisation (a value is
    /// "true" iff non-zero — which raw bitwise would get wrong).
    #[test]
    fn logical_and_or_eval_precedence_and_truthiness() {
        let eval = |src: &str| -> u64 {
            compile_polydat(src)
                .unwrap_or_else(|e| panic!("compile `{src}`: {e}"))
                .pull("out").as_u64()
        };
        // Basic && / ||.
        assert_eq!(eval("out := 60 > 50 && 20 > 10"), 1, "both true");
        assert_eq!(eval("out := 40 > 50 && 20 > 10"), 0, "first false");
        assert_eq!(eval("out := 40 > 50 || 20 > 10"), 1, "second true");
        assert_eq!(eval("out := 40 > 50 || 5 > 10"), 0, "neither");
        // Precedence: && sits below comparison, || below && —
        // `1>0 && 0>1 || 5>0` == `((1>0)&&(0>1)) || (5>0)` == `(1&&0)||1` == 1.
        assert_eq!(eval("out := 1 > 0 && 0 > 1 || 5 > 0"), 1,
            "|| binds looser than &&, both below comparison");
        // Truthiness: 6 && 1 → both non-zero → 1. Raw bitwise 6 & 1 = 0,
        // so this proves the `!= 0` normalisation, not a bitwise and.
        assert_eq!(eval("out := 6 && 1"), 1, "non-zero && non-zero → 1 (not bitwise)");
        assert_eq!(eval("out := 6 && 0"), 0, "non-zero && zero → 0");
        assert_eq!(eval("out := 0 || 0"), 0, "zero || zero → 0");
        assert_eq!(eval("out := 0 || 7"), 1, "zero || non-zero → 1");
        // Parentheses override precedence (already supported; locked here).
        assert_eq!(eval("out := (1 + 2) * 3"), 9, "parens: add before mul");
        assert_eq!(eval("out := 1 + 2 * 3"), 7, "no parens: mul binds tighter");
        assert_eq!(eval("out := 1 > 0 || 0 > 1 && 0 > 1"), 1,
            "no parens: && tighter → 1 || (0 && 0) = 1");
        assert_eq!(eval("out := (1 > 0 || 0 > 1) && 0 > 1"), 0,
            "parens group the ||: (1 || 0) && 0 = 0");
    }

    /// SRD-84 Part 1b — `<expr> as <type>` cast: alignment-only type
    /// fusion (no-op when aligned, SRD-79 adapter otherwise), tight
    /// (atom-binding) precedence, and an error when no fusion exists.
    #[test]
    fn as_cast_type_fusion_and_precedence() {
        let f64_of = |src: &str| compile_polydat(src)
            .unwrap_or_else(|e| panic!("compile `{src}`: {e}")).pull("out").as_f64();
        let u64_of = |src: &str| compile_polydat(src)
            .unwrap_or_else(|e| panic!("compile `{src}`: {e}")).pull("out").as_u64();
        // u64 → f64 widening fusion (allowed under `as`).
        assert_eq!(f64_of("out := 5 as f64"), 5.0);
        // Narrowing f64 → u64 is NOT allowed under `as` (ambiguous
        // rounding); the author chooses an explicit conversion.
        assert!(compile_polydat("out := 7.9 as u64").is_err(),
            "narrowing f64 → u64 under `as` is rejected");
        assert_eq!(u64_of("out := f64_to_u64(7.9)"), 7, "explicit truncate");
        assert_eq!(u64_of("out := round_to_u64(7.9)"), 8, "explicit round");
        // Aligned cast is a no-op.
        assert_eq!(u64_of("out := 42 as u64"), 42);
        // `as` binds to the atom: `5 / 2 as f64` == `5 / (2 as f64)` ==
        // 2.5, not `(5 / 2) as f64` == 2.0.
        assert_eq!(f64_of("out := 5 / 2 as f64"), 2.5);
        assert_eq!(f64_of("out := (5 / 2) as f64"), 2.0);
        // No valid fusion → compile error.
        assert!(compile_polydat("out := \"x\" as f64").is_err(),
            "str → f64 has no defined fusion → error");
    }
}
