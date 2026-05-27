// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Diagnostic types: [`SourceContext`] and [`ContractViolation`].

use crate::node::PortType;

use super::name::ChildName;

/// Diagnostic context attached to a [`super::ScopeModule`] —
/// where the module's source came from. Used in error messages
/// when a contract violation surfaces at spawn or finalize.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SourceContext {
    /// Logical label — workload phase / op-template name / SRD
    /// reference. Free-form; appears verbatim in diagnostics.
    pub label: String,
    /// Source file path, if applicable.
    pub file: Option<String>,
    /// Line range `(start, end)` if known.
    pub line_range: Option<(usize, usize)>,
}

impl SourceContext {
    pub fn new(label: impl Into<String>) -> Self {
        Self {
            label: label.into(),
            file: None,
            line_range: None,
        }
    }

    pub fn for_phase(name: &str) -> Self {
        Self::new(format!("phase:{name}"))
    }

    pub fn for_op(name: &str) -> Self {
        Self::new(format!("op:{name}"))
    }

    pub fn with_file(mut self, file: impl Into<String>) -> Self {
        self.file = Some(file.into());
        self
    }

    pub fn with_lines(mut self, start: usize, end: usize) -> Self {
        self.line_range = Some((start, end));
        self
    }

    /// Render as a single line for error messages.
    pub fn display(&self) -> String {
        let mut s = self.label.clone();
        if let Some(f) = &self.file {
            s.push_str(&format!(" ({f}"));
            if let Some((a, b)) = self.line_range {
                s.push_str(&format!(":{a}-{b}"));
            }
            s.push(')');
        } else if let Some((a, b)) = self.line_range {
            s.push_str(&format!(" ({a}-{b})"));
        }
        s
    }
}

/// Contract violation surfaced at finalize or spawn.
///
/// Variants per SRD-67 §"Cross-binding rules" plus the umbrella
/// [`Self::Compile`] for errors raised by the GK compiler when
/// the body fragment is converted into a program (typically an
/// unbound identifier — Rule 1's catch-all under Phase 1
/// semantics).
#[derive(Debug, Clone)]
pub enum ContractViolation {
    /// Rule 1 — Import resolution: an artifact import has no
    /// matching parent export.
    UnboundImport {
        import: String,
        site: SourceContext,
    },
    /// Rule 1 — Type mismatch on import.
    Type {
        import: String,
        required: PortType,
        parent_export: PortType,
        site: SourceContext,
    },
    /// Rule 1 — Modifier mismatch (e.g. shared import against a
    /// non-shared parent export).
    Modifier {
        import: String,
        detail: String,
        site: SourceContext,
    },
    /// Rule 2 — Final-shadow on export: a child can't redefine
    /// an immutable parent export.
    FinalShadow {
        export: String,
        site: SourceContext,
    },
    /// Rule 2 — Shared write-through rewrite was required but
    /// could not be performed.
    ///
    /// Retained as a public variant for source compatibility
    /// with Phase 1 callers that pattern-matched on it; Phase 2
    /// (this push) implements the rewrite at finalize, so the
    /// builder no longer raises this variant. New code should
    /// match on the rewrite's success / failure via the
    /// resulting [`super::ScopeModule::write_throughs`] and the
    /// underlying [`Self::Compile`] surface.
    Phase2WriteThrough {
        export: String,
        site: SourceContext,
        note: &'static str,
    },
    /// Named-child registry: a duplicate spawn under the same
    /// name (SRD-67 §"Named-child registry"). Reports both spawn
    /// sites.
    DuplicateChild {
        name: ChildName,
        prior_site: SourceContext,
        this_site: SourceContext,
    },
    /// GK compile-time error — the body failed to compile (most
    /// commonly: unbound identifier; corresponds to Rule 1's
    /// closure-binding economy detecting a free identifier with
    /// no matching import).
    Compile(String),
}

impl std::fmt::Display for ContractViolation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnboundImport { import, site } => write!(
                f,
                "unbound import `{import}` (parent does not export it) at {}",
                site.display()
            ),
            Self::Type {
                import,
                required,
                parent_export,
                site,
            } => write!(
                f,
                "type mismatch on import `{import}`: required {required:?}, parent exports {parent_export:?} at {}",
                site.display()
            ),
            Self::Modifier { import, detail, site } => write!(
                f,
                "modifier mismatch on import `{import}`: {detail} at {}",
                site.display()
            ),
            Self::FinalShadow { export, site } => write!(
                f,
                "child export `{export}` shadows parent's `final` export at {}",
                site.display()
            ),
            Self::Phase2WriteThrough { export, site, note } => write!(
                f,
                "TODO(Phase 2): write-through rewrite for shared export `{export}` at {} — {note}",
                site.display()
            ),
            Self::DuplicateChild {
                name,
                prior_site,
                this_site,
            } => write!(
                f,
                "duplicate spawn of child `{name}`: prior at {}, this at {}",
                prior_site.display(),
                this_site.display()
            ),
            Self::Compile(msg) => write!(f, "compile error: {msg}"),
        }
    }
}

impl std::error::Error for ContractViolation {}
