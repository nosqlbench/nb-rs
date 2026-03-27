// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! Structured error types for the GK DSL with rich source context.
//!
//! Every error includes a source location (line:col), the relevant
//! source text, and a clear message with suggestions where possible.

use crate::dsl::lexer::Span;
use std::fmt;

/// Severity level for diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Severity {
    Error,
    Warning,
}

/// A single diagnostic message with source context.
#[derive(Debug, Clone)]
pub struct Diagnostic {
    pub severity: Severity,
    pub span: Span,
    pub message: String,
    pub hint: Option<String>,
    /// The source line text (for display).
    pub source_line: Option<String>,
}

impl Diagnostic {
    pub fn error(span: Span, message: impl Into<String>) -> Self {
        Self {
            severity: Severity::Error,
            span,
            message: message.into(),
            hint: None,
            source_line: None,
        }
    }

    pub fn warning(span: Span, message: impl Into<String>) -> Self {
        Self {
            severity: Severity::Warning,
            span,
            message: message.into(),
            hint: None,
            source_line: None,
        }
    }

    pub fn with_hint(mut self, hint: impl Into<String>) -> Self {
        self.hint = Some(hint.into());
        self
    }

    pub fn with_source_line(mut self, line: impl Into<String>) -> Self {
        self.source_line = Some(line.into());
        self
    }
}

impl fmt::Display for Diagnostic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let severity = match self.severity {
            Severity::Error => "error",
            Severity::Warning => "warning",
        };
        write!(f, "{}:{}:{}: {}", self.span.line, self.span.col, severity, self.message)?;

        if let Some(ref line) = self.source_line {
            write!(f, "\n  | {line}")?;
            // Underline the position
            if self.span.col > 0 {
                let padding = " ".repeat(self.span.col - 1);
                write!(f, "\n  | {padding}^")?;
            }
        }

        if let Some(ref hint) = self.hint {
            write!(f, "\n  = hint: {hint}")?;
        }

        Ok(())
    }
}

/// A collection of diagnostics from compilation.
#[derive(Debug, Clone)]
pub struct DiagnosticReport {
    pub diagnostics: Vec<Diagnostic>,
    /// The original source text (for extracting source lines).
    source_lines: Vec<String>,
}

impl DiagnosticReport {
    pub fn new(source: &str) -> Self {
        Self {
            diagnostics: Vec::new(),
            source_lines: source.lines().map(|l| l.to_string()).collect(),
        }
    }

    pub fn error(&mut self, span: Span, message: impl Into<String>) {
        let mut diag = Diagnostic::error(span, message);
        if span.line > 0 && span.line <= self.source_lines.len() {
            diag.source_line = Some(self.source_lines[span.line - 1].clone());
        }
        self.diagnostics.push(diag);
    }

    pub fn error_with_hint(&mut self, span: Span, message: impl Into<String>, hint: impl Into<String>) {
        let mut diag = Diagnostic::error(span, message).with_hint(hint);
        if span.line > 0 && span.line <= self.source_lines.len() {
            diag.source_line = Some(self.source_lines[span.line - 1].clone());
        }
        self.diagnostics.push(diag);
    }

    pub fn warning(&mut self, span: Span, message: impl Into<String>) {
        let mut diag = Diagnostic::warning(span, message);
        if span.line > 0 && span.line <= self.source_lines.len() {
            diag.source_line = Some(self.source_lines[span.line - 1].clone());
        }
        self.diagnostics.push(diag);
    }

    pub fn warning_with_hint(&mut self, span: Span, message: impl Into<String>, hint: impl Into<String>) {
        let mut diag = Diagnostic::warning(span, message).with_hint(hint);
        if span.line > 0 && span.line <= self.source_lines.len() {
            diag.source_line = Some(self.source_lines[span.line - 1].clone());
        }
        self.diagnostics.push(diag);
    }

    pub fn has_errors(&self) -> bool {
        self.diagnostics.iter().any(|d| d.severity == Severity::Error)
    }

    pub fn errors(&self) -> Vec<&Diagnostic> {
        self.diagnostics.iter().filter(|d| d.severity == Severity::Error).collect()
    }

    pub fn warnings(&self) -> Vec<&Diagnostic> {
        self.diagnostics.iter().filter(|d| d.severity == Severity::Warning).collect()
    }
}

impl fmt::Display for DiagnosticReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, diag) in self.diagnostics.iter().enumerate() {
            if i > 0 { writeln!(f)?; }
            write!(f, "{diag}")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn diagnostic_display() {
        let diag = Diagnostic::error(Span { line: 3, col: 12 }, "unknown function 'foobar'")
            .with_hint("did you mean 'hash'?")
            .with_source_line("  result := foobar(cycle)");
        let s = diag.to_string();
        assert!(s.contains("3:12:error"));
        assert!(s.contains("unknown function"));
        assert!(s.contains("foobar(cycle)"));
        assert!(s.contains("did you mean"));
    }

    #[test]
    fn report_collects() {
        let mut report = DiagnosticReport::new("line1\nline2\nline3");
        report.error(Span { line: 1, col: 1 }, "first error");
        report.warning(Span { line: 2, col: 5 }, "a warning");
        report.error(Span { line: 3, col: 1 }, "second error");
        assert!(report.has_errors());
        assert_eq!(report.errors().len(), 2);
        assert_eq!(report.warnings().len(), 1);
    }

    #[test]
    fn report_includes_source_line() {
        let mut report = DiagnosticReport::new("coordinates := (cycle)\nbad := ???");
        report.error(Span { line: 2, col: 8 }, "unexpected token");
        let s = report.to_string();
        assert!(s.contains("bad := ???"));
    }
}
