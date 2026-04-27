// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Module-level pragmas for GK source.
//!
//! Pragmas are first-class GK statements the module author places at
//! the head of a `.gk` file or module body to opt into compile-time
//! graph transforms. Today they cover assertion-injection modes that
//! complement the const-constraint metadata (SRD 15):
//!
//! ```gk
//! pragma strict_values
//! pragma strict_types
//! pragma strict          // convenience alias for both
//!
//! id := mod(hash(cycle), 1000)
//! ```
//!
//! `pragma` is a reserved keyword in the GK grammar; pragmas are
//! [`Statement::Pragma`] in the AST and walked by the compiler the
//! same way other statements are. They're not comments — distinct
//! syntactic construct, distinguishable from `//`/`#` line comments.
//!
//! [`Statement::Pragma`]: crate::dsl::ast::Statement::Pragma
//!
//! ## Recognised pragma names
//!
//! - `strict_types` — auto-insert type assertion nodes on wires
//!   whose source can't be statically proven to deliver the right
//!   `PortType`. *Design target* — see SRD 15 §"Strict Wire Mode".
//! - `strict_values` — auto-insert value assertion nodes on wires
//!   whose downstream node declares a value constraint the source
//!   can't satisfy at compile time.
//! - `strict` — alias for both `strict_types` + `strict_values`.
//!
//! Unknown pragmas are recorded but warned about, not errored:
//! pragmas are forward-compatible by design so old binaries can
//! parse modules that opt into newer features they don't yet
//! support.
//!
//! ## Scoping (SRD 15 §"Pragma Scope")
//!
//! Each GK graph or module has its own [`PragmaSet`]. Inner
//! contexts inherit the outer scope's pragmas automatically — an
//! enclosing `strict_values` applies to every nested module body
//! attached to it. On conflict (an inner pragma whose effective
//! value disagrees with an outer one), the outer scope wins; a
//! warning is emitted in non-strict compilation, and the conflict
//! becomes a hard error in `--strict` mode.

/// One pragma entry parsed from the source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pragma {
    /// Bare pragma name (e.g. `"strict_values"`).
    pub name: String,
    /// Whitespace-separated arguments after the name, if any.
    pub args: Vec<String>,
    /// 1-based line number where the pragma appeared, for diagnostics.
    pub line: usize,
}

/// All pragmas declared in one GK scope. Multiple `PragmaSet`s
/// chain via [`PragmaSet::with_parent`] to model nested scopes
/// (workload → phase → `for_each` iteration). Per SRD 13b
/// §"Scope composition" + SRD 15 §"Pragma Scope": each scope is
/// its own `PragmaSet`, the chain is walked at lookup time, and
/// outer scopes win on conflict.
#[derive(Debug, Clone, Default)]
pub struct PragmaSet {
    pub entries: Vec<Pragma>,
    /// Outer scope, if any. Lookups walk this chain after their
    /// own entries miss; conflicts are detected at attach time
    /// via [`PragmaSet::attach_to`]. The `Arc` keeps the outer
    /// scope cheap to share across many child scopes (e.g. one
    /// workload scope feeding a fan-out of phase scopes).
    pub parent: Option<std::sync::Arc<PragmaSet>>,
}

impl PragmaSet {
    /// Returns true if the named pragma is present in this scope
    /// or any enclosing scope.
    pub fn contains(&self, name: &str) -> bool {
        if self.entries.iter().any(|p| p.name == name) {
            return true;
        }
        match &self.parent {
            Some(p) => p.contains(name),
            None => false,
        }
    }

    /// Returns true if either `strict_types` or the `strict` alias
    /// is set in this scope or any enclosing scope.
    pub fn strict_types(&self) -> bool {
        self.contains("strict_types") || self.contains("strict")
    }

    /// Returns true if either `strict_values` or the `strict`
    /// alias is set in this scope or any enclosing scope.
    pub fn strict_values(&self) -> bool {
        self.contains("strict_values") || self.contains("strict")
    }

    /// Iterate pragmas this scope declares that the compiler
    /// doesn't recognise. Local-only — does not walk parents (the
    /// outer scope already reported its own unknowns at its own
    /// compile time).
    pub fn unknown(&self) -> impl Iterator<Item = &Pragma> {
        self.entries.iter().filter(|p| !is_known(&p.name))
    }

    /// Attach this `PragmaSet` to an outer scope, returning
    /// `(attached, conflicts)`. Conflicts arise when this scope
    /// declares a pragma whose effective value (currently just
    /// `args`) differs from a same-named declaration in the
    /// outer chain. Outer wins; the conflict is returned for
    /// diagnostic reporting.
    ///
    /// The caller decides what to do with conflicts:
    /// - non-strict: emit warning event(s)
    /// - strict: turn each conflict into a compile error
    ///
    /// Today's pragma vocabulary is presence-only so `args` is
    /// always empty; conflicts are degenerate. The framework is
    /// in place for future value-bearing pragmas.
    pub fn attach_to(self, outer: std::sync::Arc<PragmaSet>) -> (PragmaSet, Vec<PragmaConflict>) {
        let mut conflicts = Vec::new();
        for entry in &self.entries {
            // Walk the outer chain looking for a same-named
            // declaration with disagreeing args.
            let mut cursor: &PragmaSet = outer.as_ref();
            loop {
                if let Some(existing) = cursor.entries.iter().find(|p| p.name == entry.name)
                    && existing.args != entry.args
                {
                    conflicts.push(PragmaConflict {
                        name: entry.name.clone(),
                        outer_line: existing.line,
                        inner_line: entry.line,
                    });
                    break;
                }
                match &cursor.parent {
                    Some(p) => cursor = p.as_ref(),
                    None => break,
                }
            }
        }
        let attached = PragmaSet {
            entries: self.entries,
            parent: Some(outer),
        };
        (attached, conflicts)
    }
}

/// Recognised pragma names. Add new names here as features land.
fn is_known(name: &str) -> bool {
    matches!(name, "strict_types" | "strict_values" | "strict")
}

/// Walk a parsed AST and collect every `Statement::Pragma` into a
/// [`PragmaSet`]. This is the canonical extraction path — pragmas
/// are first-class grammar (the `pragma` keyword) and the parser
/// produces them as proper statements.
pub fn collect_from_ast(file: &crate::dsl::ast::GkFile) -> PragmaSet {
    use crate::dsl::ast::Statement;
    let mut entries = Vec::new();
    for stmt in &file.statements {
        if let Statement::Pragma { name, span } = stmt {
            entries.push(Pragma {
                name: name.clone(),
                args: Vec::new(),
                line: span.line,
            });
        }
    }
    PragmaSet { entries, parent: None }
}

/// A pragma that disagreed across nested scopes. Used by
/// [`PragmaSet::attach_to`] to surface conflicts up to the caller
/// for either advisory logging (non-strict) or hard error (strict).
/// Per SRD 15 §"Pragma Scope" + SRD 13b §"Scope composition", the
/// outer scope's value wins; the conflict report is for
/// diagnostics, not for resolution.
#[derive(Debug, Clone)]
pub struct PragmaConflict {
    pub name: String,
    pub outer_line: usize,
    pub inner_line: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dsl::lexer::{lex, Span};
    use crate::dsl::parser::parse;

    fn pragmas_from(src: &str) -> PragmaSet {
        let tokens = lex(src).expect("lex");
        let ast = parse(tokens).expect("parse");
        collect_from_ast(&ast)
    }

    #[test]
    fn parse_strict_alias() {
        let set = pragmas_from("pragma strict\nid := cycle\n");
        assert!(set.strict_types());
        assert!(set.strict_values());
    }

    #[test]
    fn parse_individual_modes() {
        let set = pragmas_from("pragma strict_types\npragma strict_values\nid := cycle\n");
        assert!(set.strict_types());
        assert!(set.strict_values());
    }

    #[test]
    fn unknown_pragmas_are_collected() {
        let set = pragmas_from("pragma warp_drive\npragma strict\nid := cycle\n");
        assert!(set.strict_types());
        let unknown: Vec<_> = set.unknown().collect();
        assert_eq!(unknown.len(), 1);
        assert_eq!(unknown[0].name, "warp_drive");
    }

    #[test]
    fn attached_inherits_outer_pragmas() {
        let outer = std::sync::Arc::new(PragmaSet {
            entries: vec![Pragma {
                name: "strict_values".into(),
                args: vec![],
                line: 1,
            }],
            parent: None,
        });
        let inner = PragmaSet::default();
        let (attached, conflicts) = inner.attach_to(outer);
        assert!(attached.strict_values(), "inner should see outer's strict_values via parent walk");
        assert!(conflicts.is_empty());
    }

    #[test]
    fn attached_local_pragma_wins_for_unrelated_names() {
        // Outer says strict_types, inner adds strict_values. No
        // conflict — both apply via the chain walk.
        let outer = std::sync::Arc::new(PragmaSet {
            entries: vec![Pragma {
                name: "strict_types".into(),
                args: vec![],
                line: 1,
            }],
            parent: None,
        });
        let inner = PragmaSet {
            entries: vec![Pragma {
                name: "strict_values".into(),
                args: vec![],
                line: 5,
            }],
            parent: None,
        };
        let (attached, conflicts) = inner.attach_to(outer);
        assert!(attached.strict_types());
        assert!(attached.strict_values());
        assert!(conflicts.is_empty());
    }

    #[test]
    fn attached_records_arg_conflict() {
        // Forward-compat scenario: a value-bearing pragma like
        // `assert_for(name)` that disagrees across scopes. The
        // keyword grammar doesn't accept args today, so build the
        // PragmaSet by hand. Outer wins; conflict is reported.
        let outer = std::sync::Arc::new(PragmaSet {
            entries: vec![Pragma {
                name: "assert_for".into(),
                args: vec!["alpha".into()],
                line: 1,
            }],
            parent: None,
        });
        let inner = PragmaSet {
            entries: vec![Pragma {
                name: "assert_for".into(),
                args: vec!["beta".into()],
                line: 5,
            }],
            parent: None,
        };
        let (_attached, conflicts) = inner.attach_to(outer);
        assert_eq!(conflicts.len(), 1);
        assert_eq!(conflicts[0].name, "assert_for");
        assert_eq!(conflicts[0].outer_line, 1);
        assert_eq!(conflicts[0].inner_line, 5);
    }
}
