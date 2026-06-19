// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Phase-name filter compiled from a `phases=<pattern>` CLI arg.
//!
//! Three input dialects, picked by the shape of the source string:
//!
//! - **Bareword** (no regex metachars, no `*`): full-string match
//!   against the phase name. `phases=schema` matches exactly the
//!   phase named `schema`, nothing else.
//! - **Glob** (only `*` as the special char among regex metachars):
//!   `*` expands to `.*`. `phases=jolokia_*` matches every phase
//!   whose name starts with `jolokia_`.
//! - **Regex** (anything else): treated as a Rust `regex::Regex`
//!   pattern with implicit anchors so a bare class like `[a-z]+`
//!   still has to match the whole name.
//!
//! Compilation is one-shot at session start (planner reads the
//! `phases=` param, calls [`PhasePattern::parse`], stores the
//! resulting matcher on the scene-tree filter walk). Per-phase
//! match decisions are pure read-only against the resulting
//! `Regex` so the planner can apply the filter without taking
//! any mutexes.

use regex::Regex;

/// Characters whose presence in an input means "this is a
/// regular expression already, don't glob-expand". Excludes `*`
/// because the glob dialect uses `*` and only `*`.
const REGEX_METACHARS: &[char] = &[
    '.', '+', '?', '(', ')', '|', '[', ']', '{', '}',
    '^', '$', '\\',
];

/// Compiled phase-name pattern. Wraps a `regex::Regex` plus the
/// original source for diagnostics.
#[derive(Debug, Clone)]
pub struct PhasePattern {
    /// The compiled matcher.
    re: Regex,
    /// The original source string the user passed (kept verbatim
    /// for log messages so a typo'd pattern surfaces back in
    /// `phases=<source>` form).
    source: String,
    /// Which dialect the source was interpreted as. Surfaced in
    /// the "phases=… matched N/M" log line so operators see what
    /// the runner thought they meant.
    dialect: PhaseDialect,
}

/// Which input dialect [`PhasePattern::parse`] picked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PhaseDialect {
    /// Source had no regex metachars and no `*` — treated as a
    /// literal full-string match.
    Literal,
    /// Source had `*` (and no other regex metachars) — each
    /// `*` expanded to `.*` and the result wrapped in anchors.
    Glob,
    /// Source had at least one regex metachar — treated as a
    /// full Rust regex, anchored so a bare class still has to
    /// match the entire name.
    Regex,
}

impl PhaseDialect {
    pub fn as_str(self) -> &'static str {
        match self {
            PhaseDialect::Literal => "literal",
            PhaseDialect::Glob    => "glob",
            PhaseDialect::Regex   => "regex",
        }
    }
}

impl PhasePattern {
    /// Compile a `phases=<source>` string into a `PhasePattern`.
    /// Empty source → error (the caller should treat `phases=`
    /// without a value as "no filter set" before this function
    /// is even reached).
    pub fn parse(source: &str) -> Result<Self, String> {
        if source.is_empty() {
            return Err("phases= pattern is empty".into());
        }
        let has_regex_metachar = source.chars().any(|c| REGEX_METACHARS.contains(&c));
        let has_glob_star = source.contains('*');
        let (anchored, dialect) = if has_regex_metachar {
            // Full regex dialect — wrap in anchors so a partial
            // match doesn't accidentally fire across the whole
            // namespace.
            (format!("^(?:{source})$"), PhaseDialect::Regex)
        } else if has_glob_star {
            // Glob: escape every literal char, then turn each
            // escaped `\*` into `.*`. regex::escape() produces
            // `\*` for `*`, which is the marker we replace.
            let escaped = regex::escape(source);
            let pattern = escaped.replace("\\*", ".*");
            (format!("^{pattern}$"), PhaseDialect::Glob)
        } else {
            // Literal: escape the whole string and anchor it.
            (format!("^{}$", regex::escape(source)), PhaseDialect::Literal)
        };
        let re = Regex::new(&anchored).map_err(|e| {
            format!("phases='{source}' did not compile (dialect={}): {e}",
                dialect.as_str())
        })?;
        Ok(Self { re, source: source.to_string(), dialect })
    }

    /// Whether `phase_name` matches the compiled pattern.
    pub fn is_match(&self, phase_name: &str) -> bool {
        self.re.is_match(phase_name)
    }

    /// The original source string the user passed.
    pub fn source(&self) -> &str { &self.source }

    /// Which dialect the source was interpreted as.
    pub fn dialect(&self) -> PhaseDialect { self.dialect }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn literal_full_string_match() {
        let p = PhasePattern::parse("schema").unwrap();
        assert_eq!(p.dialect(), PhaseDialect::Literal);
        assert!(p.is_match("schema"));
        assert!(!p.is_match("schemaplus"));
        assert!(!p.is_match("pre_schema"));
    }

    #[test]
    fn glob_star_expands_anchored() {
        let p = PhasePattern::parse("jolokia_*").unwrap();
        assert_eq!(p.dialect(), PhaseDialect::Glob);
        assert!(p.is_match("jolokia_flush"));
        assert!(p.is_match("jolokia_compact"));
        assert!(p.is_match("jolokia_"));
        assert!(!p.is_match("post_jolokia_flush"));
        assert!(!p.is_match("jolokia"));
    }

    #[test]
    fn glob_middle_star_expands() {
        let p = PhasePattern::parse("pre*post").unwrap();
        assert_eq!(p.dialect(), PhaseDialect::Glob);
        assert!(p.is_match("pre_test_post"));
        assert!(p.is_match("prepost"));
        assert!(!p.is_match("pretest"));
    }

    #[test]
    fn regex_metachar_promotes_to_full_regex() {
        let p = PhasePattern::parse("(setup|teardown)").unwrap();
        assert_eq!(p.dialect(), PhaseDialect::Regex);
        assert!(p.is_match("setup"));
        assert!(p.is_match("teardown"));
        assert!(!p.is_match("setup2"));
    }

    #[test]
    fn regex_charclass_anchored() {
        let p = PhasePattern::parse("[a-z]+").unwrap();
        assert_eq!(p.dialect(), PhaseDialect::Regex);
        assert!(p.is_match("schema"));
        assert!(!p.is_match("Schema"));  // anchored, capital S breaks the class
        assert!(!p.is_match("schema_1"));  // anchored, digit breaks the class
    }

    #[test]
    fn empty_source_errors() {
        assert!(PhasePattern::parse("").is_err());
    }

    #[test]
    fn invalid_regex_errors_with_source() {
        let err = PhasePattern::parse("(unclosed").unwrap_err();
        assert!(err.contains("(unclosed"));
        assert!(err.contains("dialect=regex"));
    }
}
