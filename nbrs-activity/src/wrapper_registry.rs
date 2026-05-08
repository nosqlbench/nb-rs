// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! SRD-32a — Op Wrapper Registry.
//!
//! Single source of truth for the named wrappers that compose
//! around an adapter's base dispenser: which op-template fields
//! each owns, what triggers them, what constraints they declare,
//! and how they describe their assignment to an op.
//!
//! The companion module [`crate::wrapper_resolver`] consumes the
//! registrations submitted via `inventory::submit!` to compute
//! the per-op composition order. The wrapper implementations
//! themselves live in [`crate::wrappers`] and
//! [`crate::validation`] — this module is data, not code.

use nbrs_workload::model::ParsedOp;

/// Stable identifier for a wrapper. Used in workload override
/// directives, CLI flags, and registry lookups. Wrapped around a
/// `&'static str` so the value is always interned at the
/// registration site and lookups are pointer-cheap.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct WrapperName(pub &'static str);

impl WrapperName {
    pub const fn new(name: &'static str) -> Self {
        Self(name)
    }

    pub fn as_str(&self) -> &'static str {
        self.0
    }
}

impl std::fmt::Display for WrapperName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}

/// One entry per registered wrapper. Entries are submitted at
/// link time via `inventory::submit!` and collected at startup
/// into the [`WrapperRegistry`] view.
///
/// The fields here are pure declaration: which fields the
/// wrapper consumes, when it applies, what relationships it
/// has to other wrappers, and how it describes its assignment.
/// Construction of the dispenser layer itself is NOT in the
/// registration — the cascade in `activity.rs` continues to
/// hold the per-wrapper `wrap()` calls (each has a different
/// signature). The registry decides PRESENCE and ORDER; the
/// cascade looks up the resolved plan and dispatches by name.
pub struct WrapperRegistration {
    /// Stable name (`"validate"`, `"poll"`, `"throttle"`,
    /// `"if"`, `"emit"`, `"result"`, `"metrics"`, `"traverse"`).
    pub name: WrapperName,

    /// Op-template field names this wrapper exclusively owns.
    /// Listed for parse-time validation: a misplaced field like
    /// `poll_interval_ms: 5000` on an op without `poll:` becomes
    /// a hard error pointing at THIS registration, not an opaque
    /// "unknown param".
    ///
    /// Pure data — the parse-time guard reads this; the wrapper
    /// implementation reads its own fields directly off the
    /// `ParsedOp`.
    pub owned_fields: &'static [&'static str],

    /// Predicate over the op template: "does this wrapper apply
    /// to this op?" Default behaviour: any owned field present.
    /// Wrappers with no owned fields (e.g. `result`, which fires
    /// whenever the op declares any `result:` wires) override
    /// this with their own logic.
    pub triggers: fn(&ParsedOp) -> bool,

    /// Wrappers that MUST sit inside this one (closer to the
    /// adapter, called *after* this one per cycle).
    /// Activates transitively: triggering `validate` pulls in
    /// `traverse` whether or not a traverse field was declared.
    pub requires_inner: &'static [WrapperName],

    /// Wrappers that MUST NOT sit outside this one. Hard error
    /// when the constraint graph would permit any of the listed
    /// wrappers to wrap this one.
    pub forbids_outer: &'static [WrapperName],

    /// Wrappers that cannot coexist with this one on a given
    /// op. Triggering both is a hard error.
    pub mutually_exclusive_with: &'static [WrapperName],

    /// One-line summary of what this wrapper, configured for
    /// the given op template, will do at runtime.
    /// Emitted at Info level once per op-template activation,
    /// alongside the other wrappers in the resolved plan.
    ///
    /// Examples:
    /// - `validate`: `"validate: min_rows ≥ 1 (strict)"`
    /// - `poll`:     `"poll: every 5s, timeout 600s, on \`await_empty\`"`
    /// - `if`:       `"if: cql_dialect == 'cass5'"`
    ///
    /// Returns `None` for wrappers that have nothing useful to
    /// say (e.g. an always-on `traverse` with no per-op
    /// configuration); operators see the wrappers that actually
    /// shape behaviour, the boilerplate stays at Debug.
    ///
    /// Distinct from `OpDispenser::describe()` which describes
    /// the *runtime op* — e.g. the CQL statement text — for
    /// error-context dumps. This describes the *wrapper's
    /// contribution* for init-time diagnostics.
    pub describe_assignment: fn(&ParsedOp) -> Option<String>,
}

inventory::collect!(WrapperRegistration);

/// Live view over every registered wrapper. Built once at
/// startup from the `inventory` collection.
///
/// The struct is cheap to clone (it borrows the static
/// registrations) and is passed to the [`crate::wrapper_resolver::WrapperResolver`]
/// for per-op-template plan computation.
pub struct WrapperRegistry {
    entries: Vec<&'static WrapperRegistration>,
}

impl WrapperRegistry {
    /// Build the registry from every `inventory::submit!`
    /// entry currently linked into the binary. Invoked once
    /// at startup.
    pub fn from_inventory() -> Self {
        let mut entries: Vec<&'static WrapperRegistration> =
            inventory::iter::<WrapperRegistration>().collect();
        entries.sort_by_key(|r| r.name);
        Self { entries }
    }

    /// Iterate every registered wrapper.
    pub fn iter(&self) -> impl Iterator<Item = &'static WrapperRegistration> + '_ {
        self.entries.iter().copied()
    }

    /// Look up by name. Returns `None` for unknown names; the
    /// caller surfaces that as a typo diagnostic with a
    /// closest-match suggestion (see [`closest_match`]).
    pub fn get(&self, name: WrapperName) -> Option<&'static WrapperRegistration> {
        self.entries.iter().copied().find(|r| r.name == name)
    }

    /// Look up by raw string. Convenience for parsing the
    /// `--wrap-default-order` / `wrappers.order` lists.
    pub fn get_str(&self, name: &str) -> Option<&'static WrapperRegistration> {
        self.entries.iter().copied().find(|r| r.name.as_str() == name)
    }

    /// Number of registered wrappers.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Find the registered wrapper name closest to `query` by
    /// Levenshtein distance. Used for "did you mean … ?"
    /// diagnostics on unknown names.
    pub fn closest_match(&self, query: &str) -> Option<&'static str> {
        closest_match(query, self.entries.iter().map(|r| r.name.as_str()))
    }

    /// SRD-32a Push 2 — find every owned field that's present
    /// on the op template but whose owning wrapper does NOT
    /// trigger. Returns one (wrapper, field) pair per
    /// violation; an empty vec means every field is in its
    /// proper place.
    ///
    /// `field_present_on_template` is the predicate the
    /// caller uses to ask "is field X set on this op?" — it
    /// abstracts the fact that some wrapper-owned fields
    /// (e.g. `if`, `delay`) live outside `params:`. In
    /// practice the owned fields that AREN'T also their
    /// wrapper's trigger always live under `params:`, so a
    /// caller can pass a closure over `template.params
    /// .contains_key`.
    pub fn misplaced_fields<F>(
        &self,
        template: &nbrs_workload::model::ParsedOp,
        field_present_on_template: F,
    ) -> Vec<(WrapperName, &'static str)>
    where
        F: Fn(&str) -> bool,
    {
        let mut out: Vec<(WrapperName, &'static str)> = Vec::new();
        for reg in self.iter() {
            if (reg.triggers)(template) {
                continue;
            }
            for &field in reg.owned_fields {
                if field_present_on_template(field) {
                    out.push((reg.name, field));
                }
            }
        }
        out
    }
}

/// Find the closest match in a list of candidate names, using
/// Levenshtein edit distance. Returns `None` when no candidate
/// is within distance 3, since beyond that the suggestion is
/// noise.
pub fn closest_match<'a>(
    query: &str,
    candidates: impl IntoIterator<Item = &'a str>,
) -> Option<&'a str> {
    let mut best: Option<(&str, usize)> = None;
    for c in candidates {
        let d = levenshtein(query, c);
        match best {
            Some((_, prev)) if d >= prev => {}
            _ => best = Some((c, d)),
        }
    }
    best.filter(|&(_, d)| d <= 3).map(|(s, _)| s)
}

fn levenshtein(a: &str, b: &str) -> usize {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    let (n, m) = (a.len(), b.len());
    if n == 0 { return m; }
    if m == 0 { return n; }
    let mut prev: Vec<usize> = (0..=m).collect();
    let mut curr: Vec<usize> = vec![0; m + 1];
    for i in 1..=n {
        curr[0] = i;
        for j in 1..=m {
            let cost = if a[i - 1] == b[j - 1] { 0 } else { 1 };
            curr[j] = (prev[j] + 1)
                .min(curr[j - 1] + 1)
                .min(prev[j - 1] + cost);
        }
        std::mem::swap(&mut prev, &mut curr);
    }
    prev[m]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn levenshtein_basic() {
        assert_eq!(levenshtein("", "abc"), 3);
        assert_eq!(levenshtein("abc", ""), 3);
        assert_eq!(levenshtein("kitten", "sitting"), 3);
        assert_eq!(levenshtein("validate", "validatte"), 1);
    }

    #[test]
    fn closest_match_finds_typo() {
        let names = ["validate", "poll", "throttle"];
        assert_eq!(closest_match("validatte", names), Some("validate"));
        assert_eq!(closest_match("plll", names), Some("poll"));
        assert_eq!(closest_match("wildly_different", names), None);
    }
}
