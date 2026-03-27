// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! Bind point detection and field classification.
//!
//! Scans op template fields to detect `{name}` bind point references
//! and `{{expr}}` inline binding definitions. Classifies fields as
//! static (no bind points) or dynamic (has bind points).

/// A detected bind point in an op template field.
#[derive(Debug, Clone, PartialEq)]
pub enum BindPoint {
    /// `{name}` — references a named binding.
    Reference(String),
    /// `{{expr}}` — inline binding definition.
    InlineDefinition(String),
}

/// The classification of an op template field value.
#[derive(Debug, Clone, PartialEq)]
pub enum FieldType {
    /// No bind points — value is constant across cycles.
    Static,
    /// Pure binding reference — the entire value is `{name}`.
    BindRef(String),
    /// String template with interleaved literals and bind points.
    Template(Vec<BindPoint>),
}

/// Scan a string value for bind points and classify it.
pub fn classify_field(value: &str) -> FieldType {
    let bind_points = extract_bind_points(value);
    if bind_points.is_empty() {
        FieldType::Static
    } else if bind_points.len() == 1
        && value.starts_with('{')
        && !value.starts_with("{{")
        && value.ends_with('}')
    {
        match &bind_points[0] {
            BindPoint::Reference(name) => FieldType::BindRef(name.clone()),
            _ => FieldType::Template(bind_points),
        }
    } else {
        FieldType::Template(bind_points)
    }
}

/// Extract all bind points from a string.
pub fn extract_bind_points(value: &str) -> Vec<BindPoint> {
    let mut points = Vec::new();
    let chars: Vec<char> = value.chars().collect();
    let mut i = 0;

    while i < chars.len() {
        if chars[i] == '{' {
            if i + 1 < chars.len() && chars[i + 1] == '{' {
                // Inline definition: {{expr}}
                i += 2;
                let start = i;
                while i + 1 < chars.len() && !(chars[i] == '}' && chars[i + 1] == '}') {
                    i += 1;
                }
                if i + 1 < chars.len() {
                    let expr: String = chars[start..i].iter().collect();
                    points.push(BindPoint::InlineDefinition(expr.trim().to_string()));
                    i += 2; // skip }}
                }
            } else {
                // Reference: {name}
                i += 1;
                let start = i;
                while i < chars.len() && chars[i] != '}' {
                    i += 1;
                }
                if i < chars.len() {
                    let name: String = chars[start..i].iter().collect();
                    points.push(BindPoint::Reference(name.trim().to_string()));
                    i += 1; // skip }
                }
            }
        } else {
            i += 1;
        }
    }

    points
}

/// Extract all referenced binding names from a string (only `{name}`, not `{{expr}}`).
pub fn referenced_bindings(value: &str) -> Vec<String> {
    extract_bind_points(value)
        .into_iter()
        .filter_map(|bp| match bp {
            BindPoint::Reference(name) => Some(name),
            _ => None,
        })
        .collect()
}

// =================================================================
// Capture points: [name] and [name as alias] in op template strings
// =================================================================

/// A capture point extracted from an op template.
///
/// Capture points mark result fields that should be extracted from
/// the operation result and stored as named variables for use in
/// subsequent operations or verification.
///
/// Formats:
/// - `[username]` — capture "username" as "username"
/// - `[username as u1]` — capture "username", store as "u1"
/// - `[(List) field]` — capture with type assertion
/// - `[*]` — capture all available fields
#[derive(Debug, Clone, PartialEq)]
pub struct CapturePoint {
    /// The field name to capture from the result.
    pub source_name: String,
    /// The variable name to store the captured value under.
    /// Same as source_name if no `as` clause.
    pub as_name: String,
    /// Optional type assertion (e.g., "List", "int[]").
    pub cast_type: Option<String>,
}

/// Result of parsing capture points from a string.
#[derive(Debug, Clone)]
pub struct CaptureParseResult {
    /// The raw template with capture brackets removed.
    /// `select [username] from t` → `select username from t`
    pub raw_template: String,
    /// The capture points found.
    pub captures: Vec<CapturePoint>,
}

/// Parse capture points from an op template string.
///
/// Detects `[name]`, `[name as alias]`, `[(Type) name]`, and `[*]`
/// patterns. Returns the cleaned template (brackets removed) and
/// the list of capture points.
pub fn parse_capture_points(template: &str) -> CaptureParseResult {
    let mut captures = Vec::new();
    let mut raw = String::with_capacity(template.len());
    let chars: Vec<char> = template.chars().collect();
    let mut i = 0;

    while i < chars.len() {
        if chars[i] == '[' {
            let bracket_start = i;
            i += 1;

            // Skip whitespace
            while i < chars.len() && chars[i].is_whitespace() { i += 1; }

            // Optional type cast: (Type)
            let cast_type = if i < chars.len() && chars[i] == '(' {
                i += 1;
                let cast_start = i;
                while i < chars.len() && chars[i] != ')' { i += 1; }
                let cast: String = chars[cast_start..i].iter().collect();
                if i < chars.len() { i += 1; } // skip ')'
                while i < chars.len() && chars[i].is_whitespace() { i += 1; }
                Some(cast.trim().to_string())
            } else {
                None
            };

            // Source name (word chars, digits, hyphens, underscores, dots, or *)
            let name_start = i;
            while i < chars.len() && (chars[i].is_alphanumeric() || chars[i] == '_' || chars[i] == '-' || chars[i] == '.' || chars[i] == '*') {
                i += 1;
            }
            let source_name: String = chars[name_start..i].iter().collect();

            // Optional "as alias"
            while i < chars.len() && chars[i].is_whitespace() { i += 1; }
            let as_name = if i + 2 < chars.len()
                && (chars[i] == 'a' || chars[i] == 'A')
                && (chars[i+1] == 's' || chars[i+1] == 'S')
                && chars[i+2].is_whitespace()
            {
                i += 2; // skip "as"
                while i < chars.len() && chars[i].is_whitespace() { i += 1; }
                let alias_start = i;
                while i < chars.len() && (chars[i].is_alphanumeric() || chars[i] == '_' || chars[i] == '-' || chars[i] == '.') {
                    i += 1;
                }
                let alias: String = chars[alias_start..i].iter().collect();
                alias
            } else {
                source_name.clone()
            };

            // Skip whitespace and closing bracket
            while i < chars.len() && chars[i].is_whitespace() { i += 1; }
            if i < chars.len() && chars[i] == ']' {
                i += 1;
                captures.push(CapturePoint {
                    source_name: source_name.clone(),
                    as_name,
                    cast_type,
                });
                // Emit source name without brackets into raw template
                raw.push_str(&source_name);
            } else {
                // Malformed — pass through as-is
                raw.push_str(&template[bracket_start..i]);
            }
        } else {
            raw.push(chars[i]);
            i += 1;
        }
    }

    CaptureParseResult { raw_template: raw, captures }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn static_field() {
        assert_eq!(classify_field("plain text"), FieldType::Static);
        assert_eq!(classify_field("42"), FieldType::Static);
        assert_eq!(classify_field(""), FieldType::Static);
    }

    #[test]
    fn pure_bind_ref() {
        assert_eq!(classify_field("{userid}"), FieldType::BindRef("userid".into()));
    }

    #[test]
    fn template_with_bind_points() {
        let ft = classify_field("SELECT * FROM t WHERE id={id} AND name={name}");
        match ft {
            FieldType::Template(points) => {
                assert_eq!(points.len(), 2);
                assert_eq!(points[0], BindPoint::Reference("id".into()));
                assert_eq!(points[1], BindPoint::Reference("name".into()));
            }
            _ => panic!("expected Template"),
        }
    }

    #[test]
    fn inline_definition() {
        let ft = classify_field("value is {{Template('user-{}', ToString())}}");
        match ft {
            FieldType::Template(points) => {
                assert_eq!(points.len(), 1);
                match &points[0] {
                    BindPoint::InlineDefinition(expr) => {
                        assert!(expr.contains("Template"));
                    }
                    _ => panic!("expected InlineDefinition"),
                }
            }
            _ => panic!("expected Template"),
        }
    }

    #[test]
    fn mixed_references_and_literals() {
        let refs = referenced_bindings("INSERT INTO t (a, b) VALUES ({col_a}, {col_b})");
        assert_eq!(refs, vec!["col_a", "col_b"]);
    }

    #[test]
    fn no_bind_points() {
        let refs = referenced_bindings("just a plain string");
        assert!(refs.is_empty());
    }

    // --- Capture point tests ---

    #[test]
    fn capture_simple() {
        let result = parse_capture_points("select [username] from users where id={id}");
        assert_eq!(result.captures.len(), 1);
        assert_eq!(result.captures[0].source_name, "username");
        assert_eq!(result.captures[0].as_name, "username");
        assert_eq!(result.raw_template, "select username from users where id={id}");
    }

    #[test]
    fn capture_with_alias() {
        let result = parse_capture_points("select [username as u1] from users");
        assert_eq!(result.captures.len(), 1);
        assert_eq!(result.captures[0].source_name, "username");
        assert_eq!(result.captures[0].as_name, "u1");
        assert_eq!(result.raw_template, "select username from users");
    }

    #[test]
    fn capture_with_type_cast() {
        let result = parse_capture_points("select [(List) items] from orders");
        assert_eq!(result.captures.len(), 1);
        assert_eq!(result.captures[0].source_name, "items");
        assert_eq!(result.captures[0].cast_type, Some("List".into()));
    }

    #[test]
    fn capture_wildcard() {
        let result = parse_capture_points("select [*] from users");
        assert_eq!(result.captures.len(), 1);
        assert_eq!(result.captures[0].source_name, "*");
    }

    #[test]
    fn capture_multiple() {
        let result = parse_capture_points("select [a], [b as x] from t where id={id}");
        assert_eq!(result.captures.len(), 2);
        assert_eq!(result.captures[0].source_name, "a");
        assert_eq!(result.captures[0].as_name, "a");
        assert_eq!(result.captures[1].source_name, "b");
        assert_eq!(result.captures[1].as_name, "x");
    }

    #[test]
    fn capture_no_captures() {
        let result = parse_capture_points("select * from users where id={id}");
        assert!(result.captures.is_empty());
        assert_eq!(result.raw_template, "select * from users where id={id}");
    }

    #[test]
    fn capture_mixed_with_bind_points() {
        let result = parse_capture_points("select [name], [age as user_age] from users where id={userid}");
        assert_eq!(result.captures.len(), 2);
        // Bind point {userid} should remain in the raw template
        assert!(result.raw_template.contains("{userid}"));
        // Capture brackets should be removed
        assert!(!result.raw_template.contains('['));
        assert!(!result.raw_template.contains(']'));
    }
}
