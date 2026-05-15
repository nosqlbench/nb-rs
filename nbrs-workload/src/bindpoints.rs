// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Bind point detection and field classification.
//!
//! Scans op template fields to detect `{name}` bind point references
//! and `{{expr}}` inline binding definitions. Classifies fields as
//! static (no bind points) or dynamic (has bind points).

/// Namespace qualifier for a bind point reference.
#[derive(Debug, Clone, PartialEq)]
pub enum BindQualifier {
    /// No qualifier — resolved by priority: bind → capture → input.
    None,
    /// `{input:name}` — graph input value.
    Input,
    /// `{bind:name}` — GK binding output.
    Bind,
    /// `{capture:name}` — capture context (volatile or sticky port).
    /// Also accepts `{port:name}` as an alias.
    Capture,
}

/// A detected bind point in an op template field.
#[derive(Debug, Clone, PartialEq)]
pub enum BindPoint {
    /// `{name}` or `{qualifier:name}` — references a named value.
    Reference { name: String, qualifier: BindQualifier },
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
            BindPoint::Reference { name, .. } => FieldType::BindRef(name.clone()),
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
                // Single brace: {name}, {:=expr}, {:=expr:=}, or {expr}
                // First, peek ahead to check if this is a CQL map literal
                // (starts with ' or ", possibly after whitespace — covers
                // multi-line CQL maps where the opening `{` sits on its
                // own line). If so, skip just the opening brace and
                // continue scanning — inner {name} refs are still valid.
                let next_nonspace = chars[i + 1..]
                    .iter()
                    .find(|c| !c.is_whitespace())
                    .copied();
                if matches!(next_nonspace, Some(c) if is_literal_start(c)) {
                    // CQL map literal: {'key': '{value}'} — skip the opening {
                    // but continue scanning so inner bind points are found.
                    i += 1;
                    continue;
                }

                i += 1;
                let start = i;
                let mut depth = 1u32;
                while i < chars.len() {
                    if chars[i] == '{' { depth += 1; }
                    if chars[i] == '}' { depth -= 1; if depth == 0 { break; } }
                    i += 1;
                }
                if i < chars.len() {
                    let raw: String = chars[start..i].iter().collect();
                    let raw = raw.trim();

                    if is_literal_content(raw) {
                        // Fallback: content has quotes — literal text, not a bind point.
                        i += 1;
                    } else if let Some(expr) = raw.strip_prefix(":=") {
                        // Explicit {:=expr} or {:=expr:=} syntax
                        let expr = expr.strip_suffix(":=").unwrap_or(expr).trim();
                        points.push(BindPoint::InlineDefinition(expr.to_string()));
                        i += 1;
                    } else if is_expression(raw) {
                        // Content has operators/parens — treat as inline expression
                        points.push(BindPoint::InlineDefinition(raw.to_string()));
                        i += 1;
                    } else {
                        // Simple identifier — reference bind point
                        let (qualifier, name) = parse_qualified_ref(raw);
                        points.push(BindPoint::Reference { name, qualifier });
                        i += 1;
                    }
                }
            }
        } else {
            i += 1;
        }
    }

    points
}

/// Detect whether bind point content is a GK expression (not a simple name).
///
/// Returns true if the content contains operators, function calls,
/// or other syntax that can't be a plain identifier.
pub fn is_expression_public(s: &str) -> bool {
    is_expression(s)
}

fn is_expression(s: &str) -> bool {
    // Simple identifiers: [a-zA-Z_][a-zA-Z0-9_-]*
    // Hyphens are valid in identifiers (e.g. my-variable), so only treat
    // `-` as an expression indicator when it is a unary minus (first char
    // followed by a digit) which marks a negative numeric literal.
    s.contains('(') || s.contains(')') ||
    s.contains('+') || s.contains('*') || s.contains('/') ||
    s.contains('%') || s.contains('^') || s.contains('&') ||
    s.contains('|') || s.contains('!') || s.contains('<') ||
    s.contains('>') ||
    // Numeric literal (starts with digit)
    s.starts_with(|c: char| c.is_ascii_digit()) ||
    // Negative literal: starts with - followed by digit
    (s.starts_with('-') && s.len() > 1 && s.as_bytes()[1].is_ascii_digit())
}

/// Content between `{` and `}` that is clearly literal text — not a
/// binding name or GK expression. If the content starts with a quote
/// character, it's a literal value (e.g. CQL map `{'class': ...}`),
/// never a bind point or expression.
fn is_literal_content(s: &str) -> bool {
    s.starts_with('\'') || s.starts_with('"')
}

/// Check if a character indicates the start of a CQL map/JSON literal
/// after an opening brace. `{'key': ...}` and `{"key": ...}` are
/// literal map content, not bind points.
fn is_literal_start(c: char) -> bool {
    c == '\'' || c == '"'
}

/// Parse a qualified reference like "coord:cycle" or just "cycle".
fn parse_qualified_ref(raw: &str) -> (BindQualifier, String) {
    if let Some((prefix, name)) = raw.split_once(':') {
        let qualifier = match prefix.trim().to_lowercase().as_str() {
            "input" | "coord" | "coordinate" => BindQualifier::Input,
            "bind" => BindQualifier::Bind,
            "capture" => BindQualifier::Capture,
            _ => return (BindQualifier::None, raw.to_string()), // not a known qualifier
        };
        (qualifier, name.trim().to_string())
    } else {
        (BindQualifier::None, raw.to_string())
    }
}

/// Extract all referenced binding names from a string (only `{name}`, not `{{expr}}`).
/// Returns the bare name without qualifier.
pub fn referenced_bindings(value: &str) -> Vec<String> {
    extract_bind_points(value)
        .into_iter()
        .filter_map(|bp| match bp {
            BindPoint::Reference { name, .. } => Some(name),
            _ => None,
        })
        .collect()
}

/// Replace `{name}` bind point references with `?` markers for
/// CQL prepared statements. Returns the parameterized statement.
///
/// Also strips quotes that immediately surround a bind point:
/// `'{id}'` → `?` (not `'?'`), because CQL prepared bind markers
/// must not be inside string literals.
pub fn replace_bind_points_with_markers(value: &str) -> String {
    let names = referenced_bindings(value);
    let mut result = value.to_string();
    for name in &names {
        // Try quoted form first: '{name}' → ?
        let quoted = format!("'{{{name}}}'");
        if let Some(pos) = result.find(&quoted) {
            result.replace_range(pos..pos + quoted.len(), "?");
            continue;
        }
        // Bare form: {name} → ?
        let bare = format!("{{{name}}}");
        if let Some(pos) = result.find(&bare) {
            result.replace_range(pos..pos + bare.len(), "?");
        }
    }
    result
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
/// - `[username]` — capture "username" as "username" (single value)
/// - `[username as u1]` — capture "username", store as "u1" (single value)
/// - `[(List) field]` — capture with type assertion
/// - `[*]` — capture all available fields
/// - `[@keys]` — **slurp**: collect every row's `keys` column into
///   a `Value::Json` array. Use when the result has multiple rows
///   and the consumer needs all per-row column values as a list
///   (e.g. recall-evaluator's `actual:` reads).
/// - `[@col as values]` — slurp with alias.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct CapturePoint {
    /// The field name to capture from the result.
    pub source_name: String,
    /// The variable name to store the captured value under.
    /// Same as source_name if no `as` clause.
    pub as_name: String,
    /// Optional type assertion (e.g., "List", "int[]").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cast_type: Option<String>,
    /// `true` when the capture-point was declared with the `@`
    /// slurp prefix (`[@name]`). Slurp captures collect every
    /// row's column value across the result body into a single
    /// `Value::Json` array; non-slurp captures take the first
    /// row's value as a scalar.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub slurp: bool,
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
            let attempt_start = i + 1;
            i = attempt_start;

            // Speculatively parse a capture-point spec. If anything
            // about the grammar fails to match — including the
            // source name being absent or not starting with an
            // identifier character — we reset to `bracket_start + 1`
            // and emit the `[` as a literal. This keeps JSON / CQL
            // array literals (`[]`, `["foo", 42]`), JNI type
            // signatures (`[Ljava.lang.String;`), and other uses of
            // `[...]` from being silently consumed.

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

            // Optional slurp modifier: `[@name]` collects every
            // row's column value into a `Value::Json` array. The
            // `@` is a syntax-only marker — stripped from the
            // emitted raw_template so the adapter sees clean
            // column-reference text.
            let slurp = if i < chars.len() && chars[i] == '@' {
                i += 1;
                while i < chars.len() && chars[i].is_whitespace() { i += 1; }
                true
            } else {
                false
            };

            // Source name — must look like a real identifier (or
            // `*` for wildcard). Starts with `_` or an ASCII alpha
            // character; continues with alphanumerics, `_`, `-`,
            // `.`, or `*`. The strict-leading-char rule prevents
            // JSON array literals like `["..."]`, `[42]`, and JNI
            // signatures like `[Ljava.lang.String;` from being
            // misread as captures.
            let name_start = i;
            if i < chars.len() {
                let first = chars[i];
                let is_valid_first = first.is_ascii_alphabetic()
                    || first == '_'
                    || first == '*';
                if !is_valid_first {
                    // Not a capture-point opening — emit `[` and
                    // resume one char in.
                    raw.push('[');
                    i = attempt_start;
                    continue;
                }
            }
            while i < chars.len() && (chars[i].is_alphanumeric() || chars[i] == '_' || chars[i] == '-' || chars[i] == '.' || chars[i] == '*') {
                i += 1;
            }
            let source_name: String = chars[name_start..i].iter().collect();
            if source_name.is_empty() {
                // Defense-in-depth — the leading-char check above
                // already excludes this path, but keep it explicit.
                raw.push('[');
                i = attempt_start;
                continue;
            }

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
                    slurp,
                });
                // Emit source name without brackets into raw template
                raw.push_str(&source_name);
            } else {
                // No matching `]` for what otherwise looked like a
                // capture spec — likely a CQL/JSON array containing
                // an identifier (e.g. `[foo, bar]`). Treat the
                // opening `[` as a literal and resume one char in.
                let _ = bracket_start;
                raw.push('[');
                i = attempt_start;
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
    fn is_expression_detects_function_calls() {
        assert!(is_expression("hash(cycle)"));
        assert!(is_expression("mod(x, 100)"));
    }

    #[test]
    fn is_expression_detects_operators() {
        assert!(is_expression("x + 1"));
        assert!(is_expression("a * b"));
        assert!(is_expression("x & 0xFF"));
    }

    #[test]
    fn is_expression_rejects_simple_names() {
        assert!(!is_expression("cycle"));
        assert!(!is_expression("my_var"));
        assert!(!is_expression("user_id"));
    }

    #[test]
    fn is_expression_rejects_hyphenated_names() {
        assert!(!is_expression("my-variable"));
        assert!(!is_expression("some-long-name"));
    }

    #[test]
    fn is_expression_detects_numeric_literals() {
        assert!(is_expression("42"));
        assert!(is_expression("3.14"));
        assert!(is_expression("-5"));
    }

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
                assert_eq!(points[0], BindPoint::Reference { name: "id".into(), qualifier: BindQualifier::None });
                assert_eq!(points[1], BindPoint::Reference { name: "name".into(), qualifier: BindQualifier::None });
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

    // --- Qualified bind point tests ---

    #[test]
    fn qualified_coord() {
        let points = extract_bind_points("{coord:cycle}");
        assert_eq!(points.len(), 1);
        assert_eq!(points[0], BindPoint::Reference {
            name: "cycle".into(),
            qualifier: BindQualifier::Input,
        });
    }

    #[test]
    fn qualified_capture() {
        let points = extract_bind_points("{capture:balance}");
        assert_eq!(points.len(), 1);
        assert_eq!(points[0], BindPoint::Reference {
            name: "balance".into(),
            qualifier: BindQualifier::Capture,
        });
    }

    #[test]
    fn qualified_bind() {
        let points = extract_bind_points("{bind:user_id}");
        assert_eq!(points.len(), 1);
        assert_eq!(points[0], BindPoint::Reference {
            name: "user_id".into(),
            qualifier: BindQualifier::Bind,
        });
    }

    #[test]
    fn unknown_qualifier_becomes_unqualified() {
        // "port" is not a recognized qualifier — treated as unqualified
        let points = extract_bind_points("{port:auth_token}");
        assert_eq!(points.len(), 1);
        assert_eq!(points[0], BindPoint::Reference {
            name: "port:auth_token".into(),
            qualifier: BindQualifier::None,
        });
    }

    #[test]
    fn unqualified_still_works() {
        let points = extract_bind_points("{user_id}");
        assert_eq!(points[0], BindPoint::Reference {
            name: "user_id".into(),
            qualifier: BindQualifier::None,
        });
    }

    #[test]
    fn qualified_referenced_bindings_returns_bare_name() {
        let refs = referenced_bindings("VALUES ({coord:cycle}, {capture:balance}, {user_id})");
        assert_eq!(refs, vec!["cycle", "balance", "user_id"]);
    }

    #[test]
    fn coordinate_long_form() {
        let points = extract_bind_points("{coordinate:row}");
        assert_eq!(points[0], BindPoint::Reference {
            name: "row".into(),
            qualifier: BindQualifier::Input,
        });
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
    fn capture_empty_brackets_pass_through() {
        // `[]` (empty JSON array literal) is NOT a capture point.
        // The parser must emit it verbatim — eating the brackets
        // would break Jolokia payloads like
        // `"arguments":["foo",[]]` which became `"arguments":["foo",]`
        // (invalid JSON, JMX op gets 1 arg instead of 2).
        let result = parse_capture_points(r#"{"arguments":["foo",[]]}"#);
        assert!(result.captures.is_empty(),
            "no capture should be extracted: {:?}", result.captures);
        assert_eq!(result.raw_template, r#"{"arguments":["foo",[]]}"#);
    }

    #[test]
    fn capture_jni_array_signature_pass_through() {
        // JNI array signature `[Ljava.lang.String;` opens with `[`
        // but is not a capture point (no closing `]` follows the
        // identifier chunk). The parser must not consume anything.
        let template =
            r#""operation":"forceKeyspaceFlush(java.lang.String,[Ljava.lang.String;)""#;
        let result = parse_capture_points(template);
        assert!(result.captures.is_empty(),
            "JNI signature should not parse as capture: {:?}", result.captures);
        assert_eq!(result.raw_template, template);
    }

    #[test]
    fn capture_json_array_with_string_pass_through() {
        // JSON string-array literal — opens `[`, content begins
        // with `"`, no identifier; parser must leave it alone.
        let result = parse_capture_points(r#"["alpha","beta"]"#);
        assert!(result.captures.is_empty());
        assert_eq!(result.raw_template, r#"["alpha","beta"]"#);
    }

    #[test]
    fn capture_json_array_with_number_pass_through() {
        // Numeric-literal-only array — leading digit is not a
        // valid identifier start, so no capture is extracted.
        let result = parse_capture_points("[42]");
        assert!(result.captures.is_empty());
        assert_eq!(result.raw_template, "[42]");
    }

    #[test]
    fn capture_cql_collection_literal_pass_through() {
        // CQL collection literal like `[1, 2, 3]` — content has
        // digits + commas, not a valid capture spec. Must
        // pass through verbatim.
        let template = "INSERT INTO t (vals) VALUES ([1, 2, 3])";
        let result = parse_capture_points(template);
        assert!(result.captures.is_empty());
        assert_eq!(result.raw_template, template);
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
