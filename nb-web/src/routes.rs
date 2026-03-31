// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! Route handlers for the web UI.

use axum::extract::Query;
use axum::response::Html;
use axum::Form;

use nb_variates::dsl::registry;
use nb_variates::viz;

const BASE_CSS: &str = include_str!("../static/style.css");
const HTMX_CDN: &str = "https://unpkg.com/htmx.org@2.0.4";

fn shell(title: &str, active: &str, content: &str) -> String {
    let nav = |label: &str, href: &str| -> String {
        let cls = if label == active { " class=\"active\"" } else { "" };
        format!("<a href=\"{href}\" hx-get=\"{href}\" hx-target=\"main\" hx-push-url=\"true\"{cls}>{label}</a>")
    };
    let nav_html = [
        nav("Dashboard", "/"),
        nav("Functions", "/functions"),
        nav("Stdlib", "/stdlib"),
        nav("DAG Viewer", "/dag"),
    ].join("\n            ");

    format!(
        "<!DOCTYPE html>\n<html lang=\"en\">\n<head>\n\
         <meta charset=\"UTF-8\">\n<meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">\n\
         <title>nbrs &mdash; {title}</title>\n\
         <script src=\"{HTMX_CDN}\"></script>\n\
         <style>{BASE_CSS}</style>\n</head>\n<body>\n\
         <header><h1>nbrs</h1><nav>{nav_html}</nav></header>\n\
         <main>{content}</main>\n</body>\n</html>"
    )
}

fn esc(s: &str) -> String {
    s.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;").replace('"', "&quot;")
}

// ─── Dashboard ──────────────────────────────────────────────

pub async fn dashboard() -> Html<String> {
    Html(shell("Dashboard", "Dashboard", include_str!("../templates/page_dashboard.html")))
}

// ─── Functions ──────────────────────────────────────────────

pub async fn functions_page() -> Html<String> {
    let table = render_function_table(None);
    let content = format!(
        "{}\n<div id=\"function-table\">{table}</div>\n</div>",
        include_str!("../templates/page_functions_header.html")
    );
    Html(shell("Functions", "Functions", &content))
}

#[derive(serde::Deserialize)]
pub struct FunctionQuery { pub q: Option<String> }

pub async fn functions_api(Query(query): Query<FunctionQuery>) -> Html<String> {
    Html(render_function_table(query.q.as_deref()))
}

// ─── Stdlib ─────────────────────────────────────────────────

pub async fn stdlib_page() -> Html<String> {
    let modules = render_stdlib_list();
    let content = format!(
        "<div class=\"card\">\n<h2>GK Standard Library</h2>\n\
         <p style=\"color: var(--text-dim); margin-bottom: 16px;\">\
         Embedded modules &mdash; call by name, no imports needed.</p>\n{modules}\n</div>"
    );
    Html(shell("Stdlib", "Stdlib", &content))
}

pub async fn stdlib_source(axum::extract::Path(name): axum::extract::Path<String>) -> Html<String> {
    let sources = nb_variates::dsl::stdlib_sources();
    for (_filename, source) in sources {
        if source.contains(&format!("{name}(")) {
            return Html(format!("<pre style=\"margin-top: 8px;\">{}</pre>", esc(source)));
        }
    }
    Html("<pre>Module not found</pre>".into())
}

// ─── DAG Viewer ─────────────────────────────────────────────

pub async fn dag_page() -> Html<String> {
    Html(shell("DAG Viewer", "DAG Viewer", include_str!("../templates/page_dag.html")))
}

#[derive(serde::Deserialize)]
pub struct DagRenderForm { pub source: String, pub format: Option<String> }

pub async fn dag_render(Form(form): Form<DagRenderForm>) -> Html<String> {
    let source = form.source.trim();
    if source.is_empty() {
        return Html("<p style=\"color: var(--text-dim);\">Enter GK source to render</p>".into());
    }

    let fmt = form.format.as_deref().unwrap_or("svg");
    let result = match fmt {
        "svg" => viz::gk_to_svg(source),
        "mermaid" => viz::gk_to_mermaid(source).map(|m| format!("<pre>{}</pre>", esc(&m))),
        "dot" => viz::gk_to_dot(source).map(|d| format!("<pre>{}</pre>", esc(&d))),
        _ => Err("unknown format".into()),
    };
    match result {
        Ok(content) => Html(content),
        Err(e) => Html(format!("<pre style=\"color: var(--accent);\">Error: {}</pre>", esc(&e))),
    }
}

// ─── Activities API ─────────────────────────────────────────

pub async fn activities_api() -> Html<String> {
    Html("<p style=\"color: var(--text-dim)\">No activities running</p>".into())
}

// ─── Rendering ──────────────────────────────────────────────

fn render_function_table(filter: Option<&str>) -> String {
    let grouped = registry::by_category();
    let filter_lower = filter.map(|f| f.to_lowercase());
    let mut html = String::new();

    for (cat, funcs) in grouped {
        let views: Vec<_> = funcs.iter()
            .filter(|sig| match &filter_lower {
                Some(q) if !q.is_empty() =>
                    sig.name.contains(q.as_str()) || sig.description.to_lowercase().contains(q.as_str()),
                _ => true,
            })
            .collect();
        if views.is_empty() { continue; }

        html.push_str(&format!(
            "<h3 style=\"color: var(--blue); margin: 16px 0 8px; font-size: 13px;\">{}</h3>\n\
             <table><thead><tr><th>Name</th><th>Params</th><th>Arity</th><th>Level</th><th>Description</th></tr></thead><tbody>\n",
            cat.display_name()
        ));
        for sig in views {
            let params = if sig.const_params.is_empty() { String::new() } else {
                let p: Vec<String> = sig.const_params.iter()
                    .map(|(name, req)| if *req { name.to_string() } else { format!("[{name}]") }).collect();
                format!("({})", p.join(", "))
            };
            let arity = if sig.outputs == 0 { format!("{}&#8594;N", sig.wire_inputs) }
                else { format!("{}&#8594;{}", sig.wire_inputs, sig.outputs) };
            let level = nb_activity::bindings::probe_compile_level(sig.name);
            let (ls, lc) = match level {
                registry::CompileLevel::Phase3 => ("P3", "green"),
                registry::CompileLevel::Phase2 => ("P2", "yellow"),
                registry::CompileLevel::Phase1 => ("P1", "blue"),
            };
            html.push_str(&format!(
                "<tr><td style=\"color: var(--accent); font-weight: 600;\">{}</td>\
                 <td style=\"color: var(--text-dim);\">{}</td><td>{}</td>\
                 <td><span class=\"badge badge-{}\">{}</span></td>\
                 <td style=\"color: var(--text-dim);\">{}</td></tr>\n",
                esc(sig.name), esc(&params), arity, lc, ls, esc(sig.description)
            ));
        }
        html.push_str("</tbody></table>\n");
    }
    html
}

fn render_stdlib_list() -> String {
    use nb_variates::dsl::{lexer, parser};
    use nb_variates::dsl::ast::Statement;

    let sources = nb_variates::dsl::stdlib_sources();
    let mut html = String::new();

    for (filename, source) in sources {
        let category = source.lines()
            .find(|l| l.trim().starts_with("// @category:"))
            .and_then(|l| l.trim().strip_prefix("// @category:"))
            .map(|s| s.trim().to_string())
            .unwrap_or_else(|| filename.replace(".gk", ""));

        let tokens = match lexer::lex(source) { Ok(t) => t, Err(_) => continue };
        let ast = match parser::parse(tokens) { Ok(a) => a, Err(_) => continue };

        let mut has_modules = false;
        for stmt in &ast.statements {
            if let Statement::ModuleDef(mdef) = stmt {
                if !has_modules {
                    html.push_str(&format!(
                        "<h3 style=\"color: var(--blue); margin: 16px 0 8px; font-size: 13px;\">{category}</h3>\n"
                    ));
                    has_modules = true;
                }
                let params: Vec<String> = mdef.params.iter()
                    .map(|p| format!("{}: {}", p.name, p.typ)).collect();
                let outputs: Vec<String> = mdef.outputs.iter()
                    .map(|o| format!("{}: {}", o.name, o.typ)).collect();
                let sig = format!("({}) &#8594; ({})", params.join(", "), outputs.join(", "));
                let name = esc(&mdef.name);

                html.push_str(&format!(
                    "<div class=\"card\" style=\"margin-bottom: 8px; padding: 12px;\">\
                     <div style=\"display: flex; justify-content: space-between; align-items: center;\">\
                     <div><span style=\"color: var(--accent); font-weight: 600;\">{name}</span>\
                     <span style=\"color: var(--text-dim); margin-left: 8px;\">{sig}</span></div>\
                     <button hx-get=\"/api/stdlib/{name}\" hx-target=\"#src-{name}\" hx-swap=\"innerHTML\" \
                     style=\"background: var(--border);\">source</button></div>\
                     <div id=\"src-{name}\"></div></div>\n"
                ));
            }
        }
    }
    html
}
