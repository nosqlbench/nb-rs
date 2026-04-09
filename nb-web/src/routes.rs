// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! Route handlers for the web UI.
//!
//! Each page route checks the `HX-Request` header. When present (htmx
//! navigation), only the `<main>` content fragment is returned. When
//! absent (direct browser load), the full page with base shell is
//! returned. This avoids separate `/api/*` routes for navigation.

use askama::Template;
use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::Html;
use axum::Form;

use nb_metrics::reporters::openmetrics_parse;
use nb_variates::dsl::registry;
use nb_variates::viz;

use crate::models::*;
use crate::ws::MetricsBroadcast;

/// Returns `true` when the request comes from htmx (partial swap).
fn is_htmx(headers: &HeaderMap) -> bool {
    headers.contains_key("HX-Request")
}

// ─── Dashboard ──────────────────────────────────────────────

pub async fn dashboard(headers: HeaderMap) -> Html<String> {
    let (tc, ops, p99, ec) = (
        "0".into(),
        "\u{2014}".into(),
        "\u{2014}".into(),
        "0".into(),
    );
    let activities = vec![];

    if is_htmx(&headers) {
        let frag = DashboardContentFragment {
            total_cycles: tc, ops_per_sec: ops, p99_ms: p99,
            error_count: ec, activities,
        };
        Html(frag.render().expect("dashboard content fragment"))
    } else {
        let page = DashboardPage {
            total_cycles: tc, ops_per_sec: ops, p99_ms: p99,
            error_count: ec, activities,
        };
        Html(page.render().expect("dashboard template"))
    }
}

// ─── Functions ──────────────────────────────────────────────

pub async fn functions_page(headers: HeaderMap) -> Html<String> {
    let groups = build_function_groups(None);
    if is_htmx(&headers) {
        let frag = FunctionsContentFragment { groups };
        Html(frag.render().expect("functions content fragment"))
    } else {
        let page = FunctionsPage { groups };
        Html(page.render().expect("functions template"))
    }
}

#[derive(serde::Deserialize)]
pub struct FunctionQuery {
    pub q: Option<String>,
}

pub async fn functions_api(Query(query): Query<FunctionQuery>) -> Html<String> {
    let groups = build_function_groups(query.q.as_deref());
    let fragment = FunctionTableFragment { groups };
    Html(fragment.render().expect("function_table fragment"))
}

// ─── Stdlib ─────────────────────────────────────────────────

pub async fn stdlib_page(headers: HeaderMap) -> Html<String> {
    let groups = build_stdlib_groups();
    if is_htmx(&headers) {
        let frag = StdlibContentFragment { groups };
        Html(frag.render().expect("stdlib content fragment"))
    } else {
        let page = StdlibPage { groups };
        Html(page.render().expect("stdlib template"))
    }
}

pub async fn stdlib_source(
    axum::extract::Path(name): axum::extract::Path<String>,
) -> Html<String> {
    let sources = nb_variates::dsl::stdlib_sources();
    for (_filename, source) in sources {
        if source.contains(&format!("{name}(")) {
            return Html(format!(
                "<pre style=\"margin-top: 8px;\">{}</pre>",
                esc(source)
            ));
        }
    }
    Html("<pre>Module not found</pre>".into())
}

// ─── DAG Viewer ─────────────────────────────────────────────

pub async fn dag_page(headers: HeaderMap) -> Html<String> {
    if is_htmx(&headers) {
        let frag = DagContentFragment;
        Html(frag.render().expect("dag content fragment"))
    } else {
        let page = DagPage;
        Html(page.render().expect("dag template"))
    }
}

#[derive(serde::Deserialize)]
pub struct DagRenderForm {
    pub source: String,
    pub format: Option<String>,
}

pub async fn dag_render(Form(form): Form<DagRenderForm>) -> Html<String> {
    let source = form.source.trim();
    if source.is_empty() {
        return Html(
            "<p style=\"color: var(--text-dim);\">Enter GK source to render</p>".into(),
        );
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
        Err(e) => Html(format!(
            "<pre style=\"color: var(--accent);\">Error: {}</pre>",
            esc(&e)
        )),
    }
}

// ─── Activities API ─────────────────────────────────────────

pub async fn activities_api() -> Html<String> {
    let fragment = ActivitiesFragment {
        activities: vec![],
    };
    Html(fragment.render().expect("activities_table fragment"))
}

// ─── Graph Editor ───────────────────────────────────────────

pub async fn graph_editor_page(headers: HeaderMap) -> Html<String> {
    if is_htmx(&headers) {
        let frag = GraphEditorContentFragment;
        Html(frag.render().expect("graph editor content fragment"))
    } else {
        let page = GraphEditorPage;
        Html(page.render().expect("graph editor template"))
    }
}

pub async fn graph_palette() -> axum::Json<Vec<crate::graph::PaletteCategory>> {
    axum::Json(crate::graph::build_palette())
}

pub async fn graph_compile(body: String) -> axum::Json<crate::graph::CompileResult> {
    axum::Json(crate::graph::compile_graph(&body))
}

pub async fn graph_eval(
    axum::Json(req): axum::Json<crate::graph::EvalRequest>,
) -> axum::Json<crate::graph::EvalResult> {
    axum::Json(crate::graph::eval_graph(req))
}

pub async fn graph_plot(
    axum::Json(req): axum::Json<crate::graph::PlotRequest>,
) -> axum::Json<crate::graph::PlotResult> {
    axum::Json(crate::graph::plot_graph(req))
}

// ─── Metrics Ingestion ──────────────────────────────────────

/// Accept metrics in Prometheus text exposition format.
///
/// Running `nbrs run --web=host:port` sessions POST metrics here.
/// The parsed frame is published to all WebSocket subscribers.
pub async fn ingest_prometheus(
    State(broadcast): State<MetricsBroadcast>,
    body: String,
) -> StatusCode {
    let frame = openmetrics_parse::parse_prometheus_text(&body);
    if !frame.samples.is_empty() {
        broadcast.publish(frame);
    }
    StatusCode::NO_CONTENT
}

// ─── Data Building ──────────────────────────────────────────

fn build_function_groups(filter: Option<&str>) -> Vec<(String, Vec<FunctionView>)> {
    let grouped = registry::by_category();
    let filter_lower = filter.map(|f| f.to_lowercase());
    let mut result = Vec::new();

    for (cat, funcs) in grouped {
        let views: Vec<FunctionView> = funcs
            .iter()
            .filter(|sig| match &filter_lower {
                Some(q) if !q.is_empty() => {
                    sig.name.contains(q.as_str())
                        || sig.description.to_lowercase().contains(q.as_str())
                }
                _ => true,
            })
            .map(|sig| {
                let const_info = sig.const_param_info();
                let params = if const_info.is_empty() {
                    String::new()
                } else {
                    let p: Vec<String> = const_info
                        .iter()
                        .map(|(name, req)| {
                            if *req {
                                name.to_string()
                            } else {
                                format!("[{name}]")
                            }
                        })
                        .collect();
                    format!("({})", p.join(", "))
                };
                let arity = if sig.outputs == 0 {
                    format!("{}\u{2192}N", sig.wire_input_count())
                } else {
                    format!("{}\u{2192}{}", sig.wire_input_count(), sig.outputs)
                };
                let level = nb_activity::bindings::probe_compile_level(sig.name);
                let (ls, lc) = match level {
                    registry::CompileLevel::Phase3 => ("P3", "green"),
                    registry::CompileLevel::Phase2 => ("P2", "yellow"),
                    registry::CompileLevel::Phase1 => ("P1", "blue"),
                };
                FunctionView {
                    name: sig.name.to_string(),
                    params_display: params,
                    arity_display: arity,
                    level: ls.to_string(),
                    level_class: lc.to_string(),
                    description: sig.description.to_string(),
                }
            })
            .collect();

        if !views.is_empty() {
            result.push((cat.display_name().to_string(), views));
        }
    }
    result
}

fn build_stdlib_groups() -> Vec<(String, Vec<StdlibModuleView>)> {
    use nb_variates::dsl::ast::Statement;
    use nb_variates::dsl::{lexer, parser};

    let sources = nb_variates::dsl::stdlib_sources();
    let mut result: Vec<(String, Vec<StdlibModuleView>)> = Vec::new();

    for (filename, source) in sources {
        let category = source
            .lines()
            .find(|l| l.trim().starts_with("// @category:"))
            .and_then(|l| l.trim().strip_prefix("// @category:"))
            .map(|s| s.trim().to_string())
            .unwrap_or_else(|| filename.replace(".gk", ""));

        let tokens = match lexer::lex(source) {
            Ok(t) => t,
            Err(_) => continue,
        };
        let ast = match parser::parse(tokens) {
            Ok(a) => a,
            Err(_) => continue,
        };

        let mut modules = Vec::new();
        for stmt in &ast.statements {
            if let Statement::ModuleDef(mdef) = stmt {
                let params: Vec<String> = mdef
                    .params
                    .iter()
                    .map(|p| format!("{}: {}", p.name, p.typ))
                    .collect();
                let outputs: Vec<String> = mdef
                    .outputs
                    .iter()
                    .map(|o| format!("{}: {}", o.name, o.typ))
                    .collect();
                let sig = format!("({}) \u{2192} ({})", params.join(", "), outputs.join(", "));

                modules.push(StdlibModuleView {
                    name: mdef.name.clone(),
                    signature: sig,
                    description: String::new(),
                });
            }
        }
        if !modules.is_empty() {
            result.push((category, modules));
        }
    }
    result
}

fn esc(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}
