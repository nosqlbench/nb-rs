// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! CQL/Cassandra adapter for nb-rs.
//!
//! Uses the Apache Cassandra C++ driver via the `cassandra-cpp`
//! crate. Compatible with Apache Cassandra, ScyllaDB, and DataStax
//! Astra.
//!
//! The engine-agnostic surface — config parsing, consistency enum,
//! op-mode dispatch, the `cql_timeuuid` GK node, default status
//! metrics — lives in [`crate::common`]. This module only contains
//! the cassandra-cpp-specific pieces: connection setup, the three
//! dispenser shapes, and the type-aware value binders.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

use cassandra_cpp as cass;
use cass::LendingIterator;

mod tracing;
use tracing::{TraceLog, TraceRecord};

/// One-shot guard for the cpp-driver's process-global log level.
/// The driver requires `cass_log_set_level` to fire **before** any
/// `cass_cluster_*` / `cass_ssl_*` call; we honor that by setting
/// it on the very first `CqlAdapter::connect` and ignoring any
/// later attempts. Last-write-wins isn't safe here because the
/// driver caches the level at first session creation.
static LOG_LEVEL_INIT: OnceLock<()> = OnceLock::new();

/// Default cpp-driver log threshold. ERROR squelches the noisy
/// "Server-side warning" decoder messages (SAI ANN experimental
/// notices etc.) while still surfacing real connection / auth /
/// driver-internal errors. Override per session via the
/// `cassandra_log_level=` workload param.
const DEFAULT_LOG_LEVEL: cass::LogLevel = cass::LogLevel::ERROR;

fn parse_log_level(s: &str) -> Option<cass::LogLevel> {
    match s.to_ascii_uppercase().as_str() {
        "DISABLED" | "OFF" | "NONE" => Some(cass::LogLevel::DISABLED),
        "CRITICAL"                  => Some(cass::LogLevel::CRITICAL),
        "ERROR"                     => Some(cass::LogLevel::ERROR),
        "WARN" | "WARNING"          => Some(cass::LogLevel::WARN),
        "INFO"                      => Some(cass::LogLevel::INFO),
        "DEBUG"                     => Some(cass::LogLevel::DEBUG),
        "TRACE"                     => Some(cass::LogLevel::TRACE),
        _                           => None,
    }
}

fn apply_log_level_once(params: &HashMap<String, String>) -> Result<(), String> {
    // Decide once: parse the user value (if any) and apply it on
    // the first connect. The OnceLock guard prevents later calls
    // from racing — the cpp-driver doesn't honor level changes
    // after the first session is created anyway.
    let level = match params.get("cassandra_log_level") {
        Some(raw) => parse_log_level(raw).ok_or_else(|| format!(
            "invalid cassandra_log_level '{raw}' — expected one of \
             DISABLED, CRITICAL, ERROR, WARN, INFO, DEBUG, TRACE"
        ))?,
        None => DEFAULT_LOG_LEVEL,
    };
    LOG_LEVEL_INIT.get_or_init(|| {
        cass::set_level(level);
    });
    Ok(())
}

use nbrs_activity::adapter::{
    AdapterError, DriverAdapter, ExecutionError, OpDispenser, OpResult, ResultBody,
};
use crate::common::{CqlConfig, CqlConsistency, STMT_FIELD_NAMES};
use nbrs_workload::model::ParsedOp;

// Bridge: `crate::common::CqlConsistency` → `cass::Consistency`.
// Engine-specific because each driver has its own consistency
// enum; the shared type stays driver-agnostic.
fn to_cass_consistency(c: CqlConsistency) -> cass::Consistency {
    match c {
        CqlConsistency::Any          => cass::Consistency::ANY,
        CqlConsistency::One          => cass::Consistency::ONE,
        CqlConsistency::Two          => cass::Consistency::TWO,
        CqlConsistency::Three        => cass::Consistency::THREE,
        CqlConsistency::Quorum       => cass::Consistency::QUORUM,
        CqlConsistency::All          => cass::Consistency::ALL,
        CqlConsistency::LocalQuorum  => cass::Consistency::LOCAL_QUORUM,
        CqlConsistency::EachQuorum   => cass::Consistency::EACH_QUORUM,
        CqlConsistency::LocalOne     => cass::Consistency::LOCAL_ONE,
    }
}

// =========================================================================
// CqlResultBody: native result type for validation and capture
// =========================================================================

/// Native CQL result body carrying typed row data.
///
/// Consumers can downcast via `as_any()` to extract typed column values
/// without JSON round-tripping — used by `ValidatingDispenser` for
/// relevancy measurement.
#[derive(Debug)]
pub struct CqlResultBody {
    /// Rows as JSON-compatible maps: column_name → value.
    pub rows: Vec<HashMap<String, serde_json::Value>>,
}

impl CqlResultBody {
    /// Build from a cassandra-cpp CassResult by iterating rows and columns.
    fn from_cass_result(result: &cass::CassResult) -> Self {
        let row_count = result.row_count() as usize;
        let mut rows = Vec::with_capacity(row_count);
        let col_count = result.column_count() as usize;
        let mut iter = result.iter();
        while let Some(row) = iter.next() {
            let mut map = HashMap::new();
            for col_idx in 0..col_count {
                let col_name = result.column_name(col_idx)
                    .unwrap_or("?")
                    .to_string();
                let value = Self::extract_column_value(&row, col_idx);
                map.insert(col_name, value);
            }
            rows.push(map);
        }
        Self { rows }
    }

    /// Extract a single column value as serde_json::Value.
    fn extract_column_value(row: &cass::Row, col_idx: usize) -> serde_json::Value {
        // Try common types in order of likelihood
        if let Ok(v) = row.get_column(col_idx).and_then(|c| c.get_string()) {
            return serde_json::Value::String(v);
        }
        if let Ok(v) = row.get_column(col_idx).and_then(|c| c.get_i64()) {
            return serde_json::json!(v);
        }
        if let Ok(v) = row.get_column(col_idx).and_then(|c| c.get_i32()) {
            return serde_json::json!(v);
        }
        if let Ok(v) = row.get_column(col_idx).and_then(|c| c.get_f64()) {
            return serde_json::json!(v);
        }
        if let Ok(v) = row.get_column(col_idx).and_then(|c| c.get_f32()) {
            return serde_json::json!(v);
        }
        if let Ok(v) = row.get_column(col_idx).and_then(|c| c.get_bool()) {
            return serde_json::json!(v);
        }
        // Fallback: null for unsupported types
        serde_json::Value::Null
    }

    /// Get a column value from the first row as i64 (for relevancy extraction).
    pub fn get_column_i64_values(&self, column: &str) -> Vec<i64> {
        self.rows.iter()
            .filter_map(|row| row.get(column)?.as_i64())
            .collect()
    }

    /// Get a column value from the first row as string (for capture).
    pub fn get_column_string_values(&self, column: &str) -> Vec<String> {
        self.rows.iter()
            .filter_map(|row| {
                let v = row.get(column)?;
                match v {
                    serde_json::Value::String(s) => Some(s.clone()),
                    other => Some(other.to_string()),
                }
            })
            .collect()
    }
}

impl ResultBody for CqlResultBody {
    fn to_json(&self) -> serde_json::Value {
        serde_json::Value::Array(
            self.rows.iter()
                .map(|row| serde_json::Value::Object(
                    row.iter()
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect()
                ))
                .collect()
        )
    }

    fn as_any(&self) -> &dyn std::any::Any { self }

    fn element_count(&self) -> u64 { self.rows.len() as u64 }
}

// CqlConfig + consistency parsing live in `crate::common`.
// Use `CqlConfig::from_params(params)` to parse; convert the
// resulting `CqlConsistency` to `cass::Consistency` via
// `to_cass_consistency()` above.

/// CQL adapter using the Apache Cassandra C++ driver.
pub struct CqlAdapter {
    session: cass::Session,
    consistency: cass::Consistency,
    /// Per-execute tracing probability (0.0–1.0). Stored as
    /// `f64::to_bits` in an AtomicU64 so dispensers can read
    /// without locking. Backs the `cql_trace_rate` dynamic
    /// control declared in [`Self::declare_controls`]; the
    /// control's applier is the single writer. Zero by default
    /// (tracing off).
    trace_rate_bits: Arc<AtomicU64>,
    /// Bounded retirement queue + JSON-line log writer for
    /// traced ops. Cloned into every dispenser the adapter
    /// materialises; `submit` is the only producer surface and
    /// is non-blocking. `None` when the operator declined
    /// tracing entirely (`trace_log=` set to a sentinel like
    /// `off`, or initial trace_rate==0 *and* no override path
    /// configured) — but in practice we always allocate one
    /// because the rate is a live dynamic control and we
    /// don't want to require a process restart to enable it.
    trace_log: Option<TraceLog>,
}

unsafe impl Send for CqlAdapter {}
unsafe impl Sync for CqlAdapter {}

/// Collapse a multi-line statement to a single line for
/// error-diagnostic display: trim each line, drop empty
/// lines, and join with a single space. Truncates at a
/// SRD-68 Push 5c — walk a CQL statement text and resolve every
/// `{name}` placeholder, classifying each into one of two buckets:
///
/// - **Structural** — `lookup(name)` returns `Some(v)` against the
///   dispenser's canonical kernel at construction time. The value
///   is stable for the duration of a phase activation (workload
///   param, iter var, cascaded extern) and CAN'T be a `?` marker
///   in CQL's prepared-statement grammar (table names, keyspace,
///   option values). Substitute the value as text inline.
///
/// - **Per-cycle** — `lookup(name)` returns `None`. The name is
///   either an output binding (phase `bindings:` LHS, `result:`
///   LHS) that varies per cycle, or a coordinate / capture port.
///   Replace with `?`; remember the name in `bind_names` in
///   declaration order so the dispenser can pull the value via
///   `wires.get(name)` at cycle time.
///
/// Honours the same brace-discipline as
/// `nbrs_workload::bindpoints::extract_bind_points`: `{` followed
/// by `'`/`"` is a CQL map-literal opener (emit the brace,
/// continue scanning so nested `{name}` placeholders inside still
/// resolve); depth-tracking finds the true matching `}`;
/// inline-expression `{{...}}` and qualifier-prefixed
/// `{bind:name}` shapes pass through unchanged.
fn resolve_structural_and_mark_remaining<F>(
    template: &str,
    mut lookup: F,
) -> (String, Vec<String>)
where
    F: FnMut(&str) -> Option<nbrs_variates::node::Value>,
{
    let chars: Vec<char> = template.chars().collect();
    let n = chars.len();
    let mut out = String::with_capacity(template.len());
    let mut bind_names: Vec<String> = Vec::new();
    let mut i = 0;
    while i < n {
        // `\{` / `\}` — pass through, two chars.
        if chars[i] == '\\' && i + 1 < n && (chars[i + 1] == '{' || chars[i + 1] == '}') {
            out.push(chars[i]);
            out.push(chars[i + 1]);
            i += 2;
            continue;
        }
        // `{{ ... }}` — inline-expression form, pass through.
        if i + 1 < n && chars[i] == '{' && chars[i + 1] == '{' {
            let start = i;
            let mut j = i + 2;
            while j + 1 < n && !(chars[j] == '}' && chars[j + 1] == '}') {
                j += 1;
            }
            let end = (j + 2).min(n);
            for k in start..end { out.push(chars[k]); }
            i = end;
            continue;
        }
        if chars[i] != '{' {
            out.push(chars[i]);
            i += 1;
            continue;
        }
        // CQL map / JSON object literal: `{` followed by `'`/`"`.
        // Emit just the `{` and continue scanning so any nested
        // `{name}` placeholders inside still resolve.
        if i + 1 < n && (chars[i + 1] == '\'' || chars[i + 1] == '"') {
            out.push('{');
            i += 1;
            continue;
        }
        let body_start = i + 1;
        let mut j = body_start;
        let mut depth: u32 = 1;
        while j < n {
            if chars[j] == '{' { depth += 1; }
            if chars[j] == '}' { depth -= 1; if depth == 0 { break; } }
            j += 1;
        }
        if j >= n {
            out.push('{');
            i += 1;
            continue;
        }
        let body: String = chars[body_start..j].iter().collect();
        let body = body.trim();
        let after = j + 1;
        if body.is_empty() {
            out.push('{');
            out.push('}');
            i = after;
            continue;
        }
        // Qualifier-prefixed (`{bind:name}`, etc.) and non-bare
        // identifiers pass through verbatim — same discipline as
        // `nbrs_activity::wires::substitute_via_wires`.
        if body.contains(':') || !body.chars().next().is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
            || !body.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
        {
            out.push('{');
            out.push_str(body);
            out.push('}');
            i = after;
            continue;
        }
        // Bare identifier — try construction-time lookup. Some →
        // structural inline; None → per-cycle `?` marker.
        match lookup(body) {
            Some(v) => {
                // Structural: substitute the value as text. If
                // the workload-author wrote `'{name}'` with
                // surrounding quotes (the typical CQL pattern for
                // string-typed values), the substituted display
                // form lands inside those quotes — same shape as
                // the legacy text-mutation pass.
                out.push_str(&v.to_display_string());
            }
            None => {
                // Per-cycle binding becomes a `?` marker. If the
                // workload wrapped this placeholder in matching
                // quotes (`'{name}'`), strip them — CQL `?`
                // markers stand in place of the entire quoted
                // string literal, never inside quotes. Mirrors
                // the legacy `replace_bind_points_with_markers`
                // quoted-form-first heuristic.
                let next_after = chars.get(after).copied();
                let last_emitted = out.chars().last();
                let strip_quotes = match (last_emitted, next_after) {
                    (Some('\''), Some('\'')) => true,
                    (Some('"'), Some('"')) => true,
                    _ => false,
                };
                if strip_quotes {
                    out.pop();
                    out.push('?');
                    bind_names.push(body.to_string());
                    i = after + 1;
                    continue;
                }
                out.push('?');
                bind_names.push(body.to_string());
            }
        }
        i = after;
    }
    (out, bind_names)
}

/// generous bound so a hand-rolled `BATCH` with thousands
/// of statements doesn't blow the error message size.
fn flatten_one_line(s: &str) -> String {
    const MAX: usize = 400;
    let joined: String = s.lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .collect::<Vec<_>>()
        .join(" ");
    if joined.len() > MAX {
        // Honour char boundaries — naive [..MAX] could
        // split a multi-byte char and panic.
        let cutoff = joined.char_indices()
            .take_while(|(i, _)| *i < MAX)
            .last()
            .map(|(i, c)| i + c.len_utf8())
            .unwrap_or(0);
        format!("{}…", &joined[..cutoff])
    } else {
        joined
    }
}

/// Wrap a connect-error string with actionable resource-exhaustion
/// diagnostics when the cpp-driver's failure code points at
/// per-process limits (`LIB_UNABLE_TO_INIT`,
/// `LIB_NO_HOSTS_AVAILABLE` after a long-running session, …).
///
/// libuv (which the cpp-driver uses for its event loop) reports
/// `UNABLE_TO_INIT` when `epoll_create1` / `eventfd` / pipe2
/// returns `EMFILE` / `ENFILE` / `EAGAIN` — i.e. the process has
/// run through its file-descriptor or thread allowance. The bare
/// driver string ("Unable to initialize cluster event loop")
/// gives the operator no hint that this is environmental rather
/// than a Cassandra-side problem; with the driver also being a C++
/// dependency, the chase up the stack is non-obvious.
///
/// We append a snapshot of the relevant per-process limits and
/// counters so the operator can see at a glance whether they're
/// up against `RLIMIT_NOFILE` or `RLIMIT_NPROC`. The snapshot is
/// best-effort — `/proc` reads may fail in a sandbox; on those
/// platforms the suffix is just the contextual hint without raw
/// numbers.
fn enrich_connect_error(stage: &str, raw: String) -> String {
    let needs_diag =
        raw.contains("LIB_UNABLE_TO_INIT")
        || raw.contains("Unable to initialize");
    if !needs_diag {
        return format!("{stage}: {raw}");
    }
    let snap = process_resource_snapshot();
    format!(
        "{stage}: {raw}\n\
         \n\
         This error from the Cassandra C++ driver almost always\n\
         indicates *per-process resource exhaustion* — usually\n\
         file descriptors or threads — not a Cassandra-side\n\
         problem. The driver's libuv backend reports this when\n\
         `epoll_create1` / `eventfd` / `pipe2` fails inside\n\
         `uv_loop_init`, which on Linux means the kernel is\n\
         refusing the syscall (`EMFILE`/`ENFILE`/`EAGAIN`).\n\
         \n\
         Process resource snapshot:\n\
         {snap}\n\
         \n\
         If `fds_in_use` is at or near `nofile_soft`, raise the\n\
         FD limit (e.g. `ulimit -n 65536` before the run, or set\n\
         `LimitNOFILE=` in the systemd unit). If the run\n\
         exhausted FDs over many phases (consistent failure at\n\
         the same phase index), suspect a per-phase\n\
         CqlAdapter/session leak — each phase rebuilds the\n\
         adapter and the previous session's resources need to\n\
         release fully before the next phase opens a new one."
    )
}

/// Best-effort snapshot of the per-process resource counters
/// we care about for the LIB_UNABLE_TO_INIT diagnostic. Each
/// line is rendered as `key: value` with `?` filling in for
/// platforms or sandboxes where the source isn't readable.
fn process_resource_snapshot() -> String {
    fn read_or_q(path: &str) -> String {
        std::fs::read_to_string(path).map(|s| s.trim().to_string())
            .unwrap_or_else(|_| "?".into())
    }
    let fds = std::fs::read_dir("/proc/self/fd")
        .map(|d| d.count().to_string())
        .unwrap_or_else(|_| "?".into());
    let nofile_soft = read_or_q("/proc/self/limits");
    // /proc/self/limits is multi-line; pull just the rows we need.
    let limit_for = |needle: &str| -> (String, String) {
        if nofile_soft == "?" { return ("?".into(), "?".into()); }
        for line in nofile_soft.lines() {
            if line.starts_with(needle) {
                // Format: "Max open files            65536                65536                files"
                let cols: Vec<&str> = line.split_whitespace().collect();
                if cols.len() >= 4 {
                    let n = cols.len();
                    return (cols[n-3].into(), cols[n-2].into());
                }
            }
        }
        ("?".into(), "?".into())
    };
    let (nofile_s, nofile_h)   = limit_for("Max open files");
    let (nproc_s,  nproc_h)    = limit_for("Max processes");
    let threads = std::fs::read_dir("/proc/self/task")
        .map(|d| d.count().to_string())
        .unwrap_or_else(|_| "?".into());
    format!(
        "  fds_in_use:    {fds}\n\
         \x20 nofile_soft:   {nofile_s}\n\
         \x20 nofile_hard:   {nofile_h}\n\
         \x20 threads_alive: {threads}\n\
         \x20 nproc_soft:    {nproc_s}\n\
         \x20 nproc_hard:    {nproc_h}",
    )
}

impl CqlAdapter {
    pub async fn connect(config: &CqlConfig) -> Result<Self, String> {
        let mut cluster = cass::Cluster::default();
        cluster.set_contact_points(&config.hosts)
            .map_err(|e| format!("set contact points: {e}"))?;
        cluster.set_port(config.port)
            .map_err(|e| format!("set port: {e}"))?;
        if let (Some(u), Some(p)) = (&config.username, &config.password) {
            cluster.set_credentials(u, p)
                .map_err(|e| format!("set credentials: {e}"))?;
        }
        cluster.set_request_timeout(std::time::Duration::from_millis(config.request_timeout_ms));

        // `common::CqlConfig::from_params` already validated the
        // consistency string at parse time, so this conversion is
        // total.
        let consistency = to_cass_consistency(config.consistency);

        // Try to connect to the specified keyspace. If it doesn't exist,
        // fall back to connecting without a keyspace (needed for DDL phases
        // that create the keyspace).
        let session = if config.keyspace.is_empty() {
            cluster.connect().await
                .map_err(|e| enrich_connect_error("connect", e.to_string()))?
        } else {
            match cluster.connect_keyspace(&config.keyspace).await {
                Ok(s) => s,
                Err(e) => {
                    let msg = e.to_string();
                    // Only fall back for keyspace-not-found errors.
                    // Auth failures, network errors, etc. should propagate.
                    if msg.contains("Keyspace") || msg.contains("keyspace") || msg.contains("not found") {
                        nbrs_activity::observer::log(
                            nbrs_activity::observer::LogLevel::Warn,
                            &format!(
                                "cql/cassandra-cpp: keyspace '{}' not found, connecting without keyspace",
                                config.keyspace));
                        cluster.connect().await
                            .map_err(|e| enrich_connect_error("connect (no keyspace)", e.to_string()))?
                    } else {
                        return Err(enrich_connect_error(
                            &format!("connect to keyspace '{}'", config.keyspace),
                            msg,
                        ));
                    }
                }
            }
        };

        // Initial trace rate is the workload-param value (0.0
        // when absent / off). Stored as f64-bits in the Atomic
        // so dispensers can `f64::from_bits(load())` per cycle.
        let initial_trace_rate: f64 = config.trace_rate.unwrap_or(0.0);
        let trace_rate_bits = Arc::new(AtomicU64::new(initial_trace_rate.to_bits()));

        // Trace log: per-process file the retirement worker
        // appends to. Default lives under the active session
        // dir so traces ride along with the rest of the run's
        // artifacts; explicit override via `trace_log=` lets
        // operators redirect to a known stable path.
        let trace_log_path = resolve_trace_log_path(config);
        let trace_log = match TraceLog::open(trace_log_path.clone(), session.clone()) {
            Ok(log) => Some(log),
            Err(e) => {
                nbrs_activity::observer::log(
                    nbrs_activity::observer::LogLevel::Warn,
                    &format!(
                        "cql tracing log unavailable at {}: {} \
                         — `cql_trace_rate` writes will succeed but no \
                         records will be retired",
                        trace_log_path.display(), e,
                    ));
                None
            }
        };

        Ok(Self {
            session,
            consistency,
            trace_rate_bits,
            trace_log,
        })
    }

    // Note: dispensers read `trace_rate_bits` and `trace_log`
    // directly via `self.trace_rate_bits.clone()` /
    // `self.trace_log.clone()` from inside `map_op`. Earlier
    // getter wrappers (`trace_rate_handle` / `trace_log_handle`)
    // were removed as dead code — the field-direct path is the
    // canonical one.
}

/// Resolve where the trace log gets written. Operator override
/// via the `trace_log=` workload param wins; otherwise the
/// `logs/latest/cql_traces.jsonl` path that the runner's
/// `logs/latest -> logs/<session_id>` symlink keeps current.
/// The symlink is created by `Session::new` before adapters
/// connect, so this resolves consistently across the run.
fn resolve_trace_log_path(config: &CqlConfig) -> std::path::PathBuf {
    if let Some(ref explicit) = config.trace_log_path {
        return std::path::PathBuf::from(explicit);
    }
    std::path::PathBuf::from("logs/latest/cql_traces.jsonl")
}

// =========================================================================
// DriverAdapter: dispatch to the correct dispenser based on field name
// =========================================================================

// `STMT_FIELD_NAMES` is imported from `crate::common`; its
// dispatch logic is the same across every CQL engine.

impl DriverAdapter for CqlAdapter {
    fn name(&self) -> &str { "cql" }

    fn default_status_metrics(&self) -> Vec<nbrs_activity::adapter::StatusMetric> {
        crate::common::default_status_metrics()
    }

    fn declare_controls(
        &self,
        parent: &Arc<std::sync::RwLock<nbrs_metrics::component::Component>>,
    ) {
        use nbrs_metrics::component::{attach, Component};
        use nbrs_metrics::controls::{
            BranchScope, ControlBuilder, SyncApplier,
        };
        use nbrs_metrics::labels::Labels;

        // One subcomponent per adapter instance, attached to the
        // activity. The name `cql` matches the user-facing adapter
        // name so the control's effective-labels path reads
        // `…/adapter=cql/cql_trace_rate` — discoverable in
        // dryrun=controls and the web /api/controls listing.
        //
        // Idempotency (SRD 23): `declare_controls` is the trait
        // contract for adapter control surface. The runtime calls
        // it both at phase-attach time (so `dryrun=controls` can
        // walk the tree before any cycles run) and at run start
        // (so adapters that materialize only at run time still get
        // a chance). Look up an existing `adapter=cql` subcomponent
        // before creating one so a second call doesn't produce a
        // duplicate sibling, and short-circuit if `cql_trace_rate`
        // is already declared on it.
        let cql_component = {
            let parent_guard = parent.read().unwrap_or_else(|e| e.into_inner());
            let existing = parent_guard.children()
                .find(|c| {
                    let g = c.read().unwrap_or_else(|e| e.into_inner());
                    g.labels().get("adapter") == Some("cql")
                })
                .cloned();
            drop(parent_guard);
            match existing {
                Some(c) => {
                    if c.read().unwrap_or_else(|e| e.into_inner())
                        .controls().get_erased("cql_trace_rate").is_some()
                    {
                        return;
                    }
                    c
                }
                None => {
                    let new_c = Arc::new(std::sync::RwLock::new(Component::new(
                        Labels::of("adapter", "cql"),
                        std::collections::HashMap::new(),
                    )));
                    attach(parent, &new_c);
                    new_c
                }
            }
        };

        // The applier writes f64-bits into the AtomicU64 the
        // dispensers read per cycle. SyncApplier is fine here:
        // the write is just an atomic store, no I/O.
        let bits_for_apply = self.trace_rate_bits.clone();
        let initial_rate = f64::from_bits(self.trace_rate_bits.load(Ordering::Acquire));
        let trace_control: nbrs_metrics::controls::Control<f64> =
            ControlBuilder::new("cql_trace_rate", initial_rate)
                .reify_as_gauge(|v: &f64| Some(*v))
                .from_f64(|v| {
                    if !v.is_finite() || !(0.0..=1.0).contains(&v) {
                        Err(format!(
                            "cql_trace_rate must be a finite probability in [0.0, 1.0]; got {v}"
                        ))
                    } else {
                        Ok(v)
                    }
                })
                .branch_scope(BranchScope::Local)
                .build();
        trace_control.register_applier(SyncApplier::new(move |v: f64| {
            bits_for_apply.store(v.to_bits(), Ordering::Release);
            Ok(())
        }));
        cql_component.read().unwrap_or_else(|e| e.into_inner())
            .controls().declare(trace_control);
    }

    fn map_op(
        &self,
        template: &ParsedOp,
        parent: std::sync::Arc<nbrs_variates::kernel::GkKernel>,
    ) -> Result<Box<dyn OpDispenser>, String> {
        // Find the statement text and determine execution mode from the field name.
        let (stmt_text, mode) = STMT_FIELD_NAMES.iter()
            .find_map(|key| -> Option<(String, &str)> {
                let v = template.op.get(*key)?;
                let text = v.as_str()?;
                Some((text.to_string(), *key))
            })
            .ok_or_else(|| "CQL op requires a 'poll:', 'raw:', 'simple:', 'prepared:', or 'stmt:' field".to_string())?;

        // SRD-68 Push 5c — construction-time structural resolution
        // for prepared mode. Walk every `{name}` in the statement
        // text against the dispenser's canonical kernel:
        //   - If `canonical.lookup(name)` returns `Some(v)` the
        //     name resolves to a stable-per-phase-activation value
        //     (workload param, iter var, cascaded extern). Inline
        //     `v.to_display_string()` directly into the SQL — the
        //     CQL prepared-statement compiler can't accept `?`
        //     markers for structural positions like keyspace /
        //     table / option values.
        //   - Else the name is a per-cycle output binding (phase
        //     `bindings:` LHS, `result:` LHS). Mark it with `?`
        //     and remember its name for cycle-time `wires.get`
        //     binding.
        // The result is a CQL-parameterised `prepared_text` plus
        // `bind_names` in `?`-position order.
        //
        // The dispenser is now self-sufficient — it doesn't
        // depend on the upstream `resolve_placeholders_via_kernel`
        // mutation pass having pre-resolved structural names. When
        // that pass lands its validator-only form (Push 5c step 3)
        // the dispenser keeps working unchanged.
        let parent_for_lookup = parent.clone();
        let (prepared_text, bind_names) = resolve_structural_and_mark_remaining(
            &stmt_text,
            |name| parent_for_lookup.lookup(name),
        );

        let session = SessionHandle(&self.session as *const cass::Session);
        let consistency = self.consistency;

        // Check for batch configuration on this op.
        // batch: <integer> — batch size (rows per batch), type defaults to unlogged.
        // batchtype: logged|unlogged|counter — overrides batch type.
        let has_batch = template.params.contains_key("batch");
        let batch_size: usize = template.params.get("batch")
            .and_then(|v| v.as_u64().or_else(|| v.as_str().and_then(|s| s.parse().ok())))
            .unwrap_or(0) as usize;
        let batch_type = template.params.get("batchtype")
            .and_then(|v| v.as_str())
            .map(|s| match s.to_lowercase().as_str() {
                "logged" => cass::BatchType::LOGGED,
                "counter" => cass::BatchType::COUNTER,
                _ => cass::BatchType::UNLOGGED,
            })
            .unwrap_or(cass::BatchType::UNLOGGED);

        // SRD-68 invariant I-3: dispenser owns its canonical kernel.
        // For Push 2b, no op-level GK matter is assembled here yet —
        // the canonical kernel is the parent (phase scope) directly.
        // Push 3 will fan out per-fiber kernels from this canonical;
        // a follow-up will let CQL ops with their own `bindings:` /
        // `result:` block materialise a child subscope via
        // `parent.build_subscope(matter)`.
        let canonical_kernel = parent;

        match mode {
            "raw" => {
                Ok(Box::new(CqlRawDispenser {
                    session,
                    stmt_template: stmt_text.clone(),
                    canonical_kernel,
                    trace_rate_bits: self.trace_rate_bits.clone(),
                    trace_log: self.trace_log.clone(),
                }))
            }
            "simple" => {
                Ok(Box::new(CqlRawDispenser {
                    session,
                    stmt_template: stmt_text.clone(),
                    canonical_kernel,
                    trace_rate_bits: self.trace_rate_bits.clone(),
                    trace_log: self.trace_log.clone(),
                }))
            }
            _ => {
                if has_batch {
                    // Batch dispenser created (type logged via observer)
                    Ok(Box::new(CqlBatchDispenser {
                        session,
                        consistency,
                        stmt_text: prepared_text.clone(),
                        stmt_field: "stmt".to_string(),
                        bind_names,
                        canonical_kernel,
                        batch_size: if batch_size == 0 { 1 } else { batch_size },
                        prepared: std::sync::OnceLock::new(),
                        batch_type,
                        rows_timer: nbrs_metrics::instruments::timer::Timer::new(
                            nbrs_metrics::labels::Labels::of("name", "rows_inserted"),
                        ),
                        rows_total: std::sync::atomic::AtomicU64::new(0),
                        trace_rate_bits: self.trace_rate_bits.clone(),
                        trace_log: self.trace_log.clone(),
                    }))
                } else if bind_names.is_empty() {
                    // No bind points — execute as raw (DDL, simple queries)
                    Ok(Box::new(CqlRawDispenser {
                        session,
                        stmt_template: stmt_text.clone(),
                        canonical_kernel,
                        trace_rate_bits: self.trace_rate_bits.clone(),
                        trace_log: self.trace_log.clone(),
                    }))
                } else {
                    Ok(Box::new(CqlPreparedDispenser {
                        session,
                        consistency,
                        stmt_text: prepared_text,
                        bind_names,
                        canonical_kernel,
                        prepared: std::sync::OnceLock::new(),
                        binders: std::sync::OnceLock::new(),
                        trace_rate_bits: self.trace_rate_bits.clone(),
                        trace_log: self.trace_log.clone(),
                    }))
                }
            }
        }
    }

    /// SRD-35 Push D — graceful CQL session close.
    ///
    /// The vendored `cassandra-cpp::Session::close()` wraps
    /// `cass_session_close()`, which the C++ driver implements
    /// as a flush of in-flight requests followed by a connection
    /// teardown. We await the resulting future inside a 5-second
    /// timeout so a hung node doesn't pin the runtime; on
    /// timeout the underlying `Drop` still runs the synchronous
    /// close as a fallback when the adapter's last reference
    /// goes away.
    ///
    /// This override fires from
    /// [`nbrs_activity::resource_pool::SharedAdapterResource::close`]
    /// when the pool determines a shared CQL adapter has no
    /// remaining users.
    fn shutdown<'a>(
        &'a self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            let close_future = self.session.close();
            let timeout = std::time::Duration::from_secs(5);
            match tokio::time::timeout(timeout, close_future).await {
                Ok(Ok(())) => {
                    nbrs_activity::diag!(
                        nbrs_activity::observer::LogLevel::Info,
                        "cql session closed cleanly",
                    );
                }
                Ok(Err(e)) => {
                    nbrs_activity::diag!(
                        nbrs_activity::observer::LogLevel::Error,
                        "cql session close returned error: {e}; falling back to Drop teardown",
                    );
                }
                Err(_) => {
                    nbrs_activity::diag!(
                        nbrs_activity::observer::LogLevel::Error,
                        "cql session close timed out after 5s; falling back to Drop teardown",
                    );
                }
            }
        })
    }
}

// =========================================================================
// Session handle wrapper (Send+Sync for raw pointer)
// =========================================================================

struct SessionHandle(*const cass::Session);
unsafe impl Send for SessionHandle {}
unsafe impl Sync for SessionHandle {}

impl SessionHandle {
    fn get(&self) -> &cass::Session {
        unsafe { &*self.0 }
    }
}

// =========================================================================
// CqlRawDispenser: string interpolation, direct execute
// =========================================================================

/// Executes the fully-interpolated statement text directly.
///
/// Used for:
/// - `raw:` mode (all bind points resolved to text by the executor)
/// - `simple:` mode (same driver path, distinction preserved for API)
/// - `prepared:`/`stmt:` mode when there are no bind points (DDL)
struct CqlRawDispenser {
    session: SessionHandle,
    /// Statement template captured at `map_op` time —
    /// retains the `{name}` bind-point placeholders the
    /// operator wrote in the workload yaml. Used by
    /// [`OpDispenser::describe`] to surface the op shape
    /// in error diagnostics. Per-cycle execution reads
    /// the fully-interpolated text from `ctx.fields`; this
    /// field is informational only.
    stmt_template: String,
    /// SRD-68 invariant I-3: dispenser-owned canonical GK
    /// kernel for op-template-scope name resolution. Push 2b
    /// stores the parent reference directly; Push 3 will fan
    /// out per-fiber kernels from this canonical via
    /// `build_subscope` for cycle-time reads through the
    /// narrow `WireSource` trait.
    #[allow(dead_code)]
    canonical_kernel: std::sync::Arc<nbrs_variates::kernel::GkKernel>,
    /// Live tracing probability (f64 bits). Loaded per execute;
    /// `cql_trace_rate` control writes here.
    trace_rate_bits: Arc<AtomicU64>,
    /// Bounded retirement queue handle for traced ops.
    /// `None` when the trace log file couldn't be opened at
    /// adapter init — dispenser still respects the rate
    /// (sets the tracing flag on the statement) but skips the
    /// post-execute submit.
    trace_log: Option<TraceLog>,
}

impl OpDispenser for CqlRawDispenser {
    fn describe(&self) -> Option<String> {
        // Single-line shape of the statement template,
        // collapsing internal whitespace runs to one
        // space so an indented multi-line `raw: |` block
        // reads cleanly in an error message.
        Some(format!("CQL raw: {}", flatten_one_line(&self.stmt_template)))
    }

    fn describe_resolved(&self, wires: &dyn nbrs_activity::wires::WireSource) -> Option<String> {
        // SRD-68 Push 5: render the post-substitution statement
        // through the same `substitute_via_wires` path the cycle
        // uses so the operator sees exactly what was sent. Bind
        // failures (unresolved name) surface in the message; the
        // describe-resolved is best-effort, so a None on error is
        // fine.
        nbrs_activity::wires::substitute_via_wires(&self.stmt_template, wires)
            .ok()
            .map(|s| format!("CQL raw: {}", flatten_one_line(&s)))
    }

    fn canonical_kernel(&self) -> Option<&std::sync::Arc<nbrs_activity::adapter::GkKernel>> {
        Some(&self.canonical_kernel)
    }

    fn execute<'a>(
        &'a self,
        cycle: u64,
        ctx: &'a nbrs_activity::adapter::ExecCtx<'a>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<OpResult, ExecutionError>> + Send + 'a>> {
        let wires = ctx.wires;
        Box::pin(async move {
            // SRD-68 Push 5: cycle-time bind-point resolution
            // through the dispenser's per-fiber GK kernel via the
            // narrow `WireSource` trait. Walks the pristine
            // statement template stored at construction and
            // resolves each `{name}` against the per-fiber kernel
            // slot that the executor handed in via
            // `ExecCtx::wires`. Single resolution surface per
            // SRD-68 invariant I-1; legacy `fields.get_str` path
            // retired for CQL raw mode.
            let stmt_text_owned = nbrs_activity::wires::substitute_via_wires(
                &self.stmt_template, wires,
            ).map_err(|msg| ExecutionError::Op(AdapterError {
                error_name: "unresolved_bind_point".into(),
                message: msg,
                retryable: false,
            }))?;
            let stmt_text: &str = stmt_text_owned.as_str();

            // Sparse-tracing decision per execute. Atomic load is
            // cheap (single 64-bit read); the RNG roll only fires
            // when the rate is non-zero, so the no-tracing hot
            // path stays effectively free.
            let trace_rate = f64::from_bits(self.trace_rate_bits.load(Ordering::Acquire));
            let trace_this = trace_rate > 0.0
                && rand::random::<f64>() < trace_rate;

            // Capture metadata for the trace log before running.
            // `started_at` is the wall-clock for the
            // `system_traces.sessions` time-window correlation;
            // `started` is the monotonic clock for latency.
            let started_at = std::time::SystemTime::now();
            let started = std::time::Instant::now();

            // Two execute paths so the no-trace hot path stays
            // exactly the existing shape — `Session::execute(&str)`
            // doesn't expose tracing, so on hit we explicitly
            // build the Statement, set the tracing flag, and use
            // the vendored `execute_with_tracing` surface that
            // pairs result with `cass_future_tracing_id`.
            let exec_outcome = if trace_this {
                let mut stmt = self.session.get().statement(stmt_text);
                let _ = stmt.set_tracing(true);
                self.session.get()
                    .execute_with_tracing(&stmt)
                    .await
                    .map(|(r, tid)| (r, tid))
            } else {
                self.session.get().execute(stmt_text).await.map(|r| (r, None))
            };

            let latency_nanos = started.elapsed().as_nanos() as u64;

            let exec_result = match exec_outcome {
                Ok((result, trace_id)) => {
                    if trace_this
                        && let Some(log) = self.trace_log.as_ref()
                    {
                        log.submit(TraceRecord {
                            cycle,
                            started_at,
                            query: stmt_text.to_string(),
                            // Raw ops have no bind points — the
                            // statement text is already fully
                            // interpolated by the executor.
                            binds: Vec::new(),
                            latency_nanos,
                            ok: true,
                            error_name: None,
                            trace_id: trace_id.map(|u| {
                                let std_uuid: uuid::Uuid = u.into();
                                std_uuid.to_string()
                            }),
                        });
                    }
                    Ok(result)
                }
                Err(e) => {
                    let truncated = if stmt_text.len() > 200 {
                        format!("{}...", &stmt_text[..200])
                    } else {
                        stmt_text.to_string()
                    };
                    if trace_this
                        && let Some(log) = self.trace_log.as_ref()
                    {
                        log.submit(TraceRecord {
                            cycle,
                            started_at,
                            query: stmt_text.to_string(),
                            binds: Vec::new(),
                            latency_nanos,
                            ok: false,
                            error_name: Some("cql_error".into()),
                            trace_id: None,
                        });
                    }
                    Err(ExecutionError::Op(AdapterError {
                        error_name: "cql_error".into(),
                        message: format!("{e}\n  statement: {truncated}"),
                        retryable: false,
                    }))
                }
            };

            let result = exec_result?;

            let body = if result.row_count() > 0 {
                Some(Box::new(CqlResultBody::from_cass_result(&result)) as Box<dyn ResultBody>)
            } else {
                None
            };
            Ok(OpResult {
                body,
                skipped: false,
            })
        })
    }
}

// =========================================================================
// CqlPreparedDispenser: prepare once, bind typed values per cycle
// =========================================================================

/// Prepares the statement lazily on first execute, then binds typed
/// values by name for each subsequent cycle.
struct CqlPreparedDispenser {
    session: SessionHandle,
    consistency: cass::Consistency,
    stmt_text: String,
    /// Names of bind point fields to extract from ResolvedFields.
    bind_names: Vec<String>,
    /// SRD-68 invariant I-3: dispenser-owned canonical GK kernel.
    /// See `CqlRawDispenser::canonical_kernel`.
    #[allow(dead_code)]
    canonical_kernel: std::sync::Arc<nbrs_variates::kernel::GkKernel>,
    /// Prepared once on first execute, then lock-free reads thereafter.
    prepared: std::sync::OnceLock<Arc<cass::PreparedStatement>>,
    /// Type-aware binders built once from prepared statement metadata.
    binders: std::sync::OnceLock<Vec<BinderFn>>,
    /// Live tracing probability (f64 bits). Loaded per execute;
    /// `cql_trace_rate` control writes here.
    trace_rate_bits: Arc<AtomicU64>,
    /// Bounded retirement queue handle for traced ops.
    /// `None` when the trace log file couldn't be opened at
    /// adapter init — dispenser still respects the rate
    /// (sets the tracing flag on the statement) but skips the
    /// post-execute submit.
    trace_log: Option<TraceLog>,
}

impl CqlPreparedDispenser {
    async fn get_prepared(&self) -> Result<Arc<cass::PreparedStatement>, ExecutionError> {
        if let Some(p) = self.prepared.get() {
            return Ok(p.clone());
        }
        let prepared = self.session.get().prepare(&self.stmt_text).await
            .map_err(|e| ExecutionError::Op(AdapterError {
                error_name: "prepare_error".into(),
                message: format!("prepare '{}': {e}", self.stmt_text),
                retryable: false,
            }))?;
        let arc = Arc::new(prepared);
        let _ = self.prepared.set(arc.clone());
        Ok(self.prepared.get().unwrap().clone())
    }
}

impl OpDispenser for CqlPreparedDispenser {
    fn describe(&self) -> Option<String> {
        Some(format!("CQL prepared: {}", flatten_one_line(&self.stmt_text)))
    }

    fn canonical_kernel(&self) -> Option<&std::sync::Arc<nbrs_activity::adapter::GkKernel>> {
        Some(&self.canonical_kernel)
    }

    fn describe_resolved(&self, wires: &dyn nbrs_activity::wires::WireSource) -> Option<String> {
        // SRD-68 Push 5: walk the prepared text and replace each
        // `?` placeholder with the bound name's value pulled
        // through the dispenser's wires surface. The output is
        // not a literal replayable SQL statement (vector / blob
        // literals need adapter-side encoding), but it's an
        // honest representation of what the bind step received
        // for this cycle. String-typed values get single-quoted
        // so operators can spot quoting / escape issues at a
        // glance.
        let mut out = String::with_capacity(self.stmt_text.len() + 64);
        let mut bind_idx = 0usize;
        for ch in self.stmt_text.chars() {
            if ch == '?' {
                if let Some(name) = self.bind_names.get(bind_idx) {
                    let rendered = match wires.get(name) {
                        Some(nbrs_variates::node::Value::Str(s)) => format!("'{s}'"),
                        Some(v) => v.to_display_string(),
                        None => format!("{{?{name}}}"),
                    };
                    out.push_str(&rendered);
                } else {
                    out.push('?');
                }
                bind_idx += 1;
            } else {
                out.push(ch);
            }
        }
        Some(format!("CQL prepared: {}", flatten_one_line(&out)))
    }


    fn execute<'a>(
        &'a self,
        cycle: u64,
        ctx: &'a nbrs_activity::adapter::ExecCtx<'a>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<OpResult, ExecutionError>> + Send + 'a>> {
        let wires = ctx.wires;
        Box::pin(async move {
            let prepared = self.get_prepared().await?;

            // Build type-aware binders once from prepared statement metadata.
            // Cached in OnceLock — lock-free after first execute.
            let binders = self.binders.get_or_init(|| {
                (0..self.bind_names.len())
                    .map(|i| {
                        let dt = prepared.parameter_data_type(i);
                        let vt = get_const_data_type_value_type(&dt);
                        make_binder(vt)
                    })
                    .collect()
            });

            let mut stmt = prepared.bind();
            let _ = stmt.set_consistency(self.consistency)
                .map_err(|e| ExecutionError::Op(AdapterError {
                    error_name: "bind_error".into(),
                    message: format!("set consistency: {e}"),
                    retryable: false,
                }))?;

            // SRD-68 Push 5b: cycle-time `?`-parameter binding
            // through the dispenser's per-fiber GK kernel via the
            // narrow `WireSource` trait. `wires.get(bind_name)`
            // returns the typed `Value` for the position's bind
            // point — same name resolution surface the raw mode
            // uses, no adapter-specific fields path.
            for (bind_idx, name) in self.bind_names.iter().enumerate() {
                if let Some(value) = wires.get(name) {
                    binders[bind_idx](&mut stmt, bind_idx, &value)
                        .map_err(|e| ExecutionError::Op(AdapterError {
                            error_name: "bind_error".into(),
                            message: format!("bind position {bind_idx} ('{name}'): {e}"),
                            retryable: false,
                        }))?;
                }
            }

            // Sparse-tracing decision per execute. Atomic load is
            // cheap (single 64-bit read); the RNG roll only fires
            // when the rate is non-zero, so the no-tracing hot
            // path stays effectively free.
            let trace_rate = f64::from_bits(self.trace_rate_bits.load(Ordering::Acquire));
            let trace_this = trace_rate > 0.0
                && rand::random::<f64>() < trace_rate;
            if trace_this {
                let _ = stmt.set_tracing(true);
            }

            // Capture metadata for the trace log before consuming
            // the statement. `started_at` is the wall-clock for
            // the `system_traces.sessions` time-window correlation;
            // `started` is the monotonic clock for latency.
            let started_at = std::time::SystemTime::now();
            let started = std::time::Instant::now();

            // Two execute paths so the no-trace hot path stays
            // exactly the existing shape. The `execute_with_tracing`
            // route is only taken when we actually want the
            // server-side trace UUID — it's the vendored
            // cassandra-cpp surface that pairs result with
            // `cass_future_tracing_id`.
            let exec_outcome = if trace_this {
                self.session.get()
                    .execute_with_tracing(&stmt)
                    .await
                    .map(|(r, tid)| (r, tid))
            } else {
                stmt.execute().await.map(|r| (r, None))
            };

            let latency_nanos = started.elapsed().as_nanos() as u64;

            let exec_result = match exec_outcome {
                Ok((result, trace_id)) => {
                    if trace_this {
                        if let Some(log) = self.trace_log.as_ref() {
                            let binds = self.bind_names.iter()
                                .map(|name| match wires.get(name) {
                                    Some(v) => tracing::format_bind_value(name, &v),
                                    None => format!("{name}=<missing>"),
                                })
                                .collect();
                            log.submit(TraceRecord {
                                cycle,
                                started_at,
                                query: self.stmt_text.clone(),
                                binds,
                                latency_nanos,
                                ok: true,
                                error_name: None,
                                trace_id: trace_id.map(|u| {
                                    let std_uuid: uuid::Uuid = u.into();
                                    std_uuid.to_string()
                                }),
                            });
                        }
                    }
                    Ok(result)
                }
                Err(e) => {
                    let truncated = if self.stmt_text.len() > 200 {
                        format!("{}...", &self.stmt_text[..200])
                    } else {
                        self.stmt_text.clone()
                    };
                    if trace_this
                        && let Some(log) = self.trace_log.as_ref()
                    {
                        let binds = self.bind_names.iter()
                            .map(|name| match wires.get(name) {
                                Some(v) => tracing::format_bind_value(name, &v),
                                None => format!("{name}=<missing>"),
                            })
                            .collect();
                        log.submit(TraceRecord {
                            cycle,
                            started_at,
                            query: self.stmt_text.clone(),
                            binds,
                            latency_nanos,
                            ok: false,
                            error_name: Some("cql_error".into()),
                            trace_id: None,
                        });
                    }
                    Err(ExecutionError::Op(AdapterError {
                        error_name: "cql_error".into(),
                        message: format!("{e}\n  statement: {truncated}"),
                        retryable: false,
                    }))
                }
            };

            let result = exec_result?;

            let body = if result.row_count() > 0 {
                Some(Box::new(CqlResultBody::from_cass_result(&result)) as Box<dyn ResultBody>)
            } else {
                None
            };
            Ok(OpResult {
                body,
                skipped: false,
            })
        })
    }
}

// =========================================================================
// CqlBatchDispenser: groups multiple bound statements into one CQL BATCH
// =========================================================================

/// Wraps a prepared statement template and executes batches of bound
/// statements as one CQL BATCH call.
///
/// The executor calls `execute_batch()` with N resolved field sets
/// (one per cycle in the batch). Each is bound to the prepared
/// statement and added to a `cass::Batch`. The batch is executed
/// once. Per-cycle latency is meaningless — only batch latency matters.
struct CqlBatchDispenser {
    session: SessionHandle,
    consistency: cass::Consistency,
    stmt_text: String,
    /// The op field name carrying the statement (for finding it in resolved fields).
    #[allow(dead_code)]
    stmt_field: String,
    bind_names: Vec<String>,
    /// SRD-68 invariant I-3: dispenser-owned canonical GK kernel.
    /// See `CqlRawDispenser::canonical_kernel`.
    #[allow(dead_code)]
    canonical_kernel: std::sync::Arc<nbrs_variates::kernel::GkKernel>,
    /// Batch row count from `batch: N` op param. Per the SRD-68
    /// invariant "batch is an iteration container, each row is
    /// another pull," the dispenser internally advances the
    /// kernel coord N times per fiber-cycle, calling
    /// `wires.get(bind_name)` for each row's typed values.
    batch_size: usize,
    /// Prepared once on first execute, then lock-free reads thereafter.
    prepared: std::sync::OnceLock<Arc<cass::PreparedStatement>>,
    batch_type: cass::BatchType,
    /// Per-row timer: records amortized latency (batch_nanos / row_count)
    /// for each row in the batch. Enables rows/s throughput in the summary.
    rows_timer: nbrs_metrics::instruments::timer::Timer,
    /// Cumulative row counter for the status line. Not reset on snapshot.
    rows_total: std::sync::atomic::AtomicU64,
    /// Live tracing probability (f64 bits). Loaded once per
    /// batch execute; `cql_trace_rate` control writes here.
    /// Sparse means "trace this batch invocation" — we roll
    /// once for the whole batch, not per row.
    trace_rate_bits: Arc<AtomicU64>,
    /// Bounded retirement queue handle for traced batches.
    /// `None` when the trace log file couldn't be opened at
    /// adapter init — dispenser still respects the rate
    /// (sets the tracing flag on each statement) but skips
    /// the post-execute submit.
    trace_log: Option<TraceLog>,
}

impl CqlBatchDispenser {
    /// Get or prepare the statement. Lock-free after first call.
    /// Multiple fibers may race to prepare on first execute — the
    /// OnceLock ensures only one result is stored; the CQL driver
    /// handles duplicate prepares gracefully.
    async fn get_prepared(&self) -> Result<Arc<cass::PreparedStatement>, ExecutionError> {
        if let Some(p) = self.prepared.get() {
            return Ok(p.clone());
        }
        let prepared = self.session.get().prepare(&self.stmt_text).await
            .map_err(|e| ExecutionError::Op(AdapterError {
                error_name: "prepare_error".into(),
                message: format!("prepare '{}': {e}", self.stmt_text),
                retryable: false,
            }))?;
        let arc = Arc::new(prepared);
        // First to finish wins; others' results are harmlessly dropped.
        let _ = self.prepared.set(arc.clone());
        Ok(self.prepared.get().unwrap().clone())
    }
}

impl OpDispenser for CqlBatchDispenser {
    fn describe(&self) -> Option<String> {
        Some(format!("CQL batch: {}", flatten_one_line(&self.stmt_text)))
    }

    fn canonical_kernel(&self) -> Option<&std::sync::Arc<nbrs_activity::adapter::GkKernel>> {
        Some(&self.canonical_kernel)
    }

    fn describe_resolved(&self, wires: &dyn nbrs_activity::wires::WireSource) -> Option<String> {
        // SRD-68 Push 5: render the head row by pulling each
        // bind name through the wires surface. The footer reports
        // the configured batch size so the operator sees how many
        // rows the failing batch was sized for. Wires reflect the
        // current per-fiber coord; for diagnostic intent that is
        // close enough — the row in question is whatever was last
        // active, and the operator's interest is whether the bind
        // values look right at all.
        let mut out = String::with_capacity(self.stmt_text.len() + 64);
        let mut bind_idx = 0usize;
        for ch in self.stmt_text.chars() {
            if ch == '?' {
                if let Some(name) = self.bind_names.get(bind_idx) {
                    let rendered = match wires.get(name) {
                        Some(nbrs_variates::node::Value::Str(s)) => format!("'{s}'"),
                        Some(v) => v.to_display_string(),
                        None => format!("{{?{name}}}"),
                    };
                    out.push_str(&rendered);
                } else {
                    out.push('?');
                }
                bind_idx += 1;
            } else {
                out.push(ch);
            }
        }
        let suffix = if self.batch_size > 1 {
            format!("  -- batch_size={}", self.batch_size)
        } else {
            String::new()
        };
        Some(format!("CQL batch: {}{}", flatten_one_line(&out), suffix))
    }


    fn status_counters(&self) -> Vec<(&str, u64)> {
        let total = self.rows_total.load(std::sync::atomic::Ordering::Relaxed);
        if total == 0 { return Vec::new(); }
        vec![("rows_inserted", total)]
    }

    fn adapter_metrics(&self) -> Vec<(String, nbrs_metrics::labels::Labels, nbrs_metrics::snapshot::MetricValue)> {
        use nbrs_metrics::snapshot::{CounterValue, HistogramValue, MetricValue, split_name_label};
        let snap = self.rows_timer.snapshot();
        let total = self.rows_total.load(std::sync::atomic::Ordering::Relaxed);
        let mut out = Vec::new();
        if snap.count > 0 {
            let (name, labels) = split_name_label(self.rows_timer.labels());
            out.push((
                name,
                labels,
                MetricValue::Histogram(HistogramValue::from_hdr(snap.histogram)),
            ));
        }
        if total > 0 {
            out.push((
                "rows_inserted_total".to_string(),
                nbrs_metrics::labels::Labels::default(),
                MetricValue::Counter(CounterValue::new(total)),
            ));
        }
        out
    }

    fn execute<'a>(
        &'a self,
        cycle: u64,
        ctx: &'a nbrs_activity::adapter::ExecCtx<'a>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<OpResult, ExecutionError>> + Send + 'a>> {
        let wires = ctx.wires;
        Box::pin(async move {
            let prepared = self.get_prepared().await?;
            let mut batch = self.session.get().batch(self.batch_type);

            // Sparse-tracing decision once for the whole batch
            // (not per row). Atomic load is cheap; the RNG roll
            // only fires when the rate is non-zero, so the
            // no-tracing hot path stays effectively free.
            let trace_rate = f64::from_bits(self.trace_rate_bits.load(Ordering::Acquire));
            let trace_this = trace_rate > 0.0
                && rand::random::<f64>() < trace_rate;

            // Per-position type-aware binders, built once from
            // prepared statement metadata. Each binder coerces
            // a GK `Value` to the CQL column type for its `?`
            // position.
            let binders: Vec<Box<dyn Fn(&mut cass::Statement, usize, &nbrs_variates::node::Value)
                -> Result<(), cass::Error> + Send + Sync>> =
                (0..self.bind_names.len()).map(|i| {
                    let dt = prepared.parameter_data_type(i);
                    let vt = get_const_data_type_value_type(&dt);
                    make_binder(vt)
                }).collect();

            // SRD-68 Push 5b' batch contract: "each iteration of
            // the batch is considered another pull, just as if
            // the operation inside the batch were separate. It
            // is simply an iteration container." Drive the
            // `batch_size` rows by advancing the per-fiber
            // kernel coord via `wires.advance(coord)` and
            // pulling each bind name via `wires.get(name)` for
            // the row's typed values. Same single resolution
            // surface (per SRD-68 invariant I-1) the prepared
            // single-cycle path uses; no parallel fields/pulls
            // path.
            for row_idx in 0..self.batch_size {
                let row_coord = cycle + row_idx as u64;
                wires.advance(row_coord);
                let mut stmt = prepared.bind();
                let _ = stmt.set_consistency(self.consistency)
                    .map_err(|e| ExecutionError::Op(AdapterError {
                        error_name: "bind_error".into(),
                        message: format!("set consistency: {e}"),
                        retryable: false,
                    }))?;
                if trace_this {
                    let _ = stmt.set_tracing(true);
                }
                for (idx, name) in self.bind_names.iter().enumerate() {
                    if let Some(value) = wires.get(name) {
                        binders[idx](&mut stmt, idx, &value)
                            .map_err(|e| ExecutionError::Op(AdapterError {
                                error_name: "bind_error".into(),
                                message: format!(
                                    "bind position {idx} ('{name}') row {row_idx}: {e}"
                                ),
                                retryable: false,
                            }))?;
                    }
                }
                batch.add_statement(stmt)
                    .map_err(|e| ExecutionError::Op(AdapterError {
                        error_name: "batch_error".into(),
                        message: format!("add_statement (row {row_idx}): {e}"),
                        retryable: false,
                    }))?;
            }
            let row_count = self.batch_size;

            // Capture metadata for the trace log before dispatch.
            // `started_at` is the wall-clock for the
            // `system_traces.sessions` time-window correlation;
            // `batch_start` is the monotonic clock used both for
            // the rows_timer accounting and the trace log
            // latency_nanos.
            let started_at = std::time::SystemTime::now();
            let batch_start = std::time::Instant::now();

            // Two execute paths so the no-trace hot path stays
            // exactly the existing shape. Traced batches go
            // through the vendored `execute_batch_with_tracing`
            // which pairs result with `cass_future_tracing_id`.
            let exec_outcome = if trace_this {
                self.session.get()
                    .execute_batch_with_tracing(&batch)
                    .await
                    .map(|(r, tid)| (r, tid))
            } else {
                self.session.get()
                    .execute_batch(&batch)
                    .await
                    .map(|r| (r, None))
            };

            let batch_nanos = batch_start.elapsed().as_nanos() as u64;

            match exec_outcome {
                Ok((_result, trace_id)) => {
                    if trace_this
                        && let Some(log) = self.trace_log.as_ref()
                    {
                        log.submit(TraceRecord {
                            // First-row cycle of the batch — the
                            // dispenser's `cycle` arg points at
                            // it.
                            cycle,
                            started_at,
                            query: self.stmt_text.clone(),
                            // Batches don't render every row's
                            // binds (could be thousands). One
                            // synthetic entry summarises the
                            // batch dispatch.
                            binds: vec![format!("batch of {} rows", row_count)],
                            latency_nanos: batch_nanos,
                            ok: true,
                            error_name: None,
                            trace_id: trace_id.map(|u| {
                                let std_uuid: uuid::Uuid = u.into();
                                std_uuid.to_string()
                            }),
                        });
                    }
                }
                Err(e) => {
                    if trace_this
                        && let Some(log) = self.trace_log.as_ref()
                    {
                        log.submit(TraceRecord {
                            cycle,
                            started_at,
                            query: self.stmt_text.clone(),
                            binds: vec![format!("batch of {} rows", row_count)],
                            latency_nanos: batch_nanos,
                            ok: false,
                            error_name: Some("batch_error".into()),
                            trace_id: None,
                        });
                    }
                    return Err(ExecutionError::Op(AdapterError {
                        error_name: "batch_error".into(),
                        message: format!("execute_batch ({row_count} statements): {e}"),
                        retryable: false,
                    }));
                }
            }

            let per_row_nanos = batch_nanos / row_count.max(1) as u64;
            for _ in 0..row_count {
                self.rows_timer.record(per_row_nanos);
            }
            self.rows_total.fetch_add(row_count as u64, std::sync::atomic::Ordering::Relaxed);

            // `rows_inserted` lands on the per-fiber kernel via
            // ctx.wires.write — wrappers above this layer see it
            // through wires.get on the same cycle.
            let _ = ctx.wires.write(
                "rows_inserted",
                nbrs_variates::node::Value::U64(row_count as u64),
            );
            Ok(OpResult {
                body: None,
                skipped: false,
            })
        })
    }
}

// =========================================================================
// CqlTimeuuid GK node + its inventory registration moved to
// `crate::common::nodes`. Every CQL persona that links this
// adapter gets the node for free regardless of which engine
// feature is enabled.
// =========================================================================

// =========================================================================
// Adapter Registration (inventory-based, link-time)
// =========================================================================

// Register `cassandra-cpp` as a driver implementation of the
// `cql` adapter. `cassandra-cpp` is the internal driver name —
// `adapter=cql` is the user-facing knob; `cqldriver=cassandra-cpp`
// selects this driver from inside that adapter.
//
// Lower rank wins; cassandra-cpp ranks 100 so binaries that
// link both drivers default to cassandra-cpp ahead of scylla
// (200).
inventory::submit! {
    nbrs_activity::adapter::DriverImpl {
        adapter: "cql",
        driver: "cassandra-cpp",
        default_rank: 100,
        create: |params| Box::pin(async move {
            // Set the cpp-driver log threshold *before* any
            // cass_cluster_* call — the driver only honors the
            // level set prior to first session construction.
            apply_log_level_once(&params)?;
            let config = CqlConfig::from_params(&params)
                .map_err(|e| format!("CQL config error: {e}"))?;
            CqlAdapter::connect(&config).await
                .map(|a| std::sync::Arc::new(a) as std::sync::Arc<dyn nbrs_activity::adapter::DriverAdapter>)
                .map_err(|e| format!("CQL connection failed: {e}"))
        }),
        known_params: || &[
            "hosts", "host", "port", "keyspace", "connect_keyspace", "consistency",
            "username", "password", "request_timeout_ms",
            "cassandra_log_level",
            "trace_rate", "trace_log",
        ],
    }
}

// SRD-35 Push B: declare the cassandra-cpp engine as
// pool-shareable. Phases whose params produce equal
// `CqlConfig::to_resource_key("cassandra-cpp")` keys share
// a single `CqlAdapter` (and therefore a single
// `cass::Session`) for the whole workload — directly
// fixing the per-phase open/close storm that motivates
// SRD-35.
inventory::submit! {
    nbrs_activity::adapter::SharedDriverRegistration {
        adapter: "cql",
        driver: "cassandra-cpp",
        share_capability: nbrs_activity::resource_pool::ShareCapability::Shared,
        resource_key: |params| {
            let cfg = crate::common::CqlConfig::from_params(params)
                .map_err(|e| format!("CQL config error: {e}"))?;
            Ok(cfg.to_resource_key("cassandra-cpp"))
        },
    }
}

// =========================================================================
// Type-aware value binders
// =========================================================================

/// Extract the CQL ValueType from a ConstDataType.
///
/// The safe cassandra-cpp wrapper doesn't expose get_type() on
/// ConstDataType, so we call the C FFI directly. ConstDataType
/// is a newtype over `*const CassDataType` (with PhantomData).
fn get_const_data_type_value_type<T>(dt: &T) -> cass::ValueType {
    // ConstDataType layout: (*const _CassDataType, PhantomData)
    // We read the first pointer-sized field.
    let raw: *const cassandra_cpp_sys::CassDataType_ = unsafe {
        *(dt as *const _ as *const *const cassandra_cpp_sys::CassDataType_)
    };
    let cass_vt = unsafe { cassandra_cpp_sys::cass_data_type_type(raw) };
    // Map the C enum value to the Rust ValueType.
    // CassValueType_ values match ValueType variant ordering.
    cass_value_type_from_raw(cass_vt)
}

/// Convert a raw CassValueType_ C enum to cass::ValueType.
fn cass_value_type_from_raw(raw: cassandra_cpp_sys::CassValueType_) -> cass::ValueType {
    use cassandra_cpp_sys::CassValueType_::*;
    match raw {
        CASS_VALUE_TYPE_ASCII => cass::ValueType::ASCII,
        CASS_VALUE_TYPE_BIGINT => cass::ValueType::BIGINT,
        CASS_VALUE_TYPE_BLOB => cass::ValueType::BLOB,
        CASS_VALUE_TYPE_BOOLEAN => cass::ValueType::BOOLEAN,
        CASS_VALUE_TYPE_COUNTER => cass::ValueType::COUNTER,
        CASS_VALUE_TYPE_DOUBLE => cass::ValueType::DOUBLE,
        CASS_VALUE_TYPE_FLOAT => cass::ValueType::FLOAT,
        CASS_VALUE_TYPE_INT => cass::ValueType::INT,
        CASS_VALUE_TYPE_TEXT => cass::ValueType::TEXT,
        CASS_VALUE_TYPE_VARCHAR => cass::ValueType::VARCHAR,
        CASS_VALUE_TYPE_SMALL_INT => cass::ValueType::SMALL_INT,
        CASS_VALUE_TYPE_TINY_INT => cass::ValueType::TINY_INT,
        CASS_VALUE_TYPE_CUSTOM => cass::ValueType::CUSTOM,
        _ => cass::ValueType::UNKNOWN,
    }
}

/// Create a binder function for a given CQL column type.
///
/// The returned closure converts a GK `Value` to the correct CQL
/// type and binds it at the given position. Built once per `?`
/// position in a prepared statement; applied per row.
/// Parse a GK vector string `[0.1, 0.2, ...]` into CQL vector
/// binary encoding (big-endian IEEE 754 floats, concatenated).
fn parse_vector_to_bytes(s: &str) -> Vec<u8> {
    let trimmed = s.trim();
    if !trimmed.starts_with('[') || !trimmed.ends_with(']') {
        return Vec::new();
    }
    let inner = &trimmed[1..trimmed.len()-1];
    let mut bytes = Vec::new();
    for part in inner.split(',') {
        let part = part.trim();
        if let Ok(f) = part.parse::<f32>() {
            bytes.extend_from_slice(&f.to_be_bytes());
        } else {
            return Vec::new(); // not a float vector
        }
    }
    bytes
}

/// Convert LE f32 bytes (GK native) to BE f32 bytes (CQL vector encoding).
///
/// Swaps each 4-byte group from little-endian to big-endian in place.
/// If the length is not a multiple of 4, trailing bytes are passed through.
fn le_to_be_f32_bytes(le: &[u8]) -> Vec<u8> {
    let mut be = Vec::with_capacity(le.len());
    for chunk in le.chunks(4) {
        if chunk.len() == 4 {
            // Reinterpret as LE f32, emit as BE f32
            be.extend_from_slice(&[chunk[3], chunk[2], chunk[1], chunk[0]]);
        } else {
            be.extend_from_slice(chunk);
        }
    }
    be
}

type BinderFn = Box<dyn Fn(&mut cass::Statement, usize, &nbrs_variates::node::Value)
    -> cass::Result<()> + Send + Sync>;

fn make_binder(cql_type: cass::ValueType) -> BinderFn {
    match cql_type {
        // String types
        cass::ValueType::ASCII | cass::ValueType::TEXT | cass::ValueType::VARCHAR => {
            Box::new(|stmt, idx, value| {
                stmt.bind_string(idx, &value.to_display_string())?; Ok(())
            })
        }
        // 32-bit integer types
        cass::ValueType::INT | cass::ValueType::SMALL_INT | cass::ValueType::TINY_INT => {
            Box::new(|stmt, idx, value| {
                let n = match value {
                    nbrs_variates::node::Value::U64(v) => *v as i32,
                    nbrs_variates::node::Value::F64(v) => *v as i32,
                    nbrs_variates::node::Value::Str(s) => s.parse::<i32>().unwrap_or(0),
                    _ => 0,
                };
                stmt.bind_int32(idx, n)?; Ok(())
            })
        }
        // 64-bit integer types
        cass::ValueType::BIGINT | cass::ValueType::COUNTER => {
            Box::new(|stmt, idx, value| {
                let n = match value {
                    nbrs_variates::node::Value::U64(v) => *v as i64,
                    nbrs_variates::node::Value::F64(v) => *v as i64,
                    nbrs_variates::node::Value::Str(s) => s.parse::<i64>().unwrap_or(0),
                    _ => 0,
                };
                stmt.bind_int64(idx, n)?; Ok(())
            })
        }
        // Float
        cass::ValueType::FLOAT => {
            Box::new(|stmt, idx, value| {
                let f = match value {
                    nbrs_variates::node::Value::F64(v) => *v as f32,
                    nbrs_variates::node::Value::U64(v) => *v as f32,
                    nbrs_variates::node::Value::Str(s) => s.parse::<f32>().unwrap_or(0.0),
                    _ => 0.0,
                };
                stmt.bind_float(idx, f)?; Ok(())
            })
        }
        // Double
        cass::ValueType::DOUBLE => {
            Box::new(|stmt, idx, value| {
                let f = match value {
                    nbrs_variates::node::Value::F64(v) => *v,
                    nbrs_variates::node::Value::U64(v) => *v as f64,
                    nbrs_variates::node::Value::Str(s) => s.parse::<f64>().unwrap_or(0.0),
                    _ => 0.0,
                };
                stmt.bind_double(idx, f)?; Ok(())
            })
        }
        // Boolean
        cass::ValueType::BOOLEAN => {
            Box::new(|stmt, idx, value| {
                let b = match value {
                    nbrs_variates::node::Value::Bool(v) => *v,
                    nbrs_variates::node::Value::U64(v) => *v != 0,
                    nbrs_variates::node::Value::Str(s) => s == "true" || s == "1",
                    _ => false,
                };
                stmt.bind_bool(idx, b)?; Ok(())
            })
        }
        // CUSTOM type includes CQL vectors. Two paths:
        // 1. Value::Bytes — direct bind (optimal: no string round-trip).
        //    GK `_bytes` nodes produce LE f32; CQL vectors are BE f32.
        //    Swap each 4-byte group from LE to BE before binding.
        // 2. Value::Str — parse `[0.1, 0.2, ...]` into BE f32 bytes.
        cass::ValueType::CUSTOM => {
            Box::new(|stmt, idx, value| {
                match value {
                    nbrs_variates::node::Value::Bytes(le_bytes) => {
                        // LE f32 from GK → BE f32 for CQL
                        let be_bytes = le_to_be_f32_bytes(le_bytes);
                        stmt.bind_bytes(idx, be_bytes)?;
                    }
                    _ => {
                        let s = value.to_display_string();
                        let bytes = parse_vector_to_bytes(&s);
                        if bytes.is_empty() {
                            stmt.bind_string(idx, &s)?;
                        } else {
                            stmt.bind_bytes(idx, bytes)?;
                        }
                    }
                }
                Ok(())
            })
        }
        // BLOB: raw bytes binding
        cass::ValueType::BLOB => {
            Box::new(|stmt, idx, value| {
                match value {
                    nbrs_variates::node::Value::Bytes(bytes) => {
                        stmt.bind_bytes(idx, bytes.clone())?;
                    }
                    _ => {
                        stmt.bind_string(idx, &value.to_display_string())?;
                    }
                }
                Ok(())
            })
        }
        // Everything else: bind as string
        _ => {
            Box::new(|stmt, idx, value| {
                stmt.bind_string(idx, &value.to_display_string())?; Ok(())
            })
        }
    }
}

#[cfg(test)]
mod connect_diag_tests {
    use super::{enrich_connect_error, process_resource_snapshot};

    #[test]
    fn unrelated_errors_pass_through_unchanged() {
        // Auth, network, syntax — anything that isn't a libuv
        // init failure — must NOT trigger the resource-limit
        // diagnostic. The user shouldn't be told "check your
        // ulimit" when the password was wrong.
        let out = enrich_connect_error("connect", "Bad credentials".into());
        assert_eq!(out, "connect: Bad credentials");
        assert!(!out.contains("RLIMIT"), "no resource diag expected, got: {out}");
        assert!(!out.contains("nofile_soft"), "no resource diag expected, got: {out}");
    }

    #[test]
    fn lib_unable_to_init_attaches_resource_snapshot() {
        // The exact error string the user pasted in the bug
        // report — confirms the contains-match catches it and
        // appends the actionable section.
        let raw = "Cassandra error LIB_UNABLE_TO_INIT: \
                   Unable to initialize cluster event loop";
        let out = enrich_connect_error("connect to keyspace 'baselines'", raw.into());
        assert!(out.contains("LIB_UNABLE_TO_INIT"), "raw error preserved");
        assert!(out.contains("per-process resource exhaustion"),
            "diagnostic explanation present");
        assert!(out.contains("Process resource snapshot:"),
            "snapshot section present");
        assert!(out.contains("fds_in_use:"), "FD count line present");
        assert!(out.contains("nofile_soft:") && out.contains("nofile_hard:"),
            "FD limit lines present");
        assert!(out.contains("ulimit -n"),
            "remediation hint present");
    }

    #[test]
    fn snapshot_renders_numeric_values_on_linux() {
        // On Linux (the typical CI / test environment) the
        // /proc reads succeed and we should see real numbers
        // rather than `?` placeholders. Skip the assertion if
        // /proc isn't available (sandboxed CI, non-Linux dev
        // host) — the function must still return *something*.
        let snap = process_resource_snapshot();
        assert!(snap.contains("fds_in_use:"));
        if std::path::Path::new("/proc/self/fd").exists() {
            assert!(!snap.contains("fds_in_use:    ?"),
                "/proc/self/fd should yield a numeric count on Linux, got: {snap}");
        }
    }
}
