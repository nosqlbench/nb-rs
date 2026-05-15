// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Session: the root context for a workload run.
//!
//! A session has a human-readable ID, a directory for all diagnostic
//! artifacts (metrics, logs, flamegraphs), and is the root of the
//! component tree for metrics labeling.
//!
//! Session ID format: `{scenario}_{YYYYMMDD_HHmmss}`
//! Session directory: `logs/{session_id}/`
//!
//! All files from a run live under the session directory:
//! - `metrics.db` — SQLite metrics
//! - `flamegraph.svg` — profiler output
//! - `session.log` — diagnostic log (future)

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};

use nbrs_metrics::component::Component;
use nbrs_metrics::labels::Labels;
use nbrs_metrics::metrics_query::MetricsQuery;

/// A workload run session.
///
/// The session is the root of the component tree and — once the
/// runner has installed one — holds the shared [`MetricsQuery`] that
/// every in-process reader (TUI, summary, GK metric nodes) reads
/// through. See SRD-42 §"MetricsQuery — the unified read interface".
pub struct Session {
    /// Human-readable session identifier.
    pub id: String,
    /// Output directory for diagnostic artifacts (metrics, logs, flamegraphs).
    /// Located at `logs/{session_id}/`. Not the working directory.
    pub output_dir: PathBuf,
    /// Workload file path (for metadata).
    pub workload: String,
    /// Scenario name.
    pub scenario: String,
    /// Session root component (owns the component tree for metrics labeling).
    pub component: Arc<RwLock<Component>>,
    /// Shared `MetricsQuery` handle — installed by the runner once the
    /// cadence reporter is built. `None` before the runner wires it.
    pub metrics_query: Mutex<Option<Arc<MetricsQuery>>>,
}

/// Translate a CLI flag name (`--session-path`) into its
/// canonical `NBRS_`-prefixed env-var name (`NBRS_SESSION_PATH`).
/// Per SRD-04, every CLI flag automatically has an env-var
/// equivalent following this convention.
pub fn flag_env_name(flag: &str) -> String {
    let stem = flag.trim_start_matches("--");
    format!("NBRS_{}", stem.replace('-', "_").to_ascii_uppercase())
}

/// Resolve a flag value from CLI args + its `NBRS_`-prefixed
/// env var. Returns `None` if neither is set. **Exits with
/// status 2** if BOTH are set — configuration conflict; we
/// refuse to silently disambiguate.
///
/// Per SRD-04 the env-var name is automatically derived from
/// the flag name (`--foo-bar` → `NBRS_FOO_BAR`).
pub fn resolve_flag(args: &[String], flag: &str) -> Option<String> {
    let cli = {
        let eq_prefix = format!("{flag}=");
        let mut iter = args.iter();
        let mut found = None;
        while let Some(arg) = iter.next() {
            if let Some(rest) = arg.strip_prefix(&eq_prefix) {
                found = Some(rest.to_string());
                break;
            }
            if arg == flag {
                found = iter.next().cloned();
                break;
            }
        }
        found
    };
    let env_name = flag_env_name(flag);
    let env = std::env::var(&env_name).ok().filter(|v| !v.trim().is_empty());
    match (cli, env) {
        (Some(_), Some(_)) => {
            eprintln!(
                "error: configuration conflict — both `{flag}` (CLI) and \
                 `{env_name}` (env) are set. Pick one. Per SRD-04, every \
                 CLI flag has an env equivalent prefixed with `NBRS_`; \
                 specifying both at once is a hard error so the operator \
                 sees their inputs are fighting."
            );
            std::process::exit(2);
        }
        (Some(v), None) | (None, Some(v)) => Some(v),
        (None, None) => None,
    }
}

/// Classify a session-path value: does it look like a
/// `key=value` workload-param token (e.g. `scenario=foo`)
/// rather than a real filesystem path? The umbrella
/// `--session <kv>` parser splits only on `:`, so a
/// `=`-shaped token slipping into the path slot would silently
/// materialise directories like `<cwd>/scenario=foo/...`.
///
/// Heuristic: the head of `head=tail` matches the workload-
/// param ABNF (alphanumeric / underscore / hyphen, no slash).
/// A real path like `/var/tmp/k=v` keeps a leading slash in
/// the head and passes through unchanged. Leading `./` or
/// `../` likewise excludes the head from the param-shape
/// check (the head contains a `/`).
///
/// Returns `Err(<error message>)` when the value should be
/// rejected; `Ok(())` when it's safe to use as a session
/// path.
pub fn check_session_path(p: &str, source: &str) -> Result<(), String> {
    if let Some((head, _)) = p.split_once('=') {
        let head_looks_like_param = !head.is_empty()
            && head.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '-')
            && !head.contains('/');
        if head_looks_like_param {
            return Err(format!(
                "session path from {source} is '{p}' — that looks \
                 like a `key=value` workload param, not a path. The umbrella \
                 `--session <kv>` parser splits only on `:`, so this would \
                 silently create a `<cwd>/{p}/…` directory tree. \
                 Did you mean: `--session-path <path>` (with `{p}` as a \
                 separate workload arg), or `--session path:<path>` (umbrella \
                 form, `:` as the kv separator)?"
            ));
        }
    }
    Ok(())
}

/// Wrapper around [`check_session_path`] that prints to
/// stderr and exits with code 2 on rejection. Used at every
/// entry point that sets `session_path` from CLI / env input:
/// [`parse_session_kv`] (bare-token branch and `path:`/`dir:`
/// keys), [`resolve_session_dir`] for `--session-path`, and
/// the legacy `SESSION_DIRECTORY` env fallback. Same exit-
/// shape as the existing `--session` configuration-conflict
/// error in [`resolve_flag`].
pub(crate) fn validate_session_path_or_exit(p: &str, source: &str) {
    if let Err(msg) = check_session_path(p, source) {
        eprintln!("error: {msg}");
        std::process::exit(2);
    }
}

/// Legacy env-var name for `--session-path`. Pre-SRD-04
/// shipping name that some operators may have in their shell
/// config; honored as a deprecated fallback below
/// `NBRS_SESSION_PATH` and warns when read.
pub const SESSION_DIRECTORY_ENV: &str = "SESSION_DIRECTORY";

/// Token within a session-dir path that's replaced with the
/// auto-generated session id at write time. Lets users template
/// per-run directories without changing the path between runs:
/// `--session-dir /data/sessions/SESSION_run` →
/// `/data/sessions/default_20260101_120000_run`.
pub const SESSION_TOKEN: &str = "SESSION";

/// Policy when a session about to be created lands on an
/// existing, non-empty session directory.
///
/// Defaults to `Error` so accidental reuse can never silently
/// destroy a prior session's artifacts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SessionReuse {
    /// Refuse to start. Exit with a clear message naming the
    /// existing path and the available reuse-mode flags.
    /// Default when `--session-reuse` is not specified.
    #[default]
    Error,
    /// Wipe the existing artifacts (`metrics.db`, `session.log`,
    /// `checkpoint.jsonl`, `summary.md`, etc.) and start fresh.
    /// The dir itself stays; only its contents are cleared.
    Restart,
    /// Continue with the existing session — equivalent to
    /// `--resume <session>` against this directory. Surfaces a
    /// hint if the operator probably wanted Restart.
    Resume,
}

impl SessionReuse {
    /// Parse from a string value (CLI flag or env var).
    /// Accepts `error` / `restart` / `resume`, case-insensitive.
    pub fn parse(s: &str) -> Result<Self, String> {
        match s.trim().to_ascii_lowercase().as_str() {
            "error" | "fail" | "abort" => Ok(Self::Error),
            "restart" | "wipe" | "clean" => Ok(Self::Restart),
            "resume" | "continue" => Ok(Self::Resume),
            other => Err(format!(
                "session-reuse: expected one of 'error' | 'restart' | 'resume', got '{other}'",
            )),
        }
    }
}

/// Where the default session directory lives when the user hasn't
/// passed `--session-path`. Three cases:
///
/// * **Normal invocation** (installed binary, user shell): the
///   session lands at `<cwd>/logs/<id>`. This is the documented,
///   long-standing user-facing behavior.
///
/// * **In-process tests that pre-sandboxed cwd** (e.g.
///   `checkpoint_resume_staircase` `in_dir(tmp, runner::run)`):
///   `<cwd>/logs/<id>` is already inside the test's tempdir
///   sandbox, so leave it alone — the test reads back from
///   `<cwd>/logs/latest` and would be broken by a redirect.
///
/// * **Cargo-spawned invocation where cwd lands inside the
///   workspace** (`cargo test` integration tests that spawn
///   `nbrs` as a subprocess without `--session-path`, plus
///   `cargo run --bin nbrs` runs from the workspace root):
///   the default would otherwise be the user-visible
///   `<workspace>/logs/<id>`, and the `session-keep` rotation
///   would evict real run sessions. Redirect into
///   `$TMPDIR/nbrs-sessions/<id>` — `.cargo/config.toml`
///   already points `TMPDIR` at `<workspace>/target/test-tmp`,
///   so cargo-spawned sessions live in `target/test-tmp/`
///   alongside other test artifacts. See
///   `feedback_tests_no_project_root`.
///
/// Tests that want to read back their session contents must
/// still pass `--session-path` to a known path — this
/// fallback is the blast-radius limiter, not a substitute for
/// explicit sandboxing.
pub fn default_session_dir(id: &str) -> PathBuf {
    default_logs_root().join(id)
}

/// Resolve the parent directory the runtime treats as the session
/// root when `--session-path` is absent. See [`default_session_dir`]
/// for the three-case logic.
pub fn default_logs_root() -> PathBuf {
    if cwd_is_workspace_dir() {
        // Cargo-spawned invocation, cwd is a cargo workspace
        // dir (`Cargo.toml` present) — the cwd-relative default
        // would write into the user-visible `<workspace>/logs/`.
        // Redirect to TMPDIR (per `.cargo/config.toml`, this is
        // `<workspace>/target/test-tmp/`), under an
        // `nbrs-sessions/` infix so siblings tempfiles stay
        // segregated.
        std::env::temp_dir().join("nbrs-sessions")
    } else {
        PathBuf::from("logs")
    }
}

/// `true` when both (a) we're running under cargo
/// (`CARGO_MANIFEST_DIR` env present, inherited from the cargo
/// parent through `std::process::Command`'s default env-inherit)
/// and (b) the current working directory has a `Cargo.toml` at
/// its root (workspace member or workspace root). Tests that
/// sandbox cwd into a tempdir won't satisfy (b) — the redirect
/// stays off for those.
fn cwd_is_workspace_dir() -> bool {
    if std::env::var_os("CARGO_MANIFEST_DIR").is_none() {
        return false;
    }
    std::env::current_dir()
        .map(|cwd| cwd.join("Cargo.toml").is_file())
        .unwrap_or(false)
}

/// Resolved session inputs from CLI / env. Built by
/// [`resolve_session_dir`] from one of:
///
/// - The umbrella `--session <kv-list>` flag (a
///   comma-separated list of `key:value` pairs and bare
///   shortcuts).
/// - Per-key long-form flags (`--session-name`,
///   `--session-path`, `--session-reuse`, `--session-keep`,
///   `--session-shelflife`).
/// - `SESSION_DIRECTORY` env var (equivalent to
///   `--session-path`).
///
/// When both forms are present the long-form flag wins; the
/// umbrella flag is shorthand. Within the umbrella value the
/// last-wins rule applies if a key is repeated.
#[derive(Debug, Clone, Default)]
pub struct SessionDirSpec {
    /// `name` — session id (basename of the session directory).
    /// Used in metric labels and as the resolved value of the
    /// `SESSION` token in `path`. When unset, defaults to the
    /// auto-generated `<scenario>_<timestamp>` form.
    pub session_name: Option<String>,
    /// `path` — full directory path. Optional `SESSION` token
    /// inside the path is replaced with `session_name` at write
    /// time. When unset, the path is `logs/<session_name>/`.
    pub session_path: Option<String>,
    /// `reuse` — policy when the resolved directory already
    /// contains prior session artifacts. Default `Error`.
    pub reuse: SessionReuse,
    /// `keep` — number of session directories retained under
    /// the parent at startup. Default 10. `0` disables.
    pub session_keep: usize,
    /// `shelflife` — max age of a session directory before
    /// purge at startup. Default 4 weeks. `0` disables.
    pub session_shelflife: std::time::Duration,
}

impl SessionDirSpec {
    /// `true` when no path/name flag is set — caller falls back
    /// to the default `logs/<auto_id>/` behavior. (`reuse`,
    /// `keep`, `shelflife` are always present at their defaults
    /// and don't count toward "is this empty?".)
    pub fn is_empty(&self) -> bool {
        self.session_name.is_none() && self.session_path.is_none()
    }

    /// Resolve to a concrete `(output_dir, session_id)`.
    ///
    /// - `auto_id` is the fallback session name when
    ///   `session_name` is unset.
    /// - The resolved id = `session_name.unwrap_or(auto_id)`.
    /// - The resolved path = `session_path` (with `SESSION`
    ///   token replaced by the resolved id) if set, else
    ///   `logs/<id>/`.
    ///
    /// Returns `None` only when the spec is empty and the
    /// caller should use defaults; otherwise `Some` with the
    /// resolved values.
    pub fn resolve(&self, auto_id: &str) -> Option<(PathBuf, String)> {
        if self.is_empty() {
            return None;
        }
        let id = self.session_name.clone().unwrap_or_else(|| auto_id.to_string());
        let path = match &self.session_path {
            Some(p) => PathBuf::from(p.replace(SESSION_TOKEN, &id)),
            None => default_session_dir(&id),
        };
        // Re-derive id from basename so a path-only spec
        // (`--session-path /tmp/foo`) still yields id="foo".
        let id = path.file_name()
            .and_then(|s| s.to_str())
            .map(String::from)
            .unwrap_or(id);
        Some((path, id))
    }

    /// `true` if the resolved path needs the auto-id (i.e.
    /// neither `session_name` nor a fully-concrete
    /// `session_path` is set). Used at startup to decide
    /// whether `logs/latest` can be wired immediately or must
    /// wait for session creation.
    pub fn needs_auto_id(&self) -> bool {
        if self.session_name.is_some() {
            return false;
        }
        match self.session_path.as_deref() {
            Some(p) => p.contains(SESSION_TOKEN),
            None => true,
        }
    }
}

/// Parse the umbrella `--session <kv-list>` argument into a
/// [`SessionDirSpec`]. The list is comma-separated; each item
/// is either a bare shortcut (`restart`, `resume`, `error`)
/// or a `key:value` pair.
///
/// Recognised keys (and their long-form flag equivalents):
///
/// | Key            | Long-form flag             |
/// | -------------- | -------------------------- |
/// | `name`         | `--session-name`           |
/// | `path` / `dir` | `--session-path`           |
/// | `reuse`        | `--session-reuse`          |
/// | `keep`         | `--session-keep`           |
/// | `shelflife`    | `--session-shelflife`      |
///
/// Bare shortcuts:
///
/// | Token     | Equivalent     |
/// | --------- | -------------- |
/// | `restart` | `reuse:restart`|
/// | `resume`  | `reuse:resume` |
/// | `error`   | `reuse:error`  |
///
/// Whitespace around items + key/value separators is trimmed.
/// Unknown keys produce a `Warn` log; the rest of the spec is
/// kept (so a typo doesn't kill the run).
pub fn parse_session_kv(s: &str) -> SessionDirSpec {
    let mut spec = SessionDirSpec {
        session_keep: DEFAULT_SESSIONS_MAX,
        session_shelflife: DEFAULT_SESSIONS_SHELFLIFE,
        ..SessionDirSpec::default()
    };
    for raw_item in s.split(',') {
        let item = raw_item.trim();
        if item.is_empty() { continue; }
        // Bare shortcut?
        match item {
            "restart" => { spec.reuse = SessionReuse::Restart; continue; }
            "resume"  => { spec.reuse = SessionReuse::Resume;  continue; }
            "error"   => { spec.reuse = SessionReuse::Error;   continue; }
            _ => {}
        }
        // key:value pair takes precedence over the
        // bare-token heuristics so `dir:/tmp/x` etc.
        // disambiguate cleanly.
        let (key, value) = match item.split_once(':') {
            Some((k, v)) => (k.trim(), v.trim()),
            None => {
                // Bare token without `:`. Two heuristics:
                //   - contains `/` OR resolves to an existing
                //     directory → treat as `path:<value>`.
                //     Lets operators write
                //     `--session logs/fulltest_2026...` without
                //     remembering the `path:` prefix.
                //   - otherwise → session name (SRD-04
                //     most-specific-name rule).
                if item.contains('/') || std::path::Path::new(item).is_dir() {
                    validate_session_path_or_exit(item, "umbrella `--session <bare-token>`");
                    spec.session_path = Some(item.to_string());
                } else {
                    spec.session_name = Some(item.to_string());
                }
                continue;
            }
        };
        match key {
            "name"      => spec.session_name = Some(value.to_string()),
            "path" | "dir" => {
                validate_session_path_or_exit(value, "umbrella `--session path:<v>`");
                spec.session_path = Some(value.to_string());
            }
            "reuse" => match SessionReuse::parse(value) {
                Ok(r) => spec.reuse = r,
                Err(e) => crate::observer::log(
                    crate::observer::LogLevel::Warn,
                    &format!("--session: {e}"),
                ),
            },
            "keep" => match value.parse::<usize>() {
                Ok(n) => spec.session_keep = n,
                Err(_) => crate::observer::log(
                    crate::observer::LogLevel::Warn,
                    &format!("--session: keep:{value:?} is not a non-negative integer"),
                ),
            },
            "shelflife" => match parse_duration(value) {
                Ok(d) => spec.session_shelflife = d,
                Err(e) => crate::observer::log(
                    crate::observer::LogLevel::Warn,
                    &format!("--session: shelflife: {e}"),
                ),
            },
            other => crate::observer::log(
                crate::observer::LogLevel::Warn,
                &format!("--session: unknown key {other:?} (recognised: name, path, dir, reuse, keep, shelflife)"),
            ),
        }
    }
    spec
}

/// Parse the umbrella `--session <kv>` flag + per-key
/// long-form flags + env vars into a [`SessionDirSpec`].
///
/// **Umbrella form** — `--session <kv-list>` where `<kv-list>`
/// is comma-separated `key:value` pairs and bare shortcuts.
/// See [`parse_session_kv`] for the full key list.
///
/// **Long-form flags** — same keys, individually:
/// `--session-name`, `--session-path`, `--session-reuse`,
/// `--session-keep`, `--session-shelflife`. Both `=value` and
/// space-separated `<flag> <value>` shapes are accepted.
///
/// **Env vars:**
/// - `SESSION_DIRECTORY` → `--session-path`
/// - `SESSIONS_MAX` → `--session-keep`
/// - `SESSIONS_SHELFLIFE` → `--session-shelflife`
///
/// **Precedence** (highest first):
/// 1. Long-form per-key flag.
/// 2. Umbrella `--session` value (parsed left-to-right; later
///    keys override earlier ones).
/// 3. Env var equivalents.
/// 4. Default.
pub fn resolve_session_dir(args: &[String]) -> SessionDirSpec {
    // Each flag goes through `resolve_flag` which checks both
    // CLI and the auto-derived `NBRS_<FLAG>` env var. Setting
    // both is a hard error.

    // Umbrella --session / NBRS_SESSION first, so long-form
    // and per-key env can override individual fields.
    let mut spec = match resolve_flag(args, "--session") {
        Some(kv) => parse_session_kv(&kv),
        None => SessionDirSpec {
            session_keep: DEFAULT_SESSIONS_MAX,
            session_shelflife: DEFAULT_SESSIONS_SHELFLIFE,
            ..SessionDirSpec::default()
        },
    };

    // Long-form per-key flags (each with NBRS_ env equivalent).
    if let Some(v) = resolve_flag(args, "--session-name") {
        spec.session_name = Some(v);
    }
    if let Some(v) = resolve_flag(args, "--session-path") {
        validate_session_path_or_exit(&v, "`--session-path` flag (or NBRS_SESSION_PATH env)");
        spec.session_path = Some(v);
    }
    if let Some(v) = resolve_flag(args, "--session-reuse") {
        if let Ok(r) = SessionReuse::parse(&v) {
            spec.reuse = r;
        }
    }
    if let Some(v) = resolve_flag(args, "--session-keep") {
        if let Ok(n) = v.trim().parse::<usize>() {
            spec.session_keep = n;
        }
    }
    if let Some(v) = resolve_flag(args, "--session-shelflife") {
        if let Ok(d) = parse_duration(&v) {
            spec.session_shelflife = d;
        }
    }

    // Legacy env: SESSION_DIRECTORY is the pre-SRD-04 name for
    // NBRS_SESSION_PATH. Honor it for back-compat with one
    // deprecation warning. Skip silently if NBRS_SESSION_PATH
    // already won.
    if spec.session_path.is_none()
        && let Ok(v) = std::env::var(SESSION_DIRECTORY_ENV)
        && !v.trim().is_empty()
    {
        crate::observer::log(
            crate::observer::LogLevel::Warn,
            "SESSION_DIRECTORY is deprecated; use NBRS_SESSION_PATH (SRD-04 NBRS_-prefix convention).",
        );
        validate_session_path_or_exit(&v, "legacy `SESSION_DIRECTORY` env");
        spec.session_path = Some(v);
    }

    spec
}

/// Resolve the `--session` / `--session-path` / `--session-name`
/// arguments to a session directory path that `metrics.db`
/// would live under. Used by every read-side tool (`nbrs plot`,
/// `nbrs report`, `nbrs metrics ...`, completion) so the same
/// flag means the same thing everywhere.
///
/// Returns `None` when no session flag is on the line — the
/// caller falls back to its own default (typically
/// `logs/latest`).
///
/// **Unlike** [`apply_session_directory_at_startup`] this never
/// mutates the filesystem (no `logs/latest` symlink rewrite,
/// no purge). It's a pure path computation: read-side tools
/// shouldn't have side effects on the active session symlink.
pub fn read_session_dir(args: &[String]) -> Option<PathBuf> {
    let spec = resolve_session_dir(args);
    if spec.is_empty() || spec.needs_auto_id() {
        return None;
    }
    spec.resolve("").map(|(p, _)| p)
}

/// Active-session resolver consolidating the patterns used by
/// `replay.rs` / `summary.rs` / `report_cmd.rs` /
/// `metricsql_cmd.rs` / `plot_metrics.rs` / `completion.rs`.
///
/// Resolves to an existing session directory in this order:
///
/// 1. `--session` / `--session-path` / `--session-name` from
///    `args` (via [`read_session_dir`]).
/// 2. The `logs/latest` symlink, when it exists and points at a
///    session directory with the expected artifacts
///    (`metrics.db` or `session.log`).
/// 3. `Err` with a remediation message naming the flags that
///    would have worked.
///
/// Read-only — no filesystem mutation. Use this anywhere a
/// post-run command needs to operate on an existing session.
///
/// **Why a separate function from [`read_session_dir`].**
/// `read_session_dir` returns `Option` (no opinion on what to do
/// when nothing's set); `resolve_active` makes that the call
/// site's failure mode, with a single canonical error message.
pub fn resolve_active(args: &[String]) -> Result<PathBuf, String> {
    if let Some(p) = read_session_dir(args) {
        if !p.exists() {
            return Err(format!(
                "session directory '{}' does not exist", p.display(),
            ));
        }
        return Ok(p);
    }
    let latest = PathBuf::from("logs/latest");
    if latest.exists() {
        // Resolve through the symlink so callers get a stable
        // path that won't change underneath them mid-run.
        let resolved = std::fs::canonicalize(&latest)
            .unwrap_or(latest.clone());
        return Ok(resolved);
    }
    Err(
        "no active session — run a workload first, or pass \
         `--session <name>` / `--session-path <dir>` to point \
         at an existing one".to_string(),
    )
}

/// Default for `--sessions-max`: keep the 10 most-recent
/// sessions, purge older ones at startup.
pub const DEFAULT_SESSIONS_MAX: usize = 10;

/// Default for `--sessions-shelflife`: 4 weeks. Sessions older
/// than this are purged at startup regardless of the
/// `--sessions-max` cap.
pub const DEFAULT_SESSIONS_SHELFLIFE: std::time::Duration =
    std::time::Duration::from_secs(60 * 60 * 24 * 7 * 4);

/// Parse a duration suffix-string. Accepts:
/// - `<n>s` — seconds
/// - `<n>m` — minutes
/// - `<n>h` — hours
/// - `<n>d` — days
/// - `<n>w` — weeks
/// - bare integer — seconds (back-compat with raw numeric input)
///
/// Whitespace is trimmed. `0` (any unit) disables the cap.
pub fn parse_duration(s: &str) -> Result<std::time::Duration, String> {
    let s = s.trim();
    if s.is_empty() {
        return Err("empty duration".into());
    }
    let (num_part, unit_seconds) = if let Some(n) = s.strip_suffix('w') {
        (n, 60 * 60 * 24 * 7)
    } else if let Some(n) = s.strip_suffix('d') {
        (n, 60 * 60 * 24)
    } else if let Some(n) = s.strip_suffix('h') {
        (n, 60 * 60)
    } else if let Some(n) = s.strip_suffix('m') {
        (n, 60)
    } else if let Some(n) = s.strip_suffix('s') {
        (n, 1)
    } else {
        (s, 1)  // bare number = seconds
    };
    let n: u64 = num_part.trim().parse().map_err(|_| format!(
        "duration: '{s}' is not a valid number with optional s/m/h/d/w suffix",
    ))?;
    Ok(std::time::Duration::from_secs(n * unit_seconds))
}

/// Return `true` when `dir` exists AND contains artifacts that
/// indicate a prior session's state (metrics db, session log,
/// or checkpoint). Used by [`Session::new_with_args`] to decide
/// whether the reuse-policy check applies.
pub fn session_dir_has_prior_artifacts(dir: &Path) -> bool {
    if !dir.exists() {
        return false;
    }
    for marker in ["metrics.db", "session.log", "checkpoint.jsonl"] {
        if dir.join(marker).exists() {
            return true;
        }
    }
    false
}

/// Count directory entries under `parent` (excluding
/// symlinks, files, and the `latest` symlink target). Used
/// for end-of-run keep-cap forecasting.
pub fn count_session_dirs(parent: &Path) -> usize {
    let Ok(rd) = std::fs::read_dir(parent) else { return 0; };
    rd.filter_map(|e| e.ok())
        .filter(|e| {
            std::fs::symlink_metadata(e.path())
                .map(|m| !m.file_type().is_symlink() && m.file_type().is_dir())
                .unwrap_or(false)
        })
        .count()
}

/// Forecast how many session directories the **next** new
/// session would auto-purge under `parent` given the current
/// keep cap. Returns `0` when no purge would happen (or when
/// `keep_cap == 0`, which disables the cap).
///
/// Logged at INFO level by the end-of-run notice guard so
/// operators see the imminent cleanup before it happens, with
/// instructions for disabling it.
pub fn forecast_keep_purge(parent: &Path, keep_cap: usize) -> usize {
    if keep_cap == 0 { return 0; }
    let current = count_session_dirs(parent);
    // After +1 new session, total would be current+1. Anything
    // past keep_cap gets purged.
    (current + 1).saturating_sub(keep_cap)
}

/// `true` if `path` carries one of the signature artifacts an
/// nbrs run writes — the gate the purge logic uses to avoid
/// destroying unrelated directories that happen to share a
/// parent with an explicit `--session-path`. Any one of
/// `metrics.db`, `session.log`, or `checkpoint.jsonl` is
/// enough; the runtime writes at least one of them per
/// session, so the test is robust across early-aborted runs.
fn looks_like_session_dir(path: &Path) -> bool {
    const SIGNATURES: &[&str] = &["metrics.db", "session.log", "checkpoint.jsonl"];
    SIGNATURES.iter().any(|s| path.join(s).exists())
}

/// Purge stale session directories under `parent` according to
/// `max_sessions` (keep the latest N) and `shelflife` (drop
/// anything older than this).
///
/// Skipped: the `latest` symlink and whatever it points at
/// (the active session). Non-directory entries (loose files
/// like a stray `summary.md`) are left alone.
///
/// Errors during enumeration / removal are logged at Warn
/// (this is a best-effort housekeeping pass; failure shouldn't
/// abort startup).
pub fn purge_stale_sessions(
    parent: &Path,
    max_sessions: usize,
    shelflife: std::time::Duration,
) {
    if !parent.exists() {
        return;
    }
    // Resolve the latest-symlink's target so we never delete
    // the active session out from under a running operator.
    let latest_target: Option<PathBuf> = std::fs::read_link(parent.join("latest"))
        .ok()
        .map(|t| if t.is_absolute() { t } else { parent.join(t) });

    // Collect (path, mtime) for every subdirectory.
    let mut entries: Vec<(PathBuf, std::time::SystemTime)> = match std::fs::read_dir(parent) {
        Ok(rd) => rd,
        Err(e) => {
            crate::observer::log(crate::observer::LogLevel::Warn, &format!(
                "warning: session cleanup: failed to read {}: {e}", parent.display(),
            ));
            return;
        }
    }.filter_map(|entry| entry.ok())
        .filter_map(|entry| {
            let path = entry.path();
            // Skip symlinks (logs/latest) — only cleanup real
            // directories.
            let md = std::fs::symlink_metadata(&path).ok()?;
            if md.file_type().is_symlink() || !md.file_type().is_dir() {
                return None;
            }
            // Don't delete the active session.
            if let Some(target) = latest_target.as_ref()
                && path == *target
            {
                return None;
            }
            // Only consider directories that *look like* nbrs
            // sessions — i.e. carry one of the signature
            // artifacts the runtime writes. Without this, an
            // explicit `--session-path /tmp/foo` would set
            // `cleanup_parent = /tmp` and drag arbitrary
            // unrelated dirs (snap.rootfs_*, systemd-private-*,
            // …) into the purge set.
            if !looks_like_session_dir(&path) {
                return None;
            }
            let mtime = md.modified().ok()?;
            Some((path, mtime))
        })
        .collect();

    if entries.is_empty() {
        return;
    }

    // Sort newest-first for the max-cap pass.
    entries.sort_by(|a, b| b.1.cmp(&a.1));

    let now = std::time::SystemTime::now();
    let mut to_purge: Vec<&PathBuf> = Vec::new();

    // Cap by count: anything past the max is purged.
    if max_sessions > 0 && entries.len() > max_sessions {
        for (p, _) in &entries[max_sessions..] {
            to_purge.push(p);
        }
    }

    // Cap by age: anything older than now - shelflife is purged.
    if !shelflife.is_zero() {
        for (p, mtime) in &entries[..entries.len().min(if max_sessions == 0 { usize::MAX } else { max_sessions })] {
            if let Ok(age) = now.duration_since(*mtime)
                && age > shelflife
                && !to_purge.contains(&p)
            {
                to_purge.push(p);
            }
        }
    }

    for path in to_purge {
        if let Err(e) = std::fs::remove_dir_all(path) {
            crate::observer::log(crate::observer::LogLevel::Warn, &format!(
                "warning: session cleanup: failed to remove {}: {e}", path.display(),
            ));
        }
    }
}

/// Apply session-dir overrides at binary startup so every
/// subcommand (`run`, `plot`, `report`, `summary`, `tui`, …)
/// sees consistent session wiring.
///
/// **Effect when a fully-resolvable path is set** (i.e. no
/// `SESSION` token, or `--session` provides the name): updates
/// the `logs/latest` symlink to point at the resolved path.
/// Every read-side command that defaults to
/// `logs/latest/metrics.db` etc. now targets the configured
/// session automatically.
///
/// **Effect when `SESSION` token is present and no
/// `--session`** is given: no-op at startup (the auto-id isn't
/// known yet). The write-side [`Session::new`] resolves the
/// token at run creation and updates the symlink there.
///
/// **Effect when no flags / env are set:** no-op. Existing
/// behavior preserved.
///
/// Idempotent. Failures log Warn and return; this is a
/// convenience surface, not a hard dependency.
pub fn apply_session_directory_at_startup(args: &[String]) {
    let spec = resolve_session_dir(args);

    // Lifecycle cleanup runs unconditionally — it consults
    // `--sessions-max` / `--sessions-shelflife` (with defaults).
    // Targets the parent dir: `--logs-dir` if specified, else
    // `logs/` under cwd. When `--session-dir` is explicit, its
    // *parent* directory is the cleanup target.
    let cleanup_parent = if let Some(sd) = spec.session_path.as_ref() {
        let resolved = sd.replace(SESSION_TOKEN, "");
        PathBuf::from(resolved).parent()
            .map(|p| p.to_path_buf())
            .unwrap_or_else(default_logs_root)
    } else {
        default_logs_root()
    };
    purge_stale_sessions(&cleanup_parent, spec.session_keep, spec.session_shelflife);

    if spec.is_empty() || spec.needs_auto_id() {
        return;
    }
    // `auto_id` won't be consumed because `needs_auto_id()` is
    // false above; pass an empty placeholder for the contract.
    let Some((path, _id)) = spec.resolve("") else { return; };
    let logs = default_logs_root();
    // Only touch `logs/` when the resolved session path lives
    // under it. A `--session-path /tmp/x` (or any path the user
    // redirected outside `logs/`) is an explicit opt-out:
    // - The mkdir below would otherwise create a stray
    //   `<cwd>/logs/` directory in test sandboxes / CI
    //   working trees that don't want it (the
    //   `feedback_tests_no_project_root` rule), and
    // - hijacking `logs/latest` to point there would dangle
    //   the moment the external dir is cleaned up — exactly
    //   what test fixtures do when they wipe their own
    //   tempdirs.
    if !target_is_under(&logs, &path) {
        return;
    }
    if let Err(e) = std::fs::create_dir_all(&logs) {
        crate::observer::log(crate::observer::LogLevel::Warn, &format!(
            "warning: --session-dir startup hook: failed to create logs/: {e}",
        ));
        return;
    }
    let latest = logs.join("latest");
    // Symlink target is computed RELATIVE to the link's parent
    // (`logs/`) so the link survives directory moves and stays
    // readable in `ls -la` output as `logs/latest -> foo_2026...`
    // rather than `logs/latest -> /home/.../nb-rs/logs/foo_2026...`.
    // See `relative_symlink_target` for the path-arithmetic.
    let relative_target = relative_symlink_target(&latest, &path);
    let _ = std::fs::remove_file(&latest);
    if let Err(e) = std::os::unix::fs::symlink(&relative_target, &latest) {
        crate::observer::log(crate::observer::LogLevel::Warn, &format!(
            "warning: --session-dir: failed to update logs/latest → {}: {e}",
            relative_target.display(),
        ));
    }
}

/// True when `target`'s absolute path is `logs_dir`'s absolute
/// path or lies below it. Both paths are resolved against the
/// current cwd if relative; canonicalize is avoided so this works
/// for not-yet-created targets.
pub(crate) fn target_is_under(logs_dir: &Path, target: &Path) -> bool {
    let cwd = std::env::current_dir().ok();
    let abs = |p: &Path| -> Option<PathBuf> {
        if p.is_absolute() { Some(p.to_path_buf()) }
        else { cwd.as_ref().map(|c| c.join(p)) }
    };
    match (abs(logs_dir), abs(target)) {
        (Some(l), Some(t)) => t.starts_with(&l),
        _ => false,
    }
}

/// Compute a target string for a symlink at `link_path` that
/// addresses `target` via a path relative to the link's parent
/// directory. Falls back to the absolute target when neither
/// path can be canonicalised (e.g. target doesn't exist yet,
/// which is normal for `logs/latest -> <id>` at session-create
/// time).
///
/// Examples:
///   `logs/latest`, `logs/foo_20260504/`        → `foo_20260504`
///   `logs/latest`, `target/test-tmp/sandbox/`  → `../target/test-tmp/sandbox`
///   `logs/latest`, `/tmp/explore/`             → `/tmp/explore`  (no common root)
pub(crate) fn relative_symlink_target(link_path: &Path, target: &Path) -> PathBuf {
    let link_parent = link_path.parent().unwrap_or_else(|| Path::new("."));
    // Resolve both sides to absolute paths to compute a relative
    // route. `canonicalize` would also follow symlinks; we want
    // logical absolutes so we use cwd-prefixing for the
    // not-yet-existing target case.
    let cwd = std::env::current_dir().ok();
    let abs = |p: &Path| -> Option<PathBuf> {
        if p.is_absolute() { Some(p.to_path_buf()) }
        else { cwd.as_ref().map(|c| c.join(p)) }
    };
    let (Some(link_abs), Some(tgt_abs)) = (abs(link_parent), abs(target)) else {
        return target.to_path_buf();
    };
    let link_comps: Vec<_> = link_abs.components().collect();
    let tgt_comps: Vec<_>  = tgt_abs.components().collect();
    let common = link_comps.iter().zip(tgt_comps.iter())
        .take_while(|(a, b)| a == b)
        .count();
    if common == 0 {
        // Different roots (e.g. `/home/...` vs `/tmp/...`) —
        // can't express as a relative path without more `..`s
        // than is sensible. Fall back to absolute.
        return tgt_abs;
    }
    let ups = link_comps.len() - common;
    let mut rel = PathBuf::new();
    for _ in 0..ups { rel.push(".."); }
    for c in &tgt_comps[common..] { rel.push(c.as_os_str()); }
    if rel.as_os_str().is_empty() { rel.push("."); }
    rel
}

impl Session {
    /// Create a new session. Picks the output directory in this
    /// priority order:
    ///
    /// 1. `--session-dir <path>` CLI flag (or
    ///    `SESSION_DIRECTORY` env var, equivalent). `SESSION`
    ///    token in the path is replaced with the auto-generated
    ///    `{scenario}_{timestamp}` name. The basename becomes
    ///    the session id.
    /// 2. `--logs-dir <parent>` and/or `--session <name>` CLI
    ///    flags. Compose into `<parent>/<name>` (defaulting to
    ///    `logs/<auto-id>` for any unspecified component).
    /// 3. Default — `logs/{scenario}_{timestamp}/`.
    ///
    /// `args` is the raw CLI args slice. Pass an empty slice to
    /// get env-only resolution.
    pub fn new_with_args(workload: &str, scenario: &str, args: &[String]) -> Self {
        let timestamp = format_timestamp();
        let workload_stem = Path::new(workload)
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("workload");
        let auto_id = format!("{scenario}_{timestamp}");

        let spec = resolve_session_dir(args);
        let (output_dir, id) = spec.resolve(&auto_id)
            .unwrap_or_else(|| (default_session_dir(&auto_id), auto_id.clone()));

        // Reuse-policy check. Only fires when the resolved
        // directory already holds prior session artifacts. The
        // resume path enters via `Session::resume`, not here, so
        // any pre-existing artifacts at this entry point are a
        // collision the operator must explicitly resolve.
        if session_dir_has_prior_artifacts(&output_dir) {
            match spec.reuse {
                SessionReuse::Error => {
                    eprintln!(
                        "error: session directory {} already contains artifacts \
                         (metrics.db / session.log / checkpoint.jsonl).\n  \
                         Pick a reuse policy:\n    \
                         --session-reuse=restart  (wipe artifacts, fresh run)\n    \
                         --session-reuse=resume   (continue with the prior session — \
                         equivalent to --resume <id>)\n  \
                         Or pick a different path via --session, --logs-dir, \
                         --session-dir, or SESSION_DIRECTORY env.",
                        output_dir.display(),
                    );
                    std::process::exit(2);
                }
                SessionReuse::Restart => {
                    for marker in [
                        "metrics.db", "session.log", "checkpoint.jsonl",
                        "checkpoint.lock", "summary.md", "summary.txt",
                        "summary.json", "tui.dump",
                        "flamegraph.svg", "flamegraph-perf.svg",
                        "flamegraph-perf.md",
                    ] {
                        let _ = std::fs::remove_file(output_dir.join(marker));
                    }
                    crate::observer::log(
                        crate::observer::LogLevel::Warn,
                        &format!(
                            "session-reuse=restart: wiped prior artifacts in {}",
                            output_dir.display(),
                        ),
                    );
                }
                SessionReuse::Resume => {
                    eprintln!(
                        "error: --session-reuse=resume requires --resume to actually \
                         continue the prior session. Add --resume (or --resume-latest) \
                         to your command line. Path: {}",
                        output_dir.display(),
                    );
                    std::process::exit(2);
                }
            }
        }

        // Create the output directory (and any missing parents)
        if let Err(e) = std::fs::create_dir_all(&output_dir) {
            crate::observer::log(
                crate::observer::LogLevel::Warn,
                &format!("warning: failed to create session output directory {}: {e}", output_dir.display()),
            );
        }

        // Refresh `logs/latest` → this session, then *clear* every
        // per-artifact convenience symlink from the previous
        // session. Optional artifacts (flamegraphs, summary,
        // tui.dump) get their convenience link only when their
        // writer actually produces the file — eager pre-creation
        // would leave a dangling link any time the run skips that
        // artifact (e.g. no `profiler=` on the CLI). The two
        // guaranteed-written artifacts (`session.log` / `metrics.db`)
        // are linked here so live tooling can `tail -f
        // logs/session.log` or open `logs/metrics.db` without
        // chasing the timestamped session id.
        let logs = PathBuf::from("logs");
        // Only touch `logs/` when the session output dir is under
        // it. An explicit `--session-path /tmp/x` (or any redirect
        // outside `logs/`) is treated as opt-out:
        // - The mkdir below would otherwise stamp a stray
        //   `<cwd>/logs/` directory in test sandboxes / CI
        //   working trees that don't want it (the
        //   `feedback_tests_no_project_root` rule), and
        // - `logs/latest` shouldn't get hijacked by test fixtures
        //   or one-off `--session-path` runs.
        if target_is_under(&logs, &output_dir) {
            let _ = std::fs::create_dir_all(&logs);
            let latest = logs.join("latest");
            let _ = std::fs::remove_file(&latest);
            let _ = std::os::unix::fs::symlink(
                &latest_symlink_target(&output_dir, &logs, &id),
                &latest,
            );
            for stale in [
                "summary.md",
                "flamegraph.svg",
                "flamegraph-perf.svg",
                "flamegraph-perf.md",
                "tui.dump",
            ] {
                let _ = std::fs::remove_file(logs.join(stale));
            }
            for artifact in ["session.log", "metrics.db"] {
                let link = logs.join(artifact);
                let _ = std::fs::remove_file(&link);
                // Relative target routes through the `latest`
                // symlink so swapping sessions updates every artifact
                // link in a single `latest` update.
                let target = PathBuf::from("latest").join(artifact);
                let _ = std::os::unix::fs::symlink(&target, &link);
            }
        }

        // Root labels carry both the session id (already
        // unique per run) and the workload's bare stem
        // (filename without path or extension, falling
        // back to `"workload"` for inline / op= runs that
        // have no file). Every metric and component
        // descendant inherits these via the component
        // tree, so cross-session queries can group by
        // `workload="full_cql_vector"` regardless of
        // whether the operator ran it from
        // `./full_cql_vector.yaml`,
        // `adapters/cql/workloads/full_cql_vector.yaml`,
        // or any other path.
        let component = Component::root(
            Labels::of("session", &id).with("workload", workload_stem),
            std::collections::HashMap::new(),
        );
        // Install the session root as the resolver backing for
        // GK runtime-context nodes (`control(...)`, `rate()`,
        // `concurrency()`, etc.). See SRD 12 §"Runtime context
        // nodes" and nbrs-variates/src/nodes/runtime_context.rs.
        nbrs_variates::nodes::runtime_context::set_session_root(component.clone());

        Self {
            id,
            output_dir,
            workload: workload_stem.to_string(),
            scenario: scenario.to_string(),
            component,
            metrics_query: Mutex::new(None),
        }
    }

    /// Backward-compat shim for callers that don't have CLI
    /// args handy. Equivalent to
    /// `Session::new_with_args(workload, scenario, &[])` —
    /// resolves session-dir from `SESSION_DIRECTORY` env only.
    pub fn new(workload: &str, scenario: &str) -> Self {
        Self::new_with_args(workload, scenario, &[])
    }

    /// Resume an existing session — reuse its directory, id,
    /// and (consequently) its `metrics.db` so the resumed
    /// invocation appends to the same metrics history rather
    /// than starting fresh in a new dir. Per SRD-44 §"Wholesale
    /// metrics-purge", phases that re-run on resume need their
    /// prior sample rows purged in-place; that requires writing
    /// to the same db, which requires reusing the same session
    /// dir. The id is read from the directory's basename, so it
    /// preserves the original timestamp suffix.
    pub fn resume(
        prior_session_dir: PathBuf,
        workload: &str,
        scenario: &str,
    ) -> Self {
        let workload_stem = Path::new(workload)
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("workload");
        let id = prior_session_dir.file_name()
            .and_then(|s| s.to_str())
            .map(String::from)
            .unwrap_or_else(|| {
                // Fallback: synthesize a fresh id from scenario
                // + timestamp. Shouldn't fire in practice (we
                // resolved the dir to a real session before
                // calling here).
                format!("{scenario}_{}", format_timestamp())
            });

        // Re-establish the convenience symlinks under `logs/` so
        // `tail -f logs/session.log` and `sqlite3 logs/metrics.db`
        // resolve to this resumed session's artifacts. Same
        // shape as Session::new — a no-op when the symlinks
        // already point here from a prior run. Skipped when the
        // session lives outside `logs/` (see Session::new for
        // rationale: one-off external session dirs shouldn't
        // hijack the user's `logs/latest`).
        let logs = PathBuf::from("logs");
        if target_is_under(&logs, &prior_session_dir) {
            let latest = logs.join("latest");
            let _ = std::fs::remove_file(&latest);
            let _ = std::os::unix::fs::symlink(&id, &latest);
            for artifact in ["session.log", "metrics.db"] {
                let link = logs.join(artifact);
                let _ = std::fs::remove_file(&link);
                let target = PathBuf::from("latest").join(artifact);
                let _ = std::os::unix::fs::symlink(&target, &link);
            }
        }

        // Root labels carry session + workload stem (see
        // `Session::new_with_args` for rationale). Resume
        // reuses the same workload stem so the resumed
        // metrics keep the same `workload=...` label and
        // continue to match cross-session queries that
        // grouped on it.
        let component = Component::root(
            Labels::of("session", &id).with("workload", workload_stem),
            std::collections::HashMap::new(),
        );
        nbrs_variates::nodes::runtime_context::set_session_root(component.clone());

        Self {
            id,
            output_dir: prior_session_dir,
            workload: workload_stem.to_string(),
            scenario: scenario.to_string(),
            component,
            metrics_query: Mutex::new(None),
        }
    }

    /// Create a `logs/<artifact>` convenience symlink that points
    /// (through `logs/latest`) at the named artifact in the
    /// current session's output dir. Idempotent — replaces any
    /// existing link with the same name. Call from the writer
    /// at the moment the artifact has actually been produced, so
    /// the convenience link never dangles.
    pub fn link_artifact(name: &str) {
        let logs = PathBuf::from("logs");
        let link = logs.join(name);
        let _ = std::fs::remove_file(&link);
        let target = PathBuf::from("latest").join(name);
        let _ = std::os::unix::fs::symlink(&target, &link);
    }

    /// Install the shared [`MetricsQuery`] handle. Called once by the
    /// runner after it has planned the cadence tree and built the
    /// cadence reporter. Panics if called twice.
    pub fn set_metrics_query(&self, query: Arc<MetricsQuery>) {
        let mut slot = self.metrics_query.lock().unwrap_or_else(|e| e.into_inner());
        assert!(slot.is_none(), "session metrics_query already installed");
        *slot = Some(query);
    }

    /// Borrow the installed [`MetricsQuery`]. Returns `None` before
    /// the runner wires it.
    pub fn metrics_query(&self) -> Option<Arc<MetricsQuery>> {
        self.metrics_query.lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// Path to the SQLite metrics file for this session.
    pub fn metrics_path(&self) -> PathBuf {
        self.output_dir.join("metrics.db")
    }

    /// Path for a profiler output file.
    pub fn profiler_path(&self, suffix: &str) -> PathBuf {
        self.output_dir.join(format!("flamegraph{suffix}.svg"))
    }

    /// Path for an arbitrary session artifact.
    pub fn artifact_path(&self, filename: &str) -> PathBuf {
        self.output_dir.join(filename)
    }
}

/// Format the current time as `YYYYMMDD_HHmmss`.
/// Compute the symlink target string for `logs/latest`,
/// always relative. For sessions under `logs/<id>/` the link
/// resolves to a bare `{id}`; sessions outside `logs/` get an
/// `../...` route up to a common ancestor. Relative targets
/// keep the link portable across directory moves and readable
/// in `ls -la` output.
fn latest_symlink_target(output_dir: &Path, logs: &Path, id: &str) -> PathBuf {
    if output_dir.parent() == Some(logs) {
        return PathBuf::from(id);
    }
    let latest = logs.join("latest");
    relative_symlink_target(&latest, output_dir)
}

fn format_timestamp() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    // Convert epoch seconds to date/time components.
    // Simple implementation without chrono dependency.
    let days = secs / 86400;
    let time = secs % 86400;
    let hours = time / 3600;
    let minutes = (time % 3600) / 60;
    let seconds = time % 60;

    // Days since epoch to Y/M/D (simplified Gregorian)
    let (year, month, day) = days_to_ymd(days);

    format!("{year:04}{month:02}{day:02}_{hours:02}{minutes:02}{seconds:02}")
}

/// Current wall-clock time as `YYYY-MM-DD HH:MM:SS.mmm` (UTC).
/// Used by the session log writer for human-readable line timestamps.
pub fn now_log_timestamp() -> String {
    format_log_timestamp(std::time::SystemTime::now())
}

/// Format a specific `SystemTime` in the same shape as
/// [`now_log_timestamp`]. Used by the failure-dump path
/// in `nbrs-tui::observer` to render per-LogEntry
/// timestamps captured at log-emit time.
pub fn format_log_timestamp(t: std::time::SystemTime) -> String {
    let dur = t.duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let secs = dur.as_secs();
    let millis = dur.subsec_millis();
    let days = secs / 86400;
    let time = secs % 86400;
    let hours = time / 3600;
    let minutes = (time % 3600) / 60;
    let seconds = time % 60;
    let (year, month, day) = days_to_ymd(days);
    format!("{year:04}-{month:02}-{day:02} {hours:02}:{minutes:02}:{seconds:02}.{millis:03}")
}

/// Convert days since Unix epoch to (year, month, day).
fn days_to_ymd(days: u64) -> (u64, u64, u64) {
    // Algorithm from Howard Hinnant's date library
    let z = days + 719468;
    let era = z / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamp_format() {
        let ts = format_timestamp();
        // Should be 15 chars: YYYYMMDD_HHmmss
        assert_eq!(ts.len(), 15, "timestamp: {ts}");
        assert!(ts.contains('_'), "timestamp should contain underscore: {ts}");
    }

    /// Serialize all tests that mutate process-global state
    /// (SESSION_DIRECTORY env var, cwd-relative `logs/` writes,
    /// etc.). Cargo runs tests in parallel by default; tests
    /// touching shared state must hold this lock for the
    /// duration of the operation.
    fn env_test_lock() -> std::sync::MutexGuard<'static, ()> {
        use std::sync::Mutex;
        static LOCK: Mutex<()> = Mutex::new(());
        LOCK.lock().unwrap_or_else(|e| e.into_inner())
    }

    #[test]
    fn session_id_format() {
        let _g = env_test_lock();
        unsafe { std::env::remove_var(SESSION_DIRECTORY_ENV); }
        let session = Session::new("full_cql_vector.yaml", "fknn_rampup");
        assert!(session.id.starts_with("fknn_rampup_"), "id: {}", session.id);
        let expected_root = default_logs_root();
        assert!(session.output_dir.starts_with(&expected_root),
            "output_dir {} should start with {}",
            session.output_dir.display(), expected_root.display());
    }

    #[test]
    fn session_paths() {
        let _g = env_test_lock();
        unsafe { std::env::remove_var(SESSION_DIRECTORY_ENV); }
        let session = Session::new("test.yaml", "smoke");
        assert!(session.metrics_path().ends_with("metrics.db"));
        assert!(session.profiler_path("").ends_with("flamegraph.svg"));
        assert!(session.profiler_path("-perf").ends_with("flamegraph-perf.svg"));
    }

    /// Clear every env var that `resolve_session_dir` reads, so a
    /// `spec_*` test sees CLI-only inputs. Without this, a
    /// concurrent test that sets `NBRS_SESSION_NAME` (or any
    /// peer var) collides with this test's CLI flag, hits the
    /// "configuration conflict" path in `resolve_flag`, and
    /// calls `process::exit(2)` — killing the entire test
    /// process and surfacing as an unrelated test failure.
    /// Must be called under [`env_test_lock`] so the cleanup
    /// holds for the duration of the spec resolution.
    fn clear_session_env() {
        // SAFETY: under env_test_lock, no other test thread is
        // touching these vars.
        unsafe {
            std::env::remove_var("NBRS_SESSION");
            std::env::remove_var("NBRS_SESSION_NAME");
            std::env::remove_var("NBRS_SESSION_PATH");
            std::env::remove_var("NBRS_SESSION_REUSE");
            std::env::remove_var("NBRS_SESSION_KEEP");
            std::env::remove_var("NBRS_SESSION_SHELFLIFE");
            std::env::remove_var(SESSION_DIRECTORY_ENV);
        }
    }

    #[test]
    fn spec_session_path_flag_yields_basename_id() {
        let _g = env_test_lock();
        clear_session_env();
        let args = vec!["--session-path=/tmp/explicit".into()];
        let spec = resolve_session_dir(&args);
        let (path, id) = spec.resolve("auto-id").unwrap();
        assert_eq!(path.to_str(), Some("/tmp/explicit"));
        assert_eq!(id, "explicit");
    }

    #[test]
    fn spec_session_name_only_yields_default_logs_dir() {
        let _g = env_test_lock();
        clear_session_env();
        let args = vec!["--session-name=alpha".into()];
        let (path, id) = resolve_session_dir(&args).resolve("autogen").unwrap();
        assert_eq!(path, default_logs_root().join("alpha"));
        assert_eq!(id, "alpha");
    }

    #[test]
    fn spec_session_path_token_replaced_with_name() {
        let _g = env_test_lock();
        clear_session_env();
        let args = vec![
            "--session-path=/data/SESSION_run".into(),
            "--session-name=alpha".into(),
        ];
        let (path, id) = resolve_session_dir(&args).resolve("autogen").unwrap();
        assert_eq!(path.to_str(), Some("/data/alpha_run"));
        assert_eq!(id, "alpha_run");
    }

    #[test]
    fn spec_session_path_token_falls_back_to_auto_id_when_no_name() {
        let _g = env_test_lock();
        clear_session_env();
        let args = vec!["--session-path=/data/SESSION_run".into()];
        let (path, id) = resolve_session_dir(&args).resolve("autogen").unwrap();
        assert_eq!(path.to_str(), Some("/data/autogen_run"));
        assert_eq!(id, "autogen_run");
    }

    #[test]
    fn spec_space_form_session_path_flag() {
        let _g = env_test_lock();
        clear_session_env();
        let args = vec![
            "--session-path".into(),
            "/data/path".into(),
        ];
        let (path, _) = resolve_session_dir(&args).resolve("auto").unwrap();
        assert_eq!(path.to_str(), Some("/data/path"));
    }

    #[test]
    fn spec_falls_back_to_env() {
        let _g = env_test_lock();
        let prior = std::env::var(SESSION_DIRECTORY_ENV).ok();
        unsafe { std::env::set_var(SESSION_DIRECTORY_ENV, "/from/env"); }
        let spec = resolve_session_dir(&[]);
        match prior {
            Some(v) => unsafe { std::env::set_var(SESSION_DIRECTORY_ENV, v); }
            None    => unsafe { std::env::remove_var(SESSION_DIRECTORY_ENV); }
        }
        let (path, _) = spec.resolve("auto").unwrap();
        assert_eq!(path.to_str(), Some("/from/env"));
    }

    #[test]
    fn spec_cli_flag_overrides_env() {
        let _g = env_test_lock();
        let prior = std::env::var(SESSION_DIRECTORY_ENV).ok();
        unsafe { std::env::set_var(SESSION_DIRECTORY_ENV, "/from/env"); }
        let args = vec!["--session-path=/from/cli".into()];
        let spec = resolve_session_dir(&args);
        match prior {
            Some(v) => unsafe { std::env::set_var(SESSION_DIRECTORY_ENV, v); }
            None    => unsafe { std::env::remove_var(SESSION_DIRECTORY_ENV); }
        }
        let (path, _) = spec.resolve("auto").unwrap();
        assert_eq!(path.to_str(), Some("/from/cli"),
            "CLI --session-path must win over SESSION_DIRECTORY env");
    }

    #[test]
    fn spec_empty_returns_no_resolution() {
        let _g = env_test_lock();
        let prior = std::env::var(SESSION_DIRECTORY_ENV).ok();
        unsafe { std::env::remove_var(SESSION_DIRECTORY_ENV); }
        let spec = resolve_session_dir(&[]);
        if let Some(v) = prior {
            unsafe { std::env::set_var(SESSION_DIRECTORY_ENV, v); }
        }
        assert!(spec.is_empty());
        assert!(spec.resolve("auto").is_none());
    }

    #[test]
    fn spec_needs_auto_id_when_token_present() {
        let _g = env_test_lock();
        clear_session_env();
        let args = vec!["--session-path=/data/SESSION_x".into()];
        assert!(resolve_session_dir(&args).needs_auto_id());
    }

    #[test]
    fn spec_does_not_need_auto_id_when_path_is_concrete() {
        let _g = env_test_lock();
        clear_session_env();
        let args = vec!["--session-path=/data/specific".into()];
        assert!(!resolve_session_dir(&args).needs_auto_id());
    }

    #[test]
    fn spec_does_not_need_auto_id_with_explicit_name() {
        let _g = env_test_lock();
        clear_session_env();
        let args = vec![
            "--session-path=/data/SESSION_x".into(),
            "--session-name=alpha".into(),
        ];
        assert!(!resolve_session_dir(&args).needs_auto_id());
    }

    // -----------------------------------------------------------
    // Umbrella --session kv-list parsing
    // -----------------------------------------------------------

    #[test]
    fn umbrella_dir_shortcut_sets_path() {
        let _g = env_test_lock();
        clear_session_env();
        let args = vec!["--session=dir:asldkfjsldfj".into()];
        let (path, id) = resolve_session_dir(&args).resolve("auto").unwrap();
        assert_eq!(path.to_str(), Some("asldkfjsldfj"));
        assert_eq!(id, "asldkfjsldfj",
            "session id is the basename of the path");
    }

    #[test]
    fn umbrella_dir_with_subpath_yields_basename_id() {
        let _g = env_test_lock();
        clear_session_env();
        let args = vec!["--session=dir:l2k3j4/drr".into()];
        let (path, id) = resolve_session_dir(&args).resolve("auto").unwrap();
        assert_eq!(path.to_str(), Some("l2k3j4/drr"));
        assert_eq!(id, "drr");
    }

    #[test]
    fn umbrella_full_kv_list() {
        let _g = env_test_lock();
        clear_session_env();
        let args = vec![
            "--session=keep:42,name:sessname42,path:sessions/dir/SESSION,reuse:resume".into()
        ];
        let spec = resolve_session_dir(&args);
        assert_eq!(spec.session_name.as_deref(), Some("sessname42"));
        assert_eq!(spec.session_path.as_deref(), Some("sessions/dir/SESSION"));
        assert_eq!(spec.reuse, SessionReuse::Resume);
        assert_eq!(spec.session_keep, 42);
        let (path, id) = spec.resolve("autogen").unwrap();
        assert_eq!(path.to_str(), Some("sessions/dir/sessname42"));
        assert_eq!(id, "sessname42");
    }

    #[test]
    fn umbrella_bare_restart_token_sets_reuse() {
        let _g = env_test_lock();
        clear_session_env();
        let args = vec!["--session=restart,dir:/tmp/x".into()];
        let spec = resolve_session_dir(&args);
        assert_eq!(spec.reuse, SessionReuse::Restart);
        assert_eq!(spec.session_path.as_deref(), Some("/tmp/x"));
    }

    #[test]
    fn umbrella_bare_resume_token_sets_reuse() {
        let _g = env_test_lock();
        clear_session_env();
        let args = vec!["--session=resume,name:foo".into()];
        let spec = resolve_session_dir(&args);
        assert_eq!(spec.reuse, SessionReuse::Resume);
        assert_eq!(spec.session_name.as_deref(), Some("foo"));
    }

    #[test]
    fn umbrella_long_form_overrides_umbrella() {
        let _g = env_test_lock();
        clear_session_env();
        let args = vec![
            "--session=name:from-umbrella".into(),
            "--session-name=from-longform".into(),
        ];
        let spec = resolve_session_dir(&args);
        assert_eq!(spec.session_name.as_deref(), Some("from-longform"));
    }

    #[test]
    fn umbrella_unknown_key_logs_warn_but_keeps_rest() {
        let _g = env_test_lock();
        clear_session_env();
        let args = vec!["--session=name:foo,what:nope,reuse:restart".into()];
        let spec = resolve_session_dir(&args);
        // Unknown 'what:nope' was logged + skipped; recognized
        // entries still applied.
        assert_eq!(spec.session_name.as_deref(), Some("foo"));
        assert_eq!(spec.reuse, SessionReuse::Restart);
    }

    // ── check_session_path: catch the `scenario=foo`-as-path footgun ──

    #[test]
    fn check_session_path_rejects_workload_param_shape() {
        // The classic recurring footgun: `--session-path scenario=foo`
        // would silently create `<cwd>/scenario=foo/...` because the
        // umbrella parser splits only on `:`.
        for bad in &[
            "scenario=foo",
            "scenario=/tmp/foo",
            "scenario=target",
            "scenario=target/test-tmp/x",
            "k=v",
            "key_with_underscore=value",
            "kebab-case=value",
        ] {
            assert!(check_session_path(bad, "test").is_err(),
                "should reject '{bad}'");
        }
    }

    #[test]
    fn check_session_path_accepts_real_paths() {
        for good in &[
            "/tmp/foo",
            "/tmp/foo/bar",
            "logs/session_2026",
            "./local/x",
            "../sibling/y",
            "relative/path",
            "/var/run/x=y",                  // `=` inside path, not at the head
            "C:/Windows/maybe",              // exotic but harmless
            "logs/SESSION/x",
        ] {
            assert!(check_session_path(good, "test").is_ok(),
                "should accept '{good}'");
        }
    }

    #[test]
    fn check_session_path_message_names_remediation() {
        // The error must point the user at the right flag form,
        // not just say "bad path".
        let err = check_session_path("scenario=foo", "test").unwrap_err();
        assert!(err.contains("--session-path"), "missing flag hint: {err}");
        assert!(err.contains(":") && err.contains("kv separator"),
            "missing umbrella-form hint: {err}");
    }

    #[test]
    fn check_session_path_empty_head_passes() {
        // `=value` (empty head) is unusual but doesn't match the
        // param shape — let it through; downstream path APIs will
        // reject it on their own terms.
        assert!(check_session_path("=foo", "test").is_ok());
    }

    #[test]
    fn flag_env_name_canonicalisation() {
        assert_eq!(flag_env_name("--session"), "NBRS_SESSION");
        assert_eq!(flag_env_name("--session-name"), "NBRS_SESSION_NAME");
        assert_eq!(flag_env_name("--session-path"), "NBRS_SESSION_PATH");
        assert_eq!(flag_env_name("--multi-word-flag"), "NBRS_MULTI_WORD_FLAG");
    }

    #[test]
    fn resolve_flag_picks_cli_when_only_cli_set() {
        let _g = env_test_lock();
        unsafe { std::env::remove_var("NBRS_SESSION_NAME"); }
        let args = vec!["--session-name=foo".into()];
        assert_eq!(resolve_flag(&args, "--session-name").as_deref(), Some("foo"));
    }

    #[test]
    fn resolve_flag_picks_env_when_only_env_set() {
        let _g = env_test_lock();
        unsafe { std::env::set_var("NBRS_SESSION_NAME", "bar"); }
        let v = resolve_flag(&[], "--session-name");
        unsafe { std::env::remove_var("NBRS_SESSION_NAME"); }
        assert_eq!(v.as_deref(), Some("bar"));
    }

    #[test]
    fn forecast_keep_purge_counts_excess() {
        let parent = std::env::temp_dir().join(format!(
            "nbrs-forecast-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos(),
        ));
        std::fs::create_dir_all(&parent).unwrap();

        // 5 dirs present, keep=10 → next run wouldn't purge.
        for i in 0..5 {
            std::fs::create_dir(parent.join(format!("s{i}"))).unwrap();
        }
        assert_eq!(forecast_keep_purge(&parent, 10), 0);

        // 5 dirs present, keep=5 → next run would purge 1.
        assert_eq!(forecast_keep_purge(&parent, 5), 1);

        // 5 dirs present, keep=3 → next run would purge 3.
        assert_eq!(forecast_keep_purge(&parent, 3), 3);

        // keep=0 disables.
        assert_eq!(forecast_keep_purge(&parent, 0), 0);

        let _ = std::fs::remove_dir_all(&parent);
    }

    #[test]
    fn resolve_flag_returns_none_when_neither_set() {
        let _g = env_test_lock();
        unsafe { std::env::remove_var("NBRS_SESSION_NAME"); }
        assert!(resolve_flag(&[], "--session-name").is_none());
    }
    // Note: the conflict-error path (both CLI and env set)
    // calls process::exit(2). Testing it requires spawning a
    // subprocess; left to the e2e level.

    #[test]
    fn parse_duration_units() {
        use std::time::Duration;
        assert_eq!(parse_duration("30s").unwrap(), Duration::from_secs(30));
        assert_eq!(parse_duration("5m").unwrap(), Duration::from_secs(300));
        assert_eq!(parse_duration("2h").unwrap(), Duration::from_secs(7200));
        assert_eq!(parse_duration("3d").unwrap(), Duration::from_secs(259200));
        assert_eq!(parse_duration("4w").unwrap(), Duration::from_secs(2419200));
        // Bare integer = seconds.
        assert_eq!(parse_duration("60").unwrap(), Duration::from_secs(60));
    }

    #[test]
    fn parse_duration_rejects_garbage() {
        assert!(parse_duration("").is_err());
        assert!(parse_duration("not-a-number").is_err());
        assert!(parse_duration("abc4w").is_err());
    }

    #[test]
    fn purge_keeps_latest_n_sessions() {
        let parent = std::env::temp_dir().join(format!(
            "nbrs-purge-test-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos(),
        ));
        std::fs::create_dir_all(&parent).unwrap();

        // Create 5 dirs with staggered mtimes.
        for i in 0..5 {
            let d = parent.join(format!("sess_{i}"));
            std::fs::create_dir(&d).unwrap();
            let now = std::time::SystemTime::now() - std::time::Duration::from_secs(60 * (5 - i));
            // Touch via filetime-style: re-create a marker so mtime
            // reflects roughly the right ordering.
            std::fs::write(d.join("metrics.db"), "x").unwrap();
            // Set the dir's modified time via a fresh file write; OS
            // updates mtime as side effect. For test stability, sort
            // order will follow the creation order, which is
            // newest-last.
            let _ = now;
        }

        // Newest 2 should survive after purge with max=2.
        purge_stale_sessions(&parent, 2, std::time::Duration::ZERO);

        let surviving: Vec<_> = std::fs::read_dir(&parent).unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.is_dir())
            .collect();
        assert!(surviving.len() <= 2, "expected ≤2 survivors, got {}: {:?}",
            surviving.len(), surviving);

        // Cleanup
        let _ = std::fs::remove_dir_all(&parent);
    }

    #[test]
    fn purge_skips_logs_latest_symlink() {
        let parent = std::env::temp_dir().join(format!(
            "nbrs-purge-symlink-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos(),
        ));
        std::fs::create_dir_all(&parent).unwrap();
        let active = parent.join("active_session");
        std::fs::create_dir(&active).unwrap();
        std::fs::write(active.join("metrics.db"), "x").unwrap();
        // Create logs/latest symlink → active.
        let _ = std::os::unix::fs::symlink(&active, parent.join("latest"));

        // Purge with aggressive caps — `active` must not be deleted
        // because it's the symlink target.
        purge_stale_sessions(&parent, 0, std::time::Duration::from_secs(1));

        assert!(active.exists(),
            "active session pointed-at by logs/latest must survive purge");
        let _ = std::fs::remove_dir_all(&parent);
    }
}
