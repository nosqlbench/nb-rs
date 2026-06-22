// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! SRD-88 — the task-local **`ExecutionContext`**.
//!
//! The load-bearing seam for running multiple executions concurrently in one
//! process, all sharing one session. A deeply-nested fiber finds *its*
//! execution's isolated state — `exec_id`, stop flag, and (as the
//! de-globalization proceeds in later pushes) observer / output channel /
//! scene tree — by reading this task-local context instead of a
//! process-global static.
//!
//! **Axiom A1 — additive de-globalization.** The process-globals
//! (`SESSION_STOP`, `GLOBAL_OBSERVER`, `CHANNEL`, …) remain the **process
//! default**. Code that does not run inside [`scope`] (bootstrap, the CLI,
//! single-run `nbrs run`, tests) reads the default — behavior is identical to
//! before this seam existed. Only concurrent executions scope a context and
//! get isolation, so the migration is safe and incremental: each accessor that
//! learns to consult the context is a no-op until someone scopes one.
//!
//! **Axiom A2 — `exec_id` is the only new global.** Allocating `exec_id` needs
//! one process-global counter so two concurrent *fresh* executions can't both
//! claim the same id. [`alloc_exec_id`] is that single synchronization point.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use arc_swap::ArcSwapOption;

use crate::scene_tree::SceneTree;

/// Per-execution context (SRD-88 §3). Grows as the de-globalization proceeds;
/// Push 1 carries the `exec_id`, the per-execution stop flag, and the
/// per-execution observer (display/lifecycle routing).
pub struct ExecutionContext {
    /// This execution's id — the partition key for its metrics, log lines,
    /// and checkpoint events in the shared session store (SRD-77).
    pub exec_id: u64,
    /// This execution's stop flag. A stop scoped here halts *this* execution
    /// only; a global Ctrl-C (`session_signals::SESSION_STOP`) still halts
    /// every execution (see [`crate::session_signals::stop_requested`]).
    pub stop: Arc<AtomicBool>,
    /// This execution's observer (lifecycle + log routing). `None` falls back
    /// to the process-global `GLOBAL_OBSERVER` (axiom A1) — so a context with
    /// no observer set behaves exactly as the single-run path.
    pub observer: Option<Arc<dyn crate::observer::RunObserver>>,
    /// This execution's scene tree (its own scenario structure + lifecycle
    /// status). **Late-bound:** the pre-map walker installs it *during* the
    /// run, so it is interior-mutable. Empty until installed; reads fall back
    /// to the process-global `GLOBAL_TREE` (A1). The session is the common
    /// root (SRD-88) — each execution's tree derives from the shared session
    /// scope; this slot holds the per-execution derived structure.
    pub scene_tree: ArcSwapOption<RwLock<SceneTree>>,
    /// This execution's output channel (SRD-87 op-output / log / status /
    /// raster buckets). `None` falls back to the process-global `CHANNEL`
    /// (axiom A1). A concurrent in-process execution scopes its own (e.g. a
    /// `CaptureChannel`) so its op stdout is captured per-execution instead of
    /// colliding on the one process fd — what in-process example verification
    /// needs to check `expect` regexes against each execution's output.
    pub channel: Option<Arc<dyn crate::output_channel::OutputChannel>>,
}

impl ExecutionContext {
    /// A fresh context with a freshly-allocated `exec_id` and its own stop
    /// flag, no per-execution observer (falls back to the global — A1).
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            exec_id: alloc_exec_id(),
            stop: Arc::new(AtomicBool::new(false)),
            observer: None,
            scene_tree: ArcSwapOption::const_empty(),
            channel: None,
        })
    }

    /// A fresh context that routes lifecycle/log through its own `observer`
    /// instead of the process-global. Used by concurrent executions so each
    /// folds its own outcome independently.
    pub fn with_observer(observer: Arc<dyn crate::observer::RunObserver>) -> Arc<Self> {
        Arc::new(Self {
            exec_id: alloc_exec_id(),
            stop: Arc::new(AtomicBool::new(false)),
            observer: Some(observer),
            scene_tree: ArcSwapOption::const_empty(),
            channel: None,
        })
    }

    /// A fresh context routing both lifecycle/log (`observer`) AND the SRD-87
    /// output buckets (`channel`) through its own per-execution sinks. Used by
    /// in-process example verification so each concurrent execution's op stdout
    /// is captured separately.
    pub fn with_observer_and_channel(
        observer: Arc<dyn crate::observer::RunObserver>,
        channel: Arc<dyn crate::output_channel::OutputChannel>,
    ) -> Arc<Self> {
        Arc::new(Self {
            exec_id: alloc_exec_id(),
            stop: Arc::new(AtomicBool::new(false)),
            observer: Some(observer),
            scene_tree: ArcSwapOption::const_empty(),
            channel: Some(channel),
        })
    }
}

tokio::task_local! {
    static EXEC_CTX: Arc<ExecutionContext>;
}


/// The `exec_id` allocator — SRD-88 axiom A2, the one unavoidable global.
/// Monotonic per process; the first allocated id is `1`, matching the legacy
/// single-execution session's `exec_id`.
static NEXT_EXEC_ID: AtomicU64 = AtomicU64::new(1);

/// Allocate the next process-unique `exec_id`.
pub fn alloc_exec_id() -> u64 {
    NEXT_EXEC_ID.fetch_add(1, Ordering::Relaxed)
}

/// The current task's [`ExecutionContext`], if running inside a [`scope`].
pub fn try_current() -> Option<Arc<ExecutionContext>> {
    EXEC_CTX.try_with(|c| c.clone()).ok()
}

/// The current execution's `exec_id`, or `1` (the legacy single-execution
/// default) outside any execution scope.
pub fn current_exec_id() -> u64 {
    EXEC_CTX.try_with(|c| c.exec_id).unwrap_or(1)
}

/// The current execution's stop flag, if scoped (`None` outside a scope, so
/// the caller falls back to the process-global stop — A1).
pub fn current_stop() -> Option<Arc<AtomicBool>> {
    EXEC_CTX.try_with(|c| c.stop.clone()).ok()
}

/// The current execution's observer, if a scoped context set one. `None`
/// outside a scope OR when the scoped context has no observer — the caller
/// falls back to the process-global `GLOBAL_OBSERVER` (A1).
pub fn current_observer() -> Option<Arc<dyn crate::observer::RunObserver>> {
    EXEC_CTX.try_with(|c| c.observer.clone()).ok().flatten()
}

/// The current execution's output channel, if a scoped context set one.
/// `None` outside a scope OR when the scoped context has no channel — the
/// caller falls back to the process-global `CHANNEL` (A1).
pub fn current_channel() -> Option<Arc<dyn crate::output_channel::OutputChannel>> {
    EXEC_CTX.try_with(|c| c.channel.clone()).ok().flatten()
}

/// The current execution's scene tree, if scoped AND installed (`None`
/// otherwise — the caller falls back to the process-global `GLOBAL_TREE`, A1).
pub fn current_scene_tree() -> Option<Arc<RwLock<SceneTree>>> {
    EXEC_CTX.try_with(|c| c.scene_tree.load_full()).ok().flatten()
}

/// Install `tree` into the current execution's context, returning `true` if a
/// context was scoped (so the caller installs into the global only when there
/// is no execution scope). The pre-map walker calls this so each execution's
/// lifecycle mutations land on its own tree.
pub fn install_scene_tree(tree: Arc<RwLock<SceneTree>>) -> bool {
    EXEC_CTX
        .try_with(|c| c.scene_tree.store(Some(tree)))
        .is_ok()
}

/// Run `fut` with `ctx` as the task-local [`ExecutionContext`] for the whole
/// future. Concurrent executions each scope their own context, so their
/// task-local reads (stop flag, `exec_id`, …) resolve independently.
pub async fn scope<F: std::future::Future>(ctx: Arc<ExecutionContext>, fut: F) -> F::Output {
    EXEC_CTX.scope(ctx, fut).await
}

/// Wrap `fut` so it runs under the CURRENT execution context — captured **now**
/// (at the call site, which is still inside the parent's scope) and
/// re-established as the task-local inside a freshly-spawned task. A no-op
/// pass-through when there is no current context (single-run / CLI / tests —
/// axiom A1).
///
/// `tokio::spawn` / `JoinSet::spawn` start a NEW task that does **not** inherit
/// the parent's task-locals, so the per-execution context (observer, scene
/// tree, stop flag, `exec_id`) would be lost in the spawned per-cycle fibers.
/// Wrapping each spawned future with `propagate` carries the context across the
/// spawn boundary, so a fiber deep inside a concurrent execution still resolves
/// to *its* execution's state.
pub fn propagate<F>(fut: F) -> impl std::future::Future<Output = F::Output> + Send
where
    F: std::future::Future + Send + 'static,
    F::Output: Send,
{
    let ctx = try_current();
    async move {
        match ctx {
            Some(c) => scope(c, fut).await,
            None => fut.await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alloc_exec_id_is_monotonic_and_unique() {
        let a = alloc_exec_id();
        let b = alloc_exec_id();
        assert!(b > a, "exec_id must be monotonic: {a} then {b}");
    }

    #[tokio::test]
    async fn outside_a_scope_defaults_to_legacy_single_execution() {
        // A1: no context scoped → legacy defaults, no isolation surface.
        assert_eq!(current_exec_id(), 1);
        assert!(current_stop().is_none());
        assert!(try_current().is_none());
    }

    #[tokio::test]
    async fn propagate_carries_context_across_spawn() {
        // A `tokio::spawn`ed task does NOT inherit the parent's task-locals, so
        // a bare spawn would see the default exec_id (1). `propagate` captures
        // the current context and re-establishes it inside the spawned task.
        let ctx = ExecutionContext::new();
        let id = ctx.exec_id;
        let (bare, wrapped) = scope(ctx, async move {
            let bare = tokio::spawn(async { current_exec_id() }).await.unwrap();
            let wrapped = tokio::spawn(propagate(async { current_exec_id() }))
                .await
                .unwrap();
            (bare, wrapped)
        })
        .await;
        assert_eq!(bare, 1, "a bare spawn loses the context (sees the default)");
        assert_eq!(wrapped, id, "propagate carries the exec_id across the spawn");
    }

    #[tokio::test]
    async fn per_execution_stop_is_isolated() {
        let a = ExecutionContext::new();
        let b = ExecutionContext::new();
        assert_ne!(a.exec_id, b.exec_id, "concurrent executions get distinct ids");

        // Stop A only.
        a.stop.store(true, Ordering::Relaxed);

        // Inside A's scope the stop is observed; inside B's it is not — a stop
        // scoped to one execution does NOT halt its concurrent sibling.
        let a_id = scope(a.clone(), async { current_exec_id() }).await;
        let a_stopped = scope(a.clone(), async {
            crate::session_signals::stop_requested()
        })
        .await;
        let b_stopped = scope(b.clone(), async {
            crate::session_signals::stop_requested()
        })
        .await;

        assert_eq!(a_id, a.exec_id, "the scoped exec_id resolves to A's");
        assert!(a_stopped, "A observes its own stop inside A's scope");
        assert!(!b_stopped, "B must NOT see A's stop — executions are isolated");
    }
}
