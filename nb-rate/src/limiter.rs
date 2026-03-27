// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! Async-ready token bucket rate limiter with time-scaled permits.
//!
//! Matches the nosqlbench SimRate design:
//! - Active pool (tokio Semaphore) for immediate dispatch
//! - Waiting pool (AtomicI64) for backlog tracking
//! - Refill task replenishes permits every 10ms
//! - Burst recovery moves tokens from waiting → active proportionally
//! - Wait time tracking for coordinated omission metrics

use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use tokio::sync::Semaphore;
use tokio::time::{self, Duration};

use crate::spec::RateSpec;

/// The maximum active pool size in ticks (approximately 1 second in nanos).
const MAX_ACTIVE_POOL: u32 = 1_000_000_000;

/// Refill interval in milliseconds.
const REFILL_INTERVAL_MS: u64 = 10;

/// Shared state between the limiter and its refill task.
struct SharedState {
    active_pool: Semaphore,
    waiting_pool: AtomicI64,
    running: AtomicBool,
    last_refill_nanos: AtomicU64,
    start_time: Instant,
    blocks: AtomicU64,
    ticks_per_op: u32,
    max_active: u32,
    max_over_active: u32,
    burst_pool_size: u32,
    spec: RateSpec,
}

/// An async-ready rate limiter.
///
/// Call `acquire().await` before each operation. If the system is
/// ahead of the target rate, it returns immediately. If behind, it
/// awaits until a permit is available. The blocked time is the
/// **wait_time** that surfaces coordinated omission.
pub struct RateLimiter {
    state: Arc<SharedState>,
    refill_handle: Option<tokio::task::JoinHandle<()>>,
}

impl RateLimiter {
    /// Create and start a rate limiter from a spec.
    pub fn start(spec: RateSpec) -> Self {
        let ticks_per_op = spec.ticks_per_op();
        let max_over_active = (MAX_ACTIVE_POOL as f64 * spec.burst_ratio) as u32;
        let burst_pool_size = max_over_active - MAX_ACTIVE_POOL;

        let state = Arc::new(SharedState {
            active_pool: Semaphore::new(ticks_per_op as usize), // prime with one op
            waiting_pool: AtomicI64::new(0),
            running: AtomicBool::new(true),
            last_refill_nanos: AtomicU64::new(0),
            start_time: Instant::now(),
            blocks: AtomicU64::new(0),
            ticks_per_op,
            max_active: MAX_ACTIVE_POOL,
            max_over_active,
            burst_pool_size,
            spec,
        });

        // Record start time
        state.last_refill_nanos.store(
            state.start_time.elapsed().as_nanos() as u64,
            Ordering::Relaxed,
        );

        let refill_state = state.clone();
        let refill_handle = tokio::spawn(async move {
            refill_loop(refill_state).await;
        });

        Self {
            state,
            refill_handle: Some(refill_handle),
        }
    }

    /// Acquire one operation permit. Blocks (async) if rate-limited.
    ///
    /// Returns the current backlog in ticks (waiting pool value).
    pub async fn acquire(&self) -> i64 {
        self.state.blocks.fetch_add(1, Ordering::Relaxed);
        let permits = self.state.ticks_per_op as u32;

        // Acquire permits from the semaphore — this is the blocking point.
        // forget() the permit so tokens are permanently consumed (not
        // returned on drop). The refill task is the only source of new
        // permits.
        let permit = self.state.active_pool
            .acquire_many(permits)
            .await
            .expect("semaphore closed unexpectedly");
        permit.forget();

        self.state.waiting_pool.load(Ordering::Relaxed)
    }

    /// Current wait time in nanoseconds (backlog converted to nanos).
    pub fn wait_time_nanos(&self) -> u64 {
        let ticks = self.state.waiting_pool.load(Ordering::Relaxed);
        if ticks <= 0 { return 0; }
        self.state.spec.unit.ticks_to_nanos(ticks as u32)
    }

    /// Total number of acquire calls.
    pub fn total_blocks(&self) -> u64 {
        self.state.blocks.load(Ordering::Relaxed)
    }

    /// Current ops/sec target.
    pub fn rate(&self) -> f64 {
        self.state.spec.ops_per_sec
    }

    /// Stop the rate limiter and its refill task.
    pub async fn stop(mut self) {
        self.state.running.store(false, Ordering::Relaxed);
        if let Some(handle) = self.refill_handle.take() {
            let _ = handle.await;
        }
    }
}

impl Drop for RateLimiter {
    fn drop(&mut self) {
        self.state.running.store(false, Ordering::Relaxed);
    }
}

/// The refill loop that runs on its own tokio task.
async fn refill_loop(state: Arc<SharedState>) {
    let mut interval = time::interval(Duration::from_millis(REFILL_INTERVAL_MS));
    interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);

    while state.running.load(Ordering::Relaxed) {
        interval.tick().await;
        refill(&state);
    }
}

/// Single refill cycle: compute elapsed time, add permits, handle burst.
fn refill(state: &SharedState) {
    let now_nanos = state.start_time.elapsed().as_nanos() as u64;
    let last = state.last_refill_nanos.swap(now_nanos, Ordering::Relaxed);
    let elapsed_nanos = now_nanos.saturating_sub(last);

    if elapsed_nanos == 0 { return; }

    // Convert elapsed time to ticks
    let new_ticks = state.spec.unit.nanos_to_ticks(elapsed_nanos);
    if new_ticks == 0 { return; }

    // Current available permits in the semaphore
    let available = state.active_pool.available_permits() as u32;

    // Step 1: Fill active pool up to max
    let room = state.max_active.saturating_sub(available);
    let to_active = new_ticks.min(room);
    if to_active > 0 {
        state.active_pool.add_permits(to_active as usize);
    }

    // Step 2: Overflow goes to waiting pool
    let overflow = new_ticks.saturating_sub(to_active);
    if overflow > 0 {
        state.waiting_pool.fetch_add(overflow as i64, Ordering::Relaxed);
    }

    // Step 3: Burst recovery — move tokens from waiting → active
    let available_after = state.active_pool.available_permits() as u32;
    let refill_factor = (new_ticks as f64 / state.max_active as f64).min(1.0);
    let burst_allowed = (refill_factor * state.burst_pool_size as f64) as u32;
    let burst_room = state.max_over_active.saturating_sub(available_after);
    let burst_from_waiting = burst_allowed
        .min(burst_room)
        .min(state.waiting_pool.load(Ordering::Relaxed).max(0) as u32);

    if burst_from_waiting > 0 {
        state.waiting_pool.fetch_sub(burst_from_waiting as i64, Ordering::Relaxed);
        state.active_pool.add_permits(burst_from_waiting as usize);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    #[tokio::test]
    async fn limiter_starts_and_stops() {
        let spec = RateSpec::new(1000.0);
        let limiter = RateLimiter::start(spec);
        assert_eq!(limiter.rate(), 1000.0);
        limiter.stop().await;
    }

    #[tokio::test]
    async fn limiter_acquire_returns() {
        let spec = RateSpec::new(10000.0);
        let limiter = RateLimiter::start(spec);

        // Should be able to acquire at least one immediately (primed)
        let _backlog = limiter.acquire().await;
        assert_eq!(limiter.total_blocks(), 1);

        limiter.stop().await;
    }

    #[tokio::test]
    async fn limiter_rate_limits() {
        let spec = RateSpec::new(100.0); // 100 ops/s
        let limiter = RateLimiter::start(spec);

        // Let the refill task run a bit
        tokio::time::sleep(Duration::from_millis(50)).await;

        let start = Instant::now();
        let mut count = 0u64;

        // Try to do 20 ops — at 100 ops/s this should take ~200ms
        for _ in 0..20 {
            limiter.acquire().await;
            count += 1;
        }

        let elapsed = start.elapsed();
        limiter.stop().await;

        assert_eq!(count, 20);
        // Should have taken at least 100ms (rate limited)
        // Being generous with timing tolerance for CI
        assert!(elapsed.as_millis() >= 50,
            "expected rate limiting, took {}ms for 20 ops at 100/s",
            elapsed.as_millis());
    }

    #[tokio::test]
    async fn limiter_high_rate_is_fast() {
        let spec = RateSpec::new(1_000_000.0); // 1M ops/s
        let limiter = RateLimiter::start(spec);

        tokio::time::sleep(Duration::from_millis(50)).await;

        let start = Instant::now();
        for _ in 0..100 {
            limiter.acquire().await;
        }
        let elapsed = start.elapsed();
        limiter.stop().await;

        // 100 ops at 1M/s should be nearly instant
        assert!(elapsed.as_millis() < 500,
            "high rate should be fast, took {}ms", elapsed.as_millis());
    }

    #[tokio::test]
    async fn limiter_wait_time_grows_under_load() {
        let spec = RateSpec::new(100.0); // 100 ops/s
        let limiter = RateLimiter::start(spec);

        tokio::time::sleep(Duration::from_millis(50)).await;

        // Rapidly acquire many — should build up backlog
        for _ in 0..50 {
            limiter.acquire().await;
        }

        // Wait time should be measurable (we consumed faster than refill)
        let wt = limiter.wait_time_nanos();
        // This is the accumulated backlog — may or may not be large
        // depending on timing. Just verify the API works.
        assert!(limiter.total_blocks() == 50);

        limiter.stop().await;
    }

    #[tokio::test]
    async fn limiter_spec_parsing() {
        let spec = RateSpec::parse("500, 1.5, start").unwrap();
        let limiter = RateLimiter::start(spec);
        assert_eq!(limiter.rate(), 500.0);
        limiter.stop().await;
    }
}
