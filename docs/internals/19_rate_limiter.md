# Rate Limiter — Design Brief

Study of the nosqlbench SimRate implementation and implications for
nb-rs.

---

## What It Does

Controls the rate of operation dispatch: "run at N ops/sec." The
caller calls `block()` before each operation. If the system is ahead
of target rate, `block()` returns immediately. If behind, it blocks
until a token is available. The blocked time is the **wait_time**
metric that surfaces coordinated omission.

---

## Core Algorithm: Hybrid Token/Leaky Bucket

### Three Pools

```
                   refill thread (every 10ms)
                        │
          ┌─────────────▼─────────────┐
  time →  │  Active Pool (Semaphore)  │ ← callers acquire()
          │     capacity: 1B ticks    │
          └───────────────────────────┘
                        │ overflow
                        ▼
          ┌───────────────────────────┐
          │  Waiting Pool (AtomicLong)│ ← unbounded backlog
          └───────────────────────────┘
                        │ burst recovery
                        ▼
          ┌───────────────────────────┐
          │  Burst Pool (logical)     │ ← 10% extra in Active
          └───────────────────────────┘
```

- **Active Pool** — a `Semaphore` with capacity ~1 billion ticks
  (approximately 1 second of nanoseconds). Callers `acquire(ticksPerOp)`
  to proceed. If insufficient permits, they block.

- **Waiting Pool** — an `AtomicLong` accumulating backlog ticks.
  When the active pool overflows (more time elapsed than the pool can
  hold), excess goes here. This is the "how far behind are we" meter.

- **Burst Pool** — logical extension of the active pool sized by the
  burst ratio (default 1.1x = 10% extra). During recovery, ticks
  flow from waiting pool → burst pool, allowing the system to run
  temporarily faster than target rate to catch up.

### The Time-Scaling Trick

The Semaphore is 32-bit (max 2^31 permits). At high rates (50 ops/s),
nanosecond granularity works: `ticksPerOp = 1B / 50 = 20M` (fits).
At low rates (0.5 ops/s), nanosecond granularity would need 2B ticks
per op — exceeding 2^31 with burst headroom.

**Solution:** Scale the time unit based on rate:

| Rate | Unit | Ticks/Op |
|------|------|---------|
| > 1 ops/s | nanoseconds | 1B / rate |
| > 0.001 ops/s | microseconds | 1M / rate |
| > 0.000001 ops/s | milliseconds | 1K / rate |
| slower | seconds | 1 / rate |

All internal accounting uses "ticks" in the selected unit. Conversion
functions translate between nanos (wall clock) and ticks (semaphore
permits) based on the unit.

### Refill Thread

A dedicated thread runs every ~10ms:

1. **Compute elapsed:** `now - lastRefillAt` in nanoseconds
2. **Convert to ticks:** `nanosToTicks(elapsed)` using the scaled unit
3. **Fill active pool:** add ticks up to `maxActivePool` capacity
4. **Overflow to waiting pool:** excess ticks go to `waitingPool`
5. **Burst recovery:** move ticks from waiting → active (up to burst
   pool size, proportional to time elapsed)

### Burst Recovery Normalization

The key insight: burst recovery is **time-proportional**. If only
10ms elapsed (1% of the 1-second active pool), only 1% of the burst
pool capacity can be backfilled from the waiting pool. This prevents
unfair accumulation — catching up happens gradually, not in one burst.

```
refillFactor = min(newTokens / maxActivePool, 1.0)
burstFillAllowed = refillFactor * burstPoolSize
burstRecovery = min(burstFillAllowed, waitingPool, room_in_active)
```

### Blocking

```rust
fn block(&self) -> u64 {
    self.active_pool.acquire(self.ticks_per_op);  // blocks if insufficient
    self.waiting_pool.load()  // return current backlog
}
```

The Semaphore handles all the blocking/waking efficiently, including
virtual thread compatibility.

---

## Properties

- **Deterministic rate:** exactly N ops/sec sustained
- **Coordinated omission aware:** wait_time = time blocked, reported
  as a metric. response_time = service_time + wait_time.
- **Burst recovery:** after a slow period, the system can temporarily
  exceed target rate (by burst ratio) to catch up
- **Dynamic adjustment:** rate can be changed at runtime (filler
  thread restarts with new parameters)
- **Thread-safe:** Semaphore + AtomicLong, no locks on the hot path
- **Virtual thread friendly:** Semaphore is the canonical Java
  mechanism for this

---

## Implications for Rust

### What maps cleanly

- The time-scaling trick works identically in Rust
- AtomicU64 for the waiting pool
- The refill thread is a simple `thread::spawn` + `thread::park_timeout`

### What changes

- **No Semaphore in std Rust.** The `tokio::sync::Semaphore` works for
  async, but for sync blocking we need either:
  - A `parking_lot` condvar-based approach
  - A crossbeam-channel based token dispenser
  - A custom implementation using `std::sync::Condvar`
  - The `async-semaphore` or `tokio` Semaphore if we're async

- **Atomic ops are the same.** Rust's `AtomicU64` is equivalent.

- **The refill thread** maps directly to `std::thread::spawn` with
  `std::thread::sleep` for the 10ms interval.

### Open question

Should the rate limiter be sync (blocking thread) or async (tokio)?
The answer depends on whether the engine's execution loop is sync
threads or async tasks — which is part of the execution layer design
we'll discuss separately.

---

## Configuration

```
rate=1000           — 1000 ops/s, 1.1x burst, auto-start
rate=1000,1.5       — 1000 ops/s, 1.5x burst
rate=1000,1.1,restart — restart (zero backlog)
```

The spec is a simple string: `<rate>[,<burst_ratio>][,<verb>]`
where verb is `start`, `configure`, `restart`, or `stop`.
