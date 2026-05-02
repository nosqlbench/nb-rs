// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Delta HDR Histogram for latency/value distribution recording.
//!
//! Each `snapshot()` call returns the data accumulated since the last
//! snapshot (delta semantics). The Recorder's interval is swapped
//! atomically, so the hot path (`record()`) and the snapshot path
//! don't contend.

use std::sync::Mutex;
use hdrhistogram::Histogram as HdrHistogram;
use crate::labels::Labels;

/// Default significant digits for HDR Histograms (0.1% error).
pub const DEFAULT_HDR_SIGDIGS: u8 = 3;


/// Component property name read by [`Histogram::with_sigdigs_from`]
/// and [`crate::instruments::timer::Timer::with_sigdigs_from`] to
/// resolve the active precision. Set on the session root by the
/// runner; descendants pick it up via [`crate::component::Component::get_prop`]
/// walk-up. See SRD 40 §"HDR significant digits — subtree-scoped
/// setting".
pub const HDR_SIGDIGS_PROP: &str = "hdr.sigdigs";

/// Maximum trackable value in nanoseconds (~1 hour).
const MAX_VALUE: u64 = 3_600_000_000_000;

pub struct Histogram {
    labels: Labels,
    /// The accumulating histogram. Protected by mutex for the swap.
    current: Mutex<HdrHistogram<u64>>,
}

impl Histogram {
    pub fn new(labels: Labels) -> Self {
        Self::with_sigdigs(labels, DEFAULT_HDR_SIGDIGS)
    }

    /// Construct with an explicit HDR significant-digits
    /// precision. Use this from a call site that already
    /// knows the desired precision (e.g. the runner that has
    /// already resolved [`HDR_SIGDIGS_PROP`] up the component
    /// tree).
    pub fn with_sigdigs(labels: Labels, sigdigs: u8) -> Self {
        Self {
            labels,
            current: Mutex::new(
                HdrHistogram::new_with_bounds(1, MAX_VALUE, sigdigs)
                    .expect("failed to create HDR histogram")
            ),
        }
    }

    /// Construct from a component, walking up the tree to find
    /// the configured [`HDR_SIGDIGS_PROP`] property. Falls back
    /// to [`DEFAULT_HDR_SIGDIGS`] if no ancestor declares it.
    /// SRD 40 §"HDR significant digits — subtree-scoped setting".
    pub fn with_sigdigs_from(
        labels: Labels,
        component: &crate::component::Component,
    ) -> Self {
        let sigdigs = resolve_hdr_sigdigs(component);
        Self::with_sigdigs(labels, sigdigs)
    }
}

/// Resolve the configured HDR significant-digits precision
/// from a component, walking up the tree. Returns
/// [`DEFAULT_HDR_SIGDIGS`] if no ancestor declares
/// [`HDR_SIGDIGS_PROP`] or the value is unparseable.
pub fn resolve_hdr_sigdigs(component: &crate::component::Component) -> u8 {
    component.get_prop(HDR_SIGDIGS_PROP)
        .and_then(|s| s.parse::<u8>().ok())
        .filter(|&n| n >= 1 && n <= 5)
        .unwrap_or(DEFAULT_HDR_SIGDIGS)
}

impl Histogram {

    /// Record a value (typically nanoseconds).
    pub fn record(&self, value: u64) {
        let value = value.min(MAX_VALUE);
        let mut h = self.current.lock()
            .unwrap_or_else(|e| e.into_inner());
        if let Err(e) = h.record(value) {
            crate::diag::warn(&format!("warning: histogram record failed for value {value}: {e}"));
        }
    }

    /// Swap out the current histogram and return the delta.
    ///
    /// The returned histogram contains all data since the last
    /// `snapshot()` call. The internal histogram is reset.
    pub fn snapshot(&self) -> HdrHistogram<u64> {
        let mut current = self.current.lock()
            .unwrap_or_else(|e| e.into_inner());
        let snapshot = current.clone();
        current.reset();
        snapshot
    }

    /// Produce a snapshot by CLONING the current histogram rather than
    /// swapping it out. The instrument keeps accumulating against the
    /// same state — no reservoir disturbance — so consumers reading
    /// "now" values between reporter ticks don't steal samples from
    /// the next delta snapshot.
    ///
    /// Cost: one HDR histogram clone (~200 KiB at 3-significant-digit
    /// precision over a 1-hour range). Acceptable for occasional
    /// calls — not intended for the per-sample hot path.
    pub fn peek_snapshot(&self) -> HdrHistogram<u64> {
        self.current.lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    pub fn labels(&self) -> &Labels {
        &self.labels
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_hdr_sigdigs_uses_default_without_property() {
        let comp = crate::component::Component::root(
            crate::labels::Labels::empty(),
            std::collections::HashMap::new(),
        );
        let guard = comp.read().unwrap();
        assert_eq!(resolve_hdr_sigdigs(&guard), DEFAULT_HDR_SIGDIGS);
    }

    #[test]
    fn resolve_hdr_sigdigs_reads_root_property() {
        let mut props = std::collections::HashMap::new();
        props.insert(HDR_SIGDIGS_PROP.to_string(), "4".to_string());
        let comp = crate::component::Component::root(
            crate::labels::Labels::empty(),
            props,
        );
        let guard = comp.read().unwrap();
        assert_eq!(resolve_hdr_sigdigs(&guard), 4);
    }

    #[test]
    fn resolve_hdr_sigdigs_walks_up_from_descendant() {
        use std::sync::Arc;
        use std::sync::RwLock;

        let mut root_props = std::collections::HashMap::new();
        root_props.insert(HDR_SIGDIGS_PROP.to_string(), "5".to_string());
        let root = crate::component::Component::root(
            crate::labels::Labels::empty().with("type", "session"),
            root_props,
        );
        let phase = Arc::new(RwLock::new(crate::component::Component::new(
            crate::labels::Labels::empty().with("type", "phase"),
            std::collections::HashMap::new(),
        )));
        crate::component::attach(&root, &phase);

        // Descendant has no own hdr.sigdigs but walks up to find
        // the session-scoped 5.
        let pg = phase.read().unwrap();
        assert_eq!(resolve_hdr_sigdigs(&pg), 5);
    }

    #[test]
    fn resolve_hdr_sigdigs_clamps_invalid_value_to_default() {
        let mut props = std::collections::HashMap::new();
        props.insert(HDR_SIGDIGS_PROP.to_string(), "99".to_string());
        let comp = crate::component::Component::root(
            crate::labels::Labels::empty(),
            props,
        );
        let guard = comp.read().unwrap();
        assert_eq!(resolve_hdr_sigdigs(&guard), DEFAULT_HDR_SIGDIGS,
            "invalid sigdigs values fall back to the default");
    }

    #[test]
    fn histogram_with_sigdigs_from_uses_walk_up_value() {
        let mut props = std::collections::HashMap::new();
        props.insert(HDR_SIGDIGS_PROP.to_string(), "2".to_string());
        let comp = crate::component::Component::root(
            crate::labels::Labels::empty(),
            props,
        );
        let guard = comp.read().unwrap();
        // Constructs without panic at the resolved precision.
        let _h = Histogram::with_sigdigs_from(
            Labels::of("name", "latency"),
            &guard,
        );
    }

    #[test]
    fn histogram_record_and_snapshot() {
        let h = Histogram::new(Labels::of("name", "latency"));
        h.record(1_000_000); // 1ms
        h.record(2_000_000); // 2ms
        h.record(3_000_000); // 3ms

        let snap = h.snapshot();
        assert_eq!(snap.len(), 3);
        assert!(snap.min() >= 999_000); // HDR bucketing
        assert!(snap.max() <= 3_100_000);
    }

    #[test]
    fn histogram_delta_semantics() {
        let h = Histogram::new(Labels::of("name", "test"));
        h.record(1_000);
        h.record(2_000);

        let snap1 = h.snapshot();
        assert_eq!(snap1.len(), 2);

        // After snapshot, histogram is reset
        h.record(3_000);
        let snap2 = h.snapshot();
        assert_eq!(snap2.len(), 1); // only the new record
    }

    #[test]
    fn histogram_empty_snapshot() {
        let h = Histogram::new(Labels::of("name", "empty"));
        let snap = h.snapshot();
        assert_eq!(snap.len(), 0);
    }

    #[test]
    fn peek_snapshot_does_not_drain() {
        let h = Histogram::new(Labels::of("name", "peek"));
        h.record(1_000_000);
        h.record(2_000_000);
        h.record(3_000_000);

        // Peek: full data visible, instrument NOT reset.
        let peek1 = h.peek_snapshot();
        assert_eq!(peek1.len(), 3);
        let peek2 = h.peek_snapshot();
        assert_eq!(peek2.len(), 3, "peek should be idempotent");

        // After a real snapshot() the instrument IS reset — peek
        // returns empty, proving peek and snapshot target the same
        // reservoir.
        let _drained = h.snapshot();
        let peek_after = h.peek_snapshot();
        assert_eq!(peek_after.len(), 0);
    }

    #[test]
    fn histogram_quantiles() {
        let h = Histogram::new(Labels::of("name", "q"));
        for i in 1..=1000 {
            h.record(i * 1000); // 1µs to 1ms
        }
        let snap = h.snapshot();
        let p50 = snap.value_at_quantile(0.5);
        let p99 = snap.value_at_quantile(0.99);
        assert!(p50 > 400_000 && p50 < 600_000, "p50={p50}");
        assert!(p99 > 980_000 && p99 < 1_100_000, "p99={p99}");
    }
}
