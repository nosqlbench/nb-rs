// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! TUI reporter: receives metrics frames and sends them to the TUI
//! render thread via a channel.

use std::sync::mpsc;

use nb_metrics::frame::MetricsFrame;
use nb_metrics::scheduler::Reporter;

/// A metrics reporter that forwards frames to the TUI via a channel.
pub struct TuiReporter {
    sender: mpsc::Sender<MetricsFrame>,
}

impl TuiReporter {
    /// Create a reporter and its receiving end.
    ///
    /// The sender goes to the metrics scheduler (as a Reporter).
    /// The receiver goes to the TUI thread (to update MetricsState).
    pub fn channel() -> (Self, mpsc::Receiver<MetricsFrame>) {
        let (sender, receiver) = mpsc::channel();
        (Self { sender }, receiver)
    }
}

impl Reporter for TuiReporter {
    fn report(&mut self, frame: &MetricsFrame) {
        // Non-blocking send — if the TUI is slow, drop frames
        let _ = self.sender.send(frame.clone());
    }
}
