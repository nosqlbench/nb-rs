// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! HTTP push reporter for sending metrics in OpenMetrics text format.
//!
//! Posts metrics in Prometheus text exposition format to any HTTP
//! endpoint that accepts it. Works with `nbrs web` instances
//! (`/api/v1/import/prometheus`), VictoriaMetrics, Prometheus
//! Pushgateway, or any compatible receiver.

use nb_metrics::frame::MetricsFrame;
use nb_metrics::reporters::openmetrics::render_prometheus_text;
use nb_metrics::scheduler::Reporter;

/// A `Reporter` that pushes OpenMetrics text to an HTTP endpoint.
pub struct OpenMetricsPushReporter {
    endpoint: String,
    client: reqwest::blocking::Client,
}

impl OpenMetricsPushReporter {
    /// Create a reporter targeting the given URL.
    ///
    /// The URL should be the full endpoint, e.g.
    /// `http://localhost:8080/api/v1/import/prometheus`.
    pub fn new(url: &str) -> Self {
        Self {
            endpoint: url.to_string(),
            client: reqwest::blocking::Client::new(),
        }
    }
}

impl Reporter for OpenMetricsPushReporter {
    fn report(&mut self, frame: &MetricsFrame) {
        let body = render_prometheus_text(frame);
        if body.is_empty() {
            return;
        }
        match self.client
            .post(&self.endpoint)
            .header("Content-Type", "text/plain; charset=utf-8")
            .body(body)
            .send()
        {
            Ok(resp) if !resp.status().is_success() => {
                eprintln!("nbrs: openmetrics push failed: {}", resp.status());
            }
            Err(e) => {
                eprintln!("nbrs: openmetrics push error: {e}");
            }
            _ => {}
        }
    }
}
