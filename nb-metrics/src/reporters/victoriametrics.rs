// Copyright 2024-2026 nosqlbench contributors
// SPDX-License-Identifier: Apache-2.0

//! VictoriaMetrics push reporter.
//!
//! Pushes metrics in Prometheus text exposition format directly to
//! VictoriaMetrics `/api/v1/import/prometheus` endpoint. This is a
//! direct push — no Prometheus pushgateway needed.
//!
//! Feature-gated behind `victoriametrics`.

#[cfg(feature = "victoriametrics")]
mod inner {
    use crate::frame::MetricsFrame;
    use crate::reporters::openmetrics::render_prometheus_text;
    use crate::scheduler::Reporter;

    pub struct VictoriaMetricsReporter {
        endpoint: String,
        client: reqwest::blocking::Client,
        bearer_token: Option<String>,
    }

    impl VictoriaMetricsReporter {
        /// Create a reporter pushing to a VictoriaMetrics instance.
        ///
        /// `endpoint` should be the full URL to the import API, e.g.:
        /// - `http://localhost:8428/api/v1/import/prometheus`
        /// - `https://vm.example.com/api/v1/import/prometheus`
        ///
        /// For VictoriaMetrics Cloud, provide the bearer token.
        pub fn new(endpoint: impl Into<String>) -> Self {
            Self {
                endpoint: endpoint.into(),
                client: reqwest::blocking::Client::new(),
                bearer_token: None,
            }
        }

        /// Parse a shorthand spec:
        /// - `victoria:plain:host:port` → `http://host:port/api/v1/import/prometheus`
        /// - `victoria:tls:host:port` → `https://host:port/api/v1/import/prometheus`
        /// - Full URL → used as-is
        pub fn from_spec(spec: &str) -> Result<Self, String> {
            let endpoint = if spec.starts_with("victoria:") {
                let parts: Vec<&str> = spec.splitn(4, ':').collect();
                if parts.len() < 4 {
                    return Err(format!(
                        "invalid victoria spec: expected 'victoria:plain|tls:host:port', got '{spec}'"
                    ));
                }
                let scheme = match parts[1] {
                    "plain" => "http",
                    "tls" => "https",
                    other => return Err(format!("unknown scheme '{other}', use 'plain' or 'tls'")),
                };
                let host = parts[2];
                let port = parts[3];
                format!("{scheme}://{host}:{port}/api/v1/import/prometheus")
            } else if spec.starts_with("http://") || spec.starts_with("https://") {
                spec.to_string()
            } else {
                return Err(format!("invalid endpoint: '{spec}'"));
            };

            Ok(Self::new(endpoint))
        }

        /// Set a bearer token for authentication (VictoriaMetrics Cloud).
        pub fn with_bearer_token(mut self, token: impl Into<String>) -> Self {
            self.bearer_token = Some(token.into());
            self
        }

        /// Load a bearer token from a file.
        pub fn with_bearer_token_file(mut self, path: &str) -> Result<Self, String> {
            let token = std::fs::read_to_string(path)
                .map_err(|e| format!("failed to read token file '{path}': {e}"))?
                .trim()
                .to_string();
            self.bearer_token = Some(token);
            Ok(self)
        }
    }

    impl Reporter for VictoriaMetricsReporter {
        fn report(&mut self, frame: &MetricsFrame) {
            let body = render_prometheus_text(frame);
            if body.is_empty() { return; }

            let mut request = self.client
                .post(&self.endpoint)
                .header("Content-Type", "text/plain; charset=utf-8")
                .body(body);

            if let Some(ref token) = self.bearer_token {
                request = request.header("Authorization", format!("Bearer {token}"));
            }

            match request.send() {
                Ok(resp) => {
                    if !resp.status().is_success() {
                        eprintln!(
                            "[nb-metrics] VictoriaMetrics push failed: {} {}",
                            resp.status(),
                            resp.text().unwrap_or_default()
                        );
                    }
                }
                Err(e) => {
                    eprintln!("[nb-metrics] VictoriaMetrics push error: {e}");
                }
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn parse_victoria_plain() {
            let r = VictoriaMetricsReporter::from_spec("victoria:plain:localhost:8428").unwrap();
            assert_eq!(r.endpoint, "http://localhost:8428/api/v1/import/prometheus");
        }

        #[test]
        fn parse_victoria_tls() {
            let r = VictoriaMetricsReporter::from_spec("victoria:tls:vm.example.com:8443").unwrap();
            assert_eq!(r.endpoint, "https://vm.example.com:8443/api/v1/import/prometheus");
        }

        #[test]
        fn parse_full_url() {
            let r = VictoriaMetricsReporter::from_spec("https://custom.url/api/v1/import/prometheus").unwrap();
            assert_eq!(r.endpoint, "https://custom.url/api/v1/import/prometheus");
        }

        #[test]
        fn parse_invalid() {
            assert!(VictoriaMetricsReporter::from_spec("victoria:plain").is_err());
        }
    }
}

#[cfg(feature = "victoriametrics")]
pub use inner::VictoriaMetricsReporter;
