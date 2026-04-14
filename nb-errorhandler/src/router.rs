// Copyright 2024-2026 Jonathan Shook
// SPDX-License-Identifier: Apache-2.0

//! Error router: parses config spec, matches errors by type name,
//! dispatches through the handler chain.

use std::collections::HashMap;
use std::sync::Arc;

use regex::Regex;

use crate::detail::ErrorDetail;
use crate::handler::ErrorHandler;
use crate::handlers::builtin_handler;

/// A compiled mapping from error pattern to handler chain.
struct HandlerMapping {
    patterns: Vec<Regex>,
    handlers: Vec<Arc<dyn ErrorHandler>>,
}

/// The error router: classifies errors and dispatches to handler chains.
///
/// # Config Syntax
///
/// ```text
/// "TimeoutError:retry,warn,counter;.*:stop"
/// ```
///
/// - Error patterns (left of `:`) are regex matched against the error name
/// - Handler names (right of `:`) are comma-separated, executed in chain order
/// - Multiple rules are semicolon-separated
/// - If no pattern prefix, `.*` (match all) is assumed
///
/// # Example
///
/// ```
/// use nb_errorhandler::ErrorRouter;
///
/// let router = ErrorRouter::parse("TimeoutError:retry,warn;.*:counter,stop").unwrap();
/// ```
pub struct ErrorRouter {
    mappings: Vec<HandlerMapping>,
    /// Cache: error name → handler chain (lazily populated).
    cache: std::sync::Mutex<HashMap<String, Vec<Arc<dyn ErrorHandler>>>>,
}

impl ErrorRouter {
    /// Parse a config spec into a router.
    pub fn parse(spec: &str) -> Result<Self, String> {
        let mut mappings = Vec::new();

        for rule in spec.split(';') {
            let rule = rule.trim();
            if rule.is_empty() { continue; }

            let (pattern_str, handler_str) = if let Some(colon) = rule.find(':') {
                (&rule[..colon], &rule[colon + 1..])
            } else {
                // No pattern — treat entire string as handler list, match all
                (".*", rule)
            };

            let patterns: Vec<Regex> = pattern_str
                .split(',')
                .map(|p| p.trim())
                .filter(|p| !p.is_empty())
                .map(|p| Regex::new(p).map_err(|e| format!("invalid error pattern '{p}': {e}")))
                .collect::<Result<Vec<_>, _>>()?;

            let handlers: Vec<Arc<dyn ErrorHandler>> = handler_str
                .split(',')
                .map(|h| h.trim())
                .filter(|h| !h.is_empty())
                .map(|h| {
                    builtin_handler(h)
                        .map(|bh| Arc::from(bh) as Arc<dyn ErrorHandler>)
                        .ok_or_else(|| format!("unknown error handler: '{h}'"))
                })
                .collect::<Result<Vec<_>, _>>()?;

            if patterns.is_empty() || handlers.is_empty() {
                continue;
            }

            mappings.push(HandlerMapping { patterns, handlers });
        }

        Ok(Self {
            mappings,
            cache: std::sync::Mutex::new(HashMap::new()),
        })
    }

    /// Create a simple router with a default handler for all errors.
    pub fn default_stop() -> Self {
        Self::parse(".*:stop").unwrap()
    }

    /// Create a router that warns and counts all errors.
    pub fn default_warn_count() -> Self {
        Self::parse(".*:warn,counter").unwrap()
    }

    /// Handle an error: classify by name, dispatch through the matching
    /// handler chain, return the final ErrorDetail.
    pub fn handle_error(
        &self,
        error_name: &str,
        error_msg: &str,
        cycle: u64,
        duration_nanos: u64,
    ) -> ErrorDetail {
        let handlers = self.lookup(error_name);
        let mut detail = ErrorDetail::non_retryable(error_name);

        for handler in &handlers {
            detail = handler.handle(error_name, error_msg, cycle, duration_nanos, detail);
        }

        detail
    }

    fn lookup(&self, error_name: &str) -> Vec<Arc<dyn ErrorHandler>> {
        // Check cache first
        {
            let cache = self.cache.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(handlers) = cache.get(error_name) {
                return handlers.clone();
            }
        }

        // Find first matching rule
        for mapping in &self.mappings {
            for pattern in &mapping.patterns {
                if pattern.is_match(error_name) {
                    let handlers = mapping.handlers.clone();
                    self.cache.lock().unwrap_or_else(|e| e.into_inner()).insert(error_name.to_string(), handlers.clone());
                    return handlers;
                }
            }
        }

        // No match — unhandled error type. Default to stop so
        // unconfigured errors don't silently pass through.
        eprintln!("error: no handler matched error type '{error_name}' — stopping (add a handler pattern to configure)");
        let stop_handler = crate::handlers::builtin_handler("stop").unwrap();
        let handlers = vec![Arc::from(stop_handler) as Arc<dyn ErrorHandler>];
        self.cache.lock().unwrap_or_else(|e| e.into_inner()).insert(error_name.to_string(), handlers.clone());
        handlers
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    

    #[test]
    fn parse_simple() {
        let router = ErrorRouter::parse(".*:warn").unwrap();
        let detail = router.handle_error("AnyError", "msg", 0, 0);
        assert!(!detail.is_retryable());
    }

    #[test]
    fn parse_multiple_handlers() {
        let router = ErrorRouter::parse(".*:retry,warn").unwrap();
        let detail = router.handle_error("AnyError", "msg", 0, 0);
        assert!(detail.is_retryable());
    }

    #[test]
    fn parse_multiple_rules() {
        let router = ErrorRouter::parse("Timeout.*:retry,warn;.*:ignore").unwrap();

        let d1 = router.handle_error("TimeoutError", "timed out", 0, 0);
        assert!(d1.is_retryable());

        let d2 = router.handle_error("OtherError", "other", 0, 0);
        assert!(!d2.is_retryable());
    }

    #[test]
    fn first_matching_rule_wins() {
        let router = ErrorRouter::parse("Timeout:retry;.*:ignore").unwrap();
        let d = router.handle_error("Timeout", "msg", 0, 0);
        assert!(d.is_retryable());
    }

    #[test]
    fn cache_works() {
        let router = ErrorRouter::parse(".*:warn").unwrap();
        // First call populates cache
        router.handle_error("Err1", "msg", 0, 0);
        // Second call hits cache
        router.handle_error("Err1", "msg", 1, 0);
        let cache = router.cache.lock().unwrap();
        assert!(cache.contains_key("Err1"));
    }

    #[test]
    fn no_pattern_defaults_to_catch_all() {
        let router = ErrorRouter::parse("warn,counter").unwrap();
        let detail = router.handle_error("AnyError", "msg", 0, 0);
        // Should have matched — warn doesn't change retry, counter doesn't either
        assert!(!detail.is_retryable());
    }

    #[test]
    fn stop_handler_in_chain() {
        let router = ErrorRouter::parse(".*:warn,stop").unwrap();
        let detail = router.handle_error("Fatal", "kaboom", 42, 0);
        assert!(detail.should_stop, "stop handler in chain should set should_stop");
    }

    #[test]
    fn unknown_handler_rejected() {
        let result = ErrorRouter::parse(".*:bogus_handler");
        assert!(result.is_err());
    }

    #[test]
    fn empty_spec_stops_on_unmatched() {
        let router = ErrorRouter::parse("").unwrap();
        // No rules — unmatched errors default to stop
        let detail = router.handle_error("Err", "msg", 0, 0);
        assert!(detail.should_stop, "unmatched errors should stop execution");
    }

    #[test]
    fn default_constructors() {
        let _ = ErrorRouter::default_stop();
        let r = ErrorRouter::default_warn_count();
        let d = r.handle_error("test", "msg", 0, 0);
        assert!(!d.is_retryable());
    }
}
